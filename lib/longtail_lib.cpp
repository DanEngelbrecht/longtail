#include "longtail_lib.h"

#define BIKESHED_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "../src/longtail.h"
#include "../src/stb_ds.h"

#include "../third-party/bikeshed/bikeshed.h"
#include "../third-party/lizard/lib/lizard_common.h"
#include "../third-party/lizard/lib/lizard_decompress.h"
#include "../third-party/lizard/lib/lizard_compress.h"
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"
#include "../third-party/nadir/src/nadir.h"
#include "../third-party/trove/src/trove.h"

#include <stdio.h>

#define TEST_LOG(fmt, ...) \
    fprintf(stderr, "--- ");fprintf(stderr, fmt, __VA_ARGS__);

#if !defined(PLATFORM_ATOMICADD)
    #if defined(__clang__) || defined(__GNUC__)
        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (__sync_add_and_fetch (value, amount))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (_InterlockedExchangeAdd((volatile LONG *)value, amount) + amount)
    #else
        inline int32_t Platform_NonAtomicAdd(volatile int32_t* store, int32_t value) { *store += value; return *store; }
        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (Platform_NonAtomicAdd(value, amount))
    #endif
#else
    #define PLATFORM_ATOMICADD_PRIVATE PLATFORM_ATOMICADD
#endif

#if !defined(PLATFORM_SLEEP)
    #if defined(__clang__) || defined(__GNUC__)
        #define PLATFORM_SLEEP_PRIVATE(timeout_us) (::usleep((useconds_t)timeout_us))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define PLATFORM_SLEEP_PRIVATE(timeout_us) (::Sleep((DWORD)(timeout_us / 1000)))
    #endif
#else
    #define PLATFORM_SLEEP_PRIVATE PLATFORM_SLEEP
#endif

#if defined(_WIN32)

#if !defined(_WINDOWS_)
    #if !defined(_WINDOWS_)
        #define WIN32_LEAN_AND_MEAN
        #include <Windows.h>
        #undef WIN32_LEAN_AND_MEAN
    #endif
#endif

int GetCPUCount()
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
}

#endif

#if defined(__APPLE__) || defined(__linux__)

#include <unistd.h>
#include <sys/stat.h>

int GetCPUCount()
{
   return sysconf(_SC_NPROCESSORS_ONLN);
}



#endif

struct ReadyCallback
{
    Bikeshed_ReadyCallback cb = {Ready};
    ReadyCallback()
    {
        m_Semaphore = nadir::CreateSema(malloc(nadir::GetSemaSize()), 0);
    }
    ~ReadyCallback()
    {
        nadir::DeleteSema(m_Semaphore);
        free(m_Semaphore);
    }
    static void Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
    {
        ReadyCallback* cb = (ReadyCallback*)ready_callback;
        nadir::PostSema(cb->m_Semaphore, ready_count);
    }
    static void Wait(ReadyCallback* cb)
    {
        nadir::WaitSema(cb->m_Semaphore);
    }
    nadir::HSema m_Semaphore;
};

struct ThreadWorker
{
    ThreadWorker()
        : stop(0)
        , shed(0)
        , semaphore(0)
        , thread(0)
    {
    }

    ~ThreadWorker()
    {
    }

    bool CreateThread(Bikeshed in_shed, nadir::HSema in_semaphore, int32_t volatile* in_stop)
    {
        shed               = in_shed;
        stop               = in_stop;
        semaphore          = in_semaphore;
        thread             = nadir::CreateThread(malloc(nadir::GetThreadSize()), ThreadWorker::Execute, 0, this);
        return thread != 0;
    }

    void JoinThread()
    {
        nadir::JoinThread(thread, nadir::TIMEOUT_INFINITE);
    }

    void DisposeThread()
    {
        nadir::DeleteThread(thread);
        free(thread);
    }

    static int32_t Execute(void* context)
    {
        ThreadWorker* _this = reinterpret_cast<ThreadWorker*>(context);

        while (*_this->stop == 0)
        {
            if (!Bikeshed_ExecuteOne(_this->shed, 0))
            {
                nadir::WaitSema(_this->semaphore);
            }
        }
        return 0;
    }

    int32_t volatile*   stop;
    Bikeshed            shed;
    nadir::HSema        semaphore;
    nadir::HThread      thread;
};

struct MeowHashAPI
{
    HashAPI m_HashAPI;
    MeowHashAPI()
        : m_HashAPI{
            BeginContext,
            Hash,
            EndContext
        }
    {
    }

    static HashAPI_HContext BeginContext(HashAPI* hash_api)
    {
        meow_state* state = (meow_state*)malloc(sizeof(meow_state));
        MeowBegin(state, MeowDefaultSeed);
        return (HashAPI_HContext)state;
    }
    static void Hash(HashAPI* hash_api, HashAPI_HContext context, uint32_t length, void* data)
    {
        meow_state* state = (meow_state*)context;
        MeowAbsorb(state, length, data);
    }
    static uint64_t EndContext(HashAPI* hash_api, HashAPI_HContext context)
    {
        meow_state* state = (meow_state*)context;
        uint64_t hash = MeowU64From(MeowEnd(state, 0), 0);
        free(state);
        return hash;
    }
};

struct TroveStorageAPI
{
    StorageAPI m_StorageAPI;
    TroveStorageAPI()
        : m_StorageAPI{
            OpenReadFile,
            GetSize,
            Read,
            CloseRead,
            OpenWriteFile,
            Write,
            SetSize,
            CloseWrite,
            CreateDir,
            RenameFile,
            ConcatPath,
            IsDir,
            IsFile,
            RemoveDir,
            RemoveFile,
            StartFind,
            FindNext,
            CloseFind,
            GetFileName,
            GetDirectoryName,
            GetEntrySize
        }
    {

    }
    static StorageAPI_HOpenFile OpenReadFile(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        StorageAPI_HOpenFile r = (StorageAPI_HOpenFile)Trove_OpenReadFile(tmp_path);
        free(tmp_path);
        return r;
    }
    static uint64_t GetSize(StorageAPI* , StorageAPI_HOpenFile f)
    {
        return Trove_GetFileSize((HTroveOpenReadFile)f);
    }
    static int Read(StorageAPI* , StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
    {
        return Trove_Read((HTroveOpenReadFile)f, offset,length, output);
    }
    static void CloseRead(StorageAPI* , StorageAPI_HOpenFile f)
    {
        Trove_CloseReadFile((HTroveOpenReadFile)f);
    }

    static StorageAPI_HOpenFile OpenWriteFile(StorageAPI* , const char* path, uint64_t initial_size)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        StorageAPI_HOpenFile r = (StorageAPI_HOpenFile)Trove_OpenWriteFile(tmp_path, initial_size);
        free(tmp_path);
        return r;
    }
    static int Write(StorageAPI* , StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
    {
        return Trove_Write((HTroveOpenWriteFile)f, offset,length, input);
    }

    static int SetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t length)
    {
        return Trove_SetFileSize((HTroveOpenWriteFile)f, length);
    }

    static void CloseWrite(StorageAPI* , StorageAPI_HOpenFile f)
    {
        Trove_CloseWriteFile((HTroveOpenWriteFile)f);
    }

    static int CreateDir(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int ok = Trove_CreateDirectory(tmp_path);
        free(tmp_path);
        return ok;
    }

    static int RenameFile(StorageAPI* , const char* source_path, const char* target_path)
    {
        char* tmp_source_path = strdup(source_path);
        Trove_DenormalizePath(tmp_source_path);
        char* tmp_target_path = strdup(target_path);
        Trove_DenormalizePath(tmp_target_path);
        int ok = Trove_MoveFile(tmp_source_path, tmp_target_path);
        free(tmp_target_path);
        free(tmp_source_path);
        return ok;
    }
    static char* ConcatPath(StorageAPI* , const char* root_path, const char* sub_path)
    {
        // TODO: Trove is inconsistent - it works on normalized paths!
        char* path = (char*)Trove_ConcatPath(root_path, sub_path);
        Trove_NormalizePath(path);
        return path;
    }

    static int IsDir(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int is_dir = Trove_IsDir(tmp_path);
        free(tmp_path);
        return is_dir;
    }

    static int IsFile(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int is_file = Trove_IsFile(tmp_path);
        free(tmp_path);
        return is_file;
    }

    static int RemoveDir(struct StorageAPI* storage_api, const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        #ifdef _WIN32
        int ok = ::RemoveDirectoryA(tmp_path) == TRUE;
        #else
        int ok = rmdir(path) == 0;
        #endif
        free(tmp_path);
        return ok;
    }

    static int RemoveFile(struct StorageAPI* storage_api, const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        #ifdef _WIN32
        int ok = ::DeleteFileA(tmp_path) == TRUE;
        #else
        int ok = unlink(path) == 0;
        #endif
        free(tmp_path);
        return ok;
    }

    static StorageAPI_HIterator StartFind(StorageAPI* , const char* path)
    {
        StorageAPI_HIterator iterator = (StorageAPI_HIterator)malloc(Trove_GetFSIteratorSize());
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int ok = Trove_StartFind((HTrove_FSIterator)iterator, tmp_path);
        free(tmp_path);
        if (!ok)
        {
            free(iterator);
            iterator = 0;
        }
        return iterator;
    }
    static int FindNext(StorageAPI* , StorageAPI_HIterator iterator)
    {
        return Trove_FindNext((HTrove_FSIterator)iterator);
    }
    static void CloseFind(StorageAPI* , StorageAPI_HIterator iterator)
    {
        Trove_CloseFind((HTrove_FSIterator)iterator);
        free(iterator);
    }
    static const char* GetFileName(StorageAPI* , StorageAPI_HIterator iterator)
    {
        return Trove_GetFileName((HTrove_FSIterator)iterator);
    }
    static const char* GetDirectoryName(StorageAPI* , StorageAPI_HIterator iterator)
    {
        return Trove_GetDirectoryName((HTrove_FSIterator)iterator);
    }

    static uint64_t GetEntrySize(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        return Trove_GetEntrySize((HTrove_FSIterator)iterator);
    }
};

struct InMemStorageAPI
{
    StorageAPI m_StorageAPI;
    struct PathEntry
    {
        char* m_FileName;
        TLongtail_Hash m_ParentHash;
        uint8_t* m_Content;
    };
    HashAPI* m_HashAPI;
    struct Lookup
    {
        TLongtail_Hash key;
        uint32_t value;
    };
    Lookup* m_PathHashToContent;
    PathEntry* m_PathEntries;

    InMemStorageAPI()
        : m_StorageAPI{
            OpenReadFile,
            GetSize,
            Read,
            CloseRead,
            OpenWriteFile,
            Write,
            SetSize,
            CloseWrite,
            CreateDir,
            RenameFile,
            ConcatPath,
            IsDir,
            IsFile,
            RemoveDir,
            RemoveFile,
            StartFind,
            FindNext,
            CloseFind,
            GetFileName,
            GetDirectoryName,
            GetEntrySize
            }
        , m_HashAPI(GetMeowHashAPI())
        , m_PathHashToContent(0)
        , m_PathEntries(0)
    {
    }

    ~InMemStorageAPI()
    {
        size_t c = arrlen(m_PathEntries);
        while(c--)
        {
            PathEntry* path_entry = &m_PathEntries[c];
            free(path_entry->m_FileName);
            path_entry->m_FileName = 0;
            arrfree(path_entry->m_Content);
            path_entry->m_Content = 0;
        }
        hmfree(m_PathHashToContent);
        m_PathHashToContent = 0;
        arrfree(m_PathEntries);
        m_PathEntries = 0;
    }

    static uint64_t GetPathHash(HashAPI* hash_api, const char* path)
    {
        HashAPI_HContext context = hash_api->BeginContext(hash_api);
        hash_api->Hash(hash_api, context, (uint32_t)strlen(path), (void*)path);
        return hash_api->EndContext(hash_api, context);
    }

    static StorageAPI_HOpenFile OpenReadFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
        if (it != -1)
        {
            return (StorageAPI_HOpenFile)&instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        }
        return 0;
    }
    static uint64_t GetSize(StorageAPI* storage_api, StorageAPI_HOpenFile f)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        PathEntry* path_entry = (PathEntry*)f;
        return arrlen(path_entry->m_Content);
    }
    static int Read(StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        PathEntry* path_entry = (PathEntry*)f;
        if ((ptrdiff_t)(offset + length) > arrlen(path_entry->m_Content))
        {
            return 0;
        }
        memcpy(output, &path_entry->m_Content[offset], length);
        return 1;
    }
    static void CloseRead(StorageAPI* , StorageAPI_HOpenFile)
    {
    }

    static TLongtail_Hash GetParentPathHash(InMemStorageAPI* instance, const char* path)
    {
        const char* dir_path_begin = strrchr(path, '/');
        if (!dir_path_begin)
        {
            return 0;
        }
        size_t dir_length = (uintptr_t)dir_path_begin - (uintptr_t)path;
        char* dir_path = (char*)malloc(dir_length + 1);
        strncpy(dir_path, path, dir_length);
        dir_path[dir_length] = '\0';
        TLongtail_Hash hash = GetPathHash(instance->m_HashAPI, dir_path);
        free(dir_path);
        return hash;
    }

    static const char* GetFileName(const char* path)
    {
        const char* file_name = strrchr(path, '/');
        if (file_name == 0)
        {
            return path;
        }
        return &file_name[1];
    }

    static StorageAPI_HOpenFile OpenWriteFile(StorageAPI* storage_api, const char* path, uint64_t initial_size)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
        {
            TEST_LOG("InMemStorageAPI_OpenWriteFile `%s` failed - parent folder does not exist\n", path)
            return 0;
        }
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        PathEntry* path_entry = 0;
        intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
        if (it != -1)
        {
            path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        }
        else
        {
            ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
            arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
            path_entry = &instance->m_PathEntries[entry_index];
            path_entry->m_ParentHash = parent_path_hash;
            path_entry->m_FileName = strdup(GetFileName(path));
            path_entry->m_Content = 0;
            hmput(instance->m_PathHashToContent, path_hash, entry_index);
        }
        arrsetcap(path_entry->m_Content, initial_size == 0 ? 16 : (uint32_t)initial_size);
        arrsetlen(path_entry->m_Content, (uint32_t)initial_size);
        return (StorageAPI_HOpenFile)path_hash;
    }
    static int Write(StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = (TLongtail_Hash)f;
        intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
        if (it == -1)
        {
            return 0;
        }
        PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        ptrdiff_t size = arrlen(path_entry->m_Content);
        if ((ptrdiff_t)offset > size)
        {
            return 0;
        }
        if ((ptrdiff_t)(offset + length) > size)
        {
            size = offset + length;
        }
        arrsetcap(path_entry->m_Content, size == 0 ? 16 : (uint32_t)size);
        arrsetlen(path_entry->m_Content, (uint32_t)size);
        memcpy(&(path_entry->m_Content)[offset], input, length);
        return 1;
    }

    static int SetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t length)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = (TLongtail_Hash)f;
        intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
        if (it == -1)
        {
            return 0;
        }
        PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        arrsetlen(path_entry->m_Content, (uint32_t)length);
        return 1;
    }

    static void CloseWrite(StorageAPI* , StorageAPI_HOpenFile)
    {
    }

    static int CreateDir(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
        {
            TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - parent folder does not exist\n", path)
            return 0;
        }
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
        if (source_path_ptr != -1)
        {
            PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
            if (source_entry->m_Content == 0)
            {
                return 1;
            }
            TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - path exists and is not a directory\n", path)
            return 0;
        }

        ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
        arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
        PathEntry* path_entry = &instance->m_PathEntries[entry_index];
        path_entry->m_ParentHash = parent_path_hash;
        path_entry->m_FileName = strdup(GetFileName(path));
        path_entry->m_Content = 0;
        hmput(instance->m_PathHashToContent, path_hash, entry_index);
        return 1;
    }

    static int RenameFile(StorageAPI* storage_api, const char* source_path, const char* target_path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash source_path_hash = GetPathHash(instance->m_HashAPI, source_path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
        if (source_path_ptr == -1)
        {
            TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - source path does not exist\n", source_path, target_path)
            return 0;
        }
        PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];

        TLongtail_Hash target_path_hash = GetPathHash(instance->m_HashAPI, target_path);
        intptr_t target_path_ptr = hmgeti(instance->m_PathHashToContent, target_path_hash);
        if (target_path_ptr != -1)
        {
            TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - target path already exist\n", source_path, target_path)
            return 0;
        }
        source_entry->m_ParentHash = GetParentPathHash(instance, target_path);
        free(source_entry->m_FileName);
        source_entry->m_FileName = strdup(GetFileName(target_path));
        hmput(instance->m_PathHashToContent, target_path_hash, instance->m_PathHashToContent[source_path_ptr].value);
        hmdel(instance->m_PathHashToContent, source_path_hash);
        return 1;
    }
    static char* ConcatPath(StorageAPI* , const char* root_path, const char* sub_path)
    {
        if (root_path[0] == 0)
        {
            return strdup(sub_path);
        }
        size_t path_len = strlen(root_path) + 1 + strlen(sub_path) + 1;
        char* path = (char*)malloc(path_len);
        strcpy(path, root_path);
        strcat(path, "/");
        strcat(path, sub_path);
        return path;
    }

    static int IsDir(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash source_path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
        if (source_path_ptr == -1)
        {
            return 0;
        }
        PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        return source_entry->m_Content == 0;
    }
    static int IsFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
        if (source_path_ptr == -1)
        {
            return 0;
        }
        PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        return source_entry->m_Content != 0;
    }

    static int RemoveDir(struct StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
        if (source_path_ptr == -1)
        {
            return 0;
        }
        PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        if (path_entry->m_Content)
        {
            // Not a directory
            return 0;
        }
        free(path_entry->m_FileName);
        path_entry->m_FileName = 0;
        arrfree(path_entry->m_Content);
        path_entry->m_Content = 0;
        path_entry->m_ParentHash = 0;
        hmdel(instance->m_PathHashToContent, path_hash);
        return 1;
    }

    static int RemoveFile(struct StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(instance->m_HashAPI, path);
        intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
        if (source_path_ptr == -1)
        {
            return 0;
        }
        PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        if (!path_entry->m_Content)
        {
            // Not a file
            return 0;
        }
        free(path_entry->m_FileName);
        path_entry->m_FileName = 0;
        arrfree(path_entry->m_Content);
        path_entry->m_Content = 0;
        path_entry->m_ParentHash = 0;
        hmdel(instance->m_PathHashToContent, path_hash);
        return 1;
    }

    static StorageAPI_HIterator StartFind(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = path[0] ? GetPathHash(instance->m_HashAPI, path) : 0;
        ptrdiff_t* i = (ptrdiff_t*)LONGTAIL_MALLOC(sizeof(ptrdiff_t));
        *i = 0;
        while (*i < arrlen(instance->m_PathEntries))
        {
            if (instance->m_PathEntries[*i].m_ParentHash == path_hash)
            {
                return (StorageAPI_HIterator)i;
            }
            *i += 1;
        }
        return (StorageAPI_HIterator)0;
    }
    static int FindNext(StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        ptrdiff_t* i = (ptrdiff_t*)iterator;
        TLongtail_Hash path_hash = instance->m_PathEntries[*i].m_ParentHash;
        *i += 1;
        while (*i < arrlen(instance->m_PathEntries))
        {
            if (instance->m_PathEntries[*i].m_ParentHash == path_hash)
            {
                return 1;
            }
            *i += 1;
        }
        return 0;
    }
    static void CloseFind(StorageAPI* , StorageAPI_HIterator iterator)
    {
        LONGTAIL_FREE((uint32_t*)iterator);
    }
    static const char* GetFileName(StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        ptrdiff_t* i = (ptrdiff_t*)iterator;
        if (instance->m_PathEntries[*i].m_Content == 0)
        {
            return 0;
        }
        return instance->m_PathEntries[*i].m_FileName;
    }
    static const char* GetDirectoryName(StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        uint32_t* i = (uint32_t*)iterator;
        if (instance->m_PathEntries[*i].m_Content != 0)
        {
            return 0;
        }
        return instance->m_PathEntries[*i].m_FileName;
    }

    static uint64_t GetEntrySize(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        uint32_t* i = (uint32_t*)iterator;
        if (instance->m_PathEntries[*i].m_Content == 0)
        {
            return 0;
        }
        return arrlen(instance->m_PathEntries[*i].m_Content);
    }
};



struct LizardCompressionAPI
{
    CompressionAPI m_CompressionAPI;

    LizardCompressionAPI()
        : m_CompressionAPI{
            GetDefaultSettings,
            GetMaxCompressionSetting,
            CreateCompressionContext,
            GetMaxCompressedSize,
            Compress,
            DeleteCompressionContext,
            CreateDecompressionContext,
            Decompress,
            DeleteDecompressionContext
            }
    {
    }

    static int DefaultCompressionSetting;
    static int MaxCompressionSetting;
    static CompressionAPI_HSettings GetDefaultSettings(CompressionAPI*)
    {
        return (CompressionAPI_HSettings)&DefaultCompressionSetting;
    }
    static CompressionAPI_HSettings GetMaxCompressionSetting(CompressionAPI*)
    {
        return (CompressionAPI_HSettings)&MaxCompressionSetting;
    }
    static CompressionAPI_HCompressionContext CreateCompressionContext(CompressionAPI*, CompressionAPI_HSettings settings)
    {
        return (CompressionAPI_HCompressionContext)settings;
    }
    static size_t GetMaxCompressedSize(CompressionAPI*, CompressionAPI_HCompressionContext , size_t size)
    {
        return (size_t)Lizard_compressBound((int)size);
    }
    static size_t Compress(CompressionAPI*, CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size)
    {
        int compression_setting = *(int*)context;
        int compressed_size = Lizard_compress(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
        return (size_t)(compressed_size >= 0 ? compressed_size : 0);
    }
    static void DeleteCompressionContext(CompressionAPI*, CompressionAPI_HCompressionContext)
    {
    }
    static CompressionAPI_HDecompressionContext CreateDecompressionContext(CompressionAPI* compression_api)
    {
        return (CompressionAPI_HDecompressionContext)GetDefaultSettings(compression_api);
    }
    static size_t Decompress(CompressionAPI*, CompressionAPI_HDecompressionContext, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size)
    {
        int result = Lizard_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)uncompressed_size);
        return (size_t)(result >= 0 ? result : 0);
    }
    static void DeleteDecompressionContext(CompressionAPI*, CompressionAPI_HDecompressionContext)
    {
    }
};

int LizardCompressionAPI::DefaultCompressionSetting = 44;
int LizardCompressionAPI::MaxCompressionSetting = LIZARD_MAX_CLEVEL;

struct BikeshedJobAPI
{
    JobAPI m_JobAPI;

    struct JobWrapper
    {
        BikeshedJobAPI* m_JobAPI;
        JobAPI_JobFunc m_JobFunc;
        void* m_Context;
    };

    ReadyCallback m_ReadyCallback;
    Bikeshed m_Shed;
    uint32_t m_WorkerCount;
    ThreadWorker* m_Workers;
    int32_t volatile m_Stop;
    JobWrapper* m_ReservedJobs;
    Bikeshed_TaskID* m_ReservedTasksIDs;
    uint32_t m_ReservedJobCount;
    int32_t volatile m_SubmittedJobCount;
    int32_t volatile m_PendingJobCount;
    int32_t volatile m_JobsCompleted;

    explicit BikeshedJobAPI(uint32_t worker_count)
        : m_JobAPI{
            GetWorkerCount,
            ReserveJobs,
            CreateJobs,
            AddDependecies,
            ReadyJobs,
            WaitForAllJobs
            }
        , m_Shed(0)
        , m_WorkerCount(worker_count)//
        , m_Workers(0)
        , m_Stop(0)
        , m_ReservedJobs(0)
        , m_ReservedTasksIDs(0)
        , m_ReservedJobCount(0)
        , m_SubmittedJobCount(0)
        , m_PendingJobCount(0)
        , m_JobsCompleted(0)
    {
        m_Shed = Bikeshed_Create(malloc(BIKESHED_SIZE(1048576, 7340032, 1)), 1048576, 7340032, 1, &m_ReadyCallback.cb);
        m_Workers = new ThreadWorker[m_WorkerCount];
        for (uint32_t i = 0; i < m_WorkerCount; ++i)
        {
            m_Workers[i].CreateThread(m_Shed, m_ReadyCallback.m_Semaphore, &m_Stop);
        }
    }
    ~BikeshedJobAPI()
    {
        nadir::AtomicAdd32(&m_Stop, 1);
        ReadyCallback::Ready(&m_ReadyCallback.cb, 0, m_WorkerCount);
        for (uint32_t i = 0; i < m_WorkerCount; ++i)
        {
            m_Workers[i].JoinThread();
        }
        for (uint32_t i = 0; i < m_WorkerCount; ++i)
        {
            m_Workers[i].DisposeThread();
        }
        delete []m_Workers;
        free(m_Shed);
    }

    static int GetWorkerCount(struct JobAPI* job_api)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        return bikeshed_job_api->m_WorkerCount;
    }

    static int ReserveJobs(JobAPI* job_api, uint32_t job_count)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        if (bikeshed_job_api->m_PendingJobCount)
        {
            return 0;
        }
        if (bikeshed_job_api->m_SubmittedJobCount)
        {
            return 0;
        }
        if (bikeshed_job_api->m_ReservedJobs)
        {
            return 0;
        }
        if (bikeshed_job_api->m_ReservedJobCount)
        {
            return 0;
        }
        bikeshed_job_api->m_ReservedJobs = (JobWrapper*)malloc(sizeof(JobWrapper) * job_count);
        bikeshed_job_api->m_ReservedTasksIDs = (Bikeshed_TaskID*)malloc(sizeof(Bikeshed_TaskID) * job_count);
        if (bikeshed_job_api->m_ReservedJobs && bikeshed_job_api->m_ReservedTasksIDs)
        {
            bikeshed_job_api->m_ReservedJobCount = job_count;
            return 1;
        }
        free(bikeshed_job_api->m_ReservedTasksIDs);
        free(bikeshed_job_api->m_ReservedJobs);
        return 0;
    }

    static JobAPI_Jobs CreateJobs(JobAPI* job_api, uint32_t job_count, JobAPI_JobFunc job_funcs[], void* job_contexts[])
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        int32_t new_job_count = PLATFORM_ATOMICADD_PRIVATE(&bikeshed_job_api->m_SubmittedJobCount, (int32_t)job_count);
        if (new_job_count > (int32_t)bikeshed_job_api->m_ReservedJobCount)
        {
            PLATFORM_ATOMICADD_PRIVATE(&bikeshed_job_api->m_SubmittedJobCount, -((int32_t)job_count));
            return 0;
        }
        int32_t job_range_start = new_job_count - job_count;

        BikeShed_TaskFunc* func = (BikeShed_TaskFunc*)malloc(sizeof(BikeShed_TaskFunc) * job_count);
        void** ctx = (void**)malloc(sizeof(void*) * job_count);
        Bikeshed_TaskID* task_ids = &bikeshed_job_api->m_ReservedTasksIDs[job_range_start];
        for (uint32_t i = 0; i < job_count; ++i)
        {
            JobWrapper* job_wrapper = &bikeshed_job_api->m_ReservedJobs[job_range_start + i];
            job_wrapper->m_JobAPI = bikeshed_job_api;
            job_wrapper->m_Context = job_contexts[i];
            job_wrapper->m_JobFunc = job_funcs[i];
            func[i] = Job;
            ctx[i] = job_wrapper;
        }

        while (!Bikeshed_CreateTasks(bikeshed_job_api->m_Shed, job_count, func, ctx, task_ids))
        {
            Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
        }

        PLATFORM_ATOMICADD_PRIVATE(&bikeshed_job_api->m_PendingJobCount, job_count);

        free(ctx);
        free(func);
        return task_ids;
    }

    static void AddDependecies(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs, uint32_t dependency_job_count, JobAPI_Jobs dependency_jobs)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        while (!Bikeshed_AddDependencies(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs, dependency_job_count, (Bikeshed_TaskID*)dependency_jobs))
        {
            Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
        }
    }

    static void ReadyJobs(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs);
    }

    static void WaitForAllJobs(JobAPI* job_api, void* context, JobAPI_ProgressFunc process_func)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        int32_t old_pending_count = 0;
        while (bikeshed_job_api->m_PendingJobCount > 0)
        {
            if (process_func)
            {
                process_func(context, bikeshed_job_api->m_ReservedJobCount, bikeshed_job_api->m_JobsCompleted);
            }
            if (Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0))
            {
                continue;
            }
            if (old_pending_count != bikeshed_job_api->m_PendingJobCount)
            {
                old_pending_count = bikeshed_job_api->m_PendingJobCount;
            }
            PLATFORM_SLEEP_PRIVATE(1000);
        }
        if (process_func)
        {
            process_func(context, bikeshed_job_api->m_SubmittedJobCount, bikeshed_job_api->m_SubmittedJobCount);
        }
        bikeshed_job_api->m_SubmittedJobCount = 0;
        free(bikeshed_job_api->m_ReservedTasksIDs);
        bikeshed_job_api->m_ReservedTasksIDs = 0;
        free(bikeshed_job_api->m_ReservedJobs);
        bikeshed_job_api->m_ReservedJobs = 0;
        bikeshed_job_api->m_JobsCompleted = 0;
        bikeshed_job_api->m_ReservedJobCount = 0;
    }

    static Bikeshed_TaskResult Job(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
    {
        JobWrapper* wrapper = (JobWrapper*)context;
        wrapper->m_JobFunc(wrapper->m_Context);
        if (wrapper->m_JobAPI->m_PendingJobCount <= 0)
        {
            // TODO! Error handling!
            return BIKESHED_TASK_RESULT_COMPLETE;
        }
        PLATFORM_ATOMICADD_PRIVATE(&wrapper->m_JobAPI->m_PendingJobCount, -1);
        PLATFORM_ATOMICADD_PRIVATE(&wrapper->m_JobAPI->m_JobsCompleted, 1);
        return BIKESHED_TASK_RESULT_COMPLETE;
    }
};

#undef PLATFORM_ATOMICADD_PRIVATE
#undef PLATFORM_SLEEP_PRIVATE



HashAPI* GetMeowHashAPI()
{
    static MeowHashAPI meow;
    return &meow.m_HashAPI;
}

StorageAPI* GetFSStorageAPI()
{
    static TroveStorageAPI trove;
    return &trove.m_StorageAPI;
}

struct StorageAPI* CreateInMemStorageAPI()
{
    InMemStorageAPI* mem_storage = new InMemStorageAPI;
    return &mem_storage->m_StorageAPI;
}

void DestroyInMemStorageAPI(struct StorageAPI* storage_api)
{
    InMemStorageAPI* mem_storage = (InMemStorageAPI*)storage_api;
    delete mem_storage;
}

CompressionAPI* GetLizardCompressionAPI()
{
    static LizardCompressionAPI lizard;
    return &lizard.m_CompressionAPI;
}

JobAPI* GetBikeshedJobAPI(uint32_t worker_count)
{
    static BikeshedJobAPI bikeshed(worker_count);
    return &bikeshed.m_JobAPI;
}

struct CompressionRegistry* GetCompressionRegistry()
{
    static const CompressionAPI* compression_apis[] = {
        GetLizardCompressionAPI()};
    static const uint32_t compression_types[] = {
        LIZARD_DEFAULT_COMPRESSION_TYPE};
    static const CompressionAPI_HSettings compression_settings[] = {
        GetLizardCompressionAPI()->GetDefaultSettings(GetLizardCompressionAPI())};
    struct CompressionRegistry* compression_registry = CreateCompressionRegistry(
        1,
        &compression_types[0],
        &compression_apis[0],
        &compression_settings[0]);
    return compression_registry;
}
