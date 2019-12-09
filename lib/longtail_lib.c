#include "longtail_lib.h"

#define BIKESHED_IMPLEMENTATION
#define STB_DS_IMPLEMENTATION
#include "../src/longtail.h"
#include "../src/stb_ds.h"

#include "longtail_platform.h"

#include "../third-party/bikeshed/bikeshed.h"
#include "../third-party/lizard/lib/lizard_common.h"
#include "../third-party/lizard/lib/lizard_decompress.h"
#include "../third-party/lizard/lib/lizard_compress.h"
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"

#include <stdio.h>

#define TEST_LOG(fmt, ...) \
    fprintf(stderr, "--- ");fprintf(stderr, fmt, __VA_ARGS__);

int GetCPUCount()
{
    return (int)Longtail_GetCPUCount();
}

struct ReadyCallback
{
    struct Bikeshed_ReadyCallback cb;
    HLongtail_Sema m_Semaphore;
};

static void ReadyCallback_Dispose(struct ReadyCallback* ready_callback)
{
    Longtail_DeleteSema(ready_callback->m_Semaphore);
	Longtail_Free(ready_callback->m_Semaphore);
}

static void ReadyCallback_Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
{
    struct ReadyCallback* cb = (struct ReadyCallback*)ready_callback;
    Longtail_PostSema(cb->m_Semaphore, ready_count);
}

static void ReadyCallback_Wait(struct ReadyCallback* cb)
{
    Longtail_WaitSema(cb->m_Semaphore);
}

static void ReadyCallback_Init(struct ReadyCallback* ready_callback)
{
    ready_callback->cb.SignalReady = ReadyCallback_Ready;
    ready_callback->m_Semaphore = Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0);
}


struct ThreadWorker
{
    int32_t volatile*   stop;
    Bikeshed            shed;
    HLongtail_Sema        semaphore;
    HLongtail_Thread      thread;
};

static void ThreadWorker_Init(struct ThreadWorker* thread_worker)
{
    thread_worker->stop = 0;
    thread_worker->shed = 0;
    thread_worker->semaphore = 0;
    thread_worker->thread = 0;
}

static void ThreadWorker_Dispose(struct ThreadWorker* thread_worker)
{
}

static int32_t ThreadWorker_Execute(void* context)
{
    struct ThreadWorker* thread_worker = (struct ThreadWorker*)(context);

    while (*thread_worker->stop == 0)
    {
        if (!Bikeshed_ExecuteOne(thread_worker->shed, 0))
        {
            Longtail_WaitSema(thread_worker->semaphore);
        }
    }
    return 0;
}

int ThreadWorker_CreateThread(struct ThreadWorker* thread_worker, Bikeshed in_shed, HLongtail_Sema in_semaphore, int32_t volatile* in_stop)
{
    thread_worker->shed               = in_shed;
    thread_worker->stop               = in_stop;
    thread_worker->semaphore          = in_semaphore;
    thread_worker->thread             = Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), ThreadWorker_Execute, 0, thread_worker);
    return thread_worker->thread != 0;
}

void ThreadWorker_JoinThread(struct ThreadWorker* thread_worker)
{
    Longtail_JoinThread(thread_worker->thread, LONGTAIL_TIMEOUT_INFINITE);
}

void ThreadWorker_DisposeThread(struct ThreadWorker* thread_worker)
{
    Longtail_DeleteThread(thread_worker->thread);
	Longtail_Free(thread_worker->thread);
}

struct ManagedHashAPI
{
    struct HashAPI m_API;
    void (*Dispose)(struct ManagedHashAPI* managed_hash_api);
};

struct MeowHashAPI
{
    struct ManagedHashAPI m_ManagedAPI;
};

static HashAPI_HContext MeowHash_BeginContext(struct HashAPI* hash_api)
{
    meow_state* state = (meow_state*)Longtail_Alloc(sizeof(meow_state));
    MeowBegin(state, MeowDefaultSeed);
    return (HashAPI_HContext)state;
}

static void MeowHash_Hash(struct HashAPI* hash_api, HashAPI_HContext context, uint32_t length, void* data)
{
    meow_state* state = (meow_state*)context;
    MeowAbsorb(state, length, data);
}

static uint64_t MeowHash_EndContext(struct HashAPI* hash_api, HashAPI_HContext context)
{
    meow_state* state = (meow_state*)context;
    uint64_t hash = MeowU64From(MeowEnd(state, 0), 0);
	Longtail_Free(state);
    return hash;
}

static void MeowHash_Dispose(struct ManagedHashAPI* hash_api)
{
}

static void MeowHash_Init(struct MeowHashAPI* hash_api)
{
    hash_api->m_ManagedAPI.m_API.BeginContext = MeowHash_BeginContext;
    hash_api->m_ManagedAPI.m_API.Hash = MeowHash_Hash;
    hash_api->m_ManagedAPI.m_API.EndContext = MeowHash_EndContext;
    hash_api->m_ManagedAPI.Dispose = MeowHash_Dispose;
}

struct HashAPI* CreateMeowHashAPI()
{
    struct MeowHashAPI* meow_hash = (struct MeowHashAPI*)Longtail_Alloc(sizeof(struct MeowHashAPI));
    MeowHash_Init(meow_hash);
    return &meow_hash->m_ManagedAPI.m_API;
}

void DestroyHashAPI(struct HashAPI* hash_api)
{
    struct ManagedHashAPI* managed = (struct ManagedHashAPI*)hash_api;
    managed->Dispose(managed);
    Longtail_Free(hash_api);
}










struct ManagedStorageAPI
{
    struct StorageAPI m_API;
    void (*Dispose)(struct ManagedStorageAPI* managed_storage_api);
};

struct FSStorageAPI
{
    struct ManagedStorageAPI m_StorageAPI;
};

static void FSStorageAPI_Dispose(struct ManagedStorageAPI* storage_api)
{
}

static StorageAPI_HOpenFile FSStorageAPI_OpenReadFile(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    StorageAPI_HOpenFile r = (StorageAPI_HOpenFile)Longtail_OpenReadFile(tmp_path);
    Longtail_Free(tmp_path);
    return r;
}

static uint64_t FSStorageAPI_GetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
    return Longtail_GetFileSize((HLongtail_OpenReadFile)f);
}

static int FSStorageAPI_Read(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
{
    return Longtail_Read((HLongtail_OpenReadFile)f, offset,length, output);
}

static void FSStorageAPI_CloseRead(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
    Longtail_CloseReadFile((HLongtail_OpenReadFile)f);
}

static StorageAPI_HOpenFile FSStorageAPI_OpenWriteFile(struct StorageAPI* storage_api, const char* path, uint64_t initial_size)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    StorageAPI_HOpenFile r = (StorageAPI_HOpenFile)Longtail_OpenWriteFile(tmp_path, initial_size);
    Longtail_Free(tmp_path);
    return r;
}

static int FSStorageAPI_Write(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
{
    return Longtail_Write((HLongtail_OpenWriteFile)f, offset,length, input);
}

static int FSStorageAPI_SetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t length)
{
    return Longtail_SetFileSize((HLongtail_OpenWriteFile)f, length);
}

static void FSStorageAPI_CloseWrite(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
    Longtail_CloseWriteFile((HLongtail_OpenWriteFile)f);
}

static int FSStorageAPI_CreateDir(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int ok = Longtail_CreateDirectory(tmp_path);
    Longtail_Free(tmp_path);
    return ok;
}

static int FSStorageAPI_RenameFile(struct StorageAPI* storage_api, const char* source_path, const char* target_path)
{
    char* tmp_source_path = Longtail_Strdup(source_path);
    Longtail_DenormalizePath(tmp_source_path);
    char* tmp_target_path = Longtail_Strdup(target_path);
    Longtail_DenormalizePath(tmp_target_path);
    int ok = Longtail_MoveFile(tmp_source_path, tmp_target_path);
    Longtail_Free(tmp_target_path);
    Longtail_Free(tmp_source_path);
    return ok;
}

static char* FSStorageAPI_ConcatPath(struct StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
    // TODO: Trove is inconsistent - it works on normalized paths!
    char* path = (char*)Longtail_ConcatPath(root_path, sub_path);
    Longtail_NormalizePath(path);
    return path;
}

static int FSStorageAPI_IsDir(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int is_dir = Longtail_IsDir(tmp_path);
    Longtail_Free(tmp_path);
    return is_dir;
}

static int FSStorageAPI_IsFile(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int is_file = Longtail_IsFile(tmp_path);
    Longtail_Free(tmp_path);
    return is_file;
}

static int FSStorageAPI_RemoveDir(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int ok = Longtail_RemoveDir(tmp_path);
    return ok;
}

static int FSStorageAPI_RemoveFile(struct StorageAPI* storage_api, const char* path)
{
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int ok = Longtail_RemoveFile(tmp_path);
    Longtail_Free(tmp_path);
    return ok;
}

static StorageAPI_HIterator FSStorageAPI_StartFind(struct StorageAPI* storage_api, const char* path)
{
    StorageAPI_HIterator iterator = (StorageAPI_HIterator)Longtail_Alloc(Longtail_GetFSIteratorSize());
    char* tmp_path = Longtail_Strdup(path);
    Longtail_DenormalizePath(tmp_path);
    int ok = Longtail_StartFind((HLongtail_FSIterator)iterator, tmp_path);
    Longtail_Free(tmp_path);
    if (!ok)
    {
		Longtail_Free(iterator);
        iterator = 0;
    }
    return iterator;
}

static int FSStorageAPI_FindNext(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    return Longtail_FindNext((HLongtail_FSIterator)iterator);
}

static void FSStorageAPI_CloseFind(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    Longtail_CloseFind((HLongtail_FSIterator)iterator);
	Longtail_Free(iterator);
}

static const char* FSStorageAPI_GetFileName(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    return Longtail_GetFileName((HLongtail_FSIterator)iterator);
}

static const char* FSStorageAPI_GetDirectoryName(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    return Longtail_GetDirectoryName((HLongtail_FSIterator)iterator);
}

static uint64_t FSStorageAPI_GetEntrySize(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    return Longtail_GetEntrySize((HLongtail_FSIterator)iterator);
}

static void FSStorageAPI_Init(struct FSStorageAPI* storage_api)
{
    storage_api->m_StorageAPI.m_API.OpenReadFile = FSStorageAPI_OpenReadFile;
    storage_api->m_StorageAPI.m_API.GetSize = FSStorageAPI_GetSize;
    storage_api->m_StorageAPI.m_API.Read = FSStorageAPI_Read;
    storage_api->m_StorageAPI.m_API.CloseRead = FSStorageAPI_CloseRead;
    storage_api->m_StorageAPI.m_API.OpenWriteFile = FSStorageAPI_OpenWriteFile;
    storage_api->m_StorageAPI.m_API.Write = FSStorageAPI_Write;
    storage_api->m_StorageAPI.m_API.SetSize = FSStorageAPI_SetSize;
    storage_api->m_StorageAPI.m_API.CloseWrite = FSStorageAPI_CloseWrite;
    storage_api->m_StorageAPI.m_API.CreateDir = FSStorageAPI_CreateDir;
    storage_api->m_StorageAPI.m_API.RenameFile = FSStorageAPI_RenameFile;
    storage_api->m_StorageAPI.m_API.ConcatPath = FSStorageAPI_ConcatPath;
    storage_api->m_StorageAPI.m_API.IsDir = FSStorageAPI_IsDir;
    storage_api->m_StorageAPI.m_API.IsFile = FSStorageAPI_IsFile;
    storage_api->m_StorageAPI.m_API.RemoveDir = FSStorageAPI_RemoveDir;
    storage_api->m_StorageAPI.m_API.RemoveFile = FSStorageAPI_RemoveFile;
    storage_api->m_StorageAPI.m_API.StartFind = FSStorageAPI_StartFind;
    storage_api->m_StorageAPI.m_API.FindNext = FSStorageAPI_FindNext;
    storage_api->m_StorageAPI.m_API.CloseFind = FSStorageAPI_CloseFind;
    storage_api->m_StorageAPI.m_API.GetFileName = FSStorageAPI_GetFileName;
    storage_api->m_StorageAPI.m_API.GetDirectoryName = FSStorageAPI_GetDirectoryName;
    storage_api->m_StorageAPI.m_API.GetEntrySize = FSStorageAPI_GetEntrySize;
    storage_api->m_StorageAPI.Dispose = FSStorageAPI_Dispose;
}


struct StorageAPI* CreateFSStorageAPI()
{
    struct FSStorageAPI* storage_api = (struct FSStorageAPI*)Longtail_Alloc(sizeof(struct FSStorageAPI));
    FSStorageAPI_Init(storage_api);
    return &storage_api->m_StorageAPI.m_API;
}

struct PathEntry
{
    char* m_FileName;
    TLongtail_Hash m_ParentHash;
    uint8_t* m_Content;
};

struct Lookup
{
    TLongtail_Hash key;
    uint32_t value;
};

struct InMemStorageAPI
{
    struct ManagedStorageAPI m_StorageAPI;
    struct HashAPI* m_HashAPI;
    struct Lookup* m_PathHashToContent;
    struct PathEntry* m_PathEntries;
};

static void InMemStorageAPI_Dispose(struct ManagedStorageAPI* storage_api)
{
    struct InMemStorageAPI* in_mem_storage_api = (struct InMemStorageAPI*)storage_api;
    size_t c = arrlen(in_mem_storage_api->m_PathEntries);
    while(c--)
    {
        struct PathEntry* path_entry = &in_mem_storage_api->m_PathEntries[c];
        Longtail_Free(path_entry->m_FileName);
        path_entry->m_FileName = 0;
        arrfree(path_entry->m_Content);
        path_entry->m_Content = 0;
    }
    hmfree(in_mem_storage_api->m_PathHashToContent);
    in_mem_storage_api->m_PathHashToContent = 0;
    arrfree(in_mem_storage_api->m_PathEntries);
    in_mem_storage_api->m_PathEntries = 0;
    DestroyHashAPI(in_mem_storage_api->m_HashAPI);
}

static uint64_t InMemStorageAPI_GetPathHash(struct HashAPI* hash_api, const char* path)
{
    HashAPI_HContext context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, context, (uint32_t)strlen(path), (void*)path);
    return hash_api->EndContext(hash_api, context);
}

static StorageAPI_HOpenFile InMemStorageAPI_OpenReadFile(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it != -1)
    {
        return (StorageAPI_HOpenFile)&instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    }
    return 0;
}

static uint64_t InMemStorageAPI_GetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    struct PathEntry* path_entry = (struct PathEntry*)f;
    return arrlen(path_entry->m_Content);
}

static int InMemStorageAPI_Read(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    struct PathEntry* path_entry = (struct PathEntry*)f;
    if ((ptrdiff_t)(offset + length) > arrlen(path_entry->m_Content))
    {
        return 0;
    }
    memcpy(output, &path_entry->m_Content[offset], length);
    return 1;
}

static void InMemStorageAPI_CloseRead(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
}

static TLongtail_Hash InMemStorageAPI_GetParentPathHash(struct InMemStorageAPI* instance, const char* path)
{
    const char* dir_path_begin = strrchr(path, '/');
    if (!dir_path_begin)
    {
        return 0;
    }
    size_t dir_length = (uintptr_t)dir_path_begin - (uintptr_t)path;
    char* dir_path = (char*)Longtail_Alloc(dir_length + 1);
    strncpy(dir_path, path, dir_length);
    dir_path[dir_length] = '\0';
    TLongtail_Hash hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, dir_path);
    Longtail_Free(dir_path);
    return hash;
}

static const char* InMemStorageAPI_GetFileNamePart(const char* path)
{
    const char* file_name = strrchr(path, '/');
    if (file_name == 0)
    {
        return path;
    }
    return &file_name[1];
}

static StorageAPI_HOpenFile InMemStorageAPI_OpenWriteFile(struct StorageAPI* storage_api, const char* path, uint64_t initial_size)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash parent_path_hash = InMemStorageAPI_GetParentPathHash(instance, path);
    if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
    {
        TEST_LOG("InMemStorageAPI_OpenWriteFile `%s` failed - parent folder does not exist\n", path)
        return 0;
    }
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    struct PathEntry* path_entry = 0;
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
        path_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(path));
        path_entry->m_Content = 0;
        hmput(instance->m_PathHashToContent, path_hash, entry_index);
    }
    arrsetcap(path_entry->m_Content, initial_size == 0 ? 16 : (uint32_t)initial_size);
    arrsetlen(path_entry->m_Content, (uint32_t)initial_size);
    return (StorageAPI_HOpenFile)path_hash;
}

static int InMemStorageAPI_Write(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = (TLongtail_Hash)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
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

static int InMemStorageAPI_SetSize(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t length)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = (TLongtail_Hash)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    arrsetlen(path_entry->m_Content, (uint32_t)length);
    return 1;
}

static void InMemStorageAPI_CloseWrite(struct StorageAPI* storage_api, StorageAPI_HOpenFile f)
{
}

static int InMemStorageAPI_CreateDir(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash parent_path_hash = InMemStorageAPI_GetParentPathHash(instance, path);
    if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
    {
        TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - parent folder does not exist\n", path)
        return 0;
    }
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr != -1)
    {
        struct PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        if (source_entry->m_Content == 0)
        {
            return 1;
        }
        TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - path exists and is not a directory\n", path)
        return 0;
    }

    ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
    arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
    struct PathEntry* path_entry = &instance->m_PathEntries[entry_index];
    path_entry->m_ParentHash = parent_path_hash;
    path_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(path));
    path_entry->m_Content = 0;
    hmput(instance->m_PathHashToContent, path_hash, entry_index);
    return 1;
}

static int InMemStorageAPI_RenameFile(struct StorageAPI* storage_api, const char* source_path, const char* target_path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash source_path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, source_path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
    if (source_path_ptr == -1)
    {
        TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - source path does not exist\n", source_path, target_path)
        return 0;
    }
    struct PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];

    TLongtail_Hash target_path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, target_path);
    intptr_t target_path_ptr = hmgeti(instance->m_PathHashToContent, target_path_hash);
    if (target_path_ptr != -1)
    {
        TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - target path already exist\n", source_path, target_path)
        return 0;
    }
    source_entry->m_ParentHash = InMemStorageAPI_GetParentPathHash(instance, target_path);
    Longtail_Free(source_entry->m_FileName);
    source_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(target_path));
    hmput(instance->m_PathHashToContent, target_path_hash, instance->m_PathHashToContent[source_path_ptr].value);
    hmdel(instance->m_PathHashToContent, source_path_hash);
    return 1;
}

static char* InMemStorageAPI_ConcatPath(struct StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
    if (root_path[0] == 0)
    {
        return Longtail_Strdup(sub_path);
    }
    size_t path_len = strlen(root_path) + 1 + strlen(sub_path) + 1;
    char* path = (char*)Longtail_Alloc(path_len);
    strcpy(path, root_path);
    strcat(path, "/");
    strcat(path, sub_path);
    return path;
}

static int InMemStorageAPI_IsDir(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash source_path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
    if (source_path_ptr == -1)
    {
        return 0;
    }
    struct PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    return source_entry->m_Content == 0;
}
static int InMemStorageAPI_IsFile(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        return 0;
    }
    struct PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    return source_entry->m_Content != 0;
}

static int InMemStorageAPI_RemoveDir(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    if (path_entry->m_Content)
    {
        // Not a directory
        return 0;
    }
    Longtail_Free(path_entry->m_FileName);
    path_entry->m_FileName = 0;
    arrfree(path_entry->m_Content);
    path_entry->m_Content = 0;
    path_entry->m_ParentHash = 0;
    hmdel(instance->m_PathHashToContent, path_hash);
    return 1;
}

static int InMemStorageAPI_RemoveFile(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = InMemStorageAPI_GetPathHash(instance->m_HashAPI, path);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    if (!path_entry->m_Content)
    {
        // Not a file
        return 0;
    }
    Longtail_Free(path_entry->m_FileName);
    path_entry->m_FileName = 0;
    arrfree(path_entry->m_Content);
    path_entry->m_Content = 0;
    path_entry->m_ParentHash = 0;
    hmdel(instance->m_PathHashToContent, path_hash);
    return 1;
}

static StorageAPI_HIterator InMemStorageAPI_StartFind(struct StorageAPI* storage_api, const char* path)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    TLongtail_Hash path_hash = path[0] ? InMemStorageAPI_GetPathHash(instance->m_HashAPI, path) : 0;
    ptrdiff_t* i = (ptrdiff_t*)Longtail_Alloc(sizeof(ptrdiff_t));
    *i = 0;
    while (*i < arrlen(instance->m_PathEntries))
    {
        if (instance->m_PathEntries[*i].m_ParentHash == path_hash)
        {
            return (StorageAPI_HIterator)i;
        }
        *i += 1;
    }
    Longtail_Free(i);
    return (StorageAPI_HIterator)0;
}

static int InMemStorageAPI_FindNext(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
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
static void InMemStorageAPI_CloseFind(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    ptrdiff_t* i = (ptrdiff_t*)iterator;
    Longtail_Free(i);
}

static const char* InMemStorageAPI_GetFileName(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    ptrdiff_t* i = (ptrdiff_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content == 0)
    {
        return 0;
    }
    return instance->m_PathEntries[*i].m_FileName;
}

static const char* InMemStorageAPI_GetDirectoryName(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t* i = (uint32_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content != 0)
    {
        return 0;
    }
    return instance->m_PathEntries[*i].m_FileName;
}

static uint64_t InMemStorageAPI_GetEntrySize(struct StorageAPI* storage_api, StorageAPI_HIterator iterator)
{
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t* i = (uint32_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content == 0)
    {
        return 0;
    }
    return arrlen(instance->m_PathEntries[*i].m_Content);
}

static void InMemStorageAPI_Init(struct InMemStorageAPI* storage_api)
{
    storage_api->m_StorageAPI.m_API.OpenReadFile = InMemStorageAPI_OpenReadFile;
    storage_api->m_StorageAPI.m_API.GetSize = InMemStorageAPI_GetSize;
    storage_api->m_StorageAPI.m_API.Read = InMemStorageAPI_Read;
    storage_api->m_StorageAPI.m_API.CloseRead = InMemStorageAPI_CloseRead;
    storage_api->m_StorageAPI.m_API.OpenWriteFile = InMemStorageAPI_OpenWriteFile;
    storage_api->m_StorageAPI.m_API.Write = InMemStorageAPI_Write;
    storage_api->m_StorageAPI.m_API.SetSize = InMemStorageAPI_SetSize;
    storage_api->m_StorageAPI.m_API.CloseWrite = InMemStorageAPI_CloseWrite;
    storage_api->m_StorageAPI.m_API.CreateDir = InMemStorageAPI_CreateDir;
    storage_api->m_StorageAPI.m_API.RenameFile = InMemStorageAPI_RenameFile;
    storage_api->m_StorageAPI.m_API.ConcatPath = InMemStorageAPI_ConcatPath;
    storage_api->m_StorageAPI.m_API.IsDir = InMemStorageAPI_IsDir;
    storage_api->m_StorageAPI.m_API.IsFile = InMemStorageAPI_IsFile;
    storage_api->m_StorageAPI.m_API.RemoveDir = InMemStorageAPI_RemoveDir;
    storage_api->m_StorageAPI.m_API.RemoveFile = InMemStorageAPI_RemoveFile;
    storage_api->m_StorageAPI.m_API.StartFind = InMemStorageAPI_StartFind;
    storage_api->m_StorageAPI.m_API.FindNext = InMemStorageAPI_FindNext;
    storage_api->m_StorageAPI.m_API.CloseFind = InMemStorageAPI_CloseFind;
    storage_api->m_StorageAPI.m_API.GetFileName = InMemStorageAPI_GetFileName;
    storage_api->m_StorageAPI.m_API.GetDirectoryName = InMemStorageAPI_GetDirectoryName;
    storage_api->m_StorageAPI.m_API.GetEntrySize = InMemStorageAPI_GetEntrySize;
    storage_api->m_StorageAPI.Dispose = InMemStorageAPI_Dispose;

    storage_api->m_HashAPI = CreateMeowHashAPI();
    storage_api->m_PathHashToContent = 0;
    storage_api->m_PathEntries = 0;
}

struct StorageAPI* CreateInMemStorageAPI()
{
    struct InMemStorageAPI* storage_api = (struct InMemStorageAPI*)Longtail_Alloc(sizeof(struct InMemStorageAPI));
    InMemStorageAPI_Init(storage_api);
    return &storage_api->m_StorageAPI.m_API;
}



void DestroyStorageAPI(struct StorageAPI* storage_api)
{
    struct ManagedStorageAPI* managed = (struct ManagedStorageAPI*)storage_api;
    managed->Dispose(managed);
    Longtail_Free(managed);
}





struct ManagedJobAPI
{
    struct JobAPI m_API;
    void (*Dispose)(struct ManagedJobAPI* managed_job_api);
};

struct JobWrapper
{
    struct BikeshedJobAPI* m_JobAPI;
    JobAPI_JobFunc m_JobFunc;
    void* m_Context;
};

struct BikeshedJobAPI
{
    struct ManagedJobAPI m_ManagedAPI;

    struct ReadyCallback m_ReadyCallback;
    Bikeshed m_Shed;
    uint32_t m_WorkerCount;
    struct ThreadWorker* m_Workers;
    int32_t volatile m_Stop;
    struct JobWrapper* m_ReservedJobs;
    Bikeshed_TaskID* m_ReservedTasksIDs;
    uint32_t m_ReservedJobCount;
    int32_t volatile m_SubmittedJobCount;
    int32_t volatile m_PendingJobCount;
    int32_t volatile m_JobsCompleted;
};

static enum Bikeshed_TaskResult Bikeshed_Job(Bikeshed shed, Bikeshed_TaskID task_id, uint8_t channel, void* context)
{
    struct JobWrapper* wrapper = (struct JobWrapper*)context;
    wrapper->m_JobFunc(wrapper->m_Context);
    if (wrapper->m_JobAPI->m_PendingJobCount <= 0)
    {
        // TODO! Error handling!
        return BIKESHED_TASK_RESULT_COMPLETE;
    }
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_PendingJobCount, -1);
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_JobsCompleted, 1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

static int Bikeshed_GetWorkerCount(struct JobAPI* job_api)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    return bikeshed_job_api->m_WorkerCount;
}

static int Bikeshed_ReserveJobs(struct JobAPI* job_api, uint32_t job_count)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
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
    bikeshed_job_api->m_ReservedJobs = (struct JobWrapper*)Longtail_Alloc(sizeof(struct JobWrapper) * job_count);
    bikeshed_job_api->m_ReservedTasksIDs = (Bikeshed_TaskID*)Longtail_Alloc(sizeof(Bikeshed_TaskID) * job_count);
    if (bikeshed_job_api->m_ReservedJobs && bikeshed_job_api->m_ReservedTasksIDs)
    {
        bikeshed_job_api->m_ReservedJobCount = job_count;
        return 1;
    }
	Longtail_Free(bikeshed_job_api->m_ReservedTasksIDs);
	Longtail_Free(bikeshed_job_api->m_ReservedJobs);
    return 0;
}

static JobAPI_Jobs Bikeshed_CreateJobs(struct JobAPI* job_api, uint32_t job_count, JobAPI_JobFunc job_funcs[], void* job_contexts[])
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    int32_t new_job_count = Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, (int32_t)job_count);
    if (new_job_count > (int32_t)bikeshed_job_api->m_ReservedJobCount)
    {
        Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, -((int32_t)job_count));
        return 0;
    }
    int32_t job_range_start = new_job_count - job_count;

    BikeShed_TaskFunc* func = (BikeShed_TaskFunc*)Longtail_Alloc(sizeof(BikeShed_TaskFunc) * job_count);
    void** ctx = (void**)Longtail_Alloc(sizeof(void*) * job_count);
    Bikeshed_TaskID* task_ids = &bikeshed_job_api->m_ReservedTasksIDs[job_range_start];
    for (uint32_t i = 0; i < job_count; ++i)
    {
        struct JobWrapper* job_wrapper = &bikeshed_job_api->m_ReservedJobs[job_range_start + i];
        job_wrapper->m_JobAPI = bikeshed_job_api;
        job_wrapper->m_Context = job_contexts[i];
        job_wrapper->m_JobFunc = job_funcs[i];
        func[i] = Bikeshed_Job;
        ctx[i] = job_wrapper;
    }

    while (!Bikeshed_CreateTasks(bikeshed_job_api->m_Shed, job_count, func, ctx, task_ids))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }

    Longtail_AtomicAdd32(&bikeshed_job_api->m_PendingJobCount, job_count);

	Longtail_Free(ctx);
	Longtail_Free(func);
    return task_ids;
}

static void Bikeshed_AddDependecies(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs, uint32_t dependency_job_count, JobAPI_Jobs dependency_jobs)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    while (!Bikeshed_AddDependencies(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs, dependency_job_count, (Bikeshed_TaskID*)dependency_jobs))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }
}

static void Bikeshed_ReadyJobs(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs);
}

static void Bikeshed_WaitForAllJobs(struct JobAPI* job_api, void* context, JobAPI_ProgressFunc process_func)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
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
        Longtail_Sleep(1000);
    }
    if (process_func)
    {
        process_func(context, bikeshed_job_api->m_SubmittedJobCount, bikeshed_job_api->m_SubmittedJobCount);
    }
    bikeshed_job_api->m_SubmittedJobCount = 0;
	Longtail_Free(bikeshed_job_api->m_ReservedTasksIDs);
    bikeshed_job_api->m_ReservedTasksIDs = 0;
	Longtail_Free(bikeshed_job_api->m_ReservedJobs);
    bikeshed_job_api->m_ReservedJobs = 0;
    bikeshed_job_api->m_JobsCompleted = 0;
    bikeshed_job_api->m_ReservedJobCount = 0;
}

static void Bikeshed_Dispose(struct ManagedJobAPI* job_api)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Longtail_AtomicAdd32(&bikeshed_job_api->m_Stop, 1);
    ReadyCallback_Ready(&bikeshed_job_api->m_ReadyCallback.cb, 0, bikeshed_job_api->m_WorkerCount);
    for (uint32_t i = 0; i < bikeshed_job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_JoinThread(&bikeshed_job_api->m_Workers[i]);
    }
    for (uint32_t i = 0; i < bikeshed_job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_DisposeThread(&bikeshed_job_api->m_Workers[i]);
    }
	Longtail_Free(bikeshed_job_api->m_Workers);
	Longtail_Free(bikeshed_job_api->m_Shed);
}

static void Bikeshed_Init(struct BikeshedJobAPI* job_api, uint32_t worker_count)
{
    job_api->m_ManagedAPI.m_API.GetWorkerCount = Bikeshed_GetWorkerCount;
    job_api->m_ManagedAPI.m_API.ReserveJobs = Bikeshed_ReserveJobs;
    job_api->m_ManagedAPI.m_API.CreateJobs = Bikeshed_CreateJobs;
    job_api->m_ManagedAPI.m_API.AddDependecies = Bikeshed_AddDependecies;
    job_api->m_ManagedAPI.m_API.ReadyJobs = Bikeshed_ReadyJobs;
    job_api->m_ManagedAPI.m_API.WaitForAllJobs = Bikeshed_WaitForAllJobs;
    job_api->m_ManagedAPI.Dispose = Bikeshed_Dispose;
    job_api->m_Shed = 0;
    job_api->m_WorkerCount = worker_count;
    job_api->m_Workers = 0;
    job_api->m_Stop = 0;
    job_api->m_ReservedJobs = 0;
    job_api->m_ReservedTasksIDs = 0;
    job_api->m_ReservedJobCount = 0;
    job_api->m_SubmittedJobCount = 0;
    job_api->m_PendingJobCount = 0;
    job_api->m_JobsCompleted = 0;

	ReadyCallback_Init(&job_api->m_ReadyCallback);

    job_api->m_Shed = Bikeshed_Create(Longtail_Alloc(BIKESHED_SIZE(1048576, 7340032, 1)), 1048576, 7340032, 1, &job_api->m_ReadyCallback.cb);
    job_api->m_Workers = (struct ThreadWorker*)Longtail_Alloc(sizeof(struct ThreadWorker) * job_api->m_WorkerCount);
    for (uint32_t i = 0; i < job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_Init(&job_api->m_Workers[i]);
        ThreadWorker_CreateThread(&job_api->m_Workers[i], job_api->m_Shed, job_api->m_ReadyCallback.m_Semaphore, &job_api->m_Stop);
    }
}

struct JobAPI* CreateBikeshedJobAPI(uint32_t worker_count)
{
    struct BikeshedJobAPI* job_api = (struct BikeshedJobAPI*)Longtail_Alloc(sizeof(struct BikeshedJobAPI));
    Bikeshed_Init(job_api, worker_count);
    return &job_api->m_ManagedAPI.m_API;
}

void DestroyJobAPI(struct JobAPI* job_api)
{
    struct ManagedJobAPI* managed = (struct ManagedJobAPI*)job_api;
    managed->Dispose(managed);
    Longtail_Free(job_api);
}

struct ManagedCompressionAPI
{
    struct CompressionAPI m_API;
    void (*Dispose)(struct ManagedCompressionAPI* managed_compression_api);
};

struct LizardCompressionAPI
{
    struct ManagedCompressionAPI m_CompressionAPI;
};

void LizardCompressionAPI_Dispose(struct ManagedCompressionAPI* compression_api)
{
}

static int LizardCompressionAPI_DefaultCompressionSetting = 44;
static int LizardCompressionAPI_MaxCompressionSetting = LIZARD_MAX_CLEVEL;

static CompressionAPI_HSettings LizardCompressionAPI_GetDefaultSettings(struct CompressionAPI* compression_api)
{
    return (CompressionAPI_HSettings)&LizardCompressionAPI_DefaultCompressionSetting;
}

static CompressionAPI_HSettings LizardCompressionAPI_GetMaxCompressionSetting(struct CompressionAPI* compression_api)
{
    return (CompressionAPI_HSettings)&LizardCompressionAPI_MaxCompressionSetting;
}

static CompressionAPI_HCompressionContext LizardCompressionAPI_CreateCompressionContext(struct CompressionAPI* compression_api, CompressionAPI_HSettings settings)
{
    return (CompressionAPI_HCompressionContext)settings;
}

static size_t LizardCompressionAPI_GetMaxCompressedSize(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context, size_t size)
{
    return (size_t)Lizard_compressBound((int)size);
}

static size_t LizardCompressionAPI_Compress(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size)
{
    int compression_setting = *(int*)context;
    int compressed_size = Lizard_compress(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
    return (size_t)(compressed_size >= 0 ? compressed_size : 0);
}

static void LizardCompressionAPI_DeleteCompressionContext(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context)
{
}

static CompressionAPI_HDecompressionContext LizardCompressionAPI_CreateDecompressionContext(struct CompressionAPI* compression_api)
{
    return (CompressionAPI_HDecompressionContext)LizardCompressionAPI_GetDefaultSettings(compression_api);
}

static size_t LizardCompressionAPI_Decompress(struct CompressionAPI* compression_api, CompressionAPI_HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size)
{
    int result = Lizard_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)uncompressed_size);
    return (size_t)(result >= 0 ? result : 0);
}

static void LizardCompressionAPI_DeleteDecompressionContext(struct CompressionAPI* compression_api, CompressionAPI_HDecompressionContext context)
{
}

static void LizardCompressionAPI_Init(struct LizardCompressionAPI* compression_api)
{
    compression_api->m_CompressionAPI.m_API.GetDefaultSettings = LizardCompressionAPI_GetDefaultSettings;
    compression_api->m_CompressionAPI.m_API.GetMaxCompressionSetting = LizardCompressionAPI_GetMaxCompressionSetting;
    compression_api->m_CompressionAPI.m_API.CreateCompressionContext = LizardCompressionAPI_CreateCompressionContext;
    compression_api->m_CompressionAPI.m_API.GetMaxCompressedSize = LizardCompressionAPI_GetMaxCompressedSize;
    compression_api->m_CompressionAPI.m_API.Compress = LizardCompressionAPI_Compress;
    compression_api->m_CompressionAPI.m_API.DeleteCompressionContext = LizardCompressionAPI_DeleteCompressionContext;
    compression_api->m_CompressionAPI.m_API.CreateDecompressionContext = LizardCompressionAPI_CreateDecompressionContext;
    compression_api->m_CompressionAPI.m_API.Decompress = LizardCompressionAPI_Decompress;
    compression_api->m_CompressionAPI.m_API.DeleteDecompressionContext = LizardCompressionAPI_DeleteDecompressionContext;
    compression_api->m_CompressionAPI.Dispose = LizardCompressionAPI_Dispose;
}

struct CompressionAPI* CreateLizardCompressionAPI()
{
    struct LizardCompressionAPI* compression_api = (struct LizardCompressionAPI*)Longtail_Alloc(sizeof(struct LizardCompressionAPI));
    LizardCompressionAPI_Init(compression_api);
    return &compression_api->m_CompressionAPI.m_API;
}

void DestroyCompressionAPI(struct CompressionAPI* compression_api)
{
    struct ManagedCompressionAPI* managed = (struct ManagedCompressionAPI*)compression_api;
    managed->Dispose(managed);
    Longtail_Free(managed);
}


// TODO: Ugly hack!
static struct CompressionAPI* lizard_compression_api = 0;

struct CompressionRegistry* CreateDefaultCompressionRegistry()
{
    if (lizard_compression_api != 0)
    {
        return 0;
    }
    lizard_compression_api = CreateLizardCompressionAPI();
    static struct CompressionAPI* compression_apis[1];
    compression_apis[0] = lizard_compression_api;
    static uint32_t compression_types[] = {LIZARD_DEFAULT_COMPRESSION_TYPE};
    static CompressionAPI_HSettings compression_settings[1];
    compression_settings[0] = lizard_compression_api->GetDefaultSettings(lizard_compression_api);

    struct CompressionRegistry* compression_registry = CreateCompressionRegistry(
        1,
        &compression_types[0],
        (const struct CompressionAPI**)&compression_apis[0],
        &compression_settings[0]);
    return compression_registry;
}

void DestroyCompressionRegistry(struct CompressionRegistry* compression_registry)
{
    Longtail_Free(compression_registry);
    DestroyCompressionAPI(lizard_compression_api);
    lizard_compression_api = 0;
}
