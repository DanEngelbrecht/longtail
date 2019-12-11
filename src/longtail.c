#include "longtail.h"
#include "stb_ds.h"

#include <stdio.h>
#include <inttypes.h>

//#define SLOW_VALIDATION 1

/*
#if defined(LONGTAIL_ASSERTS)
void* Longtail_NukeMalloc(size_t s);
void Longtail_NukeFree(void* p);
#    define Longtail_Alloc(s) \
        Longtail_NukeMalloc(s)
#    define Longtail_Free(p) \
        Longtail_NukeFree(p)
#else
#    define Longtail_Alloc(s) \
        malloc(s)
#    define Longtail_Free(p) \
        Longtail_Free(p)
#endif // defined(LONGTAIL_ASSERTS)
*/

#if defined(LONGTAIL_ASSERTS)

static Longtail_Assert Longtail_Assert_private = 0;

#    define LONGTAIL_FATAL_ASSERT_PRIVATE(x, bail) \
        if (!(x)) \
        { \
            if (Longtail_Assert_private) \
            { \
                Longtail_Assert_private(#x, __FILE__, __LINE__); \
            } \
            bail; \
        }
#else // defined(LONGTAIL_ASSERTS)
#    define LONGTAIL_FATAL_ASSERT_PRIVATE(x, y)
#endif // defined(LONGTAIL_ASSERTS)

void Longtail_SetAssert(Longtail_Assert assert_func)
{
#if defined(LONGTAIL_ASSERTS)
    Longtail_Assert_private = assert_func;
#else  // defined(LONGTAIL_ASSERTS)
    (void)assert_func;
#endif // defined(LONGTAIL_ASSERTS)
}

static Longtail_Alloc_Func Longtail_Alloc_private = 0;
static Longtail_Free_Func Free_private = 0;

void Longtail_SetAllocAndFree(Longtail_Alloc_Func alloc, Longtail_Free_Func Longtail_Free)
{
    Longtail_Alloc_private = alloc;
    Free_private = Longtail_Free;
}

void* Longtail_Alloc(size_t s)
{
    return Longtail_Alloc_private ? Longtail_Alloc_private(s) : malloc(s);
}

void Longtail_Free(void* p)
{
    Free_private ? Free_private(p) : free(p);
}

static Longtail_Log Longtail_Log_private = 0;

void Longtail_SetLog(Longtail_Log log_func)
{
    Longtail_Log_private = log_func;
}

/*
static void* Longtail_NukeMalloc(size_t s)
{
    size_t aligned_size = ((s + sizeof(size_t) - 1) / sizeof(size_t)) * sizeof(size_t);
    size_t dbg_size = sizeof(size_t) + aligned_size + sizeof(size_t);
    char* r = (char*)malloc(dbg_size);
    LONGTAIL_FATAL_ASSERT_PRIVATE(r !=0, return 0)
    *(size_t*)r = dbg_size;
    *(size_t*)&r[dbg_size - sizeof(size_t)] = dbg_size;
    memset(&r[sizeof(size_t)], 127, aligned_size);
    return &r[sizeof(size_t)];
}

static void Longtail_NukeFree(void* p)
{
    if (!p)
    {
        return;
    }
    char* r = ((char*)p) - sizeof(size_t);
    size_t s1 = *(size_t*)r;
    size_t s2 = *(size_t*)(&r[s1 - sizeof(size_t)]);
    LONGTAIL_FATAL_ASSERT_PRIVATE(s1 == s2, return )
    memset(r, 255, s1);
    Longtail_Free(r);
}
*/

#ifndef LONGTAIL_LOG
    #ifdef LONGTAIL_VERBOSE_LOGS
        #define LONGTAIL_LOG(fmt, ...) \
            if (Longtail_Log_private) Longtail_Log_private(0, fmt, __VA_ARGS__);
    #else
        #define LONGTAIL_LOG(fmt, ...)
    #endif
#endif

#if !defined(LONGTAIL_ATOMICADD)
    #if defined(__clang__)
        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (__sync_add_and_fetch (value, amount))
        #define qsort_s qsort_r
    #elif defined(__GNUC__)
        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (__sync_add_and_fetch (value, amount))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (_InterlockedExchangeAdd((volatile LONG *)value, amount) + amount)
    #else
        inline int32_t LONGTAIL_NonAtomicAdd(volatile int32_t* store, int32_t value) { *store += value; return *store; }
        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (LONGTAIL_NonAtomicAdd(value, amount))
    #endif
#else
    #define LONGTAIL_ATOMICADD_PRIVATE LONGTAIL_ATOMICADD
#endif

char* Longtail_Strdup(const char* path)
{
    char* r = Longtail_Alloc(strlen(path) + 1);
    strcpy(r, path);
    return r;
}

int IsDirPath(const char* path)
{
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

TLongtail_Hash GetPathHash(struct HashAPI* hash_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    HashAPI_HContext context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, context, (uint32_t)strlen(path), (void*)path);
    return (TLongtail_Hash)hash_api->EndContext(hash_api, context);
}

int SafeCreateDir(struct StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    if (storage_api->CreateDir(storage_api, path))
    {
        return 1;
    }
    if (storage_api->IsDir(storage_api, path))
    {
        return 1;
    }
    LONGTAIL_LOG("Failed to create directory `%s`\n", path)
    return 0;
}

int EnsureParentPathExists(struct StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    char* dir_path = Longtail_Strdup(path);
    LONGTAIL_FATAL_ASSERT_PRIVATE(dir_path != 0, return 0);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 1;
    }
    else
    {
        if (!EnsureParentPathExists(storage_api, dir_path))
        {
            LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
            Longtail_Free(dir_path);
            dir_path = 0;
            return 0;
        }
        if (SafeCreateDir(storage_api, dir_path))
        {
            Longtail_Free(dir_path);
            dir_path = 0;
            return 1;
        }
    }
    LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
    Longtail_Free(dir_path);
    dir_path = 0;
    return 0;
}



struct CompressionRegistry
{
    uint32_t m_Count;
    uint32_t* m_Types;
    struct CompressionAPI** m_APIs;
    CompressionAPI_HSettings* m_Settings;
};

struct CompressionRegistry* CreateCompressionRegistry(
    uint32_t compression_type_count,
    const uint32_t* compression_types,
    const struct CompressionAPI** compression_apis,
    const CompressionAPI_HSettings* compression_settings)
{
    size_t size = sizeof(struct CompressionRegistry) +
        sizeof(uint32_t) * compression_type_count +
        sizeof(struct CompressionAPI*) * compression_type_count +
        sizeof(CompressionAPI_HSettings) * compression_type_count;
    struct CompressionRegistry* registry = (struct CompressionRegistry*)Longtail_Alloc(size);
    registry->m_Count = compression_type_count;
    char* p = (char*)&registry[1];
    registry->m_Types = (uint32_t*)p;
    p += sizeof(uint32_t) * compression_type_count;

    registry->m_APIs = (struct CompressionAPI**)p;
    p += sizeof(struct CompressionAPI*) * compression_type_count;

    registry->m_Settings = (CompressionAPI_HSettings*)p;

    memmove(registry->m_Types, compression_types, sizeof(uint32_t) * compression_type_count);
    memmove(registry->m_APIs, compression_apis, sizeof(struct CompressionAPI*) * compression_type_count);
    memmove(registry->m_Settings, compression_settings, sizeof(const CompressionAPI_HSettings) * compression_type_count);

    return registry;
}

struct CompressionAPI* GetCompressionAPI(struct CompressionRegistry* compression_registry, uint32_t compression_type)
{
    for (uint32_t i = 0; i < compression_registry->m_Count; ++i)
    {
        if (compression_registry->m_Types[i] == compression_type)
        {
            return compression_registry->m_APIs[i];
        }
    }
    return 0;
}

CompressionAPI_HSettings GetCompressionSettings(struct CompressionRegistry* compression_registry, uint32_t compression_type)
{
    for (uint32_t i = 0; i < compression_registry->m_Count; ++i)
    {
        if (compression_registry->m_Types[i] == compression_type)
        {
            return compression_registry->m_Settings[i];
        }
    }
    return 0;
}




struct HashToIndexItem
{
    TLongtail_Hash key;
    uint64_t value;
};

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size);

int RecurseTree(struct StorageAPI* storage_api, const char* root_folder, ProcessEntry entry_processor, void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_folder != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(entry_processor != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return 0);
    LONGTAIL_LOG("RecurseTree: Scanning folder `%s`\n", root_folder)

    uint32_t folder_index = 0;

    char** folder_paths = 0;
    arrsetcap(folder_paths, 256);

    arrput(folder_paths, Longtail_Strdup(root_folder));

    while (folder_index < (uint32_t)arrlen(folder_paths))
    {
        const char* asset_folder = folder_paths[folder_index++];

        StorageAPI_HIterator fs_iterator = storage_api->StartFind(storage_api, asset_folder);
        if (fs_iterator)
        {
            do
            {
                const char* dir_name = storage_api->GetDirectoryName(storage_api, fs_iterator);
                if (dir_name)
                {
                    entry_processor(context, asset_folder, dir_name, 1, 0);
                    if (arrlen(folder_paths) == arrcap(folder_paths))
                    {
                        if (folder_index > 0)
                        {
                            uint32_t unprocessed_count = (uint32_t)(arrlen(folder_paths) - folder_index);
                            memmove(folder_paths, &folder_paths[folder_index], sizeof(const char*) * unprocessed_count);
                            arrsetlen(folder_paths, unprocessed_count);
                            folder_index = 0;
                        }
                    }
                    arrput(folder_paths, storage_api->ConcatPath(storage_api, asset_folder, dir_name));
                }
                else
                {
                    const char* file_name = storage_api->GetFileName(storage_api, fs_iterator);
                    if (file_name)
                    {
                        uint64_t size = storage_api->GetEntrySize(storage_api, fs_iterator);
                        entry_processor(context, asset_folder, file_name, 0, size);
                    }
                }
            }while(storage_api->FindNext(storage_api, fs_iterator));
            storage_api->CloseFind(storage_api, fs_iterator);
        }
        Longtail_Free((void*)asset_folder);
        asset_folder = 0;
    }
    arrfree(folder_paths);
    folder_paths = 0;
    return 1;
}

size_t GetPathsSize(uint32_t path_count, uint32_t path_data_size)
{
    return sizeof(struct Paths) +
        sizeof(uint32_t) +                // PathCount
        sizeof(uint32_t) * path_count +    // m_Offsets
        path_data_size;
};

struct Paths* CreatePaths(uint32_t path_count, uint32_t path_data_size)
{
    struct Paths* paths = (struct Paths*)Longtail_Alloc(GetPathsSize(path_count, path_data_size));
    LONGTAIL_FATAL_ASSERT_PRIVATE(paths != 0, return 0)
    char* p = (char*)&paths[1];
    paths->m_DataSize = 0;
    paths->m_PathCount = (uint32_t*)p;
    p += sizeof(uint32_t);
    paths->m_Offsets = (uint32_t*)p;
    p += sizeof(uint32_t) * path_count;
    paths->m_Data = p;
    *paths->m_PathCount = 0;
    return paths;
};

struct Paths* MakePaths(uint32_t path_count, const char* const* path_names)
{
    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    struct Paths* paths = CreatePaths(path_count, name_data_size);
    uint32_t offset = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        uint32_t length = (uint32_t)strlen(path_names[i]) + 1;
        paths->m_Offsets[i] = offset;
        memmove(&paths->m_Data[offset], path_names[i], length);
        offset += length;
    }
    paths->m_DataSize = offset;
    *paths->m_PathCount = path_count;
    return paths;
}

struct Paths* AppendPath(struct Paths* paths, const char* path, uint32_t* max_path_count, uint32_t* max_data_size, uint32_t path_count_increment, uint32_t data_size_increment)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(paths != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_path_count != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_data_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path_count_increment > 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(data_size_increment > 0, return 0);
    uint32_t path_size = (uint32_t)(strlen(path) + 1);

    int out_of_path_data = paths->m_DataSize + path_size > *max_data_size;
    int out_of_path_count = *paths->m_PathCount >= *max_path_count;
    if (out_of_path_count | out_of_path_data)
    {
        uint32_t extra_path_count = out_of_path_count ? path_count_increment : 0;
        uint32_t extra_path_data_size = out_of_path_data ? (path_count_increment * data_size_increment) : 0;

        const uint32_t new_path_count = *max_path_count + extra_path_count;
        const uint32_t new_path_data_size = *max_data_size + extra_path_data_size;
        struct Paths* new_paths = CreatePaths(new_path_count, new_path_data_size);
        *max_path_count = new_path_count;
        *max_data_size = new_path_data_size;
        new_paths->m_DataSize = paths->m_DataSize;
        *new_paths->m_PathCount = *paths->m_PathCount;

        memmove(new_paths->m_Offsets, paths->m_Offsets, sizeof(uint32_t) * *paths->m_PathCount);
        memmove(new_paths->m_Data, paths->m_Data, paths->m_DataSize);

        Longtail_Free(paths);
        paths = new_paths;
    }

    memmove(&paths->m_Data[paths->m_DataSize], path, path_size);
    paths->m_Offsets[*paths->m_PathCount] = paths->m_DataSize;
    paths->m_DataSize += path_size;
    (*paths->m_PathCount)++;

    return paths;
}

struct AddFile_Context {
    struct StorageAPI* m_StorageAPI;
    uint32_t m_ReservedPathCount;
    uint32_t m_ReservedPathSize;
    uint32_t m_RootPathLength;
    struct Paths* m_Paths;
    uint64_t* m_FileSizes;
};

void AddFile(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_path != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(file_name != 0, return);
    struct AddFile_Context* paths_context = (struct AddFile_Context*)context;
    struct StorageAPI* storage_api = paths_context->m_StorageAPI;

    char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
    if (is_dir)
    {
        uint32_t path_length = (uint32_t)strlen(full_path);
        char* full_dir_path = (char*)Longtail_Alloc(path_length + 1 + 1);
        strcpy(full_dir_path, full_path);
        strcpy(&full_dir_path[path_length], "/");
        Longtail_Free(full_path);
        full_path = full_dir_path;
    }

    struct Paths* paths = paths_context->m_Paths;
    const uint32_t root_path_length = paths_context->m_RootPathLength;
    const char* s = &full_path[root_path_length];
    if (*s == '/')
    {
        ++s;
    }

    paths_context->m_Paths = AppendPath(paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);

    arrpush(paths_context->m_FileSizes, size);

    Longtail_Free(full_path);
    full_path = 0;
}

struct FileInfos* GetFilesRecursively(struct StorageAPI* storage_api, const char* root_path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_path != 0, return 0);
    LONGTAIL_LOG("GetFilesRecursively: Scanning `%s`\n", root_path)
    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    struct AddFile_Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), paths, 0};
    paths = 0;
    arrsetcap(context.m_FileSizes, 4096);

    if(!RecurseTree(storage_api, root_path, AddFile, &context))
    {
        LONGTAIL_LOG("GetFilesRecursively: Failed get files in folder `%s`\n", root_path)
        Longtail_Free(context.m_Paths);
        context.m_Paths = 0;
        arrfree(context.m_FileSizes);
        context.m_FileSizes = 0;
        return 0;
    }

    uint32_t asset_count = *context.m_Paths->m_PathCount;
    struct FileInfos* result = (struct FileInfos*)Longtail_Alloc(
        sizeof(struct FileInfos) +
        sizeof(uint64_t) * asset_count +
        GetPathsSize(asset_count, context.m_Paths->m_DataSize));

    result->m_Paths.m_DataSize = context.m_Paths->m_DataSize;
    result->m_Paths.m_PathCount = (uint32_t*)&result[1];
    *result->m_Paths.m_PathCount = asset_count;
    result->m_FileSizes = (uint64_t*)&result->m_Paths.m_PathCount[1];
    result->m_Paths.m_Offsets = (uint32_t*)(&result->m_FileSizes[asset_count]);
    result->m_Paths.m_Data = (char*)&result->m_Paths.m_Offsets[asset_count];
    memmove(result->m_FileSizes, context.m_FileSizes, sizeof(uint64_t) * asset_count);
    memmove(result->m_Paths.m_Offsets, context.m_Paths->m_Offsets, sizeof(uint32_t) * asset_count);
    memmove(result->m_Paths.m_Data, context.m_Paths->m_Data, result->m_Paths.m_DataSize);

    Longtail_Free(context.m_Paths);
    context.m_Paths = 0;
    arrfree(context.m_FileSizes);
    context.m_FileSizes = 0;

    return result;
}

struct StorageChunkFeederContext
{
    struct StorageAPI* m_StorageAPI;
    StorageAPI_HOpenFile m_AssetFile;
    const char* m_AssetPath;
    uint64_t m_StartRange;
    uint64_t m_Size;
    uint64_t m_Offset;
};

static uint32_t StorageChunkFeederFunc(void* context, struct Chunker* chunker, uint32_t requested_size, char* buffer)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunker != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(requested_size > 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(buffer != 0, return 0);
    struct StorageChunkFeederContext* c = (struct StorageChunkFeederContext*)context;
    uint64_t read_count = c->m_Size - c->m_Offset;
    if (read_count > 0)
    {
        if (requested_size < read_count)
        {
            read_count = requested_size;
        }
        if (!c->m_StorageAPI->Read(c->m_StorageAPI, c->m_AssetFile, c->m_StartRange + c->m_Offset, (uint32_t)read_count, buffer))
        {
            LONGTAIL_LOG("StorageChunkFeederFunc: Failed to read from asset file `%s`\n", c->m_AssetPath)
            return 0;
        }
        c->m_Offset += read_count;
    }
    return (uint32_t)read_count;
}

// ChunkerWindowSize is the number of bytes in the rolling hash window
#define ChunkerWindowSize 48

struct HashJob
{
    struct StorageAPI* m_StorageAPI;
    struct HashAPI* m_HashAPI;
    TLongtail_Hash* m_PathHash;
    uint64_t m_AssetIndex;
    uint32_t m_ContentCompressionType;
    const char* m_RootPath;
    const char* m_Path;
    uint32_t m_MaxChunkCount;
    uint64_t m_StartRange;
    uint64_t m_SizeRange;
    uint32_t* m_AssetChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkCompressionTypes;
    uint32_t* m_ChunkSizes;
    uint32_t m_MaxChunkSize;
    int m_Success;
};

void DynamicChunking(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Success = 0;

    *hash_job->m_PathHash = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path);

    if (IsDirPath(hash_job->m_Path))
    {
        hash_job->m_Success = 1;
        *hash_job->m_AssetChunkCount = 0;
        return;
    }
    uint32_t chunk_count = 0;

    struct StorageAPI* storage_api = hash_job->m_StorageAPI;
    char* path = storage_api->ConcatPath(storage_api, hash_job->m_RootPath, hash_job->m_Path);
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        LONGTAIL_LOG("DynamicChunking: Failed to open file `%s`\n", path)
        Longtail_Free(path);
        path = 0;
        return;
    }

    uint64_t hash_size = hash_job->m_SizeRange;
    TLongtail_Hash content_hash = 0;
    if (hash_size == 0)
    {
        content_hash = 0;
    }
    else if (hash_size <= ChunkerWindowSize || hash_job->m_MaxChunkSize <= ChunkerWindowSize)
    {
        char* buffer = (char*)Longtail_Alloc((size_t)hash_size);
        if (!storage_api->Read(storage_api, file_handle, 0, hash_size, buffer))
        {
            LONGTAIL_LOG("DynamicChunking: Failed to read from file `%s`\n", path)
            Longtail_Free(buffer);
            buffer = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            return;
        }

        HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
        hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, (uint32_t)hash_size, buffer);
        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);

        Longtail_Free(buffer);
        buffer = 0;

        hash_job->m_ChunkHashes[chunk_count] = content_hash;
        hash_job->m_ChunkSizes[chunk_count] = (uint32_t)hash_size;
        hash_job->m_ChunkCompressionTypes[chunk_count] = hash_job->m_ContentCompressionType;

        ++chunk_count;
    }
    else
    {
        uint32_t min_chunk_size = hash_job->m_MaxChunkSize / 8;
        min_chunk_size = min_chunk_size < ChunkerWindowSize ? ChunkerWindowSize : min_chunk_size;
        uint32_t avg_chunk_size = hash_job->m_MaxChunkSize / 2;
        avg_chunk_size = avg_chunk_size < ChunkerWindowSize ? ChunkerWindowSize : avg_chunk_size;
        uint32_t max_chunk_size = hash_job->m_MaxChunkSize * 2;

        struct StorageChunkFeederContext feeder_context =
        {
            storage_api,
            file_handle,
            path,
            hash_job->m_StartRange,
            hash_size,
            0
        };

        struct ChunkerParams chunker_params = { min_chunk_size, avg_chunk_size, max_chunk_size };

        struct Chunker* chunker = CreateChunker(
            &chunker_params,
            StorageChunkFeederFunc,
            &feeder_context);

        if (!chunker)
        {
            LONGTAIL_LOG("DynamicChunking: Failed to create chunker for asset `%s`\n", path)
            hash_job->m_Success = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            return;
        }

        HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);

        uint64_t remaining = hash_size;
        struct ChunkRange r = NextChunk(chunker);
        while (r.len)
        {
            if(remaining < r.len)
            {
                LONGTAIL_LOG("DynamicChunking: Chunking size is larger than remaining file size for asset `%s`\n", path)
                Longtail_Free(chunker);
                chunker = 0;
                hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
                hash_job->m_Success = 0;
                storage_api->CloseRead(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                return;
            }

            {
                HashAPI_HContext chunk_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
                hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, chunk_hash_context, r.len, (void*)r.buf);
                TLongtail_Hash chunk_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, chunk_hash_context);
                hash_job->m_ChunkHashes[chunk_count] = chunk_hash;
                hash_job->m_ChunkSizes[chunk_count] = r.len;
                hash_job->m_ChunkCompressionTypes[chunk_count] = hash_job->m_ContentCompressionType;
            }

            ++chunk_count;
            hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, r.len, (void*)r.buf);

            remaining -= r.len;
            r = NextChunk(chunker);
        }
        if(remaining != 0)
        {
            LONGTAIL_LOG("DynamicChunking: Chunking stopped before end of file size for asset `%s`\n", path)
            Longtail_Free(chunker);
            chunker = 0;
            hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
            hash_job->m_Success = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            return;
        }

        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
        Longtail_Free(chunker);
        chunker = 0;
    }

    storage_api->CloseRead(storage_api, file_handle);
    file_handle = 0;
    
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count <= hash_job->m_MaxChunkCount, return);
    *hash_job->m_AssetChunkCount = chunk_count;

    Longtail_Free((char*)path);
    path = 0;

    hash_job->m_Success = 1;
}

int ChunkAssets(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* root_path,
    const struct Paths* paths,
    TLongtail_Hash* path_hashes,
    TLongtail_Hash* content_hashes,
    const uint64_t* content_sizes,
    const uint32_t* content_compression_types,
    uint32_t* asset_chunk_start_index,
    uint32_t* asset_chunk_counts,
    uint32_t** chunk_sizes,
    TLongtail_Hash** chunk_hashes,
    uint32_t** chunk_compression_types,
    uint32_t max_chunk_size,
    uint32_t* chunk_count)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(paths != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path_hashes != 0, return 0);

    LONGTAIL_FATAL_ASSERT_PRIVATE(content_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_sizes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_chunk_start_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_chunk_counts != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_sizes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_compression_types != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_chunk_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count != 0, return 0);

    LONGTAIL_LOG("ChunkAssets: Hashing and chunking folder `%s` with %u assets\n", root_path, (uint32_t)*paths->m_PathCount)
    uint32_t asset_count = *paths->m_PathCount;

    uint64_t max_hash_size = max_chunk_size * 1024;
    uint32_t job_count = 0;
    uint64_t min_chunk_size = max_chunk_size / 8;
    uint64_t max_chunk_count = 0;
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = content_sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);
        job_count += (uint32_t)asset_part_count;

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            uint32_t max_count = job_size == 0 ? 0 : 1 + (job_size / min_chunk_size);
            max_chunk_count += max_count;
        }
    }

    if (!job_api->ReserveJobs(job_api, job_count))
    {
        LONGTAIL_LOG("ChunkAssets: Failed to reserve %u jobs for folder `%s`\n", (uint32_t)*paths->m_PathCount, root_path)
        return 0;
    }

    uint32_t* job_chunk_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * job_count);
    TLongtail_Hash* hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * max_chunk_count);
    uint32_t* sizes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_chunk_count);
    uint32_t* compression_types = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_chunk_count);

    struct HashJob* hash_jobs = (struct HashJob*)Longtail_Alloc(sizeof(struct HashJob) * job_count);

    uint64_t jobs_started = 0;
    uint64_t chunks_offset = 0;
    uint64_t assets_left = asset_count;
    uint64_t offset = 0;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = content_sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            LONGTAIL_FATAL_ASSERT_PRIVATE(jobs_started < job_count, return 0;)

            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            uint32_t max_chunk_count = job_size == 0 ? 0 : 1 + (job_size / min_chunk_size);

            struct HashJob* job = &hash_jobs[jobs_started];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_RootPath = root_path;
            job->m_Path = &paths->m_Data[paths->m_Offsets[asset_index]];
            job->m_PathHash = &path_hashes[asset_index];
            job->m_AssetIndex = asset_index;
            job->m_StartRange = range_start;
            job->m_SizeRange = job_size;
            job->m_ContentCompressionType = content_compression_types[asset_index];
            job->m_MaxChunkCount = max_chunk_count;
            job->m_AssetChunkCount = &job_chunk_counts[jobs_started];
            job->m_ChunkHashes = &hashes[chunks_offset];
            job->m_ChunkSizes = &sizes[chunks_offset];
            job->m_ChunkCompressionTypes = &compression_types[chunks_offset];
            job->m_MaxChunkSize = max_chunk_size;

            JobAPI_JobFunc func[1] = {DynamicChunking};
            void* ctx[1] = {&hash_jobs[jobs_started]};

            JobAPI_Jobs jobs = job_api->CreateJobs(job_api, 1, func, ctx);
            LONGTAIL_FATAL_ASSERT_PRIVATE(jobs != 0, return 0)
            job_api->ReadyJobs(job_api, 1, jobs);

            jobs_started++;

            chunks_offset += max_chunk_count;
        }
    }

    job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);

    int success = 1;
    for (uint32_t i = 0; i < jobs_started; ++i)
    {
        if (!hash_jobs[i].m_Success)
        {
            LONGTAIL_LOG("ChunkAssets: Failed to hash `%s`\n", hash_jobs[i].m_Path)
            success = 0;
        }
    }

    if (success)
    {
        uint32_t built_chunk_count = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            LONGTAIL_FATAL_ASSERT_PRIVATE(*hash_jobs[i].m_AssetChunkCount <= hash_jobs[i].m_MaxChunkCount, return 0;);
            built_chunk_count += *hash_jobs[i].m_AssetChunkCount;
        }
        *chunk_count = built_chunk_count;
        *chunk_sizes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * *chunk_count);
        *chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * *chunk_count);
        *chunk_compression_types = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * *chunk_count);

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            uint64_t asset_index = hash_jobs[i].m_AssetIndex;
            if (hash_jobs[i].m_StartRange == 0)
            {
                asset_chunk_start_index[asset_index] = chunk_offset;
                asset_chunk_counts[asset_index] = 0;
            }
            uint32_t job_chunk_counts = *hash_jobs[i].m_AssetChunkCount;
            asset_chunk_counts[asset_index] += job_chunk_counts;
            for (uint32_t chunk_index = 0; chunk_index < job_chunk_counts; ++chunk_index)
            {
                (*chunk_sizes)[chunk_offset] = hash_jobs[i].m_ChunkSizes[chunk_index];
                (*chunk_hashes)[chunk_offset] = hash_jobs[i].m_ChunkHashes[chunk_index];
                (*chunk_compression_types)[chunk_offset] = hash_jobs[i].m_ChunkCompressionTypes[chunk_index];
                ++chunk_offset;
            }
        }
        for (uint32_t a = 0; a < asset_count; ++a)
        {
            uint32_t chunk_start_index = asset_chunk_start_index[a];
            HashAPI_HContext hash_context = hash_api->BeginContext(hash_api);
            hash_api->Hash(hash_api, hash_context, sizeof(TLongtail_Hash) * asset_chunk_counts[a], &(*chunk_hashes)[chunk_start_index]);
            content_hashes[a] = hash_api->EndContext(hash_api, hash_context);
        }
    }
    else
    {
        *chunk_count = 0;
        *chunk_sizes = 0;
        *chunk_hashes = 0;
        *chunk_compression_types = 0;
    }

    Longtail_Free(compression_types);
    compression_types = 0;

    Longtail_Free(hashes);
    hashes = 0;

    Longtail_Free(sizes);
    sizes = 0;

    Longtail_Free(job_chunk_counts);
    job_chunk_counts = 0;

    Longtail_Free(hash_jobs);
    hash_jobs = 0;

    return success;
}

size_t GetVersionIndexDataSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    size_t version_index_data_size =
        sizeof(uint32_t) +                              // m_AssetCount
        sizeof(uint32_t) +                              // m_ChunkCount
        sizeof(uint32_t) +                              // m_AssetChunkIndexCount
        (sizeof(TLongtail_Hash) * asset_count) +        // m_PathHashes
        (sizeof(TLongtail_Hash) * asset_count) +        // m_ContentHashes
        (sizeof(uint64_t) * asset_count) +              // m_AssetSizes
        (sizeof(uint32_t) * asset_count) +              // m_AssetChunkCounts
        (sizeof(uint32_t) * asset_count) +              // m_AssetChunkIndexStarts
        (sizeof(uint32_t) * asset_chunk_index_count) +  // m_AssetChunkIndexes
        (sizeof(TLongtail_Hash) * chunk_count) +        // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count) +              // m_ChunkSizes
        (sizeof(uint32_t) * chunk_count) +              // m_ChunkCompressionTypes
        (sizeof(uint32_t) * asset_count) +              // m_NameOffsets
        path_data_size;

    return version_index_data_size;
}

size_t GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    return sizeof(struct VersionIndex) +
            GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, path_data_size);
}

void InitVersionIndex(struct VersionIndex* version_index, size_t version_index_data_size)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return);

    char* p = (char*)version_index;
    p += sizeof(struct VersionIndex);

    size_t version_index_data_start = (size_t)p;

    version_index->m_AssetCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    uint32_t asset_count = *version_index->m_AssetCount;

    version_index->m_ChunkCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    uint32_t chunk_count = *version_index->m_ChunkCount;

    version_index->m_AssetChunkIndexCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    uint32_t asset_chunk_index_count = *version_index->m_AssetChunkIndexCount;

    version_index->m_PathHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_ContentHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetSizes = (uint64_t*)p;
    p += (sizeof(uint64_t) * asset_count);

    version_index->m_AssetChunkCounts = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_AssetChunkIndexStarts = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_AssetChunkIndexes = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_chunk_index_count);

    version_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);

    version_index->m_ChunkSizes = (uint32_t*)p;
    p += (sizeof(uint32_t) * chunk_count);

    version_index->m_ChunkCompressionTypes = (uint32_t*)p;
    p += (sizeof(uint32_t) * chunk_count);

    version_index->m_NameOffsets = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    size_t version_index_name_data_start = (size_t)p;

    version_index->m_NameDataSize = (uint32_t)(version_index_data_size - (version_index_name_data_start - version_index_data_start));

    version_index->m_NameData = (char*)p;
}

struct VersionIndex* BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const struct Paths* paths,
    const TLongtail_Hash* path_hashes,
    const TLongtail_Hash* content_hashes,
    const uint64_t* content_sizes,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_counts,
    uint32_t asset_chunk_index_count,
    const uint32_t* asset_chunk_indexes,
    uint32_t chunk_count,
    const uint32_t* chunk_sizes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_compression_types)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(mem != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(mem_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(paths != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_sizes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_chunk_index_starts != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(*paths->m_PathCount == 0 || asset_chunk_counts != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_chunk_index_count >= chunk_count, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || asset_chunk_indexes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_sizes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_compression_types != 0, return 0);

    uint32_t asset_count = *paths->m_PathCount;
    struct VersionIndex* version_index = (struct VersionIndex*)mem;
    uint32_t* p = (uint32_t*)&version_index[1];
    version_index->m_AssetCount = &p[0];
    version_index->m_ChunkCount = &p[1];
    version_index->m_AssetChunkIndexCount = &p[2];
    *version_index->m_AssetCount = asset_count;
    *version_index->m_ChunkCount = chunk_count;
    *version_index->m_AssetChunkIndexCount = asset_chunk_index_count;

    InitVersionIndex(version_index, mem_size - sizeof(struct VersionIndex));

    memmove(version_index->m_PathHashes, path_hashes, sizeof(TLongtail_Hash) * asset_count);
    memmove(version_index->m_ContentHashes, content_hashes, sizeof(TLongtail_Hash) * asset_count);
    memmove(version_index->m_AssetSizes, content_sizes, sizeof(uint64_t) * asset_count);
    memmove(version_index->m_NameOffsets, paths, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_AssetChunkCounts, asset_chunk_counts, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_AssetChunkIndexStarts, asset_chunk_index_starts, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_AssetChunkIndexes, asset_chunk_indexes, sizeof(uint32_t) * asset_chunk_index_count);
    memmove(version_index->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);
    memmove(version_index->m_ChunkSizes, chunk_sizes, sizeof(uint32_t) * chunk_count);
    memmove(version_index->m_ChunkCompressionTypes, chunk_compression_types, sizeof(uint32_t) * chunk_count);
    memmove(version_index->m_NameOffsets, paths->m_Offsets, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_NameData, paths->m_Data, paths->m_DataSize);

    return version_index;
}

struct VersionIndex* CreateVersionIndex(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* root_path,
    const struct Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_compression_types,
    uint32_t max_chunk_size)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(paths != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_chunk_size != 0, return 0);

    uint32_t path_count = *paths->m_PathCount;
    TLongtail_Hash* path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * path_count);
    TLongtail_Hash* content_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * path_count);
    uint32_t* asset_chunk_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * path_count);

    uint32_t assets_chunk_index_count = 0;
    uint32_t* asset_chunk_sizes = 0;
    uint32_t* asset_chunk_compression_types = 0;
    TLongtail_Hash* asset_chunk_hashes = 0;
    uint32_t* asset_chunk_start_index = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * path_count);

    if (!ChunkAssets(
        storage_api,
        hash_api,
        job_api,
        job_progress_func,
        job_progress_context,
        root_path,
        paths,
        path_hashes,
        content_hashes,
        asset_sizes,
        asset_compression_types,
        asset_chunk_start_index,
        asset_chunk_counts,
        &asset_chunk_sizes,
        &asset_chunk_hashes,
        &asset_chunk_compression_types,
        max_chunk_size,
        &assets_chunk_index_count))
    {
        LONGTAIL_LOG("CreateVersionIndex: Failed to chunk and hash assets in `%s`\n", root_path);
        Longtail_Free(asset_chunk_compression_types);
        asset_chunk_compression_types = 0;
        Longtail_Free(asset_chunk_start_index);
        asset_chunk_start_index = 0;
        Longtail_Free(asset_chunk_hashes);
        asset_chunk_hashes = 0;
        Longtail_Free(asset_chunk_sizes);
        asset_chunk_sizes = 0;
        Longtail_Free(content_hashes);
        content_hashes = 0;
        Longtail_Free(path_hashes);
        path_hashes = 0;
        return 0;
    }

    uint32_t* asset_chunk_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);
    TLongtail_Hash* compact_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * assets_chunk_index_count);
    uint32_t* compact_chunk_sizes =  (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);
    uint32_t* compact_chunk_compression_types =  (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);

    uint32_t unique_chunk_count = 0;
    struct HashToIndexItem* chunk_hash_to_index = 0;
    for (uint32_t c = 0; c < assets_chunk_index_count; ++c)
    {
        TLongtail_Hash h = asset_chunk_hashes[c];
        intptr_t i = hmgeti(chunk_hash_to_index, h);
        if (i == -1)
        {
            hmput(chunk_hash_to_index, h, unique_chunk_count);
            compact_chunk_hashes[unique_chunk_count] = h;
            compact_chunk_sizes[unique_chunk_count] = asset_chunk_sizes[c];
            compact_chunk_compression_types[unique_chunk_count] = asset_chunk_compression_types[c];
            asset_chunk_indexes[c] = unique_chunk_count;
            ++unique_chunk_count;
        }
        else
        {
            asset_chunk_indexes[c] = chunk_hash_to_index[i].value;
        }
    }

    hmfree(chunk_hash_to_index);
    chunk_hash_to_index = 0;

    size_t version_index_size = GetVersionIndexSize(path_count, unique_chunk_count, assets_chunk_index_count, paths->m_DataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);

    struct VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,              // mem
        version_index_size,             // mem_size
        paths,                          // paths
        path_hashes,                    // path_hashes
        content_hashes,                 // content_hashes
        asset_sizes,                    // content_sizes
        asset_chunk_start_index,        // asset_chunk_index_starts
        asset_chunk_counts,             // asset_chunk_counts
        assets_chunk_index_count,       // asset_chunk_index_count
        asset_chunk_indexes,            // asset_chunk_indexes
        unique_chunk_count,             // chunk_count
        compact_chunk_sizes,            // chunk_sizes
        compact_chunk_hashes,           // chunk_hashes
        compact_chunk_compression_types); // chunk_compression_types

    Longtail_Free(compact_chunk_compression_types);
    compact_chunk_compression_types = 0;
    Longtail_Free(compact_chunk_sizes);
    compact_chunk_sizes = 0;
    Longtail_Free(compact_chunk_hashes);
    compact_chunk_hashes = 0;
    Longtail_Free(asset_chunk_indexes);
    asset_chunk_indexes = 0;
    Longtail_Free(asset_chunk_compression_types);
    asset_chunk_compression_types = 0;
    Longtail_Free(asset_chunk_sizes);
    asset_chunk_sizes = 0;
    Longtail_Free(asset_chunk_hashes);
    asset_chunk_hashes = 0;
    Longtail_Free(asset_chunk_start_index);
    asset_chunk_start_index = 0;
    Longtail_Free(asset_chunk_counts);
    asset_chunk_counts = 0;
    Longtail_Free(content_hashes);
    content_hashes = 0;
    Longtail_Free(path_hashes);
    path_hashes = 0;

    return version_index;
}

int WriteVersionIndex(
    struct StorageAPI* storage_api,
    struct VersionIndex* version_index,
    const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    LONGTAIL_LOG("WriteVersionIndex: Writing index to `%s` containing %u assets in %u chunks.\n", path, *version_index->m_AssetCount, *version_index->m_ChunkCount);
    size_t index_data_size = GetVersionIndexDataSize((uint32_t)(*version_index->m_AssetCount), (*version_index->m_ChunkCount), (*version_index->m_AssetChunkIndexCount), version_index->m_NameDataSize);

    if (!EnsureParentPathExists(storage_api, path))
    {
        LONGTAIL_LOG("WriteVersionIndex: Failed create parent path for `%s`\n", path);
        return 0;
    }
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path, 0);
    if (!file_handle)
    {
        LONGTAIL_LOG("WriteVersionIndex: Failed open `%s` for write\n", path);
        return 0;
    }
    if (!storage_api->Write(storage_api, file_handle, 0, index_data_size, &version_index[1]))
    {
        LONGTAIL_LOG("WriteVersionIndex: Failed to write to `%s`\n", path);
        storage_api->CloseWrite(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    storage_api->CloseWrite(storage_api, file_handle);
    file_handle = 0;

    return 1;
}

struct VersionIndex* ReadVersionIndex(
    struct StorageAPI* storage_api,
    const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);

    LONGTAIL_LOG("ReadVersionIndex: Reading from `%s`\n", path)
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        LONGTAIL_LOG("ReadVersionIndex: Failed to open file `%s`\n", path);
        return 0;
    }
    uint64_t version_index_data_size = storage_api->GetSize(storage_api, file_handle);
    struct VersionIndex* version_index = (struct VersionIndex*)Longtail_Alloc((size_t)(sizeof(struct VersionIndex) + version_index_data_size));
    if (!version_index)
    {
        LONGTAIL_LOG("ReadVersionIndex: Failed to allocate memory for `%s`\n", path);
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]))
    {
        LONGTAIL_LOG("ReadVersionIndex: Failed to read from `%s`\n", path);
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        return 0;
    }
    InitVersionIndex(version_index, (size_t)version_index_data_size);
    storage_api->CloseRead(storage_api, file_handle);
    LONGTAIL_LOG("ReadVersionIndex: Read index from `%s` containing %u assets in  %u chunks.\n", path, *version_index->m_AssetCount, *version_index->m_ChunkCount);
    return version_index;
}

struct BlockIndex
{
    TLongtail_Hash* m_BlockHash;
    uint32_t* m_ChunkCompressionType;
    TLongtail_Hash* m_ChunkHashes; //[]
    uint32_t* m_ChunkSizes; // []
    uint32_t* m_ChunkCount;
};

size_t GetBlockIndexDataSize(uint32_t chunk_count)
{
    return
        sizeof(TLongtail_Hash) +                    // m_BlockHash
        sizeof(uint32_t) +                          // m_ChunkCompressionType
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count) +          // m_ChunkSizes
        sizeof(uint32_t);                           // m_ChunkCount
}

struct BlockIndex* InitBlockIndex(void* mem, uint32_t chunk_count)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(mem != 0, return 0);

    struct BlockIndex* block_index = (struct BlockIndex*)mem;
    char* p = (char*)&block_index[1];

    block_index->m_BlockHash = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash);

    block_index->m_ChunkCompressionType = (uint32_t*)p;
    p += sizeof(uint32_t);

    block_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash) * chunk_count;

    block_index->m_ChunkSizes = (uint32_t*)p;
    p += sizeof(uint32_t) * chunk_count;

    block_index->m_ChunkCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    return block_index;
}

size_t GetBlockIndexSize(uint32_t chunk_count)
{
    size_t block_index_size =
        sizeof(struct BlockIndex) +
        GetBlockIndexDataSize(chunk_count);

    return block_index_size;
}

struct BlockIndex* CreateBlockIndex(
    void* mem,
    struct HashAPI* hash_api,
    uint64_t chunk_compression_nethod,
    uint32_t chunk_count_in_block,
    uint64_t* chunk_indexes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(mem != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count_in_block != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_indexes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_sizes != 0, return 0);

    struct BlockIndex* block_index = InitBlockIndex(mem, chunk_count_in_block);
    for (uint32_t i = 0; i < chunk_count_in_block; ++i)
    {
        uint64_t chunk_index = chunk_indexes[i];
        block_index->m_ChunkHashes[i] = chunk_hashes[chunk_index];
        block_index->m_ChunkSizes[i] = chunk_sizes[chunk_index];
    }
    HashAPI_HContext hash_context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, hash_context, (uint32_t)(sizeof(TLongtail_Hash) * chunk_count_in_block), (void*)block_index->m_ChunkHashes);
    *block_index->m_ChunkCompressionType = chunk_compression_nethod;
    *block_index->m_BlockHash = hash_api->EndContext(hash_api, hash_context);
    *block_index->m_ChunkCount = chunk_count_in_block;

    return block_index;
}

size_t GetContentIndexDataSize(uint64_t block_count, uint64_t chunk_count)
{
    size_t block_index_data_size = (size_t)(
        sizeof(uint64_t) +                          // m_BlockCount
        sizeof(uint64_t) +                          // m_ChunkCount
        (sizeof(TLongtail_Hash) * block_count) +    // m_BlockHashes[]
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes[]
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkBlockIndexes[]
        (sizeof(uint32_t) * chunk_count) +          // m_ChunkBlockOffsets[]
        (sizeof(uint32_t) * chunk_count)            // m_ChunkLengths[]
        );

    return block_index_data_size;
}

size_t GetContentIndexSize(uint64_t block_count, uint64_t chunk_count)
{
    return sizeof(struct ContentIndex) +
        GetContentIndexDataSize(block_count, chunk_count);
}

void InitContentIndex(struct ContentIndex* content_index)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return);

    char* p = (char*)&content_index[1];
    content_index->m_BlockCount = (uint64_t*)p;
    p += sizeof(uint64_t);
    content_index->m_ChunkCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    content_index->m_BlockHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);
    content_index->m_ChunkBlockIndexes = (uint64_t*)p;
    p += (sizeof(uint64_t) * chunk_count);
    content_index->m_ChunkBlockOffsets = (uint32_t*)p;
    p += (sizeof(uint32_t) * chunk_count);
    content_index->m_ChunkLengths = (uint32_t*)p;
    p += (sizeof(uint32_t) * chunk_count);
}

uint64_t GetUniqueHashes(uint64_t hash_count, const TLongtail_Hash* hashes, uint64_t* out_unique_hash_indexes)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_count != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hashes != 0, return 0);

    struct HashToIndexItem* lookup_table = 0;

    uint64_t unique_hash_count = 0;
    for (uint64_t i = 0; i < hash_count; ++i)
    {
        TLongtail_Hash hash = hashes[i];
        ptrdiff_t lookup_index = hmgeti(lookup_table, hash);
        if (lookup_index == -1)
        {
            hmput(lookup_table, hash, 1);
            out_unique_hash_indexes[unique_hash_count] = i;
            ++unique_hash_count;
        }
        else
        {
            ++lookup_table[lookup_index].value;
        }
    }
    hmfree(lookup_table);
    lookup_table = 0;
    return unique_hash_count;
}

struct ContentIndex* CreateContentIndex(
    struct HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* chunk_compression_types,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_sizes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_compression_types != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_block_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_chunks_per_block != 0, return 0);

    LONGTAIL_LOG("CreateContentIndex: Creating index for %" PRIu64 " chunks\n", chunk_count)
    if (chunk_count == 0)
    {
        size_t content_index_size = GetContentIndexSize(0, 0);
        struct ContentIndex* content_index = (struct ContentIndex*)Longtail_Alloc(content_index_size);

        content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
        content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
        *content_index->m_BlockCount = 0;
        *content_index->m_ChunkCount = 0;
        InitContentIndex(content_index);
        return content_index;
    }
    uint64_t* chunk_indexes = (uint64_t*)Longtail_Alloc((size_t)(sizeof(uint64_t) * chunk_count));
    uint64_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, chunk_indexes);

    struct BlockIndex** block_indexes = (struct BlockIndex**)Longtail_Alloc(sizeof(struct BlockIndex*) * unique_chunk_count);

    #define MAX_ASSETS_PER_BLOCK 16384u
    uint64_t* stored_chunk_indexes = (uint64_t*)Longtail_Alloc(sizeof(uint64_t) * max_chunks_per_block);

    uint32_t current_size = 0;
    uint64_t i = 0;
    uint32_t chunk_count_in_block = 0;
    uint32_t block_count = 0;
    uint32_t current_compression_type = 0;

    while (i < unique_chunk_count)
    {
        chunk_count_in_block = 0;

        uint64_t chunk_index = chunk_indexes[i];

        uint32_t current_size = chunk_sizes[chunk_index];
        current_compression_type = chunk_compression_types[chunk_index];

        stored_chunk_indexes[chunk_count_in_block] = chunk_index;
        ++chunk_count_in_block;

        while((i + 1) < unique_chunk_count)
        {
            chunk_index = chunk_indexes[(i + 1)];
            uint32_t chunk_size = chunk_sizes[chunk_index];
            uint32_t compression_type = chunk_compression_types[chunk_index];

            if (compression_type != current_compression_type)
            {
                break;
            }

            // Break if resulting chunk count will exceed MAX_ASSETS_PER_BLOCK
            if (chunk_count_in_block == max_chunks_per_block)
            {
                break;
            }

            // Overshoot by 10% is ok
            if ((current_size + chunk_size) > (max_block_size + (max_block_size / 10)))
            {
                break;
            }

            current_size += chunk_size;
            stored_chunk_indexes[chunk_count_in_block] = chunk_index;
            ++chunk_count_in_block;

            ++i;
        }

        block_indexes[block_count] = CreateBlockIndex(
            Longtail_Alloc(GetBlockIndexSize(chunk_count_in_block)),
            hash_api,
            current_compression_type,
            chunk_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes);

        ++block_count;
        ++i;
    }

    if (current_size > 0)
    {
        block_indexes[block_count] = CreateBlockIndex(
            Longtail_Alloc(GetBlockIndexSize(chunk_count_in_block)),
            hash_api,
            current_compression_type,
            chunk_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes);
        ++block_count;
    }

    Longtail_Free(stored_chunk_indexes);
    stored_chunk_indexes = 0;
    Longtail_Free(chunk_indexes);
    chunk_indexes = 0;

    // Build Content Index (from block list)
    size_t content_index_size = GetContentIndexSize(block_count, unique_chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)Longtail_Alloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = unique_chunk_count;
    InitContentIndex(content_index);

    uint64_t asset_index = 0;
    for (uint32_t i = 0; i < block_count; ++i)
    {
        struct BlockIndex* block_index = block_indexes[i];
        content_index->m_BlockHashes[i] = *block_index->m_BlockHash;
        uint32_t chunk_offset = 0;
        for (uint32_t a = 0; a < *block_index->m_ChunkCount; ++a)
        {
            content_index->m_ChunkHashes[asset_index] = block_index->m_ChunkHashes[a];
            content_index->m_ChunkBlockIndexes[asset_index] = i;
            content_index->m_ChunkBlockOffsets[asset_index] = chunk_offset;
            content_index->m_ChunkLengths[asset_index] = block_index->m_ChunkSizes[a];

            chunk_offset += block_index->m_ChunkSizes[a];
            ++asset_index;
            if (asset_index > unique_chunk_count)
            {
                break;
            }
        }
        Longtail_Free(block_index);
        block_index = 0;
    }
    Longtail_Free(block_indexes);
    block_indexes = 0;

    return content_index;
}

int WriteContentIndex(
    struct StorageAPI* storage_api,
    struct ContentIndex* content_index,
    const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);

    LONGTAIL_LOG("WriteContentIndex: Write index to `%s`, chunks %u, blocks %u\n", path, (uint32_t)*content_index->m_ChunkCount, (uint32_t)*content_index->m_BlockCount)
    size_t index_data_size = GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);

    if (!EnsureParentPathExists(storage_api, path))
    {
        LONGTAIL_LOG("WriteContentIndex: Failed to create parent folder for `%s`\n", path);
        return 0;
    }
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path, 0);
    if (!file_handle)
    {
        LONGTAIL_LOG("WriteContentIndex: Failed to create `%s`\n", path);
        return 0;
    }
    if (!storage_api->Write(storage_api, file_handle, 0, index_data_size, &content_index[1]))
    {
        LONGTAIL_LOG("WriteContentIndex: Failed to write to `%s`\n", path);
        storage_api->CloseWrite(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    storage_api->CloseWrite(storage_api, file_handle);

    return 1;
}

struct ContentIndex* ReadContentIndex(
    struct StorageAPI* storage_api,
    const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);

    LONGTAIL_LOG("ReadContentIndex from `%s`\n", path)
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        LONGTAIL_LOG("ReadContentIndex: Failed to open `%s`\n", path);
        return 0;
    }
    uint64_t content_index_data_size = storage_api->GetSize(storage_api, file_handle);
    struct ContentIndex* content_index = (struct ContentIndex*)Longtail_Alloc((size_t)(sizeof(struct ContentIndex) + content_index_data_size));
    if (!content_index)
    {
        LONGTAIL_LOG("ReadContentIndex: Failed allocate memory for `%s`\n", path);
        Longtail_Free(content_index);
        content_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]))
    {
        LONGTAIL_LOG("ReadContentIndex: Failed to read from `%s`\n", path);
        Longtail_Free(content_index);
        content_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    InitContentIndex(content_index);
    storage_api->CloseRead(storage_api, file_handle);
    return content_index;
}

struct AssetPart
{
    const char* m_Path;
    uint64_t m_Start;
    uint32_t m_CompressionType;
#ifdef SLOW_VALIDATION
    uint32_t m_CunkSize;    // TODO: Just for validation, remove
#endif // SLOW_VALIDATION
};

struct ChunkHashToAssetPart
{
    TLongtail_Hash key;
    struct AssetPart value;
};

struct ChunkHashToAssetPart* CreateAssetPartLookup(
    struct VersionIndex* version_index)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);

    struct ChunkHashToAssetPart* asset_part_lookup = 0;
    for (uint64_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint64_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunk_offset = 0;
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
            LONGTAIL_FATAL_ASSERT_PRIVATE(asset_chunk_index_start + asset_chunk_index < *version_index->m_AssetChunkIndexCount, return 0);
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_index < *version_index->m_ChunkCount, return 0);
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            uint32_t compression_type = version_index->m_ChunkCompressionTypes[chunk_index];
            intptr_t lookup_ptr = hmgeti(asset_part_lookup, chunk_hash);
            if (lookup_ptr == -1)
            {
                struct AssetPart asset_part = {
                    path,
                    asset_chunk_offset,
                    compression_type
#ifdef SLOW_VALIDATION
                    , chunk_size
#endif // SLOW_VALIDATION
                };
                hmput(asset_part_lookup, chunk_hash, asset_part);
            }
#ifdef SLOW_VALIDATION
            else
            {
                struct AssetPart* asset_part = &asset_part_lookup[lookup_ptr].value;
                LONGTAIL_FATAL_ASSERT_PRIVATE(asset_part->m_CunkSize == chunk_size, return 0);
            }
#endif // SLOW_VALIDATION
            asset_chunk_offset += chunk_size;
        }
    }
    return asset_part_lookup;
}

struct WriteBlockJob
{
    struct StorageAPI* m_SourceStorageAPI;
    struct StorageAPI* m_TargetStorageAPI;
    struct CompressionRegistry* m_CompressionRegistry;
    const char* m_ContentFolder;
    const char* m_AssetsFolder;
    TLongtail_Hash m_BlockHash;
    const char* m_BlockPath;
    const struct ContentIndex* m_ContentIndex;
    struct ChunkHashToAssetPart* m_AssetPartLookup;
    uint64_t m_FirstChunkIndex;
    uint32_t m_ChunkCount;
    uint32_t m_Success;
};

#define MAX_BLOCK_NAME_LENGTH   32

void GetBlockName(TLongtail_Hash block_hash, char* out_name)
{
    sprintf(out_name, "0x%016" PRIx64, block_hash);
//    sprintf(&out_name[5], "0x%016" PRIx64, block_hash);
//    memmove(out_name, &out_name[5], 4);
//    out_name[4] = '/';
}

static char* ReadBlockData(
    struct StorageAPI* storage_api,
    struct CompressionRegistry* compression_registry,
    const char* content_folder,
    TLongtail_Hash block_hash)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_registry != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_folder != 0, return 0);

    char file_name[MAX_BLOCK_NAME_LENGTH + 4];
    GetBlockName(block_hash, file_name);
    strcat(file_name, ".lrb");
    char* block_path = storage_api->ConcatPath(storage_api, content_folder, file_name);

    StorageAPI_HOpenFile block_file = storage_api->OpenReadFile(storage_api, block_path);
    if (!block_file)
    {
        LONGTAIL_LOG("ReadBlockData: Failed to open block `%s`\n", block_path)
        Longtail_Free(block_path);
        block_path = 0;
        return 0;
    }
    uint64_t compressed_block_size = storage_api->GetSize(storage_api, block_file);

    char* compressed_block_content = (char*)Longtail_Alloc(compressed_block_size);
    int ok = storage_api->Read(storage_api, block_file, 0, compressed_block_size, compressed_block_content);
    storage_api->CloseRead(storage_api, block_file);
    block_file = 0;
    if (!ok){
        LONGTAIL_LOG("ReadBlockData: Failed to read block `%s`\n", block_path)
        Longtail_Free(block_path);
        block_path = 0;
        Longtail_Free(compressed_block_content);
        compressed_block_content = 0;
        return 0;
    }

    uint32_t chunk_count = *(const uint32_t*)(&compressed_block_content[compressed_block_size - sizeof(uint32_t)]);
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);
    if (compressed_block_size < block_index_data_size)
    {
        LONGTAIL_LOG("ReadBlockData: Malformed content block (size to small) `%s`\n", block_path)
        Longtail_Free(block_path);
        block_path = 0;
        Longtail_Free(compressed_block_content);
        compressed_block_content = 0;
        return 0;
    }

    char* block_data = compressed_block_content;
    const TLongtail_Hash* block_hash_ptr = (const TLongtail_Hash*)&block_data[compressed_block_size - block_index_data_size];
    // TODO: This could be cleaner
    TLongtail_Hash verify_block_hash = *block_hash_ptr;
    if (block_hash != verify_block_hash)
    {
        LONGTAIL_LOG("ReadBlockData: Malformed content block (mismatching block hash) `%s`\n", block_path)
        Longtail_Free(block_path);
        block_path = 0;
        Longtail_Free(block_data);
        block_data = 0;
        return 0;
    }

    const uint32_t* compression_type_ptr = (const uint32_t*)&block_hash_ptr[1];
    uint32_t compression_type = *compression_type_ptr;
    if (0 != compression_type)
    {
        struct CompressionAPI* compression_api = GetCompressionAPI(compression_registry, compression_type);
        if (!compression_api)
        {
            LONGTAIL_LOG("ReadBlockData: Compression type not supported `%u`\n", compression_type)
            Longtail_Free(block_path);
            block_path = 0;
            Longtail_Free(block_data);
            block_data = 0;
            return 0;
        }
        uint32_t uncompressed_size = ((uint32_t*)compressed_block_content)[0];
        uint32_t compressed_size = ((uint32_t*)compressed_block_content)[1];
        block_data = (char*)Longtail_Alloc(uncompressed_size);
        CompressionAPI_HDecompressionContext compression_context = compression_api->CreateDecompressionContext(compression_api);
        if (!compression_context)
        {
            LONGTAIL_LOG("ReadBlockData: Failed to create decompressor for block `%s`\n", block_path)
            Longtail_Free(block_data);
            block_data = 0;
            Longtail_Free(block_path);
            block_path = 0;
            Longtail_Free(compressed_block_content);
            compressed_block_content = 0;
            return 0;
        }
        size_t result = compression_api->Decompress(compression_api, compression_context, &compressed_block_content[sizeof(uint32_t) * 2], block_data, compressed_size, uncompressed_size);
        ok = result == uncompressed_size;
        compression_api->DeleteDecompressionContext(compression_api, compression_context);
        Longtail_Free(compressed_block_content);
        compressed_block_content = 0;

        if (!ok)
        {
            LONGTAIL_LOG("ReadBlockData: Failed to decompress block `%s`\n", block_path)
            Longtail_Free(block_data);
            block_data = 0;
            Longtail_Free(block_path);
            block_path = 0;
            return 0;
        }
    }

    Longtail_Free(block_path);
    block_path = 0;

    return block_data;
}

struct BlockIndex* ReadBlockIndex(
    struct StorageAPI* storage_api,
    const char* full_block_path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(full_block_path != 0, return 0);

    StorageAPI_HOpenFile f = storage_api->OpenReadFile(storage_api, full_block_path);
    if (!f)
    {
        LONGTAIL_LOG("ReadBlock: Failed to open block `%s`\n", full_block_path)
        return 0;
    }
    uint64_t s = storage_api->GetSize(storage_api, f);
    if (s < (sizeof(uint32_t)))
    {
        storage_api->CloseRead(storage_api, f);
        return 0;
    }
    uint32_t chunk_count = 0;
    if (!storage_api->Read(storage_api, f, s - sizeof(uint32_t), sizeof(uint32_t), &chunk_count))
    {
        storage_api->CloseRead(storage_api, f);
        return 0;
    }
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);
    if (s < block_index_data_size)
    {
        storage_api->CloseRead(storage_api, f);
        return 0;
    }

    struct BlockIndex* block_index = InitBlockIndex(Longtail_Alloc(GetBlockIndexSize(chunk_count)), chunk_count);

    int ok = storage_api->Read(storage_api, f, s - block_index_data_size, block_index_data_size, &block_index[1]);
    storage_api->CloseRead(storage_api, f);
    if (!ok)
    {
        LONGTAIL_LOG("ReadBlock: Failed to read block `%s`\n", full_block_path)
        Longtail_Free(block_index);
        block_index = 0;
        return 0;
    }

    return block_index;
}

void WriteContentBlockJob(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct WriteBlockJob* job = (struct WriteBlockJob*)context;
    struct StorageAPI* source_storage_api = job->m_SourceStorageAPI;
    struct StorageAPI* target_storage_api = job->m_TargetStorageAPI;
    struct CompressionRegistry* compression_registry = job->m_CompressionRegistry;

    const struct ContentIndex* content_index = job->m_ContentIndex;
    const char* content_folder = job->m_ContentFolder;
    uint64_t first_chunk_index = job->m_FirstChunkIndex;
    uint32_t chunk_count = job->m_ChunkCount;
    uint64_t block_index = content_index->m_ChunkBlockIndexes[first_chunk_index];
    TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];

    char tmp_block_name[MAX_BLOCK_NAME_LENGTH + 4];
    GetBlockName(job->m_BlockHash, tmp_block_name);
    strcat(tmp_block_name, ".tmp");

    char* tmp_block_path = (char*)target_storage_api->ConcatPath(target_storage_api, content_folder, tmp_block_name);

    uint32_t block_data_size = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        LONGTAIL_FATAL_ASSERT_PRIVATE(content_index->m_ChunkBlockIndexes[chunk_index] == block_index, return;)
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        block_data_size += chunk_size;
    }

    char* block_data_buffer = (char*)Longtail_Alloc(block_data_size);
    char* write_buffer = block_data_buffer;
    char* write_ptr = write_buffer;

    uint32_t compression_type = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t tmp;
        intptr_t asset_part_index = hmgeti_ts(job->m_AssetPartLookup, chunk_hash, tmp);
        if (asset_part_index == -1)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Failed to get path for asset content 0x%" PRIx64 " in `%s`\n", chunk_hash, content_folder)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        struct AssetPart* asset_part = &job->m_AssetPartLookup[asset_part_index].value;
        const char* asset_path = asset_part->m_Path;
        if (IsDirPath(asset_path))
        {
            LONGTAIL_LOG("WriteContentBlockJob: Directory should not have any chunks `%s`\n", asset_path)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }

        char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
        uint64_t asset_content_offset = asset_part->m_Start;
        if (chunk_index != first_chunk_index && compression_type != asset_part->m_CompressionType)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Warning: Inconsistend compression type for chunks inside block 0x%" PRIx64 " in `%s`, retaining %u\n", block_hash, content_folder, compression_type)
        }
        else
        {
            compression_type = asset_part->m_CompressionType;
        }
        StorageAPI_HOpenFile file_handle = source_storage_api->OpenReadFile(source_storage_api, full_path);
        if (!file_handle)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Failed to open asset file `%s`\n", full_path);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        uint64_t asset_file_size = source_storage_api->GetSize(source_storage_api, file_handle);
        if (asset_file_size < (asset_content_offset + chunk_size))
        {
            LONGTAIL_LOG("WriteContentBlockJob: Mismatching asset size in asset `%s`, size is %" PRIu64 ", but expecting at least %" PRIu64 "\n", full_path, asset_file_size, asset_content_offset + chunk_size);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            source_storage_api->CloseRead(source_storage_api, file_handle);
            file_handle = 0;
            return;
        }
        source_storage_api->Read(source_storage_api, file_handle, asset_content_offset, chunk_size, write_ptr);
        write_ptr += chunk_size;

        source_storage_api->CloseRead(source_storage_api, file_handle);
        Longtail_Free((char*)full_path);
        full_path = 0;
    }

    if (compression_type != 0)
    {
        struct CompressionAPI* compression_api = GetCompressionAPI(compression_registry, compression_type);
        if (!compression_api)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Compression type not supported `%u`\n", compression_type)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        CompressionAPI_HSettings compression_settings = GetCompressionSettings(compression_registry, compression_type);
        CompressionAPI_HCompressionContext compression_context = compression_api->CreateCompressionContext(compression_api, compression_settings);
        const size_t max_dst_size = compression_api->GetMaxCompressedSize(compression_api, compression_context, block_data_size);
        char* compressed_buffer = (char*)Longtail_Alloc((sizeof(uint32_t) * 2) + max_dst_size);
        ((uint32_t*)compressed_buffer)[0] = (uint32_t)block_data_size;

        size_t compressed_size = compression_api->Compress(compression_api, compression_context, (const char*)write_buffer, &((char*)compressed_buffer)[sizeof(int32_t) * 2], block_data_size, max_dst_size);
        compression_api->DeleteCompressionContext(compression_api, compression_context);
        if (compressed_size <= 0)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Failed to compress data for block for `%s`\n", tmp_block_path)
            Longtail_Free(compressed_buffer);
            compressed_buffer = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        ((uint32_t*)compressed_buffer)[1] = (uint32_t)compressed_size;

        Longtail_Free(block_data_buffer);
        block_data_buffer = 0;
        block_data_size = (uint32_t)(sizeof(uint32_t) + sizeof(uint32_t) + compressed_size);
        block_data_buffer = compressed_buffer;
    }

    if (!EnsureParentPathExists(target_storage_api, tmp_block_path))
    {
        LONGTAIL_LOG("WriteContentBlockJob: Failed to create parent path for `%s`\n", tmp_block_path)
        Longtail_Free(block_data_buffer);
        block_data_buffer = 0;
        Longtail_Free((char*)tmp_block_path);
        return;
    }

    StorageAPI_HOpenFile block_file_handle = target_storage_api->OpenWriteFile(target_storage_api, tmp_block_path, 0);
    if (!block_file_handle)
    {
        LONGTAIL_LOG("WriteContentBlockJob: Failed to create block file `%s`\n", tmp_block_path)
        Longtail_Free(block_data_buffer);
        block_data_buffer = 0;
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        return;
    }
    int write_ok = target_storage_api->Write(target_storage_api, block_file_handle, 0, block_data_size, block_data_buffer);
    Longtail_Free(block_data_buffer);
    block_data_buffer = 0;
    uint32_t write_offset = block_data_size;

    uint32_t aligned_size = (((write_offset + 15) / 16) * 16);
    uint32_t padding = aligned_size - write_offset;
    if (padding)
    {
        target_storage_api->Write(target_storage_api, block_file_handle, write_offset, padding, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        write_offset = aligned_size;
    }
    struct BlockIndex* block_index_ptr = (struct BlockIndex*)Longtail_Alloc(GetBlockIndexSize(chunk_count));
    InitBlockIndex(block_index_ptr, chunk_count);
    memmove(block_index_ptr->m_ChunkHashes, &content_index->m_ChunkHashes[first_chunk_index], sizeof(TLongtail_Hash) * chunk_count);
    memmove(block_index_ptr->m_ChunkSizes, &content_index->m_ChunkLengths[first_chunk_index], sizeof(uint32_t) * chunk_count);
    *block_index_ptr->m_BlockHash = block_hash;
    *block_index_ptr->m_ChunkCompressionType = compression_type;
    *block_index_ptr->m_ChunkCount = chunk_count;
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);
    write_ok = target_storage_api->Write(target_storage_api, block_file_handle, write_offset, block_index_data_size, &block_index_ptr[1]);
    Longtail_Free(block_index_ptr);
    block_index_ptr = 0;

    target_storage_api->CloseWrite(target_storage_api, block_file_handle);
    write_ok = write_ok & target_storage_api->RenameFile(target_storage_api, tmp_block_path, job->m_BlockPath);
#if SLOW_VALIDATION
    void* block_data = ReadBlockData(target_storage_api, compression_api, content_folder, block_hash);
    LONGTAIL_FATAL_ASSERT_PRIVATE(block_data != 0, return; )
    Longtail_Free(block_data);
    block_data = 0;
#endif
    job->m_Success = write_ok;

    Longtail_Free((char*)tmp_block_path);
    tmp_block_path = 0;

    job->m_Success = 1;
}

int WriteContent(
    struct StorageAPI* source_storage_api,
    struct StorageAPI* target_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct ContentIndex* content_index,
    struct VersionIndex* version_index,
    const char* assets_folder,
    const char* content_folder)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(source_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(target_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_registry != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(assets_folder != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_folder != 0, return 0);

    LONGTAIL_LOG("WriteContent: Writing content from `%s` to `%s`, chunks %u, blocks %u\n", assets_folder, content_folder, (uint32_t)*content_index->m_ChunkCount, (uint32_t)*content_index->m_BlockCount)
    uint64_t block_count = *content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 1;
    }

    if (!job_api->ReserveJobs(job_api, (uint32_t)block_count))
    {
        LONGTAIL_LOG("WriteContent: Failed to reserve jobs when writing to `%s`\n", content_folder)
        return 0;
    }

    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version_index);
    if (!asset_part_lookup)
    {
        return 0;
    }

    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteBlockJob) * block_count));
    uint32_t block_start_chunk_index = 0;
    uint32_t job_count = 0;
    for (uint64_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        uint32_t chunk_count = 0;
        while(content_index->m_ChunkBlockIndexes[block_start_chunk_index + chunk_count] == block_index)
        {
            ++chunk_count;
        }

        char block_name[MAX_BLOCK_NAME_LENGTH];
        GetBlockName(block_hash, block_name);
        char file_name[64];
        sprintf(file_name, "%s.lrb", block_name);
        char* block_path = target_storage_api->ConcatPath(target_storage_api, content_folder, file_name);
        if (target_storage_api->IsFile(target_storage_api, block_path))
        {
            Longtail_Free((char*)block_path);
            block_path = 0;
            block_start_chunk_index += chunk_count;
            continue;
        }

        struct WriteBlockJob* job = &write_block_jobs[job_count++];
        job->m_SourceStorageAPI = source_storage_api;
        job->m_TargetStorageAPI = target_storage_api;
        job->m_CompressionRegistry = compression_registry;
        job->m_ContentFolder = content_folder;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = content_index;
        job->m_BlockHash = block_hash;
        job->m_BlockPath = block_path;
        job->m_AssetPartLookup = asset_part_lookup;
        job->m_FirstChunkIndex = block_start_chunk_index;
        job->m_ChunkCount = chunk_count;
        job->m_Success = 0;

        JobAPI_JobFunc func[1] = { WriteContentBlockJob };
        void* ctx[1] = { job };

        JobAPI_Jobs jobs = job_api->CreateJobs(job_api, 1, func, ctx);
        LONGTAIL_FATAL_ASSERT_PRIVATE(jobs != 0, return 0)
        job_api->ReadyJobs(job_api, 1, jobs);

        block_start_chunk_index += chunk_count;
    }

    job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);

    int success = 1;
    while (job_count--)
    {
        struct WriteBlockJob* job = &write_block_jobs[job_count];
        if (!job->m_Success)
        {
            LONGTAIL_LOG("WriteContent: Failed to write content to `%s`\n", content_folder)
            uint64_t first_chunk_index = job->m_FirstChunkIndex;
            uint64_t block_index = content_index->m_ChunkBlockIndexes[first_chunk_index];
            TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
            success = 0;
        }
        Longtail_Free((char*)job->m_BlockPath);
        job->m_BlockPath = 0;
    }

    hmfree(asset_part_lookup);
    asset_part_lookup = 0;
    Longtail_Free(write_block_jobs);
    write_block_jobs = 0;

    return success;
}


struct ContentLookup
{
    struct HashToIndexItem* m_BlockHashToBlockIndex;
    struct HashToIndexItem* m_ChunkHashToChunkIndex;
    struct HashToIndexItem* m_ChunkHashToBlockIndex;
};

void DeleteContentLookup(struct ContentLookup* cl)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(cl != 0, return);

    hmfree(cl->m_ChunkHashToBlockIndex);
    cl->m_ChunkHashToBlockIndex = 0;
    hmfree(cl->m_BlockHashToBlockIndex);
    cl->m_BlockHashToBlockIndex = 0;
    hmfree(cl->m_ChunkHashToChunkIndex);
    cl->m_ChunkHashToChunkIndex = 0;
    Longtail_Free(cl);
}

struct ContentLookup* CreateContentLookup(
    uint64_t block_count,
    const TLongtail_Hash* block_hashes,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint64_t* chunk_block_indexes)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(block_count == 0 || block_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count == 0 || chunk_block_indexes != 0, return 0);

    struct ContentLookup* cl = (struct ContentLookup*)Longtail_Alloc(sizeof(struct ContentLookup));
    cl->m_BlockHashToBlockIndex = 0;
    cl->m_ChunkHashToChunkIndex = 0;
    cl->m_ChunkHashToBlockIndex = 0;
    for (uint64_t i = 0; i < block_count; ++i)
    {
        TLongtail_Hash block_hash = block_hashes[i];
        hmput(cl->m_BlockHashToBlockIndex, block_hash, i);
    }
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = chunk_hashes[i];
        hmput(cl->m_ChunkHashToChunkIndex, chunk_hash, i);
        uint64_t block_index = chunk_block_indexes[i];
        hmput(cl->m_ChunkHashToBlockIndex, chunk_hash, block_index);
    }
    return cl;
}


struct BlockDecompressorJob
{
    struct StorageAPI* m_ContentStorageAPI;
    struct CompressionRegistry* m_CompressionRegistry;
    const char* m_ContentFolder;
    TLongtail_Hash m_BlockHash;
    char* m_BlockData;
};

void BlockDecompressor(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct BlockDecompressorJob* job = (struct BlockDecompressorJob*)context;
    job->m_BlockData = ReadBlockData(
        job->m_ContentStorageAPI,
        job->m_CompressionRegistry,
        job->m_ContentFolder,
        job->m_BlockHash);
    if (!job->m_BlockData)
    {
        LONGTAIL_LOG("BlockDecompressor: Failed to read block 0x%" PRIx64 " from `%s`\n", job->m_BlockHash, job->m_ContentFolder)
        return;
    }
}

void WriteReady(void* context)
{
    // Nothing to do here, we are just a syncronization point
}

#define MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE  64

struct WritePartialAssetFromBlocksJob
{
    struct StorageAPI* m_ContentStorageAPI;
    struct StorageAPI* m_VersionStorageAPI;
    struct CompressionRegistry* m_CompressionRegistry;
    struct JobAPI* m_JobAPI;
    const struct ContentIndex* m_ContentIndex;
    const struct VersionIndex* m_VersionIndex;
    const char* m_ContentFolder;
    const char* m_VersionFolder;
    struct ContentLookup* m_ContentLookup;
    uint32_t m_AssetIndex;

    struct BlockDecompressorJob m_BlockDecompressorJobs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    uint32_t m_BlockDecompressorJobCount;

    uint32_t m_AssetChunkIndexOffset;
    uint32_t m_AssetChunkCount;

    StorageAPI_HOpenFile m_AssetOutputFile;

    int m_Success;
};

void WritePartialAssetFromBlocks(void* context);

// Returns the write sync task, or the write task if there is no need for decompression of block
JobAPI_Jobs CreatePartialAssetWriteJob(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const char* content_folder,
    const char* version_folder,
    struct ContentLookup* content_lookup,
    uint32_t asset_index,
    struct WritePartialAssetFromBlocksJob* job,
    uint32_t asset_chunk_index_offset,
    StorageAPI_HOpenFile asset_output_file)
{
    job->m_ContentStorageAPI = content_storage_api;
    job->m_VersionStorageAPI = version_storage_api;
    job->m_CompressionRegistry = compression_registry;
    job->m_JobAPI = job_api;
    job->m_ContentIndex = content_index;
    job->m_VersionIndex = version_index;
    job->m_ContentFolder = content_folder;
    job->m_VersionFolder = version_folder;
    job->m_ContentLookup = content_lookup;
    job->m_AssetIndex = asset_index;
    job->m_BlockDecompressorJobCount = 0;
    job->m_AssetChunkIndexOffset = asset_chunk_index_offset;
    job->m_AssetChunkCount = 0;
    job->m_AssetOutputFile = asset_output_file;
    job->m_Success = 0;

    uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
    uint32_t chunk_start_index_offset = chunk_index_start + asset_chunk_index_offset;
    uint32_t chunk_index_end = chunk_index_start + version_index->m_AssetChunkCounts[asset_index];
    uint32_t chunk_index_offset = chunk_start_index_offset;

    JobAPI_JobFunc decompress_funcs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    void* decompress_ctx[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];

    const uint32_t worker_count = job_api->GetWorkerCount(job_api) + 2;
    const uint32_t max_parallell_decompress_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;

    while (chunk_index_offset != chunk_index_end && job->m_BlockDecompressorJobCount < max_parallell_decompress_jobs)
    {
        uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        intptr_t tmp;
        uint64_t block_index = hmget_ts(content_lookup->m_ChunkHashToBlockIndex, chunk_hash, tmp);
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        int has_block = 0;
        for (uint32_t d = 0; d < job->m_BlockDecompressorJobCount; ++d)
        {
            if (job->m_BlockDecompressorJobs[d].m_BlockHash == block_hash)
            {
                has_block = 1;
                break;
            }
        }
        if (!has_block)
        {
            struct BlockDecompressorJob* block_job = &job->m_BlockDecompressorJobs[job->m_BlockDecompressorJobCount];
            block_job->m_ContentStorageAPI = content_storage_api;
            block_job->m_CompressionRegistry = compression_registry;
            block_job->m_ContentFolder = content_folder;
            block_job->m_BlockHash = block_hash;
            decompress_funcs[job->m_BlockDecompressorJobCount] = BlockDecompressor;
            decompress_ctx[job->m_BlockDecompressorJobCount] = block_job;
            ++job->m_BlockDecompressorJobCount;
        }
        ++job->m_AssetChunkCount;
        ++chunk_index_offset;
    }

    JobAPI_JobFunc write_funcs[1] = { WritePartialAssetFromBlocks };
    void* write_ctx[1] = { job };
    JobAPI_Jobs write_job = job_api->CreateJobs(job_api, 1, write_funcs, write_ctx);

    if (job->m_BlockDecompressorJobCount > 0)
    {
        JobAPI_Jobs decompression_jobs = job_api->CreateJobs(job_api, job->m_BlockDecompressorJobCount, decompress_funcs, decompress_ctx);
        JobAPI_JobFunc sync_write_funcs[1] = { WriteReady };
        void* sync_write_ctx[1] = { 0 };
        JobAPI_Jobs write_sync_job = job_api->CreateJobs(job_api, 1, sync_write_funcs, sync_write_ctx);

        job_api->AddDependecies(job_api, 1, write_job, 1, write_sync_job);
        job_api->AddDependecies(job_api, 1, write_job, job->m_BlockDecompressorJobCount, decompression_jobs);
        job_api->ReadyJobs(job_api, job->m_BlockDecompressorJobCount, decompression_jobs);

        return write_sync_job;
    }
    return write_job;
}

void WritePartialAssetFromBlocks(void* context)
{
    struct WritePartialAssetFromBlocksJob* job = (struct WritePartialAssetFromBlocksJob*)context;

    // Need to fetch all the data we need from the context since we will reuse it
    uint32_t block_decompressor_job_count = job->m_BlockDecompressorJobCount;
    TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    char* block_datas[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    uint32_t decompressed_block_count = 0;
    for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
    {
        block_hashes[d] =job->m_BlockDecompressorJobs[d].m_BlockHash;
        block_datas[d] =job->m_BlockDecompressorJobs[d].m_BlockData;
        if (block_datas[d])
        {
            ++decompressed_block_count;
        }
    }

    if (decompressed_block_count != block_decompressor_job_count)
    {
        LONGTAIL_LOG("WritePartialAssetFromBlocks: %u blocks failed to decompress\n", block_decompressor_job_count - decompressed_block_count)
        for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
        {
            Longtail_Free(block_datas[d]);
        }
        return;
    }

    uint32_t write_chunk_index_offset = job->m_AssetChunkIndexOffset;
    uint32_t write_chunk_count = job->m_AssetChunkCount;
    uint32_t asset_chunk_count = job->m_VersionIndex->m_AssetChunkCounts[job->m_AssetIndex];
    const char* asset_path = &job->m_VersionIndex->m_NameData[job->m_VersionIndex->m_NameOffsets[job->m_AssetIndex]];

    if (!job->m_AssetOutputFile && job->m_AssetChunkIndexOffset)
    {
        LONGTAIL_LOG("WritePartialAssetFromBlocks: Skipping write to asset `%s` due to previous write failure\n", asset_path)
        for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
        {
            Longtail_Free(block_datas[d]);
        }
        return;
    }
    if (!job->m_AssetOutputFile)
    {
        char* full_asset_path = job->m_VersionStorageAPI->ConcatPath(job->m_VersionStorageAPI, job->m_VersionFolder, asset_path);
        if (!EnsureParentPathExists(job->m_VersionStorageAPI, full_asset_path))
        {
            LONGTAIL_LOG("WritePartialAssetFromBlocks: Failed to create parent folder for `%s` in `%s`\n", asset_path, job->m_VersionFolder)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
            {
                Longtail_Free(block_datas[d]);
            }
            return;
        }
        if (IsDirPath(full_asset_path))
        {
            LONGTAIL_FATAL_ASSERT_PRIVATE(block_decompressor_job_count == 0, return; )
            if (!SafeCreateDir(job->m_VersionStorageAPI, full_asset_path))
            {
                LONGTAIL_LOG("WritePartialAssetFromBlocks: Failed to create folder for `%s` in `%s`\n", asset_path, job->m_VersionFolder)
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                return;
            }
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            job->m_Success = 1;
            return;
        }

        uint64_t asset_size = job->m_VersionIndex->m_AssetSizes[job->m_AssetIndex];
        job->m_AssetOutputFile = job->m_VersionStorageAPI->OpenWriteFile(job->m_VersionStorageAPI, full_asset_path, asset_size);
        if (!job->m_AssetOutputFile)
        {
            LONGTAIL_LOG("WritePartialAssetFromBlocks: Unable to create asset `%s` in `%s`\n", asset_path, job->m_VersionFolder)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
            {
                Longtail_Free(block_datas[d]);
            }
            return;
        }
        Longtail_Free(full_asset_path);
        full_asset_path = 0;
    }

    JobAPI_Jobs sync_write_job = 0;
    if (write_chunk_index_offset + write_chunk_count < asset_chunk_count)
    {
        sync_write_job = CreatePartialAssetWriteJob(
            job->m_ContentStorageAPI,
            job->m_VersionStorageAPI,
            job->m_CompressionRegistry,
            job->m_JobAPI,
            job->m_ContentIndex,
            job->m_VersionIndex,
            job->m_ContentFolder,
            job->m_VersionFolder,
            job->m_ContentLookup,
            job->m_AssetIndex,
            job,    // Reuse job
            write_chunk_index_offset + write_chunk_count,
            job->m_AssetOutputFile);

        if (!sync_write_job)
        {
            LONGTAIL_LOG("WritePartialAssetFromBlocks: Failed to create next write/decompress job for asset `%s`\n", asset_path)
            for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
            {
                Longtail_Free(block_datas[d]);
            }
            return;
        }
        // Decompression of blocks will start immediately
    }

    uint32_t chunk_index_offset = write_chunk_index_offset;
    uint32_t chunk_index_start = job->m_VersionIndex->m_AssetChunkIndexStarts[job->m_AssetIndex];

    uint64_t write_offset = 0;
    for (uint32_t c = 0; c < chunk_index_offset; ++c)
    {
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[chunk_index_start + c];
        uint32_t chunk_size = job->m_VersionIndex->m_ChunkSizes[chunk_index];
        write_offset += chunk_size;
    }

    while (chunk_index_offset < write_chunk_index_offset + write_chunk_count)
    {
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[chunk_index_start + chunk_index_offset];
        TLongtail_Hash chunk_hash = job->m_VersionIndex->m_ChunkHashes[chunk_index];
        intptr_t tmp;
        uint64_t content_chunk_index = hmget_ts(job->m_ContentLookup->m_ChunkHashToChunkIndex, chunk_hash, tmp);
        uint64_t block_index = job->m_ContentIndex->m_ChunkBlockIndexes[content_chunk_index];
        TLongtail_Hash block_hash = job->m_ContentIndex->m_BlockHashes[block_index];
        uint32_t decompressed_block_index = 0;
        while (block_hashes[decompressed_block_index] != block_hash)
        {
            if (decompressed_block_index == block_decompressor_job_count)
            {
                break;
            }
            ++decompressed_block_index;
        }
        LONGTAIL_FATAL_ASSERT_PRIVATE(decompressed_block_index != block_decompressor_job_count, return; );
        char* block_data = block_datas[decompressed_block_index];

        uint32_t chunk_offset = job->m_ContentIndex->m_ChunkBlockOffsets[content_chunk_index];
        uint32_t chunk_size = job->m_ContentIndex->m_ChunkLengths[content_chunk_index];

        if (!job->m_VersionStorageAPI->Write(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, chunk_size, &block_data[chunk_offset]))
        {
            LONGTAIL_LOG("WritePartialAssetFromBlocks: Failed to write to asset `%s`\n", asset_path)
            job->m_VersionStorageAPI->CloseWrite(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;

            for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
            {
                Longtail_Free(block_datas[d]);
            }
            if (sync_write_job)
            {
                job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
            }
            return;
        }
        write_offset += chunk_size;

        ++chunk_index_offset;
    }

    for (uint32_t d = 0; d < block_decompressor_job_count; ++d)
    {
        Longtail_Free(block_datas[d]);
    }

    if (sync_write_job)
    {
        // We can now release the next write job
        job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
        return;
    }

    int ok = 1;//job->m_VersionStorageAPI->SetSize(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset);
    job->m_VersionStorageAPI->CloseWrite(job->m_VersionStorageAPI, job->m_AssetOutputFile);

    job->m_AssetOutputFile = 0;
    if (!ok)
    {
        LONGTAIL_LOG("WritePartialAssetFromBlocks: Failed to set final size of asset `%s` to %" PRIx64 "\n", asset_path, write_offset)
        return;
    }

    job->m_Success = 1;
}

struct WriteAssetsFromBlockJob
{
    struct StorageAPI* m_ContentStorageAPI;
    struct StorageAPI* m_VersionStorageAPI;
    struct CompressionRegistry* m_CompressionRegistry;
    const struct ContentIndex* m_ContentIndex;
    const struct VersionIndex* m_VersionIndex;
    const char* m_ContentFolder;
    const char* m_VersionFolder;
    struct BlockDecompressorJob m_DecompressBlockJob;
    uint64_t m_BlockIndex;
    uint32_t* m_AssetIndexes;
    uint32_t m_AssetCount;
    struct HashToIndexItem* m_ContentChunkLookup;
    int m_Success;
};

void WriteAssetsFromBlock(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct WriteAssetsFromBlockJob* job = (struct WriteAssetsFromBlockJob*)context;
    job->m_Success = 0;
    struct StorageAPI* content_storage_api = job->m_ContentStorageAPI;
    struct StorageAPI* version_storage_api = job->m_VersionStorageAPI;
    struct CompressionRegistry* compression_registry = job->m_CompressionRegistry;
    const char* content_folder = job->m_ContentFolder;
    const char* version_folder = job->m_VersionFolder;
    const uint64_t block_index = job->m_BlockIndex;
    const struct ContentIndex* content_index = job->m_ContentIndex;
    const struct VersionIndex* version_index = job->m_VersionIndex;
    uint32_t* asset_indexes = job->m_AssetIndexes;
    uint32_t asset_count = job->m_AssetCount;
    struct HashToIndexItem* content_chunk_lookup = job->m_ContentChunkLookup;

    TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
    char* block_data = job->m_DecompressBlockJob.m_BlockData;
    if (!block_data)
    {
        LONGTAIL_LOG("WriteAssetsFromBlock: Failed to read block 0x%" PRIx64 "\n", block_hash)
        return;
    }

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_folder, asset_path);
        int ok = EnsureParentPathExists(version_storage_api, full_asset_path);
        if (!ok)
        {
            LONGTAIL_LOG("WriteAssetsFromBlock: Failed to create parent folder for `%s`\n", full_asset_path)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            Longtail_Free(block_data);
            block_data = 0;
            return;
        }

        StorageAPI_HOpenFile asset_file = version_storage_api->OpenWriteFile(version_storage_api, full_asset_path, 0);
        if (!asset_file)
        {
            LONGTAIL_LOG("WriteAssetsFromBlock: Unable to create asset `%s`\n", full_asset_path)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            Longtail_Free(block_data);
            block_data = 0;
            return;
        }

        uint64_t asset_write_offset = 0;
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < version_index->m_AssetChunkCounts[asset_index]; ++asset_chunk_index)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];

            ptrdiff_t tmp;
            uint64_t content_chunk_index = hmget_ts(content_chunk_lookup, chunk_hash, tmp);
            uint32_t chunk_block_offset = content_index->m_ChunkBlockOffsets[content_chunk_index];
            uint32_t chunk_size = content_index->m_ChunkLengths[content_chunk_index];
            ok = version_storage_api->Write(version_storage_api, asset_file, asset_write_offset, chunk_size, &block_data[chunk_block_offset]);
            if (!ok)
            {
                LONGTAIL_LOG("WriteAssetsFromBlock: Failed to write to asset `%s`\n", full_asset_path)
                content_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                Longtail_Free(block_data);
                block_data = 0;
                return;
            }
            asset_write_offset += chunk_size;
        }

        version_storage_api->CloseWrite(version_storage_api, asset_file);
        asset_file = 0;

        Longtail_Free(full_asset_path);
        full_asset_path = 0;
    }

    Longtail_Free(block_data);
    block_data = 0;
    job->m_Success = 1;
}

struct AssetWriteList
{
    uint32_t m_BlockJobCount;
    uint32_t m_AssetJobCount;
    uint32_t* m_BlockJobAssetIndexes;
    uint32_t* m_AssetIndexJobs;
};

struct BlockJobCompareContext
{
    const struct AssetWriteList* m_AssetWriteList;
    const uint32_t* asset_chunk_index_starts;
    const TLongtail_Hash* chunk_hashes;
    struct ContentLookup* cl;
};

#if defined(__clang__)
static int BlockJobCompare(const void* a_ptr, const void* b_ptr, void* context)
#elif defined(_MSC_VER) || defined(__GNUC__)
static int BlockJobCompare(void* context, const void* a_ptr, const void* b_ptr)
#endif
{
    struct BlockJobCompareContext* c = (struct BlockJobCompareContext*)context;
    const struct AssetWriteList* awl = c->m_AssetWriteList;
    struct HashToIndexItem* chunk_hash_to_block_index = c->cl->m_ChunkHashToBlockIndex;

    uint32_t a = *(uint32_t*)a_ptr;
    uint32_t b = *(uint32_t*)b_ptr;
    TLongtail_Hash a_first_chunk_hash = c->chunk_hashes[c->asset_chunk_index_starts[a]];
    TLongtail_Hash b_first_chunk_hash = c->chunk_hashes[c->asset_chunk_index_starts[b]];
    if (a_first_chunk_hash == b_first_chunk_hash)
    {
        return 0;
    }
    uint64_t a_block_index = hmget(chunk_hash_to_block_index, a_first_chunk_hash);
    uint64_t b_block_index = hmget(chunk_hash_to_block_index, b_first_chunk_hash);
    if (a_block_index == b_block_index)
    {
        return 0;
    }
    else if (a_block_index < b_block_index)
    {
        return -1;
    }
    else if (a_block_index > b_block_index)
    {
        return 1;
    }
    return 0;
}


struct AssetWriteList* CreateAssetWriteList(uint32_t asset_count)
{
    struct AssetWriteList* awl = (struct AssetWriteList*)(Longtail_Alloc(sizeof(struct AssetWriteList) + sizeof(uint32_t) * asset_count + sizeof(uint32_t) * asset_count));
    awl->m_BlockJobCount = 0;
    awl->m_AssetJobCount = 0;
    awl->m_BlockJobAssetIndexes = (uint32_t*)&awl[1];
    awl->m_AssetIndexJobs = &awl->m_BlockJobAssetIndexes[asset_count];
    return awl;
}

struct AssetWriteList* BuildAssetWriteList(
    uint32_t asset_count,
    const uint32_t* optional_asset_indexes,
    uint32_t* name_offsets,
    const char* name_data,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* asset_chunk_counts,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_indexes,
    struct ContentLookup* cl)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || name_offsets != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || name_data != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || chunk_hashes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || asset_chunk_counts != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || asset_chunk_index_starts != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_count == 0 || asset_chunk_indexes != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(cl != 0, return 0);

    struct AssetWriteList* awl = CreateAssetWriteList(asset_count);

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        uint32_t asset_index = optional_asset_indexes ? optional_asset_indexes[i] : i;
        const char* path = &name_data[name_offsets[asset_index]];
        uint32_t chunk_count = asset_chunk_counts[asset_index];
        uint32_t asset_chunk_offset = asset_chunk_index_starts[asset_index];
        if (chunk_count == 0)
        {
            awl->m_AssetIndexJobs[awl->m_AssetJobCount] = asset_index;
            ++awl->m_AssetJobCount;
            continue;
        }
        uint32_t chunk_index = asset_chunk_indexes[asset_chunk_offset];
        TLongtail_Hash chunk_hash = chunk_hashes[chunk_index];
        intptr_t find_i = hmgeti(cl->m_ChunkHashToBlockIndex, chunk_hash);
        if (find_i == -1)
        {
            LONGTAIL_LOG("WriteVersion: Failed to find chunk 0x%" PRIx64 " in content index for asset `%s`\n", chunk_hash, path)
            Longtail_Free(awl);
            awl = 0;
            return 0;
        }
        uint64_t content_block_index = cl->m_ChunkHashToBlockIndex[find_i].value;
        int is_block_job = 1;
        for (uint32_t c = 1; c < chunk_count; ++c)
        {
            uint32_t next_chunk_index = asset_chunk_indexes[asset_chunk_offset + c];
            TLongtail_Hash next_chunk_hash = chunk_hashes[next_chunk_index];
            intptr_t find_i = hmgeti(cl->m_ChunkHashToBlockIndex, next_chunk_hash);
            if (find_i == -1)
            {
                LONGTAIL_LOG("WriteVersion: Failed to find chunk 0x%" PRIx64 " in content index for asset `%s`\n", next_chunk_hash, path)
                Longtail_Free(awl);
                awl = 0;
                return 0;
            }
            uint64_t next_content_block_index = cl->m_ChunkHashToBlockIndex[find_i].value;
            if (content_block_index != next_content_block_index)
            {
                is_block_job = 0;
                // We don't break here since we want to validate that all the chunks are in the content index
            }
        }

        if (is_block_job)
        {
            awl->m_BlockJobAssetIndexes[awl->m_BlockJobCount] = asset_index;
            ++awl->m_BlockJobCount;
        }
        else
        {
            awl->m_AssetIndexJobs[awl->m_AssetJobCount] = asset_index;
            ++awl->m_AssetJobCount;
        }
    }

    struct BlockJobCompareContext block_job_compare_context = {
            awl,    // m_AssetWriteList
            asset_chunk_index_starts,
            chunk_hashes,   // chunk_hashes
            cl  // cl
        };
    qsort_s(awl->m_BlockJobAssetIndexes, (size_t)awl->m_BlockJobCount, sizeof(uint32_t), BlockJobCompare, &block_job_compare_context);
    return awl;
}

struct DecompressBlockContext
{
    struct StorageAPI* m_StorageAPI;
    struct CompressionRegistry* m_CompressonRegistry;
    const char* m_ContentFolder;
    uint64_t m_BlockHash;
    void* m_UncompressedBlockData;
};

static void DecompressBlock(void* c)
{
    struct DecompressBlockContext* context = (struct DecompressBlockContext*)c;
    context->m_UncompressedBlockData = ReadBlockData(
        context->m_StorageAPI,
        context->m_CompressonRegistry,
        context->m_ContentFolder,
        context->m_BlockHash);
    if (!context->m_UncompressedBlockData)
    {
        LONGTAIL_LOG("DecompressBlock: Failed to decompress block 0x%" PRIx64 " in content `%s`\n", context->m_BlockHash, context->m_ContentFolder)
    }
}

int WriteAssets(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const struct VersionIndex* optional_base_version,
    const char* content_path,
    const char* version_path,
    struct ContentLookup* content_lookup,
    struct AssetWriteList* awl)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_registry != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_lookup != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(awl != 0, return 0);

    const uint32_t worker_count = job_api->GetWorkerCount(job_api) + 2;
    const uint32_t max_parallell_decompress_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;

    uint32_t asset_job_count = 0;
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        uint32_t asset_index = awl->m_AssetIndexJobs[a];
        uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t chunk_start_index_offset = chunk_index_start;
        uint32_t chunk_index_end = chunk_index_start + version_index->m_AssetChunkCounts[asset_index];
        uint32_t chunk_index_offset = chunk_start_index_offset;

        if (chunk_index_offset == chunk_index_end)
        {
            asset_job_count += 1;   // Write job
            continue;
        }

        while(chunk_index_offset != chunk_index_end)
        {
            uint32_t decompress_job_count = 0;
            TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
            while (chunk_index_offset != chunk_index_end && decompress_job_count < max_parallell_decompress_jobs)
            {
                uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
                TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
                intptr_t tmp;
                uint64_t block_index = hmget_ts(content_lookup->m_ChunkHashToBlockIndex, chunk_hash, tmp);
                TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
                int has_block = 0;
                for (uint32_t d = 0; d < decompress_job_count; ++d)
                {
                    if (block_hashes[d] == block_hash)
                    {
                        has_block = 1;
                        break;
                    }
                }
                if (!has_block)
                {
                    block_hashes[decompress_job_count++] = block_hash;
                }
                ++chunk_index_offset;
            }
            asset_job_count += 1;   // Write job
            asset_job_count += 1;   // Sync job
            asset_job_count += decompress_job_count;
        }
    }

    if (!job_api->ReserveJobs(job_api, (awl->m_BlockJobCount * 2) + asset_job_count))
    {
        LONGTAIL_LOG("WriteAssets: Failed to reserve %u jobs for folder `%s`\n", awl->m_BlockJobCount + awl->m_AssetJobCount, version_path)
        Longtail_Free(awl);
        awl = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return 0;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    uint32_t b = 0;
    uint32_t block_job_count = 0;
    while (b < awl->m_BlockJobCount)
    {
        uint32_t asset_index = awl->m_BlockJobAssetIndexes[b];
        TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
        uint64_t block_index = hmget(content_lookup->m_ChunkHashToBlockIndex, first_chunk_hash);

        struct WriteAssetsFromBlockJob* job = &block_jobs[block_job_count++];
        struct BlockDecompressorJob* block_job = &job->m_DecompressBlockJob;
        block_job->m_ContentStorageAPI = content_storage_api;
        block_job->m_CompressionRegistry = compression_registry;
        block_job->m_ContentFolder = content_path;
        block_job->m_BlockHash = content_index->m_BlockHashes[block_index];
        JobAPI_JobFunc decompress_funcs[1] = { BlockDecompressor };
        void* decompress_ctxs[1] = {block_job};
        JobAPI_Jobs decompression_job = job_api->CreateJobs(job_api, 1, decompress_funcs, decompress_ctxs);
        LONGTAIL_FATAL_ASSERT_PRIVATE(decompression_job != 0, return 0)

        job->m_ContentStorageAPI = content_storage_api;
        job->m_VersionStorageAPI = version_storage_api;
        job->m_CompressionRegistry = compression_registry;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_ContentFolder = content_path;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = (uint64_t)block_index;
        job->m_ContentChunkLookup = content_lookup->m_ChunkHashToChunkIndex;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[b];

        job->m_AssetCount = 1;
        ++b;
        while (b < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[b];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            intptr_t next_block_index_ptr = hmgeti(content_lookup->m_ChunkHashToBlockIndex, next_first_chunk_hash);
            LONGTAIL_FATAL_ASSERT_PRIVATE(-1 != next_block_index_ptr, return 0)
            uint64_t next_block_index = content_lookup->m_ChunkHashToBlockIndex[next_block_index_ptr].value;
            if (block_index != next_block_index)
            {
                break;
            }

            ++job->m_AssetCount;
            ++b;
        }

        JobAPI_JobFunc func[1] = { WriteAssetsFromBlock };
        void* ctx[1] = { job };

        JobAPI_Jobs block_write_job = job_api->CreateJobs(job_api, 1, func, ctx);
        LONGTAIL_FATAL_ASSERT_PRIVATE(block_write_job != 0, return 0)
        job_api->AddDependecies(job_api, 1, block_write_job, 1, decompression_job);
        job_api->ReadyJobs(job_api, 1, decompression_job);
    }
/*
DecompressorCount = blocks_remaning > 8 ? 8 : blocks_remaning

Create Decompressor Tasks [DecompressorCount]
Create WriteSync Task
Create Write Task
    Depends on Decompressor Tasks [DecompressorCount]
    Depends on WriteSync Task

Ready Decompressor Tasks [DecompressorCount]
Ready WriteSync Task

WaitForAllTasks()

JOBS:

Write Task Execute (When Decompressor Tasks [DecompressorCount] and WriteSync Task is complete)
    NewDecompressorCount = blocks_remaning > 8 ? 8 : blocks_remaning
    if ([DecompressorCount] > 0)
        Create Decompressor Tasks for up to remaining blocks [NewDecompressorCount]
        Create WriteSync Task
        Create Write Task
            Depends on Decompressor Tasks [NewDecompressorCount]
            Depends on WriteSync Task
        Ready Decompressor Tasks [NewDecompressorCount]
    Write and Longtail_Free Decompressed Tasks Data [DecompressorCount] To Disk
    if ([DecompressorCount] > 0)
        Ready WriteSync Task
*/

    struct WritePartialAssetFromBlocksJob* asset_jobs = (struct WritePartialAssetFromBlocksJob*)Longtail_Alloc(sizeof(struct WritePartialAssetFromBlocksJob) * awl->m_AssetJobCount);
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        JobAPI_Jobs write_sync_job = CreatePartialAssetWriteJob(
            content_storage_api,
            version_storage_api,
            compression_registry,
            job_api,
            content_index,
            version_index,
            content_path,
            version_path,
            content_lookup,
            awl->m_AssetIndexJobs[a],
            &asset_jobs[a],
            0,
            (StorageAPI_HOpenFile)0);
        job_api->ReadyJobs(job_api, 1, write_sync_job);
    }

    job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);

    int success = 1;
    for (uint32_t b = 0; b < block_job_count; ++b)
    {
        struct WriteAssetsFromBlockJob* job = &block_jobs[b];
        if (!job->m_Success)
        {
            LONGTAIL_LOG("WriteAssets: Failed to write single block assets content from `%s` to folder `%s`\n", content_path, version_path)
            success = 0;
        }
    }
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        struct WritePartialAssetFromBlocksJob* job = &asset_jobs[a];
        if (!job->m_Success)
        {
            LONGTAIL_LOG("WriteAssets: Failed to write multi block assets content from `%s` to folder `%s`\n", content_path, version_path)
            success = 0;
        }
    }

    Longtail_Free(asset_jobs);
    asset_jobs = 0;
    Longtail_Free(block_jobs);
    block_jobs = 0;

    return success;
}

int WriteVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const char* content_path,
    const char* version_path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_registry != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_path != 0, return 0);

    LONGTAIL_LOG("WriteVersion: Write version from `%s` to `%s`, assets %u, chunks %u\n", content_path, version_path, *version_index->m_AssetCount, *version_index->m_ChunkCount);
    if (*version_index->m_AssetCount == 0)
    {
        return 1;
    }
    struct ContentLookup* content_lookup = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);
    if (!content_lookup)
    {
        LONGTAIL_LOG("WriteVersion: Failed create content lookup for content `%s`\n", content_path);
        return 0;
    }

    uint32_t asset_count = *version_index->m_AssetCount;

    struct AssetWriteList* awl = BuildAssetWriteList(
        asset_count,
        0,
        version_index->m_NameOffsets,
        version_index->m_NameData,
        version_index->m_ChunkHashes,
        version_index->m_AssetChunkCounts,
        version_index->m_AssetChunkIndexStarts,
        version_index->m_AssetChunkIndexes,
        content_lookup);

    if (!awl)
    {
        LONGTAIL_LOG("WriteVersion: Failed to create asset write list for version `%s`\n", content_path)
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return 0;
    }

    int success = WriteAssets(
        content_storage_api,
        version_storage_api,
        compression_registry,
        job_api,
        job_progress_func,
        job_progress_context,
        content_index,
        version_index,
        0,
        content_path,
        version_path,
        content_lookup,
        awl);

    Longtail_Free(awl);
    awl = 0;

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    return success;
}

struct ReadContentContext {
    struct StorageAPI* m_StorageAPI;
    uint32_t m_ReservedPathCount;
    uint32_t m_ReservedPathSize;
    uint32_t m_RootPathLength;
    struct Paths* m_Paths;
    uint64_t m_ChunkCount;
};

void ReadContentAddPath(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(root_path != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(file_name != 0, return);

    struct ReadContentContext* paths_context = (struct ReadContentContext*)context;
    struct StorageAPI* storage_api = paths_context->m_StorageAPI;

    if (is_dir)
    {
        return;
    }

    char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
    struct Paths* paths = paths_context->m_Paths;
    const uint32_t root_path_length = paths_context->m_RootPathLength;
    const char* s = &full_path[root_path_length];
    if (*s == '/')
    {
        ++s;
    }

    paths_context->m_Paths = AppendPath(paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);

    Longtail_Free(full_path);
    full_path = 0;
};

struct ScanBlockJob
{
    struct StorageAPI* m_StorageAPI;
    struct HashAPI* m_HashAPI;
    const char* m_ContentPath;
    const char* m_BlockPath;
    struct BlockIndex* m_BlockIndex;
};

void ScanBlock(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct ScanBlockJob* job = (struct ScanBlockJob*)context;
    struct StorageAPI* storage_api = job->m_StorageAPI;
    const char* content_path = job->m_ContentPath;
    const char* block_path = job->m_BlockPath;
    char* full_block_path = storage_api->ConcatPath(storage_api, content_path, block_path);

    job->m_BlockIndex = 0;

    job->m_BlockIndex = ReadBlockIndex(
        storage_api,
        full_block_path);

    Longtail_Free(full_block_path);
    full_block_path = 0;
}

struct ContentIndex* ReadContent(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* content_path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_path != 0, return 0);

    LONGTAIL_LOG("ReadContent: Reading from `%s`\n", content_path)

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    struct ReadContentContext context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(content_path)), paths, 0};
    if(!RecurseTree(storage_api, content_path, ReadContentAddPath, &context))
    {
        LONGTAIL_LOG("ReadContent: Failed to scan folder `%s`\n", content_path)
        Longtail_Free(context.m_Paths);
        context.m_Paths = 0;
        return 0;
    }
    paths = context.m_Paths;
    context.m_Paths = 0;

    if (!job_api->ReserveJobs(job_api, *paths->m_PathCount))
    {
        LONGTAIL_LOG("ReadContent: Failed to reserve jobs for `%s`\n", content_path)
        Longtail_Free(paths);
        paths = 0;
        return 0;
    }

    LONGTAIL_LOG("ReadContent: Scanning %u files from `%s`\n", *paths->m_PathCount, content_path);

    struct ScanBlockJob* jobs = (struct ScanBlockJob*)Longtail_Alloc(sizeof(struct ScanBlockJob) * *paths->m_PathCount);

    for (uint32_t path_index = 0; path_index < *paths->m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &jobs[path_index];
        const char* block_path = &paths->m_Data[paths->m_Offsets[path_index]];
        job->m_BlockIndex = 0;

        job->m_StorageAPI = storage_api;
        job->m_HashAPI = hash_api;
        job->m_ContentPath = content_path;
        job->m_BlockPath = block_path;
        job->m_BlockIndex = 0;

        JobAPI_JobFunc job_func[] = {ScanBlock};
        void* ctx[] = {job};
        JobAPI_Jobs jobs = job_api->CreateJobs(job_api, 1, job_func, ctx);
        LONGTAIL_FATAL_ASSERT_PRIVATE(jobs != 0, return 0)
        job_api->ReadyJobs(job_api, 1, jobs);
    }

    job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);

    uint64_t block_count = 0;
    uint64_t chunk_count = 0;
    for (uint32_t path_index = 0; path_index < *paths->m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &jobs[path_index];
        if (job->m_BlockIndex)
        {
            ++block_count;
            chunk_count += *job->m_BlockIndex->m_ChunkCount;
        }
    }

    LONGTAIL_LOG("ReadContent: Found %" PRIu64 " chunks in %" PRIu64 " blocks from `%s`\n", chunk_count, block_count, content_path);

    size_t content_index_data_size = GetContentIndexDataSize(block_count, chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)Longtail_Alloc(sizeof(struct ContentIndex) + content_index_data_size);
    content_index->m_BlockCount = (uint64_t*) & ((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*) & ((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    InitContentIndex(content_index);

    uint64_t block_offset = 0;
    uint64_t chunk_offset = 0;
    for (uint32_t path_index = 0; path_index < *paths->m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &jobs[path_index];
        if (job->m_BlockIndex)
        {
            content_index->m_BlockHashes[block_offset] = *job->m_BlockIndex->m_BlockHash;
            uint32_t block_chunk_count = *job->m_BlockIndex->m_ChunkCount;
            memmove(&content_index->m_ChunkHashes[chunk_offset], job->m_BlockIndex->m_ChunkHashes, sizeof(TLongtail_Hash) * block_chunk_count);
            memmove(&content_index->m_ChunkLengths[chunk_offset], job->m_BlockIndex->m_ChunkSizes, sizeof(uint32_t) * block_chunk_count);
            uint32_t chunk_block_offset = 0;
            for (uint32_t block_chunk_index = 0; block_chunk_index < block_chunk_count; ++block_chunk_index)
            {
                content_index->m_ChunkBlockIndexes[chunk_offset + block_chunk_index] = block_offset;
                content_index->m_ChunkBlockOffsets[chunk_offset + block_chunk_index] = chunk_block_offset;
                chunk_block_offset += content_index->m_ChunkLengths[chunk_offset + block_chunk_index];
            }

            ++block_offset;
            chunk_offset += block_chunk_count;

            Longtail_Free(job->m_BlockIndex);
            job->m_BlockIndex = 0;
        }
    }

    Longtail_Free(jobs);
    jobs = 0;

    Longtail_Free(paths);
    paths = 0;

    return content_index;
}

int CompareHash(const void* a_ptr, const void* b_ptr) 
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(a_ptr != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(b_ptr != 0, return 0);

    TLongtail_Hash a = *((TLongtail_Hash*)a_ptr);
    TLongtail_Hash b = *((TLongtail_Hash*)b_ptr);
    if (a > b) return  1;
    if (a < b) return -1;
    return 0;
}

uint64_t MakeUnique(TLongtail_Hash* hashes, uint64_t count)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(count == 0 || hashes != 0, return 0);

    uint64_t w = 0;
    uint64_t r = 0;
    while (r < count)
    {
        hashes[w] = hashes[r];
        ++r;
        while (r < count && hashes[r - 1] == hashes[r])
        {
            ++r;
        }
        ++w;
    }
    return w;
}

void DiffHashes(
    const TLongtail_Hash* reference_hashes,
    uint64_t reference_hash_count,
    const TLongtail_Hash* new_hashes,
    uint64_t new_hash_count,
    uint64_t* added_hash_count,
    TLongtail_Hash* added_hashes,
    uint64_t* removed_hash_count,
    TLongtail_Hash* removed_hashes)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(reference_hash_count == 0 || reference_hashes != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(new_hash_count == 0 || added_hashes != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(added_hash_count != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE(added_hashes != 0, return);
    LONGTAIL_FATAL_ASSERT_PRIVATE((removed_hash_count == 0 && removed_hashes == 0) || (removed_hash_count != 0 && removed_hashes != 0), return);

    TLongtail_Hash* refs = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    TLongtail_Hash* news = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * new_hash_count));
    memmove(refs, reference_hashes, (size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    memmove(news, new_hashes, (size_t)(sizeof(TLongtail_Hash) * new_hash_count));

    qsort(&refs[0], (size_t)reference_hash_count, sizeof(TLongtail_Hash), CompareHash);
    reference_hash_count = MakeUnique(&refs[0], reference_hash_count);

    qsort(&news[0], (size_t)new_hash_count, sizeof(TLongtail_Hash), CompareHash);
    new_hash_count = MakeUnique(&news[0], new_hash_count);

    uint64_t removed = 0;
    uint64_t added = 0;
    uint64_t ni = 0;
    uint64_t ri = 0;
    while (ri < reference_hash_count && ni < new_hash_count)
    {
        if (refs[ri] == news[ni])
        {
            ++ri;
            ++ni;
            continue;
        }
        else if (refs[ri] < news[ni])
        {
            if (removed_hashes)
            {
                removed_hashes[removed] = refs[ri];
            }
            ++removed;
            ++ri;
        }
        else if (refs[ri] > news[ni])
        {
            added_hashes[added++] = news[ni++];
        }
    }
    while (ni < new_hash_count)
    {
        added_hashes[added++] = news[ni++];
    }
    *added_hash_count = added;
    while (ri < reference_hash_count)
    {
        if (removed_hashes)
        {
            removed_hashes[removed] = refs[ri];
        }
        ++removed;
        ++ri;
    }
    if (removed_hash_count)
    {
        *removed_hash_count = removed;
    }

    Longtail_Free(news);
    news = 0;
    Longtail_Free(refs);
    refs = 0;
}

struct ContentIndex* CreateMissingContent(
    struct HashAPI* hash_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_block_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_chunks_per_block != 0, return 0);

    LONGTAIL_LOG("CreateMissingContent: Checking for %u version chunks in %" PRIu64 " content chunks\n", *version_index->m_ChunkCount, *content_index->m_ChunkCount)
    uint64_t chunk_count = *version_index->m_ChunkCount;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * chunk_count));

    uint64_t added_hash_count = 0;
    DiffHashes(
        content_index->m_ChunkHashes,
        *content_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        chunk_count,
        &added_hash_count,
        added_hashes,
        0,
        0);

    if (added_hash_count == 0)
    {
        Longtail_Free(added_hashes);
        added_hashes = 0;
        struct ContentIndex* diff_content_index = CreateContentIndex(
            hash_api,
            0,
            0,
            0,
            0,
            max_block_size,
            max_chunks_per_block);
        return diff_content_index;
    }

    uint32_t* diff_chunk_sizes = (uint32_t*)Longtail_Alloc((size_t)(sizeof(uint32_t) * added_hash_count));
    uint32_t* diff_chunk_compression_types = (uint32_t*)Longtail_Alloc((size_t)(sizeof(uint32_t) * added_hash_count));

    struct HashToIndexItem* chunk_index_lookup = 0;
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        hmput(chunk_index_lookup, version_index->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        uint64_t chunk_index = hmget(chunk_index_lookup, added_hashes[j]);
        diff_chunk_sizes[j] = version_index->m_ChunkSizes[chunk_index];
        diff_chunk_compression_types[j] = version_index->m_ChunkCompressionTypes[chunk_index];
    }
    hmfree(chunk_index_lookup);
    chunk_index_lookup = 0;

    struct ContentIndex* diff_content_index = CreateContentIndex(
        hash_api,
        added_hash_count,
        added_hashes,
        diff_chunk_sizes,
        diff_chunk_compression_types,
        max_block_size,
        max_chunks_per_block);

    Longtail_Free(diff_chunk_compression_types);
    diff_chunk_compression_types = 0;
    Longtail_Free(diff_chunk_sizes);
    diff_chunk_sizes = 0;
    Longtail_Free(added_hashes);
    added_hashes = 0;

    return diff_content_index;
}

struct Paths* GetPathsForContentBlocks(
    struct ContentIndex* content_index)
{
    if (*content_index->m_BlockCount == 0)
    {
        return CreatePaths(0, 0);
    }
    uint32_t max_path_count = *content_index->m_BlockCount;
    uint32_t max_path_data_size = max_path_count * (MAX_BLOCK_NAME_LENGTH + 4);
    struct Paths* paths = CreatePaths(max_path_count, max_path_data_size);
    for (uint64_t b = 0; b < *content_index->m_BlockCount; ++b)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHashes[b];
        char block_name[MAX_BLOCK_NAME_LENGTH];
        GetBlockName(block_hash, block_name);
        strcat(block_name, ".lrb");
        paths = AppendPath(paths, block_name, &max_path_count, &max_path_data_size, 0, (MAX_BLOCK_NAME_LENGTH + 4) * 32);
    }
    return paths;
}

struct ContentIndex* RetargetContent(
    const struct ContentIndex* reference_content_index,
    const struct ContentIndex* content_index)
{
    struct HashToIndexItem* chunk_to_remote_block_index_lookup = 0;
    for (uint64_t i = 0; i < *reference_content_index->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = reference_content_index->m_ChunkHashes[i];
        uint64_t block_index = reference_content_index->m_ChunkBlockIndexes[i];
        hmput(chunk_to_remote_block_index_lookup, chunk_hash, block_index);
    }

    TLongtail_Hash* requested_block_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * *reference_content_index->m_BlockCount);
    uint64_t requested_block_count = 0;
    struct HashToIndexItem* requested_blocks_lookup = 0;
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
        intptr_t remote_block_index_ptr = hmgeti(chunk_to_remote_block_index_lookup, chunk_hash);
        if (remote_block_index_ptr == -1)
        {
            LONGTAIL_LOG("RetargetContent: reference content does not contain the chunk 0x%" PRIx64 "\n", chunk_hash);
            hmfree(requested_blocks_lookup);
            requested_blocks_lookup = 0;
            Longtail_Free(requested_block_hashes);
            requested_block_hashes = 0;
            hmfree(chunk_to_remote_block_index_lookup);
            chunk_to_remote_block_index_lookup = 0;
            return 0;
        }
        uint64_t remote_block_index = chunk_to_remote_block_index_lookup[remote_block_index_ptr].value;
        TLongtail_Hash remote_block_hash = reference_content_index->m_BlockHashes[remote_block_index];

        intptr_t request_block_index_ptr = hmgeti(requested_blocks_lookup, remote_block_hash);
        if (-1 == request_block_index_ptr)
        {
            requested_block_hashes[requested_block_count] = remote_block_hash;
            hmput(requested_blocks_lookup, remote_block_hash, requested_block_count);
            ++requested_block_count;
        }
    }
    hmfree(chunk_to_remote_block_index_lookup);
    chunk_to_remote_block_index_lookup = 0;

    uint64_t chunk_count = 0;
    for (uint64_t c = 0; c < *reference_content_index->m_ChunkCount; ++c)
    {
        TLongtail_Hash block_hash = reference_content_index->m_BlockHashes[reference_content_index->m_ChunkBlockIndexes[c]];
        if (-1 == hmgeti(requested_blocks_lookup, block_hash))
        {
            continue;
        }
        ++chunk_count;
    }

    size_t content_index_size = GetContentIndexSize(requested_block_count, chunk_count);
    struct ContentIndex* resulting_content_index = (struct ContentIndex*)Longtail_Alloc(content_index_size);

    resulting_content_index->m_BlockCount = (uint64_t*)&((char*)resulting_content_index)[sizeof(struct ContentIndex)];
    resulting_content_index->m_ChunkCount = (uint64_t*)&((char*)resulting_content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *resulting_content_index->m_BlockCount = requested_block_count;
    *resulting_content_index->m_ChunkCount = chunk_count;
    InitContentIndex(resulting_content_index);

    memmove(resulting_content_index->m_BlockHashes, requested_block_hashes, sizeof(TLongtail_Hash) * requested_block_count);

    uint64_t chunk_index = 0;
    for (uint64_t c = 0; c < *reference_content_index->m_ChunkCount; ++c)
    {
        TLongtail_Hash block_hash = reference_content_index->m_BlockHashes[reference_content_index->m_ChunkBlockIndexes[c]];
        intptr_t block_index_ptr = hmgeti(requested_blocks_lookup, block_hash);
        if (-1 == block_index_ptr)
        {
            continue;
        }
        TLongtail_Hash chunk_hash = reference_content_index->m_ChunkHashes[c];
        uint32_t chunk_length = reference_content_index->m_ChunkLengths[c];
        uint32_t chunk_block_offset = reference_content_index->m_ChunkBlockOffsets[c];
        uint64_t block_index = requested_blocks_lookup[block_index_ptr].value;
        resulting_content_index->m_ChunkBlockIndexes[chunk_index] = block_index;
        resulting_content_index->m_ChunkHashes[chunk_index] = chunk_hash;
        resulting_content_index->m_ChunkBlockOffsets[chunk_index] = chunk_block_offset;
        resulting_content_index->m_ChunkLengths[chunk_index] = chunk_length;
        ++chunk_index;
    }

    hmfree(requested_blocks_lookup);
    requested_blocks_lookup = 0;
    Longtail_Free(requested_block_hashes);
    requested_block_hashes = 0;
    return resulting_content_index;
}

// TODO: This could be more efficient - if a block exists in both local_content_index and remote_content_index it will
// be present twice in the resulting content index. This is fine but a waste.
struct ContentIndex* MergeContentIndex(
    struct ContentIndex* local_content_index,
    struct ContentIndex* remote_content_index)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(local_content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(remote_content_index != 0, return 0);

    uint64_t local_block_count = *local_content_index->m_BlockCount;
    uint64_t remote_block_count = *remote_content_index->m_BlockCount;
    uint64_t local_chunk_count = *local_content_index->m_ChunkCount;
    uint64_t remote_chunk_count = *remote_content_index->m_ChunkCount;
    uint64_t block_count = local_block_count + remote_block_count;
    uint64_t chunk_count = local_chunk_count + remote_chunk_count;
    size_t content_index_size = GetContentIndexSize(block_count, chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)Longtail_Alloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    InitContentIndex(content_index);

    for (uint64_t b = 0; b < local_block_count; ++b)
    {
        content_index->m_BlockHashes[b] = local_content_index->m_BlockHashes[b];
    }
    for (uint64_t b = 0; b < remote_block_count; ++b)
    {
        content_index->m_BlockHashes[local_block_count + b] = remote_content_index->m_BlockHashes[b];
    }
    for (uint64_t a = 0; a < local_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[a] = local_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[a] = local_content_index->m_ChunkBlockIndexes[a];
        content_index->m_ChunkBlockOffsets[a] = local_content_index->m_ChunkBlockOffsets[a];
        content_index->m_ChunkLengths[a] = local_content_index->m_ChunkLengths[a];
    }
    for (uint64_t a = 0; a < remote_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[local_chunk_count + a] = remote_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[local_chunk_count + a] = local_block_count + remote_content_index->m_ChunkBlockIndexes[a];
        content_index->m_ChunkBlockOffsets[local_chunk_count + a] = remote_content_index->m_ChunkBlockOffsets[a];
        content_index->m_ChunkLengths[local_chunk_count + a] = remote_content_index->m_ChunkLengths[a];
    }
    return content_index;
}

static int CompareHashes(const void* a_ptr, const void* b_ptr)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(a_ptr != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(b_ptr != 0, return 0);

    TLongtail_Hash a = *(TLongtail_Hash*)a_ptr;
    TLongtail_Hash b = *(TLongtail_Hash*)b_ptr;
    return (a > b) ? 1 : (a < b) ? -1 : 0;
}
/*
static int CompareIndexs(const void* a_ptr, const void* b_ptr)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(a_ptr != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(b_ptr != 0, return 0);

    int64_t a = *(uint32_t*)a_ptr;
    int64_t b = *(uint32_t*)b_ptr;
    return (int)a - b;
}
*/

#if defined(__clang__)
static int SortPathShortToLong(const void* a_ptr, const void* b_ptr, void* context)
#elif defined(_MSC_VER) || defined(__GNUC__)
static int SortPathShortToLong(void* context, const void* a_ptr, const void* b_ptr)
#endif
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(a_ptr != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(b_ptr != 0, return 0);

    const struct VersionIndex* version_index = (const struct VersionIndex*)context;
    uint32_t a = *(uint32_t*)a_ptr;
    uint32_t b = *(uint32_t*)b_ptr;
    const char* a_path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
    const char* b_path = &version_index->m_NameData[version_index->m_NameOffsets[b]];
    size_t a_len = strlen(a_path);
    size_t b_len = strlen(b_path);
    return (a_len > b_len) ? 1 : (a_len < b_len) ? -1 : 0;
}

#if defined(__clang__)
static int SortPathLongToShort(const void* a_ptr, const void* b_ptr, void* context)
#elif defined(_MSC_VER) || defined(__GNUC__)
static int SortPathLongToShort(void* context, const void* a_ptr, const void* b_ptr)
#endif
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(a_ptr != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(b_ptr != 0, return 0);

    const struct VersionIndex* version_index = (const struct VersionIndex*)context;
    uint32_t a = *(uint32_t*)a_ptr;
    uint32_t b = *(uint32_t*)b_ptr;
    const char* a_path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
    const char* b_path = &version_index->m_NameData[version_index->m_NameOffsets[b]];
    size_t a_len = strlen(a_path);
    size_t b_len = strlen(b_path);
    return (a_len < b_len) ? 1 : (a_len > b_len) ? -1 : 0;
}

size_t GetVersionDiffDataSize(uint32_t removed_count, uint32_t added_count, uint32_t modified_count)
{
    return
        sizeof(uint32_t) +                  // m_SourceRemovedCount
        sizeof(uint32_t) +                  // m_TargetAddedCount
        sizeof(uint32_t) +                  // m_ModifiedCount
        sizeof(uint32_t) * removed_count +  // m_SourceRemovedAssetIndexes
        sizeof(uint32_t) * added_count +    // m_TargetAddedAssetIndexes
        sizeof(uint32_t) * modified_count + // m_SourceModifiedAssetIndexes
        sizeof(uint32_t) * modified_count;  // m_TargetModifiedAssetIndexes
}

size_t GetVersionDiffSize(uint32_t removed_count, uint32_t added_count, uint32_t modified_count)
{
    return sizeof(struct VersionDiff) +
        GetVersionDiffDataSize(removed_count, added_count, modified_count);
}

void InitVersionDiff(struct VersionDiff* version_diff)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_diff != 0, return);

    char* p = (char*)version_diff;
    p += sizeof(struct VersionDiff);

    version_diff->m_SourceRemovedCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    version_diff->m_TargetAddedCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    version_diff->m_ModifiedCount = (uint32_t*)p;
    p += sizeof(uint32_t);

    uint32_t removed_count = *version_diff->m_SourceRemovedCount;
    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_count = *version_diff->m_ModifiedCount;

    version_diff->m_SourceRemovedAssetIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * removed_count;

    version_diff->m_TargetAddedAssetIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * added_count;

    version_diff->m_SourceModifiedAssetIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * modified_count;

    version_diff->m_TargetModifiedAssetIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * modified_count;
}

struct VersionDiff* CreateVersionDiff(
    const struct VersionIndex* source_version,
    const struct VersionIndex* target_version)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(source_version != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(target_version != 0, return 0);

    struct HashToIndexItem* source_path_hash_to_index = 0;
    struct HashToIndexItem* target_path_hash_to_index = 0;

    uint32_t source_asset_count = *source_version->m_AssetCount;
    uint32_t target_asset_count = *target_version->m_AssetCount;

    TLongtail_Hash* source_path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof (TLongtail_Hash) * source_asset_count);
    TLongtail_Hash* target_path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof (TLongtail_Hash) * target_asset_count);

    for (uint32_t i = 0; i < source_asset_count; ++i)
    {
        TLongtail_Hash path_hash = source_version->m_PathHashes[i];
        source_path_hashes[i] = path_hash;
        hmput(source_path_hash_to_index, path_hash, i);
    }

    for (uint32_t i = 0; i < target_asset_count; ++i)
    {
        TLongtail_Hash path_hash = target_version->m_PathHashes[i];
        target_path_hashes[i] = path_hash;
        hmput(target_path_hash_to_index, path_hash, i);
    }

    qsort(source_path_hashes, source_asset_count, sizeof(TLongtail_Hash), CompareHashes);
    qsort(target_path_hashes, target_asset_count, sizeof(TLongtail_Hash), CompareHashes);

    uint32_t* removed_source_asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * source_asset_count);
    uint32_t* added_target_asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * target_asset_count);

    const uint32_t max_modified_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    uint32_t* modified_source_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_count);
    uint32_t* modified_target_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_count);

    uint32_t source_removed_count = 0;
    uint32_t target_added_count = 0;
    uint32_t modified_count = 0;

    uint32_t source_index = 0;
    uint32_t target_index = 0;
    while (source_index < source_asset_count && target_index < target_asset_count)
    {
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
        uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);

        const char* source_path = &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]];
        const char* target_path = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];

        if (source_path_hash == target_path_hash)
        {
            TLongtail_Hash source_content_hash = source_version->m_ContentHashes[source_asset_index];
            TLongtail_Hash target_content_hash = target_version->m_ContentHashes[target_asset_index];
            if (source_content_hash != target_content_hash)
            {
                modified_source_indexes[modified_count] = source_asset_index;
                modified_target_indexes[modified_count] = target_asset_index;
                ++modified_count;
                LONGTAIL_LOG("CreateVersionDiff: Mismatching content for asset `%s`\n", source_path)
            }
            ++source_index;
            ++target_index;
        }
        else if (source_path_hash < target_path_hash)
        {
            uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
            LONGTAIL_LOG("CreateVersionDiff: Removed asset `%s`\n", source_path)
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        }
        else
        {
            uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
            LONGTAIL_LOG("CreateVersionDiff: Added asset `%s`\n", target_path)
            added_target_asset_indexes[target_added_count] = target_asset_index;
            ++target_added_count;
            ++target_index;
        } 
    }
    while (source_index < source_asset_count)
    {
        // source_path_hash removed
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
        const char* source_path = &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]];
        LONGTAIL_LOG("CreateVersionDiff: Removed asset `%s`\n", source_path)
        removed_source_asset_indexes[source_removed_count] = source_asset_index;
        ++source_removed_count;
        ++source_index;
    }
    while (target_index < target_asset_count)
    {
        // target_path_hash added
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
        const char* target_path = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];
        LONGTAIL_LOG("CreateVersionDiff: Added asset `%s`\n", target_path)
        added_target_asset_indexes[target_added_count] = target_asset_index;
        ++target_added_count;
        ++target_index;
    }
    if (source_removed_count > 0)
    {
        LONGTAIL_LOG("CreateVersionDiff: Found %u removed assets\n", source_removed_count)
    }
    if (target_added_count > 0)
    {
        LONGTAIL_LOG("CreateVersionDiff: Found %u added assets\n", target_added_count)
    }
    if (modified_count > 0)
    {
        LONGTAIL_LOG("CreateVersionDiff: Mismatching content for %u assets found\n", modified_count)
    }

    struct VersionDiff* version_diff = (struct VersionDiff*)Longtail_Alloc(GetVersionDiffSize(source_removed_count, target_added_count, modified_count));
    uint32_t* counts_ptr = (uint32_t*)&version_diff[1];
    counts_ptr[0] = source_removed_count;
    counts_ptr[1] = target_added_count;
    counts_ptr[2] = modified_count;
    InitVersionDiff(version_diff);

    memmove(version_diff->m_SourceRemovedAssetIndexes, removed_source_asset_indexes, sizeof(uint32_t) * source_removed_count);
    memmove(version_diff->m_TargetAddedAssetIndexes, added_target_asset_indexes, sizeof(uint32_t) * target_added_count);
    memmove(version_diff->m_SourceModifiedAssetIndexes, modified_source_indexes, sizeof(uint32_t) * modified_count);
    memmove(version_diff->m_TargetModifiedAssetIndexes, modified_target_indexes, sizeof(uint32_t) * modified_count);

    qsort_s(version_diff->m_SourceRemovedAssetIndexes, source_removed_count, sizeof(uint32_t), SortPathLongToShort, (void*)source_version);
    qsort_s(version_diff->m_TargetAddedAssetIndexes, target_added_count, sizeof(uint32_t), SortPathShortToLong, (void*)target_version);

    Longtail_Free(removed_source_asset_indexes);
    removed_source_asset_indexes = 0;

    Longtail_Free(added_target_asset_indexes);
    added_target_asset_indexes = 0;

    Longtail_Free(modified_source_indexes);
    modified_source_indexes = 0;

    Longtail_Free(modified_target_indexes);
    modified_target_indexes = 0;

    Longtail_Free(target_path_hashes);
    target_path_hashes = 0;

    Longtail_Free(source_path_hashes);
    source_path_hashes = 0;

    hmfree(target_path_hash_to_index);
    target_path_hash_to_index = 0;

    hmfree(source_path_hash_to_index);
    source_path_hash_to_index = 0;

    return version_diff;
}

int ChangeVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct CompressionRegistry* compression_registry,
    const struct ContentIndex* content_index,
    const struct VersionIndex* source_version,
    const struct VersionIndex* target_version,
    const struct VersionDiff* version_diff,
    const char* content_path,
    const char* version_path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_registry != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(source_version != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(target_version != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_diff != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_path != 0, return 0);

    LONGTAIL_LOG("ChangeVersion: Removing %u assets, adding %u assets and modifying %u assets in `%s` from `%s`\n", *version_diff->m_SourceRemovedCount, *version_diff->m_TargetAddedCount, *version_diff->m_ModifiedCount, version_path, content_path);

    if (!EnsureParentPathExists(version_storage_api, version_path))
    {
        LONGTAIL_LOG("ChangeVersion: Failed to create parent path for `%s`\n", version_path);
        return 0;
    }
    if (!SafeCreateDir(version_storage_api, version_path))
    {
        LONGTAIL_LOG("ChangeVersion: Failed to create folder `%s`\n", version_path);
        return 0;
    }
    struct ContentLookup* content_lookup = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);
    if (!content_lookup)
    {
        LONGTAIL_LOG("ChangeVersion: Failed create content lookup for content `%s`\n", content_path);
        return 0;
    }

    for (uint32_t i = 0; i < *target_version->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[i];
        intptr_t chunk_content_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
        if (-1 == chunk_content_index_ptr)
        {
            LONGTAIL_LOG("ChangeVersion: Not all chunks in target version in `%s` is available in content folder `%s`\n", version_path, content_path);
            DeleteContentLookup(content_lookup);
            content_lookup = 0;
            return 0;
       }
    }

    for (uint32_t i = 0; i < *version_diff->m_TargetAddedCount; ++i)
    {
        uint32_t target_asset_index = version_diff->m_TargetAddedAssetIndexes[i];
        const char* target_name = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];
        uint32_t target_chunk_count = target_version->m_AssetChunkCounts[target_asset_index];
        uint32_t target_chunk_index_start = target_version->m_AssetChunkIndexStarts[target_asset_index];
        for (uint32_t i = 0; i < target_chunk_count; ++i)
        {
            uint32_t target_chunk = target_version->m_AssetChunkIndexes[target_chunk_index_start + i];
            TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[target_chunk];
            intptr_t chunk_content_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
            if (-1 == chunk_content_index_ptr)
            {
                LONGTAIL_LOG("ChangeVersion: Not all chunks for asset `%s` is in target version in `%s` is available in content folder `%s`\n", target_name, version_path, content_path);
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return 0;
           }
        }
    }

    uint32_t retry_count = 10;
    uint32_t successful_remove_count = 0;
    uint32_t removed_count = *version_diff->m_SourceRemovedCount;
    while (successful_remove_count < removed_count)
    {
        --retry_count;
        for (uint32_t r = 0; r < removed_count; ++r)
        {
            uint32_t asset_index = version_diff->m_SourceRemovedAssetIndexes[r];
            const char* asset_path = &source_version->m_NameData[source_version->m_NameOffsets[asset_index]];
            char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            if (IsDirPath(asset_path))
            {
                full_asset_path[strlen(full_asset_path) - 1] = '\0';
                int ok = version_storage_api->RemoveDir(version_storage_api, full_asset_path);
                if (!ok)
                {
                    if (version_storage_api->IsDir(version_storage_api, full_asset_path))
                    {
                        if (!retry_count)
                        {
                            LONGTAIL_LOG("ChangeVersion: Failed to remove directory `%s`\n", full_asset_path);
                            Longtail_Free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(content_lookup);
                            content_lookup = 0;
                            return 0;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        break;
                    }
                }
            }
            else
            {
                int ok = version_storage_api->RemoveFile(version_storage_api, full_asset_path);
                if (!ok)
                {
                    if (version_storage_api->IsFile(version_storage_api, full_asset_path))
                    {
                        if (!retry_count)
                        {
                            LONGTAIL_LOG("ChangeVersion: Failed to remove file `%s`\n", full_asset_path);
                            Longtail_Free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(content_lookup);
                            content_lookup = 0;
                            return 0;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        break;
                    }
                }
            }
            ++successful_remove_count;
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
        }
        if (successful_remove_count < removed_count)
        {
            --retry_count;
            if (retry_count == 1)
            {
                LONGTAIL_LOG("ChangeVersion: Retrying removal of remaning %u assets in `%s`\n", removed_count - successful_remove_count, version_path);
            }
        }
    }

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_count = *version_diff->m_ModifiedCount;
    uint32_t asset_count = added_count + modified_count;

    uint32_t* asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * asset_count);
    for (uint32_t i = 0; i < added_count; ++i)
    {
        asset_indexes[i] = version_diff->m_TargetAddedAssetIndexes[i];
    }
    for (uint32_t i = 0; i < modified_count; ++i)
    {
        asset_indexes[added_count + i] = version_diff->m_TargetModifiedAssetIndexes[i];
    }

    struct AssetWriteList* awl = BuildAssetWriteList(
        asset_count,
        asset_indexes,
        target_version->m_NameOffsets,
        target_version->m_NameData,
        target_version->m_ChunkHashes,
        target_version->m_AssetChunkCounts,
        target_version->m_AssetChunkIndexStarts,
        target_version->m_AssetChunkIndexes,
        content_lookup);

    if (!awl)
    {
        LONGTAIL_LOG("ChangeVersion: Failed to create asset write list for version `%s`\n", content_path)
        Longtail_Free(asset_indexes);
        asset_indexes = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return 0;
    }

    int success = WriteAssets(
        content_storage_api,
        version_storage_api,
        compression_registry,
        job_api,
        job_progress_func,
        job_progress_context,
        content_index,
        target_version,
        source_version,
        content_path,
        version_path,
        content_lookup,
        awl);

    Longtail_Free(asset_indexes);
    asset_indexes = 0;

    Longtail_Free(awl);
    awl = 0;

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    return success;
}

int ValidateContent(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index)
{
    struct ContentLookup* content_lookup = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);

    if (!content_lookup)
    {
        return 0;
    }

    for (uint32_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
    {
        TLongtail_Hash asset_content_hash = version_index->m_ChunkHashes[asset_index];
        uint64_t asset_size = version_index->m_AssetSizes[asset_index];
        uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t first_chunk_index = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunked_size = 0;
        for (uint32_t i = 0; i < chunk_count; ++i)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[first_chunk_index + i];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            asset_chunked_size += chunk_size;
            intptr_t content_chunk_index_ptr = hmget(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
            if (content_chunk_index_ptr == -1)
            {
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return 0;
            }
            if (content_index->m_ChunkHashes[content_chunk_index_ptr] != chunk_hash)
            {
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return 0;
            }
            if (content_index->m_ChunkLengths[content_chunk_index_ptr] != chunk_size)
            {
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return 0;
            }
        }
        if (asset_chunked_size != asset_size)
        {
            DeleteContentLookup(content_lookup);
            content_lookup = 0;
            return 0;
        }
    }

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    return 1;
}

int ValidateVersion(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index)
{
    struct HashToIndexItem* version_chunk_lookup = 0;

    struct ContentLookup* content_lookup = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);

    if (!content_lookup)
    {
        return 0;
    }

    for (uint32_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
    {
        TLongtail_Hash asset_content_hash = version_index->m_ChunkHashes[asset_index];
        uint64_t asset_size = version_index->m_AssetSizes[asset_index];
        uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t first_chunk_index = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunked_size = 0;
        for (uint32_t i = 0; i < chunk_count; ++i)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[first_chunk_index + i];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            hmput(version_chunk_lookup, chunk_hash, chunk_index);
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            asset_chunked_size += chunk_size;
        }
        if (asset_chunked_size != asset_size)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return 0;
        }
    }

    for (uint64_t chunk_index = 0; chunk_index < *content_index->m_ChunkCount; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t version_chunk_index = hmgeti(version_chunk_lookup, chunk_hash);
        if (version_chunk_index == -1)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return 0;
        }
        if (version_index->m_ChunkHashes[version_chunk_index] != chunk_hash)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return 0;
        }
        if (version_index->m_ChunkSizes[version_chunk_index] != chunk_size)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return 0;
        }
    }

    hmfree(version_chunk_lookup);
    version_chunk_lookup = 0;

    return 1;
}


static uint32_t hashTable[] = {
    0x458be752, 0xc10748cc, 0xfbbcdbb8, 0x6ded5b68,
    0xb10a82b5, 0x20d75648, 0xdfc5665f, 0xa8428801,
    0x7ebf5191, 0x841135c7, 0x65cc53b3, 0x280a597c,
    0x16f60255, 0xc78cbc3e, 0x294415f5, 0xb938d494,
    0xec85c4e6, 0xb7d33edc, 0xe549b544, 0xfdeda5aa,
    0x882bf287, 0x3116737c, 0x05569956, 0xe8cc1f68,
    0x0806ac5e, 0x22a14443, 0x15297e10, 0x50d090e7,
    0x4ba60f6f, 0xefd9f1a7, 0x5c5c885c, 0x82482f93,
    0x9bfd7c64, 0x0b3e7276, 0xf2688e77, 0x8fad8abc,
    0xb0509568, 0xf1ada29f, 0xa53efdfe, 0xcb2b1d00,
    0xf2a9e986, 0x6463432b, 0x95094051, 0x5a223ad2,
    0x9be8401b, 0x61e579cb, 0x1a556a14, 0x5840fdc2,
    0x9261ddf6, 0xcde002bb, 0x52432bb0, 0xbf17373e,
    0x7b7c222f, 0x2955ed16, 0x9f10ca59, 0xe840c4c9,
    0xccabd806, 0x14543f34, 0x1462417a, 0x0d4a1f9c,
    0x087ed925, 0xd7f8f24c, 0x7338c425, 0xcf86c8f5,
    0xb19165cd, 0x9891c393, 0x325384ac, 0x0308459d,
    0x86141d7e, 0xc922116a, 0xe2ffa6b6, 0x53f52aed,
    0x2cd86197, 0xf5b9f498, 0xbf319c8f, 0xe0411fae,
    0x977eb18c, 0xd8770976, 0x9833466a, 0xc674df7f,
    0x8c297d45, 0x8ca48d26, 0xc49ed8e2, 0x7344f874,
    0x556f79c7, 0x6b25eaed, 0xa03e2b42, 0xf68f66a4,
    0x8e8b09a2, 0xf2e0e62a, 0x0d3a9806, 0x9729e493,
    0x8c72b0fc, 0x160b94f6, 0x450e4d3d, 0x7a320e85,
    0xbef8f0e1, 0x21d73653, 0x4e3d977a, 0x1e7b3929,
    0x1cc6c719, 0xbe478d53, 0x8d752809, 0xe6d8c2c6,
    0x275f0892, 0xc8acc273, 0x4cc21580, 0xecc4a617,
    0xf5f7be70, 0xe795248a, 0x375a2fe9, 0x425570b6,
    0x8898dcf8, 0xdc2d97c4, 0x0106114b, 0x364dc22f,
    0x1e0cad1f, 0xbe63803c, 0x5f69fac2, 0x4d5afa6f,
    0x1bc0dfb5, 0xfb273589, 0x0ea47f7b, 0x3c1c2b50,
    0x21b2a932, 0x6b1223fd, 0x2fe706a8, 0xf9bd6ce2,
    0xa268e64e, 0xe987f486, 0x3eacf563, 0x1ca2018c,
    0x65e18228, 0x2207360a, 0x57cf1715, 0x34c37d2b,
    0x1f8f3cde, 0x93b657cf, 0x31a019fd, 0xe69eb729,
    0x8bca7b9b, 0x4c9d5bed, 0x277ebeaf, 0xe0d8f8ae,
    0xd150821c, 0x31381871, 0xafc3f1b0, 0x927db328,
    0xe95effac, 0x305a47bd, 0x426ba35b, 0x1233af3f,
    0x686a5b83, 0x50e072e5, 0xd9d3bb2a, 0x8befc475,
    0x487f0de6, 0xc88dff89, 0xbd664d5e, 0x971b5d18,
    0x63b14847, 0xd7d3c1ce, 0x7f583cf3, 0x72cbcb09,
    0xc0d0a81c, 0x7fa3429b, 0xe9158a1b, 0x225ea19a,
    0xd8ca9ea3, 0xc763b282, 0xbb0c6341, 0x020b8293,
    0xd4cd299d, 0x58cfa7f8, 0x91b4ee53, 0x37e4d140,
    0x95ec764c, 0x30f76b06, 0x5ee68d24, 0x679c8661,
    0xa41979c2, 0xf2b61284, 0x4fac1475, 0x0adb49f9,
    0x19727a23, 0x15a7e374, 0xc43a18d5, 0x3fb1aa73,
    0x342fc615, 0x924c0793, 0xbee2d7f0, 0x8a279de9,
    0x4aa2d70c, 0xe24dd37f, 0xbe862c0b, 0x177c22c2,
    0x5388e5ee, 0xcd8a7510, 0xf901b4fd, 0xdbc13dbc,
    0x6c0bae5b, 0x64efe8c7, 0x48b02079, 0x80331a49,
    0xca3d8ae6, 0xf3546190, 0xfed7108b, 0xc49b941b,
    0x32baf4a9, 0xeb833a4a, 0x88a3f1a5, 0x3a91ce0a,
    0x3cc27da1, 0x7112e684, 0x4a3096b1, 0x3794574c,
    0xa3c8b6f3, 0x1d213941, 0x6e0a2e00, 0x233479f1,
    0x0f4cd82f, 0x6093edd2, 0x5d7d209e, 0x464fe319,
    0xd4dcac9e, 0x0db845cb, 0xfb5e4bc3, 0xe0256ce1,
    0x09fb4ed1, 0x0914be1e, 0xa5bdb2c3, 0xc6eb57bb,
    0x30320350, 0x3f397e91, 0xa67791bc, 0x86bc0e2c,
    0xefa0a7e2, 0xe9ff7543, 0xe733612c, 0xd185897b,
    0x329e5388, 0x91dd236b, 0x2ecb0d93, 0xf4d82a3d,
    0x35b5c03f, 0xe4e606f0, 0x05b21843, 0x37b45964,
    0x5eff22f4, 0x6027f4cc, 0x77178b3c, 0xae507131,
    0x7bf7cabc, 0xf9c18d66, 0x593ade65, 0xd95ddf11,
};

struct ChunkerWindow
{
    uint8_t* buf;
    uint32_t len;
    uint32_t m_ScanPosition;
};

struct Array
{
    uint8_t* data;
    uint32_t len;
};

struct Chunker
{
    struct ChunkerParams params;
    struct Array buf;
    uint32_t off;
    uint32_t hValue;
    uint8_t hWindow[ChunkerWindowSize];
    int32_t hIdx;
    uint32_t hDiscriminator;
    Chunker_Feeder fFeeder;
    void* cFeederContext;
    uint64_t processed_count;
};

uint32_t discriminatorFromAvg(double avg)
{
    return (uint32_t)(avg / (-1.42888852e-7*avg + 1.33237515));
}

struct Chunker* CreateChunker(
    struct ChunkerParams* params,
    Chunker_Feeder feeder,
    void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(params != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(feeder != 0, return 0);

    if (params->min < ChunkerWindowSize)
    {
        LONGTAIL_LOG("Chunker: Min chunk size too small, must be over %u\n", ChunkerWindowSize);
        return 0;
    }
    if (params->min > params->max)
    {
        LONGTAIL_LOG("Chunker: Min (%u) chunk size must not be greater than max (%u)\n", (uint32_t)params->min, (uint32_t)params->max);
        return 0;
    }
    if (params->min > params->avg)
    {
        LONGTAIL_LOG("Chunker: Min (%u) chunk size must not be greater than avg (%u)\n", (uint32_t)params->min, (uint32_t)params->avg);
        return 0;
    }
    if (params->avg > params->max)
    {
        LONGTAIL_LOG("Chunker: Avg (%u) chunk size must not be greater than max (%u)\n", (uint32_t)params->avg, (uint32_t)params->max);
        return 0;
    }
    struct Chunker* c = (struct Chunker*)Longtail_Alloc((size_t)((sizeof(struct Chunker) + params->max)));
    c->params = *params;
    c->buf.data = (uint8_t*)&c[1];
    c->buf.len = 0;
    c->off = 0;
    c->hValue = 0;
    c->hIdx = 0;
    c->hDiscriminator = discriminatorFromAvg((double)params->avg);
    c->fFeeder = feeder,
    c->cFeederContext = context;
    c->processed_count = 0;
    return c;
}

void FeedChunker(struct Chunker* c)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(c != 0, return);

    if (c->off != 0)
    {
        memmove(c->buf.data, &c->buf.data[c->off], c->buf.len - c->off);
        c->processed_count += c->off;
        c->buf.len -= c->off;
        c->off = 0;
    }
    uint32_t feed_max = (uint32_t)(c->params.max - c->buf.len);
    uint32_t feed_count = c->fFeeder(c->cFeederContext, c, feed_max, (char*)&c->buf.data[c->buf.len]);
    c->buf.len += feed_count;
}

#ifndef _MSC_VER
inline uint32_t _rotl(uint32_t x, int shift) {
    shift &= 31;
    if (!shift) return x;
    return (x << shift) | (x >> (32 - shift));
}
#endif // _MSC_VER

struct ChunkRange NextChunk(struct Chunker* c)
{
//    LONGTAIL_FATAL_ASSERT_PRIVATE(c != 0, return 0);

    if (c->buf.len - c->off < c->params.max)
    {
        FeedChunker(c);
    }
    if (c->off == c->buf.len)
    {
        // All done
        struct ChunkRange r = {0, c->processed_count + c->off, 0};
        c->hIdx = 0;
        c->hValue = 0;
        return r;
    }

    uint32_t left = c->buf.len - c->off;
    if (left <= c->params.min)
    {
        // Less than min-size left, just consume it all
        struct ChunkRange r = {&c->buf.data[c->off], c->processed_count + c->off, left};
        c->off += left;
        c->hIdx = 0;
        c->hValue = 0;
        return r;
    }

    struct ChunkRange scoped_data = {&c->buf.data[c->off], c->processed_count + c->off, left};
    {
        struct ChunkRange window = {&scoped_data.buf[c->params.min - ChunkerWindowSize], c->processed_count + c->off + c->params.min - ChunkerWindowSize, ChunkerWindowSize};
        for (uint32_t i = 0; i < ChunkerWindowSize; ++i)
        {
            uint8_t b = window.buf[i];
            c->hValue ^= _rotl(hashTable[b], ChunkerWindowSize-i-1);
            c->hWindow[i] = b;
        }
    }

    uint32_t pos = c->params.min;
    while(1)
    {
        uint8_t in = scoped_data.buf[pos];
        uint8_t out = c->hWindow[c->hIdx];
        c->hWindow[c->hIdx] = in;
        c->hIdx = (c->hIdx == ChunkerWindowSize - 1) ? 0 : c->hIdx + 1;
        c->hValue = _rotl(c->hValue, 1) ^
            _rotl(hashTable[out], ChunkerWindowSize) ^
            hashTable[in];

        ++pos;

        if (pos >= scoped_data.len)
        {
            struct ChunkRange r = {scoped_data.buf, c->processed_count + c->off, pos};
            c->off += pos;
            c->hIdx = 0;
            c->hValue = 0;
            return r;
        }

        if ((c->hValue % c->hDiscriminator) == (c->hDiscriminator - 1))
        {
            struct ChunkRange r = {scoped_data.buf, c->processed_count + c->off, pos};
            c->off += pos;
            c->hIdx = 0;
            c->hValue = 0;
            return r;
        }
    }
}
