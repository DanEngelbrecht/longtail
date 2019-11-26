#include "longtail.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "stb_ds.h"

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
        void* Longtail_NukeMalloc(size_t s)
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
        void Longtail_NukeFree(void* p)
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
            free(r);
        }
#    define LONGTAIL_MALLOC(s) \
        Longtail_NukeMalloc(s)
#    define LONGTAIL_FREE(p) \
        Longtail_NukeFree(p)
#else // defined(LONGTAIL_ASSERTS)
#    define LONGTAIL_FATAL_ASSERT_PRIVATE(x, y)
#    define LONGTAIL_MALLOC(s) \
        malloc(s)
#    define LONGTAIL_FREE(p) \
        free(p)
#endif // defined(LONGTAIL_ASSERTS)

void Longtail_SetAssert(Longtail_Assert assert_func)
{
#if defined(LONGTAIL_ASSERTS)
    Longtail_Assert_private = assert_func;
#else  // defined(LONGTAIL_ASSERTS)
    (void)assert_func;
#endif // defined(LONGTAIL_ASSERTS)
}

#ifndef LONGTAIL_LOG
    #ifdef LONGTAIL_VERBOSE_LOGS
        #define LONGTAIL_LOG(fmt, ...) \
            printf("--- ");printf(fmt, __VA_ARGS__);
    #else
        #define LONGTAIL_LOG(fmt, ...)
    #endif
#endif

#if !defined(LONGTAIL_ATOMICADD)
    #if defined(__clang__) || defined(__GNUC__)
        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (__sync_add_and_fetch (value, amount))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (_InterlockedExchangeAdd((volatile LONG *)value, amount) + amount)
        #define qsort_r qsort_s
    #else
        inline int32_t LONGTAIL_NonAtomicAdd(volatile int32_t* store, int32_t value) { *store += value; return *store; }
        #define LONGTAIL_ATOMICADD_PRIVATE(value, amount) (LONGTAIL_NonAtomicAdd(value, amount))
    #endif
#else
    #define LONGTAIL_ATOMICADD_PRIVATE LONGTAIL_ATOMICADD
#endif

int IsDirPath(const char* path)
{
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

//int IsCompressedFileType(const char* path)
//{
//    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
//    const char* extension = strrchr(path, '.');
//    if (!extension)
//    {
//        return 0;
//    }
//    if (stricmp(path, ".pak"))
//    {
//        return 1;
//    }
//    if (stricmp(path, ".zip"))
//    {
//        return 1;
//    }
//    if (stricmp(path, ".rar"))
//    {
//        return 1;
//    }
//    if (stricmp(path, ".7z"))
//    {
//        return 1;
//    }
//    return 0;
//}

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
    return 0;
}

int EnsureParentPathExists(struct StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(path != 0, return 0);
    char* dir_path = strdup(path);
    LONGTAIL_FATAL_ASSERT_PRIVATE(dir_path != 0, return 0);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        free(dir_path);
        dir_path = 0;
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        free(dir_path);
        dir_path = 0;
        return 1;
    }
    else
    {
        if (!EnsureParentPathExists(storage_api, dir_path))
        {
            LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
            free(dir_path);
            dir_path = 0;
            return 0;
        }
        if (SafeCreateDir(storage_api, dir_path))
        {
            free(dir_path);
            dir_path = 0;
            return 1;
        }
    }
    LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
    free(dir_path);
    dir_path = 0;
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

    arrput(folder_paths, strdup(root_folder));

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
        free((void*)asset_folder);
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
    struct Paths* paths = (struct Paths*)LONGTAIL_MALLOC(GetPathsSize(path_count, path_data_size));
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

        LONGTAIL_FREE(paths);
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
        char* full_dir_path = (char*)malloc(path_length + 1 + 1);
        strcpy(full_dir_path, full_path);
        strcpy(&full_dir_path[path_length], "/");
        free(full_path);
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

    free(full_path);
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
        LONGTAIL_FREE(context.m_Paths);
        context.m_Paths = 0;
        arrfree(context.m_FileSizes);
        context.m_FileSizes = 0;
        return 0;
    }

    uint32_t asset_count = *context.m_Paths->m_PathCount;
    struct FileInfos* result = (struct FileInfos*)LONGTAIL_MALLOC(
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

    LONGTAIL_FREE(context.m_Paths);
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
        if (!c->m_StorageAPI->Read(c->m_StorageAPI, c->m_AssetFile, c->m_Offset, (uint32_t)read_count, buffer))
        {
            LONGTAIL_LOG("StorageChunkFeederFunc: Failed to read from asset file `%s`\n", c->m_AssetPath)
            return 0;
        }
        c->m_Offset += read_count;
    }
    return (uint32_t)read_count;
}

// ChunkerWindowSize is the number of bytes in the rolling hash window
//const uint32_t ChunkerWindowSize = 48;
#define ChunkerWindowSize 48

struct HashJob
{
    struct StorageAPI* m_StorageAPI;
    struct HashAPI* m_HashAPI;
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_ContentHash;
    uint64_t m_ContentSize;
    uint32_t m_ContentCompressionType;
    const char* m_RootPath;
    const char* m_Path;
    uint32_t m_MaxChunkCount;
    uint32_t* m_AssetChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkCompressionTypes;
    uint32_t* m_ChunkSizes;
    uint32_t m_MaxChunkSize;
    int m_Success;
};

void LinearChunking(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Success = 0;

    *hash_job->m_AssetChunkCount = 0;
//    *hash_job->m_ContentSize = 0;
    *hash_job->m_ContentHash = 0;

    *hash_job->m_PathHash = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path);

    if (IsDirPath(hash_job->m_Path))
    {
        hash_job->m_Success = 1;
        return;
    }
    uint32_t chunk_count = 0;

    struct StorageAPI* storage_api = hash_job->m_StorageAPI;
    char* path = storage_api->ConcatPath(storage_api, hash_job->m_RootPath, hash_job->m_Path);
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        LONGTAIL_LOG("LinearChunking: Failed to open file `%s`\n", path)
        free(path);
        path = 0;
        return;
    }

    uint64_t asset_size = hash_job->m_ContentSize;
    if (asset_size > 1024*1024*1024)
    {
        LONGTAIL_LOG("LinearChunking: Hashing a very large file `%s`\n", path);
    }

    uint8_t* batch_data = (uint8_t*)LONGTAIL_MALLOC((size_t)(asset_size < hash_job->m_MaxChunkSize ? asset_size : hash_job->m_MaxChunkSize));
    HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);

    uint64_t offset = 0;
    while (offset != asset_size)
    {
        uint32_t len = (uint32_t)((asset_size - offset) < hash_job->m_MaxChunkSize ? (asset_size - offset) : hash_job->m_MaxChunkSize);
        int read_ok = storage_api->Read(storage_api, file_handle, offset, len, batch_data);
        if (!read_ok)
        {
            LONGTAIL_LOG("LinearChunking: Failed to read from `%s`\n", path)
            hash_job->m_Success = 0;
            LONGTAIL_FREE(batch_data);
            batch_data = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            free(path);
            path = 0;
            return;
        }

        {
            HashAPI_HContext chunk_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
            hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, chunk_hash_context, len, batch_data);
            TLongtail_Hash chunk_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, chunk_hash_context);
            hash_job->m_ChunkHashes[chunk_count] = chunk_hash;
            hash_job->m_ChunkSizes[chunk_count] = len;
            hash_job->m_ChunkCompressionTypes[chunk_count] = hash_job->m_ContentCompressionType;
        }
        ++chunk_count;

        offset += len;
        hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, len, batch_data);
    }

    LONGTAIL_FREE(batch_data);
    batch_data = 0;

    TLongtail_Hash content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);

    storage_api->CloseRead(storage_api, file_handle);
    file_handle = 0;

    *hash_job->m_ContentHash = content_hash;
//    *hash_job->m_ContentSize = asset_size;
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count <= hash_job->m_MaxChunkCount, return);
    *hash_job->m_AssetChunkCount = chunk_count;

    free((char*)path);
    path = 0;

    hash_job->m_Success = 1;
}

void DynamicChunking(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Success = 0;

    *hash_job->m_AssetChunkCount = 0;
    *hash_job->m_ContentHash = 0;

    *hash_job->m_PathHash = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path);

    if (IsDirPath(hash_job->m_Path))
    {
        hash_job->m_Success = 1;
        return;
    }
    uint32_t chunk_count = 0;

    struct StorageAPI* storage_api = hash_job->m_StorageAPI;
    char* path = storage_api->ConcatPath(storage_api, hash_job->m_RootPath, hash_job->m_Path);
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        LONGTAIL_LOG("DynamicChunking: Failed to open file `%s`\n", path)
        free(path);
        path = 0;
        return;
    }

    uint64_t asset_size = hash_job->m_ContentSize;
    if (asset_size > 1024*1024*1024)
    {
        LONGTAIL_LOG("DynamicChunking: Hashing a very large file `%s`\n", path);
    }

    TLongtail_Hash content_hash = 0;
    if (asset_size == 0)
    {
        content_hash = 0;
    }
    else if (asset_size <= ChunkerWindowSize || hash_job->m_MaxChunkSize <= ChunkerWindowSize)
    {
        char* buffer = (char*)LONGTAIL_MALLOC((size_t)asset_size);
        if (!storage_api->Read(storage_api, file_handle, 0, asset_size, buffer))
        {
            LONGTAIL_LOG("DynamicChunking: Failed to read from file `%s`\n", path)
            LONGTAIL_FREE(buffer);
            buffer = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            free(path);
            path = 0;
            return;
        }

        HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
        hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, (uint32_t)asset_size, buffer);
        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);

        LONGTAIL_FREE(buffer);
        buffer = 0;

        hash_job->m_ChunkHashes[chunk_count] = content_hash;
        hash_job->m_ChunkSizes[chunk_count] = (uint32_t)asset_size;
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
            asset_size,
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
            free(path);
            path = 0;
            return;
        }

        HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);

        uint64_t remaining = asset_size;
        struct ChunkRange r = NextChunk(chunker);
        while (r.len)
        {
            if(remaining < r.len)
            {
                LONGTAIL_LOG("DynamicChunking: Chunking size is larger than remaining file size for asset `%s`\n", path)
                LONGTAIL_FREE(chunker);
                chunker = 0;
                hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
                hash_job->m_Success = 0;
                storage_api->CloseRead(storage_api, file_handle);
                file_handle = 0;
                free(path);
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
            LONGTAIL_FREE(chunker);
            chunker = 0;
            hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
            hash_job->m_Success = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            free(path);
            path = 0;
            return;
        }

        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
        LONGTAIL_FREE(chunker);
        chunker = 0;
    }

    storage_api->CloseRead(storage_api, file_handle);
    file_handle = 0;
    
    *hash_job->m_ContentHash = content_hash;
    LONGTAIL_FATAL_ASSERT_PRIVATE(chunk_count <= hash_job->m_MaxChunkCount, return);
    *hash_job->m_AssetChunkCount = chunk_count;

    free((char*)path);
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

    if (!job_api->ReserveJobs(job_api, asset_count))
    {
        LONGTAIL_LOG("ChunkAssets: Failed to reserve %u jobs for folder `%s`\n", (uint32_t)*paths->m_PathCount, root_path)
        return 0;
    }

    const int linear_chunking = 0;
    uint64_t min_chunk_size = linear_chunking ? max_chunk_size : (max_chunk_size / 8);
    uint64_t max_chunk_count = 0;
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint32_t max_count = content_sizes[asset_index] == 0 ? 0 : 1 + (content_sizes[asset_index] / min_chunk_size);
        max_chunk_count += max_count;
    }

    TLongtail_Hash* hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof(TLongtail_Hash) * max_chunk_count);
    uint32_t* sizes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * max_chunk_count);
    uint32_t* compression_types = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * max_chunk_count);

    struct HashJob* hash_jobs = (struct HashJob*)LONGTAIL_MALLOC(sizeof(struct HashJob) * asset_count);

    uint64_t chunks_offset = 0;
    uint64_t assets_left = asset_count;
    uint64_t offset = 0;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint32_t max_chunk_count = content_sizes[asset_index] == 0 ? 0 : 1 + (content_sizes[asset_index] / min_chunk_size);
        struct HashJob* job = &hash_jobs[asset_index];
        void* ctx = &hash_jobs[asset_index];
        job->m_StorageAPI = storage_api;
        job->m_HashAPI = hash_api;
        job->m_RootPath = root_path;
        job->m_Path = &paths->m_Data[paths->m_Offsets[asset_index]];
        job->m_PathHash = &path_hashes[asset_index];
        job->m_ContentHash = &content_hashes[asset_index];
        job->m_ContentSize = content_sizes[asset_index];
        job->m_ContentCompressionType = content_compression_types[asset_index];
        job->m_MaxChunkCount = max_chunk_count;
        job->m_AssetChunkCount = &asset_chunk_counts[asset_index];
        job->m_ChunkHashes = &hashes[chunks_offset];
        job->m_ChunkSizes = &sizes[chunks_offset];
        job->m_ChunkCompressionTypes = &compression_types[chunks_offset];
        job->m_MaxChunkSize = max_chunk_size;

        JobAPI_JobFunc func = linear_chunking ? LinearChunking : DynamicChunking;

        JobAPI_Jobs jobs = job_api->CreateJobs(job_api, 1, &func, &ctx);
        LONGTAIL_FATAL_ASSERT_PRIVATE(jobs != 0, return 0)
        job_api->ReadyJobs(job_api, 1, jobs);

        chunks_offset += max_chunk_count;
    }

    job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);

    int success = 1;
    for (uint32_t i = 0; i < asset_count; ++i)
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
        for (uint32_t i = 0; i < asset_count; ++i)
        {
            built_chunk_count += asset_chunk_counts[i];
        }
        *chunk_count = built_chunk_count;
        *chunk_sizes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * *chunk_count);
        *chunk_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof(TLongtail_Hash) * *chunk_count);
        *chunk_compression_types = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * *chunk_count);

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < asset_count; ++i)
        {
            asset_chunk_start_index[i] = chunk_offset;
            for (uint32_t chunk_index = 0; chunk_index < asset_chunk_counts[i]; ++chunk_index)
            {
                (*chunk_sizes)[chunk_offset] = hash_jobs[i].m_ChunkSizes[chunk_index];
                (*chunk_hashes)[chunk_offset] = hash_jobs[i].m_ChunkHashes[chunk_index];
                (*chunk_compression_types)[chunk_offset] = hash_jobs[i].m_ChunkCompressionTypes[chunk_index];
                ++chunk_offset;
            }
        }
    }
    else
    {
        *chunk_count = 0;
        *chunk_sizes = 0;
        *chunk_hashes = 0;
        *chunk_compression_types = 0;
    }

    LONGTAIL_FREE(compression_types);
    compression_types = 0;

    LONGTAIL_FREE(hashes);
    hashes = 0;

    LONGTAIL_FREE(sizes);
    sizes = 0;

    LONGTAIL_FREE(hash_jobs);
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
    TLongtail_Hash* path_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof(TLongtail_Hash) * path_count);
    TLongtail_Hash* content_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof(TLongtail_Hash) * path_count);
    uint32_t* asset_chunk_counts = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * path_count);

    uint32_t assets_chunk_index_count = 0;
    uint32_t* asset_chunk_sizes = 0;
    uint32_t* asset_chunk_compression_types = 0;
    TLongtail_Hash* asset_chunk_hashes = 0;
    uint32_t* asset_chunk_start_index = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * path_count);

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
        LONGTAIL_FREE(asset_chunk_compression_types);
        asset_chunk_compression_types = 0;
        LONGTAIL_FREE(asset_chunk_start_index);
        asset_chunk_start_index = 0;
        LONGTAIL_FREE(asset_chunk_hashes);
        asset_chunk_hashes = 0;
        LONGTAIL_FREE(asset_chunk_sizes);
        asset_chunk_sizes = 0;
        LONGTAIL_FREE(content_hashes);
        content_hashes = 0;
        LONGTAIL_FREE(path_hashes);
        path_hashes = 0;
        return 0;
    }

    uint32_t* asset_chunk_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * assets_chunk_index_count);
    TLongtail_Hash* compact_chunk_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof(TLongtail_Hash) * assets_chunk_index_count);
    uint32_t* compact_chunk_sizes =  (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * assets_chunk_index_count);
    uint32_t* compact_chunk_compression_types =  (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * assets_chunk_index_count);

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
    void* version_index_mem = LONGTAIL_MALLOC(version_index_size);

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

    LONGTAIL_FREE(compact_chunk_compression_types);
    compact_chunk_compression_types = 0;
    LONGTAIL_FREE(compact_chunk_sizes);
    compact_chunk_sizes = 0;
    LONGTAIL_FREE(compact_chunk_hashes);
    compact_chunk_hashes = 0;
    LONGTAIL_FREE(asset_chunk_indexes);
    asset_chunk_indexes = 0;
    LONGTAIL_FREE(asset_chunk_compression_types);
    asset_chunk_compression_types = 0;
    LONGTAIL_FREE(asset_chunk_sizes);
    asset_chunk_sizes = 0;
    LONGTAIL_FREE(asset_chunk_hashes);
    asset_chunk_hashes = 0;
    LONGTAIL_FREE(asset_chunk_start_index);
    asset_chunk_start_index = 0;
    LONGTAIL_FREE(asset_chunk_counts);
    asset_chunk_counts = 0;
    LONGTAIL_FREE(content_hashes);
    content_hashes = 0;
    LONGTAIL_FREE(path_hashes);
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
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path, 1);
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
    struct VersionIndex* version_index = (struct VersionIndex*)LONGTAIL_MALLOC((size_t)(sizeof(struct VersionIndex) + version_index_data_size));
    if (!version_index)
    {
        LONGTAIL_LOG("ReadVersionIndex: Failed to allocate memory for `%s`\n", path);
        LONGTAIL_FREE(version_index);
        version_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]))
    {
        LONGTAIL_LOG("ReadVersionIndex: Failed to read from `%s`\n", path);
        LONGTAIL_FREE(version_index);
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
        sizeof(uint32_t);                          // m_ChunkCount
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
        struct ContentIndex* content_index = (struct ContentIndex*)LONGTAIL_MALLOC(content_index_size);

        content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
        content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
        *content_index->m_BlockCount = 0;
        *content_index->m_ChunkCount = 0;
        InitContentIndex(content_index);
        return content_index;
    }
    uint64_t* chunk_indexes = (uint64_t*)LONGTAIL_MALLOC((size_t)(sizeof(uint64_t) * chunk_count));
    uint64_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, chunk_indexes);

    struct BlockIndex** block_indexes = (struct BlockIndex**)LONGTAIL_MALLOC(sizeof(struct BlockIndex*) * unique_chunk_count);

    #define MAX_ASSETS_PER_BLOCK 16384u
    uint64_t* stored_chunk_indexes = (uint64_t*)LONGTAIL_MALLOC(sizeof(uint64_t) * max_chunks_per_block);

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
            LONGTAIL_MALLOC(GetBlockIndexSize(chunk_count_in_block)),
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
            LONGTAIL_MALLOC(GetBlockIndexSize(chunk_count_in_block)),
            hash_api,
            current_compression_type,
            chunk_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes);
        ++block_count;
    }

    LONGTAIL_FREE(stored_chunk_indexes);
    stored_chunk_indexes = 0;
    LONGTAIL_FREE(chunk_indexes);
    chunk_indexes = 0;

    // Build Content Index (from block list)
    size_t content_index_size = GetContentIndexSize(block_count, unique_chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)LONGTAIL_MALLOC(content_index_size);

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
        LONGTAIL_FREE(block_index);
        block_index = 0;
    }
    LONGTAIL_FREE(block_indexes);
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
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path, 1);
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
    struct ContentIndex* content_index = (struct ContentIndex*)LONGTAIL_MALLOC((size_t)(sizeof(struct ContentIndex) + content_index_data_size));
    if (!content_index)
    {
        LONGTAIL_LOG("ReadContentIndex: Failed allocate memory for `%s`\n", path);
        LONGTAIL_FREE(content_index);
        content_index = 0;
        storage_api->CloseRead(storage_api, file_handle);
        file_handle = 0;
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]))
    {
        LONGTAIL_LOG("ReadContentIndex: Failed to read from `%s`\n", path);
        LONGTAIL_FREE(content_index);
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
                    compression_type,
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

void FreeAssetPartLookup(struct ChunkHashToAssetPart* asset_part_lookup)
{
    hmfree(asset_part_lookup);
    asset_part_lookup = 0;
}

struct WriteBlockJob
{
    struct StorageAPI* m_SourceStorageAPI;
    struct StorageAPI* m_TargetStorageAPI;
    struct CompressionAPI* m_CompressionAPI;
    const char* m_ContentFolder;
    const char* m_AssetsFolder;
    const char* m_BlockName;
    const char* m_BlockPath;
    const struct ContentIndex* m_ContentIndex;
    struct ChunkHashToAssetPart* m_AssetPartLookup;
    uint64_t m_FirstChunkIndex;
    uint32_t m_ChunkCount;
    uint32_t m_Success;
};

char* GetBlockName(TLongtail_Hash block_hash)
{
    char* name = (char*)LONGTAIL_MALLOC(64);
    sprintf(name, "0x%" PRIx64, block_hash);
    return name;
}

void WriteContentBlockJob(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct WriteBlockJob* job = (struct WriteBlockJob*)context;
    struct StorageAPI* source_storage_api = job->m_SourceStorageAPI;
    struct StorageAPI* target_storage_api = job->m_TargetStorageAPI;
    struct CompressionAPI* compression_api = job->m_CompressionAPI;

    const struct ContentIndex* content_index = job->m_ContentIndex;
    const char* content_folder = job->m_ContentFolder;
    uint64_t first_chunk_index = job->m_FirstChunkIndex;
    uint32_t chunk_count = job->m_ChunkCount;
    uint64_t block_index = content_index->m_ChunkBlockIndexes[first_chunk_index];
    TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];

    char tmp_block_name[64];
    sprintf(tmp_block_name, "%s.tmp", job->m_BlockName);

    char* tmp_block_path = (char*)target_storage_api->ConcatPath(target_storage_api, content_folder, tmp_block_name);

    uint32_t block_data_size = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        if (content_index->m_ChunkBlockIndexes[chunk_index] != block_index)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Invalid chunk order! 0x%" PRIx64 " in `%s`\n", block_hash, content_folder)
            return;
        }
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        block_data_size += chunk_size;
    }

    uint64_t block_data_buffer_size = sizeof(uint32_t) + sizeof(uint32_t) + block_data_size;
    char* block_data_buffer = (char*)LONGTAIL_MALLOC(block_data_buffer_size);
    ((uint32_t*)block_data_buffer)[0] = (uint32_t)block_data_size;
    ((uint32_t*)block_data_buffer)[1] = (uint32_t)block_data_size;
    char* write_buffer = (char*)&((uint32_t*)block_data_buffer)[2];
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
            LONGTAIL_FREE(block_data_buffer);
            block_data_buffer = 0;
            free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        struct AssetPart* asset_part = &job->m_AssetPartLookup[asset_part_index].value;
        const char* asset_path = asset_part->m_Path;
        if (IsDirPath(asset_path))
        {
            LONGTAIL_LOG("WriteContentBlockJob: Directory should not have any chunks `%s`\n", asset_path)
            LONGTAIL_FREE(block_data_buffer);
            block_data_buffer = 0;
            free((char*)tmp_block_path);
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
            LONGTAIL_FREE(block_data_buffer);
            block_data_buffer = 0;
            free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        uint64_t asset_file_size = source_storage_api->GetSize(source_storage_api, file_handle);
        if (asset_file_size < (asset_content_offset + chunk_size))
        {
            LONGTAIL_LOG("WriteContentBlockJob: Mismatching asset size in asset `%s`, size is %" PRIu64 ", but expecting at least %" PRIu64 "\n", full_path, asset_file_size, asset_content_offset + chunk_size);
            LONGTAIL_FREE(block_data_buffer);
            block_data_buffer = 0;
            free((char*)tmp_block_path);
            tmp_block_path = 0;
            source_storage_api->CloseRead(source_storage_api, file_handle);
            file_handle = 0;
            return;
        }
        source_storage_api->Read(source_storage_api, file_handle, asset_content_offset, chunk_size, write_ptr);
        write_ptr += chunk_size;

        source_storage_api->CloseRead(source_storage_api, file_handle);
        free((char*)full_path);
        full_path = 0;
    }

    if (compression_type != 0)
    {
        // TODO: We should pick compression method based on 'compression_type'
        CompressionAPI_HCompressionContext compression_context = compression_api->CreateCompressionContext(compression_api, compression_api->GetDefaultSettings(compression_api));
        const size_t max_dst_size = compression_api->GetMaxCompressedSize(compression_api, compression_context, block_data_size);
        char* compressed_buffer = (char*)LONGTAIL_MALLOC((sizeof(uint32_t) * 2) + max_dst_size);
        ((uint32_t*)compressed_buffer)[0] = (uint32_t)block_data_size;

        size_t compressed_size = compression_api->Compress(compression_api, compression_context, (const char*)write_buffer, &((char*)compressed_buffer)[sizeof(int32_t) * 2], block_data_size, max_dst_size);
        compression_api->DeleteCompressionContext(compression_api, compression_context);
        if (compressed_size <= 0)
        {
            LONGTAIL_LOG("WriteContentBlockJob: Failed to compress data for block for `%s`\n", tmp_block_path)
            LONGTAIL_FREE(compressed_buffer);
            compressed_buffer = 0;
            free((char*)tmp_block_path);
            tmp_block_path = 0;
            return;
        }
        ((uint32_t*)compressed_buffer)[1] = (uint32_t)compressed_size;

        LONGTAIL_FREE(block_data_buffer);
        block_data_buffer = 0;
        block_data_buffer_size = sizeof(uint32_t) + sizeof(uint32_t) + compressed_size;
        block_data_buffer = compressed_buffer;
    }

    if (!EnsureParentPathExists(target_storage_api, tmp_block_path))
    {
        LONGTAIL_LOG("WriteContentBlockJob: Failed to create parent path for `%s`\n", tmp_block_path)
        LONGTAIL_FREE(block_data_buffer);
        block_data_buffer = 0;
        free((char*)tmp_block_path);
        return;
    }

    StorageAPI_HOpenFile block_file_handle = target_storage_api->OpenWriteFile(target_storage_api, tmp_block_path, 1);
    if (!block_file_handle)
    {
        LONGTAIL_LOG("WriteContentBlockJob: Failed to create block file `%s`\n", tmp_block_path)
        LONGTAIL_FREE(block_data_buffer);
        block_data_buffer = 0;
        free((char*)tmp_block_path);
        tmp_block_path = 0;
        return;
    }
    int write_ok = target_storage_api->Write(target_storage_api, block_file_handle, 0, block_data_buffer_size, block_data_buffer);
    LONGTAIL_FREE(block_data_buffer);
    block_data_buffer = 0;
    uint32_t write_offset = block_data_buffer_size;

    uint32_t aligned_size = (((write_offset + 15) / 16) * 16);
    uint32_t padding = aligned_size - write_offset;
    if (padding)
    {
        target_storage_api->Write(target_storage_api, block_file_handle, write_offset, padding, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        write_offset = aligned_size;
    }
    struct BlockIndex* block_index_ptr = (struct BlockIndex*)LONGTAIL_MALLOC(GetBlockIndexSize(chunk_count));
    InitBlockIndex(block_index_ptr, chunk_count);
    memmove(block_index_ptr->m_ChunkHashes, &content_index->m_ChunkHashes[first_chunk_index], sizeof(TLongtail_Hash) * chunk_count);
    memmove(block_index_ptr->m_ChunkSizes, &content_index->m_ChunkLengths[first_chunk_index], sizeof(uint32_t) * chunk_count);
    *block_index_ptr->m_BlockHash = block_hash;
    *block_index_ptr->m_ChunkCount = chunk_count;
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);
    write_ok = target_storage_api->Write(target_storage_api, block_file_handle, write_offset, block_index_data_size, &block_index_ptr[1]);
    LONGTAIL_FREE(block_index_ptr);
    block_index_ptr = 0;

    target_storage_api->CloseWrite(target_storage_api, block_file_handle);
    write_ok = write_ok & target_storage_api->RenameFile(target_storage_api, tmp_block_path, job->m_BlockPath);
    job->m_Success = write_ok;

    free((char*)tmp_block_path);
    tmp_block_path = 0;

    job->m_Success = 1;
}

int WriteContent(
    struct StorageAPI* source_storage_api,
    struct StorageAPI* target_storage_api,
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct ContentIndex* content_index,
    struct ChunkHashToAssetPart* asset_part_lookup,
    const char* assets_folder,
    const char* content_folder)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(source_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(target_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(asset_part_lookup != 0, return 0);
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

    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)LONGTAIL_MALLOC((size_t)(sizeof(struct WriteBlockJob) * block_count));
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

        char* block_name = GetBlockName(block_hash);
        char file_name[64];
        sprintf(file_name, "%s.lrb", block_name);
        char* block_path = target_storage_api->ConcatPath(target_storage_api, content_folder, file_name);
        if (target_storage_api->IsFile(target_storage_api, block_path))
        {
            free((char*)block_path);
            block_path = 0;
            block_start_chunk_index += chunk_count;
            LONGTAIL_FREE(block_name);
            block_name = 0;
            continue;
        }

        struct WriteBlockJob* job = &write_block_jobs[job_count++];
        job->m_SourceStorageAPI = source_storage_api;
        job->m_TargetStorageAPI = target_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentFolder = content_folder;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = content_index;
        job->m_BlockName = block_name;
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
        free((char*)job->m_BlockPath);
        job->m_BlockPath = 0;
        LONGTAIL_FREE((char*)job->m_BlockName);
        job->m_BlockName = 0;
    }

    LONGTAIL_FREE(write_block_jobs);
    write_block_jobs = 0;

    return success;
}

static char* ReadBlockData(
    struct StorageAPI* storage_api,
    struct CompressionAPI* compression_api,
    const char* content_folder,
    TLongtail_Hash block_hash)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_folder != 0, return 0);

    char* block_name = GetBlockName(block_hash);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    char* block_path = storage_api->ConcatPath(storage_api, content_folder, file_name);
    LONGTAIL_FREE(block_name);
    block_name = 0;

    StorageAPI_HOpenFile block_file = storage_api->OpenReadFile(storage_api, block_path);
    if (!block_file)
    {
        LONGTAIL_LOG("ReadBlockData: Failed to open block `%s`\n", block_path)
        free(block_path);
        block_path = 0;
        return 0;
    }
    uint64_t compressed_block_size = storage_api->GetSize(storage_api, block_file);

    char* compressed_block_content = (char*)LONGTAIL_MALLOC(compressed_block_size);
    int ok = storage_api->Read(storage_api, block_file, 0, compressed_block_size, compressed_block_content);
    storage_api->CloseRead(storage_api, block_file);
    block_file = 0;
    if (!ok){
        LONGTAIL_LOG("ReadBlockData: Failed to read block `%s`\n", block_path)
        free(block_path);
        block_path = 0;
        LONGTAIL_FREE(compressed_block_content);
        compressed_block_content = 0;
        return 0;
    }

    uint32_t chunk_count = *(const uint32_t*)(&compressed_block_content[compressed_block_size - sizeof(uint32_t)]);
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);
    if (compressed_block_size < block_index_data_size)
    {
        LONGTAIL_LOG("ReadBlockData: Malformed content block (size to small) `%s`\n", block_path)
        free(block_path);
        block_path = 0;
        LONGTAIL_FREE(compressed_block_content);
        compressed_block_content = 0;
        return 0;
    }

    char* block_data = compressed_block_content;
    const TLongtail_Hash* block_index_start = (const TLongtail_Hash*)&block_data[compressed_block_size - block_index_data_size];
    // TODO: This could be cleaner
    TLongtail_Hash verify_block_hash = block_index_start[0];
    if (block_hash != verify_block_hash)
    {
        LONGTAIL_LOG("ReadBlockData: Malformed content block (mismatching block hash) `%s`\n", block_path)
        free(block_path);
        block_path = 0;
        LONGTAIL_FREE(block_data);
        block_data = 0;
        return 0;
    }
    uint32_t compression_type = block_index_start[1];
    if (0 != compression_type)
    {
        uint32_t uncompressed_size = ((uint32_t*)compressed_block_content)[0];
        uint32_t compressed_size = ((uint32_t*)compressed_block_content)[1];
        block_data = (char*)LONGTAIL_MALLOC(uncompressed_size);
        CompressionAPI_HDecompressionContext compression_context = compression_api->CreateDecompressionContext(compression_api);
        if (!compression_context)
        {
            LONGTAIL_LOG("ReadBlockData: Failed to create decompressor for block `%s`\n", block_path)
            LONGTAIL_FREE(block_data);
            block_data = 0;
            free(block_path);
            block_path = 0;
            LONGTAIL_FREE(compressed_block_content);
            compressed_block_content = 0;
            return 0;
        }
        size_t result = compression_api->Decompress(compression_api, compression_context, &compressed_block_content[sizeof(uint32_t) * 2], block_data, compressed_size, uncompressed_size);
        ok = result == uncompressed_size;
        compression_api->DeleteDecompressionContext(compression_api, compression_context);
        LONGTAIL_FREE(compressed_block_content);
        compressed_block_content = 0;

        if (!ok)
        {
            LONGTAIL_LOG("ReadBlockData: Failed to decompress block `%s`\n", block_path)
            LONGTAIL_FREE(block_data);
            block_data = 0;
            free(block_path);
            block_path = 0;
            return 0;
        }
    }

    free(block_path);
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

    struct BlockIndex* block_index = InitBlockIndex(LONGTAIL_MALLOC(GetBlockIndexSize(chunk_count)), chunk_count);

    int ok = storage_api->Read(storage_api, f, s - block_index_data_size, block_index_data_size, &block_index[1]);
    storage_api->CloseRead(storage_api, f);
    if (!ok)
    {
        LONGTAIL_LOG("ReadBlock: Failed to read block `%s`\n", full_block_path)
        LONGTAIL_FREE(block_index);
        block_index = 0;
        return 0;
    }

    return block_index;
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
    LONGTAIL_FREE(cl);
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

    struct ContentLookup* cl = (struct ContentLookup*)LONGTAIL_MALLOC(sizeof(struct ContentLookup));
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

struct WriteAssetFromBlocksJob
{
    struct StorageAPI* m_ContentStorageAPI;
    struct StorageAPI* m_VersionStorageAPI;
    struct CompressionAPI* m_CompressionAPI;
    const struct ContentIndex* m_ContentIndex;
    const struct VersionIndex* m_BaseVersionIndex;
    const struct VersionIndex* m_VersionIndex;
    const char* m_ContentFolder;
    const char* m_VersionFolder;
    uint32_t m_AssetIndex;
    uint32_t m_BaseAssetIndex;
    struct HashToIndexItem* m_ContentChunkLookup;
    int m_Success;
};

void WriteAssetFromBlocks(void* context)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(context != 0, return);

    struct WriteAssetFromBlocksJob* job = (struct WriteAssetFromBlocksJob*)context;
    job->m_Success = 0;
    struct StorageAPI* content_storage_api = job->m_ContentStorageAPI;
    struct StorageAPI* version_storage_api = job->m_VersionStorageAPI;
    struct CompressionAPI* compression_api = job->m_CompressionAPI;
    const char* content_folder = job->m_ContentFolder;
    const char* version_folder = job->m_VersionFolder;
    const uint32_t asset_index = job->m_AssetIndex;
    const struct ContentIndex* content_index = job->m_ContentIndex;
    const struct VersionIndex* version_index = job->m_VersionIndex;
    struct HashToIndexItem* content_chunk_lookup = job->m_ContentChunkLookup;
    const struct VersionIndex* base_version_index = job->m_BaseVersionIndex;
    const uint32_t base_asset_index = job->m_BaseAssetIndex;

    const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
    char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_folder, asset_path);
    if (!EnsureParentPathExists(version_storage_api, full_asset_path))
    {
        LONGTAIL_LOG("WriteAssetFromBlocks: Failed to create parent folder for `%s`\n", full_asset_path)
        free(full_asset_path);
        full_asset_path = 0;
        return;
    }
    if (IsDirPath(full_asset_path))
    {
        if (!SafeCreateDir(version_storage_api, full_asset_path))
        {
            LONGTAIL_LOG("WriteAssetFromBlocks: Failed to create folder for `%s`\n", full_asset_path)
            free(full_asset_path);
            full_asset_path = 0;
            return;
        }
        free(full_asset_path);
        full_asset_path = 0;
        job->m_Success = 1;
        return;
    }

    StorageAPI_HOpenFile asset_file = version_storage_api->OpenWriteFile(version_storage_api, full_asset_path, 0);
    if (!asset_file)
    {
        LONGTAIL_LOG("WriteAssetFromBlocks: Unable to create asset `%s`\n", full_asset_path)
        free(full_asset_path);
        full_asset_path = 0;
        return;
    }

    const uint32_t* base_chunk_indexes = 0;
    uint32_t base_chunk_count = 0;
    if (base_version_index)
    {
        uint32_t asset_chunk_index_start = base_version_index->m_AssetChunkIndexStarts[base_asset_index];
        base_chunk_indexes = &base_version_index->m_AssetChunkIndexes[asset_chunk_index_start];
        base_chunk_count = base_version_index->m_AssetChunkCounts[base_asset_index];
    }

    uint32_t asset_chunk_start_index = version_index->m_AssetChunkIndexStarts[asset_index];
    const uint32_t* chunk_indexes = &version_index->m_AssetChunkIndexes[asset_chunk_start_index];
    uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
    TLongtail_Hash prev_block_hash = 0;
    uint64_t asset_offset = 0;
    uint64_t base_asset_offset = 0;
    char* block_data = 0;
    uint32_t base_c = 0;
    uint32_t written_chunk_count = 0;
    uint32_t skipped_chunk_count = 0;
#ifdef SLOW_VALIDATION
    struct BlockIndex* test_slow_block_data_index = 0;
#endif // SLOW_VALIDATION
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        uint32_t chunk_index = chunk_indexes[c];
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];

        while (base_c < base_chunk_count && base_asset_offset < asset_offset)
        {
            uint32_t base_chunk_index = base_chunk_indexes[base_c];
            base_asset_offset += base_version_index->m_ChunkSizes[base_chunk_index];
            ++base_c;
        }

        if (base_c < base_chunk_count && base_asset_offset == asset_offset)
        {
            uint32_t base_chunk_index = base_chunk_indexes[base_c];
            TLongtail_Hash base_chunk_hash = base_version_index->m_ChunkHashes[base_chunk_index];
            if (chunk_hash == base_chunk_hash)
            {
                asset_offset += chunk_size;
                base_asset_offset += chunk_size;
                ++base_c;
                ++skipped_chunk_count;
                continue;
            }
        }

        ptrdiff_t tmp;
        uint64_t chunk_content_index = hmget_ts(content_chunk_lookup, chunk_hash, tmp);
        if (content_index->m_ChunkHashes[chunk_content_index] != chunk_hash)
        {
#ifdef SLOW_VALIDATION
            LONGTAIL_FREE(test_slow_block_data_index);
            test_slow_block_data_index = 0;
#endif // SLOW_VALIDATION
            LONGTAIL_FREE(block_data);
            block_data = 0;
            version_storage_api->CloseWrite(version_storage_api, asset_file);
            asset_file = 0;
            free(full_asset_path);
            full_asset_path = 0;
            return;
        }
        uint64_t block_index = content_index->m_ChunkBlockIndexes[chunk_content_index];
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        uint32_t chunk_offset = content_index->m_ChunkBlockOffsets[chunk_content_index];

        if (block_hash != prev_block_hash)
        {
            LONGTAIL_FREE(block_data);
            block_data = 0;
#ifdef SLOW_VALIDATION
            LONGTAIL_FREE(test_slow_block_data_index);
            test_slow_block_data_index = 0;
#endif // SLOW_VALIDATION
        }
#ifdef SLOW_VALIDATION
        if (!test_slow_block_data_index)
        {
            char* block_name = GetBlockName(block_hash);
            char file_name[64];
            sprintf(file_name, "%s.lrb", block_name);
            char* block_path = version_storage_api->ConcatPath(content_storage_api, content_folder, file_name);

            test_slow_block_data_index = ReadBlockIndex(content_storage_api, block_path);

            free(block_path);
            block_path = 0;
            LONGTAIL_FREE(block_name);
            block_name = 0;

            if (test_slow_block_data_index == 0)
            {
                LONGTAIL_LOG("WriteAssetFromBlocks: Block 0x%" PRIx64 " could not be opened\n", block_hash)
                LONGTAIL_FREE(block_data);
                block_data = 0;
                version_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                free(full_asset_path);
                full_asset_path = 0;
                return;
            }
        }
        {
            uint32_t offset_in_block = 0;
            uint32_t b = 0;
            while (b < *test_slow_block_data_index->m_ChunkCount)
            {
                if (test_slow_block_data_index->m_ChunkHashes[b] == chunk_hash)
                {
                    break;
                }
                offset_in_block += test_slow_block_data_index->m_ChunkSizes[b];
                ++b;
            }
            
            if (b == *test_slow_block_data_index->m_ChunkCount)
            {
                LONGTAIL_LOG("WriteAssetFromBlocks: Block 0x%" PRIx64 " does not contain chunk 0x%" PRIx64 "\n", block_hash, chunk_hash)
                LONGTAIL_FREE(test_slow_block_data_index);
                test_slow_block_data_index = 0;
                LONGTAIL_FREE(block_data);
                block_data = 0;
                version_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                free(full_asset_path);
                full_asset_path = 0;
                return;
            }

            if (chunk_size != test_slow_block_data_index->m_ChunkSizes[b])
            {
                LONGTAIL_LOG("WriteAssetFromBlocks: Block 0x%" PRIx64 " has mismatching chunk size for chunk 0x%" PRIx64 "\n", block_hash, chunk_hash)
                LONGTAIL_FREE(test_slow_block_data_index);
                test_slow_block_data_index = 0;
                LONGTAIL_FREE(block_data);
                block_data = 0;
                version_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                free(full_asset_path);
                full_asset_path = 0;
                return;
            }

            if (chunk_offset != offset_in_block)
            {
                LONGTAIL_LOG("WriteAssetFromBlocks: Block 0x%" PRIx64 " has mismatching chunk offset for chunk 0x%" PRIx64 "\n", block_hash, chunk_hash)
                LONGTAIL_FREE(test_slow_block_data_index);
                test_slow_block_data_index = 0;
                LONGTAIL_FREE(block_data);
                block_data = 0;
                version_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                free(full_asset_path);
                full_asset_path = 0;
                return;
            }
        }
#endif // SLOW_VALIDATION
        if (content_index->m_ChunkHashes[chunk_content_index] != chunk_hash)
        {
            LONGTAIL_LOG("WriteAssetFromBlocks: Chunk hash mismatch in content index for chunk 0x%" PRIx64 " in `%s`\n", chunk_hash, asset_path)
            LONGTAIL_FREE(block_data);
            block_data = 0;
            version_storage_api->CloseWrite(version_storage_api, asset_file);
            asset_file = 0;
            free(full_asset_path);
            full_asset_path = 0;
            return;
        }
        if (content_index->m_ChunkLengths[chunk_content_index] != chunk_size)
        {
            LONGTAIL_LOG("WriteAssetFromBlocks: Chunk size mismatch in content index for chunk 0x%" PRIx64 "  in `%s`\n", chunk_hash, asset_path)
            LONGTAIL_FREE(block_data);
            block_data = 0;
            version_storage_api->CloseWrite(version_storage_api, asset_file);
            asset_file = 0;
            free(full_asset_path);
            full_asset_path = 0;
            return;
        }
        if (!block_data)
        {
            block_data = ReadBlockData(content_storage_api, compression_api, content_folder, block_hash);
            if (!block_data)
            {
                LONGTAIL_LOG("WriteAssetFromBlocks: Failed to read block 0x%" PRIx64 " to asset `%s`\n", block_hash, full_asset_path)
                version_storage_api->CloseWrite(version_storage_api, asset_file);
                asset_file = 0;
                free(full_asset_path);
                full_asset_path = 0;
                return;
            }
            prev_block_hash = block_hash;
        }

        int ok = version_storage_api->Write(version_storage_api, asset_file, asset_offset, chunk_size, &block_data[chunk_offset]);
        if (!ok)
        {
            LONGTAIL_LOG("WriteAssetFromBlocks: Failed to write chunk 0x%" PRIx64 " to asset `%s`\n", chunk_hash, full_asset_path)
            LONGTAIL_FREE(block_data);
            block_data = 0;
            version_storage_api->CloseWrite(version_storage_api, asset_file);
            asset_file = 0;
            free(full_asset_path);
            full_asset_path = 0;
            return;
        }
        asset_offset += chunk_size;
        ++written_chunk_count;
    }
    LONGTAIL_FREE(block_data);
    block_data = 0;
#ifdef SLOW_VALIDATION
    LONGTAIL_FREE(test_slow_block_data_index);
    test_slow_block_data_index = 0;
#endif // SLOW_VALIDATION
    if (version_storage_api->SetSize(version_storage_api, asset_file, asset_offset))
    {
        job->m_Success = 1;
    }
    else
    {
        LONGTAIL_LOG("WriteAssetFromBlocks: Failed to set size of asset `%s`\n", full_asset_path)
    }
    version_storage_api->CloseWrite(version_storage_api, asset_file);
#if 0
    if ((written_chunk_count + skipped_chunk_count) / 10 < skipped_chunk_count)
    {
        LONGTAIL_LOG("WriteAssetFromBlocks: Wrote %u chunks of %u, skipping %u of asset `%s`\n", written_chunk_count, written_chunk_count + skipped_chunk_count, skipped_chunk_count, full_asset_path);
    }
#endif
    asset_file = 0;
    free(full_asset_path);
    full_asset_path = 0;
}

struct WriteAssetsFromBlockJob
{
    struct StorageAPI* m_ContentStorageAPI;
    struct StorageAPI* m_VersionStorageAPI;
    struct CompressionAPI* m_CompressionAPI;
    const struct ContentIndex* m_ContentIndex;
    const struct VersionIndex* m_VersionIndex;
    const char* m_ContentFolder;
    const char* m_VersionFolder;
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
    struct CompressionAPI* compression_api = job->m_CompressionAPI;
    const char* content_folder = job->m_ContentFolder;
    const char* version_folder = job->m_VersionFolder;
    const uint64_t block_index = job->m_BlockIndex;
    const struct ContentIndex* content_index = job->m_ContentIndex;
    const struct VersionIndex* version_index = job->m_VersionIndex;
    uint32_t* asset_indexes = job->m_AssetIndexes;
    uint32_t asset_count = job->m_AssetCount;
    struct HashToIndexItem* content_chunk_lookup = job->m_ContentChunkLookup;

    TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
    char* block_data = ReadBlockData(content_storage_api, compression_api, content_folder, block_hash);
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
            free(full_asset_path);
            full_asset_path = 0;
            LONGTAIL_FREE(block_data);
            block_data = 0;
            return;
        }

        StorageAPI_HOpenFile asset_file = version_storage_api->OpenWriteFile(version_storage_api, full_asset_path, 1);
        if (!asset_file)
        {
            LONGTAIL_LOG("WriteAssetsFromBlock: Unable to create asset `%s`\n", full_asset_path)
            free(full_asset_path);
            full_asset_path = 0;
            LONGTAIL_FREE(block_data);
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
                free(full_asset_path);
                full_asset_path = 0;
                LONGTAIL_FREE(block_data);
                block_data = 0;
                return;
            }
            asset_write_offset += chunk_size;
        }

        version_storage_api->CloseWrite(version_storage_api, asset_file);
        asset_file = 0;

        free(full_asset_path);
        full_asset_path = 0;
    }

    LONGTAIL_FREE(block_data);
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

#if defined(_MSC_VER)
static int BlockJobCompare(void* context, const void* a_ptr, const void* b_ptr)
#else
static int BlockJobCompare(const void* a_ptr, const void* b_ptr, void* context)
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
    struct AssetWriteList* awl = (struct AssetWriteList*)(LONGTAIL_MALLOC(sizeof(struct AssetWriteList) + sizeof(uint32_t) * asset_count + sizeof(uint32_t) * asset_count));
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
            LONGTAIL_FREE(awl);
            awl = 0;
            return 0;
        }
        uint64_t content_block_index = cl->m_ChunkHashToBlockIndex[find_i].value;
        int is_block_job = 1;
        for (uint32_t c = 1; c < chunk_count; ++c)
        {
            uint32_t next_chunk_index = asset_chunk_indexes[asset_chunk_offset + c];
            TLongtail_Hash next_chunk_hash = chunk_hashes[next_chunk_index];
            intptr_t find_i = hmgeti(cl->m_ChunkHashToBlockIndex, next_chunk_hash); // TODO: Validate existance!
            if (find_i == -1)
            {
                LONGTAIL_LOG("WriteVersion: Failed to find chunk 0x%" PRIx64 " in content index for asset `%s`\n", next_chunk_hash, path)
                LONGTAIL_FREE(awl);
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
    qsort_r(awl->m_BlockJobAssetIndexes, (rsize_t)awl->m_BlockJobCount, sizeof(uint32_t), BlockJobCompare, &block_job_compare_context);
    return awl;
}

struct DecompressBlockContext
{
    struct StorageAPI* m_StorageAPI;
    struct CompressionAPI* m_CompressonAPI;
    const char* m_ContentFolder;
    uint64_t m_BlockHash;
    void* m_UncompressedBlockData;
};

static void DecompressBlock(void* c)
{
    struct DecompressBlockContext* context = (struct DecompressBlockContext*)c;
    context->m_UncompressedBlockData = ReadBlockData(
        context->m_StorageAPI,
        context->m_CompressonAPI,
        context->m_ContentFolder,
        context->m_BlockHash);
    if (!context->m_UncompressedBlockData)
    {
        LONGTAIL_LOG("DecompressBlock: Failed to decompress block 0x%" PRIx64 " in content `%s`\n", context->m_BlockHash, context->m_ContentFolder)
    }
}

struct WriteAssetBlocksContext
{
    struct StorageAPI* m_StorageAPI;
    struct DecompressBlockContext* m_DecompressBlockContexts;
    const char* m_AssetsPath;
    uint32_t m_BlockCount;
    uint32_t m_ChunkOffset;
    uint32_t m_ChunkCount;
    uint32_t m_AssetIndex;
    const struct VersionIndex* m_VersionIndex;
    const struct ContentIndex* m_ContentIndex;
    struct ContentLookup* m_ContentLookup;
    int m_Success;
};

void WriteAssetBlocks(void* c)
{
    struct WriteAssetBlocksContext* context = (struct WriteAssetBlocksContext*)c;

    int decompressed_block_count = 0;
    for (uint32_t block_index = 0; block_index < context->m_BlockCount; ++block_index)
    {
        if (context->m_DecompressBlockContexts[block_index].m_UncompressedBlockData)
        {
            ++decompressed_block_count;
        }
    }

    if (decompressed_block_count != context->m_BlockCount)
    {
        while (context->m_BlockCount--)
        {
            context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-1;
            LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
            context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
        }
        return;
    }

    const char* path = &context->m_VersionIndex->m_NameData[context->m_VersionIndex->m_NameOffsets[context->m_AssetIndex]];
    char* full_path = context->m_StorageAPI->ConcatPath(context->m_StorageAPI, context->m_AssetsPath, path);
    if (!full_path)
    {
        while (context->m_BlockCount--)
        {
            context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-2;
            LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
            context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
        }
        return;
    }

    if (!EnsureParentPathExists(context->m_StorageAPI, full_path))
    {
        LONGTAIL_LOG("ChangeVersion: Failed to create parent path for `%s`\n", full_path);
        while (context->m_BlockCount--)
        {
            context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-3;
            LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
            context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
        }
        free(full_path);
        full_path = 0;
        return;
    }

    if (IsDirPath(full_path))
    {
        int ok = SafeCreateDir(context->m_StorageAPI, full_path);
        if (!ok)
        {
            LONGTAIL_LOG("WriteAssetFromBlocks: Failed to create folder for `%s`\n", full_path)
            while (context->m_BlockCount--)
            {
                context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-4;
                LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
                context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
            }
            free(full_path);
            full_path = 0;
            return;
        }
        free(full_path);
        full_path = 0;
        context->m_Success = 1;
        return;
    }

    StorageAPI_HOpenFile asset_file = context->m_StorageAPI->OpenWriteFile(context->m_StorageAPI, full_path, 0);
    if (!asset_file)
    {
        free(full_path);
        full_path = 0;
        while (context->m_BlockCount--)
        {
            context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-5;
            LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
            context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
        }
        return;
    }

    uint32_t chunk_index_start = context->m_VersionIndex->m_AssetChunkIndexStarts[context->m_AssetIndex];
    const uint32_t* asset_chunk_indexes = &context->m_VersionIndex->m_AssetChunkIndexes[chunk_index_start];
    uint64_t write_pos = 0;
    for (uint32_t o = 0; o < context->m_ChunkOffset; ++o)
    {
        uint32_t chunk_index = asset_chunk_indexes[o];
        uint32_t chunk_size = context->m_VersionIndex->m_ChunkSizes[chunk_index];
        write_pos += chunk_size;
    }
    asset_chunk_indexes += context->m_ChunkOffset;
    for (uint32_t c = 0 ; c < context->m_ChunkCount; ++c)
    {
        uint32_t chunk_index = asset_chunk_indexes[c];
        TLongtail_Hash chunk_hash = context->m_VersionIndex->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = context->m_VersionIndex->m_ChunkSizes[chunk_index];
        intptr_t hmtmp;
        intptr_t content_chunk_index_ptr = hmgeti_ts(context->m_ContentLookup->m_ChunkHashToChunkIndex, chunk_hash, hmtmp);
        LONGTAIL_FATAL_ASSERT_PRIVATE(content_chunk_index_ptr != -1, return;)
        uint64_t content_chunk_index = context->m_ContentLookup->m_ChunkHashToChunkIndex[content_chunk_index_ptr].value;
        uint64_t block_index = context->m_ContentIndex->m_ChunkBlockIndexes[content_chunk_index];
        TLongtail_Hash block_hash = context->m_ContentIndex->m_BlockHashes[block_index];
        uint32_t decompressed_block_index = 0;
        while (context->m_DecompressBlockContexts[decompressed_block_index].m_BlockHash != block_hash)
        {
            LONGTAIL_FATAL_ASSERT_PRIVATE(context->m_DecompressBlockContexts[decompressed_block_index].m_UncompressedBlockData, return)
            ++decompressed_block_index;
            LONGTAIL_FATAL_ASSERT_PRIVATE(decompressed_block_index < context->m_BlockCount, return)
        }
        const uint8_t* block_data = context->m_DecompressBlockContexts[decompressed_block_index].m_UncompressedBlockData;
        uint64_t read_offset = context->m_ContentIndex->m_ChunkBlockOffsets[content_chunk_index];
        if (!context->m_StorageAPI->Write(context->m_StorageAPI, asset_file, write_pos, chunk_size, &block_data[read_offset]))
        {
            context->m_StorageAPI->CloseWrite(context->m_StorageAPI, asset_file);
            asset_file = 0;
            free(full_path);
            full_path = 0;
            while (context->m_BlockCount--)
            {
                LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
                context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-7;
                context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
            }
            return;
        }
        write_pos += chunk_size;
    }

    if (context->m_ChunkOffset + context->m_ChunkCount == *context->m_VersionIndex->m_AssetChunkCounts)
    {
        context->m_StorageAPI->SetSize(context->m_StorageAPI, asset_file, write_pos);
    }

    context->m_StorageAPI->CloseWrite(context->m_StorageAPI, asset_file);
    asset_file = 0;
    free(full_path);
    full_path = 0;

    while (context->m_BlockCount--)
    {
        LONGTAIL_FREE(context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData);
        context->m_DecompressBlockContexts[context->m_BlockCount].m_BlockHash = (TLongtail_Hash)-8;
        context->m_DecompressBlockContexts[context->m_BlockCount].m_UncompressedBlockData = 0;
    }

    context->m_Success = 1;
}

int WriteAssets(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const struct VersionIndex* optional_base_version,
    const char* content_path,
    const char* version_path,
    struct ContentLookup* cl,
    struct AssetWriteList* awl)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_storage_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(job_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version_path != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(cl != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(awl != 0, return 0);

    if (!job_api->ReserveJobs(job_api, awl->m_BlockJobCount + awl->m_AssetJobCount))
    {
        LONGTAIL_LOG("WriteAssets: Failed to reserve %u jobs for folder `%s`\n", awl->m_BlockJobCount + awl->m_AssetJobCount, version_path)
        LONGTAIL_FREE(awl);
        awl = 0;
        DeleteContentLookup(cl);
        cl = 0;
        return 0;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)LONGTAIL_MALLOC((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    uint32_t b = 0;
    uint32_t block_job_count = 0;
    while (b < awl->m_BlockJobCount)
    {
        uint32_t asset_index = awl->m_BlockJobAssetIndexes[b];
        TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
        uint64_t block_index = hmget(cl->m_ChunkHashToBlockIndex, first_chunk_hash);
        struct WriteAssetsFromBlockJob* job = &block_jobs[block_job_count++];
        job->m_ContentStorageAPI = content_storage_api;
        job->m_VersionStorageAPI = version_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_ContentFolder = content_path;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = (uint64_t)block_index;
        job->m_ContentChunkLookup = cl->m_ChunkHashToChunkIndex;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[b];

        job->m_AssetCount = 1;
        ++b;
        while (b < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[b];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            intptr_t next_block_index_ptr = hmgeti(cl->m_ChunkHashToBlockIndex, next_first_chunk_hash);
            LONGTAIL_FATAL_ASSERT_PRIVATE(-1 != next_block_index_ptr, return 0)
            uint64_t next_block_index = cl->m_ChunkHashToBlockIndex[next_block_index_ptr].value;
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
        job_api->ReadyJobs(job_api, 1, block_write_job);
    }

    struct HashToIndexItem* base_asset_lookup = 0;
    if (optional_base_version)
    {
        for (uint32_t a = 0; a < *optional_base_version->m_AssetCount; ++a)
        {
            hmput(base_asset_lookup, optional_base_version->m_ContentHashes[a], a);
        }
    }

    struct WriteAssetFromBlocksJob* asset_jobs = (struct WriteAssetFromBlocksJob*)LONGTAIL_MALLOC(sizeof(struct WriteAssetFromBlocksJob) * awl->m_AssetJobCount);
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        struct WriteAssetFromBlocksJob* job = &asset_jobs[a];
        job->m_ContentStorageAPI = content_storage_api;
        job->m_VersionStorageAPI = version_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_ContentFolder = content_path;
        job->m_VersionFolder = version_path;
        job->m_ContentChunkLookup = cl->m_ChunkHashToChunkIndex;
        job->m_AssetIndex = awl->m_AssetIndexJobs[a];
        job->m_BaseVersionIndex = 0;
        job->m_BaseAssetIndex = 0;
        intptr_t base_asset_ptr = hmgeti(base_asset_lookup, version_index->m_ContentHashes[job->m_AssetIndex]);
        if (base_asset_ptr != -1)
        {
            job->m_BaseVersionIndex = optional_base_version;
            job->m_BaseAssetIndex = base_asset_lookup[base_asset_ptr].value;
        }

        JobAPI_JobFunc func[1] = { WriteAssetFromBlocks };
        void* ctx[1] = { job };

        JobAPI_Jobs asset_write_job = job_api->CreateJobs(job_api, 1, func, ctx);
        LONGTAIL_FATAL_ASSERT_PRIVATE(asset_write_job != 0, return 0)
        job_api->ReadyJobs(job_api, 1, asset_write_job);
        //WriteAssetFromBlocks(job);
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
        struct WriteAssetFromBlocksJob* job = &asset_jobs[a];
        if (!job->m_Success)
        {
            LONGTAIL_LOG("WriteAssets: Failed to write multi block assets content from `%s` to folder `%s`\n", content_path, version_path)
            success = 0;
        }
    }

    LONGTAIL_FREE(asset_jobs);
    asset_jobs = 0;
    LONGTAIL_FREE(block_jobs);
    block_jobs = 0;

    return success;
#if 0
    uint32_t decompress_blocks_per_write = job_api->GetWorkerCount(job_api);//GetCPUCount();
    uint64_t decompress_job_count = 0;
    uint64_t write_job_count = 0;
    for (uint64_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        uint32_t asset_index = awl->m_AssetIndexJobs[a];
        uint64_t asset_size = version_index->m_AssetSizes[asset_index];
        if (asset_size == 0)
        {
            ++write_job_count;
            continue;
        }

        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];

        uint32_t block_count = 0;
        uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t chunk_offset = 0;

        uint32_t c = 0;
        while (c < asset_chunk_count)
        {
            uint32_t decompress_job_start_chunk_index = version_index->m_AssetChunkIndexes[chunk_index_start + c];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[decompress_job_start_chunk_index];
            intptr_t last_block_index_ptr = hmgeti(cl->m_ChunkHashToBlockIndex, chunk_hash);
            LONGTAIL_FATAL_ASSERT_PRIVATE(last_block_index_ptr != -1, return 0;)
            uint64_t last_block_index = cl->m_ChunkHashToBlockIndex[last_block_index_ptr].value;
            ++c;

            while (c < asset_chunk_count)
            {
                uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_start + c];
                TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
                intptr_t block_index_ptr = hmgeti(cl->m_ChunkHashToBlockIndex, chunk_hash);
                LONGTAIL_FATAL_ASSERT_PRIVATE(block_index_ptr != -1, return 0;)
                uint64_t block_index = cl->m_ChunkHashToBlockIndex[block_index_ptr].value;
                if (last_block_index != block_index)
                {
                    break;
                }
                ++c;
            }

            ++block_count;

            if (block_count == decompress_blocks_per_write || c == asset_chunk_count)
            {
                uint32_t chunk_count = c - chunk_offset;
                decompress_job_count += block_count;
                chunk_offset += chunk_count;
                ++write_job_count;
                block_count = 0;
            }
        }
    }
    if (!job_api->ReserveJobs(job_api, awl->m_BlockJobCount + write_job_count + decompress_job_count))
    {
        LONGTAIL_LOG("WriteAssets: Failed to reserve %u jobs for folder `%s`\n", awl->m_BlockJobCount + awl->m_AssetJobCount, version_path)
        LONGTAIL_FREE(awl);
        awl = 0;
        DeleteContentLookup(cl);
        cl = 0;
        return 0;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)LONGTAIL_MALLOC((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    uint32_t b = 0;
    uint32_t block_job_count = 0;
    while (b < awl->m_BlockJobCount)
    {
        uint32_t asset_index = awl->m_BlockJobAssetIndexes[b];
        TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
        intptr_t hmtmp;
        uintptr_t block_index_ptr = hmgeti_ts(cl->m_ChunkHashToBlockIndex, first_chunk_hash, hmtmp);
        LONGTAIL_FATAL_ASSERT_PRIVATE(block_index_ptr != -1, return 0;)
        uint64_t block_index = cl->m_ChunkHashToBlockIndex[block_index_ptr].value;
        struct WriteAssetsFromBlockJob* job = &block_jobs[block_job_count++];
        job->m_ContentStorageAPI = content_storage_api;
        job->m_VersionStorageAPI = version_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_ContentFolder = content_path;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = (uint64_t)block_index;
        job->m_ContentChunkLookup = cl->m_ChunkHashToChunkIndex;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[b];

        job->m_AssetCount = 1;
        ++b;
        while (b < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[b];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            intptr_t hmtmp;
            intptr_t next_block_index_ptr = hmgeti_ts(cl->m_ChunkHashToBlockIndex, next_first_chunk_hash, hmtmp);
            LONGTAIL_FATAL_ASSERT_PRIVATE(-1 != next_block_index_ptr, return 0)
            uint64_t next_block_index = cl->m_ChunkHashToBlockIndex[next_block_index_ptr].value;
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

        job_api->ReadyJobs(job_api, 1, block_write_job);
    }

    struct HashToIndexItem* base_asset_lookup = 0;
    if (optional_base_version)
    {
        for (uint32_t a = 0; a < *optional_base_version->m_AssetCount; ++a)
        {
            hmput(base_asset_lookup, optional_base_version->m_ContentHashes[a], a);
        }
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
    for (uint32_t a = 0; a < write_job_count; ++a)
    {
        struct WriteAssetBlocksContext* job = &write_blocks_contexts[a];
        if (!job->m_Success)
        {
            LONGTAIL_LOG("WriteAssets: Failed to write multi block assets content from `%s` to folder `%s`\n", content_path, version_path)
            success = 0;
        }
    }

    LONGTAIL_FREE(decompres_job_funcs);
    decompres_job_funcs = 0;
    LONGTAIL_FREE(write_blocks_contexts);
    write_blocks_contexts = 0;
    LONGTAIL_FREE(decompress_block_contexts);
    decompress_block_contexts = 0;
    LONGTAIL_FREE(block_jobs);
    block_jobs = 0;

    return success;
#endif // 0
}


int WriteVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionAPI* compression_api,
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
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_api != 0, return 0);
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
    struct ContentLookup* cl = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);
    if (!cl)
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
        cl);

    if (!awl)
    {
        LONGTAIL_LOG("WriteVersion: Failed to create asset write list for version `%s`\n", content_path)
        DeleteContentLookup(cl);
        cl = 0;
        return 0;
    }

    int success = WriteAssets(
        content_storage_api,
        version_storage_api,
        compression_api,
        job_api,
        job_progress_func,
        job_progress_context,
        content_index,
        version_index,
        0,
        content_path,
        version_path,
        cl,
        awl);

    LONGTAIL_FREE(awl);
    awl = 0;

    DeleteContentLookup(cl);
    cl = 0;

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

    free(full_path);
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

    free(full_block_path);
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
        LONGTAIL_FREE(context.m_Paths);
        context.m_Paths = 0;
        return 0;
    }
    paths = context.m_Paths;
    context.m_Paths = 0;

    if (!job_api->ReserveJobs(job_api, *paths->m_PathCount))
    {
        LONGTAIL_LOG("ReadContent: Failed to reserve jobs for `%s`\n", content_path)
        LONGTAIL_FREE(paths);
        paths = 0;
        return 0;
    }

    LONGTAIL_LOG("ReadContent: Scanning %u files from `%s`\n", *paths->m_PathCount, content_path);

    struct ScanBlockJob* jobs = (struct ScanBlockJob*)LONGTAIL_MALLOC(sizeof(struct ScanBlockJob) * *paths->m_PathCount);

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
    struct ContentIndex* content_index = (struct ContentIndex*)LONGTAIL_MALLOC(sizeof(struct ContentIndex) + content_index_data_size);
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

            LONGTAIL_FREE(job->m_BlockIndex);
            job->m_BlockIndex = 0;
        }
    }

    LONGTAIL_FREE(jobs);
    jobs = 0;

    LONGTAIL_FREE(paths);
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

    TLongtail_Hash* refs = (TLongtail_Hash*)LONGTAIL_MALLOC((size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    TLongtail_Hash* news = (TLongtail_Hash*)LONGTAIL_MALLOC((size_t)(sizeof(TLongtail_Hash) * new_hash_count));
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

    LONGTAIL_FREE(news);
    news = 0;
    LONGTAIL_FREE(refs);
    refs = 0;
}

struct ContentIndex* CreateMissingContent(
    struct HashAPI* hash_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block)
{
    LONGTAIL_FATAL_ASSERT_PRIVATE(hash_api != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(content_index != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(version != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_block_size != 0, return 0);
    LONGTAIL_FATAL_ASSERT_PRIVATE(max_chunks_per_block != 0, return 0);

    LONGTAIL_LOG("CreateMissingContent: Checking for %u version chunks in %" PRIu64 " content chunks\n", *version->m_ChunkCount, *content_index->m_ChunkCount)
    uint64_t chunk_count = *version->m_ChunkCount;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC((size_t)(sizeof(TLongtail_Hash) * chunk_count));

    uint64_t added_hash_count = 0;
    DiffHashes(
        content_index->m_ChunkHashes,
        *content_index->m_ChunkCount,
        version->m_ChunkHashes,
        chunk_count,
        &added_hash_count,
        added_hashes,
        0,
        0);

    if (added_hash_count == 0)
    {
        LONGTAIL_FREE(added_hashes);
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

    uint32_t* diff_chunk_sizes = (uint32_t*)LONGTAIL_MALLOC((size_t)(sizeof(uint32_t) * added_hash_count));
    uint32_t* diff_chunk_compression_types = (uint32_t*)LONGTAIL_MALLOC((size_t)(sizeof(uint32_t) * added_hash_count));

    struct HashToIndexItem* chunk_index_lookup = 0;
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        hmput(chunk_index_lookup, version->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        uint64_t chunk_index = hmget(chunk_index_lookup, added_hashes[j]);
        diff_chunk_sizes[j] = version->m_ChunkSizes[chunk_index];
        diff_chunk_compression_types[j] = version->m_ChunkCompressionTypes[chunk_index];
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

    LONGTAIL_FREE(diff_chunk_compression_types);
    diff_chunk_compression_types = 0;
    LONGTAIL_FREE(diff_chunk_sizes);
    diff_chunk_sizes = 0;
    LONGTAIL_FREE(added_hashes);
    added_hashes = 0;

    return diff_content_index;
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
    struct ContentIndex* content_index = (struct ContentIndex*)LONGTAIL_MALLOC(content_index_size);

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
#if defined(_MSC_VER)
static int SortPathShortToLong(void* context, const void* a_ptr, const void* b_ptr)
#else
static int SortPathShortToLong(const void* a_ptr, const void* b_ptr, void* context)
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

#if defined(_MSC_VER)
static int SortPathLongToShort(void* context, const void* a_ptr, const void* b_ptr)
#else
static int SortPathLongToShort(const void* a_ptr, const void* b_ptr, void* context)
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

    TLongtail_Hash* source_path_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof (TLongtail_Hash) * source_asset_count);
    TLongtail_Hash* target_path_hashes = (TLongtail_Hash*)LONGTAIL_MALLOC(sizeof (TLongtail_Hash) * target_asset_count);

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

    uint32_t* removed_source_asset_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * source_asset_count);
    uint32_t* added_target_asset_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * target_asset_count);

    const uint32_t max_modified_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    uint32_t* modified_source_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * max_modified_count);
    uint32_t* modified_target_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * max_modified_count);

    uint32_t source_removed_count = 0;
    uint32_t target_added_count = 0;
    uint32_t modified_count = 0;

    uint32_t source_index = 0;
    uint32_t target_index = 0;
    while (source_index < source_asset_count && target_index < target_asset_count)
    {
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        if (source_path_hash == target_path_hash)
        {
            uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
            TLongtail_Hash source_content_hash = source_version->m_ContentHashes[source_asset_index];
            uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
            TLongtail_Hash target_content_hash = target_version->m_ContentHashes[target_asset_index];
            if (source_content_hash != target_content_hash)
            {
                modified_source_indexes[modified_count] = source_asset_index;
                modified_target_indexes[modified_count] = target_asset_index;
                ++modified_count;
                if (modified_count < 10)
                {
                    LONGTAIL_LOG("CreateVersionDiff: Missmatching content for asset `%s`\n", &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]])
                }
            }
            ++source_index;
            ++target_index;
        } else if (source_path_hash < target_path_hash)
        {
            uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        } else
        {
            uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
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
        removed_source_asset_indexes[source_removed_count] = source_asset_index;
        ++source_removed_count;
        ++source_index;
    }
    while (target_index < target_asset_count)
    {
        // target_path_hash added
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
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
        LONGTAIL_LOG("CreateVersionDiff: Missmatching content for %u assets found\n", modified_count)
    }

    struct VersionDiff* version_diff = (struct VersionDiff*)LONGTAIL_MALLOC(GetVersionDiffSize(source_removed_count, target_added_count, modified_count));
    uint32_t* counts_ptr = (uint32_t*)&version_diff[1];
    counts_ptr[0] = source_removed_count;
    counts_ptr[1] = target_added_count;
    counts_ptr[2] = modified_count;
    InitVersionDiff(version_diff);

    memmove(version_diff->m_SourceRemovedAssetIndexes, removed_source_asset_indexes, sizeof(uint32_t) * source_removed_count);
    memmove(version_diff->m_TargetAddedAssetIndexes, added_target_asset_indexes, sizeof(uint32_t) * target_added_count);
    memmove(version_diff->m_SourceModifiedAssetIndexes, modified_source_indexes, sizeof(uint32_t) * modified_count);
    memmove(version_diff->m_TargetModifiedAssetIndexes, modified_target_indexes, sizeof(uint32_t) * modified_count);

    qsort_r(version_diff->m_SourceRemovedAssetIndexes, source_removed_count, sizeof(uint32_t), SortPathLongToShort, (void*)source_version);
    qsort_r(version_diff->m_TargetAddedAssetIndexes, target_added_count, sizeof(uint32_t), SortPathShortToLong, (void*)target_version);

    LONGTAIL_FREE(target_path_hashes);
    target_path_hashes = 0;

    LONGTAIL_FREE(target_path_hashes);
    target_path_hashes = 0;

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
    struct CompressionAPI* compression_api,
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
    LONGTAIL_FATAL_ASSERT_PRIVATE(compression_api != 0, return 0);
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
    struct ContentLookup* cl = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);
    if (!cl)
    {
        LONGTAIL_LOG("ChangeVersion: Failed create content lookup for content `%s`\n", content_path);
        return 0;
    }

    for (uint32_t i = 0; i < *target_version->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[i];
        intptr_t chunk_content_index_ptr = hmgeti(cl->m_ChunkHashToChunkIndex, chunk_hash);
        if (-1 == chunk_content_index_ptr)
        {
            LONGTAIL_LOG("ChangeVersion: Not all chunks in target version in `%s` is available in content folder `%s`\n", version_path, content_path);
            DeleteContentLookup(cl);
            cl = 0;
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
            intptr_t chunk_content_index_ptr = hmgeti(cl->m_ChunkHashToChunkIndex, chunk_hash);
            if (-1 == chunk_content_index_ptr)
            {
                LONGTAIL_LOG("ChangeVersion: Not all chunks for asset `%s` is in target version in `%s` is available in content folder `%s`\n", target_name, version_path, content_path);
                DeleteContentLookup(cl);
                cl = 0;
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
                            free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(cl);
                            cl = 0;
                            return 0;
                        }
                        free(full_asset_path);
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
                            free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(cl);
                            cl = 0;
                            return 0;
                        }
                        free(full_asset_path);
                        full_asset_path = 0;
                        break;
                    }
                }
            }
            ++successful_remove_count;
            free(full_asset_path);
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

    uint32_t* asset_indexes = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * asset_count);
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
        cl);

    if (!awl)
    {
        LONGTAIL_LOG("ChangeVersion: Failed to create asset write list for version `%s`\n", content_path)
        DeleteContentLookup(cl);
        cl = 0;
        return 0;
    }

    int success = WriteAssets(
        content_storage_api,
        version_storage_api,
        compression_api,
        job_api,
        job_progress_func,
        job_progress_context,
        content_index,
        target_version,
        source_version,
        content_path,
        version_path,
        cl,
        awl);

    LONGTAIL_FREE(awl);
    awl = 0;

    DeleteContentLookup(cl);
    cl = 0;

    return success;
}

int ValidateContent(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index)
{
    struct ContentLookup* cl = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);

    if (!cl)
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
            intptr_t content_chunk_index_ptr = hmget(cl->m_ChunkHashToChunkIndex, chunk_hash);
            if (content_chunk_index_ptr == -1)
            {
                DeleteContentLookup(cl);
                cl = 0;
                return 0;
            }
            if (content_index->m_ChunkHashes[content_chunk_index_ptr] != chunk_hash)
            {
                DeleteContentLookup(cl);
                cl = 0;
                return 0;
            }
            if (content_index->m_ChunkLengths[content_chunk_index_ptr] != chunk_size)
            {
                DeleteContentLookup(cl);
                cl = 0;
                return 0;
            }
        }
        if (asset_chunked_size != asset_size)
        {
            DeleteContentLookup(cl);
            cl = 0;
            return 0;
        }
    }

    DeleteContentLookup(cl);
    cl = 0;

    return 1;
}

int ValidateVersion(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index)
{
    struct HashToIndexItem* version_chunk_lookup = 0;

    struct ContentLookup* cl = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes);

    if (!cl)
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
    struct Chunker* c = (struct Chunker*)LONGTAIL_MALLOC((size_t)((sizeof(struct Chunker) + params->max)));
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
