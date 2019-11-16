#include "longtail.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "stb_ds.h"

#ifdef LONGTAIL_VERBOSE_LOGS
    #define LONGTAIL_LOG(fmt, ...) \
        printf("--- ");printf(fmt, __VA_ARGS__);
#else
    #define LONGTAIL_LOG(fmr, ...)
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

TLongtail_Hash GetPathHash(struct HashAPI* hash_api, const char* path)
{
    HashAPI_HContext context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, context, (uint32_t)strlen(path), (void*)path);
    return (TLongtail_Hash)hash_api->EndContext(hash_api, context);
}

int SafeCreateDir(struct StorageAPI* storage_api, const char* path)
{
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
    char* dir_path = strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        free(dir_path);
        return 1;
    }
    else
    {
        if (!EnsureParentPathExists(storage_api, dir_path))
        {
            LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
            free(dir_path);
            return 0;
        }
        if (SafeCreateDir(storage_api, dir_path))
        {
            free(dir_path);
            return 1;
        }
    }
    int ok = EnsureParentPathExists(storage_api, dir_path);
    if (!ok)
    {
        LONGTAIL_LOG("EnsureParentPathExists failed: `%s`\n", dir_path)
    }
    free(dir_path);
    return ok;
}

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

int RecurseTree(struct StorageAPI* storage_api, const char* root_folder, ProcessEntry entry_processor, void* context)
{
    LONGTAIL_LOG("RecurseTree `%s`\n", root_folder)

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
                    entry_processor(context, asset_folder, dir_name);
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
                        entry_processor(context, asset_folder, file_name);
                    }
                }
            }while(storage_api->FindNext(storage_api, fs_iterator));
            storage_api->CloseFind(storage_api, fs_iterator);
        }
        free((void*)asset_folder);
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
    struct Paths* paths = (struct Paths*)malloc(GetPathsSize(path_count, path_data_size));
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

        free(paths);
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
};

void AddFile(void* context, const char* root_path, const char* file_name)
{
    struct AddFile_Context* paths_context = (struct AddFile_Context*)context;
    struct StorageAPI* storage_api = paths_context->m_StorageAPI;

    char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
    if (storage_api->IsDir(storage_api, full_path))
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

    free(full_path);
    full_path = 0;
}

struct Paths* GetFilesRecursively(struct StorageAPI* storage_api, const char* root_path)
{
    LONGTAIL_LOG("GetFilesRecursively `%s`\n", root_path)
    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    struct AddFile_Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), paths};
    paths = 0;

    if(!RecurseTree(storage_api, root_path, AddFile, &context))
    {
        free(context.m_Paths);
        return 0;
    }

    return context.m_Paths;
}

struct HashJob
{
    struct StorageAPI* m_StorageAPI;
    struct HashAPI* m_HashAPI;
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_ContentHash;
    uint32_t* m_ContentSize;
    uint32_t* m_AssetChunkCount;
    uint32_t* m_AssetChunkIndexStart;
    const char* m_RootPath;
    const char* m_Path;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkSizes;
    uint32_t m_MaxChunkSize;
    volatile int32_t* m_ChunkHashesOffset;
    int m_Success;
};

void HashFile(void* context)
{
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Success = 0;

    *hash_job->m_AssetChunkIndexStart = 0;
    *hash_job->m_AssetChunkCount = 0;
    *hash_job->m_ContentSize = 0;
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
        free(path);
        return;
    }

    uint32_t asset_size = (uint32_t)storage_api->GetSize(storage_api, file_handle);
    uint8_t* batch_data = (uint8_t*)malloc(hash_job->m_MaxChunkSize);
    uint32_t max_chunks = (asset_size + hash_job->m_MaxChunkSize - 1) / hash_job->m_MaxChunkSize;

    hash_job->m_ChunkHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * max_chunks);
	hash_job->m_ChunkSizes = (uint32_t*)malloc(sizeof(uint32_t) * max_chunks);

    HashAPI_HContext asset_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);

    uint32_t offset = 0;
    while (offset != asset_size)
    {
        uint32_t len = (uint32_t)((asset_size - offset) < hash_job->m_MaxChunkSize ? (asset_size - offset) : hash_job->m_MaxChunkSize);
        int read_ok = storage_api->Read(storage_api, file_handle, offset, len, batch_data);
        if (!read_ok)
        {
            LONGTAIL_LOG("HashFile failed to read from `%s`\n", path)
            hash_job->m_Success = 0;
            free(hash_job->m_ChunkSizes);
			hash_job->m_ChunkSizes = 0;
            free(hash_job->m_ChunkHashes);
			hash_job->m_ChunkHashes = 0;
            storage_api->CloseRead(storage_api, file_handle);
            file_handle = 0;
            free(path);
            return;
        }

        {
            HashAPI_HContext chunk_hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
            hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, chunk_hash_context, len, batch_data);
            TLongtail_Hash chunk_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, chunk_hash_context);
			hash_job->m_ChunkHashes[chunk_count] = chunk_hash;
			hash_job->m_ChunkSizes[chunk_count] = len;
        }
        ++chunk_count;

        offset += len;
        hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, len, batch_data);
    }

    TLongtail_Hash content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);

    storage_api->CloseRead(storage_api, file_handle);
    file_handle = 0;
    
    int chunk_hashes_start = LONGTAIL_ATOMICADD_PRIVATE(hash_job->m_ChunkHashesOffset, (int32_t)chunk_count) - chunk_count;

    *hash_job->m_ContentSize = asset_size;
    *hash_job->m_ContentHash = content_hash;
    *hash_job->m_ContentSize = asset_size;
    *hash_job->m_AssetChunkIndexStart = chunk_hashes_start;
    *hash_job->m_AssetChunkCount = chunk_count;

    free((char*)path);
    path = 0;

    hash_job->m_Success = 1;
}

int ChunkAssets(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    const char* root_path,
    const struct Paths* paths,
    TLongtail_Hash* pathHashes,
    TLongtail_Hash* contentHashes,
    uint32_t* contentSizes,
    uint32_t* asset_chunk_start_index,
    uint32_t* asset_chunk_counts,
    uint32_t** chunk_sizes,
    TLongtail_Hash** chunk_hashes,
    uint32_t max_chunk_size,
    uint32_t* chunk_count)
{
    LONGTAIL_LOG("ChunkAssets in folder `%s` for %u assets\n", root_path, (uint32_t)*paths->m_PathCount)
    uint32_t asset_count = *paths->m_PathCount;

    if (job_api)
    {
        if (!job_api->ReserveJobs(job_api, asset_count))
        {
            return 0;
        }
    }
    struct HashJob* hash_jobs = (struct HashJob*)malloc(sizeof(struct HashJob) * asset_count);
    volatile int32_t chunk_hashes_offset = 0;

    uint64_t assets_left = asset_count;
    static const uint32_t BATCH_SIZE = 64;
    JobAPI_JobFunc* func = (JobAPI_JobFunc*)malloc(sizeof(JobAPI_JobFunc) * BATCH_SIZE);
    void** ctx = (void*)malloc(sizeof(void*) * BATCH_SIZE);
    for (uint32_t i = 0; i < BATCH_SIZE; ++i)
    {
        func[i] = HashFile;
    }
    uint64_t offset = 0;
    while (offset < asset_count) {
        uint64_t assets_left = asset_count - offset;
        uint32_t batch_count = assets_left > BATCH_SIZE ? BATCH_SIZE : (uint32_t)assets_left;
        for (uint32_t i = 0; i < batch_count; ++i)
        {
            struct HashJob* job = &hash_jobs[offset + i];
            ctx[i] = &hash_jobs[i + offset];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_RootPath = root_path;
            job->m_Path = &paths->m_Data[paths->m_Offsets[i + offset]];
            job->m_PathHash = &pathHashes[i + offset];
            job->m_ContentHash = &contentHashes[i + offset];
            job->m_ContentSize = &contentSizes[i + offset];
            job->m_AssetChunkCount = &asset_chunk_counts[i + offset];
            job->m_AssetChunkIndexStart = &asset_chunk_start_index[i + offset];
            job->m_ChunkHashes = 0;
            job->m_ChunkSizes = 0;
            job->m_MaxChunkSize = max_chunk_size;
            job->m_ChunkHashesOffset = &chunk_hashes_offset;
            if (!job_api)
            {
                HashFile(job);
            }
        }

        if (job_api)
        {
            job_api->SubmitJobs(job_api, batch_count, func, ctx);
        }

        offset += batch_count;
    }

    if (job_api)
    {
        job_api->WaitForAllJobs(job_api);
    }

    free(ctx);
    ctx = 0;
    free(func);
    func = 0;

    int success = 1;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        if (!hash_jobs[i].m_Success)
        {
            success = 0;
            LONGTAIL_LOG("Failed to hash `%s`\n", hash_jobs[i].m_Path)
        }
    }

    if (success)
    {
        *chunk_count = (uint32_t)chunk_hashes_offset;
        *chunk_sizes = (uint32_t*)malloc(sizeof(uint32_t) * *chunk_count);
        *chunk_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * *chunk_count);

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < asset_count; ++i)
        {
            for (uint32_t chunk_index = 0; chunk_index < asset_chunk_counts[i]; ++chunk_index)
            {
                (*chunk_sizes)[chunk_offset] = hash_jobs[i].m_ChunkSizes[chunk_index];
                (*chunk_hashes)[chunk_offset] = hash_jobs[i].m_ChunkHashes[chunk_index];
                ++chunk_offset;
            }
        }
    }
    else
    {
        *chunk_count = 0;
        *chunk_sizes = 0;
        *chunk_hashes = 0;
    }

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        free(hash_jobs[i].m_ChunkHashes);
        hash_jobs[i].m_ChunkHashes = 0;
        free(hash_jobs[i].m_ChunkSizes);
        hash_jobs[i].m_ChunkSizes = 0;
    }

    free(hash_jobs);
    hash_jobs = 0;

    return success;
}

size_t GetVersionIndexDataSize(uint32_t asset_count, uint32_t chunk_count, uint32_t name_data_size)
{
    size_t version_index_size =
        sizeof(uint64_t) +                            // m_AssetCount
        sizeof(uint64_t) +                            // m_ChunkCount
        (sizeof(TLongtail_Hash) * asset_count) +      // m_PathHashes
        (sizeof(TLongtail_Hash) * asset_count) +      // m_ContentHashes
        (sizeof(uint32_t) * asset_count) +            // m_AssetSizes
        (sizeof(uint32_t) * asset_count) +            // m_AssetChunkCounts
        (sizeof(uint32_t) * chunk_count) +            // m_ChunkSizes
        (sizeof(TLongtail_Hash) * chunk_count) +      // m_ChunkHashes
        (sizeof(uint32_t) * asset_count) +            // m_NameOffsets
        name_data_size;

    return version_index_size;
}

size_t GetVersionIndexSize(uint32_t asset_count, uint32_t chunk_count, uint32_t path_data_size)
{
    return sizeof(struct VersionIndex) +
            GetVersionIndexDataSize(asset_count, chunk_count, path_data_size);
}

void InitVersionIndex(struct VersionIndex* version_index, size_t version_index_data_size)
{
    char* p = (char*)version_index;
    p += sizeof(struct VersionIndex);

    size_t version_index_data_start = (size_t)p;

    version_index->m_AssetCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t asset_count = *version_index->m_AssetCount;

    version_index->m_ChunkCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t chunk_count = *version_index->m_ChunkCount;

    version_index->m_PathHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_ContentHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetSizes = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_AssetChunkCounts = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_ChunkSizes = (uint32_t*)p;
    p += (sizeof(uint32_t) * chunk_count);

    version_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);

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
    const TLongtail_Hash* pathHashes,
    const TLongtail_Hash* contentHashes,
    const uint32_t* contentSizes,
    const uint32_t* asset_chunk_start_index,
    const uint32_t* asset_chunk_counts,
    uint64_t chunk_count,
    const uint32_t* chunk_sizes,
    const TLongtail_Hash* chunk_hashes)
{
    uint32_t asset_count = *paths->m_PathCount;
    struct VersionIndex* version_index = (struct VersionIndex*)mem;
    version_index->m_AssetCount = (uint64_t*)&((char*)mem)[sizeof(struct VersionIndex)];
    version_index->m_ChunkCount = (uint64_t*)&((char*)mem)[sizeof(struct VersionIndex) + sizeof(uint64_t)];
    *version_index->m_AssetCount = asset_count;
    *version_index->m_ChunkCount = chunk_count;

    InitVersionIndex(version_index, mem_size - sizeof(struct VersionIndex));

    uint32_t chunk_offset = 0;

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        version_index->m_PathHashes[i] = pathHashes[i];
        version_index->m_ContentHashes[i] = contentHashes[i];
        version_index->m_AssetSizes[i] = contentSizes[i];
        version_index->m_NameOffsets[i] = paths->m_Offsets[i];
        version_index->m_AssetChunkCounts[i] = asset_chunk_counts[i];
        for (uint32_t j = 0; j < asset_chunk_counts[i]; ++j)
        {
            version_index->m_ChunkSizes[chunk_offset] = chunk_sizes[asset_chunk_start_index[i] + j];
            version_index->m_ChunkHashes[chunk_offset] = chunk_hashes[asset_chunk_start_index[i] + j];
            ++chunk_offset;
        }
    }
    memmove(version_index->m_NameData, paths->m_Data, paths->m_DataSize);

    return version_index;
}

struct VersionIndex* CreateVersionIndex(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    const char* root_path,
    const struct Paths* paths,
    uint32_t max_chunk_size)
{
    uint32_t path_count = *paths->m_PathCount;
    uint32_t* contentSizes = (uint32_t*)malloc(sizeof(uint32_t) * path_count);
    TLongtail_Hash* pathHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * path_count);
    TLongtail_Hash* contentHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * path_count);
    uint32_t* asset_chunk_counts = (uint32_t*)malloc(sizeof(uint32_t) * path_count);

    uint32_t chunk_count = 0;
    uint32_t* chunk_sizes = 0;
    TLongtail_Hash* chunk_hashes = 0;
    uint32_t* asset_chunk_start_index = (uint32_t*)malloc(sizeof(uint32_t) * path_count);
    
    if (!ChunkAssets(
        storage_api,
        hash_api,
        job_api,
        root_path,
        paths,
        pathHashes,
        contentHashes,
        contentSizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        &chunk_sizes,
        &chunk_hashes,
        max_chunk_size,
        &chunk_count))
    {
        LONGTAIL_LOG("Failed to hash assets in `%s`\n", root_path);
        free(asset_chunk_start_index);
        free(chunk_hashes);
        free(chunk_sizes);
        free(contentHashes);
        free(pathHashes);
        free(contentSizes);
        return 0;
    }

    size_t version_index_size = GetVersionIndexSize(path_count, chunk_count, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    struct VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        pathHashes,
        contentHashes,
        contentSizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        chunk_count,
        chunk_sizes,
        chunk_hashes);

    free(asset_chunk_start_index);
    asset_chunk_start_index = 0;
    free(asset_chunk_counts);
    asset_chunk_counts = 0;
    free(chunk_sizes);
    chunk_sizes = 0;
    free(chunk_hashes);
    chunk_hashes = 0;
    free(contentHashes);
    contentHashes = 0;
    free(pathHashes);
    pathHashes = 0;
    free(contentSizes);
    contentSizes = 0;

    return version_index;
}

int WriteVersionIndex(struct StorageAPI* storage_api, struct VersionIndex* version_index, const char* path)
{
    LONGTAIL_LOG("WriteVersionIndex to `%s`\n", path)
    size_t index_data_size = GetVersionIndexDataSize((uint32_t)(*version_index->m_AssetCount), (uint32_t)(*version_index->m_ChunkCount), version_index->m_NameDataSize);

    if (!EnsureParentPathExists(storage_api, path))
    {
        return 0;
    }
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    if (!storage_api->Write(storage_api, file_handle, 0, index_data_size, &version_index[1]))
    {
        storage_api->CloseWrite(storage_api, file_handle);
        return 0;
    }
    storage_api->CloseWrite(storage_api, file_handle);

    return 1;
}

struct VersionIndex* ReadVersionIndex(struct StorageAPI* storage_api, const char* path)
{
    LONGTAIL_LOG("ReadVersionIndex from `%s`\n", path)
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    size_t version_index_data_size = storage_api->GetSize(storage_api, file_handle);
    struct VersionIndex* version_index = (struct VersionIndex*)malloc(sizeof(struct VersionIndex) + version_index_data_size);
    if (!version_index)
    {
        storage_api->CloseRead(storage_api, file_handle);
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]))
    {
        storage_api->CloseRead(storage_api, file_handle);
        return 0;
    }
    InitVersionIndex(version_index, version_index_data_size);
    storage_api->CloseRead(storage_api, file_handle);
    return version_index;
}

struct BlockIndex
{
    TLongtail_Hash m_BlockHash;
    TLongtail_Hash* m_ChunkHashes; //[]
    uint32_t* m_ChunkSizes; // []
    uint32_t* m_ChunkCount;
};

size_t GetBlockIndexDataSize(uint32_t chunk_count)
{
    return
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count) +          // m_ChunkSizes
        sizeof(uint32_t);                           // m_ChunkCount
}

struct BlockIndex* InitBlockIndex(void* mem, uint32_t asset_count)
{
    struct BlockIndex* block_index = (struct BlockIndex*)mem;
    char* p = (char*)&block_index[1];
    block_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash) * asset_count;
    block_index->m_ChunkSizes = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;
    block_index->m_ChunkCount = (uint32_t*)p;
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
    uint32_t chunk_count_in_block,
    uint32_t* chunk_indexes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes)
{
    struct BlockIndex* block_index = InitBlockIndex(mem, chunk_count_in_block);
    for (uint32_t i = 0; i < chunk_count_in_block; ++i)
    {
        uint32_t asset_index = chunk_indexes[i];
        block_index->m_ChunkHashes[i] = chunk_hashes[asset_index];
        block_index->m_ChunkSizes[i] = chunk_sizes[asset_index];
    }
    *block_index->m_ChunkCount = chunk_count_in_block;
    HashAPI_HContext hash_context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, hash_context, (uint32_t)(GetBlockIndexDataSize(chunk_count_in_block)), (void*)&block_index[1]);
    TLongtail_Hash block_hash = hash_api->EndContext(hash_api, hash_context);
    block_index->m_BlockHash = block_hash;

    return block_index;
}

size_t GetContentIndexDataSize(uint64_t block_count, uint64_t asset_count)
{
    size_t block_index_data_size = sizeof(uint64_t) +
        sizeof(uint64_t) +
        (sizeof(TLongtail_Hash) * block_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        (sizeof(uint32_t) * asset_count);

    return block_index_data_size;
}

size_t GetContentIndexSize(uint64_t block_count, uint64_t asset_count)
{
    return sizeof(struct ContentIndex) +
        GetContentIndexDataSize(block_count, asset_count);
}

void InitContentIndex(struct ContentIndex* content_index)
{
    char* p = (char*)&content_index[1];
    content_index->m_BlockCount = (uint64_t*)p;
    p += sizeof(uint64_t);
    content_index->m_ChunkCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t asset_count = *content_index->m_ChunkCount;

    content_index->m_BlockHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_ChunkHashes = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);
    content_index->m_ChunkBlockIndexes = (uint64_t*)p;
    p += (sizeof(uint64_t) * asset_count);
    content_index->m_ChunkBlockOffsets = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
    content_index->m_ChunkLengths = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
}

struct HashToIndexItem
{
    TLongtail_Hash key;
    uint32_t value;
};

uint32_t GetUniqueHashes(uint64_t asset_count, const TLongtail_Hash* hashes, uint32_t* out_unique_hash_indexes)
{
    struct HashToIndexItem* lookup_table = 0;

    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash hash = hashes[i];
        ptrdiff_t lookup_index = hmgeti(lookup_table, hash);
        if (lookup_index == -1)
        {
            hmput(lookup_table, hash, 1);
            out_unique_hash_indexes[unique_asset_count] = i;
            ++unique_asset_count;
        }
        else
        {
            ++lookup_table[lookup_index].value;
        }
    }
    hmfree(lookup_table);
    lookup_table = 0;
    return unique_asset_count;
}

struct CompareAssetEntry
{
    // This sorting algorithm is very arbitrary!
    const TLongtail_Hash* asset_path_hashes;
    const uint32_t* asset_sizes;
    const TLongtail_Hash* asset_tags;
};

static int CompareAssetEntryCompare(void* context, const void* a_ptr, const void* b_ptr)
{   
    uint32_t a = *(uint32_t*)a_ptr;
    uint32_t b = *(uint32_t*)b_ptr;
    const struct CompareAssetEntry* c = (const struct CompareAssetEntry*)context;
    TLongtail_Hash a_tag = c->asset_tags[a];
    TLongtail_Hash b_tag = c->asset_tags[b];
    if (a_tag > b_tag)
    {
        return 1;
    }
    else if (b_tag > a_tag)
    {
        return -1;
    }
    uint32_t a_size = c->asset_sizes[a];
    uint32_t b_size = c->asset_sizes[b];
    if (a_size > b_size)
    {
        return 1;
    }
    else if (b_size > a_size)
    {
        return -1;
    }
    TLongtail_Hash a_hash = c->asset_path_hashes[a];
    TLongtail_Hash b_hash = c->asset_path_hashes[b];
    if (a_hash > b_hash)
    {
        return 1;
    }
    else if (b_hash > a_hash)
    {
        return -1;
    }
    return 0;
}

struct ContentIndex* CreateContentIndex(
    struct HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block)
{
    LONGTAIL_LOG("CreateContentIndex\n")
    if (chunk_count == 0)
    {
        size_t content_index_size = GetContentIndexSize(0, 0);
        struct ContentIndex* content_index = (struct ContentIndex*)malloc(content_index_size);

        content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
        content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
        *content_index->m_BlockCount = 0;
        *content_index->m_ChunkCount = 0;
        InitContentIndex(content_index);
        return content_index;
    }
    uint32_t* chunk_indexes = (uint32_t*)malloc(sizeof(uint32_t) * chunk_count);
    uint32_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, chunk_indexes);

//    struct CompareAssetEntry compare_asset_entry = {asset_path_hashes, asset_sizes, content_tags};
//    qsort_s(&assets_index[0], unique_asset_count, sizeof(uint32_t), CompareAssetEntryCompare, &compare_asset_entry);

    struct BlockIndex** block_indexes = (struct BlockIndex**)malloc(sizeof(struct BlockIndex*) * unique_chunk_count);

    #define MAX_ASSETS_PER_BLOCK 16384u
    uint32_t* stored_chunk_indexes = (uint32_t*)malloc(sizeof(uint32_t) * max_chunks_per_block);

    uint32_t current_size = 0;
    uint64_t i = 0;
    uint32_t asset_count_in_block = 0;
    uint32_t block_count = 0;

    while (i < unique_chunk_count)
    {
        asset_count_in_block = 0;

        uint64_t chunk_index = chunk_indexes[i];

        uint32_t current_size = chunk_sizes[chunk_index];

        stored_chunk_indexes[asset_count_in_block] = chunk_index;
        ++asset_count_in_block;

        while((i + 1) < unique_chunk_count)
        {
            chunk_index = chunk_indexes[(i + 1)];
            uint32_t asset_size = chunk_sizes[chunk_index];

            // Break if resulting asset count will exceed MAX_ASSETS_PER_BLOCK
            if (asset_count_in_block == max_chunks_per_block)
            {
                break;
            }

            if ((current_size + asset_size) > max_block_size)
            {
                break;
            }

            current_size += asset_size;
            stored_chunk_indexes[asset_count_in_block] = chunk_index;
            ++asset_count_in_block;

            ++i;
        }

        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            hash_api,
            asset_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes);

        ++block_count;
        ++i;
    }

    if (current_size > 0)
    {
        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            hash_api,
            asset_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes);
        ++block_count;
    }

    free(stored_chunk_indexes);
    stored_chunk_indexes = 0;
    free(chunk_indexes);
    chunk_indexes = 0;

    // Build Content Index (from block list)
    size_t content_index_size = GetContentIndexSize(block_count, unique_chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)malloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = unique_chunk_count;
    InitContentIndex(content_index);

    uint64_t asset_index = 0;
    for (uint32_t i = 0; i < block_count; ++i)
    {
        struct BlockIndex* block_index = block_indexes[i];
        content_index->m_BlockHashes[i] = block_index->m_BlockHash;
        uint64_t asset_offset = 0;
        for (uint32_t a = 0; a < *block_index->m_ChunkCount; ++a)
        {
            content_index->m_ChunkHashes[asset_index] = block_index->m_ChunkHashes[a];
            content_index->m_ChunkBlockIndexes[asset_index] = i;
            content_index->m_ChunkBlockOffsets[asset_index] = asset_offset;
            content_index->m_ChunkLengths[asset_index] = block_index->m_ChunkSizes[a];

            asset_offset += block_index->m_ChunkSizes[a];
            ++asset_index;
            if (asset_index > unique_chunk_count)
            {
                break;
            }
        }
        free(block_index);
        block_index = 0;
    }
    free(block_indexes);
    block_indexes = 0;

    return content_index;
}

int WriteContentIndex(struct StorageAPI* storage_api, struct ContentIndex* content_index, const char* path)
{
    LONGTAIL_LOG("WriteContentIndex to `%s`, assets %u, blocks %u\n", path, (uint32_t)*content_index->m_ChunkCount, (uint32_t)*content_index->m_BlockCount)
    size_t index_data_size = GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);

    if (!EnsureParentPathExists(storage_api, path))
    {
        return 0;
    }
    StorageAPI_HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    if (!storage_api->Write(storage_api, file_handle, 0, index_data_size, &content_index[1]))
    {
        return 0;
    }
    storage_api->CloseWrite(storage_api, file_handle);

    return 1;
}

struct ContentIndex* ReadContentIndex(struct StorageAPI* storage_api, const char* path)
{
    LONGTAIL_LOG("ReadContentIndex from `%s`\n", path)
    StorageAPI_HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    size_t content_index_data_size = storage_api->GetSize(storage_api, file_handle);
    struct ContentIndex* content_index = (struct ContentIndex*)malloc(sizeof(struct ContentIndex) + content_index_data_size);
    if (!content_index)
    {
        storage_api->CloseRead(storage_api, file_handle);
        return 0;
    }
    if (!storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]))
    {
        storage_api->CloseRead(storage_api, file_handle);
        return 0;
    }
    InitContentIndex(content_index);
    storage_api->CloseRead(storage_api, file_handle);
    return content_index;
}

struct PathLookup
{
    struct HashToIndexItem* m_HashToNameOffset;
    const char* m_NameData;
};

struct PathLookup* CreateContentHashToPathLookup(const struct VersionIndex* version_index, uint64_t* out_unique_asset_indexes)
{
    uint32_t asset_count = (uint32_t)(*version_index->m_AssetCount);
    struct PathLookup* path_lookup = (struct PathLookup*)malloc(sizeof(struct PathLookup));
    path_lookup->m_HashToNameOffset = 0;
    path_lookup->m_NameData = version_index->m_NameData;

    // Only pick up unique assets
    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash content_hash = version_index->m_ContentHashes[i];
        ptrdiff_t lookup_index = hmgeti(path_lookup->m_HashToNameOffset, content_hash);
        if (lookup_index == -1)
        {
            hmput(path_lookup->m_HashToNameOffset, content_hash, version_index->m_NameOffsets[i]);
            if (out_unique_asset_indexes)
            {
                out_unique_asset_indexes[unique_asset_count] = i;
            }
            ++unique_asset_count;
        }
    }
    return path_lookup;
}

const char* GetPathFromAssetContentHash(const struct PathLookup* path_lookup, TLongtail_Hash asset_content_hash)
{
    struct HashToIndexItem* lookup = (struct HashToIndexItem*)path_lookup->m_HashToNameOffset;
    ptrdiff_t lookup_index = hmgeti(lookup, asset_content_hash);
    if (lookup_index == -1)
    {
        return 0;
    }
    uint32_t offset = lookup[lookup_index].value;
    return &path_lookup->m_NameData[offset];
}

void FreePathLookup(struct PathLookup* path_lookup)
{
    if (!path_lookup)
    {
        return;
    }
    hmfree(path_lookup->m_HashToNameOffset);
    free(path_lookup);
}

struct AssetPart
{
    const char* m_Path;
    uint64_t m_AssetIndex;
    uint64_t m_Start;
};

struct ChunkHashToAssetPart
{
    TLongtail_Hash key;
    struct AssetPart value;
};

struct ChunkHashToAssetPart* CreateAssetPartLookup(
    struct VersionIndex* version_index)
{
    // We need a map from chunk hash -> asset_index + asset_start
    uint64_t chunk_index = 0;
    struct ChunkHashToAssetPart* asset_part_lookup = 0;
    for (uint64_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint64_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint64_t asset_chunk_offset = 0;
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
			uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
			TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
			if (hmgeti(asset_part_lookup, chunk_hash) == -1)
			{
				struct AssetPart asset_part = { path, asset_index, asset_chunk_offset };
				hmput(asset_part_lookup, chunk_hash, asset_part);
			}

            ++chunk_index;
            asset_chunk_offset += chunk_size;
        }
    }
	ptrdiff_t unique_chunks = hmlen(asset_part_lookup);
    return asset_part_lookup;
}

void FreeAssetPartLookup(struct ChunkHashToAssetPart* asset_part_lookup)
{
    hmfree(asset_part_lookup);
}

struct WriteBlockJob
{
    struct StorageAPI* m_SourceStorageAPI;
    struct StorageAPI* m_TargetStorageAPI;
    struct CompressionAPI* m_CompressionAPI;
    const char* m_ContentFolder;
    const char* m_AssetsFolder;
    const struct ContentIndex* m_ContentIndex;
    struct ChunkHashToAssetPart* m_AssetPartLookup;
    uint64_t m_FirstChunkIndex;
    uint32_t m_ChunkCount;
    uint32_t m_Success;
};

struct WriteBlockJob* CreateWriteContentBlockJob()
{
    size_t job_size = sizeof(struct WriteBlockJob);
    struct WriteBlockJob* job = (struct WriteBlockJob*)malloc(job_size);
    return job;
}

char* GetBlockName(TLongtail_Hash block_hash)
{
    char* name = (char*)malloc(64);
    sprintf(name, "0x%" PRIx64, block_hash);
    return name;
}

// Broken, we need to use asset part lookup
void WriteContentBlockJob(void* context)
{
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

    char* block_name = GetBlockName(block_hash);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    char* block_path = target_storage_api->ConcatPath(target_storage_api, content_folder, file_name);

    char tmp_block_name[64];
    sprintf(tmp_block_name, "%s.tmp", block_name);
    char* tmp_block_path = (char*)target_storage_api->ConcatPath(target_storage_api, content_folder, tmp_block_name);

    uint32_t block_data_size = 0;
    for (uint32_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        block_data_size += chunk_size;
    }

    char* write_buffer = (char*)malloc(block_data_size);
    char* write_ptr = write_buffer;

    for (uint32_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];	// TODO: We get mismatch on chunk size vs size on disk
        intptr_t asset_part_index = hmgeti(job->m_AssetPartLookup, chunk_hash);
        if (asset_part_index == -1)
        {
            LONGTAIL_LOG("Failed to get path for asset content 0x%" PRIx64 "\n", chunk_hash)
            free(write_buffer);
            free(block_name);
            return;
        }
        const char* asset_path = job->m_AssetPartLookup[asset_part_index].value.m_Path;
        uint64_t asset_offset = job->m_AssetPartLookup[asset_part_index].value.m_Start;

        if (IsDirPath(asset_path))
        {
            LONGTAIL_LOG("Directory should not have any chunks `%s`\n", asset_path)
            free(write_buffer);
            free(block_name);
            return;
        }

        char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
        uint64_t asset_content_offset = job->m_AssetPartLookup[asset_part_index].value.m_Start;
        StorageAPI_HOpenFile file_handle = source_storage_api->OpenReadFile(source_storage_api, full_path);
        if (!file_handle || (source_storage_api->GetSize(source_storage_api, file_handle) < (asset_offset + chunk_size)))
        {
            LONGTAIL_LOG("Missing or mismatching asset content `%s`\n", asset_path)
            free(write_buffer);
            free(block_name);
            source_storage_api->CloseRead(source_storage_api, file_handle);
            return;
        }
        source_storage_api->Read(source_storage_api, file_handle, asset_offset, chunk_size, write_ptr);
        write_ptr += chunk_size;

        source_storage_api->CloseRead(source_storage_api, file_handle);
        free((char*)full_path);
        full_path = 0;
    }

    CompressionAPI_HCompressionContext compression_context = compression_api->CreateCompressionContext(compression_api, compression_api->GetDefaultSettings(compression_api));
    const size_t max_dst_size = compression_api->GetMaxCompressedSize(compression_api, compression_context, block_data_size);
    char* compressed_buffer = (char*)malloc((sizeof(uint32_t) * 2) + max_dst_size);
    ((uint32_t*)compressed_buffer)[0] = (uint32_t)block_data_size;

    size_t compressed_size = compression_api->Compress(compression_api, compression_context, (const char*)write_buffer, &((char*)compressed_buffer)[sizeof(int32_t) * 2], block_data_size, max_dst_size);
    compression_api->DeleteCompressionContext(compression_api, compression_context);
    free(write_buffer);
    if (compressed_size > 0)
    {
        ((uint32_t*)compressed_buffer)[1] = (uint32_t)compressed_size;

        if (!EnsureParentPathExists(target_storage_api, tmp_block_path))
        {
            LONGTAIL_LOG("Failed to create parent path for `%s`\n", tmp_block_path)
            free(compressed_buffer);
            return;
        }
        StorageAPI_HOpenFile block_file_handle = target_storage_api->OpenWriteFile(target_storage_api, tmp_block_path);
        if (!block_file_handle)
        {
            LONGTAIL_LOG("Failed to create block file `%s`\n", tmp_block_path)
            free(compressed_buffer);
            return;
        }
        int write_ok = target_storage_api->Write(target_storage_api, block_file_handle, 0, (sizeof(uint32_t) * 2) + compressed_size, compressed_buffer);
        free(compressed_buffer);
        uint64_t write_offset = (sizeof(uint32_t) * 2) + compressed_size;

        uint32_t aligned_size = (((write_offset + 15) / 16) * 16);
        uint32_t padding = aligned_size - write_offset;
        if (padding)
        {
            target_storage_api->Write(target_storage_api, block_file_handle, write_offset, padding, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
            write_offset = aligned_size;
        }

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(TLongtail_Hash) * chunk_count, &content_index->m_ChunkHashes[first_chunk_index]);
        write_offset += sizeof(TLongtail_Hash) * chunk_count;

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(uint32_t) * chunk_count, &content_index->m_ChunkLengths[first_chunk_index]);
        write_offset += sizeof(uint32_t) * chunk_count;

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(uint32_t), &chunk_count);
        write_offset += sizeof(uint32_t);
        target_storage_api->CloseWrite(target_storage_api, block_file_handle);
        write_ok = write_ok & target_storage_api->RenameFile(target_storage_api, tmp_block_path, block_path);
        job->m_Success = write_ok;
    }
    free(block_name);
    block_name = 0;

    free((char*)block_path);
    block_path = 0;

    free((char*)tmp_block_path);
    tmp_block_path = 0;
}

int WriteContent(
    struct StorageAPI* source_storage_api,
    struct StorageAPI* target_storage_api,
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
    struct ContentIndex* content_index,
    struct ChunkHashToAssetPart* asset_part_lookup,
    const char* assets_folder,
    const char* content_folder)
{
    LONGTAIL_LOG("WriteContent from `%s` to `%s`, assets %u, blocks %u\n", assets_folder, content_folder, (uint32_t)*content_index->m_ChunkCount, (uint32_t)*content_index->m_BlockCount)
    uint64_t block_count = *content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 1;
    }

    if (job_api)
    {
        if (!job_api->ReserveJobs(job_api, block_count))
        {
            return 0;
        }
    }

    struct WriteBlockJob** write_block_jobs = (struct WriteBlockJob**)malloc(sizeof(struct WriteBlockJob*) * block_count);
    uint32_t block_start_chunk_index = 0;
    for (uint32_t block_index = 0; block_index < block_count; ++block_index)
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
        free(block_name);
        char* block_path = target_storage_api->ConcatPath(target_storage_api, content_folder, file_name);
        if (target_storage_api->IsFile(target_storage_api, block_path))
        {
            write_block_jobs[block_index] = 0;
            free((char*)block_path);
            block_path = 0;
            block_start_chunk_index += chunk_count;
            continue;
        }

        struct WriteBlockJob* job = CreateWriteContentBlockJob();
        write_block_jobs[block_index] = job;
        job->m_SourceStorageAPI = source_storage_api;
        job->m_TargetStorageAPI = target_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentFolder = content_folder;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = content_index;
        job->m_AssetPartLookup = asset_part_lookup;
        job->m_FirstChunkIndex = block_start_chunk_index;
        job->m_ChunkCount = chunk_count;
        job->m_Success = 0;

        if (job_api == 0)
        {
            WriteContentBlockJob(job);
        }
        else
        {
            JobAPI_JobFunc func[1] = { WriteContentBlockJob };
            void* ctx[1] = { job };

            job_api->SubmitJobs(job_api, 1, func, ctx);
        }

        block_start_chunk_index += chunk_count;
    }

    if (job_api)
    {
        job_api->WaitForAllJobs(job_api);
    }

    int success = 1;
    while (block_count--)
    {
        struct WriteBlockJob* job = write_block_jobs[block_count];
        if (!job)
        {
            continue;
        }
        if (!job->m_Success)
        {
            success = 0;
        }
        free(job);
    }

    free(write_block_jobs);

    return success;
}

struct ReconstructJob
{
    struct StorageAPI* m_StorageAPI;
    struct CompressionAPI* m_CompressionAPI;
    char* m_BlockPath;
    char** m_AssetPaths;
    uint32_t m_ChunkCount;
    uint32_t* m_ChunkBlockOffsets;
    uint32_t* m_ChunkLengths;
    uint32_t m_Success;
};

struct ReconstructJob* CreateReconstructJob(uint32_t asset_count)
{
    size_t job_size = sizeof(struct ReconstructJob) +
        sizeof(char*) * asset_count +
        sizeof(uint32_t) * asset_count +
        sizeof(uint32_t) * asset_count;

    struct ReconstructJob* job = (struct ReconstructJob*)malloc(job_size);
    char* p = (char*)&job[1];
    job->m_AssetPaths = (char**)p;
    p += sizeof(char*) * asset_count;

    job->m_ChunkBlockOffsets = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    job->m_ChunkLengths = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    return job;
}

static void ReconstructFromBlock(void* context)
{
    struct ReconstructJob* job = (struct ReconstructJob*)context;
    struct StorageAPI* storage_api = job->m_StorageAPI;

    struct CompressionAPI* compression_api = job->m_CompressionAPI;
    StorageAPI_HOpenFile block_file_handle = storage_api->OpenReadFile(storage_api, job->m_BlockPath);
    if (!block_file_handle)
    {
        LONGTAIL_LOG("Failed to open block file `%s`\n", job->m_BlockPath)
        return;
    }

    uint64_t compressed_block_size = storage_api->GetSize(storage_api, block_file_handle);
    char* compressed_block_content = (char*)malloc(compressed_block_size);
    if (!storage_api->Read(storage_api, block_file_handle, 0, compressed_block_size, compressed_block_content))
    {
        LONGTAIL_LOG("Failed to read block file `%s`\n", job->m_BlockPath)
        storage_api->CloseRead(storage_api, block_file_handle);
        free(compressed_block_content);
        return;
    }
    uint32_t uncompressed_size = ((uint32_t*)compressed_block_content)[0];
    uint32_t compressed_size = ((uint32_t*)compressed_block_content)[1];
    char* decompressed_buffer = (char*)malloc(uncompressed_size);
    CompressionAPI_HDecompressionContext compression_context = compression_api->CreateDecompressionContext(compression_api);
    size_t result = compression_api->Decompress(compression_api, compression_context, &compressed_block_content[sizeof(uint32_t) * 2], decompressed_buffer, compressed_size, uncompressed_size);
    compression_api->DeleteDecompressionContext(compression_api, compression_context);
    free(compressed_block_content);
    storage_api->CloseRead(storage_api, block_file_handle);
    block_file_handle = 0;
    if (result < uncompressed_size)
    {
        LONGTAIL_LOG("Block file is malformed (compression header) `%s`\n", job->m_BlockPath)
        free(decompressed_buffer);
        decompressed_buffer = 0;
        return;
    }

    for (uint32_t asset_index = 0; asset_index < job->m_ChunkCount; ++asset_index)
    {
        char* asset_path = job->m_AssetPaths[asset_index];
        if (!EnsureParentPathExists(storage_api, asset_path))
        {
            LONGTAIL_LOG("Failed to create parent path for `%s`\n", asset_path)
            free(decompressed_buffer);
            decompressed_buffer = 0;
            return;
        }

        if (IsDirPath(asset_path))
        {
            if (!SafeCreateDir(storage_api, asset_path))
            {
                LONGTAIL_LOG("Failed to create asset folder `%s`\n", asset_path)
                free(decompressed_buffer);
                decompressed_buffer = 0;
                return;
            }
        }
        else
        {
            StorageAPI_HOpenFile asset_file_handle = storage_api->OpenWriteFile(storage_api, asset_path);
            if(!asset_file_handle)
            {
                LONGTAIL_LOG("Failed to create asset file `%s`\n", asset_path)
                free(decompressed_buffer);
                decompressed_buffer = 0;
                return;
            }
            uint32_t chunk_length = job->m_ChunkLengths[asset_index];
            uint64_t read_offset = job->m_ChunkBlockOffsets[asset_index];
            uint64_t write_offset = 0;
            int write_ok = storage_api->Write(storage_api, asset_file_handle, write_offset, chunk_length, &decompressed_buffer[read_offset]);
            storage_api->CloseWrite(storage_api, asset_file_handle);
            asset_file_handle = 0;
            if (!write_ok)
            {
                LONGTAIL_LOG("Failed to write asset file `%s`\n", asset_path)
                free(decompressed_buffer);
                decompressed_buffer = 0;
                return;
            }
        }
    }
    storage_api->CloseRead(storage_api, block_file_handle);
    block_file_handle = 0;
    free(decompressed_buffer);
    decompressed_buffer = 0;

    job->m_Success = 1;
}

struct ReconstructOrder
{
    const struct ContentIndex* content_index;
    const TLongtail_Hash* asset_hashes;
    const uint64_t* version_index_to_content_index;
};

int ReconstructOrderCompare(void* context, const void* a_ptr, const void* b_ptr)
{   
    struct ReconstructOrder* c = (struct ReconstructOrder*)context;
    uint64_t a = *(uint64_t*)a_ptr;
    uint64_t b = *(uint64_t*)b_ptr;
    TLongtail_Hash a_hash = c->asset_hashes[a];
    TLongtail_Hash b_hash = c->asset_hashes[b];
    uint32_t a_asset_index_in_content_index = (uint32_t)c->version_index_to_content_index[a];
    uint32_t b_asset_index_in_content_index = (uint32_t)c->version_index_to_content_index[b];

    uint64_t a_block_index = c->content_index->m_ChunkBlockIndexes[a_asset_index_in_content_index];
    uint64_t b_block_index = c->content_index->m_ChunkBlockIndexes[b_asset_index_in_content_index];
    if (a_block_index > b_block_index)
    {
        return 1;
    }
    if (a_block_index > b_block_index)
    {
        return -1;
    }

    uint32_t a_offset_in_block = c->content_index->m_ChunkBlockOffsets[a_asset_index_in_content_index];
    uint32_t b_offset_in_block = c->content_index->m_ChunkBlockOffsets[b_asset_index_in_content_index];
    if (a_offset_in_block > b_offset_in_block)
    {
        return 1;
    }
    if (b_offset_in_block > a_offset_in_block)
    {
        return -1;
    }
    return 0;
}

// TODO: This is confused with regards to chunk/asset index vs chunk/asset hash!
int ReconstructVersion(
    struct StorageAPI* storage_api,
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const char* content_path,
    const char* version_path)
{
    LONGTAIL_LOG("ReconstructVersion from `%s` to `%s`, assets %u\n", content_path, version_path, (uint32_t)*version_index->m_AssetCount)
    struct HashToIndexItem* content_hash_to_content_asset_index = 0;
    for (uint64_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
        hmput(content_hash_to_content_asset_index, chunk_hash, i);
    }

    uint64_t asset_count = *version_index->m_AssetCount;
    uint64_t* asset_order = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
    uint64_t* version_index_to_content_index = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
    uint64_t asset_found_count = 0;
    for (uint64_t i = 0; i < asset_count; ++i)
    {
        asset_order[i] = i;
        ptrdiff_t lookup_index = hmgeti(content_hash_to_content_asset_index, version_index->m_ContentHashes[i]);
        if (lookup_index == -1)
        {
            LONGTAIL_LOG("Asset 0x%" PRIx64 " for asset `%s` was not find in content index\n", version_index->m_ContentHashes[i], &version_index->m_NameData[version_index->m_NameOffsets[i]])
            continue;
        }
        version_index_to_content_index[i] = content_hash_to_content_asset_index[lookup_index].value;
        ++asset_found_count;
    }

    hmfree(content_hash_to_content_asset_index);
    content_hash_to_content_asset_index = 0;

    if (asset_found_count != asset_count)
    {
        return 0;
    }

    struct ReconstructOrder reconstruct_order = {content_index, version_index->m_ContentHashes, version_index_to_content_index};
    qsort_s(&asset_order[0], asset_count, sizeof(uint64_t), ReconstructOrderCompare, &reconstruct_order);

    if (job_api)
    {
        if (!job_api->ReserveJobs(job_api, asset_count))
        {
            free(asset_order);
            return 0;
        }
    }
    struct ReconstructJob** reconstruct_jobs = (struct ReconstructJob**)malloc(sizeof(struct ReconstructJob*) * asset_count);
    uint32_t job_count = 0;
    uint64_t i = 0;
    while (i < asset_count)
    {
        uint64_t asset_index = asset_order[i];

        uint32_t asset_index_in_content_index = version_index_to_content_index[asset_index];
        uint64_t block_index = content_index->m_ChunkBlockIndexes[asset_index_in_content_index];

        uint32_t asset_count_from_block = 1;
        while(((i + asset_count_from_block) < asset_count))
        {
            uint32_t next_asset_index = asset_order[i + asset_count_from_block];
            uint64_t next_asset_index_in_content_index = version_index_to_content_index[next_asset_index];
            uint64_t next_block_index = content_index->m_ChunkBlockIndexes[next_asset_index_in_content_index];
            if (block_index != next_block_index)
            {
                break;
            }
            ++asset_count_from_block;
        }

        struct ReconstructJob* job = CreateReconstructJob(asset_count_from_block);
        reconstruct_jobs[job_count++] = job;
        job->m_StorageAPI = storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_Success = 0;

        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        char* block_name = GetBlockName(block_hash);
        char block_file_name[64];
        sprintf(block_file_name, "%s.lrb", block_name);

        job->m_BlockPath = storage_api->ConcatPath(storage_api, content_path, block_file_name);
        job->m_ChunkCount = asset_count_from_block;

        for (uint32_t j = 0; j < asset_count_from_block; ++j)
        {
            uint64_t asset_index = asset_order[i + j];
            const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
            job->m_AssetPaths[j] = storage_api->ConcatPath(storage_api, version_path, asset_path);
            uint64_t asset_index_in_content_index = version_index_to_content_index[asset_index];
            job->m_ChunkBlockOffsets[j] = content_index->m_ChunkBlockOffsets[asset_index_in_content_index];
            job->m_ChunkLengths[j] = content_index->m_ChunkLengths[asset_index_in_content_index];
        }

        if (job_api == 0)
        {
            ReconstructFromBlock(job);
        }
        else
        {
            JobAPI_JobFunc func[1] = { ReconstructFromBlock };
            void* ctx[1] = { job };
            job_api->SubmitJobs(job_api, 1, func, ctx);
        }

        i += asset_count_from_block;
    }

    if (job_api)
    {
        job_api->WaitForAllJobs(job_api);
    }

    int success = 1;
    while (job_count--)
    {
        struct ReconstructJob* job = reconstruct_jobs[job_count];
        if (!job->m_Success)
        {
            success = 0;
            LONGTAIL_LOG("Failed reconstructing `%s`\n", job->m_AssetPaths[i]);
        }
        free(job->m_BlockPath);
        for (uint32_t i = 0; i < job->m_ChunkCount; ++i)
        {
            free(job->m_AssetPaths[i]);
        }
        free(reconstruct_jobs[job_count]);
    }

    free(reconstruct_jobs);

    free(version_index_to_content_index);
    free(asset_order);
    return success;
}

struct BlockIndex* ReadBlock(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    const char* full_block_path)
{
    StorageAPI_HOpenFile f = storage_api->OpenReadFile(storage_api, full_block_path);
    if (!f)
    {
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
    struct BlockIndex* block_index = InitBlockIndex(malloc(GetBlockIndexSize(chunk_count)), chunk_count);
    size_t block_index_data_size = GetBlockIndexDataSize(chunk_count);

    int ok = storage_api->Read(storage_api, f, s - block_index_data_size, block_index_data_size, &block_index[1]);
    storage_api->CloseRead(storage_api, f);
    if (!ok)
    {
        free(block_index);
        return 0;
    }
    HashAPI_HContext hash_context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, hash_context, (uint32_t)(GetBlockIndexDataSize(chunk_count)), (void*)&block_index[1]);
    TLongtail_Hash block_hash = hash_api->EndContext(hash_api, hash_context);
    block_index->m_BlockHash = block_hash;

    return block_index;
}

struct ReadContentContext {
    struct StorageAPI* m_StorageAPI;
    uint32_t m_ReservedPathCount;
    uint32_t m_ReservedPathSize;
    uint32_t m_RootPathLength;
    struct Paths* m_Paths;
    uint64_t m_ChunkCount;
};

void ReadContentAddPath(void* context, const char* root_path, const char* file_name)
{
    struct ReadContentContext* paths_context = (struct ReadContentContext*)context;
    struct StorageAPI* storage_api = paths_context->m_StorageAPI;

    char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
    if (storage_api->IsDir(storage_api, full_path))
    {
        return;
    }

    struct Paths* paths = paths_context->m_Paths;
    const uint32_t root_path_length = paths_context->m_RootPathLength;
    const char* s = &full_path[root_path_length];
    if (*s == '/')
    {
        ++s;
    }

    StorageAPI_HOpenFile f = storage_api->OpenReadFile(storage_api, full_path);
    if (!f)
    {
        free(full_path);
        return;
    }
    uint64_t block_size = storage_api->GetSize(storage_api, f);
    if (block_size < (sizeof(uint32_t)))
    {
        free(full_path);
        storage_api->CloseRead(storage_api, f);
        return;
    }
    uint32_t chunk_count = 0;
    int ok = storage_api->Read(storage_api, f, block_size - sizeof(uint32_t), sizeof(uint32_t), &chunk_count);

    if (!ok)
    {
        free(full_path);
        storage_api->CloseRead(storage_api, f);
        return;
    }

    uint32_t asset_index_size = sizeof(uint32_t) + (chunk_count) * (sizeof(TLongtail_Hash) + sizeof(uint32_t));
    ok = block_size >= asset_index_size;

    if (!ok)
    {
        free(full_path);
        return;
    }

    paths_context->m_ChunkCount += chunk_count;
    paths_context->m_Paths = AppendPath(paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);

    free(full_path);
    full_path = 0;
};

struct ContentIndex* ReadContent(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    const char* content_path)
{
    LONGTAIL_LOG("ReadContent from `%s`\n", content_path)

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;


    struct Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    struct ReadContentContext context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(content_path)), paths, 0};
    if(!RecurseTree(storage_api, content_path, ReadContentAddPath, &context))
    {
        free(context.m_Paths);
        return 0;
    }
    paths = context.m_Paths;

    uint32_t chunk_count = context.m_ChunkCount;
    uint64_t block_count = *paths->m_PathCount;
    size_t content_index_data_size = GetContentIndexDataSize(block_count, chunk_count);
    struct ContentIndex* content_index = (struct ContentIndex*)malloc(sizeof(struct ContentIndex) + content_index_data_size);
    content_index->m_BlockCount = (uint64_t*) & ((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*) & ((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    InitContentIndex(content_index);

    uint64_t asset_offset = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        const char* block_path = &paths->m_Data[paths->m_Offsets[b]];
        char* full_block_path = storage_api->ConcatPath(storage_api, content_path, block_path);

        const char* last_delimiter = strrchr(full_block_path, '/');
        const char* file_name = last_delimiter ? last_delimiter + 1 : full_block_path;

        struct BlockIndex* block_index = ReadBlock(
            storage_api,
            hash_api,
            full_block_path);

        free(full_block_path);
        if (block_index == 0)
        {
            free(content_index);
            return 0;
        }

        char* block_name = GetBlockName(block_index->m_BlockHash);
        char verify_file_name[64];
        sprintf(verify_file_name, "%s.lrb", block_name);
        free(block_name);

        if (strcmp(verify_file_name, file_name))
        {
            free(block_index);
            free(content_index);
            return 0;
        }

        uint32_t block_chunk_count = *block_index->m_ChunkCount;
        uint32_t block_offset = 0;
        for (uint32_t a = 0; a < block_chunk_count; ++a)
        {
            content_index->m_ChunkHashes[asset_offset + a] = block_index->m_ChunkHashes[a];
            content_index->m_ChunkLengths[asset_offset + a] = block_index->m_ChunkSizes[a];
            content_index->m_ChunkBlockOffsets[asset_offset + a] = block_offset;
            content_index->m_ChunkBlockIndexes[asset_offset + a] = b;
            block_offset += content_index->m_ChunkLengths[asset_offset + a];
        }
        content_index->m_BlockHashes[b] = block_index->m_BlockHash;
        asset_offset += block_chunk_count;

        free(block_index);
    }
    free(paths);
    return content_index;
}

int CompareHash(const void* a_ptr, const void* b_ptr) 
{
    TLongtail_Hash a = *((TLongtail_Hash*)a_ptr);
    TLongtail_Hash b = *((TLongtail_Hash*)b_ptr);
    if (a > b) return  1;
    if (a < b) return -1;
    return 0;
}

uint32_t MakeUnique(TLongtail_Hash* hashes, uint32_t count)
{
    uint32_t w = 0;
    uint32_t r = 0;
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

void DiffHashes(const TLongtail_Hash* reference_hashes, uint32_t reference_hash_count, const TLongtail_Hash* new_hashes, uint32_t new_hash_count, uint32_t* added_hash_count, TLongtail_Hash* added_hashes, uint32_t* removed_hash_count, TLongtail_Hash* removed_hashes)
{
    TLongtail_Hash* refs = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * reference_hash_count);
    TLongtail_Hash* news = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * new_hash_count);
    memmove(refs, reference_hashes, sizeof(TLongtail_Hash) * reference_hash_count);
    memmove(news, new_hashes, sizeof(TLongtail_Hash) * new_hash_count);

    qsort(&refs[0], reference_hash_count, sizeof(TLongtail_Hash), CompareHash);
    reference_hash_count = MakeUnique(&refs[0], reference_hash_count);

    qsort(&news[0], new_hash_count, sizeof(TLongtail_Hash), CompareHash);
    new_hash_count = MakeUnique(&news[0], new_hash_count);

    uint32_t removed = 0;
    uint32_t added = 0;
    uint32_t ni = 0;
    uint32_t ri = 0;
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

    free(news);
    news = 0;
    free(refs);
    refs = 0;
}

struct ContentIndex* CreateMissingContent(
    struct HashAPI* hash_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block)
{
    LONGTAIL_LOG("CreateMissingContent\n")
    uint64_t chunk_count = *version->m_ChunkCount;
    TLongtail_Hash* removed_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * chunk_count);
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * chunk_count);

    uint32_t added_hash_count = 0;
    uint32_t removed_hash_count = 0;
    DiffHashes(content_index->m_ChunkHashes, *content_index->m_ChunkCount, version->m_ChunkHashes, chunk_count, &added_hash_count, added_hashes, &removed_hash_count, removed_hashes);

    if (added_hash_count == 0)
    {
        struct ContentIndex* diff_content_index = CreateContentIndex(
            hash_api,
            0,
            0,
            0,
            max_block_size,
            max_chunks_per_block);
        return diff_content_index;
    }

    uint32_t* diff_chunk_sizes = (uint32_t*)malloc(sizeof(uint32_t) * added_hash_count);

    struct HashToIndexItem* chunk_index_lookup = 0;
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        hmput(chunk_index_lookup, version->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        ptrdiff_t lookup_index = hmgeti(chunk_index_lookup, added_hashes[j]);
        if (lookup_index == -1)
        {
            hmfree(chunk_index_lookup);
            free(removed_hashes);
            free(added_hashes);
            return 0;
        }

        uint64_t chunk_index = chunk_index_lookup[lookup_index].value;
        diff_chunk_sizes[j] = version->m_ChunkSizes[chunk_index];
    }
    hmfree(chunk_index_lookup);
    chunk_index_lookup = 0;

    struct ContentIndex* diff_content_index = CreateContentIndex(
        hash_api,
        added_hash_count,
        added_hashes,
        diff_chunk_sizes,
        max_block_size,
        max_chunks_per_block);

    free(removed_hashes);
    free(added_hashes);

    return diff_content_index;
}

struct ContentIndex* MergeContentIndex(
    struct ContentIndex* local_content_index,
    struct ContentIndex* remote_content_index)
{
    uint64_t local_block_count = *local_content_index->m_BlockCount;
    uint64_t remote_block_count = *remote_content_index->m_BlockCount;
    uint64_t local_chunk_count = *local_content_index->m_ChunkCount;
    uint64_t remote_chunk_count = *remote_content_index->m_ChunkCount;
    uint64_t block_count = local_block_count + remote_block_count;
    uint64_t asset_count = local_chunk_count + remote_chunk_count;
    size_t content_index_size = GetContentIndexSize(block_count, asset_count);
    struct ContentIndex* content_index = (struct ContentIndex*)malloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex)];
    content_index->m_ChunkCount = (uint64_t*)&((char*)content_index)[sizeof(struct ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = asset_count;
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

#if 0
struct ChunkedAssets* ChunkAssets(
    struct StorageAPI* read_storage_api,
    struct StorageAPI* write_storage_api,
    struct HashAPI* hash_api,
    struct VersionIndex* version_index,
    const char* assets_path,
    const char* chunks_path)
{
    struct HashToIndexItem* chunk_lookup_table = 0;

    uint32_t* store_chunk_indexes = 0;
    arrsetcap(store_chunk_indexes, 4096);
    uint32_t* store_chunk_sizes = 0;
    arrsetcap(store_chunk_sizes, 4096);
    TLongtail_Hash* store_chunk_hashes = 0;
    arrsetcap(store_chunk_hashes, 4096);
    uint8_t* current_chunk_data = 0;

    uint64_t asset_count = *version_index->m_AssetCount;
    uint32_t* chunk_start_indexes = 0;
    arrsetcap(chunk_start_indexes, asset_count);
    uint32_t* chunk_counts = 0;
    arrsetcap(chunk_counts, asset_count);
    TLongtail_Hash* chunk_hashes = 0;
    arrsetcap(chunk_hashes, asset_count);

    uint32_t chunk_start_index = 0;
    arrsetcap(current_chunk_data, 65536);
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        arrput(chunk_start_indexes, chunk_start_index);
        uint32_t asset_size = version_index->m_AssetSizes[asset_index];
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        char* full_path = read_storage_api->ConcatPath(read_storage_api, assets_path, asset_path);
        StorageAPI_HOpenFile f = read_storage_api->OpenReadFile(read_storage_api, full_path);
        if (!f)
        {
            arrfree(chunk_hashes);
            arrfree(chunk_counts);
            arrfree(chunk_start_indexes);
            hmfree(chunk_lookup_table);
            return 0;
        }

        uint32_t asset_chunk_count = 0;

        TLongtail_Hash chunk_hash = 0;
        if (asset_size == 0)
        {
            arrput(chunk_hashes, 0);
            ++asset_chunk_count;
        }
        else
        {
            uint8_t chunk_data[65536];
            uint32_t size_left = asset_size;
            while (size_left > 0)
            {
                uint32_t chunk_size = size_left < 65536 ? size_left : 65536;
                read_storage_api->Read(read_storage_api, f, asset_size - size_left, chunk_size, chunk_data);
                TLongtail_Hash chunk_hash = 0;

                ptrdiff_t lookup_index = hmgeti(chunk_lookup_table, chunk_hash);
                if (lookup_index == -1)
                {
                    if (arrcap(current_chunk_data) < (chunk_size + arrlen(current_chunk_data)))
                    {
                        arrsetcap(current_chunk_data, arrcap(current_chunk_data) + 65536);
                    }
                    memcpy(&current_chunk_data[arrlen(current_chunk_data)], chunk_data, chunk_size);
                    arrsetlen(current_chunk_data, arrlen(current_chunk_data) + chunk_size);

                    if (arrlen(current_chunk_data) > 262144 || arrlen(store_chunk_hashes) == 4096)
                    {
                        size_t block_mem_size = GetBlockIndexSize(arrlen(store_chunk_hashes));
                        struct BlockIndex* block_index = CreateBlockIndex(
                            malloc(block_mem_size),
                            hash_api,
                            arrlen(store_chunk_hashes),
                            store_chunk_indexes,
                            store_chunk_hashes,
                            store_chunk_sizes);

                        char name[64];
                        sprintf(name, "0x%" PRIx64 ".ldb", block_index->m_BlockHash);
                        char* block_path = write_storage_api->ConcatPath(write_storage_api, chunks_path, name);
                        write_storage_api->OpenWriteFile(write_storage_api, block_path);
                        StorageAPI_HOpenFile w = write_storage_api->OpenWriteFile(write_storage_api, block_path);
                        uint64_t write_offset = 0;
                        write_storage_api->Write(write_storage_api, w, write_offset, arrlen(store_chunk_hashes), current_chunk_data);
                        write_offset += arrlen(store_chunk_hashes);

                        write_storage_api->Write(write_storage_api, w, write_offset, sizeof(TLongtail_Hash) * *block_index->m_ChunkCount, block_index->m_ContentHashes);
                        write_offset += sizeof(TLongtail_Hash) * *block_index->m_ChunkCount;
                        write_storage_api->Write(write_storage_api, w, write_offset, sizeof(uint32_t) * *block_index->m_ChunkCount, block_index->m_AssetSizes);
                        write_offset += sizeof(uint32_t) * *block_index->m_ChunkCount;
                        write_storage_api->Write(write_storage_api, w, write_offset, sizeof(uint32_t), block_index->m_ChunkCount);
                        write_offset += sizeof(uint32_t);
                        write_storage_api->CloseWrite(write_storage_api, w);

                        free(block_index);
                    }
                    hmput(chunk_lookup_table, chunk_hash, 1);
                }
                ++asset_chunk_count;
                size_left -= chunk_size;
                arrput(chunk_hashes, chunk_hash);

                arrput(store_chunk_indexes, (uint32_t)arrlen(store_chunk_hashes));
                arrput(store_chunk_sizes, chunk_size);
                arrput(store_chunk_hashes, chunk_hash);

                arrsetlen(store_chunk_indexes, 0);
                arrsetlen(store_chunk_sizes, 0);
                arrsetlen(store_chunk_hashes, 0);
            }
        }

        chunk_start_index += asset_chunk_count;
        arrput(chunk_counts, asset_chunk_count);

        read_storage_api->CloseRead(read_storage_api, f);
        f = 0;
    }

    size_t chunked_assets_size =
        sizeof(struct ChunkedAssets) +
        sizeof(TLongtail_Hash) * asset_count +
        sizeof(uint32_t) * asset_count +
        sizeof(uint32_t) * asset_count +
        sizeof(TLongtail_Hash) * chunk_start_index;
    struct ChunkedAssets* chunked_assets = (struct ChunkedAssets*)malloc(chunked_assets_size);
    char* p = (char*)&chunked_assets[1];
    chunked_assets->m_ContentHashes = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash) * asset_count;
    chunked_assets->m_ChunkStartIndex = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;
    chunked_assets->m_ChunkCount = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;
    chunked_assets->m_ChunkHashes = (TLongtail_Hash*)p;

    memcpy(chunked_assets->m_ContentHashes, version_index->m_ContentHashes, sizeof(TLongtail_Hash) * asset_count);
    memcpy(chunked_assets->m_ChunkStartIndex, chunk_start_indexes, sizeof(uint32_t) * asset_count);
    memcpy(chunked_assets->m_ChunkCount, chunk_counts, sizeof(uint32_t) * asset_count);
    memcpy(chunked_assets->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_start_index);

    arrfree(chunk_hashes);
    arrfree(chunk_counts);
    arrfree(chunk_start_indexes);

    hmfree(chunk_lookup_table);
    chunk_lookup_table = 0;

    return chunked_assets;
};

#endif // 0
