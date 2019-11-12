#pragma once

#include "longtail_array.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>

// Need to kill this if we want to go C99!
// Check out https://github.com/nothings/stb/blob/master/stb_ds.h
#include "../third-party/jc_containers/src/jc_hashtable.h"

struct HashAPI
{
    typedef struct Context* HContext;
    HContext (*BeginContext)(HashAPI* hash_api);
    void (*Hash)(HashAPI* hash_api, HContext context, uint32_t length, void* data);
    uint64_t (*EndContext)(HashAPI* hash_api, HContext context);
};

struct StorageAPI
{
    typedef struct OpenFile* HOpenFile;
    HOpenFile (*OpenReadFile)(StorageAPI* storage_api, const char* path);
    uint64_t (*GetSize)(StorageAPI* storage_api, HOpenFile f);
    int (*Read)(StorageAPI* storage_api, HOpenFile f, uint64_t offset, uint64_t length, void* output);
    void (*CloseRead)(StorageAPI* storage_api, HOpenFile f);

    HOpenFile (*OpenWriteFile)(StorageAPI* storage_api, const char* path);
    int (*Write)(StorageAPI* storage_api, HOpenFile f, uint64_t offset, uint64_t length, const void* input);
    void (*CloseWrite)(StorageAPI* storage_api, HOpenFile f);

    int (*CreateDir)(StorageAPI* storage_api, const char* path);

    int (*RenameFile)(StorageAPI* storage_api, const char* source_path, const char* target_path);
    char* (*ConcatPath)(StorageAPI* storage_api, const char* root_path, const char* sub_path);

    int (*IsDir)(StorageAPI* storage_api, const char* path);
    int (*IsFile)(StorageAPI* storage_api, const char* path);

    typedef struct Iterator* HIterator;
    StorageAPI::HIterator (*StartFind)(StorageAPI* storage_api, const char* path);
    int (*FindNext)(StorageAPI* storage_api, HIterator iterator);
    void (*CloseFind)(StorageAPI* storage_api, HIterator iterator);
    const char* (*GetFileName)(StorageAPI* storage_api, HIterator iterator);
    const char* (*GetDirectoryName)(StorageAPI* storage_api, HIterator iterator);
};

struct CompressionAPI
{
    typedef struct CompressionContext* HCompressionContext;
    typedef struct DecompressionContext* HDecompressionContext;
    typedef struct Settings* HSettings;
    HSettings (*GetDefaultSettings)(CompressionAPI* compression_api);
    HSettings (*GetMaxCompressionSetting)(CompressionAPI* compression_api);

    HCompressionContext (*CreateCompressionContext)(CompressionAPI* compression_api, HSettings settings);
    size_t (*GetMaxCompressedSize)(CompressionAPI* compression_api, HCompressionContext context, size_t size);
    size_t (*Compress)(CompressionAPI* compression_api, HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size);
    void (*DeleteCompressionContext)(CompressionAPI* compression_api, HCompressionContext context);

    HDecompressionContext (*CreateDecompressionContext)(CompressionAPI* compression_api);
    size_t (*Decompress)(CompressionAPI* compression_api, HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size);
    void (*DeleteDecompressionContext)(CompressionAPI* compression_api, HDecompressionContext context);
};

struct JobAPI
{
    typedef void (*TJobFunc)(void* context);

    int (*ReserveJobs)(JobAPI* job_api, uint32_t job_count);
    void (*SubmitJobs)(JobAPI* job_api, uint32_t job_count, TJobFunc job_funcs[], void* job_contexts[]);
    void (*WaitForAllJobs)(JobAPI* job_api);
};

typedef uint64_t TLongtail_Hash;
struct Paths;
struct VersionIndex;
struct ContentIndex;
struct PathLookup;

Paths* GetFilesRecursively(
    StorageAPI* storage_api,
    const char* root_path);

VersionIndex* CreateVersionIndex(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    const char* root_path,
    const Paths* paths);

int WriteVersionIndex(
    StorageAPI* storage_api,
    VersionIndex* version_index,
    const char* path);

VersionIndex* ReadVersionIndex(
    StorageAPI* storage_api,
    const char* path);

typedef TLongtail_Hash (*GetContentTagFunc)(const char* assets_path, const char* path);

ContentIndex* CreateContentIndex(
    HashAPI* hash_api,
    const char* assets_path,
    uint64_t asset_count,
    const TLongtail_Hash* asset_content_hashes,
    const TLongtail_Hash* asset_path_hashes,
    const uint32_t* asset_sizes,
    const uint32_t* asset_name_offsets,
    const char* asset_name_data,
    GetContentTagFunc get_content_tag);

int WriteContentIndex(
    StorageAPI* storage_api,
    ContentIndex* content_index,
    const char* path);

ContentIndex* ReadContentIndex(
    StorageAPI* storage_api,
    const char* path);

int WriteContent(
    StorageAPI* source_storage_api,
    StorageAPI* target_storage_api,
    CompressionAPI* compression_api,
    JobAPI* job_api,
    ContentIndex* content_index,
    PathLookup* asset_content_hash_to_path,
    const char* assets_folder,
    const char* content_folder);

ContentIndex* ReadContent(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    const char* content_path);

ContentIndex* CreateMissingContent(
    HashAPI* hash_api,
    const ContentIndex* content_index,
    const char* version_assets_path,
    const VersionIndex* version,
    GetContentTagFunc get_content_tag);

ContentIndex* MergeContentIndex(
    ContentIndex* local_content_index,
    ContentIndex* remote_content_index);

PathLookup* CreateContentHashToPathLookup(
    const VersionIndex* version_index,
    uint64_t* out_unique_asset_indexes);

int ReconstructVersion(
    StorageAPI* storage_api,
    CompressionAPI* compression_api,
    JobAPI* job_api,
    const ContentIndex* content_index,
    const VersionIndex* version_index,
    const char* content_path,
    const char* version_path);




///////////// Test functions

size_t GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t path_data_size);

VersionIndex* BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const Paths* paths,
    const TLongtail_Hash* pathHashes,
    const TLongtail_Hash* contentHashes,
    const uint32_t* contentSizes);

struct Paths
{
    uint32_t m_DataSize;
    uint32_t* m_PathCount;
    uint32_t* m_Offsets;
    char* m_Data;
};

struct ContentIndex
{
    uint64_t* m_BlockCount;
    uint64_t* m_AssetCount;

    TLongtail_Hash* m_BlockHash; // []
    TLongtail_Hash* m_AssetContentHash; // []
    uint64_t* m_AssetBlockIndex; // []
    uint32_t* m_AssetBlockOffset; // []
    uint32_t* m_AssetLength; // []
};

struct VersionIndex
{
    uint64_t* m_AssetCount;
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_AssetContentHash;
    uint32_t* m_AssetSize;
    // uint64_t m_CreationDate;
    // uint64_t m_ModificationDate;
    uint32_t* m_NameOffset;
    uint32_t m_NameDataSize;
    char* m_NameData;
};

Paths* MakePaths(
    uint32_t path_count,
    const char* const* path_names);

TLongtail_Hash GetPathHash(HashAPI* hash_api, const char* path);

char* GetBlockName(TLongtail_Hash block_hash);

#if defined(LONGTAIL_IMPLEMENTATION)

#ifdef LONGTAIL_VERBOSE_LOGS
    #define LONGTAIL_LOG(fmt, ...) \
        printf("--- ");printf(fmt, __VA_ARGS__);
#else
    #define LONGTAIL_LOG(fmr, ...)
#endif

#include <inttypes.h>

int IsDirPath(const char* path)
{
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

TLongtail_Hash GetPathHash(HashAPI* hash_api, const char* path)
{
    HashAPI::HContext context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, context, (uint32_t)strlen(path), (void*)path);
    return (TLongtail_Hash)hash_api->EndContext(hash_api, context);
}

int SafeCreateDir(StorageAPI* storage_api, const char* path)
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

int EnsureParentPathExists(StorageAPI* storage_api, const char* path)
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

typedef char* TFolderPaths;
LONGTAIL_DECLARE_ARRAY_TYPE(TFolderPaths, malloc, free)

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

int RecurseTree(StorageAPI* storage_api, const char* root_folder, ProcessEntry entry_processor, void* context)
{
    LONGTAIL_LOG("RecurseTree `%s`\n", root_folder)
    TFolderPaths* folder_paths = SetCapacity_TFolderPaths((TFolderPaths*)0, 256);

    uint32_t folder_index = 0;

    *Push_TFolderPaths(folder_paths) = strdup(root_folder);

    while (folder_index != GetSize_TFolderPaths(folder_paths))
    {
        const char* asset_folder = folder_paths[folder_index++];

        StorageAPI::HIterator fs_iterator = storage_api->StartFind(storage_api, asset_folder);
        if (fs_iterator)
        {
            do
            {
                if (const char* dir_name = storage_api->GetDirectoryName(storage_api, fs_iterator))
                {
                    entry_processor(context, asset_folder, dir_name);
                    *Push_TFolderPaths(folder_paths) = storage_api->ConcatPath(storage_api, asset_folder, dir_name);
                    if (GetSize_TFolderPaths(folder_paths) == GetCapacity_TFolderPaths(folder_paths))
                    {
                        uint32_t unprocessed_count = (GetSize_TFolderPaths(folder_paths) - folder_index);
                        if (folder_index > 0)
                        {
                            if (unprocessed_count > 0)
                            {
                                memmove(folder_paths, &folder_paths[folder_index], sizeof(TFolderPaths) * unprocessed_count);
                                SetSize_TFolderPaths(folder_paths, unprocessed_count);
                            }
                            folder_index = 0;
                        }
                        else
                        {
                            TFolderPaths* folder_paths_new = SetCapacity_TFolderPaths((TFolderPaths*)0, GetCapacity_TFolderPaths(folder_paths) + 256);
                            if (unprocessed_count > 0)
                            {
                                SetSize_TFolderPaths(folder_paths_new, unprocessed_count);
                                memcpy(folder_paths_new, &folder_paths[folder_index], sizeof(TFolderPaths) * unprocessed_count);
                            }
                            Free_TFolderPaths(folder_paths);
                            folder_paths = folder_paths_new;
                            folder_index = 0;
                        }
                    }
                }
                else if(const char* file_name = storage_api->GetFileName(storage_api, fs_iterator))
                {
                    entry_processor(context, asset_folder, file_name);
                }
            }while(storage_api->FindNext(storage_api, fs_iterator));
            storage_api->CloseFind(storage_api, fs_iterator);
        }
        free((void*)asset_folder);
    }
    Free_TFolderPaths(folder_paths);
    return 1;
}

size_t GetPathsSize(uint32_t path_count, uint32_t path_data_size)
{
    return sizeof(Paths) +
        sizeof(uint32_t) +
        sizeof(uint32_t) * path_count +
        path_data_size;
};

Paths* CreatePaths(uint32_t path_count, uint32_t path_data_size)
{
    Paths* paths = (Paths*)malloc(GetPathsSize(path_count, path_data_size));
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

Paths* MakePaths(uint32_t path_count, const char* const* path_names)
{
    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    Paths* paths = CreatePaths(path_count, name_data_size);
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

Paths* AppendPath(Paths* paths, const char* path, uint32_t* max_path_count, uint32_t* max_data_size, uint32_t path_count_increment, uint32_t data_size_increment)
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
        Paths* new_paths = CreatePaths(new_path_count, new_path_data_size);
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

Paths* GetFilesRecursively(StorageAPI* storage_api, const char* root_path)
{
    LONGTAIL_LOG("GetFilesRecursively `%s`\n", root_path)
    struct Context {
        StorageAPI* m_StorageAPI;
        uint32_t m_ReservedPathCount;
        uint32_t m_ReservedPathSize;
        uint32_t m_RootPathLength;
        Paths* m_Paths;
    };

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    auto add_file = [](void* context, const char* root_path, const char* file_name)
    {
        Context* paths_context = (Context*)context;
        StorageAPI* storage_api = paths_context->m_StorageAPI;

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

        Paths* paths = paths_context->m_Paths;
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

    Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), paths};
    paths = 0;

    if(!RecurseTree(storage_api, root_path, add_file, &context))
    {
        free(context.m_Paths);
        return 0;
    }

    return context.m_Paths;
}

struct HashJob
{
    StorageAPI* m_StorageAPI;
    HashAPI* m_HashAPI;
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_ContentHash;
    uint32_t* m_ContentSize;
    const char* m_RootPath;
    const char* m_Path;
    int m_Success;
};

void HashFile(void* context)
{
    HashJob* hash_job = (HashJob*)context;

    TLongtail_Hash content_hash = 0;
    hash_job->m_Success = 0;

    if (!IsDirPath(hash_job->m_Path))
    {
        StorageAPI* storage_api = hash_job->m_StorageAPI;
        char* path = storage_api->ConcatPath(storage_api, hash_job->m_RootPath, hash_job->m_Path);

        StorageAPI::HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
        HashAPI::HContext hash_context = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI);
        if(file_handle)
        {
            hash_job->m_Success = 1;
            uint32_t file_size = (uint32_t)storage_api->GetSize(storage_api, file_handle);
            *hash_job->m_ContentSize = file_size;

            uint8_t batch_data[65536];
            uint32_t offset = 0;
            while (offset != file_size)
            {
                uint32_t len = (uint32_t)((file_size - offset) < sizeof(batch_data) ? (file_size - offset) : sizeof(batch_data));
                bool read_ok = storage_api->Read(storage_api, file_handle, offset, len, batch_data);
                if (!read_ok)
                {
                    LONGTAIL_LOG("HashFile failed to read from `%s`\n", path)
                    hash_job->m_Success = 0;
                    break;
                }
                offset += len;
                hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, hash_context, len, batch_data);
            }
            storage_api->CloseRead(storage_api, file_handle);
        }
        else
        {
            LONGTAIL_LOG("HashFile failed to open `%s`\n", path)
        }
        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, hash_context);
        free((char*)path);
    }
    else {
        content_hash = 0;
		*hash_job->m_ContentSize = 0;
        hash_job->m_Success = 1;
    }
    *hash_job->m_ContentHash = content_hash;
    *hash_job->m_PathHash = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path);
}

int GetFileHashes(StorageAPI* storage_api, HashAPI* hash_api, JobAPI* job_api, const char* root_path, const Paths* paths, TLongtail_Hash* pathHashes, TLongtail_Hash* contentHashes, uint32_t* contentSizes)
{
    LONGTAIL_LOG("GetFileHashes in folder `%s` for %u assets\n", root_path, (uint32_t)*paths->m_PathCount)
    uint32_t asset_count = *paths->m_PathCount;

    if (job_api)
    {
        if (!job_api->ReserveJobs(job_api, asset_count))
        {
            return 0;
        }
    }
    HashJob* hash_jobs = new HashJob[asset_count];

    uint64_t assets_left = asset_count;
    static const uint32_t BATCH_SIZE = 64;
    JobAPI::TJobFunc func[BATCH_SIZE];
    void* ctx[BATCH_SIZE];
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
            HashJob* job = &hash_jobs[offset + i];
            ctx[i] = &hash_jobs[i + offset];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_RootPath = root_path;
            job->m_Path = &paths->m_Data[paths->m_Offsets[i + offset]];
            job->m_PathHash = &pathHashes[i + offset];
            job->m_ContentHash = &contentHashes[i + offset];
            job->m_ContentSize = &contentSizes[i + offset];
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

    int success = 1;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        if (!hash_jobs[i].m_Success)
        {
            success = 0;
            LONGTAIL_LOG("Failed to hash `%s`\n", hash_jobs[i].m_Path)
        }
    }

    delete [] hash_jobs;
    return success;
}

size_t GetVersionIndexDataSize(uint32_t asset_count, uint32_t name_data_size)
{
    size_t version_index_size = sizeof(uint64_t) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        name_data_size;

    return version_index_size;
}

size_t GetVersionIndexSize(uint32_t asset_count, uint32_t path_data_size)
{
    return sizeof(VersionIndex) +
            GetVersionIndexDataSize(asset_count, path_data_size);
}

void InitVersionIndex(VersionIndex* version_index, size_t version_index_data_size)
{
    char* p = (char*)version_index;
    p += sizeof(VersionIndex);

    size_t version_index_data_start = (size_t)p;

    version_index->m_AssetCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t asset_count = *version_index->m_AssetCount;

    version_index->m_PathHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetContentHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetSize = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_NameOffset = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    size_t version_index_name_data_start = (size_t)p;

    version_index->m_NameDataSize = (uint32_t)(version_index_data_size - (version_index_name_data_start - version_index_data_start));

    version_index->m_NameData = (char*)p;
}

VersionIndex* BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const Paths* paths,
    const TLongtail_Hash* pathHashes,
    const TLongtail_Hash* contentHashes,
    const uint32_t* contentSizes)
{
    uint32_t asset_count = *paths->m_PathCount;
    VersionIndex* version_index = (VersionIndex*)mem;
    version_index->m_AssetCount = (uint64_t*)&((char*)mem)[sizeof(VersionIndex)];
    *version_index->m_AssetCount = asset_count;

    InitVersionIndex(version_index, mem_size - sizeof(VersionIndex));

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        version_index->m_PathHash[i] = pathHashes[i];
        version_index->m_AssetContentHash[i] = contentHashes[i];
        version_index->m_AssetSize[i] = contentSizes[i];
        version_index->m_NameOffset[i] = paths->m_Offsets[i];
    }
    memmove(version_index->m_NameData, paths->m_Data, paths->m_DataSize);

    return version_index;
}

VersionIndex* CreateVersionIndex(StorageAPI* storage_api, HashAPI* hash_api, JobAPI* job_api, const char* root_path, const Paths* paths)
{
    uint32_t path_count = *paths->m_PathCount;
    uint32_t* contentSizes = (uint32_t*)malloc(sizeof(uint32_t) * path_count);
    TLongtail_Hash* pathHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * path_count);
    TLongtail_Hash* contentHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * path_count);
    
    if (!GetFileHashes(storage_api, hash_api, job_api, root_path, paths, pathHashes, contentHashes, contentSizes))
    {
        LONGTAIL_LOG("Failed to hash assets in `%s`\n", root_path);
        free(contentHashes);
        free(pathHashes);
        free(contentSizes);
        return 0;
    }

    size_t version_index_size = GetVersionIndexSize(path_count, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        pathHashes,
        contentHashes,
        contentSizes);

    free(contentHashes);
    contentHashes = 0;
    free(pathHashes);
    pathHashes = 0;
    free(contentSizes);
    contentSizes = 0;

    return version_index;
}

int WriteVersionIndex(StorageAPI* storage_api, VersionIndex* version_index, const char* path)
{
    LONGTAIL_LOG("WriteVersionIndex to `%s`\n", path)
    size_t index_data_size = GetVersionIndexDataSize((uint32_t)(*version_index->m_AssetCount), version_index->m_NameDataSize);

    if (!EnsureParentPathExists(storage_api, path))
    {
        return 0;
    }
    StorageAPI::HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path);
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

VersionIndex* ReadVersionIndex(StorageAPI* storage_api, const char* path)
{
    LONGTAIL_LOG("ReadVersionIndex from `%s`\n", path)
    StorageAPI::HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    size_t version_index_data_size = storage_api->GetSize(storage_api, file_handle);
    VersionIndex* version_index = (VersionIndex*)malloc(sizeof(VersionIndex) + version_index_data_size);
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
    TLongtail_Hash* m_AssetContentHashes; //[]
    uint32_t* m_AssetSizes; // []
    uint32_t* m_AssetCount;
};

size_t GetBlockIndexDataSize(uint32_t asset_count)
{
    return (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        sizeof(uint32_t);
}

BlockIndex* InitBlockIndex(void* mem, uint32_t asset_count)
{
    BlockIndex* block_index = (BlockIndex*)mem;
    char* p = (char*)&block_index[1];
    block_index->m_AssetContentHashes = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash) * asset_count;
    block_index->m_AssetSizes = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;
    block_index->m_AssetCount = (uint32_t*)p;
    return block_index;
}

size_t GetBlockIndexSize(uint32_t asset_count)
{
    size_t block_index_size =
        sizeof(BlockIndex) +
        GetBlockIndexDataSize(asset_count);

    return block_index_size;
}

BlockIndex* CreateBlockIndex(
    void* mem,
    HashAPI* hash_api,
    uint32_t asset_count_in_block,
    uint32_t* asset_indexes,
    const TLongtail_Hash* asset_content_hashes,
    const uint32_t* asset_sizes)
{
    BlockIndex* block_index = InitBlockIndex(mem, asset_count_in_block);
    for (uint32_t i = 0; i < asset_count_in_block; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        block_index->m_AssetContentHashes[i] = asset_content_hashes[asset_index];
        block_index->m_AssetSizes[i] = asset_sizes[asset_index];
    }
	*block_index->m_AssetCount = asset_count_in_block;
	HashAPI::HContext hash_context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, hash_context, (uint32_t)(GetBlockIndexDataSize(asset_count_in_block)), (void*)&block_index[1]);
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
    return sizeof(ContentIndex) +
        GetContentIndexDataSize(block_count, asset_count);
}

void InitContentIndex(ContentIndex* content_index)
{
    char* p = (char*)&content_index[1];
    content_index->m_BlockCount = (uint64_t*)p;
    p += sizeof(uint64_t);
    content_index->m_AssetCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t asset_count = *content_index->m_AssetCount;

    content_index->m_BlockHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_AssetContentHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);
    content_index->m_AssetBlockIndex = (uint64_t*)p;
    p += (sizeof(uint64_t) * asset_count);
    content_index->m_AssetBlockOffset = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
    content_index->m_AssetLength = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
}

uint32_t GetUniqueAssets(uint64_t asset_count, const TLongtail_Hash* asset_content_hashes, uint32_t* out_unique_asset_indexes)
{
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)asset_count);
    void* hash_mem = malloc(hash_size);
    jc::HashTable<TLongtail_Hash, uint32_t> lookup_table;
    lookup_table.Create((uint32_t)asset_count, hash_mem);
    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash hash = asset_content_hashes[i];
        uint32_t* existing_index = lookup_table.Get(hash);
        if (existing_index)
        {
            (*existing_index)++;
        }
        else
        {
            lookup_table.Put(hash, 1);
            out_unique_asset_indexes[unique_asset_count] = i;
            ++unique_asset_count;
        }
    }
    free(hash_mem);
    hash_mem = 0;
    return unique_asset_count;
}

struct CompareAssetEntry
{
    CompareAssetEntry(const TLongtail_Hash* asset_path_hashes, const uint32_t* asset_sizes, const TLongtail_Hash* asset_tags)
        : asset_path_hashes(asset_path_hashes)
        , asset_sizes(asset_sizes)
        , asset_tags(asset_tags)
    {

    }

    // This sorting algorithm is very arbitrary!
    static int Compare(void* context, const void* a_ptr, const void* b_ptr)
    {   
        uint32_t a = *(uint32_t*)a_ptr;
        uint32_t b = *(uint32_t*)b_ptr;
        const CompareAssetEntry* c = (const CompareAssetEntry*)context;
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

    const TLongtail_Hash* asset_path_hashes;
    const uint32_t* asset_sizes;
    const TLongtail_Hash* asset_tags;
};

ContentIndex* CreateContentIndex(
    HashAPI* hash_api,
    const char* assets_path,
    uint64_t asset_count,
    const TLongtail_Hash* asset_content_hashes,
    const TLongtail_Hash* asset_path_hashes,
    const uint32_t* asset_sizes,
    const uint32_t* asset_name_offsets,
    const char* asset_name_data,
    GetContentTagFunc get_content_tag)
{
    LONGTAIL_LOG("CreateContentIndex from `%s`\n", assets_path)
    if (asset_count == 0)
    {
        size_t content_index_size = GetContentIndexSize(0, 0);
        ContentIndex* content_index = (ContentIndex*)malloc(content_index_size);

        content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex)];
        content_index->m_AssetCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
        *content_index->m_BlockCount = 0;
        *content_index->m_AssetCount = 0;
        InitContentIndex(content_index);
        return content_index;
    }
    uint32_t* assets_index = (uint32_t*)malloc(sizeof(uint32_t) * asset_count);
    uint32_t unique_asset_count = GetUniqueAssets(asset_count, asset_content_hashes, assets_index);

    TLongtail_Hash* content_tags = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);
    for (uint32_t i = 0; i < unique_asset_count; ++i)
    {
        uint32_t asset_index = assets_index[i];
        content_tags[asset_index] = get_content_tag(assets_path, &asset_name_data[asset_name_offsets[asset_index]]);
    }

    CompareAssetEntry compare_asset_entry(asset_path_hashes, asset_sizes, content_tags);
    qsort_s(&assets_index[0], unique_asset_count, sizeof(uint32_t), CompareAssetEntry::Compare, &compare_asset_entry);

    BlockIndex** block_indexes = (BlockIndex**)malloc(sizeof(BlockIndex*) * unique_asset_count);

    static const uint32_t MAX_ASSETS_PER_BLOCK = 16384u;
    static const uint32_t MAX_BLOCK_SIZE = 131072u;
    uint32_t stored_asset_indexes[MAX_ASSETS_PER_BLOCK];

    uint32_t current_size = 0;
    uint64_t i = 0;
    uint32_t asset_count_in_block = 0;
    uint32_t block_count = 0;

    while (i < unique_asset_count)
    {
        asset_count_in_block = 0;

        uint64_t asset_index = assets_index[i];

        uint32_t current_size = asset_sizes[asset_index];
        TLongtail_Hash current_tag = content_tags[asset_index];

        stored_asset_indexes[asset_count_in_block] = asset_index;
        ++asset_count_in_block;

		while((i + 1) < unique_asset_count)
        {
			asset_index = assets_index[(i + 1)];
            uint32_t asset_size = asset_sizes[asset_index];

            // Break if resulting asset count will exceed MAX_ASSETS_PER_BLOCK
            if (asset_count_in_block == MAX_ASSETS_PER_BLOCK)
            {
                break;
            }

            // Break if resulting size will exceed MAX_BLOCK_SIZE and our block is bigger than one fourth of max size
            if ((current_size > MAX_BLOCK_SIZE / 4) && ((asset_size + current_size) > MAX_BLOCK_SIZE))
            {
                break;
            }

            // Break on tag change when we are more than four fifths filled in the block
            if (asset_size + current_size > (MAX_BLOCK_SIZE - (MAX_BLOCK_SIZE / 5) ) && current_tag != content_tags[asset_index])
            {
                break;
            }

            current_size += asset_size;
            stored_asset_indexes[asset_count_in_block] = asset_index;
            ++asset_count_in_block;

			++i;
		}

        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            hash_api,
            asset_count_in_block,
            stored_asset_indexes,
            asset_content_hashes,
            asset_sizes);

        ++block_count;
        ++i;
    }

    if (current_size > 0)
    {
        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            hash_api,
            asset_count_in_block,
            stored_asset_indexes,
            asset_content_hashes,
            asset_sizes);
        ++block_count;
    }

    free(content_tags);
    content_tags = 0;
    free(assets_index);
    assets_index = 0;

    // Build Content Index (from block list)
    size_t content_index_size = GetContentIndexSize(block_count, unique_asset_count);
    ContentIndex* content_index = (ContentIndex*)malloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex)];
    content_index->m_AssetCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_AssetCount = unique_asset_count;
    InitContentIndex(content_index);

    uint64_t asset_index = 0;
    for (uint32_t i = 0; i < block_count; ++i)
    {
        BlockIndex* block_index = block_indexes[i];
        content_index->m_BlockHash[i] = block_index->m_BlockHash;
        uint64_t asset_offset = 0;
        for (uint32_t a = 0; a < *block_index->m_AssetCount; ++a)
        {
            content_index->m_AssetContentHash[asset_index] = block_index->m_AssetContentHashes[a];
            content_index->m_AssetBlockIndex[asset_index] = i;
            content_index->m_AssetBlockOffset[asset_index] = asset_offset;
            content_index->m_AssetLength[asset_index] = block_index->m_AssetSizes[a];

            asset_offset += block_index->m_AssetSizes[a];
            ++asset_index;
            if (asset_index > unique_asset_count)
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

int WriteContentIndex(StorageAPI* storage_api, ContentIndex* content_index, const char* path)
{
    LONGTAIL_LOG("WriteContentIndex to `%s`, assets %u, blocks %u\n", path, (uint32_t)*content_index->m_AssetCount, (uint32_t)*content_index->m_BlockCount)
    size_t index_data_size = GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_AssetCount);

    if (!EnsureParentPathExists(storage_api, path))
    {
        return 0;
    }
    StorageAPI::HOpenFile file_handle = storage_api->OpenWriteFile(storage_api, path);
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

ContentIndex* ReadContentIndex(StorageAPI* storage_api, const char* path)
{
    LONGTAIL_LOG("ReadContentIndex from `%s`\n", path)
    StorageAPI::HOpenFile file_handle = storage_api->OpenReadFile(storage_api, path);
    if (!file_handle)
    {
        return 0;
    }
    size_t content_index_data_size = storage_api->GetSize(storage_api, file_handle);
    ContentIndex* content_index = (ContentIndex*)malloc(sizeof(ContentIndex) + content_index_data_size);
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
    jc::HashTable<TLongtail_Hash, uint32_t> m_HashToNameOffset;
    const char* m_NameData;
};

PathLookup* CreateContentHashToPathLookup(const VersionIndex* version_index, uint64_t* out_unique_asset_indexes)
{
    uint32_t asset_count = (uint32_t)(*version_index->m_AssetCount);
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize(asset_count);
    PathLookup* path_lookup = (PathLookup*)malloc(sizeof(PathLookup) + hash_size);
    path_lookup->m_HashToNameOffset.Create(asset_count, &path_lookup[1]);
    path_lookup->m_NameData = version_index->m_NameData;

    // Only pick up unique assets
    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash content_hash = version_index->m_AssetContentHash[i];
        uint32_t* existing_index = path_lookup->m_HashToNameOffset.Get(content_hash);
        if (existing_index == 0)
        {
            path_lookup->m_HashToNameOffset.Put(content_hash, version_index->m_NameOffset[i]);
            if (out_unique_asset_indexes)
            {
                out_unique_asset_indexes[unique_asset_count] = i;
            }
            ++unique_asset_count;
        }
    }
    return path_lookup;
}

const char* GetPathFromAssetContentHash(const PathLookup* path_lookup, TLongtail_Hash asset_content_hash)
{
    const uint32_t* index_ptr = path_lookup->m_HashToNameOffset.Get(asset_content_hash);
    if (index_ptr == 0)
    {
        return 0;
    }
    return &path_lookup->m_NameData[*index_ptr];
}

struct WriteBlockJob
{
    StorageAPI* m_SourceStorageAPI;
    StorageAPI* m_TargetStorageAPI;
    CompressionAPI* m_CompressionAPI;
    const char* m_ContentFolder;
    const char* m_AssetsFolder;
    const ContentIndex* m_ContentIndex;
    const PathLookup* m_PathLookup;
    uint64_t m_FirstAssetIndex;
    uint32_t m_AssetCount;
    uint32_t m_Success;
};

WriteBlockJob* CreateWriteContentBlockJob()
{
    size_t job_size = sizeof(WriteBlockJob);
    WriteBlockJob* job = (WriteBlockJob*)malloc(job_size);
    return job;
}

char* GetBlockName(TLongtail_Hash block_hash)
{
    char* name = (char*)malloc(64);
    sprintf(name, "0x%" PRIx64, block_hash);
    return name;
}

void WriteContentBlockJob(void* context)
{
    WriteBlockJob* job = (WriteBlockJob*)context;
    StorageAPI* source_storage_api = job->m_SourceStorageAPI;
    StorageAPI* target_storage_api = job->m_TargetStorageAPI;
    CompressionAPI* compression_api = job->m_CompressionAPI;

    const ContentIndex* content_index = job->m_ContentIndex;
    const char* content_folder = job->m_ContentFolder;
    uint64_t block_start_asset_index = job->m_FirstAssetIndex;
    uint32_t asset_count = job->m_AssetCount;
    uint64_t block_index = content_index->m_AssetBlockIndex[block_start_asset_index];
    TLongtail_Hash block_hash = content_index->m_BlockHash[block_index];

    char* block_name = GetBlockName(block_hash);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    char* block_path = target_storage_api->ConcatPath(target_storage_api, content_folder, file_name);

    char tmp_block_name[64];
    sprintf(tmp_block_name, "%s.tmp", block_name);
    char* tmp_block_path = (char*)target_storage_api->ConcatPath(target_storage_api, content_folder, tmp_block_name);

    uint32_t block_data_size = 0;
    for (uint32_t asset_index = block_start_asset_index; asset_index < block_start_asset_index + asset_count; ++asset_index)
    {
        uint32_t asset_size = content_index->m_AssetLength[asset_index];
        block_data_size += asset_size;
    }

    char* write_buffer = (char*)malloc(block_data_size);
    char* write_ptr = write_buffer;

    for (uint32_t asset_index = block_start_asset_index; asset_index < block_start_asset_index + asset_count; ++asset_index)
    {
        TLongtail_Hash asset_content_hash = content_index->m_AssetContentHash[asset_index];
        uint32_t asset_size = content_index->m_AssetLength[asset_index];
        const char* asset_path = GetPathFromAssetContentHash(job->m_PathLookup, asset_content_hash);
        if (!asset_path)
        {
            LONGTAIL_LOG("Failed to get path for asset content 0x%" PRIx64 "\n", asset_content_hash)
            free(write_buffer);
            free(block_name);
            return;
        }

        if (!IsDirPath(asset_path))
        {
            char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
            StorageAPI::HOpenFile file_handle = source_storage_api->OpenReadFile(source_storage_api, full_path);
            if (!file_handle || (source_storage_api->GetSize(source_storage_api, file_handle) != asset_size))
            {
                LONGTAIL_LOG("Missing or mismatching asset content `%s`\n", asset_path)
                free(write_buffer);
                free(block_name);
                source_storage_api->CloseRead(source_storage_api, file_handle);
                return;
            }
            //printf("Storing `%s` in block `%s` at offset %u, size %u\n", asset_path, block_name, (uint32_t)write_offset, (uint32_t)asset_size);
            source_storage_api->Read(source_storage_api, file_handle, 0, asset_size, write_ptr);
            write_ptr += asset_size;

            source_storage_api->CloseRead(source_storage_api, file_handle);
            free((char*)full_path);
            full_path = 0;
        }
    }

    CompressionAPI::HCompressionContext compression_context = compression_api->CreateCompressionContext(compression_api, compression_api->GetDefaultSettings(compression_api));
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
        StorageAPI::HOpenFile block_file_handle = target_storage_api->OpenWriteFile(target_storage_api, tmp_block_path);
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

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(TLongtail_Hash) * asset_count, &content_index->m_AssetContentHash[block_start_asset_index]);
        write_offset += sizeof(TLongtail_Hash) * asset_count;

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(uint32_t) * asset_count, &content_index->m_AssetLength[block_start_asset_index]);
        write_offset += sizeof(uint32_t) * asset_count;

        write_ok = write_ok & target_storage_api->Write(target_storage_api, block_file_handle, write_offset, sizeof(uint32_t), &asset_count);
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
    StorageAPI* source_storage_api,
    StorageAPI* target_storage_api,
    CompressionAPI* compression_api,
    JobAPI* job_api,
    ContentIndex* content_index,
    PathLookup* asset_content_hash_to_path,
    const char* assets_folder,
    const char* content_folder)
{
    LONGTAIL_LOG("WriteContent from `%s` to `%s`, assets %u, blocks %u\n", assets_folder, content_folder, (uint32_t)*content_index->m_AssetCount, (uint32_t)*content_index->m_BlockCount)
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

    WriteBlockJob** write_block_jobs = (WriteBlockJob**)malloc(sizeof(WriteBlockJob*) * block_count);
    uint32_t block_start_asset_index = 0;
    for (uint32_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHash[block_index];
        uint32_t asset_count = 1;
        while (content_index->m_AssetBlockIndex[block_start_asset_index + asset_count] == block_index)
        {
            ++asset_count;
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
            block_start_asset_index += asset_count;
            continue;
        }

        WriteBlockJob* job = CreateWriteContentBlockJob();
        write_block_jobs[block_index] = job;
        job->m_SourceStorageAPI = source_storage_api;
        job->m_TargetStorageAPI = target_storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_ContentFolder = content_folder;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = content_index;
        job->m_PathLookup = asset_content_hash_to_path;
        job->m_FirstAssetIndex = block_start_asset_index;
        job->m_AssetCount = asset_count;
        job->m_Success = 0;

        if (job_api == 0)
        {
            WriteContentBlockJob(job);
        }
        else
        {
            JobAPI::TJobFunc func[1] = { WriteContentBlockJob };
            void* ctx[1] = { job };

            job_api->SubmitJobs(job_api, 1, func, ctx);
        }

        block_start_asset_index += asset_count;
    }

    if (job_api)
    {
        job_api->WaitForAllJobs(job_api);
    }

    int success = 1;
    while (block_count--)
    {
        WriteBlockJob* job = write_block_jobs[block_count];
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
    StorageAPI* m_StorageAPI;
    CompressionAPI* m_CompressionAPI;
    char* m_BlockPath;
    char** m_AssetPaths;
    uint32_t m_AssetCount;
    uint32_t* m_AssetBlockOffsets;
    uint32_t* m_AssetLengths;
    uint32_t m_Success;
};

ReconstructJob* CreateReconstructJob(uint32_t asset_count)
{
    size_t job_size = sizeof(ReconstructJob) +
        sizeof(char*) * asset_count +
        sizeof(uint32_t) * asset_count +
        sizeof(uint32_t) * asset_count;

    ReconstructJob* job = (ReconstructJob*)malloc(job_size);
    char* p = (char*)&job[1];
    job->m_AssetPaths = (char**)p;
    p += sizeof(char*) * asset_count;

    job->m_AssetBlockOffsets = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    job->m_AssetLengths = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    return job;
}

static void ReconstructFromBlock(void* context)
{
    ReconstructJob* job = (ReconstructJob*)context;
    StorageAPI* storage_api = job->m_StorageAPI;

    CompressionAPI* compression_api = job->m_CompressionAPI;
    StorageAPI::HOpenFile block_file_handle = storage_api->OpenReadFile(storage_api, job->m_BlockPath);
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
    CompressionAPI::HDecompressionContext compression_context = compression_api->CreateDecompressionContext(compression_api);
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

    for (uint32_t asset_index = 0; asset_index < job->m_AssetCount; ++asset_index)
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
            StorageAPI::HOpenFile asset_file_handle = storage_api->OpenWriteFile(storage_api, asset_path);
            if(!asset_file_handle)
            {
                LONGTAIL_LOG("Failed to create asset file `%s`\n", asset_path)
                free(decompressed_buffer);
                decompressed_buffer = 0;
                return;
            }
            uint32_t asset_length = job->m_AssetLengths[asset_index];
            uint64_t read_offset = job->m_AssetBlockOffsets[asset_index];
            uint64_t write_offset = 0;
            bool write_ok = storage_api->Write(storage_api, asset_file_handle, write_offset, asset_length, &decompressed_buffer[read_offset]);
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
    ReconstructOrder(const ContentIndex* content_index, const TLongtail_Hash* asset_hashes, const uint64_t* version_index_to_content_index)
        : content_index(content_index)
        , asset_hashes(asset_hashes)
        , version_index_to_content_index(version_index_to_content_index)
    {

    }

    static int Compare(void* context, const void* a_ptr, const void* b_ptr)
    {   
        ReconstructOrder* c = (ReconstructOrder*)context;
        uint64_t a = *(uint64_t*)a_ptr;
        uint64_t b = *(uint64_t*)b_ptr;
        TLongtail_Hash a_hash = c->asset_hashes[a];
        TLongtail_Hash b_hash = c->asset_hashes[b];
        uint32_t a_asset_index_in_content_index = (uint32_t)c->version_index_to_content_index[a];
        uint32_t b_asset_index_in_content_index = (uint32_t)c->version_index_to_content_index[b];

        uint64_t a_block_index = c->content_index->m_AssetBlockIndex[a_asset_index_in_content_index];
        uint64_t b_block_index = c->content_index->m_AssetBlockIndex[b_asset_index_in_content_index];
        if (a_block_index > b_block_index)
        {
            return 1;
        }
        if (a_block_index > b_block_index)
        {
            return -1;
        }

        uint32_t a_offset_in_block = c->content_index->m_AssetBlockOffset[a_asset_index_in_content_index];
        uint32_t b_offset_in_block = c->content_index->m_AssetBlockOffset[b_asset_index_in_content_index];
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

    const ContentIndex* content_index;
    const TLongtail_Hash* asset_hashes;
    const uint64_t* version_index_to_content_index;
};

int ReconstructVersion(
    StorageAPI* storage_api,
    CompressionAPI* compression_api,
    JobAPI* job_api,
    const ContentIndex* content_index,
    const VersionIndex* version_index,
    const char* content_path,
    const char* version_path)
{
    LONGTAIL_LOG("ReconstructVersion from `%s` to `%s`, assets %u\n", content_path, version_path, (uint32_t)*version_index->m_AssetCount)
    uint32_t hash_size = jc::HashTable<uint64_t, uint32_t>::CalcSize((uint32_t)*content_index->m_AssetCount);
    jc::HashTable<uint64_t, uint32_t> content_hash_to_content_asset_index;
    void* content_hash_to_content_asset_index_mem = malloc(hash_size);
    content_hash_to_content_asset_index.Create((uint32_t)*content_index->m_AssetCount, content_hash_to_content_asset_index_mem);
    for (uint64_t i = 0; i < *content_index->m_AssetCount; ++i)
    {
        content_hash_to_content_asset_index.Put(content_index->m_AssetContentHash[i], i);
    }

    uint64_t asset_count = *version_index->m_AssetCount;
    uint64_t* asset_order = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
    uint64_t* version_index_to_content_index = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
    uint64_t asset_found_count = 0;
    for (uint64_t i = 0; i < asset_count; ++i)
    {
        asset_order[i] = i;
        uint32_t* asset_index_ptr = content_hash_to_content_asset_index.Get(version_index->m_AssetContentHash[i]);
        if (!asset_index_ptr)
        {
			LONGTAIL_LOG("Asset 0x%" PRIx64 " for asset `%s` was not find in content index\n", version_index->m_AssetContentHash[i], &version_index->m_NameData[version_index->m_NameOffset[i]])
            continue;
        }
        version_index_to_content_index[i] = *asset_index_ptr;
		++asset_found_count;
    }

    if (asset_found_count != asset_count)
    {
        free(content_hash_to_content_asset_index_mem);
        return 0;
    }

    free(content_hash_to_content_asset_index_mem);
    content_hash_to_content_asset_index_mem = 0;

    ReconstructOrder reconstruct_order(content_index, version_index->m_AssetContentHash, version_index_to_content_index);
    qsort_s(&asset_order[0], asset_count, sizeof(uint64_t), ReconstructOrder::Compare, &reconstruct_order);

	if (job_api)
	{
		if (!job_api->ReserveJobs(job_api, asset_count))
		{
			free(asset_order);
			return 0;
		}
	}
    ReconstructJob** reconstruct_jobs = (ReconstructJob**)malloc(sizeof(ReconstructJob*) * asset_count);
    uint32_t job_count = 0;
    uint64_t i = 0;
    while (i < asset_count)
    {
        uint64_t asset_index = asset_order[i];

        uint32_t asset_index_in_content_index = version_index_to_content_index[asset_index];
        uint64_t block_index = content_index->m_AssetBlockIndex[asset_index_in_content_index];

        uint32_t asset_count_from_block = 1;
        while(((i + asset_count_from_block) < asset_count))
        {
            uint32_t next_asset_index = asset_order[i + asset_count_from_block];
            uint64_t next_asset_index_in_content_index = version_index_to_content_index[next_asset_index];
            uint64_t next_block_index = content_index->m_AssetBlockIndex[next_asset_index_in_content_index];
            if (block_index != next_block_index)
            {
                break;
            }
            ++asset_count_from_block;
        }

        ReconstructJob* job = CreateReconstructJob(asset_count_from_block);
        reconstruct_jobs[job_count++] = job;
        job->m_StorageAPI = storage_api;
        job->m_CompressionAPI = compression_api;
        job->m_Success = 0;

        TLongtail_Hash block_hash = content_index->m_BlockHash[block_index];
        char* block_name = GetBlockName(block_hash);
        char block_file_name[64];
        sprintf(block_file_name, "%s.lrb", block_name);

        job->m_BlockPath = storage_api->ConcatPath(storage_api, content_path, block_file_name);
        job->m_AssetCount = asset_count_from_block;

        for (uint32_t j = 0; j < asset_count_from_block; ++j)
        {
            uint64_t asset_index = asset_order[i + j];
            const char* asset_path = &version_index->m_NameData[version_index->m_NameOffset[asset_index]];
            job->m_AssetPaths[j] = storage_api->ConcatPath(storage_api, version_path, asset_path);
            uint64_t asset_index_in_content_index = version_index_to_content_index[asset_index];
            job->m_AssetBlockOffsets[j] = content_index->m_AssetBlockOffset[asset_index_in_content_index];
            job->m_AssetLengths[j] = content_index->m_AssetLength[asset_index_in_content_index];
        }

        if (job_api == 0)
        {
            ReconstructFromBlock(job);
        }
        else
        {
            JobAPI::TJobFunc func[1] = { ReconstructFromBlock };
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
        ReconstructJob* job = reconstruct_jobs[job_count];
        if (!job->m_Success)
        {
            success = 0;
            LONGTAIL_LOG("Failed reconstructing `%s`\n", job->m_AssetPaths[i]);
        }
        free(job->m_BlockPath);
        for (uint32_t i = 0; i < job->m_AssetCount; ++i)
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

BlockIndex* ReadBlock(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    const char* full_block_path)
{
    StorageAPI::HOpenFile f = storage_api->OpenReadFile(storage_api, full_block_path);
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
    uint32_t asset_count = 0;
    if (!storage_api->Read(storage_api, f, s - sizeof(uint32_t), sizeof(uint32_t), &asset_count))
    {
        storage_api->CloseRead(storage_api, f);
        return 0;
    }
    BlockIndex* block_index = InitBlockIndex(malloc(GetBlockIndexSize(asset_count)), asset_count);
    size_t block_index_data_size = GetBlockIndexDataSize(asset_count);

    int ok = storage_api->Read(storage_api, f, s - block_index_data_size, block_index_data_size, &block_index[1]);
    storage_api->CloseRead(storage_api, f);
    if (!ok)
    {
        free(block_index);
        return 0;
    }
    HashAPI::HContext hash_context = hash_api->BeginContext(hash_api);
    hash_api->Hash(hash_api, hash_context, (uint32_t)(GetBlockIndexDataSize(asset_count)), (void*)&block_index[1]);
    TLongtail_Hash block_hash = hash_api->EndContext(hash_api, hash_context);
    block_index->m_BlockHash = block_hash;

    return block_index;
}

ContentIndex* ReadContent(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    const char* content_path)
{
    LONGTAIL_LOG("ReadContent from `%s`\n", content_path)

    struct Context {
        StorageAPI* m_StorageAPI;
        uint32_t m_ReservedPathCount;
        uint32_t m_ReservedPathSize;
        uint32_t m_RootPathLength;
        Paths* m_Paths;
        uint64_t m_AssetCount;
    };

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    auto on_path = [](void* context, const char* root_path, const char* file_name)
    {
        Context* paths_context = (Context*)context;
        StorageAPI* storage_api = paths_context->m_StorageAPI;

        char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
        if (storage_api->IsDir(storage_api, full_path))
        {
            return;
        }

        Paths* paths = paths_context->m_Paths;
        const uint32_t root_path_length = paths_context->m_RootPathLength;
        const char* s = &full_path[root_path_length];
        if (*s == '/')
        {
            ++s;
        }

        StorageAPI::HOpenFile f = storage_api->OpenReadFile(storage_api, full_path);
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
        uint32_t asset_count = 0;
        int ok = storage_api->Read(storage_api, f, block_size - sizeof(uint32_t), sizeof(uint32_t), &asset_count);

        if (!ok)
        {
            free(full_path);
            storage_api->CloseRead(storage_api, f);
            return;
        }

        uint32_t asset_index_size = sizeof(uint32_t) + (asset_count) * (sizeof(TLongtail_Hash) + sizeof(uint32_t));
        ok = block_size >= asset_index_size;

        if (!ok)
        {
            free(full_path);
            return;
        }

        paths_context->m_AssetCount += asset_count;
        paths_context->m_Paths = AppendPath(paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);

        free(full_path);
        full_path = 0;
    };

    Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(content_path)), paths, 0};
    if(!RecurseTree(storage_api, content_path, on_path, &context))
    {
        free(context.m_Paths);
        return 0;
    }
	paths = context.m_Paths;

    uint32_t asset_count = context.m_AssetCount;
    uint64_t block_count = *paths->m_PathCount;
    size_t content_index_data_size = GetContentIndexDataSize(block_count, asset_count);
    ContentIndex* content_index = (ContentIndex*)malloc(sizeof(ContentIndex) + content_index_data_size);
	content_index->m_BlockCount = (uint64_t*) & ((char*)content_index)[sizeof(ContentIndex)];
	content_index->m_AssetCount = (uint64_t*) & ((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
	*content_index->m_BlockCount = block_count;
	*content_index->m_AssetCount = asset_count;
	InitContentIndex(content_index);

    uint64_t asset_offset = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        const char* block_path = &paths->m_Data[paths->m_Offsets[b]];
        char* full_block_path = storage_api->ConcatPath(storage_api, content_path, block_path);

        const char* last_delimiter = strrchr(full_block_path, '/');
        const char* file_name = last_delimiter ? last_delimiter + 1 : full_block_path;

        BlockIndex* block_index = ReadBlock(
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

        uint32_t block_asset_count = *block_index->m_AssetCount;
        uint32_t block_offset = 0;
        for (uint32_t a = 0; a < block_asset_count; ++a)
        {
            content_index->m_AssetContentHash[asset_offset + a] = block_index->m_AssetContentHashes[a];
            content_index->m_AssetLength[asset_offset + a] = block_index->m_AssetSizes[a];
            content_index->m_AssetBlockOffset[asset_offset + a] = block_offset;
            content_index->m_AssetBlockIndex[asset_offset + a] = b;
            block_offset += content_index->m_AssetLength[asset_offset + a];
        }
        content_index->m_BlockHash[b] = block_index->m_BlockHash;
        asset_offset += block_asset_count;

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

ContentIndex* CreateMissingContent(
    HashAPI* hash_api,
    const ContentIndex* content_index,
    const char* version_assets_path,
    const VersionIndex* version,
    GetContentTagFunc get_content_tag)
{
    LONGTAIL_LOG("CreateMissingContent from `%s`\n", version_assets_path)
    uint64_t asset_count = *version->m_AssetCount;
    TLongtail_Hash* removed_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);

    uint32_t added_hash_count = 0;
    uint32_t removed_hash_count = 0;
    DiffHashes(content_index->m_AssetContentHash, *content_index->m_AssetCount, version->m_AssetContentHash, asset_count, &added_hash_count, added_hashes, &removed_hash_count, removed_hashes);

    if (added_hash_count == 0)
    {
        ContentIndex* diff_content_index = CreateContentIndex(
            hash_api,
            version_assets_path,
            0,
            0,
            0,
            0,
            0,
            0,
            get_content_tag);
        return diff_content_index;
    }

    uint32_t* diff_asset_sizes = (uint32_t*)malloc(sizeof(uint32_t) * added_hash_count);
    uint32_t* diff_name_offsets = (uint32_t*)malloc(sizeof(uint32_t) * added_hash_count);

    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)asset_count);
    jc::HashTable<TLongtail_Hash, uint32_t> asset_index_lookup;
    void* path_lookup_mem = malloc(hash_size);
    asset_index_lookup.Create(asset_count, path_lookup_mem);
    for (uint64_t i = 0; i < asset_count; ++i)
    {
        asset_index_lookup.Put(version->m_AssetContentHash[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        uint32_t* asset_index_ptr = asset_index_lookup.Get(added_hashes[j]);
        if (!asset_index_ptr)
        {
            free(path_lookup_mem);
            free(removed_hashes);
            free(added_hashes);
            return 0;
        }
        uint64_t asset_index = *asset_index_ptr;
        diff_asset_sizes[j] = version->m_AssetSize[asset_index];
        diff_name_offsets[j] = version->m_NameOffset[asset_index];
    }
    free(path_lookup_mem);
    path_lookup_mem = 0;

    ContentIndex* diff_content_index = CreateContentIndex(
        hash_api,
        version_assets_path,
        added_hash_count,
        added_hashes,
        added_hashes,
        diff_asset_sizes,
        diff_name_offsets,
        version->m_NameData,
        get_content_tag);

    return diff_content_index;
}

ContentIndex* MergeContentIndex(
    ContentIndex* local_content_index,
    ContentIndex* remote_content_index)
{
    uint64_t local_block_count = *local_content_index->m_BlockCount;
    uint64_t remote_block_count = *remote_content_index->m_BlockCount;
    uint64_t local_asset_count = *local_content_index->m_AssetCount;
    uint64_t remote_asset_count = *remote_content_index->m_AssetCount;
    uint64_t block_count = local_block_count + remote_block_count;
    uint64_t asset_count = local_asset_count + remote_asset_count;
    size_t content_index_size = GetContentIndexSize(block_count, asset_count);
    ContentIndex* content_index = (ContentIndex*)malloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex)];
    content_index->m_AssetCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_AssetCount = asset_count;
    InitContentIndex(content_index);

    for (uint64_t b = 0; b < local_block_count; ++b)
    {
        content_index->m_BlockHash[b] = local_content_index->m_BlockHash[b];
    }
    for (uint64_t b = 0; b < remote_block_count; ++b)
    {
        content_index->m_BlockHash[local_block_count + b] = remote_content_index->m_BlockHash[b];
    }
    for (uint64_t a = 0; a < local_asset_count; ++a)
    {
        content_index->m_AssetContentHash[a] = local_content_index->m_AssetContentHash[a];
        content_index->m_AssetBlockIndex[a] = local_content_index->m_AssetBlockIndex[a];
        content_index->m_AssetBlockOffset[a] = local_content_index->m_AssetBlockOffset[a];
        content_index->m_AssetLength[a] = local_content_index->m_AssetLength[a];
    }
    for (uint64_t a = 0; a < remote_asset_count; ++a)
    {
        content_index->m_AssetContentHash[local_asset_count + a] = remote_content_index->m_AssetContentHash[a];
        content_index->m_AssetBlockIndex[local_asset_count + a] = local_block_count + remote_content_index->m_AssetBlockIndex[a];
        content_index->m_AssetBlockOffset[local_asset_count + a] = remote_content_index->m_AssetBlockOffset[a];
        content_index->m_AssetLength[local_asset_count + a] = remote_content_index->m_AssetLength[a];
    }
    return content_index;
}

#endif // LONGTAIL_IMPLEMENTATION
