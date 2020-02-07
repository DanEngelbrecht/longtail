#include "longtail_fsblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct FSBlockStoreJobAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_JobAPI* m_JobAPI;
    char* m_ContentPath;
    void* m_ContentIndexBuffer;
    uint64_t m_ContentIndexSize;
};

struct FSStoredBlock
{
    struct Longtail_StoredBlock m_StoredBlock;
};

static int FSStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free(stored_block);
    return 0;
}

#define MAX_BLOCK_NAME_LENGTH   32

static void GetBlockName(TLongtail_Hash block_hash, char* out_name)
{
    sprintf(&out_name[5], "0x%016" PRIx64, block_hash);
    memmove(out_name, &out_name[7], 4);
    out_name[4] = '/';
}

static char* GetBlockPath(struct FSBlockStoreJobAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static char* GetTempBlockPath(struct FSBlockStoreJobAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.tmp", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static int ReadBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* block_path,
    struct Longtail_BlockIndex** out_block_index)
{
    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, block_path, &f);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to open block `%s`, %d", block_path, err)
        return err;
    }
    uint64_t block_size;
    err = storage_api->GetSize(storage_api, f, &block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to get size of block `%s`, %d", block_path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size < (sizeof(TLongtail_Hash) + sizeof(uint32_t)))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size > 0xffffffff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    uint64_t read_offset = 0;
    TLongtail_Hash block_hash;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(TLongtail_Hash), &block_hash);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    read_offset += sizeof(TLongtail_Hash);
    uint32_t chunk_count;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(uint32_t), &chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);

    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    void* block_index_mem = Longtail_Alloc(block_index_size);
    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(block_index_mem, chunk_count);
    err = storage_api->Read(storage_api, f, 0, block_index_data_size, &block_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        Longtail_Free(block_index);
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    *out_block_index = block_index;
    return 0;
}

struct ScanBlockJob
{
    struct Longtail_StorageAPI* m_StorageAPI;
    const char* m_ContentPath;
    const char* m_BlockPath;
    struct Longtail_BlockIndex* m_BlockIndex;
    int m_Err;
};

int EndsWith(const char *str, const char *suffix)
{
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

static void ScanBlock(void* context)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return)

    struct ScanBlockJob* job = (struct ScanBlockJob*)context;
    const char* block_path = job->m_BlockPath;
    if (!EndsWith(block_path, ".lrb"))
    {
        job->m_Err = ENOENT;
        return;
    }

    struct Longtail_StorageAPI* storage_api = job->m_StorageAPI;
    const char* content_path = job->m_ContentPath;
    char* full_block_path = storage_api->ConcatPath(storage_api, content_path, block_path);

    job->m_Err = ReadBlockIndex(
        storage_api,
        full_block_path,
        &job->m_BlockIndex);

    Longtail_Free(full_block_path);
    full_block_path = 0;
}

static int ReadContent(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_JobAPI* job_api,
    uint32_t content_index_hash_identifier,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* content_path,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore::ReadContent: Reading from `%s`", content_path)

    struct Longtail_FileInfos* file_infos;
    int err = Longtail_GetFilesRecursively(
        storage_api,
        content_path,
        &file_infos);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore::ReadContent: Failed to scan folder `%s`, %d", content_path, err)
        return err;
    }

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    err = job_api->ReserveJobs(job_api, *file_infos->m_Paths.m_PathCount);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent: Failed to reserve jobs for `%s`, %d", content_path, err)
        Longtail_Free(file_infos);
        file_infos = 0;
        return err;
    }

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "FSBlockStore::ReadContent: Scanning %u files from `%s`", *file_infos->m_Paths.m_PathCount, content_path)

    struct ScanBlockJob* scan_jobs = (struct ScanBlockJob*)Longtail_Alloc(sizeof(struct ScanBlockJob) * *file_infos->m_Paths.m_PathCount);
    LONGTAIL_FATAL_ASSERT(scan_jobs, return ENOMEM)

    for (uint32_t path_index = 0; path_index < *file_infos->m_Paths.m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        const char* block_path = &file_infos->m_Paths.m_Data[file_infos->m_Paths.m_Offsets[path_index]];
        job->m_BlockIndex = 0;

        job->m_StorageAPI = storage_api;
        job->m_ContentPath = content_path;
        job->m_BlockPath = block_path;
        job->m_BlockIndex = 0;
        job->m_Err = EINVAL;

        Longtail_JobAPI_JobFunc job_func[] = {ScanBlock};
        void* ctx[] = {job};
        Longtail_JobAPI_Jobs jobs;
        err = job_api->CreateJobs(job_api, 1, job_func, ctx, &jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, 1, jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
    }

    err = job_api->WaitForAllJobs(job_api, job_progress_context, job_progress_func);
    LONGTAIL_FATAL_ASSERT(!err, return err)

    uint64_t block_count = 0;
    uint64_t chunk_count = 0;
    for (uint32_t path_index = 0; path_index < *file_infos->m_Paths.m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        if (job->m_Err == 0)
        {
            ++block_count;
            chunk_count += *job->m_BlockIndex->m_ChunkCount;
        }
    }

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "FSBlockStore::ReadContent: Found %" PRIu64 " chunks in %" PRIu64 " blocks from `%s`", chunk_count, block_count, content_path)

    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    LONGTAIL_FATAL_ASSERT(content_index, return ENOMEM)

    // TODO: This is a bit low level to be outside of longtail, but will do for now
    content_index->m_Version = (uint32_t*)(void*)&((char*)content_index)[sizeof(struct Longtail_ContentIndex)];
    content_index->m_HashAPI = (uint32_t*)(void*)&((char*)content_index)[sizeof(struct Longtail_ContentIndex) + sizeof(uint32_t)];
    content_index->m_BlockCount = (uint64_t*)(void*)&((char*)content_index)[sizeof(struct Longtail_ContentIndex) + sizeof(uint32_t) + sizeof(uint32_t)];
    content_index->m_ChunkCount = (uint64_t*)(void*)&((char*)content_index)[sizeof(struct Longtail_ContentIndex) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t)];
    *content_index->m_Version = Longtail_CurrentContentIndexVersion;
    *content_index->m_HashAPI = content_index_hash_identifier;
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    Longtail_InitContentIndex(content_index, content_index_size);

    uint64_t block_offset = 0;
    uint64_t chunk_offset = 0;
    for (uint32_t path_index = 0; path_index < *file_infos->m_Paths.m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
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

    Longtail_Free(scan_jobs);
    scan_jobs = 0;

    Longtail_Free(file_infos);
    file_infos = 0;

    *out_content_index = content_index;
    return 0;
}

static int FSBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;

    char* block_path = GetBlockPath(fsblockstore_api, *stored_block->m_BlockIndex->m_BlockHash);
    if (fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return 0;
    }

    char* tmp_block_path = GetTempBlockPath(fsblockstore_api, *stored_block->m_BlockIndex->m_BlockHash);
    int err = EnsureParentPathExists(fsblockstore_api->m_StorageAPI, block_path);
    if (err)
    {
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    Longtail_StorageAPI_HOpenFile block_file_handle;
    err = fsblockstore_api->m_StorageAPI->OpenWriteFile(fsblockstore_api->m_StorageAPI, tmp_block_path, 0, &block_file_handle);
    if (err)
    {
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    uint32_t write_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, block_index_data_size, &stored_block->m_BlockIndex[1]);
    if (err)
    {
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        block_file_handle = 0;
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    write_offset += block_index_data_size;

    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, stored_block->m_BlockDataSize, stored_block->m_BlockData);
    if (err)
    {
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    write_offset = stored_block->m_BlockDataSize;

    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
    err = fsblockstore_api->m_StorageAPI->RenameFile(fsblockstore_api->m_StorageAPI, tmp_block_path, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock: Failed to rename block file from `%s` to `%s`, %d", tmp_block_path, block_path, err)
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    Longtail_Free((char*)tmp_block_path);
    tmp_block_path = 0;
    Longtail_Free((char*)block_path);
    block_path = 0;

    // TODO: Be better - for now now, flush local cache of content index
    void* tmp = fsblockstore_api->m_ContentIndexBuffer;
    fsblockstore_api->m_ContentIndexBuffer = 0;
    fsblockstore_api->m_ContentIndexSize = 0;
    if (tmp)
    {
        Longtail_Free(tmp);
    }

    return 0;
}

static int FSBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_StoredBlock** out_stored_block)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;
    char* block_path = GetBlockPath(fsblockstore_api, block_hash);
    if (!fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return ENOENT;
    }
    if (!out_stored_block)
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return 0;
    }

    Longtail_StorageAPI_HOpenFile f;
    int err = fsblockstore_api->m_StorageAPI->OpenReadFile(fsblockstore_api->m_StorageAPI, block_path, &f);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to open block `%s`, %d", block_path, err)
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    uint64_t block_size;
    err = fsblockstore_api->m_StorageAPI->GetSize(fsblockstore_api->m_StorageAPI, f, &block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to get size of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    if (block_size < (sizeof(TLongtail_Hash) + sizeof(uint32_t)))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    if (block_size > 0xffffffff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    size_t block_mem_size = sizeof(struct Longtail_StoredBlock) + sizeof(struct Longtail_BlockIndex) + block_size;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    LONGTAIL_FATAL_ASSERT(stored_block, return ENOMEM)
    void* block_data_mem = &((uint8_t*)(&stored_block[1]))[sizeof(struct Longtail_BlockIndex)];
    err = fsblockstore_api->m_StorageAPI->Read(fsblockstore_api->m_StorageAPI, f, 0, block_size, block_data_mem);
    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
    f = 0;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to read from block `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    TLongtail_Hash* block_hash_ptr = (TLongtail_Hash*)block_data_mem;
    if ((*block_hash_ptr) != block_hash)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format for block at `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    uint32_t chunk_count = *((uint32_t*)&block_hash_ptr[1]);
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    if (block_size < block_index_data_size)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format for block at `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    stored_block->m_BlockIndex = Longtail_InitBlockIndex(&stored_block[1], chunk_count);
    stored_block->m_BlockData = &((uint8_t*)block_data_mem)[block_index_data_size];
    stored_block->m_BlockDataSize = (uint32_t)(block_size - block_index_data_size);

    stored_block->Dispose = FSStoredBlock_Dispose;
    Longtail_Free(block_path);
    block_path = 0;

    *out_stored_block = stored_block;
    return 0;
}

static int FSBlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, void* context, Longtail_JobAPI_ProgressFunc progress_func, struct Longtail_ContentIndex** out_content_index)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;
    if (!fsblockstore_api->m_ContentIndexBuffer)
    {
        int err = ReadContent(
            fsblockstore_api->m_StorageAPI,
            fsblockstore_api->m_JobAPI,
            default_hash_api_identifier,
            progress_func,
            context,
            fsblockstore_api->m_ContentPath,
            out_content_index);
        if (err)
        {
            return err;
        }
        err = Longtail_WriteContentIndexToBuffer(*out_content_index, &fsblockstore_api->m_ContentIndexBuffer, &fsblockstore_api->m_ContentIndexSize);
        if (err)
        {
            Longtail_Free(*out_content_index);
            *out_content_index = 0;
            return err;
        }
        return 0;
    }

    int err = Longtail_ReadContentIndexFromBuffer(fsblockstore_api->m_ContentIndexBuffer, fsblockstore_api->m_ContentIndexSize, out_content_index);
    return err;
}

static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)api;
    Longtail_Free(fsblockstore_api->m_ContentPath);
    Longtail_Free(fsblockstore_api->m_ContentIndexBuffer);
    Longtail_Free(fsblockstore_api);
}

static int FSBlockStore_Init(
    struct FSBlockStoreJobAPI* api,
    struct Longtail_StorageAPI* storage_api,
	struct Longtail_JobAPI* job_api,
	const char* content_path)
{
    api->m_BlockStoreAPI.m_API.Dispose = FSBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = FSBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = FSBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = FSBlockStore_GetIndex;
    api->m_StorageAPI = storage_api;
    api->m_JobAPI = job_api;
    api->m_ContentPath = Longtail_Strdup(content_path);
    api->m_ContentIndexBuffer = 0;
    api->m_ContentIndexSize = 0;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
	struct Longtail_JobAPI* job_api,
	const char* content_path)
{
    struct FSBlockStoreJobAPI* api = (struct FSBlockStoreJobAPI*)Longtail_Alloc(sizeof(struct FSBlockStoreJobAPI));
    FSBlockStore_Init(
        api,
        storage_api,
        job_api,
        content_path);
    return &api->m_BlockStoreAPI;
}
