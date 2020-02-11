#include "longtail_fsblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct FSBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    char* m_ContentPath;
    void* m_ContentIndexBuffer;
    uint64_t m_ContentIndexSize;
    HLongtail_SpinLock m_Lock;
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

static char* GetBlockPath(struct FSBlockStoreAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static char* GetTempBlockPath(struct FSBlockStoreAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.tmp", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
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

    job->m_Err = Longtail_ReadBlockIndex(
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
    struct Longtail_ProgressAPI* progress_api,
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

    err = job_api->WaitForAllJobs(job_api, progress_api);
    LONGTAIL_FATAL_ASSERT(!err, return err)

    struct Longtail_BlockIndex** block_indexes = (struct Longtail_BlockIndex**)Longtail_Alloc(sizeof(struct Longtail_BlockIndex*) * (*file_infos->m_Paths.m_PathCount));
    LONGTAIL_FATAL_ASSERT(block_indexes != 0, return ENOMEM)

    uint64_t block_count = 0;
    uint64_t chunk_count = 0;
    for (uint32_t path_index = 0; path_index < *file_infos->m_Paths.m_PathCount; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        if (job->m_Err == 0)
        {
            block_indexes[block_count] = job->m_BlockIndex;
            chunk_count += *job->m_BlockIndex->m_ChunkCount;
            ++block_count;
        }
    }
    Longtail_Free(scan_jobs);
    scan_jobs = 0;

    Longtail_Free(file_infos);
    file_infos = 0;

    err = Longtail_CreateContentIndexFromBlocks(
        content_index_hash_identifier,
        block_count,
        block_indexes,
        out_content_index);

    for (uint32_t b = 0; b < block_count; ++b)
    {
        Longtail_Free(block_indexes[b]);
    }
    Longtail_Free(block_indexes);
    return err;
}

static int FSBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block)
{
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;

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

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    Longtail_StorageAPI_HOpenFile block_file_handle;
    err = fsblockstore_api->m_StorageAPI->OpenWriteFile(fsblockstore_api->m_StorageAPI, tmp_block_path, 0, &block_file_handle);
    if (err)
    {
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
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
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
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
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
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

    if (err == EEXIST)
    {
        err = fsblockstore_api->m_StorageAPI->RemoveFile(fsblockstore_api->m_StorageAPI, tmp_block_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PutStoredBlock: Failed to remote temp block file from `%s`, %d", block_path, err)
            err = 0;
        }
    }
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

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
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
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

    size_t block_mem_size = sizeof(struct Longtail_StoredBlock) + sizeof(struct Longtail_BlockIndex) + block_size;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    LONGTAIL_FATAL_ASSERT(stored_block, return ENOMEM)
    stored_block->m_BlockIndex = (struct Longtail_BlockIndex*)&stored_block[1];
    err = fsblockstore_api->m_StorageAPI->Read(fsblockstore_api->m_StorageAPI, f, 0, block_size, &stored_block->m_BlockIndex[1]);
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
    err = Longtail_InitBlockIndexFromData(
        stored_block->m_BlockIndex,
        &stored_block->m_BlockIndex[1],
        block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format from block `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    stored_block->m_BlockData = &((uint8_t*)stored_block->m_BlockIndex)[Longtail_GetBlockIndexSize(*stored_block->m_BlockIndex->m_ChunkCount)];
    stored_block->m_BlockDataSize = (uint32_t)(block_size - Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount));
    stored_block->Dispose = FSStoredBlock_Dispose;
    Longtail_Free(block_path);
    block_path = 0;

    *out_stored_block = stored_block;
    return 0;
}

static int FSBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
	struct Longtail_JobAPI* job_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_ContentIndex** out_content_index)
{
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    if (!fsblockstore_api->m_ContentIndexBuffer)
    {
        int err = ReadContent(
            fsblockstore_api->m_StorageAPI,
			job_api,
            default_hash_api_identifier,
            progress_api,
            fsblockstore_api->m_ContentPath,
            out_content_index);
        if (err)
        {
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            return err;
        }
        err = Longtail_WriteContentIndexToBuffer(*out_content_index, &fsblockstore_api->m_ContentIndexBuffer, &fsblockstore_api->m_ContentIndexSize);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        if (err)
        {
            Longtail_Free(*out_content_index);
            *out_content_index = 0;
            return err;
        }
        return 0;
    }

    int err = Longtail_ReadContentIndexFromBuffer(fsblockstore_api->m_ContentIndexBuffer, fsblockstore_api->m_ContentIndexSize, out_content_index);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    return err;
}

static int FSBlockStore_GetStoredBlockPath(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path)
{
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    *out_path = GetBlockPath(fsblockstore_api, block_hash);
    return 0;
}


static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)api;
    Longtail_DeleteSpinLock(fsblockstore_api->m_Lock);
    Longtail_Free(fsblockstore_api->m_Lock);
    Longtail_Free(fsblockstore_api->m_ContentPath);
    Longtail_Free(fsblockstore_api->m_ContentIndexBuffer);
    Longtail_Free(fsblockstore_api);
}

static int FSBlockStore_Init(
    struct FSBlockStoreAPI* api,
    struct Longtail_StorageAPI* storage_api,
	const char* content_path)
{
    api->m_BlockStoreAPI.m_API.Dispose = FSBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = FSBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = FSBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = FSBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStoredBlockPath = FSBlockStore_GetStoredBlockPath;
    api->m_StorageAPI = storage_api;
    api->m_ContentPath = Longtail_Strdup(content_path);
    api->m_ContentIndexBuffer = 0;
    api->m_ContentIndexSize = 0;
    int err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
	const char* content_path)
{
    struct FSBlockStoreAPI* api = (struct FSBlockStoreAPI*)Longtail_Alloc(sizeof(struct FSBlockStoreAPI));
    FSBlockStore_Init(
        api,
        storage_api,
        content_path);
    return &api->m_BlockStoreAPI;
}
