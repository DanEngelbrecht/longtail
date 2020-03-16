#include "longtail_fsblockstore.h"

#include "../../src/longtail.h"
#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct BlockHashToBlockState
{
    uint64_t key;
    uint32_t value;
};

struct FSBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    char* m_ContentPath;
    HLongtail_SpinLock m_Lock;

    struct Longtail_ContentIndex* m_ContentIndex;
    struct BlockHashToBlockState* m_BlockState;
};

struct FSStoredBlock
{
    struct Longtail_StoredBlock m_StoredBlock;
};

static int FSStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSStoredBlock_Dispose(%p)", stored_block)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    Longtail_Free(stored_block);
    return 0;
}

#define MAX_BLOCK_NAME_LENGTH   32

static void GetBlockName(TLongtail_Hash block_hash, char* out_name)
{
    LONGTAIL_FATAL_ASSERT(out_name, return)
    sprintf(&out_name[5], "0x%016" PRIx64, block_hash);
    memmove(out_name, &out_name[7], 4);
    out_name[4] = '/';
}

static char* GetBlockPath(struct FSBlockStoreAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    LONGTAIL_FATAL_ASSERT(fsblockstore_api, return 0)
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[72];
    sprintf(file_name, "chunks/%s.lrb", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static char* GetTempBlockPath(struct FSBlockStoreAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    LONGTAIL_FATAL_ASSERT(fsblockstore_api, return 0)
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[72];
    sprintf(file_name, "chunks/%s.tmp", block_name);
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

static int ScanBlock(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)

    struct ScanBlockJob* job = (struct ScanBlockJob*)context;
    const char* block_path = job->m_BlockPath;
    if (!EndsWith(block_path, ".lrb"))
    {
        job->m_Err = ENOENT;
        return 0;
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
    return 0;
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
    LONGTAIL_FATAL_ASSERT(out_content_index != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore::ReadContent(%p, %p, %u, %p, %s, %p",
        storage_api, job_api, content_index_hash_identifier, progress_api, content_path, out_content_index)

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

static int FSBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncCompleteAPI* async_complete_api)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_PutStoredBlock(%p, %p, %p", block_store_api, stored_block, async_complete_api)

    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;

    uint64_t block_hash = *stored_block->m_BlockIndex->m_BlockHash;

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    intptr_t block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    if (block_ptr != -1)
    {
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        return async_complete_api->OnComplete(async_complete_api, 0);
    }

    hmput(fsblockstore_api->m_BlockState, block_hash, 0);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    char* block_path = GetBlockPath(fsblockstore_api, block_hash);
    char* tmp_block_path = GetTempBlockPath(fsblockstore_api, block_hash);

    int err = EnsureParentPathExists(fsblockstore_api->m_StorageAPI, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PutStoredBlock: Failed to create parent path for `%s`, %d", tmp_block_path, err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }

    Longtail_StorageAPI_HOpenFile block_file_handle;
    err = fsblockstore_api->m_StorageAPI->OpenWriteFile(fsblockstore_api->m_StorageAPI, tmp_block_path, 0, &block_file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PutStoredBlock: Failed to open block for write file `%s`, %d", tmp_block_path, err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }

    uint32_t write_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, block_index_data_size, &stored_block->m_BlockIndex[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PutStoredBlock: Failed to write block index to file `%s`, %d", tmp_block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        block_file_handle = 0;
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    write_offset += block_index_data_size;

    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, stored_block->m_BlockChunksDataSize, stored_block->m_BlockData);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PutStoredBlock: Failed to write chunk data to file `%s`, %d", tmp_block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        block_file_handle = 0;
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    write_offset = stored_block->m_BlockChunksDataSize;

    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
    err = fsblockstore_api->m_StorageAPI->RenameFile(fsblockstore_api->m_StorageAPI, tmp_block_path, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock: Failed to rename block file from `%s` to `%s`, %d", tmp_block_path, block_path, err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    Longtail_Free((char*)tmp_block_path);
    tmp_block_path = 0;
    Longtail_Free((char*)block_path);
    block_path = 0;

    // We added a block, the stored file on disk is no longer valid, remove it
    const char* content_index_path = Longtail_ConcatPath(fsblockstore_api->m_ContentPath, "store.lci");

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    if (Longtail_IsFile(content_index_path))
    {
        Longtail_RemoveFile(content_index_path);
    }

    hmput(fsblockstore_api->m_BlockState, block_hash, 1);
    if (fsblockstore_api->m_ContentIndex)
    {
        struct Longtail_ContentIndex* added_content_index;
        int err = Longtail_CreateContentIndexFromBlocks(
            *fsblockstore_api->m_ContentIndex->m_HashAPI,
            1,
            &stored_block->m_BlockIndex,
            &added_content_index);
        if (err)
        {
            hmdel(fsblockstore_api->m_BlockState, block_hash);
        }
        else
        {
            struct Longtail_ContentIndex* new_content_index;
            err = Longtail_MergeContentIndex(
                fsblockstore_api->m_ContentIndex,
                added_content_index,
                &new_content_index);
            Longtail_Free(added_content_index);
            if (err)
            {
                hmdel(fsblockstore_api->m_BlockState, block_hash);
            }
            else
            {
                Longtail_Free(fsblockstore_api->m_ContentIndex);
                fsblockstore_api->m_ContentIndex = new_content_index;
            }
        }
    }
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    Longtail_Free((void*)content_index_path);

    return async_complete_api->OnComplete(async_complete_api, 0);
}

static int FSBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_StoredBlock** out_stored_block,
    struct Longtail_AsyncCompleteAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetStoredBlock(%p, 0x" PRIx64 ", %p, %p", block_store_api, block_hash, out_stored_block, async_complete_api)
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    intptr_t block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    if (block_ptr == -1)
    {
        char* block_path = GetBlockPath(fsblockstore_api, block_hash);
        if (!fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
        {
            Longtail_Free((void*)block_path);
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            return async_complete_api->OnComplete(async_complete_api, ENOENT);
        }
        Longtail_Free((void*)block_path);
        hmput(fsblockstore_api->m_BlockState, block_hash, 1);
        block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    }
    uint32_t state = fsblockstore_api->m_BlockState[block_ptr].value;
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    while (state == 0)
    {
        Longtail_Sleep(1000);
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        state = hmget(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    }
    char* block_path = GetBlockPath(fsblockstore_api, block_hash);
    Longtail_StorageAPI_HOpenFile f;
    int err = fsblockstore_api->m_StorageAPI->OpenReadFile(fsblockstore_api->m_StorageAPI, block_path, &f);
    if (err)
    {
        // This can fail if someone is currently writing the block but is not finished! Should be really rare since it is just a rename?
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to open block `%s`, %d", block_path, err)
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    uint64_t stored_block_data_size;
    err = fsblockstore_api->m_StorageAPI->GetSize(fsblockstore_api->m_StorageAPI, f, &stored_block_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to get size of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }

    size_t block_mem_size = Longtail_GetStoredBlockSize(stored_block_data_size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    LONGTAIL_FATAL_ASSERT(stored_block, return ENOMEM)
    void* block_data = &((uint8_t*)stored_block)[block_mem_size - stored_block_data_size];
    err = fsblockstore_api->m_StorageAPI->Read(fsblockstore_api->m_StorageAPI, f, 0, stored_block_data_size, block_data);
    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
    f = 0;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to read from block `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    err = Longtail_InitStoredBlockFromData(
        stored_block,
        block_data,
        stored_block_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format from block `%s`, %d", block_path, err)
        Longtail_Free(stored_block);
        stored_block = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    stored_block->Dispose = FSStoredBlock_Dispose;
    Longtail_Free(block_path);
    block_path = 0;

    *out_stored_block = stored_block;
    return async_complete_api->OnComplete(async_complete_api, 0);
}

static int FSBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_JobAPI* job_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_content_index, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetIndex(%p, %p, %u, %p, %p", block_store_api, job_api, default_hash_api_identifier, progress_api, out_content_index)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    if (!fsblockstore_api->m_ContentIndex)
    {
        int err = ReadContent(
            fsblockstore_api->m_StorageAPI,
            job_api,
            default_hash_api_identifier,
            progress_api,
            fsblockstore_api->m_ContentPath,
            &fsblockstore_api->m_ContentIndex);
        if (err)
        {
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            return err;
        }

        uint64_t block_count = *fsblockstore_api->m_ContentIndex->m_BlockCount;
        for (uint64_t b = 0; b < block_count; ++b)
        {
            uint64_t block_hash = fsblockstore_api->m_ContentIndex->m_BlockHashes[b];
            hmput(fsblockstore_api->m_BlockState, block_hash, 1);
        }

        const char* content_index_path = Longtail_ConcatPath(fsblockstore_api->m_ContentPath, "store.lci");
        err = Longtail_WriteContentIndex(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentIndex, content_index_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Failed to store content index for `%s`, %d", fsblockstore_api->m_ContentPath, err);
        }
        Longtail_Free((void*)content_index_path);
    }
    size_t content_index_size;
    void* tmp_content_buffer;
    int err = Longtail_WriteContentIndexToBuffer(fsblockstore_api->m_ContentIndex, &tmp_content_buffer, &content_index_size);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    if (err)
    {
        Longtail_Free(tmp_content_buffer);
        return err;
    }
    err = Longtail_ReadContentIndexFromBuffer(tmp_content_buffer, content_index_size, out_content_index);
    Longtail_Free(tmp_content_buffer);
    return err;
}

static int FSBlockStore_GetStoredBlockPath(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_path, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetStoredBlockPath(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, out_path)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    *out_path = GetBlockPath(fsblockstore_api, block_hash);
    return 0;
}


static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_FATAL_ASSERT(api, return)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)api;
    if (fsblockstore_api->m_ContentIndex)
    {
        const char* content_index_path = Longtail_ConcatPath(fsblockstore_api->m_ContentPath, "store.lci");
        Longtail_WriteContentIndex(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentIndex, content_index_path);
        Longtail_Free((void*)content_index_path);
    }
    hmfree(fsblockstore_api->m_BlockState);
    fsblockstore_api->m_BlockState = 0;
    Longtail_DeleteSpinLock(fsblockstore_api->m_Lock);
    Longtail_Free(fsblockstore_api->m_Lock);
    Longtail_Free(fsblockstore_api->m_ContentPath);
    Longtail_Free(fsblockstore_api->m_ContentIndex);
    Longtail_Free(fsblockstore_api);
}

static int FSBlockStore_Init(
    struct FSBlockStoreAPI* api,
    struct Longtail_StorageAPI* storage_api,
    const char* content_path)
{
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(storage_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_path, return EINVAL)
    api->m_BlockStoreAPI.m_API.Dispose = FSBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = FSBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = FSBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = FSBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStoredBlockPath = FSBlockStore_GetStoredBlockPath;
    api->m_StorageAPI = storage_api;
    api->m_ContentPath = Longtail_Strdup(content_path);
    api->m_ContentIndex = 0;
    api->m_BlockState = 0;
    const char* content_index_path = Longtail_ConcatPath(api->m_ContentPath, "store.lci");
    if (Longtail_IsFile(content_index_path))
    {
        int err = Longtail_ReadContentIndex(api->m_StorageAPI, content_index_path, &api->m_ContentIndex);
        if (!err)
        {
            uint64_t block_count = *api->m_ContentIndex->m_BlockCount;
            for (uint64_t b = 0; b < block_count; ++b)
            {
                uint64_t block_hash = api->m_ContentIndex->m_BlockHashes[b];
                hmput(api->m_BlockState, block_hash, 1);
            }
        }
    }
    Longtail_Free((void*)content_index_path);
    int err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
    const char* content_path)
{
    LONGTAIL_FATAL_ASSERT(storage_api, return 0)
    LONGTAIL_FATAL_ASSERT(content_path, return 0)
    struct FSBlockStoreAPI* api = (struct FSBlockStoreAPI*)Longtail_Alloc(sizeof(struct FSBlockStoreAPI));
    if (!api)
    {
        // TODO: Log
        return 0;
    }
    FSBlockStore_Init(
        api,
        storage_api,
        content_path);
    return &api->m_BlockStoreAPI;
}
