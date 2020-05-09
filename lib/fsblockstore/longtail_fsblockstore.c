#include "longtail_fsblockstore.h"

#include "../../src/longtail.h"
#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"
#include "../bikeshed/longtail_bikeshed.h"

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
    struct Longtail_BlockIndex** m_AddedBlockIndexes;
    uint32_t m_DefaultMaxBlockSize;
    uint32_t m_DefaultMaxChunksPerBlock;

    TLongtail_Atomic64 m_IndexGetCount;
    TLongtail_Atomic64 m_BlocksGetCount;
    TLongtail_Atomic64 m_BlocksPutCount;
    TLongtail_Atomic64 m_ChunksGetCount;
    TLongtail_Atomic64 m_ChunksPutCount;
    TLongtail_Atomic64 m_BytesGetCount;
    TLongtail_Atomic64 m_BytesPutCount;
    TLongtail_Atomic64 m_IndexGetRetryCount;
    TLongtail_Atomic64 m_BlockGetRetryCount;
    TLongtail_Atomic64 m_BlockPutRetryCount;
    TLongtail_Atomic64 m_IndexGetFailCount;
    TLongtail_Atomic64 m_BlockGetFailCount;
    TLongtail_Atomic64 m_BlockPutFailCount;
};

static int UpdateContentIndex(
    struct Longtail_ContentIndex* current_content_index,
    struct Longtail_BlockIndex** added_block_indexes,
    struct Longtail_ContentIndex** out_content_index)
{
    struct Longtail_ContentIndex* added_content_index;
    int err = Longtail_CreateContentIndexFromBlocks(
        *current_content_index->m_MaxBlockSize,
        *current_content_index->m_MaxChunksPerBlock,
        (uint64_t)(arrlen(added_block_indexes)),
        added_block_indexes,
        &added_content_index);
    if (err)
    {
        return err;
    }
    struct Longtail_ContentIndex* new_content_index;
    err = Longtail_AddContentIndex(
        current_content_index,
        added_content_index,
        &new_content_index);
    Longtail_Free(added_content_index);
    if (err)
    {
        Longtail_Free(added_content_index);
        return err;
    }
    *out_content_index = new_content_index;
    return 0;
}

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

static int ScanBlock(void* context, uint32_t job_id, int is_cancelled)
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

    if (job->m_Err)
    {
        storage_api->RemoveFile(storage_api, full_block_path);
    }

    Longtail_Free(full_block_path);
    full_block_path = 0;
    return 0;
}

static int ReadContent(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_JobAPI* job_api,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    const char* content_path,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_content_index != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore::ReadContent(%p, %p, %s, %p)",
        storage_api, job_api, content_path, out_content_index)

    struct Longtail_FileInfos* file_infos;
    int err = Longtail_GetFilesRecursively(
        storage_api,
        0,
        0,
        0,
        content_path,
        &file_infos);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            err)
        return err;
    }

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    uint32_t path_count = file_infos->m_Count;
    if (path_count == 0)
    {
        err = Longtail_CreateContentIndexFromBlocks(
            max_block_size,
            max_chunks_per_block,
            0,
            0,
            out_content_index);
        Longtail_Free(file_infos);
        return err;
    }
    Longtail_JobAPI_Group job_group;
    err = job_api->ReserveJobs(job_api, path_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            err)
        Longtail_Free(file_infos);
        file_infos = 0;
        return err;
    }

    size_t scan_jobs_size = sizeof(struct ScanBlockJob) * path_count;
    struct ScanBlockJob* scan_jobs = (struct ScanBlockJob*)Longtail_Alloc(scan_jobs_size);
    if (!scan_jobs)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            ENOMEM)
        return ENOMEM;
    }

    for (uint32_t path_index = 0; path_index < path_count; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        const char* block_path = &file_infos->m_PathData[file_infos->m_PathStartOffsets[path_index]];
        job->m_BlockIndex = 0;

        job->m_StorageAPI = storage_api;
        job->m_ContentPath = content_path;
        job->m_BlockPath = block_path;
        job->m_BlockIndex = 0;
        job->m_Err = EINVAL;

        Longtail_JobAPI_JobFunc job_func[] = {ScanBlock};
        void* ctx[] = {job};
        Longtail_JobAPI_Jobs jobs;
        err = job_api->CreateJobs(job_api, job_group, 1, job_func, ctx, &jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, 1, jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
    }

    err = job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);
    if (err)
    {
        LONGTAIL_LOG(err == ECANCELED ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            err)
        Longtail_Free(scan_jobs);
        Longtail_Free(file_infos);
        return err;
    }
    LONGTAIL_FATAL_ASSERT(!err, return err)

    size_t block_indexes_size = sizeof(struct Longtail_BlockIndex*) * (path_count);
    struct Longtail_BlockIndex** block_indexes = (struct Longtail_BlockIndex**)Longtail_Alloc(block_indexes_size);
    if (!block_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            ENOMEM)
        Longtail_Free(scan_jobs);
        Longtail_Free(file_infos);
        return ENOMEM;
    }

    uint64_t block_count = 0;
    uint64_t chunk_count = 0;
    for (uint32_t path_index = 0; path_index < path_count; ++path_index)
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
        max_block_size,
        max_chunks_per_block,
        block_count,
        block_indexes,
        out_content_index);

    for (uint32_t b = 0; b < block_count; ++b)
    {
        Longtail_Free(block_indexes[b]);
    }
    Longtail_Free(block_indexes);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore::ReadContent(%p, %p, %s, %p) failed with %d",
            storage_api, job_api, content_path, out_content_index,
            err)
    }
    return err;
}

static int FSBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_PutStoredBlock(%p, %p, %p)",
        block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api != 0, return EINVAL)

    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_BlocksPutCount, 1);
    Longtail_AtomicAdd64(&fsblockstore_api->m_ChunksPutCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&fsblockstore_api->m_BytesPutCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    uint64_t block_hash = *stored_block->m_BlockIndex->m_BlockHash;

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    intptr_t block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    if (block_ptr != -1)
    {
        // Already busy doing put
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        async_complete_api->OnComplete(async_complete_api, 0);
        return 0;
    }

    hmput(fsblockstore_api->m_BlockState, block_hash, 0);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    char* block_path = GetBlockPath(fsblockstore_api, block_hash);
    char* tmp_block_path = GetTempBlockPath(fsblockstore_api, block_hash);

    int err = EnsureParentPathExists(fsblockstore_api->m_StorageAPI, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    err = Longtail_WriteStoredBlock(fsblockstore_api->m_StorageAPI, stored_block, tmp_block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    err = fsblockstore_api->m_StorageAPI->RenameFile(fsblockstore_api->m_StorageAPI, tmp_block_path, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
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

    void* block_index_buffer;
    size_t block_index_buffer_size;
    err = Longtail_WriteBlockIndexToBuffer(stored_block->m_BlockIndex, &block_index_buffer, &block_index_buffer_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        return err;
    }
    struct Longtail_BlockIndex* block_index_copy;
    err = Longtail_ReadBlockIndexFromBuffer(block_index_buffer, block_index_buffer_size, &block_index_copy);
    Longtail_Free(block_index_buffer);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        return err;
    }

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    hmput(fsblockstore_api->m_BlockState, block_hash, 1);
    arrput(fsblockstore_api->m_AddedBlockIndexes, block_index_copy);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    async_complete_api->OnComplete(async_complete_api, 0);
    return 0;
}

static int FSBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_PreflightGet(%p, %u %p, %p)",
        block_store_api, block_count, block_hashes, block_ref_counts)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_ref_counts != 0, return EINVAL)
    return 0;
}

static int FSBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetStoredBlock(%p, 0x" PRIx64 ", %p)",
        block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

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
            return ENOENT;
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

    struct Longtail_StoredBlock* stored_block;
    int err = Longtail_ReadStoredBlock(fsblockstore_api->m_StorageAPI, block_path, &stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock(%p, 0x" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            err)
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    Longtail_AtomicAdd64(&fsblockstore_api->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&fsblockstore_api->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&fsblockstore_api->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    Longtail_Free(block_path);
    block_path = 0;

    async_complete_api->OnComplete(async_complete_api, stored_block, 0);
    return 0;
}

static int FSBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetIndex(%p, %u, %p)",
        block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount());
    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    if (!fsblockstore_api->m_ContentIndex)
    {
        int err = ReadContent(
            fsblockstore_api->m_StorageAPI,
            job_api,
            fsblockstore_api->m_DefaultMaxBlockSize,
            fsblockstore_api->m_DefaultMaxChunksPerBlock,
            fsblockstore_api->m_ContentPath,
            &fsblockstore_api->m_ContentIndex);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndex(%p, %u, %p) failed with %d",
                block_store_api, async_complete_api,
                err)
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            Longtail_DisposeAPI(&job_api->m_API);
            return err;
        }

        uint64_t block_count = *fsblockstore_api->m_ContentIndex->m_BlockCount;
        for (uint64_t b = 0; b < block_count; ++b)
        {
            uint64_t block_hash = fsblockstore_api->m_ContentIndex->m_BlockHashes[b];
            hmput(fsblockstore_api->m_BlockState, block_hash, 1);
        }

        const char* content_index_path = fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, "store.lci");
        err = Longtail_WriteContentIndex(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentIndex, content_index_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Failed to store content index for `%s`, %d", fsblockstore_api->m_ContentPath, err);
        }
        Longtail_Free((void*)content_index_path);
    }
    Longtail_DisposeAPI(&job_api->m_API);

    intptr_t new_block_count = arrlen(fsblockstore_api->m_AddedBlockIndexes);
    if (new_block_count > 0)
    {
        struct Longtail_ContentIndex* new_content_index;
        int err = UpdateContentIndex(
            fsblockstore_api->m_ContentIndex,
            fsblockstore_api->m_AddedBlockIndexes,
            &new_content_index);
        if (err)
        {
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            return err;
        }

        Longtail_Free(fsblockstore_api->m_ContentIndex);
        fsblockstore_api->m_ContentIndex = new_content_index;

        while(new_block_count-- > 0)
        {
            struct Longtail_BlockIndex* block_index = fsblockstore_api->m_AddedBlockIndexes[new_block_count];
            Longtail_Free(block_index);
        }
        arrfree(fsblockstore_api->m_AddedBlockIndexes);
    }

    job_api = 0;
    size_t content_index_size;
    void* tmp_content_buffer;
    int err = Longtail_WriteContentIndexToBuffer(fsblockstore_api->m_ContentIndex, &tmp_content_buffer, &content_index_size);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            err)
        Longtail_Free(tmp_content_buffer);
        return err;
    }
    struct Longtail_ContentIndex* content_index;
    err = Longtail_ReadContentIndexFromBuffer(tmp_content_buffer, content_index_size, &content_index);
    Longtail_Free(tmp_content_buffer);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            err)
        return err;
    }
    Longtail_AtomicAdd64(&fsblockstore_api->m_IndexGetCount, 1);
    async_complete_api->OnComplete(async_complete_api, content_index, 0);
    return 0;
}

static int FSBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    out_stats->m_IndexGetCount = fsblockstore_api->m_IndexGetCount;
    out_stats->m_BlocksGetCount = fsblockstore_api->m_BlocksGetCount;
    out_stats->m_BlocksPutCount = fsblockstore_api->m_BlocksPutCount;
    out_stats->m_ChunksGetCount = fsblockstore_api->m_ChunksGetCount;
    out_stats->m_ChunksPutCount = fsblockstore_api->m_ChunksPutCount;
    out_stats->m_BytesGetCount = fsblockstore_api->m_BytesGetCount;
    out_stats->m_BytesPutCount = fsblockstore_api->m_BytesPutCount;
    out_stats->m_IndexGetRetryCount = fsblockstore_api->m_IndexGetRetryCount;
    out_stats->m_BlockGetRetryCount = fsblockstore_api->m_BlockGetRetryCount;
    out_stats->m_BlockPutRetryCount = fsblockstore_api->m_BlockPutRetryCount;
    out_stats->m_IndexGetFailCount = fsblockstore_api->m_IndexGetFailCount;
    out_stats->m_BlockGetFailCount = fsblockstore_api->m_BlockGetFailCount;
    out_stats->m_BlockPutFailCount = fsblockstore_api->m_BlockPutFailCount;
    return 0;
}

static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "FSBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)
    struct FSBlockStoreAPI* fsblockstore_api = (struct FSBlockStoreAPI*)api;
    intptr_t new_block_count = arrlen(fsblockstore_api->m_AddedBlockIndexes);
    if (new_block_count > 0)
    {
        if (fsblockstore_api->m_ContentIndex)
        {
            struct Longtail_ContentIndex* new_content_index;
            int err = UpdateContentIndex(
                fsblockstore_api->m_ContentIndex,
                fsblockstore_api->m_AddedBlockIndexes,
                &new_content_index);
            if (!err)
            {
                Longtail_Free(fsblockstore_api->m_ContentIndex);
                fsblockstore_api->m_ContentIndex = new_content_index;
            }
        }
        else
        {
            Longtail_CreateContentIndexFromBlocks(
                fsblockstore_api->m_DefaultMaxBlockSize,
                fsblockstore_api->m_DefaultMaxChunksPerBlock,
                (uint64_t)(arrlen(fsblockstore_api->m_AddedBlockIndexes)),
                fsblockstore_api->m_AddedBlockIndexes,
                &fsblockstore_api->m_ContentIndex);
        }
        while(new_block_count-- > 0)
        {
            struct Longtail_BlockIndex* block_index = fsblockstore_api->m_AddedBlockIndexes[new_block_count];
            Longtail_Free(block_index);
        }
        arrfree(fsblockstore_api->m_AddedBlockIndexes);
    }

    if (fsblockstore_api->m_ContentIndex)
    {
        const char* content_index_path = fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, "store.lci");
        struct Longtail_ContentIndex* existing_content_index;
        if (0 == Longtail_ReadContentIndex(fsblockstore_api->m_StorageAPI, content_index_path, &existing_content_index))
        {
            struct Longtail_ContentIndex* merged_content_index;
            if (0 == Longtail_MergeContentIndex(existing_content_index, fsblockstore_api->m_ContentIndex, &merged_content_index))
            {
                Longtail_Free(fsblockstore_api->m_ContentIndex);
                fsblockstore_api->m_ContentIndex = merged_content_index;
            }
            Longtail_Free(existing_content_index);
        }
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
    const char* content_path,
    uint32_t default_max_block_size,
    uint32_t default_max_chunks_per_block)
{
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(storage_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_path, return EINVAL)
    api->m_BlockStoreAPI.m_API.Dispose = FSBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = FSBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = FSBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = FSBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = FSBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = FSBlockStore_GetStats;
    api->m_StorageAPI = storage_api;
    api->m_ContentPath = Longtail_Strdup(content_path);
    api->m_ContentIndex = 0;
    api->m_BlockState = 0;
    api->m_AddedBlockIndexes = 0;
    api->m_DefaultMaxBlockSize = default_max_block_size;
    api->m_DefaultMaxChunksPerBlock = default_max_chunks_per_block;

    api->m_IndexGetCount = 0;
    api->m_BlocksGetCount = 0;
    api->m_BlocksPutCount = 0;
    api->m_ChunksGetCount = 0;
    api->m_ChunksPutCount = 0;
    api->m_BytesGetCount = 0;
    api->m_BytesPutCount = 0;
    api->m_IndexGetRetryCount = 0;
    api->m_BlockGetRetryCount = 0;
    api->m_BlockPutRetryCount = 0;
    api->m_IndexGetFailCount = 0;
    api->m_BlockGetFailCount = 0;
    api->m_BlockPutFailCount = 0;

    const char* content_index_path = storage_api->ConcatPath(storage_api, content_path, "store.lci");
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
    return err;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
    const char* content_path,
    uint32_t default_max_block_size,
    uint32_t default_max_chunks_per_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateFSBlockStoreAPI(%p, %s)", storage_api, content_path)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(content_path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(default_max_block_size != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(default_max_chunks_per_block != 0, return 0)
    size_t api_size = sizeof(struct FSBlockStoreAPI);
    struct FSBlockStoreAPI* api = (struct FSBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateFSBlockStoreAPI(%p, %s) failed with %d",
            storage_api, content_path,
            ENOMEM)
        return 0;
    }
    int err = FSBlockStore_Init(
        api,
        storage_api,
        content_path,
        default_max_block_size,
        default_max_chunks_per_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateFSBlockStoreAPI(%p, %s) failed with %d",
            storage_api, content_path,
            err)
        Longtail_Free(api);
        return 0;
    }
    return &api->m_BlockStoreAPI;
}
