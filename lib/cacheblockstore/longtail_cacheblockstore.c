#include "longtail_cacheblockstore.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct CacheBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_LocalBlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_RemoteBlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;

    TLongtail_Atomic64 m_StatU64[Longtail_BlockStoreAPI_StatU64_Count];

    HLongtail_SpinLock m_Lock;
    struct Longtail_AsyncFlushAPI** m_PendingAsyncFlushAPIs;

    TLongtail_Atomic32 m_PendingRequestCount;
};

static void CacheBlockStore_CompleteRequest(struct CacheBlockStoreAPI* cacheblockstore_api)
{
    LONGTAIL_FATAL_ASSERT(cacheblockstore_api->m_PendingRequestCount > 0, return)
    struct Longtail_AsyncFlushAPI** pendingAsyncFlushAPIs = 0;
    Longtail_LockSpinLock(cacheblockstore_api->m_Lock);
    if (0 == Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1))
    {
        pendingAsyncFlushAPIs = cacheblockstore_api->m_PendingAsyncFlushAPIs;
        cacheblockstore_api->m_PendingAsyncFlushAPIs = 0;
    }
    Longtail_UnlockSpinLock(cacheblockstore_api->m_Lock);
    size_t c = arrlen(pendingAsyncFlushAPIs);
    for (size_t n = 0; n < c; ++n)
    {
        pendingAsyncFlushAPIs[n]->OnComplete(pendingAsyncFlushAPIs[n], 0);
    }
    arrfree(pendingAsyncFlushAPIs);
}

struct CachedStoredBlock {
    struct Longtail_StoredBlock m_StoredBlock;
    struct Longtail_StoredBlock* m_OriginalStoredBlock;
    TLongtail_Atomic32 m_RefCount;
};

int CachedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    struct CachedStoredBlock* b = (struct CachedStoredBlock*)stored_block;
    int32_t ref_count = Longtail_AtomicAdd32(&b->m_RefCount, -1);
    if (ref_count > 0)
    {
        return 0;
    }
    struct Longtail_StoredBlock* original_stored_block = b->m_OriginalStoredBlock;
    Longtail_Free(b);
    original_stored_block->Dispose(original_stored_block);
    return 0;
}

struct Longtail_StoredBlock* CachedStoredBlock_CreateBlock(struct Longtail_StoredBlock* original_stored_block, int32_t refCount)
{
    size_t cached_stored_block_size = sizeof(struct CachedStoredBlock);
    struct CachedStoredBlock* cached_stored_block = (struct CachedStoredBlock*)Longtail_Alloc(cached_stored_block_size);
    if (!cached_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CachedStoredBlock_CreateBlock(%p) failed with %d",
            original_stored_block,
            ENOMEM)
        return 0;
    }
    cached_stored_block->m_StoredBlock.Dispose = CachedStoredBlock_Dispose;
    cached_stored_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    cached_stored_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    cached_stored_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    cached_stored_block->m_OriginalStoredBlock = original_stored_block;
    cached_stored_block->m_RefCount = refCount;
    return &cached_stored_block->m_StoredBlock;
}

struct PutStoredBlockPutRemoteComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    TLongtail_Atomic32 m_PendingCount;
    int m_RemoteErr;
    struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
};

struct PutStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    struct PutStoredBlockPutRemoteComplete_API* m_PutStoredBlockPutRemoteComplete_API;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
};

static void PutStoredBlockPutLocalComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "PutStoredBlockPutLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct PutStoredBlockPutLocalComplete_API* api = (struct PutStoredBlockPutLocalComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PutStoredBlockPutLocalComplete(%p, %d) failed to store block in local block store, %d",
            async_complete_api, err,
            err)
    }
    struct PutStoredBlockPutRemoteComplete_API* remote_put_api = api->m_PutStoredBlockPutRemoteComplete_API;
    Longtail_Free(api);
    int remain = Longtail_AtomicAdd32(&remote_put_api->m_PendingCount, -1);
    if (remain == 0)
    {
        remote_put_api->m_AsyncCompleteAPI->OnComplete(remote_put_api->m_AsyncCompleteAPI, remote_put_api->m_RemoteErr);
        Longtail_Free(remote_put_api);
    }
    CacheBlockStore_CompleteRequest(cacheblockstore_api);
}

static void PutStoredBlockPutRemoteComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "PutStoredBlockPutRemoteComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct PutStoredBlockPutRemoteComplete_API* api = (struct PutStoredBlockPutRemoteComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "PutStoredBlockPutRemoteComplete(%p, %d) failed store block in remote block store, %d",
            async_complete_api, err,
            err)
    }
    api->m_RemoteErr = err;
    int remain = Longtail_AtomicAdd32(&api->m_PendingCount, -1);
    if (remain == 0)
    {
        api->m_AsyncCompleteAPI->OnComplete(api->m_AsyncCompleteAPI, api->m_RemoteErr);
        if (err)
        {
            Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        }
        Longtail_Free(api);
    }
    CacheBlockStore_CompleteRequest(cacheblockstore_api);
}

static int CacheBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    size_t put_stored_block_put_remote_complete_api_size = sizeof(struct PutStoredBlockPutRemoteComplete_API);
    struct PutStoredBlockPutRemoteComplete_API* put_stored_block_put_remote_complete_api = (struct PutStoredBlockPutRemoteComplete_API*)Longtail_Alloc(put_stored_block_put_remote_complete_api_size);
    if (!put_stored_block_put_remote_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            ENOMEM)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return ENOMEM;
    }
    put_stored_block_put_remote_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_remote_complete_api->m_API.OnComplete = PutStoredBlockPutRemoteComplete;
    put_stored_block_put_remote_complete_api->m_PendingCount = 2;
    put_stored_block_put_remote_complete_api->m_RemoteErr = EINVAL;
    put_stored_block_put_remote_complete_api->m_AsyncCompleteAPI = async_complete_api;
    put_stored_block_put_remote_complete_api->m_CacheBlockStoreAPI = cacheblockstore_api;
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
    int err = cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, stored_block, &put_stored_block_put_remote_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        Longtail_Free(put_stored_block_put_remote_complete_api);
        return err;
    }

    size_t put_stored_block_put_local_complete_api_size = sizeof(struct PutStoredBlockPutLocalComplete_API);
    struct PutStoredBlockPutLocalComplete_API* put_stored_block_put_local_complete_api = (struct PutStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_stored_block_put_local_complete_api_size);
    if (!put_stored_block_put_local_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            ENOMEM)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return ENOMEM;
    }
    put_stored_block_put_local_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_local_complete_api->m_API.OnComplete = PutStoredBlockPutLocalComplete;
    put_stored_block_put_local_complete_api->m_PutStoredBlockPutRemoteComplete_API = put_stored_block_put_remote_complete_api;
    put_stored_block_put_local_complete_api->m_CacheBlockStoreAPI = cacheblockstore_api;
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
    err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, stored_block, &put_stored_block_put_local_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        PutStoredBlockPutLocalComplete(&put_stored_block_put_local_complete_api->m_API, err);
        return 0;
    }
    return 0;
}

struct PreflightGetLocalStoreIndex
{
    struct Longtail_AsyncGetIndexAPI m_API;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    uint64_t m_PreflightBlockCount;
    TLongtail_Hash* m_PreflightBlockHashes;
    uint32_t* m_PreflightBlockRefCounts;
};

struct BlockLookup
{
    TLongtail_Hash key;
    uint64_t value;
};

void PreflightGetLocalStoreIndexOnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    struct PreflightGetLocalStoreIndex* api = (struct PreflightGetLocalStoreIndex*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PreflightGetLocalStoreIndexOnComplete(%p, %p) failed with %d",
            async_complete_api, content_index,
            err)
        Longtail_AtomicAdd64(&api->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        Longtail_Free(api);
        return;
    }

    size_t local_block_indexes_size = Longtail_LookupTable_GetSize(*content_index->m_BlockCount);
    size_t work_mem_size =
        local_block_indexes_size +
        sizeof(TLongtail_Hash) * api->m_PreflightBlockCount +
        sizeof(uint32_t) * api->m_PreflightBlockCount;
    char* work_mem = (char*)Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PreflightGetLocalStoreIndexOnComplete(%p, %p) failed with %d",
            async_complete_api, content_index,
            ENOMEM)
        Longtail_AtomicAdd64(&api->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        Longtail_Free(content_index);
        Longtail_Free(api);
        return;
    }

    struct Longtail_LookupTable* local_block_indexes = Longtail_LookupTable_Create(work_mem, *content_index->m_BlockCount, 0);
    for (uint64_t b = 0; b < *content_index->m_BlockCount; ++b)
    {
        Longtail_LookupTable_Put(local_block_indexes, content_index->m_BlockHashes[b], b);
    }
    TLongtail_Hash* remote_preflight_blocks = (TLongtail_Hash*)&work_mem[local_block_indexes_size];
    uint32_t* remote_preflight_blocks_ref_counts = (uint32_t*)&remote_preflight_blocks[api->m_PreflightBlockCount];
    uint64_t remote_preflight_block_count = 0;

    for (uint64_t b = 0; b < api->m_PreflightBlockCount; ++b)
    {
        TLongtail_Hash block_hash = api->m_PreflightBlockHashes[b];
        if (Longtail_LookupTable_Get(local_block_indexes, block_hash) == 0)
        {
            remote_preflight_blocks[remote_preflight_block_count] = block_hash;
            remote_preflight_blocks_ref_counts[remote_preflight_block_count] = api->m_PreflightBlockRefCounts[b];
            ++remote_preflight_block_count;
        }
    }

    err = api->m_CacheBlockStoreAPI->m_RemoteBlockStoreAPI->PreflightGet(
        api->m_CacheBlockStoreAPI->m_RemoteBlockStoreAPI,
        remote_preflight_block_count,
        remote_preflight_blocks,
        remote_preflight_blocks_ref_counts);

    Longtail_Free(work_mem);

    Longtail_Free(content_index);
    Longtail_Free(api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PreflightGetLocalStoreIndexOnComplete(%p, %p) failed with %d",
            async_complete_api, content_index,
            err)
        Longtail_AtomicAdd64(&api->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        return;
    }
}


static int CacheBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_PreflightGet(%p, %u %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_ref_counts != 0, return EINVAL)
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);
    
    int err = api->m_LocalBlockStoreAPI->PreflightGet(
        api->m_LocalBlockStoreAPI,
        block_count,
        block_hashes,
        block_ref_counts);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PreflightGet(%p, % " PRIu64 ", %p, %p) failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        return err;
    }

    size_t preflight_get_local_store_index_size =
        sizeof(struct PreflightGetLocalStoreIndex) +
        sizeof(TLongtail_Hash) * block_count +
        sizeof(uint32_t) * block_count;
    struct PreflightGetLocalStoreIndex* preflight_get_local_store_index = (struct PreflightGetLocalStoreIndex*)Longtail_Alloc(preflight_get_local_store_index_size);
    if (!preflight_get_local_store_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PreflightGet(%p, % " PRIu64 ", %p, %p) failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            ENOMEM)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        return ENOMEM;
    }

    preflight_get_local_store_index->m_API.OnComplete = PreflightGetLocalStoreIndexOnComplete;
    preflight_get_local_store_index->m_CacheBlockStoreAPI = api;
    preflight_get_local_store_index->m_PreflightBlockCount = block_count;
    preflight_get_local_store_index->m_PreflightBlockHashes = (TLongtail_Hash*)&preflight_get_local_store_index[1];
    preflight_get_local_store_index->m_PreflightBlockRefCounts = (uint32_t*)&preflight_get_local_store_index->m_PreflightBlockHashes[block_count];

    for (uint64_t b = 0; b < block_count; ++b)
    {
        preflight_get_local_store_index->m_PreflightBlockHashes[b] = block_hashes[b];
        preflight_get_local_store_index->m_PreflightBlockRefCounts[b] = block_ref_counts[b];
    }

    err = api->m_LocalBlockStoreAPI->GetIndex(api->m_LocalBlockStoreAPI, &preflight_get_local_store_index->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PreflightGet(%p, % " PRIu64 ", %p, %p) failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        Longtail_Free(preflight_get_local_store_index);
        return err;
    }
    return 0;
}

struct OnGetStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    struct Longtail_StoredBlock* m_StoredBlock;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
};

static void OnGetStoredBlockPutLocalComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockPutLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct OnGetStoredBlockPutLocalComplete_API* api = (struct OnGetStoredBlockPutLocalComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockPutLocalComplete: Failed store block in local block store, %d", err)
    }
    api->m_StoredBlock->Dispose(api->m_StoredBlock);
    Longtail_Free(api);
    CacheBlockStore_CompleteRequest(cacheblockstore_api);
}

struct OnGetStoredBlockGetRemoteComplete_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api;
};

static int StoreBlockCopyToLocalCache(struct CacheBlockStoreAPI* cacheblockstore_api, struct Longtail_BlockStoreAPI* local_block_store, struct Longtail_StoredBlock* cached_stored_block)
{
    size_t put_local_size = sizeof(struct OnGetStoredBlockPutLocalComplete_API);
    struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_local_size);
    if (!put_local)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) failed with %d",
            local_block_store, cached_stored_block,
            ENOMEM)
        return ENOMEM;
    }
    put_local->m_API.m_API.Dispose = 0;
    put_local->m_API.OnComplete = OnGetStoredBlockPutLocalComplete;
    put_local->m_StoredBlock = cached_stored_block;
    put_local->m_CacheBlockStoreAPI = cacheblockstore_api;

    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
    int err = local_block_store->PutStoredBlock(local_block_store, cached_stored_block, &put_local->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) local_block_store->PutStoredBlock(%p, %p, %p) failed with %d",
            local_block_store, cached_stored_block,
            local_block_store, cached_stored_block, &put_local->m_API,
            err)
        Longtail_Free(put_local);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return err;
    }
    return 0;
}

static void OnGetStoredBlockGetRemoteComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct OnGetStoredBlockGetRemoteComplete_API* api = (struct OnGetStoredBlockGetRemoteComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) failed with %d", async_complete_api, stored_block, err, err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return)

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    struct Longtail_StoredBlock* cached_stored_block = CachedStoredBlock_CreateBlock(stored_block, 2);
    if (!cached_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) CachedStoredBlock_CreateBlock(%p) failed with %d",
            async_complete_api, stored_block, err,
            stored_block,
            ENOMEM)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        stored_block->Dispose(stored_block);
        api->async_complete_api->OnComplete(api->async_complete_api, 0, ENOMEM);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }

    api->async_complete_api->OnComplete(api->async_complete_api, cached_stored_block, 0);

    int store_err = StoreBlockCopyToLocalCache(cacheblockstore_api, cacheblockstore_api->m_LocalBlockStoreAPI, cached_stored_block);
    if (store_err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) StoreBlockCopyToLocalCache(%p, %p) failed with %d",
            async_complete_api, stored_block, err,
            cacheblockstore_api->m_LocalBlockStoreAPI, cached_stored_block,
            store_err)
        cached_stored_block->Dispose(cached_stored_block);
    }
    Longtail_Free(api);
    CacheBlockStore_CompleteRequest(cacheblockstore_api);
}

struct OnGetStoredBlockGetLocalComplete_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    uint64_t block_hash;
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api;
};

static void OnGetStoredBlockGetLocalComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetLocalComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct OnGetStoredBlockGetLocalComplete_API* api = (struct OnGetStoredBlockGetLocalComplete_API*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(api->async_complete_api, return)
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err == ENOENT || err == EACCES)
    {
        size_t on_get_stored_block_get_remote_complete_size = sizeof(struct OnGetStoredBlockGetRemoteComplete_API);
        struct OnGetStoredBlockGetRemoteComplete_API* on_get_stored_block_get_remote_complete = (struct OnGetStoredBlockGetRemoteComplete_API*)Longtail_Alloc(on_get_stored_block_get_remote_complete_size);
        if (!on_get_stored_block_get_remote_complete)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) failed with %d",
                async_complete_api, stored_block, err,
                ENOMEM)
            api->async_complete_api->OnComplete(api->async_complete_api, 0, ENOMEM);
            return;
        }
        on_get_stored_block_get_remote_complete->m_API.m_API.Dispose = 0;
        on_get_stored_block_get_remote_complete->m_API.OnComplete = OnGetStoredBlockGetRemoteComplete;
        on_get_stored_block_get_remote_complete->m_CacheBlockStoreAPI = cacheblockstore_api;
        on_get_stored_block_get_remote_complete->async_complete_api = api->async_complete_api;
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
        err = cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(
            cacheblockstore_api->m_RemoteBlockStoreAPI,
            api->block_hash,
            &on_get_stored_block_get_remote_complete->m_API);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) failed with %d",
                async_complete_api, stored_block, err,
                err)
            Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free(on_get_stored_block_get_remote_complete);
            api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
            CacheBlockStore_CompleteRequest(cacheblockstore_api);
        }
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) failed with %d",
            async_complete_api, stored_block, err,
            err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return)
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);
    api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
    Longtail_Free(api);
    CacheBlockStore_CompleteRequest(cacheblockstore_api);
}

static int CacheBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    size_t on_get_stored_block_get_local_complete_api_size = sizeof(struct OnGetStoredBlockGetLocalComplete_API);
    struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc(on_get_stored_block_get_local_complete_api_size);
    if (!on_get_stored_block_get_local_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            ENOMEM)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        return ENOMEM;
    }
    on_get_stored_block_get_local_complete_api->m_API.m_API.Dispose = 0;
    on_get_stored_block_get_local_complete_api->m_API.OnComplete = OnGetStoredBlockGetLocalComplete;
    on_get_stored_block_get_local_complete_api->m_CacheBlockStoreAPI = cacheblockstore_api;
    on_get_stored_block_get_local_complete_api->block_hash = block_hash;
    on_get_stored_block_get_local_complete_api->async_complete_api = async_complete_api;
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
    int err = cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, &on_get_stored_block_get_local_complete_api->m_API);
    if (err)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        // We shortcut here since the logic to get from remote store is in OnComplete
        on_get_stored_block_get_local_complete_api->m_API.OnComplete(&on_get_stored_block_get_local_complete_api->m_API, 0, err);
    }
    return 0;
}

typedef void (*Gather_GetIndexPartialComplete)(void* context);

struct Gather_GetIndexCompleteAPI
{
    struct Longtail_AsyncGetIndexAPI m_API;
    void* m_Context;
    Gather_GetIndexPartialComplete m_CompleteCallback;
    struct Longtail_ContentIndex** m_ContentIndex;
    TLongtail_Atomic32* m_PendingCount;
    int* m_Err;
};

static void Gather_GetIndexCompleteAPI_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    struct Gather_GetIndexCompleteAPI* api = (struct Gather_GetIndexCompleteAPI*)async_complete_api;
    *api->m_Err = err;
    *api->m_ContentIndex = content_index;
    if (0 == Longtail_AtomicAdd32(api->m_PendingCount, -1))
    {
        api->m_CompleteCallback(api->m_Context);
    }
    Longtail_Free(api);
}


struct GetIndexContext
{
    struct Longtail_AsyncGetIndexAPI* m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    struct Longtail_ContentIndex* m_LocalIndex;
    struct Longtail_ContentIndex* m_RemoteIndex;
    TLongtail_Atomic32 m_PendingCount;
    int m_LocalErr;
    int m_RemoteErr;
};

static void GetIndex_GetIndexesCompleteAPI_OnComplete(void* context)
{
    struct GetIndexContext* retarget_context = (struct GetIndexContext*)context;
    int err = retarget_context->m_LocalErr ? retarget_context->m_LocalErr : retarget_context->m_RemoteErr;
    if (err)
    {
        Longtail_AtomicAdd64(&retarget_context->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1);
        Longtail_Free(retarget_context->m_LocalIndex);
        Longtail_Free(retarget_context->m_RemoteIndex);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* full_content_index = 0;
    err = Longtail_MergeContentIndex(retarget_context->m_JobAPI, retarget_context->m_RemoteIndex, retarget_context->m_LocalIndex, &full_content_index);
    Longtail_Free(retarget_context->m_LocalIndex);
    Longtail_Free(retarget_context->m_RemoteIndex);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "GetIndex_GetIndexesCompleteAPI_OnComplete(%p) failed with %d",
            context,
            err)
        Longtail_AtomicAdd64(&retarget_context->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, full_content_index, 0);
    Longtail_Free(retarget_context);
}

static int CacheBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetIndex(%p, %u, %p)", block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_Count], 1);

    size_t getindex_context_size = sizeof(struct GetIndexContext);
    struct GetIndexContext* getindex_context = (struct GetIndexContext*)Longtail_Alloc(getindex_context_size);
    if (!getindex_context)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            ENOMEM)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1);
        return ENOMEM;
    }

    size_t get_local_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_local_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_local_index_size);
    if (!get_local_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            ENOMEM)
        Longtail_Free(getindex_context);
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1);
        return ENOMEM;
    }

    size_t get_remote_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_remote_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_remote_index_size);
    if (!get_remote_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            ENOMEM)
        Longtail_Free(get_local_index);
        Longtail_Free(getindex_context);
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1);
        return ENOMEM;
    }

    getindex_context->m_AsyncCompleteAPI = async_complete_api;
    getindex_context->m_CacheBlockStoreAPI = cacheblockstore_api;
    getindex_context->m_JobAPI = cacheblockstore_api->m_JobAPI;
    getindex_context->m_LocalIndex = 0;
    getindex_context->m_RemoteIndex = 0;
    getindex_context->m_PendingCount = 2;
    getindex_context->m_LocalErr = EINVAL;
    getindex_context->m_RemoteErr = EINVAL;

    get_local_index->m_Context = getindex_context;
    get_local_index->m_CompleteCallback = GetIndex_GetIndexesCompleteAPI_OnComplete;
    get_local_index->m_ContentIndex = &getindex_context->m_LocalIndex;
    get_local_index->m_Err = &getindex_context->m_LocalErr;
    get_local_index->m_PendingCount = &getindex_context->m_PendingCount;

    get_remote_index->m_Context = getindex_context;
    get_remote_index->m_CompleteCallback = GetIndex_GetIndexesCompleteAPI_OnComplete;
    get_remote_index->m_ContentIndex = &getindex_context->m_RemoteIndex;
    get_remote_index->m_Err = &getindex_context->m_RemoteErr;
    get_remote_index->m_PendingCount = &getindex_context->m_PendingCount;

    struct Longtail_AsyncGetIndexAPI* get_local_async_api = Longtail_MakeAsyncGetIndexAPI(
        get_local_index,
        0,
        Gather_GetIndexCompleteAPI_OnComplete);
    LONGTAIL_FATAL_ASSERT(get_local_async_api != 0, return EINVAL)

    struct Longtail_AsyncGetIndexAPI* get_remote_async_api = Longtail_MakeAsyncGetIndexAPI(
        get_remote_index,
        0,
        Gather_GetIndexCompleteAPI_OnComplete);
    LONGTAIL_FATAL_ASSERT(get_remote_async_api != 0, return EINVAL)

    int err = Longtail_BlockStore_GetIndex(cacheblockstore_api->m_LocalBlockStoreAPI, get_local_async_api);
    if (err)
    {
        Gather_GetIndexCompleteAPI_OnComplete(get_local_async_api, 0, err);
    }

    err = Longtail_BlockStore_GetIndex(cacheblockstore_api->m_RemoteBlockStoreAPI, get_remote_async_api);
    if (err)
    {
        Gather_GetIndexCompleteAPI_OnComplete(get_remote_async_api, 0, err);
    }
    return 0;
}

struct RetargetContext
{
    struct Longtail_AsyncRetargetContentAPI* m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    struct Longtail_ContentIndex* m_ContentIndex;
    struct Longtail_ContentIndex* m_LocalIndex;
    struct Longtail_ContentIndex* m_RemoteIndex;
    TLongtail_Atomic32 m_PendingCount;
    int m_LocalErr;
    int m_RemoteErr;
};

static void RetargetContent_GetIndexesCompleteAPI_OnComplete(void* context)
{
    struct RetargetContext* retarget_context = (struct RetargetContext*)context;
    int err = retarget_context->m_LocalErr ? retarget_context->m_LocalErr : retarget_context->m_RemoteErr;
    if (err)
    {
        Longtail_AtomicAdd64(&retarget_context->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        Longtail_Free(retarget_context->m_LocalIndex);
        Longtail_Free(retarget_context->m_RemoteIndex);
        Longtail_Free(retarget_context->m_ContentIndex);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* full_content_index = 0;
    err = Longtail_MergeContentIndex(retarget_context->m_JobAPI, retarget_context->m_RemoteIndex, retarget_context->m_LocalIndex, &full_content_index);
    Longtail_Free(retarget_context->m_LocalIndex);
    Longtail_Free(retarget_context->m_RemoteIndex);
    if (err)
    {
        Longtail_AtomicAdd64(&retarget_context->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context->m_ContentIndex);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* retargeted_content_index = 0;
    err = Longtail_RetargetContent(full_content_index, retarget_context->m_ContentIndex, &retargeted_content_index);
    Longtail_Free(full_content_index);
    Longtail_Free(retarget_context->m_ContentIndex);
    if (err)
    {
        Longtail_AtomicAdd64(&retarget_context->m_CacheBlockStoreAPI->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, retargeted_content_index, 0);
    Longtail_Free(retarget_context);
}

static int CacheBlockStore_RetargetContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_RetargetContent(%p, %p, %p)",
        block_store_api, content_index, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_Count], 1);

    size_t retarget_context_size = sizeof(struct RetargetContext);
    struct RetargetContext* retarget_context = (struct RetargetContext*)Longtail_Alloc(retarget_context_size);
    if (!retarget_context_size)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        return ENOMEM;
    }

    size_t get_local_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_local_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_local_index_size);
    if (!get_local_index)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        Longtail_Free(retarget_context);
        return ENOMEM;
    }

    size_t get_remote_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_remote_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_remote_index_size);
    if (!get_remote_index)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        Longtail_Free(get_local_index);
        Longtail_Free(retarget_context);
        return ENOMEM;
    }
    void* tmp_buffer;
    size_t tmp_size;
    int err = Longtail_WriteContentIndexToBuffer(content_index, &tmp_buffer, &tmp_size);
    if (err)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        Longtail_Free(get_remote_index);
        Longtail_Free(get_local_index);
        Longtail_Free(retarget_context);
        return err;
    }
    struct Longtail_ContentIndex* content_index_copy = 0;
    err = Longtail_ReadContentIndexFromBuffer(tmp_buffer, tmp_size, &content_index_copy);
    Longtail_Free(tmp_buffer);
    if (err)
    {
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_RetargetContent_FailCount], 1);
        Longtail_Free(get_remote_index);
        Longtail_Free(get_local_index);
        Longtail_Free(retarget_context);
        return err;
    }

    retarget_context->m_ContentIndex = content_index_copy;
    retarget_context->m_CacheBlockStoreAPI = cacheblockstore_api;
    retarget_context->m_AsyncCompleteAPI = async_complete_api;
    retarget_context->m_JobAPI = cacheblockstore_api->m_JobAPI;
    retarget_context->m_LocalIndex = 0;
    retarget_context->m_RemoteIndex = 0;
    retarget_context->m_PendingCount = 2;
    retarget_context->m_LocalErr = EINVAL;
    retarget_context->m_RemoteErr = EINVAL;

    get_local_index->m_Context = retarget_context;
    get_local_index->m_CompleteCallback = RetargetContent_GetIndexesCompleteAPI_OnComplete;
    get_local_index->m_ContentIndex = &retarget_context->m_LocalIndex;
    get_local_index->m_Err = &retarget_context->m_LocalErr;
    get_local_index->m_PendingCount = &retarget_context->m_PendingCount;

    get_remote_index->m_Context = retarget_context;
    get_remote_index->m_CompleteCallback = RetargetContent_GetIndexesCompleteAPI_OnComplete;
    get_remote_index->m_ContentIndex = &retarget_context->m_RemoteIndex;
    get_remote_index->m_Err = &retarget_context->m_RemoteErr;
    get_remote_index->m_PendingCount = &retarget_context->m_PendingCount;

    struct Longtail_AsyncGetIndexAPI* get_local_async_api = Longtail_MakeAsyncGetIndexAPI(
        get_local_index,
        0,
        Gather_GetIndexCompleteAPI_OnComplete);
    LONGTAIL_FATAL_ASSERT(get_local_async_api != 0, return EINVAL)

    struct Longtail_AsyncGetIndexAPI* get_remote_async_api = Longtail_MakeAsyncGetIndexAPI(
        get_remote_index,
        0,
        Gather_GetIndexCompleteAPI_OnComplete);
    LONGTAIL_FATAL_ASSERT(get_remote_async_api != 0, return EINVAL)

    err = Longtail_BlockStore_GetIndex(cacheblockstore_api->m_LocalBlockStoreAPI, get_local_async_api);
    if (err)
    {
        Gather_GetIndexCompleteAPI_OnComplete(get_local_async_api, 0, err);
        Longtail_Free(retarget_context->m_ContentIndex);
        return 0;
    }

    err = Longtail_BlockStore_GetIndex(cacheblockstore_api->m_RemoteBlockStoreAPI, get_remote_async_api);
    if (err)
    {
        Gather_GetIndexCompleteAPI_OnComplete(get_remote_async_api, 0, err);
    }
    return 0;
}

static int CacheBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStats_Count], 1);
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        out_stats->m_StatU64[s] = cacheblockstore_api->m_StatU64[s];
    }
    return 0;
}

static int CacheBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Flush(%p, %p)", block_store_api, async_complete_api)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_LockSpinLock(cacheblockstore_api->m_Lock);
    if (cacheblockstore_api->m_PendingRequestCount > 0)
    {
        arrput(cacheblockstore_api->m_PendingAsyncFlushAPIs, async_complete_api);
        Longtail_UnlockSpinLock(cacheblockstore_api->m_Lock);
        return 0;
    }
    Longtail_UnlockSpinLock(cacheblockstore_api->m_Lock);
    async_complete_api->OnComplete(async_complete_api, 0);
    return 0;
}

static void CacheBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Dispose(%p)", api)
    LONGTAIL_VALIDATE_INPUT(api, return)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)api;
    while (cacheblockstore_api->m_PendingRequestCount > 0)
    {
        Longtail_Sleep(1000);
        if (cacheblockstore_api->m_PendingRequestCount > 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Dispose(%p) waiting for %d pending requests",
                api,
                (int32_t)cacheblockstore_api->m_PendingRequestCount);
        }
    }
    Longtail_DeleteSpinLock(cacheblockstore_api->m_Lock);
    Longtail_Free(cacheblockstore_api->m_Lock);
    Longtail_Free(cacheblockstore_api);
}

static int CacheBlockStore_Init(
    void* mem,
    struct Longtail_JobAPI* job_api,
    struct Longtail_BlockStoreAPI* local_block_store,
    struct Longtail_BlockStoreAPI* remote_block_store,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Dispose(%p, %p, %p, %p)",
        mem, local_block_store, remote_block_store, out_block_store_api)
    LONGTAIL_FATAL_ASSERT(mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(local_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        CacheBlockStore_Dispose,
        CacheBlockStore_PutStoredBlock,
        CacheBlockStore_PreflightGet,
        CacheBlockStore_GetStoredBlock,
        CacheBlockStore_GetIndex,
        CacheBlockStore_RetargetContent,
        CacheBlockStore_GetStats,
        CacheBlockStore_Flush);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    api->m_LocalBlockStoreAPI = local_block_store;
    api->m_RemoteBlockStoreAPI = remote_block_store;
    api->m_JobAPI = job_api;
    api->m_PendingRequestCount = 0;
    api->m_PendingAsyncFlushAPIs = 0;

    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        api->m_StatU64[s] = 0;
    }

    int err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        return err;
    }

    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_BlockStoreAPI* local_block_store,
    struct Longtail_BlockStoreAPI* remote_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateCacheBlockStoreAPI(%p, %p)", local_block_store, remote_block_store)
    LONGTAIL_VALIDATE_INPUT(local_block_store, return 0)
    LONGTAIL_VALIDATE_INPUT(remote_block_store, return 0)

    size_t api_size = sizeof(struct CacheBlockStoreAPI);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p) failed with %d",
            local_block_store, remote_block_store,
            ENOMEM)
        return 0;
    }
    struct Longtail_BlockStoreAPI* block_store_api;
    int err = CacheBlockStore_Init(
        mem,
        job_api,
        local_block_store,
        remote_block_store,
        &block_store_api);
    if (err)
    {
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
