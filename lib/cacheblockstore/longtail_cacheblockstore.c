#include "longtail_cacheblockstore.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct CacheBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_LocalBlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_RemoteBlockStoreAPI;

    TLongtail_Atomic32 m_PendingRequestCount;

    TLongtail_Atomic64 m_IndexGetCount;
    TLongtail_Atomic64 m_RetargetIndexCount;
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
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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
        Longtail_Free(api);
    }
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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

    Longtail_AtomicAdd64(&cacheblockstore_api->m_BlocksPutCount, 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_ChunksPutCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_BytesPutCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    size_t put_stored_block_put_remote_complete_api_size = sizeof(struct PutStoredBlockPutRemoteComplete_API);
    struct PutStoredBlockPutRemoteComplete_API* put_stored_block_put_remote_complete_api = (struct PutStoredBlockPutRemoteComplete_API*)Longtail_Alloc(put_stored_block_put_remote_complete_api_size);
    if (!put_stored_block_put_remote_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            ENOMEM)
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
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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
        PutStoredBlockPutLocalComplete(&put_stored_block_put_local_complete_api->m_API, err);
        Longtail_Free(put_stored_block_put_local_complete_api);
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
        Longtail_Free(api->m_PreflightBlockRefCounts);
        Longtail_Free(api->m_PreflightBlockHashes);
        Longtail_Free(api);
        return;
    }

    struct BlockLookup* local_block_indexes = 0;
    for (uint64_t b = 0; b < *content_index->m_BlockCount; ++b)
    {
        hmput(local_block_indexes, content_index->m_BlockHashes[b], b);
    }

    TLongtail_Hash* remote_preflight_blocks = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * api->m_PreflightBlockCount);
    uint32_t* remote_preflight_blocks_ref_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * api->m_PreflightBlockCount);
    uint64_t remote_preflight_block_count = 0;

    for (uint64_t b = 0; b < api->m_PreflightBlockCount; ++b)
    {
        if (hmgeti(local_block_indexes, api->m_PreflightBlockHashes[b]) == -1)
        {
            remote_preflight_blocks[remote_preflight_block_count] = api->m_PreflightBlockHashes[b];
            remote_preflight_blocks_ref_counts[remote_preflight_block_count] = api->m_PreflightBlockRefCounts[b];
            ++remote_preflight_block_count;
        }
    }
    hmfree(local_block_indexes);

    err = api->m_CacheBlockStoreAPI->m_RemoteBlockStoreAPI->PreflightGet(
        api->m_CacheBlockStoreAPI->m_RemoteBlockStoreAPI,
        remote_preflight_block_count,
        remote_preflight_blocks,
        remote_preflight_blocks_ref_counts);

    Longtail_Free(remote_preflight_blocks_ref_counts);
    Longtail_Free(remote_preflight_blocks);

    Longtail_Free(api->m_PreflightBlockRefCounts);
    Longtail_Free(api->m_PreflightBlockHashes);
    Longtail_Free(content_index);
    Longtail_Free(api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PreflightGetLocalStoreIndexOnComplete(%p, %p) failed with %d",
            async_complete_api, content_index,
            err)
    }
}


static int CacheBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_PreflightGet(%p, %u %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_ref_counts != 0, return EINVAL)
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;
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
        return err;
    }

    struct PreflightGetLocalStoreIndex* preflight_get_local_store_index = (struct PreflightGetLocalStoreIndex*)Longtail_Alloc(sizeof(struct PreflightGetLocalStoreIndex));

    preflight_get_local_store_index->m_API.OnComplete = PreflightGetLocalStoreIndexOnComplete;
    preflight_get_local_store_index->m_CacheBlockStoreAPI = api;
    preflight_get_local_store_index->m_PreflightBlockCount = block_count;
    preflight_get_local_store_index->m_PreflightBlockHashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * block_count);
    preflight_get_local_store_index->m_PreflightBlockRefCounts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * block_count);

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
        Longtail_Free(preflight_get_local_store_index->m_PreflightBlockHashes);
        Longtail_Free(preflight_get_local_store_index->m_PreflightBlockRefCounts);
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
    Longtail_Free(api->m_StoredBlock);
    Longtail_Free(api);
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
}

struct OnGetStoredBlockGetRemoteComplete_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api;
};

int CopyBlock(struct Longtail_StoredBlock* stored_block, struct Longtail_StoredBlock** out_stored_block_copy)
{
    size_t copy_size;
    void* copy_data;
    int err = Longtail_WriteStoredBlockToBuffer(stored_block, &copy_data, &copy_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "StoreBlockCopyToLocalCache: Longtail_WriteStoredBlockToBuffer() failed with %d", err)
        return err;
    }
    struct Longtail_StoredBlock* copy_stored_block;
    err = Longtail_ReadStoredBlockFromBuffer(copy_data, copy_size, &copy_stored_block);
    Longtail_Free(copy_data);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "StoreBlockCopyToLocalCache: Longtail_ReadStoredBlockFromBuffer() failed with %d", err)
        return err;
    }
    *out_stored_block_copy = copy_stored_block;
    return 0;
}

static int StoreBlockCopyToLocalCache(struct CacheBlockStoreAPI* cacheblockstore_api, struct Longtail_BlockStoreAPI* local_block_store, struct Longtail_StoredBlock* copy_stored_block)
{
    size_t put_local_size = sizeof(struct OnGetStoredBlockPutLocalComplete_API);
    struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_local_size);
    if (!put_local)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) failed with %d",
            local_block_store, copy_stored_block,
            ENOMEM)
        return ENOMEM;
    }
    put_local->m_API.m_API.Dispose = 0;
    put_local->m_API.OnComplete = OnGetStoredBlockPutLocalComplete;
    put_local->m_StoredBlock = copy_stored_block;
    put_local->m_CacheBlockStoreAPI = cacheblockstore_api;

    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, 1);
    int err = local_block_store->PutStoredBlock(local_block_store, copy_stored_block, &put_local->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) local_block_store->PutStoredBlock(%p, %p, %p) failed with %d",
            local_block_store, copy_stored_block,
            local_block_store, copy_stored_block, &put_local->m_API,
            err)
        Longtail_Free(put_local);
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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
        api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
        Longtail_Free(api);
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
        return;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return)

    Longtail_AtomicAdd64(&cacheblockstore_api->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    struct Longtail_StoredBlock* stored_block_copy = 0;
    int copy_err = CopyBlock(stored_block, &stored_block_copy);
    if (copy_err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) CopyBlock(%p, %p) failed with %d",
            async_complete_api, stored_block, err,
            stored_block, &stored_block_copy,
            copy_err)
    }

    api->async_complete_api->OnComplete(api->async_complete_api, stored_block, 0);

    if (!copy_err){
        copy_err = StoreBlockCopyToLocalCache(cacheblockstore_api, cacheblockstore_api->m_LocalBlockStoreAPI, stored_block_copy);
        if (copy_err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) StoreBlockCopyToLocalCache(%p, %p) failed with %d",
                async_complete_api, stored_block, err,
                cacheblockstore_api->m_LocalBlockStoreAPI, stored_block_copy,
                copy_err)
            stored_block_copy->Dispose(stored_block_copy);
        }
    }
    Longtail_Free(api);
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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
            Longtail_Free(on_get_stored_block_get_remote_complete);
            api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
            Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
        }
        Longtail_Free(api);
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
        return;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) failed with %d",
            async_complete_api, stored_block, err,
            err)
        api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
        Longtail_Free(api);
        Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
        return;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return)
    Longtail_AtomicAdd64(&cacheblockstore_api->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);
    api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
    Longtail_Free(api);
    Longtail_AtomicAdd32(&cacheblockstore_api->m_PendingRequestCount, -1);
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
    size_t on_get_stored_block_get_local_complete_api_size = sizeof(struct OnGetStoredBlockGetLocalComplete_API);
    struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc(on_get_stored_block_get_local_complete_api_size);
    if (!on_get_stored_block_get_local_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            ENOMEM)
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
        Longtail_Free(retarget_context->m_LocalIndex);
        Longtail_Free(retarget_context->m_RemoteIndex);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* full_content_index = 0;
    err = Longtail_MergeContentIndex(retarget_context->m_RemoteIndex, retarget_context->m_LocalIndex, &full_content_index);
    Longtail_Free(retarget_context->m_LocalIndex);
    Longtail_Free(retarget_context->m_RemoteIndex);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "GetIndex_GetIndexesCompleteAPI_OnComplete(%p) failed with %d",
            context,
            err)
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
    Longtail_AtomicAdd64(&cacheblockstore_api->m_IndexGetCount, 1);

    size_t getindex_context_size = sizeof(struct GetIndexContext);
    struct GetIndexContext* getindex_context = (struct GetIndexContext*)Longtail_Alloc(getindex_context_size);
    if (!getindex_context)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetIndex(%p, %u, %p) failed with %d",
            block_store_api, async_complete_api,
            ENOMEM)
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
        return ENOMEM;
    }

    getindex_context->m_AsyncCompleteAPI = async_complete_api;
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
    struct Longtail_ContentIndex* m_ContentIndex;
    struct Longtail_AsyncRetargetContentAPI* m_AsyncCompleteAPI;
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
        Longtail_Free(retarget_context->m_LocalIndex);
        Longtail_Free(retarget_context->m_RemoteIndex);
        Longtail_Free(retarget_context->m_ContentIndex);
        retarget_context->m_AsyncCompleteAPI->OnComplete(retarget_context->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* full_content_index = 0;
    err = Longtail_MergeContentIndex(retarget_context->m_RemoteIndex, retarget_context->m_LocalIndex, &full_content_index);
    Longtail_Free(retarget_context->m_LocalIndex);
    Longtail_Free(retarget_context->m_RemoteIndex);
    if (err)
    {
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
    Longtail_AtomicAdd64(&cacheblockstore_api->m_RetargetIndexCount, 1);

    size_t retarget_context_size = sizeof(struct RetargetContext);
    struct RetargetContext* retarget_context = (struct RetargetContext*)Longtail_Alloc(retarget_context_size);
    if (!retarget_context_size)
    {
        return ENOMEM;
    }

    size_t get_local_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_local_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_local_index_size);
    if (!get_local_index)
    {
        Longtail_Free(retarget_context);
        return ENOMEM;
    }

    size_t get_remote_index_size = sizeof(struct Gather_GetIndexCompleteAPI);
    struct Gather_GetIndexCompleteAPI* get_remote_index = (struct Gather_GetIndexCompleteAPI*)Longtail_Alloc(get_remote_index_size);
    if (!get_remote_index)
    {
        Longtail_Free(get_local_index);
        Longtail_Free(retarget_context);
        return ENOMEM;
    }
    void* tmp_buffer;
    size_t tmp_size;
    int err = Longtail_WriteContentIndexToBuffer(content_index, &tmp_buffer, &tmp_size);
    if (err)
    {
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
        Longtail_Free(get_remote_index);
        Longtail_Free(get_local_index);
        Longtail_Free(retarget_context);
        return err;
    }

    retarget_context->m_ContentIndex = content_index_copy;
    retarget_context->m_AsyncCompleteAPI = async_complete_api;
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
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    out_stats->m_IndexGetCount = cacheblockstore_api->m_IndexGetCount;
//    out_stats->m_RetargetIndexCount = cacheblockstore_api->m_RetargetIndexCount;
    out_stats->m_BlocksGetCount = cacheblockstore_api->m_BlocksGetCount;
    out_stats->m_BlocksPutCount = cacheblockstore_api->m_BlocksPutCount;
    out_stats->m_ChunksGetCount = cacheblockstore_api->m_ChunksGetCount;
    out_stats->m_ChunksPutCount = cacheblockstore_api->m_ChunksPutCount;
    out_stats->m_BytesGetCount = cacheblockstore_api->m_BytesGetCount;
    out_stats->m_BytesPutCount = cacheblockstore_api->m_BytesPutCount;
    out_stats->m_IndexGetRetryCount = cacheblockstore_api->m_IndexGetRetryCount;
    out_stats->m_BlockGetRetryCount = cacheblockstore_api->m_BlockGetRetryCount;
    out_stats->m_BlockPutRetryCount = cacheblockstore_api->m_BlockPutRetryCount;
    out_stats->m_IndexGetFailCount = cacheblockstore_api->m_IndexGetFailCount;
    out_stats->m_BlockGetFailCount = cacheblockstore_api->m_BlockGetFailCount;
    out_stats->m_BlockPutFailCount = cacheblockstore_api->m_BlockPutFailCount;
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
    Longtail_Free(cacheblockstore_api);
}

static int CacheBlockStore_Init(
    void* mem,
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
        CacheBlockStore_GetStats);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    api->m_LocalBlockStoreAPI = local_block_store;
    api->m_RemoteBlockStoreAPI = remote_block_store;
    api->m_PendingRequestCount = 0;

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

    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
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
