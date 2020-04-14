#include "longtail_cacheblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct CacheBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_LocalBlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_RemoteBlockStoreAPI;

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

struct PutStoredBlockPutRemoteComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    TLongtail_Atomic32 m_PendingCount;
    int m_RemoteErr;
    struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
};

struct PutStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    struct PutStoredBlockPutRemoteComplete_API* m_PutStoredBlockPutRemoteComplete_API;
};

static int PutStoredBlockPutLocalComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "PutStoredBlockPutLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct PutStoredBlockPutLocalComplete_API* api = (struct PutStoredBlockPutLocalComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "PutStoredBlockPutLocalComplete: Failed store block in local block store, %d", err)
    }
    struct PutStoredBlockPutRemoteComplete_API* remote_put_api = api->m_PutStoredBlockPutRemoteComplete_API;
    Longtail_Free(api);
    int remain = Longtail_AtomicAdd32(&remote_put_api->m_PendingCount, -1);
    if (remain == 0)
    {
        remote_put_api->m_AsyncCompleteAPI->OnComplete(remote_put_api->m_AsyncCompleteAPI, remote_put_api->m_RemoteErr);
        Longtail_Free(remote_put_api);
        return 0;
    }
    return 0;
}

int PutStoredBlockPutRemoteComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "PutStoredBlockPutRemoteComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct PutStoredBlockPutRemoteComplete_API* api = (struct PutStoredBlockPutRemoteComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "PutStoredBlockPutRemoteComplete(%p, %d): Failed store block in remote block store, %d", async_complete_api, err, err)
    }
    api->m_RemoteErr = err;
    int remain = Longtail_AtomicAdd32(&api->m_PendingCount, -1);
    if (remain == 0)
    {
        api->m_AsyncCompleteAPI->OnComplete(api->m_AsyncCompleteAPI, api->m_RemoteErr);
        Longtail_Free(api);
        return 0;
    }
    return 0;
}

static int CacheBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            block_store_api, stored_block, async_complete_api,
            put_stored_block_put_remote_complete_api_size,
            ENOMEM)
        return ENOMEM;
    }
    put_stored_block_put_remote_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_remote_complete_api->m_API.OnComplete = PutStoredBlockPutRemoteComplete;
    put_stored_block_put_remote_complete_api->m_PendingCount = 2;
    put_stored_block_put_remote_complete_api->m_RemoteErr = EINVAL;
    put_stored_block_put_remote_complete_api->m_AsyncCompleteAPI = async_complete_api;
    int err = cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, stored_block, &put_stored_block_put_remote_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock(%p, %p, %p) failed with, %d",
            block_store_api, stored_block, async_complete_api,
            cacheblockstore_api->m_RemoteBlockStoreAPI, stored_block, &put_stored_block_put_remote_complete_api->m_API,
            err)
        Longtail_Free(put_stored_block_put_remote_complete_api);
        return err;
    }

    size_t put_stored_block_put_local_complete_api_size = sizeof(struct PutStoredBlockPutLocalComplete_API);
    struct PutStoredBlockPutLocalComplete_API* put_stored_block_put_local_complete_api = (struct PutStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_stored_block_put_local_complete_api_size);
    if (!put_stored_block_put_local_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            block_store_api, stored_block, async_complete_api,
            put_stored_block_put_local_complete_api_size,
            ENOMEM)
        return ENOMEM;
    }
    put_stored_block_put_local_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_local_complete_api->m_API.OnComplete = PutStoredBlockPutLocalComplete;
    put_stored_block_put_local_complete_api->m_PutStoredBlockPutRemoteComplete_API = put_stored_block_put_remote_complete_api;
    err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, stored_block, &put_stored_block_put_local_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PutStoredBlock(%p, %p, %p) cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            cacheblockstore_api->m_LocalBlockStoreAPI, stored_block, &put_stored_block_put_local_complete_api->m_API,
            err)
        PutStoredBlockPutLocalComplete(&put_stored_block_put_local_complete_api->m_API, err);
        Longtail_Free(put_stored_block_put_local_complete_api);
    }
    return 0;
}

static int CacheBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    return 0;
}

struct OnGetStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    struct Longtail_StoredBlock* m_StoredBlock;
};

static int OnGetStoredBlockPutLocalComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockPutLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockPutLocalComplete_API* api = (struct OnGetStoredBlockPutLocalComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockPutLocalComplete: Failed store block in local block store, %d", err)
    }
    Longtail_Free(api->m_StoredBlock);
    Longtail_Free(api);
    return 0;
}

struct OnGetStoredBlockGetRemoteComplete_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CacheBlockStoreAPI* cacheblockstore_api;
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

static int StoreBlockCopyToLocalCache(struct Longtail_BlockStoreAPI* local_block_store, struct Longtail_StoredBlock* copy_stored_block)
{
    size_t put_local_size = sizeof(struct OnGetStoredBlockPutLocalComplete_API);
    struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_local_size);
    if (!put_local)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_block_store, copy_stored_block,
            put_local_size,
            ENOMEM)
        return ENOMEM;
    }
    put_local->m_API.m_API.Dispose = 0;
    put_local->m_API.OnComplete = OnGetStoredBlockPutLocalComplete;
    put_local->m_StoredBlock = copy_stored_block;

    int err = local_block_store->PutStoredBlock(local_block_store, copy_stored_block, &put_local->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StoreBlockCopyToLocalCache(%p, %p) local_block_store->PutStoredBlock(%p, %p, %p) failed with %d",
            local_block_store, copy_stored_block,
            local_block_store, copy_stored_block, &put_local->m_API,
            err)
        Longtail_Free(copy_stored_block);
        Longtail_Free(put_local);
        return err;
    }
    return 0;
}

static int OnGetStoredBlockGetRemoteComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockGetRemoteComplete_API* api = (struct OnGetStoredBlockGetRemoteComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) failed with, %d", async_complete_api, stored_block, err, err)
        api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
        Longtail_Free(api);
        return 0;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)

    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    struct Longtail_StoredBlock* stored_block_copy = 0;
    int copy_err = CopyBlock(stored_block, &stored_block_copy);
    if (copy_err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) CopyBlock(%p, %p) failed with, %d",
            async_complete_api, stored_block, err,
            stored_block, &stored_block_copy,
            copy_err)
    }

    err = api->async_complete_api->OnComplete(api->async_complete_api, stored_block, 0);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) api->async_complete_api->OnComplete(%p, %p, %p) failed with, %d",
            async_complete_api, stored_block, err,
            api->async_complete_api, stored_block, 0,
            err)
        stored_block->Dispose(stored_block_copy);
        stored_block->Dispose(stored_block);
        Longtail_Free(api);
        return err;
    }

    if (!copy_err){
        copy_err = StoreBlockCopyToLocalCache(api->cacheblockstore_api->m_LocalBlockStoreAPI, stored_block_copy);
        if (copy_err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete(%p, %p, %d) StoreBlockCopyToLocalCache(%p, %p) failed with, %d",
                async_complete_api, stored_block, err,
                api->cacheblockstore_api->m_LocalBlockStoreAPI, stored_block_copy,
                err)
            stored_block->Dispose(stored_block_copy);
        }
    }
    Longtail_Free(api);
    return 0;
}

struct OnGetStoredBlockGetLocalComplete_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CacheBlockStoreAPI* cacheblockstore_api;
    uint64_t block_hash;
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api;
};

static int OnGetStoredBlockGetLocalComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetLocalComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockGetLocalComplete_API* api = (struct OnGetStoredBlockGetLocalComplete_API*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(api->async_complete_api, return EINVAL)
    if (err == ENOENT || err == EACCES)
    {
        size_t on_get_stored_block_get_remote_complete_size = sizeof(struct OnGetStoredBlockGetRemoteComplete_API);
        struct OnGetStoredBlockGetRemoteComplete_API* on_get_stored_block_get_remote_complete = (struct OnGetStoredBlockGetRemoteComplete_API*)Longtail_Alloc(on_get_stored_block_get_remote_complete_size);
        if (!on_get_stored_block_get_remote_complete)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) Longtail_Alloc(%" PRIu64 ") failed with %d",
                async_complete_api, stored_block, err,
                on_get_stored_block_get_remote_complete_size,
                ENOMEM)
            return ENOMEM;
        }
        on_get_stored_block_get_remote_complete->m_API.m_API.Dispose = 0;
        on_get_stored_block_get_remote_complete->m_API.OnComplete = OnGetStoredBlockGetRemoteComplete;
        on_get_stored_block_get_remote_complete->cacheblockstore_api = api->cacheblockstore_api;
        on_get_stored_block_get_remote_complete->async_complete_api = api->async_complete_api;
        err = api->cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(
            api->cacheblockstore_api->m_RemoteBlockStoreAPI,
            api->block_hash,
            &on_get_stored_block_get_remote_complete->m_API);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) api->cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
                async_complete_api, stored_block, err,
                api->cacheblockstore_api->m_RemoteBlockStoreAPI, api->block_hash, &on_get_stored_block_get_remote_complete->m_API,
                err)
            Longtail_Free(on_get_stored_block_get_remote_complete);
            api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
        }
        Longtail_Free(api);
        return 0;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete(%p, %p, %d) failed with %d",
            async_complete_api, stored_block, err,
            err)
        err = api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
        Longtail_Free(api);
        return err;
    }
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->cacheblockstore_api->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);
    err = api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
    Longtail_Free(api);
    return err;
}

static int CacheBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    size_t on_get_stored_block_get_local_complete_api_size = sizeof(struct OnGetStoredBlockGetLocalComplete_API);
    struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc(on_get_stored_block_get_local_complete_api_size);
    if (!on_get_stored_block_get_local_complete_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, block_hash, async_complete_api,
            on_get_stored_block_get_local_complete_api_size,
            ENOMEM)
        return ENOMEM;
    }
    on_get_stored_block_get_local_complete_api->m_API.m_API.Dispose = 0;
    on_get_stored_block_get_local_complete_api->m_API.OnComplete = OnGetStoredBlockGetLocalComplete;
    on_get_stored_block_get_local_complete_api->cacheblockstore_api = cacheblockstore_api;
    on_get_stored_block_get_local_complete_api->block_hash = block_hash;
    on_get_stored_block_get_local_complete_api->async_complete_api = async_complete_api;
    int err = cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, &on_get_stored_block_get_local_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock: Failed async get block in local block store, %d", err)
        Longtail_Free(on_get_stored_block_get_local_complete_api);
        return err;
    }
    return 0;
}

static int CacheBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_GetIndex(%p, %u, %p)", block_store_api, default_hash_api_identifier, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&cacheblockstore_api->m_IndexGetCount, 1);
    return cacheblockstore_api->m_RemoteBlockStoreAPI->GetIndex(cacheblockstore_api->m_RemoteBlockStoreAPI, default_hash_api_identifier, async_complete_api);
}

static int CacheBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    out_stats->m_IndexGetCount = cacheblockstore_api->m_IndexGetCount;
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)api;
    Longtail_Free(cacheblockstore_api);
}

static int CacheBlockStore_Init(
    struct CacheBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* local_block_store,
    struct Longtail_BlockStoreAPI* remote_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CacheBlockStore_Dispose(%p, %p, %p)", api, local_block_store, remote_block_store)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(local_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = CacheBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = CacheBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = CacheBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = CacheBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = CacheBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = CacheBlockStore_GetStats;
    api->m_LocalBlockStoreAPI = local_block_store;
    api->m_RemoteBlockStoreAPI = remote_block_store;

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

    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
    struct Longtail_BlockStoreAPI* local_block_store,
    struct Longtail_BlockStoreAPI* remote_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateCacheBlockStoreAPI(%p, %p)", local_block_store, remote_block_store)
    LONGTAIL_FATAL_ASSERT(local_block_store, return 0)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return 0)

    size_t api_size = sizeof(struct CacheBlockStoreAPI);
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock(%p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            local_block_store, remote_block_store,
            api_size,
            ENOMEM)
        return 0;
    }
    CacheBlockStore_Init(
        api,
        local_block_store,
        remote_block_store);
    return &api->m_BlockStoreAPI;
}
