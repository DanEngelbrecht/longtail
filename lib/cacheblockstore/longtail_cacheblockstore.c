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

    TLongtail_Atomic64 m_StatU64[Longtail_BlockStoreAPI_StatU64_Count];

    HLongtail_SpinLock m_Lock;
    struct Longtail_AsyncFlushAPI** m_PendingAsyncFlushAPIs;

    TLongtail_Atomic32 m_PendingRequestCount;
};

static void CacheBlockStore_CompleteRequest(struct CacheBlockStoreAPI* cacheblockstore_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cacheblockstore_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, cacheblockstore_api->m_PendingRequestCount > 0, return)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(original_stored_block, "%p"),
        LONGTAIL_LOGFIELD(refCount, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    size_t cached_stored_block_size = sizeof(struct CachedStoredBlock);
    struct CachedStoredBlock* cached_stored_block = (struct CachedStoredBlock*)Longtail_Alloc(cached_stored_block_size);
    if (!cached_stored_block)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct PutStoredBlockPutLocalComplete_API* api = (struct PutStoredBlockPutLocalComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "PutStoredBlockPutLocalComplete called with with error %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct PutStoredBlockPutRemoteComplete_API* api = (struct PutStoredBlockPutRemoteComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "PutStoredBlockPutRemoteComplete called with error %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    size_t put_stored_block_put_remote_complete_api_size = sizeof(struct PutStoredBlockPutRemoteComplete_API);
    struct PutStoredBlockPutRemoteComplete_API* put_stored_block_put_remote_complete_api = (struct PutStoredBlockPutRemoteComplete_API*)Longtail_Alloc(put_stored_block_put_remote_complete_api_size);
    if (!put_stored_block_put_remote_complete_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock() failed with %d", err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        Longtail_Free(put_stored_block_put_remote_complete_api);
        return err;
    }

    size_t put_stored_block_put_local_complete_api_size = sizeof(struct PutStoredBlockPutLocalComplete_API);
    struct PutStoredBlockPutLocalComplete_API* put_stored_block_put_local_complete_api = (struct PutStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_stored_block_put_local_complete_api_size);
    if (!put_stored_block_put_local_complete_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock() failed with %d", err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        PutStoredBlockPutLocalComplete(&put_stored_block_put_local_complete_api->m_API, err);
        return 0;
    }
    return 0;
}

struct PreflightRetargetContext
{
    struct Longtail_AsyncGetExistingContentAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    uint64_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
};

static void PreflightGet_GetExistingContentCompleteAPI_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct PreflightRetargetContext* retarget_context = (struct PreflightRetargetContext*)async_complete_api;
    struct CacheBlockStoreAPI* api = retarget_context->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "PreflightGet_GetExistingContentCompleteAPI_OnComplete called with error %d", err)
        Longtail_Free(retarget_context);
        return;
    }
    if (*content_index->m_ChunkCount > 0)
    {
        err = api->m_LocalBlockStoreAPI->PreflightGet(api->m_LocalBlockStoreAPI, *content_index->m_ChunkCount, content_index->m_ChunkHashes);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_LocalBlockStoreAPI->PreflightGet() failed with %d", err)
            Longtail_Free(content_index);
            Longtail_Free(retarget_context);
            return;
        }
    }

    uint64_t missing_chunk_count = 0;
    TLongtail_Hash* missing_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * retarget_context->m_ChunkCount);
    if (!missing_chunk_hashes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(content_index);
        Longtail_Free(retarget_context);
        return;
    }
    err = Longtail_GetMissingChunks(
        content_index,
        retarget_context->m_ChunkCount,
        retarget_context->m_ChunkHashes,
        &missing_chunk_count,
        missing_chunk_hashes);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_GetMissingChunks() failed with %d", err)
        Longtail_Free(missing_chunk_hashes);
        Longtail_Free(content_index);
        Longtail_Free(retarget_context);
        return;
    }
    if (missing_chunk_count > 0)
    {
        err = api->m_RemoteBlockStoreAPI->PreflightGet(api->m_RemoteBlockStoreAPI, missing_chunk_count, missing_chunk_hashes);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_RemoteBlockStoreAPI->PreflightGet() failed with %d", err)
            Longtail_Free(missing_chunk_hashes);
            Longtail_Free(content_index);
            Longtail_Free(retarget_context);
            return;
        }
    }
    Longtail_Free(missing_chunk_hashes);
    Longtail_Free(content_index);
    Longtail_Free(retarget_context);
}

static int CacheBlockStore_PreflightGet(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);

    struct PreflightRetargetContext* context = (struct PreflightRetargetContext*)Longtail_Alloc(sizeof(struct PreflightRetargetContext) + sizeof(TLongtail_Hash) * chunk_count);
    context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    context->m_AsyncCompleteAPI.OnComplete = PreflightGet_GetExistingContentCompleteAPI_OnComplete;
    context->m_CacheBlockStoreAPI = api;
    context->m_ChunkCount = chunk_count;
    context->m_ChunkHashes = (TLongtail_Hash*)&context[1];
    memcpy(context->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);

    int err = api->m_LocalBlockStoreAPI->GetExistingContent(
        api->m_LocalBlockStoreAPI,
        chunk_count,
        chunk_hashes,
        0,
        &context->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_LocalBlockStoreAPI->GetExistingContent() failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        Longtail_Free(context);
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct OnGetStoredBlockPutLocalComplete_API* api = (struct OnGetStoredBlockPutLocalComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockPutLocalComplete called with error", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cacheblockstore_api, "%p"),
        LONGTAIL_LOGFIELD(local_block_store, "%p"),
        LONGTAIL_LOGFIELD(cached_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    size_t put_local_size = sizeof(struct OnGetStoredBlockPutLocalComplete_API);
    struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc(put_local_size);
    if (!put_local)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "local_block_store->PutStoredBlock() failed with %d", err)
        Longtail_Free(put_local);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return err;
    }
    return 0;
}

static void OnGetStoredBlockGetRemoteComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct OnGetStoredBlockGetRemoteComplete_API* api = (struct OnGetStoredBlockGetRemoteComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete called with error %d", err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        api->async_complete_api->OnComplete(api->async_complete_api, stored_block, err);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }
    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return)

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    struct Longtail_StoredBlock* cached_stored_block = CachedStoredBlock_CreateBlock(stored_block, 2);
    if (!cached_stored_block)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "CachedStoredBlock_CreateBlock() failed with %d", ENOMEM)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "StoreBlockCopyToLocalCache() StoreBlockCopyToLocalCache() failed with %d", store_err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct OnGetStoredBlockGetLocalComplete_API* api = (struct OnGetStoredBlockGetLocalComplete_API*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, api->async_complete_api, return)
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err == ENOENT || err == EACCES)
    {
        size_t on_get_stored_block_get_remote_complete_size = sizeof(struct OnGetStoredBlockGetRemoteComplete_API);
        struct OnGetStoredBlockGetRemoteComplete_API* on_get_stored_block_get_remote_complete = (struct OnGetStoredBlockGetRemoteComplete_API*)Longtail_Alloc(on_get_stored_block_get_remote_complete_size);
        if (!on_get_stored_block_get_remote_complete)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock() failed with %d", err)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete called with error %d", err)
        Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        api->async_complete_api->OnComplete(api->async_complete_api, 0, err);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }
    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    size_t on_get_stored_block_get_local_complete_api_size = sizeof(struct OnGetStoredBlockGetLocalComplete_API);
    struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc(on_get_stored_block_get_local_complete_api_size);
    if (!on_get_stored_block_get_local_complete_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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

struct RetargetContext_RetargetToRemote_Context
{
    struct Longtail_AsyncGetExistingContentAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetExistingContentAPI* m_RetargetAsyncCompleteAPI;
    struct Longtail_ContentIndex* m_LocalRetargettedContentIndex;
    uint64_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
};

static void RetargetRemoteContent_GetExistingContentCompleteAPI_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct RetargetContext_RetargetToRemote_Context* retarget_context = (struct RetargetContext_RetargetToRemote_Context*)async_complete_api;
    struct CacheBlockStoreAPI* api = retarget_context->m_CacheBlockStoreAPI;
    if (err)
    {
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct Longtail_ContentIndex* merged_content_index;
    err = Longtail_AddContentIndex(
        retarget_context->m_LocalRetargettedContentIndex,
        content_index,
        &merged_content_index);
    Longtail_Free(content_index);
    Longtail_Free(retarget_context->m_LocalRetargettedContentIndex);
    if (err)
    {
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, merged_content_index, 0);
    Longtail_Free(retarget_context);
}

struct RetargetContext_RetargetToLocal_Context
{
    struct Longtail_AsyncGetExistingContentAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetExistingContentAPI* m_RetargetAsyncCompleteAPI;
    uint64_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t m_MinBlockUsagePercent;
};

static void RetargetLocalContent_GetExistingContentCompleteAPI_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct RetargetContext_RetargetToLocal_Context* retarget_context = (struct RetargetContext_RetargetToLocal_Context*)async_complete_api;
    struct CacheBlockStoreAPI* api = retarget_context->m_CacheBlockStoreAPI;
    if (err)
    {
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    struct RetargetContext_RetargetToRemote_Context* retarget_remote_context = (struct RetargetContext_RetargetToRemote_Context*)Longtail_Alloc(sizeof(struct RetargetContext_RetargetToRemote_Context) + (sizeof(TLongtail_Hash) * retarget_context->m_ChunkCount));
    retarget_remote_context->m_ChunkHashes = (TLongtail_Hash*)&retarget_remote_context[1];
    err = Longtail_GetMissingChunks(
        content_index,
        retarget_context->m_ChunkCount,
        retarget_context->m_ChunkHashes,
        &retarget_remote_context->m_ChunkCount,
        retarget_remote_context->m_ChunkHashes);
    if (err)
    {
        Longtail_Free(content_index);
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_context);
        return;
    }
    if (retarget_remote_context->m_ChunkCount == 0)
    {
        Longtail_Free(retarget_remote_context);
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, content_index, 0);
        Longtail_Free(retarget_context);
        return;
    }
    retarget_remote_context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    retarget_remote_context->m_AsyncCompleteAPI.OnComplete = RetargetRemoteContent_GetExistingContentCompleteAPI_OnComplete;
    retarget_remote_context->m_CacheBlockStoreAPI = api;
    retarget_remote_context->m_LocalRetargettedContentIndex = content_index;
    retarget_remote_context->m_RetargetAsyncCompleteAPI = retarget_context->m_RetargetAsyncCompleteAPI;
    err = api->m_RemoteBlockStoreAPI->GetExistingContent(
        api->m_RemoteBlockStoreAPI,
        retarget_remote_context->m_ChunkCount,
        retarget_remote_context->m_ChunkHashes,
        retarget_context->m_MinBlockUsagePercent,
        &retarget_remote_context->m_AsyncCompleteAPI);
    if (err)
    {
        retarget_context->m_RetargetAsyncCompleteAPI->OnComplete(retarget_context->m_RetargetAsyncCompleteAPI, 0, err);
        Longtail_Free(retarget_remote_context);
        Longtail_Free(content_index);
        Longtail_Free(retarget_context);
        return;
    }
    Longtail_Free(retarget_context);
}

static int CacheBlockStore_GetExistingContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint32_t min_block_usage_percent,
    struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count], 1);

    size_t async_complete_size = sizeof(struct RetargetContext_RetargetToLocal_Context) + sizeof(TLongtail_Hash) * chunk_count;
    struct RetargetContext_RetargetToLocal_Context* retarget_local_context = (struct RetargetContext_RetargetToLocal_Context*)Longtail_Alloc(async_complete_size);
    retarget_local_context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    retarget_local_context->m_AsyncCompleteAPI.OnComplete = RetargetLocalContent_GetExistingContentCompleteAPI_OnComplete;
    retarget_local_context->m_CacheBlockStoreAPI = api;
    retarget_local_context->m_RetargetAsyncCompleteAPI = async_complete_api;
    retarget_local_context->m_ChunkCount = chunk_count;
    retarget_local_context->m_ChunkHashes = (TLongtail_Hash*)&retarget_local_context[1];
    retarget_local_context->m_MinBlockUsagePercent = min_block_usage_percent;
    memcpy(retarget_local_context->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);

    int err = api->m_LocalBlockStoreAPI->GetExistingContent(
        api->m_LocalBlockStoreAPI,
        chunk_count,
        chunk_hashes,
        min_block_usage_percent,
        &retarget_local_context->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_LocalBlockStoreAPI->GetExistingContent() failed with %d", err)
        Longtail_Free(retarget_local_context);
        return err;
    }
    return 0;
}

static int CacheBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(out_stats, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)LONGTAIL_VALIDATE_INPUT(ctx, out_stats, return EINVAL)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_Count], 1);
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, api, return)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)api;
    while (cacheblockstore_api->m_PendingRequestCount > 0)
    {
        Longtail_Sleep(1000);
        if (cacheblockstore_api->m_PendingRequestCount > 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Waiting for %d pending requests",
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(local_block_store, "%p"),
        LONGTAIL_LOGFIELD(remote_block_store, "%p"),
        LONGTAIL_LOGFIELD(out_block_store_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, local_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, remote_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        CacheBlockStore_Dispose,
        CacheBlockStore_PutStoredBlock,
        CacheBlockStore_PreflightGet,
        CacheBlockStore_GetStoredBlock,
        CacheBlockStore_GetExistingContent,
        CacheBlockStore_GetStats,
        CacheBlockStore_Flush);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    api->m_LocalBlockStoreAPI = local_block_store;
    api->m_RemoteBlockStoreAPI = remote_block_store;
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(local_block_store, "%p"),
        LONGTAIL_LOGFIELD(remote_block_store, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, local_block_store, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, remote_block_store, return 0)

    size_t api_size = sizeof(struct CacheBlockStoreAPI);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
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
