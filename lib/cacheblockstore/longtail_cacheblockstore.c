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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cacheblockstore_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    struct CachedStoredBlock* b = (struct CachedStoredBlock*)stored_block;
    int32_t ref_count = Longtail_AtomicAdd32(&b->m_RefCount, -1);
    if (ref_count > 0)
    {
        return 0;
    }
    struct Longtail_StoredBlock* original_stored_block = b->m_OriginalStoredBlock;
    Longtail_Free(b);
    SAFE_DISPOSE_STORED_BLOCK(original_stored_block);
    return 0;
}

struct Longtail_StoredBlock* CachedStoredBlock_CreateBlock(struct Longtail_StoredBlock* original_stored_block, int32_t refCount)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(original_stored_block, "%p"),
        LONGTAIL_LOGFIELD(refCount, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    size_t cached_stored_block_size = sizeof(struct CachedStoredBlock);
    struct CachedStoredBlock* cached_stored_block = (struct CachedStoredBlock*)Longtail_Alloc("CacheBlockStore", cached_stored_block_size);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    size_t put_stored_block_put_remote_complete_api_size = sizeof(struct PutStoredBlockPutRemoteComplete_API);
    struct PutStoredBlockPutRemoteComplete_API* put_stored_block_put_remote_complete_api = (struct PutStoredBlockPutRemoteComplete_API*)Longtail_Alloc("CacheBlockStore", put_stored_block_put_remote_complete_api_size);
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
    struct PutStoredBlockPutLocalComplete_API* put_stored_block_put_local_complete_api = (struct PutStoredBlockPutLocalComplete_API*)Longtail_Alloc("CacheBlockStore", put_stored_block_put_local_complete_api_size);
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

struct PreflightStartedContext
{
    struct Longtail_AsyncPreflightStartedAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    uint32_t m_BlockCount;
    TLongtail_Hash* m_BlockHashes;
    struct Longtail_AsyncPreflightStartedAPI* m_FinalAsyncCompleteAPI;
};

static void PreflightGet_PreflightStartedAPI_OnComplete(struct Longtail_AsyncPreflightStartedAPI* async_complete_api, uint32_t block_count, TLongtail_Hash* block_hashes, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(block_hashes, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct PreflightStartedContext* get_existing_content_context = (struct PreflightStartedContext*)async_complete_api;
    struct CacheBlockStoreAPI* api = get_existing_content_context->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "PreflightGet_PreflightStartedAPI_OnComplete called with error %d", err)
        if (get_existing_content_context->m_FinalAsyncCompleteAPI)
        {
            get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, 0, err);
        }
        Longtail_Free(get_existing_content_context);
        CacheBlockStore_CompleteRequest(api);
        return;
    }

    // We found everything in the cache
    if (block_count == get_existing_content_context->m_BlockCount)
    {
        if (get_existing_content_context->m_FinalAsyncCompleteAPI)
        {
            get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, block_count, block_hashes, 0);
        }
        Longtail_Free(get_existing_content_context);
        CacheBlockStore_CompleteRequest(api);
        return;
    }

    if (block_count == 0)
    {
        err = api->m_RemoteBlockStoreAPI->PreflightGet(api->m_RemoteBlockStoreAPI, get_existing_content_context->m_BlockCount, get_existing_content_context->m_BlockHashes, get_existing_content_context->m_FinalAsyncCompleteAPI);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_RemoteBlockStoreAPI->PreflightGet() failed with %d", err)
            if (get_existing_content_context->m_FinalAsyncCompleteAPI)
            {
                get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, 0, err);
            }
            Longtail_Free(get_existing_content_context);
            CacheBlockStore_CompleteRequest(api);
            return;
        }
        Longtail_Free(get_existing_content_context);
        CacheBlockStore_CompleteRequest(api);
        return;
    }

    struct Longtail_LookupTable* local_store_block_lookup = LongtailPrivate_LookupTable_Create(Longtail_Alloc("CacheBlockStore", LongtailPrivate_LookupTable_GetSize(get_existing_content_context->m_BlockCount)), get_existing_content_context->m_BlockCount, 0);
    if (!local_store_block_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(get_existing_content_context);
        CacheBlockStore_CompleteRequest(api);
        return;
    }

    TLongtail_Hash* missing_block_hashes = (TLongtail_Hash*)Longtail_Alloc("CacheBlockStore", sizeof(TLongtail_Hash) * get_existing_content_context->m_BlockCount);
    if (!missing_block_hashes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(local_store_block_lookup);
        Longtail_Free(get_existing_content_context);
        CacheBlockStore_CompleteRequest(api);
        return;
    }

    for (uint32_t b = 0; b < block_count; ++b)
    {
        TLongtail_Hash block_hash = block_hashes[b];
        LongtailPrivate_LookupTable_PutUnique(local_store_block_lookup, block_hash, b);
    }

    uint32_t missing_block_count = 0;
    for (uint32_t b = 0; b < get_existing_content_context->m_BlockCount; ++b)
    {
        TLongtail_Hash block_hash = get_existing_content_context->m_BlockHashes[b];
        if (LongtailPrivate_LookupTable_PutUnique(local_store_block_lookup, block_hash, b))
        {
            continue;
        }
        missing_block_hashes[missing_block_count++] = block_hash;
    }

    if (missing_block_count > 0)
    {
        // We currently do not support calling back with a merged result for start of prefecth block
        // That is OK, because that would only need to happen if there were two layers of cache in a store block stack
        // and that use case is so far not a priority.
        err = api->m_RemoteBlockStoreAPI->PreflightGet(api->m_RemoteBlockStoreAPI, missing_block_count, missing_block_hashes, 0);
        if (get_existing_content_context->m_FinalAsyncCompleteAPI)
        {
            get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, 0, err);
        }
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_RemoteBlockStoreAPI->PreflightGet() failed with %d", err)
            Longtail_Free(missing_block_hashes);
            Longtail_Free(local_store_block_lookup);
            Longtail_Free(get_existing_content_context);
            CacheBlockStore_CompleteRequest(api);
            return;
        }
    }
    else if (get_existing_content_context->m_FinalAsyncCompleteAPI)
    {
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, block_count, block_hashes, 0);
    }

    Longtail_Free(missing_block_hashes);
    Longtail_Free(local_store_block_lookup);
    Longtail_Free(get_existing_content_context);
    CacheBlockStore_CompleteRequest(api);
}

static int CacheBlockStore_PreflightGet(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_count,
    const TLongtail_Hash* block_hashes,
    struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(block_hashes, "%p"),
        LONGTAIL_LOGFIELD(optional_async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (block_count == 0) || (block_hashes != 0), return EINVAL)
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);

    struct PreflightStartedContext* context = (struct PreflightStartedContext*)Longtail_Alloc("CacheBlockStore", sizeof(struct PreflightStartedContext) + sizeof(TLongtail_Hash) * block_count);
    context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    context->m_AsyncCompleteAPI.OnComplete = PreflightGet_PreflightStartedAPI_OnComplete;
    context->m_CacheBlockStoreAPI = api;
    context->m_BlockCount = block_count;
    context->m_BlockHashes = (TLongtail_Hash*)&context[1];
    context->m_FinalAsyncCompleteAPI = optional_async_complete_api;
    memcpy(context->m_BlockHashes, block_hashes, sizeof(TLongtail_Hash) * block_count);

    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);
    int err = api->m_LocalBlockStoreAPI->PreflightGet(
        api->m_LocalBlockStoreAPI,
        block_count,
        block_hashes,
        &context->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "api->m_LocalBlockStoreAPI->GetExistingContent() failed with %d", err)
        Longtail_Free(context);
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
        CacheBlockStore_CompleteRequest(api);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct OnGetStoredBlockPutLocalComplete_API* api = (struct OnGetStoredBlockPutLocalComplete_API*)async_complete_api;
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockPutLocalComplete called with error %d", err)
    }
    SAFE_DISPOSE_STORED_BLOCK(api->m_StoredBlock);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cacheblockstore_api, "%p"),
        LONGTAIL_LOGFIELD(local_block_store, "%p"),
        LONGTAIL_LOGFIELD(cached_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    size_t put_local_size = sizeof(struct OnGetStoredBlockPutLocalComplete_API);
    struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc("CacheBlockStore", put_local_size);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
        SAFE_DISPOSE_STORED_BLOCK(stored_block);
        api->async_complete_api->OnComplete(api->async_complete_api, 0, ENOMEM);
        Longtail_Free(api);
        CacheBlockStore_CompleteRequest(cacheblockstore_api);
        return;
    }

    api->async_complete_api->OnComplete(api->async_complete_api, cached_stored_block, 0);

    int store_err = StoreBlockCopyToLocalCache(cacheblockstore_api, cacheblockstore_api->m_LocalBlockStoreAPI, cached_stored_block);
    if (store_err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "StoreBlockCopyToLocalCache() failed with %d", store_err)
        SAFE_DISPOSE_STORED_BLOCK(cached_stored_block);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api, return)
    struct OnGetStoredBlockGetLocalComplete_API* api = (struct OnGetStoredBlockGetLocalComplete_API*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, api->async_complete_api, return)
    struct CacheBlockStoreAPI* cacheblockstore_api = api->m_CacheBlockStoreAPI;
    if (err == ENOENT || err == EACCES)
    {
        size_t on_get_stored_block_get_remote_complete_size = sizeof(struct OnGetStoredBlockGetRemoteComplete_API);
        struct OnGetStoredBlockGetRemoteComplete_API* on_get_stored_block_get_remote_complete = (struct OnGetStoredBlockGetRemoteComplete_API*)Longtail_Alloc("CacheBlockStore", on_get_stored_block_get_remote_complete_size);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&cacheblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    size_t on_get_stored_block_get_local_complete_api_size = sizeof(struct OnGetStoredBlockGetLocalComplete_API);
    struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc("CacheBlockStore", on_get_stored_block_get_local_complete_api_size);
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

struct GetExistingContext_GetExistingRemoteContent_Context
{
    struct Longtail_AsyncGetExistingContentAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetExistingContentAPI* m_FinalAsyncCompleteAPI;
    struct Longtail_StoreIndex* m_LocalExistingStoreIndex;
    uint32_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
};

static void GetExistingContent_GetExistingRemoteContentCompleteAPI_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_StoreIndex* store_index, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct GetExistingContext_GetExistingRemoteContent_Context* get_existing_content_context = (struct GetExistingContext_GetExistingRemoteContent_Context*)async_complete_api;
    struct CacheBlockStoreAPI* api = get_existing_content_context->m_CacheBlockStoreAPI;
    if (err)
    {
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, err);
        Longtail_Free(get_existing_content_context->m_LocalExistingStoreIndex);
        Longtail_Free(get_existing_content_context);
        return;
    }
    struct Longtail_StoreIndex* merged_store_index;
    err = Longtail_MergeStoreIndex(
        get_existing_content_context->m_LocalExistingStoreIndex,
        store_index,
        &merged_store_index);
    Longtail_Free(store_index);
    Longtail_Free(get_existing_content_context->m_LocalExistingStoreIndex);
    if (err)
    {
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, err);
        Longtail_Free(get_existing_content_context);
        return;
    }
    get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, merged_store_index, 0);
    Longtail_Free(get_existing_content_context);
}

struct GetExistingContext_GetExistingLocalContent_Context
{
    struct Longtail_AsyncGetExistingContentAPI m_AsyncCompleteAPI;
    struct CacheBlockStoreAPI* m_CacheBlockStoreAPI;
    struct Longtail_AsyncGetExistingContentAPI* m_FinalAsyncCompleteAPI;
    uint32_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t m_MinBlockUsagePercent;
};

static void GetExistingLocalContent_GetExistingContentCompleteAPI_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_StoreIndex* store_index, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct GetExistingContext_GetExistingLocalContent_Context* get_existing_content_context = (struct GetExistingContext_GetExistingLocalContent_Context*)async_complete_api;
    struct CacheBlockStoreAPI* api = get_existing_content_context->m_CacheBlockStoreAPI;
    if (err)
    {
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, err);
        Longtail_Free(get_existing_content_context);
        return;
    }
    struct GetExistingContext_GetExistingRemoteContent_Context* existing_remote_content_context = (struct GetExistingContext_GetExistingRemoteContent_Context*)Longtail_Alloc("CacheBlockStore", sizeof(struct GetExistingContext_GetExistingRemoteContent_Context) + (sizeof(TLongtail_Hash) * get_existing_content_context->m_ChunkCount));
    existing_remote_content_context->m_ChunkHashes = (TLongtail_Hash*)&existing_remote_content_context[1];
    err = Longtail_GetMissingChunks(
        store_index,
        get_existing_content_context->m_ChunkCount,
        get_existing_content_context->m_ChunkHashes,
        &existing_remote_content_context->m_ChunkCount,
        existing_remote_content_context->m_ChunkHashes);
    if (err)
    {
        Longtail_Free(store_index);
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, err);
        Longtail_Free(get_existing_content_context);
        return;
    }
    if (existing_remote_content_context->m_ChunkCount == 0)
    {
        Longtail_Free(existing_remote_content_context);
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, store_index, 0);
        Longtail_Free(get_existing_content_context);
        return;
    }
    existing_remote_content_context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    existing_remote_content_context->m_AsyncCompleteAPI.OnComplete = GetExistingContent_GetExistingRemoteContentCompleteAPI_OnComplete;
    existing_remote_content_context->m_CacheBlockStoreAPI = api;
    existing_remote_content_context->m_LocalExistingStoreIndex = store_index;
    existing_remote_content_context->m_FinalAsyncCompleteAPI = get_existing_content_context->m_FinalAsyncCompleteAPI;
    err = api->m_RemoteBlockStoreAPI->GetExistingContent(
        api->m_RemoteBlockStoreAPI,
        existing_remote_content_context->m_ChunkCount,
        existing_remote_content_context->m_ChunkHashes,
        get_existing_content_context->m_MinBlockUsagePercent,
        &existing_remote_content_context->m_AsyncCompleteAPI);
    if (err)
    {
        get_existing_content_context->m_FinalAsyncCompleteAPI->OnComplete(get_existing_content_context->m_FinalAsyncCompleteAPI, 0, err);
        Longtail_Free(existing_remote_content_context);
        Longtail_Free(store_index);
        Longtail_Free(get_existing_content_context);
        return;
    }
    Longtail_Free(get_existing_content_context);
}

static int CacheBlockStore_GetExistingContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t chunk_count,
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

    size_t async_complete_size = sizeof(struct GetExistingContext_GetExistingLocalContent_Context) + sizeof(TLongtail_Hash) * chunk_count;
    struct GetExistingContext_GetExistingLocalContent_Context* existing_local_content_context = (struct GetExistingContext_GetExistingLocalContent_Context*)Longtail_Alloc("CacheBlockStore", async_complete_size);
    existing_local_content_context->m_AsyncCompleteAPI.m_API.Dispose = 0;
    existing_local_content_context->m_AsyncCompleteAPI.OnComplete = GetExistingLocalContent_GetExistingContentCompleteAPI_OnComplete;
    existing_local_content_context->m_CacheBlockStoreAPI = api;
    existing_local_content_context->m_FinalAsyncCompleteAPI = async_complete_api;
    existing_local_content_context->m_ChunkCount = chunk_count;
    existing_local_content_context->m_ChunkHashes = (TLongtail_Hash*)&existing_local_content_context[1];
    existing_local_content_context->m_MinBlockUsagePercent = min_block_usage_percent;
    memcpy(existing_local_content_context->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);

    int err = api->m_LocalBlockStoreAPI->GetExistingContent(
        api->m_LocalBlockStoreAPI,
        chunk_count,
        chunk_hashes,
        min_block_usage_percent,
        &existing_local_content_context->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_LocalBlockStoreAPI->GetExistingContent() failed with %d", err)
        Longtail_Free(existing_local_content_context);
        return err;
    }
    return 0;
}

static int CacheBlockStore_PruneBlocks(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_keep_count,
    const TLongtail_Hash* block_keep_hashes,
    struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_keep_count, "%u"),
        LONGTAIL_LOGFIELD(block_keep_hashes, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (block_keep_count == 0) || (block_keep_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    return ENOTSUP;
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
        CacheBlockStore_PruneBlocks,
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

    int err = Longtail_CreateSpinLock(Longtail_Alloc("CacheBlockStore", Longtail_GetSpinLockSize()), &api->m_Lock);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, local_block_store, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, remote_block_store, return 0)

    size_t api_size = sizeof(struct CacheBlockStoreAPI);
    void* mem = Longtail_Alloc("CacheBlockStore", api_size);
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
