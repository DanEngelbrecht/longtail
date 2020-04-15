#include "longtail_shareblockstore.h"

#include "../../src/longtail.h"
#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>

struct ShareBlockStoreAPI;

struct SharedStoredBlock {
    struct Longtail_StoredBlock m_StoredBlock;
    struct Longtail_StoredBlock* m_OriginalStoredBlock;
    struct ShareBlockStoreAPI* m_ShareBlockStoreAPI;
    TLongtail_Atomic32 m_RefCount;
};

struct BlockHashToSharedStoredBlock
{
    TLongtail_Hash key;
    struct SharedStoredBlock* value;
};

struct BlockHashToCompleteCallbacks
{
    TLongtail_Hash key;
    struct Longtail_AsyncGetStoredBlockAPI** value;
};

struct ShareBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    struct BlockHashToSharedStoredBlock* m_BlockHashToSharedStoredBlock;
    struct BlockHashToCompleteCallbacks* m_BlockHashToCompleteCallbacks;
    HLongtail_SpinLock m_Lock;
};

int SharedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    struct SharedStoredBlock* b = (struct SharedStoredBlock*)stored_block;
    int32_t ref_count = Longtail_AtomicAdd32(&b->m_RefCount, -1);
    if (ref_count > 0)
    {
        return 0;
    }
    struct ShareBlockStoreAPI* api = b->m_ShareBlockStoreAPI;
    TLongtail_Hash block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    Longtail_LockSpinLock(api->m_Lock);
    if (b->m_RefCount != 0)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        return 0;
    }
    hmdel(api->m_BlockHashToSharedStoredBlock, block_hash);
    Longtail_UnlockSpinLock(api->m_Lock);

    struct Longtail_StoredBlock* original_stored_block = b->m_OriginalStoredBlock;
    Longtail_Free(b);
    original_stored_block->Dispose(original_stored_block);
    return 0;
}

struct SharedStoredBlock* SharedStoredBlock_CreateBlock(struct ShareBlockStoreAPI* sharing_block_store_api, struct Longtail_StoredBlock* original_stored_block)
{
    size_t shared_stored_block_size = sizeof(struct SharedStoredBlock);
    struct SharedStoredBlock* shared_stored_block = (struct SharedStoredBlock*)Longtail_Alloc(shared_stored_block_size);
    if (!shared_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "SharedStoredBlock_CreateBlock(%p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            sharing_block_store_api, original_stored_block,
            shared_stored_block_size,
            ENOMEM)
        return 0;
    }
    shared_stored_block->m_StoredBlock.Dispose = SharedStoredBlock_Dispose;
    shared_stored_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    shared_stored_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    shared_stored_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    shared_stored_block->m_OriginalStoredBlock = original_stored_block;
    shared_stored_block->m_ShareBlockStoreAPI = sharing_block_store_api;
    shared_stored_block->m_RefCount = 0;
    return shared_stored_block;
}

static int ShareBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)block_store_api;

    return api->m_BackingBlockStore->PutStoredBlock(
        api->m_BackingBlockStore,
        stored_block,
        async_complete_api);
}

static int ShareBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_hashes, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_ref_counts, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)block_store_api;
    return api->m_BackingBlockStore->PreflightGet(
        api->m_BackingBlockStore,
        block_count,
        block_hashes,
        block_ref_counts);
}

struct ShareBlockStore_AsyncGetStoredBlockAPI
{
    struct Longtail_AsyncGetStoredBlockAPI m_AsyncGetStoredBlockAPI;
    struct ShareBlockStoreAPI* m_ShareblockstoreAPI;
};

static int ShareBlockStore_AsyncGetStoredBlockAPI_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return EINVAL)
    struct ShareBlockStore_AsyncGetStoredBlockAPI* async_api = (struct ShareBlockStore_AsyncGetStoredBlockAPI*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(async_api->m_ShareblockstoreAPI != 0, return EINVAL)

    struct ShareBlockStoreAPI* api = async_api->m_ShareblockstoreAPI;
    Longtail_Free(async_api);

    TLongtail_Hash block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    if (err)
    {
        struct Longtail_AsyncGetStoredBlockAPI** list;
        Longtail_LockSpinLock(api->m_Lock);
        list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
        hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
        Longtail_UnlockSpinLock(api->m_Lock);

        // Anybody else who was successfully put up on wait list will get the error forwarded in their OnComplete
        size_t wait_count = arrlen(list);
        for (size_t i = 0; i < wait_count; ++i)
        {
            list[i]->OnComplete(list[i], 0, err);
        }
        arrfree(list);
        return err;
    }

    struct SharedStoredBlock* shared_stored_block = SharedStoredBlock_CreateBlock(api, stored_block);
    if (!shared_stored_block)
    {
        struct Longtail_AsyncGetStoredBlockAPI** list;
        Longtail_LockSpinLock(api->m_Lock);
        list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
        hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
        Longtail_UnlockSpinLock(api->m_Lock);

        // Anybody else who was successfully put up on wait list will get the error forwarded in their OnComplete
        size_t wait_count = arrlen(list);
        for (size_t i = 0; i < wait_count; ++i)
        {
            list[i]->OnComplete(list[i], 0, err);
        }
        arrfree(list);
        return err;
    }

    struct Longtail_AsyncGetStoredBlockAPI** list;
    Longtail_LockSpinLock(api->m_Lock);
    list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
    hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
    hmput(api->m_BlockHashToSharedStoredBlock, block_hash, shared_stored_block);
    Longtail_AtomicAdd32(&shared_stored_block->m_RefCount, (int32_t)arrlen(list));
    Longtail_UnlockSpinLock(api->m_Lock);
    size_t wait_count = arrlen(list);
    for (size_t i = 0; i < wait_count; ++i)
    {
        int callback_err = list[i]->OnComplete(list[i], &shared_stored_block->m_StoredBlock, 0);
        if (callback_err)
        {
            shared_stored_block->m_StoredBlock.Dispose(&shared_stored_block->m_StoredBlock);
        }
    }
    arrfree(list);
    return 0;
}

static int ShareBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)
    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)block_store_api;

    Longtail_LockSpinLock(api->m_Lock);

    intptr_t find_block_ptr = hmgeti(api->m_BlockHashToSharedStoredBlock, block_hash);
    if (find_block_ptr != -1)
    {
        struct SharedStoredBlock* shared_stored_block = api->m_BlockHashToSharedStoredBlock[find_block_ptr].value;
        Longtail_AtomicAdd32(&shared_stored_block->m_RefCount, 1);
        Longtail_UnlockSpinLock(api->m_Lock);
        int err = async_complete_api->OnComplete(async_complete_api, &shared_stored_block->m_StoredBlock, 0);
        if (err)
        {
            shared_stored_block->m_StoredBlock.Dispose(&shared_stored_block->m_StoredBlock);
        }
        return err;
    }

    intptr_t find_wait_list_ptr = hmgeti(api->m_BlockHashToCompleteCallbacks, block_hash);
    if (find_wait_list_ptr != -1)
    {
        arrput(api->m_BlockHashToCompleteCallbacks[find_wait_list_ptr].value, async_complete_api);
        Longtail_UnlockSpinLock(api->m_Lock);
        return 0;
    }

    struct Longtail_AsyncGetStoredBlockAPI** wait_list = 0;
    arrput(wait_list, async_complete_api);

    hmput(api->m_BlockHashToCompleteCallbacks, block_hash, wait_list);

    Longtail_UnlockSpinLock(api->m_Lock);

    size_t share_lock_store_async_get_stored_block_API_size = sizeof(struct ShareBlockStore_AsyncGetStoredBlockAPI);
    struct ShareBlockStore_AsyncGetStoredBlockAPI* share_lock_store_async_get_stored_block_API = (struct ShareBlockStore_AsyncGetStoredBlockAPI*)Longtail_Alloc(share_lock_store_async_get_stored_block_API_size);
    if (!share_lock_store_async_get_stored_block_API)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ShareBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, block_hash, async_complete_api,
            share_lock_store_async_get_stored_block_API_size,
            ENOMEM)
        return ENOMEM;
    }

    share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.m_API.Dispose = 0;
    share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.OnComplete = ShareBlockStore_AsyncGetStoredBlockAPI_OnComplete;
    share_lock_store_async_get_stored_block_API->m_ShareblockstoreAPI = api;

    int err = api->m_BackingBlockStore->GetStoredBlock(
        api->m_BackingBlockStore,
        block_hash,
        &share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI);
    if (err)
    {
        // TODO: Log
        struct Longtail_AsyncGetStoredBlockAPI** list;
        Longtail_LockSpinLock(api->m_Lock);
        list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
        hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
        Longtail_UnlockSpinLock(api->m_Lock);

        // Anybody else who was successfully put up on wait list will get the error forwarded in their OnComplete
        size_t wait_count = arrlen(list);
        for (size_t i = 1; i < wait_count; ++i)
        {
            list[i]->OnComplete(list[i], 0, err);
        }
        arrfree(list);
        Longtail_Free(share_lock_store_async_get_stored_block_API);
        return err;
    }
    return 0;
}

static int ShareBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_GetIndex(%p, %u, %p)", block_store_api, default_hash_api_identifier, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)block_store_api;
    return api->m_BackingBlockStore->GetIndex(
        api->m_BackingBlockStore,
        default_hash_api_identifier,
        async_complete_api);
}

static int ShareBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

static void ShareBlockStore_Dispose(struct Longtail_API* base_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_Dispose(%p)", base_api)
    LONGTAIL_FATAL_ASSERT(base_api, return)

    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)base_api;
    hmfree(api->m_BlockHashToCompleteCallbacks);
    hmfree(api->m_BlockHashToSharedStoredBlock);
    Longtail_DeleteSpinLock(api->m_Lock);
    Longtail_Free(api->m_Lock);
    Longtail_Free(api);
}

static int ShareBlockStore_Init(
    struct ShareBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ShareBlockStore_Dispose(%p, %p, %" PRIu64 ")", api, backing_block_store)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = ShareBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = ShareBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = ShareBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = ShareBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = ShareBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = ShareBlockStore_GetStats;
    api->m_BackingBlockStore = backing_block_store;
    api->m_BlockHashToSharedStoredBlock = 0;
    api->m_BlockHashToCompleteCallbacks = 0;
    int err =Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        return err;
    }
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateShareBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateShareBlockStoreAPI(%p)", backing_block_store)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return 0)

    size_t api_size = sizeof(struct ShareBlockStoreAPI);
    struct ShareBlockStoreAPI* api = (struct ShareBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateShareBlockStoreAPI(%p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            backing_block_store,
            api_size,
            ENOMEM)
        return 0;
    }
    int err = ShareBlockStore_Init(
        api,
        backing_block_store);
    if (err)
    {
        Longtail_Free(api);
        return 0;
    }
    return &api->m_BlockStoreAPI;
}
