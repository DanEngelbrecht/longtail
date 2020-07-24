#include "longtail_lrublockstore.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>

struct LRUBlockStoreAPI;

struct LRUStoredBlock {
    struct Longtail_StoredBlock m_StoredBlock;
    struct Longtail_StoredBlock* m_OriginalStoredBlock;
    struct LRUBlockStoreAPI* m_LRUBlockStoreAPI;
    TLongtail_Atomic32 m_RefCount;
};

struct BlockHashToLRUStoredBlock
{
    TLongtail_Hash key;
    uint32_t value;
};

struct BlockHashToCompleteCallbacks
{
    TLongtail_Hash key;
    struct Longtail_AsyncGetStoredBlockAPI** value;
};

struct LRUBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    struct BlockHashToLRUStoredBlock* m_BlockHashToLRUStoredBlock;
    struct BlockHashToCompleteCallbacks* m_BlockHashToCompleteCallbacks;
    HLongtail_SpinLock m_Lock;
    TLongtail_Atomic32 m_PendingRequestCount;
    uint32_t m_AllocatedCount;
    uint32_t m_MaxCount;
    struct LRUStoredBlock* m_CachedBlocks;
    uint32_t* m_FreeCacheBlocks;
    uint32_t* m_AllocatedBlocks;
};

int LRUStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    struct LRUStoredBlock* b = (struct LRUStoredBlock*)stored_block;
    Longtail_AtomicAdd32(&b->m_RefCount, -1);
    return 0;
}

uint32_t EvictLRUBlock(struct LRUBlockStoreAPI* api)
{
    uint32_t evict_index = api->m_AllocatedCount;
    while (evict_index > 0)
    {
        --evict_index;
        uint32_t block_index = api->m_AllocatedBlocks[evict_index];
        struct LRUStoredBlock* stored_block = &api->m_CachedBlocks[block_index];
        if (stored_block->m_RefCount == 0)
        {
            TLongtail_Hash block_hash = *stored_block->m_StoredBlock.m_BlockIndex->m_BlockHash;
            api->m_FreeCacheBlocks[--api->m_AllocatedCount] = block_index;
            hmdel(api->m_BlockHashToLRUStoredBlock, block_hash);
            if (stored_block->m_OriginalStoredBlock->Dispose)
            {
                stored_block->m_OriginalStoredBlock->Dispose(stored_block->m_OriginalStoredBlock);
            }
            return block_index;
        }
    }
    return api->m_MaxCount;
}

struct LRUStoredBlock* StoreBlock(struct LRUBlockStoreAPI* api, struct Longtail_StoredBlock* original_stored_block)
{
    uint32_t block_index = EvictLRUBlock(api);
    if (block_index == api->m_MaxCount)
    {
        return 0;
    }
    TLongtail_Hash block_hash = *original_stored_block->m_BlockIndex->m_BlockHash;
    struct LRUStoredBlock* allocated_block = &api->m_CachedBlocks[block_index];
    allocated_block->m_OriginalStoredBlock = original_stored_block;
    allocated_block->m_LRUBlockStoreAPI = api;
    allocated_block->m_StoredBlock.Dispose = 0;
    allocated_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    allocated_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    allocated_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    hmput(api->m_BlockHashToLRUStoredBlock, block_hash, block_index);
    return allocated_block;
}

struct LRUStoredBlock* GetLRUBlock(struct LRUBlockStoreAPI* api, TLongtail_Hash block_hash)
{
    intptr_t tmp;
    intptr_t find_ptr = hmget_ts(api->m_BlockHashToLRUStoredBlock, block_hash, tmp);
    if (find_ptr == -1)
    {
        return 0;
    }
    uint32_t block_index = api->m_BlockHashToLRUStoredBlock[find_ptr].value;
    struct LRUStoredBlock* stored_block = &api->m_CachedBlocks[block_index];
    uint32_t allocation_slot = 0;
    while (allocation_slot < api->m_AllocatedCount)
    {
        if (api->m_AllocatedBlocks[allocation_slot] == block_index)
        {
            break;
        }
        ++allocation_slot;
    }
    if (allocation_slot > 0)
    {
        uint32_t tmp = api->m_AllocatedBlocks[0];
        api->m_AllocatedBlocks[0] = block_index;
        api->m_AllocatedBlocks[allocation_slot] = tmp;
    }
    return stored_block;
}

struct LRUStoredBlock* LRUStoredBlock_CreateBlock(struct LRUBlockStoreAPI* sharing_block_store_api, struct Longtail_StoredBlock* original_stored_block)
{
    size_t shared_stored_block_size = sizeof(struct LRUStoredBlock);
    struct LRUStoredBlock* shared_stored_block = (struct LRUStoredBlock*)Longtail_Alloc(shared_stored_block_size);
    if (!shared_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUStoredBlock_CreateBlock(%p, %p) failed with %d",
            sharing_block_store_api, original_stored_block,
            ENOMEM)
        return 0;
    }
    shared_stored_block->m_StoredBlock.Dispose = LRUStoredBlock_Dispose;
    shared_stored_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    shared_stored_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    shared_stored_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    shared_stored_block->m_OriginalStoredBlock = original_stored_block;
    shared_stored_block->m_LRUBlockStoreAPI = sharing_block_store_api;
    shared_stored_block->m_RefCount = 0;
    return shared_stored_block;
}

static int LRUBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;

    return api->m_BackingBlockStore->PutStoredBlock(
        api->m_BackingBlockStore,
        stored_block,
        async_complete_api);
}

static int LRUBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_hashes, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_ref_counts, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    return api->m_BackingBlockStore->PreflightGet(
        api->m_BackingBlockStore,
        block_count,
        block_hashes,
        block_ref_counts);
}

struct LRUBlockStore_AsyncGetStoredBlockAPI
{
    struct Longtail_AsyncGetStoredBlockAPI m_AsyncGetStoredBlockAPI;
    struct LRUBlockStoreAPI* m_LRUBlockStoreAPI;
    TLongtail_Hash m_BlockHash;
};

static void LRUBlockStore_AsyncGetStoredBlockAPI_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return)
    struct LRUBlockStore_AsyncGetStoredBlockAPI* async_api = (struct LRUBlockStore_AsyncGetStoredBlockAPI*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(async_api->m_LRUBlockStoreAPI != 0, return)
    TLongtail_Hash block_hash = async_api->m_BlockHash;

    struct LRUBlockStoreAPI* api = async_api->m_LRUBlockStoreAPI;
    Longtail_Free(async_api);

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
        Longtail_AtomicAdd32(&api->m_PendingRequestCount, -1);
        return;
    }

    struct LRUStoredBlock* shared_stored_block = LRUStoredBlock_CreateBlock(api, stored_block);
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
            list[i]->OnComplete(list[i], 0, ENOMEM);
        }
        arrfree(list);
        stored_block->Dispose(stored_block);
        Longtail_AtomicAdd32(&api->m_PendingRequestCount, -1);
        return;
    }

    struct Longtail_AsyncGetStoredBlockAPI** list;
    Longtail_LockSpinLock(api->m_Lock);
    list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
    hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
    Longtail_AtomicAdd32(&shared_stored_block->m_RefCount, (int32_t)arrlen(list));
    Longtail_UnlockSpinLock(api->m_Lock);
    size_t wait_count = arrlen(list);
    for (size_t i = 0; i < wait_count; ++i)
    {
        list[i]->OnComplete(list[i], &shared_stored_block->m_StoredBlock, 0);
    }
    arrfree(list);
    Longtail_AtomicAdd32(&api->m_PendingRequestCount, -1);
}

static int LRUBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)",
        block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)
    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;

    Longtail_LockSpinLock(api->m_Lock);

    struct LRUStoredBlock* lru_block = GetLRUBlock(api, block_hash);
    if (lru_block != 0)
    {
        Longtail_AtomicAdd32(&lru_block->m_RefCount, 1);
        Longtail_UnlockSpinLock(api->m_Lock);
        async_complete_api->OnComplete(async_complete_api, &lru_block->m_StoredBlock, 0);
        return 0;
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

    size_t share_lock_store_async_get_stored_block_API_size = sizeof(struct LRUBlockStore_AsyncGetStoredBlockAPI);
    struct LRUBlockStore_AsyncGetStoredBlockAPI* share_lock_store_async_get_stored_block_API = (struct LRUBlockStore_AsyncGetStoredBlockAPI*)Longtail_Alloc(share_lock_store_async_get_stored_block_API_size);
    if (!share_lock_store_async_get_stored_block_API)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            ENOMEM)
        return ENOMEM;
    }

    share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.m_API.Dispose = 0;
    share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.OnComplete = LRUBlockStore_AsyncGetStoredBlockAPI_OnComplete;
    share_lock_store_async_get_stored_block_API->m_LRUBlockStoreAPI = api;
    share_lock_store_async_get_stored_block_API->m_BlockHash = block_hash;

    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);
    int err = api->m_BackingBlockStore->GetStoredBlock(
        api->m_BackingBlockStore,
        block_hash,
        &share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            err)

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
    return 0;
}

static int LRUBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_GetIndex(%p, %u, %p)", block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    int err = api->m_BackingBlockStore->GetIndex(
        api->m_BackingBlockStore,
        async_complete_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_GetIndex(%p, %p) failed with %d",
            block_store_api, async_complete_api,
            err)
        return err;
    }
    return 0;
}

static int LRUBlockStore_RetargetContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_RetargetContent(%p, %p, %p)",
        block_store_api, content_index, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    int err = api->m_BackingBlockStore->RetargetContent(
        api->m_BackingBlockStore,
        content_index,
        async_complete_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_RetargetContent(%p, %p, %p) failed with %d",
            block_store_api, content_index, async_complete_api,
            err)
        return err;
    }
    return 0;
}

static int LRUBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

static void LRUBlockStore_Dispose(struct Longtail_API* base_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_Dispose(%p)", base_api)
    LONGTAIL_FATAL_ASSERT(base_api, return)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)base_api;
    while (api->m_PendingRequestCount > 0)
    {
        Longtail_Sleep(1000);
        if (api->m_PendingRequestCount > 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_Dispose(%p) waiting for %d pending requests", api, (int32_t)api->m_PendingRequestCount);
        }
    }
    hmfree(api->m_BlockHashToCompleteCallbacks);
    hmfree(api->m_BlockHashToLRUStoredBlock);
    Longtail_Free(api->m_CachedBlocks);
    Longtail_Free(api->m_FreeCacheBlocks);
    Longtail_Free(api->m_AllocatedBlocks);
    Longtail_DeleteSpinLock(api->m_Lock);
    Longtail_Free(api->m_Lock);
    Longtail_Free(api);
}

static int LRUBlockStore_Init(
    void* mem,
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t max_lru_count,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_Dispose(%p, %p, %p)",
        mem, backing_block_store, out_block_store_api)
    LONGTAIL_FATAL_ASSERT(mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        LRUBlockStore_Dispose,
        LRUBlockStore_PutStoredBlock,
        LRUBlockStore_PreflightGet,
        LRUBlockStore_GetStoredBlock,
        LRUBlockStore_GetIndex,
        LRUBlockStore_RetargetContent,
        LRUBlockStore_GetStats);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    api->m_BackingBlockStore = backing_block_store;
    api->m_BlockHashToLRUStoredBlock = 0;
    api->m_BlockHashToCompleteCallbacks = 0;
    api->m_PendingRequestCount = 0;

    api->m_AllocatedCount = 0;
    api->m_MaxCount = max_lru_count;
    api->m_CachedBlocks = (struct LRUStoredBlock*)Longtail_Alloc(sizeof(struct LRUStoredBlock*) * max_lru_count);
    api->m_FreeCacheBlocks = (uint32_t*)Longtail_Alloc(sizeof(uint32_t*) * max_lru_count);
    api->m_AllocatedBlocks = (uint32_t*)Longtail_Alloc(sizeof(uint32_t*) * max_lru_count);
    for (uint32_t b = 0; b < max_lru_count; ++b)
    {
        api->m_FreeCacheBlocks[b] = b;
    }

    int err =Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateLRUBlockStoreAPI(%p, %p) failed with %d",
            api, backing_block_store,
            ENOMEM)
        return err;
    }
    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateLRUBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t max_lru_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateLRUBlockStoreAPI(%p)", backing_block_store)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return 0)

    size_t api_size = sizeof(struct LRUBlockStoreAPI);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateLRUBlockStoreAPI(%p) failed with %d",
            backing_block_store,
            ENOMEM)
        return 0;
    }
    struct Longtail_BlockStoreAPI* block_store_api;
    int err = LRUBlockStore_Init(
        mem,
        backing_block_store,
        max_lru_count,
        &block_store_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateLRUBlockStoreAPI(%p) failed with %d",
            backing_block_store,
            err)
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
