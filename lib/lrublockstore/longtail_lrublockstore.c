#include "longtail_lrublockstore.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>

struct LRU
{
    struct LRUStoredBlock** m_StoredBlocks;
    uint32_t m_MaxCount;
    uint32_t m_AllocatedCount;
};

size_t LRU_GetSize(uint32_t max_count)
{
    return sizeof(struct LRU) +
        sizeof(struct LRUStoredBlock*) * max_count;
}

struct LRU* LRU_Create(void* mem, uint32_t max_count)
{
    LONGTAIL_FATAL_ASSERT(mem, return 0)
    struct LRU* lru = (struct LRU*)mem;
    lru->m_StoredBlocks = (struct LRUStoredBlock**)&lru[1];
    lru->m_MaxCount = max_count;
    lru->m_AllocatedCount = 0;
    return lru;
}

struct LRUStoredBlock* LRU_Evict(struct LRU* lru)
{
    LONGTAIL_FATAL_ASSERT(lru, return 0)
    LONGTAIL_FATAL_ASSERT(lru->m_AllocatedCount > 0, return 0)
    struct LRUStoredBlock* stored_block = lru->m_StoredBlocks[0];
    --lru->m_AllocatedCount;
    for(uint32_t scan = 0; scan < lru->m_AllocatedCount; ++scan)
    {
        lru->m_StoredBlocks[scan] = lru->m_StoredBlocks[scan + 1];
    }
    return stored_block;
}

struct LRUStoredBlock** LRU_Put(struct LRU* lru)
{
    LONGTAIL_FATAL_ASSERT(lru, return 0)
    LONGTAIL_FATAL_ASSERT(lru->m_AllocatedCount != lru->m_MaxCount, return 0)
    struct LRUStoredBlock** slot = &lru->m_StoredBlocks[lru->m_AllocatedCount];
    ++lru->m_AllocatedCount;
    return slot;
}

void LRU_Refresh(struct LRU* lru, struct LRUStoredBlock* stored_block)
{
    LONGTAIL_FATAL_ASSERT(lru, return)
    for (uint32_t i = 0; i < lru->m_AllocatedCount; ++i)
    {
        if (lru->m_StoredBlocks[i] == stored_block)
        {
            while(++i < lru->m_AllocatedCount)
            {
                lru->m_StoredBlocks[i - 1] = lru->m_StoredBlocks[i];
            }
            lru->m_StoredBlocks[lru->m_AllocatedCount - 1] = stored_block;
            return;
        }
    }
    LONGTAIL_FATAL_ASSERT(0, return)
}

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
    struct LRUStoredBlock* value;
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

    TLongtail_Atomic64 m_StatU64[Longtail_BlockStoreAPI_StatU64_Count];

    HLongtail_SpinLock m_Lock;
    struct Longtail_AsyncFlushAPI** m_PendingAsyncFlushAPIs;
    struct LRU* m_LRU;
    struct BlockHashToLRUStoredBlock* m_BlockHashToLRUStoredBlock;
    struct BlockHashToCompleteCallbacks* m_BlockHashToCompleteCallbacks;

    TLongtail_Atomic32 m_PendingRequestCount;
};

static void LRUBlockStore_CompleteRequest(struct LRUBlockStoreAPI* lrublockstore_api)
{
    LONGTAIL_FATAL_ASSERT(lrublockstore_api->m_PendingRequestCount > 0, return)
    struct Longtail_AsyncFlushAPI** pendingAsyncFlushAPIs = 0;
    Longtail_LockSpinLock(lrublockstore_api->m_Lock);
    if (0 == Longtail_AtomicAdd32(&lrublockstore_api->m_PendingRequestCount, -1))
    {
        pendingAsyncFlushAPIs = lrublockstore_api->m_PendingAsyncFlushAPIs;
        lrublockstore_api->m_PendingAsyncFlushAPIs = 0;
    }
    Longtail_UnlockSpinLock(lrublockstore_api->m_Lock);
    size_t c = arrlen(pendingAsyncFlushAPIs);
    for (size_t n = 0; n < c; ++n)
    {
        pendingAsyncFlushAPIs[n]->OnComplete(pendingAsyncFlushAPIs[n], 0);
    }
    arrfree(pendingAsyncFlushAPIs);
}

int LRUStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    LONGTAIL_FATAL_ASSERT(stored_block != 0, return EINVAL)
    struct LRUStoredBlock* b = (struct LRUStoredBlock*)stored_block;
    TLongtail_Hash block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    int32_t ref_count = Longtail_AtomicAdd32(&b->m_RefCount, -1);
    if (ref_count > 1)
    {
        return 0;
    }
    struct LRUBlockStoreAPI* api = b->m_LRUBlockStoreAPI;
    if (ref_count == 1)
    {
        // We dipped down to just our LRU ref count - see if it is still around and refresh LRU position if so
        Longtail_LockSpinLock(api->m_Lock);
        intptr_t tmp;
        intptr_t find_ptr = hmgeti_ts(api->m_BlockHashToLRUStoredBlock, block_hash, tmp);
        if (find_ptr == -1)
        {
            Longtail_UnlockSpinLock(api->m_Lock);
            return 0;
        }
        struct LRUStoredBlock* stored_block = api->m_BlockHashToLRUStoredBlock[find_ptr].value;
        LRU_Refresh(api->m_LRU, stored_block);
        Longtail_UnlockSpinLock(api->m_Lock);
        return 0;
    }
    if (b->m_RefCount == 0)
    {
        struct Longtail_StoredBlock* original_stored_block = b->m_OriginalStoredBlock;
        if (original_stored_block->Dispose)
        {
            original_stored_block->Dispose(original_stored_block);
        }
    }
    Longtail_Free(b);
    return 0;
}

struct LRUStoredBlock* CreateLRUBlock(struct LRUBlockStoreAPI* api, struct Longtail_StoredBlock* original_stored_block)
{
    TLongtail_Hash block_hash = *original_stored_block->m_BlockIndex->m_BlockHash;
    struct LRUStoredBlock* allocated_block = (struct LRUStoredBlock*)Longtail_Alloc(sizeof(struct LRUStoredBlock));
    allocated_block->m_OriginalStoredBlock = original_stored_block;
    allocated_block->m_LRUBlockStoreAPI = api;
    allocated_block->m_StoredBlock.Dispose = LRUStoredBlock_Dispose;
    allocated_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    allocated_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    allocated_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    allocated_block->m_RefCount = 1;
    return allocated_block;
}

struct LRUStoredBlock* GetLRUBlock(struct LRUBlockStoreAPI* api, TLongtail_Hash block_hash)
{
    intptr_t tmp;
    intptr_t find_ptr = hmgeti_ts(api->m_BlockHashToLRUStoredBlock, block_hash, tmp);
    if (find_ptr == -1)
    {
        return 0;
    }
    struct LRUStoredBlock* stored_block = api->m_BlockHashToLRUStoredBlock[find_ptr].value;
//    LRU_Refresh(api->m_LRU, block_index);
    LONGTAIL_FATAL_ASSERT(stored_block->m_RefCount > 0, return 0)
    Longtail_AtomicAdd32(&stored_block->m_RefCount, 1);
    return stored_block;
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

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    int err = api->m_BackingBlockStore->PutStoredBlock(
        api->m_BackingBlockStore,
        stored_block,
        async_complete_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
    }
    return err;
}

static int LRUBlockStore_PreflightGet(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_PreflightGet(%p, %" PRIu64 ", %p)",
        block_store_api, chunk_count, chunk_hashes)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((chunk_count == 0) || (chunk_hashes != 0), return EINVAL)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);

    int err = api->m_BackingBlockStore->PreflightGet(
        api->m_BackingBlockStore,
        chunk_count,
        chunk_hashes);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_PreflightGet(%p, %" PRIu64 ", %p) failed with %d",
            block_store_api, chunk_count, chunk_hashes,
            err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount], 1);
    }
    return err;
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
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);

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
        LRUBlockStore_CompleteRequest(api);
        return;
    }

    struct LRUStoredBlock* shared_stored_block = CreateLRUBlock(api, stored_block);
    if (!shared_stored_block)
    {
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
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
        LRUBlockStore_CompleteRequest(api);
        return;
    }

    struct Longtail_AsyncGetStoredBlockAPI** list;
    struct Longtail_StoredBlock* dispose_block = 0;

    Longtail_LockSpinLock(api->m_Lock);
    if (api->m_LRU->m_AllocatedCount == api->m_LRU->m_MaxCount)
    {
        dispose_block = &LRU_Evict(api->m_LRU)->m_StoredBlock;
        hmdel(api->m_BlockHashToLRUStoredBlock, *dispose_block->m_BlockIndex->m_BlockHash);
    }
    *LRU_Put(api->m_LRU) = shared_stored_block;
    hmput(api->m_BlockHashToLRUStoredBlock, block_hash, shared_stored_block);

    list = hmget(api->m_BlockHashToCompleteCallbacks, block_hash);
    hmdel(api->m_BlockHashToCompleteCallbacks, block_hash);
    size_t wait_count = arrlen(list);
    Longtail_AtomicAdd32(&shared_stored_block->m_RefCount, (int32_t)wait_count);

    Longtail_UnlockSpinLock(api->m_Lock);

    if (dispose_block && dispose_block->Dispose)
    {
        dispose_block->Dispose(dispose_block);
    }

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *shared_stored_block->m_StoredBlock.m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*shared_stored_block->m_StoredBlock.m_BlockIndex->m_ChunkCount) + shared_stored_block->m_StoredBlock.m_BlockChunksDataSize);
    for (size_t i = 0; i < wait_count; ++i)
    {
        list[i]->OnComplete(list[i], &shared_stored_block->m_StoredBlock, 0);
    }
    arrfree(list);
    LRUBlockStore_CompleteRequest(api);
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
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    Longtail_LockSpinLock(api->m_Lock);

    struct LRUStoredBlock* lru_block = GetLRUBlock(api, block_hash);
    if (lru_block != 0 && lru_block->m_RefCount > 0)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *lru_block->m_StoredBlock.m_BlockIndex->m_ChunkCount);
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*lru_block->m_StoredBlock.m_BlockIndex->m_ChunkCount) + lru_block->m_StoredBlock.m_BlockChunksDataSize);
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
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
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
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        // We shortcut here since the logic to get from backing store is in OnComplete
        LRUBlockStore_AsyncGetStoredBlockAPI_OnComplete(&share_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI, 0, err);
        LRUBlockStore_CompleteRequest(api);
    }
    return 0;
}

static int LRUBlockStore_GetExistingContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint32_t min_block_usage_percent,
    struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_GetExistingContent(%p, %" PRIu64 ", %p, %p)",
        block_store_api, chunk_count, chunk_hashes, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count], 1);
    int err = api->m_BackingBlockStore->GetExistingContent(
        api->m_BackingBlockStore,
        chunk_count,
        chunk_hashes,
        min_block_usage_percent,
        async_complete_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "LRUBlockStore_GetExistingContent(%p, %" PRIu64 ", %p, %p) failed with %d",
            block_store_api, chunk_count, chunk_hashes, async_complete_api,
            err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount], 1);
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
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStats_Count], 1);
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        out_stats->m_StatU64[s] = api->m_StatU64[s];
    }
    return 0;
}

static int LRUBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_Flush(%p, %p)", block_store_api, async_complete_api)
    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_Count], 1);
    Longtail_LockSpinLock(api->m_Lock);
    if (api->m_PendingRequestCount > 0)
    {
        arrput(api->m_PendingAsyncFlushAPIs, async_complete_api);
        Longtail_UnlockSpinLock(api->m_Lock);
        return 0;
    }
    Longtail_UnlockSpinLock(api->m_Lock);
    async_complete_api->OnComplete(async_complete_api, 0);
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
    while (api->m_LRU->m_AllocatedCount > 0)
    {
        struct Longtail_StoredBlock* lru_block = &LRU_Evict(api->m_LRU)->m_StoredBlock;
        hmdel(api->m_BlockHashToLRUStoredBlock, *lru_block->m_BlockIndex->m_BlockHash);
        if (lru_block->Dispose)
        {
            lru_block->Dispose(lru_block);
        }
    }
    hmfree(api->m_BlockHashToCompleteCallbacks);
    hmfree(api->m_BlockHashToLRUStoredBlock);
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "LRUBlockStore_Init(%p, %p, %p)",
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
        LRUBlockStore_GetExistingContent,
        LRUBlockStore_GetStats,
        LRUBlockStore_Flush);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct LRUBlockStoreAPI* api = (struct LRUBlockStoreAPI*)block_store_api;
    api->m_BackingBlockStore = backing_block_store;
    api->m_BlockHashToLRUStoredBlock = 0;
    api->m_BlockHashToCompleteCallbacks = 0;
    api->m_PendingRequestCount = 0;
    api->m_PendingAsyncFlushAPIs = 0;

    api->m_LRU = LRU_Create(&api[1], max_lru_count);

    int err =Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateLRUBlockStoreAPI(%p, %p) failed with %d",
            api, backing_block_store,
            ENOMEM)
        return err;
    }

    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        api->m_StatU64[s] = 0;
    }

    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateLRUBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t max_lru_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateLRUBlockStoreAPI(%p)", backing_block_store)
    LONGTAIL_VALIDATE_INPUT(backing_block_store, return 0)

    size_t api_size =
        sizeof(struct LRUBlockStoreAPI) +
        sizeof(struct LRUStoredBlock) * max_lru_count +
        LRU_GetSize(max_lru_count);

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
