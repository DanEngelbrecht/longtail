#include "longtail_retainingblockstore.h"

#include "../../src/longtail.h"
#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>

struct RetainingBlockStoreAPI;

struct RetainedStoredBlock
{
    struct Longtail_StoredBlock m_StoredBlock;
    struct Longtail_StoredBlock* m_OriginalStoredBlock;
    struct RetainingBlockStoreAPI* m_RetainingBlockStoreAPI;
};

struct BlockHashToRetainedIndex
{
    TLongtail_Hash key;
    uint64_t value;
};

struct RetainingBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    HLongtail_SpinLock m_Lock;
    uint64_t m_RetainedBlockCount;
    struct BlockHashToRetainedIndex* m_BlockHashToRetainedIndex;
    TLongtail_Hash* m_BlockHashes;
    TLongtail_Atomic32* m_BlockRetainCounts;
    struct RetainedStoredBlock** m_RetainedStoredBlocks;
};

int RetainedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    struct RetainedStoredBlock* b = (struct RetainedStoredBlock*)stored_block;
    struct RetainingBlockStoreAPI* retainingblockstore_api = b->m_RetainingBlockStoreAPI;
    TLongtail_Hash block_hash = *b->m_StoredBlock.m_BlockIndex->m_BlockHash;
    intptr_t tmp;
    uint64_t block_index = hmget_ts(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, tmp);
    TLongtail_Atomic32* retain_count_ptr = &retainingblockstore_api->m_BlockRetainCounts[block_index];
    if (Longtail_AtomicAdd32(retain_count_ptr, -1) != 0)
    {
        return 0;
    }
    LONGTAIL_FATAL_ASSERT(retainingblockstore_api->m_RetainedStoredBlocks[block_index], return EINVAL)
    retainingblockstore_api->m_RetainedStoredBlocks[block_index] = 0;
    struct Longtail_StoredBlock* original_stored_block = b->m_OriginalStoredBlock;
    Longtail_Free(b);
    original_stored_block->Dispose(original_stored_block);
    return 0;
}

struct RetainedStoredBlock* RetainedStoredBlock_CreateBlock(struct RetainingBlockStoreAPI* retaining_block_store_api, struct Longtail_StoredBlock* original_stored_block)
{
    size_t retained_stored_block_size = sizeof(struct RetainedStoredBlock);
    struct RetainedStoredBlock* retained_stored_block = (struct RetainedStoredBlock*)Longtail_Alloc(retained_stored_block_size);
    if (!retained_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RetainedStoredBlock_CreateBlock(%p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            retaining_block_store_api, original_stored_block,
            retained_stored_block_size,
            ENOMEM)
        return 0;
    }
    retained_stored_block->m_StoredBlock.Dispose = RetainedStoredBlock_Dispose;
    retained_stored_block->m_StoredBlock.m_BlockIndex = original_stored_block->m_BlockIndex;
    retained_stored_block->m_StoredBlock.m_BlockData = original_stored_block->m_BlockData;
    retained_stored_block->m_StoredBlock.m_BlockChunksDataSize = original_stored_block->m_BlockChunksDataSize;
    retained_stored_block->m_OriginalStoredBlock = original_stored_block;
    retained_stored_block->m_RetainingBlockStoreAPI = retaining_block_store_api;
    return retained_stored_block;
}

static int RetainingBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;

    return retainingblockstore_api->m_BackingBlockStore->PutStoredBlock(
        retainingblockstore_api->m_BackingBlockStore,
        stored_block,
        async_complete_api);
}

static int RetainingBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_hashes, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_ref_counts, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    int err = retainingblockstore_api->m_BackingBlockStore->PreflightGet(
        retainingblockstore_api->m_BackingBlockStore,
        block_count,
        block_hashes,
        block_ref_counts);
    if (err)
    {
        return err;
    }
    uint64_t retain_block_count = 0;
    for (uint64_t b = 0; b < block_count; ++b)
    {
        uint32_t retain_count = block_ref_counts[b];
        if (retain_count > 1)
        {
            ++retain_block_count;
        }
    }

    size_t block_hashes_size = sizeof(TLongtail_Hash) * retain_block_count;
    retainingblockstore_api->m_BlockHashes = (TLongtail_Hash*)Longtail_Alloc(block_hashes_size);
    if (!retainingblockstore_api->m_BlockHashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RetainingBlockStore_PreflightGet(%p, %" PRIu64 ", %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            block_hashes_size,
            ENOMEM)
        return ENOMEM;
    }

    size_t block_retain_counts_size = sizeof(TLongtail_Atomic32) * retain_block_count;
    retainingblockstore_api->m_BlockRetainCounts = (TLongtail_Atomic32*)Longtail_Alloc(block_retain_counts_size);
    if (!retainingblockstore_api->m_BlockRetainCounts)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RetainingBlockStore_PreflightGet(%p, %" PRIu64 ", %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            block_retain_counts_size,
            ENOMEM)
        Longtail_Free(retainingblockstore_api->m_BlockHashes);
        return ENOMEM;
    }

    size_t retained_stored_blocks_size = sizeof(struct RetainedStoredBlock*) * retain_block_count;
    retainingblockstore_api->m_RetainedStoredBlocks = (struct RetainedStoredBlock**)Longtail_Alloc(retained_stored_blocks_size);
    if (!retainingblockstore_api->m_RetainedStoredBlocks)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RetainingBlockStore_PreflightGet(%p, %" PRIu64 ", %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, block_count, block_hashes, block_ref_counts,
            retained_stored_blocks_size,
            ENOMEM)
        Longtail_Free((void*)retainingblockstore_api->m_BlockRetainCounts);
        Longtail_Free(retainingblockstore_api->m_BlockHashes);
        return ENOMEM;
    }

    uint64_t retained_block_count = 0;
    for (uint64_t b = 0; b < block_count; ++b)
    {
        uint32_t retain_count = block_ref_counts[b];
        if (retain_count > 1)
        {
            TLongtail_Hash block_hash = block_hashes[b];
            hmput(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, retained_block_count);
            retainingblockstore_api->m_BlockHashes[retained_block_count] = block_hash;
            retainingblockstore_api->m_BlockRetainCounts[retained_block_count] = retain_count;
            retainingblockstore_api->m_RetainedStoredBlocks[retained_block_count] = 0;
            retained_block_count++;
        }
    }
    retainingblockstore_api->m_RetainedBlockCount = retained_block_count;
    return 0;
}

struct RetainingBlockStore_AsyncGetStoredBlockAPI
{
    struct Longtail_AsyncGetStoredBlockAPI m_AsyncGetStoredBlockAPI;
    struct RetainingBlockStoreAPI* m_RetainingblockstoreAPI;
    struct Longtail_AsyncGetStoredBlockAPI* m_BaseAsyncGetStoredBlockAPI;
};

static void RetainingBlockStore_AsyncGetStoredBlockAPI_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return)
    struct RetainingBlockStore_AsyncGetStoredBlockAPI* api = (struct RetainingBlockStore_AsyncGetStoredBlockAPI*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(api->m_RetainingblockstoreAPI != 0, return)
    LONGTAIL_FATAL_ASSERT(api->m_BaseAsyncGetStoredBlockAPI != 0, return)
    LONGTAIL_FATAL_ASSERT(api->m_BaseAsyncGetStoredBlockAPI->OnComplete, return)

    struct RetainingBlockStoreAPI* retainingblockstore_api = api->m_RetainingblockstoreAPI;
    struct Longtail_AsyncGetStoredBlockAPI* base_async_get_stored_block_api = api->m_BaseAsyncGetStoredBlockAPI;
    Longtail_Free(api);

    if (err)
    {
        base_async_get_stored_block_api->OnComplete(base_async_get_stored_block_api, stored_block, err);
        return;
    }

    TLongtail_Hash block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    intptr_t tmp;
    intptr_t block_index_ptr = hmgeti_ts(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, tmp);
    if (block_index_ptr == -1)
    {
        base_async_get_stored_block_api->OnComplete(base_async_get_stored_block_api, stored_block, err);
        return;
    }
    uint64_t block_index = retainingblockstore_api->m_BlockHashToRetainedIndex[block_index_ptr].value;

    Longtail_LockSpinLock(retainingblockstore_api->m_Lock);
    struct RetainedStoredBlock* retained_stored_block = retainingblockstore_api->m_RetainedStoredBlocks[block_index];
    Longtail_UnlockSpinLock(retainingblockstore_api->m_Lock);
    if (retained_stored_block)
    {
        base_async_get_stored_block_api->OnComplete(base_async_get_stored_block_api, &retained_stored_block->m_StoredBlock, 0);
        return;
    }
    retained_stored_block = RetainedStoredBlock_CreateBlock(retainingblockstore_api, stored_block);
    if (!retained_stored_block)
    {
        base_async_get_stored_block_api->OnComplete(base_async_get_stored_block_api, stored_block, 0);
        return;
    }

    Longtail_LockSpinLock(retainingblockstore_api->m_Lock);
    if (retainingblockstore_api->m_RetainedStoredBlocks[block_index])
    {
        Longtail_Free(retained_stored_block);
        retained_stored_block = retainingblockstore_api->m_RetainedStoredBlocks[block_index];
    }
    else
    {
        retainingblockstore_api->m_RetainedStoredBlocks[block_index] = retained_stored_block;
    }
    Longtail_UnlockSpinLock(retainingblockstore_api->m_Lock);
    base_async_get_stored_block_api->OnComplete(base_async_get_stored_block_api, &retained_stored_block->m_StoredBlock, 0);
}



static int RetainingBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)
    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;

    intptr_t tmp;
    intptr_t block_index_ptr = hmgeti_ts(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, tmp);
    if (block_index_ptr != -1)
    {
        uint64_t block_index = retainingblockstore_api->m_BlockHashToRetainedIndex[block_index_ptr].value;
        LONGTAIL_FATAL_ASSERT(block_index < retainingblockstore_api->m_RetainedBlockCount, return EINVAL);
        LONGTAIL_FATAL_ASSERT(retainingblockstore_api->m_BlockRetainCounts[block_index] > 0, return EINVAL);

        Longtail_LockSpinLock(retainingblockstore_api->m_Lock);
        struct RetainedStoredBlock* stored_block = retainingblockstore_api->m_RetainedStoredBlocks[block_index];
        if (stored_block)
        {
            Longtail_UnlockSpinLock(retainingblockstore_api->m_Lock);
            async_complete_api->OnComplete(async_complete_api, &stored_block->m_StoredBlock, 0);
            return 0;
        }
        Longtail_UnlockSpinLock(retainingblockstore_api->m_Lock);
        size_t retaining_lock_store_async_get_stored_block_API_size = sizeof(struct RetainingBlockStore_AsyncGetStoredBlockAPI);
        struct RetainingBlockStore_AsyncGetStoredBlockAPI* retaining_lock_store_async_get_stored_block_API = (struct RetainingBlockStore_AsyncGetStoredBlockAPI*)Longtail_Alloc(retaining_lock_store_async_get_stored_block_API_size);
        if (!retaining_lock_store_async_get_stored_block_API)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RetainingBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
                block_store_api, block_hash, async_complete_api,
                retaining_lock_store_async_get_stored_block_API_size,
                ENOMEM)
            return ENOMEM;
        }
        retaining_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.m_API.Dispose = 0;
        retaining_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI.OnComplete = RetainingBlockStore_AsyncGetStoredBlockAPI_OnComplete;
        retaining_lock_store_async_get_stored_block_API->m_RetainingblockstoreAPI = retainingblockstore_api;
        retaining_lock_store_async_get_stored_block_API->m_BaseAsyncGetStoredBlockAPI = async_complete_api;

        int err = retainingblockstore_api->m_BackingBlockStore->GetStoredBlock(
            retainingblockstore_api->m_BackingBlockStore,
            block_hash,
            &retaining_lock_store_async_get_stored_block_API->m_AsyncGetStoredBlockAPI);
        if (err)
        {
            // TODO: Log
            Longtail_Free(retaining_lock_store_async_get_stored_block_API);
            return err;
        }
        return 0;
    }
    return retainingblockstore_api->m_BackingBlockStore->GetStoredBlock(
        retainingblockstore_api->m_BackingBlockStore,
        block_hash,
        async_complete_api);
}

static int RetainingBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_GetIndex(%p, %u, %p)", block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    return retainingblockstore_api->m_BackingBlockStore->GetIndex(
        retainingblockstore_api->m_BackingBlockStore,
        async_complete_api);
}

static int RetainingBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

static void RetainingBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)api;
    for (uint64_t b = 0; b < retainingblockstore_api->m_RetainedBlockCount; ++b)
    {
        LONGTAIL_FATAL_ASSERT(retainingblockstore_api->m_BlockRetainCounts[b] == 0, return)
        struct RetainedStoredBlock* retained_block = retainingblockstore_api->m_RetainedStoredBlocks[b];
        if (retained_block)
        {
            struct Longtail_StoredBlock* original_stored_block = retained_block->m_OriginalStoredBlock;
            Longtail_Free(retained_block);
            original_stored_block->Dispose(original_stored_block);
        }
    }
    hmfree(retainingblockstore_api->m_BlockHashToRetainedIndex);
    Longtail_Free(retainingblockstore_api->m_BlockHashes);
    Longtail_Free((void*)retainingblockstore_api->m_BlockRetainCounts);
    Longtail_Free(retainingblockstore_api->m_RetainedStoredBlocks);
    Longtail_DeleteSpinLock(retainingblockstore_api->m_Lock);
    Longtail_Free(retainingblockstore_api->m_Lock);
    Longtail_Free(retainingblockstore_api);
}

static int RetainingBlockStore_Init(
    struct RetainingBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RetainingBlockStore_Dispose(%p, %p, %" PRIu64 ")", api, backing_block_store)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = RetainingBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = RetainingBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = RetainingBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = RetainingBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = RetainingBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = RetainingBlockStore_GetStats;
    api->m_BackingBlockStore = backing_block_store;
    int err =Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        return err;
    }
    api->m_RetainedBlockCount = 0;
    api->m_BlockHashToRetainedIndex = 0;
    api->m_BlockHashes = 0;
    api->m_BlockRetainCounts = 0;
    api->m_RetainedStoredBlocks = 0;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateRetainingBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateRetainingBlockStoreAPI(%p)", backing_block_store)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return 0)

    size_t api_size = sizeof(struct RetainingBlockStoreAPI);
    struct RetainingBlockStoreAPI* api = (struct RetainingBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateRetainingBlockStoreAPI(%p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            backing_block_store,
            api_size,
            ENOMEM)
        return 0;
    }
    int err = RetainingBlockStore_Init(
        api,
        backing_block_store);
    if (err)
    {
        Longtail_Free(api);
        return 0;
    }
    return &api->m_BlockStoreAPI;
}
