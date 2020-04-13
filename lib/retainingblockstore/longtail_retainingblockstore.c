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
    uint64_t m_MaxBlockRetainCount;
    uint64_t m_RetainedBlockCount;
    struct BlockHashToRetainedIndex* m_BlockHashToRetainedIndex;
    TLongtail_Hash* m_BlockHashes;
    uint32_t* m_BlockRetainCounts;
    struct RetainedStoredBlock** m_RetainedStoredBlocks;
};

int RetainedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    struct RetainedStoredBlock* b = (struct RetainedStoredBlock*)stored_block;
    TLongtail_Hash block_hash = *b->m_StoredBlock.m_BlockIndex->m_BlockHash;
    intptr_t tmp;
    uint64_t block_index = hmget_ts(b->m_RetainingBlockStoreAPI->m_BlockHashToRetainedIndex, block_hash, tmp);
    uint32_t* retain_count_ptr = &b->m_RetainingBlockStoreAPI->m_BlockRetainCounts[block_index];
    if (--(*retain_count_ptr))
    {
        return 0;
    }
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
        // TODO: Log
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
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

    retainingblockstore_api->m_BlockHashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * retain_block_count);
    retainingblockstore_api->m_BlockRetainCounts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * retain_block_count);
    retainingblockstore_api->m_RetainedStoredBlocks = (struct RetainedStoredBlock**)Longtail_Alloc(sizeof(struct RetainedStoredBlock*) * retain_block_count);

    for (uint64_t b = 0; b < block_count; ++b)
    {
        uint32_t retain_count = block_ref_counts[b];
        if (retain_count > 1)
        {
            TLongtail_Hash block_hash = block_hashes[b];
            hmput(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, retainingblockstore_api->m_RetainedBlockCount);
            retainingblockstore_api->m_BlockHashes[retainingblockstore_api->m_RetainedBlockCount] = block_hash;
            retainingblockstore_api->m_BlockRetainCounts[retainingblockstore_api->m_RetainedBlockCount] = retain_count;
            retainingblockstore_api->m_RetainedStoredBlocks[retainingblockstore_api->m_RetainedBlockCount] = 0;
            retainingblockstore_api->m_RetainedBlockCount++;
        }
    }
    return 0;
}

static int RetainingBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;

    intptr_t tmp;
    intptr_t block_index = hmgeti_ts(retainingblockstore_api->m_BlockHashToRetainedIndex, block_hash, tmp);
    if (block_index != -1)
    {
        struct RetainedStoredBlock* stored_block = retainingblockstore_api->m_RetainedStoredBlocks[block_index];
        if (stored_block)
        {
            int err = async_complete_api->OnComplete(async_complete_api, &stored_block->m_StoredBlock, 0);
            return err;
        }
        if (retainingblockstore_api->m_BlockRetainCounts[block_index] > 1)
        {
            // TODO: Here we should have our own on-complete so we can retain the block
//            struct RetainedStoredBlock* retained_stored_block = RetainedStoredBlock_CreateBlock(retainingblockstore_api, 0);
//            if (retained_stored_block)
//            {
//                retainingblockstore_api->m_RetainedStoredBlocks[block_index] = retained_stored_block;
//                return &retained_stored_block->m_StoredBlock;
//            }
        }
    }

    return retainingblockstore_api->m_BackingBlockStore->GetStoredBlock(
        retainingblockstore_api->m_BackingBlockStore,
        block_hash,
        async_complete_api);
}

static int RetainingBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_GetIndex(%p, %u, %p)", block_store_api, default_hash_api_identifier, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    return retainingblockstore_api->m_BackingBlockStore->GetIndex(
        retainingblockstore_api->m_BackingBlockStore,
        default_hash_api_identifier,
        async_complete_api);
}

static int RetainingBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

static void RetainingBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)api;
    for (uint64_t b = 0; b < retainingblockstore_api->m_RetainedBlockCount; ++b)
    {
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
    Longtail_Free(retainingblockstore_api->m_BlockRetainCounts);
    Longtail_Free(retainingblockstore_api->m_RetainedStoredBlocks);
    Longtail_Free(retainingblockstore_api);
}

static int RetainingBlockStore_Init(
    struct RetainingBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint64_t max_block_retain_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_Dispose(%p, %p, %" PRIu64 ")", api, backing_block_store, max_block_retain_count)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_block_retain_count > 0, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = RetainingBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = RetainingBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = RetainingBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = RetainingBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = RetainingBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = RetainingBlockStore_GetStats;
    api->m_BackingBlockStore = backing_block_store;
    api->m_MaxBlockRetainCount = max_block_retain_count;
    api->m_RetainedBlockCount = 0;
    api->m_BlockHashToRetainedIndex = 0;
    api->m_BlockHashes = 0;
    api->m_BlockRetainCounts = 0;
    api->m_RetainedStoredBlocks = 0;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateRetainingBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store,
	uint64_t max_block_retain_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateRetainingBlockStoreAPI(%p, %" PRIu64 ")", backing_block_store, max_block_retain_count)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return 0)

    size_t api_size = sizeof(struct RetainingBlockStoreAPI);
    struct RetainingBlockStoreAPI* api = (struct RetainingBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateRetainingBlockStoreAPI(%p, %" PRIu64 ") Longtail_Alloc(%" PRIu64 ") failed with, %d",
            backing_block_store, max_block_retain_count,
            api_size,
            ENOMEM)
        return 0;
    }
    RetainingBlockStore_Init(
        api,
        backing_block_store,
        max_block_retain_count);
    return &api->m_BlockStoreAPI;
}
