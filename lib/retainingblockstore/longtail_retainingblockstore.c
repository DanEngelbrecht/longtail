#include "longtail_retainingblockstore.h"

#include "../../src/longtail.h"
#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>


struct RetainingBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    uint64_t m_MaxBlockRetainCount;
};

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

    struct RetainingBlockStoreAPI* retainingblockstore_api = (struct RetainingBlockStoreAPI*)block_store_api;
    Longtail_Free(retainingblockstore_api);
}

static int RetainingBlockStore_Init(
    struct RetainingBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint64_t max_block_retain_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RetainingBlockStore_Dispose(%p, %p, %" PRIu64 ")", api, backing_block_store, max_block_retain_count)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(local_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = RetainingBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = RetainingBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.PreflightGet = RetainingBlockStore_PreflightGet;
    api->m_BlockStoreAPI.GetStoredBlock = RetainingBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = RetainingBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStats = RetainingBlockStore_GetStats;
    api->m_BackingBlockStore = backing_block_store;
    api->m_MaxBlockRetainCount = max_block_retain_count;
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
