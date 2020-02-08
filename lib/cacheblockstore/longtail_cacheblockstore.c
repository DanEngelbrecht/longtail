#include "longtail_cacheblockstore.h"

#include "../../src/longtail.h"

#include <errno.h>
#include <inttypes.h>

struct CacheBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_LocalBlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_RemoteBlockStoreAPI;
};

static int CacheBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block)
{
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    int err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock: Failed store block in local block store, %d", err)
        return err;
    }
    err = cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock: Failed store block in remote block store, %d", err)
        return err;
    }
    return 0;
}

static int CacheBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_StoredBlock** out_stored_block)
{
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    int err = cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, out_stored_block);
    if (err == 0)
    {
        return 0;
    }
    if (err != ENOENT)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock: Failed get block from local block store, %d", err)
        return err;
    }

    struct Longtail_StoredBlock* remote_stored_block;
    err = cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, block_hash, &remote_stored_block);
    if (err == ENOENT)
    {
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock: Failed get block from remote block store, %d", err)
        return err;
    }
    if (out_stored_block)
    {
        err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, remote_stored_block);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PutStoredBlock: Failed store block in local block store, %d", err)
        }
        *out_stored_block = remote_stored_block;
    }
    return 0;
}

static int CacheBlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, Longtail_JobAPI_ProgressFunc progress_func, void* progress_context, struct Longtail_ContentIndex** out_content_index)
{
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    return cacheblockstore_api->m_RemoteBlockStoreAPI->GetIndex(cacheblockstore_api->m_RemoteBlockStoreAPI, default_hash_api_identifier, progress_func, progress_context, out_content_index);
}

static int CacheBlockStore_GetStoredBlockPath(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path)
{
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    return cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlockPath(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, out_path);
}


static void CacheBlockStore_Dispose(struct Longtail_API* api)
{
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)api;
    Longtail_Free(cacheblockstore_api);
}

static int CacheBlockStore_Init(
    struct CacheBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* local_block_store,
	struct Longtail_BlockStoreAPI* remote_block_store)
{
    api->m_BlockStoreAPI.m_API.Dispose = CacheBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = CacheBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = CacheBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = CacheBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStoredBlockPath = CacheBlockStore_GetStoredBlockPath;
    api->m_LocalBlockStoreAPI = local_block_store;
    api->m_RemoteBlockStoreAPI = remote_block_store;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
    struct Longtail_BlockStoreAPI* local_block_store,
	struct Longtail_BlockStoreAPI* remote_block_store)
{
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)Longtail_Alloc(sizeof(struct CacheBlockStoreAPI));
    CacheBlockStore_Init(
        api,
        local_block_store,
        remote_block_store);
    return &api->m_BlockStoreAPI;
}
