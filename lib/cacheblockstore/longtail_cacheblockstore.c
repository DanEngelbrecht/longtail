#include "longtail_cacheblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>

struct CacheBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_LocalBlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_RemoteBlockStoreAPI;
};

struct PutStoredBlockPutRemoteComplete_API
{
    struct Longtail_AsyncCompleteAPI m_API;
    TLongtail_Atomic32 m_PendingCount;
    int m_RemoteErr;
    struct Longtail_AsyncCompleteAPI* m_AsyncCompleteAPI;
};

struct PutStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncCompleteAPI m_API;
    struct PutStoredBlockPutRemoteComplete_API* m_PutStoredBlockPutRemoteComplete_API;
};

int PutStoredBlockPutLocalComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
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

int PutStoredBlockPutRemoteComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "PutStoredBlockPutRemoteComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct PutStoredBlockPutRemoteComplete_API* api = (struct PutStoredBlockPutRemoteComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "PutStoredBlockPutRemoteComplete: Failed store block in remote block store, %d", err)
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
    struct Longtail_AsyncCompleteAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;

    struct PutStoredBlockPutRemoteComplete_API* put_stored_block_put_remote_complete_api = (struct PutStoredBlockPutRemoteComplete_API*)Longtail_Alloc(sizeof(struct PutStoredBlockPutRemoteComplete_API));
    put_stored_block_put_remote_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_remote_complete_api->m_API.OnComplete = PutStoredBlockPutRemoteComplete;
    put_stored_block_put_remote_complete_api->m_PendingCount = 2;
    put_stored_block_put_remote_complete_api->m_RemoteErr = EINVAL;
    put_stored_block_put_remote_complete_api->m_AsyncCompleteAPI = async_complete_api;
    int err = cacheblockstore_api->m_RemoteBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, stored_block, &put_stored_block_put_remote_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_PutStoredBlock: Failed async store block in remote block store, %d", err)
        Longtail_Free(put_stored_block_put_remote_complete_api);
        return err;
    }

    struct PutStoredBlockPutLocalComplete_API* put_stored_block_put_local_complete_api = (struct PutStoredBlockPutLocalComplete_API*)Longtail_Alloc(sizeof(struct PutStoredBlockPutLocalComplete_API));
    put_stored_block_put_local_complete_api->m_API.m_API.Dispose = 0;
    put_stored_block_put_local_complete_api->m_API.OnComplete = PutStoredBlockPutLocalComplete;
    put_stored_block_put_local_complete_api->m_PutStoredBlockPutRemoteComplete_API = put_stored_block_put_remote_complete_api;
    err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, stored_block, &put_stored_block_put_local_complete_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_PutStoredBlock: Failed async store block in local block store, %d", err)
        PutStoredBlockPutLocalComplete(&put_stored_block_put_local_complete_api->m_API, err);
        Longtail_Free(put_stored_block_put_local_complete_api);
    }
    return 0;
}

struct OnGetStoredBlockPutLocalComplete_API
{
    struct Longtail_AsyncCompleteAPI m_API;
    struct Longtail_AsyncCompleteAPI* async_complete_api;
};

int OnGetStoredBlockPutLocalComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockPutLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockPutLocalComplete_API* api = (struct OnGetStoredBlockPutLocalComplete_API*)async_complete_api;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockPutLocalComplete: Failed store block in local block store, %d", err)
    }
    api->async_complete_api->OnComplete(api->async_complete_api, 0);
    Longtail_Free(api);
    return 0;
}

struct OnGetStoredBlockGetRemoteComplete_API
{
    struct Longtail_AsyncCompleteAPI m_API;
    struct CacheBlockStoreAPI* cacheblockstore_api;
    struct Longtail_StoredBlock** out_stored_block;
    struct Longtail_AsyncCompleteAPI* async_complete_api;
};

int OnGetStoredBlockGetRemoteComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetRemoteComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockGetRemoteComplete_API* api = (struct OnGetStoredBlockGetRemoteComplete_API*)async_complete_api;
    if (err)
    {
        if (api->out_stored_block)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetRemoteComplete: Failed to get block from remote block store, %d", err)
        }
        api->async_complete_api->OnComplete(api->async_complete_api, err);
        Longtail_Free(api);
        return 0;
    }
    if (api->out_stored_block)
    {
        struct OnGetStoredBlockPutLocalComplete_API* put_local = (struct OnGetStoredBlockPutLocalComplete_API*)Longtail_Alloc(sizeof(struct OnGetStoredBlockPutLocalComplete_API));
        put_local->m_API.m_API.Dispose = 0;
        put_local->m_API.OnComplete = OnGetStoredBlockPutLocalComplete;
        put_local->async_complete_api = api->async_complete_api;
        err = api->cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(api->cacheblockstore_api->m_LocalBlockStoreAPI, *api->out_stored_block, &put_local->m_API);
        if (!err)
        {
            return 0;
        }
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetStoredBlockGetRemoteComplete: Failed store block in local block store, %d", err)
        Longtail_Free(put_local);
    }
    api->async_complete_api->OnComplete(api->async_complete_api, 0);
    Longtail_Free(api);
    return 0;
}

struct OnGetStoredBlockGetLocalComplete_API
{
    struct Longtail_AsyncCompleteAPI m_API;
    struct CacheBlockStoreAPI* cacheblockstore_api;
    uint64_t block_hash;
    struct Longtail_StoredBlock** out_stored_block;
    struct Longtail_AsyncCompleteAPI* async_complete_api;
};

int OnGetStoredBlockGetLocalComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetStoredBlockGetLocalComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetStoredBlockGetLocalComplete_API* api = (struct OnGetStoredBlockGetLocalComplete_API*)async_complete_api;
    if (err == ENOENT || err == EACCES)
    {
        struct OnGetStoredBlockGetRemoteComplete_API* on_get_stored_block_get_remote_complete = (struct OnGetStoredBlockGetRemoteComplete_API*)Longtail_Alloc(sizeof(struct OnGetStoredBlockGetRemoteComplete_API));
        on_get_stored_block_get_remote_complete->m_API.m_API.Dispose = 0;
        on_get_stored_block_get_remote_complete->m_API.OnComplete = OnGetStoredBlockGetRemoteComplete;
        on_get_stored_block_get_remote_complete->cacheblockstore_api = api->cacheblockstore_api;
        on_get_stored_block_get_remote_complete->out_stored_block = api->out_stored_block;
        on_get_stored_block_get_remote_complete->async_complete_api = api->async_complete_api;
        err = api->cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(
            api->cacheblockstore_api->m_RemoteBlockStoreAPI,
            api->block_hash,
            api->out_stored_block,
            &on_get_stored_block_get_remote_complete->m_API);
        if (err)
        {
            Longtail_Free(on_get_stored_block_get_remote_complete);
            api->async_complete_api->OnComplete(api->async_complete_api, err);
            Longtail_Free(api);
            return 0;
        }
        return 0;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetStoredBlockGetLocalComplete: Failed to get block from remote block store, %d", err)
    }
    api->async_complete_api->OnComplete(api->async_complete_api, err);
    Longtail_Free(api);
    return 0;
}

static int CacheBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_StoredBlock** out_stored_block,
    struct Longtail_AsyncCompleteAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_hash, out_stored_block, async_complete_api)
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    if (async_complete_api)
    {
        struct OnGetStoredBlockGetLocalComplete_API* on_get_stored_block_get_local_complete_api = (struct OnGetStoredBlockGetLocalComplete_API*)Longtail_Alloc(sizeof(struct OnGetStoredBlockGetLocalComplete_API));
        on_get_stored_block_get_local_complete_api->m_API.m_API.Dispose = 0;
        on_get_stored_block_get_local_complete_api->m_API.OnComplete = OnGetStoredBlockGetLocalComplete;
        on_get_stored_block_get_local_complete_api->cacheblockstore_api = cacheblockstore_api;
        on_get_stored_block_get_local_complete_api->block_hash = block_hash;
        on_get_stored_block_get_local_complete_api->out_stored_block = out_stored_block;
        on_get_stored_block_get_local_complete_api->async_complete_api = async_complete_api;
        int err = cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, out_stored_block, &on_get_stored_block_get_local_complete_api->m_API);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock: Failed async get block in local block store, %d", err)
            Longtail_Free(on_get_stored_block_get_local_complete_api);
            return err;
        }
        return 0;
    }

    int err = cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, out_stored_block, 0);
    if (err == 0)
    {
        return 0;
    }
    if (err != ENOENT)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock: Failed get block from local block store, %d", err)
        return err;
    }

    err = cacheblockstore_api->m_RemoteBlockStoreAPI->GetStoredBlock(cacheblockstore_api->m_RemoteBlockStoreAPI, block_hash, out_stored_block, 0);
    if (err == ENOENT)
    {
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CacheBlockStore_GetStoredBlock: Failed get block from remote block store, %d", err)
        return err;
    }
    if (out_stored_block)
    {
        err = cacheblockstore_api->m_LocalBlockStoreAPI->PutStoredBlock(cacheblockstore_api->m_LocalBlockStoreAPI, *out_stored_block, 0);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "CacheBlockStore_GetStoredBlock: Failed store block in local block store, %d", err)
        }
        return 0;
    }
    return 0;
}

static int CacheBlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_JobAPI* job_api, uint32_t default_hash_api_identifier, struct Longtail_ProgressAPI* progress_api, struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetIndex(%p, %p, %u, %p, %p)", block_store_api, job_api, default_hash_api_identifier, progress_api, out_content_index)
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    return cacheblockstore_api->m_RemoteBlockStoreAPI->GetIndex(cacheblockstore_api->m_RemoteBlockStoreAPI, job_api, default_hash_api_identifier, progress_api, out_content_index);
}

static int CacheBlockStore_GetStoredBlockPath(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_GetStoredBlockPath(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, out_path)
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_path, return EINVAL)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)block_store_api;
    return cacheblockstore_api->m_LocalBlockStoreAPI->GetStoredBlockPath(cacheblockstore_api->m_LocalBlockStoreAPI, block_hash, out_path);
}


static void CacheBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)
    struct CacheBlockStoreAPI* cacheblockstore_api = (struct CacheBlockStoreAPI*)api;
    Longtail_Free(cacheblockstore_api);
}

static int CacheBlockStore_Init(
    struct CacheBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* local_block_store,
	struct Longtail_BlockStoreAPI* remote_block_store)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CacheBlockStore_Dispose(%p, %p, %p)", api, local_block_store, remote_block_store)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(local_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return EINVAL)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateCacheBlockStoreAPI(%p, %p)", local_block_store, remote_block_store)
    LONGTAIL_FATAL_ASSERT(local_block_store, return 0)
    LONGTAIL_FATAL_ASSERT(remote_block_store, return 0)
    struct CacheBlockStoreAPI* api = (struct CacheBlockStoreAPI*)Longtail_Alloc(sizeof(struct CacheBlockStoreAPI));
    CacheBlockStore_Init(
        api,
        local_block_store,
        remote_block_store);
    return &api->m_BlockStoreAPI;
}
