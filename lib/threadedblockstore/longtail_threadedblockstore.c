#include "longtail_threadedblockstore.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"


#include <errno.h>
#include <inttypes.h>

struct ThreadBlockStoreAPI;

struct BlockHashToCompleteCallbacks
{
    TLongtail_Hash key;
    struct Longtail_AsyncGetStoredBlockAPI** value;
};

struct WorkerGetStoredBlockRequest
{
    // TODO: Could bunch up requests if a block is requested that is currently being processed (RestStoredBlock)
    TLongtail_Hash m_BlockHash;
    struct Longtail_AsyncGetStoredBlockAPI* m_AsyncCompleteAPI;
};

struct WorkerPutStoredBlockRequest
{
    struct Longtail_StoredBlock* m_StoredBlock;
    struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
};

struct WorkerGetIndexRequest
{
    struct Longtail_AsyncGetIndexAPI* m_AsyncCompleteAPI;
};

struct WorkerRetargetContentRequest
{
    struct Longtail_ContentIndex* m_ContentIndex;
    struct Longtail_AsyncRetargetContentAPI* m_AsyncCompleteAPI;
};

struct WorkerRequest
{
    union Request
    {
        struct WorkerGetStoredBlockRequest m_GetStoredBlock;
        struct WorkerPutStoredBlockRequest m_PutStoredBlock;
        struct WorkerGetIndexRequest m_GetIndex;
        struct WorkerRetargetContentRequest m_RetargetContent;
    } m_Request;
    enum Type
    {
        GET_STORED_BLOCK,
        PUT_STORED_BLOCK,
        GET_INDEX,
        RETARGET_CONTENT,
        EXIT
    } m_Type;
};

#define REQUEST_QUEUE_SIZE 256

struct ThreadBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    uint32_t m_WorkerCount;
    struct WorkerRequest m_RequestQueue[REQUEST_QUEUE_SIZE];
    uint64_t m_RequestQueueReadPos;
    uint64_t m_RequestQueueWritePos;
    HLongtail_SpinLock m_Lock;

    HLongtail_Sema m_RequestSemaphore;

    TLongtail_Atomic32 m_PendingRequestCount;

    HLongtail_Thread* m_WorkerThreads;
};

static int Worker(void* context)
{
    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)context;
    while (1)
    {
        int res = Longtail_WaitSema(api->m_RequestSemaphore, LONGTAIL_TIMEOUT_INFINITE);
        LONGTAIL_FATAL_ASSERT(0 == res, return res)

        struct WorkerRequest request;
        Longtail_LockSpinLock(api->m_Lock);
        if (api->m_RequestQueueReadPos == api->m_RequestQueueWritePos)
        {
            // TODO: This should never happen?
            continue;
        }
        request = api->m_RequestQueue[api->m_RequestQueueReadPos % REQUEST_QUEUE_SIZE];
        ++api->m_RequestQueueReadPos;
        Longtail_UnlockSpinLock(api->m_Lock);

        switch (request.m_Type)
        {
            case GET_STORED_BLOCK:
            {
                int err = api->m_BackingBlockStore->GetStoredBlock(
                    api->m_BackingBlockStore,
                    request.m_Request.m_GetStoredBlock.m_BlockHash,
                    request.m_Request.m_GetStoredBlock.m_AsyncCompleteAPI);
                if (err)
                {
                    request.m_Request.m_GetStoredBlock.m_AsyncCompleteAPI->OnComplete(request.m_Request.m_GetStoredBlock.m_AsyncCompleteAPI, 0, err);
                }
                break;
            }
            case PUT_STORED_BLOCK:
            {
                int err = api->m_BackingBlockStore->PutStoredBlock(
                    api->m_BackingBlockStore,
                    request.m_Request.m_PutStoredBlock.m_StoredBlock,
                    request.m_Request.m_PutStoredBlock.m_AsyncCompleteAPI);
                if (err)
                {
                    request.m_Request.m_PutStoredBlock.m_AsyncCompleteAPI->OnComplete(request.m_Request.m_PutStoredBlock.m_AsyncCompleteAPI, err);
                }
                break;
            }
            case GET_INDEX:
            {
                int err = api->m_BackingBlockStore->GetIndex(
                    api->m_BackingBlockStore,
                    request.m_Request.m_GetIndex.m_AsyncCompleteAPI);
                if (err)
                {
                    request.m_Request.m_GetIndex.m_AsyncCompleteAPI->OnComplete(request.m_Request.m_GetIndex.m_AsyncCompleteAPI, 0, err);
                }
                break;
            }
            case RETARGET_CONTENT:
            {
                int err = api->m_BackingBlockStore->RetargetContent(
                    api->m_BackingBlockStore,
                    request.m_Request.m_RetargetContent.m_ContentIndex,
                    request.m_Request.m_RetargetContent.m_AsyncCompleteAPI);
                if (err)
                {
                    request.m_Request.m_RetargetContent.m_AsyncCompleteAPI->OnComplete(request.m_Request.m_RetargetContent.m_AsyncCompleteAPI, 0, err);
                }
                break;
            }
            case EXIT:
                return 0;
        }

        Longtail_AtomicAdd32(&api->m_PendingRequestCount, -1);
    }
    return 0;
}

static int ThreadBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);

    Longtail_LockSpinLock(api->m_Lock);
    while (api->m_RequestQueueWritePos - api->m_RequestQueueReadPos == REQUEST_QUEUE_SIZE)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Sleep(100);
        Longtail_LockSpinLock(api->m_Lock);
    }
    struct WorkerRequest* request = &api->m_RequestQueue[api->m_RequestQueueWritePos % REQUEST_QUEUE_SIZE];
    request->m_Type = PUT_STORED_BLOCK;
    request->m_Request.m_PutStoredBlock.m_StoredBlock = stored_block;
    request->m_Request.m_PutStoredBlock.m_AsyncCompleteAPI = async_complete_api;
    ++api->m_RequestQueueWritePos;
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_PostSema(api->m_RequestSemaphore, 1);
    return 0;
}

static int ThreadBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_hashes, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_ref_counts, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    return api->m_BackingBlockStore->PreflightGet(
        api->m_BackingBlockStore,
        block_count,
        block_hashes,
        block_ref_counts);
}

static int ThreadBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)",
        block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);

    Longtail_LockSpinLock(api->m_Lock);
    while (api->m_RequestQueueWritePos - api->m_RequestQueueReadPos == REQUEST_QUEUE_SIZE)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Sleep(100);
        Longtail_LockSpinLock(api->m_Lock);
    }
    struct WorkerRequest* request = &api->m_RequestQueue[api->m_RequestQueueWritePos % REQUEST_QUEUE_SIZE];
    request->m_Type = GET_STORED_BLOCK;
    request->m_Request.m_GetStoredBlock.m_BlockHash = block_hash;
    request->m_Request.m_GetStoredBlock.m_AsyncCompleteAPI = async_complete_api;
    ++api->m_RequestQueueWritePos;
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_PostSema(api->m_RequestSemaphore, 1);
    return 0;
}

static int ThreadBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_GetIndex(%p, %u, %p)", block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api->OnComplete, return EINVAL)

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);

    Longtail_LockSpinLock(api->m_Lock);
    while (api->m_RequestQueueWritePos - api->m_RequestQueueReadPos == REQUEST_QUEUE_SIZE)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Sleep(100);
        Longtail_LockSpinLock(api->m_Lock);
    }
    struct WorkerRequest* request = &api->m_RequestQueue[api->m_RequestQueueWritePos % REQUEST_QUEUE_SIZE];
    request->m_Type = GET_INDEX;
    request->m_Request.m_GetIndex.m_AsyncCompleteAPI = async_complete_api;
    ++api->m_RequestQueueWritePos;
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_PostSema(api->m_RequestSemaphore, 1);
    return 0;
}

static int ThreadBlockStore_RetargetContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_RetargetContent(%p, %p, %p)",
        block_store_api, content_index, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd32(&api->m_PendingRequestCount, 1);

    Longtail_LockSpinLock(api->m_Lock);
    while (api->m_RequestQueueWritePos - api->m_RequestQueueReadPos == REQUEST_QUEUE_SIZE)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Sleep(100);
        Longtail_LockSpinLock(api->m_Lock);
    }
    struct WorkerRequest* request = &api->m_RequestQueue[api->m_RequestQueueWritePos % REQUEST_QUEUE_SIZE];
    request->m_Type = RETARGET_CONTENT;
    request->m_Request.m_RetargetContent.m_ContentIndex = content_index;
    request->m_Request.m_RetargetContent.m_AsyncCompleteAPI = async_complete_api;
    ++api->m_RequestQueueWritePos;
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_PostSema(api->m_RequestSemaphore, 1);
    return 0;
}

static int ThreadBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

static void ThreadBlockStore_Dispose(struct Longtail_API* base_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_Dispose(%p)", base_api)
    LONGTAIL_FATAL_ASSERT(base_api, return)

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)base_api;
    while (api->m_PendingRequestCount > 0)
    {
        Longtail_Sleep(1000);
        if (api->m_PendingRequestCount > 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_Dispose(%p) waiting for %d pending requests", api, (int32_t)api->m_PendingRequestCount);
        }
    }

    Longtail_LockSpinLock(api->m_Lock);
    while ((api->m_RequestQueueReadPos + api->m_WorkerCount - 1) == api->m_RequestQueueWritePos)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Sleep(100);
        Longtail_LockSpinLock(api->m_Lock);
    }
    for (uint32_t w = 0; w < api->m_WorkerCount; ++w)
    {
        struct WorkerRequest* request = &api->m_RequestQueue[(api->m_RequestQueueWritePos + w) % REQUEST_QUEUE_SIZE];
        request->m_Type = EXIT;
    }
    api->m_RequestQueueWritePos += api->m_WorkerCount;
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_PostSema(api->m_RequestSemaphore, api->m_WorkerCount);

    for (uint32_t t = 0; t < api->m_WorkerCount; ++t)
    {
        Longtail_JoinThread(api->m_WorkerThreads[t], LONGTAIL_TIMEOUT_INFINITE);
        Longtail_DeleteThread(api->m_WorkerThreads[t]);
    }

    Longtail_Free(api->m_WorkerThreads);
    Longtail_DeleteSema(api->m_RequestSemaphore);
    Longtail_Free(api->m_RequestSemaphore);
    Longtail_DeleteSpinLock(api->m_Lock);
    Longtail_Free(api->m_Lock);
    Longtail_Free(api);
}

static int ThreadBlockStore_Init(
    void* mem,
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t thread_count,
    int thread_priority,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ThreadBlockStore_Dispose(%p, %p, %p)",
        mem, backing_block_store, out_block_store_api)
    LONGTAIL_FATAL_ASSERT(mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        ThreadBlockStore_Dispose,
        ThreadBlockStore_PutStoredBlock,
        ThreadBlockStore_PreflightGet,
        ThreadBlockStore_GetStoredBlock,
        ThreadBlockStore_GetIndex,
        ThreadBlockStore_RetargetContent,
        ThreadBlockStore_GetStats);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct ThreadBlockStoreAPI* api = (struct ThreadBlockStoreAPI*)block_store_api;
    api->m_BackingBlockStore = backing_block_store;
    api->m_WorkerCount = thread_count;
    api->m_RequestQueueReadPos = 0;
    api->m_RequestQueueWritePos = 0;
    int err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateThreadedBlockStoreAPI(%p, %p) failed with %d",
            api, backing_block_store,
            ENOMEM)
        return err;
    }
    err = Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &api->m_RequestSemaphore);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateThreadedBlockStoreAPI(%p, %p) failed with %d",
            api, backing_block_store,
            ENOMEM)
        Longtail_DeleteSpinLock(api->m_Lock);
        Longtail_Free(api->m_Lock);
        return err;
    }
    api->m_PendingRequestCount = 0;

    api->m_WorkerThreads = (HLongtail_Thread*)Longtail_Alloc(sizeof(HLongtail_Thread) * thread_count + Longtail_GetThreadSize() * thread_count);
    // TODO: Check OOM
    uint8_t* thread_mem = (uint8_t*)&api->m_WorkerThreads[thread_count];
    for (uint32_t t = 0; t < thread_count; ++t)
    {
        int res = Longtail_CreateThread(thread_mem, Worker, 0, api, thread_priority, &api->m_WorkerThreads[t]);
        // TODO: Check OOM
        thread_mem += Longtail_GetThreadSize();
    }
    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateThreadedBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t thread_count,
    int thread_priority)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateThreadedBlockStoreAPI(%p)", backing_block_store)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return 0)

    size_t api_size = sizeof(struct ThreadBlockStoreAPI);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateThreadedBlockStoreAPI(%p) failed with %d",
            backing_block_store,
            ENOMEM)
        return 0;
    }
    struct Longtail_BlockStoreAPI* block_store_api;
    int err = ThreadBlockStore_Init(
        mem,
        backing_block_store,
        thread_count,
        thread_priority,
        &block_store_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateThreadedBlockStoreAPI(%p) failed with %d",
            backing_block_store,
            err)
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
