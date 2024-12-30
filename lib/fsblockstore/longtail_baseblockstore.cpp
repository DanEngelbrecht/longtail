#include "longtail_fsblockstore.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"
#include "../longtail_sha1.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_NAME_LENGTH   23

static const char* HashLUT = "0123456789abcdef";

static void GetBlockName(TLongtail_Hash block_hash, char* out_name)
{
    LONGTAIL_FATAL_ASSERT(0, out_name, return)
        out_name[7] = HashLUT[(block_hash >> 60) & 0xf];
    out_name[8] = HashLUT[(block_hash >> 56) & 0xf];
    out_name[9] = HashLUT[(block_hash >> 52) & 0xf];
    out_name[10] = HashLUT[(block_hash >> 48) & 0xf];
    out_name[11] = HashLUT[(block_hash >> 44) & 0xf];
    out_name[12] = HashLUT[(block_hash >> 40) & 0xf];
    out_name[13] = HashLUT[(block_hash >> 36) & 0xf];
    out_name[14] = HashLUT[(block_hash >> 32) & 0xf];
    out_name[15] = HashLUT[(block_hash >> 28) & 0xf];
    out_name[16] = HashLUT[(block_hash >> 24) & 0xf];
    out_name[17] = HashLUT[(block_hash >> 20) & 0xf];
    out_name[18] = HashLUT[(block_hash >> 16) & 0xf];
    out_name[19] = HashLUT[(block_hash >> 12) & 0xf];
    out_name[20] = HashLUT[(block_hash >> 8) & 0xf];
    out_name[21] = HashLUT[(block_hash >> 4) & 0xf];
    out_name[22] = HashLUT[(block_hash >> 0) & 0xf];
    out_name[0] = out_name[7];
    out_name[1] = out_name[8];
    out_name[2] = out_name[9];
    out_name[3] = out_name[10];
    out_name[4] = '/';
    out_name[5] = '0';
    out_name[6] = 'x';
}

#define SHA1_NAME_LENGTH   (SHA1_BLOCK_SIZE * 2)

static void SHA1String(unsigned char SHA[SHA1_BLOCK_SIZE], char out_name[SHA1_NAME_LENGTH])
{
    LONGTAIL_FATAL_ASSERT(0, out_name, return)
    for (size_t i = 0; i < SHA1_BLOCK_SIZE; i++)
    {
        *out_name++ = HashLUT[(SHA[i] >> 4) & 0xf];
        *out_name++ = HashLUT[SHA[i] & 0xf];
    }
}

static char* GetBlobPath(
    const char* block_extension,
    TLongtail_Hash block_hash)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_extension, "%s"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64)
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, block_extension, return 0)

    char file_name[7 + BLOCK_NAME_LENGTH + 4 + 1];
    strcpy(file_name, "chunks/");
    GetBlockName(block_hash, &file_name[7]);
    strcpy(&file_name[7 + BLOCK_NAME_LENGTH], block_extension);
    return Longtail_Strdup(file_name);
}

class BaseBlockStore
{
public:
    BaseBlockStore(struct Longtail_PersistenceAPI* PersistanceAPI,
        struct Longtail_StorageAPI* cache_storage_api,
        const char* cache_base_path);
    ~BaseBlockStore();

    int PutStoredBlock(struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
    int PreflightGet(uint32_t block_count, const TLongtail_Hash* block_hashes, struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api);
    int GetStoredBlock(uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
    int GetExistingContent(uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_AsyncGetExistingContentAPI* async_complete_api);
    int PruneBlocks(uint32_t block_keep_count, const TLongtail_Hash* block_keep_hashes, struct Longtail_AsyncPruneBlocksAPI* async_complete_api);
    int GetStats(struct Longtail_BlockStore_Stats* out_stats);
    int Flush(struct Longtail_AsyncFlushAPI* async_complete_api);

    operator struct Longtail_BlockStoreAPI*() { return &m_API.m_BlockStoreAPI; };
private:
    struct API
    {
        struct Longtail_BlockStoreAPI m_BlockStoreAPI;
        BaseBlockStore* m_Owner;
    } m_API;

    struct Longtail_PersistenceAPI* m_Persistance = 0;
    struct Longtail_StorageAPI* m_CacheStorage = 0;
    const char* m_CacheBasePath = 0;

    struct Longtail_BlockIndex** m_AddedBlockIndexes = 0;

    struct BlockHashToBlockState
    {
        struct V
        {
            size_t size;
            void* buffer;
        };
        uint64_t key;
        V value;
    };

    struct BlockHashToBlockState* m_BlockState = 0;


    static void BaseBlockStore_Dispose(struct Longtail_API* longtail_api)
    {
        struct API* api = (struct API*)(longtail_api);
        delete api->m_Owner;
    }

    static int BaseBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->PutStoredBlock(stored_block, async_complete_api);
    }

    static int BaseBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->PreflightGet(chunk_count, chunk_hashes, optional_async_complete_api);
    }

    static int BaseBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->GetStoredBlock(block_hash, async_complete_api);
    }

    static int BaseBlockStore_GetExistingContent(struct Longtail_BlockStoreAPI* block_store_api, uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->GetExistingContent(chunk_count, chunk_hashes, min_block_usage_percent, async_complete_api);
    }

    static int BaseBlockStore_PruneBlocks(struct Longtail_BlockStoreAPI* block_store_api, uint32_t block_keep_count, const TLongtail_Hash* block_keep_hashes, struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->PruneBlocks(block_keep_count, block_keep_hashes, async_complete_api);
    }

    static int BaseBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->GetStats(out_stats);
    }

    static int BaseBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
    {
        struct API* api = (struct API*)(block_store_api);
        return api->m_Owner->Flush(async_complete_api);
    }

};

struct SyncIndexToLocalCache_GetBlobCallback {
    LONGTAIL_CALLBACK_API(GetBlob) m_API;
    TLongtail_Atomic32* m_PendingItems;
    HLongtail_Sema* m_CompletionEvent;
    char* m_Path;
    void* m_Buffer;
    uint64_t m_Size;
    int m_Err;

    static void Dispose(struct Longtail_API* longtail_api)
    {
        struct SyncIndexToLocalCache_GetBlobCallback* api = (struct SyncIndexToLocalCache_GetBlobCallback*)longtail_api;
        Longtail_Free((void*)api->m_Buffer);
        Longtail_Free((void*)api);
    }

    static void OnComplete(struct Longtail_AsyncGetBlobAPI* async_complete_api, int err)
    {
        struct SyncIndexToLocalCache_GetBlobCallback* api = (struct SyncIndexToLocalCache_GetBlobCallback*)async_complete_api;
        api->m_Err = err;
        int32_t remaining_items = Longtail_AtomicAdd32(api->m_PendingItems, -1);
        if (remaining_items == 0)
        {
            Longtail_PostSema(*api->m_CompletionEvent, 1);
        }
    }
    static struct SyncIndexToLocalCache_GetBlobCallback* Make(TLongtail_Atomic32* pending_items,
        HLongtail_Sema* completion_event, char* path)
    {
        size_t MemSize = sizeof(SyncIndexToLocalCache_GetBlobCallback);
        void* Mem = Longtail_Alloc("SyncIndexToLocalCache_GetBlobCallback::Make", MemSize);
        if (Mem == 0)
        {
            return 0;
        }
        struct SyncIndexToLocalCache_GetBlobCallback* api = (struct SyncIndexToLocalCache_GetBlobCallback*)Longtail_MakeAsyncGetBlobAPI(
            Mem,
            Dispose,
            OnComplete);
        api->m_PendingItems = pending_items;
        api->m_CompletionEvent = completion_event;
        api->m_Path = path;
        api->m_Buffer = 0;
        api->m_Size = 0;
        api->m_Err = ENOENT;
        return api;
    }
};


struct SyncIndexToLocalCache_ListBlobsCallback {
    LONGTAIL_CALLBACK_API(ListBlobs) m_API;
    char* m_NameBuffer;
    uint64_t m_Size;
    HLongtail_Sema m_CompletionEvent;

    static void Dispose(struct Longtail_API* longtail_api)
    {
        struct SyncIndexToLocalCache_ListBlobsCallback* api = (struct SyncIndexToLocalCache_ListBlobsCallback*)longtail_api;
        if (api->m_NameBuffer)
        {
            Longtail_Free(api->m_NameBuffer);
            api->m_NameBuffer = 0;
        }
        Longtail_DeleteSema(api->m_CompletionEvent);
        Longtail_Free((void*)api);
    }

    static void OnComplete(struct Longtail_AsyncListBlobsAPI* async_complete_api, int err)
    {
        struct SyncIndexToLocalCache_ListBlobsCallback* api = (struct SyncIndexToLocalCache_ListBlobsCallback*)async_complete_api;
        Longtail_PostSema(api->m_CompletionEvent, 1);
    }
    static struct SyncIndexToLocalCache_ListBlobsCallback* Make()
    {
        size_t MemSize = sizeof(SyncIndexToLocalCache_ListBlobsCallback) + Longtail_GetSemaSize();
        void* Mem = Longtail_Alloc("SyncIndexToLocalCache_ListBlobsCallback::Make", MemSize);
        if (Mem == 0)
        {
            return 0;
        }
        struct SyncIndexToLocalCache_ListBlobsCallback* api = (struct SyncIndexToLocalCache_ListBlobsCallback*)Longtail_MakeAsyncListBlobsAPI(
            Mem,
            Dispose,
            OnComplete);
        void* MemPtr = &api[1];
        int err = Longtail_CreateSema(MemPtr, 0, &api->m_CompletionEvent);
        if (err)
        {
            Longtail_Free(Mem);
            return 0;
        }
        api->m_NameBuffer = 0;
        api->m_Size = 0;
        return api;
    }
    static int Wait(struct SyncIndexToLocalCache_ListBlobsCallback* api)
    {
        return Longtail_WaitSema(api->m_CompletionEvent, LONGTAIL_TIMEOUT_INFINITE);
    }
};

static SORTFUNC(SortLSINames)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    const char* a_name = *(const char**)a_ptr;
    const char* b_name = *(const char**)b_ptr;
    return strcmp(a_name, b_name);
}

static int GetLocalCacheIndexFiles(struct Longtail_StorageAPI* cache_storage_api,
    const char* cache_base_path, char*** OutLocalNames)
{
    char** LocalNames = 0;
    Longtail_StorageAPI_HIterator iterator = 0;
    int err = cache_storage_api->StartFind(cache_storage_api, cache_base_path, &iterator);
    if (err == 0)
    {
        while (err == 0)
        {
            struct Longtail_StorageAPI_EntryProperties properties;
            err = cache_storage_api->GetEntryProperties(cache_storage_api, iterator, &properties);
            if (err)
            {
                break;
            }
            size_t name_length = strlen(properties.m_Name);
            if (name_length == SHA1_NAME_LENGTH + 4)
            {
                if (strcmp(&properties.m_Name[SHA1_NAME_LENGTH], ".lsi") == 0)
                {
                    arrput(LocalNames, Longtail_Strdup(properties.m_Name));
                }
            }
            err = cache_storage_api->FindNext(cache_storage_api, iterator);
        }
        cache_storage_api->CloseFind(cache_storage_api, iterator);
    }
    if (err == ENOENT)
    {
        err = 0;
    }
    if (err == 0)
    {
        *OutLocalNames = LocalNames;
    }
    return err;
}

static int BuildStoreIndexFromLocalCache(struct Longtail_StorageAPI* cache_storage_api,
    const char* cache_base_path, struct Longtail_StoreIndex** out_store_index)
{
    char** LocalNames = 0;
    int err = GetLocalCacheIndexFiles(cache_storage_api, cache_base_path, &LocalNames);
    if (err)
    {
        return err;
    }

    intptr_t count = arrlen(LocalNames);
    if (count > 0)
    {
        QSORT(LocalNames, arrlen(LocalNames), sizeof(const char*), SortLSINames, 0);
        char* local_path = cache_storage_api->ConcatPath(cache_storage_api, cache_base_path, LocalNames[0]);
        struct Longtail_StoreIndex* store_index;
        err = Longtail_ReadStoreIndex(cache_storage_api, local_path, &store_index);
        Longtail_Free((void*)local_path);
        if (err == 0)
        {
            if (count > 1)
            {
                for (intptr_t index = 1; index < count; index++)
                {
                    char* next_local_path = cache_storage_api->ConcatPath(cache_storage_api, cache_base_path, LocalNames[index]);
                    struct Longtail_StoreIndex* next_store_index;
                    err = Longtail_ReadStoreIndex(cache_storage_api, next_local_path, &next_store_index);
                    Longtail_Free((void*)next_local_path);
                    if (err == 0)
                    {
                        struct Longtail_StoreIndex* merged_store_index;
                        err = Longtail_MergeStoreIndex(store_index, next_store_index, &merged_store_index);
                        if (err == 0)
                        {
                            Longtail_Free((void*)store_index);
                            store_index = merged_store_index;
                        }
                        Longtail_Free((void*)next_store_index);
                    }
                    if (err)
                    {
                        Longtail_Free((void*)store_index);
                        break;
                    }
                }
            }
            if (err == 0)
            {
                *out_store_index = store_index;
            }
        }
    }
    else
    {
        err = Longtail_CreateStoreIndexFromBlocks(0, 0, out_store_index);
    }

    for (intptr_t i = 0; i < arrlen(LocalNames); i++)
    {
        Longtail_Free(LocalNames[i]);
    }
    arrfree(LocalNames);

    return err;
}

static int SyncIndexToLocalCache(struct Longtail_PersistenceAPI* PersistanceAPI,
    struct Longtail_StorageAPI* cache_storage_api,
    const char* cache_base_path)
{
    struct SyncIndexToLocalCache_ListBlobsCallback* CB = SyncIndexToLocalCache_ListBlobsCallback::Make();
    if (CB == 0)
    {
        return ENOMEM;
    }
    int err = PersistanceAPI->List(PersistanceAPI, "index", 0, &CB->m_NameBuffer, &CB->m_Size, &CB->m_API);
    if (err)
    {
        SAFE_DISPOSE_API(&CB->m_API);
        return err;
    }
    err = SyncIndexToLocalCache_ListBlobsCallback::Wait(CB);
    if (err)
    {
        SAFE_DISPOSE_API(&CB->m_API);
        return err;
    }
    char** RemoteNames = 0;
    char* NameBufferPtr = CB->m_NameBuffer;
    while (*NameBufferPtr != 0)
    {
        arrput(RemoteNames, Longtail_Strdup(NameBufferPtr));
        NameBufferPtr += strlen(NameBufferPtr) + 1;
    }

    QSORT(RemoteNames, arrlen(RemoteNames), sizeof(const char*), SortLSINames, 0);

    char** LocalNames = 0;
    err = GetLocalCacheIndexFiles(cache_storage_api, cache_base_path, &LocalNames);
    if (err == 0)
    {
        QSORT(LocalNames, arrlen(LocalNames), sizeof(const char*), SortLSINames, 0);

        char** AddedNames = 0;
        char** RemovedNames = 0;
        intptr_t remote_index = 0;
        intptr_t local_index = 0;
        while (local_index < arrlen(LocalNames) && remote_index < arrlen(RemoteNames))
        {
            int comp = strcmp(LocalNames[local_index], RemoteNames[remote_index]);
            if (comp == 0)
            {
                local_index++;
                remote_index++;
            }
            else if (comp < 0)
            {
                arrput(RemovedNames, LocalNames[local_index]);
                local_index++;
            }
            else
            {
                arrput(AddedNames, RemoteNames[remote_index]);
                remote_index++;
            }
        }
        while (local_index < arrlen(LocalNames))
        {
            arrput(RemovedNames, LocalNames[local_index]);
            local_index++;
        }
        while (remote_index < arrlen(RemoteNames))
        {
            arrput(AddedNames, RemoteNames[remote_index]);
            remote_index++;
        }

        TLongtail_Atomic32 pending_items = 1;
        HLongtail_Sema completion_event;
        err = Longtail_CreateSema(Longtail_Alloc("SyncIndexToLocalCache", Longtail_GetSemaSize()), 0, &completion_event);
        if (err == 0)
        {
            SyncIndexToLocalCache_GetBlobCallback** get_blob_callbacks = 0;
            for (intptr_t added_index = 0; added_index < arrlen(AddedNames); added_index++)
            {
                SyncIndexToLocalCache_GetBlobCallback* CB = SyncIndexToLocalCache_GetBlobCallback::Make(&pending_items, &completion_event, AddedNames[added_index]);
                if (CB)
                {
                    Longtail_AtomicAdd32(&pending_items, 1);
                    err = PersistanceAPI->Read(PersistanceAPI, CB->m_Path, &CB->m_Buffer, &CB->m_Size, &CB->m_API);
                    if (err)
                    {
                        Longtail_AtomicAdd32(&pending_items, -1);
                        SAFE_DISPOSE_API(&CB->m_API);
                        break;
                    }
                    arrput(get_blob_callbacks, CB);
                }
                else
                {
                    err = ENOMEM;
                }
                if (err)
                {
                    break;
                }
            }

            if (Longtail_AtomicAdd32(&pending_items, -1) == 0)
            {
                Longtail_PostSema(completion_event, 1);
            }

            Longtail_WaitSema(completion_event, LONGTAIL_TIMEOUT_INFINITE);
            if (err == 0)
            {
                for (intptr_t i = 0; i < arrlen(get_blob_callbacks); i++)
                {
                    SyncIndexToLocalCache_GetBlobCallback* CB = get_blob_callbacks[i];
                    if (CB->m_Err == 0)
                    {
                        char* name_part = strrchr(CB->m_Path, '/');
                        if (name_part == 0)
                        {
                            name_part = CB->m_Path;
                        }
                        else
                        {
                            name_part++;
                        }

                        char* local_path = cache_storage_api->ConcatPath(cache_storage_api, cache_base_path, name_part);
                        if (local_path != 0)
                        {
                            err = EnsureParentPathExists(cache_storage_api, local_path);
                            if (err == 0)
                            {
                                Longtail_StorageAPI_HOpenFile f;
                                err = cache_storage_api->OpenWriteFile(cache_storage_api, local_path, 0, &f);
                                if (err == 0)
                                {
                                    err = cache_storage_api->Write(cache_storage_api, f, 0, CB->m_Size, CB->m_Buffer);
                                    cache_storage_api->CloseFile(cache_storage_api, f);
                                }
                            }
                            Longtail_Free((void*)local_path);
                        }
                        else
                        {
                            err = ENOMEM;
                            break;
                        }
                    }
                    else if (CB->m_Err == ENOENT)
                    {
                        err = EAGAIN;
                    }
                }
            }

            for (intptr_t i = 0; i < arrlen(get_blob_callbacks); i++)
            {
                SAFE_DISPOSE_API(&get_blob_callbacks[i]->m_API);
            }
            arrfree(get_blob_callbacks);
            Longtail_DeleteSema(completion_event);
            Longtail_Free((void*)completion_event);
        }

        if (err == 0)
        {
            for (intptr_t removed_index = 0; removed_index < arrlen(RemovedNames); removed_index++)
            {
                const char* local_path = RemovedNames[removed_index];
                err = cache_storage_api->RemoveFile(cache_storage_api, local_path);
                if (err == ENOENT)
                {
                    err = 0;
                }
            }
        }

        arrfree(AddedNames);
        arrfree(RemovedNames);
    }

    for (intptr_t i = 0; i < arrlen(LocalNames); i++)
    {
        Longtail_Free(LocalNames[i]);
    }
    arrfree(LocalNames);

    for (intptr_t i = 0; i < arrlen(RemoteNames); i++)
    {
        Longtail_Free((void*)RemoteNames[i]);
    }
    arrfree(RemoteNames);
    SAFE_DISPOSE_API(&CB->m_API);
    return err;
}


BaseBlockStore::BaseBlockStore(struct Longtail_PersistenceAPI* PersistanceAPI,
    struct Longtail_StorageAPI* cache_storage_api,
    const char* cache_base_path)
    :m_Persistance(PersistanceAPI)
    ,m_CacheStorage(cache_storage_api)
    ,m_CacheBasePath(Longtail_Strdup(cache_base_path))
{
    Longtail_MakeBlockStoreAPI(
        &m_API, 
        BaseBlockStore_Dispose, 
        BaseBlockStore_PutStoredBlock, 
        BaseBlockStore_PreflightGet, 
        BaseBlockStore_GetStoredBlock, 
        BaseBlockStore_GetExistingContent, 
        BaseBlockStore_PruneBlocks, 
        BaseBlockStore_GetStats, 
        BaseBlockStore_Flush);
    m_API.m_Owner = this;
}

BaseBlockStore::~BaseBlockStore()
{
    m_API.m_Owner = 0;
    hmfree(m_BlockState);
    for (intptr_t i = 0; i < arrlen(m_AddedBlockIndexes); i++)
    {
        Longtail_Free(m_AddedBlockIndexes[i]);
    }
    arrfree(m_AddedBlockIndexes);
    Longtail_Free((void*)m_CacheBasePath);
}

int BaseBlockStore::PutStoredBlock(struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    uint64_t block_hash = *stored_block->m_BlockIndex->m_BlockHash;

    struct Longtail_BlockIndex* block_index_copy = Longtail_CopyBlockIndex(stored_block->m_BlockIndex);
    if (!block_index_copy)
    {
        //          LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)        
        //          Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return ENOMEM;
    }

    void* buffer;
    size_t size;
    int err = Longtail_WriteStoredBlockToBuffer(
        stored_block,
        &buffer,
        &size);
    if (err)
    {
        Longtail_Free(block_index_copy);
        return err;
    }

    {
        // With lock
        intptr_t block_ptr = hmgeti(m_BlockState, block_hash);
        if (block_ptr != -1)
        {
            // Already busy doing put or the block has already been stored

            Longtail_Free(buffer);
            Longtail_Free(block_index_copy);
            return 0;
        }
        BlockHashToBlockState::V InProgressState { size, buffer };
        hmput(m_BlockState, block_hash, InProgressState);
    }

    char* block_path = GetBlobPath(".lsb", block_hash);

    class CB
    {
    public:
        CB(BaseBlockStore* BaseBlockAPI, char* block_path, void* buffer, struct Longtail_BlockIndex* block_index, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
            : m_BaseBlockAPI(BaseBlockAPI)
            , m_BlockPath(block_path)
            , m_Buffer(buffer)
            , m_BlockIndex(block_index)
            , m_AsyncCompleteAPI(async_complete_api)
        {
            Longtail_MakeAsyncPutBlobAPI(&m_API, Dispose, OnComplete);
            m_API.m_CB = this;
        }
        ~CB()
        {
            Longtail_Free(m_Buffer);
            m_Buffer = 0;
            Longtail_Free(m_BlockIndex);
            m_BlockIndex = 0;
            Longtail_Free(m_BlockPath);
            m_BlockPath = 0;
        }
        void Complete(int err)
        {
            if (err == 0)
            {
                {
                    // With lock
                    BlockHashToBlockState::V CompleteState{ 0, 0 };
                    hmput(m_BaseBlockAPI->m_BlockState, *m_BlockIndex->m_BlockHash, CompleteState);
                    arrput(m_BaseBlockAPI->m_AddedBlockIndexes, m_BlockIndex);
                    m_BlockIndex = 0;
                }
            }
            Longtail_AsyncPutStoredBlock_OnComplete(m_AsyncCompleteAPI, err);
            delete this;
        }

        operator LONGTAIL_CALLBACK_API(PutBlob)*() { return &m_API.m_API; };
    private:
        struct API {
            LONGTAIL_CALLBACK_API(PutBlob) m_API;
            CB* m_CB;
        } m_API;

        static void Dispose(struct Longtail_API* longtail_api)
        {
            struct API* api = (struct API*)longtail_api;
            delete api->m_CB;
        }

        static void OnComplete(struct Longtail_AsyncPutBlobAPI* async_complete_api, int err)
        {
            struct API* api = (struct API*)async_complete_api;
            api->m_CB->Complete(err);
        }

        BaseBlockStore* m_BaseBlockAPI;
        struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
        void* m_BlockPath;
        void* m_Buffer;
        struct Longtail_BlockIndex* m_BlockIndex;
    };

    // Callback owns buffer and block_index_copy if m_Persistance->Write returns 0;
    CB* Callback = new CB(this, block_path, buffer, block_index_copy, async_complete_api);
    err = Longtail_PersistenceAPI_Write(m_Persistance, block_path, buffer, size, *Callback);
    if (err)
    {
        delete Callback;
        {
            // With lock
            hmdel(m_BlockState, block_hash);
        }
    }
    return err;
}

int BaseBlockStore::PreflightGet(uint32_t block_count, const TLongtail_Hash* block_hashes, struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api)
{
    if (!optional_async_complete_api)
    {
        return 0;
    }

    int err = SyncIndexToLocalCache(m_Persistance,
        m_CacheStorage,
        m_CacheBasePath);
    while (err == EAGAIN)
    {
        err = SyncIndexToLocalCache(m_Persistance,
            m_CacheStorage,
            m_CacheBasePath);
    }

    struct Longtail_StoreIndex* store_index;
    err = BuildStoreIndexFromLocalCache(m_CacheStorage, m_CacheBasePath, &store_index);
    if (err)
    {
        return err;
    }

    struct Longtail_LookupTable* requested_block_lookup = LongtailPrivate_LookupTable_Create(Longtail_Alloc("BaseBlockStore::PreflightGet", LongtailPrivate_LookupTable_GetSize(block_count)), block_count, 0);
    if (!requested_block_lookup)
    {
        Longtail_Free(store_index);
        return ENOMEM;
    }

    TLongtail_Hash* found_block_hashes = (TLongtail_Hash*)Longtail_Alloc("BaseBlockStore::PreflightGet", sizeof(TLongtail_Hash) * block_count);
    if (!found_block_hashes)
    {
        Longtail_Free(requested_block_lookup);
        Longtail_Free(store_index);
        return ENOMEM;
    }

    uint32_t found_block_count = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        TLongtail_Hash block_hash = block_hashes[b];
        LongtailPrivate_LookupTable_PutUnique(requested_block_lookup, block_hash, b);
    }

    for (uint32_t b = 0; b < *store_index->m_BlockCount; ++b)
    {
        TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
        if (LongtailPrivate_LookupTable_Get(requested_block_lookup, block_hash))
        {
            found_block_hashes[found_block_count++] = block_hash;
        }
    }
    Longtail_Free(store_index);
    store_index = 0;
    Longtail_Free(requested_block_lookup);

    optional_async_complete_api->OnComplete(optional_async_complete_api, found_block_count, found_block_hashes, 0);

    Longtail_Free(found_block_hashes);

    return 0;
}

int BaseBlockStore::GetStoredBlock(uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    Longtail_StoredBlock* stored_block = 0;
    {
        // With lock
        intptr_t block_ptr = hmgeti(m_BlockState, block_hash);
        if (block_ptr != -1)
        {
            if (m_BlockState[block_ptr].value.buffer != 0)
            {
                int err = Longtail_ReadStoredBlockFromBuffer(
                    m_BlockState[block_ptr].value.buffer,
                    m_BlockState[block_ptr].value.size,
                    &stored_block);
                if (err)
                {
                    return err;
                }
            }
        }
    }
    if (stored_block != 0)
    {
        async_complete_api->OnComplete(async_complete_api, stored_block, 0);
        return 0;
    }

    char* block_path = GetBlobPath(".lsb", block_hash);

    class CB
    {
    public:
        CB(BaseBlockStore* BaseBlockAPI, char* block_path, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
            : m_BlockPath(block_path)
            , m_Buffer(0)
            , m_Size(0)
            , m_AsyncCompleteAPI(async_complete_api)
        {
            Longtail_MakeAsyncGetBlobAPI(&m_API, Dispose, OnComplete);
            m_API.m_CB = this;
        }
        ~CB()
        {
            Longtail_Free(m_Buffer);
            m_Buffer = 0;
            Longtail_Free(m_BlockPath);
            m_BlockPath = 0;
        }
        void Complete(int err)
        {
            Longtail_StoredBlock* stored_block = 0;
            if (err == 0)
            {
                err = Longtail_ReadStoredBlockFromBuffer(
                    m_Buffer,
                    m_Size,
                    &stored_block);
            }
            Longtail_AsyncGetStoredBlock_OnComplete(m_AsyncCompleteAPI, stored_block, err);
            delete this;
        }

        operator LONGTAIL_CALLBACK_API(GetBlob)* () { return &m_API.m_API; };
    private:
        struct API {
            LONGTAIL_CALLBACK_API(GetBlob) m_API;
            CB* m_CB;
        } m_API;

        static void Dispose(struct Longtail_API* longtail_api)
        {
            struct API* api = (struct API*)longtail_api;
            delete api->m_CB;
        }

        static void OnComplete(struct Longtail_AsyncGetBlobAPI* async_complete_api, int err)
        {
            struct API* api = (struct API*)async_complete_api;
            api->m_CB->Complete(err);
        }

        struct Longtail_AsyncGetStoredBlockAPI* m_AsyncCompleteAPI;
    public:
        void* m_BlockPath;
        void* m_Buffer;
        uint64_t m_Size;
    };

    CB* Callback = new CB(this, block_path, async_complete_api);
    int err = Longtail_PersistenceAPI_Read(m_Persistance, block_path, &Callback->m_Buffer, &Callback->m_Size, *Callback);
    if (err)
    {
        delete Callback;
    }
    return err;
}

int BaseBlockStore::GetExistingContent(uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
{
    int err = SyncIndexToLocalCache(m_Persistance,
        m_CacheStorage,
        m_CacheBasePath);
    while (err == EAGAIN)
    {
        err = SyncIndexToLocalCache(m_Persistance,
            m_CacheStorage,
            m_CacheBasePath);
    }

    struct Longtail_StoreIndex* existing_store_index = 0;
    {
        struct Longtail_StoreIndex* local_store_index;
        err = BuildStoreIndexFromLocalCache(m_CacheStorage, m_CacheBasePath, &local_store_index);
        if (err == 0)
        {
            err = Longtail_GetExistingStoreIndex(local_store_index, chunk_count, chunk_hashes, min_block_usage_percent, &existing_store_index);
            Longtail_Free((void*)local_store_index);
        }
    }
    Longtail_AsyncGetExistingContent_OnComplete(async_complete_api, existing_store_index, err);
    return 0;
}

int BaseBlockStore::PruneBlocks(uint32_t block_keep_count, const TLongtail_Hash* block_keep_hashes, struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
{
    // TODO
    Longtail_AsyncPruneBlocks_OnComplete(async_complete_api, 0, ENOTSUP);
    return 0;
}

int BaseBlockStore::GetStats(struct Longtail_BlockStore_Stats* out_stats)
{
    // TODO
    return ENOTSUP;
}

struct BaseBlockStore_FlushPutBlobCallback {
    LONGTAIL_CALLBACK_API(PutBlob) m_API;
    struct Longtail_AsyncFlushAPI* m_FlushCallback;
    char* m_Path;
    void* m_Buffer;

    static void Dispose(struct Longtail_API* longtail_api)
    {
        struct BaseBlockStore_FlushPutBlobCallback* api = (struct BaseBlockStore_FlushPutBlobCallback*)longtail_api;
        Longtail_Free(api->m_Path);
        api->m_Path = 0;
        Longtail_Free(api->m_Buffer);
        api->m_Buffer = 0;
        Longtail_Free((void*)api);
    }

    static void OnComplete(struct Longtail_AsyncPutBlobAPI* async_complete_api, int err)
    {
        struct BaseBlockStore_FlushPutBlobCallback* api = (struct BaseBlockStore_FlushPutBlobCallback*)async_complete_api;
        struct Longtail_AsyncFlushAPI* flush_callback = api->m_FlushCallback;
        Longtail_Free(api->m_Path);
        api->m_Path = 0;
        Longtail_Free(api->m_Buffer);
        api->m_Buffer = 0;
        Longtail_Free((void*)api);
        flush_callback->OnComplete(flush_callback, err);
    }
};


int BaseBlockStore::Flush(struct Longtail_AsyncFlushAPI* async_complete_api)
{
    int err = SyncIndexToLocalCache(m_Persistance,
        m_CacheStorage,
        m_CacheBasePath);
    while (err == EAGAIN)
    {
        err = SyncIndexToLocalCache(m_Persistance,
            m_CacheStorage,
            m_CacheBasePath);
    }
    if (err == 0)
    {
        // TODO:
        // + Merge small index files with m_AddedBlockIndexes
        // + Save merged index to local and remote
        // + Remove merged small index files from remote and local

        intptr_t pending_index_count = arrlen(m_AddedBlockIndexes);
        if (pending_index_count > 0)
        {
            struct Longtail_StoreIndex* added_store_index = 0;
            err = Longtail_CreateStoreIndexFromBlocks((uint32_t)pending_index_count, (const struct Longtail_BlockIndex**)m_AddedBlockIndexes, &added_store_index);
            if (err == 0)
            {
                void* buffer = 0;
                uint64_t size = 0;
                err = Longtail_WriteStoreIndexToBuffer(added_store_index, &buffer, &size);
                if (err == 0)
                {
                    char index_path[6 + (SHA1_NAME_LENGTH)+4 + 1];
                    strcpy(index_path, "index/");
                    unsigned char SHA[SHA1_BLOCK_SIZE];
                    SHA1((const unsigned char*)buffer, size_t(size), SHA);
                    SHA1String(SHA, &index_path[6]);
                    strcpy(&index_path[6 + (SHA1_NAME_LENGTH)], ".lsi");
                    index_path[6 + (SHA1_NAME_LENGTH)+4] = '\0';

                    struct BaseBlockStore_FlushPutBlobCallback* CB = (struct BaseBlockStore_FlushPutBlobCallback*)Longtail_MakeAsyncPutBlobAPI(Longtail_Alloc("BaseBlockStore::Flush", sizeof(struct BaseBlockStore_FlushPutBlobCallback)), BaseBlockStore_FlushPutBlobCallback::Dispose, BaseBlockStore_FlushPutBlobCallback::OnComplete);
                    if (CB)
                    {
                        CB->m_FlushCallback = async_complete_api;
                        CB->m_Path = Longtail_Strdup(index_path);
                        if (CB->m_Path)
                        {
                            CB->m_Buffer = buffer;
                            buffer = 0;
                            err = m_Persistance->Write(m_Persistance, CB->m_Path, CB->m_Buffer, size, &CB->m_API);
                            if (err)
                            {
                                SAFE_DISPOSE_API(&CB->m_API);
                            }
                        }
                        else
                        {
                            SAFE_DISPOSE_API(&CB->m_API);
                            err = ENOMEM;
                        }
                    }
                    else
                    {
                        err = ENOMEM;
                    }
                    if (buffer)
                    {
                        Longtail_Free((void*)buffer);
                    }
                }
                Longtail_Free((void*)added_store_index);
            }
            if (err == 0)
            {
                for (intptr_t i = 0; i < arrlen(m_AddedBlockIndexes); i++)
                {
                    Longtail_Free(m_AddedBlockIndexes[i]);
                }
                arrsetlen(m_AddedBlockIndexes, 0);
            }
        }
    }
    async_complete_api->OnComplete(async_complete_api, err);
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateBaseBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_StorageAPI* cache_storage_api,
    const char* cache_base_path)
{
    BaseBlockStore* Store = new BaseBlockStore(persistence_api, cache_storage_api, cache_base_path);
    return *Store;
}
