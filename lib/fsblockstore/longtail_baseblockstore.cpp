#include "longtail_fsblockstore.h"
#include "longtail_sha1.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
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
    BaseBlockStore(struct Longtail_PersistenceAPI* PersistanceAPI);
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




BaseBlockStore::BaseBlockStore(struct Longtail_PersistenceAPI* PersistanceAPI)
    :m_Persistance(PersistanceAPI)
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
    if (optional_async_complete_api)
    {
        Longtail_AsyncPreflightStarted_OnComplete(optional_async_complete_api, 0, 0, ENOTSUP);
        return 0;
    }
    return ENOTSUP;
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
    Longtail_AsyncGetExistingContent_OnComplete(async_complete_api, 0, ENOTSUP);
    return 0;
}

int BaseBlockStore::PruneBlocks(uint32_t block_keep_count, const TLongtail_Hash* block_keep_hashes, struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
{
    Longtail_AsyncPruneBlocks_OnComplete(async_complete_api, 0, ENOTSUP);
    return 0;
}

int BaseBlockStore::GetStats(struct Longtail_BlockStore_Stats* out_stats)
{
    return ENOTSUP;
}

int BaseBlockStore::Flush(struct Longtail_AsyncFlushAPI* async_complete_api)
{
    Longtail_AsyncFlush_OnComplete(async_complete_api, ENOTSUP);
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateBaseBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api)
{
    BaseBlockStore* Store = new BaseBlockStore(persistence_api);
    return *Store;
}
