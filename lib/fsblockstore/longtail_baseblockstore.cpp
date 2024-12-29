#include "longtail_fsblockstore.h"
#include "longtail_sha1.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

//class PersistanceAPI
//{
//public:
//    class WriteCallback
//    {
//    public:
//        virtual void OnComplete(int err) = 0;
//    };
//    class ReadCallback
//    {
//    public:
//        virtual void OnComplete(int err) = 0;
//    };
//    class ListCallback
//    {
//    public:
//        virtual void OnComplete(int err) = 0;
//    };
//
//
//    // TODO: These functions should have a OnComplete parameter
//    virtual int Write(const char* sub_path, const void* data, uint64_t size, WriteCallback* callback) = 0;
//    virtual int Read(const char* sub_path, void* data, uint64_t size, ReadCallback* callback) = 0;
//    virtual int List(const char* sub_path, int recursive, ListCallback* callback) = 0;
//};

class TestPersistanceAPI
{
public:
    TestPersistanceAPI(struct Longtail_StorageAPI* storage_api)
        : m_StorageAPI(storage_api)
    {
        Longtail_MakePersistenceAPI(&m_API, Dispose, WriteItem, ReadItem, ListItems);
        m_API.m_Owner = this;
    }
    ~TestPersistanceAPI()
    {
        m_API.m_Owner = 0;
    }
    operator struct Longtail_PersistenceAPI* () { return &m_API.m_API; };
private:
    struct API
    {
        struct Longtail_PersistenceAPI m_API;
        TestPersistanceAPI* m_Owner;
    } m_API;

    struct Longtail_StorageAPI* m_StorageAPI;

    int Write(const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
    {
        int err = EnsureParentPathExists(m_StorageAPI, sub_path);
        if (err)
        {
            Longtail_AsyncPutBlob_OnComplete(callback, err);
            return 0;
        }

        Longtail_StorageAPI_HOpenFile f;
        err = m_StorageAPI->OpenWriteFile(m_StorageAPI, sub_path, 0, &f);
        if (err)
        {
            Longtail_AsyncPutBlob_OnComplete(callback, err);
            return 0;
        }
        err = m_StorageAPI->Write(m_StorageAPI, f, 0, size, data);
        m_StorageAPI->CloseFile(m_StorageAPI, f);
        Longtail_AsyncPutBlob_OnComplete(callback, err);
        return 0;
    }
    int Read(const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
    {
        Longtail_StorageAPI_HOpenFile f;
        int err = m_StorageAPI->OpenReadFile(m_StorageAPI, sub_path, &f);
        if (err)
        {
            Longtail_AsyncGetBlob_OnComplete(callback, err);
            return 0;
        }
        err = m_StorageAPI->GetSize(m_StorageAPI, f, size_buffer);
        if (err)
        {
            Longtail_AsyncGetBlob_OnComplete(callback, err);
            return 0;
        }
        *data = Longtail_Alloc("TestPersistanceAPI", *size_buffer);
        if (*data == 0)
        {
            Longtail_AsyncGetBlob_OnComplete(callback, ENOMEM);
        }
        err = m_StorageAPI->Read(m_StorageAPI, f, 0, *size_buffer, *data);
        if (err)
        {
            Longtail_Free(*data);
            *data = 0;
            *size_buffer = 0;
        }
        m_StorageAPI->CloseFile(m_StorageAPI, f);
        Longtail_AsyncGetBlob_OnComplete(callback, err);
        return 0;
    }
    int List(const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
    {
        size_t NamesBufferSize = 0;
        char** Names = 0;
        char** ScanDirs = 0;
        arrput(ScanDirs, Longtail_Strdup(sub_path));
        intptr_t ScanDirIndex = 0;

        while (ScanDirIndex < arrlen(ScanDirs))
        {
            Longtail_StorageAPI_HIterator Iterator;
            int err = m_StorageAPI->StartFind(m_StorageAPI, ScanDirs[ScanDirIndex], &Iterator);
            if (err)
            {
                for (intptr_t D = 0; D < arrlen(Names); D++)
                {
                    Longtail_Free(Names[D]);
                }
                arrfree(Names);

                for (intptr_t D = 0; D < arrlen(ScanDirs); D++)
                {
                    Longtail_Free(ScanDirs[D]);
                }
                arrfree(ScanDirs);
                Longtail_AsyncListBlobs_OnComplete(callback, err);
                return 0;
            }

            while (err != ENOENT)
            {
                struct Longtail_StorageAPI_EntryProperties properties;
                err = m_StorageAPI->GetEntryProperties(m_StorageAPI, Iterator, &properties);
                if (err)
                {
                    break;
                }
                if (properties.m_IsDir && recursive)
                {
                    arrput(ScanDirs, m_StorageAPI->ConcatPath(m_StorageAPI, ScanDirs[ScanDirIndex], properties.m_Name));
                }
                arrput(Names, m_StorageAPI->ConcatPath(m_StorageAPI, ScanDirs[ScanDirIndex], properties.m_Name));
                NamesBufferSize += strlen(Names[arrlen(Names) - 1]) + 1;
                err = m_StorageAPI->FindNext(m_StorageAPI, Iterator);
            }
            m_StorageAPI->CloseFind(m_StorageAPI, Iterator);
            if (err != ENOENT)
            {
                for (intptr_t D = 0; D < arrlen(Names); D++)
                {
                    Longtail_Free(Names[D]);
                }
                arrfree(Names);

                for (intptr_t D = 0; D < arrlen(ScanDirs); D++)
                {
                    Longtail_Free(ScanDirs[D]);
                }
                arrfree(ScanDirs);
                Longtail_AsyncListBlobs_OnComplete(callback, err);
                return 0;
            }
            ScanDirIndex++;
        }

        for (intptr_t D = 0; D < arrlen(ScanDirs); D++)
        {
            Longtail_Free(ScanDirs[D]);
        }
        arrfree(ScanDirs);

        *size_buffer = NamesBufferSize + 1;
        *name_buffer = (char*)Longtail_Alloc("TestPersistanceAPI", *size_buffer);
        if (*name_buffer == 0)
        {
            *size_buffer = 0;
            for (intptr_t D = 0; D < arrlen(Names); D++)
            {
                Longtail_Free(Names[D]);
            }
            arrfree(Names);
            Longtail_AsyncListBlobs_OnComplete(callback, ENOMEM);
            return 0;
        }
        char* target_ptr = *name_buffer;
        for (intptr_t I = 0; I < arrlen(Names); I++)
        {
            strcpy(target_ptr, Names[I]);
            target_ptr += strlen(Names[I]) + 1;
        }
        *target_ptr = 0;
        for (intptr_t D = 0; D < arrlen(Names); D++)
        {
            Longtail_Free(Names[D]);
        }
        arrfree(Names);
        Longtail_AsyncListBlobs_OnComplete(callback, 0);
        return 0;
    }

    static void Dispose(struct Longtail_API* longtail_api)
    {
        struct API* api = (struct API*)(longtail_api);
        delete api->m_Owner;
    }

    static int WriteItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->Write(sub_path, data, size, callback);
    }
    static int ReadItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->Read(sub_path, data, size_buffer, callback);
    }
    static int ListItems(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->List(sub_path, recursive, name_buffer, size_buffer, callback);
    }

};

struct Longtail_PersistenceAPI* Longtail_CreateTestPersistanceAPI(struct Longtail_StorageAPI* storage_api)
{
    TestPersistanceAPI* api = new TestPersistanceAPI(storage_api);
    return *api;
}

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

    char file_name[7 + BLOCK_NAME_LENGTH + 3 + 1];
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
        enum class EState : uint8_t
        {
            Writing = 0,
            Complete = 1
        };
        uint64_t key;
        enum EState value;
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

    char* block_path = GetBlobPath(".lsb", block_hash);

    {
        // With lock
        intptr_t block_ptr = hmgeti(m_BlockState, block_hash);
        if (block_ptr != -1)
        {
            // Already busy doing put or the block has already been stored

            Longtail_Free(buffer);
            Longtail_Free(block_index_copy);
            Longtail_Free(block_path);
            return 0;
        }
        hmput(m_BlockState, block_hash, BlockHashToBlockState::EState::Writing);
    }

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
                    hmput(m_BaseBlockAPI->m_BlockState, *m_BlockIndex->m_BlockHash, BlockHashToBlockState::EState::Complete);
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
        void* m_BlockPath;
        void* m_Buffer;
        struct Longtail_BlockIndex* m_BlockIndex;
        struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
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
    Longtail_AsyncGetStoredBlock_OnComplete(async_complete_api, 0, ENOTSUP);
    return 0;
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

LONGTAIL_IMPLEMENT_CALLBACK_API(PutBlob)
LONGTAIL_IMPLEMENT_CALLBACK_API(GetBlob)
LONGTAIL_IMPLEMENT_CALLBACK_API(ListBlobs)

struct Longtail_BlockStoreAPI* Longtail_CreateBaseBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_PersistenceAPI* cache_storage_api,
    const char* content_path,
    const char* optional_extension,
    int enable_file_mapping)
{
    BaseBlockStore* Store = new BaseBlockStore(persistence_api); // TODO: Memory leak!
    return *Store;
}

int Longtail_PersistenceAPI_Write(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
{
    return persistance_api->Write(persistance_api, sub_path, data, size, callback);
}

int Longtail_PersistenceAPI_Read(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
{
    return persistance_api->Read(persistance_api, sub_path, data, size_buffer, callback);
}

int Longtail_PersistenceAPI_List(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
{
    return persistance_api->List(persistance_api, sub_path, recursive, name_buffer, size_buffer, callback);
}

struct Longtail_PersistenceAPI* Longtail_MakePersistenceAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_PersistenceAPI_WriteItemFunc write_func,
    Longtail_PersistenceAPI_ReadItemFunc read_func,
    Longtail_PersistenceAPI_ListItemsFunc list_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(write_func, "%p"),
        LONGTAIL_LOGFIELD(read_func, "%p"),
        LONGTAIL_LOGFIELD(list_func, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

        LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
        struct Longtail_PersistenceAPI* api = (struct Longtail_PersistenceAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->Write = write_func;
    api->Read = read_func;
    api->List = list_func;
    return api;
}



#if 0
struct BlockHashToBlockState
{
    uint64_t key;
    uint32_t value;
};

#define TMP_EXTENSION_LENGTH (1 + 16)




struct Longtail_TestPersistanceAPI
{
    struct Longtail_PersistenceAPI m_api;
    void** blobs_array;
    uint64_t* blobs_sizes_array;
    char** blobs_paths_array;
};

static int TestWriteItemFunc(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
{
    struct Longtail_TestPersistanceAPI* api = (struct Longtail_TestPersistanceAPI*)persistance_api;
    return 0;
}

static int TestReadItemFunc(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
{
    struct Longtail_TestPersistanceAPI* api = (struct Longtail_TestPersistanceAPI*)persistance_api;
    intptr_t blob_count = arrlen(api->blobs_array);
    for (intptr_t blob_index = 0; blob_index < blob_count; blob_index++)
    {
        if (strcmp(api->blobs_paths_array[blob_index], sub_path) == 0)
        {
            *data = api->blobs_array[blob_index];
            *size_buffer = api->blobs_sizes_array[blob_index];
            LONGTAIL_CALL_CALLBACK_API(GetBlob, callback, 0);
            return 0;
        }
    }
    return ENOENT;
}

static int TestListItemsFunc(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
{
    struct Longtail_TestPersistanceAPI* api = (struct Longtail_TestPersistanceAPI*)persistance_api;
    return 0;
}

static void Test_Dispose(struct Longtail_API* in_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(in_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, in_api, return)

    struct Longtail_TestPersistanceAPI* api = (struct Longtail_TestPersistanceAPI*)in_api;

    arrfree(api->blobs_array);
    arrfree(api->blobs_sizes_array);
    arrfree(api->blobs_paths_array);

    Longtail_Free(api);
}

int Longtail_CreateTestPersistanceInit(void* mem, struct Longtail_TestPersistanceAPI** out_api)
{
    struct Longtail_TestPersistanceAPI* api = (struct Longtail_TestPersistanceAPI*)Longtail_MakePersistenceAPI(
        mem,
        Test_Dispose,
        TestWriteItemFunc,
        TestReadItemFunc,
        TestListItemsFunc);
    if (!api)
    {
        return EINVAL;
    }

    api->blobs_array = 0;
    api->blobs_sizes_array = 0;
    api->blobs_paths_array = 0;

    *out_api = api;
    return 0;
}

struct Longtail_PersistenceAPI* Longtail_CreateTestPersistanceAPI()
{
//    MAKE_LOG_CONTEXT_FIELDS(ctx)
//    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    struct Longtail_LogContextFmt_Private* ctx = 0;

    size_t api_size = sizeof(struct Longtail_TestPersistanceAPI);
    void* mem = Longtail_Alloc("FSBlockStore2API", api_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    struct Longtail_TestPersistanceAPI* api;
    int err = Longtail_CreateTestPersistanceInit(
        mem,
        &api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateTestPersistanceInit() failed with %d", err)
            Longtail_Free(mem);
        return 0;
    }
    return (struct Longtail_PersistenceAPI*)api;
}



#if 0
struct FSBlockStore2API
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_PersistenceAPI* m_PersistanceAPI;
    struct Longtail_PersistenceAPI* m_CachePersistanceAPI;
    struct Longtail_JobAPI* m_JobAPI;
    char* m_StorePath;

    TLongtail_Atomic64 m_StatU64[Longtail_BlockStoreAPI_StatU64_Count];

    HLongtail_SpinLock m_Lock;

    struct Longtail_StoreIndex* m_StoreIndex;
    struct BlockHashToBlockState* m_BlockState;
    struct Longtail_BlockIndex** m_AddedBlockIndexes;
    const char* m_BlockExtension;
    const char* m_StoreIndexLockPath;
    uint32_t m_StoreIndexIsDirty;
    int m_EnableFileMapping;
    char m_TmpExtension[TMP_EXTENSION_LENGTH + 1];
};

#define BLOCK_NAME_LENGTH   23

static const char* HashLUT = "0123456789abcdef";

static void GetUniqueExtension(uint64_t id, char* extension)
{
    extension[0] = '.';
    extension[1] = HashLUT[(id >> 60) & 0xf];
    extension[2] = HashLUT[(id >> 56) & 0xf];
    extension[3] = HashLUT[(id >> 52) & 0xf];
    extension[4] = HashLUT[(id >> 48) & 0xf];
    extension[5] = HashLUT[(id >> 44) & 0xf];
    extension[6] = HashLUT[(id >> 40) & 0xf];
    extension[7] = HashLUT[(id >> 36) & 0xf];
    extension[8] = HashLUT[(id >> 32) & 0xf];
    extension[9] = HashLUT[(id >> 28) & 0xf];
    extension[10] = HashLUT[(id >> 24) & 0xf];
    extension[11] = HashLUT[(id >> 20) & 0xf];
    extension[12] = HashLUT[(id >> 16) & 0xf];
    extension[13] = HashLUT[(id >> 12) & 0xf];
    extension[14] = HashLUT[(id >> 8) & 0xf];
    extension[15] = HashLUT[(id >> 4) & 0xf];
    extension[16] = HashLUT[(id >> 0) & 0xf];
    extension[17] = 0;
}

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

static char* GetBlockPath(
    struct Longtail_StorageAPI* storage_api,
    const char* store_path,
    const char* block_extension,
    TLongtail_Hash block_hash)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(store_path, "%s"),
        LONGTAIL_LOGFIELD(block_extension, "%s"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, store_path, return 0)
    char file_name[7 + BLOCK_NAME_LENGTH + 15 + 1];
    strcpy(file_name, "chunks/");
    GetBlockName(block_hash, &file_name[7]);
    strcpy(&file_name[7 + BLOCK_NAME_LENGTH], block_extension);
    return storage_api->ConcatPath(storage_api, store_path, file_name);
}

static char* GetTempBlockPath(
    struct Longtail_StorageAPI* storage_api,
    const char* store_path,
    TLongtail_Hash block_hash,
    const char* tmp_extension)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(store_path, "%s"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(tmp_extension, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, store_path, return 0)
    char file_name[7 + BLOCK_NAME_LENGTH + TMP_EXTENSION_LENGTH + 1];
    strcpy(file_name, "chunks/");
    GetBlockName(block_hash, &file_name[7]);
    strcpy(&file_name[7 + BLOCK_NAME_LENGTH], tmp_extension);
    return storage_api->ConcatPath(storage_api, store_path, file_name);
}
#if 0
static int AddStoreIndex(struct FSBlockStore2API* api, struct Longtail_StoreIndex* added_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(added_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_StorageAPI* storage_api = api->m_StorageAPI;
    const char* store_path = api->m_StorePath;
    Longtail_StorageAPI_HIterator iterator;
    int err = storage_api->StartFind(storage_api, store_path, &iterator);
    while (err == 0)
    {
        struct Longtail_StorageAPI_EntryProperties properties;
        err = storage_api->GetEntryProperties(storage_api, iterator, &properties);
        if (err == 0)
        {
            if (properties.m_IsDir == 0)
            {
                size_t name_length = strlen(properties.m_Name);
                if (name_length > 4)
                {
                    if (strcmp(&properties.m_Name[name_length - 4], ".lsi") == 0)
                    {

                    }
                }
            }
            err = storage_api->FindNext(storage_api, iterator);
        }
    }
    if (err != 0 && err != ENOENT)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "StartFind() failed with %d", err)
        err = storage_api->FindNext(storage_api, iterator);
        return err;
    }
    storage_api->CloseFind(storage_api, iterator);
}
#endif // 0

struct LongtailStoreindexState
{
    const char** store_index_names;
    struct Longtail_StoreIndex** store_index_array;
};

static struct LongtailStoreindexState* AllocState()
{
    struct Longtail_LogContextFmt_Private* ctx = 0;

    struct LongtailStoreindexState* state = (struct LongtailStoreindexState*)Longtail_Alloc("FSBlockStore2API", sizeof(struct LongtailStoreindexState));
    if (state == 0)
    {
        return 0;
    }

    state->store_index_names = 0;
    state->store_index_array = 0;

    return state;
}

static void FreeState(struct LongtailStoreindexState* state)
{
    intptr_t name_count = arrlen(state->store_index_names);
    for (intptr_t index = 0; index < name_count; index++)
    {
        Longtail_Free((void*)state->store_index_names[name_count]);
    }
    arrfree(state->store_index_array);

    intptr_t index_count = arrlen(state->store_index_array);
    for (intptr_t index = 0; index < index_count; index++)
    {
        Longtail_Free(state->store_index_array[index]);
    }
    arrfree(state->store_index_array);

    Longtail_Free(state);
}

static int ReadStoreIndexes(struct FSBlockStore2API* api, struct LongtailStoreindexState** out_state)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(out_state, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct LongtailStoreindexState* state = AllocState();
    if (state == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "AllocState() failed with %d", ENOMEM)
        return ENOMEM;
    }

    struct Longtail_StorageAPI* storage_api = api->m_StorageAPI;
    const char* store_path = api->m_StorePath;
    Longtail_StorageAPI_HIterator iterator;
    int err = storage_api->StartFind(storage_api, store_path, &iterator);
    if (err != 0)
    {
        if (err != ENOENT)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "StartFind() failed with %d", err)
            FreeState(state);
            return err;
        }
        *out_state = state;
        return 0;
    }
    while (1)
    {
        struct Longtail_StorageAPI_EntryProperties properties;
        err = storage_api->GetEntryProperties(storage_api, iterator, &properties);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "GetEntryProperties() failed with %d", err)
            storage_api->CloseFind(storage_api, iterator);
            FreeState(state);
            return err;
        }
        if (properties.m_IsDir == 0)
        {
            size_t name_length = strlen(properties.m_Name);
            if (name_length > 4)
            {
                if (strcmp(&properties.m_Name[name_length - 4], ".lsi") == 0)
                {
                    const char* store_index_path = storage_api->ConcatPath(storage_api, store_path, properties.m_Name);
                    arrput(state->store_index_names, store_index_path);
                    struct Longtail_StoreIndex* store_index = 0;
                    err = Longtail_ReadStoreIndex(storage_api, store_index_path, &store_index);
                    if (err != 0)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadStoreIndex() failed with %d", err)
                        storage_api->CloseFind(storage_api, iterator);
                        FreeState(state);
                        return err;
                    }
                }
            }
        }
        err = storage_api->FindNext(storage_api, iterator);
        if (err == ENOENT)
        {
            err = 0;
            break;
        }
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FindNext() failed with %d", err)
            storage_api->CloseFind(storage_api, iterator);
            FreeState(state);
            return err;
        }
    }
    storage_api->CloseFind(storage_api, iterator);
    *out_state = state;
    return 0;
}

static int CollapseStoreState(struct FSBlockStore2API* api, struct LongtailStoreindexState* state, struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(state, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, state, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_store_index, return EINVAL)

    intptr_t name_count = arrlen(state->store_index_names);
    intptr_t index_count = arrlen(state->store_index_array);
    LONGTAIL_FATAL_ASSERT(ctx, name_count = index_count, return EINVAL)
    if (index_count == 0)
    {
        return Longtail_CreateStoreIndexFromBlocks(0, 0, out_store_index);
    }
    if (index_count == 1)
    {
        *out_store_index = state->store_index_array[0];
        state->store_index_array[0] = 0;
        return 0;
    }
    struct Longtail_StoreIndex* store_index = state->store_index_array[0];
    for (intptr_t index = 1; index < index_count; index++)
    {
        struct Longtail_StoreIndex* merged_store_index = state->store_index_array[0];
        int err = Longtail_MergeStoreIndex(store_index, state->store_index_array[index], &merged_store_index);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeStoreIndex() failed with %d", err);
            Longtail_Free(store_index);
            return err;
        }
        Longtail_Free(store_index);
        store_index = merged_store_index;
    }
    *out_store_index = store_index;
    return 0;
}

static int ReadCollpsedStoreIndex(struct FSBlockStore2API* api, struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_store_index, return EINVAL)

    struct LongtailStoreindexState* state;
    int err = ReadStoreIndexes(api, &state);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ReadStoreIndexes() failed with %d", err);
        return err;
    }
    struct Longtail_StoreIndex* store_index = 0;
    err = CollapseStoreState(api, state, &store_index);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ReadStoreIndexes() failed with %d", err);
        FreeState(state);
        return err;
    }
    FreeState(state);
    *out_store_index = store_index;
    return 0;
}

static int WriteStoreIndex(struct FSBlockStore2API* api, struct Longtail_StoreIndex* store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_StorageAPI* storage_api = api->m_PersistanceAPI;
    void* buffer;
    size_t size;
    int err = Longtail_WriteStoreIndexToBuffer(
        store_index,
        &buffer,
        &size);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoreIndexToBuffer() failed with %d", err)
        return err;
    }

    unsigned char store_index_hash[SHA1_BLOCK_SIZE];
    SHA1(buffer, size, store_index_hash);
    char store_index_name[SHA1_BLOCK_SIZE * 2 + 4 + 1];
    char* store_index_name_ptr = store_index_name;
    for (size_t sha_offset = 0; sha_offset < SHA1_BLOCK_SIZE; sha_offset++)
    {
        *store_index_name_ptr++ = HashLUT[(store_index_hash[sha_offset] >> 8)];
        *store_index_name_ptr++ = HashLUT[(store_index_hash[sha_offset] & 0xf)];
    }
    strcpy(store_index_name_ptr, ".lsi");

//    struct SyncFlush
//    {
//        struct Longtail_AsyncFlushAPI m_API;
//        HLongtail_Sema m_NotifySema;
//        int m_Err;
//    };

    Longtail_PersistenceAPI_WriteCallback CompleteCallback;
    err = Longtail_PersistenceAPI_Write(api->m_PersistanceAPI, store_index_name, buffer, size, CompleteCallback);
    if (err == 0 && api->m_CachePersistanceAPI != 0)
    {
        err = Longtail_PersistenceAPI_Write(api->m_CachePersistanceAPI, store_index_name, buffer, size, CompleteCallback);
    }
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_PersistenceAPI_Write() failed with %d", err)
        Longtail_Free(buffer);
        return err;
    }
    // Wait

    if (err)
    {
        // Handle callback
    }

    return 0;
}

static int ReplaceStoreIndex(struct FSBlockStore2API* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

//    struct Longtail_StorageAPI* storage_api = api->m_StorageAPI;
//    const char* store_path = api->m_StorePath;

    struct LongtailStoreindexState* index_state = 0;
    int err = ReadStoreIndexes(api, index_state);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ReadStoreIndexes() failed with %d", err)
        return err;
    }

    for (intptr_t index = 0; index < arrlen(index_state->store_index_names); index++)
    {
        storage_api->RemoveFile(storage_api, index_state->store_index_names[index]);
    }

    FreeState(index_state);
    index_state = 0;

    WriteStoreIndex(api, api->m_StoreIndex);

#if 0
    char tmp_store_path[5 + TMP_EXTENSION_LENGTH + 1];
    strcpy(tmp_store_path, "store");
    strcpy(&tmp_store_path[5], api->m_TmpExtension);
    const char* store_index_path_tmp = storage_api->ConcatPath(storage_api, store_path, tmp_store_path);
    int err = EnsureParentPathExists(storage_api, store_index_path_tmp);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        Longtail_Free((char*)store_index_path_tmp);
        return err;
    }

    const char* store_index_path = storage_api->ConcatPath(storage_api, store_path, "store.lsi");

    struct Longtail_StoreIndex* store_index = api->m_StoreIndex;
    if (merge_with_existing_index && storage_api->IsFile(storage_api, store_index_path))
    {
        struct Longtail_StoreIndex* existing_store_index = 0;
        err = Longtail_ReadStoreIndex(storage_api, store_index_path, &existing_store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadStoreIndex() failed with %d", err)
            Longtail_Free((void*)store_index_path);
            Longtail_Free((void*)store_index_path_tmp);
            return err;
        }
        struct Longtail_StoreIndex* merged_store_index = 0;
        err = Longtail_MergeStoreIndex(
            store_index, // Our opinion of the store index has precedence
            existing_store_index,
            &merged_store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeStoreIndex() failed with %d", err)
            Longtail_Free(existing_store_index);
            Longtail_Free((void*)store_index_path);
            Longtail_Free((void*)store_index_path_tmp);
            return err;
        }
        Longtail_Free(existing_store_index);
        store_index = merged_store_index;
    }

    err = Longtail_WriteStoreIndex(storage_api, store_index, store_index_path_tmp);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoreIndex() failed with %d", err)
        Longtail_Free((void*)store_index_path);
        Longtail_Free((void*)store_index_path_tmp);
        return err;
    }

    if (storage_api->IsFile(storage_api, store_index_path))
    {
        err = storage_api->RemoveFile(storage_api, store_index_path);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->RemoveFile() failed with %d", err)
            Longtail_Free((void*)store_index_path);
            storage_api->RemoveFile(storage_api, store_index_path_tmp);
            Longtail_Free((void*)store_index_path_tmp);
            return err;
        }
    }

    err = storage_api->RenameFile(storage_api, store_index_path_tmp, store_index_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->RenameFile() failed with %d", err)
        storage_api->RemoveFile(storage_api, store_index_path_tmp);
    }

    if (!err)
    {
        if (api->m_StoreIndex != store_index)
        {
            Longtail_Free(api->m_StoreIndex);
            api->m_StoreIndex = store_index;
        }
        api->m_StoreIndexIsDirty = 0;
    }

    Longtail_Free((void*)store_index_path);
    Longtail_Free((void*)store_index_path_tmp);
#endif // 0
    return err;

}
static int SafeWriteStoredBlock(
    struct FSBlockStore2API* api,
    struct Longtail_StorageAPI* storage_api,
    const char* store_path,
    const char* block_extension,
    struct Longtail_StoredBlock* stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p"),
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(store_path, "%s"),
        LONGTAIL_LOGFIELD(block_extension, "%s"),
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    TLongtail_Hash block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    char* block_path = GetBlockPath(storage_api, store_path, block_extension, block_hash);

    // Check if block exists, if it does it is just the store store index that is out of sync.
    // Don't write the block unless we have to
    if (storage_api->IsFile(storage_api, block_path))
    {
        Longtail_Free((void*)block_path);
        return 0;
    }

    char* tmp_block_path = GetTempBlockPath(storage_api, store_path, block_hash, api->m_TmpExtension);
    int err = EnsureParentPathExists(storage_api, tmp_block_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        Longtail_Free((char*)tmp_block_path);
        Longtail_Free((char*)block_path);
        return err;
    }

    err = Longtail_WriteStoredBlock(storage_api, stored_block, tmp_block_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoredBlock() failed with %d", err)
        Longtail_Free((char*)tmp_block_path);
        Longtail_Free((char*)block_path);
        return err;
    }

    err = storage_api->RenameFile(storage_api, tmp_block_path, block_path);
    if (err)
    {
        int remove_err = storage_api->RemoveFile(storage_api, tmp_block_path);
        if (remove_err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "storage_api->RemoveFile(), failed with %d", remove_err)
        }
    }

    if (err && (err != EEXIST))
    {
        // Someone beat us to it, all good.
        if (storage_api->IsFile(storage_api, block_path))
        {
            Longtail_Free((char*)tmp_block_path);
            Longtail_Free((void*)block_path);
            return 0;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to rename temp block file %s, failed with %d", block_path, err)
        Longtail_Free((char*)tmp_block_path);
        Longtail_Free((char*)block_path);
        return err;
    }

    Longtail_Free((char*)tmp_block_path);
    Longtail_Free((char*)block_path);
    return 0;
}
#if 0
static int UpdateStoreIndex(
    struct Longtail_StoreIndex* current_store_index,
    struct Longtail_BlockIndex** added_block_indexes,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(current_store_index, "%p"),
        LONGTAIL_LOGFIELD(added_block_indexes, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_StoreIndex* added_store_index;
    int err = Longtail_CreateStoreIndexFromBlocks(
        (uint32_t)(arrlen(added_block_indexes)),
        (const struct Longtail_BlockIndex** )added_block_indexes,
        &added_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
        return err;
    }
    struct Longtail_StoreIndex* new_store_index;
    err = Longtail_MergeStoreIndex(
        added_store_index,  // Added first as it has precedence
        current_store_index,
        &new_store_index);
    Longtail_Free(added_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeStoreIndex() failed with %d", err)
        return err;
    }
    *out_store_index = new_store_index;
    return 0;
}
#endif // 0

struct FSStoredBlock
{
    struct Longtail_StoredBlock m_StoredBlock;
};

static int FSStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return EINVAL)
    Longtail_Free(stored_block);
    return 0;
}

struct ScanBlockJob
{
    struct Longtail_StorageAPI* m_StorageAPI;
    const char* m_StorePath;
    const char* m_ChunksPath;
    const char* m_BlockPath;
    const char* m_BlockExtension;
    struct Longtail_BlockIndex* m_BlockIndex;
};

int EndsWith(const char *str, const char *suffix)
{
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

static int ScanBlock(void* context, uint32_t job_id, int detected_error)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    struct ScanBlockJob* job = (struct ScanBlockJob*)context;
    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "ScanBlock aborted due to previous error %d", detected_error)
        return 0;
    }

    const char* block_path = job->m_BlockPath;
    if (!EndsWith(block_path, job->m_BlockExtension))
    {
        // Not a block, skip it
        return 0;
    }

    struct Longtail_StorageAPI* storage_api = job->m_StorageAPI;
    const char* chunks_path = job->m_ChunksPath;
    char* full_block_path = storage_api->ConcatPath(storage_api, chunks_path, block_path);

    int err = Longtail_ReadBlockIndex(
        storage_api,
        full_block_path,
        &job->m_BlockIndex);

    if (err == 0)
    {
        TLongtail_Hash block_hash = *job->m_BlockIndex->m_BlockHash;
        char* validate_file_name = GetBlockPath(storage_api, job->m_StorePath, job->m_BlockExtension, block_hash);
        if (strcmp(validate_file_name, full_block_path) != 0)
        {
            Longtail_Free(job->m_BlockIndex);
            job->m_BlockIndex = 0;
            err = EBADF;
        }
        Longtail_Free(validate_file_name);
    }

    Longtail_Free(full_block_path);
    full_block_path = 0;
    return err;
}

static int ReadContent(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_JobAPI* job_api,
    const char* store_path,
    const char* block_extension,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(store_path, "%s"),
        LONGTAIL_LOGFIELD(block_extension, "%s"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, store_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_store_index != 0, return EINVAL)

    const char* chunks_path = Longtail_Strdup(store_path);//storage_api->ConcatPath(storage_api, store_path, "chunks");
    if (!chunks_path)
    {
        return ENOMEM;
    }

    struct Longtail_FileInfos* file_infos;
    int err = Longtail_GetFilesRecursively2(
        storage_api,
        job_api,
        0,
        0,
        0,
        chunks_path,
        &file_infos);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_GetFilesRecursively() failed with %d", err)
        Longtail_Free((void*)chunks_path);
        return err;
    }

    uint32_t path_count = file_infos->m_Count;
    if (path_count == 0)
    {
        err = Longtail_CreateStoreIndexFromBlocks(
            0,
            0,
            out_store_index);
        Longtail_Free(file_infos);
        Longtail_Free((void*)chunks_path);
        return err;
    }
    Longtail_JobAPI_Group job_group;
    err = job_api->ReserveJobs(job_api, path_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReserveJobs() failed with %d", err)
        Longtail_Free(file_infos);
        Longtail_Free((void*)chunks_path);
        return err;
    }

    size_t scan_jobs_size = sizeof(struct ScanBlockJob) * path_count;
    struct ScanBlockJob* scan_jobs = (struct ScanBlockJob*)Longtail_Alloc("FSBlockStore2API", scan_jobs_size);
    if (!scan_jobs)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(file_infos);
        Longtail_Free((void*)chunks_path);
        return ENOMEM;
    }

    for (uint32_t path_index = 0; path_index < path_count; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        const char* block_path = &file_infos->m_PathData[file_infos->m_PathStartOffsets[path_index]];
        job->m_StorageAPI = storage_api;
        job->m_StorePath = store_path;
        job->m_ChunksPath = chunks_path;
        job->m_BlockPath = block_path;
        job->m_BlockExtension = block_extension;
        job->m_BlockIndex = 0;

        Longtail_JobAPI_JobFunc job_func[] = {ScanBlock};
        void* ctxs[] = {job};
        Longtail_JobAPI_Jobs jobs;
        err = job_api->CreateJobs(job_api, job_group, 0, 0, 0, 1, job_func, ctxs, 0, &jobs);
        LONGTAIL_FATAL_ASSERT(ctx, !err, return err)
        err = job_api->ReadyJobs(job_api, 1, jobs);
        LONGTAIL_FATAL_ASSERT(ctx, !err, return err)
    }

    err = job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_ERROR, "job_api->WaitForAllJobs() failed with %d", err)
        for (uint32_t path_index = 0; path_index < path_count; ++path_index)
        {
            struct ScanBlockJob* job = &scan_jobs[path_index];
            Longtail_Free(job->m_BlockIndex);
        }
        Longtail_Free(scan_jobs);
        Longtail_Free(file_infos);
        Longtail_Free((void*)chunks_path);
        return err;
    }

    size_t block_indexes_size = sizeof(struct Longtail_BlockIndex*) * (path_count);
    struct Longtail_BlockIndex** block_indexes = (struct Longtail_BlockIndex**)Longtail_Alloc("FSBlockStore2API", block_indexes_size);
    if (!block_indexes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        for (uint32_t path_index = 0; path_index < path_count; ++path_index)
        {
            struct ScanBlockJob* job = &scan_jobs[path_index];
            Longtail_Free(job->m_BlockIndex);
        }
        Longtail_Free(scan_jobs);
        Longtail_Free(file_infos);
        Longtail_Free((void*)chunks_path);
        return ENOMEM;
    }

    uint32_t block_count = 0;
    for (uint32_t path_index = 0; path_index < path_count; ++path_index)
    {
        struct ScanBlockJob* job = &scan_jobs[path_index];
        if (job->m_BlockIndex)
        {
            block_indexes[block_count] = job->m_BlockIndex;
            ++block_count;
        }
    }

    Longtail_Free(scan_jobs);
    scan_jobs = 0;

    Longtail_Free(file_infos);
    file_infos = 0;

    Longtail_Free((void*)chunks_path);
    chunks_path = 0;

    err = Longtail_CreateStoreIndexFromBlocks(
        block_count,
        (const struct Longtail_BlockIndex**)block_indexes,
        out_store_index);

    for (uint32_t b = 0; b < block_count; ++b)
    {
        Longtail_Free(block_indexes[b]);
    }
    Longtail_Free(block_indexes);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
    }
    return err;
}

#if 0
int FSBlockStore_GetStoreIndexFromStorage(
    struct FSBlockStore2API* fsblockstore_api,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(fsblockstore_api, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_StorageAPI* storage_api = fsblockstore_api->m_StorageAPI;
    struct Longtail_JobAPI* job_api = fsblockstore_api->m_JobAPI;
    const char* store_path = fsblockstore_api->m_StorePath;
    const char* block_extension = fsblockstore_api->m_BlockExtension;

    struct Longtail_StoreIndex* store_index = 0;

    int err = EnsureParentPathExists(storage_api, fsblockstore_api->m_StoreIndexLockPath);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    Longtail_StorageAPI_HLockFile store_index_lock_file;
    err = storage_api->LockFile(storage_api, fsblockstore_api->m_StoreIndexLockPath, &store_index_lock_file);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->LockFile() failed with %d", err)
        return err;
    }

    const char* store_index_path = storage_api->ConcatPath(storage_api, store_path, "store.lsi");

    if (storage_api->IsFile(storage_api, store_index_path))
    {
        int err = Longtail_ReadStoreIndex(storage_api, store_index_path, &store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadStoreIndex() failed with %d", err)
            Longtail_Free((void*)store_index_path);
            storage_api->UnlockFile(storage_api, store_index_lock_file);
            return err;
        }
    }
    storage_api->UnlockFile(storage_api, store_index_lock_file);

    Longtail_Free((void*)store_index_path);
    if (store_index)
    {
        *out_store_index = store_index;
        return 0;
    }
    err = ReadContent(
        storage_api,
        job_api,
        store_path,
        block_extension,
        &store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ReadContent() failed with %d", err)
        return err;
    }
    *out_store_index = store_index;
    fsblockstore_api->m_StoreIndexIsDirty = 1;
    return 0;
}
#endif // 0

static int FSBlockStore_UpdateStoreIndex(
    struct FSBlockStore2API* fsblockstore_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(fsblockstore_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    intptr_t new_block_count = arrlen(fsblockstore_api->m_AddedBlockIndexes);
    if (new_block_count != 0)
    {
        struct Longtail_StoreIndex* added_store_index = 0;
        int err = Longtail_CreateStoreIndexFromBlocks(new_block_count, fsblockstore_api->m_AddedBlockIndexes, added_store_index);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
            return err;
        }
        WriteStoreIndex(fsblockstore_api, added_store_index);
        Longtail_Free(added_store_index);

        while(new_block_count-- > 0)
        {
            struct Longtail_BlockIndex* block_index = fsblockstore_api->m_AddedBlockIndexes[new_block_count];
            Longtail_Free(block_index);
        }
        arrfree(fsblockstore_api->m_AddedBlockIndexes);
        Longtail_Free(fsblockstore_api->m_StoreIndex);
        fsblockstore_api->m_StoreIndex = 0;
    }

    if (fsblockstore_api->m_StoreIndex == 0)
    {
        struct Longtail_StoreIndex* store_index;
        int err = ReadCollpsedStoreIndex(fsblockstore_api, store_index);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
            return err;
        }
        Longtail_Free(fsblockstore_api->m_StoreIndex);
        fsblockstore_api->m_StoreIndex = store_index;
    }
#if 0

        int err = FSBlockStore_GetStoreIndexFromStorage(
            fsblockstore_api,
            &store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoreIndexFromStorage() failed with %d", err)
            return err;
        }

        if (fsblockstore_api->m_StoreIndex)
        {
            struct Longtail_StoreIndex* merged_store_index;
            err = Longtail_MergeStoreIndex(
                fsblockstore_api->m_StoreIndex, // Our opinion of the store index has precedence
                store_index,
                &merged_store_index);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeStoreIndex() failed with %d", err)
                Longtail_Free(store_index);
                return err;
            }
            Longtail_Free(store_index);
            Longtail_Free(fsblockstore_api->m_StoreIndex);
            fsblockstore_api->m_StoreIndex = 0;
            store_index = merged_store_index;
            fsblockstore_api->m_StoreIndexIsDirty = 1;
        }

        fsblockstore_api->m_StoreIndex = store_index;
        uint64_t block_count = *store_index->m_BlockCount;
        for (uint64_t b = 0; b < block_count; ++b)
        {
            uint64_t block_hash = store_index->m_BlockHashes[b];
            hmput(fsblockstore_api->m_BlockState, block_hash, 1);
        }
    }

    intptr_t new_block_count = arrlen(fsblockstore_api->m_AddedBlockIndexes);
    if (new_block_count > 0)
    {
        struct Longtail_StoreIndex* new_store_index;
        int err = UpdateStoreIndex(
            fsblockstore_api->m_StoreIndex,
            fsblockstore_api->m_AddedBlockIndexes,
            &new_store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "UpdateStoreIndex() failed with %d", err)
            return err;
        }

        Longtail_Free(fsblockstore_api->m_StoreIndex);
        fsblockstore_api->m_StoreIndex = new_store_index;

        while(new_block_count-- > 0)
        {
            struct Longtail_BlockIndex* block_index = fsblockstore_api->m_AddedBlockIndexes[new_block_count];
            Longtail_Free(block_index);
        }
        arrfree(fsblockstore_api->m_AddedBlockIndexes);
        fsblockstore_api->m_StoreIndexIsDirty = 1;
    }
#endif // 0
    return 0;
}

static int FSBlockStore_GetIndexSync(
    struct FSBlockStore2API* fsblockstore_api,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(fsblockstore_api, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    int err = FSBlockStore_UpdateStoreIndex(fsblockstore_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_UpdateStoreIndex() failed with %d", err)
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        return err;
    }

    struct Longtail_StoreIndex* store_index = Longtail_CopyStoreIndex(fsblockstore_api->m_StoreIndex);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CopyStoreIndex() failed with %d", ENOMEM)
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        return ENOMEM;
    }

    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    *out_store_index = store_index;
    return 0;
}

static int FSBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api != 0, return EINVAL)

    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    uint64_t block_hash = *stored_block->m_BlockIndex->m_BlockHash;

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    intptr_t block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    if (block_ptr != -1)
    {
        // Already busy doing put or the block already has been stored
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        async_complete_api->OnComplete(async_complete_api, 0);
        return 0;
    }

    hmput(fsblockstore_api->m_BlockState, block_hash, 0);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    int err = SafeWriteStoredBlock(fsblockstore_api, fsblockstore_api->m_StorageAPI, fsblockstore_api->m_StorePath, fsblockstore_api->m_BlockExtension, stored_block);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "SafeWriteStoredBlock() failed with %d", err)
        Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        hmdel(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
        async_complete_api->OnComplete(async_complete_api, err);
        return 0;
    }


    struct Longtail_BlockIndex* block_index_copy = Longtail_CopyBlockIndex(stored_block->m_BlockIndex);
    if (!block_index_copy)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        async_complete_api->OnComplete(async_complete_api, ENOMEM);
        return 0;
    }

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    hmput(fsblockstore_api->m_BlockState, block_hash, 1);
    arrput(fsblockstore_api->m_AddedBlockIndexes, block_index_copy);
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);

    async_complete_api->OnComplete(async_complete_api, 0);
    return 0;
}

static int FSBlockStore_PreflightGet(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_count,
    const TLongtail_Hash* block_hashes,
    struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(block_hashes, "%p"),
        LONGTAIL_LOGFIELD(optional_async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (block_count == 0) || (block_hashes != 0), return EINVAL)
    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);

    if (!optional_async_complete_api)
    {
        return 0;
    }

    struct Longtail_StoreIndex* store_index;
    int err = FSBlockStore_GetIndexSync(fsblockstore_api, &store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndexSync() failed with %d", err)
        Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount], 1);
        return err;
    }

    struct Longtail_LookupTable* requested_block_lookup = LongtailPrivate_LookupTable_Create(Longtail_Alloc("FSBlockStore", LongtailPrivate_LookupTable_GetSize(block_count)), block_count, 0);
    if (!requested_block_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(store_index);
        return ENOMEM;
    }

    TLongtail_Hash* found_block_hashes = (TLongtail_Hash*)Longtail_Alloc("CacheBlockStore", sizeof(TLongtail_Hash) * block_count);
    if (!found_block_hashes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_Alloc() failed with %d", ENOMEM)
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

static int MappedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return EINVAL)

    const char* p = (const char*)stored_block;
    p += Longtail_GetStoredBlockSize(0);

    struct Longtail_StorageAPI* storage_api = *(struct Longtail_StorageAPI**)p;
    p += sizeof(struct Longtail_StorageAPI*);
    Longtail_StorageAPI_HOpenFile file_handle = *(Longtail_StorageAPI_HOpenFile*)p;
    p += sizeof(Longtail_StorageAPI_HOpenFile);
    Longtail_StorageAPI_HFileMap file_map = *(Longtail_StorageAPI_HFileMap*)p;

    storage_api->UnMapFile(storage_api, file_map);
    storage_api->CloseFile(storage_api, file_handle);

    Longtail_Free(stored_block);
    return 0;
}


static int FSBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    Longtail_LockSpinLock(fsblockstore_api->m_Lock);
    intptr_t block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    if (block_ptr == -1)
    {
        char* block_path = GetBlockPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_StorePath, fsblockstore_api->m_BlockExtension, block_hash);
        if (!fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
        {
            Longtail_Free((void*)block_path);
            Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
            return ENOENT;
        }
        Longtail_Free((void*)block_path);
        hmput(fsblockstore_api->m_BlockState, block_hash, 1);
        block_ptr = hmgeti(fsblockstore_api->m_BlockState, block_hash);
    }
    uint32_t state = fsblockstore_api->m_BlockState[block_ptr].value;
    Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    while (state == 0)
    {
        Longtail_Sleep(1000);
        Longtail_LockSpinLock(fsblockstore_api->m_Lock);
        state = hmget(fsblockstore_api->m_BlockState, block_hash);
        Longtail_UnlockSpinLock(fsblockstore_api->m_Lock);
    }
    char* block_path = GetBlockPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_StorePath, fsblockstore_api->m_BlockExtension, block_hash);

    struct Longtail_StoredBlock* stored_block = 0;
    if (fsblockstore_api->m_EnableFileMapping)
    {
        Longtail_StorageAPI_HOpenFile file_handle;
        int err = fsblockstore_api->m_StorageAPI->OpenReadFile(fsblockstore_api->m_StorageAPI, block_path, &file_handle);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_WARNING, "fsblockstore_api->m_StorageAPI->OpenReadFile() failed with %d", err)
            Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free((char*)block_path);
            return err;
        }
        uint64_t block_size;
        err = fsblockstore_api->m_StorageAPI->GetSize(fsblockstore_api->m_StorageAPI, file_handle, &block_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_WARNING, "fsblockstore_api->m_StorageAPI->GetSize() failed with %d", err)
            Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, file_handle);
            Longtail_Free((char*)block_path);
            return err;
        }
        void* block_data;
        Longtail_StorageAPI_HFileMap file_map;
        err = fsblockstore_api->m_StorageAPI->MapFile(fsblockstore_api->m_StorageAPI, file_handle, 0, block_size, &file_map, (const void**)&block_data);
        if (!err)
        {
            size_t block_mem_size = Longtail_GetStoredBlockSize(0) + sizeof(struct Longtail_StorageAPI*) + sizeof(Longtail_StorageAPI_HOpenFile) + sizeof(Longtail_StorageAPI_HFileMap);
            stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("ArchiveBlockStore_GetStoredBlock", block_mem_size);
            if (!stored_block)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
                Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
                fsblockstore_api->m_StorageAPI->UnMapFile(fsblockstore_api->m_StorageAPI, file_map);
                fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, file_handle);
                Longtail_Free((char*)block_path);
                return ENOMEM;
            }
            int err = Longtail_InitStoredBlockFromData(
                stored_block,
                block_data,
                block_size);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData() failed with %d", err)
                Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
                Longtail_Free(stored_block);
                fsblockstore_api->m_StorageAPI->UnMapFile(fsblockstore_api->m_StorageAPI, file_map);
                fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, file_handle);
                Longtail_Free((char*)block_path);
                return err;
            }
            char* p = (char*)stored_block;
            p += Longtail_GetStoredBlockSize(0);
            *(struct Longtail_StorageAPI**)p = fsblockstore_api->m_StorageAPI;
            p += sizeof(struct Longtail_StorageAPI*);
            *(Longtail_StorageAPI_HOpenFile*)p = file_handle;
            p += sizeof(Longtail_StorageAPI_HOpenFile);
            *(Longtail_StorageAPI_HFileMap*)p = file_map;
            stored_block->Dispose = MappedStoredBlock_Dispose;
        }
        else if (err != ENOTSUP)
        {
            LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_WARNING, "fsblockstore_api->m_StorageAPI->MapFile() failed with %d", err)
            Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, file_handle);
            Longtail_Free((char*)block_path);
            return err;
        }
    }
    if (stored_block == 0)
    {
        int err = Longtail_ReadStoredBlock(fsblockstore_api->m_StorageAPI, block_path, &stored_block);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadStoredBlock() failed with %d", err)
            Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free((char*)block_path);
            return err;
        }
    }
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    Longtail_Free(block_path);

    async_complete_api->OnComplete(async_complete_api, stored_block, 0);
    return 0;
}

static int FSBlockStore_GetExistingContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint32_t min_block_usage_percent,
    struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count], 1);
    struct Longtail_StoreIndex* store_index;
    int err = FSBlockStore_GetIndexSync(fsblockstore_api, &store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndexSync() failed with %d", err)
        Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount], 1);
        return err;
    }

    struct Longtail_StoreIndex* existing_store_index;
    err = Longtail_GetExistingStoreIndex(
        store_index,
        chunk_count,
        chunk_hashes,
        min_block_usage_percent,
        &existing_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetExistingStoreIndex() failed with %d", err)
        Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount], 1);
        Longtail_Free(store_index);
        return err;
    }
    Longtail_Free(store_index);
    async_complete_api->OnComplete(async_complete_api, existing_store_index, 0);
    return 0;
}

static int FSBlockStore_PruneBlocks(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_keep_count,
    const TLongtail_Hash* block_keep_hashes,
    struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_keep_count, "%u"),
        LONGTAIL_LOGFIELD(block_keep_hashes, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (block_keep_count == 0) || (block_keep_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct FSBlockStore2API* api = (struct FSBlockStore2API*)block_store_api;

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_Count], 1);
    struct Longtail_StoreIndex* store_index;
    int err = FSBlockStore_GetIndexSync(api, &store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetIndexSync() failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);
        return err;
    }

    // We have a gap here where the index on disk *could* change from another Flush operation
    // Pruning while other process/task is writing or pruning to the same disk database is not supported

    Longtail_StorageAPI_HLockFile store_index_lock_file;
    err = api->m_StorageAPI->LockFile(api->m_StorageAPI, api->m_StoreIndexLockPath, &store_index_lock_file);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "m_StorageAPI->LockFile() failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);
        Longtail_Free(store_index);
        return err;
    }
    Longtail_LockSpinLock(api->m_Lock);

    {
        struct Longtail_StoreIndex* pruned_store_index;
        err = Longtail_PruneStoreIndex(
            store_index,
            block_keep_count,
            block_keep_hashes,
            &pruned_store_index);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_PruneStoreIndex() failed with %d", err)
            api->m_StorageAPI->UnlockFile(api->m_StorageAPI, store_index_lock_file);
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);
            Longtail_Free(store_index);
            Longtail_UnlockSpinLock(api->m_Lock);
            return err;
        }

        if (api->m_StoreIndex != pruned_store_index)
        {
            Longtail_Free(api->m_StoreIndex);
            api->m_StoreIndex = pruned_store_index;
        }
        api->m_StoreIndexIsDirty = 0;
    }

    err = ReplaceStoreIndex(api);
    if (err != 0) {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "SafeWriteStoreIndex() failed with %d", err)
        api->m_StorageAPI->UnlockFile(api->m_StorageAPI, store_index_lock_file);
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);
        Longtail_Free(store_index);
        Longtail_UnlockSpinLock(api->m_Lock);
        return err;
    }

    uint32_t old_block_count = *store_index->m_BlockCount;
    uint32_t block_count = *api->m_StoreIndex->m_BlockCount;
    uint32_t pruned_count = *store_index->m_BlockCount - block_count;
    if (pruned_count > 0)
    {
        size_t kept_block_lookup_size = LongtailPrivate_LookupTable_GetSize(block_count);
        void* kept_block_lookup_mem = Longtail_Alloc("FSBlockStore_PruneBlocks", kept_block_lookup_size);
        if (kept_block_lookup_mem == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
            api->m_StorageAPI->UnlockFile(api->m_StorageAPI, store_index_lock_file);
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);
            Longtail_Free(store_index);
            Longtail_UnlockSpinLock(api->m_Lock);
            return err;
        }
        struct Longtail_LookupTable* kept_block_lookup = LongtailPrivate_LookupTable_Create(kept_block_lookup_mem, block_count, 0);
        for (uint32_t b = 0; b < block_count; ++b)
        {
            TLongtail_Hash block_hash = api->m_StoreIndex->m_BlockHashes[b];
            LongtailPrivate_LookupTable_PutUnique(kept_block_lookup, block_hash, b);
        }

        for (uint32_t b = 0; b < old_block_count; ++b)
        {
            TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
            if (LongtailPrivate_LookupTable_Get(kept_block_lookup, block_hash))
            {
                continue;
            }
            char* block_path = GetBlockPath(api->m_StorageAPI, api->m_StorePath, api->m_BlockExtension, block_hash);

            // Check if block exists, if it does it is just the store store index that is out of sync.
            // Don't write the block unless we have to
            if (!api->m_StorageAPI->IsFile(api->m_StorageAPI, block_path))
            {
                Longtail_Free((void*)block_path);
                continue;
            }
            err = api->m_StorageAPI->RemoveFile(api->m_StorageAPI, block_path);
            if (err != 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_PruneBlocks() failed to remove file `%s`, error %d", block_path, err);
            }
            Longtail_Free((void*)block_path);
            hmdel(api->m_BlockState, block_hash);
        }
        Longtail_Free(kept_block_lookup_mem);
    }

    api->m_StorageAPI->UnlockFile(api->m_StorageAPI, store_index_lock_file);
    Longtail_Free(store_index);
    Longtail_UnlockSpinLock(api->m_Lock);

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_Count], 1);

    async_complete_api->OnComplete(async_complete_api, pruned_count, 0);

    return 0;
}

static int FSBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(out_stats, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_stats, return EINVAL)
    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&fsblockstore_api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStats_Count], 1);
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        out_stats->m_StatU64[s] = fsblockstore_api->m_StatU64[s];
    }
    return 0;
}

static int FSBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct FSBlockStore2API* api = (struct FSBlockStore2API*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_Count], 1);

    Longtail_LockSpinLock(api->m_Lock);
    intptr_t new_block_count = arrlen(api->m_AddedBlockIndexes);
    int err = 0;
    if (new_block_count > 0)
    {
        err = FSBlockStore_UpdateStoreIndex(api);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_UpdateStoreIndex() failed with %d", err)
        }
    }
#if 0
    if ((err == 0) && api->m_StoreIndex && api->m_StoreIndexIsDirty)
    {
        int err = EnsureParentPathExists(api->m_StorageAPI, api->m_StoreIndexLockPath);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        }
        else
        {
            const char* store_index_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_StorePath, "store.lsi");
            Longtail_StorageAPI_HLockFile store_index_lock_file;
            int err = api->m_StorageAPI->LockFile(api->m_StorageAPI, api->m_StoreIndexLockPath, &store_index_lock_file);
            if (!err)
            {
                if (new_block_count > 0 || (!api->m_StorageAPI->IsFile(api->m_StorageAPI, store_index_path)))
                {
                    err = SafeWriteStoreIndex(api, 1);
                    if (err)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "SafeWriteStoreIndex() failed with %d", err);
                    }
                }
                api->m_StorageAPI->UnlockFile(api->m_StorageAPI, store_index_lock_file);
            }
            Longtail_Free((void*)store_index_path);
        }
    }
#endif // 0

    Longtail_UnlockSpinLock(api->m_Lock);

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_FailCount], 1);
    }

    if (async_complete_api)
    {
        async_complete_api->OnComplete(async_complete_api, err);
        return 0;
    }
    return err;
}

static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, api, return)
    struct FSBlockStore2API* fsblockstore_api = (struct FSBlockStore2API*)api;

    int err = FSBlockStore_Flush(&fsblockstore_api->m_BlockStoreAPI, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_Flush() failed with %d", err);
    }

    hmfree(fsblockstore_api->m_BlockState);
    fsblockstore_api->m_BlockState = 0;
    Longtail_DeleteSpinLock(fsblockstore_api->m_Lock);
    Longtail_Free(fsblockstore_api->m_Lock);
    Longtail_Free((void*)fsblockstore_api->m_StoreIndexLockPath);
    Longtail_Free(fsblockstore_api->m_StorePath);
    Longtail_Free(fsblockstore_api->m_StoreIndex);
    Longtail_Free(fsblockstore_api);
}

static int FSBlockStore_Init(
    void* mem,
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_PersistenceAPI* cache_storage_api,
    const char* content_path,
    const char* block_extension,
    uint64_t unique_id,
    int enable_file_mapping,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(persistence_api, "%p"),
        LONGTAIL_LOGFIELD(cache_storage_api, "%p"),
        LONGTAIL_LOGFIELD(content_path, "%s"),
        LONGTAIL_LOGFIELD(block_extension, "%p"),
        LONGTAIL_LOGFIELD(unique_id, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_block_store_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, persistence_api, return EINVAL)
//    LONGTAIL_FATAL_ASSERT(ctx, cache_storage_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, content_path, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, block_extension, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        FSBlockStore_Dispose,
        FSBlockStore_PutStoredBlock,
        FSBlockStore_PreflightGet,
        FSBlockStore_GetStoredBlock,
        FSBlockStore_GetExistingContent,
        FSBlockStore_PruneBlocks,
        FSBlockStore_GetStats,
        FSBlockStore_Flush);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct FSBlockStore2API* api = (struct FSBlockStore2API*)block_store_api;

    api->m_JobAPI = job_api;
    api->m_Longtail_PersistenceAPI = persistence_api;
    api->m_CacheStorageAPI = cache_storage_api;
    api->m_StorePath = Longtail_Strdup(content_path);
    api->m_StoreIndex = 0;
    api->m_BlockState = 0;
    api->m_AddedBlockIndexes = 0;
    api->m_BlockExtension = (char*)&api[1];
    strcpy((char*)api->m_BlockExtension, block_extension);
//    api->m_StoreIndexLockPath = cache_storage_api->ConcatPath(cache_storage_api, content_path, "store.lsi.sync");

    GetUniqueExtension(unique_id, api->m_TmpExtension);
    api->m_StoreIndexIsDirty = 0;
    api->m_EnableFileMapping = enable_file_mapping;

    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        api->m_StatU64[s] = 0;
    }


    int err = Longtail_CreateSpinLock(Longtail_Alloc("FSBlockStore2API", Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        hmfree(api->m_BlockState);
        api->m_BlockState = 0;
        Longtail_Free(api->m_StoreIndex);
        api->m_StoreIndex = 0;
        return err;
    }
    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStore2API(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_PersistenceAPI* cache_storage_api,
    const char* content_path,
    const char* optional_extension,
    int enable_file_mapping)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(persistence_api, "%p"),
        LONGTAIL_LOGFIELD(cache_storage_api, "%p"),
        LONGTAIL_LOGFIELD(content_path, "%s"),
        LONGTAIL_LOGFIELD(optional_extension, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistence_api != 0, return 0)
//    LONGTAIL_VALIDATE_INPUT(ctx, cache_storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, content_path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, optional_extension == 0 || strlen(optional_extension) < 15, return 0)
    size_t api_size = sizeof(struct FSBlockStore2API);
    const char* block_extension = optional_extension ? optional_extension : ".lrb";
    size_t block_extension_length = strlen(block_extension);
    void* mem = Longtail_Alloc("FSBlockStore2API", api_size + block_extension_length + 1);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    uint64_t computer_id = Longtail_GetProcessIdentity();
    uintptr_t instance_id = (uintptr_t)mem;
    uint64_t unique_id = computer_id ^ instance_id;

    struct Longtail_BlockStoreAPI* block_store_api;
    int err = FSBlockStore_Init(
        mem,
        job_api,
        persistence_api,
        cache_storage_api,
        content_path,
        block_extension,
        unique_id,
        enable_file_mapping,
        &block_store_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_Init() failed with %d", err)
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
#endif // 0 

#endif // 0
