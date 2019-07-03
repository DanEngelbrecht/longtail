#include "../third-party/jctest/src/jc_test.h"
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"

#define LONGTAIL_IMPLEMENTATION
#include "../src/longtail.h"
#define BIKESHED_IMPLEMENTATION
#include "../third-party/bikeshed/bikeshed.h"
#include "../third-party/nadir/src/nadir.h"
#include "../third-party/jc_containers/src/jc_hashtable.h"

#include "../third-party/lizard/lib/lizard_common.h"
#include "../third-party/lizard/lib/lizard_decompress.h"
#include "../third-party/lizard/lib/lizard_compress.h"

#include <inttypes.h>

struct TestStorage
{
    Longtail_ReadStorage m_Storge = {/*PreflightBlocks, */AqcuireBlockStorage, ReleaseBlock};
//    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockStore* blocks)
//    {
//        TestStorage* test_storage = (TestStorage*)storage;
//        return 0;
//    }
    static const uint8_t* AqcuireBlockStorage(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        TestStorage* test_storage = (TestStorage*)storage;
        return 0;
    }
    static void ReleaseBlock(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        TestStorage* test_storage = (TestStorage*)storage;
    }
};

TEST(Longtail, Basic)
{
    TestStorage storage;
}

#if defined(_WIN32)

#include <Windows.h>

struct FSIterator
{
    WIN32_FIND_DATAA m_FindData;
    HANDLE m_Handle;
};

static bool IsSkippableFile(FSIterator* fs_iterator)
{
    const char* p = fs_iterator->m_FindData.cFileName;
    if ((*p++) != '.')
    {
        return false;
    }
    if ((*p) == '\0')
    {
        return true;
    }
    if ((*p++) != '.')
    {
        return false;
    }
    if ((*p) == '\0')
    {
        return true;
    }
    return false;
}

static bool Skip(FSIterator* fs_iterator)
{
    while (IsSkippableFile(fs_iterator))
    {
        if (FALSE == ::FindNextFileA(fs_iterator->m_Handle, &fs_iterator->m_FindData))
        {
            return false;
        }
    }
    return true;
}

bool StartFindFile(FSIterator* fs_iterator, const char* path)
{
    char scan_pattern[MAX_PATH];
    strcpy(scan_pattern, path);
    strncat(scan_pattern, "\\*.*", MAX_PATH - strlen(scan_pattern));
    fs_iterator->m_Handle = ::FindFirstFileA(scan_pattern, &fs_iterator->m_FindData);
    if (fs_iterator->m_Handle == INVALID_HANDLE_VALUE)
    {
        return false;
    }
    return Skip(fs_iterator);
}

bool FindNextFile(FSIterator* fs_iterator)
{
    if (FALSE == ::FindNextFileA(fs_iterator->m_Handle, &fs_iterator->m_FindData))
    {
        return false;
    }
    return Skip(fs_iterator);
}

void CloseFindFile(FSIterator* fs_iterator)
{
    ::FindClose(fs_iterator->m_Handle);
    fs_iterator->m_Handle = INVALID_HANDLE_VALUE;
}

const char* GetFileName(FSIterator* fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        return 0;
    }
    return fs_iterator->m_FindData.cFileName;
}

const char* GetDirectoryName(FSIterator* fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        return fs_iterator->m_FindData.cFileName;
    }
    return 0;
}

void* OpenReadFile(const char* path)
{
    HANDLE handle = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 0, OPEN_EXISTING, 0, 0);
    if (handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }
    return (void*)handle;
}

void* OpenWriteFile(const char* path)
{
    HANDLE handle = ::CreateFileA(path, GENERIC_READ | GENERIC_WRITE, 0, 0, CREATE_ALWAYS, 0, 0);
    if (handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }
    return (void*)handle;
}

bool Read(void* handle, uint64_t offset, uint64_t length, void* output)
{
    HANDLE h = (HANDLE)(handle);
    ::SetFilePointer(h, (LONG)offset, 0, FILE_BEGIN);
    return TRUE == ::ReadFile(h, output, (LONG)length, 0, 0);
}

bool Write(void* handle, uint64_t offset, uint64_t length, const void* input)
{
    HANDLE h = (HANDLE)(handle);
    ::SetFilePointer(h, (LONG)offset, 0, FILE_BEGIN);
    return TRUE == ::WriteFile(h, input, (LONG)length, 0, 0);
}

uint64_t GetFileSize(void* handle)
{
    HANDLE h = (HANDLE)(handle);
    return ::GetFileSize(h, 0);
}

void CloseReadFile(void* handle)
{
    HANDLE h = (HANDLE)(handle);
    ::CloseHandle(h);
}

void CloseWriteFile(void* handle)
{
    HANDLE h = (HANDLE)(handle);
    ::CloseHandle(h);
}

const char* ConcatPath(const char* folder, const char* file)
{
    size_t path_len = strlen(folder) + 1 + strlen(file) + 1;
    char* path = (char*)malloc(path_len);
    strcpy(path, folder);
    strcat(path, "\\");
    strcat(path, file);
    return path;
}

#endif

struct Asset
{
    const char* m_Path;
    uint64_t m_Size;
    meow_u128 m_Hash;
};

struct HashJob
{
    Asset m_Asset;
    nadir::TAtomic32* m_PendingCount;
};

static Bikeshed_TaskResult HashFile(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
{
    HashJob* hash_job = (HashJob*)context;
    void* file_handle = OpenReadFile(hash_job->m_Asset.m_Path);
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    if(file_handle)
    {
        uint64_t file_size = GetFileSize(file_handle);
        hash_job->m_Asset.m_Size = file_size;

        uint8_t batch_data[65536];
        uint64_t offset = 0;
        while (offset != file_size)
        {
            meow_umm len = (file_size - offset) < sizeof(batch_data) ? (file_size - offset) : sizeof(batch_data);
            bool read_ok = Read(file_handle, offset, len, batch_data);
            assert(read_ok);
            offset += len;
            MeowAbsorb(&state, len, batch_data);
        }
        CloseReadFile(file_handle);
    }
    meow_u128 hash = MeowEnd(&state, 0);
    hash_job->m_Asset.m_Hash = hash;
    nadir::AtomicAdd32(hash_job->m_PendingCount, -1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

struct AssetFolder
{
    const char* m_FolderPath;
};

struct ProcessHashContext
{
    Bikeshed m_Shed;
    HashJob* m_HashJobs;
    nadir::TAtomic32* m_AssetCount;
    nadir::TAtomic32* m_PendingCount;
};

static void ProcessHash(void* context, const char* root_path, const char* file_name)
{
    ProcessHashContext* process_hash_context = (ProcessHashContext*)context;
    Bikeshed shed = process_hash_context->m_Shed;
    uint32_t asset_count = nadir::AtomicAdd32(process_hash_context->m_AssetCount, 1);
    HashJob* job = &process_hash_context->m_HashJobs[asset_count - 1];
    job->m_Asset.m_Path = ConcatPath(root_path, file_name);
    job->m_Asset.m_Size = 0;
    job->m_PendingCount = process_hash_context->m_PendingCount;
    BikeShed_TaskFunc func[1] = {HashFile};
    void* ctx[1] = {job};
    Bikeshed_TaskID task_id;
    while (!Bikeshed_CreateTasks(shed, 1, func, ctx, &task_id))
    {
        nadir::Sleep(1000);
    }
    {
        nadir::AtomicAdd32(process_hash_context->m_PendingCount, 1);
        Bikeshed_ReadyTasks(shed, 1, &task_id);
    }
}

LONGTAIL_DECLARE_ARRAY_TYPE(AssetFolder, malloc, free)

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

static int RecurseTree(const char* root_folder, ProcessEntry entry_processor, void* context)
{
    AssetFolder* asset_folders = SetCapacity_AssetFolder((AssetFolder*)0, 256);

    uint32_t folder_index = 0;

    Push_AssetFolder(asset_folders)->m_FolderPath = _strdup(root_folder);

    FSIterator fs_iterator;
    while (folder_index != GetSize_AssetFolder(asset_folders))
    {
        const char* asset_folder = asset_folders[folder_index++].m_FolderPath;

        if (StartFindFile(&fs_iterator, asset_folder))
        {
            do
            {
                if (const char* dir_name = GetDirectoryName(&fs_iterator))
                {
                    Push_AssetFolder(asset_folders)->m_FolderPath = ConcatPath(asset_folder, dir_name);
                    if (GetSize_AssetFolder(asset_folders) == GetCapacity_AssetFolder(asset_folders))
                    {
                        AssetFolder* asset_folders_new = SetCapacity_AssetFolder((AssetFolder*)0, GetSize_AssetFolder(asset_folders) + 256);
                        uint32_t unprocessed_count = (GetSize_AssetFolder(asset_folders) - folder_index);
                        if (unprocessed_count > 0)
                        {
                            SetSize_AssetFolder(asset_folders_new, unprocessed_count);
                            memcpy(asset_folders_new, &asset_folders[folder_index], sizeof(AssetFolder) * unprocessed_count);
                        }
                        Free_AssetFolder(asset_folders);
                        asset_folders = asset_folders_new;
                        folder_index = 0;
                    }
                }
                else if(const char* file_name = GetFileName(&fs_iterator))
                {
                    entry_processor(context, asset_folder, file_name);
                }
            }while(FindNextFile(&fs_iterator));
            CloseFindFile(&fs_iterator);
        }
        free((void*)asset_folder);
    }
    Free_AssetFolder(asset_folders);
    return 1;
}

struct ReadyCallback
{
    Bikeshed_ReadyCallback cb = {Ready};
    ReadyCallback()
    {
        m_Semaphore = nadir::CreateSema(malloc(nadir::GetSemaSize()), 0);
    }
    ~ReadyCallback()
    {
        nadir::DeleteSema(m_Semaphore);
        free(m_Semaphore);
    }
    static void Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
    {
        ReadyCallback* cb = (ReadyCallback*)ready_callback;
        nadir::PostSema(cb->m_Semaphore, ready_count);
    }
    static void Wait(ReadyCallback* cb)
    {
        nadir::WaitSema(cb->m_Semaphore);
    }
    nadir::HSema m_Semaphore;
};

struct ThreadWorker
{
    ThreadWorker()
        : stop(0)
        , shed(0)
        , semaphore(0)
        , thread(0)
    {
    }

    ~ThreadWorker()
    {
    }

    bool CreateThread(Bikeshed in_shed, nadir::HSema in_semaphore, nadir::TAtomic32* in_stop)
    {
        shed               = in_shed;
        stop               = in_stop;
        semaphore          = in_semaphore;
        thread             = nadir::CreateThread(malloc(nadir::GetThreadSize()), ThreadWorker::Execute, 0, this);
        return thread != 0;
    }

    void JoinThread()
    {
        nadir::JoinThread(thread, nadir::TIMEOUT_INFINITE);
    }

    void DisposeThread()
    {
        nadir::DeleteThread(thread);
        free(thread);
    }

    static int32_t Execute(void* context)
    {
        ThreadWorker* _this = reinterpret_cast<ThreadWorker*>(context);

        while (*_this->stop == 0)
        {
            if (!Bikeshed_ExecuteOne(_this->shed, 0))
            {
                nadir::WaitSema(_this->semaphore);
            }
        }
        return 0;
    }

    nadir::TAtomic32*   stop;
    Bikeshed            shed;
    nadir::HSema        semaphore;
    nadir::HThread      thread;
};


struct StoredBlock
{
    TLongtail_Hash m_Tag;
    TLongtail_Hash m_Hash;
    uint64_t m_Size;
};

struct LiveBlock
{
    uint8_t* m_Data;
    uint64_t m_CommitedSize;
};

LONGTAIL_DECLARE_ARRAY_TYPE(StoredBlock, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(LiveBlock, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(uint32_t, malloc, free)

// This storage is responsible for storing/retrieveing the data, caching if data is on remote store and compression
struct BlockStorage
{
    int (*BlockStorage_WriteBlock)(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, const void* data);
    int (*BlockStorage_ReadBlock)(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, void* data);
    uint64_t (*BlockStorage_GetStoredSize)(BlockStorage* storage, TLongtail_Hash hash);
};

struct DiskBlockStorage;

struct CompressJob
{
    nadir::TAtomic32* active_job_count;
    BlockStorage* base_storage;
    TLongtail_Hash hash;
    uint64_t length;
    const void* data;
};

static Bikeshed_TaskResult CompressFile(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
{
    CompressJob* compress_job = (CompressJob*)context;
    const size_t max_dst_size = Lizard_compressBound((int)compress_job->length);
    void* compressed_buffer = malloc(sizeof(int32_t) + max_dst_size);

    bool ok = false;
    int compressed_size = Lizard_compress((const char*)compress_job->data, &((char*)compressed_buffer)[sizeof(int32_t)], (int)compress_job->length, (int)max_dst_size, 44);//LIZARD_MAX_CLEVEL);
    if (compressed_size > 0)
    {
        compressed_buffer = realloc(compressed_buffer, (size_t)(sizeof(int) + compressed_size));
        ((int*)compressed_buffer)[0] = (int)compress_job->length;

        compress_job->base_storage->BlockStorage_WriteBlock(compress_job->base_storage, compress_job->hash, sizeof(int32_t) + compressed_size, compressed_buffer);
        free(compressed_buffer);
    }
    nadir::AtomicAdd32(compress_job->active_job_count, -1);
    free(compress_job);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

struct CompressStorage
{
    BlockStorage m_Storage = {WriteBlock, ReadBlock, GetStoredSize};

    CompressStorage(BlockStorage* base_storage, Bikeshed shed, nadir::TAtomic32* active_job_count)
        : m_BaseStorage(base_storage)
        , m_Shed(shed)
        , m_ActiveJobCount(active_job_count)
    {

    }

    static int WriteBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, const void* data)
    {
        CompressStorage* compress_storage = (CompressStorage*)storage;

        uint8_t* p = (uint8_t*)(malloc(sizeof(CompressJob) + length));

        CompressJob* compress_job = (CompressJob*)p;
        p += sizeof(CompressJob);
        compress_job->active_job_count = compress_storage->m_ActiveJobCount;
        compress_job->base_storage = compress_storage->m_BaseStorage;
        compress_job->hash = hash;
        compress_job->length = length;
        compress_job->data = p;

        memcpy((void*)compress_job->data, data, length);
        BikeShed_TaskFunc taskfunc[1] = { CompressFile };
        void* context[1] = {compress_job};
        Bikeshed_TaskID task_ids[1];
        while (!Bikeshed_CreateTasks(compress_storage->m_Shed, 1, taskfunc, context, task_ids))
        {
            nadir::Sleep(1000);
        }
        nadir::AtomicAdd32(compress_storage->m_ActiveJobCount, 1);
        Bikeshed_ReadyTasks(compress_storage->m_Shed, 1, task_ids);
        return 1;
    }

    static int ReadBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, void* data)
    {
        CompressStorage* compress_storage = (CompressStorage*)storage;

        uint64_t compressed_size = compress_storage->m_BaseStorage->BlockStorage_GetStoredSize(compress_storage->m_BaseStorage, hash);
        if (compressed_size == 0)
        {
            return 0;
        }

        char* compressed_buffer = (char*)malloc(compressed_size);
        int ok = compress_storage->m_BaseStorage->BlockStorage_ReadBlock(compress_storage->m_BaseStorage, hash, compressed_size, compressed_buffer);
        if (!ok)
        {
            free(compressed_buffer);
            return false;
        }

        int32_t raw_size = ((int32_t*)compressed_buffer)[0];
        assert(length <= raw_size);

        int result = Lizard_decompress_safe((const char*)(compressed_buffer + sizeof(int32_t)), (char*)data, (int)compressed_size, (int)length);
        free(compressed_buffer);
        ok = result >= length;
        return ok ? 1 : 0;
    }

    static uint64_t GetStoredSize(BlockStorage* storage, TLongtail_Hash hash)
    {
        CompressStorage* compress_storage = (CompressStorage*)storage;
        int32_t size = 0;
        int ok = compress_storage->m_BaseStorage->BlockStorage_ReadBlock(compress_storage->m_BaseStorage, hash, sizeof(int32_t), &size);

        return ok ? size : 0;
    }


    nadir::TAtomic32* m_ActiveJobCount;
    BlockStorage* m_BaseStorage;
    Bikeshed m_Shed;
};

struct DiskBlockStorage
{
    BlockStorage m_Storage = {WriteBlock, ReadBlock, GetStoredSize};
    DiskBlockStorage(const char* store_path)
        : m_StorePath(store_path)
        , m_ActiveJobCount(0)
    {}

    static int WriteBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, const void* data)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;

        const char* path = disk_block_storage->MakeBlockPath(hash);

        void* f = OpenWriteFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }

        bool ok = Write(f, 0, length, data);
        CloseWriteFile(f);
        return ok ? 1 : 0;
    }

    static int ReadBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, void* data)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        void* f = OpenReadFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }

        bool ok = Read(f, 0, length, data);
        CloseReadFile(f);
        return ok ? 1 : 0;
    }

    static uint64_t GetStoredSize(BlockStorage* storage, TLongtail_Hash hash)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        void* f = OpenReadFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }
        uint64_t size = GetFileSize(f);
        CloseReadFile(f);
        return size;
    }

    const char* MakeBlockPath(TLongtail_Hash hash)
    {
        char file_name[64];
        sprintf(file_name, "0x%" PRIx64, hash);
        const char* path = ConcatPath(m_StorePath, file_name);
        return path;
    }
    const char* m_StorePath;
    nadir::TAtomic32 m_ActiveJobCount;
};


struct SimpleWriteStorage
{
    Longtail_WriteStorage m_Storage = {AddExistingBlock, AllocateBlockStorage, WriteBlockData, CommitBlockData, FinalizeBlock};

    SimpleWriteStorage(BlockStorage* block_storage, uint32_t default_block_size)
        : m_BlockStore(block_storage)
        , m_DefaultBlockSize(default_block_size)
        , m_Blocks()
        , m_CurrentBlockIndex(0)
        , m_CurrentBlockData(0)
        , m_CurrentBlockSize(0)
        , m_CurrentBlockUsedSize(0)
    {

    }

    ~SimpleWriteStorage()
    {
        if (m_CurrentBlockData)
        {
            PersistCurrentBlock(this);
            free((void*)m_CurrentBlockData);
        }
        Free_TLongtail_Hash(m_Blocks);
    }

    static int AddExistingBlock(struct Longtail_WriteStorage* storage, TLongtail_Hash hash, uint32_t* out_block_index)
    {
        SimpleWriteStorage* simple_storage = (SimpleWriteStorage*)storage;
        *out_block_index = GetSize_TLongtail_Hash(simple_storage->m_Blocks);
        simple_storage->m_Blocks = EnsureCapacity_TLongtail_Hash(simple_storage->m_Blocks, 16u);
        *Push_TLongtail_Hash(simple_storage->m_Blocks) = hash;
        return 1;
    }

    static int AllocateBlockStorage(struct Longtail_WriteStorage* storage, TLongtail_Hash tag, uint64_t length, Longtail_BlockStore* out_block_entry)
    {
        SimpleWriteStorage* simple_storage = (SimpleWriteStorage*)storage;
        simple_storage->m_Blocks = EnsureCapacity_TLongtail_Hash(simple_storage->m_Blocks, 16u);
        if (simple_storage->m_CurrentBlockData && (simple_storage->m_CurrentBlockUsedSize + length) > simple_storage->m_DefaultBlockSize)
        {
            PersistCurrentBlock(simple_storage);
            if (length > simple_storage->m_CurrentBlockSize)
            {
                free((void*)simple_storage->m_CurrentBlockData);
                simple_storage->m_CurrentBlockData = 0;
                simple_storage->m_CurrentBlockSize = 0;
                simple_storage->m_CurrentBlockUsedSize = 0;
            }
            else
            {
                simple_storage->m_CurrentBlockIndex = GetSize_TLongtail_Hash(simple_storage->m_Blocks);
                *Push_TLongtail_Hash(simple_storage->m_Blocks) = 0xfffffffffffffffflu;
                simple_storage->m_CurrentBlockUsedSize = 0;
            }
        }
        if (0 == simple_storage->m_CurrentBlockData)
        {
            uint32_t block_size = (uint32_t)(length > simple_storage->m_DefaultBlockSize ? length : simple_storage->m_DefaultBlockSize);
            simple_storage->m_CurrentBlockSize = block_size;
            simple_storage->m_CurrentBlockData = (uint8_t*)malloc(block_size);
            simple_storage->m_CurrentBlockUsedSize = 0;
            simple_storage->m_CurrentBlockIndex = GetSize_TLongtail_Hash(simple_storage->m_Blocks);
            *Push_TLongtail_Hash(simple_storage->m_Blocks) = 0xfffffffffffffffflu;
        }
        out_block_entry->m_BlockIndex = simple_storage->m_CurrentBlockIndex;
        out_block_entry->m_StartOffset = simple_storage->m_CurrentBlockUsedSize;
        out_block_entry->m_Length = length;
        simple_storage->m_CurrentBlockUsedSize += (uint32_t)length;
        return 1;
    }

    static int WriteBlockData(struct Longtail_WriteStorage* storage, const Longtail_BlockStore* block_entry, Longtail_InputStream input_stream, void* context)
    {
        SimpleWriteStorage* simple_storage = (SimpleWriteStorage*)storage;
        assert(block_entry->m_BlockIndex == simple_storage->m_CurrentBlockIndex);
        return input_stream(context, block_entry->m_Length, &simple_storage->m_CurrentBlockData[block_entry->m_StartOffset]);
    }

    static int CommitBlockData(struct Longtail_WriteStorage* , const Longtail_BlockStore* )
    {
        return 1;
    }

    static TLongtail_Hash FinalizeBlock(struct Longtail_WriteStorage* storage, uint32_t block_index)
    {
        SimpleWriteStorage* simple_storage = (SimpleWriteStorage*)storage;
        if (block_index == simple_storage->m_CurrentBlockIndex)
        {
            if (0 == PersistCurrentBlock(simple_storage))
            {
                return 0;
            }
            free(simple_storage->m_CurrentBlockData);
            simple_storage->m_CurrentBlockData = 0;
            simple_storage->m_CurrentBlockSize = 0u;
            simple_storage->m_CurrentBlockUsedSize = 0u;
        }
        assert(simple_storage->m_Blocks[block_index] != 0xfffffffffffffffflu);
        return simple_storage->m_Blocks[block_index];
    }

    static int PersistCurrentBlock(SimpleWriteStorage* simple_storage)
    {
        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, simple_storage->m_CurrentBlockUsedSize, simple_storage->m_CurrentBlockData);
        TLongtail_Hash hash = MeowU64From(MeowEnd(&state, 0), 0);
        simple_storage->m_Blocks[simple_storage->m_CurrentBlockIndex] = hash;

        if (simple_storage->m_BlockStore->BlockStorage_GetStoredSize(simple_storage->m_BlockStore, hash) == 0)
        {
            return simple_storage->m_BlockStore->BlockStorage_WriteBlock(simple_storage->m_BlockStore, hash, simple_storage->m_CurrentBlockUsedSize, simple_storage->m_CurrentBlockData);
        }
        return 1;
    }

    BlockStorage* m_BlockStore;
    const uint32_t m_DefaultBlockSize;
    TLongtail_Hash* m_Blocks;
    uint32_t m_CurrentBlockIndex;
    uint8_t* m_CurrentBlockData;
    uint32_t m_CurrentBlockSize;
    uint32_t m_CurrentBlockUsedSize;
};

typedef uint8_t* TBlockData;

LONGTAIL_DECLARE_ARRAY_TYPE(TBlockData, malloc, free)

struct SimpleReadStorage
{
    Longtail_ReadStorage m_Storage = {/*PreflightBlocks, */AqcuireBlockStorage, ReleaseBlock};
    SimpleReadStorage(TLongtail_Hash* block_hashes_array, BlockStorage* block_storage)
        : m_Blocks(block_hashes_array)
        , m_BlockStorage(block_storage)
        , m_BlockData()
    {
        uint32_t block_count = GetSize_TLongtail_Hash(m_Blocks);
        m_BlockData = SetCapacity_TBlockData(m_BlockData, block_count);
        SetSize_TBlockData(m_BlockData, block_count);
        for (uint32_t i = 0; i < block_count; ++i)
        {
            m_BlockData[i] = 0;
        }
    }

    ~SimpleReadStorage()
    {
        Free_TBlockData(m_BlockData);
    }
//    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockStore* blocks)
//    {
//        return 1;
//    }
    static const uint8_t* AqcuireBlockStorage(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        SimpleReadStorage* simple_read_storage = (SimpleReadStorage*)storage;
        if (simple_read_storage->m_BlockData[block_index] == 0)
        {
            TLongtail_Hash hash = simple_read_storage->m_Blocks[block_index];
            uint32_t block_size = (uint32_t)simple_read_storage->m_BlockStorage->BlockStorage_GetStoredSize(simple_read_storage->m_BlockStorage, hash);
            if (block_size == 0)
            {
                return 0;
            }

            simple_read_storage->m_BlockData[block_index] = (uint8_t*)malloc(block_size);
            if (0 == simple_read_storage->m_BlockStorage->BlockStorage_ReadBlock(simple_read_storage->m_BlockStorage, hash, block_size, simple_read_storage->m_BlockData[block_index]))
            {
                free(simple_read_storage->m_BlockData[block_index]);
                simple_read_storage->m_BlockData[block_index] = 0;
                return 0;
            }
        }
        return simple_read_storage->m_BlockData[block_index];
    }

    static void ReleaseBlock(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        SimpleReadStorage* simple_read_storage = (SimpleReadStorage*)storage;
        free(simple_read_storage->m_BlockData[block_index]);
        simple_read_storage->m_BlockData[block_index] = 0;
    }
    TLongtail_Hash* m_Blocks;
    BlockStorage* m_BlockStorage;
    uint8_t** m_BlockData;
};

#if 0
///////////////////// TODO: Refine and make part of Longtail

struct WriteStorage
{
    Longtail_WriteStorage m_Storage = {AddExistingBlock, AllocateBlockStorage, WriteBlockData, CommitBlockData, FinalizeBlock};

    WriteStorage(StoredBlock** blocks, uint64_t block_size, uint32_t tag_types, BlockStorage* block_storage)
        : m_Blocks(blocks)
        , m_LiveBlocks(0)
        , m_BlockStorage(block_storage)
        , m_BlockSize(block_size)
    {
        size_t hash_table_size = jc::HashTable<uint64_t, StoredBlock*>::CalcSize(tag_types);
        m_TagToBlocksMem = malloc(hash_table_size);
        m_TagToBlocks.Create(tag_types, m_TagToBlocksMem);
    }

    ~WriteStorage()
    {
        uint32_t size = Longtail_Array_GetSize(m_LiveBlocks);
        for (uint32_t i = 0; i < size; ++i)
        {
            LiveBlock* last_live_block = &m_LiveBlocks[i];
            if (last_live_block->m_Data)
            {
                PersistBlock(this, i);
            }
        }
        Longtail_Array_Free(m_LiveBlocks);
        free(m_TagToBlocksMem);
    }

    static int AddExistingBlock(struct Longtail_WriteStorage* storage, TLongtail_Hash hash, uint32_t* out_block_index)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        uint32_t size = Longtail_Array_GetSize(*write_storage->m_Blocks);
        uint32_t capacity = Longtail_Array_GetCapacity(*write_storage->m_Blocks);
        if (capacity < (size + 1))
        {
            *write_storage->m_Blocks = Longtail_Array_SetCapacity(*write_storage->m_Blocks, capacity + 16u);
            write_storage->m_LiveBlocks = Longtail_Array_SetCapacity(write_storage->m_LiveBlocks, capacity + 16u);
        }

        StoredBlock* new_block = Longtail_Array_Push(*write_storage->m_Blocks);
        new_block->m_Tag = 0;
        new_block->m_Hash = hash;
        new_block->m_Size = 0;
        LiveBlock* live_block = Longtail_Array_Push(write_storage->m_LiveBlocks);
        live_block->m_Data = 0;
        live_block->m_CommitedSize = 0;
        *out_block_index = size;
        return 1;
    }

    static int AllocateBlockStorage(struct Longtail_WriteStorage* storage, TLongtail_Hash tag, uint64_t length, Longtail_BlockStore* out_block_entry)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        uint32_t** existing_block_idx_array = write_storage->m_TagToBlocks.Get(tag);
        uint32_t* block_idx_ptr;
        if (existing_block_idx_array == 0)
        {
            uint32_t* block_idx_storage = Longtail_Array_IncreaseCapacity((uint32_t*)0, 16);
            write_storage->m_TagToBlocks.Put(tag, block_idx_storage);
            block_idx_ptr = block_idx_storage;
        }
        else
        {
            block_idx_ptr = *existing_block_idx_array;
        }

        uint32_t size = Longtail_Array_GetSize(block_idx_ptr);
        uint32_t capacity = Longtail_Array_GetCapacity(block_idx_ptr);
        if (capacity < (size + 1))
        {
            block_idx_ptr = Longtail_Array_SetCapacity(block_idx_ptr, capacity + 16u);
            write_storage->m_TagToBlocks.Put(tag, block_idx_ptr);
        }

        if (length < write_storage->m_BlockSize)
        {
            size = Longtail_Array_GetSize(block_idx_ptr);
            uint32_t best_fit = size;
            for (uint32_t i = 0; i < size; ++i)
            {
                uint32_t reuse_block_index = block_idx_ptr[i];
                LiveBlock* reuse_live_block = &write_storage->m_LiveBlocks[reuse_block_index];
                if (reuse_live_block->m_Data)
                {
                    StoredBlock* reuse_block = &(*write_storage->m_Blocks)[reuse_block_index];
                    uint64_t space_left = reuse_block->m_Size - reuse_live_block->m_CommitedSize;
                    if (space_left >= length)
                    {
                        if ((best_fit == size) || space_left < ((*write_storage->m_Blocks)[best_fit].m_Size - write_storage->m_LiveBlocks[best_fit].m_CommitedSize))
                        {
                            best_fit = reuse_block_index;
                        }
                    }
                }
            }
            if (best_fit != size)
            {
                out_block_entry->m_BlockIndex = best_fit;
                out_block_entry->m_Length = length;
                out_block_entry->m_StartOffset = write_storage->m_LiveBlocks[best_fit].m_CommitedSize;
                return 1;
            }
        }

        size = Longtail_Array_GetSize(*write_storage->m_Blocks);
        capacity = Longtail_Array_GetCapacity(*write_storage->m_Blocks);
        if (capacity < (size + 1))
        {
            *write_storage->m_Blocks = Longtail_Array_SetCapacity(*write_storage->m_Blocks, capacity + 16u);
            write_storage->m_LiveBlocks = Longtail_Array_SetCapacity(write_storage->m_LiveBlocks, capacity + 16u);
        }
        *Longtail_Array_Push(block_idx_ptr) = size;
        StoredBlock* new_block = Longtail_Array_Push(*write_storage->m_Blocks);
        new_block->m_Tag = tag;
        LiveBlock* live_block = Longtail_Array_Push(write_storage->m_LiveBlocks);
        live_block->m_CommitedSize = 0;
        new_block->m_Size = 0;

        uint64_t block_size = length > write_storage->m_BlockSize ? length : write_storage->m_BlockSize;
        new_block->m_Size = block_size;
        live_block->m_Data = (uint8_t*)malloc(block_size);

        out_block_entry->m_BlockIndex = size;
        out_block_entry->m_Length = length;
        out_block_entry->m_StartOffset = 0;

        return 1;
    }

    static int WriteBlockData(struct Longtail_WriteStorage* storage, const Longtail_BlockStore* block_entry, Longtail_InputStream input_stream, void* context)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_entry->m_BlockIndex];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_entry->m_BlockIndex];
        return input_stream(context, block_entry->m_Length, &live_block->m_Data[block_entry->m_StartOffset]);
    }

    static int CommitBlockData(struct Longtail_WriteStorage* storage, const Longtail_BlockStore* block_entry)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_entry->m_BlockIndex];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_entry->m_BlockIndex];
        live_block->m_CommitedSize = block_entry->m_StartOffset + block_entry->m_Length;
        if (live_block->m_CommitedSize >= (stored_block->m_Size - 8))
        {
            PersistBlock(write_storage, block_entry->m_BlockIndex);
        }

        return 1;
    }

    static TLongtail_Hash FinalizeBlock(struct Longtail_WriteStorage* storage, uint32_t block_index)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_index];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_index];
        if (live_block->m_Data)
        {
            if (0 == PersistBlock(write_storage, block_index))
            {
                return 0;
            }
        }
        return stored_block->m_Hash;
    }

    static int PersistBlock(WriteStorage* write_storage, uint32_t block_index)
    {
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_index];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_index];
        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, live_block->m_CommitedSize, live_block->m_Data);
        stored_block->m_Hash = MeowU64From(MeowEnd(&state, 0), 0);

        if (write_storage->m_BlockStorage->BlockStorage_GetStoredSize(write_storage->m_BlockStorage, stored_block->m_Hash) == 0)
        {
            int result = write_storage->m_BlockStorage->BlockStorage_WriteBlock(write_storage->m_BlockStorage, stored_block->m_Hash, live_block->m_CommitedSize, live_block->m_Data);
            free(live_block->m_Data);
            live_block->m_Data = 0;
            stored_block->m_Size = live_block->m_CommitedSize;
            return result;
        }
        return 1;
    }

    jc::HashTable<uint64_t, uint32_t*> m_TagToBlocks;
    void* m_TagToBlocksMem;
    StoredBlock** m_Blocks;
    LiveBlock* m_LiveBlocks;
    BlockStorage* m_BlockStorage;
    uint64_t m_BlockSize;
};

struct ReadStorage
{
    Longtail_ReadStorage m_Storage = {/*PreflightBlocks, */AqcuireBlockStorage, ReleaseBlock};
    ReadStorage(StoredBlock* stored_blocks, BlockStorage* block_storage)
        : m_Blocks(stored_blocks)
        , m_LiveBlocks(0)
        , m_BlockStorage(block_storage)
    {
        m_LiveBlocks = Longtail_Array_SetCapacity(m_LiveBlocks, Longtail_Array_GetSize(m_Blocks));
    }
    ~ReadStorage()
    {
        Longtail_Array_Free(m_LiveBlocks);
    }
//    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockStore* blocks)
//    {
//        return 1;
//    }
    static const uint8_t* AqcuireBlockStorage(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        ReadStorage* read_storage = (ReadStorage*)storage;
        StoredBlock* stored_block = &read_storage->m_Blocks[block_index];
        LiveBlock* live_block = &read_storage->m_LiveBlocks[block_index];
        live_block->m_Data = (uint8_t*)malloc(stored_block->m_Size);
        live_block->m_CommitedSize = stored_block->m_Size;
        read_storage->m_BlockStorage->BlockStorage_ReadBlock(read_storage->m_BlockStorage, stored_block->m_Hash, live_block->m_CommitedSize, live_block->m_Data);
        return &live_block->m_Data[0];
    }

    static void ReleaseBlock(struct Longtail_ReadStorage* storage, uint32_t block_index)
    {
        ReadStorage* read_storage = (ReadStorage*)storage;
        LiveBlock* live_block = &read_storage->m_LiveBlocks[block_index];
        free(live_block->m_Data);
    }
    StoredBlock* m_Blocks;
    LiveBlock* m_LiveBlocks;
    BlockStorage* m_BlockStorage;
};

#endif














static int InputStream(void* context, uint64_t byte_count, uint8_t* data)
{
    Asset* asset = (Asset*)context;
    void*  f= OpenReadFile(asset->m_Path);
    if (f == 0)
    {
        return 0;
    }
    Read(f, 0, byte_count, data);
    CloseReadFile(f);
    return 1;
}

LONGTAIL_DECLARE_ARRAY_TYPE(uint8_t, malloc, free)

int OutputStream(void* context, uint64_t byte_count, const uint8_t* data)
{
    uint8_t** buffer = (uint8_t**)context;
    *buffer = SetCapacity_uint8_t(*buffer, (uint32_t)byte_count);
    SetSize_uint8_t(*buffer, (uint32_t)byte_count);
    memmove(*buffer, data, byte_count);
    return 1;
}

static uint64_t GetPathHash(const char* path)
{
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    MeowAbsorb(&state, strlen(path), (void*)path);
    uint64_t path_hash = MeowU64From(MeowEnd(&state, 0), 0);
    return path_hash;
}

//LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_AssetEntry, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_BlockStore, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_BlockAssets, malloc, free)

struct Longtail_IndexDiffer
{
    struct Longtail_AssetEntry* m_AssetArray;
    struct Longtail_BlockStore* m_BlockArray;
    struct Longtail_BlockAssets* m_BlockAssets;
    TLongtail_Hash* m_AssetHashes;
};

int Longtail_CompareAssetEntry(const void* element1, const void* element2)
{
    const struct Longtail_AssetEntry* entry1 = (const struct Longtail_AssetEntry*)element1;
    const struct Longtail_AssetEntry* entry2 = (const struct Longtail_AssetEntry*)element2;
    return (int)((int64_t)entry1->m_BlockStore.m_BlockIndex - (int64_t)entry2->m_BlockStore.m_BlockIndex);
}

Longtail_IndexDiffer* CreateDiffer(void* mem, uint32_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint32_t block_entry_count, struct Longtail_BlockStore* block_entries, uint32_t new_asset_count, TLongtail_Hash* new_asset_hashes)
{
    Longtail_IndexDiffer* index_differ = (Longtail_IndexDiffer*)mem;
    index_differ->m_AssetArray = 0;
    index_differ->m_BlockArray = 0;
//    index_differ->m_AssetArray = Longtail_Array_SetCapacity(index_differ->m_AssetArray, asset_entry_count);
//    index_differ->m_BlockArray = Longtail_Array_SetCapacity(index_differ->m_BlockArray, block_entry_count);
//    memcpy(index_differ->m_AssetArray, asset_entries, sizeof(Longtail_AssetEntry) * asset_entry_count);
//    memcpy(index_differ->m_BlockArray, block_entries, sizeof(Longtail_BlockStore) * block_entry_count);
    qsort(asset_entries, asset_entry_count, sizeof(Longtail_AssetEntry), Longtail_CompareAssetEntry);
    uint32_t hash_size = jc::HashTable<uint64_t, uint32_t>::CalcSize(new_asset_count);
    void* hash_mem = malloc(hash_size);
    jc::HashTable<uint64_t, uint32_t> hashes;
    hashes.Create(new_asset_count, hash_mem);
    for (uint32_t i = 0; i < new_asset_count; ++i)
    {
        hashes.Put(new_asset_hashes[i], new_asset_count);
    }
    uint32_t b = 0;
    while (b < asset_entry_count)
    {
        uint32_t block_index = asset_entries[b].m_BlockStore.m_BlockIndex;
        uint32_t scan_b = b;
        uint32_t found_assets = 0;

        while (scan_b < asset_entry_count && (asset_entries[scan_b].m_BlockStore.m_BlockIndex) == block_index)
        {
            if (hashes.Get(asset_entries[scan_b].m_AssetHash))
            {
                ++found_assets;
            }
            ++scan_b;
        }
        uint32_t assets_in_block = scan_b - b;
        if (assets_in_block == found_assets)
        {
            for (uint32_t f = b; f < scan_b; ++f)
            {
                hashes.Put(asset_entries[f].m_AssetHash, block_index);
            }
        }
    }
    // We now have a map with assets that are not in the index already

    // TODO: Need to sort out the asset->block_entry->block_hash thing
    // How we add data to an existing block store, indexes and all

    index_differ->m_AssetArray = SetCapacity_Longtail_AssetEntry(index_differ->m_AssetArray, asset_entry_count);
    index_differ->m_BlockArray = SetCapacity_Longtail_BlockStore(index_differ->m_BlockArray, block_entry_count);

    for (uint32_t a = 0; a < new_asset_count; ++a)
    {
        TLongtail_Hash asset_hash = new_asset_hashes[a];
        uint32_t* existing_block_index = hashes.Get(asset_hash);
        if (existing_block_index)
        {
            Longtail_AssetEntry* asset_entry = Push_Longtail_AssetEntry(index_differ->m_AssetArray);
            Longtail_BlockStore* block_entry = Push_Longtail_BlockStore(index_differ->m_BlockArray);
            block_entry->m_BlockIndex = *existing_block_index;
            asset_entry->m_AssetHash = 0;
        }
    }

    return index_differ;
}

static TLongtail_Hash GetContentTag(const char* path)
{
    const char * extension = strrchr(path, '.');
    if (extension)
    {
        if (strcmp(extension, ".uasset") == 0)
        {
            return 1000;
        }
        if (strcmp(extension, ".uexp") == 0)
        {
            if (strstr(path, "Meshes"))
            {
                return GetPathHash("Meshes");
            }
            if (strstr(path, "Textures"))
            {
                return GetPathHash("Textures");
            }
            if (strstr(path, "Sounds"))
            {
                return GetPathHash("Sounds");
            }
            if (strstr(path, "Animations"))
            {
                return GetPathHash("Animations");
            }
            if (strstr(path, "Blueprints"))
            {
                return GetPathHash("Blueprints");
            }
            if (strstr(path, "Characters"))
            {
                return GetPathHash("Characters");
            }
            if (strstr(path, "Effects"))
            {
                return GetPathHash("Effects");
            }
            if (strstr(path, "Materials"))
            {
                return GetPathHash("Materials");
            }
            if (strstr(path, "Maps"))
            {
                return GetPathHash("Maps");
            }
            if (strstr(path, "Movies"))
            {
                return GetPathHash("Movies");
            }
            if (strstr(path, "Slate"))
            {
                return GetPathHash("Slate");
            }
            if (strstr(path, "Sounds"))
            {
                return GetPathHash("MeshSoundses");
            }
        }
        return GetPathHash(extension);
    }
    return 2000;
}

struct ScanExistingDataContext
{
//    Bikeshed m_Shed;
//    HashJob* m_HashJobs;
//    nadir::TAtomic32* m_AssetCount;
//    nadir::TAtomic32* m_PendingCount;
    TLongtail_Hash* m_Hashes;
};

static void ScanHash(void* context, const char* , const char* file_name)
{
    ScanExistingDataContext* scan_context = (ScanExistingDataContext*)context;
    TLongtail_Hash hash;
    if (1 == sscanf(file_name, "0x%" PRIx64, &hash))
    {
        scan_context->m_Hashes = EnsureCapacity_TLongtail_Hash(scan_context->m_Hashes, 16u);
        *(Push_TLongtail_Hash(scan_context->m_Hashes)) = hash;
    }
}


TEST(Longtail, ScanContent)
{
    ReadyCallback ready_callback;
    Bikeshed shed = Bikeshed_Create(malloc(BIKESHED_SIZE(131071, 0, 1)), 131071, 0, 1, &ready_callback.cb);

    nadir::TAtomic32 stop = 0;

    static const uint32_t WORKER_COUNT = 7;
    ThreadWorker workers[WORKER_COUNT];
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].CreateThread(shed, ready_callback.m_Semaphore, &stop);
    }


    const char* root_path = "D:\\TestContent\\Version_1";
    const char* cache_path = "D:\\Temp\\longtail\\cache";

    ScanExistingDataContext scan_context;
    scan_context.m_Hashes = 0;
    RecurseTree(cache_path, ScanHash, &scan_context);
    uint32_t found_count = GetSize_TLongtail_Hash(scan_context.m_Hashes);
    for (uint32_t b = 0; b < found_count; ++b)
    {
        printf("Block %u, hash %llu\n",
            b,
            scan_context.m_Hashes[b]);
    }
    Free_TLongtail_Hash(scan_context.m_Hashes);


    nadir::TAtomic32 pendingCount = 0;
    nadir::TAtomic32 assetCount = 0;
 
    ProcessHashContext context;
    context.m_Shed = shed;
    context.m_HashJobs = new HashJob[1048576];
    context.m_AssetCount = &assetCount;
    context.m_PendingCount = &pendingCount;

    RecurseTree(root_path, ProcessHash, &context);

    int32_t old_pending_count = 0;
    while (pendingCount > 0)
    {
        if (Bikeshed_ExecuteOne(shed, 0))
        {
            continue;
        }
        if (old_pending_count != pendingCount)
        {
            old_pending_count = pendingCount;
            printf("Files left to hash: %d\n", old_pending_count);
        }
        nadir::Sleep(1000);
    }

    uint32_t hash_size = jc::HashTable<uint64_t, char*>::CalcSize(assetCount);
    void* hash_mem = malloc(hash_size);
    jc::HashTable<uint64_t, Asset*> hashes;
    hashes.Create(assetCount, hash_mem);

    uint64_t redundant_file_count = 0;
    uint64_t redundant_byte_size = 0;
    uint64_t total_byte_size = 0;

    for (int32_t i = 0; i < assetCount; ++i)
    {
        Asset* asset = &context.m_HashJobs[i].m_Asset;
        total_byte_size += asset->m_Size;
        meow_u128 hash = asset->m_Hash;
        uint64_t hash_key = MeowU64From(hash, 0);
        Asset** existing = hashes.Get(hash_key);
        if (existing)
        {
            if (MeowHashesAreEqual(asset->m_Hash, (*existing)->m_Hash))
            {
                printf("File `%s` matches file `%s` (%llu bytes): %08X-%08X-%08X-%08X\n",
                    asset->m_Path,
                    (*existing)->m_Path,
                    asset->m_Size,
                    MeowU32From(hash, 3), MeowU32From(hash, 2), MeowU32From(hash, 1), MeowU32From(hash, 0));
                redundant_byte_size += asset->m_Size;
                ++redundant_file_count;
                continue;
            }
            else
            {
                printf("Collision!\n");
                assert(false);
            }
        }
        hashes.Put(hash_key, asset);
    }
    printf("Found %llu redundant files comprising %llu bytes out of %llu bytes\n", redundant_file_count, redundant_byte_size, total_byte_size);

    Longtail_AssetEntry* asset_array = 0;
    Longtail_BlockStore* block_entry_array = 0;

    Longtail_Builder builder;

    DiskBlockStorage disk_block_storage(cache_path);
    CompressStorage block_storage(&disk_block_storage.m_Storage, shed, &pendingCount);
    {
        SimpleWriteStorage write_storage(&block_storage.m_Storage, 131072u);
        Longtail_Builder_Initialize(&builder, &write_storage.m_Storage);
        {
            jc::HashTable<uint64_t, Asset*>::Iterator it = hashes.Begin();
            while (it != hashes.End())
            {
                uint64_t content_hash = *it.GetKey();
                Asset* asset = *it.GetValue();
                uint64_t path_hash = GetPathHash(asset->m_Path);
                TLongtail_Hash tag = GetContentTag(asset->m_Path);


                if (!Longtail_Builder_Add(&builder, path_hash, InputStream, asset, asset->m_Size, tag))
                {
                    assert(false);
                }
                ++it;
            }

            Longtail_FinalizeBuilder(&builder);

            uint32_t asset_count = GetSize_Longtail_AssetEntry(builder.m_AssetEntries);
            uint32_t block_count = GetSize_TLongtail_Hash(builder.m_BlockHashes);

            for (uint32_t a = 0; a < asset_count; ++a)
            {
                Longtail_AssetEntry* asset_entry = &builder.m_AssetEntries[a];
                printf("Asset %u, %llu, in block %u, at %llu, size %llu\n",
                    a,
                    asset_entry->m_AssetHash,
                    asset_entry->m_BlockStore.m_BlockIndex,
                    asset_entry->m_BlockStore.m_StartOffset,
                    asset_entry->m_BlockStore.m_Length);
            }

            for (uint32_t b = 0; b < block_count; ++b)
            {
                printf("Block %u, hash %llu\n",
                    b,
                    builder.m_BlockHashes[b]);
            }
        }
    }


    old_pending_count = 0;
    while (pendingCount > 0)
    {
        if (Bikeshed_ExecuteOne(shed, 0))
        {
            continue;
        }
        if (old_pending_count != pendingCount)
        {
            old_pending_count = pendingCount;
            printf("Files left to store: %d\n", old_pending_count);
        }
        nadir::Sleep(1000);
    }

    printf("Comitted %u files\n", hashes.Size());

    struct Longtail* longtail = Longtail_Open(malloc(Longtail_GetSize(GetSize_Longtail_AssetEntry(builder.m_AssetEntries), GetSize_TLongtail_Hash(builder.m_BlockHashes))),
        GetSize_Longtail_AssetEntry(builder.m_AssetEntries), builder.m_AssetEntries,
        GetSize_TLongtail_Hash(builder.m_BlockHashes), builder.m_BlockHashes);

    {
        SimpleReadStorage read_storage(builder.m_BlockHashes, &block_storage.m_Storage);
        jc::HashTable<uint64_t, Asset*>::Iterator it = hashes.Begin();
        while (it != hashes.End())
        {
            uint64_t hash_key = *it.GetKey();
            Asset* asset = *it.GetValue();
            uint64_t path_hash = GetPathHash(asset->m_Path);

            uint8_t* data = 0;
            if (!Longtail_Read(longtail, &read_storage.m_Storage, path_hash, OutputStream, &data))
            {
                assert(false);
            }

            meow_state state;
            MeowBegin(&state, MeowDefaultSeed);
            MeowAbsorb(&state, GetSize_uint8_t(data), data);
            uint64_t verify_hash = MeowU64From(MeowEnd(&state, 0), 0);
            assert(hash_key == verify_hash);

            Free_uint8_t(data);
            ++it;
        }
    }
    printf("Read back %u files\n", hashes.Size());

    free(longtail);

    free(hash_mem);
    for (int32_t i = 0; i < assetCount; ++i)
    {
        Asset* asset = &context.m_HashJobs[i].m_Asset;
        free((void*)asset->m_Path);
    }
    delete [] context.m_HashJobs;

    nadir::AtomicAdd32(&stop, 1);
    ReadyCallback::Ready(&ready_callback.cb, 0, WORKER_COUNT);
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].JoinThread();
    }

    free(shed);
}
