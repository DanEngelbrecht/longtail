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
    Longtail_ReadStorage m_Storge = {PreflightBlocks, AqcuireBlockStorage, ReleaseBlock};
    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockEntry* blocks)
    {
        TestStorage* test_storage = (TestStorage*)storage;
        return 0;
    }
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

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

static uint32_t RecurseTree(uint32_t max_folder_count, const char* root_folder, ProcessEntry entry_processor, void* context)
{
    AssetFolder* asset_folders = new AssetFolder[max_folder_count];

    uint32_t folder_index = 0;
    uint32_t folder_count = 1;

    asset_folders[0].m_FolderPath = _strdup(root_folder);

    FSIterator fs_iterator;
    while (folder_index != folder_count)
    {
        AssetFolder* asset_folder = &asset_folders[folder_index % max_folder_count];

        if (StartFindFile(&fs_iterator, asset_folder->m_FolderPath))
        {
            do
            {
                if (const char* dir_name = GetDirectoryName(&fs_iterator))
                {
                    AssetFolder* new_asset_folder = &asset_folders[folder_count % max_folder_count];
                    assert(new_asset_folder != asset_folder);
                    new_asset_folder->m_FolderPath = ConcatPath(asset_folder->m_FolderPath, dir_name);
                    ++folder_count;
                }
                else if(const char* file_name = GetFileName(&fs_iterator))
                {
                    entry_processor(context, asset_folder->m_FolderPath, file_name);
                }
            }while(FindNextFile(&fs_iterator));
            CloseFindFile(&fs_iterator);
        }
        free((void*)asset_folder->m_FolderPath);
        ++folder_index;
    }
    delete [] asset_folders;
    return folder_count;
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
    TLongtail_Hash m_CompressionType;
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
};

struct DiskBlockStorage
{
    BlockStorage m_Storage = {WriteBlock, ReadBlock};
    DiskBlockStorage(const char* store_path)
        : m_StorePath(store_path)
    {}

    static int WriteBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, const void* data)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        if (void* r = OpenReadFile(path))
        {
            // Already exists and filename indicates content so we know it correct
            CloseReadFile(r);
            return 1;
        }

        const size_t max_dst_size = Lizard_compressBound((int)length);
        void* compressed_buffer = malloc(max_dst_size);
        int compressed_size = Lizard_compress((const char*)data, (char*)compressed_buffer, (int)length, (int)max_dst_size, 43);//LIZARD_MAX_CLEVEL);
        if (compressed_size < 0)
        {
            free(compressed_buffer);
            return 0;
        }
        compressed_buffer = realloc(compressed_buffer, (size_t)compressed_size);

        void* f = OpenWriteFile(path);
        if (!f)
        {
            free(compressed_buffer);
            return 0;
        }
        bool ok = Write(f, 0, compressed_size, compressed_buffer);
        CloseWriteFile(f);
        free((char*)path);
        free(compressed_buffer);
        return ok ? 1 : 0;
    }

    static int ReadBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, void* data)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        void* f = OpenReadFile(path);
        if (!f)
        {
            return 0;
        }
        uint64_t compressed_size = GetFileSize(f);
        void* compressed_buffer = malloc(compressed_size);
        bool ok = Read(f, 0, compressed_size, compressed_buffer);
        CloseReadFile(f);
        free((char*)path);

        if (!ok)
        {
            free(compressed_buffer);
            return false;
        }

        int result = Lizard_decompress_safe((const char*)compressed_buffer, (char*)data, (int)compressed_size, (int)length);
        free(compressed_buffer);
        ok = result >= length;
        return ok ? 1 : 0;
    }

    const char* MakeBlockPath(TLongtail_Hash hash)
    {
        char file_name[64];
        sprintf(file_name, "0x%" PRIx64, hash);
        const char* path = ConcatPath(m_StorePath, file_name);
        return path;
    }
    const char* m_StorePath;
};





///////////////////// TODO: Refine and make part of Longtail

struct WriteStorage
{
    Longtail_WriteStorage m_Storage = {AllocateBlockStorage, WriteBlockData, CommitBlockData};

    WriteStorage(StoredBlock** blocks, uint64_t block_size, uint32_t compressionTypes, BlockStorage* block_storage)
        : m_Blocks(blocks)
        , m_LiveBlocks(0)
        , m_BlockStorage(block_storage)
        , m_BlockSize(block_size)
    {
        size_t hash_table_size = jc::HashTable<uint64_t, StoredBlock*>::CalcSize(compressionTypes);
        m_CompressionToBlocksMem = malloc(hash_table_size);
        m_CompressionToBlocks.Create(compressionTypes, m_CompressionToBlocksMem);
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
        free(m_CompressionToBlocksMem);
    }

    static int AllocateBlockStorage(struct Longtail_WriteStorage* storage, TLongtail_Hash compression_type, uint64_t length, Longtail_BlockEntry* out_block_entry)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        uint32_t** existing_block_idx_array = write_storage->m_CompressionToBlocks.Get(compression_type);
        uint32_t* block_idx_ptr;
        if (existing_block_idx_array == 0)
        {
            uint32_t* block_idx_storage = Longtail_Array_IncreaseCapacity((uint32_t*)0, 16);
            write_storage->m_CompressionToBlocks.Put(compression_type, block_idx_storage);
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
            write_storage->m_CompressionToBlocks.Put(compression_type, block_idx_ptr);
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
        new_block->m_CompressionType = compression_type;
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

    static int WriteBlockData(struct Longtail_WriteStorage* storage, const Longtail_BlockEntry* block_entry, Longtail_InputStream input_stream, void* context)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_entry->m_BlockIndex];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_entry->m_BlockIndex];
        return input_stream(context, block_entry->m_Length, &live_block->m_Data[block_entry->m_StartOffset]);
    }

    static int CommitBlockData(struct Longtail_WriteStorage* storage, const Longtail_BlockEntry* block_entry)
    {
        WriteStorage* write_storage = (WriteStorage*)storage;
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_entry->m_BlockIndex];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_entry->m_BlockIndex];
        live_block->m_CommitedSize = block_entry->m_StartOffset + block_entry->m_Length;
        if (live_block->m_CommitedSize >= (stored_block->m_Size - 512))
        {
            PersistBlock(write_storage, block_entry->m_BlockIndex);
        }

        return 1;
    }

    static int PersistBlock(WriteStorage* write_storage, uint32_t block_index)
    {
        StoredBlock* stored_block = &(*write_storage->m_Blocks)[block_index];
        LiveBlock* live_block = &write_storage->m_LiveBlocks[block_index];
        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, live_block->m_CommitedSize, live_block->m_Data);
        stored_block->m_Hash = MeowU64From(MeowEnd(&state, 0), 0);

        int result = write_storage->m_BlockStorage->BlockStorage_WriteBlock(write_storage->m_BlockStorage, stored_block->m_Hash, live_block->m_CommitedSize, live_block->m_Data);
        free(live_block->m_Data);
        live_block->m_Data = 0;
        stored_block->m_Size = live_block->m_CommitedSize;
        return result;
    }

    jc::HashTable<uint64_t, uint32_t*> m_CompressionToBlocks;
    void* m_CompressionToBlocksMem;
    StoredBlock** m_Blocks;
    LiveBlock* m_LiveBlocks;
    BlockStorage* m_BlockStorage;
    uint64_t m_BlockSize;
};

struct ReadStorage
{
    Longtail_ReadStorage m_Storage = {PreflightBlocks, AqcuireBlockStorage, ReleaseBlock};
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
    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockEntry* blocks)
    {
        return 1;
    }
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
    *buffer = Longtail_Array_SetCapacity(*buffer, (uint32_t)byte_count);
    Longtail_Array_SetSize(*buffer, (uint32_t)byte_count);
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

TEST(Longtail, ScanContent)
{
    ReadyCallback ready_callback;
    Bikeshed shed = Bikeshed_Create(malloc(BIKESHED_SIZE(131071, 0, 1)), 131071, 0, 1, &ready_callback.cb);
    const char* root_path = "D:\\TestContent\\Version_1";
    ProcessHashContext context;
    nadir::TAtomic32 pendingCount = 0;
    nadir::TAtomic32 assetCount = 0;
    context.m_HashJobs = new HashJob[1048576];
    context.m_AssetCount = &assetCount;
    context.m_PendingCount = &pendingCount;
    context.m_Shed = shed;
    nadir::TAtomic32 stop = 0;

    static const uint32_t WORKER_COUNT = 3;
    ThreadWorker workers[WORKER_COUNT];
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].CreateThread(shed, ready_callback.m_Semaphore, &stop);
    }

    RecurseTree(1048576, root_path, ProcessHash, &context);
    while (pendingCount > 0)
    {
        if (Bikeshed_ExecuteOne(shed, 0))
        {
            continue;
        }
        ReadyCallback::Wait(&ready_callback);
    }

    nadir::AtomicAdd32(&stop, 1);
    ReadyCallback::Ready(&ready_callback.cb, 0, WORKER_COUNT);
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].JoinThread();
    }
    uint32_t hash_size = jc::HashTable<meow_u128, char*>::CalcSize(assetCount);
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
    Longtail_BlockEntry* block_entry_array = 0;

    StoredBlock* blocks = 0;

    DiskBlockStorage block_storage("D:\\Temp\\longtail\\cache");
    {
        WriteStorage write_storage(&blocks, 131072u, 512, &block_storage.m_Storage);
        {
            jc::HashTable<uint64_t, Asset*>::Iterator it = hashes.Begin();
            while (it != hashes.End())
            {
                uint64_t content_hash = *it.GetKey();
                Asset* asset = *it.GetValue();
                uint64_t path_hash = GetPathHash(asset->m_Path);
                const char * extension = strrchr(asset->m_Path, '.');
                TLongtail_Hash type_hash = extension ? GetPathHash(extension) : 0;
                if (!Longtail_Write(&write_storage.m_Storage, path_hash, InputStream, asset, asset->m_Size, type_hash, &asset_array, &block_entry_array))
                {
                    assert(false);
                }
                ++it;
            }
        }
    }

    struct Longtail* longtail = Longtail_Open(malloc(Longtail_GetSize(Longtail_Array_GetSize(asset_array), Longtail_Array_GetSize(block_entry_array))),
        Longtail_Array_GetSize(asset_array), asset_array,
        Longtail_Array_GetSize(block_entry_array), block_entry_array);

    Longtail_Array_Free(asset_array);
    Longtail_Array_Free(block_entry_array);

    {
        ReadStorage read_storage(blocks, &block_storage.m_Storage);
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
            MeowAbsorb(&state, Longtail_Array_GetSize(data), data);
            uint64_t verify_hash = MeowU64From(MeowEnd(&state, 0), 0);
            assert(hash_key == verify_hash);

            Longtail_Array_Free(data);
            ++it;
        }
    }

    free(longtail);

    Longtail_Array_Free(blocks);

    free(hash_mem);
    for (int32_t i = 0; i < assetCount; ++i)
    {
        Asset* asset = &context.m_HashJobs[i].m_Asset;
        free((void*)asset->m_Path);
    }
    delete [] context.m_HashJobs;
    free(shed);
}
