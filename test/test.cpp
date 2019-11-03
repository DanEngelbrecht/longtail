#include "../third-party/jctest/src/jc_test.h"
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"

#define LONGTAIL_IMPLEMENTATION
#include "../src/longtail.h"
#define BIKESHED_IMPLEMENTATION
#include "../third-party/bikeshed/bikeshed.h"
#include "../third-party/nadir/src/nadir.h"
#include "../third-party/jc_containers/src/jc_hashtable.h"
#include "../third-party/trove/src/trove.h"

#include "../third-party/lizard/lib/lizard_common.h"
#include "../third-party/lizard/lib/lizard_decompress.h"
#include "../third-party/lizard/lib/lizard_compress.h"

#include <inttypes.h>

#include <algorithm>

#if defined(_WIN32)
    #include <malloc.h>
    #define alloca _alloca
#else
    #include <alloca.h>
#endif

#if defined(_WIN32)

void Trove_NormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '\\' ? '/' : *path;
    }
}

void Trove_DenormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '/' ? '\\' : *path;
    }
}

int Trove_EnsureParentPathExistsNative(char* path)
{
	char* last_path_delimiter = (char*)strrchr(path, '\\');
	if (last_path_delimiter == 0)
	{
		return 1;
	}
	*last_path_delimiter = '\0';
	DWORD attr = ::GetFileAttributesA(path);
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		if (!Trove_EnsureParentPathExistsNative(path))
		{
			*last_path_delimiter = '\\';
			return 0;
		}
		BOOL success = ::CreateDirectoryA(path, 0);
		*last_path_delimiter = '\\';
		return success ? 1 : 0;
	}
	*last_path_delimiter = '\\';
	return 1;
}

int Trove_EnsureParentPathExists(char* path)
{
	Trove_DenormalizePath(path);
	int success = Trove_EnsureParentPathExistsNative(path);
	Trove_NormalizePath(path);
	return success;
}

#endif

#if defined(__APPLE__) || defined(__linux__)

void Trove_NormalizePath(char* )
{

}

void Trove_DenormalizePath(char* )
{

}

#endif

namespace Jobs
{

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

}







struct AssetFolder
{
    const char* m_FolderPath;
};

LONGTAIL_DECLARE_ARRAY_TYPE(AssetFolder, malloc, free)

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

static int RecurseTree(const char* root_folder, ProcessEntry entry_processor, void* context)
{
    AssetFolder* asset_folders = SetCapacity_AssetFolder((AssetFolder*)0, 256);

    uint32_t folder_index = 0;

    Push_AssetFolder(asset_folders)->m_FolderPath = strdup(root_folder);

    HTrove_FSIterator fs_iterator = (HTrove_FSIterator)alloca(Trove_GetFSIteratorSize());
    while (folder_index != GetSize_AssetFolder(asset_folders))
    {
        const char* asset_folder = asset_folders[folder_index++].m_FolderPath;

        if (Trove_StartFind(fs_iterator, asset_folder))
        {
            do
            {
                if (const char* dir_name = Trove_GetDirectoryName(fs_iterator))
                {
                    Push_AssetFolder(asset_folders)->m_FolderPath = Trove_ConcatPath(asset_folder, dir_name);
                    if (GetSize_AssetFolder(asset_folders) == GetCapacity_AssetFolder(asset_folders))
                    {
                        uint32_t unprocessed_count = (GetSize_AssetFolder(asset_folders) - folder_index);
                        if (folder_index > 0)
                        {
                            if (unprocessed_count > 0)
                            {
                                memmove(asset_folders, &asset_folders[folder_index], sizeof(AssetFolder) * unprocessed_count);
                                SetSize_AssetFolder(asset_folders, unprocessed_count);
                            }
                            folder_index = 0;
                        }
                        else
                        {
                            AssetFolder* asset_folders_new = SetCapacity_AssetFolder((AssetFolder*)0, GetCapacity_AssetFolder(asset_folders) + 256);
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
                }
                else if(const char* file_name = Trove_GetFileName(fs_iterator))
                {
                    entry_processor(context, asset_folder, file_name);
                }
            }while(Trove_FindNext(fs_iterator));
            Trove_CloseFind(fs_iterator);
        }
        free((void*)asset_folder);
    }
    Free_AssetFolder(asset_folders);
    return 1;
}

struct AssetPaths
{
    char** paths;
    uint32_t count;
};

void FreeAssetpaths(AssetPaths* assets)
{
    for (uint32_t i = 0; i < assets->count; ++i)
    {
        free(assets->paths[i]);
    }

    free(assets->paths);
}

int GetContent(const char* root_path, AssetPaths* out_assets)
{
    out_assets->paths = 0;
    out_assets->count = 0;

    struct Context {
        const char* root_path;
        AssetPaths* assets;
    };

    auto add_folder = [](void* context, const char* root_path, const char* file_name)
    {
        Context* asset_context = (Context*)context;
        AssetPaths* assets = asset_context->assets;

        size_t base_path_length = strlen(asset_context->root_path);

        char* full_path = (char*)Trove_ConcatPath(root_path, file_name);
        Trove_NormalizePath(full_path);
        const char* s = &full_path[base_path_length];
        if (*s == '/')
        {
            ++s;
        }

        assets->paths = (char**)realloc(assets->paths, sizeof(char*) * (assets->count + 1));
        assets->paths[assets->count] = strdup(s);

        free(full_path);
        full_path = 0;

        ++assets->count;
    };

    Context context = { root_path, out_assets };

    RecurseTree(root_path, add_folder, &context);

    return 0;
}

struct HashJob
{
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_ContentHash;
    uint32_t* m_ContentSize;
    const char* m_RootPath;
    const char* m_Path;
    nadir::TAtomic32* m_PendingCount;
};

static TLongtail_Hash GetPathHash(const char* path)
{
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    MeowAbsorb(&state, strlen(path), (void*)path);
    TLongtail_Hash path_hash = MeowU64From(MeowEnd(&state, 0), 0);
    return path_hash;
}

static Bikeshed_TaskResult HashFile(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
{
    HashJob* hash_job = (HashJob*)context;
    char* path = (char*)Trove_ConcatPath(hash_job->m_RootPath, hash_job->m_Path);
    Trove_DenormalizePath(path);

    HTroveOpenReadFile file_handle = Trove_OpenReadFile(path);
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    if(file_handle)
    {
        uint32_t file_size = (uint32_t)Trove_GetFileSize(file_handle);
        *hash_job->m_ContentSize = file_size;

        uint8_t batch_data[65536];
        uint32_t offset = 0;
        while (offset != file_size)
        {
            meow_umm len = (meow_umm)((file_size - offset) < sizeof(batch_data) ? (file_size - offset) : sizeof(batch_data));
            bool read_ok = Trove_Read(file_handle, offset, len, batch_data);
            assert(read_ok);
            offset += len;
            MeowAbsorb(&state, len, batch_data);
        }
        Trove_CloseReadFile(file_handle);
    }
    meow_u128 hash = MeowEnd(&state, 0);
    *hash_job->m_ContentHash = MeowU64From(hash, 0);
    *hash_job->m_PathHash = GetPathHash(hash_job->m_Path);
    nadir::AtomicAdd32(hash_job->m_PendingCount, -1);
    free((char*)path);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

struct ProcessHashContext
{
    Bikeshed m_Shed;
    HashJob* m_HashJobs;
    nadir::TAtomic32* m_PendingCount;
};

void GetFileHashes(Bikeshed shed, const char* root_path, const char** paths, uint64_t asset_count, TLongtail_Hash* pathHashes, TLongtail_Hash* contentHashes, uint32_t* contentSizes)
{
    ProcessHashContext context;
    context.m_Shed = shed;
    context.m_HashJobs = new HashJob[asset_count];
    nadir::TAtomic32 pendingCount = 0;
    context.m_PendingCount = &pendingCount;

    uint64_t assets_left = asset_count;
    static const uint32_t BATCH_SIZE = 64;
    Bikeshed_TaskID task_ids[BATCH_SIZE];
    BikeShed_TaskFunc func[BATCH_SIZE];
    void* ctx[BATCH_SIZE];
    for (uint32_t i = 0; i < BATCH_SIZE; ++i)
    {
        func[i] = HashFile;
    }
    uint64_t offset = 0;
    while (offset < asset_count) {
        uint64_t assets_left = asset_count - offset;
        uint32_t batch_count = assets_left > BATCH_SIZE ? BATCH_SIZE : (uint32_t)assets_left;
        for (uint32_t i = 0; i < batch_count; ++i)
        {
            HashJob* job = &context.m_HashJobs[offset + i];
            ctx[i] = &context.m_HashJobs[i + offset];
            job->m_RootPath = root_path;
            job->m_Path = paths[i + offset];
            job->m_PendingCount = &pendingCount;
            job->m_PathHash = &pathHashes[i + offset];
            job->m_ContentHash = &contentHashes[i + offset];
            job->m_ContentSize = &contentSizes[i + offset];
        }

        while (!Bikeshed_CreateTasks(shed, batch_count, func, ctx, task_ids))
        {
            nadir::Sleep(1000);
        }

        {
            nadir::AtomicAdd32(&pendingCount, batch_count);
            Bikeshed_ReadyTasks(shed, batch_count, task_ids);
        }
        offset += batch_count;
    }

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

    delete [] context.m_HashJobs;
}

struct VersionIndex
{
    uint64_t* m_AssetCount;
    TLongtail_Hash* m_PathHash;
    TLongtail_Hash* m_AssetContentHash;
    uint32_t* m_AssetSize;
    uint32_t* m_NameOffset;
    uint32_t m_NameDataSize;
    char* m_NameData;
};

uint32_t GetNameDataSize(uint32_t asset_count, const char* const* asset_paths)
{
    size_t name_data_size = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        name_data_size += strlen(asset_paths[i]) + 1;
    }
    return (uint32_t)name_data_size;
}

size_t GetVersionIndexDataSize(uint32_t asset_count, uint32_t name_data_size)
{
    size_t version_index_size = sizeof(uint64_t) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        name_data_size;

    return version_index_size;
}

size_t GetVersionIndexSize(uint32_t asset_count, const char* const* asset_paths)
{
    return sizeof(VersionIndex) +
            GetVersionIndexDataSize(asset_count, GetNameDataSize(asset_count, asset_paths));
}

void InitVersionIndex(VersionIndex* version_index, size_t version_index_data_size)
{
    char* p = (char*)version_index;
    p += sizeof(VersionIndex);

    size_t version_index_data_start = (size_t)p;

    version_index->m_AssetCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t asset_count = *version_index->m_AssetCount;

    version_index->m_PathHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetContentHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetSize = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_NameOffset = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);

    size_t version_index_name_data_start = (size_t)p;

    version_index->m_NameDataSize = (uint32_t)(version_index_data_size - (version_index_name_data_start - version_index_data_start));

    version_index->m_NameData = (char*)p;
}

VersionIndex* BuildVersionIndex(
    void* mem,
    size_t mem_size,
    uint32_t asset_count,
    const char* const* asset_paths,
    const TLongtail_Hash* pathHashes,
    const TLongtail_Hash* contentHashes,
    const uint32_t* contentSizes)
{
    VersionIndex* version_index = (VersionIndex*)mem;
    version_index->m_AssetCount = (uint64_t*)&((char*)mem)[sizeof(VersionIndex)];
    *version_index->m_AssetCount = asset_count;

    InitVersionIndex(version_index, mem_size - sizeof(VersionIndex));

    uint32_t name_offset = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        version_index->m_PathHash[i] = pathHashes[i];
        version_index->m_AssetContentHash[i] = contentHashes[i];
        version_index->m_AssetSize[i] = contentSizes[i];
        version_index->m_NameOffset[i] = name_offset;
        uint32_t path_length = (uint32_t)strlen(asset_paths[i]) + 1;
        memcpy(&version_index->m_NameData[name_offset], asset_paths[i], path_length);
        name_offset += path_length;
    }

    return version_index;
}

// Very hacky!
static TLongtail_Hash GetContentTag(const char* , const char* path)
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

struct BlockIndexEntry
{
    TLongtail_Hash m_AssetContentHash;
    uint32_t m_AssetSize;
};

struct BlockIndex
{
    TLongtail_Hash m_BlockHash;
    uint32_t m_AssetCountInBlock;
    BlockIndexEntry* m_Entries;
};

BlockIndex* InitBlockIndex(void* mem, uint32_t asset_count_in_block)
{
    BlockIndex* block_index = (BlockIndex*)mem;
    block_index->m_AssetCountInBlock = asset_count_in_block;
    block_index->m_Entries = (BlockIndexEntry*)&block_index[1];
    return block_index;
}

size_t GetBlockIndexSize(uint32_t asset_count_in_block)
{
    size_t block_index_size = sizeof(BlockIndex) +
        (sizeof(BlockIndexEntry) * asset_count_in_block);

    return block_index_size;
}

BlockIndex* CreateBlockIndex(
    void* mem,
    uint32_t asset_count_in_block,
    uint32_t* asset_indexes,
    const TLongtail_Hash* asset_content_hashes,
    const uint32_t* asset_sizes)
{
    BlockIndex* block_index = InitBlockIndex(mem, asset_count_in_block);
	meow_state state;
	MeowBegin(&state, MeowDefaultSeed);
	for (uint32_t i = 0; i < asset_count_in_block; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        block_index->m_Entries[i].m_AssetContentHash = asset_content_hashes[asset_index];
        block_index->m_Entries[i].m_AssetSize = asset_sizes[asset_index];
		MeowAbsorb(&state, (meow_umm)(sizeof(TLongtail_Hash)), (void*)&asset_content_hashes[asset_index]);
		MeowAbsorb(&state, (meow_umm)(sizeof(uint32_t)), (void*)&asset_sizes[asset_index]);
	}
	MeowAbsorb(&state, (meow_umm)(sizeof(uint32_t)), (void*)& asset_count_in_block);

    TLongtail_Hash block_hash = MeowU64From(MeowEnd(&state, 0), 0);
    block_index->m_BlockHash = block_hash;

    return block_index;
}

struct ContentIndex
{
    uint64_t* m_BlockCount;
    uint64_t* m_AssetCount;

    TLongtail_Hash* m_BlockHash; // []
    TLongtail_Hash* m_AssetContentHash; // []
    uint64_t* m_AssetBlockIndex; // []
    uint32_t* m_AssetBlockOffset; // []
    uint32_t* m_AssetLength; // []
};

size_t GetContentIndexDataSize(uint64_t block_count, uint64_t asset_count)
{
    size_t block_index_data_size = sizeof(uint64_t) +
        sizeof(uint64_t) +
        (sizeof(TLongtail_Hash) * block_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(TLongtail_Hash) * asset_count) +
        (sizeof(uint32_t) * asset_count) +
        (sizeof(uint32_t) * asset_count);

    return block_index_data_size;
}

size_t GetContentIndexSize(uint64_t block_count, uint64_t asset_count)
{
    return sizeof(ContentIndex) +
        GetContentIndexDataSize(block_count, asset_count);
}

void InitContentIndex(ContentIndex* content_index)
{
    char* p = (char*)&content_index[1];
    content_index->m_BlockCount = (uint64_t*)p;
    p += sizeof(uint64_t);
    content_index->m_AssetCount = (uint64_t*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t asset_count = *content_index->m_AssetCount;

    content_index->m_BlockHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_AssetContentHash = (TLongtail_Hash*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);
    content_index->m_AssetBlockIndex = (uint64_t*)p;
    p += (sizeof(uint64_t) * asset_count);
    content_index->m_AssetBlockOffset = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
    content_index->m_AssetLength = (uint32_t*)p;
    p += (sizeof(uint32_t) * asset_count);
}

int WriteContentIndex(ContentIndex* content_index, const char* path)
{
    size_t index_data_size = GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_AssetCount);

    HTroveOpenWriteFile file_handle = Trove_OpenWriteFile(path);
    if (!file_handle)
    {
        return 0;
    }
    if (!Trove_Write(file_handle, 0, index_data_size, &content_index[1]))
    {
        return 0;
    }
    Trove_CloseWriteFile(file_handle);

    return 1;
}

ContentIndex* ReadContentIndex(const char* path)
{
    HTroveOpenReadFile file_handle = Trove_OpenReadFile(path);
    if (!file_handle)
    {
        return 0;
    }
    size_t content_index_data_size = Trove_GetFileSize(file_handle);
    ContentIndex* content_index = (ContentIndex*)malloc(sizeof(ContentIndex) + content_index_data_size);
    if (!content_index)
    {
        Trove_CloseReadFile(file_handle);
        return 0;
    }
    if (!Trove_Read(file_handle, 0, content_index_data_size, &content_index[1]))
    {
        Trove_CloseReadFile(file_handle);
        return 0;
    }
    InitContentIndex(content_index);
    Trove_CloseReadFile(file_handle);
    return content_index;
}

int WriteVersionIndex(VersionIndex* version_index, const char* path)
{
    size_t index_data_size = GetVersionIndexDataSize((uint32_t)(*version_index->m_AssetCount), version_index->m_NameDataSize);

    HTroveOpenWriteFile file_handle = Trove_OpenWriteFile(path);
    if (!file_handle)
    {
        return 0;
    }
    if (!Trove_Write(file_handle, 0, index_data_size, &version_index[1]))
    {
        return 0;
    }
    Trove_CloseWriteFile(file_handle);

    return 1;
}

VersionIndex* ReadVersionIndex(const char* path)
{
    HTroveOpenReadFile file_handle = Trove_OpenReadFile(path);
    if (!file_handle)
    {
        return 0;
    }
    size_t version_index_data_size = Trove_GetFileSize(file_handle);
    VersionIndex* version_index = (VersionIndex*)malloc(sizeof(VersionIndex) + version_index_data_size);
    if (!version_index)
    {
        Trove_CloseReadFile(file_handle);
        return 0;
    }
    if (!Trove_Read(file_handle, 0, version_index_data_size, &version_index[1]))
    {
        Trove_CloseReadFile(file_handle);
        return 0;
    }
    InitVersionIndex(version_index, version_index_data_size);
    Trove_CloseReadFile(file_handle);
    return version_index;
}

VersionIndex* CreateVersionIndex(Bikeshed shed, const char* assets_path)
{
    AssetPaths asset_paths;

    GetContent(assets_path, &asset_paths);

    uint32_t* contentSizes = (uint32_t*)malloc(sizeof(uint32_t) * asset_paths.count);
    TLongtail_Hash* pathHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_paths.count);
    TLongtail_Hash* contentHashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_paths.count);
    
    GetFileHashes(shed, assets_path, (const char**)asset_paths.paths, asset_paths.count, pathHashes, contentHashes, contentSizes);

    size_t version_index_size = GetVersionIndexSize(asset_paths.count, asset_paths.paths);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        asset_paths.count,
        asset_paths.paths,
        pathHashes,
        contentHashes,
        contentSizes);

    free(contentHashes);
    contentHashes = 0;
    free(pathHashes);
    pathHashes = 0;
    free(contentSizes);
    contentSizes = 0;

    FreeAssetpaths(&asset_paths);

    return version_index;
}

uint32_t GetUniqueAssets(uint64_t asset_count, const TLongtail_Hash* asset_content_hashes, uint32_t* out_unique_asset_indexes)
{
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)asset_count);
    void* hash_mem = malloc(hash_size);
    jc::HashTable<TLongtail_Hash, uint32_t> lookup_table;
    lookup_table.Create((uint32_t)asset_count, hash_mem);
    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash hash = asset_content_hashes[i];
        uint32_t* existing_index = lookup_table.Get(hash);
        if (existing_index)
        {
            (*existing_index)++;
        }
        else
        {
            lookup_table.Put(hash, 1);
            out_unique_asset_indexes[unique_asset_count] = i;
            ++unique_asset_count;
        }
    }
    free(hash_mem);
    hash_mem = 0;
    return unique_asset_count;
}

typedef TLongtail_Hash (*GetContentTagFunc)(const char* assets_path, const char* path);

ContentIndex* CreateContentIndex(
    const char* assets_path,
    uint64_t asset_count,
    const TLongtail_Hash* asset_content_hashes,
    const TLongtail_Hash* asset_path_hashes,
    const uint32_t* asset_sizes,
    const uint32_t* asset_name_offsets,
    const char* asset_name_data,
    GetContentTagFunc get_content_tag)
{
    if (asset_count == 0)
    {
        return 0;
    }
    uint32_t* assets_index = (uint32_t*)malloc(sizeof(uint32_t) * asset_count);
    uint32_t unique_asset_count = GetUniqueAssets(asset_count, asset_content_hashes, assets_index);

    TLongtail_Hash* content_tags = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);
    for (uint32_t i = 0; i < unique_asset_count; ++i)
    {
        uint32_t asset_index = assets_index[i];
        content_tags[asset_index] = get_content_tag(assets_path, &asset_name_data[asset_name_offsets[asset_index]]);
    }

    struct CompareAssetEntry
    {
        CompareAssetEntry(const TLongtail_Hash* asset_path_hashes, const uint32_t* asset_sizes, const TLongtail_Hash* asset_tags)
            : asset_path_hashes(asset_path_hashes)
            , asset_sizes(asset_sizes)
            , asset_tags(asset_tags)
        {

        }

        // This sorting algorithm is very arbitrary!
        bool operator()(uint32_t a, uint32_t b) const
        {   
            TLongtail_Hash a_tag = asset_tags[a];
            TLongtail_Hash b_tag = asset_tags[b];
            if (a_tag < b_tag)
            {
                return true;
            }
            else if (b_tag < a_tag)
            {
                return false;
            }
            uint32_t a_size = asset_sizes[a];
            uint32_t b_size = asset_sizes[b];
            if (a_size < b_size)
            {
                return true;
            }
            else if (b_size < a_size)
            {
                return false;
            }
            TLongtail_Hash a_hash = asset_path_hashes[a];
            TLongtail_Hash b_hash = asset_path_hashes[b];
            return (a_hash < b_hash);
        }

        const TLongtail_Hash* asset_path_hashes;
        const uint32_t* asset_sizes;
        const TLongtail_Hash* asset_tags;
    };

    std::sort(&assets_index[0], &assets_index[unique_asset_count], CompareAssetEntry(asset_path_hashes, asset_sizes, content_tags));

    BlockIndex** block_indexes = (BlockIndex**)malloc(sizeof(BlockIndex*) * unique_asset_count);

    static const uint32_t MAX_ASSETS_PER_BLOCK = 4096;
    static const uint32_t MAX_BLOCK_SIZE = 65536;
    uint32_t stored_asset_indexes[MAX_ASSETS_PER_BLOCK];

    uint32_t current_size = 0;
    TLongtail_Hash current_tag = content_tags[assets_index[0]];
    uint64_t i = 0;
    uint32_t asset_count_in_block = 0;
    uint32_t block_count = 0;

    while (i < unique_asset_count)
    {
        uint64_t asset_index = assets_index[i];
        while (i < unique_asset_count && current_size < MAX_BLOCK_SIZE && (current_tag == content_tags[asset_index] || current_size < (MAX_BLOCK_SIZE / 2)) && asset_count_in_block < MAX_ASSETS_PER_BLOCK)
        {
            current_size += asset_sizes[asset_index];
            stored_asset_indexes[asset_count_in_block] = asset_index;
            ++i;
            asset_index = assets_index[i];
            ++asset_count_in_block;
        }

        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            asset_count_in_block,
            stored_asset_indexes,
            asset_content_hashes,
            asset_sizes);

        ++block_count;
		current_tag = i < unique_asset_count ? content_tags[asset_index] : 0;
        current_size = 0;
        asset_count_in_block = 0;
    }

    if (current_size > 0)
    {
        block_indexes[block_count] = CreateBlockIndex(
            malloc(GetBlockIndexSize(asset_count_in_block)),
            asset_count_in_block,
            stored_asset_indexes,
            asset_content_hashes,
            asset_sizes);
        ++block_count;
    }

    free(content_tags);
    content_tags = 0;
    free(assets_index);
    assets_index = 0;

    // Build Content Index (from block list)
    size_t content_index_size = GetContentIndexSize(block_count, unique_asset_count);
    ContentIndex* content_index = (ContentIndex*)malloc(content_index_size);

    content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex)];
    content_index->m_AssetCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
    *content_index->m_BlockCount = block_count;
    *content_index->m_AssetCount = unique_asset_count;
    InitContentIndex(content_index);

    uint64_t asset_index = 0;
    for (uint32_t i = 0; i < block_count; ++i)
    {
        content_index->m_BlockHash[i] = block_indexes[i]->m_BlockHash;
        uint64_t asset_offset = 0;
        for (uint32_t a = 0; a < block_indexes[i]->m_AssetCountInBlock; ++a)
        {
            content_index->m_AssetContentHash[asset_index] = block_indexes[i]->m_Entries[a].m_AssetContentHash;
            content_index->m_AssetBlockIndex[asset_index] = i;
            content_index->m_AssetBlockOffset[asset_index] = asset_offset;
            content_index->m_AssetLength[asset_index] = block_indexes[i]->m_Entries[a].m_AssetSize;
            asset_offset += block_indexes[i]->m_Entries[a].m_AssetSize;
            ++asset_index;
            if (asset_index > unique_asset_count)
            {
                break;
            }
        }
        free(block_indexes[i]);
        block_indexes[i] = 0;
    }
    free(block_indexes);
    block_indexes = 0;

    return content_index;
}

void DiffHashes(const TLongtail_Hash* reference_hashes, uint32_t reference_hash_count, const TLongtail_Hash* new_hashes, uint32_t new_hash_count, uint32_t* added_hash_count, TLongtail_Hash* added_hashes, uint32_t* removed_hash_count, TLongtail_Hash* removed_hashes)
{
    TLongtail_Hash* refs = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * reference_hash_count);
    TLongtail_Hash* news = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * new_hash_count);
    memmove(refs, reference_hashes, sizeof(TLongtail_Hash) * reference_hash_count);
    memmove(news, new_hashes, sizeof(TLongtail_Hash) * new_hash_count);

    std::sort(&refs[0], &refs[reference_hash_count]);
    std::sort(&news[0], &news[new_hash_count]);

    uint32_t removed = 0;
    uint32_t added = 0;
    uint32_t ni = 0;
    uint32_t ri = 0;
    while (ri < reference_hash_count && ni < new_hash_count)
    {
        if (refs[ri] == news[ni])
        {
            ++ri;
            ++ni;
            continue;
        }
        else if (refs[ri] < news[ni])
        {
            if (removed_hashes)
            {
                removed_hashes[removed] = refs[ri];
            }
			++removed;
			++ri;
        }
        else if (refs[ri] > news[ni])
        {
            added_hashes[added++] = news[ni++];
        }
    }
    while (ni < new_hash_count)
    {
        added_hashes[added++] = news[ni++];
    }
    *added_hash_count = added;
    while (ri < reference_hash_count)
    {
		if (removed_hashes)
		{
			removed_hashes[removed] = refs[ri];
		}
		++removed;
		++ri;
	}
    if (removed_hash_count)
    {
        *removed_hash_count = removed;
    }

    free(news);
    news = 0;
    free(refs);
    refs = 0;
}

struct PathLookup
{
    jc::HashTable<TLongtail_Hash, uint32_t> m_HashToNameOffset;
    const char* m_NameData;
};

PathLookup* CreateContentHashToPathLookup(const VersionIndex* version_index, uint64_t* out_unique_asset_indexes)
{
    uint32_t asset_count = (uint32_t)(*version_index->m_AssetCount);
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize(asset_count);
    PathLookup* path_lookup = (PathLookup*)malloc(sizeof(PathLookup) + hash_size);
    path_lookup->m_HashToNameOffset.Create(asset_count, &path_lookup[1]);
    path_lookup->m_NameData = version_index->m_NameData;

    // Only pick up unique assets
    uint32_t unique_asset_count = 0;
    for (uint32_t i = 0; i < asset_count; ++i)
    {
        TLongtail_Hash content_hash = version_index->m_AssetContentHash[i];
        uint32_t* existing_index = path_lookup->m_HashToNameOffset.Get(content_hash);
        if (existing_index == 0)
        {
            path_lookup->m_HashToNameOffset.Put(content_hash, version_index->m_NameOffset[i]);
            if (out_unique_asset_indexes)
            {
                out_unique_asset_indexes[unique_asset_count] = i;
            }
            ++unique_asset_count;
        }
    }
    return path_lookup;
}

const char* GetPathFromAssetContentHash(const PathLookup* path_lookup, TLongtail_Hash asset_content_hash)
{
    const uint32_t* index_ptr = path_lookup->m_HashToNameOffset.Get(asset_content_hash);
    if (index_ptr == 0)
    {
        return 0;
    }
    return &path_lookup->m_NameData[*index_ptr];
}

int WriteContentBlocks(
    Bikeshed shed,
    ContentIndex* content_index,
    PathLookup* asset_content_hash_to_path,
    const char* assets_folder,
    const char* content_folder)
{
    uint64_t block_count = *content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 1;
    }

    uint32_t block_start_asset_index = 0;
    for (uint32_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHash[block_index];

        char file_name[64];
        sprintf(file_name, "0x%" PRIx64 ".lrb", block_hash);
        char* block_path = (char*)Trove_ConcatPath(content_folder, file_name);
        Trove_DenormalizePath(block_path);
		DWORD attrs = GetFileAttributesA(block_path);
        if (attrs != INVALID_FILE_ATTRIBUTES)
        {
            free((char*)block_path);
            block_path = 0;
            continue;
        }

        char tmp_block_name[64];
        sprintf(tmp_block_name, "0x%" PRIx64 ".tmp", block_hash);
        char* tmp_block_path = (char*)Trove_ConcatPath(content_folder, tmp_block_name);
        Trove_DenormalizePath(tmp_block_path);
        HTroveOpenWriteFile block_file_handle = Trove_OpenWriteFile(tmp_block_path);
        uint32_t write_offset = 0;

        uint32_t asset_index = block_start_asset_index;
        while (content_index->m_AssetBlockIndex[asset_index] == block_index)
        {
            TLongtail_Hash asset_content_hash = content_index->m_AssetContentHash[asset_index];
            uint32_t asset_size = content_index->m_AssetLength[asset_index];
            const char* asset_path = GetPathFromAssetContentHash(asset_content_hash_to_path, asset_content_hash);

            char* full_path = (char*)Trove_ConcatPath(assets_folder, asset_path);
            Trove_DenormalizePath(full_path);
            HTroveOpenReadFile file_handle = Trove_OpenReadFile(full_path);
            if (Trove_GetFileSize(file_handle) != asset_size)
            {
                Trove_CloseWriteFile(block_file_handle);
                return 0;
            }
            void* buffer = malloc(asset_size);
            Trove_Read(file_handle, 0, asset_size, buffer);
            Trove_Write(block_file_handle, write_offset, asset_size, buffer);
            write_offset += asset_size;

            free(buffer);
            buffer = 0;
            Trove_CloseReadFile(file_handle);
            free((char*)full_path);
            full_path = 0;

            ++asset_index;
        }

        uint32_t asset_count = (uint32_t)asset_index - block_start_asset_index;

        for (uint64_t i = block_start_asset_index; i < asset_index; ++i)
        {
            TLongtail_Hash asset_content_hash = content_index->m_AssetContentHash[i];
            Trove_Write(block_file_handle, write_offset, sizeof(TLongtail_Hash), &asset_content_hash);
            write_offset += sizeof(TLongtail_Hash);

            uint32_t asset_size = content_index->m_AssetLength[i];
            Trove_Write(block_file_handle, write_offset, sizeof(uint32_t), &asset_size);
            write_offset += sizeof(uint32_t);
        }

        Trove_Write(block_file_handle, write_offset, sizeof(uint32_t), &asset_count);
        write_offset += sizeof(uint32_t);
        Trove_CloseWriteFile(block_file_handle);

        // TODO: Non-portable!
        BOOL ok = ::MoveFileA(tmp_block_path, block_path);
        if (!ok)
        {
            printf("Failed to move `%s` to `%s`\n", tmp_block_path, block_path);
        }
        
        free((char*)block_path);
        block_path = 0;

        free((char*)tmp_block_path);
        tmp_block_path = 0;

        block_start_asset_index += asset_count;
    }

    return 1;
}

uint64_t GetMissingAssets(const ContentIndex* content_index, const VersionIndex* version, TLongtail_Hash* missing_assets)
{
    uint32_t missing_hash_count = 0;
    DiffHashes(content_index->m_AssetContentHash, *content_index->m_AssetCount, version->m_AssetContentHash, *version->m_AssetCount, &missing_hash_count, missing_assets, 0, 0);
    return missing_hash_count;
}

ContentIndex* CreateMissingContent(const ContentIndex* content_index, const char* content_path, const VersionIndex* version, GetContentTagFunc get_content_tag)
{
    uint64_t asset_count = *version->m_AssetCount;
    TLongtail_Hash* removed_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);

    uint32_t added_hash_count = 0;
    uint32_t removed_hash_count = 0;
    DiffHashes(content_index->m_AssetContentHash, *content_index->m_AssetCount, version->m_AssetContentHash, asset_count, &added_hash_count, added_hashes, &removed_hash_count, removed_hashes);

    uint32_t* diff_asset_sizes = (uint32_t*)malloc(sizeof(uint32_t) * added_hash_count);
    uint32_t* diff_name_offsets = (uint32_t*)malloc(sizeof(uint32_t) * added_hash_count);

    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)asset_count);
    jc::HashTable<TLongtail_Hash, uint32_t> asset_index_lookup;
    void* path_lookup_mem = malloc(hash_size);
    asset_index_lookup.Create(asset_count, path_lookup_mem);
    for (uint64_t i = 0; i < asset_count; ++i)
    {
        asset_index_lookup.Put(version->m_AssetContentHash[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        uint32_t* asset_index_ptr = asset_index_lookup.Get(added_hashes[j]);
        if (!asset_index_ptr)
        {
            free(path_lookup_mem);
            free(removed_hashes);
            free(added_hashes);
            return 0;
        }
        uint64_t asset_index = *asset_index_ptr;
        diff_asset_sizes[j] = version->m_AssetSize[asset_index];
        diff_name_offsets[j] = version->m_NameOffset[asset_index];
    }
    free(path_lookup_mem);
    path_lookup_mem = 0;

    ContentIndex* diff_content_index = CreateContentIndex(
        content_path,
        added_hash_count,
        added_hashes,
        added_hashes,
        diff_asset_sizes,
        diff_name_offsets,
        version->m_NameData,
        get_content_tag);

    return diff_content_index;
}

ContentIndex* GetBlocksForAssets(const ContentIndex* content_index, uint64_t asset_count, const TLongtail_Hash* asset_hashes, uint64_t* out_missing_asset_count, uint64_t* out_missing_assets)
{
    uint64_t found_asset_count = 0;
    uint64_t* found_assets = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);

    {
        uint64_t content_index_asset_count = *content_index->m_AssetCount;
        uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint64_t>::CalcSize((uint32_t)content_index_asset_count);
        jc::HashTable<TLongtail_Hash, uint32_t> content_hash_to_asset_index;
        void* content_hash_to_asset_index_mem = malloc(hash_size);
        content_hash_to_asset_index.Create(content_index_asset_count, content_hash_to_asset_index_mem);
        for (uint64_t i = 0; i < content_index_asset_count; ++i)
        {
            TLongtail_Hash asset_content_hash = content_index->m_AssetContentHash[i];
            content_hash_to_asset_index.Put(asset_content_hash, i);
        }

        for (uint64_t i = 0; i < asset_count; ++i)
        {
            TLongtail_Hash asset_hash = asset_hashes[i];
            const uint32_t* asset_index_ptr = content_hash_to_asset_index.Get(asset_hash);
            if (asset_index_ptr != 0)
            {
                found_assets[found_asset_count++] = *asset_index_ptr;
            }
            else
            {
                out_missing_assets[(*out_missing_asset_count)++];
            }
        }

        free(content_hash_to_asset_index_mem);
    }

    uint32_t* asset_count_in_blocks = (uint32_t*)malloc(sizeof(uint32_t) * *content_index->m_BlockCount);
    uint64_t* asset_start_in_blocks = (uint64_t*)malloc(sizeof(uint64_t) * *content_index->m_BlockCount);
    uint64_t i = 0;
    while (i < *content_index->m_AssetCount)
    {
        uint64_t prev_block_index = content_index->m_AssetBlockIndex[i++];
        asset_start_in_blocks[prev_block_index] = i;
        uint32_t assets_count_in_block = 1;
        while (i < *content_index->m_AssetCount && prev_block_index == content_index->m_AssetBlockIndex[i])
        {
            ++assets_count_in_block;
            ++i;
        }
        asset_count_in_blocks[prev_block_index] = assets_count_in_block;
    }

    uint32_t hash_size = jc::HashTable<uint64_t, uint32_t>::CalcSize((uint32_t)*content_index->m_AssetCount);
    jc::HashTable<uint64_t, uint32_t> block_index_to_asset_count;
    void* block_index_to_first_asset_index_mem = malloc(hash_size);
    block_index_to_asset_count.Create((uint32_t)*content_index->m_AssetCount, block_index_to_first_asset_index_mem);

    uint64_t copy_block_count = 0;
    uint64_t* copy_blocks = (uint64_t*)malloc(sizeof(uint64_t) * found_asset_count);

    uint64_t unique_asset_count = 0;
    for (uint64_t i = 0; i < found_asset_count; ++i)
    {
        uint64_t asset_index = found_assets[i];
        uint64_t block_index = content_index->m_AssetBlockIndex[asset_index];
        uint32_t* block_index_ptr = block_index_to_asset_count.Get(block_index);
        if (!block_index_ptr)
        {
            block_index_to_asset_count.Put(block_index, 1);
            copy_blocks[copy_block_count++] = block_index;
			unique_asset_count += asset_count_in_blocks[block_index];
		}
        else
        {
            (*block_index_ptr)++;
        }
    }
    free(block_index_to_first_asset_index_mem);
    block_index_to_first_asset_index_mem = 0;

    size_t content_index_size = GetContentIndexSize(copy_block_count, unique_asset_count);
    ContentIndex* existing_content_index = (ContentIndex*)malloc(content_index_size);

    existing_content_index->m_BlockCount = (uint64_t*)&((char*)existing_content_index)[sizeof(ContentIndex)];
    existing_content_index->m_AssetCount = (uint64_t*)&((char*)existing_content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
    *existing_content_index->m_BlockCount = copy_block_count;
    *existing_content_index->m_AssetCount = unique_asset_count;
    InitContentIndex(existing_content_index);
    uint64_t asset_offset = 0;
    for (uint64_t i = 0; i < copy_block_count; ++i)
    {
        uint64_t block_index = copy_blocks[i];
        uint32_t block_asset_count = asset_count_in_blocks[block_index];
        existing_content_index->m_BlockHash[i] = content_index->m_BlockHash[block_index];
        for (uint32_t j = 0; j < block_asset_count; ++j)
        {
            uint64_t source_asset_index = asset_start_in_blocks[block_index] + j;
            uint64_t target_asset_index = asset_offset + j;
            existing_content_index->m_AssetContentHash[target_asset_index] = content_index->m_AssetBlockOffset[source_asset_index];
            existing_content_index->m_AssetBlockIndex[target_asset_index] = block_index;
            existing_content_index->m_AssetBlockOffset[target_asset_index] = content_index->m_AssetBlockOffset[source_asset_index];
            existing_content_index->m_AssetLength[target_asset_index] = content_index->m_AssetLength[source_asset_index];
        }
        asset_offset += block_asset_count;
    }

    free(asset_count_in_blocks);
    asset_count_in_blocks = 0;
    free(asset_start_in_blocks);
    asset_start_in_blocks = 0;
    free(asset_count_in_blocks);
    asset_count_in_blocks = 0;

    free(copy_blocks);
    copy_blocks = 0;

    return existing_content_index;
}

struct ReconstructJob
{
    char* m_BlockPath;
    char** m_AssetPaths;
    uint32_t m_AssetCount;
    uint32_t* m_AssetBlockOffsets;
    uint32_t* m_AssetLengths;
    nadir::TAtomic32* m_PendingCount;
};

ReconstructJob* CreateReconstructJob(uint32_t asset_count)
{
    size_t job_size = sizeof(ReconstructJob) +
        sizeof(char*) * asset_count +
        sizeof(uint32_t) * asset_count +
        sizeof(uint32_t) * asset_count;

    ReconstructJob* job = (ReconstructJob*)malloc(job_size);
    char* p = (char*)&job[1];
    job->m_AssetPaths = (char**)p;
    p += sizeof(char*) * asset_count;

    job->m_AssetBlockOffsets = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    job->m_AssetLengths = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_count;

    return job;
}

static Bikeshed_TaskResult ReconstructFromBlock(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
{
    ReconstructJob* job = (ReconstructJob*)context;
    HTroveOpenReadFile block_file_handle = Trove_OpenReadFile(job->m_BlockPath);
    if(block_file_handle)
    {
        uint8_t batch_data[65536];

        for (uint32_t asset_index = 0; asset_index < job->m_AssetCount; ++asset_index)
        {
            char* asset_path = job->m_AssetPaths[asset_index];
			Trove_EnsureParentPathExists(asset_path);

            Trove_DenormalizePath(asset_path);
            HTroveOpenWriteFile asset_file_handle = Trove_OpenWriteFile(asset_path);
            if(asset_file_handle)
            {
                uint32_t asset_length = job->m_AssetLengths[asset_index];
                uint64_t read_offset = job->m_AssetBlockOffsets[asset_index];
                uint64_t write_offset = 0;

                while (write_offset != asset_length)
                {
                    uint64_t left = asset_length - write_offset;
                    uint64_t len = left < sizeof(batch_data) ? left : sizeof(batch_data);
                    bool read_ok = Trove_Read(block_file_handle, read_offset, len, batch_data);
                    assert(read_ok);
                    bool write_ok = Trove_Write(asset_file_handle, write_offset, len, batch_data);
                    read_offset += len;
                    write_offset += len;
                }
            }
            Trove_CloseWriteFile(asset_file_handle);
            asset_file_handle = 0;
        }
        Trove_CloseReadFile(block_file_handle);
        block_file_handle = 0;
    }
    nadir::AtomicAdd32(job->m_PendingCount, -1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

void ReconstructVersion(Bikeshed shed, const ContentIndex* content_index, const VersionIndex* version_index, const char* content_path, const char* version_path)
{
    uint32_t hash_size = jc::HashTable<uint64_t, uint32_t>::CalcSize((uint32_t)*content_index->m_AssetCount);
    jc::HashTable<uint64_t, uint32_t> content_hash_to_content_asset_index;
    void* content_hash_to_content_asset_index_mem = malloc(hash_size);
    content_hash_to_content_asset_index.Create((uint32_t)*content_index->m_AssetCount, content_hash_to_content_asset_index_mem);
    for (uint64_t i = 0; i < *content_index->m_AssetCount; ++i)
    {
        content_hash_to_content_asset_index.Put(content_index->m_AssetContentHash[i], i);
    }

	uint64_t asset_count = *version_index->m_AssetCount;
    uint64_t* asset_order = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
	uint64_t* version_index_to_content_index = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);
	for (uint64_t i = 0; i < asset_count; ++i)
	{
		asset_order[i] = i;
		version_index_to_content_index[i] = *content_hash_to_content_asset_index.Get(version_index->m_AssetContentHash[i]);
	}

    free(content_hash_to_content_asset_index_mem);
    content_hash_to_content_asset_index_mem = 0;

    struct ReconstructOrder
    {
        ReconstructOrder(const ContentIndex* content_index, const TLongtail_Hash* asset_hashes, const uint64_t* version_index_to_content_index)
            : content_index(content_index)
            , asset_hashes(asset_hashes)
            , version_index_to_content_index(version_index_to_content_index)
        {

        }

        bool operator()(uint32_t a, uint32_t b) const
        {   
            TLongtail_Hash a_hash = asset_hashes[a];
            TLongtail_Hash b_hash = asset_hashes[b];
            uint32_t a_asset_index_in_content_index = (uint32_t)version_index_to_content_index[a];
            uint32_t b_asset_index_in_content_index = (uint32_t)version_index_to_content_index[b];

            uint64_t a_block_index = content_index->m_AssetBlockIndex[a_asset_index_in_content_index];
            uint64_t b_block_index = content_index->m_AssetBlockIndex[b_asset_index_in_content_index];
            if (a_block_index < b_block_index)
            {
                return true;
            }
            if (a_block_index > b_block_index)
            {
                return false;
            }

            uint32_t a_offset_in_block = content_index->m_AssetBlockOffset[a_asset_index_in_content_index];
            uint32_t b_offset_in_block = content_index->m_AssetBlockOffset[b_asset_index_in_content_index];
            return a_offset_in_block < b_offset_in_block;
        }

        const ContentIndex* content_index;
        const TLongtail_Hash* asset_hashes;
        const uint64_t* version_index_to_content_index;
    };
    std::sort(&asset_order[0], &asset_order[asset_count], ReconstructOrder(content_index, version_index->m_AssetContentHash, version_index_to_content_index));

    nadir::TAtomic32 pending_job_count = 0;
    ReconstructJob** reconstruct_jobs = (ReconstructJob**)malloc(sizeof(ReconstructJob*) * asset_count);
    uint32_t job_count = 0;
    uint64_t i = 0;
    while (i < asset_count)
    {
        uint64_t asset_index = asset_order[i];

        uint32_t asset_index_in_content_index = version_index_to_content_index[asset_index];
        uint64_t block_index = content_index->m_AssetBlockIndex[asset_index_in_content_index];

        uint32_t asset_count_from_block = 1;
        while(((i + asset_count_from_block) < asset_count))
        {
            uint32_t next_asset_index = asset_order[i + asset_count_from_block];
            uint64_t next_asset_index_in_content_index = version_index_to_content_index[next_asset_index];
            uint64_t next_block_index = content_index->m_AssetBlockIndex[next_asset_index_in_content_index];
            if (block_index != next_block_index)
            {
                break;
            }
            ++asset_count_from_block;
        }

        ReconstructJob* job = CreateReconstructJob(asset_count_from_block);
        reconstruct_jobs[job_count++] = job;
        job->m_PendingCount = &pending_job_count;

        TLongtail_Hash block_hash = content_index->m_BlockHash[block_index];
        char block_file_name[64];
        sprintf(block_file_name, "0x%" PRIx64 ".lrb", block_hash);

        job->m_BlockPath = (char*)Trove_ConcatPath(content_path, block_file_name);
        job->m_AssetCount = asset_count_from_block;

        for (uint32_t j = 0; j < asset_count_from_block; ++j)
        {
            uint64_t asset_index = asset_order[i + j];
            const char* asset_path = &version_index->m_NameData[version_index->m_NameOffset[asset_index]];
            job->m_AssetPaths[j] = (char*)Trove_ConcatPath(version_path, asset_path);
            Trove_NormalizePath(job->m_AssetPaths[j]);
            uint64_t asset_index_in_content_index = version_index_to_content_index[asset_index];
            job->m_AssetBlockOffsets[j] = content_index->m_AssetBlockOffset[asset_index_in_content_index];
            job->m_AssetLengths[j] = content_index->m_AssetLength[asset_index_in_content_index];
        }

        if (shed == 0)
        {
            nadir::AtomicAdd32(&pending_job_count, 1);
            Bikeshed_TaskResult result = ReconstructFromBlock(shed, 0, 0, job);
            assert(result == BIKESHED_TASK_RESULT_COMPLETE);
        }
		else
		{
			Bikeshed_TaskID task_ids[1];
			BikeShed_TaskFunc func[1] = { ReconstructFromBlock };
			void* ctx[1] = { job };

			while (!Bikeshed_CreateTasks(shed, 1, func, ctx, task_ids))
			{
				nadir::Sleep(1000);
			}

			{
				nadir::AtomicAdd32(&pending_job_count, 1);
				Bikeshed_ReadyTasks(shed, 1, task_ids);
			}
		}

        i += asset_count_from_block;
    }

    if (shed)
    {
        int32_t old_pending_count = 0;
        while (pending_job_count > 0)
        {
            if (Bikeshed_ExecuteOne(shed, 0))
            {
                continue;
            }
            if (old_pending_count != pending_job_count)
            {
                old_pending_count = pending_job_count;
                printf("Blocks left to reconstruct from: %d\n", pending_job_count);
            }
            nadir::Sleep(1000);
        }
    }

    while (job_count--)
    {
        ReconstructJob* job = reconstruct_jobs[job_count];
        free(job->m_BlockPath);
        for (uint32_t i = 0; i < job->m_AssetCount; ++i)
        {
            free(job->m_AssetPaths[i]);
        }
        free(reconstruct_jobs[job_count]);
    }

    free(reconstruct_jobs);

    free(version_index_to_content_index);
    free(asset_order);
}

void RunStuff()
{
    Jobs::ReadyCallback ready_callback;
    Bikeshed shed = Bikeshed_Create(malloc(BIKESHED_SIZE(65536, 0, 1)), 65536, 0, 1, &ready_callback.cb);

    nadir::TAtomic32 stop = 0;

    static const uint32_t WORKER_COUNT = 7;
    Jobs::ThreadWorker workers[WORKER_COUNT];
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].CreateThread(shed, ready_callback.m_Semaphore, &stop);
    }

//    #define HOME "test\\data"
    #define HOME "D:\\Temp\\longtail"

	#define VERSION1 "75a99408249875e875f8fba52b75ea0f5f12a00e"
	#define VERSION2 "b1d3adb4adce93d0f0aa27665a52be0ab0ee8b59"

	#define VERSION1_FOLDER "git" VERSION1 "_Win64_Editor"
	#define VERSION2_FOLDER "git" VERSION2 "_Win64_Editor"

//    #define VERSION1_FOLDER "version1"
//    #define VERSION2_FOLDER "version2"

	const char* local_path_1 = HOME "\\local\\" VERSION1_FOLDER;
    const char* version_index_path_1 = HOME "\\local\\" VERSION1_FOLDER ".lvi";

    const char* local_path_2 = HOME "\\local\\" VERSION2_FOLDER;
    const char* version_index_path_2 = HOME "\\local\\" VERSION1_FOLDER ".lvi";

    const char* local_content_path = "C:\\Temp\\local_content";//HOME "\\local_content";
    const char* local_content_index_path = HOME "\\local.lci";

    const char* remote_content_path = HOME "\\remote_content";
    const char* remote_content_index_path = HOME "\\remote.lci";

    const char* remote_path_1 = "C:\\Temp\\remote";// HOME "\\remote\\" VERSION1_FOLDER;
    const char* remote_path_2 = HOME "\\remote\\" VERSION2_FOLDER;

    VersionIndex* version1 = CreateVersionIndex(shed, local_path_1);
    WriteVersionIndex(version1, version_index_path_1);
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version1->m_AssetCount, local_path_1, version_index_path_1);

    ContentIndex* local_content_index = CreateContentIndex(
        local_path_1,
        *version1->m_AssetCount,
        version1->m_AssetContentHash,
        version1->m_PathHash,
        version1->m_AssetSize,
        version1->m_NameOffset,
        version1->m_NameData,
        GetContentTag);

    WriteContentIndex(local_content_index, local_content_index_path);
    printf("%" PRIu64 " blocks from version `%s` indexed to `%s`\n", *local_content_index->m_BlockCount, local_path_1, local_content_index_path);

    if (1)
    {
		printf("Writing %" PRIu64 " block to `%s`\n", *local_content_index->m_BlockCount, local_content_path);
		PathLookup* path_lookup = CreateContentHashToPathLookup(version1, 0);
        WriteContentBlocks(
            shed,
            local_content_index,
            path_lookup,
            local_path_1,
            local_content_path);

        free(path_lookup);
        path_lookup = 0;
    }

	printf("Reconstructing %" PRIu64 " assets to `%s`\n", *version1->m_AssetCount, remote_path_1);
	ReconstructVersion(shed, local_content_index, version1, local_content_path, remote_path_1);

	free(local_content_index);
	local_content_index = 0;
	free(version1);
	version1 = 0;

	return;


    VersionIndex* version2 = CreateVersionIndex(shed, local_path_2);
    WriteVersionIndex(version2, version_index_path_2);
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version2->m_AssetCount, local_path_2, version_index_path_2);
    free(version2);
    version2 = 0;
    
    VersionIndex* version1b = ReadVersionIndex(version_index_path_1);
    printf("%" PRIu64 " assets in index `%s`\n", *version1b->m_AssetCount, version_index_path_1);
    VersionIndex* version2b = ReadVersionIndex(version_index_path_2);
    printf("%" PRIu64 " assets in index `%s`\n", *version2b->m_AssetCount, version_index_path_2);

    ContentIndex* local_content_indexb = ReadContentIndex(local_content_index_path);
    printf("%" PRIu64 " blocks in index `%s`\n", *local_content_indexb->m_BlockCount, local_content_index_path);

    // What is missing in local content that we need from remote version in new blocks with just the missing assets.
    ContentIndex* missing_content = CreateMissingContent(local_content_indexb, local_path_2, version2b, GetContentTag);

    if (1)
    {
		printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, local_content_path);
		PathLookup* path_lookup = CreateContentHashToPathLookup(version2b, 0);
        WriteContentBlocks(
            shed,
            missing_content,
            path_lookup,
            local_path_2,
            local_content_path);

		free(path_lookup);
        path_lookup = 0;
    }

	printf("%" PRIu64 " blocks for version `%s` needed in content index `%s`\n", *missing_content->m_BlockCount, local_path_1, local_content_path);
	
	free(missing_content);
    missing_content = 0;

    ContentIndex* remote_content_index = CreateContentIndex(
        local_path_2,
        *version2b->m_AssetCount,
        version2b->m_AssetContentHash,
        version2b->m_PathHash,
        version2b->m_AssetSize,
        version2b->m_NameOffset,
        version2b->m_NameData,
        GetContentTag);

    uint64_t* missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * *version2b->m_AssetCount);
    uint64_t missing_asset_count = GetMissingAssets(local_content_indexb, version2b, missing_assets);

	uint64_t* remaining_missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * missing_asset_count);
	uint64_t remaining_missing_asset_count = 0;
	ContentIndex* existing_blocks = GetBlocksForAssets(remote_content_index, missing_asset_count, missing_assets, &remaining_missing_asset_count, remaining_missing_assets);
	printf("%" PRIu64 " blocks for version `%s` available in content index `%s`\n", *existing_blocks->m_BlockCount, local_path_2, remote_content_path);

    // Copy existing blocks

    // Merge existing_blocks into local_content_indexb!

    //     ReconstructVersion(local_content_indexb, version2b, const char* content_path, const char* version_path)

    ReconstructVersion(0, local_content_indexb, version1b, local_content_path, local_path_1);


    free(existing_blocks);
    existing_blocks = 0;
	free(remaining_missing_assets);
    remaining_missing_assets = 0;
    free(missing_assets);
    missing_assets = 0;
    free(remote_content_index);
    remote_content_index = 0;

    free(version1b);
    version1b = 0;
    free(version2b);
    version2b = 0;

    nadir::AtomicAdd32(&stop, 1);
    Jobs::ReadyCallback::Ready(&ready_callback.cb, 0, WORKER_COUNT);
    for (uint32_t i = 0; i < WORKER_COUNT; ++i)
    {
        workers[i].JoinThread();
    }

    free(shed);
    shed = 0;

    return;
}

TLongtail_Hash GetContentTagFake(const char* , const char* path)
{
    return 0u;
 //   return (TLongtail_Hash)((uintptr_t)path) / 14;
}

TEST(Longtail, SlowAndPainful)
{
	RunStuff();
}

TEST(Longtail, TestVersionIndex)
{
	const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};

    size_t version_index_size = GetVersionIndexSize(5, asset_paths);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        5,
        asset_paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);

    free(version_index);
}

TEST(Longtail, TestContentIndex)
{
    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

    ContentIndex* content_index = CreateContentIndex(
        assets_path,
        asset_count,
        asset_content_hashes,
        asset_path_hashes,
        asset_sizes,
        asset_name_offsets,
        asset_name_data,
        GetContentTagFake);

    ASSERT_EQ(5u, *content_index->m_AssetCount);
    ASSERT_EQ(2u, *content_index->m_BlockCount);
    for (uint32_t i = 0; i < *content_index->m_AssetCount; ++i)
    {
		ASSERT_EQ(asset_content_hashes[4 - i], content_index->m_AssetContentHash[i]);
		ASSERT_EQ(asset_sizes[4 - i], content_index->m_AssetLength[i]);
	}
	ASSERT_EQ(0u, content_index->m_AssetBlockIndex[0]);
	ASSERT_EQ(0u, content_index->m_AssetBlockIndex[1]);
	ASSERT_EQ(0u, content_index->m_AssetBlockIndex[2]);
	ASSERT_EQ(1u, content_index->m_AssetBlockIndex[3]);
	ASSERT_EQ(1u, content_index->m_AssetBlockIndex[4]);

	ASSERT_EQ(0u, content_index->m_AssetBlockOffset[0]);
	ASSERT_EQ(32001u, content_index->m_AssetBlockOffset[1]);
	ASSERT_EQ(64002u, content_index->m_AssetBlockOffset[2]);
	ASSERT_EQ(0u, content_index->m_AssetBlockOffset[3]);
	ASSERT_EQ(32003u, content_index->m_AssetBlockOffset[4]);

	free(content_index);
}

TEST(Longtail, TestDiffHashes)
{

//void DiffHashes(const TLongtail_Hash* reference_hashes, uint32_t reference_hash_count, const TLongtail_Hash* new_hashes, uint32_t new_hash_count, uint32_t* added_hash_count, TLongtail_Hash* added_hashes, uint32_t* removed_hash_count, TLongtail_Hash* removed_hashes)
}

TEST(Longtail, TestGetUniqueAssets)
{
//uint32_t GetUniqueAssets(uint64_t asset_count, const TLongtail_Hash* asset_content_hashes, uint32_t* out_unique_asset_indexes)

}

TEST(Longtail, TestGetBlocksForAssets)
{
//    ContentIndex* GetBlocksForAssets(const ContentIndex* content_index, uint64_t asset_count, const TLongtail_Hash* asset_hashes, uint64_t* out_missing_asset_count, uint64_t* out_missing_assets)
}

TEST(Longtail, TestCreateMissingContent)
{
    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

    ContentIndex* content_index = CreateContentIndex(
        assets_path,
        asset_count - 4,
        asset_content_hashes,
        asset_path_hashes,
        asset_sizes,
        asset_name_offsets,
        asset_name_data,
        GetContentTagFake);

    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    size_t version_index_size = GetVersionIndexSize(5, asset_paths);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        5,
        asset_paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);

    ContentIndex* missing_content_index = CreateMissingContent(
        content_index,
        "",
        version_index,
        GetContentTagFake);

    ASSERT_EQ(2u, *missing_content_index->m_BlockCount);
    ASSERT_EQ(4u, *missing_content_index->m_AssetCount);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[0]);
    ASSERT_EQ(asset_content_hashes[4], missing_content_index->m_AssetContentHash[0]);
    ASSERT_EQ(asset_sizes[4], missing_content_index->m_AssetLength[0]);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[0]);
    ASSERT_EQ(asset_content_hashes[3], missing_content_index->m_AssetContentHash[1]);
    ASSERT_EQ(asset_sizes[3], missing_content_index->m_AssetLength[1]);
    ASSERT_EQ(32001u, missing_content_index->m_AssetBlockOffset[1]);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[2]);
    ASSERT_EQ(asset_content_hashes[2], missing_content_index->m_AssetContentHash[2]);
    ASSERT_EQ(asset_sizes[2], missing_content_index->m_AssetLength[2]);
    ASSERT_EQ(64002u, missing_content_index->m_AssetBlockOffset[2]);

    ASSERT_EQ(1u, missing_content_index->m_AssetBlockIndex[3]);
    ASSERT_EQ(asset_content_hashes[1], missing_content_index->m_AssetContentHash[3]);
    ASSERT_EQ(asset_sizes[1], missing_content_index->m_AssetLength[3]);
    ASSERT_EQ(0u, missing_content_index->m_AssetBlockOffset[3]);

    free(version_index);
    free(content_index);

    free(missing_content_index);
}

TEST(Longtail, TestGetMissingAssets)
{
//    uint64_t GetMissingAssets(const ContentIndex* content_index, const VersionIndex* version, TLongtail_Hash* missing_assets)
}

TEST(Longtail, TestReconstructVersion)
{
    ContentIndex* content_index = 0;
    {
        const char* assets_path = "";
        const uint64_t asset_count = 5;
        const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
        const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
        const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
        const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
        const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

        content_index = CreateContentIndex(
            assets_path,
            asset_count,
            asset_content_hashes,
            asset_path_hashes,
            asset_sizes,
            asset_name_offsets,
            asset_name_data,
            GetContentTagFake);
    }

    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    const TLongtail_Hash asset_path_hashes[5] = {10, 20, 30, 40, 50};
    const TLongtail_Hash asset_content_hashes[5] = { 1, 2, 3, 4, 5};
    const uint32_t asset_sizes[5] = {32001, 32001, 32002, 32003, 32003};

    size_t version_index_size = GetVersionIndexSize(5, asset_paths);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        5,
        asset_paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);

    ReconstructVersion(
        0,
        content_index,
        version_index,
        "",
        "");
}


#if 0
    // 2. Read version index
    {
        HTroveOpenReadFile file_handle = Trove_OpenReadFile("D:\\Temp\\VersionIndex.lvi");
        size_t version_index_data_size = Trove_GetFileSize(file_handle);
        VersionIndex* version_index = (VersionIndex*)malloc(sizeof(VersionIndex) + version_index_data_size);
        Trove_Read(file_handle, 0, version_index_data_size, &version_index[1]);
        InitVersionIndex(version_index, version_index_data_size);
        Trove_CloseReadFile(file_handle);

        if (0)
        {
            for (uint64_t i = 0; i < *version_index->m_AssetCount; ++i)
            {
                const char* asset_path  = &version_index->m_NameData[version_index->m_NameOffset[i]];

                char path_hash_str[64];
                sprintf(path_hash_str, "0x%" PRIx64, version_index->m_AssetHash[i]);
                char content_hash_str[64];
                sprintf(content_hash_str, "0x%" PRIx64, version_index->m_AssetHash[i]);

                printf("%s (%s) = %s\n", asset_path, path_hash_str, content_hash_str);
            }
        }

        free(version_index);
    }


    // 3. Build content index
    struct ContentBlock
    {
        uint64_t m_AssetCount;
        TLongtail_Hash* m_AssetHashes;
        uint64_t* m_AssetSizes;
    };

    // 3. Sort the assets
    {
        const char* asset_folder = local_path_1;
        HTroveOpenReadFile file_handle = Trove_OpenReadFile("D:\\Temp\\VersionIndex.lvi");
        size_t version_index_data_size = Trove_GetFileSize(file_handle);
        VersionIndex* version_index = (VersionIndex*)malloc(sizeof(VersionIndex) + version_index_data_size);
        Trove_Read(file_handle, 0, version_index_data_size, &version_index[1]);
        InitVersionIndex(version_index, version_index_data_size);
        Trove_CloseReadFile(file_handle);

        uint64_t asset_count = *version_index->m_AssetCount;
        if (asset_count > 0)
        {
            uint64_t* asset_sizes = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);

            uint32_t* assets_index = (uint32_t*)malloc(sizeof(uint64_t) * asset_count);
            TLongtail_Hash* content_tags = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * asset_count);

            // Map asset_hash to unique asset path
            uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint64_t>::CalcSize(asset_count);
            void* hash_mem = malloc(hash_size);
            jc::HashTable<TLongtail_Hash, uint64_t> hashes;
            hashes.Create(asset_count, hash_mem);

            // Only pick up unique assets
            uint32_t unique_asset_count = 0;
            for (uint32_t i = 0; i < asset_count; ++i)
            {
                TLongtail_Hash hash = version_index->m_AssetHash[i];
                uint64_t* existing_index = hashes.Get(hash);
                if (existing_index == 0)
                {
                    hashes.Put(hash, i);

                    assets_index[unique_asset_count++] = i;

                    const char* asset_path = &version_index->m_NameData[version_index->m_NameOffset[i]];
                    content_tags[i] = GetContentTag(asset_folder, asset_path);
                    char* full_path = (char*)Trove_ConcatPath(asset_folder, asset_path);
                    Trove_DenormalizePath(full_path);
                    HTroveOpenReadFile file_handle = Trove_OpenReadFile(full_path);
                    asset_sizes[i] = (uint64_t)Trove_GetFileSize(file_handle);
                    Trove_CloseReadFile(file_handle);
                    free((char*)full_path);
                }
            }

            struct CompareAssetEntry
            {
                CompareAssetEntry(const TLongtail_Hash* asset_hashes, const uint64_t* asset_sizes, const TLongtail_Hash* asset_tags)
                    : asset_hashes(asset_hashes)
                    , asset_sizes(asset_sizes)
                    , asset_tags(asset_tags)
                {

                }

                // This sorting algorithm is very arbitrary!
                bool operator()(uint32_t a, uint32_t b) const
                {   
                    TLongtail_Hash a_hash = asset_hashes[a];
                    TLongtail_Hash b_hash = asset_hashes[b];
                    uint64_t a_size = asset_sizes[a];
                    uint64_t b_size = asset_sizes[b];
                    TLongtail_Hash a_tag = asset_tags[a];
                    TLongtail_Hash b_tag = asset_tags[b];
                    if (a_tag < b_tag)
                    {
                        return true;
                    }
                    else if (b_tag < a_tag)
                    {
                        return false;
                    }
                    if (a_size < b_size)
                    {
                        return true;
                    }
                    else if (b_size < a_size)
                    {
                        return false;
                    }
                    return (a_hash < b_hash);
                }

                const TLongtail_Hash* asset_hashes;
                const uint64_t* asset_sizes;
                const TLongtail_Hash* asset_tags;
            };

            std::sort(&assets_index[0], &assets_index[unique_asset_count], CompareAssetEntry(version_index->m_AssetHash, asset_sizes, content_tags));

            if (0)
            {
                for (uint64_t i = 0; i < unique_asset_count; ++i)
                {
                    uint32_t asset_index = assets_index[i];
                    const char* asset_path  = &version_index->m_NameData[version_index->m_NameOffset[asset_index]];

                    char path_hash_str[64];
                    sprintf(path_hash_str, "0x%" PRIx64, version_index->m_AssetHash[asset_index]);
                    char content_hash_str[64];
                    sprintf(content_hash_str, "0x%" PRIx64, version_index->m_AssetHash[asset_index]);

                    printf("%s (%s) = %s\n", asset_path, path_hash_str, content_hash_str);
                }
            }

            const char* content_folder = "D:\\Temp\\local_content";
            BlockIndex** block_indexes = (BlockIndex**)malloc(sizeof(BlockIndex*) * unique_asset_count);
            // Build blocks
            {
                static const uint32_t MAX_ASSETS_PER_BLOCK = 4096;
                static const uint32_t MAX_BLOCK_SIZE = 65536;
                uint32_t stored_asset_indexes[MAX_ASSETS_PER_BLOCK];

                uint64_t current_size = 0;
                TLongtail_Hash current_tag = content_tags[assets_index[0]];
                uint64_t i = 0;
                uint32_t asset_count_in_block = 0;
                uint32_t block_count = 0;

                while (i < unique_asset_count)
                {
                    uint64_t asset_index = assets_index[i];
                    while (current_size < MAX_BLOCK_SIZE && current_tag == content_tags[asset_index] && asset_count_in_block < MAX_ASSETS_PER_BLOCK)
                    {
                        current_size += asset_sizes[asset_index];
                        stored_asset_indexes[asset_count_in_block] = asset_index;
                        ++i;
                        asset_index = assets_index[i];
                        ++asset_count_in_block;
                    }

                    block_indexes[block_count] = WriteBlock(malloc(GetBlockIndexSize(asset_count_in_block)), content_folder, local_path_1, asset_count_in_block, stored_asset_indexes, version_index, asset_sizes);

                    ++block_count;
                    current_tag = content_tags[asset_index];
                    current_size = 0;
                    asset_count_in_block = 0;
                }
                if (current_size > 0)
                {
                    block_indexes[block_count] = WriteBlock(malloc(GetBlockIndexSize(asset_count_in_block)), content_folder, local_path_1, asset_count_in_block, stored_asset_indexes, version_index, asset_sizes);
                    ++block_count;
                }
                printf("Block count: %u\n", block_count);

                // Build Content Index (from block list)
                size_t content_index_size = GetContentIndexSize(block_count, unique_asset_count);
                ContentIndex* content_index = (ContentIndex*)malloc(content_index_size);

                printf("Block count: %" PRIu64 ", Content index size: %" PRIu64 "\n", (uint64_t)block_count, (uint64_t)content_index_size);

                content_index->m_BlockCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex)];
                content_index->m_AssetCount = (uint64_t*)&((char*)content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
                *content_index->m_BlockCount = block_count;
                *content_index->m_AssetCount = unique_asset_count;
                InitContentIndex(content_index);

                uint64_t asset_index = 0;
                for (uint32_t i = 0; i < block_count; ++i)
                {
                    content_index->m_BlockHash[i] = block_indexes[i]->m_BlockHash;
                    uint64_t asset_offset = 0;
                    for (uint64_t a = 0; a < block_indexes[i]->m_AssetCount; ++a)
                    {
                        content_index->m_AssetHash[asset_index] = block_indexes[i]->m_Entries[a].m_AssetHash;
                        content_index->m_AssetBlock[asset_index] = i;
                        content_index->m_AssetOffset[asset_index] = asset_offset;
                        content_index->m_AssetLength[asset_index] = block_indexes[i]->m_Entries[a].m_AssetSize;
                        asset_offset += block_indexes[i]->m_Entries[a].m_AssetSize;
                    }
                    free(block_indexes[block_count]);
                }
                free(block_indexes);

                HTroveOpenWriteFile file_handle = Trove_OpenWriteFile("D:\\Temp\\ContentIndex.lci");
                Trove_Write(file_handle, 0, content_index_size - sizeof(ContentIndex), &content_index[1]);
                Trove_CloseWriteFile(file_handle);

                free(content_index);
            }

            free(hash_mem);

            free(content_tags);
            free(assets_index);
            free(asset_sizes);

            ////////// Upload
            // download remote_content_index.lci
            // build_local_content_index(content_folder, local_content_index_path)
            // build_local_version_index(asset_folder, local_version_index_path)

            // build_blocks_for_missing_assets(asset_folder, local_version_index_path, content_folder, local_content_index_path, remote_content_index_path, block_list_path)
            // upload [block_list_path] from content_folder
            // upload local_content_index_path

            ////////// Download
            // download version_index.lci
            // build_local_content_index(content_folder, local_content_index_path)
            // build_list_of_missing_assets(local_version_index_path, local_content_index_path)
            // download remote_content_index.lci
            // list_blocks_for_missing_assets(local_version_index_path, remote_content_index.lci)
            // download [block_list_path] to content_folder
            // merge_content_index(local_content_index_path, content_folder, block_list_path)
            // build_local_asset_folder(local_version_index_path, local_content_index_path, content_folder, asset_folder)




            // Build Local Version Index    <- asset folder (local fs) -> local version index (local fs)
            // Load Remote Content Index    <- remote version index (remote fs) <- local version index (local fs)
            // Calculate list of assets not in Remote Content Index
            // Load Local Content Index
            // Calculate list of assets not in Local Content Index
            // Build blocks missing assets into Local Content Index
            // Upload blocks from Local Content Index for all missing assets to Remote Content Index
            // Upload Version Index
        }
        free(version_index);
    }
}

#endif




#if 0
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

struct AssetFolder
{
    const char* m_FolderPath;
};

LONGTAIL_DECLARE_ARRAY_TYPE(AssetFolder, malloc, free)

typedef void (*ProcessEntry)(void* context, const char* root_path, const char* file_name);

static int RecurseTree(const char* root_folder, ProcessEntry entry_processor, void* context)
{
    AssetFolder* asset_folders = SetCapacity_AssetFolder((AssetFolder*)0, 256);

    uint32_t folder_index = 0;

    Push_AssetFolder(asset_folders)->m_FolderPath = strdup(root_folder);

    HTrove_FSIterator fs_iterator = (HTrove_FSIterator)alloca(Trove_GetFSIteratorSize());
    while (folder_index != GetSize_AssetFolder(asset_folders))
    {
        const char* asset_folder = asset_folders[folder_index++].m_FolderPath;

        if (Trove_StartFind(fs_iterator, asset_folder))
        {
            do
            {
                if (const char* dir_name = Trove_GetDirectoryName(fs_iterator))
                {
                    Push_AssetFolder(asset_folders)->m_FolderPath = Trove_ConcatPath(asset_folder, dir_name);
                    if (GetSize_AssetFolder(asset_folders) == GetCapacity_AssetFolder(asset_folders))
                    {
                        uint32_t unprocessed_count = (GetSize_AssetFolder(asset_folders) - folder_index);
                        if (folder_index > 0)
                        {
                            if (unprocessed_count > 0)
                            {
                                memmove(asset_folders, &asset_folders[folder_index], sizeof(AssetFolder) * unprocessed_count);
                                SetSize_AssetFolder(asset_folders, unprocessed_count);
                            }
                            folder_index = 0;
                        }
                        else
                        {
                            AssetFolder* asset_folders_new = SetCapacity_AssetFolder((AssetFolder*)0, GetCapacity_AssetFolder(asset_folders) + 256);
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
                }
                else if(const char* file_name = Trove_GetFileName(fs_iterator))
                {
                    entry_processor(context, asset_folder, file_name);
                }
            }while(Trove_FindNext(fs_iterator));
            Trove_CloseFind(fs_iterator);
        }
        free((void*)asset_folder);
    }
    Free_AssetFolder(asset_folders);
    return 1;
}

TEST(Longtail, Basic)
{
    TestStorage storage;
}

struct Longtail_PathEntry
{
    TLongtail_Hash m_PathHash;
    uint32_t m_PathOffset;
    uint32_t m_ParentIndex;
    uint32_t m_ChildCount;  // (uint32_t)-1 for files
};

struct Longtail_StringCacheEntry
{
    TLongtail_Hash m_StringHash;
    uint32_t m_StringOffset;
};

struct Longtail_StringCache
{
    Longtail_StringCacheEntry* m_Entry;
    char* m_DataBuffer;
    uint32_t m_EntryCount;
    uint32_t m_DataBufferSize;
};

uint32_t AddString(Longtail_StringCache* cache, const char* string, uint32_t string_length, TLongtail_Hash string_hash)
{
    for (uint32_t i = 0; i < cache->m_EntryCount; ++i)
    {
        if (cache->m_Entry->m_StringHash == string_hash)
        {
            return cache->m_Entry->m_StringOffset;
        }
    }
    uint32_t string_offset = cache->m_DataBufferSize;
    cache->m_Entry = (Longtail_StringCacheEntry*)realloc(cache->m_Entry, sizeof(Longtail_StringCacheEntry) * (cache->m_EntryCount + 1));
    cache->m_Entry[cache->m_EntryCount].m_StringHash = string_hash;
    cache->m_DataBuffer = (char*)realloc(cache->m_DataBuffer, cache->m_DataBufferSize + string_length + 1);
    memcpy(&cache->m_DataBuffer[cache->m_DataBufferSize], string, string_length);
    cache->m_DataBuffer[cache->m_DataBufferSize + string_length] = '\0';
    cache->m_DataBufferSize += (string_length + 1);
    cache->m_EntryCount += 1;
    return string_offset;
}

struct Longtail_Paths
{
    Longtail_PathEntry* m_PathEntries;
    char* m_PathStorage;
};
/*
uint32_t find_path_offset(const Longtail_Paths& paths, const TLongtail_Hash* sub_path_hashes, uint32_t path_count, TLongtail_Hash sub_path_hash)
{
    uint32_t path_index = 0;
    while (path_index < path_count)
    {
        if (sub_path_hashes[path_index] == sub_path_hash)
        {
            return paths.m_PathEntries[path_index].m_PathOffset;
        }
        ++path_index;
    }
    return path_count;
};
*/
TEST(Longtail, PathIndex)
{
    Longtail_StringCache string_cache;
    memset(&string_cache, 0, sizeof(string_cache));

    uint32_t path_count = 0;
    uint32_t path_data_size = 0;
    Longtail_Paths paths;
    paths.m_PathEntries = 0;
    paths.m_PathStorage = (char*)0;
    TLongtail_Hash* sub_path_hashes = 0;

    auto find_path_index = [](const Longtail_Paths& paths, TLongtail_Hash* sub_path_hashes, uint32_t path_count, uint32_t parent_path_index, TLongtail_Hash sub_path_hash)
    {
        uint32_t path_index = 0;
        while (path_index < path_count)
        {
            if (sub_path_hashes[path_index] == sub_path_hash && paths.m_PathEntries[path_index].m_ParentIndex == parent_path_index)
            {
                return path_index;
            }
            ++path_index;
        }
        return path_count;
    };

    auto get_sub_path_hash = [](const char* path_begin, const char* path_end)
    {
        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, (meow_umm)(path_end - path_begin), (void*)path_begin);
        uint64_t path_hash = MeowU64From(MeowEnd(&state, 0), 0);
        return path_hash;
    };

    auto find_sub_path_end = [](const char* path)
    {
        while (*path && *path != '/')
        {
            ++path;
        }
        return path;
    };

    auto add_sub_path = [](Longtail_Paths& paths, uint32_t path_count, uint32_t parent_path_index, bool is_directory, TLongtail_Hash path_hash, Longtail_StringCache* string_cache, TLongtail_Hash*& sub_path_hashes, const char* sub_path, uint32_t sub_path_length, TLongtail_Hash sub_path_hash)
    {
        sub_path_hashes = (TLongtail_Hash*)realloc(sub_path_hashes, sizeof(TLongtail_Hash) * (path_count + 1));
        sub_path_hashes[path_count] = sub_path_hash;
        paths.m_PathEntries = (Longtail_PathEntry*)realloc(paths.m_PathEntries, sizeof(Longtail_PathEntry) * (path_count + 1));
        paths.m_PathEntries[path_count].m_PathHash = path_hash;
        paths.m_PathEntries[path_count].m_ParentIndex = parent_path_index;
        paths.m_PathEntries[path_count].m_PathOffset = AddString(string_cache, sub_path, sub_path_length, sub_path_hash);
        paths.m_PathEntries[path_count].m_ChildCount = is_directory ? 0u : (uint32_t)-1;
        if (parent_path_index != (uint32_t)-1)
        {
            paths.m_PathEntries[parent_path_index].m_ChildCount++;
        }
        return path_count;
    };

    const char* root_path = "D:\\TestContent\\Version_1";
//    const char* root_path = "/Users/danengelbrecht/Documents/Projects/blossom_blast_saga/build/default";
    const char* cache_path = "D:\\Temp\\longtail\\cache";
//    const char* cache_path = "/Users/danengelbrecht/tmp/cache";

    struct Folders
    {
        char** folder_names;
        uint32_t folder_count;
    };

    Folders folders;
    folders.folder_names = 0;
    folders.folder_count = 0;

    auto add_folder = [](void* context, const char* root_path, const char* file_name)
    {
        Folders* folders = (Folders*)context;
        uint32_t folder_count = 0;
        folders->folder_names = (char**)realloc(folders->folder_names, sizeof(char*) * (folders->folder_count + 1));
        folders->folder_names[folders->folder_count] = (char*)malloc(strlen(root_path) + 1 + strlen(file_name) + 1);
        strcpy(folders->folder_names[folders->folder_count], root_path);
        strcpy(&folders->folder_names[folders->folder_count][strlen(root_path)], "/");
        strcpy(&folders->folder_names[folders->folder_count][strlen(root_path) + 1], file_name);
        char* backslash = strchr(folders->folder_names[folders->folder_count], '\\');
        while (backslash)
        {
            *backslash = '/';
            backslash = strchr(folders->folder_names[folders->folder_count], '\\');
        }
        ++folders->folder_count;
    };

    RecurseTree(root_path, add_folder, &folders);

    for (uint32_t p = 0; p < folders.folder_count; ++p)
    {
        uint32_t parent_path_index = (uint32_t)-1;
        const char* path = folders.folder_names[p];
        const char* sub_path_end = find_sub_path_end(path);
        while (*sub_path_end)
        {
            TLongtail_Hash sub_path_hash = get_sub_path_hash(path, sub_path_end);
            uint32_t path_index = find_path_index(paths, sub_path_hashes, path_count, parent_path_index, sub_path_hash);
            if (path_index == path_count)
            {
                const uint32_t path_length = (uint32_t)(sub_path_end - path);
                TLongtail_Hash path_hash = get_sub_path_hash(folders.folder_names[p], sub_path_end);
                path_index = add_sub_path(paths, path_count, parent_path_index, true, path_hash, &string_cache, sub_path_hashes, path, path_length, sub_path_hash);
                ++path_count;
            }

            parent_path_index = path_index;
            path = sub_path_end + 1;
            sub_path_end = find_sub_path_end(path);
        }
        TLongtail_Hash sub_path_hash = get_sub_path_hash(path, sub_path_end);
        ASSERT_EQ(path_count, find_path_index(paths, sub_path_hashes, path_count, parent_path_index, sub_path_hash));
        const uint32_t path_length = (uint32_t)(sub_path_end - path);
        TLongtail_Hash path_hash = get_sub_path_hash(folders.folder_names[p], sub_path_end);
        add_sub_path(paths, path_count, parent_path_index, false, path_hash, &string_cache, sub_path_hashes, path, path_length, sub_path_hash);
        ++path_count;
    }

    free(folders.folder_names);

    paths.m_PathStorage = string_cache.m_DataBuffer;
    string_cache.m_DataBuffer = 0;

    for (uint32_t sub_path_index = 0; sub_path_index < path_count; ++sub_path_index)
    {
        char* path = strdup(&paths.m_PathStorage[paths.m_PathEntries[sub_path_index].m_PathOffset]);
        uint32_t parent_path_index = paths.m_PathEntries[sub_path_index].m_ParentIndex;
        while (parent_path_index != (uint32_t)-1)
        {
            const char* parent_path = &paths.m_PathStorage[paths.m_PathEntries[parent_path_index].m_PathOffset];
            size_t path_length = strlen(path);
            size_t parent_path_length = strlen(parent_path);
            path = (char*)realloc(path, path_length + 1 + parent_path_length + 1);
            memmove(&path[parent_path_length + 1], path, path_length);
            path[path_length + 1 + parent_path_length] = '\0';
            path[parent_path_length] = '/';
            memcpy(path, parent_path, parent_path_length);
            parent_path_index = paths.m_PathEntries[parent_path_index].m_ParentIndex;
        }
        if (paths.m_PathEntries[sub_path_index].m_ChildCount == (uint32_t)-1)
        {
            printf("File '%s'\n", path);
        }
        else
        {
            printf("Folder '%s' (%u items)\n", path, paths.m_PathEntries[sub_path_index].m_ChildCount);
        }
        free(path);
    };

    free(sub_path_hashes);
    free(paths.m_PathStorage);
    free(paths.m_PathEntries);
}

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
    HTroveOpenReadFile file_handle = Trove_OpenReadFile(hash_job->m_Asset.m_Path);
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    if(file_handle)
    {
        uint64_t file_size = Trove_GetFileSize(file_handle);
        hash_job->m_Asset.m_Size = file_size;

        uint8_t batch_data[65536];
        uint64_t offset = 0;
        while (offset != file_size)
        {
            meow_umm len = (meow_umm)((file_size - offset) < sizeof(batch_data) ? (file_size - offset) : sizeof(batch_data));
            bool read_ok = Trove_Read(file_handle, offset, len, batch_data);
            assert(read_ok);
            offset += len;
            MeowAbsorb(&state, len, batch_data);
        }
        Trove_CloseReadFile(file_handle);
    }
    meow_u128 hash = MeowEnd(&state, 0);
    hash_job->m_Asset.m_Hash = hash;
    nadir::AtomicAdd32(hash_job->m_PendingCount, -1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

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
    job->m_Asset.m_Path = Trove_ConcatPath(root_path, file_name);
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


//struct StoredBlock
//{
//    TLongtail_Hash m_Tag;
//    TLongtail_Hash m_Hash;
//    uint64_t m_Size;
//};

//struct LiveBlock
//{
//    uint8_t* m_Data;
//    uint64_t m_CommitedSize;
//};

//LONGTAIL_DECLARE_ARRAY_TYPE(StoredBlock, malloc, free)
//LONGTAIL_DECLARE_ARRAY_TYPE(LiveBlock, malloc, free)
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

        uint8_t* p = (uint8_t*)(malloc((size_t)(sizeof(CompressJob) + length)));

        CompressJob* compress_job = (CompressJob*)p;
        p += sizeof(CompressJob);
        compress_job->active_job_count = compress_storage->m_ActiveJobCount;
        compress_job->base_storage = compress_storage->m_BaseStorage;
        compress_job->hash = hash;
        compress_job->length = length;
        compress_job->data = p;

        memcpy((void*)compress_job->data, data, (size_t)length);
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

        char* compressed_buffer = (char*)malloc((size_t)(compressed_size));
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

        HTroveOpenWriteFile f = Trove_OpenWriteFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }

        bool ok = Trove_Write(f, 0, length, data);
        Trove_CloseWriteFile(f);
        return ok ? 1 : 0;
    }

    static int ReadBlock(BlockStorage* storage, TLongtail_Hash hash, uint64_t length, void* data)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        HTroveOpenReadFile f = Trove_OpenReadFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }

        bool ok = Trove_Read(f, 0, length, data);
        Trove_CloseReadFile(f);
        return ok ? 1 : 0;
    }

    static uint64_t GetStoredSize(BlockStorage* storage, TLongtail_Hash hash)
    {
        DiskBlockStorage* disk_block_storage = (DiskBlockStorage*)storage;
        const char* path = disk_block_storage->MakeBlockPath(hash);
        HTroveOpenReadFile f = Trove_OpenReadFile(path);
        free((char*)path);
        if (!f)
        {
            return 0;
        }
        uint64_t size = Trove_GetFileSize(f);
        Trove_CloseReadFile(f);
        return size;
    }

    const char* MakeBlockPath(TLongtail_Hash hash)
    {
        char file_name[64];
        sprintf(file_name, "0x%" PRIx64, hash);
        const char* path = Trove_ConcatPath(m_StorePath, file_name);
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
        if (block_index == simple_storage->m_CurrentBlockIndex && simple_storage->m_CurrentBlockData)
        {
            if (0 == PersistCurrentBlock(simple_storage))
            {
                return 0;
            }
            free(simple_storage->m_CurrentBlockData);
            simple_storage->m_CurrentBlockData = 0;
            simple_storage->m_CurrentBlockSize = 0u;
            simple_storage->m_CurrentBlockUsedSize = 0u;
            simple_storage->m_CurrentBlockIndex = GetSize_TLongtail_Hash(simple_storage->m_Blocks);
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
    HTroveOpenReadFile f= Trove_OpenReadFile(asset->m_Path);
    if (f == 0)
    {
        return 0;
    }
    Trove_Read(f, 0, byte_count, data);
    Trove_CloseReadFile(f);
    return 1;
}

LONGTAIL_DECLARE_ARRAY_TYPE(uint8_t, malloc, free)

int OutputStream(void* context, uint64_t byte_count, const uint8_t* data)
{
    uint8_t** buffer = (uint8_t**)context;
    *buffer = SetCapacity_uint8_t(*buffer, (uint32_t)byte_count);
    SetSize_uint8_t(*buffer, (uint32_t)byte_count);
    memmove(*buffer, data, (size_t)byte_count);
    return 1;
}

static TLongtail_Hash GetPathHash(const char* path)
{
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    MeowAbsorb(&state, strlen(path), (void*)path);
    TLongtail_Hash path_hash = MeowU64From(MeowEnd(&state, 0), 0);
    return path_hash;
}

LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_BlockStore, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_BlockAssets, malloc, free)

struct Longtail_AssetRegistry
{
    // If all the assets are still in the new index, keep the block and the assets, otherwise throw it away

    // For each block we want the assets in that block
    // Build a map of all new asset content hash to "invalid block"

    // Iterate over asset content hashes is each block, check if we all the content hashes are in the new asset set
    //  Yes: Store block and set all the asset content hases in block to point to index of stored block
    //  No: Don't store block
    // Iterate over all assets, get path hash, look up content hash, look up block index in map from asset content has to block index
    // if "invalid block" start a new block and store content there

    // Queries:
    //  Is the asset content hash present

    // Need:
    //   A list of blocks with the asset content in them

    struct Longtail_AssetEntry* m_AssetArray;

};

struct OrderedCacheContent
{
    TLongtail_Hash* m_BlockHashes;
    Longtail_AssetEntry* m_AssetEntries;
    struct Longtail_BlockAssets* m_BlockAssets;
};

struct AssetSorter
{
    AssetSorter(const struct Longtail_AssetEntry* asset_entries)
        : asset_entries(asset_entries)
    {}
    const struct Longtail_AssetEntry* asset_entries;
    bool operator()(uint32_t left, uint32_t right)
    {
        const Longtail_AssetEntry& a = asset_entries[left];
        const Longtail_AssetEntry& b = asset_entries[right];
        if (a.m_BlockStore.m_BlockIndex == b.m_BlockStore.m_BlockIndex)
        {
            return a.m_BlockStore.m_StartOffset < b.m_BlockStore.m_StartOffset;
        }
        return a.m_BlockStore.m_BlockIndex < b.m_BlockStore.m_BlockIndex;
    }
};

struct OrderedCacheContent* BuildOrderedCacheContent(uint32_t asset_entry_count, const struct Longtail_AssetEntry* asset_entries, uint32_t block_count, const TLongtail_Hash* block_hashes)
{
    OrderedCacheContent* ordered_content = (OrderedCacheContent*)malloc(sizeof(OrderedCacheContent));
    ordered_content->m_BlockHashes = SetCapacity_TLongtail_Hash((TLongtail_Hash*)0, block_count);
    ordered_content->m_AssetEntries = SetCapacity_Longtail_AssetEntry((Longtail_AssetEntry*)0, asset_entry_count);
    ordered_content->m_BlockAssets = SetCapacity_Longtail_BlockAssets((Longtail_BlockAssets*)0, asset_entry_count); // Worst case

    uint32_t* asset_order = new uint32_t[asset_entry_count];
    for (uint32_t ai = 0; ai < asset_entry_count; ++ai)
    {
        asset_order[ai] = ai;
    }

    // Sort the asset references so the blocks are consecutive ascending
    std::sort(&asset_order[0], &asset_order[asset_entry_count], AssetSorter(asset_entries));

    uint32_t ai = 0;
    while (ai < asset_entry_count)
    {
        uint32_t asset_index = asset_order[ai];
        const struct Longtail_AssetEntry* a = &asset_entries[asset_index];
        uint32_t block_index = a->m_BlockStore.m_BlockIndex;
        Longtail_BlockAssets* block_asset = Push_Longtail_BlockAssets(ordered_content->m_BlockAssets);
        uint32_t block_asset_index = GetSize_Longtail_AssetEntry(ordered_content->m_AssetEntries);
        block_asset->m_AssetIndex = block_asset_index;
        block_asset->m_AssetCount = 1;
        *Push_Longtail_AssetEntry(ordered_content->m_AssetEntries) = *a;
        *Push_TLongtail_Hash(ordered_content->m_BlockHashes) = block_hashes[block_index];

        ++ai;
        asset_index = asset_order[ai];

        while (ai < asset_entry_count && asset_entries[asset_index].m_BlockStore.m_BlockIndex == block_index)
        {
            const struct Longtail_AssetEntry* a = &asset_entries[asset_index];
            *Push_Longtail_AssetEntry(ordered_content->m_AssetEntries) = *a;
            ++block_asset->m_AssetCount;

            ++ai;
            asset_index = asset_order[ai];
        }

    }

    delete [] asset_order;

    return ordered_content;
}

struct Longtail_IndexDiffer
{
    struct Longtail_AssetEntry* m_AssetArray;
    TLongtail_Hash* m_BlockHashes;
    struct Longtail_BlockAssets* m_BlockAssets;
    TLongtail_Hash* m_AssetHashes;
};

int Longtail_CompareAssetEntry(const void* element1, const void* element2)
{
    const struct Longtail_AssetEntry* entry1 = (const struct Longtail_AssetEntry*)element1;
    const struct Longtail_AssetEntry* entry2 = (const struct Longtail_AssetEntry*)element2;
    return (int)((int64_t)entry1->m_BlockStore.m_BlockIndex - (int64_t)entry2->m_BlockStore.m_BlockIndex);
}

Longtail_IndexDiffer* CreateDiffer(void* mem, uint32_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint32_t block_count, TLongtail_Hash* block_hashes, uint32_t new_asset_count, TLongtail_Hash* new_asset_hashes)
{
    Longtail_IndexDiffer* index_differ = (Longtail_IndexDiffer*)mem;
    index_differ->m_AssetArray = 0;
    index_differ->m_BlockHashes = 0;
//    index_differ->m_AssetArray = Longtail_Array_SetCapacity(index_differ->m_AssetArray, asset_entry_count);
//    index_differ->m_BlockArray = Longtail_Array_SetCapacity(index_differ->m_BlockArray, block_entry_count);
//    memcpy(index_differ->m_AssetArray, asset_entries, sizeof(Longtail_AssetEntry) * asset_entry_count);
//    memcpy(index_differ->m_BlockArray, block_entries, sizeof(Longtail_BlockStore) * block_entry_count);
    qsort(asset_entries, asset_entry_count, sizeof(Longtail_AssetEntry), Longtail_CompareAssetEntry);
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize(new_asset_count);
    void* hash_mem = malloc(hash_size);
    jc::HashTable<TLongtail_Hash, uint32_t> hashes;
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
    index_differ->m_BlockHashes = SetCapacity_TLongtail_Hash(index_differ->m_BlockHashes, block_count);

    for (uint32_t a = 0; a < new_asset_count; ++a)
    {
        TLongtail_Hash asset_hash = new_asset_hashes[a];
        uint32_t* existing_block_index = hashes.Get(asset_hash);
        if (existing_block_index)
        {
            Longtail_AssetEntry* asset_entry = Push_Longtail_AssetEntry(index_differ->m_AssetArray);
//            TLongtail_Hash* block_hash = Push_TLongtail_Hash(index_differ->m_BlockHashes);
            asset_entry->m_BlockStore.m_BlockIndex = *existing_block_index;
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

#if 0
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


//    const char* root_path = "D:\\TestContent\\Version_1";
    const char* root_path = "/Users/danengelbrecht/Documents/Projects/blossom_blast_saga/build/default";
//    const char* cache_path = "D:\\Temp\\longtail\\cache";
    const char* cache_path = "/Users/danengelbrecht/tmp/cache";

    ScanExistingDataContext scan_context;
    scan_context.m_Hashes = 0;
    RecurseTree(cache_path, ScanHash, &scan_context);
    uint32_t found_count = GetSize_TLongtail_Hash(scan_context.m_Hashes);
    for (uint32_t b = 0; b < found_count; ++b)
    {
        printf("Block %u, hash %llu\n",
            b,
            (unsigned long long)scan_context.m_Hashes[b]);
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
                    (unsigned long long)asset->m_Size,
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
    printf("Found %llu redundant files comprising %llu bytes out of %llu bytes\n", (unsigned long long)redundant_file_count, (unsigned long long)redundant_byte_size, (unsigned long long)total_byte_size);

    Longtail_Builder builder;

    {
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
                        (unsigned long long)asset_entry->m_AssetHash,
                        (unsigned int)asset_entry->m_BlockStore.m_BlockIndex,
                        (unsigned long long)asset_entry->m_BlockStore.m_StartOffset,
                        (unsigned long long)asset_entry->m_BlockStore.m_Length);
                }

                for (uint32_t b = 0; b < block_count; ++b)
                {
                    printf("Block %u, hash %llu\n",
                        b,
                        (unsigned long long)builder.m_BlockHashes[b]);
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

        {
            const size_t longtail_size = Longtail_GetSize(GetSize_Longtail_AssetEntry(builder.m_AssetEntries), GetSize_TLongtail_Hash(builder.m_BlockHashes));
            struct Longtail* longtail = Longtail_Open(malloc(longtail_size),
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
        }
    }
    struct OrderedCacheContent* ordered_content = BuildOrderedCacheContent(
        GetSize_Longtail_AssetEntry(builder.m_AssetEntries), builder.m_AssetEntries,
        GetSize_TLongtail_Hash(builder.m_BlockHashes), builder.m_BlockHashes);

    Longtail_Builder builder2;
    {
        DiskBlockStorage disk_block_storage2(cache_path);
        CompressStorage block_storage2(&disk_block_storage2.m_Storage, shed, &pendingCount);

        {
            SimpleWriteStorage write_storage2(&block_storage2.m_Storage, 131072u);
            Longtail_Builder_Initialize(&builder2, &write_storage2.m_Storage);

            uint32_t block_asset_count = GetSize_Longtail_BlockAssets(ordered_content->m_BlockAssets);

            for (uint32_t ba = 0; ba < block_asset_count; ++ba)
            {
                TLongtail_Hash block_hash = ordered_content->m_BlockHashes[ba];

                uint32_t block_asset_count = ordered_content->m_BlockAssets[ba].m_AssetCount;
                uint32_t asset_entry_index = ordered_content->m_BlockAssets[ba].m_AssetIndex;

                Longtail_Builder_AddExistingBlock(&builder2, block_hash, block_asset_count, &ordered_content->m_AssetEntries[asset_entry_index]);
            }

            Longtail_FinalizeBuilder(&builder2);
        }

        while (pendingCount > 0)
        {
            if (Bikeshed_ExecuteOne(shed, 0))
            {
                continue;
            }
            if (old_pending_count != pendingCount)
            {
                old_pending_count = pendingCount;
                printf("Files2 left to store: %d\n", old_pending_count);
            }
            nadir::Sleep(1000);
        }
        printf("Added %u existing assets in %u blocks\n", GetSize_Longtail_AssetEntry(ordered_content->m_AssetEntries), GetSize_TLongtail_Hash(ordered_content->m_BlockHashes));

        const size_t longtail_size = Longtail_GetSize(GetSize_Longtail_AssetEntry(builder2.m_AssetEntries), GetSize_TLongtail_Hash(builder2.m_BlockHashes));
        struct Longtail* longtail = Longtail_Open(malloc(longtail_size),
            GetSize_Longtail_AssetEntry(builder2.m_AssetEntries), builder2.m_AssetEntries,
            GetSize_TLongtail_Hash(builder2.m_BlockHashes), builder2.m_BlockHashes);

        {
            SimpleReadStorage read_storage(builder2.m_BlockHashes, &block_storage2.m_Storage);
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
    }

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
#endif

#endif // 0