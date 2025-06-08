#include "longtail_concurrentchunkwrite.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"
#include <inttypes.h>
#include <errno.h>

struct OpenFileEntry
{
    Longtail_StorageAPI_HOpenFile m_FileHandle;
    char* m_FullPath;
    TLongtail_Atomic32 m_ActiveOpenCount;
    HLongtail_SpinLock m_SpinLock;
    uint32_t m_OpenCount;
    TLongtail_Atomic64 m_FileSize;
    TLongtail_Atomic64 m_BytesLeftToWrite;
};

struct ConcurrentChunkWriteAPI
{
    struct Longtail_ConcurrentChunkWriteAPI m_ConcurrentChunkWriteAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    const struct Longtail_VersionIndex* m_VersionIndex;
    struct OpenFileEntry** m_AssetEntries;
    char* m_BasePath;
};

static int ConcurrentChunkWriteAPI_Open(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api,
    uint32_t asset_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_chunk_write_api != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return EINVAL);
    struct OpenFileEntry* open_file_entry = api->m_AssetEntries[asset_index];

    if (open_file_entry->m_SpinLock == 0)
    {
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle == 0, return EINVAL);
        int err = EnsureParentPathExists(api->m_StorageAPI, open_file_entry->m_FullPath);
        if (err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
            return err;
        }
        err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, open_file_entry->m_FullPath, 0, &open_file_entry->m_FileHandle);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile() failed with %d", err)
            return err;
        }
        open_file_entry->m_OpenCount++;
        Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
        return 0;
    }

    Longtail_LockSpinLock(open_file_entry->m_SpinLock);
    if (open_file_entry->m_FileHandle != 0)
    {
        Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        return 0;
    }
    if (open_file_entry->m_OpenCount > 0)
    {
        int err = api->m_StorageAPI->OpenAppendFile(api->m_StorageAPI, open_file_entry->m_FullPath, &open_file_entry->m_FileHandle);
        if (err != 0)
        {
            Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenAppendFile() failed with %d", err)
            return err;
        }

        open_file_entry->m_OpenCount++;
        Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        return 0;
    }
    Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);

    int err = EnsureParentPathExists(api->m_StorageAPI, open_file_entry->m_FullPath);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
        return err;
    }

    Longtail_LockSpinLock(open_file_entry->m_SpinLock);
    if (open_file_entry->m_FileHandle != 0)
    {
        Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        return 0;
    }

    if (open_file_entry->m_OpenCount == 0)
    {
        err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, open_file_entry->m_FullPath, open_file_entry->m_FileSize, &open_file_entry->m_FileHandle);
    }
    else
    {
        err = api->m_StorageAPI->OpenAppendFile(api->m_StorageAPI, open_file_entry->m_FullPath, &open_file_entry->m_FileHandle);
    }
    if (err)
    {
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile/OpenAppendFile() failed with %d", err)
        return err;
    }
    open_file_entry->m_OpenCount++;
    Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
    Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);

    return 0;
}

static int ConcurrentChunkWriteAPI_Write(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api,
    uint32_t asset_index,
    uint64_t offset,
    uint32_t size,
    const void* input)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%u"),
        LONGTAIL_LOGFIELD(input, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_chunk_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return EINVAL);

    struct OpenFileEntry* open_file_entry = api->m_AssetEntries[asset_index];
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_ActiveOpenCount > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FullPath != 0, return EINVAL)

    Longtail_AtomicAdd64(&open_file_entry->m_BytesLeftToWrite, -(int64_t)size);
    int err = api->m_StorageAPI->Write(api->m_StorageAPI, open_file_entry->m_FileHandle, offset, size, input);
    if (err)
    {
        return err;
    }
    return 0;
}

static void ConcurrentChunkWriteAPI_Close(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api,
    uint32_t asset_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_chunk_write_api != 0, return);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return );
    struct OpenFileEntry* open_file_entry = api->m_AssetEntries[asset_index];

    int32_t OpenCount = Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, -1);
    if (open_file_entry->m_SpinLock == 0)
    {
        LONGTAIL_FATAL_ASSERT(ctx, OpenCount == 0, return)
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle != 0, return)
        api->m_StorageAPI->CloseFile(api->m_StorageAPI, open_file_entry->m_FileHandle);
        open_file_entry->m_FileHandle = 0;
        return;
    }
    if (OpenCount == 0)
    {
        if (open_file_entry->m_BytesLeftToWrite == 0)
        {
            Longtail_LockSpinLock(open_file_entry->m_SpinLock);
            Longtail_StorageAPI_HOpenFile file_handle = open_file_entry->m_FileHandle;
            if (file_handle)
            {
                api->m_StorageAPI->CloseFile(api->m_StorageAPI, file_handle);
                open_file_entry->m_FileHandle = 0;
            }
            Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        }
    }
}

static int ConcurrentChunkWriteAPI_Flush(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_chunk_write_api != 0, return EINVAL);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;

    for (ptrdiff_t i = 0; i < *api->m_VersionIndex->m_AssetCount; ++i)
    {
        struct OpenFileEntry* open_file_entry = api->m_AssetEntries[i];
        if (open_file_entry == 0)
        {
            continue;
        }
        if (open_file_entry->m_SpinLock == 0)
        {
            continue;
        }
        if (open_file_entry->m_ActiveOpenCount > 0)
        {
            continue;
        }
        Longtail_LockSpinLock(open_file_entry->m_SpinLock);
        if (open_file_entry->m_ActiveOpenCount == 0)
        {
            Longtail_StorageAPI_HOpenFile file_handle = open_file_entry->m_FileHandle;
            if (file_handle)
            {
                api->m_StorageAPI->CloseFile(api->m_StorageAPI, file_handle);
                open_file_entry->m_FileHandle = 0;
            }
        }
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
    }
    return 0;
}

static int ConcurrentChunkWriteAPI_CreateDir(struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api, uint32_t asset_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_chunk_write_api != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return EINVAL);

    const char* asset_path = &api->m_VersionIndex->m_NameData[api->m_VersionIndex->m_NameOffsets[asset_index]];
    char* full_asset_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_BasePath, asset_path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "storageAPI->ConcatPath() failed with %d", ENOMEM)
            return ENOMEM;
    }
    full_asset_path[strlen(full_asset_path) - 1] = '\0';

    int err = EnsureParentPathExists(api->m_StorageAPI, full_asset_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
        Longtail_Free(full_asset_path);
        return err;
    }

    err = api->m_StorageAPI->CreateDir(api->m_StorageAPI, full_asset_path);
    if (err == EEXIST)
    {
        err = 0;
    }
    Longtail_Free(full_asset_path);
    return err;
}

static void DisposeFileEntries(struct ConcurrentChunkWriteAPI* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    uint32_t asset_count = *api->m_VersionIndex->m_AssetCount;
    for (uint32_t a = 0; a < asset_count; ++a)
    {
        struct OpenFileEntry* open_file_entry = api->m_AssetEntries[a];
        if (open_file_entry == 0)
        {
            continue;
        }
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_ActiveOpenCount == 0, return)
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle == 0, return)
        if (open_file_entry->m_SpinLock)
        {
            Longtail_DeleteSpinLock(open_file_entry->m_SpinLock);
        }
        Longtail_Free((void*)open_file_entry);
    }
}

static void ConcurrentChunkWriteAPI_Dispose(struct Longtail_API* concurrent_chunk_write_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, concurrent_chunk_write_api != 0, return);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_chunk_write_api;

//    api->m_MaxOpenFileEntryCount = 0;
    int err = ConcurrentChunkWriteAPI_Flush(&api->m_ConcurrentChunkWriteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Flush() failed with %d", err)
    }

    DisposeFileEntries(api);

    Longtail_Free(api->m_BasePath);
    Longtail_Free(concurrent_chunk_write_api);
}

static int AllocateFileEntry(
    struct Longtail_StorageAPI* storageAPI,
    struct Longtail_VersionIndex* version_index,
    const char* base_path,
    uint32_t asset_index,
    struct OpenFileEntry** out_file_entry)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storageAPI, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(base_path, "%s"),
        LONGTAIL_LOGFIELD(asset_index, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storageAPI != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, base_path != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *version_index->m_AssetCount, return 0);

    const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
    int is_dir_path = asset_path[0] && asset_path[strlen(asset_path) - 1] == '/';
    if (is_dir_path)
    {
        *out_file_entry = 0;
        return 0;
    }
    char* full_asset_path = storageAPI->ConcatPath(storageAPI, base_path, asset_path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "storageAPI->ConcatPath() failed with %d", ENOMEM)
        return ENOMEM;
    }
    if (is_dir_path)
    {
        full_asset_path[strlen(full_asset_path) - 1] = '\0';
    }

    // If we had access to the store index we could figure out if an asset spans multiple blocks
    // more exactly, but that would probably add more execution time at this initializaton - not sure if
    // it is worth it.
    int has_spin_lock = version_index->m_AssetChunkCounts[asset_index] > 1;

    size_t open_file_entry_size = sizeof(struct OpenFileEntry) + (has_spin_lock ? Longtail_GetSpinLockSize() : 0) + strlen(full_asset_path) + 1;
    void* new_open_file_entry_mem = Longtail_Alloc("AllocateFileEntry", open_file_entry_size);
    if (new_open_file_entry_mem == 0)
    {
        Longtail_Free(full_asset_path);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)new_open_file_entry_mem;
    struct OpenFileEntry* new_open_file_entry = (struct OpenFileEntry*)p;
    p += sizeof(struct OpenFileEntry);
    if (has_spin_lock)
    {
        int err = Longtail_CreateSpinLock(p, &new_open_file_entry->m_SpinLock);
        if (err)
        {
            Longtail_Free((void*)new_open_file_entry);
            Longtail_Free(full_asset_path);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateSpinLock() failed with %d", err)
            return err;
        }
        p += Longtail_GetSpinLockSize();
    }
    else
    {
        new_open_file_entry->m_SpinLock = 0;
    }
    new_open_file_entry->m_FileHandle = 0;
    new_open_file_entry->m_ActiveOpenCount = 0;
    new_open_file_entry->m_FullPath = p;
    new_open_file_entry->m_OpenCount = 0;
    new_open_file_entry->m_FileSize = version_index->m_AssetSizes[asset_index];
    new_open_file_entry->m_BytesLeftToWrite = version_index->m_AssetSizes[asset_index];
    strcpy(new_open_file_entry->m_FullPath, full_asset_path);

    Longtail_Free(full_asset_path);
    full_asset_path = 0;
    *out_file_entry = new_open_file_entry;
    return 0;
}

static int ConcurrentChunkWriteAPI_Init(
    void* mem,
    struct Longtail_StorageAPI* storageAPI,
    struct Longtail_VersionIndex* version_index,
    struct Longtail_VersionDiff* version_diff,
    const char* base_path,
    struct Longtail_ConcurrentChunkWriteAPI** out_concurrent_chunk_write_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_concurrent_chunk_write_api, "%p"),
        LONGTAIL_LOGFIELD(base_path, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, storageAPI != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, base_path != 0, return 0);
    struct Longtail_ConcurrentChunkWriteAPI* api = Longtail_MakeConcurrentChunkWriteAPI(
        mem,
        ConcurrentChunkWriteAPI_Dispose,
        ConcurrentChunkWriteAPI_CreateDir,
        ConcurrentChunkWriteAPI_Open,
        ConcurrentChunkWriteAPI_Close,
        ConcurrentChunkWriteAPI_Write,
        ConcurrentChunkWriteAPI_Flush);

    struct ConcurrentChunkWriteAPI* concurrent_chunk_write_api = (struct ConcurrentChunkWriteAPI*)api;
    concurrent_chunk_write_api->m_StorageAPI = storageAPI;
    concurrent_chunk_write_api->m_VersionIndex = version_index;
    uint32_t asset_count = *version_index->m_AssetCount;
    concurrent_chunk_write_api->m_AssetEntries = (struct OpenFileEntry**)&concurrent_chunk_write_api[1];
    memset(concurrent_chunk_write_api->m_AssetEntries, 0, sizeof(struct OpenFileEntry*) * asset_count);
    for (uint32_t m = 0; m < *version_diff->m_ModifiedContentCount; m++)
    {
        uint32_t asset_index = version_diff->m_TargetContentModifiedAssetIndexes[m];
        int err = AllocateFileEntry(storageAPI, version_index, base_path, asset_index, &concurrent_chunk_write_api->m_AssetEntries[asset_index]);
        if (err != 0)
        {
            DisposeFileEntries(concurrent_chunk_write_api);
            return err;
        }
    }
    for (uint32_t a = 0; a < *version_diff->m_TargetAddedCount; a++)
    {
        uint32_t asset_index = version_diff->m_TargetAddedAssetIndexes[a];
        int err = AllocateFileEntry(storageAPI, version_index, base_path, asset_index, &concurrent_chunk_write_api->m_AssetEntries[asset_index]);
        if (err != 0)
        {
            DisposeFileEntries(concurrent_chunk_write_api);
            return err;
        }
    }
    concurrent_chunk_write_api->m_BasePath = Longtail_Strdup(base_path);
//    concurrent_chunk_write_api->m_MaxOpenFileEntryCount = 0;
    *out_concurrent_chunk_write_api = api;
    return 0;
}

struct Longtail_ConcurrentChunkWriteAPI* Longtail_CreateConcurrentChunkWriteAPI(
    struct Longtail_StorageAPI* storageAPI,
    struct Longtail_VersionIndex* version_index,
    struct Longtail_VersionDiff* version_diff,
    const char* base_path)
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, storageAPI != 0, return 0);

    size_t mem_size = sizeof(struct ConcurrentChunkWriteAPI) + sizeof(struct OpenFileEntry*) * (*version_index->m_AssetCount);
    void* mem = Longtail_Alloc("ConcurrentChunkWriteAPI", mem_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_ConcurrentChunkWriteAPI* out_api;
    int err = ConcurrentChunkWriteAPI_Init(mem, storageAPI, version_index, version_diff, base_path, &out_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Init() failed with %d", err)
        Longtail_Free(mem);
        return 0;
    }
    return out_api;
}
