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
    TLongtail_Atomic64 m_BytesLeftToWrite;
};

struct ConcurrentChunkWriteAPI
{
    struct Longtail_ConcurrentChunkWriteAPI m_ConcurrentChunkWriteAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    const struct Longtail_VersionIndex* m_VersionIndex;
//    uint32_t m_MaxOpenFileEntryCount;
    struct OpenFileEntry** m_AssetEntries;
};

static int OpenFileEntry(struct ConcurrentChunkWriteAPI* api, struct OpenFileEntry* open_file_entry)
{
    if (open_file_entry->m_FileHandle == 0)
    {
        if (open_file_entry->m_OpenCount == 0)
        {
            int err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, open_file_entry->m_FullPath, 0, &open_file_entry->m_FileHandle);
            if (err != 0)
            {
                return err;
            }
        }
        else
        {
            int err = api->m_StorageAPI->OpenAppendFile(api->m_StorageAPI, open_file_entry->m_FullPath, &open_file_entry->m_FileHandle);
            if (err != 0)
            {
                return err;
            }
        }

        open_file_entry->m_OpenCount++;
    }
    Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
    return 0;
}

static int ConcurrentChunkWriteAPI_Open(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    uint32_t asset_index,
    Longtail_ConcurrentChunkWriteAPI_HOpenFile* out_open_file)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u"),
        //        LONGTAIL_LOGFIELD(chunk_write_count, "%u"),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return EINVAL);
    struct OpenFileEntry* open_file_entry = api->m_AssetEntries[asset_index];

    Longtail_LockSpinLock(open_file_entry->m_SpinLock);
    if (open_file_entry->m_FileHandle != 0)
    {
        Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, 1);
        *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
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
        *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        return 0;
    }

    err = OpenFileEntry(api, open_file_entry);
    if (err)
    {
        Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenFileEntry() failed with %d", err)
        return err;
    }
    *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
    Longtail_UnlockSpinLock(open_file_entry->m_SpinLock);

    return 0;
}

static int ConcurrentChunkWriteAPI_Write(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    uint32_t asset_index,
    uint64_t offset,
    uint32_t size,
    const void* input)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%u"),
        LONGTAIL_LOGFIELD(input, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
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
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    Longtail_ConcurrentChunkWriteAPI_HOpenFile in_open_file)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return);
    LONGTAIL_VALIDATE_INPUT(ctx, in_open_file != 0, return);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    struct OpenFileEntry* open_file_entry = (struct OpenFileEntry*)in_open_file;
    int32_t OpenCount = Longtail_AtomicAdd32(&open_file_entry->m_ActiveOpenCount, -1);
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
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;

    for (ptrdiff_t i = 0; i < *api->m_VersionIndex->m_AssetCount; ++i)
    {
        struct OpenFileEntry* open_file_entry = api->m_AssetEntries[i];
        if (open_file_entry == 0)
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

static int ConcurrentChunkWriteAPI_CreateDir(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, uint32_t asset_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    LONGTAIL_VALIDATE_INPUT(ctx, asset_index < *api->m_VersionIndex->m_AssetCount, return EINVAL);

    struct OpenFileEntry* open_file_entry = api->m_AssetEntries[asset_index];

    if (api->m_StorageAPI->IsDir(api->m_StorageAPI, open_file_entry->m_FullPath))
    {
        return 0;
    }
    int err = EnsureParentPathExists(api->m_StorageAPI, open_file_entry->m_FullPath);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
        return err;
    }

    err = api->m_StorageAPI->CreateDir(api->m_StorageAPI, open_file_entry->m_FullPath);
    if (err == EEXIST)
    {
        err = 0;
    }
    return err;
}

static void ConcurrentChunkWriteAPI_Dispose(struct Longtail_API* concurrent_file_write_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, concurrent_file_write_api != 0, return);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;

//    api->m_MaxOpenFileEntryCount = 0;
    int err = ConcurrentChunkWriteAPI_Flush(&api->m_ConcurrentChunkWriteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Flush() failed with %d", err)
    }

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
        Longtail_DeleteSpinLock(open_file_entry->m_SpinLock);
        Longtail_Free((void*)open_file_entry);
    }
    Longtail_Free(concurrent_file_write_api);
}

static struct OpenFileEntry* AllocateFileEntry(struct Longtail_StorageAPI* storageAPI, struct Longtail_VersionIndex* version_index, const char* base_path, uint32_t asset_index)
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
    char* full_asset_path = storageAPI->ConcatPath(storageAPI, base_path, asset_path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "storageAPI->ConcatPath() failed with %d", ENOMEM)
        return 0;
    }
    int is_dir_path = asset_path[0] && asset_path[strlen(asset_path) - 1] == '/';
    if (is_dir_path)
    {
        full_asset_path[strlen(full_asset_path) - 1] = '\0';
    }
    size_t open_file_entry_size = sizeof(struct OpenFileEntry) + Longtail_GetSpinLockSize() + strlen(full_asset_path) + 1;
    void* new_open_file_entry_mem = Longtail_Alloc("AllocateFileEntry", open_file_entry_size);
    if (new_open_file_entry_mem == 0)
    {
        Longtail_Free(full_asset_path);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile() failed with %d", ENOMEM)
        return 0;
    }
    char* p = (char*)new_open_file_entry_mem;
    struct OpenFileEntry* new_open_file_entry = (struct OpenFileEntry*)p;
    p += sizeof(struct OpenFileEntry);
    int err = Longtail_CreateSpinLock(p, &new_open_file_entry->m_SpinLock);
    if (err)
    {
        Longtail_Free((void*)new_open_file_entry);
        Longtail_Free(full_asset_path);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile() failed with %d", err)
        return 0;
    }
    p += Longtail_GetSpinLockSize();
    new_open_file_entry->m_FileHandle = 0;
    new_open_file_entry->m_ActiveOpenCount = 0;
    new_open_file_entry->m_FullPath = p;
    new_open_file_entry->m_OpenCount = 0;
    new_open_file_entry->m_BytesLeftToWrite = version_index->m_AssetSizes[asset_index];
    strcpy(new_open_file_entry->m_FullPath, full_asset_path);

    Longtail_Free(full_asset_path);
    full_asset_path = 0;
    return new_open_file_entry;
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
        concurrent_chunk_write_api->m_AssetEntries[asset_index] = AllocateFileEntry(storageAPI, version_index, base_path, asset_index);
        if (concurrent_chunk_write_api->m_AssetEntries[asset_index] == 0)
        {
            // TODO: Clean up
            return ENOMEM;
        }
    }
    for (uint32_t a = 0; a < *version_diff->m_TargetAddedCount; a++)
    {
        uint32_t asset_index = version_diff->m_TargetAddedAssetIndexes[a];
        concurrent_chunk_write_api->m_AssetEntries[asset_index] = AllocateFileEntry(storageAPI, version_index, base_path, asset_index);
        if (concurrent_chunk_write_api->m_AssetEntries[asset_index] == 0)
        {
            // TODO: Clean up
            return ENOMEM;
        }
    }

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

    size_t mem_size = sizeof(struct ConcurrentChunkWriteAPI) + /*Longtail_GetRWLockSize() + */sizeof(struct OpenFileEntry*) * (*version_index->m_AssetCount);
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
