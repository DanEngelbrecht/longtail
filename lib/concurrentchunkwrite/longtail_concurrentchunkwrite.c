#include "longtail_concurrentchunkwrite.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"
#include <inttypes.h>
#include <errno.h>

#if !defined(alloca)
    #if defined(__GLIBC__) || defined(__sun) || defined(__CYGWIN__)
        #include <alloca.h>     // alloca
    #elif defined(_WIN32)
        #include <malloc.h>     // alloca
        #if !defined(alloca)
            #define alloca _alloca  // for clang with MS Codegen
        #endif
        #define CompareIgnoreCase _stricmp
    #else
        #include <stdlib.h>     // alloca
    #endif
#endif

#include <string.h>

static inline uint32_t murmur_32_scramble(uint32_t k) {
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    return k;
}

static uint32_t murmur3_32(const uint8_t* key, size_t len, uint32_t seed)
{
    uint32_t h = seed;
    uint32_t k;
    /* Read in groups of 4. */
    for (size_t i = len >> 2; i; i--) {
        // Here is a source of differing results across endiannesses.
        // A swap here has no effects on hash properties though.
        memcpy(&k, key, sizeof(uint32_t));
        key += sizeof(uint32_t);
        h ^= murmur_32_scramble(k);
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }
    /* Read the rest. */
    k = 0;
    for (size_t i = len & 3; i; i--) {
        k <<= 8;
        k |= key[i - 1];
    }
    // A swap is *not* necessary here because the preceding loop already
    // places the low bytes in the low places according to whatever endianness
    // we use. Swaps only apply when the memory is copied in a chunk.
    h ^= murmur_32_scramble(k);
    /* Finalize. */
    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

static const uint32_t Seed = 0xF12C4DA7;

static uint32_t ConcurrentChunkWriteAPI_GetPathHash(const char* path)
{
    uint32_t pathlen = (uint32_t)strlen(path);
    return murmur3_32((const uint8_t*)path, pathlen, Seed);
}

// TODO: This caching does not work - if a file is closed in between writes it will truncate on next open :(

struct OpenFileEntry
{
    uint32_t m_TotalWriteCount;
    uint32_t m_PendingWriteCount;
    Longtail_StorageAPI_HOpenFile m_FileHandle;
};

struct Lookup
{
    uint32_t key;
    struct OpenFileEntry value;
};

struct ConcurrentChunkWriteAPI
{
    struct Longtail_ConcurrentChunkWriteAPI m_ConcurrentChunkWriteAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    HLongtail_Mutex m_Mutex;
    struct Lookup* m_PathHashToOpenFile;
    char* m_BasePath;
};

static void ConcurrentChunkWriteAPI_Dispose(struct Longtail_API* concurrent_file_write_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, concurrent_file_write_api != 0, return);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    // TODO: Flush explicit?
    Longtail_Free(api->m_BasePath);
    hmfree(api->m_PathHashToOpenFile);
    Longtail_DeleteMutex(api->m_Mutex);
    Longtail_Free(concurrent_file_write_api);
}

static int ConcurrentChunkWriteAPI_Open(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    const char* path,
    uint32_t chunk_write_count,
    Longtail_ConcurrentChunkWriteAPI_HOpenFile* out_open_file)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(chunk_write_count, "%u"),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    uint32_t path_hash = ConcurrentChunkWriteAPI_GetPathHash(path);

    Longtail_LockMutex(api->m_Mutex);
    intptr_t i = hmgeti(api->m_PathHashToOpenFile, path_hash);
    if (i != -1)
    {
        struct OpenFileEntry* open_file_entry = &api->m_PathHashToOpenFile[i].value;
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_TotalWriteCount == chunk_write_count, Longtail_UnlockMutex(api->m_Mutex); return EINVAL);
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_PendingWriteCount > 0, Longtail_UnlockMutex(api->m_Mutex); return EINVAL);
        *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)(uintptr_t)path_hash;
//        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "reopened: `%s` (0x%08x) TotalWriteChunks: %d, PendingWriteCount: %d", path, path_hash, open_file_entry->m_TotalWriteCount, open_file_entry->m_PendingWriteCount);
        Longtail_UnlockMutex(api->m_Mutex);
        return 0;
    }
    char* full_asset_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_BasePath, path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "ConcatPath() failed with %d", ENOMEM)
        return ENOMEM;
    }

    int err = EnsureParentPathExists(api->m_StorageAPI, full_asset_path);
    if (err != 0)
    {
        Longtail_UnlockMutex(api->m_Mutex);
        Longtail_Free(full_asset_path);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile r;
    err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, full_asset_path, 0, &r);
    if (err != 0)
    {
        Longtail_UnlockMutex(api->m_Mutex);
        Longtail_Free(full_asset_path);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile() failed with %d", err)
        return err;
    }
    if (chunk_write_count == 0)
    {
        // Empty file, close immediately
        api->m_StorageAPI->CloseFile(api->m_StorageAPI, r);
        Longtail_UnlockMutex(api->m_Mutex);
        Longtail_Free(full_asset_path);
        *out_open_file = 0;
        return 0;
    }
    struct OpenFileEntry entry;
    entry.m_FileHandle = r;
    entry.m_TotalWriteCount = chunk_write_count;
    entry.m_PendingWriteCount = chunk_write_count;
    hmput(api->m_PathHashToOpenFile, path_hash, entry);
//    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "opened: `%s` (0x%08x) TotalWriteChunks: %d, PendingWriteCount: %d", path, path_hash, chunk_write_count, chunk_write_count);
    Longtail_UnlockMutex(api->m_Mutex);
    Longtail_Free(full_asset_path);
    *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)(uintptr_t)path_hash;
    return 0;
}

static int ConcurrentChunkWriteAPI_Write(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    Longtail_ConcurrentChunkWriteAPI_HOpenFile in_open_file,
    uint64_t offset,
    uint32_t size,
    const void* input)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(in_open_file, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%u"),
        LONGTAIL_LOGFIELD(input, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, (uintptr_t)in_open_file <= 0xffffffffu, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    uint32_t path_hash = (uint32_t)(uintptr_t)in_open_file;

    Longtail_LockMutex(api->m_Mutex);

    intptr_t i = hmgeti(api->m_PathHashToOpenFile, path_hash);
    if (i == -1)
    {
        Longtail_UnlockMutex(api->m_Mutex);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Write() (0x%08x) file not open, error %d", path_hash, EINVAL)
        return EINVAL;
    }
    struct OpenFileEntry* open_file_entry = &api->m_PathHashToOpenFile[i].value;
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_PendingWriteCount > 0, Longtail_UnlockMutex(api->m_Mutex); return EINVAL);
//    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "write: (0x%08x) TotalWriteChunks: %d, PendingWriteCount: %d", path_hash, open_file_entry->m_TotalWriteCount, open_file_entry->m_PendingWriteCount);
    --open_file_entry->m_PendingWriteCount;
    int close_on_completion = open_file_entry->m_PendingWriteCount == 0;
    Longtail_StorageAPI_HOpenFile file_handle = open_file_entry->m_FileHandle;
    if (close_on_completion)
    {
//        hmdel(api->m_PathHashToOpenFile, path_hash);
    }
    Longtail_UnlockMutex(api->m_Mutex);

    int err = api->m_StorageAPI->Write(api->m_StorageAPI, file_handle, offset, size, input);

    if (close_on_completion)
    {
        api->m_StorageAPI->CloseFile(api->m_StorageAPI, file_handle);
//        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "closed (0x%08x)", path_hash);
    }
    return err;
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
    Longtail_LockMutex(api->m_Mutex);
    ptrdiff_t entry_count = hmlen(api->m_PathHashToOpenFile);
    for (ptrdiff_t i = 0; i < entry_count; ++i)
    {
        if (api->m_PathHashToOpenFile[i].value.m_PendingWriteCount > 0)
        {
            Longtail_StorageAPI_HOpenFile file_handle = api->m_PathHashToOpenFile[i].value.m_FileHandle;
            api->m_StorageAPI->CloseFile(api->m_StorageAPI, file_handle);
        }
    }
    hmfree(api->m_PathHashToOpenFile);
    Longtail_UnlockMutex(api->m_Mutex);
    return 0;
}

static int ConcurrentChunkWriteAPI_CreateDir(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;

    char* full_asset_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_BasePath, path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "ConcatPath() failed with %d", ENOMEM)
        return ENOMEM;
    }
    full_asset_path[strlen(full_asset_path) - 1] = '\0';

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    int err = api->m_StorageAPI->CreateDir(api->m_StorageAPI, full_asset_path);
    if (err == EEXIST)
    {
        err = 0;
    }
    Longtail_Free(full_asset_path);
    return err;
}

static int ConcurrentChunkWriteAPI_Init(
    void* mem,
    struct Longtail_StorageAPI* storageAPI,
    const char* base_path,
    struct Longtail_ConcurrentChunkWriteAPI** out_storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_storage_api, "%p"),
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
        ConcurrentChunkWriteAPI_Write,
        ConcurrentChunkWriteAPI_Flush);

    struct ConcurrentChunkWriteAPI* storage_api = (struct ConcurrentChunkWriteAPI*)api;
    storage_api->m_StorageAPI = storageAPI;
    Longtail_CreateMutex(&storage_api[1], &storage_api->m_Mutex);
    storage_api->m_PathHashToOpenFile = 0;
    storage_api->m_BasePath = Longtail_Strdup(base_path);

    *out_storage_api = api;
    return 0;
}

struct Longtail_ConcurrentChunkWriteAPI* Longtail_CreateConcurrentChunkWriteAPI(struct Longtail_StorageAPI* storageAPI, const char* base_path)
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, storageAPI != 0, return 0);

    size_t mem_size = sizeof(struct ConcurrentChunkWriteAPI) + Longtail_GetMutexSize();
    void* mem = Longtail_Alloc("ConcurrentChunkWriteAPI", mem_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_ConcurrentChunkWriteAPI* out_api;
    int err = ConcurrentChunkWriteAPI_Init(mem, storageAPI, base_path, &out_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Init() failed with %d", err)
        return 0;
    }
    return out_api;
}
