#include "longtail_concurrentchunkwrite.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"
#include <inttypes.h>
#include <errno.h>

#if defined(_MSC_VER)

#define FORCE_INLINE	__forceinline

#include <stdlib.h>

#define ROTL32(x,y)	_rotl(x,y)
#define ROTL64(x,y)	_rotl64(x,y)

#define BIG_CONSTANT(x) (x)

// Other compilers

#else	// defined(_MSC_VER)

#define	FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32(uint32_t x, int8_t r)
{
    return (x << r) | (x >> (32 - r));
}

inline uint64_t rotl64(uint64_t x, int8_t r)
{
    return (x << r) | (x >> (64 - r));
}

#define	ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)

#endif // !defined(_MSC_VER)

FORCE_INLINE uint64_t getblock64(const uint64_t* p, size_t i)
{
    return p[i];
}

FORCE_INLINE uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;

    return k;
}

uint64_t hashmurmur3_64(const void* key, size_t len) {
    const uint8_t* data = (const uint8_t*)key;
    const size_t nblocks = len / 16;

    uint64_t h1 = BIG_CONSTANT(0xf12c4da7f12c4da7);
    uint64_t h2 = BIG_CONSTANT(0x4da7f12c4da7f12c);

    const uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
    const uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

    //----------
    // body

    const uint64_t* blocks = (const uint64_t*)(data);

    for (size_t i = 0; i < nblocks; i++)
    {
        uint64_t k1 = getblock64(blocks, i * 2 + 0);
        uint64_t k2 = getblock64(blocks, i * 2 + 1);

        k1 *= c1; k1 = ROTL64(k1, 31); k1 *= c2; h1 ^= k1;

        h1 = ROTL64(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;

        k2 *= c2; k2 = ROTL64(k2, 33); k2 *= c1; h2 ^= k2;

        h2 = ROTL64(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
    }

    //----------
    // tail

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 16);

    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15)
    {
    case 15: k2 ^= ((uint64_t)tail[14]) << 48;
    case 14: k2 ^= ((uint64_t)tail[13]) << 40;
    case 13: k2 ^= ((uint64_t)tail[12]) << 32;
    case 12: k2 ^= ((uint64_t)tail[11]) << 24;
    case 11: k2 ^= ((uint64_t)tail[10]) << 16;
    case 10: k2 ^= ((uint64_t)tail[9]) << 8;
    case  9: k2 ^= ((uint64_t)tail[8]) << 0;
        k2 *= c2; k2 = ROTL64(k2, 33); k2 *= c1; h2 ^= k2;

    case  8: k1 ^= ((uint64_t)tail[7]) << 56;
    case  7: k1 ^= ((uint64_t)tail[6]) << 48;
    case  6: k1 ^= ((uint64_t)tail[5]) << 40;
    case  5: k1 ^= ((uint64_t)tail[4]) << 32;
    case  4: k1 ^= ((uint64_t)tail[3]) << 24;
    case  3: k1 ^= ((uint64_t)tail[2]) << 16;
    case  2: k1 ^= ((uint64_t)tail[1]) << 8;
    case  1: k1 ^= ((uint64_t)tail[0]) << 0;
        k1 *= c1; k1 = ROTL64(k1, 31); k1 *= c2; h1 ^= k1;
    };

    //----------
    // finalization

    h1 ^= len; h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return h2;
}

static uint64_t ConcurrentChunkWriteAPI_GetPathHash(const char* path)
{
    uint32_t pathlen = (uint32_t)strlen(path);
    return hashmurmur3_64((const void*)path, pathlen);
}

struct OpenFileEntry
{
    Longtail_StorageAPI_HOpenFile m_FileHandle;
    const char* m_FullPath;
    TLongtail_Atomic32 m_OpenCount;
};

struct PathLookup
{
    uint64_t key;       // path_hash
    ptrdiff_t value;    // index into ConcurrentChunkWriteAPI::m_OpenFileEntries
};

struct HandleLookup
{
    Longtail_StorageAPI_HOpenFile key;       // open_file_handle
    ptrdiff_t value;    // index into ConcurrentChunkWriteAPI::m_OpenFileEntries
};

struct ConcurrentChunkWriteAPI
{
    struct Longtail_ConcurrentChunkWriteAPI m_ConcurrentChunkWriteAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    HLongtail_RWLock m_RWLock;
    struct PathLookup* m_PathHashToOpenFile;
    struct OpenFileEntry** m_OpenFileEntries;
    char* m_BasePath;
};

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

    uint64_t path_hash = ConcurrentChunkWriteAPI_GetPathHash(path);

    {
        Longtail_LockRWLockRead(api->m_RWLock);
        ptrdiff_t tmp;
        intptr_t i = api->m_PathHashToOpenFile ? hmgeti_ts(api->m_PathHashToOpenFile, path_hash, tmp) : -1;
        if (i != -1)
        {
            intptr_t open_file_index = api->m_PathHashToOpenFile[i].value;
            struct OpenFileEntry* open_file_entry = api->m_OpenFileEntries[open_file_index];
            if (open_file_entry->m_FileHandle != 0)
            {
                LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle != 0, Longtail_UnlockRWLockRead(api->m_RWLock); return EINVAL);
                *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
                Longtail_AtomicAdd32(&open_file_entry->m_OpenCount, 1);
                Longtail_UnlockRWLockRead(api->m_RWLock);
                return 0;
            }
        }
        Longtail_UnlockRWLockRead(api->m_RWLock);
    }

    Longtail_LockRWLockWrite(api->m_RWLock);
    ptrdiff_t tmp;
    intptr_t i = api->m_PathHashToOpenFile ? hmgeti_ts(api->m_PathHashToOpenFile, path_hash, tmp) : -1;
    if (i == -1)
    {
        Longtail_UnlockRWLockWrite(api->m_RWLock);

        char* full_asset_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_BasePath, path);
        if (full_asset_path == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "ConcatPath() failed with %d", ENOMEM)
            return ENOMEM;
        }

        int err = EnsureParentPathExists(api->m_StorageAPI, full_asset_path);
        if (err != 0)
        {
            Longtail_Free(full_asset_path);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "EnsureParentPathExists() failed with %d", err)
            return err;
        }

        Longtail_LockRWLockWrite(api->m_RWLock);
        i = api->m_PathHashToOpenFile ? hmgeti_ts(api->m_PathHashToOpenFile, path_hash, tmp) : -1;
        if (i == -1)
        {
            struct OpenFileEntry* open_file_entry = (struct OpenFileEntry*)Longtail_Alloc("ConcurrentChunkWriteAPI_Open", sizeof(struct OpenFileEntry));
            if (open_file_entry == 0)
            {
                err = ENOMEM;
                Longtail_UnlockRWLockWrite(api->m_RWLock);
                Longtail_Free(full_asset_path);
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenWriteFile() failed with %d", err)
                return err;
            }
            open_file_entry->m_FileHandle = 0;
            open_file_entry->m_FullPath = full_asset_path;
            open_file_entry->m_OpenCount = 1;
            full_asset_path = 0;
            ptrdiff_t entry_index = arrlen(api->m_OpenFileEntries);
            arrput(api->m_OpenFileEntries, open_file_entry);
            hmput(api->m_PathHashToOpenFile, path_hash, entry_index);

            int err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, open_file_entry->m_FullPath, 0, &open_file_entry->m_FileHandle);
            if (err != 0)
            {
                Longtail_UnlockRWLockWrite(api->m_RWLock);
                Longtail_Free((void*)open_file_entry->m_FullPath);
                Longtail_Free((void*)open_file_entry);
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenAppendFile() failed with %d", err)
                return err;
            }
            *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
            Longtail_UnlockRWLockWrite(api->m_RWLock);
            return 0;
        }
        Longtail_Free(full_asset_path);
    }

    intptr_t open_file_index = api->m_PathHashToOpenFile[i].value;
    struct OpenFileEntry* open_file_entry = api->m_OpenFileEntries[open_file_index];
    if (open_file_entry->m_FileHandle != 0)
    {
        LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle != 0, Longtail_UnlockRWLockRead(api->m_RWLock); return EINVAL);
        *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
        Longtail_AtomicAdd32(&open_file_entry->m_OpenCount, 1);
        Longtail_UnlockRWLockWrite(api->m_RWLock);
        return 0;
    }
    int err = api->m_StorageAPI->OpenAppendFile(api->m_StorageAPI, open_file_entry->m_FullPath, &open_file_entry->m_FileHandle);
    if (err != 0)
    {
        Longtail_UnlockRWLockWrite(api->m_RWLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "OpenAppendFile() failed with %d", err)
        return err;
    }
    Longtail_AtomicAdd32(&open_file_entry->m_OpenCount, 1);
    *out_open_file = (Longtail_ConcurrentChunkWriteAPI_HOpenFile)open_file_entry;
    Longtail_UnlockRWLockWrite(api->m_RWLock);
    return 0;
}

static int ConcurrentChunkWriteAPI_Write(
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api,
    Longtail_ConcurrentChunkWriteAPI_HOpenFile in_open_file,
    uint64_t offset,
    uint32_t size,
    uint32_t chunk_count,
    const void* input,
    uint32_t* out_chunks_remaining)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p"),
        LONGTAIL_LOGFIELD(in_open_file, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(input, "%p"),
        LONGTAIL_LOGFIELD(out_chunks_remaining, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, concurrent_file_write_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, in_open_file != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;
    struct OpenFileEntry* open_file_entry = (struct OpenFileEntry*)in_open_file;
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_OpenCount > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FileHandle != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, open_file_entry->m_FullPath != 0, return EINVAL)

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

    {
        Longtail_LockRWLockRead(api->m_RWLock);
        int32_t OpenCount = Longtail_AtomicAdd32(&open_file_entry->m_OpenCount, -1);
        if (OpenCount > 0)
        {
            Longtail_UnlockRWLockRead(api->m_RWLock);
            return;
        }
        Longtail_UnlockRWLockRead(api->m_RWLock);
    }

    {
        Longtail_LockRWLockWrite(api->m_RWLock);
        if (open_file_entry->m_OpenCount == 0)
        {
            if (open_file_entry->m_FileHandle)
            {
                api->m_StorageAPI->CloseFile(api->m_StorageAPI, open_file_entry->m_FileHandle);
                open_file_entry->m_FileHandle = 0;
            }
        }
        Longtail_UnlockRWLockWrite(api->m_RWLock);
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
    Longtail_LockRWLockWrite(api->m_RWLock);
    ptrdiff_t entry_count = arrlen(api->m_OpenFileEntries);
    for (ptrdiff_t i = 0; i < entry_count; ++i)
    {
        struct OpenFileEntry* open_file_entry = api->m_OpenFileEntries[i];
        Longtail_StorageAPI_HOpenFile file_handle = open_file_entry->m_FileHandle;
        if (file_handle)
        {
            api->m_StorageAPI->CloseFile(api->m_StorageAPI, file_handle);
        }
        Longtail_Free((void*)open_file_entry->m_FullPath);
        Longtail_Free((void*)open_file_entry);
    }
    hmfree(api->m_PathHashToOpenFile);
    arrfree(api->m_OpenFileEntries);
    Longtail_UnlockRWLockWrite(api->m_RWLock);
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
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);

    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;

    char* full_asset_path = api->m_StorageAPI->ConcatPath(api->m_StorageAPI, api->m_BasePath, path);
    if (full_asset_path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "ConcatPath() failed with %d", ENOMEM)
        return ENOMEM;
    }
    full_asset_path[strlen(full_asset_path) - 1] = '\0';

    if (api->m_StorageAPI->IsDir(api->m_StorageAPI, full_asset_path))
    {
        Longtail_Free(full_asset_path);
        return 0;
    }
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

static void ConcurrentChunkWriteAPI_Dispose(struct Longtail_API* concurrent_file_write_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(concurrent_file_write_api, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, concurrent_file_write_api != 0, return);
    struct ConcurrentChunkWriteAPI* api = (struct ConcurrentChunkWriteAPI*)concurrent_file_write_api;

    int err = ConcurrentChunkWriteAPI_Flush(&api->m_ConcurrentChunkWriteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ConcurrentChunkWriteAPI_Flush() failed with %d", err)
    }
    Longtail_Free(api->m_BasePath);
    Longtail_DeleteRWLock(api->m_RWLock);
    Longtail_Free(concurrent_file_write_api);
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
        ConcurrentChunkWriteAPI_Close,
        ConcurrentChunkWriteAPI_Write,
        ConcurrentChunkWriteAPI_Flush);

    struct ConcurrentChunkWriteAPI* storage_api = (struct ConcurrentChunkWriteAPI*)api;
    storage_api->m_StorageAPI = storageAPI;
    Longtail_CreateRWLock(&storage_api[1], &storage_api->m_RWLock);
    storage_api->m_PathHashToOpenFile = 0;
    storage_api->m_OpenFileEntries = 0;
    storage_api->m_BasePath = Longtail_Strdup(base_path);

    *out_storage_api = api;
    return 0;
}

struct Longtail_ConcurrentChunkWriteAPI* Longtail_CreateConcurrentChunkWriteAPI(struct Longtail_StorageAPI* storageAPI, const char* base_path)
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, storageAPI != 0, return 0);

    size_t mem_size = sizeof(struct ConcurrentChunkWriteAPI) + Longtail_GetRWLockSize();
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
