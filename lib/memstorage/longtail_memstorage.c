#include "longtail_memstorage.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"

#include <errno.h>
#include <inttypes.h>
#include <ctype.h>

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

struct PathEntry
{
    char* m_FileName;
    uint32_t m_ParentHash;
    uint8_t* m_Content;
    uint16_t m_Permissions;
    uint8_t m_IsOpenWrite;
    uint32_t m_IsOpenRead;
};

struct Lookup
{
    uint32_t key;
    uint32_t value;
};

struct InMemStorageAPI
{
    struct Longtail_StorageAPI m_InMemStorageAPI;
    struct Lookup* m_PathHashToContent;
    struct PathEntry* m_PathEntries;
    HLongtail_SpinLock m_SpinLock;
};

static void InMemStorageAPI_Dispose(struct Longtail_API* storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct InMemStorageAPI* in_mem_storage_api = (struct InMemStorageAPI*)storage_api;
    size_t c = (size_t)arrlen(in_mem_storage_api->m_PathEntries);
    while(c--)
    {
        struct PathEntry* path_entry = &in_mem_storage_api->m_PathEntries[c];
        Longtail_Free(path_entry->m_FileName);
        path_entry->m_FileName = 0;
        arrfree(path_entry->m_Content);
        path_entry->m_Content = 0;
    }
    Longtail_DeleteSpinLock(in_mem_storage_api->m_SpinLock);
    hmfree(in_mem_storage_api->m_PathHashToContent);
    in_mem_storage_api->m_PathHashToContent = 0;
    arrfree(in_mem_storage_api->m_PathEntries);
    in_mem_storage_api->m_PathEntries = 0;
    Longtail_Free(storage_api);
}

static void InMemStorageAPI_ToLowerCase(char *str)
{
    for ( ; *str; ++str)
    {
        *str = tolower(*str);
    }
}

static const uint32_t Seed  = 0x811C9DC5;

static uint32_t InMemStorageAPI_GetPathHash(const char* path)
{
    uint32_t pathlen = (uint32_t)strlen(path);
    char* buf = (char*)alloca(pathlen + 1);
    memcpy(buf, path, pathlen + 1);
    InMemStorageAPI_ToLowerCase(buf);
    return murmur3_32((const uint8_t*)buf, pathlen, Seed);
}

static int InMemStorageAPI_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it != -1)
    {
        struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        if ((path_entry->m_Permissions & (Longtail_StorageAPI_OtherReadAccess | Longtail_StorageAPI_GroupReadAccess | Longtail_StorageAPI_UserReadAccess)) == 0)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "No permission to read, failed with %d", EACCES)
            return EACCES;
        }
        if (path_entry->m_IsOpenWrite)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "File already open for write, failed with %d", EPERM)
            return EPERM;
        }
        ++path_entry->m_IsOpenRead;
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        *out_open_file = (Longtail_StorageAPI_HOpenFile)(uintptr_t)path_hash;
        return 0;
    }
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File not found, failed with %d", ENOENT)
    return ENOENT;
}

static int InMemStorageAPI_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    Longtail_LockSpinLock(instance->m_SpinLock);
    uint32_t path_hash = (uint32_t)(uintptr_t)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1) {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "File not found, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = (struct PathEntry*)&instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    uint64_t size = (uint64_t)arrlen(path_entry->m_Content);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    *out_size = size;
    return 0;
}

static int InMemStorageAPI_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(length, "%" PRIu64),
        LONGTAIL_LOGFIELD(output, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, output != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    Longtail_LockSpinLock(instance->m_SpinLock);
    uint32_t path_hash = (uint32_t)(uintptr_t)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1) {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "File not found, failed with %d", EINVAL)
        return EINVAL;
    }
    struct PathEntry* path_entry = (struct PathEntry*)&instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    if ((ptrdiff_t)(offset + length) > arrlen(path_entry->m_Content))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Read out of bounds, failed with %d", EIO)
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        return EIO;
    }
    void* content_ptr = &path_entry->m_Content[offset];
    // A bit dangerous - we assume nobody is writing to the file while we are reading (which is unsupported here)
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    memcpy(output, content_ptr, length);
    return 0;
}

static uint32_t InMemStorageAPI_GetParentPathHash(const char* path)
{
    const char* dir_path_begin = strrchr(path, '/');
    if (!dir_path_begin)
    {
        return 0;
    }
    size_t dir_length = (uintptr_t)dir_path_begin - (uintptr_t)path;
    char* dir_path = (char*)alloca(dir_length + 1);
    strncpy(dir_path, path, dir_length);
    dir_path[dir_length] = '\0';
    uint32_t hash = InMemStorageAPI_GetPathHash(dir_path);
    return hash;
}

static const char* InMemStorageAPI_GetFileNamePart(const char* path)
{
    const char* file_name = strrchr(path, '/');
    if (file_name == 0)
    {
        return path;
    }
    return &file_name[1];
}

static int InMemStorageAPI_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(initial_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    uint32_t parent_path_hash = InMemStorageAPI_GetParentPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File not found, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = 0;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it != -1)
    {
        path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
        if ((path_entry->m_Permissions & (Longtail_StorageAPI_OtherWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_UserWriteAccess)) == 0)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "No permission to write, failed with %d", EACCES)
            return EACCES;
        }
        if (path_entry->m_IsOpenWrite)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File already open for write, failed with %d", EPERM)
            return EPERM;
        }

        if (path_entry->m_IsOpenRead)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File already open for read, failed with %d", EPERM)
            return EPERM;
        }
        path_entry->m_IsOpenWrite = 1;
    }
    else
    {
        ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
        arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
        path_entry = &instance->m_PathEntries[entry_index];
        path_entry->m_ParentHash = parent_path_hash;
        path_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(path));
        path_entry->m_Content = 0;
        path_entry->m_Permissions = 0644;
        path_entry->m_IsOpenRead = 0;
        path_entry->m_IsOpenWrite = 1;
        hmput(instance->m_PathHashToContent, path_hash, (uint32_t)entry_index);
    }
    arrsetcap(path_entry->m_Content, initial_size == 0 ? 16 : (uint32_t)initial_size);
    arrsetlen(path_entry->m_Content, (uint32_t)initial_size);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    *out_open_file = (Longtail_StorageAPI_HOpenFile)(uintptr_t)path_hash;
    return 0;
}

static int InMemStorageAPI_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(length, "%" PRIu64),
        LONGTAIL_LOGFIELD(input, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    Longtail_LockSpinLock(instance->m_SpinLock);
    uint32_t path_hash = (uint32_t)(uintptr_t)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "File not found, failed with %d", EINVAL)
        return EINVAL;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    ptrdiff_t size = arrlen(path_entry->m_Content);
    if ((ptrdiff_t)offset > size)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Write out of bounds, failed with %d", EIO)
        return EIO;
    }
    if ((ptrdiff_t)(offset + length) > size)
    {
        size = offset + length;
    }
    arrsetcap(path_entry->m_Content, size == 0 ? 16 : (uint32_t)size);
    arrsetlen(path_entry->m_Content, (uint32_t)size);
    memcpy(&(path_entry->m_Content)[offset], input, length);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(length, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    Longtail_LockSpinLock(instance->m_SpinLock);
    uint32_t path_hash = (uint32_t)(uintptr_t)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "File not found, failed with %d", EINVAL)
        return EINVAL;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    arrsetlen(path_entry->m_Content, (uint32_t)length);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(permissions, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "file not found, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    path_entry->m_Permissions = permissions;
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_GetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t* out_permissions)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_permissions, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File not found, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    *out_permissions = path_entry->m_Permissions;
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static void InMemStorageAPI_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    Longtail_LockSpinLock(instance->m_SpinLock);
    uint32_t path_hash = (uint32_t)(uintptr_t)f;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Invalid file handle, failed with %d", EINVAL)
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    if (path_entry->m_IsOpenRead > 0)
    {
        LONGTAIL_FATAL_ASSERT(ctx, path_entry->m_IsOpenWrite == 0, return);
        --path_entry->m_IsOpenRead;
    }
    else
    {
        LONGTAIL_FATAL_ASSERT(ctx, path_entry->m_IsOpenRead == 0, return);
        LONGTAIL_FATAL_ASSERT(ctx, path_entry->m_IsOpenWrite == 1, return);
        path_entry->m_IsOpenWrite = 0;
    }

    Longtail_UnlockSpinLock(instance->m_SpinLock);
}

static int InMemStorageAPI_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t parent_path_hash = InMemStorageAPI_GetParentPathHash(path);
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Invalid path, failed with %d", EINVAL)
        return EINVAL;
    }
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr != -1)
    {
        struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
        if (path_entry->m_IsOpenRead || path_entry->m_IsOpenWrite)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Directory already open as a file, failed with %d", EIO)
            return EIO;
        }
        if ((path_entry->m_Permissions & (Longtail_StorageAPI_OtherWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_UserWriteAccess)) == 0)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "No permission to create dir, failed with %d", EACCES)
            return EACCES;
        }
        if (path_entry->m_Content == 0)
        {
            Longtail_UnlockSpinLock(instance->m_SpinLock);
            return EEXIST;
        }
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Directory already exists as a file, failed with %d", EIO)
        return EIO;
    }

    ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
    arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
    struct PathEntry* path_entry = &instance->m_PathEntries[entry_index];
    path_entry->m_ParentHash = parent_path_hash;
    path_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(path));
    path_entry->m_Content = 0;
    path_entry->m_Permissions = 0775;
    path_entry->m_IsOpenRead = 0;
    path_entry->m_IsOpenWrite = 0;
    hmput(instance->m_PathHashToContent, path_hash, (uint32_t)entry_index);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, source_path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, target_path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t source_path_hash = InMemStorageAPI_GetPathHash(source_path);
    uint32_t target_path_hash = InMemStorageAPI_GetPathHash(target_path);
    uint32_t target_parent_path_hash = InMemStorageAPI_GetParentPathHash(target_path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
    if (source_path_ptr == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File not found failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* source_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];

    intptr_t target_path_ptr = hmgeti(instance->m_PathHashToContent, target_path_hash);
    if (target_path_ptr != -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File already exists, failed with %d", EEXIST)
        return EEXIST;
    }
    source_entry->m_ParentHash = target_parent_path_hash;
    Longtail_Free(source_entry->m_FileName);
    source_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(target_path));
    hmput(instance->m_PathHashToContent, target_path_hash, instance->m_PathHashToContent[source_path_ptr].value);
    hmdel(instance->m_PathHashToContent, source_path_hash);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static char* InMemStorageAPI_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(sub_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, root_path != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return 0);
    if (root_path[0] == 0)
    {
        return Longtail_Strdup(sub_path);
    }
    size_t path_len = strlen(root_path) + 1 + strlen(sub_path) + 1;
    char* path = (char*)Longtail_Alloc("InMemStorageAPI", path_len);
    if (path == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    strcpy(path, root_path);
    strcat(path, "/");
    strcat(path, sub_path);
    return path;
}

static int InMemStorageAPI_IsDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t source_path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, source_path_hash);
    if (source_path_ptr == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    int is_dir = path_entry->m_Content == 0;
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return is_dir;
}
static int InMemStorageAPI_IsFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        return 0;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    int is_file = path_entry->m_Content != 0;
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return is_file;
}

static int InMemStorageAPI_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Directory does not exist, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    if (path_entry->m_Content)
    {
        // Not a directory
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Path is not a directory, failed with %d", EINVAL)
        return EINVAL;
    }
    Longtail_Free(path_entry->m_FileName);
    path_entry->m_FileName = 0;
    arrfree(path_entry->m_Content);
    path_entry->m_Content = 0;
    path_entry->m_ParentHash = 0;
    hmdel(instance->m_PathHashToContent, path_hash);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t source_path_ptr = hmgeti(instance->m_PathHashToContent, path_hash);
    if (source_path_ptr == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File does not exist, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[source_path_ptr].value];
    if (!path_entry->m_Content)
    {
        // Not a file
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Path is not a file, failed with %d", EINVAL)
        return EINVAL;
    }
    if (path_entry->m_IsOpenRead || path_entry->m_IsOpenWrite)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "File is open, failed with %d", EPERM)
        return EPERM;
    }
    Longtail_Free(path_entry->m_FileName);
    path_entry->m_FileName = 0;
    arrfree(path_entry->m_Content);
    path_entry->m_Content = 0;
    path_entry->m_ParentHash = 0;
    hmdel(instance->m_PathHashToContent, path_hash);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static int InMemStorageAPI_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_iterator != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = path[0] ? InMemStorageAPI_GetPathHash(path) : 0;
    Longtail_LockSpinLock(instance->m_SpinLock);
    ptrdiff_t* i = (ptrdiff_t*)Longtail_Alloc("InMemStorageAPI", sizeof(ptrdiff_t));
    if (!i)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    *i = 0;
    while (*i < arrlen(instance->m_PathEntries))
    {
        if (instance->m_PathEntries[*i].m_ParentHash == path_hash)
        {
            *out_iterator = (Longtail_StorageAPI_HIterator)i;
            return 0;
        }
        *i += 1;
    }
    Longtail_Free(i);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return ENOENT;
}

static int InMemStorageAPI_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    ptrdiff_t* i = (ptrdiff_t*)iterator;
    uint32_t path_hash = instance->m_PathEntries[*i].m_ParentHash;
    *i += 1;
    while (*i < arrlen(instance->m_PathEntries))
    {
        if (instance->m_PathEntries[*i].m_ParentHash == path_hash)
        {
            return 0;
        }
        *i += 1;
    }
    return ENOENT;
}
static void InMemStorageAPI_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    ptrdiff_t* i = (ptrdiff_t*)iterator;
    Longtail_Free(i);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
}

static const char* InMemStorageAPI_GetFileName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return 0);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    ptrdiff_t* i = (ptrdiff_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content == 0)
    {
        return 0;
    }
    const char* file_name = instance->m_PathEntries[*i].m_FileName;
    return file_name;
}

static const char* InMemStorageAPI_GetDirectoryName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return 0);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t* i = (uint32_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content != 0)
    {
        return 0;
    }
    return instance->m_PathEntries[*i].m_FileName;
}

static int InMemStorageAPI_GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, struct Longtail_StorageAPI_EntryProperties* out_properties)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p"),
        LONGTAIL_LOGFIELD(out_properties, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_properties != 0, return EINVAL);
    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t* i = (uint32_t*)iterator;
    if (instance->m_PathEntries[*i].m_Content == 0)
    {
        out_properties->m_Size = 0;
        out_properties->m_IsDir = 1;
    }
    else
    {
        out_properties->m_Size = (uint64_t)arrlen(instance->m_PathEntries[*i].m_Content);
        out_properties->m_IsDir = 0;
    }
    out_properties->m_Permissions = instance->m_PathEntries[*i].m_Permissions;
    out_properties->m_Name = instance->m_PathEntries[*i].m_FileName;
    return 0;
}

static int InMemStorageAPI_LockFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HLockFile* out_lock_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_lock_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = InMemStorageAPI_GetPathHash(path);
    uint32_t parent_path_hash = InMemStorageAPI_GetParentPathHash(path);
    Longtail_LockSpinLock(instance->m_SpinLock);
    if (parent_path_hash != 0 && hmgeti(instance->m_PathHashToContent, parent_path_hash) == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Parent directory does not exist, failed with %d", ENOENT)
        return ENOENT;
    }
    struct PathEntry* path_entry = 0;
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);

    int try_count = 50;
    uint64_t retry_delay = 1000;
    uint64_t total_delay = 0;

    while (it != -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        if (--try_count == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Failed with %d, waited %f seconds",
                EACCES,
                (total_delay / 1000) / 1000.f)
            return EACCES;
        }
        Longtail_Sleep(retry_delay);
        total_delay += retry_delay;

        Longtail_LockSpinLock(instance->m_SpinLock);
        it = hmgeti(instance->m_PathHashToContent, path_hash);
        retry_delay += 2000;
    }
    ptrdiff_t entry_index = arrlen(instance->m_PathEntries);
    arrsetlen(instance->m_PathEntries, (size_t)(entry_index + 1));
    path_entry = &instance->m_PathEntries[entry_index];
    path_entry->m_ParentHash = parent_path_hash;
    path_entry->m_FileName = Longtail_Strdup(InMemStorageAPI_GetFileNamePart(path));
    path_entry->m_Content = 0;
    path_entry->m_Permissions = 0644;
    path_entry->m_IsOpenRead = 0;
    path_entry->m_IsOpenWrite = 2;
    hmput(instance->m_PathHashToContent, path_hash, (uint32_t)entry_index);

    arrsetcap(path_entry->m_Content, 16);
    Longtail_UnlockSpinLock(instance->m_SpinLock);
    *out_lock_file = (Longtail_StorageAPI_HLockFile)(uintptr_t)path_hash;
    return 0;
}

static int InMemStorageAPI_UnlockFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HLockFile lock_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(lock_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct InMemStorageAPI* instance = (struct InMemStorageAPI*)storage_api;
    uint32_t path_hash = (uint32_t)(uintptr_t)lock_file;
    Longtail_LockSpinLock(instance->m_SpinLock);
    intptr_t it = hmgeti(instance->m_PathHashToContent, path_hash);
    if (it == -1)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Invalid file handle, failed with %d", EINVAL)
    }
    struct PathEntry* path_entry = &instance->m_PathEntries[instance->m_PathHashToContent[it].value];
    if (path_entry->m_IsOpenRead > 0)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Invalid file handle, failed with %d", EINVAL)
        return EINVAL;
    }
    if (path_entry->m_IsOpenWrite != 2)
    {
        Longtail_UnlockSpinLock(instance->m_SpinLock);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Invalid file handle, failed with %d", EINVAL)
        return EINVAL;
    }
    Longtail_Free(path_entry->m_FileName);
    path_entry->m_FileName = 0;
    arrfree(path_entry->m_Content);
    path_entry->m_Content = 0;
    path_entry->m_ParentHash = 0;
    hmdel(instance->m_PathHashToContent, path_hash);

    Longtail_UnlockSpinLock(instance->m_SpinLock);
    return 0;
}

static char* InMemStorageAPI_GetParentPath(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)

    size_t delim_pos = 0;
    size_t path_len = 0;
    while (path[path_len] != 0)
    {
        if (path[path_len] == '/')
        {
            delim_pos = path_len;
        }
        ++path_len;
    }
    if (path[delim_pos] != '/' || delim_pos == 0)
    {
        return 0;
    }

    char* result = (char*)Longtail_Alloc("InMemStorageAPI_GetParentPath", delim_pos + 1);
    if (!result)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    result[delim_pos] = 0;
    while (delim_pos--)
    {
        result[delim_pos] = path[delim_pos];
    }
    return result;
}

static int InMemStorageAPI_Init(
    void* mem,
    struct Longtail_StorageAPI** out_storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_storage_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0);
    struct Longtail_StorageAPI* api = Longtail_MakeStorageAPI(
        mem,
        InMemStorageAPI_Dispose,
        InMemStorageAPI_OpenReadFile,
        InMemStorageAPI_GetSize,
        InMemStorageAPI_Read,
        InMemStorageAPI_OpenWriteFile,
        InMemStorageAPI_Write,
        InMemStorageAPI_SetSize,
        InMemStorageAPI_SetPermissions,
        InMemStorageAPI_GetPermissions,
        InMemStorageAPI_CloseFile,
        InMemStorageAPI_CreateDir,
        InMemStorageAPI_RenameFile,
        InMemStorageAPI_ConcatPath,
        InMemStorageAPI_IsDir,
        InMemStorageAPI_IsFile,
        InMemStorageAPI_RemoveDir,
        InMemStorageAPI_RemoveFile,
        InMemStorageAPI_StartFind,
        InMemStorageAPI_FindNext,
        InMemStorageAPI_CloseFind,
        InMemStorageAPI_GetEntryProperties,
        InMemStorageAPI_LockFile,
        InMemStorageAPI_UnlockFile,
        InMemStorageAPI_GetParentPath);

    struct InMemStorageAPI* storage_api = (struct InMemStorageAPI*)api;

    storage_api->m_PathHashToContent = 0;
    storage_api->m_PathEntries = 0;
    int err = Longtail_CreateSpinLock(&storage_api[1], &storage_api->m_SpinLock);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateSpinLock() failed with %d", err)
        return err;
    }
    *out_storage_api = api;
    return 0;
}

struct Longtail_StorageAPI* Longtail_CreateInMemStorageAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    void* mem = (struct InMemStorageAPI*)Longtail_Alloc("InMemStorageAPI", sizeof(struct InMemStorageAPI) + Longtail_GetSpinLockSize());
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_StorageAPI* storage_api;
    int err = InMemStorageAPI_Init(mem, &storage_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InMemStorageAPI_Init() failed with %d", err)
        Longtail_Free(storage_api);
        return 0;
    }
    return storage_api;
}
