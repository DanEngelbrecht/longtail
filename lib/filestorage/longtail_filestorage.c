#include "longtail_filestorage.h"

#include "../longtail_platform.h"
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

struct FSStorageAPI
{
    struct Longtail_StorageAPI m_FSStorageAPI;
};

static void FSStorageAPI_Dispose(struct Longtail_API* storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return);
    Longtail_Free(storage_api);
}

static int FSStorageAPI_OpenReadFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    Longtail_StorageAPI_HOpenFile* out_open_file)
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

    HLongtail_OpenFile r;
    int err = Longtail_OpenReadFile(path, &r);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_OpenReadFile() failed with %d", err)
        return err;
    }
    *out_open_file = (Longtail_StorageAPI_HOpenFile)r;
    return 0;
}

static int FSStorageAPI_GetSize(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t* out_size)
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
    int err = Longtail_GetFileSize((HLongtail_OpenFile)f, out_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetFileSize() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_Read(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t offset,
    uint64_t length,
    void* output)
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
    int err = Longtail_Read((HLongtail_OpenFile)f, offset,length, output);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Read() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_OpenWriteFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint64_t initial_size,
    Longtail_StorageAPI_HOpenFile* out_open_file)
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
    HLongtail_OpenFile r;
    int err = Longtail_OpenWriteFile(path, initial_size, &r);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_OpenWriteFile() failed with %d", err)
        return err;
    }
    *out_open_file = (Longtail_StorageAPI_HOpenFile)r;
    return 0;
}

static int FSStorageAPI_Write(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t offset,
    uint64_t length,
    const void* input)
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
    int err = Longtail_Write((HLongtail_OpenFile)f, offset,length, input);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Write() failed with %d", err)
    }
    return err;
}

static int FSStorageAPI_SetSize(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t length)
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
    int err = Longtail_SetFileSize((HLongtail_OpenFile)f, length);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_SetFileSize() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_SetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t permissions)
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
    int err = Longtail_SetFilePermissions(path, permissions);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_SetFilePermissions() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_GetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t* out_permissions)
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
    int err = Longtail_GetFilePermissions(path, out_permissions);
    if (err == ENOENT)
    {
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetFilePermissions() failed with %d", err)
        return err;
    }
    return 0;
}

static void FSStorageAPI_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return);
    Longtail_CloseFile((HLongtail_OpenFile)f);
}

static int FSStorageAPI_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
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
    int err = Longtail_CreateDirectory(path);
    if (err && err != EEXIST)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateDirectory() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path)
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
    int err = Longtail_MoveFile(source_path, target_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_MoveFile() failed with %d", err)
        return err;
    }
    return 0;
}

static char* FSStorageAPI_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path)
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
    char* path = Longtail_ConcatPath(root_path, sub_path);
    if (!path)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ConcatPath() failed with %d", ENOMEM)
        return 0;
    }
    return path;
}

static int FSStorageAPI_IsDir(struct Longtail_StorageAPI* storage_api, const char* path)
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
    int is_dir = Longtail_IsDir(path);
    return is_dir;
}

static int FSStorageAPI_IsFile(struct Longtail_StorageAPI* storage_api, const char* path)
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
    int is_file = Longtail_IsFile(path);
    return is_file;
}

static int FSStorageAPI_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path)
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
    int err = Longtail_RemoveDir(path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_RemoveDir() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path)
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
    int err = Longtail_RemoveFile(path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_RemoveFile() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, out_iterator != 0, return EINVAL);
    Longtail_StorageAPI_HIterator iterator = (Longtail_StorageAPI_HIterator)Longtail_Alloc("FSStorageAPI", Longtail_GetFSIteratorSize());
    int err = Longtail_StartFind((HLongtail_FSIterator)iterator, path);
    if (err == ENOENT)
    {
        Longtail_Free(iterator);
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_StartFind() failed with %d", err)
        Longtail_Free(iterator);
        return err;
    }
    *out_iterator = iterator;
    return 0;
}

static int FSStorageAPI_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return EINVAL);
    int err = Longtail_FindNext((HLongtail_FSIterator)iterator);
    if (err && (err != ENOENT))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_FindNext() failed with %d", err)
    }
    return err;
}

static void FSStorageAPI_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return );
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return );
    Longtail_CloseFind((HLongtail_FSIterator)iterator);
    Longtail_Free(iterator);
}

static int FSStorageAPI_GetEntryProperties(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator,
    struct Longtail_StorageAPI_EntryProperties* out_properties)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s"),
        LONGTAIL_LOGFIELD(out_properties, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, iterator != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, out_properties != 0, return EINVAL);
    int err = Longtail_GetEntryProperties((HLongtail_FSIterator)iterator, &out_properties->m_Size, &out_properties->m_Permissions, &out_properties->m_IsDir);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetEntryProperties() failed with %d", err)
        return err;
    }
    out_properties->m_Name = (out_properties->m_IsDir) ? Longtail_GetDirectoryName((HLongtail_FSIterator)iterator) : Longtail_GetFileName((HLongtail_FSIterator)iterator);
    return 0;
}

static int FSStorageAPI_LockFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HLockFile* out_lock_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_lock_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, path != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, out_lock_file != 0, return EINVAL);

    void* mem = Longtail_Alloc("FSStorageAPI", Longtail_GetFileLockSize());
    if (!mem)
    {
        return ENOMEM;
    }
    HLongtail_FileLock file_lock;
    int err = Longtail_LockFile(mem, path, &file_lock);
    if (err)
    {
        Longtail_Free(mem);
        return err;
    }
    *out_lock_file = (Longtail_StorageAPI_HLockFile)file_lock;
    return 0;
}

static int FSStorageAPI_UnlockFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HLockFile lock_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(lock_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, lock_file != 0, return EINVAL);

    int err = Longtail_UnlockFile((HLongtail_FileLock)lock_file);
    if (err)
    {
        return err;
    }
    Longtail_Free(lock_file);
    return 0;
}

static char* FSStorageAPI_GetParentPath(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)

    return Longtail_GetParentPath(path);
}

static int FSStorageAPI_MapFile(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t offset,
    uint64_t length,
    Longtail_StorageAPI_HFileMap* out_file_map,
    const void** out_data_ptr)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(length, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_file_map, "%p"),
        LONGTAIL_LOGFIELD(out_data_ptr, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, length > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_file_map !=0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_data_ptr !=0, return EINVAL)

    return Longtail_MapFile((HLongtail_OpenFile)f, offset, length, (HLongtail_FileMap*)out_file_map, out_data_ptr);
}

static void FSStorageAPI_UnmapFile(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HFileMap m)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(m, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return)
    LONGTAIL_VALIDATE_INPUT(ctx, m != 0, return)

    Longtail_UnmapFile((HLongtail_FileMap)m);
}

static int FSStorageAPI_Init(
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
        FSStorageAPI_Dispose,
        FSStorageAPI_OpenReadFile,
        FSStorageAPI_GetSize,
        FSStorageAPI_Read,
        FSStorageAPI_OpenWriteFile,
        FSStorageAPI_Write,
        FSStorageAPI_SetSize,
        FSStorageAPI_SetPermissions,
        FSStorageAPI_GetPermissions,
        FSStorageAPI_CloseFile,
        FSStorageAPI_CreateDir,
        FSStorageAPI_RenameFile,
        FSStorageAPI_ConcatPath,
        FSStorageAPI_IsDir,
        FSStorageAPI_IsFile,
        FSStorageAPI_RemoveDir,
        FSStorageAPI_RemoveFile,
        FSStorageAPI_StartFind,
        FSStorageAPI_FindNext,
        FSStorageAPI_CloseFind,
        FSStorageAPI_GetEntryProperties,
        FSStorageAPI_LockFile,
        FSStorageAPI_UnlockFile,
        FSStorageAPI_GetParentPath,
        FSStorageAPI_MapFile,
        FSStorageAPI_UnmapFile);
    *out_storage_api = api;
    return 0;
}


struct Longtail_StorageAPI* Longtail_CreateFSStorageAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    void* mem = (struct FSStorageAPI*)Longtail_Alloc("FSStorageAPI", sizeof(struct FSStorageAPI));
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_StorageAPI* storage_api;
    int err = FSStorageAPI_Init(mem, &storage_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSStorageAPI_Init() failed with %d", err)
        return 0;
    }
    return storage_api;
}

