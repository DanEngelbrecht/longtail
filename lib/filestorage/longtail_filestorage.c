#include "longtail_filestorage.h"

#include "../longtail_platform.h"
#include <inttypes.h>
#include <errno.h>

#if defined(__clang__) || defined(__GNUC__)
#if defined(WIN32)
    #include <malloc.h>
#else
    #include <alloca.h>
#endif
#elif defined(_MSC_VER)
    #include <malloc.h>
    #define alloca _alloca
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_FATAL_ASSERT_WITH_CTX(ctx, storage_api != 0, return);
    Longtail_Free(storage_api);
}

#define TMP_STR(str) \
    size_t len_##str = strlen(str); \
    char* tmp_##str = (char*)alloca(len_##str + 1); \
    memmove(tmp_##str, str, len_##str); \
    tmp_##str[len_##str] = '\0';

static int FSStorageAPI_OpenReadFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    Longtail_StorageAPI_HOpenFile* out_open_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, out_open_file != 0, return EINVAL);

    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    HLongtail_OpenFile r;
    int err = Longtail_OpenReadFile(tmp_path, &r);
    if (err != 0)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_OpenReadFile() failed with %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, out_size != 0, return EINVAL);
    int err = Longtail_GetFileSize((HLongtail_OpenFile)f, out_size);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetFileSize() failed with %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(length, "%" PRIu64),
        LONGTAIL_LOGFIELD(output, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, output != 0, return EINVAL);
    int err = Longtail_Read((HLongtail_OpenFile)f, offset,length, output);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Read() failed with %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(initial_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, out_open_file != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    HLongtail_OpenFile r;
    int err = Longtail_OpenWriteFile(tmp_path, initial_size, &r);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_OpenWriteFile() failed with %d", err)
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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(offset, "%" PRIu64),
        LONGTAIL_LOGFIELD(length, "%" PRIu64),
        LONGTAIL_LOGFIELD(input, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, input != 0, return EINVAL);
    int err = Longtail_Write((HLongtail_OpenFile)f, offset,length, input);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Write() failed with %d", err)
        return 0;
    }
    return err;
}

static int FSStorageAPI_SetSize(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t length)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(length, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, f != 0, return EINVAL);
    int err = Longtail_SetFileSize((HLongtail_OpenFile)f, length);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_SetFileSize() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_SetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(permissions, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_SetFilePermissions(tmp_path, permissions);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_SetFilePermissions() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_GetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t* out_permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_permissions, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_GetFilePermissions(tmp_path, out_permissions);
    if (err == ENOENT)
    {
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetFilePermissions() failed with %d", err)
        return err;
    }
    return 0;
}

static void FSStorageAPI_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return);
    Longtail_CloseFile((HLongtail_OpenFile)f);
}

static int FSStorageAPI_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_CreateDirectory(tmp_path);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateDirectory() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, source_path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, target_path != 0, return EINVAL);
    TMP_STR(source_path)
    TMP_STR(target_path)
    Longtail_DenormalizePath(tmp_source_path);
    Longtail_DenormalizePath(tmp_target_path);
    int err = Longtail_MoveFile(tmp_source_path, tmp_target_path);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_MoveFile() failed with %d", err)
        return err;
    }
    return 0;
}

static char* FSStorageAPI_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(sub_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, root_path != 0, return 0);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, sub_path != 0, return 0);
    TMP_STR(root_path)
    Longtail_DenormalizePath(tmp_root_path);
    TMP_STR(sub_path)
    Longtail_DenormalizePath(tmp_sub_path);
    char* path = (char*)Longtail_ConcatPath(tmp_root_path, tmp_sub_path);
    if (!path)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ConcatPath() failed with %d", ENOMEM)
        return 0;
    }
    Longtail_NormalizePath(path);
    return path;
}

static int FSStorageAPI_IsDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int is_dir = Longtail_IsDir(tmp_path);
    return is_dir;
}

static int FSStorageAPI_IsFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int is_file = Longtail_IsFile(tmp_path);
    return is_file;
}

static int FSStorageAPI_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_RemoveDir(tmp_path);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_RemoveDir() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_RemoveFile(tmp_path);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_RemoveFile() failed with %d", err)
        return err;
    }
    return 0;
}

static int FSStorageAPI_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, out_iterator != 0, return EINVAL);
    Longtail_StorageAPI_HIterator iterator = (Longtail_StorageAPI_HIterator)Longtail_Alloc(Longtail_GetFSIteratorSize());
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_StartFind((HLongtail_FSIterator)iterator, tmp_path);
    if (err == ENOENT)
    {
        Longtail_Free(iterator);
        return err;
    }
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_StartFind() failed with %d", err)
        Longtail_Free(iterator);
        return err;
    }
    *out_iterator = iterator;
    return 0;
}

static int FSStorageAPI_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, iterator != 0, return EINVAL);
    int err = Longtail_FindNext((HLongtail_FSIterator)iterator);
    if (err && (err != ENOENT))
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_FindNext() failed with %d", err)
    }
    return err;
}

static void FSStorageAPI_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, storage_api != 0, return );
    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, iterator != 0, return );
    Longtail_CloseFind((HLongtail_FSIterator)iterator);
    Longtail_Free(iterator);
}

static int FSStorageAPI_GetEntryProperties(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator,
    struct Longtail_StorageAPI_EntryProperties* out_properties)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%s"),
        LONGTAIL_LOGFIELD(out_properties, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT_WITH_CTX(ctx, storage_api != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT_WITH_CTX(ctx, iterator != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT_WITH_CTX(ctx, out_properties != 0, return EINVAL);
    int err = Longtail_GetEntryProperties((HLongtail_FSIterator)iterator, &out_properties->m_Size, &out_properties->m_Permissions, &out_properties->m_IsDir);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetEntryProperties() failed with %d", err)
        return err;
    }
    out_properties->m_Name = (out_properties->m_IsDir) ? Longtail_GetDirectoryName((HLongtail_FSIterator)iterator) : Longtail_GetFileName((HLongtail_FSIterator)iterator);
    return 0;
}

static int FSStorageAPI_LockFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HLockFile* out_lock_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(out_lock_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    void* mem = Longtail_Alloc(Longtail_GetFileLockSize());
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

    int err = Longtail_UnlockFile((HLongtail_FileLock)lock_file);
    if (err)
    {
        return err;
    }
    Longtail_Free(lock_file);
    return 0;
}

static int FSStorageAPI_Init(
    void* mem,
    struct Longtail_StorageAPI** out_storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_storage_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT_WITH_CTX(ctx, mem != 0, return 0);
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
        FSStorageAPI_UnlockFile);
    *out_storage_api = api;
    return 0;
}


struct Longtail_StorageAPI* Longtail_CreateFSStorageAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    void* mem = (struct FSStorageAPI*)Longtail_Alloc(sizeof(struct FSStorageAPI));
    if (!mem)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_StorageAPI* storage_api;
    int err = FSStorageAPI_Init(mem, &storage_api);
    if (err)
    {
        LONGTAIL_LOG_WITH_CTX(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FSStorageAPI_Init() failed with %d", err)
        return 0;
    }
    return storage_api;
}

