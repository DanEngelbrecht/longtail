#include "longtail_filestorage.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return);
    Longtail_Free(storage_api);
}

#define TMP_STR(str) \
    size_t len_##str = strlen(str); \
    char* tmp_##str = (char*)alloca(len_##str + 1); \
    memmove(tmp_##str, str, len_##str); \
    tmp_##str[len_##str] = '\0';

static int FSStorageAPI_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(out_open_file != 0, return EINVAL);

    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    HLongtail_OpenFile r;
    int err = Longtail_OpenReadFile(tmp_path, &r);
    if (err != 0)
    {
        return err;
    }
    *out_open_file = (Longtail_StorageAPI_HOpenFile)r;
    return 0;
}

static int FSStorageAPI_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(out_size != 0, return EINVAL);
    return Longtail_GetFileSize((HLongtail_OpenFile)f, out_size);
}

static int FSStorageAPI_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(output != 0, return EINVAL);
    return Longtail_Read((HLongtail_OpenFile)f, offset,length, output);
}

static int FSStorageAPI_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(out_open_file != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    HLongtail_OpenFile r;
    int err = Longtail_OpenWriteFile(tmp_path, initial_size, &r);
    if (err)
    {
        return err;
    }
    *out_open_file = (Longtail_StorageAPI_HOpenFile)r;
    return 0;
}

static int FSStorageAPI_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(f != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(input != 0, return EINVAL);
    return Longtail_Write((HLongtail_OpenFile)f, offset,length, input);
}

static int FSStorageAPI_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(f != 0, return EINVAL);
    return Longtail_SetFileSize((HLongtail_OpenFile)f, length);
}

static int FSStorageAPI_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    return Longtail_SetFilePermissions(tmp_path, permissions);
}

static void FSStorageAPI_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return);
    Longtail_CloseFile((HLongtail_OpenFile)f);
}

static int FSStorageAPI_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_CreateDirectory(tmp_path);
    return err;
}

static int FSStorageAPI_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(source_path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(target_path != 0, return EINVAL);
    TMP_STR(source_path)
    TMP_STR(target_path)
    Longtail_DenormalizePath(tmp_source_path);
    Longtail_DenormalizePath(tmp_target_path);
    int err = Longtail_MoveFile(tmp_source_path, tmp_target_path);
    return err;
}

static char* FSStorageAPI_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(root_path != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(sub_path != 0, return 0);
    TMP_STR(root_path)
    Longtail_DenormalizePath(tmp_root_path);
    TMP_STR(sub_path)
    Longtail_DenormalizePath(tmp_sub_path);
    char* path = (char*)Longtail_ConcatPath(tmp_root_path, tmp_sub_path);
    Longtail_NormalizePath(path);
    return path;
}

static int FSStorageAPI_IsDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int is_dir = Longtail_IsDir(tmp_path);
    return is_dir;
}

static int FSStorageAPI_IsFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int is_file = Longtail_IsFile(tmp_path);
    return is_file;
}

static int FSStorageAPI_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_RemoveDir(tmp_path);
    return err;
}

static int FSStorageAPI_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_RemoveFile(tmp_path);
    return err;
}

static int FSStorageAPI_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(out_iterator != 0, return EINVAL);
    Longtail_StorageAPI_HIterator iterator = (Longtail_StorageAPI_HIterator)Longtail_Alloc(Longtail_GetFSIteratorSize());
    TMP_STR(path)
    Longtail_DenormalizePath(tmp_path);
    int err = Longtail_StartFind((HLongtail_FSIterator)iterator, tmp_path);
    if (err)
    {
		Longtail_Free(iterator);
        iterator = 0;
        return err;
    }
    *out_iterator = iterator;
    return 0;
}

static int FSStorageAPI_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL);
    LONGTAIL_VALIDATE_INPUT(iterator != 0, return EINVAL);
    return Longtail_FindNext((HLongtail_FSIterator)iterator);
}

static void FSStorageAPI_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return );
    LONGTAIL_VALIDATE_INPUT(iterator != 0, return );
    Longtail_CloseFind((HLongtail_FSIterator)iterator);
	Longtail_Free(iterator);
}

static int FSStorageAPI_GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, struct Longtail_StorageAPI_EntryProperties* out_properties)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(iterator != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_properties != 0, return EINVAL);
    int err = Longtail_GetEntryProperties((HLongtail_FSIterator)iterator, &out_properties->m_Size, &out_properties->m_Permissions, &out_properties->m_IsDir);
    if (err)
    {
        return err;
    }
    out_properties->m_Name = (out_properties->m_IsDir) ? Longtail_GetDirectoryName((HLongtail_FSIterator)iterator) : Longtail_GetFileName((HLongtail_FSIterator)iterator);
    return 0;
}

static void FSStorageAPI_Init(struct FSStorageAPI* storage_api)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return);
    storage_api->m_FSStorageAPI.m_API.Dispose = FSStorageAPI_Dispose;
    storage_api->m_FSStorageAPI.OpenReadFile = FSStorageAPI_OpenReadFile;
    storage_api->m_FSStorageAPI.GetSize = FSStorageAPI_GetSize;
    storage_api->m_FSStorageAPI.Read = FSStorageAPI_Read;
    storage_api->m_FSStorageAPI.OpenWriteFile = FSStorageAPI_OpenWriteFile;
    storage_api->m_FSStorageAPI.Write = FSStorageAPI_Write;
    storage_api->m_FSStorageAPI.SetSize = FSStorageAPI_SetSize;
    storage_api->m_FSStorageAPI.SetPermissions = FSStorageAPI_SetPermissions;
    storage_api->m_FSStorageAPI.CloseFile = FSStorageAPI_CloseFile;
    storage_api->m_FSStorageAPI.CreateDir = FSStorageAPI_CreateDir;
    storage_api->m_FSStorageAPI.RenameFile = FSStorageAPI_RenameFile;
    storage_api->m_FSStorageAPI.ConcatPath = FSStorageAPI_ConcatPath;
    storage_api->m_FSStorageAPI.IsDir = FSStorageAPI_IsDir;
    storage_api->m_FSStorageAPI.IsFile = FSStorageAPI_IsFile;
    storage_api->m_FSStorageAPI.RemoveDir = FSStorageAPI_RemoveDir;
    storage_api->m_FSStorageAPI.RemoveFile = FSStorageAPI_RemoveFile;
    storage_api->m_FSStorageAPI.StartFind = FSStorageAPI_StartFind;
    storage_api->m_FSStorageAPI.FindNext = FSStorageAPI_FindNext;
    storage_api->m_FSStorageAPI.CloseFind = FSStorageAPI_CloseFind;
    storage_api->m_FSStorageAPI.GetEntryProperties = FSStorageAPI_GetEntryProperties;
}


struct Longtail_StorageAPI* Longtail_CreateFSStorageAPI()
{
    struct FSStorageAPI* storage_api = (struct FSStorageAPI*)Longtail_Alloc(sizeof(struct FSStorageAPI));
    if (!storage_api)
    {
        // TODO: Log
        return 0;
    }
    FSStorageAPI_Init(storage_api);
    return &storage_api->m_FSStorageAPI;
}

