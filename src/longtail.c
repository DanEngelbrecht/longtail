#include "longtail.h"

#if defined(__GNUC__) && !defined(__clang__) && !defined(APPLE) && !defined(__USE_GNU)
#define __USE_GNU
#endif

#include "ext/stb_ds.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#define LONGTAIL_VERSION(major, minor, patch)  ((((uint32_t)major) << 24) | ((uint32_t)minor << 16) | ((uint32_t)patch))
#define LONGTAIL_VERSION_INDEX_VERSION_0_0_1  LONGTAIL_VERSION(0,0,1)
#define LONGTAIL_VERSION_INDEX_VERSION_0_0_2  LONGTAIL_VERSION(0,0,2)
#define LONGTAIL_STORE_INDEX_VERSION_1_0_0    LONGTAIL_VERSION(1,0,0)
#define LONGTAIL_ARCHIVE_VERSION_0_0_1        LONGTAIL_VERSION(0,0,1)

uint32_t Longtail_CurrentVersionIndexVersion = LONGTAIL_VERSION_INDEX_VERSION_0_0_2;
uint32_t Longtail_CurrentStoreIndexVersion = LONGTAIL_STORE_INDEX_VERSION_1_0_0;
uint32_t Longtail_CurrentArchiveVersion = LONGTAIL_ARCHIVE_VERSION_0_0_1;

#if defined(_WIN32)
    #define SORTFUNC(name) int name(void* context, const void* a_ptr, const void* b_ptr)
    #define QSORT(base, count, size, func, context) qsort_s(base, count, size, func, context)
#elif defined(__clang__) || defined(__GNUC__)
    #if defined(__APPLE__)
        #define SORTFUNC(name) int name(void* context, const void* a_ptr, const void* b_ptr)
        #define QSORT(base, count, size, func, context) qsort_r(base, count, size, context, func)
    #else
        #define SORTFUNC(name) int name(const void* a_ptr, const void* b_ptr, void* context)
        #define QSORT(base, count, size, func, context) qsort_r(base, count, size, func, context)
    #endif
#endif

uint64_t Longtail_GetCancelAPISize()
{
    return sizeof(struct Longtail_CancelAPI);
}

 struct Longtail_CancelAPI* Longtail_MakeCancelAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_CancelAPI_CreateTokenFunc create_token_func,
    Longtail_CancelAPI_CancelFunc cancel_func,
    Longtail_CancelAPI_IsCancelledFunc is_cancelled,
    Longtail_CancelAPI_DisposeTokenFunc dispose_token_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(create_token_func, "%p"),
        LONGTAIL_LOGFIELD(cancel_func, "%p"),
        LONGTAIL_LOGFIELD(is_cancelled, "%p"),
        LONGTAIL_LOGFIELD(dispose_token_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_CancelAPI* api = (struct Longtail_CancelAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->CreateToken = create_token_func;
    api->Cancel = cancel_func;
    api->IsCancelled = is_cancelled;
    api->DisposeToken = dispose_token_func;
    return api;
}

int Longtail_CancelAPI_CreateToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken* out_token) { return cancel_api->CreateToken(cancel_api, out_token); }
int Longtail_CancelAPI_Cancel(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token) { return cancel_api->Cancel(cancel_api, token); }
int Longtail_CancelAPI_IsCancelled(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token) { return cancel_api->IsCancelled(cancel_api, token); }
int Longtail_CancelAPI_DisposeToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token) { return cancel_api->DisposeToken(cancel_api, token); }

uint64_t Longtail_GetPathFilterAPISize()
{
    return sizeof(struct Longtail_PathFilterAPI);
}

struct Longtail_PathFilterAPI* Longtail_MakePathFilterAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_PathFilter_IncludeFunc include_filter_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(include_filter_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_PathFilterAPI* api = (struct Longtail_PathFilterAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->Include = include_filter_func;
    return api;
}

int Longtail_PathFilter_Include(struct Longtail_PathFilterAPI* path_filter_api, const char* root_path, const char* asset_path, const char* asset_name, int is_dir, uint64_t size, uint16_t permissions)
{
    return path_filter_api->Include(path_filter_api, root_path, asset_path, asset_name, is_dir, size, permissions);
}

uint64_t Longtail_GetHashAPISize()
{
    return sizeof(struct Longtail_HashAPI);
}

struct Longtail_HashAPI* Longtail_MakeHashAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Hash_GetIdentifierFunc get_identifier_func,
    Longtail_Hash_BeginContextFunc begin_context_func,
    Longtail_Hash_HashFunc hash_func,
    Longtail_Hash_EndContextFunc end_context_func,
    Longtail_Hash_HashBufferFunc hash_buffer_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_identifier_func, "%p"),
        LONGTAIL_LOGFIELD(begin_context_func, "%p"),
        LONGTAIL_LOGFIELD(hash_func, "%p"),
        LONGTAIL_LOGFIELD(end_context_func, "%p"),
        LONGTAIL_LOGFIELD(hash_buffer_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_HashAPI* api = (struct Longtail_HashAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetIdentifier = get_identifier_func;
    api->BeginContext = begin_context_func;
    api->Hash = hash_func;
    api->EndContext = end_context_func;
    api->HashBuffer = hash_buffer_func;
    return api;
}

uint32_t Longtail_Hash_GetIdentifier(struct Longtail_HashAPI* hash_api) { return hash_api->GetIdentifier(hash_api);}
int Longtail_Hash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context) { return hash_api->BeginContext(hash_api, out_context); }
void Longtail_Hash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data) { hash_api->Hash(hash_api, context, length, data); }
uint64_t Longtail_Hash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context) { return hash_api->EndContext(hash_api, context); }
int Longtail_Hash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash) { return hash_api->HashBuffer(hash_api, length, data, out_hash); }


uint64_t Longtail_GetHashRegistrySize()
{
    return sizeof(struct Longtail_HashRegistryAPI);
}

struct Longtail_HashRegistryAPI* Longtail_MakeHashRegistryAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_HashRegistry_GetHashAPIFunc get_hash_api_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_hash_api_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_HashRegistryAPI* api = (struct Longtail_HashRegistryAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetHashAPI = get_hash_api_func;
    return api;
}

int Longtail_GetHashRegistry_GetHashAPI(struct Longtail_HashRegistryAPI* hash_registry, uint32_t hash_type, struct Longtail_HashAPI** out_hash_api) { return hash_registry->GetHashAPI(hash_registry, hash_type, out_hash_api); }


uint64_t Longtail_GetCompressionAPISize()
{
    return sizeof(struct Longtail_CompressionAPI);
}

struct Longtail_CompressionAPI* Longtail_MakeCompressionAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_CompressionAPI_GetMaxCompressedSizeFunc get_max_compressed_size_func,
    Longtail_CompressionAPI_CompressFunc compress_func,
    Longtail_CompressionAPI_DecompressFunc decompress_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_max_compressed_size_func, "%p"),
        LONGTAIL_LOGFIELD(compress_func, "%p"),
        LONGTAIL_LOGFIELD(decompress_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_CompressionAPI* api = (struct Longtail_CompressionAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetMaxCompressedSize = get_max_compressed_size_func;
    api->Compress = compress_func;
    api->Decompress = decompress_func;
    return api;
}

size_t Longtail_CompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size) { return compression_api->GetMaxCompressedSize(compression_api, settings_id, size); }
int Longtail_CompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size) { return compression_api->Compress(compression_api, settings_id, uncompressed, compressed, uncompressed_size, max_compressed_size, out_compressed_size); }
int Longtail_CompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size) { return compression_api->Decompress(compression_api, compressed, uncompressed, compressed_size, max_uncompressed_size, out_uncompressed_size); }



uint64_t Longtail_GetCompressionRegistryAPISize()
{
    return sizeof(struct Longtail_CompressionRegistryAPI);
}

struct Longtail_CompressionRegistryAPI* Longtail_MakeCompressionRegistryAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_CompressionRegistry_GetCompressionAPIFunc get_compression_api_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_compression_api_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_CompressionRegistryAPI* api = (struct Longtail_CompressionRegistryAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetCompressionAPI = get_compression_api_func;
    return api;
}

int Longtail_GetCompressionRegistry_GetCompressionAPI(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, uint32_t* out_settings_id) { return compression_registry->GetCompressionAPI(compression_registry, compression_type, out_compression_api, out_settings_id); }



////////////// StorageAPI

uint64_t Longtail_GetStorageAPISize()
{
    return sizeof(struct Longtail_StorageAPI);
}

struct Longtail_StorageAPI* Longtail_MakeStorageAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Storage_OpenReadFileFunc open_read_file_func,
    Longtail_Storage_GetSizeFunc get_size_func,
    Longtail_Storage_ReadFunc read_func,
    Longtail_Storage_OpenWriteFileFunc open_write_file_func,
    Longtail_Storage_WriteFunc write_func,
    Longtail_Storage_SetSizeFunc set_size_func,
    Longtail_Storage_SetPermissionsFunc set_permissions_func,
    Longtail_Storage_GetPermissionsFunc get_permissions_func,
    Longtail_Storage_CloseFileFunc close_file_func,
    Longtail_Storage_CreateDirFunc create_dir_func,
    Longtail_Storage_RenameFileFunc rename_file_func,
    Longtail_Storage_ConcatPathFunc concat_path_func,
    Longtail_Storage_IsDirFunc is_dir_func,
    Longtail_Storage_IsFileFunc is_file_func,
    Longtail_Storage_RemoveDirFunc remove_dir_func,
    Longtail_Storage_RemoveFileFunc remove_file_func,
    Longtail_Storage_StartFindFunc start_find_func,
    Longtail_Storage_FindNextFunc find_next_func,
    Longtail_Storage_CloseFindFunc close_find_func,
    Longtail_Storage_GetEntryPropertiesFunc get_entry_properties_func,
    Longtail_Storage_LockFileFunc lock_file_func,
    Longtail_Storage_UnlockFileFunc unlock_file_func,
    Longtail_Storage_GetParentPathFunc get_parent_path_func,
    Longtail_Storage_MapFileFunc map_file_func,
    Longtail_Storage_UnmapFileFunc unmap_file_func,
    Longtail_Storage_OpenAppendFileFunc open_append_file_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(open_read_file_func, "%p"),
        LONGTAIL_LOGFIELD(get_size_func, "%p"),
        LONGTAIL_LOGFIELD(read_func, "%p"),
        LONGTAIL_LOGFIELD(open_write_file_func, "%p"),
        LONGTAIL_LOGFIELD(write_func, "%p"),
        LONGTAIL_LOGFIELD(set_size_func, "%p"),
        LONGTAIL_LOGFIELD(set_permissions_func, "%p"),
        LONGTAIL_LOGFIELD(get_permissions_func, "%p"),
        LONGTAIL_LOGFIELD(close_file_func, "%p"),
        LONGTAIL_LOGFIELD(create_dir_func, "%p"),
        LONGTAIL_LOGFIELD(rename_file_func, "%p"),
        LONGTAIL_LOGFIELD(concat_path_func, "%p"),
        LONGTAIL_LOGFIELD(is_dir_func, "%p"),
        LONGTAIL_LOGFIELD(is_file_func, "%p"),
        LONGTAIL_LOGFIELD(remove_dir_func, "%p"),
        LONGTAIL_LOGFIELD(remove_file_func, "%p"),
        LONGTAIL_LOGFIELD(start_find_func, "%p"),
        LONGTAIL_LOGFIELD(find_next_func, "%p"),
        LONGTAIL_LOGFIELD(close_find_func, "%p"),
        LONGTAIL_LOGFIELD(get_entry_properties_func, "%p"),
        LONGTAIL_LOGFIELD(lock_file_func, "%p"),
        LONGTAIL_LOGFIELD(unlock_file_func, "%p"),
        LONGTAIL_LOGFIELD(get_parent_path_func, "%p"),
        LONGTAIL_LOGFIELD(map_file_func, "%p"),
        LONGTAIL_LOGFIELD(unmap_file_func, "%p"),
        LONGTAIL_LOGFIELD(open_append_file_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_StorageAPI* api = (struct Longtail_StorageAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OpenReadFile = open_read_file_func;
    api->GetSize = get_size_func;
    api->Read = read_func;
    api->OpenWriteFile = open_write_file_func;
    api->Write = write_func;
    api->SetSize = set_size_func;
    api->SetPermissions = set_permissions_func;
    api->GetPermissions = get_permissions_func;
    api->CloseFile = close_file_func;
    api->CreateDir = create_dir_func;
    api->RenameFile = rename_file_func;
    api->ConcatPath = concat_path_func;
    api->IsDir = is_dir_func;
    api->IsFile = is_file_func;
    api->RemoveDir = remove_dir_func;
    api->RemoveFile = remove_file_func;
    api->StartFind = start_find_func;
    api->FindNext = find_next_func;
    api->CloseFind = close_find_func;
    api->GetEntryProperties = get_entry_properties_func;
    api->LockFile = lock_file_func;
    api->UnlockFile = unlock_file_func;
    api->GetParentPath = get_parent_path_func;
    api->MapFile = map_file_func;
    api->UnMapFile = unmap_file_func;
    api->OpenAppendFile = open_append_file_func;
    return api;
}

int Longtail_Storage_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file) { return storage_api->OpenReadFile(storage_api, path, out_open_file); }
int Longtail_Storage_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size) { return storage_api->GetSize(storage_api, f, out_size); }
int Longtail_Storage_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output) { return storage_api->Read(storage_api, f, offset, length, output); }
int Longtail_Storage_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file) { return storage_api->OpenWriteFile(storage_api, path, initial_size, out_open_file); }
int Longtail_Storage_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input) { return storage_api->Write(storage_api, f, offset, length, input); }
int Longtail_Storage_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length) { return storage_api->SetSize(storage_api, f, length); }
int Longtail_Storage_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions) { return storage_api->SetPermissions(storage_api, path, permissions); }
int Longtail_Storage_GetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t* out_permissions) { return storage_api->GetPermissions(storage_api, path, out_permissions); }
void Longtail_Storage_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f) { storage_api->CloseFile(storage_api, f); }
int Longtail_Storage_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->CreateDir(storage_api, path); }
int Longtail_Storage_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path) { return storage_api->RenameFile(storage_api, source_path, target_path); }
char* Longtail_Storage_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path) { return storage_api->ConcatPath(storage_api, root_path, sub_path); }
int Longtail_Storage_IsDir(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->IsDir(storage_api, path); }
int Longtail_Storage_IsFile(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->IsFile(storage_api, path); }
int Longtail_Storage_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->RemoveDir(storage_api, path); }
int Longtail_Storage_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->RemoveFile(storage_api, path); }
int Longtail_Storage_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator) { return storage_api->StartFind(storage_api, path, out_iterator); }
int Longtail_Storage_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { return storage_api->FindNext(storage_api, iterator); }
void Longtail_Storage_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { storage_api->CloseFind(storage_api, iterator); }
int Longtail_Storage_GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, struct Longtail_StorageAPI_EntryProperties* out_properties) { return storage_api->GetEntryProperties(storage_api, iterator, out_properties); }
int Longtail_Storage_LockFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HLockFile* out_lock_file) { return storage_api->LockFile(storage_api, path, out_lock_file); }
int Longtail_Storage_UnlockFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HLockFile lock_file) { return storage_api->UnlockFile(storage_api, lock_file); }
char* Longtail_Storage_GetParentPath(struct Longtail_StorageAPI* storage_api, const char* path) { return storage_api->GetParentPath(storage_api, path); }
int Longtail_Storage_MapFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, Longtail_StorageAPI_HFileMap* out_file_map, const void** out_data_ptr) { return storage_api->MapFile(storage_api, f, offset, length, out_file_map, out_data_ptr); }
void Longtail_Storage_UnmapFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HFileMap m) { storage_api->UnMapFile(storage_api, m); }
int Longtail_Storage_OpenAppendFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file) { return storage_api->OpenAppendFile(storage_api, path, out_open_file); }

////////////// ConcurrentChunkWriteAPI

LONGTAIL_EXPORT uint64_t Longtail_GetConcurrentChunkWriteAPISize()
{
    return sizeof(struct Longtail_ConcurrentChunkWriteAPI);
}

LONGTAIL_EXPORT struct Longtail_ConcurrentChunkWriteAPI* Longtail_MakeConcurrentChunkWriteAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_ConcurrentChunkWrite_CreateDirFunc create_dir_func,
    Longtail_ConcurrentChunkWrite_OpenFunc open_func,
    Longtail_ConcurrentChunkWrite_CloseFunc close_func,
    Longtail_ConcurrentChunkWrite_WriteFunc write_func,
    Longtail_ConcurrentChunkWrite_FlushFunc flush_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(create_dir_func, "%p"),
        LONGTAIL_LOGFIELD(open_func, "%p"),
        LONGTAIL_LOGFIELD(close_func, "%p"),
        LONGTAIL_LOGFIELD(write_func, "%p"),
        LONGTAIL_LOGFIELD(flush_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_ConcurrentChunkWriteAPI* api = (struct Longtail_ConcurrentChunkWriteAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->CreateDir = create_dir_func;
    api->Open = open_func;
    api->Close = close_func;
    api->Write = write_func;
    api->Flush = flush_func;
    return api;
}

int Longtail_ConcurrentChunkWrite_CreateDir(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, uint32_t asset_index) { return concurrent_file_write_api->CreateDir(concurrent_file_write_api, asset_index); }
int Longtail_ConcurrentChunkWrite_Open(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, uint32_t asset_index) { return concurrent_file_write_api->Open(concurrent_file_write_api, asset_index); }
void Longtail_ConcurrentChunkWrite_Close(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, uint32_t asset_index) { concurrent_file_write_api->Close(concurrent_file_write_api, asset_index); }
int Longtail_ConcurrentChunkWrite_Write(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api, uint32_t asset_index, uint64_t offset, uint32_t size, const void* input) { return concurrent_file_write_api->Write(concurrent_file_write_api, asset_index, offset, size, input); }
int Longtail_ConcurrentChunkWrite_Flush(struct Longtail_ConcurrentChunkWriteAPI* concurrent_file_write_api) { return concurrent_file_write_api->Flush(concurrent_file_write_api); }

////////////// ProgressAPI

uint64_t Longtail_GetProgressAPISize()
{
    return sizeof(struct Longtail_ProgressAPI);
}

struct Longtail_ProgressAPI* Longtail_MakeProgressAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Progress_OnProgressFunc on_progress_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_progress_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_ProgressAPI* api = (struct Longtail_ProgressAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnProgress = on_progress_func;
    return api;
}

void Longtail_Progress_OnProgress(struct Longtail_ProgressAPI* progressAPI, uint32_t total_count, uint32_t done_count) { progressAPI->OnProgress(progressAPI, total_count, done_count); }

////////////// JobAPI

uint64_t Longtail_GetJobAPISize()
{
    return sizeof(struct Longtail_JobAPI);
}

struct Longtail_JobAPI* Longtail_MakeJobAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Job_GetWorkerCountFunc get_worker_count_func,
    Longtail_Job_ReserveJobsFunc reserve_jobs_func,
    Longtail_Job_CreateJobsFunc create_jobs_func,
    Longtail_Job_AddDependeciesFunc add_dependecies_func,
    Longtail_Job_ReadyJobsFunc ready_jobs_func,
    Longtail_Job_WaitForAllJobsFunc wait_for_all_jobs_func,
    Longtail_Job_ResumeJobFunc resume_job_func,
    Longtail_Job_GetMaxBatchCountFunc get_max_batch_count_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_worker_count_func, "%p"),
        LONGTAIL_LOGFIELD(reserve_jobs_func, "%p"),
        LONGTAIL_LOGFIELD(create_jobs_func, "%p"),
        LONGTAIL_LOGFIELD(add_dependecies_func, "%p"),
        LONGTAIL_LOGFIELD(ready_jobs_func, "%p"),
        LONGTAIL_LOGFIELD(wait_for_all_jobs_func, "%p"),
        LONGTAIL_LOGFIELD(resume_job_func, "%p"),
        LONGTAIL_LOGFIELD(get_max_batch_count_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_JobAPI* api = (struct Longtail_JobAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetWorkerCount = get_worker_count_func;
    api->ReserveJobs = reserve_jobs_func;
    api->CreateJobs = create_jobs_func;
    api->AddDependecies = add_dependecies_func;
    api->ReadyJobs = ready_jobs_func;
    api->WaitForAllJobs = wait_for_all_jobs_func;
    api->ResumeJob = resume_job_func;
    api->GetMaxBatchCount = get_max_batch_count_func;
    return api;
}

uint32_t Longtail_Job_GetWorkerCount(struct Longtail_JobAPI* job_api) { return job_api->GetWorkerCount(job_api); }
int Longtail_Job_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Group* out_job_group) { return job_api->ReserveJobs(job_api, job_count, out_job_group); }
int Longtail_Job_CreateJobs(struct Longtail_JobAPI* job_api, Longtail_JobAPI_Group job_group, struct Longtail_ProgressAPI* progressAPI, struct Longtail_CancelAPI* optional_cancel_api, Longtail_CancelAPI_HCancelToken optional_cancel_token, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], uint8_t job_channel, Longtail_JobAPI_Jobs* out_jobs) { return job_api->CreateJobs(job_api, job_group, progressAPI, optional_cancel_api, optional_cancel_token, job_count, job_funcs, job_contexts, job_channel, out_jobs); }
int Longtail_Job_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs) { return job_api->AddDependecies(job_api, job_count, jobs, dependency_job_count, dependency_jobs); }
int Longtail_Job_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs) { return job_api->ReadyJobs(job_api, job_count, jobs); }
int Longtail_Job_WaitForAllJobs(struct Longtail_JobAPI* job_api, Longtail_JobAPI_Group job_group, struct Longtail_ProgressAPI* progressAPI, struct Longtail_CancelAPI* optional_cancel_api, Longtail_CancelAPI_HCancelToken optional_cancel_token) { return job_api->WaitForAllJobs(job_api, job_group, progressAPI, optional_cancel_api, optional_cancel_token); }
int Longtail_Job_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id) { return job_api->ResumeJob(job_api, job_id); }
int Longtail_Job_GetMaxBatchCount(struct Longtail_JobAPI* job_api, uint32_t* out_max_job_batch_count, uint32_t* out_max_dependency_batch_count) { return job_api->GetMaxBatchCount(job_api, out_max_job_batch_count, out_max_dependency_batch_count); }

////////////// ChunkerAPI

uint64_t Longtail_GetChunkerAPISize()
{
    return sizeof(struct Longtail_ChunkerAPI);
}

struct Longtail_ChunkerAPI* Longtail_MakeChunkerAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Chunker_GetMinChunkSizeFunc get_min_chunk_size_func,
    Longtail_Chunker_CreateChunkerFunc create_chunker_func,
    Longtail_Chunker_NextChunkFunc next_chunk_func,
    Longtail_Chunker_DisposeChunkerFunc dispose_chunker_func,
    Longtail_Chunker_NextChunkFromBufferFunc next_chunk_from_buffer)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_min_chunk_size_func, "%p"),
        LONGTAIL_LOGFIELD(create_chunker_func, "%p"),
        LONGTAIL_LOGFIELD(next_chunk_func, "%p"),
        LONGTAIL_LOGFIELD(dispose_chunker_func, "%p"),
        LONGTAIL_LOGFIELD(next_chunk_from_buffer, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_ChunkerAPI* api = (struct Longtail_ChunkerAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetMinChunkSize = get_min_chunk_size_func;
    api->CreateChunker = create_chunker_func;
    api->NextChunk = next_chunk_func;
    api->DisposeChunker = dispose_chunker_func;
    api->NextChunkFromBuffer = next_chunk_from_buffer;
    return api;
}

int Longtail_Chunker_GetMinChunkSize(struct Longtail_ChunkerAPI* chunker_api, uint32_t* out_min_chunk_size) { return chunker_api->GetMinChunkSize(chunker_api, out_min_chunk_size); }
int Longtail_Chunker_CreateChunker(struct Longtail_ChunkerAPI* chunker_api, uint32_t min_chunk_size, uint32_t avg_chunk_size, uint32_t max_chunk_size, Longtail_ChunkerAPI_HChunker* out_chunker) { return chunker_api->CreateChunker(chunker_api, min_chunk_size, avg_chunk_size, max_chunk_size, out_chunker); }
int Longtail_Chunker_NextChunk(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker, Longtail_Chunker_Feeder feeder, void* feeder_context, struct Longtail_Chunker_ChunkRange* out_chunk_range) { return chunker_api->NextChunk(chunker_api, chunker, feeder, feeder_context, out_chunk_range); }
int Longtail_Chunker_DisposeChunker(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker) { return chunker_api->DisposeChunker(chunker_api, chunker); }
int Longtail_Chunker_NextChunkFromBuffer(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker, const void* buffer, uint64_t buffer_size, const void** out_next_chunk_start) { return chunker_api->NextChunkFromBuffer(chunker_api, chunker, buffer, buffer_size, out_next_chunk_start); }

////////////// AsyncPutStoredBlockAPI

uint64_t Longtail_GetAsyncPutStoredBlockAPISize()
{
    return sizeof(struct Longtail_AsyncPutStoredBlockAPI);
}

struct Longtail_AsyncPutStoredBlockAPI* Longtail_MakeAsyncPutStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncPutStoredBlock_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncPutStoredBlockAPI* api = (struct Longtail_AsyncPutStoredBlockAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncPutStoredBlock_OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err) { async_complete_api->OnComplete(async_complete_api, err); }

////////////// AsyncGetStoredBlockAPI

uint64_t Longtail_GetAsyncGetStoredBlockAPISize()
{
    return sizeof(struct Longtail_AsyncGetStoredBlockAPI);
}

struct Longtail_AsyncGetStoredBlockAPI* Longtail_MakeAsyncGetStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetStoredBlock_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncGetStoredBlockAPI* api = (struct Longtail_AsyncGetStoredBlockAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncGetStoredBlock_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err) { async_complete_api->OnComplete(async_complete_api, stored_block, err); }

////////////// AsyncGetExistingContentAPI

uint64_t Longtail_GetAsyncGetExistingContentAPISize()
{
    return sizeof(struct Longtail_AsyncGetExistingContentAPI);
}

struct Longtail_AsyncGetExistingContentAPI* Longtail_MakeAsyncGetExistingContentAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetExistingContent_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncGetExistingContentAPI* api = (struct Longtail_AsyncGetExistingContentAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncGetExistingContent_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_StoreIndex* store_index, int err) { async_complete_api->OnComplete(async_complete_api, store_index, err); }

////////////// AsyncPruneBlocksAPI

uint64_t Longtail_GetAsyncPruneBlocksAPISize()
{
    return sizeof(struct Longtail_AsyncPruneBlocksAPI);
}

struct Longtail_AsyncPruneBlocksAPI* Longtail_MakeAsyncPruneBlocksAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncPruneBlocks_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncPruneBlocksAPI* api = (struct Longtail_AsyncPruneBlocksAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncPruneBlocks_OnComplete(struct Longtail_AsyncPruneBlocksAPI* async_complete_api, uint32_t pruned_block_count, int err) { async_complete_api->OnComplete(async_complete_api, pruned_block_count, err); }

////////////// Longtail_AsyncPreflightStartedAPI

uint64_t Longtail_GetAsyncPreflightStartedAPISize()
{
    return sizeof(struct Longtail_AsyncPreflightStartedAPI);
}

struct Longtail_AsyncPreflightStartedAPI* Longtail_MakeAsyncPreflightStartedAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncPreflightStarted_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncPreflightStartedAPI* api = (struct Longtail_AsyncPreflightStartedAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;

}

void Longtail_AsyncPreflightStarted_OnComplete(struct Longtail_AsyncPreflightStartedAPI* async_complete_api, uint32_t block_count, TLongtail_Hash* block_hashes, int err) {async_complete_api->OnComplete(async_complete_api, block_count, block_hashes, err); }

////////////// AsyncFlushAPI

uint64_t Longtail_GetAsyncFlushAPISize()
{
    return sizeof(struct Longtail_AsyncFlushAPI);
}

struct Longtail_AsyncFlushAPI* Longtail_MakeAsyncFlushAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncFlush_OnCompleteFunc on_complete_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(on_complete_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncFlushAPI* api = (struct Longtail_AsyncFlushAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncFlush_OnComplete(struct Longtail_AsyncFlushAPI* async_complete_api, int err) { async_complete_api->OnComplete(async_complete_api, err); }

////////////// BlockStoreAPI

uint64_t Longtail_GetBlockStoreAPISize()
{
    return sizeof(struct Longtail_BlockStoreAPI);
}

struct Longtail_BlockStoreAPI* Longtail_MakeBlockStoreAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_BlockStore_PutStoredBlockFunc put_stored_block_func,
    Longtail_BlockStore_PreflightGetFunc preflight_get_func,
    Longtail_BlockStore_GetStoredBlockFunc get_stored_block_func,
    Longtail_BlockStore_GetExistingContentFunc get_existing_content_func,
    Longtail_BlockStore_PruneBlocksFunc prune_blocks_func,
    Longtail_BlockStore_GetStatsFunc get_stats_func,
    Longtail_BlockStore_FlushFunc flush_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(put_stored_block_func, "%p"),
        LONGTAIL_LOGFIELD(preflight_get_func, "%p"),
        LONGTAIL_LOGFIELD(get_stored_block_func, "%p"),
        LONGTAIL_LOGFIELD(get_existing_content_func, "%p"),
        LONGTAIL_LOGFIELD(prune_blocks_func, "%p"),
        LONGTAIL_LOGFIELD(get_stats_func, "%p"),
        LONGTAIL_LOGFIELD(flush_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_BlockStoreAPI* api = (struct Longtail_BlockStoreAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->PutStoredBlock = put_stored_block_func;
    api->PreflightGet = preflight_get_func;
    api->GetStoredBlock = get_stored_block_func;
    api->GetExistingContent = get_existing_content_func;
    api->PruneBlocks = prune_blocks_func;
    api->GetStats = get_stats_func;
    api->Flush = flush_func;
    return api;
}

int Longtail_BlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api) { return block_store_api->PutStoredBlock(block_store_api, stored_block, async_complete_api); }
int Longtail_BlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api) { return block_store_api->PreflightGet(block_store_api, chunk_count, chunk_hashes, optional_async_complete_api); }
int Longtail_BlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api) { return block_store_api->GetStoredBlock(block_store_api, block_hash, async_complete_api); }
int Longtail_BlockStore_GetExistingContent(struct Longtail_BlockStoreAPI* block_store_api, uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_AsyncGetExistingContentAPI* async_complete_api) { return block_store_api->GetExistingContent(block_store_api, chunk_count, chunk_hashes, min_block_usage_percent, async_complete_api); }
int Longtail_BlockStore_PruneBlocks(struct Longtail_BlockStoreAPI* block_store_api, uint32_t block_keep_count, const TLongtail_Hash* block_keep_hashes, struct Longtail_AsyncPruneBlocksAPI* async_complete_api) { return block_store_api->PruneBlocks(block_store_api, block_keep_count, block_keep_hashes, async_complete_api);}
int Longtail_BlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats) { return block_store_api->GetStats(block_store_api, out_stats); }
int Longtail_BlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api) {return block_store_api->Flush(block_store_api, async_complete_api); }

static struct Longtail_Monitor Monitor_private = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

#define LONGTAIL_MONTITOR_BLOCK_PREPARE(store_index, block_index) if (Monitor_private.BlockPrepare) {Monitor_private.BlockPrepare(store_index, block_index);}
#define LONGTAIL_MONTITOR_BLOCK_LOAD(store_index, block_index) if (Monitor_private.BlockLoad) {Monitor_private.BlockLoad(store_index, block_index);}
#define LONGTAIL_MONTITOR_BLOCK_LOADED(store_index, block_index, err) if (Monitor_private.BlockLoaded) {Monitor_private.BlockLoaded(store_index, block_index, err);}
#define LONGTAIL_MONTITOR_BLOCK_COMPLETE(store_index, block_index, err) if (Monitor_private.BlockLoadComplete) {Monitor_private.BlockLoadComplete(store_index, block_index, err);}

#define LONGTAIL_MONITOR_ASSET_REMOVE(version_index, asset_index, err) if (Monitor_private.AssetRemove) {Monitor_private.AssetRemove(version_index, asset_index, err);}
#define LONGTAIL_MONTITOR_ASSET_OPEN(version_index, asset_index, err) if (Monitor_private.AssetOpen) {Monitor_private.AssetOpen(version_index, asset_index, err);}
#define LONGTAIL_MONTITOR_ASSET_WRITE(store_index, version_index, asset_index, write_offset, size, chunk_index, chunk_index_in_block, chunk_count_in_block, block_index, block_data_offset, err) if (Monitor_private.AssetWrite) {Monitor_private.AssetWrite(store_index, version_index, asset_index, write_offset, size, chunk_index, chunk_index_in_block, chunk_count_in_block, block_index, block_data_offset, err);}

#define LONGTAIL_MONTITOR_CHUNK_READ(store_index, version_index, block_index, chunk_index, chunk_index_in_block, err) if (Monitor_private.ChunkRead) {Monitor_private.ChunkRead(store_index, version_index, block_index, chunk_index, chunk_index_in_block, err);}

#define LONGTAIL_MONTITOR_BLOCK_COMPOSE(store_index, block_index) if (Monitor_private.BlockCompose) {Monitor_private.BlockCompose(store_index, block_index);}
#define LONGTAIL_MONTITOR_ASSET_READ(store_index, version_index, asset_index, read_offset, size, chunk_hash, block_index, block_data_offset, err) if (Monitor_private.AssetRead) {Monitor_private.AssetRead(store_index, version_index, asset_index, read_offset, size, chunk_hash, block_index, block_data_offset, err);}
#define LONGTAIL_MONTITOR_ASSET_CLOSE(version_index, asset_index) if (Monitor_private.AssetClose) {Monitor_private.AssetClose(version_index, asset_index);}
#define LONGTAIL_MONITOR_BLOCK_SAVE(store_index, block_index, block_size) if (Monitor_private.BlockSave) {Monitor_private.BlockSave(store_index, block_index, block_size);}
#define LONGTAIL_MONITOR_BLOCK_SAVED(store_index, block_index, err) if (Monitor_private.BlockSaved) {Monitor_private.BlockSaved(store_index, block_index, err);}

void Longtail_SetMonitor(struct Longtail_Monitor* monitor)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(monitor, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, monitor != 0, return)
    LONGTAIL_VALIDATE_INPUT(ctx, monitor->StructSize == (uint64_t)sizeof(struct Longtail_Monitor), return)

    Monitor_private = *monitor;
}

Longtail_Assert Longtail_Assert_private = 0;

void Longtail_SetAssert(Longtail_Assert assert_func)
{
#if defined(LONGTAIL_ASSERTS)
    Longtail_Assert_private = assert_func;
#else  // defined(LONGTAIL_ASSERTS)
    (void)assert_func;
#endif // defined(LONGTAIL_ASSERTS)
}

void Longtail_DisposeAPI(struct Longtail_API* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    if (api->Dispose)
    {
        api->Dispose(api);
    }
}

static Longtail_ReAlloc_Func Longtail_ReAlloc_private = 0;
static Longtail_Free_Func Free_private = 0;

static void* Longtail_SetAllocWrapper_private(const char* context, size_t s)
{
    return Longtail_ReAlloc_private(context, 0, s);
}

void Longtail_SetReAllocAndFree(Longtail_ReAlloc_Func alloc, Longtail_Free_Func Longtail_Free)
{
    Longtail_ReAlloc_private = alloc;
    Free_private = Longtail_Free;
}

void* Longtail_Alloc(const char* context, size_t s)
{
    return Longtail_ReAlloc(context, 0, s);
}

void* Longtail_ReAlloc(const char* context, void* old, size_t s)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(s, "%" PRIu64),
        LONGTAIL_LOGFIELD(old, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    void* mem = Longtail_ReAlloc_private ? Longtail_ReAlloc_private(context, old, s) : realloc(old, s);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "%s failed with %d", Longtail_ReAlloc_private ? "Longtail_ReAlloc_private" : "realloc()", ENOMEM);
        return 0;
    }
    return mem;
}

void Longtail_Free(void* p)
{
    Free_private ? Free_private(p) : free(p);
}

#if !defined(LONGTAIL_LOG_LEVEL)
    #define LONGTAIL_LOG_LEVEL   LONGTAIL_LOG_LEVEL_WARNING
#endif

static Longtail_Log Longtail_Log_private = 0;
static void* Longtail_LogContext = 0;
static int Longtail_LogLevel_private = LONGTAIL_LOG_LEVEL;

void Longtail_SetLog(Longtail_Log log_func, void* context)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(log_func, "%p"),
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    Longtail_Log_private = log_func;
    Longtail_LogContext = context;
}

void Longtail_SetLogLevel(int level)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(level, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    Longtail_LogLevel_private = level;
}

int Longtail_GetLogLevel()
{
    return Longtail_LogLevel_private;
}

static uint32_t Longtail_MakeLogFields(struct Longtail_LogContextFmt_Private* log_context, struct Longtail_LogField** fields_ptr, uint32_t* fields_left_ptr, char** char_buffer_ptr, uint32_t* chars_left_ptr)
{
    if (log_context == 0)
    {
        return 0;
    }
    uint32_t count = Longtail_MakeLogFields(log_context->parent_context, fields_ptr, fields_left_ptr, char_buffer_ptr, chars_left_ptr);
    struct Longtail_LogField* field = *fields_ptr;
    uint32_t fields_left = *fields_left_ptr;
    char* char_buffer = *char_buffer_ptr;
    uint32_t chars_left = *chars_left_ptr;
    for (size_t f = 0; (f < log_context->field_count) && (f < fields_left > 0) && (chars_left > 1); ++f)
    {
        struct Longtail_LogFieldFmt_Private* log_field_fmt = &log_context->fields[f];
        field->name = log_field_fmt->name;
        char_buffer[0] = '\0';
        int chars_used = snprintf(char_buffer, chars_left, log_field_fmt->fmt, log_field_fmt->value) + 1;
        if (chars_used == chars_left)
        {
            char_buffer[chars_used] = 0;
        }
        chars_left -= chars_used;
        field->value = char_buffer;
        char_buffer += ((int64_t)chars_used) + 1;
        --fields_left;
        ++count;
        ++field;
    }
    *fields_ptr = field;
    *fields_left_ptr = fields_left;
    *char_buffer_ptr = char_buffer;
    *chars_left_ptr = chars_left;
    return count;
}

void Longtail_CallLogger(const char* file, const char* function, int line, struct Longtail_LogContextFmt_Private* log_context_fmt, int level, const char* fmt, ...)
{
    LONGTAIL_FATAL_ASSERT(0, fmt != 0, return)
    if (level < Longtail_LogLevel_private || (level == LONGTAIL_LOG_LEVEL_OFF))
    {
        return;
    }
    if (!Longtail_Log_private)
    {
        return;
    }
    char buffer[2048];
    struct Longtail_LogField tmp_fields[32];
    struct Longtail_LogField* fields = &tmp_fields[0];
    char* char_buffer = &buffer[0];
    uint32_t fields_left = 32;
    uint32_t chars_left = 2048;
    uint32_t field_count = Longtail_MakeLogFields(log_context_fmt, &fields, &fields_left, &char_buffer, &chars_left);
    struct Longtail_LogContext log_context;
    log_context.context = Longtail_LogContext;
    log_context.field_count = field_count;
    log_context.file = file;
    log_context.function = function;
    log_context.fields = &tmp_fields[0];
    log_context.field_count = (int)field_count;
    log_context.line = line;
    log_context.level = level;
    va_list argptr;
    va_start(argptr, fmt);
    vsnprintf(char_buffer, chars_left, fmt, argptr);
    va_end(argptr);
    Longtail_Log_private(&log_context, char_buffer);
}

char* Longtail_Strdup(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    char* r = (char*)Longtail_Alloc("Strdup", strlen(path) + 1);
    if (!r)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", path, ENOMEM)
        return 0;
    }
    strcpy(r, path);
    return r;
}

int Longtail_RunJobsBatched(
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    uint32_t total_job_count,
    Longtail_JobAPI_JobFunc* job_funcs,
    void** job_ctxs,
    uint32_t* out_jobs_submitted)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(total_job_count, "%u"),
        LONGTAIL_LOGFIELD(job_funcs, "%p"),
        LONGTAIL_LOGFIELD(job_ctxs, "%p"),
        LONGTAIL_LOGFIELD(out_jobs_submitted, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_funcs != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, total_job_count > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_ctxs != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_jobs_submitted != 0, return EINVAL)

    * out_jobs_submitted = 0;
    uint32_t max_job_batch_count = 0;
    int err = job_api->GetMaxBatchCount(job_api, &max_job_batch_count, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->GetMaxBatchCount() failed with %d", err)
        return err;
    }

    uint32_t batch_adjust_count = job_api->GetWorkerCount(job_api) + 1;

    // Adjust how many we submit each time so we get some overlap when tasks free up so we don't stall on each batch
    uint32_t smaller_job_batch_count = batch_adjust_count < max_job_batch_count ? max_job_batch_count - batch_adjust_count : max_job_batch_count;

    Longtail_JobAPI_Group job_group = 0;
    err = job_api->ReserveJobs(job_api, (uint32_t)total_job_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReserveJobs() failed with %d", err)
        return err;
    }

    uint32_t submitted_count = 0;
    while (submitted_count < total_job_count)
    {
        if (optional_cancel_api)
        {
            if (optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
            {
                job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
                return ECANCELED;
            }
        }
        uint32_t submit_count = total_job_count - submitted_count;
        uint32_t max_submit_count = submitted_count == 0 ? max_job_batch_count : smaller_job_batch_count;
        if (submit_count > max_submit_count)
        {
            submit_count = max_submit_count;
        }

        Longtail_JobAPI_Jobs write_job;
        err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, (uint32_t)submit_count, &job_funcs[submitted_count], &job_ctxs[submitted_count], 0, &write_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
            job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
            return err;
        }
        err = job_api->ReadyJobs(job_api, (uint32_t)submit_count, write_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReadyJobs() failed with %d", err)
            job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
            return err;
        }

        submitted_count += submit_count;
        *out_jobs_submitted = submitted_count;
    }

    err = job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->WaitForAllJobs() failed with %d", err)
        return err;
    }

    return 0;
}




//////////////////////////////// Longtail_LookupTable

struct Longtail_LookupTable
{
    uint32_t  m_BucketCount;

    uint32_t m_Capacity;
    uint32_t m_Count;

    uint32_t* m_Buckets;
    uint64_t* m_Keys;
    uint32_t* m_Values;
    uint32_t* m_NextIndex;
};

int LongtailPrivate_LookupTable_Put(struct Longtail_LookupTable* lut, uint64_t key, uint32_t value)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(lut, "%p"),
        LONGTAIL_LOGFIELD(key, "%" PRIu64),
        LONGTAIL_LOGFIELD(value, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    LONGTAIL_FATAL_ASSERT(ctx, lut->m_Count < lut->m_Capacity, return ENOMEM)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint32_t entry_index = lut->m_Count++;
    lut->m_Keys[entry_index] = key;
    lut->m_Values[entry_index] = value;

    uint32_t bucket_index = (uint32_t)(key & (lut->m_BucketCount - 1));
    uint32_t* buckets = lut->m_Buckets;
    uint32_t index = buckets[bucket_index];
    if (index == 0xffffffffu)
    {
        buckets[bucket_index] = entry_index;
        return 0;
    }
    uint32_t* next_index = lut->m_NextIndex;
    uint32_t next = next_index[index];
    while (next != 0xffffffffu)
    {
        index = next;
        next = next_index[index];
    }

    next_index[index] = entry_index;
    return 0;
}

uint32_t* LongtailPrivate_LookupTable_PutUnique(struct Longtail_LookupTable* lut, uint64_t key, uint32_t value)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(lut, "%p"),
        LONGTAIL_LOGFIELD(key, "%" PRIu64),
        LONGTAIL_LOGFIELD(value, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint32_t bucket_index = (uint32_t)(key & (lut->m_BucketCount - 1));
    uint32_t* buckets = lut->m_Buckets;
    uint32_t index = buckets[bucket_index];
    if (index == 0xffffffffu)
    {
        LONGTAIL_FATAL_ASSERT(ctx, lut->m_Count < lut->m_Capacity, return 0)
        uint32_t entry_index = lut->m_Count++;
        lut->m_Keys[entry_index] = key;
        lut->m_Values[entry_index] = value;
        buckets[bucket_index] = entry_index;
        return 0;
    }
    if (lut->m_Keys[index] == key)
    {
        return &lut->m_Values[index];
    }
    uint64_t* keys = lut->m_Keys;
    uint32_t* next_index = lut->m_NextIndex;
    uint32_t next = next_index[index];
    while (next != 0xffffffffu)
    {
        index = next;
        if (keys[index] == key)
        {
            return &lut->m_Values[index];
        }
        next = next_index[index];
    }

    LONGTAIL_FATAL_ASSERT(ctx, lut->m_Count < lut->m_Capacity, return 0)
    uint32_t entry_index = lut->m_Count++;
    keys[entry_index] = key;
    lut->m_Values[entry_index] = value;
    next_index[index] = entry_index;
    return 0;
}

uint32_t* LongtailPrivate_LookupTable_Get(const struct Longtail_LookupTable* lut, uint64_t key)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(lut, "%p"),
        LONGTAIL_LOGFIELD(key, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint32_t bucket_index = (uint32_t)(key & (lut->m_BucketCount - 1));
    uint32_t index = lut->m_Buckets[bucket_index];
    const uint64_t* keys = lut->m_Keys;
    const uint32_t* next_index = lut->m_NextIndex;
    while (index != 0xffffffffu)
    {
        if (keys[index] == key)
        {
            return &lut->m_Values[index];
        }
        index = next_index[index];
    }
    return 0;
}

uint32_t LongtailPrivate_LookupTable_GetSpaceLeft(const struct Longtail_LookupTable* lut)
{
    return lut->m_Capacity - lut->m_Count;
}

static uint32_t GetLookupTableSize(uint32_t capacity)
{
    uint32_t table_size = 1;
    while (table_size < (capacity / 4))
    {
        table_size <<= 1;
    }
    return table_size;
}

size_t LongtailPrivate_LookupTable_GetSize(uint32_t capacity)
{
    uint32_t table_size = GetLookupTableSize(capacity);
    size_t mem_size = sizeof(struct Longtail_LookupTable) +
        sizeof(uint32_t) * table_size +
        sizeof(uint64_t) * capacity +
        sizeof(uint32_t) * capacity +
        sizeof(uint32_t) * capacity;
    return mem_size;
}

struct Longtail_LookupTable* LongtailPrivate_LookupTable_Create(void* mem, uint32_t capacity, struct Longtail_LookupTable* optional_source_entries)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(capacity, "%u"),
        LONGTAIL_LOGFIELD(optional_source_entries, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct Longtail_LookupTable* lut = (struct Longtail_LookupTable*)mem;
    memset(lut, 0xff, LongtailPrivate_LookupTable_GetSize(capacity));
    uint32_t table_size = GetLookupTableSize(capacity);
    lut->m_BucketCount = table_size;
    lut->m_Capacity = capacity;
    lut->m_Count = 0;
    lut->m_Buckets = (uint32_t*)&lut[1];
    lut->m_Keys = (uint64_t*)&lut->m_Buckets[table_size];
    lut->m_Values = (uint32_t*)&lut->m_Keys[capacity];
    lut->m_NextIndex = &lut->m_Values[capacity];

//    memset(lut->m_Buckets, 0xff, sizeof(uint32_t) * table_size);
//    memset(lut->m_NextIndex, 0xff, sizeof(uint32_t) * capacity);

    if (optional_source_entries == 0)
    {
        return lut;
    }
    for (uint32_t i = 0; i < optional_source_entries->m_BucketCount; ++i)
    {
        if (optional_source_entries->m_Buckets[i] != 0xffffffffu)
        {
            uint32_t index = optional_source_entries->m_Buckets[i];
            while (index != 0xffffffffu)
            {
                uint64_t key = optional_source_entries->m_Keys[index];
                uint32_t value = optional_source_entries->m_Values[index];
                LongtailPrivate_LookupTable_Put(lut, key, value);
                index = optional_source_entries->m_NextIndex[index];
            }
        }
    }
    return lut;
}

static int IsDirPath(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

static int GetPathHashWithLength(struct Longtail_HashAPI* hash_api, const char* path, uint32_t pathlen, TLongtail_Hash* out_hash)
{
    uint64_t hash;
    int err = hash_api->HashBuffer(hash_api, pathlen, (void*)path, &hash);
    if (err)
    {
        return err;
    }
    *out_hash = (TLongtail_Hash)hash;
    return 0;
}

int LongtailPrivate_GetPathHash(struct Longtail_HashAPI* hash_api, const char* path, TLongtail_Hash* out_hash)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_hash, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_hash != 0, return EINVAL)
    uint32_t pathlen = (uint32_t)strlen(path);
    int err = GetPathHashWithLength(hash_api, path, pathlen, out_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_api->HashBuffer() failed with %d", err)
        return err;
    }
    return 0;
}

int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path != 0, return EINVAL)

    char* parent_path = storage_api->GetParentPath(storage_api, path);
    if (parent_path == 0)
    {
        Longtail_Free(parent_path);
        return 0;
    }

    if (storage_api->IsDir(storage_api, parent_path))
    {
        Longtail_Free(parent_path);
        return 0;
    }

    int err = EnsureParentPathExists(storage_api, parent_path);
    if (err != 0 && err != EEXIST)
    {
        Longtail_Free(parent_path);
        return err;
    }

    err = storage_api->CreateDir(storage_api, parent_path);
    Longtail_Free(parent_path);
    if (err == 0 || err == EEXIST)
    {
        return 0;
    }
    return err;
}

static size_t GetFileInfosSize(uint32_t path_count, uint32_t path_data_size)
{
    return sizeof(struct Longtail_FileInfos) +
        sizeof(uint32_t) * path_count +    // m_Permissions[path_count]
        sizeof(uint32_t) * path_count +    // m_Offsets[path_count]
        sizeof(uint64_t) * path_count +    // m_Sizes[path_count]
        path_data_size;
};

static struct Longtail_FileInfos* CreateFileInfos(uint32_t path_count, uint32_t path_data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path_count, "%u"),
        LONGTAIL_LOGFIELD(path_data_size, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, (path_count == 0 && path_data_size == 0) || (path_count > 0 && path_data_size > path_count), return 0)
    size_t file_infos_size = GetFileInfosSize(path_count, path_data_size);
    struct Longtail_FileInfos* file_infos = (struct Longtail_FileInfos*)Longtail_Alloc("CreateFileInfos", file_infos_size);
    if (!file_infos)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    char* p = (char*)&file_infos[1];
    file_infos->m_Count = 0;
    file_infos->m_PathDataSize = 0;
    file_infos->m_Sizes = (uint64_t*)p;
    p += sizeof(uint64_t) * path_count;
    file_infos->m_PathStartOffsets = (uint32_t*)p;
    p += sizeof(uint32_t) * path_count;
    file_infos->m_Permissions = (uint16_t*)p;
    p += sizeof(uint16_t) * path_count;
    file_infos->m_PathData = p;
    return file_infos;
};

int LongtailPrivate_MakeFileInfos(
    uint32_t path_count,
    const char* const* path_names,
    const uint64_t* file_sizes,
    const uint16_t* file_permissions,
    struct Longtail_FileInfos** out_file_infos)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path_count, "%u"),
        LONGTAIL_LOGFIELD(path_names, "%p"),
        LONGTAIL_LOGFIELD(file_sizes, "%p"),
        LONGTAIL_LOGFIELD(file_permissions, "%p"),
        LONGTAIL_LOGFIELD(out_file_infos, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, (path_count == 0 && path_names == 0) || (path_count > 0 && path_names != 0), return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, (path_count == 0 && file_sizes == 0) || (path_count > 0 && file_sizes != 0), return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, (path_count == 0 && file_permissions == 0) || (path_count > 0 && file_permissions != 0), return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_file_infos != 0, return 0)

    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    struct Longtail_FileInfos* file_infos = CreateFileInfos(path_count, name_data_size);
    if (file_infos == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateFileInfos() failed with %d", ENOMEM)
        return ENOMEM;
    }
    uint32_t offset = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        uint32_t length = (uint32_t)strlen(path_names[i]) + 1;
        file_infos->m_Sizes[i] = file_sizes[i];
        file_infos->m_Permissions[i] = file_permissions[i];
        file_infos->m_PathStartOffsets[i] = offset;
        memmove(&file_infos->m_PathData[offset], path_names[i], length);
        offset += length;
    }
    file_infos->m_PathDataSize = offset;
    file_infos->m_Count = path_count;
    *out_file_infos = file_infos;
    return 0;
}

struct ScanFolderResult
{
    uint32_t m_PropertyCount;
    struct Longtail_StorageAPI_EntryProperties* m_Properties;
};

static int ScanFolder(
    struct Longtail_StorageAPI* storage_api,
    const char* root_path,
    const char* folder_sub_path,
    struct ScanFolderResult** out_result)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(folder_sub_path, "%p"),
        LONGTAIL_LOGFIELD(out_result, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_result != 0, return EINVAL)

    int IsEmptySubFolderPath = (folder_sub_path == 0) || (strlen(folder_sub_path) == 0) || (strcmp(folder_sub_path, ".") == 0);

    const char* full_path = IsEmptySubFolderPath ? root_path : storage_api->ConcatPath(storage_api, root_path, folder_sub_path);
    if (full_path == 0)
    {
        return ENOMEM;
    }
    Longtail_StorageAPI_HIterator fs_iterator;
    int err = storage_api->StartFind(storage_api, full_path, &fs_iterator);
    if (err == ENOENT)
    {
        if (full_path != root_path)
        {
            Longtail_Free((void*)full_path);
            full_path = 0;
        }
        size_t result_mem_size = sizeof(struct ScanFolderResult);
        void* result_mem = Longtail_Alloc("ScanFolder", result_mem_size);
        if (result_mem == 0)
        {
            err = ENOMEM;
        }
        struct ScanFolderResult* result = (struct ScanFolderResult*)result_mem;
        result->m_PropertyCount = 0;
        result->m_Properties = 0;
        *out_result = result;
        return 0;
    }
    else if (err)
    {
        if (full_path != root_path)
        {
            Longtail_Free((void*)full_path);
            full_path = 0;
        }
        return err;
    }
    struct Longtail_StorageAPI_EntryProperties* properties_array = 0;
    size_t name_data_size = 0;
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Scanning `%s`", full_path)
    while (err == 0)
    {
        struct Longtail_StorageAPI_EntryProperties properties;
        err = storage_api->GetEntryProperties(storage_api, fs_iterator, &properties);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "storage_api->GetEntryProperties() failed with %d", err)
            break;
        }

        char* path = IsEmptySubFolderPath ? Longtail_Strdup(properties.m_Name) : storage_api->ConcatPath(storage_api, folder_sub_path, properties.m_Name);
        if (path == 0)
        {
            err = ENOMEM;
            break;
        }

        arrput(properties_array, properties);
        name_data_size += strlen(path) + 1;
        ptrdiff_t count = arrlen(properties_array);
        properties_array[count - 1].m_Name = path;

        err = storage_api->FindNext(storage_api, fs_iterator);
        if (err == ENOENT)
        {
            err = 0;
            break;
        }
    }
    storage_api->CloseFind(storage_api, fs_iterator);

    if (full_path != root_path)
    {
        Longtail_Free((void*)full_path);
        full_path = 0;
    }

    ptrdiff_t count = arrlen(properties_array);
    if (err == 0)
    {
        // Build result
        size_t result_mem_size = sizeof(struct ScanFolderResult) +
            (sizeof(struct Longtail_StorageAPI_EntryProperties) * count) +
            name_data_size;
        void* result_mem = Longtail_Alloc("ScanFolder", result_mem_size);
        if (result_mem == 0)
        {
            err = ENOMEM;
        }
        else
        {
            struct ScanFolderResult* result = (struct ScanFolderResult*)result_mem;
            result->m_PropertyCount = (uint32_t)count;
            uint8_t* p = (uint8_t*)&result[1];
            result->m_Properties = (struct Longtail_StorageAPI_EntryProperties*)p;
            p += sizeof(struct Longtail_StorageAPI_EntryProperties) * count;
            char* name_data_ptr = (char*)p;
            memcpy(result->m_Properties, properties_array, sizeof(struct Longtail_StorageAPI_EntryProperties) * count);
            for (ptrdiff_t index = 0; index < count; index++)
            {
                strcpy(name_data_ptr, properties_array[index].m_Name);
                result->m_Properties[index].m_Name = name_data_ptr;
                name_data_ptr += strlen(name_data_ptr) + 1;
            }
            *out_result = result;
        }
    }
    {
        while (count--)
        {
            Longtail_Free((void*)properties_array[count].m_Name);
        }
        arrfree(properties_array);
    }
    return err;
}

struct ScanFolderJobContext
{
    struct Longtail_StorageAPI* m_StorageAPI;
    const char* m_RootPath;
    const char* m_FolderSubPath;
    struct ScanFolderResult** m_Result;
};

static int ScanFolderJob(void* context, uint32_t job_id, int detected_error)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)
    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "ScanFolderJob aborted due to previous error %d", detected_error)
        return 0;
    }
    struct ScanFolderJobContext* job = (struct ScanFolderJobContext*)context;
    int err = ScanFolder(job->m_StorageAPI, job->m_RootPath, job->m_FolderSubPath, job->m_Result);
    return err;
}

static SORTFUNC(SortScannedPaths)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    struct ScanFolderResult** results = (struct ScanFolderResult**)context;

    uint64_t a_index = *(uint64_t*)a_ptr;
    uint64_t b_index = *(uint64_t*)b_ptr;

    uint32_t a_result_index = (uint32_t)((a_index >> 32) & 0xffffffffu);
    uint32_t a_property_index = (uint32_t)(a_index & 0xffffffffu);
    uint32_t b_result_index = (uint32_t)((b_index >> 32) & 0xffffffffu);
    uint32_t b_property_index = (uint32_t)(b_index & 0xffffffffu);
    const char* a_name = results[a_result_index]->m_Properties[a_property_index].m_Name;
    const char* b_name = results[b_result_index]->m_Properties[b_property_index].m_Name;
    return strcmp(a_name, b_name);
}

static int IncludeFoundFile(const char* root_path, struct Longtail_PathFilterAPI* optional_path_filter_api, const struct Longtail_StorageAPI_EntryProperties* properties)
{
    if (optional_path_filter_api == 0)
    {
        return 1;
    }
    const char* name = strrchr(properties->m_Name, '/');
    if (name == 0)
    {
        name = properties->m_Name;
    }
    else
    {
        name = &name[1];
    }
    if (optional_path_filter_api->Include(optional_path_filter_api, root_path, properties->m_Name, name, properties->m_IsDir, properties->m_Size, properties->m_Permissions))
    {
        return 1;
    }
    return 0;
}

int Longtail_GetFilesRecursively2(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_JobAPI* optional_job_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(optional_job_api, "%p"),
        LONGTAIL_LOGFIELD(optional_path_filter_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(out_file_infos, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_file_infos != 0, return EINVAL)

    struct ScanFolderResult* root_result = 0;
    int err = ScanFolder(storage_api, root_path, 0, &root_result);
    if (err != 0)
    {
        err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ScanFolder() failed with %d", err)
        return err;
    }
    struct ScanFolderResult** results = 0;
    arrput(results, root_result);
    ptrdiff_t scan_index = 0;
    ptrdiff_t result_count = arrlen(results);
    while (scan_index < result_count)
    {
        if (optional_job_api)
        {
            ptrdiff_t job_count = 0;
            for (ptrdiff_t result_index = scan_index; result_index < result_count; result_index++)
            {
                struct ScanFolderResult* folder_result = results[result_index];
                for (uint32_t property_index = 0; property_index < folder_result->m_PropertyCount; property_index++)
                {
                    struct Longtail_StorageAPI_EntryProperties* properties = &folder_result->m_Properties[property_index];
                    if (properties->m_IsDir)
                    {
                        if (IncludeFoundFile(root_path, optional_path_filter_api, properties))
                        {
                            job_count++;
                            arrput(results, 0);
                        }
                    }
                }
            }
            if (job_count > 0)
            {
                size_t work_mem_size = (sizeof(struct ScanFolderJobContext) * job_count) +
                    (sizeof(Longtail_JobAPI_JobFunc) * job_count) +
                    (sizeof(void*) * job_count);
                void* work_mem = Longtail_Alloc("Longtail_GetFilesRecursively2", work_mem_size);
                if (!work_mem)
                {
                    err = ENOMEM;
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
                    break;
                }

                struct ScanFolderJobContext* job_contexts = (struct ScanFolderJobContext*)work_mem;
                Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)&job_contexts[job_count];
                void** ctxs = (void**)&funcs[job_count];

                ptrdiff_t job_index = 0;
                for (ptrdiff_t result_index = scan_index; result_index < result_count; result_index++)
                {
                    for (uint32_t property_index = 0; property_index < results[result_index]->m_PropertyCount; property_index++)
                    {
                        struct Longtail_StorageAPI_EntryProperties* properties = &results[result_index]->m_Properties[property_index];
                        if (properties->m_IsDir)
                        {
                            if (IncludeFoundFile(root_path, optional_path_filter_api, properties))
                            {
                                job_contexts[job_index].m_StorageAPI = storage_api;
                                job_contexts[job_index].m_RootPath = root_path;
                                job_contexts[job_index].m_FolderSubPath = properties->m_Name;
                                job_contexts[job_index].m_Result = &results[result_count + job_index];
                                funcs[job_index] = ScanFolderJob;
                                ctxs[job_index] = (void*)&job_contexts[job_index];
                                job_index++;
                            }
                        }
                    }
                }

                LONGTAIL_FATAL_ASSERT(ctx, job_index == job_count, return ENOMEM);

                uint32_t jobs_submitted = 0;
                int err = Longtail_RunJobsBatched(optional_job_api, 0, optional_cancel_api, optional_cancel_token, (uint32_t)job_count, funcs, ctxs, &jobs_submitted);
                if (err)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RunJobsBatched() failed with %d", err)
                    Longtail_Free(work_mem);
                    break;
                }
                Longtail_Free(work_mem);
            }
        }
        else
        {
            for (ptrdiff_t result_index = scan_index; result_index < result_count && err == 0; result_index++)
            {
                struct ScanFolderResult* folder_result = results[result_index];
                for (uint32_t property_index = 0; property_index < folder_result->m_PropertyCount; property_index++)
                {
                    struct Longtail_StorageAPI_EntryProperties* properties = &folder_result->m_Properties[property_index];
                    if (properties->m_IsDir)
                    {
                        if (IncludeFoundFile(root_path, optional_path_filter_api, properties))
                        {
                            struct ScanFolderResult* scan_result = 0;
                            err = ScanFolder(storage_api, root_path, properties->m_Name, &scan_result);
                            if (err)
                            {
                                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ScanFolder() failed with %d", err)
                                break;
                            }
                            arrput(results, scan_result);
                        }
                    }
                }
                if (err == 0 && optional_cancel_api && optional_cancel_token)
                {
                    if (optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token))
                    {
                        err = ECANCELED;
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_RunJobsBatched() failed with %d", err)
                        break;
                    }
                }
            }
        }
        scan_index = result_count;
        result_count = arrlen(results);
    }

    if (err == 0)
    {
        uint32_t max_path_count = 0;

        ptrdiff_t result_count = arrlen(results);
        for (ptrdiff_t result_index = 0; result_index < result_count; result_index++)
        {
            struct ScanFolderResult* folder_result = results[result_index];
            max_path_count += folder_result->m_PropertyCount;
        }

        uint64_t* sort_array = (uint64_t*)Longtail_Alloc("Longtail_GetFilesRecursively2", sizeof(uint64_t) * max_path_count);
        if (sort_array == 0)
        {
            err = ENOMEM;
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        }
        else
        {
            uint32_t path_count = 0;
            uint32_t path_data_size = 0;
            for (ptrdiff_t result_index = 0; result_index < result_count; result_index++)
            {
                struct ScanFolderResult* folder_result = results[result_index];
                for (uint32_t property_index = 0; property_index < folder_result->m_PropertyCount; property_index++)
                {
                    struct Longtail_StorageAPI_EntryProperties* properties = &folder_result->m_Properties[property_index];
                    if (IncludeFoundFile(root_path, optional_path_filter_api, properties))
                    {
                        path_data_size += (uint32_t)(strlen(properties->m_Name) + 1);
                        if (properties->m_IsDir)
                        {
                            path_data_size++;
                        }
                        sort_array[path_count++] = (((uint64_t)result_index) << 32) + property_index;
                    }
                }
            }

            QSORT(sort_array, path_count, sizeof(uint64_t), SortScannedPaths, (void*)results);

            struct Longtail_FileInfos* file_infos = CreateFileInfos(path_count, path_data_size);
            if (!file_infos)
            {
                err = ENOMEM;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateFileInfos() failed with %d", err)
            }
            else
            {
                file_infos->m_Count = path_count;
                file_infos->m_PathDataSize = path_data_size;

                uint32_t path_data_offset = 0;
                for (uint32_t path_index = 0; path_index < path_count; path_index++)
                {
                    uint32_t result_index = (uint32_t)((sort_array[path_index] >> 32) & 0xffffffffu);
                    struct ScanFolderResult* folder_result = results[result_index];
                    uint32_t property_index = (uint32_t)(sort_array[path_index] & 0xffffffffu);
                    struct Longtail_StorageAPI_EntryProperties* properties = &folder_result->m_Properties[property_index];

                    file_infos->m_PathStartOffsets[path_index] = path_data_offset;
                    file_infos->m_Sizes[path_index] = properties->m_Size;
                    file_infos->m_Permissions[path_index] = properties->m_Permissions;

                    strcpy(&file_infos->m_PathData[path_data_offset], properties->m_Name);
                    size_t path_len = strlen(properties->m_Name);
                    path_data_offset += (uint32_t)path_len;
                    if (properties->m_IsDir)
                    {
                        file_infos->m_PathData[path_data_offset++] = '/';
                        file_infos->m_PathData[path_data_offset] = '\0';
                    }
                    path_data_offset++;
                }

                *out_file_infos = file_infos;
            }
            Longtail_Free(sort_array);
        }
    }

    while (result_count--)
    {
        Longtail_Free(results[result_count]);
    }
    arrfree(results);
    return err;
}

int Longtail_GetFilesRecursively(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos)
{
    return Longtail_GetFilesRecursively2(
        storage_api,
        0,
        optional_path_filter_api,
        optional_cancel_api,
        optional_cancel_token,
        root_path,
        out_file_infos);
}

struct StorageChunkFeederContext
{
    struct Longtail_StorageAPI* m_StorageAPI;
    Longtail_StorageAPI_HOpenFile m_AssetFile;
    const char* m_AssetPath;
    uint64_t m_StartRange;
    uint64_t m_Size;
    uint64_t m_Offset;
};

static int StorageChunkFeederFunc(void* context, Longtail_ChunkerAPI_HChunker chunker, uint32_t requested_size, char* buffer, uint32_t* out_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(chunker, "%p"),
        LONGTAIL_LOGFIELD(requested_size, "%u"),
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunker != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, requested_size > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_size != 0, return EINVAL)
    struct StorageChunkFeederContext* c = (struct StorageChunkFeederContext*)context;
    uint64_t read_count = c->m_Size - c->m_Offset;
    if (read_count > 0)
    {
        if (requested_size < read_count)
        {
            read_count = requested_size;
        }
        int err = c->m_StorageAPI->Read(c->m_StorageAPI, c->m_AssetFile, c->m_StartRange + c->m_Offset, read_count, buffer);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "m_StorageAPI->Read() failed with %d", err)
            return err;
        }
        c->m_Offset += read_count;
    }
    *out_size = (uint32_t)read_count;
    return 0;
}

#define HASHJOB_INLINE_CHUNK_HASH_COUNT 16

struct HashJob
{
    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_HashAPI* m_HashAPI;
    struct Longtail_ChunkerAPI* m_ChunkerAPI;
    TLongtail_Hash* m_PathHash;
    uint32_t m_AssetIndex;
    const char* m_RootPath;
    const char* m_Path;
    uint64_t m_StartRange;
    uint64_t m_SizeRange;
    uint32_t* m_AssetChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkSizes;
    uint32_t m_TargetChunkSize;
    int m_EnableFileMap;

    TLongtail_Hash m_InlineChunkHashes[HASHJOB_INLINE_CHUNK_HASH_COUNT];
    uint32_t m_InlineChunkSizes[HASHJOB_INLINE_CHUNK_HASH_COUNT];
};

#define MIN_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size / 8) < min_chunk_size) ? min_chunk_size : (target_chunk_size / 8))
#define AVG_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size / 2) < min_chunk_size) ? min_chunk_size : (target_chunk_size / 2))
#define MAX_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size * 2) < min_chunk_size) ? min_chunk_size : (target_chunk_size * 2))

static int DynamicChunking(void* context, uint32_t job_id, int detected_error)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)
    struct HashJob* hash_job = (struct HashJob*)context;

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "DynamicChunking aborted due to previous error %d", detected_error)
        return 0;
    }

    if (hash_job->m_PathHash)
    {
        int err = LongtailPrivate_GetPathHash(hash_job->m_HashAPI, hash_job->m_Path, hash_job->m_PathHash);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_GetPathHash() failed with %d", err)
            return err;
        }
    }
    if (hash_job->m_SizeRange == 0)
    {
        *hash_job->m_AssetChunkCount = 0;
        return 0;
    }

    uint32_t chunk_count = 0;

    struct Longtail_StorageAPI* storage_api = hash_job->m_StorageAPI;
    char* path = storage_api->ConcatPath(storage_api, hash_job->m_RootPath, hash_job->m_Path);
    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        Longtail_Free(path);
        path = 0;
        return err;
    }

    uint64_t hash_size = hash_job->m_SizeRange;
    if (hash_size > 0)
    {
        uint32_t chunker_min_size;
        err = hash_job->m_ChunkerAPI->GetMinChunkSize(hash_job->m_ChunkerAPI, &chunker_min_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->GetMinChunkSize() failed with %d", err)
            storage_api->CloseFile(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            return err;
        }
        hash_job->m_ChunkHashes = hash_job->m_InlineChunkHashes;
        hash_job->m_ChunkSizes = hash_job->m_InlineChunkSizes;
        if (hash_size <= chunker_min_size)
        {
            char* buffer = 0;
            void* heap_buffer = 0;
            char stack_buffer[2048];
            if (chunker_min_size > sizeof(stack_buffer))
            {
                heap_buffer = Longtail_Alloc("DynamicChunking", (size_t)hash_size);
                if (!buffer)
                {
                    err = ENOMEM;
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
                        storage_api->CloseFile(storage_api, file_handle);
                    file_handle = 0;
                    Longtail_Free(path);
                    path = 0;
                    return err;
                }
                buffer = (char*)heap_buffer;
            }
            else
            {
                buffer = stack_buffer;
            }

            err = storage_api->Read(storage_api, file_handle, hash_job->m_StartRange, hash_size, buffer);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
                Longtail_Free(heap_buffer);
                heap_buffer = 0;
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                return err;
            }

            err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, (uint32_t)hash_size, buffer, &hash_job->m_ChunkHashes[0]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_HashAPI->HashBuffer() failed with %d", err)
                Longtail_Free(heap_buffer);
                heap_buffer = 0;
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                return err;
            }

            Longtail_Free(heap_buffer);
            heap_buffer = 0;

            hash_job->m_ChunkSizes[0] = (uint32_t)hash_size;

            chunk_count = 1;
        }
        else
        {
            uint32_t min_chunk_size = MIN_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);
            uint32_t avg_chunk_size = AVG_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);
            uint32_t max_chunk_size = MAX_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);

            uint32_t chunk_capacity = HASHJOB_INLINE_CHUNK_HASH_COUNT;

            Longtail_ChunkerAPI_HChunker chunker;
            err = hash_job->m_ChunkerAPI->CreateChunker(hash_job->m_ChunkerAPI, min_chunk_size, avg_chunk_size, max_chunk_size, &chunker);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->CreateChunker() failed with %d", err)
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                return err;
            }

            int use_read_file = 1;
            if (hash_job->m_EnableFileMap)
            {
                const uint8_t* mapped_ptr = 0;
                Longtail_StorageAPI_HFileMap mapping = 0;
                err = storage_api->MapFile(storage_api, file_handle, hash_job->m_StartRange, hash_size, &mapping, (const void**)&mapped_ptr);
                if (err == 0)
                {
                    use_read_file = 0;
                    const uint8_t* chunk_start_ptr = mapped_ptr;
                    const uint8_t* buffer_end_ptr = &mapped_ptr[hash_size];
                    while (chunk_start_ptr != buffer_end_ptr)
                    {
                        uint64_t bytes_left = buffer_end_ptr - chunk_start_ptr;
                        if (chunk_capacity == chunk_count)
                        {
                            uint64_t bytes_read = chunk_start_ptr - mapped_ptr;
                            uint64_t avg_size = bytes_read / chunk_count;
                            uint32_t new_chunk_capacity = chunk_count + 1 + (uint32_t)(bytes_left / avg_size);

                            void* new_output_mem = Longtail_Alloc("DynamicChunking", sizeof(TLongtail_Hash) * new_chunk_capacity + sizeof(uint32_t) * new_chunk_capacity);
                            if (!new_output_mem)
                            {
                                err = ENOMEM;
                                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
                                storage_api->UnMapFile(storage_api, mapping);
                                mapping = 0;
                                hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
                                chunker = 0;
                                storage_api->CloseFile(storage_api, file_handle);
                                file_handle = 0;
                                Longtail_Free(path);
                                path = 0;
                                return err;
                            }

                            TLongtail_Hash* new_chunk_hashes = (TLongtail_Hash*)new_output_mem;
                            uint32_t* new_chunk_sizes = (uint32_t*)&new_chunk_hashes[new_chunk_capacity];
                            if (hash_job->m_ChunkHashes)
                            {
                                memcpy(new_chunk_hashes, hash_job->m_ChunkHashes, sizeof(TLongtail_Hash) * chunk_count);
                                memcpy(new_chunk_sizes, hash_job->m_ChunkSizes, sizeof(uint32_t) * chunk_count);
                                if (hash_job->m_ChunkHashes != hash_job->m_InlineChunkHashes)
                                {
                                    Longtail_Free(hash_job->m_ChunkHashes);
                                }
                            }
                            hash_job->m_ChunkHashes = new_chunk_hashes;
                            hash_job->m_ChunkSizes = new_chunk_sizes;
                            chunk_capacity = new_chunk_capacity;
                        }

                        const uint8_t* next_chunk_start;
                        err = hash_job->m_ChunkerAPI->NextChunkFromBuffer(hash_job->m_ChunkerAPI, chunker, chunk_start_ptr, bytes_left, (const void**)&next_chunk_start);
                        if (err != 0)
                        {
                            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->NextChunkFromBuffer() failed with %d", err)
                            storage_api->UnMapFile(storage_api, mapping);
                            mapping = 0;
                            hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
                            chunker = 0;
                            storage_api->CloseFile(storage_api, file_handle);
                            file_handle = 0;
                            Longtail_Free(path);
                            path = 0;
                            return err;
                        }
                        uint32_t range_length = (uint32_t)(next_chunk_start - chunk_start_ptr);
                        err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, range_length, (const void*)chunk_start_ptr, &hash_job->m_ChunkHashes[chunk_count]);
                        if (err != 0)
                        {
                            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_HashAPI->HashBuffer() failed with %d", err)
                            storage_api->UnMapFile(storage_api, mapping);
                            mapping = 0;
                            hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
                            chunker = 0;
                            storage_api->CloseFile(storage_api, file_handle);
                            file_handle = 0;
                            Longtail_Free(path);
                            path = 0;
                            return err;
                        }
                        hash_job->m_ChunkSizes[chunk_count] = range_length;
                        ++chunk_count;
                        chunk_start_ptr = next_chunk_start;
                    }
                    storage_api->UnMapFile(storage_api, mapping);
                }
            }
            if (use_read_file)
            {
                struct StorageChunkFeederContext feeder_context =
                {
                    storage_api,
                    file_handle,
                    path,
                    hash_job->m_StartRange,
                    hash_size,
                    0
                };

                struct Longtail_Chunker_ChunkRange chunk_range;
                err = hash_job->m_ChunkerAPI->NextChunk(hash_job->m_ChunkerAPI, chunker, StorageChunkFeederFunc, &feeder_context, &chunk_range);
                while (err == 0)
                {
                    if (chunk_capacity == chunk_count)
                    {
                        uint64_t bytes_left = hash_size - (chunk_range.offset);
                        uint64_t avg_size = chunk_range.offset / chunk_count;
                        uint32_t new_chunk_capacity = chunk_count + 1 + (uint32_t)(bytes_left / avg_size);

                        void* new_output_mem = Longtail_Alloc("DynamicChunking", sizeof(TLongtail_Hash) * new_chunk_capacity + sizeof(uint32_t) * new_chunk_capacity);
                        if (!new_output_mem)
                        {
                            err = ENOMEM;
                            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
                                hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
                            chunker = 0;
                            storage_api->CloseFile(storage_api, file_handle);
                            file_handle = 0;
                            Longtail_Free(path);
                            path = 0;
                            return err;
                        }

                        TLongtail_Hash* new_chunk_hashes = (TLongtail_Hash*)new_output_mem;
                        uint32_t* new_chunk_sizes = (uint32_t*)&new_chunk_hashes[new_chunk_capacity];
                        if (hash_job->m_ChunkHashes)
                        {
                            memcpy(new_chunk_hashes, hash_job->m_ChunkHashes, sizeof(TLongtail_Hash) * chunk_count);
                            memcpy(new_chunk_sizes, hash_job->m_ChunkSizes, sizeof(uint32_t) * chunk_count);
                            if (hash_job->m_ChunkHashes != hash_job->m_InlineChunkHashes)
                            {
                                Longtail_Free(hash_job->m_ChunkHashes);
                            }
                        }
                        hash_job->m_ChunkHashes = new_chunk_hashes;
                        hash_job->m_ChunkSizes = new_chunk_sizes;
                        chunk_capacity = new_chunk_capacity;
                    }

                    err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, chunk_range.len, (void*)chunk_range.buf, &hash_job->m_ChunkHashes[chunk_count]);
                    if (err != 0)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_HashAPI->HashBuffer() failed with %d", err)
                            hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
                        chunker = 0;
                        storage_api->CloseFile(storage_api, file_handle);
                        file_handle = 0;
                        Longtail_Free(path);
                        path = 0;
                        return err;
                    }
                    hash_job->m_ChunkSizes[chunk_count] = chunk_range.len;

                    ++chunk_count;

                    err = hash_job->m_ChunkerAPI->NextChunk(hash_job->m_ChunkerAPI, chunker, StorageChunkFeederFunc, &feeder_context, &chunk_range);
                    if (err == ESPIPE)
                    {
                        err = 0;
                        break;
                    }
                    else if (err != 0)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->NextChunk() failed with %d", err)
                    }
                }
            }
            hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
        }
    }

    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;
    
    *hash_job->m_AssetChunkCount = chunk_count;

    Longtail_Free((char*)path);
    path = 0;

    return err;
}

struct ChunkAssetsData {
    uint32_t m_ChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkSizes;
    uint32_t* m_ChunkTags;
};

struct ChunkAssetsData* AllocChunkAssetsData(uint32_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    size_t size = sizeof(struct ChunkAssetsData) +
        sizeof(TLongtail_Hash) * chunk_count +
        sizeof(uint32_t) * chunk_count +
        sizeof(uint32_t) * chunk_count;
    void* mem = Longtail_Alloc("AllocChunkAssetsData", size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct ChunkAssetsData* chunk_assets_data = (struct ChunkAssetsData*)mem;
    chunk_assets_data->m_ChunkCount = chunk_count;
    chunk_assets_data->m_ChunkHashes = (TLongtail_Hash*)&chunk_assets_data[1];
    chunk_assets_data->m_ChunkSizes = (uint32_t*)&chunk_assets_data->m_ChunkHashes[chunk_count];
    chunk_assets_data->m_ChunkTags = (uint32_t*)&chunk_assets_data->m_ChunkSizes[chunk_count];
    return chunk_assets_data;
}

static int ChunkAssets(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_ChunkerAPI* chunker_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_path,
    const struct Longtail_FileInfos* file_infos,
    TLongtail_Hash* path_hashes,
    TLongtail_Hash* content_hashes,
    const uint32_t* optional_asset_tags,
    uint32_t* asset_chunk_start_index,
    uint32_t* asset_chunk_counts,
    uint32_t target_chunk_size,
    int enable_file_map,
    struct ChunkAssetsData** out_chunk_assets_data)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(file_infos, "%p"),
        LONGTAIL_LOGFIELD(path_hashes, "%p"),
        LONGTAIL_LOGFIELD(content_hashes, "%p"),
        LONGTAIL_LOGFIELD(optional_asset_tags, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_start_index, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_counts, "%p"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(out_chunk_assets_data, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, file_infos != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path_hashes != 0, return EINVAL)

    LONGTAIL_FATAL_ASSERT(ctx, content_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_chunk_start_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, target_chunk_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_chunk_assets_data != 0, return EINVAL)

    uint32_t asset_count = file_infos->m_Count;

    uint64_t max_hash_size = ((uint64_t)target_chunk_size) * 1024;
    uint32_t job_count = 0;

    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = file_infos->m_Sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);
        job_count += (uint32_t)asset_part_count;
    }

    if (job_count == 0)
    {
        return 0;
    }

    size_t work_mem_size = (sizeof(uint32_t) * job_count) +
        (sizeof(struct HashJob) * job_count) +
        (sizeof(Longtail_JobAPI_JobFunc) * job_count) +
        (sizeof(void*) * job_count);
    void* work_mem = Longtail_Alloc("ChunkAssets", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint32_t* tmp_job_chunk_counts = (uint32_t*)work_mem;
    struct HashJob* tmp_hash_jobs = (struct HashJob*)&tmp_job_chunk_counts[job_count];
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)&tmp_hash_jobs[job_count];
    void** ctxs = (void**)&funcs[job_count];

    uint32_t jobs_prepared = 0;
    uint64_t chunks_offset = 0;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = file_infos->m_Sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            struct HashJob* job = &tmp_hash_jobs[jobs_prepared];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_ChunkerAPI = chunker_api;
            job->m_RootPath = root_path;
            job->m_Path = &file_infos->m_PathData[file_infos->m_PathStartOffsets[asset_index]];
            job->m_PathHash = (job_part == 0) ? &path_hashes[asset_index] : 0;
            job->m_AssetIndex = asset_index;
            job->m_StartRange = range_start;
            job->m_SizeRange = job_size;
            job->m_AssetChunkCount = &tmp_job_chunk_counts[jobs_prepared];
            job->m_ChunkHashes = 0;
            job->m_ChunkSizes = 0;
            job->m_TargetChunkSize = target_chunk_size;
            job->m_EnableFileMap = 0;
            funcs[jobs_prepared] = DynamicChunking;
            ctxs[jobs_prepared] = job;
            ++jobs_prepared;
        }
    }

    LONGTAIL_FATAL_ASSERT(ctx, jobs_prepared == job_count, return ENOMEM);
    uint32_t jobs_submitted = 0;
    int err = Longtail_RunJobsBatched(job_api, progress_api, optional_cancel_api, optional_cancel_token, job_count, funcs, ctxs, &jobs_submitted);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RunJobsBatched() failed with %d", err)
        for (uint32_t i = 0; i < jobs_submitted; ++i)
        {
            if (tmp_hash_jobs[i].m_ChunkHashes != tmp_hash_jobs[i].m_InlineChunkHashes)
            {
                Longtail_Free(tmp_hash_jobs[i].m_ChunkHashes);
            }
        }
        Longtail_Free(work_mem);
        return err;
    }

    {
        uint32_t built_chunk_count = 0;
        for (uint32_t i = 0; i < job_count; ++i)
        {
            built_chunk_count += *tmp_hash_jobs[i].m_AssetChunkCount;
        }

        struct ChunkAssetsData* cad = AllocChunkAssetsData(built_chunk_count);
        if (!cad)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "AllocChunkAssetsData() failed with %d", ENOMEM)
            for (uint32_t i = 0; i < job_count; ++i)
            {
                if (tmp_hash_jobs[i].m_ChunkHashes != tmp_hash_jobs[i].m_InlineChunkHashes)
                {
                    Longtail_Free(tmp_hash_jobs[i].m_ChunkHashes);
                }
            }
            Longtail_Free(work_mem);
            return ENOMEM;
        }

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < job_count; ++i)
        {
            uint32_t asset_index = tmp_hash_jobs[i].m_AssetIndex;
            if (tmp_hash_jobs[i].m_StartRange == 0)
            {
                asset_chunk_start_index[asset_index] = chunk_offset;
                asset_chunk_counts[asset_index] = 0;
            }
            uint32_t job_chunk_count = *tmp_hash_jobs[i].m_AssetChunkCount;
            asset_chunk_counts[asset_index] += job_chunk_count;
            for (uint32_t chunk_index = 0; chunk_index < job_chunk_count; ++chunk_index)
            {
                cad->m_ChunkSizes[chunk_offset] = tmp_hash_jobs[i].m_ChunkSizes[chunk_index];
                cad->m_ChunkHashes[chunk_offset] = tmp_hash_jobs[i].m_ChunkHashes[chunk_index];
                cad->m_ChunkTags[chunk_offset] = optional_asset_tags ? optional_asset_tags[asset_index] : 0;
                ++chunk_offset;
            }
        }
        for (uint32_t a = 0; a < asset_count; ++a)
        {
            uint32_t chunk_start_index = asset_chunk_start_index[a];
            uint32_t hash_size = (uint32_t)(sizeof(TLongtail_Hash) * asset_chunk_counts[a]);
            err = hash_api->HashBuffer(hash_api, hash_size, &cad->m_ChunkHashes[chunk_start_index], &content_hashes[a]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_api->HashBuffer() failed with %d", err)
                Longtail_Free(cad);
                for (uint32_t i = 0; i < job_count; ++i)
                {
                    if (tmp_hash_jobs[i].m_ChunkHashes != tmp_hash_jobs[i].m_InlineChunkHashes)
                    {
                        Longtail_Free(tmp_hash_jobs[i].m_ChunkHashes);
                    }
                }
                Longtail_Free(work_mem);
                return err;
            }
        }
        *out_chunk_assets_data = cad;
    }

    for (uint32_t i = 0; i < job_count; ++i)
    {
        if (tmp_hash_jobs[i].m_ChunkHashes != tmp_hash_jobs[i].m_InlineChunkHashes)
        {
            Longtail_Free(tmp_hash_jobs[i].m_ChunkHashes);
        }
    }
    Longtail_Free(work_mem);
    return err;
}

static size_t Longtail_GetVersionIndexDataSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(asset_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(asset_chunk_index_count, "%u"),
        LONGTAIL_LOGFIELD(path_data_size, "%u"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_index_count >= chunk_count, return EINVAL)

    size_t version_index_data_size =
        sizeof(uint32_t) +                              // m_Version
        sizeof(uint32_t) +                              // m_HashIdentifier
        sizeof(uint32_t) +                              // m_TargetChunkSize
        sizeof(uint32_t) +                              // m_AssetCount
        sizeof(uint32_t) +                              // m_ChunkCount
        sizeof(uint32_t) +                              // m_AssetChunkIndexCount
        (sizeof(TLongtail_Hash) * asset_count) +        // m_PathHashes
        (sizeof(TLongtail_Hash) * asset_count) +        // m_ContentHashes
        (sizeof(uint64_t) * asset_count) +              // m_AssetSizes
        (sizeof(uint32_t) * asset_count) +              // m_AssetChunkCounts
        (sizeof(uint32_t) * asset_count) +              // m_AssetChunkIndexStarts
        (sizeof(uint32_t) * asset_chunk_index_count) +  // m_AssetChunkIndexes
        (sizeof(TLongtail_Hash) * chunk_count) +        // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count) +              // m_ChunkSizes
        (sizeof(uint32_t) * chunk_count) +              // m_ChunkTags
        (sizeof(uint32_t) * asset_count) +              // m_NameOffsets
        (sizeof(uint16_t) * asset_count) +              // m_Permissions
        path_data_size;

    return version_index_data_size;
}

size_t Longtail_GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(asset_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(asset_chunk_index_count, "%u"),
        LONGTAIL_LOGFIELD(path_data_size, "%u"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_index_count >= chunk_count, return EINVAL)
    return sizeof(struct Longtail_VersionIndex) +
            Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, path_data_size);
}

static int InitVersionIndexFromData(
    struct Longtail_VersionIndex* version_index,
    void* data,
    size_t data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(data_size, "%" PRIu64),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    LONGTAIL_FATAL_ASSERT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, data != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, data_size >= sizeof(uint32_t), return EBADF)

    char* p = (char*)data;

    if (data_size < (6 * sizeof(uint32_t)))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Version index is invalid, not big enough for minimal header. Size %" PRIu64 " < %" PRIu64 "", data_size, (6 * sizeof(uint32_t)));
        return EBADF;
    }

    size_t version_index_data_start = (size_t)(uintptr_t)p;

    version_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*version_index->m_Version) != Longtail_CurrentVersionIndexVersion)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Mismatching versions in version index data %" PRIu64 " != %" PRIu64 "", (void*)version_index->m_Version, Longtail_CurrentVersionIndexVersion);
        return EBADF;
    }

    version_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    version_index->m_TargetChunkSize = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    version_index->m_AssetCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t asset_count = *version_index->m_AssetCount;

    version_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t chunk_count = *version_index->m_ChunkCount;

    version_index->m_AssetChunkIndexCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t asset_chunk_index_count = *version_index->m_AssetChunkIndexCount;

    size_t version_index_data_size = Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, 0);
    if (version_index_data_size > data_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Version index data is truncated: %" PRIu64 " <= %" PRIu64, data_size, version_index_data_size)
        return EBADF;
    }

    version_index->m_PathHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_ContentHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * asset_count);

    version_index->m_AssetSizes = (uint64_t*)(void*)p;
    p += (sizeof(uint64_t) * asset_count);

    version_index->m_AssetChunkCounts = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_AssetChunkIndexStarts = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_AssetChunkIndexes = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * asset_chunk_index_count);

    version_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);

    version_index->m_ChunkSizes = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * chunk_count);

    version_index->m_ChunkTags = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * chunk_count);

    version_index->m_NameOffsets = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * asset_count);

    version_index->m_Permissions = (uint16_t*)(void*)p;
    p += (sizeof(uint16_t) * asset_count);

    size_t version_index_name_data_start = (size_t)p;

    version_index->m_NameDataSize = (uint32_t)(data_size - (version_index_name_data_start - version_index_data_start));

    version_index->m_NameData = (char*)p;

    return 0;
}

int Longtail_BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const struct Longtail_FileInfos* file_infos,
    const TLongtail_Hash* path_hashes,
    const TLongtail_Hash* content_hashes,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_counts,
    uint32_t asset_chunk_index_count,
    const uint32_t* asset_chunk_indexes,
    uint32_t chunk_count,
    const uint32_t* chunk_sizes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* optional_chunk_tags,
    uint32_t hash_api_identifier,
    uint32_t target_chunk_size,
    struct Longtail_VersionIndex** out_version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(mem_size, "%p"),
        LONGTAIL_LOGFIELD(file_infos, "%p"),
        LONGTAIL_LOGFIELD(path_hashes, "%p"),
        LONGTAIL_LOGFIELD(content_hashes, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_index_starts, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_counts, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_index_count, "%u"),
        LONGTAIL_LOGFIELD(asset_chunk_indexes, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(optional_chunk_tags, "%p"),
        LONGTAIL_LOGFIELD(hash_api_identifier, "%u"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(out_version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, mem_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || path_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || content_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_counts == 0 || asset_chunk_index_starts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_index_count >= chunk_count, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || asset_chunk_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    uint32_t asset_count = file_infos == 0 ? 0u : file_infos->m_Count;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)mem;
    uint32_t* p = (uint32_t*)(void*)&version_index[1];
    version_index->m_Version = &p[0];
    version_index->m_HashIdentifier = &p[1];
    version_index->m_TargetChunkSize = &p[2];
    version_index->m_AssetCount = &p[3];
    version_index->m_ChunkCount = &p[4];
    version_index->m_AssetChunkIndexCount = &p[5];
    *version_index->m_Version = Longtail_CurrentVersionIndexVersion;
    *version_index->m_HashIdentifier = hash_api_identifier;
    *version_index->m_TargetChunkSize = target_chunk_size;
    *version_index->m_AssetCount = asset_count;
    *version_index->m_ChunkCount = chunk_count;
    *version_index->m_AssetChunkIndexCount = asset_chunk_index_count;

    size_t index_data_size = mem_size - sizeof(struct Longtail_VersionIndex);
    int err = InitVersionIndexFromData(version_index, &version_index[1], index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData() failed with %d", err);
        return err;
    }

    if (asset_count > 0)
    {
        memmove(version_index->m_PathHashes, path_hashes, sizeof(TLongtail_Hash) * asset_count);
        memmove(version_index->m_ContentHashes, content_hashes, sizeof(TLongtail_Hash) * asset_count);
        memmove(version_index->m_AssetSizes, file_infos->m_Sizes, sizeof(uint64_t) * asset_count);
        memmove(version_index->m_AssetChunkCounts, asset_chunk_counts, sizeof(uint32_t) * asset_count);
        memmove(version_index->m_AssetChunkIndexStarts, asset_chunk_index_starts, sizeof(uint32_t) * asset_count);
        memmove(version_index->m_AssetChunkIndexes, asset_chunk_indexes, sizeof(uint32_t) * asset_chunk_index_count);
        memmove(version_index->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);
        memmove(version_index->m_ChunkSizes, chunk_sizes, sizeof(uint32_t) * chunk_count);
        if (optional_chunk_tags)
        {
            memmove(version_index->m_ChunkTags, optional_chunk_tags, sizeof(uint32_t) * chunk_count);
        }
        else
        {
            memset(version_index->m_ChunkTags, 0, sizeof(uint32_t) * chunk_count);
        }
        memmove(version_index->m_NameOffsets, file_infos->m_PathStartOffsets, sizeof(uint32_t) * asset_count);
        memmove(version_index->m_Permissions, file_infos->m_Permissions, sizeof(uint16_t) * asset_count);
        memmove(version_index->m_NameData, file_infos->m_PathData, file_infos->m_PathDataSize);
    }

    *out_version_index = version_index;
    return 0;
}

int Longtail_CreateVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_ChunkerAPI* chunker_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_path,
    const struct Longtail_FileInfos* file_infos,
    const uint32_t* optional_asset_tags,
    uint32_t target_chunk_size,
    int enable_file_map,
    struct Longtail_VersionIndex** out_version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(file_infos, "%p"),
        LONGTAIL_LOGFIELD(optional_asset_tags, "%u"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(out_version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || target_chunk_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || out_version_index != 0, return EINVAL)

    uint32_t path_count = file_infos == 0 ? 0u : file_infos->m_Count;

    if (path_count == 0)
    {
        size_t version_index_size = Longtail_GetVersionIndexSize(path_count, 0, 0, 0);
        void* version_index_mem = Longtail_Alloc("CreateVersionIndex", version_index_size);
        if (!version_index_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }

        struct Longtail_VersionIndex* version_index;
        int err = Longtail_BuildVersionIndex(
            version_index_mem,              // mem
            version_index_size,             // mem_size
            file_infos,                          // paths
            0,                    // path_hashes
            0,                 // content_hashes
            0,        // asset_chunk_index_starts
            0,             // asset_chunk_counts
            0,       // asset_chunk_index_count
            0,            // asset_chunk_indexes
            0,             // chunk_count
            0,           // chunk_sizes
            0,           // chunk_hashes
            0,          // chunk_tags
            hash_api->GetIdentifier(hash_api),
            target_chunk_size,
            &version_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_BuildVersionIndex() failed with %d", err)
            return err;
        }
        *out_version_index = version_index;
        return 0;
    }

    size_t work_mem_size = (sizeof(TLongtail_Hash) * path_count) +
        (sizeof(TLongtail_Hash) * path_count) +
        (sizeof(uint32_t) * path_count) +
        (sizeof(uint32_t) * path_count);
    void* work_mem = Longtail_Alloc("CreateVersionIndex", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    TLongtail_Hash* tmp_path_hashes = (TLongtail_Hash*)work_mem;
    TLongtail_Hash* tmp_content_hashes = (TLongtail_Hash*)&tmp_path_hashes[path_count];
    uint32_t* tmp_asset_chunk_counts = (uint32_t*)&tmp_content_hashes[path_count];
    uint32_t* tmp_asset_chunk_start_index = (uint32_t*)&tmp_asset_chunk_counts[path_count];

    struct ChunkAssetsData* chunk_assets_data;

    int err = ChunkAssets(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        progress_api,
        optional_cancel_api,
        optional_cancel_token,
        root_path,
        file_infos,
        tmp_path_hashes,
        tmp_content_hashes,
        optional_asset_tags,
        tmp_asset_chunk_start_index,
        tmp_asset_chunk_counts,
        target_chunk_size,
        enable_file_map,
        &chunk_assets_data);
    if (err)
    {
        LONGTAIL_LOG(ctx, (err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets() failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }

    uint32_t assets_chunk_index_count = chunk_assets_data->m_ChunkCount;
    uint32_t* asset_chunk_sizes = chunk_assets_data->m_ChunkSizes;
    uint32_t* asset_chunk_tags = chunk_assets_data->m_ChunkTags;
    TLongtail_Hash* asset_chunk_hashes = chunk_assets_data->m_ChunkHashes;

    size_t work_mem_compact_size = (sizeof(uint32_t) * assets_chunk_index_count) +
        (sizeof(TLongtail_Hash) * assets_chunk_index_count) + 
        (sizeof(uint32_t) * assets_chunk_index_count) +
        (sizeof(uint32_t) * assets_chunk_index_count) +
        LongtailPrivate_LookupTable_GetSize(assets_chunk_index_count);
    void* work_mem_compact = Longtail_Alloc("CreateVersionIndex", work_mem_compact_size);
    if (!work_mem_compact)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(chunk_assets_data);
        Longtail_Free(work_mem);
        return ENOMEM;
    }

    uint32_t* tmp_asset_chunk_indexes = (uint32_t*)work_mem_compact;
    TLongtail_Hash* tmp_compact_chunk_hashes = (TLongtail_Hash*)&tmp_asset_chunk_indexes[assets_chunk_index_count];
    uint32_t* tmp_compact_chunk_sizes =  (uint32_t*)&tmp_compact_chunk_hashes[assets_chunk_index_count];
    uint32_t* tmp_compact_chunk_tags =  (uint32_t*)&tmp_compact_chunk_sizes[assets_chunk_index_count];

    uint32_t unique_chunk_count = 0;
    struct Longtail_LookupTable* chunk_hash_to_index = LongtailPrivate_LookupTable_Create(&tmp_compact_chunk_tags[assets_chunk_index_count], assets_chunk_index_count, 0);

    for (uint32_t c = 0; c < assets_chunk_index_count; ++c)
    {
        TLongtail_Hash h = asset_chunk_hashes[c];
        uint32_t* chunk_index = LongtailPrivate_LookupTable_PutUnique(chunk_hash_to_index, h, unique_chunk_count);
        if (chunk_index == 0)
        {
            tmp_compact_chunk_hashes[unique_chunk_count] = h;
            tmp_compact_chunk_sizes[unique_chunk_count] = asset_chunk_sizes[c];
            tmp_compact_chunk_tags[unique_chunk_count] = asset_chunk_tags[c];
            tmp_asset_chunk_indexes[c] = unique_chunk_count;
            ++unique_chunk_count;
        }
        else
        {
            tmp_asset_chunk_indexes[c] = *chunk_index;
        }
    }

    size_t version_index_size = Longtail_GetVersionIndexSize(path_count, unique_chunk_count, assets_chunk_index_count, file_infos->m_PathDataSize);
    void* version_index_mem = Longtail_Alloc("CreateVersionIndex", version_index_size);
    if (!version_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(work_mem_compact);
        Longtail_Free(chunk_assets_data);
        Longtail_Free(work_mem);
        return ENOMEM;
    }

    struct Longtail_VersionIndex* version_index;
    err = Longtail_BuildVersionIndex(
        version_index_mem,              // mem
        version_index_size,             // mem_size
        file_infos,                          // paths
        tmp_path_hashes,                    // path_hashes
        tmp_content_hashes,                 // content_hashes
        tmp_asset_chunk_start_index,        // asset_chunk_index_starts
        tmp_asset_chunk_counts,             // asset_chunk_counts
        assets_chunk_index_count,       // asset_chunk_index_count
        tmp_asset_chunk_indexes,            // asset_chunk_indexes
        unique_chunk_count,             // chunk_count
        tmp_compact_chunk_sizes,            // chunk_sizes
        tmp_compact_chunk_hashes,           // chunk_hashes
        tmp_compact_chunk_tags,// chunk_tags
        hash_api->GetIdentifier(hash_api),
        target_chunk_size,
        &version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_BuildVersionIndex() failed with %d", err)
        Longtail_Free(work_mem_compact);
        Longtail_Free(version_index_mem);
        Longtail_Free(chunk_assets_data);
        Longtail_Free(work_mem);
        return err;
    }

    Longtail_Free(work_mem_compact);
    Longtail_Free(chunk_assets_data);
    Longtail_Free(work_mem);

    *out_version_index = version_index;
    return 0;
}

static SORTFUNC(SortPathShortToLongVersionMerge)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    const uint32_t* path_lengths = (const uint32_t*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    uint32_t a_len = path_lengths[a];
    uint32_t b_len = path_lengths[b];
    if (a_len < b_len)
    {
        return -1;
    }
    if (a_len > b_len)
    {
        return 1;
    }
    if (a < b)
    {
        return -1;
    }
    if (a > b)
    {
        return 1;
    }
    return 0;
}

int Longtail_MergeVersionIndex(
    const struct Longtail_VersionIndex* base_version_index,
    const struct Longtail_VersionIndex* overlay_version_index,
    struct Longtail_VersionIndex** out_version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(base_version_index, "%p"),
        LONGTAIL_LOGFIELD(overlay_version_index, "%p"),
        LONGTAIL_LOGFIELD(out_version_index, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, base_version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, overlay_version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, *base_version_index->m_TargetChunkSize == *overlay_version_index->m_TargetChunkSize, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, *base_version_index->m_HashIdentifier == *overlay_version_index->m_HashIdentifier, return EINVAL)

    uint32_t base_asset_count = *base_version_index->m_AssetCount;
    uint32_t overlay_asset_count = *overlay_version_index->m_AssetCount;

    size_t max_asset_count = (size_t)(base_asset_count) + (size_t)(overlay_asset_count);
    if (max_asset_count == 0)
    {
        size_t version_index_size = Longtail_GetVersionIndexSize(0, 0, 0, 0);
        void* version_index_mem = Longtail_Alloc("CreateVersionIndex", version_index_size);
        if (!version_index_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }

        struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)version_index_mem;
        uint32_t* p = (uint32_t*)(void*)&version_index[1];
        version_index->m_Version = &p[0];
        version_index->m_HashIdentifier = &p[1];
        version_index->m_TargetChunkSize = &p[2];
        version_index->m_AssetCount = &p[3];
        version_index->m_ChunkCount = &p[4];
        version_index->m_AssetChunkIndexCount = &p[5];
        *version_index->m_Version = Longtail_CurrentVersionIndexVersion;
        *version_index->m_HashIdentifier = *base_version_index->m_HashIdentifier;
        *version_index->m_TargetChunkSize = *base_version_index->m_TargetChunkSize;
        *version_index->m_AssetCount = 0;
        *version_index->m_ChunkCount = 0;
        *version_index->m_AssetChunkIndexCount = 0;
        *out_version_index = version_index;
        return 0;
    }

    uint32_t base_chunk_count = *base_version_index->m_ChunkCount;
    uint32_t overlay_chunk_count = *overlay_version_index->m_ChunkCount;

    size_t max_chunk_count = (size_t)(base_chunk_count) + (size_t)(overlay_chunk_count);

    size_t base_asset_lookup_table_size = LongtailPrivate_LookupTable_GetSize(base_asset_count);
    size_t overlay_asset_lookup_table_size = LongtailPrivate_LookupTable_GetSize(overlay_asset_count);
    size_t base_chunk_lookup_table_size = LongtailPrivate_LookupTable_GetSize(base_chunk_count);
    size_t overlay_chunk_lookup_table_size = LongtailPrivate_LookupTable_GetSize(overlay_chunk_count);
    size_t path_hashes_size = sizeof(TLongtail_Hash) * max_asset_count;
    size_t chunk_hashes_size = sizeof(TLongtail_Hash) * max_chunk_count;
    size_t name_lengths_size = sizeof(uint32_t) * max_asset_count;
    size_t asset_indexes_size = sizeof(uint32_t) * max_asset_count;

    size_t tmp_mem_size = base_asset_lookup_table_size + overlay_asset_lookup_table_size + base_chunk_lookup_table_size + overlay_chunk_lookup_table_size + path_hashes_size + chunk_hashes_size + name_lengths_size + asset_indexes_size;
    void* tmp_mem = Longtail_Alloc("Longtail_MergeVersionIndex", tmp_mem_size);
    if (!tmp_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)tmp_mem;
    struct Longtail_LookupTable* base_asset_lut = LongtailPrivate_LookupTable_Create(p, base_asset_count, 0);
    p += base_asset_lookup_table_size;
    struct Longtail_LookupTable* overlay_asset_lut = LongtailPrivate_LookupTable_Create(p, overlay_asset_count, 0);
    p += overlay_asset_lookup_table_size;

    struct Longtail_LookupTable* base_chunk_lut = LongtailPrivate_LookupTable_Create(p, base_chunk_count, 0);
    p += base_chunk_lookup_table_size;
    struct Longtail_LookupTable* overlay_chunk_lut = LongtailPrivate_LookupTable_Create(p, overlay_chunk_count, 0);
    p += overlay_chunk_lookup_table_size;
    uint32_t* asset_indexes = (uint32_t*)p;
    p += asset_indexes_size;

    uint32_t* name_lengths = (uint32_t*)p;
    p += name_lengths_size;

    TLongtail_Hash* path_hashes = (TLongtail_Hash*)p;
    p += path_hashes_size;
    TLongtail_Hash* chunk_hashes = (TLongtail_Hash*)p;
    p += chunk_hashes_size;

    size_t asset_chunk_index_count = 0;

    size_t path_name_size = 0;
    for (uint32_t i = 0; i < base_asset_count; ++i)
    {
        TLongtail_Hash path_hash = base_version_index->m_PathHashes[i];
        path_hashes[i] = path_hash;
        const char* path = &base_version_index->m_NameData[base_version_index->m_NameOffsets[i]];
        uint32_t path_length = (uint32_t)strlen(path);
        name_lengths[i] = path_length;
        path_name_size += ((size_t)path_length) + 1;
        asset_indexes[i] = i;
        LongtailPrivate_LookupTable_Put(base_asset_lut, path_hash, i);
        asset_chunk_index_count += base_version_index->m_AssetChunkCounts[i];
    }
    
    size_t unique_asset_count = base_asset_count;
    for (uint32_t i = 0; i < overlay_asset_count; ++i)
    {
        TLongtail_Hash path_hash = overlay_version_index->m_PathHashes[i];
        const uint32_t* base_asset_index = LongtailPrivate_LookupTable_Get(base_asset_lut, path_hash);
        if (base_asset_index != 0)
        {
            asset_chunk_index_count -= base_version_index->m_AssetChunkCounts[*base_asset_index];
        }
        else
        {
            path_hashes[unique_asset_count] = path_hash;
            const char* path = &overlay_version_index->m_NameData[overlay_version_index->m_NameOffsets[i]];
            uint32_t path_length = (uint32_t)strlen(path);
            name_lengths[unique_asset_count] = path_length;
            path_name_size += ((size_t)path_length) + 1;
            asset_indexes[unique_asset_count] = (uint32_t)unique_asset_count;
            unique_asset_count++;
        }
        LongtailPrivate_LookupTable_Put(overlay_asset_lut, path_hash, i);
        asset_chunk_index_count += overlay_version_index->m_AssetChunkCounts[i];
    }
    if (unique_asset_count > 0xffffffffu)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Merged version index would contain to many assets (%" PRIu64 "), failed with %d", unique_asset_count, ENOMEM)
        Longtail_Free(tmp_mem);
        return ENOMEM;
    }
    if (asset_chunk_index_count > 0xffffffffu)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Merged version index would reference to many asset chunk references (%" PRIu64 "), failed with %d", asset_chunk_index_count, ENOMEM)
        Longtail_Free(tmp_mem);
        return ENOMEM;
    }
    if (path_name_size > 0xffffffffu)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Merged version index would contain to much path name data (%" PRIu64 "), failed with %d", path_name_size, ENOMEM)
            Longtail_Free(tmp_mem);
        return ENOMEM;
    }

    size_t unique_chunk_count = 0;
    for (size_t i = 0; i < unique_asset_count; ++i)
    {
        TLongtail_Hash path_hash = path_hashes[i];

        const uint32_t* overlay_it = LongtailPrivate_LookupTable_Get(overlay_asset_lut, path_hash);
        if (overlay_it)
        {
            uint32_t asset_index = *overlay_it;
            uint32_t asset_chunk_count = overlay_version_index->m_AssetChunkCounts[asset_index];
            uint32_t asset_chunk_index_start = overlay_version_index->m_AssetChunkIndexStarts[asset_index];
            for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
            {
                uint32_t chunk_index = overlay_version_index->m_AssetChunkIndexes[asset_chunk_index + asset_chunk_index_start];
                TLongtail_Hash chunk_hash = overlay_version_index->m_ChunkHashes[chunk_index];
                if (LongtailPrivate_LookupTable_PutUnique(overlay_chunk_lut, chunk_hash, chunk_index) == 0)
                {
                    chunk_hashes[unique_chunk_count] = chunk_hash;
                    unique_chunk_count++;
                }
            }
            continue;
        }
        const uint32_t* base_it = LongtailPrivate_LookupTable_Get(base_asset_lut, path_hash);
        if (base_it)
        {
            uint32_t asset_index = *base_it;
            uint32_t asset_chunk_count = base_version_index->m_AssetChunkCounts[asset_index];
            uint32_t asset_chunk_index_start = base_version_index->m_AssetChunkIndexStarts[asset_index];
            for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
            {
                uint32_t chunk_index = base_version_index->m_AssetChunkIndexes[asset_chunk_index + asset_chunk_index_start];
                TLongtail_Hash chunk_hash = base_version_index->m_ChunkHashes[chunk_index];
                if (LongtailPrivate_LookupTable_Get(overlay_chunk_lut, chunk_hash) == 0)
                {
                    if (LongtailPrivate_LookupTable_PutUnique(base_chunk_lut, chunk_hash, chunk_index) == 0)
                    {
                        chunk_hashes[unique_chunk_count] = chunk_hash;
                        unique_chunk_count++;
                    }
                }
            }
            continue;
        }
    }
    if (unique_chunk_count > 0xffffffffu)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Merged version index contain to many chunks (%" PRIu64 "), failed with %d", unique_chunk_count, ENOMEM)
        Longtail_Free(tmp_mem);
        return ENOMEM;
    }

    QSORT(asset_indexes, unique_asset_count, sizeof(uint32_t), SortPathShortToLongVersionMerge, (void*)name_lengths);

    size_t chunk_lut_size = LongtailPrivate_LookupTable_GetSize((uint32_t)unique_chunk_count);
    void* chunk_lut_mem = Longtail_Alloc("Longtail_MergeVersionIndex", chunk_lut_size);
    if (chunk_lut_mem == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(tmp_mem);
        return ENOMEM;
    }

    struct Longtail_LookupTable* chunk_lut = LongtailPrivate_LookupTable_Create(chunk_lut_mem, (uint32_t)unique_chunk_count, 0);
    for (uint32_t chunk_index = 0; chunk_index < (uint32_t)unique_chunk_count; ++chunk_index)
    {
        LongtailPrivate_LookupTable_Put(chunk_lut, chunk_hashes[chunk_index], chunk_index);
    }

    size_t version_index_size = Longtail_GetVersionIndexSize((uint32_t)unique_asset_count, (uint32_t)unique_chunk_count, (uint32_t)asset_chunk_index_count, (uint32_t)path_name_size);
    void* out_mem = Longtail_Alloc("Longtail_MergeVersionIndex", version_index_size);
    if (out_mem == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(chunk_lut_mem);
        Longtail_Free(tmp_mem);
        return ENOMEM;
    }

    struct Longtail_VersionIndex* merged_version_index = (struct Longtail_VersionIndex*)out_mem;
    {
        char* p = (char*)&merged_version_index[1];

        merged_version_index->m_Version = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_HashIdentifier = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_TargetChunkSize = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_AssetCount = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_ChunkCount = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_AssetChunkIndexCount = (uint32_t*)p;
        p += sizeof(uint32_t);
        merged_version_index->m_PathHashes = (TLongtail_Hash*)p;
        p += sizeof(TLongtail_Hash) * unique_asset_count;
        merged_version_index->m_ContentHashes = (TLongtail_Hash*)p;
        p += sizeof(TLongtail_Hash) * unique_asset_count;
        merged_version_index->m_AssetSizes = (TLongtail_Hash*)p;
        p += sizeof(TLongtail_Hash) * unique_asset_count;
        merged_version_index->m_AssetChunkCounts = (uint32_t*)p;
        p += sizeof(uint32_t) * unique_asset_count;
        merged_version_index->m_AssetChunkIndexStarts = (uint32_t*)p;
        p += sizeof(uint32_t) * unique_asset_count;
        merged_version_index->m_AssetChunkIndexes = (uint32_t*)p;
        p += sizeof(uint32_t) * asset_chunk_index_count;
        merged_version_index->m_ChunkHashes = (TLongtail_Hash*)p;
        p += sizeof(TLongtail_Hash) * unique_chunk_count;
        merged_version_index->m_ChunkSizes = (uint32_t*)p;
        p += sizeof(uint32_t) * unique_chunk_count;
        merged_version_index->m_ChunkTags = (uint32_t*)p;
        p += sizeof(uint32_t) * unique_chunk_count;
        merged_version_index->m_NameOffsets = (uint32_t*)p;
        p += sizeof(uint32_t) * unique_asset_count;
        merged_version_index->m_Permissions = (uint16_t*)p;
        p += sizeof(uint16_t) * unique_asset_count;
        merged_version_index->m_NameData = (char*)p;
    }
    *merged_version_index->m_Version = Longtail_CurrentVersionIndexVersion;
    *merged_version_index->m_HashIdentifier = *base_version_index->m_HashIdentifier;
    *merged_version_index->m_TargetChunkSize = *base_version_index->m_TargetChunkSize;
    *merged_version_index->m_AssetCount = (uint32_t)unique_asset_count;
    *merged_version_index->m_ChunkCount = (uint32_t)unique_chunk_count;
    *merged_version_index->m_AssetChunkIndexCount = (uint32_t)asset_chunk_index_count;
    merged_version_index->m_NameDataSize = (uint32_t)path_name_size;
    memcpy(merged_version_index->m_PathHashes, path_hashes, sizeof(TLongtail_Hash) * unique_asset_count);
    memcpy(merged_version_index->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * unique_chunk_count);

    uint32_t asset_index_offset = 0;
    uint32_t path_name_offset = 0;
    for (uint32_t asset_index = 0; asset_index < (uint32_t)unique_asset_count; ++asset_index)
    {
        TLongtail_Hash path_hash = path_hashes[asset_index];
        const struct Longtail_VersionIndex* source_version_index = 0;
        uint32_t source_asset_index = 0;
        const uint32_t* overlay_asset_index = LongtailPrivate_LookupTable_Get(overlay_asset_lut, path_hash);
        if (overlay_asset_index)
        {
            source_version_index = overlay_version_index;
            source_asset_index = *overlay_asset_index;
        }
        else
        {
            const uint32_t* base_asset_index = LongtailPrivate_LookupTable_Get(base_asset_lut, path_hash);
            LONGTAIL_FATAL_ASSERT(ctx, base_asset_index != 0, return EINVAL)
            source_version_index = base_version_index;
            source_asset_index = *base_asset_index;
        }

        uint32_t asset_index_start = source_version_index->m_AssetChunkIndexStarts[source_asset_index];
        const uint32_t* asset_chunk_indexes = &source_version_index->m_AssetChunkIndexes[asset_index_start];
        uint32_t asset_chunk_count = source_version_index->m_AssetChunkCounts[source_asset_index];
        const TLongtail_Hash* source_chunk_hashes = source_version_index->m_ChunkHashes;
        TLongtail_Hash source_content_hash = source_version_index->m_ContentHashes[source_asset_index];
        uint16_t source_permissions = source_version_index->m_Permissions[source_asset_index];
        uint64_t source_asset_size = source_version_index->m_AssetSizes[source_asset_index];

        merged_version_index->m_ContentHashes[asset_index] = source_content_hash;
        merged_version_index->m_Permissions[asset_index] = source_permissions;
        merged_version_index->m_AssetSizes[asset_index] = source_asset_size;
        merged_version_index->m_AssetChunkIndexStarts[asset_index] = asset_index_offset;
        merged_version_index->m_AssetChunkCounts[asset_index] = asset_chunk_count;
        for (uint32_t asset_chunk_index_offset = 0; asset_chunk_index_offset < asset_chunk_count; ++asset_chunk_index_offset)
        {
            uint32_t source_chunk_index = asset_chunk_indexes[asset_chunk_index_offset];
            TLongtail_Hash chunk_hash = source_chunk_hashes[source_chunk_index];
            const uint32_t* chunk_index = LongtailPrivate_LookupTable_Get(chunk_lut, chunk_hash);
            LONGTAIL_FATAL_ASSERT(ctx, chunk_index != 0, return EINVAL)
            merged_version_index->m_AssetChunkIndexes[asset_index_offset] = *chunk_index;
            asset_index_offset++;
        }

        merged_version_index->m_NameOffsets[asset_index] = path_name_offset;
        const char* path_name = &source_version_index->m_NameData[source_version_index->m_NameOffsets[source_asset_index]];
        size_t path_name_length = strlen(path_name);
        memcpy(&merged_version_index->m_NameData[path_name_offset], path_name, path_name_length + 1);
        path_name_offset += (uint32_t)(path_name_length + 1);
    }

    for (uint32_t chunk_index = 0; chunk_index < (uint32_t)unique_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = chunk_hashes[chunk_index];
        const struct Longtail_VersionIndex* source_version_index = 0;
        uint32_t source_chunk_index = 0;
        const uint32_t* overlay_chunk_index = LongtailPrivate_LookupTable_Get(overlay_chunk_lut, chunk_hash);
        if (overlay_chunk_index)
        {
            source_version_index = overlay_version_index;
            source_chunk_index = *overlay_chunk_index;
        }
        else
        {
            const uint32_t* base_chunk_index = LongtailPrivate_LookupTable_Get(base_chunk_lut, chunk_hash);
            LONGTAIL_FATAL_ASSERT(ctx, base_chunk_index != 0, return EINVAL)
            source_version_index = base_version_index;
            source_chunk_index = *base_chunk_index;
        }
        merged_version_index->m_ChunkSizes[chunk_index] = source_version_index->m_ChunkSizes[source_chunk_index];
        merged_version_index->m_ChunkTags[chunk_index] = source_version_index->m_ChunkTags[source_chunk_index];
    }

    Longtail_Free(chunk_lut_mem);
    Longtail_Free(tmp_mem);

    *out_version_index = merged_version_index;
    return 0;
}

int Longtail_WriteVersionIndexToBuffer(
    const struct Longtail_VersionIndex* version_index,
    void** out_buffer,
    size_t* out_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(out_buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);
    *out_buffer = Longtail_Alloc("WriteVersionIndexToBuffer", index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, version_index->m_Version, index_data_size);
    *out_size = index_data_size;
    return 0;
}

int Longtail_WriteVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_VersionIndex* version_index,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)

    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenWriteFile) failed with %d",err)
        return err;
    }

    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, version_index->m_Version);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }

    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "wrote %" PRIu64 " bytes", index_data_size)

    return 0;
}

int Longtail_ReadVersionIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_VersionIndex** out_version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    size_t version_index_size = sizeof(struct Longtail_VersionIndex) + size;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc("ReadVersionIndexFromBuffer", version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(&version_index[1], buffer, size);
    int err = InitVersionIndexFromData(version_index, &version_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData() failed with %d", err)
        Longtail_Free(version_index);
        return err;
    }
    *out_version_index = version_index;
    return 0;
}

int Longtail_ReadVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_VersionIndex** out_version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint64_t version_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &version_index_data_size);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->GetSize() failed with %d", err)
        return err;
    }
    size_t version_index_size = version_index_data_size + sizeof(struct Longtail_VersionIndex);
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc("ReadVersionIndex", version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(version_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(version_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = InitVersionIndexFromData(version_index, &version_index[1], version_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData() failed with %d", err)
        Longtail_Free(version_index);
        return err;
    }

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Read version index containing %u assets in %u chunks",
        *version_index->m_AssetCount, *version_index->m_ChunkCount)

    *out_version_index = version_index;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "read %" PRIu64 " bytes", version_index_data_size)

    return 0;
}

size_t Longtail_GetBlockIndexDataSize(uint32_t chunk_count)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    return
        sizeof(TLongtail_Hash) +                    // m_BlockHash
        sizeof(uint32_t) +                          // m_HashIdentifier
        sizeof(uint32_t) +                          // m_ChunkCount
        sizeof(uint32_t) +                          // m_Tag
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count);           // m_ChunkSizes
}

struct Longtail_BlockIndex* Longtail_InitBlockIndex(void* mem, uint32_t chunk_count)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)

    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)mem;
    char* p = (char*)&block_index[1];

    block_index->m_BlockHash = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash);

    block_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_Tag = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * chunk_count;

    block_index->m_ChunkSizes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * chunk_count;

    return block_index;
}

struct Longtail_BlockIndex* Longtail_CopyBlockIndex(struct Longtail_BlockIndex* block_index)
{
    uint32_t chunk_count = *block_index->m_ChunkCount;
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    void* mem = Longtail_Alloc("Longtail_CopyBlockIndex", block_index_size);
    struct Longtail_BlockIndex* copy_block_index = Longtail_InitBlockIndex(mem, chunk_count);
    *copy_block_index->m_BlockHash = *block_index->m_BlockHash;
    *copy_block_index->m_HashIdentifier = *block_index->m_HashIdentifier;
    *copy_block_index->m_ChunkCount = *block_index->m_ChunkCount;
    *copy_block_index->m_Tag = *block_index->m_Tag;
    memcpy(copy_block_index->m_ChunkHashes, block_index->m_ChunkHashes, sizeof(TLongtail_Hash) * chunk_count);
    memcpy(copy_block_index->m_ChunkSizes, block_index->m_ChunkSizes, sizeof(uint32_t) * chunk_count);
    return copy_block_index;
}

int Longtail_InitBlockIndexFromData(
    struct Longtail_BlockIndex* block_index,
    void* data,
    uint64_t data_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_index, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(data_size, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    LONGTAIL_VALIDATE_INPUT(ctx, block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)

    char* p = (char*)data;

    block_index->m_BlockHash = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash);

    block_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_Tag = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t chunk_count = *block_index->m_ChunkCount;

    size_t block_index_data_size = Longtail_GetBlockIndexDataSize(chunk_count);
    if (block_index_data_size > data_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Chunk count results in block index data %" PRIu64 " larger that data size (%" PRIu64 ")", block_index_data_size, data_size)
        return EBADF;
    }

    block_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * chunk_count;

    block_index->m_ChunkSizes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * chunk_count;

    return 0;
}

size_t Longtail_GetBlockIndexSize(uint32_t chunk_count)
{
    size_t block_index_size =
        sizeof(struct Longtail_BlockIndex) +
        Longtail_GetBlockIndexDataSize(chunk_count);

    return block_index_size;
}

int Longtail_CreateBlockIndex(
    struct Longtail_HashAPI* hash_api,
    uint32_t tag,
    uint32_t chunk_count,
    const uint32_t* chunk_indexes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    struct Longtail_BlockIndex** out_block_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(tag, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_indexes, "%p"),
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(out_block_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_block_index != 0, return EINVAL)

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    void* mem = Longtail_Alloc("CreateBlockIndex", block_index_size);
    if (mem == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(mem, chunk_count);
    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        uint32_t chunk_index = chunk_indexes[i];
        block_index->m_ChunkHashes[i] = chunk_hashes[chunk_index];
        block_index->m_ChunkSizes[i] = chunk_sizes[chunk_index];
    }
    size_t hash_buffer_size = sizeof(TLongtail_Hash) * chunk_count;
    int err = hash_api->HashBuffer(hash_api, (uint32_t)(hash_buffer_size), (void*)block_index->m_ChunkHashes, block_index->m_BlockHash);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_api->HashBuffer() failed with %d", err)
        Longtail_Free(mem);
        return err;
    }
    *block_index->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
    *block_index->m_Tag = tag;
    *block_index->m_ChunkCount = chunk_count;

    *out_block_index = block_index;
    return 0;
}

int Longtail_WriteBlockIndexToBuffer(
    const struct Longtail_BlockIndex* block_index,
    void** out_buffer,
    size_t* out_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_index, "%p"),
        LONGTAIL_LOGFIELD(out_buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc("WriteBlockIndexToBuffer", index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, block_index->m_BlockHash, index_data_size);
    *out_size = index_data_size;
    return 0;
}

int Longtail_ReadBlockIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_BlockIndex** out_block_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_block_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_block_index != 0, return EINVAL)

    size_t block_index_size = size + sizeof(struct Longtail_BlockIndex);
    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)Longtail_Alloc("ReadBlockIndexFromBuffer", block_index_size);
    if (!block_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(&block_index[1], buffer, size);
    int err = Longtail_InitBlockIndexFromData(block_index, &block_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitBlockIndexFromData() failed with %d", err)
        Longtail_Free(block_index);
        return err;
    }
    *out_block_index = block_index;
    return 0;
}

int Longtail_WriteBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_BlockIndex* block_index,
    const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(block_index, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenWriteFile) failed with %d", err)
        return err;
    }
    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, block_index->m_BlockHash);
    if (err){
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "wrote %" PRIu64 " bytes", index_data_size)

    return 0;
}

struct Longtail_BlockIndexHeader
{
    TLongtail_Hash m_BlockHash;
    uint32_t m_HashIdentifier;
    uint32_t m_ChunkCount;
    uint32_t m_Tag;
};

int Longtail_ReadBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_BlockIndex** out_block_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_block_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_block_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, path, &f);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint64_t block_size;
    err = storage_api->GetSize(storage_api, f, &block_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->GetSize() failed with %d", err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size < sizeof(struct Longtail_BlockIndexHeader))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "file size %" PRIu64 " is to small, expected at least %" PRIu64 " bytes", block_size, sizeof(struct Longtail_BlockIndexHeader))
        storage_api->CloseFile(storage_api, f);
        return EBADF;
    }
    if (block_size > 0xffffffff)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "file size %" PRIu64 " is to large, size can be at most %" PRIu64 " bytes", block_size, 0xffffffff)
        storage_api->CloseFile(storage_api, f);
        return EBADF;
    }
    uint64_t read_offset = 0;

    struct Longtail_BlockIndexHeader blockIndexHeader;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(struct Longtail_BlockIndexHeader), &blockIndexHeader);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    read_offset += sizeof(struct Longtail_BlockIndexHeader);

    if (blockIndexHeader.m_ChunkCount == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "blockIndexHeader.m_ChunkCount (%u) must be larger than zero", blockIndexHeader.m_ChunkCount)
        storage_api->CloseFile(storage_api, f);
        return EBADF;
    }

    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(blockIndexHeader.m_ChunkCount);
    if (block_index_data_size >= block_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "block_index_data_size %" PRIu64 " is to large, size can be at most %" PRIu64 " bytes", block_index_data_size, block_size)
        storage_api->CloseFile(storage_api, f);
        return EBADF;
    }

    size_t block_index_size = Longtail_GetBlockIndexSize(blockIndexHeader.m_ChunkCount);
    void* block_index_mem = Longtail_Alloc("ReadBlockIndex", block_index_size);
    if (!block_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        storage_api->CloseFile(storage_api, f);
        return ENOMEM;
    }
    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(block_index_mem, blockIndexHeader.m_ChunkCount);
    err = storage_api->Read(storage_api, f, 0, block_index_data_size, &block_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(block_index);
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    storage_api->CloseFile(storage_api, f);
    *out_block_index = block_index;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "read %" PRIu64 " bytes", sizeof(struct Longtail_BlockIndexHeader) + block_index_data_size)

    return 0;
}

static int DisposeStoredBlock(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free(stored_block);
    return 0;
}

size_t Longtail_GetStoredBlockSize(size_t block_data_size)
{
    return sizeof(struct Longtail_StoredBlock) + sizeof(struct Longtail_BlockIndex) + block_data_size;
}

int Longtail_InitStoredBlockFromData(
    struct Longtail_StoredBlock* stored_block,
    void* block_data,
    size_t block_data_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(block_data, "%p"),
        LONGTAIL_LOGFIELD(block_data_size, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_data_size > 0, return EINVAL)

    stored_block->m_BlockIndex = (struct Longtail_BlockIndex*)&stored_block[1];
    int err = Longtail_InitBlockIndexFromData(
        stored_block->m_BlockIndex,
        block_data,
        block_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitBlockIndexFromData() failed with %d", err)
        return err;
    }
    size_t BlockIndexDataSize = Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount);
    stored_block->m_BlockData = &((uint8_t*)block_data)[BlockIndexDataSize];
    stored_block->m_BlockChunksDataSize = (uint32_t)(block_data_size - BlockIndexDataSize);
    stored_block->Dispose = 0;
    return 0;
}

int Longtail_CreateStoredBlock(
    TLongtail_Hash block_hash,
    uint32_t hash_identifier,
    uint32_t chunk_count,
    uint32_t tag,
    TLongtail_Hash* chunk_hashes,
    uint32_t* chunk_sizes,
    uint32_t block_data_size,
    struct Longtail_StoredBlock** out_stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(hash_identifier, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(tag, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(block_data_size, "%u"),
        LONGTAIL_LOGFIELD(out_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_stored_block != 0, return EINVAL)

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t stored_block_size = sizeof(struct Longtail_StoredBlock) + block_index_size + block_data_size;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("CreateStoredBlock", stored_block_size);
    if (stored_block == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    stored_block->m_BlockIndex = Longtail_InitBlockIndex(&stored_block[1], chunk_count);
    LONGTAIL_FATAL_ASSERT(ctx, stored_block->m_BlockIndex != 0, return EINVAL)

    *stored_block->m_BlockIndex->m_BlockHash = block_hash;
    *stored_block->m_BlockIndex->m_HashIdentifier = hash_identifier;
    *stored_block->m_BlockIndex->m_ChunkCount = chunk_count;
    *stored_block->m_BlockIndex->m_Tag = tag;
    memmove(stored_block->m_BlockIndex->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);
    memmove(stored_block->m_BlockIndex->m_ChunkSizes, chunk_sizes, sizeof(uint32_t) * chunk_count);

    stored_block->Dispose = DisposeStoredBlock;
    stored_block->m_BlockData = ((uint8_t*)stored_block->m_BlockIndex) + block_index_size;
    stored_block->m_BlockChunksDataSize = block_data_size;
    *out_stored_block = stored_block;
    return 0;
}

static int ReadStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return EINVAL)

    Longtail_Free(stored_block);
    return 0;
}

int Longtail_WriteStoredBlockToBuffer(
    const struct Longtail_StoredBlock* stored_block,
    void** out_buffer,
    size_t* out_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(out_buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);

    size_t size = ((size_t)block_index_data_size) + stored_block->m_BlockChunksDataSize;

    void* mem = (uint8_t*)Longtail_Alloc("WriteStoredBlockToBuffer", size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    uint8_t* write_ptr = (uint8_t*)mem;

    memcpy(write_ptr, stored_block->m_BlockIndex->m_BlockHash, block_index_data_size);
    write_ptr += block_index_data_size;
    memcpy(write_ptr, stored_block->m_BlockData, stored_block->m_BlockChunksDataSize);

    *out_size = size;
    *out_buffer = mem;
    return 0;
}

int Longtail_ReadStoredBlockFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_StoredBlock** out_stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_stored_block != 0, return EINVAL)

    size_t block_mem_size = Longtail_GetStoredBlockSize(size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("ReadStoredBlockFromBuffer", block_mem_size);
    if (!stored_block)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    void* block_data = &((uint8_t*)stored_block)[block_mem_size - size];
    memcpy(block_data, buffer, size);
    int err = Longtail_InitStoredBlockFromData(
        stored_block,
        block_data,
        size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData() failed with %d", err)
        Longtail_Free(stored_block);
        return err;
    }
    stored_block->Dispose = ReadStoredBlock_Dispose;
    *out_stored_block = stored_block;
    return 0;
}

int Longtail_WriteStoredBlock(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_StoredBlock* stored_block,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile block_file_handle;
    int err = storage_api->OpenWriteFile(storage_api, path, 0, &block_file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenWriteFile() failed with %d", err)
        return err;
    }
    uint32_t write_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    err = storage_api->Write(storage_api, block_file_handle, write_offset, block_index_data_size, stored_block->m_BlockIndex->m_BlockHash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        return err;
    }
    write_offset += block_index_data_size;

    err = storage_api->Write(storage_api, block_file_handle, write_offset, stored_block->m_BlockChunksDataSize, stored_block->m_BlockData);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, block_file_handle);
        return err;
    }
    storage_api->CloseFile(storage_api, block_file_handle);

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "wrote %u bytes", block_index_data_size + stored_block->m_BlockChunksDataSize)

    return 0;
}

int Longtail_ReadStoredBlock(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_StoredBlock** out_stored_block)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_stored_block != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, path, &f);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_INFO : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint64_t stored_block_data_size;
    err = storage_api->GetSize(storage_api, f, &stored_block_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->GetSize() failed with %d", err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    size_t block_mem_size = Longtail_GetStoredBlockSize(stored_block_data_size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("ReadStoredBlock", block_mem_size);
    if (!stored_block)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        storage_api->CloseFile(storage_api, f);
        return ENOMEM;
    }
    void* block_data = &((uint8_t*)stored_block)[block_mem_size - stored_block_data_size];
    err = storage_api->Read(storage_api, f, 0, stored_block_data_size, block_data);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(stored_block);
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    storage_api->CloseFile(storage_api, f);
    err = Longtail_InitStoredBlockFromData(
        stored_block,
        block_data,
        stored_block_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData() failed with %d", err)
        Longtail_Free(stored_block);
        return err;
    }
    stored_block->Dispose = ReadStoredBlock_Dispose;
    *out_stored_block = stored_block;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "read %" PRIu64 " bytes", stored_block_data_size)

    return 0;
}

static uint32_t GetUniqueHashes(
    uint32_t hash_count,
    const TLongtail_Hash* hashes,
    uint32_t* out_unique_hash_indexes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_count, "%u"),
        LONGTAIL_LOGFIELD(hashes, "%p"),
        LONGTAIL_LOGFIELD(out_unique_hash_indexes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, hash_count != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, hash_count == 0 || out_unique_hash_indexes != 0, return 0)

    struct Longtail_LookupTable* lookup_table = LongtailPrivate_LookupTable_Create(Longtail_Alloc("GetUniqueHashes", LongtailPrivate_LookupTable_GetSize(hash_count)), hash_count, 0);
    if (!lookup_table)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    uint32_t unique_hash_count = 0;
    for (uint32_t i = 0; i < hash_count; ++i)
    {
        TLongtail_Hash hash = hashes[i];
        uint32_t* lookup_index = LongtailPrivate_LookupTable_PutUnique(lookup_table, hash, unique_hash_count);
        if (lookup_index == 0)
        {
            out_unique_hash_indexes[unique_hash_count++] = i;
        }
        else
        {
            // Take the last chunk hash we find
            out_unique_hash_indexes[*lookup_index] = i;
        }
    }
    Longtail_Free(lookup_table);
    lookup_table = 0;
    return unique_hash_count;
}

int Longtail_GetRequiredChunkHashes(
    const struct Longtail_VersionIndex* version_index,
    const struct Longtail_VersionDiff* version_diff,
    uint32_t* out_chunk_count,
    TLongtail_Hash* out_chunk_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_count, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (*version_index->m_ChunkCount == 0) ||  out_chunk_hashes != 0, return EINVAL)

    uint32_t max_chunk_count = *version_index->m_ChunkCount;
    void* work_mem = Longtail_Alloc("GetRequiredChunkHashes", LongtailPrivate_LookupTable_GetSize(max_chunk_count));
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    struct Longtail_LookupTable* chunk_lookup = LongtailPrivate_LookupTable_Create(work_mem, max_chunk_count, 0);

    uint32_t chunk_count = 0;

    uint32_t added_asset_count = *version_diff->m_TargetAddedCount;
    for (uint32_t a = 0; a < added_asset_count; ++a)
    {
        uint32_t asset_index = version_diff->m_TargetAddedAssetIndexes[a];
        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        for (uint32_t ci = 0; ci < asset_chunk_count; ++ci)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + ci];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            if (0 == LongtailPrivate_LookupTable_PutUnique(chunk_lookup, chunk_hash, chunk_count))
            {
                out_chunk_hashes[chunk_count] = chunk_hash;
                ++chunk_count;
            }
        }
    }
    uint32_t modified_asset_count = *version_diff->m_ModifiedContentCount;
    for (uint32_t a = 0; a < modified_asset_count; ++a)
    {
        uint32_t asset_index = version_diff->m_TargetContentModifiedAssetIndexes[a];
        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        for (uint32_t ci = 0; ci < asset_chunk_count; ++ci)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + ci];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            if (0 == LongtailPrivate_LookupTable_PutUnique(chunk_lookup, chunk_hash, chunk_count))
            {
                out_chunk_hashes[chunk_count] = chunk_hash;
                ++chunk_count;
            }
        }
    }

    Longtail_Free(work_mem);
    *out_chunk_count = chunk_count;

    return 0;
}

struct AssetPartLookup
{
    uint64_t* m_AssetOffsets;
    uint32_t* m_AssetIndexes;
    uint32_t* m_ChunkIndexes;
    uint32_t* m_Tags;
    struct Longtail_LookupTable* m_ChunkHashToIndex;
};

static int CreateAssetPartLookup(
    struct Longtail_VersionIndex* version_index,
    struct AssetPartLookup** out_assert_part_lookup)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(out_assert_part_lookup, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_assert_part_lookup != 0, return EINVAL)

    uint32_t asset_chunk_index_count = *version_index->m_AssetChunkIndexCount;
    size_t asset_part_lookup_size =
        sizeof(struct AssetPartLookup) +
        sizeof(uint64_t) * asset_chunk_index_count +
        sizeof(uint32_t) * asset_chunk_index_count +
        sizeof(uint32_t) * asset_chunk_index_count +
        sizeof(uint32_t) * asset_chunk_index_count +
        LongtailPrivate_LookupTable_GetSize(asset_chunk_index_count);
    struct AssetPartLookup* asset_part_lookup = (struct AssetPartLookup*)Longtail_Alloc("CreateAssetPartLookup", asset_part_lookup_size);
    if (!asset_part_lookup_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)&asset_part_lookup[1];

    asset_part_lookup->m_AssetOffsets = (uint64_t*)p;
    p += sizeof(uint64_t) * asset_chunk_index_count;

    asset_part_lookup->m_AssetIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_chunk_index_count;

    asset_part_lookup->m_ChunkIndexes = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_chunk_index_count;

    asset_part_lookup->m_Tags = (uint32_t*)p;
    p += sizeof(uint32_t) * asset_chunk_index_count;

    asset_part_lookup->m_ChunkHashToIndex = LongtailPrivate_LookupTable_Create(p, asset_chunk_index_count, 0);

    uint32_t unique_chunk_count = 0;
    uint32_t asset_count = *version_index->m_AssetCount;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunk_offset = 0;
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
            LONGTAIL_FATAL_ASSERT(ctx, asset_chunk_index_start + asset_chunk_index < *version_index->m_AssetChunkIndexCount, return EINVAL)
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            LONGTAIL_FATAL_ASSERT(ctx, chunk_index < *version_index->m_ChunkCount, return EINVAL)
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            uint32_t tag = version_index->m_ChunkTags[chunk_index];
            if (0 == LongtailPrivate_LookupTable_PutUnique(asset_part_lookup->m_ChunkHashToIndex, chunk_hash, unique_chunk_count))
            {
                asset_part_lookup->m_AssetOffsets[unique_chunk_count] = asset_chunk_offset;
                asset_part_lookup->m_AssetIndexes[unique_chunk_count] = asset_index;
                asset_part_lookup->m_ChunkIndexes[unique_chunk_count] = unique_chunk_count;
                asset_part_lookup->m_Tags[unique_chunk_count] = tag;
                unique_chunk_count++;
            }
            asset_chunk_offset += chunk_size;
        }
    }
    *out_assert_part_lookup = asset_part_lookup;
    return 0;
}

struct WriteBlockJob
{
    struct Longtail_AsyncPutStoredBlockAPI m_AsyncCompleteAPI;
    struct Longtail_StorageAPI* m_SourceStorageAPI;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    uint32_t m_JobID;
    struct Longtail_StoredBlock* m_StoredBlock;
    const char* m_AssetsFolder;
    const struct Longtail_StoreIndex* m_StoreIndex;
    const struct Longtail_VersionIndex* m_VersionIndex;
    struct AssetPartLookup* m_AssetPartLookup;
    uint32_t m_BlockIndex;
    int m_PutStoredBlockErr;
};

static void BlockWriterJobOnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockWriterJobOnComplete() caller failed with %d", err)
    }
    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api != 0, return)
    struct WriteBlockJob* job = (struct WriteBlockJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_AsyncCompleteAPI.OnComplete != 0, return);
    LONGTAIL_FATAL_ASSERT(ctx, job->m_StoredBlock != 0, return);
    LONGTAIL_FATAL_ASSERT(ctx, job->m_JobID != 0, return);
    uint32_t job_id = job->m_JobID;
    SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
    job->m_JobID = 0;
    job->m_PutStoredBlockErr = err;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job_id);
}

static int DisposePutBlock(struct Longtail_StoredBlock* stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    Longtail_Free(stored_block);
    return 0;
}

static int WriteContentBlockJob(void* context, uint32_t job_id, int detected_error)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct WriteBlockJob* job = (struct WriteBlockJob*)context;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_JobID == 0, return EINVAL);

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteContentBlockJob aborted due to previous error %d", detected_error)
        return 0;
    }

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        LONGTAIL_MONITOR_BLOCK_SAVED(job->m_StoreIndex, job->m_BlockIndex, job->m_PutStoredBlockErr);
        return job->m_PutStoredBlockErr;
    }

    LONGTAIL_MONTITOR_BLOCK_COMPOSE(job->m_StoreIndex, job->m_BlockIndex);

    struct Longtail_StorageAPI* source_storage_api = job->m_SourceStorageAPI;
    struct Longtail_BlockStoreAPI* block_store_api = job->m_BlockStoreAPI;

    const struct Longtail_StoreIndex* store_index = job->m_StoreIndex;

    TLongtail_Hash block_hash = store_index->m_BlockHashes[job->m_BlockIndex];

    uint32_t chunk_count = store_index->m_BlockChunkCounts[job->m_BlockIndex];
    uint32_t first_chunk_index = store_index->m_BlockChunksOffsets[job->m_BlockIndex];

    uint32_t block_data_size = 0;
    for (uint32_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        uint32_t chunk_size = store_index->m_ChunkSizes[chunk_index];
        block_data_size += chunk_size;
    }

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t stored_block_size = sizeof(struct Longtail_StoredBlock);
    size_t put_block_mem_size =
        stored_block_size +
        block_index_size +
        block_data_size;

    void* put_block_mem = Longtail_Alloc("WriteContentBlockJob", put_block_mem_size);
    if (!put_block_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM);
        return ENOMEM;
    }

    char* p = (char*)put_block_mem;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)p;
    p += stored_block_size;
    struct Longtail_BlockIndex* block_index_ptr = (struct Longtail_BlockIndex*)p;
    p += block_index_size;
    char* block_data_buffer = p;

    char* write_buffer = block_data_buffer;

    uint32_t last_asset_index = 0;
    uint32_t path_name_offset = 0xffffffffu;
    uint32_t tag = 0;

    Longtail_StorageAPI_HOpenFile file_handle = 0;
    uint64_t asset_file_size = 0;
    uint32_t write_offset = 0;

    for (uint32_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = store_index->m_ChunkSizes[chunk_index];

        uint32_t* asset_part_index = LongtailPrivate_LookupTable_Get(job->m_AssetPartLookup->m_ChunkHashToIndex, chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, asset_part_index != 0, return EINVAL)
        uint32_t next_asset_index = *asset_part_index;

        uint32_t next_tag = job->m_AssetPartLookup->m_Tags[next_asset_index];
        if (path_name_offset != 0xffffffffu && tag != next_tag)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "WriteContentBlockJob(%p, %u, %d): Warning: Inconsistent tag type for chunks inside block 0x%" PRIx64 ", retaining 0x%" PRIx64 "",
                context, job, job_id, detected_error,
                block_hash, tag)
        }
        else
        {
            tag = next_tag;
        }

        uint32_t asset_index = job->m_AssetPartLookup->m_AssetIndexes[next_asset_index];
        uint32_t next_path_name_offset = job->m_VersionIndex->m_NameOffsets[asset_index];
        if (next_path_name_offset != path_name_offset)
        {
            if (file_handle)
            {
                source_storage_api->CloseFile(source_storage_api, file_handle);
                file_handle = 0;
                LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, last_asset_index);
            }
            const char* asset_path = &job->m_VersionIndex->m_NameData[next_path_name_offset];
            LONGTAIL_FATAL_ASSERT(ctx, !IsDirPath(asset_path), return EINVAL)

            char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
            int err = source_storage_api->OpenReadFile(source_storage_api, full_path, &file_handle);
            LONGTAIL_MONTITOR_ASSET_OPEN(job->m_VersionIndex, asset_index, err);
            Longtail_Free(full_path);
            full_path = 0;
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->OpenReadFile() failed with %d", err);
                Longtail_Free(put_block_mem);
                return err;
            }
            last_asset_index = asset_index;
            uint64_t next_asset_file_size = 0;
            err = source_storage_api->GetSize(source_storage_api, file_handle, &next_asset_file_size);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->GetSize() failed with %d", err);
                Longtail_Free(put_block_mem);
                LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, last_asset_index);
                source_storage_api->CloseFile(source_storage_api, file_handle);
                return err;
            }
            asset_file_size = next_asset_file_size;
            path_name_offset = next_path_name_offset;
        }

        uint64_t asset_offset = job->m_AssetPartLookup->m_AssetOffsets[next_asset_index];
        if (asset_file_size < (asset_offset + chunk_size))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Source asset file does not match indexed size %" PRIu64 " < %" PRIu64,
                asset_file_size, (asset_offset + chunk_size))
            Longtail_Free(put_block_mem);
            source_storage_api->CloseFile(source_storage_api, file_handle);
            LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, asset_index);
            return EBADF;
        }
        int err = source_storage_api->Read(source_storage_api, file_handle, asset_offset, chunk_size, write_buffer + write_offset);
        LONGTAIL_MONTITOR_ASSET_READ(job->m_StoreIndex, job->m_VersionIndex, asset_index, asset_offset, chunk_size, chunk_hash, job->m_BlockIndex, write_offset, err);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->Read() failed with %d", err);
            Longtail_Free(put_block_mem);
            source_storage_api->CloseFile(source_storage_api, file_handle);
            LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, last_asset_index);
            return err;
        }
        write_offset += chunk_size;
    }

    if (file_handle)
    {
        source_storage_api->CloseFile(source_storage_api, file_handle);
        file_handle = 0;
        LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, last_asset_index);
    }

    Longtail_InitBlockIndex(block_index_ptr, chunk_count);
    memmove(block_index_ptr->m_ChunkHashes, &store_index->m_ChunkHashes[first_chunk_index], sizeof(TLongtail_Hash) * chunk_count);
    memmove(block_index_ptr->m_ChunkSizes, &store_index->m_ChunkSizes[first_chunk_index], sizeof(uint32_t) * chunk_count);
    *block_index_ptr->m_BlockHash = block_hash;
    *block_index_ptr->m_HashIdentifier = *store_index->m_HashIdentifier;
    *block_index_ptr->m_Tag = tag;
    *block_index_ptr->m_ChunkCount = chunk_count;
    job->m_StoredBlock = stored_block;
    job->m_StoredBlock->Dispose = DisposePutBlock;
    job->m_StoredBlock->m_BlockIndex = block_index_ptr;
    job->m_StoredBlock->m_BlockData = block_data_buffer;
    job->m_StoredBlock->m_BlockChunksDataSize = block_data_size;

    job->m_JobID = job_id;
    job->m_AsyncCompleteAPI.OnComplete = BlockWriterJobOnComplete;

    LONGTAIL_MONITOR_BLOCK_SAVE(job->m_StoreIndex, job->m_BlockIndex, put_block_mem_size);
    int err = block_store_api->PutStoredBlock(block_store_api, job->m_StoredBlock, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "block_store_api->PutStoredBlock() failed with %d", err);
        SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
        job->m_JobID = 0;
        LONGTAIL_MONITOR_BLOCK_SAVED(job->m_StoreIndex, job->m_BlockIndex, err);
        return err;
    }

    return EBUSY;
}

int Longtail_WriteContent(
    struct Longtail_StorageAPI* source_storage_api,
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    struct Longtail_StoreIndex* store_index,
    struct Longtail_VersionIndex* version_index,
    const char* assets_folder)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(source_storage_api, "%p"),
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(assets_folder, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, source_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, assets_folder != 0, return EINVAL)

    uint32_t block_count = *store_index->m_BlockCount;
    if (block_count == 0)
    {
        return 0;
    }

    uint32_t version_chunk_count = *version_index->m_ChunkCount;
    uint32_t version_store_index_chunk_count = *store_index->m_ChunkCount;

    size_t chunk_lookup_size = LongtailPrivate_LookupTable_GetSize(version_chunk_count);
    size_t chunk_sizes_size = sizeof(uint32_t) * version_store_index_chunk_count;
    size_t write_block_jobs_size = sizeof(struct WriteBlockJob) * block_count;
    size_t funcs_size = sizeof(Longtail_JobAPI_JobFunc*) * block_count;
    size_t ctxs_size = sizeof(void*) * block_count;

    size_t work_mem_size =
        chunk_lookup_size +
        chunk_sizes_size +
        write_block_jobs_size +
        funcs_size +
        ctxs_size;

    void* work_mem = Longtail_Alloc("WriteContent", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;

    struct Longtail_LookupTable* chunk_lookup = LongtailPrivate_LookupTable_Create(p, version_chunk_count, 0);
    p += chunk_lookup_size;
    uint32_t* chunk_sizes = (uint32_t*)p;
    p += chunk_sizes_size;
    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)p;
    p += write_block_jobs_size;
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)p;
    p += funcs_size;
    void** ctxs = (void**)p;
    p += ctxs_size;

    for (uint32_t c = 0; c < version_chunk_count; ++c)
    {
        LongtailPrivate_LookupTable_Put(chunk_lookup, version_index->m_ChunkHashes[c], c);
    }

    for (uint32_t c = 0; c < version_store_index_chunk_count; ++c)
    {
        uint32_t* version_chunk_index = LongtailPrivate_LookupTable_Get(chunk_lookup, store_index->m_ChunkHashes[c]);
        if (version_chunk_index == 0)
        {
            Longtail_Free(work_mem);
            return EINVAL;
        }
        chunk_sizes[c] = version_index->m_ChunkSizes[*version_chunk_index];
    }

    struct AssetPartLookup* asset_part_lookup;
    int err = CreateAssetPartLookup(version_index, &asset_part_lookup);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateAssetPartLookup() failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }

    uint32_t job_count = 0;
    for (uint32_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = store_index->m_BlockHashes[block_index];

        struct WriteBlockJob* job = &write_block_jobs[job_count];
        job->m_AsyncCompleteAPI.m_API.Dispose = 0;
        job->m_AsyncCompleteAPI.OnComplete = 0;
        job->m_SourceStorageAPI = source_storage_api;
        job->m_BlockStoreAPI = block_store_api;
        job->m_JobAPI = job_api;
        job->m_JobID = 0;
        job->m_StoredBlock = 0;
        job->m_AssetsFolder = assets_folder;
        job->m_StoreIndex = store_index;
        job->m_VersionIndex = version_index;
        job->m_BlockIndex = block_index;
        job->m_AssetPartLookup = asset_part_lookup;

        funcs[job_count] = WriteContentBlockJob;
        ctxs[job_count] = job;

        ++job_count;
    }

    uint32_t jobs_submitted = 0;
    err = Longtail_RunJobsBatched(
        job_api,
        progress_api,
        optional_cancel_api,
        optional_cancel_token,
        job_count,
        funcs,
        ctxs,
        &jobs_submitted);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RunJobsBatched() failed with %d", err)
        Longtail_Free(asset_part_lookup);
        Longtail_Free(work_mem);
        return err;
    }

    Longtail_Free(asset_part_lookup);
    asset_part_lookup = 0;

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "WriteBlockJob() failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }

    Longtail_Free(work_mem);
    return 0;
}

struct BlockReaderJob
{
    struct Longtail_AsyncGetStoredBlockAPI m_AsyncCompleteAPI;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    uint32_t m_JobID;
    TLongtail_Hash m_BlockHash;
    struct Longtail_StoredBlock* m_StoredBlock;
    int m_GetStoredBlockErr;
};

void BlockReaderJobOnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockReaderJobOnComplete() failed with %d", err)
    }
    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api != 0, return)
    struct BlockReaderJob* job = (struct BlockReaderJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_AsyncCompleteAPI.OnComplete != 0, return);
    job->m_GetStoredBlockErr = err;
    job->m_StoredBlock = stored_block;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job->m_JobID);
}

static int BlockReader(void* context, uint32_t job_id, int detected_error)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct BlockReaderJob* job = (struct BlockReaderJob*)context;

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "BlockReader aborted due to previous error %d", detected_error)
        return 0;
    }

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        return job->m_GetStoredBlockErr;
    }

    job->m_JobID = job_id;
    job->m_StoredBlock = 0;
    job->m_AsyncCompleteAPI.OnComplete = BlockReaderJobOnComplete;
    
    int err = job->m_BlockStoreAPI->GetStoredBlock(job->m_BlockStoreAPI, job->m_BlockHash, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job->m_BlockStoreAPI->GetStoredBlock() failed with %d", err)
        return err;
    }
    return EBUSY;
}

static int WriteReady(void* context, uint32_t job_id, int detected_error)
{
    // Nothing to do here, we are just a syncronization point
    return 0;
}

#define MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE  32u

struct WritePartialAssetFromBlocksJob
{
    struct Longtail_StorageAPI* m_VersionStorageAPI;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    struct Longtail_ProgressAPI* m_ProgressAPI;
    struct Longtail_CancelAPI* m_OptionalCancelAPI;
    Longtail_CancelAPI_HCancelToken m_OptionalCancelToken;
    const struct Longtail_StoreIndex* m_StoreIndex;
    const struct Longtail_VersionIndex* m_VersionIndex;
    const char* m_VersionFolder;
    struct Longtail_LookupTable* m_ChunkHashToBlockIndex;
    uint32_t m_AssetIndex;
    int m_RetainPermissions;

    Longtail_JobAPI_Group m_JobGroup;
    struct BlockReaderJob m_BlockReaderJobs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    uint32_t m_BlockReaderJobCount;

    uint32_t m_AssetChunkIndexOffset;
    uint32_t m_AssetChunkCount;

    Longtail_StorageAPI_HOpenFile m_AssetOutputFile;
};

int WritePartialAssetFromBlocks(void* context, uint32_t job_id, int detected_error);

static uint32_t GetMaxParallelBlockReadJobs(struct Longtail_JobAPI* job_api)
{
    uint32_t worker_count = job_api->GetWorkerCount(job_api);
    if (worker_count > 2)
    {
        worker_count--;
    }
    else if (worker_count == 0)
    {
        worker_count = 1;
    }
    const uint32_t max_parallell_block_read_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;
    return max_parallell_block_read_jobs;
}

// Returns the write sync task, or the write task if there is no need for reading new blocks
static int CreatePartialAssetWriteJob(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_folder,
    struct Longtail_LookupTable* chunk_hash_to_block_index,
    uint32_t asset_index,
    int retain_permissions,
    Longtail_JobAPI_Group job_group,
    struct WritePartialAssetFromBlocksJob* job,
    uint32_t asset_chunk_index_offset,
    Longtail_StorageAPI_HOpenFile asset_output_file,
    Longtail_JobAPI_Jobs* out_jobs)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_folder, "%s"),
        LONGTAIL_LOGFIELD(chunk_hash_to_block_index, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d"),
        LONGTAIL_LOGFIELD(job_group, "%p"),
        LONGTAIL_LOGFIELD(job, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_index_offset, "%u"),
        LONGTAIL_LOGFIELD(asset_output_file, "%p"),
        LONGTAIL_LOGFIELD(out_jobs, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, block_store_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_storage_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, store_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_folder !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hash_to_block_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_index < *version_index->m_AssetCount, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_jobs !=0, return EINVAL)

    job->m_VersionStorageAPI = version_storage_api;
    job->m_BlockStoreAPI = block_store_api;
    job->m_JobAPI = job_api;
    job->m_ProgressAPI = progress_api;
    job->m_OptionalCancelAPI = optional_cancel_api;
    job->m_OptionalCancelToken = optional_cancel_token;
    job->m_StoreIndex = store_index;
    job->m_VersionIndex = version_index;
    job->m_VersionFolder = version_folder;
    job->m_ChunkHashToBlockIndex = chunk_hash_to_block_index;
    job->m_AssetIndex = asset_index;
    job->m_JobGroup = job_group;
    job->m_RetainPermissions = retain_permissions;
    job->m_BlockReaderJobCount = 0;
    job->m_AssetChunkIndexOffset = asset_chunk_index_offset;
    job->m_AssetChunkCount = 0;
    job->m_AssetOutputFile = asset_output_file;

    uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
    uint32_t chunk_start_index_offset = chunk_index_start + asset_chunk_index_offset;
    uint32_t chunk_index_end = chunk_index_start + version_index->m_AssetChunkCounts[asset_index];
    uint32_t chunk_index_offset = chunk_start_index_offset;

    Longtail_JobAPI_JobFunc block_read_funcs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    void* block_read_ctx[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];

    const uint32_t max_parallell_block_read_jobs = GetMaxParallelBlockReadJobs(job_api);

    while (chunk_index_offset != chunk_index_end && job->m_BlockReaderJobCount <= max_parallell_block_read_jobs)
    {
        uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
        uint32_t block_index = *block_index_ptr;
        TLongtail_Hash block_hash = store_index->m_BlockHashes[block_index];
        int has_block = 0;
        for (uint32_t d = 0; d < job->m_BlockReaderJobCount; ++d)
        {
            if (job->m_BlockReaderJobs[d].m_BlockHash == block_hash)
            {
                has_block = 1;
                break;
            }
        }
        if (!has_block)
        {
            if (job->m_BlockReaderJobCount == max_parallell_block_read_jobs)
            {
                break;
            }
            struct BlockReaderJob* block_job = &job->m_BlockReaderJobs[job->m_BlockReaderJobCount];
            block_job->m_BlockStoreAPI = block_store_api;
            block_job->m_BlockHash = block_hash;
            block_job->m_AsyncCompleteAPI.m_API.Dispose = 0;
            block_job->m_AsyncCompleteAPI.OnComplete = 0;
            block_job->m_JobAPI = job_api;
            block_job->m_JobID = 0;
            block_job->m_StoredBlock = 0;
            block_read_funcs[job->m_BlockReaderJobCount] = BlockReader;
            block_read_ctx[job->m_BlockReaderJobCount] = block_job;
            ++job->m_BlockReaderJobCount;
        }
        ++job->m_AssetChunkCount;
        ++chunk_index_offset;
    }

    Longtail_JobAPI_JobFunc write_funcs[1] = { WritePartialAssetFromBlocks };
    void* write_ctx[1] = { job };
    Longtail_JobAPI_Jobs write_job;
    int err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, 1, write_funcs, write_ctx, 0, &write_job);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
        return err;
    }

    if (job->m_BlockReaderJobCount > 0)
    {
        Longtail_JobAPI_Jobs block_read_jobs;
        err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, job->m_BlockReaderJobCount, block_read_funcs, block_read_ctx, 1, &block_read_jobs);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
            return err;
        }
        Longtail_JobAPI_JobFunc sync_write_funcs[1] = { WriteReady };
        void* sync_write_ctx[1] = { 0 };
        Longtail_JobAPI_Jobs write_sync_job;
        err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, 1, sync_write_funcs, sync_write_ctx, 0, &write_sync_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
            return err;
        }

        err = job_api->AddDependecies(job_api, 1, write_job, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
        err = job_api->AddDependecies(job_api, 1, write_job, job->m_BlockReaderJobCount, block_read_jobs);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
        err = job_api->ReadyJobs(job_api, job->m_BlockReaderJobCount, block_read_jobs);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)

        *out_jobs = write_sync_job;
        return 0;
    }
    *out_jobs = write_job;
    return 0;
}

int WritePartialAssetFromBlocks(void* context, uint32_t job_id, int detected_error)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context !=0, return EINVAL)
    struct WritePartialAssetFromBlocksJob* job = (struct WritePartialAssetFromBlocksJob*)context;

    uint32_t block_reader_job_count = job->m_BlockReaderJobCount;

    if ((!job->m_AssetOutputFile) && job->m_AssetChunkIndexOffset)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WritePartialAssetFromBlocks(%p, %u, %d) aborted due to previous error %d",
            context, job, job_id, detected_error, detected_error)
        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReaderJobs[d].m_StoredBlock);
        }
        return 0;
    }

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WritePartialAssetFromBlocks aborted due to previous error %d", detected_error)

        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReaderJobs[d].m_StoredBlock);
        }
        if (job->m_AssetOutputFile)
        {
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;
        }
        return 0;
    }

    // Need to fetch all the data we need from the context since we will reuse the context
    int block_reader_errors = 0;
    TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    struct Longtail_StoredBlock* stored_block[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    for (uint32_t d = 0; d < block_reader_job_count; ++d)
    {
        if (job->m_BlockReaderJobs[d].m_GetStoredBlockErr)
        {
            block_reader_errors = block_reader_errors == 0 ? job->m_BlockReaderJobs[d].m_GetStoredBlockErr : block_reader_errors;
            block_hashes[d] = 0;
            stored_block[d] = 0;
            continue;
        }
        block_hashes[d] = job->m_BlockReaderJobs[d].m_BlockHash;
        stored_block[d] = job->m_BlockReaderJobs[d].m_StoredBlock;
    }

    if (block_reader_errors)
    {
        LONGTAIL_LOG(ctx, (block_reader_errors == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "BlockRead() failed with %d", block_reader_errors)
        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            if (stored_block[d] && stored_block[d]->Dispose)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
        }

        if (job->m_AssetOutputFile)
        {
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;
        }
        return block_reader_errors;
    }

    uint32_t write_chunk_index_offset = job->m_AssetChunkIndexOffset;
    uint32_t write_chunk_count = job->m_AssetChunkCount;
    uint32_t asset_chunk_count = job->m_VersionIndex->m_AssetChunkCounts[job->m_AssetIndex];
    const char* asset_path = &job->m_VersionIndex->m_NameData[job->m_VersionIndex->m_NameOffsets[job->m_AssetIndex]];

    if (!job->m_AssetOutputFile)
    {
        char* full_asset_path = job->m_VersionStorageAPI->ConcatPath(job->m_VersionStorageAPI, job->m_VersionFolder, asset_path);
        int err = EnsureParentPathExists(job->m_VersionStorageAPI, full_asset_path);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
            Longtail_Free(full_asset_path);
            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            return err;
        }
        if (IsDirPath(full_asset_path))
        {
            // Remove trailing forward slash
            full_asset_path[strlen(full_asset_path) - 1] = '\0';
            err = job->m_VersionStorageAPI->CreateDir(job->m_VersionStorageAPI, full_asset_path);
            if (err == EEXIST)
            {
                err = 0;
            }
            if (err != 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->CreateDir() failed with %d", err)
                Longtail_Free(full_asset_path);
                return err;
            }
            Longtail_Free(full_asset_path);
            return err;
        }

        uint16_t permissions;
        err = job->m_VersionStorageAPI->GetPermissions(job->m_VersionStorageAPI, full_asset_path, &permissions);
        if (err && (err != ENOENT))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->GetPermissions() failed with %d", err)
            Longtail_Free(full_asset_path);
            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            return err;
        }

        if (err != ENOENT)
        {
            if (!(permissions & Longtail_StorageAPI_UserWriteAccess))
            {
                err = job->m_VersionStorageAPI->SetPermissions(job->m_VersionStorageAPI, full_asset_path, permissions | (Longtail_StorageAPI_UserWriteAccess));
                if (err)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->SetPermissions() failed with %d", err)
                    Longtail_Free(full_asset_path);
                    for (uint32_t d = 0; d < block_reader_job_count; ++d)
                    {
                        stored_block[d]->Dispose(stored_block[d]);
                        stored_block[d] = 0;
                    }
                    return err;
                }
            }
        }

        uint64_t asset_size = job->m_VersionIndex->m_AssetSizes[job->m_AssetIndex];
        err = job->m_VersionStorageAPI->OpenWriteFile(job->m_VersionStorageAPI, full_asset_path, asset_size, &job->m_AssetOutputFile);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->OpenWriteFile() failed with %d", err)
            Longtail_Free(full_asset_path);
            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            return err;
        }
        Longtail_Free(full_asset_path);
        full_asset_path = 0;
    }

    Longtail_JobAPI_Jobs sync_write_job = 0;
    if (write_chunk_index_offset + write_chunk_count < asset_chunk_count)
    {
        int err = CreatePartialAssetWriteJob(
            job->m_BlockStoreAPI,
            job->m_VersionStorageAPI,
            job->m_JobAPI,
            job->m_ProgressAPI,
            job->m_OptionalCancelAPI,
            job->m_OptionalCancelToken,
            job->m_StoreIndex,
            job->m_VersionIndex,
            job->m_VersionFolder,
            job->m_ChunkHashToBlockIndex,
            job->m_AssetIndex,
            job->m_RetainPermissions,
            job->m_JobGroup,
            job,    // Reuse job
            write_chunk_index_offset + write_chunk_count,
            job->m_AssetOutputFile,
            &sync_write_job);

        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreatePartialAssetWriteJob() failed with %d", err)
            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            return err;
        }
        // Reading of blocks will start immediately
    }

    uint32_t chunk_index_offset = write_chunk_index_offset;
    uint32_t chunk_index_start = job->m_VersionIndex->m_AssetChunkIndexStarts[job->m_AssetIndex];

    uint64_t write_offset = 0;
    for (uint32_t c = 0; c < chunk_index_offset; ++c)
    {
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[chunk_index_start + c];
        uint32_t chunk_size = job->m_VersionIndex->m_ChunkSizes[chunk_index];
        write_offset += chunk_size;
    }

    uint32_t block_chunks_count = 0;
    for(uint32_t b = 0; b < block_reader_job_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = stored_block[b]->m_BlockIndex;
        block_chunks_count += *block_index->m_ChunkCount;
    }

    size_t block_chunks_lookup_size = LongtailPrivate_LookupTable_GetSize(block_chunks_count);
    size_t chunk_sizes_size = sizeof(uint32_t) * block_chunks_count;
    size_t chunk_offsets_size = sizeof(uint32_t) * block_chunks_count;
    size_t block_indexes_size = sizeof(uint32_t) * block_chunks_count;
    size_t buffer_size = 512*1024;

    size_t work_mem_size =
        block_chunks_lookup_size +
        chunk_sizes_size +
        chunk_offsets_size +
        block_indexes_size +
        buffer_size;
    void* work_mem = Longtail_Alloc("WritePartialAssetFromBlocks", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            stored_block[d]->Dispose(stored_block[d]);
            stored_block[d] = 0;
        }
        job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
        job->m_AssetOutputFile = 0;
        if (sync_write_job)
        {
            int err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
            LONGTAIL_FATAL_ASSERT(ctx, err == 0, err = 0)
        }
        return ENOMEM;
    }
    char* p = (char*)work_mem;
    struct Longtail_LookupTable* block_chunks_lookup = LongtailPrivate_LookupTable_Create(p, block_chunks_count, 0);
    p += block_chunks_lookup_size;
    uint32_t* chunk_sizes = (uint32_t*)p;
    p += chunk_sizes_size;
    uint32_t* chunk_offsets = (uint32_t*)p;
    p += chunk_offsets_size;
    uint32_t* block_indexes = (uint32_t*)p;
    p += block_indexes_size;
    char* buffer = p;

    uint32_t block_chunk_index_offset = 0;
    for(uint32_t b = 0; b < block_reader_job_count; ++b)
    {
        uint32_t chunk_offset = 0;
        struct Longtail_BlockIndex* block_index = stored_block[b]->m_BlockIndex;
        for (uint32_t c = 0; c < *block_index->m_ChunkCount; ++c)
        {
            TLongtail_Hash chunk_hash = block_index->m_ChunkHashes[c];
            uint32_t chunk_size = block_index->m_ChunkSizes[c];
            chunk_sizes[block_chunk_index_offset] = chunk_size;
            chunk_offsets[block_chunk_index_offset] = chunk_offset;
            block_indexes[block_chunk_index_offset] = b;
            chunk_offset += chunk_size;

            LongtailPrivate_LookupTable_Put(block_chunks_lookup, chunk_hash, block_chunk_index_offset);

            block_chunk_index_offset++;
        }
    }

    size_t buffer_used_size = 0;
    uint32_t chunk_index_end = write_chunk_index_offset + write_chunk_count;

    while (chunk_index_offset < chunk_index_end)
    {
        uint32_t asset_chunk_index = chunk_index_start + chunk_index_offset;
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[asset_chunk_index];
        TLongtail_Hash chunk_hash = job->m_VersionIndex->m_ChunkHashes[chunk_index];

        uint32_t* chunk_block_index = LongtailPrivate_LookupTable_Get(block_chunks_lookup, chunk_hash);
        if (chunk_block_index == 0)
        {
            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;
            if (sync_write_job)
            {
                int err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(ctx, err == 0, err = 0; return 0)
            }
            Longtail_Free(work_mem);
            return EINVAL;
        }

        uint32_t block_index = block_indexes[*chunk_block_index];
        uint32_t chunk_block_offset = chunk_offsets[*chunk_block_index];
        uint32_t chunk_size = chunk_sizes[*chunk_block_index];
        const char* block_data = (char*)stored_block[block_index]->m_BlockData;

        while(chunk_index_offset < (chunk_index_end - 1))
        {
            uint32_t next_chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[asset_chunk_index + 1];
            TLongtail_Hash next_chunk_hash = job->m_VersionIndex->m_ChunkHashes[next_chunk_index];

            uint32_t* next_chunk_block_index = LongtailPrivate_LookupTable_Get(block_chunks_lookup, next_chunk_hash);
            if (next_chunk_block_index == 0)
            {
                for (uint32_t d = 0; d < block_reader_job_count; ++d)
                {
                    stored_block[d]->Dispose(stored_block[d]);
                    stored_block[d] = 0;
                }
                job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
                job->m_AssetOutputFile = 0;
                if (sync_write_job)
                {
                    int err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                    LONGTAIL_FATAL_ASSERT(ctx, err == 0, err = 0; return 0)
                }
                Longtail_Free(work_mem);
                return EINVAL;
            }
            uint32_t next_block_index = block_indexes[*next_chunk_block_index];
            if (next_block_index != block_index)
            {
                break;
            }
            uint32_t next_chunk_block_offset = chunk_offsets[*next_chunk_block_index];
            if (next_chunk_block_offset != chunk_block_offset + chunk_size)
            {
                break;
            }
            uint32_t next_chunk_size = chunk_sizes[*next_chunk_block_index];
            chunk_size += next_chunk_size;
            ++chunk_index_offset;
        }

        if (buffer_used_size + chunk_size <= buffer_size)
        {
            if (buffer_used_size > 0 || (chunk_index_offset < chunk_index_end - 1))
            {
                memcpy(&buffer[buffer_used_size], &block_data[chunk_block_offset], chunk_size);
                buffer_used_size += chunk_size;
                ++chunk_index_offset;
                continue;
            }
        }

        if (buffer_used_size > 0)
        {
            int err = job->m_VersionStorageAPI->Write(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, buffer_used_size, buffer);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->Write() failed with %d", err)
                job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
                job->m_AssetOutputFile = 0;

                for (uint32_t d = 0; d < block_reader_job_count; ++d)
                {
                    stored_block[d]->Dispose(stored_block[d]);
                    stored_block[d] = 0;
                }
                if (sync_write_job)
                {
                    int sync_err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                    LONGTAIL_FATAL_ASSERT(ctx, sync_err == 0, return 0)
                }
                Longtail_Free(work_mem);
                return err;
            }
            write_offset += buffer_used_size;
            buffer_used_size = 0;
        }

        if (buffer_used_size + chunk_size <= buffer_size)
        {
            if (buffer_used_size > 0 || (chunk_index_offset < chunk_index_end - 1))
            {
                memcpy(&buffer[buffer_used_size], &block_data[chunk_block_offset], chunk_size);
                buffer_used_size += chunk_size;
                ++chunk_index_offset;
                continue;
            }
        }

        int err = job->m_VersionStorageAPI->Write(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, chunk_size, &block_data[chunk_block_offset]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->Write() failed with %d", err)
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;

            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            if (sync_write_job)
            {
                int sync_err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(ctx, sync_err == 0, return 0)
            }
            Longtail_Free(work_mem);
            return err;
        }
        write_offset += chunk_size;

        ++chunk_index_offset;
    }

    if (buffer_used_size > 0)
    {
        int err = job->m_VersionStorageAPI->Write(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, buffer_used_size, buffer);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->Write() failed with %d", err)
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;

            for (uint32_t d = 0; d < block_reader_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
                stored_block[d] = 0;
            }
            if (sync_write_job)
            {
                int sync_err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(ctx, sync_err == 0, return 0)
            }
            Longtail_Free(work_mem);
            return err;
        }
        write_offset += buffer_used_size;
        buffer_used_size = 0;
    }

    Longtail_Free(work_mem);

    for (uint32_t d = 0; d < block_reader_job_count; ++d)
    {
        stored_block[d]->Dispose(stored_block[d]);
        stored_block[d] = 0;
    }

    if (sync_write_job)
    {
        // We can now release the next write job which will in turn close the job->m_AssetOutputFile
        int err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
        if (err)
        {
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            return err;
        }
        return 0;
    }

    job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
    job->m_AssetOutputFile = 0;

    if (job->m_RetainPermissions)
    {
        char* full_asset_path = job->m_VersionStorageAPI->ConcatPath(job->m_VersionStorageAPI, job->m_VersionFolder, asset_path);
        int err = job->m_VersionStorageAPI->SetPermissions(job->m_VersionStorageAPI, full_asset_path, (uint16_t)job->m_VersionIndex->m_Permissions[job->m_AssetIndex]);
        Longtail_Free(full_asset_path);
        full_asset_path = 0;
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_VersionStorageAPI->SetPermissions() failed with %d", err)
            return err;
        }
    }

    return 0;
}

struct WriteAssetsFromBlockJob
{
    struct Longtail_StorageAPI* m_VersionStorageAPI;
    const struct Longtail_VersionIndex* m_VersionIndex;
    const char* m_VersionFolder;
    struct BlockReaderJob m_BlockReadJob;
    uint32_t m_BlockIndex;
    uint32_t* m_AssetIndexes;
    uint32_t m_AssetCount;
    int m_RetainPermissions;
};

static int WriteAssetsFromBlock(void* context, uint32_t job_id, int detected_error)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)

    struct WriteAssetsFromBlockJob* job = (struct WriteAssetsFromBlockJob*)context;
    struct Longtail_StorageAPI* version_storage_api = job->m_VersionStorageAPI;
    const char* version_folder = job->m_VersionFolder;
    const struct Longtail_VersionIndex* version_index = job->m_VersionIndex;
    uint32_t* asset_indexes = job->m_AssetIndexes;
    uint32_t asset_count = job->m_AssetCount;

    if (detected_error)
    {
       LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteAssetsFromBlock aborted due to previous error %d", detected_error)
       SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
       return 0;
    }

    if (job->m_BlockReadJob.m_GetStoredBlockErr)
    {
        LONGTAIL_LOG(ctx, (job->m_BlockReadJob.m_GetStoredBlockErr == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "BlockReadJob() failed with %d", job->m_BlockReadJob.m_GetStoredBlockErr)
        return job->m_BlockReadJob.m_GetStoredBlockErr;
    }

    const char* block_data = (char*)job->m_BlockReadJob.m_StoredBlock->m_BlockData;
    struct Longtail_BlockIndex* block_index = job->m_BlockReadJob.m_StoredBlock->m_BlockIndex;

    uint32_t block_chunks_count = *block_index->m_ChunkCount;

    size_t chuck_offsets_size = sizeof(uint32_t) * block_chunks_count;
    size_t block_chunks_lookup_size = LongtailPrivate_LookupTable_GetSize(block_chunks_count);
    size_t tmp_mem_size =
        chuck_offsets_size +
        block_chunks_lookup_size;

    char* tmp_mem = (char*)Longtail_Alloc("WriteAssetsFromBlock", tmp_mem_size);
    if (!tmp_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
        return ENOMEM;
    }
    struct Longtail_LookupTable* block_chunks_lookup = LongtailPrivate_LookupTable_Create(tmp_mem, block_chunks_count, 0);
    uint32_t* chunk_offsets = (uint32_t*)(&tmp_mem[block_chunks_lookup_size]);
    const uint32_t* chunk_sizes = block_index->m_ChunkSizes;

    uint32_t block_chunk_index_offset = 0;
    uint32_t chunk_offset = 0;
    for (uint32_t c = 0; c < block_chunks_count; ++c)
    {
        TLongtail_Hash chunk_hash = block_index->m_ChunkHashes[c];
        uint32_t chunk_size = block_index->m_ChunkSizes[c];
        chunk_offsets[block_chunk_index_offset] = chunk_offset;
        chunk_offset += chunk_size;

        LongtailPrivate_LookupTable_Put(block_chunks_lookup, chunk_hash, block_chunk_index_offset);

        block_chunk_index_offset++;
    }

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_folder, asset_path);
        int err = EnsureParentPathExists(version_storage_api, full_asset_path);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
            Longtail_Free(full_asset_path);
            SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
            Longtail_Free(tmp_mem);
            return err;
        }

        uint16_t permissions;
        err = version_storage_api->GetPermissions(version_storage_api, full_asset_path, &permissions);
        if (err && (err != ENOENT))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->GetPermissions() failed with %d", err)
            Longtail_Free(full_asset_path);
            SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
            Longtail_Free(tmp_mem);
            return err;
        }

        if (err != ENOENT)
        {
            if (!(permissions & Longtail_StorageAPI_UserWriteAccess))
            {
                err = version_storage_api->SetPermissions(version_storage_api, full_asset_path, permissions | (Longtail_StorageAPI_UserWriteAccess));
                if (err)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                    Longtail_Free(full_asset_path);
                    SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
                    Longtail_Free(tmp_mem);
                    return err;
                }
            }
        }

        Longtail_StorageAPI_HOpenFile asset_file;
        err = version_storage_api->OpenWriteFile(version_storage_api, full_asset_path, 0, &asset_file);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->OpenWriteFile() failed with %d", err)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
            Longtail_Free(tmp_mem);
            return err;
        }

        uint64_t asset_write_offset = 0;
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];

            uint32_t* chunk_block_index = LongtailPrivate_LookupTable_Get(block_chunks_lookup, chunk_hash);
            if (chunk_block_index == 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", err)
                version_storage_api->CloseFile(version_storage_api, asset_file);
                asset_file = 0;
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
                Longtail_Free(tmp_mem);
                return err;
            }

            uint32_t chunk_block_offset = chunk_offsets[*chunk_block_index];
            uint32_t chunk_size = chunk_sizes[*chunk_block_index];

            while(asset_chunk_index < (asset_chunk_count - 1))
            {
                uint32_t next_chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index + 1];
                TLongtail_Hash next_chunk_hash = version_index->m_ChunkHashes[next_chunk_index];
                uint32_t* next_chunk_block_index = LongtailPrivate_LookupTable_Get(block_chunks_lookup, next_chunk_hash);
                if (next_chunk_block_index == 0)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", err)
                    version_storage_api->CloseFile(version_storage_api, asset_file);
                    asset_file = 0;
                    Longtail_Free(full_asset_path);
                    full_asset_path = 0;
                    SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
                    Longtail_Free(tmp_mem);
                    return err;
                }

                uint32_t next_chunk_block_offset = chunk_offsets[*next_chunk_block_index];
                if (next_chunk_block_offset != chunk_block_offset + chunk_size)
                {
                    break;
                }
                uint32_t next_chunk_size = chunk_sizes[*next_chunk_block_index];
                chunk_size += next_chunk_size;
                ++asset_chunk_index;
            }

            err = version_storage_api->Write(version_storage_api, asset_file, asset_write_offset, chunk_size, &block_data[chunk_block_offset]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->Write() failed with %d", err)
                version_storage_api->CloseFile(version_storage_api, asset_file);
                asset_file = 0;
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
                Longtail_Free(tmp_mem);
                return err;
            }
            asset_write_offset += chunk_size;
        }

        version_storage_api->CloseFile(version_storage_api, asset_file);
        asset_file = 0;

        if (job->m_RetainPermissions)
        {
            err = version_storage_api->SetPermissions(version_storage_api, full_asset_path, version_index->m_Permissions[asset_index]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
                Longtail_Free(tmp_mem);
                return err;
            }
        }
        Longtail_Free(full_asset_path);
        full_asset_path = 0;
    }
    Longtail_Free(tmp_mem);

    SAFE_DISPOSE_STORED_BLOCK(job->m_BlockReadJob.m_StoredBlock);
    return 0;
}

struct AssetWriteList
{
    uint32_t m_BlockJobCount;
    uint32_t m_AssetJobCount;
    uint32_t* m_BlockJobAssetIndexes;
    uint32_t* m_AssetIndexJobs;
};

struct JobCompareContext
{
    const struct AssetWriteList* m_AssetWriteList;
    const uint32_t* asset_chunk_counts;
    const uint32_t* asset_chunk_index_starts;
    const uint32_t* asset_chunk_indexes;
    const TLongtail_Hash* chunk_hashes;
    struct Longtail_LookupTable* chunk_hash_to_block_index;
};

static uint32_t GetJobBlockIndex(struct JobCompareContext* c, uint32_t asset_index)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(c, "%p"),
        LONGTAIL_LOGFIELD(asset_index, "%u"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint32_t asset_chunk_offset = c->asset_chunk_index_starts[asset_index];
    uint32_t chunk_index = c->asset_chunk_indexes[asset_chunk_offset];

    TLongtail_Hash chunk_hash = c->chunk_hashes[chunk_index];

    const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(c->chunk_hash_to_block_index, chunk_hash);
    LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return 0)

    return *block_index_ptr;
}

static SORTFUNC(JobCompare)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    struct JobCompareContext* c = (struct JobCompareContext*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;

    uint32_t a_chunk_count = c->asset_chunk_counts[a];
    uint32_t b_chunk_count = c->asset_chunk_counts[b];
    if (a_chunk_count == 0)
    {
        if (b_chunk_count == 0)
        {
            return 0;
        }
        else
        {
            return -1;
        }
    }
    else if (b_chunk_count == 0)
    {
        return 1;
    }
    uint32_t a_block_index = GetJobBlockIndex(c, a);
    uint32_t b_block_index = GetJobBlockIndex(c, b);
    if (a_block_index < b_block_index)
    {
        return -1;
    }
    else if (a_block_index > b_block_index)
    {
        return 1;
    }
    return 0;
}

static struct AssetWriteList* CreateAssetWriteList(uint32_t asset_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(asset_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    size_t awl_size = sizeof(struct AssetWriteList) + sizeof(uint32_t) * asset_count + sizeof(uint32_t) * asset_count;
    struct AssetWriteList* awl = (struct AssetWriteList*)(Longtail_Alloc("CreateAssetWriteList", awl_size));
    if (!awl)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    awl->m_BlockJobCount = 0;
    awl->m_AssetJobCount = 0;
    awl->m_BlockJobAssetIndexes = (uint32_t*)(void*)&awl[1];
    awl->m_AssetIndexJobs = &awl->m_BlockJobAssetIndexes[asset_count];
    return awl;
}

static int BuildAssetWriteList(
    uint32_t asset_count,
    const uint32_t* optional_asset_indexes,
    uint32_t* name_offsets,
    const char* name_data,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* asset_chunk_counts,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_indexes,
    struct Longtail_LookupTable* chunk_hash_to_block_index,
    struct AssetWriteList** out_asset_write_list)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(asset_count, "%u"),
        LONGTAIL_LOGFIELD(optional_asset_indexes, "%p"),
        LONGTAIL_LOGFIELD(name_offsets, "%p"),
        LONGTAIL_LOGFIELD(name_data, "%p"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_counts, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_index_starts, "%p"),
        LONGTAIL_LOGFIELD(asset_chunk_indexes, "%p"),
        LONGTAIL_LOGFIELD(chunk_hash_to_block_index, "%p"),
        LONGTAIL_LOGFIELD(out_asset_write_list, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || name_offsets != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || name_data != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || asset_chunk_index_starts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_count == 0 || asset_chunk_indexes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hash_to_block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_asset_write_list != 0, return EINVAL)

    struct AssetWriteList* awl = CreateAssetWriteList(asset_count);
    if (awl == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateAssetWriteList() CreateAssetWriteList() failed with %d", ENOMEM)
        return ENOMEM;
    }

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        uint32_t asset_index = optional_asset_indexes ? optional_asset_indexes[i] : i;
        const char* path = &name_data[name_offsets[asset_index]];
        uint32_t chunk_count = asset_chunk_counts[asset_index];
        uint32_t asset_chunk_offset = asset_chunk_index_starts[asset_index];
        if (chunk_count == 0)
        {
            awl->m_AssetIndexJobs[awl->m_AssetJobCount] = asset_index;
            ++awl->m_AssetJobCount;
            continue;
        }
        uint32_t chunk_index = asset_chunk_indexes[asset_chunk_offset];
        TLongtail_Hash chunk_hash = chunk_hashes[chunk_index];
        uint32_t* content_block_index = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
        if (content_block_index == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", ENOENT)
            Longtail_Free(awl);
            return ENOENT;
        }

        int is_block_job = 1;
        for (uint32_t c = 1; c < chunk_count; ++c)
        {
            uint32_t next_chunk_index = asset_chunk_indexes[asset_chunk_offset + c];
            TLongtail_Hash next_chunk_hash = chunk_hashes[next_chunk_index];
            uint32_t* next_content_block_index = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, next_chunk_hash);
            if (next_content_block_index == 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", ENOENT)
                Longtail_Free(awl);
                return ENOENT;
            }
            if (*content_block_index != *next_content_block_index)
            {
                is_block_job = 0;
                // We don't break here since we want to validate that all the chunks are in the content index
            }
        }

        if (is_block_job)
        {
            awl->m_BlockJobAssetIndexes[awl->m_BlockJobCount] = asset_index;
            ++awl->m_BlockJobCount;
        }
        else
        {
            awl->m_AssetIndexJobs[awl->m_AssetJobCount] = asset_index;
            ++awl->m_AssetJobCount;
        }
    }

    struct JobCompareContext block_job_compare_context = {
            awl,
            asset_chunk_counts,
            asset_chunk_index_starts,
            asset_chunk_indexes,
            chunk_hashes,
            chunk_hash_to_block_index
        };
    QSORT(awl->m_BlockJobAssetIndexes, (size_t)awl->m_BlockJobCount, sizeof(uint32_t), JobCompare, &block_job_compare_context);
    QSORT(awl->m_AssetIndexJobs, (size_t)awl->m_AssetJobCount, sizeof(uint32_t), JobCompare, &block_job_compare_context);

    *out_asset_write_list = awl;
    return 0;
}

static int WriteAssets(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    struct Longtail_LookupTable* chunk_hash_to_block_index,
    struct AssetWriteList* awl,
    int retain_permssions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(chunk_hash_to_block_index, "%p"),
        LONGTAIL_LOGFIELD(awl, "%p"),
        LONGTAIL_LOGFIELD(retain_permssions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hash_to_block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, awl != 0, return EINVAL)

#if defined(LONGTAIL_ASSERTS)
    {
        uint32_t j = 0;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
            const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
            if (!block_index_ptr)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", EINVAL)
                return EINVAL;
            }
            uint32_t block_index = *block_index_ptr;
            TLongtail_Hash block_hash = store_index->m_BlockHashes[block_index];

            ++j;
            while (j < awl->m_BlockJobCount)
            {
                uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
                TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
                const uint32_t* next_block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
                if (!next_block_index_ptr)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", EINVAL)
                    return EINVAL;
                }
                uint32_t next_block_index = *next_block_index_ptr;
                if (next_block_index != block_index)
                {
                    break;
                }
                ++j;
            }
        }
    }
#endif // defined(LONGTAIL_ASSERTS)

    const uint32_t max_parallell_block_read_jobs = GetMaxParallelBlockReadJobs(job_api);

    uint32_t asset_job_count = 0;
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        uint32_t asset_index = awl->m_AssetIndexJobs[a];
        uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
        if (chunk_count == 0)
        {
            asset_job_count += 1;   // Write job
            continue;
        }

        uint32_t chunk_index_end = chunk_index_start + chunk_count;
        uint32_t chunk_index_offset = chunk_index_start;

        while(chunk_index_offset != chunk_index_end)
        {
            uint32_t block_read_job_count = 0;
            TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
            while (chunk_index_offset != chunk_index_end && block_read_job_count < max_parallell_block_read_jobs)
            {
                uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
                TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
                const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
                LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
                uint32_t block_index = *block_index_ptr;
                TLongtail_Hash block_hash = store_index->m_BlockHashes[block_index];
                int has_block = 0;
                for (uint32_t d = 0; d < block_read_job_count; ++d)
                {
                    if (block_hashes[d] == block_hash)
                    {
                        has_block = 1;
                        break;
                    }
                }
                if (!has_block)
                {
                    block_hashes[block_read_job_count++] = block_hash;
                }
                ++chunk_index_offset;
            }
            asset_job_count += 1;   // Write job
            asset_job_count += 1;   // Sync job
            asset_job_count += block_read_job_count;
        }
    }

    int err = block_store_api->PreflightGet(block_store_api, *store_index->m_BlockCount, store_index->m_BlockHashes, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "block_store_api->PreflightGet() failed with %d", err)
        return err;
    }

    if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Cancelled, failed with %d", ECANCELED)
        return ECANCELED;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)Longtail_Alloc("WriteAssets", (size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    if (!block_jobs)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint32_t job_count = asset_job_count;
    {
        uint32_t j = 0;
        uint32_t block_job_count = 0;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
            const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
            LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
            uint32_t block_index = *block_index_ptr;

            job_count++;

            ++j;
            while (j < awl->m_BlockJobCount)
            {
                uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[j];
                TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
                uint32_t* next_block_index = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, next_first_chunk_hash);
                LONGTAIL_FATAL_ASSERT(ctx, next_block_index != 0, return EINVAL)
                if (block_index != *next_block_index)
                {
                    break;
                }

                ++j;
            }

            job_count++;
        }
    }

    Longtail_JobAPI_Group job_group = 0;
    err = job_api->ReserveJobs(job_api, job_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReserveJobs() failed with %d", err)
        Longtail_Free(block_jobs);
        return err;
    }

    uint32_t j = 0;
    uint32_t block_job_count = 0;
    while (j < awl->m_BlockJobCount)
    {
        uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
        TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
        const uint32_t* block_index_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
        uint32_t block_index = *block_index_ptr;

        struct WriteAssetsFromBlockJob* job = &block_jobs[block_job_count++];
        struct BlockReaderJob* block_job = &job->m_BlockReadJob;
        block_job->m_BlockStoreAPI = block_store_api;
        block_job->m_AsyncCompleteAPI.m_API.Dispose = 0;
        block_job->m_AsyncCompleteAPI.OnComplete = 0;
        block_job->m_BlockHash = store_index->m_BlockHashes[block_index];
        block_job->m_JobAPI = job_api;
        block_job->m_JobID = 0;
        block_job->m_StoredBlock = 0;
        Longtail_JobAPI_JobFunc block_read_funcs[1] = { BlockReader };
        void* block_read_ctxs[1] = {block_job};
        Longtail_JobAPI_Jobs block_read_job;
        err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, 1, block_read_funcs, block_read_ctxs, 1, &block_read_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
            return err;
        }

        job->m_VersionStorageAPI = version_storage_api;
        job->m_VersionIndex = version_index;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = block_index;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[j];
        job->m_RetainPermissions = retain_permssions;

        job->m_AssetCount = 1;
        ++j;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            uint32_t* next_block_index = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, next_first_chunk_hash);
            LONGTAIL_FATAL_ASSERT(ctx, next_block_index != 0, return EINVAL)
            if (block_index != *next_block_index)
            {
                break;
            }

            ++job->m_AssetCount;
            ++j;
        }

        Longtail_JobAPI_JobFunc funcs[1] = { WriteAssetsFromBlock };
        void* ctxs[1] = { job };

        Longtail_JobAPI_Jobs block_write_job;
        err = job_api->CreateJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token, 1, funcs, ctxs, 0, &block_write_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
            return err;
        }
        err = job_api->AddDependecies(job_api, 1, block_write_job, 1, block_read_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
        err = job_api->ReadyJobs(job_api, 1, block_read_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    }
/*
block_readorCount = blocks_remaning > 8 ? 8 : blocks_remaning

Create block_reador Tasks [block_readorCount]
Create WriteSync Task
Create Write Task
    Depends on block_reador Tasks [block_readorCount]
    Depends on WriteSync Task

Ready block_reador Tasks [block_readorCount]
Ready WriteSync Task

WaitForAllTasks()

JOBS:

Write Task Execute (When block_reador Tasks [block_readorCount] and WriteSync Task is complete)
    Newblock_readorCount = blocks_remaning > 8 ? 8 : blocks_remaning
    if ([block_readorCount] > 0)
        Create block_reador Tasks for up to remaining blocks [Newblock_readorCount]
        Create WriteSync Task
        Create Write Task
            Depends on block_reador Tasks [Newblock_readorCount]
            Depends on WriteSync Task
        Ready block_reador Tasks [Newblock_readorCount]
    Write and Longtail_Free block_readed Tasks Data [block_readorCount] To Disk
    if ([block_readorCount] > 0)
        Ready WriteSync Task
*/

    size_t asset_jobs_size = sizeof(struct WritePartialAssetFromBlocksJob) * awl->m_AssetJobCount;
    struct WritePartialAssetFromBlocksJob* asset_jobs = (struct WritePartialAssetFromBlocksJob*)Longtail_Alloc("WriteAssets", asset_jobs_size);
    if (!asset_jobs)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(block_jobs);
        return ENOMEM;
    }
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        Longtail_JobAPI_Jobs write_sync_job;
        err = CreatePartialAssetWriteJob(
            block_store_api,
            version_storage_api,
            job_api,
            progress_api,
            optional_cancel_api,
            optional_cancel_token,
            store_index,
            version_index,
            version_path,
            chunk_hash_to_block_index,
            awl->m_AssetIndexJobs[a],
            retain_permssions,
            job_group,
            &asset_jobs[a],
            0,
            (Longtail_StorageAPI_HOpenFile)0,
            &write_sync_job);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreatePartialAssetWriteJob() failed with %d", err)
            Longtail_Free(asset_jobs);
            Longtail_Free(block_jobs);
            return err;
        }
        err = job_api->ReadyJobs(job_api, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    }

    err = job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->WaitForAllJobs() failed with %d", err)
        Longtail_Free(asset_jobs);
        Longtail_Free(block_jobs);
        return err;
    }

    Longtail_Free(asset_jobs);
    Longtail_Free(block_jobs);

    return 0;
}

int Longtail_WriteVersion(
    struct Longtail_BlockStoreAPI* block_storage_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    int retain_permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_storage_api, "%p"),
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    if (*version_index->m_AssetCount == 0)
    {
        return 0;
    }

    uint32_t block_count = *store_index->m_BlockCount;
    uint32_t chunk_count = *store_index->m_ChunkCount;
    struct Longtail_LookupTable* chunk_hash_to_block_index = LongtailPrivate_LookupTable_Create(Longtail_Alloc("WriteVersion", LongtailPrivate_LookupTable_GetSize(chunk_count)), chunk_count, 0);
    if (!chunk_hash_to_block_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    for (uint32_t b = 0; b < block_count; ++b)
    {
        uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
        uint32_t chunk_index_offset = store_index->m_BlockChunksOffsets[b];
        for (uint32_t c = 0; c < block_chunk_count; ++c)
    {
            uint32_t chunk_index = chunk_index_offset + c;
            TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
            LongtailPrivate_LookupTable_PutUnique(chunk_hash_to_block_index, chunk_hash, b);
        }
        chunk_index_offset += block_chunk_count;
    }

    uint32_t asset_count = *version_index->m_AssetCount;

    struct AssetWriteList* awl;
    int err = BuildAssetWriteList(
        asset_count,
        0,
        version_index->m_NameOffsets,
        version_index->m_NameData,
        version_index->m_ChunkHashes,
        version_index->m_AssetChunkCounts,
        version_index->m_AssetChunkIndexStarts,
        version_index->m_AssetChunkIndexes,
        chunk_hash_to_block_index,
        &awl);

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BuildAssetWriteList() failed with %d", err)
        Longtail_Free(chunk_hash_to_block_index);
        return err;
    }

    err = WriteAssets(
        block_storage_api,
        version_storage_api,
        job_api,
        progress_api,
        optional_cancel_api,
        optional_cancel_token,
        store_index,
        version_index,
        version_path,
        chunk_hash_to_block_index,
        awl,
        retain_permissions);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets() failed with %d", err)
    }

    Longtail_Free(awl);
    Longtail_Free(chunk_hash_to_block_index);

    return err;
}

static int CompareHash(const void* a_ptr, const void* b_ptr) 
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    TLongtail_Hash a = *((const TLongtail_Hash*)a_ptr);
    TLongtail_Hash b = *((const TLongtail_Hash*)b_ptr);
    if (a > b) return  1;
    if (a < b) return -1;
    return 0;
}

static uint32_t MakeUnique(TLongtail_Hash* hashes, uint32_t count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hashes, "%p"),
        LONGTAIL_LOGFIELD(count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, count == 0 || hashes != 0, return 0)

    uint32_t w = 0;
    uint32_t r = 0;
    while (r < count)
    {
        hashes[w] = hashes[r];
        ++r;
        while (r < count && hashes[r - 1] == hashes[r])
        {
            ++r;
        }
        ++w;
    }
    return w;
}

static int DiffHashes(
    const TLongtail_Hash* reference_hashes,
    uint32_t reference_hash_count,
    const TLongtail_Hash* new_hashes,
    uint32_t new_hash_count,
    uint32_t* added_hash_count,
    TLongtail_Hash* added_hashes,
    uint32_t* removed_hash_count,
    TLongtail_Hash* removed_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(reference_hashes, "%p"),
        LONGTAIL_LOGFIELD(reference_hash_count, "%u"),
        LONGTAIL_LOGFIELD(new_hashes, "%p"),
        LONGTAIL_LOGFIELD(new_hash_count, "%u"),
        LONGTAIL_LOGFIELD(added_hash_count, "%p"),
        LONGTAIL_LOGFIELD(added_hashes, "%p"),
        LONGTAIL_LOGFIELD(removed_hash_count, "%p"),
        LONGTAIL_LOGFIELD(removed_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, reference_hash_count == 0 || reference_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, new_hash_count == 0 || added_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, added_hash_count != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, added_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, (removed_hash_count == 0 && removed_hashes == 0) || (removed_hash_count != 0 && removed_hashes != 0), return EINVAL)

    size_t work_mem_size = (sizeof(TLongtail_Hash) * reference_hash_count) +
        (sizeof(TLongtail_Hash) * new_hash_count);
    void* work_mem = Longtail_Alloc("DiffHashes", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    TLongtail_Hash* tmp_refs = (TLongtail_Hash*)work_mem;
    TLongtail_Hash* tmp_news = (TLongtail_Hash*)&tmp_refs[reference_hash_count];

    memmove(tmp_refs, reference_hashes, (size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    memmove(tmp_news, new_hashes, (size_t)(sizeof(TLongtail_Hash) * new_hash_count));

    qsort(&tmp_refs[0], (size_t)reference_hash_count, sizeof(TLongtail_Hash), CompareHash);
    reference_hash_count = MakeUnique(&tmp_refs[0], reference_hash_count);

    qsort(&tmp_news[0], (size_t)new_hash_count, sizeof(TLongtail_Hash), CompareHash);
    new_hash_count = MakeUnique(&tmp_news[0], new_hash_count);

    uint32_t removed = 0;
    uint32_t added = 0;
    uint32_t ni = 0;
    uint32_t ri = 0;
    while (ri < reference_hash_count && ni < new_hash_count)
    {
        if (tmp_refs[ri] == tmp_news[ni])
        {
            ++ri;
            ++ni;
            continue;
        }
        else if (tmp_refs[ri] < tmp_news[ni])
        {
            if (removed_hashes)
            {
                removed_hashes[removed] = tmp_refs[ri];
            }
            ++removed;
            ++ri;
        }
        else if (tmp_refs[ri] > tmp_news[ni])
        {
            added_hashes[added++] = tmp_news[ni++];
        }
    }
    while (ni < new_hash_count)
    {
        added_hashes[added++] = tmp_news[ni++];
    }
    *added_hash_count = added;
    while (ri < reference_hash_count)
    {
        if (removed_hashes)
        {
            removed_hashes[removed] = tmp_refs[ri];
        }
        ++removed;
        ++ri;
    }
    if (removed_hash_count)
    {
        *removed_hash_count = removed;
    }

    Longtail_Free(work_mem);
    work_mem = 0;

    if (added > 0)
    {
        // Reorder the new hashes so they are in the same order that they where when they were created
        // so chunks that belongs together are group together in blocks
        struct Longtail_LookupTable* added_hashes_lookup = LongtailPrivate_LookupTable_Create(Longtail_Alloc("DiffHashes", LongtailPrivate_LookupTable_GetSize(added)), added, 0);
        if (!added_hashes_lookup)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }
        for (uint32_t i = 0; i < added; ++i)
        {
            LongtailPrivate_LookupTable_Put(added_hashes_lookup, added_hashes[i], i);
        }
        added = 0;
        for (uint32_t i = 0; i < new_hash_count; ++i)
        {
            TLongtail_Hash hash = new_hashes[i];
            if (LongtailPrivate_LookupTable_Get(added_hashes_lookup, hash) == 0)
            {
                continue;
            }
            added_hashes[added++] = hash;
        }
        Longtail_Free(added_hashes_lookup);
    }
    return 0;
}

int Longtail_CreateStoreIndex(
    struct Longtail_HashAPI* hash_api,
    uint32_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* optional_chunk_tags,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(optional_chunk_tags, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    if (chunk_count == 0)
    {
        int err = Longtail_CreateStoreIndexFromBlocks(0, 0, out_store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
            return err;
        }
        return 0;
    }

    size_t work_mem_size = (sizeof(uint32_t) * chunk_count) +
        (sizeof(struct Longtail_BlockIndex*) * chunk_count) +
        (sizeof(uint32_t) * max_chunks_per_block);
    void* work_mem = Longtail_Alloc("Longtail_CreateStoreIndex", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    uint32_t* tmp_chunk_indexes = (uint32_t*)work_mem;
    struct Longtail_BlockIndex** tmp_block_indexes = (struct Longtail_BlockIndex**)&tmp_chunk_indexes[chunk_count];
    uint32_t* tmp_stored_chunk_indexes = (uint32_t*)&tmp_block_indexes[chunk_count];
    uint32_t unique_chunk_count = GetUniqueHashes((uint32_t)chunk_count, chunk_hashes, tmp_chunk_indexes);

    uint32_t i = 0;
    uint32_t block_count = 0;

    while (i < unique_chunk_count)
    {
        uint32_t chunk_count_in_block = 0;

        uint32_t chunk_index = tmp_chunk_indexes[i];

        uint32_t current_size = chunk_sizes[chunk_index];
        uint32_t current_tag = optional_chunk_tags ? optional_chunk_tags[chunk_index] : 0;

        tmp_stored_chunk_indexes[chunk_count_in_block] = chunk_index;
        ++chunk_count_in_block;

        while((i + 1) < unique_chunk_count)
        {
            chunk_index = tmp_chunk_indexes[(i + 1)];
            uint32_t chunk_size = chunk_sizes[chunk_index];
            uint32_t tag = optional_chunk_tags ? optional_chunk_tags[chunk_index] : 0;

            if (tag != current_tag)
            {
                break;
            }

            // Break if resulting chunk count will exceed max_chunks_per_block
            if (chunk_count_in_block == max_chunks_per_block)
            {
                break;
            }

            // Overshoot by 10% is ok
            if ((current_size + chunk_size) > (max_block_size + (max_block_size / 10)))
            {
                break;
            }

            current_size += chunk_size;
            tmp_stored_chunk_indexes[chunk_count_in_block] = chunk_index;
            ++chunk_count_in_block;

            ++i;
        }

        int err = Longtail_CreateBlockIndex(
            hash_api,
            current_tag,
            chunk_count_in_block,
            tmp_stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes,
            &tmp_block_indexes[block_count]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateBlockIndex() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }

        ++block_count;
        ++i;
    }

    int err = Longtail_CreateStoreIndexFromBlocks(
        block_count,
        (const struct Longtail_BlockIndex**)tmp_block_indexes,
        out_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
        return err;
    }

    for (uint32_t b = 0; b < block_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = tmp_block_indexes[b];
        Longtail_Free(block_index);
        block_index = 0;
    }
    Longtail_Free(work_mem);
    return err;
}

int Longtail_CreateMissingContent(
    struct Longtail_HashAPI* hash_api,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_chunks_per_block != 0, return EINVAL)

    uint32_t chunk_count = *version_index->m_ChunkCount;
    size_t added_hashes_size = sizeof(TLongtail_Hash) * chunk_count;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)Longtail_Alloc("CreateMissingContent", added_hashes_size);
    if (!added_hashes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint32_t added_hash_count = 0;
    int err = DiffHashes(
        store_index->m_ChunkHashes,
        *store_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        chunk_count,
        &added_hash_count,
        added_hashes,
        0,
        0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "DiffHashes() failed with %d", err)
        Longtail_Free(added_hashes);
        return err;
    }

    if (added_hash_count == 0)
    {
        Longtail_Free(added_hashes);
        err = Longtail_CreateStoreIndexFromBlocks(
            0,
            0,
            out_store_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
        }
        return err;
    }

    size_t chunk_index_lookup_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
    size_t tmp_diff_chunk_sizes_size = sizeof(uint32_t) * added_hash_count;
    size_t tmp_diff_chunk_tags_size = sizeof(uint32_t) * added_hash_count;
    size_t work_mem_size =
        chunk_index_lookup_size +
        tmp_diff_chunk_sizes_size +
        tmp_diff_chunk_tags_size;
    void* work_mem = Longtail_Alloc("CreateMissingContent", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(added_hashes);
        return ENOMEM;
    }
    char* p = (char*)work_mem;
    struct Longtail_LookupTable* chunk_index_lookup = LongtailPrivate_LookupTable_Create(p, chunk_count, 0);
    p += chunk_index_lookup_size;
    uint32_t* tmp_diff_chunk_sizes = (uint32_t*)p;
    p += tmp_diff_chunk_sizes_size;
    uint32_t* tmp_diff_chunk_tags = (uint32_t*)p;

    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        LongtailPrivate_LookupTable_Put(chunk_index_lookup, version_index->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        const uint32_t* chunk_index_ptr = LongtailPrivate_LookupTable_Get(chunk_index_lookup, added_hashes[j]);
        LONGTAIL_FATAL_ASSERT(ctx, chunk_index_ptr, return EINVAL)
        uint32_t chunk_index = *chunk_index_ptr;
        tmp_diff_chunk_sizes[j] = version_index->m_ChunkSizes[chunk_index];
        tmp_diff_chunk_tags[j] = version_index->m_ChunkTags[chunk_index];
    }

    err = Longtail_CreateStoreIndex(
        hash_api,
        added_hash_count,
        added_hashes,
        tmp_diff_chunk_sizes,
        tmp_diff_chunk_tags,
        max_block_size,
        max_chunks_per_block,
        out_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexRaw() failed with %d", err)
    }

    Longtail_Free(work_mem);
    Longtail_Free(added_hashes);

    return err;
}

int Longtail_GetMissingChunks(
    const struct Longtail_StoreIndex* store_index,
    uint32_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint32_t* out_chunk_count,
    TLongtail_Hash* out_missing_chunk_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_count, "%p"),
        LONGTAIL_LOGFIELD(out_missing_chunk_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (out_missing_chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count <= 0xffffffffu, return EINVAL)

    uint32_t reference_chunk_count = *store_index->m_ChunkCount;
    void* chunk_to_reference_block_index_lookup_mem = Longtail_Alloc("GetMissingChunks", LongtailPrivate_LookupTable_GetSize(reference_chunk_count));
    if (!chunk_to_reference_block_index_lookup_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_LookupTable* chunk_to_reference_block_index_lookup = LongtailPrivate_LookupTable_Create(chunk_to_reference_block_index_lookup_mem, reference_chunk_count, 0);
    LONGTAIL_FATAL_ASSERT(ctx, chunk_to_reference_block_index_lookup != 0, return EINVAL )

    uint32_t reference_block_count = *store_index->m_BlockCount;
    for (uint32_t b = 0; b < reference_block_count; ++b)
    {
        uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
        uint32_t chunk_index_offset = store_index->m_BlockChunksOffsets[b];
        for (uint32_t c = 0; c < block_chunk_count; ++c)
    {
            uint32_t chunk_index = chunk_index_offset + c;
            TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
            LongtailPrivate_LookupTable_PutUnique(chunk_to_reference_block_index_lookup, chunk_hash, b);
        }
    }

    uint32_t missing_chunk_count = 0;
    for (uint32_t c = 0; c < (uint32_t)chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = chunk_hashes[c];
        if (LongtailPrivate_LookupTable_Get(chunk_to_reference_block_index_lookup, chunk_hash))
        {
            continue;
        }
        out_missing_chunk_hashes[missing_chunk_count++] = chunk_hash;
    }
    Longtail_Free(chunk_to_reference_block_index_lookup);
    *out_chunk_count = missing_chunk_count;
    return 0;
}

static SORTFUNC(SortBlockUsageHighToLow)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    const uint32_t* block_usages = (const uint32_t*)context;
    const uint32_t a_index = *(const uint32_t*)a_ptr;
    const uint32_t b_index = *(const uint32_t*)b_ptr;
    const uint32_t a_usage = block_usages[a_index];
    const uint32_t b_usage = block_usages[b_index];
    return (a_usage < b_usage) ?
        1 : ((a_usage > b_usage) ?
            -1 : ((a_index < b_index) ?
                -1 : ((a_index == b_index) ?
                    0 : 1)));
}

int Longtail_GetExistingStoreIndex(
    const struct Longtail_StoreIndex* store_index,
    uint32_t chunk_count,
    const TLongtail_Hash* chunks,
    uint32_t min_block_usage_percent,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunks, "%p"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunks != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t hash_identifier = *store_index->m_HashIdentifier;
    uint32_t store_block_count = *store_index->m_BlockCount;
    uint32_t store_chunk_count = *store_index->m_ChunkCount;

    size_t chunk_to_index_lookup_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
    size_t block_to_index_lookup_size = LongtailPrivate_LookupTable_GetSize(store_block_count);
    size_t chunk_to_store_index_lookup_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
    size_t found_store_block_hashes_size = sizeof(TLongtail_Hash) * store_block_count;
    size_t block_uses_percent_size = sizeof(uint32_t) * store_block_count;
    size_t block_index_size = sizeof(uint32_t) * store_block_count;
    size_t block_order_size = sizeof(uint32_t) * store_block_count;

    size_t tmp_mem_size = chunk_to_index_lookup_size +
        block_to_index_lookup_size +
        chunk_to_store_index_lookup_size +
        found_store_block_hashes_size +
        block_uses_percent_size +
        block_index_size +
        block_order_size;

    void* tmp_mem = Longtail_Alloc("Longtail_GetExistingStoreIndex", tmp_mem_size);
    if (!tmp_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)tmp_mem;

    struct Longtail_LookupTable* chunk_to_index_lookup = LongtailPrivate_LookupTable_Create(p, chunk_count, 0);
    p += chunk_to_index_lookup_size;

    struct Longtail_LookupTable* block_to_index_lookup = LongtailPrivate_LookupTable_Create(p, store_block_count, 0);
    p += block_to_index_lookup_size;

    struct Longtail_LookupTable* chunk_to_store_index_lookup = LongtailPrivate_LookupTable_Create(p, chunk_count, 0);
    p += chunk_to_store_index_lookup_size;

    TLongtail_Hash* found_store_block_hashes = (TLongtail_Hash*)p;
    p += found_store_block_hashes_size;

    uint32_t* block_uses_percent = (uint32_t*)p;
    p += block_uses_percent_size;

    uint32_t* block_index = (uint32_t*)p;
    p += block_index_size;

    uint32_t* block_order = (uint32_t*)p;
    p += block_order_size;

    uint32_t unique_chunk_count = 0;
    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = chunks[i];
        uint32_t* c_ptr = LongtailPrivate_LookupTable_PutUnique(chunk_to_index_lookup, chunk_hash, i);
        if (c_ptr)
        {
            continue;
        }
        ++unique_chunk_count;
    }

    uint32_t found_block_count = 0;
    uint32_t found_chunk_count = 0;
    if (min_block_usage_percent <= 100)
    {
        uint32_t potential_block_count = 0;
        for (uint32_t b = 0; b < store_block_count; ++b)
        {
            uint32_t block_use = 0;
            uint32_t block_size = 0;
            TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
            uint32_t chunk_offset = store_index->m_BlockChunksOffsets[b];
            for (uint32_t c = 0; c < block_chunk_count; ++c)
            {
                uint32_t chunk_size = store_index->m_ChunkSizes[chunk_offset];
                TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_offset];
                ++chunk_offset;
                block_size += chunk_size;
                if (LongtailPrivate_LookupTable_Get(chunk_to_index_lookup, chunk_hash))
                {
                    block_use += chunk_size;
                }
            }
            if (block_use > 0)
            {
                uint32_t block_usage_percent = (uint32_t)(((uint64_t)block_use * 100) / block_size);
                if (min_block_usage_percent > 0 &&
                    block_usage_percent < min_block_usage_percent) {
                    continue;
                }

                block_order[potential_block_count] = potential_block_count;
                block_index[potential_block_count] = b;
                block_uses_percent[potential_block_count] = block_usage_percent;

                ++potential_block_count;
            }
        }

        if (potential_block_count == 0)
        {
            Longtail_Free(tmp_mem);
            return Longtail_CreateStoreIndexFromBlocks(
                0,
                0,
                out_store_index);
        }

        // Favour blocks we use more data out of - if a chunk is in multiple blocks we want to pick
        // the blocks that has highest ratio of use inside the block.
        // This does not guarantee a perfect block match as one block can be a 100% match which
        // could lead to skipping part or whole of another 100% match block resulting in us
        // picking a block that we will not use 100% of
        QSORT(block_order, potential_block_count, sizeof(uint32_t), SortBlockUsageHighToLow, (void*)block_uses_percent);

        for (uint32_t bo = 0; (bo < potential_block_count) && (found_chunk_count < unique_chunk_count); ++bo)
        {
            uint32_t pb = block_order[bo];
            uint32_t b = block_index[pb];

            TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
            uint32_t store_chunk_index_offset = store_index->m_BlockChunksOffsets[b];
            uint32_t current_found_block_index = found_block_count;
            for (uint32_t c = 0; c < block_chunk_count; ++c)
            {
                TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[store_chunk_index_offset];
                if (!LongtailPrivate_LookupTable_Get(chunk_to_index_lookup, chunk_hash))
                {
                    ++store_chunk_index_offset;
                    continue;
                }
                if (LongtailPrivate_LookupTable_PutUnique(chunk_to_store_index_lookup, chunk_hash, store_chunk_index_offset))
                {
                    ++store_chunk_index_offset;
                    continue;
                }
                found_chunk_count++;
                if (current_found_block_index == found_block_count)
                {
                    uint32_t* block_index_ptr = LongtailPrivate_LookupTable_PutUnique(block_to_index_lookup, block_hash, current_found_block_index);
                    if (block_index_ptr == 0)
                    {
                        found_store_block_hashes[found_block_count++] = block_hash;
                    }
                    ++store_chunk_index_offset;
                    continue;
                }
                ++store_chunk_index_offset;
            }
        }
    }

    if (found_block_count == 0)
    {
        Longtail_Free(tmp_mem);
        return Longtail_CreateStoreIndexFromBlocks(
            0,
            0,
            out_store_index);
    }

    // We have a list of block hashes we want to keep in found_store_block_hashes
    // We have a lookup from block hash to block index in store index in block_to_index_lookup

    size_t block_hash_lookup_size = LongtailPrivate_LookupTable_GetSize(found_block_count);
    size_t block_index_header_ptrs_size = sizeof(struct LongtailBlockIndex*) * found_block_count;
    size_t block_index_headers_size = sizeof(struct Longtail_BlockIndex) * found_block_count;
    size_t tmp_mem_2_size = block_index_header_ptrs_size +
        block_index_headers_size +
        block_hash_lookup_size;
    void* tmp_mem_2 = Longtail_Alloc("Longtail_GetExistingStoreIndex", tmp_mem_2_size);
    if (!tmp_mem_2){
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(tmp_mem);
        return ENOMEM;

    }
    struct Longtail_BlockIndex** block_index_header_ptrs = (struct Longtail_BlockIndex**)tmp_mem_2;
    struct Longtail_BlockIndex* block_index_headers = (struct Longtail_BlockIndex*)&block_index_header_ptrs[found_block_count];
    struct Longtail_LookupTable* block_hash_lookup = LongtailPrivate_LookupTable_Create(&((char*)block_index_headers)[block_index_headers_size], found_block_count, 0);
    for (uint32_t b = 0; b < store_block_count; ++b)
    {
        TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
        if (0 == LongtailPrivate_LookupTable_Get(block_to_index_lookup, block_hash))
        {
            continue;
        }
        LongtailPrivate_LookupTable_Put(block_hash_lookup, block_hash, b);
    }

    for (uint32_t b = 0; b < found_block_count; ++b)
    {
        TLongtail_Hash block_hash = found_store_block_hashes[b];
        uint32_t store_block_index = *LongtailPrivate_LookupTable_Get(block_hash_lookup, block_hash);
        uint32_t block_chunk_count = store_index->m_BlockChunkCounts[store_block_index];
        uint32_t block_chunk_index_offset = store_index->m_BlockChunksOffsets[store_block_index];
        block_index_headers[b].m_BlockHash = &store_index->m_BlockHashes[store_block_index];
        block_index_headers[b].m_HashIdentifier = store_index->m_HashIdentifier;
        block_index_headers[b].m_ChunkCount = &store_index->m_BlockChunkCounts[store_block_index];
        block_index_headers[b].m_Tag = &store_index->m_BlockTags[block_chunk_index_offset];
        block_index_headers[b].m_ChunkHashes = &store_index->m_ChunkHashes[block_chunk_index_offset];
        block_index_headers[b].m_ChunkSizes = &store_index->m_ChunkSizes[block_chunk_index_offset];
        block_index_header_ptrs[b] = &block_index_headers[b];
    }
    Longtail_Free(tmp_mem);

    int err = Longtail_CreateStoreIndexFromBlocks(
        found_block_count,
        (const struct Longtail_BlockIndex**)block_index_header_ptrs,
        out_store_index);
    Longtail_Free(tmp_mem_2);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
        return err;
    }
    return 0;
}

static int CompareHashes(const void* a_ptr, const void* b_ptr)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    TLongtail_Hash a = *(const TLongtail_Hash*)a_ptr;
    TLongtail_Hash b = *(const TLongtail_Hash*)b_ptr;
    return (a > b) ? 1 : (a < b) ? -1 : 0;
}

static SORTFUNC(SortPathShortToLong)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

        const uint32_t* asset_path_lengths = (const uint32_t*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    uint32_t a_len = asset_path_lengths[a];
    uint32_t b_len = asset_path_lengths[b];
    if (a_len < b_len)
    {
        return -1;
    }
    if (a_len > b_len)
    {
        return 1;
    }
    if (a < b)
    {
        return -1;
    }
    if (a > b)
    {
        return 1;
    }
    return 0;
}

static SORTFUNC(SortPathLongToShort)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)
    const uint32_t* asset_path_lengths = (const uint32_t*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    uint32_t a_len = asset_path_lengths[a];
    uint32_t b_len = asset_path_lengths[b];
    if (a_len < b_len)
    {
        return 1;
    }
    if (a_len > b_len)
    {
        return -1;
    }
    if (a < b)
    {
        return 1;
    }
    if (a > b)
    {
        return -1;
    }
    return 0;
}

static size_t GetVersionDiffDataSize(uint32_t removed_count, uint32_t added_count, uint32_t modified_content_count, uint32_t modified_permission_count)
{
    return
        sizeof(uint32_t) +                              // m_SourceRemovedCount
        sizeof(uint32_t) +                              // m_TargetAddedCount
        sizeof(uint32_t) +                              // m_ModifiedContentCount
        sizeof(uint32_t) +                              // m_ModifiedPermissionsCount
        sizeof(uint32_t) * removed_count +              // m_SourceRemovedAssetIndexes
        sizeof(uint32_t) * added_count +                // m_TargetAddedAssetIndexes
        sizeof(uint32_t) * modified_content_count +     // m_SourceContentModifiedAssetIndexes
        sizeof(uint32_t) * modified_content_count +     // m_TargetContentModifiedAssetIndexes
        sizeof(uint32_t) * modified_permission_count +  // m_SourcePermissionsModifiedAssetIndexes
        sizeof(uint32_t) * modified_permission_count;   // m_TargetPermissionsModifiedAssetIndexes
}

static size_t GetVersionDiffSize(uint32_t removed_count, uint32_t added_count, uint32_t modified_content_count, uint32_t modified_permission_count)
{
    return sizeof(struct Longtail_VersionDiff) +
        GetVersionDiffDataSize(removed_count, added_count, modified_content_count, modified_permission_count);
}

static void InitVersionDiff(struct Longtail_VersionDiff* version_diff)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_diff, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, version_diff != 0, return)

    char* p = (char*)version_diff;
    p += sizeof(struct Longtail_VersionDiff);

    version_diff->m_SourceRemovedCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    version_diff->m_TargetAddedCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    version_diff->m_ModifiedContentCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    version_diff->m_ModifiedPermissionsCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t removed_count = *version_diff->m_SourceRemovedCount;
    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t modified_permissions_count = *version_diff->m_ModifiedPermissionsCount;

    version_diff->m_SourceRemovedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * removed_count;

    version_diff->m_TargetAddedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * added_count;

    version_diff->m_SourceContentModifiedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * modified_content_count;

    version_diff->m_TargetContentModifiedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * modified_content_count;

    version_diff->m_SourcePermissionsModifiedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * modified_permissions_count;

    version_diff->m_TargetPermissionsModifiedAssetIndexes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * modified_permissions_count;
}

int Longtail_CreateVersionDiff(
    struct Longtail_HashAPI* hash_api,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    struct Longtail_VersionDiff** out_version_diff)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(source_version, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(out_version_diff, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api->GetIdentifier(hash_api) == *source_version->m_HashIdentifier, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api->GetIdentifier(hash_api) == *target_version->m_HashIdentifier, return EINVAL)

    uint32_t source_asset_count = *source_version->m_AssetCount;
    uint32_t target_asset_count = *target_version->m_AssetCount;

    size_t source_asset_lookup_table_size = LongtailPrivate_LookupTable_GetSize(source_asset_count);
    size_t target_asset_lookup_table_size = LongtailPrivate_LookupTable_GetSize(target_asset_count);

    size_t work_mem_size =
        source_asset_lookup_table_size +
        target_asset_lookup_table_size +
        sizeof(TLongtail_Hash) * source_asset_count +
        sizeof(TLongtail_Hash) * target_asset_count +
        sizeof(uint32_t) * source_asset_count +
        sizeof(uint32_t) * target_asset_count +
        sizeof(uint32_t) * source_asset_count +
        sizeof(uint32_t) * target_asset_count +
        sizeof(uint32_t) * source_asset_count +
        sizeof(uint32_t) * target_asset_count +
        sizeof(uint32_t) * source_asset_count +
        sizeof(uint32_t) * target_asset_count;
    void* work_mem = Longtail_Alloc("CreateVersionDiff", work_mem_size);
    uint8_t* p = (uint8_t*)work_mem;

    struct Longtail_LookupTable* source_path_hash_to_index = LongtailPrivate_LookupTable_Create(p, source_asset_count ,0);
    p += source_asset_lookup_table_size;
    struct Longtail_LookupTable* target_path_hash_to_index = LongtailPrivate_LookupTable_Create(p, target_asset_count ,0);
    p += target_asset_lookup_table_size;

    TLongtail_Hash* source_path_hashes = (TLongtail_Hash*)p;
    TLongtail_Hash* target_path_hashes = &source_path_hashes[source_asset_count];

    uint32_t* removed_source_asset_indexes = (uint32_t*)&target_path_hashes[target_asset_count];
    uint32_t* added_target_asset_indexes = &removed_source_asset_indexes[source_asset_count];

    uint32_t* modified_source_content_indexes = &added_target_asset_indexes[target_asset_count];
    uint32_t* modified_target_content_indexes = &modified_source_content_indexes[source_asset_count];

    uint32_t* modified_source_permissions_indexes = &modified_target_content_indexes[target_asset_count];
    uint32_t* modified_target_permissions_indexes = &modified_source_permissions_indexes[source_asset_count];

    uint32_t* source_assets_path_lengths = &modified_target_permissions_indexes[target_asset_count];
    uint32_t* target_assets_path_lengths = &source_assets_path_lengths[source_asset_count];

    if (*source_version->m_Version < LONGTAIL_VERSION_INDEX_VERSION_0_0_2)
    {
        // We are re-hashing since we might have an older version hash that is incompatible
        for (uint32_t i = 0; i < source_asset_count; ++i)
        {
            const char* path = &source_version->m_NameData[source_version->m_NameOffsets[i]];
            source_assets_path_lengths[i] = (uint32_t)strlen(path);
            int err = GetPathHashWithLength(hash_api, path, source_assets_path_lengths[i], &source_path_hashes[i]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_GetPathHash() failed with %d", err)
                Longtail_Free(work_mem);
                return err;
            }
            LongtailPrivate_LookupTable_Put(source_path_hash_to_index, source_path_hashes[i], i);
        }
    }
    else
    {
        memcpy(source_path_hashes, source_version->m_PathHashes, sizeof(TLongtail_Hash) * source_asset_count);
        for (uint32_t i = 0; i < source_asset_count; ++i)
        {
            const char* path = &source_version->m_NameData[source_version->m_NameOffsets[i]];
            source_assets_path_lengths[i] = (uint32_t)strlen(path);
            LongtailPrivate_LookupTable_Put(source_path_hash_to_index, source_version->m_PathHashes[i], i);
        }
    }

    if (*target_version->m_Version < LONGTAIL_VERSION_INDEX_VERSION_0_0_2)
    {
        // We are re-hashing since we might have an older version hash that is incompatible
        for (uint32_t i = 0; i < target_asset_count; ++i)
        {
            const char* path = &target_version->m_NameData[target_version->m_NameOffsets[i]];
            target_assets_path_lengths[i] = (uint32_t)strlen(path);
            int err = GetPathHashWithLength(hash_api, path, target_assets_path_lengths[i], &target_path_hashes[i]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_GetPathHash() failed with %d", err)
                Longtail_Free(work_mem);
                return err;
            }
            LongtailPrivate_LookupTable_Put(target_path_hash_to_index, target_path_hashes[i], i);
        }
    }
    else
    {
        memcpy(target_path_hashes, target_version->m_PathHashes, sizeof(TLongtail_Hash) * target_asset_count);
        for (uint32_t i = 0; i < target_asset_count; ++i)
        {
            const char* path = &target_version->m_NameData[target_version->m_NameOffsets[i]];
            target_assets_path_lengths[i] = (uint32_t)strlen(path);
            LongtailPrivate_LookupTable_Put(target_path_hash_to_index, target_version->m_PathHashes[i], i);
        }
    }

    qsort(source_path_hashes, source_asset_count, sizeof(TLongtail_Hash), CompareHashes);
    qsort(target_path_hashes, target_asset_count, sizeof(TLongtail_Hash), CompareHashes);

    const uint32_t max_modified_content_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    const uint32_t max_modified_permission_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    const uint32_t indexes_count = source_asset_count + target_asset_count + max_modified_content_count + max_modified_content_count + max_modified_permission_count + max_modified_permission_count;

    uint32_t source_removed_count = 0;
    uint32_t target_added_count = 0;
    uint32_t modified_content_count = 0;
    uint32_t modified_permissions_count = 0;

    uint32_t source_index = 0;
    uint32_t target_index = 0;
    while (source_index < source_asset_count && target_index < target_asset_count)
    {
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        const uint32_t* source_asset_index_ptr = LongtailPrivate_LookupTable_Get(source_path_hash_to_index, source_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
        uint32_t source_asset_index = *source_asset_index_ptr;
        const uint32_t* target_asset_index_ptr = LongtailPrivate_LookupTable_Get(target_path_hash_to_index, target_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
        uint32_t target_asset_index = *target_asset_index_ptr;

        const char* source_path = &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]];
        const char* target_path = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];

        if (source_path_hash == target_path_hash)
        {
            TLongtail_Hash source_content_hash = source_version->m_ContentHashes[source_asset_index];
            TLongtail_Hash target_content_hash = target_version->m_ContentHashes[target_asset_index];
            if (source_content_hash != target_content_hash)
            {
                modified_source_content_indexes[modified_content_count] = source_asset_index;
                modified_target_content_indexes[modified_content_count] = target_asset_index;
                ++modified_content_count;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching content for asset %s", source_path)
            }

            uint16_t source_permissions = source_version->m_Permissions[source_asset_index];
            uint16_t target_permissions = target_version->m_Permissions[target_asset_index];
            if (source_permissions != target_permissions)
            {
                modified_source_permissions_indexes[modified_permissions_count] = source_asset_index;
                modified_target_permissions_indexes[modified_permissions_count] = target_asset_index;
                ++modified_permissions_count;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching permissions for asset %s", source_path)
            }

            ++source_index;
            ++target_index;
        }
        else if (source_path_hash < target_path_hash)
        {
            const uint32_t* source_asset_index_ptr = LongtailPrivate_LookupTable_Get(source_path_hash_to_index, source_path_hash);
            LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
            source_asset_index = *source_asset_index_ptr;
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Removed asset %s", source_path)
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        }
        else
        {
            const uint32_t* target_asset_index_ptr = LongtailPrivate_LookupTable_Get(target_path_hash_to_index, target_path_hash);
            LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
            target_asset_index = *target_asset_index_ptr;
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Added asset %s", target_path)
            added_target_asset_indexes[target_added_count] = target_asset_index;
            ++target_added_count;
            ++target_index;
        } 
    }
    while (source_index < source_asset_count)
    {
        // source_path_hash removed
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        const uint32_t* source_asset_index_ptr = LongtailPrivate_LookupTable_Get(source_path_hash_to_index, source_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
        uint32_t source_asset_index = *source_asset_index_ptr;
        const char* source_path = &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]];
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Removed asset %s", source_path)
        removed_source_asset_indexes[source_removed_count] = source_asset_index;
        ++source_removed_count;
        ++source_index;
    }
    while (target_index < target_asset_count)
    {
        // target_path_hash added
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        const uint32_t* target_asset_index_ptr = LongtailPrivate_LookupTable_Get(target_path_hash_to_index, target_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
        uint32_t target_asset_index = *target_asset_index_ptr;
        const char* target_path = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Added asset %s", target_path)
        added_target_asset_indexes[target_added_count] = target_asset_index;
        ++target_added_count;
        ++target_index;
    }
    if (source_removed_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Found %u removed assets", source_removed_count)
    }
    if (target_added_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Found %u added assets", target_added_count)
    }
    if (modified_content_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching content for %u assets found", modified_content_count)
    }
    if (modified_permissions_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching permission for %u assets found", modified_permissions_count)
    }

    size_t version_diff_size = GetVersionDiffSize(source_removed_count, target_added_count, modified_content_count, modified_permissions_count);
    struct Longtail_VersionDiff* version_diff = (struct Longtail_VersionDiff*)Longtail_Alloc("CreateVersionDiff", version_diff_size);
    if (!version_diff)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(work_mem);
        return ENOMEM;
    }
    uint32_t* counts_ptr = (uint32_t*)(void*)&version_diff[1];
    counts_ptr[0] = source_removed_count;
    counts_ptr[1] = target_added_count;
    counts_ptr[2] = modified_content_count;
    counts_ptr[3] = modified_permissions_count;
    InitVersionDiff(version_diff);

    memmove(version_diff->m_SourceRemovedAssetIndexes, removed_source_asset_indexes, sizeof(uint32_t) * source_removed_count);
    memmove(version_diff->m_TargetAddedAssetIndexes, added_target_asset_indexes, sizeof(uint32_t) * target_added_count);
    memmove(version_diff->m_SourceContentModifiedAssetIndexes, modified_source_content_indexes, sizeof(uint32_t) * modified_content_count);
    memmove(version_diff->m_TargetContentModifiedAssetIndexes, modified_target_content_indexes, sizeof(uint32_t) * modified_content_count);
    memmove(version_diff->m_SourcePermissionsModifiedAssetIndexes, modified_source_permissions_indexes, sizeof(uint32_t) * modified_permissions_count);
    memmove(version_diff->m_TargetPermissionsModifiedAssetIndexes, modified_target_permissions_indexes, sizeof(uint32_t) * modified_permissions_count);

    QSORT(version_diff->m_SourceRemovedAssetIndexes, source_removed_count, sizeof(uint32_t), SortPathLongToShort, (void*)source_assets_path_lengths);
    QSORT(version_diff->m_TargetAddedAssetIndexes, target_added_count, sizeof(uint32_t), SortPathShortToLong, (void*)target_assets_path_lengths);

    Longtail_Free(work_mem);
    *out_version_diff = version_diff;
    return 0;
}

static int CleanUpRemoveAssets(
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(source_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

        LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
        LONGTAIL_VALIDATE_INPUT(ctx, source_version != 0, return EINVAL)
        LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
        LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    uint32_t remove_count = *version_diff->m_SourceRemovedCount;
    LONGTAIL_FATAL_ASSERT(ctx, remove_count <= *source_version->m_AssetCount, return EINVAL);
    if (remove_count == 0)
    {
        return 0;
    }

    int err = 0;
    char* full_asset_path = 0;
    uint32_t* remove_indexes = (uint32_t*)Longtail_Alloc("ChangeVersion", sizeof(uint32_t) * remove_count);
    if (!remove_indexes)
    {
        err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        return err;
    }
    memcpy(remove_indexes, version_diff->m_SourceRemovedAssetIndexes, sizeof(uint32_t) * remove_count);

    uint32_t retry_count = 10;
    uint32_t successful_remove_count = 0;
    while (retry_count && (successful_remove_count < remove_count))
    {
        if (retry_count < 10)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ChangeVersion: Retrying removal of remaning %u assets in %s", remove_count - successful_remove_count, version_path)
        }
        --retry_count;
        if ((successful_remove_count & 0x7f) == 0x7f) {
            if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
            {
                err = ECANCELED;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Operation cancelled, failed with %d", err)
                Longtail_Free(remove_indexes);
                return err;
            }
        }
        for (uint32_t r = 0; r < remove_count; ++r)
        {
            uint32_t asset_index = remove_indexes[r];
            if (asset_index == 0xffffffff)
            {
                continue;
            }
            const char* asset_path = &source_version->m_NameData[source_version->m_NameOffsets[asset_index]];
            char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            if (IsDirPath(asset_path))
            {
                full_asset_path[strlen(full_asset_path) - 1] = '\0';
                if (!version_storage_api->IsDir(version_storage_api, full_asset_path))
                {
                    remove_indexes[r] = 0xffffffff;
                    Longtail_Free(full_asset_path);
                    Longtail_Free(remove_indexes);
                    return err;
                }
                uint16_t permissions = 0;
                err = version_storage_api->GetPermissions(version_storage_api, full_asset_path, &permissions);
                if (err)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->GetPermissions() failed with %d", err)
                    LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                    Longtail_Free(full_asset_path);
                    Longtail_Free(remove_indexes);
                    return err;
                }
                if (!(permissions & Longtail_StorageAPI_UserWriteAccess))
                {
                    err = version_storage_api->SetPermissions(version_storage_api, full_asset_path, permissions | (Longtail_StorageAPI_UserWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_OtherWriteAccess));
                    if (err)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                        LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                        Longtail_Free(full_asset_path);
                        Longtail_Free(remove_indexes);
                        return err;
                    }
                }
                err = version_storage_api->RemoveDir(version_storage_api, full_asset_path);
                if (err && version_storage_api->IsDir(version_storage_api, full_asset_path))
                {
                    if (!retry_count)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Can't to remove dir `%s`, failed with %d", full_asset_path, err)
                        Longtail_Free(full_asset_path);
                        Longtail_Free(remove_indexes);
                        LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                        return err;
                    }
                    Longtail_Free(full_asset_path);
                    full_asset_path = 0;
                    continue;
                }
                LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, 0)
            }
            else
            {
                if (!version_storage_api->IsFile(version_storage_api, full_asset_path))
                {
                    remove_indexes[r] = 0xffffffff;
                    Longtail_Free(full_asset_path);
                    continue;
                }
                uint16_t permissions = 0;
                err = version_storage_api->GetPermissions(version_storage_api, full_asset_path, &permissions);
                if (err)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->GetPermissions() failed with %d", err)
                    LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                    Longtail_Free(full_asset_path);
                    Longtail_Free(remove_indexes);
                    return err;
                }
                if (!(permissions & Longtail_StorageAPI_UserWriteAccess))
                {
                    err = version_storage_api->SetPermissions(version_storage_api, full_asset_path, permissions | (Longtail_StorageAPI_UserWriteAccess));
                    if (err)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                        LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                        Longtail_Free(full_asset_path);
                        Longtail_Free(remove_indexes);
                        return err;
                    }
                }
                err = version_storage_api->RemoveFile(version_storage_api, full_asset_path);
                if (err && version_storage_api->IsFile(version_storage_api, full_asset_path))
                {
                    if (!retry_count)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Can't to file dir `%s`, failed with %d", full_asset_path, err)
                        LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, err)
                        Longtail_Free(full_asset_path);
                        Longtail_Free(remove_indexes);
                        return err;
                    }
                    Longtail_Free(full_asset_path);
                    full_asset_path = 0;
                    continue;
                }
                LONGTAIL_MONITOR_ASSET_REMOVE(source_version, asset_index, 0)
            }
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            remove_indexes[r] = 0xffffffff;
            ++successful_remove_count;
        }
    }
    Longtail_Free(remove_indexes);
    return err;
}

static int RetainPermissions(
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    char* full_path = 0;

    uint32_t version_diff_modified_permissions_count = *version_diff->m_ModifiedPermissionsCount;
    for (uint32_t i = 0; i < version_diff_modified_permissions_count; ++i)
    {
        if ((i & 0x7f) == 0x7f)
        {
            if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
            {
                int err = ECANCELED;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Operation cancelled, failed with %d", err)
                return err;
            }
        }
        uint32_t asset_index = version_diff->m_TargetPermissionsModifiedAssetIndexes[i];
        const char* asset_path = &target_version->m_NameData[target_version->m_NameOffsets[asset_index]];
        char* full_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
        uint16_t permissions = (uint16_t)target_version->m_Permissions[asset_index];
        int err = version_storage_api->SetPermissions(version_storage_api, full_path, permissions);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed for `%s` with %d", full_path, err)
            Longtail_Free(full_path);
            return err;
        }
        Longtail_Free(full_path);
    }

    uint32_t version_diff_added_permissions_count = *version_diff->m_TargetAddedCount;
    for (uint32_t i = 0; i < version_diff_added_permissions_count; ++i)
    {
        if ((i & 0x7f) == 0x7f)
        {
            if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
            {
                int err = ECANCELED;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Operation cancelled, failed with %d", err)
                return err;
            }
        }
        uint32_t asset_index = version_diff->m_TargetAddedAssetIndexes[i];
        const char* asset_path = &target_version->m_NameData[target_version->m_NameOffsets[asset_index]];
        if (!IsDirPath(asset_path))
        {
            char* full_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            uint16_t permissions = (uint16_t)target_version->m_Permissions[asset_index];
            int err = version_storage_api->SetPermissions(version_storage_api, full_path, permissions);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed for `%s` with %d", full_path, err)
                Longtail_Free(full_path);
                return err;
            }
            Longtail_Free(full_path);
        }
    }

    return 0;
}

int Longtail_ChangeVersion(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path,
    int retain_permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(source_version, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    int err = EnsureParentPathExists(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    err = version_storage_api->CreateDir(version_storage_api, version_path);
    if (err == EEXIST)
    {
        err = 0;
    }
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->CreateDir() failed with %d", err)
        return err;
    }

    err = CleanUpRemoveAssets(version_storage_api, optional_cancel_api, optional_cancel_token, source_version, version_diff, version_path);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CleanUpRemoveAssets() failed with %d", err)
        return err;
    }

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t write_asset_count = added_count + modified_content_count;

    LONGTAIL_FATAL_ASSERT(ctx, write_asset_count <= *target_version->m_AssetCount, return EINVAL);
    if (write_asset_count > 0)
    {
        uint32_t chunk_count = (uint32_t)*store_index->m_ChunkCount;
        size_t chunk_hash_to_block_index_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
        size_t asset_indexes_size = sizeof(uint32_t) * write_asset_count;
        size_t work_mem_size = chunk_hash_to_block_index_size + asset_indexes_size;

        void* work_mem = Longtail_Alloc("ChangeVersion", work_mem_size);
        if (!work_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }

        char* p = (char*)work_mem;
        struct Longtail_LookupTable* chunk_hash_to_block_index = LongtailPrivate_LookupTable_Create(p, chunk_count, 0);
        p += chunk_hash_to_block_index_size;
        uint32_t* asset_indexes = (uint32_t*)p;

        uint32_t block_count = *store_index->m_BlockCount;
        for (uint32_t b = 0; b < block_count; ++b)
        {
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
            uint32_t chunk_index_offset = store_index->m_BlockChunksOffsets[b];
            for (uint32_t c = 0; c < block_chunk_count; ++c)
        {
                uint32_t chunk_index = chunk_index_offset + c;
                TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
                LongtailPrivate_LookupTable_PutUnique(chunk_hash_to_block_index, chunk_hash, b);
            }
        }

        for (uint32_t i = 0; i < added_count; ++i)
        {
            asset_indexes[i] = version_diff->m_TargetAddedAssetIndexes[i];
        }
        for (uint32_t i = 0; i < modified_content_count; ++i)
        {
            asset_indexes[added_count + i] = version_diff->m_TargetContentModifiedAssetIndexes[i];
        }

        struct AssetWriteList* awl;
        err = BuildAssetWriteList(
            write_asset_count,
            asset_indexes,
            target_version->m_NameOffsets,
            target_version->m_NameData,
            target_version->m_ChunkHashes,
            target_version->m_AssetChunkCounts,
            target_version->m_AssetChunkIndexStarts,
            target_version->m_AssetChunkIndexes,
            chunk_hash_to_block_index,
            &awl);

        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BuildAssetWriteList() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }

        err = WriteAssets(
            block_store_api,
            version_storage_api,
            job_api,
            progress_api,
            optional_cancel_api,
            optional_cancel_token,
            store_index,
            target_version,
            version_path,
            chunk_hash_to_block_index,
            awl,
            retain_permissions);

        Longtail_Free(awl);
        awl = 0;

        if (err)
        {
            LONGTAIL_LOG(ctx, err == ECANCELED ?  LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }

        Longtail_Free(work_mem);
        work_mem = 0;
    }

    if (err == 0 && retain_permissions)
    {
        err = RetainPermissions(version_storage_api, optional_cancel_api, optional_cancel_token, target_version, version_diff, version_path);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "RetainPermissions() failed with %d", err)
        }
    }

    return err;
}


struct Longtail_BlockChunkWriteInfo
{
    uint32_t ChunkIndex;
    uint32_t AssetIndex;
    uint64_t Offset;
};

typedef struct Longtail_BlockChunkWriteInfo* TBlockChunkWriteArray;

struct Longtail_BlockWriteInfos
{
    TBlockChunkWriteArray* m_BlockWritesArrays;
    uint32_t m_BlockCount;
    TBlockChunkWriteArray m_ZeroSizeWriteInfoArray;
};

struct ContentBlock2JobContext
{
    const struct Longtail_StoreIndex* m_StoreIndex;
    const struct Longtail_VersionIndex* m_VersionIndex;
    const struct Longtail_BlockWriteInfos* m_BlockWriteInfos;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_ConcurrentChunkWriteAPI* m_ConcurrentChunkWriteApi;
    struct Longtail_JobAPI* m_JobAPI;
    const char* m_VersionPath;
};

struct ContentBlock2Job
{
    struct Longtail_AsyncGetStoredBlockAPI m_AsyncCompleteAPI;
    const struct ContentBlock2JobContext* m_Context;
    uint32_t m_BlockIndex;
    struct Longtail_StoredBlock* m_StoredBlock;
    int m_GetStoredBlockErr;
    uint32_t m_JobID;
};

static SORTFUNC(SortBlockChunkWriteInfo)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(a_ptr, "%p"),
        LONGTAIL_LOGFIELD(b_ptr, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context == 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, b_ptr != 0, return 0)

    const struct Longtail_BlockChunkWriteInfo* a = (const struct Longtail_BlockChunkWriteInfo*)a_ptr;
    const struct Longtail_BlockChunkWriteInfo* b = (const struct Longtail_BlockChunkWriteInfo*)b_ptr;
    if (a->AssetIndex < b->AssetIndex)
    {
        return -1;
    }
    if (a->AssetIndex > b->AssetIndex)
    {
        return 1;
    }
    if (a->Offset < b->Offset)
    {
        return -1;
    }
    if (a->Offset > b->Offset)
    {
        return 1;
    }
    if (a->ChunkIndex < b->ChunkIndex)
    {
        return -1;
    }
    if (a->ChunkIndex > b->ChunkIndex)
    {
        return 1;
    }
    return 0;
}

static void WriteContentBlock2GetStoredBlockComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlock2GetStoredBlockComplete() failed with %d", err)
    }
    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api != 0, return)
    struct ContentBlock2Job* job = (struct ContentBlock2Job*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_AsyncCompleteAPI.OnComplete != 0, return);
    LONGTAIL_MONTITOR_BLOCK_LOADED(job->m_Context->m_StoreIndex, job->m_BlockIndex, err);
    job->m_GetStoredBlockErr = err;
    job->m_StoredBlock = stored_block;
    job->m_Context->m_JobAPI->ResumeJob(job->m_Context->m_JobAPI, job->m_JobID);
}

static int WriteNonBlockAssetsJob(void* context, uint32_t job_id, int detected_error)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct ContentBlock2JobContext* job = (struct ContentBlock2JobContext*)context;

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteNonBlockAssetsJob aborted due to previous error %d", detected_error)
        return 0;
    }

    TBlockChunkWriteArray block_write_infos = job->m_BlockWriteInfos->m_ZeroSizeWriteInfoArray;
    ptrdiff_t block_write_chunk_info_count = arrlen(block_write_infos);
    for (ptrdiff_t block_write_chunk_info_index = 0; block_write_chunk_info_index < block_write_chunk_info_count; ++block_write_chunk_info_index)
    {
        const TBlockChunkWriteArray block_chunk_write_info = &block_write_infos[block_write_chunk_info_index];
        const uint32_t asset_index = block_chunk_write_info->AssetIndex;
        const char* asset_path = &job->m_VersionIndex->m_NameData[job->m_VersionIndex->m_NameOffsets[asset_index]];
        if (IsDirPath(asset_path))
        {
            int err = job->m_ConcurrentChunkWriteApi->CreateDir(job->m_ConcurrentChunkWriteApi, asset_index);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job->m_ConcurrentChunkWriteApi->CreateDir() failed with %d", err)
                LONGTAIL_MONTITOR_ASSET_OPEN(job->m_VersionIndex, asset_index, err);
                return err;
            }
        }
        else
        {
            int err = job->m_ConcurrentChunkWriteApi->Open(job->m_ConcurrentChunkWriteApi, asset_index);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "m_ConcurrentChunkWriteApi->Open() failed with %d", err)
                return err;
            }
            LONGTAIL_MONTITOR_ASSET_OPEN(job->m_VersionIndex, asset_index, err);
            job->m_ConcurrentChunkWriteApi->Close(job->m_ConcurrentChunkWriteApi, asset_index);
        }
        LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_VersionIndex, asset_index);
    }
    return 0;
}

static int WriteContentBlock2Job(void* context, uint32_t job_id, int detected_error)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(detected_error, "%d")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct ContentBlock2Job* job = (struct ContentBlock2Job*)context;

    if (detected_error)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteContentBlock2Job aborted due to previous error %d", detected_error)
        SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
        return 0;
    }

    uint32_t block_index = job->m_BlockIndex;
    TLongtail_Hash block_hash = job->m_Context->m_StoreIndex->m_BlockHashes[block_index];

    if (job->m_AsyncCompleteAPI.OnComplete == 0)
    {
        LONGTAIL_FATAL_ASSERT(ctx, job->m_StoredBlock == 0, return EINVAL)
        job->m_JobID = job_id;
        job->m_AsyncCompleteAPI.OnComplete = WriteContentBlock2GetStoredBlockComplete;

        LONGTAIL_MONTITOR_BLOCK_LOAD(job->m_Context->m_StoreIndex, block_index);
        int err = job->m_Context->m_BlockStoreAPI->GetStoredBlock(job->m_Context->m_BlockStoreAPI, block_hash, &job->m_AsyncCompleteAPI);
        if (err == 0)
        {
            return EBUSY;
        }
        else
        {
            LONGTAIL_MONTITOR_BLOCK_LOADED(job->m_Context->m_StoreIndex, block_index, err);
            return err;
        }
    }

    if (job->m_GetStoredBlockErr != 0)
    {
        return job->m_GetStoredBlockErr;
    }

    LONGTAIL_FATAL_ASSERT(ctx, job->m_StoredBlock != 0, return EINVAL)
    struct Longtail_StoredBlock* stored_block = job->m_StoredBlock;

    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    size_t chunk_hash_to_chunk_index_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
    size_t chunk_offsets_size = sizeof(uint32_t) * chunk_count;

    size_t work_mem_size = chunk_hash_to_chunk_index_size + chunk_offsets_size;
    void* work_mem = Longtail_Alloc("WriteContentBlock2Job", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
        LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, block_index, ENOMEM);
        return ENOMEM;
    }
    uint8_t* data_ptr = (uint8_t*)work_mem;

    struct Longtail_LookupTable* chunk_hash_to_chunk_index = LongtailPrivate_LookupTable_Create(data_ptr, chunk_count, 0);
    data_ptr += chunk_hash_to_chunk_index_size;
    uint32_t* chunk_offsets_in_block = (uint32_t*)data_ptr;

    uint32_t chunk_offset = 0;
    for (uint32_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index)
    {
        uint32_t* existing_ptr = LongtailPrivate_LookupTable_PutUnique(chunk_hash_to_chunk_index, stored_block->m_BlockIndex->m_ChunkHashes[chunk_index], chunk_index);
        LONGTAIL_FATAL_ASSERT(ctx, !existing_ptr, return EINVAL)
        chunk_offsets_in_block[chunk_index] = chunk_offset;
        chunk_offset += stored_block->m_BlockIndex->m_ChunkSizes[chunk_index];
    }

    const uint8_t* block_data = (const uint8_t*)stored_block->m_BlockData;

    int asset_is_open = 0;
    uint32_t last_asset_index = 0;

    TBlockChunkWriteArray write_infos = job->m_Context->m_BlockWriteInfos->m_BlockWritesArrays[block_index];
    ptrdiff_t block_write_chunk_info_count = arrlen(write_infos);

    QSORT(write_infos, block_write_chunk_info_count, sizeof(struct Longtail_BlockChunkWriteInfo), SortBlockChunkWriteInfo, 0);

    ptrdiff_t block_write_chunk_info_index = 0;
    while (block_write_chunk_info_index < block_write_chunk_info_count)
    {
        const TBlockChunkWriteArray block_chunk_write_info = &write_infos[block_write_chunk_info_index];
        const uint32_t asset_index = block_chunk_write_info->AssetIndex;
        const char* asset_path = &job->m_Context->m_VersionIndex->m_NameData[job->m_Context->m_VersionIndex->m_NameOffsets[asset_index]];

        if (asset_is_open)
        {
            if (last_asset_index != asset_index)
            {
                job->m_Context->m_ConcurrentChunkWriteApi->Close(job->m_Context->m_ConcurrentChunkWriteApi, last_asset_index);
                asset_is_open = 0;
            }
        }

        if (asset_is_open == 0)
        {
            uint32_t asset_chunk_count = job->m_Context->m_VersionIndex->m_AssetChunkCounts[asset_index];
            int err = job->m_Context->m_ConcurrentChunkWriteApi->Open(job->m_Context->m_ConcurrentChunkWriteApi, asset_index);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Open() failed for `%s` with %d", asset_path, err)
                LONGTAIL_MONTITOR_ASSET_OPEN(job->m_Context->m_VersionIndex, asset_index, err);
                Longtail_Free(work_mem);
                SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
                LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, job->m_BlockIndex, err);
                return err;
            }
            last_asset_index = asset_index;
            asset_is_open = 1;
        }

        uint32_t chunk_index = block_chunk_write_info->ChunkIndex;
        TLongtail_Hash chunk_hash = job->m_Context->m_VersionIndex->m_ChunkHashes[chunk_index];
        uint32_t* chunk_index_in_block_ptr = LongtailPrivate_LookupTable_Get(chunk_hash_to_chunk_index, chunk_hash);
        if (chunk_index_in_block_ptr == 0)
        {
            job->m_Context->m_ConcurrentChunkWriteApi->Close(job->m_Context->m_ConcurrentChunkWriteApi, asset_index);
            asset_is_open = 0;
            LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_Context->m_VersionIndex, asset_index);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to find chunk in block for `%s` with %d", asset_path, ENOENT)
            Longtail_Free(work_mem);
            SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
            LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, job->m_BlockIndex, ENOENT);
            return ENOENT;
        }

        uint32_t chunk_index_in_block = *chunk_index_in_block_ptr;
        uint32_t chunk_size = stored_block->m_BlockIndex->m_ChunkSizes[chunk_index_in_block];
        uint32_t chunk_offset_in_block = chunk_offsets_in_block[chunk_index_in_block];
        LONGTAIL_MONTITOR_CHUNK_READ(job->m_Context->m_StoreIndex, job->m_Context->m_VersionIndex, block_index, chunk_index, chunk_index_in_block, 0);

        uint32_t chunk_run_count = 1;
        uint32_t chunk_run_size = chunk_size;
        while (block_write_chunk_info_index + chunk_run_count < block_write_chunk_info_count)
        {
            const TBlockChunkWriteArray block_chunk_write_info_next = &write_infos[block_write_chunk_info_index + chunk_run_count];
            if (block_chunk_write_info_next->AssetIndex != asset_index)
            {
                break;
            }
            if (block_chunk_write_info_next->Offset != (block_chunk_write_info->Offset + chunk_run_size))
            {
                break;
            }
            uint32_t chunk_index_next = block_chunk_write_info_next->ChunkIndex;
            TLongtail_Hash chunk_hash_next = job->m_Context->m_VersionIndex->m_ChunkHashes[chunk_index_next];
            uint32_t* chunk_index_in_block_ptr_next = LongtailPrivate_LookupTable_Get(chunk_hash_to_chunk_index, chunk_hash_next);
            if (chunk_index_in_block_ptr_next == 0)
            {
                job->m_Context->m_ConcurrentChunkWriteApi->Close(job->m_Context->m_ConcurrentChunkWriteApi, asset_index);
                asset_is_open = 0;
                LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_Context->m_VersionIndex, asset_index);
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to find chunk in block for `%s` with %d", asset_path, ENOENT)
                Longtail_Free(work_mem);
                SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
                LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, job->m_BlockIndex, ENOENT);
                return ENOENT;
            }
            uint32_t chunk_index_in_block_next = *chunk_index_in_block_ptr_next;
            if (chunk_index_in_block_next != chunk_index_in_block + chunk_run_count)
            {
                break;
            }
            uint32_t chunk_size_next = stored_block->m_BlockIndex->m_ChunkSizes[chunk_index_in_block_next];
            LONGTAIL_MONTITOR_CHUNK_READ(job->m_Context->m_StoreIndex, job->m_Context->m_VersionIndex, block_index, chunk_index_next, chunk_index_in_block_next, 0);

            chunk_run_size += chunk_size_next;
            chunk_run_count++;
        }

        int err = job->m_Context->m_ConcurrentChunkWriteApi->Write(job->m_Context->m_ConcurrentChunkWriteApi, asset_index, block_chunk_write_info->Offset, chunk_run_size, &block_data[chunk_offset_in_block]);
        LONGTAIL_MONTITOR_ASSET_WRITE(job->m_Context->m_StoreIndex, job->m_Context->m_VersionIndex, asset_index, block_chunk_write_info->Offset, chunk_run_size, chunk_index, chunk_index_in_block, chunk_run_count, block_index, chunk_offset_in_block, err);
        if (err)
        {
            job->m_Context->m_ConcurrentChunkWriteApi->Close(job->m_Context->m_ConcurrentChunkWriteApi, asset_index);
            asset_is_open = 0;
            LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_Context->m_VersionIndex, asset_index);
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Write() failed for `%s` with %d", asset_path, err)
            Longtail_Free(work_mem);
            SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
            LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, job->m_BlockIndex, err);
            return err;
        }
        block_write_chunk_info_index += chunk_run_count;
    }
    if (asset_is_open != 0)
    {
        job->m_Context->m_ConcurrentChunkWriteApi->Close(job->m_Context->m_ConcurrentChunkWriteApi, last_asset_index);
        asset_is_open = 0;
        LONGTAIL_MONTITOR_ASSET_CLOSE(job->m_Context->m_VersionIndex, last_asset_index);
    }

    Longtail_Free(work_mem);
    SAFE_DISPOSE_STORED_BLOCK(job->m_StoredBlock);
    LONGTAIL_MONTITOR_BLOCK_COMPLETE(job->m_Context->m_StoreIndex, job->m_BlockIndex, 0);
    job->m_Context->m_ConcurrentChunkWriteApi->Flush(job->m_Context->m_ConcurrentChunkWriteApi);
    return 0;
}

static void FreeBlockWriteInfos(struct Longtail_BlockWriteInfos* block_write_infos)
{
    for (ptrdiff_t i = 0; i < block_write_infos->m_BlockCount; ++i)
    {
        arrfree(block_write_infos->m_BlockWritesArrays[i]);
    }
    arrfree(block_write_infos->m_ZeroSizeWriteInfoArray);
    Longtail_Free(block_write_infos);
}

#define SAVE_FREE_BLOCK_WRITE_INFOS(i) if (i) {FreeBlockWriteInfos(i); i = 0;}

static int CreateBlockWriteInfos(
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const struct Longtail_StoreIndex* store_index,
    struct Longtail_BlockWriteInfos** out_block_write_infos)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t write_asset_count = added_count + modified_content_count;

    if (write_asset_count == 0)
    {
        return 0;
    }

    int err = 0;

    uint32_t chunk_count = *store_index->m_ChunkCount;
    uint32_t block_count = *store_index->m_BlockCount;

    size_t block_write_infos_size = sizeof(struct Longtail_BlockWriteInfos) + sizeof(TBlockChunkWriteArray) * (block_count);
    void* block_write_infos_mem = Longtail_Alloc("ChangeVersion2", block_write_infos_size);
    if (!block_write_infos_mem)
    {
        err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        return err;
    }
    struct Longtail_BlockWriteInfos* block_write_infos = (struct Longtail_BlockWriteInfos*)block_write_infos_mem;
    block_write_infos->m_BlockCount = block_count;
    if (block_count > 0)
    {
        block_write_infos->m_BlockWritesArrays = (TBlockChunkWriteArray*)&block_write_infos[1];
        memset(block_write_infos->m_BlockWritesArrays, 0, sizeof(TBlockChunkWriteArray) * block_count);
    }
    else
    {
        block_write_infos->m_BlockWritesArrays = 0;
    }
    block_write_infos->m_ZeroSizeWriteInfoArray = 0;

    size_t chunk_hash_to_block_index_size = LongtailPrivate_LookupTable_GetSize(chunk_count);
    size_t asset_indexes_size = sizeof(uint32_t) * write_asset_count;
    size_t work_mem_size = chunk_hash_to_block_index_size + asset_indexes_size;
    void* work_mem = Longtail_Alloc("ChangeVersion2", work_mem_size);
    if (!work_mem)
    {
        err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        SAVE_FREE_BLOCK_WRITE_INFOS(block_write_infos);
        return err;
    }
    uint8_t* work_mem_ptr = (uint8_t*)work_mem;

    struct Longtail_LookupTable* chunk_hash_to_block_index = LongtailPrivate_LookupTable_Create(work_mem_ptr, chunk_count, 0);
    work_mem_ptr += chunk_hash_to_block_index_size;

    uint32_t* asset_indexes = (uint32_t*)work_mem_ptr;

    for (uint32_t b = 0; b < block_count; ++b)
    {
        LONGTAIL_MONTITOR_BLOCK_PREPARE(store_index, b);
        uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
        uint32_t chunk_index_offset = store_index->m_BlockChunksOffsets[b];
        for (uint32_t c = 0; c < block_chunk_count; ++c)
        {
            uint32_t chunk_index = chunk_index_offset + c;
            TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
            LongtailPrivate_LookupTable_PutUnique(chunk_hash_to_block_index, chunk_hash, b);
        }
    }

    for (uint32_t i = 0; i < added_count; ++i)
    {
        asset_indexes[i] = version_diff->m_TargetAddedAssetIndexes[i];
    }
    for (uint32_t i = 0; i < modified_content_count; ++i)
    {
        asset_indexes[added_count + i] = version_diff->m_TargetContentModifiedAssetIndexes[i];
    }

    uint32_t* name_offsets = target_version->m_NameOffsets;
    const char* name_data = target_version->m_NameData;
    const TLongtail_Hash* chunk_hashes = target_version->m_ChunkHashes;
    const TLongtail_Hash* block_hashes = store_index->m_BlockHashes;
    const uint32_t* asset_chunk_counts = target_version->m_AssetChunkCounts;
    const uint32_t* asset_chunk_index_starts = target_version->m_AssetChunkIndexStarts;
    const uint32_t* asset_chunk_indexes = target_version->m_AssetChunkIndexes;

    for (uint32_t i = 0; i < write_asset_count; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        const char* path = &name_data[name_offsets[asset_index]];
        uint32_t chunk_count = asset_chunk_counts[asset_index];
        uint32_t asset_chunk_offset = asset_chunk_index_starts[asset_index];
        if (chunk_count == 0)
        {
            struct Longtail_BlockChunkWriteInfo chunk_write_info;
            chunk_write_info.ChunkIndex = 0;
            chunk_write_info.AssetIndex = asset_index;
            chunk_write_info.Offset = 0;

            arrput(block_write_infos->m_ZeroSizeWriteInfoArray, chunk_write_info);
            continue;
        }
        uint64_t file_offset = 0;
        for (uint32_t chunk_offset = 0; chunk_offset < chunk_count; ++chunk_offset)
        {
            uint32_t chunk_index = asset_chunk_indexes[asset_chunk_offset + chunk_offset];
            TLongtail_Hash chunk_hash = chunk_hashes[chunk_index];
            uint32_t* content_block_index = LongtailPrivate_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
            if (content_block_index == 0)
            {
                err = ENOENT;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LongtailPrivate_LookupTable_Get() failed with %d", err)
                SAVE_FREE_BLOCK_WRITE_INFOS(block_write_infos);
                Longtail_Free(work_mem);
                return err;
            }
            LONGTAIL_MONTITOR_BLOCK_PREPARE(store_index, *content_block_index);
            TBlockChunkWriteArray* block_write_info = &block_write_infos->m_BlockWritesArrays[*content_block_index];

            struct Longtail_BlockChunkWriteInfo chunk_write_info;
            chunk_write_info.ChunkIndex = chunk_index;
            chunk_write_info.AssetIndex = asset_index;
            chunk_write_info.Offset = file_offset;

            arrput(*block_write_info, chunk_write_info);

            file_offset += target_version->m_ChunkSizes[chunk_index];
        }
    }
    Longtail_Free(work_mem);
    work_mem = 0;

    *out_block_write_infos = block_write_infos;
    return 0;
}

int Longtail_ChangeVersion2(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_ConcurrentChunkWriteAPI* concurrent_chunk_write_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path,
    int retain_permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(version_storage_api, "%p"),
        LONGTAIL_LOGFIELD(concurrent_chunk_write_api, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(source_version, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    int err = EnsureParentPathExists(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    err = version_storage_api->CreateDir(version_storage_api, version_path);
    if (err == EEXIST)
    {
        err = 0;
    }
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->CreateDir() failed with %d", err)
        return err;
    }

    err = block_store_api->PreflightGet(block_store_api, *store_index->m_BlockCount, store_index->m_BlockHashes, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "block_store_api->PreflightGet() failed with %d", err)
        return err;
    }

    err = CleanUpRemoveAssets(version_storage_api, optional_cancel_api, optional_cancel_token, source_version, version_diff, version_path);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CleanUpRemoveAssets() failed with %d", err)
        return err;
    }

    err = concurrent_chunk_write_api->Flush(concurrent_chunk_write_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Flush failed with %d", err)
        return err;
    }

    struct Longtail_BlockWriteInfos* block_write_infos = 0;
    err = CreateBlockWriteInfos(
        target_version,
        version_diff,
        store_index,
        &block_write_infos);
    
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateBlockWriteInfos failed with %d", err)
        return err;
    }

    if (block_write_infos != 0)
    {
        ptrdiff_t block_write_info_count = block_write_infos->m_BlockCount;
        ptrdiff_t zero_size_job_count = (arrlen(block_write_infos->m_ZeroSizeWriteInfoArray) > 0) ? 1 : 0;
        size_t job_count = block_write_info_count + zero_size_job_count;
        if (job_count > 0)
        {
            size_t job_mem_size =
                sizeof(struct ContentBlock2Job) * block_write_info_count +
                sizeof(Longtail_JobAPI_JobFunc) * job_count +
                sizeof(void*) * job_count;
            void* job_mem = Longtail_Alloc("ChangeVersion2", job_mem_size);
            if (job_mem == 0)
            {
                err = ENOMEM;
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
                SAVE_FREE_BLOCK_WRITE_INFOS(block_write_infos);
                return err;
            }
            uint8_t* job_mem_ptr = (uint8_t*)job_mem;
            struct ContentBlock2Job* jobs = (struct ContentBlock2Job*)job_mem_ptr;
            job_mem_ptr += sizeof(struct ContentBlock2Job) * block_write_info_count;
            Longtail_JobAPI_JobFunc* job_funcs = (Longtail_JobAPI_JobFunc*)job_mem_ptr;
            job_mem_ptr += sizeof(Longtail_JobAPI_JobFunc) * job_count;
            void** job_ctxs = (void**)job_mem_ptr;

            struct ContentBlock2JobContext context;
            context.m_StoreIndex = store_index;
            context.m_VersionIndex = target_version;
            context.m_BlockWriteInfos = block_write_infos;
            context.m_BlockStoreAPI = block_store_api;
            context.m_ConcurrentChunkWriteApi = concurrent_chunk_write_api;
            context.m_JobAPI = job_api;
            context.m_VersionPath = version_path;

            if (zero_size_job_count)
            {
                job_funcs[0] = WriteNonBlockAssetsJob;
                job_ctxs[0] = &context;
            }

            for (ptrdiff_t i = 0; i < block_write_info_count; ++i)
            {
                struct ContentBlock2Job* job = &jobs[i];
                job->m_AsyncCompleteAPI.m_API.Dispose = 0;
                job->m_AsyncCompleteAPI.OnComplete = 0;
                job->m_Context = &context;
                job->m_BlockIndex = (uint32_t)i;
                job->m_StoredBlock = 0;
                job->m_JobID = 0;

                job_funcs[i + zero_size_job_count] = WriteContentBlock2Job;
                job_ctxs[i + zero_size_job_count] = job;
            }

            uint32_t jobs_submitted = 0;
            err = Longtail_RunJobsBatched(
                job_api,
                progress_api,
                optional_cancel_api,
                optional_cancel_token,
                (uint32_t)job_count,
                job_funcs,
                job_ctxs,
                &jobs_submitted);
            if (err)
            {
                LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RunJobsBatched() failed with %d", err)
                Longtail_Free(job_mem);
                SAVE_FREE_BLOCK_WRITE_INFOS(block_write_infos);
                return err;
            }

            SAVE_FREE_BLOCK_WRITE_INFOS(block_write_infos);
            Longtail_Free(job_mem);
            job_mem = 0;
        }

        err = concurrent_chunk_write_api->Flush(concurrent_chunk_write_api);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Flush() failed with %d", err)
            return err;
        }
    }

    if (retain_permissions)
    {
        err = RetainPermissions(version_storage_api, optional_cancel_api, optional_cancel_token, target_version, version_diff, version_path);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "RetainPermissions() failed with %d", err)
            (void)concurrent_chunk_write_api->Flush(concurrent_chunk_write_api);
            return err;
        }
    }
    return 0;
}

size_t Longtail_GetStoreIndexDataSize(uint32_t block_count, uint32_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    return
        sizeof(uint32_t) +                          // m_Version
        sizeof(uint32_t) +                          // m_HashIdentifier
        sizeof(uint32_t) +                          // m_BlockCount
        sizeof(uint32_t) +                          // m_ChunkCount
        (sizeof(TLongtail_Hash) * block_count) +    // m_BlockHashes
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * block_count) +          // m_BlockChunksOffsets
        (sizeof(uint32_t) * block_count) +          // m_BlockChunkCounts
        (sizeof(uint32_t) * block_count) +          // m_BlockTags
        (sizeof(uint32_t) * chunk_count);           // m_ChunkSizes
}

struct Longtail_StoreIndex* Longtail_InitStoreIndex(void* mem, uint32_t block_count, uint32_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)

    struct Longtail_StoreIndex* store_index = (struct Longtail_StoreIndex*)mem;
    char* p = (char*)&store_index[1];

    store_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_BlockCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_BlockHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * block_count;

    store_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * chunk_count;

    store_index->m_BlockChunksOffsets = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_BlockChunkCounts = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_BlockTags = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_ChunkSizes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * chunk_count;

    return store_index;
}

static int InitStoreIndexFromData(
    struct Longtail_StoreIndex* store_index,
    void* data,
    uint64_t data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(data_size, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)

    if (data_size < (4 * sizeof(uint32_t)))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Store index is invalid, not big enough for minimal header. Size %" PRIu64 " < %" PRIu64 "", data_size, (4 * sizeof(uint32_t)));
        return EBADF;
    }

    char* p = (char*)data;

    store_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if (*store_index->m_Version != Longtail_CurrentStoreIndexVersion)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Mismatching versions in store index data %" PRIu64 " != %" PRIu64 "", (void*)store_index->m_Version, Longtail_CurrentStoreIndexVersion);
        return EBADF;
    }

    store_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_BlockCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    store_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t block_count = *store_index->m_BlockCount;
    uint32_t chunk_count = *store_index->m_ChunkCount;

    size_t store_index_data_size = Longtail_GetStoreIndexDataSize(block_count, chunk_count);
    if (store_index_data_size > data_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Store index data is truncated: %" PRIu64 " <= %" PRIu64, data_size, store_index_data_size)
        return EBADF;
    }

    store_index->m_BlockHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * block_count;

    store_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash) * chunk_count;

    store_index->m_BlockChunksOffsets = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_BlockChunkCounts = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_BlockTags = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * block_count;

    store_index->m_ChunkSizes = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * chunk_count;

    return 0;
}

size_t Longtail_GetStoreIndexSize(uint32_t block_count, uint32_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    size_t store_index_size =
        sizeof(struct Longtail_StoreIndex) +
        Longtail_GetStoreIndexDataSize(block_count, chunk_count);

    return store_index_size;
}

int Longtail_CreateStoreIndexFromBlocks(
    uint32_t block_count,
    const struct Longtail_BlockIndex** block_indexes,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(block_indexes, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_count == 0 || block_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t hash_identifier = 0;

    uint32_t chunk_count = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        const struct Longtail_BlockIndex* block_index = block_indexes[b];
        hash_identifier = (hash_identifier == 0) ? *block_index->m_HashIdentifier : hash_identifier;
        chunk_count += *block_index->m_ChunkCount;
    }
    size_t store_index_size = Longtail_GetStoreIndexSize(block_count, chunk_count);
    void* store_index_mem = (struct Longtail_StoreIndex*)Longtail_Alloc("CreateStoreIndexFromBlocks", store_index_size);
    if (!store_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_StoreIndex* store_index = Longtail_InitStoreIndex(
        store_index_mem,
        block_count,
        chunk_count);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoreIndex() failed with %d", ENOMEM)
        return ENOMEM;
    }

    *store_index->m_Version = Longtail_CurrentStoreIndexVersion;
    *store_index->m_HashIdentifier = hash_identifier;
    *store_index->m_BlockCount = block_count;
    *store_index->m_ChunkCount = chunk_count;
    uint32_t c = 0;

    for (uint32_t b = 0; b < block_count; ++b)
    {
        const struct Longtail_BlockIndex* block_index = block_indexes[b];
        uint32_t block_chunk_count = *block_index->m_ChunkCount;
        store_index->m_BlockHashes[b] = *block_index->m_BlockHash;
        store_index->m_BlockTags[b] = *block_index->m_Tag;
        store_index->m_BlockChunkCounts[b] = block_chunk_count;
        store_index->m_BlockChunksOffsets[b] = c;
        memcpy(&store_index->m_ChunkHashes[c], block_index->m_ChunkHashes, sizeof(TLongtail_Hash) * block_chunk_count);
        memcpy(&store_index->m_ChunkSizes[c], block_index->m_ChunkSizes, sizeof(uint32_t) * block_chunk_count);
        c += block_chunk_count;
    }

    *out_store_index = store_index;
    return 0;
}

int Longtail_MakeBlockIndex(
    const struct Longtail_StoreIndex* store_index,
    uint32_t block_index,
    struct Longtail_BlockIndex* out_block_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(block_index, "%u"),
        LONGTAIL_LOGFIELD(out_block_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_index < (*store_index->m_BlockCount), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    uint32_t block_chunks_offset = store_index->m_BlockChunksOffsets[block_index];
    out_block_index->m_BlockHash = &store_index->m_BlockHashes[block_index];
    out_block_index->m_HashIdentifier = store_index->m_HashIdentifier;
    out_block_index->m_ChunkCount = &store_index->m_BlockChunkCounts[block_index];
    out_block_index->m_Tag = &store_index->m_BlockTags[block_index];
    out_block_index->m_ChunkHashes = &store_index->m_ChunkHashes[block_chunks_offset];
    out_block_index->m_ChunkSizes = &store_index->m_ChunkSizes[block_chunks_offset];
    return 0;
}

int Longtail_MergeStoreIndex(
    const struct Longtail_StoreIndex* local_store_index,
    const struct Longtail_StoreIndex* remote_store_index,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(local_store_index, "%p"),
        LONGTAIL_LOGFIELD(remote_store_index, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, local_store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, local_store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t local_block_count = *local_store_index->m_BlockCount;
    uint32_t remote_block_count = *remote_store_index->m_BlockCount;
    uint32_t hash_identifier = 0;
    if (local_block_count == 0)
    {
        if (remote_block_count == 0)
        {
            return Longtail_CreateStoreIndexFromBlocks(0, 0, out_store_index);
        }
        hash_identifier = *remote_store_index->m_HashIdentifier;
    }
    else
    {
        hash_identifier = *local_store_index->m_HashIdentifier;
        if (remote_block_count != 0)
        {
            if (hash_identifier != *remote_store_index->m_HashIdentifier)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeStoreIndex(), store indexes has conflicting hash identifier, failed with %d", EINVAL)
                return EINVAL;
            }
        }
    }
    size_t local_block_hash_to_index_size = LongtailPrivate_LookupTable_GetSize(local_block_count);
    size_t remote_block_hash_to_index_size = LongtailPrivate_LookupTable_GetSize(remote_block_count);
    size_t block_hashes_size = sizeof(TLongtail_Hash) * ((size_t)local_block_count + (size_t)remote_block_count);
    size_t work_mem_size = local_block_hash_to_index_size + remote_block_hash_to_index_size + block_hashes_size;

    void* work_mem = Longtail_Alloc("MergeStoreIndex", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;
    struct Longtail_LookupTable* local_block_hash_to_index = LongtailPrivate_LookupTable_Create(p, local_block_count, 0);
    p += local_block_hash_to_index_size;
    struct Longtail_LookupTable* remote_block_hash_to_index = LongtailPrivate_LookupTable_Create(p, remote_block_count, 0);
    p += remote_block_hash_to_index_size;
    TLongtail_Hash* block_hashes = (TLongtail_Hash*)p;

    uint32_t unique_block_count = 0;
    uint32_t chunk_count = 0;
    for (uint32_t local_block = 0; local_block < local_block_count; ++local_block)
    {
        TLongtail_Hash block_hash = local_store_index->m_BlockHashes[local_block];
        if (LongtailPrivate_LookupTable_PutUnique(local_block_hash_to_index, block_hash, local_block))
        {
            continue;
        }
        block_hashes[unique_block_count] = block_hash;
        ++unique_block_count;
        chunk_count += local_store_index->m_BlockChunkCounts[local_block];
    }
    for (uint32_t remote_block = 0; remote_block < remote_block_count; ++remote_block)
    {
        TLongtail_Hash block_hash = remote_store_index->m_BlockHashes[remote_block];
        if (LongtailPrivate_LookupTable_Get(local_block_hash_to_index, block_hash))
        {
            continue;
        }
        if (LongtailPrivate_LookupTable_PutUnique(remote_block_hash_to_index, block_hash, remote_block))
        {
            continue;
        }
        block_hashes[unique_block_count] = block_hash;
        ++unique_block_count;
        chunk_count += remote_store_index->m_BlockChunkCounts[remote_block];
    }

    size_t merged_block_store_index_size = Longtail_GetStoreIndexSize(unique_block_count, chunk_count);
    void* merged_block_store_index_mem = Longtail_Alloc("MergeStoreIndex", merged_block_store_index_size);
    if (!merged_block_store_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(work_mem);
        return ENOMEM;
    }
    struct Longtail_StoreIndex* merged_store_index = Longtail_InitStoreIndex(merged_block_store_index_mem, unique_block_count, chunk_count);
    *merged_store_index->m_Version = Longtail_CurrentStoreIndexVersion;
    *merged_store_index->m_HashIdentifier = hash_identifier;
    *merged_store_index->m_BlockCount = unique_block_count;
    *merged_store_index->m_ChunkCount = chunk_count;
    uint32_t chunk_index_offset = 0;
    const struct Longtail_StoreIndex* source_index = local_store_index;
    struct Longtail_LookupTable* source_lookup_table = local_block_hash_to_index;
    for (uint32_t b = 0; b < unique_block_count; ++b)
    {
        TLongtail_Hash block_hash = block_hashes[b];
        uint32_t* index_ptr = LongtailPrivate_LookupTable_Get(source_lookup_table, block_hash);
        if (!index_ptr)
        {
            // When block is no longer found in local_store_index, switch over to remote_store_index
            LONGTAIL_FATAL_ASSERT(ctx, source_index != remote_store_index, return EINVAL)
            source_index = remote_store_index;
            source_lookup_table = remote_block_hash_to_index;
            index_ptr = LongtailPrivate_LookupTable_Get(source_lookup_table, block_hash);
            LONGTAIL_FATAL_ASSERT(ctx, index_ptr, return EINVAL)
        }
        uint32_t source_block = *index_ptr;
        uint32_t block_chunk_count = source_index->m_BlockChunkCounts[source_block];
        merged_store_index->m_BlockTags[b] = source_index->m_BlockTags[source_block];
        uint32_t block_chunk_offset = source_index->m_BlockChunksOffsets[source_block];
        const TLongtail_Hash* local_chunk_hashes = &source_index->m_ChunkHashes[block_chunk_offset];
        const uint32_t* local_chunk_sizes = &source_index->m_ChunkSizes[block_chunk_offset];

        merged_store_index->m_BlockHashes[b] = block_hash;
        merged_store_index->m_BlockChunkCounts[b] = block_chunk_count;
        merged_store_index->m_BlockChunksOffsets[b] = chunk_index_offset;
        TLongtail_Hash* merged_chunk_hashes = &merged_store_index->m_ChunkHashes[chunk_index_offset];
        memcpy(merged_chunk_hashes, local_chunk_hashes, sizeof(TLongtail_Hash) * block_chunk_count);
        uint32_t* merged_chunk_sizes = &merged_store_index->m_ChunkSizes[chunk_index_offset];
        memcpy(merged_chunk_sizes, local_chunk_sizes, sizeof(uint32_t) * block_chunk_count);
        chunk_index_offset += block_chunk_count;
    }
    Longtail_Free(work_mem);
    *out_store_index = merged_store_index;
    return 0;
}

LONGTAIL_EXPORT int Longtail_PruneStoreIndex(
    const struct Longtail_StoreIndex* source_store_index,
    uint32_t keep_block_count,
    const TLongtail_Hash* keep_block_hashes,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(source_store_index, "%p"),
        LONGTAIL_LOGFIELD(keep_block_count, "%u"),
        LONGTAIL_LOGFIELD(keep_block_hashes, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, source_store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (keep_block_count == 0) || (keep_block_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t store_block_count = *source_store_index->m_BlockCount;
    uint32_t store_chunk_count = *source_store_index->m_ChunkCount;
    size_t keep_block_hash_lookup_size = LongtailPrivate_LookupTable_GetSize(keep_block_count);
    size_t block_hashes_size = sizeof(TLongtail_Hash) * store_block_count;
    size_t chunk_hashes_size = sizeof(TLongtail_Hash) * store_chunk_count;
    size_t block_chunks_offsets_size = sizeof(uint32_t) * store_block_count;
    size_t block_chunks_counts_size = sizeof(uint32_t) * store_block_count;
    size_t block_tags_size = sizeof(uint32_t) * store_block_count;
    size_t chunk_sizes_size = sizeof(uint32_t) * store_chunk_count;

    size_t work_mem_size = keep_block_hash_lookup_size +
        block_hashes_size +
        chunk_hashes_size +
        block_chunks_offsets_size +
        block_chunks_counts_size +
        block_tags_size +
        chunk_sizes_size;

    void* work_mem = Longtail_Alloc("PruneStoreIndex", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;
    struct Longtail_LookupTable* keep_block_hash_lookup = LongtailPrivate_LookupTable_Create(p, keep_block_count, 0);
    p += keep_block_hash_lookup_size;

    TLongtail_Hash* block_hashes = (TLongtail_Hash*)p;
    p += block_hashes_size;

    TLongtail_Hash* chunk_hashes = (TLongtail_Hash*)p;
    p += chunk_hashes_size;

    uint32_t* block_chunks_offsets = (uint32_t*)p;
    p += block_chunks_offsets_size;

    uint32_t* block_chunks_counts = (uint32_t*)p;
    p += block_chunks_counts_size;

    uint32_t* block_tags = (uint32_t*)p;
    p += block_tags_size;

    uint32_t* chunk_sizes = (uint32_t*)p;
    p += chunk_sizes_size;

    for (uint32_t keep_block = 0; keep_block < keep_block_count; ++keep_block)
    {
        TLongtail_Hash block_hash = keep_block_hashes[keep_block];
        if (LongtailPrivate_LookupTable_PutUnique(keep_block_hash_lookup, block_hash, keep_block))
        {
            continue;
        }
    }

    uint32_t block_count = 0;
    uint32_t chunk_count = 0;
    for (uint32_t block = 0; block < store_block_count; ++block)
    {
        TLongtail_Hash block_hash = source_store_index->m_BlockHashes[block];
        if (!LongtailPrivate_LookupTable_Get(keep_block_hash_lookup, block_hash))
        {
            continue;
        }
        uint32_t block_chunk_count = source_store_index->m_BlockChunkCounts[block];
        uint32_t chunk_offset = source_store_index->m_BlockChunksOffsets[block];

        block_hashes[block_count] = block_hash;
        block_tags[block_count] = source_store_index->m_BlockTags[block];
        block_chunks_offsets[block_count] = chunk_count;
        block_chunks_counts[block_count] = block_chunk_count;
        for (uint32_t chunk = 0; chunk < block_chunk_count; ++chunk)
        {
            chunk_hashes[chunk_count + chunk] = source_store_index->m_ChunkHashes[chunk_offset + chunk];
            chunk_sizes[chunk_count + chunk] = source_store_index->m_ChunkSizes[chunk_offset + chunk];
        }

        chunk_count += block_chunk_count;
        block_count++;
    }

    size_t store_index_size = Longtail_GetStoreIndexSize(block_count, chunk_count);
    void* store_index_mem = (struct Longtail_StoreIndex*)Longtail_Alloc("PruneStoreIndex", store_index_size);
    if (!store_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(work_mem);
        return ENOMEM;
    }
    struct Longtail_StoreIndex* store_index = Longtail_InitStoreIndex(
        store_index_mem,
        block_count,
        chunk_count);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoreIndex() failed with %d", ENOMEM)
        Longtail_Free(work_mem);
        return ENOMEM;
    }

    *store_index->m_Version = Longtail_CurrentStoreIndexVersion;
    *store_index->m_HashIdentifier = *source_store_index->m_HashIdentifier;
    *store_index->m_BlockCount = block_count;
    *store_index->m_ChunkCount = chunk_count;

    memcpy(store_index->m_BlockHashes, block_hashes, sizeof(TLongtail_Hash) * block_count);
    memcpy(store_index->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);
    memcpy(store_index->m_BlockChunksOffsets, block_chunks_offsets, sizeof(uint32_t) * block_count);
    memcpy(store_index->m_BlockChunkCounts, block_chunks_counts, sizeof(uint32_t) * block_count);
    memcpy(store_index->m_BlockTags, block_tags, sizeof(uint32_t) * block_count);
    memcpy(store_index->m_ChunkSizes, chunk_sizes, sizeof(uint32_t) * chunk_count);

    Longtail_Free(work_mem);

    *out_store_index = store_index;
    return 0;
}

LONGTAIL_EXPORT int Longtail_ValidateStore(
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)

    uint32_t store_index_chunk_count = (uint32_t)*store_index->m_ChunkCount;
    struct Longtail_LookupTable* content_chunk_lookup = LongtailPrivate_LookupTable_Create(Longtail_Alloc("ValidateContent", LongtailPrivate_LookupTable_GetSize(store_index_chunk_count)), store_index_chunk_count ,0);
    if (!content_chunk_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
       return ENOMEM;
    }
    for (uint32_t chunk_index = 0; chunk_index < store_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_index];
        LongtailPrivate_LookupTable_Put(content_chunk_lookup, chunk_hash, chunk_index);
    }

    uint32_t chunk_missing_count = 0;
    uint32_t asset_size_mismatch_count = 0;

    uint32_t version_index_chunk_count = *version_index->m_ChunkCount;
    for (uint32_t chunk_index = 0; chunk_index < version_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        if (LongtailPrivate_LookupTable_Get(content_chunk_lookup, chunk_hash) == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ValidateStore() content index does not contain chunk 0x%" PRIx64 "",
                chunk_hash)
            ++chunk_missing_count;
            continue;
        }
    }

    uint32_t version_index_asset_count = *version_index->m_AssetCount;
    for (uint32_t asset_index = 0; asset_index < version_index_asset_count; ++asset_index)
    {
        uint64_t asset_size = version_index->m_AssetSizes[asset_index];
        uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t first_chunk_index = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunked_size = 0;
        for (uint32_t i = 0; i < chunk_count; ++i)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[first_chunk_index + i];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            asset_chunked_size += chunk_size;
        }
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        if ((asset_chunked_size != asset_size) && (!IsDirPath(asset_path)))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateStore() asset size for %s mismatch, accumulated chunks size: %" PRIu64 ", asset size:  %" PRIu64 "",
                asset_path, asset_chunked_size, asset_size)
            ++asset_size_mismatch_count;
        }
    }

    int err = 0;
    if (asset_size_mismatch_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateStore() has %u assets that does not match chunk sizes",
            asset_size_mismatch_count)
        err = EINVAL;
    }

    if (chunk_missing_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateStore() has %u missing chunks",
            chunk_missing_count)
        err = err ? err : ENOENT;
    }

    Longtail_Free(content_chunk_lookup);

    return err;
}

struct Longtail_StoreIndex* Longtail_CopyStoreIndex(const struct Longtail_StoreIndex* store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return 0)

    uint32_t block_count = *store_index->m_BlockCount;
    uint32_t chunk_count = *store_index->m_ChunkCount;
    size_t store_index_size = Longtail_GetStoreIndexSize(block_count, chunk_count);
    void* mem = Longtail_Alloc("Longtail_CopyStoreIndex", store_index_size);
    struct Longtail_StoreIndex* copy_store_index = Longtail_InitStoreIndex(mem, block_count, chunk_count);

    *copy_store_index->m_Version = Longtail_CurrentStoreIndexVersion;
    *copy_store_index->m_HashIdentifier = *store_index->m_HashIdentifier;
    *copy_store_index->m_BlockCount = block_count;
    *copy_store_index->m_ChunkCount = chunk_count;
    memcpy(copy_store_index->m_BlockHashes, store_index->m_BlockHashes, sizeof(TLongtail_Hash) * block_count);
    memcpy(copy_store_index->m_ChunkHashes, store_index->m_ChunkHashes, sizeof(TLongtail_Hash) * chunk_count);
    memcpy(copy_store_index->m_BlockChunksOffsets, store_index->m_BlockChunksOffsets, sizeof(uint32_t) * block_count);
    memcpy(copy_store_index->m_BlockChunkCounts, store_index->m_BlockChunkCounts, sizeof(uint32_t) * block_count);
    memcpy(copy_store_index->m_BlockTags, store_index->m_BlockTags, sizeof(uint32_t) * block_count);
    memcpy(copy_store_index->m_ChunkSizes, store_index->m_ChunkSizes, sizeof(uint32_t) * chunk_count);
    return copy_store_index;
}

static int CreateStoreIndexFromRange(
    const struct Longtail_StoreIndex* store_index,
    uint32_t block_range_start,
    uint32_t block_range_end,
    uint32_t block_range_chunk_count,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(block_range_start, "%u"),
        LONGTAIL_LOGFIELD(block_range_end, "%u"),
        LONGTAIL_LOGFIELD(block_range_chunk_count, "%u"),
        LONGTAIL_LOGFIELD(out_store_index, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_range_start < block_range_end, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_range_chunk_count > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t block_range_count = block_range_end - block_range_start;

    size_t block_indexes_size = 0;
    for (uint32_t block_offset = 0; block_offset < block_range_count; ++block_offset)
    {
        uint32_t b = block_offset + block_range_start;
        block_indexes_size += Longtail_GetBlockIndexSize(store_index->m_BlockChunkCounts[b]);
    }
    size_t work_mem_size =
        sizeof(struct Longtail_BlockIndex*) * block_range_count +
        block_indexes_size;

    void* work_mem = (uint8_t*)Longtail_Alloc("CreateStoreIndexFromRange", work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;
    struct Longtail_BlockIndex** block_index_array = (struct Longtail_BlockIndex**)p;
    p += sizeof(struct Longtail_BlockIndex*) * block_range_count;

    for (uint32_t block_offset = 0; block_offset < block_range_count; ++block_offset)
    {
        uint32_t b = block_offset + block_range_start;
        uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
        struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(p, block_chunk_count);

        *block_index->m_BlockHash = store_index->m_BlockHashes[b];
        *block_index->m_HashIdentifier = *store_index->m_HashIdentifier;
        *block_index->m_ChunkCount = block_chunk_count;
        *block_index->m_Tag = store_index->m_BlockTags[b];
        uint32_t block_chunk_offset = store_index->m_BlockChunksOffsets[b];
        for (uint32_t chunk_offset = 0; chunk_offset < block_chunk_count; ++chunk_offset)
        {
            block_index->m_ChunkHashes[chunk_offset] = store_index->m_ChunkHashes[block_chunk_offset + chunk_offset];
            block_index->m_ChunkSizes[chunk_offset] = store_index->m_ChunkSizes[block_chunk_offset + chunk_offset];
        }
        block_index_array[block_offset] = block_index;
        p += Longtail_GetBlockIndexSize(block_chunk_count);
    }
    int err = Longtail_CreateStoreIndexFromBlocks(block_range_count, (const struct Longtail_BlockIndex**)block_index_array, out_store_index);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }
    Longtail_Free(work_mem);
    return 0;
}

int Longtail_SplitStoreIndex(
    struct Longtail_StoreIndex* store_index,
    size_t split_size,
    struct Longtail_StoreIndex*** out_store_indexes,
    uint64_t* out_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(split_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_store_indexes, "%p"),
        LONGTAIL_LOGFIELD(out_count, "%p"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, split_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_count != 0, return EINVAL)

    uint32_t block_count = *store_index->m_BlockCount;
    if (block_count == 0)
    {
        struct Longtail_StoreIndex** result = (struct Longtail_StoreIndex**)Longtail_Alloc("Longtail_SplitStoreIndex", sizeof(struct Longtail_StoreIndex*));
        if (!result)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }
        int err = Longtail_CreateStoreIndexFromBlocks(0, 0, &result[0]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoreIndexFromBlocks() failed with %d", err)
            Longtail_Free((void*)result);
            return err;
        }
        *out_store_indexes = result;
        *out_count = 1;
        return 0;
    }

    size_t result_count = 0;
    {
        uint32_t block_range_start = 0;
        uint32_t block_range_end = 0;
        uint32_t block_range_chunk_count = 0;
        size_t current_index_size = 0;
        while (block_range_end < block_count)
        {
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[block_range_end];
            size_t next_index_size = Longtail_GetStoreIndexSize(block_range_end - block_range_start + 1, block_range_chunk_count + block_chunk_count);
            if (block_range_end > block_range_start && next_index_size > split_size)
            {
                result_count++;
                block_range_start = block_range_end;
                block_range_chunk_count = 0;
                current_index_size = 0;
                continue;
            }
            block_range_chunk_count += block_chunk_count;
            current_index_size += next_index_size;
            ++block_range_end;
        }
        if (block_range_start < block_range_end)
        {
            result_count++;
        }
    }
    LONGTAIL_FATAL_ASSERT(ctx, result_count > 0, return EINVAL);

    struct Longtail_StoreIndex** result = (struct Longtail_StoreIndex**)Longtail_Alloc("Longtail_SplitStoreIndex", sizeof(struct Longtail_StoreIndex*) * result_count);
    if (!result)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    {
        result_count = 0;
        uint32_t block_range_start = 0;
        uint32_t block_range_end = 0;
        uint32_t block_range_chunk_count = 0;
        size_t current_index_size = 0;
        while (block_range_end < block_count)
        {
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[block_range_end];
            size_t next_index_size = Longtail_GetStoreIndexSize(block_range_end - block_range_start + 1, block_range_chunk_count + block_chunk_count);
            if (block_range_end > block_range_start && next_index_size > split_size)
            {
                int err = CreateStoreIndexFromRange(
                    store_index,
                    block_range_start,
                    block_range_end,
                    block_range_chunk_count,
                    &result[result_count]);
                if (err != 0)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateStoreIndexFromRange failed with %d", err)
                    while (result_count > 0)
                    {
                        --result_count;
                        Longtail_Free((void*)result[result_count]);
                    }
                    Longtail_Free((void*)result);
                    return err;
                }
                result_count++;
                block_range_start = block_range_end;
                block_range_chunk_count = 0;
                current_index_size = 0;
                continue;
            }
            block_range_chunk_count += block_chunk_count;
            current_index_size += next_index_size;
            ++block_range_end;
        }
        if (block_range_start < block_range_end)
        {
            LONGTAIL_FATAL_ASSERT(ctx, block_range_chunk_count > 0, return EINVAL);
            int err = CreateStoreIndexFromRange(
                store_index,
                block_range_start,
                block_range_end,
                block_range_chunk_count,
                &result[result_count]);
            if (err != 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateStoreIndexFromRange failed with %d", err)
                while (result_count > 0)
                {
                    --result_count;
                    Longtail_Free((void*)result[result_count]);
                }
                Longtail_Free((void*)result);
                return err;
            }
            result_count++;
        }
    }
    *out_store_indexes = result;
    *out_count = result_count;

    return 0;
}

int Longtail_WriteStoreIndexToBuffer(
    const struct Longtail_StoreIndex* store_index,
    void** out_buffer,
    size_t* out_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(out_buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetStoreIndexDataSize(*store_index->m_BlockCount, *store_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc("WriteStoreIndexToBuffer", index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetStoreIndexDataSize() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, store_index->m_Version, index_data_size);
    *out_size = index_data_size;
    return 0;
}

int Longtail_WriteStoreIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_StoreIndex* store_index,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)

    size_t index_data_size = Longtail_GetStoreIndexDataSize(*store_index->m_BlockCount, *store_index->m_ChunkCount);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenWriteFile() failed with %d", err)
        return err;
    }

    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, store_index->m_Version);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "wrote %" PRIu64 " bytes", index_data_size)

    return 0;
}

int Longtail_ReadStoreIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    size_t store_index_size = sizeof(struct Longtail_StoreIndex) + size;
    struct Longtail_StoreIndex* store_index = (struct Longtail_StoreIndex*)Longtail_Alloc("ReadStoreIndexFromBuffer", store_index_size);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(&store_index[1], buffer, size);
    int err = InitStoreIndexFromData(store_index, &store_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitStoreIndexFromData() failed with %d", err)
        Longtail_Free(store_index);
        return err;
    }
    *out_store_index = store_index;
    return 0;
}

int Longtail_ReadStoreIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint64_t store_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &store_index_data_size);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->GetSize() failed with %d", err)
        return err;
    }
    size_t store_index_size = store_index_data_size + sizeof(struct Longtail_StoreIndex);
    struct Longtail_StoreIndex* store_index = (struct Longtail_StoreIndex*)Longtail_Alloc("ReadStoreIndex", store_index_size);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = storage_api->Read(storage_api, file_handle, 0, store_index_data_size, &store_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(store_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = InitStoreIndexFromData(store_index, &store_index[1], store_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitStoreIndexFromData() failed with %d", err)
        Longtail_Free(store_index);
        return err;
    }

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Read store index containing %u chunk in %u blocks",
        *store_index->m_ChunkCount, *store_index->m_BlockCount)

    *out_store_index = store_index;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "read %" PRIu64 " bytes", store_index_data_size)

    return 0;
}

LONGTAIL_EXPORT int Longtail_CreateArchiveIndex(
    const struct Longtail_StoreIndex* store_index,
    const struct Longtail_VersionIndex* version_index,
    struct Longtail_ArchiveIndex** out_archive_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(out_archive_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_archive_index != 0, return EINVAL)

    size_t store_index_data_size = Longtail_GetStoreIndexDataSize(*store_index->m_BlockCount, *store_index->m_ChunkCount);
    size_t version_index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);
    size_t archive_data_index_size =
        sizeof(uint32_t) +
        sizeof(uint32_t) +
        store_index_data_size +
        sizeof(uint64_t) * (*store_index->m_BlockCount) +
        sizeof(uint32_t) * (*store_index->m_BlockCount) +
        version_index_data_size;
    archive_data_index_size = (archive_data_index_size + 7) & 0xfffffff8;

    size_t archive_index_size =
        sizeof(struct Longtail_ArchiveIndex) +
        archive_data_index_size;

    struct Longtail_ArchiveIndex* archive_index = (struct Longtail_ArchiveIndex*)Longtail_Alloc("Longtail_CreateArchive", archive_index_size);
    if (!archive_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint8_t* p = (uint8_t*)&archive_index[1];
    archive_index->m_Version = (uint32_t*)p;
    p += sizeof(uint32_t);

    *archive_index->m_Version = Longtail_CurrentArchiveVersion;

    archive_index->m_IndexDataSize = (uint32_t*)p;
    p += sizeof(uint32_t);

    *archive_index->m_IndexDataSize = (uint32_t)archive_data_index_size;

    void* store_index_data_ptr = p;
    memcpy(p, store_index->m_Version, store_index_data_size);
    p += store_index_data_size;

    archive_index->m_BlockStartOffets = (uint64_t*)p;
    p += sizeof(uint64_t) * (*store_index->m_BlockCount);

    archive_index->m_BlockSizes = (uint32_t*)p;
    p += sizeof(uint32_t) * (*store_index->m_BlockCount);

    void* version_index_data_ptr = p;
    memcpy(p, version_index->m_Version, version_index_data_size);

    int err = InitStoreIndexFromData(&archive_index->m_StoreIndex, store_index_data_ptr, store_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitStoreIndexFromData() failed with %d", err)
        Longtail_Free(archive_index);
        return err;
    }
    err = InitVersionIndexFromData(&archive_index->m_VersionIndex, version_index_data_ptr, version_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData() failed with %d", err)
        Longtail_Free(archive_index);
        return err;
    }

    *out_archive_index = archive_index;

    return 0;
}

LONGTAIL_EXPORT int Longtail_ReadArchiveIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_ArchiveIndex** out_archive_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_archive_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint32_t version_and_size[2];
    err = storage_api->Read(storage_api, file_handle, 0, sizeof(uint32_t) * 2, &version_and_size[0]);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        return err;
    }
    if (version_and_size[0] != Longtail_CurrentArchiveVersion)
    {
        return EBADF;
    }
    uint32_t archive_index_data_size = version_and_size[1];
    size_t archive_index_size = sizeof(struct Longtail_ArchiveIndex) + archive_index_data_size;
    struct Longtail_ArchiveIndex* archive_index = (struct Longtail_ArchiveIndex*)Longtail_Alloc("Longtail_ReadArchiveIndex", archive_index_size);
    if (!archive_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = storage_api->Read(storage_api, file_handle, 0, archive_index_data_size, &archive_index[1]);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(archive_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }

    uint8_t* p = (uint8_t*)&archive_index[1];
    archive_index->m_Version = (uint32_t*)p;
    p += sizeof(uint32_t);

    archive_index->m_IndexDataSize = (uint32_t*)p;
    p += sizeof(uint32_t);

    err = InitStoreIndexFromData(&archive_index->m_StoreIndex, p, archive_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitStoreIndexFromData() failed with %d", err)
        Longtail_Free(archive_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    size_t store_index_data_size = Longtail_GetStoreIndexDataSize(*archive_index->m_StoreIndex.m_BlockCount, *archive_index->m_StoreIndex.m_ChunkCount);
    p += store_index_data_size;

    archive_index->m_BlockStartOffets = (uint64_t*)p;
    p += sizeof(uint64_t) * (*archive_index->m_StoreIndex.m_BlockCount);

    archive_index->m_BlockSizes = (uint32_t*)p;
    p += sizeof(uint32_t) * (*archive_index->m_StoreIndex.m_BlockCount);

    uint32_t offset = (uint32_t)(p - (const uint8_t*)&archive_index[1]);

    uint32_t version_index_data_size = (uint32_t)(archive_index_data_size -
        (sizeof(uint32_t) +
        sizeof(uint32_t) +
        store_index_data_size +
        sizeof(uint64_t) * (*archive_index->m_StoreIndex.m_BlockCount) +
        sizeof(uint32_t) * (*archive_index->m_StoreIndex.m_BlockCount)));

    err = InitVersionIndexFromData(&archive_index->m_VersionIndex, p, version_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData() failed with %d", err)
        Longtail_Free(archive_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }

    storage_api->CloseFile(storage_api, file_handle);
    *out_archive_index = archive_index;

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "read %" PRIu64 " bytes", sizeof(uint32_t) * 2 + archive_index_data_size)

    return 0;
}


uint32_t Longtail_BlockIndex_GetChunkCount(const struct Longtail_BlockIndex* block_index) { return *block_index->m_ChunkCount; }
const uint32_t* Longtail_BlockIndex_GetChunkTag(const struct Longtail_BlockIndex* block_index) { return block_index->m_Tag; }
const TLongtail_Hash* Longtail_BlockIndex_GetChunkHashes(const struct Longtail_BlockIndex* block_index) { return block_index->m_ChunkHashes; }
const uint32_t* Longtail_BlockIndex_GetChunkSizes(const struct Longtail_BlockIndex* block_index) { return block_index->m_ChunkSizes; }

void Longtail_StoredBlock_Dispose(struct Longtail_StoredBlock* stored_block) { if (stored_block && stored_block->Dispose) { stored_block->Dispose(stored_block); } }
struct Longtail_BlockIndex* Longtail_StoredBlock_GetBlockIndex(struct Longtail_StoredBlock* stored_block) { return stored_block->m_BlockIndex; }
void* Longtail_BlockIndex_BlockData(struct Longtail_StoredBlock* stored_block) { return stored_block->m_BlockData; }
uint32_t Longtail_BlockIndex_GetBlockChunksDataSize(struct Longtail_StoredBlock* stored_block) { return stored_block->m_BlockChunksDataSize; }

uint32_t Longtail_FileInfos_GetCount(const struct Longtail_FileInfos* file_infos) { return file_infos->m_Count; }
const char* Longtail_FileInfos_GetPath(const struct Longtail_FileInfos* file_infos, uint32_t index) { return &file_infos->m_PathData[file_infos->m_PathStartOffsets[index]]; }
uint64_t Longtail_FileInfos_GetSize(const struct Longtail_FileInfos* file_infos, uint32_t index) { return file_infos->m_Sizes[index]; }
const uint16_t* Longtail_FileInfos_GetPermissions(const struct Longtail_FileInfos* file_infos, uint32_t index) { return file_infos->m_Permissions; }

uint32_t Longtail_VersionIndex_GetVersion(const struct Longtail_VersionIndex* version_index) { return *version_index->m_Version; }
uint32_t Longtail_VersionIndex_GetHashAPI(const struct Longtail_VersionIndex* version_index) { return *version_index->m_HashIdentifier; }
uint32_t Longtail_VersionIndex_GetAssetCount(const struct Longtail_VersionIndex* version_index) { return *version_index->m_AssetCount; }
uint32_t Longtail_VersionIndex_GetChunkCount(const struct Longtail_VersionIndex* version_index) { return *version_index->m_ChunkCount; }
const TLongtail_Hash* Longtail_VersionIndex_GetChunkHashes(const struct Longtail_VersionIndex* version_index) { return version_index->m_ChunkHashes;}
const uint32_t* Longtail_VersionIndex_GetChunkSizes(const struct Longtail_VersionIndex* version_index) { return version_index->m_ChunkSizes;}
const uint32_t* Longtail_VersionIndex_GetChunkTags(const struct Longtail_VersionIndex* version_index) { return version_index->m_ChunkTags;}

uint32_t Longtail_StoreIndex_GetVersion(const struct Longtail_StoreIndex* store_index) { return *store_index->m_Version;}
uint32_t Longtail_StoreIndex_GetHashIdentifier(const struct Longtail_StoreIndex* store_index) { return *store_index->m_HashIdentifier;}
uint32_t Longtail_StoreIndex_GetBlockCount(const struct Longtail_StoreIndex* store_index) { return *store_index->m_BlockCount;}
uint32_t Longtail_StoreIndex_GetChunkCount(const struct Longtail_StoreIndex* store_index) { return *store_index->m_ChunkCount;}
const TLongtail_Hash* Longtail_StoreIndex_GetBlockHashes(const struct Longtail_StoreIndex* store_index) { return store_index->m_BlockHashes;}
const TLongtail_Hash* Longtail_StoreIndex_GetChunkHashes(const struct Longtail_StoreIndex* store_index) { return store_index->m_ChunkHashes;}
const uint32_t* Longtail_StoreIndex_GetBlockChunksOffsets(const struct Longtail_StoreIndex* store_index) { return store_index->m_BlockChunksOffsets;}
const uint32_t* Longtail_StoreIndex_GetBlockChunkCounts(const struct Longtail_StoreIndex* store_index) { return store_index->m_BlockChunkCounts;}
const uint32_t* Longtail_StoreIndex_GetBlockTags(const struct Longtail_StoreIndex* store_index) { return store_index->m_BlockTags;}
const uint32_t* Longtail_StoreIndex_GetChunkSizes(const struct Longtail_StoreIndex* store_index) { return store_index->m_ChunkSizes;}
