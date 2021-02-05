#include "longtail.h"

#if defined(__GNUC__) && !defined(__clang__) && !defined(APPLE) && !defined(__USE_GNU)
#define __USE_GNU
#endif

#include "ext/stb_ds.h"

#include <ctype.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>
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

/*
#if defined(LONGTAIL_ASSERTS)
void* Longtail_NukeMalloc(size_t s);
void Longtail_NukeFree(void* p);
#    define Longtail_Alloc(s) \
        Longtail_NukeMalloc(s)
#    define Longtail_Free(p) \
        Longtail_NukeFree(p)
#else
#    define Longtail_Alloc(s) \
        malloc(s)
#    define Longtail_Free(p) \
        Longtail_Free(p)
#endif // defined(LONGTAIL_ASSERTS)
*/

#define LONGTAIL_VERSION(major, minor, patch)  ((((uint32_t)major) << 24) | ((uint32_t)minor << 16) | ((uint32_t)patch))
#define LONGTAIL_VERSION_0_0_1  LONGTAIL_VERSION(0,0,1)
#define LONGTAIL_VERSION_INDEX_VERSION_0_0_2  LONGTAIL_VERSION(0,0,2)
#define LONGTAIL_CONTENT_INDEX_VERSION_0_0_1  LONGTAIL_VERSION(0,0,1)
#define LONGTAIL_CONTENT_INDEX_VERSION_1_0_0  LONGTAIL_VERSION(1,0,0)
#define LONGTAIL_STORE_INDEX_VERSION_1_0_0    LONGTAIL_VERSION(1,0,0)

uint32_t Longtail_CurrentContentIndexVersion = LONGTAIL_CONTENT_INDEX_VERSION_1_0_0;
uint32_t Longtail_CurrentVersionIndexVersion = LONGTAIL_VERSION_INDEX_VERSION_0_0_2;
uint32_t Longtail_CurrentStoreIndexVersion = LONGTAIL_STORE_INDEX_VERSION_1_0_0;

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    Longtail_Storage_UnlockFileFunc unlock_file_func)
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
        LONGTAIL_LOGFIELD(unlock_file_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    Longtail_Job_ResumeJobFunc resume_job_func)
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
        LONGTAIL_LOGFIELD(resume_job_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    return api;
}

uint32_t Longtail_Job_GetWorkerCount(struct Longtail_JobAPI* job_api) { return job_api->GetWorkerCount(job_api); }
int Longtail_Job_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Group* out_job_group) { return job_api->ReserveJobs(job_api, job_count, out_job_group); }
int Longtail_Job_CreateJobs(struct Longtail_JobAPI* job_api, Longtail_JobAPI_Group job_group, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs) { return job_api->CreateJobs(job_api, job_group, job_count, job_funcs, job_contexts, out_jobs); }
int Longtail_Job_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs) { return job_api->AddDependecies(job_api, job_count, jobs, dependency_job_count, dependency_jobs); }
int Longtail_Job_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs) { return job_api->ReadyJobs(job_api, job_count, jobs); }
int Longtail_Job_WaitForAllJobs(struct Longtail_JobAPI* job_api, Longtail_JobAPI_Group job_group, struct Longtail_ProgressAPI* progressAPI, struct Longtail_CancelAPI* optional_cancel_api, Longtail_CancelAPI_HCancelToken optional_cancel_token) { return job_api->WaitForAllJobs(job_api, job_group, progressAPI, optional_cancel_api, optional_cancel_token); }
int Longtail_Job_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id) { return job_api->ResumeJob(job_api, job_id); }

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
    Longtail_Chunker_DisposeChunkerFunc dispose_chunker_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(get_min_chunk_size_func, "%p"),
        LONGTAIL_LOGFIELD(create_chunker_func, "%p"),
        LONGTAIL_LOGFIELD(next_chunk_func, "%p"),
        LONGTAIL_LOGFIELD(dispose_chunker_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_ChunkerAPI* api = (struct Longtail_ChunkerAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->GetMinChunkSize = get_min_chunk_size_func;
    api->CreateChunker = create_chunker_func;
    api->NextChunk = next_chunk_func;
    api->DisposeChunker = dispose_chunker_func;
    return api;
}

int Longtail_Chunker_GetMinChunkSize(struct Longtail_ChunkerAPI* chunker_api, uint32_t* out_min_chunk_size) { return chunker_api->GetMinChunkSize(chunker_api, out_min_chunk_size); }
int Longtail_Chunker_CreateChunker(struct Longtail_ChunkerAPI* chunker_api, uint32_t min_chunk_size, uint32_t avg_chunk_size, uint32_t max_chunk_size, Longtail_ChunkerAPI_HChunker* out_chunker) { return chunker_api->CreateChunker(chunker_api, min_chunk_size, avg_chunk_size, max_chunk_size, out_chunker); }
int Longtail_Chunker_NextChunk(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker, Longtail_Chunker_Feeder feeder, void* feeder_context, struct Longtail_Chunker_ChunkRange* out_chunk_range) { return chunker_api->NextChunk(chunker_api, chunker, feeder, feeder_context, out_chunk_range); }
int Longtail_Chunker_DisposeChunker(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker) { return chunker_api->DisposeChunker(chunker_api, chunker); }

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_AsyncGetExistingContentAPI* api = (struct Longtail_AsyncGetExistingContentAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

void Longtail_AsyncGetExistingContent_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err) { async_complete_api->OnComplete(async_complete_api, content_index, err); }

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
        LONGTAIL_LOGFIELD(get_stats_func, "%p"),
        LONGTAIL_LOGFIELD(flush_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_BlockStoreAPI* api = (struct Longtail_BlockStoreAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->PutStoredBlock = put_stored_block_func;
    api->PreflightGet = preflight_get_func;
    api->GetStoredBlock = get_stored_block_func;
    api->GetExistingContent = get_existing_content_func;
    api->GetStats = get_stats_func;
    api->Flush = flush_func;
    return api;
}

int Longtail_BlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api) { return block_store_api->PutStoredBlock(block_store_api, stored_block, async_complete_api); }
int Longtail_BlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t chunk_count, const TLongtail_Hash* chunk_hashes) { return block_store_api->PreflightGet(block_store_api, chunk_count, chunk_hashes); }
int Longtail_BlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api) { return block_store_api->GetStoredBlock(block_store_api, block_hash, async_complete_api); }
int Longtail_BlockStore_GetExistingContent(struct Longtail_BlockStoreAPI* block_store_api, uint64_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_AsyncGetExistingContentAPI* async_complete_api) { return block_store_api->GetExistingContent(block_store_api, chunk_count, chunk_hashes, min_block_usage_percent, async_complete_api); }
int Longtail_BlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats) { return block_store_api->GetStats(block_store_api, out_stats); }
int Longtail_BlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api) {return block_store_api->Flush(block_store_api, async_complete_api); }

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    if (api->Dispose)
    {
        api->Dispose(api);
    }
}

static Longtail_Alloc_Func Longtail_Alloc_private = 0;
static Longtail_Free_Func Free_private = 0;

void Longtail_SetAllocAndFree(Longtail_Alloc_Func alloc, Longtail_Free_Func Longtail_Free)
{
    Longtail_Alloc_private = alloc;
    Free_private = Longtail_Free;
}

void* Longtail_Alloc(size_t s)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(s, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    void* mem = Longtail_Alloc_private ? Longtail_Alloc_private(s) : malloc(s);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "%s failed with %d", Longtail_Alloc_private ? "Longtail_Alloc_private" : "malloc()", ENOMEM);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
    Longtail_Log_private = log_func;
    Longtail_LogContext = context;
}

void Longtail_SetLogLevel(int level)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(level, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
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
        char_buffer += chars_used + 1;
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
    char* r = (char*)Longtail_Alloc(strlen(path) + 1);
    if (!r)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", path, ENOMEM)
        return 0;
    }
    strcpy(r, path);
    return r;
}





//////////////////////////////// Longtail_LookupTable

struct Longtail_LookupTable
{
    uint64_t  m_BucketCount;

    uint64_t m_Capacity;
    uint64_t m_Count;

    uint64_t* m_Buckets;
    uint64_t* m_Keys;
    uint64_t* m_Values;
    uint64_t* m_NextIndex;
};

int Longtail_LookupTable_Put(struct Longtail_LookupTable* lut, uint64_t key, uint64_t value)
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

    uint64_t entry_index = lut->m_Count++;
    lut->m_Keys[entry_index] = key;
    lut->m_Values[entry_index] = value;

    uint64_t bucket_index = key & (lut->m_BucketCount - 1);
    uint64_t* buckets = lut->m_Buckets;
    uint64_t index = buckets[bucket_index];
    if (index == 0xfffffffffffffffful)
    {
        buckets[bucket_index] = entry_index;
        return 0;
    }
    uint64_t* next_index = lut->m_NextIndex;
    uint64_t next = next_index[index];
    while (next != 0xfffffffffffffffful)
    {
        index = next;
        next = next_index[index];
    }

    next_index[index] = entry_index;
    return 0;
}

uint64_t* Longtail_LookupTable_PutUnique(struct Longtail_LookupTable* lut, uint64_t key, uint64_t value)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(lut, "%p"),
        LONGTAIL_LOGFIELD(key, "%" PRIu64),
        LONGTAIL_LOGFIELD(value, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint64_t bucket_index = key & (lut->m_BucketCount - 1);
    uint64_t* buckets = lut->m_Buckets;
    uint64_t index = buckets[bucket_index];
    if (index == 0xfffffffffffffffful)
    {
        LONGTAIL_FATAL_ASSERT(ctx, lut->m_Count < lut->m_Capacity, return 0)
        uint64_t entry_index = lut->m_Count++;
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
    uint64_t* next_index = lut->m_NextIndex;
    uint64_t next = next_index[index];
    while (next != 0xfffffffffffffffful)
    {
        index = next;
        if (keys[index] == key)
        {
            return &lut->m_Values[index];
        }
        next = next_index[index];
    }

    LONGTAIL_FATAL_ASSERT(ctx, lut->m_Count < lut->m_Capacity, return 0)
    uint64_t entry_index = lut->m_Count++;
    keys[entry_index] = key;
    lut->m_Values[entry_index] = value;
    next_index[index] = entry_index;
    return 0;
}

uint64_t* Longtail_LookupTable_Get(const struct Longtail_LookupTable* lut, uint64_t key)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(lut, "%p"),
        LONGTAIL_LOGFIELD(key, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    uint64_t bucket_index = key & (lut->m_BucketCount - 1);
    uint64_t index = lut->m_Buckets[bucket_index];
    const uint64_t* keys = lut->m_Keys;
    const uint64_t* next_index = lut->m_NextIndex;
    while (index != 0xfffffffffffffffful)
    {
        if (keys[index] == key)
        {
            return &lut->m_Values[index];
        }
        index = next_index[index];
    }
    return 0;
}

uint64_t Longtail_LookupTable_GetSpaceLeft(const struct Longtail_LookupTable* lut)
{
    return lut->m_Capacity - lut->m_Count;
}

static size_t GetLookupTableSize(size_t capacity)
{
    size_t table_size = 1;
    while (table_size < (capacity / 8))
    {
        table_size <<= 1;
    }
    return table_size;
}

size_t Longtail_LookupTable_GetSize(size_t capacity)
{
    size_t table_size = GetLookupTableSize(capacity);
    size_t mem_size = sizeof(struct Longtail_LookupTable) +
        sizeof(uint64_t) * table_size +
        sizeof(uint64_t) * capacity +
        sizeof(uint64_t) * capacity +
        sizeof(uint64_t) * capacity;
    return mem_size;
}

struct Longtail_LookupTable* Longtail_LookupTable_Create(void* mem, size_t capacity, struct Longtail_LookupTable* optional_source_entries)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(capacity, "%" PRIu64),
        LONGTAIL_LOGFIELD(optional_source_entries, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct Longtail_LookupTable* lut = (struct Longtail_LookupTable*)mem;
    size_t table_size = GetLookupTableSize(capacity);
    lut->m_BucketCount = table_size;
    lut->m_Capacity = capacity;
    lut->m_Count = 0;
    lut->m_Buckets = (uint64_t*)&lut[1];
    lut->m_Keys = (uint64_t*)&lut->m_Buckets[table_size];
    lut->m_Values = &lut->m_Keys[capacity];
    lut->m_NextIndex = &lut->m_Values[capacity];

    memset(lut->m_Buckets, 0xff, sizeof(uint64_t) * table_size);
    memset(lut->m_NextIndex, 0xff, sizeof(uint64_t) * capacity);

    if (optional_source_entries == 0)
    {
        return lut;
    }
    for (uint64_t i = 0; i < optional_source_entries->m_BucketCount; ++i)
    {
        if (optional_source_entries->m_Buckets[i] != 0xfffffffffffffffful)
        {
            uint64_t index = optional_source_entries->m_Buckets[i];
            while (index != 0xfffffffffffffffful)
            {
                uint64_t key = optional_source_entries->m_Keys[index];
                uint64_t value = optional_source_entries->m_Values[index];
                Longtail_LookupTable_Put(lut, key, value);
                index = optional_source_entries->m_NextIndex[index];
            }
        }
    }
    return lut;
}

static void Longtail_ToLowerCase(char *str)
{
    for ( ; *str; ++str)
    {
        *str = tolower(*str);
    }
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

int Longtail_GetPathHash(struct Longtail_HashAPI* hash_api, const char* path, TLongtail_Hash* out_hash)
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
    char* buf = (char*)alloca(pathlen + 1);
    memcpy(buf, path, pathlen + 1);
    Longtail_ToLowerCase(buf);
    uint64_t hash;
    int err = hash_api->HashBuffer(hash_api, pathlen, (void*)buf, &hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_api->HashBuffer() failed with %d", err)
        return err;
    }
    *out_hash = (TLongtail_Hash)hash;
    return 0;
}

static int SafeCreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path != 0, return EINVAL)
    int err = storage_api->CreateDir(storage_api, path);
    if (err)
    {
        if ((err == EEXIST) || storage_api->IsDir(storage_api, path))
        {
            return 0;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->CreateDir() failed with %d", err)
        return err;
    }
    return 0;
}

int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path != 0, return EINVAL)

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
    if (path[delim_pos] != '/')
    {
        return 0;
    }

    size_t path_size = path_len + 1;
    char* dir_path = (char*)alloca(path_size);
    memcpy(dir_path, path, path_size);

    dir_path[delim_pos] = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        return 0;
    }

    int err = EnsureParentPathExists(storage_api, dir_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
        return err;
    }
    err = SafeCreateDir(storage_api, dir_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "SafeCreateDir() failed with %d", err)
        return err;
    }
    return 0;
}






struct HashToIndexItem
{
    TLongtail_Hash key;
    uint64_t value;
};

typedef int (*ProcessEntry)(void* context, const char* root_path, const char* relative_path, const struct Longtail_StorageAPI_EntryProperties* properties);

static int RecurseTree(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_folder,
    ProcessEntry entry_processor,
    void* context)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(optional_path_filter_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(root_folder, "%s"),
        LONGTAIL_LOGFIELD(entry_processor, "%p"),
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, root_folder != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, entry_processor != 0, return EINVAL)

    char* root_folder_copy = Longtail_Strdup(root_folder);
    if (!root_folder_copy)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Strdup(p) failed with %d",
            ENOMEM)
        return ENOMEM;
    }
    uint32_t folder_index = 0;

    char** full_search_paths = 0;
    arrsetcap(full_search_paths, 256);
    char** relative_parent_paths = 0;
    arrsetcap(relative_parent_paths, 256);

    arrput(full_search_paths, root_folder_copy);
    arrput(relative_parent_paths, 0);

    int err = 0;
    while (folder_index < (uint32_t)arrlen(full_search_paths))
    {
        if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
        {
            err = ECANCELED;
            break;
        }

        char* full_search_path = full_search_paths[folder_index];
        char* relative_parent_path = relative_parent_paths[folder_index++];

        Longtail_StorageAPI_HIterator fs_iterator = 0;
        MAKE_LOG_CONTEXT_FIELDS(ctx2)
            LONGTAIL_LOGFIELD(storage_api, "%p"),
            LONGTAIL_LOGFIELD(full_search_path, "%s"),
            LONGTAIL_LOGFIELDFMT_REF(fs_iterator, "%p")
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx2, ctx, LONGTAIL_LOG_LEVEL_DEBUG)
        err = storage_api->StartFind(storage_api, full_search_path, &fs_iterator);
        if (err == ENOENT)
        {
            Longtail_Free((void*)full_search_path);
            full_search_path = 0;
            Longtail_Free((void*)relative_parent_path);
            relative_parent_path = 0;
            err = 0;
            continue;
        }
        else if (err)
        {
            LONGTAIL_LOG(ctx2, LONGTAIL_LOG_LEVEL_WARNING, "storage_api->StartFind() failed with %d", err)
            Longtail_Free((void*)full_search_path);
            full_search_path = 0;
            Longtail_Free((void*)relative_parent_path);
            relative_parent_path = 0;
            break;
        }
        LONGTAIL_LOG(ctx2, LONGTAIL_LOG_LEVEL_DEBUG, "Scanning `%s`", full_search_path)
        while(err == 0)
        {
            struct Longtail_StorageAPI_EntryProperties properties;
            err = storage_api->GetEntryProperties(storage_api, fs_iterator, &properties);
            if (err)
            {
                LONGTAIL_LOG(ctx2, LONGTAIL_LOG_LEVEL_WARNING, "storage_api->GetEntryProperties() failed with %d", err)
            }
            else
            {
                char* asset_path = 0;
                if (!relative_parent_path)
                {
                    asset_path = Longtail_Strdup(properties.m_Name);
                }
                else
                {
                    size_t current_relative_path_length = strlen(relative_parent_path);
                    size_t new_parent_path_length = current_relative_path_length + 1 + strlen(properties.m_Name);
                    asset_path = (char*)Longtail_Alloc(new_parent_path_length + 1);
                    if (!asset_path)
                    {
                        LONGTAIL_LOG(ctx2, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_Alloc() failed with %d", ENOMEM)
                        break;
                    }
                    strcpy(asset_path, relative_parent_path);
                    asset_path[current_relative_path_length] = '/';
                    strcpy(&asset_path[current_relative_path_length + 1], properties.m_Name);
                }

                if (!optional_path_filter_api
                    || optional_path_filter_api->Include(
                        optional_path_filter_api,
                        root_folder,
                        asset_path,
                        properties.m_Name,
                        properties.m_IsDir,
                        properties.m_Size,
                        properties.m_Permissions)
                    )
                {
                    MAKE_LOG_CONTEXT_FIELDS(ctx3)
                        LONGTAIL_LOGFIELD(context, "%p"),
                        LONGTAIL_LOGFIELD(full_search_path, "%s"),
                        LONGTAIL_LOGFIELD(asset_path, "%s"),
                        LONGTAIL_LOGFIELDFMT_REF(properties, "%p")
                    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx3, ctx2, LONGTAIL_LOG_LEVEL_DEBUG)
                    err = entry_processor(context, full_search_path, asset_path, &properties);
                    if (err)
                    {
                        LONGTAIL_LOG(ctx3, LONGTAIL_LOG_LEVEL_WARNING, "entry_processor() failed with %d",
                            err)
                        Longtail_Free(asset_path);
                        asset_path = 0;
                        break;
                    }
                }
                if (properties.m_IsDir)
                {
                    if ((size_t)arrlen(full_search_paths) == arrcap(full_search_paths))
                    {
                        if (folder_index > 0)
                        {
                            uint32_t unprocessed_count = (uint32_t)(arrlen(full_search_paths) - folder_index);
                            memmove(full_search_paths, &full_search_paths[folder_index], sizeof(const char*) * unprocessed_count);
                            arrsetlen(full_search_paths, unprocessed_count);
                            memmove(relative_parent_paths, &relative_parent_paths[folder_index], sizeof(const char*) * unprocessed_count);
                            arrsetlen(relative_parent_paths, unprocessed_count);
                            folder_index = 0;
                        }
                    }
                    arrput(full_search_paths, storage_api->ConcatPath(storage_api, full_search_path, properties.m_Name));
                    arrput(relative_parent_paths, asset_path);
                    asset_path = 0;
                }
                Longtail_Free(asset_path);
            }
            err = storage_api->FindNext(storage_api, fs_iterator);
            if (err == ENOENT)
            {
                err = 0;
                break;
            }
        }
        storage_api->CloseFind(storage_api, fs_iterator);
        Longtail_Free((void*)full_search_path);
        full_search_path = 0;
        Longtail_Free((void*)relative_parent_path);
        relative_parent_path = 0;
    }
    while (folder_index < (uint32_t)arrlen(full_search_paths))
    {
        Longtail_Free(full_search_paths[folder_index]);
        Longtail_Free(relative_parent_paths[folder_index++]);
    }
    arrfree(relative_parent_paths);
    arrfree(full_search_paths);
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
    struct Longtail_FileInfos* file_infos = (struct Longtail_FileInfos*)Longtail_Alloc(file_infos_size);
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

int Longtail_MakeFileInfos(
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

static int AppendPath(
    struct Longtail_FileInfos** file_infos,
    const char* path,
    uint64_t file_size,
    uint16_t file_permissions,
    uint32_t* max_path_count,
    uint32_t* max_data_size,
    uint32_t path_count_increment,
    uint32_t data_size_increment)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(file_infos, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(file_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(file_permissions, "%u"),
        LONGTAIL_LOGFIELD(max_path_count, "%p"),
        LONGTAIL_LOGFIELD(max_data_size, "%p"),
        LONGTAIL_LOGFIELD(path_count_increment, "%u"),
        LONGTAIL_LOGFIELD(data_size_increment, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    LONGTAIL_FATAL_ASSERT(ctx, (*file_infos) != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, max_path_count != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, max_data_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, path_count_increment > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, data_size_increment > 0, return EINVAL)
    uint32_t path_size = (uint32_t)(strlen(path) + 1);

    int out_of_path_data = (*file_infos)->m_PathDataSize + path_size > *max_data_size;
    int out_of_path_count = (*file_infos)->m_Count >= *max_path_count;
    if (out_of_path_count | out_of_path_data)
    {
        uint32_t extra_path_count = out_of_path_count ? path_count_increment : 0;
        uint32_t extra_path_data_size = out_of_path_data ? (path_count_increment * data_size_increment) : 0;

        const uint32_t new_path_count = *max_path_count + extra_path_count;
        const uint32_t new_path_data_size = *max_data_size + extra_path_data_size;
        struct Longtail_FileInfos* new_file_infos = CreateFileInfos(new_path_count, new_path_data_size);
        if (new_file_infos == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateFileInfos() failed with %d", ENOMEM)
            return ENOMEM;
        }
        *max_path_count = new_path_count;
        *max_data_size = new_path_data_size;
        new_file_infos->m_PathDataSize = (*file_infos)->m_PathDataSize;
        new_file_infos->m_Count = (*file_infos)->m_Count;

        memmove(new_file_infos->m_Sizes, (*file_infos)->m_Sizes, sizeof(uint64_t) * (*file_infos)->m_Count);
        memmove(new_file_infos->m_PathStartOffsets, (*file_infos)->m_PathStartOffsets, sizeof(uint32_t) * (*file_infos)->m_Count);
        memmove(new_file_infos->m_Permissions, (*file_infos)->m_Permissions, sizeof(uint32_t) * (*file_infos)->m_Count);
        memmove(new_file_infos->m_PathData, (*file_infos)->m_PathData, (*file_infos)->m_PathDataSize);

        Longtail_Free(*file_infos);
        *file_infos = new_file_infos;
    }

    memmove(&(*file_infos)->m_PathData[(*file_infos)->m_PathDataSize], path, path_size);
    (*file_infos)->m_PathStartOffsets[(*file_infos)->m_Count] = (*file_infos)->m_PathDataSize;
    (*file_infos)->m_PathDataSize += path_size;
    (*file_infos)->m_Sizes[(*file_infos)->m_Count] = file_size;
    (*file_infos)->m_Permissions[(*file_infos)->m_Count] = file_permissions;
    (*file_infos)->m_Count++;

    return 0;
}

struct AddFile_Context {
    struct Longtail_StorageAPI* m_StorageAPI;
    uint32_t m_ReservedPathCount;
    uint32_t m_ReservedPathSize;
    uint32_t m_RootPathLength;
    struct Longtail_FileInfos* m_FileInfos;
};

static int AddFile(void* context, const char* root_path, const char* asset_path, const struct Longtail_StorageAPI_EntryProperties* properties)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(asset_path, "%s"),
        LONGTAIL_LOGFIELD(properties, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, properties != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, properties->m_Name != 0, return EINVAL)
    struct AddFile_Context* paths_context = (struct AddFile_Context*)context;
    struct Longtail_StorageAPI* storage_api = paths_context->m_StorageAPI;

    char* full_path = (char*)asset_path;
    if (properties->m_IsDir)
    {
        size_t asset_path_length = strlen(asset_path);
        full_path = (char*)Longtail_Alloc(asset_path_length + 1 + 1);
        strcpy(full_path, asset_path);
        full_path[asset_path_length] = '/';
        full_path[asset_path_length + 1] = 0;
    }

    int err = AppendPath(&paths_context->m_FileInfos, full_path, properties->m_Size, properties->m_Permissions, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "AppendPath() failed with %d", err)
        if (full_path != asset_path)
        {
            Longtail_Free(full_path);
        }
        return err;
    }

    if (full_path != asset_path)
    {
        Longtail_Free(full_path);
    }
    return 0;
}

int Longtail_GetFilesRecursively(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(optional_path_filter_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(out_file_infos, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_file_infos != 0, return EINVAL)

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Longtail_FileInfos* file_infos = CreateFileInfos(default_path_count, default_path_data_size);
    if (!file_infos)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateFileInfos() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct AddFile_Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), file_infos};
    file_infos = 0;

    int err = RecurseTree(storage_api, optional_path_filter_api, optional_cancel_api, optional_cancel_token, root_path, AddFile, &context);
    if(err)
    {
        LONGTAIL_LOG(ctx, (err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "RecurseTree() failed with %d", err)
        Longtail_Free(context.m_FileInfos);
        context.m_FileInfos = 0;
        return err;
    }

    *out_file_infos = context.m_FileInfos;
    context.m_FileInfos = 0;
    return 0;
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
        int err = c->m_StorageAPI->Read(c->m_StorageAPI, c->m_AssetFile, c->m_StartRange + c->m_Offset, (uint32_t)read_count, buffer);
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

struct HashJob
{
    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_HashAPI* m_HashAPI;
    struct Longtail_ChunkerAPI* m_ChunkerAPI;
    TLongtail_Hash* m_PathHash;
    uint64_t m_AssetIndex;
    uint32_t m_ContentTag;
    const char* m_RootPath;
    const char* m_Path;
    uint32_t m_MaxChunkCount;
    uint64_t m_StartRange;
    uint64_t m_SizeRange;
    uint32_t* m_AssetChunkCount;
    TLongtail_Hash* m_ChunkHashes;
    uint32_t* m_ChunkTags;
    uint32_t* m_ChunkSizes;
    uint32_t m_TargetChunkSize;
    int m_Err;
};

#define MIN_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size / 8) < min_chunk_size) ? min_chunk_size : (target_chunk_size / 8))
#define AVG_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size / 2) < min_chunk_size) ? min_chunk_size : (target_chunk_size / 2))
#define MAX_CHUNKER_SIZE(min_chunk_size, target_chunk_size) (((target_chunk_size * 2) < min_chunk_size) ? min_chunk_size : (target_chunk_size * 2))

static int DynamicChunking(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)
    struct HashJob* hash_job = (struct HashJob*)context;

    if (is_cancelled)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Cancelled with errno %d", ECANCELED)
        hash_job->m_Err = ECANCELED;
        return 0;
    }

    if (hash_job->m_PathHash)
    {
        hash_job->m_Err = Longtail_GetPathHash(hash_job->m_HashAPI, hash_job->m_Path, hash_job->m_PathHash);
        if (hash_job->m_Err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", hash_job->m_Err)
            return 0;
        }
    }

    if (IsDirPath(hash_job->m_Path))
    {
        hash_job->m_Err = 0;
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
        hash_job->m_Err = err;
        return 0;
    }


    uint64_t hash_size = hash_job->m_SizeRange;
    if (hash_size > 0)
    {
        uint32_t chunker_min_size;
        err = hash_job->m_ChunkerAPI->GetMinChunkSize(hash_job->m_ChunkerAPI, &chunker_min_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->GetMinChunkSize() failed with %d", err)
            Longtail_Free(path);
            path = 0;
            hash_job->m_Err = err;
            return 0;
        }
        if (hash_size <= chunker_min_size)
        {
            char* buffer = (char*)Longtail_Alloc((size_t)hash_size);
            if (!buffer)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                hash_job->m_Err = ENOMEM;
                return 0;
            }
            err = storage_api->Read(storage_api, file_handle, hash_job->m_StartRange, hash_size, buffer);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
                Longtail_Free(buffer);
                buffer = 0;
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                hash_job->m_Err = err;
                return 0;
            }

            err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, (uint32_t)hash_size, buffer, &hash_job->m_ChunkHashes[chunk_count]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_HashAPI->HashBuffer() failed with ", err)
                Longtail_Free(buffer);
                buffer = 0;
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                hash_job->m_Err = err;
                return 0;
            }

            Longtail_Free(buffer);
            buffer = 0;

            hash_job->m_ChunkSizes[chunk_count] = (uint32_t)hash_size;
            hash_job->m_ChunkTags[chunk_count] = hash_job->m_ContentTag;

            ++chunk_count;
        }
        else
        {
            uint32_t min_chunk_size = MIN_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);
            uint32_t avg_chunk_size = AVG_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);
            uint32_t max_chunk_size = MAX_CHUNKER_SIZE(chunker_min_size, hash_job->m_TargetChunkSize);
            Longtail_ChunkerAPI_HChunker chunker;
            err = hash_job->m_ChunkerAPI->CreateChunker(hash_job->m_ChunkerAPI, min_chunk_size, avg_chunk_size, max_chunk_size, &chunker);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_job->m_ChunkerAPI->CreateChunker() failed with %d", err)
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                hash_job->m_Err = err;
                return 0;
            }

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
                    hash_job->m_Err = err;
                    return 0;
                }
                hash_job->m_ChunkSizes[chunk_count] = chunk_range.len;
                hash_job->m_ChunkTags[chunk_count] = hash_job->m_ContentTag;

                ++chunk_count;

                err = hash_job->m_ChunkerAPI->NextChunk(hash_job->m_ChunkerAPI, chunker, StorageChunkFeederFunc, &feeder_context, &chunk_range);
            }

            hash_job->m_ChunkerAPI->DisposeChunker(hash_job->m_ChunkerAPI, chunker);
        }
    }

    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;
    
    LONGTAIL_FATAL_ASSERT(ctx, chunk_count <= hash_job->m_MaxChunkCount, hash_job->m_Err = EINVAL; return 0)
    *hash_job->m_AssetChunkCount = chunk_count;

    Longtail_Free((char*)path);
    path = 0;

    hash_job->m_Err = 0;
    return 0;
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
    uint32_t** chunk_sizes,
    TLongtail_Hash** chunk_hashes,
    uint32_t** chunk_tags,
    uint32_t target_chunk_size,
    uint32_t* chunk_count)
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
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(chunk_tags, "%p"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(chunk_count, "%p")
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
    LONGTAIL_FATAL_ASSERT(ctx, chunk_sizes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_tags != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, target_chunk_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_count != 0, return EINVAL)

    uint32_t asset_count = file_infos->m_Count;

    uint64_t max_hash_size = target_chunk_size * 1024;
    uint32_t job_count = 0;

    uint32_t min_chunk_size;
    int err = chunker_api->GetMinChunkSize(chunker_api, &min_chunk_size);

    uint64_t max_chunk_count = 0;
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = file_infos->m_Sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);
        job_count += (uint32_t)asset_part_count;

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            uint32_t max_count = (uint32_t)(job_size == 0 ? 0 : 1 + (job_size / min_chunk_size));
            max_chunk_count += max_count;
        }
    }

    if (job_count == 0)
    {
        return 0;
    }

    Longtail_JobAPI_Group job_group = 0;
    err = job_api->ReserveJobs(job_api, job_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReserveJobs() failed with %d", err)
        return err;
    }

    size_t work_mem_size = (sizeof(uint32_t) * job_count) +
        (sizeof(TLongtail_Hash) * max_chunk_count) +
        (sizeof(uint32_t) * max_chunk_count) +
        (sizeof(uint32_t) * max_chunk_count) +
        (sizeof(struct HashJob) * job_count) +
        (sizeof(Longtail_JobAPI_JobFunc) * job_count) +
        (sizeof(void*) * job_count);
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint32_t* tmp_job_chunk_counts = (uint32_t*)work_mem;
    TLongtail_Hash* tmp_hashes = (TLongtail_Hash*)&tmp_job_chunk_counts[job_count];
    uint32_t* tmp_sizes = (uint32_t*)&tmp_hashes[max_chunk_count];
    uint32_t* tmp_tags = (uint32_t*)&tmp_sizes[max_chunk_count];
    struct HashJob* tmp_hash_jobs = (struct HashJob*)&tmp_tags[max_chunk_count];
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)&tmp_hash_jobs[job_count];
    void** ctxs = (void**)&funcs[job_count];

    uint64_t jobs_started = 0;
    uint64_t chunks_offset = 0;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = file_infos->m_Sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            LONGTAIL_FATAL_ASSERT(ctx, jobs_started < job_count, return EINVAL)

            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            uint32_t asset_max_chunk_count = (uint32_t)(job_size == 0 ? 0 : 1 + (job_size / min_chunk_size));

            struct HashJob* job = &tmp_hash_jobs[jobs_started];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_ChunkerAPI = chunker_api;
            job->m_RootPath = root_path;
            job->m_Path = &file_infos->m_PathData[file_infos->m_PathStartOffsets[asset_index]];
            job->m_PathHash = (job_part == 0) ? &path_hashes[asset_index] : 0;
            job->m_AssetIndex = asset_index;
            job->m_StartRange = range_start;
            job->m_SizeRange = job_size;
            job->m_ContentTag = optional_asset_tags ? optional_asset_tags[asset_index] : 0;
            job->m_MaxChunkCount = asset_max_chunk_count;
            job->m_AssetChunkCount = &tmp_job_chunk_counts[jobs_started];
            job->m_ChunkHashes = &tmp_hashes[chunks_offset];
            job->m_ChunkSizes = &tmp_sizes[chunks_offset];
            job->m_ChunkTags = &tmp_tags[chunks_offset];
            job->m_TargetChunkSize = target_chunk_size;
            job->m_Err = EINVAL;
            funcs[jobs_started] = DynamicChunking;
            ctxs[jobs_started] = job;
            chunks_offset += asset_max_chunk_count;
            ++jobs_started;
        }
    }
    // TODO: Add logic here if we end up creating more jobs than can be held in a uint32_t
    LONGTAIL_FATAL_ASSERT(ctx, jobs_started < 0xffffffff, return ENOMEM);
    Longtail_JobAPI_Jobs jobs;
    err = job_api->CreateJobs(job_api, job_group, (uint32_t)jobs_started, funcs, ctxs, &jobs);
    LONGTAIL_FATAL_ASSERT(ctx, !err, return err)
    err = job_api->ReadyJobs(job_api, (uint32_t)jobs_started, jobs);
    LONGTAIL_FATAL_ASSERT(ctx, !err, return err)

    err = job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->WaitForAllJobs() failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }

    err = 0;
    for (uint32_t i = 0; i < jobs_started; ++i)
    {
        if (tmp_hash_jobs[i].m_Err)
        {
            LONGTAIL_LOG(ctx, (tmp_hash_jobs[i].m_Err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "tmp_hash_jobs[i].m_Err failed with %d", tmp_hash_jobs[i].m_Err)
            err = err ? err : tmp_hash_jobs[i].m_Err;
        }
    }

    if (!err)
    {
        uint32_t built_chunk_count = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            LONGTAIL_FATAL_ASSERT(ctx, *tmp_hash_jobs[i].m_AssetChunkCount <= tmp_hash_jobs[i].m_MaxChunkCount, return EINVAL)
            built_chunk_count += *tmp_hash_jobs[i].m_AssetChunkCount;
        }
        *chunk_count = built_chunk_count;
        size_t chunk_sizes_size = sizeof(uint32_t) * *chunk_count;
        *chunk_sizes = (uint32_t*)Longtail_Alloc(chunk_sizes_size);
        if (!*chunk_sizes)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_Free(work_mem);
            return ENOMEM;
        }
        size_t chunk_hashes_size = sizeof(TLongtail_Hash) * *chunk_count;
        *chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(chunk_hashes_size);
        if (!*chunk_hashes)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_Free(*chunk_sizes);
            *chunk_sizes = 0;
            Longtail_Free(work_mem);
            return ENOMEM;
        }
        size_t chunk_tags_size = sizeof(uint32_t) * *chunk_count;
        *chunk_tags = (uint32_t*)Longtail_Alloc(chunk_tags_size);
        if (!*chunk_tags)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_Free(*chunk_hashes);
            *chunk_hashes = 0;
            Longtail_Free(*chunk_sizes);
            *chunk_sizes = 0;
            Longtail_Free(work_mem);
            return ENOMEM;
        }

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            uint64_t asset_index = tmp_hash_jobs[i].m_AssetIndex;
            if (tmp_hash_jobs[i].m_StartRange == 0)
            {
                asset_chunk_start_index[asset_index] = chunk_offset;
                asset_chunk_counts[asset_index] = 0;
            }
            uint32_t job_chunk_count = *tmp_hash_jobs[i].m_AssetChunkCount;
            asset_chunk_counts[asset_index] += job_chunk_count;
            for (uint32_t chunk_index = 0; chunk_index < job_chunk_count; ++chunk_index)
            {
                (*chunk_sizes)[chunk_offset] = tmp_hash_jobs[i].m_ChunkSizes[chunk_index];
                (*chunk_hashes)[chunk_offset] = tmp_hash_jobs[i].m_ChunkHashes[chunk_index];
                (*chunk_tags)[chunk_offset] = tmp_hash_jobs[i].m_ChunkTags[chunk_index];
                ++chunk_offset;
            }
        }
        for (uint32_t a = 0; a < asset_count; ++a)
        {
            uint32_t chunk_start_index = asset_chunk_start_index[a];
            uint32_t hash_size = (uint32_t)(sizeof(TLongtail_Hash) * asset_chunk_counts[a]);
            err = hash_api->HashBuffer(hash_api, hash_size, &(*chunk_hashes)[chunk_start_index], &content_hashes[a]);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "hash_api->HashBuffer() failed with %d", err)
                Longtail_Free(*chunk_tags);
                *chunk_tags = 0;
                Longtail_Free(*chunk_hashes);
                *chunk_hashes = 0;
                Longtail_Free(*chunk_sizes);
                *chunk_sizes = 0;
                Longtail_Free(work_mem);
                return err;
            }
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

    size_t version_index_data_start = (size_t)(uintptr_t)p;

    version_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*version_index->m_Version) != LONGTAIL_VERSION_INDEX_VERSION_0_0_2)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Missmatching versions in version index data %" PRIu64 " != %" PRIu64 "", (void*)version_index->m_Version, Longtail_CurrentVersionIndexVersion);
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

    size_t versiom_index_data_size = Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, 0);
    if (versiom_index_data_size > data_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Version index data is truncated: %" PRIu64 " <= %" PRIu64, data_size, versiom_index_data_size)
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
    LONGTAIL_VALIDATE_INPUT(ctx, file_infos != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || path_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || content_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_counts == 0 || asset_chunk_index_starts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, file_infos->m_Count == 0 || asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, asset_chunk_index_count >= chunk_count, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || asset_chunk_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    uint32_t asset_count = file_infos->m_Count;
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || target_chunk_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (file_infos == 0 || file_infos->m_Count == 0) || out_version_index != 0, return EINVAL)

    uint32_t path_count = file_infos->m_Count;

    if (path_count == 0)
    {
        size_t version_index_size = Longtail_GetVersionIndexSize(path_count, 0, 0, 0);
        void* version_index_mem = Longtail_Alloc(version_index_size);
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
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    TLongtail_Hash* tmp_path_hashes = (TLongtail_Hash*)work_mem;
    TLongtail_Hash* tmp_content_hashes = (TLongtail_Hash*)&tmp_path_hashes[path_count];
    uint32_t* tmp_asset_chunk_counts = (uint32_t*)&tmp_content_hashes[path_count];
    uint32_t* tmp_asset_chunk_start_index = (uint32_t*)&tmp_asset_chunk_counts[path_count];

    uint32_t assets_chunk_index_count = 0;
    uint32_t* asset_chunk_sizes = 0;
    uint32_t* asset_chunk_tags = 0;
    TLongtail_Hash* asset_chunk_hashes = 0;

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
        &asset_chunk_sizes,
        &asset_chunk_hashes,
        &asset_chunk_tags,
        target_chunk_size,
        &assets_chunk_index_count);
    if (err)
    {
        LONGTAIL_LOG(ctx, (err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets() failed with %d", err)
        Longtail_Free(work_mem);
        return err;
    }

    size_t work_mem_compact_size = (sizeof(uint32_t) * assets_chunk_index_count) +
        (sizeof(TLongtail_Hash) * assets_chunk_index_count) + 
        (sizeof(uint32_t) * assets_chunk_index_count) +
        (sizeof(uint32_t) * assets_chunk_index_count) +
        Longtail_LookupTable_GetSize(assets_chunk_index_count);
    void* work_mem_compact = Longtail_Alloc(work_mem_compact_size);
    if (!work_mem_compact)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(work_mem);
        return ENOMEM;
    }

    uint32_t* tmp_asset_chunk_indexes = (uint32_t*)work_mem_compact;
    TLongtail_Hash* tmp_compact_chunk_hashes = (TLongtail_Hash*)&tmp_asset_chunk_indexes[assets_chunk_index_count];
    uint32_t* tmp_compact_chunk_sizes =  (uint32_t*)&tmp_compact_chunk_hashes[assets_chunk_index_count];
    uint32_t* tmp_compact_chunk_tags =  (uint32_t*)&tmp_compact_chunk_sizes[assets_chunk_index_count];

    uint32_t unique_chunk_count = 0;
    struct Longtail_LookupTable* chunk_hash_to_index = Longtail_LookupTable_Create(&tmp_compact_chunk_tags[assets_chunk_index_count], assets_chunk_index_count, 0);

    for (uint32_t c = 0; c < assets_chunk_index_count; ++c)
    {
        TLongtail_Hash h = asset_chunk_hashes[c];
        uint64_t* chunk_index = Longtail_LookupTable_PutUnique(chunk_hash_to_index, h, unique_chunk_count);
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
            tmp_asset_chunk_indexes[c] = (uint32_t)*chunk_index;
        }
    }

    size_t version_index_size = Longtail_GetVersionIndexSize(path_count, unique_chunk_count, assets_chunk_index_count, file_infos->m_PathDataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);
    if (!version_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(work_mem_compact);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
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
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(work_mem);
        return err;
    }

    Longtail_Free(work_mem_compact);
    Longtail_Free(asset_chunk_tags);
    Longtail_Free(asset_chunk_hashes);
    Longtail_Free(asset_chunk_sizes);
    Longtail_Free(work_mem);

    *out_version_index = version_index;
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, &version_index[1], index_data_size);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
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
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    size_t version_index_size = sizeof(struct Longtail_VersionIndex) + size;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)
    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) failed with %d",
            storage_api, path, out_version_index,
            err)
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
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
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
    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    LONGTAIL_VALIDATE_INPUT(ctx, block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Chunk count results in block index data %" PRIu64 " larger that data size (%" PRIu64 ")", block_index_data_size, data)
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
    const uint64_t* chunk_indexes,
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
    void* mem = Longtail_Alloc(block_index_size);
    if (mem == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(mem, chunk_count);
    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        uint64_t chunk_index = chunk_indexes[i];
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
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, &block_index[1], index_data_size);
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
    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)Longtail_Alloc(block_index_size);
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
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &block_index[1]);
    if (err){
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);

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
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile) failed with %d", err)
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
    void* block_index_mem = Longtail_Alloc(block_index_size);
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
    *out_block_index = block_index;
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
    stored_block->m_BlockData = &((uint8_t*)stored_block->m_BlockIndex)[Longtail_GetBlockIndexSize(*stored_block->m_BlockIndex->m_ChunkCount)];
    stored_block->m_BlockChunksDataSize = (uint32_t)(block_data_size - Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount));
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
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(stored_block_size);
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

    size_t size = block_index_data_size + stored_block->m_BlockChunksDataSize;

    void* mem = (uint8_t*)Longtail_Alloc(size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    uint8_t* write_ptr = (uint8_t*)mem;

    memcpy(write_ptr, &stored_block->m_BlockIndex[1], block_index_data_size);
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
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
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
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    err = storage_api->Write(storage_api, block_file_handle, write_offset, block_index_data_size, &stored_block->m_BlockIndex[1]);
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
    return 0;
}

int Longtail_ReadStoredBlock(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_StoredBlock** out_stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
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
    return 0;
}



size_t Longtail_GetContentIndexDataSize(uint64_t block_count, uint64_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, block_count <= chunk_count, return 0)

    size_t block_index_data_size = (size_t)(
        sizeof(uint32_t) +                          // m_Version
        sizeof(uint32_t) +                          // m_HashIdentifier
        sizeof(uint32_t) +                          // m_MaxBlockSize
        sizeof(uint32_t) +                          // m_MaxChunksPerBlock
        sizeof(uint64_t) +                          // m_BlockCount
        sizeof(uint64_t) +                          // m_ChunkCount
        (sizeof(TLongtail_Hash) * block_count) +    // m_BlockHashes[]
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes[]
        (sizeof(uint64_t) * chunk_count)            // m_ChunkBlockIndexes[]
        );

    return block_index_data_size;
}

size_t Longtail_GetContentIndexSize(uint64_t block_count, uint64_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, block_count <= chunk_count, return 0)
    
    return sizeof(struct Longtail_ContentIndex) +
        Longtail_GetContentIndexDataSize(block_count, chunk_count);
}

int Longtail_InitContentIndexFromData(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(data_size, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data_size >= sizeof(uint32_t), return EINVAL)

    char* p = (char*)data;
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if (((*content_index->m_Version) != LONGTAIL_CONTENT_INDEX_VERSION_0_0_1) &&
        ((*content_index->m_Version) != LONGTAIL_CONTENT_INDEX_VERSION_1_0_0))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Unsupported version of content data %" PRIu64 "", (void*)content_index->m_Version);
        return EBADF;
    }

    content_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);
    content_index->m_MaxBlockSize = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);
    content_index->m_MaxChunksPerBlock = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);
    content_index->m_BlockCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);
    content_index->m_ChunkCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    size_t content_index_data_size = Longtail_GetContentIndexDataSize(block_count, chunk_count);
    if (content_index_data_size > data_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Content index data is truncated: %" PRIu64 " <= %" PRIu64, data_size, content_index_data_size)
        return EBADF;
    }

    content_index->m_BlockHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);
    content_index->m_ChunkBlockIndexes = (uint64_t*)(void*)p;

    return 0;
}

struct Longtail_ContentIndex* Longtail_InitContentIndex(
    void* mem,
    uint32_t hash_api,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    uint64_t chunk_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%u"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(block_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)

    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)mem;

    uint8_t* p = (uint8_t*)&content_index[1];
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_HashIdentifier = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_MaxBlockSize = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_MaxChunksPerBlock = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_BlockCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    content_index->m_ChunkCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    content_index->m_BlockHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);
    content_index->m_ChunkBlockIndexes = (uint64_t*)(void*)p;

    *content_index->m_Version = Longtail_CurrentContentIndexVersion;
    *content_index->m_HashIdentifier = hash_api;
    *content_index->m_MaxBlockSize = max_block_size;
    *content_index->m_MaxChunksPerBlock = max_chunks_per_block;
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;

    return content_index;
}

static uint64_t GetUniqueHashes(
    uint64_t hash_count,
    const TLongtail_Hash* hashes,
    uint64_t* out_unique_hash_indexes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(hashes, "%p"),
        LONGTAIL_LOGFIELD(out_unique_hash_indexes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, hash_count != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, hash_count == 0 || out_unique_hash_indexes != 0, return 0)

    struct Longtail_LookupTable* lookup_table = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(hash_count)), hash_count, 0);
    if (!lookup_table)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    uint64_t unique_hash_count = 0;
    for (uint64_t i = 0; i < hash_count; ++i)
    {
        TLongtail_Hash hash = hashes[i];
        uint64_t* lookup_index = Longtail_LookupTable_PutUnique(lookup_table, hash, unique_hash_count);
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

int Longtail_CreateContentIndexFromBlocks(
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    struct Longtail_BlockIndex** block_indexes,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(block_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(block_indexes, "%p"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, block_count == 0 || block_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    uint32_t hash_identifier = 0;
    uint64_t chunk_count = 0;
    for (uint64_t b = 0; b < block_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = block_indexes[b];
        chunk_count += *block_index->m_ChunkCount;
        uint32_t block_hash_identifier = *block_index->m_HashIdentifier;
        if (hash_identifier == 0)
        {
            hash_identifier = block_hash_identifier;
        }
        else if (hash_identifier != block_hash_identifier)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Block hash identifier are inconsistent for block 0x%" PRIx64 ", %u != %u", *block_index->m_BlockHash, hash_identifier, block_hash_identifier)
            return EINVAL;
        }
    }

    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    void* content_index_mem = Longtail_Alloc(content_index_size);
    if (!content_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_ContentIndex* content_index = Longtail_InitContentIndex(
        content_index_mem,
        hash_identifier,
        max_block_size,
        max_chunks_per_block,
        block_count,
        chunk_count);
    LONGTAIL_FATAL_ASSERT(ctx, content_index != 0, return EINVAL)

    uint64_t asset_index = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = block_indexes[b];
        content_index->m_BlockHashes[b] = *block_index->m_BlockHash;
        uint32_t chunk_offset = 0;
        uint32_t block_chunk_count = *block_index->m_ChunkCount;
        for (uint32_t a = 0; a < block_chunk_count; ++a)
        {
            content_index->m_ChunkHashes[asset_index] = block_index->m_ChunkHashes[a];
            content_index->m_ChunkBlockIndexes[asset_index] = b;

            chunk_offset += block_index->m_ChunkSizes[a];
            ++asset_index;
            if (asset_index > chunk_count)
            {
                break;
            }
        }
    }

    *out_content_index = content_index;
    return 0;
}

int Longtail_CreateContentIndexRaw(
    struct Longtail_HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* optional_chunk_tags,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(chunk_sizes, "%p"),
        LONGTAIL_LOGFIELD(optional_chunk_tags, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunk_count == 0 || max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    if (chunk_count == 0)
    {
        size_t content_index_size = Longtail_GetContentIndexSize(0, 0);
        void* content_index_mem = Longtail_Alloc(content_index_size);
        if (!content_index_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }
        struct Longtail_ContentIndex* content_index = Longtail_InitContentIndex(
            content_index_mem,
            hash_api ? hash_api->GetIdentifier(hash_api) : 0u,
            max_block_size,
            max_chunks_per_block,
            0,
            0);
        LONGTAIL_FATAL_ASSERT(ctx, content_index != 0, return EINVAL)
        *out_content_index = content_index;
        return 0;
    }

    size_t work_mem_size = (sizeof(uint64_t) * chunk_count) +
        (sizeof(struct Longtail_BlockIndex*) * chunk_count) +
        (sizeof(uint64_t) * max_chunks_per_block);
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    uint64_t* tmp_chunk_indexes = (uint64_t*)work_mem;
    struct Longtail_BlockIndex** tmp_block_indexes = (struct Longtail_BlockIndex**)&tmp_chunk_indexes[chunk_count];
    uint64_t* tmp_stored_chunk_indexes = (uint64_t*)&tmp_block_indexes[chunk_count];
    uint64_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, tmp_chunk_indexes);

    uint64_t i = 0;
    uint32_t block_count = 0;

    while (i < unique_chunk_count)
    {
        uint32_t chunk_count_in_block = 0;

        uint64_t chunk_index = tmp_chunk_indexes[i];

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

    int err = Longtail_CreateContentIndexFromBlocks(
        max_block_size,
        max_chunks_per_block,
        block_count,
        tmp_block_indexes,
        out_content_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexFromBlocks() failed with %d", err)
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

int Longtail_CreateContentIndex(
    struct Longtail_HashAPI* hash_api,
    struct Longtail_VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)
    int err = Longtail_CreateContentIndexRaw(
        hash_api,
        version_index ? *version_index->m_ChunkCount : 0,
        version_index ? version_index->m_ChunkHashes : 0,
        version_index ? version_index->m_ChunkSizes : 0,
        version_index ? version_index->m_ChunkTags : 0,
        max_block_size,
        max_chunks_per_block,
        out_content_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw() failed with %d", err)
        return err;
    }
    return 0;
}

int Longtail_GetRequiredChunkHashes(
    const struct Longtail_VersionIndex* version_index,
    const struct Longtail_VersionDiff* version_diff,
    uint64_t* out_chunk_count,
    TLongtail_Hash* out_chunk_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_count, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (*version_index->m_ChunkCount == 0) ||  out_chunk_hashes != 0, return EINVAL)

    uint32_t max_chunk_count = *version_index->m_ChunkCount;
    void* work_mem = Longtail_Alloc(Longtail_LookupTable_GetSize(max_chunk_count));
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    struct Longtail_LookupTable* chunk_lookup = Longtail_LookupTable_Create(work_mem, max_chunk_count, 0);

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
            if (0 == Longtail_LookupTable_PutUnique(chunk_lookup, chunk_hash, chunk_count))
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
            if (0 == Longtail_LookupTable_PutUnique(chunk_lookup, chunk_hash, chunk_count))
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

int Longtail_CreateContentIndexFromStoreIndex(
    const struct Longtail_StoreIndex* store_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    uint32_t max_block_count = *store_index->m_BlockCount;

    size_t block_indexes_size = sizeof(struct Longtail_BlockIndex) * max_block_count;
    size_t block_index_ptrs_size = sizeof(struct Longtail_BlockIndex*) * max_block_count;
    size_t work_mem_size = block_indexes_size + block_index_ptrs_size;
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;

    struct Longtail_BlockIndex* block_indexes = (struct Longtail_BlockIndex*)p;
    p += block_indexes_size;
    struct Longtail_BlockIndex** block_index_ptrs = (struct Longtail_BlockIndex**)p;
    for (uint32_t b = 0; b < max_block_count; ++b)
    {
        int err = Longtail_MakeBlockIndex(
            store_index,
            b,
            &block_indexes[b]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MakeBlockIndex() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }
        block_index_ptrs[b] = &block_indexes[b];
    }

    int err = Longtail_CreateContentIndexFromBlocks(
        max_block_size,
        max_chunks_per_block,
        max_block_count,
        block_index_ptrs,
        out_content_index);
    Longtail_Free(work_mem);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexFromBlocks() failed with %d", err)
        return err;
    }
    return 0;
}

int Longtail_WriteContentIndexToBuffer(
    const struct Longtail_ContentIndex* content_index,
    void** out_buffer,
    size_t* out_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(out_buffer, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, &content_index[1], index_data_size);
    *out_size = index_data_size;
    return 0;
}

int Longtail_ReadContentIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    size_t content_index_size = size + sizeof(struct Longtail_ContentIndex);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!content_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndexFromBuffer(%p, %" PRIu64 ", %p) failed with %d",
            buffer, size, out_content_index,
            ENOMEM)
        return ENOMEM;
    }
    memcpy(&content_index[1], buffer, size);
    int err = Longtail_InitContentIndexFromData(content_index, &content_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitContentIndexFromData() failed with %d", err)
        Longtail_Free(content_index);
        return err;
    }
    *out_content_index = content_index;
    return 0;
}

int Longtail_WriteContentIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_ContentIndex* content_index,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenWriteFile() failed with %d", err)
        return err;
    }
    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &content_index[1]);
    if (err){
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }

    storage_api->CloseFile(storage_api, file_handle);

    return 0;
}

int Longtail_ReadContentIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ENOENT ? LONGTAIL_LOG_LEVEL_WARNING : LONGTAIL_LOG_LEVEL_ERROR, "storage_api->OpenReadFile() failed with %d", err)
        return err;
    }
    uint64_t content_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &content_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->GetSize() failed with %d", err)
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    uint64_t content_index_size = sizeof(struct Longtail_ContentIndex) + content_index_data_size;
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc((size_t)(content_index_size));
    if (!content_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(content_index);
        storage_api->CloseFile(storage_api, file_handle);
        return ENOMEM;
    }
    err = storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
        Longtail_Free(content_index);
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = Longtail_InitContentIndexFromData(content_index, &content_index[1], content_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitContentIndexFromData() failed with %d", err)
        Longtail_Free(content_index);
        return err;
    }
    *out_content_index = content_index;
    return 0;
}

struct ChunkAssetPartReference
{
    const char* m_AssetPath;
    uint32_t m_ChunkIndex;
    uint64_t m_AssetOffset;
    uint32_t m_Tag;
};

struct AssetPartLookup
{
    struct ChunkAssetPartReference* m_ChunkAssetPartReferences;
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
        Longtail_LookupTable_GetSize(asset_chunk_index_count) +
        sizeof(struct ChunkAssetPartReference) * asset_chunk_index_count;
    struct AssetPartLookup* asset_part_lookup = (struct AssetPartLookup*)Longtail_Alloc(asset_part_lookup_size);
    if (!asset_part_lookup_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    asset_part_lookup->m_ChunkAssetPartReferences = (struct ChunkAssetPartReference*)&asset_part_lookup[1];
    asset_part_lookup->m_ChunkHashToIndex = Longtail_LookupTable_Create(&asset_part_lookup->m_ChunkAssetPartReferences[asset_chunk_index_count], asset_chunk_index_count, 0);

    uint32_t unique_chunk_count = 0;
    uint32_t asset_count = *version_index->m_AssetCount;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint64_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
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
            if (0 == Longtail_LookupTable_PutUnique(asset_part_lookup->m_ChunkHashToIndex, chunk_hash, unique_chunk_count))
            {
                struct ChunkAssetPartReference chunk_asset_part_reference =
                {
                    path,
                    unique_chunk_count,
                    asset_chunk_offset,
                    tag
                };
                asset_part_lookup->m_ChunkAssetPartReferences[unique_chunk_count++] = chunk_asset_part_reference;
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
    TLongtail_Hash m_BlockHash;
    const struct Longtail_ContentIndex* m_ContentIndex;
    const uint32_t* m_ChunkSizes;
    struct AssetPartLookup* m_AssetPartLookup;
    uint64_t m_FirstChunkIndex;
    uint32_t m_ChunkCount;
    int m_Err;
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockWriterJobOnComplete(%p, %d)",
            async_complete_api, err)
    }
    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api != 0, return)
    struct WriteBlockJob* job = (struct WriteBlockJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_AsyncCompleteAPI.OnComplete != 0, return);
    LONGTAIL_FATAL_ASSERT(ctx, job->m_StoredBlock != 0, return);
    LONGTAIL_FATAL_ASSERT(ctx, job->m_JobID != 0, return);
    uint32_t job_id = job->m_JobID;
    job->m_StoredBlock->Dispose(job->m_StoredBlock);
    job->m_StoredBlock = 0;
    job->m_JobID = 0;
    job->m_Err = err;
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

static int WriteContentBlockJob(void* context, uint32_t job_id, int is_cancelled)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct WriteBlockJob* job = (struct WriteBlockJob*)context;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_JobID == 0, return EINVAL);

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        return 0;
    }

    if (is_cancelled)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteContentBlock was cancelled, failed with %d", ECANCELED)
        job->m_Err = ECANCELED;
        return 0;
    }

    struct Longtail_StorageAPI* source_storage_api = job->m_SourceStorageAPI;
    struct Longtail_BlockStoreAPI* block_store_api = job->m_BlockStoreAPI;

    const struct Longtail_ContentIndex* content_index = job->m_ContentIndex;
    uint64_t first_chunk_index = job->m_FirstChunkIndex;
    uint32_t chunk_count = job->m_ChunkCount;
    uint64_t block_index = content_index->m_ChunkBlockIndexes[first_chunk_index];
    TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];

    uint32_t block_data_size = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        LONGTAIL_FATAL_ASSERT(ctx, content_index->m_ChunkBlockIndexes[chunk_index] == block_index, job->m_Err = EINVAL; return 0)
        uint32_t chunk_size = job->m_ChunkSizes[chunk_index];
        block_data_size += chunk_size;
    }

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t stored_block_size = sizeof(struct Longtail_StoredBlock);
    size_t put_block_mem_size =
        stored_block_size +
        block_index_size +
        block_data_size;

    void* put_block_mem = Longtail_Alloc(put_block_mem_size);
    if (!put_block_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM);
        job->m_Err = ENOMEM;
        return 0;
    }

    char* p = (char*)put_block_mem;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)p;
    p += stored_block_size;
    struct Longtail_BlockIndex* block_index_ptr = (struct Longtail_BlockIndex*)p;
    p += block_index_size;
    char* block_data_buffer = p;

    char* write_buffer = block_data_buffer;
    char* write_ptr = write_buffer;

    const char* asset_path = 0;
    uint32_t tag = 0;

    Longtail_StorageAPI_HOpenFile file_handle = 0;
    uint64_t asset_file_size = 0;

    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = job->m_ChunkSizes[chunk_index];
        uint64_t* asset_part_index = Longtail_LookupTable_Get(job->m_AssetPartLookup->m_ChunkHashToIndex, chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, asset_part_index != 0, job->m_Err = EINVAL; return 0)
        const struct ChunkAssetPartReference* next_asset_part = &job->m_AssetPartLookup->m_ChunkAssetPartReferences[*asset_part_index];

        if (asset_path && tag != next_asset_part->m_Tag)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "WriteContentBlockJob(%p, %u, %d): Warning: Inconsistent tag type for chunks inside block 0x%" PRIx64 ", retaining 0x%" PRIx64 "",
                context, job_id, is_cancelled,
                block_hash, tag)
        }
        else
        {
            tag = next_asset_part->m_Tag;
        }

        if (next_asset_part->m_AssetPath != asset_path)
        {
            if (file_handle)
            {
                source_storage_api->CloseFile(source_storage_api, file_handle);
                file_handle = 0;
            }
            LONGTAIL_FATAL_ASSERT(ctx, !IsDirPath(next_asset_part->m_AssetPath), job->m_Err = EINVAL; return 0)

            char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, next_asset_part->m_AssetPath);
            int err = source_storage_api->OpenReadFile(source_storage_api, full_path, &file_handle);
            Longtail_Free(full_path);
            full_path = 0;
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->OpenReadFile() failed with %d", err);
                Longtail_Free(put_block_mem);
                job->m_Err = err;
                return 0;
            }
            uint64_t next_asset_file_size = 0;
            err = source_storage_api->GetSize(source_storage_api, file_handle, &next_asset_file_size);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->GetSize() failed with %d", err);
                Longtail_Free(put_block_mem);
                job->m_Err = err;
                return 0;
            }
            asset_file_size = next_asset_file_size;
            asset_path = next_asset_part->m_AssetPath;
        }

        uint64_t asset_offset = next_asset_part->m_AssetOffset;
        if (asset_file_size < (asset_offset + chunk_size))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Source asset file does not match indexed size %" PRIu64 " < %" PRIu64,
                asset_file_size, (asset_offset + chunk_size))
            Longtail_Free(put_block_mem);
            source_storage_api->CloseFile(source_storage_api, file_handle);
            job->m_Err = EBADF;
            return 0;
        }
        int err = source_storage_api->Read(source_storage_api, file_handle, asset_offset, chunk_size, write_ptr);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "source_storage_api->Read() failed with %d", err);
            Longtail_Free(put_block_mem);
            source_storage_api->CloseFile(source_storage_api, file_handle);
            job->m_Err = err;
            return 0;
        }
        write_ptr += chunk_size;
    }

    if (file_handle)
    {
        source_storage_api->CloseFile(source_storage_api, file_handle);
        file_handle = 0;
    }

    Longtail_InitBlockIndex(block_index_ptr, chunk_count);
    memmove(block_index_ptr->m_ChunkHashes, &content_index->m_ChunkHashes[first_chunk_index], sizeof(TLongtail_Hash) * chunk_count);
    memmove(block_index_ptr->m_ChunkSizes, &job->m_ChunkSizes[first_chunk_index], sizeof(uint32_t) * chunk_count);
    *block_index_ptr->m_BlockHash = block_hash;
    *block_index_ptr->m_HashIdentifier = *content_index->m_HashIdentifier;
    *block_index_ptr->m_Tag = tag;
    *block_index_ptr->m_ChunkCount = chunk_count;
    job->m_StoredBlock = stored_block;
    job->m_StoredBlock->Dispose = DisposePutBlock;
    job->m_StoredBlock->m_BlockIndex = block_index_ptr;
    job->m_StoredBlock->m_BlockData = block_data_buffer;
    job->m_StoredBlock->m_BlockChunksDataSize = block_data_size;

    job->m_JobID = job_id;
    job->m_AsyncCompleteAPI.OnComplete = BlockWriterJobOnComplete;

    int err = block_store_api->PutStoredBlock(block_store_api, job->m_StoredBlock, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "block_store_api->PutStoredBlock() failed with %d", err);
        job->m_StoredBlock->Dispose(job->m_StoredBlock);
        job->m_StoredBlock = 0;
        job->m_JobID = 0;
        job->m_Err = err;
        return 0;
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
    struct Longtail_ContentIndex* content_index,
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
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(assets_folder, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, source_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, assets_folder != 0, return EINVAL)

    uint64_t block_count = *content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 0;
    }

    uint32_t version_chunk_count = *version_index->m_ChunkCount;
    uint64_t version_content_index_chunk_count = *content_index->m_ChunkCount;

    size_t chunk_lookup_size = Longtail_LookupTable_GetSize(version_chunk_count);
    size_t chunk_sizes_size = sizeof(uint32_t) * version_content_index_chunk_count;
    size_t write_block_jobs_size = sizeof(struct WriteBlockJob) * block_count;
    size_t funcs_size = sizeof(Longtail_JobAPI_JobFunc*) * block_count;
    size_t ctxs_size = sizeof(void*) * block_count;

    size_t work_mem_size =
        chunk_lookup_size +
        chunk_sizes_size +
        write_block_jobs_size +
        funcs_size +
        ctxs_size;

    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;

    struct Longtail_LookupTable* chunk_lookup = Longtail_LookupTable_Create(p, version_chunk_count, 0);
    p += chunk_lookup_size;
    uint32_t* chunk_sizes = (uint32_t*)p;
    p += chunk_sizes_size;
    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)p;
    p += write_block_jobs_size;
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)p;
    p += funcs_size;
    void** ctxs = (void**)p;

    for (uint32_t c = 0; c < version_chunk_count; ++c)
    {
        Longtail_LookupTable_Put(chunk_lookup, version_index->m_ChunkHashes[c], c);
    }

    for (uint64_t c = 0; c < version_content_index_chunk_count; ++c)
    {
        uint64_t* version_chunk_index = Longtail_LookupTable_Get(chunk_lookup, content_index->m_ChunkHashes[c]);
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

    uint32_t block_start_chunk_index = 0;
    uint32_t job_count = 0;
    for (uint64_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        LONGTAIL_FATAL_ASSERT(ctx, content_index->m_ChunkBlockIndexes[block_start_chunk_index] == block_index, return EINVAL);
        uint64_t chunk_offset_index = block_start_chunk_index + 1;
        while((chunk_offset_index < version_content_index_chunk_count) && content_index->m_ChunkBlockIndexes[chunk_offset_index] == block_index)
        {
            ++chunk_offset_index;
        }
        uint32_t block_chunk_count = (uint32_t)(chunk_offset_index - block_start_chunk_index);

        struct WriteBlockJob* job = &write_block_jobs[job_count];
        job->m_AsyncCompleteAPI.m_API.Dispose = 0;
        job->m_AsyncCompleteAPI.OnComplete = 0;
        job->m_SourceStorageAPI = source_storage_api;
        job->m_BlockStoreAPI = block_store_api;
        job->m_JobAPI = job_api;
        job->m_JobID = 0;
        job->m_StoredBlock = 0;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = content_index;
        job->m_BlockHash = block_hash;
        job->m_AssetPartLookup = asset_part_lookup;
        job->m_FirstChunkIndex = block_start_chunk_index;
        job->m_ChunkCount = block_chunk_count;
        job->m_ChunkSizes = chunk_sizes;
        job->m_Err = EINVAL;

        funcs[job_count] = WriteContentBlockJob;
        ctxs[job_count] = job;

        ++job_count;
        block_start_chunk_index += block_chunk_count;
    }

    if (job_count == 0)
    {
        Longtail_Free(asset_part_lookup);
        Longtail_Free(work_mem);
        return 0;
    }

    Longtail_JobAPI_Group job_group = 0;
    err = job_api->ReserveJobs(job_api, (uint32_t)job_count, &job_group);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->ReserveJobs() failed with %d", err)
        Longtail_Free(asset_part_lookup);
        Longtail_Free(work_mem);
        return err;
    }

    Longtail_JobAPI_Jobs jobs;
    err = job_api->CreateJobs(job_api, job_group, job_count, funcs, ctxs, &jobs);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    err = job_api->ReadyJobs(job_api, job_count, jobs);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)

    err = job_api->WaitForAllJobs(job_api, job_group, progress_api, optional_cancel_api, optional_cancel_token);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job_api->WaitForAllJobs() failed with %d", err)
        Longtail_Free(asset_part_lookup);
        Longtail_Free(work_mem);
        return err;
    }

    err = 0;
    while (job_count--)
    {
        struct WriteBlockJob* job = &write_block_jobs[job_count];
        if (job->m_Err)
        {
            err = err ? err : job->m_Err;
        }
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
    int m_Err;
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
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockReaderJobOnComplete(%p, %p, %d)",
            async_complete_api, stored_block, err)
    }
    LONGTAIL_FATAL_ASSERT(ctx, async_complete_api != 0, return)
    struct BlockReaderJob* job = (struct BlockReaderJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(ctx, job->m_AsyncCompleteAPI.OnComplete != 0, return);
    job->m_Err = err;
    job->m_StoredBlock = stored_block;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job->m_JobID);
}

static int BlockReader(void* context, uint32_t job_id, int is_cancelled)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return EINVAL)

    struct BlockReaderJob* job = (struct BlockReaderJob*)context;

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        return 0;
    }

    if (is_cancelled)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "BlockReader was cancelled, failed with %d", ECANCELED)
        job->m_Err = ECANCELED;
        return 0;
    }

    job->m_JobID = job_id;
    job->m_StoredBlock = 0;
    job->m_AsyncCompleteAPI.OnComplete = BlockReaderJobOnComplete;
    
    int err = job->m_BlockStoreAPI->GetStoredBlock(job->m_BlockStoreAPI, job->m_BlockHash, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(ctx, err == ECANCELED ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "job->m_BlockStoreAPI->GetStoredBlock() failed with %d", err)
        job->m_Err = err;
        return 0;
    }
    return EBUSY;
}

static int WriteReady(void* context, uint32_t job_id, int is_cancelled)
{
    // Nothing to do here, we are just a syncronization point
    return 0;
}

#define MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE  64u

struct WritePartialAssetFromBlocksJob
{
    struct Longtail_StorageAPI* m_VersionStorageAPI;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    const struct Longtail_ContentIndex* m_ContentIndex;
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

    int m_Err;
};

int WritePartialAssetFromBlocks(void* context, uint32_t job_id, int is_cancelled);

// Returns the write sync task, or the write task if there is no need for reading new blocks
static int CreatePartialAssetWriteJob(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    const struct Longtail_ContentIndex* content_index,
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
        LONGTAIL_LOGFIELD(content_index, "%p"),
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
    LONGTAIL_FATAL_ASSERT(ctx, content_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_folder !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hash_to_block_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, asset_index < *version_index->m_AssetCount, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, out_jobs !=0, return EINVAL)

    job->m_VersionStorageAPI = version_storage_api;
    job->m_BlockStoreAPI = block_store_api;
    job->m_JobAPI = job_api;
    job->m_ContentIndex = content_index;
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
    job->m_Err = EINVAL;

    uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
    uint32_t chunk_start_index_offset = chunk_index_start + asset_chunk_index_offset;
    uint32_t chunk_index_end = chunk_index_start + version_index->m_AssetChunkCounts[asset_index];
    uint32_t chunk_index_offset = chunk_start_index_offset;

    Longtail_JobAPI_JobFunc block_read_funcs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    void* block_read_ctx[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];

    const uint32_t worker_count = job_api->GetWorkerCount(job_api) + 1;
    const uint32_t max_parallell_block_read_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;

    while (chunk_index_offset != chunk_index_end && job->m_BlockReaderJobCount <= max_parallell_block_read_jobs)
    {
        uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        const uint64_t* block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
        uint64_t block_index = *block_index_ptr;
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
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
            block_job->m_Err = EINVAL;
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
    int err = job_api->CreateJobs(job_api, job_group, 1, write_funcs, write_ctx, &write_job);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "job_api->CreateJobs() failed with %d", err)
        return err;
    }

    if (job->m_BlockReaderJobCount > 0)
    {
        Longtail_JobAPI_Jobs block_read_jobs;
        err = job_api->CreateJobs(job_api, job_group, job->m_BlockReaderJobCount, block_read_funcs, block_read_ctx, &block_read_jobs);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
        Longtail_JobAPI_JobFunc sync_write_funcs[1] = { WriteReady };
        void* sync_write_ctx[1] = { 0 };
        Longtail_JobAPI_Jobs write_sync_job;
        err = job_api->CreateJobs(job_api, job_group, 1, sync_write_funcs, sync_write_ctx, &write_sync_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)

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

int WritePartialAssetFromBlocks(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context !=0, return EINVAL)
    struct WritePartialAssetFromBlocksJob* job = (struct WritePartialAssetFromBlocksJob*)context;

    uint32_t block_reader_job_count = job->m_BlockReaderJobCount;

    if ((!job->m_AssetOutputFile) && job->m_AssetChunkIndexOffset)
    {
        LONGTAIL_FATAL_ASSERT(ctx, job->m_Err != 0, return 0);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "WritePartialAssetFromBlocks(%p, %u, %d) failed due to previous error",
            context, job_id, is_cancelled)
        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            struct Longtail_StoredBlock* stored_block = job->m_BlockReaderJobs[d].m_StoredBlock;
            if (stored_block && stored_block->Dispose)
            {
                stored_block->Dispose(stored_block);
            }
            job->m_BlockReaderJobs[d].m_StoredBlock = 0;
        }
        return 0;
    }

    if (is_cancelled)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WritePartialAssetFromBlocks was cancelled, failed with %d", ECANCELED)

        for (uint32_t d = 0; d < block_reader_job_count; ++d)
        {
            struct Longtail_StoredBlock* stored_block = job->m_BlockReaderJobs[d].m_StoredBlock;
            if (stored_block && stored_block->Dispose)
            {
                stored_block->Dispose(stored_block);
            }
            job->m_BlockReaderJobs[d].m_StoredBlock = 0;
        }
        if (job->m_AssetOutputFile)
        {
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;
        }
        job->m_Err = ECANCELED;
        return 0;
    }

    // Need to fetch all the data we need from the context since we will reuse the context
    int block_reader_errors = 0;
    TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    struct Longtail_StoredBlock* stored_block[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    for (uint32_t d = 0; d < block_reader_job_count; ++d)
    {
        if (job->m_BlockReaderJobs[d].m_Err)
        {
            block_reader_errors = block_reader_errors == 0 ? job->m_BlockReaderJobs[d].m_Err : block_reader_errors;
            block_hashes[d] = 0;
            stored_block[d] = 0;
            continue;
        }
        block_hashes[d] = job->m_BlockReaderJobs[d].m_BlockHash;
        stored_block[d] = job->m_BlockReaderJobs[d].m_StoredBlock;
    }

    if (block_reader_errors)
    {
        LONGTAIL_LOG(ctx, (block_reader_errors == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "BlockRead()) failed with %d", block_reader_errors)
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
        job->m_Err = block_reader_errors;
        return 0;
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
            job->m_Err = err;
            return 0;
        }
        if (IsDirPath(full_asset_path))
        {
            LONGTAIL_FATAL_ASSERT(ctx, block_reader_job_count == 0, job->m_Err = EINVAL; return 0)
            // Remove trailing forward slash
            full_asset_path[strlen(full_asset_path) - 1] = '\0';
            err = SafeCreateDir(job->m_VersionStorageAPI, full_asset_path);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "SafeCreateDir() failed with %d", err)
                Longtail_Free(full_asset_path);
                job->m_Err = err;
                return 0;
            }
            Longtail_Free(full_asset_path);
            job->m_Err = 0;
            return 0;
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
            job->m_Err = err;
            return 0;
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
                    job->m_Err = err;
                    return 0;
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
            job->m_Err = err;
            return 0;
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
            job->m_ContentIndex,
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
            job->m_Err = err;
            return 0;
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

    uint64_t block_chunks_count = 0;
    for(uint32_t b = 0; b < block_reader_job_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = stored_block[b]->m_BlockIndex;
        block_chunks_count += *block_index->m_ChunkCount;
    }

    size_t block_chunks_lookup_size = Longtail_LookupTable_GetSize(block_chunks_count);
    size_t chunk_sizes_size = sizeof(uint32_t) * block_chunks_count;
    size_t chunk_offsets_size = sizeof(uint32_t) * block_chunks_count;
    size_t block_indexes_size = sizeof(uint32_t) * block_chunks_count;

    size_t work_mem_size =
        block_chunks_lookup_size +
        chunk_sizes_size +
        chunk_offsets_size +
        block_indexes_size;
    void* work_mem = Longtail_Alloc(work_mem_size);
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
            LONGTAIL_FATAL_ASSERT(ctx, err == 0, job->m_Err = EINVAL; return 0)
        }
        job->m_Err = ENOMEM;
        return 0;
    }
    char* p = (char*)work_mem;
    struct Longtail_LookupTable* block_chunks_lookup = Longtail_LookupTable_Create(p, block_chunks_count, 0);
    p += block_chunks_lookup_size;
    uint32_t* chunk_sizes = (uint32_t*)p;
    p += chunk_sizes_size;
    uint32_t* chunk_offsets = (uint32_t*)p;
    p += chunk_offsets_size;
    uint32_t* block_indexes = (uint32_t*)p;

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

            Longtail_LookupTable_Put(block_chunks_lookup, chunk_hash, block_chunk_index_offset);

            block_chunk_index_offset++;
        }
    }

    while (chunk_index_offset < write_chunk_index_offset + write_chunk_count)
    {
        uint32_t asset_chunk_index = chunk_index_start + chunk_index_offset;
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[asset_chunk_index];
        TLongtail_Hash chunk_hash = job->m_VersionIndex->m_ChunkHashes[chunk_index];

        uint64_t* chunk_block_index = Longtail_LookupTable_Get(block_chunks_lookup, chunk_hash);
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
                LONGTAIL_FATAL_ASSERT(ctx, err == 0, job->m_Err = EINVAL; return 0)
            }
            job->m_Err = EINVAL;
            Longtail_Free(work_mem);
            return 0;
        }

        uint32_t block_index = block_indexes[*chunk_block_index];
        uint32_t chunk_block_offset = chunk_offsets[*chunk_block_index];
        uint32_t chunk_size = chunk_sizes[*chunk_block_index];
        const char* block_data = (char*)stored_block[block_index]->m_BlockData;

        while(chunk_index_offset < (write_chunk_index_offset + write_chunk_count - 1))
        {
            uint32_t next_chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[asset_chunk_index + 1];
            TLongtail_Hash next_chunk_hash = job->m_VersionIndex->m_ChunkHashes[next_chunk_index];

            uint64_t* next_chunk_block_index = Longtail_LookupTable_Get(block_chunks_lookup, next_chunk_hash);
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
                    LONGTAIL_FATAL_ASSERT(ctx, err == 0, job->m_Err = EINVAL; return 0)
                }
                job->m_Err = EINVAL;
                Longtail_Free(work_mem);
                return 0;
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
            job->m_Err = err;
            if (sync_write_job)
            {
                int sync_err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(ctx, sync_err == 0, return 0)
            }
            Longtail_Free(work_mem);
            return 0;
        }
        write_offset += chunk_size;

        ++chunk_index_offset;
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
            job->m_Err = err;
            return 0;
        }
        job->m_Err = 0;
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
            job->m_Err = err;
            return 0;
        }
    }

    job->m_Err = 0;
    return 0;
}

struct WriteAssetsFromBlockJob
{
    struct Longtail_StorageAPI* m_VersionStorageAPI;
    const struct Longtail_ContentIndex* m_ContentIndex;
    const struct Longtail_VersionIndex* m_VersionIndex;
    const char* m_VersionFolder;
    struct BlockReaderJob m_BlockReadJob;
    uint64_t m_BlockIndex;
    uint32_t* m_AssetIndexes;
    uint32_t m_AssetCount;
    int m_RetainPermissions;
    int m_Err;
};

static int WriteAssetsFromBlock(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)

    struct WriteAssetsFromBlockJob* job = (struct WriteAssetsFromBlockJob*)context;
    struct Longtail_StorageAPI* version_storage_api = job->m_VersionStorageAPI;
    const char* version_folder = job->m_VersionFolder;
    const struct Longtail_ContentIndex* content_index = job->m_ContentIndex;
    const struct Longtail_VersionIndex* version_index = job->m_VersionIndex;
    uint32_t* asset_indexes = job->m_AssetIndexes;
    uint32_t asset_count = job->m_AssetCount;

    if (job->m_BlockReadJob.m_Err)
    {
        LONGTAIL_LOG(ctx, (job->m_BlockReadJob.m_Err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "BlockReadJob() failed with %d", job->m_BlockReadJob.m_Err)
        job->m_Err = job->m_BlockReadJob.m_Err;
        return 0;
    }

    if (is_cancelled)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "WriteAssetsFromBlock was cancelled, failed with %d", job->m_BlockReadJob.m_Err)
       job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
       job->m_Err = ECANCELED;
       return 0;
    }

    const char* block_data = (char*)job->m_BlockReadJob.m_StoredBlock->m_BlockData;
    struct Longtail_BlockIndex* block_index = job->m_BlockReadJob.m_StoredBlock->m_BlockIndex;

    uint32_t block_chunks_count = *block_index->m_ChunkCount;

    size_t chuck_offsets_size = sizeof(uint32_t) * block_chunks_count;
    size_t block_chunks_lookup_size = Longtail_LookupTable_GetSize(block_chunks_count);
    size_t tmp_mem_size =
        chuck_offsets_size +
        block_chunks_lookup_size;

    char* tmp_mem = (char*)Longtail_Alloc(tmp_mem_size);
    if (!tmp_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
        job->m_BlockReadJob.m_StoredBlock = 0;
        job->m_Err = ENOMEM;
        return 0;
    }
    struct Longtail_LookupTable* block_chunks_lookup = Longtail_LookupTable_Create(tmp_mem, block_chunks_count, 0);
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

        Longtail_LookupTable_Put(block_chunks_lookup, chunk_hash, block_chunk_index_offset);

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
            job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
            job->m_BlockReadJob.m_StoredBlock = 0;
            job->m_Err = err;
            Longtail_Free(tmp_mem);
            return 0;
        }

        uint16_t permissions;
        err = version_storage_api->GetPermissions(version_storage_api, full_asset_path, &permissions);
        if (err && (err != ENOENT))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->GetPermissions() failed with %d", err)
            Longtail_Free(full_asset_path);
            job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
            job->m_BlockReadJob.m_StoredBlock = 0;
            job->m_Err = err;
            Longtail_Free(tmp_mem);
            return 0;
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
                    job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
                    job->m_BlockReadJob.m_StoredBlock = 0;
                    job->m_Err = err;
                    Longtail_Free(tmp_mem);
                    return 0;
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
            job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
            job->m_BlockReadJob.m_StoredBlock = 0;
            job->m_Err = err;
            Longtail_Free(tmp_mem);
            return 0;
        }

        uint64_t asset_write_offset = 0;
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];

            uint64_t* chunk_block_index = Longtail_LookupTable_Get(block_chunks_lookup, chunk_hash);
            LONGTAIL_FATAL_ASSERT(ctx, chunk_block_index != 0, job->m_Err = EINVAL; return 0)

            uint32_t chunk_block_offset = chunk_offsets[*chunk_block_index];
            uint32_t chunk_size = chunk_sizes[*chunk_block_index];

            while(asset_chunk_index < (asset_chunk_count - 1))
            {
                uint32_t next_chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index + 1];
                TLongtail_Hash next_chunk_hash = version_index->m_ChunkHashes[next_chunk_index];
                uint64_t* next_chunk_block_index = Longtail_LookupTable_Get(block_chunks_lookup, next_chunk_hash);
                LONGTAIL_FATAL_ASSERT(ctx, next_chunk_block_index != 0, job->m_Err = EINVAL; return 0)
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
                job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
                job->m_BlockReadJob.m_StoredBlock = 0;
                job->m_Err = err;
                Longtail_Free(tmp_mem);
                return 0;
            }
            asset_write_offset += chunk_size;
        }

        version_storage_api->CloseFile(version_storage_api, asset_file);
        asset_file = 0;

        if (job->m_RetainPermissions)
        {
            err = version_storage_api->SetPermissions(version_storage_api, full_asset_path, (uint16_t)version_index->m_Permissions[asset_index]);
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
                job->m_BlockReadJob.m_StoredBlock = 0;
                job->m_Err = err;
                Longtail_Free(tmp_mem);
                return 0;
            }
        }
    }
    Longtail_Free(tmp_mem);

    job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
    job->m_BlockReadJob.m_StoredBlock = 0;
    job->m_Err = 0;
    return 0;
}

struct AssetWriteList
{
    uint32_t m_BlockJobCount;
    uint32_t m_AssetJobCount;
    uint32_t* m_BlockJobAssetIndexes;
    uint32_t* m_AssetIndexJobs;
};

struct BlockJobCompareContext
{
    const struct AssetWriteList* m_AssetWriteList;
    const uint32_t* asset_chunk_index_starts;
    const uint32_t* asset_chunk_indexes;
    const TLongtail_Hash* chunk_hashes;
    struct Longtail_LookupTable* chunk_hash_to_block_index;
};

static SORTFUNC(BlockJobCompare)
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

    struct BlockJobCompareContext* c = (struct BlockJobCompareContext*)context;
    struct Longtail_LookupTable* chunk_hash_to_block_index = c->chunk_hash_to_block_index;

    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;

    uint32_t asset_chunk_offset_a = c->asset_chunk_index_starts[a];
    uint32_t asset_chunk_offset_b = c->asset_chunk_index_starts[b];
    uint32_t chunk_index_a = c->asset_chunk_indexes[asset_chunk_offset_a];
    uint32_t chunk_index_b = c->asset_chunk_indexes[asset_chunk_offset_b];

    TLongtail_Hash a_first_chunk_hash = c->chunk_hashes[chunk_index_a];
    TLongtail_Hash b_first_chunk_hash = c->chunk_hashes[chunk_index_b];
//    if (a_first_chunk_hash == b_first_chunk_hash)
//    {
//        return 0;
//    }
    const uint64_t* a_block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, a_first_chunk_hash);
    LONGTAIL_FATAL_ASSERT(ctx, a_block_index_ptr, return 0)
    const uint64_t* b_block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, b_first_chunk_hash);
    LONGTAIL_FATAL_ASSERT(ctx, b_block_index_ptr, return 0)
    uint64_t a_block_index = *a_block_index_ptr;
    uint64_t b_block_index = *b_block_index_ptr;
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
    struct AssetWriteList* awl = (struct AssetWriteList*)(Longtail_Alloc(awl_size));
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
        uint64_t* content_block_index = Longtail_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
        if (content_block_index == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", ENOENT)
            Longtail_Free(awl);
            return ENOENT;
        }

        int is_block_job = 1;
        for (uint32_t c = 1; c < chunk_count; ++c)
        {
            uint32_t next_chunk_index = asset_chunk_indexes[asset_chunk_offset + c];
            TLongtail_Hash next_chunk_hash = chunk_hashes[next_chunk_index];
            uint64_t* next_content_block_index = Longtail_LookupTable_Get(chunk_hash_to_block_index, next_chunk_hash);
            if (next_content_block_index == 0)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", ENOENT)
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

    struct BlockJobCompareContext block_job_compare_context = {
            awl,    // m_AssetWriteList
            asset_chunk_index_starts,
            asset_chunk_indexes,
            chunk_hashes,   // chunk_hashes
            chunk_hash_to_block_index  // chunk_hash_to_block_index
        };
    QSORT(awl->m_BlockJobAssetIndexes, (size_t)awl->m_BlockJobCount, sizeof(uint32_t), BlockJobCompare, &block_job_compare_context);

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
    const struct Longtail_ContentIndex* content_index,
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
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(chunk_hash_to_block_index, "%p"),
        LONGTAIL_LOGFIELD(awl, "%p"),
        LONGTAIL_LOGFIELD(retain_permssions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, version_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, chunk_hash_to_block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, awl != 0, return EINVAL)

    {
        uint32_t j = 0;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
            const uint64_t* block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
            if (!block_index_ptr)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", EINVAL)
                return EINVAL;
            }
            LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL);
            uint64_t block_index = *block_index_ptr;
            TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];

            ++j;
            while (j < awl->m_BlockJobCount)
            {
                uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
                TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
                const uint64_t* next_block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
                if (!next_block_index_ptr)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", EINVAL)
                    return EINVAL;
                }
                uint64_t next_block_index = *next_block_index_ptr;
                if (next_block_index != block_index)
                {
                    break;
                }
                ++j;
            }
        }
    }

    const uint32_t worker_count = job_api->GetWorkerCount(job_api) + 1;
    const uint32_t max_parallell_block_read_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;

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
                const uint64_t* block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, chunk_hash);
                LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
                uint64_t block_index = *block_index_ptr;
                TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
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

    int err = block_store_api->PreflightGet(block_store_api, *content_index->m_ChunkCount, content_index->m_ChunkHashes);
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

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    if (!block_jobs)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    Longtail_JobAPI_Group job_group = 0;
    err = job_api->ReserveJobs(job_api, (awl->m_BlockJobCount * 2u) + asset_job_count, &job_group);
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
        const uint64_t* block_index_ptr = Longtail_LookupTable_Get(chunk_hash_to_block_index, first_chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, return EINVAL)
        uint64_t block_index = *block_index_ptr;

        struct WriteAssetsFromBlockJob* job = &block_jobs[block_job_count++];
        struct BlockReaderJob* block_job = &job->m_BlockReadJob;
        block_job->m_BlockStoreAPI = block_store_api;
        block_job->m_AsyncCompleteAPI.m_API.Dispose = 0;
        block_job->m_AsyncCompleteAPI.OnComplete = 0;
        block_job->m_BlockHash = content_index->m_BlockHashes[block_index];
        block_job->m_JobAPI = job_api;
        block_job->m_JobID = 0;
        block_job->m_Err = EINVAL;
        block_job->m_StoredBlock = 0;
        Longtail_JobAPI_JobFunc block_read_funcs[1] = { BlockReader };
        void* block_read_ctxs[1] = {block_job};
        Longtail_JobAPI_Jobs block_read_job;
        err = job_api->CreateJobs(job_api, job_group, 1, block_read_funcs, block_read_ctxs, &block_read_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)

        job->m_VersionStorageAPI = version_storage_api;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = (uint64_t)block_index;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[j];
        job->m_RetainPermissions = retain_permssions;
        job->m_Err = EINVAL;

        job->m_AssetCount = 1;
        ++j;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            uint64_t* next_block_index = Longtail_LookupTable_Get(chunk_hash_to_block_index, next_first_chunk_hash);
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
        err = job_api->CreateJobs(job_api, job_group, 1, funcs, ctxs, &block_write_job);
        LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
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
    struct WritePartialAssetFromBlocksJob* asset_jobs = (struct WritePartialAssetFromBlocksJob*)Longtail_Alloc(asset_jobs_size);
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
            content_index,
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

    err = 0;
    for (uint32_t b = 0; b < block_job_count; ++b)
    {
        struct WriteAssetsFromBlockJob* job = &block_jobs[b];
        if (job->m_Err)
        {
            LONGTAIL_LOG(ctx, (job->m_Err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlockJob failed with %d", job->m_Err)
            err = err ? err : job->m_Err;
        }
    }
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        struct WritePartialAssetFromBlocksJob* job = &asset_jobs[a];
        if (job->m_Err)
        {
            const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[job->m_AssetIndex]];
            LONGTAIL_LOG(ctx, (job->m_Err == ECANCELED) ? LONGTAIL_LOG_LEVEL_DEBUG : LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocksJob failed with %d", job->m_Err)
            if (err == 0)
            {
                err = job->m_Err;
            }
            else if ((err == ENOTRECOVERABLE) && (job->m_Err != ENOTRECOVERABLE))
            {
                err = job->m_Err;
            }
        }
    }

    Longtail_Free(asset_jobs);
    Longtail_Free(block_jobs);

    return err;
}

int Longtail_WriteVersion(
    struct Longtail_BlockStoreAPI* block_storage_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_CancelAPI* optional_cancel_api,
    Longtail_CancelAPI_HCancelToken optional_cancel_token,
    const struct Longtail_ContentIndex* content_index,
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
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, block_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_path != 0, return EINVAL)

    if (*version_index->m_AssetCount == 0)
    {
        return 0;
    }

    uint64_t chunk_count = *content_index->m_ChunkCount;
    struct Longtail_LookupTable* chunk_hash_to_block_index = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(chunk_count)), chunk_count, 0);
    if (!chunk_hash_to_block_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
        uint64_t block_index = content_index->m_ChunkBlockIndexes[i];
        Longtail_LookupTable_PutUnique(chunk_hash_to_block_index, chunk_hash, block_index);
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
        content_index,
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

static uint64_t MakeUnique(TLongtail_Hash* hashes, uint64_t count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hashes, "%p"),
        LONGTAIL_LOGFIELD(count, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, count == 0 || hashes != 0, return 0)

    uint64_t w = 0;
    uint64_t r = 0;
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
    uint64_t reference_hash_count,
    const TLongtail_Hash* new_hashes,
    uint64_t new_hash_count,
    uint64_t* added_hash_count,
    TLongtail_Hash* added_hashes,
    uint64_t* removed_hash_count,
    TLongtail_Hash* removed_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(reference_hashes, "%p"),
        LONGTAIL_LOGFIELD(reference_hash_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(new_hashes, "%p"),
        LONGTAIL_LOGFIELD(new_hash_count, "%" PRIu64),
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
    void* work_mem = Longtail_Alloc(work_mem_size);
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

    uint64_t removed = 0;
    uint64_t added = 0;
    uint64_t ni = 0;
    uint64_t ri = 0;
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
        struct Longtail_LookupTable* added_hashes_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(added)), added, 0);
        if (!added_hashes_lookup)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
        }
        for (uint64_t i = 0; i < added; ++i)
        {
            Longtail_LookupTable_Put(added_hashes_lookup, added_hashes[i], i);
        }
        added = 0;
        for (uint64_t i = 0; i < new_hash_count; ++i)
        {
            TLongtail_Hash hash = new_hashes[i];
            if (Longtail_LookupTable_Get(added_hashes_lookup, hash) == 0)
            {
                continue;
            }
            added_hashes[added++] = hash;
        }
        Longtail_Free(added_hashes_lookup);
    }
    return 0;
}

int Longtail_CreateMissingContent(
    struct Longtail_HashAPI* hash_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, max_chunks_per_block != 0, return EINVAL)

    uint64_t chunk_count = *version_index->m_ChunkCount;
    size_t added_hashes_size = sizeof(TLongtail_Hash) * chunk_count;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)Longtail_Alloc(added_hashes_size);
    if (!added_hashes)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    uint64_t added_hash_count = 0;
    int err = DiffHashes(
        content_index->m_ChunkHashes,
        *content_index->m_ChunkCount,
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
        err = Longtail_CreateContentIndex(
            hash_api,
            0,
            max_block_size,
            max_chunks_per_block,
            out_content_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndex() failed with %d", err)
        }
        return err;
    }

    size_t chunk_index_lookup_size = Longtail_LookupTable_GetSize(chunk_count);
    size_t tmp_diff_chunk_sizes_size = sizeof(uint32_t) * added_hash_count;
    size_t tmp_diff_chunk_tags_size = sizeof(uint32_t) * added_hash_count;
    size_t work_mem_size =
        chunk_index_lookup_size +
        tmp_diff_chunk_sizes_size +
        tmp_diff_chunk_tags_size;
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(added_hashes);
        return ENOMEM;
    }
    char* p = (char*)work_mem;
    struct Longtail_LookupTable* chunk_index_lookup = Longtail_LookupTable_Create(p, chunk_count, 0);
    p += chunk_index_lookup_size;
    uint32_t* tmp_diff_chunk_sizes = (uint32_t*)p;
    p += tmp_diff_chunk_sizes_size;
    uint32_t* tmp_diff_chunk_tags = (uint32_t*)p;

    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        Longtail_LookupTable_Put(chunk_index_lookup, version_index->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        const uint64_t* chunk_index_ptr = Longtail_LookupTable_Get(chunk_index_lookup, added_hashes[j]);
        LONGTAIL_FATAL_ASSERT(ctx, chunk_index_ptr, return EINVAL)
        uint64_t chunk_index = *chunk_index_ptr;
        tmp_diff_chunk_sizes[j] = version_index->m_ChunkSizes[chunk_index];
        tmp_diff_chunk_tags[j] = version_index->m_ChunkTags[chunk_index];
    }

    err = Longtail_CreateContentIndexRaw(
        hash_api,
        added_hash_count,
        added_hashes,
        tmp_diff_chunk_sizes,
        tmp_diff_chunk_tags,
        max_block_size,
        max_chunks_per_block,
        out_content_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw() failed with %d", err)
    }

    Longtail_Free(work_mem);
    Longtail_Free(added_hashes);

    return err;
}

int Longtail_GetMissingChunks(
    const struct Longtail_ContentIndex* content_index,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint64_t* out_chunk_count,
    TLongtail_Hash* out_missing_chunk_hashes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%" PRIu64),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_count, "%p"),
        LONGTAIL_LOGFIELD(out_missing_chunk_hashes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (out_missing_chunk_hashes != 0), return EINVAL)

    uint64_t reference_chunk_count = *content_index->m_ChunkCount;
    struct Longtail_LookupTable* chunk_to_reference_block_index_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(reference_chunk_count)), reference_chunk_count, 0);
    if (!chunk_to_reference_block_index_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    for (uint64_t c = 0; c < reference_chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[c];
        uint64_t block_index = content_index->m_ChunkBlockIndexes[c];
        Longtail_LookupTable_Put(chunk_to_reference_block_index_lookup, chunk_hash, block_index);
    }

    uint64_t missing_chunk_count = 0;
    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = chunk_hashes[c];
        if (Longtail_LookupTable_Get(chunk_to_reference_block_index_lookup, chunk_hash))
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

int Longtail_GetExistingContentIndex(
    const struct Longtail_StoreIndex* store_index,
    uint32_t chunk_count,
    const TLongtail_Hash* chunks,
    uint32_t min_block_usage_percent,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(store_index, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunks, "%p"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(max_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunks != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_content_index != 0, return EINVAL)

    uint32_t hash_identifier = *store_index->m_HashIdentifier;
    uint32_t store_block_count = *store_index->m_BlockCount;
    uint32_t store_chunk_count = *store_index->m_ChunkCount;

    size_t chunk_to_index_lookup_size = Longtail_LookupTable_GetSize(chunk_count);
    size_t block_to_index_lookup_size = Longtail_LookupTable_GetSize(store_block_count);
    size_t chunk_to_store_index_lookup_size = Longtail_LookupTable_GetSize(chunk_count);
    size_t found_store_block_hashes_size = sizeof(TLongtail_Hash) * store_block_count;
    size_t found_store_chunk_hashes_size = sizeof(TLongtail_Hash) * chunk_count;
    size_t found_store_chunk_per_block_size = sizeof(uint32_t) * store_block_count;
    size_t block_uses_size = sizeof(uint32_t) * store_block_count;
    size_t block_sizes_size = sizeof(uint32_t) * store_block_count;
    size_t block_order_size = sizeof(uint32_t) * store_block_count;
    size_t chunk_index_offsets_size = sizeof(uint32_t) * store_block_count;

    size_t tmp_mem_size = chunk_to_index_lookup_size +
        block_to_index_lookup_size +
        chunk_to_store_index_lookup_size +
        found_store_chunk_per_block_size +
        found_store_block_hashes_size +
        found_store_chunk_hashes_size +
        block_uses_size +
        block_sizes_size +
        block_order_size +
        chunk_index_offsets_size;

    void* tmp_mem = Longtail_Alloc(tmp_mem_size);
    if (!tmp_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)tmp_mem;

    struct Longtail_LookupTable* chunk_to_index_lookup = Longtail_LookupTable_Create(p, chunk_count, 0);
    p += chunk_to_index_lookup_size;

    struct Longtail_LookupTable* block_to_index_lookup = Longtail_LookupTable_Create(p, store_block_count, 0);
    p += block_to_index_lookup_size;

    struct Longtail_LookupTable* chunk_to_store_index_lookup = Longtail_LookupTable_Create(p, chunk_count, 0);
    p += chunk_to_store_index_lookup_size;

    TLongtail_Hash* found_store_block_hashes = (TLongtail_Hash*)p;
    p += found_store_block_hashes_size;

    TLongtail_Hash* found_store_chunk_hashes = (TLongtail_Hash*)p;
    p += found_store_chunk_hashes_size;

    uint32_t* found_store_chunk_per_block = (uint32_t*)p;
    p += found_store_chunk_per_block_size;

    uint32_t* block_uses = (uint32_t*)p;
    p += block_uses_size;

    uint32_t* block_sizes = (uint32_t*)p;
    p += block_sizes_size;

    uint32_t* block_order = (uint32_t*)p;
    p += block_order_size;

    uint32_t* chunk_index_offsets = (uint32_t*)p;
    p += chunk_index_offsets_size;

    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = chunks[i];
        Longtail_LookupTable_Put(chunk_to_index_lookup, chunk_hash, i);
    }

    uint32_t found_block_count = 0;
    uint32_t found_chunk_count = 0;
    if (min_block_usage_percent <= 100)
    {
        uint32_t chunk_offset = 0;
        for (uint32_t b = 0; b < store_block_count; ++b)
        {
            block_order[b] = b;
            block_uses[b] = 0;
            block_sizes[b] = 0;
            chunk_index_offsets[b] = chunk_offset;
            TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
            for (uint32_t c = 0; c < block_chunk_count; ++c)
            {
                uint32_t chunk_size = store_index->m_ChunkSizes[chunk_offset];
                TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[chunk_offset];
                ++chunk_offset;
                block_sizes[b] += chunk_size;
                if (Longtail_LookupTable_Get(chunk_to_index_lookup, chunk_hash))
                {
                    block_uses[b] += chunk_size;
                }
            }
        }
        // Favour blocks we use more data out of - if a chunk is in mutliple blocks we want to pick
        // the blocks that has the most requested chunk data
        // This does not guarantee a perfect block match as one block can be a 100% match which
        // could lead to skipping part or whole of another 100% match block resulting in us
        // picking a block that we will not use 100% of
        QSORT(block_order, store_block_count, sizeof(uint32_t), SortBlockUsageHighToLow, (void*)block_uses);

        for (uint32_t bo = 0; (bo < store_block_count) && (found_chunk_count < chunk_count); ++bo)
        {
            uint32_t b = block_order[bo];
            uint32_t block_use = block_uses[b];
            uint32_t block_size = block_sizes[b];
            if (min_block_usage_percent > 0) {
                if (block_use == 0)
                {
                    break;
                }
                uint32_t block_usage_percent = (uint32_t)(((uint64_t)block_use * 100) / block_size);
                if (block_usage_percent < min_block_usage_percent)
                {
                    continue;
                }
            }
            TLongtail_Hash block_hash = store_index->m_BlockHashes[b];
            uint32_t block_chunk_count = store_index->m_BlockChunkCounts[b];
            uint32_t store_chunk_index_offset = chunk_index_offsets[b];
            uint32_t current_found_block_index = found_block_count;
            for (uint32_t c = 0; c < block_chunk_count; ++c)
            {
                TLongtail_Hash chunk_hash = store_index->m_ChunkHashes[store_chunk_index_offset];
                if ((block_use != block_size) && (!Longtail_LookupTable_Get(chunk_to_index_lookup, chunk_hash)))
                {
                    ++store_chunk_index_offset;
                    continue;
                }
                if (Longtail_LookupTable_PutUnique(chunk_to_store_index_lookup, chunk_hash, store_chunk_index_offset))
                {
                    ++store_chunk_index_offset;
                    continue;
                }
                found_store_chunk_hashes[found_chunk_count++] = chunk_hash;
                if (current_found_block_index == found_block_count)
                {
                    uint64_t* block_index_ptr = Longtail_LookupTable_PutUnique(block_to_index_lookup, block_hash, current_found_block_index);
                    if (block_index_ptr == 0)
                    {
                        found_store_block_hashes[found_block_count++] = block_hash;
                        found_store_chunk_per_block[current_found_block_index] = 1;
                    }
                    ++store_chunk_index_offset;
                    continue;
                }
                found_store_chunk_per_block[current_found_block_index]++;
                ++store_chunk_index_offset;
            }
        }
    }

    // We have a list of indexes into chunks of the chunks we found in the store
    // We have a list of indexes into store_index->m_BlockHashes of the blocks we need
    size_t content_index_size = Longtail_GetContentIndexSize(found_block_count, found_chunk_count);
    void* content_index_mem = Longtail_Alloc(content_index_size);
    if (!content_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_ContentIndex* content_index = Longtail_InitContentIndex(
        content_index_mem,
        hash_identifier,
        max_block_size,
        max_chunks_per_block,
        found_block_count,
        found_chunk_count);
    LONGTAIL_FATAL_ASSERT(ctx, content_index != 0, return EINVAL)

    uint32_t content_chunk_offset = 0;
    for (uint32_t b = 0; b < found_block_count; ++b)
    {
        content_index->m_BlockHashes[b] = found_store_block_hashes[b];
        uint32_t chunks_per_block = found_store_chunk_per_block[b];
        for (uint32_t c = 0; c < chunks_per_block; ++c)
        {
            content_index->m_ChunkHashes[content_chunk_offset] = found_store_chunk_hashes[content_chunk_offset];
            content_index->m_ChunkBlockIndexes[content_chunk_offset] = b;
            ++content_chunk_offset;
        }
    }

    Longtail_Free(tmp_mem);
    *out_content_index = content_index;

    return 0;
}

struct FindNewChunksJob
{
    const struct Longtail_ContentIndex* m_ContentIndex;
    const struct Longtail_LookupTable* m_ChunkHashToBlockIndex;
    uint64_t m_StartRange;
    uint64_t m_EndRange;
    uint64_t* m_ChunkBlockIndexes;
};

static int FindNewChunks(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context != 0, return 0)
    struct FindNewChunksJob* job = (struct FindNewChunksJob*)context;

    for (uint64_t c = job->m_StartRange; c < job->m_EndRange; ++c)
    {
        TLongtail_Hash chunk_hash = job->m_ContentIndex->m_ChunkHashes[c];
        uint64_t* block_index = Longtail_LookupTable_Get(job->m_ChunkHashToBlockIndex, chunk_hash);
        job->m_ChunkBlockIndexes[c] = block_index ? *block_index : 0xfffffffffffffffful;
    }

    return 0;
}

struct MergeContentIndex_BuildBlockHashToBlockIndex_Context
{
    struct Longtail_ContentIndex* new_content_index;
    struct Longtail_LookupTable* block_hash_to_block_index;
    TLongtail_Hash* tmp_compact_block_hashes;
};

static int MergeContentIndex_BuildBlockHashToBlockIndex(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct MergeContentIndex_BuildBlockHashToBlockIndex_Context* c = (struct MergeContentIndex_BuildBlockHashToBlockIndex_Context*)context;
    const uint64_t compact_block_count = *c->new_content_index->m_BlockCount;
    for (uint64_t block_index = 0; block_index < compact_block_count; ++block_index)
    {
        TLongtail_Hash block_hash = c->new_content_index->m_BlockHashes[block_index];
        Longtail_LookupTable_Put(c->block_hash_to_block_index, block_hash, block_index);
        c->tmp_compact_block_hashes[block_index] = block_hash;
    }
    return 0;
}

struct MergeContentIndex_BuildChunkHashToBlockIndex_Context
{
    struct Longtail_ContentIndex* new_content_index;
    struct Longtail_LookupTable* chunk_hash_to_block_index;
    TLongtail_Hash* tmp_compact_chunk_hashes;
    TLongtail_Hash* tmp_compact_chunk_block_indexes;
};

static int MergeContentIndex_BuildChunkHashToBlockIndex(void* context, uint32_t job_id, int is_cancelled)
{
    struct MergeContentIndex_BuildChunkHashToBlockIndex_Context* c = (struct MergeContentIndex_BuildChunkHashToBlockIndex_Context*)context;
    const uint64_t compact_chunk_count = *c->new_content_index->m_ChunkCount;
    for (uint64_t chunk_index = 0; chunk_index < compact_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = c->new_content_index->m_ChunkHashes[chunk_index];
        uint64_t block_index = c->new_content_index->m_ChunkBlockIndexes[chunk_index];

        c->tmp_compact_chunk_hashes[chunk_index] = chunk_hash;
        c->tmp_compact_chunk_block_indexes[chunk_index] = block_index;

        Longtail_LookupTable_PutUnique(c->chunk_hash_to_block_index, chunk_hash, block_index);
    }
    return 0;
}

int Longtail_MergeContentIndex(
    struct Longtail_JobAPI* job_api,
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* new_content_index,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(local_content_index, "%p"),
        LONGTAIL_LOGFIELD(new_content_index, "%p"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, local_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, new_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, ((*local_content_index->m_BlockCount) == 0 || (*new_content_index->m_BlockCount) == 0) || ((*local_content_index->m_HashIdentifier) == (*new_content_index->m_HashIdentifier)), return EINVAL)

    uint32_t hash_identifier = (*local_content_index->m_BlockCount) != 0 ? (*local_content_index->m_HashIdentifier) : (*new_content_index->m_HashIdentifier);

    uint64_t max_block_count = *local_content_index->m_BlockCount + *new_content_index->m_BlockCount;
    uint64_t max_chunk_count = *local_content_index->m_ChunkCount + *new_content_index->m_ChunkCount;
    size_t block_hash_to_block_index_size = Longtail_LookupTable_GetSize(max_block_count);
    size_t chunk_hash_to_block_index_size = Longtail_LookupTable_GetSize(max_chunk_count);
    size_t tmp_compact_block_hashes_size = sizeof(TLongtail_Hash) * max_block_count;
    size_t tmp_compact_chunk_hashes_size = sizeof(TLongtail_Hash) * max_chunk_count;
    size_t tmp_compact_chunk_block_indexes_size = sizeof(TLongtail_Hash) * max_chunk_count;
    size_t work_mem_size =
        block_hash_to_block_index_size +
        chunk_hash_to_block_index_size +
        tmp_compact_block_hashes_size +
        tmp_compact_chunk_hashes_size +
        tmp_compact_chunk_block_indexes_size;
    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    char* p = (char*)work_mem;
    struct Longtail_LookupTable* block_hash_to_block_index = Longtail_LookupTable_Create(p, max_block_count, 0);
    p += block_hash_to_block_index_size;
    struct Longtail_LookupTable* chunk_hash_to_block_index = Longtail_LookupTable_Create(p, max_chunk_count, 0);
    p += chunk_hash_to_block_index_size;
    TLongtail_Hash* tmp_compact_block_hashes = (TLongtail_Hash*)p;
    p += tmp_compact_block_hashes_size;
    TLongtail_Hash* tmp_compact_chunk_hashes = (TLongtail_Hash*)p;
    p += tmp_compact_chunk_hashes_size;
    TLongtail_Hash* tmp_compact_chunk_block_indexes = (TLongtail_Hash*)p;

    uint64_t compact_chunk_count = *new_content_index->m_ChunkCount;
    uint64_t compact_block_count = *new_content_index->m_BlockCount;

    {
        struct MergeContentIndex_BuildBlockHashToBlockIndex_Context block_hash_to_block_index_context;
        block_hash_to_block_index_context.new_content_index = new_content_index;
        block_hash_to_block_index_context.block_hash_to_block_index = block_hash_to_block_index;
        block_hash_to_block_index_context.tmp_compact_block_hashes = tmp_compact_block_hashes;

        struct MergeContentIndex_BuildChunkHashToBlockIndex_Context chunk_hash_to_block_index_context;
        chunk_hash_to_block_index_context.new_content_index = new_content_index;
        chunk_hash_to_block_index_context.chunk_hash_to_block_index = chunk_hash_to_block_index;
        chunk_hash_to_block_index_context.tmp_compact_chunk_hashes = tmp_compact_chunk_hashes;
        chunk_hash_to_block_index_context.tmp_compact_chunk_block_indexes = tmp_compact_chunk_block_indexes;

        Longtail_JobAPI_JobFunc funcs[2] = {MergeContentIndex_BuildBlockHashToBlockIndex, MergeContentIndex_BuildChunkHashToBlockIndex};
        void* ctxs[2] = {&block_hash_to_block_index_context, &chunk_hash_to_block_index_context};

        Longtail_JobAPI_Group job_group = 0;
        job_api->ReserveJobs(job_api, 2, &job_group);

        Longtail_JobAPI_Jobs jobs;
        job_api->CreateJobs(job_api, job_group, 2, funcs, ctxs, &jobs);
        job_api->ReadyJobs(job_api, 2, jobs);
        job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);
    }
    uint64_t local_content_chunk_count = *local_content_index->m_ChunkCount;
    if (local_content_chunk_count > 0)
    {
        uint32_t worker_count = job_api->GetWorkerCount(job_api);
        const uint32_t min_chunks_per_job = 65536;
        uint64_t chunks_per_job = worker_count > 0 ? local_content_chunk_count / worker_count : local_content_chunk_count;
        if (chunks_per_job < min_chunks_per_job)
        {
            chunks_per_job = min_chunks_per_job;
        }
        uint32_t job_count = (uint32_t)((local_content_chunk_count + chunks_per_job  - 1)/ chunks_per_job);

        size_t chunk_block_indexes_size = sizeof(uint64_t) * local_content_chunk_count;
        size_t jobs_size = sizeof(struct FindNewChunksJob) * job_count;
        size_t funcs_size = sizeof(Longtail_JobAPI_JobFunc) * job_count;
        size_t ctxs_size = sizeof(void*) * job_count;
        size_t job_mem_size = chunk_block_indexes_size + jobs_size + funcs_size + ctxs_size;

        void* job_mem = Longtail_Alloc(job_mem_size);
        if (!job_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_Free(work_mem);
            return ENOMEM;
        }

        char* p = (char*)job_mem;
        uint64_t* chunk_block_indexes = (uint64_t*)p;
        p += chunk_block_indexes_size;

        struct FindNewChunksJob* jobs = (struct FindNewChunksJob*)p;
        p += jobs_size;

        Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)p;
        p += funcs_size;

        void** ctxs = (void**)p;

        Longtail_JobAPI_Group job_group = 0;
        job_api->ReserveJobs(job_api, job_count, &job_group);

        uint64_t chunk_job_start = 0;
        for (uint32_t j = 0; j < job_count; ++j)
        {
            struct FindNewChunksJob* job = &jobs[j];
            job->m_ContentIndex = local_content_index;
            job->m_ChunkHashToBlockIndex = chunk_hash_to_block_index;
            job->m_StartRange = chunk_job_start;
            job->m_EndRange = chunk_job_start + chunks_per_job;
            if (job->m_EndRange > local_content_chunk_count)
            {
                job->m_EndRange = local_content_chunk_count;
            }
            job->m_ChunkBlockIndexes = chunk_block_indexes;
            funcs[j] = FindNewChunks;
            ctxs[j] = job;
            chunk_job_start += chunks_per_job;
        }
        Longtail_JobAPI_Jobs runjobs;
        job_api->CreateJobs(job_api, job_group, job_count, funcs, ctxs, &runjobs);
        job_api->ReadyJobs(job_api, job_count, runjobs);

        job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);

        for (uint64_t c = 0; c < local_content_chunk_count; ++c)
        {
            TLongtail_Hash chunk_hash = local_content_index->m_ChunkHashes[c];
            uint64_t compact_block_index = chunk_block_indexes[c];
            if (compact_block_index != 0xfffffffffffffffful)
            {
                continue;
            }
            tmp_compact_chunk_hashes[compact_chunk_count] = chunk_hash;
            uint64_t block_index = local_content_index->m_ChunkBlockIndexes[c];
            TLongtail_Hash block_hash = local_content_index->m_BlockHashes[block_index];
            uint64_t* compact_block_index_ptr = Longtail_LookupTable_PutUnique(block_hash_to_block_index, block_hash, compact_block_count);
            if (compact_block_index_ptr == 0)
            {
                tmp_compact_chunk_block_indexes[compact_chunk_count++] = compact_block_count;
                tmp_compact_block_hashes[compact_block_count++] = block_hash;
                continue;
            }
            tmp_compact_chunk_block_indexes[compact_chunk_count++] = *compact_block_index_ptr;
        }
        Longtail_Free(job_mem);
    }


    size_t content_index_size = Longtail_GetContentIndexSize(compact_block_count, compact_chunk_count);
    void* compact_content_index_mem = Longtail_Alloc(content_index_size);
    if (!compact_content_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_ContentIndex* compact_content_index = Longtail_InitContentIndex(
        compact_content_index_mem,
        hash_identifier,
        *new_content_index->m_MaxBlockSize,
        *new_content_index->m_MaxChunksPerBlock,
        compact_block_count,
        compact_chunk_count);
    LONGTAIL_FATAL_ASSERT(ctx, compact_content_index != 0, return EINVAL)

    memcpy(compact_content_index->m_BlockHashes, tmp_compact_block_hashes, sizeof(TLongtail_Hash) * compact_block_count);
    memcpy(compact_content_index->m_ChunkHashes, tmp_compact_chunk_hashes, sizeof(TLongtail_Hash) * compact_chunk_count);
    memcpy(compact_content_index->m_ChunkBlockIndexes, tmp_compact_chunk_block_indexes, sizeof(uint64_t) * compact_chunk_count);

    *out_content_index = compact_content_index;
    Longtail_Free(work_mem);
    return 0;
}

int Longtail_AddContentIndex(
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* new_content_index,
    struct Longtail_ContentIndex** out_content_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(local_content_index, "%p"),
        LONGTAIL_LOGFIELD(new_content_index, "%p"),
        LONGTAIL_LOGFIELD(out_content_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, local_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, new_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, ((*local_content_index->m_BlockCount) == 0 || (*new_content_index->m_BlockCount) == 0) || ((*local_content_index->m_HashIdentifier) == (*new_content_index->m_HashIdentifier)), return EINVAL)

    uint32_t hash_identifier = (*local_content_index->m_BlockCount) != 0 ? (*local_content_index->m_HashIdentifier) : (*new_content_index->m_HashIdentifier);
    uint64_t local_block_count = *local_content_index->m_BlockCount;
    uint64_t remote_block_count = *new_content_index->m_BlockCount;
    uint64_t local_chunk_count = *local_content_index->m_ChunkCount;
    uint64_t remote_chunk_count = *new_content_index->m_ChunkCount;
    uint64_t block_count = local_block_count + remote_block_count;
    uint64_t chunk_count = local_chunk_count + remote_chunk_count;

    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    void* content_index_mem = Longtail_Alloc(content_index_size);
    if (!content_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_ContentIndex* content_index = Longtail_InitContentIndex(
        content_index_mem,
        hash_identifier,
        *local_content_index->m_MaxBlockSize,
        *local_content_index->m_MaxChunksPerBlock,
        block_count,
        chunk_count);
    LONGTAIL_FATAL_ASSERT(ctx, content_index != 0, return EINVAL)

    for (uint64_t b = 0; b < local_block_count; ++b)
    {
        content_index->m_BlockHashes[b] = local_content_index->m_BlockHashes[b];
    }
    for (uint64_t b = 0; b < remote_block_count; ++b)
    {
        content_index->m_BlockHashes[local_block_count + b] = new_content_index->m_BlockHashes[b];
    }
    for (uint64_t a = 0; a < local_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[a] = local_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[a] = local_content_index->m_ChunkBlockIndexes[a];
    }
    for (uint64_t a = 0; a < remote_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[local_chunk_count + a] = new_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[local_chunk_count + a] = local_block_count + new_content_index->m_ChunkBlockIndexes[a];
    }
    *out_content_index = content_index;
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

    const struct Longtail_VersionIndex* version_index = (const struct Longtail_VersionIndex*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    const char* a_path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
    const char* b_path = &version_index->m_NameData[version_index->m_NameOffsets[b]];
    size_t a_len = strlen(a_path);
    size_t b_len = strlen(b_path);
    return (a_len > b_len) ? 1 : (a_len < b_len) ? -1 : 0;
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

    const struct Longtail_VersionIndex* version_index = (const struct Longtail_VersionIndex*)context;
    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    const char* a_path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
    const char* b_path = &version_index->m_NameData[version_index->m_NameOffsets[b]];
    size_t a_len = strlen(a_path);
    size_t b_len = strlen(b_path);
    return (a_len < b_len) ? 1 : (a_len > b_len) ? -1 : 0;
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api->GetIdentifier(hash_api) == *source_version->m_HashIdentifier, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api->GetIdentifier(hash_api) == *target_version->m_HashIdentifier, return EINVAL)

    uint32_t source_asset_count = *source_version->m_AssetCount;
    uint32_t target_asset_count = *target_version->m_AssetCount;

    size_t source_asset_lookup_table_size = Longtail_LookupTable_GetSize(source_asset_count);
    size_t target_asset_lookup_table_size = Longtail_LookupTable_GetSize(target_asset_count);

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
        sizeof(uint32_t) * target_asset_count;
    void* work_mem = Longtail_Alloc(work_mem_size);
    uint8_t* p = (uint8_t*)work_mem;

    struct Longtail_LookupTable* source_path_hash_to_index = Longtail_LookupTable_Create(p, source_asset_count ,0);
    p += source_asset_lookup_table_size;
    struct Longtail_LookupTable* target_path_hash_to_index = Longtail_LookupTable_Create(p, target_asset_count ,0);
    p += target_asset_lookup_table_size;

    TLongtail_Hash* source_path_hashes = (TLongtail_Hash*)p;
    TLongtail_Hash* target_path_hashes = &source_path_hashes[source_asset_count];

    uint32_t* removed_source_asset_indexes = (uint32_t*)&target_path_hashes[target_asset_count];
    uint32_t* added_target_asset_indexes = &removed_source_asset_indexes[source_asset_count];

    uint32_t* modified_source_content_indexes = &added_target_asset_indexes[target_asset_count];
    uint32_t* modified_target_content_indexes = &modified_source_content_indexes[source_asset_count];

    uint32_t* modified_source_permissions_indexes = &modified_target_content_indexes[target_asset_count];
    uint32_t* modified_target_permissions_indexes = &modified_source_permissions_indexes[source_asset_count];

    for (uint32_t i = 0; i < source_asset_count; ++i)
    {
        // We are re-hashing since we might have an older version hash that is incompatible
        const char* path = &source_version->m_NameData[source_version->m_NameOffsets[i]];
        int err = Longtail_GetPathHash(hash_api, path, &source_path_hashes[i]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }
        Longtail_LookupTable_Put(source_path_hash_to_index, source_path_hashes[i], i);
    }

    for (uint32_t i = 0; i < target_asset_count; ++i)
    {
        // We are re-hashing since we might have an older version hash that is incompatible
        const char* path = &target_version->m_NameData[target_version->m_NameOffsets[i]];
        int err = Longtail_GetPathHash(hash_api, path, &target_path_hashes[i]);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
            Longtail_Free(work_mem);
            return err;
        }
        Longtail_LookupTable_Put(target_path_hash_to_index, target_path_hashes[i], i);
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
        const uint64_t* source_asset_index_ptr = Longtail_LookupTable_Get(source_path_hash_to_index, source_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
        uint32_t source_asset_index = (uint32_t)*source_asset_index_ptr;
        const uint64_t* target_asset_index_ptr = Longtail_LookupTable_Get(target_path_hash_to_index, target_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
        uint32_t target_asset_index = (uint32_t)*target_asset_index_ptr;

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
            else
            {
                uint16_t source_permissions = source_version->m_Permissions[source_asset_index];
                uint16_t target_permissions = target_version->m_Permissions[target_asset_index];
                if (source_permissions != target_permissions)
                {
                    modified_source_permissions_indexes[modified_permissions_count] = source_asset_index;
                    modified_target_permissions_indexes[modified_permissions_count] = target_asset_index;
                    ++modified_permissions_count;
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching permissions for asset %s", source_path)
                }
            }

            ++source_index;
            ++target_index;
        }
        else if (source_path_hash < target_path_hash)
        {
            const uint64_t* source_asset_index_ptr = Longtail_LookupTable_Get(source_path_hash_to_index, source_path_hash);
            LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
            source_asset_index = (uint32_t)*source_asset_index_ptr;
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Removed asset %s", source_path)
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        }
        else
        {
            const uint64_t* target_asset_index_ptr = Longtail_LookupTable_Get(target_path_hash_to_index, target_path_hash);
            LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
            target_asset_index = (uint32_t)*target_asset_index_ptr;
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
        const uint64_t* source_asset_index_ptr = Longtail_LookupTable_Get(source_path_hash_to_index, source_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, source_asset_index_ptr, return EINVAL)
        uint32_t source_asset_index = (uint32_t)*source_asset_index_ptr;
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
        const uint64_t* target_asset_index_ptr = Longtail_LookupTable_Get(target_path_hash_to_index, target_path_hash);
        LONGTAIL_FATAL_ASSERT(ctx, target_asset_index_ptr, return EINVAL)
        uint32_t target_asset_index = (uint32_t)*target_asset_index_ptr;
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
    struct Longtail_VersionDiff* version_diff = (struct Longtail_VersionDiff*)Longtail_Alloc(version_diff_size);
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

    QSORT(version_diff->m_SourceRemovedAssetIndexes, source_removed_count, sizeof(uint32_t), SortPathLongToShort, (void*)source_version);
    QSORT(version_diff->m_TargetAddedAssetIndexes, target_added_count, sizeof(uint32_t), SortPathShortToLong, (void*)target_version);

    Longtail_Free(work_mem);
    *out_version_diff = version_diff;
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
    const struct Longtail_ContentIndex* content_index,
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
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(source_version, "%p"),
        LONGTAIL_LOGFIELD(target_version, "%p"),
        LONGTAIL_LOGFIELD(version_diff, "%p"),
        LONGTAIL_LOGFIELD(version_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
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
    err = SafeCreateDir(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "SafeCreateDir() failed with %d", err)
        return err;
    }

    uint32_t remove_count = *version_diff->m_SourceRemovedCount;
    LONGTAIL_FATAL_ASSERT(ctx, remove_count <= *source_version->m_AssetCount, return EINVAL);
    if (remove_count > 0)
    {
        uint32_t* remove_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * remove_count);
        if (!remove_indexes)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            return ENOMEM;
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
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Opeation cancelled, failed with %d", ECANCELED)
                    return ECANCELED;
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
                        continue;
                    }
                    uint16_t permissions = 0;
                    err = version_storage_api->GetPermissions(version_storage_api, full_asset_path, &permissions);
                    if (err)
                    {
                        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->GetPermissions() failed with %d", err)
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
                            return err;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        continue;
                    }
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
                            Longtail_Free(full_asset_path);
                            Longtail_Free(remove_indexes);
                            return err;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        continue;
                    }
                }
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                remove_indexes[r] = 0xffffffff;
                ++successful_remove_count;
            }
        }
        Longtail_Free(remove_indexes);
    }

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t write_asset_count = added_count + modified_content_count;

    LONGTAIL_FATAL_ASSERT(ctx, write_asset_count <= *target_version->m_AssetCount, return EINVAL);
    if (write_asset_count > 0)
    {
        uint64_t chunk_count = *content_index->m_ChunkCount;
        size_t chunk_hash_to_block_index_size = Longtail_LookupTable_GetSize(chunk_count);
        size_t asset_indexes_size = sizeof(uint32_t) * write_asset_count;
        size_t work_mem_size = chunk_hash_to_block_index_size + asset_indexes_size;

        void* work_mem = Longtail_Alloc(work_mem_size);
        if (!work_mem)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) failed with %d",
                block_store_api, version_storage_api, hash_api, job_api, progress_api, optional_cancel_api, optional_cancel_token, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
                ENOMEM)
            return ENOMEM;
        }

        char* p = (char*)work_mem;
        struct Longtail_LookupTable* chunk_hash_to_block_index = Longtail_LookupTable_Create(p, chunk_count, 0);
        p += chunk_hash_to_block_index_size;
        uint32_t* asset_indexes = (uint32_t*)p;

        for (uint64_t i = 0; i < chunk_count; ++i)
        {
            TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
            uint64_t block_index = content_index->m_ChunkBlockIndexes[i];
            Longtail_LookupTable_PutUnique(chunk_hash_to_block_index, chunk_hash, block_index);
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
            content_index,
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

    if (retain_permissions)
    {
        uint32_t version_diff_modified_permissions_count = *version_diff->m_ModifiedPermissionsCount;
        for (uint32_t i = 0; i < version_diff_modified_permissions_count; ++i)
        {
            if ((i & 0x7f) == 0x7f) {
                if (optional_cancel_api && optional_cancel_token && optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
                {
                    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Operation cancelled, failed with %d", ECANCELED)
                    return ECANCELED;
                }
            }
            uint32_t asset_index = version_diff->m_TargetPermissionsModifiedAssetIndexes[i];
            const char* asset_path = &target_version->m_NameData[target_version->m_NameOffsets[asset_index]];
            char* full_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            uint16_t permissions = (uint16_t)target_version->m_Permissions[asset_index];
            err = version_storage_api->SetPermissions(version_storage_api, full_path, permissions);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "version_storage_api->SetPermissions() failed with %d", err)
                Longtail_Free(full_path);
                return err;
            }
            Longtail_Free(full_path);
        }
    }

    return err;
}

int Longtail_ValidateContent(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return EINVAL)

    uint64_t content_index_chunk_count = *content_index->m_ChunkCount;
    struct Longtail_LookupTable* content_chunk_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(content_index_chunk_count)), content_index_chunk_count ,0);
    if (!content_chunk_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
       return ENOMEM;
    }
    for (uint64_t chunk_index = 0; chunk_index < content_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        Longtail_LookupTable_Put(content_chunk_lookup, chunk_hash, chunk_index);
    }

    uint32_t chunk_missing_count = 0;
    uint32_t asset_size_mismatch_count = 0;

    uint64_t version_index_chunk_count = *version_index->m_ChunkCount;
    for (uint32_t chunk_index = 0; chunk_index < version_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        if (Longtail_LookupTable_Get(content_chunk_lookup, chunk_hash) == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ValidateContent(%p, %p) content index does not contain chunk 0x%" PRIx64 "",
                content_index, version_index,
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
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateContent(%p, %p) asset size for %s mismatch, accumulated chunks size: %" PRIu64 ", asset size:  %" PRIu64 "",
                content_index, version_index,
                asset_path, asset_chunked_size, asset_size)
            ++asset_size_mismatch_count;
        }
    }

    int err = 0;
    if (asset_size_mismatch_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateContent(%p, %p) has %u assets that does not match chunk sizes",
            content_index, version_index,
            asset_size_mismatch_count)
        err = EINVAL;
    }

    if (chunk_missing_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateContent(%p, %p) has %u missing chunks",
            content_index, version_index,
            chunk_missing_count)
        err = err ? err : ENOENT;
    }

    Longtail_Free(content_chunk_lookup);

    return err;
}

int Longtail_ValidateVersion(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, EINVAL)

    uint64_t version_index_chunk_count = *version_index->m_ChunkCount;
    struct Longtail_LookupTable* version_chunk_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(version_index_chunk_count)), version_index_chunk_count, 0);
    if (!version_chunk_lookup)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    for (uint32_t chunk_index = 0; chunk_index < version_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        Longtail_LookupTable_Put(version_chunk_lookup, chunk_hash, chunk_index);
    }

    uint32_t chunk_missing_count = 0;
    uint32_t asset_size_mismatch_count = 0;
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
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            asset_chunked_size += chunk_size;
        }
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        if ((asset_chunked_size != asset_size) && (!IsDirPath(asset_path)))
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateVersion(%p, %p) asset size for %s mismatch, accumulated chunks size: %" PRIu64 ", asset size:  %" PRIu64 "",
                content_index, version_index,
                asset_path, asset_chunked_size, asset_size)
            ++asset_size_mismatch_count;
        }
    }

    uint64_t content_index_chunk_count = *content_index->m_ChunkCount;
    for (uint64_t chunk_index = 0; chunk_index < content_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        if (Longtail_LookupTable_Get(version_chunk_lookup, chunk_hash) == 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateVersion(%p, %p) version index does not contain chunk 0x%" PRIx64 "",
                content_index, version_index,
                chunk_hash)
            ++chunk_missing_count;
            continue;
        }
    }

    Longtail_Free(version_chunk_lookup);
    version_chunk_lookup = 0;

    int err = 0;
    if (asset_size_mismatch_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateVersion(%p, %p) has %u assets that does not match chunk sizes",
            content_index, version_index,
            asset_size_mismatch_count)
        err = EINVAL;
    }

    if (chunk_missing_count > 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateVersion(%p, %p) has %u missing chunks",
            content_index, version_index,
            chunk_missing_count)
        err = err ? err : ENOENT;
    }

    return err;
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

    char* p = (char*)data;

    store_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

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
        return EBADF;
    }
    if (*store_index->m_Version != LONGTAIL_STORE_INDEX_VERSION_1_0_0)
    {
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    void* store_index_mem = (struct Longtail_StoreIndex*)Longtail_Alloc(store_index_size);
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

int Longtail_CreateStoreIndexFromContentIndex(
    const struct Longtail_ContentIndex* content_index,
    uint32_t blocks_tag,
    struct Longtail_StoreIndex** out_store_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(out_store_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t block_count = (uint32_t)*content_index->m_BlockCount;
    uint32_t chunk_count = (uint32_t)*content_index->m_ChunkCount;
    size_t store_index_size = Longtail_GetStoreIndexSize(block_count, chunk_count);
    void* store_index_mem = Longtail_Alloc(store_index_size);
    if (!store_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    struct Longtail_StoreIndex* store_index = Longtail_InitStoreIndex(store_index_mem, block_count, chunk_count);

    size_t block_hash_to_block_index_size = Longtail_LookupTable_GetSize(block_count);
    void* block_hash_to_block_index_mem = Longtail_Alloc(block_hash_to_block_index_size);
    if (!block_hash_to_block_index_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(store_index_mem);
        return ENOMEM;
    }
    struct Longtail_LookupTable* block_hash_to_block_index = Longtail_LookupTable_Create(block_hash_to_block_index_mem, block_count, 0);

    *store_index->m_Version = Longtail_CurrentStoreIndexVersion;
    *store_index->m_HashIdentifier = *content_index->m_HashIdentifier;
    *store_index->m_BlockCount = block_count;
    *store_index->m_ChunkCount = chunk_count;

    uint32_t current_block_index = 0;
    uint32_t block_write_index = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[c];
        uint64_t block_index = content_index->m_ChunkBlockIndexes[c];
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        uint64_t* block_index_ptr = Longtail_LookupTable_PutUnique(block_hash_to_block_index, block_hash, block_write_index);
        if (block_index_ptr)
        {
            current_block_index = (uint32_t)*block_index_ptr;
        }
        else
        {
            current_block_index = block_write_index++;
            store_index->m_BlockHashes[current_block_index] = block_hash;
            store_index->m_BlockTags[current_block_index] = blocks_tag;
            store_index->m_BlockChunksOffsets[current_block_index] = c;
            store_index->m_BlockChunkCounts[current_block_index] = 0;
        }
        store_index->m_ChunkHashes[c] = chunk_hash;
        store_index->m_ChunkSizes[c] = 0;
        store_index->m_BlockChunkCounts[current_block_index]++;
    }

    // Fake the chunk sizes
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        uint64_t block_index = content_index->m_ChunkBlockIndexes[c];
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        uint64_t write_block_index = *Longtail_LookupTable_Get(block_hash_to_block_index, block_hash);
        uint32_t chunks_in_block = store_index->m_BlockChunkCounts[write_block_index];
        store_index->m_ChunkSizes[c] = *content_index->m_MaxBlockSize / chunks_in_block;
    }

    Longtail_Free(block_hash_to_block_index);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, local_store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, local_store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    uint32_t hash_identifier = 0;
    uint32_t local_block_count = *local_store_index->m_BlockCount;
    uint32_t remote_block_count = *remote_store_index->m_BlockCount;
    size_t local_block_hash_to_index_size = Longtail_LookupTable_GetSize(local_block_count);
    size_t remote_block_hash_to_index_size = Longtail_LookupTable_GetSize(remote_block_count);
    size_t work_mem_size = local_block_hash_to_index_size + remote_block_hash_to_index_size;

    void* work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }

    char* p = (char*)work_mem;
    struct Longtail_LookupTable* local_block_hash_to_index = Longtail_LookupTable_Create(p, local_block_count, 0);
    p += local_block_hash_to_index_size;
    struct Longtail_LookupTable* remote_block_hash_to_index = Longtail_LookupTable_Create(p, remote_block_count, 0);

    uint32_t unique_block_count = 0;
    uint32_t chunk_count = 0;
    for (uint32_t local_block = 0; local_block < local_block_count; ++local_block)
    {
        TLongtail_Hash block_hash = local_store_index->m_BlockHashes[local_block];
        if (Longtail_LookupTable_PutUnique(local_block_hash_to_index, block_hash, local_block))
        {
            continue;
        }
        hash_identifier = *local_store_index->m_HashIdentifier;
        ++unique_block_count;
        chunk_count += local_store_index->m_BlockChunkCounts[local_block];
    }
    for (uint32_t remote_block = 0; remote_block < remote_block_count; ++remote_block)
    {
        TLongtail_Hash block_hash = remote_store_index->m_BlockHashes[remote_block];
        if (Longtail_LookupTable_Get(local_block_hash_to_index, block_hash))
        {
            continue;
        }
        if (Longtail_LookupTable_PutUnique(remote_block_hash_to_index, block_hash, remote_block))
        {
            continue;
        }
        hash_identifier = *remote_store_index->m_HashIdentifier;
        ++unique_block_count;
        chunk_count += remote_store_index->m_BlockChunkCounts[remote_block];
    }

    size_t merged_block_store_index_size = Longtail_GetStoreIndexSize(unique_block_count, chunk_count);
    void* merged_block_store_index_mem = Longtail_Alloc(merged_block_store_index_size);
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
    uint32_t block_index_offset = 0;
    uint32_t chunk_index_offset = 0;
    for (uint32_t local_block = 0; local_block < local_block_count; ++local_block)
    {
        TLongtail_Hash block_hash = local_store_index->m_BlockHashes[local_block];
        uint64_t* index_ptr = Longtail_LookupTable_Get(local_block_hash_to_index, block_hash);
        if (!index_ptr)
        {
            continue;
        }
        merged_store_index->m_BlockHashes[block_index_offset] = block_hash;
        uint32_t block_chunk_count = local_store_index->m_BlockChunkCounts[local_block];
        merged_store_index->m_BlockChunkCounts[block_index_offset] = block_chunk_count;
        merged_store_index->m_BlockChunksOffsets[block_index_offset] = chunk_index_offset;
        merged_store_index->m_BlockTags[block_index_offset] = local_store_index->m_BlockTags[local_block];
        uint32_t block_chunk_offset = local_store_index->m_BlockChunksOffsets[local_block];
        TLongtail_Hash* merged_chunk_hashes = &merged_store_index->m_ChunkHashes[chunk_index_offset];
        const TLongtail_Hash* local_chunk_hashes = &local_store_index->m_ChunkHashes[block_chunk_offset];
        memcpy(merged_chunk_hashes, local_chunk_hashes, sizeof(TLongtail_Hash) * block_chunk_count);
        uint32_t* merged_chunk_sizes = &merged_store_index->m_ChunkSizes[chunk_index_offset];
        const uint32_t* local_chunk_sizes = &local_store_index->m_ChunkSizes[block_chunk_offset];
        memcpy(merged_chunk_sizes, local_chunk_sizes, sizeof(uint32_t) * block_chunk_count);
        ++block_index_offset;
        chunk_index_offset += block_chunk_count;
    }
    for (uint32_t remote_block = 0; remote_block < remote_block_count; ++remote_block)
    {
        TLongtail_Hash block_hash = remote_store_index->m_BlockHashes[remote_block];
        if (Longtail_LookupTable_Get(local_block_hash_to_index, block_hash))
        {
            continue;
        }

        uint64_t* index_ptr = Longtail_LookupTable_Get(remote_block_hash_to_index, block_hash);
        if (!index_ptr)
        {
            continue;
        }

        merged_store_index->m_BlockHashes[block_index_offset] = block_hash;
        uint32_t block_chunk_count = remote_store_index->m_BlockChunkCounts[remote_block];
        merged_store_index->m_BlockChunkCounts[block_index_offset] = block_chunk_count;
        merged_store_index->m_BlockChunksOffsets[block_index_offset] = chunk_index_offset;
        merged_store_index->m_BlockTags[block_index_offset] = remote_store_index->m_BlockTags[remote_block];
        uint32_t block_chunk_offset = remote_store_index->m_BlockChunksOffsets[remote_block];
        TLongtail_Hash* merged_chunk_hashes = &merged_store_index->m_ChunkHashes[chunk_index_offset];
        const TLongtail_Hash* remote_chunk_hashes = &remote_store_index->m_ChunkHashes[block_chunk_offset];
        memcpy(merged_chunk_hashes, remote_chunk_hashes, sizeof(TLongtail_Hash) * block_chunk_count);
        uint32_t* merged_chunk_sizes = &merged_store_index->m_ChunkSizes[chunk_index_offset];
        const uint32_t* remote_chunk_sizes = &remote_store_index->m_ChunkSizes[block_chunk_offset];
        memcpy(merged_chunk_sizes, remote_chunk_sizes, sizeof(uint32_t) * block_chunk_count);
        ++block_index_offset;
        chunk_index_offset += block_chunk_count;
    }
    Longtail_Free(work_mem);
    *out_store_index = merged_store_index;
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetStoreIndexDataSize(*store_index->m_BlockCount, *store_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetStoreIndexDataSize() failed with %d", ENOMEM)
        return ENOMEM;
    }
    memcpy(*out_buffer, &store_index[1], index_data_size);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, store_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return EINVAL)

    size_t index_data_size = Longtail_GetStoreIndexDataSize(*store_index->m_BlockCount, *store_index->m_ChunkCount);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoreIndex(%p, %p, %s)",
            storage_api, store_index, path,
            err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoreIndex(%p, %p, %s)",
            storage_api, store_index, path,
            err)
        return err;
    }
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &store_index[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoreIndex(%p, %p, %s)",
            storage_api, store_index, path,
            err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;

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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_store_index != 0, return EINVAL)

    size_t store_index_size = sizeof(struct Longtail_StoreIndex) + size;
    struct Longtail_StoreIndex* store_index = (struct Longtail_StoreIndex*)Longtail_Alloc(store_index_size);
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

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
    struct Longtail_StoreIndex* store_index = (struct Longtail_StoreIndex*)Longtail_Alloc(store_index_size);
    if (!store_index)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        Longtail_Free(store_index);
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

uint32_t Longtail_ContentIndex_GetVersion(const struct Longtail_ContentIndex* content_index) { return *content_index->m_Version; }
uint32_t Longtail_ContentIndex_GetHashAPI(const struct Longtail_ContentIndex* content_index) { return *content_index->m_HashIdentifier; }
uint64_t Longtail_ContentIndex_GetBlockCount(const struct Longtail_ContentIndex* content_index) { return *content_index->m_BlockCount; }
uint64_t Longtail_ContentIndex_GetChunkCount(const struct Longtail_ContentIndex* content_index) { return *content_index->m_ChunkCount; }
const TLongtail_Hash* Longtail_ContentIndex_BlockHashes(const struct Longtail_ContentIndex* content_index) { return content_index->m_BlockHashes; }

uint32_t Longtail_VersionIndex_GetVersion(const struct Longtail_VersionIndex* content_index) { return *content_index->m_Version; }
uint32_t Longtail_VersionIndex_GetHashAPI(const struct Longtail_VersionIndex* content_index) { return *content_index->m_HashIdentifier; }
uint32_t Longtail_VersionIndex_GetAssetCount(const struct Longtail_VersionIndex* content_index) { return *content_index->m_AssetCount; }
uint32_t Longtail_VersionIndex_GetChunkCount(const struct Longtail_VersionIndex* content_index) { return *content_index->m_ChunkCount; }

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
