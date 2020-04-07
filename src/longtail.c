#include "longtail.h"

#if defined(__GNUC__) && !defined(__clang__) && !defined(APPLE) && !defined(__USE_GNU)
#define __USE_GNU
#endif

#include "ext/stb_ds.h"

#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>

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
#define LONGTAIL_VERSION_INDEX_VERSION_0_0_1  LONGTAIL_VERSION(0,0,1)
#define LONGTAIL_CONTENT_INDEX_VERSION_0_0_1  LONGTAIL_VERSION(0,0,1)

uint32_t Longtail_CurrentContentIndexVersion = LONGTAIL_VERSION_INDEX_VERSION_0_0_1;

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


uint64_t Longtail_GetPathFilterAPISize()
{
    return sizeof(struct Longtail_PathFilterAPI);
}

struct Longtail_PathFilterAPI* Longtail_MakePathFilterAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_PathFilter_IncludeFunc include_filter_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_PathFilterAPI* api = (struct Longtail_PathFilterAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->Include = include_filter_func;
    return api;
}

int Longtail_PathFilter_Include(struct Longtail_PathFilterAPI* path_filter_api, const char* root_path, const char* asset_folder, const char* asset_name, int is_dir, uint64_t size, uint16_t permissions)
{
    return path_filter_api->Include(path_filter_api, root_path, asset_folder, asset_name, is_dir, size, permissions);
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
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
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
    Longtail_Storage_GetFileNameFunc get_file_name_func,
    Longtail_Storage_GetDirectoryNameFunc get_directory_name_func,
    Longtail_Storage_GetEntryPropertiesFunc get_entry_properties_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_StorageAPI* api = (struct Longtail_StorageAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OpenReadFile = open_read_file_func;
    api->GetSize = get_size_func;
    api->Read = read_func;
    api->OpenWriteFile = open_write_file_func;
    api->Write = write_func;
    api->SetSize = set_size_func;
    api->SetPermissions = set_permissions_func;
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
    api->GetFileName = get_file_name_func;
    api->GetDirectoryName = get_directory_name_func;
    api->GetEntryProperties = get_entry_properties_func;
    return api;
}

int Longtail_Storage_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file) { return storage_api->OpenReadFile(storage_api, path, out_open_file); }
int Longtail_Storage_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size) { return storage_api->GetSize(storage_api, f, out_size); }
int Longtail_Storage_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output) { return storage_api->Read(storage_api, f, offset, length, output); }
int Longtail_Storage_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file) { return storage_api->OpenWriteFile(storage_api, path, initial_size, out_open_file); }
int Longtail_Storage_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input) { return storage_api->Write(storage_api, f, offset, length, input); }
int Longtail_Storage_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length) { return storage_api->SetSize(storage_api, f, length); }
int Longtail_Storage_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions) { return storage_api->SetPermissions(storage_api, path, permissions); }
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
const char* Longtail_Storage_GetFileName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { return storage_api->GetFileName(storage_api, iterator); }
const char* Longtail_Storage_GetDirectoryName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { return storage_api->GetDirectoryName(storage_api, iterator); }
int Longtail_Storage_GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, uint64_t* out_size, uint16_t* out_permissions) { return storage_api->GetEntryProperties(storage_api, iterator, out_size, out_permissions); }

uint64_t Longtail_GetProgressAPISize()
{
    return sizeof(struct Longtail_ProgressAPI);
}

struct Longtail_ProgressAPI* Longtail_MakeProgressAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Progress_OnProgressFunc on_progress_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_ProgressAPI* api = (struct Longtail_ProgressAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnProgress = on_progress_func;
    return api;
}

void Longtail_Progress_OnProgress(struct Longtail_ProgressAPI* progressAPI, uint32_t total_count, uint32_t done_count) { progressAPI->OnProgress(progressAPI, total_count, done_count); }

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
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
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
int Longtail_Job_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count) { return job_api->ReserveJobs(job_api, job_count); }
int Longtail_Job_CreateJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs) { return job_api->CreateJobs(job_api, job_count, job_funcs, job_contexts, out_jobs); }
int Longtail_Job_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs) { return job_api->AddDependecies(job_api, job_count, jobs, dependency_job_count, dependency_jobs); }
int Longtail_Job_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs) { return job_api->ReadyJobs(job_api, job_count, jobs); }
int Longtail_Job_WaitForAllJobs(struct Longtail_JobAPI* job_api, struct Longtail_ProgressAPI* progressAPI) { return job_api->WaitForAllJobs(job_api, progressAPI); }
int Longtail_Job_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id) { return job_api->ResumeJob(job_api, job_id); }

uint64_t Longtail_GetAsyncPutStoredBlockAPISize()
{
    return sizeof(struct Longtail_AsyncPutStoredBlockAPI);
}

struct Longtail_AsyncPutStoredBlockAPI* Longtail_MakeAsyncPutStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncPutStoredBlock_OnCompleteFunc on_complete_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_AsyncPutStoredBlockAPI* api = (struct Longtail_AsyncPutStoredBlockAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

int Longtail_AsyncPutStoredBlock_OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err) { return async_complete_api->OnComplete(async_complete_api, err); }

uint64_t Longtail_GetAsyncGetStoredBlockAPISize()
{
    return sizeof(struct Longtail_AsyncGetStoredBlockAPI);
}

struct Longtail_AsyncGetStoredBlockAPI* Longtail_MakeAsyncGetStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetStoredBlock_OnCompleteFunc on_complete_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_AsyncGetStoredBlockAPI* api = (struct Longtail_AsyncGetStoredBlockAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

int Longtail_AsyncGetStoredBlock_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err) { return async_complete_api->OnComplete(async_complete_api, stored_block, err); }

uint64_t Longtail_GetAsyncGetIndexAPISize()
{
    return sizeof(struct Longtail_AsyncGetIndexAPI);
}

struct Longtail_AsyncGetIndexAPI* Longtail_MakeAsyncGetIndexAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetIndex_OnCompleteFunc on_complete_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_AsyncGetIndexAPI* api = (struct Longtail_AsyncGetIndexAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->OnComplete = on_complete_func;
    return api;
}

int Longtail_AsyncGetIndex_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err) { return async_complete_api->OnComplete(async_complete_api, content_index, err); }

uint64_t Longtail_GetBlockStoreAPISize()
{
    return sizeof(struct Longtail_BlockStoreAPI);
}

struct Longtail_BlockStoreAPI* Longtail_MakeBlockStoreAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_BlockStore_PutStoredBlockFunc put_stored_block_func,
    Longtail_BlockStore_GetStoredBlockFunc get_stored_block_func,
    Longtail_BlockStore_GetIndexFunc get_index_func,
    Longtail_BlockStore_GetStatsFunc get_stats_func)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    struct Longtail_BlockStoreAPI* api = (struct Longtail_BlockStoreAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->PutStoredBlock = put_stored_block_func;
    api->GetStoredBlock = get_stored_block_func;
    api->GetIndex = get_index_func;
    api->GetStats = get_stats_func;
    return api;
}

int Longtail_BlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api) { return block_store_api->PutStoredBlock(block_store_api, stored_block, async_complete_api); }
int Longtail_BlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api) { return block_store_api->GetStoredBlock(block_store_api, block_hash, async_complete_api); }
int Longtail_BlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, struct Longtail_AsyncGetIndexAPI* async_complete_api) { return block_store_api->GetIndex(block_store_api, default_hash_api_identifier, async_complete_api); }
int Longtail_BlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats) { return block_store_api->GetStats(block_store_api, out_stats); }

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
    return Longtail_Alloc_private ? Longtail_Alloc_private(s) : malloc(s);
}

void Longtail_Free(void* p)
{
    Free_private ? Free_private(p) : free(p);
}

#if !defined(LONGTAIL_LOG_LEVEL)
    #define LONGTAIL_LOG_LEVEL   0
#endif

static Longtail_Log Longtail_Log_private = 0;
static void* Longtail_LogContext = 0;
static int Longtail_LogLevel_private = LONGTAIL_LOG_LEVEL;

void Longtail_SetLog(Longtail_Log log_func, void* context)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_SetLog(%p, %p)", (void*)log_func, context)
    Longtail_Log_private = log_func;
    Longtail_LogContext = context;
}

void Longtail_SetLogLevel(int level)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_SetLogLevel(%d)", level)
    Longtail_LogLevel_private = level;
}

void Longtail_CallLogger(int level, const char* fmt, ...)
{
    LONGTAIL_FATAL_ASSERT(fmt != 0, return)
    if (!Longtail_Log_private || (level < Longtail_LogLevel_private))
    {
        return;
    }
    va_list argptr;
    va_start(argptr, fmt);
    char buffer[2048];
    vsprintf(buffer, fmt, argptr);
    va_end(argptr);
    Longtail_Log_private(Longtail_LogContext, level, buffer);
}

char* Longtail_Strdup(const char* path)
{
    LONGTAIL_VALIDATE_INPUT(path != 0, return 0)
    char* r = (char*)Longtail_Alloc(strlen(path) + 1);
    if (!r)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Strdup(%s) failed with %d", path, ENOMEM)
        return 0;
    }
    strcpy(r, path);
    return r;
}

static int IsDirPath(const char* path)
{
    LONGTAIL_VALIDATE_INPUT(path != 0, return 0)
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

static int GetPathHash(struct Longtail_HashAPI* hash_api, const char* path, TLongtail_Hash* out_hash)
{
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_hash != 0, return EINVAL)
    uint64_t hash;
    int err = hash_api->HashBuffer(hash_api, (uint32_t)strlen(path), (void*)path, &hash);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "GetPathHash(%p, %s, %p) hash_api->HashBuffer() failed with %d", (void*)hash_api, path, (void*)out_hash, err)
        return err;
    }
    *out_hash = (TLongtail_Hash)hash;
    return 0;
}

static int SafeCreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)
    int err = storage_api->CreateDir(storage_api, path);
    if (!err)
    {
        return 0;
    }
    if (storage_api->IsDir(storage_api, path))
    {
        return 0;
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "SafeCreateDir(%p, %s) failed with %d", (void*)storage_api, path, err)
    return err;
}

int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    char* dir_path = Longtail_Strdup(path);
    if (!dir_path)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists(%p ,%s) Longtail_Strdup(%s) failed with %d", (void*)storage_api, path, path, ENOMEM)
        return ENOMEM;
    }
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 0;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 0;
    }

    int err = EnsureParentPathExists(storage_api, dir_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists(%p ,%s) EnsureParentPathExists(%s) failed with %d", (void*)storage_api, path, dir_path, err)
        Longtail_Free(dir_path);
        dir_path = 0;
        return err;
    }
    err = SafeCreateDir(storage_api, dir_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists(%p ,%s) SafeCreateDir(%p, %s) failed with %d", (void*)storage_api, path, (void*)storage_api, dir_path, err)
        Longtail_Free(dir_path);
        dir_path = 0;
        return err;
    }

    Longtail_Free(dir_path);
    dir_path = 0;
    return 0;
}







struct HashToIndexItem
{
    TLongtail_Hash key;
    uint64_t value;
};

typedef int (*ProcessEntry)(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size, uint16_t permissions);

static int RecurseTree(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    const char* root_folder,
    ProcessEntry entry_processor,
    void* context)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_folder != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(entry_processor != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RecurseTree: Scanning folder %s", root_folder)

    char* root_folder_copy = Longtail_Strdup(root_folder);
    if (!root_folder_copy)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "RecurseTree(%p, %s, %p, %p) Longtail_Strdup(%s) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, root_folder, ENOMEM)
        return ENOMEM;
    }
    uint32_t folder_index = 0;

    char** folder_paths = 0;
    arrsetcap(folder_paths, 256);

    arrput(folder_paths, root_folder_copy);

    int err = 0;
    while (folder_index < (uint32_t)arrlen(folder_paths))
    {
        const char* asset_folder = folder_paths[folder_index++];

        Longtail_StorageAPI_HIterator fs_iterator = 0;
        err = storage_api->StartFind(storage_api, asset_folder, &fs_iterator);
        if (err == ENOENT)
        {
            Longtail_Free((void*)asset_folder);
            asset_folder = 0;
            err = 0;
            continue;
        }
        else if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree(%p, %s, %p, %p) storage_api->StartFind(%s) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, root_folder, asset_folder, err)
            Longtail_Free((void*)asset_folder);
            asset_folder = 0;
            break;
        }
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RecurseTree(%p, %s, %p, %p) storage_api->StartFind(%p, %s, %p)", (void*)storage_api, root_folder, (void*)entry_processor, context, storage_api, asset_folder, &fs_iterator)
        while(err == 0)
        {
            const char* dir_name = storage_api->GetDirectoryName(storage_api, fs_iterator);
            if (dir_name)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RecurseTree(%p, %s, %p, %p) storage_api->GetDirectoryName(%p, %s) found directory `%s` in `%s`", (void*)storage_api, root_folder, (void*)entry_processor, context, storage_api, fs_iterator, dir_name, asset_folder)
                uint64_t size;
                uint16_t permissions;
                err = storage_api->GetEntryProperties(storage_api, fs_iterator, &size, &permissions);
                if (err)
                {
                    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree(%p, %s, %p, %p) storage_api->GetEntryProperties(%p, %s, %p, %p) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, (void*)storage_api, dir_name, (void*)&size, (void*)&permissions, err)
                    break;
                }
                if (!optional_path_filter_api || optional_path_filter_api->Include(optional_path_filter_api, root_folder, asset_folder, dir_name, 1, size, permissions))
                {
                    err = entry_processor(context, asset_folder, dir_name, 1, size, permissions);
                    if (err)
                    {
                        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree(%p, %s, %p, %p) entry_processor(%p, %s, %s, 1, %" PRIu64 ", %u) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, context, asset_folder, dir_name, size, permissions, err)
                        break;
                    }
                    if ((size_t)arrlen(folder_paths) == arrcap(folder_paths))
                    {
                        if (folder_index > 0)
                        {
                            uint32_t unprocessed_count = (uint32_t)(arrlen(folder_paths) - folder_index);
                            memmove(folder_paths, &folder_paths[folder_index], sizeof(const char*) * unprocessed_count);
                            arrsetlen(folder_paths, unprocessed_count);
                            folder_index = 0;
                        }
                    }
                    arrput(folder_paths, storage_api->ConcatPath(storage_api, asset_folder, dir_name));
                }
            }
            else
            {
                const char* file_name = storage_api->GetFileName(storage_api, fs_iterator);
                if (file_name)
                {
                    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "RecurseTree(%p, %s, %p, %p) storage_api->GetFileName(%p, %s) found file `%s` in `%s`", (void*)storage_api, root_folder, (void*)entry_processor, context, storage_api, fs_iterator, file_name, asset_folder)
                    uint64_t size;
                    uint16_t permissions;
                    err = storage_api->GetEntryProperties(storage_api, fs_iterator, &size, &permissions);
                    if (err)
                    {
                        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree(%p, %s, %p, %p) storage_api->GetEntryProperties(%p, %s, %p, %p) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, (void*)storage_api, file_name, (void*)&size, (void*)&permissions, err)
                        break;
                    }
                    if (!optional_path_filter_api || optional_path_filter_api->Include(optional_path_filter_api, root_folder, asset_folder, file_name, 0, size, permissions))
                    {
                        err = entry_processor(context, asset_folder, file_name, 0, size, permissions);
                        if (err)
                        {
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree(%p, %s, %p, %p) entry_processor(%p, %s, %s, 0, %" PRIu64 ", %u) failed with %d", (void*)storage_api, root_folder, (void*)entry_processor, context, context, asset_folder, file_name, size, permissions, err)
                            break;
                        }
                    }
                }
            }
            err = storage_api->FindNext(storage_api, fs_iterator);
            if (err == ENOENT)
            {
                err = 0;
                break;
            }
        }
        storage_api->CloseFind(storage_api, fs_iterator);
        Longtail_Free((void*)asset_folder);
        asset_folder = 0;
    }
    while (folder_index < (uint32_t)arrlen(folder_paths))
    {
        const char* asset_folder = folder_paths[folder_index++];
        Longtail_Free((void*)asset_folder);
    }
    arrfree(folder_paths);
    folder_paths = 0;
    return err;
}

static size_t GetPathsSize(uint32_t path_count, uint32_t path_data_size)
{
    return sizeof(struct Longtail_Paths) +
        sizeof(uint32_t) +                 // PathCount
        sizeof(uint32_t) * path_count +    // m_Offsets[path_count]
        path_data_size;
};

static struct Longtail_Paths* CreatePaths(uint32_t path_count, uint32_t path_data_size)
{
    LONGTAIL_FATAL_ASSERT((path_count == 0 && path_data_size == 0) || (path_count > 0 && path_data_size > path_count), return 0)
    size_t paths_size = GetPathsSize(path_count, path_data_size);
    struct Longtail_Paths* paths = (struct Longtail_Paths*)Longtail_Alloc(paths_size);
    if (!paths)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CreatePaths(`%u`, `%u`) Longtail_Alloc(%" PRIu64 ") failed with %d", path_count, path_data_size, paths_size, ENOMEM)
        return 0;
    }
    char* p = (char*)&paths[1];
    paths->m_DataSize = 0;
    paths->m_PathCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);
    paths->m_Offsets = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * path_count;
    paths->m_Data = p;
    *paths->m_PathCount = 0;
    return paths;
};

int Longtail_MakePaths(uint32_t path_count, const char* const* path_names, struct Longtail_Paths** out_paths)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_MakePaths(%u, %p, %p)", path_count, (void*)path_names, (void*)out_paths)
    LONGTAIL_VALIDATE_INPUT((path_count == 0 && path_names == 0) || (path_count > 0 && path_names != 0), return 0)
    LONGTAIL_VALIDATE_INPUT(out_paths != 0, return 0)

    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    struct Longtail_Paths* paths = CreatePaths(path_count, name_data_size);
    if (paths == 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MakePaths(%u, %p, %p) CreatePaths(%u, %u) failed with %d", path_count, (void*)path_names, (void*)out_paths, path_count, name_data_size, ENOMEM)
        return ENOMEM;
    }
    uint32_t offset = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        uint32_t length = (uint32_t)strlen(path_names[i]) + 1;
        paths->m_Offsets[i] = offset;
        memmove(&paths->m_Data[offset], path_names[i], length);
        offset += length;
    }
    paths->m_DataSize = offset;
    *paths->m_PathCount = path_count;
    *out_paths = paths;
    return 0;
}

static int AppendPath(struct Longtail_Paths** paths, const char* path, uint32_t* max_path_count, uint32_t* max_data_size, uint32_t path_count_increment, uint32_t data_size_increment)
{
    LONGTAIL_FATAL_ASSERT((*paths) != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_path_count != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_data_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path_count_increment > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data_size_increment > 0, return EINVAL)
    uint32_t path_size = (uint32_t)(strlen(path) + 1);

    int out_of_path_data = (*paths)->m_DataSize + path_size > *max_data_size;
    int out_of_path_count = *(*paths)->m_PathCount >= *max_path_count;
    if (out_of_path_count | out_of_path_data)
    {
        uint32_t extra_path_count = out_of_path_count ? path_count_increment : 0;
        uint32_t extra_path_data_size = out_of_path_data ? (path_count_increment * data_size_increment) : 0;

        const uint32_t new_path_count = *max_path_count + extra_path_count;
        const uint32_t new_path_data_size = *max_data_size + extra_path_data_size;
        struct Longtail_Paths* new_paths = CreatePaths(new_path_count, new_path_data_size);
        if (new_paths == 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "AppendPath(%p, %s, %p, %p, %u, %u) CreatePaths(%u, %u) failed with %d", (void*)paths, path, (void*)max_path_count, (void*)max_data_size, path_count_increment, data_size_increment, new_path_count, new_path_data_size, ENOMEM)
            return ENOMEM;
        }
        *max_path_count = new_path_count;
        *max_data_size = new_path_data_size;
        new_paths->m_DataSize = (*paths)->m_DataSize;
        *new_paths->m_PathCount = *(*paths)->m_PathCount;

        memmove(new_paths->m_Offsets, (*paths)->m_Offsets, sizeof(uint32_t) * *(*paths)->m_PathCount);
        memmove(new_paths->m_Data, (*paths)->m_Data, (*paths)->m_DataSize);

        Longtail_Free(*paths);
        *paths = new_paths;
    }

    memmove(&(*paths)->m_Data[(*paths)->m_DataSize], path, path_size);
    (*paths)->m_Offsets[*(*paths)->m_PathCount] = (*paths)->m_DataSize;
    (*paths)->m_DataSize += path_size;
    (*(*paths)->m_PathCount)++;

    return 0;
}

struct AddFile_Context {
    struct Longtail_StorageAPI* m_StorageAPI;
    uint32_t m_ReservedPathCount;
    uint32_t m_ReservedPathSize;
    uint32_t m_RootPathLength;
    struct Longtail_Paths* m_Paths;
    uint64_t* m_FileSizes;
    uint32_t* m_Permissions;
};

static int AddFile(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size, uint16_t permissions)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(file_name != 0, return EINVAL)
    struct AddFile_Context* paths_context = (struct AddFile_Context*)context;
    struct Longtail_StorageAPI* storage_api = paths_context->m_StorageAPI;

    char* full_path = storage_api->ConcatPath(storage_api, root_path, file_name);
    if (is_dir)
    {
        uint32_t path_length = (uint32_t)strlen(full_path);
        size_t full_dir_path_size = path_length + 1 + 1;
        char* full_dir_path = (char*)Longtail_Alloc(full_dir_path_size);
        if (!full_dir_path)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "AddFile(%p, %s, %s, %d, %" PRIu64 ", %u) Longtail_Alloc(%" PRIu64 ") failed with %d", context, root_path, file_name, is_dir, size, permissions, full_dir_path_size, ENOMEM)
            return ENOMEM;
        }
        strcpy(full_dir_path, full_path);
        strcpy(&full_dir_path[path_length], "/");
        Longtail_Free(full_path);
        full_path = full_dir_path;
    }

    const uint32_t root_path_length = paths_context->m_RootPathLength;
    const char* s = &full_path[root_path_length];
    if (*s == '/')
    {
        ++s;
    }

    int err = AppendPath(&paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "AddFile(%p, %s, %s, %d, %u, %u) AppendPath(%p, %s, %p, %p, %u, %u) failed with %d", context, root_path, file_name, is_dir, size, permissions, &paths_context->m_Paths, s, &paths_context->m_ReservedPathCount, &paths_context->m_ReservedPathSize, 512, 128, err)
        Longtail_Free(full_path);
        return err;
    }

    arrpush(paths_context->m_FileSizes, size);
    arrpush(paths_context->m_Permissions, (uint32_t)permissions);

    Longtail_Free(full_path);
    full_path = 0;
    return 0;
}

int Longtail_GetFilesRecursively(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_PathFilterAPI* optional_path_filter_api,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetFilesRecursively(%p, %s, %p)", storage_api, root_path, out_file_infos)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_file_infos != 0, return EINVAL)

    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Longtail_Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    if (!paths)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetFilesRecursively(%p, %s, %p) CreatePaths(%u, %u) failed with %d", storage_api, root_path, out_file_infos, default_path_count, default_path_data_size, ENOMEM)
        return ENOMEM;
    }
    struct AddFile_Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), paths, 0};
    paths = 0;
    arrsetcap(context.m_FileSizes, 4096);
    arrsetcap(context.m_Permissions, 4096);

    int err = RecurseTree(storage_api, optional_path_filter_api, root_path, AddFile, &context);
    if(err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetFilesRecursively(%p, %s, %p) RecurseTree(%p, %s, %p, %p) failed with %d", storage_api, root_path, out_file_infos, (void*)storage_api, root_path, (void*)AddFile, (void*)&context, err)
        Longtail_Free(context.m_Paths);
        context.m_Paths = 0;
        arrfree(context.m_Permissions);
        context.m_Permissions = 0;
        arrfree(context.m_FileSizes);
        context.m_FileSizes = 0;
        return err;
    }

    uint32_t asset_count = *context.m_Paths->m_PathCount;
    size_t file_infos_size = sizeof(struct Longtail_FileInfos) +
        sizeof(uint64_t) * asset_count +    // Asset sizes
        sizeof(uint32_t) * asset_count +    // Permissions
        GetPathsSize(asset_count, context.m_Paths->m_DataSize);
    struct Longtail_FileInfos* result = (struct Longtail_FileInfos*)Longtail_Alloc(file_infos_size);
    if (!result)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetFilesRecursively(%p, %s, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", storage_api, root_path, out_file_infos, file_infos_size, ENOMEM)
        Longtail_Free(context.m_Paths);
        context.m_Paths = 0;
        arrfree(context.m_Permissions);
        context.m_Permissions = 0;
        arrfree(context.m_FileSizes);
        context.m_FileSizes = 0;
        return ENOMEM;
    }

    result->m_Paths.m_DataSize = context.m_Paths->m_DataSize;
    result->m_Paths.m_PathCount = (uint32_t*)(void*)&result[1];
    *result->m_Paths.m_PathCount = asset_count;
    result->m_FileSizes = (uint64_t*)(void*)&result->m_Paths.m_PathCount[1];
    result->m_Permissions = (uint32_t*)(void*)&result->m_FileSizes[asset_count];
    result->m_Paths.m_Offsets = (uint32_t*)(void*)(&result->m_Permissions[asset_count]);
    result->m_Paths.m_Data = (char*)&result->m_Paths.m_Offsets[asset_count];
    memmove(result->m_FileSizes, context.m_FileSizes, sizeof(uint64_t) * asset_count);
    memmove(result->m_Permissions, context.m_Permissions, sizeof(uint32_t) * asset_count);
    memmove(result->m_Paths.m_Offsets, context.m_Paths->m_Offsets, sizeof(uint32_t) * asset_count);
    memmove(result->m_Paths.m_Data, context.m_Paths->m_Data, result->m_Paths.m_DataSize);

    Longtail_Free(context.m_Paths);
    context.m_Paths = 0;
    arrfree(context.m_Permissions);
    context.m_Permissions = 0;
    arrfree(context.m_FileSizes);
    context.m_FileSizes = 0;

    *out_file_infos = result;
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

static int StorageChunkFeederFunc(void* context, struct Longtail_Chunker* chunker, uint32_t requested_size, char* buffer, uint32_t* out_size)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunker != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(requested_size > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_size != 0, return EINVAL)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StorageChunkFeederFunc(%p, %p, %u, %p, %p) m_StorageAPI->Read(%p, %s, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", context, (void*)chunker, requested_size, (void*)buffer, (void*)out_size, (void*)c->m_StorageAPI, c->m_AssetPath, c->m_StartRange + c->m_Offset, read_count, (void*)buffer, err)
            return err;
        }
        c->m_Offset += read_count;
    }
    *out_size = (uint32_t)read_count;
    return 0;
}

// ChunkerWindowSize is the number of bytes in the rolling hash window
#define ChunkerWindowSize 48u

#define MIN_CHUNKER_SIZE(target_chunk_size) (((target_chunk_size / 8) < ChunkerWindowSize) ? ChunkerWindowSize : (target_chunk_size / 8))
#define AVG_CHUNKER_SIZE(target_chunk_size) (((target_chunk_size / 2) < ChunkerWindowSize) ? ChunkerWindowSize : (target_chunk_size / 2))
#define MAX_CHUNKER_SIZE(target_chunk_size) (target_chunk_size * 2)


struct HashJob
{
    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_HashAPI* m_HashAPI;
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

static int DynamicChunking(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Err = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path, hash_job->m_PathHash);
    if (hash_job->m_Err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) GetPathHash(%p, %s, %p) failed with %d", context, job_id, (void*)hash_job->m_HashAPI, hash_job->m_Path, (void*)hash_job->m_PathHash, hash_job->m_Err)
        return 0;
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) storage_api->OpenReadFile(%p, %s, %p) failed with %d", context, job_id, (void*)storage_api, path, (void*)&file_handle, err)
        Longtail_Free(path);
        path = 0;
        hash_job->m_Err = err;
        return 0;
    }

    uint64_t hash_size = hash_job->m_SizeRange;
    TLongtail_Hash content_hash = 0;
    if (hash_size == 0)
    {
        content_hash = 0;
    }
    else if (hash_size <= ChunkerWindowSize || hash_job->m_TargetChunkSize <= ChunkerWindowSize)
    {
        char* buffer = (char*)Longtail_Alloc((size_t)hash_size);
        if (!buffer)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) Longtail_Alloc(%" PRIu64 ") failed with %d", context, job_id, hash_size, err)
            storage_api->CloseFile(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            hash_job->m_Err = err;
            return 0;
        }
        err = storage_api->Read(storage_api, file_handle, 0, hash_size, buffer);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) storage_api->Read(%p, %s, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", context, job_id, (void*)storage_api, path, 0, hash_size, (void*)buffer, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) m_HashAPI->HashBuffer(%p, %u, %p, %p) failed with %d", context, job_id, (void*)hash_job->m_HashAPI, hash_size, (void*)buffer, (void*)&hash_job->m_ChunkHashes[chunk_count], err)
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
        uint32_t min_chunk_size = MIN_CHUNKER_SIZE(hash_job->m_TargetChunkSize);
        uint32_t avg_chunk_size = AVG_CHUNKER_SIZE(hash_job->m_TargetChunkSize);
        uint32_t max_chunk_size = MAX_CHUNKER_SIZE(hash_job->m_TargetChunkSize);

        struct StorageChunkFeederContext feeder_context =
        {
            storage_api,
            file_handle,
            path,
            hash_job->m_StartRange,
            hash_size,
            0
        };

        struct Longtail_ChunkerParams chunker_params = { min_chunk_size, avg_chunk_size, max_chunk_size };

        struct Longtail_Chunker* chunker;
        err = Longtail_CreateChunker(
            &chunker_params,
            StorageChunkFeederFunc,
            &feeder_context,
            &chunker);

        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) Longtail_CreateChunker(%p, %p, %p, %p) failed with %d", context, job_id, (void*)&chunker_params, (void*)StorageChunkFeederFunc, (void*)&feeder_context, (void*)&chunker, err)
            storage_api->CloseFile(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            hash_job->m_Err = err;
            return 0;
        }

        Longtail_HashAPI_HContext asset_hash_context;
        err = hash_job->m_HashAPI->BeginContext(hash_job->m_HashAPI, &asset_hash_context);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) m_HashAPI->BeginContext(%p, %p) failed with %d", context, job_id, (void*)hash_job->m_HashAPI, (void*)&asset_hash_context, err)
            storage_api->CloseFile(storage_api, file_handle);
            file_handle = 0;
            Longtail_Free(path);
            path = 0;
            hash_job->m_Err = err;
            return 0;
        }

        uint64_t remaining = hash_size;
        struct Longtail_ChunkRange r = Longtail_NextChunk(chunker);
        while (r.len)
        {
            err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, r.len, (void*)r.buf, &hash_job->m_ChunkHashes[chunk_count]);
            if (err != 0)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking(%p, %u) m_HashAPI->HashBuffer(%p, %u, %p, %p) for %s failed with %d", context, job_id, (void*)hash_job->m_HashAPI, r.len, (void*)r.buf, (void*)&hash_job->m_ChunkHashes[chunk_count], err)
                Longtail_Free(chunker);
                chunker = 0;
                hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
                storage_api->CloseFile(storage_api, file_handle);
                file_handle = 0;
                Longtail_Free(path);
                path = 0;
                hash_job->m_Err = err;
                return 0;
            }
            hash_job->m_ChunkSizes[chunk_count] = r.len;
            hash_job->m_ChunkTags[chunk_count] = hash_job->m_ContentTag;

            ++chunk_count;
            hash_job->m_HashAPI->Hash(hash_job->m_HashAPI, asset_hash_context, r.len, (void*)r.buf);

            remaining -= r.len;
            r = Longtail_NextChunk(chunker);
        }

        content_hash = hash_job->m_HashAPI->EndContext(hash_job->m_HashAPI, asset_hash_context);
        Longtail_Free(chunker);
        chunker = 0;
    }

    storage_api->CloseFile(storage_api, file_handle);
    file_handle = 0;
    
    LONGTAIL_FATAL_ASSERT(chunk_count <= hash_job->m_MaxChunkCount, hash_job->m_Err = EINVAL; return 0)
    *hash_job->m_AssetChunkCount = chunk_count;

    Longtail_Free((char*)path);
    path = 0;

    hash_job->m_Err = 0;
    return 0;
}

static int ChunkAssets(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const char* root_path,
    const struct Longtail_Paths* paths,
    TLongtail_Hash* path_hashes,
    TLongtail_Hash* content_hashes,
    const uint64_t* content_sizes,
    const uint32_t* content_tags,
    uint32_t* asset_chunk_start_index,
    uint32_t* asset_chunk_counts,
    uint32_t** chunk_sizes,
    TLongtail_Hash** chunk_hashes,
    uint32_t** chunk_tags,
    uint32_t target_chunk_size,
    uint32_t* chunk_count)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(paths != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path_hashes != 0, return EINVAL)

    LONGTAIL_FATAL_ASSERT(content_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_sizes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_chunk_start_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_sizes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_tags != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(target_chunk_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count != 0, return EINVAL)

    uint32_t asset_count = *paths->m_PathCount;

    uint64_t max_hash_size = target_chunk_size * 1024;
    uint32_t job_count = 0;

    uint64_t min_chunk_size = MIN_CHUNKER_SIZE(target_chunk_size);

    uint64_t max_chunk_count = 0;
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = content_sizes[asset_index];
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

    int err = job_api->ReserveJobs(job_api, job_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "ChunkAssets(%s) job_api->ReserveJobs(%p, %u) failed with %d", root_path, (void*)job_api, job_count, err)
        return err;
    }

    size_t job_chunk_counts_size = sizeof(uint32_t) * job_count;
    uint32_t* job_chunk_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * job_count);
    if (!job_chunk_counts)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, job_chunk_counts_size, err)
        return err;
    }

    size_t hashes_size = sizeof(TLongtail_Hash) * max_chunk_count;
    TLongtail_Hash* hashes = (TLongtail_Hash*)Longtail_Alloc(hashes_size);
    if (!hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, hashes_size, err)
        Longtail_Free(job_chunk_counts);
        job_chunk_counts = 0;
        return err;
    }

    size_t sizes_size = sizeof(uint32_t) * max_chunk_count;
    uint32_t* sizes = (uint32_t*)Longtail_Alloc(sizes_size);
    if (!sizes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, sizes_size, err)
        Longtail_Free(hashes);
        hashes = 0;
        Longtail_Free(job_chunk_counts);
        job_chunk_counts = 0;
        return err;
    }

    size_t tags_size = sizeof(uint32_t) * max_chunk_count;
    uint32_t* tags = (uint32_t*)Longtail_Alloc(tags_size);
    if (!tags)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, tags_size, err)
        Longtail_Free(sizes);
        sizes = 0;
        Longtail_Free(hashes);
        hashes = 0;
        Longtail_Free(job_chunk_counts);
        job_chunk_counts = 0;
        return err;
    }

    size_t hash_jobs_size = sizeof(struct HashJob) * job_count;
    struct HashJob* hash_jobs = (struct HashJob*)Longtail_Alloc(hash_jobs_size);
    if (!hash_jobs)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_jobs_size, err)
        Longtail_Free(tags);
        tags = 0;
        Longtail_Free(sizes);
        sizes = 0;
        Longtail_Free(hashes);
        hashes = 0;
        Longtail_Free(job_chunk_counts);
        job_chunk_counts = 0;
        return err;
    }

    uint64_t jobs_started = 0;
    uint64_t chunks_offset = 0;
    for (uint32_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        uint64_t asset_size = content_sizes[asset_index];
        uint64_t asset_part_count = 1 + (asset_size / max_hash_size);

        for (uint64_t job_part = 0; job_part < asset_part_count; ++job_part)
        {
            LONGTAIL_FATAL_ASSERT(jobs_started < job_count, return EINVAL)

            uint64_t range_start = job_part * max_hash_size;
            uint64_t job_size = (asset_size - range_start) > max_hash_size ? max_hash_size : (asset_size - range_start);

            uint32_t asset_max_chunk_count = (uint32_t)(job_size == 0 ? 0 : 1 + (job_size / min_chunk_size));

            struct HashJob* job = &hash_jobs[jobs_started];
            job->m_StorageAPI = storage_api;
            job->m_HashAPI = hash_api;
            job->m_RootPath = root_path;
            job->m_Path = &paths->m_Data[paths->m_Offsets[asset_index]];
            job->m_PathHash = &path_hashes[asset_index];
            job->m_AssetIndex = asset_index;
            job->m_StartRange = range_start;
            job->m_SizeRange = job_size;
            job->m_ContentTag = content_tags[asset_index];
            job->m_MaxChunkCount = asset_max_chunk_count;
            job->m_AssetChunkCount = &job_chunk_counts[jobs_started];
            job->m_ChunkHashes = &hashes[chunks_offset];
            job->m_ChunkSizes = &sizes[chunks_offset];
            job->m_ChunkTags = &tags[chunks_offset];
            job->m_TargetChunkSize = target_chunk_size;
            job->m_Err = EINVAL;

            Longtail_JobAPI_JobFunc func[1] = {DynamicChunking};
            void* ctx[1] = {&hash_jobs[jobs_started]};

            Longtail_JobAPI_Jobs jobs;
            err = job_api->CreateJobs(job_api, 1, func, ctx, &jobs);
            LONGTAIL_FATAL_ASSERT(!err, return err)
            err = job_api->ReadyJobs(job_api, 1, jobs);
            LONGTAIL_FATAL_ASSERT(!err, return err)

            jobs_started++;

            chunks_offset += asset_max_chunk_count;
        }
    }

    err = job_api->WaitForAllJobs(job_api, progress_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) job_api->WaitForAllJobs(%p, %p) failed with %d", root_path, (void*)job_api, (void*)progress_api, err)
        Longtail_Free(tags);
        tags = 0;
        Longtail_Free(hash_jobs);
        hash_jobs = 0;
        Longtail_Free(hashes);
        hashes = 0;
        Longtail_Free(sizes);
        sizes = 0;
        Longtail_Free(job_chunk_counts);
        job_chunk_counts = 0;
        return err;
    }
    LONGTAIL_FATAL_ASSERT(!err, return err)

    err = 0;
    for (uint32_t i = 0; i < jobs_started; ++i)
    {
        if (hash_jobs[i].m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "ChunkAssets(%s) Failed to hash %s, %d", root_path, hash_jobs[i].m_Path, hash_jobs[i].m_Err)
            err = err ? err : hash_jobs[i].m_Err;
        }
    }

    if (!err)
    {
        uint32_t built_chunk_count = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            LONGTAIL_FATAL_ASSERT(*hash_jobs[i].m_AssetChunkCount <= hash_jobs[i].m_MaxChunkCount, return EINVAL)
            built_chunk_count += *hash_jobs[i].m_AssetChunkCount;
        }
        *chunk_count = built_chunk_count;
        size_t chunk_sizes_size = sizeof(uint32_t) * *chunk_count;
        *chunk_sizes = (uint32_t*)Longtail_Alloc(chunk_sizes_size);
        if (!*chunk_sizes)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, chunk_sizes_size, ENOMEM)
            Longtail_Free(tags);
            tags = 0;
            Longtail_Free(hash_jobs);
            hash_jobs = 0;
            Longtail_Free(hashes);
            hashes = 0;
            Longtail_Free(sizes);
            sizes = 0;
            Longtail_Free(job_chunk_counts);
            job_chunk_counts = 0;
            return ENOMEM;
        }
        size_t chunk_hashes_size = sizeof(TLongtail_Hash) * *chunk_count;
        *chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(chunk_hashes_size);
        if (!*chunk_hashes)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, chunk_hashes_size, ENOMEM)
            Longtail_Free(*chunk_sizes);
            *chunk_sizes = 0;
            Longtail_Free(tags);
            tags = 0;
            Longtail_Free(hash_jobs);
            hash_jobs = 0;
            Longtail_Free(hashes);
            hashes = 0;
            Longtail_Free(sizes);
            sizes = 0;
            Longtail_Free(job_chunk_counts);
            job_chunk_counts = 0;
            return ENOMEM;
        }
        size_t chunk_tags_size = sizeof(uint32_t) * *chunk_count;
        *chunk_tags = (uint32_t*)Longtail_Alloc(chunk_tags_size);
        if (!*chunk_tags)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, chunk_tags_size, ENOMEM)
            Longtail_Free(*chunk_hashes);
            *chunk_hashes = 0;
            Longtail_Free(*chunk_sizes);
            *chunk_sizes = 0;
            Longtail_Free(tags);
            tags = 0;
            Longtail_Free(hash_jobs);
            hash_jobs = 0;
            Longtail_Free(hashes);
            hashes = 0;
            Longtail_Free(sizes);
            sizes = 0;
            Longtail_Free(job_chunk_counts);
            job_chunk_counts = 0;
            return ENOMEM;
        }

        uint32_t chunk_offset = 0;
        for (uint32_t i = 0; i < jobs_started; ++i)
        {
            uint64_t asset_index = hash_jobs[i].m_AssetIndex;
            if (hash_jobs[i].m_StartRange == 0)
            {
                asset_chunk_start_index[asset_index] = chunk_offset;
                asset_chunk_counts[asset_index] = 0;
            }
            uint32_t job_chunk_count = *hash_jobs[i].m_AssetChunkCount;
            asset_chunk_counts[asset_index] += job_chunk_count;
            for (uint32_t chunk_index = 0; chunk_index < job_chunk_count; ++chunk_index)
            {
                (*chunk_sizes)[chunk_offset] = hash_jobs[i].m_ChunkSizes[chunk_index];
                (*chunk_hashes)[chunk_offset] = hash_jobs[i].m_ChunkHashes[chunk_index];
                (*chunk_tags)[chunk_offset] = hash_jobs[i].m_ChunkTags[chunk_index];
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ChunkAssets(%s) hash_api->HashBuffer(%p, %u, %p, %p) failed with %d", root_path, (void*)hash_api, hash_size, (void*)&(*chunk_hashes)[chunk_start_index], (void*)&content_hashes[a], err)
                Longtail_Free(*chunk_tags);
                *chunk_tags = 0;
                Longtail_Free(*chunk_hashes);
                *chunk_hashes = 0;
                Longtail_Free(*chunk_sizes);
                *chunk_sizes = 0;
                Longtail_Free(tags);
                tags = 0;
                Longtail_Free(hash_jobs);
                hash_jobs = 0;
                Longtail_Free(hashes);
                hashes = 0;
                Longtail_Free(sizes);
                sizes = 0;
                Longtail_Free(job_chunk_counts);
                job_chunk_counts = 0;
                return err;
            }
        }
    }

    Longtail_Free(tags);
    tags = 0;
    Longtail_Free(hash_jobs);
    hash_jobs = 0;
    Longtail_Free(hashes);
    hashes = 0;
    Longtail_Free(sizes);
    sizes = 0;
    Longtail_Free(job_chunk_counts);
    job_chunk_counts = 0;

    return err;
}

size_t Longtail_GetVersionIndexDataSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetVersionIndexDataSize(%u, %u, %u, %u)", asset_count, chunk_count, asset_chunk_index_count, path_data_size)
    LONGTAIL_VALIDATE_INPUT(asset_chunk_index_count >= chunk_count, return EINVAL)

    size_t version_index_data_size =
        sizeof(uint32_t) +                              // m_Version
        sizeof(uint32_t) +                              // m_HashAPI
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
        (sizeof(uint32_t) * asset_count) +              // m_Permissions
        path_data_size;

    return version_index_data_size;
}

size_t Longtail_GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetVersionIndexSize(%u, %u, %u, %u)", asset_count, chunk_count, asset_chunk_index_count, path_data_size)

    return sizeof(struct Longtail_VersionIndex) +
            Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, path_data_size);
}

static int InitVersionIndexFromData(
    struct Longtail_VersionIndex* version_index,
    void* data,
    size_t data_size)
{
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data_size >= sizeof(uint32_t), return EBADF)

    char* p = (char*)data;

    size_t version_index_data_start = (size_t)(uintptr_t)p;

    version_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*version_index->m_Version) != LONGTAIL_VERSION_INDEX_VERSION_0_0_1)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData(%p, %p, %" PRIu64 ") %" PRIu64 " == %" PRIu64 "", (void*)version_index, data, data_size, (void*)version_index->m_Version, LONGTAIL_VERSION_INDEX_VERSION_0_0_1)
        return EBADF;
    }

    version_index->m_HashAPI = (uint32_t*)(void*)p;
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "InitVersionIndexFromData(%p, %p, %" PRIu64 ")  data_size <= %" PRIu64 " failed with %d", (void*)version_index, data, data_size, (void*)version_index->m_Version, data_size, versiom_index_data_size, EBADF)
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

    version_index->m_Permissions = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * asset_count);

    size_t version_index_name_data_start = (size_t)p;

    version_index->m_NameDataSize = (uint32_t)(data_size - (version_index_name_data_start - version_index_data_start));

    version_index->m_NameData = (char*)p;

    return 0;
}

int Longtail_BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const struct Longtail_Paths* paths,
    const TLongtail_Hash* path_hashes,
    const TLongtail_Hash* content_hashes,
    const uint64_t* content_sizes,
    const uint32_t* asset_permissions,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_counts,
    uint32_t asset_chunk_index_count,
    const uint32_t* asset_chunk_indexes,
    uint32_t chunk_count,
    const uint32_t* chunk_sizes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_tags,
    uint32_t hash_api_identifier,
    uint32_t target_chunk_size,
    struct Longtail_VersionIndex** out_version_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_BuildVersionIndex(%p, %" PRIu64 ", %p, %p, %p, %p, %p, %p, %p, %u, %p, %u,%p ,%p, %p, %u, %p)",
        mem,
        mem_size,
        paths,
        path_hashes,
        content_hashes,
        content_sizes,
        asset_permissions,
        asset_chunk_index_starts,
        asset_chunk_counts,
        asset_chunk_index_count,
        asset_chunk_indexes,
        chunk_count,
        chunk_sizes,
        chunk_hashes,
        chunk_tags,
        hash_api_identifier,
        out_version_index);
    LONGTAIL_VALIDATE_INPUT(mem != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(mem_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(paths != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || path_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || content_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || content_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || asset_permissions != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(asset_chunk_counts == 0 || asset_chunk_index_starts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(*paths->m_PathCount == 0 || asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(asset_chunk_index_count >= chunk_count, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || asset_chunk_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_tags != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_version_index != 0, return EINVAL)

    uint32_t asset_count = *paths->m_PathCount;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)mem;
    uint32_t* p = (uint32_t*)(void*)&version_index[1];
    version_index->m_Version = &p[0];
    version_index->m_HashAPI = &p[1];
    version_index->m_TargetChunkSize = &p[2];
    version_index->m_AssetCount = &p[3];
    version_index->m_ChunkCount = &p[4];
    version_index->m_AssetChunkIndexCount = &p[5];
    *version_index->m_Version = LONGTAIL_VERSION_INDEX_VERSION_0_0_1;
    *version_index->m_HashAPI = hash_api_identifier;
    *version_index->m_TargetChunkSize = target_chunk_size;
    *version_index->m_AssetCount = asset_count;
    *version_index->m_ChunkCount = chunk_count;
    *version_index->m_AssetChunkIndexCount = asset_chunk_index_count;

    size_t index_data_size = mem_size - sizeof(struct Longtail_VersionIndex);
    int err = InitVersionIndexFromData(version_index, &version_index[1], index_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_BuildVersionIndex(%" PRIu64 ", %u, %u) InitVersionIndexFromData(%p, %p, %u) failed with %d", mem_size, chunk_count, hash_api_identifier, version_index, &version_index[1], index_data_size, err)
        return err;
    }

    memmove(version_index->m_PathHashes, path_hashes, sizeof(TLongtail_Hash) * asset_count);
    memmove(version_index->m_ContentHashes, content_hashes, sizeof(TLongtail_Hash) * asset_count);
    memmove(version_index->m_AssetSizes, content_sizes, sizeof(uint64_t) * asset_count);
    memmove(version_index->m_AssetChunkCounts, asset_chunk_counts, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_AssetChunkIndexStarts, asset_chunk_index_starts, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_AssetChunkIndexes, asset_chunk_indexes, sizeof(uint32_t) * asset_chunk_index_count);
    memmove(version_index->m_ChunkHashes, chunk_hashes, sizeof(TLongtail_Hash) * chunk_count);
    memmove(version_index->m_ChunkSizes, chunk_sizes, sizeof(uint32_t) * chunk_count);
    memmove(version_index->m_ChunkTags, chunk_tags, sizeof(uint32_t) * chunk_count);
    memmove(version_index->m_NameOffsets, paths->m_Offsets, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_Permissions, asset_permissions, sizeof(uint32_t) * asset_count);
    memmove(version_index->m_NameData, paths->m_Data, paths->m_DataSize);

    *out_version_index = version_index;
    return 0;
}

int Longtail_CreateVersionIndexRaw(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const char* root_path,
    const struct Longtail_Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_permissions,
    const uint32_t* asset_tags,
    uint32_t target_chunk_size,
    struct Longtail_VersionIndex** out_version_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionIndexRaw(%p, %p, %p, %p, %s, %p, %p, %p, %p, %u, %p)",
        storage_api,
        hash_api,
        job_api,
        progress_api,
        root_path,
        paths,
        asset_sizes,
        asset_permissions,
        asset_tags,
        target_chunk_size,
        out_version_index)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || asset_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || asset_permissions != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || asset_tags != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || target_chunk_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((paths == 0 || *paths->m_PathCount == 0) || out_version_index != 0, return EINVAL)

    uint32_t path_count = *paths->m_PathCount;

    if (path_count == 0)
    {
        size_t version_index_size = Longtail_GetVersionIndexSize(path_count, 0, 0, 0);
        void* version_index_mem = Longtail_Alloc(version_index_size);
        if (!version_index_mem)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, version_index_size, ENOMEM)
            return ENOMEM;
        }

        struct Longtail_VersionIndex* version_index;
        int err = Longtail_BuildVersionIndex(
            version_index_mem,              // mem
            version_index_size,             // mem_size
            paths,                          // paths
            0,                    // path_hashes
            0,                 // content_hashes
            0,                    // content_sizes
            0,              // asset_permissions
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_BuildVersionIndex(%s) Longtail_BuildVersionIndex(%" PRIu64 ") failed with %d", root_path, version_index_size, err)
            return err;
        }
        *out_version_index = version_index;
        return 0;
    }

    size_t path_hashes_size = sizeof(TLongtail_Hash) * path_count;
    TLongtail_Hash* path_hashes = (TLongtail_Hash*)Longtail_Alloc(path_hashes_size);
    if (!path_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, path_hashes_size, ENOMEM)
        return ENOMEM;
    }
    size_t content_hashes_size = sizeof(TLongtail_Hash) * path_count;
    TLongtail_Hash* content_hashes = (TLongtail_Hash*)Longtail_Alloc(content_hashes_size);
    if (!content_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, content_hashes_size, ENOMEM)
        Longtail_Free(path_hashes);
        return ENOMEM;
    }
    size_t asset_chunk_counts_size = sizeof(uint32_t) * path_count;
    uint32_t* asset_chunk_counts = (uint32_t*)Longtail_Alloc(asset_chunk_counts_size);
    if (!asset_chunk_counts)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, asset_chunk_counts_size, ENOMEM)
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }

    uint32_t assets_chunk_index_count = 0;
    uint32_t* asset_chunk_sizes = 0;
    uint32_t* asset_chunk_tags = 0;
    TLongtail_Hash* asset_chunk_hashes = 0;
    size_t asset_chunk_start_index_size = sizeof(uint32_t) * path_count;
    uint32_t* asset_chunk_start_index = (uint32_t*)Longtail_Alloc(asset_chunk_start_index_size);
    if (!asset_chunk_start_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, asset_chunk_start_index_size, ENOMEM)
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }

    int err = ChunkAssets(
        storage_api,
        hash_api,
        job_api,
        progress_api,
        root_path,
        paths,
        path_hashes,
        content_hashes,
        asset_sizes,
        asset_tags,
        asset_chunk_start_index,
        asset_chunk_counts,
        &asset_chunk_sizes,
        &asset_chunk_hashes,
        &asset_chunk_tags,
        target_chunk_size,
        &assets_chunk_index_count);
    if (err) {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) ChunkAssets(%s) failed with %d", root_path, root_path, err)
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return err;
    }

    size_t asset_chunk_indexes_size = sizeof(uint32_t) * assets_chunk_index_count;
    uint32_t* asset_chunk_indexes = (uint32_t*)Longtail_Alloc(asset_chunk_indexes_size);
    if (!asset_chunk_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, asset_chunk_indexes_size, ENOMEM)
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }
    size_t compact_chunk_hashes_size = sizeof(TLongtail_Hash) * assets_chunk_index_count;
    TLongtail_Hash* compact_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(compact_chunk_hashes_size);
    if (!compact_chunk_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, compact_chunk_hashes_size, ENOMEM)
        Longtail_Free(asset_chunk_indexes);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }
    size_t compact_chunk_sizes_size = sizeof(uint32_t) * assets_chunk_index_count;
    uint32_t* compact_chunk_sizes =  (uint32_t*)Longtail_Alloc(compact_chunk_sizes_size);
    if (!compact_chunk_sizes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, compact_chunk_sizes_size, ENOMEM)
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(asset_chunk_indexes);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }
    size_t compact_chunk_tags_size = sizeof(uint32_t) * assets_chunk_index_count;
    uint32_t* compact_chunk_tags =  (uint32_t*)Longtail_Alloc(compact_chunk_tags_size);
    if (!compact_chunk_tags)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, compact_chunk_tags_size, ENOMEM)
        Longtail_Free(compact_chunk_sizes);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(asset_chunk_indexes);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }

    uint32_t unique_chunk_count = 0;
    struct HashToIndexItem* chunk_hash_to_index = 0;
    for (uint32_t c = 0; c < assets_chunk_index_count; ++c)
    {
        TLongtail_Hash h = asset_chunk_hashes[c];
        intptr_t i = hmgeti(chunk_hash_to_index, h);
        if (i == -1)
        {
            hmput(chunk_hash_to_index, h, unique_chunk_count);
            compact_chunk_hashes[unique_chunk_count] = h;
            compact_chunk_sizes[unique_chunk_count] = asset_chunk_sizes[c];
            compact_chunk_tags[unique_chunk_count] = asset_chunk_tags[c];
            asset_chunk_indexes[c] = unique_chunk_count;
            ++unique_chunk_count;
        }
        else
        {
            asset_chunk_indexes[c] = (uint32_t)chunk_hash_to_index[i].value;
        }
    }

    hmfree(chunk_hash_to_index);
    chunk_hash_to_index = 0;

    size_t version_index_size = Longtail_GetVersionIndexSize(path_count, unique_chunk_count, assets_chunk_index_count, paths->m_DataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);
    if (!version_index_mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_Alloc(%" PRIu64 ") failed with %d", root_path, version_index_size, ENOMEM)
        Longtail_Free(compact_chunk_tags);
        Longtail_Free(compact_chunk_sizes);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(asset_chunk_indexes);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return ENOMEM;
    }

    struct Longtail_VersionIndex* version_index;
    err = Longtail_BuildVersionIndex(
        version_index_mem,              // mem
        version_index_size,             // mem_size
        paths,                          // paths
        path_hashes,                    // path_hashes
        content_hashes,                 // content_hashes
        asset_sizes,                    // content_sizes
        asset_permissions,              // asset_permissions
        asset_chunk_start_index,        // asset_chunk_index_starts
        asset_chunk_counts,             // asset_chunk_counts
        assets_chunk_index_count,       // asset_chunk_index_count
        asset_chunk_indexes,            // asset_chunk_indexes
        unique_chunk_count,             // chunk_count
        compact_chunk_sizes,            // chunk_sizes
        compact_chunk_hashes,           // chunk_hashes
        compact_chunk_tags,// chunk_tags
        hash_api->GetIdentifier(hash_api),
        target_chunk_size,
        &version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionIndexRaw(%s) Longtail_BuildVersionIndex() failed with %d", root_path, err)
        Longtail_Free(compact_chunk_tags);
        Longtail_Free(compact_chunk_sizes);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(asset_chunk_indexes);
        Longtail_Free(asset_chunk_tags);
        Longtail_Free(asset_chunk_hashes);
        Longtail_Free(asset_chunk_sizes);
        Longtail_Free(asset_chunk_start_index);
        Longtail_Free(asset_chunk_counts);
        Longtail_Free(content_hashes);
        Longtail_Free(path_hashes);
        return err;
    }

    Longtail_Free(compact_chunk_tags);
    Longtail_Free(compact_chunk_sizes);
    Longtail_Free(compact_chunk_hashes);
    Longtail_Free(asset_chunk_indexes);
    Longtail_Free(asset_chunk_tags);
    Longtail_Free(asset_chunk_hashes);
    Longtail_Free(asset_chunk_sizes);
    Longtail_Free(asset_chunk_start_index);
    Longtail_Free(asset_chunk_counts);
    Longtail_Free(content_hashes);
    Longtail_Free(path_hashes);

    *out_version_index = version_index;
    return 0;
}

int Longtail_CreateVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const char* root_path,
    struct Longtail_FileInfos* file_infos,
    const uint32_t* asset_tags,
    uint32_t target_chunk_size,
    struct Longtail_VersionIndex** out_version_index)
{
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((file_infos == 0 || *file_infos->m_Paths.m_PathCount == 0) || root_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((file_infos == 0 || *file_infos->m_Paths.m_PathCount == 0) || file_infos, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((file_infos == 0 || *file_infos->m_Paths.m_PathCount == 0) || asset_tags != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((file_infos == 0 || *file_infos->m_Paths.m_PathCount == 0) || target_chunk_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((file_infos == 0 || *file_infos->m_Paths.m_PathCount == 0) || out_version_index !=0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_version_index != 0, return EINVAL)

    return Longtail_CreateVersionIndexRaw(
        storage_api,
        hash_api,
        job_api,
        progress_api,
        root_path,
        file_infos ? &file_infos->m_Paths : 0,
        file_infos ? file_infos->m_FileSizes : 0,
        file_infos ? file_infos->m_Permissions : 0,
        asset_tags,
        target_chunk_size,
        out_version_index);
}


int Longtail_WriteVersionIndexToBuffer(
    const struct Longtail_VersionIndex* version_index,
    void** out_buffer,
    size_t* out_size)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteVersionIndexToBuffer(%p, %p, %p)", version_index, out_buffer, out_size)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndexToBuffer(%u) Longtail_Alloc(%" PRIu64 ") failed with %d", version_index->m_AssetCount, index_data_size, ENOMEM)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteVersionIndex(%s, %u, %u)", path, *version_index->m_AssetCount, *version_index->m_ChunkCount)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)

    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex() EnsureParentPathExists(%s) failed with %d", path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex() storage_api->OpenWriteFile(%s) failed with %d", path, err)
        return err;
    }
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex() storage_api->Write(%s, %u, %" PRIu64 ") failed with %d", path, 0, index_data_size, err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadVersionIndexFromBuffer(%p, %" PRIu64 ", %p)", buffer, size, out_version_index)
    LONGTAIL_VALIDATE_INPUT(buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_version_index != 0, return EINVAL)

    size_t version_index_size = sizeof(struct Longtail_VersionIndex) + size;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndexFromBuffer(%p, %" PRIu64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with ", buffer, size, out_version_index, version_index_size, ENOMEM)
        return ENOMEM;
    }
    memcpy(&version_index[1], buffer, size);
    int err = InitVersionIndexFromData(version_index, &version_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndexFromBuffer(%p, %" PRIu64 ", %p) InitVersionIndexFromData(%p, %p, %" PRIu64 ") failed with ", buffer, size, out_version_index, version_index, &version_index[1], size, err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadVersionIndex(%p, %s, %p)", storage_api, path, out_version_index)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_version_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) storage_api->OpenReadFile(%p, %s, %p) failed with %d", storage_api, path, out_version_index, storage_api, path, &file_handle, err)
        return err;
    }
    uint64_t version_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &version_index_data_size);
    if (err != 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) storage_api->GetSize(%p, %p, %p), %d", storage_api, path, out_version_index, storage_api, file_handle, &version_index_data_size, err)
        return err;
    }
    size_t version_index_size = version_index_data_size + sizeof(struct Longtail_VersionIndex);
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", storage_api, path, out_version_index, version_index_size, ENOMEM)
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    err = storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, path, out_version_index, storage_api, file_handle, 0, version_index_data_size, &version_index[1], err)
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = InitVersionIndexFromData(version_index, &version_index[1], version_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex(%p, %s, %p) InitVersionIndexFromData(%p, %p, %" PRIu64 ") failed with %d", storage_api, path, out_version_index, version_index, &version_index[1], version_index_data_size, err)
        Longtail_Free(version_index);
        return err;
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadVersionIndex(%p, %s, %p) containing %u assets in %u chunks", storage_api, path, out_version_index, version_index, *version_index->m_AssetCount, *version_index->m_ChunkCount)
    *out_version_index = version_index;
    return 0;
}

size_t Longtail_GetBlockIndexDataSize(uint32_t chunk_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_GetBlockIndexDataSize(%u)", chunk_count)
    return
        sizeof(TLongtail_Hash) +                    // m_BlockHash
        sizeof(uint32_t) +                          // m_ChunkCount
        sizeof(uint32_t) +                          // m_Tag
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count);           // m_ChunkSizes
}

struct Longtail_BlockIndex* Longtail_InitBlockIndex(void* mem, uint32_t chunk_count)
{
    LONGTAIL_VALIDATE_INPUT(mem != 0, return 0)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_InitBlockIndex(%p, %u)", mem, chunk_count)

    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)mem;
    char* p = (char*)&block_index[1];

    block_index->m_BlockHash = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash);

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
    LONGTAIL_VALIDATE_INPUT(block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(data != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_InitBlockIndexFromData(%p, %p, %" PRIu64 ")", block_index, data, data_size)

    char* p = (char*)data;

    block_index->m_BlockHash = (TLongtail_Hash*)(void*)p;
    p += sizeof(TLongtail_Hash);

    block_index->m_ChunkCount = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    block_index->m_Tag = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    uint32_t chunk_count = *block_index->m_ChunkCount;

    size_t block_index_data_size = Longtail_GetBlockIndexDataSize(chunk_count);
    if (block_index_data_size > data_size)
    {
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
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_block_index != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateBlockIndex(%p, %u, %u, %p, %p, %p, %p)", hash_api, tag, chunk_count, chunk_indexes, chunk_hashes, chunk_sizes, out_block_index)

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    void* mem = Longtail_Alloc(block_index_size);
    if (mem == 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateBlockIndex(%p, %u, %u, %p, %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with", hash_api, tag, chunk_count, chunk_indexes, chunk_hashes, chunk_sizes, out_block_index, block_index_size, ENOMEM)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateBlockIndex(%p, %u, %u, %p, %p, %p, %p) hash_api->HashBuffer(%p, %u, %p)",
            hash_api, tag, chunk_count, chunk_indexes, chunk_hashes, chunk_sizes, out_block_index,
            hash_api, hash_buffer_size, (void*)block_index->m_ChunkHashes, block_index->m_BlockHash)
        Longtail_Free(mem);
        return err;
    }
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteBlockIndexToBuffer(%p, %p, %p)", block_index, out_buffer, out_size)
    LONGTAIL_VALIDATE_INPUT(block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndexToBuffer(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", block_index, out_buffer, out_size, index_data_size, ENOMEM)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadBlockIndexFromBuffer(%p, %" PRIu64 ", %p)", buffer, size, out_block_index)
    LONGTAIL_VALIDATE_INPUT(buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_block_index != 0, return EINVAL)

    size_t block_index_size = size + sizeof(struct Longtail_BlockIndex);
    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)Longtail_Alloc(block_index_size);
    if (!block_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndexFromBuffer(%p, %" PRIu64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with %d", buffer, size, out_block_index, block_index_size, ENOMEM)
        return ENOMEM;
    }
    memcpy(&block_index[1], buffer, size);
    int err = Longtail_InitBlockIndexFromData(block_index, &block_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndexFromBuffer(%p, %" PRIu64 ", %p) Longtail_InitBlockIndexFromData(%p, %p, %" PRIu64 ") failed with %d", buffer, size, out_block_index, block_index, &block_index[1], size, err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteBlockIndex(%p, %p, %s)", storage_api, block_index, path)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex(%p, %p, %s) EnsureParentPathExists(%p, %s) failed with %d", storage_api, block_index, path, storage_api, path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex(%p, %p, %s) storage_api->OpenWriteFile(%p, %s, %u, %p) failed with %d", storage_api, block_index, path, storage_api, path, 0, &file_handle, err)
        return err;
    }
    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &block_index[1]);
    if (err){
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex(%p, %p, %s) storage_api->Write(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, block_index, path, storage_api, file_handle, 0, index_data_size, &block_index[1], err)
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    storage_api->CloseFile(storage_api, file_handle);

    return 0;
}

int Longtail_ReadBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_BlockIndex** out_block_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadBlockIndex(%p, %s, %p)", storage_api, path, out_block_index)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_block_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, path, &f);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) storage_api->OpenReadFile(%p, %s, %p) failed with %d", storage_api, path, out_block_index, storage_api, path, &f, err)
        return err;
    }
    uint64_t block_size;
    err = storage_api->GetSize(storage_api, f, &block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) storage_api->GetSize(%p, %p, %p) failed with %d", storage_api, path, out_block_index, storage_api, f, &block_size, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size < (sizeof(TLongtail_Hash) + sizeof(uint32_t)))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) truncated block, failed with %d", storage_api, path, out_block_index, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size > 0xffffffff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) block size to large failed with %d", storage_api, path, out_block_index, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    uint64_t read_offset = 0;
    TLongtail_Hash block_hash;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(TLongtail_Hash), &block_hash);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, path, out_block_index, storage_api, f, read_offset, sizeof(TLongtail_Hash), &block_hash, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    read_offset += sizeof(TLongtail_Hash);
    uint32_t chunk_count;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(uint32_t), &chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, path, out_block_index, storage_api, f, read_offset, sizeof(uint32_t), &chunk_count, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);

    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    void* block_index_mem = Longtail_Alloc(block_index_size);
    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(block_index_mem, chunk_count);
    err = storage_api->Read(storage_api, f, 0, block_index_data_size, &block_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadBlockIndex(%p, %s, %p) storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, path, out_block_index, storage_api, f, 0, block_index_data_size, &block_index[1], err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_InitStoredBlockFromData(%p, %p, %" PRIu64 ")", stored_block, block_data, block_data_size)
    LONGTAIL_VALIDATE_INPUT(stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_data_size > 0, return EINVAL)

    stored_block->m_BlockIndex = (struct Longtail_BlockIndex*)&stored_block[1];
    int err = Longtail_InitBlockIndexFromData(
        stored_block->m_BlockIndex,
        block_data,
        block_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData(%p, %p, %" PRIu64 ") Longtail_InitBlockIndexFromData(%p, %p, %" PRIu64 ") failed with %d", stored_block, block_data, block_data_size, stored_block->m_BlockIndex, block_data, block_data_size, err)
        return err;
    }
    stored_block->m_BlockData = &((uint8_t*)stored_block->m_BlockIndex)[Longtail_GetBlockIndexSize(*stored_block->m_BlockIndex->m_ChunkCount)];
    stored_block->m_BlockChunksDataSize = (uint32_t)(block_data_size - Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount));
    stored_block->Dispose = 0;
    return 0;
}

int Longtail_CreateStoredBlock(
    TLongtail_Hash block_hash,
    uint32_t chunk_count,
    uint32_t tag,
    TLongtail_Hash* chunk_hashes,
    uint32_t* chunk_sizes,
    uint32_t block_data_size,
    struct Longtail_StoredBlock** out_stored_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateStoredBlock(0x%" PRIx64 ", %u, %u, %p, %p, %u, %p)", block_hash, chunk_count, tag, chunk_hashes, chunk_sizes, block_data_size, out_stored_block)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stored_block != 0, return EINVAL)

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t stored_block_size = sizeof(struct Longtail_StoredBlock) + block_index_size + block_data_size;
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(stored_block_size);
    if (stored_block == 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoredBlock(0x%" PRIx64 ", %u, %u, %p, %p, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", block_hash, chunk_count, tag, chunk_hashes, chunk_sizes, block_data_size, out_stored_block, stored_block_size, ENOMEM)
        return ENOMEM;
    }
    stored_block->m_BlockIndex = Longtail_InitBlockIndex(&stored_block[1], chunk_count);
    if (!stored_block->m_BlockIndex)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateStoredBlock(0x%" PRIx64 ", %u, %u, %p, %p, %u, %p) Longtail_InitBlockIndex(%p, %u) failed with %d", block_hash, chunk_count, tag, chunk_hashes, chunk_sizes, block_data_size, out_stored_block, &stored_block[1], chunk_count, EINVAL)
        return EINVAL;
    }
    *stored_block->m_BlockIndex->m_BlockHash = block_hash;
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ReadStoredBlock_Dispose(%p)", stored_block)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)

    Longtail_Free(stored_block);
    return 0;
}

int Longtail_WriteStoredBlockToBuffer(
    const struct Longtail_StoredBlock* stored_block,
    void** out_buffer,
    size_t* out_size)
{
    LONGTAIL_VALIDATE_INPUT(stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_size != 0, return EINVAL)

    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);

    size_t size = block_index_data_size + stored_block->m_BlockChunksDataSize;

    void* mem = (uint8_t*)Longtail_Alloc(size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteStoredBlockToBuffer(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            stored_block, out_buffer, out_size,
            size,
            ENOMEM)
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
    LONGTAIL_VALIDATE_INPUT(buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stored_block != 0, return EINVAL)

    size_t block_mem_size = Longtail_GetStoredBlockSize(size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    if (!stored_block)
    {
        // TODO: Log
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
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile block_file_handle;
    int err = storage_api->OpenWriteFile(storage_api, path, 0, &block_file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_WriteStoredBlock: Failed to open block for write file `%s`, %d", path, err)
        return err;
    }
    uint32_t write_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    err = storage_api->Write(storage_api, block_file_handle, write_offset, block_index_data_size, &stored_block->m_BlockIndex[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_WriteStoredBlock: Failed to write block index to file `%s`, %d", path, err)
        storage_api->CloseFile(storage_api, block_file_handle);
        return err;
    }
    write_offset += block_index_data_size;

    err = storage_api->Write(storage_api, block_file_handle, write_offset, stored_block->m_BlockChunksDataSize, stored_block->m_BlockData);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_WriteStoredBlock: Failed to write block index to file `%s`, %d", path, err)
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
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stored_block != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, path, &f);
    if (err)
    {
        // TODO: Log
        return err;
    }
    uint64_t stored_block_data_size;
    err = storage_api->GetSize(storage_api, f, &stored_block_data_size);
    if (err)
    {
        // TODO: Log
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    size_t block_mem_size = Longtail_GetStoredBlockSize(stored_block_data_size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    if (!stored_block)
    {
        // TODO: Log
        storage_api->CloseFile(storage_api, f);
        return ENOMEM;
    }
    void* block_data = &((uint8_t*)stored_block)[block_mem_size - stored_block_data_size];
    err = storage_api->Read(storage_api, f, 0, stored_block_data_size, block_data);
    if (err)
    {
        // TODO: Log
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
        Longtail_Free(stored_block);
        return err;
    }
    stored_block->Dispose = ReadStoredBlock_Dispose;
    *out_stored_block = stored_block;
    return 0;
}



size_t Longtail_GetContentIndexDataSize(uint64_t block_count, uint64_t chunk_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetContentIndexDataSize(%" PRIu64 ", %" PRIu64 ")", block_count, chunk_count)

    size_t block_index_data_size = (size_t)(
        sizeof(uint32_t) +                          // m_Version
        sizeof(uint32_t) +                          // m_HashAPI
        sizeof(uint32_t) +                          // m_MaxBlockSize
        sizeof(uint32_t) +                          // m_MaxChunksPerBlock
        sizeof(uint64_t) +                          // m_BlockCount
        sizeof(uint64_t) +                          // m_ChunkCount
        (sizeof(TLongtail_Hash) * block_count) +    // m_BlockHashes[]
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes[]
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkBlockIndexes[]
        (sizeof(uint32_t) * chunk_count) +          // m_ChunkBlockOffsets[]
        (sizeof(uint32_t) * chunk_count)            // m_ChunkLengths[]
        );

    return block_index_data_size;
}

size_t Longtail_GetContentIndexSize(uint64_t block_count, uint64_t chunk_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_GetContentIndexSize(%" PRIu64 ", %" PRIu64 ")", block_count, chunk_count)
    
    return sizeof(struct Longtail_ContentIndex) +
        Longtail_GetContentIndexDataSize(block_count, chunk_count);
}

int Longtail_InitContentIndexFromData(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_InitContentIndexFromData(%p, %p, %" PRIu64 ")", content_index, data, data_size)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(data_size >= sizeof(uint32_t), return EINVAL)

    char* p = (char*)data;
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*content_index->m_Version) != LONGTAIL_CONTENT_INDEX_VERSION_0_0_1)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitContentIndexFromData(%p, %p, %" PRIu64 ") invalid version %u", content_index, data, data_size, *content_index->m_Version)
        return EBADF;
    }

    content_index->m_HashAPI = (uint32_t*)(void*)p;
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

    if (Longtail_GetContentIndexDataSize(block_count, chunk_count) > data_size)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitContentIndexFromData(%p, %p, %" PRIu64 ") Longtail_GetContentIndexDataSize(%u, %u) > %" PRIu64 "", content_index, data, data_size, block_count, chunk_count, data_size)
        return EBADF;
    }

    content_index->m_BlockHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * block_count);
    content_index->m_ChunkHashes = (TLongtail_Hash*)(void*)p;
    p += (sizeof(TLongtail_Hash) * chunk_count);
    content_index->m_ChunkBlockIndexes = (uint64_t*)(void*)p;
    p += (sizeof(uint64_t) * chunk_count);
    content_index->m_ChunkBlockOffsets = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * chunk_count);
    content_index->m_ChunkLengths = (uint32_t*)(void*)p;
    p += (sizeof(uint32_t) * chunk_count);

    return 0;
}

int Longtail_InitContentIndex(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size,
    uint32_t hash_api,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    uint64_t chunk_count)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_InitContentIndex(%p, %p, %" PRIu64 ", %u, %" PRIu64 ", %" PRIu64 ")", content_index, data, data_size, hash_api, block_count, chunk_count)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(data != 0, return EINVAL)

    uint8_t* p = (uint8_t*)data;
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_HashAPI = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_MaxBlockSize = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_MaxChunksPerBlock = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_BlockCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    content_index->m_ChunkCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    *content_index->m_Version = LONGTAIL_CONTENT_INDEX_VERSION_0_0_1;
    *content_index->m_HashAPI = hash_api;
    *content_index->m_MaxBlockSize = max_block_size;
    *content_index->m_MaxChunksPerBlock = max_chunks_per_block;
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    return Longtail_InitContentIndexFromData(content_index, &content_index[1], data_size);
}

static uint64_t GetUniqueHashes(
    uint64_t hash_count,
    const TLongtail_Hash* hashes,
    uint64_t* out_unique_hash_indexes)
{
    LONGTAIL_FATAL_ASSERT(hash_count != 0, return 0)
    LONGTAIL_FATAL_ASSERT(hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(hash_count == 0 || out_unique_hash_indexes != 0, return 0)

    struct HashToIndexItem* lookup_table = 0;

    uint64_t unique_hash_count = 0;
    for (uint64_t i = 0; i < hash_count; ++i)
    {
        TLongtail_Hash hash = hashes[i];
        ptrdiff_t lookup_index = hmgeti(lookup_table, hash);
        if (lookup_index == -1)
        {
            hmput(lookup_table, hash, 1);
            out_unique_hash_indexes[unique_hash_count] = i;
            ++unique_hash_count;
        }
        else
        {
            ++lookup_table[lookup_index].value;
        }
    }
    hmfree(lookup_table);
    lookup_table = 0;
    return unique_hash_count;
}

int Longtail_CreateContentIndexFromBlocks(
    uint32_t hash_identifier,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    struct Longtail_BlockIndex** block_indexes,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateContentIndexFromBlocks(%u, %" PRIu64 ", %p, %p)", hash_identifier, block_count, block_indexes, out_content_index)
    LONGTAIL_VALIDATE_INPUT(block_count == 0 || block_indexes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)

    uint64_t chunk_count = 0;
    for (uint64_t b = 0; b < block_count; ++b)
    {
        chunk_count += *block_indexes[b]->m_ChunkCount;
    }

    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexFromBlocks(%u, %" PRIu64 ", %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_identifier, block_count, block_indexes, out_content_index, content_index_size, ENOMEM)
        return ENOMEM;
    }
    int err = Longtail_InitContentIndex(
        content_index,
        &content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        hash_identifier,
        max_block_size,
        max_chunks_per_block,
        block_count,
        chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexFromBlocks(%u, %" PRIu64 ", %p, %p) Longtail_InitContentIndex(%p, %p, %" PRIu64 ", %u, %" PRIu64 ", %" PRIu64 ") failed with %d", hash_identifier, block_count, block_indexes, out_content_index, content_index, &content_index[1], content_index_size - sizeof(struct Longtail_ContentIndex), hash_identifier, block_count, chunk_count, err)
        Longtail_Free(content_index);
        return err;
    }

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
            content_index->m_ChunkBlockOffsets[asset_index] = chunk_offset;
            content_index->m_ChunkLengths[asset_index] = block_index->m_ChunkSizes[a];

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
    const uint32_t* chunk_tags,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p)", hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || chunk_tags != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(chunk_count == 0 || max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)

    if (chunk_count == 0)
    {
        size_t content_index_size = Longtail_GetContentIndexSize(0, 0);
        struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
        if (!content_index)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index, content_index_size, ENOMEM)
            return ENOMEM;
        }
        int err = Longtail_InitContentIndex(
            content_index,
            &content_index[1],
            content_index_size - sizeof(struct Longtail_ContentIndex),
            hash_api->GetIdentifier(hash_api),
            max_block_size,
            max_chunks_per_block,
            0,
            0);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_InitContentIndex(%p, %p, %" PRIu64 ", %u, %u, %u) failed with %d",
                hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index,
                content_index, &content_index[1], content_index_size - sizeof(struct Longtail_ContentIndex), hash_api->GetIdentifier(hash_api), 0, 0,
                err)
            Longtail_Free(content_index);
            return err;
        }
        *out_content_index = content_index;
        return 0;
    }
    size_t chunk_indexes_size = sizeof(uint64_t) * chunk_count;
    uint64_t* chunk_indexes = (uint64_t*)Longtail_Alloc(chunk_indexes_size);
    if (!chunk_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index, chunk_indexes_size, ENOMEM)
        return ENOMEM;
    }
    uint64_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, chunk_indexes);

    size_t block_indexes_size = sizeof(struct Longtail_BlockIndex*) * unique_chunk_count;
    struct Longtail_BlockIndex** block_indexes = (struct Longtail_BlockIndex**)Longtail_Alloc(block_indexes_size);
    if (!block_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index, block_indexes_size, ENOMEM)
        return ENOMEM;
    }

    size_t stored_chunk_indexes_size = sizeof(uint64_t) * max_chunks_per_block;
    uint64_t* stored_chunk_indexes = (uint64_t*)Longtail_Alloc(stored_chunk_indexes_size);
    if (!stored_chunk_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index, stored_chunk_indexes_size, ENOMEM)
        return ENOMEM;
    }

    uint64_t i = 0;
    uint32_t chunk_count_in_block = 0;
    uint32_t block_count = 0;
    uint32_t current_tag = 0;

    while (i < unique_chunk_count)
    {
        chunk_count_in_block = 0;

        uint64_t chunk_index = chunk_indexes[i];

        uint32_t current_size = chunk_sizes[chunk_index];
        current_tag = chunk_tags[chunk_index];

        stored_chunk_indexes[chunk_count_in_block] = chunk_index;
        ++chunk_count_in_block;

        while((i + 1) < unique_chunk_count)
        {
            chunk_index = chunk_indexes[(i + 1)];
            uint32_t chunk_size = chunk_sizes[chunk_index];
            uint32_t tag = chunk_tags[chunk_index];

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
            stored_chunk_indexes[chunk_count_in_block] = chunk_index;
            ++chunk_count_in_block;

            ++i;
        }

        int err = Longtail_CreateBlockIndex(
            hash_api,
            current_tag,
            chunk_count_in_block,
            stored_chunk_indexes,
            chunk_hashes,
            chunk_sizes,
            &block_indexes[block_count]);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_CreateBlockIndex(%p, %u, %u, %p, %p, %p, %p) failed with %d",
                hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index,
                hash_api, current_tag, chunk_count_in_block, stored_chunk_indexes, chunk_hashes, chunk_sizes, &block_indexes[block_count],
                err)
            return err;
        }

        ++block_count;
        ++i;
    }

    Longtail_Free(stored_chunk_indexes);
    stored_chunk_indexes = 0;
    Longtail_Free(chunk_indexes);
    chunk_indexes = 0;

    int err = Longtail_CreateContentIndexFromBlocks(
        hash_api->GetIdentifier(hash_api),
        max_block_size,
        max_chunks_per_block,
        block_count,
        block_indexes,
        out_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %u, %u, %p) Longtail_CreateContentIndexFromBlocks(%u, %u, %p, %p) failed with %d",
            hash_api, chunk_count, chunk_hashes, chunk_sizes, chunk_tags, max_block_size, max_chunks_per_block, out_content_index,
            hash_api->GetIdentifier(hash_api), block_count, block_indexes, out_content_index,
            err)
        return err;
    }

    for (uint32_t b = 0; b < block_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = block_indexes[b];
        Longtail_Free(block_index);
        block_index = 0;
    }
    Longtail_Free(block_indexes);
    block_indexes = 0;
    return err;
}

int Longtail_CreateContentIndex(
    struct Longtail_HashAPI* hash_api,
    struct Longtail_VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((version_index == 0 || (*version_index->m_ChunkCount) == 0) || max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((version_index == 0 || (*version_index->m_ChunkCount) == 0) || max_chunks_per_block != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)
    return Longtail_CreateContentIndexRaw(
        hash_api,
        version_index ? *version_index->m_ChunkCount : 0,
        version_index ? version_index->m_ChunkHashes : 0,
        version_index ? version_index->m_ChunkSizes : 0,
        version_index ? version_index->m_ChunkTags : 0,
        max_block_size,
        max_chunks_per_block,
        out_content_index);
}


int Longtail_WriteContentIndexToBuffer(
    const struct Longtail_ContentIndex* content_index,
    void** out_buffer,
    size_t* out_size)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteContentIndexToBuffer(%p, %p, %p)", content_index, out_buffer, out_size)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_size != 0, return EINVAL)

    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndexToBuffer(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", content_index, out_buffer, out_size, index_data_size, ENOMEM)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadContentIndexFromBuffer(%p, %" PRIu64 ", %p)", buffer, size, out_content_index)
    LONGTAIL_VALIDATE_INPUT(buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)

    size_t content_index_size = size + sizeof(struct Longtail_ContentIndex);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndexFromBuffer(%p, %" PRIu64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with %d", buffer, size, out_content_index, content_index_size, ENOMEM)
        return ENOMEM;
    }
    memcpy(&content_index[1], buffer, size);
    int err = Longtail_InitContentIndexFromData(content_index, &content_index[1], size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndexFromBuffer(%p, %" PRIu64 ", %p) Longtail_InitContentIndexFromData(%p, %p, %" PRIu64 ") failed with %d", buffer, size, out_content_index, content_index, &content_index[1], size, err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteContentIndex(%p, %p, %s)", storage_api, content_index, path)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex(%p, %p, %s) EnsureParentPathExists(%p, %s) failed with %d", storage_api, content_index, path, storage_api, path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex(%p, %p, %s) storage_api->OpenWriteFile(%p, %s, %u, %p) failed with %d", storage_api, content_index, path, storage_api, path, 0, &file_handle, err)
        return err;
    }
    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &content_index[1]);
    if (err){
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex(%p, %p, %s) storage_api->Write(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, content_index, path, storage_api, file_handle, 0, index_data_size, &content_index[1], err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadContentIndex(%p, %s, %p)", storage_api, path, out_content_index)
    LONGTAIL_VALIDATE_INPUT(storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)

    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadContentIndex(%p, %s, %p) storage_api->OpenReadFile(%p, %s, %p) failed with %d", storage_api, path, out_content_index, storage_api, path, &file_handle, err)
        return err;
    }
    uint64_t content_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &content_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex(%p, %s, %p) storage_api->GetSize(%p, %p, %p) failed with %d", storage_api, path, out_content_index, storage_api, file_handle, &content_index_data_size, err)
        return err;
    }
    uint64_t content_index_size = sizeof(struct Longtail_ContentIndex) + content_index_data_size;
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc((size_t)(content_index_size));
    if (!content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex(%p, %s, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", storage_api, path, out_content_index, content_index_size, ENOMEM)
        Longtail_Free(content_index);
        content_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return ENOMEM;
    }
    err = storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex(%p, %s, %p) storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", storage_api, path, out_content_index, storage_api, file_handle, 0, content_index_data_size, &content_index[1], err)
        Longtail_Free(content_index);
        content_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    err = Longtail_InitContentIndexFromData(content_index, &content_index[1], content_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex(%p, %s, %p) Longtail_InitContentIndexFromData(%p, %p, %" PRIu64 ") failed with %d", storage_api, path, out_content_index, content_index, &content_index[1], content_index_data_size, err)
        Longtail_Free(content_index);
        return err;
    }
    *out_content_index = content_index;
    return 0;
}

struct AssetPart
{
    const char* m_Path;
    uint64_t m_Start;
    uint32_t m_Tag;
};

struct ChunkHashToAssetPart
{
    TLongtail_Hash key;
    struct AssetPart value;
};

static int CreateAssetPartLookup(
    struct Longtail_VersionIndex* version_index,
    struct ChunkHashToAssetPart** out_assert_part_lookup)
{
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_assert_part_lookup != 0, return EINVAL)

    struct ChunkHashToAssetPart* asset_part_lookup = 0;
    uint64_t asset_count = *version_index->m_AssetCount;
    for (uint64_t asset_index = 0; asset_index < asset_count; ++asset_index)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint64_t asset_chunk_count = version_index->m_AssetChunkCounts[asset_index];
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint64_t asset_chunk_offset = 0;
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < asset_chunk_count; ++asset_chunk_index)
        {
            LONGTAIL_FATAL_ASSERT(asset_chunk_index_start + asset_chunk_index < *version_index->m_AssetChunkIndexCount, return EINVAL)
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            LONGTAIL_FATAL_ASSERT(chunk_index < *version_index->m_ChunkCount, return EINVAL)
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
            uint32_t tag = version_index->m_ChunkTags[chunk_index];
            intptr_t lookup_ptr = hmgeti(asset_part_lookup, chunk_hash);
            if (lookup_ptr == -1)
            {
                struct AssetPart asset_part = {
                    path,
                    asset_chunk_offset,
                    tag
                };
                hmput(asset_part_lookup, chunk_hash, asset_part);
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
    struct ChunkHashToAssetPart* m_AssetPartLookup;
    uint64_t m_FirstChunkIndex;
    uint32_t m_ChunkCount;
    int m_Err;
};

static int BlockWriterJobOnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return EINVAL)
    struct WriteBlockJob* job = (struct WriteBlockJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(job->m_AsyncCompleteAPI.OnComplete != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(job->m_StoredBlock != 0, return EINVAL);
    LONGTAIL_FATAL_ASSERT(job->m_JobID != 0, return EINVAL);
    uint32_t job_id = job->m_JobID;
    job->m_StoredBlock->Dispose(job->m_StoredBlock);
    job->m_StoredBlock = 0;
    job->m_JobID = 0;
    job->m_Err = err;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job_id);
    return 0;
}

static int DisposePutBlock(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free(stored_block->m_BlockIndex);
    Longtail_Free(stored_block->m_BlockData);
    Longtail_Free(stored_block);
    return 0;
}

static int WriteContentBlockJob(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)

    struct WriteBlockJob* job = (struct WriteBlockJob*)context;
    LONGTAIL_FATAL_ASSERT(job->m_JobID == 0, return EINVAL);

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
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
        LONGTAIL_FATAL_ASSERT(content_index->m_ChunkBlockIndexes[chunk_index] == block_index, job->m_Err = EINVAL; return 0)
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        block_data_size += chunk_size;
    }

    char* block_data_buffer = (char*)Longtail_Alloc(block_data_size);
    if (!block_data_buffer)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) Longtail_Alloc(%" PRIu64 ") failed with %d", context, job_id, block_data_size, ENOMEM);
        job->m_Err = ENOMEM;
        return 0;
    }
    char* write_buffer = block_data_buffer;
    char* write_ptr = write_buffer;

    uint32_t tag = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t tmp;
        intptr_t asset_part_index = hmgeti_ts(job->m_AssetPartLookup, chunk_hash, tmp);
        LONGTAIL_FATAL_ASSERT(asset_part_index != -1, job->m_Err = EINVAL; return 0)
        struct AssetPart* asset_part = &job->m_AssetPartLookup[asset_part_index].value;
        const char* asset_path = asset_part->m_Path;
        LONGTAIL_FATAL_ASSERT(!IsDirPath(asset_path), job->m_Err = EINVAL; return 0)

        char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
        uint64_t asset_content_offset = asset_part->m_Start;
        if (chunk_index != first_chunk_index && tag != asset_part->m_Tag)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "WriteContentBlockJob: Warning: Inconsistent tag type for chunks inside block 0x%" PRIx64 ", retaining 0x%" PRIx64 "", block_hash, tag)
        }
        else
        {
            tag = asset_part->m_Tag;
        }
        Longtail_StorageAPI_HOpenFile file_handle;
        int err = source_storage_api->OpenReadFile(source_storage_api, full_path, &file_handle);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) source_storage_api->OpenReadFile(%p, %s, %p) failed with %d", context, job_id, source_storage_api, full_path, &file_handle, err);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = err;
            return 0;
        }
        uint64_t asset_file_size;
        err = source_storage_api->GetSize(source_storage_api, file_handle, &asset_file_size);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) source_storage_api->GetSize(%p, %p, %p) failed with %d", context, job_id, source_storage_api, file_handle, &asset_file_size, err);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = err;
            return 0;
        }
        if (asset_file_size < (asset_content_offset + chunk_size))
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) %" PRIu64 " < %" PRIu64 " + %" PRIu64 "", context, job_id, asset_file_size, asset_content_offset, chunk_size);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            source_storage_api->CloseFile(source_storage_api, file_handle);
            file_handle = 0;
            job->m_Err = EBADF;
            return 0;
        }
        err = source_storage_api->Read(source_storage_api, file_handle, asset_content_offset, chunk_size, write_ptr);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) source_storage_api->Read(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", context, job_id, source_storage_api, file_handle, asset_content_offset, chunk_size, write_ptr, err);
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            source_storage_api->CloseFile(source_storage_api, file_handle);
            file_handle = 0;
            job->m_Err = err;
            return 0;
        }
        write_ptr += chunk_size;

        source_storage_api->CloseFile(source_storage_api, file_handle);
        Longtail_Free((char*)full_path);
        full_path = 0;
    }

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    struct Longtail_BlockIndex* block_index_ptr = (struct Longtail_BlockIndex*)Longtail_Alloc(block_index_size);
    if (!block_index_ptr)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) Longtail_Alloc(%" PRIu64 ") failed with %d", context, job_id, block_index_size, ENOMEM);
        job->m_Err = ENOMEM;
        return 0;
    }
    Longtail_InitBlockIndex(block_index_ptr, chunk_count);
    memmove(block_index_ptr->m_ChunkHashes, &content_index->m_ChunkHashes[first_chunk_index], sizeof(TLongtail_Hash) * chunk_count);
    memmove(block_index_ptr->m_ChunkSizes, &content_index->m_ChunkLengths[first_chunk_index], sizeof(uint32_t) * chunk_count);
    *block_index_ptr->m_BlockHash = block_hash;
    *block_index_ptr->m_Tag = tag;
    *block_index_ptr->m_ChunkCount = chunk_count;

    job->m_StoredBlock = (struct Longtail_StoredBlock*)Longtail_Alloc(sizeof(struct Longtail_StoredBlock));
    job->m_StoredBlock->Dispose = DisposePutBlock;
    job->m_StoredBlock->m_BlockIndex = block_index_ptr;
    job->m_StoredBlock->m_BlockData = block_data_buffer;
    job->m_StoredBlock->m_BlockChunksDataSize = block_data_size;

    job->m_JobID = job_id;
    job->m_AsyncCompleteAPI.OnComplete = BlockWriterJobOnComplete;

    int err = block_store_api->PutStoredBlock(block_store_api, job->m_StoredBlock, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteContentBlockJob(%p, %u) block_store_api->PutStoredBlock(%p, %p, %p) failed with %d", context, job_id, block_store_api, job->m_StoredBlock, &job->m_AsyncCompleteAPI, err);
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
    struct Longtail_ContentIndex* block_store_content_index,
    struct Longtail_ContentIndex* version_content_index,
    struct Longtail_VersionIndex* version_index,
    const char* assets_folder)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s)", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder)
    LONGTAIL_VALIDATE_INPUT(source_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(assets_folder != 0, return EINVAL)

    uint64_t chunk_count = *version_content_index->m_ChunkCount;
    uint64_t total_chunk_size = 0;
    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        total_chunk_size += version_content_index->m_ChunkLengths[c];
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s) chunks %" PRIu64 ", blocks %" PRIu64 ", size: %" PRIu64 " bytes", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder, *version_content_index->m_ChunkCount, *version_content_index->m_BlockCount, total_chunk_size)
    uint64_t block_count = *version_content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 0;
    }

    int err = job_api->ReserveJobs(job_api, (uint32_t)block_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s) ReserveJobs(%p, %u) failed with %d", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder, job_api, (uint32_t)block_count, err)
        return err;
    }

    struct ChunkHashToAssetPart* asset_part_lookup;
    err = CreateAssetPartLookup(version_index, &asset_part_lookup);
    if (!asset_part_lookup)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s) CreateAssetPartLookup(%p, %p) failed with %d", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder, version_index, &asset_part_lookup, err)
        return err;
    }

    struct HashToIndexItem* block_store_lookup = 0;
    uint64_t block_store_block_count = block_store_content_index ? *block_store_content_index->m_BlockCount : 0;
    for (uint64_t b = 0; b < block_store_block_count; ++b)
    {
        hmput(block_store_lookup, block_store_content_index->m_BlockHashes[b], b);
    }

    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteBlockJob) * block_count));
    LONGTAIL_FATAL_ASSERT(write_block_jobs != 0, return ENOMEM)
    uint32_t block_start_chunk_index = 0;
    uint32_t job_count = 0;
    for (uint64_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = version_content_index->m_BlockHashes[block_index];
        uint32_t chunk_count = 0;
        while(version_content_index->m_ChunkBlockIndexes[block_start_chunk_index + chunk_count] == block_index)
        {
            ++chunk_count;
        }

        intptr_t block_index_ptr = hmgeti(block_store_lookup, block_hash);
        if (block_index_ptr != -1)
        {
            block_start_chunk_index += chunk_count;
            continue;
        }

        struct WriteBlockJob* job = &write_block_jobs[job_count++];
        job->m_AsyncCompleteAPI.m_API.Dispose = 0;
        job->m_AsyncCompleteAPI.OnComplete = 0;
        job->m_SourceStorageAPI = source_storage_api;
        job->m_BlockStoreAPI = block_store_api;
        job->m_JobAPI = job_api;
        job->m_JobID = 0;
        job->m_StoredBlock = 0;
        job->m_AssetsFolder = assets_folder;
        job->m_ContentIndex = version_content_index;
        job->m_BlockHash = block_hash;
        job->m_AssetPartLookup = asset_part_lookup;
        job->m_FirstChunkIndex = block_start_chunk_index;
        job->m_ChunkCount = chunk_count;
        job->m_Err = EINVAL;

        Longtail_JobAPI_JobFunc func[1] = { WriteContentBlockJob };
        void* ctx[1] = { job };

        Longtail_JobAPI_Jobs jobs;
        err = job_api->CreateJobs(job_api, 1, func, ctx, &jobs);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        err = job_api->ReadyJobs(job_api, 1, jobs);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)

        block_start_chunk_index += chunk_count;
    }
    hmfree(block_store_lookup);
    block_store_lookup = 0;

    err = job_api->WaitForAllJobs(job_api, progress_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s) job_api->WaitForAllJobs(%p, %p) failed with %d", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder, job_api, progress_api, err)
        return err;
    }
    LONGTAIL_FATAL_ASSERT(err == 0, return err)

    err = 0;
    while (job_count--)
    {
        struct WriteBlockJob* job = &write_block_jobs[job_count];
        if (job->m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent(%p, %p, %p, %p, %p, %p, %s) Failed to write content for block 0x%" PRIx64 " failed with %d", source_storage_api, block_store_api, job_api, progress_api, version_content_index, version_index, assets_folder, job->m_BlockHash, job->m_Err)
            err = err ? err : job->m_Err;
        }
    }

    hmfree(asset_part_lookup);
    asset_part_lookup = 0;
    Longtail_Free(write_block_jobs);
    write_block_jobs = 0;

    return err;
}


struct ContentLookup
{
    struct HashToIndexItem* m_BlockHashToBlockIndex;
    struct HashToIndexItem* m_ChunkHashToChunkIndex;
    struct HashToIndexItem* m_ChunkHashToBlockIndex;
};

static void DeleteContentLookup(struct ContentLookup* cl)
{
    LONGTAIL_FATAL_ASSERT(cl != 0, return)

    hmfree(cl->m_ChunkHashToBlockIndex);
    cl->m_ChunkHashToBlockIndex = 0;
    hmfree(cl->m_BlockHashToBlockIndex);
    cl->m_BlockHashToBlockIndex = 0;
    hmfree(cl->m_ChunkHashToChunkIndex);
    cl->m_ChunkHashToChunkIndex = 0;
    Longtail_Free(cl);
}

static int CreateContentLookup(
    uint64_t block_count,
    const TLongtail_Hash* block_hashes,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint64_t* chunk_block_indexes,
    struct ContentLookup** out_content_lookup)
{
    LONGTAIL_FATAL_ASSERT(block_count == 0 || block_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_block_indexes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_content_lookup != 0, return EINVAL)

    struct ContentLookup* cl = (struct ContentLookup*)Longtail_Alloc(sizeof(struct ContentLookup));
    if (!cl)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CreateContentLookup(%" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d", block_count, block_hashes, chunk_count, chunk_hashes, chunk_block_indexes, out_content_lookup, sizeof(struct ContentLookup), ENOMEM)
        return ENOMEM;
    }
    cl->m_BlockHashToBlockIndex = 0;
    cl->m_ChunkHashToChunkIndex = 0;
    cl->m_ChunkHashToBlockIndex = 0;
    for (uint64_t i = 0; i < block_count; ++i)
    {
        TLongtail_Hash block_hash = block_hashes[i];
        hmput(cl->m_BlockHashToBlockIndex, block_hash, i);
    }
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = chunk_hashes[i];
        hmput(cl->m_ChunkHashToChunkIndex, chunk_hash, i);
        uint64_t block_index = chunk_block_indexes[i];
        hmput(cl->m_ChunkHashToBlockIndex, chunk_hash, block_index);
    }
    *out_content_lookup = cl;
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

int BlockReaderJobOnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_FATAL_ASSERT(async_complete_api != 0, return EINVAL)
    struct BlockReaderJob* job = (struct BlockReaderJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(job->m_AsyncCompleteAPI.OnComplete != 0, return EINVAL);
    job->m_Err = err;
    job->m_StoredBlock = stored_block;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job->m_JobID);
    return 0;
}

static int BlockReader(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)

    struct BlockReaderJob* job = (struct BlockReaderJob*)context;

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        return 0;
    }

    job->m_JobID = job_id;
    job->m_StoredBlock = 0;
    job->m_AsyncCompleteAPI.OnComplete = BlockReaderJobOnComplete;
    
    int err = job->m_BlockStoreAPI->GetStoredBlock(job->m_BlockStoreAPI, job->m_BlockHash, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ReadBlockData(%p, %u) job->m_BlockStoreAPI->GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d", context, job_id, job->m_BlockStoreAPI, job->m_BlockHash, &job->m_AsyncCompleteAPI)
        return err;
    }
    return EBUSY;
}

static int WriteReady(void* context, uint32_t job_id)
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
    struct ContentLookup* m_ContentLookup;
    uint32_t m_AssetIndex;
    int m_RetainPermissions;

    struct BlockReaderJob m_BlockReaderJobs[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    uint32_t m_BlockReaderJobCount;

    uint32_t m_AssetChunkIndexOffset;
    uint32_t m_AssetChunkCount;

    Longtail_StorageAPI_HOpenFile m_AssetOutputFile;

    int m_Err;
};

int WritePartialAssetFromBlocks(void* context, uint32_t job_id);

// Returns the write sync task, or the write task if there is no need for block_readion of block
static int CreatePartialAssetWriteJob(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_folder,
    struct ContentLookup* content_lookup,
    uint32_t asset_index,
    int retain_permissions,
    struct WritePartialAssetFromBlocksJob* job,
    uint32_t asset_chunk_index_offset,
    Longtail_StorageAPI_HOpenFile asset_output_file,
    Longtail_JobAPI_Jobs* out_jobs)
{
    LONGTAIL_FATAL_ASSERT(block_store_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_storage_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_folder !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_lookup !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_index < *version_index->m_AssetCount, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job !=0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_jobs !=0, return EINVAL)

    job->m_VersionStorageAPI = version_storage_api;
    job->m_BlockStoreAPI = block_store_api;
    job->m_JobAPI = job_api;
    job->m_ContentIndex = content_index;
    job->m_VersionIndex = version_index;
    job->m_VersionFolder = version_folder;
    job->m_ContentLookup = content_lookup;
    job->m_AssetIndex = asset_index;
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

    while (chunk_index_offset != chunk_index_end && job->m_BlockReaderJobCount < max_parallell_block_read_jobs)
    {
        uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
        TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
        intptr_t tmp;
        uint64_t block_index = hmget_ts(content_lookup->m_ChunkHashToBlockIndex, chunk_hash, tmp);
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
    int err = job_api->CreateJobs(job_api, 1, write_funcs, write_ctx, &write_job);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CreatePartialAssetWriteJob(%p, %p, %p, %p, %p, %s, %p, %u, %d, %p, %u, %p, %p) storage_api->Read(%p, %u, %p, %p, %p) failed with %d",
            block_store_api, version_storage_api, job_api, content_index, version_index, version_folder, content_lookup, asset_index, retain_permissions, job, asset_chunk_index_offset, asset_output_file, out_jobs,
            job_api, 1, write_funcs, write_ctx, &write_job,
            err)
        return err;
    }

    if (job->m_BlockReaderJobCount > 0)
    {
        Longtail_JobAPI_Jobs block_readion_jobs;
        err = job_api->CreateJobs(job_api, job->m_BlockReaderJobCount, block_read_funcs, block_read_ctx, &block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        Longtail_JobAPI_JobFunc sync_write_funcs[1] = { WriteReady };
        void* sync_write_ctx[1] = { 0 };
        Longtail_JobAPI_Jobs write_sync_job;
        err = job_api->CreateJobs(job_api, 1, sync_write_funcs, sync_write_ctx, &write_sync_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)

        err = job_api->AddDependecies(job_api, 1, write_job, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        err = job_api->AddDependecies(job_api, 1, write_job, job->m_BlockReaderJobCount, block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        err = job_api->ReadyJobs(job_api, job->m_BlockReaderJobCount, block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)

        *out_jobs = write_sync_job;
        return 0;
    }
    *out_jobs = write_job;
    return 0;
}

int WritePartialAssetFromBlocks(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context !=0, return EINVAL)
    struct WritePartialAssetFromBlocksJob* job = (struct WritePartialAssetFromBlocksJob*)context;

    // Need to fetch all the data we need from the context since we will reuse it
    job->m_Err = 0;
    uint32_t block_block_reador_job_count = job->m_BlockReaderJobCount;
    TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    struct Longtail_StoredBlock* stored_block[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
    for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
    {
        if (job->m_BlockReaderJobs[d].m_Err)
        {
            job->m_Err = job->m_BlockReaderJobs[d].m_Err;
            block_hashes[d] = 0;
            stored_block[d] = 0;
            continue;
        }
        block_hashes[d] = job->m_BlockReaderJobs[d].m_BlockHash;
        stored_block[d] = job->m_BlockReaderJobs[d].m_StoredBlock;
    }

    if (job->m_Err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) Failed to block_read blocks, %d", context, job_id, job->m_Err)
        for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
        {
            if (stored_block[d] && stored_block[d]->Dispose)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
        }
        return 0;
    }

    uint32_t write_chunk_index_offset = job->m_AssetChunkIndexOffset;
    uint32_t write_chunk_count = job->m_AssetChunkCount;
    uint32_t asset_chunk_count = job->m_VersionIndex->m_AssetChunkCounts[job->m_AssetIndex];
    const char* asset_path = &job->m_VersionIndex->m_NameData[job->m_VersionIndex->m_NameOffsets[job->m_AssetIndex]];

    if (!job->m_AssetOutputFile && job->m_AssetChunkIndexOffset)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) Skipping write to asset %s due to previous write failure", context, job_id, asset_path)
        for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
        {
            stored_block[d]->Dispose(stored_block[d]);
        }
        job->m_Err = ENOENT;
        return 0;
    }
    if (!job->m_AssetOutputFile)
    {
        char* full_asset_path = job->m_VersionStorageAPI->ConcatPath(job->m_VersionStorageAPI, job->m_VersionFolder, asset_path);
        int err = EnsureParentPathExists(job->m_VersionStorageAPI, full_asset_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) EnsureParentPathExists(%p, %s) failed with %d", context, job_id, job->m_VersionStorageAPI, full_asset_path, err)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
            job->m_Err = err;
            return 0;
        }
        if (IsDirPath(full_asset_path))
        {
            LONGTAIL_FATAL_ASSERT(block_block_reador_job_count == 0, job->m_Err = EINVAL; return 0)
            err = SafeCreateDir(job->m_VersionStorageAPI, full_asset_path);
            if (err)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) SafeCreateDir(%p, %s) failed with %d", context, job_id, job->m_VersionStorageAPI, full_asset_path, err)
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                job->m_Err = err;
                return 0;
            }
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            job->m_Err = 0;
            return 0;
        }

        uint64_t asset_size = job->m_VersionIndex->m_AssetSizes[job->m_AssetIndex];
        err = job->m_VersionStorageAPI->OpenWriteFile(job->m_VersionStorageAPI, full_asset_path, asset_size, &job->m_AssetOutputFile);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) job->m_VersionStorageAPI->OpenWriteFile(%p, %s, %" PRIu64 ", %p) failed with %d", context, job_id, job->m_VersionStorageAPI, full_asset_path, asset_size, &job->m_AssetOutputFile, err)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
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
            job->m_ContentLookup,
            job->m_AssetIndex,
            job->m_RetainPermissions,
            job,    // Reuse job
            write_chunk_index_offset + write_chunk_count,
            job->m_AssetOutputFile,
            &sync_write_job);

        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) CreatePartialAssetWriteJob(%p, %p, %p, %p, %p, %s, %p, %u, %u, %p, %u, %p, %p) failed with %d",
                context, job_id,
                job->m_BlockStoreAPI, job->m_VersionStorageAPI, job->m_JobAPI, job->m_ContentIndex, job->m_VersionIndex, job->m_VersionFolder, job->m_ContentLookup, job->m_AssetIndex, job->m_RetainPermissions, job, write_chunk_index_offset + write_chunk_count, job->m_AssetOutputFile, &sync_write_job,
                err)
            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
            job->m_Err = err;
            return 0;
        }
        // block_readion of blocks will start immediately
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

    while (chunk_index_offset < write_chunk_index_offset + write_chunk_count)
    {
        uint32_t chunk_index = job->m_VersionIndex->m_AssetChunkIndexes[chunk_index_start + chunk_index_offset];
        TLongtail_Hash chunk_hash = job->m_VersionIndex->m_ChunkHashes[chunk_index];
        intptr_t tmp;
        uint64_t content_chunk_index = hmget_ts(job->m_ContentLookup->m_ChunkHashToChunkIndex, chunk_hash, tmp);
        uint64_t block_index = job->m_ContentIndex->m_ChunkBlockIndexes[content_chunk_index];
        TLongtail_Hash block_hash = job->m_ContentIndex->m_BlockHashes[block_index];
        uint32_t block_readed_block_index = 0;
        while (block_hashes[block_readed_block_index] != block_hash)
        {
            if (block_readed_block_index == block_block_reador_job_count)
            {
                break;
            }
            ++block_readed_block_index;
        }
        if(block_readed_block_index == block_block_reador_job_count)
        {
            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;
            if (sync_write_job)
            {
                int err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(err == 0, job->m_Err = EINVAL; return 0)
            }
            job->m_Err = EINVAL;
            return 0;
        }
        char* block_data = (char*)stored_block[block_readed_block_index]->m_BlockData;

        uint32_t chunk_offset = job->m_ContentIndex->m_ChunkBlockOffsets[content_chunk_index];
        uint32_t chunk_size = job->m_ContentIndex->m_ChunkLengths[content_chunk_index];

        int err = job->m_VersionStorageAPI->Write(job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, chunk_size, &block_data[chunk_offset]);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) job->m_VersionStorageAPI->Write(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d",
                context, job_id,
                job->m_VersionStorageAPI, job->m_AssetOutputFile, write_offset, chunk_size, &block_data[chunk_offset],
                err)
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;

            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
            if (sync_write_job)
            {
                err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(err == 0, job->m_Err = err; return 0)
            }
            job->m_Err = err;
            return 0;
        }
        write_offset += chunk_size;

        ++chunk_index_offset;
    }

    for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
    {
        stored_block[d]->Dispose(stored_block[d]);
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) job->m_VersionStorageAPI->SetPermissions(%p, %s, %u) failed with %d",
                context, job_id,
                job->m_VersionStorageAPI, full_asset_path, (uint16_t)job->m_VersionIndex->m_Permissions[job->m_AssetIndex],
                err)
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
    struct HashToIndexItem* m_ContentChunkLookup;
    int m_RetainPermissions;
    int m_Err;
};

static int WriteAssetsFromBlock(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)

    struct WriteAssetsFromBlockJob* job = (struct WriteAssetsFromBlockJob*)context;
    struct Longtail_StorageAPI* version_storage_api = job->m_VersionStorageAPI;
    const char* version_folder = job->m_VersionFolder;
    const uint64_t block_index = job->m_BlockIndex;
    const struct Longtail_ContentIndex* content_index = job->m_ContentIndex;
    const struct Longtail_VersionIndex* version_index = job->m_VersionIndex;
    uint32_t* asset_indexes = job->m_AssetIndexes;
    uint32_t asset_count = job->m_AssetCount;
    struct HashToIndexItem* content_chunk_lookup = job->m_ContentChunkLookup;

    if (job->m_BlockReadJob.m_Err)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock(%p, %u) Failed to read block 0x%" PRIx64 ", %d", context, job_id, block_hash, job->m_BlockReadJob.m_Err)
        job->m_Err = job->m_BlockReadJob.m_Err;
        return 0;
    }

    const char* block_data = (char*)job->m_BlockReadJob.m_StoredBlock->m_BlockData;

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        uint32_t asset_index = asset_indexes[i];
        const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_folder, asset_path);
        int err = EnsureParentPathExists(version_storage_api, full_asset_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock(%p, %u) EnsureParentPathExists(%p, %s) failed with %d", context, job_id, version_storage_api, full_asset_path, err)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
            job->m_BlockReadJob.m_StoredBlock = 0;
            job->m_Err = err;
            return 0;
        }

        Longtail_StorageAPI_HOpenFile asset_file;
        err = version_storage_api->OpenWriteFile(version_storage_api, full_asset_path, 0, &asset_file);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock(%p, %u) version_storage_api->OpenWriteFile(%p, %s, %" PRIu64 ", %p) failed with %d", context, job_id, version_storage_api, full_asset_path, 0, &asset_file, err)
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
            job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
            job->m_BlockReadJob.m_StoredBlock = 0;
            job->m_Err = err;
            return 0;
        }

        uint64_t asset_write_offset = 0;
        uint32_t asset_chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        for (uint32_t asset_chunk_index = 0; asset_chunk_index < version_index->m_AssetChunkCounts[asset_index]; ++asset_chunk_index)
        {
            uint32_t chunk_index = version_index->m_AssetChunkIndexes[asset_chunk_index_start + asset_chunk_index];
            TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];

            ptrdiff_t tmp;
            uint64_t content_chunk_index = hmget_ts(content_chunk_lookup, chunk_hash, tmp);
            uint32_t chunk_block_offset = content_index->m_ChunkBlockOffsets[content_chunk_index];
            uint32_t chunk_size = content_index->m_ChunkLengths[content_chunk_index];
            err = version_storage_api->Write(version_storage_api, asset_file, asset_write_offset, chunk_size, &block_data[chunk_block_offset]);
            if (err)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock(%p, %u) version_storage_api->Write(%p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d", context, job_id, version_storage_api, asset_file, asset_write_offset, chunk_size, &block_data[chunk_block_offset], err)
                version_storage_api->CloseFile(version_storage_api, asset_file);
                asset_file = 0;
                Longtail_Free(full_asset_path);
                full_asset_path = 0;
                job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
                job->m_BlockReadJob.m_StoredBlock = 0;
                job->m_Err = err;
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks(%p, %u) job->m_VersionStorageAPI->SetPermissions(%p, %s, %u) failed with %d",
                    context, job_id,
                    version_storage_api, full_asset_path, (uint16_t)version_index->m_Permissions[asset_index],
                    err)
                job->m_BlockReadJob.m_StoredBlock->Dispose(job->m_BlockReadJob.m_StoredBlock);
                job->m_BlockReadJob.m_StoredBlock = 0;
                job->m_Err = err;
                return 0;
            }
        }
    }

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
    const TLongtail_Hash* chunk_hashes;
    struct ContentLookup* cl;
};

static SORTFUNC(BlockJobCompare)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

    struct BlockJobCompareContext* c = (struct BlockJobCompareContext*)context;
    struct HashToIndexItem* chunk_hash_to_block_index = c->cl->m_ChunkHashToBlockIndex;

    uint32_t a = *(const uint32_t*)a_ptr;
    uint32_t b = *(const uint32_t*)b_ptr;
    TLongtail_Hash a_first_chunk_hash = c->chunk_hashes[c->asset_chunk_index_starts[a]];
    TLongtail_Hash b_first_chunk_hash = c->chunk_hashes[c->asset_chunk_index_starts[b]];
    if (a_first_chunk_hash == b_first_chunk_hash)
    {
        return 0;
    }
    uint64_t a_block_index = hmget(chunk_hash_to_block_index, a_first_chunk_hash);
    uint64_t b_block_index = hmget(chunk_hash_to_block_index, b_first_chunk_hash);
    if (a_block_index == b_block_index)
    {
        return 0;
    }
    else if (a_block_index < b_block_index)
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
    size_t awl_size = sizeof(struct AssetWriteList) + sizeof(uint32_t) * asset_count + sizeof(uint32_t) * asset_count;
    struct AssetWriteList* awl = (struct AssetWriteList*)(Longtail_Alloc(awl_size));
    if (!awl)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CreateAssetWriteList(%u) Longtail_Alloc(%" PRIu64 ") failed with %d",
            asset_count,
            awl_size,
            ENOMEM)
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
    struct ContentLookup* cl,
    struct AssetWriteList** out_asset_write_list)
{
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || name_offsets != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || name_data != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || asset_chunk_counts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || asset_chunk_index_starts != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(asset_count == 0 || asset_chunk_indexes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(cl != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_asset_write_list != 0, return EINVAL)

    struct AssetWriteList* awl = CreateAssetWriteList(asset_count);
    if (awl == 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "BuildAssetWriteList(%u, %p, %p, %p, %p, %p, %p, %p, %p, %p) CreateAssetWriteList(%u) failed with %d",
            asset_count, optional_asset_indexes, name_offsets, name_data, chunk_hashes, asset_chunk_counts, asset_chunk_index_starts, asset_chunk_indexes, cl, out_asset_write_list,
            asset_count,
            ENOMEM)
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
        intptr_t find_i = hmgeti(cl->m_ChunkHashToBlockIndex, chunk_hash);
        if (find_i == -1)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "BuildAssetWriteList(%u, %p, %p, %p, %p, %p, %p, %p, %p, %p) Failed to find chunk 0x%" PRIx64 " in content index for asset %s",
                asset_count, optional_asset_indexes, name_offsets, name_data, chunk_hashes, asset_chunk_counts, asset_chunk_index_starts, asset_chunk_indexes, cl, out_asset_write_list,
                chunk_hash, path,
                ENOENT)
            Longtail_Free(awl);
            return ENOENT;
        }

        uint64_t content_block_index = cl->m_ChunkHashToBlockIndex[find_i].value;
        int is_block_job = 1;
        for (uint32_t c = 1; c < chunk_count; ++c)
        {
            uint32_t next_chunk_index = asset_chunk_indexes[asset_chunk_offset + c];
            TLongtail_Hash next_chunk_hash = chunk_hashes[next_chunk_index];
            find_i = hmgeti(cl->m_ChunkHashToBlockIndex, next_chunk_hash);
            if (find_i == -1)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "BuildAssetWriteList(%u, %p, %p, %p, %p, %p, %p, %p, %p, %p) Failed to find chunk 0x%" PRIx64 " in content index for asset %s",
                    asset_count, optional_asset_indexes, name_offsets, name_data, chunk_hashes, asset_chunk_counts, asset_chunk_index_starts, asset_chunk_indexes, cl, out_asset_write_list,
                    next_chunk_hash, path,
                    ENOENT)
                Longtail_Free(awl);
                awl = 0;
                return ENOENT;
            }
            uint64_t next_content_block_index = cl->m_ChunkHashToBlockIndex[find_i].value;
            if (content_block_index != next_content_block_index)
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
            chunk_hashes,   // chunk_hashes
            cl  // cl
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
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    struct ContentLookup* content_lookup,
    struct AssetWriteList* awl,
    int retain_permssions)
{
    LONGTAIL_FATAL_ASSERT(block_store_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_lookup != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(awl != 0, return EINVAL)

    const uint32_t worker_count = job_api->GetWorkerCount(job_api) + 1;
    const uint32_t max_parallell_block_read_jobs = worker_count < MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE ? worker_count : MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE;

    uint32_t asset_job_count = 0;
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        uint32_t asset_index = awl->m_AssetIndexJobs[a];
        uint32_t chunk_index_start = version_index->m_AssetChunkIndexStarts[asset_index];
        uint32_t chunk_start_index_offset = chunk_index_start;
        uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
        if (chunk_count == 0)
        {
            asset_job_count += 1;   // Write job
            continue;
        }

        uint32_t chunk_index_end = chunk_index_start + chunk_count;
        uint32_t chunk_index_offset = chunk_start_index_offset;

        while(chunk_index_offset != chunk_index_end)
        {
            uint32_t block_read_job_count = 0;
            TLongtail_Hash block_hashes[MAX_BLOCKS_PER_PARTIAL_ASSET_WRITE];
            while (chunk_index_offset != chunk_index_end && block_read_job_count < max_parallell_block_read_jobs)
            {
                uint32_t chunk_index = version_index->m_AssetChunkIndexes[chunk_index_offset];
                TLongtail_Hash chunk_hash = version_index->m_ChunkHashes[chunk_index];
                intptr_t tmp;
                uint64_t block_index = hmget_ts(content_lookup->m_ChunkHashToBlockIndex, chunk_hash, tmp);
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

    int err = job_api->ReserveJobs(job_api, (awl->m_BlockJobCount * 2u) + asset_job_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) job_api->ReserveJobs(%p, %u) failed with %d",
            block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
            job_api, (awl->m_BlockJobCount * 2u) + asset_job_count,
            err)
        Longtail_Free(awl);
        awl = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return err;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    LONGTAIL_FATAL_ASSERT(block_jobs != 0, return ENOMEM)
    uint32_t j = 0;
    uint32_t block_job_count = 0;
    while (j < awl->m_BlockJobCount)
    {
        uint32_t asset_index = awl->m_BlockJobAssetIndexes[j];
        TLongtail_Hash first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[asset_index]]];
        uint64_t block_index = hmget(content_lookup->m_ChunkHashToBlockIndex, first_chunk_hash);

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
        Longtail_JobAPI_Jobs block_readion_job;
        err = job_api->CreateJobs(job_api, 1, block_read_funcs, block_read_ctxs, &block_readion_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)

        job->m_VersionStorageAPI = version_storage_api;
        job->m_ContentIndex = content_index;
        job->m_VersionIndex = version_index;
        job->m_VersionFolder = version_path;
        job->m_BlockIndex = (uint64_t)block_index;
        job->m_ContentChunkLookup = content_lookup->m_ChunkHashToChunkIndex;
        job->m_AssetIndexes = &awl->m_BlockJobAssetIndexes[j];
        job->m_RetainPermissions = retain_permssions;
        job->m_Err = EINVAL;

        job->m_AssetCount = 1;
        ++j;
        while (j < awl->m_BlockJobCount)
        {
            uint32_t next_asset_index = awl->m_BlockJobAssetIndexes[j];
            TLongtail_Hash next_first_chunk_hash = version_index->m_ChunkHashes[version_index->m_AssetChunkIndexes[version_index->m_AssetChunkIndexStarts[next_asset_index]]];
            intptr_t next_block_index_ptr = hmgeti(content_lookup->m_ChunkHashToBlockIndex, next_first_chunk_hash);
            LONGTAIL_FATAL_ASSERT(-1 != next_block_index_ptr, return EINVAL)
            uint64_t next_block_index = content_lookup->m_ChunkHashToBlockIndex[next_block_index_ptr].value;
            if (block_index != next_block_index)
            {
                break;
            }

            ++job->m_AssetCount;
            ++j;
        }

        Longtail_JobAPI_JobFunc func[1] = { WriteAssetsFromBlock };
        void* ctx[1] = { job };

        Longtail_JobAPI_Jobs block_write_job;
        err = job_api->CreateJobs(job_api, 1, func, ctx, &block_write_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        err = job_api->AddDependecies(job_api, 1, block_write_job, 1, block_readion_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
        err = job_api->ReadyJobs(job_api, 1, block_readion_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
            asset_jobs_size,
            ENOMEM)
        Longtail_Free(block_jobs);
        block_jobs = 0;
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
            content_lookup,
            awl->m_AssetIndexJobs[a],
            retain_permssions,
            &asset_jobs[a],
            0,
            (Longtail_StorageAPI_HOpenFile)0,
            &write_sync_job);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) CreatePartialAssetWriteJob(%p, %p, %p, %p, %p, %s, %p, %p, %u, %p, %u, %p, %p) failed with %d",
                block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
                block_store_api, version_storage_api, job_api, content_index, version_index, version_path, content_lookup, awl->m_AssetIndexJobs[a], retain_permssions, asset_jobs[a], 0, (void*)0, &write_sync_job,
                err)
            Longtail_Free(asset_jobs);
            asset_jobs = 0;
            Longtail_Free(block_jobs);
            block_jobs = 0;
            return err;
        }
        err = job_api->ReadyJobs(job_api, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(err == 0, return err)
    }

    err = job_api->WaitForAllJobs(job_api, progress_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) job_api->WaitForAllJobs(%p, %p) failed with %d",
            block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
            job_api, progress_api,
            err)
        Longtail_Free(asset_jobs);
        asset_jobs = 0;
        Longtail_Free(block_jobs);
        block_jobs = 0;
        return err;
    }

    err = 0;
    for (uint32_t b = 0; b < block_job_count; ++b)
    {
        struct WriteAssetsFromBlockJob* job = &block_jobs[b];
        if (job->m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) Write single block 0x%" PRIx64 " assets to folder %s failed with %d",
                block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
                content_index->m_BlockHashes[job->m_BlockIndex], version_path,
                job->m_Err)
            err = err ? err : job->m_Err;
        }
    }
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        struct WritePartialAssetFromBlocksJob* job = &asset_jobs[a];
        if (job->m_Err)
        {
            const char* asset_path = &version_index->m_NameData[version_index->m_NameOffsets[job->m_AssetIndex]];
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %d) Write asset %s to folder %s failed with %d",
                block_store_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permssions,
                asset_path, version_path,
                job->m_Err)
            err = err ? err : job->m_Err;
        }
    }

    Longtail_Free(asset_jobs);
    asset_jobs = 0;
    Longtail_Free(block_jobs);
    block_jobs = 0;

    return err;
}

int Longtail_WriteVersion(
    struct Longtail_BlockStoreAPI* block_storage_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    int retain_permissions)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_WriteVersion(%p, %p, %p, %p, %p, %p, %s, %u)", block_storage_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, retain_permissions)
    LONGTAIL_VALIDATE_INPUT(block_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_path != 0, return EINVAL)

    if (*version_index->m_AssetCount == 0)
    {
        return 0;
    }
    struct ContentLookup* content_lookup;
    int err = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes,
        &content_lookup);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersion(%p, %p, %p, %p, %p, %p, %s, %u) CreateContentLookup(%" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p) failed with %d",
            block_storage_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, retain_permissions,
            *content_index->m_BlockCount, content_index->m_BlockHashes, *content_index->m_ChunkCount, content_index->m_ChunkHashes, content_index->m_ChunkBlockIndexes, &content_lookup,
            err)
        return err;
    }

    uint32_t asset_count = *version_index->m_AssetCount;

    struct AssetWriteList* awl;
    err = BuildAssetWriteList(
        asset_count,
        0,
        version_index->m_NameOffsets,
        version_index->m_NameData,
        version_index->m_ChunkHashes,
        version_index->m_AssetChunkCounts,
        version_index->m_AssetChunkIndexStarts,
        version_index->m_AssetChunkIndexes,
        content_lookup,
        &awl);

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersion(%p, %p, %p, %p, %p, %p, %s, %u) BuildAssetWriteList(%u, %p, %p, %p, %p, %p, %p, %p, %p, %p) failed with %d",
            block_storage_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, retain_permissions,
            asset_count, (void*)0, version_index->m_NameOffsets, version_index->m_NameData, version_index->m_ChunkHashes, version_index->m_AssetChunkCounts, version_index->m_AssetChunkIndexStarts, version_index->m_AssetChunkIndexes, content_lookup, &awl,
            err)
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return err;
    }

    err = WriteAssets(
        block_storage_api,
        version_storage_api,
        job_api,
        progress_api,
        content_index,
        version_index,
        version_path,
        content_lookup,
        awl,
        retain_permissions);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersion(%p, %p, %p, %p, %p, %p, %s, %u) WriteAssets(%p, %p, %p, %p, %p, %p, %s, %p, %p, %u) failed with %d",
            block_storage_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, retain_permissions,
            block_storage_api, version_storage_api, job_api, progress_api, content_index, version_index, version_path, content_lookup, awl, retain_permissions,
            err)
    }

    Longtail_Free(awl);
    awl = 0;

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    return err;
}

static int CompareHash(const void* a_ptr, const void* b_ptr) 
{
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

    TLongtail_Hash a = *((const TLongtail_Hash*)a_ptr);
    TLongtail_Hash b = *((const TLongtail_Hash*)b_ptr);
    if (a > b) return  1;
    if (a < b) return -1;
    return 0;
}

static uint64_t MakeUnique(TLongtail_Hash* hashes, uint64_t count)
{
    LONGTAIL_FATAL_ASSERT(count == 0 || hashes != 0, return 0)

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
    LONGTAIL_FATAL_ASSERT(reference_hash_count == 0 || reference_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(new_hash_count == 0 || added_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(added_hash_count != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(added_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT((removed_hash_count == 0 && removed_hashes == 0) || (removed_hash_count != 0 && removed_hashes != 0), return EINVAL)

    size_t refs_size = sizeof(TLongtail_Hash) * reference_hash_count;
    TLongtail_Hash* refs = (TLongtail_Hash*)Longtail_Alloc(refs_size);
    if (!refs)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DiffHashes(%p, %" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            reference_hashes, reference_hash_count, new_hashes, new_hash_count, added_hash_count, added_hashes, removed_hash_count, removed_hashes,
            refs_size,
            ENOMEM)
        return ENOMEM;
    }

    size_t news_size = sizeof(TLongtail_Hash) * new_hash_count;
    TLongtail_Hash* news = (TLongtail_Hash*)Longtail_Alloc(news_size);
    if (!news)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DiffHashes(%p, %" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            reference_hashes, reference_hash_count, new_hashes, new_hash_count, added_hash_count, added_hashes, removed_hash_count, removed_hashes,
            news_size,
            ENOMEM)
        Longtail_Free(refs);
        refs = 0;
        return ENOMEM;
    }

    memmove(refs, reference_hashes, (size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    memmove(news, new_hashes, (size_t)(sizeof(TLongtail_Hash) * new_hash_count));

    qsort(&refs[0], (size_t)reference_hash_count, sizeof(TLongtail_Hash), CompareHash);
    reference_hash_count = MakeUnique(&refs[0], reference_hash_count);

    qsort(&news[0], (size_t)new_hash_count, sizeof(TLongtail_Hash), CompareHash);
    new_hash_count = MakeUnique(&news[0], new_hash_count);

    uint64_t removed = 0;
    uint64_t added = 0;
    uint64_t ni = 0;
    uint64_t ri = 0;
    while (ri < reference_hash_count && ni < new_hash_count)
    {
        if (refs[ri] == news[ni])
        {
            ++ri;
            ++ni;
            continue;
        }
        else if (refs[ri] < news[ni])
        {
            if (removed_hashes)
            {
                removed_hashes[removed] = refs[ri];
            }
            ++removed;
            ++ri;
        }
        else if (refs[ri] > news[ni])
        {
            added_hashes[added++] = news[ni++];
        }
    }
    while (ni < new_hash_count)
    {
        added_hashes[added++] = news[ni++];
    }
    *added_hash_count = added;
    while (ri < reference_hash_count)
    {
        if (removed_hashes)
        {
            removed_hashes[removed] = refs[ri];
        }
        ++removed;
        ++ri;
    }
    if (removed_hash_count)
    {
        *removed_hash_count = removed;
    }

    Longtail_Free(news);
    news = 0;
    Longtail_Free(refs);
    refs = 0;

    if (added > 0)
    {
        // Reorder the new hashes so they are in the same order that they where when they were created
        // so chunks that belongs together are group together in blocks
        struct HashToIndexItem* added_hashes_lookup = 0;
        for (uint64_t i = 0; i < added; ++i)
        {
            hmput(added_hashes_lookup, added_hashes[i], i);
        }
        added = 0;
        for (uint64_t i = 0; i < new_hash_count; ++i)
        {
            TLongtail_Hash hash = new_hashes[i];
            intptr_t hash_ptr = hmgeti(added_hashes_lookup, hash);
            if (hash_ptr == -1)
            {
                continue;
            }
            added_hashes[added++] = hash;
        }
        hmfree(added_hashes_lookup);
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p)", hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(max_block_size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(max_chunks_per_block != 0, return EINVAL)

    uint64_t chunk_count = *version_index->m_ChunkCount;
    size_t added_hashes_size = sizeof(TLongtail_Hash) * chunk_count;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)Longtail_Alloc(added_hashes_size);
    if (!added_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
            added_hashes_size,
            ENOMEM)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) DiffHashes(%p, %" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p, %p) failed with %d",
            hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
            content_index->m_ChunkHashes, *content_index->m_ChunkCount, version_index->m_ChunkHashes, chunk_count, &added_hash_count, added_hashes, (void*)0, (void*)0,
            err)
        Longtail_Free(added_hashes);
        return err;
    }

    if (added_hash_count == 0)
    {
        Longtail_Free(added_hashes);
        added_hashes = 0;
        err = Longtail_CreateContentIndex(
            hash_api,
            0,
            max_block_size,
            max_chunks_per_block,
            out_content_index);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %p, %u, %u, %p) failed with %d",
                hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
                hash_api, 0, (void*)0, (void*)0, (void*)0, max_block_size, max_chunks_per_block, out_content_index,
                err)
        }
        return err;
    }

    size_t diff_chunk_sizes_size = sizeof(uint32_t) * added_hash_count;
    uint32_t* diff_chunk_sizes = (uint32_t*)Longtail_Alloc(diff_chunk_sizes_size);
    if (!diff_chunk_sizes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
            diff_chunk_sizes_size,
            ENOMEM)
        return ENOMEM;
    }
    size_t diff_chunk_tags_size = sizeof(uint32_t) * added_hash_count;
    uint32_t* diff_chunk_tags = (uint32_t*)Longtail_Alloc(diff_chunk_tags_size);
    if (!diff_chunk_sizes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
            diff_chunk_tags_size,
            ENOMEM)
        Longtail_Free(diff_chunk_sizes);
        diff_chunk_sizes = 0;
        return ENOMEM;
    }

    struct HashToIndexItem* chunk_index_lookup = 0;
    for (uint64_t i = 0; i < chunk_count; ++i)
    {
        hmput(chunk_index_lookup, version_index->m_ChunkHashes[i], i);
    }

    for (uint32_t j = 0; j < added_hash_count; ++j)
    {
        uint64_t chunk_index = hmget(chunk_index_lookup, added_hashes[j]);
        diff_chunk_sizes[j] = version_index->m_ChunkSizes[chunk_index];
        diff_chunk_tags[j] = version_index->m_ChunkTags[chunk_index];
    }
    hmfree(chunk_index_lookup);
    chunk_index_lookup = 0;

    err = Longtail_CreateContentIndexRaw(
        hash_api,
        added_hash_count,
        added_hashes,
        diff_chunk_sizes,
        diff_chunk_tags,
        max_block_size,
        max_chunks_per_block,
        out_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateMissingContent(%p, %p, %p, %u, %u, %p) Longtail_CreateContentIndexRaw(%p, %" PRIu64 ", %p, %p, %p, %p, %u, %u, %p) failed with %d",
            hash_api, content_index, version_index, max_block_size, max_chunks_per_block, out_content_index,
            hash_api, added_hash_count, added_hashes, diff_chunk_sizes, diff_chunk_tags, max_block_size, max_chunks_per_block, out_content_index,
            err)
    }

    Longtail_Free(diff_chunk_tags);
    diff_chunk_tags = 0;
    Longtail_Free(diff_chunk_sizes);
    diff_chunk_sizes = 0;
    Longtail_Free(added_hashes);
    added_hashes = 0;

    return err;
}

int Longtail_RetargetContent(
    const struct Longtail_ContentIndex* reference_content_index,
    const struct Longtail_ContentIndex* content_index,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_RetargetContent(%p, %p, %p)", reference_content_index, content_index, out_content_index)
    LONGTAIL_VALIDATE_INPUT(reference_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((*reference_content_index->m_HashAPI) == (*content_index->m_HashAPI), return EINVAL)

    struct HashToIndexItem* chunk_to_reference_block_index_lookup = 0;
    uint64_t reference_chunk_count = *reference_content_index->m_ChunkCount;
    for (uint64_t i = 0; i < reference_chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = reference_content_index->m_ChunkHashes[i];
        uint64_t block_index = reference_content_index->m_ChunkBlockIndexes[i];
        hmput(chunk_to_reference_block_index_lookup, chunk_hash, block_index);
    }

    size_t requested_block_hashes_size = sizeof(TLongtail_Hash) * *reference_content_index->m_BlockCount;
    TLongtail_Hash* requested_block_hashes = (TLongtail_Hash*)Longtail_Alloc(requested_block_hashes_size);
    if (requested_block_hashes == 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            reference_content_index, content_index, out_content_index,
            requested_block_hashes_size,
            ENOMEM)
        hmfree(chunk_to_reference_block_index_lookup);
        return ENOMEM;
    }

    uint64_t requested_block_count = 0;
    struct HashToIndexItem* requested_blocks_lookup = 0;
    uint64_t content_index_chunk_count = *content_index->m_ChunkCount;
    for (uint32_t i = 0; i < content_index_chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
        intptr_t reference_block_index_ptr = hmgeti(chunk_to_reference_block_index_lookup, chunk_hash);
        if (reference_block_index_ptr == -1)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent(%p, %p, %p) reference content does not contain the chunk 0x%" PRIx64 "", reference_content_index, content_index, out_content_index, chunk_hash)
            hmfree(requested_blocks_lookup);
            requested_blocks_lookup = 0;
            Longtail_Free(requested_block_hashes);
            requested_block_hashes = 0;
            hmfree(chunk_to_reference_block_index_lookup);
            chunk_to_reference_block_index_lookup = 0;
            return EINVAL;
        }
        uint64_t reference_block_index = chunk_to_reference_block_index_lookup[reference_block_index_ptr].value;
        TLongtail_Hash reference_block_hash = reference_content_index->m_BlockHashes[reference_block_index];

        intptr_t request_block_index_ptr = hmgeti(requested_blocks_lookup, reference_block_hash);
        if (-1 == request_block_index_ptr)
        {
            requested_block_hashes[requested_block_count] = reference_block_hash;
            hmput(requested_blocks_lookup, reference_block_hash, requested_block_count);
            ++requested_block_count;
        }
    }
    hmfree(chunk_to_reference_block_index_lookup);
    chunk_to_reference_block_index_lookup = 0;

    uint64_t chunk_count = 0;
    uint64_t reference_content_chunk_count = *reference_content_index->m_ChunkCount;
    for (uint64_t c = 0; c < reference_content_chunk_count; ++c)
    {
        TLongtail_Hash block_hash = reference_content_index->m_BlockHashes[reference_content_index->m_ChunkBlockIndexes[c]];
        if (-1 == hmgeti(requested_blocks_lookup, block_hash))
        {
            continue;
        }
        ++chunk_count;
    }

    size_t content_index_size = Longtail_GetContentIndexSize(requested_block_count, chunk_count);
    struct Longtail_ContentIndex* resulting_content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!resulting_content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            reference_content_index, content_index, out_content_index,
            content_index_size,
            ENOMEM)
        hmfree(chunk_to_reference_block_index_lookup);
        chunk_to_reference_block_index_lookup = 0;
        Longtail_Free(requested_block_hashes);
        requested_block_hashes = 0;
        return ENOMEM;
    }
    int err = Longtail_InitContentIndex(
        resulting_content_index,
        &resulting_content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        *reference_content_index->m_HashAPI,
        *reference_content_index->m_MaxBlockSize,
        *reference_content_index->m_MaxChunksPerBlock,
        requested_block_count,
        chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent(%p, %p, %p) Longtail_InitContentIndex(%p, %p, %" PRIu64 ", %u, %u, %u, %" PRIu64 ", %" PRIu64 ") failed with %d",
            reference_content_index, content_index, out_content_index,
            resulting_content_index, &resulting_content_index[1], content_index_size - sizeof(struct Longtail_ContentIndex), *reference_content_index->m_HashAPI, *reference_content_index->m_MaxBlockSize, *reference_content_index->m_MaxChunksPerBlock, requested_block_count, chunk_count,
            err)

        Longtail_Free(resulting_content_index);
        resulting_content_index = 0;
        hmfree(requested_blocks_lookup);
        requested_blocks_lookup = 0;
        Longtail_Free(requested_block_hashes);
        requested_block_hashes = 0;
        return err;
    }

    memmove(resulting_content_index->m_BlockHashes, requested_block_hashes, sizeof(TLongtail_Hash) * requested_block_count);

    uint64_t chunk_index = 0;
    for (uint64_t c = 0; c < reference_content_chunk_count; ++c)
    {
        TLongtail_Hash block_hash = reference_content_index->m_BlockHashes[reference_content_index->m_ChunkBlockIndexes[c]];
        intptr_t block_index_ptr = hmgeti(requested_blocks_lookup, block_hash);
        if (-1 == block_index_ptr)
        {
            continue;
        }
        TLongtail_Hash chunk_hash = reference_content_index->m_ChunkHashes[c];
        uint32_t chunk_length = reference_content_index->m_ChunkLengths[c];
        uint32_t chunk_block_offset = reference_content_index->m_ChunkBlockOffsets[c];
        uint64_t block_index = requested_blocks_lookup[block_index_ptr].value;
        resulting_content_index->m_ChunkBlockIndexes[chunk_index] = block_index;
        resulting_content_index->m_ChunkHashes[chunk_index] = chunk_hash;
        resulting_content_index->m_ChunkBlockOffsets[chunk_index] = chunk_block_offset;
        resulting_content_index->m_ChunkLengths[chunk_index] = chunk_length;
        ++chunk_index;
    }

    hmfree(requested_blocks_lookup);
    requested_blocks_lookup = 0;
    Longtail_Free(requested_block_hashes);
    requested_block_hashes = 0;
    *out_content_index = resulting_content_index;
    return 0;
}

int Longtail_MergeContentIndex(
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* new_content_index,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_MergeContentIndex(%p, %p, %p)", local_content_index, new_content_index, out_content_index)
    LONGTAIL_VALIDATE_INPUT(local_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(new_content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT((*local_content_index->m_HashAPI) == (*new_content_index->m_HashAPI), return EINVAL)

    uint64_t max_block_count = *local_content_index->m_BlockCount + *new_content_index->m_BlockCount;
    uint64_t max_chunk_count = *local_content_index->m_ChunkCount + *new_content_index->m_ChunkCount;

    size_t compact_block_hashes_size = sizeof(TLongtail_Hash) * max_block_count;
    TLongtail_Hash* compact_block_hashes = (TLongtail_Hash*)Longtail_Alloc(compact_block_hashes_size);
    if (!compact_block_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_content_index, new_content_index, out_content_index,
            compact_block_hashes_size,
            ENOMEM)
        return ENOMEM;
    }
    uint64_t compact_block_count = 0;

    size_t compact_chunk_hashes_size = sizeof(TLongtail_Hash) * max_chunk_count;
    TLongtail_Hash* compact_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(compact_chunk_hashes_size);
    if (!compact_block_hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_content_index, new_content_index, out_content_index,
            compact_chunk_hashes,
            ENOMEM)
        Longtail_Free(compact_block_hashes);
        return ENOMEM;
    }
    uint64_t compact_chunk_count = 0;

    size_t compact_chunk_offsets_size = sizeof(uint32_t) * max_chunk_count;
    uint32_t* compact_chunk_offsets = (uint32_t*)Longtail_Alloc(compact_chunk_offsets_size);
    if (!compact_chunk_offsets)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_content_index, new_content_index, out_content_index,
            compact_chunk_offsets_size,
            ENOMEM)
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(compact_block_hashes);
        return ENOMEM;
    }
    size_t compact_chunk_sizes_size = sizeof(uint32_t) * max_chunk_count;
    uint32_t* compact_chunk_sizes = (uint32_t*)Longtail_Alloc(compact_chunk_sizes_size);
    if (!compact_chunk_sizes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_content_index, new_content_index, out_content_index,
            compact_chunk_sizes_size,
            ENOMEM)
        Longtail_Free(compact_chunk_offsets);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(compact_block_hashes);
        return ENOMEM;
    }

    struct HashToIndexItem* chunk_hash_to_block_index = 0;
    struct HashToIndexItem* block_hash_to_block_index = 0;
    uint64_t new_content_chunk_count = *new_content_index->m_ChunkCount;
    for (uint64_t c = 0; c < new_content_chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = new_content_index->m_ChunkHashes[c];
        intptr_t find_block_ptr = hmgeti(chunk_hash_to_block_index, chunk_hash);
        if (find_block_ptr != -1)
        {
            continue;
        }
        uint64_t block_index = new_content_index->m_ChunkBlockIndexes[c];
        TLongtail_Hash block_hash = new_content_index->m_BlockHashes[block_index];
        uint32_t block_offset = new_content_index->m_ChunkBlockOffsets[c];
        uint32_t chunk_size = new_content_index->m_ChunkLengths[c];

        compact_chunk_offsets[compact_chunk_count] = block_offset;
        compact_chunk_sizes[compact_chunk_count] = chunk_size;
        compact_chunk_hashes[compact_chunk_count++] = chunk_hash;

        intptr_t find_block_index_ptr = hmgeti(block_hash_to_block_index, block_hash);
        if (find_block_index_ptr == -1)
        {
            hmput(block_hash_to_block_index, block_hash, compact_block_count);
            hmput(chunk_hash_to_block_index, chunk_hash, compact_block_count);
            compact_block_hashes[compact_block_count++] = block_hash;
            continue;
        }
        hmput(chunk_hash_to_block_index, chunk_hash, block_hash_to_block_index[find_block_index_ptr].value);
    }

    uint64_t local_content_chunk_count = *local_content_index->m_ChunkCount;
    for (uint64_t c = 0; c < local_content_chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = local_content_index->m_ChunkHashes[c];
        intptr_t find_block_ptr = hmgeti(chunk_hash_to_block_index, chunk_hash);
        if (find_block_ptr != -1)
        {
            continue;
        }
        uint64_t block_index = local_content_index->m_ChunkBlockIndexes[c];
        TLongtail_Hash block_hash = local_content_index->m_BlockHashes[block_index];
        uint32_t block_offset = local_content_index->m_ChunkBlockOffsets[c];
        uint32_t chunk_size = local_content_index->m_ChunkLengths[c];

        compact_chunk_offsets[compact_chunk_count] = block_offset;
        compact_chunk_sizes[compact_chunk_count] = chunk_size;
        compact_chunk_hashes[compact_chunk_count++] = chunk_hash;

        intptr_t find_block_index_ptr = hmgeti(block_hash_to_block_index, block_hash);
        if (find_block_index_ptr == -1)
        {
            hmput(block_hash_to_block_index, block_hash, compact_block_count);
            hmput(chunk_hash_to_block_index, chunk_hash, compact_block_count);
            compact_block_hashes[compact_block_count++] = block_hash;
            continue;
        }
        hmput(chunk_hash_to_block_index, chunk_hash, block_hash_to_block_index[find_block_index_ptr].value);
    }

    size_t content_index_size = Longtail_GetContentIndexSize(compact_block_count, compact_chunk_count);
    struct Longtail_ContentIndex* compact_content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!compact_content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            local_content_index, new_content_index, out_content_index,
            content_index_size,
            ENOMEM)
        hmfree(block_hash_to_block_index);
        Longtail_Free(compact_chunk_sizes);
        Longtail_Free(compact_chunk_offsets);
        hmfree(chunk_hash_to_block_index);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(compact_block_hashes);
        return ENOMEM;
    }
    int err = Longtail_InitContentIndex(
        compact_content_index,
        &compact_content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        *new_content_index->m_HashAPI,
        *new_content_index->m_MaxBlockSize,
        *new_content_index->m_MaxChunksPerBlock,
        compact_block_count,
        compact_chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex(%p, %p, %p) Longtail_InitContentIndex(%p, %p, %" PRIu64 ", %u, %u, %u, %" PRIu64 ", %" PRIu64 " ) failed with %d",
            local_content_index, new_content_index, out_content_index,
            compact_content_index, &compact_content_index[1], content_index_size - sizeof(struct Longtail_ContentIndex), *new_content_index->m_HashAPI, *new_content_index->m_MaxBlockSize, *new_content_index->m_MaxChunksPerBlock, compact_block_count, compact_chunk_count,
            err)
        Longtail_Free(compact_content_index);
        hmfree(block_hash_to_block_index);
        Longtail_Free(compact_chunk_sizes);
        Longtail_Free(compact_chunk_offsets);
        hmfree(chunk_hash_to_block_index);
        Longtail_Free(compact_chunk_hashes);
        Longtail_Free(compact_block_hashes);
        return err;
    }

    memcpy(compact_content_index->m_BlockHashes, compact_block_hashes, sizeof(TLongtail_Hash) * compact_block_count);
    memcpy(compact_content_index->m_ChunkHashes, compact_chunk_hashes, sizeof(TLongtail_Hash) * compact_chunk_count);
    for (uint64_t c = 0; c < compact_chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = compact_chunk_hashes[c];
        uint64_t block_index = hmget(chunk_hash_to_block_index, chunk_hash);
        uint32_t chunk_offset = compact_chunk_offsets[c];
        uint32_t chunk_size = compact_chunk_sizes[c];
        compact_content_index->m_ChunkBlockIndexes[c] = block_index;
        compact_content_index->m_ChunkBlockOffsets[c] = chunk_offset;
        compact_content_index->m_ChunkLengths[c] = chunk_size;
    }
    *out_content_index = compact_content_index;
    hmfree(block_hash_to_block_index);
    Longtail_Free(compact_chunk_sizes);
    Longtail_Free(compact_chunk_offsets);
    hmfree(chunk_hash_to_block_index);
    Longtail_Free(compact_chunk_hashes);
    Longtail_Free(compact_block_hashes);
    return 0;
}

static int CompareHashes(const void* a_ptr, const void* b_ptr)
{
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

    TLongtail_Hash a = *(const TLongtail_Hash*)a_ptr;
    TLongtail_Hash b = *(const TLongtail_Hash*)b_ptr;
    return (a > b) ? 1 : (a < b) ? -1 : 0;
}

static SORTFUNC(SortPathShortToLong)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

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
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

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
    LONGTAIL_FATAL_ASSERT(version_diff != 0, return)

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
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    struct Longtail_VersionDiff** out_version_diff)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff(%p, %p, %p)", source_version, target_version, out_version_diff)
    LONGTAIL_VALIDATE_INPUT(source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_version_diff != 0, return EINVAL)

    struct HashToIndexItem* source_path_hash_to_index = 0;
    struct HashToIndexItem* target_path_hash_to_index = 0;

    uint32_t source_asset_count = *source_version->m_AssetCount;
    uint32_t target_asset_count = *target_version->m_AssetCount;

    uint32_t hashes_count = source_asset_count + target_asset_count;
    size_t hashes_size = sizeof(TLongtail_Hash) * hashes_count;
    TLongtail_Hash* hashes = (TLongtail_Hash*)Longtail_Alloc(hashes_size);
    if (!hashes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionDiff(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            source_version, target_version, out_version_diff,
            hashes_size,
            ENOMEM)
        return ENOMEM;
    }

    TLongtail_Hash* source_path_hashes = &hashes[0];
    TLongtail_Hash* target_path_hashes = &hashes[source_asset_count];

    for (uint32_t i = 0; i < source_asset_count; ++i)
    {
        TLongtail_Hash path_hash = source_version->m_PathHashes[i];
        source_path_hashes[i] = path_hash;
        hmput(source_path_hash_to_index, path_hash, i);
    }

    for (uint32_t i = 0; i < target_asset_count; ++i)
    {
        TLongtail_Hash path_hash = target_version->m_PathHashes[i];
        target_path_hashes[i] = path_hash;
        hmput(target_path_hash_to_index, path_hash, i);
    }

    qsort(source_path_hashes, source_asset_count, sizeof(TLongtail_Hash), CompareHashes);
    qsort(target_path_hashes, target_asset_count, sizeof(TLongtail_Hash), CompareHashes);

    const uint32_t max_modified_content_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    const uint32_t max_modified_permission_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    const uint32_t indexes_count = source_asset_count + target_asset_count + max_modified_content_count + max_modified_content_count + max_modified_permission_count + max_modified_permission_count;

    size_t indexes_size = sizeof(uint32_t) * indexes_count;
    uint32_t* indexes = (uint32_t*)Longtail_Alloc(indexes_size);
    if (!indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionDiff(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            source_version, target_version, out_version_diff,
            indexes_size,
            ENOMEM)
        Longtail_Free(hashes);
        return ENOMEM;
    }

    uint32_t* indexes_ptr = &indexes[0];

    uint32_t* removed_source_asset_indexes = indexes_ptr;
    indexes_ptr += source_asset_count;
    uint32_t* added_target_asset_indexes = indexes_ptr;
    indexes_ptr += target_asset_count;

    uint32_t* modified_source_content_indexes = indexes_ptr;
    indexes_ptr += max_modified_content_count;
    uint32_t* modified_target_content_indexes = indexes_ptr;
    indexes_ptr += max_modified_content_count;

    uint32_t* modified_source_permissions_indexes = indexes_ptr;
    indexes_ptr += max_modified_permission_count;
    uint32_t* modified_target_permissions_indexes = indexes_ptr;
    indexes_ptr += max_modified_permission_count;

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
        uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
        uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);

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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching content for asset %s", source_path)
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
                    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching permissions for asset %s", source_path)
                }
            }

            ++source_index;
            ++target_index;
        }
        else if (source_path_hash < target_path_hash)
        {
            source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Removed asset %s", source_path)
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        }
        else
        {
            target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Added asset %s", target_path)
            added_target_asset_indexes[target_added_count] = target_asset_index;
            ++target_added_count;
            ++target_index;
        } 
    }
    while (source_index < source_asset_count)
    {
        // source_path_hash removed
        TLongtail_Hash source_path_hash = source_path_hashes[source_index];
        uint32_t source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
        const char* source_path = &source_version->m_NameData[source_version->m_NameOffsets[source_asset_index]];
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Removed asset %s", source_path)
        removed_source_asset_indexes[source_removed_count] = source_asset_index;
        ++source_removed_count;
        ++source_index;
    }
    while (target_index < target_asset_count)
    {
        // target_path_hash added
        TLongtail_Hash target_path_hash = target_path_hashes[target_index];
        uint32_t target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
        const char* target_path = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Added asset %s", target_path)
        added_target_asset_indexes[target_added_count] = target_asset_index;
        ++target_added_count;
        ++target_index;
    }
    if (source_removed_count > 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Found %u removed assets", source_removed_count)
    }
    if (target_added_count > 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Found %u added assets", target_added_count)
    }
    if (modified_content_count > 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching content for %u assets found", modified_content_count)
    }
    if (modified_permissions_count > 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Mismatching permission for %u assets found", modified_permissions_count)
    }

    size_t version_diff_size = GetVersionDiffSize(source_removed_count, target_added_count, modified_content_count, modified_permissions_count);
    struct Longtail_VersionDiff* version_diff = (struct Longtail_VersionDiff*)Longtail_Alloc(version_diff_size);
    if (!version_diff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateVersionDiff(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            source_version, target_version, out_version_diff,
            version_diff_size,
            ENOMEM)
        Longtail_Free(indexes);
        Longtail_Free(hashes);
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

    Longtail_Free(indexes);
    indexes = 0;

    Longtail_Free(hashes);
    hashes = 0;

    hmfree(target_path_hash_to_index);
    target_path_hash_to_index = 0;

    hmfree(source_path_hash_to_index);
    source_path_hash_to_index = 0;

    *out_version_diff = version_diff;
    return 0;
}

int Longtail_ChangeVersion(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path,
    int retain_permissions)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u)",
        block_store_api,
        version_storage_api,
        hash_api,
        job_api,
        progress_api,
        content_index,
        source_version,
        target_version,
        version_diff,
        version_path,
        retain_permissions)
    LONGTAIL_VALIDATE_INPUT(block_store_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_storage_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(source_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(target_version != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_diff != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_path != 0, return EINVAL)

    int err = EnsureParentPathExists(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) EnsureParentPathExists(%p, %s) failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            version_storage_api, version_path,
            err)
        return err;
    }
    err = SafeCreateDir(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) SafeCreateDir(%p, %s) failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            version_storage_api, version_path,
            err)
        return err;
    }
    struct ContentLookup* content_lookup;
    err = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes,
        &content_lookup);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) CreateContentLookup(%" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p) failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            *content_index->m_BlockCount, content_index->m_BlockHashes, *content_index->m_ChunkCount, content_index->m_ChunkHashes, content_index->m_ChunkBlockIndexes, &content_lookup,
            err)
        return err;
    }

    uint32_t target_version_chunk_count = *target_version->m_ChunkCount;
    for (uint32_t i = 0; i < target_version_chunk_count; ++i)
    {
        TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[i];
        intptr_t chunk_content_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
        if (-1 == chunk_content_index_ptr)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) can't find chunk 0x%" PRIx64 " in content index",
                block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
                chunk_hash)
            DeleteContentLookup(content_lookup);
            content_lookup = 0;
            return EINVAL;
       }
    }

    uint32_t retry_count = 10;
    uint32_t successful_remove_count = 0;
    uint32_t removed_count = *version_diff->m_SourceRemovedCount;
    while (successful_remove_count < removed_count)
    {
        --retry_count;
        for (uint32_t r = 0; r < removed_count; ++r)
        {
            uint32_t asset_index = version_diff->m_SourceRemovedAssetIndexes[r];
            const char* asset_path = &source_version->m_NameData[source_version->m_NameOffsets[asset_index]];
            char* full_asset_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            if (IsDirPath(asset_path))
            {
                full_asset_path[strlen(full_asset_path) - 1] = '\0';
                err = version_storage_api->RemoveDir(version_storage_api, full_asset_path);
                if (err)
                {
                    if (version_storage_api->IsDir(version_storage_api, full_asset_path))
                    {
                        if (!retry_count)
                        {
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to remove directory %s, %d", full_asset_path, err)
                            Longtail_Free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(content_lookup);
                            content_lookup = 0;
                            return err;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        break;
                    }
                }
            }
            else
            {
                err = version_storage_api->RemoveFile(version_storage_api, full_asset_path);
                if (err)
                {
                    if (version_storage_api->IsFile(version_storage_api, full_asset_path))
                    {
                        if (!retry_count)
                        {
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to remove file %s, %d", full_asset_path, err)
                            Longtail_Free(full_asset_path);
                            full_asset_path = 0;
                            DeleteContentLookup(content_lookup);
                            content_lookup = 0;
                            return err;
                        }
                        Longtail_Free(full_asset_path);
                        full_asset_path = 0;
                        break;
                    }
                }
            }
            ++successful_remove_count;
            Longtail_Free(full_asset_path);
            full_asset_path = 0;
        }
        if (successful_remove_count < removed_count)
        {
            --retry_count;
            if (retry_count == 1)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ChangeVersion: Retrying removal of remaning %u assets in %s", removed_count - successful_remove_count, version_path)
            }
        }
    }

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t write_asset_count = added_count + modified_content_count;

    size_t asset_indexes_size = sizeof(uint32_t) * write_asset_count;
    uint32_t* asset_indexes = (uint32_t*)Longtail_Alloc(asset_indexes_size);
    if (!asset_indexes)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) Longtail_Alloc(%" PRIu64 ") failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            asset_indexes_size,
            ENOMEM)
        DeleteContentLookup(content_lookup);
        return ENOMEM;
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
        content_lookup,
        &awl);

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) BuildAssetWriteList(%u, %p, %p, %p, %p, %p, %p, %p, %p, %p) failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            write_asset_count, asset_indexes, target_version->m_NameOffsets, target_version->m_NameData, target_version->m_ChunkHashes, target_version->m_AssetChunkCounts, target_version->m_AssetChunkIndexStarts, target_version->m_AssetChunkIndexes, content_lookup, &awl,
            err)
        Longtail_Free(asset_indexes);
        asset_indexes = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return err;
    }

    Longtail_Free(asset_indexes);
    asset_indexes = 0;

    err = WriteAssets(
        block_store_api,
        version_storage_api,
        job_api,
        progress_api,
        content_index,
        target_version,
        version_path,
        content_lookup,
        awl,
        retain_permissions);

    Longtail_Free(awl);
    awl = 0;

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) WriteAssets(%p, %p, %p, %p, %p, %p, %p, %p, %p, %p, %d) failed with %d",
            block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
            block_store_api, version_storage_api, job_api, progress_api, content_index, target_version, version_path, content_lookup, awl, retain_permissions,
            err)
        return err;
    }

    if (retain_permissions)
    {
        uint32_t version_diff_modified_permissions_count = *version_diff->m_ModifiedPermissionsCount;
        for (uint32_t i = 0; i < version_diff_modified_permissions_count; ++i)
        {
            uint32_t asset_index = version_diff->m_TargetPermissionsModifiedAssetIndexes[i];
            const char* asset_path = &target_version->m_NameData[target_version->m_NameOffsets[asset_index]];
            char* full_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            uint16_t permissions = (uint16_t)target_version->m_Permissions[asset_index];
            err = version_storage_api->SetPermissions(version_storage_api, full_path, permissions);
            if (err)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion(%p, %p, %p, %p, %p, %p, %p, %p, %p, %s, %u) version_storage_api->SetPermissions(%p, %s, %u) failed with %d",
                    block_store_api, version_storage_api, hash_api, job_api, progress_api, content_index, source_version, target_version, version_diff, version_path, retain_permissions,
                    version_storage_api, full_path, permissions,
                    err)
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateContent(%p, %p)", content_index, version_index)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return EINVAL)

    struct ContentLookup* content_lookup;
    int err = CreateContentLookup(
        *content_index->m_BlockCount,
        content_index->m_BlockHashes,
        *content_index->m_ChunkCount,
        content_index->m_ChunkHashes,
        content_index->m_ChunkBlockIndexes,
        &content_lookup);

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ValidateContent(%p, %p) CreateContentLookup(%" PRIu64 ", %p, %" PRIu64 ", %p, %p, %p) failed with %d",
            content_index, version_index,
            *content_index->m_BlockCount, content_index->m_BlockHashes, *content_index->m_ChunkCount, content_index->m_ChunkHashes, content_index->m_ChunkBlockIndexes, &content_lookup,
            err)
        return err;
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
            intptr_t content_chunk_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
            if (content_chunk_index_ptr == -1)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateContent(%p, %p) content index does not contain chunk 0x%" PRIx64 "",
                    content_index, version_index,
                    chunk_hash)
                DeleteContentLookup(content_lookup);
                return EINVAL;
            }
            uint64_t content_chunk_index = content_lookup->m_ChunkHashToChunkIndex[content_chunk_index_ptr].value;
            if (content_index->m_ChunkHashes[content_chunk_index] != chunk_hash)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateContent(%p, %p) content index lookup table is corrupt",
                    content_index, version_index)
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
            }
            if (content_index->m_ChunkLengths[content_chunk_index] != chunk_size)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateContent(%p, %p) chunk size for 0x%" PRIx64 " mismatch, content index: %u, version index: %u",
                    content_index, version_index,
                    chunk_hash, content_index->m_ChunkLengths[content_chunk_index], chunk_size)
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
            }
        }
        if (asset_chunked_size != asset_size)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateContent(%p, %p) asset size for %s mismatch, accumulated chunks size: %" PRIu64 ", asset size:  %" PRIu64 "",
                content_index, version_index,
                &version_index->m_NameData[version_index->m_NameOffsets[asset_index]], asset_chunked_size, asset_size)
            DeleteContentLookup(content_lookup);
            content_lookup = 0;
            return EINVAL;
        }
    }

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    return 0;
}

int Longtail_ValidateVersion(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ValidateVersion(%p, %p)", content_index, version_index)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, EINVAL)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, EINVAL)

    struct HashToIndexItem* version_chunk_lookup = 0;

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
            hmput(version_chunk_lookup, chunk_hash, chunk_index);
            uint32_t chunk_size = version_index->m_ChunkSizes[chunk_index];
            asset_chunked_size += chunk_size;
        }
        if (asset_chunked_size != asset_size)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateVersion(%p, %p) asset size for %s mismatch, accumulated chunks size: %" PRIu64 ", asset size:  %" PRIu64 "",
                content_index, version_index,
                &version_index->m_NameData[version_index->m_NameOffsets[asset_index]], asset_chunked_size, asset_size)
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
    }

    uint64_t content_index_chunk_count = *content_index->m_ChunkCount;
    for (uint64_t chunk_index = 0; chunk_index < content_index_chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t version_chunk_index_ptr = hmgeti(version_chunk_lookup, chunk_hash);
        if (version_chunk_index_ptr == -1)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateVersion(%p, %p) version index does not contain chunk 0x%" PRIx64 "",
                content_index, version_index,
                chunk_hash)
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
        uint64_t version_chunk_index = version_chunk_lookup[version_chunk_index_ptr].value;
        LONGTAIL_FATAL_ASSERT(version_index->m_ChunkHashes[version_chunk_index] == chunk_hash, return EINVAL)
        if (version_index->m_ChunkSizes[version_chunk_index] != chunk_size)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ValidateVersion(%p, %p) chunk size for 0x%" PRIx64 " mismatch, content index: %u, version index: %u",
                content_index, version_index,
                chunk_hash, chunk_size, version_index->m_ChunkSizes[version_chunk_index])
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
    }

    hmfree(version_chunk_lookup);
    version_chunk_lookup = 0;

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

uint32_t Longtail_Paths_GetCount(const struct Longtail_Paths* paths) { return *paths->m_PathCount; }
const char* Longtail_Paths_GetPath(const struct Longtail_Paths* paths, uint32_t index) { return &paths->m_Data[paths->m_Offsets[index]]; }

uint32_t Longtail_FileInfos_GetCount(const struct Longtail_FileInfos* file_infos) { return *file_infos->m_Paths.m_PathCount; }
const struct Longtail_Paths* Longtail_FileInfos_GetPaths(const struct Longtail_FileInfos* file_infos) { return &file_infos->m_Paths; }
uint64_t Longtail_FileInfos_GetSize(const struct Longtail_FileInfos* file_infos, uint32_t index) { return file_infos->m_FileSizes[index]; }
const uint32_t* Longtail_FileInfos_GetPermissions(const struct Longtail_FileInfos* file_infos, uint32_t index) { return file_infos->m_Permissions; }

uint32_t Longtail_ContentIndex_GetVersion(const struct Longtail_ContentIndex* content_index) { return *content_index->m_Version; }
uint32_t Longtail_ContentIndex_GetHashAPI(const struct Longtail_ContentIndex* content_index) { return *content_index->m_HashAPI; }
uint64_t Longtail_ContentIndex_GetBlockCount(const struct Longtail_ContentIndex* content_index) { return *content_index->m_BlockCount; }
uint64_t Longtail_ContentIndex_GetChunkCount(const struct Longtail_ContentIndex* content_index) { return *content_index->m_ChunkCount; }
TLongtail_Hash* Longtail_ContentIndex_BlockHashes(const struct Longtail_ContentIndex* content_index) { return content_index->m_BlockHashes; }

uint32_t Longtail_VersionIndex_GetVersion(const struct Longtail_VersionIndex* content_index) { return *content_index->m_Version; }
uint32_t Longtail_VersionIndex_GetHashAPI(const struct Longtail_VersionIndex* content_index) { return *content_index->m_HashAPI; }
uint32_t Longtail_VersionIndex_GetAssetCount(const struct Longtail_VersionIndex* content_index) { return *content_index->m_AssetCount; }
uint32_t Longtail_VersionIndex_GetChunkCount(const struct Longtail_VersionIndex* content_index) { return *content_index->m_ChunkCount; }

static uint32_t hashTable[] = {
    0x458be752, 0xc10748cc, 0xfbbcdbb8, 0x6ded5b68,
    0xb10a82b5, 0x20d75648, 0xdfc5665f, 0xa8428801,
    0x7ebf5191, 0x841135c7, 0x65cc53b3, 0x280a597c,
    0x16f60255, 0xc78cbc3e, 0x294415f5, 0xb938d494,
    0xec85c4e6, 0xb7d33edc, 0xe549b544, 0xfdeda5aa,
    0x882bf287, 0x3116737c, 0x05569956, 0xe8cc1f68,
    0x0806ac5e, 0x22a14443, 0x15297e10, 0x50d090e7,
    0x4ba60f6f, 0xefd9f1a7, 0x5c5c885c, 0x82482f93,
    0x9bfd7c64, 0x0b3e7276, 0xf2688e77, 0x8fad8abc,
    0xb0509568, 0xf1ada29f, 0xa53efdfe, 0xcb2b1d00,
    0xf2a9e986, 0x6463432b, 0x95094051, 0x5a223ad2,
    0x9be8401b, 0x61e579cb, 0x1a556a14, 0x5840fdc2,
    0x9261ddf6, 0xcde002bb, 0x52432bb0, 0xbf17373e,
    0x7b7c222f, 0x2955ed16, 0x9f10ca59, 0xe840c4c9,
    0xccabd806, 0x14543f34, 0x1462417a, 0x0d4a1f9c,
    0x087ed925, 0xd7f8f24c, 0x7338c425, 0xcf86c8f5,
    0xb19165cd, 0x9891c393, 0x325384ac, 0x0308459d,
    0x86141d7e, 0xc922116a, 0xe2ffa6b6, 0x53f52aed,
    0x2cd86197, 0xf5b9f498, 0xbf319c8f, 0xe0411fae,
    0x977eb18c, 0xd8770976, 0x9833466a, 0xc674df7f,
    0x8c297d45, 0x8ca48d26, 0xc49ed8e2, 0x7344f874,
    0x556f79c7, 0x6b25eaed, 0xa03e2b42, 0xf68f66a4,
    0x8e8b09a2, 0xf2e0e62a, 0x0d3a9806, 0x9729e493,
    0x8c72b0fc, 0x160b94f6, 0x450e4d3d, 0x7a320e85,
    0xbef8f0e1, 0x21d73653, 0x4e3d977a, 0x1e7b3929,
    0x1cc6c719, 0xbe478d53, 0x8d752809, 0xe6d8c2c6,
    0x275f0892, 0xc8acc273, 0x4cc21580, 0xecc4a617,
    0xf5f7be70, 0xe795248a, 0x375a2fe9, 0x425570b6,
    0x8898dcf8, 0xdc2d97c4, 0x0106114b, 0x364dc22f,
    0x1e0cad1f, 0xbe63803c, 0x5f69fac2, 0x4d5afa6f,
    0x1bc0dfb5, 0xfb273589, 0x0ea47f7b, 0x3c1c2b50,
    0x21b2a932, 0x6b1223fd, 0x2fe706a8, 0xf9bd6ce2,
    0xa268e64e, 0xe987f486, 0x3eacf563, 0x1ca2018c,
    0x65e18228, 0x2207360a, 0x57cf1715, 0x34c37d2b,
    0x1f8f3cde, 0x93b657cf, 0x31a019fd, 0xe69eb729,
    0x8bca7b9b, 0x4c9d5bed, 0x277ebeaf, 0xe0d8f8ae,
    0xd150821c, 0x31381871, 0xafc3f1b0, 0x927db328,
    0xe95effac, 0x305a47bd, 0x426ba35b, 0x1233af3f,
    0x686a5b83, 0x50e072e5, 0xd9d3bb2a, 0x8befc475,
    0x487f0de6, 0xc88dff89, 0xbd664d5e, 0x971b5d18,
    0x63b14847, 0xd7d3c1ce, 0x7f583cf3, 0x72cbcb09,
    0xc0d0a81c, 0x7fa3429b, 0xe9158a1b, 0x225ea19a,
    0xd8ca9ea3, 0xc763b282, 0xbb0c6341, 0x020b8293,
    0xd4cd299d, 0x58cfa7f8, 0x91b4ee53, 0x37e4d140,
    0x95ec764c, 0x30f76b06, 0x5ee68d24, 0x679c8661,
    0xa41979c2, 0xf2b61284, 0x4fac1475, 0x0adb49f9,
    0x19727a23, 0x15a7e374, 0xc43a18d5, 0x3fb1aa73,
    0x342fc615, 0x924c0793, 0xbee2d7f0, 0x8a279de9,
    0x4aa2d70c, 0xe24dd37f, 0xbe862c0b, 0x177c22c2,
    0x5388e5ee, 0xcd8a7510, 0xf901b4fd, 0xdbc13dbc,
    0x6c0bae5b, 0x64efe8c7, 0x48b02079, 0x80331a49,
    0xca3d8ae6, 0xf3546190, 0xfed7108b, 0xc49b941b,
    0x32baf4a9, 0xeb833a4a, 0x88a3f1a5, 0x3a91ce0a,
    0x3cc27da1, 0x7112e684, 0x4a3096b1, 0x3794574c,
    0xa3c8b6f3, 0x1d213941, 0x6e0a2e00, 0x233479f1,
    0x0f4cd82f, 0x6093edd2, 0x5d7d209e, 0x464fe319,
    0xd4dcac9e, 0x0db845cb, 0xfb5e4bc3, 0xe0256ce1,
    0x09fb4ed1, 0x0914be1e, 0xa5bdb2c3, 0xc6eb57bb,
    0x30320350, 0x3f397e91, 0xa67791bc, 0x86bc0e2c,
    0xefa0a7e2, 0xe9ff7543, 0xe733612c, 0xd185897b,
    0x329e5388, 0x91dd236b, 0x2ecb0d93, 0xf4d82a3d,
    0x35b5c03f, 0xe4e606f0, 0x05b21843, 0x37b45964,
    0x5eff22f4, 0x6027f4cc, 0x77178b3c, 0xae507131,
    0x7bf7cabc, 0xf9c18d66, 0x593ade65, 0xd95ddf11,
};

struct ChunkerWindow
{
    uint8_t* buf;
    uint32_t len;
    uint32_t m_ScanPosition;
};

struct Array
{
    uint8_t* data;
    uint32_t len;
};

struct Longtail_Chunker
{
    struct Longtail_ChunkerParams params;
    struct Array buf;
    uint32_t off;
    uint32_t hValue;
    uint8_t hWindow[ChunkerWindowSize];
    uint32_t hDiscriminator;
    Longtail_Chunker_Feeder fFeeder;
    void* cFeederContext;
    uint64_t processed_count;
};

static uint32_t discriminatorFromAvg(double avg)
{
    return (uint32_t)(avg / (-1.42888852e-7*avg + 1.33237515));
}

 int Longtail_CreateChunker(
    struct Longtail_ChunkerParams* params,
    Longtail_Chunker_Feeder feeder,
    void* context,
    struct Longtail_Chunker** out_chunker)
{
    LONGTAIL_FATAL_ASSERT(params != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(feeder != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(params->min >= ChunkerWindowSize, return EINVAL)
    LONGTAIL_FATAL_ASSERT(params->min <= params->max, return EINVAL)
    LONGTAIL_FATAL_ASSERT(params->min <= params->avg, return EINVAL)
    LONGTAIL_FATAL_ASSERT(params->avg <= params->max, return EINVAL)

    size_t chunker_size = sizeof(struct Longtail_Chunker) + params->max;
    struct Longtail_Chunker* c = (struct Longtail_Chunker*)Longtail_Alloc(chunker_size);
    if (!c)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateChunker(%p ,%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with %d",
            params, feeder, context, out_chunker,
            chunker_size,
            ENOMEM)
        return ENOMEM;
    }
    c->params = *params;
    c->buf.data = (uint8_t*)&c[1];
    c->buf.len = 0;
    c->off = 0;
    c->hValue = 0;
    c->hDiscriminator = discriminatorFromAvg((double)params->avg);
    c->fFeeder = feeder;
    c->cFeederContext = context;
    c->processed_count = 0;
    *out_chunker = c;
    return 0;
}

static int FeedChunker(struct Longtail_Chunker* c)
{
    LONGTAIL_FATAL_ASSERT(c != 0, return EINVAL)

    if (c->off != 0)
    {
        memmove(c->buf.data, &c->buf.data[c->off], c->buf.len - c->off);
        c->processed_count += c->off;
        c->buf.len -= c->off;
        c->off = 0;
    }
    uint32_t feed_max = (uint32_t)(c->params.max - c->buf.len);
    uint32_t feed_count;
    int err = c->fFeeder(c->cFeederContext, c, feed_max, (char*)&c->buf.data[c->buf.len], &feed_count);
    c->buf.len += feed_count;
    return err;
}

#ifndef _MSC_VER
inline uint32_t _rotl(uint32_t x, int shift) {
    shift &= 31;
    if (!shift) return x;
    return (x << shift) | (x >> (32 - shift));
}
#endif // _MSC_VER

static const struct Longtail_ChunkRange EmptyChunkRange = {0, 0, 0};

struct Longtail_ChunkRange Longtail_NextChunk(struct Longtail_Chunker* c)
{
    LONGTAIL_FATAL_ASSERT(c != 0, return EmptyChunkRange)
    if (c->buf.len - c->off < c->params.max)
    {
        int err = FeedChunker(c);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_NextChunk(%p) FeedChunker(%p) failed with %d",
                c,
                c,
                err)
            return EmptyChunkRange;
        }
    }
    if (c->off == c->buf.len)
    {
        // All done
        struct Longtail_ChunkRange r = {0, c->processed_count + c->off, 0};
        return r;
    }

    uint32_t left = c->buf.len - c->off;
    if (left <= c->params.min)
    {
        // Less than min-size left, just consume it all
        struct Longtail_ChunkRange r = {&c->buf.data[c->off], c->processed_count + c->off, left};
        c->off += left;
        return r;
    }

    uint32_t hash = 0;
    struct Longtail_ChunkRange scoped_data = {&c->buf.data[c->off], c->processed_count + c->off, left};
    {
        struct Longtail_ChunkRange window = {&scoped_data.buf[c->params.min - ChunkerWindowSize], c->processed_count + c->off + c->params.min - ChunkerWindowSize, ChunkerWindowSize};
        for (uint32_t i = 0; i < ChunkerWindowSize; ++i)
        {
            uint8_t b = window.buf[i];
            hash ^= _rotl(hashTable[b], (int)(ChunkerWindowSize-i-1u));
            c->hWindow[i] = b;
        }
    }

    uint32_t pos = c->params.min;
    uint32_t idx = 0;

    uint32_t data_len = scoped_data.len;
    uint8_t* window = c->hWindow;
    const uint32_t discriminator = c->hDiscriminator - 1;
    const uint8_t* scoped_buf = scoped_data.buf;
    const uint32_t d = c->hDiscriminator;
    while(pos < data_len)
    {
        uint8_t in = scoped_buf[pos++];
        uint8_t out = window[idx];
        window[idx++] = in;
        hash = _rotl(hash, 1) ^
            _rotl(hashTable[out], (int)(ChunkerWindowSize)) ^
            hashTable[in];

        if ((hash % d) == discriminator)
        {
            break;
        }
        if (idx == ChunkerWindowSize)
        {
            idx = 0;
        }
    }
    struct Longtail_ChunkRange r = {scoped_buf, c->processed_count + c->off, pos};
    c->off += pos;
    return r;
}
