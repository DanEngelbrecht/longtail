#pragma once

#include <stdint.h>
#include <stddef.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t TLongtail_Hash;
struct Longtail_BlockIndex;
struct Longtail_Paths;
struct Longtail_FileInfos;
struct Longtail_VersionIndex;
struct Longtail_StoredBlock;
struct Longtail_ContentIndex;
struct Longtail_VersionDiff;

////////////// Longtail_API

struct Longtail_API;

typedef void (*Longtail_DisposeFunc)(struct Longtail_API* api);

struct Longtail_API
{
    Longtail_DisposeFunc Dispose;
};

LONGTAIL_EXPORT void Longtail_DisposeAPI(struct Longtail_API* api);
#define SAFE_DISPOSE_API(api) if (api) { Longtail_DisposeAPI(&api->m_API);}

////////////// Longtail_HashAPI

struct Longtail_HashAPI;

typedef struct Longtail_HashAPI_Context* Longtail_HashAPI_HContext;
typedef uint32_t (*Longtail_Hash_GetIdentifierFunc)(struct Longtail_HashAPI* hash_api);
typedef int (*Longtail_Hash_BeginContextFunc)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context);
typedef void (*Longtail_Hash_HashFunc)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data);
typedef uint64_t (*Longtail_Hash_EndContextFunc)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context);
typedef int (*Longtail_Hash_HashBufferFunc)(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash);

struct Longtail_HashAPI
{
    struct Longtail_API m_API;
    Longtail_Hash_GetIdentifierFunc GetIdentifier;
    Longtail_Hash_BeginContextFunc BeginContext;
    Longtail_Hash_HashFunc Hash;
    Longtail_Hash_EndContextFunc EndContext;
    Longtail_Hash_HashBufferFunc HashBuffer;
};

LONGTAIL_EXPORT uint64_t Longtail_GetHashAPISize();

LONGTAIL_EXPORT struct Longtail_HashAPI* Longtail_MakeHashAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Hash_GetIdentifierFunc get_identifier_func,
    Longtail_Hash_BeginContextFunc begin_context_func,
    Longtail_Hash_HashFunc hash_func,
    Longtail_Hash_EndContextFunc end_context_func,
    Longtail_Hash_HashBufferFunc hash_buffer_func);

LONGTAIL_EXPORT uint32_t Longtail_Hash_GetIdentifier(struct Longtail_HashAPI* hash_api);
LONGTAIL_EXPORT int Longtail_Hash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context);
LONGTAIL_EXPORT void Longtail_Hash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data);
LONGTAIL_EXPORT uint64_t Longtail_Hash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context);
LONGTAIL_EXPORT int Longtail_Hash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash);

////////////// Longtail_StorageAPI

typedef struct Longtail_StorageAPI_OpenFile* Longtail_StorageAPI_HOpenFile;
typedef struct Longtail_StorageAPI_Iterator* Longtail_StorageAPI_HIterator;

enum {
    Longtail_StorageAPI_OtherExecuteAccess  = 0001,
    Longtail_StorageAPI_OtherWriteAccess    = 0002,
    Longtail_StorageAPI_OtherReadAccess     = 0004,

    Longtail_StorageAPI_GroupExecuteAccess  = 0010,
    Longtail_StorageAPI_GroupWriteAccess    = 0020,
    Longtail_StorageAPI_GroupReadAccess     = 0040,

    Longtail_StorageAPI_UserExecuteAccess   = 0100,
    Longtail_StorageAPI_UserWriteAccess     = 0200,
    Longtail_StorageAPI_UserReadAccess      = 0400
};

struct Longtail_StorageAPI;

typedef int (*Longtail_Storage_OpenReadFileFunc)(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file);
typedef int (*Longtail_Storage_GetSizeFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size);
typedef int (*Longtail_Storage_ReadFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output);
typedef int (*Longtail_Storage_OpenWriteFileFunc)(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file);
typedef int (*Longtail_Storage_WriteFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input);
typedef int (*Longtail_Storage_SetSizeFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length);
typedef int (*Longtail_Storage_SetPermissionsFunc)(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions);
typedef void (*Longtail_Storage_CloseFileFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f);
typedef int (*Longtail_Storage_CreateDirFunc)(struct Longtail_StorageAPI* storage_api, const char* path);
typedef int (*Longtail_Storage_RenameFileFunc)(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path);
typedef char* (*Longtail_Storage_ConcatPathFunc)(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path);
typedef int (*Longtail_Storage_IsDirFunc)(struct Longtail_StorageAPI* storage_api, const char* path);
typedef int (*Longtail_Storage_IsFileFunc)(struct Longtail_StorageAPI* storage_api, const char* path);
typedef int (*Longtail_Storage_RemoveDirFunc)(struct Longtail_StorageAPI* storage_api, const char* path);
typedef int (*Longtail_Storage_RemoveFileFunc)(struct Longtail_StorageAPI* storage_api, const char* path);
typedef int (*Longtail_Storage_StartFindFunc)(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator);
typedef int (*Longtail_Storage_FindNextFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
typedef void (*Longtail_Storage_CloseFindFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
typedef const char* (*Longtail_Storage_GetFileNameFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
typedef const char* (*Longtail_Storage_GetDirectoryNameFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
typedef int (*Longtail_Storage_GetEntryPropertiesFunc)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, uint64_t* out_size, uint16_t* out_permissions);

struct Longtail_StorageAPI
{
    struct Longtail_API m_API;
    Longtail_Storage_OpenReadFileFunc OpenReadFile;
    Longtail_Storage_GetSizeFunc GetSize;
    Longtail_Storage_ReadFunc Read;
    Longtail_Storage_OpenWriteFileFunc OpenWriteFile;
    Longtail_Storage_WriteFunc Write;
    Longtail_Storage_SetSizeFunc SetSize;
    Longtail_Storage_SetPermissionsFunc SetPermissions;
    Longtail_Storage_CloseFileFunc CloseFile;
    Longtail_Storage_CreateDirFunc CreateDir;
    Longtail_Storage_RenameFileFunc RenameFile;
    Longtail_Storage_ConcatPathFunc ConcatPath;
    Longtail_Storage_IsDirFunc IsDir;
    Longtail_Storage_IsFileFunc IsFile;
    Longtail_Storage_RemoveDirFunc RemoveDir;
    Longtail_Storage_RemoveFileFunc RemoveFile;
    Longtail_Storage_StartFindFunc StartFind;
    Longtail_Storage_FindNextFunc FindNext;
    Longtail_Storage_CloseFindFunc CloseFind;
    Longtail_Storage_GetFileNameFunc GetFileName;
    Longtail_Storage_GetDirectoryNameFunc GetDirectoryName;
    Longtail_Storage_GetEntryPropertiesFunc GetEntryProperties;
};

LONGTAIL_EXPORT uint64_t Longtail_GetStorageAPISize();

LONGTAIL_EXPORT struct Longtail_StorageAPI* Longtail_MakeStorageAPI(
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
    Longtail_Storage_GetEntryPropertiesFunc get_entry_properties_func);

LONGTAIL_EXPORT int Longtail_Storage_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file);
LONGTAIL_EXPORT int Longtail_Storage_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size);
LONGTAIL_EXPORT int Longtail_Storage_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output);
LONGTAIL_EXPORT int Longtail_Storage_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file);
LONGTAIL_EXPORT int Longtail_Storage_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input);
LONGTAIL_EXPORT int Longtail_Storage_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length);
LONGTAIL_EXPORT int Longtail_Storage_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions);
LONGTAIL_EXPORT void Longtail_Storage_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f);
LONGTAIL_EXPORT int Longtail_Storage_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT int Longtail_Storage_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path);
LONGTAIL_EXPORT char* Longtail_Storage_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path);
LONGTAIL_EXPORT int Longtail_Storage_IsDir(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT int Longtail_Storage_IsFile(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT int Longtail_Storage_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT int Longtail_Storage_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT int Longtail_Storage_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator);
LONGTAIL_EXPORT int Longtail_Storage_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
LONGTAIL_EXPORT void Longtail_Storage_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
LONGTAIL_EXPORT const char* Longtail_Storage_GetFileName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
LONGTAIL_EXPORT const char* Longtail_Storage_GetDirectoryName(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
LONGTAIL_EXPORT int Longtail_Storage_GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, uint64_t* out_size, uint16_t* out_permissions);

////////////// Longtail_ProgressAPI

struct Longtail_ProgressAPI;

typedef void (*Longtail_Progress_OnProgressFunc)(struct Longtail_ProgressAPI* progressAPI, uint32_t total_count, uint32_t done_count);

struct Longtail_ProgressAPI
{
    struct Longtail_API m_API;
    Longtail_Progress_OnProgressFunc OnProgress;
};

LONGTAIL_EXPORT uint64_t Longtail_GetProgressAPISize();

LONGTAIL_EXPORT struct Longtail_ProgressAPI* Longtail_MakeProgressAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Progress_OnProgressFunc on_progress_func);

LONGTAIL_EXPORT void Longtail_Progress_OnProgress(struct Longtail_ProgressAPI* progressAPI, uint32_t total_count, uint32_t done_count);

////////////// Longtail_JobAPI

struct Longtail_JobAPI;
typedef void* Longtail_JobAPI_Jobs;
typedef int (*Longtail_JobAPI_JobFunc)(void* context, uint32_t job_id);

typedef uint32_t (*Longtail_Job_GetWorkerCountFunc)(struct Longtail_JobAPI* job_api);
typedef int (*Longtail_Job_ReserveJobsFunc)(struct Longtail_JobAPI* job_api, uint32_t job_count);
typedef int (*Longtail_Job_CreateJobsFunc)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs);
typedef int (*Longtail_Job_AddDependeciesFunc)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs);
typedef int (*Longtail_Job_ReadyJobsFunc)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs);
typedef int (*Longtail_Job_WaitForAllJobsFunc)(struct Longtail_JobAPI* job_api, struct Longtail_ProgressAPI* progressAPI);
typedef int (*Longtail_Job_ResumeJobFunc)(struct Longtail_JobAPI* job_api, uint32_t job_id);

struct Longtail_JobAPI
{
    struct Longtail_API m_API;
    Longtail_Job_GetWorkerCountFunc GetWorkerCount;
    Longtail_Job_ReserveJobsFunc ReserveJobs;
    Longtail_Job_CreateJobsFunc CreateJobs;
    Longtail_Job_AddDependeciesFunc AddDependecies;
    Longtail_Job_ReadyJobsFunc ReadyJobs;
    Longtail_Job_WaitForAllJobsFunc WaitForAllJobs;
    Longtail_Job_ResumeJobFunc ResumeJob;
};

LONGTAIL_EXPORT uint64_t Longtail_GetJobAPISize();

struct Longtail_JobAPI* Longtail_MakeJobAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_Job_GetWorkerCountFunc get_worker_count_func,
    Longtail_Job_ReserveJobsFunc reserve_jobs_func,
    Longtail_Job_CreateJobsFunc create_jobs_func,
    Longtail_Job_AddDependeciesFunc add_dependecies_func,
    Longtail_Job_ReadyJobsFunc ready_jobs_func,
    Longtail_Job_WaitForAllJobsFunc wait_for_all_jobs_func,
    Longtail_Job_ResumeJobFunc resume_job_func);

LONGTAIL_EXPORT uint32_t Longtail_Job_GetWorkerCount(struct Longtail_JobAPI* job_api);
LONGTAIL_EXPORT int Longtail_Job_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count);
LONGTAIL_EXPORT int Longtail_Job_CreateJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs);
LONGTAIL_EXPORT int Longtail_Job_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs);
LONGTAIL_EXPORT int Longtail_Job_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs);
LONGTAIL_EXPORT int Longtail_Job_WaitForAllJobs(struct Longtail_JobAPI* job_api, struct Longtail_ProgressAPI* progressAPI);
LONGTAIL_EXPORT int Longtail_Job_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id);

////////////// Longtail_AsyncPutStoredBlockAPI

struct Longtail_AsyncPutStoredBlockAPI;

typedef int (*Longtail_AsyncPutStoredBlock_OnCompleteFunc)(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err);

struct Longtail_AsyncPutStoredBlockAPI
{
    struct Longtail_API m_API;
    Longtail_AsyncPutStoredBlock_OnCompleteFunc OnComplete;
};

LONGTAIL_EXPORT uint64_t Longtail_GetAsyncPutStoredBlockAPISize();

LONGTAIL_EXPORT struct Longtail_AsyncPutStoredBlockAPI* Longtail_MakeAsyncPutStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncPutStoredBlock_OnCompleteFunc on_complete_func);

LONGTAIL_EXPORT int Longtail_AsyncPutStoredBlock_OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err);

////////////// Longtail_AsyncGetStoredBlockAPI

struct Longtail_AsyncGetStoredBlockAPI;

typedef int (*Longtail_AsyncGetStoredBlock_OnCompleteFunc)(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err);

struct Longtail_AsyncGetStoredBlockAPI
{
    struct Longtail_API m_API;
    Longtail_AsyncGetStoredBlock_OnCompleteFunc OnComplete;
};

LONGTAIL_EXPORT uint64_t Longtail_GetAsyncGetStoredBlockAPISize();

LONGTAIL_EXPORT struct Longtail_AsyncGetStoredBlockAPI* Longtail_MakeAsyncGetStoredBlockAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetStoredBlock_OnCompleteFunc on_complete_func);

LONGTAIL_EXPORT int Longtail_AsyncGetStoredBlock_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err);

////////////// Longtail_AsyncGetIndexAPI

struct Longtail_AsyncGetIndexAPI;

typedef int (*Longtail_AsyncGetIndex_OnCompleteFunc)(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err);

struct Longtail_AsyncGetIndexAPI
{
    struct Longtail_API m_API;
    Longtail_AsyncGetIndex_OnCompleteFunc OnComplete;
};

LONGTAIL_EXPORT uint64_t Longtail_GetAsyncGetIndexAPISize();

LONGTAIL_EXPORT struct Longtail_AsyncGetIndexAPI* Longtail_MakeAsyncGetIndexAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_AsyncGetIndex_OnCompleteFunc on_complete_func);

LONGTAIL_EXPORT int Longtail_AsyncGetIndex_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err);

////////////// Longtail_BlockStoreAPI

struct Longtail_BlockStoreAPI;

typedef int (*Longtail_BlockStore_PutStoredBlockFunc)(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
typedef int (*Longtail_BlockStore_GetStoredBlockFunc)(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
typedef int (*Longtail_BlockStore_GetIndexFunc)(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, struct Longtail_AsyncGetIndexAPI* async_complete_api);

struct Longtail_BlockStoreAPI
{
    struct Longtail_API m_API;
    Longtail_BlockStore_PutStoredBlockFunc PutStoredBlock;
    Longtail_BlockStore_GetStoredBlockFunc GetStoredBlock;
    Longtail_BlockStore_GetIndexFunc GetIndex;
};


LONGTAIL_EXPORT uint64_t Longtail_GetBlockStoreAPISize();

LONGTAIL_EXPORT struct Longtail_BlockStoreAPI* Longtail_MakeBlockStoreAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_BlockStore_PutStoredBlockFunc put_stored_block_func,
    Longtail_BlockStore_GetStoredBlockFunc get_stored_block_func,
    Longtail_BlockStore_GetIndexFunc get_index_func);

LONGTAIL_EXPORT int Longtail_BlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
LONGTAIL_EXPORT int Longtail_BlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
LONGTAIL_EXPORT int Longtail_BlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, struct Longtail_AsyncGetIndexAPI* async_complete_api);

typedef void (*Longtail_Assert)(const char* expression, const char* file, int line);
LONGTAIL_EXPORT void Longtail_SetAssert(Longtail_Assert assert_func);
typedef void (*Longtail_Log)(void* context, int level, const char* str);
LONGTAIL_EXPORT void Longtail_SetLog(Longtail_Log log_func, void* context);
LONGTAIL_EXPORT void Longtail_SetLogLevel(int level);

#define LONGTAIL_LOG_LEVEL_DEBUG    0
#define LONGTAIL_LOG_LEVEL_INFO     1
#define LONGTAIL_LOG_LEVEL_WARNING  2
#define LONGTAIL_LOG_LEVEL_ERROR    3
#define LONGTAIL_LOG_LEVEL_OFF      4

#ifndef LONGTAIL_LOG
    void Longtail_CallLogger(int level, const char* fmt, ...);
    #define LONGTAIL_LOG(level, fmt, ...) \
        Longtail_CallLogger(level, fmt, __VA_ARGS__);
#endif

#if defined(LONGTAIL_ASSERTS)
    extern Longtail_Assert Longtail_Assert_private;
#    define LONGTAIL_FATAL_ASSERT(x, bail) \
        if (!(x)) \
        { \
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "%s(%d): Assert failed `%s`\n", __FILE__, __LINE__, #x); \
            if (Longtail_Assert_private) \
            { \
                Longtail_Assert_private(#x, __FILE__, __LINE__); \
            } \
            bail; \
        }
#   define LONGTAIL_VALIDATE_INPUT(x, bail) \
    if (!(x)) \
    { \
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "%s(%d): Input validation failed for `%s`\n", __FILE__, __LINE__, #x); \
        if (Longtail_Assert_private) \
        { \
            Longtail_Assert_private(#x, __FILE__, __LINE__); \
        } \
        bail; \
    }
#else // defined(LONGTAIL_ASSERTS)
#   define LONGTAIL_FATAL_ASSERT(x, y)
#   define LONGTAIL_VALIDATE_INPUT(x, bail) \
    if (!(x)) \
    { \
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "%s(%d): Input validation failed for `%s`\n", __FILE__, __LINE__, #x); \
        bail; \
    }
#endif // defined(LONGTAIL_ASSERTS)


typedef void* (*Longtail_Alloc_Func)(size_t s);
typedef void (*Longtail_Free_Func)(void* p);
LONGTAIL_EXPORT void Longtail_SetAllocAndFree(Longtail_Alloc_Func alloc, Longtail_Free_Func free);

LONGTAIL_EXPORT void* Longtail_Alloc(size_t s);
LONGTAIL_EXPORT void Longtail_Free(void* p);

LONGTAIL_EXPORT int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path);
LONGTAIL_EXPORT char* Longtail_Strdup(const char* path);

LONGTAIL_EXPORT int Longtail_GetFilesRecursively(
    struct Longtail_StorageAPI* storage_api,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos);

LONGTAIL_EXPORT int Longtail_CreateVersionIndexRaw(
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
    struct Longtail_VersionIndex** out_version_index);

LONGTAIL_EXPORT int Longtail_CreateVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const char* root_path,
    struct Longtail_FileInfos* file_infos,
    const uint32_t* asset_tags,
    uint32_t target_chunk_size,
    struct Longtail_VersionIndex** out_version_index);

LONGTAIL_EXPORT int Longtail_WriteVersionIndexToBuffer(
    const struct Longtail_VersionIndex* version_index,
    void** out_buffer,
    size_t* out_size);

LONGTAIL_EXPORT int Longtail_ReadVersionIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_VersionIndex** out_version_index);

LONGTAIL_EXPORT int Longtail_WriteVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_VersionIndex* version_index,
    const char* path);

LONGTAIL_EXPORT int Longtail_ReadVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_VersionIndex** out_version_index);

LONGTAIL_EXPORT size_t Longtail_GetContentIndexDataSize(
    uint64_t block_count,
    uint64_t chunk_count);

LONGTAIL_EXPORT size_t Longtail_GetContentIndexSize(
    uint64_t block_count,
    uint64_t chunk_count);

LONGTAIL_EXPORT int Longtail_InitContentIndexFromData(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size);

LONGTAIL_EXPORT int Longtail_InitContentIndex(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size,
    uint32_t hash_api,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    uint64_t chunk_count);

LONGTAIL_EXPORT int Longtail_CreateContentIndexFromBlocks(
    uint32_t hash_identifier,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    uint64_t block_count,
    struct Longtail_BlockIndex** block_indexes,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_CreateContentIndex(
    struct Longtail_HashAPI* hash_api,
    struct Longtail_VersionIndex* version_index,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_CreateContentIndexRaw(
    struct Longtail_HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* chunk_tags,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_WriteContentIndexToBuffer(
    const struct Longtail_ContentIndex* content_index,
    void** out_buffer,
    size_t* out_size);

LONGTAIL_EXPORT int Longtail_ReadContentIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_WriteContentIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_ContentIndex* content_index,
    const char* path);

LONGTAIL_EXPORT int Longtail_ReadContentIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_WriteContent(
    struct Longtail_StorageAPI* source_storage_api,
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_ContentIndex* block_store_content_index,
    struct Longtail_ContentIndex* version_content_index,
    struct Longtail_VersionIndex* version_index,
    const char* assets_folder);

LONGTAIL_EXPORT int Longtail_CreateMissingContent(
    struct Longtail_HashAPI* hash_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_RetargetContent(
    const struct Longtail_ContentIndex* reference_content_index,
    const struct Longtail_ContentIndex* content_index,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_MergeContentIndex(
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* remote_content_index,
    struct Longtail_ContentIndex** out_content_index);

LONGTAIL_EXPORT int Longtail_WriteVersion(
    struct Longtail_BlockStoreAPI* block_storage_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    int retain_permissions);

LONGTAIL_EXPORT int Longtail_CreateVersionDiff(
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    struct Longtail_VersionDiff** out_version_diff);

LONGTAIL_EXPORT int Longtail_ChangeVersion(
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
    int retain_permissions);

LONGTAIL_EXPORT size_t Longtail_GetBlockIndexSize(uint32_t chunk_count);
LONGTAIL_EXPORT size_t Longtail_GetBlockIndexDataSize(uint32_t chunk_count);
LONGTAIL_EXPORT struct Longtail_BlockIndex* Longtail_InitBlockIndex(void* mem, uint32_t chunk_count);

LONGTAIL_EXPORT int Longtail_InitBlockIndexFromData(
    struct Longtail_BlockIndex* block_index,
    void* data,
    uint64_t data_size);

LONGTAIL_EXPORT int Longtail_CreateBlockIndex(
    struct Longtail_HashAPI* hash_api,
    uint32_t tag,
    uint32_t chunk_count,
    const uint64_t* chunk_indexes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    struct Longtail_BlockIndex** out_block_index);

LONGTAIL_EXPORT int Longtail_WriteBlockIndexToBuffer(
    const struct Longtail_BlockIndex* block_index,
    void** out_buffer,
    size_t* out_size);

LONGTAIL_EXPORT int Longtail_ReadBlockIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_BlockIndex** out_block_index);

LONGTAIL_EXPORT int Longtail_WriteBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_BlockIndex* block_index,
    const char* path);

LONGTAIL_EXPORT int Longtail_ReadBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_BlockIndex** out_block_index);

LONGTAIL_EXPORT size_t Longtail_GetStoredBlockSize(size_t block_data_size);
LONGTAIL_EXPORT int Longtail_InitStoredBlockFromData(
    struct Longtail_StoredBlock* stored_block,
    void* block_data,
    size_t block_data_size);

LONGTAIL_EXPORT int Longtail_CreateStoredBlock(
    TLongtail_Hash block_hash,
    uint32_t chunk_count,
    uint32_t tag,
    TLongtail_Hash* chunk_hashes,
    uint32_t* chunk_sizes,
    uint32_t block_data_size,
    struct Longtail_StoredBlock** out_stored_block);

LONGTAIL_EXPORT int Longtail_WriteStoredBlockToBuffer(
    const struct Longtail_StoredBlock* stored_block,
    void** out_buffer,
    size_t* out_size);

LONGTAIL_EXPORT int Longtail_ReadStoredBlockFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_StoredBlock** out_stored_block);

LONGTAIL_EXPORT int Longtail_WriteStoredBlock(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_StoredBlock* stored_block,
    const char* path);

LONGTAIL_EXPORT int Longtail_ReadStoredBlock(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_StoredBlock** out_stored_block);

LONGTAIL_EXPORT int Longtail_ValidateContent(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index);

LONGTAIL_EXPORT int Longtail_ValidateVersion(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index);

struct Longtail_BlockIndex
{
    TLongtail_Hash* m_BlockHash;
    uint32_t* m_ChunkCount;
    uint32_t* m_Tag;
    TLongtail_Hash* m_ChunkHashes; //[]
    uint32_t* m_ChunkSizes; // []
};

LONGTAIL_EXPORT uint32_t Longtail_BlockIndex_GetChunkCount(const struct Longtail_BlockIndex* block_index);
LONGTAIL_EXPORT const uint32_t* Longtail_BlockIndex_GetChunkTag(const struct Longtail_BlockIndex* block_index);
LONGTAIL_EXPORT const TLongtail_Hash* Longtail_BlockIndex_GetChunkHashes(const struct Longtail_BlockIndex* block_index);
LONGTAIL_EXPORT const uint32_t* Longtail_BlockIndex_GetChunkSizes(const struct Longtail_BlockIndex* block_index);

struct Longtail_StoredBlock
{
    int (*Dispose)(struct Longtail_StoredBlock* stored_block);
    struct Longtail_BlockIndex* m_BlockIndex;
    void* m_BlockData;
    uint32_t m_BlockChunksDataSize;
};

LONGTAIL_EXPORT void Longtail_StoredBlock_Dispose(struct Longtail_StoredBlock* stored_block);
LONGTAIL_EXPORT struct Longtail_BlockIndex* Longtail_StoredBlock_GetBlockIndex(struct Longtail_StoredBlock* stored_block);
LONGTAIL_EXPORT void* Longtail_BlockIndex_BlockData(struct Longtail_StoredBlock* stored_block);
LONGTAIL_EXPORT uint32_t Longtail_BlockIndex_GetBlockChunksDataSize(struct Longtail_StoredBlock* stored_block);

struct Longtail_Paths
{
    uint32_t m_DataSize;
    uint32_t* m_PathCount;
    uint32_t* m_Offsets;
    char* m_Data;
};

LONGTAIL_EXPORT uint32_t Longtail_Paths_GetCount(const struct Longtail_Paths* paths);
LONGTAIL_EXPORT const char* Longtail_Paths_GetPath(const struct Longtail_Paths* paths, uint32_t index);

struct Longtail_FileInfos
{
    struct Longtail_Paths m_Paths;
    uint64_t* m_FileSizes;
    uint32_t* m_Permissions;
};

LONGTAIL_EXPORT uint32_t Longtail_FileInfos_GetCount(const struct Longtail_FileInfos* file_infos);
LONGTAIL_EXPORT const struct Longtail_Paths* Longtail_FileInfos_GetPaths(const struct Longtail_FileInfos* file_infos);
LONGTAIL_EXPORT uint64_t Longtail_FileInfos_GetSize(const struct Longtail_FileInfos* file_infos, uint32_t index);
LONGTAIL_EXPORT const uint32_t* Longtail_FileInfos_GetPermissions(const struct Longtail_FileInfos* file_infos, uint32_t index);

extern uint32_t Longtail_CurrentContentIndexVersion;

struct Longtail_ContentIndex
{
    uint32_t* m_Version;
    uint32_t* m_HashAPI;
    uint32_t* m_MaxBlockSize;
    uint32_t* m_MaxChunksPerBlock;
    uint64_t* m_BlockCount;
    uint64_t* m_ChunkCount;

    TLongtail_Hash* m_BlockHashes;      // []
    TLongtail_Hash* m_ChunkHashes;      // []
    uint64_t* m_ChunkBlockIndexes;      // []
    uint32_t* m_ChunkBlockOffsets;      // []
    uint32_t* m_ChunkLengths;           // []
};

LONGTAIL_EXPORT uint32_t Longtail_ContentIndex_GetVersion(const struct Longtail_ContentIndex* content_index);
LONGTAIL_EXPORT uint32_t Longtail_ContentIndex_GetHashAPI(const struct Longtail_ContentIndex* content_index);
LONGTAIL_EXPORT uint64_t Longtail_ContentIndex_GetBlockCount(const struct Longtail_ContentIndex* content_index);
LONGTAIL_EXPORT uint64_t Longtail_ContentIndex_GetChunkCount(const struct Longtail_ContentIndex* content_index);
LONGTAIL_EXPORT TLongtail_Hash* Longtail_ContentIndex_BlockHashes(const struct Longtail_ContentIndex* content_index);

struct Longtail_VersionIndex
{
    uint32_t* m_Version;
    uint32_t* m_HashAPI;
    uint32_t* m_TargetChunkSize;
    uint32_t* m_AssetCount;
    uint32_t* m_ChunkCount;
    uint32_t* m_AssetChunkIndexCount;
    TLongtail_Hash* m_PathHashes;       // []
    TLongtail_Hash* m_ContentHashes;    // []
    uint64_t* m_AssetSizes;             // []
    uint32_t* m_AssetChunkCounts;       // []
    // uint64_t* m_CreationDates;       // []
    // uint64_t* m_ModificationDates;   // []
    uint32_t* m_AssetChunkIndexStarts;  // []
    uint32_t* m_AssetChunkIndexes;      // []
    TLongtail_Hash* m_ChunkHashes;      // []

    uint32_t* m_ChunkSizes;             // []
    uint32_t* m_ChunkTags;              // []

    uint32_t* m_NameOffsets;            // []
    uint32_t m_NameDataSize;
    uint32_t* m_Permissions;            // []
    char* m_NameData;
};

LONGTAIL_EXPORT uint32_t Longtail_VersionIndex_GetVersion(const struct Longtail_VersionIndex* content_index);
LONGTAIL_EXPORT uint32_t Longtail_VersionIndex_GetHashAPI(const struct Longtail_VersionIndex* content_index);
LONGTAIL_EXPORT uint32_t Longtail_VersionIndex_GetAssetCount(const struct Longtail_VersionIndex* content_index);
LONGTAIL_EXPORT uint32_t Longtail_VersionIndex_GetChunkCount(const struct Longtail_VersionIndex* content_index);

struct Longtail_VersionDiff
{
    uint32_t* m_SourceRemovedCount;
    uint32_t* m_TargetAddedCount;
    uint32_t* m_ModifiedContentCount;
    uint32_t* m_ModifiedPermissionsCount;
    uint32_t* m_SourceRemovedAssetIndexes;
    uint32_t* m_TargetAddedAssetIndexes;
    uint32_t* m_SourceContentModifiedAssetIndexes;
    uint32_t* m_TargetContentModifiedAssetIndexes;
    uint32_t* m_SourcePermissionsModifiedAssetIndexes;
    uint32_t* m_TargetPermissionsModifiedAssetIndexes;
};

///////////// Test functions

int Longtail_MakePaths(
    uint32_t path_count,
    const char* const* path_names,
    struct Longtail_Paths** out_paths);

size_t Longtail_GetVersionIndexDataSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size);

size_t Longtail_GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size);

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
    struct Longtail_VersionIndex** out_version_index);

struct Longtail_Chunker;

struct Longtail_ChunkerParams
{
    uint32_t min;
    uint32_t avg;
    uint32_t max;
};

struct Longtail_ChunkRange
{
    const uint8_t* buf;
    uint64_t offset;
    uint32_t len;
};

struct Longtail_ChunkRange Longtail_NextChunk(struct Longtail_Chunker* c);

typedef int (*Longtail_Chunker_Feeder)(void* context, struct Longtail_Chunker* chunker, uint32_t requested_size, char* buffer, uint32_t* out_size);

 int Longtail_CreateChunker(
    struct Longtail_ChunkerParams* params,
    Longtail_Chunker_Feeder feeder,
    void* context,
    struct Longtail_Chunker** out_chunker);

#ifdef __cplusplus
}
#endif
