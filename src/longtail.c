#include "longtail.h"

#if defined(__GNUC__) && !defined(__clang__) && !defined(APPLE)
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
    Longtail_Log_private = log_func;
    Longtail_LogContext = context;
}

void Longtail_SetLogLevel(int level)
{
    Longtail_LogLevel_private = level;
}

void Longtail_CallLogger(int level, const char* fmt, ...)
{
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
    char* r = (char*)Longtail_Alloc(strlen(path) + 1);
    LONGTAIL_FATAL_ASSERT(r, return 0)
    strcpy(r, path);
    return r;
}

static int IsDirPath(const char* path)
{
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

static int GetPathHash(struct Longtail_HashAPI* hash_api, const char* path, TLongtail_Hash* out_hash)
{
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)
    uint64_t hash;
    int err = hash_api->HashBuffer(hash_api, (uint32_t)strlen(path), (void*)path, &hash);
    if (err)
    {
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create directory `%s`, %d", path, err)
    return err;
}

int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)
    char* dir_path = Longtail_Strdup(path);
    LONGTAIL_FATAL_ASSERT(dir_path != 0, return ENOMEM)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "EnsureParentPathExists failed: `%s`, %d", dir_path, err)
        Longtail_Free(dir_path);
        dir_path = 0;
        return err;
    }
    err = SafeCreateDir(storage_api, dir_path);
    if (!err)
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 0;
    }

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "EnsureParentPathExists failed: `%s`, %d", dir_path, err)
    Longtail_Free(dir_path);
    dir_path = 0;
    return err;
}







struct HashToIndexItem
{
    TLongtail_Hash key;
    uint64_t value;
};

typedef int (*ProcessEntry)(void* context, const char* root_path, const char* file_name, int is_dir, uint64_t size, uint16_t permissions);

static int RecurseTree(struct Longtail_StorageAPI* storage_api, const char* root_folder, ProcessEntry entry_processor, void* context)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_folder != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(entry_processor != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(context != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "RecurseTree: Scanning folder `%s`", root_folder)

    uint32_t folder_index = 0;

    char** folder_paths = 0;
    arrsetcap(folder_paths, 256);

    arrput(folder_paths, Longtail_Strdup(root_folder));

    int err = 0;
    while (folder_index < (uint32_t)arrlen(folder_paths))
    {
        const char* asset_folder = folder_paths[folder_index++];

        Longtail_StorageAPI_HIterator fs_iterator = 0;
        err = err ? err : storage_api->StartFind(storage_api, asset_folder, &fs_iterator);
        if (!err)
        {
            do
            {
                const char* dir_name = storage_api->GetDirectoryName(storage_api, fs_iterator);
                if (dir_name)
                {
                    uint64_t size;
                    uint16_t permissions;
                    err = storage_api->GetEntryProperties(storage_api, fs_iterator, &size, &permissions);
                    if (err)
                    {
                        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree: Get size and permissions for dir `%s` in `%s` failed with %d", dir_name, asset_folder, err)
                        break;
                    }
                    err = entry_processor(context, asset_folder, dir_name, 1, size, permissions);
                    if (err)
                    {
                        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree: Process dir `%s` in `%s` failed with %d", dir_name, asset_folder, err)
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
                else
                {
                    const char* file_name = storage_api->GetFileName(storage_api, fs_iterator);
                    if (file_name)
                    {
                        uint64_t size;
                        uint16_t permissions;
                        err = storage_api->GetEntryProperties(storage_api, fs_iterator, &size, &permissions);
                        if (err)
                        {
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree: Get size and permissions for file `%s` in `%s` failed with %d", file_name, asset_folder, err)
                            break;
                        }
                        err = entry_processor(context, asset_folder, file_name, 0, size, permissions);
                        if (err)
                        {
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree: Process file `%s` in `%s` failed with %d", file_name, asset_folder, err)
                            break;
                        }
                    }
                }
                err = storage_api->FindNext(storage_api, fs_iterator);
            }while(err == 0);
            storage_api->CloseFind(storage_api, fs_iterator);
        }
        if (err == ENOENT)
        {
            err = 0;
        }
        else if (err != 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "RecurseTree: StartFind on `%s` failed with %d", asset_folder, err)
            break;
        }
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
        sizeof(uint32_t) +                // PathCount
        sizeof(uint32_t) * path_count +    // m_Offsets
        path_data_size;
};

static struct Longtail_Paths* CreatePaths(uint32_t path_count, uint32_t path_data_size)
{
    struct Longtail_Paths* paths = (struct Longtail_Paths*)Longtail_Alloc(GetPathsSize(path_count, path_data_size));
    LONGTAIL_FATAL_ASSERT(paths != 0, return 0)
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
    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    struct Longtail_Paths* paths = CreatePaths(path_count, name_data_size);
    if (paths == 0)
    {
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
        char* full_dir_path = (char*)Longtail_Alloc(path_length + 1 + 1);
        LONGTAIL_FATAL_ASSERT(full_dir_path, return ENOMEM)
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
        return err;
    }

    arrpush(paths_context->m_FileSizes, size);
    arrpush(paths_context->m_Permissions, (uint32_t)permissions);

    Longtail_Free(full_path);
    full_path = 0;
    return 0;
}

int Longtail_GetFilesRecursively(struct Longtail_StorageAPI* storage_api, const char* root_path, struct Longtail_FileInfos** out_file_infos)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_path != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_GetFilesRecursively: Scanning `%s`", root_path)
    const uint32_t default_path_count = 512;
    const uint32_t default_path_data_size = default_path_count * 128;

    struct Longtail_Paths* paths = CreatePaths(default_path_count, default_path_data_size);
    if (paths == 0)
    {
        return ENOMEM;
    }
    struct AddFile_Context context = {storage_api, default_path_count, default_path_data_size, (uint32_t)(strlen(root_path)), paths, 0};
    paths = 0;
    arrsetcap(context.m_FileSizes, 4096);
    arrsetcap(context.m_Permissions, 4096);

    int err = RecurseTree(storage_api, root_path, AddFile, &context);
    if(err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_GetFilesRecursively: Failed get files in folder `%s`, %d", root_path, err)
        Longtail_Free(context.m_Paths);
        context.m_Paths = 0;
        arrfree(context.m_Permissions);
        context.m_Permissions = 0;
        arrfree(context.m_FileSizes);
        context.m_FileSizes = 0;
        return err;
    }

    uint32_t asset_count = *context.m_Paths->m_PathCount;
    struct Longtail_FileInfos* result = (struct Longtail_FileInfos*)Longtail_Alloc(
        sizeof(struct Longtail_FileInfos) +
        sizeof(uint64_t) * asset_count +    // Asset sizes
        sizeof(uint32_t) * asset_count +    // Permissions
        GetPathsSize(asset_count, context.m_Paths->m_DataSize));
    LONGTAIL_FATAL_ASSERT(result, return ENOMEM)

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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "StorageChunkFeederFunc: Failed to read from asset file `%s`, %d", c->m_AssetPath, err)
            return err;
        }
        c->m_Offset += read_count;
    }
    *out_size = (uint32_t)read_count;
    return 0;
}

// ChunkerWindowSize is the number of bytes in the rolling hash window
#define ChunkerWindowSize 48u

#define MIN_CHUNKER_SIZE(max_chunk_size) (((max_chunk_size / 8) < ChunkerWindowSize) ? ChunkerWindowSize : (max_chunk_size / 8))
#define AVG_CHUNKER_SIZE(max_chunk_size) ((max_chunk_size < ChunkerWindowSize) ? ChunkerWindowSize : max_chunk_size)
#define MAX_CHUNKER_SIZE(max_chunk_size) (max_chunk_size * 4)

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
    uint32_t m_MaxChunkSize;
    int m_Err;
};

static int DynamicChunking(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)
    struct HashJob* hash_job = (struct HashJob*)context;

    hash_job->m_Err = GetPathHash(hash_job->m_HashAPI, hash_job->m_Path, hash_job->m_PathHash);
    if (hash_job->m_Err)
    {
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to open file `%s`, %d", path, err)
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
    else if (hash_size <= ChunkerWindowSize || hash_job->m_MaxChunkSize <= ChunkerWindowSize)
    {
        char* buffer = (char*)Longtail_Alloc((size_t)hash_size);
        LONGTAIL_FATAL_ASSERT(buffer, hash_job->m_Err = ENOMEM; return 0)
        err = storage_api->Read(storage_api, file_handle, 0, hash_size, buffer);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to read from file `%s`, %d", path, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to create hash context for path `%s`", path)
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
        uint32_t min_chunk_size = MIN_CHUNKER_SIZE(hash_job->m_MaxChunkSize);
        uint32_t avg_chunk_size = AVG_CHUNKER_SIZE(hash_job->m_MaxChunkSize);
        uint32_t max_chunk_size = MAX_CHUNKER_SIZE(hash_job->m_MaxChunkSize);

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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to create chunker for asset `%s`, %d", path, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to create hash context for path `%s`", path)
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
            LONGTAIL_FATAL_ASSERT(remaining >= r.len, hash_job->m_Err = EINVAL; return 0)
            err = hash_job->m_HashAPI->HashBuffer(hash_job->m_HashAPI, r.len, (void*)r.buf, &hash_job->m_ChunkHashes[chunk_count]);
            if (err != 0)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to create hash for chunk of `%s`", path)
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
        LONGTAIL_FATAL_ASSERT(remaining == 0, hash_job->m_Err = EINVAL; return 0)

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
    uint32_t max_chunk_size,
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
    LONGTAIL_FATAL_ASSERT(max_chunk_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "ChunkAssets: Hashing and chunking folder `%s` with %" PRIu64 " assets", root_path, *paths->m_PathCount)
    uint32_t asset_count = *paths->m_PathCount;

    uint64_t max_hash_size = max_chunk_size * 512;
    uint32_t job_count = 0;

    uint64_t min_chunk_size = MIN_CHUNKER_SIZE(max_chunk_size);

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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "ChunkAssets: Failed to reserve %" PRIu64 " jobs for folder `%s`, %d", paths->m_PathCount, root_path, err)
        return err;
    }

    uint32_t* job_chunk_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * job_count);
    LONGTAIL_FATAL_ASSERT(job_chunk_counts, return ENOMEM)
    TLongtail_Hash* hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * max_chunk_count);
    LONGTAIL_FATAL_ASSERT(hashes, return ENOMEM)
    uint32_t* sizes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_chunk_count);
    LONGTAIL_FATAL_ASSERT(sizes, return ENOMEM)
    uint32_t* tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_chunk_count);
    LONGTAIL_FATAL_ASSERT(tags, return ENOMEM)

    struct HashJob* hash_jobs = (struct HashJob*)Longtail_Alloc(sizeof(struct HashJob) * job_count);
    LONGTAIL_FATAL_ASSERT(hash_jobs, return ENOMEM)

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
            job->m_MaxChunkSize = max_chunk_size;
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
    LONGTAIL_FATAL_ASSERT(!err, return err)

    err = 0;
    for (uint32_t i = 0; i < jobs_started; ++i)
    {
        if (hash_jobs[i].m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "ChunkAssets: Failed to hash `%s`, %d", hash_jobs[i].m_Path, hash_jobs[i].m_Err)
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
        *chunk_sizes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * *chunk_count);
        LONGTAIL_FATAL_ASSERT(*chunk_sizes, return ENOMEM)
        *chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * *chunk_count);
        LONGTAIL_FATAL_ASSERT(*chunk_hashes, return ENOMEM)
        *chunk_tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * *chunk_count);
        LONGTAIL_FATAL_ASSERT(*chunk_tags, return ENOMEM)

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
            err = hash_api->HashBuffer(hash_api, sizeof(TLongtail_Hash) * asset_chunk_counts[a], &(*chunk_hashes)[chunk_start_index], &content_hashes[a]);
            if (err)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "ChunkAssets: Failed to hash chunks for `%s`, %d", &paths->m_Data[paths->m_Offsets[a]], err)
                Longtail_Free(*chunk_sizes);
                *chunk_sizes = 0;
                Longtail_Free(*chunk_hashes);
                *chunk_hashes = 0;
                Longtail_Free(*chunk_tags);
                *chunk_tags = 0;
                return err;
            }
        }
    }

    Longtail_Free(tags);
    tags = 0;

    Longtail_Free(hashes);
    hashes = 0;

    Longtail_Free(sizes);
    sizes = 0;

    Longtail_Free(job_chunk_counts);
    job_chunk_counts = 0;

    Longtail_Free(hash_jobs);
    hash_jobs = 0;

    return err;
}

size_t Longtail_GetVersionIndexDataSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size)
{
    size_t version_index_data_size =
        sizeof(uint32_t) +                              // m_Version
        sizeof(uint32_t) +                              // m_HashAPI
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
    return sizeof(struct Longtail_VersionIndex) +
            Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, path_data_size);
}

static int InitVersionIndexFromData(
    struct Longtail_VersionIndex* version_index,
    void* data,
    size_t data_size)
{
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)

    char* p = (char*)data;

    size_t version_index_data_start = (size_t)(uintptr_t)p;

    version_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*version_index->m_Version) != LONGTAIL_VERSION_INDEX_VERSION_0_0_1)
    {
        return EBADF;
    }

    version_index->m_HashAPI = (uint32_t*)(void*)p;
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

    if (Longtail_GetVersionIndexDataSize(asset_count, chunk_count, asset_chunk_index_count, 0) > data_size)
    {
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

struct Longtail_VersionIndex* Longtail_BuildVersionIndex(
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
    uint32_t hash_api_identifier)
{
    LONGTAIL_FATAL_ASSERT(mem != 0, return 0)
    LONGTAIL_FATAL_ASSERT(mem_size != 0, return 0)
    LONGTAIL_FATAL_ASSERT(paths != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || path_hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || content_hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || content_sizes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(asset_chunk_counts == 0 || asset_chunk_index_starts != 0, return 0)
    LONGTAIL_FATAL_ASSERT(*paths->m_PathCount == 0 || asset_chunk_counts != 0, return 0)
    LONGTAIL_FATAL_ASSERT(asset_chunk_index_count >= chunk_count, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || asset_chunk_indexes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_sizes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_hashes != 0, return 0)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_tags != 0, return 0)

    uint32_t asset_count = *paths->m_PathCount;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)mem;
    uint32_t* p = (uint32_t*)(void*)&version_index[1];
    version_index->m_Version = &p[0];
    version_index->m_HashAPI = &p[1];
    version_index->m_AssetCount = &p[2];
    version_index->m_ChunkCount = &p[3];
    version_index->m_AssetChunkIndexCount = &p[4];
    *version_index->m_Version = LONGTAIL_VERSION_INDEX_VERSION_0_0_1;
    *version_index->m_HashAPI = hash_api_identifier;
    *version_index->m_AssetCount = asset_count;
    *version_index->m_ChunkCount = chunk_count;
    *version_index->m_AssetChunkIndexCount = asset_chunk_index_count;

    InitVersionIndexFromData(version_index, &version_index[1], mem_size - sizeof(struct Longtail_VersionIndex));

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

    return version_index;
}

int Longtail_CreateVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_ProgressAPI* progress_api,
    const char* root_path,
    const struct Longtail_Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_permissions,
    const uint32_t* asset_tags,
    uint32_t max_chunk_size,
    struct Longtail_VersionIndex** out_version_index)
{
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(root_path != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(paths != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_chunk_size != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionIndex: From `%s` with %u assets", root_path, *paths->m_PathCount)

    uint32_t path_count = *paths->m_PathCount;

    if (path_count == 0)
    {
        size_t version_index_size = Longtail_GetVersionIndexSize(path_count, 0, 0, 0);
        void* version_index_mem = Longtail_Alloc(version_index_size);
        LONGTAIL_FATAL_ASSERT(version_index_mem, return ENOMEM)

        struct Longtail_VersionIndex* version_index = Longtail_BuildVersionIndex(
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
            0,            // chunk_sizes
            0,           // chunk_hashes
            0,// chunk_tags
            hash_api->GetIdentifier(hash_api));
        LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
        *out_version_index = version_index;
        return 0;
    }

    TLongtail_Hash* path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * path_count);
    LONGTAIL_FATAL_ASSERT(path_hashes != 0, return ENOMEM)
    TLongtail_Hash* content_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * path_count);
    LONGTAIL_FATAL_ASSERT(content_hashes != 0, return ENOMEM)
    uint32_t* asset_chunk_counts = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * path_count);
    LONGTAIL_FATAL_ASSERT(asset_chunk_counts != 0, return ENOMEM)

    uint32_t assets_chunk_index_count = 0;
    uint32_t* asset_chunk_sizes = 0;
    uint32_t* asset_chunk_tags = 0;
    TLongtail_Hash* asset_chunk_hashes = 0;
    uint32_t* asset_chunk_start_index = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * path_count);
    LONGTAIL_FATAL_ASSERT(asset_chunk_start_index, return ENOMEM)

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
        max_chunk_size,
        &assets_chunk_index_count);
    if (err) {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionIndex: Failed to chunk and hash assets in `%s`, %d", root_path, err)
        Longtail_Free(asset_chunk_tags);
        asset_chunk_tags = 0;
        Longtail_Free(asset_chunk_start_index);
        asset_chunk_start_index = 0;
        Longtail_Free(asset_chunk_hashes);
        asset_chunk_hashes = 0;
        Longtail_Free(asset_chunk_sizes);
        asset_chunk_sizes = 0;
        Longtail_Free(content_hashes);
        content_hashes = 0;
        Longtail_Free(path_hashes);
        path_hashes = 0;
        return err;
    }

    uint32_t* asset_chunk_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);
    LONGTAIL_FATAL_ASSERT(asset_chunk_indexes != 0, return ENOMEM)
    TLongtail_Hash* compact_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * assets_chunk_index_count);
    LONGTAIL_FATAL_ASSERT(compact_chunk_hashes != 0, return ENOMEM)
    uint32_t* compact_chunk_sizes =  (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);
    LONGTAIL_FATAL_ASSERT(compact_chunk_sizes != 0, return ENOMEM)
    uint32_t* compact_chunk_tags =  (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * assets_chunk_index_count);
    LONGTAIL_FATAL_ASSERT(compact_chunk_tags != 0, return ENOMEM)

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
    LONGTAIL_FATAL_ASSERT(version_index_mem, return ENOMEM)

    struct Longtail_VersionIndex* version_index = Longtail_BuildVersionIndex(
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
        hash_api->GetIdentifier(hash_api));
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)

    Longtail_Free(compact_chunk_tags);
    compact_chunk_tags = 0;
    Longtail_Free(compact_chunk_sizes);
    compact_chunk_sizes = 0;
    Longtail_Free(compact_chunk_hashes);
    compact_chunk_hashes = 0;
    Longtail_Free(asset_chunk_indexes);
    asset_chunk_indexes = 0;
    Longtail_Free(asset_chunk_tags);
    asset_chunk_tags = 0;
    Longtail_Free(asset_chunk_sizes);
    asset_chunk_sizes = 0;
    Longtail_Free(asset_chunk_hashes);
    asset_chunk_hashes = 0;
    Longtail_Free(asset_chunk_start_index);
    asset_chunk_start_index = 0;
    Longtail_Free(asset_chunk_counts);
    asset_chunk_counts = 0;
    Longtail_Free(content_hashes);
    content_hashes = 0;
    Longtail_Free(path_hashes);
    path_hashes = 0;

    *out_version_index = version_index;
    return 0;
}

int Longtail_WriteVersionIndexToBuffer(
    const struct Longtail_VersionIndex* version_index,
    void** out_buffer,
    size_t* out_size)
{
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_size != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteVersionIndexToBuffer: %u assets", version_index->m_AssetCount)
    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteVersionIndex: Writing index to `%s` containing %u assets in %u chunks", path, *version_index->m_AssetCount, *version_index->m_ChunkCount)
    size_t index_data_size = Longtail_GetVersionIndexDataSize(*version_index->m_AssetCount, *version_index->m_ChunkCount, *version_index->m_AssetChunkIndexCount, version_index->m_NameDataSize);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex: Failed create parent path for `%s`, %d", path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex: Failed open `%s` for write, %d", path, err)
        return err;
    }
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersionIndex: Failed to write to `%s`, %d", path, err)
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
    LONGTAIL_FATAL_ASSERT(buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_version_index != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadVersionIndexFromBuffer: Buffer size %u", size)

    size_t version_index_size = sizeof(struct Longtail_VersionIndex) + size;
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
    if (!version_index)
    {
        return ENOMEM;
    }
    memcpy(&version_index[1], buffer, size);
    int err = InitVersionIndexFromData(version_index, &version_index[1], size);
    if (err)
    {
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadVersionIndex: Reading from `%s`", path)
    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err != 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadVersionIndex: Failed to open file `%s`, %d", path, err)
        return err;
    }
    uint64_t version_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &version_index_data_size);
    if (err != 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadVersionIndex: Failed to get size of file `%s`, %d", path, err)
        return err;
    }
    size_t version_index_size = version_index_data_size + sizeof(struct Longtail_VersionIndex);
    struct Longtail_VersionIndex* version_index = (struct Longtail_VersionIndex*)Longtail_Alloc(version_index_size);
    if (!version_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex: Failed to allocate memory for `%s`", path)
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return err;
    }
    err = storage_api->Read(storage_api, file_handle, 0, version_index_data_size, &version_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadVersionIndex: Failed to read from `%s`, %d", path, err)
        Longtail_Free(version_index);
        version_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        return err;
    }
    err = InitVersionIndexFromData(version_index, &version_index[1], version_index_data_size);
    storage_api->CloseFile(storage_api, file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadVersionIndex: Bad format of file `%s`, %d", path, err)
        Longtail_Free(version_index);
        return err;
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ReadVersionIndex: Read index from `%s` containing %u assets in  %u chunks", path, *version_index->m_AssetCount, *version_index->m_ChunkCount)
    *out_version_index = version_index;
    return 0;
}

size_t Longtail_GetBlockIndexDataSize(uint32_t chunk_count)
{
    return
        sizeof(TLongtail_Hash) +                    // m_BlockHash
        sizeof(uint32_t) +                          // m_ChunkCount
        sizeof(uint32_t) +                          // m_Tag
        (sizeof(TLongtail_Hash) * chunk_count) +    // m_ChunkHashes
        (sizeof(uint32_t) * chunk_count);           // m_ChunkSizes
}

struct Longtail_BlockIndex* Longtail_InitBlockIndex(void* mem, uint32_t chunk_count)
{
    LONGTAIL_FATAL_ASSERT(mem != 0, return 0)

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
    LONGTAIL_FATAL_ASSERT(block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data != 0, return EINVAL)

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
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_indexes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_sizes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_index != 0, return EINVAL)

    void* mem = Longtail_Alloc(Longtail_GetBlockIndexSize(chunk_count));
    LONGTAIL_FATAL_ASSERT(mem, return ENOMEM)

    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(mem, chunk_count);
    for (uint32_t i = 0; i < chunk_count; ++i)
    {
        uint64_t chunk_index = chunk_indexes[i];
        block_index->m_ChunkHashes[i] = chunk_hashes[chunk_index];
        block_index->m_ChunkSizes[i] = chunk_sizes[chunk_index];
    }
    int err = hash_api->HashBuffer(hash_api, (uint32_t)(sizeof(TLongtail_Hash) * chunk_count), (void*)block_index->m_ChunkHashes, block_index->m_BlockHash);
    if (err != 0)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DynamicChunking: Failed to create hash for block index containing %u chunks", chunk_count)
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
    LONGTAIL_FATAL_ASSERT(block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_size != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteBlockIndexToBuffer: %" PRIu64 " chunks", *block_index->m_ChunkCount)

    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
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
    LONGTAIL_FATAL_ASSERT(buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_index != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadBlockIndexFromBuffer: Buffer size %u", size)

    size_t block_index_size = size + sizeof(struct Longtail_BlockIndex);
    struct Longtail_BlockIndex* block_index = (struct Longtail_BlockIndex*)Longtail_Alloc(block_index_size);
    if (!block_index)
    {
        return ENOMEM;
    }
    memcpy(&block_index[1], buffer, size);
    int err = Longtail_InitBlockIndexFromData(block_index, &block_index[1], size);
    if (err)
    {
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(block_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteBlockIndex: Write index to `%s`, chunks %" PRIu64, path, *block_index->m_ChunkCount)
    size_t index_data_size = Longtail_GetBlockIndexDataSize(*block_index->m_ChunkCount);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex: Failed to create parent folder for `%s`, %d", path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex: Failed to create `%s`, %d", path, err)
        return err;
    }
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &block_index[1]);
    if (err){
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteBlockIndex: Failed to write to `%s`, %d", path, err)
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadBlockIndex from `%s`", path)
    Longtail_StorageAPI_HOpenFile f;
    int err = storage_api->OpenReadFile(storage_api, path, &f);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to open block `%s`, %d", path, err)
        return err;
    }
    uint64_t block_size;
    err = storage_api->GetSize(storage_api, f, &block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to get size of block `%s`, %d", path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size < (sizeof(TLongtail_Hash) + sizeof(uint32_t)))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    if (block_size > 0xffffffff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    uint64_t read_offset = 0;
    TLongtail_Hash block_hash;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(TLongtail_Hash), &block_hash);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", path, err)
        storage_api->CloseFile(storage_api, f);
        return err;
    }
    read_offset += sizeof(TLongtail_Hash);
    uint32_t chunk_count;
    err = storage_api->Read(storage_api, f, read_offset, sizeof(uint32_t), &chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", path, err)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", path, err)
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
    stored_block->m_BlockIndex = (struct Longtail_BlockIndex*)&stored_block[1];
    int err = Longtail_InitBlockIndexFromData(
        stored_block->m_BlockIndex,
        block_data,
        block_data_size);
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
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(sizeof(struct Longtail_StoredBlock) + block_index_size + block_data_size);
    if (stored_block == 0)
    {
        return ENOMEM;
    }
    stored_block->m_BlockIndex = Longtail_InitBlockIndex(&stored_block[1], chunk_count);
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

size_t Longtail_GetContentIndexDataSize(uint64_t block_count, uint64_t chunk_count)
{
    size_t block_index_data_size = (size_t)(
        sizeof(uint32_t) +                          // m_Version
        sizeof(uint32_t) +                          // m_HashAPI
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
    return sizeof(struct Longtail_ContentIndex) +
        Longtail_GetContentIndexDataSize(block_count, chunk_count);
}

int Longtail_InitContentIndexFromData(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size)
{
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data != 0, return EINVAL)

    char* p = (char*)data;
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    if ((*content_index->m_Version) != LONGTAIL_CONTENT_INDEX_VERSION_0_0_1)
    {
        return EBADF;
    }

    content_index->m_HashAPI = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);
    content_index->m_BlockCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);
    content_index->m_ChunkCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    if (Longtail_GetContentIndexDataSize(block_count, chunk_count) > data_size)
    {
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

int Longtail_InitiContentIndex(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size,
    uint32_t hash_api,
    uint64_t block_count,
    uint64_t chunk_count)
{
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(data != 0, return EINVAL)

    uint8_t* p = (uint8_t*)data;
    content_index->m_Version = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_HashAPI = (uint32_t*)(void*)p;
    p += sizeof(uint32_t);

    content_index->m_BlockCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    content_index->m_ChunkCount = (uint64_t*)(void*)p;
    p += sizeof(uint64_t);

    *content_index->m_Version = LONGTAIL_CONTENT_INDEX_VERSION_0_0_1;
    *content_index->m_HashAPI = hash_api;
    *content_index->m_BlockCount = block_count;
    *content_index->m_ChunkCount = chunk_count;
    return Longtail_InitContentIndexFromData(content_index, &content_index[1], data_size);
}

static uint64_t GetUniqueHashes(uint64_t hash_count, const TLongtail_Hash* hashes, uint64_t* out_unique_hash_indexes)
{
    LONGTAIL_FATAL_ASSERT(hash_count != 0, return 0)
    LONGTAIL_FATAL_ASSERT(hashes != 0, return 0)

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
    uint64_t block_count,
    struct Longtail_BlockIndex** block_indexes,
    struct Longtail_ContentIndex** out_content_index)
{
    uint64_t chunk_count = 0;
    for (uint64_t b = 0; b < block_count; ++b)
    {
        chunk_count += *block_indexes[b]->m_ChunkCount;
    }

    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    LONGTAIL_FATAL_ASSERT(content_index, return ENOMEM)
    int err = Longtail_InitiContentIndex(
        content_index,
        &content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        hash_identifier,
        block_count,
        chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndexFromBlocks: Failed to initialize content index %d", err)
        Longtail_Free(content_index);
        return err;
    }

    uint64_t asset_index = 0;
    for (uint32_t b = 0; b < block_count; ++b)
    {
        struct Longtail_BlockIndex* block_index = block_indexes[b];
        content_index->m_BlockHashes[b] = *block_index->m_BlockHash;
        uint32_t chunk_offset = 0;
        for (uint32_t a = 0; a < *block_index->m_ChunkCount; ++a)
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

int Longtail_CreateContentIndex(
    struct Longtail_HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* chunk_tags,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_hashes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_sizes != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(chunk_count == 0 || chunk_tags != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_block_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_chunks_per_block != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateContentIndex: Creating index for %" PRIu64 " chunks", chunk_count)
    if (chunk_count == 0)
    {
        size_t content_index_size = Longtail_GetContentIndexSize(0, 0);
        struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
        LONGTAIL_FATAL_ASSERT(content_index, return ENOMEM)
        int err = Longtail_InitiContentIndex(
            content_index,
            &content_index[1],
            content_index_size - sizeof(struct Longtail_ContentIndex),
            hash_api->GetIdentifier(hash_api),
            0,
            0);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateContentIndex: Failed to initialize content index %d", err)
            Longtail_Free(content_index);
            return err;
        }
        *out_content_index = content_index;
        return 0;
    }
    uint64_t* chunk_indexes = (uint64_t*)Longtail_Alloc((size_t)(sizeof(uint64_t) * chunk_count));
    LONGTAIL_FATAL_ASSERT(chunk_indexes, return ENOMEM)
    uint64_t unique_chunk_count = GetUniqueHashes(chunk_count, chunk_hashes, chunk_indexes);

    struct Longtail_BlockIndex** block_indexes = (struct Longtail_BlockIndex**)Longtail_Alloc(sizeof(struct Longtail_BlockIndex*) * unique_chunk_count);
    LONGTAIL_FATAL_ASSERT(block_indexes, return ENOMEM)

    uint64_t* stored_chunk_indexes = (uint64_t*)Longtail_Alloc(sizeof(uint64_t) * max_chunks_per_block);
    LONGTAIL_FATAL_ASSERT(stored_chunk_indexes, return ENOMEM)

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
        LONGTAIL_FATAL_ASSERT(!err, return err)

        ++block_count;
        ++i;
    }

    Longtail_Free(stored_chunk_indexes);
    stored_chunk_indexes = 0;
    Longtail_Free(chunk_indexes);
    chunk_indexes = 0;

    int err = Longtail_CreateContentIndexFromBlocks(
        hash_api->GetIdentifier(hash_api),
        block_count,
        block_indexes,
        out_content_index);

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

int Longtail_WriteContentIndexToBuffer(
    const struct Longtail_ContentIndex* content_index,
    void** out_buffer,
    size_t* out_size)
{
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_size != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteContentIndexToBuffer: %" PRIu64 " blocks", *content_index->m_BlockCount)

    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);
    *out_buffer = Longtail_Alloc(index_data_size);
    if (!(*out_buffer))
    {
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
    LONGTAIL_FATAL_ASSERT(buffer != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_content_index != 0, return EINVAL)
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadContentIndexFromBuffer: Buffer size %u", size)

    size_t content_index_size = size + sizeof(struct Longtail_ContentIndex);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    if (!content_index)
    {
        return ENOMEM;
    }
    memcpy(&content_index[1], buffer, size);
    int err = Longtail_InitContentIndexFromData(content_index, &content_index[1], size);
    if (err)
    {
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteContentIndex: Write index to `%s`, chunks %" PRIu64 ", blocks %" PRIu64 "", path, *content_index->m_ChunkCount, *content_index->m_BlockCount)
    size_t index_data_size = Longtail_GetContentIndexDataSize(*content_index->m_BlockCount, *content_index->m_ChunkCount);

    int err = EnsureParentPathExists(storage_api, path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex: Failed to create parent folder for `%s`, %d", path, err)
        return err;
    }
    Longtail_StorageAPI_HOpenFile file_handle;
    err = storage_api->OpenWriteFile(storage_api, path, 0, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex: Failed to create `%s`, %d", path, err)
        return err;
    }
    err = storage_api->Write(storage_api, file_handle, 0, index_data_size, &content_index[1]);
    if (err){
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentIndex: Failed to write to `%s`, %d", path, err)
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
    LONGTAIL_FATAL_ASSERT(storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ReadContentIndex from `%s`", path)
    Longtail_StorageAPI_HOpenFile file_handle;
    int err = storage_api->OpenReadFile(storage_api, path, &file_handle);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadContentIndex: Failed to open `%s`, %d", path, err)
        return err;
    }
    uint64_t content_index_data_size;
    err = storage_api->GetSize(storage_api, file_handle, &content_index_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadContentIndex: Failed to get size of `%s`, %d", path, err)
        return err;
    }
    uint64_t content_index_size = sizeof(struct Longtail_ContentIndex) + content_index_data_size;
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc((size_t)(content_index_size));
    if (!content_index)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex: Failed allocate memory for `%s`", path)
        Longtail_Free(content_index);
        content_index = 0;
        storage_api->CloseFile(storage_api, file_handle);
        file_handle = 0;
        return ENOMEM;
    }
    err = storage_api->Read(storage_api, file_handle, 0, content_index_data_size, &content_index[1]);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ReadContentIndex: Failed to read from `%s`, %d", path, err)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ReadContentIndex: Bad format of file `%s`, %d", path, err)
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

    struct ChunkHashToAssetPart* asset_part_lookup = 0;
    for (uint64_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
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
    struct Longtail_AsyncCompleteAPI m_AsyncCompleteAPI;
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

static int BlockWriterJobOnComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    struct WriteBlockJob* job = (struct WriteBlockJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(job->m_AsyncCompleteAPI.OnComplete, return EINVAL);
    LONGTAIL_FATAL_ASSERT(job->m_StoredBlock, return EINVAL);
    LONGTAIL_FATAL_ASSERT(job->m_JobID, return EINVAL);
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

static int Longtail_WriteContentBlockJob(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)

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
    LONGTAIL_FATAL_ASSERT(block_data_buffer, job->m_Err = ENOMEM; return 0)
    char* write_buffer = block_data_buffer;
    char* write_ptr = write_buffer;

    uint32_t tag = 0;
    for (uint64_t chunk_index = first_chunk_index; chunk_index < first_chunk_index + chunk_count; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t tmp;
        intptr_t asset_part_index = hmgeti_ts(job->m_AssetPartLookup, chunk_hash, tmp);
        if (asset_part_index == -1)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Failed to get path for asset content 0x%" PRIx64, chunk_hash)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = EINVAL;
            return 0;
        }
        struct AssetPart* asset_part = &job->m_AssetPartLookup[asset_part_index].value;
        const char* asset_path = asset_part->m_Path;
        if (IsDirPath(asset_path))
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Directory should not have any chunks `%s`", asset_path)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = EINVAL;
            return 0;
        }

        char* full_path = source_storage_api->ConcatPath(source_storage_api, job->m_AssetsFolder, asset_path);
        uint64_t asset_content_offset = asset_part->m_Start;
        if (chunk_index != first_chunk_index && tag != asset_part->m_Tag)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_WriteContentBlockJob: Warning: Inconsistent tag type for chunks inside block 0x%" PRIx64 ", retaining %u", block_hash, tag)
        }
        else
        {
            tag = asset_part->m_Tag;
        }
        Longtail_StorageAPI_HOpenFile file_handle;
        int err = source_storage_api->OpenReadFile(source_storage_api, full_path, &file_handle);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Failed to open asset file `%s`, %d", full_path, err)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = err;
            return 0;
        }
        uint64_t asset_file_size;
        err = source_storage_api->GetSize(source_storage_api, file_handle, &asset_file_size);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Failed to get size of asset file `%s`, %d", full_path, err)
            Longtail_Free(block_data_buffer);
            block_data_buffer = 0;
            job->m_Err = err;
            return 0;
        }
        if (asset_file_size < (asset_content_offset + chunk_size))
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Mismatching asset size in asset `%s`, size is %" PRIu64 ", but expecting at least %" PRIu64 "", full_path, asset_file_size, asset_content_offset + chunk_size)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Failed to read from asset file `%s`, %d", full_path, err)
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

    struct Longtail_BlockIndex* block_index_ptr = (struct Longtail_BlockIndex*)Longtail_Alloc(Longtail_GetBlockIndexSize(chunk_count));
    LONGTAIL_FATAL_ASSERT(block_index_ptr, job->m_Err = ENOMEM; return 0)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContentBlockJob: Failed to store block 0x%" PRIx64 ", %d", block_hash, err)
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
    struct Longtail_ContentIndex* content_index,
    struct Longtail_VersionIndex* version_index,
    const char* assets_folder)
{
    LONGTAIL_FATAL_ASSERT(source_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(block_store_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(assets_folder != 0, return EINVAL)

    uint64_t chunk_count = *content_index->m_ChunkCount;
    uint64_t total_chunk_size = 0;
    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        total_chunk_size += content_index->m_ChunkLengths[c];
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteContent: Writing content from `%s`, chunks %" PRIu64 ", blocks %" PRIu64 ", size: %" PRIu64 " bytes", assets_folder, *content_index->m_ChunkCount, *content_index->m_BlockCount, total_chunk_size)
    uint64_t block_count = *content_index->m_BlockCount;
    if (block_count == 0)
    {
        return 0;
    }

    int err = job_api->ReserveJobs(job_api, (uint32_t)block_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent: Failed to reserve jobs when writing from `%s`, %d", assets_folder, err)
        return err;
    }

    struct ChunkHashToAssetPart* asset_part_lookup;
    err = CreateAssetPartLookup(version_index, &asset_part_lookup);
    if (!asset_part_lookup)
    {
        return err;
    }

    struct WriteBlockJob* write_block_jobs = (struct WriteBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteBlockJob) * block_count));
    LONGTAIL_FATAL_ASSERT(write_block_jobs, return ENOMEM)
    uint32_t block_start_chunk_index = 0;
    uint32_t job_count = 0;
    for (uint64_t block_index = 0; block_index < block_count; ++block_index)
    {
        TLongtail_Hash block_hash = content_index->m_BlockHashes[block_index];
        uint32_t chunk_count = 0;
        while(content_index->m_ChunkBlockIndexes[block_start_chunk_index + chunk_count] == block_index)
        {
            ++chunk_count;
        }

        int err = block_store_api->GetStoredBlock(block_store_api, block_hash, 0, 0);
        if (err == 0)
        {
            block_start_chunk_index += chunk_count;
            continue;
        }

        if (err != ENOENT)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent: Failed to check block 0x%" PRIx64 ", %d", block_hash, err)
            hmfree(asset_part_lookup);
            asset_part_lookup = 0;
            Longtail_Free(write_block_jobs);
            write_block_jobs = 0;
            return err;
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
        job->m_ContentIndex = content_index;
        job->m_BlockHash = block_hash;
        job->m_AssetPartLookup = asset_part_lookup;
        job->m_FirstChunkIndex = block_start_chunk_index;
        job->m_ChunkCount = chunk_count;
        job->m_Err = EINVAL;

        Longtail_JobAPI_JobFunc func[1] = { Longtail_WriteContentBlockJob };
        void* ctx[1] = { job };

        Longtail_JobAPI_Jobs jobs;
        err = job_api->CreateJobs(job_api, 1, func, ctx, &jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, 1, jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)

        block_start_chunk_index += chunk_count;
    }

    err = job_api->WaitForAllJobs(job_api, progress_api);
    LONGTAIL_FATAL_ASSERT(!err, return err)

    err = 0;
    while (job_count--)
    {
        struct WriteBlockJob* job = &write_block_jobs[job_count];
        if (job->m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteContent: Failed to write content, %d", job->m_Err)
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

    struct ContentLookup* cl = (struct ContentLookup*)Longtail_Alloc(sizeof(struct ContentLookup));
    LONGTAIL_FATAL_ASSERT(cl, return ENOMEM)
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
    struct Longtail_AsyncCompleteAPI m_AsyncCompleteAPI;
    struct Longtail_BlockStoreAPI* m_BlockStoreAPI;
    struct Longtail_JobAPI* m_JobAPI;
    uint32_t m_JobID;
    TLongtail_Hash m_BlockHash;
    struct Longtail_StoredBlock* m_StoredBlock;
    int m_Err;
};

int BlockReaderJobOnComplete(struct Longtail_AsyncCompleteAPI* async_complete_api, int err)
{
    struct BlockReaderJob* job = (struct BlockReaderJob*)async_complete_api;
    LONGTAIL_FATAL_ASSERT(job->m_AsyncCompleteAPI.OnComplete, return EINVAL);
    job->m_Err = err;
    job->m_JobAPI->ResumeJob(job->m_JobAPI, job->m_JobID);
    return 0;
}

static int BlockReader(void* context, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(context != 0, return 0)

    struct BlockReaderJob* job = (struct BlockReaderJob*)context;

    if (job->m_AsyncCompleteAPI.OnComplete)
    {
        // We got a notification so we are complete
        job->m_AsyncCompleteAPI.OnComplete = 0;
        return 0;
    }

    job->m_JobID = job_id;
    job->m_AsyncCompleteAPI.OnComplete = BlockReaderJobOnComplete;
    
    int err = job->m_BlockStoreAPI->GetStoredBlock(job->m_BlockStoreAPI, job->m_BlockHash, &job->m_StoredBlock, &job->m_AsyncCompleteAPI);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ReadBlockData: Failed to read block 0x%" PRIx64 ", %d", job->m_BlockHash, err)
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
    LONGTAIL_FATAL_ASSERT(!err, return err)

    if (job->m_BlockReaderJobCount > 0)
    {
        Longtail_JobAPI_Jobs block_readion_jobs;
        err = job_api->CreateJobs(job_api, job->m_BlockReaderJobCount, block_read_funcs, block_read_ctx, &block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        Longtail_JobAPI_JobFunc sync_write_funcs[1] = { WriteReady };
        void* sync_write_ctx[1] = { 0 };
        Longtail_JobAPI_Jobs write_sync_job;
        err = job_api->CreateJobs(job_api, 1, sync_write_funcs, sync_write_ctx, &write_sync_job);
        LONGTAIL_FATAL_ASSERT(!err, return err)

        err = job_api->AddDependecies(job_api, 1, write_job, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->AddDependecies(job_api, 1, write_job, job->m_BlockReaderJobCount, block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, job->m_BlockReaderJobCount, block_readion_jobs);
        LONGTAIL_FATAL_ASSERT(!err, return err)

        *out_jobs = write_sync_job;
        return 0;
    }
    *out_jobs = write_job;
    return 0;
}

int WritePartialAssetFromBlocks(void* context, uint32_t job_id)
{
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
            break;
        }
        block_hashes[d] = job->m_BlockReaderJobs[d].m_BlockHash;
        stored_block[d] = job->m_BlockReaderJobs[d].m_StoredBlock;
    }

    if (job->m_Err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to block_read blocks, %d", job->m_Err)
        for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
        {
            if (stored_block[d])
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Skipping write to asset `%s` due to previous write failure", asset_path)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to create parent folder for `%s` in `%s`, %d", asset_path, job->m_VersionFolder, err)
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to create folder for `%s` in `%s`, %d", asset_path, job->m_VersionFolder, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Unable to create asset `%s` in `%s`, %d", asset_path, job->m_VersionFolder, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to create next write/block_read job for asset `%s`, %d", asset_path, err)
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
                LONGTAIL_FATAL_ASSERT(!err, job->m_Err = EINVAL; return 0)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to write to asset `%s`, %d", asset_path, err)
            job->m_VersionStorageAPI->CloseFile(job->m_VersionStorageAPI, job->m_AssetOutputFile);
            job->m_AssetOutputFile = 0;

            for (uint32_t d = 0; d < block_block_reador_job_count; ++d)
            {
                stored_block[d]->Dispose(stored_block[d]);
            }
            if (sync_write_job)
            {
                err = job->m_JobAPI->ReadyJobs(job->m_JobAPI, 1, sync_write_job);
                LONGTAIL_FATAL_ASSERT(!err, job->m_Err = err; return 0)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WritePartialAssetFromBlocks: Failed to set permissions for asset `%s`, %d", asset_path, err)
            job->m_Err = err;
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock: Failed to read block 0x%" PRIx64 ", %d", block_hash, job->m_BlockReadJob.m_Err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock: Failed to create parent folder for `%s`, %d", full_asset_path, err)
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock: Unable to create asset `%s`, %d", full_asset_path, err)
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock: Failed to write to asset `%s`, %d", full_asset_path, err)
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssetsFromBlock: Failed to set permissions for asset `%s`, %d", asset_path, err)
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
    struct AssetWriteList* awl = (struct AssetWriteList*)(Longtail_Alloc(sizeof(struct AssetWriteList) + sizeof(uint32_t) * asset_count + sizeof(uint32_t) * asset_count));
    LONGTAIL_FATAL_ASSERT(awl, return 0)
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

    struct AssetWriteList* awl = CreateAssetWriteList(asset_count);
    if (awl == 0)
    {
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
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "BuildAssetWriteList: Failed to find chunk 0x%" PRIx64 " in content index for asset `%s`", chunk_hash, path)
            Longtail_Free(awl);
            awl = 0;
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "BuildAssetWriteList: Failed to find chunk 0x%" PRIx64 " in content index for asset `%s`", next_chunk_hash, path)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets: Failed to reserve %u jobs for folder `%s`, %d", awl->m_BlockJobCount + awl->m_AssetJobCount, version_path, err)
        Longtail_Free(awl);
        awl = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return err;
    }

    struct WriteAssetsFromBlockJob* block_jobs = (struct WriteAssetsFromBlockJob*)Longtail_Alloc((size_t)(sizeof(struct WriteAssetsFromBlockJob) * awl->m_BlockJobCount));
    LONGTAIL_FATAL_ASSERT(block_jobs, return ENOMEM)
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
        LONGTAIL_FATAL_ASSERT(!err, return err)

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
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->AddDependecies(job_api, 1, block_write_job, 1, block_readion_job);
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, 1, block_readion_job);
        LONGTAIL_FATAL_ASSERT(!err, return err)
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

    struct WritePartialAssetFromBlocksJob* asset_jobs = (struct WritePartialAssetFromBlocksJob*)Longtail_Alloc(sizeof(struct WritePartialAssetFromBlocksJob) * awl->m_AssetJobCount);
    LONGTAIL_FATAL_ASSERT(asset_jobs, return ENOMEM)
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
        LONGTAIL_FATAL_ASSERT(!err, return err)
        err = job_api->ReadyJobs(job_api, 1, write_sync_job);
        LONGTAIL_FATAL_ASSERT(!err, return err)
    }

    err = job_api->WaitForAllJobs(job_api, progress_api);
    LONGTAIL_FATAL_ASSERT(!err, return err)

    err = 0;
    for (uint32_t b = 0; b < block_job_count; ++b)
    {
        struct WriteAssetsFromBlockJob* job = &block_jobs[b];
        if (job->m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets: Failed to write single block assets to folder `%s`, %d", version_path, job->m_Err)
            err = err ? err : job->m_Err;
        }
    }
    for (uint32_t a = 0; a < awl->m_AssetJobCount; ++a)
    {
        struct WritePartialAssetFromBlocksJob* job = &asset_jobs[a];
        if (job->m_Err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "WriteAssets: Failed to write multi block assets to folder `%s`, %d", version_path, err)
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
    LONGTAIL_FATAL_ASSERT(block_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_WriteVersion: Write version to `%s`, assets %u, chunks %u", version_path, *version_index->m_AssetCount, *version_index->m_ChunkCount)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersion: Failed create content lookup to write version `%s`, %d", version_path, err)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_WriteVersion: Failed to create asset write list for version `%s`, %d", version_path, err)
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

    TLongtail_Hash* refs = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * reference_hash_count));
    LONGTAIL_FATAL_ASSERT(refs, return ENOMEM)
    TLongtail_Hash* news = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * new_hash_count));
    LONGTAIL_FATAL_ASSERT(news, return ENOMEM)
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
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_block_size != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(max_chunks_per_block != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateMissingContent: Checking for %u version chunks in %" PRIu64 " content chunks", *version_index->m_ChunkCount, *content_index->m_ChunkCount)
    uint64_t chunk_count = *version_index->m_ChunkCount;
    TLongtail_Hash* added_hashes = (TLongtail_Hash*)Longtail_Alloc((size_t)(sizeof(TLongtail_Hash) * chunk_count));
    LONGTAIL_FATAL_ASSERT(added_hashes, return ENOMEM)

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
            0,
            0,
            0,
            max_block_size,
            max_chunks_per_block,
            out_content_index);
        return err;
    }

    uint32_t* diff_chunk_sizes = (uint32_t*)Longtail_Alloc((size_t)(sizeof(uint32_t) * added_hash_count));
    LONGTAIL_FATAL_ASSERT(diff_chunk_sizes, return ENOMEM)
    uint32_t* diff_chunk_tags = (uint32_t*)Longtail_Alloc((size_t)(sizeof(uint32_t) * added_hash_count));
    LONGTAIL_FATAL_ASSERT(diff_chunk_tags, return ENOMEM)

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

    err = Longtail_CreateContentIndex(
        hash_api,
        added_hash_count,
        added_hashes,
        diff_chunk_sizes,
        diff_chunk_tags,
        max_block_size,
        max_chunks_per_block,
        out_content_index);

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
    LONGTAIL_FATAL_ASSERT(reference_content_index, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_content_index, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_RetargetContent: From %" PRIu64 " pick %" PRIu64 " chunks", *reference_content_index->m_ChunkCount, *content_index->m_ChunkCount)
    LONGTAIL_FATAL_ASSERT((*reference_content_index->m_HashAPI) == (*content_index->m_HashAPI), return EINVAL)

    struct HashToIndexItem* chunk_to_remote_block_index_lookup = 0;
    for (uint64_t i = 0; i < *reference_content_index->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = reference_content_index->m_ChunkHashes[i];
        uint64_t block_index = reference_content_index->m_ChunkBlockIndexes[i];
        hmput(chunk_to_remote_block_index_lookup, chunk_hash, block_index);
    }

    TLongtail_Hash* requested_block_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof(TLongtail_Hash) * *reference_content_index->m_BlockCount);
    if (requested_block_hashes == 0)
    {
        hmfree(chunk_to_remote_block_index_lookup);
        return ENOMEM;
    }
    uint64_t requested_block_count = 0;
    struct HashToIndexItem* requested_blocks_lookup = 0;
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[i];
        intptr_t remote_block_index_ptr = hmgeti(chunk_to_remote_block_index_lookup, chunk_hash);
        if (remote_block_index_ptr == -1)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent: reference content does not contain the chunk 0x%" PRIx64 "", chunk_hash)
            hmfree(requested_blocks_lookup);
            requested_blocks_lookup = 0;
            Longtail_Free(requested_block_hashes);
            requested_block_hashes = 0;
            hmfree(chunk_to_remote_block_index_lookup);
            chunk_to_remote_block_index_lookup = 0;
            return EINVAL;
        }
        uint64_t remote_block_index = chunk_to_remote_block_index_lookup[remote_block_index_ptr].value;
        TLongtail_Hash remote_block_hash = reference_content_index->m_BlockHashes[remote_block_index];

        intptr_t request_block_index_ptr = hmgeti(requested_blocks_lookup, remote_block_hash);
        if (-1 == request_block_index_ptr)
        {
            requested_block_hashes[requested_block_count] = remote_block_hash;
            hmput(requested_blocks_lookup, remote_block_hash, requested_block_count);
            ++requested_block_count;
        }
    }
    hmfree(chunk_to_remote_block_index_lookup);
    chunk_to_remote_block_index_lookup = 0;

    uint64_t chunk_count = 0;
    for (uint64_t c = 0; c < *reference_content_index->m_ChunkCount; ++c)
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
    LONGTAIL_FATAL_ASSERT(resulting_content_index, return ENOMEM)
    int err = Longtail_InitiContentIndex(
        resulting_content_index,
        &resulting_content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        *reference_content_index->m_HashAPI,
        requested_block_count,
        chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_RetargetContent: Failed to initialize content index %d", err)
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
    for (uint64_t c = 0; c < *reference_content_index->m_ChunkCount; ++c)
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

// TODO: This could be more efficient - we should only include blocks from remote_content_index that contains chunks not in local_content_index
int Longtail_MergeContentIndex(
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* remote_content_index,
    struct Longtail_ContentIndex** out_content_index)
{
    LONGTAIL_FATAL_ASSERT(local_content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(remote_content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT((*local_content_index->m_HashAPI) == (*remote_content_index->m_HashAPI), return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_MergeContentIndex: Merge %" PRIu64 " with %" PRIu64 " chunks", *local_content_index->m_ChunkCount, *remote_content_index->m_ChunkCount)

    uint64_t local_block_count = *local_content_index->m_BlockCount;
    uint64_t remote_block_count = *remote_content_index->m_BlockCount;
    uint64_t local_chunk_count = *local_content_index->m_ChunkCount;
    uint64_t remote_chunk_count = *remote_content_index->m_ChunkCount;
    uint64_t block_count = local_block_count + remote_block_count;
    uint64_t chunk_count = local_chunk_count + remote_chunk_count;
    size_t content_index_size = Longtail_GetContentIndexSize(block_count, chunk_count);
    struct Longtail_ContentIndex* content_index = (struct Longtail_ContentIndex*)Longtail_Alloc(content_index_size);
    LONGTAIL_FATAL_ASSERT(content_index, return ENOMEM)

    int err = Longtail_InitiContentIndex(
        content_index,
        &content_index[1],
        content_index_size - sizeof(struct Longtail_ContentIndex),
        *local_content_index->m_HashAPI,
        block_count,
        chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MergeContentIndex: Failed to initialize content index %d", err)
        Longtail_Free(content_index);
        return err;
    }

    for (uint64_t b = 0; b < local_block_count; ++b)
    {
        content_index->m_BlockHashes[b] = local_content_index->m_BlockHashes[b];
    }
    for (uint64_t b = 0; b < remote_block_count; ++b)
    {
        content_index->m_BlockHashes[local_block_count + b] = remote_content_index->m_BlockHashes[b];
    }
    for (uint64_t a = 0; a < local_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[a] = local_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[a] = local_content_index->m_ChunkBlockIndexes[a];
        content_index->m_ChunkBlockOffsets[a] = local_content_index->m_ChunkBlockOffsets[a];
        content_index->m_ChunkLengths[a] = local_content_index->m_ChunkLengths[a];
    }
    for (uint64_t a = 0; a < remote_chunk_count; ++a)
    {
        content_index->m_ChunkHashes[local_chunk_count + a] = remote_content_index->m_ChunkHashes[a];
        content_index->m_ChunkBlockIndexes[local_chunk_count + a] = local_block_count + remote_content_index->m_ChunkBlockIndexes[a];
        content_index->m_ChunkBlockOffsets[local_chunk_count + a] = remote_content_index->m_ChunkBlockOffsets[a];
        content_index->m_ChunkLengths[local_chunk_count + a] = remote_content_index->m_ChunkLengths[a];
    }
    *out_content_index = content_index;
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
/*
static int CompareIndexs(const void* a_ptr, const void* b_ptr)
{
    LONGTAIL_FATAL_ASSERT(a_ptr != 0, return 0)
    LONGTAIL_FATAL_ASSERT(b_ptr != 0, return 0)

    int64_t a = *(uint32_t*)a_ptr;
    int64_t b = *(uint32_t*)b_ptr;
    return (int)a - b;
}
*/

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
    LONGTAIL_FATAL_ASSERT(source_version != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(target_version != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_version_diff != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_CreateVersionDiff: Diff %u with %u assets", *source_version->m_AssetCount, *target_version->m_AssetCount)

    struct HashToIndexItem* source_path_hash_to_index = 0;
    struct HashToIndexItem* target_path_hash_to_index = 0;

    uint32_t source_asset_count = *source_version->m_AssetCount;
    uint32_t target_asset_count = *target_version->m_AssetCount;

    TLongtail_Hash* source_path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof (TLongtail_Hash) * source_asset_count);
    LONGTAIL_FATAL_ASSERT(source_path_hashes, return ENOMEM)
    TLongtail_Hash* target_path_hashes = (TLongtail_Hash*)Longtail_Alloc(sizeof (TLongtail_Hash) * target_asset_count);
    LONGTAIL_FATAL_ASSERT(target_path_hashes, return ENOMEM)

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

    uint32_t* removed_source_asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * source_asset_count);
    LONGTAIL_FATAL_ASSERT(removed_source_asset_indexes, return ENOMEM)
    uint32_t* added_target_asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * target_asset_count);
    LONGTAIL_FATAL_ASSERT(added_target_asset_indexes, return ENOMEM)

    const uint32_t max_modified_content_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    uint32_t* modified_source_content_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_content_count);
    LONGTAIL_FATAL_ASSERT(modified_source_content_indexes, return ENOMEM)
    uint32_t* modified_target_content_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_content_count);
    LONGTAIL_FATAL_ASSERT(modified_target_content_indexes, return ENOMEM)

    const uint32_t max_modified_permission_count = source_asset_count < target_asset_count ? source_asset_count : target_asset_count;
    uint32_t* modified_source_permissions_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_permission_count);
    LONGTAIL_FATAL_ASSERT(modified_source_permissions_indexes, return ENOMEM)
    uint32_t* modified_target_permissions_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * max_modified_permission_count);
    LONGTAIL_FATAL_ASSERT(modified_target_permissions_indexes, return ENOMEM)

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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Mismatching content for asset `%s`", source_path)
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
                    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Mismatching permissions for asset `%s`", source_path)
                }
            }

            ++source_index;
            ++target_index;
        }
        else if (source_path_hash < target_path_hash)
        {
            source_asset_index = (uint32_t)hmget(source_path_hash_to_index, source_path_hash);
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Removed asset `%s`", source_path)
            removed_source_asset_indexes[source_removed_count] = source_asset_index;
            ++source_removed_count;
            ++source_index;
        }
        else
        {
            target_asset_index = (uint32_t)hmget(target_path_hash_to_index, target_path_hash);
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Added asset `%s`", target_path)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Removed asset `%s`", source_path)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateVersionDiff: Added asset `%s`", target_path)
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

    struct Longtail_VersionDiff* version_diff = (struct Longtail_VersionDiff*)Longtail_Alloc(GetVersionDiffSize(source_removed_count, target_added_count, modified_content_count, modified_permissions_count));
    LONGTAIL_FATAL_ASSERT(version_diff, return ENOMEM)
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

    Longtail_Free(removed_source_asset_indexes);
    removed_source_asset_indexes = 0;

    Longtail_Free(added_target_asset_indexes);
    added_target_asset_indexes = 0;

    Longtail_Free(modified_source_permissions_indexes);
    modified_source_permissions_indexes = 0;

    Longtail_Free(modified_target_permissions_indexes);
    modified_target_permissions_indexes = 0;

    Longtail_Free(modified_source_content_indexes);
    modified_source_content_indexes = 0;

    Longtail_Free(modified_target_content_indexes);
    modified_target_content_indexes = 0;

    Longtail_Free(target_path_hashes);
    target_path_hashes = 0;

    Longtail_Free(source_path_hashes);
    source_path_hashes = 0;

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
    LONGTAIL_FATAL_ASSERT(block_store_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_storage_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(hash_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_api != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(source_version != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(target_version != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_diff != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_path != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ChangeVersion: Removing %u assets, adding %u assets and modifying %u assets in `%s`", *version_diff->m_SourceRemovedCount, *version_diff->m_TargetAddedCount, *version_diff->m_ModifiedContentCount, version_path)

    int err = EnsureParentPathExists(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to create parent path for `%s`, %d", version_path, err)
        return err;
    }
    err = SafeCreateDir(version_storage_api, version_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to create folder `%s`, %d", version_path, err)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed create content lookup for `%s`, %d", version_path, err)
        return err;
    }

    for (uint32_t i = 0; i < *target_version->m_ChunkCount; ++i)
    {
        TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[i];
        intptr_t chunk_content_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
        if (-1 == chunk_content_index_ptr)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ChangeVersion: Not all chunks in target version in `%s` is available", version_path)
            DeleteContentLookup(content_lookup);
            content_lookup = 0;
            return EINVAL;
       }
    }

    for (uint32_t i = 0; i < *version_diff->m_TargetAddedCount; ++i)
    {
        uint32_t target_asset_index = version_diff->m_TargetAddedAssetIndexes[i];
        const char* target_name = &target_version->m_NameData[target_version->m_NameOffsets[target_asset_index]];
        uint32_t target_chunk_count = target_version->m_AssetChunkCounts[target_asset_index];
        uint32_t target_chunk_index_start = target_version->m_AssetChunkIndexStarts[target_asset_index];
        for (uint32_t c = 0; c < target_chunk_count; ++c)
        {
            uint32_t target_chunk = target_version->m_AssetChunkIndexes[target_chunk_index_start + c];
            TLongtail_Hash chunk_hash = target_version->m_ChunkHashes[target_chunk];
            intptr_t chunk_content_index_ptr = hmgeti(content_lookup->m_ChunkHashToChunkIndex, chunk_hash);
            if (-1 == chunk_content_index_ptr)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Longtail_ChangeVersion: Not all chunks for asset `%s` is in target version in `%s` is available in content store", target_name, version_path)
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
           }
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
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to remove directory `%s`, %d", full_asset_path, err)
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
                            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to remove file `%s`, %d", full_asset_path, err)
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
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_ChangeVersion: Retrying removal of remaning %u assets in `%s`", removed_count - successful_remove_count, version_path)
            }
        }
    }

    uint32_t added_count = *version_diff->m_TargetAddedCount;
    uint32_t modified_content_count = *version_diff->m_ModifiedContentCount;
    uint32_t write_asset_count = added_count + modified_content_count;

    uint32_t* asset_indexes = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * write_asset_count);
    LONGTAIL_FATAL_ASSERT(asset_indexes, return ENOMEM)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to create asset write list for version `%s`, %d", version_path, err)
        Longtail_Free(asset_indexes);
        asset_indexes = 0;
        DeleteContentLookup(content_lookup);
        content_lookup = 0;
        return err;
    }

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

    Longtail_Free(asset_indexes);
    asset_indexes = 0;

    Longtail_Free(awl);
    awl = 0;

    DeleteContentLookup(content_lookup);
    content_lookup = 0;

    if (err)
    {
        return err;
    }

    if (retain_permissions)
    {
        for (uint32_t i = 0; i < *version_diff->m_ModifiedPermissionsCount; ++i)
        {
            uint32_t asset_index = version_diff->m_TargetPermissionsModifiedAssetIndexes[i];
            const char* asset_path = &target_version->m_NameData[target_version->m_NameOffsets[asset_index]];
            char* full_path = version_storage_api->ConcatPath(version_storage_api, version_path, asset_path);
            uint16_t permissions = (uint16_t)target_version->m_Permissions[asset_index];
            err = version_storage_api->SetPermissions(version_storage_api, full_path, permissions);
            Longtail_Free(full_path);
            if (err)
            {
                LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_ChangeVersion: Failed to set permissions for asset `%s`, %d", asset_path, err)
                break;
            }
        }
    }

    return err;
}

int Longtail_ValidateContent(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index)
{
    LONGTAIL_FATAL_ASSERT(content_index != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(version_index != 0, return EINVAL)

    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "Longtail_ValidateContent: %" PRIu64 " content chunks with %u version chunks", *content_index->m_ChunkCount, *version_index->m_ChunkCount)

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
        return err;
    }

    for (uint32_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
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
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
            }
            uint64_t content_chunk_index = content_lookup->m_ChunkHashToChunkIndex[content_chunk_index_ptr].value;
            if (content_index->m_ChunkHashes[content_chunk_index] != chunk_hash)
            {
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
            }
            if (content_index->m_ChunkLengths[content_chunk_index] != chunk_size)
            {
                DeleteContentLookup(content_lookup);
                content_lookup = 0;
                return EINVAL;
            }
        }
        if (asset_chunked_size != asset_size)
        {
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
    struct HashToIndexItem* version_chunk_lookup = 0;

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
        return err;
    }

    for (uint32_t asset_index = 0; asset_index < *version_index->m_AssetCount; ++asset_index)
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
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
    }

    for (uint64_t chunk_index = 0; chunk_index < *content_index->m_ChunkCount; ++chunk_index)
    {
        TLongtail_Hash chunk_hash = content_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = content_index->m_ChunkLengths[chunk_index];
        intptr_t version_chunk_index = hmgeti(version_chunk_lookup, chunk_hash);
        if (version_chunk_index == -1)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
        if (version_index->m_ChunkHashes[version_chunk_index] != chunk_hash)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
        if (version_index->m_ChunkSizes[version_chunk_index] != chunk_size)
        {
            hmfree(version_chunk_lookup);
            version_chunk_lookup = 0;
            return EINVAL;
        }
    }

    hmfree(version_chunk_lookup);
    version_chunk_lookup = 0;

    return 0;
}

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

    struct Longtail_Chunker* c = (struct Longtail_Chunker*)Longtail_Alloc((size_t)((sizeof(struct Longtail_Chunker) + params->max)));
    LONGTAIL_FATAL_ASSERT(c, return ENOMEM)
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

struct Longtail_ChunkRange Longtail_NextChunk(struct Longtail_Chunker* c)
{
    if (c->buf.len - c->off < c->params.max)
    {
        int err = FeedChunker(c);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to feed chunker, %d", err)
            struct Longtail_ChunkRange r = {0, 0, 0};
            return r;
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
