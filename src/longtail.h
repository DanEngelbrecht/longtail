#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_API
{
    void (*Dispose)(struct Longtail_API* api);
};

void Longtail_DisposeAPI(struct Longtail_API* api);
#define SAFE_DISPOSE_API(api) if (api) { Longtail_DisposeAPI(&api->m_API);}

typedef uint64_t TLongtail_Hash;
struct Longtail_BlockIndex;
struct Longtail_Paths;
struct Longtail_FileInfos;
struct Longtail_VersionIndex;
struct Longtail_StoredBlock;
struct Longtail_ContentIndex;
struct PathLookup;
struct ChunkHashToAssetPart;
struct Longtail_VersionDiff;

typedef struct Longtail_HashAPI_Context* Longtail_HashAPI_HContext;
struct Longtail_HashAPI
{
    struct Longtail_API m_API;
    uint32_t (*GetIdentifier)(struct Longtail_HashAPI* hash_api);
    int (*BeginContext)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context);
    void (*Hash)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data);
    uint64_t (*EndContext)(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context);
    int (*HashBuffer)(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash);
};

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

struct Longtail_StorageAPI
{
    struct Longtail_API m_API;
    int (*OpenReadFile)(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file);
    int (*GetSize)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size);
    int (*Read)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output);

    int (*OpenWriteFile)(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file);
    int (*Write)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input);
    int (*SetSize)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length);
    int (*SetPermissions)(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions);

    void (*CloseFile)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f);

    int (*CreateDir)(struct Longtail_StorageAPI* storage_api, const char* path);

    int (*RenameFile)(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path);
    char* (*ConcatPath)(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path);

    int (*IsDir)(struct Longtail_StorageAPI* storage_api, const char* path);
    int (*IsFile)(struct Longtail_StorageAPI* storage_api, const char* path);

    int (*RemoveDir)(struct Longtail_StorageAPI* storage_api, const char* path);
    int (*RemoveFile)(struct Longtail_StorageAPI* storage_api, const char* path);

    int (*StartFind)(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator);
    int (*FindNext)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
    void (*CloseFind)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
    const char* (*GetFileName)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
    const char* (*GetDirectoryName)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator);
    int (*GetEntryProperties)(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, uint64_t* out_size, uint16_t* out_permissions);
};

typedef struct Longtail_CompressionAPI_CompressionContext* Longtail_CompressionAPI_HCompressionContext;
typedef struct Longtail_CompressionAPI_DecompressionContext* Longtail_CompressionAPI_HDecompressionContext;
typedef struct Longtail_CompressionAPI_Settings* Longtail_CompressionAPI_HSettings;

struct Longtail_CompressionAPI
{
    struct Longtail_API m_API;

    size_t (*GetMaxCompressedSize)(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, size_t size);
    int (*Compress)(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size);
    int (*Decompress)(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size);
};

struct Longtail_CompressionRegistryAPI
{
    struct Longtail_API m_API;
    int (*GetCompressionType)(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, Longtail_CompressionAPI_HSettings* out_settings);
};

typedef void (*Longtail_JobAPI_JobFunc)(void* context);
typedef void (*Longtail_JobAPI_ProgressFunc)(void* context, uint32_t total_count, uint32_t done_count);
typedef void* Longtail_JobAPI_Jobs;

struct Longtail_JobAPI
{
    struct Longtail_API m_API;
    uint32_t (*GetWorkerCount)(struct Longtail_JobAPI* job_api);
    int (*ReserveJobs)(struct Longtail_JobAPI* job_api, uint32_t job_count);
    int (*CreateJobs)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs);
    int (*AddDependecies)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs);
    int (*ReadyJobs)(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs);
    int (*WaitForAllJobs)(struct Longtail_JobAPI* job_api, Longtail_JobAPI_ProgressFunc progress_func, void* progress_context);
};

struct Longtail_BlockStoreAPI
{
    struct Longtail_API m_API;
    int (*PutStoredBlock)(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block);
    int (*GetStoredBlock)(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_StoredBlock** out_stored_block);
    int (*GetIndex)(struct Longtail_BlockStoreAPI* block_store_api, uint32_t default_hash_api_identifier, Longtail_JobAPI_ProgressFunc progress_func, void* progress_context, struct Longtail_ContentIndex** out_content_index);
    int (*GetStoredBlockPath)(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path);
};

typedef void (*Longtail_Assert)(const char* expression, const char* file, int line);
void Longtail_SetAssert(Longtail_Assert assert_func);
typedef void (*Longtail_Log)(void* context, int level, const char* str);
void Longtail_SetLog(Longtail_Log log_func, void* context);
void Longtail_SetLogLevel(int level);

#if defined(LONGTAIL_ASSERTS)
    extern Longtail_Assert Longtail_Assert_private;
#    define LONGTAIL_FATAL_ASSERT(x, bail) \
        if (!(x)) \
        { \
            if (Longtail_Assert_private) \
            { \
                Longtail_Assert_private(#x, __FILE__, __LINE__); \
            } \
            bail; \
        }
#else // defined(LONGTAIL_ASSERTS)
#    define LONGTAIL_FATAL_ASSERT(x, y)
#endif // defined(LONGTAIL_ASSERTS)

#define LONGTAIL_LOG_LEVEL_INFO     0
#define LONGTAIL_LOG_LEVEL_DEBUG    1
#define LONGTAIL_LOG_LEVEL_WARNING  2
#define LONGTAIL_LOG_LEVEL_ERROR    3
#define LONGTAIL_LOG_LEVEL_OFF      4

#ifndef LONGTAIL_LOG
    void Longtail_CallLogger(int level, const char* fmt, ...);
    #define LONGTAIL_LOG(level, fmt, ...) \
        Longtail_CallLogger(level, fmt, __VA_ARGS__);
#endif


typedef void* (*Longtail_Alloc_Func)(size_t s);
typedef void (*Longtail_Free_Func)(void* p);
void Longtail_SetAllocAndFree(Longtail_Alloc_Func alloc, Longtail_Free_Func free);

void* Longtail_Alloc(size_t s);
void Longtail_Free(void* p);

int EnsureParentPathExists(struct Longtail_StorageAPI* storage_api, const char* path);
char* Longtail_Strdup(const char* path);

int Longtail_GetFilesRecursively(
    struct Longtail_StorageAPI* storage_api,
    const char* root_path,
    struct Longtail_FileInfos** out_file_infos);

int Longtail_CreateVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* root_path,
    const struct Longtail_Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_permissions,
    const uint32_t* asset_compression_types,
    uint32_t max_chunk_size,
    struct Longtail_VersionIndex** out_version_index);

int Longtail_CreateVersionIndexFromBlocks(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* root_path,
    const struct Longtail_Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_permissions,
    const uint32_t* asset_compression_types,
    uint32_t max_chunk_size,
    struct Longtail_VersionIndex** out_version_index);

int Longtail_WriteVersionIndexToBuffer(
    const struct Longtail_VersionIndex* version_index,
    void** out_buffer,
    size_t* out_size);

int Longtail_ReadVersionIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_VersionIndex** out_version_index);

int Longtail_WriteVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_VersionIndex* version_index,
    const char* path);

int Longtail_ReadVersionIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_VersionIndex** out_version_index);

size_t Longtail_GetContentIndexDataSize(
    uint64_t block_count,
    uint64_t chunk_count);

size_t Longtail_GetContentIndexSize(
    uint64_t block_count,
    uint64_t chunk_count);

int Longtail_InitContentIndexFromData(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size);

int Longtail_InitiContentIndex(
    struct Longtail_ContentIndex* content_index,
    void* data,
    uint64_t data_size,
    uint32_t hash_api,
    uint64_t block_count,
    uint64_t chunk_count);

int Longtail_CreateContentIndexFromBlocks(
    uint32_t hash_identifier,
    uint64_t block_count,
    struct Longtail_BlockIndex** block_indexes,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_CreateContentIndex(
    struct Longtail_HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* chunk_compression_types,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_WriteContentIndexToBuffer(
    const struct Longtail_ContentIndex* content_index,
    void** out_buffer,
    size_t* out_size);

int Longtail_ReadContentIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_WriteContentIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_ContentIndex* content_index,
    const char* path);

int Longtail_ReadContentIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_WriteContent(
    struct Longtail_StorageAPI* source_storage_api,
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_CompressionRegistryAPI* compression_registry_api,
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_VersionIndex* version_index,
    const char* assets_folder);

int Longtail_CreateMissingContent(
    struct Longtail_HashAPI* hash_api,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_RetargetContent(
    const struct Longtail_ContentIndex* reference_content_index,
    const struct Longtail_ContentIndex* content_index,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_MergeContentIndex(
    struct Longtail_ContentIndex* local_content_index,
    struct Longtail_ContentIndex* remote_content_index,
    struct Longtail_ContentIndex** out_content_index);

int Longtail_WriteVersion(
    struct Longtail_BlockStoreAPI* block_storage_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_CompressionRegistryAPI* compression_registry,
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index,
    const char* version_path,
    int retain_permissions);

int Longtail_CreateVersionDiff(
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    struct Longtail_VersionDiff** out_version_diff);

int Longtail_ChangeVersion(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StorageAPI* version_storage_api,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct Longtail_CompressionRegistryAPI* compression_registry,
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* source_version,
    const struct Longtail_VersionIndex* target_version,
    const struct Longtail_VersionDiff* version_diff,
    const char* version_path,
    int retain_permissions);

size_t Longtail_GetBlockIndexSize(uint32_t chunk_count);
size_t Longtail_GetBlockIndexDataSize(uint32_t chunk_count);
struct Longtail_BlockIndex* Longtail_InitBlockIndex(void* mem, uint32_t chunk_count);

int Longtail_InitBlockIndexFromData(
    struct Longtail_BlockIndex* block_index,
    void* data,
    uint64_t data_size);

int Longtail_CreateBlockIndex(
    struct Longtail_HashAPI* hash_api,
    uint32_t chunk_compression_type,
    uint32_t chunk_count,
    const uint64_t* chunk_indexes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    struct Longtail_BlockIndex** out_block_index);

int Longtail_WriteBlockIndexToBuffer(
    const struct Longtail_BlockIndex* block_index,
    void** out_buffer,
    size_t* out_size);

int Longtail_ReadBlockIndexFromBuffer(
    const void* buffer,
    size_t size,
    struct Longtail_BlockIndex** out_block_index);

int Longtail_WriteBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    struct Longtail_BlockIndex* block_index,
    const char* path);

int Longtail_ReadBlockIndex(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_BlockIndex** out_block_index);

int Longtail_CreateStoredBlock(
    TLongtail_Hash block_hash,
    uint32_t chunk_count,
    uint32_t compression_type,
    TLongtail_Hash* chunk_hashes,
    uint32_t* chunk_sizes,
    uint32_t block_data_size,
    struct Longtail_StoredBlock** out_stored_block);

struct Longtail_BlockIndex
{
    TLongtail_Hash* m_BlockHash;
    uint32_t* m_ChunkCount;
    uint32_t* m_ChunkCompressionType;
    TLongtail_Hash* m_ChunkHashes; //[]
    uint32_t* m_ChunkSizes; // []
};

struct Longtail_StoredBlock
{
    int (*Dispose)(struct Longtail_StoredBlock* stored_block);
    struct Longtail_BlockIndex* m_BlockIndex;
    void* m_BlockData;
    uint32_t m_BlockDataSize;
};

struct Longtail_Paths
{
    uint32_t m_DataSize;
    uint32_t* m_PathCount;
    uint32_t* m_Offsets;
    char* m_Data;
};

struct Longtail_FileInfos
{
    struct Longtail_Paths m_Paths;
    uint64_t* m_FileSizes;
    uint32_t* m_Permissions;
};

extern uint32_t Longtail_CurrentContentIndexVersion;

struct Longtail_ContentIndex
{
    uint32_t* m_Version;
    uint32_t* m_HashAPI;
    uint64_t* m_BlockCount;
    uint64_t* m_ChunkCount;

    TLongtail_Hash* m_BlockHashes;      // []
    TLongtail_Hash* m_ChunkHashes;      // []
    uint64_t* m_ChunkBlockIndexes;      // []
    uint32_t* m_ChunkBlockOffsets;      // []
    uint32_t* m_ChunkLengths;           // []
};

struct Longtail_VersionIndex
{
    uint32_t* m_Version;
    uint32_t* m_HashAPI;
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
    uint32_t* m_ChunkCompressionTypes;  // []

    uint32_t* m_NameOffsets;            // []
    uint32_t m_NameDataSize;
    uint32_t* m_Permissions;            // []
    char* m_NameData;
};

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

int Longtail_ValidateContent(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index);

int Longtail_ValidateVersion(
    const struct Longtail_ContentIndex* content_index,
    const struct Longtail_VersionIndex* version_index);

extern struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
        uint32_t compression_type_count,
        const uint32_t* compression_types,
        const struct Longtail_CompressionAPI** compression_apis,
        const Longtail_CompressionAPI_HSettings* compression_settings);

extern const uint32_t LONGTAIL_NO_COMPRESSION_TYPE;

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
    const uint32_t* chunk_compression_types,
    uint32_t hash_api_identifier);

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
