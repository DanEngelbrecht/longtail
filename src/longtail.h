#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct HashAPI_Context* HashAPI_HContext;
struct HashAPI
{
    HashAPI_HContext (*BeginContext)(struct HashAPI* hash_api);
    void (*Hash)(struct HashAPI* hash_api, HashAPI_HContext context, uint32_t length, void* data);
    uint64_t (*EndContext)(struct HashAPI* hash_api, HashAPI_HContext context);
};

typedef struct StorageAPI_OpenFile* StorageAPI_HOpenFile;
typedef struct StorageAPI_Iterator* StorageAPI_HIterator;

struct StorageAPI
{
    StorageAPI_HOpenFile (*OpenReadFile)(struct StorageAPI* storage_api, const char* path);
    uint64_t (*GetSize)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f);
    int (*Read)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output);
    void (*CloseRead)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f);

    StorageAPI_HOpenFile (*OpenWriteFile)(struct StorageAPI* storage_api, const char* path, uint64_t initial_size);
    int (*Write)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input);
    int (*SetSize)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t length);
    void (*CloseWrite)(struct StorageAPI* storage_api, StorageAPI_HOpenFile f);

    int (*CreateDir)(struct StorageAPI* storage_api, const char* path);

    int (*RenameFile)(struct StorageAPI* storage_api, const char* source_path, const char* target_path);
    char* (*ConcatPath)(struct StorageAPI* storage_api, const char* root_path, const char* sub_path);

    int (*IsDir)(struct StorageAPI* storage_api, const char* path);
    int (*IsFile)(struct StorageAPI* storage_api, const char* path);

    int (*RemoveDir)(struct StorageAPI* storage_api, const char* path);
    int (*RemoveFile)(struct StorageAPI* storage_api, const char* path);

    StorageAPI_HIterator (*StartFind)(struct StorageAPI* storage_api, const char* path);
    int (*FindNext)(struct StorageAPI* storage_api, StorageAPI_HIterator iterator);
    void (*CloseFind)(struct StorageAPI* storage_api, StorageAPI_HIterator iterator);
    const char* (*GetFileName)(struct StorageAPI* storage_api, StorageAPI_HIterator iterator);
    const char* (*GetDirectoryName)(struct StorageAPI* storage_api, StorageAPI_HIterator iterator);
    uint64_t (*GetEntrySize)(struct StorageAPI* storage_api, StorageAPI_HIterator iterator);
};

typedef struct CompressionAPI_CompressionContext* CompressionAPI_HCompressionContext;
typedef struct CompressionAPI_DecompressionContext* CompressionAPI_HDecompressionContext;
typedef struct CompressionAPI_Settings* CompressionAPI_HSettings;

struct CompressionAPI
{
    CompressionAPI_HSettings (*GetDefaultSettings)(struct CompressionAPI* compression_api);
    CompressionAPI_HSettings (*GetMaxCompressionSetting)(struct CompressionAPI* compression_api);

    CompressionAPI_HCompressionContext (*CreateCompressionContext)(struct CompressionAPI* compression_api, CompressionAPI_HSettings settings);
    size_t (*GetMaxCompressedSize)(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context, size_t size);
    size_t (*Compress)(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size);
    void (*DeleteCompressionContext)(struct CompressionAPI* compression_api, CompressionAPI_HCompressionContext context);

    CompressionAPI_HDecompressionContext (*CreateDecompressionContext)(struct CompressionAPI* compression_api);
    size_t (*Decompress)(struct CompressionAPI* compression_api, CompressionAPI_HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size);
    void (*DeleteDecompressionContext)(struct CompressionAPI* compression_api, CompressionAPI_HDecompressionContext context);
};

struct CompressionRegistry;

struct CompressionRegistry* CreateCompressionRegistry(
    uint32_t compression_type_count,
    const uint32_t* compression_types,
    const struct CompressionAPI** compression_apis,
    const CompressionAPI_HSettings* compression_settings);

typedef void (*JobAPI_JobFunc)(void* context);
typedef void (*JobAPI_ProgressFunc)(void* context, uint32_t total_count, uint32_t done_count);
typedef void* JobAPI_Jobs;

struct JobAPI
{
    int (*GetWorkerCount)(struct JobAPI* job_api);
    int (*ReserveJobs)(struct JobAPI* job_api, uint32_t job_count);
    JobAPI_Jobs (*CreateJobs)(struct JobAPI* job_api, uint32_t job_count, JobAPI_JobFunc job_funcs[], void* job_contexts[]);
    void (*AddDependecies)(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs, uint32_t dependency_job_count, JobAPI_Jobs dependency_jobs);
    void (*ReadyJobs)(struct JobAPI* job_api, uint32_t job_count, JobAPI_Jobs jobs);
    void (*WaitForAllJobs)(struct JobAPI* job_api, void* context, JobAPI_ProgressFunc process_func);
};

typedef void (*Longtail_Assert)(const char* expression, const char* file, int line);
void Longtail_SetAssert(Longtail_Assert assert_func);
typedef void (*Longtail_Log)(int level, const char* format, ...);
void Longtail_SetLog(Longtail_Log log_func);

typedef void* (*Longtail_Alloc_Func)(size_t s);
typedef void (*Longtail_Free_Func)(void* p);
void Longtail_SetAllocAndFree(Longtail_Alloc_Func alloc, Longtail_Free_Func free);

void* Longtail_Alloc(size_t s);
void Longtail_Free(void* p);

typedef uint64_t TLongtail_Hash;
struct Paths;
struct FileInfos;
struct VersionIndex;
struct ContentIndex;
struct PathLookup;
struct ChunkHashToAssetPart;
struct VersionDiff;

char* Longtail_Strdup(const char* path);

int EnsureParentPathExists(
    struct StorageAPI* storage_api,
    const char* path);

struct FileInfos* GetFilesRecursively(
    struct StorageAPI* storage_api,
    const char* root_path);

struct VersionIndex* CreateVersionIndex(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* root_path,
    const struct Paths* paths,
    const uint64_t* asset_sizes,
    const uint32_t* asset_compression_types,
    uint32_t max_chunk_size);

int WriteVersionIndex(
    struct StorageAPI* storage_api,
    struct VersionIndex* version_index,
    const char* path);

struct VersionIndex* ReadVersionIndex(
    struct StorageAPI* storage_api,
    const char* path);

struct ContentIndex* CreateContentIndex(
    struct HashAPI* hash_api,
    uint64_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_sizes,
    const uint32_t* chunk_compression_types,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block);

int WriteContentIndex(
    struct StorageAPI* storage_api,
    struct ContentIndex* content_index,
    const char* path);

struct ContentIndex* ReadContentIndex(
    struct StorageAPI* storage_api,
    const char* path);

int WriteContent(
    struct StorageAPI* source_storage_api,
    struct StorageAPI* target_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct ContentIndex* content_index,
    struct VersionIndex* version_index,
    const char* assets_folder,
    const char* content_folder);

struct ContentIndex* ReadContent(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const char* content_path);

struct ContentIndex* CreateMissingContent(
    struct HashAPI* hash_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block);

struct Paths* GetPathsForContentBlocks(
    struct ContentIndex* content_index);

struct ContentIndex* RetargetContent(
    const struct ContentIndex* reference_content_index,
    const struct ContentIndex* content_index);

struct ContentIndex* MergeContentIndex(
    struct ContentIndex* local_content_index,
    struct ContentIndex* remote_content_index);

int WriteVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionRegistry* compression_registry,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index,
    const char* content_path,
    const char* version_path);

struct VersionDiff* CreateVersionDiff(
    const struct VersionIndex* source_version,
    const struct VersionIndex* target_version);

int ChangeVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    JobAPI_ProgressFunc job_progress_func,
    void* job_progress_context,
    struct CompressionRegistry* compression_registry,
    const struct ContentIndex* content_index,
    const struct VersionIndex* source_version,
    const struct VersionIndex* target_version,
    const struct VersionDiff* version_diff,
    const char* content_path,
    const char* version_path);

struct Paths
{
    uint32_t m_DataSize;
    uint32_t* m_PathCount;
    uint32_t* m_Offsets;
    char* m_Data;
};

struct FileInfos
{
    struct Paths m_Paths;
    uint64_t* m_FileSizes;
};

struct ContentIndex
{
    uint64_t* m_BlockCount;
    uint64_t* m_ChunkCount;

    TLongtail_Hash* m_BlockHashes;      // []
    TLongtail_Hash* m_ChunkHashes;      // []
    uint64_t* m_ChunkBlockIndexes;      // []
    uint32_t* m_ChunkBlockOffsets;      // []
    uint32_t* m_ChunkLengths;           // []
};

struct VersionIndex
{
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
    char* m_NameData;
};

struct VersionDiff
{
    uint32_t* m_SourceRemovedCount;
    uint32_t* m_TargetAddedCount;
    uint32_t* m_ModifiedCount;
    uint32_t* m_SourceRemovedAssetIndexes;
    uint32_t* m_TargetAddedAssetIndexes;
    uint32_t* m_SourceModifiedAssetIndexes;
    uint32_t* m_TargetModifiedAssetIndexes;
};

int ValidateContent(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index);

int ValidateVersion(
    const struct ContentIndex* content_index,
    const struct VersionIndex* version_index);

///////////// Test functions

struct Paths* MakePaths(
    uint32_t path_count,
    const char* const* path_names);

size_t GetVersionIndexSize(
    uint32_t asset_count,
    uint32_t chunk_count,
    uint32_t asset_chunk_index_count,
    uint32_t path_data_size);

struct VersionIndex* BuildVersionIndex(
    void* mem,
    size_t mem_size,
    const struct Paths* paths,
    const TLongtail_Hash* path_hashes,
    const TLongtail_Hash* content_hashes,
    const uint64_t* content_sizes,
    const uint32_t* asset_chunk_index_starts,
    const uint32_t* asset_chunk_counts,
    uint32_t asset_chunk_index_count,
    const uint32_t* asset_chunk_indexes,
    uint32_t chunk_count,
    const uint32_t* chunk_sizes,
    const TLongtail_Hash* chunk_hashes,
    const uint32_t* chunk_compression_types);

struct Chunker;

struct ChunkerParams
{
    uint32_t min;
    uint32_t avg;
    uint32_t max;
};

struct ChunkRange
{
    const uint8_t* buf;
    uint64_t offset;
    uint32_t len;
};

struct ChunkRange NextChunk(struct Chunker* c);

typedef uint32_t (*Chunker_Feeder)(void* context, struct Chunker* chunker, uint32_t requested_size, char* buffer);

struct Chunker* CreateChunker(
    struct ChunkerParams* params,
    Chunker_Feeder feeder,
    void* context);

#ifdef __cplusplus
}
#endif
