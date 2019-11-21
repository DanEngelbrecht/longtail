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

    StorageAPI_HOpenFile (*OpenWriteFile)(struct StorageAPI* storage_api, const char* path, int truncate);
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

typedef void (*JobAPI_JobFunc)(void* context);

struct JobAPI
{
    int (*ReserveJobs)(struct JobAPI* job_api, uint32_t job_count);
    void (*SubmitJobs)(struct JobAPI* job_api, uint32_t job_count, JobAPI_JobFunc job_funcs[], void* job_contexts[]);
    void (*WaitForAllJobs)(struct JobAPI* job_api);
};

typedef void (*Longtail_Assert)(const char* expression, const char* file, int line);
void Longtail_SetAssert(Longtail_Assert assert_func);

#if defined(LONGTAIL_ASSERTS)
void* Longtail_NukeMalloc(size_t s);
void Longtail_NukeFree(void* p);
#    define LONGTAIL_MALLOC(s) \
        Longtail_NukeMalloc(s)
#    define LONGTAIL_FREE(p) \
        Longtail_NukeFree(p)
#else
#    define LONGTAIL_MALLOC(s) \
        malloc(s)
#    define LONGTAIL_FREE(p) \
        free(p)
#endif // defined(LONGTAIL_ASSERTS)

typedef uint64_t TLongtail_Hash;
struct Paths;
struct VersionIndex;
struct ContentIndex;
struct PathLookup;
struct ChunkHashToAssetPart;
struct VersionDiff;

struct Paths* GetFilesRecursively(
    struct StorageAPI* storage_api,
    const char* root_path);

struct VersionIndex* CreateVersionIndex(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    const char* root_path,
    const struct Paths* paths,
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
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
    struct ContentIndex* content_index,
    struct ChunkHashToAssetPart* asset_part_lookup,
    const char* assets_folder,
    const char* content_folder);

struct ContentIndex* ReadContent(
    struct StorageAPI* storage_api,
    struct HashAPI* hash_api,
    struct JobAPI* job_api,
    const char* content_path);

struct ContentIndex* CreateMissingContent(
    struct HashAPI* hash_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* version,
    uint32_t max_block_size,
    uint32_t max_chunks_per_block);

struct ContentIndex* MergeContentIndex(
    struct ContentIndex* local_content_index,
    struct ContentIndex* remote_content_index);

struct PathLookup* CreateContentHashToPathLookup(
    const struct VersionIndex* version_index,
    uint64_t* out_unique_asset_indexes);

void FreePathLookup(struct PathLookup* path_lookup);

struct ChunkHashToAssetPart* CreateAssetPartLookup(
    struct VersionIndex* version_index);
void FreeAssetPartLookup(struct ChunkHashToAssetPart* asset_part_lookup);

int WriteVersion(
    struct StorageAPI* content_storage_api,
    struct StorageAPI* version_storage_api,
    struct CompressionAPI* compression_api,
    struct JobAPI* job_api,
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
    struct CompressionAPI* compression_api,
    const struct ContentIndex* content_index,
    const struct VersionIndex* source_version,
    const struct VersionIndex* target_version,
    const struct VersionDiff* version_diff,
    const char* content_path,
    const char* version_path);

///////////// Test functions

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
    const TLongtail_Hash* chunk_hashes);

struct Paths
{
    uint32_t m_DataSize;
    uint32_t* m_PathCount;
    uint32_t* m_Offsets;
    char* m_Data;
};

struct ContentIndex
{
    uint64_t* m_BlockCount;
    uint64_t* m_ChunkCount;

    TLongtail_Hash* m_BlockHashes;  // []
    TLongtail_Hash* m_ChunkHashes;  // []
    uint64_t* m_ChunkBlockIndexes;  // []
    uint32_t* m_ChunkBlockOffsets;  // []
    uint32_t* m_ChunkLengths;       // []
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
    uint32_t* m_ChunkSizes;         // []

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

struct Paths* MakePaths(
    uint32_t path_count,
    const char* const* path_names);

TLongtail_Hash GetPathHash(struct HashAPI* hash_api, const char* path);

char* GetBlockName(TLongtail_Hash block_hash);

struct Chunker;

struct ChunkerParams
{
    uint64_t min;
    uint64_t avg;
    uint64_t max;
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
