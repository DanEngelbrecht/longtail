#include "longtail_blockstorestorage.h"

#if defined(__GNUC__) && !defined(__clang__) && !defined(APPLE) && !defined(__USE_GNU)
#define __USE_GNU
#endif

#include "../../src/ext/stb_ds.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

#if defined(__clang__) || defined(__GNUC__)
#if defined(WIN32)
    #include <malloc.h>
    #define CompareIgnoreCase _stricmp
#else
    #include <alloca.h>
    #define CompareIgnoreCase strcasecmp
#endif
#elif defined(_MSC_VER)
    #include <malloc.h>
    #define alloca _alloca
    #define CompareIgnoreCase _stricmp
#endif

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

struct BlockStoreStorageAPI_PathEntry
{
    const char* m_Name;
    TLongtail_Hash m_ParentHash;
    uint32_t m_AssetIndex;
    uint32_t m_ChildCount;
    uint32_t m_ChildStartIndex;
};

static SORTFUNC(BlockStoreStorageAPI_PathEntryParentPathCompare)
{
    struct BlockStoreStorageAPI_PathLookup* path_lookup = (struct BlockStoreStorageAPI_PathLookup*)context;
    struct BlockStoreStorageAPI_PathEntry* a = (struct BlockStoreStorageAPI_PathEntry*)a_ptr;
    struct BlockStoreStorageAPI_PathEntry* b = (struct BlockStoreStorageAPI_PathEntry*)b_ptr;
    if (a->m_ParentHash < b->m_ParentHash)
    {
        return -1;
    }
    else if (a->m_ParentHash > b->m_ParentHash)
    {
        return 1;
    }
    size_t a_length = strlen(a->m_Name);
    size_t b_length = strlen(b->m_Name);
    int a_is_dir = a_length == 0 || (a->m_Name[a_length - 1] == '/');
    int b_is_dir = b_length == 0 || (b->m_Name[b_length - 1] == '/');
    if (a_is_dir != b_is_dir)
    {
        return a_is_dir ? -1 : 1;
    }
    return CompareIgnoreCase(a->m_Name, b->m_Name);
}

static TLongtail_Hash BlockStoreStorageAPI_GetParentPathHash(struct Longtail_HashAPI* hash_api, const char* path)
{
    size_t path_length = strlen(path);
    size_t scan_pos = path_length;
    if (scan_pos > 0 && path[scan_pos - 1] == '/')
    {
        --scan_pos;
    }
    while (scan_pos > 0)
    {
        --scan_pos;
        if (path[scan_pos] == '/')
        {
            break;
        }
    }
    if (scan_pos == 0)
    {
        return 0;
    }
    size_t dir_length = scan_pos;
    char* dir_path = (char*)alloca(dir_length + 2);
    strncpy(dir_path, path, dir_length + 1);
    dir_path[dir_length + 1] = '\0';
    TLongtail_Hash hash;
    Longtail_GetPathHash(hash_api, dir_path, &hash);
    return hash;
}

const char* BlockStoreStorageAPI_GetPathName(const char* path)
{
    const char* name_start = path;
    size_t search_pos = 0;
    int is_dir = 0;
    while (path[search_pos] != '\0')
    {
        if (path[search_pos] == '/')
        {
            if (path[search_pos + 1] == '\0')
            {
                is_dir = 1;
                break;
            }
            ++search_pos;
            name_start = &path[search_pos];
        }
        ++search_pos;
    }
    return name_start;
}


struct BlockStoreStorageAPI_PathLookup
{
    struct BlockStoreStorageAPI_PathEntry* m_PathEntries;
    struct Longtail_LookupTable* m_LookupTable;
};

size_t GetPathEntriesSize(uint32_t asset_count)
{
    size_t path_lookup_size = sizeof(struct BlockStoreStorageAPI_PathLookup) +
        sizeof(struct BlockStoreStorageAPI_PathEntry) * (asset_count + 1) +
        Longtail_LookupTable_GetSize(asset_count + 1);
    return path_lookup_size;
}

static struct BlockStoreStorageAPI_PathLookup* BlockStoreStorageAPI_CreatePathLookup(
    void* mem,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_VersionIndex* version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(hash_api, "%s"),
        LONGTAIL_LOGFIELD(version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct BlockStoreStorageAPI_PathLookup* path_lookup = (struct BlockStoreStorageAPI_PathLookup*)mem;

    uint32_t asset_count = *version_index->m_AssetCount;
    path_lookup->m_PathEntries = (struct BlockStoreStorageAPI_PathEntry*)&path_lookup[1];
    path_lookup->m_LookupTable = Longtail_LookupTable_Create(&path_lookup->m_PathEntries[asset_count + 1], asset_count + 1, 0);
    path_lookup->m_PathEntries[0].m_Name = "";
    path_lookup->m_PathEntries[0].m_ParentHash = 0;
    path_lookup->m_PathEntries[0].m_AssetIndex = 0;
    path_lookup->m_PathEntries[0].m_ChildCount = 0;
    path_lookup->m_PathEntries[0].m_ChildStartIndex = 0;
    for (uint32_t a = 0; a < asset_count; ++a)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
        struct BlockStoreStorageAPI_PathEntry* path_entry = &path_lookup->m_PathEntries[a + 1];
        path_entry->m_Name = BlockStoreStorageAPI_GetPathName(path);
        path_entry->m_ParentHash = BlockStoreStorageAPI_GetParentPathHash(hash_api, path);
        path_entry->m_AssetIndex = a;
        path_entry->m_ChildCount = 0;
        path_entry->m_ChildStartIndex = 0;
    }
    QSORT(&path_lookup->m_PathEntries[1], asset_count, sizeof(struct BlockStoreStorageAPI_PathEntry), BlockStoreStorageAPI_PathEntryParentPathCompare, path_lookup);

    Longtail_LookupTable_Put(path_lookup->m_LookupTable, 0, 0);
    for (uint32_t a = 0; a < asset_count; ++a)
    {
        uint32_t asset_index = path_lookup->m_PathEntries[a + 1].m_AssetIndex;
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[asset_index]];
        uint64_t path_hash = 0;
        // We need to use new case insensitive path hash!
        int err = Longtail_GetPathHash(hash_api, path, &path_hash);
        if (err)
        {
            return 0;
        }
        Longtail_LookupTable_Put(path_lookup->m_LookupTable, path_hash, a + 1);
    }
    struct Longtail_LookupTable* find_first_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(asset_count + 1)), asset_count, 0);
    for (uint32_t p = 0; p < asset_count; ++p)
    {
        TLongtail_Hash parent_hash = path_lookup->m_PathEntries[p + 1].m_ParentHash;
        const uint64_t* parent_index_ptr = Longtail_LookupTable_Get(path_lookup->m_LookupTable, parent_hash);
        LONGTAIL_FATAL_ASSERT(ctx, parent_index_ptr, return 0)
        uint32_t parent_index = (uint32_t)*parent_index_ptr;
        if (0 == Longtail_LookupTable_PutUnique(find_first_lookup, parent_hash, p))
        {
            path_lookup->m_PathEntries[parent_index].m_ChildStartIndex = p + 1;
        }
        path_lookup->m_PathEntries[parent_index].m_ChildCount++;
    }
    Longtail_Free(find_first_lookup);

    return path_lookup;
};


struct BlockStoreStorageAPI
{
    struct Longtail_StorageAPI m_API;
    struct Longtail_HashAPI* m_HashAPI;
    struct Longtail_JobAPI* m_JobAPI;
    struct Longtail_BlockStoreAPI* m_BlockStore;
    struct Longtail_ContentIndex* m_ContentIndex;
    struct Longtail_VersionIndex* m_VersionIndex;
    struct Longtail_LookupTable* m_ChunkHashToBlockIndexLookup;
    struct BlockStoreStorageAPI_PathLookup* m_PathLookup;
    uint64_t* m_ChunkAssetOffsets;
};

struct BlockStoreStorageAPI_OpenFile
{
    uint32_t m_AssetIndex;
    uint32_t m_SeekChunkOffset;
    uint64_t m_SeekAssetPos;
};

struct BlockStoreStorageAPI_ChunkRange
{
    TLongtail_Hash m_BlockHash;
    uint64_t m_AssetStartOffset;
    uint32_t m_ChunkStart;
    uint32_t m_ChunkEnd;
};

struct BlockStoreStorageAPI_BlockRange
{
    TLongtail_Hash key;
    struct BlockStoreStorageAPI_ChunkRange value;
};

static int BlockStoreStorageAPI_ReadFromBlock(
    struct Longtail_StoredBlock* stored_block,
    struct BlockStoreStorageAPI_ChunkRange* range,
    struct BlockStoreStorageAPI* block_store_fs,
    struct BlockStoreStorageAPI_OpenFile* block_store_file,
    uint64_t start,
    uint64_t size,
    char* buffer,
    const uint32_t* chunk_indexes)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(range, "%p"),
        LONGTAIL_LOGFIELD(block_store_fs, "%p"),
        LONGTAIL_LOGFIELD(block_store_file, "%p"),
        LONGTAIL_LOGFIELD(start, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(chunk_indexes, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    uint64_t read_end = start + size;
    uint32_t chunk_block_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    size_t block_chunk_lookup_size = Longtail_LookupTable_GetSize(chunk_count);
    void* work_mem = Longtail_Alloc(block_chunk_lookup_size);
    if (work_mem == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    const TLongtail_Hash* block_chunk_hashes = stored_block->m_BlockIndex->m_ChunkHashes;
    const uint32_t* block_chunk_sizes = stored_block->m_BlockIndex->m_ChunkSizes;
    struct Longtail_LookupTable* block_chunk_lookup = Longtail_LookupTable_Create(work_mem, chunk_count, 0);
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        TLongtail_Hash chunk_hash = block_chunk_hashes[c];
        Longtail_LookupTable_Put(block_chunk_lookup, chunk_hash, chunk_block_offset);
        chunk_block_offset += block_chunk_sizes[c];
    }
    uint64_t asset_offset = range->m_AssetStartOffset;
    const char* block_data = (char*)stored_block->m_BlockData;
    const TLongtail_Hash* version_chunk_hashes = block_store_fs->m_VersionIndex->m_ChunkHashes;
    const uint32_t* version_chunk_sizes = block_store_fs->m_VersionIndex->m_ChunkSizes;
    for (uint32_t c = range->m_ChunkStart; c < range->m_ChunkEnd; ++c)
    {
        uint32_t chunk_index = chunk_indexes[c];
        TLongtail_Hash chunk_hash = version_chunk_hashes[chunk_index];
        uint32_t chunk_size = version_chunk_sizes[chunk_index];
        uint64_t asset_offset_chunk_end = asset_offset + chunk_size;
        LONGTAIL_FATAL_ASSERT(ctx, asset_offset_chunk_end >= start, return EINVAL)

        uint64_t* chunk_block_offset_ptr = Longtail_LookupTable_Get(block_chunk_lookup, chunk_hash);
        if (chunk_block_offset_ptr == 0)
        {
            asset_offset += chunk_size;
            continue;
        }
        uint32_t chunk_offset = 0;
        uint32_t read_length = chunk_size;
        if (asset_offset < start)
        {
            chunk_offset = (uint32_t)(start - asset_offset);
            read_length = chunk_size - chunk_offset;
            asset_offset = start;
        }
        if (asset_offset + read_length > read_end)
        {
            read_length = (uint32_t)(read_end - asset_offset);
        }

        uint32_t chunk_block_offset = (uint32_t)*chunk_block_offset_ptr;
        memcpy(&buffer[asset_offset - start], &block_data[chunk_block_offset + chunk_offset], read_length);
        asset_offset += read_length;
    }
    Longtail_Free(work_mem);
    return 0;
}

struct BlockStoreStorageAPI_ReadFromBlockJobData;

struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    uint32_t m_JobID;
    struct BlockStoreStorageAPI_ReadFromBlockJobData* m_Data;
};


struct BlockStoreStorageAPI_ReadFromBlockJobData
{
	struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI m_OnReadBlockCompleteAPI;
    struct BlockStoreStorageAPI_ChunkRange* m_Range;
    struct BlockStoreStorageAPI* m_BlockStoreFS;
    struct BlockStoreStorageAPI_OpenFile* m_BlockStoreFile;
    uint64_t m_Start;
    uint64_t m_Size;
    char* m_Buffer;
    const uint32_t* m_ChunkIndexes;
    struct Longtail_StoredBlock* m_StoredBlock;
    int m_Err;
};

static void BlockStoreStorageAPI_ReadBlock_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(async_complete_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(err, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI* cb = (struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI*)async_complete_api;
    struct Longtail_JobAPI* job_api = cb->m_Data->m_BlockStoreFS->m_JobAPI;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockStoreStorageAPI_ReadBlock_OnComplete(%p, %p, %d)",
            async_complete_api, stored_block, err)
    }
    cb->m_Data->m_Err = err;
    cb->m_Data->m_StoredBlock = stored_block;
    job_api->ResumeJob(job_api, cb->m_JobID);
}

static int BlockStoreStorageAPI_ReadFromBlockJob(void* context, uint32_t job_id, int is_cancelled)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u"),
        LONGTAIL_LOGFIELD(is_cancelled, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct BlockStoreStorageAPI_ReadFromBlockJobData* data = (struct BlockStoreStorageAPI_ReadFromBlockJobData*)context;

    if (!data->m_StoredBlock)
    {
        if (data->m_Err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "read from block failed with %d", data->m_Err)
            return 0;
        }
        // Don't need dynamic alloc, could be part of struct BlockStoreStorageAPI_ReadFromBlockJobData since it covers the lifetime of struct BlockStoreStorageAPI_ReadBlock_OnComplete
        struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI* complete_cb = &data->m_OnReadBlockCompleteAPI;
        complete_cb->m_API.OnComplete = BlockStoreStorageAPI_ReadBlock_OnComplete;
        complete_cb->m_JobID = job_id;
        complete_cb->m_Data = data;

        int err = data->m_BlockStoreFS->m_BlockStore->GetStoredBlock(data->m_BlockStoreFS->m_BlockStore, data->m_Range->m_BlockHash, &complete_cb->m_API);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "data->m_BlockStoreFS->m_BlockStore->GetStoredBlock() failed with %d",
                context, job_id, is_cancelled,
                err)
            data->m_Err = err;
            return 0;
        }
        return EBUSY;
    }

    data->m_Err = BlockStoreStorageAPI_ReadFromBlock(
        data->m_StoredBlock,
        data->m_Range,
        data->m_BlockStoreFS,
        data->m_BlockStoreFile,
        data->m_Start,
        data->m_Size,
        data->m_Buffer,
        data->m_ChunkIndexes);
    if (data->m_Err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockStoreStorageAPI_ReadFromBlock() failed with %d", data->m_Err)
    }
    if (data->m_StoredBlock->Dispose)
    {
        data->m_StoredBlock->Dispose(data->m_StoredBlock);
    }
    return 0;
}

static uint32_t BlockStoreStorageAPI_FindStartChunk(const uint64_t* a, uint32_t n, uint64_t val) {
    uint32_t first = 0;
    uint32_t count = n;
    while (count > 0)
    {
        uint32_t step = count/2;
        uint32_t it = first + step;
        if (a[it] < val)
        {
            first = it + 1;
            count -= step + 1;
        }
        else
        {
            count = step;
        }
    }
    if (first == n)
    {
        return first - 1;
    }
    if (a[first] > val)
    {
        return first - 1;
    }
    return first;
}

static int BlockStoreStorageAPI_SeekFile(
    struct BlockStoreStorageAPI* block_store_fs,
    struct BlockStoreStorageAPI_OpenFile* block_store_file,
    uint64_t pos)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_fs, "%p"),
        LONGTAIL_LOGFIELD(block_store_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    if (pos == block_store_file->m_SeekAssetPos)
    {
        return 0;
    }
    uint32_t asset_index = block_store_file->m_AssetIndex;
    uint32_t chunk_count = block_store_fs->m_VersionIndex->m_AssetChunkCounts[asset_index];
    if (chunk_count == 0)
    {
        return pos == 0 ? 0 : EIO;
    }
    uint32_t chunk_start_index = block_store_fs->m_VersionIndex->m_AssetChunkIndexStarts[asset_index];

    const uint64_t* chunk_asset_offsets = &block_store_fs->m_ChunkAssetOffsets[chunk_start_index];
    if (block_store_file->m_SeekChunkOffset < chunk_count)
    {
        if (pos >= chunk_asset_offsets[block_store_file->m_SeekChunkOffset])
        {
            if (block_store_file->m_SeekChunkOffset == chunk_count - 1)
            {
                // on last block
                return 0;
            }
            else if (pos < chunk_asset_offsets[block_store_file->m_SeekChunkOffset + 1])
            {
                return 0;
            }
        }
    }

    const uint32_t* chunk_indexes = &block_store_fs->m_VersionIndex->m_AssetChunkIndexes[chunk_start_index];

    uint64_t asset_size = block_store_fs->m_VersionIndex->m_AssetSizes[asset_index];

    uint32_t start_chunk_index = BlockStoreStorageAPI_FindStartChunk(chunk_asset_offsets, chunk_count, pos);
    uint64_t start_asset_offset = chunk_asset_offsets[start_chunk_index];

    block_store_file->m_SeekAssetPos = start_asset_offset;
    block_store_file->m_SeekChunkOffset = start_chunk_index;
    return 0;
}

static int BlockStoreStorageAPI_ReadFile(
    struct BlockStoreStorageAPI* block_store_fs,
    struct BlockStoreStorageAPI_OpenFile* block_store_file,
    uint64_t start,
    uint64_t size,
    void* out_buffer)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_fs, "%p"),
        LONGTAIL_LOGFIELD(block_store_file, "%p"),
        LONGTAIL_LOGFIELD(start, "%" PRIu64),
        LONGTAIL_LOGFIELD(size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_buffer, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    char* buffer = (char*)out_buffer;

    uint32_t asset_index = block_store_file->m_AssetIndex;
    const uint64_t read_end = start + size;
    const struct Longtail_VersionIndex* version_index = block_store_fs->m_VersionIndex;
    uint64_t asset_size = version_index->m_AssetSizes[asset_index];
    if (read_end > asset_size)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed trying to read past end of file", EIO)
        return EIO;
    }

    uint32_t chunk_count = version_index->m_AssetChunkCounts[asset_index];
    uint32_t chunk_start_index = version_index->m_AssetChunkIndexStarts[asset_index];
    const uint32_t* chunk_indexes = &version_index->m_AssetChunkIndexes[chunk_start_index];
    const TLongtail_Hash* chunk_hashes = version_index->m_ChunkHashes;
    const uint32_t* chunk_sizes = version_index->m_ChunkSizes;

    int err = BlockStoreStorageAPI_SeekFile(block_store_fs, block_store_file, start);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockStoreStorageAPI_SeekFile() failed with %d", err)
        return err;
    }

    uint32_t seek_chunk_offset = block_store_file->m_SeekChunkOffset;
    uint64_t seek_asset_pos = block_store_file->m_SeekAssetPos;

    uint32_t avg_chunk_size = (uint32_t)(asset_size / chunk_count);
    const uint32_t max_block_count = chunk_count - seek_chunk_offset;
    uint32_t estimated_block_count = (uint32_t)(size / avg_chunk_size) + 2;
    estimated_block_count = estimated_block_count > max_block_count ? max_block_count : estimated_block_count;

    size_t block_range_map_size = Longtail_LookupTable_GetSize(estimated_block_count);
    struct Longtail_LookupTable* block_range_map = Longtail_LookupTable_Create(Longtail_Alloc(block_range_map_size), estimated_block_count, 0);
    struct BlockStoreStorageAPI_ChunkRange* chunk_ranges = 0;
    arrsetcap(chunk_ranges, estimated_block_count);
    const TLongtail_Hash* block_hashes = block_store_fs->m_ContentIndex->m_BlockHashes;

    for (uint32_t c = seek_chunk_offset; c < chunk_count; ++c)
    {
        uint32_t chunk_index = chunk_indexes[c];
        TLongtail_Hash chunk_hash = chunk_hashes[chunk_index];
        const uint64_t* block_index_ptr = Longtail_LookupTable_Get(block_store_fs->m_ChunkHashToBlockIndexLookup, chunk_hash);
        LONGTAIL_FATAL_ASSERT(ctx, block_index_ptr, EINVAL)
        uint64_t block_index = *block_index_ptr;
        TLongtail_Hash block_hash = block_hashes[block_index];
        uint64_t* chunk_range_index = Longtail_LookupTable_PutUnique(block_range_map, block_hash, arrlen(chunk_ranges));
        if (chunk_range_index)
        {
            chunk_ranges[*chunk_range_index].m_ChunkEnd = c + 1;
        }
        else
        {
            struct BlockStoreStorageAPI_ChunkRange range = {block_hash, seek_asset_pos, c, c + 1};
            arrput(chunk_ranges, range);
        }
        block_store_file->m_SeekChunkOffset = c;
        block_store_file->m_SeekAssetPos = seek_asset_pos;

        uint32_t chunk_size = chunk_sizes[chunk_index];
        seek_asset_pos += chunk_size;
        if (seek_asset_pos >= read_end)
        {
            break;
        }
        if (Longtail_LookupTable_GetSpaceLeft(block_range_map) == 0)
        {
            uint64_t new_capacity = estimated_block_count + (estimated_block_count >> 2) + 2;
            estimated_block_count = new_capacity > max_block_count ? max_block_count : (uint32_t)new_capacity;
            block_range_map_size = Longtail_LookupTable_GetSize(estimated_block_count);
            struct Longtail_LookupTable* new_block_range_map = Longtail_LookupTable_Create(Longtail_Alloc(block_range_map_size), estimated_block_count, block_range_map);
            Longtail_Free(block_range_map);
            block_range_map = new_block_range_map;
        }
    }

    uint32_t block_count = (uint32_t)arrlen(chunk_ranges);
    LONGTAIL_FATAL_ASSERT(ctx, block_count > 0, return EINVAL);

    struct Longtail_JobAPI* job_api = block_store_fs->m_JobAPI;

	size_t work_mem_size = sizeof(struct BlockStoreStorageAPI_ReadFromBlockJobData) * block_count +
		sizeof(Longtail_JobAPI_JobFunc) * block_count +
		sizeof(void*) * block_count;
	void* work_mem = Longtail_Alloc(work_mem_size);
	if (!work_mem)
	{
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
		return ENOMEM;
	}
    struct BlockStoreStorageAPI_ReadFromBlockJobData* job_datas = (struct BlockStoreStorageAPI_ReadFromBlockJobData*)work_mem;
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)&job_datas[block_count];
    void** ctxs = (void**)&funcs[block_count];

    Longtail_JobAPI_Group job_group;
    err = job_api->ReserveJobs(job_api, block_count, &job_group);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)

    for (uint32_t b = 0; b < block_count; ++b)
    {
        struct BlockStoreStorageAPI_ChunkRange* range = &chunk_ranges[b];

        job_datas[b].m_Range = range;
        job_datas[b].m_BlockStoreFS = block_store_fs;
        job_datas[b].m_BlockStoreFile = block_store_file;
        job_datas[b].m_Start = start;
        job_datas[b].m_Size = size;
        job_datas[b].m_Buffer = buffer;
        job_datas[b].m_ChunkIndexes = chunk_indexes;
        job_datas[b].m_StoredBlock = 0;
        job_datas[b].m_Err = 0;

        funcs[b] = BlockStoreStorageAPI_ReadFromBlockJob;
        ctxs[b] = &job_datas[b];
    }
    Longtail_JobAPI_Jobs jobs;
    err = job_api->CreateJobs(job_api, job_group, block_count, funcs, ctxs, &jobs);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    err = job_api->ReadyJobs(job_api, block_count, jobs);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    err = job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);
    LONGTAIL_FATAL_ASSERT(ctx, err == 0, return err)
    Longtail_Free(work_mem);

    Longtail_Free(block_range_map);
    arrfree(chunk_ranges);
    return 0;
}

static int BlockStoreStorageAPI_OpenReadFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    Longtail_StorageAPI_HOpenFile* out_open_file)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_open_file, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;

    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, path, &path_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
        return err;
    }

    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_LookupTable_Get() failed with %d", ENOENT)
        return ENOENT;
    }
    uint32_t asset_index = block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index].m_AssetIndex;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)Longtail_Alloc(sizeof(struct BlockStoreStorageAPI_OpenFile));
    block_store_file->m_AssetIndex = asset_index;
    block_store_file->m_SeekChunkOffset = 0;
    block_store_file->m_SeekAssetPos = 0;
    *out_open_file = (Longtail_StorageAPI_HOpenFile)block_store_file;
    return 0;
}

static int BlockStoreStorageAPI_GetSize(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t* out_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(out_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_size != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    *out_size = block_store_fs->m_VersionIndex->m_AssetSizes[block_store_file->m_AssetIndex];
    return 0;
}

static int BlockStoreStorageAPI_Read(
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, output != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    int err = BlockStoreStorageAPI_ReadFile(block_store_fs, block_store_file, offset, length, output);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockStoreStorageAPI_ReadFile() failed with %d", err)
        return err;
    }
    return 0;
}

static int BlockStoreStorageAPI_OpenWriteFile(
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_open_file != 0, return 0)

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_Write(
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
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, input != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_SetSize(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f,
    uint64_t length)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p"),
        LONGTAIL_LOGFIELD(length, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_SetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(permissions, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_GetPermissions(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    uint16_t* out_permissions)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_permissions, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_permissions != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, path, &path_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
        return err;
    }

    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Longtail_LookupTable_Get() failed with %d", ENOENT)
        return ENOENT;
    }
    uint32_t asset_index = block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index].m_AssetIndex;
    *out_permissions = block_store_fs->m_VersionIndex->m_Permissions[asset_index];
    return 0;
}

static void BlockStoreStorageAPI_CloseFile(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HOpenFile f)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(f, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return)
    LONGTAIL_VALIDATE_INPUT(ctx, f != 0, return)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    Longtail_Free(block_store_file);
}

static int BlockStoreStorageAPI_CreateDir(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_RenameFile(
    struct Longtail_StorageAPI* storage_api,
    const char* source_path,
    const char* target_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(storage_api, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, source_path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, target_path != 0, return 0)

    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static char* BlockStoreStorageAPI_ConcatPath(
    struct Longtail_StorageAPI* storage_api,
    const char* root_path,
    const char* sub_path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(root_path, "%s"),
        LONGTAIL_LOGFIELD(sub_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, root_path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return 0)

    size_t root_len = strlen(root_path);
    if (root_len == 0)
    {
        return Longtail_Strdup(sub_path);
    }
    if (root_path[root_len - 1] == '/')
    {
        --root_len;
    }
    size_t sub_len = strlen(sub_path);
    size_t path_len = root_len + 1 + sub_len;
    char* path = (char*)Longtail_Alloc(path_len + 1);
    if (!path)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    memcpy(path, root_path, root_len);
    path[root_len] = '/';
    strcpy(&path[root_len + 1], sub_path);
    return path;
}

static int BlockStoreStorageAPI_IsDir(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)

    size_t path_len = strlen(path);
    if (path_len == 0)
    {
        return 0;
    }
    if (path[path_len- 1] == '/')
    {
        return 1;
    }
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, path, &path_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
        return err;
    }

    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        // Its a file since all paths in version index ends with forward-slash
        return 0;
    }

    char* tmp_path = (char*)alloca(path_len + 1 + 1);
    strcpy(tmp_path, path);
    tmp_path[path_len] = '/';
    tmp_path[path_len + 1] = 0;
    path_hash = 0;
    err = Longtail_GetPathHash(block_store_fs->m_HashAPI, tmp_path, &path_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
        return err;
    }

    path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return 0;
    }
    return 1;
}

static int BlockStoreStorageAPI_IsFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)

    size_t path_len = strlen(path);
    if (path_len == 0)
    {
        return 0;
    }
    if (path[path_len- 1] == '/')
    {
        return 0;
    }
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, path, &path_hash);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
        return err;
    }

    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return 0;
    }
    return 1;
}

static int BlockStoreStorageAPI_RemoveDir(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

static int BlockStoreStorageAPI_RemoveFile(
    struct Longtail_StorageAPI* storage_api,
    const char* path)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Unsupported, failed with %d", ENOTSUP)
    return ENOTSUP;
}

struct BlockStoreStorageAPI_Iterator
{
    char* m_TempPath;
    uint32_t m_PathEntryOffsetEnd;
    uint32_t m_PathEntryOffset;
};

static int BlockStoreStorageAPI_StartFind(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    Longtail_StorageAPI_HIterator* out_iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_iterator != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    size_t path_len = strlen(path);
    TLongtail_Hash path_hash = 0;
    if (path_len > 0)
    {
        if (path[path_len - 1] == '/')
        {
            int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, path, &path_hash);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
                return err;
            }
        }
        else
        {
            char* tmp_path = (char*)alloca(path_len + 2);
            memcpy(tmp_path, path, path_len);
            tmp_path[path_len] = '/';
            tmp_path[path_len + 1] = 0;
            int err = Longtail_GetPathHash(block_store_fs->m_HashAPI, tmp_path, &path_hash);
            if (err)
            {
                LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_GetPathHash() failed with %d", err)
                return err;
            }
        }
    }
    uint64_t* path_entry_index_ptr = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (!path_entry_index_ptr)
    {
        return ENOENT;
    }
    struct BlockStoreStorageAPI_PathEntry* p = &block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index_ptr];
    if (p->m_ChildCount == 0)
    {
        return ENOENT;
    }
    struct BlockStoreStorageAPI_Iterator* path_iterator = (struct BlockStoreStorageAPI_Iterator*)Longtail_Alloc(sizeof(struct BlockStoreStorageAPI_Iterator));
    if (path_iterator == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return ENOMEM;
    }
    path_iterator->m_TempPath = 0;
    path_iterator->m_PathEntryOffset = p->m_ChildStartIndex;
    path_iterator->m_PathEntryOffsetEnd = p->m_ChildStartIndex + p->m_ChildCount;
    *out_iterator = (Longtail_StorageAPI_HIterator)path_iterator;
    return 0;
}

static int BlockStoreStorageAPI_FindNext(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return 0)

    struct BlockStoreStorageAPI_Iterator* path_iterator = (struct BlockStoreStorageAPI_Iterator*)iterator;
    if (path_iterator->m_TempPath)
    {
        Longtail_Free(path_iterator->m_TempPath);
        path_iterator->m_TempPath = 0;
    }
    if (path_iterator->m_PathEntryOffset == path_iterator->m_PathEntryOffsetEnd)
    {
        return EINVAL;
    }
    ++path_iterator->m_PathEntryOffset;
    if (path_iterator->m_PathEntryOffset == path_iterator->m_PathEntryOffsetEnd)
    {
        return ENOENT;
    }
    return 0;
}

static void BlockStoreStorageAPI_CloseFind(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return)
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return)
    struct BlockStoreStorageAPI_Iterator* path_iterator = (struct BlockStoreStorageAPI_Iterator*)iterator;
    Longtail_Free(path_iterator);
}

static int BlockStoreStorageAPI_GetEntryProperties(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator,
    struct Longtail_StorageAPI_EntryProperties* out_properties)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(iterator, "%p"),
        LONGTAIL_LOGFIELD(out_properties, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, iterator != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_properties != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_Iterator* path_iterator = (struct BlockStoreStorageAPI_Iterator*)iterator;
    if (path_iterator->m_TempPath)
    {
        Longtail_Free(path_iterator->m_TempPath);
        path_iterator->m_TempPath = 0;
    }
    struct BlockStoreStorageAPI_PathEntry* path_entry = &block_store_fs->m_PathLookup->m_PathEntries[path_iterator->m_PathEntryOffset];
    size_t name_length = strlen(path_entry->m_Name);
    int is_dir = ((name_length > 0) && (path_entry->m_Name[name_length - 1] == '/')) ? 1 : 0;
    path_iterator->m_TempPath = Longtail_Strdup(path_entry->m_Name);
    if (is_dir)
    {
        path_iterator->m_TempPath[name_length - 1] = '\0';
    }

    out_properties->m_Name = path_iterator->m_TempPath;
    out_properties->m_IsDir = is_dir;
    out_properties->m_Permissions = block_store_fs->m_VersionIndex->m_Permissions[path_entry->m_AssetIndex];
    out_properties->m_Size = block_store_fs->m_VersionIndex->m_AssetSizes[path_entry->m_AssetIndex];
    return 0;
}

static void BlockStoreStorageAPI_Dispose(struct Longtail_API* api)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)api;
    Longtail_Free(block_store_fs);
}

static int BlockStoreStorageAPI_Init(
    void* mem,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_BlockStoreAPI* block_store,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_VersionIndex* version_index,
    struct Longtail_StorageAPI** out_storage_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(block_store, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p"),
        LONGTAIL_LOGFIELD(out_storage_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, block_store != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, out_storage_api != 0, return 0)

    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)mem;
    uint64_t content_index_chunk_count = *content_index->m_ChunkCount;
    uint32_t version_index_asset_count = *version_index->m_AssetCount;

    block_store_fs->m_API.m_API.Dispose = BlockStoreStorageAPI_Dispose;
    block_store_fs->m_API.OpenReadFile = BlockStoreStorageAPI_OpenReadFile;
    block_store_fs->m_API.GetSize = BlockStoreStorageAPI_GetSize;
    block_store_fs->m_API.Read = BlockStoreStorageAPI_Read;
    block_store_fs->m_API.OpenWriteFile = BlockStoreStorageAPI_OpenWriteFile;
    block_store_fs->m_API.Write = BlockStoreStorageAPI_Write;
    block_store_fs->m_API.SetSize = BlockStoreStorageAPI_SetSize;
    block_store_fs->m_API.SetPermissions = BlockStoreStorageAPI_SetPermissions;
    block_store_fs->m_API.GetPermissions = BlockStoreStorageAPI_GetPermissions;
    block_store_fs->m_API.CloseFile = BlockStoreStorageAPI_CloseFile;
    block_store_fs->m_API.CreateDir = BlockStoreStorageAPI_CreateDir;
    block_store_fs->m_API.RenameFile = BlockStoreStorageAPI_RenameFile;
    block_store_fs->m_API.ConcatPath = BlockStoreStorageAPI_ConcatPath;
    block_store_fs->m_API.IsDir = BlockStoreStorageAPI_IsDir;
    block_store_fs->m_API.IsFile = BlockStoreStorageAPI_IsFile;
    block_store_fs->m_API.RemoveDir = BlockStoreStorageAPI_RemoveDir;
    block_store_fs->m_API.RemoveFile = BlockStoreStorageAPI_RemoveFile;
    block_store_fs->m_API.StartFind = BlockStoreStorageAPI_StartFind;
    block_store_fs->m_API.FindNext = BlockStoreStorageAPI_FindNext;
    block_store_fs->m_API.CloseFind = BlockStoreStorageAPI_CloseFind;
    block_store_fs->m_API.GetEntryProperties = BlockStoreStorageAPI_GetEntryProperties;
    block_store_fs->m_HashAPI = hash_api;
    block_store_fs->m_JobAPI = job_api;
    block_store_fs->m_BlockStore = block_store;
    block_store_fs->m_ContentIndex = content_index;
    block_store_fs->m_VersionIndex = version_index;

    char* p = (char*)&block_store_fs[1];
    block_store_fs->m_ChunkHashToBlockIndexLookup = Longtail_LookupTable_Create(p, content_index_chunk_count, 0);
    p += Longtail_LookupTable_GetSize(content_index_chunk_count);
    block_store_fs->m_PathLookup = BlockStoreStorageAPI_CreatePathLookup(p, hash_api, version_index);
    p += GetPathEntriesSize(version_index_asset_count);
    block_store_fs->m_ChunkAssetOffsets = (uint64_t*)p;

    const uint64_t* content_index_chunk_block_indexes = content_index->m_ChunkBlockIndexes;
    const TLongtail_Hash* content_index_chunk_hashes = content_index->m_ChunkHashes;
    for (uint64_t c = 0; c < content_index_chunk_count; ++c)
    {
        uint64_t block_index = content_index_chunk_block_indexes[c];
        Longtail_LookupTable_Put(block_store_fs->m_ChunkHashToBlockIndexLookup, content_index_chunk_hashes[c], block_index);
    }
    const uint32_t* asset_chunk_index_starts = block_store_fs->m_VersionIndex->m_AssetChunkIndexStarts;
    const uint32_t* asset_chunk_indexes = block_store_fs->m_VersionIndex->m_AssetChunkIndexes;
    const uint32_t* version_chunk_sizes = block_store_fs->m_VersionIndex->m_ChunkSizes;
    uint64_t* version_chunk_asset_offsets = block_store_fs->m_ChunkAssetOffsets;
    for (uint32_t a = 0; a < version_index_asset_count; ++a)
    {
        uint32_t version_chunk_count = block_store_fs->m_VersionIndex->m_AssetChunkCounts[a];
        if (version_chunk_count == 0)
        {
            continue;
        }
        uint32_t chunk_start_index = asset_chunk_index_starts[a];
        const uint32_t* chunk_indexes = &asset_chunk_indexes[chunk_start_index];
        uint64_t* chunk_asset_offsets = &version_chunk_asset_offsets[chunk_start_index];
        uint64_t offset = 0;
        for (uint32_t c = 0; c < version_chunk_count; ++c)
        {
            uint32_t chunk_index = chunk_indexes[c];
            uint32_t chunk_size = version_chunk_sizes[chunk_index];
            chunk_asset_offsets[c] = offset;
            offset += chunk_size;
        }
    }

    *out_storage_api = &block_store_fs->m_API;
    return 0;
}

struct Longtail_StorageAPI* Longtail_CreateBlockStoreStorageAPI(
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_BlockStoreAPI* block_store,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_VersionIndex* version_index)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_api, "%p"),
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(block_store, "%p"),
        LONGTAIL_LOGFIELD(content_index, "%p"),
        LONGTAIL_LOGFIELD(version_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, hash_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, job_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, block_store != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, content_index != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, version_index != 0, return 0)

    size_t api_size = sizeof(struct BlockStoreStorageAPI) + 
        Longtail_LookupTable_GetSize(*content_index->m_ChunkCount) +
        GetPathEntriesSize(*version_index->m_AssetCount) +
        sizeof(uint64_t) * (*version_index->m_AssetChunkIndexCount);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_StorageAPI* storage_api;
    int err = BlockStoreStorageAPI_Init(
        mem,
        hash_api,
        job_api,
        block_store,
        content_index,
        version_index,
        &storage_api);

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "BlockStoreStorageAPI_Init() failed with %d", err)
        Longtail_Free(mem);
        return 0;
    }

    return storage_api;
}
