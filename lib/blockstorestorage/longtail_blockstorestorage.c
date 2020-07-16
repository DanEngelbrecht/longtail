#include "longtail_blockstorestorage.h"

#include "../../src/ext/stb_ds.h"

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

struct PathEntry
{
    const char* m_Path;
    TLongtail_Hash m_ParentHash;
    uint32_t m_AssetIndex;
    uint32_t m_ChildCount;
    uint32_t m_ChildStartIndex;
};

static SORTFUNC(PathEntryParentPathCompare)
{
    struct PathLookup* path_lookup = (struct PathLookup*)context;
    struct PathEntry* a = (struct PathEntry*)a_ptr;
    struct PathEntry* b = (struct PathEntry*)b_ptr;
    if (a->m_ParentHash < b->m_ParentHash)
    {
        return -1;
    }
    else if (a->m_ParentHash > b->m_ParentHash)
    {
        return 1;
    }
    size_t a_length = strlen(a->m_Path);
    size_t b_length = strlen(b->m_Path);
    int a_is_dir = a_length == 0 || (a->m_Path[a_length - 1] == '/');
    int b_is_dir = b_length == 0 || (b->m_Path[b_length - 1] == '/');
    if (a_is_dir != b_is_dir)
    {
        return a_is_dir ? -1 : 1;
    }
    return stricmp(a->m_Path, b->m_Path);
}

static TLongtail_Hash GetParentPathHash(struct Longtail_HashAPI* hash_api, const char* path)
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

struct PathLookup
{
    struct Longtail_VersionIndex* m_VersionIndex;
    struct PathEntry* m_PathEntries;
    struct Longtail_LookupTable* m_LookupTable;
};

size_t GetPathEntriesSize(uint32_t asset_count)
{
    size_t path_lookup_size = sizeof(struct PathLookup) +
        sizeof(struct PathEntry) * (asset_count + 1) +
        Longtail_LookupTable_GetSize(asset_count + 1);
    return path_lookup_size;
}

struct PathLookup* BuildPathEntires(
    void* mem,
    struct Longtail_HashAPI* hash_api,
    struct Longtail_VersionIndex* version_index)
{
    struct PathLookup* path_lookup = (struct PathLookup*)mem;

    uint32_t asset_count = *version_index->m_AssetCount;
    path_lookup->m_VersionIndex = version_index;
    path_lookup->m_PathEntries = (struct PathEntry*)&path_lookup[1];
    path_lookup->m_LookupTable = Longtail_LookupTable_Create(&path_lookup->m_PathEntries[asset_count + 1], asset_count + 1, 0);
    path_lookup->m_PathEntries[0].m_Path = "";
    path_lookup->m_PathEntries[0].m_ParentHash = 0;
    path_lookup->m_PathEntries[0].m_AssetIndex = 0;
    path_lookup->m_PathEntries[0].m_ChildCount = 0;
    path_lookup->m_PathEntries[0].m_ChildStartIndex = 0;
    for (uint32_t a = 0; a < asset_count; ++a)
    {
        const char* path = &version_index->m_NameData[version_index->m_NameOffsets[a]];
        struct PathEntry* path_entry = &path_lookup->m_PathEntries[a + 1];
        path_entry->m_Path = path;
        path_entry->m_ParentHash = GetParentPathHash(hash_api, path);
        path_entry->m_AssetIndex = a;
        path_entry->m_ChildCount = 0;
        path_entry->m_ChildStartIndex = 0;
    }
    QSORT(&path_lookup->m_PathEntries[1], asset_count, sizeof(struct PathEntry), PathEntryParentPathCompare, path_lookup);

    Longtail_LookupTable_Put(path_lookup->m_LookupTable, 0, 0);
    for (uint32_t a = 0; a < asset_count; ++a)
    {
        uint32_t asset_index = path_lookup->m_PathEntries[a + 1].m_AssetIndex;
        Longtail_LookupTable_Put(path_lookup->m_LookupTable, version_index->m_PathHashes[asset_index], a  + 1);
    }
    struct Longtail_LookupTable* find_first_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(asset_count + 1)), asset_count, 0);
    for (uint32_t p = 0; p < asset_count; ++p)
    {
        TLongtail_Hash parent_hash = path_lookup->m_PathEntries[p + 1].m_ParentHash;
        uint32_t parent_index = (uint32_t)*Longtail_LookupTable_Get(path_lookup->m_LookupTable, parent_hash);
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
    struct Longtail_HashAPI* hash_api;
    struct Longtail_JobAPI* job_api;
    struct Longtail_BlockStoreAPI* block_store;
    struct Longtail_ContentIndex* content_index;
    struct Longtail_VersionIndex* version_index;
    struct Longtail_LookupTable* chunk_hash_to_block_index_lookup;
//    struct Longtail_LookupTable* asset_lookup;
    struct PathLookup* m_PathLookup;
    uint64_t* chunk_asset_offsets;
};

struct BlockStoreStorageAPI_OpenFile
{
    uint32_t asset_index;
    uint32_t seek_chunk_offset;
    uint64_t seek_asset_pos;
};

struct BlockStoreStorageAPI_ChunkRange
{
    uint64_t asset_start_offset;
    uint32_t chunk_start;
    uint32_t chunk_end;
};

struct BlockStoreStorageAPI_BlockRange
{
    TLongtail_Hash key;
    struct BlockStoreStorageAPI_ChunkRange value;
};

static int BlockStoreStorageAPI_ReadFromBlock(
    struct Longtail_StoredBlock* stored_block,
    struct BlockStoreStorageAPI_ChunkRange range,
    struct BlockStoreStorageAPI* block_store_fs,
    struct BlockStoreStorageAPI_OpenFile* block_store_file,
    uint64_t start,
    uint64_t size,
    char* buffer,
    const uint32_t* chunk_indexes)
{
    uint64_t read_end = start + size;
    uint32_t chunk_block_offset = 0;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    size_t block_chunk_lookup_size = Longtail_LookupTable_GetSize(chunk_count);
    void* mem = Longtail_Alloc(block_chunk_lookup_size);
    if (mem == 0)
    {
        return ENOMEM;
    }
    struct Longtail_LookupTable* block_chunk_lookup = Longtail_LookupTable_Create(mem, chunk_count, 0);
    for (uint32_t c = 0; c < *stored_block->m_BlockIndex->m_ChunkCount; ++c)
    {
        TLongtail_Hash chunk_hash = stored_block->m_BlockIndex->m_ChunkHashes[c];
        Longtail_LookupTable_Put(block_chunk_lookup, chunk_hash, chunk_block_offset);
        chunk_block_offset += stored_block->m_BlockIndex->m_ChunkSizes[c];
    }
    uint64_t asset_offset = range.asset_start_offset;
    for (uint32_t c = range.chunk_start; c < range.chunk_end; ++c)
    {
        uint32_t chunk_index = chunk_indexes[c];
        TLongtail_Hash chunk_hash = block_store_fs->version_index->m_ChunkHashes[chunk_index];
        uint32_t chunk_size = block_store_fs->version_index->m_ChunkSizes[chunk_index];
        uint64_t asset_offset_chunk_end = asset_offset + chunk_size;
        LONGTAIL_FATAL_ASSERT(asset_offset_chunk_end >= start, return EINVAL)

        const char* block_data = (char*)stored_block->m_BlockData;

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
    Longtail_Free(block_chunk_lookup);
    return 0;
}

struct BlockStoreStorageAPI_ReadFromBlockJobData;

struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    uint32_t job_id;
    struct BlockStoreStorageAPI_ReadFromBlockJobData* data;
};


struct BlockStoreStorageAPI_ReadFromBlockJobData
{
	struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI on_read_block_complete_api;
    uint64_t block_index;
    struct BlockStoreStorageAPI_ChunkRange range;
    struct BlockStoreStorageAPI* block_store_fs;
    struct BlockStoreStorageAPI_OpenFile* block_store_file;
    uint64_t start;
    uint64_t size;
    char* buffer;
    const uint32_t* chunk_indexes;
    struct Longtail_StoredBlock* stored_block;
    int err;
};

static void BlockStoreStorageAPI_ReadBlock_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI* cb = (struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI*)async_complete_api;
    struct Longtail_JobAPI* job_api = cb->data->block_store_fs->job_api;
    cb->data->err = err;
    cb->data->stored_block = stored_block;
    job_api->ResumeJob(job_api, cb->job_id);
}

static int BlockStoreStorageAPI_ReadFromBlockJob(void* context, uint32_t job_id, int is_cancelled)
{
    struct BlockStoreStorageAPI_ReadFromBlockJobData* data = (struct BlockStoreStorageAPI_ReadFromBlockJobData*)context;

    if (!data->stored_block)
    {
        if (data->err)
        {
            return 0;
        }
        // Don't need dynamic alloc, could be part of struct BlockStoreStorageAPI_ReadFromBlockJobData since it covers the lifetime of struct BlockStoreStorageAPI_ReadBlock_OnComplete
        struct BlockStoreStorageAPI_ReadBlock_OnCompleteAPI* complete_cb = &data->on_read_block_complete_api;
        complete_cb->m_API.OnComplete = BlockStoreStorageAPI_ReadBlock_OnComplete;
        complete_cb->job_id = job_id;
        complete_cb->data = data;

        TLongtail_Hash block_hash = data->block_store_fs->content_index->m_BlockHashes[data->block_index];
        int err = data->block_store_fs->block_store->GetStoredBlock(data->block_store_fs->block_store, block_hash, &complete_cb->m_API);
        if (err)
        {
            data->err = err;
            return 0;
        }
        return EBUSY;
    }

    data->err = BlockStoreStorageAPI_ReadFromBlock(
        data->stored_block,
        data->range,
        data->block_store_fs,
        data->block_store_file,
        data->start,
        data->size,
        data->buffer,
        data->chunk_indexes);
    if (data->stored_block->Dispose)
    {
        data->stored_block->Dispose(data->stored_block);
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
    if (pos == block_store_file->seek_asset_pos)
    {
        return 0;
    }
    uint32_t chunk_count = block_store_fs->version_index->m_AssetChunkCounts[block_store_file->asset_index];
    if (chunk_count == 0)
    {
        return pos == 0 ? 0 : EIO;
    }
    uint32_t chunk_start_index = block_store_fs->version_index->m_AssetChunkIndexStarts[block_store_file->asset_index];

    const uint64_t* chunk_asset_offsets = &block_store_fs->chunk_asset_offsets[chunk_start_index];
    if (block_store_file->seek_chunk_offset < chunk_count)
    {
        if (pos >= chunk_asset_offsets[block_store_file->seek_chunk_offset])
        {
            if (block_store_file->seek_chunk_offset == chunk_count - 1)
            {
                // on last block
                return 0;
            }
            else if (pos < chunk_asset_offsets[block_store_file->seek_chunk_offset + 1])
            {
                return 0;
            }
        }
    }

    const uint32_t* chunk_indexes = &block_store_fs->version_index->m_AssetChunkIndexes[chunk_start_index];

    uint64_t asset_size = block_store_fs->version_index->m_AssetSizes[block_store_file->asset_index];

    uint32_t start_chunk_index = BlockStoreStorageAPI_FindStartChunk(chunk_asset_offsets, chunk_count, pos);
    uint64_t start_asset_offset = chunk_asset_offsets[start_chunk_index];

    block_store_file->seek_asset_pos = start_asset_offset;
    block_store_file->seek_chunk_offset = start_chunk_index;
    return 0;
}

static int BlockStoreStorageAPI_ReadFile(
    struct BlockStoreStorageAPI* block_store_fs,
    struct BlockStoreStorageAPI_OpenFile* block_store_file,
    uint64_t start,
    uint64_t size,
    void* out_buffer)
{
    char* buffer = (char*)out_buffer;

    uint32_t chunk_count = block_store_fs->version_index->m_AssetChunkCounts[block_store_file->asset_index];
    uint32_t chunk_start_index = block_store_fs->version_index->m_AssetChunkIndexStarts[block_store_file->asset_index];
    const uint32_t* chunk_indexes = &block_store_fs->version_index->m_AssetChunkIndexes[chunk_start_index];

    const uint64_t read_end = start + size;
    int err = BlockStoreStorageAPI_SeekFile(block_store_fs, block_store_file, start);
    if (err)
    {
        return err;
    }

    uint32_t seek_chunk_offset = block_store_file->seek_chunk_offset;
    uint64_t seek_asset_pos = block_store_file->seek_asset_pos;

    struct BlockStoreStorageAPI_BlockRange* block_range_map = 0;
    uint64_t* block_indexes = 0;

    uint64_t last_block_index = 0xfffffffffffffffful;
    uint64_t last_block_range_index_ptr = -1;

    for (uint32_t c = seek_chunk_offset; c < chunk_count; ++c)
    {
        uint32_t chunk_index = chunk_indexes[c];
        TLongtail_Hash chunk_hash = block_store_fs->version_index->m_ChunkHashes[chunk_index];
        uint64_t block_index = *Longtail_LookupTable_Get(block_store_fs->chunk_hash_to_block_index_lookup, chunk_hash);
        if ((last_block_index != 0xfffffffffffffffful) && (block_index == last_block_index))
        {
            block_range_map[last_block_range_index_ptr].value.chunk_end = c + 1;
        }
        else
        {
            last_block_range_index_ptr = hmgeti(block_range_map, block_index);
            if (last_block_range_index_ptr == -1)
            {
                struct BlockStoreStorageAPI_ChunkRange range = {seek_asset_pos, c, c + 1};
                hmput(block_range_map, block_index, range);
                arrput(block_indexes, block_index);
                last_block_range_index_ptr = hmgeti(block_range_map, block_index);
            }
            else
            {
                block_range_map[last_block_range_index_ptr].value.chunk_end = c + 1;
            }
            last_block_index = block_index;
        }
        block_store_file->seek_chunk_offset = c;
        block_store_file->seek_asset_pos = seek_asset_pos;

        uint32_t chunk_size = block_store_fs->version_index->m_ChunkSizes[chunk_index];
        seek_asset_pos += chunk_size;
        if (seek_asset_pos >= read_end)
        {
            break;
        }
    }

    uint32_t block_count = (uint32_t)arrlen(block_indexes);
    if (block_count == 0)
    {
        return 0;
    }

    struct Longtail_JobAPI* job_api = block_store_fs->job_api;

	size_t work_mem_size = sizeof(struct BlockStoreStorageAPI_ReadFromBlockJobData) * block_count +
		sizeof(Longtail_JobAPI_JobFunc) * block_count +
		sizeof(void*) * block_count;
	void* work_mem = Longtail_Alloc(work_mem_size);
	if (!work_mem)
	{
		return ENOMEM;
	}
    struct BlockStoreStorageAPI_ReadFromBlockJobData* job_datas = (struct BlockStoreStorageAPI_ReadFromBlockJobData*)work_mem;
    Longtail_JobAPI_JobFunc* funcs = (Longtail_JobAPI_JobFunc*)&job_datas[block_count];
    void** ctxs = (void**)&funcs[block_count];

    Longtail_JobAPI_Group job_group;
    err = job_api->ReserveJobs(job_api, block_count, &job_group);
    LONGTAIL_FATAL_ASSERT(err == 0, return err)

    for (uint32_t b = 0; b < block_count; ++b)
    {
        uint64_t block_index = block_indexes[b];
        struct BlockStoreStorageAPI_ChunkRange range = hmget(block_range_map, block_index);

        job_datas[b].block_index = block_index;
        job_datas[b].range = range;
        job_datas[b].block_store_fs = block_store_fs;
        job_datas[b].block_store_file = block_store_file;
        job_datas[b].start = start;
        job_datas[b].size = size;
        job_datas[b].buffer = buffer;
        job_datas[b].chunk_indexes = chunk_indexes;
        job_datas[b].stored_block = 0;
        job_datas[b].err = 0;

        funcs[b] = BlockStoreStorageAPI_ReadFromBlockJob;
        ctxs[b] = &job_datas[b];
    }
    Longtail_JobAPI_Jobs jobs;
    err = job_api->CreateJobs(job_api, job_group, block_count, funcs, ctxs, &jobs);
    LONGTAIL_FATAL_ASSERT(err == 0, return err)
    err = job_api->ReadyJobs(job_api, block_count, jobs);
    LONGTAIL_FATAL_ASSERT(err == 0, return err)
    err = job_api->WaitForAllJobs(job_api, job_group, 0, 0, 0);
    LONGTAIL_FATAL_ASSERT(err == 0, return err)
    Longtail_Free(work_mem);

    hmfree(block_range_map);
    arrfree(block_indexes);
    return 0;
}

static int BlockStoreStorageAPI_OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;

    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
    if (err)
    {
        return err;
    }
    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return ENOENT;
    }
    uint32_t asset_index = block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index].m_AssetIndex;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)Longtail_Alloc(sizeof(struct BlockStoreStorageAPI_OpenFile));
    block_store_file->asset_index = asset_index;
    block_store_file->seek_chunk_offset = 0;
    block_store_file->seek_asset_pos = 0;
    *out_open_file = (Longtail_StorageAPI_HOpenFile)block_store_file;
    return 0;
}

static int BlockStoreStorageAPI_GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    *out_size = block_store_fs->version_index->m_AssetSizes[block_store_file->asset_index];
    return 0;
}

static int BlockStoreStorageAPI_Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    int err = BlockStoreStorageAPI_ReadFile(block_store_fs, block_store_file, offset, length, output);
    return err;
}

static int BlockStoreStorageAPI_OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_GetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t* out_permissions)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
    if (err)
    {
        return err;
    }
    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return ENOENT;
    }
    uint32_t asset_index = block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index].m_AssetIndex;
    *out_permissions = block_store_fs->version_index->m_Permissions[asset_index];
    return 0;
}

static void BlockStoreStorageAPI_CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    struct BlockStoreStorageAPI_OpenFile* block_store_file = (struct BlockStoreStorageAPI_OpenFile*)f;
    Longtail_Free(block_store_file);
}

static int BlockStoreStorageAPI_CreateDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path)
{
    return ENOTSUP;
}

static char* BlockStoreStorageAPI_ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path)
{
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
        return 0;
    }
    memcpy(path, root_path, root_len);
    path[root_len] = '/';
    memcpy(&path[root_len + 1], sub_path, sub_len);
    return path;
}

static int BlockStoreStorageAPI_IsDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    size_t path_len = strlen(path);
    if (path_len == 0)
    {
        return 0;
    }
    if (path[strlen(path)- 1] == '/')
    {
        return 1;
    }
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
    if (err)
    {
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
    err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
    if (err)
    {
        return err;
    }
    path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return 0;
    }
    return 1;
}

static int BlockStoreStorageAPI_IsFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    size_t path_len = strlen(path);
    if (path_len == 0)
    {
        return 0;
    }
    if (path[strlen(path)- 1] == '/')
    {
        return 0;
    }
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    uint64_t path_hash = 0;
    int err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
    if (err)
    {
        return err;
    }
    uint64_t* path_entry_index = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (path_entry_index == 0)
    {
        return 0;
    }
    return 1;
}

static int BlockStoreStorageAPI_RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path)
{
    return ENOTSUP;
}

static int BlockStoreStorageAPI_RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path)
{
    return ENOTSUP;
}

struct PathIterator
{
    struct BlockStoreStorageAPI* block_store_fs;
    uint32_t m_PathEntryOffsetEnd;
    uint32_t m_PathEntryOffset;
};

static int BlockStoreStorageAPI_StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator)
{
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)storage_api;
    size_t path_len = strlen(path);
    TLongtail_Hash path_hash = 0;
    if (path_len > 0)
    {
        if (path[path_len -1 ] == '/')
        {
            int err = Longtail_GetPathHash(block_store_fs->hash_api, path, &path_hash);
            if (err)
            {
                return err;
            }
        }
        else
        {
            char* tmp_path = (char*)alloca(path_len + 2);
            memcpy(tmp_path, path, path_len);
            tmp_path[path_len] = '/';
            tmp_path[path_len + 1] = 0;
            int err = Longtail_GetPathHash(block_store_fs->hash_api, tmp_path, &path_hash);
            if (err)
            {
                return err;
            }
        }
    }
    uint64_t* path_entry_index_ptr = Longtail_LookupTable_Get(block_store_fs->m_PathLookup->m_LookupTable, path_hash);
    if (!path_entry_index_ptr)
    {
        return ENOENT;
    }
    struct PathEntry* p = &block_store_fs->m_PathLookup->m_PathEntries[*path_entry_index_ptr];
    if (p->m_ChildCount == 0)
    {
        return 0;
    }
    struct PathIterator* path_iterator = (struct PathIterator*)Longtail_Alloc(sizeof(struct PathIterator));
    if (path_iterator == 0)
    {
        return ENOMEM;
    }
    path_iterator->block_store_fs = block_store_fs;
    path_iterator->m_PathEntryOffset = p->m_ChildStartIndex;
    path_iterator->m_PathEntryOffsetEnd = p->m_ChildStartIndex + p->m_ChildCount;
    *out_iterator = (Longtail_StorageAPI_HIterator)path_iterator;
    return 0;
}

static int BlockStoreStorageAPI_FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    struct PathIterator* path_iterator = (struct PathIterator*)iterator;
    ++path_iterator->m_PathEntryOffset;
    if (path_iterator->m_PathEntryOffset == path_iterator->m_PathEntryOffsetEnd)
    {
        return ENOENT;
    }
    return 0;
}

static void BlockStoreStorageAPI_CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator)
{
    struct PathIterator* path_iterator = (struct PathIterator*)iterator;
    Longtail_Free(path_iterator);
}

static int BlockStoreStorageAPI_GetEntryProperties(
    struct Longtail_StorageAPI* storage_api,
    Longtail_StorageAPI_HIterator iterator,
    struct Longtail_StorageAPI_EntryProperties* out_properties)
{
    struct PathIterator* path_iterator = (struct PathIterator*)iterator;
    struct PathEntry* path_entry = &path_iterator->block_store_fs->m_PathLookup->m_PathEntries[path_iterator->m_PathEntryOffset];
    const char* name_start = path_entry->m_Path;
    size_t search_pos = 0;
    int is_dir = 0;
    while (path_entry->m_Path[search_pos] != '\0')
    {
        if (path_entry->m_Path[search_pos] == '/')
        {
            if (path_entry->m_Path[search_pos + 1] == '\0')
            {
                is_dir = 1;
                break;
            }
            ++search_pos;
            name_start = &path_entry->m_Path[search_pos];
        }
        ++search_pos;
    }

    out_properties->m_Name = name_start;
    out_properties->m_IsDir = is_dir;
    out_properties->m_Permissions = path_iterator->block_store_fs->version_index->m_Permissions[path_entry->m_AssetIndex];
    out_properties->m_Size = path_iterator->block_store_fs->version_index->m_AssetSizes[path_entry->m_AssetIndex];
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
    struct BlockStoreStorageAPI* block_store_fs = (struct BlockStoreStorageAPI*)mem;

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
    block_store_fs->hash_api = hash_api;
    block_store_fs->job_api = job_api;
    block_store_fs->block_store = block_store;
    block_store_fs->content_index = content_index;
    block_store_fs->version_index = version_index;

    char* p = (char*)&block_store_fs[1];
    block_store_fs->chunk_hash_to_block_index_lookup = Longtail_LookupTable_Create(p, *content_index->m_ChunkCount, 0);
    p += Longtail_LookupTable_GetSize(*content_index->m_ChunkCount);
    block_store_fs->m_PathLookup = BuildPathEntires(p, hash_api, version_index);
    p += GetPathEntriesSize(*version_index->m_AssetCount);
    block_store_fs->chunk_asset_offsets = (uint64_t*)p;

    for (uint64_t c = 0; c < *content_index->m_ChunkCount; ++c)
    {
        uint64_t block_index = content_index->m_ChunkBlockIndexes[c];
        Longtail_LookupTable_Put(block_store_fs->chunk_hash_to_block_index_lookup, content_index->m_ChunkHashes[c], block_index);
    }
    for (uint32_t a = 0; a < *version_index->m_AssetCount; ++a)
    {
        uint32_t chunk_count = block_store_fs->version_index->m_AssetChunkCounts[a];
        if (chunk_count == 0)
        {
            continue;
        }
        uint32_t chunk_start_index = block_store_fs->version_index->m_AssetChunkIndexStarts[a];
        const uint32_t* chunk_indexes = &block_store_fs->version_index->m_AssetChunkIndexes[chunk_start_index];
        uint64_t* chunk_asset_offsets = &block_store_fs->chunk_asset_offsets[chunk_start_index];
        uint64_t offset = 0;
        for (uint32_t c = 0; c < chunk_count; ++c)
        {
            uint32_t chunk_index = chunk_indexes[c];
            uint32_t chunk_size = block_store_fs->version_index->m_ChunkSizes[chunk_index];
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateBlockStoreStorageAPI(%p, %p, %p, %p, %p,) failed with %d",
        hash_api, job_api, block_store, content_index, version_index)
    LONGTAIL_VALIDATE_INPUT(hash_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(job_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(block_store != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(content_index != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(version_index != 0, return 0)

    size_t api_size = sizeof(struct BlockStoreStorageAPI) + 
        Longtail_LookupTable_GetSize(*content_index->m_ChunkCount) +
        GetPathEntriesSize(*version_index->m_AssetCount) +
        sizeof(uint64_t) * (*version_index->m_AssetChunkIndexCount);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateBlockStoreStorageAPI(%p, %p, %p, %p, %p,) failed with %d",
            hash_api, job_api, block_store, content_index, version_index,
            ENOMEM)
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateBlockStoreStorageAPI(%p, %p, %p, %p, %p,) failed with %d",
            hash_api, job_api, block_store, content_index, version_index,
            err)
        Longtail_Free(mem);
        return 0;
    }
    return storage_api;
}
