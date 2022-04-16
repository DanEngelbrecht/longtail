#include "longtail_archiveblockstore.h"

#include "../longtail_platform.h"
#include <errno.h>
#include <inttypes.h>
#include <string.h>

#define MEASURE_ACCESS  1

struct ArchiveBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    TLongtail_Atomic64 m_StatU64[Longtail_BlockStoreAPI_StatU64_Count];

    HLongtail_SpinLock m_Lock;
    uint64_t m_BlockDataOffset;
    Longtail_StorageAPI_HOpenFile m_ArchiveFileHandle;
#if LONGTAIL_ENABLE_MMAPED_FILES
    Longtail_StorageAPI_HFileMap m_ArchiveFileMapping;
    uint64_t m_BlockBytesSize;
    const void* m_BlockBytes;
#endif

    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_ArchiveIndex* m_ArchiveIndex;
    char* m_ArchivePath;
    int m_IsWriteMode;
    struct Longtail_LookupTable* m_BlockIndexLookup;

#if MEASURE_ACCESS
    struct Longtail_LookupTable* m_BlockAccessLookup;
#endif
};

static int ArchiveBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(stored_block, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    LONGTAIL_FATAL_ASSERT(ctx, api->m_ArchiveFileHandle != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, api->m_IsWriteMode == 1, return EINVAL)

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    uint32_t* block_index_ptr = Longtail_LookupTable_Get(api->m_BlockIndexLookup, *stored_block->m_BlockIndex->m_BlockHash);
    if (!block_index_ptr)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", ENOENT)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return ENOENT;
    }
    int block_index = *block_index_ptr;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);

    Longtail_LockSpinLock(api->m_Lock);

    api->m_ArchiveIndex->m_BlockStartOffets[block_index] = api->m_BlockDataOffset;
    api->m_ArchiveIndex->m_BlockSizes[block_index] = block_index_data_size + stored_block->m_BlockChunksDataSize;
    api->m_BlockDataOffset += api->m_ArchiveIndex->m_BlockSizes[block_index];
    uint64_t write_pos = api->m_BlockDataOffset + *api->m_ArchiveIndex->m_IndexDataSize;

    Longtail_UnlockSpinLock(api->m_Lock);

    int err = api->m_StorageAPI->Write(api->m_StorageAPI, api->m_ArchiveFileHandle, write_pos, block_index_data_size, &stored_block->m_BlockIndex[1]);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return err;
    }
    write_pos += block_index_data_size;

    err = api->m_StorageAPI->Write(api->m_StorageAPI, api->m_ArchiveFileHandle, write_pos, stored_block->m_BlockChunksDataSize, stored_block->m_BlockData);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Write() failed with %d", err)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1);
        return err;
    }

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1);

    async_complete_api->OnComplete(async_complete_api, 0);

    return 0;
}

static int ArchiveBlockStore_PreflightGet(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_count,
    const TLongtail_Hash* block_hashes,
    struct Longtail_AsyncPreflightStartedAPI* optional_async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_count, "%u"),
        LONGTAIL_LOGFIELD(block_hashes, "%p"),
        LONGTAIL_LOGFIELD(optional_async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count], 1);

    if (optional_async_complete_api)
    {
        optional_async_complete_api->OnComplete(optional_async_complete_api, 0, 0, 0);
    }

    return 0;
}

static int ArchiveBlockStore_StoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(stored_block, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, stored_block, return EINVAL)

//    memset(stored_block, (int)Longtail_GetStoredBlockSize(0), 0xff);
    Longtail_Free(stored_block);
    return 0;
}

static int ArchiveBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_hash, "%" PRIx64),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    if (api->m_IsWriteMode != 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ArchiveBlockStore_GetStoredBlock() can't read from a write store, failed with %d", EINVAL)
        Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
        return EINVAL;
    }

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1);

    uint32_t* block_index_ptr = Longtail_LookupTable_Get(api->m_BlockIndexLookup, block_hash);
    if (!block_index_ptr)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_LookupTable_Get() failed with %d", ENOENT)
        return ENOENT;
    }
    int block_index = *block_index_ptr;
    uint64_t block_offset = api->m_ArchiveIndex->m_BlockStartOffets[block_index];
    uint32_t block_size = api->m_ArchiveIndex->m_BlockSizes[block_index];


#if MEASURE_ACCESS
    Longtail_LockSpinLock(api->m_Lock);
    uint32_t* count_ptr = Longtail_LookupTable_PutUnique(api->m_BlockAccessLookup, block_hash, 1);
    if (count_ptr)
    {
        (*count_ptr)++;
    }
    Longtail_UnlockSpinLock(api->m_Lock);
#endif

    struct Longtail_StoredBlock* stored_block = 0;
#if LONGTAIL_ENABLE_MMAPED_FILES
    if (api->m_BlockBytes)
    {
        size_t block_mem_size = Longtail_GetStoredBlockSize(0);
        stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("ArchiveBlockStore_GetStoredBlock", block_mem_size);
        if (!stored_block)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            return ENOMEM;
        }
        void* block_data = &((uint8_t*)api->m_BlockBytes)[block_offset];
        int err = Longtail_InitStoredBlockFromData(
            stored_block,
            block_data,
            block_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData() failed with %d", err)
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free(stored_block);
            return err;
        }
    }
    else
#endif
    {
        uint64_t read_offset = (*api->m_ArchiveIndex->m_IndexDataSize) + block_offset;
        uint64_t stored_block_data_size = block_size;
        size_t block_mem_size = Longtail_GetStoredBlockSize(stored_block_data_size);
        stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc("ArchiveBlockStore_GetStoredBlock", block_mem_size);
        if (!stored_block)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            return ENOMEM;
        }
        void* block_data = &((uint8_t*)stored_block)[block_mem_size - stored_block_data_size];

        int err = api->m_StorageAPI->Read(api->m_StorageAPI, api->m_ArchiveFileHandle, read_offset, stored_block_data_size, block_data);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "storage_api->Read() failed with %d", err)
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free(stored_block);
            return err;
        }
        err = Longtail_InitStoredBlockFromData(
            stored_block,
            block_data,
            stored_block_data_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_InitStoredBlockFromData() failed with %d", err)
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1);
            Longtail_Free(stored_block);
            return err;
        }
    }

    stored_block->Dispose = ArchiveBlockStore_StoredBlock_Dispose;

    LONGTAIL_FATAL_ASSERT(ctx, stored_block->m_BlockChunksDataSize < block_size, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, *stored_block->m_BlockIndex->m_ChunkCount <= 1024, return EINVAL);

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    async_complete_api->OnComplete(async_complete_api, stored_block, 0);

    return 0;
}

static int ArchiveBlockStore_GetExistingContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t chunk_count,
    const TLongtail_Hash* chunk_hashes,
    uint32_t min_block_usage_percent,
    struct Longtail_AsyncGetExistingContentAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(chunk_count, "%u"),
        LONGTAIL_LOGFIELD(chunk_hashes, "%p"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (chunk_count == 0) || (chunk_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count], 1);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount], 1);

    return ENOTSUP;
}

static int ArchiveBlockStore_PruneBlocks(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t block_keep_count,
    const TLongtail_Hash* block_keep_hashes,
    struct Longtail_AsyncPruneBlocksAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(block_keep_count, "%u"),
        LONGTAIL_LOGFIELD(block_keep_hashes, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, (block_keep_count == 0) || (block_keep_hashes != 0), return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, async_complete_api, return EINVAL)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_Count], 1);
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount], 1);

    return ENOTSUP;
}

static int ArchiveBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(out_stats, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_stats, return EINVAL)
    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStats_Count], 1);
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        out_stats->m_StatU64[s] = api->m_StatU64[s];
    }
    return 0;
}

static int ArchiveBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p"),
        LONGTAIL_LOGFIELD(async_complete_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, block_store_api, return EINVAL)

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_Count], 1);

    if (api->m_IsWriteMode)
    {
        Longtail_LockSpinLock(api->m_Lock);
        int err = api->m_StorageAPI->Write(api->m_StorageAPI, api->m_ArchiveFileHandle, 0, *api->m_ArchiveIndex->m_IndexDataSize, &api->m_ArchiveIndex[1]);
        Longtail_UnlockSpinLock(api->m_Lock);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_StorageAPI->Write() failed with %d", err);
            Longtail_AtomicAdd64(&api->m_StatU64[Longtail_BlockStoreAPI_StatU64_Flush_FailCount], 1);
            return err;
        }
    }

    if (async_complete_api)
    {
        async_complete_api->OnComplete(async_complete_api, 0);
    }

    return 0;
}

static void ArchiveBlockStore_Dispose(struct Longtail_API* block_store_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(block_store_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, block_store_api, return)
    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;

    int err = ArchiveBlockStore_Flush(&api->m_BlockStoreAPI, 0);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_Flush() failed with %d", err);
    }

    Longtail_LockSpinLock(api->m_Lock);
#if MEASURE_ACCESS
    uint32_t total_count = 0;
    uint32_t worst_offender = 0;
    for (uint32_t b = 0; b < *api->m_ArchiveIndex->m_StoreIndex.m_BlockCount; ++b)
    {
        TLongtail_Hash block_hash = api->m_ArchiveIndex->m_StoreIndex.m_BlockHashes[b];
        const uint32_t* count_ptr = Longtail_LookupTable_Get(api->m_BlockAccessLookup, block_hash);
        if (count_ptr)
        {
            total_count += *count_ptr;
            if (*count_ptr > worst_offender)
            {
                worst_offender = *count_ptr;
            }
        }
    }
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "ArchiveBlockStore_Dispose() count %u, fetch_count %u, overfetch %u, worst %u", *api->m_ArchiveIndex->m_StoreIndex.m_BlockCount, total_count, total_count - *api->m_ArchiveIndex->m_StoreIndex.m_BlockCount, worst_offender);
#endif
#if LONGTAIL_ENABLE_MMAPED_FILES
    if (api->m_BlockBytes)
    {
        api->m_StorageAPI->UnMapFile(api->m_StorageAPI, api->m_ArchiveFileMapping, api->m_BlockBytes, api->m_BlockBytesSize);
        api->m_ArchiveFileMapping = 0;
    }
#endif
    api->m_StorageAPI->CloseFile(api->m_StorageAPI, api->m_ArchiveFileHandle);
    api->m_ArchiveFileHandle = 0;
    Longtail_UnlockSpinLock(api->m_Lock);

    Longtail_DeleteSpinLock(api->m_Lock);
    Longtail_Free(api->m_Lock);
    Longtail_Free(api->m_ArchivePath);
    Longtail_Free(api);
}

static int ArchiveBlockStore_Init(
    void* mem,
    struct Longtail_StorageAPI* storage_api,
    const char* archive_path,
    struct Longtail_ArchiveIndex* archive_index,
    int enable_write,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(enable_write, "%d"),
        LONGTAIL_LOGFIELD(out_block_store_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, storage_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, archive_path, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, enable_write == 0 || enable_write == 1, return 0)
    LONGTAIL_FATAL_ASSERT(ctx, out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        ArchiveBlockStore_Dispose,
        ArchiveBlockStore_PutStoredBlock,
        ArchiveBlockStore_PreflightGet,
        ArchiveBlockStore_GetStoredBlock,
        ArchiveBlockStore_GetExistingContent,
        ArchiveBlockStore_PruneBlocks,
        ArchiveBlockStore_GetStats,
        ArchiveBlockStore_Flush);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct ArchiveBlockStoreAPI* api = (struct ArchiveBlockStoreAPI*)block_store_api;

    api->m_StorageAPI = storage_api;
    api->m_BlockDataOffset = 0;
    api->m_ArchiveIndex = archive_index;
    api->m_ArchivePath = Longtail_Strdup(archive_path);
    api->m_IsWriteMode = enable_write;
#if LONGTAIL_ENABLE_MMAPED_FILES
    api->m_ArchiveFileMapping = 0;
    api->m_BlockBytesSize = 0;
    api->m_BlockBytes = 0;
#endif
    char* p = (char*)&api[1];
    api->m_BlockIndexLookup = Longtail_LookupTable_Create(p, *archive_index->m_StoreIndex.m_BlockCount, 0);
#if MEASURE_ACCESS
    p += Longtail_LookupTable_GetSize(*archive_index->m_StoreIndex.m_BlockCount);
    api->m_BlockAccessLookup = Longtail_LookupTable_Create(p, *archive_index->m_StoreIndex.m_BlockCount, 0);
#endif

    for (uint32_t s = 0; s < Longtail_BlockStoreAPI_StatU64_Count; ++s)
    {
        api->m_StatU64[s] = 0;
    }

    for (uint32_t b = 0; b < *archive_index->m_StoreIndex.m_BlockCount; ++b)
    {
        Longtail_LookupTable_Put(api->m_BlockIndexLookup, archive_index->m_StoreIndex.m_BlockHashes[b], b);
    }

    if (enable_write)
    {
        int err = EnsureParentPathExists(api->m_StorageAPI, api->m_ArchivePath);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "EnsureParentPathExists() failed with %d", err)
            Longtail_Free(api->m_ArchivePath);
            return err;
        }
        err = api->m_StorageAPI->OpenWriteFile(api->m_StorageAPI, api->m_ArchivePath, 0, &api->m_ArchiveFileHandle);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_StorageAPI->OpenWriteFile() failed with %d", err)
            Longtail_Free(api->m_ArchivePath);
            return err;
        }
        api->m_StorageAPI->Write(api->m_StorageAPI, api->m_ArchiveFileHandle, 0, *archive_index->m_IndexDataSize, &archive_index[1]);
    }
    else
    {
        int err = api->m_StorageAPI->OpenReadFile(api->m_StorageAPI, api->m_ArchivePath, &api->m_ArchiveFileHandle);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_StorageAPI->OpenReadFile() failed with %d", err)
            Longtail_Free(api->m_ArchivePath);
            return err;
        }
#if LONGTAIL_ENABLE_MMAPED_FILES
        uint64_t archive_size;
        err = api->m_StorageAPI->GetSize(api->m_StorageAPI, api->m_ArchiveFileHandle, &archive_size);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_MapFile() failed with %d", err)
            api->m_StorageAPI->CloseFile(api->m_StorageAPI, api->m_ArchiveFileHandle);
            Longtail_Free(api->m_ArchivePath);
            return err;
        }
        
        api->m_BlockBytesSize = archive_size - *api->m_ArchiveIndex->m_IndexDataSize;
        err = api->m_StorageAPI->MapFile(api->m_StorageAPI, api->m_ArchiveFileHandle, *api->m_ArchiveIndex->m_IndexDataSize, api->m_BlockBytesSize, &api->m_ArchiveFileMapping, (const void**)&api->m_BlockBytes);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_INFO, "Longtail_MapFile() failed with %d, using normal file IO", err)
        }
#endif
    }

    int err = Longtail_CreateSpinLock(Longtail_Alloc("FSBlockStoreAPI", Longtail_GetSpinLockSize()), &api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "api->m_StorageAPI->Longtail_CreateSpinLock() failed with %d", err)
        api->m_StorageAPI->CloseFile(api->m_StorageAPI, api->m_ArchiveFileHandle);
        Longtail_Free(api->m_ArchivePath);
        return err;
    }

    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateArchiveBlockStore(
    struct Longtail_StorageAPI* storage_api,
    const char* archive_path,
    struct Longtail_ArchiveIndex* archive_index,
    int enable_write)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_api, "%p"),
        LONGTAIL_LOGFIELD(archive_path, "%s"),
        LONGTAIL_LOGFIELD(enable_write, "%d"),
        LONGTAIL_LOGFIELD(archive_index, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, storage_api != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, archive_path != 0, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, archive_index, return 0)
    LONGTAIL_VALIDATE_INPUT(ctx, enable_write == 0 || enable_write == 1, return 0)

    size_t api_size = sizeof(struct ArchiveBlockStoreAPI);

    api_size += Longtail_LookupTable_GetSize(*archive_index->m_StoreIndex.m_BlockCount);
    api_size += Longtail_LookupTable_GetSize(*archive_index->m_StoreIndex.m_BlockCount);

    void* mem = Longtail_Alloc("ArchiveBlockStoreAPI", api_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    struct Longtail_BlockStoreAPI* block_store_api;
    int err = ArchiveBlockStore_Init(
        mem,
        storage_api,
        archive_path,
        archive_index,
        enable_write,
        &block_store_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ArchiveBlockStore_Init() failed with %d", err)
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
