#include "longtail_compressblockstore.h"

#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct CompressBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    struct Longtail_CompressionRegistryAPI* m_CompressionRegistryAPI;
    struct Longtail_BlockStore_Stats m_Stats;

    TLongtail_Atomic32 m_PendingRequestCount;

    TLongtail_Atomic64 m_IndexGetCount;
    TLongtail_Atomic64 m_BlocksGetCount;
    TLongtail_Atomic64 m_BlocksPutCount;
    TLongtail_Atomic64 m_ChunksGetCount;
    TLongtail_Atomic64 m_ChunksPutCount;
    TLongtail_Atomic64 m_BytesGetCount;
    TLongtail_Atomic64 m_BytesPutCount;
    TLongtail_Atomic64 m_IndexGetRetryCount;
    TLongtail_Atomic64 m_BlockGetRetryCount;
    TLongtail_Atomic64 m_BlockPutRetryCount;
    TLongtail_Atomic64 m_IndexGetFailCount;
    TLongtail_Atomic64 m_BlockGetFailCount;
    TLongtail_Atomic64 m_BlockPutFailCount;
};

static int CompressedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressedStoredBlock_Dispose(%p)", stored_block)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    Longtail_Free(stored_block);
    return 0;
}

static int CompressBlock(
    struct Longtail_CompressionRegistryAPI* compression_registry,
    struct Longtail_StoredBlock* uncompressed_stored_block,
    struct Longtail_StoredBlock** out_compressed_stored_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlock(%p, %p, %p)", compression_registry, uncompressed_stored_block, out_compressed_stored_block)
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL)
    LONGTAIL_FATAL_ASSERT(uncompressed_stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_compressed_stored_block, return EINVAL)
    uint32_t compressionType = *uncompressed_stored_block->m_BlockIndex->m_Tag;
    if (compressionType == 0)
    {
        *out_compressed_stored_block = 0;
        return 0;
    }
    struct Longtail_CompressionAPI* compression_api;
    uint32_t compression_settings;
    int err = compression_registry->GetCompressionAPI(
        compression_registry,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) failed with %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            err)
        return err;
    }
    uint32_t block_chunk_data_size = uncompressed_stored_block->m_BlockChunksDataSize;
    uint32_t chunk_count = *uncompressed_stored_block->m_BlockIndex->m_ChunkCount;
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t max_compressed_chunk_data_size = compression_api->GetMaxCompressedSize(compression_api, compression_settings, block_chunk_data_size);
    size_t compressed_stored_block_size = sizeof(struct Longtail_StoredBlock) + block_index_size + sizeof(uint32_t) + sizeof(uint32_t) + max_compressed_chunk_data_size;
    struct Longtail_StoredBlock* compressed_stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(compressed_stored_block_size);
    if (!compressed_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) failed with %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            ENOMEM)
        return ENOMEM;
    }
    compressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&compressed_stored_block[1], chunk_count);
    LONGTAIL_FATAL_ASSERT(compressed_stored_block->m_BlockIndex != 0, return EINVAL; )

    uint32_t* header_ptr = (uint32_t*)(&((uint8_t*)compressed_stored_block->m_BlockIndex)[block_index_size]);
    compressed_stored_block->m_BlockData = header_ptr;
    memmove(compressed_stored_block->m_BlockIndex, uncompressed_stored_block->m_BlockIndex, block_index_size);
    size_t compressed_chunk_data_size;
    err = compression_api->Compress(
        compression_api,
        compression_settings,
        (const char*)uncompressed_stored_block->m_BlockData,
        (char*)&header_ptr[2],
        block_chunk_data_size,
        max_compressed_chunk_data_size,
        &compressed_chunk_data_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) failed with %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            err)
        Longtail_Free(compressed_stored_block);
        return err;
    }
    header_ptr[0] = block_chunk_data_size;
    header_ptr[1] = (uint32_t)compressed_chunk_data_size;
    compressed_stored_block->m_BlockChunksDataSize = (uint32_t)(sizeof(uint32_t) + sizeof(uint32_t) + compressed_chunk_data_size);
    compressed_stored_block->Dispose = CompressedStoredBlock_Dispose;
    *out_compressed_stored_block = compressed_stored_block;
    return 0;
}

struct OnPutBackingStoreAsync_API
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    struct Longtail_StoredBlock* m_CompressedBlock;
    struct Longtail_AsyncPutStoredBlockAPI* m_AsyncCompleteAPI;
    struct CompressBlockStoreAPI* m_CompressBlockStoreAPI;
};

static void OnPutBackingStoreComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnPuttBackingStoreComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct OnPutBackingStoreAsync_API* async_block_store = (struct OnPutBackingStoreAsync_API*)async_complete_api;
    struct CompressBlockStoreAPI* compressblockstore_api = async_block_store->m_CompressBlockStoreAPI;
    if (async_block_store->m_CompressedBlock)
    {
        async_block_store->m_CompressedBlock->Dispose(async_block_store->m_CompressedBlock);
    }
    async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, err);
    Longtail_Free(async_block_store);
    Longtail_AtomicAdd32(&compressblockstore_api->m_PendingRequestCount, -1);
}

static int CompressBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL);

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;

    Longtail_AtomicAdd64(&block_store->m_BlocksPutCount, 1);
    Longtail_AtomicAdd64(&block_store->m_ChunksPutCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&block_store->m_BytesPutCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    struct Longtail_StoredBlock* compressed_stored_block;

    int err = CompressBlock(block_store->m_CompressionRegistryAPI, stored_block, &compressed_stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        return err;
    }
    struct Longtail_StoredBlock* to_store = compressed_stored_block ? compressed_stored_block : stored_block;

    size_t on_put_backing_store_async_api_size = sizeof(struct OnPutBackingStoreAsync_API);
    struct OnPutBackingStoreAsync_API* on_put_backing_store_async_api = (struct OnPutBackingStoreAsync_API*)Longtail_Alloc(on_put_backing_store_async_api_size);
    if (!on_put_backing_store_async_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            ENOMEM)
        Longtail_Free(compressed_stored_block);
        return ENOMEM;
    }
    on_put_backing_store_async_api->m_API.OnComplete = OnPutBackingStoreComplete;
    on_put_backing_store_async_api->m_API.m_API.Dispose = 0;
    on_put_backing_store_async_api->m_CompressedBlock = compressed_stored_block;
    on_put_backing_store_async_api->m_AsyncCompleteAPI = async_complete_api;
    on_put_backing_store_async_api->m_CompressBlockStoreAPI = block_store;
    Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, 1);
    err = block_store->m_BackingBlockStore->PutStoredBlock(block_store->m_BackingBlockStore, to_store, &on_put_backing_store_async_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) failed with %d",
            block_store_api, stored_block, async_complete_api,
            err)
        Longtail_Free(on_put_backing_store_async_api);
        compressed_stored_block->Dispose(compressed_stored_block);
        Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, -1);
    }
    return err;
}

static int CompressBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_count, const TLongtail_Hash* block_hashes, const uint32_t* block_ref_counts)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "ShareBlockStore_PreflightGet(%p, 0x%" PRIx64 ", %p, %p)", block_store_api, block_count, block_hashes, block_ref_counts)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_hashes, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(block_ref_counts, return EINVAL)
    struct CompressBlockStoreAPI* api = (struct CompressBlockStoreAPI*)block_store_api;
    return api->m_BackingBlockStore->PreflightGet(
        api->m_BackingBlockStore,
        block_count,
        block_hashes,
        block_ref_counts);
}

static int DecompressBlock(
    struct Longtail_CompressionRegistryAPI* compression_registry,
    struct Longtail_StoredBlock* compressed_stored_block,
    struct Longtail_StoredBlock** out_stored_block)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "DecompressBlock(%p, %p, %p)", compression_registry, compressed_stored_block, out_stored_block)
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL)
    LONGTAIL_FATAL_ASSERT(compressed_stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_stored_block, return EINVAL)
    uint32_t compressionType = *compressed_stored_block->m_BlockIndex->m_Tag;
    struct Longtail_CompressionAPI* compression_api;
    uint32_t compression_settings;
    int err = compression_registry->GetCompressionAPI(
        compression_registry,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DecompressBlock(%p, %p, %p) failed with %d",
            compression_registry, compressed_stored_block, out_stored_block,
            err)
        return err;
    }

    uint32_t chunk_count = *compressed_stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    uint32_t* header_ptr = (uint32_t*)compressed_stored_block->m_BlockData;
    void* compressed_chunks_data = &header_ptr[2];
    uint32_t uncompressed_size = header_ptr[0];
    uint32_t compressed_size = header_ptr[1];

    uint32_t uncompressed_block_data_size = block_index_data_size + uncompressed_size;
    size_t uncompressed_stored_block_size = Longtail_GetStoredBlockSize(uncompressed_block_data_size);
    struct Longtail_StoredBlock* uncompressed_stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(uncompressed_stored_block_size);
    if (!uncompressed_stored_block)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DecompressBlock(%p, %p, %p) failed with %d",
            compression_registry, compressed_stored_block, out_stored_block,
            ENOMEM)
        return ENOMEM;
    }
    uncompressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&uncompressed_stored_block[1], chunk_count);
    LONGTAIL_FATAL_ASSERT(uncompressed_stored_block->m_BlockIndex, return EINVAL; )
    uncompressed_stored_block->m_BlockData = &((uint8_t*)(&uncompressed_stored_block->m_BlockIndex[1]))[block_index_data_size];
    uncompressed_stored_block->m_BlockChunksDataSize = uncompressed_size;
    memmove(&uncompressed_stored_block->m_BlockIndex[1], &compressed_stored_block->m_BlockIndex[1], block_index_data_size);

    size_t real_uncompressed_size = 0;
    err = compression_api->Decompress(
        compression_api,
        (const char*)compressed_chunks_data,
        (char*)uncompressed_stored_block->m_BlockData,
        compressed_size,
        uncompressed_size,
        &real_uncompressed_size);
    if (real_uncompressed_size != uncompressed_size)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DecompressBlock(%p, %p, %p) failed with %d",
            compression_registry, compressed_stored_block, out_stored_block,
            ENOMEM)
        Longtail_Free(uncompressed_stored_block);
        return EBADF;
    }
    compressed_stored_block->Dispose(compressed_stored_block);
    uncompressed_stored_block->Dispose = CompressedStoredBlock_Dispose;
    *out_stored_block = uncompressed_stored_block;
    return 0;
}

struct OnGetBackingStoreAsync_API
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    struct CompressBlockStoreAPI* m_BlockStore;
    struct Longtail_AsyncGetStoredBlockAPI* m_AsyncCompleteAPI;
};

static void OnGetBackingStoreComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetBackingStoreComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return)
    struct OnGetBackingStoreAsync_API* async_block_store = (struct OnGetBackingStoreAsync_API*)async_complete_api;
    struct CompressBlockStoreAPI* blockstore = async_block_store->m_BlockStore;
    if (err)
    {
        if (err != ENOENT)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetBackingStoreComplete(%p, %p, %d) failed with %d",
                async_complete_api, stored_block, err,
                err)
        }
        async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, err);
        Longtail_Free(async_block_store);
        Longtail_AtomicAdd32(&blockstore->m_PendingRequestCount, -1);
        return;
    }

    Longtail_AtomicAdd64(&async_block_store->m_BlockStore->m_BlocksGetCount, 1);
    Longtail_AtomicAdd64(&async_block_store->m_BlockStore->m_ChunksGetCount, *stored_block->m_BlockIndex->m_ChunkCount);
    Longtail_AtomicAdd64(&async_block_store->m_BlockStore->m_BytesGetCount, Longtail_GetBlockIndexDataSize(*stored_block->m_BlockIndex->m_ChunkCount) + stored_block->m_BlockChunksDataSize);

    uint32_t compressionType = *stored_block->m_BlockIndex->m_Tag;
    if (compressionType == 0)
    {
        async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, 0);
        Longtail_Free(async_block_store);
        Longtail_AtomicAdd32(&blockstore->m_PendingRequestCount, -1);
        return;
    }

    err = DecompressBlock(
        async_block_store->m_BlockStore->m_CompressionRegistryAPI,
        stored_block,
        &stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetBackingStoreComplete(%p, %p, %d) failed with %d",
            async_complete_api, stored_block, err,
            err)
        stored_block->Dispose(stored_block);
        async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(async_block_store);
        Longtail_AtomicAdd32(&blockstore->m_PendingRequestCount, -1);
        return;
    }
    async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, 0);
    Longtail_Free(async_block_store);
    Longtail_AtomicAdd32(&blockstore->m_PendingRequestCount, -1);
}

static int CompressBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    size_t on_fetch_backing_store_async_api_size = sizeof(struct OnGetBackingStoreAsync_API);
    struct OnGetBackingStoreAsync_API* on_fetch_backing_store_async_api = (struct OnGetBackingStoreAsync_API*)Longtail_Alloc(on_fetch_backing_store_async_api_size);
    if (!on_fetch_backing_store_async_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
            block_store_api, block_hash, async_complete_api,
            ENOMEM)
        return ENOMEM;
    }

    on_fetch_backing_store_async_api->m_API.OnComplete = OnGetBackingStoreComplete;
    on_fetch_backing_store_async_api->m_API.m_API.Dispose = 0;
    on_fetch_backing_store_async_api->m_BlockStore = block_store;
    on_fetch_backing_store_async_api->m_AsyncCompleteAPI = async_complete_api;

    Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, 1);
    int err = block_store->m_BackingBlockStore->GetStoredBlock(block_store->m_BackingBlockStore, block_hash, &on_fetch_backing_store_async_api->m_API);
    if (err)
    {
        if (err != ENOENT)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with %d",
                block_store_api, block_hash, async_complete_api,
                err)
        }
        Longtail_Free(on_fetch_backing_store_async_api);
        Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, -1);
        return err;
    }
    return 0;
}

static int CompressBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_GetIndex(%p, %u, %p)",
        block_store_api, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    Longtail_AtomicAdd64(&block_store->m_IndexGetCount, 1);
    return block_store->m_BackingBlockStore->GetIndex(
        block_store->m_BackingBlockStore,
        async_complete_api);
}

static int CompressBlockStore_RetargetContent(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_ContentIndex* content_index,
    struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_RetargetContent(%p, %p, %p)",
        block_store_api, content_index, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(content_index, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CompressBlockStoreAPI* api = (struct CompressBlockStoreAPI*)block_store_api;
    int err = api->m_BackingBlockStore->RetargetContent(
        api->m_BackingBlockStore,
        content_index,
        async_complete_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_RetargetContent(%p, %p, %p) failed with %d",
            block_store_api, content_index, async_complete_api,
            err)
        return err;
    }
    return 0;
}

static int CompressBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_GetStats(%p, %p)", block_store_api, out_stats)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(out_stats, return EINVAL)
    struct CompressBlockStoreAPI* compressblockstore_api = (struct CompressBlockStoreAPI*)block_store_api;
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    out_stats->m_IndexGetCount = compressblockstore_api->m_IndexGetCount;
    out_stats->m_BlocksGetCount = compressblockstore_api->m_BlocksGetCount;
    out_stats->m_BlocksPutCount = compressblockstore_api->m_BlocksPutCount;
    out_stats->m_ChunksGetCount = compressblockstore_api->m_ChunksGetCount;
    out_stats->m_ChunksPutCount = compressblockstore_api->m_ChunksPutCount;
    out_stats->m_BytesGetCount = compressblockstore_api->m_BytesGetCount;
    out_stats->m_BytesPutCount = compressblockstore_api->m_BytesPutCount;
    out_stats->m_IndexGetRetryCount = compressblockstore_api->m_IndexGetRetryCount;
    out_stats->m_BlockGetRetryCount = compressblockstore_api->m_BlockGetRetryCount;
    out_stats->m_BlockPutRetryCount = compressblockstore_api->m_BlockPutRetryCount;
    out_stats->m_IndexGetFailCount = compressblockstore_api->m_IndexGetFailCount;
    out_stats->m_BlockGetFailCount = compressblockstore_api->m_BlockGetFailCount;
    out_stats->m_BlockPutFailCount = compressblockstore_api->m_BlockPutFailCount;
    return 0;
}

static void CompressBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_Dispose(%p)", api)
    LONGTAIL_VALIDATE_INPUT(api, return)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)api;
    while (block_store->m_PendingRequestCount > 0)
    {
        Longtail_Sleep(1000);
        if (block_store->m_PendingRequestCount > 0)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_Dispose(%p) waiting for %d pending requests", block_store, (int32_t)block_store->m_PendingRequestCount);
        }
    }
    Longtail_Free(block_store);
}

static int CompressBlockStore_Init(
    void* mem,
    struct Longtail_BlockStoreAPI* backing_block_store,
    struct Longtail_CompressionRegistryAPI* compression_registry,
    struct Longtail_BlockStoreAPI** out_block_store_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_Init(%p, %p, %p, %p)",
        mem, backing_block_store, compression_registry, out_block_store_api)
    LONGTAIL_FATAL_ASSERT(mem, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL)
    LONGTAIL_FATAL_ASSERT(out_block_store_api, return EINVAL)

    struct Longtail_BlockStoreAPI* block_store_api = Longtail_MakeBlockStoreAPI(
        mem,
        CompressBlockStore_Dispose,
        CompressBlockStore_PutStoredBlock,
        CompressBlockStore_PreflightGet,
        CompressBlockStore_GetStoredBlock,
        CompressBlockStore_GetIndex,
        CompressBlockStore_RetargetContent,
        CompressBlockStore_GetStats);
    if (!block_store_api)
    {
        return EINVAL;
    }

    struct CompressBlockStoreAPI* api = (struct CompressBlockStoreAPI*)block_store_api;

    api->m_BackingBlockStore = backing_block_store;
    api->m_CompressionRegistryAPI = compression_registry;
    api->m_PendingRequestCount = 0;

    api->m_IndexGetCount = 0;
    api->m_BlocksGetCount = 0;
    api->m_BlocksPutCount = 0;
    api->m_ChunksGetCount = 0;
    api->m_ChunksPutCount = 0;
    api->m_BytesGetCount = 0;
    api->m_BytesPutCount = 0;
    api->m_IndexGetRetryCount = 0;
    api->m_BlockGetRetryCount = 0;
    api->m_BlockPutRetryCount = 0;
    api->m_IndexGetFailCount = 0;
    api->m_BlockGetFailCount = 0;
    api->m_BlockPutFailCount = 0;

    *out_block_store_api = block_store_api;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCompressBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    struct Longtail_CompressionRegistryAPI* compression_registry)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateCompressBlockStoreAPI(%p, %p)", backing_block_store, compression_registry)
    LONGTAIL_VALIDATE_INPUT(backing_block_store, return 0)
    LONGTAIL_VALIDATE_INPUT(compression_registry, return 0)

    size_t api_size = sizeof(struct CompressBlockStoreAPI);
    void* mem = Longtail_Alloc(api_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateCompressBlockStoreAPI(%p, %p) failed with %d",
            backing_block_store, compression_registry,
            ENOMEM)
        return 0;
    }
    struct Longtail_BlockStoreAPI* block_store_api;
    int err = CompressBlockStore_Init(
        mem,
        backing_block_store,
        compression_registry,
        &block_store_api);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateCompressBlockStoreAPI(%p, %p) failed with %d",
            backing_block_store, compression_registry,
            err)
        Longtail_Free(mem);
        return 0;
    }
    return block_store_api;
}
