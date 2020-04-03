#include "longtail_compressblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct CompressBlockStoreAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_BlockStoreAPI* m_BackingBlockStore;
    struct Longtail_CompressionRegistryAPI* m_CompressionRegistryAPI;
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
    int err = compression_registry->GetCompressionType(
        compression_registry,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) compression_registry->GetCompressionType(%p, %u, %p, %p) failed with %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            compression_registry, compressionType, &compression_api, &compression_settings,
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            compressed_stored_block_size,
            ENOMEM)
        return ENOMEM;
    }
    compressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&compressed_stored_block[1], chunk_count);
    if (compressed_stored_block->m_BlockIndex == 0)
    {
        // TODO: Log
        Longtail_Free(compressed_stored_block);
        return EINVAL;
    }

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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlock(%p, %p, %p) compression_api->Compress(%p, %p, %p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d",
            compression_registry, uncompressed_stored_block, out_compressed_stored_block,
            compression_api, compression_settings, (const char*)uncompressed_stored_block->m_BlockData, (char*)&header_ptr[2], block_chunk_data_size, max_compressed_chunk_data_size, &compressed_chunk_data_size,
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
};

static int OnPutBackingStoreComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnPuttBackingStoreComplete(%p, %d)", async_complete_api, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnPutBackingStoreAsync_API* async_block_store = (struct OnPutBackingStoreAsync_API*)async_complete_api;
    if (async_block_store->m_CompressedBlock)
    {
        async_block_store->m_CompressedBlock->Dispose(async_block_store->m_CompressedBlock);
    }
    async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, err);
    Longtail_Free(async_block_store);
    return 0;
}

static int CompressBlockStore_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CompressBlockStore_PutStoredBlock(%p, %p, %p)", block_store_api, stored_block, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(stored_block, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL);

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    struct Longtail_StoredBlock* compressed_stored_block;
    int err = CompressBlock(block_store->m_CompressionRegistryAPI, stored_block, &compressed_stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) CompressBlock(%p, %p, %p) failed with, %d",
            block_store_api, stored_block, async_complete_api,
            block_store->m_CompressionRegistryAPI, stored_block, &compressed_stored_block,
            err)
        return err;
    }
    struct Longtail_StoredBlock* to_store = compressed_stored_block ? compressed_stored_block : stored_block;

    size_t on_put_backing_store_async_api_size = sizeof(struct OnPutBackingStoreAsync_API);
    struct OnPutBackingStoreAsync_API* on_put_backing_store_async_api = (struct OnPutBackingStoreAsync_API*)Longtail_Alloc(on_put_backing_store_async_api_size);
    if (!on_put_backing_store_async_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            block_store_api, stored_block, async_complete_api,
            on_put_backing_store_async_api_size,
            ENOMEM)
        Longtail_Free(compressed_stored_block);
        return ENOMEM;
    }
    on_put_backing_store_async_api->m_API.OnComplete = OnPutBackingStoreComplete;
    on_put_backing_store_async_api->m_API.m_API.Dispose = 0;
    on_put_backing_store_async_api->m_CompressedBlock = compressed_stored_block;
    on_put_backing_store_async_api->m_AsyncCompleteAPI = async_complete_api;
    err = block_store->m_BackingBlockStore->PutStoredBlock(block_store->m_BackingBlockStore, to_store, &on_put_backing_store_async_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_PutStoredBlock(%p, %p, %p) block_store->m_BackingBlockStore->PutStoredBlock(%p, %p, %p) failed with, %d",
            block_store_api, stored_block, async_complete_api,
            block_store->m_BackingBlockStore, to_store, &on_put_backing_store_async_api->m_API,
            err)
        Longtail_Free(on_put_backing_store_async_api);
        compressed_stored_block->Dispose(compressed_stored_block);
    }
    return err;
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
    int err = compression_registry->GetCompressionType(
        compression_registry,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DecompressBlock(%p, %p, %p) compression_registry->GetCompressionType(%p, %u, %p, %p) failed with %d",
            compression_registry, compressed_stored_block, out_stored_block,
            compression_registry, compressionType, &compression_api, &compression_settings,
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "DecompressBlock(%p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            compression_registry, compressed_stored_block, out_stored_block,
            uncompressed_stored_block_size,
            ENOMEM)
        return ENOMEM;
    }
    uncompressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&uncompressed_stored_block[1], chunk_count);
    if (!uncompressed_stored_block->m_BlockIndex)
    {
        // TODO: Log
        Longtail_Free(uncompressed_stored_block);
        return EINVAL;
    }
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
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressionBlockStore_GetStoredBlock: Can't decompress block with %u", compressionType)
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

static int OnGetBackingStoreComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "OnGetBackingStoreComplete(%p, %p, %d)", async_complete_api, stored_block, err)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    struct OnGetBackingStoreAsync_API* async_block_store = (struct OnGetBackingStoreAsync_API*)async_complete_api;
    if (err)
    {
        if (err != ENOENT)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "OnGetBackingStoreComplete(%p, %p, %d) failed with %d",
                async_complete_api, stored_block, err,
                err)
        }
        err = async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, err);
        Longtail_Free(async_block_store);
        return err;
    }

    uint32_t compressionType = *stored_block->m_BlockIndex->m_Tag;
    if (compressionType == 0)
    {
        err = async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, 0);
        Longtail_Free(async_block_store);
        return err;
    }

    err = DecompressBlock(
        async_block_store->m_BlockStore->m_CompressionRegistryAPI,
        stored_block,
        &stored_block);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "OnGetBackingStoreComplete(%p, %p, %d) DecompressBlock(%p, %p, %p) failed with %d",
            async_complete_api, stored_block, err,
            async_block_store->m_BlockStore->m_CompressionRegistryAPI, stored_block, &stored_block,
            err)
        stored_block->Dispose(stored_block);
        err = async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, 0, err);
        Longtail_Free(async_block_store);
        return err;
    }
    err = async_block_store->m_AsyncCompleteAPI->OnComplete(async_block_store->m_AsyncCompleteAPI, stored_block, 0);
    Longtail_Free(async_block_store);
    return err;
}

static int CompressBlockStore_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p)", block_store_api, block_hash, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    size_t on_fetch_backing_store_async_api_size = sizeof(struct OnGetBackingStoreAsync_API);
    struct OnGetBackingStoreAsync_API* on_fetch_backing_store_async_api = (struct OnGetBackingStoreAsync_API*)Longtail_Alloc(on_fetch_backing_store_async_api_size);
    if (!on_fetch_backing_store_async_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            block_store_api, block_hash, async_complete_api,
            on_fetch_backing_store_async_api_size,
            ENOMEM)
        return ENOMEM;
    }

    on_fetch_backing_store_async_api->m_API.OnComplete = OnGetBackingStoreComplete;
    on_fetch_backing_store_async_api->m_API.m_API.Dispose = 0;
    on_fetch_backing_store_async_api->m_BlockStore = block_store;
    on_fetch_backing_store_async_api->m_AsyncCompleteAPI = async_complete_api;

    int err = block_store->m_BackingBlockStore->GetStoredBlock(block_store->m_BackingBlockStore, block_hash, &on_fetch_backing_store_async_api->m_API);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "CompressBlockStore_GetStoredBlock(%p, 0x%" PRIx64 ", %p) block_store->m_BackingBlockStore->GetStoredBlock(%p, 0x%" PRIx64 ", %p) failed with, %d",
            block_store_api, block_hash, async_complete_api,
            block_store->m_BackingBlockStore, block_hash, &on_fetch_backing_store_async_api->m_API,
            err)
        Longtail_Free(on_fetch_backing_store_async_api);
        return err;
    }
    return 0;
}

static int CompressBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CompressBlockStore_GetIndex(%p, %u, %p)", block_store_api, default_hash_api_identifier, async_complete_api)
    LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    return block_store->m_BackingBlockStore->GetIndex(
        block_store->m_BackingBlockStore,
        default_hash_api_identifier,
        async_complete_api);
}

static void CompressBlockStore_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CompressBlockStore_Dispose(%p)", api)
    LONGTAIL_FATAL_ASSERT(api, return)

    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)api;
    Longtail_Free(block_store);
}

static int CompressBlockStore_Init(
    struct CompressBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "CompressBlockStore_Dispose(%p, %p, %p)", api, backing_block_store, compression_registry)
    LONGTAIL_FATAL_ASSERT(api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(backing_block_store, return EINVAL)
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL)

    api->m_BlockStoreAPI.m_API.Dispose = CompressBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = CompressBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = CompressBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = CompressBlockStore_GetIndex;
    api->m_BackingBlockStore = backing_block_store;
    api->m_CompressionRegistryAPI = compression_registry;
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
    struct CompressBlockStoreAPI* api = (struct CompressBlockStoreAPI*)Longtail_Alloc(api_size);
    if (!api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateCompressBlockStoreAPI(%p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            backing_block_store, compression_registry,
            api_size,
            ENOMEM)
        return 0;
    }
    CompressBlockStore_Init(
        api,
        backing_block_store,
        compression_registry);
    return &api->m_BlockStoreAPI;
}





struct Default_CompressionRegistry
{
    struct Longtail_CompressionRegistryAPI m_CompressionRegistryAPI;
    uint32_t m_Count;
    uint32_t* m_Types;
    struct Longtail_CompressionAPI** m_APIs;
    uint32_t* m_Settings;
};

static void DefaultCompressionRegistry_Dispose(struct Longtail_API* api)
{
    LONGTAIL_FATAL_ASSERT(api, return);
    struct Longtail_CompressionAPI* last_api = 0;
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)api;
    for (uint32_t c = 0; c < default_compression_registry->m_Count; ++c)
    {
        struct Longtail_CompressionAPI* api = default_compression_registry->m_APIs[c];
        if (api != last_api)
        {
            api->m_API.Dispose(&api->m_API);
            last_api = api;
        }
    }
    Longtail_Free(default_compression_registry);
}

static int Default_GetCompressionType(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, uint32_t* out_settings)
{
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_compression_api, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_settings, return EINVAL);
    
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)compression_registry;
    for (uint32_t i = 0; i < default_compression_registry->m_Count; ++i)
    {
        if (default_compression_registry->m_Types[i] == compression_type)
        {
            *out_compression_api = default_compression_registry->m_APIs[i];
            *out_settings = default_compression_registry->m_Settings[i];
            return 0;
        }
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Default_GetCompressionType(%p, %u, %p, %p) failed with %d", compression_registry, compression_type, out_compression_api, out_settings, ENOENT)
    return ENOENT;
}

struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
    uint32_t compression_type_count,
    const uint32_t* compression_types,
    const struct Longtail_CompressionAPI** compression_apis,
    const uint32_t* compression_settings)
{
    LONGTAIL_VALIDATE_INPUT(compression_types, return 0);
    LONGTAIL_VALIDATE_INPUT(compression_apis, return 0);
    LONGTAIL_VALIDATE_INPUT(compression_settings, return 0);
    size_t registry_size = sizeof(struct Default_CompressionRegistry) +
        sizeof(uint32_t) * compression_type_count +
        sizeof(struct Longtail_CompressionAPI*) * compression_type_count +
        sizeof(uint32_t) * compression_type_count;
    struct Default_CompressionRegistry* registry = (struct Default_CompressionRegistry*)Longtail_Alloc(registry_size);
    if (!registry)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateDefaultCompressionRegistry(%u, %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            compression_type_count, compression_types, compression_apis, compression_settings,
            registry_size,
            ENOMEM)
        return 0;
    }

    registry->m_CompressionRegistryAPI.m_API.Dispose = DefaultCompressionRegistry_Dispose;
    registry->m_CompressionRegistryAPI.GetCompressionType = Default_GetCompressionType;

    registry->m_Count = compression_type_count;
    char* p = (char*)&registry[1];
    registry->m_Types = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * compression_type_count;

    registry->m_APIs = (struct Longtail_CompressionAPI**)(void*)p;
    p += sizeof(struct Longtail_CompressionAPI*) * compression_type_count;

    registry->m_Settings = (uint32_t*)(void*)p;

    memmove(registry->m_Types, compression_types, sizeof(uint32_t) * compression_type_count);
    memmove(registry->m_APIs, compression_apis, sizeof(struct Longtail_CompressionAPI*) * compression_type_count);
    memmove(registry->m_Settings, compression_settings, sizeof(const uint32_t) * compression_type_count);

    return &registry->m_CompressionRegistryAPI;
}

