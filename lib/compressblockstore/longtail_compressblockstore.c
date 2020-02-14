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
    Longtail_Free(stored_block);
    return 0;
}

static int CompressBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block)
{
    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    uint32_t compressionType = *stored_block->m_BlockIndex->m_DataCompressionType;
    if (compressionType == 0)
    {
        return block_store->m_BackingBlockStore->PutStoredBlock(block_store->m_BackingBlockStore, stored_block);
    }
    struct Longtail_CompressionAPI* compression_api;
    Longtail_CompressionAPI_HSettings compression_settings;
    int err = block_store->m_CompressionRegistryAPI->GetCompressionType(
        block_store->m_CompressionRegistryAPI,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        return err;
    }
    uint32_t block_data_size = stored_block->m_BlockChunksDataSize;
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    size_t block_index_data_size = Longtail_GetBlockIndexDataSize(chunk_count);
    size_t max_compressed_chunk_data_size = compression_api->GetMaxCompressedSize(compression_api, compression_settings, block_data_size);
    size_t stored_block_size = Longtail_GetStoredBlockSize(block_index_data_size + sizeof(uint32_t) + sizeof(uint32_t) + max_compressed_chunk_data_size);
    struct Longtail_StoredBlock* compressed_stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(stored_block_size);

    compressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&compressed_stored_block[1], chunk_count);
    uint8_t* block_index_data_ptr = (uint8_t*)&compressed_stored_block->m_BlockIndex[1];
    memmove(block_index_data_ptr, &stored_block->m_BlockIndex[1], block_index_data_size);

    uint32_t* header_ptr = (uint32_t*)(&block_index_data_ptr[block_index_data_size]);
    compressed_stored_block->m_BlockData = header_ptr;
    size_t compressed_chunk_data_size;
    err = compression_api->Compress(
        compression_api,
        compression_settings,
        (const char*)stored_block->m_BlockData,
        (char*)&header_ptr[2],
        stored_block->m_BlockChunksDataSize,
        max_compressed_chunk_data_size,
        &compressed_chunk_data_size);
    if (err)
    {
        Longtail_Free(compressed_stored_block);
        return err;
    }
    header_ptr[0] = stored_block->m_BlockChunksDataSize;
    header_ptr[1] = (uint32_t)compressed_chunk_data_size;
    compressed_stored_block->m_BlockChunksDataSize = (uint32_t)(sizeof(uint32_t) + sizeof(uint32_t) + compressed_chunk_data_size);
    err = block_store->m_BackingBlockStore->PutStoredBlock(block_store->m_BackingBlockStore, compressed_stored_block);
    Longtail_Free(compressed_stored_block);
    if (err)
    {
        return err;
    }
    return 0;
}

static int CompressBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_StoredBlock** out_stored_block)
{
    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    int err = block_store->m_BackingBlockStore->GetStoredBlock(block_store->m_BackingBlockStore, block_hash, out_stored_block);
    if (err)
    {
        return err;
    }
    if (!out_stored_block)
    {
        return 0;
    }

    uint32_t compressionType = *(*out_stored_block)->m_BlockIndex->m_DataCompressionType;
    if (compressionType == 0)
    {
        return 0;
    }
    struct Longtail_StoredBlock* compressed_stored_block = *out_stored_block;
    *out_stored_block = 0;

    struct Longtail_CompressionAPI* compression_api;
    Longtail_CompressionAPI_HSettings compression_settings;
    err = block_store->m_CompressionRegistryAPI->GetCompressionType(
        block_store->m_CompressionRegistryAPI,
        compressionType,
        &compression_api,
        &compression_settings);
    if (err)
    {
        compressed_stored_block->Dispose(compressed_stored_block);
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
    uncompressed_stored_block->m_BlockIndex = Longtail_InitBlockIndex(&uncompressed_stored_block[1], chunk_count);
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
        Longtail_Free(uncompressed_stored_block);
        compressed_stored_block->Dispose(compressed_stored_block);
        return EBADF;
    }
    compressed_stored_block->Dispose(compressed_stored_block);
    uncompressed_stored_block->Dispose = CompressedStoredBlock_Dispose;
    *out_stored_block = uncompressed_stored_block;
    return 0;
}

static int CompressBlockStore_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
	struct Longtail_JobAPI* job_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_ContentIndex** out_content_index)
{
    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    return block_store->m_BackingBlockStore->GetIndex(
        block_store->m_BackingBlockStore,
        job_api,
        default_hash_api_identifier,
        progress_api,
        out_content_index);
}

static int CompressBlockStore_GetStoredBlockPath(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, char** out_path)
{
    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)block_store_api;
    return block_store->m_BackingBlockStore->GetStoredBlockPath(
        block_store->m_BackingBlockStore,
        block_hash,
        out_path);
}


static void CompressBlockStore_Dispose(struct Longtail_API* api)
{
    struct CompressBlockStoreAPI* block_store = (struct CompressBlockStoreAPI*)api;
    Longtail_Free(block_store);
}

static int CompressBlockStore_Init(
    struct CompressBlockStoreAPI* api,
    struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry)
{
    api->m_BlockStoreAPI.m_API.Dispose = CompressBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = CompressBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = CompressBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = CompressBlockStore_GetIndex;
    api->m_BlockStoreAPI.GetStoredBlockPath = CompressBlockStore_GetStoredBlockPath;
    api->m_BackingBlockStore = backing_block_store;
    api->m_CompressionRegistryAPI = compression_registry;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateCompressBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry)
{
    struct CompressBlockStoreAPI* api = (struct CompressBlockStoreAPI*)Longtail_Alloc(sizeof(struct CompressBlockStoreAPI));
    CompressBlockStore_Init(
        api,
        backing_block_store,
        compression_registry);
    return &api->m_BlockStoreAPI;
}
