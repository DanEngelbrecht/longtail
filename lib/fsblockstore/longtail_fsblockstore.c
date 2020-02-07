#include "longtail_fsblockstore.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct FSBlockStoreJobAPI
{
    struct Longtail_BlockStoreAPI m_BlockStoreAPI;
    struct Longtail_StorageAPI* m_StorageAPI;
    struct Longtail_HashAPI* m_HashAPI;
    struct Longtail_JobAPI* m_JobAPI;
    struct Longtail_CompressionRegistryAPI* m_CompressionRegistryApi;
    char* m_ContentPath;
    void* m_ContentIndexBuffer;
    uint64_t m_ContentIndexSize;
};

struct FSStoredBlock
{
    struct Longtail_StoredBlock m_StoredBlock;
};

static int FSStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free(stored_block);
    return 0;
}

#define MAX_BLOCK_NAME_LENGTH   32

static void GetBlockName(TLongtail_Hash block_hash, char* out_name)
{
    sprintf(&out_name[5], "0x%016" PRIx64, block_hash);
    memmove(out_name, &out_name[7], 4);
    out_name[4] = '/';
}

static char* GetBlockPath(struct FSBlockStoreJobAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.lrb", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static char* GetTempBlockPath(struct FSBlockStoreJobAPI* fsblockstore_api, TLongtail_Hash block_hash)
{
    char block_name[MAX_BLOCK_NAME_LENGTH];
    GetBlockName(block_hash, block_name);
    char file_name[64];
    sprintf(file_name, "%s.tmp", block_name);
    return fsblockstore_api->m_StorageAPI->ConcatPath(fsblockstore_api->m_StorageAPI, fsblockstore_api->m_ContentPath, file_name);
}

static int DecompressBlock(
    struct Longtail_CompressionRegistryAPI* compression_registry_api,
    uint32_t compression_type,
    size_t compressed_size,
    size_t uncompressed_size,
    const char* compressed_buffer,
    char* uncompressed_buffer)
{
    struct Longtail_CompressionAPI* compression_api;
    Longtail_CompressionAPI_HSettings compression_settings;
    int err = compression_registry_api->GetCompressionType(compression_registry_api, compression_type, &compression_api, &compression_settings);
    if (err)
    {
        return err;
    }
    size_t size;
    err = compression_api->Decompress(compression_api, compressed_buffer, uncompressed_buffer, compressed_size, uncompressed_size, &size);
    if (err)
    {
        return err;
    }
    if (size != uncompressed_size)
    {
        return EBADF;
    }
    return 0;
}

static int FSBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;

    char* block_path = GetBlockPath(fsblockstore_api, *stored_block->m_BlockIndex->m_BlockHash);
    if (fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return 0;
    }

    char* tmp_block_path = GetTempBlockPath(fsblockstore_api, *stored_block->m_BlockIndex->m_BlockHash);

    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_data_size = 0;
    for (uint32_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index)
    {
        block_data_size += stored_block->m_BlockIndex->m_ChunkSizes[chunk_index];
    }

    int err = EnsureParentPathExists(fsblockstore_api->m_StorageAPI, block_path);
    if (err)
    {
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    Longtail_StorageAPI_HOpenFile block_file_handle;
    err = fsblockstore_api->m_StorageAPI->OpenWriteFile(fsblockstore_api->m_StorageAPI, tmp_block_path, 0, &block_file_handle);
    if (err)
    {
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, 0, block_data_size, stored_block->m_BlockData);
    if (err)
    {
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    uint32_t write_offset = block_data_size;

    uint32_t aligned_size = (((write_offset + 15) / 16) * 16);
    uint32_t padding = aligned_size - write_offset;
    if (padding)
    {
        err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, padding, "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock: Failed to write to block file `%s`, %d", tmp_block_path, err)
            fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
            block_file_handle = 0;
            Longtail_Free((char*)tmp_block_path);
            tmp_block_path = 0;
            Longtail_Free((char*)block_path);
            block_path = 0;
            return err;
        }
        write_offset = aligned_size;
    }
    size_t block_index_data_size = Longtail_GetBlockIndexDataSize(chunk_count);
    err = fsblockstore_api->m_StorageAPI->Write(fsblockstore_api->m_StorageAPI, block_file_handle, write_offset, block_index_data_size, &stored_block->m_BlockIndex[1]);
    if (err)
    {
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
        block_file_handle = 0;
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, block_file_handle);
    err = fsblockstore_api->m_StorageAPI->RenameFile(fsblockstore_api->m_StorageAPI, tmp_block_path, block_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_PutStoredBlock: Failed to rename block file from `%s` to `%s`, %d", tmp_block_path, block_path, err)
        Longtail_Free((char*)tmp_block_path);
        tmp_block_path = 0;
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    Longtail_Free((char*)tmp_block_path);
    tmp_block_path = 0;
    Longtail_Free((char*)block_path);
    block_path = 0;

    // TODO: Be better - for now now, flush local cache of content index
    void* tmp = fsblockstore_api->m_ContentIndexBuffer;
    fsblockstore_api->m_ContentIndexBuffer = 0;
    fsblockstore_api->m_ContentIndexSize = 0;
    if (tmp)
    {
        Longtail_Free(tmp);
    }

    return 0;
}

static int FSBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_StoredBlock** out_stored_block)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;
    char* block_path = GetBlockPath(fsblockstore_api, block_hash);
    if (!fsblockstore_api->m_StorageAPI->IsFile(fsblockstore_api->m_StorageAPI, block_path))
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return ENOENT;
    }
    if (!out_stored_block)
    {
        Longtail_Free((char*)block_path);
        block_path = 0;
        return 0;
    }

    Longtail_StorageAPI_HOpenFile f;
    int err = fsblockstore_api->m_StorageAPI->OpenReadFile(fsblockstore_api->m_StorageAPI, block_path, &f);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to open block `%s`, %d", block_path, err)
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    uint64_t compressed_block_size;
    err = fsblockstore_api->m_StorageAPI->GetSize(fsblockstore_api->m_StorageAPI, f, &compressed_block_size);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to get size of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    if (compressed_block_size < (sizeof(uint32_t)))
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    if (compressed_block_size > 0xffffffff)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Invalid format of block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    uint32_t chunk_count = 0;
    err = fsblockstore_api->m_StorageAPI->Read(fsblockstore_api->m_StorageAPI, f, compressed_block_size - sizeof(uint32_t), sizeof(uint32_t), &chunk_count);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to read from block `%s`, %d", block_path, err)
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }
    size_t block_index_data_size = Longtail_GetBlockIndexDataSize(chunk_count);
    if (compressed_block_size < block_index_data_size)
    {
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return EBADF;
    }

    char* compressed_block_content = (char*)Longtail_Alloc(compressed_block_size);
    LONGTAIL_FATAL_ASSERT(compressed_block_content, return ENOMEM)
    err = fsblockstore_api->m_StorageAPI->Read(fsblockstore_api->m_StorageAPI, f, 0, compressed_block_size, compressed_block_content);
    fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
    f = 0;
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to read block `%s`, %d", block_path, err)
        Longtail_Free(compressed_block_content);
        compressed_block_content = 0;
        fsblockstore_api->m_StorageAPI->CloseFile(fsblockstore_api->m_StorageAPI, f);
        Longtail_Free((char*)block_path);
        block_path = 0;
        return err;
    }

    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    char* block_data = compressed_block_content;
    const TLongtail_Hash* block_hash_ptr = (const TLongtail_Hash*)(void*)&block_data[compressed_block_size - block_index_data_size];
    TLongtail_Hash verify_block_hash = *block_hash_ptr;
    if (block_hash != verify_block_hash)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "FSBlockStore_GetStoredBlock: Malformed content block (mismatching block hash) `%s`", block_path)
        Longtail_Free(block_data);
        block_data = 0;
        Longtail_Free(block_path);
        block_path = 0;
        return EBADF;
    }

    uint32_t block_data_size = (uint32_t)(compressed_block_size - block_index_size); // TODO: We should verify that blocks are not larger than 32-bit
    const uint32_t* compression_type_ptr = (const uint32_t*)(void*)&block_hash_ptr[1];
    uint32_t compression_type = *compression_type_ptr;
    if (0 != compression_type)
    {
        block_data_size = ((uint32_t*)(void*)compressed_block_content)[0];
        uint32_t compressed_size = ((uint32_t*)(void*)compressed_block_content)[1];
        block_data = (char*)Longtail_Alloc(block_data_size);
        LONGTAIL_FATAL_ASSERT(block_data, return ENOMEM)
        err = DecompressBlock(
            fsblockstore_api->m_CompressionRegistryApi,
            compression_type,
            compressed_size,
            block_data_size,
            &compressed_block_content[sizeof(uint32_t) * 2],
            block_data);

        Longtail_Free(compressed_block_content);
        compressed_block_content = 0;

        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "FSBlockStore_GetStoredBlock: Failed to decompress block `%s`, %d", block_path, err)
            Longtail_Free(block_data);
            block_data = 0;
            Longtail_Free(block_path);
            block_path = 0;
            return EBADF;
        }
    }

    size_t block_mem_size = sizeof(struct Longtail_StoredBlock) + 
        block_index_size +
        block_data_size;

    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    LONGTAIL_FATAL_ASSERT(stored_block, return ENOMEM)
    stored_block->m_BlockIndex = (struct Longtail_BlockIndex*)&stored_block[1];
    struct Longtail_BlockIndex* block_index = Longtail_InitBlockIndex(stored_block->m_BlockIndex, chunk_count);
    memmove(&block_index[1], block_hash_ptr, block_index_data_size);
    stored_block->m_BlockData = &((uint8_t*)stored_block->m_BlockIndex)[block_index_size];
 
    stored_block->Dispose = FSStoredBlock_Dispose;
    Longtail_Free(compressed_block_content);
    compressed_block_content = 0;
    Longtail_Free(block_path);
    block_path = 0;

    *out_stored_block = stored_block;
    return 0;
}

static int FSBlockStore_GetIndex(struct Longtail_BlockStoreAPI* block_store_api, void* context, Longtail_JobAPI_ProgressFunc progress_func, struct Longtail_ContentIndex** out_content_index)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)block_store_api;
    if (!fsblockstore_api->m_ContentIndexBuffer)
    {
        // TODO: Longtail_ReadContent should be a local function
        int err = Longtail_ReadContent(
            fsblockstore_api->m_StorageAPI,
            fsblockstore_api->m_HashAPI,
            fsblockstore_api->m_JobAPI,
            progress_func,
            context,
            fsblockstore_api->m_ContentPath,
            out_content_index);
        if (err)
        {
            return err;
        }
        err = Longtail_WriteContentIndexToBuffer(*out_content_index, &fsblockstore_api->m_ContentIndexBuffer, &fsblockstore_api->m_ContentIndexSize);
        if (err)
        {
            Longtail_Free(*out_content_index);
            out_content_index = 0;
            return err;
        }
    }

    int err = Longtail_ReadContentIndexFromBuffer(fsblockstore_api->m_ContentIndexBuffer, fsblockstore_api->m_ContentIndexSize, out_content_index);
    return err;
}

static void FSBlockStore_Dispose(struct Longtail_API* api)
{
    struct FSBlockStoreJobAPI* fsblockstore_api = (struct FSBlockStoreJobAPI*)api;
    Longtail_Free(fsblockstore_api->m_ContentPath);
    Longtail_Free(fsblockstore_api->m_ContentIndexBuffer);
    Longtail_Free(fsblockstore_api);
}

static int FSBlockStore_Init(
    struct FSBlockStoreJobAPI* api,
    struct Longtail_StorageAPI* storage_api,
	struct Longtail_HashAPI* hash_api,
	struct Longtail_JobAPI* job_api,
    struct Longtail_CompressionRegistryAPI* compression_registry_api,
	const char* content_path)
{
    api->m_BlockStoreAPI.m_API.Dispose = FSBlockStore_Dispose;
    api->m_BlockStoreAPI.PutStoredBlock = FSBlockStore_PutStoredBlock;
    api->m_BlockStoreAPI.GetStoredBlock = FSBlockStore_GetStoredBlock;
    api->m_BlockStoreAPI.GetIndex = FSBlockStore_GetIndex;
    api->m_StorageAPI = storage_api;
    api->m_HashAPI = hash_api;
    api->m_JobAPI = job_api;
    api->m_CompressionRegistryApi = compression_registry_api;
    api->m_ContentPath = Longtail_Strdup(content_path);
    api->m_ContentIndexBuffer = 0;
    api->m_ContentIndexSize = 0;
    return 0;
}

struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
	struct Longtail_HashAPI* hash_api,
	struct Longtail_JobAPI* job_api,
    struct Longtail_CompressionRegistryAPI* compression_registry_api,
	const char* content_path)
{
    struct FSBlockStoreJobAPI* api = (struct FSBlockStoreJobAPI*)Longtail_Alloc(sizeof(struct FSBlockStoreJobAPI));
    FSBlockStore_Init(
        api,
        storage_api,
        hash_api,
        job_api,
        compression_registry_api,
        content_path);
    return &api->m_BlockStoreAPI;
}
