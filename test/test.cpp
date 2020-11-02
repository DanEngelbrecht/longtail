#include "../src/longtail.h"
#include "../src/ext/stb_ds.h"

#include "ext/jc_test.h"

#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/brotli/longtail_brotli.h"
#include "../lib/atomiccancel/longtail_atomiccancel.h"
#include "../lib/blockstorestorage/longtail_blockstorestorage.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/compressblockstore/longtail_compressblockstore.h"
#include "../lib/compressionregistry/longtail_full_compression_registry.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/hpcdcchunker/longtail_hpcdcchunker.h"
#include "../lib/hashregistry/longtail_full_hash_registry.h"
#include "../lib/hashregistry/longtail_blake3_hash_registry.h"
#include "../lib/lrublockstore/longtail_lrublockstore.h"
#include "../lib/lz4/longtail_lz4.h"
#include "../lib/memstorage/longtail_memstorage.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/shareblockstore/longtail_shareblockstore.h"
#include "../lib/zstd/longtail_zstd.h"

#include "../lib/longtail_platform.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define TEST_LOG(fmt, ...) \
    fprintf(stderr, "--- ");fprintf(stderr, fmt, __VA_ARGS__);

static int CreateParentPath(struct Longtail_StorageAPI* storage_api, const char* path)
{
    char* dir_path = Longtail_Strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        Longtail_Free(dir_path);
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        Longtail_Free(dir_path);
        return 1;
    }
    else
    {
        if (!CreateParentPath(storage_api, dir_path))
        {
            TEST_LOG("CreateParentPath failed: `%s`\n", dir_path)
            Longtail_Free(dir_path);
            return 0;
        }
        if (0 == storage_api->CreateDir(storage_api, dir_path))
        {
            Longtail_Free(dir_path);
            return 1;
        }
    }
    TEST_LOG("CreateParentPath failed: `%s`\n", dir_path)
    Longtail_Free(dir_path);
    return 0;
}




static int MakePath(Longtail_StorageAPI* storage_api, const char* path)
{
    char* dir_path = Longtail_Strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        Longtail_Free(dir_path);
        dir_path = 0;
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        Longtail_Free(dir_path);
        return 1;
    }
    else
    {
        if (!MakePath(storage_api, dir_path))
        {
            TEST_LOG("MakePath failed: `%s`\n", dir_path)
            Longtail_Free(dir_path);
            return 0;
        }
        if (0 == storage_api->CreateDir(storage_api, dir_path))
        {
            Longtail_Free(dir_path);
            return 1;
        }
        if (storage_api->IsDir(storage_api, dir_path))
        {
            Longtail_Free(dir_path);
            return 1;
        }
        return 0;
    }
}

static int CreateFakeContent(Longtail_StorageAPI* storage_api, const char* parent_path, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i)
    {
        char path[128];
        sprintf(path, "%s%s%u", parent_path ? parent_path : "", parent_path && parent_path[0] ? "/" : "", i);
        if (0 == MakePath(storage_api, path))
        {
            return 0;
        }
        Longtail_StorageAPI_HOpenFile content_file;
        if (storage_api->OpenWriteFile(storage_api, path, 0, &content_file))
        {
            return 0;
        }
        uint64_t content_size = 64000 + 1 + i;
        char* data = (char*)Longtail_Alloc(sizeof(char) * content_size);
        memset(data, (int)i, content_size);
        int err = storage_api->Write(storage_api, content_file, 0, content_size, data);
        Longtail_Free(data);
        if (err)
        {
            return 0;
        }
        storage_api->CloseFile(storage_api, content_file);
    }
    return 1;
}

struct TestAsyncRetargetContentComplete
{
    struct Longtail_AsyncRetargetContentAPI m_API;
    HLongtail_Sema m_NotifySema;
    TestAsyncRetargetContentComplete()
        : m_Err(EINVAL)
    {
        m_API.m_API.Dispose = 0;
        m_API.OnComplete = OnComplete;
        m_ContentIndex = 0;
        Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &m_NotifySema);
    }
    ~TestAsyncRetargetContentComplete()
    {
        Longtail_DeleteSema(m_NotifySema);
        Longtail_Free(m_NotifySema);
    }

    static void OnComplete(struct Longtail_AsyncRetargetContentAPI* async_complete_api, Longtail_ContentIndex* content_index, int err)
    {
        struct TestAsyncRetargetContentComplete* cb = (struct TestAsyncRetargetContentComplete*)async_complete_api;
        cb->m_Err = err;
        cb->m_ContentIndex = content_index;
        Longtail_PostSema(cb->m_NotifySema, 1);
    }

    void Wait()
    {
        Longtail_WaitSema(m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
    }

    int m_Err;
    Longtail_ContentIndex* m_ContentIndex;
};

static struct Longtail_ContentIndex* SyncRetargetContent(Longtail_BlockStoreAPI* block_store, struct Longtail_ContentIndex* version_content_index)
{
    TestAsyncRetargetContentComplete retarget_content_index_complete;
    if (block_store->RetargetContent(block_store, version_content_index, &retarget_content_index_complete.m_API))
    {
        return 0;
    }
    retarget_content_index_complete.Wait();
    return retarget_content_index_complete.m_ContentIndex;
}

TEST(Longtail, Longtail_Malloc)
{
    void* p = Longtail_Alloc(77);
    ASSERT_NE((void*)0, p);
    Longtail_Free(p);
}

TEST(Longtail, Longtail_LZ4)
{
    Longtail_CompressionAPI* compression_api = Longtail_CreateLZ4CompressionAPI();
    ASSERT_NE((Longtail_CompressionAPI*)0, compression_api);
    uint32_t compression_settings = Longtail_GetLZ4DefaultQuality();

    const char* raw_data =
        "A very long file that should be able to be recreated"
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
        "And in the end it is not the same, it is different, just because why not";

    size_t data_len = strlen(raw_data) + 1;
    size_t compressed_size = 0;
    size_t max_compressed_size = compression_api->GetMaxCompressedSize(compression_api, compression_settings, data_len);
    char* compressed_buffer = (char*)Longtail_Alloc(max_compressed_size);
    ASSERT_NE((char*)0, compressed_buffer);
    ASSERT_EQ(0, compression_api->Compress(compression_api, compression_settings, raw_data, compressed_buffer, data_len, max_compressed_size, &compressed_size));

    char* decompressed_buffer = (char*)Longtail_Alloc(data_len);
    ASSERT_NE((char*)0, decompressed_buffer);
    size_t uncompressed_size;
    ASSERT_EQ(0, compression_api->Decompress(compression_api, compressed_buffer, decompressed_buffer, compressed_size, data_len, &uncompressed_size));
    ASSERT_EQ(data_len, uncompressed_size);
    ASSERT_STREQ(raw_data, decompressed_buffer);
    Longtail_Free(decompressed_buffer);
    Longtail_Free(compressed_buffer);

    Longtail_DisposeAPI(&compression_api->m_API);
}

TEST(Longtail, Longtail_Brotli)
{
    Longtail_CompressionAPI* compression_api = Longtail_CreateBrotliCompressionAPI();
    ASSERT_NE((Longtail_CompressionAPI*)0, compression_api);
    uint32_t compression_settings = Longtail_GetBrotliTextMaxQuality();

    const char* raw_data =
        "A very long file that should be able to be recreated"
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
        "And in the end it is not the same, it is different, just because why not";

    size_t data_len = strlen(raw_data) + 1;
    size_t compressed_size = 0;
    size_t max_compressed_size = compression_api->GetMaxCompressedSize(compression_api, compression_settings, data_len);
    char* compressed_buffer = (char*)Longtail_Alloc(max_compressed_size);
    ASSERT_NE((char*)0, compressed_buffer);
    ASSERT_EQ(0, compression_api->Compress(compression_api, compression_settings, raw_data, compressed_buffer, data_len, max_compressed_size, &compressed_size));

    char* decompressed_buffer = (char*)Longtail_Alloc(data_len);
    ASSERT_NE((char*)0, decompressed_buffer);
    size_t uncompressed_size;
    ASSERT_EQ(0, compression_api->Decompress(compression_api, compressed_buffer, decompressed_buffer, compressed_size, data_len, &uncompressed_size));
    ASSERT_EQ(data_len, uncompressed_size);
    ASSERT_STREQ(raw_data, decompressed_buffer);
    Longtail_Free(decompressed_buffer);
    Longtail_Free(compressed_buffer);

    Longtail_DisposeAPI(&compression_api->m_API);
}

TEST(Longtail, Longtail_ZStd)
{
    Longtail_CompressionAPI* compression_api = Longtail_CreateZStdCompressionAPI();
    ASSERT_NE((Longtail_CompressionAPI*)0, compression_api);
    uint32_t compression_settings = Longtail_GetZStdMaxQuality();

    const char* raw_data =
        "A very long file that should be able to be recreated"
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
        "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
        "And in the end it is not the same, it is different, just because why not";

    size_t data_len = strlen(raw_data) + 1;
    size_t compressed_size = 0;
    size_t max_compressed_size = compression_api->GetMaxCompressedSize(compression_api, compression_settings, data_len);
    char* compressed_buffer = (char*)Longtail_Alloc(max_compressed_size);
    ASSERT_NE((char*)0, compressed_buffer);
    ASSERT_EQ(0, compression_api->Compress(compression_api, compression_settings, raw_data, compressed_buffer, data_len, max_compressed_size, &compressed_size));

    char* decompressed_buffer = (char*)Longtail_Alloc(data_len);
    ASSERT_NE((char*)0, decompressed_buffer);
    size_t uncompressed_size;
    ASSERT_EQ(0, compression_api->Decompress(compression_api, compressed_buffer, decompressed_buffer, compressed_size, data_len, &uncompressed_size));
    ASSERT_EQ(data_len, uncompressed_size);
    ASSERT_STREQ(raw_data, decompressed_buffer);
    Longtail_Free(decompressed_buffer);
    Longtail_Free(compressed_buffer);

    Longtail_DisposeAPI(&compression_api->m_API);
}

TEST(Longtail, Longtail_Blake2)
{
    const char* test_string = "This is the first test string which is fairly long and should - reconstructed properly, than you very much";
    struct Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    uint64_t hash;
    ASSERT_EQ(0, hash_api->HashBuffer(hash_api, (uint32_t)(strlen(test_string) + 1), test_string, &hash));
    ASSERT_EQ(0xd336e5afa4fa1f4d, hash);
    Longtail_DisposeAPI(&hash_api->m_API);
}

TEST(Longtail, Longtail_Blake3)
{
    const char* test_string = "This is the first test string which is fairly long and should - reconstructed properly, than you very much";
    struct Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    uint64_t hash;
    ASSERT_EQ(0, hash_api->HashBuffer(hash_api, (uint32_t)(strlen(test_string) + 1), test_string, &hash));
    ASSERT_EQ(0xd38bbe79f1f03fda, hash);
    Longtail_DisposeAPI(&hash_api->m_API);
}

TEST(Longtail, Longtail_MeowHash)
{
    const char* test_string = "This is the first test string which is fairly long and should - reconstructed properly, than you very much";
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    uint64_t hash;
    ASSERT_EQ(0, hash_api->HashBuffer(hash_api, (uint32_t)(strlen(test_string) + 1), test_string, &hash));
    ASSERT_EQ(0x4edc68dac105c4ee, hash);
    Longtail_DisposeAPI(&hash_api->m_API);
}

TEST(Longtail, Longtail_CreateBlockIndex)
{
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    const uint64_t chunk_indexes[2] = {0, 1};
    const TLongtail_Hash chunk_hashes[2] = {0xdeadbeeffeed5a17, 0xfeed5a17deadbeef};
    const uint32_t chunk_sizes[2] = {4711, 1147};
    struct Longtail_BlockIndex* block_index;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0,
        2,
        chunk_indexes,
        chunk_hashes,
        chunk_sizes,
        &block_index));
    ASSERT_NE(0u, *block_index->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *block_index->m_HashIdentifier);
    ASSERT_EQ(2u, *block_index->m_ChunkCount);
    ASSERT_EQ(chunk_hashes[0], block_index->m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[1], block_index->m_ChunkHashes[1]);
    ASSERT_EQ(chunk_sizes[0], block_index->m_ChunkSizes[0]);
    ASSERT_EQ(chunk_sizes[1], block_index->m_ChunkSizes[1]);
    Longtail_Free(block_index);
    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_ReadWriteBlockIndexInBuffer)
{
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    const uint64_t chunk_indexes[2] = {0, 1};
    const TLongtail_Hash chunk_hashes[2] = {0xdeadbeeffeed5a17, 0xfeed5a17deadbeef};
    const uint32_t chunk_sizes[2] = {4711, 1147};
    struct Longtail_BlockIndex* block_index;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0,
        2,
        chunk_indexes,
        chunk_hashes,
        chunk_sizes,
        &block_index));

    void* buffer;
    size_t size;
    ASSERT_EQ(0, Longtail_WriteBlockIndexToBuffer(
        block_index,
        &buffer,
        &size));
    Longtail_Free(block_index);

    struct Longtail_BlockIndex* block_index_copy;
    ASSERT_EQ(0, Longtail_ReadBlockIndexFromBuffer(
        buffer,
        size,
        &block_index_copy));
    
    Longtail_Free(buffer);

    ASSERT_NE(0u, *block_index_copy->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *block_index_copy->m_HashIdentifier);
    ASSERT_EQ(2u, *block_index_copy->m_ChunkCount);
    ASSERT_EQ(chunk_hashes[0], block_index_copy->m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[1], block_index_copy->m_ChunkHashes[1]);
    ASSERT_EQ(chunk_sizes[0], block_index_copy->m_ChunkSizes[0]);
    ASSERT_EQ(chunk_sizes[1], block_index_copy->m_ChunkSizes[1]);
    Longtail_Free(block_index_copy);
    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_ReadWriteStoredBlockBuffer)
{
    TLongtail_Hash block_hash = 0x77aa661199bb0011;
    const uint32_t chunk_count = 4;
    TLongtail_Hash chunk_hashes[chunk_count] = {0xdeadbeefbeefdead, 0xa11ab011a66a5a55, 0xeadbeefbeefdeadd, 0x11ab011a66a5a55a};
    uint32_t chunk_sizes[chunk_count] = {10, 20, 30, 40};
    uint32_t block_data_size = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        block_data_size += chunk_sizes[c];
    }
    struct Longtail_StoredBlock* stored_block;
    ASSERT_EQ(0, Longtail_CreateStoredBlock(
        block_hash,
        0xdeadbeef,
        4,
        0,
        chunk_hashes,
        chunk_sizes,
        block_data_size,
        &stored_block));
    uint32_t offset = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        for (uint32_t b = 0; b < chunk_sizes[c]; ++b)
        {
            ((uint8_t*)stored_block->m_BlockData)[offset +b] = c + 1;
        }
        offset += chunk_sizes[c];
    }

    void* block_index_buffer;
    size_t block_index_buffer_size;
    ASSERT_EQ(0, Longtail_WriteBlockIndexToBuffer(
        stored_block->m_BlockIndex,
        &block_index_buffer,
        &block_index_buffer_size));

    size_t stored_block_data_size = block_index_buffer_size + stored_block->m_BlockChunksDataSize;
    void* stored_block_data_buffer = Longtail_Alloc(stored_block_data_size);
    memcpy(stored_block_data_buffer, block_index_buffer, block_index_buffer_size);
    memcpy(&((uint8_t*)stored_block_data_buffer)[block_index_buffer_size], stored_block->m_BlockData, stored_block->m_BlockChunksDataSize);

    Longtail_Free(block_index_buffer);
    block_index_buffer = 0;
    Longtail_Free(stored_block);
    stored_block = 0;

    size_t stored_block_size = Longtail_GetStoredBlockSize(stored_block_data_size);
    stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(stored_block_size);
    void* block_data = &((uint8_t*)stored_block)[stored_block_size - stored_block_data_size];
    memcpy(block_data, stored_block_data_buffer, stored_block_data_size);
    Longtail_Free(stored_block_data_buffer);
    stored_block_data_buffer = 0;

    ASSERT_EQ(0, Longtail_InitStoredBlockFromData(
        stored_block,
        block_data,
        stored_block_data_size));

    stored_block->Dispose = (int (*)(struct Longtail_StoredBlock* stored_block))Longtail_Free;

    ASSERT_EQ(block_data_size, stored_block->m_BlockChunksDataSize);
    ASSERT_EQ(block_hash, *stored_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(0xdeadbeef, *stored_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(chunk_count, *stored_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, *stored_block->m_BlockIndex->m_Tag);
    offset = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        ASSERT_EQ(chunk_hashes[c], stored_block->m_BlockIndex->m_ChunkHashes[c]);
        ASSERT_EQ(chunk_sizes[c], stored_block->m_BlockIndex->m_ChunkSizes[c]);
        for (uint32_t b = 0; b < chunk_sizes[c]; ++b)
        {
            ASSERT_EQ(((uint8_t*)stored_block->m_BlockData)[offset +b], c + 1);
        }
        offset += chunk_sizes[c];
    }
    stored_block->Dispose(stored_block);
}
/*
TEST(Longtail, Longtail_VersionIndex)
{
    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const uint64_t asset_sizes[5] = {64003u, 64003u, 64002u, 64001u, 64001u};
    const uint16_t asset_permissions[5] = {0644, 0644, 0644, 0644, 0644};
    const uint32_t chunk_sizes[5] = {64003u, 64003u, 64002u, 64001u, 64001u};
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};
    const uint32_t asset_tags[5] = {0, 0, 0, 0, 0};

    Longtail_FileInfos* file_infos;
    ASSERT_EQ(0, Longtail_MakeFileInfos(5, asset_paths, asset_sizes, asset_permissions, &file_infos));
    size_t version_index_size = Longtail_GetVersionIndexSize(5, 5, 5, file_infos->m_PathDataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);

    static const uint32_t TARGET_CHUNK_SIZE = 32768u;

    Longtail_VersionIndex* version_index;
    ASSERT_EQ(0, Longtail_BuildVersionIndex(
        version_index_mem,
        version_index_size,
        file_infos,
        asset_path_hashes,
        asset_content_hashes,
        asset_chunk_start_index,
        asset_chunk_counts,
        file_infos->m_Count,
        asset_chunk_start_index,
        file_infos->m_Count,
        chunk_sizes,
        asset_content_hashes,
        asset_tags,
        0u, // Dummy hash identifier
        TARGET_CHUNK_SIZE,
        &version_index));

    void* store_buffer = 0;
    size_t store_size = 0;
    ASSERT_EQ(0, Longtail_WriteVersionIndexToBuffer(version_index, &store_buffer, &store_size));
    Longtail_VersionIndex* version_index_copy;
    ASSERT_EQ(0, Longtail_ReadVersionIndexFromBuffer(store_buffer, store_size, &version_index_copy));
    ASSERT_EQ(*version_index->m_AssetCount, *version_index_copy->m_AssetCount);
    Longtail_Free(version_index_copy);
    Longtail_Free(store_buffer);

    Longtail_Free(version_index);
    Longtail_Free(file_infos);
}
*/
TEST(Longtail, Longtail_ContentIndex)
{
//    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
//    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = { 43593u, 43593u, 43592u, 43591u, 43591u };
    const uint32_t asset_tags[5] = {0, 0, 0, 0, 0};
//    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
//    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    ASSERT_NE((Longtail_HashAPI*)0, hash_api);
    Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        asset_count,
        asset_content_hashes,
        asset_sizes,
        asset_tags,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    ASSERT_EQ(2u, *content_index->m_BlockCount);
    ASSERT_EQ(5u, *content_index->m_ChunkCount);
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        ASSERT_EQ(asset_content_hashes[i], content_index->m_ChunkHashes[i]);
    }
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[1]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[4]);

    void* store_buffer = 0;
    size_t store_size = 0;
    ASSERT_EQ(0, Longtail_WriteContentIndexToBuffer(content_index, &store_buffer, &store_size));
    Longtail_ContentIndex* content_index_copy;
    ASSERT_EQ(0, Longtail_ReadContentIndexFromBuffer(store_buffer, store_size, &content_index_copy));
    ASSERT_EQ(*content_index->m_BlockCount, *content_index_copy->m_BlockCount);
    ASSERT_EQ(*content_index->m_ChunkCount, *content_index_copy->m_ChunkCount);
    Longtail_Free(content_index_copy);
    Longtail_Free(store_buffer);

    Longtail_Free(content_index);

    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_CreateStoreIndexFromBlocks)
{
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    const uint64_t chunk_indexes[4] = {0, 1, 2, 3};
    const TLongtail_Hash chunk_hashes[4] = {0xdeadbeeffeed5a17, 0xfeed5a17deadbeef, 0xaeed5a17deadbeea, 0xdaedbeeffeed5a57};
    const uint32_t chunk_sizes[4] = {4711, 1147, 1137, 3219};
    struct Longtail_BlockIndex* block_index1;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0x3127841,
        2,
        &chunk_indexes[0],
        chunk_hashes,
        chunk_sizes,
        &block_index1));

    struct Longtail_BlockIndex* block_index2;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0x3127841,
        2,
        &chunk_indexes[2],
        chunk_hashes,
        chunk_sizes,
        &block_index2));

    const struct Longtail_BlockIndex* block_indexes[2] = {block_index1, block_index2};

    struct Longtail_StoreIndex* store_index;
    ASSERT_EQ(0, Longtail_CreateStoreIndexFromBlocks(
        2,
        (const struct Longtail_BlockIndex**)block_indexes,
        &store_index));

    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *store_index->m_HashIdentifier);
    ASSERT_EQ(2, *store_index->m_BlockCount);
    ASSERT_EQ(4, *store_index->m_ChunkCount);
    ASSERT_EQ(*block_index1->m_BlockHash, store_index->m_BlockHashes[0]);
    ASSERT_EQ(*block_index1->m_Tag, store_index->m_BlockTags[0]);
    ASSERT_EQ(0, store_index->m_BlockChunksOffsets[0]);
    ASSERT_EQ(2, store_index->m_BlockChunkCounts[0]);
    ASSERT_EQ(*block_index2->m_BlockHash, store_index->m_BlockHashes[1]);
    ASSERT_EQ(*block_index2->m_Tag, store_index->m_BlockTags[1]);
    ASSERT_EQ(2, store_index->m_BlockChunksOffsets[1]);
    ASSERT_EQ(2, store_index->m_BlockChunkCounts[1]);
    ASSERT_EQ(chunk_hashes[0], store_index->m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[1], store_index->m_ChunkHashes[1]);
    ASSERT_EQ(chunk_hashes[2], store_index->m_ChunkHashes[2]);
    ASSERT_EQ(chunk_hashes[3], store_index->m_ChunkHashes[3]);


    struct Longtail_BlockIndex restored_block_indexes[2];
    ASSERT_EQ(0, Longtail_MakeBlockIndex(store_index, 0, &restored_block_indexes[0]));
    ASSERT_EQ(0, Longtail_MakeBlockIndex(store_index, 1, &restored_block_indexes[1]));

    ASSERT_EQ(*block_index1->m_BlockHash, *restored_block_indexes[0].m_BlockHash);
    ASSERT_EQ(*block_index1->m_HashIdentifier, *restored_block_indexes[0].m_HashIdentifier);
    ASSERT_EQ(2, *restored_block_indexes[0].m_ChunkCount);
    ASSERT_EQ(*block_index1->m_Tag, *restored_block_indexes[0].m_Tag);
    ASSERT_EQ(chunk_hashes[0], restored_block_indexes[0].m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[1], restored_block_indexes[0].m_ChunkHashes[1]);

    ASSERT_EQ(*block_index2->m_BlockHash, *restored_block_indexes[1].m_BlockHash);
    ASSERT_EQ(*block_index2->m_HashIdentifier, *restored_block_indexes[1].m_HashIdentifier);
    ASSERT_EQ(2, *restored_block_indexes[1].m_ChunkCount);
    ASSERT_EQ(*block_index2->m_Tag, *restored_block_indexes[1].m_Tag);
    ASSERT_EQ(chunk_hashes[2], restored_block_indexes[1].m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[3], restored_block_indexes[1].m_ChunkHashes[1]);

    Longtail_Free(store_index);
    Longtail_Free(block_index2);
    Longtail_Free(block_index1);
    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_MergeStoreIndex)
{
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    ASSERT_NE((struct Longtail_HashAPI*)0, hash_api);
    const uint64_t chunk_indexes[5] = {0, 1, 2, 3, 4};
    const TLongtail_Hash chunk_hashes[5] = {0xdeadbeeffeed5a17, 0xfeed5a17deadbeef, 0xaeed5a17deadbeea, 0xdaedbeeffeed5a57, 0xfeed5a17deadbeef};
    const uint32_t chunk_sizes[5] = {4711, 1147, 1137, 3219, 1147};
    struct Longtail_BlockIndex* block_index1;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0x3127841,
        2,
        &chunk_indexes[0],
        chunk_hashes,
        chunk_sizes,
        &block_index1));

    struct Longtail_BlockIndex* block_index2;
    ASSERT_EQ(0, Longtail_CreateBlockIndex(
        hash_api,
        0x3127841,
        3,
        &chunk_indexes[2],
        chunk_hashes,
        chunk_sizes,
        &block_index2));

    struct Longtail_StoreIndex* store_index_local;
    ASSERT_EQ(0, Longtail_CreateStoreIndexFromBlocks(
        1,
        (const struct Longtail_BlockIndex** )&block_index1,
        &store_index_local));

    struct Longtail_StoreIndex* store_index_remote;
    ASSERT_EQ(0, Longtail_CreateStoreIndexFromBlocks(
        1,
        (const struct Longtail_BlockIndex** )&block_index2,
        &store_index_remote));

    struct Longtail_StoreIndex* store_index;
    ASSERT_EQ(0, Longtail_MergeStoreIndex(
        store_index_local,
        store_index_remote,
        &store_index));

    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *store_index->m_HashIdentifier);
    ASSERT_EQ(2, *store_index->m_BlockCount);
    ASSERT_EQ(5, *store_index->m_ChunkCount);
    ASSERT_EQ(*block_index1->m_BlockHash, store_index->m_BlockHashes[0]);
    ASSERT_EQ(*block_index1->m_Tag, store_index->m_BlockTags[0]);
    ASSERT_EQ(0, store_index->m_BlockChunksOffsets[0]);
    ASSERT_EQ(2, store_index->m_BlockChunkCounts[0]);
    ASSERT_EQ(*block_index2->m_BlockHash, store_index->m_BlockHashes[1]);
    ASSERT_EQ(*block_index2->m_Tag, store_index->m_BlockTags[1]);
    ASSERT_EQ(2, store_index->m_BlockChunksOffsets[1]);
    ASSERT_EQ(3, store_index->m_BlockChunkCounts[1]);
    ASSERT_EQ(chunk_hashes[0], store_index->m_ChunkHashes[0]);
    ASSERT_EQ(chunk_hashes[1], store_index->m_ChunkHashes[1]);
    ASSERT_EQ(chunk_hashes[2], store_index->m_ChunkHashes[2]);
    ASSERT_EQ(chunk_hashes[3], store_index->m_ChunkHashes[3]);
    ASSERT_EQ(chunk_hashes[4], store_index->m_ChunkHashes[1]);

    Longtail_Free(store_index);
    Longtail_Free(store_index_remote);
    Longtail_Free(store_index_local);
    Longtail_Free(block_index2);
    Longtail_Free(block_index1);
    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_RetargetContentIndex)
{
//    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const uint32_t asset_sizes[5] = { 43593u, 43593u, 43592u, 43591u, 43591u };
    const uint32_t asset_tags[5] = {0, 0, 0, 0, 0};

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    ASSERT_NE((Longtail_HashAPI*)0, hash_api);
    Longtail_HashAPI_HContext c;
    int err = hash_api->BeginContext(hash_api, &c);
    ASSERT_EQ(0, err);
    ASSERT_NE((Longtail_HashAPI_HContext)0, c);
    hash_api->EndContext(hash_api, c);
    Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        asset_count,
        asset_content_hashes,
        asset_sizes,
        asset_tags,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    ASSERT_EQ(2u, *content_index->m_BlockCount);
    ASSERT_EQ(5u, *content_index->m_ChunkCount);
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        ASSERT_EQ(asset_content_hashes[i], content_index->m_ChunkHashes[i]);
    }
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[1]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[4]);

    Longtail_ContentIndex* other_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        asset_count - 1,
        &asset_content_hashes[1],
        &asset_sizes[1],
        &asset_tags[1],
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &other_content_index));

    Longtail_ContentIndex* retargetted_content_index;
    ASSERT_EQ(0, Longtail_RetargetContent(
        content_index,
        other_content_index,
        &retargetted_content_index));
    Longtail_Free(retargetted_content_index);
    Longtail_Free(other_content_index);
    Longtail_Free(content_index);

    SAFE_DISPOSE_API(hash_api);
}

static uint32_t* GetAssetTags(Longtail_StorageAPI* , const Longtail_FileInfos* file_infos)
{
    uint32_t count = file_infos->m_Count;
    uint32_t* result = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * count);
    const uint32_t compression_types[4] = {
        0,
        Longtail_GetBrotliGenericDefaultQuality(),
        Longtail_GetLZ4DefaultQuality(),
        Longtail_GetZStdDefaultQuality()};
    for (uint32_t i = 0; i < count; ++i)
    {
        result[i] = compression_types[i % 4];
    }
    return result;
}

static uint32_t* SetAssetTags(Longtail_StorageAPI* , const Longtail_FileInfos* file_infos, uint32_t compression_type)
{
    uint32_t count = file_infos->m_Count;
    uint32_t* result = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * count);
    for (uint32_t i = 0; i < count; ++i)
    {
        result[i] = compression_type;
    }
    return result;
}

TEST(Longtail, CreateEmptyVersionIndex)
{
    Longtail_StorageAPI* local_storage = Longtail_CreateFSStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, 0, 0, 0, "data/non-existent", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* compression_types = GetAssetTags(local_storage, version1_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "source/version1",
        version1_paths,
        compression_types,
        16384,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(vindex);
    Longtail_Free(compression_types);
    Longtail_Free(version1_paths);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(local_storage);
}

TEST(Longtail, ContentIndexSerialization)
{
    Longtail_StorageAPI* local_storage = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);

    ASSERT_EQ(1, CreateFakeContent(local_storage, "source/version1/two_items", 2));
    ASSERT_EQ(1, CreateFakeContent(local_storage, "source/version1/five_items", 5));
    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, 0, 0, 0, "source/version1", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* compression_types = GetAssetTags(local_storage, version1_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "source/version1",
        version1_paths,
        compression_types,
        16384,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(compression_types);
    Longtail_Free(version1_paths);

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        vindex,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    Longtail_Free(vindex);
    vindex = 0;

    ASSERT_EQ(0, Longtail_WriteContentIndex(local_storage, cindex, "cindex.lci"));

    Longtail_ContentIndex* cindex2;
    ASSERT_EQ(0, Longtail_ReadContentIndex(local_storage, "cindex.lci", &cindex2));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex2);

    ASSERT_EQ(*cindex->m_BlockCount, *cindex2->m_BlockCount);
    for (uint64_t i = 0; i < *cindex->m_BlockCount; ++i)
    {
        ASSERT_EQ(cindex->m_BlockHashes[i], cindex2->m_BlockHashes[i]);
    }
    ASSERT_EQ(*cindex->m_ChunkCount, *cindex2->m_ChunkCount);
    for (uint64_t i = 0; i < *cindex->m_ChunkCount; ++i)
    {
        ASSERT_EQ(cindex->m_ChunkBlockIndexes[i], cindex2->m_ChunkBlockIndexes[i]);
    }

    Longtail_Free(cindex);
    cindex = 0;

    Longtail_Free(cindex2);
    cindex2 = 0;

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(local_storage);
}

TEST(Longtail, Longtail_CreateStoredBlock)
{
    TLongtail_Hash block_hash = 0x77aa661199bb0011;
    const uint32_t chunk_count = 4;
    TLongtail_Hash chunk_hashes[chunk_count] = {0xdeadbeefbeefdead, 0xa11ab011a66a5a55, 0xeadbeefbeefdeadd, 0x11ab011a66a5a55a};
    uint32_t chunk_sizes[chunk_count] = {10, 20, 30, 40};
    uint32_t block_data_size = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        block_data_size += chunk_sizes[c];
    }
    struct Longtail_StoredBlock* stored_block;
    ASSERT_EQ(0, Longtail_CreateStoredBlock(
        block_hash,
        0xdeadbeef,
        4,
        0,
        chunk_hashes,
        chunk_sizes,
        block_data_size,
        &stored_block));
    uint32_t offset = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        for (uint32_t b = 0; b < chunk_sizes[c]; ++b)
        {
            ((uint8_t*)stored_block->m_BlockData)[offset +b] = c + 1;
        }
        offset += chunk_sizes[c];
    }

    ASSERT_EQ(block_data_size, stored_block->m_BlockChunksDataSize);
    ASSERT_EQ(block_hash, *stored_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(0xdeadbeef, *stored_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(chunk_count, *stored_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, *stored_block->m_BlockIndex->m_Tag);
    offset = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        ASSERT_EQ(chunk_hashes[c], stored_block->m_BlockIndex->m_ChunkHashes[c]);
        ASSERT_EQ(chunk_sizes[c], stored_block->m_BlockIndex->m_ChunkSizes[c]);
        for (uint32_t b = 0; b < chunk_sizes[c]; ++b)
        {
            ASSERT_EQ(((uint8_t*)stored_block->m_BlockData)[offset +b], c + 1);
        }
        offset += chunk_sizes[c];
    }
    stored_block->Dispose(stored_block);
}

struct TestAsyncPutBlockComplete
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    HLongtail_Sema m_NotifySema;
    TestAsyncPutBlockComplete()
        : m_Err(EINVAL)
    {
        m_API.m_API.Dispose = 0;
        m_API.OnComplete = OnComplete;
        Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &m_NotifySema);
    }
    ~TestAsyncPutBlockComplete()
    {
        Longtail_DeleteSema(m_NotifySema);
        Longtail_Free(m_NotifySema);
    }

    static void OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
    {
        struct TestAsyncPutBlockComplete* cb = (struct TestAsyncPutBlockComplete*)async_complete_api;
        cb->m_Err = err;
        Longtail_PostSema(cb->m_NotifySema, 1);
    }

    void Wait()
    {
        Longtail_WaitSema(m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
    }

    int m_Err;

};

struct TestAsyncGetBlockComplete
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    HLongtail_Sema m_NotifySema;
    TestAsyncGetBlockComplete()
        : m_Err(EINVAL)
    {
        m_API.m_API.Dispose = 0;
        m_API.OnComplete = OnComplete;
        m_StoredBlock = 0;
        Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &m_NotifySema);
    }
    ~TestAsyncGetBlockComplete()
    {
        Longtail_DeleteSema(m_NotifySema);
        Longtail_Free(m_NotifySema);
    }

    static void OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, Longtail_StoredBlock* stored_block, int err)
    {
        struct TestAsyncGetBlockComplete* cb = (struct TestAsyncGetBlockComplete*)async_complete_api;
        cb->m_Err = err;
        cb->m_StoredBlock = stored_block;
        Longtail_PostSema(cb->m_NotifySema, 1);
    }

    void Wait()
    {
        Longtail_WaitSema(m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
    }

    int m_Err;
    Longtail_StoredBlock* m_StoredBlock;
};


struct TestAsyncFlushComplete
{
    struct Longtail_AsyncFlushAPI m_API;
    HLongtail_Sema m_NotifySema;
    TestAsyncFlushComplete()
        : m_Err(EINVAL)
    {
        m_API.m_API.Dispose = 0;
        m_API.OnComplete = OnComplete;
        Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &m_NotifySema);
    }
    ~TestAsyncFlushComplete()
    {
        Longtail_DeleteSema(m_NotifySema);
        Longtail_Free(m_NotifySema);
    }

    static void OnComplete(struct Longtail_AsyncFlushAPI* async_complete_api, int err)
    {
        struct TestAsyncFlushComplete* cb = (struct TestAsyncFlushComplete*)async_complete_api;
        cb->m_Err = err;
        Longtail_PostSema(cb->m_NotifySema, 1);
    }

    void Wait()
    {
        Longtail_WaitSema(m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
    }

    int m_Err;
};

TEST(Longtail, Longtail_FSBlockStore)
{
    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", 524288, 1024, 0);

    Longtail_StoredBlock put_block;
    put_block.Dispose = 0;
    put_block.m_BlockIndex = 0;
    put_block.m_BlockData = 0;

    TestAsyncGetBlockComplete getCB;
    ASSERT_EQ(ENOENT, block_store_api->GetStoredBlock(block_store_api, 4711, &getCB.m_API));
    struct Longtail_StoredBlock* get_block = getCB.m_StoredBlock;

    size_t block_index_size = Longtail_GetBlockIndexSize(2);
    void* block_index_mem = Longtail_Alloc(block_index_size);
    put_block.m_BlockIndex = Longtail_InitBlockIndex(block_index_mem, 2);
    *put_block.m_BlockIndex->m_BlockHash = 0xdeadbeef;
    *put_block.m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
    *put_block.m_BlockIndex->m_Tag = 0;
    put_block.m_BlockIndex->m_ChunkHashes[0] = 0xf001fa5;
    put_block.m_BlockIndex->m_ChunkHashes[1] = 0xfff1fa5;
    put_block.m_BlockIndex->m_ChunkSizes[0] = 4711;
    put_block.m_BlockIndex->m_ChunkSizes[1] = 1147;
    *put_block.m_BlockIndex->m_ChunkCount = 2;
    put_block.m_BlockChunksDataSize = 4711 + 1147;

    put_block.m_BlockData = Longtail_Alloc(put_block.m_BlockChunksDataSize);
    memset(put_block.m_BlockData, 77, 4711);
    memset(&((uint8_t*)put_block.m_BlockData)[4711], 13, 1147);

    TestAsyncPutBlockComplete putCB;
    ASSERT_EQ(0, block_store_api->PutStoredBlock(block_store_api, &put_block, &putCB.m_API));
    putCB.Wait();
    Longtail_Free(put_block.m_BlockIndex);
    ASSERT_EQ(0, putCB.m_Err);

    struct TestAsyncGetBlockComplete getCB1;
    ASSERT_EQ(0, block_store_api->GetStoredBlock(block_store_api, 0xdeadbeef, &getCB1.m_API));
    getCB1.Wait();
    get_block = getCB1.m_StoredBlock;
    ASSERT_EQ(0, getCB1.m_Err);

    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, memcmp(put_block.m_BlockData, get_block->m_BlockData, put_block.m_BlockChunksDataSize));
    Longtail_Free(put_block.m_BlockData);
    get_block->Dispose(get_block);

    TestAsyncFlushComplete flushCB;
    ASSERT_EQ(0, block_store_api->Flush(block_store_api, &flushCB.m_API));
    flushCB.Wait();
    ASSERT_EQ(0, flushCB.m_Err);

    Longtail_BlockStore_Stats stats;
    block_store_api->GetStats(block_store_api, &stats);

    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(1, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5902, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(5902, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, Longtail_FSBlockStoreReadContent)
{
    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", 524288, 1024, 0);

    Longtail_StoredBlock put_block;
    put_block.Dispose = 0;
    put_block.m_BlockIndex = 0;
    put_block.m_BlockData = 0;

    TestAsyncGetBlockComplete getCB;
    ASSERT_EQ(ENOENT, block_store_api->GetStoredBlock(block_store_api, 4711, &getCB.m_API));
    struct Longtail_StoredBlock* get_block = getCB.m_StoredBlock;

    size_t block_index_size = Longtail_GetBlockIndexSize(2);
    void* block_index_mem = Longtail_Alloc(block_index_size);
    put_block.m_BlockIndex = Longtail_InitBlockIndex(block_index_mem, 2);
    *put_block.m_BlockIndex->m_BlockHash = 0xdeadbeef;
    *put_block.m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
    *put_block.m_BlockIndex->m_Tag = 0;
    put_block.m_BlockIndex->m_ChunkHashes[0] = 0xf001fa5;
    put_block.m_BlockIndex->m_ChunkHashes[1] = 0xfff1fa5;
    put_block.m_BlockIndex->m_ChunkSizes[0] = 4711;
    put_block.m_BlockIndex->m_ChunkSizes[1] = 1147;
    *put_block.m_BlockIndex->m_ChunkCount = 2;
    put_block.m_BlockChunksDataSize = 4711 + 1147;

    put_block.m_BlockData = Longtail_Alloc(put_block.m_BlockChunksDataSize);
    memset(put_block.m_BlockData, 77, 4711);
    memset(&((uint8_t*)put_block.m_BlockData)[4711], 13, 1147);

    TestAsyncPutBlockComplete putCB;
    ASSERT_EQ(0, block_store_api->PutStoredBlock(block_store_api, &put_block, &putCB.m_API));
    putCB.Wait();
    Longtail_Free(put_block.m_BlockIndex);
    ASSERT_EQ(0, putCB.m_Err);

    struct TestAsyncGetBlockComplete getCB1;
    ASSERT_EQ(0, block_store_api->GetStoredBlock(block_store_api, 0xdeadbeef, &getCB1.m_API));
    getCB1.Wait();
    get_block = getCB1.m_StoredBlock;
    ASSERT_EQ(0, getCB1.m_Err);

    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, memcmp(put_block.m_BlockData, get_block->m_BlockData, put_block.m_BlockChunksDataSize));
    get_block->Dispose(get_block);

    Longtail_BlockStore_Stats stats;
    block_store_api->GetStats(block_store_api, &stats);

    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(1, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(2, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5902, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(5902, stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    SAFE_DISPOSE_API(block_store_api);

    block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", 524288, 1024, 0);
    struct TestAsyncGetBlockComplete getCB2;
    ASSERT_EQ(0, block_store_api->GetStoredBlock(block_store_api, 0xdeadbeef, &getCB2.m_API));
    getCB2.Wait();
    get_block = getCB2.m_StoredBlock;

    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, memcmp(put_block.m_BlockData, get_block->m_BlockData, put_block.m_BlockChunksDataSize));
    get_block->Dispose(get_block);
    SAFE_DISPOSE_API(block_store_api);

    storage_api->RemoveFile(storage_api, "chunks/store.lci");

    block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", 524288, 1024, 0);
    struct TestAsyncGetBlockComplete getCB4;
    ASSERT_EQ(0, block_store_api->GetStoredBlock(block_store_api, 0xdeadbeef, &getCB4.m_API));
    getCB4.Wait();
    get_block = getCB4.m_StoredBlock;

    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, memcmp(put_block.m_BlockData, get_block->m_BlockData, put_block.m_BlockChunksDataSize));
    Longtail_Free(put_block.m_BlockData);
    get_block->Dispose(get_block);
    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

static uint8_t* GenerateRandomData(uint8_t* data, size_t size)
{
    for (size_t n = 0; n < size; n++) {
        int r = rand();
        int key = r & 0xff;
        data[n] = (uint8_t)key;
    }
    return data;
}

static int GeneratedStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free(stored_block);
    return 0;
}

static Longtail_StoredBlock* GenerateStoredBlock(struct Longtail_HashAPI* hash_api, uint32_t chunk_count, const uint32_t* chunk_sizes)
{
    uint32_t block_chunks_data_size = 0;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        block_chunks_data_size += chunk_sizes[c];
    }
    size_t block_index_size = Longtail_GetBlockIndexSize(chunk_count);
    size_t block_size = sizeof(struct Longtail_StoredBlock) + block_index_size + block_chunks_data_size;

    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_size);
    if (!stored_block)
    {
        return 0;
    }
    stored_block->Dispose = GeneratedStoredBlock_Dispose;
    stored_block->m_BlockChunksDataSize = block_chunks_data_size;
    stored_block->m_BlockIndex = Longtail_InitBlockIndex(&stored_block[1], chunk_count);
    stored_block->m_BlockData = &((uint8_t*)stored_block->m_BlockIndex)[block_index_size];
    uint8_t* chunk_data = (uint8_t*)stored_block->m_BlockData;
    for (uint32_t c = 0; c < chunk_count; ++c)
    {
        stored_block->m_BlockIndex->m_ChunkSizes[c] = chunk_sizes[c];
        GenerateRandomData(chunk_data, chunk_sizes[c]);
        hash_api->HashBuffer(hash_api, chunk_sizes[c], chunk_data, &stored_block->m_BlockIndex->m_ChunkHashes[c]);
        chunk_data += chunk_sizes[c];
    }
    size_t hash_buffer_size = sizeof(TLongtail_Hash) * chunk_count;
    hash_api->HashBuffer(hash_api, (uint32_t)(hash_buffer_size), (void*)stored_block->m_BlockIndex->m_ChunkHashes, stored_block->m_BlockIndex->m_BlockHash);
    *stored_block->m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
    *stored_block->m_BlockIndex->m_ChunkCount = chunk_count;
    *stored_block->m_BlockIndex->m_Tag = 0;
    return stored_block;
}


TEST(Longtail, Longtail_TestLRUBlockStore)
{
    Longtail_StorageAPI* local_storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, local_storage_api, "chunks", 524288, 1024, 0);
    Longtail_BlockStoreAPI* lru_block_store_api = Longtail_CreateLRUBlockStoreAPI(local_block_store_api, 3);

    static const uint32_t BLOCK_COUNT = 7;
    TLongtail_Hash block_hashes[BLOCK_COUNT];

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 2;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {1244, 4323};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[0] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 1;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {124};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[1] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 3;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {144, 423, 1239};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[2] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 3;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {1, 1244, 4323};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[3] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 1;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {124444};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[4] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 2;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {124, 323};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[5] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    {
        static const uint32_t BLOCK_CHUNK_COUNT = 4;
        static const uint32_t BLOCK_CHUNK_SIZES[BLOCK_CHUNK_COUNT] = {624, 885, 81, 771};
        static Longtail_StoredBlock* block = GenerateStoredBlock(hash_api, BLOCK_CHUNK_COUNT, BLOCK_CHUNK_SIZES);
        struct TestAsyncPutBlockComplete putCB;
        ASSERT_EQ(0, lru_block_store_api->PutStoredBlock(lru_block_store_api, block, &putCB.m_API));
        putCB.Wait();
        ASSERT_EQ(0, putCB.m_Err);
        block_hashes[6] = *block->m_BlockIndex->m_BlockHash;
        block->Dispose(block);
    }

    for (uint32_t b = 0; b < BLOCK_COUNT; ++b)
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[b], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }

    Longtail_BlockStore_Stats lru_stats;
    lru_block_store_api->GetStats(lru_block_store_api, &lru_stats);
    Longtail_BlockStore_Stats local_stats;
    local_block_store_api->GetStats(local_block_store_api, &local_stats);

    ASSERT_EQ(BLOCK_COUNT, lru_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(BLOCK_COUNT, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);

    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[0], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[6], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[5], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[3], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[0], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    {
        struct TestAsyncGetBlockComplete getCB1;
        ASSERT_EQ(0, lru_block_store_api->GetStoredBlock(lru_block_store_api, block_hashes[3], &getCB1.m_API));
        getCB1.Wait();
        struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
        ASSERT_EQ(0, getCB1.m_Err);
        if (get_block->Dispose)
        {
            get_block->Dispose(get_block);
        }
    }
    lru_block_store_api->GetStats(lru_block_store_api, &lru_stats);
    local_block_store_api->GetStats(local_block_store_api, &local_stats);
    ASSERT_EQ(BLOCK_COUNT + 6, lru_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(BLOCK_COUNT + 3, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);

    SAFE_DISPOSE_API(lru_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(local_storage_api);
}

TEST(Longtail, Longtail_CacheBlockStore)
{
    Longtail_StorageAPI* local_storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_StorageAPI* remote_storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, local_storage_api, "chunks", 524288, 1024, 0);
    Longtail_BlockStoreAPI* remote_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, remote_storage_api, "chunks", 524288, 1024, 0);
    Longtail_BlockStoreAPI* cache_block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store_api, remote_block_store_api);

    Longtail_StoredBlock put_block;
    put_block.Dispose = 0;
    put_block.m_BlockIndex = 0;
    put_block.m_BlockData = 0;

    struct TestAsyncGetBlockComplete getCB1;
    ASSERT_EQ(0, cache_block_store_api->GetStoredBlock(cache_block_store_api, 4711, &getCB1.m_API));
    getCB1.Wait();
    struct Longtail_StoredBlock* get_block = getCB1.m_StoredBlock;
    ASSERT_EQ(ENOENT, getCB1.m_Err);

    size_t block_index_size = Longtail_GetBlockIndexSize(2);
    void* block_index_mem = Longtail_Alloc(block_index_size);
    put_block.m_BlockIndex = Longtail_InitBlockIndex(block_index_mem, 2);
    *put_block.m_BlockIndex->m_BlockHash = 0xdeadbeef;
    *put_block.m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
    *put_block.m_BlockIndex->m_Tag = 0;
    put_block.m_BlockIndex->m_ChunkHashes[0] = 0xf001fa5;
    put_block.m_BlockIndex->m_ChunkHashes[1] = 0xfff1fa5;
    put_block.m_BlockIndex->m_ChunkSizes[0] = 4711;
    put_block.m_BlockIndex->m_ChunkSizes[1] = 1147;
    *put_block.m_BlockIndex->m_ChunkCount = 2;
    put_block.m_BlockChunksDataSize = 4711 + 1147;

    put_block.m_BlockData = Longtail_Alloc(put_block.m_BlockChunksDataSize);
    memset(put_block.m_BlockData, 77, 4711);
    memset(&((uint8_t*)put_block.m_BlockData)[4711], 13, 1147);

    struct TestAsyncPutBlockComplete putCB;
    ASSERT_EQ(0, remote_block_store_api->PutStoredBlock(remote_block_store_api, &put_block, &putCB.m_API));
    putCB.Wait();
    Longtail_Free(put_block.m_BlockIndex);
    ASSERT_EQ(0, putCB.m_Err);

    struct TestAsyncGetBlockComplete getCB2;
    ASSERT_EQ(0, cache_block_store_api->GetStoredBlock(cache_block_store_api, 0xdeadbeef, &getCB2.m_API));
    getCB2.Wait();
    get_block = getCB2.m_StoredBlock;
    ASSERT_EQ(0, getCB2.m_Err);
    ASSERT_NE((Longtail_StoredBlock*)0, get_block);

    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    ASSERT_EQ(0, memcmp(put_block.m_BlockData, get_block->m_BlockData, put_block.m_BlockChunksDataSize));
    Longtail_Free(put_block.m_BlockData);
    get_block->Dispose(get_block);

    Longtail_BlockStore_Stats cache_stats;
    cache_block_store_api->GetStats(cache_block_store_api, &cache_stats);
    Longtail_BlockStore_Stats remote_stats;
    remote_block_store_api->GetStats(remote_block_store_api, &remote_stats);
    Longtail_BlockStore_Stats local_stats;
    local_block_store_api->GetStats(local_block_store_api, &local_stats);

    ASSERT_EQ(2, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(0, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(2, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(0, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5902, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(0, cache_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    ASSERT_EQ(2, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(1, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(2, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(2, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5902, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(5902, remote_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    ASSERT_EQ(2, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(1, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(0, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(2, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(0, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(5902, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    SAFE_DISPOSE_API(cache_block_store_api);
    SAFE_DISPOSE_API(remote_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(remote_storage_api);
    SAFE_DISPOSE_API(local_storage_api);
}

TEST(Longtail, Longtail_CompressBlockStore)
{
    Longtail_StorageAPI* local_storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_StorageAPI* remote_storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, local_storage_api, "chunks", 524288, 1024, 0);
    Longtail_BlockStoreAPI* compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(local_block_store_api, compression_registry);

    struct TestAsyncGetBlockComplete getCB0;
    ASSERT_EQ(ENOENT, compress_block_store_api->GetStoredBlock(compress_block_store_api, 4711, &getCB0.m_API));
    struct Longtail_StoredBlock* get_block = getCB0.m_StoredBlock;

    Longtail_StoredBlock* put_block;

    {
        size_t block_index_size = Longtail_GetBlockIndexSize(2);
        size_t block_chunks_data_size = 4711 + 1147;
        size_t put_block_size = Longtail_GetStoredBlockSize(block_index_size + block_chunks_data_size);
        put_block = (struct Longtail_StoredBlock*)Longtail_Alloc(put_block_size);
        put_block->Dispose = 0;
        put_block->m_BlockIndex = Longtail_InitBlockIndex(&put_block[1], 2);
        *put_block->m_BlockIndex->m_BlockHash = 0xdeadbeef;
        *put_block->m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
        *put_block->m_BlockIndex->m_Tag = 0;
        put_block->m_BlockIndex->m_ChunkHashes[0] = 0xf001fa5;
        put_block->m_BlockIndex->m_ChunkHashes[1] = 0xfff1fa5;
        put_block->m_BlockIndex->m_ChunkSizes[0] = 4711;
        put_block->m_BlockIndex->m_ChunkSizes[1] = 1147;
        *put_block->m_BlockIndex->m_ChunkCount = 2;
        put_block->m_BlockChunksDataSize = (uint32_t)block_chunks_data_size;
        put_block->m_BlockData = &((uint8_t*)put_block->m_BlockIndex)[block_index_size];
        memset(put_block->m_BlockData, 77, 4711);
        memset(&((uint8_t*)put_block->m_BlockData)[4711], 13, 1147);
    }

    Longtail_StoredBlock* put_block2;
    {
        size_t block_index_size = Longtail_GetBlockIndexSize(2);
        size_t block_chunks_data_size = 1147 + 4711;
        size_t put_block_size = Longtail_GetStoredBlockSize(block_index_size + block_chunks_data_size);
        put_block2 = (struct Longtail_StoredBlock*)Longtail_Alloc(put_block_size);
        put_block2->Dispose = 0;
        put_block2->m_BlockIndex = Longtail_InitBlockIndex(&put_block2[1], 2);
        *put_block2->m_BlockIndex->m_BlockHash = 0xbeaddeef;
        *put_block2->m_BlockIndex->m_HashIdentifier = hash_api->GetIdentifier(hash_api);
        *put_block2->m_BlockIndex->m_Tag = Longtail_GetLZ4DefaultQuality();
        put_block2->m_BlockIndex->m_ChunkHashes[0] = 0xfff1fa5;
        put_block2->m_BlockIndex->m_ChunkHashes[1] = 0xf001fa5;
        put_block2->m_BlockIndex->m_ChunkSizes[0] = 1147;
        put_block2->m_BlockIndex->m_ChunkSizes[1] = 4711;
        *put_block2->m_BlockIndex->m_ChunkCount = 2;
        put_block2->m_BlockChunksDataSize = (uint32_t)block_chunks_data_size;
        put_block2->m_BlockData = &((uint8_t*)put_block2->m_BlockIndex)[block_index_size];
        memset(put_block2->m_BlockData, 13, 1147);
        memset(&((uint8_t*)put_block2->m_BlockData)[1147], 77, 4711);
    }

    struct TestAsyncPutBlockComplete putCB1;
    ASSERT_EQ(0, compress_block_store_api->PutStoredBlock(compress_block_store_api, put_block, &putCB1.m_API));
    putCB1.Wait();
    Longtail_Free(put_block);
    ASSERT_EQ(0, putCB1.m_Err);
    struct TestAsyncPutBlockComplete putCB2;
    ASSERT_EQ(0, compress_block_store_api->PutStoredBlock(compress_block_store_api, put_block2, &putCB2.m_API));
    putCB2.Wait();
    Longtail_Free(put_block2);
    ASSERT_EQ(0, putCB2.m_Err);

    struct TestAsyncGetBlockComplete getCB1;
    ASSERT_EQ(0, compress_block_store_api->GetStoredBlock(compress_block_store_api, 0xdeadbeef, &getCB1.m_API));
    getCB1.Wait();
    get_block = getCB1.m_StoredBlock;
    ASSERT_EQ(0, getCB1.m_Err);
    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xdeadbeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(0, *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(4711u + 1147u, get_block->m_BlockChunksDataSize);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    for (uint32_t i = 0; i < 4711; ++i)
    {
        ASSERT_EQ(77, ((uint8_t*)get_block->m_BlockData)[i]);
    }
    for (uint32_t i = 0; i < 1147; ++i)
    {
        ASSERT_EQ(13, ((uint8_t*)get_block->m_BlockData)[4711 + i]);
    }
    get_block->Dispose(get_block);

    struct TestAsyncGetBlockComplete getCB3;
    ASSERT_EQ(0, compress_block_store_api->GetStoredBlock(compress_block_store_api, 0xbeaddeef, &getCB3.m_API));
    getCB3.Wait();
    get_block = getCB3.m_StoredBlock;
    ASSERT_EQ(0, getCB3.m_Err);
    ASSERT_NE((Longtail_StoredBlock*)0, get_block);
    ASSERT_EQ(0xbeaddeef, *get_block->m_BlockIndex->m_BlockHash);
    ASSERT_EQ(hash_api->GetIdentifier(hash_api), *get_block->m_BlockIndex->m_HashIdentifier);
    ASSERT_EQ(Longtail_GetLZ4DefaultQuality(), *get_block->m_BlockIndex->m_Tag);
    ASSERT_EQ(4711u + 1147u, get_block->m_BlockChunksDataSize);
    ASSERT_EQ(0xfff1fa5, get_block->m_BlockIndex->m_ChunkHashes[0]);
    ASSERT_EQ(0xf001fa5, get_block->m_BlockIndex->m_ChunkHashes[1]);
    ASSERT_EQ(1147, get_block->m_BlockIndex->m_ChunkSizes[0]);
    ASSERT_EQ(4711, get_block->m_BlockIndex->m_ChunkSizes[1]);
    ASSERT_EQ(2, *get_block->m_BlockIndex->m_ChunkCount);
    for (uint32_t i = 0; i < 1147; ++i)
    {
        ASSERT_EQ(13, ((uint8_t*)get_block->m_BlockData)[i]);
    }
    for (uint32_t i = 0; i < 4711; ++i)
    {
        ASSERT_EQ(77, ((uint8_t*)get_block->m_BlockData)[1147 + i]);
    }
    get_block->Dispose(get_block);

    Longtail_BlockStore_Stats compress_stats;
    compress_block_store_api->GetStats(compress_block_store_api, &compress_stats);
    Longtail_BlockStore_Stats local_stats;
    local_block_store_api->GetStats(local_block_store_api, &local_stats);

    ASSERT_EQ(3, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(2, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(4, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(4, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5992, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(11804, compress_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    ASSERT_EQ(3, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]);
    ASSERT_EQ(2, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]);
    ASSERT_EQ(4, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]);
    ASSERT_EQ(4, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]);
    ASSERT_EQ(5992, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]);
    ASSERT_EQ(5992, local_stats.m_StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]);

    SAFE_DISPOSE_API(compress_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(remote_storage_api);
    SAFE_DISPOSE_API(local_storage_api);
}

TEST(Longtail, Longtail_TestGetFilesRecursively)
{
    Longtail_StorageAPI* storage = Longtail_CreateInMemStorageAPI();

    const uint32_t ASSET_COUNT = 10u;

    const char* TEST_FILENAMES[ASSET_COUNT] = {
        "ContentChangedSameLength.txt",
        "WillBeRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "OldRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe",
        "permissions_changed.txt"
    };

    const char* TEST_STRINGS[ASSET_COUNT] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "This is the documentation we are all craving to understand the complexities of life",
        "More than chunk less than block",
        "",
        "A very long file that should be able to be recreated"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
            "And in the end it is not the same, it is different, just because why not",
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the sixth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the eigth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the ninth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the tenth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough...",
        "Content stays the same but permissions change"
    };

    const size_t TEST_SIZES[ASSET_COUNT] = {
        strlen(TEST_STRINGS[0]) + 1,
        strlen(TEST_STRINGS[1]) + 1,
        strlen(TEST_STRINGS[2]) + 1,
        strlen(TEST_STRINGS[3]) + 1,
        strlen(TEST_STRINGS[4]) + 1,
        strlen(TEST_STRINGS[5]) + 1,
        strlen(TEST_STRINGS[6]) + 1,
        strlen(TEST_STRINGS[7]) + 1,
        strlen(TEST_STRINGS[8]) + 1,
        strlen(TEST_STRINGS[9]) + 1
    };

    const uint16_t TEST_PERMISSIONS[ASSET_COUNT] = {
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0755,
        0646
    };

    for (uint32_t i = 0; i < ASSET_COUNT; ++i)
    {
        const char* file_name = TEST_FILENAMES[i];
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
        storage->SetPermissions(storage, file_name, TEST_PERMISSIONS[i]);
    }

    Longtail_FileInfos* all_file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "", &all_file_infos));
    ASSERT_NE((Longtail_FileInfos*)0, all_file_infos);
    ASSERT_EQ(19u, all_file_infos->m_Count);
    Longtail_Free(all_file_infos);

    struct TestFileFilter
    {
        struct Longtail_PathFilterAPI m_API;

        static int IncludeFunc(struct Longtail_PathFilterAPI* path_filter_api, const char* root_path, const char* asset_path, const char* asset_name, int is_dir, uint64_t size, uint16_t permissions)
        {
            if(!is_dir)
            {
                return 1;
            }
            if (strcmp(asset_path, "a/file") == 0)
            {
                return 0;
            }
            return 1;
        }
    } test_filter;

    test_filter.m_API.m_API.Dispose = 0;
    test_filter.m_API.Include = TestFileFilter::IncludeFunc;

    Longtail_FileInfos* filtered_file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, &test_filter.m_API, 0, 0, "", &filtered_file_infos));
    ASSERT_NE((Longtail_FileInfos*)0, filtered_file_infos);
    ASSERT_EQ(12u, filtered_file_infos->m_Count);
    Longtail_Free(filtered_file_infos);

    SAFE_DISPOSE_API(storage);
}

TEST(Longtail, Longtail_WriteContent)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* source_storage = Longtail_CreateInMemStorageAPI();
    Longtail_StorageAPI* target_storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* fs_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, target_storage, "chunks", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateCompressBlockStoreAPI(fs_block_store_api, compression_registry);

    const char* TEST_FILENAMES[5] = {
        "local/TheLongFile.txt",
        "local/ShortString.txt",
        "local/AnotherSample.txt",
        "local/folder/ShortString.txt",
        "local/AlsoShortString.txt"
    };

    const char* TEST_STRINGS[5] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "Short string"
    };

    for (uint32_t i = 0; i < 5; ++i)
    {
        ASSERT_NE(0, CreateParentPath(source_storage, TEST_FILENAMES[i]));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, source_storage->OpenWriteFile(source_storage, TEST_FILENAMES[i], 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        ASSERT_EQ(0, source_storage->Write(source_storage, w, 0, strlen(TEST_STRINGS[i]) + 1, TEST_STRINGS[i]));
        source_storage->CloseFile(source_storage, w);
        w = 0;
    }

    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(source_storage, 0, 0, 0, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* compression_types = GetAssetTags(source_storage, version1_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        source_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "local",
        version1_paths,
        compression_types,
        16,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(compression_types);
    compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        vindex,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, cindex);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        source_storage,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "local"));
    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    Longtail_ContentIndex* cindex2 = SyncRetargetContent(block_store_api, cindex);

    ASSERT_EQ(*cindex->m_BlockCount, *cindex2->m_BlockCount);
    for (uint64_t i = 0; i < *cindex->m_BlockCount; ++i)
    {
        uint64_t i2 = 0;
        while (cindex->m_BlockHashes[i] != cindex2->m_BlockHashes[i2])
        {
            ++i2;
            ASSERT_NE(i2, *cindex2->m_BlockCount);
        }
        ASSERT_EQ(cindex->m_BlockHashes[i], cindex2->m_BlockHashes[i2]);
    }
    ASSERT_EQ(*cindex->m_ChunkCount, *cindex2->m_ChunkCount);
    for (uint64_t i = 0; i < *cindex->m_ChunkCount; ++i)
    {
        uint64_t i2 = 0;
        while (cindex->m_ChunkHashes[i] != cindex2->m_ChunkHashes[i2])
        {
            ++i2;
            ASSERT_NE(i2, *cindex2->m_ChunkCount);
        }
        ASSERT_EQ(cindex->m_BlockHashes[cindex->m_ChunkBlockIndexes[i]], cindex2->m_BlockHashes[cindex2->m_ChunkBlockIndexes[i2]]);
    }

    Longtail_Free(cindex2);
    Longtail_Free(cindex);
    Longtail_Free(vindex);

    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(fs_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(target_storage);
    SAFE_DISPOSE_API(source_storage);
}

#if 0
TEST(Longtail, TestVeryLargeFile)
{
    const char* assets_path = "C:\\Temp\\longtail\\local\\WinClient\\CL6332_WindowsClient\\WindowsClient\\PioneerGame\\Content\\Paks";

    Longtail_FileInfos* paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, assets_path, &paths));
    Longtail_VersionIndex* version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        assets_path,
        paths,
        32758u,
        &version_index));

    Longtail_Free(version_index);
}
#endif // 0

TEST(Longtail, DiffHashes)
{

//void DiffHashes(const TLongtail_Hash* reference_hashes, uint32_t reference_hash_count, const TLongtail_Hash* new_hashes, uint32_t new_hash_count, uint32_t* added_hash_count, TLongtail_Hash* added_hashes, uint32_t* removed_hash_count, TLongtail_Hash* removed_hashes)
}

TEST(Longtail, GetUniqueAssets)
{
//uint32_t GetUniqueAssets(uint64_t asset_count, const TLongtail_Hash* asset_content_hashes, uint32_t* out_unique_asset_indexes)

}

TEST(Longtail, GetBlocksForAssets)
{
//    Longtail_ContentIndex* GetBlocksForAssets(const Longtail_ContentIndex* content_index, uint64_t asset_count, const TLongtail_Hash* asset_hashes, uint64_t* out_missing_asset_count, uint64_t* out_missing_assets)
}

TEST(Longtail, Longtail_CreateMissingContent)
{
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();

//    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint64_t asset_sizes[5] = {43593, 43593, 43592, 43591, 43591};
    const uint32_t chunk_sizes[5] = {43593, 43593, 43592, 43591, 43591};
    const uint16_t asset_permissions[5] = {0644, 0644, 0644, 0644, 0644};
//    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
//    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};

    static const uint32_t TARGET_CHUNK_SIZE = 32768u;
    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        asset_count - 4,
        asset_content_hashes,
        chunk_sizes,
        0,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    Longtail_FileInfos* file_infos;
    ASSERT_EQ(0, Longtail_MakeFileInfos(5, asset_paths, asset_sizes, asset_permissions, &file_infos));
    size_t version_index_size = Longtail_GetVersionIndexSize(5, 5, 5, file_infos->m_PathDataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);

    Longtail_VersionIndex* version_index;
    ASSERT_EQ(0, Longtail_BuildVersionIndex(
        version_index_mem,
        version_index_size,
        file_infos,
        asset_path_hashes,
        asset_content_hashes,
        asset_chunk_start_index,
        asset_chunk_counts,
        file_infos->m_Count,
        asset_chunk_start_index,
        file_infos->m_Count,
        chunk_sizes,
        asset_content_hashes,
        0,
        0u,    // Dummy hash identifier
        TARGET_CHUNK_SIZE,
        &version_index));
    Longtail_Free(file_infos);

    Longtail_ContentIndex* missing_content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        content_index,
        version_index,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &missing_content_index));

    ASSERT_EQ(2u, *missing_content_index->m_BlockCount);
    ASSERT_EQ(4u, *missing_content_index->m_ChunkCount);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(asset_content_hashes[4], missing_content_index->m_ChunkHashes[3]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(asset_content_hashes[3], missing_content_index->m_ChunkHashes[2]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(asset_content_hashes[2], missing_content_index->m_ChunkHashes[1]);

    ASSERT_EQ(1u, missing_content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(asset_content_hashes[1], missing_content_index->m_ChunkHashes[0]);

    Longtail_Free(version_index);
    Longtail_Free(content_index);

    Longtail_Free(missing_content_index);

    SAFE_DISPOSE_API(hash_api);
}


TEST(Longtail, VersionIndexDirectories)
{
    Longtail_StorageAPI* local_storage = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);

    ASSERT_EQ(1, CreateFakeContent(local_storage, "two_items", 2));
    ASSERT_EQ(0, local_storage->CreateDir(local_storage, "no_items"));
    ASSERT_EQ(1, CreateFakeContent(local_storage, "deep/file/down/under/three_items", 3));
    ASSERT_EQ(1, MakePath(local_storage, "deep/folders/with/nothing/in/menoexists.nop"));

    Longtail_FileInfos* local_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, 0, 0, 0, "", &local_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_paths);
    uint32_t* compression_types = GetAssetTags(local_storage, local_paths);
    ASSERT_NE((uint32_t*)0, compression_types);

    Longtail_VersionIndex* local_version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "",
        local_paths,
        compression_types,
        16384,
        &local_version_index));
    ASSERT_NE((Longtail_VersionIndex*)0, local_version_index);
    ASSERT_EQ(16u, *local_version_index->m_AssetCount);

    Longtail_Free(compression_types);
    Longtail_Free(local_version_index);
    Longtail_Free(local_paths);

    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(local_storage);
}

TEST(Longtail, Longtail_MergeContentIndex)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(8, 0);
    Longtail_ContentIndex* cindex1;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        0,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex1));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex1);
    Longtail_ContentIndex* cindex2;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        0,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex2));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex2);
    Longtail_ContentIndex* cindex3;
    ASSERT_EQ(0, Longtail_MergeContentIndex(job_api, cindex1, cindex2, &cindex3));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex3);

    TLongtail_Hash chunk_hashes_4[] = {5, 6, 7};
    uint32_t chunk_sizes_4[] = {10, 20, 10};
    uint32_t chunk_compression_types_4[] = {0, 0, 0};
    Longtail_ContentIndex* cindex4;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        3,
        chunk_hashes_4,
        chunk_sizes_4,
        chunk_compression_types_4,
        30,
        2,
        &cindex4));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex4);

    TLongtail_Hash chunk_hashes_5[] = {8, 7, 6};
    uint32_t chunk_sizes_5[] = {20, 10, 20};
    uint32_t chunk_compression_types_5[] = {0, 0, 0};

    Longtail_ContentIndex* cindex5;
    ASSERT_EQ(0, Longtail_CreateContentIndexRaw(
        hash_api,
        3,
        chunk_hashes_5,
        chunk_sizes_5,
        chunk_compression_types_5,
        30,
        2,
        &cindex5));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex5);

    Longtail_ContentIndex* cindex6;
    ASSERT_EQ(0, Longtail_MergeContentIndex(job_api, cindex4, cindex5, &cindex6));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex6);
    ASSERT_EQ(3u, *cindex6->m_BlockCount);
    ASSERT_EQ(4u, *cindex6->m_ChunkCount);

    Longtail_ContentIndex* cindex7;
    ASSERT_EQ(0, Longtail_MergeContentIndex(job_api, cindex6, cindex1, &cindex7));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex7);
    ASSERT_EQ(3u, *cindex7->m_BlockCount);
    ASSERT_EQ(4u, *cindex7->m_ChunkCount);

    Longtail_Free(cindex7);
    Longtail_Free(cindex6);
    Longtail_Free(cindex5);
    Longtail_Free(cindex4);
    Longtail_Free(cindex3);
    Longtail_Free(cindex2);
    Longtail_Free(cindex1);

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_VersionDiff)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* fs_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage, "chunks", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateCompressBlockStoreAPI(fs_block_store_api, compression_registry);

    const uint32_t OLD_ASSET_COUNT = 10u;

    const char* OLD_TEST_FILENAMES[OLD_ASSET_COUNT] = {
        "ContentChangedSameLength.txt",
        "WillBeRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "OldRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe",
        "permissions_changed.txt"
    };

    const char* OLD_TEST_STRINGS[OLD_ASSET_COUNT] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "This is the documentation we are all craving to understand the complexities of life",
        "More than chunk less than block",
        "",
        "A very long file that should be able to be recreated"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
            "And in the end it is not the same, it is different, just because why not",
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the sixth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the eigth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the ninth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the tenth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough...",
        "Content stays the same but permissions change"
    };

    const size_t OLD_TEST_SIZES[OLD_ASSET_COUNT] = {
        strlen(OLD_TEST_STRINGS[0]) + 1,
        strlen(OLD_TEST_STRINGS[1]) + 1,
        strlen(OLD_TEST_STRINGS[2]) + 1,
        strlen(OLD_TEST_STRINGS[3]) + 1,
        strlen(OLD_TEST_STRINGS[4]) + 1,
        strlen(OLD_TEST_STRINGS[5]) + 1,
        strlen(OLD_TEST_STRINGS[6]) + 1,
        strlen(OLD_TEST_STRINGS[7]) + 1,
        strlen(OLD_TEST_STRINGS[8]) + 1,
        strlen(OLD_TEST_STRINGS[9]) + 1
    };

    const uint16_t OLD_TEST_PERMISSIONS[OLD_ASSET_COUNT] = {
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0755,
        0646
    };

    const uint32_t NEW_ASSET_COUNT = 10u;

    const char* NEW_TEST_FILENAMES[NEW_ASSET_COUNT] = {
        "ContentChangedSameLength.txt",
        "HasBeenRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "NewRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe",
        "permissions_changed.txt"
    };

    const char* NEW_TEST_STRINGS[NEW_ASSET_COUNT] = {
        "This is the first test string which is fairly long and *will* - reconstructed properly, than you very much",   // Content changed, same length
        "Short string", // Renamed
        "Another sample string that does not match any other string but -reconstructed properly.",   // Shorter, same base content
        "Short string but with added stuff",    // Longer, same base content
        "This is the documentation we are all craving to understand the complexities of life",  // Same but moved to different folder
        "I wish I was a baller.", // Just different
        "", // Unchanged
        "!A very long file that should be able to be recreated"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff.",
            "And in the end it is not the same, it is different, just because why not"  // Modified at start
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the sixth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the eigth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the ninth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the tenth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough...!",  // Modified at end
        "Content stays the same but permissions change" // Permissions changed
    };

    const size_t NEW_TEST_SIZES[NEW_ASSET_COUNT] = {
        strlen(NEW_TEST_STRINGS[0]) + 1,
        strlen(NEW_TEST_STRINGS[1]) + 1,
        strlen(NEW_TEST_STRINGS[2]) + 1,
        strlen(NEW_TEST_STRINGS[3]) + 1,
        strlen(NEW_TEST_STRINGS[4]) + 1,
        strlen(NEW_TEST_STRINGS[5]) + 1,
        strlen(NEW_TEST_STRINGS[6]) + 1,
        strlen(NEW_TEST_STRINGS[7]) + 1,
        strlen(NEW_TEST_STRINGS[8]) + 1,
        strlen(NEW_TEST_STRINGS[9]) + 1
    };

    const uint16_t NEW_TEST_PERMISSIONS[NEW_ASSET_COUNT] = {
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0755,
        0644
    };

    for (uint32_t i = 0; i < OLD_ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "old", OLD_TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (OLD_TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, OLD_TEST_SIZES[i], OLD_TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
        storage->SetPermissions(storage, file_name, OLD_TEST_PERMISSIONS[i]);
        Longtail_Free(file_name);
    }

    for (uint32_t i = 0; i < NEW_ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "new", NEW_TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (NEW_TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, NEW_TEST_SIZES[i], NEW_TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
        storage->SetPermissions(storage, file_name, NEW_TEST_PERMISSIONS[i]);
        Longtail_Free(file_name);
    }

    Longtail_FileInfos* old_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "old", &old_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, old_version_paths);
    uint32_t* old_compression_types = GetAssetTags(storage, old_version_paths);
    ASSERT_NE((uint32_t*)0, old_compression_types);
    Longtail_VersionIndex* old_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "old",
        old_version_paths,
        old_compression_types,
        16,
        &old_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, old_vindex);
    Longtail_Free(old_compression_types);
    old_compression_types = 0;
    Longtail_Free(old_version_paths);
    old_version_paths = 0;

    Longtail_FileInfos* new_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "new", &new_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, new_version_paths);
    uint32_t* new_compression_types = GetAssetTags(storage, new_version_paths);
    ASSERT_NE((uint32_t*)0, new_compression_types);
    Longtail_VersionIndex* new_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "new",
        new_version_paths,
        new_compression_types,
        16,
        &new_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, new_vindex);
    Longtail_Free(new_compression_types);
    new_compression_types = 0;
    Longtail_Free(new_version_paths);
    new_version_paths = 0;

    Longtail_ContentIndex* version_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            new_vindex,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &version_content_index));

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, version_content_index);
    ASSERT_NE((struct Longtail_ContentIndex*)0, block_store_content_index);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        new_vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        new_vindex,
        "new"));

    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;
    Longtail_Free(version_content_index);
    version_content_index = 0;

    Longtail_VersionDiff* version_diff;
    ASSERT_EQ(0, Longtail_CreateVersionDiff(
        hash_api,
        old_vindex,
        new_vindex,
        &version_diff));
    ASSERT_NE((Longtail_VersionDiff*)0, version_diff);

    ASSERT_EQ(3u, *version_diff->m_SourceRemovedCount);
    ASSERT_EQ(3u, *version_diff->m_TargetAddedCount);
    ASSERT_EQ(6u, *version_diff->m_ModifiedContentCount);
    ASSERT_EQ(1u, *version_diff->m_ModifiedPermissionsCount);

    ASSERT_EQ(0, Longtail_CreateContentIndexFromDiff(
            hash_api,
            new_vindex,
            version_diff,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &version_content_index));

    content_index = SyncRetargetContent(block_store_api, version_content_index);
    ASSERT_NE((struct Longtail_ContentIndex*)0, content_index);

    Longtail_Free(version_content_index);
    version_content_index = 0;

    ASSERT_EQ(0, Longtail_ChangeVersion(
        block_store_api,
        storage,
        hash_api,
        job_api,
        0,
        0,
        0,
        content_index,
        old_vindex,
        new_vindex,
        version_diff,
        "old",
        1));

    Longtail_Free(version_diff);
    version_diff = 0;
    Longtail_Free(content_index);
    content_index = 0;

    // Make null-diff and see that we handle zero changes
    ASSERT_EQ(0, Longtail_CreateVersionDiff(
        hash_api,
        new_vindex,
        new_vindex,
        &version_diff));
    ASSERT_NE((Longtail_VersionDiff*)0, version_diff);

    ASSERT_EQ(0, Longtail_CreateContentIndexFromDiff(
            hash_api,
            new_vindex,
            version_diff,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &version_content_index));

    content_index = SyncRetargetContent(block_store_api, version_content_index);

    ASSERT_EQ(0, Longtail_ChangeVersion(
        block_store_api,
        storage,
        hash_api,
        job_api,
        0,
        0,
        0,
        content_index,
        old_vindex,
        new_vindex,
        version_diff,
        "old",
        1));

    Longtail_Free(version_content_index);
    Longtail_Free(content_index);
    Longtail_Free(version_diff);

    Longtail_Free(new_vindex);
    Longtail_Free(old_vindex);

    // Verify that our old folder now matches the new folder data
    Longtail_FileInfos* updated_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "old", &updated_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, updated_version_paths);
    const uint32_t NEW_ASSET_FOLDER_EXTRA_COUNT = 9u;
    ASSERT_EQ(NEW_ASSET_COUNT + NEW_ASSET_FOLDER_EXTRA_COUNT, updated_version_paths->m_Count);
    Longtail_Free(updated_version_paths);

    for (uint32_t i = 0; i < NEW_ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "old", NEW_TEST_FILENAMES[i]);
        Longtail_StorageAPI_HOpenFile r;
        ASSERT_EQ(0, storage->OpenReadFile(storage, file_name, &r));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, r);
        uint64_t size;
        ASSERT_EQ(0, storage->GetSize(storage, r, &size));
        ASSERT_EQ(NEW_TEST_SIZES[i], size);
        char* test_data = (char*)Longtail_Alloc(sizeof(char) * size);
        if (size)
        {
            ASSERT_EQ(0, storage->Read(storage, r, 0, size, test_data));
            ASSERT_STREQ(NEW_TEST_STRINGS[i], test_data);
        }
        storage->CloseFile(storage, r);
        r = 0;
        Longtail_Free(test_data);
        test_data = 0;
    }

    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(fs_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage);
}


TEST(Longtail, Longtail_WriteVersion)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* fs_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateCompressBlockStoreAPI(fs_block_store_api, compression_registry);

    const uint32_t asset_count = 8u;

    const char* TEST_FILENAMES[] = {
        "TheLongFile.txt",
        "ShortString.txt",
        "AnotherSample.txt",
        "folder/ShortString.txt",
        "WATCHIOUT.txt",
        "empty/.init.py",
        "TheVeryLongFile.txt",
        "AnotherVeryLongFile.txt"
    };

    const char* TEST_STRINGS[] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "More than chunk less than block",
        "",
        "A very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end...",
        "Another very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end..."
    };

    const size_t TEST_SIZES[] = {
        strlen(TEST_STRINGS[0]) + 1,
        strlen(TEST_STRINGS[1]) + 1,
        strlen(TEST_STRINGS[2]) + 1,
        strlen(TEST_STRINGS[3]) + 1,
        strlen(TEST_STRINGS[4]) + 1,
        0,
        strlen(TEST_STRINGS[6]) + 1,
        strlen(TEST_STRINGS[7]) + 1
    };

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "local", TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage_api, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        storage_api->CloseFile(storage_api, w);
        w = 0;
    }

    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* version1_compression_types = GetAssetTags(storage_api, version1_paths);
    ASSERT_NE((uint32_t*)0, version1_compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "local",
        version1_paths,
        version1_compression_types,
        50,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(version1_compression_types);
    version1_compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        vindex,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, cindex);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "local"));
    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    ASSERT_EQ(0, Longtail_WriteVersion(
        block_store_api,
        storage_api,
        job_api,
        0,
        0,
        0,
        cindex,
        vindex,
        "remote",
        1));

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "remote", TEST_FILENAMES[i]);
        Longtail_StorageAPI_HOpenFile r;
        ASSERT_EQ(0, storage_api->OpenReadFile(storage_api, file_name, &r));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, r);
        uint64_t size;
        ASSERT_EQ(0, storage_api->GetSize(storage_api, r, &size));
        ASSERT_EQ(TEST_SIZES[i], size);
        char* test_data = (char*)Longtail_Alloc(sizeof(char) * size);
        if (size)
        {
            ASSERT_EQ(0, storage_api->Read(storage_api, r, 0, size, test_data));
            ASSERT_STREQ(TEST_STRINGS[i], test_data);
        }
        storage_api->CloseFile(storage_api, r);
        r = 0;
        Longtail_Free(test_data);
        test_data = 0;
    }

    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;
    SAFE_DISPOSE_API(fs_block_store_api);
    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, ChunkerLargeFile)
{
    FILE* large_file = fopen("testdata/chunker.input", "rb");
    ASSERT_NE((FILE*)0, large_file);

    fseek(large_file, 0, SEEK_END);
    long size = ftell(large_file);
    fseek(large_file, 0, SEEK_SET);

    struct FeederContext
    {
        FILE* f;
        long size;
        long offset;

        static int FeederFunc(void* context, Longtail_ChunkerAPI_HChunker chunker, uint32_t requested_size, char* buffer, uint32_t* out_size)
        {
            FeederContext* c = (FeederContext*)context;
            uint32_t read_count = (uint32_t)(c->size - c->offset);
            if (read_count > 0)
            {
                if (requested_size < read_count)
                {
                    read_count = requested_size;
                }
                int err = fseek(c->f, c->offset, SEEK_SET);
                if (err)
                {
                    return err;
                }
                size_t r = fread(buffer, (size_t)read_count, 1, c->f);
                if (r != 1)
                {
                    return errno;
                }
            }
            c->offset += read_count;
            *out_size = read_count;
            return 0;
        }
    };

    FeederContext feeder_context = {large_file, size, 0};

    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();

    const uint64_t ChunkSizeAvgDefault    = 64 * 1024;
    const uint64_t ChunkSizeMinDefault    = ChunkSizeAvgDefault / 4;
    const uint64_t ChunkSizeMaxDefault    = ChunkSizeAvgDefault * 4;

    Longtail_ChunkerAPI_HChunker chunker;
    ASSERT_EQ(0, chunker_api->CreateChunker(
        chunker_api,
        ChunkSizeMinDefault,
        ChunkSizeAvgDefault,
        ChunkSizeMaxDefault,
        &chunker));
    ASSERT_NE((Longtail_ChunkerAPI_HChunker)0, chunker);

    const uint32_t expected_chunk_count = 20u;
    const struct Longtail_Chunker_ChunkRange expected_chunks[expected_chunk_count] =
    {
        { (const uint8_t*)0, 0,       81590},
        { (const uint8_t*)0, 81590,   46796},
        { (const uint8_t*)0, 128386,  36543},
        { (const uint8_t*)0, 164929,  83172},
        { (const uint8_t*)0, 248101,  76749},
        { (const uint8_t*)0, 324850,  79550},
        { (const uint8_t*)0, 404400,  41484},
        { (const uint8_t*)0, 445884,  20326},
        { (const uint8_t*)0, 466210,  31652},
        { (const uint8_t*)0, 497862,  19995},
        { (const uint8_t*)0, 517857,  103873},
        { (const uint8_t*)0, 621730,  38087},
        { (const uint8_t*)0, 659817,  38377},
        { (const uint8_t*)0, 698194,  23449},
        { (const uint8_t*)0, 721643,  47321},
        { (const uint8_t*)0, 768964,  86692},
        { (const uint8_t*)0, 855656,  28268},
        { (const uint8_t*)0, 883924,  65465},
        { (const uint8_t*)0, 949389,  33255},
        { (const uint8_t*)0, 982644,  65932}
    };

    Longtail_Chunker_ChunkRange r;
    for (uint32_t i = 0; i < expected_chunk_count; ++i)
    {
        ASSERT_EQ(0, chunker_api->NextChunk(chunker_api, chunker, FeederContext::FeederFunc, &feeder_context, &r));
        ASSERT_EQ(expected_chunks[i].offset, r.offset);
        ASSERT_EQ(expected_chunks[i].len, r.len);
    }
    ASSERT_EQ(ESPIPE, chunker_api->NextChunk(chunker_api, chunker, FeederContext::FeederFunc, &feeder_context, &r));
    ASSERT_EQ((const uint8_t*)0, r.buf);
    ASSERT_EQ(0, r.len);

    chunker_api->DisposeChunker(chunker_api, chunker);
    chunker = 0;

    fclose(large_file);
    large_file = 0;

    SAFE_DISPOSE_API(chunker_api);
}

TEST(Longtail, FileSystemStorage)
{
    Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();

    const uint32_t ASSET_COUNT = 9u;

    const char* TEST_FILENAMES[] = {
        "ContentChangedSameLength.txt",
        "WillBeRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "OldRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe"
    };

    const char* TEST_STRINGS[] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "This is the documentation we are all craving to understand the complexities of life",
        "More than chunk less than block",
        "",
        "A very long file that should be able to be recreated"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
            "And in the end it is not the same, it is different, just because why not",
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the sixth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the eigth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the ninth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the tenth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough..."
    };

    const char* root_path = "testdata/sample_folder";

    Longtail_FileInfos* file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, root_path, &file_infos));
    ASSERT_EQ(18u, file_infos->m_Count);
    Longtail_Free(file_infos);

    for (uint32_t a = 0; a < ASSET_COUNT; ++a)
    {
        char* full_path = storage_api->ConcatPath(storage_api, root_path, TEST_FILENAMES[a]);
        ASSERT_NE((char*)0, full_path);
        Longtail_StorageAPI_HOpenFile f;
        ASSERT_EQ(0, storage_api->OpenReadFile(storage_api, full_path, &f));
        uint64_t expected_size = (uint64_t)strlen(TEST_STRINGS[a]);
        uint64_t size;
        ASSERT_EQ(0, storage_api->GetSize(storage_api, f, &size));
        ASSERT_EQ(expected_size, size);
        if (size > 0)
        {
            char* data = (char*)Longtail_Alloc(size);
            ASSERT_EQ(0, storage_api->Read(storage_api, f, 0, size, data));
            for (uint64_t b = 0; b < size; ++b)
            {
                ASSERT_EQ(data[b], TEST_STRINGS[a][b]);
            }
            Longtail_Free(data);
        }
        storage_api->CloseFile(storage_api, f);
        Longtail_Free(full_path);
    }

    SAFE_DISPOSE_API(storage_api);
}

struct TestPutBlockRequest
{
    uint64_t block_hash;
    struct Longtail_StoredBlock* stored_block;
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api;
};

struct TestGetBlockRequest
{
    uint64_t block_hash;
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api;
};

struct TestRetargetContentRequest
{
    struct Longtail_AsyncRetargetContentAPI* async_complete_api;
    struct Longtail_ContentIndex* content_index;
};

struct TestStoredBlockLookup
{
    TLongtail_Hash key;
    uint8_t* value;
};

class TestAsyncBlockStore
{
public:
    struct Longtail_BlockStoreAPI m_API;

    static int InitBlockStore(TestAsyncBlockStore* block_store, struct Longtail_HashAPI* hash_api, struct Longtail_JobAPI* job_api);
    static void Dispose(struct Longtail_API* api);
    static int PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
    static int PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index);
    static int GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
    static int GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats);
    static int Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api);
    static int RetargetContent(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index, struct Longtail_AsyncRetargetContentAPI* async_complete_api);
    static void CompleteRequest(class TestAsyncBlockStore* block_store);
private:
    struct Longtail_HashAPI* m_HashAPI;
    struct Longtail_JobAPI* m_JobAPI;
    TLongtail_Atomic32 m_ExitFlag;
    HLongtail_SpinLock m_IOLock;
    HLongtail_SpinLock m_IndexLock;
    HLongtail_Sema m_RequestSema;
    HLongtail_Thread m_IOThread[8];
    TLongtail_Atomic32 m_PendingRequestCount;
    struct Longtail_AsyncFlushAPI** m_PendingAsyncFlushAPIs;

    intptr_t m_PutRequestOffset;
    struct TestPutBlockRequest* m_PutRequests;
    intptr_t m_GetRequestOffset;
    struct TestGetBlockRequest* m_GetRequests;
    intptr_t m_RetargetContentRequestOffset;
    struct TestRetargetContentRequest* m_RetargetContentRequests;
    struct Longtail_ContentIndex* m_ContentIndex;
    struct TestStoredBlockLookup* m_StoredBlockLookup;

    static int Worker(void* context_data);
};

void TestAsyncBlockStore::CompleteRequest(class TestAsyncBlockStore* block_store)
{
    LONGTAIL_FATAL_ASSERT(block_store->m_PendingRequestCount > 0, return)
    struct Longtail_AsyncFlushAPI** pendingAsyncFlushAPIs = 0;
    Longtail_LockSpinLock(block_store->m_IOLock);
    if (0 == Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, -1))
    {
        pendingAsyncFlushAPIs = block_store->m_PendingAsyncFlushAPIs;
        block_store->m_PendingAsyncFlushAPIs = 0;
    }
    Longtail_UnlockSpinLock(block_store->m_IOLock);
    size_t c = arrlen(pendingAsyncFlushAPIs);
    for (size_t n = 0; n < c; ++n)
    {
        pendingAsyncFlushAPIs[n]->OnComplete(pendingAsyncFlushAPIs[n], 0);
    }
    arrfree(pendingAsyncFlushAPIs);
}

int TestAsyncBlockStore::InitBlockStore(TestAsyncBlockStore* block_store, struct Longtail_HashAPI* hash_api, struct Longtail_JobAPI* job_api)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    struct Longtail_BlockStoreAPI* api = Longtail_MakeBlockStoreAPI(
        &block_store->m_API,
        TestAsyncBlockStore::Dispose,
        TestAsyncBlockStore::PutStoredBlock,
        TestAsyncBlockStore::PreflightGet,
        TestAsyncBlockStore::GetStoredBlock,
        TestAsyncBlockStore::RetargetContent,
        TestAsyncBlockStore::GetStats,
        TestAsyncBlockStore::Flush);
    if (!api)
    {
        return ENOMEM;
    }
    block_store->m_HashAPI = hash_api;
    block_store->m_JobAPI = job_api;
    block_store->m_ExitFlag = 0;
    block_store->m_PutRequestOffset = 0;
    block_store->m_PutRequests = 0;
    block_store->m_GetRequestOffset = 0;
    block_store->m_GetRequests = 0;
    block_store->m_RetargetContentRequestOffset = 0;
    block_store->m_RetargetContentRequests = 0;
    block_store->m_StoredBlockLookup = 0;
    block_store->m_PendingRequestCount = 0;
    block_store->m_PendingAsyncFlushAPIs = 0;
    int err = Longtail_CreateContentIndex(
            block_store->m_HashAPI,
            0,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &block_store->m_ContentIndex);
    if (err)
    {
        return err;
    }
    err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &block_store->m_IOLock);
    if (err)
    {
        Longtail_Free(block_store->m_ContentIndex);
        return err;
    }
    err = Longtail_CreateSpinLock(Longtail_Alloc(Longtail_GetSpinLockSize()), &block_store->m_IndexLock);
    if (err)
    {
        Longtail_DeleteSpinLock(block_store->m_IOLock);
        Longtail_Free(block_store->m_ContentIndex);
        return err;
    }
    err = Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &block_store->m_RequestSema);
    if (err)
    {
        Longtail_DeleteSpinLock(block_store->m_IndexLock);
        Longtail_Free(block_store->m_IndexLock);
        Longtail_DeleteSpinLock(block_store->m_IOLock);
        Longtail_Free(block_store->m_IOLock);
        Longtail_Free(block_store->m_ContentIndex);
        return err;
    }
    for (uint32_t t = 0; t < 8; ++t)
    {
        err = Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), TestAsyncBlockStore::Worker, 0, block_store, -1, &block_store->m_IOThread[t]);
        if (err)
        {
            while (t--)
            {
                Longtail_JoinThread(block_store->m_IOThread[t], LONGTAIL_TIMEOUT_INFINITE);
                Longtail_DeleteThread(block_store->m_IOThread[t]);
                Longtail_Free(block_store->m_IOThread[t]);
            }
            Longtail_DeleteSema(block_store->m_RequestSema);
            Longtail_Free(block_store->m_RequestSema);
            Longtail_DeleteSpinLock(block_store->m_IndexLock);
            Longtail_Free(block_store->m_IndexLock);
            Longtail_DeleteSpinLock(block_store->m_IOLock);
            Longtail_Free(block_store->m_IOLock);
            return err;
        }
    }
    return 0;
}

void TestAsyncBlockStore::Dispose(struct Longtail_API* api)
{
    TestAsyncBlockStore* block_store = (TestAsyncBlockStore*)api;
    Longtail_AtomicAdd32(&block_store->m_ExitFlag, 1);
    Longtail_PostSema(block_store->m_RequestSema, 8);
    for (uint32_t t = 0; t < 8; ++t)
    {
        Longtail_JoinThread(block_store->m_IOThread[t], LONGTAIL_TIMEOUT_INFINITE);
        Longtail_DeleteThread(block_store->m_IOThread[t]);
        Longtail_Free(block_store->m_IOThread[t]);
    }
    Longtail_DeleteSema(block_store->m_RequestSema);
    Longtail_Free(block_store->m_RequestSema);
    Longtail_DeleteSpinLock(block_store->m_IndexLock);
    Longtail_Free(block_store->m_IndexLock);
    Longtail_DeleteSpinLock(block_store->m_IOLock);
    Longtail_Free(block_store->m_IOLock);
    for (ptrdiff_t i = 0; i < block_store->m_PutRequestOffset; ++i)
    {
        uint64_t block_hash = block_store->m_PutRequests[i].block_hash;
        intptr_t i_ptr = hmgeti(block_store->m_StoredBlockLookup, block_hash);
        if (i_ptr == -1)
        {
            continue;
        }
        uint8_t* d = block_store->m_StoredBlockLookup[i_ptr].value;
        arrfree(d);
    }
    arrfree(block_store->m_PendingAsyncFlushAPIs);
    hmfree(block_store->m_StoredBlockLookup);
    arrfree(block_store->m_PutRequests);
    arrfree(block_store->m_GetRequests);
    arrfree(block_store->m_RetargetContentRequests);
    Longtail_Free(block_store->m_ContentIndex);
}

static int TestStoredBlock_Dispose(struct Longtail_StoredBlock* stored_block)
{
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    Longtail_Free(stored_block);
    return 0;
}

static int WorkerPutRequest(struct Longtail_StoredBlock* stored_block, uint8_t** out_serialized_block_data)
{
    uint32_t chunk_count = *stored_block->m_BlockIndex->m_ChunkCount;
    uint32_t block_index_data_size = (uint32_t)Longtail_GetBlockIndexDataSize(chunk_count);
    size_t total_block_size = block_index_data_size + stored_block->m_BlockChunksDataSize;
    uint8_t* serialized_block_data = 0;
    arrsetlen(serialized_block_data, total_block_size);
    memcpy(&serialized_block_data[0], &stored_block->m_BlockIndex[1], block_index_data_size);
    memcpy(&serialized_block_data[block_index_data_size], stored_block->m_BlockData, stored_block->m_BlockChunksDataSize);
    *out_serialized_block_data = serialized_block_data;
    return 0;    
}

static int WorkerGetRequest(uint8_t* serialized_block_data, struct Longtail_StoredBlock** out_stored_block)
{
    size_t serialized_block_data_size = size_t(arrlen(serialized_block_data));
    size_t block_mem_size = Longtail_GetStoredBlockSize(serialized_block_data_size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)Longtail_Alloc(block_mem_size);
    if (!stored_block)
    {
        return ENOMEM;
    }
    void* block_data = &((uint8_t*)stored_block)[block_mem_size - serialized_block_data_size];
    memcpy(block_data, serialized_block_data, serialized_block_data_size);
    int err = Longtail_InitStoredBlockFromData(
        stored_block,
        block_data,
        serialized_block_data_size);
    if (err)
    {
        Longtail_Free(stored_block);
        return err;
    }
    stored_block->Dispose = TestStoredBlock_Dispose;
    *out_stored_block = stored_block;
    return 0;
}

int TestAsyncBlockStore::Worker(void* context_data)
{
    TestAsyncBlockStore* block_store = (TestAsyncBlockStore*)context_data;
    while (1)
    {
        int err = Longtail_WaitSema(block_store->m_RequestSema, LONGTAIL_TIMEOUT_INFINITE);
        LONGTAIL_FATAL_ASSERT(err == 0, continue)

        Longtail_LockSpinLock(block_store->m_IOLock);
        ptrdiff_t put_request_count = arrlen(block_store->m_PutRequests);
        ptrdiff_t put_request_offset = block_store->m_PutRequestOffset;
        if (put_request_count > put_request_offset)
        {
            ptrdiff_t put_request_index = block_store->m_PutRequestOffset++;
            struct TestPutBlockRequest* put_request = &block_store->m_PutRequests[put_request_index];
            struct Longtail_StoredBlock* stored_block = put_request->stored_block;
            struct Longtail_AsyncPutStoredBlockAPI* async_complete_api = put_request->async_complete_api;
            Longtail_UnlockSpinLock(block_store->m_IOLock);
            uint8_t* serialized_block_data;
            int err = WorkerPutRequest(stored_block, &serialized_block_data);

            Longtail_LockSpinLock(block_store->m_IndexLock);
            ptrdiff_t stored_block_count = hmlen(block_store->m_StoredBlockLookup);
            hmput(block_store->m_StoredBlockLookup, *stored_block->m_BlockIndex->m_BlockHash, serialized_block_data);

            static const uint32_t MAX_BLOCK_SIZE = 32u;
            static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

            struct Longtail_ContentIndex* block_content_index;
            Longtail_CreateContentIndexFromBlocks(MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 1, &stored_block->m_BlockIndex, &block_content_index);
            if (block_store->m_ContentIndex == 0)
            {
                block_store->m_ContentIndex = block_content_index;
            }
            else
            {
                struct Longtail_ContentIndex* context_index = block_store->m_ContentIndex;
                size_t buf_size;
                void* buf;
                Longtail_WriteContentIndexToBuffer(context_index, &buf, &buf_size);

                struct Longtail_ContentIndex* context_index_copy;
                Longtail_ReadContentIndexFromBuffer(buf, buf_size, &context_index_copy);
                Longtail_Free(buf);

                struct Longtail_ContentIndex* merged_content_index;
                Longtail_MergeContentIndex(block_store->m_JobAPI, context_index_copy, block_content_index, &merged_content_index);
                if (*context_index->m_BlockCount != (*merged_content_index->m_BlockCount) - 1)
                {
                    Longtail_Free(merged_content_index);
                    merged_content_index = 0;
                    Longtail_MergeContentIndex(block_store->m_JobAPI, context_index_copy, block_content_index, &merged_content_index);
                }
                Longtail_Free(context_index_copy);
                Longtail_Free(block_store->m_ContentIndex);
                block_store->m_ContentIndex = merged_content_index;
            }

            Longtail_UnlockSpinLock(block_store->m_IndexLock);
            Longtail_Free(block_content_index);

            async_complete_api->OnComplete(async_complete_api, err);

            CompleteRequest(block_store);
            continue;
        }
        ptrdiff_t get_request_count = arrlen(block_store->m_GetRequests);
        ptrdiff_t get_request_offset = block_store->m_GetRequestOffset;
        if (get_request_count > get_request_offset)
        {
            ptrdiff_t get_request_index = block_store->m_GetRequestOffset++;
            struct TestGetBlockRequest* get_request = &block_store->m_GetRequests[get_request_index];
            uint64_t block_hash = get_request->block_hash;
            struct Longtail_AsyncGetStoredBlockAPI* async_complete_api = get_request->async_complete_api;
            uint8_t* serialized_block_data = 0;
            Longtail_UnlockSpinLock(block_store->m_IOLock);

            Longtail_LockSpinLock(block_store->m_IndexLock);
            ptrdiff_t stored_block_count = hmlen(block_store->m_StoredBlockLookup);
            intptr_t i_ptr = hmgeti(block_store->m_StoredBlockLookup, block_hash);
            if (i_ptr != -1)
            {
                serialized_block_data = block_store->m_StoredBlockLookup[i_ptr].value;
            }
            Longtail_UnlockSpinLock(block_store->m_IndexLock);
            if (serialized_block_data)
            {
                struct Longtail_StoredBlock* stored_block = 0;
                int err = WorkerGetRequest(serialized_block_data, &stored_block);
                async_complete_api->OnComplete(async_complete_api, stored_block, err);
                CompleteRequest(block_store);
                continue;
            }
            else
            {
                async_complete_api->OnComplete(async_complete_api, 0, ENOENT);
                CompleteRequest(block_store);
                continue;
            }
        }
        ptrdiff_t retarget_content_request_count = arrlen(block_store->m_RetargetContentRequests);
        ptrdiff_t retarget_content_request_offset = block_store->m_RetargetContentRequestOffset;
        if (retarget_content_request_count > retarget_content_request_offset)
        {
            ptrdiff_t retarget_content_request_index = block_store->m_RetargetContentRequestOffset++;
            struct TestRetargetContentRequest* retarget_content_request = &block_store->m_RetargetContentRequests[retarget_content_request_index];
            struct Longtail_AsyncRetargetContentAPI* async_complete_api = retarget_content_request->async_complete_api;
            Longtail_UnlockSpinLock(block_store->m_IOLock);

            Longtail_LockSpinLock(block_store->m_IndexLock);

            struct Longtail_ContentIndex* content_index;
            int err = Longtail_RetargetContent(block_store->m_ContentIndex, retarget_content_request->content_index, &content_index);
            Longtail_UnlockSpinLock(block_store->m_IndexLock);
            Longtail_Free(retarget_content_request->content_index);

            if (err)
            {
                async_complete_api->OnComplete(async_complete_api, 0, err);
                CompleteRequest(block_store);
                continue;
            }
            async_complete_api->OnComplete(async_complete_api, content_index, 0);
            CompleteRequest(block_store);
            continue;
        }

        Longtail_UnlockSpinLock(block_store->m_IOLock);

        if (block_store->m_ExitFlag != 0)
        {
            return 0;
        }
    }
}

int TestAsyncBlockStore::PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(stored_block, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)

    TestAsyncBlockStore* block_store = (TestAsyncBlockStore*)block_store_api;
    struct TestPutBlockRequest put_request;
    put_request.block_hash = *stored_block->m_BlockIndex->m_BlockHash;
    put_request.stored_block = stored_block;
    put_request.async_complete_api = async_complete_api;
    Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, 1);
    Longtail_LockSpinLock(block_store->m_IOLock);
    arrput(block_store->m_PutRequests, put_request);
    Longtail_UnlockSpinLock(block_store->m_IOLock);
    Longtail_PostSema(block_store->m_RequestSema, 1);
    return 0;
}

int TestAsyncBlockStore::PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index)
{
    return 0;
}


int TestAsyncBlockStore::GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    TestAsyncBlockStore* block_store = (TestAsyncBlockStore*)block_store_api;
    struct TestGetBlockRequest get_request;
    get_request.block_hash = block_hash;
    get_request.async_complete_api = async_complete_api;

    Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, 1);
    Longtail_LockSpinLock(block_store->m_IOLock);
    arrput(block_store->m_GetRequests, get_request);
    Longtail_UnlockSpinLock(block_store->m_IOLock);
    Longtail_PostSema(block_store->m_RequestSema, 1);
    return 0;
}

int TestAsyncBlockStore::RetargetContent(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index, struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    LONGTAIL_FATAL_ASSERT(block_store_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(async_complete_api, return EINVAL)
    TestAsyncBlockStore* block_store = (TestAsyncBlockStore*)block_store_api;

    struct TestRetargetContentRequest retarget_content_request;
    retarget_content_request.async_complete_api = async_complete_api;
    void* buffer;
    size_t size;
    int err = Longtail_WriteContentIndexToBuffer(content_index, &buffer, &size);
    if (err)
    {
        return err;
    }
    struct Longtail_ContentIndex* content_index_copy;
    err = Longtail_ReadContentIndexFromBuffer(buffer, size, &content_index_copy);
    Longtail_Free(buffer);
    if (err)
    {
        return err;
    }
    retarget_content_request.content_index = content_index_copy;

    Longtail_AtomicAdd32(&block_store->m_PendingRequestCount, 1);
    Longtail_LockSpinLock(block_store->m_IOLock);
    arrput(block_store->m_RetargetContentRequests, retarget_content_request);
    Longtail_UnlockSpinLock(block_store->m_IOLock);
    Longtail_PostSema(block_store->m_RequestSema, 1);
    return 0;
}

int TestAsyncBlockStore::GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    memset(out_stats, 0, sizeof(struct Longtail_BlockStore_Stats));
    return 0;
}

int TestAsyncBlockStore::Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    class TestAsyncBlockStore* block_store = (class TestAsyncBlockStore*)block_store_api;
    Longtail_LockSpinLock(block_store->m_IOLock);
    if (block_store->m_PendingRequestCount > 0)
    {
        arrput(block_store->m_PendingAsyncFlushAPIs, async_complete_api);
        Longtail_UnlockSpinLock(block_store->m_IOLock);
        return 0;
    }
    Longtail_UnlockSpinLock(block_store->m_IOLock);
    async_complete_api->OnComplete(async_complete_api, 0);
    return 0;
}

TEST(Longtail, AsyncBlockStore)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);

    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_BlockStoreAPI* cache_block_store = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "cache", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);


    TestAsyncBlockStore block_store;
    ASSERT_EQ(0, TestAsyncBlockStore::InitBlockStore(&block_store, hash_api, job_api));
    struct Longtail_BlockStoreAPI* async_block_store_api = &block_store.m_API; 
    Longtail_BlockStoreAPI* compressed_block_store_api = Longtail_CreateCompressBlockStoreAPI(async_block_store_api, compression_registry);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, cache_block_store, compressed_block_store_api);

#define TO_ACTUAL_TEST
#if defined(TO_ACTUAL_TEST)
    const uint32_t asset_count = 8u;

    const char* TEST_FILENAMES[] = {
        "TheLongFile.txt",
        "ShortString.txt",
        "AnotherSample.txt",
        "folder/ShortString.txt",
        "WATCHIOUT.txt",
        "empty/.init.py",
        "TheVeryLongFile.txt",
        "AnotherVeryLongFile.txt"
    };

    const char* TEST_STRINGS[] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "More than chunk less than block",
        "",
        "A very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end...",
        "Another very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end..."
    };

    const size_t TEST_SIZES[] = {
        strlen(TEST_STRINGS[0]) + 1,
        strlen(TEST_STRINGS[1]) + 1,
        strlen(TEST_STRINGS[2]) + 1,
        strlen(TEST_STRINGS[3]) + 1,
        strlen(TEST_STRINGS[4]) + 1,
        0,
        strlen(TEST_STRINGS[6]) + 1,
        strlen(TEST_STRINGS[7]) + 1
    };

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "local", TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage_api, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        storage_api->CloseFile(storage_api, w);
        w = 0;
    }

    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* version1_compression_types = GetAssetTags(storage_api, version1_paths);
    ASSERT_NE((uint32_t*)0, version1_compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
            0,
        0,
    "local",
        version1_paths,
        version1_compression_types,
        50,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(version1_compression_types);
    version1_compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        vindex,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, cindex);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "local"));
    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    TestAsyncFlushComplete asyncStoreFlushCB;
    ASSERT_EQ(0, async_block_store_api->Flush(async_block_store_api, &asyncStoreFlushCB.m_API));

    TestAsyncFlushComplete cacheStoreFlushCB;
    ASSERT_EQ(0, async_block_store_api->Flush(async_block_store_api, &cacheStoreFlushCB.m_API));

    TestAsyncFlushComplete compressStoreFlushCB;
    ASSERT_EQ(0, async_block_store_api->Flush(async_block_store_api, &compressStoreFlushCB.m_API));

    asyncStoreFlushCB.Wait();
    ASSERT_EQ(0, asyncStoreFlushCB.m_Err);
    cacheStoreFlushCB.Wait();
    ASSERT_EQ(0, cacheStoreFlushCB.m_Err);
    compressStoreFlushCB.Wait();
    ASSERT_EQ(0, compressStoreFlushCB.m_Err);

    ASSERT_EQ(0, Longtail_WriteVersion(
        block_store_api,
        storage_api,
        job_api,
        0,
        0,
        0,
        cindex,
        vindex,
        "remote",
        1));

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "remote", TEST_FILENAMES[i]);
        Longtail_StorageAPI_HOpenFile r;
        ASSERT_EQ(0, storage_api->OpenReadFile(storage_api, file_name, &r));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, r);
        uint64_t size;
        ASSERT_EQ(0, storage_api->GetSize(storage_api, r, &size));
        ASSERT_EQ(TEST_SIZES[i], size);
        char* test_data = (char*)Longtail_Alloc(sizeof(char) * size);
        if (size)
        {
            ASSERT_EQ(0, storage_api->Read(storage_api, r, 0, size, test_data));
            ASSERT_STREQ(TEST_STRINGS[i], test_data);
        }
        storage_api->CloseFile(storage_api, r);
        r = 0;
        Longtail_Free(test_data);
        test_data = 0;
    }

    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;
#endif

    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(compressed_block_store_api);
    SAFE_DISPOSE_API(async_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(cache_block_store);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, Longtail_WriteVersionShareBlocks)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* fs_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "chunks", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* block_store_api = Longtail_CreateShareBlockStoreAPI(fs_block_store_api);

    const uint32_t asset_count = 8u;

    const char* TEST_FILENAMES[] = {
        "TheLongFile.txt",
        "ShortString.txt",
        "AnotherSample.txt",
        "folder/ShortString.txt",
        "WATCHIOUT.txt",
        "empty/.init.py",
        "TheVeryLongFile.txt",
        "AnotherVeryLongFile.txt"
    };

    const char* TEST_STRINGS[] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "More than chunk less than block",
        "",
        "A very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end...",
        "Another very long string that should go over multiple blocks so we can test our super funky multi-threading version"
            "restore function that spawns a bunch of decompress jobs and makes the writes to disc sequentially using dependecies"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "so we write in good order but still use all our cores in a reasonable fashion. So this should be a long long string"
            "longer than seems reasonable, and here is a lot of rambling in this string. Because it is late and I just need to fill"
            "the string but make sure it actually comes back fine"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "repeat, repeat, repeate, endless repeat, and some more repeat. You need more? Yes, repeat!"
            "this is the end..."
    };

    const size_t TEST_SIZES[] = {
        strlen(TEST_STRINGS[0]) + 1,
        strlen(TEST_STRINGS[1]) + 1,
        strlen(TEST_STRINGS[2]) + 1,
        strlen(TEST_STRINGS[3]) + 1,
        strlen(TEST_STRINGS[4]) + 1,
        0,
        strlen(TEST_STRINGS[6]) + 1,
        strlen(TEST_STRINGS[7]) + 1
    };

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "local", TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage_api, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        storage_api->CloseFile(storage_api, w);
        w = 0;
    }

    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* version1_compression_types = GetAssetTags(storage_api, version1_paths);
    ASSERT_NE((uint32_t*)0, version1_compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "local",
        version1_paths,
        version1_compression_types,
        50,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(version1_compression_types);
    version1_compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        vindex,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, cindex);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "local"));

    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    ASSERT_EQ(0, Longtail_WriteVersion(
        block_store_api,
        storage_api,
        job_api,
        0,
        0,
        0,
        cindex,
        vindex,
        "remote",
        1));

    for (uint32_t i = 0; i < asset_count; ++i)
    {
        char* file_name = storage_api->ConcatPath(storage_api, "remote", TEST_FILENAMES[i]);
        Longtail_StorageAPI_HOpenFile r;
        ASSERT_EQ(0, storage_api->OpenReadFile(storage_api, file_name, &r));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, r);
        uint64_t size;
        ASSERT_EQ(0, storage_api->GetSize(storage_api, r, &size));
        ASSERT_EQ(TEST_SIZES[i], size);
        char* test_data = (char*)Longtail_Alloc(sizeof(char) * size);
        if (size)
        {
            ASSERT_EQ(0, storage_api->Read(storage_api, r, 0, size, test_data));
            ASSERT_STREQ(TEST_STRINGS[i], test_data);
        }
        storage_api->CloseFile(storage_api, r);
        r = 0;
        Longtail_Free(test_data);
        test_data = 0;
    }

    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;
    SAFE_DISPOSE_API(fs_block_store_api);
    SAFE_DISPOSE_API(block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, TestFullHashRegistry)
{
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    ASSERT_NE((struct Longtail_HashRegistryAPI*)0, hash_registry);
    struct Longtail_HashAPI* blake2_hash_api = 0;
    ASSERT_EQ(0, hash_registry->GetHashAPI(hash_registry, Longtail_GetBlake2HashType(), &blake2_hash_api));
    ASSERT_NE((struct Longtail_HashAPI*)0, blake2_hash_api);

    struct Longtail_HashAPI* blake3_hash_api = 0;
    ASSERT_EQ(0, hash_registry->GetHashAPI(hash_registry, Longtail_GetBlake3HashType(), &blake3_hash_api));
    ASSERT_NE((struct Longtail_HashAPI*)0, blake3_hash_api);

    struct Longtail_HashAPI* meow_hash_api = 0;
    ASSERT_EQ(0, hash_registry->GetHashAPI(hash_registry, Longtail_GetMeowHashType(), &meow_hash_api));
    ASSERT_NE((struct Longtail_HashAPI*)0, meow_hash_api);

    struct Longtail_HashAPI* error_hash_api = 0;
    ASSERT_EQ(ENOENT, hash_registry->GetHashAPI(hash_registry, 0xdeadbeefu, &error_hash_api));
    ASSERT_EQ((struct Longtail_HashAPI*)0, error_hash_api);

   SAFE_DISPOSE_API(hash_registry);
}

TEST(Longtail, TestBlake3HashRegistry)
{
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateBlake3HashRegistry();
    ASSERT_NE((struct Longtail_HashRegistryAPI*)0, hash_registry);
    struct Longtail_HashAPI* blake2_hash_api = 0;
    ASSERT_EQ(ENOENT, hash_registry->GetHashAPI(hash_registry, Longtail_GetBlake2HashType(), &blake2_hash_api));
    ASSERT_EQ((struct Longtail_HashAPI*)0, blake2_hash_api);

    struct Longtail_HashAPI* blake3_hash_api = 0;
    ASSERT_EQ(0, hash_registry->GetHashAPI(hash_registry, Longtail_GetBlake3HashType(), &blake3_hash_api));
    ASSERT_NE((struct Longtail_HashAPI*)0, blake3_hash_api);

    struct Longtail_HashAPI* meow_hash_api = 0;
    ASSERT_EQ(ENOENT, hash_registry->GetHashAPI(hash_registry, Longtail_GetMeowHashType(), &meow_hash_api));
    ASSERT_EQ((struct Longtail_HashAPI*)0, meow_hash_api);

    struct Longtail_HashAPI* error_hash_api = 0;
    ASSERT_EQ(ENOENT, hash_registry->GetHashAPI(hash_registry, 0xdeadbeefu, &error_hash_api));
    ASSERT_EQ((struct Longtail_HashAPI*)0, error_hash_api);

   SAFE_DISPOSE_API(hash_registry);
}

TEST(Longtail, TestCancelAPI)
{
    struct Longtail_CancelAPI* cancel_api = Longtail_CreateAtomicCancelAPI();
    ASSERT_NE((struct Longtail_CancelAPI*)0, cancel_api);
    Longtail_CancelAPI_HCancelToken cancel_token;
    ASSERT_EQ(0, cancel_api->CreateToken(cancel_api, &cancel_token));
    ASSERT_NE((Longtail_CancelAPI_HCancelToken)0, cancel_token);
    ASSERT_EQ(0, cancel_api->IsCancelled(cancel_api, cancel_token));
    ASSERT_EQ(0, cancel_api->Cancel(cancel_api, cancel_token));
    ASSERT_EQ(ECANCELED, cancel_api->IsCancelled(cancel_api, cancel_token));
    ASSERT_EQ(0, cancel_api->Cancel(cancel_api, cancel_token));
    ASSERT_EQ(0, cancel_api->Cancel(cancel_api, cancel_token));
    ASSERT_EQ(ECANCELED, cancel_api->IsCancelled(cancel_api, cancel_token));
    ASSERT_EQ(0, cancel_api->DisposeToken(cancel_api, cancel_token));
    SAFE_DISPOSE_API(cancel_api);
}

TEST(Longtail, TestFileScanCancelOperation)
{
    Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_CancelAPI* cancel_api = Longtail_CreateAtomicCancelAPI();
    ASSERT_NE((struct Longtail_CancelAPI*)0, cancel_api);
    Longtail_CancelAPI_HCancelToken cancel_token;
    ASSERT_EQ(0, cancel_api->CreateToken(cancel_api, &cancel_token));
    ASSERT_NE((Longtail_CancelAPI_HCancelToken)0, cancel_token);

    Longtail_FileInfos* file_infos;
    ASSERT_EQ(0, cancel_api->Cancel(cancel_api, cancel_token));
    ASSERT_EQ(ECANCELED, Longtail_GetFilesRecursively(storage_api, 0, cancel_api, cancel_token, "testdata", &file_infos));
    ASSERT_EQ(0, cancel_api->DisposeToken(cancel_api, cancel_token));
    SAFE_DISPOSE_API(cancel_api);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, TestCreateVersionCancelOperation)
{
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(2, 0);

    struct Longtail_CancelAPI* cancel_api = Longtail_CreateAtomicCancelAPI();
    ASSERT_NE((struct Longtail_CancelAPI*)0, cancel_api);
    Longtail_CancelAPI_HCancelToken cancel_token;
    ASSERT_EQ(0, cancel_api->CreateToken(cancel_api, &cancel_token));
    ASSERT_NE((Longtail_CancelAPI_HCancelToken)0, cancel_token);

    Longtail_FileInfos* file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, cancel_api, cancel_token, "testdata", &file_infos));
    ASSERT_NE(0, file_infos->m_Count);

    Longtail_VersionIndex* vindex = 0;

    HLongtail_Sema sema;
    ASSERT_EQ(0, Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()),0 ,&sema));

    struct JobContext
    {
        Longtail_StorageAPI* storage_api;
        struct Longtail_HashAPI* hash_api;
        struct Longtail_ChunkerAPI* chunker_api;
        Longtail_JobAPI* job_api;
        struct Longtail_CancelAPI* cancel_api;
        Longtail_CancelAPI_HCancelToken cancel_token;
        const char* root_path;
        Longtail_FileInfos* file_infos;
        Longtail_VersionIndex** vindex;
        int err;
        HLongtail_Sema sema;

        static int JobFunc(void* context, uint32_t job_id, int is_cancelled)
        {
            struct JobContext* job = (struct JobContext*)context;
            Longtail_WaitSema(job->sema, LONGTAIL_TIMEOUT_INFINITE);

            job->err = Longtail_CreateVersionIndex(
                job->storage_api,
                job->hash_api,
                job->chunker_api,
                job->job_api,
                0,
                job->cancel_api,
                job->cancel_token,
                job->root_path,
                job->file_infos,
                0,
                16384,
                job->vindex);
            return 0;
        }
    } job_context;
    job_context.storage_api = storage_api;
    job_context.hash_api = hash_api;
    job_context.chunker_api = chunker_api;
    job_context.job_api = job_api;
    job_context.cancel_api = cancel_api;
    job_context.cancel_token = cancel_token;
    job_context.root_path = "testdata";
    job_context.file_infos = file_infos;
    job_context.vindex = &vindex;
    job_context.sema = sema;

    Longtail_JobAPI_Group job_group;
    ASSERT_EQ(0, job_api->ReserveJobs(job_api, 1, &job_group));
    Longtail_JobAPI_JobFunc job_funcs[1] = {JobContext::JobFunc};
    void* job_ctxs[1] = {&job_context};
    Longtail_JobAPI_Jobs jobs;

    ASSERT_EQ(0, job_api->CreateJobs(job_api, job_group, 1, job_funcs, job_ctxs, &jobs));
    ASSERT_EQ(0, job_api->ReadyJobs(job_api, 1, jobs));
    ASSERT_EQ(0, cancel_api->Cancel(cancel_api, cancel_token));
    ASSERT_EQ(0, Longtail_PostSema(sema, 1));
    ASSERT_EQ(ECANCELED, job_api->WaitForAllJobs(job_api, job_group, 0, cancel_api, cancel_token));

    ASSERT_EQ(ECANCELED, job_context.err);
    ASSERT_EQ((Longtail_VersionIndex*)0, vindex);

    Longtail_DeleteSema(sema);
    Longtail_Free(sema);

    Longtail_Free(file_infos);

    ASSERT_EQ(0, cancel_api->DisposeToken(cancel_api, cancel_token));
    SAFE_DISPOSE_API(cancel_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, TestChangeVersionCancelOperation)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(32, -1);

    TestAsyncBlockStore async_block_store;
    ASSERT_EQ(0, TestAsyncBlockStore::InitBlockStore(&async_block_store, hash_api, job_api));
    struct Longtail_BlockStoreAPI* remote_block_store = &async_block_store.m_API;

    Longtail_BlockStoreAPI* local_block_store = Longtail_CreateFSBlockStoreAPI(job_api, storage, "cache", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* cache_block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store, remote_block_store);
    Longtail_BlockStoreAPI* compressed_remote_block_store = Longtail_CreateCompressBlockStoreAPI(remote_block_store, compression_registry);
    Longtail_BlockStoreAPI* compressed_cached_block_store = Longtail_CreateCompressBlockStoreAPI(cache_block_store_api, compression_registry);

    const uint32_t ASSET_COUNT = 22u;

    const char* TEST_FILENAMES[ASSET_COUNT] = {
        "ContentChangedSameLength.txt",
        "WillBeRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "OldRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe",
        "permissions_changed.txt",
        "ContentChangedSameLength2.txt",
        "WillBeRenamed2.txt",
        "ContentSameButShorter2.txt",
        "folder/ContentSameButLonger2.txt",
        "OldRenamedFolder/MovedToNewFolder2.txt",
        "JustDifferent2.txt",
        "EmptyFileInFolder/.init2.py",
        "a/file/in/folder/LongWithChangedStart2.dll",
        "a/file/in/other/folder/LongChangedAtEnd2.exe",
        "permissions_changed2.txt",
        "junk1.txt",
        "junk2.txt",
    };

    const char* TEST_STRINGS[ASSET_COUNT] = {
        "This is the first test string which is fairly long and should - reconstructed properly, than you very much",
        "Short string",
        "Another sample string that does not match any other string but -reconstructed properly, than you very much",
        "Short string",
        "This is the documentation we are all craving to understand the complexities of life",
        "More than chunk less than block",
        "",
        "A very long file that should be able to be recreated"
            "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17SSAXLECOGPNELECBC4GYS57kx3d0zxbikiglqjo86ztgjhq6m05ndt5qd2ge1v0cyw2wqliyryvbmvhg10nxm31jacxvjia7lgwk8n9"
            "tljvc3yz0bhobdg8bzfhvbyi6jvnljkq8rh7tcxhtvj7hffzuncn4si8dl7oyc8n0ufnlqQN3UJJYZXOB3KJVBIPSSWLTWUDZ9QKNIUQFZTFRXPXMBWWT9JJVK4SNDSSF1QPC4FC4FCQ"
            "XGGMT3YUA3LFV2ETFCYXZWOPGLTEGS93Q2SPLEEEQ21MYLAM3Q8TEOHFVJHUAGPNODK1RCKFF8YIRA2AQRGMDCZjhx2zue15i2mt0r8efhssf4qkws2hwm9j8rwj0b6kzcd81rfm4e4l"
            "6u0h2yl3kd9pashzik8lmxnazpwxivilgchxmyrlcx97cctaangmynkt6fw9w7cxxivxpaywuexykyadc0gxQMIZZEAKVY7DBDQXAAQKAIVDLDDFPXSOMSWGRWF3JHN5ELZYCJMWRSUC"
            "EKHHHQWV7SFH9WDDLBGQDNDZZIT7MQ7LMY6Q64XROC88XFIONUZ71GOTKPFUVAHUWRM9EAX4WXIPQPULEIMKZTWP1NXJCNNKSNWV9HXQSG8JHWDEGEIY9W8QXENZKALAWD4SVL3YVGBZ"
            "U1FBM4VSNGCJ11Pizxkibievkspzqaapv2reppc5d4me3cmpakjx4oe9javm93mftqxqj1pnnt4sayg13hkjhqc8snhdr2304opkgdgc1iwrtqlephvqdrbpnnafe1sjj5im6rracd8l"
            "sbu7diwmvghgwcn6xj8urfi5ihkgfsbmpkpnBHAMUUSWLZYXOFR7UO68OOZ9IFTUY86CXV98LKFZULBWWFULJ8IXZ2ZEA7qsbzayncw2skxqty5g0m2kqynv8dpjrl5h5m1mxxjfgeu6"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17SSAXLECOGPNELECBC4GYS57kx3d0zxbikiglqjo86ztgjhq6m05ndt5qd2ge1v0cyw2wqliyryvbmvhg10nxm31jacxvjia7lgwk8n9"
            "tljvc3yz0bhobdg8bzfhvbyi6jvnljkq8rh7tcxhtvj7hffzuncn4si8dl7oyc8n0ufnlqQN3UJJYZXOB3KJVBIPSSWLTWUDZ9QKNIUQFZTFRXPXMBWWT9JJVK4SNDSSF1QPC4FC4FCQ"
            "XGGMT3YUA3LFV2ETFCYXZWOPGLTEGS93Q2SPLEEEQ21MYLAM3Q8TEOHFVJHUAGPNODK1RCKFF8YIRA2AQRGMDCZjhx2zue15i2mt0r8efhssf4qkws2hwm9j8rwj0b6kzcd81rfm4e4l"
            "6u0h2yl3kd9pashzik8lmxnazpwxivilgchxmyrlcx97cctaangmynkt6fw9w7cxxivxpaywuexykyadc0gxqmizzeakvy7dbdqxaaqkaivdlddfpxsomswgrwf3jhn5elzycjmwrsuc"
            "ekhhhqwv7sfh9wddlbgqdndzzit7mq7lmy6q64xroc88xfionuz71gotkpfuvahuwrm9eax4wxipqpuleimkztwp1nxjcnnksnwv9hxqsg8jhwdegeiy9w8qxenzkalawd4svl3yvgbz"
            "u1fbm4vsngcj11pizxkibievkspzqaapv2reppc5d4me3cmpakjx4oe9javm93mftqxqj1pnnt4sayg13hkjhqc8snhdr2304opkgdgc1iwrtqlephvqdrbpnnafe1sjj5im6rracd8l"
            "sbu7diwmvghgwcn6xj8urfi5ihkgfsbmpkpnbhamuuswlzyxofr7uo68ooz9iftuy86cxv98lkfzulbwwfulj8ixz2zea7qsbzayncw2skxqty5g0m2kqynv8dpjrl5h5m1mxxjfgeu6"
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 5 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 6 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 7 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17SSAXLECOGPNELECBC4GYS57kx3d0zxbikiglqjo86ztgjhq6m05ndt5qd2ge1v0cyw2wqliyryvbmvhg10nxm31jacxvjia7lgwk8n9"
            "tljvc3yz0bhobdg8bzfhvbyi6jvnljkq8rh7tcxhtvj7hffzuncn4si8dl7oyc8n0ufnlqQN3UJJYZXOB3KJVBIPSSWLTWUDZ9QKNIUQFZTFRXPXMBWWT9JJVK4SNDSSF1QPC4FC4FCQ"
            "XGGMT3YUA3LFV2ETFCYXZWOPGLTEGS93Q2SPLEEEQ21MYLAM3Q8TEOHFVJHUAGPNODK1RCKFF8YIRA2AQRGMDCZjhx2zue15i2mt0r8efhssf4qkws2hwm9j8rwj0b6kzcd81rfm4e4l"
            "6u0h2yl3kd9pashzik8lmxnazpwxivilgchxmyrlcx97cctaangmynkt6fw9w7cxxivxpaywuexykyadc0gxQMIZZEAKVY7DBDQXAAQKAIVDLDDFPXSOMSWGRWF3JHN5ELZYCJMWRSUC"
            "EKHHHQWV7SFH9WDDLBGQDNDZZIT7MQ7LMY6Q64XROC88XFIONUZ71GOTKPFUVAHUWRM9EAX4WXIPQPULEIMKZTWP1NXJCNNKSNWV9HXQSG8JHWDEGEIY9W8QXENZKALAWD4SVL3YVGBZ"
            "U1FBM4VSNGCJ11Pizxkibievkspzqaapv2reppc5d4me3cmpakjx4oe9javm93mftqxqj1pnnt4sayg13hkjhqc8snhdr2304opkgdgc1iwrtqlephvqdrbpnnafe1sjj5im6rracd8l"
            "sbu7diwmvghgwcn6xj8urfi5ihkgfsbmpkpnBHAMUUSWLZYXOFR7UO68OOZ9IFTUY86CXV98LKFZULBWWFULJ8IXZ2ZEA7qsbzayncw2skxqty5g0m2kqynv8dpjrl5h5m1mxxjfgeu6"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
            "And in the end it is not the same, it is different, just because why not",
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "6aFOWGAnDmt05mJntcBzV3bNWU85fbP8kQu5HkH0MzD4SgYAJEB2QCwxF6udPKDIssZbOuvTkKPQQtU1RpCHqzhJYfuQ2iHuq9bcx7jVEekkf2nZpm8Vmczsg6CPBxkEhCdrYT546e86"
            "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17SSAXLECOGPNELECBC4GYS57kx3d0zxbikiglqjo86ztgjhq6m05ndt5qd2ge1v0cyw2wqliyryvbmvhg10nxm31jacxvjia7lgwk8n9"
            "tljvc3yz0bhobdg8bzfhvbyi6jvnljkq8rh7tcxhtvj7hffzuncn4si8dl7oyc8n0ufnlqQN3UJJYZXOB3KJVBIPSSWLTWUDZ9QKNIUQFZTFRXPXMBWWT9JJVK4SNDSSF1QPC4FC4FCQ"
            "XGGMT3YUA3LFV2ETFCYXZWOPGLTEGS93Q2SPLEEEQ21MYLAM3Q8TEOHFVJHUAGPNODK1RCKFF8YIRA2AQRGMDCZJHX2ZUE15I2MT0R8EFHSSF4QKWS2HWM9J8RWJ0B6KZCD81RFM4E4L"
            "6U0H2YL3KD9PASHZIK8LMXNAZPWXIVILGCHXMYRLCX97CCTAANGMYNKT6FW9W7CXXIVXPAYWUEXYKYADC0GXQMIZZEAKVY7DBDQXAAQKAIVDLDDFPXSOMSWGRWF3JHN5ELZYCJMWRSUC"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE SIXTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE EIGTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE NINTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE TENTH TIME YOU see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough...",
        "CONTENT STAYS THE SAME BUT PERMISSIONS CHANGE",
        "THIS IS THE FIRST TEST STRING WHICH IS FAIRLY LONG AND SHOULD - RECONSTRUCTED PROPERLY, THAN YOU VERY MUCH",
        "SHORT STRING",
        "ANOTHER SAMPLE STRING THAT DOES NOT MATCH ANY OTHER STRING BUT -RECONSTRUCTED PROPERLY, THAN YOU VERY MUCH",
        "SHORT STRING",
        "THIS IS THE DOCUMENTATION WE ARE ALL CRAVING TO UNDERSTAND THE COMPLEXITIES OF LIFE",
        "MORE THAN CHUNK LESS THAN BLOCK",
        "",
        "A VERY LONG FILE THAT SHOULD BE ABLE TO BE RECREATED"
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 2 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 3 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 4 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME good, some bad but still it is repeating. this is the number 5 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 6 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 7 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 8 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but still it is repeating. this is the number 9 in a long sequence of stuff."
            "lots of repeating stuff, some good, some bad but stILL IT IS REPEATING. THIS IS THE NUMBER 10 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 11 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 12 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 13 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 14 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 15 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 16 IN A LONG SEQUENCE OF STUFF."
            "AND IN THE END IT IS NOT THE SAME, IT IS DIFFERENT, JUST BECAUSE WHY NOT",
        "A VERY LONG FILE THAT SHOULD BE ABLE TO BE RECREATED"
            "ANOTHER BIG FILE BUT THIS DOES NOT CONTAIN THE DATA AS THE ONE ABOVE, HOWEVER IT DOES START OUT THE SAME AS THE OTHER FILE,RIGHT?"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FIRST TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE SECOND TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE THIRD TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FOURTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FIFTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "6aFOWGAnDmt05mJntcBzV3bNWU85fbP8kQu5HkH0MzD4SgYAJEB2QCwxF6udPKDIssZbOuvTkKPQQtU1RpCHqzhJYfuQ2iHuq9bcx7jVEekkf2nZpm8Vmczsg6CPBxkEhCdrYT546e86"
            "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17sSAxLEcogpnelEcBc4gYs57Kx3D0ZxbikiglQJo86ZTgJhq6m05NDT5qD2gE1V0CYw2WQlIyRYVbMvHg10NxM31JacXVJiA7lGWK8N9"
            "TLJVC3YZ0bhOBDG8bzfHvBYI6jVNlJKQ8RH7tCXhtvj7HffZuncn4si8DL7Oyc8N0UfNLqqn3ujJYzXOb3kJvBiPSSWlTwuDz9QkNiuqFzTFRXpxMbwwt9jjVK4SnDssf1Qpc4Fc4fCq"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE SIXTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE EIGTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE NINTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE TENTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE ELEVTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE TWELTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "I REALIZE I'M NOT VERY GOOD AT WRITING OUT THE NUMBERING WITH THE 'TH STUFF AT THE END. NOT MUCH REASON TO USE THAT BEFORE."
            "0123456789876543213241247632464358091345+2438568736283249873298NTYVNTRNDWOIY78N43CTYERMDR498XRNHSE78TNLS43TC49MJRX3HCNTHV4T"
            "LIURHE NGVH43OECGCLRI8FHSO7R8AB3GWC409NU3P9T757NVV74OE8NFYIECFFÖMOCSRHF ,JSYVBLSE4TMOXW3UMRC9SEN8TYN8ÖOERUCDLC4IGTCOV8EVRNOCS8LHRF"
            "THAT WILL LOOK LIKE GARBAGE, WILL THAT REALLY BE A GOOD IDEA?"
            "THIS IS THE END TOUGH...",
        "CONTENT STAYS THE SAME BUT PERMISSIONS CHANGE",
        "OVOP5VDVCzTCqmpV1Dm7eci7QMEyI20BTGigIUka6raZYtFYbKsfn1c40AMGLxrqXFCXkpKYe9GQJjmlEiRmrj8hR7fuQO8fYJ3Z79BUmps3vy3FNb4fGnZJmDbmKzyCkb2rZjGE4kbC"
        "axXWo74MfsqUxPA5BxDCKkaE4gm531RUCwTkpnsvw9YjE3IT0m04vJVDQWRyBOHpj5nxkUSrEFlEMVNUgZYe8yZ6l9UQelgkifq8wtuREH3vFuZcNEsJI2whRQ8d7NxKAra5KK857nsZ"
        "dZFZu6UYyFLTPwqBJV2CxcFpgZVkkeo1qC7Wpxon9VhX2Ooxq8VL4XEE85GLVdCifhDChav260O6bj2yYpmcTF8lEZX1WXVaQTFCEwnJ35E2iwoc95DXztXHywMskG8tpkYie7AGyH4v"
        "WViKKW5apwjwkpvNjWpL0j0ukyuCZB0ATLY2xPyzBx9v0fqafa6q5HOKooXpvkinhGSu6eoYVQ9dFm0yLzOmjxlWpSPBbGZBkjCXZ6lAksVkmmLiazp7W6kWnyuAPkHuTexYeRCGduXn"
        "6aFOWGAnDmt05mJntcBzV3bNWU85fbP8kQu5HkH0MzD4SgYAJEB2QCwxF6udPKDIssZbOuvTkKPQQtU1RpCHqzhJYfuQ2iHuq9bcx7jVEekkf2nZpm8Vmczsg6CPBxkEhCdrYT546e86"
        "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17sSAxLEcogpnelEcBc4gYs57Kx3D0ZxbikiglQJo86ZTgJhq6m05NDT5qD2gE1V0CYw2WQlIyRYVbMvHg10NxM31JacXVJiA7lGWK8N9"
        "TLJVC3YZ0bhOBDG8bzfHvBYI6jVNlJKQ8RH7tCXhtvj7HffZuncn4si8DL7Oyc8N0UfNLqqn3ujJYzXOb3kJvBiPSSWlTwuDz9QkNiuqFzTFRXpxMbwwt9jjVK4SnDssf1Qpc4Fc4fCq"
        "xGgMt3YuA3LfV2eTfcYxZwOPGLtEGs93q2splEeeq21mYLam3q8tEOhFVJHUaGPnOdk1RCkFF8yIra2aqrgMdcZJHx2zue15I2Mt0r8EFhSSf4QKws2hwm9J8rwJ0b6kZCD81rFm4E4L"
        "6U0h2YL3KD9paSHzIK8LMXNaZPWXIvIlgchXMYRlCx97cctaaNGmynkt6fW9W7cxxiVxpaYwuExyKYAdC0GXqmIZZEAkVy7dBDQxaaqKaIvDLdDFPXsoMSwgRWf3Jhn5ELZycjmwrSuC"
        "ekHhhQWV7SFH9WddLbGqdNDzZiT7MQ7LmY6Q64xRoC88xfiOnUZ71GotKpfUVAhUWRM9eAX4WXiPqPULeiMkzTWP1NXjCnnksnwV9HXQSG8JHWdeGeIy9W8QxENZKalaWd4sVl3yvGBz"
        "u1fbm4vsNGCJ11pIZXkiBIeVksPzqaapV2rEpPC5d4ME3cmpAkjx4oE9JAVM93MFTQxqJ1pNNt4SaYG13hKjhqc8sNHDR2304oPkGDgc1iwrTqlEPHVQDRBpnNAFe1sJJ5IM6rraCd8L"
        "sBU7DIwmvghGWCN6xj8uRFI5ihkGfsbmPkPNBhaMUuswlZyxoFR7UO68oOz9iftuY86cXv98lkfZulBWWfuLj8Ixz2ZEA7QsbZAYNcw2sKxQTY5G0m2kQyNv8DPJRl5h5m1mxXjfgeu6"
        "v5TqERzcvdwmDAlnt5AeSTYkDQ4cnpjnYEJdB2tf6hYVLokWIZSWSGAEmDMQ8vZwZVaUysxDPLc59z9ZnO7UYSJybQShASKUYjpse4L4CA8cs16votWhMfpEz8QFFEqjdOLJr5u2cVYX"
        "dKbpdIn0A68f2o4HHQQI5U7IrY2MgS6j9VQtQoKySgKcSBeZ4b6GwObRN1YR4kq05zq4bS4L99LaVNCkmn0RXxyWX9MNd81ivrlL5phB5ljlPa68ILTJ8v9iOodBNeMIXC8peINfwS5R"
        "yuMCpEwPCDJBdRjaaw07gdKPUaG0IybnqH2n25CF",
        "OVOP5VDVCzTCqmpV1Dm7eci7QMEyI20BTGigIUka6raZYtFYbKsfn1c40AMGLxrqXFCXkpKYe9GQJjmlEiRmrj8hR7fuQO8fYJ3Z79BUmps3vy3FNb4fGnZJmDbmKzyCkb2rZjGE4kbC"
        "axXWo74MfsqUxPA5BxDCKkaE4gm531RUCwTkpnsvw9YjE3IT0m04vJVDQWRyBOHpj5nxkUSrEFlEMVNUgZYe8yZ6l9UQelgkifq8wtuREH3vFuZcNEsJI2whRQ8d7NxKAra5KK857nsZ"
        "dZFZu6UYyFLTPwqBJV2CxcFpgZVkkeo1qc7wpxon9vhx2ooxq8vl4xee85glvdcifhdchav260o6bj2yypmctf8lezx1wxvaqtfcewnj35e2iwoc95dxztxhywmskg8tpkyie7agyh4v"
        "wvikkw5apwjwkpvnjwpl0j0ukyuczb0ATLY2xPyzBx9v0fqafa6q5HOKooXpvkinhGSu6eoYVQ9dFm0yLzOmjxlWpSPBbGZBkjCXZ6lAksVkmmLiazp7W6kWnyuAPkHuTexYeRCGduXn"
        "6aFOWGAnDmt05mJntcBzV3bNWU85fbP8kQu5HkH0MzD4SgYAJEB2QCwxF6udPKDIssZbOuvTkKPQQtU1RpCHqzhJYfuQ2iHuq9bcx7jVEekkf2nZpm8Vmczsg6CPBxkEhCdrYT546e86"
        "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17SSAXLECOGPNELECBC4GYS57kx3d0zxbikiglqjo86ztgjhq6m05ndt5qd2ge1v0cyw2wqliyryvbmvhg10nxm31jacxvjia7lgwk8n9"
        "tljvc3yz0bhobdg8bzfhvbyi6jvnljkq8rh7tcxhtvj7hffzuncn4si8dl7oyc8n0ufnlqQN3UJJYZXOB3KJVBIPSSWLTWUDZ9QKNIUQFZTFRXPXMBWWT9JJVK4SNDSSF1QPC4FC4FCQ"
        "XGGMT3YUA3LFV2ETFCYXZWOPGLTEGS93Q2SPLEEEQ21MYLAM3Q8TEOHFVJHUAGPNODK1RCKFF8YIRA2AQRGMDCZjhx2zue15i2mt0r8efhssf4qkws2hwm9j8rwj0b6kzcd81rfm4e4l"
        "6u0h2yl3kd9pashzik8lmxnazpwxivilgchxmyrlcx97cctaangmynkt6fw9w7cxxivxpaywuexykyadc0gxQMIZZEAKVY7DBDQXAAQKAIVDLDDFPXSOMSWGRWF3JHN5ELZYCJMWRSUC"
        "EKHHHQWV7SFH9WDDLBGQDNDZZIT7MQ7LMY6Q64XROC88XFIONUZ71GOTKPFUVAHUWRM9EAX4WXIPQPULEIMKZTWP1NXJCNNKSNWV9HXQSG8JHWDEGEIY9W8QXENZKALAWD4SVL3YVGBZ"
        "U1FBM4VSNGCJ11Pizxkibievkspzqaapv2reppc5d4me3cmpakjx4oe9javm93mftqxqj1pnnt4sayg13hkjhqc8snhdr2304opkgdgc1iwrtqlephvqdrbpnnafe1sjj5im6rracd8l"
        "sbu7diwmvghgwcn6xj8urfi5ihkgfsbmpkpnBHAMUUSWLZYXOFR7UO68OOZ9IFTUY86CXV98LKFZULBWWFULJ8IXZ2ZEA7qsbzayncw2skxqty5g0m2kqynv8dpjrl5h5m1mxxjfgeu6"
        "v5tqerzcvdwmdalnt5aestykdq4cnpjnyejdb2tf6hyvlokwizswsgaemdmq8vzwzvauysxdplc59z9zno7uysjybqshaskuyjpse4l4ca8CS16VOTWHMFPEZ8QFFEQJDOLJR5U2CVYX"
        "DKBPDIN0A68F2O4HHQQI5U7IRY2MGS6J9VQTQOKYSGKCSBEZ4B6GWOBRN1YR4KQ05ZQ4BS4L99LAVNCKMN0RXXYWX9MND81IVRLL5PHB5LJLPA68ILTJ8V9IOODBNEMIXC8PEINFWS5R"
        "YUMCPEWPCDJBDRJAAW07GDKPUAG0IYBNQH2N25CF"
    };

    const size_t TEST_SIZES[ASSET_COUNT] = {
        strlen(TEST_STRINGS[0]) + 1,
        strlen(TEST_STRINGS[1]) + 1,
        strlen(TEST_STRINGS[2]) + 1,
        strlen(TEST_STRINGS[3]) + 1,
        strlen(TEST_STRINGS[4]) + 1,
        strlen(TEST_STRINGS[5]) + 1,
        strlen(TEST_STRINGS[6]) + 1,
        strlen(TEST_STRINGS[7]) + 1,
        strlen(TEST_STRINGS[8]) + 1,
        strlen(TEST_STRINGS[9]) + 1,
        strlen(TEST_STRINGS[10]) + 1,
        strlen(TEST_STRINGS[11]) + 1,
        strlen(TEST_STRINGS[12]) + 1,
        strlen(TEST_STRINGS[13]) + 1,
        strlen(TEST_STRINGS[14]) + 1,
        strlen(TEST_STRINGS[15]) + 1,
        strlen(TEST_STRINGS[16]) + 1,
        strlen(TEST_STRINGS[17]) + 1,
        strlen(TEST_STRINGS[18]) + 1,
        strlen(TEST_STRINGS[19]) + 1,
        strlen(TEST_STRINGS[20]) + 1,
        strlen(TEST_STRINGS[21]) + 1
    };

    const uint16_t TEST_PERMISSIONS[ASSET_COUNT] = {
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0755,
        0646,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0644,
        0755,
        0646,
        0644,
        0644
    };

    for (uint32_t i = 0; i < ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "source", TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
        storage->SetPermissions(storage, file_name, TEST_PERMISSIONS[i]);
        Longtail_Free(file_name);
    }

    Longtail_FileInfos* version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "source", &version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version_paths);
    uint32_t* compression_types = SetAssetTags(storage, version_paths, Longtail_GetLZ4DefaultQuality());
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "source",
        version_paths,
        compression_types,
        16,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(compression_types);
    compression_types = 0;
    Longtail_Free(version_paths);
    version_paths = 0;

    Longtail_ContentIndex* version_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            vindex,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &version_content_index));

    Longtail_FileInfos* current_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, 0, 0, 0, "current", &current_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, current_version_paths);
    uint32_t* current_compression_types = GetAssetTags(storage, current_version_paths);
    ASSERT_NE((uint32_t*)0, current_compression_types);
    Longtail_VersionIndex* current_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "current",
        current_version_paths,
        current_compression_types,
        16,
        &current_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, current_vindex);
    Longtail_Free(current_compression_types);
    current_compression_types = 0;
    Longtail_Free(current_version_paths);
    current_version_paths = 0;

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(compressed_cached_block_store, version_content_index);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage,
        compressed_cached_block_store,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "source"));
    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;
    Longtail_Free(version_content_index);
    version_content_index = 0;

    Longtail_VersionDiff* version_diff;
    ASSERT_EQ(0, Longtail_CreateVersionDiff(
        hash_api,
        current_vindex,
        vindex,
        &version_diff));
    ASSERT_NE((Longtail_VersionDiff*)0, version_diff);

    ASSERT_EQ(0, Longtail_CreateContentIndexFromDiff(
        hash_api,
        vindex,
        version_diff,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));
    
    block_store_content_index = SyncRetargetContent(compressed_cached_block_store, content_index);
    Longtail_Free(content_index);
    content_index = 0;

    struct Longtail_CancelAPI* cancel_api = Longtail_CreateAtomicCancelAPI();
    ASSERT_NE((struct Longtail_CancelAPI*)0, cancel_api);

    struct BlockStoreProxy
    {
        struct Longtail_BlockStoreAPI m_API;
        struct Longtail_BlockStoreAPI* m_Base;
        TLongtail_Atomic32 m_FailCounter;
        static void Dispose(struct Longtail_API* api) { }
        static int PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
        {
            struct BlockStoreProxy* api = (struct BlockStoreProxy*)block_store_api;
            return api->m_Base->PutStoredBlock(api->m_Base, stored_block, async_complete_api);
        }
        static int PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index)
        {
            struct BlockStoreProxy* api = (struct BlockStoreProxy*)block_store_api;
            return api->m_Base->PreflightGet(api->m_Base, content_index);
        }
        static int GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
        {
            struct BlockStoreProxy* api = (struct BlockStoreProxy*)block_store_api;
            if (Longtail_AtomicAdd32(&api->m_FailCounter, -1) <= 0)
            {
                return ECANCELED;
            }
            return api->m_Base->GetStoredBlock(api->m_Base, block_hash, async_complete_api);
        }
        static int RetargetContent(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index, struct Longtail_AsyncRetargetContentAPI* async_complete_api)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "CompressBlockStore_RetargetContent(%p, %p, %p)",
                block_store_api, content_index, async_complete_api)
            LONGTAIL_VALIDATE_INPUT(block_store_api, return EINVAL)
            LONGTAIL_VALIDATE_INPUT(content_index, return EINVAL)
            LONGTAIL_VALIDATE_INPUT(async_complete_api, return EINVAL)
            struct BlockStoreProxy* api = (struct BlockStoreProxy*)block_store_api;
            return api->m_Base->RetargetContent(api->m_Base, content_index, async_complete_api);
        }
        static int GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
        {
            struct BlockStoreProxy* api = (struct BlockStoreProxy*)block_store_api;
            return api->m_Base->GetStats(api->m_Base, out_stats);
        }
        static int Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
        {
            async_complete_api->OnComplete(async_complete_api, 0);
            return 0;
        }
    } blockStoreProxy;
    blockStoreProxy.m_Base = compressed_cached_block_store;
    blockStoreProxy.m_FailCounter = 2;
    struct Longtail_BlockStoreAPI* block_store_proxy = Longtail_MakeBlockStoreAPI(
        &blockStoreProxy,
        BlockStoreProxy::Dispose,
        BlockStoreProxy::PutStoredBlock,
        BlockStoreProxy::PreflightGet,
        BlockStoreProxy::GetStoredBlock,
        BlockStoreProxy::RetargetContent,
        BlockStoreProxy::GetStats,
        BlockStoreProxy::Flush);

    {
        Longtail_CancelAPI_HCancelToken cancel_token;
        ASSERT_EQ(0, cancel_api->CreateToken(cancel_api, &cancel_token));
        ASSERT_NE((Longtail_CancelAPI_HCancelToken)0, cancel_token);

        ASSERT_EQ(ECANCELED, Longtail_ChangeVersion(
            block_store_proxy,
            storage,
            hash_api,
            job_api,
            0,
            cancel_api,
            cancel_token,
            block_store_content_index,
            current_vindex,
            vindex,
            version_diff,
            "old",
            1));
        cancel_api->DisposeToken(cancel_api, cancel_token);
    }

    struct TestCancelAPI
    {
        struct Longtail_CancelAPI m_API;
        struct Longtail_CancelAPI* m_BackingAPI;
        TLongtail_Atomic32 m_QueryCounter;
        int32_t m_PassCount;
        TestCancelAPI(struct Longtail_CancelAPI* backingAPI, int passCount)
            : m_BackingAPI(backingAPI)
            , m_QueryCounter(0)
            , m_PassCount(passCount)
        {
            Longtail_MakeCancelAPI(this,
                Dispose,
                CreateToken,
                Cancel,
                IsCancelled,
                DisposeToken);
        }
        static void Dispose(struct Longtail_API* longtail_api)
        {
            struct TestCancelAPI* api = (struct TestCancelAPI*)longtail_api;
            SAFE_DISPOSE_API(api->m_BackingAPI);
        }
        static int CreateToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken* out_token)
        {
            struct TestCancelAPI* api = (struct TestCancelAPI*)cancel_api;
            return Longtail_CancelAPI_CreateToken(api->m_BackingAPI, out_token);
        }
        static int Cancel(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
        {
            struct TestCancelAPI* api = (struct TestCancelAPI*)cancel_api;
            return Longtail_CancelAPI_Cancel(api->m_BackingAPI, token);
        }
        static int IsCancelled(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
        {
            struct TestCancelAPI* api = (struct TestCancelAPI*)cancel_api;
            if (Longtail_AtomicAdd32(&api->m_QueryCounter, 1) > api->m_PassCount)
            {
                return ECANCELED;
            }
            return Longtail_CancelAPI_IsCancelled(api->m_BackingAPI, token);
        }
        static int DisposeToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
        {
            struct TestCancelAPI* api = (struct TestCancelAPI*)cancel_api;
            return Longtail_CancelAPI_DisposeToken(api->m_BackingAPI, token);
        }
    };

    for (int t = 0; t < 0x7fffffff; ++t)
    {
        TestCancelAPI testCancelAPI(cancel_api, t);
        Longtail_CancelAPI_HCancelToken cancel_token;
        ASSERT_EQ(0, testCancelAPI.m_API.CreateToken(&testCancelAPI.m_API, &cancel_token));
        ASSERT_NE((Longtail_CancelAPI_HCancelToken)0, cancel_token);

        blockStoreProxy.m_FailCounter = 0x7fffffff;
        int err = Longtail_ChangeVersion(
            block_store_proxy,
            storage,
            hash_api,
            job_api,
            0,
            &testCancelAPI.m_API,
            cancel_token,
            block_store_content_index,
            current_vindex,
            vindex,
            version_diff,
            "old",
            1);
        testCancelAPI.m_API.DisposeToken(&testCancelAPI.m_API, cancel_token);
        if (err == ECANCELED)
        {
            continue;
        }
        ASSERT_EQ(0, err);
        break;
    }

    SAFE_DISPOSE_API(block_store_proxy);
    SAFE_DISPOSE_API(cancel_api);
    Longtail_Free(block_store_content_index);
    Longtail_Free(version_diff);
    Longtail_Free(current_vindex);
    Longtail_Free(vindex);
    SAFE_DISPOSE_API(compressed_cached_block_store);
    SAFE_DISPOSE_API(compressed_remote_block_store);
    SAFE_DISPOSE_API(cache_block_store_api);
    SAFE_DISPOSE_API(local_block_store);
    SAFE_DISPOSE_API(remote_block_store);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage);
}
#if 0
TEST(Longtail, BlockStoreRetargetContent)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "local", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* remote_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "remote", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* cached_block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store_api, remote_block_store_api);

    const uint32_t ASSET_COUNT = 4u;

    const char* TEST_FILENAMES[] = {
        "junk1.txt",
        "junk2.txt",
        "junk3.txt",
        "junk4.txt"
    };

    const char* TEST_STRINGS[] = {
        "A very long file that should be able to be recreated"
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 2 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 3 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 4 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 5 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 6 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 7 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 8 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 9 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 10 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 11 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 12 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 13 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 14 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 15 in a long sequence of stuff."
            "Lots of repeating stuff, some good, some bad but still it is repeating. This is the number 16 in a long sequence of stuff."
            "And in the end it is not the same, it is different, just because why not",
        "A VERY LONG FILE THAT SHOULD BE ABLE TO BE RECREATED"
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 2 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 3 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 4 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 5 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 6 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 7 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 8 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 9 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 10 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 11 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 12 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 13 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 14 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 15 IN A LONG SEQUENCE OF STUFF."
            "LOTS OF REPEATING STUFF, SOME GOOD, SOME BAD BUT STILL IT IS REPEATING. THIS IS THE NUMBER 16 IN A LONG SEQUENCE OF STUFF."
            "AND IN THE END IT IS NOT THE SAME, IT IS DIFFERENT, JUST BECAUSE WHY NOT",
        "A very long file that should be able to be recreated"
            "Another big file but this does not contain the data as the one above, however it does start out the same as the other file,right?"
            "Yet we also repeat this line, this is the first time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the second time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the third time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fourth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the fifth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the sixth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the eigth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the ninth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the tenth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the elevth time you see this, but it will also show up again and again with only small changes"
            "Yet we also repeat this line, this is the twelth time you see this, but it will also show up again and again with only small changes"
            "I realize I'm not very good at writing out the numbering with the 'th stuff at the end. Not much reason to use that before."
            "0123456789876543213241247632464358091345+2438568736283249873298ntyvntrndwoiy78n43ctyermdr498xrnhse78tnls43tc49mjrx3hcnthv4t"
            "liurhe ngvh43oecgclri8fhso7r8ab3gwc409nu3p9t757nvv74oe8nfyiecffömocsrhf ,jsyvblse4tmoxw3umrc9sen8tyn8öoerucdlc4igtcov8evrnocs8lhrf"
            "That will look like garbage, will that really be a good idea?"
            "This is the end tough...",
        "A VERY LONG FILE THAT SHOULD BE ABLE TO BE RECREATED"
            "ANOTHER BIG FILE BUT THIS DOES NOT CONTAIN THE DATA AS THE ONE ABOVE, HOWEVER IT DOES START OUT THE SAME AS THE OTHER FILE,RIGHT?"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FIRST TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE SECOND TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE THIRD TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FOURTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE FIFTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE SIXTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE EIGTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE NINTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE TENTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE ELEVTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "YET WE ALSO REPEAT THIS LINE, THIS IS THE TWELTH TIME YOU SEE THIS, BUT IT WILL ALSO SHOW UP AGAIN AND AGAIN WITH ONLY SMALL CHANGES"
            "I REALIZE I'M NOT VERY GOOD AT WRITING OUT THE NUMBERING WITH THE 'TH STUFF AT THE END. NOT MUCH REASON TO USE THAT BEFORE."
            "0123456789876543213241247632464358091345+2438568736283249873298NTYVNTRNDWOIY78N43CTYERMDR498XRNHSE78TNLS43TC49MJRX3HCNTHV4T"
            "LIURHE NGVH43OECGCLRI8FHSO7R8AB3GWC409NU3P9T757NVV74OE8NFYIECFFÖMOCSRHF ,JSYVBLSE4TMOXW3UMRC9SEN8TYN8ÖOERUCDLC4IGTCOV8EVRNOCS8LHRF"
            "THAT WILL LOOK LIKE GARBAGE, WILL THAT REALLY BE A GOOD IDEA?"
            "THIS IS THE END TOUGH..."
    };

    const char* root_path_1 = "testdata1";
    for (uint32_t i = 0; i < ASSET_COUNT / 2; ++i)
    {
        const char* file_name = storage_api->ConcatPath(storage_api, root_path_1, TEST_FILENAMES[i]);
        ASSERT_EQ(0, EnsureParentPathExists(storage_api, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, strlen(TEST_STRINGS[i]), TEST_STRINGS[i]));
        storage_api->CloseFile(storage_api, w);
        w = 0;
        Longtail_Free((void*)file_name);
    }

    const char* root_path_2 = "testdata2";
    for (uint32_t i = 0; i < ASSET_COUNT; ++i)
    {
        const char* file_name = storage_api->ConcatPath(storage_api, root_path_2, TEST_FILENAMES[i]);
        ASSERT_EQ(0, EnsureParentPathExists(storage_api, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, strlen(TEST_STRINGS[i]), TEST_STRINGS[i]));
        storage_api->CloseFile(storage_api, w);
        w = 0;
        Longtail_Free((void*)file_name);
    }

    const char* root_path_3 = "testdata3";

/*    struct Longtail_ContentIndex* cache_store_index = SyncGetContentIndex(cached_block_store_api);
    struct Longtail_ContentIndex* remote_store_index = SyncGetContentIndex(remote_block_store_api);
    struct Longtail_ContentIndex* local_store_index = SyncGetContentIndex(local_block_store_api);*/

    struct Longtail_FileInfos* file_infos_1;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, root_path_1, &file_infos_1));

    struct Longtail_VersionIndex* version_index_1;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        root_path_1,
        file_infos_1,
        0,
        128,
        &version_index_1));
    Longtail_Free(file_infos_1);

    struct Longtail_ContentIndex* version_1_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version_index_1,
        *cache_store_index->m_MaxBlockSize,
        *cache_store_index->m_MaxChunksPerBlock,
        &version_1_content_index));

    struct Longtail_ContentIndex* retarget_cache_content_index = SyncRetargetContent(
        cached_block_store_api,
        version_1_content_index);
    ASSERT_EQ(0, *retarget_cache_content_index->m_BlockCount);
    ASSERT_EQ(0, *retarget_cache_content_index->m_ChunkCount);

    // Fill remote
    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        remote_block_store_api,
        hash_api,
        job_api,
        0,
        0,
        0,
        remote_store_index,
        version_1_content_index,
        version_index_1,
        root_path_1));

    Longtail_Free(retarget_cache_content_index);
    retarget_cache_content_index = SyncRetargetContent(cached_block_store_api, version_1_content_index);
    ASSERT_EQ(*version_1_content_index->m_BlockCount, *retarget_cache_content_index->m_BlockCount);
    ASSERT_EQ(*version_1_content_index->m_ChunkCount, *retarget_cache_content_index->m_ChunkCount);

    struct Longtail_FileInfos* file_infos_2;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, root_path_2, &file_infos_2));

    struct Longtail_VersionIndex* version_index_2;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        root_path_2,
        file_infos_2,
        0,
        *version_index_1->m_TargetChunkSize,
        &version_index_2));
    Longtail_Free(file_infos_2);

    struct Longtail_ContentIndex* version_2_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version_index_2,
        *cache_store_index->m_MaxBlockSize * 16,
        *cache_store_index->m_MaxChunksPerBlock * 8,
        &version_2_content_index));
    ASSERT_NE(*retarget_cache_content_index->m_BlockCount, *version_2_content_index->m_BlockCount);
    ASSERT_LT(*retarget_cache_content_index->m_ChunkCount, *version_2_content_index->m_ChunkCount);

    Longtail_Free(remote_store_index);
    remote_store_index = SyncRetargetContent(remote_block_store_api, version_2_content_index);
    // Fill remote
    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        remote_block_store_api,
        hash_api,
        job_api,
        0,
        0,
        0,
        remote_store_index,
        version_2_content_index,
        version_index_2,
        root_path_2));
    Longtail_Free(remote_store_index);
    remote_store_index = 0;

    struct Longtail_FileInfos* file_infos_3;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, root_path_3, &file_infos_3));

    struct Longtail_VersionIndex* version_index_3;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        root_path_3,
        file_infos_3,
        0,
        *version_index_1->m_TargetChunkSize,
        &version_index_3));
    Longtail_Free(file_infos_3);

    struct Longtail_VersionDiff* version_diff;
    ASSERT_EQ(0, Longtail_CreateVersionDiff(hash_api, version_index_3, version_index_1, &version_diff));

    // We build a content index for our target version without having access to the remote index
    struct Longtail_ContentIndex* version_1_update_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version_index_1,
        48, // Can be arbitrary - we use blockstoreapi->RetargetContent to make it fall in line
        7, // Can be arbitrary - we use blockstoreapi->RetargetContent to make it fall in line
        &version_1_update_content_index));
    struct Longtail_ContentIndex* update_content_index_3 = SyncRetargetContent(cached_block_store_api, version_1_update_content_index);

/*    Longtail_Free(local_store_index);
    local_store_index = SyncGetContentIndex(local_block_store_api);
    struct Longtail_ContentIndex* cache_missing_content_index;
    ASSERT_EQ(0, Longtail_GetMissingContent(hash_api, local_store_index, update_content_index_3, &cache_missing_content_index));

    struct Longtail_ContentIndex* missing_update_content_index_3 = SyncRetargetContent(cached_block_store_api, cache_missing_content_index);
    // Everything is missing so we should get all we got from the retarget operation
    ASSERT_EQ(*update_content_index_3->m_BlockCount, *cache_missing_content_index->m_BlockCount);
    ASSERT_EQ(*update_content_index_3->m_ChunkCount, *cache_missing_content_index->m_ChunkCount);*/

    ASSERT_EQ(0, Longtail_ChangeVersion(
        cached_block_store_api,
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        0,
        update_content_index_3,
        version_index_3,
        version_index_1,
        version_diff,
        root_path_3,
        0));

    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, 0, 0, 0, root_path_3, &file_infos_3));
    Longtail_Free(version_index_3);
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        root_path_3,
        file_infos_3,
        0,
        *version_index_2->m_TargetChunkSize,
        &version_index_3));
    Longtail_Free(file_infos_3);

    Longtail_Free(version_diff);
    ASSERT_EQ(0, Longtail_CreateVersionDiff(hash_api, version_index_3, version_index_1, &version_diff));

    // We build a content index for our target version without having access to the remote index
    struct Longtail_ContentIndex* version_2_update_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version_index_2,
        42, // Can be arbitrary - we use blockstoreapi->RetargetContent to make it fall in line
        9, // Can be arbitrary - we use blockstoreapi->RetargetContent to make it fall in line
        &version_2_update_content_index));
    Longtail_Free(update_content_index_3);
    update_content_index_3 = SyncRetargetContent(cached_block_store_api, version_2_update_content_index);

/*    Longtail_Free(local_store_index);
    local_store_index = SyncGetContentIndex(local_block_store_api);
    Longtail_Free(cache_missing_content_index);
    ASSERT_EQ(0, Longtail_GetMissingContent(hash_api, local_store_index, update_content_index_3, &cache_missing_content_index));

    Longtail_Free(missing_update_content_index_3);*/
    missing_update_content_index_3 = SyncRetargetContent(cached_block_store_api, cache_missing_content_index);
    ASSERT_GT(*update_content_index_3->m_BlockCount, *cache_missing_content_index->m_BlockCount);
    ASSERT_GT(*update_content_index_3->m_ChunkCount, *cache_missing_content_index->m_ChunkCount);
    Longtail_Free(cache_missing_content_index);
    Longtail_Free(missing_update_content_index_3);

    ASSERT_EQ(0, Longtail_ChangeVersion(
        cached_block_store_api,
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        0,
        update_content_index_3,
        version_index_3,
        version_index_2,
        version_diff,
        root_path_3,
        0));
    Longtail_Free(version_index_3);
    Longtail_Free(update_content_index_3);

    Longtail_Free(version_2_update_content_index);
    Longtail_Free(version_1_update_content_index);
    Longtail_Free(version_diff);
    Longtail_Free(version_index_2);
    Longtail_Free(version_2_content_index);
    Longtail_Free(retarget_cache_content_index);
    Longtail_Free(version_1_content_index);
    Longtail_Free(version_index_1);
//    Longtail_Free(local_store_index);
//    Longtail_Free(remote_store_index);
//    Longtail_Free(cache_store_index);
    SAFE_DISPOSE_API(cached_block_store_api);
    SAFE_DISPOSE_API(remote_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(storage_api);
}
#endif

static char* GenerateRandomPath(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKILMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKILMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKILMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKILMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKILMNOPQRSTUVWXYZ.";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            size_t allowed_range = sizeof(charset) - 1;
            int key = rand() % (int) (allowed_range);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

static void CreateRandomContent(
    struct Longtail_StorageAPI* storage_api,
    const char* root_path,
    uint32_t* file_count_left,
    uint32_t min_content_length,
    uint32_t max_content_length,
    int depth)
{
    while (*file_count_left)
    {
        switch(rand() % 16)
        {
            case 0:
                if (depth > 0)
                {
                    return;
                }
                break;
            case 1:
            case 2:
                {
                    char content_path[32];
                    size_t path_length = 2 + (rand() % 29);
                    GenerateRandomPath(content_path, path_length);
                    const char* dir_name = storage_api->ConcatPath(storage_api, root_path, content_path);
                    if (storage_api->IsDir(storage_api, dir_name) || storage_api->IsFile(storage_api, dir_name))
                    {
                        Longtail_Free((void*)dir_name);
                        continue;
                    }
                    *file_count_left = *file_count_left - 1;
                    ASSERT_EQ(0, storage_api->CreateDir(storage_api, dir_name));
                    CreateRandomContent(storage_api, dir_name, file_count_left, min_content_length, max_content_length, depth + 1);
                    Longtail_Free((void*)dir_name);
                }
                break;
            default:
                {
                    char content_path[32];
                    size_t path_length = 2 + (rand() % 29);
                    GenerateRandomPath(content_path, path_length);
                    const char* file_name = storage_api->ConcatPath(storage_api, root_path, content_path);
                    if (storage_api->IsDir(storage_api, file_name) || storage_api->IsFile(storage_api, file_name))
                    {
                        Longtail_Free((void*)file_name);
                        continue;
                    }

                    size_t content_length = ((((uint64_t)rand()) << 32) + rand()) % (max_content_length - min_content_length) + min_content_length;
                    uint8_t* content_data = (uint8_t*)malloc(content_length);
                    GenerateRandomData(content_data, content_length);

                    Longtail_StorageAPI_HOpenFile w;
                    ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, file_name, 0, &w));
                    ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
                    ASSERT_EQ(0, storage_api->Write(storage_api, w, 0, content_length, content_data));
                    storage_api->CloseFile(storage_api, w);
                    w = 0;
                    Longtail_Free((void*)file_name);
                    free(content_data);
                }
                break;
        }
    }
}

static void CreateRandomContent(struct Longtail_StorageAPI* storage_api, const char* root_path, uint32_t file_count, uint32_t min_content_length, uint32_t max_content_length)
{
    uint32_t file_count_left = file_count;
    ASSERT_EQ(0, EnsureParentPathExists(storage_api, root_path));
    storage_api->CreateDir(storage_api, root_path);
    CreateRandomContent(storage_api, root_path, &file_count_left, min_content_length, max_content_length, 0);
}

TEST(Longtail, VersionLocalContent)
{
    struct Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(8, 0);
    struct Longtail_BlockStoreAPI* block_store = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "store", 384, 8, 0);
    struct Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();

    CreateRandomContent(
        storage_api,
        "version",
        56,
        7,
        5371);
    struct Longtail_FileInfos* version1_file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(
        storage_api,
        0,
        0,
        0,
        "version",
        &version1_file_infos));

    struct Longtail_VersionIndex *version1_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "version",
        version1_file_infos,
        0,
        128,
        &version1_index));

    struct Longtail_ContentIndex* version1_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version1_index,
        128,
        9,
        &version1_content_index));

    struct Longtail_ContentIndex* version1_block_store_content_index = SyncRetargetContent(
        block_store,
        version1_content_index);

    struct Longtail_ContentIndex* version1_missing_content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        version1_block_store_content_index,
        version1_index,
        *version1_block_store_content_index->m_MaxBlockSize,
        *version1_block_store_content_index->m_MaxChunksPerBlock,
        &version1_missing_content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        block_store,
        job_api,
        0,
        0,
        0,
        version1_missing_content_index,
        version1_index,
        "version"));

    struct Longtail_ContentIndex* block_store_content_index_1;
    ASSERT_EQ(0, Longtail_MergeContentIndex(
        job_api,
        version1_missing_content_index,
        version1_block_store_content_index,
        &block_store_content_index_1));

    ASSERT_EQ(0, Longtail_ValidateContent(
        block_store_content_index_1,
        version1_index));

    // Add additional content
    CreateRandomContent(storage_api, "version", 31, 91, 19377);
    struct Longtail_FileInfos* version2_file_infos;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(
        storage_api,
        0,
        0,
        0,
        "version",
        &version2_file_infos));

    struct Longtail_VersionIndex *version2_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "version",
        version2_file_infos,
        0,
        128,
        &version2_index));

    struct Longtail_ContentIndex* version2_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        version2_index,
        192,
        7,
        &version2_content_index));

    struct Longtail_ContentIndex* version2_block_store_content_index = SyncRetargetContent(
        block_store,
        version2_content_index);

    struct Longtail_ContentIndex* version2_missing_content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        version2_block_store_content_index,
        version2_index,
        *version2_block_store_content_index->m_MaxBlockSize,
        *version2_block_store_content_index->m_MaxChunksPerBlock,
        &version2_missing_content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        block_store,
        job_api,
        0,
        0,
        0,
        version2_missing_content_index,
        version2_index,
        "version"));

    struct Longtail_ContentIndex* block_store_content_index_2;
    ASSERT_EQ(0, Longtail_MergeContentIndex(
        job_api,
        version2_missing_content_index,
        version2_block_store_content_index,
        &block_store_content_index_2));

    ASSERT_EQ(0, Longtail_ValidateContent(
        block_store_content_index_2,
        version2_index));
    ASSERT_EQ(0, Longtail_ValidateContent(
        block_store_content_index_2,
        version1_index));
    ASSERT_NE(0, Longtail_ValidateContent(
        block_store_content_index_1,
        version2_index));

    Longtail_Free(block_store_content_index_2);

    Longtail_Free(version2_missing_content_index);

    Longtail_Free(version2_block_store_content_index);

    Longtail_Free(version2_content_index);

    Longtail_Free(version2_index);

    Longtail_Free(version2_file_infos);

    Longtail_Free(block_store_content_index_1);

    Longtail_Free(version1_missing_content_index);

    Longtail_Free(version1_block_store_content_index);

    Longtail_Free(version1_content_index);

    Longtail_Free(version1_index);

    Longtail_Free(version1_file_infos);

    SAFE_DISPOSE_API(block_store);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(chunker_api);
}

struct FailableStorageAPI
{
    struct Longtail_StorageAPI m_API;
    struct Longtail_StorageAPI* m_BackingAPI;
    int m_PassCount;
    int m_WriteError;

    static void Dispose(struct Longtail_API* api) { Longtail_Free(api); }
    static int OpenReadFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HOpenFile* out_open_file) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->OpenReadFile(api->m_BackingAPI, path, out_open_file);}
    static int GetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t* out_size) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->GetSize(api->m_BackingAPI, f, out_size);}
    static int Read(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->Read(api->m_BackingAPI, f, offset, length, output);}
    static int OpenWriteFile(struct Longtail_StorageAPI* storage_api, const char* path, uint64_t initial_size, Longtail_StorageAPI_HOpenFile* out_open_file) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->OpenWriteFile(api->m_BackingAPI, path, initial_size, out_open_file);}
    static int Write(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return ((api->m_PassCount-- <= 0) && offset > 0 && api->m_WriteError != 0) ? api->m_WriteError : api->m_BackingAPI->Write(api->m_BackingAPI, f, offset, length, input);}
    static int SetSize(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f, uint64_t length) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->SetSize(api->m_BackingAPI, f, length);}
    static int SetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t permissions) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->SetPermissions(api->m_BackingAPI, path, permissions);}
    static int GetPermissions(struct Longtail_StorageAPI* storage_api, const char* path, uint16_t* out_permissions) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->GetPermissions(api->m_BackingAPI, path, out_permissions);}
    static void CloseFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HOpenFile f) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->CloseFile(api->m_BackingAPI, f);}
    static int CreateDir(struct Longtail_StorageAPI* storage_api, const char* path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->CreateDir(api->m_BackingAPI, path);}
    static int RenameFile(struct Longtail_StorageAPI* storage_api, const char* source_path, const char* target_path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->RenameFile(api->m_BackingAPI, source_path, target_path);}
    static char* ConcatPath(struct Longtail_StorageAPI* storage_api, const char* root_path, const char* sub_path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->ConcatPath(api->m_BackingAPI, root_path, sub_path);}
    static int IsDir(struct Longtail_StorageAPI* storage_api, const char* path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->IsDir(api->m_BackingAPI, path);}
    static int IsFile(struct Longtail_StorageAPI* storage_api, const char* path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->IsFile(api->m_BackingAPI, path);}
    static int RemoveDir(struct Longtail_StorageAPI* storage_api, const char* path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->RemoveDir(api->m_BackingAPI, path);}
    static int RemoveFile(struct Longtail_StorageAPI* storage_api, const char* path) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->RemoveFile(api->m_BackingAPI, path);}
    static int StartFind(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HIterator* out_iterator) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->StartFind(api->m_BackingAPI, path, out_iterator);}
    static int FindNext(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->FindNext(api->m_BackingAPI, iterator);}
    static void CloseFind(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->CloseFind(api->m_BackingAPI, iterator);}
    static int GetEntryProperties(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HIterator iterator, struct Longtail_StorageAPI_EntryProperties* out_properties) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->GetEntryProperties(api->m_BackingAPI, iterator, out_properties);}
    static int LockFile(struct Longtail_StorageAPI* storage_api, const char* path, Longtail_StorageAPI_HLockFile* out_lock_file) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->LockFile(api->m_BackingAPI, path, out_lock_file);}
    static int UnlockFile(struct Longtail_StorageAPI* storage_api, Longtail_StorageAPI_HLockFile lock_file) { struct FailableStorageAPI* api = (struct FailableStorageAPI*)storage_api; return api->m_BackingAPI->UnlockFile(api->m_BackingAPI, lock_file);}
};

struct FailableStorageAPI* CreateFailableStorageAPI(struct Longtail_StorageAPI* backing_api)
{
    void* mem = Longtail_Alloc(sizeof(struct FailableStorageAPI));
    struct Longtail_StorageAPI* api = Longtail_MakeStorageAPI(
        mem,
        FailableStorageAPI::Dispose,
        FailableStorageAPI::OpenReadFile,
        FailableStorageAPI::GetSize,
        FailableStorageAPI::Read,
        FailableStorageAPI::OpenWriteFile,
        FailableStorageAPI::Write,
        FailableStorageAPI::SetSize,
        FailableStorageAPI::SetPermissions,
        FailableStorageAPI::GetPermissions,
        FailableStorageAPI::CloseFile,
        FailableStorageAPI::CreateDir,
        FailableStorageAPI::RenameFile,
        FailableStorageAPI::ConcatPath,
        FailableStorageAPI::IsDir,
        FailableStorageAPI::IsFile,
        FailableStorageAPI::RemoveDir,
        FailableStorageAPI::RemoveFile,
        FailableStorageAPI::StartFind,
        FailableStorageAPI::FindNext,
        FailableStorageAPI::CloseFind,
        FailableStorageAPI::GetEntryProperties,
        FailableStorageAPI::LockFile,
        FailableStorageAPI::UnlockFile);
    struct FailableStorageAPI* failable_storage_api = (struct FailableStorageAPI*)api;
    failable_storage_api->m_BackingAPI = backing_api;
    failable_storage_api->m_PassCount = 0x7fffffff;
    failable_storage_api->m_WriteError = 0;
    return failable_storage_api;
}

TEST(Longtail, TestChangeVersionDiskFull)
{
    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 1u;

    Longtail_StorageAPI* mem_storage = Longtail_CreateInMemStorageAPI();
    struct FailableStorageAPI* failable_local_storage_api = CreateFailableStorageAPI(mem_storage);
    struct FailableStorageAPI* failable_remote_storage_api = CreateFailableStorageAPI(mem_storage);
    Longtail_StorageAPI* local_storage = &failable_local_storage_api->m_API;
    Longtail_StorageAPI* remote_storage = &failable_remote_storage_api->m_API;
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, local_storage, "cache", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* remote_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, remote_storage, "chunks", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* remote_compressed_block_store_api = Longtail_CreateCompressBlockStoreAPI(remote_block_store_api, compression_registry);
    Longtail_BlockStoreAPI* cached_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store_api, remote_block_store_api);
    Longtail_BlockStoreAPI* cached_compress_store_api = Longtail_CreateCompressBlockStoreAPI(cached_store_api, compression_registry);

    const uint32_t ASSET_COUNT = 1u;

    const char* TEST_FILENAMES[ASSET_COUNT] = {
        "ContentChangedSameLength.txt"
    };

    const char* TEST_STRINGS[ASSET_COUNT] = {
        "OVOP5VDVCzTCqmpV1Dm7eci7QMEyI20BTGigIUka6raZYtFYbKsfn1c40AMGLxrqXFCXkpKYe9GQJjmlEiRmrj8hR7fuQO8fYJ3Z79BUmps3vy3FNb4fGnZJmDbmKzyCkb2rZjGE4kbC"
        "axXWo74MfsqUxPA5BxDCKkaE4gm531RUCwTkpnsvw9YjE3IT0m04vJVDQWRyBOHpj5nxkUSrEFlEMVNUgZYe8yZ6l9UQelgkifq8wtuREH3vFuZcNEsJI2whRQ8d7NxKAra5KK857nsZ"
        "dZFZu6UYyFLTPwqBJV2CxcFpgZVkkeo1qC7Wpxon9VhX2Ooxq8VL4XEE85GLVdCifhDChav260O6bj2yYpmcTF8lEZX1WXVaQTFCEwnJ35E2iwoc95DXztXHywMskG8tpkYie7AGyH4v"
        "WViKKW5apwjwkpvNjWpL0j0ukyuCZB0ATLY2xPyzBx9v0fqafa6q5HOKooXpvkinhGSu6eoYVQ9dFm0yLzOmjxlWpSPBbGZBkjCXZ6lAksVkmmLiazp7W6kWnyuAPkHuTexYeRCGduXn"
        "6aFOWGAnDmt05mJntcBzV3bNWU85fbP8kQu5HkH0MzD4SgYAJEB2QCwxF6udPKDIssZbOuvTkKPQQtU1RpCHqzhJYfuQ2iHuq9bcx7jVEekkf2nZpm8Vmczsg6CPBxkEhCdrYT546e86"
        "GYAUgqhfDCUguEcwuMAO7iWmFKAVbAPEDRJ17sSAxLEcogpnelEcBc4gYs57Kx3D0ZxbikiglQJo86ZTgJhq6m05NDT5qD2gE1V0CYw2WQlIyRYVbMvHg10NxM31JacXVJiA7lGWK8N9"
        "TLJVC3YZ0bhOBDG8bzfHvBYI6jVNlJKQ8RH7tCXhtvj7HffZuncn4si8DL7Oyc8N0UfNLqqn3ujJYzXOb3kJvBiPSSWlTwuDz9QkNiuqFzTFRXpxMbwwt9jjVK4SnDssf1Qpc4Fc4fCq"
        "xGgMt3YuA3LfV2eTfcYxZwOPGLtEGs93q2splEeeq21mYLam3q8tEOhFVJHUaGPnOdk1RCkFF8yIra2aqrgMdcZJHx2zue15I2Mt0r8EFhSSf4QKws2hwm9J8rwJ0b6kZCD81rFm4E4L"
        "6U0h2YL3KD9paSHzIK8LMXNaZPWXIvIlgchXMYRlCx97cctaaNGmynkt6fW9W7cxxiVxpaYwuExyKYAdC0GXqmIZZEAkVy7dBDQxaaqKaIvDLdDFPXsoMSwgRWf3Jhn5ELZycjmwrSuC"
        "ekHhhQWV7SFH9WddLbGqdNDzZiT7MQ7LmY6Q64xRoC88xfiOnUZ71GotKpfUVAhUWRM9eAX4WXiPqPULeiMkzTWP1NXjCnnksnwV9HXQSG8JHWdeGeIy9W8QxENZKalaWd4sVl3yvGBz"
        "u1fbm4vsNGCJ11pIZXkiBIeVksPzqaapV2rEpPC5d4ME3cmpAkjx4oE9JAVM93MFTQxqJ1pNNt4SaYG13hKjhqc8sNHDR2304oPkGDgc1iwrTqlEPHVQDRBpnNAFe1sJJ5IM6rraCd8L"
        "sBU7DIwmvghGWCN6xj8uRFI5ihkGfsbmPkPNBhaMUuswlZyxoFR7UO68oOz9iftuY86cXv98lkfZulBWWfuLj8Ixz2ZEA7QsbZAYNcw2sKxQTY5G0m2kQyNv8DPJRl5h5m1mxXjfgeu6"
        "v5TqERzcvdwmDAlnt5AeSTYkDQ4cnpjnYEJdB2tf6hYVLokWIZSWSGAEmDMQ8vZwZVaUysxDPLc59z9ZnO7UYSJybQShASKUYjpse4L4CA8cs16votWhMfpEz8QFFEqjdOLJr5u2cVYX"
        "dKbpdIn0A68f2o4HHQQI5U7IrY2MgS6j9VQtQoKySgKcSBeZ4b6GwObRN1YR4kq05zq4bS4L99LaVNCkmn0RXxyWX9MNd81ivrlL5phB5ljlPa68ILTJ8v9iOodBNeMIXC8peINfwS5R"
        "yuMCpEwPCDJBdRjaaw07gdKPUaG0IybnqH2n25CF"
    };

    const size_t TEST_SIZES[ASSET_COUNT] = {
        strlen(TEST_STRINGS[0]) + 1
    };

    const uint16_t TEST_PERMISSIONS[ASSET_COUNT] = {
        0644
    };

    for (uint32_t i = 0; i < ASSET_COUNT; ++i)
    {
        char* file_name = local_storage->ConcatPath(local_storage, "source", TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(local_storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, local_storage->OpenWriteFile(local_storage, file_name, 0, &w));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (TEST_SIZES[i])
        {
            ASSERT_EQ(0, local_storage->Write(local_storage, w, 0, TEST_SIZES[i], TEST_STRINGS[i]));
        }
        local_storage->CloseFile(local_storage, w);
        w = 0;
        local_storage->SetPermissions(local_storage, file_name, TEST_PERMISSIONS[i]);
        Longtail_Free(file_name);
    }

    Longtail_FileInfos* version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, 0, 0, 0, "source", &version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version_paths);
    uint32_t* compression_types = GetAssetTags(local_storage, version_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "source",
        version_paths,
        compression_types,
        48,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(compression_types);
    compression_types = 0;
    Longtail_Free(version_paths);
    version_paths = 0;

    Longtail_ContentIndex* version_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            vindex,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &version_content_index));

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(remote_compressed_block_store_api, version_content_index);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        local_storage,
        remote_compressed_block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "source"));
    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;
    Longtail_Free(version_content_index);
    version_content_index = 0;

    Longtail_FileInfos* current_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, 0, 0, 0, "current", &current_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, current_version_paths);
    uint32_t* current_compression_types = GetAssetTags(local_storage, current_version_paths);
    ASSERT_NE((uint32_t*)0, current_compression_types);
    Longtail_VersionIndex* current_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "current",
        current_version_paths,
        current_compression_types,
        16,
        &current_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, current_vindex);
    Longtail_Free(current_compression_types);
    current_compression_types = 0;
    Longtail_Free(current_version_paths);
    current_version_paths = 0;

    Longtail_VersionDiff* version_diff;
    ASSERT_EQ(0, Longtail_CreateVersionDiff(
        hash_api,
        current_vindex,
        vindex,
        &version_diff));
    ASSERT_NE((Longtail_VersionDiff*)0, version_diff);

    ASSERT_EQ(0, Longtail_CreateContentIndexFromDiff(
        hash_api,
        vindex,
        version_diff,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    block_store_content_index = SyncRetargetContent(remote_compressed_block_store_api, content_index);
    Longtail_Free(content_index);

    Longtail_SetLogLevel(LONGTAIL_LOG_LEVEL_OFF);
    failable_local_storage_api->m_PassCount = 3;
    failable_local_storage_api->m_WriteError = ENOSPC;
    ASSERT_EQ(ENOSPC, Longtail_ChangeVersion(
        cached_compress_store_api,
        local_storage,
        hash_api,
        job_api,
        0,
        0,
        0,
        block_store_content_index,
        current_vindex,
        vindex,
        version_diff,
        "old",
        1));
    Longtail_SetLogLevel(LONGTAIL_LOG_LEVEL_ERROR);

    Longtail_Free(block_store_content_index);
    Longtail_Free(version_diff);
    Longtail_Free(current_vindex);
    Longtail_Free(vindex);

    SAFE_DISPOSE_API(cached_compress_store_api);
    SAFE_DISPOSE_API(cached_store_api);
    SAFE_DISPOSE_API(remote_compressed_block_store_api);
    SAFE_DISPOSE_API(remote_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(&failable_remote_storage_api->m_API);
    SAFE_DISPOSE_API(&failable_local_storage_api->m_API);
    SAFE_DISPOSE_API(mem_storage);
}



TEST(Longtail, TestLongtailBlockFS)
{
    static const uint32_t MAX_BLOCK_SIZE = 4096;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 16u;

    Longtail_StorageAPI* mem_storage = Longtail_CreateInMemStorageAPI();//Longtail_CreateFSStorageAPI();//
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(8, 0);
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_BlockStoreAPI* raw_block_store = Longtail_CreateFSBlockStoreAPI(job_api, mem_storage, "store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* block_store = Longtail_CreateCompressBlockStoreAPI(raw_block_store, compression_registry);

//    printf("\nCreating...\n");

    CreateRandomContent(mem_storage, "source", MAX_CHUNKS_PER_BLOCK * 3, 0, MAX_BLOCK_SIZE * 7);

    Longtail_FileInfos* version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(mem_storage, 0, 0, 0, "source", &version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version_paths);

    uint32_t* compression_types = SetAssetTags(mem_storage, version_paths, 0);//Longtail_GetZStdMinQuality());
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        mem_storage,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        "source",
        version_paths,
        compression_types,
        MAX_BLOCK_SIZE / MAX_CHUNKS_PER_BLOCK,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);

    Longtail_ContentIndex* version_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            vindex,
            MAX_BLOCK_SIZE / 4,
            MAX_CHUNKS_PER_BLOCK / 2,
            &version_content_index));

    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store, version_content_index);

    struct Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        mem_storage,
        block_store,
        job_api,
        0,
        0,
        0,
        content_index,
        vindex,
        "source"));

    Longtail_Free(content_index);
    content_index = 0;
    Longtail_Free(block_store_content_index);

    block_store_content_index = SyncRetargetContent(block_store, version_content_index);

//    printf("\nReading...\n");

    struct Longtail_StorageAPI* block_store_fs = Longtail_CreateBlockStoreStorageAPI(
        hash_api,
        job_api,
        block_store,
        block_store_content_index,
        vindex);
    ASSERT_NE((struct Longtail_StorageAPI*)0, block_store_fs);

    Longtail_FileInfos* block_store_storage_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(block_store_fs, 0, 0, 0, "", &block_store_storage_paths));
    ASSERT_NE((Longtail_FileInfos*)0, block_store_storage_paths);
    ASSERT_EQ(version_paths->m_Count, block_store_storage_paths->m_Count);

    struct Longtail_LookupTable* version_paths_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(version_paths->m_Count)), version_paths->m_Count, 0);
    for (uint32_t f = 0; f < version_paths->m_Count; ++f)
    {
        uint64_t hash;
        const char* path = &version_paths->m_PathData[version_paths->m_PathStartOffsets[f]];
        Longtail_GetPathHash(hash_api, path, &hash);
        Longtail_LookupTable_Put(version_paths_lookup, hash, f);
    }
    struct Longtail_LookupTable* block_store_storage_paths_lookup = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(block_store_storage_paths->m_Count)), block_store_storage_paths->m_Count, 0);
    for (uint32_t f = 0; f <  block_store_storage_paths->m_Count; ++f)
    {
        uint64_t hash;
        const char* path = &block_store_storage_paths->m_PathData[block_store_storage_paths->m_PathStartOffsets[f]];
        Longtail_GetPathHash(hash_api, path, &hash);
        Longtail_LookupTable_Put(block_store_storage_paths_lookup, hash, f);
        ASSERT_NE((uint64_t*)0, Longtail_LookupTable_Get(version_paths_lookup, hash));
    }

    for (uint32_t f = 0; f < version_paths->m_Count; ++f)
    {
        uint64_t hash;
        const char* path = &version_paths->m_PathData[version_paths->m_PathStartOffsets[f]];
        Longtail_GetPathHash(hash_api, path, &hash);
        uint64_t i = *Longtail_LookupTable_Get(block_store_storage_paths_lookup, hash);
        ASSERT_EQ(version_paths->m_Sizes[f], block_store_storage_paths->m_Sizes[i]);
        ASSERT_EQ(version_paths->m_Permissions[f], block_store_storage_paths->m_Permissions[i]);
        ASSERT_STREQ(path, &block_store_storage_paths->m_PathData[block_store_storage_paths->m_PathStartOffsets[i]]);
    }

    Longtail_Free(block_store_storage_paths_lookup);
    Longtail_Free(version_paths_lookup);

    ASSERT_EQ(block_store_storage_paths->m_Count, *vindex->m_AssetCount);
    Longtail_Free(block_store_storage_paths);

    for (uint32_t f = 0; f < version_paths->m_Count; ++f)
    {
        const char* path = &version_paths->m_PathData[version_paths->m_PathStartOffsets[f]];
        char* full_path = mem_storage->ConcatPath(mem_storage, "source", path);
        if (mem_storage->IsFile(mem_storage, full_path))
        {
            Longtail_StorageAPI_HOpenFile block_store_file;
            ASSERT_EQ(0, block_store_fs->OpenReadFile(block_store_fs, path, &block_store_file));
            uint64_t size;
            ASSERT_EQ(0, block_store_fs->GetSize(block_store_fs, block_store_file, &size));
            char* buf = (char*)Longtail_Alloc(size);
            if (size >= (256 + 32))
            {
                ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, 0, 32, buf));
                ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, 32, 128, &buf[32]));
                ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, 160, 96, &buf[160]));
                ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, size - 32, 32, &buf[size - 32]));
                ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, 256, size - (256 + 32), &buf[256]));
            }
            else if (size > 0)
            {
                uint64_t o = 0;
                while (o < size)
                {
                    uint64_t s = o % 3 + 1;
                    if (o + s > size)
                    {
                        s = size - o;
                    }
                    ASSERT_EQ(0, block_store_fs->Read(block_store_fs, block_store_file, o, s, &buf[o]));
                    o += s;
                }
            }

            block_store_fs->CloseFile(block_store_fs, block_store_file);

            Longtail_StorageAPI_HOpenFile open_file;
            ASSERT_EQ(0, mem_storage->OpenReadFile(mem_storage, full_path, &open_file));
            uint64_t validate_size;
            ASSERT_EQ(0, mem_storage->GetSize(mem_storage, open_file, &validate_size));
            char* validate_buf = (char*)Longtail_Alloc(validate_size);
            ASSERT_EQ(0, mem_storage->Read(mem_storage, open_file, 0, validate_size, validate_buf));
            mem_storage->CloseFile(mem_storage, open_file);

            ASSERT_EQ(size, validate_size);
            const uint8_t* p1 = (const uint8_t*)buf;
            const uint8_t* p2 = (const uint8_t*)validate_buf;
            for (uint64_t i = 0; i < size; ++i)
            {
                if (p1[i] != p2[i])
                {
                    ASSERT_TRUE(false);
                }
            }

//            data_read_count += size;

            Longtail_Free(buf);

            Longtail_Free(validate_buf);
        }
        Longtail_Free(full_path);
    }

    SAFE_DISPOSE_API(block_store_fs);

//    printf("\nDone...\n");

    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    Longtail_Free(version_content_index);
    version_content_index = 0;

    Longtail_Free(vindex);
    vindex = 0;

    Longtail_Free(compression_types);
    compression_types = 0;

    Longtail_Free(version_paths);
    version_paths = 0;
    SAFE_DISPOSE_API(block_store);
    SAFE_DISPOSE_API(raw_block_store);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(mem_storage);
}

struct FSBlockStoreSyncWriteContentWorkerContext {
    Longtail_StorageAPI* mem_storage;
    Longtail_HashAPI* hash_api;
    Longtail_JobAPI* job_api;
    const char* storage_path;
    Longtail_VersionIndex* vindex;
    Longtail_ContentIndex* version_content_index;
    uint32_t MAX_BLOCK_SIZE;
    uint32_t MAX_CHUNKS_PER_BLOCK;
};

static int FSBlockStoreSyncWriteContentWorker(
    Longtail_StorageAPI* mem_storage,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    const char* storage_path,
    Longtail_VersionIndex* vindex,
    Longtail_ContentIndex* version_content_index,
    uint32_t MAX_BLOCK_SIZE,
    uint32_t MAX_CHUNKS_PER_BLOCK)
{
    Longtail_BlockStoreAPI* block_store = Longtail_CreateFSBlockStoreAPI(job_api, mem_storage, "store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store, version_content_index);
    if (!block_store_content_index)
    {
        return EINVAL;
    }

    struct Longtail_ContentIndex* missing_content_index;
    int err = Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        vindex,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &missing_content_index);
    if (err)
    {
        return err;
    }

    err = Longtail_WriteContent(
        mem_storage,
        block_store,
        job_api,
        0,
        0,
        0,
        missing_content_index,
        vindex,
        storage_path);
    Longtail_Free(missing_content_index);
    Longtail_Free(block_store_content_index);
    SAFE_DISPOSE_API(block_store);
    return err;
}

int TestLongtailFSBlockStoreSyncWorkerCB(void* context_data)
{
    struct FSBlockStoreSyncWriteContentWorkerContext* context = (struct FSBlockStoreSyncWriteContentWorkerContext*)context_data;
    return FSBlockStoreSyncWriteContentWorker(
        context->mem_storage,
        context->hash_api,
        context->job_api,
        context->storage_path,
        context->vindex,
        context->version_content_index,
        context->MAX_BLOCK_SIZE,
        context->MAX_CHUNKS_PER_BLOCK);
}


TEST(Longtail, TestLongtailFSBlockStoreSync)
{
    static const uint32_t MAX_BLOCK_SIZE = 26973;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 11u;

    Longtail_StorageAPI* mem_storage = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(8, 0);

    for (uint32_t i = 0; i < 4; ++i)
    {
        CreateRandomContent(mem_storage, "source", MAX_CHUNKS_PER_BLOCK * 3, 0, (MAX_BLOCK_SIZE * 3) >> 1);

        Longtail_FileInfos* version_paths;
        ASSERT_EQ(0, Longtail_GetFilesRecursively(mem_storage, 0, 0, 0, "source", &version_paths));
        ASSERT_NE((Longtail_FileInfos*)0, version_paths);

        uint32_t* compression_types = SetAssetTags(mem_storage, version_paths, 0);
        ASSERT_NE((uint32_t*)0, compression_types);
        Longtail_VersionIndex* vindex;
        ASSERT_EQ(0, Longtail_CreateVersionIndex(
            mem_storage,
            hash_api,
            chunker_api,
            job_api,
            0,
            0,
            0,
            "source",
            version_paths,
            compression_types,
            (MAX_BLOCK_SIZE / MAX_CHUNKS_PER_BLOCK) * 2,
            &vindex));
        ASSERT_NE((Longtail_VersionIndex*)0, vindex);
        Longtail_Free(compression_types);
        Longtail_Free(version_paths);

        Longtail_ContentIndex* version_content_index;
        ASSERT_EQ(0, Longtail_CreateContentIndex(
                hash_api,
                vindex,
                MAX_BLOCK_SIZE,
                MAX_CHUNKS_PER_BLOCK,
                &version_content_index));

        static const uint32_t WORKER_COUNT = 8;
        struct FSBlockStoreSyncWriteContentWorkerContext ctx = { mem_storage, hash_api, job_api, "source", vindex, version_content_index, MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK};
        HLongtail_Thread workerThreads[WORKER_COUNT];
        for (uint32_t t = 0; t < WORKER_COUNT; ++t)
        {
            int err = Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), TestLongtailFSBlockStoreSyncWorkerCB, 0, &ctx, -1, &workerThreads[t]);
            if (err)
            {
                while (t--)
                {
                    Longtail_JoinThread(workerThreads[t], LONGTAIL_TIMEOUT_INFINITE);
                    Longtail_DeleteThread(workerThreads[t]);
                    Longtail_Free(workerThreads[t]);
                }
                ASSERT_EQ(0, err);
            }
        }

        for (uint32_t t = 0; t < WORKER_COUNT; ++t)
        {
            Longtail_JoinThread(workerThreads[t], LONGTAIL_TIMEOUT_INFINITE);
            Longtail_DeleteThread(workerThreads[t]);
            Longtail_Free(workerThreads[t]);
        }

        Longtail_Free(version_content_index);
        Longtail_Free(vindex);
    }

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(mem_storage);
}

static int IsDirPath(const char* path)
{
    LONGTAIL_VALIDATE_INPUT(path != 0, return 0)
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

static int CopyDir(
    Longtail_StorageAPI* storage_api,
    const char* source_path,
    const char* target_path)
{
    int err = 0;
    struct Longtail_FileInfos* version_file_infos;
    Longtail_GetFilesRecursively(storage_api, 0, 0, 0, source_path, &version_file_infos);
    for (uint32_t f = 0; !err && f < version_file_infos->m_Count; ++f)
    {
        const char* asset_path = &version_file_infos->m_PathData[version_file_infos->m_PathStartOffsets[f]];
        char* full_source_path = storage_api->ConcatPath(storage_api, source_path, asset_path);
        char* full_target_path = storage_api->ConcatPath(storage_api, target_path, asset_path);
        EnsureParentPathExists(storage_api, full_target_path);
        if (IsDirPath(asset_path))
        {
            full_source_path[strlen(full_source_path) - 1] = '\0';
            full_target_path[strlen(full_target_path) - 1] = '\0';
            err = storage_api->CreateDir(storage_api, full_target_path);
            if (err == EEXIST)
            {
                err = 0;
            }
            else if (err)
            {
                return err;
            }
        }
        else
        {
            uint64_t s;
            void* buffer;
            {
                Longtail_StorageAPI_HOpenFile r;
                err = storage_api->OpenReadFile(storage_api, full_source_path, &r);
                if (err)
                {
                    return err;
                }
                err = storage_api->GetSize(storage_api, r, &s);
                if (err)
                {
                    return err;
                }
                buffer = Longtail_Alloc(s);
                err = storage_api->Read(storage_api, r, 0, s, buffer);
                if (err)
                {
                    return err;
                }
                storage_api->CloseFile(storage_api, r);
            }

            {
                Longtail_StorageAPI_HOpenFile w;
                err = storage_api->OpenWriteFile(storage_api, full_target_path, s, &w);
                if (err)
                {
                    return err;
                }
                err = storage_api->Write(storage_api, w, 0, s, buffer);
                if (err)
                {
                    return err;
                }
                storage_api->CloseFile(storage_api, w);
            }
            Longtail_Free(buffer);
        }
        uint16_t permissions;
        err = storage_api->GetPermissions(storage_api, full_source_path, &permissions);
        if (err)
        {
            return err;
        }
        err = storage_api->SetPermissions(storage_api, full_target_path, permissions);
        if (err)
        {
            return err;
        }
        Longtail_Free((void*)full_target_path);
        Longtail_Free((void*)full_source_path);
    }
    Longtail_Free(version_file_infos);
    return err;
}

static int UploadFolder(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_ChunkerAPI* chunker_api,
    Longtail_JobAPI* job_api,
    Longtail_BlockStoreAPI* block_store_api,
    const char* source_path,
    const char* version_index_path,
    uint32_t TARGET_CHUNK_SIZE,
    uint32_t MAX_BLOCK_SIZE,
    uint32_t MAX_CHUNKS_PER_BLOCK)
{
    struct Longtail_FileInfos* version_file_infos;
    int err = Longtail_GetFilesRecursively(storage_api, 0, 0, 0, source_path, &version_file_infos);
    if (err)
    {
        return err;
    }
    struct Longtail_VersionIndex* version_index;
    err = Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        source_path,
        version_file_infos,
        0,
        TARGET_CHUNK_SIZE,
        &version_index);
    if (err)
    {
        return err;
    }
    struct Longtail_ContentIndex* version_content_index;
    err = Longtail_CreateContentIndex(hash_api, version_index, MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, &version_content_index);
    if (err)
    {
        return err;
    }
    struct Longtail_ContentIndex* block_store_content_index = SyncRetargetContent(block_store_api, version_content_index);

    struct Longtail_ContentIndex* content_index;
    err = Longtail_CreateMissingContent(
        hash_api,
        block_store_content_index,
        version_index,
        *block_store_content_index->m_MaxBlockSize,
        *block_store_content_index->m_MaxChunksPerBlock,
        &content_index);
    if (err)
    {
        return err;
    }

    err = Longtail_WriteContent(
        storage_api,
        block_store_api,
        job_api,
        0,
        0,
        0,
        content_index,
        version_index,
        source_path);
    if (err)
    {
        return err;
    }
    err = Longtail_WriteVersionIndex(storage_api, version_index, version_index_path);
    if (err)
    {
        return err;
    }
    Longtail_Free(content_index);
    Longtail_Free(block_store_content_index);
    Longtail_Free(version_content_index);
    Longtail_Free(version_index);
    Longtail_Free(version_file_infos);
    return 0;
}

static int DownloadFolder(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_ChunkerAPI* chunker_api,
    Longtail_JobAPI* job_api,
    Longtail_BlockStoreAPI* block_store_api,
    const char* version_index_path,
    const char* target_path,
    uint32_t TARGET_CHUNK_SIZE,
    uint32_t MAX_BLOCK_SIZE,
    uint32_t MAX_CHUNKS_PER_BLOCK)
{
    struct Longtail_VersionIndex* version_index;
    int err = Longtail_ReadVersionIndex(storage_api, version_index_path, &version_index);
    if (err)
    {
        return err;
    }

    struct Longtail_FileInfos* version_file_infos;
    err = Longtail_GetFilesRecursively(storage_api, 0, 0, 0, target_path, &version_file_infos);
    if (err)
    {
        return err;
    }

    struct Longtail_VersionIndex* current_version_index;
    err = Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        target_path,
        version_file_infos,
        0,
        TARGET_CHUNK_SIZE,
        &current_version_index);
    if (err)
    {
        return err;
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(hash_api, current_version_index, version_index, &version_diff);
    if (err)
    {
        return err;
    }

    struct Longtail_ContentIndex* version_content_index;
    err = Longtail_CreateContentIndexFromDiff(hash_api, version_index, version_diff, MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, &version_content_index);
    if (err)
    {
        return err;
    }

    struct Longtail_ContentIndex* content_index = SyncRetargetContent(block_store_api, version_content_index);
    if (!content_index)
    {
        return EINVAL;
    }

    err = Longtail_ChangeVersion(block_store_api, storage_api, hash_api, job_api, 0, 0, 0, content_index, current_version_index, version_index, version_diff, target_path, 1);
    if (err)
    {
        return err;
    }

    Longtail_Free(content_index);
    Longtail_Free(version_content_index);
    Longtail_Free(version_diff);
    Longtail_Free(current_version_index);
    Longtail_Free(version_file_infos);
    Longtail_Free(version_index);
    return 0;
}

static int ValidateVersion(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_ChunkerAPI* chunker_api,
    Longtail_JobAPI* job_api,
    const char* expected_content_path,
    const char* content_path,
    uint32_t TARGET_CHUNK_SIZE)
{
    struct Longtail_FileInfos* expected_file_infos;
    int err = Longtail_GetFilesRecursively(storage_api, 0, 0, 0, expected_content_path, &expected_file_infos);
    if (err)
    {
        return err;
    }

    struct Longtail_FileInfos* file_infos;
    err = Longtail_GetFilesRecursively(storage_api, 0, 0, 0, content_path, &file_infos);
    if (err)
    {
        return err;
    }

    struct Longtail_VersionIndex* expected_version_index;
    err = Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        expected_content_path,
        expected_file_infos,
        0,
        TARGET_CHUNK_SIZE,
        &expected_version_index);
    if (err)
    {
        return err;
    }

    struct Longtail_VersionIndex* version_index;
    err = Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        chunker_api,
        job_api,
        0,
        0,
        0,
        content_path,
        file_infos,
        0,
        TARGET_CHUNK_SIZE,
        &version_index);
    if (err)
    {
        return err;
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(hash_api, version_index, expected_version_index, &version_diff);
    if (err)
    {
        return err;
    }

    if (*version_diff->m_SourceRemovedCount != 0)
    {
        return EINVAL;
    }
    if (*version_diff->m_TargetAddedCount != 0)
    {
        return EINVAL;
    }
    if (*version_diff->m_ModifiedContentCount != 0)
    {
        return EINVAL;
    }
    if (*version_diff->m_ModifiedPermissionsCount != 0)
    {
        return EINVAL;
    }

    Longtail_Free(version_diff);
    Longtail_Free(version_index);
    Longtail_Free(expected_version_index);
    Longtail_Free(file_infos);
    Longtail_Free(expected_file_infos);
    return 0;
}

struct CaptureBlockStore
{
    struct Longtail_BlockStoreAPI m_API;
    struct Longtail_BlockStoreAPI* m_BackingStore;
    struct Longtail_ContentIndex* m_PreflightContentIndex;
    TLongtail_Hash* m_GetStoredBlockHashes;
};

void CaptureBlockStore_Dispose(struct Longtail_API* block_store_api)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    arrfree(api->m_GetStoredBlockHashes);
    Longtail_Free(api->m_PreflightContentIndex);
    api->m_PreflightContentIndex = 0;
    api->m_BackingStore = 0;
}

static int CaptureBlockStore_PutStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    return api->m_BackingStore->PutStoredBlock(api->m_BackingStore, stored_block, async_complete_api);
}

static int CaptureBlockStore_PreflightGet(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    if (api->m_PreflightContentIndex)
    {
        uint64_t preflight_block_count = *api->m_PreflightContentIndex->m_BlockCount;
        uint64_t get_count = (uint64_t)arrlen(api->m_GetStoredBlockHashes);
        struct Longtail_LookupTable* lut = Longtail_LookupTable_Create(Longtail_Alloc(Longtail_LookupTable_GetSize(get_count)), get_count, 0);
        for (uint64_t b = 0; b < get_count; ++b)
        {
            Longtail_LookupTable_PutUnique(lut, api->m_GetStoredBlockHashes[b], b);
        }
        for (uint64_t b = 0; b < preflight_block_count; ++b)
        {
            TLongtail_Hash block_hash = api->m_PreflightContentIndex->m_BlockHashes[b];
            if (0 == Longtail_LookupTable_Get(lut, block_hash))
            {
                return EINVAL;
            }
        }
        Longtail_Free(lut);
        arrfree(api->m_GetStoredBlockHashes);
        Longtail_Free(api->m_PreflightContentIndex);
        api->m_PreflightContentIndex = 0;
    }
    void* buffer;
    size_t size;
    int err = Longtail_WriteContentIndexToBuffer(content_index, &buffer, &size);
    if (err)
    {
        return err;
    }
    err = Longtail_ReadContentIndexFromBuffer(buffer, size, &api->m_PreflightContentIndex);
    Longtail_Free(buffer);
    if (err)
    {
        return err;
    }
    return api->m_BackingStore->PreflightGet(api->m_BackingStore, content_index);
}

static int CaptureBlockStore_GetStoredBlock(struct Longtail_BlockStoreAPI* block_store_api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    if (!api->m_PreflightContentIndex)
    {
        return EINVAL;
    }
    arrput(api->m_GetStoredBlockHashes, block_hash);
    struct Longtail_ContentIndex* preflight_content_index = api->m_PreflightContentIndex;
    uint64_t block_hash_count = *preflight_content_index->m_BlockCount;
    for (uint64_t b = 0; b < block_hash_count; ++b)
    {
        if (preflight_content_index->m_BlockHashes[b] == block_hash)
        {
            return api->m_BackingStore->GetStoredBlock(api->m_BackingStore, block_hash, async_complete_api);
        }
    }
    return EINVAL;
}

static int CaptureBlockStore_RetargetContent(struct Longtail_BlockStoreAPI* block_store_api, const struct Longtail_ContentIndex* content_index, struct Longtail_AsyncRetargetContentAPI* async_complete_api)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    return api->m_BackingStore->RetargetContent(api->m_BackingStore, content_index, async_complete_api);
}

static int CaptureBlockStore_GetStats(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_BlockStore_Stats* out_stats)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    return api->m_BackingStore->GetStats(api->m_BackingStore, out_stats);
}

static int CaptureBlockStore_Flush(struct Longtail_BlockStoreAPI* block_store_api, struct Longtail_AsyncFlushAPI* async_complete_api)
{
    struct CaptureBlockStore* api = (struct CaptureBlockStore*)block_store_api;
    return api->m_BackingStore->Flush(api->m_BackingStore, async_complete_api);
}

struct Longtail_BlockStoreAPI* CaptureBlockStoreInit(struct CaptureBlockStore* store, struct Longtail_BlockStoreAPI* backing_store)
{
    struct Longtail_BlockStoreAPI* api = Longtail_MakeBlockStoreAPI(&store->m_API,
        CaptureBlockStore_Dispose,
        CaptureBlockStore_PutStoredBlock,
        CaptureBlockStore_PreflightGet,
        CaptureBlockStore_GetStoredBlock,
        CaptureBlockStore_RetargetContent,
        CaptureBlockStore_GetStats,
        CaptureBlockStore_Flush);
    store->m_BackingStore = backing_store;
    store->m_PreflightContentIndex = 0;
    store->m_GetStoredBlockHashes = 0;
    return api;
}


TEST(Longtail, TestCacheBlockStoreRetarget)
{
    static const uint32_t TARGET_CHUNK_SIZE = 8192;
    static const uint32_t MAX_BLOCK_SIZE = 26973;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 11u;

    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake3HashAPI();
    Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0, 0);
    Longtail_BlockStoreAPI* local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "cache-store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    Longtail_BlockStoreAPI* remote_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "remote-store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
    CaptureBlockStore remote_capture_store_api_instance;
    Longtail_BlockStoreAPI* remote_capture_store_api = CaptureBlockStoreInit(&remote_capture_store_api_instance, remote_block_store_api);
    Longtail_BlockStoreAPI* cache_block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store_api, remote_capture_store_api);

    CreateRandomContent(storage_api, "version1", 3/*MAX_CHUNKS_PER_BLOCK * 3*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    ASSERT_EQ(0, CopyDir(storage_api, "version1", "version2"));
    CreateRandomContent(storage_api, "version2", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    ASSERT_EQ(0, CopyDir(storage_api, "version2", "version3"));
    CreateRandomContent(storage_api, "version3", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    ASSERT_EQ(0, CopyDir(storage_api, "version3", "version4"));
    CreateRandomContent(storage_api, "version4", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    CreateRandomContent(storage_api, "version5", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    CreateRandomContent(storage_api, "version6", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    ASSERT_EQ(0, CopyDir(storage_api, "version6", "version7"));
    CreateRandomContent(storage_api, "version7", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);
    ASSERT_EQ(0, CopyDir(storage_api, "version4", "version8"));
    ASSERT_EQ(0, CopyDir(storage_api, "version5", "version8"));
    CreateRandomContent(storage_api, "version8", 1/*MAX_CHUNKS_PER_BLOCK*/, 0, MAX_BLOCK_SIZE / 2 /*(MAX_BLOCK_SIZE * 3) >> 1*/);

    const char* version_names[8] = {"version1", "version2", "version3", "version4", "version5", "version6", "version7", "version8"};

    for (uint32_t v = 0; v < 8; ++v)
    {
        char lvi_name[64];
        sprintf(lvi_name, "%s.lvi", version_names[v]);
        ASSERT_EQ(0, UploadFolder(storage_api, hash_api, chunker_api, job_api, remote_block_store_api, version_names[v], lvi_name, TARGET_CHUNK_SIZE, MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK));
    }

    for (uint32_t v = 0; v < 8; ++v)
    {
        SAFE_DISPOSE_API(cache_block_store_api);
        SAFE_DISPOSE_API(remote_capture_store_api);
        SAFE_DISPOSE_API(remote_block_store_api);
        SAFE_DISPOSE_API(local_block_store_api);

        local_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "cache-store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
        remote_block_store_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, "remote-store", MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK, 0);
        remote_capture_store_api = CaptureBlockStoreInit(&remote_capture_store_api_instance, remote_block_store_api);
        cache_block_store_api = Longtail_CreateCacheBlockStoreAPI(job_api, local_block_store_api, remote_capture_store_api);

        for (uint32_t w = v; w < 8; ++w)
        {
            char lvi_name[64];
            sprintf(lvi_name, "%s.lvi", version_names[w]);
            ASSERT_EQ(0, DownloadFolder(storage_api, hash_api, chunker_api, job_api, cache_block_store_api, lvi_name, "current", TARGET_CHUNK_SIZE, MAX_BLOCK_SIZE, MAX_CHUNKS_PER_BLOCK));

            {
                TestAsyncFlushComplete flushCB;
                ASSERT_EQ(0, cache_block_store_api->Flush(cache_block_store_api, &flushCB.m_API));
                flushCB.Wait();
                ASSERT_EQ(0, flushCB.m_Err);
            }
            {
                TestAsyncFlushComplete flushCB;
                ASSERT_EQ(0, local_block_store_api->Flush(local_block_store_api, &flushCB.m_API));
                flushCB.Wait();
                ASSERT_EQ(0, flushCB.m_Err);
            }


            ASSERT_EQ(0, ValidateVersion(storage_api, hash_api, chunker_api, job_api,  version_names[w], "current", TARGET_CHUNK_SIZE));
        }
    }

    SAFE_DISPOSE_API(cache_block_store_api);
    SAFE_DISPOSE_API(remote_capture_store_api);
    SAFE_DISPOSE_API(remote_block_store_api);
    SAFE_DISPOSE_API(local_block_store_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

TEST(Longtail, TestFileSystemLock)
{
    HLongtail_FileLock file_lock;
    ASSERT_EQ(0, Longtail_LockFile(Longtail_Alloc(Longtail_GetFileLockSize()), "lockfile.tmp", &file_lock));
    ASSERT_EQ(0, Longtail_UnlockFile(file_lock));
    Longtail_Free(file_lock);
}
