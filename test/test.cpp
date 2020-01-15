#include "../src/longtail.h"
#include "../src/ext/stb_ds.h"

#include "ext/jc_test.h"

#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/brotli/longtail_brotli.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/lizard/longtail_lizard.h"
#include "../lib/memstorage/longtail_memstorage.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/blake2/longtail_blake2.h"

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

TEST(Longtail, LongtailMalloc)
{
    void* p = Longtail_Alloc(77);
    ASSERT_NE((void*)0, p);
    Longtail_Free(p);
}

TEST(Longtail, LongtailLizard)
{
    Longtail_CompressionAPI* compression_api = Longtail_CreateLizardCompressionAPI();
    ASSERT_NE((Longtail_CompressionAPI*)0, compression_api);

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

    Longtail_CompressionAPI_HCompressionContext compression_context;
    ASSERT_EQ(0, compression_api->CreateCompressionContext(compression_api, compression_api->GetDefaultSettings(compression_api), &compression_context));
    size_t max_compressed_size = compression_api->GetMaxCompressedSize(compression_api, compression_context, data_len);
    char* compressed_buffer = (char*)Longtail_Alloc(max_compressed_size);
    ASSERT_NE((char*)0, compressed_buffer);
    size_t compressed_size = 0;
    {
        size_t total_consumed_size = 0;
        size_t total_produced_size = 0;
        while (total_consumed_size < data_len)
        {
            size_t consumed_size = 0;
            size_t produced_size = 0;
            ASSERT_EQ(0, compression_api->Compress(compression_api, compression_context, &raw_data[total_consumed_size], &compressed_buffer[total_produced_size], data_len - total_consumed_size, max_compressed_size - total_produced_size, &consumed_size, &produced_size));
            total_consumed_size += consumed_size;
            total_produced_size += produced_size;
        }

        size_t produced_size;
        ASSERT_EQ(0, compression_api->FinishCompress(compression_api, compression_context, &compressed_buffer[total_produced_size], max_compressed_size - total_produced_size, &produced_size));
        total_produced_size += produced_size;
        compressed_size = total_produced_size;
    }

    compression_api->DeleteCompressionContext(compression_api, compression_context);

    Longtail_CompressionAPI_HDecompressionContext decompression_context;
    ASSERT_EQ(0, compression_api->CreateDecompressionContext(compression_api, &decompression_context));
    char* decompressed_buffer = (char*)Longtail_Alloc(data_len);
    ASSERT_NE((char*)0, decompressed_buffer);
    size_t decompressed_size = 0;
    {
        size_t total_consumed_size = 0;
        size_t total_produced_size = 0;
        while (total_consumed_size < compressed_size)
        {
            size_t consumed_size = 0;
            size_t produced_size = 0;
            ASSERT_EQ(0, compression_api->Decompress(compression_api, decompression_context, &compressed_buffer[total_consumed_size], &decompressed_buffer[total_produced_size], compressed_size - total_consumed_size, data_len - total_produced_size, &consumed_size, &produced_size));
            total_consumed_size += consumed_size;
            total_produced_size += produced_size;
        }
        decompressed_size = total_produced_size;
    }
    ASSERT_EQ(data_len, decompressed_size);
    ASSERT_STREQ(raw_data, decompressed_buffer);
    Longtail_Free(decompressed_buffer);
    Longtail_Free(compressed_buffer);

    Longtail_DisposeAPI(&compression_api->m_API);
}

TEST(Longtail, LongtailBrotli)
{
    Longtail_CompressionAPI* compression_api = Longtail_CreateBrotliCompressionAPI();
    ASSERT_NE((Longtail_CompressionAPI*)0, compression_api);

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

    Longtail_CompressionAPI_HCompressionContext compression_context;
    ASSERT_EQ(0, compression_api->CreateCompressionContext(compression_api, compression_api->GetDefaultSettings(compression_api), &compression_context));
    size_t max_compressed_size = compression_api->GetMaxCompressedSize(compression_api, compression_context, data_len);
    char* compressed_buffer = (char*)Longtail_Alloc(max_compressed_size);
    ASSERT_NE((char*)0, compressed_buffer);
    size_t compressed_size = 0;
    {
        size_t total_consumed_size = 0;
        size_t total_produced_size = 0;
        while (total_consumed_size < data_len)
        {
            size_t consumed_size = 0;
            size_t produced_size = 0;
            ASSERT_EQ(0, compression_api->Compress(compression_api, compression_context, &raw_data[total_consumed_size], &compressed_buffer[total_produced_size], data_len - total_consumed_size, max_compressed_size - total_produced_size, &consumed_size, &produced_size));
            total_consumed_size += consumed_size;
            total_produced_size += produced_size;
        }

        size_t produced_size;
        ASSERT_EQ(0, compression_api->FinishCompress(compression_api, compression_context, &compressed_buffer[total_produced_size], max_compressed_size - total_produced_size, &produced_size));
        total_produced_size += produced_size;
        compressed_size = total_produced_size;
    }

    compression_api->DeleteCompressionContext(compression_api, compression_context);

    Longtail_CompressionAPI_HDecompressionContext decompression_context;
    ASSERT_EQ(0, compression_api->CreateDecompressionContext(compression_api, &decompression_context));
    char* decompressed_buffer = (char*)Longtail_Alloc(data_len);
    ASSERT_NE((char*)0, decompressed_buffer);
    size_t decompressed_size = 0;
    {
        size_t total_consumed_size = 0;
        size_t total_produced_size = 0;
        while (total_consumed_size < compressed_size)
        {
            size_t consumed_size = 0;
            size_t produced_size = 0;
            ASSERT_EQ(0, compression_api->Decompress(compression_api, decompression_context, &compressed_buffer[total_consumed_size], &decompressed_buffer[total_produced_size], compressed_size - total_consumed_size, data_len - total_produced_size, &consumed_size, &produced_size));
            total_consumed_size += consumed_size;
            total_produced_size += produced_size;
        }
        decompressed_size = total_produced_size;
    }
    ASSERT_EQ(data_len, decompressed_size);
    ASSERT_STREQ(raw_data, decompressed_buffer);
    Longtail_Free(decompressed_buffer);
    Longtail_Free(compressed_buffer);
    compression_api->DeleteDecompressionContext(compression_api, decompression_context);

    Longtail_DisposeAPI(&compression_api->m_API);
}

TEST(Longtail, LongtailBlake2)
{
}

TEST(Longtail, LongtailMeowHash)
{
}

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
    const uint32_t chunk_sizes[5] = {64003u, 64003u, 64002u, 64001u, 64001u};
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};
    const uint32_t asset_compression_types[5] = {0, 0, 0, 0, 0};

    Longtail_Paths* paths;
    ASSERT_EQ(0, Longtail_MakePaths(5, asset_paths, &paths));
    size_t version_index_size = Longtail_GetVersionIndexSize(5, 5, 5, paths->m_DataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);

    Longtail_VersionIndex* version_index = Longtail_BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        *paths->m_PathCount,
        asset_chunk_start_index,
        *paths->m_PathCount,
        chunk_sizes,
        asset_content_hashes,
        asset_compression_types,
        0u); // Dummy hash API

    void* store_buffer = 0;
    size_t store_size = 0;
    ASSERT_EQ(0, Longtail_WriteVersionIndexToBuffer(version_index, &store_buffer, &store_size));
    Longtail_VersionIndex* version_index_copy;
    ASSERT_EQ(0, Longtail_ReadVersionIndexFromBuffer(store_buffer, store_size, &version_index_copy));
    ASSERT_EQ(*version_index->m_AssetCount, *version_index_copy->m_AssetCount);
    Longtail_Free(version_index_copy);
    Longtail_Free(store_buffer);

    Longtail_Free(version_index);
    Longtail_Free(paths);
}

TEST(Longtail, Longtail_ContentIndex)
{
//    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
//    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = { 43593u, 43593u, 43592u, 43591u, 43591u };
    const uint32_t asset_compression_types[5] = {0, 0, 0, 0, 0};
//    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
//    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

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
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        asset_count,
        asset_content_hashes,
        asset_sizes,
        asset_compression_types,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    ASSERT_EQ(2u, *content_index->m_BlockCount);
    ASSERT_EQ(5u, *content_index->m_ChunkCount);
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        ASSERT_EQ(asset_content_hashes[i], content_index->m_ChunkHashes[i]);
        ASSERT_EQ(asset_sizes[i], content_index->m_ChunkLengths[i]);
    }
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[1]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[4]);

    ASSERT_EQ(0u, content_index->m_ChunkBlockOffsets[0]);
    ASSERT_EQ(43593u, content_index->m_ChunkBlockOffsets[1]);
    ASSERT_EQ(43593u * 2u, content_index->m_ChunkBlockOffsets[2]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockOffsets[3]);
    ASSERT_EQ(43591u, content_index->m_ChunkBlockOffsets[4]);

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

TEST(Longtail, Longtail_RetargetContentIndex)
{
//    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
//    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = { 43593u, 43593u, 43592u, 43591u, 43591u };
    const uint32_t asset_compression_types[5] = {0, 0, 0, 0, 0};
//    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
//    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

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
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        asset_count,
        asset_content_hashes,
        asset_sizes,
        asset_compression_types,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &content_index));

    ASSERT_EQ(2u, *content_index->m_BlockCount);
    ASSERT_EQ(5u, *content_index->m_ChunkCount);
    for (uint32_t i = 0; i < *content_index->m_ChunkCount; ++i)
    {
        ASSERT_EQ(asset_content_hashes[i], content_index->m_ChunkHashes[i]);
        ASSERT_EQ(asset_sizes[i], content_index->m_ChunkLengths[i]);
    }
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[1]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(1u, content_index->m_ChunkBlockIndexes[4]);

    ASSERT_EQ(0u, content_index->m_ChunkBlockOffsets[0]);
    ASSERT_EQ(43593u, content_index->m_ChunkBlockOffsets[1]);
    ASSERT_EQ(43593u * 2u, content_index->m_ChunkBlockOffsets[2]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockOffsets[3]);
    ASSERT_EQ(43591u, content_index->m_ChunkBlockOffsets[4]);

    Longtail_ContentIndex* other_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        asset_count - 1,
        &asset_content_hashes[1],
        &asset_sizes[1],
        &asset_compression_types[1],
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

static uint32_t* GetCompressionTypes(Longtail_StorageAPI* , const Longtail_FileInfos* file_infos)
{
    uint32_t count = *file_infos->m_Paths.m_PathCount;
    uint32_t* result = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * count);
    for (uint32_t i = 0; i < count; ++i)
    {
        result[i] = (i & 1) ? LONGTAIL_BROTLI_DEFAULT_COMPRESSION_TYPE : LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE;
    }
    return result;
}

TEST(Longtail, ContentIndexSerialization)
{
    Longtail_StorageAPI* local_storage = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    ASSERT_EQ(1, CreateFakeContent(local_storage, "source/version1/two_items", 2));
    ASSERT_EQ(1, CreateFakeContent(local_storage, "source/version1/five_items", 5));
    Longtail_FileInfos* version1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, "source/version1", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* compression_types = GetCompressionTypes(local_storage, version1_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        job_api,
        0,
        0,
        "source/version1",
        &version1_paths->m_Paths,
        version1_paths->m_FileSizes,
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
        *vindex->m_ChunkCount,
        vindex->m_ChunkHashes,
        vindex->m_ChunkSizes,
        vindex->m_ChunkCompressionTypes,
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
        ASSERT_EQ(cindex->m_ChunkBlockOffsets[i], cindex2->m_ChunkBlockOffsets[i]);
        ASSERT_EQ(cindex->m_ChunkLengths[i], cindex2->m_ChunkLengths[i]);
    }

    Longtail_Free(cindex);
    cindex = 0;

    Longtail_Free(cindex2);
    cindex2 = 0;

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(local_storage);
}

Longtail_CompressionRegistryAPI* CreateDefaultCompressionRegistry()
{
    Longtail_CompressionAPI* lizard_compression = Longtail_CreateLizardCompressionAPI();
    if (lizard_compression == 0)
    {
        return 0;
    }
    Longtail_CompressionAPI_HSettings lizard_settings = lizard_compression->GetDefaultSettings(lizard_compression);

    Longtail_CompressionAPI* brotli_compression = Longtail_CreateBrotliCompressionAPI();
    if (brotli_compression == 0)
    {
        return 0;
    }
    Longtail_CompressionAPI_HSettings brotli_settings = brotli_compression->GetDefaultSettings(brotli_compression);

    uint32_t compression_types[2] = {LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE, LONGTAIL_BROTLI_DEFAULT_COMPRESSION_TYPE};
    struct Longtail_CompressionAPI* compression_apis[2] = {lizard_compression, brotli_compression};
    Longtail_CompressionAPI_HSettings compression_settings[2] = {lizard_settings, brotli_settings};

    Longtail_CompressionRegistryAPI* registry = Longtail_CreateDefaultCompressionRegistry(
        2,
        (const uint32_t*)compression_types,
        (const Longtail_CompressionAPI **)compression_apis,
        (const Longtail_CompressionAPI_HSettings*)compression_settings);
    if (registry == 0)
    {
        SAFE_DISPOSE_API(lizard_compression);
        return 0;
    }
    return registry;
}

TEST(Longtail, Longtail_WriteContent)
{
    Longtail_StorageAPI* source_storage = Longtail_CreateInMemStorageAPI();
    Longtail_StorageAPI* target_storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

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
    ASSERT_EQ(0, Longtail_GetFilesRecursively(source_storage, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* compression_types = GetCompressionTypes(source_storage, version1_paths);
    ASSERT_NE((uint32_t*)0, compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        source_storage,
        hash_api,
        job_api,
        0,
        0,
        "local",
        &version1_paths->m_Paths,
        version1_paths->m_FileSizes,
        compression_types,
        16,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(compression_types);
    compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;
    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        *vindex->m_ChunkCount,
        vindex->m_ChunkHashes,
        vindex->m_ChunkSizes,
        vindex->m_ChunkCompressionTypes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    ASSERT_EQ(0, Longtail_WriteContent(
        source_storage,
        target_storage,
        compression_registry,
        job_api,
        0,
        0,
        cindex,
        vindex,
        "local",
        "chunks"));

    Longtail_ContentIndex* cindex2;
    ASSERT_EQ(0, Longtail_ReadContent(
        target_storage,
        hash_api,
        job_api,
        0,
        0,
        "chunks",
        &cindex2));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex2);

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
        ASSERT_EQ(cindex->m_ChunkBlockOffsets[i], cindex2->m_ChunkBlockOffsets[i2]);
        ASSERT_EQ(cindex->m_ChunkLengths[i], cindex2->m_ChunkLengths[i2]);
    }

    Longtail_Free(cindex2);
    Longtail_Free(cindex);
    Longtail_Free(vindex);

    SAFE_DISPOSE_API(job_api);
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
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, assets_path, &paths));
    Longtail_VersionIndex* version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        assets_path,
        &paths->m_Paths,
        paths->m_FileSizes,
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
//    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
//    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};
    const uint32_t asset_compression_types[5] = {0, 0, 0, 0, 0};

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        asset_count - 4,
        asset_content_hashes,
        chunk_sizes,
        asset_compression_types,
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

    Longtail_Paths* paths;
    ASSERT_EQ(0, Longtail_MakePaths(5, asset_paths, &paths));
    size_t version_index_size = Longtail_GetVersionIndexSize(5, 5, 5, paths->m_DataSize);
    void* version_index_mem = Longtail_Alloc(version_index_size);

    Longtail_VersionIndex* version_index = Longtail_BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        *paths->m_PathCount,
        asset_chunk_start_index,
        *paths->m_PathCount,
        chunk_sizes,
        asset_content_hashes,
        asset_compression_types,
        0u);    // Dummy hash API
    Longtail_Free(paths);

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
    ASSERT_EQ(asset_sizes[4], missing_content_index->m_ChunkLengths[3]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(asset_content_hashes[3], missing_content_index->m_ChunkHashes[2]);
    ASSERT_EQ(asset_sizes[3], missing_content_index->m_ChunkLengths[2]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(asset_content_hashes[2], missing_content_index->m_ChunkHashes[1]);
    ASSERT_EQ(asset_sizes[2], missing_content_index->m_ChunkLengths[1]);

    ASSERT_EQ(1u, missing_content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(asset_content_hashes[1], missing_content_index->m_ChunkHashes[0]);
    ASSERT_EQ(asset_sizes[1], missing_content_index->m_ChunkLengths[0]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockOffsets[0]);
    ASSERT_EQ(43593u, missing_content_index->m_ChunkBlockOffsets[1]);
    ASSERT_EQ(43593u + 43592u, missing_content_index->m_ChunkBlockOffsets[2]);
    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockOffsets[3]);

    Longtail_Free(version_index);
    Longtail_Free(content_index);

    Longtail_Free(missing_content_index);

    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, GetMissingAssets)
{
//    uint64_t GetMissingAssets(const Longtail_ContentIndex* content_index, const Longtail_VersionIndex* version, TLongtail_Hash* missing_assets)
}

TEST(Longtail, VersionIndexDirectories)
{
    Longtail_StorageAPI* local_storage = Longtail_CreateInMemStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    ASSERT_EQ(1, CreateFakeContent(local_storage, "two_items", 2));
    ASSERT_EQ(0, local_storage->CreateDir(local_storage, "no_items"));
    ASSERT_EQ(1, CreateFakeContent(local_storage, "deep/file/down/under/three_items", 3));
    ASSERT_EQ(1, MakePath(local_storage, "deep/folders/with/nothing/in/menoexists.nop"));

    Longtail_FileInfos* local_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, "", &local_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_paths);
    uint32_t* compression_types = GetCompressionTypes(local_storage, local_paths);
    ASSERT_NE((uint32_t*)0, compression_types);

    Longtail_VersionIndex* local_version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        job_api,
        0,
        0,
        "",
        &local_paths->m_Paths,
        local_paths->m_FileSizes,
        compression_types,
        16384,
        &local_version_index));
    ASSERT_NE((Longtail_VersionIndex*)0, local_version_index);
    ASSERT_EQ(16u, *local_version_index->m_AssetCount);

    Longtail_Free(compression_types);
    Longtail_Free(local_version_index);
    Longtail_Free(local_paths);

    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(local_storage);
}

TEST(Longtail, Longtail_MergeContentIndex)
{
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_ContentIndex* cindex1;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        0,
        0,
        0,
        0,
        16,
        8,
        &cindex1));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex1);
    Longtail_ContentIndex* cindex2;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        0,
        0,
        0,
        0,
        16,
        8,
        &cindex2));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex2);
    Longtail_ContentIndex* cindex3;
    ASSERT_EQ(0, Longtail_MergeContentIndex(cindex1, cindex2, &cindex3));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex3);

    TLongtail_Hash chunk_hashes_4[] = {5, 6, 7};
    uint32_t chunk_sizes_4[] = {10, 20, 10};
    uint32_t chunk_compression_types_4[] = {0, 0, 0};
    Longtail_ContentIndex* cindex4;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
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
    ASSERT_EQ(0, Longtail_CreateContentIndex(
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
    ASSERT_EQ(0, Longtail_MergeContentIndex(cindex4, cindex5, &cindex6));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex6);
    ASSERT_EQ(4u, *cindex6->m_BlockCount);
    ASSERT_EQ(6u, *cindex6->m_ChunkCount);

    Longtail_ContentIndex* cindex7;
    ASSERT_EQ(0, Longtail_MergeContentIndex(cindex6, cindex1, &cindex7));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex7);
    ASSERT_EQ(4u, *cindex7->m_BlockCount);
    ASSERT_EQ(6u, *cindex7->m_ChunkCount);

    Longtail_Free(cindex7);
    Longtail_Free(cindex6);
    Longtail_Free(cindex5);
    Longtail_Free(cindex4);
    Longtail_Free(cindex3);
    Longtail_Free(cindex2);
    Longtail_Free(cindex1);

    SAFE_DISPOSE_API(hash_api);
}

TEST(Longtail, Longtail_VersionDiff)
{
    Longtail_StorageAPI* storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    const uint32_t OLD_ASSET_COUNT = 9u;

    const char* OLD_TEST_FILENAMES[] = {
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

    const char* OLD_TEST_STRINGS[] = {
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

    const size_t OLD_TEST_SIZES[] = {
        strlen(OLD_TEST_STRINGS[0]) + 1,
        strlen(OLD_TEST_STRINGS[1]) + 1,
        strlen(OLD_TEST_STRINGS[2]) + 1,
        strlen(OLD_TEST_STRINGS[3]) + 1,
        strlen(OLD_TEST_STRINGS[4]) + 1,
        strlen(OLD_TEST_STRINGS[5]) + 1,
        strlen(OLD_TEST_STRINGS[6]) + 1,
        strlen(OLD_TEST_STRINGS[7]) + 1,
        strlen(OLD_TEST_STRINGS[8]) + 1,
        0
    };

    const uint32_t NEW_ASSET_COUNT = 9u;

    const char* NEW_TEST_FILENAMES[] = {
        "ContentChangedSameLength.txt",
        "HasBeenRenamed.txt",
        "ContentSameButShorter.txt",
        "folder/ContentSameButLonger.txt",
        "NewRenamedFolder/MovedToNewFolder.txt",
        "JustDifferent.txt",
        "EmptyFileInFolder/.init.py",
        "a/file/in/folder/LongWithChangedStart.dll",
        "a/file/in/other/folder/LongChangedAtEnd.exe"
    };

    const char* NEW_TEST_STRINGS[] = {
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
            "This is the end tough...!"  // Modified at start
    };

    const size_t NEW_TEST_SIZES[] = {
        strlen(NEW_TEST_STRINGS[0]) + 1,
        strlen(NEW_TEST_STRINGS[1]) + 1,
        strlen(NEW_TEST_STRINGS[2]) + 1,
        strlen(NEW_TEST_STRINGS[3]) + 1,
        strlen(NEW_TEST_STRINGS[4]) + 1,
        strlen(NEW_TEST_STRINGS[5]) + 1,
        strlen(NEW_TEST_STRINGS[6]) + 1,
        strlen(NEW_TEST_STRINGS[7]) + 1,
        strlen(NEW_TEST_STRINGS[8]) + 1,
        0
    };

    for (uint32_t i = 0; i < OLD_ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "old", OLD_TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (OLD_TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, OLD_TEST_SIZES[i], OLD_TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
    }

    for (uint32_t i = 0; i < NEW_ASSET_COUNT; ++i)
    {
        char* file_name = storage->ConcatPath(storage, "new", NEW_TEST_FILENAMES[i]);
        ASSERT_NE(0, CreateParentPath(storage, file_name));
        Longtail_StorageAPI_HOpenFile w;
        ASSERT_EQ(0, storage->OpenWriteFile(storage, file_name, 0, &w));
        Longtail_Free(file_name);
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, w);
        if (NEW_TEST_SIZES[i])
        {
            ASSERT_EQ(0, storage->Write(storage, w, 0, NEW_TEST_SIZES[i], NEW_TEST_STRINGS[i]));
        }
        storage->CloseFile(storage, w);
        w = 0;
    }

    Longtail_FileInfos* old_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, "old", &old_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, old_version_paths);
    uint32_t* old_compression_types = GetCompressionTypes(storage, old_version_paths);
    ASSERT_NE((uint32_t*)0, old_compression_types);
    Longtail_VersionIndex* old_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        job_api,
        0,
        0,
        "old",
        &old_version_paths->m_Paths,
        old_version_paths->m_FileSizes,
        old_compression_types,
        16,
        &old_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, old_vindex);
    Longtail_Free(old_compression_types);
    old_compression_types = 0;
    Longtail_Free(old_version_paths);
    old_version_paths = 0;

    Longtail_FileInfos* new_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, "new", &new_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, new_version_paths);
    uint32_t* new_compression_types = GetCompressionTypes(storage, new_version_paths);
    ASSERT_NE((uint32_t*)0, new_compression_types);
    Longtail_VersionIndex* new_vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage,
        hash_api,
        job_api,
        0,
        0,
        "new",
        &new_version_paths->m_Paths,
        new_version_paths->m_FileSizes,
        new_compression_types,
        16,
        &new_vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, new_vindex);
    Longtail_Free(new_compression_types);
    new_compression_types = 0;
    Longtail_Free(new_version_paths);
    new_version_paths = 0;

    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;

    Longtail_ContentIndex* content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            *new_vindex->m_ChunkCount,
            new_vindex->m_ChunkHashes,
            new_vindex->m_ChunkSizes,
            new_vindex->m_ChunkCompressionTypes,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        storage,
        storage,
        compression_registry,
        job_api,
        0,
        0,
        content_index,
        new_vindex,
        "new",
        "chunks"));

    Longtail_VersionDiff* version_diff;
    ASSERT_EQ(0, Longtail_CreateVersionDiff(
        old_vindex,
        new_vindex,
        &version_diff));
    ASSERT_NE((Longtail_VersionDiff*)0, version_diff);

    ASSERT_EQ(3u, *version_diff->m_SourceRemovedCount);
    ASSERT_EQ(3u, *version_diff->m_TargetAddedCount);
    ASSERT_EQ(6u, *version_diff->m_ModifiedCount);

    ASSERT_EQ(0, Longtail_ChangeVersion(
        storage,
        storage,
        hash_api,
        job_api,
        0,
        0,
        compression_registry,
        content_index,
        old_vindex,
        new_vindex,
        version_diff,
        "chunks",
        "old"));

    Longtail_Free(content_index);

    Longtail_Free(version_diff);

    Longtail_Free(new_vindex);
    Longtail_Free(old_vindex);

    // Verify that our old folder now matches the new folder data
    Longtail_FileInfos* updated_version_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage, "old", &updated_version_paths));
    ASSERT_NE((Longtail_FileInfos*)0, updated_version_paths);
    const uint32_t NEW_ASSET_FOLDER_EXTRA_COUNT = 10u;
    ASSERT_EQ(NEW_ASSET_COUNT + NEW_ASSET_FOLDER_EXTRA_COUNT, *updated_version_paths->m_Paths.m_PathCount);
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

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage);
}

TEST(Longtail, FullScale)
{
    if ((1)) return;
    Longtail_StorageAPI* local_storage = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    CreateFakeContent(local_storage, 0, 5);

    Longtail_StorageAPI* remote_storage = Longtail_CreateInMemStorageAPI();
    CreateFakeContent(remote_storage, 0, 10);

    Longtail_FileInfos* local_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(local_storage, "", &local_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_paths);
    uint32_t* local_compression_types = GetCompressionTypes(local_storage, local_paths);
    ASSERT_NE((uint32_t*)0, local_compression_types);

    Longtail_VersionIndex* local_version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        local_storage,
        hash_api,
        job_api,
        0,
        0,
        "",
        &local_paths->m_Paths,
        local_paths->m_FileSizes,
        local_compression_types,
        16384,
        &local_version_index));
    ASSERT_NE((Longtail_VersionIndex*)0, local_version_index);
    ASSERT_EQ(5u, *local_version_index->m_AssetCount);
    Longtail_Free(local_compression_types);
    local_compression_types = 0;

    Longtail_FileInfos* remote_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(remote_storage, "", &remote_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_paths);
    uint32_t* remote_compression_types = GetCompressionTypes(local_storage, remote_paths);
    ASSERT_NE((uint32_t*)0, remote_compression_types);
    Longtail_VersionIndex* remote_version_index;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        remote_storage,
        hash_api,
        job_api,
        0,
        0,
        "",
        &remote_paths->m_Paths,
        remote_paths->m_FileSizes,
        remote_compression_types,
        16384,
        &remote_version_index));
    ASSERT_NE((Longtail_VersionIndex*)0, remote_version_index);
    ASSERT_EQ(10u, *remote_version_index->m_AssetCount);
    Longtail_Free(remote_compression_types);
    remote_compression_types = 0;

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;

    Longtail_ContentIndex* local_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            * local_version_index->m_ChunkCount,
            local_version_index->m_ChunkHashes,
            local_version_index->m_ChunkSizes,
            local_version_index->m_ChunkCompressionTypes,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &local_content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        local_storage,
        local_storage,
        compression_registry,
        job_api,
        0,
        0,
        local_content_index,
        local_version_index,
        "",
        ""));

    Longtail_ContentIndex* remote_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            * remote_version_index->m_ChunkCount,
            remote_version_index->m_ChunkHashes,
            remote_version_index->m_ChunkSizes,
            remote_version_index->m_ChunkCompressionTypes,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &remote_content_index));

    ASSERT_EQ(0, Longtail_WriteContent(
        remote_storage,
        remote_storage,
        compression_registry,
        job_api,
        0,
        0,
        remote_content_index,
        remote_version_index,
        "",
        ""));

    Longtail_ContentIndex* missing_content;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        local_content_index,
        remote_version_index,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &missing_content));
    ASSERT_NE((Longtail_ContentIndex*)0, missing_content);
 
    ASSERT_EQ(0, Longtail_WriteContent(
        remote_storage,
        local_storage,
        compression_registry,
        job_api,
        0,
        0,
        missing_content,
        remote_version_index,
        "",
        ""));

    Longtail_ContentIndex* merged_content_index;
    ASSERT_EQ(0, Longtail_MergeContentIndex(local_content_index, missing_content, &merged_content_index));
    ASSERT_EQ(0, Longtail_WriteVersion(
        local_storage,
        local_storage,
        compression_registry,
        job_api,
        0,
        0,
        merged_content_index,
        remote_version_index,
        "",
        ""));

    for(uint32_t i = 0; i < 10; ++i)
    {
        char path[20];
        sprintf(path, "%u", i);
        Longtail_StorageAPI_HOpenFile r;
        ASSERT_EQ(0, local_storage->OpenReadFile(local_storage, path, &r));
        ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, r);
        uint64_t size;
        ASSERT_EQ(0, local_storage->GetSize(local_storage, r, &size));
        uint64_t expected_size = 64000 + 1 + i;
        ASSERT_EQ(expected_size, size);
        char* buffer = (char*)Longtail_Alloc(expected_size);
        ASSERT_EQ(0, local_storage->Read(local_storage, r, 0, expected_size, buffer));

        for (uint64_t j = 0; j < expected_size; j++)
        {
            ASSERT_EQ((int)i, (int)buffer[j]);
        }

        Longtail_Free(buffer);
        local_storage->CloseFile(local_storage, r);
    }

    Longtail_Free(missing_content);
    Longtail_Free(merged_content_index);
    Longtail_Free(remote_content_index);
    Longtail_Free(local_content_index);
    Longtail_Free(remote_version_index);
    Longtail_Free(local_version_index);

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(local_storage);
}


TEST(Longtail, Longtail_WriteVersion)
{
    Longtail_StorageAPI* storage_api = Longtail_CreateInMemStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateBlake2HashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

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
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, "local", &version1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, version1_paths);
    uint32_t* version1_compression_types = GetCompressionTypes(storage_api, version1_paths);
    ASSERT_NE((uint32_t*)0, version1_compression_types);
    Longtail_VersionIndex* vindex;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        "local",
        &version1_paths->m_Paths,
        version1_paths->m_FileSizes,
        version1_compression_types,
        50,
        &vindex));
    ASSERT_NE((Longtail_VersionIndex*)0, vindex);
    Longtail_Free(version1_compression_types);
    version1_compression_types = 0;
    Longtail_Free(version1_paths);
    version1_paths = 0;

    static const uint32_t MAX_BLOCK_SIZE = 32u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3u;
    Longtail_ContentIndex* cindex;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        *vindex->m_ChunkCount,
        vindex->m_ChunkHashes,
        vindex->m_ChunkSizes,
        vindex->m_ChunkCompressionTypes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &cindex));
    ASSERT_NE((Longtail_ContentIndex*)0, cindex);

    ASSERT_EQ(0, Longtail_WriteContent(
        storage_api,
        storage_api,
        compression_registry,
        job_api,
        0,
        0,
        cindex,
        vindex,
        "local",
        "chunks"));

    ASSERT_EQ(0, Longtail_WriteVersion(
        storage_api,
        storage_api,
        compression_registry,
        job_api,
        0,
        0,
        cindex,
        vindex,
        "chunks",
        "remote"));

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
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);
}

static void Bench()
{
    if ((1)) return;


#if 0
    #define HOME "D:\\Temp\\longtail"

    const uint32_t VERSION_COUNT = 6u;
    const char* VERSION[VERSION_COUNT] = {
        "git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor",
        "git916600e1ecb9da13f75835cd1b2d2e6a67f1a92d_Win64_Editor",
        "gitfdeb1390885c2f426700ca653433730d1ca78dab_Win64_Editor",
        "git81cccf054b23a0b5a941612ef0a2a836b6e02fd6_Win64_Editor",
        "git558af6b2a10d9ab5a267b219af4f795a17cc032f_Win64_Editor",
        "gitc2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c_Win64_Editor"
    };
#else
    #define HOME "C:\\Temp\\longtail"

    const uint32_t VERSION_COUNT = 2u;
    const char* VERSION[VERSION_COUNT] = {
        "git75a99408249875e875f8fba52b75ea0f5f12a00e_Win64_Editor",
        "gitb1d3adb4adce93d0f0aa27665a52be0ab0ee8b59_Win64_Editor"
    };
#endif

    const char* SOURCE_VERSION_PREFIX = HOME "\\local\\";
    const char* VERSION_INDEX_SUFFIX = ".lvi";
    //const char* CONTENT_INDEX_SUFFIX = ".lci";

    const char* CONTENT_FOLDER = HOME "\\chunks";

    const char* UPLOAD_VERSION_PREFIX = HOME "\\upload\\";
    const char* UPLOAD_VERSION_SUFFIX = "_chunks";

    const char* TARGET_VERSION_PREFIX = HOME "\\remote\\";

    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_ContentIndex* full_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
            hash_api,
            0,
            0,
            0,
            0,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &full_content_index));
    ASSERT_NE((Longtail_ContentIndex*)0, full_content_index);
    Longtail_VersionIndex* version_indexes[VERSION_COUNT];

    for (uint32_t i = 0; i < VERSION_COUNT; ++i)
    {
        char version_source_folder[256];
        sprintf(version_source_folder, "%s%s", SOURCE_VERSION_PREFIX, VERSION[i]);
        printf("Indexing `%s`\n", version_source_folder);
        Longtail_FileInfos* version_source_paths;
        ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, version_source_folder, &version_source_paths));
        ASSERT_NE((Longtail_FileInfos*)0, version_source_paths);
        uint32_t* version_compression_types = GetCompressionTypes(storage_api, version_source_paths);
        ASSERT_NE((uint32_t*)0, version_compression_types);
        Longtail_VersionIndex* version_index;
        ASSERT_EQ(0, Longtail_CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            0,
            0,
            version_source_folder,
            &version_source_paths->m_Paths,
            version_source_paths->m_FileSizes,
            version_compression_types,
            16384,
            &version_index));
        Longtail_Free(version_compression_types);
        version_compression_types = 0;
        Longtail_Free(version_source_paths);
        version_source_paths = 0;
        ASSERT_NE((Longtail_VersionIndex*)0, version_index);
        printf("Indexed %u assets from `%s`\n", (uint32_t)*version_index->m_AssetCount, version_source_folder);

        char version_index_file[256];
        sprintf(version_index_file, "%s%s%s", SOURCE_VERSION_PREFIX, VERSION[i], VERSION_INDEX_SUFFIX);
        ASSERT_EQ(0, Longtail_WriteVersionIndex(storage_api, version_index, version_index_file));
        printf("Wrote version index to `%s`\n", version_index_file);

        Longtail_ContentIndex* missing_content_index;
        ASSERT_EQ(0, Longtail_CreateMissingContent(
            hash_api,
            full_content_index,
            version_index,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK,
            &missing_content_index));
        ASSERT_NE((Longtail_ContentIndex*)0, missing_content_index);

        char delta_upload_content_folder[256];
        sprintf(delta_upload_content_folder, "%s%s%s", UPLOAD_VERSION_PREFIX, VERSION[i], UPLOAD_VERSION_SUFFIX);
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder);
        ASSERT_EQ(0, Longtail_WriteContent(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            0,
            0,
            missing_content_index,
            version_index,
            version_source_folder,
            delta_upload_content_folder));

        printf("Copying %" PRIu64 " blocks from `%s` to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder, CONTENT_FOLDER);
        Longtail_StorageAPI_HIterator fs_iterator;
        int err = storage_api->StartFind(storage_api, delta_upload_content_folder, &fs_iterator);
        if (!err)
        {
            do
            {
                const char* file_name = storage_api->GetFileName(storage_api, fs_iterator);
                if (file_name)
                {
                    char* target_path = storage_api->ConcatPath(storage_api, CONTENT_FOLDER, file_name);

                    Longtail_StorageAPI_HOpenFile v;
                    if (0 == storage_api->OpenReadFile(storage_api, target_path, &v))
                    {
                        storage_api->CloseFile(storage_api, v);
                        v = 0;
                        Longtail_Free(target_path);
                        continue;
                    }

                    char* source_path = storage_api->ConcatPath(storage_api, delta_upload_content_folder, file_name);

                    Longtail_StorageAPI_HOpenFile s;
                    ASSERT_EQ(0, storage_api->OpenReadFile(storage_api, source_path, &s));
                    ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, s);

                    ASSERT_NE(0, MakePath(storage_api, target_path));
                    Longtail_StorageAPI_HOpenFile t;
                    ASSERT_EQ(0, storage_api->OpenWriteFile(storage_api, target_path, 0, &t));
                    ASSERT_NE((Longtail_StorageAPI_HOpenFile)0, t);

                    uint64_t block_file_size;
                    ASSERT_EQ(0, storage_api->GetSize(storage_api, s, &block_file_size));
                    void* buffer = Longtail_Alloc(block_file_size);

                    ASSERT_EQ(0, storage_api->Read(storage_api, s, 0, block_file_size, buffer));
                    ASSERT_EQ(0, storage_api->Write(storage_api, t, 0, block_file_size, buffer));

                    Longtail_Free(buffer);
                    buffer = 0;

                    storage_api->CloseFile(storage_api, s);
                    storage_api->CloseFile(storage_api, t);

                    Longtail_Free(target_path);
                    Longtail_Free(source_path);
                }
            }while(storage_api->FindNext(storage_api, fs_iterator) == 0);
        }
        else
        {
            ASSERT_EQ(ENOENT, err);
        }

        Longtail_ContentIndex* merged_content_index;
        ASSERT_EQ(0, Longtail_MergeContentIndex(full_content_index, missing_content_index, &merged_content_index));
        ASSERT_NE((Longtail_ContentIndex*)0, merged_content_index);
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(full_content_index);
        full_content_index = merged_content_index;
        merged_content_index = 0;

        char version_target_folder[256];
        sprintf(version_target_folder, "%s%s", TARGET_VERSION_PREFIX, VERSION[i]);
        printf("Reconstructing %u assets from `%s` to `%s`\n", *version_index->m_AssetCount, CONTENT_FOLDER, version_target_folder);
        ASSERT_EQ(0, Longtail_WriteVersion(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            0,
            0,
            full_content_index,
            version_index,
            CONTENT_FOLDER,
            version_target_folder));

        version_indexes[i] = version_index;
        version_index = 0;
    }

    for (uint32_t i = 0; i < VERSION_COUNT; ++i)
    {
        Longtail_Free(version_indexes[i]);
    }

    Longtail_Free(full_content_index);

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);

    #undef HOME
}

static void LifelikeTest()
{
    if ((1)) return;

//    #define HOME "test\\data"
    #define HOME "D:\\Temp\\longtail"

    #define VERSION1 "75a99408249875e875f8fba52b75ea0f5f12a00e"
    #define VERSION2 "b1d3adb4adce93d0f0aa27665a52be0ab0ee8b59"

    #define VERSION1_FOLDER "git" VERSION1 "_Win64_Editor"
    #define VERSION2_FOLDER "git" VERSION2 "_Win64_Editor"

//    #define VERSION1_FOLDER "version1"
//    #define VERSION2_FOLDER "version2"

    const char* local_path_1 = HOME "\\local\\" VERSION1_FOLDER;
    const char* version_index_path_1 = HOME "\\local\\" VERSION1_FOLDER ".lvi";

    const char* local_path_2 = HOME "\\local\\" VERSION2_FOLDER;
    const char* version_index_path_2 = HOME "\\local\\" VERSION1_FOLDER ".lvi";

    const char* local_content_path = HOME "\\local_content";//HOME "\\local_content";
    const char* local_content_index_path = HOME "\\local.lci";

    const char* remote_content_path = HOME "\\remote_content";
    //const char* remote_content_index_path = HOME "\\remote.lci";

    const char* remote_path_1 = HOME "\\remote\\" VERSION1_FOLDER;
    const char* remote_path_2 = HOME "\\remote\\" VERSION2_FOLDER;

    printf("Indexing `%s`...\n", local_path_1);
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(0);

    Longtail_FileInfos* local_path_1_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, local_path_1, &local_path_1_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_path_1_paths);
    uint32_t* local_compression_types = GetCompressionTypes(storage_api, local_path_1_paths);
    ASSERT_NE((uint32_t*)0, local_compression_types);
    Longtail_VersionIndex* version1;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        local_path_1,
        &local_path_1_paths->m_Paths,
        local_path_1_paths->m_FileSizes,
        local_compression_types,
        16384,
        &version1));
    ASSERT_EQ(0, Longtail_WriteVersionIndex(storage_api, version1, version_index_path_1));
    Longtail_Free(local_compression_types);
    local_compression_types = 0;
    Longtail_Free(local_path_1_paths);
    local_path_1_paths = 0;
    printf("%u assets from folder `%s` indexed to `%s`\n", *version1->m_AssetCount, local_path_1, version_index_path_1);

    printf("Creating local content index...\n");
    static const uint32_t MAX_BLOCK_SIZE = 65536u * 2u;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096u;
    Longtail_ContentIndex* local_content_index;
    ASSERT_EQ(0, Longtail_CreateContentIndex(
        hash_api,
        *version1->m_ChunkCount,
        version1->m_ChunkHashes,
        version1->m_ChunkSizes,
        version1->m_ChunkCompressionTypes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &local_content_index));

    printf("Writing local content index...\n");
    ASSERT_EQ(0, Longtail_WriteContentIndex(storage_api, local_content_index, local_content_index_path));
    printf("%" PRIu64 " blocks from version `%s` indexed to `%s`\n", *local_content_index->m_BlockCount, local_path_1, local_content_index_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *local_content_index->m_BlockCount, local_content_path);
        Longtail_WriteContent(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            0,
            0,
            local_content_index,
            version1,
            local_path_1,
            local_content_path);
    }

    printf("Reconstructing %u assets to `%s`\n", *version1->m_AssetCount, remote_path_1);
    ASSERT_EQ(0, Longtail_WriteVersion(
        storage_api,
        storage_api,
        compression_registry,
        job_api,
        0,
        0,
        local_content_index,
        version1,
        local_content_path,
        remote_path_1));
    printf("Reconstructed %u assets to `%s`\n", *version1->m_AssetCount, remote_path_1);

    printf("Indexing `%s`...\n", local_path_2);
    Longtail_FileInfos* local_path_2_paths;
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, local_path_2, &local_path_2_paths));
    ASSERT_NE((Longtail_FileInfos*)0, local_path_2_paths);
    uint32_t* local_2_compression_types = GetCompressionTypes(storage_api, local_path_2_paths);
    ASSERT_NE((uint32_t*)0, local_2_compression_types);
    Longtail_VersionIndex* version2;
    ASSERT_EQ(0, Longtail_CreateVersionIndex(
        storage_api,
        hash_api,
        job_api,
        0,
        0,
        local_path_2,
        &local_path_2_paths->m_Paths,
        local_path_2_paths->m_FileSizes,
        local_2_compression_types,
        16384,
        &version2));
    Longtail_Free(local_2_compression_types);
    local_2_compression_types = 0;
    Longtail_Free(local_path_2_paths);
    local_path_2_paths = 0;
    ASSERT_NE((Longtail_VersionIndex*)0, version2);
    ASSERT_EQ(0, Longtail_WriteVersionIndex(storage_api, version2, version_index_path_2));
    printf("%u assets from folder `%s` indexed to `%s`\n", *version2->m_AssetCount, local_path_2, version_index_path_2);
    
    // What is missing in local content that we need from remote version in new blocks with just the missing assets.
    Longtail_ContentIndex* missing_content;
    ASSERT_EQ(0, Longtail_CreateMissingContent(
        hash_api,
        local_content_index,
        version2,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK,
        &missing_content));
    ASSERT_NE((Longtail_ContentIndex*)0, missing_content);
    printf("%" PRIu64 " blocks for version `%s` needed in content index `%s`\n", *missing_content->m_BlockCount, local_path_1, local_content_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, local_content_path);
        ASSERT_EQ(0, Longtail_WriteContent(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            0,
            0,
            missing_content,
            version2,
            local_path_2,
            local_content_path));
    }

    if (1)
    {
        // Write this to disk for reference to see how big the diff is...
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, remote_content_path);
        ASSERT_EQ(0, Longtail_WriteContent(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            0,
            0,
            missing_content,
            version2,
            local_path_2,
            remote_content_path));
    }

//    Longtail_ContentIndex* remote_content_index;
//    ASSERT_EQ(0, Longtail_CreateContentIndex(
//        local_path_2,
//        *version2->m_AssetCount,
//        version2->m_ContentHashes,
//        version2->m_PathHashes,
//        version2->m_AssetSize,
//        version2->m_NameOffsets,
//        version2->m_NameData,
//        GetContentTag,
//        &remote_content_index));

//    uint64_t* missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * *version2->m_AssetCount);
//    uint64_t missing_asset_count = GetMissingAssets(local_content_index, version2, missing_assets);
//
//    uint64_t* remaining_missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * missing_asset_count);
//    uint64_t remaining_missing_asset_count = 0;
//    Longtail_ContentIndex* existing_blocks = GetBlocksForAssets(remote_content_index, missing_asset_count, missing_assets, &remaining_missing_asset_count, remaining_missing_assets);
//    printf("%" PRIu64 " blocks for version `%s` available in content index `%s`\n", *existing_blocks->m_BlockCount, local_path_2, remote_content_path);

//    // Copy existing blocks
//    for (uint64_t block_index = 0; block_index < *missing_content->m_BlockCount; ++block_index)
//    {
//        TLongtail_Hash block_hash = missing_content->m_BlockHash[block_index];
//        char* block_name = GetBlockName(block_hash);
//        char block_file_name[64];
//        sprintf(block_file_name, "%s.lrb", block_name);
//        char* source_path = storage_api.ConcatPath(remote_content_path, block_file_name);
//        Longtail_StorageAPI_HOpenFile s = storage_api.OpenReadFile(source_path);
//        char* target_path = storage_api.ConcatPath(local_content_path, block_file_name);
//        Longtail_StorageAPI_HOpenFile t = storage_api.OpenWriteFile(target_path, 0);
//        uint64_t size = storage_api.GetSize(s);
//        char* buffer = (char*)malloc(size);
//        storage_api.Read(s, 0, size, buffer);
//        storage_api.Write(t, 0, size, buffer);
//        Longtail_Free(buffer);
//        storage_api.CloseFile(t);
//        storage_api.CloseFile(s);
//    }

    Longtail_ContentIndex* merged_local_content;
    ASSERT_EQ(0, Longtail_MergeContentIndex(local_content_index, missing_content, &merged_local_content));
    ASSERT_NE((Longtail_ContentIndex*)0, merged_local_content);
    Longtail_Free(missing_content);
    missing_content = 0;
    Longtail_Free(local_content_index);
    local_content_index = 0;

    printf("Reconstructing %u assets to `%s`\n", *version2->m_AssetCount, remote_path_2);
    ASSERT_EQ(0, Longtail_WriteVersion(
        storage_api,
        storage_api,
        compression_registry,
        job_api,
        0,
        0,
        merged_local_content,
        version2,
        local_content_path,
        remote_path_2));
    printf("Reconstructed %u assets to `%s`\n", *version2->m_AssetCount, remote_path_2);

//    Longtail_Free(existing_blocks);
//    existing_blocks = 0;
//    Longtail_Free(remaining_missing_assets);
//    remaining_missing_assets = 0;
//    Longtail_Free(missing_assets);
//    missing_assets = 0;
//    Longtail_Free(remote_content_index);
//    remote_content_index = 0;

    Longtail_Free(merged_local_content);
    merged_local_content = 0;

    Longtail_Free(version1);
    version1 = 0;

    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(storage_api);

    return;
}

TEST(Longtail, Bench)
{
    Bench();
}

TEST(Longtail, LifelikeTest)
{
    LifelikeTest();
}

const uint64_t ChunkSizeAvgDefault    = 64 * 1024;
const uint64_t ChunkSizeMinDefault    = ChunkSizeAvgDefault / 4;
const uint64_t ChunkSizeMaxDefault    = ChunkSizeAvgDefault * 4;

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

        static int FeederFunc(void* context, struct Longtail_Chunker* chunker, uint32_t requested_size, char* buffer, uint32_t* out_size)
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

    struct Longtail_ChunkerParams params = {ChunkSizeMinDefault, ChunkSizeAvgDefault, ChunkSizeMaxDefault};
    Longtail_Chunker* chunker;
    ASSERT_EQ(0, Longtail_CreateChunker(
        &params,
        FeederContext::FeederFunc,
        &feeder_context,
        &chunker));

    const uint32_t expected_chunk_count = 20u;
    const struct Longtail_ChunkRange expected_chunks[expected_chunk_count] =
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

    ASSERT_NE((Longtail_Chunker*)0, chunker);

    for (uint32_t i = 0; i < expected_chunk_count; ++i)
    {
        Longtail_ChunkRange r = Longtail_NextChunk(chunker);
        ASSERT_EQ(expected_chunks[i].offset, r.offset);
        ASSERT_EQ(expected_chunks[i].len, r.len);
    }
    Longtail_ChunkRange r = Longtail_NextChunk(chunker);
    ASSERT_EQ((const uint8_t*)0, r.buf);
    ASSERT_EQ(0, r.len);

    Longtail_Free(chunker);
    chunker = 0;

    fclose(large_file);
    large_file = 0;
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
    ASSERT_EQ(0, Longtail_GetFilesRecursively(storage_api, root_path, &file_infos));
    ASSERT_EQ(17u, *file_infos->m_Paths.m_PathCount);
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
        char* data = (char*)Longtail_Alloc(size);
        ASSERT_EQ(0, storage_api->Read(storage_api, f, 0, size, data));
        ASSERT_EQ(expected_size, size);
        for (uint64_t b = 0; b < size; ++b)
        {
            ASSERT_EQ(data[b], TEST_STRINGS[a][b]);
        }
        Longtail_Free(data);
        storage_api->CloseFile(storage_api, f);
        Longtail_Free(full_path);
    }

    SAFE_DISPOSE_API(storage_api);
}