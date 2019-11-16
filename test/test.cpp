#include "../src/longtail.h"
#include "../src/longtail_array.h"

#include "../third-party/jctest/src/jc_test.h"

#include "../third-party/jc_containers/src/jc_hashtable.h"

#include "../common/platform.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#define TEST_LOG(fmt, ...) \
    printf("--- ");printf(fmt, __VA_ARGS__);




//////////////

// TODO: Move to longtail.h


/*
uint64_t GetMissingAssets(const ContentIndex* content_index, const VersionIndex* version, TLongtail_Hash* missing_assets)
{
    uint32_t missing_hash_count = 0;
    DiffHashes(content_index->m_ContentHashes, *content_index->m_AssetCount, version->m_ContentHashes, *version->m_AssetCount, &missing_hash_count, missing_assets, 0, 0);
    return missing_hash_count;
}


ContentIndex* GetBlocksForAssets(const ContentIndex* content_index, uint64_t asset_count, const TLongtail_Hash* asset_hashes, uint64_t* out_missing_asset_count, uint64_t* out_missing_assets)
{
    uint64_t found_asset_count = 0;
    uint64_t* found_assets = (uint64_t*)malloc(sizeof(uint64_t) * asset_count);

    {
        uint64_t content_index_asset_count = *content_index->m_AssetCount;
        uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint64_t>::CalcSize((uint32_t)content_index_asset_count);
        jc::HashTable<TLongtail_Hash, uint32_t> content_hash_to_asset_index;
        void* content_hash_to_asset_index_mem = malloc(hash_size);
        content_hash_to_asset_index.Create(content_index_asset_count, content_hash_to_asset_index_mem);
        for (uint64_t i = 0; i < content_index_asset_count; ++i)
        {
            TLongtail_Hash asset_content_hash = content_index->m_ContentHashes[i];
            content_hash_to_asset_index.Put(asset_content_hash, i);
        }

        for (uint64_t i = 0; i < asset_count; ++i)
        {
            TLongtail_Hash asset_hash = asset_hashes[i];
            const uint32_t* asset_index_ptr = content_hash_to_asset_index.Get(asset_hash);
            if (asset_index_ptr != 0)
            {
                found_assets[found_asset_count++] = *asset_index_ptr;
            }
            else
            {
                out_missing_assets[(*out_missing_asset_count)++];
            }
        }

        free(content_hash_to_asset_index_mem);
    }

    uint32_t* asset_count_in_blocks = (uint32_t*)malloc(sizeof(uint32_t) * *content_index->m_BlockCount);
    uint64_t* asset_start_in_blocks = (uint64_t*)malloc(sizeof(uint64_t) * *content_index->m_BlockCount);
    uint64_t i = 0;
    while (i < *content_index->m_AssetCount)
    {
        uint64_t prev_block_index = content_index->m_ChunkBlockIndexes[i++];
        asset_start_in_blocks[prev_block_index] = i;
        uint32_t assets_count_in_block = 1;
        while (i < *content_index->m_AssetCount && prev_block_index == content_index->m_ChunkBlockIndexes[i])
        {
            ++assets_count_in_block;
            ++i;
        }
        asset_count_in_blocks[prev_block_index] = assets_count_in_block;
    }

    uint32_t hash_size = jc::HashTable<uint64_t, uint32_t>::CalcSize((uint32_t)*content_index->m_AssetCount);
    jc::HashTable<uint64_t, uint32_t> block_index_to_asset_count;
    void* block_index_to_first_asset_index_mem = malloc(hash_size);
    block_index_to_asset_count.Create((uint32_t)*content_index->m_AssetCount, block_index_to_first_asset_index_mem);

    uint64_t copy_block_count = 0;
    uint64_t* copy_blocks = (uint64_t*)malloc(sizeof(uint64_t) * found_asset_count);

    uint64_t unique_asset_count = 0;
    for (uint64_t i = 0; i < found_asset_count; ++i)
    {
        uint64_t asset_index = found_assets[i];
        uint64_t block_index = content_index->m_ChunkBlockIndexes[asset_index];
        uint32_t* block_index_ptr = block_index_to_asset_count.Get(block_index);
        if (!block_index_ptr)
        {
            block_index_to_asset_count.Put(block_index, 1);
            copy_blocks[copy_block_count++] = block_index;
            unique_asset_count += asset_count_in_blocks[block_index];
        }
        else
        {
            (*block_index_ptr)++;
        }
    }
    free(block_index_to_first_asset_index_mem);
    block_index_to_first_asset_index_mem = 0;

    size_t content_index_size = GetContentIndexSize(copy_block_count, unique_asset_count);
    ContentIndex* existing_content_index = (ContentIndex*)malloc(content_index_size);

    existing_content_index->m_BlockCount = (uint64_t*)&((char*)existing_content_index)[sizeof(ContentIndex)];
    existing_content_index->m_AssetCount = (uint64_t*)&((char*)existing_content_index)[sizeof(ContentIndex) + sizeof(uint64_t)];
    *existing_content_index->m_BlockCount = copy_block_count;
    *existing_content_index->m_AssetCount = unique_asset_count;
    InitContentIndex(existing_content_index);
    uint64_t asset_offset = 0;
    for (uint64_t i = 0; i < copy_block_count; ++i)
    {
        uint64_t block_index = copy_blocks[i];
        uint32_t block_asset_count = asset_count_in_blocks[block_index];
        existing_content_index->m_BlockHash[i] = content_index->m_BlockHash[block_index];
        for (uint32_t j = 0; j < block_asset_count; ++j)
        {
            uint64_t source_asset_index = asset_start_in_blocks[block_index] + j;
            uint64_t target_asset_index = asset_offset + j;
            existing_content_index->m_ContentHashes[target_asset_index] = content_index->m_ChunkBlockOffsets[source_asset_index];
            existing_content_index->m_ChunkBlockIndexes[target_asset_index] = block_index;
            existing_content_index->m_ChunkBlockOffsets[target_asset_index] = content_index->m_ChunkBlockOffsets[source_asset_index];
            existing_content_index->m_AssetLengths[target_asset_index] = content_index->m_AssetLengths[source_asset_index];
        }
        asset_offset += block_asset_count;
    }

    free(asset_count_in_blocks);
    asset_count_in_blocks = 0;
    free(asset_start_in_blocks);
    asset_start_in_blocks = 0;
    free(asset_count_in_blocks);
    asset_count_in_blocks = 0;

    free(copy_blocks);
    copy_blocks = 0;

    return existing_content_index;
}

*/


///////////////











typedef char TContent;
LONGTAIL_DECLARE_ARRAY_TYPE(TContent, malloc, free)

struct InMemStorageAPI
{
    StorageAPI m_StorageAPI;
    struct PathEntry
    {
        char* m_FileName;
        TLongtail_Hash m_ParentHash;
        TContent* m_Content;
    };
    jc::HashTable<TLongtail_Hash, PathEntry*> m_PathHashToContent;
    void* m_PathHashToContentMem;
    MeowHashAPI m_HashAPI;

    InMemStorageAPI()
        : m_StorageAPI{
            OpenReadFile,
            GetSize,
            Read,
            CloseRead,
            OpenWriteFile,
            Write,
            CloseWrite,
            CreateDir,
            RenameFile,
            ConcatPath,
            IsDir,
            IsFile,
            StartFind,
            FindNext,
            CloseFind,
            GetFileName,
            GetDirectoryName
            }
    {
        uint32_t hash_size = jc::HashTable<TLongtail_Hash, PathEntry*>::CalcSize(65536);
        m_PathHashToContentMem = malloc(hash_size);
        m_PathHashToContent.Create(65536, m_PathHashToContentMem);
    }

    ~InMemStorageAPI()
    {
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator it = m_PathHashToContent.Begin();
        while (it != m_PathHashToContent.End())
        {
            PathEntry* path_entry = *it.GetValue();
            free(path_entry->m_FileName);
            path_entry->m_FileName = 0;
            Free_TContent(path_entry->m_Content);
            path_entry->m_Content = 0;
            free(path_entry);
            ++it;
        }
        free(m_PathHashToContentMem);
        m_PathHashToContentMem = 0;
    }

    static uint64_t GetPathHash(HashAPI* hash_api, const char* path)
    {
        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, (uint32_t)strlen(path), (void*)path);
        HashAPI_HContext context = hash_api->BeginContext(hash_api);
        return MeowU64From(MeowEnd(&state, 0), 0);;
    }

    static StorageAPI_HOpenFile OpenReadFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** it = instance->m_PathHashToContent.Get(path_hash);
        if (it)
        {
            return (StorageAPI_HOpenFile)*it;
        }
        return 0;
    }
    static uint64_t GetSize(StorageAPI* storage_api, StorageAPI_HOpenFile f)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        PathEntry* path_entry = (PathEntry*)f;
        return GetSize_TContent(path_entry->m_Content);
    }
    static int Read(StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        PathEntry* path_entry = (PathEntry*)f;
        if ((offset + length) > GetSize_TContent(path_entry->m_Content))
        {
            return 0;
        }
        memcpy(output, &path_entry->m_Content[offset], length);
        return 1;
    }
    static void CloseRead(StorageAPI* , StorageAPI_HOpenFile)
    {
    }

    static TLongtail_Hash GetParentPathHash(InMemStorageAPI* instance, const char* path)
    {
        const char* dir_path_begin = strrchr(path, '/');
        if (!dir_path_begin)
        {
            return 0;
        }
        size_t dir_length = (uintptr_t)dir_path_begin - (uintptr_t)path;
        char* dir_path = (char*)malloc(dir_length + 1);
        strncpy(dir_path, path, dir_length);
        dir_path[dir_length] = '\0';
        TLongtail_Hash hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, dir_path);
        free(dir_path);
        return hash;
    }

    static const char* GetFileName(const char* path)
    {
        const char* file_name = strrchr(path, '/');
        if (file_name == 0)
        {
            return path;
        }
        return &file_name[1];
    }

    static StorageAPI_HOpenFile OpenWriteFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash != 0 && !instance->m_PathHashToContent.Get(parent_path_hash))
        {
            TEST_LOG("InMemStorageAPI_OpenWriteFile `%s` failed - parent folder does not exist\n", path)
            return 0;
        }
        TLongtail_Hash path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** it = instance->m_PathHashToContent.Get(path_hash);
        if (it)
        {
            Free_TContent((*it)->m_Content);
            free(*it);
            *it = 0;
        }
        PathEntry* path_entry = (PathEntry*)malloc(sizeof(PathEntry));
        path_entry->m_Content = SetCapacity_TContent((TContent*)0, 16);
        path_entry->m_ParentHash = parent_path_hash;
        path_entry->m_FileName = strdup(GetFileName(path));
        instance->m_PathHashToContent.Put(path_hash, path_entry);
        return (StorageAPI_HOpenFile)path_hash;
    }
    static int Write(StorageAPI* storage_api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = (TLongtail_Hash)f;
        PathEntry** it = instance->m_PathHashToContent.Get(path_hash);
        if (!it)
        {
            return 0;
        }
        TContent* content = (*it)->m_Content;
        size_t size = GetSize_TContent(content);
        if (offset + length > size)
        {
            size = offset + length;
        }
        content = SetCapacity_TContent(content, (uint32_t)size);
        SetSize_TContent(content, (uint32_t)size);
        memcpy(&(content)[offset], input, length);
        (*it)->m_Content = content;
        return 1;
    }
    static void CloseWrite(StorageAPI* , StorageAPI_HOpenFile)
    {
    }

    static int CreateDir(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash && !instance->m_PathHashToContent.Get(parent_path_hash))
        {
            TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - parent folder does not exist\n", path)
            return 0;
        }
        TLongtail_Hash path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** source_path_ptr = instance->m_PathHashToContent.Get(path_hash);
        if (source_path_ptr)
        {
            if ((*source_path_ptr)->m_Content == 0)
            {
                return 1;
            }
            TEST_LOG("InMemStorageAPI_CreateDir `%s` failed - path exists and is not a directory\n", path)
            return 0;
        }

        PathEntry* path_entry = (PathEntry*)malloc(sizeof(PathEntry));
        path_entry->m_Content = 0;
        path_entry->m_ParentHash = parent_path_hash;
        path_entry->m_FileName = strdup(GetFileName(path));
        instance->m_PathHashToContent.Put(path_hash, path_entry);
        return 1;
    }

    static int RenameFile(StorageAPI* storage_api, const char* source_path, const char* target_path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash source_path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, source_path);
        PathEntry** source_path_ptr = instance->m_PathHashToContent.Get(source_path_hash);
        if (!source_path_ptr)
        {
            TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - source path does not exist\n", source_path, target_path)
            return 0;
        }
        TLongtail_Hash target_path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, target_path);
        PathEntry** target_path_ptr = instance->m_PathHashToContent.Get(target_path_hash);
        if (target_path_ptr)
        {
            TEST_LOG("InMemStorageAPI_RenameFile from `%s` to `%s` failed - target path does not exist\n", source_path, target_path)
            return 0;
        }
        (*source_path_ptr)->m_ParentHash = GetParentPathHash(instance, target_path);
        free((*source_path_ptr)->m_FileName);
        (*source_path_ptr)->m_FileName = strdup(GetFileName(target_path));
        instance->m_PathHashToContent.Put(target_path_hash, *source_path_ptr);
        instance->m_PathHashToContent.Erase(source_path_hash);
        return 1;
    }
    static char* ConcatPath(StorageAPI* , const char* root_path, const char* sub_path)
    {
        if (root_path[0] == 0)
        {
            return strdup(sub_path);
        }
        size_t path_len = strlen(root_path) + 1 + strlen(sub_path) + 1;
        char* path = (char*)malloc(path_len);
        strcpy(path, root_path);
        strcat(path, "/");
        strcat(path, sub_path);
        return path;
    }

    static int IsDir(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash source_path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** source_path_ptr = instance->m_PathHashToContent.Get(source_path_hash);
        if (!source_path_ptr)
        {
            return 0;
        }
        return (*source_path_ptr)->m_Content == 0;
    }
    static int IsFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash source_path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** source_path_ptr = instance->m_PathHashToContent.Get(source_path_hash);
        if (!source_path_ptr)
        {
            return 0;
        }
        return (*source_path_ptr)->m_Content != 0;
    }

    static StorageAPI_HIterator StartFind(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = path[0] ? GetPathHash(&instance->m_HashAPI.m_HashAPI, path) : 0;
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)malloc(sizeof(jc::HashTable<TLongtail_Hash, uint64_t>::Iterator));
        (*it_ptr) = instance->m_PathHashToContent.Begin();
        while ((*it_ptr) != instance->m_PathHashToContent.End())
        {
            PathEntry* path_entry = *(*it_ptr).GetValue();
            if (path_entry->m_ParentHash == path_hash)
            {
                return (StorageAPI_HIterator)it_ptr;
            }
            ++(*it_ptr);
        }
        free(it_ptr);
        return (StorageAPI_HIterator)0;
    }
    static int FindNext(StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)iterator;
        TLongtail_Hash path_hash = (*(*it_ptr).GetValue())->m_ParentHash;
        ++(*it_ptr);
        while ((*it_ptr) != instance->m_PathHashToContent.End())
        {
            PathEntry* path_entry = *(*it_ptr).GetValue();
            if (path_entry->m_ParentHash == path_hash)
            {
                return 1;
            }
            ++(*it_ptr);
        }
        return 0;
    }
    static void CloseFind(StorageAPI* storage_api, StorageAPI_HIterator iterator)
    {
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)iterator;
        free(it_ptr);
    }
    static const char* GetFileName(StorageAPI* , StorageAPI_HIterator iterator)
    {
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)iterator;
        PathEntry* path_entry = *(*it_ptr).GetValue();
        if (path_entry == 0)
        {
            return 0;
        }
        if (path_entry->m_Content == 0)
        {
            return 0;
        }
        return path_entry->m_FileName;
    }
    static const char* GetDirectoryName(StorageAPI* , StorageAPI_HIterator iterator)
    {
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)iterator;
        PathEntry* path_entry = *(*it_ptr).GetValue();
        if (path_entry == 0)
        {
            return 0;
        }
        if (path_entry->m_Content != 0)
        {
            return 0;
        }
        return path_entry->m_FileName;
    }
};

int CreateParentPath(struct StorageAPI* storage_api, const char* path)
{
    char* dir_path = strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        free(dir_path);
        return 1;
    }
    else
    {
        if (!CreateParentPath(storage_api, dir_path))
        {
            TEST_LOG("CreateParentPath failed: `%s`\n", dir_path)
            free(dir_path);
            return 0;
        }
        if (storage_api->CreateDir(storage_api, dir_path))
        {
            free(dir_path);
            return 1;
        }
    }
    TEST_LOG("CreateParentPath failed: `%s`\n", dir_path)
    free(dir_path);
    return 0;
}


struct StoreCompressionAPI
{
    CompressionAPI m_CompressionAPI;

    StoreCompressionAPI()
        : m_CompressionAPI{
            GetDefaultSettings,
            GetMaxCompressionSetting,
            CreateCompressionContext,
            GetMaxCompressedSize,
            Compress,
            DeleteCompressionContext,
            CreateDecompressionContext,
            Decompress,
            DeleteDecompressionContext
            }
    {

    }

    static int DefaultCompressionSetting;
    static CompressionAPI_HSettings GetDefaultSettings(CompressionAPI*)
    {
        return (CompressionAPI_HSettings)&DefaultCompressionSetting;
    }
    static CompressionAPI_HSettings GetMaxCompressionSetting(CompressionAPI* compression_api)
    {
        return GetDefaultSettings(compression_api);
    }
    static CompressionAPI_HCompressionContext CreateCompressionContext(CompressionAPI*, CompressionAPI_HSettings settings)
    {
        return (CompressionAPI_HCompressionContext)settings;
    }
    static size_t GetMaxCompressedSize(CompressionAPI*, CompressionAPI_HCompressionContext , size_t size)
    {
        return size;
    }
    static size_t Compress(CompressionAPI*, CompressionAPI_HCompressionContext , const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size)
    {
        memmove(compressed, uncompressed, uncompressed_size);
        return uncompressed_size;
    }
    static void DeleteCompressionContext(CompressionAPI*, CompressionAPI_HCompressionContext)
    {
    }
    static CompressionAPI_HDecompressionContext CreateDecompressionContext(CompressionAPI* compression_api)
    {
        return (CompressionAPI_HDecompressionContext)GetDefaultSettings(compression_api);
    }
    static size_t Decompress(CompressionAPI*, CompressionAPI_HDecompressionContext, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size)
    {
        memmove(uncompressed, compressed, uncompressed_size);
        return uncompressed_size;
    }
    static void DeleteDecompressionContext(CompressionAPI*, CompressionAPI_HDecompressionContext)
    {
    }
};

int StoreCompressionAPI::DefaultCompressionSetting = 0;




static TLongtail_Hash GetContentTag(const char* , const char* path)
{
    const char * extension = strrchr(path, '.');
    if (extension)
    {
        MeowHashAPI hash;
        return InMemStorageAPI::GetPathHash(&hash.m_HashAPI, path);
    }
    return (TLongtail_Hash)-1;
}

TLongtail_Hash GetContentTagFake(const char* , const char* path)
{
    return 0u;
}

int MakePath(StorageAPI* storage_api, const char* path)
{
    char* dir_path = strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        return 1;
    }
    *last_path_delimiter = '\0';
    if (storage_api->IsDir(storage_api, dir_path))
    {
        free(dir_path);
        return 1;
    }
    else
    {
        if (!MakePath(storage_api, dir_path))
        {
            TEST_LOG("MakePath failed: `%s`\n", dir_path)
            free(dir_path);
            return 0;
        }
        if (storage_api->CreateDir(storage_api, dir_path))
        {
            free(dir_path);
            return 1;
        }
        if (storage_api->IsDir(storage_api, dir_path))
        {
            free(dir_path);
            return 1;
        }
        return 0;
    }
}

static int CreateFakeContent(StorageAPI* storage_api, const char* parent_path, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i)
    {
        char path[128];
        sprintf(path, "%s%s%u", parent_path ? parent_path : "", parent_path && parent_path[0] ? "/" : "", i);
        if (0 == MakePath(storage_api, path))
        {
            return 0;
        }
        StorageAPI_HOpenFile content_file = storage_api->OpenWriteFile(storage_api, path);
        if (!content_file)
        {
            return 0;
        }
        uint64_t content_size = 64000 + 1 + i;
        char* data = new char[content_size];
        memset(data, i, content_size);
        int ok = storage_api->Write(storage_api, content_file, 0, content_size, data);
        free(data);
        if (!ok)
        {
            return 0;
        }
        storage_api->CloseWrite(storage_api, content_file);
    }
    return 1;
}

TEST(Longtail, VersionIndex)
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
    const uint32_t asset_sizes[5] = {64003, 64003, 64002, 64001, 64001};
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};

    Paths* paths = MakePaths(5, asset_paths);
    size_t version_index_size = GetVersionIndexSize(5, 5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        *paths->m_PathCount,
        asset_sizes,
        asset_content_hashes);

    free(version_index);
    free(paths);
}

TEST(Longtail, ContentIndex)
{
    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = { 43593, 43593, 43592, 43591, 43591 };
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };
    MeowHashAPI hash_api;

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        asset_count,
        asset_content_hashes,
        asset_sizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);

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
    ASSERT_EQ(43593, content_index->m_ChunkBlockOffsets[1]);
    ASSERT_EQ(43593 * 2, content_index->m_ChunkBlockOffsets[2]);
    ASSERT_EQ(0u, content_index->m_ChunkBlockOffsets[3]);
    ASSERT_EQ(43591, content_index->m_ChunkBlockOffsets[4]);

    free(content_index);
}

TEST(Longtail, ContentIndexSerialization)
{
    InMemStorageAPI local_storage;
    MeowHashAPI hash_api;
    LizardCompressionAPI compression_api;
    ASSERT_EQ(1, CreateFakeContent(&local_storage.m_StorageAPI, "source/version1/two_items", 2));
    ASSERT_EQ(1, CreateFakeContent(&local_storage.m_StorageAPI, "source/version1/five_items", 5));
    Paths* version1_paths = GetFilesRecursively(&local_storage.m_StorageAPI, "source/version1");
    ASSERT_NE((Paths*)0, version1_paths);
    VersionIndex* vindex = CreateVersionIndex(
        &local_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        0,
        "source/version1",
        version1_paths,
        16384);
    ASSERT_NE((VersionIndex*)0, vindex);
    free(version1_paths);

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* cindex = CreateContentIndex(
        &hash_api.m_HashAPI,
        *vindex->m_ChunkCount,
        vindex->m_ContentHashes,
        vindex->m_ChunkSizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, cindex);

    free(vindex);
    vindex = 0;

    ASSERT_NE(0, WriteContentIndex(&local_storage.m_StorageAPI, cindex, "cindex.lci"));

    ContentIndex* cindex2 = ReadContentIndex(&local_storage.m_StorageAPI, "cindex.lci");
    ASSERT_NE((ContentIndex*)0, cindex2);

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

    free(cindex);
    cindex = 0;

    free(cindex2);
    cindex2 = 0;
}

TEST(Longtail, WriteContent)
{
    InMemStorageAPI source_storage;
    InMemStorageAPI target_storage;
    MeowHashAPI hash_api;
    LizardCompressionAPI compression_api;

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
        ASSERT_NE(0, CreateParentPath(&source_storage.m_StorageAPI, TEST_FILENAMES[i]));
        StorageAPI_HOpenFile w = source_storage.m_StorageAPI.OpenWriteFile(&source_storage.m_StorageAPI, TEST_FILENAMES[i]);
        ASSERT_NE((StorageAPI_HOpenFile)0, w);
        ASSERT_NE(0, source_storage.m_StorageAPI.Write(&source_storage.m_StorageAPI, w, 0, strlen(TEST_STRINGS[i]) + 1, TEST_STRINGS[i]));
        source_storage.m_StorageAPI.CloseWrite(&source_storage.m_StorageAPI, w);
        w = 0;
    }

    Paths* version1_paths = GetFilesRecursively(&source_storage.m_StorageAPI, "local");
    ASSERT_NE((Paths*)0, version1_paths);
    VersionIndex* vindex = CreateVersionIndex(
        &source_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        0,
        "local",
        version1_paths,
        16);
    ASSERT_NE((VersionIndex*)0, vindex);

    static const uint32_t MAX_BLOCK_SIZE = 32;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3;
    ContentIndex* cindex = CreateContentIndex(
        &hash_api.m_HashAPI,
        *vindex->m_ChunkCount,
        vindex->m_ChunkHashes,
        vindex->m_ChunkSizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, cindex);

    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
    ASSERT_NE((ChunkHashToAssetPart*)0, asset_part_lookup);
	ASSERT_NE(0, WriteContent(
        &source_storage.m_StorageAPI,
        &target_storage.m_StorageAPI,
        &compression_api.m_CompressionAPI,
        0,
        cindex,
        asset_part_lookup,
        "local",
        "chunks"));
    FreeAssetPartLookup(asset_part_lookup);
    asset_part_lookup = 0;

    ContentIndex* cindex2 = ReadContent(
        &target_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        "chunks");
    ASSERT_NE((ContentIndex*)0, cindex2);

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

    free(cindex2);
    free(cindex);
    free(vindex);
    free(version1_paths);
}

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
//    ContentIndex* GetBlocksForAssets(const ContentIndex* content_index, uint64_t asset_count, const TLongtail_Hash* asset_hashes, uint64_t* out_missing_asset_count, uint64_t* out_missing_assets)
}

TEST(Longtail, CreateMissingContent)
{
    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = {43593, 43593, 43592, 43591, 43591};
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};

    MeowHashAPI hash_api;

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        asset_count - 4,
        asset_content_hashes,
        asset_sizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);

    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    Paths* paths = MakePaths(5, asset_paths);
    size_t version_index_size = GetVersionIndexSize(5, 5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        *paths->m_PathCount,
        asset_sizes,
        asset_content_hashes);
    free(paths);

    ContentIndex* missing_content_index = CreateMissingContent(
        &hash_api.m_HashAPI,
        content_index,
        version_index,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);

    ASSERT_EQ(2u, *missing_content_index->m_BlockCount);
    ASSERT_EQ(4u, *missing_content_index->m_ChunkCount);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(asset_content_hashes[4], missing_content_index->m_ChunkHashes[0]);
    ASSERT_EQ(asset_sizes[4], missing_content_index->m_ChunkLengths[0]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[0]);
    ASSERT_EQ(asset_content_hashes[3], missing_content_index->m_ChunkHashes[1]);
    ASSERT_EQ(asset_sizes[3], missing_content_index->m_ChunkLengths[1]);
    ASSERT_EQ(43591, missing_content_index->m_ChunkBlockOffsets[1]);

    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockIndexes[2]);
    ASSERT_EQ(asset_content_hashes[2], missing_content_index->m_ChunkHashes[2]);
    ASSERT_EQ(asset_sizes[2], missing_content_index->m_ChunkLengths[2]);
    ASSERT_EQ(43591 * 2, missing_content_index->m_ChunkBlockOffsets[2]);

    ASSERT_EQ(1u, missing_content_index->m_ChunkBlockIndexes[3]);
    ASSERT_EQ(asset_content_hashes[1], missing_content_index->m_ChunkHashes[3]);
    ASSERT_EQ(asset_sizes[1], missing_content_index->m_ChunkLengths[3]);
    ASSERT_EQ(0u, missing_content_index->m_ChunkBlockOffsets[3]);

    free(version_index);
    free(content_index);

    free(missing_content_index);
}

TEST(Longtail, GetMissingAssets)
{
//    uint64_t GetMissingAssets(const ContentIndex* content_index, const VersionIndex* version, TLongtail_Hash* missing_assets)
}

TEST(Longtail, VersionIndexDirectories)
{
    InMemStorageAPI local_storage;
    MeowHashAPI hash_api;
    ASSERT_EQ(1, CreateFakeContent(&local_storage.m_StorageAPI, "two_items", 2));
    local_storage.m_StorageAPI.CreateDir(&local_storage.m_StorageAPI, "no_items");
    ASSERT_EQ(1, CreateFakeContent(&local_storage.m_StorageAPI, "deep/file/down/under/three_items", 3));
    ASSERT_EQ(1, MakePath(&local_storage.m_StorageAPI, "deep/folders/with/nothing/in/menoexists.nop"));

    Paths* local_paths = GetFilesRecursively(&local_storage.m_StorageAPI, "");
    ASSERT_NE((Paths*)0, local_paths);

    VersionIndex* local_version_index = CreateVersionIndex(
        &local_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        0,
        "",
        local_paths,
        16384);
    ASSERT_NE((VersionIndex*)0, local_version_index);
    ASSERT_EQ(16, *local_version_index->m_AssetCount);

    free(local_version_index);
    free(local_paths);
}

TEST(Longtail, MergeContentIndex)
{
    InMemStorageAPI local_storage;
    MeowHashAPI hash_api;

    CreateFakeContent(&local_storage.m_StorageAPI, 0, 5);

    InMemStorageAPI remote_storage;
    CreateFakeContent(&remote_storage.m_StorageAPI, 0, 10);

    Paths* local_paths = GetFilesRecursively(&local_storage.m_StorageAPI, "");
    ASSERT_NE((Paths*)0, local_paths);

    VersionIndex* local_version_index = CreateVersionIndex(
        &local_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        0,
        "",
        local_paths,
        16384);
    ASSERT_NE((VersionIndex*)0, local_version_index);
    ASSERT_EQ(5, *local_version_index->m_AssetCount);

    Paths* remote_paths = GetFilesRecursively(&remote_storage.m_StorageAPI, "");
    ASSERT_NE((Paths*)0, local_paths);
    VersionIndex* remote_version_index = CreateVersionIndex(
        &remote_storage.m_StorageAPI,
        &hash_api.m_HashAPI,
        0,
        "",
        remote_paths,
        16384);
    ASSERT_NE((VersionIndex*)0, remote_version_index);
    ASSERT_EQ(10, *remote_version_index->m_AssetCount);

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;

    ContentIndex* local_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            * local_version_index->m_ChunkCount,
            local_version_index->m_ChunkHashes,
            local_version_index->m_ChunkSizes,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK);

    StoreCompressionAPI store_compression;

    struct ChunkHashToAssetPart* local_asset_part_lookup = CreateAssetPartLookup(local_version_index);
    ASSERT_EQ(1, WriteContent(
        &local_storage.m_StorageAPI,
        &local_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        local_content_index,
        local_asset_part_lookup,
        "",
        ""));
    FreeAssetPartLookup(local_asset_part_lookup);
    local_asset_part_lookup = 0;

    ContentIndex* remote_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            * remote_version_index->m_ChunkCount,
            remote_version_index->m_ChunkHashes,
            remote_version_index->m_ChunkSizes,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK);

    struct ChunkHashToAssetPart* remote_asset_part_lookup = CreateAssetPartLookup(remote_version_index);
    ASSERT_EQ(1, WriteContent(
        &remote_storage.m_StorageAPI,
        &remote_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        remote_content_index,
        remote_asset_part_lookup,
        "",
        ""));

    ContentIndex* missing_content = CreateMissingContent(
        &hash_api.m_HashAPI,
        local_content_index,
        remote_version_index,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, missing_content);
    ASSERT_EQ(1, WriteContent(
        &remote_storage.m_StorageAPI,
        &local_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        missing_content,
        remote_asset_part_lookup,
        "",
        ""));
    FreeAssetPartLookup(remote_asset_part_lookup);
    remote_asset_part_lookup = 0;

    ContentIndex* merged_content_index = MergeContentIndex(local_content_index, missing_content);
    ASSERT_EQ(1, ReconstructVersion(
        &local_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        merged_content_index,
        remote_version_index,
        "",
        ""));

    for(uint32_t i = 0; i < 10; ++i)
    {
        char path[20];
        sprintf(path, "%u", i);
        StorageAPI_HOpenFile r = local_storage.m_StorageAPI.OpenReadFile(&local_storage.m_StorageAPI, path);
        ASSERT_NE((StorageAPI_HOpenFile)0, r);
        uint64_t size = local_storage.m_StorageAPI.GetSize(&local_storage.m_StorageAPI, r);
        uint64_t expected_size = 64000 + 1 + i;
        ASSERT_EQ(expected_size, size);
        char* buffer = (char*)malloc(expected_size);
        local_storage.m_StorageAPI.Read(&local_storage.m_StorageAPI, r, 0, expected_size, buffer);

        for (uint64_t j = 0; j < expected_size; j++)
        {
            ASSERT_EQ((int)i, (int)buffer[j]);
        }

        free(buffer);
        local_storage.m_StorageAPI.CloseRead(&local_storage.m_StorageAPI, r);
    }

    free(missing_content);
    free(merged_content_index);
    free(remote_content_index);
    free(local_content_index);
    free(remote_version_index);
    free(local_version_index);
}


TEST(Longtail, ReconstructVersion)
{
    InMemStorageAPI source_storage;
    StorageAPI* storage_api = &source_storage.m_StorageAPI;

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
        ASSERT_NE(0, CreateParentPath(storage_api, TEST_FILENAMES[i]));
        StorageAPI_HOpenFile w = storage_api->OpenWriteFile(storage_api, TEST_FILENAMES[i]);
        ASSERT_NE((StorageAPI_HOpenFile)0, w);
        ASSERT_NE(0, storage_api->Write(storage_api, w, 0, strlen(TEST_STRINGS[i]) + 1, TEST_STRINGS[i]));
        storage_api->CloseWrite(storage_api, w);
        w = 0;
    }

    MeowHashAPI hash_api;
    Paths* version1_paths = GetFilesRecursively(storage_api, "local");
    ASSERT_NE((Paths*)0, version1_paths);
    VersionIndex* vindex = CreateVersionIndex(
        storage_api,
        &hash_api.m_HashAPI,
        0,
        "local",
        version1_paths,
        16);
    ASSERT_NE((VersionIndex*)0, vindex);

    static const uint32_t MAX_BLOCK_SIZE = 32;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 3;
    ContentIndex* cindex = CreateContentIndex(
        &hash_api.m_HashAPI,
        *vindex->m_ChunkCount,
        vindex->m_ChunkHashes,
        vindex->m_ChunkSizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, cindex);

    StoreCompressionAPI store_compression;
    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
    ASSERT_NE((ChunkHashToAssetPart*)0, asset_part_lookup);
	ASSERT_NE(0, WriteContent(
        storage_api,
        storage_api,
        &store_compression.m_CompressionAPI,
        0,
        cindex,
        asset_part_lookup,
        "local",
        "chunks"));
    FreeAssetPartLookup(asset_part_lookup);
    asset_part_lookup = 0;

    ASSERT_NE(0, ReconstructVersion(
        storage_api,
        &store_compression.m_CompressionAPI,
        0,
        cindex,
        vindex,
        "chunks",
        "remote"));

    free(vindex);
    vindex = 0;
    free(cindex);
    cindex = 0;

#if 0
    const char* asset_paths[5] = {
        "first_",
        "second",
        "third_",
        "fourth",
        "fifth_"
    };

    MeowHashAPI hash_api;
    TLongtail_Hash asset_path_hashes[5];// = {GetPathHash("10"), GetPathHash("20"), GetPathHash("30"), GetPathHash("40"), GetPathHash("50")};
    TLongtail_Hash asset_content_hashes[5];// = { 1, 2, 3, 4, 5};
    const uint32_t asset_sizes[5] = {64003, 64003, 64002, 64001, 64001};
    const uint32_t asset_chunk_counts[5] = {1, 1, 1, 1, 1};
    const uint32_t asset_chunk_start_index[5] = {0, 1, 2, 3, 4};
    InMemStorageAPI source_storage;
    StorageAPI* storage_api = &source_storage.m_StorageAPI;
    for (uint32_t i = 0; i < 5; ++i)
    {
        asset_path_hashes[i] = GetPathHash(&hash_api.m_HashAPI, asset_paths[i]);
        char* path = storage_api->ConcatPath(storage_api, "source_path", asset_paths[i]);
        ASSERT_NE(0, MakePath(storage_api, path));
        StorageAPI_HOpenFile f = storage_api->OpenWriteFile(storage_api, path);
        ASSERT_NE((StorageAPI_HOpenFile)0, f);
        free(path);
        char* data = (char*)malloc(asset_sizes[i]);
        for (uint32_t d = 0; d < asset_sizes[i]; ++d)
        {
            data[d] = (char)(i + 1);
        }
        ASSERT_EQ(1, storage_api->Write(storage_api, f, 0, asset_sizes[i], data));
        storage_api->CloseWrite(storage_api, f);

        meow_state state;
        MeowBegin(&state, MeowDefaultSeed);
        MeowAbsorb(&state, (meow_umm)(asset_sizes[i]), (void*)data);
        uint64_t data_hash = MeowU64From(MeowEnd(&state, 0), 0);
        asset_content_hashes[i] = data_hash;
        free(data);
    }

    Paths* paths = MakePaths(5, asset_paths);
    size_t version_index_size = GetVersionIndexSize(5, 5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes,
        asset_chunk_start_index,
        asset_chunk_counts,
        *paths->m_PathCount,
        asset_sizes,
        asset_content_hashes);
    ASSERT_NE((VersionIndex*)0, version_index);
    free(paths);

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        *version_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        version_index->m_ChunkSizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, content_index);

    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version_index);
    ASSERT_NE((ChunkHashToAssetPart*)0, asset_part_lookup);
    LizardCompressionAPI compression_api;
    ASSERT_EQ(1, WriteContent(
        storage_api,
        storage_api,
        &compression_api.m_CompressionAPI,
        0,
        content_index,
        asset_part_lookup,
        "source_path",
        "local_content"));

    FreeAssetPartLookup(asset_part_lookup);
    asset_part_lookup = 0;

    ASSERT_EQ(1, ReconstructVersion(
        storage_api,
        &compression_api.m_CompressionAPI,
        0,
        content_index,
        version_index,
        "local_content",
        "target_path"));

    for (uint32_t i = 0; i < 5; ++i)
    {
        char* path = (char*)storage_api->ConcatPath(storage_api, "target_path", asset_paths[i]);
        asset_path_hashes[i] = GetPathHash(&hash_api.m_HashAPI, path);
        StorageAPI_HOpenFile f = storage_api->OpenReadFile(storage_api, path);
        ASSERT_NE((StorageAPI_HOpenFile)0, f);
        free(path);
        ASSERT_EQ(asset_sizes[i], storage_api->GetSize(storage_api, f));
        char* data = (char*)malloc(asset_sizes[i]);
        storage_api->Read(storage_api, f, 0, asset_sizes[i], data);
        for (uint32_t d = 0; d < asset_sizes[i]; ++d)
        {
            if ((char)(i + 1) != data[d])
            {
                ASSERT_EQ((char)(i + 1), data[d]);
            }
        }
        storage_api->CloseRead(storage_api, f);
        free(data);
    }
#endif // 0
}

void Bench()
{
    if (1) return;


#if 0
    #define HOME "D:\\Temp\\longtail"

    const uint32_t VERSION_COUNT = 6;
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

    const uint32_t VERSION_COUNT = 2;
    const char* VERSION[VERSION_COUNT] = {
        "git75a99408249875e875f8fba52b75ea0f5f12a00e_Win64_Editor",
        "gitb1d3adb4adce93d0f0aa27665a52be0ab0ee8b59_Win64_Editor"
    };
#endif

    const char* SOURCE_VERSION_PREFIX = HOME "\\local\\";
    const char* VERSION_INDEX_SUFFIX = ".lvi";
    const char* CONTENT_INDEX_SUFFIX = ".lci";

    const char* CONTENT_FOLDER = HOME "\\chunks";

    const char* UPLOAD_VERSION_PREFIX = HOME "\\upload\\";
    const char* UPLOAD_VERSION_SUFFIX = "_chunks";

    const char* TARGET_VERSION_PREFIX = HOME "\\remote\\";

    TroveStorageAPI storage_api;
    MeowHashAPI hash_api;
    BikeshedJobAPI job_api;

    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* full_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            0,
            0,
            0,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, full_content_index);
    VersionIndex* version_indexes[VERSION_COUNT];

    for (uint32_t i = 0; i < VERSION_COUNT; ++i)
    {
        char version_source_folder[256];
        sprintf(version_source_folder, "%s%s", SOURCE_VERSION_PREFIX, VERSION[i]);
        printf("Indexing `%s`\n", version_source_folder);
        Paths* version_source_paths = GetFilesRecursively(&storage_api.m_StorageAPI, version_source_folder);
        ASSERT_NE((Paths*)0, version_source_paths);
        VersionIndex* version_index = CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            version_source_folder,
            version_source_paths,
            16384);
        free(version_source_paths);
        ASSERT_NE((VersionIndex*)0, version_index);
        printf("Indexed %u assets from `%s`\n", (uint32_t)*version_index->m_AssetCount, version_source_folder);

        char version_index_file[256];
        sprintf(version_index_file, "%s%s%s", SOURCE_VERSION_PREFIX, VERSION[i], VERSION_INDEX_SUFFIX);
        ASSERT_NE(0, WriteVersionIndex(&storage_api.m_StorageAPI, version_index, version_index_file));
        printf("Wrote version index to `%s`\n", version_index_file);

        ContentIndex* missing_content_index = CreateMissingContent(
            &hash_api.m_HashAPI,
            full_content_index,
            version_index,
            MAX_BLOCK_SIZE,
            MAX_CHUNKS_PER_BLOCK);
        ASSERT_NE((ContentIndex*)0, missing_content_index);

        LizardCompressionAPI compression_api;
        struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version_index);
        char delta_upload_content_folder[256];
        sprintf(delta_upload_content_folder, "%s%s%s", UPLOAD_VERSION_PREFIX, VERSION[i], UPLOAD_VERSION_SUFFIX);
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder);
        ASSERT_NE(0, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
            missing_content_index,
            asset_part_lookup,
            version_source_folder,
            delta_upload_content_folder));
        FreeAssetPartLookup(asset_part_lookup);
        asset_part_lookup = 0;

        printf("Copying %" PRIu64 " blocks from `%s` to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder, CONTENT_FOLDER);
        for (uint64_t b = 0; b < *missing_content_index->m_BlockCount; ++b)
        {
            TLongtail_Hash block_hash = missing_content_index->m_BlockHashes[b];
            char* block_name = GetBlockName(block_hash);

            char source_path[256];
            sprintf(source_path, "%s/%s.lrb", delta_upload_content_folder, block_name);

            char target_path[256];
            sprintf(target_path, "%s/%s.lrb", CONTENT_FOLDER, block_name);

            free(block_name);

            StorageAPI_HOpenFile v = storage_api.m_StorageAPI.OpenReadFile(&storage_api.m_StorageAPI, target_path);
            if (v)
            {
                storage_api.m_StorageAPI.CloseRead(&storage_api.m_StorageAPI, v);
                v = 0;
                continue;
            }

            StorageAPI_HOpenFile s = storage_api.m_StorageAPI.OpenReadFile(&storage_api.m_StorageAPI, source_path);
            ASSERT_NE((StorageAPI_HOpenFile)0, s);

            ASSERT_NE(0, MakePath(&storage_api.m_StorageAPI, target_path));
            StorageAPI_HOpenFile t = storage_api.m_StorageAPI.OpenWriteFile(&storage_api.m_StorageAPI, target_path);
            ASSERT_NE((StorageAPI_HOpenFile)0, t);

            uint64_t block_file_size = storage_api.m_StorageAPI.GetSize(&storage_api.m_StorageAPI, s);
            void* buffer = malloc(block_file_size);

            ASSERT_NE(0, storage_api.m_StorageAPI.Read(&storage_api.m_StorageAPI, s, 0, block_file_size, buffer));
            ASSERT_NE(0, storage_api.m_StorageAPI.Write(&storage_api.m_StorageAPI, t, 0, block_file_size, buffer));

            storage_api.m_StorageAPI.CloseRead(&storage_api.m_StorageAPI, s);
            storage_api.m_StorageAPI.CloseWrite(&storage_api.m_StorageAPI, t);
        }

        ContentIndex* merged_content_index = MergeContentIndex(full_content_index, missing_content_index);
        ASSERT_NE((ContentIndex*)0, merged_content_index);
        free(missing_content_index);
        missing_content_index = 0;
        free(full_content_index);
        full_content_index = merged_content_index;
        merged_content_index = 0;

        char version_target_folder[256];
        sprintf(version_target_folder, "%s%s", TARGET_VERSION_PREFIX, VERSION[i]);
        printf("Reconstructing %" PRIu64 " assets from `%s` to `%s`\n", *version_index->m_AssetCount, CONTENT_FOLDER, version_target_folder);
        ASSERT_NE(0, ReconstructVersion(
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
            full_content_index,
            version_index,
            CONTENT_FOLDER,
            version_target_folder));

        version_indexes[i] = version_index;
        version_index = 0;
    }

    for (uint32_t i = 0; i < VERSION_COUNT; ++i)
    {
        free(version_indexes[i]);
    }

    free(full_content_index);

    #undef HOME
}

void LifelikeTest()
{
    if (1) return;

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
    const char* remote_content_index_path = HOME "\\remote.lci";

    const char* remote_path_1 = HOME "\\remote\\" VERSION1_FOLDER;
    const char* remote_path_2 = HOME "\\remote\\" VERSION2_FOLDER;

    printf("Indexing `%s`...\n", local_path_1);
    TroveStorageAPI storage_api;
    MeowHashAPI hash_api;
    LizardCompressionAPI compression_api;
    BikeshedJobAPI job_api;

    Paths* local_path_1_paths = GetFilesRecursively(&storage_api.m_StorageAPI, local_path_1);
    ASSERT_NE((Paths*)0, local_path_1_paths);
    VersionIndex* version1 = CreateVersionIndex(
        &storage_api.m_StorageAPI,
        &hash_api.m_HashAPI,
        &job_api.m_JobAPI,
        local_path_1,
        local_path_1_paths,
        16384);
    WriteVersionIndex(&storage_api.m_StorageAPI, version1, version_index_path_1);
    free(local_path_1_paths);
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version1->m_AssetCount, local_path_1, version_index_path_1);

    printf("Creating local content index...\n");
    static const uint32_t MAX_BLOCK_SIZE = 65536 * 2;
    static const uint32_t MAX_CHUNKS_PER_BLOCK = 4096;
    ContentIndex* local_content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        *version1->m_ChunkCount,
        version1->m_ChunkHashes,
        version1->m_ChunkSizes,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);

    printf("Writing local content index...\n");
    WriteContentIndex(&storage_api.m_StorageAPI, local_content_index, local_content_index_path);
    printf("%" PRIu64 " blocks from version `%s` indexed to `%s`\n", *local_content_index->m_BlockCount, local_path_1, local_content_index_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *local_content_index->m_BlockCount, local_content_path);
        struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version1);
        WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
            local_content_index,
            asset_part_lookup,
            local_path_1,
            local_content_path);

        FreeAssetPartLookup(asset_part_lookup);
        asset_part_lookup = 0;
    }

    printf("Reconstructing %" PRIu64 " assets to `%s`\n", *version1->m_AssetCount, remote_path_1);
    ASSERT_EQ(1, ReconstructVersion(&storage_api.m_StorageAPI, &compression_api.m_CompressionAPI, &job_api.m_JobAPI, local_content_index, version1, local_content_path, remote_path_1));
    printf("Reconstructed %" PRIu64 " assets to `%s`\n", *version1->m_AssetCount, remote_path_1);

    printf("Indexing `%s`...\n", local_path_2);
    Paths* local_path_2_paths = GetFilesRecursively(&storage_api.m_StorageAPI, local_path_2);
    ASSERT_NE((Paths*)0, local_path_2_paths);
    VersionIndex* version2 = CreateVersionIndex(
        &storage_api.m_StorageAPI,
        &hash_api.m_HashAPI,
        &job_api.m_JobAPI,
        local_path_2,
        local_path_2_paths,
        16384);
    free(local_path_2_paths);
    ASSERT_NE((VersionIndex*)0, version2);
    ASSERT_EQ(1, WriteVersionIndex(&storage_api.m_StorageAPI, version2, version_index_path_2));
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version2->m_AssetCount, local_path_2, version_index_path_2);
    
    // What is missing in local content that we need from remote version in new blocks with just the missing assets.
    ContentIndex* missing_content = CreateMissingContent(
        &hash_api.m_HashAPI,
        local_content_index,
        version2,
        MAX_BLOCK_SIZE,
        MAX_CHUNKS_PER_BLOCK);
    ASSERT_NE((ContentIndex*)0, missing_content);
    printf("%" PRIu64 " blocks for version `%s` needed in content index `%s`\n", *missing_content->m_BlockCount, local_path_1, local_content_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, local_content_path);
        struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version2);
        ASSERT_NE((ChunkHashToAssetPart*)0, asset_part_lookup);
        ASSERT_EQ(1, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
            missing_content,
            asset_part_lookup,
            local_path_2,
            local_content_path));

        FreeAssetPartLookup(asset_part_lookup);
        asset_part_lookup = 0;
    }

    if (1)
    {
        // Write this to disk for reference to see how big the diff is...
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, remote_content_path);
        struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(version2);
        ASSERT_NE((ChunkHashToAssetPart*)0, asset_part_lookup);
        ASSERT_EQ(1, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
            missing_content,
            asset_part_lookup,
            local_path_2,
            remote_content_path));

        FreeAssetPartLookup(asset_part_lookup);
        asset_part_lookup = 0;
    }

//    ContentIndex* remote_content_index = CreateContentIndex(
//        local_path_2,
//        *version2->m_AssetCount,
//        version2->m_ContentHashes,
//        version2->m_PathHashes,
//        version2->m_AssetSize,
//        version2->m_NameOffsets,
//        version2->m_NameData,
//        GetContentTag);

//    uint64_t* missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * *version2->m_AssetCount);
//    uint64_t missing_asset_count = GetMissingAssets(local_content_index, version2, missing_assets);
//
//    uint64_t* remaining_missing_assets = (uint64_t*)malloc(sizeof(uint64_t) * missing_asset_count);
//    uint64_t remaining_missing_asset_count = 0;
//    ContentIndex* existing_blocks = GetBlocksForAssets(remote_content_index, missing_asset_count, missing_assets, &remaining_missing_asset_count, remaining_missing_assets);
//    printf("%" PRIu64 " blocks for version `%s` available in content index `%s`\n", *existing_blocks->m_BlockCount, local_path_2, remote_content_path);

//    // Copy existing blocks
//    for (uint64_t block_index = 0; block_index < *missing_content->m_BlockCount; ++block_index)
//    {
//        TLongtail_Hash block_hash = missing_content->m_BlockHash[block_index];
//        char* block_name = GetBlockName(block_hash);
//        char block_file_name[64];
//        sprintf(block_file_name, "%s.lrb", block_name);
//        char* source_path = storage_api.ConcatPath(remote_content_path, block_file_name);
//        StorageAPI_HOpenFile s = storage_api.OpenReadFile(source_path);
//        char* target_path = storage_api.ConcatPath(local_content_path, block_file_name);
//        StorageAPI_HOpenFile t = storage_api.OpenWriteFile(target_path);
//        uint64_t size = storage_api.GetSize(s);
//        char* buffer = (char*)malloc(size);
//        storage_api.Read(s, 0, size, buffer);
//        storage_api.Write(t, 0, size, buffer);
//        free(buffer);
//        storage_api.CloseWrite(t);
//        storage_api.CloseRead(s);
//    }

    ContentIndex* merged_local_content = MergeContentIndex(local_content_index, missing_content);
    ASSERT_NE((ContentIndex*)0, merged_local_content);
    free(missing_content);
    missing_content = 0;
    free(local_content_index);
    local_content_index = 0;

    printf("Reconstructing %" PRIu64 " assets to `%s`\n", *version2->m_AssetCount, remote_path_2);
    ASSERT_EQ(1, ReconstructVersion(&storage_api.m_StorageAPI, &compression_api.m_CompressionAPI, &job_api.m_JobAPI, merged_local_content, version2, local_content_path, remote_path_2));
    printf("Reconstructed %" PRIu64 " assets to `%s`\n", *version2->m_AssetCount, remote_path_2);

//    free(existing_blocks);
//    existing_blocks = 0;
//    free(remaining_missing_assets);
//    remaining_missing_assets = 0;
//    free(missing_assets);
//    missing_assets = 0;
//    free(remote_content_index);
//    remote_content_index = 0;

    free(merged_local_content);
    merged_local_content = 0;

    free(version1);
    version1 = 0;

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

#if 0


static uint32_t hashTable[] = {
    0x458be752, 0xc10748cc, 0xfbbcdbb8, 0x6ded5b68,
    0xb10a82b5, 0x20d75648, 0xdfc5665f, 0xa8428801,
    0x7ebf5191, 0x841135c7, 0x65cc53b3, 0x280a597c,
    0x16f60255, 0xc78cbc3e, 0x294415f5, 0xb938d494,
    0xec85c4e6, 0xb7d33edc, 0xe549b544, 0xfdeda5aa,
    0x882bf287, 0x3116737c, 0x05569956, 0xe8cc1f68,
    0x0806ac5e, 0x22a14443, 0x15297e10, 0x50d090e7,
    0x4ba60f6f, 0xefd9f1a7, 0x5c5c885c, 0x82482f93,
    0x9bfd7c64, 0x0b3e7276, 0xf2688e77, 0x8fad8abc,
    0xb0509568, 0xf1ada29f, 0xa53efdfe, 0xcb2b1d00,
    0xf2a9e986, 0x6463432b, 0x95094051, 0x5a223ad2,
    0x9be8401b, 0x61e579cb, 0x1a556a14, 0x5840fdc2,
    0x9261ddf6, 0xcde002bb, 0x52432bb0, 0xbf17373e,
    0x7b7c222f, 0x2955ed16, 0x9f10ca59, 0xe840c4c9,
    0xccabd806, 0x14543f34, 0x1462417a, 0x0d4a1f9c,
    0x087ed925, 0xd7f8f24c, 0x7338c425, 0xcf86c8f5,
    0xb19165cd, 0x9891c393, 0x325384ac, 0x0308459d,
    0x86141d7e, 0xc922116a, 0xe2ffa6b6, 0x53f52aed,
    0x2cd86197, 0xf5b9f498, 0xbf319c8f, 0xe0411fae,
    0x977eb18c, 0xd8770976, 0x9833466a, 0xc674df7f,
    0x8c297d45, 0x8ca48d26, 0xc49ed8e2, 0x7344f874,
    0x556f79c7, 0x6b25eaed, 0xa03e2b42, 0xf68f66a4,
    0x8e8b09a2, 0xf2e0e62a, 0x0d3a9806, 0x9729e493,
    0x8c72b0fc, 0x160b94f6, 0x450e4d3d, 0x7a320e85,
    0xbef8f0e1, 0x21d73653, 0x4e3d977a, 0x1e7b3929,
    0x1cc6c719, 0xbe478d53, 0x8d752809, 0xe6d8c2c6,
    0x275f0892, 0xc8acc273, 0x4cc21580, 0xecc4a617,
    0xf5f7be70, 0xe795248a, 0x375a2fe9, 0x425570b6,
    0x8898dcf8, 0xdc2d97c4, 0x0106114b, 0x364dc22f,
    0x1e0cad1f, 0xbe63803c, 0x5f69fac2, 0x4d5afa6f,
    0x1bc0dfb5, 0xfb273589, 0x0ea47f7b, 0x3c1c2b50,
    0x21b2a932, 0x6b1223fd, 0x2fe706a8, 0xf9bd6ce2,
    0xa268e64e, 0xe987f486, 0x3eacf563, 0x1ca2018c,
    0x65e18228, 0x2207360a, 0x57cf1715, 0x34c37d2b,
    0x1f8f3cde, 0x93b657cf, 0x31a019fd, 0xe69eb729,
    0x8bca7b9b, 0x4c9d5bed, 0x277ebeaf, 0xe0d8f8ae,
    0xd150821c, 0x31381871, 0xafc3f1b0, 0x927db328,
    0xe95effac, 0x305a47bd, 0x426ba35b, 0x1233af3f,
    0x686a5b83, 0x50e072e5, 0xd9d3bb2a, 0x8befc475,
    0x487f0de6, 0xc88dff89, 0xbd664d5e, 0x971b5d18,
    0x63b14847, 0xd7d3c1ce, 0x7f583cf3, 0x72cbcb09,
    0xc0d0a81c, 0x7fa3429b, 0xe9158a1b, 0x225ea19a,
    0xd8ca9ea3, 0xc763b282, 0xbb0c6341, 0x020b8293,
    0xd4cd299d, 0x58cfa7f8, 0x91b4ee53, 0x37e4d140,
    0x95ec764c, 0x30f76b06, 0x5ee68d24, 0x679c8661,
    0xa41979c2, 0xf2b61284, 0x4fac1475, 0x0adb49f9,
    0x19727a23, 0x15a7e374, 0xc43a18d5, 0x3fb1aa73,
    0x342fc615, 0x924c0793, 0xbee2d7f0, 0x8a279de9,
    0x4aa2d70c, 0xe24dd37f, 0xbe862c0b, 0x177c22c2,
    0x5388e5ee, 0xcd8a7510, 0xf901b4fd, 0xdbc13dbc,
    0x6c0bae5b, 0x64efe8c7, 0x48b02079, 0x80331a49,
    0xca3d8ae6, 0xf3546190, 0xfed7108b, 0xc49b941b,
    0x32baf4a9, 0xeb833a4a, 0x88a3f1a5, 0x3a91ce0a,
    0x3cc27da1, 0x7112e684, 0x4a3096b1, 0x3794574c,
    0xa3c8b6f3, 0x1d213941, 0x6e0a2e00, 0x233479f1,
    0x0f4cd82f, 0x6093edd2, 0x5d7d209e, 0x464fe319,
    0xd4dcac9e, 0x0db845cb, 0xfb5e4bc3, 0xe0256ce1,
    0x09fb4ed1, 0x0914be1e, 0xa5bdb2c3, 0xc6eb57bb,
    0x30320350, 0x3f397e91, 0xa67791bc, 0x86bc0e2c,
    0xefa0a7e2, 0xe9ff7543, 0xe733612c, 0xd185897b,
    0x329e5388, 0x91dd236b, 0x2ecb0d93, 0xf4d82a3d,
    0x35b5c03f, 0xe4e606f0, 0x05b21843, 0x37b45964,
    0x5eff22f4, 0x6027f4cc, 0x77178b3c, 0xae507131,
    0x7bf7cabc, 0xf9c18d66, 0x593ade65, 0xd95ddf11,
};

typedef struct VersionFeeder_Context* VersionFeeder_HContext;

struct VersionFeeder
{
     VersionFeeder_HContext CreateContext(
         struct VersionIndex* version,
         struct StorageAPI* storage_api,
         const char* asset_path);
typedef int (*FeedFunc)(void* context, uint8_t* buffer, uint32_t* size);
};

typedef int (*FeedFunc)(void* context, uint8_t* buffer, uint32_t* size);

int FakeFeeder(void* context, uint8_t* buffer, uint32_t* size)
{
    return 1;
}

struct VersionDataSerializer
{
    struct VersionIndex* m_VersionIndex;
    struct StorageAPI* m_StorageAPI;
    const char* m_RootPath;
    uint64_t m_AssetIndex;
    uint32_t m_AssetOffset;
    StorageAPI_HOpenFile m_OpenFile;
};

static int IsDirPath(const char* path)
{
    return path[0] ? path[strlen(path) - 1] == '/' : 0;
}

int VersionDataSerializerFeed(
    VersionDataSerializer* serializer,
    uint8_t* buffer,
    uint32_t* size)
{
    uint32_t size_left = *size;
    uint32_t size_written = 0;
    while (size_left)
    {
        uint64_t left_of_asset = 0;
        if (serializer->m_OpenFile)
        {
            uint64_t asset_size = serializer->m_StorageAPI->GetSize(serializer->m_StorageAPI, serializer->m_OpenFile);
            left_of_asset = asset_size - serializer->m_AssetOffset;
            if (left_of_asset == 0)
            {
                serializer->m_StorageAPI->CloseRead(serializer->m_StorageAPI, serializer->m_OpenFile);
                serializer->m_OpenFile = 0;
                ++serializer->m_AssetIndex;
            }
        }
        if (left_of_asset == 0)
        {
            while (serializer->m_AssetIndex < *serializer->m_VersionIndex->m_AssetCount)
            {
                const char* asset_path = &serializer->m_VersionIndex->m_NameData[serializer->m_VersionIndex->m_NameOffsets[serializer->m_AssetIndex]];
                if (!IsDirPath(asset_path))
                {
                    break;
                }
            }
            if (serializer->m_AssetIndex < *serializer->m_VersionIndex->m_AssetCount)
            {
                const char* asset_path = &serializer->m_VersionIndex->m_NameData[serializer->m_VersionIndex->m_NameOffsets[serializer->m_AssetIndex]];
                char* full_path = serializer->m_StorageAPI->ConcatPath(serializer->m_StorageAPI, serializer->m_RootPath, asset_path);
                serializer->m_OpenFile = serializer->m_OpenFile = serializer->m_StorageAPI->OpenReadFile(serializer->m_StorageAPI, full_path);
                if (!serializer->m_OpenFile)
                {
                    TEST_LOG("Failed to open `%s`\n", full_path);
                    free(full_path);
                    return 0;
                }
                free(full_path);
            }
            else
            {
                break;
            }
        }

        if (left_of_asset)
        {
            uint64_t to_read = left_of_asset < size_left ? left_of_asset : size_left;
            int ok = serializer->m_StorageAPI->Read(serializer->m_StorageAPI, serializer->m_OpenFile, serializer->m_AssetOffset, to_read, &buffer[size_written]);
            if (!ok)
            {
                serializer->m_StorageAPI->CloseRead(serializer->m_StorageAPI, serializer->m_OpenFile);
                serializer->m_OpenFile = 0;
                return 0;
            }
            serializer->m_AssetOffset += to_read;
            size_left -= to_read;
            size_written += to_read;
        }
    }
    *size = size_written;
    return 1;
}

struct VersionDataSerializer* CreateVersionDataSerializer(
    struct VersionIndex* version,
    struct StorageAPI* storage_api,
    const char* asset_path)
{
    struct VersionDataSerializer* vds = (struct VersionDataSerializer*)malloc(sizeof(struct VersionDataSerializer));
    vds->m_VersionIndex = version;
    vds->m_StorageAPI = storage_api,
    vds->m_RootPath = asset_path;
    vds->m_AssetIndex = 0;
    vds->m_AssetOffset = 0;
    vds->m_OpenFile = 0;
    return vds;
}

// Lest only try to chunk large files, not small files!

struct Chunker
{
    FeedFunc m_FeedFunction;
};

TEST(Longtail, TestFeeder)
{
    static const uint32_t CHUNK_SIZE = 16384;
    uint8_t buffer[CHUNK_SIZE];
    uint32_t size = CHUNK_SIZE;
    FeedFunc feed_func = FakeFeeder;
    void* feed_func_context = 0;
    while (feed_func(feed_func_context, buffer, &size))
    {
        size = CHUNK_SIZE;
    }
}

#endif