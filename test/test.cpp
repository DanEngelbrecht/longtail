#include "../third-party/jctest/src/jc_test.h"

#define BIKESHED_IMPLEMENTATION
#include "../third-party/bikeshed/bikeshed.h"

#include "../common/platform.h"
#include "../src/longtail.h"
#include "../src/longtail_array.h"







//////////////

// TODO: Move to longtail.h



uint64_t GetMissingAssets(const ContentIndex* content_index, const VersionIndex* version, TLongtail_Hash* missing_assets)
{
    uint32_t missing_hash_count = 0;
    DiffHashes(content_index->m_AssetContentHash, *content_index->m_AssetCount, version->m_AssetContentHash, *version->m_AssetCount, &missing_hash_count, missing_assets, 0, 0);
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
            TLongtail_Hash asset_content_hash = content_index->m_AssetContentHash[i];
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
        uint64_t prev_block_index = content_index->m_AssetBlockIndex[i++];
        asset_start_in_blocks[prev_block_index] = i;
        uint32_t assets_count_in_block = 1;
        while (i < *content_index->m_AssetCount && prev_block_index == content_index->m_AssetBlockIndex[i])
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
        uint64_t block_index = content_index->m_AssetBlockIndex[asset_index];
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
            existing_content_index->m_AssetContentHash[target_asset_index] = content_index->m_AssetBlockOffset[source_asset_index];
            existing_content_index->m_AssetBlockIndex[target_asset_index] = block_index;
            existing_content_index->m_AssetBlockOffset[target_asset_index] = content_index->m_AssetBlockOffset[source_asset_index];
            existing_content_index->m_AssetLength[target_asset_index] = content_index->m_AssetLength[source_asset_index];
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
            Free_TContent(path_entry->m_Content);
            free(path_entry);
            ++it;
        }
        free(m_PathHashToContentMem);
    }

    static StorageAPI::HOpenFile OpenReadFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, path);
        PathEntry** it = instance->m_PathHashToContent.Get(path_hash);
        if (it)
        {
            return (StorageAPI::HOpenFile)*it;
        }
        return 0;
    }
    static uint64_t GetSize(StorageAPI* storage_api, StorageAPI::HOpenFile f)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        PathEntry* path_entry = (PathEntry*)f;
        return GetSize_TContent(path_entry->m_Content);
    }
    static int Read(StorageAPI* storage_api, StorageAPI::HOpenFile f, uint64_t offset, uint64_t length, void* output)
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
    static void CloseRead(StorageAPI* , StorageAPI::HOpenFile)
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

    static StorageAPI::HOpenFile OpenWriteFile(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash != 0 && !instance->m_PathHashToContent.Get(parent_path_hash))
        {
            LONGTAIL_LOG("InMemStorageAPI::OpenWriteFile `%s` failed - parent folder does not exist\n", path)
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
        return (StorageAPI::HOpenFile)path_hash;
    }
    static int Write(StorageAPI* storage_api, StorageAPI::HOpenFile f, uint64_t offset, uint64_t length, const void* input)
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
    static void CloseWrite(StorageAPI* , StorageAPI::HOpenFile)
    {
    }

    static int CreateDir(StorageAPI* storage_api, const char* path)
    {
        InMemStorageAPI* instance = (InMemStorageAPI*)storage_api;
        TLongtail_Hash parent_path_hash = GetParentPathHash(instance, path);
        if (parent_path_hash && !instance->m_PathHashToContent.Get(parent_path_hash))
        {
            LONGTAIL_LOG("InMemStorageAPI::CreateDir `%s` failed - parent folder does not exist\n", path)
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
            LONGTAIL_LOG("InMemStorageAPI::CreateDir `%s` failed - path exists and is not a directory\n", path)
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
            LONGTAIL_LOG("InMemStorageAPI::RenameFile from `%s` to `%s` failed - source path does not exist\n", source_path, target_path)
            return 0;
        }
        TLongtail_Hash target_path_hash = GetPathHash(&instance->m_HashAPI.m_HashAPI, target_path);
        PathEntry** target_path_ptr = instance->m_PathHashToContent.Get(target_path_hash);
        if (target_path_ptr)
        {
            LONGTAIL_LOG("InMemStorageAPI::RenameFile from `%s` to `%s` failed - target path does not exist\n", source_path, target_path)
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

    static StorageAPI::HIterator StartFind(StorageAPI* storage_api, const char* path)
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
                return (StorageAPI::HIterator)it_ptr;
            }
            ++(*it_ptr);
        }
        free(it_ptr);
        return (StorageAPI::HIterator)0;
    }
    static int FindNext(StorageAPI* storage_api, StorageAPI::HIterator iterator)
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
    static void CloseFind(StorageAPI* storage_api, StorageAPI::HIterator iterator)
    {
        jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator* it_ptr = (jc::HashTable<TLongtail_Hash, PathEntry*>::Iterator*)iterator;
        free(it_ptr);
    }
    static const char* GetFileName(StorageAPI* , StorageAPI::HIterator iterator)
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
    static const char* GetDirectoryName(StorageAPI* , StorageAPI::HIterator iterator)
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
    static CompressionAPI::HSettings GetDefaultSettings(CompressionAPI*)
    {
        return (CompressionAPI::HSettings)&DefaultCompressionSetting;
    }
    static CompressionAPI::HSettings GetMaxCompressionSetting(CompressionAPI* compression_api)
    {
        return GetDefaultSettings(compression_api);
    }
    static CompressionAPI::HCompressionContext CreateCompressionContext(CompressionAPI*, CompressionAPI::HSettings settings)
    {
        return (CompressionAPI::HCompressionContext)settings;
    }
    static size_t GetMaxCompressedSize(CompressionAPI*, CompressionAPI::HCompressionContext , size_t size)
    {
        return size;
    }
    static size_t Compress(CompressionAPI*, CompressionAPI::HCompressionContext , const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size)
    {
        memmove(compressed, uncompressed, uncompressed_size);
        return uncompressed_size;
    }
    static void DeleteCompressionContext(CompressionAPI*, CompressionAPI::HCompressionContext)
    {
    }
    static CompressionAPI::HDecompressionContext CreateDecompressionContext(CompressionAPI* compression_api)
    {
        return (CompressionAPI::HDecompressionContext)GetDefaultSettings(compression_api);
    }
    static size_t Decompress(CompressionAPI*, CompressionAPI::HDecompressionContext, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size)
    {
        memmove(uncompressed, compressed, uncompressed_size);
        return uncompressed_size;
    }
    static void DeleteDecompressionContext(CompressionAPI*, CompressionAPI::HDecompressionContext)
    {
    }
};

int StoreCompressionAPI::DefaultCompressionSetting = 0;


Paths* MakePaths(uint32_t path_count, const char* const* path_names)
{
    uint32_t name_data_size = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        name_data_size += (uint32_t)strlen(path_names[i]) + 1;
    }
    Paths* paths = CreatePaths(path_count, name_data_size);
    uint32_t offset = 0;
    for (uint32_t i = 0; i < path_count; ++i)
    {
        uint32_t length = (uint32_t)strlen(path_names[i]) + 1;
        paths->m_Offsets[i] = offset;
        memmove(&paths->m_Data[offset], path_names[i], length);
        offset += length;
    }
    paths->m_DataSize = offset;
    *paths->m_PathCount = path_count;
    return paths;
}

static TLongtail_Hash GetContentTag(const char* , const char* path)
{
    const char * extension = strrchr(path, '.');
    if (extension)
    {
        MeowHashAPI hash;
        return GetPathHash(&hash.m_HashAPI, path);
    }
    return (TLongtail_Hash)-1;
}

TLongtail_Hash GetContentTagFake(const char* , const char* path)
{
    return 0u;
}

static int CreateFakeContent(StorageAPI* storage_api, const char* parent_path, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i)
    {
        char path[128];
        sprintf(path, "%s%s%u", parent_path ? parent_path : "", parent_path && parent_path[0] ? "/" : "", i);
        if (0 == EnsureParentPathExists(storage_api, path))
        {
            return 0;
        }
        StorageAPI::HOpenFile content_file = storage_api->OpenWriteFile(storage_api, path);
        if (!content_file)
        {
            return 0;
        }
        uint64_t content_size = 32000 + 1 + i;
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
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};

    Paths* paths = MakePaths(5, asset_paths);
    size_t version_index_size = GetVersionIndexSize(5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);

    free(version_index);
    free(paths);
}

TEST(Longtail, ContentIndex)
{
    const char* assets_path = "";
    const uint64_t asset_count = 5;
    const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
    const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

    MeowHashAPI hash_api;

    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        assets_path,
        asset_count,
        asset_content_hashes,
        asset_path_hashes,
        asset_sizes,
        asset_name_offsets,
        asset_name_data,
        GetContentTagFake);

    ASSERT_EQ(5u, *content_index->m_AssetCount);
    ASSERT_EQ(2u, *content_index->m_BlockCount);
    for (uint32_t i = 0; i < *content_index->m_AssetCount; ++i)
    {
        ASSERT_EQ(asset_content_hashes[4 - i], content_index->m_AssetContentHash[i]);
        ASSERT_EQ(asset_sizes[4 - i], content_index->m_AssetLength[i]);
    }
    ASSERT_EQ(0u, content_index->m_AssetBlockIndex[0]);
    ASSERT_EQ(0u, content_index->m_AssetBlockIndex[1]);
    ASSERT_EQ(0u, content_index->m_AssetBlockIndex[2]);
    ASSERT_EQ(1u, content_index->m_AssetBlockIndex[3]);
    ASSERT_EQ(1u, content_index->m_AssetBlockIndex[4]);

    ASSERT_EQ(0u, content_index->m_AssetBlockOffset[0]);
    ASSERT_EQ(32001u, content_index->m_AssetBlockOffset[1]);
    ASSERT_EQ(64002u, content_index->m_AssetBlockOffset[2]);
    ASSERT_EQ(0u, content_index->m_AssetBlockOffset[3]);
    ASSERT_EQ(32003u, content_index->m_AssetBlockOffset[4]);

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
        version1_paths);
    ASSERT_NE((VersionIndex*)0, vindex);

    ContentIndex* cindex = CreateContentIndex(
        &hash_api.m_HashAPI,
        "source/version1",
        *vindex->m_AssetCount,
        vindex->m_AssetContentHash,
        vindex->m_PathHash,
        vindex->m_AssetSize,
        vindex->m_NameOffset,
        vindex->m_NameData,
        GetContentTag);
    ASSERT_NE((ContentIndex*)0, cindex);

    PathLookup* path_lookup = CreateContentHashToPathLookup(vindex, 0);
    ASSERT_NE((PathLookup*)0, path_lookup);
    int ok = WriteContent(
        &local_storage.m_StorageAPI,
        &local_storage.m_StorageAPI,
        &compression_api.m_CompressionAPI,
        0,
        cindex,
        path_lookup,
        "source/version1",
        "chunks");

	ContentIndex* cindex2 = ReadContent(
		&local_storage.m_StorageAPI,
		&hash_api.m_HashAPI,
		"chunks");
	ASSERT_NE((ContentIndex*)0, cindex2);
	ASSERT_EQ(*cindex->m_AssetCount, *cindex2->m_AssetCount);
	ASSERT_EQ(*cindex->m_BlockCount, *cindex2->m_BlockCount);

	free(cindex2);
    free(path_lookup);
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
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
    const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
    const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

    MeowHashAPI hash_api;

    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        assets_path,
        asset_count - 4,
        asset_content_hashes,
        asset_path_hashes,
        asset_sizes,
        asset_name_offsets,
        asset_name_data,
        GetContentTagFake);

    const char* asset_paths[5] = {
        "fifth_",
        "fourth",
        "third_",
        "second",
        "first_"
    };

    Paths* paths = MakePaths(5, asset_paths);
    size_t version_index_size = GetVersionIndexSize(5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);
    free(paths);

    ContentIndex* missing_content_index = CreateMissingContent(
        &hash_api.m_HashAPI,
        content_index,
        "",
        version_index,
        GetContentTagFake);

    ASSERT_EQ(2u, *missing_content_index->m_BlockCount);
    ASSERT_EQ(4u, *missing_content_index->m_AssetCount);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[0]);
    ASSERT_EQ(asset_content_hashes[4], missing_content_index->m_AssetContentHash[0]);
    ASSERT_EQ(asset_sizes[4], missing_content_index->m_AssetLength[0]);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[0]);
    ASSERT_EQ(asset_content_hashes[3], missing_content_index->m_AssetContentHash[1]);
    ASSERT_EQ(asset_sizes[3], missing_content_index->m_AssetLength[1]);
    ASSERT_EQ(32001u, missing_content_index->m_AssetBlockOffset[1]);

    ASSERT_EQ(0u, missing_content_index->m_AssetBlockIndex[2]);
    ASSERT_EQ(asset_content_hashes[2], missing_content_index->m_AssetContentHash[2]);
    ASSERT_EQ(asset_sizes[2], missing_content_index->m_AssetLength[2]);
    ASSERT_EQ(64002u, missing_content_index->m_AssetBlockOffset[2]);

    ASSERT_EQ(1u, missing_content_index->m_AssetBlockIndex[3]);
    ASSERT_EQ(asset_content_hashes[1], missing_content_index->m_AssetContentHash[3]);
    ASSERT_EQ(asset_sizes[1], missing_content_index->m_AssetLength[3]);
    ASSERT_EQ(0u, missing_content_index->m_AssetBlockOffset[3]);

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
    ASSERT_EQ(1, CreateFakeContent(&local_storage.m_StorageAPI, "deep/file/down/under/three_items", 3));    // TODO: Bad whitespace!?
    ASSERT_EQ(1, EnsureParentPathExists(&local_storage.m_StorageAPI, "deep/folders/with/nothing/in/menoexists.nop"));

    Paths* local_paths = GetFilesRecursively(&local_storage.m_StorageAPI, "");
    ASSERT_NE((Paths*)0, local_paths);

    VersionIndex* local_version_index = CreateVersionIndex(&local_storage.m_StorageAPI, &hash_api.m_HashAPI, 0, "", local_paths);
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

    VersionIndex* local_version_index = CreateVersionIndex(&local_storage.m_StorageAPI, &hash_api.m_HashAPI, 0, "", local_paths);
    ASSERT_NE((VersionIndex*)0, local_version_index);
    ASSERT_EQ(5, *local_version_index->m_AssetCount);

    Paths* remote_paths = GetFilesRecursively(&remote_storage.m_StorageAPI, "");
    ASSERT_NE((Paths*)0, local_paths);
    VersionIndex* remote_version_index = CreateVersionIndex(&remote_storage.m_StorageAPI, &hash_api.m_HashAPI, 0, "", remote_paths);
    ASSERT_NE((VersionIndex*)0, remote_version_index);
    ASSERT_EQ(10, *remote_version_index->m_AssetCount);

    ContentIndex* local_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            "",
            * local_version_index->m_AssetCount,
            local_version_index->m_AssetContentHash,
            local_version_index->m_PathHash,
            local_version_index->m_AssetSize,
            local_version_index->m_NameOffset,
            local_version_index->m_NameData,
            GetContentTagFake);

    StoreCompressionAPI store_compression;

    PathLookup* local_path_lookup = CreateContentHashToPathLookup(local_version_index, 0);
    ASSERT_EQ(1, WriteContent(
        &local_storage.m_StorageAPI,
        &local_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        local_content_index,
        local_path_lookup,
        "",
        ""));

    ContentIndex* remote_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            "",
            * remote_version_index->m_AssetCount,
            remote_version_index->m_AssetContentHash,
            remote_version_index->m_PathHash,
            remote_version_index->m_AssetSize,
            remote_version_index->m_NameOffset,
            remote_version_index->m_NameData,
            GetContentTagFake);

    PathLookup* remote_path_lookup = CreateContentHashToPathLookup(remote_version_index, 0);
    ASSERT_EQ(1, WriteContent(
        &remote_storage.m_StorageAPI,
        &remote_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        remote_content_index,
        remote_path_lookup,
        "",
        ""));

    ContentIndex* missing_content = CreateMissingContent(
        &hash_api.m_HashAPI,
        local_content_index,
        "",
        remote_version_index,
        GetContentTagFake);
    ASSERT_NE((ContentIndex*)0, missing_content);
    ASSERT_EQ(1, WriteContent(
        &remote_storage.m_StorageAPI,
        &local_storage.m_StorageAPI,
        &store_compression.m_CompressionAPI,
        0,
        missing_content,
        remote_path_lookup,
        "",
        ""));

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
        StorageAPI::HOpenFile r = local_storage.m_StorageAPI.OpenReadFile(&local_storage.m_StorageAPI, path);
        ASSERT_NE((StorageAPI::HOpenFile)0, r);
        uint64_t size = local_storage.m_StorageAPI.GetSize(&local_storage.m_StorageAPI, r);
        uint64_t expected_size = 32000 + 1 + i;
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
/*    ContentIndex* content_index = 0;
    {
        const char* assets_path = "";
        const uint64_t asset_count = 5;
        const TLongtail_Hash asset_content_hashes[5] = { 5, 4, 3, 2, 1};
        const TLongtail_Hash asset_path_hashes[5] = {50, 40, 30, 20, 10};
        const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
        const uint32_t asset_name_offsets[5] = { 7 * 0, 7 * 1, 7 * 2, 7 * 3, 7 * 4};
        const char* asset_name_data = { "fifth_\0" "fourth\0" "third_\0" "second\0" "first_\0" };

        content_index = CreateContentIndex(
            assets_path,
            asset_count,
            asset_content_hashes,
            asset_path_hashes,
            asset_sizes,
            asset_name_offsets,
            asset_name_data,
            GetContentTagFake);
    }*/

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
    const uint32_t asset_sizes[5] = {32003, 32003, 32002, 32001, 32001};
    InMemStorageAPI source_storage;
    StorageAPI* storage_api = &source_storage.m_StorageAPI;
    for (uint32_t i = 0; i < 5; ++i)
    {
        asset_path_hashes[i] = GetPathHash(&hash_api.m_HashAPI, asset_paths[i]);
        char* path = storage_api->ConcatPath(storage_api, "source_path", asset_paths[i]);
        ASSERT_NE(0, EnsureParentPathExists(storage_api, path));
        StorageAPI::HOpenFile f = storage_api->OpenWriteFile(storage_api, path);
        ASSERT_NE((StorageAPI::HOpenFile)0, f);
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
    size_t version_index_size = GetVersionIndexSize(5, paths->m_DataSize);
    void* version_index_mem = malloc(version_index_size);

    VersionIndex* version_index = BuildVersionIndex(
        version_index_mem,
        version_index_size,
        paths,
        asset_path_hashes,
        asset_content_hashes,
        asset_sizes);
    ASSERT_NE((VersionIndex*)0, version_index);
    free(paths);

    PathLookup* path_lookup = CreateContentHashToPathLookup(version_index, 0);
    ASSERT_NE((PathLookup*)0, path_lookup);

    ContentIndex* content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        "source_path",
        *version_index->m_AssetCount,
        version_index->m_AssetContentHash,
        version_index->m_PathHash,
        version_index->m_AssetSize,
        version_index->m_NameOffset,
        version_index->m_NameData,
        GetContentTagFake);
    ASSERT_NE((ContentIndex*)0, content_index);

    LizardCompressionAPI compression_api;
    ASSERT_EQ(1, WriteContent(
        storage_api,
        storage_api,
        &compression_api.m_CompressionAPI,
        0,
        content_index,
        path_lookup,
        "source_path",
        "local_content"));

    free(path_lookup);

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
        StorageAPI::HOpenFile f = storage_api->OpenReadFile(storage_api, path);
        ASSERT_NE((StorageAPI::HOpenFile)0, f);
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

    Shed shed;
    TroveStorageAPI storage_api;
    MeowHashAPI hash_api;

    ContentIndex* full_content_index = CreateContentIndex(
            &hash_api.m_HashAPI,
            "",
            0,
            0,
            0,
            0,
            0,
            0,
            0);
    ASSERT_NE((ContentIndex*)0, full_content_index);
    VersionIndex* version_indexes[VERSION_COUNT];

    for (uint32_t i = 0; i < VERSION_COUNT; ++i)
    {
        char version_source_folder[256];
        sprintf(version_source_folder, "%s%s", SOURCE_VERSION_PREFIX, VERSION[i]);
        printf("Indexing `%s`\n", version_source_folder);
        Paths* version_source_paths = GetFilesRecursively(&storage_api.m_StorageAPI, version_source_folder);
        ASSERT_NE((Paths*)0, version_source_paths);
        VersionIndex* version_index = CreateVersionIndex(&storage_api.m_StorageAPI, &hash_api.m_HashAPI, shed.m_Shed, version_source_folder, version_source_paths);
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
            version_source_folder,
            version_index,
            GetContentTag);
        ASSERT_NE((ContentIndex*)0, missing_content_index);

        LizardCompressionAPI compression_api;
        PathLookup* path_lookup = CreateContentHashToPathLookup(version_index, 0);
        char delta_upload_content_folder[256];
        sprintf(delta_upload_content_folder, "%s%s%s", UPLOAD_VERSION_PREFIX, VERSION[i], UPLOAD_VERSION_SUFFIX);
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder);
        ASSERT_NE(0, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            missing_content_index,
            path_lookup,
            version_source_folder,
            delta_upload_content_folder));

        printf("Copying %" PRIu64 " blocks from `%s` to `%s`\n", *missing_content_index->m_BlockCount, delta_upload_content_folder, CONTENT_FOLDER);
        for (uint64_t b = 0; b < *missing_content_index->m_BlockCount; ++b)
        {
            TLongtail_Hash block_hash = missing_content_index->m_BlockHash[b];
            char* block_name = GetBlockName(block_hash);

            char source_path[256];
            sprintf(source_path, "%s/%s.lrb", delta_upload_content_folder, block_name);

            char target_path[256];
            sprintf(target_path, "%s/%s.lrb", CONTENT_FOLDER, block_name);

            free(block_name);

            StorageAPI::HOpenFile v = storage_api.m_StorageAPI.OpenReadFile(&storage_api.m_StorageAPI, target_path);
            if (v)
            {
                storage_api.m_StorageAPI.CloseRead(&storage_api.m_StorageAPI, v);
                v = 0;
                continue;
            }

            StorageAPI::HOpenFile s = storage_api.m_StorageAPI.OpenReadFile(&storage_api.m_StorageAPI, source_path);
            ASSERT_NE((StorageAPI::HOpenFile)0, s);

            ASSERT_NE(0, EnsureParentPathExists(&storage_api.m_StorageAPI, target_path));
            StorageAPI::HOpenFile t = storage_api.m_StorageAPI.OpenWriteFile(&storage_api.m_StorageAPI, target_path);
            ASSERT_NE((StorageAPI::HOpenFile)0, t);

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
            shed.m_Shed,
            full_content_index,
            version_index,
            CONTENT_FOLDER,
            version_target_folder));

        free(path_lookup);
        path_lookup = 0;

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

    Shed shed;

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

    Paths* local_path_1_paths = GetFilesRecursively(&storage_api.m_StorageAPI, local_path_1);
    ASSERT_NE((Paths*)0, local_path_1_paths);
    VersionIndex* version1 = CreateVersionIndex(&storage_api.m_StorageAPI, &hash_api.m_HashAPI, shed.m_Shed, local_path_1, local_path_1_paths);
    WriteVersionIndex(&storage_api.m_StorageAPI, version1, version_index_path_1);
    free(local_path_1_paths);
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version1->m_AssetCount, local_path_1, version_index_path_1);

    printf("Creating local content index...\n");
    ContentIndex* local_content_index = CreateContentIndex(
        &hash_api.m_HashAPI,
        local_path_1,
        *version1->m_AssetCount,
        version1->m_AssetContentHash,
        version1->m_PathHash,
        version1->m_AssetSize,
        version1->m_NameOffset,
        version1->m_NameData,
        GetContentTag);

    printf("Writing local content index...\n");
    WriteContentIndex(&storage_api.m_StorageAPI, local_content_index, local_content_index_path);
    printf("%" PRIu64 " blocks from version `%s` indexed to `%s`\n", *local_content_index->m_BlockCount, local_path_1, local_content_index_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *local_content_index->m_BlockCount, local_content_path);
        PathLookup* path_lookup = CreateContentHashToPathLookup(version1, 0);
        WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            local_content_index,
            path_lookup,
            local_path_1,
            local_content_path);

        free(path_lookup);
        path_lookup = 0;
    }

    printf("Reconstructing %" PRIu64 " assets to `%s`\n", *version1->m_AssetCount, remote_path_1);
    ASSERT_EQ(1, ReconstructVersion(&storage_api.m_StorageAPI, &compression_api.m_CompressionAPI, shed.m_Shed, local_content_index, version1, local_content_path, remote_path_1));
    printf("Reconstructed %" PRIu64 " assets to `%s`\n", *version1->m_AssetCount, remote_path_1);

    printf("Indexing `%s`...\n", local_path_2);
    Paths* local_path_2_paths = GetFilesRecursively(&storage_api.m_StorageAPI, local_path_2);
    ASSERT_NE((Paths*)0, local_path_2_paths);
    VersionIndex* version2 = CreateVersionIndex(&storage_api.m_StorageAPI, &hash_api.m_HashAPI, shed.m_Shed, local_path_2, local_path_2_paths);
    free(local_path_2_paths);
    ASSERT_NE((VersionIndex*)0, version2);
    ASSERT_EQ(1, WriteVersionIndex(&storage_api.m_StorageAPI, version2, version_index_path_2));
    printf("%" PRIu64 " assets from folder `%s` indexed to `%s`\n", *version2->m_AssetCount, local_path_2, version_index_path_2);
    
    // What is missing in local content that we need from remote version in new blocks with just the missing assets.
    ContentIndex* missing_content = CreateMissingContent(
        &hash_api.m_HashAPI,
        local_content_index,
        local_path_2,
        version2,
        GetContentTag);
    ASSERT_NE((ContentIndex*)0, missing_content);
    printf("%" PRIu64 " blocks for version `%s` needed in content index `%s`\n", *missing_content->m_BlockCount, local_path_1, local_content_path);

    if (1)
    {
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, local_content_path);
        PathLookup* path_lookup = CreateContentHashToPathLookup(version2, 0);
        ASSERT_NE((PathLookup*)0, path_lookup);
        ASSERT_EQ(1, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            missing_content,
            path_lookup,
            local_path_2,
            local_content_path));

        free(path_lookup);
        path_lookup = 0;
    }

    if (1)
    {
        // Write this to disk for reference to see how big the diff is...
        printf("Writing %" PRIu64 " block to `%s`\n", *missing_content->m_BlockCount, remote_content_path);
        PathLookup* path_lookup = CreateContentHashToPathLookup(version2, 0);
        ASSERT_NE((PathLookup*)0, path_lookup);
        ASSERT_EQ(1, WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            missing_content,
            path_lookup,
            local_path_2,
            remote_content_path));

        free(path_lookup);
        path_lookup = 0;
    }

//    ContentIndex* remote_content_index = CreateContentIndex(
//        local_path_2,
//        *version2->m_AssetCount,
//        version2->m_AssetContentHash,
//        version2->m_PathHash,
//        version2->m_AssetSize,
//        version2->m_NameOffset,
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
//        StorageAPI::HOpenFile s = storage_api.OpenReadFile(source_path);
//        char* target_path = storage_api.ConcatPath(local_content_path, block_file_name);
//        StorageAPI::HOpenFile t = storage_api.OpenWriteFile(target_path);
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
    ASSERT_EQ(1, ReconstructVersion(&storage_api.m_StorageAPI, &compression_api.m_CompressionAPI, shed.m_Shed, merged_local_content, version2, local_content_path, remote_path_2));
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
