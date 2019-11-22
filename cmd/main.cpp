
#include "../src/longtail.h"

#define KGFLAGS_IMPLEMENTATION
#include "../third-party/kgflags/kgflags.h"

#define STB_DS_IMPLEMENTATION
#include "../src/stb_ds.h"

#include "../common/platform.h"

#include <stdio.h>

int CreateParentPath(struct StorageAPI* storage_api, const char* path);

int CreatePath(struct StorageAPI* storage_api, const char* path)
{
    if (storage_api->IsDir(storage_api, path))
    {
        return 1;
    }
    else
    {
        if (!CreateParentPath(storage_api, path))
        {
            return 0;
        }
        if (storage_api->CreateDir(storage_api, path))
        {
            return 1;
        }
    }
    return 0;
}

int CreateParentPath(struct StorageAPI* storage_api, const char* path)
{
    char* dir_path = strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        return 1;
    }
    while (last_path_delimiter > dir_path && last_path_delimiter[-1] == '/')
    {
        --last_path_delimiter;
    }
    *last_path_delimiter = '\0';
    int ok = CreatePath(storage_api, dir_path);
    free(dir_path);
    return ok;
}

char* NormalizePath(const char* path)
{
    if (!path)
    {
        return 0;
    }
    char* normalized_path = strdup(path);
    size_t wi = 0;
    size_t ri = 0;
    while (path[ri])
    {
        switch (path[ri])
        {
        case '/':
            if (wi && normalized_path[wi - 1] == '/')
            {
                ++ri;
            }
            else
            {
                normalized_path[wi++] = path[ri++];
            }
            break;
        case '\\':
            if (wi && normalized_path[wi - 1] == '/')
            {
                ++ri;
            }
            else
            {
                normalized_path[wi++] = '/';
                ++ri;
            }
            break;
        default:
            normalized_path[wi++] = path[ri++];
            break;
        }
    }
    normalized_path[wi] = '\0';
    return normalized_path;
}

struct Progress
{
    Progress() : m_OldPercent(0) {}
    uint32_t m_OldPercent;
    static void ProgressFunc(void* context, uint32_t total, uint32_t jobs_done)
    {
        Progress* p = (Progress*)context;
        if (jobs_done < total)
        {
            uint32_t percent_done = (100 * jobs_done) / total;
            if (percent_done - p->m_OldPercent >= 5)
            {
                printf("%u%% ", percent_done);
                p->m_OldPercent = percent_done;
            }
            return;
        }
        if (p->m_OldPercent != 0)
        {
            if (p->m_OldPercent != 100)
            {
                printf("100%%");
            }
            printf("\n");
        }
    }
};

int Cmd_CreateVersionIndex(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    const char* create_version_index,
    const char* version,
    const char* filter,
    int target_chunk_size)
{
    Paths* version_paths = GetFilesRecursively(
        storage_api,
        version);
    if (!version_paths)
    {
        printf("Failed to scan folder `%s`\n", version);
        return 1;
    }

    Progress progress;
    VersionIndex* vindex = CreateVersionIndex(
        storage_api,
        hash_api,
        job_api,
        Progress::ProgressFunc,
        &progress,
        version,
        version_paths,
        target_chunk_size);
    free(version_paths);
    version_paths = 0;
    if (!vindex)
    {
        printf("Failed to create version index for `%s`\n", version);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_version_index) && WriteVersionIndex(storage_api, vindex, create_version_index);

    VersionIndex* target_vindex = ReadVersionIndex(storage_api, create_version_index);
    if (!target_vindex)
    {
        free(vindex);
        vindex = 0;
        printf("Failed to read version index from `%s`\n", create_version_index);
        return 0;
    }

    struct VersionDiff* version_diff = CreateVersionDiff(
        vindex,
        target_vindex);

    free(version_diff);
    version_diff = 0;

    free(target_vindex);
    target_vindex = 0;

    free(vindex);
    vindex = 0;
    if (!ok)
    {
        printf("Failed to create version index to `%s`\n", create_version_index);
        return 0;
    }



    return 1;
}

int Cmd_CreateContentIndex(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    const char* create_content_index,
    const char* content)
{
    Progress progress;
    ContentIndex* cindex = ReadContent(
        storage_api,
        hash_api,
        job_api,
        Progress::ProgressFunc,
        &progress,
        content);
    if (!cindex)
    {
        printf("Failed to create content index for `%s`\n", content);
        return 0;
    }
    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    free(cindex);
    cindex = 0;
    if (!ok)
    {
        printf("Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

int Cmd_MergeContentIndex(
    StorageAPI* storage_api,
    const char* create_content_index,
    const char* content_index,
    const char* merge_content_index)
{
    ContentIndex* cindex1 = ReadContentIndex(storage_api, content_index);
    if (!cindex1)
    {
        printf("Failed to read content index from `%s`\n", content_index);
        return 0;
    }
    ContentIndex* cindex2 = ReadContentIndex(storage_api, merge_content_index);
    if (!cindex2)
    {
        free(cindex1);
        cindex1 = 0;
        printf("Failed to read content index from `%s`\n", merge_content_index);
        return 0;
    }
    ContentIndex* cindex = MergeContentIndex(cindex1, cindex2);
    free(cindex2);
    cindex2 = 0;
    free(cindex1);
    cindex1 = 0;

    if (!cindex)
    {
        printf("Failed to merge content index `%s` with `%s`\n", content_index, merge_content_index);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    free(cindex);
    cindex = 0;

    if (!ok)
    {
        printf("Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

int Cmd_CreateMissingContentIndex(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    const char* create_content_index,
    const char* content_index,
    const char* content,
    const char* version_index,
    const char* version,
    int target_block_size,
    int max_chunks_per_block,
    int target_chunk_size)
{
    VersionIndex* vindex = 0;
    if (version_index)
    {
        vindex = ReadVersionIndex(storage_api, version_index);
        if (!vindex)
        {
            printf("Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        Paths* version_paths = GetFilesRecursively(
            storage_api,
            version);
        if (!version_paths)
        {
            printf("Failed to scan folder `%s`\n", version);
            return 0;
        }
        Progress progress;
        vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            version_paths,
            target_chunk_size);
        free(version_paths);
        version_paths = 0;
        if (!vindex)
        {
            printf("Failed to create version index for version `%s`\n", version);
            return 0;
        }
    }

    ContentIndex* existing_cindex = 0;
    if (content_index)
    {
        existing_cindex = ReadContentIndex(storage_api, content_index);
        if (!existing_cindex)
        {
            free(vindex);
            vindex = 0;
            printf("Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else if (content)
    {
        Progress progress;
        existing_cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!existing_cindex)
        {
            free(vindex);
            vindex = 0;
            printf("Failed to read contents from `%s`\n", content);
            return 0;
        }
    }
    else
    {
        existing_cindex = CreateContentIndex(
            0,
            0,
            0,
            0,
            0,
            0);
    }

    ContentIndex* cindex = CreateMissingContent(
        hash_api,
        existing_cindex,
        vindex,
        target_block_size,
        max_chunks_per_block);

    free(existing_cindex);
    existing_cindex = 0;
    free(vindex);
    vindex = 0;
    if (!cindex)
    {
        printf("Failed to create content index for version `%s`\n", version);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    free(cindex);
    cindex = 0;

    if (!ok)
    {
        printf("Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

int Cmd_CreateContent(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    CompressionAPI* compression_api,
    const char* create_content,
    const char* content_index,
    const char* version,
    const char* version_index,
    int target_block_size,
    int max_chunks_per_block,
    int target_chunk_size)
{
    VersionIndex* vindex = 0;
    if (version_index)
    {
        vindex = ReadVersionIndex(storage_api, version_index);
        if (!vindex)
        {
            printf("Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        Paths* version_paths = GetFilesRecursively(
            storage_api,
            version);
        if (!version_paths)
        {
            printf("Failed to scan folder `%s`\n", version);
            return 0;
        }
        Progress progress;
        vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            version_paths,
            target_chunk_size);
        free(version_paths);
        version_paths = 0;
        if (!vindex)
        {
            printf("Failed to create version index for version `%s`\n", version);
            return 0;
        }
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            printf("Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else
    {
        cindex = CreateContentIndex(
            hash_api,
            *vindex->m_ChunkCount,
            vindex->m_ChunkHashes,
            vindex->m_ChunkSizes,
            target_block_size,
            max_chunks_per_block);
        if (!cindex)
        {
            printf("Failed to create content index for version `%s`\n", version);
            return 0;
        }
    }

    Progress progress;
    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
    int ok = CreatePath(storage_api, create_content) && WriteContent(
        storage_api,
        storage_api,
        compression_api,
        job_api,
        Progress::ProgressFunc,
        &progress,
        cindex,
        asset_part_lookup,
        version,
        create_content);

    FreeAssetPartLookup(asset_part_lookup);
    asset_part_lookup = 0;
    free(vindex);
    vindex = 0;
    free(cindex);
    cindex = 0;

    if (!ok)
    {
        printf("Failed to write content to `%s`\n", create_content);
        return 0;
    }
    return 1;
}

int Cmd_ListMissingBlocks(
    StorageAPI* storage_api,
    const char* list_missing_blocks,
    const char* content_index)
{
    ContentIndex* have_content_index = ReadContentIndex(storage_api, list_missing_blocks);
    if (!have_content_index)
    {
        return 0;
    }
    ContentIndex* need_content_index = ReadContentIndex(storage_api, content_index);
    if (!need_content_index)
    {
        free(have_content_index);
        have_content_index = 0;
        return 0;
    }

    // TODO: Move to longtail.h
    uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)*have_content_index->m_ChunkCount);
    jc::HashTable<TLongtail_Hash, uint32_t> asset_hash_to_have_block_index;
    void* asset_hash_to_have_block_index_mem = malloc(hash_size);
    asset_hash_to_have_block_index.Create((uint32_t)*have_content_index->m_ChunkCount, asset_hash_to_have_block_index_mem);

    for (uint32_t i = 0; i < (uint32_t)*have_content_index->m_ChunkCount; ++i)
    {
        asset_hash_to_have_block_index.Put(have_content_index->m_ChunkHashes[i], have_content_index->m_ChunkHashes[i]);
    }

    hash_size = jc::HashTable<uint32_t, uint32_t>::CalcSize((uint32_t)*need_content_index->m_BlockCount);
    jc::HashTable<uint32_t, uint32_t> need_block_index_to_asset_count;
    void* need_block_index_to_asset_count_mem = malloc(hash_size);
    need_block_index_to_asset_count.Create((uint32_t)*need_content_index->m_BlockCount, need_block_index_to_asset_count_mem);

    for (uint32_t i = 0; i < *need_content_index->m_ChunkCount; ++i)
    {
        uint32_t* have_block_index_ptr = asset_hash_to_have_block_index.Get(need_content_index->m_ChunkHashes[i]);
        if (have_block_index_ptr)
        {
            continue;
        }
        uint32_t* need_block_index_ptr = need_block_index_to_asset_count.Get(need_content_index->m_ChunkBlockIndexes[i]);
        if (need_block_index_ptr)
        {
            (*need_block_index_ptr)++;
            continue;
        }
        need_block_index_to_asset_count.Put(need_content_index->m_ChunkBlockIndexes[i], 1u);
    }

    free(need_block_index_to_asset_count_mem);
    need_block_index_to_asset_count_mem = 0;
    uint32_t missing_block_count = need_block_index_to_asset_count.Size();
    if (missing_block_count == 0)
    {
        free(asset_hash_to_have_block_index_mem);
        asset_hash_to_have_block_index_mem = 0;
        free(need_content_index);
        need_content_index = 0;
        free(have_content_index);
        have_content_index = 0;
        return 0;
    }
    TLongtail_Hash* missing_block_hashes = (TLongtail_Hash*)malloc(sizeof(TLongtail_Hash) * missing_block_count);
    uint32_t block_index = 0;
    for (jc::HashTable<uint32_t, uint32_t>::Iterator it = need_block_index_to_asset_count.Begin(); it != need_block_index_to_asset_count.End(); ++it)
    {
        uint32_t need_block_index = *it.GetValue();
        TLongtail_Hash need_block_hash = need_content_index->m_BlockHashes[need_block_index];
        missing_block_hashes[block_index++] = need_block_hash;
    }
    free(need_block_index_to_asset_count_mem);
    need_block_index_to_asset_count_mem = 0;

    free(missing_block_hashes);
    missing_block_hashes = 0;

    return 1;
}

int Cmd_CreateVersion(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    CompressionAPI* compression_api,
    const char* create_version,
    const char* version_index,
    const char* content,
    const char* content_index)
{
    VersionIndex* vindex = ReadVersionIndex(storage_api, version_index);
    if (!vindex)
    {
        printf("Failed to read version index from `%s`\n", version_index);
        return 0;
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            free(vindex);
            vindex = 0;
            printf("Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else
    {
        Progress progress;
        cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!cindex)
        {
            free(vindex);
            vindex = 0;
            printf("Failed to create content index for `%s`\n", content);
            return 0;
        }
    }

    Progress progress;
    int ok = CreatePath(storage_api, create_version) && WriteVersion(
        storage_api,
        storage_api,
        compression_api,
        job_api,
        Progress::ProgressFunc,
        &progress,
        cindex,
        vindex,
        content,
        create_version);
    free(vindex);
    vindex = 0;
    free(cindex);
    cindex = 0;
    if (!ok)
    {
        printf("Failed to create version `%s`\n", create_version);
        return 0;
    }
    return 1;
}

int Cmd_UpdateVersion(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    CompressionAPI* compression_api,
    const char* update_version,
    const char* version_index,
    const char* content,
    const char* content_index,
    const char* target_version_index,
    int target_chunk_size)
{
    VersionIndex* source_vindex = 0;
    if (version_index)
    {
        source_vindex = ReadVersionIndex(storage_api, version_index);
        if (!source_vindex)
        {
            printf("Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        Paths* version_paths = GetFilesRecursively(
            storage_api,
            update_version);
        if (!version_paths)
        {
            printf("Failed to scan folder `%s`\n", update_version);
            return 0;
        }
        Progress progress;
        source_vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            update_version,
            version_paths,
            target_chunk_size);
        free(version_paths);
        version_paths = 0;
        if (!source_vindex)
        {
            printf("Failed to create version index for version `%s`\n", update_version);
            return 0;
        }
    }

    VersionIndex* target_vindex = ReadVersionIndex(storage_api, target_version_index);
    if (!target_vindex)
    {
        free(source_vindex);
        source_vindex = 0;
        printf("Failed to read version index from `%s`\n", target_version_index);
        return 0;
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            free(target_vindex);
            target_vindex = 0;
            free(source_vindex);
            source_vindex = 0;
            printf("Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else
    {
        Progress progress;
        cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!cindex)
        {
            free(target_vindex);
            target_vindex = 0;
            free(source_vindex);
            source_vindex = 0;
            printf("Failed to create content index for `%s`\n", content);
            return 0;
        }
    }

    struct VersionDiff* version_diff = CreateVersionDiff(
        source_vindex,
        target_vindex);
    if (!version_diff)
    {
        free(cindex);
        cindex = 0;
        free(target_vindex);
        target_vindex = 0;
        free(source_vindex);
        source_vindex = 0;
        printf("Failed to create version diff from `%s` to `%s`\n", version_index, target_version_index);
        return 0;
    }

    Progress progress;
    int ok = CreatePath(storage_api, update_version) && ChangeVersion(
        storage_api,
        storage_api,
        hash_api,
        job_api,
        Progress::ProgressFunc,
        &progress,
        compression_api,
        cindex,
        source_vindex,
        target_vindex,
        version_diff,
        content,
        update_version);

    free(cindex);
    cindex = 0;
    free(target_vindex);
    target_vindex = 0;
    free(source_vindex);
    source_vindex = 0;
    free(version_diff);
    version_diff = 0;

    if (!ok)
    {
        printf("Failed to update version `%s` to `%s`\n", update_version, target_version_index);
        return 0;
    }
    return 1;
}

int main(int argc, char** argv)
{
    int result = 0;

    int32_t target_chunk_size = 8;
    kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

    int32_t max_chunks_per_block = 0;
    kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

    int32_t target_block_size = 0;
    kgflags_int("target-block-size", 32768 * 8, "Target block size", false, &target_block_size);

    const char* create_version_index_raw = NULL;
    kgflags_string("create-version-index", NULL, "Path to version index output", false, &create_version_index_raw);

    const char* version_raw = NULL;
    kgflags_string("version", NULL, "Path to version assets input", false, &version_raw);

    const char* filter_raw = NULL;
    kgflags_string("filter", NULL, "Path to filter file input", false, &filter_raw);

    const char* create_content_index_raw = NULL;
    kgflags_string("create-content-index", NULL, "Path to content index output", false, &create_content_index_raw);

    const char* version_index_raw = NULL;
    kgflags_string("version-index", NULL, "Path to version index input", false, &version_index_raw);

    const char* content_raw = NULL;
    kgflags_string("content", NULL, "Path to content block input", false, &content_raw);
    
    const char* create_content_raw = NULL;
    kgflags_string("create-content", NULL, "Path to content block output", false, &create_content_raw);

    const char* content_index_raw = NULL;
    kgflags_string("content-index", NULL, "Path to content index input", false, &content_index_raw);

    const char* merge_content_index_raw = NULL;
    kgflags_string("merge-content-index", NULL, "Path to base content index", false, &merge_content_index_raw);

    const char* create_version_raw = NULL;
    kgflags_string("create-version", NULL, "Path to version", false, &create_version_raw);

    const char* update_version_raw = NULL;
    kgflags_string("update-version", NULL, "Path to version", false, &update_version_raw);

    const char* target_version_raw = NULL;
    kgflags_string("target-version", NULL, "Path to target version", false, &target_version_raw);

    const char* target_version_index_raw = NULL;
    kgflags_string("target-version-index", NULL, "Path to target version index", false, &target_version_index_raw);

    const char* list_missing_blocks_raw = NULL;
    kgflags_string("list-missing-blocks", NULL, "Path to content index", false, &list_missing_blocks_raw);

    const char* test_version_raw = NULL;
    kgflags_string("test-version", NULL, "Test everything", false, &test_version_raw);

    const char* test_base_path_raw = NULL;
    kgflags_string("test-base-path", NULL, "Base path for test everything", false, &test_base_path_raw);

    if (!kgflags_parse(argc, argv)) {
        kgflags_print_errors();
        kgflags_print_usage();
        return 1;
    }

    if (test_version_raw && test_base_path_raw)
    {
        const char* test_version = NormalizePath(test_version_raw);
        const char* test_base_path = NormalizePath(test_base_path_raw);

        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;

        char create_content_index[512];
        sprintf(create_content_index, "%s/chunks.lci", test_base_path);
        char content[512];
        sprintf(content, "%s/chunks", test_base_path);
        if (!Cmd_CreateContentIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            create_content_index,
            content))
        {
            return 1;
        }

        char create_version_index[512];
        sprintf(create_version_index, "%s/%s.lvi", test_base_path, test_version);
        char version[512];
        sprintf(version, "%s/local/%s", test_base_path, test_version);
        if (!Cmd_CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            create_version_index,
            version,
            0,
            target_chunk_size))
        {
            return 1;
        }

        sprintf(create_content_index, "%s/%s.lci", test_base_path, test_version);
        char content_index[512];
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        char version_index[512];
        sprintf(version_index, "%s/%s.lvi", test_base_path, test_version);
        sprintf(version, "%s/local/%s", test_base_path, test_version);
        if (!Cmd_CreateMissingContentIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            create_content_index,
            content_index,
            content,
            version_index,
            version,
            target_block_size,
            max_chunks_per_block,
            target_chunk_size))
        {
            return 1;
        }

        char create_content[512];
        sprintf(create_content, "%s/chunks", test_base_path);
        sprintf(content_index, "%s/%s.lci", test_base_path, test_version);
        if (!Cmd_CreateContent(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            create_content,
            content_index,
            version,
            version_index,
            target_block_size,
            max_chunks_per_block,
            target_chunk_size))
        {
            return 1;
        }

        sprintf(create_content_index, "%s/chunks.lci", test_base_path);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        char merge_content_index[512];
        sprintf(merge_content_index, "%s/%s.lci", test_base_path, test_version);
        if (!Cmd_MergeContentIndex(
            &storage_api.m_StorageAPI,
            create_content_index,
            content_index,
            merge_content_index))
        {
            return 1;
        }

        char update_version[512];
        sprintf(update_version, "%s/remote/incremental", test_base_path);
        sprintf(content, "%s/chunks", test_base_path);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        char target_version_index[512];
        sprintf(target_version_index, "%s/%s.lvi", test_base_path, test_version);
        if (!Cmd_UpdateVersion(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            update_version,
            0,
            content,
            content_index,
            target_version_index,
            target_chunk_size))
        {
            return 1;
        }

        free((char*)test_version);
        free((char*)test_base_path);
        return 0;
    }

    const char* create_version_index = NormalizePath(create_version_index_raw);
    const char* version = NormalizePath(version_raw);
    const char* filter = NormalizePath(filter_raw);
    const char* create_content_index = NormalizePath(create_content_index_raw);
    const char* version_index = NormalizePath(version_index_raw);
    const char* content = NormalizePath(content_raw);
    const char* create_content = NormalizePath(create_content_raw);
    const char* content_index = NormalizePath(content_index_raw);
    const char* merge_content_index = NormalizePath(merge_content_index_raw);
    const char* create_version = NormalizePath(create_version_raw);
    const char* update_version = NormalizePath(update_version_raw);
    const char* target_version = NormalizePath(target_version_raw);
    const char* target_version_index = NormalizePath(target_version_index_raw);
    const char* list_missing_blocks = NormalizePath(list_missing_blocks_raw);

    if (create_version_index && version)
    {
        if (filter)
        {
            printf("--filter option not yet supported\n");
            result = 1;
            goto end;
        }

        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        BikeshedJobAPI job_api;

        int ok = Cmd_CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            create_version_index,
            version,
            filter,
            target_chunk_size);
        result = ok ? 0 : 1;
        goto end;
    }

    if (create_content_index && !version)
    {
        if (content && (!content_index && !merge_content_index))
        {
            TroveStorageAPI storage_api;
            MeowHashAPI hash_api;
            BikeshedJobAPI job_api;
            int ok = Cmd_CreateContentIndex(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                create_content_index,
                content);
            result = ok ? 0 : 1;
            goto end;
        }
        if (content_index && merge_content_index)
        {
            TroveStorageAPI storage_api;
            int ok = Cmd_MergeContentIndex(
                &storage_api.m_StorageAPI,
                create_content_index,
                content_index,
                merge_content_index);
            result = ok ? 0 : 1;
            goto end;
        }
    }

    if (create_content_index && version)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        BikeshedJobAPI job_api;

        int ok = Cmd_CreateMissingContentIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            create_content_index,
            content_index,
            content,
            version_index,
            version,
            target_block_size,
            max_chunks_per_block,
            target_chunk_size);
        result = ok ? 0 : 1;
        goto end;
    }

    if (create_content && version)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;
        int ok = Cmd_CreateContent(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                &compression_api.m_CompressionAPI,
                create_content,
                content_index,
                version,
                version_index,
                target_block_size,
                max_chunks_per_block,
                target_chunk_size);
        result = ok ? 0 : 1;
        goto end;
    }

    if (list_missing_blocks && content_index)
    {
        TroveStorageAPI storage_api;
        int ok = Cmd_ListMissingBlocks(
            &storage_api.m_StorageAPI,
            list_missing_blocks,
            content_index);
        result = ok ? 0 : 1;
        goto end;
    }

    if (create_version && version_index && content)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;

        int ok = Cmd_CreateVersion(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            create_version,
            version_index,
            content,
            content_index);
        result = ok ? 0 : 1;
        goto end;
    }

    if (update_version && content && target_version_index)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;

        int ok = Cmd_UpdateVersion(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            update_version,
            version_index,
            content,
            content_index,
            target_version_index,
            target_chunk_size);

        result = ok ? 0 : 1;
        goto end;
    }

    kgflags_print_usage();
    return 1;

end: free((void*)create_version_index);
    free((void*)version);
    free((void*)filter);
    free((void*)create_content_index);
    free((void*)version_index);
    free((void*)content);
    free((void*)create_content);
    free((void*)content_index);
    free((void*)merge_content_index);
    free((void*)create_version);
    free((void*)update_version);
    free((void*)target_version);
    free((void*)target_version_index);
    free((void*)list_missing_blocks);
    return result;
}
