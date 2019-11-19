
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

int main(int argc, char** argv)
{
    int result = 0;

    int32_t target_chunk_size = 32768;
    kgflags_int("target-chunk-size", target_chunk_size, "Target chunk size", false, &target_chunk_size);

    int32_t max_chunks_per_block = 8192;
    kgflags_int("max-chunks-per-block", max_chunks_per_block, "Max chunks per block", false, &max_chunks_per_block);

    int32_t target_block_size = 65536 * 8;
    kgflags_int("target-block-size", target_block_size, "Target block size", false, &target_block_size);

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
    kgflags_string("create-version", NULL, "Path to version index", false, &create_version_raw);

    const char* list_missing_blocks_raw = NULL;
    kgflags_string("list-missing-blocks", NULL, "Path to content index", false, &list_missing_blocks_raw);

    if (!kgflags_parse(argc, argv)) {
        kgflags_print_errors();
        kgflags_print_usage();
        return 1;
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

        Paths* version_paths = GetFilesRecursively(
            &storage_api.m_StorageAPI,
            version);
        if (!version_paths)
        {
            printf("Failed to scan folder `%s`\n", version);
            result = 1;
            goto end;
        }
        VersionIndex* version_index = CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            version,
            version_paths,
            target_chunk_size);
        free(version_paths);
        version_paths = 0;
        if (!version_index)
        {
            printf("Failed to create version index for `%s`\n", version);
            result = 1;
            goto end;
        }
        
        int ok = CreateParentPath(&storage_api.m_StorageAPI, create_version_index) && WriteVersionIndex(&storage_api.m_StorageAPI, version_index, create_version_index);
        free(version_index);
        version_index = 0;
        if (!ok)
        {
            printf("Failed to create version index to `%s`\n", create_version_index);
            result = 1;
            goto end;
        }
        result = 0;
        goto end;
    }

    if (create_content_index && !version)
    {
        if (content && (!content_index && !merge_content_index))
        {
            TroveStorageAPI storage_api;
            MeowHashAPI hash_api;
            BikeshedJobAPI job_api;
            ContentIndex* cindex = ReadContent(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                content);
            if (!cindex)
            {
                printf("Failed to create content index for content `%s`\n", content);
                result = 1;
                goto end;
            }
            int ok = CreateParentPath(&storage_api.m_StorageAPI, create_content_index) &&
                WriteContentIndex(
                    &storage_api.m_StorageAPI,
                    cindex,
                    create_content_index);

            free(cindex);
            cindex = 0;
            if (!ok)
            {
                printf("Failed to write content index to `%s`\n", create_content_index);
                result = 1;
                goto end;
            }
            result = 0;
            goto end;
        }
        if (content_index && merge_content_index)
        {
            TroveStorageAPI storage_api;
            ContentIndex* cindex1 = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex1)
            {
                printf("Failed to read content index from `%s`\n", content_index);
                result = 1;
                goto end;
            }
            ContentIndex* cindex2 = ReadContentIndex(&storage_api.m_StorageAPI, merge_content_index);
            if (!cindex2)
            {
                free(cindex1);
                cindex1 = 0;
                printf("Failed to read content index from `%s`\n", merge_content_index);
                result = 1;
                goto end;
            }
            ContentIndex* cindex = MergeContentIndex(cindex1, cindex2);
            free(cindex2);
            cindex2 = 0;
            free(cindex1);
            cindex1 = 0;

            if (!cindex)
            {
                printf("Failed to merge content index `%s` with `%s`\n", content_index, merge_content_index);
                result = 1;
                goto end;
            }

            int ok = CreateParentPath(&storage_api.m_StorageAPI, create_content_index) &&
                WriteContentIndex(
                    &storage_api.m_StorageAPI,
                    cindex,
                    create_content_index);

            free(cindex);
            cindex = 0;

            if (!ok)
            {
                printf("Failed to write content index to `%s`\n", create_content_index);
                result = 1;
                goto end;
            }
            result = 0;
            goto end;
        }
    }

    if (create_content_index && version)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        BikeshedJobAPI job_api;

        VersionIndex* vindex = 0;
        if (version_index)
        {
            vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
            if (!vindex)
            {
                printf("Failed to read version index from `%s`\n", version_index);
                result = 1;
                goto end;
            }
        }
        else
        {
            Paths* version_paths = GetFilesRecursively(
                &storage_api.m_StorageAPI,
                version);
            if (!version_paths)
            {
                printf("Failed to scan folder `%s`\n", version);
                result = 1;
                goto end;
            }
            vindex = CreateVersionIndex(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                version,
                version_paths,
                target_chunk_size);
            free(version_paths);
            version_paths = 0;
            if (!vindex)
            {
                printf("Failed to create version index for version `%s`\n", version);
                result = 1;
                goto end;
            }
        }

        ContentIndex* existing_cindex = 0;
        if (content_index)
        {
            existing_cindex = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!existing_cindex)
            {
                free(vindex);
                vindex = 0;
                printf("Failed to read content index from `%s`\n", content_index);
                result = 1;
                goto end;
            }
        }
        else if (content)
        {
            existing_cindex = ReadContent(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                content);
            if (!existing_cindex)
            {
                free(vindex);
                vindex = 0;
                printf("Failed to read contents from `%s`\n", content);
                result = 1;
                goto end;
            }
        }
        else
        {
            existing_cindex = CreateContentIndex(
                &hash_api.m_HashAPI,
                0,
                0,
                0,
                0,
                0);
        }

        ContentIndex* cindex = CreateMissingContent(
            &hash_api.m_HashAPI,
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
            result = 1;
            goto end;
        }

        int ok = CreateParentPath(&storage_api.m_StorageAPI, create_content_index) &&
            WriteContentIndex(
                &storage_api.m_StorageAPI,
                cindex,
                create_content_index);

        free(cindex);
        cindex = 0;

        if (!ok)
        {
            printf("Failed to write content index to `%s`\n", create_content_index);
            result = 1;
            goto end;
        }

        result = 0;
        goto end;
    }

    if (create_content && version)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;
        VersionIndex* vindex = 0;
        if (version_index)
        {
            vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
            if (!vindex)
            {
                printf("Failed to read version index from `%s`\n", version_index);
                result = 1;
                goto end;
            }
        }
        else
        {
            Paths* version_paths = GetFilesRecursively(
                &storage_api.m_StorageAPI,
                version);
            if (!version_paths)
            {
                printf("Failed to scan folder `%s`\n", version);
                result = 1;
                goto end;
            }
            vindex = CreateVersionIndex(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                version,
                version_paths,
                target_chunk_size);
            free(version_paths);
            version_paths = 0;
            if (!vindex)
            {
                printf("Failed to create version index for version `%s`\n", version);
            result = 1;
            goto end;
            }
        }

        ContentIndex* cindex = 0;
        if (content_index)
        {
            cindex = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex)
            {
                printf("Failed to read content index from `%s`\n", content_index);
                result = 1;
                goto end;
            }
        }
        else
        {
            cindex = CreateContentIndex(
                &hash_api.m_HashAPI,
                *vindex->m_ChunkCount,
                vindex->m_ChunkHashes,
                vindex->m_ChunkSizes,
                target_block_size,
                max_chunks_per_block);
            if (!cindex)
            {
                printf("Failed to create content index for version `%s`\n", version);
                result = 1;
                goto end;
            }
        }

        struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
        int ok = CreatePath(&storage_api.m_StorageAPI, create_content) && WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
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
            result = 1;
            goto end;
        }
        result = 0;
        goto end;
    }

    if (list_missing_blocks && content_index)
    {
        TroveStorageAPI storage_api;
        ContentIndex* have_content_index = ReadContentIndex(&storage_api.m_StorageAPI, list_missing_blocks);
        if (!have_content_index)
        {
            result = 1;
            goto end;
        }
        ContentIndex* need_content_index = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
        if (!need_content_index)
        {
            free(have_content_index);
            have_content_index = 0;
            result = 1;
            goto end;
        }

        // TODO: Move to longtail.h
        uint32_t hash_size = jc::HashTable<TLongtail_Hash, uint32_t>::CalcSize((uint32_t)*have_content_index->m_ChunkCount);
        jc::HashTable<TLongtail_Hash, uint32_t> asset_hash_to_have_block_index;
        void* asset_hash_to_have_block_index_mem = malloc(hash_size);
        asset_hash_to_have_block_index.Create((uint32_t)*have_content_index->m_ChunkCount, asset_hash_to_have_block_index_mem);

        for (uint32_t i = 0; i < *have_content_index->m_ChunkCount; ++i)
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
            result = 0;
            goto end;
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
        result = 0;
        goto end;
    }


    if (create_version && version_index && content)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api;

        VersionIndex* vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
        if (!vindex)
        {
            printf("Failed to read version index from `%s`\n", version_index);
            result = 1;
            goto end;
        }

        ContentIndex* cindex = 0;
        if (content_index)
        {
            cindex = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex)
            {
                free(vindex);
                vindex = 0;
                printf("Failed to read content index from `%s`\n", content_index);
                result = 1;
                goto end;
            }
        }
        else
        {
            cindex = ReadContent(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                &job_api.m_JobAPI,
                content);
            if (!cindex)
            {
                free(vindex);
                vindex = 0;
                printf("Failed to create content index for version `%s`\n", version);
                result = 1;
                goto end;
            }
        }
        int ok = CreatePath(&storage_api.m_StorageAPI, create_version) && WriteVersion(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            &job_api.m_JobAPI,
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
            result = 1;
            goto end;
        }
        result = 0;
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
    free((void*)list_missing_blocks);
    return result;
}
