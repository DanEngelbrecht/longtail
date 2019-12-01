
#include "../src/longtail.h"

#define KGFLAGS_IMPLEMENTATION
#include "../third-party/kgflags/kgflags.h"

#define STB_DS_IMPLEMENTATION
#include "../src/stb_ds.h"

#include "../common/platform.h"

#include <stdio.h>
#include <inttypes.h>

void AssertFailure(const char* expression, const char* file, int line)
{
    fprintf(stderr, "%s(%d): Assert failed `%s`\n", file, line, expression);
    exit(-1);
}

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
        free(dir_path);
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

int_fast32_t PrintFormattedBlockList(uint64_t block_count, const TLongtail_Hash* block_hashes, const char* format_string)
{
    const char* format_start = format_string;
    const char* format_first_end = strstr(format_string, "{blockname}");
    if (!format_first_end)
    {
        return 0;
    }
    size_t first_length = (size_t)((intptr_t)format_first_end - (intptr_t)format_start);
    const char* format_second_start = &format_first_end[strlen("{blockname}")];
    for (uint64_t b = 0; b < block_count; ++b)
    {
        char block_name[64];
        sprintf(block_name, "0x%" PRIx64 ".lrb", block_hashes[b]);

        char output_str[512];
        memmove(output_str, format_string, first_length);
        memmove(&output_str[first_length], block_name, strlen(block_name));
        strcpy(&output_str[first_length + strlen(block_name)], format_second_start);

        printf("%s\n", output_str);
    }
    return 1;
}

struct Progress
{
    Progress(const char* task)
        : m_OldPercent(0)
    {
        fprintf(stderr, "%s: ", task);
    }
    ~Progress()
    {
        fprintf(stderr, " Done\n");
    }
    uint32_t m_OldPercent;
    static void ProgressFunc(void* context, uint32_t total, uint32_t jobs_done)
    {
        Progress* p = (Progress*)context;
        if (jobs_done < total)
        {
            uint32_t percent_done = (100 * jobs_done) / total;
            if (percent_done - p->m_OldPercent >= 5)
            {
                fprintf(stderr, "%u%% ", percent_done);
                p->m_OldPercent = percent_done;
            }
            return;
        }
        if (p->m_OldPercent != 0)
        {
            if (p->m_OldPercent != 100)
            {
                fprintf(stderr, "100%%");
            }
        }
    }
};

const uint32_t NO_COMPRESSION_TYPE = 0;
const uint32_t LIZARD_COMPRESSION_TYPE = (((uint32_t)'1') << 24) + (((uint32_t)'s') << 16) + (((uint32_t)'a') << '8') + ((uint32_t)'d');

static uint32_t* GetCompressionTypes(StorageAPI* , const FileInfos* file_infos)
{
    uint32_t count = *file_infos->m_Paths.m_PathCount;
    uint32_t* result = (uint32_t*)LONGTAIL_MALLOC(sizeof(uint32_t) * count);
    for (uint32_t i = 0; i < count; ++i)
    {
        const char* path = &file_infos->m_Paths.m_Data[file_infos->m_Paths.m_Offsets[i]];
        const char* extension_start = strrchr(path, '.');
        if ((extension_start == 0) ||
            (0 == strcmp(extension_start, ".zip")) ||
            (0 == strcmp(extension_start, ".7z")) ||
//            (0 == strcmp(extension_start, ".pak")) ||
            (0 == strcmp(extension_start, ".rar")) )
        {
            result[i] = NO_COMPRESSION_TYPE;
            continue;
        }
        result[i] = LIZARD_COMPRESSION_TYPE;
    }
    return result;
}
int Cmd_CreateVersionIndex(
    StorageAPI* storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    const char* create_version_index,
    const char* version,
    const char* filter,
    int target_chunk_size)
{
    struct FileInfos* file_infos = GetFilesRecursively(
        storage_api,
        version);
    if (!file_infos)
    {
        fprintf(stderr, "Failed to scan folder `%s`\n", version);
        return 0;
    }
    uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
    if (!compression_types)
    {
        fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
        LONGTAIL_FREE(file_infos);
        return 0;
    }

    VersionIndex* vindex = 0;
    {
        Progress progress("Indexing version");
        vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size);
    }
    LONGTAIL_FREE(compression_types);
    compression_types = 0;
    LONGTAIL_FREE(file_infos);
    file_infos = 0;
    if (!vindex)
    {
        fprintf(stderr, "Failed to create version index for `%s`\n", version);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_version_index) && WriteVersionIndex(storage_api, vindex, create_version_index);

    VersionIndex* target_vindex = ReadVersionIndex(storage_api, create_version_index);
    if (!target_vindex)
    {
        LONGTAIL_FREE(vindex);
        vindex = 0;
        fprintf(stderr, "Failed to read version index from `%s`\n", create_version_index);
        return 0;
    }

    struct VersionDiff* version_diff = CreateVersionDiff(
        vindex,
        target_vindex);

    LONGTAIL_FREE(version_diff);
    version_diff = 0;

    LONGTAIL_FREE(target_vindex);
    target_vindex = 0;

    LONGTAIL_FREE(vindex);
    vindex = 0;
    if (!ok)
    {
        fprintf(stderr, "Failed to create version index to `%s`\n", create_version_index);
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
    ContentIndex* cindex = 0;
    if (!content)
    {
        cindex = CreateContentIndex(
            hash_api,
            0,
            0,
            0,
            0,
            0,
            0);
        if (!cindex)
        {
            fprintf(stderr, "Failed to create empty content index\n");
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!cindex)
        {
            fprintf(stderr, "Failed to create content index for `%s`\n", content);
            return 0;
        }
    }
    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    LONGTAIL_FREE(cindex);
    cindex = 0;
    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
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
        fprintf(stderr, "Failed to read content index from `%s`\n", content_index);
        return 0;
    }
    ContentIndex* cindex2 = ReadContentIndex(storage_api, merge_content_index);
    if (!cindex2)
    {
        LONGTAIL_FREE(cindex1);
        cindex1 = 0;
        fprintf(stderr, "Failed to read content index from `%s`\n", merge_content_index);
        return 0;
    }
    ContentIndex* cindex = MergeContentIndex(cindex1, cindex2);
    LONGTAIL_FREE(cindex2);
    cindex2 = 0;
    LONGTAIL_FREE(cindex1);
    cindex1 = 0;

    if (!cindex)
    {
        fprintf(stderr, "Failed to merge content index `%s` with `%s`\n", content_index, merge_content_index);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    LONGTAIL_FREE(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
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
            fprintf(stderr, "Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        struct FileInfos* file_infos = GetFilesRecursively(
            storage_api,
            version);
        if (!file_infos)
        {
            fprintf(stderr, "Failed to scan folder `%s`\n", version);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
            LONGTAIL_FREE(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size);
        LONGTAIL_FREE(compression_types);
        compression_types = 0;
        LONGTAIL_FREE(file_infos);
        file_infos = 0;
        if (!vindex)
        {
            fprintf(stderr, "Failed to create version index for version `%s`\n", version);
            return 0;
        }
    }

    ContentIndex* existing_cindex = 0;
    if (content_index)
    {
        existing_cindex = ReadContentIndex(storage_api, content_index);
        if (!existing_cindex)
        {
            LONGTAIL_FREE(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else if (content)
    {
        Progress progress("Reading content");
        existing_cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!existing_cindex)
        {
            LONGTAIL_FREE(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read contents from `%s`\n", content);
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
            0,
            0);
    }

    ContentIndex* cindex = CreateMissingContent(
        hash_api,
        existing_cindex,
        vindex,
        target_block_size,
        max_chunks_per_block);

    LONGTAIL_FREE(existing_cindex);
    existing_cindex = 0;
    LONGTAIL_FREE(vindex);
    vindex = 0;
    if (!cindex)
    {
        fprintf(stderr, "Failed to create content index for version `%s`\n", version);
        return 0;
    }

    int ok = CreateParentPath(storage_api, create_content_index) &&
        WriteContentIndex(
            storage_api,
            cindex,
            create_content_index);

    LONGTAIL_FREE(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
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
            fprintf(stderr, "Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        struct FileInfos* file_infos = GetFilesRecursively(
            storage_api,
            version);
        if (!file_infos)
        {
            fprintf(stderr, "Failed to scan folder `%s`\n", version);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
            LONGTAIL_FREE(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size);
        LONGTAIL_FREE(compression_types);
        compression_types = 0;
        LONGTAIL_FREE(file_infos);
        file_infos = 0;
        if (!vindex)
        {
            fprintf(stderr, "Failed to create version index for version `%s`\n", version);
            return 0;
        }
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            fprintf(stderr, "Failed to read content index from `%s`\n", content_index);
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
            vindex->m_ChunkCompressionTypes,
            target_block_size,
            max_chunks_per_block);
        if (!cindex)
        {
            fprintf(stderr, "Failed to create content index for version `%s`\n", version);
            LONGTAIL_FREE(vindex);
            vindex = 0;
            return 0;
        }
    }

    if (!ValidateVersion(
        cindex,
        vindex))
    {
        LONGTAIL_FREE(cindex);
        cindex = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        fprintf(stderr, "Version `%s` does not fully encompass content `%s`\n", version, create_content);
        return 0;
    }

    struct ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
    if (!asset_part_lookup)
    {
        fprintf(stderr, "Failed to create source lookup table for version `%s`\n", version);
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }
    int ok = 0;
    {
        Progress progress("Writing content");
        ok = CreatePath(storage_api, create_content) && WriteContent(
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
    }

    FreeAssetPartLookup(asset_part_lookup);
    asset_part_lookup = 0;
    LONGTAIL_FREE(vindex);
    vindex = 0;
    LONGTAIL_FREE(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content to `%s`\n", create_content);
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
        LONGTAIL_FREE(have_content_index);
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
        LONGTAIL_FREE(need_content_index);
        need_content_index = 0;
        LONGTAIL_FREE(have_content_index);
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
        fprintf(stderr, "Failed to read version index from `%s`\n", version_index);
        return 0;
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            LONGTAIL_FREE(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!cindex)
        {
            LONGTAIL_FREE(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to create content index for `%s`\n", content);
            return 0;
        }
    }

    if (!ValidateContent(
        cindex,
        vindex))
    {
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        fprintf(stderr, "Content `%s` does not fully encompass version `%s`\n", content, create_version);
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Writing version");
        ok = CreatePath(storage_api, create_version) && WriteVersion(
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
    }
    LONGTAIL_FREE(vindex);
    vindex = 0;
    LONGTAIL_FREE(cindex);
    cindex = 0;
    if (!ok)
    {
        fprintf(stderr, "Failed to create version `%s`\n", create_version);
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
            fprintf(stderr, "Failed to read version index from `%s`\n", version_index);
            return 0;
        }
    }
    else
    {
        struct FileInfos* file_infos = GetFilesRecursively(
            storage_api,
            update_version);
        if (!file_infos)
        {
            fprintf(stderr, "Failed to scan folder `%s`\n", update_version);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", update_version);
            LONGTAIL_FREE(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        source_vindex = CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            update_version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size);
        LONGTAIL_FREE(compression_types);
        compression_types = 0;
        LONGTAIL_FREE(file_infos);
        file_infos = 0;
        if (!source_vindex)
        {
            fprintf(stderr, "Failed to create version index for version `%s`\n", update_version);
            return 0;
        }
    }

    VersionIndex* target_vindex = ReadVersionIndex(storage_api, target_version_index);
    if (!target_vindex)
    {
        LONGTAIL_FREE(source_vindex);
        source_vindex = 0;
        fprintf(stderr, "Failed to read version index from `%s`\n", target_version_index);
        return 0;
    }

    ContentIndex* cindex = 0;
    if (content_index)
    {
        cindex = ReadContentIndex(storage_api, content_index);
        if (!cindex)
        {
            LONGTAIL_FREE(target_vindex);
            target_vindex = 0;
            LONGTAIL_FREE(source_vindex);
            source_vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`\n", content_index);
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        cindex = ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content);
        if (!cindex)
        {
            LONGTAIL_FREE(target_vindex);
            target_vindex = 0;
            LONGTAIL_FREE(source_vindex);
            source_vindex = 0;
            fprintf(stderr, "Failed to create content index for `%s`\n", content);
            return 0;
        }
    }

    if (!ValidateContent(
        cindex,
        target_vindex))
    {
        LONGTAIL_FREE(target_vindex);
        target_vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        fprintf(stderr, "Content `%s` does not fully encompass version `%s`\n", content, target_version_index);
        return 0;
    }

    struct VersionDiff* version_diff = CreateVersionDiff(
        source_vindex,
        target_vindex);
    if (!version_diff)
    {
        LONGTAIL_FREE(cindex);
        cindex = 0;
        LONGTAIL_FREE(target_vindex);
        target_vindex = 0;
        LONGTAIL_FREE(source_vindex);
        source_vindex = 0;
        fprintf(stderr, "Failed to create version diff from `%s` to `%s`\n", version_index, target_version_index);
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Updating version");
        ok = CreatePath(storage_api, update_version) && ChangeVersion(
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
    }

    LONGTAIL_FREE(cindex);
    cindex = 0;
    LONGTAIL_FREE(target_vindex);
    target_vindex = 0;
    LONGTAIL_FREE(source_vindex);
    source_vindex = 0;
    LONGTAIL_FREE(version_diff);
    version_diff = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to update version `%s` to `%s`\n", update_version, target_version_index);
        return 0;
    }
    return 1;
}

int Cmd_UpSyncVersion(
    StorageAPI* source_storage_api,
    StorageAPI* target_storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    CompressionAPI* compression_api,
    const char* version_path,
    const char* version_index_path,
    const char* content_path,
    const char* content_index_path,
    const char* upload_content_path,
    const char* output_format,
    int max_chunks_per_block,
    int target_block_size,
    int target_chunk_size)
{
    VersionIndex* vindex = ReadVersionIndex(source_storage_api, version_index_path);
    if (!vindex)
    {
        struct FileInfos* file_infos = GetFilesRecursively(
            source_storage_api,
            version_path);
        if (!file_infos)
        {
            fprintf(stderr, "Failed to scan folder `%s`\n", version_path);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(
            source_storage_api,
            file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version_path);
            LONGTAIL_FREE(file_infos);
            return 0;
        }

        Progress progress("Indexing version");
        vindex = CreateVersionIndex(
            source_storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version_path,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size);
        LONGTAIL_FREE(compression_types);
        compression_types = 0;
        LONGTAIL_FREE(file_infos);
        file_infos = 0;
        if (!vindex)
        {
            fprintf(stderr, "Failed to create version index for `%s`\n", version_path);
            return 0;
        }
    }
    struct ContentIndex* cindex = 0;
    if (content_index_path)
    {
        cindex = ReadContentIndex(
            source_storage_api,
            content_index_path);
    }
    if (!cindex)
    {
        if (!content_path && content_index_path)
        {
            cindex = CreateContentIndex(
                hash_api,
                0,
                0,
                0,
                0,
                target_block_size,
                max_chunks_per_block);
            if (!cindex)
            {
                fprintf(stderr, "Failed to create empty content index\n");
                LONGTAIL_FREE(vindex);
                vindex = 0;
                return 0;
            }
        }
        else
        {
            if (!content_path)
            {
                fprintf(stderr, "--content folder must be given if no valid content index is given with --content-index\n");
                return 0;
            }
            Progress progress("Reading content");
            cindex = ReadContent(
                source_storage_api,
                hash_api,
                job_api,
                Progress::ProgressFunc,
                &progress,
                content_path);
            if (!cindex)
            {
                fprintf(stderr, "Failed to create content index for `%s`\n", content_path);
                LONGTAIL_FREE(vindex);
                vindex = 0;
                return 0;
            }
        }
    }

    ContentIndex* missing_content_index = CreateMissingContent(
        hash_api,
        cindex,
        vindex,
        target_block_size,
        max_chunks_per_block);
    if (!missing_content_index)
    {
        fprintf(stderr, "Failed to generate content index for missing content\n");
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    ChunkHashToAssetPart* asset_part_lookup = CreateAssetPartLookup(vindex);
    if (!asset_part_lookup)
    {
        fprintf(stderr, "Failed to create source lookup table for version `%s`\n", version_path);
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Writing content");
        ok = CreatePath(target_storage_api, upload_content_path) && WriteContent(
            source_storage_api,
            target_storage_api,
            compression_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            missing_content_index,
            asset_part_lookup,
            version_path,
            upload_content_path);
    }
    if (!ok)
    {
        fprintf(stderr, "Failed to create new content from `%s` to `%s`\n", version_path, upload_content_path);
        LONGTAIL_FREE(missing_content_index);
        missing_content_index = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    if (!PrintFormattedBlockList(*missing_content_index->m_BlockCount, missing_content_index->m_BlockHashes, output_format))
    {
        fprintf(stderr, "Failed to format block output using format `%s`\n", output_format);
        LONGTAIL_FREE(missing_content_index);
        missing_content_index = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    ContentIndex* new_content_index = MergeContentIndex(cindex, missing_content_index);
    if (!new_content_index)
    {
        fprintf(stderr, "Failed creating a new content index with the added content\n");
        LONGTAIL_FREE(missing_content_index);
        missing_content_index = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    ok = CreateParentPath(target_storage_api, upload_content_path) && WriteVersionIndex(
        target_storage_api,
        vindex,
        version_index_path);
    if (!ok)
    {
        fprintf(stderr, "Failed to write the new version index to `%s`\n", version_index_path);
        LONGTAIL_FREE(new_content_index);
        new_content_index = 0;
        LONGTAIL_FREE(missing_content_index);
        missing_content_index = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    ok = CreateParentPath(target_storage_api, content_index_path) && WriteContentIndex(
        target_storage_api,
        new_content_index,
        content_index_path);
    if (!ok)
    {
        fprintf(stderr, "Failed to write the new version index to `%s`\n", version_index_path);
        LONGTAIL_FREE(new_content_index);
        new_content_index = 0;
        LONGTAIL_FREE(missing_content_index);
        missing_content_index = 0;
        LONGTAIL_FREE(vindex);
        vindex = 0;
        LONGTAIL_FREE(cindex);
        cindex = 0;
        return 0;
    }

    LONGTAIL_FREE(new_content_index);
    new_content_index = 0;
    LONGTAIL_FREE(missing_content_index);
    missing_content_index = 0;
    LONGTAIL_FREE(vindex);
    vindex = 0;
    LONGTAIL_FREE(cindex);
    cindex = 0;

    fprintf(stderr, "Updated version index `%s`\n", version_index_path);
    fprintf(stderr, "Updated content index `%s`\n", content_index_path);
    fprintf(stderr, "Wrote added content to `%s`\n", upload_content_path);

    return 0;
}

struct HashToIndex
{
    TLongtail_Hash key;
    uint64_t value;
};

int Cmd_DownSyncVersion(
    StorageAPI* source_storage_api,
    StorageAPI* target_storage_api,
    HashAPI* hash_api,
    JobAPI* job_api,
    CompressionAPI* compression_api,
    const char* target_version_index_path,
    const char* have_content_index_path,
    const char* have_content_path,
    const char* remote_content_index_path,
    const char* remote_content_path,
    const char* output_format,
    int max_chunks_per_block,
    int target_block_size,
    int target_chunk_size)
{
    VersionIndex* vindex_target = ReadVersionIndex(source_storage_api, target_version_index_path);
    if (!vindex_target)
    {
        //TODO: print
        return 0;
    }

    ContentIndex* existing_cindex = 0;
    if (have_content_index_path)
    {
        existing_cindex = ReadContentIndex(source_storage_api, have_content_index_path);
        if (!existing_cindex)
        {
            // TODO: print
            return 0;
        }
    }
    if (!existing_cindex)
    {
        if (!have_content_path)
        {
            // TODO: Print
            LONGTAIL_FREE(vindex_target);
            vindex_target = 0;
            return 0;
        }
        Progress progress("Reading content");
        existing_cindex = ReadContent(
            source_storage_api,
            hash_api, job_api,
            Progress::ProgressFunc,
            &progress,
            have_content_path);
        if (!existing_cindex)
        {
            // TODO: Print
            LONGTAIL_FREE(vindex_target);
            vindex_target = 0;
            return 0;
        }
    }

    ContentIndex* cindex_missing = CreateMissingContent(
        hash_api,
        existing_cindex,
        vindex_target,
        target_block_size,
        max_chunks_per_block);
    if (!cindex_missing)
    {
        // TODO: Print
        LONGTAIL_FREE(existing_cindex);
        existing_cindex = 0;
        LONGTAIL_FREE(vindex_target);
        vindex_target = 0;
        return 0;
    }
    LONGTAIL_FREE(existing_cindex);
    existing_cindex = 0;
    LONGTAIL_FREE(vindex_target);
    vindex_target = 0;

    ContentIndex* cindex_remote = ReadContentIndex(source_storage_api, remote_content_index_path);
    if (!cindex_remote)
    {
        if (!remote_content_path)
        {
            //TODO: print
            LONGTAIL_FREE(cindex_missing);
            cindex_missing = 0;
            return 0;
        }
        Progress progress("Reading content");
        cindex_remote = ReadContent(
            source_storage_api,
            hash_api, job_api,
            Progress::ProgressFunc,
            &progress,
            remote_content_path);
        if (!cindex_remote)
        {
            //TODO: print
            LONGTAIL_FREE(cindex_missing);
            cindex_missing = 0;
            return 0;
        }

    }

    ContentIndex* request_content = RetargetContent(
        cindex_remote,
        cindex_missing);
    if (!request_content)
    {
        LONGTAIL_FREE(cindex_remote);
        cindex_remote = 0;
        LONGTAIL_FREE(cindex_missing);
        cindex_missing = 0;
        return 0;
    }

    LONGTAIL_FREE(cindex_remote);
    cindex_remote = 0;
    LONGTAIL_FREE(cindex_missing);
    cindex_missing = 0;

    if (!PrintFormattedBlockList(*request_content->m_BlockCount, request_content->m_BlockHashes, output_format))
    {
        LONGTAIL_FREE(request_content);
        request_content = 0;
        return 0;
    }

    LONGTAIL_FREE(request_content);
    request_content = 0;

    return 1;
}

int main(int argc, char** argv)
{
    int result = 0;
    Longtail_SetAssert(AssertFailure);

    int32_t target_chunk_size = 8;
    kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

    int32_t max_chunks_per_block = 0;
    kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

    int32_t target_block_size = 0;
    kgflags_int("target-block-size", 32768 * 8, "Target block size", false, &target_block_size);

    const char* create_version_index_raw = 0;
    kgflags_string("create-version-index", 0, "Path to version index output", false, &create_version_index_raw);

    const char* version_raw = 0;
    kgflags_string("version", 0, "Path to version assets input", false, &version_raw);

    const char* filter_raw = 0;
    kgflags_string("filter", 0, "Path to filter file input", false, &filter_raw);

    const char* create_content_index_raw = 0;
    kgflags_string("create-content-index", 0, "Path to content index output", false, &create_content_index_raw);

    const char* version_index_raw = 0;
    kgflags_string("version-index", 0, "Path to version index input", false, &version_index_raw);

    const char* content_raw = 0;
    kgflags_string("content", 0, "Path to content block input", false, &content_raw);
    
    const char* create_content_raw = 0;
    kgflags_string("create-content", 0, "Path to content block output", false, &create_content_raw);

    const char* content_index_raw = 0;
    kgflags_string("content-index", 0, "Path to content index input", false, &content_index_raw);

    const char* merge_content_index_raw = 0;
    kgflags_string("merge-content-index", 0, "Path to base content index", false, &merge_content_index_raw);

    const char* create_version_raw = 0;
    kgflags_string("create-version", 0, "Path to version", false, &create_version_raw);

    const char* update_version_raw = 0;
    kgflags_string("update-version", 0, "Path to version", false, &update_version_raw);

    const char* target_version_raw = 0;
    kgflags_string("target-version", 0, "Path to target version", false, &target_version_raw);

    const char* target_version_index_raw = 0;
    kgflags_string("target-version-index", 0, "Path to target version index", false, &target_version_index_raw);

    const char* list_missing_blocks_raw = 0;
    kgflags_string("list-missing-blocks", 0, "Path to content index", false, &list_missing_blocks_raw);

    bool upsync = false;
    kgflags_bool("upsync", false, "", false, &upsync);

    const char* upload_content_raw = 0;
    kgflags_string("upload-content", 0, "Path to write new content block", false, &upload_content_raw);

    bool downsync = false;
    kgflags_bool("downsync", false, "", false, &downsync);

    const char* remote_content_index_raw = 0;
    kgflags_string("remote-content-index", 0, "Path to write new content block", false, &remote_content_index_raw);

    const char* remote_content_raw = 0;
    kgflags_string("remote-content", 0, "Path to write new content block", false, &remote_content_raw);

    const char* output_format = 0;
    kgflags_string("output-format", 0, "Path to write new content block", false, &output_format);

    const char* test_version_raw = 0;
    kgflags_string("test-version", 0, "Test everything", false, &test_version_raw);

    const char* test_base_path_raw = 0;
    kgflags_string("test-base-path", 0, "Base path for test everything", false, &test_base_path_raw);

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
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);
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
            fprintf(stderr, "Failed to create content `%s` from `%s`\n", create_content, version);
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
/*
        char create_version[512];
        sprintf(create_version, "%s/remote/%s", test_base_path, test_version);
        sprintf(content, "%s/chunks", test_base_path);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        sprintf(version_index, "%s/%s.lvi", test_base_path, test_version);
        if (!Cmd_CreateVersion(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            create_version,
            version_index,
            content,
            content_index))
        {
            fprintf(stderr, "Failed to create version `%s` to `%s`\n", create_version, version_index);
            return 1;
        }
*/
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
            fprintf(stderr, "Failed to update version `%s` to `%s`\n", update_version, target_version_index);
            return 1;
        }

        char incremental_version_index[512];
        sprintf(incremental_version_index, "%s/remote/%s.lvi", test_base_path, test_version);
        char incremental_version[512];
        sprintf(incremental_version, "%s/remote/incremental", test_base_path);
        if (!Cmd_CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            incremental_version_index,
            incremental_version,
            0,
            target_chunk_size))
        {
            return 1;
        }

        struct VersionIndex* source_vindex = ReadVersionIndex(&storage_api.m_StorageAPI, create_version_index);
        if (!source_vindex)
        {
            return 1;
        }
        struct VersionIndex* target_vindex = ReadVersionIndex(&storage_api.m_StorageAPI, incremental_version_index);
        if (!target_vindex)
        {
            LONGTAIL_FREE(source_vindex);
            return 1;
        }

        struct VersionDiff* diff = CreateVersionDiff(
            source_vindex,
            target_vindex);
        LONGTAIL_FREE(source_vindex);
        LONGTAIL_FREE(target_vindex);
        if (*diff->m_SourceRemovedCount != 0)
        {
            return 1;
        }
        if (*diff->m_TargetAddedCount != 0)
        {
            return 1;
        }
        if (*diff->m_ModifiedCount != 0)
        {
            return 1;
        }
        LONGTAIL_FREE(diff);

        char verify_content_index[512];
        sprintf(verify_content_index, "%s/%s.lci", test_base_path, test_version);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
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

        free((char*)test_version);
        free((char*)test_base_path);

        printf("********* SUCCESS *********\n");
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
    const char* upload_content = NormalizePath(upload_content_raw);
    const char* remote_content_index = NormalizePath(remote_content_index_raw);
    const char* remote_content = NormalizePath(remote_content_raw);

    if (create_version_index && version)
    {
        if (filter)
        {
            fprintf(stderr, "--filter option not yet supported\n");
            result = 1;
            goto end;
        }

        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);

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
            BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);
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
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);

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
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);
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
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);

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
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);

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

    if (upsync)
    {
        if (!version)
        {
            fprintf(stderr, "--upsync requires a --version path\n");
            result = 1;
            goto end;
        }
        if (!version_index)
        {
            fprintf(stderr, "--upsync requires a --version-index path\n");
            result = 1;
            goto end;
        }
        if (!content_index)
        {
            fprintf(stderr, "--upsync requires a --content-index path\n");
            result = 1;
            goto end;
        }
        if (!upload_content)
        {
            fprintf(stderr, "--upsync requires a --upload-content path\n");
            result = 1;
            goto end;
        }
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api(GetCPUCount());
        if(!Cmd_UpSyncVersion(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            version,
            version_index,
            content,
            content_index,
            upload_content,
            output_format ? output_format : "{blockname}",
            max_chunks_per_block,
            target_block_size,
            target_chunk_size))
        {
            // TODO: printf
            result = 1;
            goto end;
        }
    }

    if (downsync)
    {
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        BikeshedJobAPI job_api(GetCPUCount());    // We oversubscribe with 1 (workers + main thread) since a lot of our time will be spent waitig for IO);

        if (!target_version_index)
        {
            fprintf(stderr, "--downsync requires a --target-version-index path\n");
            result = 1;
            goto end;
        }
        if (!content_index && !content)
        {
            fprintf(stderr, "--downsync requires a --content-index or --content path\n");
            result = 1;
            goto end;
        }
        if (!remote_content_index && !remote_content)
        {
            fprintf(stderr, "--downsync requires a --remote-content-index or --remote-content path\n");
            result = 1;
            goto end;
        }

        int ok = Cmd_DownSyncVersion(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            &job_api.m_JobAPI,
            &compression_api.m_CompressionAPI,
            target_version_index,
            content_index,
            content,
            remote_content_index,
            remote_content,
            output_format ? output_format : "{blockname}",
            max_chunks_per_block,
            target_block_size,
            target_chunk_size);

        if (!ok)
        {
            // TODO: printf
            result = 1;
            goto end;
        }
        return 0;
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
    free((void*)upload_content);
    free((void*)remote_content_index);
    free((void*)remote_content);
    return result;
}
