#include "../src/longtail.h"
#include "../lib/longtail_lib.h"
#include "../lib/longtail_meowhash.h"
#include "../lib/longtail_platform.h"
#include "../src/stb_ds.h"

#define KGFLAGS_IMPLEMENTATION
#include "../third-party/kgflags/kgflags.h"

#include <stdio.h>
#include <inttypes.h>
#include <stdarg.h>

static void AssertFailure(const char* expression, const char* file, int line)
{
    fprintf(stderr, "%s(%d): Assert failed `%s`\n", file, line, expression);
    exit(-1);
}

static const char* ERROR_LEVEL[4] = {"DEBUG", "INFO", "WARNING", "ERROR"};

static void LogStdErr(void* , int level, const char* log)
{
    fprintf(stderr, "%s: %s\n", ERROR_LEVEL[level], log);
}

static int CreateParentPath(struct Longtail_StorageAPI* storage_api, const char* path);

static int CreatePath(struct Longtail_StorageAPI* storage_api, const char* path)
{
    if (storage_api->IsDir(storage_api, path))
    {
        return 0;
    }
    else
    {
        int err = CreateParentPath(storage_api, path);
        if (err)
        {
            return err;
        }
        err = storage_api->CreateDir(storage_api, path);
        if (err)
        {
            return err;
        }
    }
    return 0;
}

static int CreateParentPath(struct Longtail_StorageAPI* storage_api, const char* path)
{
    char* dir_path = Longtail_Strdup(path);
    char* last_path_delimiter = (char*)strrchr(dir_path, '/');
    if (last_path_delimiter == 0)
    {
        Longtail_Free(dir_path);
        return 0;
    }
    while (last_path_delimiter > dir_path && last_path_delimiter[-1] == '/')
    {
        --last_path_delimiter;
    }
    *last_path_delimiter = '\0';
    int err = CreatePath(storage_api, dir_path);
    Longtail_Free(dir_path);
    return err;
}

static char* NormalizePath(const char* path)
{
    if (!path)
    {
        return 0;
    }
    char* normalized_path = Longtail_Strdup(path);
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

struct HashToIndex
{
    TLongtail_Hash key;
    uint64_t value;
};

static int PrintFormattedBlockList(Longtail_ContentIndex* content_index, const char* format_string)
{
    const char* format_start = format_string;
    const char* format_first_end = strstr(format_string, "{blockname}");
    if (!format_first_end)
    {
        return 0;
    }
    Longtail_Paths* paths;
    int err = Longtail_GetPathsForContentBlocks(content_index, &paths);
    if (err)
    {
        return 0;
    }
    size_t first_length = (size_t)((intptr_t)format_first_end - (intptr_t)format_start);
    const char* format_second_start = &format_first_end[strlen("{blockname}")];
    for (uint64_t b = 0; b < *content_index->m_BlockCount; ++b)
    {
        const char* block_name = &paths->m_Data[paths->m_Offsets[b]];

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

static uint32_t* GetCompressionTypes(Longtail_StorageAPI* , const Longtail_FileInfos* file_infos)
{
    uint32_t count = *file_infos->m_Paths.m_PathCount;
    uint32_t* result = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * count);
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
            result[i] = LONGTAIL_NO_COMPRESSION_TYPE;
            continue;
        }
        result[i] = LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE;
    }
    return result;
}





static int Cmd_Longtail_CreateVersionIndex(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    const char* create_version_index,
    const char* version,
    const char* filter,
    int target_chunk_size)
{
    struct Longtail_FileInfos* file_infos;
    int err = Longtail_GetFilesRecursively(
        storage_api,
        version,
        &file_infos);
    if (err)
    {
        fprintf(stderr, "Failed to scan folder `%s`, %d\n", version, err);
        return 0;
    }
    uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
    if (!compression_types)
    {
        fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
        Longtail_Free(file_infos);
        return 0;
    }

    Longtail_VersionIndex* vindex = 0;
    {
        Progress progress("Indexing version");
        err =Longtail_CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size,
            &vindex);
    }
    Longtail_Free(compression_types);
    compression_types = 0;
    Longtail_Free(file_infos);
    file_infos = 0;
    if (err)
    {
        fprintf(stderr, "Failed to create version index for `%s`, %d\n", version, err);
        return 0;
    }

    int ok = (0 == CreateParentPath(storage_api, create_version_index)) && (0 == Longtail_WriteVersionIndex(storage_api, vindex, create_version_index));

    Longtail_VersionIndex* target_vindex;
    err = Longtail_ReadVersionIndex(storage_api, create_version_index, &target_vindex);
    if (err)
    {
        Longtail_Free(vindex);
        vindex = 0;
        fprintf(stderr, "Failed to read version index from `%s`, %d\n", create_version_index, err);
        return 0;
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(
        vindex,
        target_vindex,
        &version_diff);

    Longtail_Free(version_diff);
    version_diff = 0;

    Longtail_Free(target_vindex);
    target_vindex = 0;

    Longtail_Free(vindex);
    vindex = 0;
    if (err)
    {
        fprintf(stderr, "Failed to create version index to `%s`, %d\n", create_version_index, err);
        return 0;
    }

    return 1;
}

static int Cmd_Longtail_CreateContentIndex(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    const char* create_content_index,
    const char* content)
{
    Longtail_ContentIndex* cindex = 0;
    if (!content)
    {
        int err = Longtail_CreateContentIndex(
            hash_api,
            0,
            0,
            0,
            0,
            0,
            0,
            &cindex);
        if (err)
        {
            fprintf(stderr, "Failed to create empty content indexm %d\n", err);
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        int err = Longtail_ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content,
            &cindex);
        if (err)
        {
            fprintf(stderr, "Failed to create content index for `%s`, %d\n", content, err);
            return 0;
        }
    }
    int ok = (0 == CreateParentPath(storage_api, create_content_index)) &&
        (0 == Longtail_WriteContentIndex(
            storage_api,
            cindex,
            create_content_index));

    Longtail_Free(cindex);
    cindex = 0;
    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

static int Cmd_MergeContentIndex(
    Longtail_StorageAPI* storage_api,
    const char* create_content_index,
    const char* content_index,
    const char* merge_content_index)
{
    Longtail_ContentIndex* cindex1;
    int err = Longtail_ReadContentIndex(storage_api, content_index, &cindex1);
    if (err)
    {
        fprintf(stderr, "Failed to read content index from `%s`, %d\n", content_index, err);
        return 0;
    }
    Longtail_ContentIndex* cindex2;
    err = Longtail_ReadContentIndex(storage_api, merge_content_index, &cindex2);
    if (err)
    {
        Longtail_Free(cindex1);
        cindex1 = 0;
        fprintf(stderr, "Failed to read content index from `%s`, %d\n", merge_content_index, err);
        return 0;
    }
    Longtail_ContentIndex* cindex;
    err = Longtail_MergeContentIndex(cindex1, cindex2, &cindex);
    Longtail_Free(cindex2);
    cindex2 = 0;
    Longtail_Free(cindex1);
    cindex1 = 0;

    if (err)
    {
        fprintf(stderr, "Failed to merge content index `%s` with `%s`, %d\n", content_index, merge_content_index, err);
        return 0;
    }

    int ok = (0 == CreateParentPath(storage_api, create_content_index)) &&
        (0 == Longtail_WriteContentIndex(
            storage_api,
            cindex,
            create_content_index));

    Longtail_Free(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

static int Cmd_Longtail_CreateMissingContentIndex(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    const char* create_content_index,
    const char* content_index,
    const char* content,
    const char* version_index,
    const char* version,
    int target_block_size,
    int max_chunks_per_block,
    int target_chunk_size)
{
    Longtail_VersionIndex* vindex = 0;
    if (version_index)
    {
        int err = Longtail_ReadVersionIndex(storage_api, version_index, &vindex);
        if (err)
        {
            fprintf(stderr, "Failed to read version index from `%s`, %d\n", version_index, err);
            return 0;
        }
    }
    else
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            version,
            &file_infos);
        if (err)
        {
            fprintf(stderr, "Failed to scan folder `%s`, %d\n", version, err);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
            Longtail_Free(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        err = Longtail_CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size,
            &vindex);
        Longtail_Free(compression_types);
        compression_types = 0;
        Longtail_Free(file_infos);
        file_infos = 0;
        if (err)
        {
            fprintf(stderr, "Failed to create version index for version `%s`, %d\n", version, err);
            return 0;
        }
    }

    Longtail_ContentIndex* existing_cindex = 0;
    if (content_index)
    {
        int err = Longtail_ReadContentIndex(storage_api, content_index, &existing_cindex);
        if (err)
        {
            Longtail_Free(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`, %d\n", content_index, err);
            return 0;
        }
    }
    else if (content)
    {
        Progress progress("Reading content");
        int err = Longtail_ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content,
            &existing_cindex);
        if (err)
        {
            Longtail_Free(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read contents from `%s`, %d\n", content, err);
            return 0;
        }
    }
    else
    {
        int err = Longtail_CreateContentIndex(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            &existing_cindex);
        if (err)
        {
            Longtail_Free(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to create empty contents from%d\n", err);
            return 0;
        }
    }

    Longtail_ContentIndex* cindex;
    int err = Longtail_CreateMissingContent(
        hash_api,
        existing_cindex,
        vindex,
        target_block_size,
        max_chunks_per_block,
        &cindex);

    Longtail_Free(existing_cindex);
    existing_cindex = 0;
    Longtail_Free(vindex);
    vindex = 0;
    if (err)
    {
        fprintf(stderr, "Failed to create content index for version `%s`, %d\n", version, err);
        return 0;
    }

    int ok = (0 == CreateParentPath(storage_api, create_content_index)) &&
        (0 == Longtail_WriteContentIndex(
            storage_api,
            cindex,
            create_content_index));

    Longtail_Free(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content index to `%s`\n", create_content_index);
        return 0;
    }
    return 1;
}

static int Cmd_CreateContent(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    Longtail_CompressionRegistry* compression_registry,
    const char* create_content,
    const char* content_index,
    const char* version,
    const char* version_index,
    int target_block_size,
    int max_chunks_per_block,
    int target_chunk_size)
{
    Longtail_VersionIndex* vindex = 0;
    if (version_index)
    {
        int err = Longtail_ReadVersionIndex(storage_api, version_index, &vindex);
        if (err)
        {
            fprintf(stderr, "Failed to read version index from `%s`, %d\n", version_index, err);
            return 0;
        }
    }
    else
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            version,
            &file_infos);
        if (err)
        {
            fprintf(stderr, "Failed to scan folder `%s`, %d\n", version, err);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version);
            Longtail_Free(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        err = Longtail_CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size,
            &vindex);
        Longtail_Free(compression_types);
        compression_types = 0;
        Longtail_Free(file_infos);
        file_infos = 0;
        if (err)
        {
            fprintf(stderr, "Failed to create version index for version `%s`, %d\n", version, err);
            return 0;
        }
    }

    Longtail_ContentIndex* cindex = 0;
    if (content_index)
    {
        int err = Longtail_ReadContentIndex(storage_api, content_index, &cindex);
        if (err)
        {
            fprintf(stderr, "Failed to read content index from `%s`, %d\n", content_index, err);
            return 0;
        }
    }
    else
    {
        int err = Longtail_CreateContentIndex(
            hash_api,
            *vindex->m_ChunkCount,
            vindex->m_ChunkHashes,
            vindex->m_ChunkSizes,
            vindex->m_ChunkCompressionTypes,
            target_block_size,
            max_chunks_per_block,
            &cindex);
        if (err)
        {
            fprintf(stderr, "Failed to create content index for version `%s`, %d\n", version, err);
            Longtail_Free(vindex);
            vindex = 0;
            return 0;
        }
    }

    int err = Longtail_ValidateVersion(
        cindex,
        vindex);
    if (err)
    {
        Longtail_Free(cindex);
        cindex = 0;
        Longtail_Free(vindex);
        vindex = 0;
        fprintf(stderr, "Version `%s` does not fully encompass content `%s`, %d\n", version, create_content, err);
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Writing content");
        ok = (0 == CreatePath(storage_api, create_content)) && (0 == Longtail_WriteContent(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            Progress::ProgressFunc,
            &progress,
            cindex,
            vindex,
            version,
            create_content));
    }

    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to write content to `%s`\n", create_content);
        return 0;
    }
    return 1;
}
/*
int Cmd_ListMissingBlocks(
    Longtail_StorageAPI* storage_api,
    const char* list_missing_blocks,
    const char* content_index)
{
    Longtail_ContentIndex* have_content_index;
    int err = Longtail_ReadContentIndex(storage_api, list_missing_blocks. &have_content_index);
    if (err)
    {
        return 0;
    }
    Longtail_ContentIndex* need_content_index;
    err = Longtail_ReadContentIndex(storage_api, content_index, &need_content_index);
    if (err)
    {
        Longtail_Free(have_content_index);
        have_content_index = 0;
        return 0;
    }

    struct HashToIndex* chunk_hash_to_have_block_index = 0;

    for (uint32_t i = 0; i < (uint32_t)*have_content_index->m_ChunkCount; ++i)
    {
        hmput(chunk_hash_to_have_block_index, have_content_index->m_ChunkHashes[i], i);
    }

    TLongtail_Hash* missing_block_hashes = 0;
    arrsetcap(missing_block_hashes, *need_content_index->m_BlockCount);

    struct HashToIndex* need_block_index_to_chunk_count = 0;

    for (uint32_t i = 0; i < *need_content_index->m_ChunkCount; ++i)
    {
        intptr_t have_block_index_ptr = hmgeti(chunk_hash_to_have_block_index, need_content_index->m_ChunkHashes[i]);
        if (have_block_index_ptr != -1)
        {
            continue;
        }
        uint64_t block_index = need_content_index->m_ChunkBlockIndexes[i];
        TLongtail_Hash block_hash = need_content_index->m_BlockHashes[block_index];
        intptr_t need_block_index_ptr = hmgeti(need_block_index_to_chunk_count, need_content_index->m_ChunkBlockIndexes[i]);
        if (need_block_index_ptr != -1)
        {
            continue;
        }
        hmput(need_block_index_to_chunk_count, need_content_index->m_ChunkBlockIndexes[i], 1u);
        arrpush(missing_block_hashes, block_hash);
    }

    hmfree(need_block_index_to_chunk_count);
    need_block_index_to_chunk_count = 0;
    hmfree(chunk_hash_to_have_block_index);
    chunk_hash_to_have_block_index = 0;

    Longtail_Free(need_content_index);
    need_content_index = 0;
    Longtail_Free(have_content_index);
    have_content_index = 0;

    uint64_t missing_block_count = arrlen(missing_block_hashes);
    if (missing_block_count == 0)
    {
        arrfree(missing_block_hashes);
        missing_block_hashes = 0;
        return 0;
    }
    int ok = PrintFormattedBlockList(missing_block_count, missing_block_hashes, "{blockname}");
    arrfree(missing_block_hashes);
    missing_block_hashes = 0;
    return ok;
}
*/
static int Cmd_CreateVersion(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    Longtail_CompressionRegistry* compression_registry,
    const char* create_version,
    const char* version_index,
    const char* content,
    const char* content_index)
{
    Longtail_VersionIndex* vindex;
    int err = Longtail_ReadVersionIndex(storage_api, version_index, &vindex);
    if (err)
    {
        fprintf(stderr, "Failed to read version index from `%s`, %d\n", version_index, err);
        return 0;
    }

    Longtail_ContentIndex* cindex = 0;
    if (content_index)
    {
        err = Longtail_ReadContentIndex(storage_api, content_index, &cindex);
        if (err)
        {
            Longtail_Free(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`, %d\n", content_index, err);
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        int err = Longtail_ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content,
            &cindex);
        if (err)
        {
            Longtail_Free(vindex);
            vindex = 0;
            fprintf(stderr, "Failed to create content index for `%s`, %d\n", content, err);
            return 0;
        }
    }

    err = Longtail_ValidateContent(
        cindex,
        vindex);
    if (err) {
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        fprintf(stderr, "Content `%s` does not fully encompass version `%s`m %d\n", content, create_version, err);
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Writing version");
        ok = (0 == CreatePath(storage_api, create_version)) && (0 == Longtail_WriteVersion(
            storage_api,
            storage_api,
            compression_registry,
            job_api,
            Progress::ProgressFunc,
            &progress,
            cindex,
            vindex,
            content,
            create_version));
    }
    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;
    if (!ok)
    {
        fprintf(stderr, "Failed to create version `%s`\n", create_version);
        return 0;
    }
    return 1;
}

static int Cmd_UpdateVersion(
    Longtail_StorageAPI* storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    Longtail_CompressionRegistry* compression_registry,
    const char* update_version,
    const char* version_index,
    const char* content,
    const char* content_index,
    const char* target_version_index,
    int target_chunk_size)
{
    Longtail_VersionIndex* source_vindex = 0;
    if (version_index)
    {
        int err = Longtail_ReadVersionIndex(storage_api, version_index, &source_vindex);
        if (err)
        {
            fprintf(stderr, "Failed to read version index from `%s`, %d\n", version_index, err);
            return 0;
        }
    }
    else
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            update_version,
            &file_infos);
        if (err)
        {
            fprintf(stderr, "Failed to scan folder `%s`, %d\n", update_version, err);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(storage_api, file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", update_version);
            Longtail_Free(file_infos);
            return 0;
        }
        Progress progress("Indexing version");
        err = Longtail_CreateVersionIndex(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            update_version,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size,
            &source_vindex);
        Longtail_Free(compression_types);
        compression_types = 0;
        Longtail_Free(file_infos);
        file_infos = 0;
        if (err)
        {
            fprintf(stderr, "Failed to create version index for version `%s`, %d\n", update_version, err);
            return 0;
        }
    }

    Longtail_VersionIndex* target_vindex;
    int err = Longtail_ReadVersionIndex(storage_api, target_version_index, &target_vindex);
    if (err)
    {
        Longtail_Free(source_vindex);
        source_vindex = 0;
        fprintf(stderr, "Failed to read version index from `%s`, %d\n", target_version_index, err);
        return 0;
    }

    Longtail_ContentIndex* cindex = 0;
    if (content_index)
    {
        err = Longtail_ReadContentIndex(storage_api, content_index, &cindex);
        if (err)
        {
            Longtail_Free(target_vindex);
            target_vindex = 0;
            Longtail_Free(source_vindex);
            source_vindex = 0;
            fprintf(stderr, "Failed to read content index from `%s`, %d\n", content_index, err);
            return 0;
        }
    }
    else
    {
        Progress progress("Reading content");
        int err = Longtail_ReadContent(
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            content,
            &cindex);
        if (err)
        {
            Longtail_Free(target_vindex);
            target_vindex = 0;
            Longtail_Free(source_vindex);
            source_vindex = 0;
            fprintf(stderr, "Failed to create content index for `%s`, %d\n", content, err);
            return 0;
        }
    }

    err = Longtail_ValidateContent(
        cindex,
        target_vindex);
    if (err)
    {
        Longtail_Free(target_vindex);
        target_vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        fprintf(stderr, "Content `%s` does not fully encompass version `%s`, %d\n", content, target_version_index, err);
        return 0;
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(
        source_vindex,
        target_vindex,
        &version_diff);
    if (err)
    {
        Longtail_Free(cindex);
        cindex = 0;
        Longtail_Free(target_vindex);
        target_vindex = 0;
        Longtail_Free(source_vindex);
        source_vindex = 0;
        fprintf(stderr, "Failed to create version diff from `%s` to `%s`, %d\n", version_index, target_version_index, err);
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Updating version");
        ok = (0 == CreatePath(storage_api, update_version)) && (0 == Longtail_ChangeVersion(
            storage_api,
            storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            compression_registry,
            cindex,
            source_vindex,
            target_vindex,
            version_diff,
            content,
            update_version));
    }

    Longtail_Free(cindex);
    cindex = 0;
    Longtail_Free(target_vindex);
    target_vindex = 0;
    Longtail_Free(source_vindex);
    source_vindex = 0;
    Longtail_Free(version_diff);
    version_diff = 0;

    if (!ok)
    {
        fprintf(stderr, "Failed to update version `%s` to `%s`\n", update_version, target_version_index);
        return 0;
    }
    return 1;
}

static int Cmd_UpSyncVersion(
    Longtail_StorageAPI* source_storage_api,
    Longtail_StorageAPI* target_storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    Longtail_CompressionRegistry* compression_registry,
    const char* version_path,
    const char* version_index_path,
    const char* content_path,
    const char* content_index_path,
    const char* missing_content_path,
    const char* missing_content_index_path,
    const char* output_format,
    int max_chunks_per_block,
    int target_block_size,
    int target_chunk_size)
{
    Longtail_VersionIndex* vindex;
    int err = Longtail_ReadVersionIndex(source_storage_api, version_index_path, &vindex);
    if (err)
    {
        if (err != ENOENT)
        {
            fprintf(stderr, "Failed to read version index from `%s`, %d\n", version_index_path, err);
            return 0;
        }
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            source_storage_api,
            version_path,
            &file_infos);
        if (err)
        {
            fprintf(stderr, "Failed to scan folder `%s`, %d\n", version_path, err);
            return 0;
        }
        uint32_t* compression_types = GetCompressionTypes(
            source_storage_api,
            file_infos);
        if (!compression_types)
        {
            fprintf(stderr, "Failed to get compression types for files in `%s`\n", version_path);
            Longtail_Free(file_infos);
            return 0;
        }

        Progress progress("Indexing version");
        err = Longtail_CreateVersionIndex(
            source_storage_api,
            hash_api,
            job_api,
            Progress::ProgressFunc,
            &progress,
            version_path,
            &file_infos->m_Paths,
            file_infos->m_FileSizes,
            compression_types,
            target_chunk_size,
            &vindex);
        Longtail_Free(compression_types);
        compression_types = 0;
        Longtail_Free(file_infos);
        file_infos = 0;
        if (err)
        {
            fprintf(stderr, "Failed to create version index for `%s`, %d\n", version_path, err);
            return 0;
        }
    }
    struct Longtail_ContentIndex* cindex = 0;
    if (content_index_path)
    {
        err = Longtail_ReadContentIndex(
            source_storage_api,
            content_index_path,
            &cindex);
    }
    if (err)
    {
        if (!content_path && content_index_path)
        {
            err = Longtail_CreateContentIndex(
                hash_api,
                0,
                0,
                0,
                0,
                target_block_size,
                max_chunks_per_block,
                &cindex);
            if (err)
            {
                fprintf(stderr, "Failed to create empty content index, %d\n", err);
                Longtail_Free(vindex);
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
            int err = Longtail_ReadContent(
                source_storage_api,
                hash_api,
                job_api,
                Progress::ProgressFunc,
                &progress,
                content_path,
                &cindex);
            if (err)
            {
                fprintf(stderr, "Failed to create content index for `%s`, %d\n", content_path, err);
                Longtail_Free(vindex);
                vindex = 0;
                return 0;
            }
        }
    }

    Longtail_ContentIndex* missing_content_index;
    err = Longtail_CreateMissingContent(
        hash_api,
        cindex,
        vindex,
        target_block_size,
        max_chunks_per_block,
        &missing_content_index);
    if (err)
    {
        fprintf(stderr, "Failed to generate content index for missing content, %d\n", err);
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }

    int ok = 0;
    {
        Progress progress("Writing content");
        ok = (0 == CreatePath(target_storage_api, missing_content_path)) && (0 == Longtail_WriteContent(
            source_storage_api,
            target_storage_api,
            compression_registry,
            job_api,
            Progress::ProgressFunc,
            &progress,
            missing_content_index,
            vindex,
            version_path,
            missing_content_path));
    }
    if (!ok)
    {
        fprintf(stderr, "Failed to create new content from `%s` to `%s`\n", version_path, missing_content_path);
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }

/*
    Longtail_ContentIndex* new_content_index;
    err = Longtail_MergeContentIndex(cindex, missing_content_index, &new_content_index);
    if (err)
    {
        fprintf(stderr, "Failed creating a new content index with the added content, %d\n", err);
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }
*/
    ok = (0 == CreateParentPath(target_storage_api, version_index_path)) && (0 == Longtail_WriteVersionIndex(
        target_storage_api,
        vindex,
        version_index_path));
    if (!ok)
    {
        fprintf(stderr, "Failed to write the new version index to `%s`\n", version_index_path);
/*        Longtail_Free(new_content_index);
        new_content_index = 0;*/
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }

    ok = (0 == CreateParentPath(target_storage_api, content_index_path)) && (0 == Longtail_WriteContentIndex(
        target_storage_api,
        missing_content_index,
        missing_content_index_path));
    if (!ok)
    {
        fprintf(stderr, "Failed to write the new version index to `%s`\n", version_index_path);
/*        Longtail_Free(new_content_index);
        new_content_index = 0;*/
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }

    if (!PrintFormattedBlockList(missing_content_index, output_format))
    {
        fprintf(stderr, "Failed to format block output using format `%s`\n", output_format);
        Longtail_Free(missing_content_index);
        missing_content_index = 0;
        Longtail_Free(vindex);
        vindex = 0;
        Longtail_Free(cindex);
        cindex = 0;
        return 0;
    }
/*    Longtail_Free(new_content_index);
    new_content_index = 0;
*/
    Longtail_Free(missing_content_index);
    missing_content_index = 0;
    Longtail_Free(vindex);
    vindex = 0;
    Longtail_Free(cindex);
    cindex = 0;

    fprintf(stderr, "Updated version index to `%s`\n", version_index_path);
    fprintf(stderr, "Wrote added content to `%s`\n", missing_content_path);
    fprintf(stderr, "Wrote added content index to `%s`\n", missing_content_index_path);

    return 1;
}

static int Cmd_DownSyncVersion(
    Longtail_StorageAPI* source_storage_api,
    Longtail_StorageAPI* target_storage_api,
    Longtail_HashAPI* hash_api,
    Longtail_JobAPI* job_api,
    Longtail_CompressionRegistry* compression_registry,
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
    Longtail_VersionIndex* vindex_target;
    int err = Longtail_ReadVersionIndex(source_storage_api, target_version_index_path, &vindex_target);
    if (err)
    {
        fprintf(stderr, "Failed to read version index from `%s`, %d\n", target_version_index_path, err);
        return 0;
    }

    Longtail_ContentIndex* existing_cindex = 0;
    if (have_content_index_path)
    {
        err = Longtail_ReadContentIndex(source_storage_api, have_content_index_path, &existing_cindex);
        if (err)
        {
            fprintf(stderr, "Failed to read content index from `%s`, %d\n", have_content_index_path, err);
            return 0;
        }
    }
    if (!existing_cindex)
    {
        if (!have_content_path)
        {
            // TODO: Print
            Longtail_Free(vindex_target);
            vindex_target = 0;
            return 0;
        }
        Progress progress("Reading content");
        int err = Longtail_ReadContent(
            source_storage_api,
            hash_api, job_api,
            Progress::ProgressFunc,
            &progress,
            have_content_path,
            &existing_cindex);
        if (err)
        {
            fprintf(stderr, "Failed to read content from `%s`, %d\n", have_content_path, err);
            Longtail_Free(vindex_target);
            vindex_target = 0;
            return 0;
        }
    }

    Longtail_ContentIndex* cindex_missing;
    err = Longtail_CreateMissingContent(
        hash_api,
        existing_cindex,
        vindex_target,
        target_block_size,
        max_chunks_per_block,
        &cindex_missing);
    if (err)
    {
        fprintf(stderr, "Failed to read create missing content for `%s` from `%s`, %d\n", target_version_index_path, have_content_path, err);
        Longtail_Free(existing_cindex);
        existing_cindex = 0;
        Longtail_Free(vindex_target);
        vindex_target = 0;
        return 0;
    }
    Longtail_Free(existing_cindex);
    existing_cindex = 0;
    Longtail_Free(vindex_target);
    vindex_target = 0;

    Longtail_ContentIndex* cindex_remote;
    err = Longtail_ReadContentIndex(source_storage_api, remote_content_index_path, &cindex_remote);
    if (err)
    {
        if (!remote_content_path)
        {
            //TODO: print
            Longtail_Free(cindex_missing);
            cindex_missing = 0;
            return 0;
        }
        Progress progress("Reading content");
        err = Longtail_ReadContent(
            source_storage_api,
            hash_api, job_api,
            Progress::ProgressFunc,
            &progress,
            remote_content_path,
            &cindex_remote);
        if (err)
        {
            fprintf(stderr, "Failed to read content from `%s`, %d\n", remote_content_path, err);
            Longtail_Free(cindex_missing);
            cindex_missing = 0;
            return 0;
        }
    }

    Longtail_ContentIndex* request_content;
    err = Longtail_RetargetContent(
        cindex_remote,
        cindex_missing,
        &request_content);
    if (err)
    {
        Longtail_Free(cindex_remote);
        cindex_remote = 0;
        Longtail_Free(cindex_missing);
        cindex_missing = 0;
        return 0;
    }

    Longtail_Free(cindex_remote);
    cindex_remote = 0;
    Longtail_Free(cindex_missing);
    cindex_missing = 0;

    if (!PrintFormattedBlockList(request_content, output_format))
    {
        Longtail_Free(request_content);
        request_content = 0;
        return 0;
    }

    Longtail_Free(request_content);
    request_content = 0;

    return 1;
}

int main(int argc, char** argv)
{
    int result = 0;
    Longtail_SetAssert(AssertFailure);
    Longtail_SetLog(LogStdErr, 0);

    int32_t target_chunk_size = 8;
    kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

    int32_t max_chunks_per_block = 0;
    kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

    int32_t target_block_size = 0;
    kgflags_int("target-block-size", 32768 * 12, "Target block size", false, &target_block_size);

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

//    const char* list_missing_blocks_raw = 0;
//    kgflags_string("list-missing-blocks", 0, "Path to content index", false, &list_missing_blocks_raw);

    bool upsync = false;
    kgflags_bool("upsync", false, "", false, &upsync);

    const char* missing_content_raw = 0;
    kgflags_string("missing-content", 0, "Path to write new content blocks", false, &missing_content_raw);

    const char* missing_content_index_raw = 0;
    kgflags_string("missing-content-index", 0, "Path to write new content block", false, &missing_content_index_raw);

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

    int log_level = 2;
    kgflags_int("log-level", 2, "log level - 0 is full logs", false, &log_level);

    if (!kgflags_parse(argc, argv)) {
        kgflags_print_errors();
        kgflags_print_usage();
        return 1;
    }

    Longtail_SetLogLevel(log_level);

    Longtail_CompressionRegistry* compression_registry = Longtail_CreateDefaultCompressionRegistry();
    Longtail_StorageAPI* fs_storage_api = Longtail_CreateFSStorageAPI();
    Longtail_HashAPI* hash_api = Longtail_CreateMeowHashAPI();
    Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount());

    if (test_version_raw && test_base_path_raw)
    {
        const char* test_version = NormalizePath(test_version_raw);
        const char* test_base_path = NormalizePath(test_base_path_raw);

        char create_content_index[512];
        sprintf(create_content_index, "%s/chunks.lci", test_base_path);
        char content[512];
        sprintf(content, "%s/chunks", test_base_path);
        if (!Cmd_Longtail_CreateContentIndex(
            fs_storage_api,
            hash_api,
            job_api,
            create_content_index,
            content))
        {
            return 1;
        }
        char create_version_index[512];
        sprintf(create_version_index, "%s/%s.lvi", test_base_path, test_version);
        char version[512];
        sprintf(version, "%s/local/%s", test_base_path, test_version);
        if (!Cmd_Longtail_CreateVersionIndex(
            fs_storage_api,
            hash_api,
            job_api,
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
        if (!Cmd_Longtail_CreateMissingContentIndex(
            fs_storage_api,
            hash_api,
            job_api,
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
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
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
            fs_storage_api,
            create_content_index,
            content_index,
            merge_content_index))
        {
            return 1;
        }
/*
        sprintf(create_version, "%s/remote/%s", test_base_path, test_version);
        sprintf(content, "%s/chunks", test_base_path);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        sprintf(version_index, "%s/%s.lvi", test_base_path, test_version);
        if (!Cmd_CreateVersion(
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
            create_version,
            version_index,
            content,
            content_index))
        {
            fprintf(stderr, "Failed to create version `%s` to `%s`\n", create_version, version_index);
            Longtail_Free(compression_registry);
            return 1;
        }
*/
        char update_version[512];
        sprintf(content, "%s/chunks", test_base_path);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        char target_version_index[512];
        sprintf(target_version_index, "%s/%s.lvi", test_base_path, test_version);
        if (!Cmd_UpdateVersion(
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
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
        if (!Cmd_Longtail_CreateVersionIndex(
            fs_storage_api,
            hash_api,
            job_api,
            incremental_version_index,
            incremental_version,
            0,
            target_chunk_size))
        {
            return 1;
        }

        struct Longtail_VersionIndex* source_vindex;
        int err = Longtail_ReadVersionIndex(fs_storage_api, create_version_index, &source_vindex);
        if (err)
        {
            fprintf(stderr, "Failed to read version index `%s`, %d\n", create_version_index, err);
            return 1;
        }
        struct Longtail_VersionIndex* target_vindex;
        err = Longtail_ReadVersionIndex(fs_storage_api, incremental_version_index, &target_vindex);
        if (err)
        {
            Longtail_Free(source_vindex);
            fprintf(stderr, "Failed to read version index `%s`, %d\n", incremental_version_index, err);
            return 1;
        }

        struct Longtail_VersionDiff* diff;
        err = Longtail_CreateVersionDiff(
            source_vindex,
            target_vindex,
            &diff);
        if (err)
        {
            Longtail_Free(target_vindex);
            Longtail_Free(source_vindex);
            fprintf(stderr, "Failed to create version diff between `%s` and `%s`, %d\n", create_version_index, incremental_version_index, err);
            return 1;
        }
        Longtail_Free(source_vindex);
        Longtail_Free(target_vindex);
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
        Longtail_Free(diff);

        char verify_content_index[512];
        sprintf(verify_content_index, "%s/%s.lci", test_base_path, test_version);
        sprintf(content_index, "%s/chunks.lci", test_base_path);
        sprintf(version_index, "%s/%s.lvi", test_base_path, test_version);
        sprintf(version, "%s/local/%s", test_base_path, test_version);
        if (!Cmd_Longtail_CreateMissingContentIndex(
            fs_storage_api,
            hash_api,
            job_api,
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

        Longtail_Free((char*)test_version);
        Longtail_Free((char*)test_base_path);

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
//    const char* list_missing_blocks = NormalizePath(list_missing_blocks_raw);
    const char* remote_content_index = NormalizePath(remote_content_index_raw);
    const char* remote_content = NormalizePath(remote_content_raw);
    const char* missing_content = NormalizePath(missing_content_raw);
    const char* missing_content_index = NormalizePath(missing_content_index_raw);


    if (create_version_index && version)
    {
        if (filter)
        {
            fprintf(stderr, "--filter option not yet supported\n");
            result = 1;
            goto end;
        }

        int ok = Cmd_Longtail_CreateVersionIndex(
            fs_storage_api,
            hash_api,
            job_api,
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
            int ok = Cmd_Longtail_CreateContentIndex(
                fs_storage_api,
                hash_api,
                job_api,
                create_content_index,
                content);
            result = ok ? 0 : 1;
            goto end;
        }
        if (content_index && merge_content_index)
        {
            int ok = Cmd_MergeContentIndex(
                fs_storage_api,
                create_content_index,
                content_index,
                merge_content_index);
            result = ok ? 0 : 1;
            goto end;
        }
    }

    if (create_content_index && version)
    {
        int ok = Cmd_Longtail_CreateMissingContentIndex(
            fs_storage_api,
            hash_api,
            job_api,
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
        int ok = Cmd_CreateContent(
                fs_storage_api,
                hash_api,
                job_api,
                compression_registry,
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

/*    if (list_missing_blocks && content_index)
    {
        int ok = Cmd_ListMissingBlocks(
            fs_storage_api,
            list_missing_blocks,
            content_index);
        result = ok ? 0 : 1;
        goto end;
    }*/

    if (create_version && version_index && content)
    {
        int ok = Cmd_CreateVersion(
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
            create_version,
            version_index,
            content,
            content_index);
        result = ok ? 0 : 1;
        goto end;
    }

    if (update_version && content && target_version_index)
    {
        int ok = Cmd_UpdateVersion(
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
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
        if (!content_index && !content)
        {
            fprintf(stderr, "--upsync requires either a --content or --content-index path\n");
            result = 1;
            goto end;
        }
        if (!missing_content)
        {
            fprintf(stderr, "--upsync requires a --missing-content path\n");
            result = 1;
            goto end;
        }
        if (!missing_content_index)
        {
            fprintf(stderr, "--upsync requires a --missing-content-index path\n");
            result = 1;
            goto end;
        }
        int ok = Cmd_UpSyncVersion(
            fs_storage_api,
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
            version,
            version_index,
            content,
            content_index,
            missing_content,
            missing_content_index,
            output_format ? output_format : "{blockname}",
            max_chunks_per_block,
            target_block_size,
            target_chunk_size);

        if (!ok){
            // TODO: printf
            result = 1;
            goto end;
        }
        result = 0;
        goto end;
    }

    if (downsync)
    {
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
            fs_storage_api,
            fs_storage_api,
            hash_api,
            job_api,
            compression_registry,
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
        result = 0;
        goto end;
    }

    kgflags_print_usage();
    return 1;

end:
    Longtail_DestroyJobAPI(job_api);
    Longtail_DestroyHashAPI(hash_api);
    Longtail_DestroyStorageAPI(fs_storage_api);
    Longtail_DestroyCompressionRegistry(compression_registry);

    Longtail_Free((void*)create_version_index);
    Longtail_Free((void*)version);
    Longtail_Free((void*)filter);
    Longtail_Free((void*)create_content_index);
    Longtail_Free((void*)version_index);
    Longtail_Free((void*)content);
    Longtail_Free((void*)create_content);
    Longtail_Free((void*)content_index);
    Longtail_Free((void*)merge_content_index);
    Longtail_Free((void*)create_version);
    Longtail_Free((void*)update_version);
    Longtail_Free((void*)target_version);
    Longtail_Free((void*)target_version_index);
//    Longtail_Free((void*)list_missing_blocks);
    Longtail_Free((void*)remote_content_index);
    Longtail_Free((void*)remote_content);
    Longtail_Free((void*)missing_content);
    Longtail_Free((void*)missing_content_index);
    return result;
}
