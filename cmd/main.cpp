
#define KGFLAGS_IMPLEMENTATION
#include "../third-party/kgflags/kgflags.h"

#define BIKESHED_IMPLEMENTATION
#include "../common/platform.h"
#include "../src/longtail.h"

#include <stdio.h>

// Temporary content tag
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

int main(int argc, char** argv)
{
    const char* create_version_index = NULL;
    kgflags_string("create-version-index", NULL, "Path to version index output", false, &create_version_index);

    const char* version = NULL;
    kgflags_string("version", NULL, "Path to version assets input", false, &version);

    const char* filter = NULL;
    kgflags_string("filter", NULL, "Path to filter file input", false, &filter);

    const char* create_content_index = NULL;
    kgflags_string("create-content-index", NULL, "Path to content index output", false, &create_content_index);

    const char* version_index = NULL;
    kgflags_string("version-index", NULL, "Path to version index input", false, &version_index);

    const char* content = NULL;
    kgflags_string("content", NULL, "Path to content block input", false, &content);
    
    const char* create_content = NULL;
    kgflags_string("create-content", NULL, "Path to content block output", false, &create_content);

    const char* content_index = NULL;
    kgflags_string("content-index", NULL, "Path to content index input", false, &content_index);

    const char* merge_content_index = NULL;
    kgflags_string("merge-content-index", NULL, "Path to base content index", false, &merge_content_index);

    const char* create_version = NULL;
    kgflags_string("create-version", NULL, "Path to version index", false, &create_version);

    if (!kgflags_parse(argc, argv)) {
        kgflags_print_errors();
        kgflags_print_usage();
        return 1;
    }
    
    if (create_version_index && version)
    {
        if (filter)
        {
            printf("--filter option not yet supported\n");
            return 1;
        }
        Shed shed;
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        Paths* version_paths = GetFilesRecursively(
            &storage_api.m_StorageAPI,
            version);
        if (!version_paths)
        {
            printf("Failed to scan folder `%s`\n", version);
            return 1;
        }
        VersionIndex* version_index = CreateVersionIndex(
            &storage_api.m_StorageAPI,
            &hash_api.m_HashAPI,
            shed.m_Shed,
            version,
            version_paths);
        free(version_paths);
        if (!version_index)
        {
            printf("Failed to create version index for `%s`\n", version);
            return 1;
        }
        int ok = WriteVersionIndex(&storage_api.m_StorageAPI, version_index, create_version_index);
        free(version_index);
        if (!ok)
        {
            printf("Failed to create version index to `%s`\n", create_version_index);
            return 1;
        }
        return 0;
    }

    if (create_content_index && !version)
    {
        if (content && (!content_index && !merge_content_index))
        {
            TroveStorageAPI storage_api;
            ContentIndex* cindex = ReadContent(
                &storage_api.m_StorageAPI,
                content);
            if (!cindex)
            {
                printf("Failed to create content index for content `%s`\n", content);
                return 1;
            }
            int ok = WriteContentIndex(&storage_api.m_StorageAPI, cindex, create_content_index);
            free(cindex);
            if (!ok)
            {
                printf("Failed to write content index to `%s`\n", create_content_index);
                return 1;
            }
            return 0;
        }
        if (content_index && merge_content_index)
        {
            TroveStorageAPI storage_api;
            ContentIndex* cindex1 = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex1)
            {
                printf("Failed to read content index from `%s`\n", content_index);
                return 1;
            }
            ContentIndex* cindex2 = ReadContentIndex(&storage_api.m_StorageAPI, merge_content_index);
            if (!cindex2)
            {
                free(cindex1);
                printf("Failed to read content index from `%s`\n", merge_content_index);
                return 1;
            }
            ContentIndex* cindex = MergeContentIndex(cindex1, cindex2);
            free(cindex2);
            free(cindex1);

            if (!cindex)
            {
                printf("Failed to merge content index `%s` with `%s`\n", content_index, merge_content_index);
                return 1;
            }

            int ok = WriteContentIndex(&storage_api.m_StorageAPI, cindex, create_content_index);
            free(cindex);

            if (!ok)
            {
                printf("Failed to write content index to `%s`\n", create_content_index);
                return 1;
            }
            return 0;
        }
    }

    if (create_content_index && version)
    {
        Shed shed;
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;

        VersionIndex* vindex = 0;
        if (version_index)
        {
            vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
            if (!vindex)
            {
                printf("Failed to read version index from `%s`\n", version_index);
                return 1;
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
                return 1;
            }
            vindex = CreateVersionIndex(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                shed.m_Shed,
                version,
                version_paths);
            free(version_paths);
            if (!vindex)
            {
                printf("Failed to create version index for version `%s`\n", version);
                return 1;
            }
        }

        ContentIndex* cindex = CreateContentIndex(
            &hash_api.m_HashAPI,
            version,
            *vindex->m_AssetCount,
            vindex->m_AssetContentHash,
            vindex->m_PathHash,
            vindex->m_AssetSize,
            vindex->m_NameOffset,
            vindex->m_NameData,
            GetContentTag);

        free(vindex);
        if (!cindex)
        {
            printf("Failed to create content index for version `%s`\n", version);
            return 1;
        }

        int ok = WriteContentIndex(
            &storage_api.m_StorageAPI,
            cindex,
            create_content_index);

        free(cindex);

        if (!ok)
        {
            printf("Failed to write content index to `%s`\n", create_content_index);
            return 1;
        }

        return 0;
    }
    
    if (create_content && version)
    {
        Shed shed;
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;
        VersionIndex* vindex = 0;
        if (version_index)
        {
            vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
            if (!vindex)
            {
                printf("Failed to read version index from `%s`\n", version_index);
                return 1;
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
                return 1;
            }
            vindex = CreateVersionIndex(
                &storage_api.m_StorageAPI,
                &hash_api.m_HashAPI,
                shed.m_Shed,
                version,
                version_paths);
            free(version_paths);
            if (!vindex)
            {
                printf("Failed to create version index for version `%s`\n", version);
                return 1;
            }
        }

        ContentIndex* cindex = 0;
        if (content_index)
        {
            cindex = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex)
            {
                printf("Failed to read content index from `%s`\n", content_index);
                return 1;
            }
        }
        else
        {
            cindex = CreateContentIndex(
                &hash_api.m_HashAPI,
                version,
                *vindex->m_AssetCount,
                vindex->m_AssetContentHash,
                vindex->m_PathHash,
                vindex->m_AssetSize,
                vindex->m_NameOffset,
                vindex->m_NameData,
                GetContentTag);
            if (!cindex)
            {
                printf("Failed to create content index for version `%s`\n", version);
                return 1;
            }
        }
        
        PathLookup* path_lookup = CreateContentHashToPathLookup(vindex, 0);
        int ok = WriteContent(
            &storage_api.m_StorageAPI,
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            cindex,
            path_lookup,
            version,
            create_content);
        free(vindex);
        free(cindex);

        if (!ok)
        {
            printf("Failed to write content to `%s`\n", create_content);
            return 1;
        }
        return 0;
    }

    if (create_version && version_index && content)
    {
        Shed shed;
        TroveStorageAPI storage_api;
        MeowHashAPI hash_api;
        LizardCompressionAPI compression_api;

        VersionIndex* vindex = ReadVersionIndex(&storage_api.m_StorageAPI, version_index);
        if (!vindex)
        {
            printf("Failed to read version index from `%s`\n", version_index);
            return 1;
        }

        ContentIndex* cindex = 0;
        if (content_index)
        {
            cindex = ReadContentIndex(&storage_api.m_StorageAPI, content_index);
            if (!cindex)
            {
                free(vindex);
                printf("Failed to read content index from `%s`\n", content_index);
                return 1;
            }
        }
        else
        {
            cindex = ReadContent(
                &storage_api.m_StorageAPI,
                content);
            if (!cindex)
            {
                free(vindex);
                printf("Failed to create content index for version `%s`\n", version);
                return 1;
            }
        }
        int ok = ReconstructVersion(
            &storage_api.m_StorageAPI,
            &compression_api.m_CompressionAPI,
            shed.m_Shed,
            cindex,
            vindex,
            content,
            create_version);
        free(vindex);
        free(cindex);
        if (!ok)
        {
            printf("Failed to create version `%s`\n", create_version);
            return 1;
        }
        return 0;
    }

    kgflags_print_usage();
    return 1;
}
