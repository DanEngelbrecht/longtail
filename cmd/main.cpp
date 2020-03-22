#if defined(_CRTDBG_MAP_ALLOC)
#include <cstdlib>
#include <crtdbg.h>
#endif

#include "../src/longtail.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/brotli/longtail_brotli.h"
#include "../lib/lizard/longtail_lizard.h"
#include "../lib/lz4/longtail_lz4.h"
#include "../lib/zstd/longtail_zstd.h"
#include "../lib/longtail_platform.h"

#define KGFLAGS_IMPLEMENTATION
#include "ext/kgflags.h"

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

struct Progress
{
    struct Longtail_ProgressAPI m_API;
    Progress(const char* task)
        : m_Task(task)
        , m_OldPercent(0)
        , m_JobsDone(0)
    {
        m_API.m_API.Dispose = 0;
        m_API.OnProgress = ProgressFunc;
    }
    ~Progress()
    {
        if (m_JobsDone != 0)
        {
            fprintf(stderr, " Done\n");
        }
    }
    const char* m_Task;
    uint32_t m_OldPercent;
    uint32_t m_JobsDone;
    static void ProgressFunc(struct Longtail_ProgressAPI* progress_api, uint32_t total, uint32_t jobs_done)
    {
        Progress* p = (Progress*)progress_api;
        if (jobs_done < total)
        {
            if (p->m_JobsDone == 0)
            {
                fprintf(stderr, "%s: ", p->m_Task);
            }
            uint32_t percent_done = (100 * jobs_done) / total;
            if (percent_done - p->m_OldPercent >= 5)
            {
                fprintf(stderr, "%u%% ", percent_done);
                p->m_OldPercent = percent_done;
            }
            p->m_JobsDone = jobs_done;
            return;
        }
        if (p->m_OldPercent != 0)
        {
            if (p->m_OldPercent != 100)
            {
                fprintf(stderr, "100%%");
            }
        }
        p->m_JobsDone = jobs_done;
    }
};

int ParseLogLevel(const char* log_level_raw) {
    if (0 == strcmp(log_level_raw, "info"))
    {
        return LONGTAIL_LOG_LEVEL_INFO;
    }
    if (0 == strcmp(log_level_raw, "debug"))
    {
        return LONGTAIL_LOG_LEVEL_DEBUG;
    }
    if (0 == strcmp(log_level_raw, "warn"))
    {
        return LONGTAIL_LOG_LEVEL_WARNING;
    }
    if (0 == strcmp(log_level_raw, "error"))
    {
        return LONGTAIL_LOG_LEVEL_ERROR;
    }
    return -1;
}

struct Longtail_HashAPI* CreateHashAPIFromIdentifier(uint32_t hash_type)
{
    if (hash_type == Longtail_GetBlake2HashType())
    {
        return Longtail_CreateBlake2HashAPI();
    }
    if (hash_type == Longtail_GetBlake3HashType())
    {
        return Longtail_CreateBlake3HashAPI();
    }
    if (hash_type == Longtail_GetMeowHashType())
    {
        return Longtail_CreateMeowHashAPI();
    }
    return 0;
}

const uint32_t LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE     = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'0');
const uint32_t LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'1');
const uint32_t LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE     = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'2');
const uint32_t LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE        = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'a');
const uint32_t LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE    = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'b');
const uint32_t LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE        = (((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8) + ((uint32_t)'c');

const uint32_t LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE     = (((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'1');
const uint32_t LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'2');
const uint32_t LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE     = (((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'3');

const uint32_t LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'l') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'4') << 8) + ((uint32_t)'2');

const uint32_t LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE = (((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'1');
const uint32_t LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'2');
const uint32_t LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE = (((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'3');

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
            result[i] = 0;
            continue;
        }
        result[i] = LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE;
    }
    return result;
}

Longtail_CompressionRegistryAPI* CreateDefaultCompressionRegistry()
{
    struct Longtail_CompressionAPI* lizard_compression = Longtail_CreateLizardCompressionAPI();
    if (lizard_compression == 0)
    {
        return 0;
    }

    struct Longtail_CompressionAPI* lz4_compression = Longtail_CreateLZ4CompressionAPI();
    if (lz4_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        return 0;
    }

    struct Longtail_CompressionAPI* brotli_compression = Longtail_CreateBrotliCompressionAPI();
    if (brotli_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        Longtail_DisposeAPI(&lz4_compression->m_API);
        return 0;
    }

    struct Longtail_CompressionAPI* zstd_compression = Longtail_CreateZStdCompressionAPI();
    if (zstd_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        Longtail_DisposeAPI(&lz4_compression->m_API);
        Longtail_DisposeAPI(&brotli_compression->m_API);
        return 0;
    }

    uint32_t compression_types[13] = {
        LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE,
        LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE,
        LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE,
        LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE,
        LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE,
        LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE,

        LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE,
        LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE,
        LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE,

        LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE,

        LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE,
        LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE,
        LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE};
    struct Longtail_CompressionAPI* compression_apis[13] = {
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        lizard_compression,
        lizard_compression,
        lizard_compression,
        lz4_compression,
        zstd_compression,
        zstd_compression,
        zstd_compression};
    Longtail_CompressionAPI_HSettings compression_settings[13] = {
        Longtail_GetBrotliGenericMinQuality(),
        Longtail_GetBrotliGenericDefaultQuality(),
        Longtail_GetBrotliGenericMaxQuality(),
        Longtail_GetBrotliTextMinQuality(),
        Longtail_GetBrotliTextDefaultQuality(),
        Longtail_GetBrotliTextMaxQuality(),
        Longtail_GetLizardMinQuality(),
        Longtail_GetLizardDefaultQuality(),
        Longtail_GetLizardMaxQuality(),
        Longtail_GetLZ4DefaultQuality(),
        Longtail_GetZStdMinCompression(),
        Longtail_GetZStdDefaultCompression(),
        Longtail_GetZStdMaxCompression()};


    struct Longtail_CompressionRegistryAPI* registry = Longtail_CreateDefaultCompressionRegistry(
        13,
        (const uint32_t*)compression_types,
        (const struct Longtail_CompressionAPI **)compression_apis,
        (const Longtail_CompressionAPI_HSettings*)compression_settings);
    if (registry == 0)
    {
        SAFE_DISPOSE_API(lizard_compression);
        SAFE_DISPOSE_API(lz4_compression);
        SAFE_DISPOSE_API(brotli_compression);
        SAFE_DISPOSE_API(zstd_compression);
        return 0;
    }
    return registry;
}

uint32_t ParseCompressionType(const char* compression_algorithm) {
	if ((compression_algorithm == 0) || (strcmp("none", compression_algorithm) == 0))
    {
		return 0;
    }
	if (strcmp("brotli", compression_algorithm) == 0)
    {
        return LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE;
    }
	if (strcmp("brotli_min", compression_algorithm) == 0)
    {
        return LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE;
    }
	if (strcmp("brotli_max", compression_algorithm) == 0)
    {
		return LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE;
    }
	if (strcmp("brotli_text", compression_algorithm) == 0)
    {
		return LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE;
    }
	if (strcmp("brotli_text_min", compression_algorithm) == 0)
    {
		return LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE;
    }
	if (strcmp("brotli_text_max", compression_algorithm) == 0)
    {
		return LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE;
    }
	if (strcmp("lz4", compression_algorithm) == 0)
    {
		return LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE;
    }
	if (strcmp("zstd", compression_algorithm) == 0)
    {
		return LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE;
    }
	if (strcmp("zstd_min", compression_algorithm) == 0)
    {
		return LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE;
    }
	if (strcmp("zstd_max", compression_algorithm) == 0)
    {
		return LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE;
    }
	return 0xffffffff;
}

uint32_t ParseHashingType(const char* hashing_type)
{
    if (0 == hashing_type || (strcmp("blake3", hashing_type) == 0))
    {
        return Longtail_GetBlake3HashType();
    }
    if (strcmp("blake2", hashing_type) == 0)
    {
        return Longtail_GetBlake2HashType();
    }
    if (strcmp("meow", hashing_type) == 0)
    {
        return Longtail_GetMeowHashType();
    }
    return 0xffffffff;
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

char* GetDefaultContentPath()
{
    char* tmp_folder = Longtail_GetTempFolder();
    if (!tmp_folder)
    {
        return 0;
    }
    const char* default_content_path = Longtail_ConcatPath(tmp_folder, "longtail_cache");
    Longtail_Free(tmp_folder);
    return (char*)default_content_path;
}

int UpSync(
    const char* storage_uri_raw,
    const char* source_path,
    const char* optional_source_index_path,
    const char* target_path,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block,
    uint32_t hashing_type,
    uint32_t compression_type)
{
    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_HashAPI* hash_api = CreateHashAPIFromIdentifier(hashing_type);
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount());
    struct Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_fsstore_api = Longtail_CreateFSBlockStoreAPI(storage_api, storage_path);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_fsstore_api, compression_registry);

    Longtail_VersionIndex* source_version_index = 0;
    if (optional_source_index_path)
    {
        int err = Longtail_ReadVersionIndex(storage_api, optional_source_index_path, &source_version_index);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Failed to read version index from `%s`, %d", optional_source_index_path, err);
        }
    }
    if (source_version_index == 0)
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            source_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * (*file_infos->m_Paths.m_PathCount));
        for (uint32_t i = 0; i < (*file_infos->m_Paths.m_PathCount); ++i)
        {
            tags[i] = compression_type;
        }
        {
            Progress create_version_progress("Indexing version");
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                job_api,
                &create_version_progress.m_API,
                source_path,
                &file_infos->m_Paths,
                file_infos->m_FileSizes,
                file_infos->m_Permissions,
                tags,
                target_chunk_size,
                &source_version_index);
        }
        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
    }
    struct Longtail_ContentIndex* version_content_index = 0;
    int err = Longtail_CreateContentIndex(
        hash_api,
        *source_version_index->m_ChunkCount,
        source_version_index->m_ChunkHashes,
        source_version_index->m_ChunkSizes,
        source_version_index->m_ChunkTags,
        target_block_size,
        max_chunks_per_block,
        &version_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create content index for `%s`, %d", source_path, err);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    struct Longtail_ContentIndex* block_store_content_index;
    {
        Progress block_store_get_content_index("Get content index");
        err = store_block_store_api->GetIndex(
            store_block_store_api,
            job_api,
            hash_api->GetIdentifier(hash_api),
            &block_store_get_content_index.m_API,
            &block_store_content_index);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to get store index for `%s`, %d", storage_uri_raw, err);
            Longtail_Free(version_content_index);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
    }
    {
        Progress write_content_progress("Writing blocks");
        err = Longtail_WriteContent(
            storage_api,
            store_block_store_api,
            job_api,
            &write_content_progress.m_API,
            block_store_content_index,
            version_content_index,
            source_version_index,
            source_path);
    }
    Longtail_Free(block_store_content_index);
    block_store_content_index = 0;

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create content blocks for `%s` to `%s`, %d", source_path, storage_uri_raw, err);
        Longtail_Free(version_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_api);
        Longtail_Free((char*)storage_path);
        return err;
    }
    Longtail_Free(version_content_index);

    const char* target_index_path = Longtail_ConcatPath(storage_path, target_path);
    err = Longtail_WriteVersionIndex(
        storage_api,
        source_version_index,
        target_index_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to write version index for `%s` to `%s`, %d", source_path, target_index_path, err);
        Longtail_Free((void*)target_index_path);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_api);
        Longtail_Free((char*)storage_path);
        return err;
    }
    Longtail_Free((void*)target_index_path);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(store_block_fsstore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    Longtail_Free((char*)storage_path);
    return 0;
}

int DownSync(
    const char* storage_uri_raw,
    const char* content_path,
    const char* source_path,
    const char* target_path,
    const char* optional_target_index_path,
    int retain_permissions,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block)
{
    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount());
    struct Longtail_CompressionRegistryAPI* compression_registry = CreateDefaultCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_remotestore_api = Longtail_CreateFSBlockStoreAPI(storage_api, storage_path);
    struct Longtail_BlockStoreAPI* store_block_localstore_api = Longtail_CreateFSBlockStoreAPI(storage_api, content_path);
    struct Longtail_BlockStoreAPI* store_block_cachestore_api = Longtail_CreateCacheBlockStoreAPI(store_block_localstore_api, store_block_remotestore_api);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_cachestore_api, compression_registry);

    const char* source_index_path = Longtail_ConcatPath(storage_path, source_path);
    Longtail_VersionIndex* source_version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, source_index_path, &source_version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", source_index_path, err);
        Longtail_Free((void*)source_index_path);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }
    Longtail_Free((void*)source_index_path);

    struct Longtail_ContentIndex* remote_content_index;
    {
        Progress get_index_progress("Get content index");
        err = store_block_store_api->GetIndex(
            store_block_store_api,
            job_api,
            Longtail_GetBlake3HashType(),  // We should not really care, since if block store is empty we can't recreate any content
            &get_index_progress.m_API,
            &remote_content_index);
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to get content index from `%s`, %d", storage_path, err);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    uint32_t hashing_type = *source_version_index->m_HashAPI;
    struct Longtail_HashAPI* hash_api = CreateHashAPIFromIdentifier(hashing_type);

    Longtail_VersionIndex* target_version_index = 0;
    if (optional_target_index_path)
    {
        int err = Longtail_ReadVersionIndex(storage_api, optional_target_index_path, &target_version_index);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Failed to read version index from `%s`, %d", optional_target_index_path, err);
        }
    }
    if (target_version_index == 0)
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            target_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", target_path, err);
            Longtail_Free(remote_content_index);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_cachestore_api);
            SAFE_DISPOSE_API(store_block_localstore_api);
            SAFE_DISPOSE_API(store_block_remotestore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_api);
            Longtail_Free((void*)storage_path);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * (*file_infos->m_Paths.m_PathCount));
        for (uint32_t i = 0; i < (*file_infos->m_Paths.m_PathCount); ++i)
        {
            tags[i] = 0;
        }
        {
            Progress create_version_progress("Indexing version");
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                job_api,
                &create_version_progress.m_API,
                target_path,
                &file_infos->m_Paths,
                file_infos->m_FileSizes,
                file_infos->m_Permissions,
                tags,
                target_chunk_size,
                &target_version_index);
        }
        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", target_path, err);
            Longtail_Free(remote_content_index);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_cachestore_api);
            SAFE_DISPOSE_API(store_block_localstore_api);
            SAFE_DISPOSE_API(store_block_remotestore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_api);
            Longtail_Free((void*)storage_path);
            return err;
        }
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(
        target_version_index,
        source_version_index,
        &version_diff);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create diff between `%s` and `%s`, %d", source_path, target_path, err);
        Longtail_Free(target_version_index);
        Longtail_Free(remote_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    {
        Progress change_version_progress("Updating version");
        err = Longtail_ChangeVersion(
            store_block_store_api,
            storage_api,
            hash_api,
            job_api,
            &change_version_progress.m_API,
            remote_content_index,
            target_version_index,
            source_version_index,
            version_diff,
            target_path,
            retain_permissions ? 1 : 0);
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to update version `%s` from `%s` using `%s`, %d", target_path, source_path, storage_uri_raw, err);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(remote_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    Longtail_Free(version_diff);
    Longtail_Free(target_version_index);
    Longtail_Free(remote_content_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(store_block_cachestore_api);
    SAFE_DISPOSE_API(store_block_localstore_api);
    SAFE_DISPOSE_API(store_block_remotestore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_api);
    Longtail_Free((void*)storage_path);
    return err;
}

int main(int argc, char** argv)
{
#if defined(_CRTDBG_MAP_ALLOC)
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    Longtail_SetAssert(AssertFailure);
    Longtail_SetLog(LogStdErr, 0);

    // General options
    const char* log_level_raw = 0;
    kgflags_string("log-level", "warn", "Log level (debug, info, warn, error)", false, &log_level_raw);

    int32_t target_chunk_size = 8;
    kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

    int32_t target_block_size = 0;
    kgflags_int("target-block-size", 32768 * 12, "Target block size", false, &target_block_size);

    int32_t max_chunks_per_block = 0;
    kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

    const char* storage_uri_raw = 0;
    kgflags_string("storage-uri", 0, "URI for chunks and content index for store", true, &storage_uri_raw);

    if (argc < 2)
    {
        kgflags_set_custom_description("Use command `upsync` or `downsync`");
        kgflags_print_usage();
        return 1;
    }

    const char* command = argv[1];
    if (((strcmp(command, "upsync") != 0) && (strcmp(command, "downsync") != 0)))
    {
        kgflags_set_custom_description("Use command `upsync` or `downsync`");
        kgflags_print_usage();
        return 1;
    }

    int err = 0;
    if (strcmp(command, "upsync") == 0){
        const char* hasing_raw = 0;
        kgflags_string("hash-algorithm", "blake3", "Hashing algorithm: blake2, blake3, meow", false, &hasing_raw);

        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source folder path", true, &source_path_raw);

        const char* source_index_raw = 0;
        kgflags_string("source-index-path", 0, "Optional pre-computed index of source-path", false, &source_index_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target file path relative to --storage-uri", true, &target_path_raw);

        const char* compression_raw = 0;
        kgflags_string("compression-algorithm", "zstd", "Comression algorithm: none, brotli, brotli_min, brotli_max, brotli_text, brotli_text_min, brotli_text_max, lz4, zstd, zstd_min, zstd_max", false, &compression_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        int log_level = log_level_raw ? ParseLogLevel(log_level_raw) : LONGTAIL_LOG_LEVEL_WARNING;
        if (log_level == -1)
        {
            printf("Invalid log level `%s`\n", log_level_raw);
            return 1;
        }
        Longtail_SetLogLevel(log_level);

        uint32_t compression = ParseCompressionType(compression_raw);
        if (compression == 0xffffffff)
        {
            printf("Invalid compression algorithm `%s`\n", compression_raw);
            return 1;
        }

        uint32_t hashing = ParseHashingType(hasing_raw);
        if (hashing == 0xffffffff)
        {
            printf("Invalid hashing algorithm `%s`\n", hasing_raw);
            return 1;
        }

        const char* source_path = NormalizePath(source_path_raw);
        const char* source_index = source_index_raw ? NormalizePath(source_index_raw) : 0;
        const char* target_path = NormalizePath(target_path_raw);

        err = UpSync(
            storage_uri_raw,
            source_path,
            source_index,
            target_path,
            target_chunk_size,
            target_block_size,
            max_chunks_per_block,
            hashing,
            compression);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)source_index);
        Longtail_Free((void*)target_path);
    }
    else
    {
        const char* content_path_raw = 0;
        kgflags_string("content-path", 0, "Location for downloaded/cached blocks", false, &content_path_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target folder path", true, &target_path_raw);

        const char* target_index_raw = 0;
        kgflags_string("target-index-path", 0, "Optional pre-computed index of target-path", false, &target_index_raw);

        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source file path relative to --storage-uri", true, &source_path_raw);

        bool retain_permission_raw = 0;
        kgflags_bool("retain-permissions", true, "Disable setting permission on file/directories from source", false, &retain_permission_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        int log_level = log_level_raw ? ParseLogLevel(log_level_raw) : LONGTAIL_LOG_LEVEL_WARNING;
        if (log_level == -1)
        {
            printf("Invalid log level `%s`\n", log_level_raw);
            return 1;
        }
        Longtail_SetLogLevel(log_level);

        const char* content_path = NormalizePath(content_path_raw ? content_path_raw : GetDefaultContentPath());
        const char* target_path = NormalizePath(target_path_raw);
        const char* target_index = target_index_raw ? NormalizePath(target_index_raw) : 0;
        const char* source_path = NormalizePath(source_path_raw);

        // Downsync!
        err = DownSync(
            storage_uri_raw,
            content_path,
            source_path,
            target_path,
            target_index,
            retain_permission_raw,
            target_chunk_size,
            target_block_size,
            max_chunks_per_block);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)target_index);
        Longtail_Free((void*)target_path);
        Longtail_Free((void*)content_path);
    }
#if defined(_CRTDBG_MAP_ALLOC)
    _CrtDumpMemoryLeaks();
#endif
    return err;
}
