#if defined(_CRTDBG_MAP_ALLOC)
#include <stdlib.h>
#include <crtdbg.h>
#endif

#include "../src/longtail.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/compressionregistry/longtail_full_compression_registry.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/hashregistry/longtail_full_hash_registry.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/retainingblockstore/longtail_retainingblockstore.h"
#include "../lib/shareblockstore/longtail_shareblockstore.h"
#include "../lib/brotli/longtail_brotli.h"
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

static void LogStdErr(void* context, int level, const char* log)
{
    fprintf(stderr, "%s: %s\n", ERROR_LEVEL[level], log);
}

struct Progress
{
    struct Longtail_ProgressAPI m_API;
    const char* m_Task;
    uint32_t m_OldPercent;
    uint32_t m_JobsDone;
};

static void Progress_OnProgress(struct Longtail_ProgressAPI* progress_api, uint32_t total, uint32_t jobs_done)
{
    struct Progress* p = (struct Progress*)progress_api;
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

static void Progress_Init(struct Progress* me, const char* task)
{
    me->m_Task = task;
    me->m_OldPercent = 0;
    me->m_JobsDone = 0;
    me->m_API.m_API.Dispose = 0;
    me->m_API.OnProgress = Progress_OnProgress;
}

static void Progress_Dispose(struct Progress* me)
{
    if (me->m_JobsDone != 0)
    {
        fprintf(stderr, " Done\n");
    }
}

int ParseLogLevel(const char* log_level_raw) {
    if (0 == strcmp(log_level_raw, "debug"))
    {
        return LONGTAIL_LOG_LEVEL_DEBUG;
    }
    if (0 == strcmp(log_level_raw, "info"))
    {
        return LONGTAIL_LOG_LEVEL_INFO;
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

static uint32_t* GetCompressionTypes(struct Longtail_StorageAPI* api, const struct Longtail_FileInfos* file_infos)
{
    uint32_t count = file_infos->m_Count;
    uint32_t* result = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * count);
    for (uint32_t i = 0; i < count; ++i)
    {
        const char* path = Longtail_FileInfos_GetPath(file_infos, i);
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
        result[i] = Longtail_GetLZ4DefaultQuality();
    }
    return result;
}

uint32_t ParseCompressionType(const char* compression_algorithm) {
    if ((compression_algorithm == 0) || (strcmp("none", compression_algorithm) == 0))
    {
        return 0;
    }
    if (strcmp("brotli", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliGenericDefaultQuality();
    }
    if (strcmp("brotli_min", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliGenericMinQuality();
    }
    if (strcmp("brotli_max", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliGenericMaxQuality();
    }
    if (strcmp("brotli_text", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliTextDefaultQuality();
    }
    if (strcmp("brotli_text_min", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliTextMinQuality();
    }
    if (strcmp("brotli_text_max", compression_algorithm) == 0)
    {
        return Longtail_GetBrotliTextMaxQuality();
    }
    if (strcmp("lz4", compression_algorithm) == 0)
    {
        return Longtail_GetLZ4DefaultQuality();
    }
    if (strcmp("zstd", compression_algorithm) == 0)
    {
        return Longtail_GetZStdDefaultQuality();
    }
    if (strcmp("zstd_min", compression_algorithm) == 0)
    {
        return Longtail_GetZStdMinQuality();
    }
    if (strcmp("zstd_max", compression_algorithm) == 0)
    {
        return Longtail_GetZStdMaxQuality();
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
    const char* default_cache_path = Longtail_ConcatPath(tmp_folder, "longtail_cache");
    Longtail_Free(tmp_folder);
    return (char*)default_cache_path;
}

struct AsyncRetargetContentComplete
{
    struct Longtail_AsyncRetargetContentAPI m_API;
    HLongtail_Sema m_NotifySema;
    int m_Err;
    struct Longtail_ContentIndex* m_ContentIndex;
};

static void AsyncRetargetContentComplete_OnComplete(struct Longtail_AsyncRetargetContentAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    struct AsyncRetargetContentComplete* cb = (struct AsyncRetargetContentComplete*)async_complete_api;
    cb->m_Err = err;
    cb->m_ContentIndex = content_index;
    Longtail_PostSema(cb->m_NotifySema, 1);
}

void AsyncRetargetContentComplete_Wait(struct AsyncRetargetContentComplete* api)
{
    Longtail_WaitSema(api->m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
}

static void AsyncRetargetContentComplete_Init(struct AsyncRetargetContentComplete* api)
{
    api->m_Err = EINVAL;
    api->m_API.m_API.Dispose = 0;
    api->m_API.OnComplete = AsyncRetargetContentComplete_OnComplete;
    api->m_ContentIndex = 0;
    Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &api->m_NotifySema);
}
static void AsyncRetargetContentComplete_Dispose(struct AsyncRetargetContentComplete* api)
{
    Longtail_DeleteSema(api->m_NotifySema);
    Longtail_Free(api->m_NotifySema);
}

static int SyncRetargetContent(struct Longtail_BlockStoreAPI* block_store, struct Longtail_ContentIndex* version_content_index, struct Longtail_ContentIndex** out_content_index)
{
    struct AsyncRetargetContentComplete retarget_content_index_complete;
    AsyncRetargetContentComplete_Init(&retarget_content_index_complete);
    int err = block_store->RetargetContent(block_store, version_content_index, &retarget_content_index_complete.m_API);
    if (err)
    {
        return err;
    }
    AsyncRetargetContentComplete_Wait(&retarget_content_index_complete);
    err = retarget_content_index_complete.m_Err;
    struct Longtail_ContentIndex* content_index =  retarget_content_index_complete.m_ContentIndex;
    AsyncRetargetContentComplete_Dispose(&retarget_content_index_complete);
    if (err)
    {
        return err;
    }
    *out_content_index = content_index;
    return 0;
}

int UpSync(
    const char* storage_uri_raw,
    const char* source_path,
    const char* optional_source_index_path,
    const char* target_index_path,
    const char* optional_target_version_content_index_path,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block,
    uint32_t hashing_type,
    uint32_t compression_type)
{
    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_fsstore_api = Longtail_CreateFSBlockStoreAPI(storage_api, storage_path, target_block_size, max_chunks_per_block);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_fsstore_api, compression_registry);

    struct Longtail_VersionIndex* source_version_index = 0;
    if (optional_source_index_path)
    {
        int err = Longtail_ReadVersionIndex(storage_api, optional_source_index_path, &source_version_index);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Failed to read version index from `%s`, %d", optional_source_index_path, err);
        }
    }

    struct Longtail_HashAPI* hash_api;
    int err = hash_registry->GetHashAPI(hash_registry, hashing_type, &hash_api);
    if (err)
    {
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }

    if (source_version_index == 0)
    {
        struct Longtail_FileInfos* file_infos;
        int err = Longtail_GetFilesRecursively(
            storage_api,
            0,
            0,
            0,
            source_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = compression_type;
        }
        {
            struct Progress create_version_progress;
            Progress_Init(&create_version_progress, "Indexing version");
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                job_api,
                &create_version_progress.m_API,
                0,
                0,
                source_path,
                file_infos,
                tags,
                target_chunk_size,
                &source_version_index);
            Progress_Dispose(&create_version_progress);
        }
        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", source_path, err);
            Longtail_Free(tags);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
    }
    struct Longtail_ContentIndex* version_content_index = 0;
    err = Longtail_CreateContentIndex(
        hash_api,
        source_version_index,
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
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    struct Longtail_ContentIndex* existing_remote_content_index;
    err = SyncRetargetContent(
        store_block_store_api,
        version_content_index,
        &existing_remote_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create missing content index %d", err);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    // Create a new missing content index which only contains the chunks that are not present in the remote store
    struct Longtail_ContentIndex* version_missing_content_index;
    err = Longtail_CreateMissingContent(
        hash_api,
        existing_remote_content_index,
        source_version_index,
        *existing_remote_content_index->m_MaxBlockSize,
        *existing_remote_content_index->m_MaxChunksPerBlock,
        &version_missing_content_index);

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create missing content index %d", err);
        Longtail_Free(existing_remote_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }
    {
        struct Progress write_content_progress;
        Progress_Init(&write_content_progress, "Writing blocks");
        err = Longtail_WriteContent(
            storage_api,
            store_block_store_api,
            job_api,
            &write_content_progress.m_API,
            0,
            0,
            existing_remote_content_index,
            version_missing_content_index,
            source_version_index,
            source_path);
        Progress_Dispose(&write_content_progress);
    }

    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create content blocks for `%s` to `%s`, %d", source_path, storage_uri_raw, err);
        Longtail_Free(version_missing_content_index);
        Longtail_Free(existing_remote_content_index);
        Longtail_Free(version_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }
    Longtail_Free(version_content_index);

    struct Longtail_ContentIndex* version_local_content_index;
    err = Longtail_MergeContentIndex(
        existing_remote_content_index,
        version_missing_content_index,
        &version_local_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version local content index %d", err);
        Longtail_Free(version_missing_content_index);
        Longtail_Free(existing_remote_content_index);
        Longtail_Free(version_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    if (optional_target_version_content_index_path)
    {
        err = Longtail_WriteContentIndex(
            storage_api,
            version_local_content_index,
            optional_target_version_content_index_path);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to write version content index for `%s` to `%s`, %d", source_path, optional_target_version_content_index_path, err);
            Longtail_Free(version_local_content_index);
            Longtail_Free(version_missing_content_index);
            Longtail_Free(existing_remote_content_index);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
    }

    err = Longtail_WriteVersionIndex(
        storage_api,
        source_version_index,
        target_index_path);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to write version index for `%s` to `%s`, %d", source_path, target_index_path, err);
        Longtail_Free(version_local_content_index);
        Longtail_Free(version_missing_content_index);
        Longtail_Free(existing_remote_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    Longtail_Free(version_local_content_index);
    Longtail_Free(version_missing_content_index);
    Longtail_Free(existing_remote_content_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(store_block_fsstore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    Longtail_Free((char*)storage_path);
    return 0;
}

struct AsyncGetIndexComplete
{
    struct Longtail_AsyncGetIndexAPI m_API;
    HLongtail_Sema m_NotifySema;
    int m_Err;
    struct Longtail_ContentIndex* m_ContentIndex;
};

static void AsyncGetIndexComplete_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    struct AsyncGetIndexComplete* cb = (struct AsyncGetIndexComplete*)async_complete_api;
    cb->m_Err = err;
    cb->m_ContentIndex = content_index;
    Longtail_PostSema(cb->m_NotifySema, 1);
}

static void AsyncGetIndexComplete_Init(struct AsyncGetIndexComplete* me)
{
    me->m_Err = EINVAL;
    me->m_API.m_API.Dispose = 0;
    me->m_API.OnComplete = AsyncGetIndexComplete_OnComplete;
    me->m_ContentIndex = 0;
    Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &me->m_NotifySema);
}

static void AsyncGetIndexComplete_Dispose(struct AsyncGetIndexComplete* me)
{
    Longtail_DeleteSema(me->m_NotifySema);
    Longtail_Free(me->m_NotifySema);
}

static void AsyncGetIndexComplete_Wait(struct AsyncGetIndexComplete* me)
{
    Longtail_WaitSema(me->m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
}

int DownSync(
    const char* storage_uri_raw,
    const char* cache_path,
    const char* source_path,
    const char* target_path,
    const char* optional_target_index_path,
    int retain_permissions,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block)
{
    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_remotestore_api = Longtail_CreateFSBlockStoreAPI(storage_api, storage_path, target_block_size, max_chunks_per_block);
    struct Longtail_BlockStoreAPI* store_block_localstore_api = Longtail_CreateFSBlockStoreAPI(storage_api, cache_path, target_block_size, max_chunks_per_block);
    struct Longtail_BlockStoreAPI* store_block_cachestore_api = Longtail_CreateCacheBlockStoreAPI(store_block_localstore_api, store_block_remotestore_api);
    struct Longtail_BlockStoreAPI* compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_cachestore_api, compression_registry);
    struct Longtail_BlockStoreAPI* retaining_block_store_api = 0;//Longtail_CreateRetainingBlockStoreAPI(compress_block_store_api);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateShareBlockStoreAPI(compress_block_store_api);//retaining_block_store_api);

    struct Longtail_VersionIndex* source_version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, source_path, &source_version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    uint32_t hashing_type = *source_version_index->m_HashIdentifier;
    struct Longtail_HashAPI* hash_api;
    err = hash_registry->GetHashAPI(hash_registry, hashing_type, &hash_api);
    if (err)
    {
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    struct Longtail_VersionIndex* target_version_index = 0;
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
            0,
            0,
            0,
            target_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", target_path, err);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(retaining_block_store_api);
            SAFE_DISPOSE_API(compress_block_store_api);
            SAFE_DISPOSE_API(store_block_cachestore_api);
            SAFE_DISPOSE_API(store_block_localstore_api);
            SAFE_DISPOSE_API(store_block_remotestore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
            Longtail_Free((void*)storage_path);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = 0;
        }
        {
            struct Progress create_version_progress;
            Progress_Init(&create_version_progress, "Indexing version");
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                job_api,
                &create_version_progress.m_API,
                0,
                0,
                target_path,
                file_infos,
                tags,
                target_chunk_size,
                &target_version_index);
            Progress_Dispose(&create_version_progress);
        }
        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", target_path, err);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(retaining_block_store_api);
            SAFE_DISPOSE_API(compress_block_store_api);
            SAFE_DISPOSE_API(store_block_cachestore_api);
            SAFE_DISPOSE_API(store_block_localstore_api);
            SAFE_DISPOSE_API(store_block_remotestore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
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
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    // IDEA: Potentially we could create the content index based on the diff, right?
    struct Longtail_ContentIndex* source_version_content_index;
    err = Longtail_CreateContentIndex(
        hash_api,
        source_version_index,
        target_block_size,
        max_chunks_per_block,
        &source_version_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to create content index for source `%s`, %d", source_path, err);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    struct Longtail_ContentIndex* retargetted_version_content_index;
    err = SyncRetargetContent(store_block_store_api, source_version_content_index, &retargetted_version_content_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to retarget the content index to remote store `%s`, %d", storage_uri_raw, err);
        Longtail_Free(source_version_content_index);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    err = Longtail_ValidateContent(retargetted_version_content_index, source_version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Store `%s` does not contain all the chunks needed for this version `%s`, Longtail_ValidateContent failed with %d", storage_uri_raw, source_path, err);
        Longtail_Free(retargetted_version_content_index);
        Longtail_Free(source_version_content_index);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    Longtail_Free(source_version_content_index);
    source_version_content_index = retargetted_version_content_index;

    {
        struct Progress change_version_progress;
        Progress_Init(&change_version_progress, "Updating version");
        err = Longtail_ChangeVersion(
            store_block_store_api,
            storage_api,
            hash_api,
            job_api,
            &change_version_progress.m_API,
            0,
            0,
            source_version_content_index,
            target_version_index,
            source_version_index,
            version_diff,
            target_path,
            retain_permissions ? 1 : 0);
        Progress_Dispose(&change_version_progress);
    }
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to update version `%s` from `%s` using `%s`, %d", target_path, source_path, storage_uri_raw, err);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_content_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(retaining_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    Longtail_Free(version_diff);
    Longtail_Free(target_version_index);
    Longtail_Free(source_version_content_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(retaining_block_store_api);
    SAFE_DISPOSE_API(compress_block_store_api);
    SAFE_DISPOSE_API(store_block_cachestore_api);
    SAFE_DISPOSE_API(store_block_localstore_api);
    SAFE_DISPOSE_API(store_block_remotestore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    Longtail_Free((void*)storage_path);
    return err;
}

int SetLogLevel(const char* log_level_raw)
{
    int log_level = log_level_raw ? ParseLogLevel(log_level_raw) : LONGTAIL_LOG_LEVEL_WARNING;
    if (log_level == -1)
    {
        printf("Invalid log level `%s`\n", log_level_raw);
        return 1;
    }
    Longtail_SetLogLevel(log_level);
    return 0;
}

int ValidateVersionIndex(
    const char* storage_uri_raw,
    const char* version_index_path,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block)
{
    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_api = Longtail_CreateFSBlockStoreAPI(storage_api, storage_path, target_block_size, max_chunks_per_block);
    struct Longtail_ContentIndex* block_store_content_index;
    {
        struct AsyncGetIndexComplete get_index_complete;
        AsyncGetIndexComplete_Init(&get_index_complete);
        int err = store_block_api->GetIndex(
            store_block_api,
            &get_index_complete.m_API);
        if (!err)
        {
            AsyncGetIndexComplete_Wait(&get_index_complete);
            err = get_index_complete.m_Err;
        }
        if (err)
        {
            LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to get store index for `%s`, %d", storage_uri_raw, err);
            SAFE_DISPOSE_API(store_block_api);
            SAFE_DISPOSE_API(storage_api);
            Longtail_Free((void*)storage_path);
            return err;
        }
        block_store_content_index = get_index_complete.m_ContentIndex;
        AsyncGetIndexComplete_Dispose(&get_index_complete);
    }

    struct Longtail_VersionIndex* version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, version_index_path, &version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", version_index_path, err);
        Longtail_Free(block_store_content_index);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    err = Longtail_ValidateContent(block_store_content_index, version_index);
    if (err)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Store `%s` does not have all the required chunks for %s, failed with %d", storage_uri_raw, version_index_path, err);
        Longtail_Free(version_index);
        Longtail_Free(block_store_content_index);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        Longtail_Free((void*)storage_path);
        return err;
    }
    Longtail_Free(version_index);
    Longtail_Free(block_store_content_index);
    SAFE_DISPOSE_API(store_block_api);
    SAFE_DISPOSE_API(storage_api);
    Longtail_Free((void*)storage_path);
    return 0;
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
    kgflags_int("target-chunk-size", 24576, "Target chunk size", false, &target_chunk_size);

    int32_t target_block_size = 0;
    kgflags_int("target-block-size", 524288, "Target block size", false, &target_block_size);

    int32_t max_chunks_per_block = 0;
    kgflags_int("max-chunks-per-block", 2048, "Max chunks per block", false, &max_chunks_per_block);

    const char* storage_uri_raw = 0;
    kgflags_string("storage-uri", 0, "URI for chunks and content index for store", true, &storage_uri_raw);

    if (argc < 2)
    {
        kgflags_set_custom_description("Use command `upsync` or `downsync`");
        kgflags_print_usage();
        return 1;
    }

    const char* command = argv[1];
    if ((strcmp(command, "upsync") != 0) &&
        (strcmp(command, "downsync") != 0) &&
        (strcmp(command, "validate") != 0))
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
        kgflags_string("target-path", 0, "Target file path", true, &target_path_raw);

        const char* optional_version_content_index_path_raw = 0;
        kgflags_string("version-content-index-path", 0, "Optional path to store minimal content index for version", false, &optional_version_content_index_path_raw);

        const char* compression_raw = 0;
        kgflags_string("compression-algorithm", "zstd", "Comression algorithm: none, brotli, brotli_min, brotli_max, brotli_text, brotli_text_min, brotli_text_max, lz4, zstd, zstd_min, zstd_max", false, &compression_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

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
        const char* optional_target_version_content_index_path = optional_version_content_index_path_raw ? NormalizePath(optional_version_content_index_path_raw) : 0;

        err = UpSync(
            storage_uri_raw,
            source_path,
            source_index,
            target_path,
            optional_target_version_content_index_path,
            target_chunk_size,
            target_block_size,
            max_chunks_per_block,
            hashing,
            compression);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)source_index);
        Longtail_Free((void*)target_path);
    }
    else if (strcmp(command, "downsync") == 0)
    {
        const char* cache_path_raw = 0;
        kgflags_string("cache-path", 0, "Location for downloaded/cached blocks", false, &cache_path_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target folder path", true, &target_path_raw);

        const char* target_index_raw = 0;
        kgflags_string("target-index-path", 0, "Optional pre-computed index of target-path", false, &target_index_raw);

        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source file path", true, &source_path_raw);

        bool retain_permission_raw = 0;
        kgflags_bool("retain-permissions", true, "Disable setting permission on file/directories from source", false, &retain_permission_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        const char* cache_path = NormalizePath(cache_path_raw ? cache_path_raw : GetDefaultContentPath());
        const char* target_path = NormalizePath(target_path_raw);
        const char* target_index = target_index_raw ? NormalizePath(target_index_raw) : 0;
        const char* source_path = NormalizePath(source_path_raw);

        // Downsync!
        err = DownSync(
            storage_uri_raw,
            cache_path,
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
        Longtail_Free((void*)cache_path);
    }
    else if (strcmp(command, "validate") == 0)
    {
        const char* version_index_path_raw = 0;
        kgflags_string("version-index-path", 0, "Path to version index", true, &version_index_path_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        const char* version_index_path = NormalizePath(version_index_path_raw);

        err = ValidateVersionIndex(
            storage_uri_raw,
            version_index_path,
            target_block_size,
            max_chunks_per_block);

        Longtail_Free((void*)version_index_path);
    }
#if defined(_CRTDBG_MAP_ALLOC)
    _CrtDumpMemoryLeaks();
#endif
    return err;
}
