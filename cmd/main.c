#if defined(_CRTDBG_MAP_ALLOC)
#include <stdlib.h>
#include <crtdbg.h>
#endif

#include "../src/longtail.h"
#include "../lib/archiveblockstore/longtail_archiveblockstore.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/blockstorestorage/longtail_blockstorestorage.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/compressionregistry/longtail_full_compression_registry.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/hpcdcchunker/longtail_hpcdcchunker.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/hashregistry/longtail_full_hash_registry.h"
#include "../lib/lrublockstore/longtail_lrublockstore.h"
#include "../lib/memstorage/longtail_memstorage.h"
#include "../lib/memtracer/longtail_memtracer.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/ratelimitedprogress/longtail_ratelimitedprogress.h"
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

static const char* ERROR_LEVEL[5] = {"DEBUG", "INFO", "WARNING", "ERROR", "OFF"};

static int LogContext(struct Longtail_LogContext* log_context, char* buffer, int buffer_size)
{
    if (log_context == 0 || log_context->field_count == 0)
    {
        return 0;
    }
    int len = sprintf(buffer, " { ");
    size_t log_field_count = log_context->field_count;
    for (size_t f = 0; f < log_field_count; ++f)
    {
        struct Longtail_LogField* log_field = &log_context->fields[f];
        len += snprintf(&buffer[len], buffer_size - len, "\"%s\": %s%s", log_field->name, log_field->value, ((f + 1) < log_field_count) ? ", " : "");
    }
    len += snprintf(&buffer[len], buffer_size - len, " }");
    return len;
}

static void LogStdErr(struct Longtail_LogContext* log_context, const char* log)
{
    char buffer[2048];
    int len = snprintf(buffer, 2048, "%s(%d) [%s] %s", log_context->file, log_context->line, log_context->function, ERROR_LEVEL[log_context->level]);
    len += LogContext(log_context, &buffer[len], 2048 - len);
    snprintf(&buffer[len], 2048 - len, " : %s\n", log);
    fprintf(stderr, "%s", buffer);
}

struct Progress
{
    struct Longtail_ProgressAPI m_API;
    struct Longtail_ProgressAPI* m_RateLimitedProgressAPI;
    const char* m_Task;
    uint32_t m_UpdateCount;
};

static void Progress_OnProgress(struct Longtail_ProgressAPI* progress_api, uint32_t total, uint32_t jobs_done)
{
    struct Progress* p = (struct Progress*)progress_api;
    if (jobs_done < total)
    {
        if (!p->m_UpdateCount)
        {
            fprintf(stderr, "%s: ", p->m_Task);
        }
        uint32_t percent_done = (100 * jobs_done) / total;
        fprintf(stderr, "%u%% ", percent_done);
        ++p->m_UpdateCount;
        return;
    }
    if (p->m_UpdateCount)
    {
        fprintf(stderr, "100%%");
    }
}

static void Progress_Dispose(struct Longtail_API* api)
{
    struct Progress* me = (struct Progress*)api;
    if (me->m_UpdateCount)
    {
        fprintf(stderr, " Done\n");
    }
    Longtail_Free(me);
}

struct Longtail_ProgressAPI* MakeProgressAPI(const char* task)
{
    void* mem = Longtail_Alloc(0, sizeof(struct Progress));
    if (!mem)
    {
        return 0;
    }
    struct Longtail_ProgressAPI* progress_api = Longtail_MakeProgressAPI(mem, Progress_Dispose, Progress_OnProgress);
    if (!progress_api)
    {
        Longtail_Free(mem);
        return 0;
    }
    struct Progress* me = (struct Progress*)progress_api;
    me->m_RateLimitedProgressAPI = Longtail_CreateRateLimitedProgress(progress_api, 5);
    me->m_Task = task;
    me->m_UpdateCount = 0;
    return me->m_RateLimitedProgressAPI;
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
    uint32_t* result = (uint32_t*)Longtail_Alloc(0, sizeof(uint32_t) * count);
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
    if (strcmp("zstd_high", compression_algorithm) == 0)
    {
        return Longtail_GetZStdHighQuality();
    }
    if (strcmp("zstd_low", compression_algorithm) == 0)
    {
        return Longtail_GetZStdLowQuality();
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
    if (normalized_path[0] == '\\' && normalized_path[1] == '\\' && normalized_path[2] == '?' && normalized_path[3] == '\\')
    {
        ri += 4;
    }
    while (path[ri])
    {
        switch (path[ri])
        {
        case '/':
        case '\\':
            if (wi && (normalized_path[wi - 1] == '/' || normalized_path[wi - 1] == '\\'))
            {
                ++ri;
            }
            else
            {
                normalized_path[wi++] = path[ri++];
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

struct AsyncGetExistingContentComplete
{
    struct Longtail_AsyncGetExistingContentAPI m_API;
    HLongtail_Sema m_NotifySema;
    int m_Err;
    struct Longtail_StoreIndex* m_StoreIndex;
};

static void AsyncGetExistingContentComplete_OnComplete(struct Longtail_AsyncGetExistingContentAPI* async_complete_api, struct Longtail_StoreIndex* store_index, int err)
{
    struct AsyncGetExistingContentComplete* cb = (struct AsyncGetExistingContentComplete*)async_complete_api;
    cb->m_Err = err;
    cb->m_StoreIndex = store_index;
    Longtail_PostSema(cb->m_NotifySema, 1);
}

void AsyncGetExistingContentComplete_Wait(struct AsyncGetExistingContentComplete* api)
{
    Longtail_WaitSema(api->m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
}

static void AsyncGetExistingContentComplete_Init(struct AsyncGetExistingContentComplete* api)
{
    api->m_Err = EINVAL;
    api->m_API.m_API.Dispose = 0;
    api->m_API.OnComplete = AsyncGetExistingContentComplete_OnComplete;
    api->m_StoreIndex = 0;
    Longtail_CreateSema(Longtail_Alloc(0, Longtail_GetSemaSize()), 0, &api->m_NotifySema);
}
static void AsyncGetExistingContentComplete_Dispose(struct AsyncGetExistingContentComplete* api)
{
    Longtail_DeleteSema(api->m_NotifySema);
    Longtail_Free(api->m_NotifySema);
}

static int SyncGetExistingContent(struct Longtail_BlockStoreAPI* block_store, uint32_t chunk_count, const TLongtail_Hash* chunk_hashes, uint32_t min_block_usage_percent, struct Longtail_StoreIndex** out_store_index)
{
    struct AsyncGetExistingContentComplete retarget_store_index_complete;
    AsyncGetExistingContentComplete_Init(&retarget_store_index_complete);
    int err = block_store->GetExistingContent(block_store, chunk_count, chunk_hashes, min_block_usage_percent, &retarget_store_index_complete.m_API);
    if (err)
    {
        return err;
    }
    AsyncGetExistingContentComplete_Wait(&retarget_store_index_complete);
    err = retarget_store_index_complete.m_Err;
    struct Longtail_StoreIndex* store_index =  retarget_store_index_complete.m_StoreIndex;
    AsyncGetExistingContentComplete_Dispose(&retarget_store_index_complete);
    if (err)
    {
        return err;
    }
    *out_store_index = store_index;
    return 0;
}

int UpSync(
    const char* storage_uri_raw,
    const char* source_path,
    const char* optional_source_index_path,
    const char* target_index_path,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block,
    uint32_t min_block_usage_percent,
    uint32_t hashing_type,
    uint32_t compression_type,
    int enable_mmap_indexing,
    int enable_mmap_block_store)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_uri_raw, "%s"),
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(optional_source_index_path, "%p"),
        LONGTAIL_LOGFIELD(target_index_path, "%s"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(target_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(hashing_type, "%u"),
        LONGTAIL_LOGFIELD(compression_type, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_fsstore_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, storage_path, 0, enable_mmap_block_store);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_fsstore_api, compression_registry);

    struct Longtail_VersionIndex* source_version_index = 0;
    if (optional_source_index_path)
    {
        int err = Longtail_ReadVersionIndex(storage_api, optional_source_index_path, &source_version_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Failed to read version index from `%s`, %d", optional_source_index_path, err);
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
    struct Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    if (!chunker_api)
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
        return ENOMEM;
    }

    if (source_version_index == 0)
    {
        struct Longtail_FileInfos* file_infos;
        err = Longtail_GetFilesRecursively(
            storage_api,
            0,
            0,
            0,
            source_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(store_block_fsstore_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(hash_registry);
            SAFE_DISPOSE_API(job_api);
            Longtail_Free((char*)storage_path);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(0, sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = compression_type;
        }

        struct Longtail_ProgressAPI* progress = MakeProgressAPI("Indexing version");
        if (progress)
        {
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                chunker_api,
                job_api,
                progress,
                0,
                0,
                source_path,
                file_infos,
                tags,
                target_chunk_size,
                enable_mmap_indexing,
                &source_version_index);
            SAFE_DISPOSE_API(progress);
        }
        else
        {
            err = ENOMEM;
        }

        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(chunker_api);
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

    struct Longtail_StoreIndex* existing_remote_store_index;
    err = SyncGetExistingContent(
        store_block_store_api,
        *source_version_index->m_ChunkCount,
        source_version_index->m_ChunkHashes,
        min_block_usage_percent,
        &existing_remote_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create missing store index %d", err);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    struct Longtail_StoreIndex* remote_missing_store_index;
    err = Longtail_CreateMissingContent(
        hash_api,
        existing_remote_store_index,
        source_version_index,
        target_block_size,
        max_chunks_per_block,
        &remote_missing_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create missing store index %d", err);
        Longtail_Free(existing_remote_store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    struct Longtail_ProgressAPI* progress = MakeProgressAPI("Writing blocks");
    if (progress)
    {
        err = Longtail_WriteContent(
            storage_api,
            store_block_store_api,
            job_api,
            progress,
            0,
            0,
            remote_missing_store_index,
            source_version_index,
            source_path);
        SAFE_DISPOSE_API(progress);
    }
    else
    {
        err = ENOMEM;
    }

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create store blocks for `%s` to `%s`, %d", source_path, storage_uri_raw, err);
        Longtail_Free(existing_remote_store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    struct Longtail_StoreIndex* version_local_store_index;
    err = Longtail_MergeStoreIndex(
        existing_remote_store_index,
        remote_missing_store_index,
        &version_local_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version local store index %d", err);
        Longtail_Free(remote_missing_store_index);
        Longtail_Free(existing_remote_store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    err = Longtail_WriteVersionIndex(
        storage_api,
        source_version_index,
        target_index_path);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to write version index for `%s` to `%s`, %d", source_path, target_index_path, err);
        Longtail_Free(version_local_store_index);
        Longtail_Free(remote_missing_store_index);
        Longtail_Free(existing_remote_store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(store_block_fsstore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((char*)storage_path);
        return err;
    }

    Longtail_Free(version_local_store_index);
    Longtail_Free(remote_missing_store_index);
    Longtail_Free(existing_remote_store_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(store_block_fsstore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    Longtail_Free((char*)storage_path);
    return 0;
}

int DownSync(
    const char* storage_uri_raw,
    const char* cache_path,
    const char* source_path,
    const char* target_path,
    const char* optional_target_index_path,
    int retain_permissions,
    int enable_mmap_indexing,
    int enable_mmap_block_store)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_uri_raw, "%s"),
        LONGTAIL_LOGFIELD(cache_path, "%s"),
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s"),
        LONGTAIL_LOGFIELD(optional_target_index_path, "%p"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_remotestore_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, storage_path, 0, enable_mmap_block_store);
    struct Longtail_BlockStoreAPI* store_block_localstore_api = 0;
    struct Longtail_BlockStoreAPI* store_block_cachestore_api = 0;
    struct Longtail_BlockStoreAPI* compress_block_store_api = 0;
    if (cache_path)
    {
        store_block_localstore_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, cache_path, 0, enable_mmap_block_store);
        store_block_cachestore_api = Longtail_CreateCacheBlockStoreAPI(job_api, store_block_localstore_api, store_block_remotestore_api);
        compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_cachestore_api, compression_registry);
    }
    else
    {
        compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_remotestore_api, compression_registry);
    }

    struct Longtail_BlockStoreAPI* lru_block_store_api = Longtail_CreateLRUBlockStoreAPI(compress_block_store_api, 32);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateShareBlockStoreAPI(lru_block_store_api);

    struct Longtail_VersionIndex* source_version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, source_path, &source_version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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
        SAFE_DISPOSE_API(lru_block_store_api);
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

    struct Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    if (!chunker_api)
    {
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return ENOMEM;
    }

    struct Longtail_VersionIndex* target_version_index = 0;
    if (optional_target_index_path)
    {
        err = Longtail_ReadVersionIndex(storage_api, optional_target_index_path, &target_version_index);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Failed to read version index from `%s`, %d", optional_target_index_path, err);
        }
    }

    uint32_t target_chunk_size = *source_version_index->m_TargetChunkSize;

    if (target_version_index == 0)
    {
        struct Longtail_FileInfos* file_infos;
        err = Longtail_GetFilesRecursively(
            storage_api,
            0,
            0,
            0,
            target_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version store from `%s`, %d", target_path, err);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(lru_block_store_api);
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
        uint32_t* tags = (uint32_t*)Longtail_Alloc(0, sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = 0;
        }

        struct Longtail_ProgressAPI* progress = MakeProgressAPI("Indexing version");
        if (progress)
        {
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                chunker_api,
                job_api,
                progress,
                0,
                0,
                target_path,
                file_infos,
                tags,
                target_chunk_size,
                enable_mmap_indexing,
                &target_version_index);
            SAFE_DISPOSE_API(progress);
        }
        else
        {
            err = ENOMEM;
        }

        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", target_path, err);
            Longtail_Free(source_version_index);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(store_block_store_api);
            SAFE_DISPOSE_API(lru_block_store_api);
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
        hash_api,
        target_version_index,
        source_version_index,
        &version_diff);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create diff between `%s` and `%s`, %d", source_path, target_path, err);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    if ((*version_diff->m_SourceRemovedCount == 0) &&
        (*version_diff->m_ModifiedContentCount == 0) &&
        (*version_diff->m_TargetAddedCount == 0) &&
        (*version_diff->m_ModifiedPermissionsCount == 0 || !retain_permissions) )
    {
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(store_block_cachestore_api);
        SAFE_DISPOSE_API(store_block_localstore_api);
        SAFE_DISPOSE_API(store_block_remotestore_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return 0;
    }

    uint32_t required_chunk_count;
    TLongtail_Hash* required_chunk_hashes = (TLongtail_Hash*)Longtail_Alloc(0, sizeof(TLongtail_Hash) * (*source_version_index->m_ChunkCount));
    err = Longtail_GetRequiredChunkHashes(
            source_version_index,
            version_diff,
            &required_chunk_count,
            required_chunk_hashes);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create get required chunks for source `%s`, %d", source_path, err);
        Longtail_Free(required_chunk_hashes);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    struct Longtail_StoreIndex* required_version_store_index;
    err = SyncGetExistingContent(
        store_block_store_api,
        required_chunk_count,
        required_chunk_hashes,
        0,
        &required_version_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to retarget the store index to remote store `%s`, %d", storage_uri_raw, err);
        Longtail_Free(required_chunk_hashes);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    Longtail_Free(required_chunk_hashes);

    struct Longtail_ProgressAPI* progress = MakeProgressAPI("Updating version");
    if (progress)
    {
        err = Longtail_ChangeVersion(
            store_block_store_api,
            storage_api,
            hash_api,
            job_api,
            progress,
            0,
            0,
            required_version_store_index,
            target_version_index,
            source_version_index,
            version_diff,
            target_path,
            retain_permissions ? 1 : 0);
        SAFE_DISPOSE_API(progress);
    }
    else
    {
        err = ENOMEM;
    }

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to update version `%s` from `%s` using `%s`, %d", target_path, source_path, storage_uri_raw, err);
        Longtail_Free(version_diff);
        Longtail_Free(target_version_index);
        Longtail_Free(required_version_store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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
    Longtail_Free(required_version_store_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(lru_block_store_api);
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
    int enable_mmap_block_store)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_uri_raw, "%s"),
        LONGTAIL_LOGFIELD(version_index_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    const char* storage_path = NormalizePath(storage_uri_raw);
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, storage_path, 0, enable_mmap_block_store);
    
    struct Longtail_VersionIndex* version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, version_index_path, &version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", version_index_path, err);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    struct Longtail_HashAPI* hash_api;
    err = hash_registry->GetHashAPI(hash_registry, *version_index->m_HashIdentifier, &hash_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Can not create hashing API for version index `%s`, failed with %d", version_index_path, err);
        Longtail_Free(version_index);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    struct Longtail_StoreIndex* block_store_store_index;
    err = SyncGetExistingContent(
        store_block_api,
        *version_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        0,
        &block_store_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create retarget store index for version index `%s` to `%s`", storage_uri_raw, version_index_path, err);
        Longtail_Free(version_index);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }

    err = Longtail_ValidateStore(block_store_store_index, version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Store `%s` does not have all the required chunks for %s, failed with %d", storage_uri_raw, version_index_path, err);
        Longtail_Free(version_index);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        Longtail_Free((void*)storage_path);
        return err;
    }
    Longtail_Free(version_index);
    Longtail_Free(block_store_store_index);
    SAFE_DISPOSE_API(store_block_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    Longtail_Free((void*)storage_path);
    return 0;
}

int VersionIndex_ls(
    const char* version_index_path,
    const char* ls_dir,
    int enable_mmap_block_store)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(version_index_path, "%s"),
        LONGTAIL_LOGFIELD(ls_dir, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_StorageAPI* fake_block_store_fs = Longtail_CreateInMemStorageAPI();
    struct Longtail_BlockStoreAPI* fake_block_store = Longtail_CreateFSBlockStoreAPI(
        job_api,
        fake_block_store_fs,
        "store",
        0,
        enable_mmap_block_store);


    struct Longtail_VersionIndex* version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, version_index_path, &version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", version_index_path, err);
        SAFE_DISPOSE_API(fake_block_store);
        SAFE_DISPOSE_API(fake_block_store_fs);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        return err;
    }

    struct Longtail_HashAPI* hash_api;
    err = hash_registry->GetHashAPI(hash_registry, *version_index->m_HashIdentifier, &hash_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Can not create hashing API for version index `%s`, failed with %d", version_index_path, err);
        SAFE_DISPOSE_API(fake_block_store);
        SAFE_DISPOSE_API(fake_block_store_fs);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        return err;
    }


    struct Longtail_StoreIndex* store_index;
    err = Longtail_CreateStoreIndex(
        hash_api,
        *version_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        version_index->m_ChunkSizes,
        version_index->m_ChunkTags,
        1024*1024*1024,
        1024,
        &store_index);

    struct Longtail_StorageAPI* block_store_fs = Longtail_CreateBlockStoreStorageAPI(
        hash_api,
        job_api,
        fake_block_store,
        store_index,
        version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create file system for version index `%s`, failed with %d", version_index_path, err);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(fake_block_store);
        SAFE_DISPOSE_API(fake_block_store_fs);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        return err;
    }

    Longtail_StorageAPI_HIterator fs_iterator;
    err = block_store_fs->StartFind(block_store_fs, ls_dir[0] == '.' ? &ls_dir[1] : ls_dir, &fs_iterator);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create file iteration for version index `%s`, failed with %d", version_index_path, err);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(store_index);
        SAFE_DISPOSE_API(fake_block_store);
        SAFE_DISPOSE_API(fake_block_store_fs);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(hash_registry);
        SAFE_DISPOSE_API(job_api);
        return err;
    }
    do
    {
        struct Longtail_StorageAPI_EntryProperties properties;
        err = block_store_fs->GetEntryProperties(block_store_fs, fs_iterator, &properties);
        if (!err)
        {
            char permission_string[11];
            permission_string[0] = properties.m_IsDir ? 'd' : '-';
            permission_string[1] = ((properties.m_Permissions & 0400) != 0) ? 'r' : '-';
            permission_string[2] = ((properties.m_Permissions & 0200) != 0) ? 'w' : '-';
            permission_string[3] = ((properties.m_Permissions & 0100) != 0) ? 'x' : '-';
            permission_string[4] = ((properties.m_Permissions & 0040) != 0) ? 'r' : '-';
            permission_string[5] = ((properties.m_Permissions & 0020) != 0) ? 'w' : '-';
            permission_string[6] = ((properties.m_Permissions & 0010) != 0) ? 'x' : '-';
            permission_string[7] = ((properties.m_Permissions & 0004) != 0) ? 'r' : '-';
            permission_string[8] = ((properties.m_Permissions & 0002) != 0) ? 'w' : '-';
            permission_string[9] = ((properties.m_Permissions & 0001) != 0) ? 'x' : '-';
            permission_string[10] = 0;
            printf("%s %12" PRIu64 " %s\n", permission_string, properties.m_Size, properties.m_Name);
        }
    } while (block_store_fs->FindNext(block_store_fs, fs_iterator) == 0);

    block_store_fs->CloseFind(block_store_fs, fs_iterator);
    
    SAFE_DISPOSE_API(block_store_fs);
    Longtail_Free(store_index);
    SAFE_DISPOSE_API(fake_block_store);
    SAFE_DISPOSE_API(fake_block_store_fs);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    return 0;
}

int VersionIndex_cp(
    const char* storage_uri_raw,
    const char* version_index_path,
    const char* cache_path,
    const char* source_path,
    const char* target_path,
    int enable_mmap_block_store)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(storage_uri_raw, "%s"),
        LONGTAIL_LOGFIELD(version_index_path, "%s"),
        LONGTAIL_LOGFIELD(cache_path, "%s"),
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    const char* storage_path = NormalizePath(storage_uri_raw);

    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();
    struct Longtail_BlockStoreAPI* store_block_remotestore_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, storage_path, 0, enable_mmap_block_store);
    struct Longtail_BlockStoreAPI* store_block_localstore_api = 0;
    struct Longtail_BlockStoreAPI* store_block_cachestore_api = 0;
    struct Longtail_BlockStoreAPI* compress_block_store_api = 0;
    if (cache_path)
    {
        store_block_localstore_api = Longtail_CreateFSBlockStoreAPI(job_api, storage_api, cache_path, 0, enable_mmap_block_store);
        store_block_cachestore_api = Longtail_CreateCacheBlockStoreAPI(job_api, store_block_localstore_api, store_block_remotestore_api);
        compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_cachestore_api, compression_registry);
    }
    else
    {
        compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(store_block_remotestore_api, compression_registry);
    }

    struct Longtail_BlockStoreAPI* lru_block_store_api = Longtail_CreateLRUBlockStoreAPI(compress_block_store_api, 32);
    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateShareBlockStoreAPI(lru_block_store_api);

    struct Longtail_VersionIndex* version_index = 0;
    int err = Longtail_ReadVersionIndex(storage_api, version_index_path, &version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to read version index from `%s`, %d", version_index_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    struct Longtail_HashAPI* hash_api;
    err = hash_registry->GetHashAPI(hash_registry, *version_index->m_HashIdentifier, &hash_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Can not create hashing API for version index `%s`, failed with %d", version_index_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    struct Longtail_StoreIndex* block_store_store_index;
    err = SyncGetExistingContent(
        store_block_store_api,
        *version_index->m_ChunkCount,
        version_index->m_ChunkHashes,
        0,
        &block_store_store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create retarget store index for version index `%s` to `%s`", storage_uri_raw, version_index_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    err = Longtail_ValidateStore(block_store_store_index, version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Store `%s` does not contain all the chunks needed for this version `%s`, Longtail_ValidateStore failed with %d", storage_uri_raw, source_path, err);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    struct Longtail_StorageAPI* block_store_fs = Longtail_CreateBlockStoreStorageAPI(
        hash_api,
        job_api,
        store_block_store_api,
        block_store_store_index,
        version_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create file system for version index `%s`, failed with %d", version_index_path, err);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    Longtail_StorageAPI_HOpenFile f;
    err = block_store_fs->OpenReadFile(block_store_fs, source_path[0] == '.' ? &source_path[1] : source_path, &f);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to open file `%s` in version index `%s`, failed with %d", source_path, version_index_path, err);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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
    uint64_t size;
    err = block_store_fs->GetSize(block_store_fs, f, &size);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to get size of file `%s` in version index `%s`, failed with %d", source_path, version_index_path, err);
        block_store_fs->CloseFile(block_store_fs, f);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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
    Longtail_StorageAPI_HOpenFile outf;
    err = storage_api->OpenWriteFile(storage_api, target_path, size, &outf);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to open file `%s`, failed with %d", target_path, err);
        block_store_fs->CloseFile(block_store_fs, f);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    const size_t BUFFER_SIZE=128*1024*1024;
    char* buffer = (char*)Longtail_Alloc(0, size > BUFFER_SIZE ? BUFFER_SIZE : size);
    uint64_t off = 0;
    while (size > off)
    {
        uint64_t part = (size - off) > BUFFER_SIZE ? BUFFER_SIZE : (size - off);
        block_store_fs->Read(block_store_fs, f, off, part, buffer);
        storage_api->Write(storage_api, outf, off, part, buffer);
        off += part;
    }
    Longtail_Free(buffer);

    storage_api->CloseFile(storage_api, outf);
    block_store_fs->CloseFile(block_store_fs, f);

    uint16_t permissions;
    err = block_store_fs->GetPermissions(block_store_fs, source_path[0] == '.' ? &source_path[1] : source_path, &permissions);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to get permissions of file `%s` in version index `%s`, failed with %d", source_path, version_index_path, err);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    err = storage_api->SetPermissions(storage_api, target_path, permissions);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to set permissions of file `%s`, failed with %d", target_path, err);
        SAFE_DISPOSE_API(block_store_fs);
        Longtail_Free(block_store_store_index);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
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

    SAFE_DISPOSE_API(block_store_fs);
    Longtail_Free(block_store_store_index);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(lru_block_store_api);
    SAFE_DISPOSE_API(compress_block_store_api);
    SAFE_DISPOSE_API(store_block_cachestore_api);
    SAFE_DISPOSE_API(store_block_localstore_api);
    SAFE_DISPOSE_API(store_block_remotestore_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(hash_registry);
    SAFE_DISPOSE_API(job_api);
    Longtail_Free((void*)storage_path);
    return 0;
}


struct SyncFlush
{
    struct Longtail_AsyncFlushAPI m_API;
    HLongtail_Sema m_NotifySema;
    int m_Err;
};

void SyncFlush_OnComplete(struct Longtail_AsyncFlushAPI* async_complete_api, int err)
{
    struct SyncFlush* api = (struct SyncFlush*)async_complete_api;
    api->m_Err = err;
    Longtail_PostSema(api->m_NotifySema, 1);
}

void SyncFlush_Wait(struct SyncFlush* sync_flush)
{
    Longtail_WaitSema(sync_flush->m_NotifySema, LONGTAIL_TIMEOUT_INFINITE);
}

void SyncFlush_Dispose(struct Longtail_API* longtail_api)
{
    struct SyncFlush* api = (struct SyncFlush*)longtail_api;
    Longtail_DeleteSema(api->m_NotifySema);
    Longtail_Free(api->m_NotifySema);
}

int SyncFlush_Init(struct SyncFlush* sync_flush)
{
    sync_flush->m_Err = EINVAL;
    sync_flush->m_API.m_API.Dispose = SyncFlush_Dispose;
    sync_flush->m_API.OnComplete = SyncFlush_OnComplete;
    return Longtail_CreateSema(Longtail_Alloc(0, Longtail_GetSemaSize()), 0, &sync_flush->m_NotifySema);
}



int Pack(
    const char* source_path,
    const char* target_path,
    uint32_t target_chunk_size,
    uint32_t target_block_size,
    uint32_t max_chunks_per_block,
    uint32_t min_block_usage_percent,
    uint32_t hashing_type,
    uint32_t compression_type,
    int enable_mmap_indexing)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s"),
        LONGTAIL_LOGFIELD(target_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(target_block_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunks_per_block, "%u"),
        LONGTAIL_LOGFIELD(min_block_usage_percent, "%u"),
        LONGTAIL_LOGFIELD(hashing_type, "%u"),
        LONGTAIL_LOGFIELD(compression_type, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();

    struct Longtail_HashAPI* hash_api;
    int err = hash_registry->GetHashAPI(hash_registry, hashing_type, &hash_api);
    if (err)
    {
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }
    struct Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    if (!chunker_api)
    {
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_VersionIndex* source_version_index = 0;
    {
        struct Longtail_FileInfos* file_infos;
        err = Longtail_GetFilesRecursively(
            storage_api,
            0,
            0,
            0,
            source_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_registry);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(0, sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = compression_type;
        }

        struct Longtail_ProgressAPI* progress = MakeProgressAPI("Indexing version");
        if (progress)
        {
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                chunker_api,
                job_api,
                progress,
                0,
                0,
                source_path,
                file_infos,
                tags,
                target_chunk_size,
                enable_mmap_indexing,
                &source_version_index);
            SAFE_DISPOSE_API(progress);
        }
        else
        {
            err = ENOMEM;
        }

        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", source_path, err);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_registry);
            return err;
        }
    }

    struct Longtail_StoreIndex* store_index;
    err  = Longtail_CreateStoreIndex(
        hash_api,
        *source_version_index->m_ChunkCount,
        source_version_index->m_ChunkHashes,
        source_version_index->m_ChunkSizes,
        source_version_index->m_ChunkTags,
        target_block_size,
        max_chunks_per_block,
        &store_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create missing store index %d", err);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }


    struct Longtail_ArchiveIndex* archive_index;
    err = Longtail_CreateArchiveIndex(
        store_index,
        source_version_index,
        &archive_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create archive index %d", err);
        Longtail_Free(store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }

    struct Longtail_BlockStoreAPI* archive_block_store_api = Longtail_CreateArchiveBlockStore(
        storage_api,
        target_path,
        archive_index,
        1,
        0);
    if (archive_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create archive block store %d", ENOMEM);
        Longtail_Free(archive_index);
        Longtail_Free(store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateCompressBlockStoreAPI(archive_block_store_api, compression_registry);
    if (store_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create compression block store %d", ENOMEM);
        SAFE_DISPOSE_API(archive_block_store_api);
        Longtail_Free(archive_index);
        Longtail_Free(store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_ProgressAPI* progress = MakeProgressAPI("Writing blocks");
    if (progress)
    {
        err = Longtail_WriteContent(
            storage_api,
            store_block_store_api,
            job_api,
            progress,
            0,
            0,
            store_index,
            source_version_index,
            source_path);
        SAFE_DISPOSE_API(progress);
    }
    else
    {
        err = ENOMEM;
    }

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create store blocks for `%s` to `%s`, %d", source_path, target_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        Longtail_Free(archive_index);
        Longtail_Free(store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }


    struct SyncFlush flushCB;
    err = SyncFlush_Init(&flushCB);
    if (err)
    {
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        Longtail_Free(archive_index);
        Longtail_Free(store_index);
        Longtail_Free(source_version_index);
        SAFE_DISPOSE_API(chunker_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }
    err = archive_block_store_api->Flush(store_block_store_api, &flushCB.m_API);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to flush block store for `%s` to `%s`, %d", source_path, target_path, err);
    }
    else
    {
        SyncFlush_Wait(&flushCB);
        if (flushCB.m_Err != 0)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to flush block store for `%s` to `%s`, %d", source_path, target_path, flushCB.m_Err);
        }
    }


    SAFE_DISPOSE_API(&flushCB.m_API);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(archive_block_store_api);
    Longtail_Free(archive_index);
    Longtail_Free(store_index);
    Longtail_Free(source_version_index);
    SAFE_DISPOSE_API(chunker_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_registry);

    return 0;
}

int Unpack(
    const char* source_path,
    const char* target_path,
    int retain_permissions,
    int enable_mmap_indexing,
    int enable_mmap_unpacking)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(source_path, "%s"),
        LONGTAIL_LOGFIELD(target_path, "%s"),
        LONGTAIL_LOGFIELD(retain_permissions, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    struct Longtail_JobAPI* job_api = Longtail_CreateBikeshedJobAPI(Longtail_GetCPUCount(), 0);
    struct Longtail_HashRegistryAPI* hash_registry = Longtail_CreateFullHashRegistry();
    struct Longtail_CompressionRegistryAPI* compression_registry = Longtail_CreateFullCompressionRegistry();
    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();

    struct Longtail_ArchiveIndex* archive_index;
    int err = Longtail_ReadArchiveIndex(
            storage_api,
            source_path,
            &archive_index);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to read archive index from `%s`, %d", source_path, err);
        return err;
    }

    uint32_t hashing_type = *archive_index->m_VersionIndex.m_HashIdentifier;
    struct Longtail_HashAPI* hash_api;
    err = hash_registry->GetHashAPI(hash_registry, hashing_type, &hash_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create hashing api from `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }
    struct Longtail_ChunkerAPI* chunker_api = Longtail_CreateHPCDCChunkerAPI();
    if (chunker_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create chunking api from `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }

    struct Longtail_VersionIndex* target_version_index = 0;
    {
        struct Longtail_FileInfos* file_infos;
        err = Longtail_GetFilesRecursively(
            storage_api,
            0,
            0,
            0,
            target_path,
            &file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to scan version content from `%s`, %d", target_path, err);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_registry);
            return err;
        }
        uint32_t* tags = (uint32_t*)Longtail_Alloc(0, sizeof(uint32_t) * file_infos->m_Count);
        for (uint32_t i = 0; i < file_infos->m_Count; ++i)
        {
            tags[i] = 0;
        }

        struct Longtail_ProgressAPI* progress = MakeProgressAPI("Indexing version");
        if (progress)
        {
            err = Longtail_CreateVersionIndex(
                storage_api,
                hash_api,
                chunker_api,
                job_api,
                progress,
                0,
                0,
                target_path,
                file_infos,
                tags,
                *archive_index->m_VersionIndex.m_TargetChunkSize,
                enable_mmap_indexing,
                &target_version_index);
            SAFE_DISPOSE_API(progress);
        }
        else
        {
            err = ENOMEM;
        }

        Longtail_Free(tags);
        Longtail_Free(file_infos);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create version index for `%s`, %d", target_path, err);
            SAFE_DISPOSE_API(chunker_api);
            SAFE_DISPOSE_API(storage_api);
            SAFE_DISPOSE_API(compression_registry);
            SAFE_DISPOSE_API(job_api);
            SAFE_DISPOSE_API(hash_registry);
            return err;
        }
    }

    struct Longtail_BlockStoreAPI* archive_block_store_api = Longtail_CreateArchiveBlockStore(
        storage_api,
        source_path,
        archive_index,
        0,
        enable_mmap_unpacking);
    if (archive_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create archive block store `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_BlockStoreAPI* compress_block_store_api = Longtail_CreateCompressBlockStoreAPI(archive_block_store_api, compression_registry);
    if (compress_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create compress block store `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_BlockStoreAPI* lru_block_store_api = Longtail_CreateLRUBlockStoreAPI(compress_block_store_api, 32);
    if (lru_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create lru block store `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_BlockStoreAPI* store_block_store_api = Longtail_CreateShareBlockStoreAPI(lru_block_store_api);
    if (store_block_store_api == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create share block store `%s`, %d", source_path, err);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return ENOMEM;
    }

    struct Longtail_VersionDiff* version_diff;
    err = Longtail_CreateVersionDiff(
        hash_api,
        target_version_index,
        &archive_index->m_VersionIndex,
        &version_diff);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to create diff between `%s` and `%s`, %d", source_path, target_path, err);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }

    if ((*version_diff->m_SourceRemovedCount == 0) &&
        (*version_diff->m_ModifiedContentCount == 0) &&
        (*version_diff->m_TargetAddedCount == 0) &&
        (*version_diff->m_ModifiedPermissionsCount == 0 || !retain_permissions) )
    {
        Longtail_Free(version_diff);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return 0;
    }

    struct Longtail_ProgressAPI* progress = MakeProgressAPI("Updating version");
    if (progress)
    {
        err = Longtail_ChangeVersion(
            store_block_store_api,
            storage_api,
            hash_api,
            job_api,
            progress,
            0,
            0,
            &archive_index->m_StoreIndex,
            target_version_index,
            &archive_index->m_VersionIndex,
            version_diff,
            target_path,
            retain_permissions ? 1 : 0);
        SAFE_DISPOSE_API(progress);
    }
    else
    {
        err = ENOMEM;
    }

    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Failed to update version `%s` from `%s`, %d", target_path, source_path, err);
        Longtail_Free(version_diff);
        SAFE_DISPOSE_API(store_block_store_api);
        SAFE_DISPOSE_API(lru_block_store_api);
        SAFE_DISPOSE_API(compress_block_store_api);
        SAFE_DISPOSE_API(archive_block_store_api);
        SAFE_DISPOSE_API(storage_api);
        SAFE_DISPOSE_API(compression_registry);
        SAFE_DISPOSE_API(job_api);
        SAFE_DISPOSE_API(hash_registry);
        return err;
    }

    Longtail_Free(version_diff);
    SAFE_DISPOSE_API(store_block_store_api);
    SAFE_DISPOSE_API(lru_block_store_api);
    SAFE_DISPOSE_API(compress_block_store_api);
    SAFE_DISPOSE_API(archive_block_store_api);
    SAFE_DISPOSE_API(storage_api);
    SAFE_DISPOSE_API(compression_registry);
    SAFE_DISPOSE_API(job_api);
    SAFE_DISPOSE_API(hash_registry);
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

    bool enable_mem_tracer_raw = 0;
    kgflags_bool("mem-tracer", false, "Enable tracing of memory usage", false, &enable_mem_tracer_raw);

    if (argc < 2)
    {
        kgflags_set_custom_description("Use command `upsync`, `downsync`, `validate`, `ls`, `cp`, `pack` or `unpack`");
        kgflags_print_usage();
        return 1;
    }

    const char* command = argv[1];
    if ((strcmp(command, "upsync") != 0) &&
        (strcmp(command, "downsync") != 0) &&
        (strcmp(command, "validate") != 0) &&
        (strcmp(command, "ls") != 0) &&
        (strcmp(command, "cp") != 0) &&
        (strcmp(command, "pack") != 0) &&
        (strcmp(command, "unpack") != 0))
    {
        kgflags_set_custom_description("Use command `upsync`, `downsync`, `validate`, `ls`, `cp`, `pack` or `unpack`");
        kgflags_print_usage();
        return 1;
    }

    int err = 0;
    if (strcmp(command, "upsync") == 0){
        const char* storage_uri_raw = 0;
        kgflags_string("storage-uri", 0, "URI for chunks and store index for store", true, &storage_uri_raw);

        const char* hasing_raw = 0;
        kgflags_string("hash-algorithm", "blake3", "Hashing algorithm: blake2, blake3, meow", false, &hasing_raw);

        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source folder path", true, &source_path_raw);

        const char* source_index_raw = 0;
        kgflags_string("source-index-path", 0, "Optional pre-computed index of source-path", false, &source_index_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target file path", true, &target_path_raw);

        const char* compression_raw = 0;
        kgflags_string("compression-algorithm", "zstd", "Compression algorithm: none, brotli, brotli_min, brotli_max, brotli_text, brotli_text_min, brotli_text_max, lz4, zstd, zstd_min, zstd_max", false, &compression_raw);

        int32_t target_chunk_size = 8;
        kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

        int32_t target_block_size = 0;
        kgflags_int("target-block-size", 8388608, "Target block size", false, &target_block_size);

        int32_t max_chunks_per_block = 0;
        kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

        int32_t min_block_usage_percent = 8;
        kgflags_int("min-block-usage-percent", 0, "Minimum percent of block content than must match for it to be considered \"existing\"", false, &min_block_usage_percent);

        bool enable_mmap_indexing_raw = 0;
        kgflags_bool("mmap-indexing", false, "Enable memory mapping of files while indexing", false, &enable_mmap_indexing_raw);

        bool enable_mmap_block_store_raw = 0;
        kgflags_bool("mmap-block-store", false, "Enable memory mapping of files in block store", false, &enable_mmap_block_store_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
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

        err = UpSync(
            storage_uri_raw,
            source_path,
            source_index,
            target_path,
            target_chunk_size,
            target_block_size,
            max_chunks_per_block,
            min_block_usage_percent,
            hashing,
            compression,
            enable_mmap_indexing_raw,
            enable_mmap_block_store_raw);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)source_index);
        Longtail_Free((void*)target_path);
    }
    else if (strcmp(command, "downsync") == 0)
    {
        const char* storage_uri_raw = 0;
        kgflags_string("storage-uri", 0, "URI for chunks and store index for store", true, &storage_uri_raw);

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

        bool enable_mmap_indexing_raw = 0;
        kgflags_bool("mmap-indexing", false, "Enable memory mapping of files while indexing", false, &enable_mmap_indexing_raw);

        bool enable_mmap_block_store_raw = 0;
        kgflags_bool("mmap-block-store", false, "Enable memory mapping of files in block store", false, &enable_mmap_block_store_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
        }

        const char* cache_path = cache_path_raw ? NormalizePath(cache_path_raw) : 0;
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
            enable_mmap_indexing_raw,
            enable_mmap_block_store_raw);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)target_index);
        Longtail_Free((void*)target_path);
        Longtail_Free((void*)cache_path);
    }
    else if (strcmp(command, "validate") == 0)
    {
        const char* storage_uri_raw = 0;
        kgflags_string("storage-uri", 0, "URI for chunks and store index for store", true, &storage_uri_raw);

        const char* version_index_path_raw = 0;
        kgflags_string("version-index-path", 0, "Path to version index", true, &version_index_path_raw);

        bool enable_mmap_block_store_raw = 0;
        kgflags_bool("mmap-block-store", false, "Enable memory mapping of files in block store", false, &enable_mmap_block_store_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
        }

        const char* version_index_path = NormalizePath(version_index_path_raw);

        err = ValidateVersionIndex(
            storage_uri_raw,
            version_index_path,
            enable_mmap_block_store_raw);

        Longtail_Free((void*)version_index_path);
    }
    else if (strcmp(command, "ls") == 0)
    {
        const char* version_index_path_raw = 0;
        kgflags_string("version-index-path", 0, "Version index file path", true, &version_index_path_raw);

        bool enable_mmap_block_store_raw = 0;
        kgflags_bool("mmap-block-store", false, "Enable memory mapping of files in block store", false, &enable_mmap_block_store_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
        }

        if (kgflags_get_non_flag_args_count() < 2)
        {
            kgflags_set_custom_description("Use ls <path>");
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }
        const char* ls_dir_raw = kgflags_get_non_flag_arg(1);

        const char* version_index_path = NormalizePath(version_index_path_raw);
        const char* ls_dir = NormalizePath(ls_dir_raw);

        err = VersionIndex_ls(
            version_index_path,
            ls_dir,
            enable_mmap_block_store_raw);
	// ls [path] --source-path <version.lvi> --storage-uri <store-uri>
	// cp source target --source-path <version.lvi> --storage-uri <store-uri>
	// 

    }
    else if (strcmp(command, "cp") == 0)
    {
        const char* storage_uri_raw = 0;
        kgflags_string("storage-uri", 0, "URI for chunks and store index for store", true, &storage_uri_raw);

        const char* cache_path_raw = 0;
        kgflags_string("cache-path", 0, "Location for downloaded/cached blocks", false, &cache_path_raw);

        const char* version_index_path_raw = 0;
        kgflags_string("version-index-path", 0, "Version index file path", true, &version_index_path_raw);

        bool enable_mmap_block_store_raw = 0;
        kgflags_bool("mmap-block-store", false, "Enable memory mapping of files in block store", false, &enable_mmap_block_store_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (kgflags_get_non_flag_args_count() < 3)
        {
            kgflags_set_custom_description("Use cp <source-path> <target-path>");
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
        }

        const char* source_path_raw = kgflags_get_non_flag_arg(1);
        const char* target_path_raw = kgflags_get_non_flag_arg(2);

        const char* cache_path = cache_path_raw ? NormalizePath(cache_path_raw) : 0;
        const char* version_index_path = NormalizePath(version_index_path_raw);
        const char* source_path = NormalizePath(source_path_raw);
        const char* target_path = NormalizePath(target_path_raw);

        err = VersionIndex_cp(
            storage_uri_raw,
            version_index_path,
            cache_path,
            source_path,
            target_path,
            enable_mmap_block_store_raw);
    }
    else if (strcmp(command, "pack") == 0)
    {
        const char* hasing_raw = 0;
        kgflags_string("hash-algorithm", "blake3", "Hashing algorithm: blake2, blake3, meow", false, &hasing_raw);

        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source folder path", true, &source_path_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target file path", true, &target_path_raw);

        const char* compression_raw = 0;
        kgflags_string("compression-algorithm", "zstd", "Compression algorithm: none, brotli, brotli_min, brotli_max, brotli_text, brotli_text_min, brotli_text_max, lz4, zstd, zstd_min, zstd_max", false, &compression_raw);

        int32_t target_chunk_size = 8;
        kgflags_int("target-chunk-size", 32768, "Target chunk size", false, &target_chunk_size);

        int32_t target_block_size = 0;
        kgflags_int("target-block-size", 8388608, "Target block size", false, &target_block_size);

        int32_t max_chunks_per_block = 0;
        kgflags_int("max-chunks-per-block", 1024, "Max chunks per block", false, &max_chunks_per_block);

        int32_t min_block_usage_percent = 8;
        kgflags_int("min-block-usage-percent", 0, "Minimum percent of block content than must match for it to be considered \"existing\"", false, &min_block_usage_percent);

        bool enable_mmap_indexing_raw = 0;
        kgflags_bool("mmap-indexing", false, "Enable memory mapping of files while indexing", false, &enable_mmap_indexing_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
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

        const char* target_path = NormalizePath(target_path_raw);

        err = Pack(
            source_path,
            target_path,
            target_chunk_size,
            target_block_size,
            max_chunks_per_block,
            min_block_usage_percent,
            hashing,
            compression,
            enable_mmap_indexing_raw);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)target_path);
    }
    if (strcmp(command, "unpack") == 0)
    {
        const char* source_path_raw = 0;
        kgflags_string("source-path", 0, "Source folder path", true, &source_path_raw);

        const char* target_path_raw = 0;
        kgflags_string("target-path", 0, "Target file path", true, &target_path_raw);

        bool retain_permission_raw = 0;
        kgflags_bool("retain-permissions", true, "Disable setting permission on file/directories from source", false, &retain_permission_raw);

        bool enable_mmap_indexing_raw = 0;
        kgflags_bool("mmap-indexing", false, "Enable memory mapping of files while indexing", false, &enable_mmap_indexing_raw);

        bool enable_mmap_unpacking_raw = 0;
        kgflags_bool("mmap-unpacking", false, "Enable memory mapping of files unpacking", false, &enable_mmap_unpacking_raw);

        if (!kgflags_parse(argc, argv)) {
            kgflags_print_errors();
            kgflags_print_usage();
            return 1;
        }

        if (SetLogLevel(log_level_raw))
        {
            return 1;
        }

        if (enable_mem_tracer_raw) {
            Longtail_MemTracer_Init();
            Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
        }

        const char* source_path = NormalizePath(source_path_raw);
        const char* target_path = NormalizePath(target_path_raw);

        err = Unpack(
            source_path,
            target_path,
            retain_permission_raw,
            enable_mmap_indexing_raw,
            enable_mmap_unpacking_raw);

        Longtail_Free((void*)source_path);
        Longtail_Free((void*)target_path);
    }
#if defined(_CRTDBG_MAP_ALLOC)
    _CrtDumpMemoryLeaks();
#endif
    if (enable_mem_tracer_raw) {
        Longtail_MemTracer_DumpStats("longtail.csv");
        char* memtrace_stats = Longtail_MemTracer_GetStats(Longtail_GetMemTracerDetailed());
        printf("%s", memtrace_stats);
        Longtail_Free(memtrace_stats);
        Longtail_MemTracer_Dispose();
    }
    return err;
}
