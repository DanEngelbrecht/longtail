#include "longtail_zstd.h"

#include "ext/zstd.h"
#include "ext/zstd_errors.h"
#include "ext/compress/clevels.h"

#include <errno.h>
#include <inttypes.h>


const int LONGTAIL_ZSTD_MIN_COMPRESSION_LEVEL      = 0;
const int LONGTAIL_ZSTD_LOW_COMPRESSION_TYPE       = 2;
const int LONGTAIL_ZSTD_DEFAULT_COMPRESSION_LEVEL  = ZSTD_CLEVEL_DEFAULT;
const int LONGTAIL_ZSTD_HIGH_COMPRESSION_LEVEL     = 8;
const int LONGTAIL_ZSTD_MAX_COMPRESSION_LEVEL      = ZSTD_MAX_CLEVEL;

#define LONGTAIL_ZSTD_COMPRESSION_TYPE         ((((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8))
#define LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE     (LONGTAIL_ZSTD_COMPRESSION_TYPE + ((uint32_t)'1'))
#define LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE (LONGTAIL_ZSTD_COMPRESSION_TYPE + ((uint32_t)'2'))
#define LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE     (LONGTAIL_ZSTD_COMPRESSION_TYPE + ((uint32_t)'3'))
#define LONGTAIL_ZSTD_HIGH_COMPRESSION_TYPE    (LONGTAIL_ZSTD_COMPRESSION_TYPE + ((uint32_t)'4'))
#define LONGTAIL_ZSTD_LOW_COMPRESSION_TYPE     (LONGTAIL_ZSTD_COMPRESSION_TYPE + ((uint32_t)'5'))

uint32_t Longtail_GetZStdMinQuality() { return LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdDefaultQuality() { return LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdMaxQuality() { return LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdHighQuality() { return LONGTAIL_ZSTD_HIGH_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdLowQuality() { return LONGTAIL_ZSTD_LOW_COMPRESSION_TYPE; }

struct Longtail_CompressionAPI* Longtail_CompressionRegistry_CreateForZstd(uint32_t compression_type, uint32_t* out_settings)
{
    if ((compression_type & 0xffffff00) != LONGTAIL_ZSTD_COMPRESSION_TYPE)
    {
        return 0;
    }
    if (out_settings)
    {
        *out_settings = compression_type;
    }
    return Longtail_CreateZStdCompressionAPI();
}

static int SettingsIDToCompressionSetting(uint32_t settings_id)
{
    switch(settings_id)
    {
        case LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE:
            return LONGTAIL_ZSTD_MIN_COMPRESSION_LEVEL;
        case LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE:
            return LONGTAIL_ZSTD_DEFAULT_COMPRESSION_LEVEL;
        case LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE:
            return LONGTAIL_ZSTD_MAX_COMPRESSION_LEVEL;
        case LONGTAIL_ZSTD_HIGH_COMPRESSION_TYPE:
            return LONGTAIL_ZSTD_HIGH_COMPRESSION_LEVEL;
        case LONGTAIL_ZSTD_LOW_COMPRESSION_TYPE:
            return LONGTAIL_ZSTD_LOW_COMPRESSION_TYPE;
       default:
           return 0;
    }
}

struct ZStdCompressionAPI
{
    struct Longtail_CompressionAPI m_ZStdCompressionAPI;
};

void ZStdCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static size_t ZStdCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size)
{
    return ZSTD_COMPRESSBOUND(size);
}

static void* alloc(void* opaque, size_t size)
{
    return Longtail_Alloc("ZStdCompressionAPI", size);
}

static void  free(void* opaque, void* address)
{
    Longtail_Free(address);
}

ZSTD_CCtx* ZSTD_CreateCompressContext()
{
    ZSTD_customMem customMem;
    customMem.customAlloc = alloc;
    customMem.customFree = free;
    customMem.opaque = 0;
    ZSTD_CCtx* ctx = ZSTD_createCCtx_advanced(customMem);
    return ctx;
}

ZSTD_DCtx* ZSTD_CreateDecompressContext()
{
    ZSTD_customMem customMem;
    customMem.customAlloc = alloc;
    customMem.customFree = free;
    customMem.opaque = 0;
    ZSTD_DCtx* ctx = ZSTD_createDCtx_advanced(customMem);
    return ctx;
}

int ZStdCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_api, "%p"),
        LONGTAIL_LOGFIELD(settings_id, "%u"),
        LONGTAIL_LOGFIELD(uncompressed, "%p"),
        LONGTAIL_LOGFIELD(compressed, "%p"),
        LONGTAIL_LOGFIELD(uncompressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(max_compressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_compressed_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    ZSTD_CCtx* zstd_ctx = ZSTD_CreateCompressContext();
    if (!zstd_ctx)
    {
        int err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ZSTD_CreateContext() failed with %d", err);
        return err;
    }
    int compression_setting = SettingsIDToCompressionSetting(settings_id);
    size_t size = ZSTD_compressCCtx(zstd_ctx, compressed, max_compressed_size, uncompressed, uncompressed_size, compression_setting);
    if (ZSTD_isError(size))
    {
        int err = ZSTD_getErrorCode(size);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ZSTD_compress() failed with %d", err);
        ZSTD_freeCCtx(zstd_ctx);
        return EINVAL;
    }
    *out_compressed_size = size;
    ZSTD_freeCCtx(zstd_ctx);
    return 0;
}


int ZStdCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_api, "%p"),
        LONGTAIL_LOGFIELD(compressed, "%p"),
        LONGTAIL_LOGFIELD(uncompressed, "%p"),
        LONGTAIL_LOGFIELD(compressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(max_uncompressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_uncompressed_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    ZSTD_DCtx* zstd_ctx = ZSTD_CreateDecompressContext();
    if (!zstd_ctx)
    {
        int err = ENOMEM;
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ZSTD_CreateContext() failed with %d", err);
        return err;
    }

    size_t size = ZSTD_decompressDCtx(zstd_ctx, uncompressed, max_uncompressed_size, compressed, compressed_size);
    if (ZSTD_isError(size))
    {
        int err = ZSTD_getErrorCode(size);
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ZSTD_decompress() failed with %d", err);
        ZSTD_freeDCtx(zstd_ctx);
        return EINVAL;
    }
    *out_uncompressed_size = size;
    ZSTD_freeDCtx(zstd_ctx);
    return 0;
}

static void ZStdCompressionAPI_Init(struct ZStdCompressionAPI* compression_api)
{
    compression_api->m_ZStdCompressionAPI.m_API.Dispose = ZStdCompressionAPI_Dispose;
    compression_api->m_ZStdCompressionAPI.GetMaxCompressedSize = ZStdCompressionAPI_GetMaxCompressedSize;
    compression_api->m_ZStdCompressionAPI.Compress = ZStdCompressionAPI_Compress;
    compression_api->m_ZStdCompressionAPI.Decompress = ZStdCompressionAPI_Decompress;
}

struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    struct ZStdCompressionAPI* compression_api = (struct ZStdCompressionAPI*)Longtail_Alloc("ZStdCompressionAPI", sizeof(struct ZStdCompressionAPI));
    if (!compression_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    ZStdCompressionAPI_Init(compression_api);
    return &compression_api->m_ZStdCompressionAPI;
}
