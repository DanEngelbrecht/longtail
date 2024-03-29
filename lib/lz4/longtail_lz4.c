#include "longtail_lz4.h"

#include "ext/lz4.h"

#include <errno.h>
#include <inttypes.h>

static int LZ4CompressionAPI_DefaultCompressionSetting   = 1;

#define LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE ((((uint32_t)'l') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'4') << 8) + ((uint32_t)'2'))

struct Longtail_CompressionAPI* Longtail_CompressionRegistry_CreateForLZ4(uint32_t compression_type, uint32_t* out_settings)
{
    if (compression_type != LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE)
    {
        return 0;
    }
    if (out_settings)
    {
        *out_settings = compression_type;
    }
    return Longtail_CreateLZ4CompressionAPI();
}

uint32_t Longtail_GetLZ4DefaultQuality() { return LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE; }

static int SettingsIDToCompressionSetting(uint32_t settings_id)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(settings_id, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, settings_id == LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE, return -1)
    return LZ4CompressionAPI_DefaultCompressionSetting;
}

struct LZ4CompressionAPI
{
    struct Longtail_CompressionAPI m_LZ4CompressionAPI;
};

void LZ4CompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static size_t LZ4CompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size)
{
    return (size_t)LZ4_COMPRESSBOUND((unsigned)size);
}

int LZ4CompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
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

    int compression_setting = SettingsIDToCompressionSetting(settings_id);
    int compressed_size = LZ4_compress_fast(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
    if (compressed_size == 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LZ4_compress_fast() failed with %d", ENOMEM);
        return ENOMEM;
    }
    *out_compressed_size = (size_t)compressed_size;
    return 0;
}

static int LZ4CompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
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

    int result = LZ4_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)max_uncompressed_size);
    if (result < 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "LZ4_decompress_safe() failed with %d", EBADF);
        return EBADF;
    }
    *out_uncompressed_size = (size_t)(result);
    return 0;
}

static void LZ4CompressionAPI_Init(struct LZ4CompressionAPI* compression_api)
{
    compression_api->m_LZ4CompressionAPI.m_API.Dispose = LZ4CompressionAPI_Dispose;
    compression_api->m_LZ4CompressionAPI.GetMaxCompressedSize = LZ4CompressionAPI_GetMaxCompressedSize;
    compression_api->m_LZ4CompressionAPI.Compress = LZ4CompressionAPI_Compress;
    compression_api->m_LZ4CompressionAPI.Decompress = LZ4CompressionAPI_Decompress;
}

struct Longtail_CompressionAPI* Longtail_CreateLZ4CompressionAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    struct LZ4CompressionAPI* compression_api = (struct LZ4CompressionAPI*)Longtail_Alloc("LZ4CompressionAPI", sizeof(struct LZ4CompressionAPI));
    if (!compression_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    LZ4CompressionAPI_Init(compression_api);
    return &compression_api->m_LZ4CompressionAPI;
}
