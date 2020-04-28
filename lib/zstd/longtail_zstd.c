#include "longtail_zstd.h"

#include "ext/zstd.h"
#include "ext/common/zstd_errors.h"

#include <errno.h>
#include <inttypes.h>


const int LONGTAIL_ZSTD_MIN_COMPRESSION_LEVEL      = 0;
const int LONGTAIL_ZSTD_DEFAULT_COMPRESSION_LEVEL  = ZSTD_CLEVEL_DEFAULT;
const int LONGTAIL_ZSTD_MAX_COMPRESSION_LEVEL      = 19;

#define LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE     ((((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'1'))
#define LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE ((((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'2'))
#define LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE     ((((uint32_t)'z') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'3'))

uint32_t Longtail_GetZStdMinQuality() { return LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdDefaultQuality() { return LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE; }
uint32_t Longtail_GetZStdMaxQuality() { return LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE; }

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

int ZStdCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
    int compression_setting = SettingsIDToCompressionSetting(settings_id);
    size_t size = ZSTD_compress( compressed, max_compressed_size, uncompressed, uncompressed_size, compression_setting);
    if (ZSTD_isError(size))
    {
        int err = ZSTD_getErrorCode(size);
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "ZStdCompressionAPI_Compress(%p, %u, %p, %p, %" PRIu64 ", %" PRIu64 ", %p) failed with %d",
            compression_api, settings_id, uncompressed, compressed, uncompressed_size, max_compressed_size, out_compressed_size,
            err);
        return EINVAL;
    }
    *out_compressed_size = size;
    return 0;
}


int ZStdCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
{
    size_t size = ZSTD_decompress( uncompressed, max_uncompressed_size, compressed, compressed_size);
    if (ZSTD_isError(size))
    {
        return EINVAL;
    }
    *out_uncompressed_size = size;
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
    struct ZStdCompressionAPI* compression_api = (struct ZStdCompressionAPI*)Longtail_Alloc(sizeof(struct ZStdCompressionAPI));
    if (!compression_api)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateZStdCompressionAPI() failed with %d",
            ENOMEM)
        return 0;
    }
    ZStdCompressionAPI_Init(compression_api);
    return &compression_api->m_ZStdCompressionAPI;
}
