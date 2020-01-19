#include "longtail_zstd.h"

#include "ext/zstd.h"
#include "ext/common/zstd_errors.h"

#include <errno.h>


const uint32_t LONGTAIL_ZSTD_MIN_COMPRESSION_LEVEL      = 0;
const uint32_t LONGTAIL_ZSTD_DEFAULT_COMPRESSION_LEVEL  = ZSTD_CLEVEL_DEFAULT;
const uint32_t LONGTAIL_ZSTD_MAX_COMPRESSION_LEVEL      = 19;

Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MIN_COMPRESSION        =(Longtail_CompressionAPI_HSettings)&LONGTAIL_ZSTD_MIN_COMPRESSION_LEVEL;
Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_DEFAULT_COMPRESSION    =(Longtail_CompressionAPI_HSettings)&LONGTAIL_ZSTD_DEFAULT_COMPRESSION_LEVEL;
Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MAX_COMPRESSION        =(Longtail_CompressionAPI_HSettings)&LONGTAIL_ZSTD_MAX_COMPRESSION_LEVEL;

struct ZStdCompressionAPI
{
    struct Longtail_CompressionAPI m_ZStdCompressionAPI;
};

void ZStdCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static size_t ZStdCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, size_t size)
{
    return ZSTD_COMPRESSBOUND(size);
}

int ZStdCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
    int compression_level = *(int*)settings;
    size_t size = ZSTD_compress( compressed, max_compressed_size, uncompressed, uncompressed_size, compression_level);
    if (ZSTD_isError(size))
    {
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
    ZStdCompressionAPI_Init(compression_api);
    return &compression_api->m_ZStdCompressionAPI;
}
