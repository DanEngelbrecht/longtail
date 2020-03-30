#include "longtail_lz4.h"

#include "ext/lz4.h"

//#define FSE_STATIC_LINKING_ONLY
//#include "ext/lizard_common.h"
//#include "ext/lizard_decompress.h"
//#include "ext/lizard_compress.h"

#include <errno.h>

static int LZ4CompressionAPI_DefaultCompressionSetting   = 1;

#define LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE ((((uint32_t)'l') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'4') << 8) + ((uint32_t)'2'))

uint32_t Longtail_GetLZ4DefaultQuality() { return LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE; }

static int SettingsIDToCompressionSetting(uint32_t settings_id)
{
    LONGTAIL_FATAL_ASSERT(settings_id == LONGTAIL_LZ4_DEFAULT_COMPRESSION_TYPE, return -1)
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
    int compression_setting = SettingsIDToCompressionSetting(settings_id);
    int compressed_size = LZ4_compress_fast(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
    if (compressed_size == 0)
    {
        return ENOMEM;
    }
    *out_compressed_size = (size_t)compressed_size;
    return 0;
}

static int LZ4CompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
{
    int result = LZ4_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)max_uncompressed_size);
    if (result < 0)
    {
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
    struct LZ4CompressionAPI* compression_api = (struct LZ4CompressionAPI*)Longtail_Alloc(sizeof(struct LZ4CompressionAPI));
    LZ4CompressionAPI_Init(compression_api);
    return &compression_api->m_LZ4CompressionAPI;
}
