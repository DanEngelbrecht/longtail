#include "longtail_brotli.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/include/brotli/decode.h"
#include "ext/include/brotli/encode.h"

#include <errno.h>

struct BrotliSettings
{
    BrotliEncoderMode m_Mode;
    int m_WindowBits;
    int m_Quality;
};

static struct BrotliSettings LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_SETTINGS     = {BROTLI_MODE_GENERIC, BROTLI_MIN_WINDOW_BITS, BROTLI_MIN_QUALITY};
static struct BrotliSettings LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_SETTINGS = {BROTLI_MODE_GENERIC, BROTLI_DEFAULT_WINDOW,  BROTLI_DEFAULT_QUALITY};
static struct BrotliSettings LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_SETTINGS     = {BROTLI_MODE_GENERIC, BROTLI_MAX_WINDOW_BITS, BROTLI_MAX_QUALITY};
static struct BrotliSettings LONGTAIL_BROTLI_TEXT_MIN_QUALITY_SETTINGS        = {BROTLI_MODE_TEXT,    BROTLI_MIN_WINDOW_BITS, BROTLI_MIN_QUALITY};
static struct BrotliSettings LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_SETTINGS    = {BROTLI_MODE_TEXT,    BROTLI_DEFAULT_WINDOW,  BROTLI_DEFAULT_QUALITY};
static struct BrotliSettings LONGTAIL_BROTLI_TEXT_MAX_QUALITY_SETTINGS        = {BROTLI_MODE_TEXT,    BROTLI_MAX_WINDOW_BITS, BROTLI_MAX_QUALITY};

Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_MIN_QUALITY       = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_SETTINGS;
Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY   = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_SETTINGS;
Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_MAX_QUALITY       = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_SETTINGS;
Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_MIN_QUALITY          = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_TEXT_MIN_QUALITY_SETTINGS;
Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY      = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_SETTINGS;
Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_MAX_QUALITY          = (Longtail_CompressionAPI_HSettings)&LONGTAIL_BROTLI_TEXT_MAX_QUALITY_SETTINGS;

struct BrotliCompressionAPI
{
    struct Longtail_CompressionAPI m_BrotliCompressionAPI;
};

void BrotliCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

size_t BrotliCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, size_t size)
{
    return BrotliEncoderMaxCompressedSize((size_t)size);
}

int BrotliCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
    struct BrotliSettings* brotli_settings = (struct BrotliSettings*)settings;

    *out_compressed_size = max_compressed_size;
    if (BROTLI_FALSE == BrotliEncoderCompress(brotli_settings->m_Quality, brotli_settings->m_WindowBits, brotli_settings->m_Mode, uncompressed_size, (const uint8_t*)uncompressed, out_compressed_size, (uint8_t*)compressed))
    {
        return EINVAL;
    }
    return 0;
}

int BrotliCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
{
    *out_uncompressed_size = max_uncompressed_size;
    BrotliDecoderResult result = BrotliDecoderDecompress(
        compressed_size,
        (const uint8_t*)compressed,
        out_uncompressed_size,
        (uint8_t*)uncompressed);
    switch (result)
    {
        case BROTLI_DECODER_RESULT_ERROR:
        return EBADF;
        case BROTLI_DECODER_RESULT_SUCCESS:
        return 0;
        default:
        return EINVAL;
    }
}

static void BrotliCompressionAPI_Init(struct BrotliCompressionAPI* compression_api)
{
    compression_api->m_BrotliCompressionAPI.m_API.Dispose = BrotliCompressionAPI_Dispose;
    compression_api->m_BrotliCompressionAPI.GetMaxCompressedSize = BrotliCompressionAPI_GetMaxCompressedSize;
    compression_api->m_BrotliCompressionAPI.Compress = BrotliCompressionAPI_Compress;
    compression_api->m_BrotliCompressionAPI.Decompress = BrotliCompressionAPI_Decompress;
}

struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI()
{
    struct BrotliCompressionAPI* compression_api = (struct BrotliCompressionAPI*)Longtail_Alloc(sizeof(struct BrotliCompressionAPI));
    BrotliCompressionAPI_Init(compression_api);
    return &compression_api->m_BrotliCompressionAPI;
}



