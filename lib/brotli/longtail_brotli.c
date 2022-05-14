#include "longtail_brotli.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/include/brotli/decode.h"
#include "ext/include/brotli/encode.h"

#include <errno.h>
#include <inttypes.h>

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

#define LONGTAIL_BROTLI_COMPRESSION_TYPE             ((((uint32_t)'b') << 24) + (((uint32_t)'t') << 16) + (((uint32_t)'l') << 8))
#define LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE     (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'0'))
#define LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'1'))
#define LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE     (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'2'))
#define LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE        (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'a'))
#define LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE    (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'b'))
#define LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE        (LONGTAIL_BROTLI_COMPRESSION_TYPE + ((uint32_t)'c'))

uint32_t Longtail_GetBrotliGenericMinQuality() { return LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE; }
uint32_t Longtail_GetBrotliGenericDefaultQuality() { return LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE; }
uint32_t Longtail_GetBrotliGenericMaxQuality() { return LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE; }
uint32_t Longtail_GetBrotliTextMinQuality() { return LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE; }
uint32_t Longtail_GetBrotliTextDefaultQuality() { return LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE; }
uint32_t Longtail_GetBrotliTextMaxQuality() { return LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE; }

struct Longtail_CompressionAPI* Longtail_CompressionRegistry_CreateForBrotli(uint32_t compression_type, uint32_t* out_settings)
{
    if ((compression_type & 0xffffff00) != LONGTAIL_BROTLI_COMPRESSION_TYPE)
    {
        return 0;
    }
    if (out_settings)
    {
        *out_settings = compression_type;
    }
    return Longtail_CreateBrotliCompressionAPI();
}

static struct BrotliSettings* SettingsIDToCompressionSetting(uint32_t settings_id)
{
    switch(settings_id)
    {
        case LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_SETTINGS;
        case LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_SETTINGS;
        case LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_SETTINGS;
        case LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_TEXT_MIN_QUALITY_SETTINGS;
        case LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_SETTINGS;
        case LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE:
            return &LONGTAIL_BROTLI_TEXT_MAX_QUALITY_SETTINGS;
       default:
           return 0;
    }
}

struct BrotliCompressionAPI
{
    struct Longtail_CompressionAPI m_BrotliCompressionAPI;
};

void BrotliCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

size_t BrotliCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size)
{
    return BrotliEncoderMaxCompressedSize((size_t)size);
}

int BrotliCompressionAPI_Compress(
    struct Longtail_CompressionAPI* compression_api,
    uint32_t settings_id,
    const char* uncompressed,
    char* compressed,
    size_t uncompressed_size,
    size_t max_compressed_size,
    size_t* out_compressed_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_api, "%p"),
        LONGTAIL_LOGFIELD(settings_id, "%u"),
        LONGTAIL_LOGFIELD(uncompressed, "%u"),
        LONGTAIL_LOGFIELD(compressed, "%u"),
        LONGTAIL_LOGFIELD(uncompressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(max_compressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_compressed_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    struct BrotliSettings* brotli_settings = SettingsIDToCompressionSetting(settings_id);
    LONGTAIL_VALIDATE_INPUT(ctx, brotli_settings != 0, return EINVAL)

    *out_compressed_size = max_compressed_size;
    if (BROTLI_FALSE == BrotliEncoderCompress(brotli_settings->m_Quality, brotli_settings->m_WindowBits, brotli_settings->m_Mode, uncompressed_size, (const uint8_t*)uncompressed, out_compressed_size, (uint8_t*)compressed))
    {
        return EINVAL;
    }
    return 0;
}

int BrotliCompressionAPI_Decompress(
    struct Longtail_CompressionAPI* compression_api,
    const char* compressed,
    char* uncompressed,
    size_t compressed_size,
    size_t max_uncompressed_size,
    size_t* out_uncompressed_size)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_api, "%p"),
        LONGTAIL_LOGFIELD(compressed, "%u"),
        LONGTAIL_LOGFIELD(uncompressed, "%u"),
        LONGTAIL_LOGFIELD(max_uncompressed_size, "%" PRIu64),
        LONGTAIL_LOGFIELD(out_uncompressed_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    compression_api->m_BrotliCompressionAPI.m_API.Dispose = BrotliCompressionAPI_Dispose;
    compression_api->m_BrotliCompressionAPI.GetMaxCompressedSize = BrotliCompressionAPI_GetMaxCompressedSize;
    compression_api->m_BrotliCompressionAPI.Compress = BrotliCompressionAPI_Compress;
    compression_api->m_BrotliCompressionAPI.Decompress = BrotliCompressionAPI_Decompress;
}

struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    struct BrotliCompressionAPI* compression_api = (struct BrotliCompressionAPI*)Longtail_Alloc("Brotli", sizeof(struct BrotliCompressionAPI));
    if (!compression_api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    BrotliCompressionAPI_Init(compression_api);
    return &compression_api->m_BrotliCompressionAPI;
}



