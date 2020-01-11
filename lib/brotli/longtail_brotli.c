#include "longtail_brotli.h"

#include "../../src/longtail.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/include/brotli/decode.h"
#include "ext/include/brotli/encode.h"

#include <errno.h>

const uint32_t LONGTAIL_BROTLI_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'b') << 24) + (((uint32_t)'r') << 16) + (((uint32_t)'t') << 8) + ((uint32_t)'0');

struct BrotliCompressionAPI
{
    struct Longtail_CompressionAPI m_BrotliCompressionAPI;
};

void BrotliCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static int BrotliCompressionAPI_DefaultCompressionSetting = 0;
static int BrotliCompressionAPI_MaxCompressionSetting = 127;

static Longtail_CompressionAPI_HSettings BrotliCompressionAPI_GetDefaultSettings(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&BrotliCompressionAPI_DefaultCompressionSetting;
}

static Longtail_CompressionAPI_HSettings BrotliCompressionAPI_GetMaxCompressionSetting(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&BrotliCompressionAPI_MaxCompressionSetting;
}

static int BrotliCompressionAPI_CreateCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, Longtail_CompressionAPI_HCompressionContext* out_context)
{
    BrotliEncoderState* state = BrotliEncoderCreateInstance(0, 0, 0);
    if (!state)
    {
        return ENOMEM;
    }
    *out_context = (Longtail_CompressionAPI_HCompressionContext)state;
    return 0;
}

static size_t BrotliCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, size_t size)
{
    BrotliEncoderState* state = (BrotliEncoderState*)context;
    return BrotliEncoderMaxCompressedSize((size_t)size);
}

static int BrotliCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* consumed_size, size_t* produced_size)
{
    BrotliEncoderState* state = (BrotliEncoderState*)context;

    const uint8_t* uncompressed_ptr = (const uint8_t*)uncompressed;
    uint8_t* compressed_ptr = (uint8_t*)compressed;
    size_t available_in = uncompressed_size;
    size_t available_out = max_compressed_size;
    if (!BrotliEncoderCompressStream(
        state,
        BROTLI_OPERATION_PROCESS,
        &available_in,
        &uncompressed_ptr,
        &available_out,
        &compressed_ptr,
        0))
    {
        return EINVAL;
    }

    *consumed_size = uncompressed_size - available_in;
    *produced_size = max_compressed_size - available_out;

    return 0;
}

static int BrotliCompressionAPI_FinishCompress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, char* compressed, size_t max_compressed_size, size_t* out_size)
{
    BrotliEncoderState* state = (BrotliEncoderState*)context;

    const uint8_t* uncompressed_ptr = 0;
    uint8_t* compressed_ptr = (uint8_t*)compressed;
    size_t available_in = 0;
    size_t available_out = max_compressed_size;
    while (!BrotliEncoderIsFinished(state)) {
        if (!BrotliEncoderCompressStream(
            state,
            BROTLI_OPERATION_FINISH,
            &available_in,
            &uncompressed_ptr,
            &available_out,
            &compressed_ptr,
            0))
        {
            return EINVAL;
        }
    }
    *out_size = max_compressed_size - available_out;
    return 0;
}

static void BrotliCompressionAPI_DeleteCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context)
{
    BrotliEncoderState* state = (BrotliEncoderState*)context;
    BrotliEncoderDestroyInstance(state);
}

static int BrotliCompressionAPI_CreateDecompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext* out_context)
{
    BrotliDecoderState* state = BrotliDecoderCreateInstance(0, 0, 0);
    if (!state)
    {
        return ENOMEM;
    }
    *out_context = (Longtail_CompressionAPI_HDecompressionContext)state;
    return 0;
}

static int BrotliCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size, size_t* consumed_size, size_t* produced_size)
{
    BrotliDecoderState* state = (BrotliDecoderState*)context;
    const uint8_t* compressed_ptr = (const uint8_t*)compressed;
    uint8_t* uncompressed_ptr = (uint8_t*)uncompressed;
    size_t available_in = compressed_size;
    size_t available_out = uncompressed_size;
    while(available_in > 0)
    {
        BrotliDecoderResult res = BrotliDecoderDecompressStream(
            state,
            &available_in,
            &compressed_ptr,
            &available_out,
            &uncompressed_ptr,
            0);
        if (res == BROTLI_DECODER_RESULT_ERROR)
        {
            return EBADF;
        }
    }
    *consumed_size = compressed_size - available_in;
    *produced_size = uncompressed_size - available_out;
    return 0;
}

static void BrotliCompressionAPI_DeleteDecompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context)
{
    BrotliDecoderState* state = (BrotliDecoderState*)context;
    BrotliDecoderDestroyInstance(state);
}

static void BrotliCompressionAPI_Init(struct BrotliCompressionAPI* compression_api)
{
    compression_api->m_BrotliCompressionAPI.m_API.Dispose = BrotliCompressionAPI_Dispose;
    compression_api->m_BrotliCompressionAPI.GetDefaultSettings = BrotliCompressionAPI_GetDefaultSettings;
    compression_api->m_BrotliCompressionAPI.GetMaxCompressionSetting = BrotliCompressionAPI_GetMaxCompressionSetting;
    compression_api->m_BrotliCompressionAPI.CreateCompressionContext = BrotliCompressionAPI_CreateCompressionContext;
    compression_api->m_BrotliCompressionAPI.GetMaxCompressedSize = BrotliCompressionAPI_GetMaxCompressedSize;
    compression_api->m_BrotliCompressionAPI.Compress = BrotliCompressionAPI_Compress;
    compression_api->m_BrotliCompressionAPI.FinishCompress = BrotliCompressionAPI_FinishCompress;
    compression_api->m_BrotliCompressionAPI.DeleteCompressionContext = BrotliCompressionAPI_DeleteCompressionContext;
    compression_api->m_BrotliCompressionAPI.CreateDecompressionContext = BrotliCompressionAPI_CreateDecompressionContext;
    compression_api->m_BrotliCompressionAPI.Decompress = BrotliCompressionAPI_Decompress;
    compression_api->m_BrotliCompressionAPI.DeleteDecompressionContext = BrotliCompressionAPI_DeleteDecompressionContext;
}

struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI()
{
    struct BrotliCompressionAPI* compression_api = (struct BrotliCompressionAPI*)Longtail_Alloc(sizeof(struct BrotliCompressionAPI));
    BrotliCompressionAPI_Init(compression_api);
    return &compression_api->m_BrotliCompressionAPI;
}



