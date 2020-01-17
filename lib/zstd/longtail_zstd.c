#include "longtail_zstd.h"

#include "../../src/longtail.h"

#include "ext/zstd.h"
#include "ext/common/zstd_errors.h"

#include <errno.h>

const uint32_t LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'z') << 24) + (((uint32_t)'s') << 16) + (((uint32_t)'t') << 8) + ((uint32_t)'d');

struct ZStdCompressionAPI
{
    struct Longtail_CompressionAPI m_ZStdCompressionAPI;
};

void ZStdCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static int ZStdCompressionAPI_DefaultCompressionSetting = ZSTD_CLEVEL_DEFAULT;
static int ZStdCompressionAPI_MaxCompressionSetting = 19;

static Longtail_CompressionAPI_HSettings ZStdCompressionAPI_GetDefaultSettings(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&ZStdCompressionAPI_DefaultCompressionSetting;
}

static Longtail_CompressionAPI_HSettings ZStdCompressionAPI_GetMaxCompressionSetting(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&ZStdCompressionAPI_MaxCompressionSetting;
}

static int ZStdCompressionAPI_CreateCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, Longtail_CompressionAPI_HCompressionContext* out_context)
{
    ZSTD_CStream* stream = ZSTD_createCStream();
    if (!stream)
    {
        return ENOMEM;
    }
    size_t res = ZSTD_CCtx_setParameter(stream, ZSTD_c_compressionLevel, *((int*)settings));
    if (ZSTD_isError(res))
    {
        ZSTD_freeCStream(stream);
        if (ZSTD_error_memory_allocation == ZSTD_getErrorCode(res))
        {
            return ENOMEM;
        }
        return EINVAL;
    }

    *out_context = (Longtail_CompressionAPI_HCompressionContext)stream;
    return 0;
}

static size_t ZStdCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, size_t size)
{
    return ZSTD_COMPRESSBOUND(size);
}

static int ZStdCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* consumed_size, size_t* produced_size)
{
    ZSTD_CStream* stream = (ZSTD_CStream*)context;
    ZSTD_inBuffer in_buffer = {uncompressed, uncompressed_size, 0};
    ZSTD_outBuffer out_buffer = {compressed, max_compressed_size, 0};
    size_t res = ZSTD_compressStream2( stream,
                                         &out_buffer,
                                         &in_buffer,
                                         ZSTD_e_continue);
    if (ZSTD_isError(res))
    {
        if (ZSTD_error_memory_allocation == ZSTD_getErrorCode(res))
        {
            return ENOMEM;
        }
        return EINVAL;
    }

    *consumed_size = in_buffer.pos;
    *produced_size = out_buffer.pos;

    return 0;
}

static void ZStdCompressionAPI_DeleteCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context)
{
    ZSTD_CStream* stream = (ZSTD_CStream*)context;
    ZSTD_freeCStream(stream);
}

static int ZStdCompressionAPI_FinishCompress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, char* compressed, size_t max_compressed_size, size_t* out_size)
{
    ZSTD_CStream* stream = (ZSTD_CStream*)context;
    ZSTD_inBuffer in_buffer = {0, 0, 0};
    ZSTD_outBuffer out_buffer = {compressed, max_compressed_size, 0};
    size_t res = ZSTD_compressStream2( stream,
                                         &out_buffer,
                                         &in_buffer,
                                         ZSTD_e_end);
    if (ZSTD_isError(res))
    {
        if (ZSTD_error_memory_allocation == ZSTD_getErrorCode(res))
        {
            return ENOMEM;
        }
        return EINVAL;
    }

    *out_size = out_buffer.pos;
    return 0;
}

static int ZStdCompressionAPI_CreateDecompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext* out_context)
{
    ZSTD_DStream* stream = ZSTD_createDStream();
    if (!stream)
    {
        return ENOMEM;
    }
    *out_context = (Longtail_CompressionAPI_HDecompressionContext)stream;
    return 0;
}

static int ZStdCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size, size_t* consumed_size, size_t* produced_size)
{
    ZSTD_DStream* stream = (ZSTD_DStream*)context;
    ZSTD_inBuffer in_buffer = {compressed, compressed_size, 0};
    ZSTD_outBuffer out_buffer = {uncompressed, uncompressed_size, 0};
    size_t res = ZSTD_decompressStream( stream,
                                         &out_buffer,
                                         &in_buffer);
    if (ZSTD_isError(res))
    {
        if (ZSTD_error_memory_allocation == ZSTD_getErrorCode(res))
        {
            return ENOMEM;
        }
        return EINVAL;
    }

    *consumed_size = in_buffer.pos;
    *produced_size = out_buffer.pos;

    return 0;
}

static void ZStdCompressionAPI_DeleteDecompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context)
{
    ZSTD_DStream* stream = (ZSTD_DStream*)context;
    ZSTD_freeDStream(stream);
}

static void ZStdCompressionAPI_Init(struct ZStdCompressionAPI* compression_api)
{
    compression_api->m_ZStdCompressionAPI.m_API.Dispose = ZStdCompressionAPI_Dispose;
    compression_api->m_ZStdCompressionAPI.GetDefaultSettings = ZStdCompressionAPI_GetDefaultSettings;
    compression_api->m_ZStdCompressionAPI.GetMaxCompressionSetting = ZStdCompressionAPI_GetMaxCompressionSetting;
    compression_api->m_ZStdCompressionAPI.CreateCompressionContext = ZStdCompressionAPI_CreateCompressionContext;
    compression_api->m_ZStdCompressionAPI.GetMaxCompressedSize = ZStdCompressionAPI_GetMaxCompressedSize;
    compression_api->m_ZStdCompressionAPI.Compress = ZStdCompressionAPI_Compress;
    compression_api->m_ZStdCompressionAPI.FinishCompress = ZStdCompressionAPI_FinishCompress;
    compression_api->m_ZStdCompressionAPI.DeleteCompressionContext = ZStdCompressionAPI_DeleteCompressionContext;
    compression_api->m_ZStdCompressionAPI.CreateDecompressionContext = ZStdCompressionAPI_CreateDecompressionContext;
    compression_api->m_ZStdCompressionAPI.Decompress = ZStdCompressionAPI_Decompress;
    compression_api->m_ZStdCompressionAPI.DeleteDecompressionContext = ZStdCompressionAPI_DeleteDecompressionContext;
}

struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI()
{
    struct ZStdCompressionAPI* compression_api = (struct ZStdCompressionAPI*)Longtail_Alloc(sizeof(struct ZStdCompressionAPI));
    ZStdCompressionAPI_Init(compression_api);
    return &compression_api->m_ZStdCompressionAPI;
}
