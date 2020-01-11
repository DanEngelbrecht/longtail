#include "longtail_lizard.h"

#include "../../src/longtail.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/lizard_common.h"
#include "ext/lizard_decompress.h"
#include "ext/lizard_compress.h"

#include <errno.h>

const uint32_t LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE = (((uint32_t)'1') << 24) + (((uint32_t)'s') << 16) + (((uint32_t)'\0') << 8) + ((uint32_t)'d');

struct LizardCompressionAPI
{
    struct Longtail_CompressionAPI m_LizardCompressionAPI;
};

void LizardCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static int LizardCompressionAPI_DefaultCompressionSetting = 44;
static int LizardCompressionAPI_MaxCompressionSetting = LIZARD_MAX_CLEVEL;

static Longtail_CompressionAPI_HSettings LizardCompressionAPI_GetDefaultSettings(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&LizardCompressionAPI_DefaultCompressionSetting;
}

static Longtail_CompressionAPI_HSettings LizardCompressionAPI_GetMaxCompressionSetting(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HSettings)&LizardCompressionAPI_MaxCompressionSetting;
}

static int LizardCompressionAPI_CreateCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, Longtail_CompressionAPI_HCompressionContext* out_context)
{
    *out_context = (Longtail_CompressionAPI_HCompressionContext)settings;
    return 0;
}

static size_t LizardCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, size_t size)
{
    return (size_t)Lizard_compressBound((int)size);
}

static int LizardCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_size)
{
    int compression_setting = *(int*)context;
    int compressed_size = Lizard_compress(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
    if (compressed_size == 0)
    {
        return ENOMEM;
    }
    *out_size = (size_t)(compressed_size);
    return 0;
}

static void LizardCompressionAPI_DeleteCompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HCompressionContext context)
{
}

static Longtail_CompressionAPI_HDecompressionContext LizardCompressionAPI_CreateDecompressionContext(struct Longtail_CompressionAPI* compression_api)
{
    return (Longtail_CompressionAPI_HDecompressionContext)LizardCompressionAPI_GetDefaultSettings(compression_api);
}

static int LizardCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size, size_t* out_size)
{
    int result = Lizard_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)uncompressed_size);
    if (result < 0)
    {
        return EBADF;
    }
    *out_size = (size_t)(result);
    return 0;
}

static void LizardCompressionAPI_DeleteDecompressionContext(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HDecompressionContext context)
{
}

static void LizardCompressionAPI_Init(struct LizardCompressionAPI* compression_api)
{
    compression_api->m_LizardCompressionAPI.m_API.Dispose = LizardCompressionAPI_Dispose;
    compression_api->m_LizardCompressionAPI.GetDefaultSettings = LizardCompressionAPI_GetDefaultSettings;
    compression_api->m_LizardCompressionAPI.GetMaxCompressionSetting = LizardCompressionAPI_GetMaxCompressionSetting;
    compression_api->m_LizardCompressionAPI.CreateCompressionContext = LizardCompressionAPI_CreateCompressionContext;
    compression_api->m_LizardCompressionAPI.GetMaxCompressedSize = LizardCompressionAPI_GetMaxCompressedSize;
    compression_api->m_LizardCompressionAPI.Compress = LizardCompressionAPI_Compress;
    compression_api->m_LizardCompressionAPI.DeleteCompressionContext = LizardCompressionAPI_DeleteCompressionContext;
    compression_api->m_LizardCompressionAPI.CreateDecompressionContext = LizardCompressionAPI_CreateDecompressionContext;
    compression_api->m_LizardCompressionAPI.Decompress = LizardCompressionAPI_Decompress;
    compression_api->m_LizardCompressionAPI.DeleteDecompressionContext = LizardCompressionAPI_DeleteDecompressionContext;
}

struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI()
{
    struct LizardCompressionAPI* compression_api = (struct LizardCompressionAPI*)Longtail_Alloc(sizeof(struct LizardCompressionAPI));
    LizardCompressionAPI_Init(compression_api);
    return &compression_api->m_LizardCompressionAPI;
}

#include "ext/entropy/entropy_common.c"
#include "ext/entropy/fse_compress.c"
#undef CHECK_F
#include "ext/entropy/fse_decompress.c"
#undef CHECK_F
#include "ext/entropy/huf_compress.c"
#undef CHECK_F
#include "ext/entropy/huf_decompress.c"
#undef CHECK_F

#include "ext/lizard_compress.c"
#include "ext/lizard_decompress.c"
#include "ext/lizard_frame.c"

//#define XXH_STATIC_LINKING_ONLY
//#include "ext/xxhash/xxhash.c"
