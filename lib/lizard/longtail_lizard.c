#include "longtail_lizard.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/lizard_common.h"
#include "ext/lizard_decompress.h"
#include "ext/lizard_compress.h"

#include <errno.h>

static int LizardCompressionAPI_MinCompressionSetting       = LIZARD_MIN_CLEVEL;
static int LizardCompressionAPI_DefaultCompressionSetting   = 44;
static int LizardCompressionAPI_MaxCompressionSetting       = LIZARD_MAX_CLEVEL;

Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MIN_COMPRESSION      = (Longtail_CompressionAPI_HSettings)&LizardCompressionAPI_MinCompressionSetting;
Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_DEFAULT_COMPRESSION  = (Longtail_CompressionAPI_HSettings)&LizardCompressionAPI_DefaultCompressionSetting;
Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MAX_COMPRESSION      = (Longtail_CompressionAPI_HSettings)&LizardCompressionAPI_MaxCompressionSetting;

struct LizardCompressionAPI
{
    struct Longtail_CompressionAPI m_LizardCompressionAPI;
};

void LizardCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static size_t LizardCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, size_t size)
{
    return (size_t)Lizard_compressBound((int)size);
}

int LizardCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
    int compression_setting = *(int*)settings;
    int compressed_size = Lizard_compress(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
    if (compressed_size == 0)
    {
        return ENOMEM;
    }
    *out_compressed_size = (size_t)compressed_size;
    return 0;
}

static int LizardCompressionAPI_Decompress(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size)
{
    int result = Lizard_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)max_uncompressed_size);
    if (result < 0)
    {
        return EBADF;
    }
    *out_uncompressed_size = (size_t)(result);
    return 0;
}

static void LizardCompressionAPI_Init(struct LizardCompressionAPI* compression_api)
{
    compression_api->m_LizardCompressionAPI.m_API.Dispose = LizardCompressionAPI_Dispose;
    compression_api->m_LizardCompressionAPI.GetMaxCompressedSize = LizardCompressionAPI_GetMaxCompressedSize;
    compression_api->m_LizardCompressionAPI.Compress = LizardCompressionAPI_Compress;
    compression_api->m_LizardCompressionAPI.Decompress = LizardCompressionAPI_Decompress;
}

struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI()
{
    struct LizardCompressionAPI* compression_api = (struct LizardCompressionAPI*)Longtail_Alloc(sizeof(struct LizardCompressionAPI));
    LizardCompressionAPI_Init(compression_api);
    return &compression_api->m_LizardCompressionAPI;
}
