#include "longtail_lizard.h"

#define FSE_STATIC_LINKING_ONLY
#include "ext/lizard_common.h"
#include "ext/lizard_decompress.h"
#include "ext/lizard_compress.h"

#include <errno.h>

static int LizardCompressionAPI_MinCompressionSetting       = LIZARD_MIN_CLEVEL;
static int LizardCompressionAPI_DefaultCompressionSetting   = 44;
static int LizardCompressionAPI_MaxCompressionSetting       = LIZARD_MAX_CLEVEL;

#define LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE     ((((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'1'))
#define LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE ((((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'2'))
#define LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE     ((((uint32_t)'1') << 24) + (((uint32_t)'z') << 16) + (((uint32_t)'d') << 8) + ((uint32_t)'3'))

uint32_t Longtail_GetLizardMinQuality() { return LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE; }
uint32_t Longtail_GetLizardDefaultQuality() { return LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE; }
uint32_t Longtail_GetLizardMaxQuality() { return LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE; }

static int SettingsIDToCompressionSetting(uint32_t settings_id)
{
    switch(settings_id)
    {
        case LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE:
            return LizardCompressionAPI_MinCompressionSetting;
        case LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE:
            return LizardCompressionAPI_DefaultCompressionSetting;
        case LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE:
            return LizardCompressionAPI_MaxCompressionSetting;
       default:
           return 0;
    }
}

struct LizardCompressionAPI
{
    struct Longtail_CompressionAPI m_LizardCompressionAPI;
};

void LizardCompressionAPI_Dispose(struct Longtail_API* compression_api)
{
    Longtail_Free(compression_api);
}

static size_t LizardCompressionAPI_GetMaxCompressedSize(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size)
{
    return (size_t)Lizard_compressBound((int)size);
}

int LizardCompressionAPI_Compress(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size)
{
    int compression_setting = SettingsIDToCompressionSetting(settings_id);
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
