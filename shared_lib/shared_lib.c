#if defined(_WIN32)
#define LONGTAIL_EXPORT __declspec(dllexport)
#endif

#include "../src/longtail.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/brotli/longtail_brotli.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/compressblockstore/longtail_compressblockstore.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/lizard/longtail_lizard.h"
#include "../lib/lz4/longtail_lz4.h"
#include "../lib/memstorage/longtail_memstorage.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/zstd/longtail_zstd.h"




LONGTAIL_EXPORT struct Longtail_CompressionRegistryAPI* Longtail_CreateCompressionRegistry()
{
    struct Longtail_CompressionAPI* lizard_compression = Longtail_CreateLizardCompressionAPI();
    if (lizard_compression == 0)
    {
        return 0;
    }

    struct Longtail_CompressionAPI* lz4_compression = Longtail_CreateLZ4CompressionAPI();
    if (lz4_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        return 0;
    }

    struct Longtail_CompressionAPI* brotli_compression = Longtail_CreateBrotliCompressionAPI();
    if (brotli_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        Longtail_DisposeAPI(&lz4_compression->m_API);
        return 0;
    }

    struct Longtail_CompressionAPI* zstd_compression = Longtail_CreateZStdCompressionAPI();
    if (zstd_compression == 0)
    {
        Longtail_DisposeAPI(&lizard_compression->m_API);
        Longtail_DisposeAPI(&lz4_compression->m_API);
        Longtail_DisposeAPI(&brotli_compression->m_API);
        return 0;
    }

    uint32_t compression_types[13] = {
        Longtail_GetBrotliGenericMinQuality(),
        Longtail_GetBrotliGenericDefaultQuality(),
        Longtail_GetBrotliGenericMaxQuality(),
        Longtail_GetBrotliTextMinQuality(),
        Longtail_GetBrotliTextDefaultQuality(),
        Longtail_GetBrotliTextMaxQuality(),

        Longtail_GetLizardMinQuality(),
        Longtail_GetLizardDefaultQuality(),
        Longtail_GetLizardMaxQuality(),

        Longtail_GetLZ4DefaultQuality(),

        Longtail_GetZStdMinQuality(),
        Longtail_GetZStdDefaultQuality(),
        Longtail_GetZStdMaxQuality()};
    struct Longtail_CompressionAPI* compression_apis[13] = {
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        brotli_compression,
        lizard_compression,
        lizard_compression,
        lizard_compression,
        lz4_compression,
        zstd_compression,
        zstd_compression,
        zstd_compression};
    uint32_t compression_settings[13] = {
        Longtail_GetBrotliGenericMinQuality(),
        Longtail_GetBrotliGenericDefaultQuality(),
        Longtail_GetBrotliGenericMaxQuality(),
        Longtail_GetBrotliTextMinQuality(),
        Longtail_GetBrotliTextDefaultQuality(),
        Longtail_GetBrotliTextMaxQuality(),
        Longtail_GetLizardMinQuality(),
        Longtail_GetLizardDefaultQuality(),
        Longtail_GetLizardMaxQuality(),
        Longtail_GetLZ4DefaultQuality(),
        Longtail_GetZStdMinQuality(),
        Longtail_GetZStdDefaultQuality(),
        Longtail_GetZStdMaxQuality()};

    struct Longtail_CompressionRegistryAPI* registry = Longtail_CreateDefaultCompressionRegistry(
        13,
        (const uint32_t*)compression_types,
        (const struct Longtail_CompressionAPI **)compression_apis,
        compression_settings);
    if (registry == 0)
    {
        SAFE_DISPOSE_API(lizard_compression);
        SAFE_DISPOSE_API(lz4_compression);
        SAFE_DISPOSE_API(brotli_compression);
        SAFE_DISPOSE_API(zstd_compression);
        return 0;
    }
    return registry;
}


#if defined(_WIN32)

#include <Windows.h>

BOOL WINAPI DllMain(
    HINSTANCE hinstDLL,
    DWORD     fdwReason,
    LPVOID    lpvReserved)
{
    return TRUE;
}

#endif
