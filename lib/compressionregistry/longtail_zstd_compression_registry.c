#include "longtail_zstd_compression_registry.h"

#include "longtail_compression_registry.h"

#include "../zstd/longtail_zstd.h"

struct Longtail_CompressionRegistryAPI* Longtail_CreateZStdCompressionRegistry()
{
    struct Longtail_CompressionAPI* zstd_compression = Longtail_CreateZStdCompressionAPI();
    if (zstd_compression == 0)
    {
        return 0;
    }

    uint32_t compression_types[3] = {
        Longtail_GetZStdMinQuality(),
        Longtail_GetZStdDefaultQuality(),
        Longtail_GetZStdMaxQuality()};
    struct Longtail_CompressionAPI* compression_apis[3] = {
        zstd_compression,
        zstd_compression,
        zstd_compression};
    uint32_t compression_settings[3] = {
        Longtail_GetZStdMinQuality(),
        Longtail_GetZStdDefaultQuality(),
        Longtail_GetZStdMaxQuality()};

    struct Longtail_CompressionRegistryAPI* registry = Longtail_CreateDefaultCompressionRegistry(
        3,
        (const uint32_t*)compression_types,
        (const struct Longtail_CompressionAPI **)compression_apis,
        compression_settings);
    if (registry == 0)
    {
        SAFE_DISPOSE_API(zstd_compression);
        return 0;
    }
    return registry;
}
