#include "longtail_zstd_compression_registry.h"

#include "longtail_compression_registry.h"

#include "../zstd/longtail_zstd.h"

struct Longtail_CompressionRegistryAPI* Longtail_CreateZStdCompressionRegistry()
{
    Longtail_CompressionRegistry_CreateForTypeFunc compression_create_api_funcs[1] = {
        Longtail_CompressionRegistry_CreateForZstd};

    return Longtail_CreateDefaultCompressionRegistry(
        1,
        (const Longtail_CompressionRegistry_CreateForTypeFunc*)compression_create_api_funcs);
}
