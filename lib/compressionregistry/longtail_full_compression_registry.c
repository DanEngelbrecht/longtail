#include "longtail_full_compression_registry.h"

#include "longtail_compression_registry.h"

//#include "../brotli/longtail_brotli.h"
#include "../lz4/longtail_lz4.h"
#include "../zstd/longtail_zstd.h"

struct Longtail_CompressionRegistryAPI* Longtail_CreateFullCompressionRegistry()
{
    Longtail_CompressionRegistry_CreateForTypeFunc compression_create_api_funcs[2] = {
        //Longtail_CompressionRegistry_CreateForBrotli,
        Longtail_CompressionRegistry_CreateForLZ4,
        Longtail_CompressionRegistry_CreateForZstd};

    return Longtail_CreateDefaultCompressionRegistry(
        2,
        (const Longtail_CompressionRegistry_CreateForTypeFunc*)compression_create_api_funcs);
}
