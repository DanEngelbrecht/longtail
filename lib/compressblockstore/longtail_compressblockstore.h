#pragma once

#include <stdint.h>
#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Longtail_CompressionAPI_CompressionContext* Longtail_CompressionAPI_HCompressionContext;
typedef struct Longtail_CompressionAPI_DecompressionContext* Longtail_CompressionAPI_HDecompressionContext;
typedef struct Longtail_CompressionAPI_Settings* Longtail_CompressionAPI_HSettings;

struct Longtail_CompressionAPI
{
    struct Longtail_API m_API;

    size_t (*GetMaxCompressedSize)(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, size_t size);
    int (*Compress)(struct Longtail_CompressionAPI* compression_api, Longtail_CompressionAPI_HSettings settings, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size);
    int (*Decompress)(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size);
};

struct Longtail_CompressionRegistryAPI
{
    struct Longtail_API m_API;
    int (*GetCompressionType)(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, Longtail_CompressionAPI_HSettings* out_settings);
};

extern struct Longtail_BlockStoreAPI* Longtail_CreateCompressBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry);

extern struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
        uint32_t compression_type_count,
        const uint32_t* compression_types,
        const struct Longtail_CompressionAPI** compression_apis,
        const Longtail_CompressionAPI_HSettings* compression_settings);

#ifdef __cplusplus
}
#endif
