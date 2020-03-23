#pragma once

#include <stdint.h>
#include "../../src/longtail.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Longtail_CompressionAPI_CompressionContext* Longtail_CompressionAPI_HCompressionContext;
typedef struct Longtail_CompressionAPI_DecompressionContext* Longtail_CompressionAPI_HDecompressionContext;

struct Longtail_CompressionAPI
{
    struct Longtail_API m_API;

    size_t (*GetMaxCompressedSize)(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, size_t size);
    int (*Compress)(struct Longtail_CompressionAPI* compression_api, uint32_t settings_id, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size, size_t* out_compressed_size);
    int (*Decompress)(struct Longtail_CompressionAPI* compression_api, const char* compressed, char* uncompressed, size_t compressed_size, size_t max_uncompressed_size, size_t* out_uncompressed_size);
};

struct Longtail_CompressionRegistryAPI
{
    struct Longtail_API m_API;
    int (*GetCompressionType)(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, uint32_t* out_settings_id);
};

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateCompressBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry);

LONGTAIL_EXPORT extern struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
        uint32_t compression_type_count,
        const uint32_t* compression_types,
        const struct Longtail_CompressionAPI** compression_apis,
        const uint32_t* compression_setting_ids);

#ifdef __cplusplus
}
#endif
