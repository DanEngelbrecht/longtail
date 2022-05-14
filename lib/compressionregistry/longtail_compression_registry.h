#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Longtail_CompressionAPI* (*Longtail_CompressionRegistry_CreateForTypeFunc)(uint32_t compression_type, uint32_t* out_settings);

LONGTAIL_EXPORT extern struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
        uint32_t compression_api_count,
        const Longtail_CompressionRegistry_CreateForTypeFunc* create_api_funcs);

#ifdef __cplusplus
}
#endif
