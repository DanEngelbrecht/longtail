#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
        uint32_t compression_type_count,
        const uint32_t* compression_types,
        const struct Longtail_CompressionAPI** compression_apis,
        const uint32_t* compression_setting_ids);

#ifdef __cplusplus
}
#endif
