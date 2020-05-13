
#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_HashRegistryAPI* Longtail_CreateDefaultHashRegistry(
        uint32_t hash_type_count,
        const uint32_t* hash_types,
        const struct Longtail_HashAPI** hash_apis);

#ifdef __cplusplus
}
#endif
