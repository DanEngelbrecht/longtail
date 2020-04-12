#pragma once

#include <stdint.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateRetainingBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint64_t max_block_retain_count);

#ifdef __cplusplus
}
#endif
