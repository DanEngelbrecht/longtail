#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateLRUBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t max_lru_count);

#ifdef __cplusplus
}
#endif
