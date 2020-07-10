#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateThreadedBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store,
    uint32_t thread_count,
    int thread_priority);

#ifdef __cplusplus
}
#endif
