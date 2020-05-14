#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateRetainingBlockStoreAPI(
    struct Longtail_BlockStoreAPI* backing_block_store);

#ifdef __cplusplus
}
#endif
