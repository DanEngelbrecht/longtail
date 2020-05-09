#pragma once

#include <stdint.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
    struct Longtail_BlockStoreAPI* local_block_store,
    struct Longtail_BlockStoreAPI* remote_block_store);

#ifdef __cplusplus
}
#endif
