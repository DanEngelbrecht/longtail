#pragma once

#include <stdint.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_StorageAPI;

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_StorageAPI* storage_api,
    const char* content_path,
    uint32_t default_max_block_size,
    uint32_t default_max_chunks_per_block);

#ifdef __cplusplus
}
#endif
