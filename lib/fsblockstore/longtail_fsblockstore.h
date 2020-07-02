#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_StorageAPI* storage_api,
    const char* content_path,
    uint32_t default_max_block_size,
    uint32_t default_max_chunks_per_block,
    const char* optional_extension);

#ifdef __cplusplus
}
#endif
