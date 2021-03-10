#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_StorageAPI* Longtail_CreateBlockStoreStorageAPI(
    struct Longtail_HashAPI* hash_api,
    struct Longtail_JobAPI* job_api,
    struct Longtail_BlockStoreAPI* block_store,
    struct Longtail_StoreIndex* store_index,
    struct Longtail_VersionIndex* version_index);

#ifdef __cplusplus
}
#endif
