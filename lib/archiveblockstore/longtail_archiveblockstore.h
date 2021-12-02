#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateArchiveBlockStore(
    struct Longtail_StorageAPI* storage_api,
    const char* archive_path,
    struct Longtail_ArchiveIndex* archive_index,
    int enable_write);

#ifdef __cplusplus
}
#endif
