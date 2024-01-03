#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_ConcurrentChunkWriteAPI* Longtail_CreateConcurrentChunkWriteAPI(struct Longtail_StorageAPI* storageAPI, const char* base_path);

#ifdef __cplusplus
}
#endif
