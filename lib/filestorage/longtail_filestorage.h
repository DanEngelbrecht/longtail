#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_StorageAPI* Longtail_CreateFSStorageAPI();

#ifdef __cplusplus
}
#endif
