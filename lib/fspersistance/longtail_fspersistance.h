#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_PersistenceAPI* Longtail_CreateFSPersistanceAPI(struct Longtail_StorageAPI* storage_api);

#ifdef __cplusplus
}
#endif
