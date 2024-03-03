#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_ConcurrentChunkWriteAPI* Longtail_CreateConcurrentChunkWriteAPI(
	struct Longtail_StorageAPI* storageAPI,
	struct Longtail_VersionIndex* version_index,
	struct Longtail_VersionDiff* version_diff,
	const char* base_path);

#ifdef __cplusplus
}
#endif
