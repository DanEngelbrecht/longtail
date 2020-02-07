#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
	struct Longtail_StorageAPI* storage_api,
	struct Longtail_JobAPI* job_api,
	const char* content_path);

#ifdef __cplusplus
}
#endif
