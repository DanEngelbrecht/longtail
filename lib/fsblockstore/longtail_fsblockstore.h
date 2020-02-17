#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_StorageAPI;

extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
	struct Longtail_StorageAPI* storage_api,
	const char* content_path);

#ifdef __cplusplus
}
#endif
