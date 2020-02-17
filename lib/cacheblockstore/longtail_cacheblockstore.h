#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_BlockStoreAPI* Longtail_CreateCacheBlockStoreAPI(
	struct Longtail_BlockStoreAPI* local_block_store,
	struct Longtail_BlockStoreAPI* remote_block_store);

#ifdef __cplusplus
}
#endif
