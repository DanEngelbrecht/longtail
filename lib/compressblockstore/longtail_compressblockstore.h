#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_CompressionRegistryAPI;

extern struct Longtail_BlockStoreAPI* Longtail_CreateCompressBlockStoreAPI(
	struct Longtail_BlockStoreAPI* backing_block_store,
	struct Longtail_CompressionRegistryAPI* compression_registry);

#ifdef __cplusplus
}
#endif
