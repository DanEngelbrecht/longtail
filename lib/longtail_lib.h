#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern int GetCPUCount();

extern struct HashAPI* CreateMeowHashAPI();
extern void DestroyHashAPI(struct HashAPI* hash_api);

extern struct StorageAPI* CreateFSStorageAPI();
extern struct StorageAPI* CreateInMemStorageAPI();
extern void DestroyStorageAPI(struct StorageAPI* storage_api);

extern struct CompressionAPI* CreateLizardCompressionAPI();
extern void DestroyCompressionAPI(struct CompressionAPI* compression_api);

extern struct JobAPI* CreateBikeshedJobAPI(uint32_t worker_count);
extern void DestroyJobAPI(struct JobAPI* job_api);

extern struct CompressionRegistry* CreateDefaultCompressionRegistry();
extern void DestroyCompressionRegistry(struct CompressionRegistry* compression_registry);

#define NO_COMPRESSION_TYPE 0u
#define LIZARD_DEFAULT_COMPRESSION_TYPE (((uint32_t)'1') << 24) + (((uint32_t)'s') << 16) + (((uint32_t)'\0') << 8) + ((uint32_t)'d')

#ifdef __cplusplus
}
#endif
