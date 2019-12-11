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

extern const uint32_t NO_COMPRESSION_TYPE;
extern const uint32_t LIZARD_DEFAULT_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
