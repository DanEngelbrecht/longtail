#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_HashAPI* Longtail_CreateMeowHashAPI();
extern void Longtail_DestroyHashAPI(struct Longtail_HashAPI* hash_api);

extern struct Longtail_StorageAPI* Longtail_CreateFSStorageAPI();
extern struct Longtail_StorageAPI* Longtail_CreateInMemStorageAPI();
extern void Longtail_DestroyStorageAPI(struct Longtail_StorageAPI* storage_api);

extern struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI();
extern void Longtail_DestroyCompressionAPI(struct Longtail_CompressionAPI* compression_api);

extern struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count);
extern void Longtail_DestroyJobAPI(struct Longtail_JobAPI* job_api);

extern struct Longtail_CompressionRegistry* Longtail_CreateDefaultCompressionRegistry();
extern void Longtail_DestroyCompressionRegistry(struct Longtail_CompressionRegistry* compression_registry);

extern const uint32_t LONGTAIL_NO_COMPRESSION_TYPE;
extern const uint32_t LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
