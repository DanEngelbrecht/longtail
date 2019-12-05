#pragma once

#include <stdint.h>

extern int GetCPUCount();
extern struct HashAPI* GetMeowHashAPI();
extern struct StorageAPI* GetFSStorageAPI();
extern struct StorageAPI* CreateInMemStorageAPI();
extern void DestroyInMemStorageAPI(struct StorageAPI* storage_api);
extern struct CompressionAPI* GetLizardCompressionAPI();
extern struct JobAPI* GetBikeshedJobAPI(uint32_t worker_count);
struct CompressionRegistry* GetCompressionRegistry();

#define NO_COMPRESSION_TYPE 0u
#define LIZARD_DEFAULT_COMPRESSION_TYPE (((uint32_t)'1') << 24) + (((uint32_t)'s') << 16) + (((uint32_t)'\0') << 8) + ((uint32_t)'d')
