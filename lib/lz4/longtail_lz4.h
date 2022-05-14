#pragma once

#include "../../src/longtail.h"
#include "../compressblockstore/longtail_compressblockstore.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateLZ4CompressionAPI();
LONGTAIL_EXPORT extern uint32_t Longtail_GetLZ4DefaultQuality();
LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CompressionRegistry_CreateForLZ4(uint32_t compression_type, uint32_t* out_settings);

#ifdef __cplusplus
}
#endif
