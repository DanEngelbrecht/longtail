#pragma once

#include "../../src/longtail.h"
#include "../compressblockstore/longtail_compressblockstore.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI();
LONGTAIL_EXPORT extern uint32_t Longtail_GetZStdMinQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetZStdDefaultQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetZStdMaxQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetZStdHighQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetZStdLowQuality();
LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CompressionRegistry_CreateForZstd(uint32_t compression_type, uint32_t* out_settings);

#ifdef __cplusplus
}
#endif
