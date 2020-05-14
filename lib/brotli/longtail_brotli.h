#pragma once

#include "../../src/longtail.h"
#include "../compressblockstore/longtail_compressblockstore.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliGenericMinQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliGenericDefaultQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliGenericMaxQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliTextMinQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliTextDefaultQuality();
LONGTAIL_EXPORT extern uint32_t Longtail_GetBrotliTextMaxQuality();

#ifdef __cplusplus
}
#endif
