#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

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
