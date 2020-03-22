#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliGenericMinQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliGenericDefaultQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliGenericMaxQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliTextMinQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliTextDefaultQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetBrotliTextMaxQuality();

#ifdef __cplusplus
}
#endif
