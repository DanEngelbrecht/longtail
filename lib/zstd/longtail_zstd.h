#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetZStdMinQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetZStdDefaultQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetZStdMaxQuality();

#ifdef __cplusplus
}
#endif
