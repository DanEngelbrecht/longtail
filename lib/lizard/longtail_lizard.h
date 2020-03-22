#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetLizardMinQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetLizardDefaultQuality();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings Longtail_GetLizardMaxQuality();

#ifdef __cplusplus
}
#endif
