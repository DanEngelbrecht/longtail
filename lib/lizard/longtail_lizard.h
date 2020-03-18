#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MIN_COMPRESSION;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_DEFAULT_COMPRESSION;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MAX_COMPRESSION;

#ifdef __cplusplus
}
#endif
