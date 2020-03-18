#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_MIN_QUALITY;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_GENERIC_MAX_QUALITY;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_MIN_QUALITY;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_BROTLI_TEXT_MAX_QUALITY;

#ifdef __cplusplus
}
#endif
