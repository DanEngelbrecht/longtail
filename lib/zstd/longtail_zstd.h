#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MIN_COMPRESSION;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_DEFAULT_COMPRESSION;
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MAX_COMPRESSION;

#ifdef __cplusplus
}
#endif
