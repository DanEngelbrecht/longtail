#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateLZ4CompressionAPI();
LONGTAIL_EXPORT extern Longtail_CompressionAPI_HSettings LONGTAIL_LZ4_DEFAULT_COMPRESSION;

#ifdef __cplusplus
}
#endif
