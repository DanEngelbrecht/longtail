#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI();
extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MIN_COMPRESSION;
extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_DEFAULT_COMPRESSION;
extern Longtail_CompressionAPI_HSettings LONGTAIL_ZSTD_MAX_COMPRESSION;

#ifdef __cplusplus
}
#endif
