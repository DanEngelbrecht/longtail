#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI();
extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MIN_COMPRESSION;
extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_DEFAULT_COMPRESSION;
extern Longtail_CompressionAPI_HSettings LONGTAIL_LIZARD_MAX_COMPRESSION;

#ifdef __cplusplus
}
#endif
