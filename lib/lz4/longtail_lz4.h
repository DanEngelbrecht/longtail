#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateLZ4CompressionAPI();
extern Longtail_CompressionAPI_HSettings LONGTAIL_LZ4_DEFAULT_COMPRESSION;

#ifdef __cplusplus
}
#endif
