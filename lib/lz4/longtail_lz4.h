#pragma once

#include "../compressblockstore/longtail_compressblockstore.h"

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_CompressionAPI* Longtail_CreateLZ4CompressionAPI();
LONGTAIL_EXPORT extern uint32_t Longtail_GetLZ4DefaultQuality();

#ifdef __cplusplus
}
#endif
