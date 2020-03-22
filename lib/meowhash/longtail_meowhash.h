#pragma once

#include <stdint.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_HashAPI* Longtail_CreateMeowHashAPI();
LONGTAIL_EXPORT extern const uint32_t Longtail_GetMeowHashType();

#ifdef __cplusplus
}
#endif
