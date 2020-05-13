#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_HashAPI* Longtail_CreateBlake2HashAPI();
LONGTAIL_EXPORT extern const uint32_t Longtail_GetBlake2HashType();

#ifdef __cplusplus
}
#endif
