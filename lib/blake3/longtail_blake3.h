#pragma once

#include <stdint.h>
#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_HashAPI* Longtail_CreateBlake3HashAPI();
LONGTAIL_EXPORT extern const uint32_t Longtail_GetBlake3HashType();

#ifdef __cplusplus
}
#endif
