#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_HashAPI* Longtail_CreateBlake2HashAPI();
extern const uint32_t LONGTAIL_BLAKE2_HASH_TYPE;

#ifdef __cplusplus
}
#endif
