#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count, int worker_priority);

#ifdef __cplusplus
}
#endif
