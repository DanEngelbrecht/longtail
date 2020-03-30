#pragma once

#include <stdint.h>

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count);

#ifdef __cplusplus
}
#endif
