#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count);

#ifdef __cplusplus
}
#endif
