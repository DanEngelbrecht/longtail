#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_ProgressAPI* Longtail_CreateRateLimitedProgress(
    struct Longtail_ProgressAPI* progress_api,
	uint32_t percent_rate_limit);

#ifdef __cplusplus
}
#endif
