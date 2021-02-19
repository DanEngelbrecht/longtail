#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT void Longtail_MemTracer_Init();
LONGTAIL_EXPORT void Longtail_MemTracer_Dispose();
LONGTAIL_EXPORT void* Longtail_MemTracer_Alloc(const char* context, size_t s);
LONGTAIL_EXPORT void Longtail_MemTracer_Free(void* p);

#ifdef __cplusplus
}
#endif
