#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern uint32_t Longtail_GetMemTracerSilent();
LONGTAIL_EXPORT extern uint32_t Longtail_GetMemTracerSummary();
LONGTAIL_EXPORT extern uint32_t Longtail_GetMemTracerDetailed();

LONGTAIL_EXPORT void Longtail_MemTracer_Init();
LONGTAIL_EXPORT void Longtail_MemTracer_Dispose(uint32_t log_level);
LONGTAIL_EXPORT void* Longtail_MemTracer_Alloc(const char* context, size_t s);
LONGTAIL_EXPORT void Longtail_MemTracer_Free(void* p);

LONGTAIL_EXPORT int Longtail_MemTracer_DumpStats(const char* name);

#ifdef __cplusplus
}
#endif
