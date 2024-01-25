#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern uint32_t Longtail_GetMemTracerSummary();
LONGTAIL_EXPORT extern uint32_t Longtail_GetMemTracerDetailed();

LONGTAIL_EXPORT void Longtail_MemTracer_Init();
LONGTAIL_EXPORT char* Longtail_MemTracer_GetStats(uint32_t log_level);
LONGTAIL_EXPORT void Longtail_MemTracer_Dispose();
LONGTAIL_EXPORT void* Longtail_MemTracer_ReAlloc(const char* context, void* old, size_t s);
LONGTAIL_EXPORT void Longtail_MemTracer_Free(void* p);

LONGTAIL_EXPORT int Longtail_MemTracer_DumpStats(const char* name);

LONGTAIL_EXPORT uint64_t Longtail_MemTracer_GetAllocationCount(const char* context);

#ifdef __cplusplus
}
#endif
