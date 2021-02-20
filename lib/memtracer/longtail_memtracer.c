#include "longtail_memtracer.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <inttypes.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const uint32_t Prime = 0x01000193;
static const uint32_t Seed  = 0x811C9DC5;

static uint32_t MemTracer_ContextIdHash(const char* context)
{
    uint32_t hash = Seed;
    while (*context)
    {
        hash = ((*context++) ^ hash) * Prime;
    }
    return hash;
}

struct MemTracer_Header {
    size_t size;
    uint32_t id;
};

struct MemTracer_ContextStats {
    size_t total_mem;
    size_t current_mem;
    size_t peak_mem;
    size_t total_count;
    size_t current_count;
    size_t peak_count;
    size_t global_peak_count;
    size_t global_peak_mem;
    const char* context_name;
};

#define MEMTRACER_MAXCONTEXTCOUNT 128

struct MemTracer_Context {
    struct Longtail_LookupTable* m_ContextLookup;
    struct MemTracer_ContextStats m_ContextStats[MEMTRACER_MAXCONTEXTCOUNT];
    HLongtail_SpinLock m_Spinlock;
    size_t m_AllocationTotalCount;
    size_t m_AllocationCurrentCount;
    size_t m_AllocationPeakCount;
    size_t m_AllocationTotalMem;
    size_t m_AllocationCurrentMem;
    size_t m_AllocationPeakMem;
    uint32_t m_ContextCount;
};

static struct MemTracer_Context* gMemTracer_Context = 0;


void Longtail_MemTracer_Init() {
    size_t lookupSize = Longtail_LookupTable_GetSize(MEMTRACER_MAXCONTEXTCOUNT);
    size_t context_size = sizeof(struct MemTracer_Context) + lookupSize + Longtail_GetSpinLockSize();
    void* mem = malloc(context_size);
    if (mem == 0)
    {
        return;
    }
    memset(mem, 0, context_size);
    gMemTracer_Context = (struct MemTracer_Context*)mem;
    gMemTracer_Context->m_ContextLookup = Longtail_LookupTable_Create(&gMemTracer_Context[1], MEMTRACER_MAXCONTEXTCOUNT, 0);
    Longtail_CreateSpinLock(&((char*)gMemTracer_Context->m_ContextLookup)[lookupSize], &gMemTracer_Context->m_Spinlock);
}

static const char* Denoms[] = {
    "b",
    "kb",
    "mb",
    "gb",
    "tb"
};

static void MemTracer_PrintSize(size_t size) {
    if (size < 1024 * 100) {
        printf("%" PRIu64, size);
        return;
    }
    int denom = 0;
    size_t factor = 1;
    while ((size / factor) > 1024) {
        factor *= 1024;
        denom++;
    }
    printf("%.2f %s (%" PRIu64 ")", (float)size / (float)factor, Denoms[denom], size);
}

void Longtail_MemTracer_Dispose() {
    for (uint32_t c = 0; c < gMemTracer_Context->m_ContextCount; ++c) {
        struct MemTracer_ContextStats* stats = &gMemTracer_Context->m_ContextStats[c];
        printf("gMemTracer_Context:  %s\n", stats->context_name);
        printf("  total_mem:         "); MemTracer_PrintSize(stats->total_mem); printf("\n");
        printf("  current_mem:       "); MemTracer_PrintSize(stats->current_mem); printf("\n");
        printf("  peak_mem:          "); MemTracer_PrintSize(stats->peak_mem); printf("\n");
        printf("  total_count:       "); MemTracer_PrintSize(stats->total_count); printf("\n");
        printf("  current_count:     "); MemTracer_PrintSize(stats->current_count); printf("\n");
        printf("  peak_count:        "); MemTracer_PrintSize(stats->peak_count); printf("\n");
        printf("  global_peak_count: "); MemTracer_PrintSize(stats->global_peak_count); printf("\n");
        printf("  global_peak_mem:   "); MemTracer_PrintSize(stats->global_peak_mem); printf("\n");
    }
    printf("total_mem:     "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationTotalMem); printf("\n");
    printf("current_mem:   "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationCurrentMem); printf("\n");
    printf("peak_mem:      "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationPeakMem); printf("\n");
    printf("total_count:   "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationTotalCount); printf("\n");
    printf("current_count: "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationCurrentCount); printf("\n");
    printf("peak_count:    "); MemTracer_PrintSize(gMemTracer_Context->m_AllocationPeakCount); printf("\n");
    Longtail_DeleteSpinLock(gMemTracer_Context->m_Spinlock);
    free(gMemTracer_Context);
    gMemTracer_Context = 0;
}

void* Longtail_MemTracer_Alloc(const char* context, size_t s)
{
#if defined(LONGTAIL_ASSERTS)
    const char* context_safe = context ? context : "";
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context_safe, "%s"),
        LONGTAIL_LOGFIELD(s, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    uint32_t context_id = context ? MemTracer_ContextIdHash(context) : 0;

    struct MemTracer_ContextStats* contextStats = 0;
    Longtail_LockSpinLock(gMemTracer_Context->m_Spinlock);
    uint32_t* context_index_ptr = Longtail_LookupTable_PutUnique(gMemTracer_Context->m_ContextLookup, context_id, gMemTracer_Context->m_ContextCount);
    if (context_index_ptr == 0) {
        LONGTAIL_FATAL_ASSERT(ctx, gMemTracer_Context->m_ContextCount < MEMTRACER_MAXCONTEXTCOUNT, return 0)
        contextStats = &gMemTracer_Context->m_ContextStats[gMemTracer_Context->m_ContextCount];
        memset(contextStats, 0, sizeof(struct MemTracer_ContextStats));
        contextStats->context_name = context;
        gMemTracer_Context->m_ContextCount++;
    }
    else
    {
        contextStats = &gMemTracer_Context->m_ContextStats[*context_index_ptr];
    }

    contextStats->total_count++;
    contextStats->current_count++;
    contextStats->total_mem += s;
    contextStats->current_mem += s;
    if (contextStats->current_mem > contextStats->peak_mem)
    {
        contextStats->peak_mem = contextStats->current_mem;
    }
    if (contextStats->current_count > contextStats->peak_count)
    {
        contextStats->peak_count = contextStats->current_count;
    }
    
    gMemTracer_Context->m_AllocationTotalCount++;
    gMemTracer_Context->m_AllocationCurrentCount++;
    gMemTracer_Context->m_AllocationTotalMem += s;
    gMemTracer_Context->m_AllocationCurrentMem += s;
    if (gMemTracer_Context->m_AllocationCurrentMem > gMemTracer_Context->m_AllocationPeakMem)
    {
        gMemTracer_Context->m_AllocationPeakMem = gMemTracer_Context->m_AllocationCurrentMem;
        for (size_t i = 0; i < gMemTracer_Context->m_ContextCount; i++)
        {
            contextStats = &gMemTracer_Context->m_ContextStats[i];
            contextStats->global_peak_mem = contextStats->current_mem;
        }
    }
    if (gMemTracer_Context->m_AllocationCurrentCount > gMemTracer_Context->m_AllocationPeakCount)
    {
        gMemTracer_Context->m_AllocationPeakCount = gMemTracer_Context->m_AllocationCurrentCount;
        for (size_t i = 0; i < gMemTracer_Context->m_ContextCount; i++)
        {
            contextStats = &gMemTracer_Context->m_ContextStats[i];
            contextStats->global_peak_count = contextStats->current_count;
        }
    }

    Longtail_UnlockSpinLock(gMemTracer_Context->m_Spinlock);

    size_t padded_size = sizeof(struct MemTracer_Header) + s;
    void* mem = malloc(padded_size);
    struct MemTracer_Header* header_ptr = (struct MemTracer_Header*)mem;
    header_ptr->id = context_id;
    header_ptr->size = s;
    return &header_ptr[1];
}

void Longtail_MemTracer_Free(void* p)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(p, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    if (p == 0)
    {
        return;
    }
    struct MemTracer_Header* header_ptr = (struct MemTracer_Header*)p;
    --header_ptr;
    uint32_t context_id = header_ptr->id;
    size_t s = header_ptr->size;
    LONGTAIL_VALIDATE_INPUT(ctx, s != (uint32_t)-1, return)
    memset(header_ptr, 255, sizeof(struct MemTracer_Header));
    Longtail_LockSpinLock(gMemTracer_Context->m_Spinlock);
    uint32_t* context_index_ptr = Longtail_LookupTable_Get(gMemTracer_Context->m_ContextLookup, context_id);
    struct MemTracer_ContextStats* contextStats = &gMemTracer_Context->m_ContextStats[*context_index_ptr];
    gMemTracer_Context->m_AllocationCurrentMem -= s;
    gMemTracer_Context->m_AllocationCurrentCount--;
    contextStats->current_mem -= s;
    contextStats->current_count--;
    Longtail_UnlockSpinLock(gMemTracer_Context->m_Spinlock);
    free(header_ptr);
}
