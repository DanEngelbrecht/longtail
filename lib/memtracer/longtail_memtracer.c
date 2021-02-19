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
    const char* context_name;
};

static uint32_t MemTracer_ContextCount = 0;
#define MemTracer_MaxContextCount 128
static struct MemTracer_ContextStats MemTracer_contextStats[MemTracer_MaxContextCount];
struct Longtail_LookupTable* MemTracer_ContextLookup = 0;
HLongtail_SpinLock MemTracer_Spinlock = 0;
size_t MemTracer_AllocationTotalCount = 0;
size_t MemTracer_AllocationCurrentCount = 0;
size_t MemTracer_PeakAllocationCount = 0;
size_t MemTracer_AllocationTotalMem = 0;
size_t MemTracer_AllocationCurrentMem = 0;
size_t MemTracer_PeakAllocationMem = 0;

void Longtail_MemTracer_Init() {
    size_t lookupSize = Longtail_LookupTable_GetSize(MemTracer_MaxContextCount);
    void* mem = malloc(lookupSize);
    MemTracer_ContextLookup = Longtail_LookupTable_Create(mem, MemTracer_MaxContextCount, 0);
    Longtail_CreateSpinLock(malloc(Longtail_GetSpinLockSize()), &MemTracer_Spinlock);
}

static const char* Denoms[] = {
    "b",
    "kb",
    "mb",
    "gb",
    "tb"
};

static void PrintSize(size_t size) {
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
    for (uint32_t c = 0; c < MemTracer_ContextCount; ++c) {
        struct MemTracer_ContextStats* stats = &MemTracer_contextStats[c];
        printf("MemTracer: %s\n", stats->context_name);
        printf("  total_mem:     "); PrintSize(stats->total_mem); printf("\n");
        printf("  current_mem:   "); PrintSize(stats->current_mem); printf("\n");
        printf("  peak_mem:      "); PrintSize(stats->peak_mem); printf("\n");
        printf("  total_count:   "); PrintSize(stats->total_count); printf("\n");
        printf("  current_count: "); PrintSize(stats->current_count); printf("\n");
        printf("  peak_count:    "); PrintSize(stats->peak_count); printf("\n");
    }
    printf("total_mem:     "); PrintSize(MemTracer_AllocationTotalMem); printf("\n");
    printf("current_mem:   "); PrintSize(MemTracer_AllocationCurrentMem); printf("\n");
    printf("peak_mem:      "); PrintSize(MemTracer_PeakAllocationMem); printf("\n");
    printf("total_count:   "); PrintSize(MemTracer_AllocationTotalCount); printf("\n");
    printf("current_count: "); PrintSize(MemTracer_AllocationCurrentCount); printf("\n");
    printf("peak_count:    "); PrintSize(MemTracer_PeakAllocationCount); printf("\n");
    Longtail_DeleteSpinLock(MemTracer_Spinlock);
    free(MemTracer_Spinlock);
    free(MemTracer_ContextLookup);
}

void* Longtail_MemTracer_Alloc(const char* context, size_t s)
{
#if defined(LONGTAIL_ASSERTS)
    const char* context_safe = context ? context : "";
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(s, "%context_safe"),
        LONGTAIL_LOGFIELD(s, "%" PRIu64)
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    uint32_t context_id = context ? MemTracer_ContextIdHash(context) : 0;
    // Oh, we need a lock here!
    struct MemTracer_ContextStats* contextStats = 0;
    Longtail_LockSpinLock(MemTracer_Spinlock);
    uint32_t* context_index_ptr = Longtail_LookupTable_PutUnique(MemTracer_ContextLookup, context_id, MemTracer_ContextCount);
    if (context_index_ptr == 0) {
        contextStats = &MemTracer_contextStats[MemTracer_ContextCount];
        memset(contextStats, 0, sizeof(struct MemTracer_ContextStats));
        contextStats->context_name = context;
        MemTracer_ContextCount++;
    }
    else
    {
        contextStats = &MemTracer_contextStats[*context_index_ptr];
    }

    MemTracer_AllocationTotalCount++;
    MemTracer_AllocationCurrentCount++;
    MemTracer_AllocationTotalMem += s;
    MemTracer_AllocationCurrentMem += s;
    if (MemTracer_AllocationCurrentMem > MemTracer_PeakAllocationMem)
    {
        MemTracer_PeakAllocationMem = MemTracer_AllocationCurrentMem;
    }
    if (MemTracer_AllocationCurrentCount > MemTracer_PeakAllocationCount)
    {
        MemTracer_PeakAllocationCount = MemTracer_AllocationCurrentCount;
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
    
    Longtail_UnlockSpinLock(MemTracer_Spinlock);

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
    Longtail_LockSpinLock(MemTracer_Spinlock);
    uint32_t* context_index_ptr = Longtail_LookupTable_Get(MemTracer_ContextLookup, context_id);
    struct MemTracer_ContextStats* contextStats = &MemTracer_contextStats[*context_index_ptr];
    MemTracer_AllocationCurrentMem -= s;
    MemTracer_AllocationCurrentCount--;
    contextStats->current_mem -= s;
    contextStats->current_count--;
    Longtail_UnlockSpinLock(MemTracer_Spinlock);
    free(header_ptr);
}
