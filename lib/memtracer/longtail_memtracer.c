#include "longtail_memtracer.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"

#include <inttypes.h>
#include <stdlib.h>
#include <stdint.h>

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
    size_t current_mem;
    size_t peak_mem;
    const char* context_name;
};

static uint32_t MemTracer_ContextCount = 0;
#define MemTracer_MaxContextCount 128
static struct MemTracer_ContextStats MemTracer_contextStats[MemTracer_MaxContextCount];
struct Longtail_LookupTable* MemTracer_ContextLookup = 0;
HLongtail_SpinLock MemTracer_Spinlock = 0;

void Longtail_MemTracer_Init() {
    size_t lookupSize = Longtail_LookupTable_GetSize(MemTracer_MaxContextCount);
    void* mem = malloc(lookupSize);
    MemTracer_ContextLookup = Longtail_LookupTable_Create(mem, MemTracer_MaxContextCount, 0);
    Longtail_CreateSpinLock(malloc(Longtail_GetSpinLockSize()), &MemTracer_Spinlock);
}

void Longtail_MemTracer_Dispose() {
    for (uint32_t c = 0; c < MemTracer_ContextCount; ++c) {
        LONGTAIL_LOG(0, LONGTAIL_LOG_LEVEL_ERROR, "MemTracer: [%s] current %" PRIu64 " peak %" PRIu64, MemTracer_contextStats[c].context_name, MemTracer_contextStats[c].current_mem, MemTracer_contextStats[c].peak_mem)
    }
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
    uint64_t* context_index_ptr = Longtail_LookupTable_PutUnique(MemTracer_ContextLookup, context_id, MemTracer_ContextCount);
    if (context_index_ptr == 0) {
        contextStats = &MemTracer_contextStats[MemTracer_ContextCount];
        contextStats->current_mem = 0;
        contextStats->peak_mem = 0;
        contextStats->context_name = context;
        ++MemTracer_ContextCount;
    }
    else
    {
        contextStats = &MemTracer_contextStats[*context_index_ptr];
    }
    contextStats->current_mem += s;
    if (contextStats->current_mem > contextStats->peak_mem)
    {
        contextStats->peak_mem = contextStats->current_mem;
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
    header_ptr->id = (uint32_t)-1;
    header_ptr->size = (size_t)-1;
    Longtail_LockSpinLock(MemTracer_Spinlock);
    uint64_t* context_index_ptr = Longtail_LookupTable_Get(MemTracer_ContextLookup, context_id);
    struct MemTracer_ContextStats* contextStats = &MemTracer_contextStats[*context_index_ptr];
    contextStats->current_mem -= s;
    Longtail_UnlockSpinLock(MemTracer_Spinlock);
    free(header_ptr);
}
