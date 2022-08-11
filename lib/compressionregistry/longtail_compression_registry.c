#include "longtail_compression_registry.h"

#include "../longtail_platform.h"
#include "../../src/ext/stb_ds.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct Compression_API_LookupValue
{
    struct Longtail_CompressionAPI* api;
    uint32_t settings;
};

struct Compression_API_Lookup
{
    uint32_t key;
    struct Compression_API_LookupValue value;
};

struct Default_CompressionRegistry
{
    struct Longtail_CompressionRegistryAPI m_CompressionRegistryAPI;
    uint32_t m_Count;
    Longtail_CompressionRegistry_CreateForTypeFunc* m_CreateAPIFuncs;
    HLongtail_SpinLock m_SpinLock;
    struct Compression_API_Lookup* m_CompressionAPIs;
};

static void DefaultCompressionRegistry_Dispose(struct Longtail_API* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, api, return);
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)api;
    intptr_t api_count = hmlen(default_compression_registry->m_CompressionAPIs);
    for (intptr_t i = 0; i < api_count; ++i)
    {
        struct Longtail_API* dispose_api = &default_compression_registry->m_CompressionAPIs[i].value.api->m_API;
        dispose_api->Dispose(dispose_api);
    }
    hmfree(default_compression_registry->m_CompressionAPIs);
    Longtail_DeleteSpinLock(default_compression_registry->m_SpinLock);
    Longtail_Free(default_compression_registry);
}

static int Default_GetCompressionAPI(
    struct Longtail_CompressionRegistryAPI* compression_registry,
    uint32_t compression_type,
    struct Longtail_CompressionAPI** out_compression_api,
    uint32_t* out_settings)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_registry, "%p"),
        LONGTAIL_LOGFIELD(compression_type, "%u"),
        LONGTAIL_LOGFIELD(out_compression_api, "%p"),
        LONGTAIL_LOGFIELD(out_settings, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, compression_registry, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, out_compression_api, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, out_settings, return EINVAL);
    
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)compression_registry;

    Longtail_LockSpinLock(default_compression_registry->m_SpinLock);
    intptr_t it = hmgeti(default_compression_registry->m_CompressionAPIs, compression_type);
    if (it != -1)
    {
        struct Compression_API_LookupValue existing_api = default_compression_registry->m_CompressionAPIs[it].value;
        Longtail_UnlockSpinLock(default_compression_registry->m_SpinLock);
        *out_compression_api = existing_api.api;
        if (out_settings)
        {
            *out_settings = existing_api.settings;
        }
        return 0;
    }

    for (uint32_t i = 0; i < default_compression_registry->m_Count; ++i)
    {
        struct Compression_API_LookupValue new_api;
        new_api.api = default_compression_registry->m_CreateAPIFuncs[i](compression_type, &new_api.settings);
        if (new_api.api)
        {
            hmput(default_compression_registry->m_CompressionAPIs, compression_type, new_api);
            Longtail_UnlockSpinLock(default_compression_registry->m_SpinLock);
            *out_compression_api = new_api.api;
            if (out_settings)
            {
                *out_settings = new_api.settings;
            }
            return 0;
        }
    }
    Longtail_UnlockSpinLock(default_compression_registry->m_SpinLock);
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Unknown compression type %u", compression_type)
    return ENOENT;
}

struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
    uint32_t compression_type_count,
    const Longtail_CompressionRegistry_CreateForTypeFunc* create_api_funcs)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_type_count, "%u"),
        LONGTAIL_LOGFIELD(create_api_funcs, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, create_api_funcs, return 0);
    size_t registry_size = sizeof(struct Default_CompressionRegistry) +
        sizeof(uint32_t) * compression_type_count +
        sizeof(Longtail_CompressionRegistry_CreateForTypeFunc) * compression_type_count +
        sizeof(Longtail_GetSpinLockSize());
    void* mem = Longtail_Alloc("CompressionRegistry", registry_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    struct Default_CompressionRegistry* registry = (struct Default_CompressionRegistry*)Longtail_MakeCompressionRegistryAPI(
        mem,
        DefaultCompressionRegistry_Dispose,
        Default_GetCompressionAPI);

    registry->m_Count = compression_type_count;
    char* p = (char*)&registry[1];
    registry->m_CreateAPIFuncs = (Longtail_CompressionRegistry_CreateForTypeFunc*)(void*)p;
    p += sizeof(Longtail_CompressionRegistry_CreateForTypeFunc) * compression_type_count;
    int err = Longtail_CreateSpinLock(p, &registry->m_SpinLock);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateSpinLock() failed with %d", ENOMEM)
        Longtail_Free(mem);
        return 0;
    }
    registry->m_CompressionAPIs = 0;

    memmove(registry->m_CreateAPIFuncs, create_api_funcs, sizeof(Longtail_CompressionRegistry_CreateForTypeFunc) * compression_type_count);

    return &registry->m_CompressionRegistryAPI;
}

