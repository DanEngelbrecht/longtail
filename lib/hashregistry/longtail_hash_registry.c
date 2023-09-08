
#include "longtail_hash_registry.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct Default_HashRegistry
{
    struct Longtail_HashRegistryAPI m_HashRegistryAPI;
    uint32_t m_Count;
    uint32_t* m_Types;
    struct Longtail_HashAPI** m_APIs;
};

static void DefaultHashRegistry_Dispose(struct Longtail_API* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, api, return);
    struct Longtail_HashAPI* last_api = 0;
    struct Default_HashRegistry* default_hash_registry = (struct Default_HashRegistry*)api;
    for (uint32_t c = 0; c < default_hash_registry->m_Count; ++c)
    {
        struct Longtail_HashAPI* hash_api = default_hash_registry->m_APIs[c];
        if (hash_api == 0)
        {
            continue;
        }
        if (hash_api != last_api)
        {
            hash_api->m_API.Dispose(&hash_api->m_API);
            last_api = hash_api;
        }
    }
    Longtail_Free(default_hash_registry);
}

static int Default_GetHashAPI(struct Longtail_HashRegistryAPI* hash_registry, uint32_t hash_type, struct Longtail_HashAPI** out_hash_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_registry, "%p"),
        LONGTAIL_LOGFIELD(hash_type, "%u"),
        LONGTAIL_LOGFIELD(out_hash_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, hash_registry, return EINVAL);
    LONGTAIL_FATAL_ASSERT(ctx, out_hash_api, return EINVAL);
    
    struct Default_HashRegistry* default_hash_registry = (struct Default_HashRegistry*)hash_registry;
    for (uint32_t i = 0; i < default_hash_registry->m_Count; ++i)
    {
        if (default_hash_registry->m_Types[i] == hash_type)
        {
            if (default_hash_registry->m_APIs[i] == 0)
            {
                return ENOTSUP;
            }
            *out_hash_api = default_hash_registry->m_APIs[i];
            return 0;
        }
    }
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Failed to find api identifier %u", hash_type)
    return ENOENT;
}

struct Longtail_HashRegistryAPI* Longtail_CreateDefaultHashRegistry(
    uint32_t hash_type_count,
    const uint32_t* hash_types,
    const struct Longtail_HashAPI** hash_apis)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(hash_type_count, "%u"),
        LONGTAIL_LOGFIELD(hash_types, "%p"),
        LONGTAIL_LOGFIELD(hash_apis, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, hash_types, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, hash_apis, return 0);
    size_t registry_size = sizeof(struct Default_HashRegistry) +
        sizeof(uint32_t) * hash_type_count +
        sizeof(struct Longtail_HashAPI*) * hash_type_count;
    void* mem = Longtail_Alloc("DefaultHashRegistry", registry_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }

    struct Default_HashRegistry* registry = (struct Default_HashRegistry*)Longtail_MakeHashRegistryAPI(
        mem,
        DefaultHashRegistry_Dispose,
        Default_GetHashAPI);

    registry->m_Count = hash_type_count;
    char* p = (char*)&registry[1];
    registry->m_Types = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * hash_type_count;

    registry->m_APIs = (struct Longtail_HashAPI**)(void*)p;
    p += sizeof(struct Longtail_HashAPI*) * hash_type_count;

    memmove(registry->m_Types, hash_types, sizeof(uint32_t) * hash_type_count);
    memmove(registry->m_APIs, hash_apis, sizeof(struct Longtail_HashAPI*) * hash_type_count);

    return &registry->m_HashRegistryAPI;
}
