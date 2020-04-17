
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
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "DefaultHashRegistry_Dispose(%p)", api)
    LONGTAIL_VALIDATE_INPUT(api, return);
    struct Longtail_HashAPI* last_api = 0;
    struct Default_HashRegistry* default_hash_registry = (struct Default_HashRegistry*)api;
    for (uint32_t c = 0; c < default_hash_registry->m_Count; ++c)
    {
        struct Longtail_HashAPI* api = default_hash_registry->m_APIs[c];
        if (api != last_api)
        {
            api->m_API.Dispose(&api->m_API);
            last_api = api;
        }
    }
    Longtail_Free(default_hash_registry);
}

static int Default_GetHashAPI(struct Longtail_HashRegistryAPI* hash_registry, uint32_t hash_type, struct Longtail_HashAPI** out_hash_api)
{
    LONGTAIL_FATAL_ASSERT(hash_registry, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_hash_api, return EINVAL);
    
    struct Default_HashRegistry* default_hash_registry = (struct Default_HashRegistry*)hash_registry;
    for (uint32_t i = 0; i < default_hash_registry->m_Count; ++i)
    {
        if (default_hash_registry->m_Types[i] == hash_type)
        {
            *out_hash_api = default_hash_registry->m_APIs[i];
            return 0;
        }
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Default_GetHashAPI(%p, %u, %p) failed with %d", hash_registry, hash_type, out_hash_api, ENOENT)
    return ENOENT;
}

struct Longtail_HashRegistryAPI* Longtail_CreateDefaultHashRegistry(
    uint32_t hash_type_count,
    const uint32_t* hash_types,
    const struct Longtail_HashAPI** hash_apis)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateDefaultHashRegistry(%u, %p, %p)", hash_type_count, hash_types, hash_apis)
    LONGTAIL_VALIDATE_INPUT(hash_types, return 0);
    LONGTAIL_VALIDATE_INPUT(hash_apis, return 0);
    size_t registry_size = sizeof(struct Default_HashRegistry) +
        sizeof(uint32_t) * hash_type_count +
        sizeof(struct Longtail_HashAPI*) * hash_type_count;
    void* mem = Longtail_Alloc(registry_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateDefaultHashRegistry(%u, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            hash_type_count, hash_types, hash_apis,
            registry_size,
            ENOMEM)
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
