#include "longtail_compression_registry.h"

#include "../../src/longtail.h"
#include <errno.h>
#include <inttypes.h>
#include <string.h>

struct Default_CompressionRegistry
{
    struct Longtail_CompressionRegistryAPI m_CompressionRegistryAPI;
    uint32_t m_Count;
    uint32_t* m_Types;
    struct Longtail_CompressionAPI** m_APIs;
    uint32_t* m_Settings;
};

static void DefaultCompressionRegistry_Dispose(struct Longtail_API* api)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_DEBUG, "DefaultCompressionRegistry_Dispose(%p)", api)
    LONGTAIL_VALIDATE_INPUT(api, return);
    struct Longtail_CompressionAPI* last_api = 0;
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)api;
    for (uint32_t c = 0; c < default_compression_registry->m_Count; ++c)
    {
        struct Longtail_CompressionAPI* api = default_compression_registry->m_APIs[c];
        if (api != last_api)
        {
            api->m_API.Dispose(&api->m_API);
            last_api = api;
        }
    }
    Longtail_Free(default_compression_registry);
}

static int Default_GetCompressionAPI(struct Longtail_CompressionRegistryAPI* compression_registry, uint32_t compression_type, struct Longtail_CompressionAPI** out_compression_api, uint32_t* out_settings)
{
    LONGTAIL_FATAL_ASSERT(compression_registry, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_compression_api, return EINVAL);
    LONGTAIL_FATAL_ASSERT(out_settings, return EINVAL);
    
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)compression_registry;
    for (uint32_t i = 0; i < default_compression_registry->m_Count; ++i)
    {
        if (default_compression_registry->m_Types[i] == compression_type)
        {
            *out_compression_api = default_compression_registry->m_APIs[i];
            *out_settings = default_compression_registry->m_Settings[i];
            return 0;
        }
    }
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_WARNING, "Default_GetCompressionAPI(%p, %u, %p, %p) failed with %d", compression_registry, compression_type, out_compression_api, out_settings, ENOENT)
    return ENOENT;
}

struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
    uint32_t compression_type_count,
    const uint32_t* compression_types,
    const struct Longtail_CompressionAPI** compression_apis,
    const uint32_t* compression_settings)
{
    LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_INFO, "Longtail_CreateDefaultCompressionRegistry(%u, %p, %p, %p)", compression_type_count, compression_types, compression_apis, compression_settings)
    LONGTAIL_VALIDATE_INPUT(compression_types, return 0);
    LONGTAIL_VALIDATE_INPUT(compression_apis, return 0);
    LONGTAIL_VALIDATE_INPUT(compression_settings, return 0);
    size_t registry_size = sizeof(struct Default_CompressionRegistry) +
        sizeof(uint32_t) * compression_type_count +
        sizeof(struct Longtail_CompressionAPI*) * compression_type_count +
        sizeof(uint32_t) * compression_type_count;
    void* mem = Longtail_Alloc(registry_size);
    if (!mem)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateDefaultCompressionRegistry(%u, %p, %p, %p) Longtail_Alloc(%" PRIu64 ") failed with, %d",
            compression_type_count, compression_types, compression_apis, compression_settings,
            registry_size,
            ENOMEM)
        return 0;
    }

    struct Default_CompressionRegistry* registry = (struct Default_CompressionRegistry*)Longtail_MakeCompressionRegistryAPI(
        mem,
        DefaultCompressionRegistry_Dispose,
        Default_GetCompressionAPI);

    registry->m_Count = compression_type_count;
    char* p = (char*)&registry[1];
    registry->m_Types = (uint32_t*)(void*)p;
    p += sizeof(uint32_t) * compression_type_count;

    registry->m_APIs = (struct Longtail_CompressionAPI**)(void*)p;
    p += sizeof(struct Longtail_CompressionAPI*) * compression_type_count;

    registry->m_Settings = (uint32_t*)(void*)p;

    memmove(registry->m_Types, compression_types, sizeof(uint32_t) * compression_type_count);
    memmove(registry->m_APIs, compression_apis, sizeof(struct Longtail_CompressionAPI*) * compression_type_count);
    memmove(registry->m_Settings, compression_settings, sizeof(const uint32_t) * compression_type_count);

    return &registry->m_CompressionRegistryAPI;
}

