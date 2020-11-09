#include "longtail_compression_registry.h"

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
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, api, return);
    struct Longtail_CompressionAPI* last_api = 0;
    struct Default_CompressionRegistry* default_compression_registry = (struct Default_CompressionRegistry*)api;
    for (uint32_t c = 0; c < default_compression_registry->m_Count; ++c)
    {
        struct Longtail_CompressionAPI* compression_api = default_compression_registry->m_APIs[c];
        if (compression_api != last_api)
        {
            compression_api->m_API.Dispose(&compression_api->m_API);
            last_api = compression_api;
        }
    }
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
    for (uint32_t i = 0; i < default_compression_registry->m_Count; ++i)
    {
        if (default_compression_registry->m_Types[i] == compression_type)
        {
            *out_compression_api = default_compression_registry->m_APIs[i];
            *out_settings = default_compression_registry->m_Settings[i];
            return 0;
        }
    }
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Unknown compression type %u", compression_type)
    return ENOENT;
}

struct Longtail_CompressionRegistryAPI* Longtail_CreateDefaultCompressionRegistry(
    uint32_t compression_type_count,
    const uint32_t* compression_types,
    const struct Longtail_CompressionAPI** compression_apis,
    const uint32_t* compression_settings)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(compression_type_count, "%u"),
        LONGTAIL_LOGFIELD(compression_types, "%p"),
        LONGTAIL_LOGFIELD(compression_apis, "%p"),
        LONGTAIL_LOGFIELD(compression_settings, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, compression_types, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, compression_apis, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, compression_settings, return 0);
    size_t registry_size = sizeof(struct Default_CompressionRegistry) +
        sizeof(uint32_t) * compression_type_count +
        sizeof(struct Longtail_CompressionAPI*) * compression_type_count +
        sizeof(uint32_t) * compression_type_count;
    void* mem = Longtail_Alloc(registry_size);
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

