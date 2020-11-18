#include "longtail_atomiccancel.h"

#include "../longtail_platform.h"
#include <errno.h>

struct AtomicCancelAPI
{
    struct Longtail_CancelAPI m_CancelAPI;
};

static int AtomicCancelAPI_CreateToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken* out_token)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cancel_api, "%p"),
        LONGTAIL_LOGFIELD(out_token, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(ctx, out_token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)Longtail_Alloc(sizeof(TLongtail_Atomic32));
    if (!atomic_counter)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d",ENOMEM)
        return ENOMEM;
    }
    *atomic_counter = 0;
    *out_token = (Longtail_CancelAPI_HCancelToken)atomic_counter;
    return 0;
}

static int AtomicCancelAPI_Cancel(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cancel_api, "%p"),
        LONGTAIL_LOGFIELD(token, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(ctx, token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)token;
    if (*atomic_counter == 0)
    {
        Longtail_AtomicAdd32(atomic_counter, 1);
    }
    return 0;
}
static int AtomicCancelAPI_DisposeToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cancel_api, "%p"),
        LONGTAIL_LOGFIELD(token, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(ctx, token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)token;
    Longtail_Free((void*)atomic_counter);
    return 0;
}

static int AtomicCancelAPI_IsCancelled(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cancel_api, "%p"),
        LONGTAIL_LOGFIELD(token, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(ctx, token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)token;
    int32_t current_value = *atomic_counter;
    if (current_value)
    {
        return ECANCELED;
    }
    return 0;
}

static void AtomicCancelAPI_Dispose(struct Longtail_API* cancel_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(cancel_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, cancel_api, return)
    Longtail_Free(cancel_api);
}

static void AtomicCancelAPI_Init(struct AtomicCancelAPI* api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, api, return)
    api->m_CancelAPI.m_API.Dispose = AtomicCancelAPI_Dispose;
    api->m_CancelAPI.CreateToken = AtomicCancelAPI_CreateToken;
    api->m_CancelAPI.Cancel = AtomicCancelAPI_Cancel;
    api->m_CancelAPI.IsCancelled = AtomicCancelAPI_IsCancelled;
    api->m_CancelAPI.DisposeToken = AtomicCancelAPI_DisposeToken;
}

struct Longtail_CancelAPI* Longtail_CreateAtomicCancelAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)Longtail_Alloc(sizeof(struct AtomicCancelAPI));
    if (!api)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    AtomicCancelAPI_Init(api);
    return &api->m_CancelAPI;
}
