#include "longtail_atomiccancel.h"

#include "../../src/longtail.h"
#include "../longtail_platform.h"
#include <errno.h>

struct AtomicCancelAPI
{
    struct Longtail_CancelAPI m_CancelAPI;
};

static int AtomicCancelAPI_CreateToken(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken* out_token)
{
    LONGTAIL_VALIDATE_INPUT(cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(out_token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)Longtail_Alloc(sizeof(sizeof(TLongtail_Atomic32)));
    if (!atomic_counter)
    {
        LONGTAIL_LOG(LONGTAIL_LOG_LEVEL_ERROR, "AtomicCancelAPI_CreateToken(%p, %p) failed with %d",
            cancel_api, out_token,
            ENOMEM)
        return ENOMEM;
    }
    *atomic_counter = 0;
    *out_token = (Longtail_CancelAPI_HCancelToken)atomic_counter;
    return 0;
}

static int AtomicCancelAPI_Cancel(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
{
    LONGTAIL_VALIDATE_INPUT(cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(token, return EINVAL )
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
    LONGTAIL_VALIDATE_INPUT(cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)token;
    Longtail_Free((void*)atomic_counter);
    return 0;
}

static int AtomicCancelAPI_IsCancelled(struct Longtail_CancelAPI* cancel_api, Longtail_CancelAPI_HCancelToken token)
{
    LONGTAIL_VALIDATE_INPUT(cancel_api, return EINVAL )
    LONGTAIL_VALIDATE_INPUT(token, return EINVAL )
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)cancel_api;
    TLongtail_Atomic32* atomic_counter = (TLongtail_Atomic32*)token;
    int32_t current_value = *atomic_counter;
    if (*atomic_counter)
    {
        return ECANCELED;
    }
    return 0;
}

static void AtomicCancelAPI_Dispose(struct Longtail_API* cancel_api)
{
    LONGTAIL_VALIDATE_INPUT(cancel_api, return)
    Longtail_Free(cancel_api);
}

static void AtomicCancelAPI_Init(struct AtomicCancelAPI* api)
{
    LONGTAIL_FATAL_ASSERT(api, return)
    api->m_CancelAPI.m_API.Dispose = AtomicCancelAPI_Dispose;
    api->m_CancelAPI.CreateToken = AtomicCancelAPI_CreateToken;
    api->m_CancelAPI.Cancel = AtomicCancelAPI_Cancel;
    api->m_CancelAPI.IsCancelled = AtomicCancelAPI_IsCancelled;
    api->m_CancelAPI.DisposeToken = AtomicCancelAPI_DisposeToken;
}

struct Longtail_CancelAPI* Longtail_CreateAtomicCancelAPI()
{
    struct AtomicCancelAPI* api = (struct AtomicCancelAPI*)Longtail_Alloc(sizeof(struct AtomicCancelAPI));
    if (!api)
    {
        // TOOD: Log
        return 0;
    }
    AtomicCancelAPI_Init(api);
    return &api->m_CancelAPI;
}
