#include "longtail_minifb.h"

#include "ext/minifb/include/MiniFB.h"

#if defined(_WIN32)
#pragma comment(lib, "winmm.lib")
#pragma comment(lib, "gdi32.lib")
#pragma comment(lib, "User32.lib")
#endif

#include <errno.h>

int Longtail_FrameBufferAPI_Open(struct Longtail_FrameBufferAPI* frame_buffer_api, const char *title, unsigned width, unsigned height, Longtail_StorageAPI_HFrameBuffer* out_frame_buffer)
{
    return frame_buffer_api->Open(frame_buffer_api, title, width, height, out_frame_buffer);
}

int Longtail_FrameBufferAPI_Close(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer)
{
    return frame_buffer_api->Close(frame_buffer_api, frame_buffer);
}
int Longtail_FrameBufferAPI_Update(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer, uint32_t* buffer)
{
    return frame_buffer_api->Update(frame_buffer_api, frame_buffer, buffer);
}

struct Longtail_FrameBufferAPI* Longtail_MakeMiniFBFrameBufferAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_FrameBufferAPI_OpenFunc open_func,
    Longtail_FrameBufferAPI_CloseFunc close_func,
    Longtail_FrameBufferAPI_UpdateFunc update_func)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(dispose_func, "%p"),
        LONGTAIL_LOGFIELD(open_func, "%p"),
        LONGTAIL_LOGFIELD(close_func, "%p"),
        LONGTAIL_LOGFIELD(update_func, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)
    struct Longtail_FrameBufferAPI* api = (struct Longtail_FrameBufferAPI*)mem;
    api->m_API.Dispose = dispose_func;
    api->Open = open_func;
    api->Close = close_func;
    api->Update = update_func;
    return api;
}

struct MiniFBFrameBufferAPI
{
    struct Longtail_FrameBufferAPI m_API;
};

static void MiniFBDispose(struct Longtail_API* api)
{
    Longtail_Free(api);
}

static int MiniFBOpen(struct Longtail_FrameBufferAPI* frame_buffer_api, const char *title, unsigned width, unsigned height, Longtail_StorageAPI_HFrameBuffer* out_frame_buffer)
{
#if defined(_WIN32)
    struct mfb_window* window = mfb_open_ex("longtail", 800, 600, WF_RESIZABLE);
    if (window == 0)
    {
        return ENOMEM;
    }
    *out_frame_buffer = (Longtail_StorageAPI_HFrameBuffer)window;
    return 0;
#else
    return ENOTSUP;
#endif
}

static int MiniFBClose(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer)
{
#if defined(_WIN32)
    struct mfb_window* window = (struct mfb_window*)frame_buffer;
    mfb_close(window);
    return 0;
#else
    return ENOTSUP;
#endif
}

static int MiniFBUpdate(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer, uint32_t* buffer)
{
#if defined(_WIN32)
    struct mfb_window* window = (struct mfb_window*)frame_buffer;
    int state = mfb_update(window, buffer);
    if (state < 0)
    {
        // TODO: What error?
        return ENOTCONN;
    }
    mfb_wait_sync(window);
    return 0;
#else
    return ENOTSUP;
#endif
}

static int MiniFBFrameBufferAPI_Init(
    void* mem,
    struct Longtail_FrameBufferAPI** out_frame_buffer_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_frame_buffer_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0);
    struct Longtail_FrameBufferAPI* api = Longtail_MakeMiniFBFrameBufferAPI(
        mem,
        MiniFBDispose,
        MiniFBOpen,
        MiniFBClose,
        MiniFBUpdate);

    struct MiniFBFrameBufferAPI* frame_buffer_api = (struct MiniFBFrameBufferAPI*)api;
    *out_frame_buffer_api = &frame_buffer_api->m_API;
    return 0;
}

struct Longtail_FrameBufferAPI* Longtail_CreateMiniFBFrameBufferAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    void* mem = (struct InMemStorageAPI*)Longtail_Alloc("MiniFBFrameBufferAPI", sizeof(struct MiniFBFrameBufferAPI));
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_FrameBufferAPI* frame_buffer_api;
    int err = MiniFBFrameBufferAPI_Init(mem, &frame_buffer_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "MiniFBFrameBufferAPI_Init() failed with %d", err)
        Longtail_Free(frame_buffer_api);
        return 0;
    }
    return frame_buffer_api;
}

