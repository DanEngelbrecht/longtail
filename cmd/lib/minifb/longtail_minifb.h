#pragma once

#include "../../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Longtail_FrameBufferAPI;

typedef struct Longtail_FrameBuffer* Longtail_StorageAPI_HFrameBuffer;

typedef int (*Longtail_FrameBufferAPI_OpenFunc)(struct Longtail_FrameBufferAPI* frambuffer_api, const char *title, unsigned width, unsigned height, Longtail_StorageAPI_HFrameBuffer* out_frame_buffer);
typedef int (*Longtail_FrameBufferAPI_CloseFunc)(struct Longtail_FrameBufferAPI* frambuffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer);
typedef int (*Longtail_FrameBufferAPI_UpdateFunc)(struct Longtail_FrameBufferAPI* frambuffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer, uint32_t* buffer);

struct Longtail_FrameBufferAPI
{
    struct Longtail_API m_API;
    Longtail_FrameBufferAPI_OpenFunc Open;
    Longtail_FrameBufferAPI_CloseFunc Close;
    Longtail_FrameBufferAPI_UpdateFunc Update;
};

LONGTAIL_EXPORT int Longtail_FrameBufferAPI_Open(struct Longtail_FrameBufferAPI* frame_buffer_api, const char *title, unsigned width, unsigned height, Longtail_StorageAPI_HFrameBuffer* out_frame_buffer);
LONGTAIL_EXPORT int Longtail_FrameBufferAPI_Close(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer);
LONGTAIL_EXPORT int Longtail_FrameBufferAPI_Update(struct Longtail_FrameBufferAPI* frame_buffer_api, Longtail_StorageAPI_HFrameBuffer frame_buffer, uint32_t* buffer);

LONGTAIL_EXPORT struct Longtail_FrameBufferAPI* Longtail_CreateMiniFBFrameBufferAPI();

#ifdef __cplusplus
}
#endif
