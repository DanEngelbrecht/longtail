#pragma once

#define LONGTAIL_ARRAY_CONCAT1(x, y) x ## y
#define LONGTAIL_ARRAY_CONCAT(x, y) LONGTAIL_ARRAY_CONCAT1(x, y)

#include <stdint.h>

#define LONGTAIL_DECLARE_ARRAY_TYPE(t, alloc_mem, free_mem) \
    inline uint32_t Longtail_Array_GetCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        return buffer ? ((uint32_t*)buffer)[-2] : 0; \
    } \
    \
    inline uint32_t Longtail_Array_GetSize(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        return buffer ? ((uint32_t*)buffer)[-1] : 0; \
    } \
    inline void Longtail_Array_SetSize(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t size) \
    { \
        ((uint32_t*)buffer)[-1] = size; \
    } \
    \
    inline void Longtail_Array_Free(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        free_mem(buffer ? &((uint32_t*)buffer)[-2] : 0); \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_SetCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t new_capacity) \
    { \
        uint32_t current_capacity = Longtail_Array_GetCapacity(buffer); \
        if (current_capacity == new_capacity) \
        { \
            return buffer; \
        } \
        if (new_capacity == 0) \
        { \
            Longtail_Array_Free(buffer); \
            return 0; \
        } \
        uint32_t* new_buffer_base = (uint32_t*)alloc_mem(sizeof(uint32_t) * 2 + sizeof(t) * new_capacity); \
        uint32_t current_size = Longtail_Array_GetSize(buffer); \
        new_buffer_base[0] = new_capacity; \
        new_buffer_base[1] = current_size; \
        LONGTAIL_ARRAY_CONCAT(t, *) new_buffer = (LONGTAIL_ARRAY_CONCAT(t, *))&new_buffer_base[2]; \
        memmove(new_buffer, buffer, sizeof(t) * current_size); \
        Longtail_Array_Free(buffer); \
        return new_buffer; \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_IncreaseCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t count) \
    { \
        uint32_t current_capacity = Longtail_Array_GetCapacity(buffer); \
        uint32_t new_capacity = current_capacity + count; \
        return Longtail_Array_SetCapacity(buffer, new_capacity); \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_Push(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        uint32_t offset = Longtail_Array_GetSize(buffer); \
        if (offset == Longtail_Array_GetCapacity(buffer)) \
        { \
            return 0; \
        } \
        ((uint32_t*)buffer)[-1] = offset + 1; \
        return &buffer[offset]; \
    }

