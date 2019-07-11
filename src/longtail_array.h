#pragma once

#define LONGTAIL_ARRAY_CONCAT1(x, y) x ## y
#define LONGTAIL_ARRAY_CONCAT(x, y) LONGTAIL_ARRAY_CONCAT1(x, y)

#include <stdint.h>
#include <string.h>

typedef void* (*longtail_impl_alloc_func)(size_t);
typedef void (*longtail_impl_free_func)(void*);

inline uint32_t Longtail_Array_GetCapacity_impl(void* buffer)
{
    return buffer ? ((uint32_t*)buffer)[-2] : 0;
}

inline uint32_t Longtail_Array_GetSize_impl(void* buffer)
{
    return buffer ? ((uint32_t*)buffer)[-1] : 0;
}

inline void Longtail_Array_SetSize_impl(void* buffer, uint32_t size)
{
    if (buffer && size)
    {
        ((uint32_t*)buffer)[-1] = size;
    }
}

inline void Longtail_Array_Free_impl(void* buffer, longtail_impl_free_func free_func)
{
    free_func(buffer ? &((uint32_t*)buffer)[-2] : 0);
}

inline void* Longtail_Array_SetCapacity_impl(void* buffer, size_t type_size, uint32_t new_capacity, longtail_impl_alloc_func alloc_func, longtail_impl_free_func free_func)
{
    uint32_t current_capacity = Longtail_Array_GetCapacity_impl(buffer);
    if (current_capacity == new_capacity)
    {
        return buffer;
    }
    if (new_capacity == 0)
    {
        Longtail_Array_Free_impl(buffer, free_func);
        return 0;
    }
    uint32_t* new_buffer_base = (uint32_t*)alloc_func(sizeof(uint32_t) * 2 + type_size * new_capacity);
    uint32_t current_size = Longtail_Array_GetSize_impl(buffer);
    new_buffer_base[0] = new_capacity;
    new_buffer_base[1] = current_size;
    void* new_buffer = (void*)&new_buffer_base[2];
    memmove(new_buffer, buffer, type_size * current_size);
    Longtail_Array_Free_impl(buffer, free_func);
    return new_buffer;
}

inline void* Longtail_Array_IncreaseCapacity_impl(void* buffer, size_t type_size, uint32_t count, longtail_impl_alloc_func alloc_func, longtail_impl_free_func free_func)
{
    uint32_t current_capacity = Longtail_Array_GetCapacity_impl(buffer);
    uint32_t new_capacity = current_capacity + count;
    return Longtail_Array_SetCapacity_impl(buffer, type_size, new_capacity, alloc_func, free_func);
}

inline void* Longtail_Array_EnsureCapacity_impl(void* buffer, size_t type_size, uint32_t size_increment, longtail_impl_alloc_func alloc_func, longtail_impl_free_func free_func)
{
    uint32_t size = Longtail_Array_GetSize_impl(buffer);
    uint32_t current_capacity = Longtail_Array_GetCapacity_impl(buffer);
    if (size < current_capacity)
    {
        return buffer;
    }
    uint32_t new_capacity = current_capacity + size_increment;
    return Longtail_Array_SetCapacity_impl(buffer, type_size, new_capacity, alloc_func, free_func);
}

inline void* Longtail_Array_Push_impl(void* buffer, size_t type_size)
{
    uint32_t offset = Longtail_Array_GetSize_impl(buffer);
    if (offset == Longtail_Array_GetCapacity_impl(buffer))
    {
        return 0;
    }
    ((uint32_t*)buffer)[-1] = offset + 1;
    return &((uint8_t*)buffer)[offset * type_size];
}

inline void Longtail_Array_Pop_impl(void* buffer)
{
    Longtail_Array_SetSize_impl(buffer, Longtail_Array_GetSize_impl(buffer) - 1);
}

#define LONGTAIL_DECLARE_ARRAY_TYPE(t, alloc_mem, free_mem) \
    inline uint32_t LONGTAIL_ARRAY_CONCAT(GetCapacity_, t)(t* buffer) { return Longtail_Array_GetCapacity_impl(buffer); } \
    inline uint32_t LONGTAIL_ARRAY_CONCAT(GetSize_, t)(t* buffer) { return Longtail_Array_GetSize_impl(buffer); } \
    inline void LONGTAIL_ARRAY_CONCAT(SetSize_, t)(t* buffer, uint32_t size) { return Longtail_Array_SetSize_impl(buffer, size); } \
    inline void LONGTAIL_ARRAY_CONCAT(Free_, t)(t* buffer) { Longtail_Array_Free_impl(buffer, free_mem); } \
    inline t* LONGTAIL_ARRAY_CONCAT(SetCapacity_, t)(t* buffer, uint32_t new_capacity) { return (t*)Longtail_Array_SetCapacity_impl(buffer, sizeof(t), new_capacity, alloc_mem, free_mem); } \
    inline t* LONGTAIL_ARRAY_CONCAT(IncreaseCapacity_, t)(t* buffer, uint32_t count) { return (t*)Longtail_Array_IncreaseCapacity_impl(buffer, sizeof(t), count, alloc_mem, free_mem); } \
    inline t* LONGTAIL_ARRAY_CONCAT(EnsureCapacity_, t)(t* buffer, uint32_t size_increment) { return (t*)Longtail_Array_EnsureCapacity_impl(buffer, sizeof(t), size_increment, alloc_mem, free_mem); } \
    inline t* LONGTAIL_ARRAY_CONCAT(Push_, t)(t* buffer) { return (t*)Longtail_Array_Push_impl(buffer, sizeof(t)); } \
    inline void LONGTAIL_ARRAY_CONCAT(Pop_, t)(t* buffer) { Longtail_Array_Pop_impl(buffer); }
