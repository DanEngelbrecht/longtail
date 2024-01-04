#define STB_DS_IMPLEMENTATION

#include <stddef.h>

#ifdef STBDS_REALLOC

#include "../longtail.h"

#endif // STBDS_REALLOC

#include "stb_ds.h"

#ifdef STBDS_REALLOC

void* Longtail_STBRealloc(void* context, void* old_ptr, size_t size)
{
	return Longtail_ReAlloc("stb", old_ptr, size);
}

void Longtail_STBFree(void* context, void* ptr)
{
	Longtail_Free(ptr);
}

#endif // STBDS_REALLOC
