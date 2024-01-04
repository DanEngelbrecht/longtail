#define STB_DS_IMPLEMENTATION

#ifdef STBDS_REALLOC

#include "../longtail.h"
#include "stb_ds.h"

void* Longtail_STBRealloc(void* context, void* old_ptr, size_t size)
{
	return Longtail_ReAlloc("stb", old_ptr, size);
}

void Longtail_STBFree(void* context, void* ptr)
{
	Longtail_Free(ptr);
}

#else

#include "stb_ds.h"

#endif // STBDS_REALLOC
