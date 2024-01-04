#define STB_DS_IMPLEMENTATION

#ifdef STBDS_REALLOC

extern void* Longtail_STBRealloc(void* context, void* old_ptr, size_t size);
extern void Longtail_STBFree(void* context, void* ptr);

#endif // STBDS_REALLOC

#include "stb_ds.h"

#ifdef STBDS_REALLOC

extern void* Longtail_ReAlloc(const char* context, void* old, size_t s);
extern void Longtail_Free(void* p);

inline void* Longtail_STBRealloc(void* context, void* old_ptr, size_t size)
{
	return Longtail_ReAlloc("stb", old_ptr, size);
}

inline void Longtail_STBFree(void* context, void* ptr)
{
	Longtail_Free(ptr);
}

#endif // STBDS_REALLOC
