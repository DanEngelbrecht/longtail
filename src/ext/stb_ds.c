#define STB_DS_IMPLEMENTATION

//#define CUSTOM_STB_ALLOC

#if CUSTOM_STB_ALLOC

extern void* Longtail_STBRealloc(void* old_ptr, size_t size);
extern void Longtail_STBFree(void* ptr);

#define STBDS_REALLOC(context,ptr,size) Longtail_STBRealloc((ptr), (size))
#define STBDS_FREE(context,ptr)         Longtail_STBFree((ptr))

#endif // CUSTOM_STB_ALLOC

#include "stb_ds.h"

#if CUSTOM_STB_ALLOC

#include <stddef.h>
#include <stdint.h>

extern void* Longtail_Alloc(const char* context, size_t s);
extern void Longtail_Free(void* p);

struct Longtail_STBAllocHeader
{
	uint64_t Size;
	uint64_t _;
};

inline void* Longtail_STBRealloc(void* old_ptr, size_t size)
{
	if (size == 0)
	{
		return 0;
	}
	struct Longtail_STBAllocHeader* header = (struct Longtail_STBAllocHeader*)Longtail_Alloc("stb", sizeof(struct Longtail_STBAllocHeader) + size);
	header->Size = size;
	header->_ = 0;
	void* new_ptr = &header[1];
	if (old_ptr != 0)
	{
		struct Longtail_STBAllocHeader* old_header = &((struct Longtail_STBAllocHeader*)old_ptr)[-1];
		size_t old_size = old_header->Size;
		memcpy(new_ptr, old_ptr, old_size);
		Longtail_Free((void*)old_header);
	}
	return new_ptr;
}

inline void Longtail_STBFree(void* ptr)
{
	if (!ptr)
	{
		return;
	}
	struct Longtail_STBAllocHeader* header = &((struct Longtail_STBAllocHeader*)ptr)[-1];
	Longtail_Free((void*)header);
}

#endif // CUSTOM_STB_ALLOC
