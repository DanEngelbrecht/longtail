#include <stdlib.h>
#include <stdio.h>
#include "../src/longtail.h"

#if defined(__GNUC__) || defined(__clang__)
#include <dlfcn.h>
#include <gnu/lib-names.h>
#endif

// clang test.c -o ../build/shared_lib_test -ldl -fsanitize=address -fno-omit-frame-pointer

typedef void (*Longtail_SetAssertDef)(Longtail_Assert assert_func);

int main()
{
#if defined(_WIN32)
#endif
#if defined(__GNUC__) || defined(__clang__)
    void* longtail = dlopen("../build/liblongtail_debug.so", RTLD_LAZY);
	if (longtail)
	{
		printf("Loaded the library!\n");

		Longtail_SetAssertDef set_assert = dlsym(longtail, "Longtail_SetAssert");
		if (set_assert != 0)
		{
			printf("Found Longtail_SetAssert function!\n");
		}
		else
		{
			printf("Failed to find the Longtail_SetAssert function: %s\n", dlerror());
		}

	    dlclose(longtail);
	}
	else
	{
		printf("Failed to load the library: %s\n", dlerror());
	}
#endif
    return 0;
}
