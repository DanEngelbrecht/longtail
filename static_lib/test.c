#include <stdlib.h>
#include <stdio.h>
#include "../src/longtail.h"

// clang test.c -o ../build/shared_lib_test -ldl -fsanitize=address -fno-omit-frame-pointer

int main()
{
    void* p = Longtail_Alloc(16);
	Longtail_Free(p);
    return 0;
}
