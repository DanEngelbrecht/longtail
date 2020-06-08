#include <stdlib.h>
#include <stdio.h>
#include "../src/longtail.h"
#include "../lib/filestorage/longtail_filestorage.h"

// clang test.c -o ../build/shared_lib_test -ldl -fsanitize=address -fno-omit-frame-pointer

int main()
{
//    void* p = Longtail_Alloc(16);
    struct Longtail_StorageAPI* fs = Longtail_CreateFSStorageAPI();
    Longtail_DisposeAPI(&fs->m_API);
//	Longtail_Free(p);
    printf("Pass\n");
    return 0;
}
