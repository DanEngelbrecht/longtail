#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <cstdlib>
#include <crtdbg.h>
#endif

#define JC_TEST_IMPLEMENTATION
#include "../third-party/jctest/src/jc_test.h"

#include "../src/longtail.h"

void TestAssert(const char* expression, const char* file, int line)
{
    printf("%s(%d): Assert failed `%s`\n", file, line, expression);
    abort();
}

int main(int argc, char** argv)
{
#ifdef _MSC_VER
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    jc_test_init(&argc, argv);
    Longtail_SetAssert(TestAssert);
    int result = jc_test_run_all();
    Longtail_SetAssert(0);
#ifdef _MSC_VER
    if (0 == result)
    {
        _CrtDumpMemoryLeaks();
    }
#endif
    return 0;
}
