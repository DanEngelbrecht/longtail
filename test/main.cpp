#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <cstdlib>
#include <crtdbg.h>
#endif

#define JC_TEST_IMPLEMENTATION
#include "ext/jc_test.h"

#include "../src/longtail.h"

static void TestAssert(const char* expression, const char* file, int line)
{
    fprintf(stderr, "%s(%d): Assert failed `%s`\n", file, line, expression);
    exit(-1);
}

static const char* ERROR_LEVEL[4] = {"DEBUG", "INFO", "WARNING", "ERROR"};

static void LogStdErr(const char* file, const char* function, int line, void* , int level, const char* log)
{
    fprintf(stderr, "%s(%d) [%s] %s: %s\n", file, line, function, ERROR_LEVEL[level], log);
}

int main(int argc, char** argv)
{
#ifdef _MSC_VER
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    jc_test_init(&argc, argv);
    Longtail_SetAssert(TestAssert);
    Longtail_SetLogLevel(LONGTAIL_LOG_LEVEL_INFO);
    Longtail_SetLog(LogStdErr, 0);
    int result = jc_test_run_all();
    Longtail_SetAssert(0);
#ifdef _MSC_VER
    if (0 == result)
    {
        _CrtDumpMemoryLeaks();
    }
#endif
    return result;
}
