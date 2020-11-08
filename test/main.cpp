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

static void LogContext(struct Longtail_LogContext* log_context)
{
    if (log_context == 0)
    {
        return;
    }
    LogContext(log_context->parent_context);
    fprintf(stderr, " { ");
    size_t log_field_count = log_context->field_count;
    for (size_t f = 0; f < log_field_count; ++f)
    {
        struct Longtail_LogField* log_field = &log_context->fields[f];
        fprintf(stderr, "\"%s\": ", log_field->name);
        fprintf(stderr, log_field->fmt, log_field->value);
        fprintf(stderr, "%s", ((f + 1) < log_field_count) ? "," : "");
    }
    fprintf(stderr, " }");
}

static void LogStdErr(const char* file, const char* function, int line, void* context, struct Longtail_LogContext* log_context, int level, const char* log)
{
    fprintf(stderr, "%s(%d) [%s] %s", file, line, function, ERROR_LEVEL[level]);
    LogContext(log_context);
    fprintf(stderr, " : %s\n", log);
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
