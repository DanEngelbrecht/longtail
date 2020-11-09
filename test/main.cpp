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

static const char* ERROR_LEVEL[5] = {"DEBUG", "INFO", "WARNING", "ERROR", "OFF"};

static int LogContext(struct Longtail_LogContext* log_context, char* buffer, int buffer_size)
{
    if (log_context == 0 || log_context->field_count == 0)
    {
        return 0;
    }
    int len = sprintf(buffer, " { ");
    size_t log_field_count = log_context->field_count;
    for (size_t f = 0; f < log_field_count; ++f)
    {
        struct Longtail_LogField* log_field = &log_context->fields[f];
        len += snprintf(&buffer[len], buffer_size - len, "\"%s\": %s%s", log_field->name, log_field->value, ((f + 1) < log_field_count) ? ", " : "");
    }
    len += snprintf(&buffer[len], buffer_size - len, " }");
    return len;
}

static void LogStdErr(struct Longtail_LogContext* log_context, const char* log)
{
    char buffer[2048];
    int len = snprintf(buffer, 2048, "%s(%d) [%s] %s", log_context->file, log_context->line, log_context->function, ERROR_LEVEL[log_context->level]);
    len += LogContext(log_context, &buffer[len], 2048 - len);
    snprintf(&buffer[len], 2048 - len, " : %s\n", log);
    fprintf(stderr, "%s", buffer);
}

int main(int argc, char** argv)
{
#ifdef _MSC_VER
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    jc_test_init(&argc, argv);
    Longtail_SetAssert(TestAssert);
    Longtail_SetLogLevel(LONGTAIL_LOG_LEVEL_ERROR);
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
