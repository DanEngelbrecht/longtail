#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <cstdlib>
#include <crtdbg.h>
#endif

#define JC_TEST_IMPLEMENTATION
#include "ext/jc_test.h"

#include "../src/longtail.h"
#include "../lib/memtracer/longtail_memtracer.h"

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
    int len = 0;
    int part_len = sprintf(buffer, " { ");
    if (len + part_len >= buffer_size)
    {
        buffer[len] = 0;
        return len;
    }
    len += part_len;
    size_t log_field_count = log_context->field_count;
    for (size_t f = 0; f < log_field_count; ++f)
    {
        struct Longtail_LogField* log_field = &log_context->fields[f];
        part_len = snprintf(&buffer[len], buffer_size - len, "\"%s\": %s%s", log_field->name, log_field->value, ((f + 1) < log_field_count) ? ", " : "");
        if (len + part_len >= buffer_size)
        {
            buffer[len] = 0;
            return len;
        }
        len += part_len;
    }
    part_len = snprintf(&buffer[len], buffer_size - len, " }");
    if (len + part_len >= buffer_size)
    {
        buffer[len] = 0;
        return len;
    }
    len += part_len;
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
    Longtail_MemTracer_Init();
    Longtail_SetAllocAndFree(Longtail_MemTracer_Alloc, Longtail_MemTracer_Free);
    Longtail_SetAssert(TestAssert);
    Longtail_SetLogLevel(LONGTAIL_LOG_LEVEL_ERROR);
    Longtail_SetLog(LogStdErr, 0);
    int result = jc_test_run_all();
    Longtail_SetAssert(0);
    Longtail_MemTracer_DumpStats("test.csv");
    char* memtrace_stats = Longtail_MemTracer_GetStats(Longtail_GetMemTracerSummary());
    printf("%s", memtrace_stats);
    Longtail_Free(memtrace_stats);
    Longtail_MemTracer_Dispose();
#ifdef _MSC_VER
    if (0 == result)
    {
        _CrtDumpMemoryLeaks();
    }
#endif
    return result;
}
