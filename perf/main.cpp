#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <cstdlib>
#include <crtdbg.h>
#endif

#include <stdio.h>

#define SOKOL_IMPL
#include "ext/sokol_time.h"
#define STB_DS_IMPLEMENTATION
#include "../src/ext/stb_ds.h"

#include "../src/longtail.h"
#include "../lib/filestorage/longtail_filestorage.h"

static void TestAssert(const char* expression, const char* file, int line)
{
    fprintf(stderr, "%s(%d): Assert failed `%s`\n", file, line, expression);
    exit(-1);
}

static const char* ERROR_LEVEL[4] = {"DEBUG", "INFO", "WARNING", "ERROR"};

static void LogStdErr(void* , int level, const char* log)
{
    fprintf(stderr, "%s: %s\n", ERROR_LEVEL[level], log);
}

uint64_t TestReadSpeed(
    struct Longtail_StorageAPI* storage_api,
    const char* path,
    struct Longtail_ContentIndex** out_content_index)
{
    uint64_t start = stm_now();

    Longtail_ReadContentIndex(storage_api, path, out_content_index);

    return stm_now() - start;
}

struct LookupEntry
{
    TLongtail_Hash key;
    uint64_t value;
};

uint64_t TestCreateHashMapSpeed(
    struct Longtail_ContentIndex* content_index,
    struct LookupEntry** block_lookup_table,
    struct LookupEntry** chunk_lookup_table)
{
    uint64_t start = stm_now();

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    for (uint64_t b = 0; b < block_count; ++b)
    {
        hmput(*block_lookup_table, content_index->m_BlockHashes[b], b);
    }

    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        hmput(*chunk_lookup_table, content_index->m_ChunkHashes[c], c);
    }
    return stm_now() - start;
}

uint64_t TestLookupHashMapSpeed(
    struct Longtail_ContentIndex* content_index,
    struct LookupEntry* block_lookup_table,
    struct LookupEntry* chunk_lookup_table)
{
    uint64_t start = stm_now();

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    for (uint64_t b = 0; b < block_count; ++b)
    {
        intptr_t i = hmgeti(block_lookup_table, content_index->m_BlockHashes[b]);
        if (i == -1)
        {
            return (uint64_t)-1;
        }
    }

    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        intptr_t i = hmgeti(chunk_lookup_table, content_index->m_ChunkHashes[c]);
        if (i == -1)
        {
            return (uint64_t)-1;
        }
    }
    return stm_now() - start;
}

int main(int argc, char** argv)
{
    int result = 0;

#ifdef _MSC_VER
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    Longtail_SetAssert(TestAssert);
    Longtail_SetLog(LogStdErr, 0);

    stm_setup();

    struct Longtail_StorageAPI* storage_api = Longtail_CreateFSStorageAPI();

    struct Longtail_ContentIndex* content_index = 0;
    uint64_t read_ticks = TestReadSpeed(storage_api, "D:\\Temp\\Pioneer_Client_store_store.lci", &content_index);

    printf("TestReadSpeed: %.3lf ms\n", stm_ms(read_ticks));


    struct LookupEntry* block_lookup_table = 0;
    struct LookupEntry* chunk_lookup_table = 0;

    uint64_t create_lookup_ticks = TestCreateHashMapSpeed(content_index, &block_lookup_table, &chunk_lookup_table);
    printf("TestCreateHashMapSpeed: %.3lf ms\n", stm_ms(create_lookup_ticks));

    uint64_t lookup_ticks = TestLookupHashMapSpeed(content_index, block_lookup_table, chunk_lookup_table);
    printf("TestLookupHashMapSpeed: %.3lf ms\n", stm_ms(lookup_ticks));


    hmfree(chunk_lookup_table);
    hmfree(block_lookup_table);

    Longtail_Free(content_index);

    SAFE_DISPOSE_API(storage_api);

    Longtail_SetAssert(0);
#ifdef _MSC_VER
    if (0 == result)
    {
        _CrtDumpMemoryLeaks();
    }
#endif
    return result;
}
