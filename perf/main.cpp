#ifdef _MSC_VER
#define _CRTDBG_MAP_ALLOC
#include <cstdlib>
#include <crtdbg.h>
#endif

#include <intrin.h>

#include <stdio.h>

#define SOKOL_IMPL
#include "ext/sokol_time.h"
#define STB_DS_IMPLEMENTATION
#include "../src/ext/stb_ds.h"

#include "../src/longtail.h"
#include "../lib/filestorage/longtail_filestorage.h"


struct Longtail_LookupTable
{
    uint64_t  m_BucketCount;

    uint64_t m_NextFreeIndex;
    uint64_t m_Capcacity;
    uint64_t m_Count;

    uint64_t* m_Buckets;
    uint64_t* m_Keys;
    uint64_t* m_Values;
    uint64_t* m_NextIndex;
};

static uint64_t Longtail_LookupTable_Capacity(struct Longtail_LookupTable* lut)
{
    return lut->m_Capcacity;
}

static uint64_t Longtail_LookupTable_Size(struct Longtail_LookupTable* lut)
{
    return lut->m_Count;
}

static int Longtail_LookupTable_Put(struct Longtail_LookupTable* lut, uint64_t key, uint64_t value)
{
    if (lut->m_NextFreeIndex == lut->m_Capcacity)
    {
        return ENOMEM;
    }

    uint64_t entry_index = lut->m_NextFreeIndex++;
    if (entry_index == 0xfffffffffffffffful)
    {
        return ENOMEM;
    }
    lut->m_Keys[entry_index] = key;
    lut->m_Values[entry_index] = value;
    lut->m_Count++;

    uint64_t bucket_index = key & (lut->m_BucketCount - 1);
    uint64_t index = lut->m_Buckets[bucket_index];
    if (index == 0xfffffffffffffffful)
    {
        lut->m_Buckets[bucket_index] = entry_index;
        return 0;
    }
    uint64_t next = lut->m_NextIndex[index];
    while (next != 0xfffffffffffffffful)
    {
        index = next;
        next = lut->m_NextIndex[index];
    }

    lut->m_NextIndex[index] = entry_index;
    return 0;
}

static uint64_t Longtail_LookupTable_Get(struct Longtail_LookupTable* lut, uint64_t key)
{
    uint64_t bucket_index = key & (lut->m_BucketCount - 1);
    uint64_t index = lut->m_Buckets[bucket_index];
    while (index != 0xfffffffffffffffful)
    {
        if (lut->m_Keys[index] == key)
        {
            return index;
        }
        index = lut->m_NextIndex[index];
    }
    return 0xfffffffffffffffful;
}

static struct Longtail_LookupTable* Longtail_LookupTable_Create(size_t capacity, struct Longtail_LookupTable* optional_source_entries)
{
    size_t table_size = 1;
    while (table_size < (capacity / 2))
    {
        table_size <<= 1;
    }
    size_t mem_size = sizeof(struct Longtail_LookupTable) +
        sizeof(uint64_t) * table_size +
        sizeof(uint64_t) * capacity +
        sizeof(uint64_t) * capacity +
        sizeof(uint64_t) * capacity;
    struct Longtail_LookupTable* lut = (struct Longtail_LookupTable*)Longtail_Alloc(mem_size);
    if (!lut)
    {
        return 0;
    }
    memset(lut, 0xff, mem_size);

    lut->m_BucketCount = table_size;
    lut->m_NextFreeIndex = 0;
    lut->m_Capcacity = capacity;
    lut->m_Count = 0;
    lut->m_Buckets = (uint64_t*)&lut[1];
    lut->m_Keys = (uint64_t*)&lut->m_Buckets[table_size];
    lut->m_Values = &lut->m_Keys[capacity];
    lut->m_NextIndex = &lut->m_Values[capacity];

    if (optional_source_entries)
    {
        for (uint64_t i = 0; i < optional_source_entries->m_BucketCount; ++i)
        {
            if (optional_source_entries->m_Buckets[i] != 0xfffffffffffffffful)
            {
                uint64_t index = optional_source_entries->m_Buckets[i];
                while (index != 0xfffffffffffffffful)
                {
                    uint64_t key = optional_source_entries->m_Keys[index];
                    uint64_t value = optional_source_entries->m_Values[index];
                    Longtail_LookupTable_Put(lut, key, value);
                    index = optional_source_entries->m_NextIndex[index];
                }
            }
        }
    }
    return lut;
}
















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
        if (i != b)
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
        if (i != c)
        {
            return (uint64_t)-1;
        }
    }
    return stm_now() - start;
}

uint64_t TestCreateBlockHashTableSpeed(struct Longtail_ContentIndex* content_index, struct Longtail_LookupTable** block_hash_table, struct Longtail_LookupTable** chunk_hash_table)
{
    uint64_t start = stm_now();

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    *block_hash_table = Longtail_LookupTable_Create(block_count, 0);
    *chunk_hash_table = Longtail_LookupTable_Create(chunk_count, 0);

    for (uint64_t b = 0; b < block_count; ++b)
    {
        Longtail_LookupTable_Put(*block_hash_table, content_index->m_BlockHashes[b], b);
    }

    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        Longtail_LookupTable_Put(*chunk_hash_table, content_index->m_ChunkHashes[c], c);
    }
    return stm_now() - start;
}

uint64_t TestLookupBlockHashTableSpeed(
    struct Longtail_ContentIndex* content_index,
    struct Longtail_LookupTable* block_lookup_table,
    struct Longtail_LookupTable* chunk_lookup_table)
{
    uint64_t start = stm_now();

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    for (uint64_t b = 0; b < block_count; ++b)
    {
        uint64_t index = Longtail_LookupTable_Get(block_lookup_table, content_index->m_BlockHashes[b]);
        if (index == 0xfffffffffffffffful)
        {
            return (uint64_t)-1;
        }
        if (index != b)
        {
            return (uint64_t)-1;
        }
    }

    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        uint64_t index = Longtail_LookupTable_Get(chunk_lookup_table, content_index->m_ChunkHashes[c]);
        if (index == 0xfffffffffffffffful)
        {
            return (uint64_t)-1;
        }
        if (index != c)
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

    struct Longtail_LookupTable* block_hash_table = 0;
    struct Longtail_LookupTable* chunk_hash_table = 0;
    uint64_t create_blockhash_lookup_ticks = TestCreateBlockHashTableSpeed(content_index, &block_hash_table, &chunk_hash_table);
    printf("TestCreateBlockHashTableSpeed: %.3lf ms\n", stm_ms(create_blockhash_lookup_ticks));

    uint64_t block_hash_lookup_ticks = TestLookupBlockHashTableSpeed(content_index, block_hash_table, chunk_hash_table);
    printf("TestLookupBlockHashTableSpeed: %.3lf ms\n", stm_ms(block_hash_lookup_ticks));

    Longtail_Free(chunk_hash_table);
    Longtail_Free(block_hash_table);

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
