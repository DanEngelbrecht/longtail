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

struct BlockHashTable
{
    size_t m_TableSize;
    TLongtail_Hash* m_Keys;
    uint64_t* m_Values;

    uint64_t* m_FreeSlots;
    uint64_t m_NextFreeSlot;
};

uint64_t AllocateSlot(struct BlockHashTable block_hash_table)
{
    
}

// m_Keys[0] && m_Values[0] is reserved for hash == 0, no other entry should have 0 in its key

uint32_t __inline clz( uint64_t value )
{
    DWORD trailing_zero = 0;

    if ( _BitScanReverse64( &trailing_zero, value ) )
    {
        return (uint64_t)trailing_zero;
    }
    else
    {
        // This is undefined, I better choose 32 than 0
        return 64;
    }
}

uint64_t NextPowerOf2(uint64_t x)
{
    return x == 1 ? 1 : 1 << (clz(x-1) + 1);
}

struct BlockHashTable* AllocateBlockHashTable(size_t entry_count)
{
    size_t table_size = NextPowerOf2(entry_count) << 1;
    size_t size = sizeof(struct BlockHashTable) + 
        sizeof(TLongtail_Hash) * (table_size + 2) +
        sizeof(uint64_t) * (table_size + 2);
    struct BlockHashTable* block_hash_table = (struct BlockHashTable*)Longtail_Alloc(size);
    memset(block_hash_table, 0, size);
    uint8_t* p = (uint8_t*)&block_hash_table[1];
    block_hash_table->m_Keys = (TLongtail_Hash*)p;
    p += sizeof(TLongtail_Hash) * (table_size + 21);
    block_hash_table->m_Values = (uint64_t*)p;
    block_hash_table->m_TableSize = table_size;
    block_hash_table->m_Keys[0] = 0;
    block_hash_table->m_Keys[1] = 1;
    block_hash_table->m_Values[0] = 0;
    block_hash_table->m_Values[1] = 0;
    return block_hash_table;
};

uint64_t Put(struct BlockHashTable* block_hash_table, TLongtail_Hash key, uint64_t value)
{
    if (key == 0)
    {
        block_hash_table->m_Values[1] = value;
        return 1;
    }
    uint64_t table_mask = block_hash_table->m_TableSize - 1;
    uint64_t initial_slot = key & table_mask;
    TLongtail_Hash* keys = &block_hash_table->m_Keys[2];
    uint64_t* values = &block_hash_table->m_Values[2];
    uint64_t slot = initial_slot;
    while (true)
    {
        if (keys[slot] == 0)
        {
            block_hash_table->m_Keys[slot] = key;
            block_hash_table->m_Values[slot] = value;
            return slot + 2;
        }
        slot = (slot + 1) & table_mask;
        if (slot == initial_slot)
        {
            break;
        }
        if (keys[slot] && ((keys[slot] & table_mask) != initial_slot))
        {
            break;
        }
    }

    return 0;
}

uint64_t Get(struct BlockHashTable* block_hash_table, TLongtail_Hash key)
{
    if (key == 0)
    {
        return block_hash_table->m_Keys[1] == 0 ? 1 : 0;
    }
    uint64_t table_mask = block_hash_table->m_TableSize - 1;
    uint64_t initial_slot = key & table_mask;
    TLongtail_Hash* keys = &block_hash_table->m_Keys[2];
    uint64_t* values = &block_hash_table->m_Values[2];
    uint64_t slot = initial_slot;
    while ((keys[initial_slot] & table_mask) == initial_slot)
    {
        if (keys[slot] == key)
        {
            return slot + 2;
        }
        slot = (slot + 1) & table_mask;
        if (slot == initial_slot)
        {
            break;
        }
    }
    return 0;
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

uint64_t TestCreateBlockHashTableSpeed(struct Longtail_ContentIndex* content_index, struct BlockHashTable** block_hash_table, struct BlockHashTable** chunk_hash_table)
{
    uint64_t start = stm_now();

    uint64_t block_count = *content_index->m_BlockCount;
    uint64_t chunk_count = *content_index->m_ChunkCount;

    *block_hash_table = AllocateBlockHashTable(block_count);
    *chunk_hash_table = AllocateBlockHashTable(chunk_count);

    for (uint64_t b = 0; b < block_count; ++b)
    {
        Put(*block_hash_table, content_index->m_BlockHashes[b], b);
    }

    for (uint64_t c = 0; c < chunk_count; ++c)
    {
        Put(*chunk_hash_table, content_index->m_ChunkHashes[c], c);
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

    struct BlockHashTable* block_hash_table = 0;
    struct BlockHashTable* chunk_hash_table = 0;
    uint64_t create_blockhash_lookup_ticks = TestCreateBlockHashTableSpeed(content_index, &block_hash_table, &chunk_hash_table);
    printf("TestCreateBlockHashTableSpeed: %.3lf ms\n", stm_ms(create_blockhash_lookup_ticks));

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
