#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct Longtail;
struct Longtail_AssetEntry;
struct Longtail_BlockEntry;
struct Longtail_ReadStorage;


// Each path -> hash
// Block content -> hash
// Block size uncompressed soft upper limit
// An asset can *not* span mutiple blocks
// Each block can have individual compression type (hash identifier)
// Lookup: path -> hash -> block hash -> block data

// Incremental update of content
//
// * Have base content with index
// * Examine new content
// * Identify blocks that have unchanged content and add asset entries pointing to unchanged blocks
// * For each asset that is not found in store in new blocks
// * New index reuses unchanged blocks!

// TODO: Restructure to put more of test.cpp into longtail (WriteStorage, ReadStorage, BlockStorage)

// Add tools to inspect index (path of resources can not be extracted!), extract content from path etc

typedef uint64_t TLongtail_Hash;

typedef int (*Longtail_OutputStream)(void* context, uint64_t byte_count, const uint8_t* data);
typedef int (*Longtail_InputStream)(void* context, uint64_t byte_count, uint8_t* data);

struct Longtail_ReadStorage
{
    // Return required memory size?
    uint64_t (*Longtail_PreflightBlocks)(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockEntry* blocks);
    const uint8_t* (*Longtail_AqcuireBlockStorage)(struct Longtail_ReadStorage* storage, uint32_t block_index);
    void (*Longtail_ReleaseBlock)(struct Longtail_ReadStorage* storage, uint32_t block_index);
};

struct Longtail_WriteStorage
{
    int (*Longtail_AllocateBlockStorage)(struct Longtail_WriteStorage* storage, TLongtail_Hash compression_type, uint64_t length, Longtail_BlockEntry* out_block_entry);
    uint8_t* (*Longtail_GetBlockData)(struct Longtail_WriteStorage* storage, const Longtail_BlockEntry* block_entry);
    int (*Longtail_CommitBlockData)(struct Longtail_WriteStorage* storage, const Longtail_BlockEntry* block_entry);
};

size_t Longtail_GetSize(uint64_t asset_entry_count, uint64_t block_entry_count);
struct Longtail* Longtail_Open(void* mem, uint64_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint64_t block_entry_count, struct Longtail_BlockEntry* block_entries);

int Longtail_Write(Longtail_WriteStorage* storage, Longtail_InputStream input_stream, uint64_t length, TLongtail_Hash compression_type, Longtail_AssetEntry** asset_entry_array, Longtail_BlockEntry** block_entry_array);
int Longtail_Preflight(struct Longtail* longtail, struct Longtail_ReadStorage* storage, uint32_t count, TLongtail_Hash* assets, uint64_t* out_sizes);
int Longtail_Read(struct Longtail* longtail, struct Longtail_ReadStorage* storage, TLongtail_Hash asset, Longtail_OutputStream output_stream, void* context);

#ifdef LONGTAIL_IMPLEMENTATION

struct Longtail_AssetEntry
{
    TLongtail_Hash m_AssetHash; // Path
    uint32_t m_BlockEntryIndex; // Block entry where it is located in m_BlockEntries array
};

struct Longtail_BlockEntry
{
    uint32_t m_BlockIndex;
    uint64_t m_StartOffset; // Raw
    uint64_t m_Length;  // Raw
};

struct Longtail
{
    uint64_t asset_entry_count;
    uint64_t block_entry_count;
    struct Longtail_AssetEntry* asset_entries;
    struct Longtail_BlockEntry* block_entries;
};

#define LONGTAIL_ARRAY_CONCAT1(x, y) x ## y
#define LONGTAIL_ARRAY_CONCAT(x, y) LONGTAIL_ARRAY_CONCAT1(x, y)


#define LONGTAIL_DECLARE_ARRAY_TYPE(t, alloc_mem, free_mem) \
    inline uint32_t Longtail_Array_GetCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        return buffer ? ((uint32_t*)buffer)[-2] : 0; \
    } \
    \
    inline uint32_t Longtail_Array_GetSize(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        return buffer ? ((uint32_t*)buffer)[-1] : 0; \
    } \
    inline void Longtail_Array_SetSize(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t size) \
    { \
        ((uint32_t*)buffer)[-1] = size; \
    } \
    \
    inline void Longtail_Array_Free(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        free_mem(buffer ? &((uint32_t*)buffer)[-2] : 0); \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_SetCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t new_capacity) \
    { \
        uint32_t current_capacity = Longtail_Array_GetCapacity(buffer); \
        if (current_capacity == new_capacity) \
        { \
            return buffer; \
        } \
        if (new_capacity == 0) \
        { \
            Longtail_Array_Free(buffer); \
            return 0; \
        } \
        uint32_t* new_buffer_base = (uint32_t*)alloc_mem(sizeof(uint32_t) * 2 + sizeof(t) * new_capacity); \
        uint32_t current_size = Longtail_Array_GetSize(buffer); \
        new_buffer_base[0] = new_capacity; \
        new_buffer_base[1] = current_size; \
        LONGTAIL_ARRAY_CONCAT(t, *) new_buffer = (LONGTAIL_ARRAY_CONCAT(t, *))&new_buffer_base[2]; \
        memmove(new_buffer, buffer, sizeof(t) * current_size); \
        Longtail_Array_Free(buffer); \
        return new_buffer; \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_IncreaseCapacity(LONGTAIL_ARRAY_CONCAT(t, *) buffer, uint32_t count) \
    { \
        uint32_t current_capacity = Longtail_Array_GetCapacity(buffer); \
        uint32_t new_capacity = current_capacity + count; \
        return Longtail_Array_SetCapacity(buffer, new_capacity); \
    } \
    \
    inline LONGTAIL_ARRAY_CONCAT(t, *) Longtail_Array_Push(LONGTAIL_ARRAY_CONCAT(t, *) buffer) \
    { \
        uint32_t offset = Longtail_Array_GetSize(buffer); \
        if (offset == Longtail_Array_GetCapacity(buffer)) \
        { \
            return 0; \
        } \
        ((uint32_t*)buffer)[-1] = offset + 1; \
        return &buffer[offset]; \
    }

#define LONGTAIL_ALIGN_SIZE_PRIVATE(x, align) (((x) + ((align)-1)) & ~((align)-1))

size_t Longtail_GetSize(uint64_t asset_entry_count, uint64_t block_entry_count)
{
    return LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail), 8) + LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail_AssetEntry) * asset_entry_count, 8) + sizeof(Longtail_BlockEntry) * block_entry_count;
}

struct Longtail* Longtail_Open(void* mem, uint64_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint64_t block_entry_count, struct Longtail_BlockEntry* block_entries)
{
    uint8_t* p = (uint8_t*)mem;
    Longtail* longtail = (Longtail*)p;
    longtail->asset_entry_count = asset_entry_count;
    longtail->block_entry_count = block_entry_count;
    p += LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail), 8);
    longtail->asset_entries = (Longtail_AssetEntry*)p;
    memcpy(longtail->asset_entries, asset_entries, sizeof(Longtail_AssetEntry) * asset_entry_count);
    p += LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail_AssetEntry) * asset_entry_count, 8);
    longtail->block_entries = (Longtail_BlockEntry*)p;
    memcpy(longtail->block_entries, block_entries, sizeof(Longtail_BlockEntry) * block_entry_count);
    return longtail;
}

static uint32_t Longtail_GetFirstBlock_private(struct Longtail* longtail, TLongtail_Hash asset)
{
    // Could do with a faster find algo
    uint32_t asset_index = 0;
    while (asset_index < longtail->asset_entry_count)
    {
        if (longtail->asset_entries[asset_index].m_AssetHash == asset)
        {
            return asset_index;
        }
        ++asset_index;
    }
    return 0xffffffffu;
}

LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_AssetEntry, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_BlockEntry, malloc, free)

int Longtail_Write(Longtail_WriteStorage* storage, TLongtail_Hash asset_hash, Longtail_InputStream input_stream, void* context, uint64_t length, TLongtail_Hash compression_type, Longtail_AssetEntry** asset_entry_array, Longtail_BlockEntry** block_entry_array)
{
    if (Longtail_Array_GetSize(*asset_entry_array) == Longtail_Array_GetCapacity(*asset_entry_array))
    {
        *asset_entry_array = Longtail_Array_IncreaseCapacity(*asset_entry_array, 16);
    }

    Longtail_BlockEntry block_entry;
    if (0 == storage->Longtail_AllocateBlockStorage(storage, compression_type, length, &block_entry))
    {
        return 0;
    }
    Longtail_AssetEntry* asset_entry = Longtail_Array_Push(*asset_entry_array);
    asset_entry->m_BlockEntryIndex = Longtail_Array_GetSize(*block_entry_array);
    asset_entry->m_AssetHash = asset_hash;
    if (Longtail_Array_GetSize(*block_entry_array) == Longtail_Array_GetCapacity(*block_entry_array))
    {
        *block_entry_array = Longtail_Array_IncreaseCapacity(*block_entry_array, 16);
    }

    uint8_t* block_data = storage->Longtail_GetBlockData(storage, &block_entry);

    if (0 == input_stream(context, length, block_data))
    {
        return 0;
    }

    *Longtail_Array_Push(*block_entry_array) = block_entry;

    storage->Longtail_CommitBlockData(storage, &block_entry);
 
    return 1;
}

int Longtail_Preflight(struct Longtail* longtail, struct Longtail_ReadStorage* storage, uint32_t count, TLongtail_Hash* assets, uint64_t* out_sizes)
{
    while(count--)
    {
        uint32_t asset_index = Longtail_GetFirstBlock_private(longtail, assets[count]);
        if (asset_index == 0xffffffffu)
        {
            return 0;
        }
        struct Longtail_AssetEntry* asset_entry = &longtail->asset_entries[asset_index];
        struct Longtail_BlockEntry* block_entry = &longtail->block_entries[asset_entry->m_BlockEntryIndex];

        out_sizes[count] = storage->Longtail_PreflightBlocks(storage, 1, block_entry);
    }
    return 1;
}

int Longtail_Read(struct Longtail* longtail, struct Longtail_ReadStorage* storage, TLongtail_Hash asset, Longtail_OutputStream output_stream, void* context)
{
    uint32_t asset_index = Longtail_GetFirstBlock_private(longtail, asset);
    if (asset_index == 0xffffffffu)
    {
        return 0;
    }

    struct Longtail_AssetEntry* asset_entry = &longtail->asset_entries[asset_index];
    struct Longtail_BlockEntry* entry = &longtail->block_entries[asset_entry->m_BlockEntryIndex];

//    uint32_t entry_count = asset_entry->m_BlockCount;
//    while (entry_count--)
//    {
        const uint8_t* block_data = storage->Longtail_AqcuireBlockStorage(storage, entry->m_BlockIndex);
        output_stream(context, entry->m_Length, &block_data[entry->m_StartOffset]);
        storage->Longtail_ReleaseBlock(storage, entry->m_BlockIndex);
//        ++entry;
//    }
    return 1;
}

#endif LONGTAIL_IMPLEMENTATION
