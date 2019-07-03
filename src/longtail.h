#pragma once

#include "longtail_array.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct Longtail;
struct Longtail_AssetEntry;
struct Longtail_BlockStore;
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

// Maybe we should keep asset hash -> block alive in writer and have the ability to reallocate (merge) blocks
// after the fact?

typedef uint64_t TLongtail_Hash;

typedef int (*Longtail_OutputStream)(void* context, uint64_t byte_count, const uint8_t* data);
typedef int (*Longtail_InputStream)(void* context, uint64_t byte_count, uint8_t* data);

struct Longtail_ReadStorage
{
    // Return required memory size?
//    uint64_t (*Longtail_PreflightBlocks)(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockStore* blocks);
    const uint8_t* (*Longtail_AqcuireBlockStorage)(struct Longtail_ReadStorage* storage, uint32_t block_index);
    void (*Longtail_ReleaseBlock)(struct Longtail_ReadStorage* storage, uint32_t block_index);
};

struct Longtail_WriteStorage
{
    int (*Longtail_AddExistingBlock)(struct Longtail_WriteStorage* storage, TLongtail_Hash hash, uint32_t* out_block_index);
    int (*Longtail_AllocateBlockStorage)(struct Longtail_WriteStorage* storage, TLongtail_Hash tag, uint64_t length, Longtail_BlockStore* out_block_entry);
    int (*Longtail_WriteBlockData)(struct Longtail_WriteStorage* storage, const Longtail_BlockStore* block_entry, Longtail_InputStream input_stream, void* context);
    int (*Longtail_CommitBlockData)(struct Longtail_WriteStorage* storage, const Longtail_BlockStore* block_entry);
    TLongtail_Hash (*Longtail_FinalizeBlock)(struct Longtail_WriteStorage* storage, uint32_t block_index);
};

size_t Longtail_GetSize(uint64_t asset_entry_count, uint64_t block_entry_count);
struct Longtail* Longtail_Open(void* mem, uint64_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint64_t block_count, TLongtail_Hash* block_hashes);

int Longtail_Write(Longtail_WriteStorage* storage, Longtail_InputStream input_stream, uint64_t length, TLongtail_Hash tag, Longtail_AssetEntry** asset_entry_array);
int Longtail_Preflight(struct Longtail* longtail, struct Longtail_ReadStorage* storage, uint32_t count, TLongtail_Hash* assets, uint64_t* out_sizes);
int Longtail_Read(struct Longtail* longtail, struct Longtail_ReadStorage* storage, TLongtail_Hash asset, Longtail_OutputStream output_stream, void* context);

#ifdef LONGTAIL_IMPLEMENTATION

struct Longtail_BlockStore
{
    uint32_t m_BlockIndex;
    uint64_t m_StartOffset; // Raw
    uint64_t m_Length;  // Raw
};

struct Longtail_AssetEntry
{
    TLongtail_Hash m_AssetHash; // Path
    Longtail_BlockStore m_BlockStore;
};

//struct Longtail_BlockEntry
//{
//    uint32_t m_BlockIndex;
//    uint64_t m_StartOffset; // Raw
//    uint64_t m_Length;  // Raw
//};

struct Longtail
{
    uint64_t asset_entry_count;
    uint64_t block_count;
    struct Longtail_AssetEntry* asset_entries;
    TLongtail_Hash* block_hashes;
};

struct Longtail_BlockAssets
{
    uint64_t m_AssetIndex;
    uint64_t m_AssetCount;
};

#define LONGTAIL_ALIGN_SIZE_PRIVATE(x, align) (((x) + ((align)-1)) & ~((align)-1))

size_t Longtail_GetSize(uint64_t asset_entry_count, uint64_t block_count)
{
    return LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail), 8) +
        LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail_AssetEntry) * asset_entry_count, 8) +
//        LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail_BlockEntry) * block_entry_count, 8) +
        (sizeof(TLongtail_Hash) * block_count);
}

struct Longtail* Longtail_Open(void* mem, uint64_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint64_t block_count, TLongtail_Hash* block_hashes)
{
    uint8_t* p = (uint8_t*)mem;
    Longtail* longtail = (Longtail*)p;
    longtail->asset_entry_count = asset_entry_count;
    longtail->block_count = block_count;
    p += LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail), 8);

    longtail->asset_entries = (Longtail_AssetEntry*)p;
    memcpy(longtail->asset_entries, asset_entries, sizeof(Longtail_AssetEntry) * asset_entry_count);
    p += LONGTAIL_ALIGN_SIZE_PRIVATE(sizeof(Longtail_AssetEntry) * asset_entry_count, 8);

    longtail->block_hashes = (TLongtail_Hash*)p;
    memcpy(longtail->block_hashes, block_hashes, sizeof(TLongtail_Hash) * block_count);
    return longtail;
}

LONGTAIL_DECLARE_ARRAY_TYPE(Longtail_AssetEntry, malloc, free)
LONGTAIL_DECLARE_ARRAY_TYPE(TLongtail_Hash, malloc, free)

struct Longtail_Builder
{
    Longtail_WriteStorage* m_Storage;
    struct Longtail_AssetEntry* m_AssetEntries;
    TLongtail_Hash* m_BlockHashes;
};

void Longtail_Builder_Initialize(struct Longtail_Builder* builder, Longtail_WriteStorage* storage)
{
    builder->m_Storage = storage;
    builder->m_AssetEntries = 0;
    builder->m_BlockHashes = 0;
}

static int Longtail_Builder_Add(struct Longtail_Builder* builder, TLongtail_Hash asset_hash, Longtail_InputStream input_stream, void* context, uint64_t length, TLongtail_Hash tag)
{
    if (GetSize_Longtail_AssetEntry(builder->m_AssetEntries) == GetCapacity_Longtail_AssetEntry(builder->m_AssetEntries))
    {
         builder->m_AssetEntries = IncreaseCapacity_Longtail_AssetEntry(builder->m_AssetEntries, 16);
    }

    Longtail_AssetEntry* asset_entry = Push_Longtail_AssetEntry(builder->m_AssetEntries);
    if (0 == builder->m_Storage->Longtail_AllocateBlockStorage(builder->m_Storage, tag, length, &asset_entry->m_BlockStore))
    {
        Pop_Longtail_AssetEntry(builder->m_AssetEntries);
        return 0;
    }

    if (asset_entry->m_BlockStore.m_BlockIndex >= GetCapacity_TLongtail_Hash(builder->m_BlockHashes))
    {
        builder->m_BlockHashes = SetCapacity_TLongtail_Hash(builder->m_BlockHashes, asset_entry->m_BlockStore.m_BlockIndex + 16);
    }

    if (asset_entry->m_BlockStore.m_BlockIndex >= GetSize_TLongtail_Hash(builder->m_BlockHashes))
    {
        SetSize_TLongtail_Hash(builder->m_BlockHashes, asset_entry->m_BlockStore.m_BlockIndex + 1);
    }

    asset_entry->m_AssetHash = asset_hash;

    if (0 == builder->m_Storage->Longtail_WriteBlockData(builder->m_Storage, &asset_entry->m_BlockStore, input_stream, context))
    {
        Pop_Longtail_AssetEntry(builder->m_AssetEntries);
        return 0;
    }
    if (0 == builder->m_Storage->Longtail_CommitBlockData(builder->m_Storage, &asset_entry->m_BlockStore))
    {
        Pop_Longtail_AssetEntry(builder->m_AssetEntries);
        return 0;
    }
    return 1;
}

static int Logtail_Builder_AddExistingBlock(struct Longtail_Builder* builder, TLongtail_Hash block_hash, uint32_t asset_count, TLongtail_Hash* asset_hashes, uint64_t* start_offsets, uint64_t* lengths)
{
    uint32_t block_index = 0;
    if (0 == builder->m_Storage->Longtail_AddExistingBlock(builder->m_Storage, block_hash, &block_index))
    {
        return 0;
    }
    if (block_index >= GetCapacity_TLongtail_Hash(builder->m_BlockHashes))
    {
        builder->m_BlockHashes = SetCapacity_TLongtail_Hash(builder->m_BlockHashes, block_index + 16);
    }

    if (block_index >= GetSize_TLongtail_Hash(builder->m_BlockHashes))
    {
        SetSize_TLongtail_Hash(builder->m_BlockHashes, block_index + 1);
    }

    if (asset_count >= GetCapacity_Longtail_AssetEntry(builder->m_AssetEntries))
    {
        builder->m_AssetEntries = SetCapacity_Longtail_AssetEntry(builder->m_AssetEntries, asset_count + 16);
    }

    for (uint32_t a = 0; a < asset_count; ++a)
    {
        Longtail_AssetEntry* asset_entry = Push_Longtail_AssetEntry(builder->m_AssetEntries);
        asset_entry->m_AssetHash = asset_hashes[a];
        asset_entry->m_BlockStore.m_Length = lengths[a];
        asset_entry->m_BlockStore.m_BlockIndex = block_index;
        asset_entry->m_BlockStore.m_StartOffset = start_offsets[a];
    }

    return 1;
}

static void Longtail_FinalizeBuilder(struct Longtail_Builder* builder)
{
    uint32_t block_count = GetSize_TLongtail_Hash(builder->m_BlockHashes);
    for (uint32_t b = 0; b < block_count; ++b)
    {
        builder->m_BlockHashes[b] = builder->m_Storage->Longtail_FinalizeBlock(builder->m_Storage, b);
    }
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

int Longtail_Write(Longtail_WriteStorage* storage, TLongtail_Hash asset_hash, Longtail_InputStream input_stream, void* context, uint64_t length, TLongtail_Hash tag, Longtail_AssetEntry** asset_entry_array)
{
    *asset_entry_array = EnsureCapacity_Longtail_AssetEntry(*asset_entry_array, 16);

    Longtail_AssetEntry* asset_entry = Push_Longtail_AssetEntry(*asset_entry_array);

    if (0 == storage->Longtail_AllocateBlockStorage(storage, tag, length, &asset_entry->m_BlockStore))
    {
        Pop_Longtail_AssetEntry(*asset_entry_array);
        return 0;
    }

    asset_entry->m_AssetHash = asset_hash;

    if (0 == storage->Longtail_WriteBlockData(storage, &asset_entry->m_BlockStore, input_stream, context))
    {
        return 0;
    }

    storage->Longtail_CommitBlockData(storage, &asset_entry->m_BlockStore);
 
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

//        out_sizes[count] = storage->Longtail_PreflightBlocks(storage, 1, &asset_entry->m_BlockStore);
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

    const uint8_t* block_data = storage->Longtail_AqcuireBlockStorage(storage, asset_entry->m_BlockStore.m_BlockIndex);
    output_stream(context, asset_entry->m_BlockStore.m_Length, &block_data[asset_entry->m_BlockStore.m_StartOffset]);
    storage->Longtail_ReleaseBlock(storage, asset_entry->m_BlockStore.m_BlockIndex);

    return 1;
}

#endif LONGTAIL_IMPLEMENTATION
