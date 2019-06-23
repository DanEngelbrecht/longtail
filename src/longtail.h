#pragma once

#include <stdint.h>
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"

struct Longtail;
struct Longtail_AssetEntry;
struct Longtail_BlockEntry;
struct Longtail_ReadStorage;

typedef int (*Longtail_OutputStream)(void* context, uint32_t byte_count, const uint8_t* data);
typedef int (*Longtail_InputStream)(void* context, uint32_t byte_count, uint8_t* data);

struct Longtail_ReadStorage
{
    // Return required memory size?
    uint64_t (*Longtail_PreflightBlocks)(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockEntry* blocks);
    struct Longtail_Block* (*Longtail_AqcuireBlock)(struct Longtail_ReadStorage* storage, meow_u128 block_hash);
    void (*Longtail_ReleaseBlock)(struct Longtail_ReadStorage* storage, meow_u128 block_hash);
};

struct Longtail_WriteStorage
{
    // TODO: We should be able to handle partially filled blocks
    int (*Longtail_StoreBlock)(struct Longtail_WriteStorage* storage, meow_u128 block_hash, Longtail_Block* block);
    struct Longtail_Block* (*Longtail_AllocateBlock)(struct Longtail_WriteStorage* storage, meow_u128 compression_type);
};

int Longtail_Preflight(struct Longtail* longtail, struct Longtail_ReadStorage* storage, uint32_t count, meow_u128* assets, uint64_t* out_sizes);
int Longtail_Read(struct Longtail* longtail, struct Longtail_ReadStorage* storage, meow_u128 asset, Longtail_OutputStream output_stream, void* context);

#ifdef LONGTAIL_IMPLEMENTATION

struct Longtail_AssetEntry
{
    meow_u128 m_AssetHash; // Path + version
    uint32_t m_FirstBlock; // First block where it is located in m_BlockEntries array
    uint32_t m_BlockCount; // Number of blocks this resource spans
};

struct Longtail_BlockEntry
{
    meow_u128 m_BlockHash;
    uint32_t m_StartOffset; // Compressed or raw?
    uint32_t m_Length;  // Compressed or raw?
};

#define LONGTAIL_BLOCK_SIZE (32768 - (sizeof(meow_u128) + sizeof(uint32_t)))
// Blocksize is the size of the block compressed?

struct Longtail_Block
{
    meow_u128 m_CompressionType;
    uint32_t m_RawSize; // ??
    uint8_t m_Data[LONGTAIL_BLOCK_SIZE];
};

struct Longtail
{
    uint64_t asset_entry_count;
    uint64_t block_entry_count;
    struct Longtail_AssetEntry* asset_entries;
    struct Longtail_BlockEntry* block_entries;
};

struct Longtail* Longtail_Open(uint64_t asset_entry_count, struct Longtail_AssetEntry* asset_entries, uint64_t block_entry_count, struct Longtail_BlockEntry* block_entries)
{
    return 0;
}

static uint32_t Longtail_GetFirstBlock_private(struct Longtail* longtail, meow_u128 asset)
{
    // Could do with a faster find algo
    uint32_t asset_index = 0;
    while (asset_index < longtail->asset_entry_count)
    {
        if (MeowHashesAreEqual(longtail->asset_entries[asset_index].m_AssetHash, asset))
        {
            break;
        }
        ++asset_index;
    }
    if (asset_index != longtail->asset_entry_count)
    {
        return 0;
    }
    return 0xffffffffu;
}

int Longtail_Preflight(struct Longtail* longtail, struct Longtail_ReadStorage* storage, uint32_t count, meow_u128* assets, uint64_t* out_sizes)
{
    while(count--)
    {
        uint32_t asset_index = Longtail_GetFirstBlock_private(longtail, assets[count]);
        if (asset_index == 0xffffffffu)
        {
            return 0;
        }
        struct Longtail_AssetEntry* asset_entry = &longtail->asset_entries[asset_index];
        struct Longtail_BlockEntry* block_entry = &longtail->block_entries[asset_entry->m_FirstBlock];

        out_sizes[count] = storage->Longtail_PreflightBlocks(storage, asset_entry->m_BlockCount, block_entry);
    }
    return 1;
}

int Longtail_Read(struct Longtail* longtail, struct Longtail_ReadStorage* storage, meow_u128 asset, Longtail_OutputStream output_stream, void* context)
{
    uint32_t asset_index = Longtail_GetFirstBlock_private(longtail, asset);
    if (asset_index == 0xffffffffu)
    {
        return 0;
    }

    struct Longtail_AssetEntry* asset_entry = &longtail->asset_entries[asset_index];
    struct Longtail_BlockEntry* entry = &longtail->block_entries[asset_entry->m_FirstBlock];

    uint32_t entry_count = asset_entry->m_BlockCount;
    while (entry_count--)
    {
        struct Longtail_Block* block = storage->Longtail_AqcuireBlock(storage, entry->m_BlockHash);
        output_stream(context, entry->m_Length, &block->m_Data[entry->m_StartOffset]);
        storage->Longtail_ReleaseBlock(storage, entry->m_BlockHash);
        ++entry;
    }
    return 1;
}

#endif LONGTAIL_IMPLEMENTATION
