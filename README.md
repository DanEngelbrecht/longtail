|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://travis-ci.org/DanEngelbrecht/longtail.svg?branch=master)](https://travis-ci.org/DanEngelbrecht/longtail?branch=master) |

# longtail
Experimental incremental asset delivery format

# Cloning
git clone --recurse-submodules https://github.com/DanEngelbrecht/longtail.git

# tests
To build builds, call `build/d_clang.sh` for debug and `build/r_clang.sh` for release on OSX/Linux, `build\d_cl.bat` for debug and `build\r_cl.bat` for release on Windows.

Run test with `output/test_debug` for debug and `output/test` for release on OSX/Linux, `output\test_debug.exe` for debug and `output\test.exe` for release on Windows.

# Concepts

## Version Index
Lookup table from asset path hash to asset content hash. A table of this can represent a full set of a games content for a specific point in version history. It can be a set of a patch or a subset of a piece of asset content.

This is the "manifest" of the games content. Small, efficient and easy to diff to make patches and easy to extend with new asset data.

## Block
A chunk of data containing one or more assets content. Name of block is stringified version of the blocks contents hash. Easy to validate for errors and makes naming blocks unique.

### Block content
PEach block contains information on the assets inside it. The block has asset content hash and length for each asset inside the block as a list at the end of the block.

| DATA | DATA LENGTH |
|-|-|
| AssetA | variable |
| AssetB | variable |
| AssetA Content Hash | 64 bit |
| AssetA Content Length | 64 bit |  
| AssetB Content Hash | 64 bit |
| AssetB Content Length | 64 bit |
| Asset Count | 64 bit |

The structure is ZIP-file like, this makes it easy to stream out asset content to a block and then finishing it up by adding the content index at the end.
To read it, jump to end of block, read how many assets there are, back up to index start (using asset count) and calculate the asset offsets to build the content index.

Each block is compressed separately so it is a good idea to bunch smaller assets together to reduce size and IO operations.

## Content Index
Lookup tabke from sset content hash to block, offset in block and length. This is updated as new blocks are added and old blocks are purged.

### Duplicates
If an asset is present in more than one block, how do we represent that in the Content Index? It really does not matter to which block it points to, it is the content that matters so duplicates is only wasting space.

*How to resolve purging of blocks the most efficient way so we keep the blocks with the least amount of obsolete data?*

# Asset requests

## Known, locally present asset
Check Content Index for the block needed, open block and read using offset and length from the Content Index.

## Known, not locally present asset
Check Content Index for the block needed, request the non-local block and store it locally. Scan the block for assets and insert them in the Content Index.

## Unknown asset
Request for block containing asset content hash, request the block and store it locally. Scan the block for assets and insert them in the Content Index.

# WIP
MHash . https://github.com/cmuratori/meow_hash

MHash for block identiy, also signifies content
Resource identifier MHash - both path and version?
Should be easy to create an incremental diff
A HTTP (or similar) service that serves up content blocks based on MHash
Use MHash to validate packages to avoid poisioning
Multiple block types with different compression types - don't zip optimally compressed audio fex
Dynamic block creation?
Local block cache - MHash based

Look at unreal storage API - client implementation must be easily pluggable for this!

Only download initial content index

Content indexes can reference other content indexes? Do have minimal download first and then look up bigger indexes?

Server should be able to:
    Serve MHash blocks (in batch!)
    Serve Context Indexes
    Consume MHash blocks
    ? Possibly hint about likely soon needed MHash blocks

Tools should be able to:
    Create MHash blocks
    Create Context Indexes
    Diff Context Indexes and produce list of different MHash blocks
    Group types of data that "belongs together"
    Group types of data that needs same type of compression

Wishlist:
    Blocks should be of a fixed size
    Assets should be able to span multiple blocks
    Multiple Assets allowed in same block
    Go from asset path (and version) to block(s) MHash, offset(s) in block.
    Random access of files?
    Completely async api? Start with sync

struct Entry
{
    MHash m_AssetHash; // Path + version
    uint32_t m_FirstBlock; // First block where it is located in m_BlockEntries array
    uint32_t m_BlockCount; // Number of blocks this resource spans
};

Entry m_Entries[];
BlockEntry m_BlockEntries[];

struct BlockEntry
{
    MHash m_BlockHash;
    uint32_t m_StartOffset;
    uint32_t m_DataLengthInBlock;
};

struct Block
{
    uint8_t m_CompressedData[BLOCK_SIZE];
};
