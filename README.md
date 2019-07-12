|Branch      | OSX / Linux / Windows |
|------------|-----------------------|
|master      | [![Build Status](https://travis-ci.org/DanEngelbrecht/longtail.svg?branch=master)](https://travis-ci.org/DanEngelbrecht/longtail?branch=master) |

# longtail
Experimental incremental asset delivery format

# tests
To build builds, call `build/d_clang.sh` for debug and `build/r_clang.sh` for release on OSX/Linux, `build\d_cl.bat` for debug and `build\r_cl.bat` for release on Windows.

Run test with `output/test_debug` for debug and `output/test` for release on OSX/Linux, `output\test_debug.exe` for debug and `output\test.exe` for release on Windows.

# wip
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
