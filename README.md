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
Each block contains information on the assets inside it. The block has asset content hash and length for each asset inside the block as a list at the end of the block.

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
Lookup table from asset content hash to block, offset in block and length. This is updated as new blocks are added and old blocks are purged.

### Duplicates
If an asset is present in more than one block, how do we represent that in the Content Index? It really does not matter to which block it points to, it is the content that matters so duplicates is only wasting space.

*How to resolve purging of blocks the most efficient way so we keep the blocks with the least amount of obsolete data?*

# Asset requests

## Known, locally present asset
Check Content Index for the block needed, open block and read using offset and length from the Content Index.

## Known, not locally present asset
Check Content Index for the block needed, request the non-local block and store it locally. Scan the block for assets and insert them in the Content Index, open block and read using offset and length from the Content Index.

## Unknown asset
Request block hash containing asset content hash, request the block and store it locally. Scan the block for assets and insert them in the Content Index, open block and read using offset and length from the Content Index.

# RSync-style application
lvi = longtail version index
lci = longtail content index
lbl = longtail block list

longtail build-version-index <asset-path> >local_version.lvi
longtail build-content-index <content-path> >local_content.lci
longtail update-content-index local_version.lvi <asset-path> local_content.lci <content-path>
wget <remote-content-index> >remote_content.lci
longtail build-version-content-index <asset-path> local_content.lci >upload_content.lbl
wput <remote-content-path> <content-path> <upload_content.lbl
wput <remote-path> local_content.lci

??? Better granularity? Should we diff against assets?

## Local
- Assets (implicit paths) - can be built on the fly from Version Index and Content Cache. Any missing content needs to be fetched from remote
- Version Index (possibly cached)
  - path hash[] -> {parent path hash, name offset, asset hash}[]
    names[]

### Content Cache
- Content Index (contains no path information)
  - asset hash[] -> {block hash, offset, length}
- Blocks (contains no path information)
  - asset data[]
  - {asset hash, block offset, size}[]
  - asset count

## Content Cache
The Content cache can either be a dedicated remote cache or a local folder of a destination target. To build the local Assets you also need a Version Index.

- Content Index (contains no path information)
  - asset hash[] -> {block hash, offset, length}
- Blocks (contains no path information)
  - asset data[]
  - {asset hash, block offset, size}[]
  - asset count



## Remote
- Assets (implicit paths) - can be built on the fly from Version Index and Content Cache. Any missing content needs to be fetched from remote
- Version Index (possibly cached)
  - path hash[] -> {parent path hash, name offset, asset hash}[]
    names[]




Scan source folder
    Build Local Version Index
        Convert paths -> path hash
        Calculate hash of each file
Bundle source assets into blocks
    Customizable association algorithm
    Custimizable compression algorithm
    Build Blocks
        Cache them locally
    Update Local Content Index

Get target Remote Content Index
Copy missing blocks from Local Content Index to Remote Content Index
Update Remote Content Index
Copy Local Version Index to Remote Version Index
Build Version Index
    Convert paths -> path hash
    Calculate hash of each file
Remove unwanted assets from Remote Folder
    Remove any files in Remote Content Index not in Local Content Index
        Diff Remote Version Index with Remote Version Index
        Remove assets
        Update Remote Version Index
Add missing assets to Remote Folder
    Add assets in Local Version Index not in Remote Version Index
        Diff Local Version Index with Remote Version Index
        Add assets from Remote Content Index
            Get asset path hash from Local Version Index
            Convert asset path hash to asset path
            Look up block in Content Index
            Copy asset from Remote Content Index to destination folder

As new assets are added they move into "new" blocks but untouched assets remains in their old blocks, this means fast-changing assets will be kept in small delta like packets while slow-changing assets will not be transmitted. It *can* lead to lots of small fragmented blocks if only a few small assets will be changed (and never the same assets). This can be remedied with re-blocking the Content Index cache by bundling up existing blocks.

A Content Index Cache can easily be cleaned up using the Content Index, any block that is not pointed to by the Content Index may be deleted.

The Local Index and the Remote Index does not *have* to be the same layout - if they differ it will only have the risk of transmitting redundant data.

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
