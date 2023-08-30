|Branch      | Linux / Windows / MacOS |
|------------|-------|
|master      | [![Build Status](https://github.com/goalsgame/longtail/workflows/Build%20Master/badge.svg)](https://github.com/goalsgame/longtail/workflows/Build%20Master/badge.svg) |

# longtail (osx-arm64 compatible)
### Known issues about osx-arm64 support
 - Blake2 and meowhash HashAPIs were removed since they don't support arm64 architectures
 - Blake3's hashmany function was removed as it was causing compilation issues
 - Had to add supprot for arm64 on bikeshed's CPU Yield directive
 - Unit tests were commented - will revisit this later
   - Have to remove all tests related to meowhash and Blake2

### Original description
Incremental asset delivery format - closely related to the casync project by Lennart Poettering (https://github.com/systemd/casync). When I started tinkering with this I did not know of that project but has since learned from it but choosen different approaches to a few things. If casync does what you need there is no point in diving into this besides curiousity. If there are aspects of casync that does not work for you (you need in-place updating of folders, or you need all the performance using threading) then it might be interesting.

Discord chat Discord chat at https://discord.com/invite/4VmZK9QBnv

# Current state
Stable and have been tested on large data sets. Used by the go command line front end https://github.com/DanEngelbrecht/golongtail.git and the C# LongtailLib https://github.com/DanEngelbrecht/LongtailLib.git both of which are used on a daily basis in CI system and an internal Windows launcher.

It is *very* fast though, most functions that takes time are very multithreaded and fairly efficient and care has been taken to handle really large files (such as multi-gigabyte PAK files for games) efficiently.

It is a bit undocumented still.

# Cloning
git clone https://github.com/DanEngelbrecht/longtail.git

# Platforms
The target platforms are Windows, Linux and MacOS and it *should* build and run on all of them with MacOS being the least tested.

# Tests
To build unit tests, cd to `test`, call `./build.sh` for debug and `./build.sh release` for release on OSX/Linux, `.\build.bat` for debug and `.\build.bat release` for release on Windows.

Run test with `output/test_debug` for debug and `output/test` for release on OSX/Linux, `output\test_debug.exe` for debug and `output\test.exe` for release on Windows.

# Command line tool
The preferred way of testing with the command line is to use https://github.com/DanEngelbrecht/golongtail.git which provides a nicer Go front end to longtail.

You can build the C command line but it is not as maintained and up to date.

To build the command line tool, cd to `cmd`, call `./build.sh` for debug and `../build.sh release` for release on OSX/Linux, `.\build.bat` for debug and `.\build.bat release` for release on Windows.

Run the command line tool with `output/longtail_debug` for debug and `output/longtail` for release on OSX/Linux, `output\longtail_debug.exe` for debug and `output\longtail.exe` for release on Windows.

# Dynamic and Static library
There are targets for dynamic libaries (.so and .dll) in `shared_lib` and static libraries (.a) in `static_lib`. Call `./build.sh` for debug and `./build.sh release` for release on OSX/Linux, `.\build.bat` for debug and `.\build.bat release` for release on Windows.

# Concepts

## "Zero-parse" data formats
The format for the data is stored in a "zero-parse" format - as long as the CPU architecture is little-endian, writing or reading one of the data formats - VersionIndex, StoreIndex or StoredBlock is just a matter of reading it into a chunk of memory and can be used in place. There are no pointers or complex data types (unless you count compression of the StoredBlock chunk data).

## Abstracted platform
The core of the library is in the `src` folder and it defines a set of APIs that it need to operate. Each API is a structure with a number of C style function callbacks.

* HashAPI - To hash a block of data
* StorageAPI - To read/write files to a storage medium
* CompressionAPI - To compress/decompress blocks of data
* JobAPI - To execute tasks in paralell (with dependencies)

It also has hooks for ASSERTs and logging.

Besides a very limited number of standard C library includes it also depends on the excellent `stb_ds.h` by Sean Barrett (http://nothings.org/stb_ds) for dynamic arrays and hash maps.

Although the core does not have any platform dependent code there are pre-built helpers in the `lib` folder that implements a cross-platform base library which is used by various api implementations.

Currently there are:

### HashAPI
* BLAKE3 - by BLAKE3 team https://github.com/BLAKE3-team/BLAKE3

### StorageAPI
* In-memory storage - used for test etc
* Native file system storage - basic file io using the platform layer in the `lib` folder

### CompressionAPI
* Brotli - by Google https://github.com/google/brotli
* ZStandard - by Facebook https://github.com/facebook/zstd
* LZ4 - by Cyan4973 https://github.com/lz4/lz4

### JobAPI
* Bikeshed - by Dan Engelbrecht https://github.com/DanEngelbrecht/bikeshed

Longtail also borrows the chunking algorithm used to split up assets into chunks from the casync project by Lennart Poettering (https://github.com/systemd/casync).

## Content Adressable Storage
An asset inside a version is composed by a series of chunk, each chunk is identified by the hash of the content of the chunk. Each asset is identified by a hash-of-hashes of the chunks that composes the asset. Each chunk is in turn stored inside blocks and the block is identified by a hash-of-hashes of the chunks inside the block.

# Data Types

## Version Index
A version index is a collection of assets (or files if you will) indexed into chunks, each asset is associated with a size and zero to many chunks and a content hash which is a combined hash of all the assets individual chunk hashes. Each asset also get a (relative) path and hash of the path for the asset.
The version index in itself *does not* contain any of the data of the assets, it merely points out which chunks (via hashes) it needs to be reconstructed.

A version index is a particular version of a collection of assets, either a complete set of assets or a subset. Version indexes can be combined or updated/overriden.

This is the "manifest" of a part of a game (or other) content- Small, efficient and easy to diff and make patches from, as well as easy to extend or replace with new asset data.

A version index can easily be constructed from a set of files/folders.

## Stored Block
A stored block contains one or more `chunks` of data with minimal meta data such as compress/uncompressed size and the hash of the uncompressed data of each chunk. Then stored block is identified by a combined hash of all the chunk hashes inside the block.

## Store Index
A store index is a collection of Content Blocks indexes. This store index is used via the Version Index to reconstruct assets to a path.

A store index can easily be constructed from a set of content blocks or you can create it based on a Version Index.

## Version Diff
Represents the difference between two Version Indexes - it contains which asset paths was created, modfied or deleted. 

# So, what can it do?
It can scan a folder on the file system, hash it and chunk it up, create content blocks and store index for the data.

It can create a diff between one folder and another, find out which chunks it needs to go between said version and apply the change to a folder. It can write one version to a *new* folder or it can *update* an existing folder.

It is *very* fast, it is significantly faster att all the stages than desync (by Frank Olbricht https://github.com/folbricht/desync) which itself does threading of tasks to speed it up compared to casync. This is when compared on the use cases that *I* care about, it might not be faster in *your* use cases.

Modifying an existing folder is something desync can't (currently) do and is crucial to achive reasonable speed when working with large amount of data (multi-gigabyte folders).

# What can't it do?
The C99 version in this repo has intentionally been restricted to not add any big complicated dependencies but instead provide interfaces where you can extend it, for example - it has two forms of storage - disk and memory, it does not have http, S3, GCS or any other fancy stuff in it, for that kind of storge, use the golang version https://github.com/DanEngelbrecht/golongtail.git.

This has been an active choice, as the library is writting in C99 for ultimate portability some sacrifices had to be done. It has minimal dependencies and no complicated build system but it is written so adding other storage mechanisms or exchanging hashing or other parts are reasonably easy. 
