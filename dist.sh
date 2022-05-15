#!/bin/bash
set -e

. arch_helper.sh

mkdir -p dist

cp build/artifacts/${PLATFORM}-cmd-debug/${PLATFORM}/longtail/debug/longtail dist/longtail_debug
cp build/artifacts/${PLATFORM}-cmd-release/${PLATFORM}/longtail/release/longtail dist/longtail

cp build/artifacts/${PLATFORM}-shared_lib-debug/${PLATFORM}/longtail_dylib/debug/longtail_dylib.so dist/longtail_${PLATFORM}_debug.so
cp build/artifacts/${PLATFORM}-shared_lib-release/${PLATFORM}/longtail_dylib/release/longtail_dylib.so dist/longtail_${PLATFORM}.so

cp build/artifacts/${PLATFORM}-static_lib-debug/${PLATFORM}/longtail_static/debug/liblongtail_static.a dist/liblongtail_${PLATFORM}_debug.a
cp build/artifacts/${PLATFORM}-static_lib-release/${PLATFORM}/longtail_static/release/liblongtail_static.a dist/liblongtail_${PLATFORM}.a

mkdir dist/include
mkdir dist/include/src
mkdir dist/include/lib
mkdir dist/include/lib/archiveblockstore
mkdir dist/include/lib/atomiccancel
mkdir dist/include/lib/bikeshed
mkdir dist/include/lib/blake2
mkdir dist/include/lib/blake3
mkdir dist/include/lib/blockstorestorage
mkdir dist/include/lib/brotli
mkdir dist/include/lib/cacheblockstore
mkdir dist/include/lib/compressblockstore
mkdir dist/include/lib/compressionregistry
mkdir dist/include/lib/filestorage
mkdir dist/include/lib/fsblockstore
mkdir dist/include/lib/hpcdcchunker
mkdir dist/include/lib/lrublockstore
mkdir dist/include/lib/hashregistry
mkdir dist/include/lib/lz4
mkdir dist/include/lib/memstorage
mkdir dist/include/lib/memtracer
mkdir dist/include/lib/meowhash
mkdir dist/include/lib/ratelimitedprogress
mkdir dist/include/lib/shareblockstore
mkdir dist/include/lib/zstd
cp src/*.h dist/include/src
cp lib/archiveblockstore/*.h dist/include/lib/archiveblockstore
cp lib/atomiccancel/*.h dist/include/lib/atomiccancel
cp lib/bikeshed/*.h dist/include/lib/bikeshed
cp lib/blake2/*.h dist/include/lib/blake2
cp lib/blake3/*.h dist/include/lib/blake3
cp lib/blockstorestorage/*.h dist/include/lib/blockstorestorage
cp lib/brotli/*.h dist/include/lib/brotli
cp lib/cacheblockstore/*.h dist/include/lib/cacheblockstore
cp lib/compressblockstore/*.h dist/include/lib/compressblockstore
cp lib/compressionregistry/*.h dist/include/lib/compressionregistry
cp lib/filestorage/*.h dist/include/lib/filestorage
cp lib/fsblockstore/*.h dist/include/lib/fsblockstore
cp lib/hpcdcchunker/*.h dist/include/lib/hpcdcchunker
cp lib/lrublockstore/*.h dist/include/lib/lrublockstore
cp lib/hashregistry/*.h dist/include/lib/hashregistry
cp lib/lz4/*.h dist/include/lib/lz4
cp lib/memstorage/*.h dist/include/lib/memstorage
cp lib/memtracer/*.h dist/include/lib/memtracer
cp lib/meowhash/*.h dist/include/lib/meowhash
cp lib/ratelimitedprogress/*.h dist/include/lib/ratelimitedprogress
cp lib/shareblockstore/*.h dist/include/lib/shareblockstore
cp lib/zstd/*.h dist/include/lib/zstd
