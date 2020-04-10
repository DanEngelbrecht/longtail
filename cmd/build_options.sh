#!/bin/bash

export TARGET=longtail
export TARGET_TYPE=EXECUTABLE

LIB_SRC="$BASE_DIR/lib/filestorage/*.c"
LIB_THIRDPARTY_SRC="$BASE_DIR/src/ext/*.c"

CACHEBLOCKSTORE_SRC="$BASE_DIR/lib/cacheblockstore/*.c"

COMPRESSBLOCKSTORE_SRC="$BASE_DIR/lib/compressblockstore/*.c"

FILESTORAGE_SRC="$BASE_DIR/lib/*.c"

FSBLOCKSTORAGE_SRC="$BASE_DIR/lib/fsblockstore/*.c"

MEMSTORAGE_SRC="$BASE_DIR/lib/memstorage/*.c"

MEOWHASH_SRC="$BASE_DIR/lib/meowhash/*.c"

FULL_COMPRESSION_REGISTRY_SRC="$BASE_DIR/lib/compressionregistry/*.c"

BIKESHED_SRC="$BASE_DIR/lib/bikeshed/*.c"

BLAKE2_SRC="$BASE_DIR/lib/blake2/*.c"
BLAKE2_THIRDPARTY_SRC="$BASE_DIR/lib/blake2/ext/*.c"

BLAKE3_SRC="$BASE_DIR/lib/blake3/*.c"
BLAKE3_THIRDPARTY_SRC="$BASE_DIR/lib/blake3/ext/*.c"

LZ4_SRC="$BASE_DIR/lib/lz4/*.c"
LZ4_THIRDPARTY_SRC="$BASE_DIR/lib/lz4/ext/*.c"

BROTLI_SRC="$BASE_DIR/lib/brotli/*.c"
BROTLI_THIRDPARTY_SRC="$BASE_DIR/lib/brotli/ext/common/*.c $BASE_DIR/lib/brotli/ext/dec/*.c $BASE_DIR/lib/brotli/ext/enc/*.c $BASE_DIR/lib/brotli/ext/fuzz/*.c"

ZSTD_SRC="${BASE_DIR}/lib/zstd/*.c"
ZSTD_THIRDPARTY_SRC="${BASE_DIR}/lib/zstd/ext/common/*.c ${BASE_DIR}/lib/zstd/ext/compress/*.c ${BASE_DIR}/lib/zstd/ext/decompress/*.c"

export SRC="$BASE_DIR/src/*.c $LIB_SRC $CACHEBLOCKSTORE_SRC $COMPRESSBLOCKSTORE_SRC $FILESTORAGE_SRC $FSBLOCKSTORAGE_SRC $MEMSTORAGE_SRC $MEOWHASH_SRC $FULL_COMPRESSION_REGISTRY_SRC $BIKESHED_SRC $BLAKE2_SRC $BLAKE3_SRC $LZ4_SRC $BROTLI_SRC $ZSTD_SRC"
export MAIN_SRC="$BASE_DIR/cmd/main.cpp"
export THIRDPARTY_SRC="$LIB_THIRDPARTY_SRC $BLAKE2_THIRDPARTY_SRC $BLAKE3_THIRDPARTY_SRC $LZ4_THIRDPARTY_SRC $BROTLI_THIRDPARTY_SRC $ZSTD_THIRDPARTY_SRC"
export CXXFLAGS="$CXXFLAGS -pthread -msse4.1 -maes"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -msse4.1 -maes"
