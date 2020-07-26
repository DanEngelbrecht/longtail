#!/bin/bash

LIB_SRC="${BASE_DIR}lib/filestorage/*.c"
LIB_THIRDPARTY_SRC="${BASE_DIR}src/ext/*.c"

ATOMICCANCEL_SRC="${BASE_DIR}lib/atomiccancel/*.c"

BLOCKSTORESTORAGE_SRC="${BASE_DIR}lib/blockstorestorage/*.c"

COMPRESSBLOCKSTORE_SRC="${BASE_DIR}lib/compressblockstore/*.c"

CACHEBLOCKSTORE_SRC="${BASE_DIR}lib/cacheblockstore/*.c"

FILESTORAGE_SRC="${BASE_DIR}lib/*.c"

FSBLOCKSTORAGE_SRC="${BASE_DIR}lib/fsblockstore/*.c"

LRUBLOCKSTORE_SRC="${BASE_DIR}lib/lrublockstore/*.c"

MEMSTORAGE_SRC="${BASE_DIR}lib/memstorage/*.c"

MEOWHASH_SRC="${BASE_DIR}lib/meowhash/*.c"

COMPRESSION_REGISTRY_SRC="${BASE_DIR}lib/compressionregistry/*.c"

HASH_REGISTRY_SRC="${BASE_DIR}lib/hashregistry/*.c"

RETAININGBLOCKSTORE_SRC="${BASE_DIR}lib/retainingblockstore/*.c"

SHAREBLOCKSTORE_SRC="${BASE_DIR}lib/shareblockstore/*.c"

BIKESHED_SRC="${BASE_DIR}lib/bikeshed/*.c"

BLAKE2_SRC="${BASE_DIR}lib/blake2/*.c"
BLAKE2_THIRDPARTY_SRC="${BASE_DIR}lib/blake2/ext/*.c"

BLAKE3_SRC="${BASE_DIR}lib/blake3/*.c"
BLAKE3_THIRDPARTY_SRC="${BASE_DIR}lib/blake3/ext/blake3.c ${BASE_DIR}lib/blake3/ext/blake3_dispatch.c ${BASE_DIR}lib/blake3/ext/blake3_portable.c ${BASE_DIR}lib/blake3/ext/blake3_sse41.c"
BLAKE3_THIRDPARTY_AVX2="${BASE_DIR}lib/blake3/ext/blake3_avx2.c"
BLAKE3_THIRDPARTY_AVX512="${BASE_DIR}lib/blake3/ext/blake3_avx512.c"

LZ4_SRC="${BASE_DIR}lib/lz4/*.c"
LZ4_THIRDPARTY_SRC="${BASE_DIR}lib/lz4/ext/*.c"

BROTLI_SRC="${BASE_DIR}lib/brotli/*.c"
BROTLI_THIRDPARTY_SRC="${BASE_DIR}lib/brotli/ext/common/*.c ${BASE_DIR}lib/brotli/ext/dec/*.c ${BASE_DIR}lib/brotli/ext/enc/*.c ${BASE_DIR}lib/brotli/ext/fuzz/*.c"

ZSTD_SRC="${BASE_DIR}lib/zstd/*.c"
ZSTD_THIRDPARTY_SRC="${BASE_DIR}lib/zstd/ext/common/*.c ${BASE_DIR}lib/zstd/ext/compress/*.c ${BASE_DIR}lib/zstd/ext/decompress/*.c"

export SRC="${BASE_DIR}src/*.c $LIB_SRC $ATOMICCANCEL_SRC $BLOCKSTORESTORAGE_SRC $COMPRESSBLOCKSTORE_SRC $CACHEBLOCKSTORE_SRC $RETAININGBLOCKSTORE_SRC $SHAREBLOCKSTORE_SRC $FILESTORAGE_SRC $FSBLOCKSTORAGE_SRC $LRUBLOCKSTORE_SRC $MEMSTORAGE_SRC $MEOWHASH_SRC $COMPRESSION_REGISTRY_SRC $HASH_REGISTRY_SRC $BIKESHED_SRC $BLAKE2_SRC $BLAKE3_SRC $LZ4_SRC $BROTLI_SRC $ZSTD_SRC"
export THIRDPARTY_SRC="$LIB_THIRDPARTY_SRC $BLAKE2_THIRDPARTY_SRC $BLAKE3_THIRDPARTY_SRC $LZ4_THIRDPARTY_SRC $BROTLI_THIRDPARTY_SRC $ZSTD_THIRDPARTY_SRC"
export THIRDPARTY_SRC_SSE42=""
export THIRDPARTY_SRC_AVX2="$BLAKE3_THIRDPARTY_AVX2"
export THIRDPARTY_SRC_AVX512="$BLAKE3_THIRDPARTY_AVX512"
