#!/bin/bash

export TARGET=test
export SRC="$BASE_DIR/src/*.c $BASE_DIR/src/ext/*.c $BASE_DIR/lib/*.c $BASE_DIR/lib/bikeshed/*.c $BASE_DIR/lib/blake2/*.c $BASE_DIR/lib/filestorage/*.c $BASE_DIR/lib/lizard/*.c $BASE_DIR/lib/memstorage/*.c $BASE_DIR/lib/meowhash/*.c $BASE_DIR/lib/xxhash/*.c"
export TEST_SRC="$BASE_DIR/test/impl_bind.c $BASE_DIR/test/main.cpp $BASE_DIR/test/test.cpp"
export THIRDPARTY_SRC="$BASE_DIR/src/ext/*.c $BASE_DIR/src/ext/*.c %BASE_DIR%lib/brotli/ext/common/*.c %BASE_DIR%lib/brotli/ext/dec/*.c %BASE_DIR%lib/brotli/ext/enc/*.c %BASE_DIR%lib/brotli/ext/fuzz/*.c"
#"$THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c $THIRDPARTY_DIR/blake2/sse/blake2s.c"
export CXXFLAGS="$CXXFLAGS -pthread -U_WIN32 -DLONGTAIL_LOG_LEVEL=5"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_LOG_LEVEL=3 -DLONGTAIL_ASSERTS"
