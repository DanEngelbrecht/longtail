#!/bin/bash

export TARGET=test
export SRC="$BASE_DIR/src/*.c $BASE_DIRlib/*.c $BASE_DIRlib/bikeshed/*.c $BASE_DIRlib/blake2/*.c $BASE_DIRlib/filestorage/*.c $BASE_DIRlib/lizard/*.c $BASE_DIRlib/memstorage/*.c $BASE_DIRlib/meowhash/*.c $BASE_DIRlib/xxhash/*.c"
export TEST_SRC="$BASE_DIR/test/impl_bind.c $BASE_DIR/test/main.cpp $BASE_DIR/test/test.cpp"
export THIRDPARTY_SRC="$THIRDPARTY_DIR/dummy.c"
#"$THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c $THIRDPARTY_DIR/blake2/sse/blake2s.c"
export CXXFLAGS="$CXXFLAGS -pthread -U_WIN32 -DLONGTAIL_LOG_LEVEL=5"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_LOG_LEVEL=3 -DLONGTAIL_ASSERTS"
