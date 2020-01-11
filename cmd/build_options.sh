#!/bin/bash

export TARGET=longtail
export SRC="$BASE_DIR/src/*.c $BASE_DIR/cmd/main.cpp $BASE_DIR/lib/*.c $BASE_DIR/lib/bikeshed/*.c $BASE_DIR/lib/blake2/*.c $BASE_DIR/lib/filestorage/*.c $BASE_DIR/lib/lizard/*.c $BASE_DIR/lib/memstorage/*.c $BASE_DIR/lib/meowhash/*.c $BASE_DIR/lib/xxhash/*.c"
export TEST_SRC=""
export THIRDPARTY_SRC="$THIRDPARTY_DIR/dummy.c"
#"$THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c $THIRDPARTY_DIR/blake2/sse/blake2s.c"
export CXXFLAGS="$CXXFLAGS -pthread"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS"
