#!/usr/bin/env bash

export TARGET=test
export SRC="$BASE_DIR/src/longtail.c $BASE_DIR/lib/longtail_lib.c $BASE_DIR/lib/longtail_platform.c"
export TEST_SRC="$BASE_DIR/test/impl_bind.c $BASE_DIR/test/main.cpp $BASE_DIR/test/test.cpp"
export THIRDPARTY_SRC="$THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c"
export CXXFLAGS="$CXXFLAGS -pthread"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_VERBOSE_LOGS -DLONGTAIL_ASSERTS"
