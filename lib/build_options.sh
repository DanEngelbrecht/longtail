#!/usr/bin/env bash

export TARGET=longtail_lib
export TARGET_MODE="lib"
export SRC="$BASE_DIR/src/longtail.c $BASE_DIR/lib/longtail_lib.c $BASE_DIR/lib/longtail_platform.c"
export TEST_SRC=""
export THIRDPARTY_SRC="$THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c"
export CXXFLAGS="$CXXFLAGS -pthread -DLONGTAIL_VERBOSE_LOGS"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_VERBOSE_LOGS"
