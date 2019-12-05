#!/usr/bin/env bash

export TARGET=longtail
export SRC="$BASE_DIR/src/longtail.c $BASE_DIR/cmd/*.cpp $BASE_DIR/lib/longtail_lib.cpp"
export TEST_SRC=""
export THIRDPARTY_SRC="$THIRDPARTY_DIR/nadir/src/nadir.cpp $THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c $THIRDPARTY_DIR/trove/src/trove.cpp"
export CXXFLAGS="$CXXFLAGS -pthread -DLONGTAIL_VERBOSE_LOGS"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_VERBOSE_LOGS"
