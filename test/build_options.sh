#!/usr/bin/env bash

export TARGET=test
export SRC="../src/longtail.c"
export TEST_SRC="impl_bind.c main.cpp test.cpp"
export THIRDPARTY_SRC="nadir/src/nadir.cpp lizard/lib/*.c lizard/lib/entropy/*.c lizard/lib/xxhash/*.c trove/src/trove.cpp"
export CXXFLAGS="$CXXFLAGS -pthread"
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DBIKESHED_ASSERTS -DLONGTAIL_VERBOSE_LOGS -DLONGTAIL_ASSERTS"
