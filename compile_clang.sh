#!/usr/bin/env bash

set +e
mkdir -p build

OPT=-O3
#DISASSEMBLY='-S -masm=intel'
ASAN=""
#CXXFLAGS="$CXXFLAGS -Wall -Weverything -pedantic -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"
ARCH=-m64

SRC=
TEST_SRC="test/main.cpp test/test.cpp"
THIRDPARTY_SRC=third-party/nadir/src/nadir.cpp third-party/lizard/lib/*.c third-party/lizard/lib/entropy/*.c third-party/lizard/lib/xxhash/*.c third-party/trove/src/trove.cpp

clang++ -o ./build/test $OPT $DISASSEMBLY $ARCH -std=c++14 $CXXFLAGS $ASAN -Isrc $SRC $TEST_SRC $THIRDPARTY_SRC -pthread
