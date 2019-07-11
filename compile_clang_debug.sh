#!/usr/bin/env bash

set +e
mkdir -p build

#OPT=-O3
OPT="-g -O1"
#DISASSEMBLY='-S -masm=intel'
ASAN="-fsanitize=address -fno-omit-frame-pointer"
#CXXFLAGS="$CXXFLAGS -Wall -Weverything -pedantic -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"
CXXFLAGS="$CXXFLAGS -Wno-deprecated-register -Wno-deprecated"
ARCH="-m64 -maes"

SRC=
TEST_SRC="test/main.cpp test/test.cpp"
THIRDPARTY_SRC="third-party/nadir/src/nadir.cpp third-party/lizard/lib/*.c third-party/lizard/lib/entropy/*.c third-party/lizard/lib/xxhash/*.c third-party/trove/src/trove.cpp"

clang++ -o ./build/test_debug $OPT $DISASSEMBLY $ARCH -std=c++14 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC $SRC $TEST_SRC -pthread
