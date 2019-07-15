#!/usr/bin/env bash

RELEASE_MODE="$1"

if [ "$RELEASE_MODE" = "release" ]; then
    export OPT=-O3
    #DISASSEMBLY='-S -masm=intel'
    export ASAN=""
    export CXXFLAGS="-Wno-deprecated-register -Wno-deprecated"
    export ARCH="-m64 -maes"
    export OUTPUT=test

    . ./build_options.sh
else
    export OPT="-g"
    export ASAN="-fsanitize=address -fno-omit-frame-pointer"
    #CXXFLAGS="-Wall -Weverything -pedantic -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"
    export CXXFLAGS="-Wno-deprecated-register -Wno-deprecated"
    export ARCH="-m64 -maes"
    export OUTPUT=test_debug

    . ./build_options.sh

    export CXXFLAGS="$CXXFLAGS $CXXFLAGS_DEBUG"
fi

mkdir -p build

clang++ -o ./build/$OUTPUT $OPT $DISASSEMBLY $ARCH -std=c++14 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC $SRC $TEST_SRC
