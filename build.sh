#!/usr/bin/env bash

if [ "$1" = "build-third-party" ]; then
    BUILD_THIRD_PARTY="$1"
    RELEASE_MODE="$2"
else
    BUILD_THIRD_PARTY=""
    RELEASE_MODE="$1"
fi

if [ "$RELEASE_MODE" = "release" ]; then
    export OPT=-O3
    #DISASSEMBLY='-S -masm=intel'
    export ASAN=""
    export CXXFLAGS="-Wno-deprecated-register -Wno-deprecated"
    export ARCH="-m64 -maes -mssse3"

    . ./build_options.sh
    export OUTPUT=$TARGET
    export THIRD_PARTY_LIB="$TARGET-third-party.a"
else
    export OPT="-g"
    export ASAN="-fsanitize=address -fno-omit-frame-pointer"
    #CXXFLAGS="-Wall -Weverything -pedantic -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"
    export CXXFLAGS="-Wno-deprecated-register -Wno-deprecated"
    export ARCH="-m64 -maes -mssse3"

    . ./build_options.sh
    export OUTPUT=${TARGET}_debug
    export THIRD_PARTY_LIB="$TARGET-third-party-debug.a"

    export CXXFLAGS="$CXXFLAGS $CXXFLAGS_DEBUG"
fi

if [ ! -e "../build/$THIRD_PARTY_LIB" ]; then
    BUILD_THIRD_PARTY="build-third-party"
fi

mkdir -p ../build

if [ "$BUILD_THIRD_PARTY" = "build-third-party" ]; then
    pushd ../third-party
    clang++ -c $OPT $DISASSEMBLY $ARCH -stdlib=libc++ -std=c++14 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC
    popd
    ar rc ../build/$THIRD_PARTY_LIB ../third-party/*.o
    rm ../third-party/*.o
fi

clang++ -o ../build/$OUTPUT $OPT $DISASSEMBLY $ARCH -stdlib=libc++ -std=c++14 $CXXFLAGS $ASAN -Isrc $SRC $TEST_SRC ../build/$THIRD_PARTY_LIB
