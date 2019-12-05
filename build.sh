#!/usr/bin/env bash

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export THIRDPARTY_DIR=${BASE_DIR}/third-party/

if [ "$1" = "build-third-party" ] || [ "$2" = "build-third-party" ] || [ "$3" = "build-third-party" ] ; then
    BUILD_THIRD_PARTY="build-third-party"
else
    BUILD_THIRD_PARTY=""
fi

if [ "$1" = "release" ] || [ "$2" = "release" ] || [ "$3" = "release" ] ; then
    RELEASE_MODE="release"
else
    RELEASE_MODE="debug"
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

if [ ! -e "$BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB" ]; then
    BUILD_THIRD_PARTY="build-third-party"
fi

mkdir -p $BASE_DIR/build/third-party-$RELEASE_MODE

if [ "$BUILD_THIRD_PARTY" = "build-third-party" ]; then
    echo "Compiling third party dependencies to library" $THIRD_PARTY_LIB
    pushd $BASE_DIR/build/third-party-$RELEASE_MODE
    rm -rf $BASE_DIR/build/third-party-$RELEASE_MODE/*.o
    clang++ -c $OPT $DISASSEMBLY $ARCH -stdlib=libc++ -std=c++14 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC
    ar rc $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB *.o
    popd
fi

if [ "$TARGET_MODE" = "lib" ]; then
    mkdir -p $BASE_DIR/build/lib-$RELEASE_MODE
    rm -rf $BASE_DIR/build/lib-$RELEASE_MODE/*.o
    pushd $BASE_DIR/build/lib-$RELEASE_MODE
    clang++ -c $OPT $DISASSEMBLY $ARCH -stdlib=libc++ -std=c++14 $CXXFLAGS $ASAN -Isrc $SRC $TEST_SRC
    echo $BASE_DIR/build/$TARGET
    ar rc $BASE_DIR/build/$TARGET *.o $BASE_DIR/build/third-party-$RELEASE_MODE/*.o
    popd
else
    echo Building $OUTPUT
    clang++ -o $BASE_DIR/build/$OUTPUT $OPT $DISASSEMBLY $ARCH -stdlib=libc++ -std=c++14 $CXXFLAGS $ASAN -Isrc $SRC $TEST_SRC $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
fi
