#!/bin/bash
set -e

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

export BASE_CXXFLAGS="-Wno-sign-conversion -Wno-missing-prototypes -Wno-cast-align -Wno-unused-function -Wno-deprecated-register -Wno-deprecated -Wno-c++98-compat-pedantic -Wno-unused-parameter -Wno-unused-template -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"
# -pedantic
# -Wno-atomic-implicit-seq-cst
# -Wno-extra-semi-stmt
# -Wno-implicit-int-conversion
if [ "$RELEASE_MODE" = "release" ]; then
    export OPT=-O3
    #DISASSEMBLY='-S -masm=intel'
    export ASAN=""
    export ARCH="-m64 -maes -mssse3 -msse4.1"

    . ./build_options.sh
    export OUTPUT=$TARGET
    export THIRD_PARTY_LIB="$TARGET-third-party.a"
    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS"
else
    export OPT="-g"
    export ASAN="-fsanitize=address -fno-omit-frame-pointer"
    BASE_CXXFLAGS="$BASE_CXXFLAGS" # -Wall -Weverything"
    export ARCH="-m64 -maes -mssse3 -msse4.1"

    . ./build_options.sh
    export OUTPUT=${TARGET}_debug
    export THIRD_PARTY_LIB="$TARGET-third-party-debug.a"

    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS_DEBUG"
fi

if [ $TARGET_TYPE == "SHAREDLIB" ]; then
    # Keep third-party lib separate from other builds
    # Disable ASAN since it would force user of .so to enable ASAN
    export THIRD_PARTY_LIB="lib${THIRD_PARTY_LIB}"
    export ASAN=""
    export OPT="$OPT -fPIC -fvisibility=hidden"
fi

if [ ! -e "$BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB" ]; then
    BUILD_THIRD_PARTY="build-third-party"
fi

mkdir -p $BASE_DIR/build/third-party-$RELEASE_MODE

if [ "$BUILD_THIRD_PARTY" = "build-third-party" ]; then
    echo "Compiling third party dependencies to library" $THIRD_PARTY_LIB
    cd $BASE_DIR/build/third-party-$RELEASE_MODE
    rm -rf $BASE_DIR/build/third-party-$RELEASE_MODE/*.o
    clang++ -c $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC
    ar rc $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB *.o
    cd $BASE_DIR
fi

if [ $TARGET_TYPE == "EXECUTABLE" ]; then
    echo Building $OUTPUT
    clang++ -o $BASE_DIR/build/$OUTPUT $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
fi

if [ $TARGET_TYPE == "SHAREDLIB" ]; then
    echo Building lib${OUTPUT}.so
    clang++ -shared -o $BASE_DIR/build/lib${OUTPUT}.so $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
fi

#if [ "$TARGET_MODE" = "lib" ]; then
#    mkdir -p $BASE_DIR/build/lib-$RELEASE_MODE
#    rm -rf $BASE_DIR/build/lib-$RELEASE_MODE/*.o
#    cd $BASE_DIR/build/lib-$RELEASE_MODE
#    clang++ -c $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC
#    echo $BASE_DIR/build/$TARGET
#    ar rc $BASE_DIR/build/$TARGET *.o $BASE_DIR/build/third-party-$RELEASE_MODE/*.o
#    cd $BASE_DIR
#else
#    echo Building $OUTPUT
#    clang++ -o $BASE_DIR/build/$OUTPUT $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC $BASE_DIR/build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
#fi
