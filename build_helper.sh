#!/bin/bash
set -e

export BUILD_DIR=$1
export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"

if [ "$2" = "build-third-party" ] || [ "$3" = "build-third-party" ] || [ "$4" = "build-third-party" ] ; then
    BUILD_THIRD_PARTY="build-third-party"
else
    BUILD_THIRD_PARTY=""
fi

if [ "$2" = "release" ] || [ "$3" = "release" ] || [ "$4" = "release" ] ; then
    RELEASE_MODE="release"
else
    RELEASE_MODE="debug"
fi

if [ "$2" = "run" ] || [ "$3" = "run" ] [ "$4" = "run" ] ; then
    RUN="run"
else
    RUN=""
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

    . ${BUILD_DIR}build_options.sh
    export OUTPUT=$TARGET
    export THIRD_PARTY_LIB="$TARGET-third-party.a"
    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS"
else
    export OPT="-g"
    export ASAN="-fsanitize=address -fno-omit-frame-pointer"
    BASE_CXXFLAGS="$BASE_CXXFLAGS" # -Wall -Weverything"
    export ARCH="-m64 -maes -mssse3 -msse4.1"

    . ${BUILD_DIR}build_options.sh
    export OUTPUT=${TARGET}_debug
    export THIRD_PARTY_LIB="$TARGET-third-party-debug.a"

    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS_DEBUG"
fi

if [ $TARGET_TYPE == "SHAREDLIB" ] || [ $TARGET_TYPE == "STATICLIB" ]; then
    # Keep third-party lib separate from other builds
    # Disable ASAN since it would force user of .so to enable ASAN
    export THIRD_PARTY_LIB="lib${THIRD_PARTY_LIB}"
    export ASAN=""
    export OPT="$OPT -fPIC -fvisibility=hidden"
fi

if [ ! -e "${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB" ]; then
    BUILD_THIRD_PARTY="build-third-party"
fi

mkdir -p ${BASE_DIR}build/third-party-$RELEASE_MODE

if [ "$BUILD_THIRD_PARTY" = "build-third-party" ]; then
    echo "Compiling third party dependencies to library" $THIRD_PARTY_LIB
    cd ${BASE_DIR}build/third-party-$RELEASE_MODE
    rm -rf ${BASE_DIR}build/third-party-$RELEASE_MODE/*.o
    clang++ -c $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC
    if [ -n "$THIRDPARTY_SRC_SSE42" ]; then
        clang++ -c $OPT -msse4.2 $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_SSE42
    fi
    if [ -n "$THIRDPARTY_SRC_AVX2" ]; then
        clang++ -c $OPT -mavx2 $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_AVX2
    fi
    if [ -n "$THIRDPARTY_SRC_AVX512" ]; then
        clang++ -c $OPT -mavx512vl -mavx512f $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_AVX512
    fi
    ar rc ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB *.o
    cd $BASE_DIR
fi

if [ $TARGET_TYPE == "EXECUTABLE" ]; then
    echo Building ${BASE_DIR}build/$OUTPUT
    clang++ -o ${BASE_DIR}build/$OUTPUT $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
fi

if [ $TARGET_TYPE == "SHAREDLIB" ]; then
    echo Building ${BASE_DIR}build/lib${OUTPUT}.so
    clang++ -shared -o ${BASE_DIR}build/lib${OUTPUT}.so $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
fi

if [ $TARGET_TYPE == "STATICLIB" ]; then
    echo Building ${BASE_DIR}build/$OUTPUT.a
    mkdir -p ${BASE_DIR}build/static-lib-$RELEASE_MODE
    cd ${BASE_DIR}build/static-lib-$RELEASE_MODE
    rm -rf ${BASE_DIR}build/static-lib-$RELEASE_MODE/*.o
    clang++ -c $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC
    ar rc ${BASE_DIR}build/$OUTPUT.a *.o ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
    cd ..
fi

if [ $TARGET_TYPE == "EXECUTABLE" ] && [ "$RUN" = "run" ]; then
    pushd ${BUILD_DIR}
    ${BASE_DIR}build/$OUTPUT
    popd
fi

#if [ "$TARGET_MODE" = "lib" ]; then
#    mkdir -p ${BASE_DIR}build/lib-$RELEASE_MODE
#    rm -rf ${BASE_DIR}build/lib-$RELEASE_MODE/*.o
#    cd ${BASE_DIR}build/lib-$RELEASE_MODE
#    clang++ -c $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC
#    echo ${BASE_DIR}build/$TARGET
#    ar rc ${BASE_DIR}build/$TARGET *.o ${BASE_DIR}build/third-party-$RELEASE_MODE/*.o
#    cd $BASE_DIR
#else
#    echo Building $OUTPUT
#    clang++ -o ${BASE_DIR}build/$OUTPUT $OPT $DISASSEMBLY $ARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
#fi
