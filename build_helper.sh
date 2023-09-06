#!/bin/bash
set -e

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"
export SOURCE_FOLDER=$1

shift

if [[ "$*" == *"arm64"* ]]
then
    ARCH="arm64"
fi

if [[ "$*" == *"x64"* ]]
then
    ARCH="x64"
fi

. ${BASE_DIR}arch_helper.sh $ARCH

if [[ "$*" == *"build-third-party"* ]]
then
    BUILD_THIRD_PARTY="build-third-party"
else
    BUILD_THIRD_PARTY=""
fi

if [[ "$*" == *"release"* ]]
then
    RELEASE_MODE="release"
else
    RELEASE_MODE="debug"
fi

if [[ "$*" == *"run"* ]]
then
    RUN="run"
else
    RUN=""
fi

export BASE_CXXFLAGS="-Wno-sign-conversion -Wno-missing-prototypes -Wno-cast-align -Wno-unused-function -Wno-deprecated-register -Wno-deprecated -Wno-c++98-compat-pedantic -Wno-unused-parameter -Wno-unused-template -Wno-zero-as-null-pointer-constant -Wno-old-style-cast -Wno-global-constructors -Wno-padded"

. ${SOURCE_FOLDER}build_options.sh

OUTPUT_FOLDER="${BASE_DIR}build/${PLATFORM}/${TARGET}/${RELEASE_MODE}"
if [ ! -d ${OUTPUT_FOLDER} ]
then
    mkdir -p ${OUTPUT_FOLDER}
fi

THIRD_PARTY_OUTPUT_FOLDER="${OUTPUT_FOLDER}/third-party"
if [ ! -d ${THIRD_PARTY_OUTPUT_FOLDER} ]
then
    mkdir -p ${THIRD_PARTY_OUTPUT_FOLDER}
fi

THIRD_PARTY_LIB="${TARGET}-third-party.a"

# -pedantic
# -Wno-atomic-implicit-seq-cst
# -Wno-extra-semi-stmt
# -Wno-implicit-int-conversion
if [ "$RELEASE_MODE" = "release" ]; then
    export OPT=-O3
    #DISASSEMBLY='-S -masm=intel'
    export ASAN=""
    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS"
else
    export OPT="-g"
    export ASAN="-fsanitize=address -fno-omit-frame-pointer"
    BASE_CXXFLAGS="$BASE_CXXFLAGS" # -Wall -Weverything"
    export CXXFLAGS="$BASE_CXXFLAGS $CXXFLAGS_DEBUG"
fi

if [ $ARCH == "x64" ]; then
    export BASEARCH="-m64 -maes -mssse3 -msse4.1"
fi

if [ $ARCH == "arm64" ]; then
    export BASEARCH="-m64"
fi

if [ $TARGET_TYPE == "SHAREDLIB" ] || [ $TARGET_TYPE == "STATICLIB" ]; then
    # Keep third-party lib separate from other builds
    # Disable ASAN since it would force user of .so to enable ASAN
    export THIRD_PARTY_LIB="lib${THIRD_PARTY_LIB}"
    export ASAN=""
    export OPT="$OPT -fPIC -fvisibility=hidden"
fi

if [ ! -f "${THIRD_PARTY_OUTPUT_FOLDER}/${THIRD_PARTY_LIB}" ]; then
    BUILD_THIRD_PARTY="build-third-party"
fi

clang++ --version

if [ "$BUILD_THIRD_PARTY" = "build-third-party" ]; then
    echo "Compiling third party dependencies to library" $THIRD_PARTY_LIB
    cd ${THIRD_PARTY_OUTPUT_FOLDER}
    rm -rf ${THIRD_PARTY_OUTPUT_FOLDER}/*.o
    clang++ -c $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC
    if [ $ARCH == "x64" ]; then
        if [ -n "$THIRDPARTY_SRC_SSE" ]; then
            clang++ -c $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_SSE
        fi
        if [ -n "$THIRDPARTY_SRC_SSE42" ]; then
            clang++ -c $OPT -msse4.2 $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_SSE42
        fi
        if [ -n "$THIRDPARTY_SRC_AVX2" ]; then
            clang++ -c $OPT -mavx2 $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_AVX2
        fi
        if [ -n "$THIRDPARTY_SRC_AVX512" ]; then
            clang++ -c $OPT -mavx512vl -mavx512f $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_AVX512
        fi
    fi
    if [ $ARCH == "arm64" ]; then
        if [ -n "$THIRDPARTY_SRC_NEON" ]; then
            clang++ -c -mfloat-abi=hard $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $THIRDPARTY_SRC_NEON
        fi
    fi
    if [ -n "$ZSTD_THIRDPARTY_GCC_SRC" ]; then
        clang++ -c $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $ZSTD_THIRDPARTY_GCC_SRC
    fi
    ar rc ${THIRD_PARTY_OUTPUT_FOLDER}/$THIRD_PARTY_LIB *.o
    cd $BASE_DIR
fi

if [ $TARGET_TYPE == "EXECUTABLE" ]; then
    echo Building ${OUTPUT_FOLDER}/${TARGET}
    clang++ -o ${OUTPUT_FOLDER}/${TARGET} $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${THIRD_PARTY_OUTPUT_FOLDER}/$THIRD_PARTY_LIB
fi

if [ $TARGET_TYPE == "SHAREDLIB" ]; then
    echo Building ${OUTPUT_FOLDER}/${TARGET}.so
    clang++ -shared -o ${OUTPUT_FOLDER}/${TARGET}.so $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${THIRD_PARTY_OUTPUT_FOLDER}/$THIRD_PARTY_LIB
fi

if [ $TARGET_TYPE == "STATICLIB" ]; then
    echo Building ${OUTPUT_FOLDER}.a
    mkdir -p ${BASE_DIR}build/static-lib-$RELEASE_MODE
    cd ${BASE_DIR}build/static-lib-$RELEASE_MODE
    rm -rf ${BASE_DIR}build/static-lib-$RELEASE_MODE/*.o
    clang++ -c $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC
    ar rc ${OUTPUT_FOLDER}.a *.o ${THIRD_PARTY_OUTPUT_FOLDER}/$THIRD_PARTY_LIB
    cd ..
fi

if [ $TARGET_TYPE == "EXECUTABLE" ] && [ "$RUN" = "run" ]; then
    pushd ${SOURCE_FOLDER}
    ${OUTPUT_FOLDER}/${TARGET}
    popd
fi

#if [ "$TARGET_MODE" = "lib" ]; then
#    mkdir -p ${BASE_DIR}build/lib-$RELEASE_MODE
#    rm -rf ${BASE_DIR}build/lib-$RELEASE_MODE/*.o
#    cd ${BASE_DIR}build/lib-$RELEASE_MODE
#    clang++ -c $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC
#    echo ${BASE_DIR}build/$TARGET
#    ar rc ${BASE_DIR}build/$TARGET *.o ${BASE_DIR}build/third-party-$RELEASE_MODE/*.o
#    cd $BASE_DIR
#else
#    echo Building $OUTPUT
#    clang++ -o ${BASE_DIR}build/$OUTPUT $OPT $DISASSEMBLY $BASEARCH -std=c++11 $CXXFLAGS $ASAN -Isrc $SRC $MAIN_SRC ${BASE_DIR}build/third-party-$RELEASE_MODE/$THIRD_PARTY_LIB
#fi
