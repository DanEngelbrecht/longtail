#!/bin/bash
set -e

SOURCEFOLDER="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"
BASE_DIR="$(dirname "$SOURCEFOLDER")/"

if [[ "$*" == *"arm64"* ]]
then
    ARCH="arm64"
fi

if [[ "$*" == *"x64"* ]]
then
    ARCH="x64"
fi

. ${BASE_DIR}arch_helper.sh $ARCH

if [[ "$*" == *"release"* ]]
then
    RELEASE_MODE="release"
else
    RELEASE_MODE="debug"
fi

CXXFLAGS="-std=gnu99 -g -pthread"
TARGET=longtail_static

. $BASE_DIR/all_sources.sh

OUTPUT_FOLDER="${BASE_DIR}build/${PLATFORM}/${TARGET}/${RELEASE_MODE}"
if [ ! -d ${OUTPUT_FOLDER} ]
then
    mkdir -p ${OUTPUT_FOLDER}
fi

if [ "$RELEASE_MODE" = "release" ]; then
    OPT="-O3"
else
    OPT=
    CXXFLAGS="${CXXFLAGS} -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS"
fi

if [ $ARCH == "x64" ]; then
    export BASEARCH="-m64 -maes -mssse3 -msse4.1"
fi

if [ $ARCH == "arm64" ]; then
    if [ $COMPILER == "clang" ]; then
        export BASEARCH="-m64 -arch arm64"
    else
        export BASEARCH="-m64"
    fi
fi

LIB_TARGET="${OUTPUT_FOLDER}/lib${TARGET}.a"

echo Building ${LIB_TARGET}

if [ -f ${LIB_TARGET} ]
then
    rm ${LIB_TARGET}
fi

rm -rf ${OUTPUT_FOLDER}/*.o

pushd ${OUTPUT_FOLDER}

${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} ${THIRDPARTY_SRC} ${SRC}

if [ $ARCH == "x64" ]; then
    if [ -n "${THIRDPARTY_SSE}" ]; then
        ${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} ${THIRDPARTY_SSE}
    fi
    if [ -n "${THIRDPARTY_SSE42}" ]; then
        ${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} -msse4.2 ${THIRDPARTY_SSE42}
    fi
    if [ -n "${THIRDPARTY_SRC_AVX2}" ]; then
        ${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} -msse4.2 -mavx2 ${THIRDPARTY_SRC_AVX2}
    fi
    if [ -n "${THIRDPARTY_SRC_AVX512}" ]; then
        ${COMPILER} -c ${THIRDPARTY_SRC_AVX512} ${OPT} ${BASEARCH} -msse4.2 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables ${THIRDPARTY_SRC_AVX512}
    fi
fi

if [ $ARCH == "arm64" ]; then
    if [ $COMPILER == "clang" ]; then
        if [ -n "$THIRDPARTY_SRC_NEON" ]; then
            ${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} ${THIRDPARTY_SRC_NEON}
        fi
    fi
fi

if [ -n ${ZSTD_THIRDPARTY_GCC_SRC} ]; then
    ${COMPILER} -c ${CXXFLAGS} ${OPT} ${BASEARCH} -fno-asynchronous-unwind-tables ${ZSTD_THIRDPARTY_GCC_SRC}
fi

popd

TEST_EXECUTABLEPATH="${OUTPUT_FOLDER}/${TARGET}_test"

ar cru -v ${LIB_TARGET} ${OUTPUT_FOLDER}/*.o
ls -la ${LIB_TARGET}

echo Validating ${LIB_TARGET}
${COMPILER} -o ${TEST_EXECUTABLEPATH} ${CXXFLAGS} ${BASEARCH} ${SOURCEFOLDER}test.c -lm -L${OUTPUT_FOLDER} -l${TARGET} --verbose

if [ $ARCH == "x64" ]; then
    ${TEST_EXECUTABLEPATH}
fi
