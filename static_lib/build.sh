#!/bin/bash
set -e

SOURCEFOLDER="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"
BASE_DIR="$(dirname "$SOURCEFOLDER")/"

. ${BASE_DIR}arch_helper.sh

CXXFLAGS="-std=gnu99 -g -m64 -maes -mssse3 -msse4.1 -pthread"
TARGET=longtail_static

. $BASE_DIR/all_sources.sh

if [ "$1" == "release" ]; then
    RELEASE_MODE="release"
    OPT="-O3"
else
    RELEASE_MODE="debug"
    OPT=
    CXXFLAGS="${CXXFLAGS} -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS"
fi

OUTPUT_FOLDER="${BASE_DIR}build/${PLATFORM}/${TARGET}/${RELEASE_MODE}"
if [ ! -d ${OUTPUT_FOLDER} ]
then
    mkdir -p ${OUTPUT_FOLDER}
fi

LIB_TARGET="${OUTPUT_FOLDER}/lib${TARGET}.a"

echo Building ${LIB_TARGET}

if [ -f ${LIB_TARGET} ]
then
    rm ${LIB_TARGET}
fi

rm -rf ${OUTPUT_FOLDER}/*.o

pushd ${OUTPUT_FOLDER}
${COMPILER} -c ${CXXFLAGS} ${OPT} ${THIRDPARTY_SRC} ${SRC}
if [ ! -z ${THIRDPARTY_SRC_SSE42} ]; then
    ${COMPILER} -c ${CXXFLAGS} ${OPT} -msse4.2 ${THIRDPARTY_SRC_SSE42}
fi
if [ ! -z ${THIRDPARTY_SRC_AVX2} ]; then
    ${COMPILER} -c ${CXXFLAGS} ${OPT} -msse4.2 -mavx2 ${THIRDPARTY_SRC_AVX2}
fi
if [ ! -z ${THIRDPARTY_SRC_AVX512} ]; then
    ${COMPILER} -c ${CXXFLAGS} ${OPT} -msse4.2 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables ${THIRDPARTY_SRC_AVX512}
fi
if [ ! -z ${ZSTD_THIRDPARTY_GCC_SRC} ]; then
    ${COMPILER} -c ${CXXFLAGS} ${OPT} -fno-asynchronous-unwind-tables ${ZSTD_THIRDPARTY_GCC_SRC}
fi

popd

TEST_EXECUTABLEPATH="${OUTPUT_FOLDER}/${TARGET}_test"

ar cru -v ${LIB_TARGET} ${OUTPUT_FOLDER}/*.o
ls -la ${LIB_TARGET}

echo Validating ${LIB_TARGET}
${COMPILER} -o ${TEST_EXECUTABLEPATH} ${CXXFLAGS} ${SOURCEFOLDER}test.c -lm -L${OUTPUT_FOLDER} -l${TARGET} --verbose
${TEST_EXECUTABLEPATH}
