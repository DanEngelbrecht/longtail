#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
    COMPILER="clang"
else
    OS="linux"
    COMPILER="gcc"
fi

ARCH=x64

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(dirname "$BASE_DIR")/"

PLATFORM="${OS}_${ARCH}"
CXXFLAGS="-std=gnu99 -g -m64 -maes -mssse3 -msse4.1 -pthread"
LIB_TARGET_FOLDER=${BASE_DIR}build/static/

mkdir -p $LIB_TARGET_FOLDER

. $BASE_DIR/all_sources.sh

if [ "$1" == "release" ]; then
    LIB_FILENAME="longtail_${PLATFORM}"
    OPT="-O3"
    OBJDIR="${BASE_DIR}build/static-lib-release"
else
    LIB_FILENAME="longtail_${PLATFORM}_debug"
    OPT=
    OBJDIR="${BASE_DIR}build/static-lib-debug"
    CXXFLAGS="${CXXFLAGS} -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS"
fi

LIB_TARGET="${LIB_TARGET_FOLDER}lib${LIB_FILENAME}.a"

echo Building ${LIB_TARGET}

if [ -f ${LIB_TARGET} ]
then
    rm ${LIB_TARGET}
fi

mkdir -p ${LIB_TARGET_FOLDER}

if [ -d ${OBJDIR} ]
then
    rm -rf ${OBJDIR}
fi

mkdir -p ${OBJDIR}

pushd ${OBJDIR}

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

popd

TEST_EXECUTABLEPATH="${BASE_DIR}build/static_lib_test"

ar cru -v ${LIB_TARGET} ${OBJDIR}/*.o
ls -la ${LIB_TARGET}

echo Validating ${LIB_TARGET}
${COMPILER} -o ${TEST_EXECUTABLEPATH} ${CXXFLAGS} test.c -lm -L${LIB_TARGET_FOLDER} -l${LIB_FILENAME} --verbose
${TEST_EXECUTABLEPATH}
