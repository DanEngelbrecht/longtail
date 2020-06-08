#!/bin/bash
set -e

COMPILER="gcc"

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
	COMPILER="clang"
else
	OS="linux"
fi

ARCH=x64

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(dirname "$BASE_DIR")/"

PLATFORM="${OS}_${ARCH}"
CXXFLAGS="-std=gnu99 -g -m64 -maes -mssse3 -msse4.1 -pthread"
LIB_TARGET_FOLDER=${BASE_DIR}build/
OBJDIR=${BASE_DIR}build/static-lib

mkdir -p $LIB_TARGET_FOLDER

. ../all_sources.sh

if [ "$1" == "release" ]; then
	LIB_FILENAME="longtail_${PLATFORM}"
	OPT="-O3"
else
	LIB_FILENAME="longtail_${PLATFORM}_debug"
	OPT=
	OBJDIR="${BASE_DIR}build/static-lib-debug"
fi

LIB_TARGET="${LIB_TARGET_FOLDER}lib${LIB_FILENAME}.a"

echo Building ${LIB_TARGET}

[ -d ${OBJDIR} ] && ( rm -r ${OBJDIR} )
mkdir ${OBJDIR}

[ ! -d "${LIB_TARGET_FOLDER}" ] && ( mkdir "${LIB_TARGET_FOLDER}" )

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

if [ "$(uname)" == "Darwin" ]; then
	libtool -static -o ${LIB_TARGET} ${OBJDIR}/*.o
else
	ar rc ${LIB_TARGET} ${OBJDIR}/*.o
fi

TEST_EXECUTABLEPATH="${BASE_DIR}build/static_lib_test"

echo Validating ${LIB_TARGET}
${COMPILER} ${CXXFLAGS} test.c -o ${TEST_EXECUTABLEPATH} -lm -L${BASE_DIR}build -l:lib${LIB_FILENAME}.a
${TEST_EXECUTABLEPATH}
