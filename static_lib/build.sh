#!/bin/bash
set -e

COMPILER="gcc"

if [ "$(uname)" == "Darwin" ]; then
    export OS="darwin"
	COMPILER="clang"
else
	export OS="linux"
fi

ARCH=x64

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/../"

PLATFORM="${OS}_${ARCH}"
CXXFLAGS="-std=gnu99 -g -m64 -pthread -msse4.1 -maes"
LIB_TARGET_FOLDER=${BASE_DIR}build/
OBJDIR=${BASE_DIR}build/static-lib

. ../all_sources.sh

if [ "$1" == "release" ]; then
	LIB_FILENAME="longtail_${PLATFORM}.a"
	OPT="-O3"
else
	LIB_FILENAME="longtail_${PLATFORM}_debug.a"
	OPT=
	OBJDIR="${BASE_DIR}build/static-lib-debug"
fi

LIB_TARGET="${LIB_TARGET_FOLDER}${LIB_FILENAME}"

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

ar rc ${LIB_TARGET} ${OBJDIR}/*.o

echo Validating ${LIB_TARGET}
${COMPILER} ${CXXFLAGS} test.c -o ${BASE_DIR}build/static_lib_test -lm -L../build -l:${LIB_FILENAME}
${BASE_DIR}build/static_lib_test
