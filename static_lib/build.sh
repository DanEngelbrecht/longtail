#!/bin/bash
set -e

COMPILER="gcc"

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
#	COMPILER="clang"
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

if [ -d ${LIB_TARGET_FOLDER} ]
then
	rm -rf ${LIB_TARGET_FOLDER}
fi

mkdir -p ${LIB_TARGET_FOLDER}

if [ -d ${OBJDIR} ]
then
	rm -rf ${OBJDIR}
fi

if [ -f ${BASE_DIR}build/test.o ]
then
	rm ${BASE_DIR}build/test.o
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

if [ "$(uname)" == "Darwin" ]; then
	echo "libtool -static -o ${LIB_TARGET} ${OBJDIR}/*.o"
	ar cru -v ${LIB_TARGET} ${OBJDIR}/*.o
#	libtool -static -o ${LIB_TARGET} ${OBJDIR}/*.o
	ls -la ${LIB_TARGET}
	LIB_IMPORT_NAME=${LIB_FILENAME}
else
	ar cr -v ${LIB_TARGET} ${OBJDIR}/*.o
	ls -la ${LIB_TARGET}
	LIB_IMPORT_NAME=${LIB_FILENAME}
fi

echo Validating ${LIB_TARGET}
pushd ${BASE_DIR}build
${COMPILER} -c ${CXXFLAGS} ${BASE_DIR}/static_lib/test.c
popd

#cp ${LIB_TARGET} .

${COMPILER} -o ${TEST_EXECUTABLEPATH} ${CXXFLAGS} ${BASE_DIR}build/test.o -lm -L${LIB_TARGET_FOLDER} -l${LIB_FILENAME} --verbose


#-l:${LIB_FILENAME}.a -L${BASE_DIR}build -lm  --verbose
#ld -o ${TEST_EXECUTABLEPATH} -lpthread ${BASE_DIR}build/test.o -l:${LIB_FILENAME}.a -lm -L. --verbose
# --verbose
# ${CXXFLAGS}

${TEST_EXECUTABLEPATH}
