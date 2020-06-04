#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    export PLATFORM=macos
else
	export PLATFORM=linux
fi

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

PLATFORM=linux_%ARCH%
CXXFLAGS=-std=gnu99 -g -m64 -pthread -msse4.1 -maes -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00

. ../default_build_options.sh

LIB_TARGET_FOLDER=${BASE_DIR}build/
OBJDIR=${BASE_DIR}build/static-lib

if [ "$1" == "release"]; then
	LIB_TARGET=${LIB_TARGET_FOLDER}longtail_${PLATFORM}.a
	OPT=-O3
else
	LIB_TARGET=${LIB_TARGET_FOLDER}longtail_${PLATFORM}_debug.a
	OPT=
	OBJDIR=${BASE_DIR}build/static-lib-debug
fi

echo Building ${LIB_TARGET}

if exist !OBJDIR! rmdir /Q /S !OBJDIR!
mkdir !OBJDIR!

if not exist "!LIB_TARGET_FOLDER!" mkdir "!LIB_TARGET_FOLDER!"

pushd !OBJDIR!

gcc -c !CXXFLAGS! !OPT! !THIRDPARTY_SRC! !SRC!
if [ -z ${THIRDPARTY_SRC_SSE42} ]; then
	gcc -c !CXXFLAGS! !OPT! -msse4.2 ${THIRDPARTY_SRC_SSE42}
)
if [ -z ${THIRDPARTY_SRC_AVX2} ]; then
	gcc -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 ${THIRDPARTY_SRC_AVX2}
)
if [ -z ${THIRDPARTY_SRC_AVX512} ]; then
	gcc -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables ${THIRDPARTY_SRC_AVX512}
)
popd

ar rc !LIB_TARGET! !BASE_DIR!build\static_library/*.o
