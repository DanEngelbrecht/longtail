#!/usr/bin/env bash

set -e

export OPT=-O3
#DISASSEMBLY='-S -masm=intel'
export ASAN=""
export CXXFLAGS="-Wno-deprecated-register -Wno-deprecated"
export ARCH="-m64 -maes"
export OUTPUT=test

. ./build_src.sh

sh build/clang_build.sh
