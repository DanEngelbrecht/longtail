#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
    export COMPILER="clang"
else
    OS="linux"
    export COMPILER="gcc"
fi

ARCH=x64
export PLATFORM="${OS}_${ARCH}"
