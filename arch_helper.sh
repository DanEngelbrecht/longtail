#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
    ARCH=arm64
#     export COMPILER="clang"
else
    OS="linux"
    ARCH=x64
fi

export COMPILER="gcc"
export PLATFORM="${OS}_${ARCH}"