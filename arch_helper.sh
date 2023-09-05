#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
    export COMPILER="clang"
else
    OS="linux"
    export COMPILER="gcc"
fi

ARCH="$2"

if [ ! -z "$ARCH" ]; then
    ARCH=x64
fi

export PLATFORM="${OS}_${ARCH}"
