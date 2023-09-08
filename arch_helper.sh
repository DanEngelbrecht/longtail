#!/bin/bash
set -e

if [ "$(uname)" == "Darwin" ]; then
    OS="darwin"
    export COMPILER="clang"
else
    OS="linux"
    export COMPILER="gcc"
fi

ARCH="x64"

if [ "$1" = "arm64" ]; then
    ARCH="arm64"
fi

export PLATFORM="${OS}_${ARCH}"
