#!/usr/bin/env bash

set -e

pushd test
sh ../build.sh
sh ../build.sh release
popd
pushd cmd
sh ../build.sh
sh ../build.sh release
popd
./build/test_debug
./build/test
