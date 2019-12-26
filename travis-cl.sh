#!/usr/bin/env bash

set -e

pushd test
../build.bat
../build.bat release
popd
pushd cmd
../build.bat
../build.bat release
popd
./build/test_debug.exe
./build/test.exe
