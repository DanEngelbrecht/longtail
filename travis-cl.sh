#!/usr/bin/env bash

set -e

cd test
../build.bat
../build.bat release
cd ..
cd cmd
../build.bat
../build.bat release
cd ..
./build/test_debug.exe
./build/test.exe
