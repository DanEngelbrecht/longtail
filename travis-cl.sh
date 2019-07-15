#!/usr/bin/env bash

set -e

./build.bat
./build/test_debug.exe
./build.bat release
./build/test.exe
