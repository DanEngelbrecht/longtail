#!/usr/bin/env bash

set -e

./build/d_cl.bat
./output/test_debug.exe
./build/r_cl.bat
./output/test.exe
