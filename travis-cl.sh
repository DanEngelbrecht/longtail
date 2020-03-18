#!/bin/bash

set -e

cd test
../build.bat
../build.bat release
cd ..
cd cmd
../build.bat
../build.bat release
cd ..
cd test
../build/test_debug.exe
../build/test.exe
cd ..
cmd shared_lib
..\build.bat
..\build.bat release
dist.bat
cd ..


