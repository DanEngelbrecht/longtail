#!/bin/bash

set -e

cd test
bash ../build.sh
bash ../build.sh release
cd ..
cd cmd
bash ../build.sh
bash ../build.sh release
cd ..
cd test
../build/test_debug
../build/test
cd ..
