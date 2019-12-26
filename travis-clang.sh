#!/bin/bash

set -e

cd test
sh ../build.sh
sh ../build.sh release
cd ..
cd cmd
sh ../build.sh
sh ../build.sh release
cd ..
./build/test_debug
./build/test
