#!/usr/bin/env bash

set -e

sh ./build/d_clang.sh
./output/test_debug
sh ./build/r_clang.sh
./output/test
