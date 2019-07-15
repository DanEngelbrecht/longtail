#!/usr/bin/env bash

set -e

sh ./build.sh
./build/test_debug
sh ./build.sh release
./build/test
