#!/bin/bash

export TARGET=test
export TARGET_TYPE=EXECUTABLE

. ../all_sources.sh
. ../default_build_options.sh

export MAIN_SRC="$BASE_DIR/test/main.cpp $BASE_DIR/test/test.cpp"
