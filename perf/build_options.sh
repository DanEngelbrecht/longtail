#!/bin/bash

export TARGET=perf
export TARGET_TYPE=EXECUTABLE

. ../all_sources.sh
. ../default_build_options.sh

export MAIN_SRC="$BASE_DIR/perf/main.cpp
