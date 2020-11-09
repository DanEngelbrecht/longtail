#!/bin/bash

export TARGET=longtail
export TARGET_TYPE=EXECUTABLE

. ${BASE_DIR}all_sources.sh
. ${BASE_DIR}default_build_options.sh

export MAIN_SRC="$BASE_DIR/cmd/main.c"
