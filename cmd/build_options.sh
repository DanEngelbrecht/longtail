#!/bin/bash

export TARGET=longtail
export TARGET_TYPE=EXECUTABLE

. ${BASE_DIR}all_sources.sh
. ${BASE_DIR}default_build_options.sh

MINIFB_SRC="${BASE_DIR}cmd/lib/minifb/*.c"

SRC="${SRC} ${MINIFB_SRC}"

export MAIN_SRC="$BASE_DIR/cmd/main.c"
