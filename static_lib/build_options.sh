#!/bin/bash

export TARGET=longtail_staticlib
export TARGET_TYPE=STATICLIB

. ${BASE_DIR}all_sources.sh
. ${BASE_DIR}default_build_options.sh

export MAIN_SRC="$BASE_DIR/static_lib/test.c"
