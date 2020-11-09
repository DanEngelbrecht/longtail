#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export PLATFORM=darwin
else
	export PLATFORM=linux
fi

export TARGET=longtail_${PLATFORM}_x64
export TARGET_TYPE=SHAREDLIB

. ${BASE_DIR}all_sources.sh
. ${BASE_DIR}default_build_options.sh

export MAIN_SRC="$BASE_DIR/shared_lib/shared_lib.c"
export CXXFLAGS="$CXXFLAGS -DLONGTAIL_EXPORT_SYMBOLS -DZSTDLIB_VISIBILITY= -DLZ4LIB_VISIBILITY="
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DLONGTAIL_EXPORT_SYMBOLS -DZSTDLIB_VISIBILITY= -DLZ4LIB_VISIBILITY="
