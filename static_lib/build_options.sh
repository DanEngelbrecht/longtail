#!/bin/bash

if [ "$(uname)" == "Darwin" ]; then
    export PLATFORM=macos
else
	export PLATFORM=linux
fi

export TARGET=longtail_${PLATFORM}_x64
export TARGET_TYPE=STATICLIB

. ../default_build_options.sh

export CXXFLAGS="$CXXFLAGS -DLONGTAIL_EXPORT_SYMBOLS -DZSTDLIB_VISIBILITY= -DLZ4LIB_VISIBILITY="
export CXXFLAGS_DEBUG="$CXXFLAGS_DEBUG -DLONGTAIL_EXPORT_SYMBOLS -DZSTDLIB_VISIBILITY= -DLZ4LIB_VISIBILITY="
