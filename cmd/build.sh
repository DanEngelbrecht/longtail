#!/bin/bash
set -e

BUILDFOLDER="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/"
HELPERFOLDER="$(dirname "$BUILDFOLDER")/"

. ${HELPERFOLDER}build_helper.sh $BUILDFOLDER $@
