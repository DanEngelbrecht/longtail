#!/usr/bin/env bash

LONGTAIL=../build/longtail_debug
BASEPATH=/mnt/c/Temp/longtail

echo Indexing currently known chunks "$BASEPATH/chunks"
$LONGTAIL --create-content-index "$BASEPATH/chunks.lci" --content "$BASEPATH/chunks"

Office=true

if $Home; then
fi

if $Office; then
./do_version.sh $LONGTAIL $BASEPATH WinClient/CL6332_WindowsClient
./do_version.sh $LONGTAIL $BASEPATH WinClient/CL6333_WindowsClient
./do_version.sh $LONGTAIL $BASEPATH WinClient/CL6336_WindowsClient
./do_version.sh $LONGTAIL $BASEPATH WinClient/CL6338_WindowsClient
./do_version.sh $LONGTAIL $BASEPATH WinClient/CL6339_WindowsClient
if

echo "Done"
