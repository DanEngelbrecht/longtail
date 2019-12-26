#!/bin/bash

if [ -z "$5" ]; then
    echo "gcs_get.sh <longtail-executable> <version_name> <target_folder> <cache_folder> <gcs_bucket_uri>"
    exit 1
fi


LONGTAIL=$1
VERSION_NAME=$2
TARGET_FOLDER=$3
CACHE_FOLDER=$4
BUCKET=$5

echo "LONGTAIL:      $LONGTAIL"
echo "VERSION_NAME:  $VERSION_NAME"
echo "TARGET_FOLDER: $TARGET_FOLDER"
echo "CACHE_FOLDER:  $CACHE_FOLDER"
echo "BUCKET:        $BUCKET"

if [ -f "remote_store.lci" ]; then
    rm remote_store.lci
fi

gsutil cp "$BUCKET/store.lci" "remote_store.lci"

if [ ! -f "$VERSION_NAME.lvi" ]; then
    gsutil cp "$BUCKET/$VERSION_NAME.lvi" "$VERSION_NAME.lvi"
fi

if [ ! -d "$CACHE_FOLDER" ]; then
    mkdir -p $CACHE_FOLDER
fi

$LONGTAIL --log-level 0 --downsync --target-version-index "$VERSION_NAME.lvi" --content "$CACHE_FOLDER" --remote-content-index "remote_store.lci" --output-format $BUCKET/store/{blockname} >download_list.txt

if [ -f download_list.txt ]; then
    cat download_list.txt | gsutil -m cp -I $CACHE_FOLDER
fi

$LONGTAIL --log-level 0 --update-version "$TARGET_FOLDER" --content "$CACHE_FOLDER" --target-version-index "$VERSION_NAME.lvi"
