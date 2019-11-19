#!/usr/bin/env bash

echo -------------------- VERSION: $3 --------------------

echo Indexing version $2/local/$3
$1 --create-version-index "$2/$3.lvi" --version "$2/local/$3"

echo Creating content index for unknown content of version $2/local/$3
$1 --create-content-index "$2/$3.lci" --content-index "$2/chunks.lci" --version-index "$2/$3.lvi" --version "$2/local/$3"

echo Creating chunks for unknown content of version $2/local/$3
$1 --create-content "$2/$3_chunks" --content-index "$2/$3.lci" --version "$2/local/$3" --version-index "$2/$3.lvi"

echo Adding the new chunks from $2/$3_chunks to $2/chunks/
[ -d "$2/chunks" ] || mkdir -p "$2/chunks"
cp $2/$3_chunks/* $2/chunks

echo Merging the new chunks from $2/local/$3.lci into $2/chunks.lci
$1 --create-content-index "$2/chunks.lci" --content-index "$2/chunks.lci" --merge-content-index "$2/$3.lci"

echo Creating $2/remote/$3
$1 --create-version "$2/remote/$3" --version-index "$2/$3.lvi" --content "$2/chunks" --content-index "$2/chunks.lci" --version "$2/remote/$3"
