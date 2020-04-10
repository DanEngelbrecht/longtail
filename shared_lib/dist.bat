@echo off

set BASE_DIR=%~dp0
set ROOT_DIR=%BASE_DIR%..
set TARGET_DIR=%ROOT_DIR%\dist

IF not exist %TARGET_DIR% (mkdir %TARGET_DIR%)

IF not exist %TARGET_DIR%\src (mkdir %TARGET_DIR%\src)
cp %ROOT_DIR%\src\longtail.h %TARGET_DIR%\src
cp %ROOT_DIR%\src\longtail.c %TARGET_DIR%\src
IF not exist %TARGET_DIR%\src\ext (mkdir %TARGET_DIR%\src\ext)
cp %ROOT_DIR%\src\ext\stb_ds.h %TARGET_DIR%\src\ext
cp %ROOT_DIR%\src\ext\stb_ds.c %TARGET_DIR%\src\ext
IF not exist %TARGET_DIR%\lib (mkdir %TARGET_DIR%\lib)

IF not exist %TARGET_DIR%\lib\bikeshed (mkdir %TARGET_DIR%\lib\bikeshed)
cp %ROOT_DIR%\lib\bikeshed\longtail_bikeshed.h %TARGET_DIR%\lib\bikeshed

IF not exist %TARGET_DIR%\lib\blake2 (mkdir %TARGET_DIR%\lib\blake2)
cp %ROOT_DIR%\lib\blake2\longtail_blake2.h %TARGET_DIR%\lib\blake2

IF not exist %TARGET_DIR%\lib\blake3 (mkdir %TARGET_DIR%\lib\blake3)
cp %ROOT_DIR%\lib\blake3\longtail_blake3.h %TARGET_DIR%\lib\blake3

IF not exist %TARGET_DIR%\lib\brotli (mkdir %TARGET_DIR%\lib\brotli)
cp %ROOT_DIR%\lib\brotli\longtail_brotli.h %TARGET_DIR%\lib\brotli

IF not exist %TARGET_DIR%\lib\cacheblockstore (mkdir %TARGET_DIR%\lib\cacheblockstore)
cp %ROOT_DIR%\lib\cacheblockstore\longtail_cacheblockstore.h %TARGET_DIR%\lib\cacheblockstore

IF not exist %TARGET_DIR%\lib\compressblockstore (mkdir %TARGET_DIR%\lib\compressblockstore)
cp %ROOT_DIR%\lib\compressblockstore\longtail_compressblockstore.h %TARGET_DIR%\lib\compressblockstore

IF not exist %TARGET_DIR%\lib\filestorage (mkdir %TARGET_DIR%\lib\filestorage)
cp %ROOT_DIR%\lib\filestorage\longtail_filestorage.h %TARGET_DIR%\lib\filestorage

IF not exist %TARGET_DIR%\lib\fsblockstore (mkdir %TARGET_DIR%\lib\fsblockstore)
cp %ROOT_DIR%\lib\fsblockstore\longtail_fsblockstore.h %TARGET_DIR%\lib\fsblockstore

IF not exist %TARGET_DIR%\lib\lz4 (mkdir %TARGET_DIR%\lib\lz4)
cp %ROOT_DIR%\lib\lz4\longtail_lz4.h %TARGET_DIR%\lib\lz4

IF not exist %TARGET_DIR%\lib\memstorage (mkdir %TARGET_DIR%\lib\memstorage)
cp %ROOT_DIR%\lib\memstorage\longtail_memstorage.h %TARGET_DIR%\lib\memstorage

IF not exist %TARGET_DIR%\lib\meowhash (mkdir %TARGET_DIR%\lib\meowhash)
cp %ROOT_DIR%\lib\meowhash\longtail_meowhash.h %TARGET_DIR%\lib\meowhash

IF not exist %TARGET_DIR%\lib\zstd (mkdir %TARGET_DIR%\lib\zstd)
cp %ROOT_DIR%\lib\zstd\longtail_zstd.h %TARGET_DIR%\lib\zstd

IF not exist %TARGET_DIR%\dll (mkdir %TARGET_DIR%\dll)
cp %ROOT_DIR%\build\longtail_debug.dll %TARGET_DIR%\dll
cp %ROOT_DIR%\build\longtail.dll %TARGET_DIR%\dll
