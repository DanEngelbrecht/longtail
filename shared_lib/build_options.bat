set TARGET=longtail
set TARGET_TYPE=SHAREDLIB

set LIB_SRC=%BASE_DIR%lib\filestorage\*.c
set LIB_THIRDPARTY_SRC=%BASE_DIR%\src\ext\*.c

set ATOMICCANCEL_SRC=%BASE_DIR%lib\atomiccancel\*.c

set CACHEBLOCKSTORE_SRC=%BASE_DIR%lib\cacheblockstore\*.c

set COMPRESSBLOCKSTORE_SRC=%BASE_DIR%lib\compressblockstore\*.c

set FILESTORAGE_SRC=%BASE_DIR%lib\*.c

set FSBLOCKSTORE_SRC=%BASE_DIR%lib\fsblockstore\*.c

set MEMSTORAGE_SRC=%BASE_DIR%lib\memstorage\*.c

set MEOWHASH_SRC=%BASE_DIR%lib\meowhash\*.c

set COMPRESSION_REGISTRY_SRC=%BASE_DIR%lib\compressionregistry\*.c

set HASH_REGISTRY_SRC=%BASE_DIR%lib\hashregistry\*.c

set RETAININGBLOCKSTORE_SRC=%BASE_DIR%lib\retainingblockstore\*.c

set SHAREBLOCKSTORE_SRC=%BASE_DIR%lib\shareblockstore\*.c

set BIKESHED_SRC=%BASE_DIR%lib\bikeshed\*.c

set BLAKE2_SRC=%BASE_DIR%lib\blake2\*.c
set BLAKE2_THIRDPARTY_SRC=%BASE_DIR%lib\blake2\ext\*.c

set BLAKE3_SRC=%BASE_DIR%lib\blake3\*.c
set BLAKE3_THIRDPARTY_SRC=%BASE_DIR%lib\blake3\ext\*.c

set LZ4_SRC=%BASE_DIR%lib\lz4\*.c
set LZ4_THIRDPARTY_SRC=%BASE_DIR%lib\lz4\ext\*.c

set BROTLI_SRC=%BASE_DIR%lib\brotli\*.c 
set BROTLI_THIRDPARTY_SRC=%BASE_DIR%lib\brotli\ext\common\*.c %BASE_DIR%lib\brotli\ext\dec\*.c %BASE_DIR%lib\brotli\ext\enc\*.c %BASE_DIR%lib\brotli\ext\fuzz\*.c

set ZSTD_SRC=%BASE_DIR%lib\zstd\*.c
set ZSTD_THIRDPARTY_SRC=%BASE_DIR%lib\zstd\ext\common\*.c %BASE_DIR%lib\zstd\ext\compress\*.c %BASE_DIR%lib\zstd\ext\decompress\*.c

set SRC=%BASE_DIR%\src\*.c %LIB_SRC% %ATOMICCANCEL_SRC% %CACHEBLOCKSTORE_SRC% %COMPRESSBLOCKSTORE_SRC% %RETAININGBLOCKSTORE_SRC% %SHAREBLOCKSTORE_SRC% %FILESTORAGE_SRC% %FSBLOCKSTORE_SRC% %MEMSTORAGE_SRC% %MEOWHASH_SRC% %COMPRESSION_REGISTRY_SRC% %HASH_REGISTRY_SRC% %BIKESHED_SRC% %BLAKE2_SRC% %BLAKE3_SRC% %LZ4_SRC% %BROTLI_SRC% %ZSTD_SRC%
set MAIN_SRC=%BASE_DIR%\shared_lib\shared_lib.c
set THIRDPARTY_SRC=%LIB_THIRDPARTY_SRC% %BLAKE2_THIRDPARTY_SRC% %BLAKE3_THIRDPARTY_SRC% %LZ4_THIRDPARTY_SRC% %BROTLI_THIRDPARTY_SRC% %ZSTD_THIRDPARTY_SRC%
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /DLONGTAIL_LOG_LEVEL=5 /D__SSE2__ /D "LONGTAIL_EXPORT=__declspec(dllexport)"
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D_DEBUG /DLONGTAIL_LOG_LEVEL=3 /D__SSE2__ /D "LONGTAIL_EXPORT=__declspec(dllexport)"
