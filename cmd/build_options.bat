set TARGET=longtail

set LIB_SRC=%BASE_DIR%lib\filestorage\*.c
set LIB_THIRDPARTY_SRC=%BASE_DIR%\src\ext\*.c

set FILESTORAGE_SRC=%BASE_DIR%lib\*.c

set MEMSTORAGE_SRC=%BASE_DIR%lib\memstorage\*.c

set MEOWHASH_SRC=%BASE_DIR%lib\meowhash\*.c

set BIKESHED_SRC=%BASE_DIR%lib\bikeshed\*.c

set BLAKE2_SRC=%BASE_DIR%lib\blake2\*.c
set BLAKE2_THIRDPARTY_SRC=%BASE_DIR%lib\blake2\ext\*.c

set LIZARD_SRC=%BASE_DIR%lib\lizard\*.c
set LIZARD_THIRDPARTY_SRC=%BASE_DIR%lib\lizard\ext\*.c %BASE_DIR%lib\lizard\ext\entropy\*.c %BASE_DIR%lib\lizard\ext\xxhash\*.c

set BROTLI_SRC=%BASE_DIR%lib\brotli\*.c 
set BROTLI_THIRDPARTY_SRC=%BASE_DIR%lib\brotli\ext\common\*.c %BASE_DIR%lib\brotli\ext\dec\*.c %BASE_DIR%lib\brotli\ext\enc\*.c %BASE_DIR%lib\brotli\ext\fuzz\*.c

set ZSTD_SRC=%BASE_DIR%lib\zstd\*.c
set ZSTD_THIRDPARTY_SRC=%BASE_DIR%lib\zstd\ext\common\*.c %BASE_DIR%lib\zstd\ext\compress\*.c %BASE_DIR%lib\zstd\ext\decompress\*.c

set SRC=%BASE_DIR%cmd\main.cpp %BASE_DIR%\src\*.c %LIB_SRC% %FILESTORAGE_SRC% %MEMSTORAGE_SRC% %MEOWHASH_SRC% %BIKESHED_SRC% %BLAKE2_SRC% %LIZARD_SRC% %BROTLI_SRC% %ZSTD_SRC%
set TEST_SRC=
set THIRDPARTY_SRC=%LIB_THIRDPARTY_SRC% %BLAKE2_THIRDPARTY_SRC% %LIZARD_THIRDPARTY_SRC% %BROTLI_THIRDPARTY_SRC% %ZSTD_THIRDPARTY_SRC%
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /D__SSE2__
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D__SSE2__
rem /D /D_DEBUG /MDd
