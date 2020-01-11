set TARGET=longtail
set SRC=%BASE_DIR%src\*.c %BASE_DIR%cmd\main.cpp %BASE_DIR%lib\*.c %BASE_DIR%lib\bikeshed\*.c %BASE_DIR%lib\blake2\*.c %BASE_DIR%lib\filestorage\*.c %BASE_DIR%lib\lizard\*.c %BASE_DIR%lib\memstorage\*.c %BASE_DIR%lib\meowhash\*.c
set TEST_SRC=
set THIRDPARTY_SRC=%BASE_DIR%\src\ext\*.c %BASE_DIR%lib\blake2\ext\*.c %BASE_DIR%lib\lizard\ext\*.c %BASE_DIR%lib\lizard\ext\entropy\*.c %BASE_DIR%lib\lizard\ext\xxhash\*.c %BASE_DIR%lib\brotli\ext\common\*.c %BASE_DIR%lib\brotli\ext\dec\*.c %BASE_DIR%lib\brotli\ext\enc\*.c %BASE_DIR%lib\brotli\ext\fuzz\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /D__SSE2__
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D__SSE2__
rem /D /D_DEBUG /MDd
