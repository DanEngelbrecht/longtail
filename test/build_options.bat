set TARGET=test
set SRC=%BASE_DIR%\src\*.c %BASE_DIR%lib\*.c %BASE_DIR%lib\bikeshed\*.c %BASE_DIR%lib\blake2\*.c %BASE_DIR%lib\filestorage\*.c %BASE_DIR%lib\lizard\*.c %BASE_DIR%lib\brotli\*.c %BASE_DIR%lib\memstorage\*.c %BASE_DIR%lib\meowhash\*.c %BASE_DIR%lib\xxhash\*.c
set TEST_SRC=%BASE_DIR%\test\test.cpp %BASE_DIR%\test\main.cpp
set THIRDPARTY_SRC=%BASE_DIR%\src\ext\*.c %BASE_DIR%\src\ext\*.c %BASE_DIR%lib\brotli\ext\common\*.c %BASE_DIR%lib\brotli\ext\dec\*.c %BASE_DIR%lib\brotli\ext\enc\*.c %BASE_DIR%lib\brotli\ext\fuzz\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /DLONGTAIL_LOG_LEVEL=5 /D__SSE2__
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D_DEBUG /DLONGTAIL_LOG_LEVEL=3 /MDd /DLONGTAIL_VERBOSE_LOGS /D__SSE2__
