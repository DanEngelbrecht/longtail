set TARGET=longtail
set SRC=%BASE_DIR%src\*.c %BASE_DIR%src\ext\*.c %BASE_DIR%cmd\main.cpp %BASE_DIR%lib\*.c %BASE_DIR%lib\bikeshed\*.c %BASE_DIR%lib\blake2\*.c %BASE_DIR%lib\filestorage\*.c %BASE_DIR%lib\lizard\*.c %BASE_DIR%lib\memstorage\*.c %BASE_DIR%lib\meowhash\*.c %BASE_DIR%lib\xxhash\*.c
set TEST_SRC=
set THIRDPARTY_SRC=%THIRDPARTY_DIR%dummy.c
rem %THIRDPARTY_DIR%lizard\lib\*.c %THIRDPARTY_DIR%lizard\lib\entropy\*.c %THIRDPARTY_DIR%lizard\lib\xxhash\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /D__SSE2__
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D__SSE2__
rem /D /D_DEBUG /MDd
