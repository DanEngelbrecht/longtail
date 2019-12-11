set TARGET=longtail
set SRC=%BASE_DIR%src\longtail.c %BASE_DIR%cmd\main.cpp %BASE_DIR%lib\longtail_lib.c %BASE_DIR%lib\longtail_platform.c
set TEST_SRC=
set THIRDPARTY_SRC=%THIRDPARTY_DIR%lizard\lib\*.c %THIRDPARTY_DIR%lizard\lib\entropy\*.c %THIRDPARTY_DIR%lizard\lib\xxhash\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS
rem /D /D_DEBUG /MDd
