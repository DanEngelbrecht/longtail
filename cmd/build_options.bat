set TARGET=longtail
set SRC=%BASE_DIR%src\longtail.c %BASE_DIR%cmd\main.cpp %BASE_DIR%lib\longtail_lib.cpp
set TEST_SRC=
set THIRDPARTY_SRC=%THIRDPARTY_DIR%nadir\src\nadir.cpp %THIRDPARTY_DIR%lizard\lib\*.c %THIRDPARTY_DIR%lizard\lib\entropy\*.c %THIRDPARTY_DIR%lizard\lib\xxhash\*.c %THIRDPARTY_DIR%trove\src\trove.cpp
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /DLONGTAIL_VERBOSE_LOGS
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_VERBOSE_LOGS /DLONGTAIL_ASSERTS
rem /D /D_DEBUG /MDd
