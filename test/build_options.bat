set TARGET=test
set SRC=%BASE_DIR%\src\longtail.c %BASE_DIR%lib\longtail_lib.c %BASE_DIR%lib\longtail_platform.c
set TEST_SRC=%BASE_DIR%\test\impl_bind.c %BASE_DIR%\test\test.cpp %BASE_DIR%\test\main.cpp
set THIRDPARTY_SRC=%THIRDPARTY_DIR%lizard\lib\*.c %THIRDPARTY_DIR%lizard\lib\entropy\*.c %THIRDPARTY_DIR%lizard\lib\xxhash\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /DLONGTAIL_LOG_LEVEL=5
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D_DEBUG /DLONGTAIL_LOG_LEVEL=3 /MDd /DLONGTAIL_VERBOSE_LOGS
