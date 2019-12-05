set TARGET=test
set SRC=%BASE_DIR%\src\longtail.c %BASE_DIR%lib\longtail_lib.cpp
set TEST_SRC=%BASE_DIR%\test\impl_bind.c %BASE_DIR%\test\test.cpp %BASE_DIR%\test\main.cpp
set THIRDPARTY_SRC=%THIRDPARTY_DIR%nadir\src\nadir.cpp %THIRDPARTY_DIR%lizard\lib\*.c %THIRDPARTY_DIR%lizard\lib\entropy\*.c %THIRDPARTY_DIR%lizard\lib\xxhash\*.c %THIRDPARTY_DIR%trove\src\trove.cpp
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS /D_DEBUG /DLONGTAIL_VERBOSE_LOGS /MDd /DLONGTAIL_VERBOSE_LOGS
