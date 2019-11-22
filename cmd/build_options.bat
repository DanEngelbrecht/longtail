set TARGET=longtail
set SRC=..\src\longtail.c main.cpp
set TEST_SRC=
set THIRDPARTY_SRC=nadir\src\nadir.cpp lizard\lib\*.c lizard\lib\entropy\*.c lizard\lib\xxhash\*.c trove\src\trove.cpp
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996 /DLONGTAIL_VERBOSE_LOGS
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_VERBOSE_LOGS /D_DEBUG /MDd
