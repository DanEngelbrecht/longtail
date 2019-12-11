set TARGET=longtail
set SRC=..\src\longtail.c ..\lib\longtail_lib.c ..\lib\longtail_platform.c main.cpp
set TEST_SRC=
set THIRDPARTY_SRC=lizard\lib\*.c lizard\lib\entropy\*.c lizard\lib\xxhash\*.c
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS /DLONGTAIL_ASSERTS
rem /D /D_DEBUG /MDd
