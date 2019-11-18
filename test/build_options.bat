set TARGET=test
set SRC=
set TEST_SRC=..\src\longtail.c impl_bind.c test.cpp main.cpp
set THIRDPARTY_SRC=nadir\src\nadir.cpp lizard\lib\*.c lizard\lib\entropy\*.c lizard\lib\xxhash\*.c trove\src\trove.cpp
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS
