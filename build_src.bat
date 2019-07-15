set SRC=
set TEST_SRC=test\test.cpp test\main.cpp
set THIRDPARTY_SRC=third-party\nadir\src\nadir.cpp third-party\lizard\lib\*.c third-party\lizard\lib\entropy\*.c third-party\lizard\lib\xxhash\*.c third-party\trove\src\trove.cpp
set CXXFLAGS=%CXXFLAGS% /wd4244 /wd4316 /wd4996
set CXXFLAGS_DEBUG=%CXXFLAGS_DEBUG% /DBIKESHED_ASSERTS

