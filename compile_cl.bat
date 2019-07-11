@echo off

call .\find_mvsc.bat

if NOT DEFINED VCINSTALLDIR (
    echo "No Visual Studio installation found, aborting, try running run vcvarsall.bat first!"
    exit 1
)

IF NOT EXIST build (
    mkdir build
)

pushd build

set OPT=/O2
set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626
set SRC=
set TEST_SRC=..\test\test.cpp ..\test\main.cpp
set THIRDPARTY_SRC=..\third-party\nadir\src\nadir.cpp ..\third-party\lizard\lib\*.c ..\third-party\lizard\lib\entropy\*.c ..\third-party\lizard\lib\xxhash\*.c ..\third-party\trove\src\trove.cpp

cl.exe %CXXFLAGS% %OPT% %SRC% %TEST_SRC% %THIRDPARTY_SRC% /link /out:test_debug.exe /pdb:test_debug.pdb

popd

exit /B %ERRORLEVEL%
