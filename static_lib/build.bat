@echo off
SetLocal EnableDelayedExpansion

If %PROCESSOR_ARCHITECTURE% == AMD64 (
    set ARCH=x64
) Else (
    set ARCH=x86
)
set OS=win32

set PLATFORM=%OS%_%ARCH%
set CXXFLAGS=-std=gnu99 -g -m64 -pthread -msse4.1 -maes -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00
set BASE_DIR=%~dp0..\
set LIB_TARGET_FOLDER=!BASE_DIR!build\
set OBJDIR=!BASE_DIR!build\static-lib

call ..\all_sources.bat

if "%1%" == "release" (
    goto build_release_mode
)

:build_debug_mode

set LIB_FILENAME=longtail_%PLATFORM%_debug.a
set OPT=
set OBJDIR=!BASE_DIR!build\static-lib-debug
set CXXFLAGS=!CXXFLAGS! -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS

goto build

:build_release_mode

set LIB_FILENAME=longtail_%PLATFORM%.a
set OPT=-O3

goto build

:build

set LIB_TARGET=%LIB_TARGET_FOLDER%%LIB_FILENAME%
echo Building %LIB_TARGET%

if exist !OBJDIR! rmdir /Q /S !OBJDIR!
mkdir !OBJDIR!

if not exist "!LIB_TARGET_FOLDER!" mkdir "!LIB_TARGET_FOLDER!"

pushd !OBJDIR!

gcc -c !CXXFLAGS! !OPT! !THIRDPARTY_SRC! !SRC!
if NOT "!THIRDPARTY_SRC_SSE42!" == "" (
	gcc -c !CXXFLAGS! !OPT! -msse4.2 %THIRDPARTY_SRC_SSE42%
)
if NOT "%THIRDPARTY_SRC_AVX2%" == "" (
	gcc -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 %THIRDPARTY_SRC_AVX2%
)
if NOT "%THIRDPARTY_SRC_AVX512%" == "" (
	gcc -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables %THIRDPARTY_SRC_AVX512%
)
popd

ar rc !LIB_TARGET! !OBJDIR!\*.o

echo Validating !LIB_TARGET!
gcc test.c -o !BASE_DIR!build\static_lib_test.exe -lm -L..\build -l:!LIB_FILENAME!
!BASE_DIR!build\static_lib_test.exe
