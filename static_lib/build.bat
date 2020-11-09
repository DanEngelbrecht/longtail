@echo off
SetLocal EnableDelayedExpansion

SET BUILDFOLDER=%~dp0
FOR %%a IN ("%BUILDFOLDER:~0,-1%") DO SET BASE_DIR=%%~dpa

If %PROCESSOR_ARCHITECTURE% == AMD64 (
    set ARCH=x64
) Else (
    set ARCH=x86
)
set OS=win32

set PLATFORM=%OS%_%ARCH%
set CXXFLAGS=-std=gnu99 -g -m64 -maes -mssse3 -msse4.1 -pthread  -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00
set LIB_TARGET_FOLDER=!BASE_DIR!build\static\

call !BASE_DIR!all_sources.bat

if "%1%" == "release" (
    goto build_release_mode
)

goto build_debug_mode

:build_release_mode

set LIB_FILENAME=longtail_%PLATFORM%
set OPT=-O3
set OBJDIR=!BASE_DIR!build\static-lib-release

goto build

:build_debug_mode

set LIB_FILENAME=longtail_%PLATFORM%_debug
set OPT=
set OBJDIR=!BASE_DIR!build\static-lib-debug
set CXXFLAGS=!CXXFLAGS! -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS

goto build

:build

set LIB_TARGET=%LIB_TARGET_FOLDER%lib%LIB_FILENAME%.a

echo Building %LIB_TARGET%

if exist !LIB_TARGET! del !LIB_TARGET!

if not exist "!LIB_TARGET_FOLDER!" mkdir "!LIB_TARGET_FOLDER!"

if exist !OBJDIR! rmdir /Q /S !OBJDIR!
mkdir !OBJDIR!

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

set TEST_EXECUTABLEPATH=%BASE_DIR%build\static_lib_test.exe

ar cru -v !LIB_TARGET! !OBJDIR!\*.o
ls -la ${LIB_TARGET}

echo Validating !LIB_TARGET!
gcc -o %TEST_EXECUTABLEPATH% !CXXFLAGS! !BUILDFOLDER!test.c -lm -L%LIB_TARGET_FOLDER% -l!LIB_FILENAME! --verbose
%TEST_EXECUTABLEPATH%
