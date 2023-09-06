@echo off
SetLocal EnableDelayedExpansion

SET SOURCEFOLDER=%~dp0
FOR %%a IN ("%SOURCEFOLDER:~0,-1%") DO SET BASE_DIR=%%~dpa

call !BASE_DIR!arch_helper.bat

set CXXFLAGS=-std=gnu99 -g -m64 -maes -mssse3 -msse4.1 -pthread  -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00
set TARGET=longtail_static

call !BASE_DIR!all_sources.bat

if "%1%" == "release" (
    goto build_release_mode
)

goto build_debug_mode

:build_release_mode

set RELEASE_MODE=release
set OPT=-O3

goto build

:build_debug_mode

set RELEASE_MODE=debug
set OPT=
set CXXFLAGS=!CXXFLAGS! -DLONGTAIL_ASSERTS -DBIKESHED_ASSERTS

goto build

:build

set OUTPUT_FOLDER=!BASE_DIR!build\!PLATFORM!\!TARGET!\!RELEASE_MODE!
if NOT EXIST !OUTPUT_FOLDER! (
    mkdir !OUTPUT_FOLDER!
)

set LIB_TARGET=!OUTPUT_FOLDER!\lib!TARGET!.a

echo Building !LIB_TARGET!

if exist !LIB_TARGET! del !LIB_TARGET!

del /q !OUTPUT_FOLDER!\*.o >nul 2>&1

pushd !OUTPUT_FOLDER!
!COMPILER! -c !CXXFLAGS! !OPT! !THIRDPARTY_SRC! !SRC!
if NOT "!THIRDPARTY_SRC_SSE42!" == "" (
    !COMPILER! -c !CXXFLAGS! !OPT! -msse4.2 %THIRDPARTY_SRC_SSE42%
)
if NOT "%THIRDPARTY_SRC_AVX2%" == "" (
    !COMPILER! -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 %THIRDPARTY_SRC_AVX2%
)
if NOT "%THIRDPARTY_SRC_AVX512%" == "" (
    !COMPILER! -c !CXXFLAGS! !OPT! -msse4.2 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables %THIRDPARTY_SRC_AVX512%
)
if NOT "%ZSTD_THIRDPARTY_GCC_SRC%" == "" (
    !COMPILER! -c !CXXFLAGS! !OPT! -fno-asynchronous-unwind-tables %ZSTD_THIRDPARTY_GCC_SRC%
)

popd

set TEST_EXECUTABLEPATH=!OUTPUT_FOLDER!\!TARGET!_test.exe

ar cru -v !LIB_TARGET! !OUTPUT_FOLDER!\*.o
ls -la ${LIB_TARGET}

echo Validating !LIB_TARGET!
!COMPILER! -o !TEST_EXECUTABLEPATH! !CXXFLAGS! !SOURCEFOLDER!test.c -lm -L!OUTPUT_FOLDER! -l!TARGET! --verbose
!TEST_EXECUTABLEPATH!
