@echo off
SetLocal EnableDelayedExpansion

if "%1%" = "build-third-party" (
    set BUILD_THIRD_PARTY=%1%
    set RELEASE_MODE=%2%
) else (
    set BUILD_THIRD_PARTY=
    set RELEASE_MODE=%1%
)

if !RELEASE! == "release" (
    set OPT=/O2
    set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626
    set OUTPUT=test
    set THIRD_PARTY_LIB=third-party.lib

    call build_options.bat
    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

) else (
    set OPT=
    set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626
    set OUTPUT=test_debug
    set THIRD_PARTY_LIB=third-party-debug.lib

    call build_options.bat
    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

    set CXXFLAGS=!CXXFLAGS! !CXXFLAGS_DEBUG!
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio 15.0\VC\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio 15.0\VC\vcvarsall.bat" amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio 13.0\VC\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio 13.0\VC\vcvarsall.bat" amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" amd64
    )
)

echo Visual Studio installed at %VCINSTALLDIR%

if NOT DEFINED VCINSTALLDIR (
    echo "No Visual Studio installation found, aborting, try running run vcvarsall.bat first!"
    exit 1
)

if NOT EXIST build/!THIRD_PARTY_LIB! (
    set BUILD_THIRD_PARTY=build-third-party
)

IF NOT EXIST build (
    mkdir build
)

if !BUILD_THIRD_PARTY! == "build-third-party" (
    echo "Compiling third party dependencies to library" %THIRD_PARTY_LIB%
)

cl.exe %CXXFLAGS% %OPT% %SRC% %TEST_SRC% %THIRDPARTY_SRC% /Fd:build\test_vc.pdb /link /out:build\%OUTPUT%.exe /pdb:build\%OUTPUT%.pdb

exit /B %ERRORLEVEL%
