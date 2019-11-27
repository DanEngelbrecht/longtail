@echo off
SetLocal EnableDelayedExpansion

if "%1%" == "build-third-party" (
    set BUILD_THIRD_PARTY=%1%
    set RELEASE_MODE=%2%
) else (
    set BUILD_THIRD_PARTY=
    set RELEASE_MODE=%1%
)

if "!RELEASE_MODE!" == "release" (
    set OPT=/O2 /Oi /Oy
    set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626

    call build_options.bat
    set OUTPUT=!TARGET!
    set THIRD_PARTY_LIB=!TARGET!-third-party.lib
    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

) else (
    set OPT=
    set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626

    call build_options.bat
    set OUTPUT=!TARGET!_debug
    set THIRD_PARTY_LIB=!TARGET!-third-party-debug.lib

    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

    set CXXFLAGS=!CXXFLAGS! !CXXFLAGS_DEBUG!
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2019\Professional\\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Professional\\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
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


if NOT EXIST ..\build\!THIRD_PARTY_LIB! (
    set BUILD_THIRD_PARTY=build-third-party
)

IF NOT EXIST ..\build (
    mkdir ..\build
)

if "!BUILD_THIRD_PARTY!" == "build-third-party" (
    echo "Compiling third party dependencies to library" %THIRD_PARTY_LIB%
    del /s ..\build\!THIRD_PARTY_LIB! >nul 2>&1
    pushd ..\third-party
    cl.exe /c %CXXFLAGS% %OPT% %THIRDPARTY_SRC% /Fd:..\build\test_vc.pdb
    set LIB_COMPILE_ERROR=%ERRORLEVEL%
    popd
    if !LIB_COMPILE_ERROR! neq 0 exit /b !LIB_COMPILE_ERROR!
    lib.exe /nologo ..\third-party\*.obj /OUT:..\build\!THIRD_PARTY_LIB!
    del ..\third-party\*.obj
)

cl.exe %CXXFLAGS% %OPT% %SRC% %TEST_SRC% /Fo:..\build\ /Fd:..\build\test_vc.pdb /link /out:..\build\%OUTPUT%.exe /pdb:..\build\%OUTPUT%.pdb ..\build\!THIRD_PARTY_LIB!

exit /B %ERRORLEVEL%
