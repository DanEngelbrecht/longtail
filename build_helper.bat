@echo off
SetLocal EnableDelayedExpansion

set BASE_DIR=%~dp0
set THIRDPARTY_DIR=!BASE_DIR!third-party\

:build_third_party_arg

if "%1%" == "build-third-party" (
    goto build_third_party
)
if "%2%" == "build-third-party" (
    goto build_third_party
)
if "%3%" == "build-third-party" (
    goto build_third_party
)

set BUILD_THIRD_PARTY=
goto release_arg

:build_third_party

set BUILD_THIRD_PARTY=build-third-party

:release_arg
if "%1%" == "release" (
    goto build_release_mode
)
if "%2%" == "release" (
    goto build_release_mode
)
if "%3%" == "release" (
    goto build_release_mode
)

set RELEASE_MODE=debug
goto arg_end

:build_release_mode

set RELEASE_MODE=release
goto arg_end

:arg_end

set BASE_CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626 /GR-

if "!RELEASE_MODE!" == "release" (
    set OPT=/O2 /Oi /Oy /GS- /Gs- /MT /GL /GS- /GF

    call build_options.bat
    set OUTPUT=!TARGET!
    set THIRD_PARTY_LIB=!TARGET!-third-party.lib
    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
    set CXXFLAGS=!BASE_CXXFLAGS! !CXXFLAGS!

) else (
    set OPT=/MTd

    call build_options.bat
    set OUTPUT=!TARGET!_debug
    set THIRD_PARTY_LIB=!TARGET!-third-party-debug.lib

    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

    set CXXFLAGS=!BASE_CXXFLAGS! !CXXFLAGS_DEBUG!
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2019\Professional\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Professional\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
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
    if exist "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" (
        call "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" amd64
    )
)

echo Visual Studio installed at %VCINSTALLDIR%

if NOT DEFINED VCINSTALLDIR (
    echo "No Visual Studio installation found, aborting, try running run vcvarsall.bat first!"
    exit 1
)


if NOT EXIST !BASE_DIR!build\third-party-!RELEASE_MODE!\!THIRD_PARTY_LIB! (
    set BUILD_THIRD_PARTY=build-third-party
)

IF NOT EXIST !BASE_DIR!build\third-party-!RELEASE_MODE! (
    mkdir !BASE_DIR!build\third-party-!RELEASE_MODE!
)

if "!BUILD_THIRD_PARTY!" == "build-third-party" (
    echo Compiling third party dependencies to library !THIRD_PARTY_LIB!
    del /q !BASE_DIR!build\third-party-!RELEASE_MODE!\*.obj >nul 2>&1
    cd !BASE_DIR!build\third-party-!RELEASE_MODE!
    cl.exe /c %CXXFLAGS% %OPT% %THIRDPARTY_SRC% %THIRDPARTY_SRC% %THIRDPARTY_SRC_SSE42% %THIRDPARTY_SRC_AVX2% %THIRDPARTY_SRC_AVX512%
    set LIB_COMPILE_ERROR=%ERRORLEVEL%
    echo Creating third party dependencies library !THIRD_PARTY_LIB!
    lib.exe /nologo *.obj /OUT:!BASE_DIR!build\third-party-!RELEASE_MODE!\!THIRD_PARTY_LIB!
    set LIB_BUILD_ERROR=%ERRORLEVEL%
    cd !BASE_DIR!
    if !LIB_COMPILE_ERROR! neq 0 exit /b !LIB_COMPILE_ERROR!
    if !LIB_BUILD_ERROR! neq 0 exit /b !LIB_BUILD_ERROR!
)

if "!TARGET_TYPE!" == "EXECUTABLE" (
    set OUTPUT_TARGET=!OUTPUT!.exe
)

if "!TARGET_TYPE!" == "SHAREDLIB" (
    set OUTPUT_TARGET=!OUTPUT!.dll
    set EXTRA_CC_OPTIONS=lib/D_USRDLL /D_WINDLL
    set EXTRA_LINK_OPTIONS=/pdbaltpath:%%_PDB%% /DLL /SUBSYSTEM:WINDOWS /NODEFAULTLIB:library
)

if "!TARGET_TYPE!" == "STATICLIB" (
    set OUTPUT_TARGET=!OUTPUT!_static.lib
)

cd !BASE_DIR!\build
echo Building %OUTPUT_TARGET%
if "!TARGET_TYPE!" == "EXECUTABLE" (
    cl.exe !EXTRA_CC_OPTIONS! %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:%OUTPUT%.pdb /link !EXTRA_LINK_OPTIONS! /out:!OUTPUT_TARGET! /pdb:%OUTPUT%.pdb !BASE_DIR!build\third-party-!RELEASE_MODE!\!THIRD_PARTY_LIB! /OPT:REF
)
if "!TARGET_TYPE!" == "SHAREDLIB" (
    cl.exe /D_USRDLL /D_WINDLL %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:%OUTPUT%.pdb /link /pdbaltpath:%%_PDB%% /DLL /SUBSYSTEM:WINDOWS /NODEFAULTLIB:library /out:!OUTPUT_TARGET! /pdb:%OUTPUT%.pdb !BASE_DIR!build\third-party-!RELEASE_MODE!\!THIRD_PARTY_LIB! /OPT:REF
)
if "!TARGET_TYPE!" == "STATICLIB" (
    IF NOT EXIST static-lib-!RELEASE_MODE! (
        mkdir static-lib-!RELEASE_MODE!
    )
    cd static-lib-!RELEASE_MODE! 
    cl.exe /c %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:%OUTPUT%.pdb
    cd ..
    lib -nologo -out:!OUTPUT_TARGET! static-lib-!RELEASE_MODE!\*.o !BASE_DIR!build\third-party-!RELEASE_MODE!\!THIRD_PARTY_LIB!
    cd !BASE_DIR!\build
)

set BUILD_ERROR=%ERRORLEVEL%
cd !BASE_DIR!

if !BUILD_ERROR! neq 0 exit /b !BUILD_ERROR!

:end
