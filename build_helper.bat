@echo off
SetLocal EnableDelayedExpansion

set BASE_DIR=%~dp0
set SOURCE_FOLDER=%1%

call !BASE_DIR!arch_helper.bat

:run_arg

if "%2%" == "run" (
    goto run
)
if "%3%" == "run" (
    goto run
)
if "%4%" == "run" (
    goto run
)

set RUN=
goto build_third_party_arg

:run
set RUN=run

:build_third_party_arg

if "%2%" == "build-third-party" (
    goto build_third_party
)
if "%3%" == "build-third-party" (
    goto build_third_party
)
if "%4%" == "build-third-party" (
    goto build_third_party
)

set BUILD_THIRD_PARTY=
goto release_arg

:build_third_party

set BUILD_THIRD_PARTY=build-third-party

:release_arg
if "%2%" == "release" (
    goto build_release_mode
)
if "%3%" == "release" (
    goto build_release_mode
)
if "%4%" == "release" (
    goto build_release_mode
)

set RELEASE_MODE=debug
goto arg_end

:build_release_mode

set RELEASE_MODE=release
goto arg_end

:arg_end

set BASE_CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626 /GR-

call !SOURCE_FOLDER!build_options.bat
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

set OUTPUT_FOLDER=!BASE_DIR!build\!PLATFORM!\!TARGET!\!RELEASE_MODE!
if NOT EXIST !OUTPUT_FOLDER! (
    mkdir !OUTPUT_FOLDER!
)

set THIRD_PARTY_OUTPUT_FOLDER=!OUTPUT_FOLDER!\third-party
IF NOT EXIST !THIRD_PARTY_OUTPUT_FOLDER! (
    mkdir !THIRD_PARTY_OUTPUT_FOLDER!
)

set THIRD_PARTY_LIB=!TARGET!-third-party.lib

if "!RELEASE_MODE!" == "release" (
    set OPT=/O2 /Oi /Oy /GS- /Gs- /MT /GL /GS- /GF
    set CXXFLAGS=!BASE_CXXFLAGS! !CXXFLAGS!
) else (
    set OPT=/MTd
    set CXXFLAGS=!BASE_CXXFLAGS! !CXXFLAGS_DEBUG!
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat" x86_amd64
    )
)

if NOT DEFINED VCINSTALLDIR (
    if exist "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" (
        call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" amd64
    )
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

if NOT EXIST !THIRD_PARTY_OUTPUT_FOLDER!\!THIRD_PARTY_LIB! (
    set BUILD_THIRD_PARTY=build-third-party
)

if "!BUILD_THIRD_PARTY!" == "build-third-party" (
    echo Compiling third party dependencies to library !THIRD_PARTY_LIB!
    del /q !THIRD_PARTY_OUTPUT_FOLDER!\*.obj >nul 2>&1
    cd !THIRD_PARTY_OUTPUT_FOLDER!
    cl.exe /c %CXXFLAGS% %OPT% %THIRDPARTY_SRC% %THIRDPARTY_SRC_SSE% %THIRDPARTY_SRC_SSE42% %THIRDPARTY_SRC_AVX2% %THIRDPARTY_SRC_AVX512%
    set LIB_COMPILE_ERROR=%ERRORLEVEL%
    echo Creating third party dependencies library !THIRD_PARTY_LIB!
    lib.exe /nologo *.obj /OUT:!THIRD_PARTY_OUTPUT_FOLDER!\!THIRD_PARTY_LIB!
    set LIB_BUILD_ERROR=%ERRORLEVEL%
    cd !BASE_DIR!
    if !LIB_COMPILE_ERROR! neq 0 exit /b !LIB_COMPILE_ERROR!
    if !LIB_BUILD_ERROR! neq 0 exit /b !LIB_BUILD_ERROR!
)

if "!TARGET_TYPE!" == "EXECUTABLE" (
    set OUTPUT_TARGET=!TARGET!.exe
)

if "!TARGET_TYPE!" == "SHAREDLIB" (
    set OUTPUT_TARGET=!TARGET!.dll
    set EXTRA_CC_OPTIONS=lib/D_USRDLL /D_WINDLL
    set EXTRA_LINK_OPTIONS=/pdbaltpath:%%_PDB%% /DLL /SUBSYSTEM:WINDOWS /NODEFAULTLIB:library
)

if "!TARGET_TYPE!" == "STATICLIB" (
    set OUTPUT_TARGET=!TARGET!_static.lib
)

cd !OUTPUT_FOLDER!
echo Building %OUTPUT_TARGET%
if "!TARGET_TYPE!" == "EXECUTABLE" (
    cl.exe !EXTRA_CC_OPTIONS! %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:!TARGET!.pdb /link !EXTRA_LINK_OPTIONS! /out:!OUTPUT_TARGET! /pdb:!TARGET!.pdb !THIRD_PARTY_OUTPUT_FOLDER!\!THIRD_PARTY_LIB! /OPT:REF
)

if "!TARGET_TYPE!" == "SHAREDLIB" (
    cl.exe /D_USRDLL /D_WINDLL %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:!TARGET!.pdb /link /pdbaltpath:%%_PDB%% /DLL /SUBSYSTEM:WINDOWS /NODEFAULTLIB:library /out:!OUTPUT_TARGET! /pdb:!TARGET!.pdb !THIRD_PARTY_OUTPUT_FOLDER!\!THIRD_PARTY_LIB! /OPT:REF
)

if "!TARGET_TYPE!" == "STATICLIB" (
    IF NOT EXIST static-lib (
        mkdir static-lib
    )
    cd static-lib 
    cl.exe /c %CXXFLAGS% %OPT% %SRC% %MAIN_SRC% /Fd:!TARGET!.pdb
    cd ..
    lib -nologo -out:!OUTPUT_TARGET! *.o !THIRD_PARTY_OUTPUT_FOLDER!\!THIRD_PARTY_LIB!
)

set BUILD_ERROR=%ERRORLEVEL%
cd !BASE_DIR!

if !BUILD_ERROR! neq 0 exit /b !BUILD_ERROR!

if "!TARGET_TYPE!" == "EXECUTABLE" (
    if "!RUN!" == "run" (
        cd !SOURCE_FOLDER!
        !OUTPUT_FOLDER!\!OUTPUT_TARGET!
        cd ..
    )
)

:end
