@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=..\build\longtail_debug.exe
set BASEPATH=C:\Temp\longtail

echo Indexing currently known chunks
!LONGTAIL! --create-content-index "!BASEBATH!\chunks.lci" --content "!BASEBATH!\chunks"

GOTO Home
GOTO Office

:Home
call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\git75a99408249875e875f8fba52b75ea0f5f12a00e_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\gitb1d3adb4adce93d0f0aa27665a52be0ab0ee8b59_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

GOTO End

:Office

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\git916600e1ecb9da13f75835cd1b2d2e6a67f1a92d_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\gitfdeb1390885c2f426700ca653433730d1ca78dab_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\git81cccf054b23a0b5a941612ef0a2a836b6e02fd6_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\git558af6b2a10d9ab5a267b219af4f795a17cc032f_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEBATH! WinEditor\gitc2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c_Win64_Editor
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

Goto End

:End
echo "Done"
