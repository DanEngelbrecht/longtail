@echo off
SetLocal EnableDelayedExpansion

echo Indexing currently known chunks
..\build\longtail_debug --create-content-index "C:\Temp\longtail\chunks.lci" --content "C:\Temp\longtail\chunks"

call do_version.bat ..\build\longtail_debug C:\Temp\longtail git75a99408249875e875f8fba52b75ea0f5f12a00e
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail_debug C:\Temp\longtail gitb1d3adb4adce93d0f0aa27665a52be0ab0ee8b59
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

GOTO End

call do_version.bat ..\build\longtail.exe C:\Temp\longtail git2f7f84a05fc290c717c8b5c0e59f8121481151e6
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe C:\Temp\longtail git916600e1ecb9da13f75835cd1b2d2e6a67f1a92d
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe C:\Temp\longtail gitfdeb1390885c2f426700ca653433730d1ca78dab
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe C:\Temp\longtail git81cccf054b23a0b5a941612ef0a2a836b6e02fd6
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe C:\Temp\longtail git558af6b2a10d9ab5a267b219af4f795a17cc032f
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe C:\Temp\longtail gitc2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

:End
echo "Done"
