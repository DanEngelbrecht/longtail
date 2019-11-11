@echo off
SetLocal EnableDelayedExpansion

echo Indexing currently known chunks
..\build\longtail --create-content-index "C:\Temp\longtail\chunks.lci" --content "C:\Temp\longtail\chunks"

call do_version.bat ..\build\longtail C:\Temp\longtail git75a99408249875e875f8fba52b75ea0f5f12a00e
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail C:\Temp\longtail gitb1d3adb4adce93d0f0aa27665a52be0ab0ee8b59
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

exit 0

call do_version.bat ..\build\longtail.exe D:\Temp\longtail gitfdeb1390885c2f426700ca653433730d1ca78dab
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe D:\Temp\longtail git81cccf054b23a0b5a941612ef0a2a836b6e02fd6
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe D:\Temp\longtail git558af6b2a10d9ab5a267b219af4f795a17cc032f
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail.exe D:\Temp\longtail gitc2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)
