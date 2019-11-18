@echo off
SetLocal EnableDelayedExpansion

echo Indexing currently known chunks
..\build\longtail_debug --create-content-index "C:\Temp\longtail\chunks.lci" --content "C:\Temp\longtail\chunks"

GOTO Office
GOTO Home

:Home

GOTO End

:Office

call do_version.bat ..\build\longtail_debug C:\Temp\longtail WinClient\CL6332_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail_debug C:\Temp\longtail WinClient\CL6333_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail_debug.exe C:\Temp\longtail WinClient\CL6336_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail_debug.exe C:\Temp\longtail WinClient\CL6338_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat ..\build\longtail_debug.exe C:\Temp\longtail WinClient\CL6339_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

Goto End

:End
echo "Done"
