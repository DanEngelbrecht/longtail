@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=..\build\longtail_debug.exe

echo Indexing currently known chunks in "C:\Temp\longtail\chunks"
!LONGTAIL! --create-content-index "C:\Temp\longtail\chunks.lci" --content "C:\Temp\longtail\chunks"
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

GOTO Office
GOTO Home

:Home

GOTO End

:Office

call do_version.bat !LONGTAIL! C:\Temp\longtail WinClient\CL6332_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! C:\Temp\longtail WinClient\CL6333_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! C:\Temp\longtail WinClient\CL6336_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! C:\Temp\longtail WinClient\CL6338_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! C:\Temp\longtail WinClient\CL6339_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

Goto End

:End
echo "Done"
