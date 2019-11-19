@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=..\build\longtail_debug.exe
set BASEPATH=C:\Temp\longtail

echo Indexing currently known chunks in "!BASEPATH!\chunks"
!LONGTAIL! --create-content-index "!BASEPATH!\chunks.lci" --content "!BASEPATH!\chunks"
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

GOTO Office
GOTO Home

:Home

GOTO End

:Office

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6332_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6333_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6336_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6338_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6339_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

Goto End

:End
echo "Done"
