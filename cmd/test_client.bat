@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=..\build\longtail.exe
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

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6465_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6467_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6469_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

GOTO End

:Office

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6135_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6157_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6203_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6226_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6308_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

call do_version.bat !LONGTAIL! !BASEPATH! WinClient\CL6382_WindowsClient
if %errorlevel% neq 0 (
    echo "FAILED:" %errorlevel%
    exit /b %errorlevel%
)

Goto End

:End
echo "Done"
