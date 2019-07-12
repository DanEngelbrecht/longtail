@echo off

set OPT=
set CXXFLAGS=%CXXFLAGS% /DBIKESHED_ASSERTS
set OUTPUT=test_debug

call build_src.bat
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

call build\mvsc_build.bat
exit /B %ERRORLEVEL%
