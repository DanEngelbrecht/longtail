@echo off

set OPT=/O2
set CXXFLAGS=
set OUTPUT=test

call build_src.bat
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

call build\mvsc_build.bat
exit /B %ERRORLEVEL%
