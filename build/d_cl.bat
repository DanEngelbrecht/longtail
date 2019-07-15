@echo off

set OPT=
set OUTPUT=test_debug

call build_src.bat
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

call build\mvsc_build.bat

set CXXFLAGS=%CXXFLAGS% %CXXFLAGS_DEBUG%

exit /B %ERRORLEVEL%
