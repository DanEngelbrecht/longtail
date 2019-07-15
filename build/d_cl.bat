@echo off

set OPT=
set CXXFLAGS=/nologo /Zi /D_CRT_SECURE_NO_WARNINGS /D_HAS_EXCEPTIONS=0 /EHsc /W3 /wd5045 /wd4514 /wd4710 /wd4820 /wd4820 /wd4668 /wd4464 /wd5039 /wd4255 /wd4626
set OUTPUT=test_debug

call build_src.bat
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

set CXXFLAGS=%CXXFLAGS% %CXXFLAGS_DEBUG%

call build\mvsc_build.bat

exit /B %ERRORLEVEL%
