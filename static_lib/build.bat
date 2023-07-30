@echo off

SET BUILDFOLDER=%~dp0
FOR %%a IN ("%BUILDFOLDER:~0,-1%") DO SET HELPERFOLDER=%%~dpa

call %HELPERFOLDER%build_helper.bat %BUILDFOLDER% %*
