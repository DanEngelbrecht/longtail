@echo on

set LONGTAIL=%1
set SOURCE_FOLDER=%2
set CACHE_FOLDER=%3
set BUCKET=%4

for /f %%f in ('dir /b %SOURCE_FOLDER%') do call .\gcs_put.bat %LONGTAIL% %%f.lvi %SOURCE_FOLDER%\%%f %CACHE_FOLDER% %BUCKET%

