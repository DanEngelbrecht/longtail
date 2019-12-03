@echo off
SetLocal EnableDelayedExpansion

if "%~5"=="" goto usage

set LONGTAIL=%1
set VERSION_NAME=%2
set TARGET_FOLDER=%3
set CACHE_FOLDER=%4
set BUCKET=%5

if exist remote_store.lci del remote_store.lci
call gsutil cp %BUCKET%/store.lci remote_store.lci
rem How do we make sure we only copy this if it exists? Should not generally be a problem but first time is!

if not exist %VERSION_NAME%.lvi call gsutil cp %BUCKET%/%VERSION_NAME%.lvi %VERSION_NAME%.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

@if not exist %CACHE_FOLDER% mkdir %CACHE_FOLDER%

%LONGTAIL% --downsync --target-version-index "%VERSION_NAME%.lvi" --content %CACHE_FOLDER% --remote-content-index "remote_store.lci" --output-format %BUCKET%/store/{blockname} >download_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if exist download_list.txt type .\download_list.txt | call gsutil -m cp -I %CACHE_FOLDER%

%LONGTAIL% --update-version %TARGET_FOLDER% --content %CACHE_FOLDER% --target-version-index "%VERSION_NAME%.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:usage

echo "gcs_get.bat <longtail-executable> <version_name> <target_folder> <cache_folder> <gcs_bucket_uri>"

:end
