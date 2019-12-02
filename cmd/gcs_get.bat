@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set VERSION_NAME=%2
set TARGET_FOLDER=%3
set CACHE_FOLDER=%4
set BUCKET=%5

if exist remote_store.lci del remote_store.lci
call gsutil cp %5/store.lci remote_store.lci
rem How do we make sure we only copy this if it exists? Should not generally be a problem but first time is!

if not exist %2.lvi call gsutil cp %5/%2.lvi %2.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

@if not exist "%4" mkdir "%4"

%1 --downsync --target-version-index "%2.lvi" --content "%4" --remote-content-index "remote_store.lci" --output-format "%5/store/{blockname}" >download_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if exist download_list.txt type .\download_list.txt | call gsutil -m cp -I %4

%1 --update-version "%3" --content "%4" --target-version-index "%2.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:end
