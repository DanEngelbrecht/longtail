@echo off
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set VERSION_NAME=%2
set TARGET_FOLDER=%4
set CACHE_FOLDER=%2
set BUCKET=%5

del remote_store.lci
rem gsutil cp !BUCKET!\store.lci remote_store.lci
@copy !BUCKET!\store.lci remote_store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

rem gsutil cp !BUCKET!\!VERSION_NAME!.lvi !VERSION_NAME!.lvi
@copy !BUCKET!\!VERSION_NAME!.lvi !VERSION_NAME!.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

!LONGTAIL! --downsync --target-version-index "!VERSION_NAME!.lvi" --content "!CACHE_FOLDER!" --remote-content-index "remote_store.lci" --output-format "!BUCKET!\store\{blockname}" >download_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

@if not exist "!CACHE_FOLDER!" mkdir "!CACHE_FOLDER!"

rem Replace with gsutil copy from download_list.txt
@for /f "delims=" %%f in (download_list.txt) do (
    @xcopy "%%f" "!CACHE_FOLDER!\"
	if %errorlevel% neq 0 exit /b %errorlevel%
)

!LONGTAIL! --update-version "!TARGET_FOLDER!" --content "!CACHE_FOLDER!" --target-version-index "!VERSION_NAME!.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:end
