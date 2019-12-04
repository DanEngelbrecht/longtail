@echo on
SetLocal EnableDelayedExpansion

if "%~5"=="" goto usage

set LONGTAIL=%1
set VERSION_NAME=%2
set SOURCE_FOLDER=%3
set CACHE_FOLDER=%4
set BUCKET=%5

echo LONGTAIL: %LONGTAIL%
echo VERSION_NAME: %VERSION_NAME%
echo SOURCE_FOLDER: %SOURCE_FOLDER%
echo CACHE_FOLDER: %CACHE_FOLDER%
echo BUCKET: %BUCKET%

if exist merged_store.lci del merged_store.lci
if exist remote_store.lci del remote_store.lci
call gsutil cp %BUCKET%/store.lci remote_store.lci
rem How do we make sure we only copy this if it exists? Should not generally be a problem but first time is!

if not exist %CACHE_FOLDER% mkdir %CACHE_FOLDER%
if exist upload_list.txt del upload_list.txt

%LONGTAIL% --upsync --version %SOURCE_FOLDER% --version-index %VERSION_NAME%.lvi --content-index "remote_store.lci" --missing-content %CACHE_FOLDER% --missing-content-index "new_content.lci" --output-format %CACHE_FOLDER%\{blockname} >upload_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if exist upload_list.txt type .\upload_list.txt | call gsutil -m cp -n -e -I %BUCKET%/store
rem call gsutil -m -q cp -r -n -e %CACHE_FOLDER%/**/*.lrb %BUCKET%/store/

call gsutil cp %VERSION_NAME%.lvi %BUCKET%/%VERSION_NAME%.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

if exist remote_store.lci %LONGTAIL% --create-content-index "merged_store.lci" --content-index "remote_store.lci" --merge-content-index "new_content.lci"
if %errorlevel% neq 0 exit /b %errorlevel%

if not exist remote_store.lci copy new_content.lci merged_store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

call gsutil cp merged_store.lci %BUCKET%/store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:usage

echo "gcs_put.bat <longtail-executable> <version_name> <source_folder> <cache_folder> <gcs_bucket_uri>"

:end
