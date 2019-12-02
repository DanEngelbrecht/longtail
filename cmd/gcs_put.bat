@echo on
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set VERSION_NAME=%2
set SOURCE_FOLDER=%3
set CACHE_FOLDER=%4
set BUCKET=%5

if exist remote_store.lci del remote_store.lci
call gsutil cp %5/store.lci remote_store.lci
rem How do we make sure we only copy this if it exists? Should not generally be a problem but first time is!

if not exist %4 mkdir %4
if exist upload_list.txt del upload_list.txt

%1 --upsync --version "%3" --version-index "%2.lvi" --content-index "remote_store.lci" --upload-content "%4" --output-format "%4\{blockname}" >upload_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if exist upload_list.txt type .\upload_list.txt | call gsutil -m cp -I %5/store

call gsutil cp %2.lvi %5/%2.lvi
rem copy %2%.lvi %5%\%2%.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

if exist remote_store.lci call gsutil cp remote_store.lci %5/store.lci
rem copy remote_store.lci %5%\store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:end
