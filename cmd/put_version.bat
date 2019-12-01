@echo on
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set VERSION_NAME=%2
set SOURCE_FOLDER=%3
set CACHE_FOLDER=%4
set BUCKET=%5

del remote_store.lci
rem gsutil cp !BUCKET!\store.lci remote_store.lci
if exist "!BUCKET!\store.lci" copy !BUCKET!\store.lci remote_store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

!LONGTAIL! --upsync --version "!SOURCE_FOLDER!" --version-index "!VERSION_NAME!.lvi" --content-index "remote_store.lci" --upload-content "!CACHE_FOLDER!" --output-format "!CACHE_FOLDER!\{blockname}" >upload_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if not exist "!CACHE_FOLDER!" mkdir "!CACHE_FOLDER!"

rem Replace with gsutil copy from upload_list.txt
for /f "delims=" %%f in (upload_list.txt) do (
    xcopy "%%f" "!BUCKET!\store\"
	if %errorlevel% neq 0 exit /b %errorlevel%
)

rem gsutil cp !VERSION_NAME!.lvi !BUCKET!\!VERSION_NAME!.lvi
copy !VERSION_NAME!.lvi !BUCKET!\!VERSION_NAME!.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

rem gsutil cp remote_store.lci !BUCKET!\store.lci
copy remote_store.lci !BUCKET!\store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

:end
