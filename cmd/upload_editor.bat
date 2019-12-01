@echo on
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set WORK_FOLDER=%2
set VERSION=%3
set SOURCE_FOLDER=%4
set BUCKET=%5

rem gsutil cp !BUCKET!\store.lci !WORK_FOLDER!\remote_store.lci
del !WORK_FOLDER!\remote_store.lci
if exist "!BUCKET!\store.lci" copy !BUCKET!\store.lci !WORK_FOLDER!\remote_store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

!LONGTAIL! --upsync --version "!SOURCE_FOLDER!" --version-index "!WORK_FOLDER!\!VERSION!.lvi" --content-index "!WORK_FOLDER!\remote_store.lci" --upload-content "!WORK_FOLDER!\cache" --output-format "!WORK_FOLDER!\cache\{blockname}" >!WORK_FOLDER!\upload_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

if not exist "!WORK_FOLDER!\cache" mkdir "!WORK_FOLDER!\cache"

rem Replace with gsutil copy from upload_list.txt
for /f "delims=" %%f in (!WORK_FOLDER!\upload_list.txt) do (
    xcopy "%%f" "!BUCKET!\store\"
	if %errorlevel% neq 0 exit /b %errorlevel%
)

rem gsutil cp !WORK_FOLDER!\!VERSION!.lvi !BUCKET!\!VERSION!.lvi
copy !WORK_FOLDER!\!VERSION!.lvi !BUCKET!\!VERSION!.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

rem gsutil cp !WORK_FOLDER!\remote_store.lci !BUCKET!\store.lci
copy !WORK_FOLDER!\remote_store.lci !BUCKET!\store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end

C:\Dev\github\engelbd\longtail\cmd\upload_editor.bat C:\Dev\github\engelbd\longtail\build\longtail_debug.exe .\upsync\ git75a99408249875e875f8fba52b75ea0f5f12a00e_Win64_Editor ..\local\WinEditor\git75a99408249875e875f8fba52b75ea0f5f12a00e_Win64_Editor gcs_bucket


IF NOT EXIST "remote" mkdir "remote"
IF NOT EXIST "upload" mkdir "upload"

echo Indexing !SOURCE_FOLDER!
!LONGTAIL! --create-version-index "local_version.lvi" --version "!SOURCE_FOLDER!"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Fetching remote content index
rem We should download remote.lci
if NOT EXISTS "remote.lci" (
	!LONGTAIL! --create-content-index "remote\remote.lci"
	if %errorlevel% neq 0 exit /b %errorlevel%
)

echo Download remote content index
copy "remote\remote.lci" "remote.lci"

echo Analyzing missing content at remote
!LONGTAIL! --create-content-index "upload.lci" --version "!SOURCE_FOLDER!" --version-index "local_version.lvi" --content-index "remote.lci"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Create the missing content
!LONGTAIL! --create-content "upload" --content-index "upload.lci" --version "!SOURCE_FOLDER!" --version-index "upload.lci"

echo Creating new remote content index
!LONGTAIL! --create_content_index "remote.lci" --merge-content-index "remote.lci" --content-index "upload.lci"

echo Uploading content
IF NOT EXIST "remote\chunks" mkdir "remote\chunks"
copy upload\* remote\chunks

echo Cleaning upload folder
del upload\*.*

echo Upload new remote content index
copy "remote.lci" "remote\remote.lci"

echo Uploading version
copy "local_version.lvi" remote\!SOURCE_FOLDER!.lvi


:end
