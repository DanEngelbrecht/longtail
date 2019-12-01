@echo on
SetLocal EnableDelayedExpansion

set LONGTAIL=%1
set WORK_FOLDER=%2
set VERSION=%3
set TARGET_FOLDER=%4
set BUCKET=%5

rem gsutil cp !BUCKET!\store.lci !WORK_FOLDER!\remote_store.lci
del !WORK_FOLDER!\remote_store.lci
@copy !BUCKET!\store.lci !WORK_FOLDER!\remote_store.lci
if %errorlevel% neq 0 exit /b %errorlevel%

rem gsutil cp !BUCKET!\!VERSION!.lvi !WORK_FOLDER!\!VERSION!.lvi
@copy !BUCKET!\!VERSION!.lvi !WORK_FOLDER!\!VERSION!.lvi
if %errorlevel% neq 0 exit /b %errorlevel%

!LONGTAIL! --downsync --target-version-index "!WORK_FOLDER!\!VERSION!.lvi" --content "!WORK_FOLDER!\cache" --remote-content-index "!WORK_FOLDER!\remote_store.lci" --output-format "!BUCKET!\store\{blockname}" >download_list.txt
if %errorlevel% neq 0 exit /b %errorlevel%

@if not exist "!WORK_FOLDER!\cache" mkdir "!WORK_FOLDER!\cache"

rem Replace with gsutil copy from download_list.txt
@for /f "delims=" %%f in (download_list.txt) do (
    @xcopy "%%f" "!WORK_FOLDER!\cache\"
	if %errorlevel% neq 0 exit /b %errorlevel%
)

!LONGTAIL! --update-version "!TARGET_FOLDER!" --content "!WORK_FOLDER!\cache" --target-version-index "!WORK_FOLDER!\!VERSION!.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

GOTO end



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
