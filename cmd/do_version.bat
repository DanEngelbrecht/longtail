@echo off
SetLocal EnableDelayedExpansion

echo -------------------- VERSION: %3 --------------------

echo Indexing version %2/local/%3_Win64_Editor
%1 --create-version-index "%2/%3_Win64_Editor.lvi" --version "%2/local/%3_Win64_Editor"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating content index for content not known to us from version %2/local/%3_Win64_Editor
%1 --create-content-index "%2/%3_Win64_Editor.lci" --content-index "%2/chunks.lci" --version-index "%2/%3_Win64_Editor.lvi" --version "%2/local/%3_Win64_Editor"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating chunks for content not known for version %2/local/%3_Win64_Editor
%1 --create-content "%2/%3_Win64_Editor_chunks" --content-index "%2/%3_Win64_Editor.lci" --version "%2/local/%3_Win64_Editor" --version-index "%2/%3_Win64_Editor.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Adding the new chunks from %2/%3_Win64_Editor_chunks to %2/chunks\
copy %2\%3_Win64_Editor_chunks\* %2\chunks\ > nul
if %errorlevel% neq 0 exit /b %errorlevel%

echo Merging the new chunk from %2/local/%3_Win64_Editor.lci int %2/chunks.lci
%1 --create-content-index "%2/chunks.lci" --content-index "%2/chunks.lci" --merge-content-index "%2/%3_Win64_Editor.lci"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Recreating %2/remote/%3_Win64_Editor
%1 --create-version "%2/remote/%3_Win64_Editor" --version-index "%2/%3_Win64_Editor.lvi" --content "%2/chunks" --content-index "%2/chunks.lci" --version "%2/remote/%3_Win64_Editor"
if %errorlevel% neq 0 exit /b %errorlevel%
