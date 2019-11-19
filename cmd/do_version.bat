@echo off
SetLocal EnableDelayedExpansion

echo -------------------- VERSION: %3 --------------------

echo Indexing version %2/local/%3
%1 --create-version-index "%2/%3.lvi" --version "%2/local/%3"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating content index for unknown content of version %2/local/%3
%1 --create-content-index "%2/%3.lci" --content-index "%2/chunks.lci" --version-index "%2/%3.lvi" --version "%2/local/%3"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating chunks for unknown content of version %2/local/%3
%1 --create-content "%2/%3_chunks" --content-index "%2/%3.lci" --version "%2/local/%3" --version-index "%2/%3.lvi"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Adding the new chunks from %2/%3_chunks to %2/chunks\
IF NOT EXIST "%2\chunks\" mkdir "%2\chunks"
copy %2\%3_chunks\* %2\chunks > nul

echo Merging the new chunks from %2/local/%3.lci into %2/chunks.lci
%1 --create-content-index "%2/chunks.lci" --content-index "%2/chunks.lci" --merge-content-index "%2/%3.lci"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating %2/remote/%3
%1 --create-version "%2/remote/%3" --version-index "%2/%3.lvi" --content "%2/chunks" --content-index "%2/chunks.lci" --version "%2/remote/%3"
if %errorlevel% neq 0 exit /b %errorlevel%
