@echo off
SetLocal EnableDelayedExpansion

echo Indexing currently known chunks
..\build\longtail.exe --create-content-index "D:\Temp\longtail\chunks.lci" --content "D:\Temp\longtail\chunks"

call do_version.bat ..\build\longtail.exe D:\Temp\longtail git2f7f84a05fc290c717c8b5c0e59f8121481151e6
call do_version.bat ..\build\longtail.exe D:\Temp\longtail git916600e1ecb9da13f75835cd1b2d2e6a67f1a92d
call do_version.bat ..\build\longtail.exe D:\Temp\longtail gitfdeb1390885c2f426700ca653433730d1ca78dab
call do_version.bat ..\build\longtail.exe D:\Temp\longtail git81cccf054b23a0b5a941612ef0a2a836b6e02fd6
call do_version.bat ..\build\longtail.exe D:\Temp\longtail git558af6b2a10d9ab5a267b219af4f795a17cc032f
call do_version.bat ..\build\longtail.exe D:\Temp\longtail gitc2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c

if %errorlevel% neq 0 echo "FAILED"

rem ..\build\longtail_debug.exe --create-version-index base.lvi --version D:\Temp\longtail\local\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor
rem ..\build\longtail_debug.exe --create-version-index target.lvi --version D:\Temp\longtail\local\git558af6b2a10d9ab5a267b219af4f795a17cc032f_Win64_Editor\
rem ..\build\longtail_debug.exe --create-content-index base.lci --version-index base.lvi --version D:\Temp\longtail\local\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor
rem ..\build\longtail_debug.exe --create-content-index target.lci --version-index target.lvi --version D:\Temp\longtail\local\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor --content-index base.lci
rem ..\build\longtail_debug.exe --list-missing-blocks "base.lci" --content-index "target.lci"
