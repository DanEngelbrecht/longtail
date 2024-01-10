set TARGET=longtail
set TARGET_TYPE=EXECUTABLE

call %BASE_DIR%all_sources.bat
call %BASE_DIR%default_build_options.bat

set MINIFB_SRC=%BASE_DIR%cmd\lib\minifb\*.c
set MINIFB_THIRDPARTY_SRC=%BASE_DIR%cmd\lib\minifb\ext\minifb\src\*.c %BASE_DIR%cmd\lib\minifb\ext\minifb\src\windows\*.c

set SRC=%SRC% %MINIFB_SRC%
set THIRDPARTY_SRC=%THIRDPARTY_SRC% %MINIFB_THIRDPARTY_SRC%

set MAIN_SRC=%BASE_DIR%cmd\main.c
