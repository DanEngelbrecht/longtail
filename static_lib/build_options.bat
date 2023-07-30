set TARGET=longtail_static
set TARGET_TYPE=STATICLIB

call %BASE_DIR%all_sources.bat
call %BASE_DIR%default_build_options.bat

set MAIN_SRC=%BASE_DIR%static_lib\test.c
