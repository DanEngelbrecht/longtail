set TARGET=test
set TARGET_TYPE=EXECUTABLE

call %BASE_DIR%all_sources.bat
call %BASE_DIR%default_build_options.bat

set MAIN_SRC=%BASE_DIR%test\main.cpp %BASE_DIR%test\test.cpp
