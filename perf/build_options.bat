set TARGET=perf
set TARGET_TYPE=EXECUTABLE

call ..\all_sources.bat
call ..\default_build_options.bat

set MAIN_SRC=%BASE_DIR%perf\main.cpp
