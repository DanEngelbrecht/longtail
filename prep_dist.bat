@echo off
SetLocal EnableDelayedExpansion

call !BASE_DIR!arch_helper.bat

mkdir build\artifacts\artifacts-cmd-debug\!PLATFORM!\longtail\debug
copy build\!PLATFORM!\longtail\debug\*.exe build\artifacts\artifacts-cmd-debug\!PLATFORM!\longtail\debug
copy build\!PLATFORM!\longtail\debug\*.pdb build\artifacts\artifacts-cmd-debug\!PLATFORM!\longtail\debug

mkdir build\artifacts\artifacts-cmd-release\!PLATFORM!\longtail\release
copy build\!PLATFORM!\longtail\release\*.exe build\artifacts\artifacts-cmd-release\!PLATFORM!\longtail\release
copy build\!PLATFORM!\longtail\release\*.pdb build\artifacts\artifacts-cmd-release\!PLATFORM!\longtail\release

mkdir build\artifacts\artifacts-shared_lib-debug\!PLATFORM!\longtail_dylib\debug
copy build\!PLATFORM!\longtail_dylib\release\longtail_dylib.* build\artifacts\artifacts-shared_lib-debug\!PLATFORM!\longtail_dylib\debug
mkdir build\artifacts\artifacts-shared_lib-release\!PLATFORM!\longtail_dylib\release
copy build\!PLATFORM!\longtail_dylib\release\longtail_dylib.* build\artifacts\artifacts-shared_lib-release\!PLATFORM!\longtail_dylib\release

mkdir build\artifacts\artifacts-static_lib-debug\!PLATFORM!\longtail_static\debug
copy build\!PLATFORM!\longtail_static\debug\liblongtail_static.* build\artifacts\artifacts-static_lib-debug\!PLATFORM!\longtail_static\debug
mkdir build\artifacts\artifacts-static_lib-release\!PLATFORM!\longtail_static\release
copy build\!PLATFORM!\longtail_static\release\liblongtail_static.* build\artifacts\artifacts-static_lib-release\!PLATFORM!\longtail_static\release
