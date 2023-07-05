# CMakeList.txt : CMake project for longtail, include source and define
# project specific logic here.
#

# Add source to this project's executable.
add_executable (longtail "main.c" "ext/kgflags.h")

if (CMAKE_VERSION VERSION_GREATER 3.12)
  set_property(TARGET longtail PROPERTY CXX_STANDARD 20)
endif()

# TODO: Add tests and install targets if needed.
target_link_libraries(longtail PRIVATE static_lib)

