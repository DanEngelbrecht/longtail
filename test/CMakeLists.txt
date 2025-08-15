add_executable(test_base
	"main.cpp"
	"test.cpp"

	"ext/jc_test.h"
)

target_link_libraries(test_base PRIVATE static_lib)
