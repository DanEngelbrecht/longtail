add_library(
	meowhash OBJECT
	
	"longtail_meowhash.c"
	"longtail_meowhash.h"

	"ext/meow_hash_x64_aesni.h"
)

if(MSVC)
	target_compile_options(blake2 INTERFACE /arch:SSE2)
	target_compile_options(blake2 PUBLIC -DHAVE_SSE2)
else()
	target_compile_options(meowhash PUBLIC -maes -mavx2)
endif(MSVC)
