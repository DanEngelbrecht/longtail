add_library(blake2 OBJECT
	"longtail_blake2.c"
	"longtail_blake2.h"
	"ext/blake2-config.h"
	"ext/blake2-impl.h"
	"ext/blake2.h"
	"ext/blake2s-load-sse2.h"
	"ext/blake2s-load-sse41.h"
	"ext/blake2s-round.h"
	"ext/blake2s.c"
)

if(MSVC)
	target_compile_options(blake2 INTERFACE /arch:SSE2)
	target_compile_options(blake2 PUBLIC -DHAVE_SSE2)
else()
	target_compile_options(blake2 PUBLIC -msse2)
endif(MSVC)
	