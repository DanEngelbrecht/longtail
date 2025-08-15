add_library(lz4 OBJECT
	"longtail_lz4.c"
	"longtail_lz4.h"

	"ext/lz4.c"
	"ext/lz4.h"
	"ext/lz4frame.c"
	"ext/lz4frame.h"
	"ext/lz4frame_static.h"
	"ext/lz4hc.c"
	"ext/lz4hc.h"
	"ext/xxhash.c"
	"ext/xxhash.h"
)
