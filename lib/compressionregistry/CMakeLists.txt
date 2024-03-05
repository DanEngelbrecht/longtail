add_library(compressionregistry_full
	OBJECT
	"longtail_compression_registry.c"
	"longtail_compression_registry.h"
	"longtail_full_compression_registry.c"
	"longtail_full_compression_registry.h"
)

add_library(compressionregistry_zstd
	OBJECT
	"longtail_compression_registry.c"
	"longtail_compression_registry.h"
	"longtail_zstd_compression_registry.c"
	"longtail_zstd_compression_registry.h"
)
