add_library(
	hashregistry OBJECT
	"longtail_hash_registry.c"
	"longtail_hash_registry.h"

	"longtail_blake3_hash_registry.c"
	"longtail_blake3_hash_registry.h"
	"longtail_full_hash_registry.c"
	"longtail_full_hash_registry.h"
)
