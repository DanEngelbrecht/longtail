if(MSVC)
else()
	set (zstd_platform_sources "ext/decompress/huf_decompress_amd64.S")
endif()


add_library(
	zstd OBJECT
	
	"longtail_zstd.c"
	"longtail_zstd.h"

	"ext/common/bits.h"
	"ext/common/bitstream.h"
	"ext/common/compiler.h"
	"ext/common/cpu.h"
	"ext/common/debug.c"
	"ext/common/debug.h"
	"ext/common/entropy_common.c"
	"ext/common/error_private.c"
	"ext/common/error_private.h"
	"ext/common/fse.h"
	"ext/common/fse_decompress.c"
	"ext/common/huf.h"
	"ext/common/mem.h"
	"ext/common/pool.c"
	"ext/common/pool.h"
	"ext/common/portability_macros.h"
	"ext/common/threading.c"
	"ext/common/threading.h"
	"ext/common/xxhash.c"
	"ext/common/xxhash.h"
	"ext/common/zstd_common.c"
	"ext/common/zstd_deps.h"
	"ext/common/zstd_internal.h"
	"ext/common/zstd_trace.h"

	"ext/compress/clevels.h"
	"ext/compress/fse_compress.c"
	"ext/compress/hist.c"
	"ext/compress/hist.h"
	"ext/compress/huf_compress.c"
	"ext/compress/zstdmt_compress.c"
	"ext/compress/zstdmt_compress.h"
	"ext/compress/zstd_compress.c"
	"ext/compress/zstd_compress_internal.h"
	"ext/compress/zstd_compress_literals.c"
	"ext/compress/zstd_compress_literals.h"
	"ext/compress/zstd_compress_sequences.c"
	"ext/compress/zstd_compress_sequences.h"
	"ext/compress/zstd_compress_superblock.c"
	"ext/compress/zstd_compress_superblock.h"
	"ext/compress/zstd_cwksp.h"
	"ext/compress/zstd_double_fast.c"
	"ext/compress/zstd_double_fast.h"
	"ext/compress/zstd_fast.c"
	"ext/compress/zstd_fast.h"
	"ext/compress/zstd_lazy.c"
	"ext/compress/zstd_lazy.h"
	"ext/compress/zstd_ldm.c"
	"ext/compress/zstd_ldm.h"
	"ext/compress/zstd_ldm_geartab.h"
	"ext/compress/zstd_opt.c"
	"ext/compress/zstd_opt.h"

	"ext/decompress/huf_decompress.c"
	"ext/decompress/huf_decompress_amd64.S"
	"ext/decompress/zstd_ddict.c"
	"ext/decompress/zstd_ddict.h"
	"ext/decompress/zstd_decompress.c"
	"ext/decompress/zstd_decompress_block.c"
	"ext/decompress/zstd_decompress_block.h"
	"ext/decompress/zstd_decompress_internal.h"

	"ext/zdict.h"
	"ext/zstd.h"
	"ext/zstd_errors.h"

	${zstd_platform_sources}
)
