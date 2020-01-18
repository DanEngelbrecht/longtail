#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateZStdCompressionAPI();
extern const uint32_t LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
