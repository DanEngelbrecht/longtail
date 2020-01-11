#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateBrotliCompressionAPI();
extern const uint32_t LONGTAIL_BROTLI_DEFAULT_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
