#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Brotli_CompressionAPI* Brotli_CreateBrotliCompressionAPI();
extern const uint32_t LONGTAIL_BROTLI_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
