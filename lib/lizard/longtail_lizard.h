#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern struct Longtail_CompressionAPI* Longtail_CreateLizardCompressionAPI();
extern const uint32_t LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE;

#ifdef __cplusplus
}
#endif
