#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT struct Longtail_CompressionRegistryAPI* Longtail_CreateZStdCompressionRegistry();

#ifdef __cplusplus
}
#endif
