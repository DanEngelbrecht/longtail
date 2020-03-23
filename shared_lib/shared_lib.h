#pragma once

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT struct Longtail_CompressionRegistryAPI* Longtail_CreateCompressionRegistry()

#ifdef __cplusplus
}
#endif
