#pragma once

#if !defined(LONGTAIL_EXPORT)
    #define LONGTAIL_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT struct Longtail_HashRegistryAPI* Longtail_CreateBlake3HashRegistry();

#ifdef __cplusplus
}
#endif
