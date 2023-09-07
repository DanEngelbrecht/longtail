#include "longtail_blake3_hash_registry.h"

#include "longtail_hash_registry.h"

#include "../blake2/longtail_blake2.h"
#include "../blake3/longtail_blake3.h"
#include "../meowhash/longtail_meowhash.h"

 struct Longtail_HashRegistryAPI* Longtail_CreateBlake3HashRegistry()
 {
     struct Longtail_HashAPI* blake3_hash = Longtail_CreateBlake3HashAPI();
     if (!blake3_hash)
     {
         return 0;
     }

     uint32_t hash_types[1] = {
         Longtail_GetBlake3HashType()};

    struct Longtail_HashAPI* hash_apis[1] = {
        blake3_hash};

    struct Longtail_HashRegistryAPI* registry = Longtail_CreateDefaultHashRegistry(
        1,
        (const uint32_t*)hash_types,
        (const struct Longtail_HashAPI**)hash_apis);
    if (!registry)
    {
         SAFE_DISPOSE_API(blake3_hash);
         return 0;
    }
    return registry;
 }
