#include "longtail_full_hash_registry.h"

#include "longtail_hash_registry.h"

#include "../blake2/longtail_blake2.h"
#include "../blake3/longtail_blake3.h"
#include "../meowhash/longtail_meowhash.h"

 struct Longtail_HashRegistryAPI* Longtail_CreateFullHashRegistry()
 {
	 struct Longtail_HashAPI* blake2_hash = Longtail_CreateBlake2HashAPI();
	 if (!blake2_hash)
	 {
		 return 0;
	 }
	 struct Longtail_HashAPI* blake3_hash = Longtail_CreateBlake3HashAPI();
	 if (!blake3_hash)
	 {
		 SAFE_DISPOSE_API(blake2_hash);
		 return 0;
	 }
	 struct Longtail_HashAPI* meow_hash = Longtail_CreateMeowHashAPI();
	 if (!meow_hash)
	 {
		 SAFE_DISPOSE_API(blake3_hash);
		 SAFE_DISPOSE_API(blake2_hash);
		 return 0;
	 }
	 
	 uint32_t hash_types[3] = {
		 Longtail_GetBlake2HashType(),
		 Longtail_GetBlake3HashType(),
		 Longtail_GetMeowHashType()};

	struct Longtail_HashAPI* hash_apis[3] = {
		blake2_hash,
		blake3_hash,
		meow_hash};

	struct Longtail_HashRegistryAPI* registry = Longtail_CreateDefaultHashRegistry(
		3,
		(const uint32_t*)hash_types,
		(const struct Longtail_HashAPI**)hash_apis);
	if (!registry)
	{
		 SAFE_DISPOSE_API(meow_hash);
		 SAFE_DISPOSE_API(blake3_hash);
		 SAFE_DISPOSE_API(blake2_hash);
		 return 0;
	}
	return registry;
 }
