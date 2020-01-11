#include "longtail_xxhash.h"

#include "../../src/longtail.h"

#define XXH_STATIC_LINKING_ONLY
#include "ext/xxhash.h"

struct XXHashAPI
{
    struct Longtail_HashAPI m_XXHashAPI;
};

static int XXHash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context)
{
    XXH64_state_t* state = XXH64_createState();
    int err = XXH64_reset(state, 0);
    if (err)
    {
        XXH64_freeState(state);
        return err; // TODO: Need to convert to errno
    }
    *out_context = (Longtail_HashAPI_HContext)state;
    return 0;
}

static void XXHash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, void* data)
{
    XXH64_state_t* state = (XXH64_state_t*)context;
    XXH64_update(state, data, length);
}

static uint64_t XXHash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context)
{
    XXH64_state_t* state = (XXH64_state_t*)context;
    uint64_t hash = (uint64_t)XXH64_digest(state);
	XXH64_freeState(state);
    return hash;
}

static int XXHash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, void* data, uint64_t* out_hash)
{
    *out_hash = (uint64_t)XXH64(data, length, 0);
    return 0;
}

static void XXHash_Dispose(struct Longtail_API* hash_api)
{
    Longtail_Free(hash_api);
}

static void XXHash_Init(struct XXHashAPI* hash_api)
{
    hash_api->m_XXHashAPI.m_API.Dispose = XXHash_Dispose;
    hash_api->m_XXHashAPI.BeginContext = XXHash_BeginContext;
    hash_api->m_XXHashAPI.Hash = XXHash_Hash;
    hash_api->m_XXHashAPI.EndContext = XXHash_EndContext;
    hash_api->m_XXHashAPI.HashBuffer = XXHash_HashBuffer;
}

struct Longtail_HashAPI* Longtail_CreateXXHashAPI()
{
    struct XXHashAPI* xx_hash = (struct XXHashAPI*)Longtail_Alloc(sizeof(struct XXHashAPI));
    XXHash_Init(xx_hash);
    return &xx_hash->m_XXHashAPI;
}

#include "ext/xxhash.c"
