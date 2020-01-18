#include "longtail_blake2.h"

#include "../../src/longtail.h"
#include "ext/blake2.h"

const uint32_t LONGTAIL_BLAKE2_HASH_TYPE = (((uint32_t)'b') << 24) + (((uint32_t)'l') << 16) + (((uint32_t)'k') << 8) + ((uint32_t)'2');

struct Blake2HashAPI
{
    struct Longtail_HashAPI m_Blake2HashAPI;
};

static uint32_t Blake2Hash_GetIdentifier(struct Longtail_HashAPI* hash_api)
{
    return LONGTAIL_BLAKE2_HASH_TYPE;
}

static int Blake2Hash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context)
{
    blake2s_state* state = (blake2s_state*)Longtail_Alloc(sizeof(blake2s_state));
    int err = blake2s_init( state, sizeof(uint64_t));
    if (err)
    {
        Longtail_Free(state);
        return err;
    }
    *out_context = (Longtail_HashAPI_HContext)state;
    return 0;
}

static void Blake2Hash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data)
{
    blake2s_state* state = (blake2s_state*)context;
    blake2s_update(state, data, length);
}

static uint64_t Blake2Hash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context)
{
    blake2s_state* state = (blake2s_state*)context;
    uint64_t hash;
    int err = blake2s_final(state, &hash, sizeof(uint64_t));
    if (err)
    {
        return 0;
    }
	Longtail_Free(state);
    return hash;
}

static int Blake2Hash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash)
{
    return blake2s(out_hash, sizeof(uint64_t), data, length, 0, 0);
}

static void Blake2Hash_Dispose(struct Longtail_API* hash_api)
{
    Longtail_Free(hash_api);
}

static void Blake2Hash_Init(struct Blake2HashAPI* hash_api)
{
    hash_api->m_Blake2HashAPI.m_API.Dispose = Blake2Hash_Dispose;
    hash_api->m_Blake2HashAPI.GetIdentifier = Blake2Hash_GetIdentifier;
    hash_api->m_Blake2HashAPI.BeginContext = Blake2Hash_BeginContext;
    hash_api->m_Blake2HashAPI.Hash = Blake2Hash_Hash;
    hash_api->m_Blake2HashAPI.EndContext = Blake2Hash_EndContext;
    hash_api->m_Blake2HashAPI.HashBuffer = Blake2Hash_HashBuffer;
}

struct Longtail_HashAPI* Longtail_CreateBlake2HashAPI()
{
    struct Blake2HashAPI* blake2_hash = (struct Blake2HashAPI*)Longtail_Alloc(sizeof(struct Blake2HashAPI));
    Blake2Hash_Init(blake2_hash);
    return &blake2_hash->m_Blake2HashAPI;
}
