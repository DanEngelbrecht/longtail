#include "longtail_blake3.h"

#include "../../src/longtail.h"
#include "ext/blake3.h"

const uint32_t LONGTAIL_BLAKE3_HASH_TYPE = (((uint32_t)'b') << 24) + (((uint32_t)'l') << 16) + (((uint32_t)'k') << 8) + ((uint32_t)'3');

struct Blake3HashAPI
{
    struct Longtail_HashAPI m_Blake3HashAPI;
};

static uint32_t Blake3Hash_GetIdentifier(struct Longtail_HashAPI* hash_api)
{
    return LONGTAIL_BLAKE3_HASH_TYPE;
}

static int Blake3Hash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context)
{
    blake3_hasher* hasher = (blake3_hasher*)Longtail_Alloc(sizeof(blake3_hasher));
    blake3_hasher_init(hasher);
    *out_context = (Longtail_HashAPI_HContext)hasher;
    return 0;
}

static void Blake3Hash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data)
{
    blake3_hasher* hasher = (blake3_hasher*)context;
    blake3_hasher_update(hasher, data, (size_t)length);
}

static uint64_t Blake3Hash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context)
{
    blake3_hasher* hasher = (blake3_hasher*)context;
    uint64_t hash;
    blake3_hasher_finalize(hasher, (uint8_t*)&hash, sizeof(uint64_t));
	Longtail_Free(hasher);
    return hash;
}

static int Blake3Hash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash)
{
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, (size_t)length);
    blake3_hasher_finalize(&hasher, (uint8_t*)out_hash, sizeof(uint64_t));
    return 0;
}

static void Blake3Hash_Dispose(struct Longtail_API* hash_api)
{
    Longtail_Free(hash_api);
}

static void Blake3Hash_Init(struct Blake3HashAPI* hash_api)
{
    hash_api->m_Blake3HashAPI.m_API.Dispose = Blake3Hash_Dispose;
    hash_api->m_Blake3HashAPI.GetIdentifier = Blake3Hash_GetIdentifier;
    hash_api->m_Blake3HashAPI.BeginContext = Blake3Hash_BeginContext;
    hash_api->m_Blake3HashAPI.Hash = Blake3Hash_Hash;
    hash_api->m_Blake3HashAPI.EndContext = Blake3Hash_EndContext;
    hash_api->m_Blake3HashAPI.HashBuffer = Blake3Hash_HashBuffer;
}

struct Longtail_HashAPI* Longtail_CreateBlake3HashAPI()
{
    struct Blake3HashAPI* blake3_hash = (struct Blake3HashAPI*)Longtail_Alloc(sizeof(struct Blake3HashAPI));
    Blake3Hash_Init(blake3_hash);
    return &blake3_hash->m_Blake3HashAPI;
}
