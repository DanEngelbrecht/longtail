#include "longtail_meowhash.h"

#include "ext/meow_hash_x64_aesni.h"

const uint32_t LONGTAIL_MEOW_HASH_TYPE = (((uint32_t)'m') << 24) + (((uint32_t)'e') << 16) + (((uint32_t)'o') << 8) + ((uint32_t)'w');
const uint32_t Longtail_GetMeowHashType() { return LONGTAIL_MEOW_HASH_TYPE; }

struct MeowHashAPI
{
    struct Longtail_HashAPI m_MeowHashAPI;
};

static uint32_t MeowHash_GetIdentifier(struct Longtail_HashAPI* hash_api)
{
    return LONGTAIL_MEOW_HASH_TYPE;
}

static int MeowHash_BeginContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext* out_context)
{
    meow_state* state = (meow_state*)Longtail_Alloc(sizeof(meow_state));
    MeowBegin(state, MeowDefaultSeed);
    *out_context = (Longtail_HashAPI_HContext)state;
    return 0;
}

static void MeowHash_Hash(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context, uint32_t length, const void* data)
{
    meow_state* state = (meow_state*)context;
    MeowAbsorb(state, length, (void*)data);
}

static uint64_t MeowHash_EndContext(struct Longtail_HashAPI* hash_api, Longtail_HashAPI_HContext context)
{
    meow_state* state = (meow_state*)context;
    uint64_t hash = (uint64_t)MeowU64From(MeowEnd(state, 0), 0);
    Longtail_Free(state);
    return hash;
}

static int MeowHash_HashBuffer(struct Longtail_HashAPI* hash_api, uint32_t length, const void* data, uint64_t* out_hash)
{
    meow_state state;
    MeowBegin(&state, MeowDefaultSeed);
    MeowAbsorb(&state, length, (void*)data);
    *out_hash = MeowU64From(MeowEnd(&state, 0), 0);
    return 0;
}

static void MeowHash_Dispose(struct Longtail_API* hash_api)
{
    Longtail_Free(hash_api);
}

static void MeowHash_Init(struct MeowHashAPI* hash_api)
{
    hash_api->m_MeowHashAPI.m_API.Dispose = MeowHash_Dispose;
    hash_api->m_MeowHashAPI.GetIdentifier = MeowHash_GetIdentifier;
    hash_api->m_MeowHashAPI.BeginContext = MeowHash_BeginContext;
    hash_api->m_MeowHashAPI.Hash = MeowHash_Hash;
    hash_api->m_MeowHashAPI.EndContext = MeowHash_EndContext;
    hash_api->m_MeowHashAPI.HashBuffer = MeowHash_HashBuffer;
}

struct Longtail_HashAPI* Longtail_CreateMeowHashAPI()
{
    struct MeowHashAPI* meow_hash = (struct MeowHashAPI*)Longtail_Alloc(sizeof(struct MeowHashAPI));
    MeowHash_Init(meow_hash);
    return &meow_hash->m_MeowHashAPI;
}

