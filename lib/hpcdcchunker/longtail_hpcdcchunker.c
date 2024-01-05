// https://moinakg.wordpress.com/2013/06/22/high-performance-content-defined-chunking/

#include "longtail_hpcdcchunker.h"

#include "../longtail_platform.h"

#include <errno.h>
#include <string.h>
#include <stdlib.h>

// ChunkerWindowSize is the number of bytes in the rolling hash window
#define ChunkerWindowSize 48u

struct Longtail_HPCDCChunker;

struct Longtail_HPCDCChunkerParams
{
    uint32_t min;
    uint32_t avg;
    uint32_t max;
};

static uint32_t hashTable[] = {
    0x458be752, 0xc10748cc, 0xfbbcdbb8, 0x6ded5b68,
    0xb10a82b5, 0x20d75648, 0xdfc5665f, 0xa8428801,
    0x7ebf5191, 0x841135c7, 0x65cc53b3, 0x280a597c,
    0x16f60255, 0xc78cbc3e, 0x294415f5, 0xb938d494,
    0xec85c4e6, 0xb7d33edc, 0xe549b544, 0xfdeda5aa,
    0x882bf287, 0x3116737c, 0x05569956, 0xe8cc1f68,
    0x0806ac5e, 0x22a14443, 0x15297e10, 0x50d090e7,
    0x4ba60f6f, 0xefd9f1a7, 0x5c5c885c, 0x82482f93,
    0x9bfd7c64, 0x0b3e7276, 0xf2688e77, 0x8fad8abc,
    0xb0509568, 0xf1ada29f, 0xa53efdfe, 0xcb2b1d00,
    0xf2a9e986, 0x6463432b, 0x95094051, 0x5a223ad2,
    0x9be8401b, 0x61e579cb, 0x1a556a14, 0x5840fdc2,
    0x9261ddf6, 0xcde002bb, 0x52432bb0, 0xbf17373e,
    0x7b7c222f, 0x2955ed16, 0x9f10ca59, 0xe840c4c9,
    0xccabd806, 0x14543f34, 0x1462417a, 0x0d4a1f9c,
    0x087ed925, 0xd7f8f24c, 0x7338c425, 0xcf86c8f5,
    0xb19165cd, 0x9891c393, 0x325384ac, 0x0308459d,
    0x86141d7e, 0xc922116a, 0xe2ffa6b6, 0x53f52aed,
    0x2cd86197, 0xf5b9f498, 0xbf319c8f, 0xe0411fae,
    0x977eb18c, 0xd8770976, 0x9833466a, 0xc674df7f,
    0x8c297d45, 0x8ca48d26, 0xc49ed8e2, 0x7344f874,
    0x556f79c7, 0x6b25eaed, 0xa03e2b42, 0xf68f66a4,
    0x8e8b09a2, 0xf2e0e62a, 0x0d3a9806, 0x9729e493,
    0x8c72b0fc, 0x160b94f6, 0x450e4d3d, 0x7a320e85,
    0xbef8f0e1, 0x21d73653, 0x4e3d977a, 0x1e7b3929,
    0x1cc6c719, 0xbe478d53, 0x8d752809, 0xe6d8c2c6,
    0x275f0892, 0xc8acc273, 0x4cc21580, 0xecc4a617,
    0xf5f7be70, 0xe795248a, 0x375a2fe9, 0x425570b6,
    0x8898dcf8, 0xdc2d97c4, 0x0106114b, 0x364dc22f,
    0x1e0cad1f, 0xbe63803c, 0x5f69fac2, 0x4d5afa6f,
    0x1bc0dfb5, 0xfb273589, 0x0ea47f7b, 0x3c1c2b50,
    0x21b2a932, 0x6b1223fd, 0x2fe706a8, 0xf9bd6ce2,
    0xa268e64e, 0xe987f486, 0x3eacf563, 0x1ca2018c,
    0x65e18228, 0x2207360a, 0x57cf1715, 0x34c37d2b,
    0x1f8f3cde, 0x93b657cf, 0x31a019fd, 0xe69eb729,
    0x8bca7b9b, 0x4c9d5bed, 0x277ebeaf, 0xe0d8f8ae,
    0xd150821c, 0x31381871, 0xafc3f1b0, 0x927db328,
    0xe95effac, 0x305a47bd, 0x426ba35b, 0x1233af3f,
    0x686a5b83, 0x50e072e5, 0xd9d3bb2a, 0x8befc475,
    0x487f0de6, 0xc88dff89, 0xbd664d5e, 0x971b5d18,
    0x63b14847, 0xd7d3c1ce, 0x7f583cf3, 0x72cbcb09,
    0xc0d0a81c, 0x7fa3429b, 0xe9158a1b, 0x225ea19a,
    0xd8ca9ea3, 0xc763b282, 0xbb0c6341, 0x020b8293,
    0xd4cd299d, 0x58cfa7f8, 0x91b4ee53, 0x37e4d140,
    0x95ec764c, 0x30f76b06, 0x5ee68d24, 0x679c8661,
    0xa41979c2, 0xf2b61284, 0x4fac1475, 0x0adb49f9,
    0x19727a23, 0x15a7e374, 0xc43a18d5, 0x3fb1aa73,
    0x342fc615, 0x924c0793, 0xbee2d7f0, 0x8a279de9,
    0x4aa2d70c, 0xe24dd37f, 0xbe862c0b, 0x177c22c2,
    0x5388e5ee, 0xcd8a7510, 0xf901b4fd, 0xdbc13dbc,
    0x6c0bae5b, 0x64efe8c7, 0x48b02079, 0x80331a49,
    0xca3d8ae6, 0xf3546190, 0xfed7108b, 0xc49b941b,
    0x32baf4a9, 0xeb833a4a, 0x88a3f1a5, 0x3a91ce0a,
    0x3cc27da1, 0x7112e684, 0x4a3096b1, 0x3794574c,
    0xa3c8b6f3, 0x1d213941, 0x6e0a2e00, 0x233479f1,
    0x0f4cd82f, 0x6093edd2, 0x5d7d209e, 0x464fe319,
    0xd4dcac9e, 0x0db845cb, 0xfb5e4bc3, 0xe0256ce1,
    0x09fb4ed1, 0x0914be1e, 0xa5bdb2c3, 0xc6eb57bb,
    0x30320350, 0x3f397e91, 0xa67791bc, 0x86bc0e2c,
    0xefa0a7e2, 0xe9ff7543, 0xe733612c, 0xd185897b,
    0x329e5388, 0x91dd236b, 0x2ecb0d93, 0xf4d82a3d,
    0x35b5c03f, 0xe4e606f0, 0x05b21843, 0x37b45964,
    0x5eff22f4, 0x6027f4cc, 0x77178b3c, 0xae507131,
    0x7bf7cabc, 0xf9c18d66, 0x593ade65, 0xd95ddf11,
};

struct HPCDCChunkerWindow
{
    uint8_t* buf;
    uint32_t len;
};

struct HPCDCArray
{
    uint8_t* data;
    uint32_t len;
};

#define HPCDCCHUNKER_MAX_CACHED_CHUNKER_COUNT 232

struct Longtail_HPCDCChunkerAPI
{
    struct Longtail_ChunkerAPI m_API;
    HLongtail_SpinLock m_Lock;
    struct Longtail_HPCDCChunker* m_CachedChunkers[HPCDCCHUNKER_MAX_CACHED_CHUNKER_COUNT];
    uint32_t m_CachedChunkerCount;
};

struct Longtail_HPCDCChunker
{
    struct Longtail_HPCDCChunkerParams params;
    struct HPCDCArray buf;
    uint32_t max_feed;
    uint32_t off;
    uint32_t hValue;
    uint8_t hWindow[ChunkerWindowSize];
    uint32_t hDiscriminator;
    Longtail_Chunker_Feeder fFeeder;
    void* cFeederContext;
    uint64_t processed_count;
};

static uint32_t HPCDCDiscriminatorFromAvg(double avg)
{
    return (uint32_t)(avg / (-1.42888852e-7*avg + 1.33237515));
}

// TODO: Create a cache for chunkers?

int Longtail_HPCDCCreateChunker(
    struct Longtail_HPCDCChunkerParams* params,
    struct Longtail_HPCDCChunker* optional_cached_chunker,
    struct Longtail_HPCDCChunker** out_chunker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(params, "%p"),
        LONGTAIL_LOGFIELD(optional_cached_chunker, "%p"),
        LONGTAIL_LOGFIELD(out_chunker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, params != 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, params->min >= ChunkerWindowSize, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, params->min <= params->max, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, params->min <= params->avg, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, params->avg <= params->max, return EINVAL)

    size_t max_feed = ((size_t)params->max) * 4;
    if (max_feed >= 0xffffffffu)
    {
        max_feed = 0xffffffffu;
    }

    struct Longtail_HPCDCChunker* c = optional_cached_chunker;
    if (c == 0 || c->max_feed < max_feed)
    {
        if (optional_cached_chunker)
        {
            Longtail_Free(optional_cached_chunker);
            optional_cached_chunker = 0;
        }
        size_t chunker_size = sizeof(struct Longtail_HPCDCChunker) + max_feed;
        c = (struct Longtail_HPCDCChunker*)Longtail_Alloc("HPCDCCreateChunker", chunker_size);
        if (!c)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
                return ENOMEM;
        }
        c->buf.data = (uint8_t*)&c[1];
        c->max_feed = (uint32_t)max_feed;
    }
    c->params = *params;
    c->buf.len = 0;
    c->off = 0;
    c->hValue = 0;
    c->hDiscriminator = HPCDCDiscriminatorFromAvg((double)params->avg);
    c->processed_count = 0;
    *out_chunker = c;
    return 0;
}

static int FeedChunker(
	struct Longtail_HPCDCChunker* c,
	Longtail_Chunker_Feeder feeder,
	void* context)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(c, "%p"),
        LONGTAIL_LOGFIELD(feeder, "%p"),
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, c != 0, return EINVAL)

    if (c->off != 0)
    {
        memmove(c->buf.data, &c->buf.data[c->off], c->buf.len - c->off);
        c->processed_count += c->off;
        c->buf.len -= c->off;
        c->off = 0;
    }
    uint32_t feed_max = (uint32_t)(c->max_feed - c->buf.len);
    uint32_t feed_count;
    int err = feeder(context, (Longtail_ChunkerAPI_HChunker)c, feed_max, (char*)&c->buf.data[c->buf.len], &feed_count);
    c->buf.len += feed_count;
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "feeder() failed with %d", err)
    }
    return err;
}

#if defined(_MSC_VER)
#  define LONGTAIL_rotl32(x,r) _rotl(x,r)
#else
#  define LONGTAIL_rotl32(x,r) ((x << r) | (x >> (32 - r)))
#endif

static const struct Longtail_Chunker_ChunkRange EmptyChunkRange = {0, 0, 0};

struct Longtail_Chunker_ChunkRange Longtail_HPCDCNextChunk(
	struct Longtail_HPCDCChunker* c,
	Longtail_Chunker_Feeder feeder,
	void* context)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(c, "%p"),
        LONGTAIL_LOGFIELD(feeder, "%p"),
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_FATAL_ASSERT(ctx, c != 0, return EmptyChunkRange)
    if (c->buf.len - c->off < c->params.max)
    {
        int err = FeedChunker(c, feeder, context);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "FeedChunker() failed with %d", err)
            return EmptyChunkRange;
        }
    }
    if (c->off == c->buf.len)
    {
        // All done
        struct Longtail_Chunker_ChunkRange r = {0, c->processed_count + c->off, 0};
        return r;
    }

    uint32_t left = c->buf.len - c->off;
    if (left <= c->params.min)
    {
        // Less than min-size left, just consume it all
        struct Longtail_Chunker_ChunkRange r = {&c->buf.data[c->off], c->processed_count + c->off, left};
        c->off += left;
        return r;
    }

    uint32_t hash = 0;
    struct Longtail_Chunker_ChunkRange scoped_data = {&c->buf.data[c->off], c->processed_count + c->off, left};
    {
        struct Longtail_Chunker_ChunkRange window = {
            &scoped_data.buf[c->params.min - ChunkerWindowSize],
            c->processed_count + c->off + c->params.min - ChunkerWindowSize,
            ChunkerWindowSize};
        for (uint32_t i = 0; i < ChunkerWindowSize; ++i)
        {
            uint8_t b = window.buf[i];
            hash ^= LONGTAIL_rotl32(hashTable[b], (int)((ChunkerWindowSize-i-1u) & 31));
            c->hWindow[i] = b;
        }
    }

    uint32_t pos = c->params.min;
    uint32_t idx = 0;

    uint32_t data_len = scoped_data.len > c->params.max ? c->params.max : scoped_data.len;
    uint8_t* window = c->hWindow;
    const uint32_t discriminator = c->hDiscriminator - 1;
    const uint8_t* scoped_buf = scoped_data.buf;
    const uint32_t d = c->hDiscriminator;
    while(pos < data_len)
    {
        uint8_t in = scoped_buf[pos++];
        uint8_t out = window[idx];
        window[idx++] = in;
        hash = LONGTAIL_rotl32(hash, 1) ^
            LONGTAIL_rotl32(hashTable[out], (int)(ChunkerWindowSize & 31)) ^
            hashTable[in];

        if ((hash % d) == discriminator)
        {
            break;
        }
        if (idx == ChunkerWindowSize)
        {
            idx = 0;
        }
    }
    struct Longtail_Chunker_ChunkRange r = {scoped_buf, c->processed_count + c->off, pos};
    c->off += pos;
    return r;
}

void HPCDCChunker_Dispose(struct Longtail_API* base_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(base_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, base_api, return)
	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)base_api;
    Longtail_LockSpinLock(api->m_Lock);
    while (api->m_CachedChunkerCount > 0)
    {
        api->m_CachedChunkerCount--;
        Longtail_Free(api->m_CachedChunkers[api->m_CachedChunkerCount]);
        api->m_CachedChunkers[api->m_CachedChunkerCount] = 0;
    }
    Longtail_UnlockSpinLock(api->m_Lock);
    Longtail_DeleteSpinLock(api->m_Lock);
	Longtail_Free(api);
}

int HPCDCChunker_GetMinChunkSize(struct Longtail_ChunkerAPI* chunker_api, uint32_t* out_min_chunk_size)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(out_min_chunk_size, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, chunker_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_min_chunk_size, return EINVAL)
	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;

	*out_min_chunk_size = ChunkerWindowSize;

	return 0;
}

int HPCDCChunker_CreateChunker(
    struct Longtail_ChunkerAPI* chunker_api,
    uint32_t min_chunk_size,
    uint32_t avg_chunk_size,
    uint32_t max_chunk_size,
    Longtail_ChunkerAPI_HChunker* out_chunker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(min_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(avg_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(max_chunk_size, "%u"),
        LONGTAIL_LOGFIELD(out_chunker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, chunker_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunker, return EINVAL)
	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;

    struct Longtail_HPCDCChunkerParams chunker_params;
    chunker_params.min = min_chunk_size;
    chunker_params.avg = avg_chunk_size;
    chunker_params.max = max_chunk_size;

    struct Longtail_HPCDCChunker* cached_chunker = 0;
    Longtail_LockSpinLock(api->m_Lock);
    if (api->m_CachedChunkerCount > 0)
    {
        api->m_CachedChunkerCount--;
        cached_chunker = api->m_CachedChunkers[api->m_CachedChunkerCount];
        api->m_CachedChunkers[api->m_CachedChunkerCount] = 0;
    }
    Longtail_UnlockSpinLock(api->m_Lock);

	struct Longtail_HPCDCChunker* chunker;
	Longtail_HPCDCCreateChunker(&chunker_params, cached_chunker, &chunker);

	*out_chunker = (Longtail_ChunkerAPI_HChunker)chunker;

	return 0;
}

int HPCDCChunker_NextChunk(
    struct Longtail_ChunkerAPI* chunker_api,
    Longtail_ChunkerAPI_HChunker chunker,
    Longtail_Chunker_Feeder feeder,
    void* feeder_context,
    struct Longtail_Chunker_ChunkRange* out_chunk_range)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(chunker, "%p"),
        LONGTAIL_LOGFIELD(feeder, "%p"),
        LONGTAIL_LOGFIELD(feeder_context, "%p"),
        LONGTAIL_LOGFIELD(out_chunk_range, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    LONGTAIL_VALIDATE_INPUT(ctx, chunker_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunker, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, feeder_context, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunk_range, return EINVAL)
	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;

	struct Longtail_HPCDCChunker* c = (struct Longtail_HPCDCChunker*)chunker;
	struct Longtail_Chunker_ChunkRange chunk_range = Longtail_HPCDCNextChunk(c, feeder, feeder_context);
	out_chunk_range->buf = chunk_range.buf;
	out_chunk_range->len = chunk_range.len;
	out_chunk_range->offset = chunk_range.offset;
	if (chunk_range.len == 0)
	{
		return ESPIPE;
	}
	return 0;
}

int HPCDCChunker_DisposeChunker(struct Longtail_ChunkerAPI* chunker_api, Longtail_ChunkerAPI_HChunker chunker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(chunker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, chunker_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunker, return EINVAL)
	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;
    Longtail_LockSpinLock(api->m_Lock);
    if (api->m_CachedChunkerCount == HPCDCCHUNKER_MAX_CACHED_CHUNKER_COUNT)
    {
        Longtail_UnlockSpinLock(api->m_Lock);
        Longtail_Free(chunker);
    }
    else
    {
        api->m_CachedChunkers[api->m_CachedChunkerCount] = (struct Longtail_HPCDCChunker*)chunker;
        api->m_CachedChunkerCount++;
        Longtail_UnlockSpinLock(api->m_Lock);
    }
	return 0;
}

int HPCDCChunker_NextChunkFromBuffer(
    struct Longtail_ChunkerAPI* chunker_api,
    Longtail_ChunkerAPI_HChunker chunker,
    const void* buffer,
    uint64_t buffer_size,
    const void** out_next_chunk_start)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(chunker_api, "%p"),
        LONGTAIL_LOGFIELD(chunker, "%p"),
        LONGTAIL_LOGFIELD(buffer, "%p"),
        LONGTAIL_LOGFIELD(buffer_size, "%p"),
        LONGTAIL_LOGFIELD(out_next_chunk_start, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)
    LONGTAIL_VALIDATE_INPUT(ctx, chunker_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, chunker, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, buffer, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, buffer_size > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_next_chunk_start, return EINVAL)

	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;
	struct Longtail_HPCDCChunker* c = (struct Longtail_HPCDCChunker*)chunker;

    if (buffer_size <= c->params.min)
    {
        // Less than min-size left, just consume it all
        *out_next_chunk_start = ((const uint8_t*)buffer) + buffer_size;
        return 0;
    }

    const uint8_t* buf = (const uint8_t*)buffer;

    uint32_t hash = 0;
    for (uint32_t i = 0; i < ChunkerWindowSize; ++i)
    {
        uint8_t b = buf[i];
        hash ^= LONGTAIL_rotl32(hashTable[b], (int)((ChunkerWindowSize-i-1u) & 31));
        c->hWindow[i] = b;
    }

    uint32_t pos = c->params.min;
    uint32_t idx = 0;

    uint32_t data_len = (uint32_t)(buffer_size > c->params.max ? c->params.max : buffer_size);
    uint8_t* window = c->hWindow;
    const uint32_t discriminator = c->hDiscriminator - 1;
    const uint32_t d = c->hDiscriminator;
    while(pos < data_len)
    {
        uint8_t in = buf[pos++];
        uint8_t out = window[idx];
        window[idx++] = in;
        hash = LONGTAIL_rotl32(hash, 1) ^
            LONGTAIL_rotl32(hashTable[out], (int)(ChunkerWindowSize & 31)) ^
            hashTable[in];

        if ((hash % d) == discriminator)
        {
            break;
        }
        if (idx == ChunkerWindowSize)
        {
            idx = 0;
        }
    }
    *out_next_chunk_start = ((const uint8_t*)buffer) + pos;
    return 0;
}

static int HPCDCChunker_Init(
    void* mem,
    struct Longtail_ChunkerAPI** out_chunker_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(out_chunker_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_chunker_api != 0, return EINVAL)

	struct Longtail_ChunkerAPI* chunker_api = Longtail_MakeChunkerAPI(
		mem,
		HPCDCChunker_Dispose,
		HPCDCChunker_GetMinChunkSize,
		HPCDCChunker_CreateChunker,
		HPCDCChunker_NextChunk,
		HPCDCChunker_DisposeChunker,
        HPCDCChunker_NextChunkFromBuffer);
    if (!chunker_api)
    {
        return EINVAL;
    }

	struct Longtail_HPCDCChunkerAPI* api = (struct Longtail_HPCDCChunkerAPI*)chunker_api;
    int err = Longtail_CreateSpinLock(&api[1], &api->m_Lock);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_CreateSpinLock() failed with %d", err)
        return err;
    }
    api->m_CachedChunkerCount = 0;

	*out_chunker_api = chunker_api;
	return 0;
}

struct Longtail_ChunkerAPI* Longtail_CreateHPCDCChunkerAPI()
{
    MAKE_LOG_CONTEXT(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    size_t api_size =
        sizeof(struct Longtail_HPCDCChunkerAPI)+
        Longtail_GetSpinLockSize();

    void* mem = Longtail_Alloc("HPCDCCreateChunker", api_size);
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_ChunkerAPI* chunker_api;
    int err = HPCDCChunker_Init(
        mem,
        &chunker_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "HPCDCChunker_Init() failed with %d", err)
        Longtail_Free(mem);
        return 0;
    }
    return chunker_api;

}
