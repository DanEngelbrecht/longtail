#include "longtail_ratelimitedprogress.h"

#include <errno.h>

struct RateLimitedProgress {
    struct Longtail_ProgressAPI m_ProgressAPI;
    struct Longtail_ProgressAPI* m_Progress;
    uint32_t m_UpdateCount;
    uint32_t m_OldPercent;
    uint32_t m_JobsDone;
    uint32_t m_PercentRateLimit;
};

void RateLimitedProgress_Dispose(struct Longtail_API* api)
{
    struct RateLimitedProgress* progress_api = (struct RateLimitedProgress*)api;
	SAFE_DISPOSE_API(progress_api->m_Progress);
    Longtail_Free(progress_api);
}

void RateLimitedProgress_OnProgress(struct Longtail_ProgressAPI* progressAPI, uint32_t total_count, uint32_t done_count)
{
    struct RateLimitedProgress* progress_api = (struct RateLimitedProgress*)progressAPI;
    if (done_count < total_count)
    {
        uint32_t percent_done = (100 * done_count) / total_count;
        uint32_t percent_diff = percent_done - progress_api->m_OldPercent;
        if ((!progress_api->m_UpdateCount) || (percent_diff >= progress_api->m_PercentRateLimit))
        {
            progress_api->m_Progress->OnProgress(progress_api->m_Progress, total_count, done_count);
            progress_api->m_OldPercent = percent_done;
        }
        ++progress_api->m_UpdateCount;
        progress_api->m_JobsDone = done_count;
        return;
    }
    progress_api->m_Progress->OnProgress(progress_api->m_Progress, total_count, done_count);
    progress_api->m_JobsDone = done_count;
}

static int RateLimitedProgress_Init(
    void* mem,
    struct Longtail_ProgressAPI* progress_api,
    uint32_t percent_rate_limit,
    struct Longtail_ProgressAPI** out_progress_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(mem, "%p"),
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(percent_rate_limit, "%u"),
        LONGTAIL_LOGFIELD(out_progress_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, progress_api != 0, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, percent_rate_limit <= 100, return 0);
    LONGTAIL_VALIDATE_INPUT(ctx, out_progress_api != 0, return 0);

    struct Longtail_ProgressAPI* api = Longtail_MakeProgressAPI(
        mem,
        RateLimitedProgress_Dispose,
        RateLimitedProgress_OnProgress);
    struct RateLimitedProgress* out_api = (struct RateLimitedProgress*)api;
    out_api->m_Progress = progress_api;

    out_api->m_UpdateCount = 0;
    out_api->m_OldPercent = 0;
    out_api->m_JobsDone = 0;
    out_api->m_PercentRateLimit = percent_rate_limit;

    *out_progress_api = api;
    return 0;
}

struct Longtail_ProgressAPI* Longtail_CreateRateLimitedProgress(
    struct Longtail_ProgressAPI* progress_api,
    uint32_t percent_rate_limit)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(progress_api, "%p"),
        LONGTAIL_LOGFIELD(percent_rate_limit, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)
    LONGTAIL_VALIDATE_INPUT(ctx, progress_api != 0, return 0);

    void* mem = (struct RateLimitedProgress*)Longtail_Alloc(sizeof(struct RateLimitedProgress));
    if (!mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    struct Longtail_ProgressAPI* out_api;
    int err = RateLimitedProgress_Init(mem, progress_api, percent_rate_limit, &out_api);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "RateLimitedProgress_Init() failed with %d", err)
        return 0;
    }
    return out_api;
}

