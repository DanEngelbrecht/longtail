#include "longtail_bikeshed.h"

#include "../longtail_platform.h"

#define BIKESHED_IMPLEMENTATION
#include "ext/bikeshed.h"

#include <errno.h>

struct ReadyCallback
{
    struct Bikeshed_ReadyCallback cb;
    HLongtail_Sema m_Semaphore;
};

static void ReadyCallback_Dispose(struct ReadyCallback* ready_callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(ready_callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, ready_callback, return)
    Longtail_DeleteSema(ready_callback->m_Semaphore);
    Longtail_Free(ready_callback->m_Semaphore);
}

static void ReadyCallback_Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(ready_callback, "%p"),
        LONGTAIL_LOGFIELD(channel, "%u"),
        LONGTAIL_LOGFIELD(ready_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, ready_callback, return)
    struct ReadyCallback* cb = (struct ReadyCallback*)ready_callback;
    Longtail_PostSema(cb->m_Semaphore, ready_count);
}

static void ReadyCallback_Wait(struct ReadyCallback* ready_callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(ready_callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, ready_callback, return)
    Longtail_WaitSema(ready_callback->m_Semaphore, LONGTAIL_TIMEOUT_INFINITE);
}

static int ReadyCallback_Init(struct ReadyCallback* ready_callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(ready_callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, ready_callback, return EINVAL)
    ready_callback->cb.SignalReady = ReadyCallback_Ready;
    return Longtail_CreateSema(Longtail_Alloc(Longtail_GetSemaSize()), 0, &ready_callback->m_Semaphore);
}


struct ThreadWorker
{
    int32_t volatile*   stop;
    Bikeshed            shed;
    HLongtail_Sema        semaphore;
    HLongtail_Thread      thread;
};

static void ThreadWorker_Init(struct ThreadWorker* thread_worker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(thread_worker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker, return)
    thread_worker->stop = 0;
    thread_worker->shed = 0;
    thread_worker->semaphore = 0;
    thread_worker->thread = 0;
}

static int32_t ThreadWorker_Execute(void* context)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, context, return 0)
    struct ThreadWorker* thread_worker = (struct ThreadWorker*)(context);

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker->stop, return 0)
    while (*thread_worker->stop == 0)
    {
        if (!Bikeshed_ExecuteOne(thread_worker->shed, 0))
        {
            Longtail_WaitSema(thread_worker->semaphore, LONGTAIL_TIMEOUT_INFINITE);
        }
    }
    return 0;
}

static int ThreadWorker_CreateThread(struct ThreadWorker* thread_worker, Bikeshed in_shed, int worker_priority, HLongtail_Sema in_semaphore, int32_t volatile* in_stop)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(thread_worker, "%p"),
        LONGTAIL_LOGFIELD(in_shed, "%p"),
        LONGTAIL_LOGFIELD(worker_priority, "%d"),
        LONGTAIL_LOGFIELD(in_semaphore, "%p"),
        LONGTAIL_LOGFIELD(in_stop, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, in_shed, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, in_semaphore, return EINVAL)
    LONGTAIL_FATAL_ASSERT(ctx, in_stop, return EINVAL)
    thread_worker->shed               = in_shed;
    thread_worker->stop               = in_stop;
    thread_worker->semaphore          = in_semaphore;
    return Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), ThreadWorker_Execute, 0, thread_worker, worker_priority, &thread_worker->thread);
}

static int ThreadWorker_JoinThread(struct ThreadWorker* thread_worker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(thread_worker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker, return EINVAL)
    return Longtail_JoinThread(thread_worker->thread, LONGTAIL_TIMEOUT_INFINITE);
}

static void ThreadWorker_DisposeThread(struct ThreadWorker* thread_worker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(thread_worker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker, return)
    Longtail_DeleteThread(thread_worker->thread);
    Longtail_Free(thread_worker->thread);
}

static void ThreadWorker_Dispose(struct ThreadWorker* thread_worker)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(thread_worker, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, thread_worker, return)
    ThreadWorker_DisposeThread(thread_worker);
}

struct JobWrapper
{
    struct Bikeshed_JobAPI_Group* m_JobGroup;
    Longtail_JobAPI_JobFunc m_JobFunc;
    void* m_Context;
};

struct BikeshedJobAPI
{
    struct Longtail_JobAPI m_BikeshedAPI;

    struct ReadyCallback m_ReadyCallback;
    Bikeshed m_Shed;
    uint32_t m_WorkerCount;
    struct ThreadWorker* m_Workers;
    int m_WorkerPriority;
    int32_t volatile m_Stop;
};

struct Bikeshed_JobAPI_Group
{
    struct BikeshedJobAPI* m_API;
    struct JobWrapper* m_ReservedJobs;
    Bikeshed_TaskID* m_ReservedTasksIDs;
    uint32_t m_ReservedJobCount;
    int32_t volatile m_Cancelled;
    int32_t volatile m_SubmittedJobCount;
    int32_t volatile m_PendingJobCount;
    int32_t volatile m_JobsCompleted;
};


struct Bikeshed_JobAPI_Group* CreateJobGroup(struct BikeshedJobAPI* job_api, uint32_t job_count)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_count, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_FATAL_ASSERT(ctx, job_api != 0, return 0)
    int err = EINVAL;
    uint8_t* p = 0;
    size_t job_group_size = sizeof(struct Bikeshed_JobAPI_Group) +
        (sizeof(struct JobWrapper) * job_count) +
        (sizeof(Bikeshed_TaskID) * job_count);
    struct Bikeshed_JobAPI_Group* job_group = (struct Bikeshed_JobAPI_Group*)Longtail_Alloc(job_group_size);
    if (!job_group)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        err = ENOMEM;
        goto on_error;
    }
    p = (uint8_t*)&job_group[1];
    job_group->m_ReservedJobs = (struct JobWrapper*)p;
    p += sizeof(struct JobWrapper) * job_count;
    job_group->m_ReservedTasksIDs = (Bikeshed_TaskID*)p;
    p += sizeof(Bikeshed_TaskID) * job_count;
    job_group->m_API = job_api;
    job_group->m_ReservedJobCount = job_count;
    job_group->m_Cancelled = 0;
    job_group->m_PendingJobCount = 0;
    job_group->m_SubmittedJobCount = 0;
    job_group->m_JobsCompleted = 0;
end:
    return job_group;
on_error:
    goto end;
}

static enum Bikeshed_TaskResult Bikeshed_Job(Bikeshed shed, Bikeshed_TaskID task_id, uint8_t channel, void* context)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(shed, "%p"),
        LONGTAIL_LOGFIELD(task_id, "%u"),
        LONGTAIL_LOGFIELD(channel, "%u"),
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, shed, return (enum Bikeshed_TaskResult)-1)
    LONGTAIL_FATAL_ASSERT(ctx, context, return (enum Bikeshed_TaskResult)-1)
    struct JobWrapper* wrapper = (struct JobWrapper*)context;
    int is_cancelled = (int)wrapper->m_JobGroup->m_Cancelled;
    int res = wrapper->m_JobFunc(wrapper->m_Context, task_id, is_cancelled);
    if (res == EBUSY)
    {
        return BIKESHED_TASK_RESULT_BLOCKED;
    }
    LONGTAIL_FATAL_ASSERT(ctx, wrapper->m_JobGroup->m_PendingJobCount > 0, return BIKESHED_TASK_RESULT_COMPLETE)
    LONGTAIL_FATAL_ASSERT(ctx, res == 0, return BIKESHED_TASK_RESULT_COMPLETE)
    Longtail_AtomicAdd32(&wrapper->m_JobGroup->m_JobsCompleted, 1);
    Longtail_AtomicAdd32(&wrapper->m_JobGroup->m_PendingJobCount, -1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

static uint32_t Bikeshed_GetWorkerCount(struct Longtail_JobAPI* job_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return 0)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    return bikeshed_job_api->m_WorkerCount;
}

static int Bikeshed_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Group* out_job_group)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_count, "%u"),
        LONGTAIL_LOGFIELD(out_job_group, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_count > 0, return EINVAL)
    int err = EINVAL;
    struct Bikeshed_JobAPI_Group* job_group = 0;

    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;

    job_group = CreateJobGroup(bikeshed_job_api, job_count);
    if (!out_job_group)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "CreateJobGroup() failed with %d", err)
        err = ENOMEM;
        goto on_error;
    }
    *out_job_group = (Longtail_JobAPI_Group)job_group;
    job_group = 0,
    err = 0;
end:
    return err;
on_error:
    goto end;
}

static int Bikeshed_CreateJobs(
    struct Longtail_JobAPI* job_api,
    Longtail_JobAPI_Group job_group,
    uint32_t job_count,
    Longtail_JobAPI_JobFunc job_funcs[],
    void* job_contexts[],
    Longtail_JobAPI_Jobs* out_jobs)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_group, "%p"),
        LONGTAIL_LOGFIELD(job_count, "%u"),
        LONGTAIL_LOGFIELD(job_funcs, "%p"),
        LONGTAIL_LOGFIELD(job_contexts, "%p"),
        LONGTAIL_LOGFIELD(out_jobs, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_funcs, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_contexts, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, out_jobs, return EINVAL)
    int err = EINVAL;
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    struct Bikeshed_JobAPI_Group* bikeshed_job_group = (struct Bikeshed_JobAPI_Group*)job_group;
    void* work_mem = 0;
    BikeShed_TaskFunc* funcs = 0;
    void** ctxs = 0;
    Bikeshed_TaskID* task_ids = 0;
    uint32_t job_range_start = 0;
    size_t work_mem_size =
        sizeof(BikeShed_TaskFunc) * job_count +
        sizeof(void*) * job_count;

    int32_t new_job_count = Longtail_AtomicAdd32(&bikeshed_job_group->m_SubmittedJobCount, (int32_t)job_count);
    LONGTAIL_FATAL_ASSERT(ctx, new_job_count > 0, return EINVAL);
    if (new_job_count > (int32_t)bikeshed_job_group->m_ReservedJobCount)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "new_job_count %d exceedes reserverd count %d", new_job_count, (int32_t)bikeshed_job_group->m_ReservedJobCount)
        err = ENOMEM;
        goto on_error;
    }
    job_range_start = (uint32_t)(new_job_count - job_count);

    work_mem = Longtail_Alloc(work_mem_size);
    if (!work_mem)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", err)
        err = ENOMEM;
        goto on_error;
    }

    funcs = (BikeShed_TaskFunc*)work_mem;
    ctxs = (void**)&funcs[job_count];

    task_ids = &bikeshed_job_group->m_ReservedTasksIDs[job_range_start];
    for (uint32_t i = 0; i < job_count; ++i)
    {
        struct JobWrapper* job_wrapper = &bikeshed_job_group->m_ReservedJobs[job_range_start + i];
        job_wrapper->m_JobGroup = bikeshed_job_group;
        job_wrapper->m_Context = job_contexts[i];
        job_wrapper->m_JobFunc = job_funcs[i];
        funcs[i] = Bikeshed_Job;
        ctxs[i] = job_wrapper;
    }

    while (!Bikeshed_CreateTasks(bikeshed_job_api->m_Shed, job_count, funcs, ctxs, task_ids))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }

    Longtail_AtomicAdd32(&bikeshed_job_group->m_PendingJobCount, (int)job_count);

    *out_jobs = task_ids;
    err = 0;
end:
    Longtail_Free(work_mem);
    return err;
on_error:
    Longtail_AtomicAdd32(&bikeshed_job_group->m_SubmittedJobCount, -((int32_t)job_count));
    goto end;
}

static int Bikeshed_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_count, "%u"),
        LONGTAIL_LOGFIELD(jobs, "%p"),
        LONGTAIL_LOGFIELD(dependency_job_count, "%u"),
        LONGTAIL_LOGFIELD(dependency_jobs, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, dependency_jobs, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_count > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, dependency_job_count > 0, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    while (!Bikeshed_AddDependencies(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs, dependency_job_count, (Bikeshed_TaskID*)dependency_jobs))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }
    return 0;
}

static int Bikeshed_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_count, "%u"),
        LONGTAIL_LOGFIELD(jobs, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, job_count > 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, jobs, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs);
    return 0;
}

static int Bikeshed_WaitForAllJobs(struct Longtail_JobAPI* job_api, Longtail_JobAPI_Group job_group, struct Longtail_ProgressAPI* progressAPI, struct Longtail_CancelAPI* optional_cancel_api, Longtail_CancelAPI_HCancelToken optional_cancel_token)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_group, "%p"),
        LONGTAIL_LOGFIELD(progressAPI, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_api, "%p"),
        LONGTAIL_LOGFIELD(optional_cancel_token, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    struct Bikeshed_JobAPI_Group* bikeshed_job_group = (struct Bikeshed_JobAPI_Group*)job_group;
    int32_t old_pending_count = 0;
    while (bikeshed_job_group->m_PendingJobCount > 0)
    {
        if (bikeshed_job_group->m_Cancelled == 0)
        {
            if (progressAPI)
            {
                progressAPI->OnProgress(progressAPI,(uint32_t)bikeshed_job_group->m_ReservedJobCount, (uint32_t)bikeshed_job_group->m_JobsCompleted);
            }
            if (optional_cancel_api && optional_cancel_token)
            {
                if (optional_cancel_api->IsCancelled(optional_cancel_api, optional_cancel_token) == ECANCELED)
                {
                    Longtail_AtomicAdd32(&bikeshed_job_group->m_Cancelled, 1);
                }
            }
        }
        if (Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0))
        {
            continue;
        }
        if (old_pending_count != bikeshed_job_group->m_PendingJobCount)
        {
            old_pending_count = bikeshed_job_group->m_PendingJobCount;
            continue;
        }
        Longtail_WaitSema(bikeshed_job_api->m_ReadyCallback.m_Semaphore, 1000);
    }
    if (progressAPI)
    {
        progressAPI->OnProgress(progressAPI, (uint32_t)bikeshed_job_group->m_SubmittedJobCount, (uint32_t)bikeshed_job_group->m_SubmittedJobCount);
    }
    int is_cancelled = bikeshed_job_group->m_Cancelled;
    Longtail_Free(job_group);
    if (is_cancelled)
    {
        return ECANCELED;
    }
    return 0;
}

static int Bikeshed_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(job_id, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, 1, &job_id);
    return 0;
}

static void Bikeshed_Dispose(struct Longtail_API* job_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, job_api, return)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Longtail_AtomicAdd32(&bikeshed_job_api->m_Stop, 1);
    ReadyCallback_Ready(&bikeshed_job_api->m_ReadyCallback.cb, 0, bikeshed_job_api->m_WorkerCount);
    for (uint32_t i = 0; i < bikeshed_job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_JoinThread(&bikeshed_job_api->m_Workers[i]);
    }
    for (uint32_t i = 0; i < bikeshed_job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_Dispose(&bikeshed_job_api->m_Workers[i]);
    }
    Longtail_Free(bikeshed_job_api->m_Workers);
    Longtail_Free(bikeshed_job_api->m_Shed);
    ReadyCallback_Dispose(&bikeshed_job_api->m_ReadyCallback);
    Longtail_Free(bikeshed_job_api);
}

static int Bikeshed_Init(struct BikeshedJobAPI* job_api, uint32_t worker_count, int worker_priority)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(job_api, "%p"),
        LONGTAIL_LOGFIELD(worker_count, "%u"),
        LONGTAIL_LOGFIELD(worker_priority, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)

    LONGTAIL_FATAL_ASSERT(ctx, job_api, return EINVAL)
    job_api->m_BikeshedAPI.m_API.Dispose = Bikeshed_Dispose;
    job_api->m_BikeshedAPI.GetWorkerCount = Bikeshed_GetWorkerCount;
    job_api->m_BikeshedAPI.ReserveJobs = Bikeshed_ReserveJobs;
    job_api->m_BikeshedAPI.CreateJobs = Bikeshed_CreateJobs;
    job_api->m_BikeshedAPI.AddDependecies = Bikeshed_AddDependecies;
    job_api->m_BikeshedAPI.ReadyJobs = Bikeshed_ReadyJobs;
    job_api->m_BikeshedAPI.WaitForAllJobs = Bikeshed_WaitForAllJobs;
    job_api->m_BikeshedAPI.ResumeJob = Bikeshed_ResumeJob;
    job_api->m_Shed = 0;
    job_api->m_WorkerCount = worker_count;
    job_api->m_Workers = 0;
    job_api->m_WorkerPriority = worker_priority;
    job_api->m_Stop = 0;

    int err = ReadyCallback_Init(&job_api->m_ReadyCallback);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ReadyCallback_Init() failed with %d", err)
        return err;
    }

    job_api->m_Shed = Bikeshed_Create(Longtail_Alloc(BIKESHED_SIZE(1048576, 7340032, 1)), 1048576, 7340032, 1, &job_api->m_ReadyCallback.cb);
    if (!job_api->m_Shed)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Bikeshed_Create() failed with %d", ENOMEM)
        ReadyCallback_Dispose(&job_api->m_ReadyCallback);
        return ENOMEM;
    }
    job_api->m_Workers = (struct ThreadWorker*)Longtail_Alloc(sizeof(struct ThreadWorker) * job_api->m_WorkerCount);
    if (!job_api->m_Workers)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        Longtail_Free(job_api->m_Shed);
        ReadyCallback_Dispose(&job_api->m_ReadyCallback);
        return ENOMEM;
    }
    for (uint32_t i = 0; i < job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_Init(&job_api->m_Workers[i]);
        err = ThreadWorker_CreateThread(&job_api->m_Workers[i], job_api->m_Shed, job_api->m_WorkerPriority, job_api->m_ReadyCallback.m_Semaphore, &job_api->m_Stop);
        if (err)
        {
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "ThreadWorker_CreateThread() failed with %d", err)
            while(i-- > 0)
            {
                ThreadWorker_DisposeThread(&job_api->m_Workers[i]);
            }
            Longtail_Free(job_api->m_Workers);
            Longtail_Free(job_api->m_Shed);
            ReadyCallback_Dispose(&job_api->m_ReadyCallback);
            return err;
        }
    }
    return 0;
}

struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count, int worker_priority)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(worker_count, "%u"),
        LONGTAIL_LOGFIELD(worker_priority, "%d")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_INFO)

    LONGTAIL_VALIDATE_INPUT(ctx, worker_priority >= -1 && worker_priority <= 1, return 0)
    struct BikeshedJobAPI* job_api = (struct BikeshedJobAPI*)Longtail_Alloc(sizeof(struct BikeshedJobAPI));
    int err = Bikeshed_Init(job_api, worker_count, worker_priority);
    if (err)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_ERROR, "Bikeshed_Init() failed with %d", err)
        return 0;
    }
    return &job_api->m_BikeshedAPI;
}
