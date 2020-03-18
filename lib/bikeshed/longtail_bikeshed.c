#include "longtail_bikeshed.h"

#include "../../src/longtail.h"
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
    LONGTAIL_FATAL_ASSERT(ready_callback, return)
    Longtail_DeleteSema(ready_callback->m_Semaphore);
	Longtail_Free(ready_callback->m_Semaphore);
}

static void ReadyCallback_Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
{
    LONGTAIL_FATAL_ASSERT(ready_callback, return)
    struct ReadyCallback* cb = (struct ReadyCallback*)ready_callback;
    Longtail_PostSema(cb->m_Semaphore, ready_count);
}

static void ReadyCallback_Wait(struct ReadyCallback* ready_callback)
{
    LONGTAIL_FATAL_ASSERT(ready_callback, return)
    Longtail_WaitSema(ready_callback->m_Semaphore);
}

static int ReadyCallback_Init(struct ReadyCallback* ready_callback)
{
    LONGTAIL_FATAL_ASSERT(ready_callback, return EINVAL)
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
    LONGTAIL_FATAL_ASSERT(thread_worker, return)
    thread_worker->stop = 0;
    thread_worker->shed = 0;
    thread_worker->semaphore = 0;
    thread_worker->thread = 0;
}

static int32_t ThreadWorker_Execute(void* context)
{
    LONGTAIL_FATAL_ASSERT(context, return 0)
    struct ThreadWorker* thread_worker = (struct ThreadWorker*)(context);

    LONGTAIL_FATAL_ASSERT(thread_worker->stop, return 0)
    while (*thread_worker->stop == 0)
    {
        if (!Bikeshed_ExecuteOne(thread_worker->shed, 0))
        {
            Longtail_WaitSema(thread_worker->semaphore);
        }
    }
    return 0;
}

static int ThreadWorker_CreateThread(struct ThreadWorker* thread_worker, Bikeshed in_shed, HLongtail_Sema in_semaphore, int32_t volatile* in_stop)
{
    LONGTAIL_FATAL_ASSERT(thread_worker, return EINVAL)
    LONGTAIL_FATAL_ASSERT(in_shed, return EINVAL)
    LONGTAIL_FATAL_ASSERT(in_semaphore, return EINVAL)
    LONGTAIL_FATAL_ASSERT(in_stop, return EINVAL)
    thread_worker->shed               = in_shed;
    thread_worker->stop               = in_stop;
    thread_worker->semaphore          = in_semaphore;
    return Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), ThreadWorker_Execute, 0, thread_worker, &thread_worker->thread);
}

static int ThreadWorker_JoinThread(struct ThreadWorker* thread_worker)
{
    LONGTAIL_FATAL_ASSERT(thread_worker, return EINVAL)
    return Longtail_JoinThread(thread_worker->thread, LONGTAIL_TIMEOUT_INFINITE);
}

static void ThreadWorker_DisposeThread(struct ThreadWorker* thread_worker)
{
    LONGTAIL_FATAL_ASSERT(thread_worker, return)
    Longtail_DeleteThread(thread_worker->thread);
	Longtail_Free(thread_worker->thread);
}

static void ThreadWorker_Dispose(struct ThreadWorker* thread_worker)
{
    LONGTAIL_FATAL_ASSERT(thread_worker, return)
    ThreadWorker_DisposeThread(thread_worker);
}

struct JobWrapper
{
    struct BikeshedJobAPI* m_JobAPI;
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
    int32_t volatile m_Stop;
    struct JobWrapper* m_ReservedJobs;
    Bikeshed_TaskID* m_ReservedTasksIDs;
    uint32_t m_ReservedJobCount;
    int32_t volatile m_SubmittedJobCount;
    int32_t volatile m_PendingJobCount;
    int32_t volatile m_JobsCompleted;
};

static enum Bikeshed_TaskResult Bikeshed_Job(Bikeshed shed, Bikeshed_TaskID task_id, uint8_t channel, void* context)
{
    LONGTAIL_FATAL_ASSERT(shed, return (enum Bikeshed_TaskResult)-1)
    LONGTAIL_FATAL_ASSERT(context, return (enum Bikeshed_TaskResult)-1)
    struct JobWrapper* wrapper = (struct JobWrapper*)context;
    int res = wrapper->m_JobFunc(wrapper->m_Context, task_id);
    if (res == EBUSY)
    {
        return BIKESHED_TASK_RESULT_BLOCKED;
    }
    LONGTAIL_FATAL_ASSERT(wrapper->m_JobAPI->m_PendingJobCount > 0, return BIKESHED_TASK_RESULT_COMPLETE)
    LONGTAIL_FATAL_ASSERT(res == 0, return BIKESHED_TASK_RESULT_COMPLETE)
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_PendingJobCount, -1);
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_JobsCompleted, 1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

static uint32_t Bikeshed_GetWorkerCount(struct Longtail_JobAPI* job_api)
{
    LONGTAIL_FATAL_ASSERT(job_api, return 0)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    return bikeshed_job_api->m_WorkerCount;
}

static int Bikeshed_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count)
{
    LONGTAIL_FATAL_ASSERT(job_api, return 0)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    if (bikeshed_job_api->m_PendingJobCount)
    {
        return EBUSY;
    }
    if (bikeshed_job_api->m_SubmittedJobCount)
    {
        return EBUSY;
    }
    if (bikeshed_job_api->m_ReservedJobs)
    {
        return EBUSY;
    }
    if (bikeshed_job_api->m_ReservedJobCount)
    {
        return EBUSY;
    }
    size_t reserved_jobs_size = sizeof(struct JobWrapper) * job_count;
    bikeshed_job_api->m_ReservedJobs = (struct JobWrapper*)Longtail_Alloc(reserved_jobs_size);
    if (!bikeshed_job_api->m_ReservedJobs)
    {
        // TODO: Log
        return ENOMEM;
    }
    size_t reserved_task_ids_size = sizeof(Bikeshed_TaskID) * job_count;
    bikeshed_job_api->m_ReservedTasksIDs = (Bikeshed_TaskID*)Longtail_Alloc(reserved_task_ids_size);
    if (!bikeshed_job_api->m_ReservedTasksIDs)
    {
        // TODO: Log
        Longtail_Free(bikeshed_job_api->m_ReservedJobs);
        return ENOMEM;
    }
    bikeshed_job_api->m_ReservedJobCount = job_count;
    return 0;
}

static int Bikeshed_CreateJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs)
{
    LONGTAIL_FATAL_ASSERT(job_api, return 0)
    LONGTAIL_FATAL_ASSERT(job_funcs, return 0)
    LONGTAIL_FATAL_ASSERT(job_contexts, return 0)
    LONGTAIL_FATAL_ASSERT(out_jobs, return 0)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    int32_t new_job_count = Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, (int32_t)job_count);
    LONGTAIL_FATAL_ASSERT(new_job_count > 0, return EINVAL);
    if (new_job_count > (int32_t)bikeshed_job_api->m_ReservedJobCount)
    {
        Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, -((int32_t)job_count));
        return ENOMEM;
    }
    uint32_t job_range_start = (uint32_t)(new_job_count - job_count);

    BikeShed_TaskFunc* func = (BikeShed_TaskFunc*)Longtail_Alloc(sizeof(BikeShed_TaskFunc) * job_count);
    if (!func)
    {
        // TODO: Log
        return ENOMEM;
    }
    void** ctx = (void**)Longtail_Alloc(sizeof(void*) * job_count);
    if (!ctx)
    {
        // TODO: Log
        return ENOMEM;
    }
    Bikeshed_TaskID* task_ids = &bikeshed_job_api->m_ReservedTasksIDs[job_range_start];
    for (uint32_t i = 0; i < job_count; ++i)
    {
        struct JobWrapper* job_wrapper = &bikeshed_job_api->m_ReservedJobs[job_range_start + i];
        job_wrapper->m_JobAPI = bikeshed_job_api;
        job_wrapper->m_Context = job_contexts[i];
        job_wrapper->m_JobFunc = job_funcs[i];
        func[i] = Bikeshed_Job;
        ctx[i] = job_wrapper;
    }

    while (!Bikeshed_CreateTasks(bikeshed_job_api->m_Shed, job_count, func, ctx, task_ids))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }

    Longtail_AtomicAdd32(&bikeshed_job_api->m_PendingJobCount, (int)job_count);

	Longtail_Free(ctx);
	Longtail_Free(func);
    *out_jobs = task_ids;
    return 0;
}

static int Bikeshed_AddDependecies(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs, uint32_t dependency_job_count, Longtail_JobAPI_Jobs dependency_jobs)
{
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(dependency_jobs, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_count > 0, return EINVAL)
    LONGTAIL_FATAL_ASSERT(dependency_job_count > 0, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    while (!Bikeshed_AddDependencies(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs, dependency_job_count, (Bikeshed_TaskID*)dependency_jobs))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }
    return 0;
}

static int Bikeshed_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs)
{
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    LONGTAIL_FATAL_ASSERT(job_count, return EINVAL)
    LONGTAIL_FATAL_ASSERT(jobs, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs);
    return 0;
}

static int Bikeshed_WaitForAllJobs(struct Longtail_JobAPI* job_api, struct Longtail_ProgressAPI* progressAPI)
{
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    int32_t old_pending_count = 0;
    while (bikeshed_job_api->m_PendingJobCount > 0)
    {
        if (progressAPI)
        {
            progressAPI->OnProgress(progressAPI,(uint32_t)bikeshed_job_api->m_ReservedJobCount, (uint32_t)bikeshed_job_api->m_JobsCompleted);
        }
        if (Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0))
        {
            continue;
        }
        if (old_pending_count != bikeshed_job_api->m_PendingJobCount)
        {
            old_pending_count = bikeshed_job_api->m_PendingJobCount;
        }
        Longtail_Sleep(1000);
    }
    if (progressAPI)
    {
        progressAPI->OnProgress(progressAPI, (uint32_t)bikeshed_job_api->m_SubmittedJobCount, (uint32_t)bikeshed_job_api->m_SubmittedJobCount);
    }
    bikeshed_job_api->m_SubmittedJobCount = 0;
	Longtail_Free(bikeshed_job_api->m_ReservedTasksIDs);
    bikeshed_job_api->m_ReservedTasksIDs = 0;
	Longtail_Free(bikeshed_job_api->m_ReservedJobs);
    bikeshed_job_api->m_ReservedJobs = 0;
    bikeshed_job_api->m_JobsCompleted = 0;
    bikeshed_job_api->m_ReservedJobCount = 0;
    return 0;
}

static int Bikeshed_ResumeJob(struct Longtail_JobAPI* job_api, uint32_t job_id)
{
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, 1, &job_id);
    return 0;
}

static void Bikeshed_Dispose(struct Longtail_API* job_api)
{
    LONGTAIL_FATAL_ASSERT(job_api, return)
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

static int Bikeshed_Init(struct BikeshedJobAPI* job_api, uint32_t worker_count)
{
    LONGTAIL_FATAL_ASSERT(job_api, return EINVAL)
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
    job_api->m_Stop = 0;
    job_api->m_ReservedJobs = 0;
    job_api->m_ReservedTasksIDs = 0;
    job_api->m_ReservedJobCount = 0;
    job_api->m_SubmittedJobCount = 0;
    job_api->m_PendingJobCount = 0;
    job_api->m_JobsCompleted = 0;

	int err = ReadyCallback_Init(&job_api->m_ReadyCallback);
    if (err)
    {
        // TODO: Log
        return err;
    }

    job_api->m_Shed = Bikeshed_Create(Longtail_Alloc(BIKESHED_SIZE(1048576, 7340032, 1)), 1048576, 7340032, 1, &job_api->m_ReadyCallback.cb);
    if (!job_api->m_Shed)
    {
        // TODO: Log
        ReadyCallback_Dispose(&job_api->m_ReadyCallback);
        return ENOMEM;
    }
    job_api->m_Workers = (struct ThreadWorker*)Longtail_Alloc(sizeof(struct ThreadWorker) * job_api->m_WorkerCount);
    if (!job_api->m_Workers)
    {
        Longtail_Free(job_api->m_Shed);
        ReadyCallback_Dispose(&job_api->m_ReadyCallback);
        return ENOMEM;
    }
    for (uint32_t i = 0; i < job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_Init(&job_api->m_Workers[i]);
        err = ThreadWorker_CreateThread(&job_api->m_Workers[i], job_api->m_Shed, job_api->m_ReadyCallback.m_Semaphore, &job_api->m_Stop);
        if (err)
        {
            // TODO: Log
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

struct Longtail_JobAPI* Longtail_CreateBikeshedJobAPI(uint32_t worker_count)
{
    struct BikeshedJobAPI* job_api = (struct BikeshedJobAPI*)Longtail_Alloc(sizeof(struct BikeshedJobAPI));
    Bikeshed_Init(job_api, worker_count);
    return &job_api->m_BikeshedAPI;
}
