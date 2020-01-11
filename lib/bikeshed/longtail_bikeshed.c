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
    Longtail_DeleteSema(ready_callback->m_Semaphore);
	Longtail_Free(ready_callback->m_Semaphore);
}

static void ReadyCallback_Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
{
    struct ReadyCallback* cb = (struct ReadyCallback*)ready_callback;
    Longtail_PostSema(cb->m_Semaphore, ready_count);
}

static void ReadyCallback_Wait(struct ReadyCallback* cb)
{
    Longtail_WaitSema(cb->m_Semaphore);
}

static int ReadyCallback_Init(struct ReadyCallback* ready_callback)
{
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
    thread_worker->stop = 0;
    thread_worker->shed = 0;
    thread_worker->semaphore = 0;
    thread_worker->thread = 0;
}

static int32_t ThreadWorker_Execute(void* context)
{
    struct ThreadWorker* thread_worker = (struct ThreadWorker*)(context);

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
    thread_worker->shed               = in_shed;
    thread_worker->stop               = in_stop;
    thread_worker->semaphore          = in_semaphore;
    return Longtail_CreateThread(Longtail_Alloc(Longtail_GetThreadSize()), ThreadWorker_Execute, 0, thread_worker, &thread_worker->thread);
}

static int ThreadWorker_JoinThread(struct ThreadWorker* thread_worker)
{
    return Longtail_JoinThread(thread_worker->thread, LONGTAIL_TIMEOUT_INFINITE);
}

static void ThreadWorker_DisposeThread(struct ThreadWorker* thread_worker)
{
    Longtail_DeleteThread(thread_worker->thread);
	Longtail_Free(thread_worker->thread);
}

static void ThreadWorker_Dispose(struct ThreadWorker* thread_worker)
{
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
    struct JobWrapper* wrapper = (struct JobWrapper*)context;
    wrapper->m_JobFunc(wrapper->m_Context);
    if (wrapper->m_JobAPI->m_PendingJobCount <= 0)
    {
        // TODO! Error handling!
        return BIKESHED_TASK_RESULT_COMPLETE;
    }
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_PendingJobCount, -1);
    Longtail_AtomicAdd32(&wrapper->m_JobAPI->m_JobsCompleted, 1);
    return BIKESHED_TASK_RESULT_COMPLETE;
}

static uint32_t Bikeshed_GetWorkerCount(struct Longtail_JobAPI* job_api)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    return bikeshed_job_api->m_WorkerCount;
}

static int Bikeshed_ReserveJobs(struct Longtail_JobAPI* job_api, uint32_t job_count)
{
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
    bikeshed_job_api->m_ReservedJobs = (struct JobWrapper*)Longtail_Alloc(sizeof(struct JobWrapper) * job_count);
    bikeshed_job_api->m_ReservedTasksIDs = (Bikeshed_TaskID*)Longtail_Alloc(sizeof(Bikeshed_TaskID) * job_count);
    if (bikeshed_job_api->m_ReservedJobs && bikeshed_job_api->m_ReservedTasksIDs)
    {
        bikeshed_job_api->m_ReservedJobCount = job_count;
        return 0;
    }
	Longtail_Free(bikeshed_job_api->m_ReservedTasksIDs);
	Longtail_Free(bikeshed_job_api->m_ReservedJobs);
    return ENOMEM;
}

static int Bikeshed_CreateJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_JobFunc job_funcs[], void* job_contexts[], Longtail_JobAPI_Jobs* out_jobs)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    int32_t new_job_count = Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, (int32_t)job_count);
    if (new_job_count > (int32_t)bikeshed_job_api->m_ReservedJobCount)
    {
        Longtail_AtomicAdd32(&bikeshed_job_api->m_SubmittedJobCount, -((int32_t)job_count));
        return ENOMEM;
    }
    uint32_t job_range_start = (uint32_t)(new_job_count - job_count);

    BikeShed_TaskFunc* func = (BikeShed_TaskFunc*)Longtail_Alloc(sizeof(BikeShed_TaskFunc) * job_count);
    void** ctx = (void**)Longtail_Alloc(sizeof(void*) * job_count);
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
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    while (!Bikeshed_AddDependencies(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs, dependency_job_count, (Bikeshed_TaskID*)dependency_jobs))
    {
        Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0);
    }
    return 0;
}

static int Bikeshed_ReadyJobs(struct Longtail_JobAPI* job_api, uint32_t job_count, Longtail_JobAPI_Jobs jobs)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, (Bikeshed_TaskID*)jobs);
    return 0;
}

static int Bikeshed_WaitForAllJobs(struct Longtail_JobAPI* job_api, void* context, Longtail_JobAPI_ProgressFunc process_func)
{
    struct BikeshedJobAPI* bikeshed_job_api = (struct BikeshedJobAPI*)job_api;
    int32_t old_pending_count = 0;
    while (bikeshed_job_api->m_PendingJobCount > 0)
    {
        if (process_func)
        {
            process_func(context, (uint32_t)bikeshed_job_api->m_ReservedJobCount, (uint32_t)bikeshed_job_api->m_JobsCompleted);
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
    if (process_func)
    {
        process_func(context, (uint32_t)bikeshed_job_api->m_SubmittedJobCount, (uint32_t)bikeshed_job_api->m_SubmittedJobCount);
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

static void Bikeshed_Dispose(struct Longtail_API* job_api)
{
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
    job_api->m_BikeshedAPI.m_API.Dispose = Bikeshed_Dispose;
    job_api->m_BikeshedAPI.GetWorkerCount = Bikeshed_GetWorkerCount;
    job_api->m_BikeshedAPI.ReserveJobs = Bikeshed_ReserveJobs;
    job_api->m_BikeshedAPI.CreateJobs = Bikeshed_CreateJobs;
    job_api->m_BikeshedAPI.AddDependecies = Bikeshed_AddDependecies;
    job_api->m_BikeshedAPI.ReadyJobs = Bikeshed_ReadyJobs;
    job_api->m_BikeshedAPI.WaitForAllJobs = Bikeshed_WaitForAllJobs;
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
        return err;
    }

    job_api->m_Shed = Bikeshed_Create(Longtail_Alloc(BIKESHED_SIZE(1048576, 7340032, 1)), 1048576, 7340032, 1, &job_api->m_ReadyCallback.cb);
    job_api->m_Workers = (struct ThreadWorker*)Longtail_Alloc(sizeof(struct ThreadWorker) * job_api->m_WorkerCount);
    if (!job_api->m_Workers)
    {
        ReadyCallback_Dispose(&job_api->m_ReadyCallback);
        return ENOMEM;
    }
    for (uint32_t i = 0; i < job_api->m_WorkerCount; ++i)
    {
        ThreadWorker_Init(&job_api->m_Workers[i]);
        err = ThreadWorker_CreateThread(&job_api->m_Workers[i], job_api->m_Shed, job_api->m_ReadyCallback.m_Semaphore, &job_api->m_Stop);
        if (err)
        {
            while(i-- > 0)
            {
                ThreadWorker_DisposeThread(&job_api->m_Workers[i]);
            }
            Longtail_Free(job_api->m_Workers);
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
