#pragma once

#define BIKESHED_IMPLEMENTATION
#include "../third-party/bikeshed/bikeshed.h"
#include "../third-party/jc_containers/src/jc_hashtable.h"
#include "../third-party/lizard/lib/lizard_common.h"
#include "../third-party/lizard/lib/lizard_decompress.h"
#include "../third-party/lizard/lib/lizard_compress.h"
#include "../third-party/meow_hash/meow_hash_x64_aesni.h"
#include "../third-party/nadir/src/nadir.h"
#include "../third-party/trove/src/trove.h"
#include "../src/longtail.h"

#if !defined(PLATFORM_ATOMICADD)
    #if defined(__clang__) || defined(__GNUC__)
        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (__sync_add_and_fetch (value, amount))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (_InterlockedExchangeAdd((volatile LONG *)value, amount) + amount)
    #else
        inline int32_t Platform_NonAtomicAdd(volatile int32_t* store, int32_t value) { *store += value; return *store; }
        #define PLATFORM_ATOMICADD_PRIVATE(value, amount) (Platform_NonAtomicAdd(value, amount))
    #endif
#else
    #define PLATFORM_ATOMICADD_PRIVATE PLATFORM_ATOMICADD
#endif

#if !defined(PLATFORM_SLEEP)
    #if defined(__clang__) || defined(__GNUC__)
        #define PLATFORM_SLEEP_PRIVATE(timeout_us) (::usleep((useconds_t)timeout_us))
    #elif defined(_MSC_VER)
        #if !defined(_WINDOWS_)
            #define WIN32_LEAN_AND_MEAN
            #include <Windows.h>
            #undef WIN32_LEAN_AND_MEAN
        #endif

        #define PLATFORM_SLEEP_PRIVATE(timeout_us) (::Sleep((DWORD)(timeout_us / 1000)))
    #endif
#else
    #define PLATFORM_SLEEP_PRIVATE PLATFORM_SLEEP
#endif

#if defined(_WIN32)

#if !defined(_WINDOWS_)
    #if !defined(_WINDOWS_)
        #define WIN32_LEAN_AND_MEAN
        #include <Windows.h>
        #undef WIN32_LEAN_AND_MEAN
    #endif
#endif


inline int GetCPUCount()
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
}

inline void Trove_NormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '\\' ? '/' : *path;
    }
}

inline void Trove_DenormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '/' ? '\\' : *path;
    }
}

inline int Trove_CreateDirectory(const char* path)
{
    BOOL ok = ::CreateDirectoryA(path, NULL);
    return ok;
}

inline int Trove_MoveFile(const char* source, const char* target)
{
    BOOL ok = ::MoveFileA(source, target);
    return ok ? 1 : 0;
}

#endif

#if defined(__APPLE__) || defined(__linux__)

inline int GetCPUCount()
{
   return sysconf(_SC_NPROCESSORS_ONLN);
}

inline void Trove_NormalizePath(char* )
{

}

inline void Trove_DenormalizePath(char* )
{

}

inline int Trove_CreateDirectory(const char* path)
{
    return 0;
}

inline int Trove_MoveFile(const char* source, const char* target)
{
    return 0;
}

#endif

struct ReadyCallback
{
    Bikeshed_ReadyCallback cb = {Ready};
    ReadyCallback()
    {
        m_Semaphore = nadir::CreateSema(malloc(nadir::GetSemaSize()), 0);
    }
    ~ReadyCallback()
    {
        nadir::DeleteSema(m_Semaphore);
        free(m_Semaphore);
    }
    static void Ready(struct Bikeshed_ReadyCallback* ready_callback, uint8_t channel, uint32_t ready_count)
    {
        ReadyCallback* cb = (ReadyCallback*)ready_callback;
        nadir::PostSema(cb->m_Semaphore, ready_count);
    }
    static void Wait(ReadyCallback* cb)
    {
        nadir::WaitSema(cb->m_Semaphore);
    }
    nadir::HSema m_Semaphore;
};

struct ThreadWorker
{
    ThreadWorker()
        : stop(0)
        , shed(0)
        , semaphore(0)
        , thread(0)
    {
    }

    ~ThreadWorker()
    {
    }

    bool CreateThread(Bikeshed in_shed, nadir::HSema in_semaphore, int32_t volatile* in_stop)
    {
        shed               = in_shed;
        stop               = in_stop;
        semaphore          = in_semaphore;
        thread             = nadir::CreateThread(malloc(nadir::GetThreadSize()), ThreadWorker::Execute, 0, this);
        return thread != 0;
    }

    void JoinThread()
    {
        nadir::JoinThread(thread, nadir::TIMEOUT_INFINITE);
    }

    void DisposeThread()
    {
        nadir::DeleteThread(thread);
        free(thread);
    }

    static int32_t Execute(void* context)
    {
        ThreadWorker* _this = reinterpret_cast<ThreadWorker*>(context);

        while (*_this->stop == 0)
        {
            if (!Bikeshed_ExecuteOne(_this->shed, 0))
            {
                nadir::WaitSema(_this->semaphore);
            }
        }
        return 0;
    }

    int32_t volatile*   stop;
    Bikeshed            shed;
    nadir::HSema        semaphore;
    nadir::HThread      thread;
};

struct MeowHashAPI
{
    HashAPI m_HashAPI;
    MeowHashAPI()
        : m_HashAPI{
            BeginContext,
            Hash,
            EndContext
        }
    {
    }

    static HashAPI::HContext BeginContext(HashAPI* hash_api)
    {
        meow_state* state = (meow_state*)malloc(sizeof(meow_state));
        MeowBegin(state, MeowDefaultSeed);
        return (HashAPI::HContext)state;
    }
    static void Hash(HashAPI* hash_api, HashAPI::HContext context, uint32_t length, void* data)
    {
        meow_state* state = (meow_state*)context;
        MeowAbsorb(state, length, data);
    }
    static uint64_t EndContext(HashAPI* hash_api, HashAPI::HContext context)
    {
        meow_state* state = (meow_state*)context;
        uint64_t hash = MeowU64From(MeowEnd(state, 0), 0);
        free(state);
        return hash;
    }
};

struct TroveStorageAPI
{
    StorageAPI m_StorageAPI;
    TroveStorageAPI()
        : m_StorageAPI{
            OpenReadFile,
            GetSize,
            Read,
            CloseRead,
            OpenWriteFile,
            Write,
            CloseWrite,
            CreateDir,
            RenameFile,
            ConcatPath,
            IsDir,
            IsFile,
            StartFind,
            FindNext,
            CloseFind,
            GetFileName,
            GetDirectoryName
        }
    {

    }
    static StorageAPI::HOpenFile OpenReadFile(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        StorageAPI::HOpenFile r = (StorageAPI::HOpenFile)Trove_OpenReadFile(tmp_path);
        free(tmp_path);
        return r;
    }
    static uint64_t GetSize(StorageAPI* , StorageAPI::HOpenFile f)
    {
        return Trove_GetFileSize((HTroveOpenReadFile)f);
    }
    static int Read(StorageAPI* , StorageAPI::HOpenFile f, uint64_t offset, uint64_t length, void* output)
    {
        return Trove_Read((HTroveOpenReadFile)f, offset,length, output);
    }
    static void CloseRead(StorageAPI* , StorageAPI::HOpenFile f)
    {
        Trove_CloseReadFile((HTroveOpenReadFile)f);
    }

    static StorageAPI::HOpenFile OpenWriteFile(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        StorageAPI::HOpenFile r = (StorageAPI::HOpenFile)Trove_OpenWriteFile(tmp_path);
        free(tmp_path);
        return r;
    }
    static int Write(StorageAPI* , StorageAPI::HOpenFile f, uint64_t offset, uint64_t length, const void* input)
    {
        return Trove_Write((HTroveOpenWriteFile)f, offset,length, input);
    }
    static void CloseWrite(StorageAPI* , StorageAPI::HOpenFile f)
    {
        Trove_CloseWriteFile((HTroveOpenWriteFile)f);
    }

    static int CreateDir(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int ok = Trove_CreateDirectory(tmp_path);
        free(tmp_path);
        return ok;
    }

    static int RenameFile(StorageAPI* , const char* source_path, const char* target_path)
    {
        char* tmp_source_path = strdup(source_path);
        Trove_DenormalizePath(tmp_source_path);
        char* tmp_target_path = strdup(target_path);
        Trove_DenormalizePath(tmp_target_path);
        int ok = Trove_MoveFile(tmp_source_path, tmp_target_path);
        free(tmp_target_path);
        free(tmp_source_path);
        return ok;
    }
    static char* ConcatPath(StorageAPI* , const char* root_path, const char* sub_path)
    {
        // TODO: Trove is inconsistent - it works on normalized paths!
        char* path = (char*)Trove_ConcatPath(root_path, sub_path);
        Trove_NormalizePath(path);
        return path;
    }

    static int IsDir(StorageAPI* , const char* path)
    {
        DWORD attrs = GetFileAttributesA(path);
        if (attrs == INVALID_FILE_ATTRIBUTES)
        {
            return 0;
        }
        return (attrs & FILE_ATTRIBUTE_DIRECTORY) ? 1 : 0;
    }

    static int IsFile(StorageAPI* , const char* path)
    {
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        DWORD attrs = GetFileAttributesA(tmp_path);
        free(tmp_path);
        if (attrs == INVALID_FILE_ATTRIBUTES)
        {
            return 0;
        }
        return (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0;
    }

    static StorageAPI::HIterator StartFind(StorageAPI* , const char* path)
    {
        StorageAPI::HIterator iterator = (StorageAPI::HIterator)malloc(Trove_GetFSIteratorSize());
        char* tmp_path = strdup(path);
        Trove_DenormalizePath(tmp_path);
        int ok = Trove_StartFind((HTrove_FSIterator)iterator, tmp_path);
        free(tmp_path);
        if (!ok)
        {
            free(iterator);
            iterator = 0;
        }
        return iterator;
    }
    static int FindNext(StorageAPI* , StorageAPI::HIterator iterator)
    {
        return Trove_FindNext((HTrove_FSIterator)iterator);
    }
    static void CloseFind(StorageAPI* , StorageAPI::HIterator iterator)
    {
        Trove_CloseFind((HTrove_FSIterator)iterator);
        free(iterator);
    }
    static const char* GetFileName(StorageAPI* , StorageAPI::HIterator iterator)
    {
        return Trove_GetFileName((HTrove_FSIterator)iterator);
    }
    static const char* GetDirectoryName(StorageAPI* , StorageAPI::HIterator iterator)
    {
        return Trove_GetDirectoryName((HTrove_FSIterator)iterator);
    }

};

struct LizardCompressionAPI
{
    CompressionAPI m_CompressionAPI;

    LizardCompressionAPI()
        : m_CompressionAPI{
            GetDefaultSettings,
            GetMaxCompressionSetting,
            CreateCompressionContext,
            GetMaxCompressedSize,
            Compress,
            DeleteCompressionContext,
            CreateDecompressionContext,
            Decompress,
            DeleteDecompressionContext
            }
    {
    }

    static int DefaultCompressionSetting;
    static int MaxCompressionSetting;
    static CompressionAPI::HSettings GetDefaultSettings(CompressionAPI*)
    {
        return (CompressionAPI::HSettings)&DefaultCompressionSetting;
    }
    static CompressionAPI::HSettings GetMaxCompressionSetting(CompressionAPI*)
    {
        return (CompressionAPI::HSettings)&MaxCompressionSetting;
    }
    static CompressionAPI::HCompressionContext CreateCompressionContext(CompressionAPI*, CompressionAPI::HSettings settings)
    {
        return (CompressionAPI::HCompressionContext)settings;
    }
    static size_t GetMaxCompressedSize(CompressionAPI*, CompressionAPI::HCompressionContext , size_t size)
    {
        return (size_t)Lizard_compressBound((int)size);
    }
    static size_t Compress(CompressionAPI*, CompressionAPI::HCompressionContext context, const char* uncompressed, char* compressed, size_t uncompressed_size, size_t max_compressed_size)
    {
        int compression_setting = *(int*)context;
        int compressed_size = Lizard_compress(uncompressed, compressed, (int)uncompressed_size, (int)max_compressed_size, compression_setting);
        return (size_t)(compressed_size >= 0 ? compressed_size : 0);
    }
    static void DeleteCompressionContext(CompressionAPI*, CompressionAPI::HCompressionContext)
    {
    }
    static CompressionAPI::HDecompressionContext CreateDecompressionContext(CompressionAPI* compression_api)
    {
        return (CompressionAPI::HDecompressionContext)GetDefaultSettings(compression_api);
    }
    static size_t Decompress(CompressionAPI*, CompressionAPI::HDecompressionContext, const char* compressed, char* uncompressed, size_t compressed_size, size_t uncompressed_size)
    {
        int result = Lizard_decompress_safe(compressed, uncompressed, (int)compressed_size, (int)uncompressed_size);
        return (size_t)(result >= 0 ? result : 0);
    }
    static void DeleteDecompressionContext(CompressionAPI*, CompressionAPI::HDecompressionContext)
    {
    }
};

int LizardCompressionAPI::DefaultCompressionSetting = 44;
int LizardCompressionAPI::MaxCompressionSetting = LIZARD_MAX_CLEVEL;

struct BikeshedJobAPI
{
    JobAPI m_JobAPI;

    struct JobWrapper
    {
        BikeshedJobAPI* m_JobAPI;
        JobAPI::TJobFunc m_JobFunc;
        void* m_Context;
    };

    ReadyCallback m_ReadyCallback;
    Bikeshed m_Shed;
    uint32_t m_WorkerCount;
    ThreadWorker* m_Workers;
    int32_t volatile m_Stop;
    JobWrapper* m_ReservedJobs;
    uint32_t m_SubmittedJobCount;
    int32_t volatile m_PendingJobCount;

    BikeshedJobAPI()
        : m_JobAPI{
            ReserveJobs,
            SubmitJobs,
            WaitForAllJobs
            }
        , m_Shed(0)
        , m_WorkerCount(GetCPUCount() - 1)
        , m_Workers(0)
        , m_Stop(0)
        , m_ReservedJobs(0)
        , m_SubmittedJobCount(0)
        , m_PendingJobCount(0)
    {
        m_Shed = Bikeshed_Create(malloc(BIKESHED_SIZE(65536, 0, 1)), 65536, 0, 1, &m_ReadyCallback.cb);
        if (m_WorkerCount == 0)
        {
            m_WorkerCount = 1;
        }
        m_Workers = new ThreadWorker[m_WorkerCount];
        for (uint32_t i = 0; i < m_WorkerCount; ++i)
        {
            m_Workers[i].CreateThread(m_Shed, m_ReadyCallback.m_Semaphore, &m_Stop);
        }
    }
    ~BikeshedJobAPI()
    {
        nadir::AtomicAdd32(&m_Stop, 1);
        ReadyCallback::Ready(&m_ReadyCallback.cb, 0, m_WorkerCount);
        for (uint32_t i = 0; i < m_WorkerCount; ++i)
        {
            m_Workers[i].JoinThread();
        }
        delete []m_Workers;
    }

    static int ReserveJobs(JobAPI* job_api, uint32_t job_count)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        if (bikeshed_job_api->m_PendingJobCount)
        {
            return 0;
        }
        if (bikeshed_job_api->m_SubmittedJobCount)
        {
            return 0;
        }
        if (bikeshed_job_api->m_ReservedJobs)
        {
            return 0;
        }
        bikeshed_job_api->m_ReservedJobs = (JobWrapper*)malloc(sizeof(JobWrapper) * job_count);
        return bikeshed_job_api->m_ReservedJobs != 0;
    }

    static void SubmitJobs(JobAPI* job_api, uint32_t job_count, JobAPI::TJobFunc job_funcs[], void* job_contexts[])
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;

        BikeShed_TaskFunc* func = (BikeShed_TaskFunc*)malloc(sizeof(BikeShed_TaskFunc) * job_count);
        void** ctx = (void**)malloc(sizeof(void*) * job_count);
        Bikeshed_TaskID* task_ids = (Bikeshed_TaskID*)malloc(sizeof(Bikeshed_TaskID) * job_count);
        for (uint32_t i = 0; i < job_count; ++i)
        {
            JobWrapper* job_wrapper = &bikeshed_job_api->m_ReservedJobs[bikeshed_job_api->m_SubmittedJobCount + i];
            job_wrapper->m_JobAPI = bikeshed_job_api;
            job_wrapper->m_Context = job_contexts[i];
            job_wrapper->m_JobFunc = job_funcs[i];
            func[i] = Job;
            ctx[i] = job_wrapper;
        }

        bikeshed_job_api->m_SubmittedJobCount += job_count;

        while (!Bikeshed_CreateTasks(bikeshed_job_api->m_Shed, job_count, func, ctx, task_ids))
        {
            PLATFORM_SLEEP_PRIVATE(1000);
        }

        free(ctx);
        free(func);

        {
            PLATFORM_ATOMICADD_PRIVATE(&bikeshed_job_api->m_PendingJobCount, job_count);
            Bikeshed_ReadyTasks(bikeshed_job_api->m_Shed, job_count, task_ids);
        }
		free(task_ids);
	}
    static void WaitForAllJobs(JobAPI* job_api)
    {
        BikeshedJobAPI* bikeshed_job_api = (BikeshedJobAPI*)job_api;
        int32_t old_pending_count = 0;
        while (bikeshed_job_api->m_PendingJobCount > 0)
        {
            if (Bikeshed_ExecuteOne(bikeshed_job_api->m_Shed, 0))
            {
                continue;
            }
            if (old_pending_count != bikeshed_job_api->m_PendingJobCount)
            {
                old_pending_count = bikeshed_job_api->m_PendingJobCount;
            }
            PLATFORM_SLEEP_PRIVATE(1000);
        }
        bikeshed_job_api->m_SubmittedJobCount = 0;
        free(bikeshed_job_api->m_ReservedJobs);
        bikeshed_job_api->m_ReservedJobs = 0;
    }

    static Bikeshed_TaskResult Job(Bikeshed shed, Bikeshed_TaskID, uint8_t, void* context)
    {
        JobWrapper* wrapper = (JobWrapper*)context;
        wrapper->m_JobFunc(wrapper->m_Context);
        PLATFORM_ATOMICADD_PRIVATE(&wrapper->m_JobAPI->m_PendingJobCount, -1);
        return BIKESHED_TASK_RESULT_COMPLETE;
    }
};

#undef PLATFORM_ATOMICADD_PRIVATE
#undef PLATFORM_SLEEP_PRIVATE
