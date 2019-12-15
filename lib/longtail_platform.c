#include "longtail_platform.h"
#include "../src/longtail.h"
#include <stdint.h>
#include <errno.h>

#if defined(_WIN32)

#include <Windows.h>

static int Win32ErrorToErrno(DWORD err)
{
    switch (err)
    {
        case ERROR_SUCCESS:
        return 0;
        case ERROR_FILE_NOT_FOUND:
        case ERROR_PATH_NOT_FOUND:
        case ERROR_INVALID_TARGET_HANDLE:
        return ENOENT;
        case ERROR_TOO_MANY_OPEN_FILES:
        case ERROR_SHARING_BUFFER_EXCEEDED:
        case ERROR_NOT_ENOUGH_MEMORY:
        case ERROR_OUTOFMEMORY:
        case ERROR_TOO_MANY_SEMAPHORES:
        case ERROR_NO_MORE_SEARCH_HANDLES:
        case ERROR_MAX_THRDS_REACHED:
        return ENOMEM;
        case ERROR_ACCESS_DENIED:
        case ERROR_INVALID_ACCESS:
        case ERROR_WRITE_PROTECT:
        case ERROR_SHARING_VIOLATION:
        case ERROR_LOCK_VIOLATION:
        case ERROR_NETWORK_ACCESS_DENIED:
        case ERROR_INVALID_PASSWORD:
        case ERROR_EXCL_SEM_ALREADY_OWNED:
        case ERROR_FORMS_AUTH_REQUIRED:
        case ERROR_NOT_OWNER:
        case ERROR_OPLOCK_NOT_GRANTED:
        return EACCES;
        case ERROR_INVALID_HANDLE:
        case ERROR_INVALID_DATA:
        case ERROR_NOT_SAME_DEVICE:
        case ERROR_BAD_COMMAND:
        case ERROR_BAD_LENGTH:
        case ERROR_NOT_SUPPORTED:
        case ERROR_INVALID_PARAMETER:
        case ERROR_SEM_IS_SET:
        case ERROR_TOO_MANY_SEM_REQUESTS:
        case ERROR_BUFFER_OVERFLOW:
        case ERROR_INSUFFICIENT_BUFFER:
        case ERROR_INVALID_NAME:
        case ERROR_INVALID_LEVEL:
        case ERROR_DIRECT_ACCESS_HANDLE:
        case ERROR_NEGATIVE_SEEK:
        case ERROR_SEEK_ON_DEVICE:
        case ERROR_BAD_ARGUMENTS:
        case ERROR_BAD_PATHNAME:
        case ERROR_SEM_NOT_FOUND:
        case ERROR_FILENAME_EXCED_RANGE:
        case ERROR_DIRECTORY:
        return EINVAL;
        case ERROR_INVALID_DRIVE:
        return ENODEV;
        case ERROR_CURRENT_DIRECTORY:
        case ERROR_BAD_UNIT:
        case ERROR_NOT_READY:
        case ERROR_REM_NOT_LIST:
        case ERROR_NO_VOLUME_LABEL:
        case ERROR_MOD_NOT_FOUND:
        case ERROR_PROC_NOT_FOUND:
        return ENOENT;
        break;
        case ERROR_SEEK:
        case ERROR_WRITE_FAULT:
        case ERROR_READ_FAULT:
        case ERROR_SECTOR_NOT_FOUND:
        case ERROR_NOT_DOS_DISK:
        case ERROR_CANNOT_MAKE:
        case ERROR_NET_WRITE_FAULT:
        case ERROR_BROKEN_PIPE:
        case ERROR_OPEN_FAILED:
        case ERROR_FILE_TOO_LARGE:
        case ERROR_BAD_FILE_TYPE:
        case ERROR_DISK_TOO_FRAGMENTED:
        return EIO;
        case ERROR_HANDLE_DISK_FULL:
        case ERROR_DISK_FULL:
        return ENOSPC;
        case ERROR_FILE_EXISTS:
        case ERROR_ALREADY_EXISTS:
        return EEXIST;
        case ERROR_SEM_TIMEOUT:
        return ETIME;
        case ERROR_WAIT_NO_CHILDREN:
        return ECHILD;
        case ERROR_BUSY_DRIVE:
        case ERROR_PATH_BUSY:
        case ERROR_BUSY:
        case ERROR_PIPE_BUSY:
        return EBUSY;
        default:
        return EINVAL;
    }
}

size_t Longtail_GetCPUCount()
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return (size_t)sysinfo.dwNumberOfProcessors;
}

void Longtail_Sleep(uint64_t timeout_us)
{
    DWORD wait_ms = timeout_us == LONGTAIL_TIMEOUT_INFINITE ? INFINITE : (DWORD)(timeout_us / 1000);
    Sleep(wait_ms);
}

int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount)
{
    return InterlockedAdd((LONG volatile*)value, amount);
}

struct Longtail_Thread
{
    HANDLE              m_Handle;
    Longtail_ThreadFunc m_ThreadFunc;
    void*               m_ContextData;
};

static DWORD WINAPI ThreadStartFunction(_In_ LPVOID lpParameter)
{
    struct Longtail_Thread* thread = (struct Longtail_Thread*)lpParameter;
    int     result = thread->m_ThreadFunc(thread->m_ContextData);
    return (DWORD)result;
}

size_t Longtail_GetThreadSize()
{
    return sizeof(struct Longtail_Thread);
}

int Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data, HLongtail_Thread* out_thread)
{
    struct Longtail_Thread* thread = (struct Longtail_Thread*)mem;
    thread->m_ThreadFunc = thread_func;
    thread->m_ContextData = context_data;
    thread->m_Handle = CreateThread(
        0,
        stack_size,
        ThreadStartFunction,
        thread,
        0,
        0);
    if (thread->m_Handle == INVALID_HANDLE_VALUE)
    {
        return Win32ErrorToErrno(GetLastError());
    }
    *out_thread = thread;
    return 0;
}

int Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us)
{
    if (thread->m_Handle == 0)
    {
        return 0;
    }
    DWORD wait_ms = (timeout_us == LONGTAIL_TIMEOUT_INFINITE) ? INFINITE : (DWORD)(timeout_us / 1000);
    DWORD result  = WaitForSingleObject(thread->m_Handle, wait_ms);
    switch (result)
    {
        case WAIT_OBJECT_0:
        return 0;
        case WAIT_TIMEOUT:
        return ETIME;
        case WAIT_FAILED:
        return Win32ErrorToErrno(GetLastError());
        default:
        return EINVAL;
    }
}

void Longtail_DeleteThread(HLongtail_Thread thread)
{
    CloseHandle(thread->m_Handle);
    thread->m_Handle = INVALID_HANDLE_VALUE;
}

struct Longtail_Sema
{
    HANDLE m_Handle;
};

size_t Longtail_GetSemaSize()
{
    return sizeof(struct Longtail_Sema);
}

int Longtail_CreateSema(void* mem, int initial_count, HLongtail_Sema* out_sema)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;
    semaphore->m_Handle = CreateSemaphore(NULL, initial_count, 0x7fffffff, NULL);
    if (semaphore->m_Handle == INVALID_HANDLE_VALUE)
    {
        return Win32ErrorToErrno(GetLastError());;
    }
    *out_sema = semaphore;
    return 0;
}

int Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    return 0 != ReleaseSemaphore(
                    semaphore->m_Handle,
                    count,
                    NULL);
}

int Longtail_WaitSema(HLongtail_Sema semaphore)
{
    return WAIT_OBJECT_0 == WaitForSingleObject(semaphore->m_Handle, INFINITE);
}

void Longtail_DeleteSema(HLongtail_Sema semaphore)
{
    CloseHandle(semaphore->m_Handle);
}

struct Longtail_SpinLock
{
    SRWLOCK m_Lock;
};

size_t Longtail_GetSpinLockSize()
{
    return sizeof(struct Longtail_SpinLock);
}

int Longtail_CreateSpinLock(void* mem, HLongtail_SpinLock* out_spin_lock)
{
    HLongtail_SpinLock spin_lock = (HLongtail_SpinLock)mem;
    InitializeSRWLock(&spin_lock->m_Lock);
    *out_spin_lock = spin_lock;
    return 0;
}

void Longtail_DeleteSpinLock(HLongtail_SpinLock spin_lock)
{
}

void Longtail_LockSpinLock(HLongtail_SpinLock spin_lock)
{
    AcquireSRWLockExclusive(&spin_lock->m_Lock);
}

void Longtail_UnlockSpinLock(HLongtail_SpinLock spin_lock)
{
    ReleaseSRWLockExclusive(&spin_lock->m_Lock);
}








void Longtail_NormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '\\' ? '/' : *path;
    }
}

void Longtail_DenormalizePath(char* path)
{
    while (*path)
    {
        *path++ = *path == '/' ? '\\' : *path;
    }
}

int Longtail_CreateDirectory(const char* path)
{
    BOOL ok = CreateDirectoryA(path, NULL);
    return ok;
}

int Longtail_MoveFile(const char* source, const char* target)
{
    BOOL ok = MoveFileA(source, target);
    return ok ? 1 : 0;
}

int Longtail_IsDir(const char* path)
{
    DWORD attrs = GetFileAttributesA(path);
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        return 0;
    }
    return (attrs & FILE_ATTRIBUTE_DIRECTORY) ? 1 : 0;
}

int Longtail_IsFile(const char* path)
{
    DWORD attrs = GetFileAttributesA(path);
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        return 0;
    }
    return (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0;
}

int Longtail_RemoveDir(const char* path)
{
    int ok = RemoveDirectoryA(path) == TRUE;
    return ok;
}

int Longtail_RemoveFile(const char* path)
{
    int ok = DeleteFileA(path) == TRUE;
    return ok;
}

struct Longtail_FSIterator_private
{
    WIN32_FIND_DATAA m_FindData;
    HANDLE m_Handle;
};

size_t Longtail_GetFSIteratorSize()
{
    return sizeof(struct Longtail_FSIterator_private);
}

static int IsSkippableFile(HLongtail_FSIterator fs_iterator)
{
    const char* p = fs_iterator->m_FindData.cFileName;
    if ((*p++) != '.')
    {
        return 0;
    }
    if ((*p) == '\0')
    {
        return 1;
    }
    if ((*p++) != '.')
    {
        return 0;
    }
    if ((*p) == '\0')
    {
        return 1;
    }
    return 0;
}

static int Skip(HLongtail_FSIterator fs_iterator)
{
    while (IsSkippableFile(fs_iterator))
    {
        if (FALSE == FindNextFileA(fs_iterator->m_Handle, &fs_iterator->m_FindData))
        {
            return 0;
        }
    }
    return 1;
}

int Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path)
{
    char scan_pattern[MAX_PATH];
    strcpy(scan_pattern, path);
    strncat(scan_pattern, "\\*.*", MAX_PATH - strlen(scan_pattern));
    fs_iterator->m_Handle = FindFirstFileA(scan_pattern, &fs_iterator->m_FindData);
    if (fs_iterator->m_Handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }
    return Skip(fs_iterator);
}

int Longtail_FindNext(HLongtail_FSIterator fs_iterator)
{
    if (FALSE == FindNextFileA(fs_iterator->m_Handle, &fs_iterator->m_FindData))
    {
        return 0;
    }
    return Skip(fs_iterator);
}

void Longtail_CloseFind(HLongtail_FSIterator fs_iterator)
{
    FindClose(fs_iterator->m_Handle);
    fs_iterator->m_Handle = INVALID_HANDLE_VALUE;
}

const char* Longtail_GetFileName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        return 0;
    }
    return fs_iterator->m_FindData.cFileName;
}

const char* Longtail_GetDirectoryName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        return fs_iterator->m_FindData.cFileName;
    }
    return 0;
}

uint64_t Longtail_GetEntrySize(HLongtail_FSIterator fs_iterator)
{
    DWORD high = fs_iterator->m_FindData.nFileSizeHigh;
    DWORD low = fs_iterator->m_FindData.nFileSizeLow;
    return (((uint64_t)high) << 32) + (uint64_t)low;
}

HLongtail_OpenReadFile Longtail_OpenReadFile(const char* path)
{
    HANDLE handle = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 0, OPEN_EXISTING, 0, 0);
    if (handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }
    return (HLongtail_OpenReadFile)handle;
}

HLongtail_OpenWriteFile Longtail_OpenWriteFile(const char* path, uint64_t initial_size)
{
    HANDLE handle = CreateFileA(path, GENERIC_READ | GENERIC_WRITE, 0, 0, initial_size == 0 ? CREATE_ALWAYS : OPEN_ALWAYS, 0, 0);
    if (handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }

    if (initial_size > 0)
    {
        LONG low = (LONG)(initial_size & 0xffffffff);
        LONG high = (LONG)(initial_size >> 32);
        if (INVALID_SET_FILE_POINTER == SetFilePointer(handle, low, &high, FILE_BEGIN))
        {
            CloseHandle(handle);
            return 0;
        }
        if(FALSE == SetEndOfFile(handle))
        {
            CloseHandle(handle);
            return 0;
        }
    }

    return (HLongtail_OpenWriteFile)handle;
}

int Longtail_SetFileSize(HLongtail_OpenWriteFile handle, uint64_t length)
{
    HANDLE h = (HANDLE)(handle);
    LONG low = (LONG)(length & 0xffffffff);
    LONG high = (LONG)(length >> 32);
    if (INVALID_SET_FILE_POINTER == SetFilePointer(h, low, &high, FILE_BEGIN))
    {
        return 0;
    }
    return TRUE == SetEndOfFile(h);
}

int Longtail_Read(HLongtail_OpenReadFile handle, uint64_t offset, uint64_t length, void* output)
{
    HANDLE h = (HANDLE)(handle);
    LONG low = (LONG)(offset & 0xffffffff);
    LONG high = (LONG)(offset >> 32);
    if (INVALID_SET_FILE_POINTER == SetFilePointer(h, low, &high, FILE_BEGIN))
    {
        return 0;
    }
    return TRUE == ReadFile(h, output, (LONG)length, 0, 0);
}

int Longtail_Write(HLongtail_OpenWriteFile handle, uint64_t offset, uint64_t length, const void* input)
{
    HANDLE h = (HANDLE)(handle);
    LONG low = (LONG)(offset & 0xffffffff);
    LONG high = (LONG)(offset >> 32);
    if (INVALID_SET_FILE_POINTER == SetFilePointer(h, low, &high, FILE_BEGIN))
    {
        return 0;
    }
    return TRUE == WriteFile(h, input, (LONG)length, 0, 0);
}

uint64_t Longtail_GetFileSize(HLongtail_OpenReadFile handle)
{
    HANDLE h = (HANDLE)(handle);
    DWORD high = 0;
    DWORD low = GetFileSize(h, &high);
    return (((uint64_t)high) << 32) + (uint64_t)low;
}

void Longtail_CloseReadFile(HLongtail_OpenReadFile handle)
{
    HANDLE h = (HANDLE)(handle);
    CloseHandle(h);
}

void Longtail_CloseWriteFile(HLongtail_OpenWriteFile handle)
{
    HANDLE h = (HANDLE)(handle);
    CloseHandle(h);
}

const char* Longtail_ConcatPath(const char* folder, const char* file)
{
    size_t folder_length = strlen(folder);
    if (folder_length > 0 && folder[folder_length - 1] == '\\')
    {
        --folder_length;
    }
    size_t path_len = folder_length + 1 + strlen(file) + 1;
    char* path = (char*)Longtail_Alloc(path_len);

    memmove(path, folder, folder_length);
    path[folder_length] = '\\';
    strcpy(&path[folder_length + 1], file);
    return path;
}

#endif

#if defined(__APPLE__) || defined(__linux__)

#include <sys/types.h>
#include <dirent.h>
#include <semaphore.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>

size_t Longtail_GetCPUCount()
{
   return (size_t)sysconf(_SC_NPROCESSORS_ONLN);
}

void Longtail_Sleep(uint64_t timeout_us)
{
    usleep((useconds_t)timeout_us);
}

int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount)
{
    return __sync_fetch_and_add(value, amount) + amount;
}

struct Longtail_Thread
{
    pthread_t           m_Handle;
    Longtail_ThreadFunc m_ThreadFunc;
    void*               m_ContextData;
    pthread_mutex_t     m_ExitLock;
    pthread_cond_t      m_ExitConditionalVariable;
    int                 m_Exited;
};

static void* ThreadStartFunction(void* data)
{
    struct Longtail_Thread* thread = (struct Longtail_Thread*)data;
    (void)thread->m_ThreadFunc(thread->m_ContextData);
    pthread_mutex_lock(&thread->m_ExitLock);
    thread->m_Exited = 1;
    pthread_cond_broadcast(&thread->m_ExitConditionalVariable);
    pthread_mutex_unlock(&thread->m_ExitLock);
    return 0;
}

size_t Longtail_GetThreadSize()
{
    return sizeof(struct Longtail_Thread);
}

int Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data, HLongtail_Thread* out_thread)
{
    struct Longtail_Thread* thread      = (struct Longtail_Thread*)mem;
    thread->m_ThreadFunc                = thread_func;
    thread->m_ContextData               = context_data;
    thread->m_Exited                    = 0;
    thread->m_ExitLock                  = 0;
    thread->m_ExitConditionalVariable   = 0;

    int err = 0;

    pthread_attr_t attr;
    int attr_err = pthread_attr_init(&attr);
    if (attr_err != 0) {
        err = attr_err;
        goto error;
    }

    int exit_lock_err = pthread_mutex_init(&thread->m_ExitLock, 0);
    if (exit_lock_err != 0) {
        err = exit_lock_err;
        goto error;
    }

    int exit_cont_err = pthread_cond_init(&thread->m_ExitConditionalVariable, 0);
    if (exit_cont_err != 0) {
        err = exit_cont_err;
        goto error;
    }

    if (stack_size != 0)
    {
        err = pthread_attr_setstacksize(&attr, stack_size);
        if (err != 0) {
            goto error;
        }
    }

    int thread_err = pthread_create(&thread->m_Handle, &attr, ThreadStartFunction, (void*)thread);
    if (thread_err != 0)
    {
        err = thread_err;
        goto error;
    }
    pthread_attr_destroy(&attr);
    return 0;

error:
    if (exit_cont_err == 0) {
        pthread_cond_destroy(&thread->m_ExitConditionalVariable);
    }
    if (exit_lock_err == 0) {
        pthread_mutex_destroy(&thread->m_ExitLock);
    }
    if (attr_err == 0) {
        pthread_attr_destroy(&attr);
    }
    return err;
}

static int GetTimeSpec(struct timespec* ts, uint64_t delay_us)
{
    if (clock_gettime(CLOCK_REALTIME, ts) == -1)
    {
        return errno;
    }
    uint64_t end_ns = (uint64_t)(ts->tv_nsec) + (delay_us * 1000u);
    uint64_t wait_s = end_ns / 1000000000u;
    ts->tv_sec += wait_s;
    ts->tv_nsec = (long)(end_ns - wait_s * 1000000000u);
    return 0;
}

int Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us)
{
    if (thread->m_Handle == 0)
    {
        return 0;
    }
    if (timeout_us == LONGTAIL_TIMEOUT_INFINITE)
    {
        int result = pthread_join(thread->m_Handle, 0);
        if (result == 0)
        {
            thread->m_Handle = 0;
        }
        return result;
    }
    struct timespec ts;
    int err = GetTimeSpec(&ts, timeout_us))
    if (err != 0){
        return err;
    }
    err = pthread_mutex_lock(&thread->m_ExitLock);
    if (err != 0){
        return err;
    }
    while (!thread->m_Exited)
    {
        int err = pthread_cond_timedwait(&thread->m_ExitConditionalVariable, &thread->m_ExitLock, &ts);
        if (err == ETIMEDOUT)
        {
            pthread_mutex_unlock(&thread->m_ExitLock);
            return err;
        }
    }
    pthread_mutex_unlock(&thread->m_ExitLock);
    err = pthread_join(thread->m_Handle, 0);
    if (result == 0)
    {
        thread->m_Handle = 0;
    }
    return err;
}

void Longtail_DeleteThread(HLongtail_Thread thread)
{
    pthread_cond_destroy(&thread->m_ExitConditionalVariable);
    pthread_mutex_destroy(&thread->m_ExitLock);
    thread->m_Handle = 0;
}

#ifdef __APPLE__

struct Longtail_Sema
{
    semaphore_t m_Semaphore;
};

size_t Longtail_GetSemaSize()
{
    return sizeof(struct Longtail_Sema);
}

int Longtail_CreateSema(void* mem, int initial_count, HLongtail_Sema* out_sema)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;

    mach_port_t self = mach_task_self();
    kern_return_t ret = semaphore_create(self, &semaphore->m_Semaphore, SYNC_POLICY_FIFO, initial_count);

    if (ret != KERN_SUCCESS)
    {
        return ret;
    }

    *out_sema = semaphore;
    return 0;
}

bool Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    while (count--)
    {
        if (KERN_SUCCESS != semaphore_signal(semaphore->m_Semaphore))
        {
            return false;
        }
    }
    return true;
}

bool Longtail_WaitSema(HLongtail_Sema semaphore)
{
    if (KERN_SUCCESS != semaphore_wait(semaphore->m_Semaphore))
    {
        return false;
    }
    return true;
}

void Longtail_DeleteSema(HLongtail_Sema semaphore)
{
    mach_port_t self = mach_task_self();
    semaphore_destroy(self, semaphore->m_Semaphore);
}

struct Longtail_SpinLock
{
    os_unfair_lock m_Lock;
};

size_t Longtail_GetSpinLockSize()
{
    return sizeof(struct Longtail_SpinLock);
}

int Longtail_CreateSpinLock(void* mem, HLongtail_SpinLock* out_spin_lock)
{
    HLongtail_SpinLock spin_lock                = (HLongtail_SpinLock)mem;
    spin_lock->m_Lock._os_unfair_lock_opaque    = 0;
    *out_spin_lock = spin_lock;
    return 0;
}

void Longtail_DeleteSpinLock(HLongtail_SpinLock spin_lock)
{
}

void Longtail_LockSpinLock(HLongtail_SpinLock spin_lock)
{
    os_unfair_lock_lock(&spin_lock->m_Lock);
}

void Longtail_UnlockSpinLock(HLongtail_SpinLock spin_lock)
{
    os_unfair_lock_unlock(&spin_lock->m_Lock);
}

#else

struct Longtail_Sema
{
    sem_t           m_Semaphore;
};

size_t Longtail_GetSemaSize()
{
    return sizeof(struct Longtail_Sema);
}

int Longtail_CreateSema(void* mem, int initial_count, HLongtail_Sema* out_sema)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;
    int err = sem_init(&semaphore->m_Semaphore, 0, (unsigned int)initial_count);
    if (err != 0){
        return 0;
    }
    *out_sema = semaphore;
    return 0;
}

int Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    while (count--)
    {
        if (0 != sem_post(&semaphore->m_Semaphore))
        {
            return 0;
        }
    }
    return 1;
}

int Longtail_WaitSema(HLongtail_Sema semaphore)
{
    if (0 != sem_wait(&semaphore->m_Semaphore))
    {
        return 0;
    }
    return 1;
}

void Longtail_DeleteSema(HLongtail_Sema semaphore)
{
    sem_destroy(&semaphore->m_Semaphore);
}

struct Longtail_SpinLock
{
    pthread_spinlock_t m_Lock;
};

size_t Longtail_GetSpinLockSize()
{
    return sizeof(struct Longtail_SpinLock);
}

int Longtail_CreateSpinLock(void* mem, HLongtail_SpinLock* out_spin_lock)
{
    HLongtail_SpinLock spin_lock = (HLongtail_SpinLock)mem;
    int err = pthread_spin_init(&spin_lock->m_Lock, 0);
    if (err != 0) {
        return 0;
    }
    *out_spin_lock = spin_lock;
    return 0;
}

void Longtail_DeleteSpinLock(HLongtail_SpinLock spin_lock)
{
}

void Longtail_LockSpinLock(HLongtail_SpinLock spin_lock)
{
    pthread_spin_lock(&spin_lock->m_Lock);
}

void Longtail_UnlockSpinLock(HLongtail_SpinLock spin_lock)
{
    pthread_spin_unlock(&spin_lock->m_Lock);
}

#endif






void Longtail_NormalizePath(char* )
{
    // Nothing to do
}

void Longtail_DenormalizePath(char* )
{
    // Nothing to do
}

int Longtail_CreateDirectory(const char* path)
{
    int err = mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (err == 0)
    {
        return 1;
    }
    int e = errno;
    if (e == EEXIST)
    {
        return 1;
    }
    printf("Can't create directory `%s`: %d\n", path, e);
    return 0;
}

int Longtail_MoveFile(const char* source, const char* target)
{
    int err = rename(source, target);
    if (err == 0)
    {
        return 1;
    }
    int e = errno;
    printf("Can't move `%s` to `%s`: %d\n", source, target, e);
    return 0;
}

int Longtail_IsDir(const char* path)
{
    struct stat path_stat;
    int err = stat(path, &path_stat);
    if (0 == err)
    {
        return S_ISDIR(path_stat.st_mode);
    }
    int e = errno;
    if (ENOENT == e)
    {
        return 0;
    }
    printf("Can't determine type of `%s`: %d\n", path, e);
    return 0;
}

int Longtail_IsFile(const char* path)
{
    struct stat path_stat;
    int err = stat(path, &path_stat);
    if (0 == err)
    {
        return S_ISREG(path_stat.st_mode);
    }
    int e = errno;
    if (ENOENT == e)
    {
        return 0;
    }
    printf("Can't determine type of `%s`: %d\n", path, e);
    return 0;
}

int Longtail_RemoveDir(const char* path)
{
    int ok = rmdir(path) == 0;
    return ok;
}

int Longtail_RemoveFile(const char* path)
{
    int ok = unlink(path) == 0;
    return ok;
}

struct Longtail_FSIterator_private
{
    char* m_DirPath;
    DIR* m_DirStream;
    struct dirent * m_DirEntry;
};

size_t Longtail_GetFSIteratorSize()
{
    return sizeof(struct Longtail_FSIterator_private);
}

static int IsSkippableFile(HLongtail_FSIterator fs_iterator)
{
    if ((fs_iterator->m_DirEntry->d_type != DT_DIR) &&
        (fs_iterator->m_DirEntry->d_type != DT_REG))
    {
        return 0;
    }
    const char* p = fs_iterator->m_DirEntry->d_name;
    if ((*p++) != '.')
    {
        return 0;
    }
    if ((*p) == '\0')
    {
        return 1;
    }
    if ((*p++) != '.')
    {
        return 0;
    }
    if ((*p) == '\0')
    {
        return 1;
    }
    return 0;
}

static int Skip(HLongtail_FSIterator fs_iterator)
{
    while (IsSkippableFile(fs_iterator))
    {
        fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
        if (fs_iterator->m_DirEntry == 0)
        {
            return 0;
        }
    }
    return 1;
}

int Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path)
{
    fs_iterator->m_DirPath = Longtail_Strdup(path);
    fs_iterator->m_DirStream = opendir(path);
    if (0 == fs_iterator->m_DirStream)
    {
        Longtail_Free(fs_iterator->m_DirPath);
        return 0;
    }

    fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
    if (fs_iterator->m_DirEntry == 0)
    {
        closedir(fs_iterator->m_DirStream);
        Longtail_Free(fs_iterator->m_DirPath);
        return 0;
    }
    int has_files = Skip(fs_iterator);
    if (has_files)
    {
        return 1;
    }
    closedir(fs_iterator->m_DirStream);
    Longtail_Free(fs_iterator->m_DirPath);
    return 0;
}

int Longtail_FindNext(HLongtail_FSIterator fs_iterator)
{
    fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
    if (fs_iterator->m_DirEntry == 0)
    {
        return 0;
    }
    return Skip(fs_iterator);
}

void Longtail_CloseFind(HLongtail_FSIterator fs_iterator)
{
    closedir(fs_iterator->m_DirStream);
    fs_iterator->m_DirStream = 0;
    Longtail_Free(fs_iterator->m_DirPath);
    fs_iterator->m_DirPath = 0;
}

const char* Longtail_GetFileName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_DirEntry->d_type != DT_REG)
    {
        return 0;
    }
    return fs_iterator->m_DirEntry->d_name;
}

const char* Longtail_GetDirectoryName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_DirEntry->d_type != DT_DIR)
    {
        return 0;
    }
    return fs_iterator->m_DirEntry->d_name;
}

uint64_t Longtail_GetEntrySize(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_DirEntry->d_type != DT_REG)
    {
        return 0;
    }
    size_t dir_len = strlen(fs_iterator->m_DirPath);
    size_t file_len = strlen(fs_iterator->m_DirEntry->d_name);
    char* path = (char*)Longtail_Alloc(dir_len + 1 + file_len + 1);
    memcpy(&path[0], fs_iterator->m_DirPath, dir_len);
    path[dir_len] = '/';
    memcpy(&path[dir_len + 1], fs_iterator->m_DirEntry->d_name, file_len);
    path[dir_len + 1 + file_len] = '\0';
    struct stat stat_buf;
    int ok = stat(path, &stat_buf);
    uint64_t size = ok ? 0 : (uint64_t)stat_buf.st_size;
    Longtail_Free(path);
    return size;
}

HLongtail_OpenReadFile Longtail_OpenReadFile(const char* path)
{
    FILE* f = fopen(path, "rb");
    return (HLongtail_OpenReadFile)f;
}

HLongtail_OpenWriteFile Longtail_OpenWriteFile(const char* path, uint64_t initial_size)
{
    FILE* f = fopen(path, initial_size == 0 ? "wb" : "rb+");
    if (!f)
    {
        int e = errno;
        printf("Can't open file `%s` with attributes `%s`: %d\n", path, initial_size == 0 ? "wb" : "rb+", e);
        return 0;
    }
    if  (initial_size > 0)
    {
        int err = ftruncate64(fileno(f), initial_size);
        if (err != 0)
        {
            int e = errno;
            printf("Can't truncate file `%s` to `%ld`: %d\n", path, (off64_t)initial_size, e);
            fclose(f);
            return 0;
        }
/*        err = fsync(fileno(f));
        if (err != 0)
        {
            int e = errno;
            printf("Can't fsync file `%s`: %d\n", path, e);
            fclose(f);
            return 0;
        }*/
    }
    return (HLongtail_OpenWriteFile)f;
}

int Longtail_SetFileSize(HLongtail_OpenWriteFile handle, uint64_t length)
{
    FILE* f = (FILE*)handle;
    fflush(f);
    int err = ftruncate(fileno(f), (off_t)length);
    if (err == 0)
    {
        fflush(f);
        uint64_t verify_size = Longtail_GetFileSize((HLongtail_OpenReadFile)handle);
        if (verify_size != length)
        {
            printf("Truncate did not set the correct size of `%ld`\n", (off_t)length);
            return 0;
        }
        return 1;
    }
    int e = errno;
    printf("Can't truncate to `%ld`: %d\n", (off_t)length, e);
    return 0;
}

int Longtail_Read(HLongtail_OpenReadFile handle, uint64_t offset, uint64_t length, void* output)
{
    FILE* f = (FILE*)handle;
    if (-1 == fseek(f, (long int)offset, SEEK_SET))
    {
        return 0;
    }
    size_t read = fread(output, (size_t)length, 1, f);
    return read == 1u;
}

int Longtail_Write(HLongtail_OpenWriteFile handle, uint64_t offset, uint64_t length, const void* input)
{
    FILE* f = (FILE*)handle;
    if (-1 == fseek(f, (long int )offset, SEEK_SET))
    {
        return 0;
    }
    size_t written = fwrite(input, (size_t)length, 1, f);
    return written == 1u;
}

uint64_t Longtail_GetFileSize(HLongtail_OpenReadFile handle)
{
    FILE* f = (FILE*)handle;
    if (-1 == fseek(f, 0, SEEK_END))
    {
        return 0;
    }
    return (uint64_t)ftell(f);
}

void Longtail_CloseReadFile(HLongtail_OpenReadFile handle)
{
    FILE* f = (FILE*)handle;
    fclose(f);
}

void Longtail_CloseWriteFile(HLongtail_OpenWriteFile handle)
{
    FILE* f = (FILE*)handle;
    fflush(f);
    fclose(f);
}

const char* Longtail_ConcatPath(const char* folder, const char* file)
{
    size_t path_len = strlen(folder) + 1 + strlen(file) + 1;
    char* path = (char*)Longtail_Alloc(path_len);
    strcpy(path, folder);
    strcat(path, "/");
    strcat(path, file);
    return path;
}



#endif
