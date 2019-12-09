#include "longtail_platform.h"
#include <stdint.h>

#if defined(_WIN32)

#include <Windows.h>

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

HLongtail_Thread Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data)
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
        return 0;
    }
    return thread;
}

int Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us)
{
    if (thread->m_Handle == 0)
    {
        return 1;
    }
    DWORD wait_ms = (timeout_us == LONGTAIL_TIMEOUT_INFINITE) ? INFINITE : (DWORD)(timeout_us / 1000);
    DWORD result  = WaitForSingleObject(thread->m_Handle, wait_ms);
    return result == WAIT_OBJECT_0;
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

HLongtail_Sema Longtail_CreateSema(void* mem, int initial_count)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;
    semaphore->m_Handle = CreateSemaphore(NULL, initial_count, 0x7fffffff, NULL);
    if (semaphore->m_Handle == INVALID_HANDLE_VALUE)
    {
        return 0;
    }
    return semaphore;
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
    char* path = (char*)malloc(path_len);

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
#include <errno.h>
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

HLongtail_Thread Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data)
{
    struct Longtail_Thread* thread = (struct Longtail_Thread*)mem;
    thread->m_ThreadFunc  = thread_func;
    thread->m_ContextData = context_data;
    thread->m_Exited      = 0;
    pthread_mutex_init(&thread->m_ExitLock, 0);
    pthread_cond_init(&thread->m_ExitConditionalVariable, 0);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    if (stack_size != 0)
    {
        pthread_attr_setstacksize(&attr, stack_size);
    }
    int result = pthread_create(&thread->m_Handle, &attr, ThreadStartFunction, (void*)thread);
    pthread_attr_destroy(&attr);
    if (result != 0)
    {
        return 0;
    }
    return thread;
}

static int GetTimeSpec(struct timespec* ts, uint64_t delay_us)
{
    if (clock_gettime(CLOCK_REALTIME, ts) == -1)
    {
        return 0;
    }
    uint64_t end_ns = (uint64_t)(ts->tv_nsec) + (delay_us * 1000u);
    uint64_t wait_s = end_ns / 1000000000u;
    ts->tv_sec += wait_s;
    ts->tv_nsec = (long)(end_ns - wait_s * 1000000000u);
    return 1;
}

int Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us)
{
    if (thread->m_Handle == 0)
    {
        return 1;
    }
    if (timeout_us == LONGTAIL_TIMEOUT_INFINITE)
    {
        int result = pthread_join(thread->m_Handle, 0);
        if (result == 0)
        {
            thread->m_Handle = 0;
        }
        return result == 0;
    }
    struct timespec ts;
    if (!GetTimeSpec(&ts, timeout_us))
    {
        return 0;
    }
    pthread_mutex_lock(&thread->m_ExitLock);
    while (!thread->m_Exited)
    {
        if (0 != pthread_cond_timedwait(&thread->m_ExitConditionalVariable, &thread->m_ExitLock, &ts))
        {
            break;
        }
    }
    int exited = thread->m_Exited;
    pthread_mutex_unlock(&thread->m_ExitLock);
    if (!exited)
    {
        return 0;
    }
    int result = pthread_join(thread->m_Handle, 0);
    if (result == 0)
    {
        thread->m_Handle = 0;
    }
    return result == 0;
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

HLongtail_Sema Longtail_CreateSema(void* mem, int initial_count)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;

    mach_port_t self = mach_task_self();
    kern_return_t ret = semaphore_create(self, &semaphore->m_Semaphore, SYNC_POLICY_FIFO, initial_count);

    if (ret != KERN_SUCCESS)
    {
        return 0;
    }

    return semaphore;
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

#else

struct Longtail_Sema
{
    sem_t           m_Semaphore;
};

size_t Longtail_GetSemaSize()
{
    return sizeof(struct Longtail_Sema);
}

HLongtail_Sema Longtail_CreateSema(void* mem, int initial_count)
{
    HLongtail_Sema semaphore = (HLongtail_Sema)mem;
    if (0 != sem_init(&semaphore->m_Semaphore, 0, (unsigned int)initial_count))
    {
        return 0;
    }
    return semaphore;
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
    fs_iterator->m_DirPath = strdup(path);
    fs_iterator->m_DirStream = opendir(path);
    if (0 == fs_iterator->m_DirStream)
    {
        free(fs_iterator->m_DirPath);
        return 0;
    }

    fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
    if (fs_iterator->m_DirEntry == 0)
    {
        closedir(fs_iterator->m_DirStream);
        free(fs_iterator->m_DirPath);
        return 0;
    }
    int has_files = Skip(fs_iterator);
    if (has_files)
    {
        return 1;
    }
    closedir(fs_iterator->m_DirStream);
    free(fs_iterator->m_DirPath);
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
    free(fs_iterator->m_DirPath);
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
    char* path = (char*)malloc(dir_len + 1 + file_len + 1);
    memcpy(&path[0], fs_iterator->m_DirPath, dir_len);
    path[dir_len] = '/';
    memcpy(&path[dir_len + 1], fs_iterator->m_DirEntry->d_name, file_len);
    path[dir_len + 1 + file_len] = '\0';
    struct stat stat_buf;
    int ok = stat(path, &stat_buf);
    uint64_t size = ok ? 0 : (uint64_t)stat_buf.st_size;
    free(path);
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
    char* path = (char*)malloc(path_len);
    strcpy(path, folder);
    strcat(path, "/");
    strcat(path, file);
    return path;
}



#endif
