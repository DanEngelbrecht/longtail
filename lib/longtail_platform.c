#include "longtail_platform.h"
#include "../src/longtail.h"
#include <stdint.h>
#include <errno.h>

static const uint32_t HostnamePrime = 0x01000193;
static const uint32_t HostnameSeed  = 0x811C9DC5;
static const uint32_t MaxChunkSize  = 4194304;

static uint32_t HostnameFNV1A(const void* data, uint32_t numBytes)
{
    uint32_t hash = HostnameSeed;
    const unsigned char* ptr = (const unsigned char*)data;
    while (numBytes--)
    {
        hash = ((*ptr++) ^ hash) * HostnamePrime;
    }
    return hash;
}

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
        case ERROR_NO_MORE_FILES:
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
        case ERROR_BAD_NETPATH:
        case ERROR_BAD_NET_NAME:
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
        case WAIT_TIMEOUT:
        return ETIME;
        case ERROR_DIR_NOT_EMPTY:
        return ENOTEMPTY;
        default:
        return EINVAL;
    }
}

static int IsUNCPath(const wchar_t* path)
{
    return path != 0 && (path[0] == '\\' && path[1] == '\\' && path[2] == '?' && path[3] == '\\');
}

static int IsNetworkPath(const wchar_t* path)
{
    return path != 0 && (path[0] == '\\' && path[1] == '\\' && path[2] != '?');
}

static const wchar_t* MakeLongPath(const wchar_t* path)
{
    if (path[0] && (path[1] != ':'))
    {
        // Don't add long path prefix if we don't specify a drive
        return path;
    }
    if (IsUNCPath(path))
    {
        // Don't add long path prefix if we aleady have an UNC path
        return path;
    }
    if (IsNetworkPath(path))
    {
        // Don't add long path prefix if we aleady have a network path
        return path;
    }
    size_t path_len = wcslen(path);
    if (path_len < MAX_PATH)
    {
        // Don't add long path prefix if the path isn't that long
        return path;
    }
    static const wchar_t* LongPathPrefix = L"\\\\?\\";
    size_t long_path_len = wcslen(path) + 4 + 1;
    wchar_t* long_path = (wchar_t*)Longtail_Alloc("MakeLongPath", long_path_len * sizeof(wchar_t));
    wcsncpy(long_path, LongPathPrefix, 4);
    wcscpy(&long_path[4], path);
    return long_path;
}

uint32_t Longtail_GetCPUCount()
{
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return (uint32_t)sysinfo.dwNumberOfProcessors;
}

void Longtail_Sleep(uint64_t timeout_us)
{
    DWORD wait_ms = timeout_us == LONGTAIL_TIMEOUT_INFINITE ? INFINITE : (DWORD)(timeout_us / 1000);
    Sleep(wait_ms);
}

int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount)
{
    return (int32_t)InterlockedAdd((LONG volatile*)value, (LONG)amount);
}

#if !defined(__GNUC__)
    #define _InterlockedAdd64 _InlineInterlockedAdd64
#endif

int64_t Longtail_AtomicAdd64(TLongtail_Atomic64* value, int64_t amount)
{
    return (int64_t)_InterlockedAdd64((LONG64 volatile*)value, (LONG64)amount);
}

int Longtail_CompareAndSwap(TLongtail_Atomic32* value, int32_t expected, int32_t wanted)
{
    return _InterlockedCompareExchange((volatile LONG*)value, wanted, expected) == expected;
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

int Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data, int priority, HLongtail_Thread* out_thread)
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
    if (thread->m_Handle == NULL)
    {
        return Win32ErrorToErrno(GetLastError());
    }
    switch (priority)
    {
        case 0:
            break;
        case 1:
            SetThreadPriority(thread->m_Handle, THREAD_PRIORITY_ABOVE_NORMAL);
            break;
        case 2:
            SetThreadPriority(thread->m_Handle, THREAD_PRIORITY_HIGHEST);
            break;
        case -1:
            SetThreadPriority(thread->m_Handle, THREAD_PRIORITY_BELOW_NORMAL);
            break;
        case -2:
            SetThreadPriority(thread->m_Handle, THREAD_PRIORITY_LOWEST);
            break;
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
        case WAIT_ABANDONED:
            return EINVAL;
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

uint64_t Longtail_GetCurrentThreadId()
{
    return (uint64_t)GetCurrentThreadId();
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
        return Win32ErrorToErrno(GetLastError());
    }
    *out_sema = semaphore;
    return 0;
}

int Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    if (ReleaseSemaphore(
                    semaphore->m_Handle,
                    count,
                    NULL))
    {
        return 0;
    }
    return EINVAL;
}

int Longtail_WaitSema(HLongtail_Sema semaphore, uint64_t timeout_us)
{
    DWORD timeout_ms = timeout_us == LONGTAIL_TIMEOUT_INFINITE ? INFINITE : (DWORD)(timeout_us / 1000);
    DWORD res = WaitForSingleObject(semaphore->m_Handle, timeout_ms);
    switch (res)
    {
        case WAIT_OBJECT_0:
            return 0;
        case WAIT_ABANDONED:
            return EINVAL;
        case WAIT_TIMEOUT:
            return ETIME;
        case WAIT_FAILED:
            return Win32ErrorToErrno(GetLastError());
        default:
            return EINVAL;
    }
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

static wchar_t* MakeWCharString(const char* s, wchar_t* buffer, size_t buffer_size)
{
    struct Longtail_LogContextFmt_Private* ctx = 0;
    int l = MultiByteToWideChar(CP_UTF8, 0, s, -1, NULL, 0);
    if (l <= 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "MultiByteToWideChar for path `%s`: failed with %d", s, Win32ErrorToErrno(GetLastError()))
        return 0;
    }
    wchar_t* r = buffer;
    if ((size_t)l > buffer_size)
    {
        r = (wchar_t*)Longtail_Alloc("MakeWCharString", l * sizeof(wchar_t));
    }
    MultiByteToWideChar(CP_UTF8, 0, s, -1, r, l);
    return r;
}

static char* MakeUTF8String(const wchar_t* s, char* buffer, size_t buffer_size)
{
    struct Longtail_LogContextFmt_Private* ctx = 0;
    int l = WideCharToMultiByte(CP_UTF8, 0, s, -1, 0, 0, NULL ,NULL);
    if (l <= 0)
    {
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "MakeUTF8String for path `%ls`: failed with %d", s, Win32ErrorToErrno(GetLastError()))
        return 0;
    }
    char* r = buffer;
    if ((size_t)l > buffer_size)
    {
        r = (char*)Longtail_Alloc("MakeUTF8String", l * sizeof(char));
    }
    WideCharToMultiByte(CP_UTF8, 0, s, -1, r, l, NULL ,NULL);
    return r;
}

static wchar_t* MakePlatformPath(const char* path, wchar_t* buffer, size_t buffer_size)
{
    wchar_t* r = MakeWCharString(path, buffer, buffer_size);
    wchar_t* p = r;
    while (*p)
    {
        *p = ((*p) == '/') ? '\\' : (*p);
        ++p;
    }
    return r;
}

static wchar_t* MakeLongPlatformPath(const char* path, wchar_t* buffer, size_t buffer_size)
{
    wchar_t* platform_path = MakePlatformPath(path, buffer, buffer_size);
    const wchar_t* r = MakeLongPath(platform_path);
    if (r != platform_path)
    {
        if (platform_path != buffer)
        {
            Longtail_Free(platform_path);
        }
    }
    return (wchar_t*)r;
}

static wchar_t* ConcatPlatformPath(const wchar_t* a, const wchar_t* b)
{
    size_t la = wcslen(a);
    size_t lb = wcslen(b);

    int add_delimiter = (la > 0) && (a[la - 1] != '\\');

    size_t l = la + (add_delimiter ? 1 : 0) + lb + 1;
    wchar_t* r = (wchar_t*)Longtail_Alloc("ConcatPath", l * sizeof(wchar_t));
    wcsncpy(r, a, la);
    if (add_delimiter)
    {
        r[la] = '\\';
        ++la;
    }
    wcscpy(&r[la], b);
    return r;
}

static DWORD NativeCreateDirectory(wchar_t* long_path)
{
    while (1)
    {
        if (IsNetworkPath(long_path))
        {
            const wchar_t* net_device_end = wcschr(&long_path[2], '\\');
            if (net_device_end == 0)
            {
                // No delimiter for network device - not a valid path
                return ERROR_BAD_NETPATH;
            }
            if (net_device_end[1] == '\0')
            {
                // No root folder given for network path
                return ERROR_BAD_NETPATH;
            }
            const wchar_t* net_root_folder_end = wcschr(&net_device_end[1], '\\');
            if (net_root_folder_end == 0)
            {
                // Root folder given without trailing backslash
                return ERROR_ALREADY_EXISTS;
            }
            if (net_root_folder_end[1] == '\0')
            {
                // Root folder has trailing backslash and nothing more
                return ERROR_ALREADY_EXISTS;
            }
        }
        else
        {
            wchar_t* root_path_test_start = long_path;
            if (IsUNCPath(long_path))
            {
                root_path_test_start = &long_path[4];
            }
            // Check for regular drive path, with or without a trailing backslash
            if (root_path_test_start[1] == ':')
            {
                if (root_path_test_start[2] == '\0')
                {
                    return ERROR_ALREADY_EXISTS;
                }
                if (root_path_test_start[2] == '\\' && root_path_test_start[3] == '\0')
                {
                    return ERROR_ALREADY_EXISTS;
                }
            }
        }

        BOOL ok = CreateDirectoryW(long_path, NULL);
        if (ok)
        {
            return ERROR_SUCCESS;
        }
        DWORD last_error = GetLastError();
        if (last_error == ERROR_FILE_EXISTS || last_error == ERROR_ALREADY_EXISTS || last_error == ERROR_ACCESS_DENIED || last_error == ERROR_BAD_NET_NAME)
        {
            return last_error;
        }

        if (last_error == ERROR_PATH_NOT_FOUND)
        {
            size_t delim_pos = 0;
            size_t path_len = 0;
            while (long_path[path_len] != 0)
            {
                if (long_path[path_len] == '\\')
                {
                    // Don't accept double backslash as path delimiter
                    if (delim_pos + 1 != path_len)
                    {
                        delim_pos = path_len;
                    }
                }
                ++path_len;
            }
            if (long_path[delim_pos] != '\\' || delim_pos == 0)
            {
                return last_error;
            }
            long_path[delim_pos] = '\0';
            DWORD parent_error = NativeCreateDirectory(long_path);
            long_path[delim_pos] = '\\';
            switch (parent_error)
            {
                case ERROR_SUCCESS:
                case ERROR_FILE_EXISTS:
                case ERROR_ALREADY_EXISTS:
                    // Try again
                    break;
                default:
                    return last_error;
            }
        }
    }
}

int Longtail_CreateDirectory(const char* path)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    DWORD error = NativeCreateDirectory(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    return Win32ErrorToErrno(error);
}

int Longtail_MoveFile(const char* source, const char* target)
{
    wchar_t* long_source_path = MakeLongPlatformPath(source, 0, 0);
    wchar_t* long_target_path = MakeLongPlatformPath(target, 0, 0);
    BOOL ok = MoveFileW(long_source_path, long_target_path);
    Longtail_Free(long_source_path);
    Longtail_Free(long_target_path);
    if (ok)
    {
        return 0;
    }
    return Win32ErrorToErrno(GetLastError());
}

int Longtail_IsDir(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    DWORD attrs = GetFileAttributesW(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        int e = Win32ErrorToErrno(GetLastError());
        if (e == ENOENT)
        {
            return 0;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e)
        return 0;
    }
    return (attrs & FILE_ATTRIBUTE_DIRECTORY) ? 1 : 0;
}

int Longtail_IsFile(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    DWORD attrs = GetFileAttributesW(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        int e = Win32ErrorToErrno(GetLastError());
        if (e == ENOENT)
        {
            return 0;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e)
        return 0;
    }
    return (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0 ? 1 : 0;
}

int Longtail_RemoveDir(const char* path)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    BOOL ok = RemoveDirectoryW(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (ok)
    {
        return 0;
    }
    return Win32ErrorToErrno(GetLastError());
}

int Longtail_RemoveFile(const char* path)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    BOOL ok = DeleteFileW(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (ok)
    {
        return 0;
    }
    return Win32ErrorToErrno(GetLastError());
}

#define LONGTAIL_FSITERATOR_PRIVATE_ITEM_BUFFER_SIZE (1024 - sizeof(WIN32_FIND_DATAW) - sizeof(HANDLE) - sizeof(wchar_t*) - sizeof(char*))

struct Longtail_FSIterator_private
{
    WIN32_FIND_DATAW m_FindData;
    HANDLE m_Handle;
    wchar_t* m_Path;
    char* m_ItemPath;
    char m_ItemPathBuffer[LONGTAIL_FSITERATOR_PRIVATE_ITEM_BUFFER_SIZE];
};

size_t Longtail_GetFSIteratorSize()
{
    return sizeof(struct Longtail_FSIterator_private);
}

static int IsSkippableFile(HLongtail_FSIterator fs_iterator)
{
    const wchar_t* p = fs_iterator->m_FindData.cFileName;
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
        if (FALSE == FindNextFileW(fs_iterator->m_Handle, &fs_iterator->m_FindData))
        {
            return Win32ErrorToErrno(GetLastError());
        }
    }
    return 0;
}

int Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path)
{
    fs_iterator->m_Path = MakeLongPlatformPath(path, 0, 0);
    wchar_t* scan_pattern = ConcatPlatformPath(fs_iterator->m_Path, L"*.*");
    fs_iterator->m_ItemPath = 0;
    fs_iterator->m_Handle = FindFirstFileW(scan_pattern, &fs_iterator->m_FindData);
    Longtail_Free(scan_pattern);
    if (fs_iterator->m_Handle == INVALID_HANDLE_VALUE)
    {
        Longtail_Free(fs_iterator->m_Path);
        return Win32ErrorToErrno(GetLastError());
    }
    int err = Skip(fs_iterator);
    if (err != 0)
    {
        FindClose(fs_iterator->m_Handle);
        Longtail_Free(fs_iterator->m_Path);
    }
    return err;
}

int Longtail_FindNext(HLongtail_FSIterator fs_iterator)
{
    if (FALSE == FindNextFileW(fs_iterator->m_Handle, &fs_iterator->m_FindData))
    {
        return Win32ErrorToErrno(GetLastError());
    }
    return Skip(fs_iterator);
}

void Longtail_CloseFind(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_ItemPath != fs_iterator->m_ItemPathBuffer)
    {
        Longtail_Free(fs_iterator->m_ItemPath);
    }
    Longtail_Free(fs_iterator->m_Path);
    FindClose(fs_iterator->m_Handle);
    fs_iterator->m_Handle = INVALID_HANDLE_VALUE;
}

const char* Longtail_GetFileName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        return 0;
    }
    if (fs_iterator->m_ItemPath != fs_iterator->m_ItemPathBuffer)
    {
        Longtail_Free(fs_iterator->m_ItemPath);
    }
    fs_iterator->m_ItemPath = MakeUTF8String(fs_iterator->m_FindData.cFileName, fs_iterator->m_ItemPathBuffer, sizeof(fs_iterator->m_ItemPathBuffer));
    return fs_iterator->m_ItemPath;
}

const char* Longtail_GetDirectoryName(HLongtail_FSIterator fs_iterator)
{
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        if (fs_iterator->m_ItemPath != fs_iterator->m_ItemPathBuffer)
        {
            Longtail_Free(fs_iterator->m_ItemPath);
        }
        fs_iterator->m_ItemPath = MakeUTF8String(fs_iterator->m_FindData.cFileName, fs_iterator->m_ItemPathBuffer, sizeof(fs_iterator->m_ItemPathBuffer));
        return fs_iterator->m_ItemPath;
    }
    return 0;
}

int Longtail_GetEntryProperties(HLongtail_FSIterator fs_iterator, uint64_t* out_size, uint16_t* out_permissions, int* out_is_dir)
{
    DWORD high = fs_iterator->m_FindData.nFileSizeHigh;
    DWORD low = fs_iterator->m_FindData.nFileSizeLow;
    *out_size = (((uint64_t)high) << 32) + (uint64_t)low;
    uint16_t permissions = Longtail_StorageAPI_UserReadAccess | Longtail_StorageAPI_GroupReadAccess | Longtail_StorageAPI_OtherReadAccess;
    if (fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
    {
        permissions = permissions | Longtail_StorageAPI_UserExecuteAccess | Longtail_StorageAPI_GroupExecuteAccess | Longtail_StorageAPI_OtherExecuteAccess;
        *out_is_dir = 1;
    }
    else
    {
        *out_is_dir = 0;
    }
    if ((fs_iterator->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_READONLY) == 0)
    {
        permissions = permissions | Longtail_StorageAPI_UserWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_OtherWriteAccess;
    }
    *out_permissions = permissions;
    return 0;
}

DWORD NativeOpenReadFileWithRetry(wchar_t* long_path, HANDLE* out_handle)
{
    int retry_count = 10;
    while (1)
    {
        HANDLE handle = CreateFileW(long_path, GENERIC_READ, FILE_SHARE_READ, 0, OPEN_EXISTING, 0, 0);
        if (handle == INVALID_HANDLE_VALUE)
        {
            DWORD error = GetLastError();
            if (error == ERROR_PATH_NOT_FOUND)
            {
                // Retry - network drives on Windows can report failure if a different thread created
                // the parent directory just before our call to CreateFileW, even if we get an OK
                // that the parent exists...
                if (retry_count--)
                {
                    Sleep(1);
                    continue;
                }
            }
            return error;
        }
        *out_handle = handle;
        return 0;
    }
}

int Longtail_OpenReadFile(const char* path, HLongtail_OpenFile* out_read_file)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    HANDLE handle;
    DWORD error = NativeOpenReadFileWithRetry(long_path, &handle);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (error != ERROR_SUCCESS)
    {
        return Win32ErrorToErrno(error);
    }
    *out_read_file = (HLongtail_OpenFile)handle;
    return 0;
}

DWORD NativeOpenWriteFileWithRetry(wchar_t* long_path, DWORD desired_access, DWORD create_disposition, HANDLE* out_handle)
{
    int retry_count = 10;
    while (1)
    {
        HANDLE handle = CreateFileW(long_path, desired_access, 0, 0, create_disposition, 0, 0);
        if (handle == INVALID_HANDLE_VALUE)
        {
            DWORD error = GetLastError();
            if (error == ERROR_PATH_NOT_FOUND)
            {
                // Retry - network drives on Windows can report failure if a different thread created
                // the parent directory just before our call to CreateFileW, even if we get an OK
                // that the parent exists...
                if (retry_count--)
                {
                    Sleep(1);
                    continue;
                }
            }
            return error;
        }
        *out_handle = handle;
        return 0;
    }
}

int Longtail_OpenWriteFile(const char* path, uint64_t initial_size, HLongtail_OpenFile* out_write_file)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    HANDLE handle;
    DWORD error = NativeOpenWriteFileWithRetry(long_path, GENERIC_WRITE, initial_size == 0 ? CREATE_ALWAYS : OPEN_ALWAYS, &handle);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (error != ERROR_SUCCESS)
    {
        return Win32ErrorToErrno(error);
    }

    if (initial_size > 0)
    {
        LONG low = (LONG)(initial_size & 0xffffffff);
        LONG high = (LONG)(initial_size >> 32);
        if (INVALID_SET_FILE_POINTER == SetFilePointer(handle, low, &high, FILE_BEGIN))
        {
            int e = Win32ErrorToErrno(GetLastError());
            CloseHandle(handle);
            return e;
        }
        if(FALSE == SetEndOfFile(handle))
        {
            int e = Win32ErrorToErrno(GetLastError());
            CloseHandle(handle);
            return e;
        }
    }

    *out_write_file = (HLongtail_OpenFile)handle;
    return 0;
}

int Longtail_OpenAppendFile(const char* path, HLongtail_OpenFile* out_write_file)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    HANDLE handle;
    DWORD error = NativeOpenWriteFileWithRetry(long_path, GENERIC_WRITE, OPEN_ALWAYS, &handle);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (error != ERROR_SUCCESS)
    {
        return Win32ErrorToErrno(error);
    }

    *out_write_file = (HLongtail_OpenFile)handle;
    return 0;
}

int Longtail_SetFileSize(HLongtail_OpenFile handle, uint64_t length)
{
    HANDLE h = (HANDLE)(handle);
    LONG low = (LONG)(length & 0xffffffff);
    LONG high = (LONG)(length >> 32);
    if (INVALID_SET_FILE_POINTER == SetFilePointer(h, low, &high, FILE_BEGIN))
    {
        return Win32ErrorToErrno(GetLastError());
    }
    if (!SetEndOfFile(h))
    {
        return Win32ErrorToErrno(GetLastError());
    }
    return 0;
}

int Longtail_SetFilePermissions(const char* path, uint16_t permissions)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(permissions, "%u")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));

    DWORD attrs = GetFileAttributesW(long_path);
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        if (long_path != long_path_buffer)
        {
            Longtail_Free(long_path);
        }
        int e = Win32ErrorToErrno(GetLastError());
        if (e == ENOENT)
        {
            return 0;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e);
        return e;
    }
    int hasWritePermission = (attrs & FILE_ATTRIBUTE_READONLY) == 0;
    int wantsWritePermission = (permissions & (Longtail_StorageAPI_OtherWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_UserWriteAccess)) != 0;
    if (hasWritePermission != wantsWritePermission)
    {
        if (wantsWritePermission)
        {
            attrs = attrs & (~FILE_ATTRIBUTE_READONLY);
        }
        else
        {
            attrs = attrs | FILE_ATTRIBUTE_READONLY;
        }
        if (FALSE == SetFileAttributesW(long_path, attrs))
        {
            if (long_path != long_path_buffer)
            {
                Longtail_Free(long_path);
            }
            int e = Win32ErrorToErrno(GetLastError());
            if (e == ENOENT)
            {
                return 0;
            }
            LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't set read only attribyte of `%s`: %d\n", path, e);
            return e;
        }
    }
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    return 0;
}

int Longtail_GetFilePermissions(const char* path, uint16_t* out_permissions)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s"),
        LONGTAIL_LOGFIELD(out_permissions, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));
    DWORD attrs = GetFileAttributesW(long_path);
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    if (attrs == INVALID_FILE_ATTRIBUTES)
    {
        int e = Win32ErrorToErrno(GetLastError());
        if (e == ENOENT)
        {
            return e;
        }
        LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e);
        return e;
    }
    uint16_t permissions = Longtail_StorageAPI_UserReadAccess | Longtail_StorageAPI_GroupReadAccess | Longtail_StorageAPI_OtherReadAccess;
    if (attrs & FILE_ATTRIBUTE_DIRECTORY)
    {
        permissions = permissions | Longtail_StorageAPI_UserExecuteAccess | Longtail_StorageAPI_GroupExecuteAccess | Longtail_StorageAPI_OtherExecuteAccess;
    }
    if ((attrs & FILE_ATTRIBUTE_READONLY) == 0)
    {
        permissions = permissions | Longtail_StorageAPI_UserWriteAccess | Longtail_StorageAPI_GroupWriteAccess | Longtail_StorageAPI_OtherWriteAccess;
    }
    *out_permissions = permissions;
    return 0;
}

int Longtail_Read(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, void* output)
{
    HANDLE h = (HANDLE)(handle);

    OVERLAPPED ReadOp;
    memset(&ReadOp, 0, sizeof(ReadOp));

    ReadOp.Offset  = (DWORD)(offset & 0xffffffff);
    ReadOp.OffsetHigh = (DWORD)(offset >> 32);

    if (FALSE == ReadFile(h, output, (DWORD)length, 0, &ReadOp))
    {
        return Win32ErrorToErrno(GetLastError());
    }

    return 0;
}

int Longtail_Write(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, const void* input)
{
    HANDLE h = (HANDLE)(handle);

    OVERLAPPED WriteOp;
    memset(&WriteOp, 0, sizeof(WriteOp));

    WriteOp.Offset  = (DWORD)(offset & 0xffffffff);
    WriteOp.OffsetHigh = (DWORD)(offset >> 32);

    if (FALSE == WriteFile(h, input, (DWORD)length, 0, &WriteOp))
    {
        return Win32ErrorToErrno(GetLastError());
    }
    return 0;
}

int Longtail_GetFileSize(HLongtail_OpenFile handle, uint64_t* out_size)
{
    HANDLE h = (HANDLE)(handle);
    DWORD high = 0;
    DWORD low = GetFileSize(h, &high);
    if (low == INVALID_FILE_SIZE)
    {
        return Win32ErrorToErrno(GetLastError());
    }
    *out_size = (((uint64_t)high) << 32) + (uint64_t)low;
    return 0;
}

void Longtail_CloseFile(HLongtail_OpenFile handle)
{
    HANDLE h = (HANDLE)(handle);
    CloseHandle(h);
}

char* Longtail_ConcatPath(const char* folder, const char* file)
{
    size_t folder_length = strlen(folder);
    size_t file_length = strlen(file);

    char delimiter = '/';
    char last_folder_char = (folder_length > 0) ? folder[folder_length - 1] : 0;
    int add_delimiter = ((last_folder_char != 0) && (last_folder_char != '/') && (last_folder_char != '\\'));

    size_t path_len = folder_length + (add_delimiter ? 1 : 0) + file_length + 1;
    char* path = (char*)Longtail_Alloc("ConcatPath", path_len);
    for (size_t p = 0; p < folder_length; ++p)
    {
        if (folder[p] == '\\')
        {
            delimiter = '\\';
        }
        path[p] = folder[p];
    }
    if (add_delimiter)
    {
        path[folder_length] = delimiter;
        ++folder_length;
    }
    strcpy(&path[folder_length], file);
    return path;
}

static int is_path_delimiter(char c)
{
    return (c == '/') || (c == '\\');
}

char* Longtail_GetParentPath(const char* path)
{
    size_t delim_pos = 0;
    size_t path_len = 0;
    while (path[path_len] != 0)
    {
        if (is_path_delimiter(path[path_len]))
        {
            delim_pos = path_len;
        }
        ++path_len;
    }
    if ((!is_path_delimiter(path[delim_pos])) || delim_pos == 0)
    {
        return 0;
    }

    char* result = (char*)Longtail_Alloc("Longtail_GetParentPath", delim_pos + 1);
    if (!result)
    {
        LONGTAIL_LOG(0, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    result[delim_pos] = 0;
    while (delim_pos--)
    {
        result[delim_pos] = path[delim_pos];
    }
    return result;
}

char* Longtail_GetTempFolder()
{
    char tmp[MAX_PATH + 1];
    DWORD res = ExpandEnvironmentStringsA("%TEMP%", tmp, MAX_PATH);
    if (res == 0 || res > MAX_PATH)
    {
        return 0;
    }
    char expanded[MAX_PATH + 1];
    res = GetFullPathNameA(tmp, MAX_PATH, expanded, 0);
    if (res == 0 || res > MAX_PATH)
    {
        return 0;
    }
    return Longtail_Strdup(expanded);
}

uint64_t Longtail_GetProcessIdentity()
{
    char computername[1023+1];
    DWORD computernamesize = sizeof(computername);
    GetComputerNameA(computername, &computernamesize);
    uint64_t hostname_hash = HostnameFNV1A(computername, computernamesize);
    return ((uint64_t)GetCurrentProcessId() << 32) + hostname_hash;
}

struct Longtail_FileLock_private
{
    HANDLE handle;
};

size_t Longtail_GetFileLockSize()
{
    return sizeof(struct Longtail_FileLock_private);
}

int Longtail_LockFile(void* mem, const char* path, HLongtail_FileLock* out_file_lock)
{
    wchar_t long_path_buffer[512];
    wchar_t* long_path = MakeLongPlatformPath(path, long_path_buffer, sizeof(long_path_buffer));

    *out_file_lock = (HLongtail_FileLock)mem;
    (*out_file_lock)->handle = INVALID_HANDLE_VALUE;

    int try_count = 140;
    uint64_t retry_delay = 1000;

    HANDLE handle = CreateFileW(
        long_path,
        GENERIC_READ | GENERIC_WRITE,
        0,
        0,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
        0);
    while (handle == INVALID_HANDLE_VALUE)
    {
        if (--try_count == 0)
        {
            if (long_path != long_path_buffer)
            {
                Longtail_Free(long_path);
            }
            return EACCES;
        }
        Longtail_Sleep(retry_delay);
        handle = CreateFileW(
            long_path,
            GENERIC_READ | GENERIC_WRITE,
            0,
            0,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE,
            0);
        if (handle != INVALID_HANDLE_VALUE)
        {
            break;
        }
        DWORD error = GetLastError();
        switch (error)
        {
            case ERROR_SHARING_VIOLATION:
            case ERROR_ACCESS_DENIED:
            case ERROR_INVALID_ACCESS:
            case ERROR_WRITE_PROTECT:
            case ERROR_LOCK_VIOLATION:
            case ERROR_NETWORK_ACCESS_DENIED:
            case ERROR_EXCL_SEM_ALREADY_OWNED:
            case ERROR_NOT_OWNER:
            case ERROR_OPLOCK_NOT_GRANTED:
                break;
            default:
                if (long_path != long_path_buffer)
                {
                    Longtail_Free(long_path);
                }
                (long_path);
                return Win32ErrorToErrno(error);
        }
        retry_delay += 2000;
    }
    if (long_path != long_path_buffer)
    {
        Longtail_Free(long_path);
    }
    (*out_file_lock)->handle = handle;
    return 0;
}

int Longtail_UnlockFile(HLongtail_FileLock file_lock)
{
    BOOL ok = CloseHandle(file_lock->handle);
    if (!ok)
    {
        DWORD error = GetLastError();
        return Win32ErrorToErrno(error);
    }
    return 0;
}

int Longtail_MapFile(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, HLongtail_FileMap* out_file_map, const void** out_data_ptr)
{
    HANDLE h = (HANDLE)(handle);

    HLongtail_FileMap m = CreateFileMapping(h,
        0,
        PAGE_READONLY,
        0,
        0,
        0);

    if (m == NULL)
    {
        DWORD error = GetLastError();
        return Win32ErrorToErrno(error);
    }

    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    uint64_t base_offset = offset & ~((uint64_t)(system_info.dwAllocationGranularity - 1));
    uint64_t address_offset = offset - base_offset;
    uint64_t base_size = length + address_offset;
    const uint8_t* base_adress = MapViewOfFile(m,
        FILE_MAP_READ,
        (uint32_t)(base_offset >> 32),
        (uint32_t)(base_offset & 0xffffffff),
        base_size);

    if (base_adress == 0)
    {
        DWORD error = GetLastError();
        CloseHandle(m);
        return Win32ErrorToErrno(error);
    }

    *out_file_map = m;
    *out_data_ptr = &base_adress[address_offset];

    return 0;
}

void Longtail_UnmapFile(HLongtail_FileMap file_map)
{
    HANDLE handle = (HANDLE)file_map;
    CloseHandle(handle);
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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <pthread.h>
#include <pwd.h>

uint32_t Longtail_GetCPUCount()
{
   return (uint32_t)sysconf(_SC_NPROCESSORS_ONLN);
}

void Longtail_Sleep(uint64_t timeout_us)
{
    usleep((useconds_t)timeout_us);
}

int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount)
{
    return __sync_fetch_and_add(value, amount) + amount;
}

int64_t Longtail_AtomicAdd64(TLongtail_Atomic64* value, int64_t amount)
{
    return __sync_fetch_and_add(value, amount) + amount;
}

int Longtail_CompareAndSwap(TLongtail_Atomic32* value, int32_t expected, int32_t wanted)
{
    return __sync_val_compare_and_swap(value, expected, wanted) == expected;
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

int Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data, int priority, HLongtail_Thread* out_thread)
{
    struct Longtail_Thread* thread      = (struct Longtail_Thread*)mem;
    thread->m_ThreadFunc                = thread_func;
    thread->m_ContextData               = context_data;
    thread->m_Exited                    = 0;

    int err = 0;
    int attr_err = EINVAL;
    int exit_lock_err = EINVAL;
    int exit_cont_err = EINVAL;
    int thread_err = EINVAL;
    int sched_attr_err = EINVAL;
    int prio_min = 0;
    int prio_max = 0;
    struct sched_param sched_options;

    pthread_attr_t attr;
    attr_err = pthread_attr_init(&attr);
    if (attr_err != 0)
    {
        err = attr_err;
        goto error;
    }

    if (priority != 0)
    {
        prio_min = sched_get_priority_min(SCHED_RR);
        prio_max = sched_get_priority_max(SCHED_RR);
        sched_attr_err = pthread_attr_setschedpolicy(&attr, SCHED_RR);
        if (sched_attr_err)
        {
            err = sched_attr_err;
            goto error;
        }

        switch (priority)
        {
            case 1:
                sched_options.sched_priority = (prio_max - prio_min) / 2 + (prio_max - prio_min) / 4;
                break;
            case 2:
                sched_options.sched_priority = prio_max;
                break;
            case -1:
                sched_options.sched_priority = (prio_max - prio_min) / 2 - (prio_max - prio_min) / 4;
                break;
            case -2:
                sched_options.sched_priority = prio_min;
                break;
           default:
               return EINVAL;
        }

        sched_attr_err = pthread_attr_setschedparam(&attr, &sched_options);
        if (sched_attr_err)
        {
            err = sched_attr_err;
            goto error;
        }
    }

    exit_lock_err = pthread_mutex_init(&thread->m_ExitLock, 0);
    if (exit_lock_err != 0)
    {
        err = exit_lock_err;
        goto error;
    }

    exit_cont_err = pthread_cond_init(&thread->m_ExitConditionalVariable, 0);
    if (exit_cont_err != 0)
    {
        err = exit_cont_err;
        goto error;
    }

    if (stack_size != 0)
    {
        err = pthread_attr_setstacksize(&attr, stack_size);
        if (err != 0)
        {
            goto error;
        }
    }

    thread_err = pthread_create(&thread->m_Handle, &attr, ThreadStartFunction, (void*)thread);
    if (thread_err != 0)
    {
        err = thread_err;
        goto error;
    }
    pthread_attr_destroy(&attr);
    *out_thread = thread;

    return 0;

error:
    if (exit_cont_err == 0)
    {
        pthread_cond_destroy(&thread->m_ExitConditionalVariable);
    }
    if (exit_lock_err == 0)
    {
        pthread_mutex_destroy(&thread->m_ExitLock);
    }
    if (attr_err == 0)
    {
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
    int err = GetTimeSpec(&ts, timeout_us);
    if (err != 0)
    {
        return err;
    }
    err = pthread_mutex_lock(&thread->m_ExitLock);
    if (err != 0)
    {
        return err;
    }
    while (!thread->m_Exited)
    {
        err = pthread_cond_timedwait(&thread->m_ExitConditionalVariable, &thread->m_ExitLock, &ts);
        if (err == ETIMEDOUT)
        {
            pthread_mutex_unlock(&thread->m_ExitLock);
            return err;
        }
    }
    pthread_mutex_unlock(&thread->m_ExitLock);
    err = pthread_join(thread->m_Handle, 0);
    if (err == 0)
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

uint64_t Longtail_GetCurrentThreadId()
{
    return (uint64_t)pthread_self();
}

/*
    struct stat path_stat;
    int err = stat(path, &path_stat);
    if (0 == err)
    {
        return S_ISDIR(path_stat.st_mode);
    }


Chown()
Chmod
int chmod(const char *path, stat().st_mode);
int chown(const char *path, stat().st_uid, stat().st_gid);


mode_t = unsigned short
uid = short
gid = short
*/
#if !defined(__clang__) || defined(__APPLE__)
#define off64_t off_t
#define ftruncate64 ftruncate
#endif

#ifdef __APPLE__
# include <os/lock.h>
# include <dispatch/dispatch.h>
# include <mach/mach_init.h>
# include <mach/mach_error.h>
# include <mach/semaphore.h>
# include <mach/task.h>

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

int Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    while (count--)
    {
        kern_return_t ret = semaphore_signal(semaphore->m_Semaphore);
        if (ret != KERN_SUCCESS)
        {
            return (int)ret;
        }
    }
    return 0;
}

int Longtail_WaitSema(HLongtail_Sema semaphore, uint64_t timeout_us)
{
    if (timeout_us == LONGTAIL_TIMEOUT_INFINITE)
    {
        kern_return_t ret = semaphore_wait(semaphore->m_Semaphore);
        return (int)ret;
    }

    mach_timespec_t wait_time;
    wait_time.tv_sec = timeout_us / 1000000u;
    wait_time.tv_nsec = (timeout_us * 1000) - (wait_time.tv_sec * 1000000u);
    kern_return_t ret = semaphore_timedwait(semaphore->m_Semaphore, wait_time);
    return (int)ret;
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
    if (err != 0)
    {
        return err;
    }
    *out_sema = semaphore;
    return 0;
}

int Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count)
{
    while (count--)
    {
        int err = sem_post(&semaphore->m_Semaphore);
        if (err != 0)
        {
            return err;
        }
    }
    return 0;
}

int Longtail_WaitSema(HLongtail_Sema semaphore, uint64_t timeout_us)
{
    if (timeout_us == LONGTAIL_TIMEOUT_INFINITE)
    {
        if (0 == sem_wait(&semaphore->m_Semaphore))
        {
            return 0;
        }
        return errno;
    }
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return errno;
    ts.tv_nsec += timeout_us * 1000;
    if (ts.tv_nsec > 1000000000u)
    {
        ++ts.tv_sec;
        ts.tv_nsec -= 1000000000u;
    }
    while (1)
    {
        int s = sem_timedwait(&semaphore->m_Semaphore, &ts);
        if (s == 0)
        {
            return 0;
        }
        int res = errno;
        if (res == EINTR)
        {
            continue;
        }
        return res;
    }
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
    if (err != 0)
    {
        return err;
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

int Longtail_CreateDirectory(const char* path)
{
    int err = mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (err == 0)
    {
        return 0;
    }
    int e = errno;
    if (e == ENOENT)
    {
        // Try to create parent folder
        const char* delim_path = strrchr(path, '/');
        if (delim_path == 0)
        {
            return e;
        }
        char* parent_path = Longtail_Strdup(path);
        parent_path[delim_path - path] = '\0';
        int parent_err = Longtail_CreateDirectory(parent_path);
        Longtail_Free(parent_path);
        if (parent_err != 0 && parent_err != EEXIST)
        {
            return e;
        }
        err = mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (err == 0)
        {
            return 0;
        }
        e = errno;
    }
    return e;
}

int Longtail_MoveFile(const char* source, const char* target)
{
    int err = rename(source, target);
    if (err == 0)
    {
        return 0;
    }
    return errno;
}

int Longtail_IsDir(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e)
    return 0;
}

int Longtail_IsFile(const char* path)
{
#if defined(LONGTAIL_ASSERTS)
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(path, "%s"),
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_OFF)
#else
    struct Longtail_LogContextFmt_Private* ctx = 0;
#endif // defined(LONGTAIL_ASSERTS)

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
    LONGTAIL_LOG(ctx, LONGTAIL_LOG_LEVEL_WARNING, "Can't determine type of `%s`: %d\n", path, e)
    return 0;
}

int Longtail_RemoveDir(const char* path)
{
    int err = rmdir(path);
    if (err == 0)
    {
        return 0;
    }
    return errno;
}

int Longtail_RemoveFile(const char* path)
{
    int err = unlink(path);
    if (err == 0)
    {
        return 0;
    }
    return errno;
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
                return ENOENT;
            }
        }
    return 0;
}

int Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path)
{
    if (path[0] == '~')
    {
        struct passwd *pw = getpwuid(getuid());
        const char *homedir = pw->pw_dir;
        fs_iterator->m_DirPath = (char*)Longtail_Alloc("FSIterator", strlen(homedir) + strlen(path));
        strcpy(fs_iterator->m_DirPath, homedir);
        strcpy(&fs_iterator->m_DirPath[strlen(homedir)], &path[1]);
    }
    else
    {
        fs_iterator->m_DirPath = Longtail_Strdup(path);
    }

    fs_iterator->m_DirStream = opendir(fs_iterator->m_DirPath);
    if (0 == fs_iterator->m_DirStream)
    {
        int e = errno;
        Longtail_Free(fs_iterator->m_DirPath);
        if (e == 0)
        {
            return ENOENT;
        }
        return e;
    }

    fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
    if (fs_iterator->m_DirEntry == 0)
    {
        closedir(fs_iterator->m_DirStream);
        fs_iterator->m_DirStream = 0;
        Longtail_Free(fs_iterator->m_DirPath);
        fs_iterator->m_DirPath = 0;
        return ENOENT;
    }
    int err = Skip(fs_iterator);
    if (err)
    {
        closedir(fs_iterator->m_DirStream);
        fs_iterator->m_DirStream = 0;
        Longtail_Free(fs_iterator->m_DirPath);
        fs_iterator->m_DirPath = 0;
        return err;
    }
    return 0;
}

int Longtail_FindNext(HLongtail_FSIterator fs_iterator)
{
    fs_iterator->m_DirEntry = readdir(fs_iterator->m_DirStream);
    if (fs_iterator->m_DirEntry == 0)
    {
        return ENOENT;
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

int Longtail_GetEntryProperties(HLongtail_FSIterator fs_iterator, uint64_t* out_size, uint16_t* out_permissions, int* out_is_dir)
{
    size_t dir_len = strlen(fs_iterator->m_DirPath);
    size_t file_len = strlen(fs_iterator->m_DirEntry->d_name);
    char* path = (char*)Longtail_Alloc("FSIterator", dir_len + 1 + file_len + 1);
    memcpy(&path[0], fs_iterator->m_DirPath, dir_len);
    path[dir_len] = '/';
    memcpy(&path[dir_len + 1], fs_iterator->m_DirEntry->d_name, file_len);
    path[dir_len + 1 + file_len] = '\0';
    struct stat stat_buf;
    int res = stat(path, &stat_buf);
    if (res == 0)
    {
        *out_permissions = (uint16_t)(stat_buf.st_mode & 0x1FF);
        if ((stat_buf.st_mode & S_IFDIR) == S_IFDIR)
        {
            *out_is_dir = 1;
            *out_size = 0;
        }
        else
        {
            *out_is_dir = 0;
            *out_size = (uint64_t)stat_buf.st_size;
        }
    }
    else
    {
        res = errno;
    }
    Longtail_Free(path);
    return res;
}

int Longtail_OpenReadFile(const char* path, HLongtail_OpenFile* out_read_file)
{
    FILE* f = fopen(path, "rb");
    if (f == 0)
    {
        return errno;
    }
    *out_read_file = (HLongtail_OpenFile)f;
    return 0;
}

int Longtail_OpenWriteFile(const char* path, uint64_t initial_size, HLongtail_OpenFile* out_write_file)
{
    FILE* f = fopen(path, "wb");
    if (!f)
    {
        int e = errno;
        return e;
    }
    if (initial_size > 0)
    {
        int err = ftruncate64(fileno(f), (off64_t)initial_size);
        if (err != 0)
        {
            int e = errno;
            fclose(f);
            return e;
        }
    }
    *out_write_file = (HLongtail_OpenFile)f;
    return 0;
}

int Longtail_OpenAppendFile(const char* path, HLongtail_OpenFile* out_write_file)
{
    FILE* f = fopen(path, "ab");
    if (!f)
    {
        int e = errno;
        return e;
    }
    *out_write_file = (HLongtail_OpenFile)f;
    return 0;
}

int Longtail_SetFileSize(HLongtail_OpenFile handle, uint64_t length)
{
    FILE* f = (FILE*)handle;
    fflush(f);
    int err = ftruncate(fileno(f), (off_t)length);
    if (err == 0)
    {
        fflush(f);
        return 0;
    }
    return errno;
}

int Longtail_SetFilePermissions(const char* path, uint16_t permissions)
{
    return chmod(path, permissions);
}

int Longtail_GetFilePermissions(const char* path, uint16_t* out_permissions)
{
    struct stat stat_buf;
    int res = stat(path, &stat_buf);
    if (res == 0)
    {
        *out_permissions = (uint16_t)(stat_buf.st_mode & 0x1FF);
        return 0;
    }
    res = errno;
    return res;
}

int Longtail_Read(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, void* output)
{
    FILE* f = (FILE*)handle;
    int fd = fileno(f);
    ssize_t length_read = pread(fd, output, (off_t)length, (off_t)offset);
    if (length_read == -1)
    {
        return errno;
    }
    return 0;
}

int Longtail_Write(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, const void* input)
{
    FILE* f = (FILE*)handle;

    int fd = fileno(f);
    ssize_t length_written = pwrite(fd, input, length, offset);
    if (length_written == -1)
    {
        return errno;
    }
    return 0;
}

int Longtail_GetFileSize(HLongtail_OpenFile handle, uint64_t* out_size)
{
    FILE* f = (FILE*)handle;
    if (-1 == fseek(f, 0, SEEK_END))
    {
        return errno;
    }
    *out_size = (uint64_t)ftell(f);
    return 0;
}

void Longtail_CloseFile(HLongtail_OpenFile handle)
{
    FILE* f = (FILE*)handle;
    fclose(f);
}

char* Longtail_ConcatPath(const char* folder, const char* file)
{
    size_t folder_length = strlen(folder);
    size_t file_length = strlen(file);

    int add_delimiter = (folder_length > 0) && (folder[folder_length - 1] != '/');

    size_t path_len = folder_length + (add_delimiter ? 1 : 0) + file_length + 1;
    char* path = (char*)Longtail_Alloc("ConcatPath", path_len);
    strcpy(path, folder);
    if (add_delimiter)
    {
        path[folder_length] = '/';
        ++folder_length;
    }
    strcpy(&path[folder_length], file);
    return path;
}

static int is_path_delimiter(char c)
{
    return (c == '/');
}

char* Longtail_GetParentPath(const char* path)
{
    size_t delim_pos = 0;
    size_t path_len = 0;
    while (path[path_len] != 0)
    {
        if (is_path_delimiter(path[path_len]))
        {
            delim_pos = path_len;
        }
        ++path_len;
    }
    if ((!is_path_delimiter(path[delim_pos])) || delim_pos == 0)
    {
        return 0;
    }

    char* result = (char*)Longtail_Alloc("Longtail_GetParentPath", delim_pos + 1);
    if (!result)
    {
        LONGTAIL_LOG(0, LONGTAIL_LOG_LEVEL_ERROR, "Longtail_Alloc() failed with %d", ENOMEM)
        return 0;
    }
    result[delim_pos] = 0;
    while (delim_pos--)
    {
        result[delim_pos] = path[delim_pos];
    }
    return result;
}

char* Longtail_GetTempFolder()
{
    return Longtail_Strdup("/tmp");
}

uint64_t Longtail_GetProcessIdentity()
{
    char hostname[1023+1];
    gethostname(hostname, sizeof(hostname));
    uint64_t hostname_hash = HostnameFNV1A(hostname, strlen(hostname));
    return ((uint64_t)getpid() << 32) + hostname_hash;
}

struct Longtail_FileLock_private
{
    int fd;
};

size_t Longtail_GetFileLockSize()
{
    return sizeof(struct Longtail_FileLock_private);
}

int Longtail_LockFile(void* mem, const char* path, HLongtail_FileLock* out_file_lock)
{
    *out_file_lock = (HLongtail_FileLock)mem;
    (*out_file_lock)->fd = -1;
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd == -1)
    {
        return errno;
    }
    int err = flock(fd, LOCK_EX);
    if (err == -1)
    {
        close(fd);
        return errno;
    }
    (*out_file_lock)->fd = fd;
    return 0;
}

int Longtail_UnlockFile(HLongtail_FileLock file_lock)
{
    int err = flock(file_lock->fd, LOCK_UN);
    if (err == -1)
    {
        return errno;
    }
    close(file_lock->fd);
    file_lock->fd = -1;
    return 0;
}

struct Longtail_FileMap_private {
    void* m_BaseAddress;
    size_t m_BaseSize;
};

int Longtail_MapFile(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, HLongtail_FileMap* out_file_map, const void** out_data_ptr)
{
    FILE* f = (FILE*)handle;

    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size < 1)
    {
        return errno;
    }
    uint64_t base_offset = offset & ~((uint64_t)(page_size - 1));
    uint64_t address_offset = offset - base_offset;

    int fd = fileno(f);

    struct Longtail_FileMap_private* m = (struct Longtail_FileMap_private*)Longtail_Alloc("Longtail_MapFile", sizeof(struct Longtail_FileMap_private));
    if (!m)
    {
        return ENOMEM;
    }

    m->m_BaseSize = (size_t)(length + offset - base_offset);

    m->m_BaseAddress = mmap(
        0,
        m->m_BaseSize,
        PROT_READ,
        MAP_PRIVATE | MAP_NORESERVE,
        fd,
        base_offset);

    if (m->m_BaseAddress == MAP_FAILED)
    {
        Longtail_Free(m);
        return errno;
    }
    *out_file_map = m;
    *out_data_ptr = &((uint8_t*)m->m_BaseAddress)[address_offset];
    return 0;
}

void Longtail_UnmapFile(HLongtail_FileMap file_map)
{
    munmap(file_map->m_BaseAddress, file_map->m_BaseSize);
    Longtail_Free(file_map);
}

#endif
