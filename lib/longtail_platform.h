#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

static const uint64_t LONGTAIL_TIMEOUT_INFINITE = ((uint64_t)-1);

uint32_t    Longtail_GetCPUCount();
void        Longtail_Sleep(uint64_t timeout_us);

typedef int32_t volatile TLongtail_Atomic32;
int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount);

typedef int64_t volatile TLongtail_Atomic64;
int64_t Longtail_AtomicAdd64(TLongtail_Atomic64* value, int64_t amount);

typedef struct Longtail_Thread* HLongtail_Thread;

typedef int (*Longtail_ThreadFunc)(void* context_data);

size_t  Longtail_GetThreadSize();
int     Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data, HLongtail_Thread* out_thread);
int     Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us);
void    Longtail_DeleteThread(HLongtail_Thread thread);

typedef struct Longtail_Sema* HLongtail_Sema;
size_t  Longtail_GetSemaSize();
int     Longtail_CreateSema(void* mem, int initial_count, HLongtail_Sema* out_sema);
int     Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count);
int     Longtail_WaitSema(HLongtail_Sema semaphore);
void    Longtail_DeleteSema(HLongtail_Sema semaphore);

typedef struct Longtail_SpinLock* HLongtail_SpinLock;
size_t  Longtail_GetSpinLockSize();
int     Longtail_CreateSpinLock(void* mem, HLongtail_SpinLock* out_spin_lock);
void    Longtail_DeleteSpinLock(HLongtail_SpinLock spin_lock);
void    Longtail_LockSpinLock(HLongtail_SpinLock spin_lock);
void    Longtail_UnlockSpinLock(HLongtail_SpinLock spin_lock);


typedef struct Longtail_FSIterator_private* HLongtail_FSIterator;

size_t Longtail_GetFSIteratorSize();

void    Longtail_NormalizePath(char* path);
void    Longtail_DenormalizePath(char* path);
int     Longtail_CreateDirectory(const char* path);
int     Longtail_MoveFile(const char* source, const char* target);
int     Longtail_IsDir(const char* path);
int     Longtail_IsFile(const char* path);
int     Longtail_RemoveDir(const char* path);
int     Longtail_RemoveFile(const char* path);

int         Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path);
int         Longtail_FindNext(HLongtail_FSIterator fs_iterator);
void        Longtail_CloseFind(HLongtail_FSIterator fs_iterator);
const char* Longtail_GetFileName(HLongtail_FSIterator fs_iterator);
const char* Longtail_GetDirectoryName(HLongtail_FSIterator fs_iterator);
int         Longtail_GetEntryProperties(HLongtail_FSIterator fs_iterator, uint64_t* out_size, uint16_t* out_permissions);

typedef struct Longtail_OpenFile_private* HLongtail_OpenFile;

int     Longtail_OpenReadFile(const char* path, HLongtail_OpenFile* out_read_file);
int     Longtail_OpenWriteFile(const char* path, uint64_t initial_size, HLongtail_OpenFile* out_write_file);
int     Longtail_SetFileSize(HLongtail_OpenFile handle, uint64_t length);
int     Longtail_SetFilePermissions(const char* path, uint64_t permissions);
int     Longtail_Read(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, void* output);
int     Longtail_Write(HLongtail_OpenFile handle, uint64_t offset, uint64_t length, const void* input);
int     Longtail_GetFileSize(HLongtail_OpenFile handle, uint64_t* out_size);
void    Longtail_CloseFile(HLongtail_OpenFile handle);
// Not sure about doing memory allocation here...
const char* Longtail_ConcatPath(const char* folder, const char* file);

char* Longtail_GetTempFolder();

#ifdef __cplusplus
}
#endif
