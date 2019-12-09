#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

static const uint64_t LONGTAIL_TIMEOUT_INFINITE = ((uint64_t)-1);

size_t  Longtail_GetCPUCount();
void    Longtail_Sleep(uint64_t timeout_us);

typedef int32_t volatile TLongtail_Atomic32;
int32_t Longtail_AtomicAdd32(TLongtail_Atomic32* value, int32_t amount);

typedef struct Longtail_Thread* HLongtail_Thread;

typedef int (*Longtail_ThreadFunc)(void* context_data);

size_t              Longtail_GetThreadSize();
HLongtail_Thread    Longtail_CreateThread(void* mem, Longtail_ThreadFunc thread_func, size_t stack_size, void* context_data);
int                 Longtail_JoinThread(HLongtail_Thread thread, uint64_t timeout_us);
void                Longtail_DeleteThread(HLongtail_Thread thread);

typedef struct Longtail_Sema* HLongtail_Sema;
size_t          Longtail_GetSemaSize();
HLongtail_Sema  Longtail_CreateSema(void* mem, int initial_count);
int             Longtail_PostSema(HLongtail_Sema semaphore, unsigned int count);
int             Longtail_WaitSema(HLongtail_Sema semaphore);
void            Longtail_DeleteSema(HLongtail_Sema semaphore);




typedef struct Longtail_FSIterator_private* HLongtail_FSIterator;

size_t Longtail_GetFSIteratorSize();

void Longtail_NormalizePath(char* path);
void Longtail_DenormalizePath(char* path);
int Longtail_CreateDirectory(const char* path);
int Longtail_MoveFile(const char* source, const char* target);
int Longtail_IsDir(const char* path);
int Longtail_IsFile(const char* path);
int Longtail_RemoveDir(const char* path);
int Longtail_RemoveFile(const char* path);

int Longtail_StartFind(HLongtail_FSIterator fs_iterator, const char* path);
int Longtail_FindNext(HLongtail_FSIterator fs_iterator);
void Longtail_CloseFind(HLongtail_FSIterator fs_iterator);
const char* Longtail_GetFileName(HLongtail_FSIterator fs_iterator);
const char* Longtail_GetDirectoryName(HLongtail_FSIterator fs_iterator);
uint64_t Longtail_GetEntrySize(HLongtail_FSIterator fs_iterator);

typedef struct Longtail_OpenReadFile_private* HLongtail_OpenReadFile;
typedef struct Longtail_OpenWriteFile_private* HLongtail_OpenWriteFile;

HLongtail_OpenReadFile Longtail_OpenReadFile(const char* path);
HLongtail_OpenWriteFile Longtail_OpenWriteFile(const char* path, uint64_t initial_size);
int Longtail_SetFileSize(HLongtail_OpenWriteFile handle, uint64_t length);
int Longtail_Read(HLongtail_OpenReadFile handle, uint64_t offset, uint64_t length, void* output);
int Longtail_Write(HLongtail_OpenWriteFile handle, uint64_t offset, uint64_t length, const void* input);
uint64_t Longtail_GetFileSize(HLongtail_OpenReadFile handle);
void Longtail_CloseReadFile(HLongtail_OpenReadFile handle);
void Longtail_CloseWriteFile(HLongtail_OpenWriteFile handle);
// Not sure about doing memory allocation here...
const char* Longtail_ConcatPath(const char* folder, const char* file);

#ifdef __cplusplus
}
#endif
