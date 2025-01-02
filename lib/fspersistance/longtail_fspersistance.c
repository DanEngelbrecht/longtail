#include "longtail_fspersistance.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

struct FSPersistanceAPI_JobItem
{
    const char* sub_path;
    struct ReadOp
    {
        void** data;
        uint64_t* size_buffer;
        LONGTAIL_CALLBACK_API(GetBlob)* callback;
    } Read;
    struct WriteOp
    {
        const void* data;
        uint64_t size;
        LONGTAIL_CALLBACK_API(PutBlob)* callback;
    } Write;
    struct ListOp
    {
        int recursive;
        char** name_buffer;
        uint64_t* size_buffer;
        LONGTAIL_CALLBACK_API(ListBlobs)* callback;
    } List;
    struct DeleteOp
    {
        LONGTAIL_CALLBACK_API(DeleteBlob)* callback;
    } Delete;
};

struct PrefetchItem
{
    char* key;
    uint64_t size;
    uint8_t* buffer;
};

struct FSPersistanceAPI
{
    struct Longtail_PersistenceAPI m_API;
    struct Longtail_StorageAPI* m_StorageAPI;

    struct PrefetchItem* m_PrefetchList;

    struct FSPersistanceAPI_JobItem* m_JobQueue;
    TLongtail_Atomic32 m_WorkerExit;

    HLongtail_SpinLock m_WorkerLock;
    HLongtail_Thread m_WorkerThread;
    HLongtail_Sema m_WorkerSema;
};

int FSPersistanceAPI_Worker(void* context)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(context, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, context != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)context;
    struct Longtail_StorageAPI* storage_api = api->m_StorageAPI;
    while (Longtail_WaitSema(api->m_WorkerSema, LONGTAIL_TIMEOUT_INFINITE) == 0)
    {
        struct FSPersistanceAPI_JobItem* queue = 0;
        {
            Longtail_LockSpinLock(api->m_WorkerLock);
            queue = api->m_JobQueue;
            api->m_JobQueue = 0;
            Longtail_UnlockSpinLock(api->m_WorkerLock);
        }
        while (queue != 0)
        {
            for (intptr_t i = 0; i < arrlen(queue); i++)
            {
                struct FSPersistanceAPI_JobItem* item = &queue[i];
                if (item->Read.size_buffer != 0)
                {
                    {
                        Longtail_LockSpinLock(api->m_WorkerLock);
                        intptr_t prefetch_count = arrlen(api->m_PrefetchList);
                        for (intptr_t index = 0; index < prefetch_count; index++)
                        {
                            if (strcmp(api->m_PrefetchList[index].key, item->sub_path) == 0)
                            {
                                *item->Read.size_buffer = api->m_PrefetchList[index].size;
                                *item->Read.data = api->m_PrefetchList[index].buffer;
                                api->m_PrefetchList[index].buffer = 0;
                            }
                            Longtail_Free(api->m_PrefetchList[index].key);
                            arrdel(api->m_PrefetchList, index);
                        }
                        Longtail_UnlockSpinLock(api->m_WorkerLock);
                    }
                    int err = 0;
                    if (*item->Read.data == 0)
                    {
                        Longtail_StorageAPI_HOpenFile f;
                        int err = storage_api->OpenReadFile(storage_api, item->sub_path, &f);
                        if (err == 0)
                        {
                            err = storage_api->GetSize(storage_api, f, item->Read.size_buffer);
                            if (err == 0)
                            {
                                *item->Read.data = Longtail_Alloc("FSPersistanceAPI", *item->Read.size_buffer);
                                if (*item->Read.data == 0)
                                {
                                    err = ENOMEM;
                                }
                                else
                                {
                                    err = storage_api->Read(storage_api, f, 0, *item->Read.size_buffer, *item->Read.data);
                                    if (err)
                                    {
                                        Longtail_Free(*item->Read.data);
                                        *item->Read.data = 0;
                                        *item->Read.size_buffer = 0;
                                    }
                                }
                            }
                            storage_api->CloseFile(storage_api, f);
                        }
                    }
                    Longtail_AsyncGetBlob_OnComplete(item->Read.callback, err);
                }
                else if (item->Write.size > 0)
                {
                    int err = EnsureParentPathExists(storage_api, item->sub_path);
                    if (err == 0)
                    {
                        Longtail_StorageAPI_HOpenFile f;
                        err = storage_api->OpenWriteFile(storage_api, item->sub_path, 0, &f);
                        if (err == 0)
                        {
                            err = storage_api->Write(storage_api, f, 0, item->Write.size, item->Write.data);
                            storage_api->CloseFile(storage_api, f);
                        }
                    }
                    Longtail_AsyncPutBlob_OnComplete(item->Write.callback, err);
                }
                else if (item->List.size_buffer != 0)
                {
                    size_t NamesBufferSize = 0;
                    char** Names = 0;
                    char** ScanDirs = 0;
                    arrput(ScanDirs, Longtail_Strdup(item->sub_path));
                    intptr_t ScanDirIndex = 0;

                    int err = 0;
                    while (ScanDirIndex < arrlen(ScanDirs))
                    {
                        Longtail_StorageAPI_HIterator Iterator;
                        int err = storage_api->StartFind(storage_api, ScanDirs[ScanDirIndex], &Iterator);
                        if (err == 0)
                        {
                            while (err == 0)
                            {
                                struct Longtail_StorageAPI_EntryProperties properties;
                                err = storage_api->GetEntryProperties(storage_api, Iterator, &properties);
                                if (err)
                                {
                                    break;
                                }
                                if (properties.m_IsDir && item->List.recursive)
                                {
                                    arrput(ScanDirs, storage_api->ConcatPath(storage_api, ScanDirs[ScanDirIndex], properties.m_Name));
                                }
                                arrput(Names, storage_api->ConcatPath(storage_api, ScanDirs[ScanDirIndex], properties.m_Name));
                                NamesBufferSize += strlen(Names[arrlen(Names) - 1]) + 1;
                                err = storage_api->FindNext(storage_api, Iterator);
                            }
                            if (err == ENOENT)
                            {
                                err = 0;
                            }
                            storage_api->CloseFind(storage_api, Iterator);
                        }
                        if (err)
                        {
                            break;
                        }
                        ScanDirIndex++;
                    }

                    for (intptr_t D = 0; D < arrlen(ScanDirs); D++)
                    {
                        Longtail_Free(ScanDirs[D]);
                    }
                    arrfree(ScanDirs);

                    if (err == 0)
                    {
                        *item->List.size_buffer = NamesBufferSize + 1;
                        *item->List.name_buffer = (char*)Longtail_Alloc("FSPersistanceAPI", *item->List.size_buffer);
                        if (*item->List.name_buffer != 0)
                        {
                            char* target_ptr = *item->List.name_buffer;
                            for (intptr_t I = 0; I < arrlen(Names); I++)
                            {
                                strcpy(target_ptr, Names[I]);
                                target_ptr += strlen(Names[I]) + 1;
                            }
                            *target_ptr = 0;
                        }
                        else
                        {
                            *item->List.size_buffer = 0;
                            err = ENOMEM;
                        }

                    }
                    for (intptr_t D = 0; D < arrlen(Names); D++)
                    {
                        Longtail_Free(Names[D]);
                    }
                    arrfree(Names);
                    Longtail_AsyncListBlobs_OnComplete(item->List.callback, err);
                }
                else
                {
                    int err = Longtail_Storage_RemoveFile(storage_api, item->sub_path);
                    if (err == ENOENT)
                    {
                        err = Longtail_Storage_RemoveDir(storage_api, item->sub_path);
                    }
                    Longtail_AsyncDeleteBlob_OnComplete(item->Delete.callback, err);
                }
            }
            arrfree(queue);
            {
                Longtail_LockSpinLock(api->m_WorkerLock);
                queue = api->m_JobQueue;
                api->m_JobQueue = 0;
                Longtail_UnlockSpinLock(api->m_WorkerLock);
            }
        }
        {
            Longtail_LockSpinLock(api->m_WorkerLock);
            char* first_pending_item = 0;
            uint64_t prefetched_size = 0;
            ptrdiff_t prefetch_count = arrlen(api->m_PrefetchList);
            for (ptrdiff_t prefetch_item = 0; prefetch_item < prefetch_count; prefetch_item++)
            {
                if (api->m_PrefetchList[prefetch_item].buffer != 0)
                {
                    prefetched_size += api->m_PrefetchList[prefetch_item].size;
                }
                else
                {
                    first_pending_item = Longtail_Strdup(api->m_PrefetchList[prefetch_item].key);
                }
            }
            Longtail_UnlockSpinLock(api->m_WorkerLock);
            if (first_pending_item != 0)
            {
                if (prefetched_size < 16u * 1024u * 1024u)   // 16MB TODO: Use constant/configure
                {
                    Longtail_StorageAPI_HOpenFile f;
                    int err = storage_api->OpenReadFile(storage_api, first_pending_item, &f);
                    if (err == 0)
                    {
                        void* buffer = 0;
                        uint64_t size = 0;
                        err = storage_api->GetSize(storage_api, f, &size);
                        if (err == 0)
                        {
                            buffer = Longtail_Alloc("FSPersistanceAPI", size);
                            if (buffer)
                            {
                                err = storage_api->Read(storage_api, f, 0, size, buffer);
                                if (err)
                                {
                                    arrfree(buffer);
                                    buffer = 0;
                                    size = 0;
                                }
                            }
                            else
                            {
                                err = ENOMEM;
                            }
                        }
                        storage_api->CloseFile(storage_api, f);
                        if (buffer)
                        {
                            Longtail_LockSpinLock(api->m_WorkerLock);

                            for (ptrdiff_t prefetch_item = 0; prefetch_item < prefetch_count; prefetch_item++)
                            {
                                if (strcmp(first_pending_item, api->m_PrefetchList[prefetch_item].key) == 0)
                                {
                                    api->m_PrefetchList[prefetch_item].size = size;
                                    api->m_PrefetchList[prefetch_item].buffer = buffer;
                                    buffer = 0;
                                    break;
                                }
                            }
                            Longtail_UnlockSpinLock(api->m_WorkerLock);
                            if (buffer)
                            {
                                arrfree(buffer);
                            }
                        }
                    }
                }
                Longtail_Free(first_pending_item);
            }
        }

        if (api->m_WorkerExit != 0)
        {
            break;
        }
    }
    return 0;
}

static void FSPersistanceAPI_Dispose(struct Longtail_API* longtail_api)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(longtail_api, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, longtail_api != 0, return)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)(longtail_api);
    Longtail_AtomicAdd32(&api->m_WorkerExit, 1);
    Longtail_PostSema(api->m_WorkerSema, 1);
    Longtail_JoinThread(api->m_WorkerThread, LONGTAIL_TIMEOUT_INFINITE);
    Longtail_DeleteThread(api->m_WorkerThread);
    Longtail_DeleteSema(api->m_WorkerSema);
    Longtail_DeleteSpinLock(api->m_WorkerLock);
    arrfree(api->m_JobQueue);

    ptrdiff_t prefetch_count = arrlen(api->m_PrefetchList);
    for (ptrdiff_t prefetch_item = 0; prefetch_item < prefetch_count; prefetch_item++)
    {
        Longtail_Free(api->m_PrefetchList[prefetch_item].key);
        Longtail_Free(api->m_PrefetchList[prefetch_item].buffer);
    }

    arrfree(api->m_PrefetchList);
    Longtail_Free(api);
}

static int FSPersistanceAPI_WriteItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(persistance_api, "%p"),
        LONGTAIL_LOGFIELD(sub_path, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(size, "%p"),
        LONGTAIL_LOGFIELD(callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistance_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, callback != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    {
        Longtail_LockSpinLock(api->m_WorkerLock);
        struct FSPersistanceAPI_JobItem Job = { sub_path, {0, 0, 0}, {data, size, callback}, {0, 0, 0, 0}, {0} };
        arrput(api->m_JobQueue, Job);
        Longtail_UnlockSpinLock(api->m_WorkerLock);
    }
    Longtail_PostSema(api->m_WorkerSema, 1);
    return 0;
}
static int FSPersistanceAPI_ReadItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(persistance_api, "%p"),
        LONGTAIL_LOGFIELD(sub_path, "%p"),
        LONGTAIL_LOGFIELD(data, "%p"),
        LONGTAIL_LOGFIELD(size_buffer, "%p"),
        LONGTAIL_LOGFIELD(callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistance_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, data != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, callback != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    {
        Longtail_LockSpinLock(api->m_WorkerLock);
        struct FSPersistanceAPI_JobItem Job = { sub_path, {data, size_buffer, callback}, {0, 0, 0}, {0, 0, 0, 0}, {0} };
        arrput(api->m_JobQueue, Job);
        Longtail_UnlockSpinLock(api->m_WorkerLock);
    }
    Longtail_PostSema(api->m_WorkerSema, 1);
    return 0;
}

static int FSPersistanceAPI_DeleteItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, LONGTAIL_CALLBACK_API(DeleteBlob)* callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(persistance_api, "%p"),
        LONGTAIL_LOGFIELD(sub_path, "%p"),
        LONGTAIL_LOGFIELD(callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistance_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, callback != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    {
        Longtail_LockSpinLock(api->m_WorkerLock);
        struct FSPersistanceAPI_JobItem Job = { sub_path, {0, 0, 0}, {0, 0, 0}, {0, 0, 0, 0}, {callback} };
        arrput(api->m_JobQueue, Job);
        Longtail_UnlockSpinLock(api->m_WorkerLock);
    }
    Longtail_PostSema(api->m_WorkerSema, 1);
    return 0;
}

static int FSPersistanceAPI_ListItems(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(persistance_api, "%p"),
        LONGTAIL_LOGFIELD(sub_path, "%p"),
        LONGTAIL_LOGFIELD(recursive, "%p"),
        LONGTAIL_LOGFIELD(name_buffer, "%p"),
        LONGTAIL_LOGFIELD(size_buffer, "%p"),
        LONGTAIL_LOGFIELD(callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistance_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, sub_path != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, name_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, recursive == 0 || recursive == 1, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, size_buffer != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, callback != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    {
        Longtail_LockSpinLock(api->m_WorkerLock);
        struct FSPersistanceAPI_JobItem Job = { sub_path, {0, 0, 0}, {0, 0, 0}, {recursive, name_buffer, size_buffer, callback}, {0} };
        arrput(api->m_JobQueue, Job);
        Longtail_UnlockSpinLock(api->m_WorkerLock);
    }
    Longtail_PostSema(api->m_WorkerSema, 1);
    return 0;
}

static int FSPersistanceAPI_PrefetchItems(struct Longtail_PersistenceAPI* persistance_api, uint32_t count, const char** sub_paths, LONGTAIL_CALLBACK_API(PrefetchBlobs)* callback)
{
    MAKE_LOG_CONTEXT_FIELDS(ctx)
        LONGTAIL_LOGFIELD(persistance_api, "%p"),
        LONGTAIL_LOGFIELD(count, "%u"),
        LONGTAIL_LOGFIELD(sub_paths, "%p"),
        LONGTAIL_LOGFIELD(callback, "%p")
    MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)

    LONGTAIL_VALIDATE_INPUT(ctx, persistance_api != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, count == 0 || sub_paths != 0, return EINVAL)
    LONGTAIL_VALIDATE_INPUT(ctx, callback != 0, return EINVAL)

    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    {
        Longtail_LockSpinLock(api->m_WorkerLock);
        for (uint32_t index = 0; index < count; index++)
        {
            struct PrefetchItem Value = { Longtail_Strdup(sub_paths[index]), 0, 0 };
            arrput(api->m_PrefetchList, Value);
        }
        Longtail_UnlockSpinLock(api->m_WorkerLock);
    }
    Longtail_PostSema(api->m_WorkerSema, 1);
    callback->OnComplete(callback, 0);
    return 0;
}

struct Longtail_PersistenceAPI* Longtail_CreateFSPersistanceAPI(struct Longtail_StorageAPI* storage_api)
{

    size_t MemSize = sizeof(struct FSPersistanceAPI) + Longtail_GetSpinLockSize() + Longtail_GetSemaSize() + Longtail_GetThreadSize();
    void* Mem = Longtail_Alloc("Longtail_CreateFSPersistanceAPI", MemSize);
    if (Mem == 0)
    {
        return 0;
    }
    struct Longtail_PersistenceAPI* persistance_api = Longtail_MakePersistenceAPI(
        Mem, 
        FSPersistanceAPI_Dispose, 
        FSPersistanceAPI_WriteItem, 
        FSPersistanceAPI_ReadItem, 
        FSPersistanceAPI_DeleteItem, 
        FSPersistanceAPI_ListItems, 
        FSPersistanceAPI_PrefetchItems);
    struct FSPersistanceAPI* api = (struct FSPersistanceAPI*)persistance_api;
    api->m_StorageAPI = storage_api;
    api->m_PrefetchList = 0;
    api->m_JobQueue = 0;
    api->m_WorkerExit = 0;
    uint8_t* MemPtr = (uint8_t*)&api[1];
    
    int err = Longtail_CreateSpinLock(MemPtr, &api->m_WorkerLock);
    if (err)
    {
        Longtail_Free(Mem);
        return 0;
    }
    MemPtr += Longtail_GetSpinLockSize();
    err = Longtail_CreateSema(MemPtr, 0, &api->m_WorkerSema);
    if (err)
    {
        Longtail_DeleteSpinLock(api->m_WorkerLock);
        Longtail_Free(Mem);
        return 0;
    }
    MemPtr += Longtail_GetSemaSize();
    err = Longtail_CreateThread(MemPtr, FSPersistanceAPI_Worker, 0, Mem, 0, &api->m_WorkerThread);
    if (err)
    {
        Longtail_DeleteSema(api->m_WorkerSema);
        Longtail_DeleteSpinLock(api->m_WorkerLock);
        Longtail_Free(Mem);
        return 0;
    }

    return persistance_api;
}
