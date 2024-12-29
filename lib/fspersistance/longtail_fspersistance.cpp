#include "longtail_fspersistance.h"

#include "../../src/ext/stb_ds.h"
#include "../longtail_platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

class FSPersistanceAPI
{
public:
    FSPersistanceAPI(struct Longtail_StorageAPI* storage_api)
        : m_StorageAPI(storage_api)
    {
        Longtail_MakePersistenceAPI(&m_API, Dispose, WriteItem, ReadItem, DeleteItem, ListItems);
        m_API.m_Owner = this;

        Longtail_CreateSpinLock(Longtail_Alloc("FSPersistanceAPI", Longtail_GetSpinLockSize()), &m_WorkerLock);
        Longtail_CreateSema(Longtail_Alloc("FSPersistanceAPI", Longtail_GetSemaSize()), 0, &m_WorkerSema);
        Longtail_CreateThread(Longtail_Alloc("FSPersistanceAPI", Longtail_GetThreadSize()), FSPersistanceAPI::Thread, 0, this, 0, &m_WorkerThread);
    }
    ~FSPersistanceAPI()
    {
        m_API.m_Owner = 0;
        Longtail_AtomicAdd32(&m_WorkerExit, 1);
        Longtail_PostSema(m_WorkerSema, 1);
        Longtail_JoinThread(m_WorkerThread, LONGTAIL_TIMEOUT_INFINITE);
        Longtail_DeleteThread(m_WorkerThread);
        Longtail_Free((void*)m_WorkerThread);
        Longtail_DeleteSema(m_WorkerSema);
        Longtail_Free((void*)m_WorkerSema);
        Longtail_DeleteSpinLock(m_WorkerLock);
        Longtail_Free((void*)m_WorkerLock);
    }
    operator struct Longtail_PersistenceAPI* () { return &m_API.m_API; };
private:
    struct API
    {
        struct Longtail_PersistenceAPI m_API;
        FSPersistanceAPI* m_Owner;
    } m_API;

    struct JobItem
    {
        const char* sub_path;
        struct ReadOp
        {
            void** data = 0;
            uint64_t* size_buffer = 0;
            LONGTAIL_CALLBACK_API(GetBlob)* callback;
        } Read;
        struct WriteOp
        {
            const void* data = 0;
            uint64_t size = 0;
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

    JobItem* m_JobQueue = 0;

    struct Longtail_StorageAPI* m_StorageAPI;
    HLongtail_SpinLock m_WorkerLock;
    HLongtail_Thread m_WorkerThread;
    HLongtail_Sema m_WorkerSema;
    TLongtail_Atomic32 m_WorkerExit = 0;

    void Worker()
    {
        while (Longtail_WaitSema(m_WorkerSema, LONGTAIL_TIMEOUT_INFINITE) == 0)
        {
            JobItem* queue = 0;
            {
                Longtail_LockSpinLock(m_WorkerLock);
                queue = m_JobQueue;
                m_JobQueue = 0;
                Longtail_UnlockSpinLock(m_WorkerLock);
            }

            for (intptr_t i = 0; i < arrlen(queue); i++)
            {
                JobItem& item = queue[i];
                if (item.Read.size_buffer != 0)
                {
                    Longtail_StorageAPI_HOpenFile f;
                    int err = m_StorageAPI->OpenReadFile(m_StorageAPI, item.sub_path, &f);
                    if (err == 0)
                    {
                        err = m_StorageAPI->GetSize(m_StorageAPI, f, item.Read.size_buffer);
                        if (err == 0)
                        {
                            *item.Read.data = Longtail_Alloc("FSPersistanceAPI", *item.Read.size_buffer);
                            if (*item.Read.data == 0)
                            {
                                err = ENOMEM;
                            }
                            else
                            {
                                err = m_StorageAPI->Read(m_StorageAPI, f, 0, *item.Read.size_buffer, *item.Read.data);
                                if (err)
                                {
                                    Longtail_Free(*item.Read.data);
                                    *item.Read.data = 0;
                                    *item.Read.size_buffer = 0;
                                }
                            }
                        }
                        m_StorageAPI->CloseFile(m_StorageAPI, f);
                    }
                    Longtail_AsyncGetBlob_OnComplete(item.Read.callback, err);
                }
                else if (item.Write.size > 0)
                {
                    int err = EnsureParentPathExists(m_StorageAPI, item.sub_path);
                    if (err == 0)
                    {
                        Longtail_StorageAPI_HOpenFile f;
                        err = m_StorageAPI->OpenWriteFile(m_StorageAPI, item.sub_path, 0, &f);
                        if (err == 0)
                        {
                            err = m_StorageAPI->Write(m_StorageAPI, f, 0, item.Write.size, item.Write.data);
                            m_StorageAPI->CloseFile(m_StorageAPI, f);
                        }
                    }
                    Longtail_AsyncPutBlob_OnComplete(item.Write.callback, err);
                }
                else if (item.List.size_buffer != 0)
                {
                    size_t NamesBufferSize = 0;
                    char** Names = 0;
                    char** ScanDirs = 0;
                    arrput(ScanDirs, Longtail_Strdup(item.sub_path));
                    intptr_t ScanDirIndex = 0;

                    int err = 0;
                    while (ScanDirIndex < arrlen(ScanDirs))
                    {
                        Longtail_StorageAPI_HIterator Iterator;
                        int err = m_StorageAPI->StartFind(m_StorageAPI, ScanDirs[ScanDirIndex], &Iterator);
                        if (err == 0)
                        {
                            while (err == 0)
                            {
                                struct Longtail_StorageAPI_EntryProperties properties;
                                err = m_StorageAPI->GetEntryProperties(m_StorageAPI, Iterator, &properties);
                                if (err)
                                {
                                    break;
                                }
                                if (properties.m_IsDir && item.List.recursive)
                                {
                                    arrput(ScanDirs, m_StorageAPI->ConcatPath(m_StorageAPI, ScanDirs[ScanDirIndex], properties.m_Name));
                                }
                                arrput(Names, m_StorageAPI->ConcatPath(m_StorageAPI, ScanDirs[ScanDirIndex], properties.m_Name));
                                NamesBufferSize += strlen(Names[arrlen(Names) - 1]) + 1;
                                err = m_StorageAPI->FindNext(m_StorageAPI, Iterator);
                            }
                            if (err == ENOENT)
                            {
                                err = 0;
                            }
                            m_StorageAPI->CloseFind(m_StorageAPI, Iterator);
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
                        *item.List.size_buffer = NamesBufferSize + 1;
                        *item.List.name_buffer = (char*)Longtail_Alloc("FSPersistanceAPI", *item.List.size_buffer);
                        if (*item.List.name_buffer != 0)
                        {
                            char* target_ptr = *item.List.name_buffer;
                            for (intptr_t I = 0; I < arrlen(Names); I++)
                            {
                                strcpy(target_ptr, Names[I]);
                                target_ptr += strlen(Names[I]) + 1;
                            }
                            *target_ptr = 0;
                        }
                        else
                        {
                            *item.List.size_buffer = 0;
                            err = ENOMEM;
                        }

                    }
                    for (intptr_t D = 0; D < arrlen(Names); D++)
                    {
                        Longtail_Free(Names[D]);
                    }
                    arrfree(Names);
                    Longtail_AsyncListBlobs_OnComplete(item.List.callback, err);
                }
                else
                {
                    int err = Longtail_Storage_RemoveFile(m_StorageAPI, item.sub_path);
                    if (err != 0)
                    {
                        int err2 = Longtail_Storage_RemoveDir(m_StorageAPI, item.sub_path);
                        if (err2 == 0)
                        {
                            err = 0;
                        }
                    }
                    Longtail_AsyncDeleteBlob_OnComplete(item.Delete.callback, err);
                }
            }
            arrfree(queue);

            if (m_WorkerExit != 0)
            {
                break;
            }
        }
    }

    static int Thread(void* context_data)
    {
        FSPersistanceAPI* persistance_api = (FSPersistanceAPI*)context_data;
        persistance_api->Worker();
        return 0;
    }

    int Write(const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
    {
        {
            Longtail_LockSpinLock(m_WorkerLock);
            JobItem Job = { sub_path, {}, {data, size, callback}, {}, {} };
            arrput(m_JobQueue, Job);
            Longtail_UnlockSpinLock(m_WorkerLock);
        }
        Longtail_PostSema(m_WorkerSema, 1);
        return 0;
    }
    int Read(const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
    {
        {
            Longtail_LockSpinLock(m_WorkerLock);
            JobItem Job = { sub_path, {data, size_buffer, callback}, {}, {}, {} };
            arrput(m_JobQueue, Job);
            Longtail_UnlockSpinLock(m_WorkerLock);
        }
        Longtail_PostSema(m_WorkerSema, 1);
        return 0;
    }
    int List(const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
    {
        {
            Longtail_LockSpinLock(m_WorkerLock);
            JobItem Job = { sub_path, {}, {}, {recursive, name_buffer, size_buffer, callback}, {} };
            arrput(m_JobQueue, Job);
            Longtail_UnlockSpinLock(m_WorkerLock);
        }
        Longtail_PostSema(m_WorkerSema, 1);
        return 0;
    }
    int Delete(const char* sub_path, LONGTAIL_CALLBACK_API(DeleteBlob)* callback)
    {
        {
            Longtail_LockSpinLock(m_WorkerLock);
            JobItem Job = { sub_path, {}, {}, {}, {callback} };
            arrput(m_JobQueue, Job);
            Longtail_UnlockSpinLock(m_WorkerLock);
        }
        Longtail_PostSema(m_WorkerSema, 1);
        return 0;
    }

    static void Dispose(struct Longtail_API* longtail_api)
    {
        struct API* api = (struct API*)(longtail_api);
        delete api->m_Owner;
    }

    static int WriteItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->Write(sub_path, data, size, callback);
    }
    static int ReadItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->Read(sub_path, data, size_buffer, callback);
    }
    static int DeleteItem(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, LONGTAIL_CALLBACK_API(DeleteBlob)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->Delete(sub_path, callback);
    }
    static int ListItems(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback)
    {
        struct API* api = (struct API*)persistance_api;
        return api->m_Owner->List(sub_path, recursive, name_buffer, size_buffer, callback);
    }

};

struct Longtail_PersistenceAPI* Longtail_CreateFSPersistanceAPI(struct Longtail_StorageAPI* storage_api)
{
    FSPersistanceAPI* api = new FSPersistanceAPI(storage_api);
    return *api;
}
