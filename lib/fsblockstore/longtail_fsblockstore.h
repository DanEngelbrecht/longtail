#pragma once

#include "../../src/longtail.h"

#ifdef __cplusplus
extern "C" {
#endif

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_StorageAPI* storage_api,
    const char* content_path,
    const char* optional_extension,
    int enable_file_mapping);

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateFSBlockStore2API(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_PersistenceAPI* cache_storage_api,
    const char* content_path,
    const char* optional_extension,
    int enable_file_mapping);

LONGTAIL_EXPORT extern struct Longtail_BlockStoreAPI* Longtail_CreateBaseBlockStoreAPI(
    struct Longtail_JobAPI* job_api,
    struct Longtail_PersistenceAPI* persistence_api,
    struct Longtail_PersistenceAPI* cache_storage_api,
    const char* content_path,
    const char* optional_extension,
    int enable_file_mapping);

#define LONGTAIL_DECLARE_CALLBACK_API(name) \
struct Longtail_Async##name##API;\
\
typedef void (*Longtail_Async##name##_OnCompleteFunc)(struct Longtail_Async##name##API* async_complete_api, int err);\
\
struct Longtail_Async##name##API\
{\
    struct Longtail_API m_API;\
    Longtail_Async##name##_OnCompleteFunc OnComplete;\
};\
\
LONGTAIL_EXPORT uint64_t Longtail_GetAsync##name##APISize();\
LONGTAIL_EXPORT struct Longtail_Async##name##API* Longtail_MakeAsync##name##API(\
    void* mem,\
    Longtail_DisposeFunc dispose_func,\
    Longtail_Async##name##_OnCompleteFunc on_complete_func);\
\
LONGTAIL_EXPORT void Longtail_Async##name##_OnComplete(struct Longtail_Async##name##API* async_complete_api, int err);

#define LONGTAIL_CALLBACK_FUNCTION_TYPE(name) Longtail_Async##name##_OnCompleteFunc


#define LONGTAIL_IMPLEMENT_CALLBACK_API(name)\
uint64_t Longtail_GetAsync##name##APISize()\
{\
    return sizeof(struct Longtail_Async##name##API);\
}\
\
struct Longtail_Async##name##API* Longtail_MakeAsync##name##API(\
void* mem,\
Longtail_DisposeFunc dispose_func,\
Longtail_Async##name##_OnCompleteFunc on_complete_func)\
{\
    MAKE_LOG_CONTEXT_FIELDS(ctx)\
        LONGTAIL_LOGFIELD(mem, "%p"),\
        LONGTAIL_LOGFIELD(dispose_func, "%p"),\
        LONGTAIL_LOGFIELD(on_complete_func, "%p")\
        MAKE_LOG_CONTEXT_WITH_FIELDS(ctx, 0, LONGTAIL_LOG_LEVEL_DEBUG)\
\
        LONGTAIL_VALIDATE_INPUT(ctx, mem != 0, return 0)\
        struct Longtail_Async##name##API* api = (struct Longtail_Async##name##API*)mem;\
    api->m_API.Dispose = dispose_func;\
    api->OnComplete = on_complete_func;\
    return api;\
}\
\
void Longtail_Async##name##_OnComplete(struct Longtail_Async##name##API* async_complete_api, int err) { async_complete_api->OnComplete(async_complete_api, err); }

#define LONGTAIL_CALL_CALLBACK_API(name, api, err)\
Longtail_Async##name##_OnComplete(api, err)

#define LONGTAIL_CALLBACK_API(name) struct Longtail_Async##name##API

LONGTAIL_DECLARE_CALLBACK_API(PutBlob)
LONGTAIL_DECLARE_CALLBACK_API(GetBlob)
LONGTAIL_DECLARE_CALLBACK_API(ListBlobs)

typedef int (*Longtail_PersistenceAPI_WriteItemFunc)(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback);
typedef int (*Longtail_PersistenceAPI_ReadItemFunc)(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback);
typedef int (*Longtail_PersistenceAPI_ListItemsFunc)(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback);

struct Longtail_PersistenceAPI
{
    struct Longtail_API m_API;
    Longtail_PersistenceAPI_WriteItemFunc Write;
    Longtail_PersistenceAPI_ReadItemFunc Read;
    Longtail_PersistenceAPI_ListItemsFunc List;
};

/*! @brief Create a Longtail_PersistenceAPI instance.
 *
 * @param[in] mem                   pointer to memory at least sizeof(struct Longtail_PersistenceAPI) bytes
 * @param[in] dispose_func          implementation of Longtail_DisposeFunc
 * @param[in] write_func            implementation of Longtail_PersistenceAPI_WriteItemFunc
 * @param[in] read_func             implementation of Longtail_PersistenceAPI_ReadItemFunc
 * @param[in] list_func             implementation of Longtail_PersistenceAPI_ListItemsFunc
 * @return                          initialize API structure (same address as @p mem)
 */
struct Longtail_PersistenceAPI* Longtail_MakePersistenceAPI(
    void* mem,
    Longtail_DisposeFunc dispose_func,
    Longtail_PersistenceAPI_WriteItemFunc write_func,
    Longtail_PersistenceAPI_ReadItemFunc read_func,
    Longtail_PersistenceAPI_ListItemsFunc list_func);

LONGTAIL_EXPORT int Longtail_PersistenceAPI_Write(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, const void* data, uint64_t size, LONGTAIL_CALLBACK_API(PutBlob)* callback);
LONGTAIL_EXPORT int Longtail_PersistenceAPI_Read(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, void** data, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(GetBlob)* callback);
LONGTAIL_EXPORT int Longtail_PersistenceAPI_List(struct Longtail_PersistenceAPI* persistance_api, const char* sub_path, int recursive, char** name_buffer, uint64_t* size_buffer, LONGTAIL_CALLBACK_API(ListBlobs)* callback);

struct Longtail_PersistenceAPI* Longtail_CreateTestPersistanceAPI(struct Longtail_StorageAPI* storage_api);

#ifdef __cplusplus
}
#endif
