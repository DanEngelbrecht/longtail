##
- **FIXED** Update permissions on added files and files with unmodified content
- **FIXED** Fix potential Longtail_GetFilesRecursively2 buffer overrun (@webbju)
- **FIXED** Fix OnGetStoredBlockPutLocalComplete error code reporting (@webbju)
- **FIXED** Fix Longtail_Storage_OpenAppendFile prototype (@webbju)
- **CHANGED** Attempt to use sparse files on windows when writing out a files larger than 16 MB

## 0.4.3
- **FIXED** Fixed file corruption on Linux when using `--use-legacy-write` option. [chris-believer](https://github.com/chris-believer)
- **FIXED** Fixed large file corruption. [timsjostrand](https://github.com/timsjostrand)
- **UPDATED** Update of ZStd: 1.5.6 https://github.com/facebook/zstd/releases/tag/v1.5.6
- **UPDATED** Update of LZ4: 1.10.0 https://github.com/lz4/lz4/releases/tag/v1.10.0
- **UPDATED** Update of Blake3: 1.5.4 https://github.com/BLAKE3-team/BLAKE3/releases/tag/1.5.4

## 0.4.2
- **CHANGED API** `Longtail_JobAPI_JobFunc` renamed `is_cancelled` to `detected_error`, now contains first error returned from a job task in the same job group (if any) or ECANCELLED if job group was cancelled
    If `detected_error` is non-zero, try to exit (and cleanup) your task directly and return `0`.
- **CHANGED_API** JobAPI `WaitForAllJobs` now returns first error encountered in a job group for a task as well as any error in the job api itself, removing the need to book keep the error for tasks separately
- **CHANGED API** `Longtail_StorageAPI.OpenAppend` added to `Longtail_StorageAPI` to open files without truncating existing data
- **CHANGED API** `Longtail_CreateConcurrentChunkWriteAPI` changed to take `source_version_index` and `version_diff`
- **CHANGED API** `Longtail_ConcurrentChunkWriteAPI` refactored to use asset index and open/close files instead of keeping all open during entire lifetime
  - `Longtail_ConcurrentChunkWriteAPI.CreateDir` now takes asset index instead of version local path
  - `Longtail_ConcurrentChunkWriteAPI.Open` now takes asset index instead of version local path and dropping `chunk_write_count` parameter
  - `Longtail_ConcurrentChunkWriteAPI.Write` now takes asset index instead of version local path and dropping `chunk_write_count` parameter
- **CHANGED API** `Longtail_SetMonitor` callback functions refactored to accomodate changes in `Longtail_ConcurrentChunkWriteAPI`
- **NEW API** `Longtail_SetReAllocAndFree`
- **NEW API** `Longtail_ReAlloc`
- **NEW API** `Longtail_MemTracer_ReAlloc`
- **NEW API** `Longtail_CompareAndSwap` compare and swap with platform implementations
- **NEW API** `Longtail_RunJobsBatched` runs jobs in batched mode to handle a job count larger than Longtail_JobAPI::GetMaxBatchCount()
- **NEW API** `Longtail_GetFilesRecursively2` that executes using parallel jobs improving execution speed for large file trees significantly
- **REMOVED API** `Longtail_SetAllocAndFree` is replaced by `Longtail_SetReAllocAndFree`
- **REMOVED API** `Longtail_MemTracer_Alloc` is replaced by `Longtail_MemTracer_ReAlloc`
- **REMOVED API** Remove platform api for Read/Write mutex
  - `Longtail_GetRWLockSize`
  - `Longtail_CreateRWLock`
  - `Longtail_DeleteRWLock`
  - `Longtail_LockRWLockRead`
  - `Longtail_LockRWLockWrite`
  - `Longtail_UnlockRWLockRead`
  - `Longtail_UnlockRWLockWrite`
- **ADDED** memtracer now tracks allocations in stb_ds
- **ADDED** memtracer now tracks allocations in zstd
- **FIXED** Fixed memory leaks in command tool
- **FIXED** `Longtail_ChangeVersion2()` can now handle workloads with a block count larger than 65535
- **FIXED** Bikeshed JobAPI implementation does efficient wait when task queue is full
- **FIXED** Bikeshed JobAPI::CreateJobs implementation now properly drains both task channels when task queue is full
- **FIXED** Make sure we retain order of assets with equal length when sorting them
- **FIXED** Fixed excessive "Disk Used" increase during `Longtail_ChangeVersion2` execution causing Out Of Disk space errors.
  The changes also improves performance for more common cases with smaller archive sizes (60 Gb raw data/many files) but causes a small regression compared to 0.4.1 for archives with many very large files. It is still performing much more reasonable than 0.4.0 for these cases.
  | Version | Files | Raw Size | Compressed Size | Unpack Time | Peak Memory |
  |-|-|-|-|-|-|
  |0.4.0|1019|735 GB|214 GB|2h44m26s|7.9 GB|
  |0.4.1|1019|735 GB|214 GB|0h12m14s|1.9 GB|
  |0.4.2|1019|735 GB|214 GB|0h13m25s|2.2 GB|
  |0.4.0|239 340|60 GB|17 GB|0h01m24s|4.2 GB|
  |0.4.1|239 340|60 GB|17 GB|0h02m48s|0.9 GB|
  |0.4.2|239 340|60 GB|17 GB|0h01m12s|0.9 GB|
- **CHANGED** Refactored all internal usage of JobAPI `ReadyJobs` with new error handling
- **UPDATED** Update of ZStd: 1.5.5 https://github.com/facebook/zstd/releases/tag/v1.5.5
- **UPDATED** Update of Blake3: 1.5.0 https://github.com/BLAKE3-team/BLAKE3/releases/tag/1.5.0
- **UPDATED** Update of Brotli: 1.1.0 https://github.com/google/brotli/releases/tag/v1.1.0
- **EXPERIMENTAL** **NEW API** `Longtail_SetMonitor` to enable more detailed feedback on `Longtail_ChangeVersion2` and `Longtail_WriteContent`. Includes simple progress UI using MiniFB and console output for upsync/downsync/pack/unpack via `--detailed-progress` option. MiniFB version only works on Windows so far, other platforms fall back to console text output.

## 0.4.1
- **NEW API** `Longtail_ChangeVersion2` added

  Implements a new strategy for decompressing/writing version assets which is significantly faster for files that spans multiple blocks while retaining the same speed for assets smaller than a block. It removes redundant decompression of blocks (so the LRU block store is no longer needed) at the expense of doing random access when writing files.
  
  It uses significantly **less** memory and **less** CPU while avoiding redundant reads from source data so it is an overall win*.
  If your main target devices are SSD drives I **highly** recommend switching to `Longtail_ChangeVersion2`.
  
  | Version | Files | Raw Size | Compressed Size | Unpack Time | Peak Memory |
  |-|-|-|-|-|-|
  |0.4.0|1024|736 GB|178 GB|2h47m39s|9.5 GB|
  |0.4.1|1024|736 GB|178 GB|0h12m02s|3.5 GB|
  |0.4.0|239 340|60 GB|17 GB|0h03m34s|1.3 GB|
  |0.4.1|239 340|60 GB|17 GB|0h01m37s|0.4 GB|

  *This strategy is well suited for target storage mediums with fast random access time (such as SDD drives) but will suffer when seek times are higher (such as mechanical drives).
  `Longtail_ChangeVersion` is still available if you need to cater for mediums with slower seek times.

- **NEW API** `Longtail_ConcurrentChunkWriteAPI` added
- **NEW API** `Longtail_CreateConcurrentChunkWriteAPI` added
- **NEW API** Added `Longtail_MemTracer_GetAllocationCount` to check the number of current memory allocations
- **NEW API** Added platform api for Read/Write mutex
  - `Longtail_GetRWLockSize`
  - `Longtail_CreateRWLock`
  - `Longtail_DeleteRWLock`
  - `Longtail_LockRWLockRead`
  - `Longtail_LockRWLockWrite`
  - `Longtail_UnlockRWLockRead`
  - `Longtail_UnlockRWLockWrite`
- **ADDED** `SAFE_DISPOSE_STORED_BLOCK` macro for easy dispose of stored blocks
- **ADDED** Check at test run exit that no memory allocations are left
- **FIXED** Updated premake5.lua to set the correct defines for debug vs release builds
- **FIXED** Use non-binary units for memtracer counts
- **FIXED** Added workaround for calling hmgeti_ts on an empty hash map as that is not thread safe
- **FIXED** Use fixed v3 of `actions/upload-artifact`/`actions/download-artifact`
- **FIXED** Propagate error from `FSStorageAPI_Write` to caller
- **FIXED** Make sure `EnsureParentPathExists` can create a folder structure and not just parent path
- **FIXED** Fixed potential `uint32` arithmetic overflow issues
- **CHANGED** Command tool uses `Longtail_ChangeVersion2` instead of `Longtail_ChangeVersion`
- **CHANGED** Command tool no longer uses LRU block store layer (obsolete with `Longtail_ChangeVersion2`)

## 0.4.0
- **NEW** added builds for Arm64 flavours of all components
  - `Longtail_CreateMeowHashAPI()` is not supported on Arm64 and will return 0 if called
  - `Longtail_CreateBlake2HashAPI()` is not supported on Arm64 and will return 0 if called
  - `darwin-arm64.zip` artifact is produced when creating a release
- **CHANGED** `Longtail_HashRegistryAPI::GetHashAPI` may now return `ENOTSUP` error code for hash types that is not supported on the target platform
- **NEW API** `Longtail_SplitStoreIndex` added
- **CHANGED** Make `Longtail_CopyStoreIndex` store_index arg const
- **FIXED** Fixed `Longtail_StoreIndex m_BlockChunksOffsets` documentation
- **FIXED** `Longtail_CreateDirectory` no longer ends up in an infinite loop when trying to create a folder when path is a root folder. [timsjostrand](https://github.com/timsjostrand)

## 0.3.8
- **CHANGED** Paths in a version index is now stored with case sensitivity to avoid confusion when a file is renamed by changing casing only
- **FIX** Changing case of a file name without changing name no longer causes EACCESS error form version index adding the same file twice with same casing
- **UPDATED** Update of ZStd: 1.5.4 https://github.com/facebook/zstd/releases/tag/v1.5.4
- **UPDATED** Update of LZ4: 1.9.4 https://github.com/lz4/lz4/releases/tag/v1.9.4
- **UPDATED** Update of Blake3: 1.3.3 https://github.com/BLAKE3-team/BLAKE3/releases/tag/1.3.3

## 0.3.7
- **NEW API** `Longtail_MergeVersionIndex` to merge a base and an overlay version index
- **CHANGED** Additional error/diagnostic logging when initialising store and version indexes from a buffer
- **FIX** Allocated proper size when adding long file path prefix Issue #211
- **FIX** Don't use potentally stale store index in `FSBlockStoreAPI` `PruneBlocks`

## 0.3.6
- **CHANGED API** `Longtail_Job_CreateJobsFunc` now takes a `channel` parameter, can be either 0 or 1, 0 has higher priority than 1
- **FIXED** Writing content to disk now has higher priority than reading blocks from store
- **CHANGED** Reworked logic for calculating the number of blocks to read per write operation when writing multi-block assets

## 0.3.5
- **CHANGED** Function entry logging change to DEBUG
- **CHANGED** Added info-debugging of read/written files (with size)
- **CHANGED** Add log context for Longtail_WriteStoredBlock and Longtail_WriteStoredBlock in non-debug build

## v0.3.4
- **FIX** Promoted `Longtail_GetVersionIndexSize` and `Longtail_BuildVersionIndex` to public API
- **FIX** Renamed private/test function with LongtailPrivate prefix
- **FIX** Removed `Longtail_FileInfos_GetPaths` and `struct Longtail_Paths`
- **FIX** Properly calculate number of jobs for WriteAssets (fixes weird progress behaviour on ChangeVersion/WriteVersion)
- **FIX** Increase PendingJobCount in bikeshed before successful create so we don't fall out of job loop while waiting for space
- **FIX** Auto detect of pre-release flag from tag name
- **CHANGE** Release notes are read from CHANGELOG in root

## v0.3.3
- **FIX** Copy block extension string in FSBlockStore_Init

## v0.3.2
- **FIX** Correctly check return values of CreateThread and CreateFileMapping in Win32

## v0.3.1
- **CHANGED API** `Longtail_StorageAPI.CreateDirectory` is now expected to create the full path hierarchy
- **CHANGED API** `Longtail_CreateDefaultCompressionRegistry` now takes callback functions to create compression apis, this makes it possible to add new compression settings for a compression type without breaking older versions
- **NEW API** `Longtail_GetZStdHighQuality`, `Longtail_GetZStdLowQuality` added
- **NEW API** `Longtail_CompressionRegistry_CreateForBrotli`, `Longtail_CompressionRegistry_CreateForLZ4`, `Longtail_CompressionRegistry_CreateForZstd`
- **FIX** Add retry when opening files and creating directories on Win32 platform due to network drives sometimes being slow in picking up created directories
- **FIX** Add path in log context in `FSStorageAPI_LockFile`
- **FIX** Add input parameter validation in `FSStorageAPI_LockFile`, `FSStorageAPI_UnlockFile`, `InMemStorageAPI_LockFile` and `InMemStorageAPI_UnlockFile`
- **FIX** Restructured build scripts and GitHub workflows to allow more parallel builds
- **FIX** Use correct path for lock file in fsblockstore on Mac and Linux
