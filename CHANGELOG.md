##
- **NEW** added builds for Arm64 flavours of all components
  - `Longtail_CreateMeowHashAPI()` is not supported on Arm64 and will return 0 if called
  - `Longtail_CreateBlake2HashAPI()` is not supported on Arm64 and will return 0 if called
  - `darwin-arm64.zip` artifact is produced when creating a release
- **CHANGED** `Longtail_HashRegistryAPI::GetHashAPI` may now return `ENOTSUP` error code for hash types that is not supported on the target platform
- **NEW API** `Longtail_SplitStoreIndex` added
- **CHANGED** Make `Longtail_CopyStoreIndex` store_index arg const
- **FIXED** Fixed `Longtail_StoreIndex m_BlockChunksOffsets` documentation
- **FIXED** `Longtail_CreateDirectory` no longer ends up in an infinite loop when trying to create a folder when path is a root folder

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
