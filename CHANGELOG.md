##
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