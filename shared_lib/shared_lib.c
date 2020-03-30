#if defined(_WIN32)
#define LONGTAIL_EXPORT __declspec(dllexport)
#endif

#include "../src/longtail.h"
#include "../lib/bikeshed/longtail_bikeshed.h"
#include "../lib/blake2/longtail_blake2.h"
#include "../lib/blake3/longtail_blake3.h"
#include "../lib/brotli/longtail_brotli.h"
#include "../lib/cacheblockstore/longtail_cacheblockstore.h"
#include "../lib/compressblockstore/longtail_compressblockstore.h"
#include "../lib/fsblockstore/longtail_fsblockstore.h"
#include "../lib/filestorage/longtail_filestorage.h"
#include "../lib/lizard/longtail_lizard.h"
#include "../lib/lz4/longtail_lz4.h"
#include "../lib/memstorage/longtail_memstorage.h"
#include "../lib/meowhash/longtail_meowhash.h"
#include "../lib/zstd/longtail_zstd.h"
#include "../lib/compressionregistry/longtail_full_compression_registry.h"

#if defined(_WIN32)

#include <Windows.h>

BOOL WINAPI DllMain(
    HINSTANCE hinstDLL,
    DWORD     fdwReason,
    LPVOID    lpvReserved)
{
    return TRUE;
}

#endif
