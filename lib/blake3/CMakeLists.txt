if(CMAKE_ANDROID_ARM_NEON)
	set ($BLAKE2_platform_sources "ext/blake3_neon.c")
elseif (MSVC OR MSYS OR MINGW)
	set ($BLAKE2_platform_sources "ext/blake3_avx2_x86-64_windows_msvc.asm" "ext/blake3_avx512_x86-64_windows_msvc.asm" "ext/blake3_sse2_x86-64_windows_msvc.asm" "ext/blake3_sse41_x86-64_windows_msvc.asm")
elseif (WIN32)
	set ($BLAKE2_platform_sources "ext/blake3_avx2_x86-64_windows_gnu.S" "ext/blake3_avx512_x86-64_windows_gnu.S" "ext/blake3_sse2_x86-64_windows_gnu.S" "ext/blake3_sse41_x86-64_windows_gnu.S")
else()
	set ($BLAKE2_platform_sources "ext/blake3_avx2_x86-64_unix.S" "ext/blake3_avx512_x86-64_unix.S" "ext/blake3_sse2_x86-64_unix.S" "ext/blake3_sse41_x86-64_unix.S")
endif()

add_library(
	blake3 OBJECT
	"longtail_blake3.c"
	"longtail_blake3.h"

	"ext/blake3.c"
	"ext/blake3.h"
	"ext/blake3_dispatch.c"
	"ext/blake3_impl.h"
	"ext/blake3_portable.c"
	${BLAKE2_platform_sources}
)

add_library(
	blake3_sse2 OBJECT
	"ext/blake3_sse2.c"
)

add_library(
	blake3_sse4_1 OBJECT
	"ext/blake3_sse41.c"
)

add_library(
	blake3_avx2 OBJECT
	"ext/blake3_avx2.c"
)

add_library(
	blake3_avx512 OBJECT
	"ext/blake3_avx512.c"
)

if(MSVC)
	target_compile_options(blake3_sse2 INTERFACE /arch:SSE2)
	target_compile_options(blake3_sse4_1 INTERFACE /arch:SSE4.1)
	target_compile_options(blake3_avx2 INTERFACE /arch:AVX2)
	target_compile_options(blake3_avx512 INTERFACE /arch:AVX512)
else()
	target_compile_options(blake3_sse2 PUBLIC -msse2)
	target_compile_options(blake3_sse4_1 PUBLIC -msse4.1)
	target_compile_options(blake3_avx2 PUBLIC -mavx2)
	target_compile_options(blake3_avx512 PUBLIC -mavx512vl -mavx512f)
endif(MSVC)
