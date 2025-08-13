If %PROCESSOR_ARCHITECTURE% == AMD64 (
    set ARCH=x64
    set OS=win32
) Else If %PROCESSOR_ARCHITECTURE% == ARM64 (
    set ARCH=arm64
    set OS=win
) Else (
    set ARCH=x86
    set OS=win32
)

set COMPILER=gcc

set PLATFORM=%OS%_%ARCH%