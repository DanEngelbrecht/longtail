If %PROCESSOR_ARCHITECTURE% == AMD64 (
    set ARCH=x64
) Else (
    set ARCH=x86
)

set COMPILER=gcc
set OS=win32

set PLATFORM=%OS%_%ARCH%
