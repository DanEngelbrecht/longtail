newoption
{
    trigger = 'to',
    default = 'build',
    value   = 'path',
    description = 'Set the output location for the generated files'
}

workspace "longtail"
   configurations { "Debug", "Release" }
   platforms { 'x86_64' }   
   location ( _OPTIONS["to"] )
   intrinsics "on"
   vectorextensions "AVX2"
   runtime "Release"

   configuration 'vs*'
      defines     { '_CRT_SECURE_NO_DEPRECATE', '_CRT_SECURE_NO_WARNINGS', '_CRT_NONSTDC_NO_WARNINGS' }   

project "longtail-lib"
   kind "StaticLib"

   files { 'lib/**.c', 'lib/**.h', 'src/**.c', 'src/**.h' }

   filter "configurations:Debug"
      defines { "DEBUG" }
      symbols "On"

   filter "configurations:Release"
      defines { "NDEBUG" }
      optimize "On"
      
project "longtail-cli"
    kind "ConsoleApp"
    files { 'cmd/**.c', 'cmd/**.h' }
    links { "longtail-lib" }

project "longtail-tests"
    kind "ConsoleApp"
    files { 'test/**.cpp', 'test/**.h' }    
    links { "longtail-lib" }