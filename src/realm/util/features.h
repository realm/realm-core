/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 *************************************************************************/
#ifndef REALM_UTIL_FEATURES_H
#define REALM_UTIL_FEATURES_H

/* See these links for information about feature check macros in GCC,
 * Clang, and MSVC:
 *
 * http://gcc.gnu.org/projects/cxx0x.html
 * http://clang.llvm.org/cxx_status.html
 * http://clang.llvm.org/docs/LanguageExtensions.html#checks-for-standard-language-features
 * http://msdn.microsoft.com/en-us/library/vstudio/hh567368.aspx
 * http://sourceforge.net/p/predef/wiki/Compilers
 */

// Compiler is GNU GCC.
#if defined(__GNUC__) && !defined(__clang__)
#  define REALM_COMPILER_GCC 1
#else
#  define REALM_COMPILER_GCC 0
#endif

// Compiler is Apple's clang (Xcode)
#if defined(__clang__) && defined(__apple_build_version__)
#  define REALM_COMPILER_APPLE_CLANG 1
#else
#  define REALM_COMPILER_APPLE_CLANG 0
#endif

// Compiler is LLVM clang
#if defined(__clang__) && !defined(__apple_build_version__)
#  define REALM_COMPILER_LLVM_CLANG 1
#else
#  define REALM_COMPILER_LLVM_CLANG 0
#endif

// Compiler is any variety of clang
#if defined(__clang__)
#  define REALM_COMPILER_CLANG 1
#else
#  define REALM_COMPILER_CLANG 0
#endif

// GCC-compatible compiler
#if REALM_COMPILER_GCC || REALM_COMPILER_CLANG
#  define REALM_COMPILER_GCC_COMPATIBLE 1
#else
#  define REALM_COMPILER_GCC_COMPATIBLE 0
#endif

// Compiler is MS VC++
#if defined(_MSC_VER)
#  define REALM_COMPILER_MSVC 1
#else
#  define REALM_COMPILER_MSVC 0
#endif

// Compiler is GCC and version is greater than or equal to the specified version
#define REALM_HAVE_AT_LEAST_GCC(maj, min) \
    (REALM_COMPILER_GCC && \
        (__GNUC__ > (maj) || __GNUC__ == (maj) && __GNUC_MINOR__ >= (min)))

// Compiler is Apple's clang (Xcode) and version is greater than or equal to the specified version
#define REALM_HAVE_AT_LEAST_APPLE_CLANG(maj, min) \
    (REALM_COMPILER_APPLE_CLANG && \
        (__clang_major__ > (maj) || __clang_major__ == (maj) && __clang_minor__ >= (min)))

// Compiler is LLVM clang and version is greater than or equal to the specified version
#define REALM_HAVE_AT_LEAST_LLVM_CLANG(maj, min) \
    (REALM_COMPILER_LLVM_CLANG && \
        (__clang_major__ > (maj) || __clang_major__ == (maj) && __clang_minor__ >= (min)))

// Compiler is MSVC and version is greater than or equal to the specified version
#define REALM_HAVE_AT_LEAST_MSVC(maj, min) \
    (REALM_COMPILER_MSVC && \
        (_MSC_VER >= ((maj * 100 + 600) + (min * 10))))

#define REALM_HAVE_AT_LEAST_MSVC_10_SP1 \
    (REALM_HAVE_AT_LEAST_MSVC(11, 0) || \
     (REALM_COMPILER_MSVC && _MSC_FULL_VER >= 160040219))

// Clang feature detection
#if REALM_COMPILER_CLANG
#  define REALM_HAVE_CLANG_FEATURE(feature) __has_feature(feature)
#else
#  define REALM_HAVE_CLANG_FEATURE(feature) 0
#endif

#define REALM_HAVE_AT_LEAST_LIBCPP(version) \
    (defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= version)

// Platform is Windows
#if defined(_WIN32) || defined(_WIN64)
#  define REALM_PLATFORM_WINDOWS 1
#else
#  define REALM_PLATFORM_WINDOWS 0
#endif

// Platform is Android
#if defined(ANDROID) || defined(__ANDROID__)
#  define REALM_PLATFORM_ANDROID 1
#else
#  define REALM_PLATFORM_ANDROID 0
#endif

// Platform is Linux
#if defined(__linux__) && !REALM_PLATFORM_ANDROID
#  define REALM_PLATFORM_LINUX 1
#else
#  define REALM_PLATFORM_LINUX 0
#endif

// Platform is Apple-made
#if defined(__APPLE__) && defined(__MACH__)
#  define REALM_PLATFORM_APPLE 1

#  include <TargetConditionals.h>

#  if TARGET_OS_IPHONE == 1
#    define REALM_PLATFORM_APPLE_IOS 1
#  else
#    define REALM_PLATFORM_APPLE_IOS 0
#  endif

#  if TARGET_OS_TV == 1
#    define REALM_PLATFORM_APPLE_TVOS 1
#  else
#    define REALM_PLATFORM_APPLE_TVOS 0
#  endif

#  if TARGET_OS_WATCH == 1
#    define REALM_PLATFORM_APPLE_WATCHOS 1
#  else
#    define REALM_PLATFORM_APPLE_WATCHOS 0
#  endif

#  if !REALM_PLATFORM_APPLE_IOS && !REALM_PLATFORM_APPLE_TVOS && !REALM_PLATFORM_APPLE_WATCHOS
#    define REALM_PLATFORM_APPLE_MACOS 1
#  else
#    define REALM_PLATFORM_APPLE_MACOS 0
#  endif
#else
#  define REALM_PLATFORM_APPLE         0
#  define REALM_PLATFORM_APPLE_IOS     0
#  define REALM_PLATFORM_APPLE_TVOS    0
#  define REALM_PLATFORM_APPLE_WATCHOS 0
#  define REALM_PLATFORM_APPLE_MACOS   0
#endif

// We are on some kind of ARM
#if defined(__arm__) || defined(_M_ARM)
#  define REALM_ARCHITECTURE_ARM 1
#else
#  define REALM_ARCHITECTURE_ARM 0
#endif

// We are on an ARMv6
#if REALM_ARCHITECTURE_ARM && (defined(__ARM_ARCH_6__) || _M_ARM == 6)
#  define REALM_ARCHITECTURE_ARMV6 1
#else
#  define REALM_ARCHITECTURE_ARMV6 0
#endif

// We are on an ARMv7
#if REALM_ARCHITECTURE_ARM && (defined(__ARM_ARCH_7__) || _M_ARM == 7)
#  define REALM_ARCHITECTURE_ARMV7 1
#else
#  define REALM_ARCHITECTURE_ARMV7 0
#endif

// We are on an ARM64 CPU
#if defined(__aarch64__) || defined(__arm64) || defined(__arm64__) \
    || (REALM_ARCHITECTURE_ARM && defined(__LP64__))
#  define REALM_ARCHITECTURE_ARM64 1
#else
#  define REALM_ARCHITECTURE_ARM64 0
#endif

// We're in i686 mode
#if defined(__i386) || defined(__i386__) || defined(__i686__) || defined(_M_I86) || defined(_M_IX86)
#  define REALM_ARCHITECTURE_X86 1
#else
#  define REALM_ARCHITECTURE_X86 0
#endif

// We're in amd64 mode
#if defined(__amd64) || defined(__amd64__) || defined(__x86_64) || defined(__x86_64__) \
    || defined(_M_X64) || defined(_M_AMD64)
#  define REALM_ARCHITECTURE_AMD64 1
#else
#  define REALM_ARCHITECTURE_AMD64 0
#endif

// Convenience macro for the two above
#if REALM_ARCHITECTURE_X86 || REALM_ARCHITECTURE_AMD64
#  define REALM_ARCHITECTURE_X86_OR_AMD64 1
#else
#  define REALM_ARCHITECTURE_X86_OR_AMD64 0
#endif

// The current platform supports 64-bit pointers
#if REALM_ARCHITECTURE_AMD64 || REALM_ARCHITECTURE_ARM64 || defined(_LP64) || defined(__LP64__) \
    || defined(__64BIT__) || _ADDR64 || defined(_WIN64) || defined(__arch64__) || __WORDSIZE == 64
#  define REALM_PTR_64 1
#else
#  define REALM_PTR_64 0
#endif

// Compiler supports SSE 4.2 through __builtin_ accessors or back-end assembler
#if REALM_PTR_64 && (REALM_ARCHITECTURE_X86 || REALM_ARCHITECTURE_AMD64)
#  define REALM_COMPILER_SSE 1
#  define REALM_COMPILER_AVX 1
#else
#  define REALM_COMPILER_SSE 0
#  define REALM_COMPILER_AVX 0
#endif

// Only specific versions of clang and gcc support retrieving ISA extensions
// through `__builtin_cpu_supports()`. Apple's clang and MSVC do not currently
// support it.
#if REALM_HAVE_AT_LEAST_LLVM_CLANG(3, 7) || REALM_HAVE_AT_LEAST_GCC(4, 8)
#  define REALM_COMPILER_HAS_ISA_INTRINSICS 1
#else
#  define REALM_COMPILER_HAS_ISA_INTRINSICS 0
#endif

// Disable some warnings on MSVC
#if REALM_COMPILER_MSVC
#  pragma warning(disable:4800) // Visual Studio int->bool performance warnings
#endif

// Include the build configuration, alternatively define some defaults
#ifdef REALM_HAVE_CONFIG
#  include <realm/util/config.h>
#else
#  define REALM_VERSION "unknown"
#  if !REALM_PLATFORM_WINDOWS
#    define REALM_INSTALL_PREFIX      "/usr/local"
#    define REALM_INSTALL_EXEC_PREFIX REALM_INSTALL_PREFIX
#    define REALM_INSTALL_INCLUDEDIR  REALM_INSTALL_PREFIX "/include"
#    define REALM_INSTALL_BINDIR      REALM_INSTALL_EXEC_PREFIX "/bin"
#    define REALM_INSTALL_LIBDIR      REALM_INSTALL_EXEC_PREFIX "/lib"
#    define REALM_INSTALL_LIBEXECDIR  REALM_INSTALL_EXEC_PREFIX "/libexec"
#  endif
#endif

// The maximum number of elements in a B+-tree node. Applies to inner nodes and
// to leaves. The minimum allowable value is 2.
#ifndef REALM_MAX_BPNODE_SIZE
#  define REALM_MAX_BPNODE_SIZE 1000
#endif

/* The way to specify that a function never returns.
 *
 * NOTE: C++11 generalized attributes are not yet fully supported in
 * MSVC++ 12 (2013). */
#if REALM_HAVE_AT_LEAST_GCC(4, 8) || REALM_HAVE_CLANG_FEATURE(cxx_attributes)
#  define REALM_NORETURN [[noreturn]]
#elif REALM_COMPILER_GCC_COMPATIBLE
#  define REALM_NORETURN __attribute__((noreturn))
#elif REALM_COMPILER_MSVC
#  define REALM_NORETURN __declspec(noreturn)
#else
#  define REALM_NORETURN
#endif

// Define some keywords to make cross-compiler development easier
#if REALM_COMPILER_GCC_COMPATIBLE
  // The way to specify that a variable or type is intended to possibly
  // not be used. Use it to suppress a warning from the compiler. */
#  define REALM_UNUSED          __attribute__((unused))
#  define REALM_UNLIKELY(expr)  __builtin_expect(!!(expr), 0)
#  define REALM_LIKELY(expr)    __builtin_expect(!!(expr), 1)
#  define REALM_FORCEINLINE     inline __attribute__((always_inline))
#  define REALM_NOINLINE        __attribute__((noinline))
#elif REALM_COMPILER_MSVC
#  define REALM_FORCEINLINE     __forceinline
#  define REALM_NOINLINE        __declspec(noinline)
#else
#  define REALM_UNUSED
#  define REALM_UNLIKELY(expr) (expr)
#  define REALM_LIKELY(expr)   (expr)
#  define REALM_FORCEINLINE    inline
#  define REALM_NOINLINE
#endif

/* Thread specific data (only for POD types) */
#if REALM_COMPILER_CLANG
#  define REALM_THREAD_LOCAL __thread
#else
#  define REALM_THREAD_LOCAL thread_local
#endif

// The necessary signal handling / mach exception APIs are not available
#if REALM_PLATFORM_APPLE_TVOS || REALM_PLATFORM_APPLE_WATCHOS
#  undef REALM_ENABLE_ENCRYPTION
#endif

#if REALM_PLATFORM_ANDROID || REALM_PLATFORM_APPLE_IOS || REALM_PLATFORM_APPLE_WATCHOS
#  define REALM_MOBILE 1
#else
#  define REALM_MOBILE 0
#endif

// async deamon does not start when launching unit tests from osx, so async is currently disabled on osx.
#if REALM_PLATFORM_LINUX
#  define REALM_ASYNC_DAEMON 1
#else
#  define REALM_ASYNC_DAEMON 0
#endif

#if defined(REALM_DEBUG)
#  define REALM_COOKIE_CHECK 1
#else
#  define REALM_COOKIE_CHECK 0
#endif

#endif /* REALM_UTIL_FEATURES_H */
