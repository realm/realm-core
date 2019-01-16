/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdlib> // size_t
#include <string>
#include <cstdint>
#include <atomic>
#include <fstream>

#ifdef _WIN32
#include "windows.h"
#include "psapi.h"
#include <chrono>
#include <thread>
#else
#include <unistd.h>
#endif

#include <realm/utilities.hpp>
#include <realm/unicode.hpp>
#include <realm/util/thread.hpp>

#ifdef REALM_COMPILER_SSE
#ifdef _MSC_VER
#include <intrin.h>
#endif
#endif

namespace {

#ifdef REALM_COMPILER_SSE
#if !defined __clang__ && ((defined(_MSC_FULL_VER) && _MSC_FULL_VER >= 160040219) || defined __GNUC__)
#if defined REALM_COMPILER_AVX && defined __GNUC__
#define _XCR_XFEATURE_ENABLED_MASK 0

inline unsigned long long _xgetbv(unsigned index)
{
#if REALM_HAVE_AT_LEAST_GCC(4, 4)
    unsigned int eax, edx;
    __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
    return (static_cast<unsigned long long>(edx) << 32) | eax;
#else
    static_cast<void>(index);
    return 0;
#endif
}

#endif
#endif
#endif

} // anonymous namespace


namespace realm {

signed char sse_support = -1;
signed char avx_support = -1;

StringCompareCallback string_compare_callback = nullptr;
string_compare_method_t string_compare_method = STRING_COMPARE_CORE;

void cpuid_init()
{
#ifdef REALM_COMPILER_SSE
    int cret;
#ifdef _MSC_VER
    int CPUInfo[4];
    __cpuid(CPUInfo, 1);
    cret = CPUInfo[2];
#else
    int a = 1;
    __asm("mov %1, %%eax; " // a into eax
          "cpuid;"
          "mov %%ecx, %0;"                 // ecx into b
          : "=r"(cret)                     // output
          : "r"(a)                         // input
          : "%eax", "%ebx", "%ecx", "%edx" // clobbered register
          );
#endif

    // Byte is atomic. Race can/will occur but that's fine
    if (cret & 0x100000) { // test for 4.2
        sse_support = 1;
    }
    else if (cret & 0x1) { // Test for 3
        sse_support = 0;
    }
    else {
        sse_support = -2;
    }

    bool avxSupported = false;

// seems like in jenkins builds, __GNUC__ is defined for clang?! todo fixme
#if !defined __clang__ && ((defined(_MSC_FULL_VER) && _MSC_FULL_VER >= 160040219) || defined __GNUC__)
    bool osUsesXSAVE_XRSTORE = cret & (1 << 27) || false;
    bool cpuAVXSuport = cret & (1 << 28) || false;

    if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
        // Check if the OS will save the YMM registers
        unsigned long long xcrFeatureMask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
        avxSupported = (xcrFeatureMask & 0x6) || false;
    }
#endif

    if (avxSupported) {
        avx_support = 0; // AVX1 supported
    }
    else {
        avx_support = -1; // No AVX supported
    }

// 1 is reserved for AVX2

#endif
}


// FIXME: Move all these rounding functions to the header file to
// allow inlining.
void* round_up(void* p, size_t align)
{
    size_t r = size_t(p) % align == 0 ? 0 : align - size_t(p) % align;
    return static_cast<char*>(p) + r;
}

void* round_down(void* p, size_t align)
{
    size_t r = size_t(p);
    return reinterpret_cast<void*>(r & ~(align - 1));
}

size_t round_up(size_t p, size_t align)
{
    size_t r = p % align == 0 ? 0 : align - p % align;
    return p + r;
}

size_t round_down(size_t p, size_t align)
{
    size_t r = p;
    return r & (~(align - 1));
}

} // namespace realm


// popcount, counts number of set (1) bits in argument. Intrinsics has been disabled because it's just 10-20% faster
// than fallback method, so a runtime-detection of support would be more expensive in total. Popcount is supported
// with SSE42 but not with SSE3, and we don't want separate builds for each architecture - hence a runtime check would
// be required.
#if 0 // defined(_MSC_VER) && _MSC_VER >= 1500
#include <intrin.h>

namespace realm {

int fast_popcount32(int32_t x)
{
    return __popcnt(x);
}
#if defined(_M_X64)
int fast_popcount64(int64_t x)
{
    return int(__popcnt64(x));
}
#else
int fast_popcount64(int64_t x)
{
    return __popcnt(unsigned(x)) + __popcnt(unsigned(x >> 32));
}
#endif

} // namespace realm

#elif 0 // defined(__GNUC__) && __GNUC__ >= 4 || defined(__INTEL_COMPILER) && __INTEL_COMPILER >= 900
#define fast_popcount32 __builtin_popcount

namespace realm {

#if ULONG_MAX == 0xffffffff
int fast_popcount64(int64_t x)
{
    return __builtin_popcount(unsigned(x)) + __builtin_popcount(unsigned(x >> 32));
}
#else
int fast_popcount64(int64_t x)
{
    return __builtin_popcountll(x);
}
#endif

} // namespace realm

#else

namespace {

const char a_popcount_bits[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2,
    3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3,
    3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
    6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
    3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4,
    5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
    6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

} // anonymous namespace

namespace realm {

// Masking away bits might be faster than bit shifting (which can be slow). Note that the compiler may optimize this
// automatically. Todo, investigate.
int fast_popcount32(int32_t x)
{
    return a_popcount_bits[255 & x] + a_popcount_bits[255 & x >> 8] + a_popcount_bits[255 & x >> 16] +
           a_popcount_bits[255 & x >> 24];
}
int fast_popcount64(int64_t x)
{
    return fast_popcount32(static_cast<int32_t>(x)) + fast_popcount32(static_cast<int32_t>(x >> 32));
}

// Mutex only to make Helgrind happy
// If it was declared as a static inside fastrand() then it could have a race being initialised and
// being locked because a static inside a function is constructed the first time execution
// hits the function declaration (lazily). To avoid that race we declare it outside the function
// which means it will be constructed on program start. Post C++11, there should be no race on a
// construction of a local static, but tsan seems to report this as a false positive sometimes.
namespace {
util::Mutex fastrand_mutex;
} // end anonymous namespace

// A fast, thread safe, mediocre-quality random number generator named Xorshift
uint64_t fastrand(uint64_t max, bool is_seed)
{
    util::LockGuard lg(fastrand_mutex);

    // All the atomics (except the add) may be eliminated completely by the compiler on x64
    static std::atomic<uint64_t> state(is_seed ? max : 1);

    // Thread safe increment to prevent two threads from producing the same value if called at the exact same time
    state.fetch_add(1, std::memory_order_release);
    uint64_t x = state.load(std::memory_order_acquire);
    // The result of this arithmetic may be overwritten by another thread, but that's fine in a rand generator
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    state.store(x, std::memory_order_release);
    return (x * 2685821657736338717ULL) % (max + 1 == 0 ? 0xffffffffffffffffULL : max + 1);
}


void millisleep(unsigned long milliseconds)
{
#ifdef _WIN32
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
#else
    // sleep() takes seconds and usleep() is deprecated, so use nanosleep()
    timespec ts;
    size_t secs = milliseconds / 1000;
    milliseconds = milliseconds % 1000;
    ts.tv_sec = secs;
    ts.tv_nsec = milliseconds * 1000 * 1000;
    nanosleep(&ts, 0);
#endif
}

#ifdef REALM_SLAB_ALLOC_TUNE
void process_mem_usage(double& vm_usage, double& resident_set)
{
    vm_usage = 0.0;
    resident_set = 0.0;
#ifdef _WIN32
    HANDLE hProc = GetCurrentProcess();
    PROCESS_MEMORY_COUNTERS_EX info;
    info.cb = sizeof(info);
    BOOL okay = GetProcessMemoryInfo(hProc, (PROCESS_MEMORY_COUNTERS*)&info, info.cb);

    SIZE_T PrivateUsage = info.PrivateUsage;
    resident_set = PrivateUsage;
#else
    // the two fields we want
    unsigned long vsize;
    long rss;
    {
        std::string ignore;
        std::ifstream ifs("/proc/self/stat", std::ios_base::in);
        ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >>
            ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >>
            ignore >> ignore >> vsize >> rss;
    }

    long page_size_kb = sysconf(_SC_PAGE_SIZE); // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
#endif
}
#endif

#ifdef _WIN32
int gettimeofday(struct timeval * tp, struct timezone * tzp)
{
    FILETIME file_time;
    SYSTEMTIME system_time;
    ULARGE_INTEGER ularge;

    GetSystemTime(&system_time);
    SystemTimeToFileTime(&system_time, &file_time);
    ularge.LowPart = file_time.dwLowDateTime;
    ularge.HighPart = file_time.dwHighDateTime;
    const uint64_t epoch = 116444736000000000;
    tp->tv_sec = (long)((ularge.QuadPart - epoch) / 10000000L);
    tp->tv_usec = (long)(system_time.wMilliseconds * 1000);
    return 0;
}
#endif

int64_t platform_timegm(tm time)
{
#ifdef _WIN32
    // limitation of _mktime64 on windows is January 1, 1970, UTC to 23:59:59, December 31, 3000, UTC
    return static_cast<int64_t>(_mkgmtime64(&time));
#elif REALM_ANDROID
    // Android-9 as well as others don't have timegm support
    time_t t = mktime(&time);
    return int64_t(static_cast<int32_t>(t + localtime(&t)->tm_gmtoff));
#else
    // limitation of a 32 bit timegm (UTC) is December 13, 1901 @ 12:45:53 to January 19 2038 @ 03:14:07
    time_t unix_time = timegm(&time);
    return int64_t(static_cast<int32_t>(unix_time)); // unix_time comes as a int32_t
#endif
}


} // namespace realm

#endif // select best popcount implementations
