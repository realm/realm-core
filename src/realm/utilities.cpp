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

#include <realm/utilities.hpp>

#include <realm/unicode.hpp>

#include <atomic>
#include <cstdint>
#include <cstdlib> // size_t
#include <fstream>
#include <string>

#ifdef _WIN32
#include "psapi.h"
#include "windows.h"
#include <chrono>
#include <intrin.h>
#include <thread>
#else
#include <unistd.h>
#endif

namespace realm {

#if defined(_MSC_VER)
int fast_popcount32(int32_t x)
{
    return __popcnt(x);
}
int fast_popcount64(int64_t x)
{
    return int(__popcnt64(x));
}
#else // _MSC_VER
int fast_popcount32(int32_t x)
{
    return __builtin_popcount(x);
}

int fast_popcount64(int64_t x)
{
    return __builtin_popcountll(x);
}
#endif


// A fast, thread safe, mediocre-quality random number generator named Xorshift
uint64_t fastrand(uint64_t max, bool is_seed)
{
    // All the atomics (except the add) may be eliminated completely by the compiler on x64
    static std::atomic<uint64_t> state(1);

    // Thread safe increment to prevent two threads from producing the same value if called at the exact same time
    state.fetch_add(1, std::memory_order_release);
    uint64_t x = is_seed ? max : state.load(std::memory_order_acquire);
    // The result of this arithmetic may be overwritten by another thread, but that's fine in a rand generator
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    state.store(x, std::memory_order_release);
    return (x * 2685821657736338717ULL) % (max + 1 == 0 ? 0xffffffffffffffffULL : max + 1);
}

uint64_t FastRand::operator()(uint64_t max)
{
    uint64_t x = m_state;
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    m_state = x;
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
#endif // REALM_SLAB_ALLOC_TUNE

#ifdef _WIN32
int gettimeofday(struct timeval* tp, struct timezone* tzp)
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
