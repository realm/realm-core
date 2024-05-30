///////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
///////////////////////////////////////////////////////////////////////////////

#ifndef _WIN32
#define __STDC_WANT_LIB_EXT1__ 1
#include <string.h>
#endif

#include "assert.hpp"
#include "sensitive_buffer.hpp"

#ifdef _WIN32
#include <Windows.h>
#if !REALM_UWP
#include <wincrypt.h>
#pragma comment(lib, "crypt32.lib")
#endif
#include <limits>
#include <mutex>
#else
#include <sys/mman.h>
#endif

using namespace realm::util;

#ifdef _WIN32

#if !REALM_UWP
/*
 * try to lock allocated buffer, or grow working set size if default quota was reached
 * make multiple attemps to handle multi-threaded allocation
 */
static void lock_or_grow_working_size(void* buffer, size_t size)
{
    BOOL res = 0;
    DWORD err = 0;
    for (int i = 0; i < 10; ++i) {
        res = VirtualLock(buffer, size);
        if (res != 0)
            return; // success

        // Try to grow the working set if we have hit our quota.
        err = GetLastError();
        REALM_ASSERT_RELEASE_EX(err == ERROR_WORKING_SET_QUOTA && "VirtualLock()", err);

        static std::mutex mutex;
        std::lock_guard<std::mutex> lock(mutex);

        struct WorkingSetLimits {
            SIZE_T min_size = 0, max_size = 0;
            const DWORDLONG mem_size = 0;
        };
        static std::optional<WorkingSetLimits> limits;
        if (!limits) {
            SIZE_T min_size = 0, max_size = 0;
            BOOL ret = GetProcessWorkingSetSize(GetCurrentProcess(), &min_size, &max_size);
            REALM_ASSERT_RELEASE_EX(ret != 0 && "GetProcessWorkingSetSize", GetLastError());
            MEMORYSTATUSEX mem;
            mem.dwLength = sizeof(mem);
            GlobalMemoryStatusEx(&mem);
            limits.emplace(WorkingSetLimits{min_size, max_size, mem.ullTotalPhys});
        }

        SIZE_T min_size = limits->min_size, max_size = limits->max_size;

        // Initial default is 50 pages (or 204,800 bytes on systems with a 4K page size)
        // for minimum working set and 345 for max (1,413,120 bytes on 4K page size)
        // and it's easy to hit the quota even with 10 to 20 concurrent threads.
        // Since we don't use VirtualAlloc with real page size and assign buffer to the portion
        // of the allocated page (this'd complicate the logic significantly),
        // simply try to double the limit a few times and attempt to lock the buffer again
        // in case when multiple threads do the same. The min limit itself is pretty small,
        // and the doc says that even this is not strictly guaranteed by the system, so
        // there should be no harm in overcommiting expected min working set size
        // In real tests: even with 128 threads on 16 cores system in 32bit app this loop
        // may go through 4-5 iterations until the lock succeeds if the increment
        // is just a multiple of a few pages, but with doubling of the limit
        // it should succeed on the first try.
        min_size *= 2;
        max_size = std::max(4 * min_size, max_size);

        // give up and don't try to ask the system to keep more than 90% memory resident in the process
        if (max_size > 0.9 * limits->mem_size)
            break;

        BOOL ret = SetProcessWorkingSetSize(GetCurrentProcess(), min_size, max_size);
        REALM_ASSERT_RELEASE_EX(ret != 0 && "SetProcessWorkingSetSizeEx", GetLastError());
        limits->min_size = min_size;
        limits->max_size = max_size;
    }

    // the loop above didn't succeed, provide some mem info and assert
    MEMORYSTATUSEX mem;
    mem.dwLength = sizeof(mem);
    GlobalMemoryStatusEx(&mem);
    REALM_ASSERT_RELEASE_EX(res != 0 && "VirtualLock()", err, mem.ullAvailPhys, mem.ullTotalPhys, mem.dwMemoryLoad,
                            mem.ullAvailPageFile, mem.ullAvailVirtual);
}
#endif // REALM_UWP

SensitiveBufferBase::SensitiveBufferBase(size_t size)
#if REALM_UWP
    : m_size(size)
#else
    : m_size(size_t(ceil((long double)size / CRYPTPROTECTMEMORY_BLOCK_SIZE) * CRYPTPROTECTMEMORY_BLOCK_SIZE))
#endif
{
    m_buffer = VirtualAlloc(nullptr, m_size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    REALM_ASSERT_RELEASE_EX(m_buffer != NULL && "VirtualAlloc()", GetLastError());

#if !REALM_UWP
    lock_or_grow_working_size(m_buffer, m_size);
#endif
}

SensitiveBufferBase::~SensitiveBufferBase()
{
    if (!m_buffer)
        return;

    secure_erase(m_buffer, m_size);

    BOOL ret = FALSE;

#if !REALM_UWP
    ret = VirtualUnlock(m_buffer, m_size);
    REALM_ASSERT_RELEASE_EX((ret != 0 || GetLastError() == ERROR_NOT_LOCKED) && "VirtualUnlock()", GetLastError());
#endif

    ret = VirtualFree(m_buffer, 0, MEM_RELEASE);
    REALM_ASSERT_RELEASE_EX(ret != 0 && "VirtualFree()", GetLastError());

    m_buffer = nullptr;
}

void SensitiveBufferBase::protect() const
{
#if !REALM_UWP
    // MEMO even though we try to lock the page with the buffer, and that should prevent it to be swapped,
    //      locking is not reliable and may fail under high demand in the system due to some opaque reason,
    //      which we can't recover from, so use second layer to protect the buffer if it is swapped
    //      note: look at attempt_to_lock where it's expected
    BOOL ret = CryptProtectMemory(m_buffer, DWORD(m_size), CRYPTPROTECTMEMORY_SAME_PROCESS);
    REALM_ASSERT_RELEASE_EX(ret == TRUE && "CryptProtectMemory()", GetLastError());
#endif
}

void SensitiveBufferBase::unprotect() const
{
#if !REALM_UWP
    BOOL ret = CryptUnprotectMemory(m_buffer, DWORD(m_size), CRYPTPROTECTMEMORY_SAME_PROCESS);
    REALM_ASSERT_RELEASE_EX(ret == TRUE && "CryptUnprotectMemory()", GetLastError());
#endif
}

#else
SensitiveBufferBase::SensitiveBufferBase(size_t size)
    : m_size(size)
{
    m_buffer = mmap(nullptr, m_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    REALM_ASSERT_RELEASE_EX(m_buffer != MAP_FAILED && "mmap()", errno);

    int ret;
    ret = mlock(m_buffer, m_size);
#if REALM_LINUX
    // mlock failing with ENOMEM under linux implies we're over RLIMIT_MEMLOCK,
    // which happens inside a container. ignore for now.
    int err = errno;
    REALM_ASSERT_RELEASE_EX((ret == 0 || err == ENOMEM) && "mlock()", err);
#else
    REALM_ASSERT_RELEASE_EX(ret == 0 && "mlock()", errno);
#endif


#if defined(MADV_DONTDUMP)
    // Linux kernel 3.4+ respects MADV_DONTDUMP, ignore return value on older
    madvise(m_buffer, m_size, MADV_DONTDUMP);
#endif
}

SensitiveBufferBase::~SensitiveBufferBase()
{
    if (!m_buffer)
        return;

    secure_erase(m_buffer, m_size);

#if defined(MADV_DONTDUMP) && defined(MADV_DODUMP)
    madvise(m_buffer, m_size, MADV_DODUMP);
#endif

    int ret;
    ret = munlock(m_buffer, m_size);
    REALM_ASSERT_RELEASE_EX(ret == 0 && "munlock()", errno);

    ret = munmap(m_buffer, m_size);
    REALM_ASSERT_RELEASE_EX(ret == 0 && "munmap()", errno);

    m_buffer = nullptr;
}

void SensitiveBufferBase::protect() const {}

void SensitiveBufferBase::unprotect() const {}
#endif

SensitiveBufferBase::SensitiveBufferBase(const SensitiveBufferBase& other)
    : SensitiveBufferBase(other.m_size)
{
    std::memcpy(m_buffer, other.m_buffer, m_size);
}

SensitiveBufferBase::SensitiveBufferBase(SensitiveBufferBase&& other) noexcept
    : m_size(other.m_size)
{
    std::swap(m_buffer, other.m_buffer);
}

void SensitiveBufferBase::secure_erase(void* buffer, size_t size)
{
#ifdef _WIN32
    SecureZeroMemory(buffer, size);
#elif defined(__STDC_LIB_EXT1__) || __APPLE__
    memset_s(buffer, size, 0, size);
#elif REALM_HAVE_EXPLICIT_BZERO
    // it's at least expected to be available on glibc >= 2.25 and Musl
    explicit_bzero(buffer, size);
#else
#error "Platforms lacks memset_s or explicit_bzero"
    (void)buffer;
    (void)size;
#endif
}
