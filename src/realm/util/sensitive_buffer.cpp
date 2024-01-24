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

#ifdef _WIN32
#include <Windows.h>
#include <wincrypt.h>
#pragma comment(lib, "crypt32.lib")
#else
#define __STDC_WANT_LIB_EXT1__ 1

#include <string.h>
#include <sys/mman.h>
#endif

#include "sensitive_buffer.hpp"
#include "assert.hpp"

using namespace realm::util;

#ifdef _WIN32
SensitiveBufferBase::SensitiveBufferBase(size_t size)
    : m_size(size_t(ceil((long double)size / CRYPTPROTECTMEMORY_BLOCK_SIZE) * CRYPTPROTECTMEMORY_BLOCK_SIZE))
{
    m_buffer = VirtualAlloc(nullptr, m_size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    REALM_ASSERT_RELEASE_EX(m_buffer != NULL && "VirtualAlloc()", GetLastError());

    BOOL ret = VirtualLock(m_buffer, m_size);
    REALM_ASSERT_RELEASE_EX(ret != 0 && "VirtualLock()", GetLastError());
}

SensitiveBufferBase::~SensitiveBufferBase()
{
    if (!m_buffer)
        return;

    SecureZeroMemory(m_buffer, m_size);

    BOOL ret = VirtualUnlock(m_buffer, m_size);
    REALM_ASSERT_RELEASE_EX(ret == TRUE && "VirtualUnlock()", GetLastError());

    ret = VirtualFree(m_buffer, 0, MEM_RELEASE);
    REALM_ASSERT_RELEASE_EX(ret == TRUE && "VirtualFree()", GetLastError());

    m_buffer = nullptr;
}

void SensitiveBufferBase::protect() const
{
    BOOL ret = CryptProtectMemory(m_buffer, DWORD(m_size), CRYPTPROTECTMEMORY_SAME_PROCESS);
    REALM_ASSERT_RELEASE_EX(ret != 0 && "CryptProtectMemory()", GetLastError());
}

void SensitiveBufferBase::unprotect() const
{
    BOOL ret = CryptUnprotectMemory(m_buffer, DWORD(m_size), CRYPTPROTECTMEMORY_SAME_PROCESS);
    REALM_ASSERT_RELEASE_EX(ret == TRUE && "CryptUnprotectMemory()", GetLastError());
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

#if defined(__STDC_LIB_EXT1__) || __APPLE__
    memset_s(m_buffer, m_size, 0, m_size);
#else
#warning "Platforms lacks memset_s"
#endif

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
