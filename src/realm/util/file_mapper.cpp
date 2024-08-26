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

#include <realm/util/file_mapper.hpp>

#include <realm/exceptions.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/features.h>
#include <realm/util/to_string.hpp>

#include <system_error>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <sys/mman.h>
#endif

#if REALM_ENABLE_ENCRYPTION

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/aes_cryptor.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/terminate.hpp>

#include <atomic>
#include <memory>
#include <csignal>
#include <sys/stat.h>
#include <cstring>
#include <atomic>
#include <thread>
#include <cstring> // for memset

#if REALM_PLATFORM_APPLE
#include <dispatch/dispatch.h>
#endif

#endif // enable encryption

namespace {
inline bool is_mmap_memory_error(int err)
{
    return (err == EAGAIN || err == EMFILE || err == ENOMEM);
}
} // Unnamed namespace

namespace realm::util {
size_t round_up_to_page_size(size_t size) noexcept
{
    auto ps = page_size();
    return (size + ps - 1) & ~(ps - 1);
}

void* mmap(const FileAttributes& file, size_t size, uint64_t offset, std::unique_ptr<EncryptedFileMapping>& mapping)
{
    _impl::SimulatedFailure::trigger_mmap(size);

#if REALM_ENABLE_ENCRYPTION
    if (file.encryption) {
        auto page_start = offset & ~(page_size() - 1);
        size += size_t(offset - page_start);
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        ScopeExitFail cleanup([&]() noexcept {
            munmap(addr, size);
        });
        mapping = file.encryption->add_mapping(page_start, addr, size, file.access);
        return static_cast<char*>(addr) - page_start + offset;
    }
    mapping = nullptr;
#else
    static_cast<void>(mapping);
#endif

#ifndef _WIN32
    int prot = PROT_READ;
    switch (file.access) {
        case File::access_ReadWrite:
            prot |= PROT_WRITE;
            break;
        case File::access_ReadOnly:
            break;
    }

    void* addr = ::mmap(nullptr, size, prot, MAP_SHARED, file.fd, offset);
    if (addr != MAP_FAILED)
        return addr;

    int err = errno; // Eliminate any risk of clobbering
    if (is_mmap_memory_error(err)) {
        throw AddressSpaceExhausted(util::format("mmap() failed: %1 (size: %2, offset: %3)",
                                                 make_basic_system_error_code(err).message(), size, offset));
    }

    throw SystemError(err, util::format("mmap() failed (size: %1, offset: %2", size, offset));

#else
    DWORD protect = PAGE_READONLY;
    DWORD desired_access = FILE_MAP_READ;
    switch (file.access) {
        case File::access_ReadOnly:
            break;
        case File::access_ReadWrite:
            protect = PAGE_READWRITE;
            desired_access = FILE_MAP_WRITE;
            break;
    }
    LARGE_INTEGER large_int;
    if (int_cast_with_overflow_detect(offset + size, large_int.QuadPart))
        throw std::runtime_error("Map size is too large");
    HANDLE map_handle = CreateFileMappingFromApp(file.fd, 0, protect, offset + size, nullptr);
    if (!map_handle)
        throw AddressSpaceExhausted(get_errno_msg("CreateFileMapping() failed: ", GetLastError()) +
                                    " size: " + util::to_string(size) + " offset: " + util::to_string(offset));

    if (int_cast_with_overflow_detect(offset, large_int.QuadPart))
        throw RuntimeError(ErrorCodes::RangeError, "Map offset is too large");

    SIZE_T _size = size;
    void* addr = MapViewOfFileFromApp(map_handle, desired_access, offset, _size);
    BOOL r = CloseHandle(map_handle);
    REALM_ASSERT_RELEASE(r);
    if (!addr)
        throw AddressSpaceExhausted(get_errno_msg("MapViewOfFileFromApp() failed: ", GetLastError()) +
                                    " size: " + util::to_string(_size) + " offset: " + util::to_string(offset));

    return addr;
#endif
}

#if REALM_ENABLE_ENCRYPTION
std::unique_ptr<EncryptedFileMapping> reserve_mapping(void* addr, const FileAttributes& file, uint64_t offset)
{
    return file.encryption->add_mapping(offset, addr, 0, file.access);
}

#endif // REALM_ENABLE_ENCRYPTION

void* mmap_anon(size_t size)
{
#ifdef _WIN32
    HANDLE hMapFile;
    LPCTSTR pBuf;

    ULARGE_INTEGER s;
    s.QuadPart = size;

    hMapFile = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, s.HighPart, s.LowPart, nullptr);
    if (hMapFile == NULL) {
        throw std::system_error(GetLastError(), std::system_category(), "CreateFileMapping() failed");
    }

    pBuf = (LPTSTR)MapViewOfFile(hMapFile, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (pBuf == nullptr) {
        throw std::system_error(GetLastError(), std::system_category(), "MapViewOfFile() failed");
    }

    CloseHandle(hMapFile);
    return (void*)pBuf;
#else
    void* addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        if (is_mmap_memory_error(err)) {
            throw AddressSpaceExhausted(get_errno_msg("mmap() failed: ", err) + " size: " + util::to_string(size));
        }
        throw std::system_error(err, std::system_category(),
                                std::string("mmap() failed (size: ") + util::to_string(size) + ", offset is 0)");
    }
    return addr;
#endif
}

#ifndef _WIN32
void* mmap_fixed(FileDesc fd, void* address_request, size_t size, File::AccessMode access, uint64_t offset)
{
    _impl::SimulatedFailure::trigger_mmap(size);
    auto prot = PROT_READ;
    if (access == File::access_ReadWrite)
        prot |= PROT_WRITE;
    auto addr = ::mmap(address_request, size, prot, MAP_SHARED | MAP_FIXED, fd, offset);
    if (addr != MAP_FAILED && addr != address_request) {
        throw std::runtime_error(get_errno_msg("mmap() failed: ", errno) +
                                 ", when mapping an already reserved memory area");
    }
    return addr;
}
#endif // _WIN32


void munmap(void* addr, size_t size)
{
    auto shift = reinterpret_cast<uintptr_t>(addr) & (page_size() - 1);
    addr = static_cast<char*>(addr) - shift;
    size += shift;
#ifdef _WIN32
    if (!UnmapViewOfFile(addr))
        throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");

#else
    if (::munmap(addr, size) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "munmap() failed");
    }
#endif
}

void msync(FileDesc fd, void* addr, size_t size)
{
#ifdef _WIN32
    // FlushViewOfFile() is asynchronous and won't flush metadata (file size, etc)
    if (!FlushViewOfFile(addr, size)) {
        throw std::system_error(GetLastError(), std::system_category(), "FlushViewOfFile() failed");
    }
    // Block until data and metadata is written physically to the media
    if (!FlushFileBuffers(fd)) {
        throw std::system_error(GetLastError(), std::system_category(), "FlushFileBuffers() failed");
    }
    return;
#else
    static_cast<void>(fd);
    int retries_left = 1000;
    while (::msync(addr, size, MS_SYNC) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        if (--retries_left < 0)
            throw std::system_error(err, std::system_category(), "msync() retries exhausted");
        if (err != EINTR)
            throw std::system_error(err, std::system_category(), "msync() failed");
    }
#endif
}

#if REALM_ENABLE_ENCRYPTION
void do_encryption_read_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping, bool to_modify)
{
    mapping->read_barrier(addr, size, to_modify);
}

void do_encryption_write_barrier(const void* addr, size_t size, EncryptedFileMapping* mapping)
{
    mapping->write_barrier(addr, size);
}
#endif // REALM_ENABLE_ENCRYPTION

} // namespace realm::util
