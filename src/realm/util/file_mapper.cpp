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

#include <realm/util/features.h>

#include <realm/util/file_mapper.hpp>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <sys/mman.h>
#include <unistd.h>
#endif

#include <mutex>
#include <map>

#include <realm/exceptions.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/to_string.hpp>
#include <system_error>

#if REALM_ENABLE_ENCRYPTION

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/aes_cryptor.hpp>

#include <atomic>
#include <memory>
#include <csignal>
#include <sys/stat.h>
#include <cstring>
#include <atomic>
#include <fstream>
#include <sstream>
#include <regex>
#include <thread>

#include <realm/util/file.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/terminate.hpp>
#include <realm/util/thread.hpp>
#include <cstring> // for memset

#if REALM_PLATFORM_APPLE
#include <dispatch/dispatch.h>
#endif

#if REALM_ANDROID
#include <linux/unistd.h>
#include <sys/syscall.h>
#endif

#endif // enable encryption

namespace {

inline bool is_mmap_memory_error(int err)
{
    return (err == EAGAIN || err == EMFILE || err == ENOMEM);
}

} // Unnamed namespace

using namespace realm;
using namespace realm::util;

namespace realm {
namespace util {

size_t round_up_to_page_size(size_t size) noexcept
{
    return (size + page_size() - 1) & ~(page_size() - 1);
}

// Support for logging memory mappings
std::mutex mmap_log_mutex;
struct MMapEntry {
    bool is_file;
    size_t offset;
    char* start;
    char* end;
};

std::map<char*, MMapEntry> all_mappings;
static void remove_logged_mapping(char* start, size_t size)
{
    char* end = start + size;
    auto it = all_mappings.lower_bound(start);
    if (it == all_mappings.end())
        return;
    // previous entry may overlap, so must be included in scan:
    if (it != all_mappings.begin()) {
        --it;
    }
    // handle case where the range "punches a hole" in existing entry
    if (it->second.start < start && it->second.end > end) {
        // reduce entry to part before hole
        it->second.end = start;
        // add second entry for part after hole
        size_t offset = 0;
        if (it->second.is_file) {
            offset = it->second.offset + end - it->second.start;
        }
        MMapEntry entry{it->second.is_file, offset, end, it->second.end};
        all_mappings[end] = entry;
        return;
    }
    // scan for and adjust/remove entries which overlaps range:
    while (it != all_mappings.end() && it->second.start < end) {
        if (it->second.start >= start && it->second.end <= end) {
            // new range completely covers entry so it must die
            auto old_it = it++;
            all_mappings.erase(old_it);
            continue;
        }
        // partial overlap
        if (it->second.start < start) {
            // range overlaps back part of entry
            it->second.end = start;
            ++it;
            continue;
        }
        if (it->second.end > end) {
            // range overlaps front part of entry.
            auto entry = it->second;
            all_mappings.erase(it);
            if (entry.is_file) {
                entry.offset += start - entry.start;
            }
            entry.start = start;
            all_mappings[start] = entry;
            // this must have been the last entry, so we're done
            return;
        }
    }
}
static void add_logged_mapping(bool is_file, size_t offset, char* start, char* end)
{
    // TODO: Expand this to unify neightbouring mappings when possible.
    // This will require capturing unique file IDs.
    MMapEntry entry{is_file, offset, start, end};
    all_mappings[start] = entry;
}
static void add_logged_file_mapping(size_t offset, char* start, size_t size)
{
    // establishing a new mapping may transparently remove old ones
    remove_logged_mapping(start, size);
    // not the little new
    add_logged_mapping(true, offset, start, start + size);
}

static void add_logged_priv_mapping(char* start, size_t size)
{
    // establishing a new mapping may transparently remove old ones
    remove_logged_mapping(start, size);
    // not the little new
    add_logged_mapping(false, 0, start, start + size);
}
// end support for logging memory mappings

#if REALM_ENABLE_ENCRYPTION

// A list of all of the active encrypted mappings for a single file
struct mappings_for_file {
#ifdef _WIN32
    HANDLE handle;
#else
    dev_t device;
    ino_t inode;
#endif
    std::shared_ptr<SharedFileInfo> info;
};

// Group the information we need to map a SIGSEGV address to an
// EncryptedFileMapping for the sake of cache-friendliness with 3+ active
// mappings (and no worse with only two)
struct mapping_and_addr {
    std::shared_ptr<EncryptedFileMapping> mapping;
    void* addr;
    size_t size;
};

util::Mutex& mapping_mutex = *(new util::Mutex);
namespace {
std::vector<mapping_and_addr>& mappings_by_addr = *new std::vector<mapping_and_addr>;
std::vector<mappings_for_file>& mappings_by_file = *new std::vector<mappings_for_file>;
static std::atomic<size_t> num_decrypted_pages(0); // this is for statistical purposes

} // anonymous namespace

size_t get_num_decrypted_pages()
{
    return num_decrypted_pages.load();
}

void encryption_mark_pages_for_IV_check(EncryptedFileMapping* mapping)
{
    UniqueLock lock(mapping_mutex);
    mapping->mark_pages_for_IV_check();
}

namespace {


mapping_and_addr* find_mapping_for_addr(void* addr, size_t size)
{
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr == addr && m.size == size)
            return &m;
        REALM_ASSERT(m.addr != addr);
    }

    return 0;
}
} // anonymous namespace


namespace {
EncryptedFileMapping* add_mapping(void* addr, size_t size, const FileAttributes& file, size_t file_offset)
{
#ifndef _WIN32
    struct stat st;

    if (fstat(file.fd, &st)) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::system_error(err, std::system_category(), "fstat() failed");
    }
#endif

    size_t fs = to_size_t(File::get_size_static(file.fd));
    if (fs > 0 && fs < page_size())
        throw DecryptionFailed();

    LockGuard lock(mapping_mutex);

    std::vector<mappings_for_file>::iterator it;
    for (it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
#ifdef _WIN32
        if (File::is_same_file_static(it->handle, file.fd))
            break;
#else
        if (it->inode == st.st_ino && it->device == st.st_dev)
            break;
#endif
    }

    // Get the potential memory allocation out of the way so that mappings_by_addr.push_back can't throw
    mappings_by_addr.reserve(mappings_by_addr.size() + 1);

    if (it == mappings_by_file.end()) {
        mappings_by_file.reserve(mappings_by_file.size() + 1);
        mappings_for_file f;
        f.info = std::make_shared<SharedFileInfo>(reinterpret_cast<const uint8_t*>(file.encryption_key));

        FileDesc fd_duped;
#ifdef _WIN32
        if (!DuplicateHandle(GetCurrentProcess(), file.fd, GetCurrentProcess(), &fd_duped, 0, FALSE,
                             DUPLICATE_SAME_ACCESS))
            throw std::system_error(GetLastError(), std::system_category(), "DuplicateHandle() failed");
        f.info->fd = f.handle = fd_duped;
#else
        fd_duped = dup(file.fd);

        if (fd_duped == -1) {
            int err = errno; // Eliminate any risk of clobbering
            throw std::system_error(err, std::system_category(), "dup() failed");
        }
        f.info->fd = fd_duped;
        f.device = st.st_dev;
        f.inode = st.st_ino;
#endif // conditonal on _WIN32

        mappings_by_file.push_back(f); // can't throw due to reserve() above
        it = mappings_by_file.end() - 1;
    }
    else {
        it->info->cryptor.check_key(reinterpret_cast<const uint8_t*>(file.encryption_key));
    }

    try {
        mapping_and_addr m;
        m.addr = addr;
        m.size = size;
        m.mapping = std::make_shared<EncryptedFileMapping>(*it->info, file_offset, addr, size, file.access);
        mappings_by_addr.push_back(m); // can't throw due to reserve() above
        return m.mapping.get();
    }
    catch (...) {
        if (it->info->mappings.empty()) {
#ifdef _WIN32
            bool b = CloseHandle(it->info->fd);
            REALM_ASSERT_RELEASE(b);
#else
            ::close(it->info->fd);
#endif
            mappings_by_file.erase(it);
        }
        throw;
    }
}

void remove_mapping(void* addr, size_t size)
{
    size = round_up_to_page_size(size);
    LockGuard lock(mapping_mutex);
    mapping_and_addr* m = find_mapping_for_addr(addr, size);
    if (!m)
        return;

    mappings_by_addr.erase(mappings_by_addr.begin() + (m - &mappings_by_addr[0]));

    for (std::vector<mappings_for_file>::iterator it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->info->mappings.empty()) {
#ifdef _WIN32
            if (!CloseHandle(it->info->fd))
                throw std::system_error(GetLastError(), std::system_category(), "CloseHandle() failed");
#else
            if (::close(it->info->fd) != 0) {
                int err = errno;                // Eliminate any risk of clobbering
                if (err == EBADF || err == EIO) // FIXME: how do we handle EINTR?
                    throw std::system_error(err, std::system_category(), "close() failed");
            }
#endif
            mappings_by_file.erase(it);
            break;
        }
    }
}
} // anonymous namespace

void* mmap(const FileAttributes& file, size_t size, size_t offset, EncryptedFileMapping*& mapping)
{
    _impl::SimulatedFailure::trigger_mmap(size);
    if (file.encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        mapping = add_mapping(addr, size, file, offset);
        return addr;
    }
    else {
        mapping = nullptr;
        return mmap(file, size, offset);
    }
}


EncryptedFileMapping* reserve_mapping(void* addr, const FileAttributes& file, size_t offset)
{
    return add_mapping(addr, 0, file, offset);
}

void extend_encrypted_mapping(EncryptedFileMapping* mapping, void* addr, size_t offset, size_t old_size,
                              size_t new_size)
{
    LockGuard lock(mapping_mutex);
    auto m = find_mapping_for_addr(addr, old_size);
    REALM_ASSERT(m);
    m->size = new_size;
    mapping->extend_to(offset, new_size);
}

void remove_encrypted_mapping(void* addr, size_t size)
{
    remove_mapping(addr, size);
}

void* mmap_reserve(const FileAttributes& file, size_t reservation_size, size_t offset_in_file,
                   EncryptedFileMapping*& mapping)
{
    auto addr = mmap_reserve(file.fd, reservation_size, offset_in_file);
    if (file.encryption_key) {
        REALM_ASSERT(reservation_size == round_up_to_page_size(reservation_size));
        // we create a mapping for the entire reserved area. This causes full initialization of some fairly
        // large std::vectors, which it would be nice to avoid. This is left as a future optimization.
        mapping = add_mapping(addr, reservation_size, file, offset_in_file);
    }
    else {
        mapping = nullptr;
    }
    return addr;
}

#endif // REALM_ENABLE_ENCRYPTION

void* mmap_anon(size_t size)
{
    std::unique_lock lock(mmap_log_mutex);
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
    add_logged_priv_mapping((char*)pBuf, size);
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
    add_logged_priv_mapping((char*)addr, size);
    return addr;
#endif
}

void* mmap_fixed(FileDesc fd, void* address_request, size_t size, File::AccessMode access, size_t offset,
                 const char* enc_key)
{
    _impl::SimulatedFailure::trigger_mmap(size);
    static_cast<void>(enc_key); // FIXME: Consider removing this parameter
#ifdef _WIN32
    REALM_ASSERT(false);
    return nullptr; // silence warning
#else
    std::unique_lock lock(mmap_log_mutex);
    auto prot = PROT_READ;
    if (access == File::access_ReadWrite)
        prot |= PROT_WRITE;
    auto flags = (fd == -1) ? (MAP_FIXED | MAP_PRIVATE | MAP_ANON) : (MAP_SHARED | MAP_FIXED);
    auto addr = ::mmap(address_request, size, prot, flags, fd, offset);
    if (addr != MAP_FAILED && addr != address_request) {
        throw std::runtime_error(get_errno_msg("mmap() failed: ", errno) +
                                 ", when mapping an already reserved memory area");
    }
    if (fd == -1)
        add_logged_priv_mapping((char*)addr, size);
    else
        add_logged_file_mapping(offset, (char*)addr, size);
    return addr;
#endif
}

void* mmap_reserve(FileDesc fd, size_t reservation_size, size_t offset_in_file)
{
    // The other mmap operations take an fd as a parameter, so we do too.
    // We're not using it for anything currently, but this may change.
    // Similarly for offset_in_file.
    static_cast<void>(fd);
    static_cast<void>(offset_in_file);
#ifdef _WIN32
    REALM_ASSERT(false); // unsupported on windows
    return nullptr;
#else
    std::unique_lock lock(mmap_log_mutex);
    auto addr = ::mmap(0, reservation_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (addr == MAP_FAILED) {
        throw std::runtime_error(get_errno_msg("mmap() failed: ", errno));
    }
    add_logged_priv_mapping((char*)addr, reservation_size);
    return addr;
#endif
}


void* mmap(const FileAttributes& file, size_t size, size_t offset)
{
    _impl::SimulatedFailure::trigger_mmap(size);
#if REALM_ENABLE_ENCRYPTION
    if (file.encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        add_mapping(addr, size, file, offset);
        return addr;
    }
    else
#else
    REALM_ASSERT(!file.encryption_key);
#endif
    {
        std::unique_lock lock(mmap_log_mutex);
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
        if (addr != MAP_FAILED) {
            add_logged_file_mapping(offset, (char*)addr, size);
            return addr;
        }

        int err = errno; // Eliminate any risk of clobbering
        if (is_mmap_memory_error(err)) {
            throw AddressSpaceExhausted(get_errno_msg("mmap() failed: ", err) + " size: " + util::to_string(size) +
                                        " offset: " + util::to_string(offset));
        }

        throw SystemError(err, std::string("mmap() failed (size: ") + util::to_string(size) +
                                   ", offset: " + util::to_string(offset));

#else
        // FIXME: Is there anything that we must do on Windows to honor map_NoSync?

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

        add_logged_file_mapping(offset, (char*)addr, size);
        return addr;
#endif
    }
}

void munmap(void* addr, size_t size)
{
#if REALM_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif

#ifdef _WIN32
    if (!UnmapViewOfFile(addr))
        throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");

    remove_logged_mapping((char*)addr, size);
#else
    if (::munmap(addr, size) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "munmap() failed");
    }
    remove_logged_mapping((char*)addr, size);
#endif
}

void* mremap(const FileAttributes& file, size_t file_offset, void* old_addr, size_t old_size, size_t new_size)
{
#if REALM_ENABLE_ENCRYPTION
    if (file.encryption_key) {
        LockGuard lock(mapping_mutex);
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (mapping_and_addr* m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            void* new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size, file_offset);
            m->addr = new_addr;
            m->size = rounded_new_size;
#ifdef _WIN32
            if (!UnmapViewOfFile(old_addr))
                throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");
#else
            if (::munmap(old_addr, rounded_old_size)) {
                int err = errno;
                throw std::system_error(err, std::system_category(), "munmap() failed");
            }
#endif
            remove_logged_mapping((char*)old_addr, rounded_old_size);
            return new_addr;
        }
        // If we are using encryption, we must have used mmap and the mapping
        // must have been added to the cache therefore find_mapping_for_addr()
        // will succeed. Otherwise we would continue to mmap it below without
        // the encryption key which is an error.
        REALM_UNREACHABLE();
    }
#endif

#ifdef _GNU_SOURCE
    {
        void* new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
        if (new_addr != MAP_FAILED)
            return new_addr;
        remove_logged_mapping((char*)old_addr, old_size);
        int err = errno; // Eliminate any risk of clobbering
        // Do not throw here if mremap is declared as "not supported" by the
        // platform Eg. When compiling with GNU libc on OSX, iOS.
        // In this case fall through to no-mremap case below.
        if (err != ENOTSUP && err != ENOSYS) {
            if (is_mmap_memory_error(err)) {
                throw AddressSpaceExhausted(get_errno_msg("mremap() failed: ", err) + " old size: " +
                                            util::to_string(old_size) + " new size: " + util::to_string(new_size));
            }
            throw std::system_error(err, std::system_category(),
                                    std::string("_gnu_src mmap() failed (") + "old_size: " +
                                        util::to_string(old_size) + ", new_size: " + util::to_string(new_size) + ")");
        }
    }
#endif

    void* new_addr = mmap(file, new_size, file_offset);

#ifdef _WIN32
    if (!UnmapViewOfFile(old_addr))
        throw std::system_error(GetLastError(), std::system_category(), "UnmapViewOfFile() failed");
#else
    if (::munmap(old_addr, old_size) != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category(), "munmap() failed");
    }
#endif
    remove_logged_mapping((char*)old_addr, old_size);

    return new_addr;
}

void msync(FileDesc fd, void* addr, size_t size)
{
#if REALM_ENABLE_ENCRYPTION
    {
        // first check the encrypted mappings
        LockGuard lock(mapping_mutex);
        if (mapping_and_addr* m = find_mapping_for_addr(addr, round_up_to_page_size(size))) {
            m->mapping->flush();
            m->mapping->sync();
            return;
        }
    }
#endif

    // not an encrypted mapping

    // FIXME: on iOS/OSX fsync may not be enough to ensure crash safety.
    // Consider adding fcntl(F_FULLFSYNC). This most likely also applies to msync.
    //
    // See description of fsync on iOS here:
    // https://developer.apple.com/library/ios/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html
    //
    // See also
    // https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/CoreData/Articles/cdPersistentStores.html
    // for a discussion of this related to core data.

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
} // namespace util
} // namespace realm
