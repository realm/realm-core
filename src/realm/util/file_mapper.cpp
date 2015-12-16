/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
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
 **************************************************************************/

#include <realm/util/features.h>

#ifndef _WIN32

#include "file_mapper.hpp"

#include <cerrno>
#include <sys/mman.h>

#include <realm/util/errno.hpp>

#if REALM_ENABLE_ENCRYPTION

#include "encrypted_file_mapping.hpp"
#include "aes_cryptor.hpp"

#include <memory>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <atomic>

#include <realm/util/errno.hpp>
#include <realm/util/shared_ptr.hpp>
#include <realm/util/terminate.hpp>
#include <realm/util/thread.hpp>
#include <string.h> // for memset

#if REALM_PLATFORM_APPLE
#   include <mach/mach.h>
#   include <mach/exc.h>
#endif

#ifdef REALM_ANDROID
#include <linux/unistd.h>
#include <sys/syscall.h>
#endif

#endif // enable encryption

using namespace realm;
using namespace realm::util;

namespace realm {
namespace util {

#if REALM_ENABLE_ENCRYPTION

// A list of all of the active encrypted mappings for a single file
struct mappings_for_file {
    dev_t device;
    ino_t inode;
    SharedPtr<SharedFileInfo> info;
};

// Group the information we need to map a SIGSEGV address to an
// EncryptedFileMapping for the sake of cache-friendliness with 3+ active
// mappings (and no worse with only two
struct mapping_and_addr {
    SharedPtr<EncryptedFileMapping> mapping;
    void* addr;
    size_t size;
};

util::Mutex mapping_mutex;
std::vector<mapping_and_addr> mappings_by_addr;
std::vector<mappings_for_file> mappings_by_file;

// If there's any active mappings when the program exits, deliberately leak them
// to avoid flushing things that were in the middle of being modified on a different thrad
struct AtExit {
    ~AtExit()
    {
        if (!mappings_by_addr.empty())
            (new std::vector<mapping_and_addr>)->swap(mappings_by_addr);
        if (!mappings_by_file.empty())
            (new std::vector<mappings_for_file>)->swap(mappings_by_file);
    }
} at_exit;

mapping_and_addr* find_mapping_for_addr(void* addr, size_t size)
{
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr == addr && m.size == size)
            return &m;
    }

    return 0;
}

EncryptedFileMapping* add_mapping(void* addr, size_t size, int fd, size_t file_offset,
                                  File::AccessMode access, const char* encryption_key)
{
    struct stat st;
    if (fstat(fd, &st)) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("fstat() failed: ", err));
    }

    if (st.st_size > 0 && static_cast<size_t>(st.st_size) < page_size())
        throw DecryptionFailed();

    LockGuard lock(mapping_mutex);

    std::vector<mappings_for_file>::iterator it;
    for (it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->inode == st.st_ino && it->device == st.st_dev)
            break;
    }

    // Get the potential memory allocation out of the way so that mappings_by_addr.push_back can't throw
    mappings_by_addr.reserve(mappings_by_addr.size() + 1);

    if (it == mappings_by_file.end()) {
        mappings_by_file.reserve(mappings_by_file.size() + 1);

        fd = dup(fd);
        if (fd == -1) {
            int err = errno; // Eliminate any risk of clobbering
            throw std::runtime_error(get_errno_msg("dup() failed: ", err));
        }

        mappings_for_file f;
        f.device = st.st_dev;
        f.inode = st.st_ino;
        try {
            f.info = new SharedFileInfo(reinterpret_cast<const uint8_t*>(encryption_key), fd);
        }
        catch (...) {
            ::close(fd);
            throw;
        }

        mappings_by_file.push_back(f); // can't throw due to reserve() above
        it = mappings_by_file.end() - 1;
    }

    try {
        mapping_and_addr m;
        m.addr = addr;
        m.size = size;
        EncryptedFileMapping* m_ptr = new EncryptedFileMapping(*it->info, file_offset, addr, size, access);
        m.mapping = m_ptr;
        mappings_by_addr.push_back(m); // can't throw due to reserve() above
        return m_ptr;
    }
    catch (...) {
        if (it->info->mappings.empty()) {
            ::close(it->info->fd);
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
            if (::close(it->info->fd) != 0) {
                int err = errno; // Eliminate any risk of clobbering
                if (err == EBADF || err == EIO) // todo, how do we handle EINTR?
                    throw std::runtime_error(get_errno_msg("close() failed: ", err));
            }
            mappings_by_file.erase(it);
            break;
        }
    }
}

void* mmap_anon(size_t size)
{
    void* addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }
    return addr;
}

size_t round_up_to_page_size(size_t size) noexcept
{
    return (size + page_size() - 1) & ~(page_size() - 1);
}

void* mmap(int fd, size_t size, File::AccessMode access, size_t offset, const char* encryption_key, EncryptedFileMapping*& mapping) {
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        mapping = add_mapping(addr, size, fd, offset, access, encryption_key);
        return addr;
    }
    else {
        mapping = nullptr;
        return mmap(fd, size, access, offset, nullptr);
    }
}

#endif // enable encryption


void* mmap(int fd, size_t size, File::AccessMode access, size_t offset, const char* encryption_key)
{
#if REALM_ENABLE_ENCRYPTION
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        add_mapping(addr, size, fd, offset, access, encryption_key);
        return addr;
    }
    else
#else
    REALM_ASSERT(!encryption_key);
    static_cast<void>(encryption_key);
#endif
    {
        int prot = PROT_READ;
        switch (access) {
            case File::access_ReadWrite:
                prot |= PROT_WRITE;
                break;
            case File::access_ReadOnly:
                break;
        }

        void* addr = ::mmap(nullptr, size, prot, MAP_SHARED, fd, offset);
        if (addr != MAP_FAILED)
            return addr;
    }

    int err = errno; // Eliminate any risk of clobbering
    throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
}

void munmap(void* addr, size_t size) noexcept
{
#if REALM_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    if(::munmap(addr, size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
}

void* mremap(int fd, size_t file_offset, void* old_addr, size_t old_size,
             File::AccessMode a, size_t new_size)
{
#if REALM_ENABLE_ENCRYPTION
    {
        LockGuard lock(mapping_mutex);
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (mapping_and_addr* m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            void* new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size, file_offset);
            int i = ::munmap(old_addr, rounded_old_size);
            m->addr = new_addr;
            m->size = rounded_new_size;
            if (i != 0) {
                int err = errno;
                throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
            }
            return new_addr;
        }
    }
#endif

#ifdef _GNU_SOURCE
    {
        void* new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
        if (new_addr != MAP_FAILED)
            return new_addr;
        int err = errno; // Eliminate any risk of clobbering
        if (err != ENOTSUP && err != ENOSYS)
            throw std::runtime_error(get_errno_msg("mremap(): failed: ", err));
    }
    // Fall back to no-mremap case if it's not supported
#endif

    void* new_addr = mmap(fd, new_size, a, file_offset, nullptr);
    if (::munmap(old_addr, old_size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
    return new_addr;
}

void msync(void* addr, size_t size)
{
#if REALM_ENABLE_ENCRYPTION
    { // first check the encrypted mappings
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
    if (::msync(addr, size, MS_SYNC) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("msync() failed: ", err));
    }
}

}
}

#endif // _WIN32
