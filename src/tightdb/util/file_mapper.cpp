/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

#ifndef _WIN32

#include "file_mapper.hpp"

#include <cerrno>
#include <sys/mman.h>

#include <tightdb/util/errno.hpp>

#ifdef TIGHTDB_ENABLE_ENCRYPTION

#include "encrypted_file_mapping.hpp"

#include <memory>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <tightdb/util/errno.hpp>
#include <tightdb/util/shared_ptr.hpp>
#include <tightdb/util/terminate.hpp>
#include <tightdb/util/thread.hpp>

using namespace tightdb;
using namespace tightdb::util;

namespace {
const size_t page_size = 4096;

class SpinLockGuard {
public:
    SpinLockGuard(Atomic<bool>& lock) : m_lock(lock)
    {
        while (m_lock.exchange_acquire(true)) ;
    }

    ~SpinLockGuard()
    {
        m_lock.store_release(false);
    }

private:
    Atomic<bool>& m_lock;
};

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

Atomic<bool> mapping_lock;
std::vector<mapping_and_addr> mappings_by_addr;
std::vector<mappings_for_file> mappings_by_file;

// The signal handlers which our handlers replaced, if any, for forwarding
// signals for segfaults outside of our encrypted pages
struct sigaction old_segv;
struct sigaction old_bus;

void signal_handler(int code, siginfo_t* info, void* ctx)
{
    SpinLockGuard lock(mapping_lock);
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr > info->si_addr || static_cast<char*>(m.addr) + m.size <= info->si_addr)
            continue;

        m.mapping->handle_access(info->si_addr);
        return;
    }

    // forward unhandled signals
    if (code == SIGSEGV) {
        if (old_segv.sa_sigaction)
            old_segv.sa_sigaction(code, info, ctx);
        else if (old_segv.sa_handler)
            old_segv.sa_handler(code);
        else
            TIGHTDB_TERMINATE("Segmentation fault");
    }
    else if (code == SIGBUS) {
        if (old_bus.sa_sigaction)
            old_bus.sa_sigaction(code, info, ctx);
        else if (old_bus.sa_handler)
            old_bus.sa_handler(code);
        else
            TIGHTDB_TERMINATE("Segmentation fault");
    }
    else
        TIGHTDB_TERMINATE("Segmentation fault");
}

mapping_and_addr* find_mapping_for_addr(void* addr, size_t size)
{
    for (size_t i = 0; i < mappings_by_addr.size(); ++i) {
        mapping_and_addr& m = mappings_by_addr[i];
        if (m.addr == addr && m.size == size)
            return &m;
    }

    return 0;
}

size_t round_up_to_page_size(size_t size)
{
    return (size + page_size - 1) & ~(page_size - 1);
}

void add_mapping(void* addr, size_t size, int fd, File::AccessMode access, const char* encryption_key)
{
    struct stat st;
    if (fstat(fd, &st)) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("fstat() failed: ", err));
    }

    if (st.st_size > 0 && static_cast<size_t>(st.st_size) < page_size)
        throw DecryptionFailed();

    SpinLockGuard lock(mapping_lock);

    static bool has_installed_handler = false;
    if (!has_installed_handler) {
        has_installed_handler = true;

        struct sigaction action;
        action.sa_sigaction = signal_handler;
        action.sa_flags = SA_SIGINFO;

        if (sigaction(SIGSEGV, &action, &old_segv) != 0)
            TIGHTDB_TERMINATE("sigaction SEGV failed");
        if (sigaction(SIGBUS, &action, &old_bus) != 0)
            TIGHTDB_TERMINATE("sigaction SIGBUS");
    }

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
        m.mapping = new EncryptedFileMapping(*it->info, addr, size, access);
        mappings_by_addr.push_back(m); // can't throw due to reserve() above
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
    SpinLockGuard lock(mapping_lock);
    mapping_and_addr* m = find_mapping_for_addr(addr, size);
    if (!m)
        return;

    mappings_by_addr.erase(mappings_by_addr.begin() + (m - &mappings_by_addr[0]));
    for (std::vector<mappings_for_file>::iterator it = mappings_by_file.begin(); it != mappings_by_file.end(); ++it) {
        if (it->info->mappings.empty()) {
            if(::close(it->info->fd) != 0) {
                int err = errno; // Eliminate any risk of clobbering
                if(err == EBADF || err == EIO) // todo, how do we handle EINTR?
                    throw std::runtime_error(get_errno_msg("close() failed: ", err));                
            }
            mappings_by_file.erase(it);
            break;
        }
    }
}

void* mmap_anon(size_t size)
{
    void* addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }
    return addr;
}

} // anonymous namespace
#endif

namespace tightdb {
namespace util {

void* mmap(int fd, size_t size, File::AccessMode access, const char* encryption_key)
{
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    if (encryption_key) {
        size = round_up_to_page_size(size);
        void* addr = mmap_anon(size);
        add_mapping(addr, size, fd, access, encryption_key);
        return addr;
    }
    else
#else
    TIGHTDB_ASSERT(!encryption_key);
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

        void* addr = ::mmap(0, size, prot, MAP_SHARED, fd, 0);
        if (addr != MAP_FAILED)
            return addr;
    }

    int err = errno; // Eliminate any risk of clobbering
    throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
}

void munmap(void* addr, size_t size) TIGHTDB_NOEXCEPT
{
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    if(::munmap(addr, size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
}

void* mremap(int fd, void* old_addr, size_t old_size, File::AccessMode a, size_t new_size)
{
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    {
        SpinLockGuard lock(mapping_lock);
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (mapping_and_addr* m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            void* new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size);
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
    static_cast<void>(fd);
    static_cast<void>(a);
    void* new_addr = ::mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
    if (new_addr != MAP_FAILED)
        return new_addr;
    int err = errno; // Eliminate any risk of clobbering
    throw std::runtime_error(get_errno_msg("mremap(): failed: ", err));
#else
    void* new_addr = mmap(fd, new_size, a, nullptr);
    if(::munmap(old_addr, old_size) != 0) {
        int err = errno;
        throw std::runtime_error(get_errno_msg("munmap() failed: ", err));
    }
    return new_addr;
#endif
}

void msync(void* addr, size_t size)
{
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    { // first check the encrypted mappings
        SpinLockGuard lock(mapping_lock);
        if (mapping_and_addr* m = find_mapping_for_addr(addr, size)) {
            m->mapping->flush();
            m->mapping->sync();
            return;
        }
    }
#endif

    // not an encrypted mapping
    if (::msync(addr, size, MS_SYNC) != 0) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("msync() failed: ", err));
    }
}

}
}

#endif // _WIN32
