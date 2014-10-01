#include "file_mapper.hpp"

#include <sys/mman.h>

#ifdef TIGHTDB_ENABLE_ENCRYPTION

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <vector>

#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>

#include <openssl/aes.h>

#include <tightdb/util/terminate.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

#endif

namespace {

std::string get_errno_msg(const char* prefix, int err) {
    char buffer[256];
    std::string str;
    str.reserve(strlen(prefix) + sizeof(buffer));
    str += prefix;

    if (TIGHTDB_LIKELY(strerror_r(err, buffer, sizeof(buffer)) == 0))
        str += buffer;
    else
        str += "Unknown error";

    return str;
}

#ifdef TIGHTDB_ENABLE_ENCRYPTION

struct spin_lock_guard {
    std::atomic_flag &lock;

    spin_lock_guard(std::atomic_flag &lock) : lock(lock) {
        while (lock.test_and_set(std::memory_order_acquire)) ;
    }

    ~spin_lock_guard() {
        lock.clear(std::memory_order_release);
    }
};

const int block_size = 16;

size_t aes_block_size(size_t len) {
    return (len + block_size - 1) & ~(block_size - 1);
}

// The system copy of OpenSSL is deprecated on OS X
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
class AESCryptor {
public:
    AESCryptor(const uint8_t *key) {
        AES_set_encrypt_key(key, 256, &ectx);
        AES_set_decrypt_key(key, 256, &dctx);
    }

    void read(int fd, off_t pos, uint8_t *dst, size_t size) {
        uint8_t buffer[4096];
        lseek(fd, pos, SEEK_SET);
        auto count = ::read(fd, buffer, aes_block_size(size));
        crypt(AES_DECRYPT, pos, dst, buffer, count);
    }

    void write(int fd, off_t pos, const uint8_t *src, size_t size) {
        uint8_t buffer[4096];
        auto bytes = crypt(AES_ENCRYPT, pos, buffer, src, size);
        lseek(fd, pos, SEEK_SET);
        ::write(fd, buffer, bytes);
    }

private:
    size_t crypt(int mode, off_t pos, uint8_t *dst, const uint8_t *src, size_t len) {
        TIGHTDB_ASSERT(len <= 4096);

        uint8_t buffer[4096];
        // if source len isn't a multiple of the block size, pad it with zeroes
        if (len & (block_size - 1)) {
            auto padded_len = aes_block_size(len);
            memcpy(buffer, src, len);
            memset(buffer + len, 0, padded_len - len);
            src = buffer;
            len = padded_len;
        }
        AES_KEY& key = mode == AES_ENCRYPT ? ectx : dctx;

        uint8_t iv[block_size] = {0};
        memcpy(iv, &pos, sizeof(pos));

        AES_cbc_encrypt(src, dst, len, &key, iv, mode);
        return len;
    }

    AES_KEY ectx;
    AES_KEY dctx;
};
#pragma GCC diagnostic pop

class EncryptedFileMapping;

std::atomic_flag mapping_lock = ATOMIC_FLAG_INIT;
EncryptedFileMapping *mappings = nullptr;

class EncryptedFileMapping {
public:
    int fd;

    void *addr;
    size_t size;

    uintptr_t page;
    size_t count;

    std::vector<bool> read_pages;
    std::vector<bool> dirty_pages;

    dev_t device;
    ino_t inode;

    EncryptedFileMapping *next;

    File::AccessMode access;

    const uint8_t *key;
    AESCryptor cryptor;

    EncryptedFileMapping(int fd, void *addr, size_t size, EncryptedFileMapping *next, File::AccessMode access, const uint8_t *key)
    : fd(dup(fd))
    , addr(addr)
    , size(size)
    , page((uintptr_t)addr >> 12)
    , count((size + 4095) >> 12)
    , read_pages(count, false)
    , dirty_pages(count, false)
    , next(next)
    , access(access)
    , cryptor(key)
    {
        struct stat st;
        if (fstat(fd, &st)) TIGHTDB_TERMINATE("fstat failed");
        inode = st.st_ino;
        device = st.st_dev;
    }

    ~EncryptedFileMapping() {
        ::close(fd);
    }

    bool same_file(const EncryptedFileMapping *m) const {
        return m != this && m->inode == inode && m->device == device;
    }

    char *page_addr(size_t i) const {
        return (char *)((page + i) << 12);
    }

    size_t page_size(size_t i) const {
        if (i < count - 1)
            return 4096;
        return min<size_t>(4096, size - (page_addr(i) - (char *)addr));
    }

    void mark_unreadable(size_t i) {
        mprotect(page_addr(i), 4096, PROT_NONE);
        read_pages[i] = false;
        dirty_pages[i] = false;
    }

    void mark_readable(size_t i) {
        mprotect(page_addr(i), 4096, PROT_READ);
        read_pages[i] = true;
        dirty_pages[i] = false;
    }

    void mark_writeable(size_t i) {
        mprotect(page_addr(i), 4096, PROT_READ | PROT_WRITE);
        dirty_pages[i] = true;
    }

    void write_page(size_t i) {
        if (!dirty_pages[i]) return;

        auto addr = page_addr(i);
        auto count = page_size(i);
        lseek(fd, i << 12, SEEK_SET);
        write(fd, addr, count);

        mark_readable(i);
    }

    void flush_others(size_t i) {
        for (auto m = mappings; m; m = m->next) {
            if (same_file(m) && i < m->count && m->dirty_pages[i]) {
                m->flush();
                return; // can't have mappings with same page dirty
            }
        }
    }

    void read_page(size_t i) {
        flush_others(i);

        auto addr = page_addr(i);
        mprotect(addr, 4096, PROT_READ | PROT_WRITE);
        cryptor.read(fd, i << 12, (uint8_t *)addr, page_size(i));

        mark_readable(i);
    }

    void validate_page(size_t i) {
        if (!read_pages[i]) return;

        uint8_t buffer[4096];
        cryptor.read(fd, i << 12, buffer, sizeof(buffer));
        if (memcmp(buffer, page_addr(i), page_size(i))) {
            printf("mismatch %p: fd(%d) page(%zu/%zu) page_size(%zu) %s %s\n",
                   this, fd, i, count, page_size(i), buffer, page_addr(i));
            TIGHTDB_TERMINATE("");
        }
    }

    void validate() {
        for (size_t i = 0; i < count; ++i)
            validate_page(i);
    }

    void flush() {
        // invalidate all read mappings for pages we're about to write to
        for (auto m = mappings; m; m = m->next) {
            if (same_file(m))
                m->invalidate(dirty_pages);
        }

        for (size_t i = 0; i < count; ++i) {
            if (!dirty_pages[i]) {
                validate_page(i);
                continue;
            }

            mark_readable(i);
            cryptor.write(fd, i << 12, (uint8_t *)page_addr(i), page_size(i));
        }

        validate();
    }

    void invalidate(std::vector<bool> const& pages) {
        for (size_t i = 0; i < count && i < pages.size(); ++i) {
            if (pages[i] && read_pages[i])
                mark_unreadable(i);
        }
    }

    void set(void *new_addr, size_t new_size) {
        flush();
        addr = new_addr;
        size = new_size;
        page = (uintptr_t)addr >> 12;
        count = (size + 4095) >> 12;
        read_pages.clear();
        read_pages.resize(count, false);
        dirty_pages.resize(count, false);
    }
};

struct sigaction old_segv;
struct sigaction old_bus;

void handler(int code, siginfo_t *info, void *ctx) {
    auto page = (uintptr_t)info->si_addr >> 12;

    spin_lock_guard lock{mapping_lock};
    for (auto m = mappings; m; m = m->next) {
        if (m->page > page || m->page + m->count <= page) continue;

        size_t idx = page - m->page;
        if (!m->read_pages[idx]) {
            m->read_page(idx);
        }
        else if (m->access == File::access_ReadWrite) {
            m->flush_others(idx);
            m->mark_writeable(idx);
        }

        return;
    }

    if (code == SIGSEGV) {
        if (old_segv.sa_sigaction)
            old_segv.sa_sigaction(code, info, ctx);
        else if (old_segv.sa_handler)
            old_segv.sa_handler(code);
    }
    else if (code == SIGBUS) {
        if (old_bus.sa_sigaction)
            old_bus.sa_sigaction(code, info, ctx);
        else if (old_bus.sa_handler)
            old_bus.sa_handler(code);
    }
    else
        TIGHTDB_TERMINATE("Segmentation fault");
}

void add_mapping(void *addr, size_t size, int fd, File::AccessMode access, const uint8_t *encryption_key) {
    spin_lock_guard lock{mapping_lock};
    mappings = new EncryptedFileMapping{fd, addr, size, mappings, access, encryption_key};

    if (!mappings->next) {
        struct sigaction action;
        action.sa_sigaction = handler;
        action.sa_flags = SA_SIGINFO;

        if (sigaction(SIGSEGV, &action, &old_segv) != 0)
            TIGHTDB_TERMINATE("sigaction SEGV failed");
        if (sigaction(SIGBUS, &action, &old_bus) != 0)
            TIGHTDB_TERMINATE("sigaction SIGBUS");
    }
}

void remove_mapping(void *addr, size_t size) {
    spin_lock_guard lock{mapping_lock};
    auto prev = &mappings;
    for (auto m = mappings; m; m = m->next) {
        if (m->addr == addr && m->size == size) {
            m->flush();
            *prev = m->next;
            delete m;
            return;
        }
        prev = &m->next;
    }
}

void *mmap_anon(size_t size) {
    void* addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }
    mprotect(addr, size, PROT_NONE);
    return addr;
}

#endif
} // anonymous namespace


namespace tightdb {
namespace util {

void *mmap(int fd, size_t size, File::AccessMode access, const uint8_t *encryption_key) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    if (encryption_key) {
        void* addr = mmap_anon(size);
        add_mapping(addr, size, fd, access, encryption_key);
        return addr;
    }
    else
#else
    TIGHTDB_ASSERT(!encryption_key);
    (void)encryption_key;
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

void munmap(void* addr, size_t size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    ::munmap(addr, size);
}

void* mremap(int fd, void* old_addr, size_t old_size, File::AccessMode a, size_t new_size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    {
        spin_lock_guard lock{mapping_lock};
        for (auto m = mappings; m; m = m->next) {
            if (m->addr == old_addr && m->size == old_size) {
                m->set(mmap_anon(new_size), new_size);
                ::munmap(old_addr, old_size);
                return m->addr;
            }
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
    throw runtime_error(get_errno_msg("mremap(): failed: ", err));
#else
    void* new_addr = mmap(fd, new_size, a, nullptr);
    ::munmap(old_addr, old_size);
    return new_addr;
#endif
}

void msync(void* addr, size_t size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    { // first check the encrypted mappings
        spin_lock_guard lock{mapping_lock};
        for (auto m = mappings; m; m = m->next) {
            if (m->addr != addr || m->size != size) continue;

            m->flush();
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
