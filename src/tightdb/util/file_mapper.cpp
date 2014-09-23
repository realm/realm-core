#include "file_mapper.hpp"

#include <sys/mman.h>

#ifdef TIGHTDB_ENABLE_ENCRYPTION

#include <atomic>
#include <cerrno>
#include <vector>

#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>

#include <openssl/evp.h>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

namespace {
#ifdef __APPLE__
#include <execinfo.h>
void print_backtrace() {
    void* callstack[128];
    int i, frames = backtrace(callstack, 128);
    char** strs = backtrace_symbols(callstack, frames);
    for (i = 0; i < frames; ++i) {
        printf("%s\n", strs[i]);
    }
    free(strs);
}
#else
void print_backtrace() { }
#endif

void die(const char *msg) {
    puts(msg);
    print_backtrace();
    abort();
}

struct spin_lock_guard {
    std::atomic_flag &lock;

    spin_lock_guard(std::atomic_flag &lock) : lock(lock) {
        while (lock.test_and_set(std::memory_order_acquire)) ;
//            die("multiple threads or re-entrant signal");
    }

    ~spin_lock_guard() {
        lock.clear(std::memory_order_release);
    }
};

#pragma mark - crypto
const int block_size = 16;

size_t aes_block_size(size_t len) {
    return (len + block_size - 1) & ~(block_size - 1);
}

// The system copy of OpenSSL is deprecated on OS X
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
class AESCryptor {
public:
    AESCryptor(const uint8_t *key) : key(key) {
        EVP_CIPHER_CTX_init(&ctx);
    }

    ~AESCryptor() {
        EVP_CIPHER_CTX_cleanup(&ctx);
    }

    void read(int fd, off_t pos, uint8_t *dst, size_t size) {
        uint8_t buffer[4096];
        lseek(fd, pos, SEEK_SET);
        auto count = ::read(fd, buffer, aes_block_size(size));
        crypt(0 /* decrypt */, pos, dst, buffer, count);
    }

    void write(int fd, off_t pos, const uint8_t *src, size_t size) {
        uint8_t buffer[4096];
        auto bytes = crypt(1 /* encrypt */, pos, buffer, src, size);
        lseek(fd, pos, SEEK_SET);
        ::write(fd, buffer, bytes);
    }

private:
    int crypt(int mode, off_t pos, uint8_t *dst, const uint8_t *src, size_t len) {
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

        uint8_t iv[block_size] = {0};
        memcpy(iv, &pos, sizeof(pos));

        int written = 0, flush_len = 0;
        int ret = EVP_CipherInit_ex(&ctx, EVP_aes_256_cfb(), nullptr, key, iv, mode);
        TIGHTDB_ASSERT(ret);
        ret = EVP_CipherUpdate(&ctx, dst, &written, src, (int)len);
        TIGHTDB_ASSERT(ret);
        TIGHTDB_ASSERT(written == (int)len);
        ret = EVP_CipherFinal_ex(&ctx, dst + written, &flush_len);
        TIGHTDB_ASSERT(flush_len == 0);
        TIGHTDB_ASSERT(ret);
        (void)ret;
        return written + flush_len;
    }

    const uint8_t *key;
    EVP_CIPHER_CTX ctx;
};
#pragma GCC diagnostic pop

#pragma mark - mmap
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
        if (fstat(fd, &st)) die("fstat failed");
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
            die("");
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
};

void handler(int, siginfo_t *info, void *) {
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

    die("segv");
}

void add_mapping(void *addr, size_t size, int fd, File::AccessMode access, const uint8_t *encryption_key) {
    spin_lock_guard lock{mapping_lock};
    mappings = new EncryptedFileMapping{fd, addr, size, mappings, access, encryption_key};

    if (!mappings->next) {
        struct sigaction action;
        action.sa_sigaction = handler;
        action.sa_flags = SA_SIGINFO;

        if (sigaction(SIGSEGV, &action, NULL) != 0) die("sigaction");
        if (sigaction(SIGBUS, &action, NULL) != 0) die("sigaction");
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

#else
namespace {
#endif

std::string get_errno_msg(const char* prefix, int err)
{
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

} // anonymous namespace


namespace tightdb {
namespace util {

void *mmap(int fd, size_t size, File::AccessMode access, const uint8_t *encryption_key) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    if (encryption_key) {
        void* addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
        if (addr != MAP_FAILED) {
            mprotect(addr, size, PROT_NONE);
            add_mapping(addr, size, fd, access, encryption_key);
            return addr;
        }
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
    std::string msg = get_errno_msg("mmap() failed: ", err);
    throw std::runtime_error(msg);
}

void munmap(void* addr, size_t size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    ::munmap(addr, size);
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
