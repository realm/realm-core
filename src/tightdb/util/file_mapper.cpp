#include "file_mapper.hpp"

#include <cerrno>
#include <sys/mman.h>

#ifdef TIGHTDB_ENABLE_ENCRYPTION

#include "encrypted_file_mapping.hpp"

#include <atomic>
#include <memory>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include <tightdb/alloc_slab.hpp>
#include <tightdb/util/terminate.hpp>

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

const int page_size = 4096;

class SpinLockGuard {
public:
    SpinLockGuard(std::atomic_flag& lock) : m_lock(lock) {
        while (m_lock.test_and_set(std::memory_order_acquire)) ;
    }

    ~SpinLockGuard() {
        m_lock.clear(std::memory_order_release);
    }

private:
    std::atomic_flag& m_lock;
};

// A list of all of the active encrypted mappings for a single file
struct mappings_for_file  {
    dev_t device;
    ino_t inode;
    std::unique_ptr<SharedFileInfo> info;
};

// Group the information we need to map a SIGSEGV address to an
// EncryptedFileMapping for the sake of cache-friendliness with 3+ active
// mappings (and no worse with only two
struct mapping_and_addr {
    std::unique_ptr<EncryptedFileMapping> mapping;
    void* addr;
    size_t size;
};

std::atomic_flag mapping_lock = ATOMIC_FLAG_INIT;
std::vector<mapping_and_addr> mappings_by_addr;
std::vector<mappings_for_file> mappings_by_file;

// The signal handlers which our handlers replaced, if any, for forwarding
// signals for segfaults outside of our encrypted pages
struct sigaction old_segv;
struct sigaction old_bus;

void signal_handler(int code, siginfo_t* info, void* ctx) {
    SpinLockGuard lock{mapping_lock};
    for (auto& m : mappings_by_addr) {
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

mapping_and_addr* find_mapping_for_addr(void* addr, size_t size) {
    for (auto& m : mappings_by_addr) {
        if (m.addr == addr && m.size == size) {
            return &m;
        }
    }

    return 0;
}

size_t round_up_to_page_size(size_t size) {
    return (size + page_size - 1) & ~(page_size - 1);
}

void add_mapping(void* addr, size_t size, int fd, File::AccessMode access, const uint8_t* encryption_key) {
    SpinLockGuard lock{mapping_lock};

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

    struct stat st;
    if (fstat(fd, &st))
        TIGHTDB_TERMINATE("fstat failed"); // FIXME: throw instead

    if (st.st_size > 0 && st.st_size < page_size)
        throw InvalidDatabase();

    auto it = find_if(begin(mappings_by_file), end(mappings_by_file), [=](mappings_for_file& m) {
        return m.inode == st.st_ino && m.device == st.st_dev;
    });

    // Get the potential memory allocation out of the way so that mappings_by_addr.push_back can't throw
    mappings_by_addr.reserve(mappings_by_addr.size() + 1);

    if (it == end(mappings_by_file)) {
        mappings_by_file.emplace_back();
        it = --end(mappings_by_file);
        it->device = st.st_dev;
        it->inode = st.st_ino;
        it->info.reset(new SharedFileInfo{encryption_key, dup(fd)});
    }

    try {
        auto mapping = new EncryptedFileMapping{*it->info, addr, size, access}; // throws
        mappings_by_addr.push_back(mapping_and_addr{std::unique_ptr<EncryptedFileMapping>{mapping}, addr, size});
    }
    catch (...) {
        if (it->info->mappings.empty()) {
            ::close(it->info->fd);
            mappings_by_file.erase(it);
        }
        throw;
    }
}

void remove_mapping(void* addr, size_t size) {
    size = round_up_to_page_size(size);
    SpinLockGuard lock{mapping_lock};
    auto m = find_mapping_for_addr(addr, size);
    if (!m)
        return;

    mappings_by_addr.erase(mappings_by_addr.begin() + (m - &mappings_by_addr[0]));
    for (auto it = begin(mappings_by_file); it != end(mappings_by_file); ++it) {
        if (it->info->mappings.empty()) {
            ::close(it->info->fd);
            mappings_by_file.erase(it);
            break;
        }
    }
}

void* mmap_anon(size_t size) {
    void* addr = ::mmap(0, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (addr == MAP_FAILED) {
        int err = errno; // Eliminate any risk of clobbering
        throw std::runtime_error(get_errno_msg("mmap() failed: ", err));
    }
    return addr;
}

#endif
} // anonymous namespace

namespace tightdb {
namespace util {

void* mmap(int fd, size_t size, File::AccessMode access, const uint8_t* encryption_key) {
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

void munmap(void* addr, size_t size) TIGHTDB_NOEXCEPT {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    remove_mapping(addr, size);
#endif
    ::munmap(addr, size);
}

void* mremap(int fd, void* old_addr, size_t old_size, File::AccessMode a, size_t new_size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    {
        SpinLockGuard lock{mapping_lock};
        size_t rounded_old_size = round_up_to_page_size(old_size);
        if (auto m = find_mapping_for_addr(old_addr, rounded_old_size)) {
            size_t rounded_new_size = round_up_to_page_size(new_size);
            if (rounded_old_size == rounded_new_size)
                return old_addr;

            auto new_addr = mmap_anon(rounded_new_size);
            m->mapping->set(new_addr, rounded_new_size);
            ::munmap(old_addr, rounded_old_size);
            m->addr = new_addr;
            m->size = rounded_new_size;
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
    ::munmap(old_addr, old_size);
    return new_addr;
#endif
}

void msync(void* addr, size_t size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    { // first check the encrypted mappings
        SpinLockGuard lock{mapping_lock};
        if (auto m = find_mapping_for_addr(addr, size)) {
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
