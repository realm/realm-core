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

#ifdef __APPLE__
#include <CommonCrypto/CommonCryptor.h>
#else
#include <openssl/aes.h>
#endif

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

const int aes_block_size = 16;
const int page_size = 4096;

#pragma mark Types

struct iv_table;

class AESCryptor {
public:
    AESCryptor(const uint8_t* key);
    ~AESCryptor();

    void read(int fd, off_t pos, char* dst, size_t size);
    void write(int fd, off_t pos, const char* src, size_t size);

private:
    enum EncryptionMode {
#ifdef __APPLE__
        mode_Encrypt = kCCEncrypt,
        mode_Decrypt = kCCDecrypt
#else
        mode_Encrypt = AES_ENCRYPT,
        mode_Decrypt = AES_DECRYPT
#endif
    };

#ifdef __APPLE__
    CCCryptorRef m_encr;
    CCCryptorRef m_decr;
#else
    AES_KEY m_ectx;
    AES_KEY m_dctx;
#endif

    size_t crypt(EncryptionMode mode, off_t pos, char* dst, const char* src,
                 size_t len, const char* stored_iv);

    iv_table read_iv_table(int fd, off_t data_pos);
};

class EncryptedFileMapping;

// We need to be able to search active mappings by two criteria: base address + size,
// and inode + device. For the sake of cache friendliness, index the active
// mappings by each of these
struct mapping_and_addr {
    EncryptedFileMapping* mapping;
    void* addr;
    size_t size;
};
struct mapping_and_file_info {
    EncryptedFileMapping* mapping;
    dev_t device;
    ino_t inode;
};

class EncryptedFileMapping {
public:
    EncryptedFileMapping(int fd, void* addr, size_t size, File::AccessMode access, const uint8_t* key);
    ~EncryptedFileMapping();

    // Write all dirty pages to disk and mark them read-only
    // Does not call fsync
    void flush();

    // Sync this file to disk
    void sync();

    // Handle a SEGV or BUS at the given address, which must be within this
    // object's mapping
    void handle_access(void* addr) TIGHTDB_NOEXCEPT;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size);

private:
    int m_fd;

    dev_t m_device;
    ino_t m_inode;

    void* m_addr = 0;
    size_t m_size = 0;

    uintptr_t m_first_page;
    size_t m_page_count = 0;

    std::vector<bool> m_read_pages;
    std::vector<bool> m_dirty_pages;

    File::AccessMode m_access;

    AESCryptor m_cryptor;

    bool same_file(mapping_and_file_info m) const;

    char* page_addr(size_t i) const;
    size_t actual_page_size(size_t i) const;

    void mark_unreadable(size_t i);
    void mark_readable(size_t i);

    void flush_page(size_t i);
    void read_page(size_t i);
    void write_page(size_t i);

    void validate_page(size_t i);
    void validate();
};

// a RAII spinlock, since pthreads aren't signal-safe
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

#pragma mark Mapping management

std::atomic_flag mapping_lock = ATOMIC_FLAG_INIT;
std::vector<mapping_and_addr> mappings_by_addr;
std::vector<mapping_and_file_info> mappings_by_file;

// The signal handlers which our handlers replaced, if any, for forwarding
// signals for segfaults outside of our encrypted pages
struct sigaction old_segv;
struct sigaction old_bus;

void signal_handler(int code, siginfo_t* info, void* ctx) {
    SpinLockGuard lock{mapping_lock};
    for (auto m : mappings_by_addr) {
        if (m.addr > info->si_addr || (char*)m.addr + m.size <= info->si_addr)
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

    new EncryptedFileMapping{fd, addr, size, access, encryption_key};
}

void remove_mapping(void* addr, size_t size) {
    SpinLockGuard lock{mapping_lock};
    if (auto m = find_mapping_for_addr(addr, size)) {
        auto pos = m - &mappings_by_addr[0];
        delete m->mapping;
        mappings_by_addr.erase(mappings_by_addr.begin() + pos);
        mappings_by_file.erase(mappings_by_file.begin() + pos);
    }
}

#pragma mark Encryption

size_t pad_to_aes_block_size(size_t len) {
    return (len + aes_block_size - 1) & ~(aes_block_size - 1);
}

AESCryptor::AESCryptor(const uint8_t* key) {
#ifdef __APPLE__
    CCCryptorCreate(kCCEncrypt, kCCAlgorithmAES, 0 /* options */, key, kCCKeySizeAES256, 0 /* IV */, &m_encr);
    CCCryptorCreate(kCCDecrypt, kCCAlgorithmAES, 0 /* options */, key, kCCKeySizeAES256, 0 /* IV */, &m_decr);
#else
    AES_set_encrypt_key(key, 256 /* key size in bits */, &m_ectx);
    AES_set_decrypt_key(key, 256 /* key size in bits */, &m_dctx);
#endif
}

AESCryptor::~AESCryptor() {
#ifdef __APPLE__
    CCCryptorRelease(m_encr);
    CCCryptorRelease(m_decr);
#endif
}

// We have the following constraints here:
//
// 1. When writing, we only know which 4k page is dirty, and not what bytes
//    within the page are dirty, so we always have to write in 4k blocks.
// 2. Pages being written need to be entirely within an 8k-aligned block to
//    ensure that they're written to the hardware in atomic blocks.
// 3. We need to store the IV used for each 4k page somewhere, so that we can
//    ensure that we never reuse an IV (and still be decryptable).
//
// Because pages need to be aligned, we can't just prepend the IV to each page,
// or we'd have to double the size of the file (as the rest of the 4k block
// containing the IV would not be usable). Writing the IVs to a different part
// of the file from the data results in them not being in the same 8k block, and
// so it is possible that only the IV or only the data actually gets updated on
// disk. We deal with this by storing four pieces of data about each page: the
// hash of the encrypted data, the current IV, the hash of the previous encrypted
// data, and the previous IV. To write, we encrypt the data, hash the ciphertext,
// then write the new IV/ciphertext hash, fsync(), and then write the new
// ciphertext. This ensures that if an error occurs between writing the IV and
// the ciphertext, we can still determine that we should use the old IV, since
// the ciphertext's hash will match the old ciphertext.

struct iv_table {
    char iv1[4];
    char hash1[4];
    char iv2[4];
    char hash2[4];
};

static const int metadata_size = sizeof(iv_table);
static const int pages_per_metadata_page = page_size / metadata_size;

// map an offset in the data to the actual location in the file
template<typename Int>
Int real_offset(Int pos) {
    const auto page_index = pos / page_size;
    const auto metadata_page_count = page_index / pages_per_metadata_page + 1;
    return pos + metadata_page_count * page_size;
}

// map a location in the file to the offset in the data
template<typename Int>
Int fake_offset(Int pos) {
    const auto page_index = pos / page_size;
    const auto metadata_page_count = (page_index + pages_per_metadata_page) / (pages_per_metadata_page + 1);
    return pos - metadata_page_count * page_size;
}

// get the location of the iv_table for the given data (not file) position
off_t iv_table_pos(off_t pos) {
    const auto page_index = pos / page_size;
    const auto metadata_block = page_index / pages_per_metadata_page;
    const auto metadata_index = page_index & (pages_per_metadata_page - 1);
    return metadata_block * (pages_per_metadata_page + 1) * page_size + metadata_index * metadata_size;
}

iv_table AESCryptor::read_iv_table(int fd, off_t data_pos) {
    off_t pos = iv_table_pos(data_pos);
    lseek(fd, pos, SEEK_SET);
    iv_table iv;
    memset(&iv, 0, sizeof(iv));
    ::read(fd, &iv, sizeof(iv));
    return iv;
}

void AESCryptor::read(int fd, off_t pos, char* dst, size_t size) {
    iv_table iv = read_iv_table(fd, pos);

    char buffer[page_size];
    lseek(fd, real_offset(pos), SEEK_SET);
    ssize_t bytes_read = ::read(fd, buffer, pad_to_aes_block_size(size));

    if (memcmp(buffer, iv.hash1, 4) != 0) {
        // we had an interrupted write and the IV was bumped but the data not
        // updated, so un-bump the IV
        memcpy(iv.iv1, iv.iv2, 8);
        lseek(fd, iv_table_pos(pos), SEEK_SET);
        ::write(fd, &iv, sizeof(iv));
    }

    crypt(mode_Decrypt, pos, dst, buffer, bytes_read, iv.iv1);
}

void AESCryptor::write(int fd, off_t pos, const char* src, size_t size) {
    iv_table iv = read_iv_table(fd, pos);

    memcpy(iv.iv2, iv.iv1, 8);
    size_t bytes;
    char buffer[page_size];
    do {
        ++*reinterpret_cast<uint32_t*>(iv.iv1);

        bytes = crypt(mode_Encrypt, pos, buffer, src, size, iv.iv1);
        // Just using the prefix as the hash should be fine since it's ciphertext
        memcpy(iv.hash1, buffer, 4);
        // If both the old and new have the same prefix we won't know which IV
        // to use, so keep bumping the new IV until they're different
    } while (TIGHTDB_UNLIKELY(memcmp(iv.hash1, iv.hash2, 4) == 0));

    lseek(fd, iv_table_pos(pos), SEEK_SET);
    ::write(fd, &iv, sizeof(iv));

    lseek(fd, real_offset(pos), SEEK_SET);
    ::write(fd, buffer, bytes);
}

size_t AESCryptor::crypt(EncryptionMode mode, off_t pos, char* dst, const char* src, size_t len, const char* stored_iv) {
    TIGHTDB_ASSERT(len <= page_size);

    char buffer[page_size] = {0};
    // if source len isn't a multiple of the block size, pad it with zeroes
    // we don't store the real size anywhere and rely on that the things
    // using this are okay with too-large files
    if (len & (aes_block_size - 1)) {
        auto padded_len = pad_to_aes_block_size(len);
        memcpy(buffer, src, len);
        memset(buffer + len, 0, padded_len - len);
        src = buffer;
        len = padded_len;
    }

    uint8_t iv[aes_block_size] = {0};
    memcpy(iv, stored_iv, 4);
    memcpy(iv + 4, &pos, sizeof(pos));

#ifdef __APPLE__
    auto cryptor = mode == mode_Encrypt ? m_encr : m_decr;
    CCCryptorReset(cryptor, iv);

    size_t bytesEncrypted = 0;
    auto err = CCCryptorUpdate(cryptor, src, len, dst, sizeof(buffer), &bytesEncrypted);
    TIGHTDB_ASSERT(err == kCCSuccess);
    TIGHTDB_ASSERT(bytesEncrypted == len);
    static_cast<void>(bytesEncrypted);
    static_cast<void>(err);
#else
    AES_cbc_encrypt(src, dst, len, mode == mode_Encrypt ? &m_ectx : &m_dctx, iv, mode);
#endif
    return len;
}

EncryptedFileMapping::EncryptedFileMapping(int fd, void* addr, size_t size, File::AccessMode access, const uint8_t* key)
: m_fd(dup(fd))
, m_access(access)
, m_cryptor(key)
{
    set(addr, size);

    struct stat st;
    if (fstat(fd, &st)) TIGHTDB_TERMINATE("fstat failed");
    m_inode = st.st_ino;
    m_device = st.st_dev;

    mappings_by_file.push_back(mapping_and_file_info{this, m_device, m_inode});
    mappings_by_addr.push_back(mapping_and_addr{this, addr, size});
}

EncryptedFileMapping::~EncryptedFileMapping() {
    flush();
    sync();
    ::close(m_fd);
}

bool EncryptedFileMapping::same_file(mapping_and_file_info m) const {
    return m.mapping != this && m.inode == m_inode && m.device == m_device;
}

char* EncryptedFileMapping::page_addr(size_t i) const {
    return reinterpret_cast<char*>(((m_first_page + i) * page_size));
}

size_t EncryptedFileMapping::actual_page_size(size_t i) const {
    if (i < m_page_count - 1)
        return page_size;
    return std::min<size_t>(page_size, m_size - (page_addr(i) - (char*)m_addr));
}

void EncryptedFileMapping::mark_unreadable(size_t i) {
    mprotect(page_addr(i), page_size, PROT_NONE);
    m_read_pages[i] = false;
    m_dirty_pages[i] = false;
}

void EncryptedFileMapping::mark_readable(size_t i) {
    mprotect(page_addr(i), page_size, PROT_READ);
    m_read_pages[i] = true;
    m_dirty_pages[i] = false;
}

void EncryptedFileMapping::flush_page(size_t i) {
    if (i <= m_page_count && m_dirty_pages[i])
        flush(); // have to flush all pages for ACID guarantess
}

void EncryptedFileMapping::read_page(size_t i) {
    bool has_copied = false;
    auto addr = page_addr(i);
    mprotect(addr, page_size, PROT_READ | PROT_WRITE);

    for (auto m : mappings_by_file) {
        if (same_file(m) && i < m.mapping->m_page_count) {
            m.mapping->flush_page(i);
            if (!has_copied && m.mapping->m_read_pages[i] && actual_page_size(i) == m.mapping->actual_page_size(i)) {
                memcpy(addr, m.mapping->page_addr(i), page_size);
                has_copied = true;
            }
        }
    }

    if (!has_copied)
        m_cryptor.read(m_fd, i * page_size, (char*)addr, actual_page_size(i));

    mark_readable(i);
}

void EncryptedFileMapping::write_page(size_t i) {
    for (auto m : mappings_by_file) {
        if (same_file(m) && i < m.mapping->m_page_count) {
            m.mapping->flush_page(i);
            m.mapping->mark_unreadable(i);
        }
    }

    mprotect(page_addr(i), page_size, PROT_READ | PROT_WRITE);
    m_dirty_pages[i] = true;
}

void EncryptedFileMapping::validate_page(size_t i) {
#ifdef TIGHTDB_DEBUG
    if (!m_read_pages[i]) return;

    char buffer[page_size];
    m_cryptor.read(m_fd, i * page_size, buffer, sizeof(buffer));
    if (memcmp(buffer, page_addr(i), actual_page_size(i))) {
        printf("mismatch %p: fd(%d) page(%zu/%zu) page_size(%zu) %s %s\n",
               this, m_fd, i, m_page_count, actual_page_size(i), buffer, page_addr(i));
        TIGHTDB_TERMINATE("");
    }
#else
    static_cast<void>(i);
#endif
}

void EncryptedFileMapping::validate() {
#ifdef TIGHTDB_DEBUG
    for (size_t i = 0; i < m_page_count; ++i)
        validate_page(i);
#endif
}

void EncryptedFileMapping::flush() {
    for (size_t i = 0; i < m_page_count; ++i) {
        if (!m_dirty_pages[i]) {
            validate_page(i);
            continue;
        }

        mark_readable(i);
        m_cryptor.write(m_fd, i * page_size, (char*)page_addr(i), actual_page_size(i));
    }

    validate();
}

void EncryptedFileMapping::sync() {
    fsync(m_fd);
}

void EncryptedFileMapping::handle_access(void* addr) TIGHTDB_NOEXCEPT {
    auto accessed_page = (uintptr_t)addr / page_size;

    size_t idx = accessed_page - m_first_page;
    if (!m_read_pages[idx]) {
        read_page(idx);
    }
    else if (m_access == File::access_ReadWrite) {
        write_page(idx);
    }
    else {
        TIGHTDB_TERMINATE("Attempt to write to read-only memory");
    }
}

void EncryptedFileMapping::set(void* new_addr, size_t new_size) {
    // Happens if mremap() was called where the old size and new size need the
    // same number of pages
    if (new_addr == m_addr) {
        TIGHTDB_ASSERT((new_size + page_size - 1) / page_size == m_page_count);
        m_size = new_size;
        return;
    }

    flush();
    m_addr = new_addr;
    m_size = new_size;

    m_first_page = (uintptr_t)m_addr / page_size;
    m_page_count = (m_size + page_size - 1)  / page_size;

    m_read_pages.clear();
    m_dirty_pages.clear();

    m_read_pages.resize(m_page_count, false);
    m_dirty_pages.resize(m_page_count, false);
}

size_t round_up_to_page_size(size_t size) {
    return (size + page_size - 1) & ~(page_size - 1);
}

void* mmap_anon(size_t size) {
    size = round_up_to_page_size(size);
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

#pragma mark Public API

namespace tightdb {
namespace util {

void* mmap(int fd, size_t size, File::AccessMode access, const uint8_t* encryption_key) {
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
        SpinLockGuard lock{mapping_lock};
        if (auto m = find_mapping_for_addr(old_addr, old_size)) {
            if (round_up_to_page_size(old_size) == round_up_to_page_size(new_size)) {
                m->mapping->set(old_addr, new_size);
                m->size = new_size;
                return old_addr;
            }

            auto new_addr = mmap_anon(new_size);
            m->mapping->set(new_addr, new_size);
            ::munmap(old_addr, old_size);
            m->addr = new_addr;
            m->size = new_size;
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

File::SizeType encrypted_size_to_data_size(File::SizeType size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    return fake_offset(size);
#else
    return size;
#endif
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) {
#ifdef TIGHTDB_ENABLE_ENCRYPTION
    return real_offset(size);
#else
    return size;
#endif
}

}
}
