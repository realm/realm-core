#include "encrypted_file_mapping.hpp"

#ifdef TIGHTDB_ENABLE_ENCRYPTION
#include <atomic>
#include <cerrno>
#include <cstdlib>

#include <sys/mman.h>
#include <unistd.h>

#include <tightdb/util/terminate.hpp>

namespace tightdb {
namespace util {

SharedFileInfo::SharedFileInfo(const uint8_t* key, int fd)
: fd(fd), cryptor(key)
{
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

AESCryptor::~AESCryptor() TIGHTDB_NOEXCEPT {
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

namespace {
const int aes_block_size = 16;
const int page_size = 4096;

size_t pad_to_aes_block_size(size_t len) {
    return (len + aes_block_size - 1) & ~(aes_block_size - 1);
}

const int metadata_size = sizeof(iv_table);
const int pages_per_metadata_page = page_size / metadata_size;

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

void check_write(int fd, off_t pos, const void *data, size_t len) {
    auto ret = pwrite(fd, data, len, pos);
    TIGHTDB_ASSERT(ret >= 0 && (size_t)ret == len);
    static_cast<void>(ret);
}

ssize_t check_read(int fd, off_t pos, void *dst, size_t len) {
    auto ret = pread(fd, dst, len, pos);
    TIGHTDB_ASSERT(ret >= 0);
    return ret;
}

} // namespace {

void AESCryptor::set_file_size(off_t new_size) {
    auto page_count = (new_size + page_size - 1) / page_size;
    m_iv_buffer.reserve((page_count + pages_per_metadata_page - 1) & ~(pages_per_metadata_page - 1));
}

iv_table& AESCryptor::get_iv_table(int fd, off_t data_pos) TIGHTDB_NOEXCEPT {
    size_t idx = data_pos / page_size;
    if (idx < m_iv_buffer.size())
        return m_iv_buffer[idx];
    TIGHTDB_ASSERT(idx < m_iv_buffer.capacity()); // not safe to allocate here

    auto old_size = m_iv_buffer.size();
    auto new_page_count = 1 + idx / pages_per_metadata_page;
    m_iv_buffer.resize(new_page_count * pages_per_metadata_page);

    for (size_t i = old_size; i < new_page_count * pages_per_metadata_page; i += pages_per_metadata_page) {
        auto bytes = check_read(fd, iv_table_pos(i * page_size), &m_iv_buffer[i], page_size);
        if (bytes < page_size)
            break; // rest is zero-filled by resize()
    }

    return m_iv_buffer[idx];
}

void AESCryptor::read(int fd, off_t pos, char* dst, size_t size) TIGHTDB_NOEXCEPT {
    char buffer[page_size];
    ssize_t bytes_read = check_read(fd, real_offset(pos), buffer, pad_to_aes_block_size(size));

    if (bytes_read == 0)
        return;

    iv_table& iv = get_iv_table(fd, pos);

    if (memcmp(buffer, iv.hash1, 4) != 0) {
        // we had an interrupted write and the IV was bumped but the data not
        // updated, so un-bump the IV
        memcpy(iv.iv1, iv.iv2, 8);
    }

    crypt(mode_Decrypt, pos, dst, buffer, bytes_read, iv.iv1);
}

void AESCryptor::write(int fd, off_t pos, const char* src, size_t size) TIGHTDB_NOEXCEPT {
    iv_table& iv = get_iv_table(fd, pos);

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

    check_write(fd, iv_table_pos(pos), &iv, sizeof(iv));
    check_write(fd, real_offset(pos), buffer, bytes);
}

size_t AESCryptor::crypt(EncryptionMode mode, off_t pos, char* dst,
                         const char* src, size_t len, const char* stored_iv) TIGHTDB_NOEXCEPT {
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

EncryptedFileMapping::EncryptedFileMapping(SharedFileInfo& file, void* addr, size_t size, File::AccessMode access)
: m_file(file)
, m_access(access)
{
    set(addr, size);
    file.mappings.push_back(this);
}

EncryptedFileMapping::~EncryptedFileMapping() {
    flush();
    sync();
    m_file.mappings.erase(remove(m_file.mappings.begin(), m_file.mappings.end(), this));
}

char* EncryptedFileMapping::page_addr(size_t i) const TIGHTDB_NOEXCEPT {
    return reinterpret_cast<char*>(((m_first_page + i) * page_size));
}

size_t EncryptedFileMapping::actual_page_size(size_t i) const TIGHTDB_NOEXCEPT {
    if (i < m_page_count - 1)
        return page_size;
    return std::min<size_t>(page_size, m_size - (page_addr(i) - (char*)m_addr));
}

void EncryptedFileMapping::mark_unreadable(size_t i) TIGHTDB_NOEXCEPT {
    if (i >= m_page_count)
        return;

    if (m_dirty_pages[i])
        flush();

    if (m_read_pages[i]) {
        mprotect(page_addr(i), page_size, PROT_NONE);
        m_read_pages[i] = false;
    }
}

void EncryptedFileMapping::mark_readable(size_t i) TIGHTDB_NOEXCEPT {
    if (i >= m_read_pages.size() || (m_read_pages[i] && !m_write_pages[i]))
        return;

    mprotect(page_addr(i), page_size, PROT_READ);
    m_read_pages[i] = true;
    m_write_pages[i] = false;
}

void EncryptedFileMapping::mark_unwritable(size_t i) TIGHTDB_NOEXCEPT {
    if (i >= m_write_pages.size() || !m_write_pages[i])
        return;

    TIGHTDB_ASSERT(m_read_pages[i]);
    mprotect(page_addr(i), page_size, PROT_READ);
    m_write_pages[i] = false;
    // leave dirty bit set
}

void EncryptedFileMapping::read_page(size_t i) TIGHTDB_NOEXCEPT {
    bool has_copied = false;
    auto addr = page_addr(i);
    mprotect(addr, page_size, PROT_READ | PROT_WRITE);

    for (auto m : m_file.mappings) {
        if (m == this || i >= m->m_page_count)
            continue;

        m->mark_unwritable(i);
        if (!has_copied && m->m_read_pages[i] && (actual_page_size(i) == m->actual_page_size(i) || m->m_dirty_pages[i])) {
            memcpy(addr, m->page_addr(i), page_size);
            has_copied = true;
        }
    }

    if (!has_copied)
        m_file.cryptor.read(m_file.fd, i * page_size, (char*)addr, actual_page_size(i));

    mark_readable(i);
}

void EncryptedFileMapping::write_page(size_t i) TIGHTDB_NOEXCEPT {
    for (auto m : m_file.mappings) {
        if (m != this)
            m->mark_unreadable(i);
    }

    mprotect(page_addr(i), page_size, PROT_READ | PROT_WRITE);
    m_write_pages[i] = true;
    m_dirty_pages[i] = true;
}

void EncryptedFileMapping::validate_page(size_t i) TIGHTDB_NOEXCEPT {
#ifdef TIGHTDB_DEBUG
    if (!m_read_pages[i]) return;

    char buffer[page_size];
    m_file.cryptor.read(m_file.fd, i * page_size, buffer, sizeof(buffer));

    for (auto m : m_file.mappings) {
        if (m != this && i < m->m_page_count && m->m_dirty_pages[i]) {
            memcpy(buffer, m->page_addr(i), page_size);
            break;
        }
    }

    if (memcmp(buffer, page_addr(i), actual_page_size(i))) {
        printf("mismatch %p: fd(%d) page(%zu/%zu) page_size(%zu) %s %s\n",
               this, m_file.fd, i, m_page_count, actual_page_size(i), buffer, page_addr(i));
        TIGHTDB_TERMINATE("");
    }
#else
    static_cast<void>(i);
#endif
}

void EncryptedFileMapping::validate() TIGHTDB_NOEXCEPT {
#ifdef TIGHTDB_DEBUG
    for (size_t i = 0; i < m_page_count; ++i)
        validate_page(i);
#endif
}

void EncryptedFileMapping::flush() TIGHTDB_NOEXCEPT {
    size_t start = 0;
    for (size_t i = 0; i < m_page_count; ++i) {
        if (!m_read_pages[i]) {
            if (start < i)
                mprotect(page_addr(start), (i - start) * page_size, PROT_READ);
            start = i + 1;
        }
        else if (start == i && !m_write_pages[i])
            start = i + 1;

        if (!m_dirty_pages[i]) {
            validate_page(i);
            continue;
        }

        m_file.cryptor.write(m_file.fd, i * page_size, (char*)page_addr(i), actual_page_size(i));
        m_dirty_pages[i] = false;
        m_write_pages[i] = false;
    }
    if (start < m_page_count)
        mprotect(page_addr(start), (m_page_count - start) * page_size, PROT_READ);

    validate();
}

void EncryptedFileMapping::sync() TIGHTDB_NOEXCEPT {
    fsync(m_file.fd);
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
    m_file.cryptor.set_file_size(new_size);

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
    m_write_pages.clear();
    m_dirty_pages.clear();

    m_read_pages.resize(m_page_count, false);
    m_write_pages.resize(m_page_count, false);
    m_dirty_pages.resize(m_page_count, false);
}

File::SizeType encrypted_size_to_data_size(File::SizeType size) TIGHTDB_NOEXCEPT {
    return fake_offset(size);
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) TIGHTDB_NOEXCEPT {
    return real_offset(size);
}

#else

namespace tightdb {
namespace util {

File::SizeType encrypted_size_to_data_size(File::SizeType size) TIGHTDB_NOEXCEPT {
    return size;
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) TIGHTDB_NOEXCEPT {
    return size;
}

#endif

} // namespace util {
} // namespace tightdb {

