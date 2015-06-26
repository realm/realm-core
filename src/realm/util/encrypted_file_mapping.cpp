/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
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
#include "encrypted_file_mapping.hpp"

#ifdef REALM_ENABLE_ENCRYPTION
#include <cstdlib>
#include <iostream>
#include <algorithm>

#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

#include <realm/util/terminate.hpp>

namespace {
template<typename T>
void dlsym_cast(T& ptr, const char *name) {
    void* addr = dlsym(RTLD_DEFAULT, name);
    REALM_ASSERT(addr);
    // This cast is forbidden by C++03, but required to work by POSIX, and
    // C++11 makes it implementation-defined specifically to support dlsym
    ptr = reinterpret_cast<T>(reinterpret_cast<size_t>(addr));
}

} // anonymous namespace

namespace realm {
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

#if defined(__linux__)
    // libcrypto isn't exposed as part of the NDK, but it happens to be loaded
    // into every process with every version of Android, so we can get to it
    // with dlsym
    // FIXME: Don't know where to write the following info:
    // on linux, we add -ldl to the linking step to get dlsym to work,
    // and set LD_PRELOAD to the location of libcrypto when running the executable.
    // (on linux mint this is currently: "LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libcrypto.so")
    // To be able to debug with gdb, use the command "handle SIGSEGV noprint"
    // before running the program.
    dlsym_cast(AES_set_encrypt_key, "AES_set_encrypt_key");
    dlsym_cast(AES_set_decrypt_key, "AES_set_decrypt_key");
    dlsym_cast(AES_cbc_encrypt, "AES_cbc_encrypt");

    dlsym_cast(SHA224_Init, "SHA224_Init");
    dlsym_cast(SHA256_Update, "SHA256_Update");
    dlsym_cast(SHA256_Final, "SHA256_Final");
#endif
    AES_set_encrypt_key(key, 256 /* key size in bits */, &m_ectx);
    AES_set_decrypt_key(key, 256 /* key size in bits */, &m_dctx);
#endif
    memcpy(m_hmacKey, key + 32, 32);
}

AESCryptor::~AESCryptor() REALM_NOEXCEPT {
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
    uint32_t iv1;
    uint8_t hmac1[28];
    uint32_t iv2;
    uint8_t hmac2[28];
};

namespace {
const int aes_block_size = 16;
const size_t block_size = 4096;

const size_t metadata_size = sizeof(iv_table);
const size_t blocks_per_metadata_block = block_size / metadata_size;

// map an offset in the data to the actual location in the file
template<typename Int>
Int real_offset(Int pos)
{
    REALM_ASSERT(pos >= 0);
    const size_t index = static_cast<size_t>(pos) / block_size;
    const size_t metadata_page_count = index / blocks_per_metadata_block + 1;
    return pos + metadata_page_count * block_size;
}

// map a location in the file to the offset in the data
template<typename Int>
Int fake_offset(Int pos)
{
    REALM_ASSERT(pos >= 0);
    const size_t index = static_cast<size_t>(pos) / block_size;
    const size_t metadata_page_count = (index + blocks_per_metadata_block) / (blocks_per_metadata_block + 1);
    return pos - metadata_page_count * block_size;
}

// get the location of the iv_table for the given data (not file) position
off_t iv_table_pos(off_t pos)
{
    REALM_ASSERT(pos >= 0);
    const size_t index = static_cast<size_t>(pos) / block_size;
    const size_t metadata_block = index / blocks_per_metadata_block;
    const size_t metadata_index = index & (blocks_per_metadata_block - 1);
    return metadata_block * (blocks_per_metadata_block + 1) * block_size + metadata_index * metadata_size;
}

void check_write(int fd, off_t pos, const void *data, size_t len)
{
    ssize_t ret = pwrite(fd, data, len, pos);
    REALM_ASSERT(ret >= 0 && static_cast<size_t>(ret) == len);
    static_cast<void>(ret);
}

size_t check_read(int fd, off_t pos, void *dst, size_t len)
{
    ssize_t ret = pread(fd, dst, len, pos);
    REALM_ASSERT(ret >= 0);
    return ret < 0 ? 0 : static_cast<size_t>(ret);
}

} // anonymous namespace

void AESCryptor::set_file_size(off_t new_size)
{
    REALM_ASSERT(new_size >= 0);
    size_t block_count = (new_size + block_size - 1) / block_size;
    m_iv_buffer.reserve((block_count + blocks_per_metadata_block - 1) & ~(blocks_per_metadata_block - 1));
}

iv_table& AESCryptor::get_iv_table(int fd, off_t data_pos) REALM_NOEXCEPT
{
    size_t idx = data_pos / block_size;
    if (idx < m_iv_buffer.size())
        return m_iv_buffer[idx];

    size_t old_size = m_iv_buffer.size();
    size_t new_block_count = 1 + idx / blocks_per_metadata_block;
    REALM_ASSERT(new_block_count * blocks_per_metadata_block <= m_iv_buffer.capacity()); // not safe to allocate here
    m_iv_buffer.resize(new_block_count * blocks_per_metadata_block);

    for (size_t i = old_size; i < new_block_count * blocks_per_metadata_block; i += blocks_per_metadata_block) {
        size_t bytes = check_read(fd, iv_table_pos(i * block_size), &m_iv_buffer[i], block_size);
        if (bytes < block_size)
            break; // rest is zero-filled by resize()
    }

    return m_iv_buffer[idx];
}

bool AESCryptor::check_hmac(const void *src, size_t len, const uint8_t *hmac) const
{
    uint8_t buffer[224 / 8];
    calc_hmac(src, len, buffer, m_hmacKey);

    // Constant-time memcmp to avoid timing attacks
    uint8_t result = 0;
    for (size_t i = 0; i < 224/8; ++i)
        result |= buffer[i] ^ hmac[i];
    return result == 0;
}

bool AESCryptor::read(int fd, off_t pos, char* dst, size_t size) REALM_NOEXCEPT
{
    try {
        return try_read(fd, pos, dst, size);
    }
    catch (...) {
        // Not recoverable since we're running in a signal handler
        REALM_TERMINATE("corrupted database");
    }
}

bool AESCryptor::try_read(int fd, off_t pos, char* dst, size_t size) {
    REALM_ASSERT(size % block_size == 0);
    while (size > 0) {
        char buffer[block_size];
        ssize_t bytes_read = check_read(fd, real_offset(pos), buffer, block_size);

        if (bytes_read == 0)
            return false;

        iv_table& iv = get_iv_table(fd, pos);
        if (iv.iv1 == 0) {
            // This block has never been written to, so we've just read pre-allocated
            // space. No memset() since the code using this doesn't rely on
            // pre-allocated space being zeroed.
            return false;
        }

        if (!check_hmac(buffer, bytes_read, iv.hmac1)) {
            // Either the DB is corrupted or we were interrupted between writing the
            // new IV and writing the data
            if (iv.iv2 == 0) {
                // Very first write was interrupted
                return false;
            }

            if (check_hmac(buffer, bytes_read, iv.hmac2)) {
                // Un-bump the IV since the write with the bumped IV never actually
                // happened
                memcpy(&iv.iv1, &iv.iv2, 32);
            }
            else {
                // If the file has been shrunk and then re-expanded, we may have
                // old hmacs that don't go with this data. ftruncate() is
                // required to fill any added space with zeroes, so assume that's
                // what happened if the buffer is all zeroes
                for (ssize_t i = 0; i < bytes_read; ++i) {
                    if (buffer[i] != 0)
                        throw DecryptionFailed();
                }
                return false;
            }
        }

        crypt(mode_Decrypt, pos, dst, buffer, reinterpret_cast<const char*>(&iv.iv1));

        pos += block_size;
        dst += block_size;
        size -= block_size;
    }
    return true;
}

void AESCryptor::write(int fd, off_t pos, const char* src, size_t size) REALM_NOEXCEPT
{
    REALM_ASSERT(size % block_size == 0);
    while (size > 0) {
        iv_table& iv = get_iv_table(fd, pos);

        memcpy(&iv.iv2, &iv.iv1, 32);
        char buffer[block_size];
        do {
            ++iv.iv1;
            // 0 is reserved for never-been-used, so bump if we just wrapped around
            if (iv.iv1 == 0)
                ++iv.iv1;

            crypt(mode_Encrypt, pos, buffer, src, reinterpret_cast<const char*>(&iv.iv1));
            calc_hmac(buffer, block_size, iv.hmac1, m_hmacKey);
            // In the extremely unlikely case that both the old and new versions have
            // the same hash we won't know which IV to use, so bump the IV until
            // they're different.
        } while (REALM_UNLIKELY(memcmp(iv.hmac1, iv.hmac2, 4) == 0));

        check_write(fd, iv_table_pos(pos), &iv, sizeof(iv));
        check_write(fd, real_offset(pos), buffer, block_size);

        pos += block_size;
        src += block_size;
        size -= block_size;
    }
}

void AESCryptor::crypt(EncryptionMode mode, off_t pos, char* dst,
                         const char* src, const char* stored_iv) REALM_NOEXCEPT
{
    uint8_t iv[aes_block_size] = {0};
    memcpy(iv, stored_iv, 4);
    memcpy(iv + 4, &pos, sizeof(pos));

#ifdef __APPLE__
    CCCryptorRef cryptor = mode == mode_Encrypt ? m_encr : m_decr;
    CCCryptorReset(cryptor, iv);

    size_t bytesEncrypted = 0;
    CCCryptorStatus err = CCCryptorUpdate(cryptor, src, block_size, dst, block_size, &bytesEncrypted);
    REALM_ASSERT(err == kCCSuccess);
    REALM_ASSERT(bytesEncrypted == block_size);
    static_cast<void>(bytesEncrypted);
    static_cast<void>(err);
#else
    AES_cbc_encrypt(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(dst),
                    block_size, mode == mode_Encrypt ? &m_ectx : &m_dctx, iv, mode);
#endif
}

void AESCryptor::calc_hmac(const void* src, size_t len, uint8_t* dst, const uint8_t* key) const
{
#ifdef __APPLE__
    CCHmac(kCCHmacAlgSHA224, key, 32, src, len, dst);
#else
    SHA256_CTX ctx;

    uint8_t ipad[64];
    for (size_t i = 0; i < 32; ++i)
        ipad[i] = key[i] ^ 0x36;
    memset(ipad + 32, 0x36, 32);

    uint8_t opad[64] = {0};
    for (size_t i = 0; i < 32; ++i)
        opad[i] = key[i] ^ 0x5C;
    memset(opad + 32, 0x5C, 32);

    // Full hmac operation is sha224(opad + sha224(ipad + data))
    SHA224_Init(&ctx);
    SHA256_Update(&ctx, ipad, 64);
    SHA256_Update(&ctx, static_cast<const uint8_t*>(src), len);
    SHA256_Final(dst, &ctx);

    SHA224_Init(&ctx);
    SHA256_Update(&ctx, opad, 64);
    SHA256_Update(&ctx, dst, SHA224_DIGEST_LENGTH);
    SHA256_Final(dst, &ctx);
#endif
}

EncryptedFileMapping::EncryptedFileMapping(SharedFileInfo& file, void* addr, size_t size, File::AccessMode access)
: m_file(file)
, m_page_size(realm::util::page_size())
, m_blocks_per_page(m_page_size / block_size)
, m_addr(0)
, m_size(0)
, m_page_count(0)
, m_access(access)
#ifdef REALM_DEBUG
, m_validate_buffer(new char[m_page_size])
#endif
{
    REALM_ASSERT(m_blocks_per_page * block_size == m_page_size);
    set(addr, size); // throws
    file.mappings.push_back(this);
}

EncryptedFileMapping::~EncryptedFileMapping()
{
    flush();
    sync();
    m_file.mappings.erase(remove(m_file.mappings.begin(), m_file.mappings.end(), this));
}

char* EncryptedFileMapping::page_addr(size_t i) const REALM_NOEXCEPT
{
    return reinterpret_cast<char*>(((m_first_page + i) * m_page_size));
}

void EncryptedFileMapping::mark_unreadable(size_t i) REALM_NOEXCEPT
{
    if (i >= m_page_count)
        return;

    if (m_dirty_pages[i])
        flush();

    if (m_read_pages[i]) {
        mprotect(page_addr(i), m_page_size, PROT_NONE);
        m_read_pages[i] = false;
    }
}

void EncryptedFileMapping::mark_readable(size_t i) REALM_NOEXCEPT
{
    if (i >= m_read_pages.size() || (m_read_pages[i] && !m_write_pages[i]))
        return;

    mprotect(page_addr(i), m_page_size, PROT_READ);
    m_read_pages[i] = true;
    m_write_pages[i] = false;
}

void EncryptedFileMapping::mark_unwritable(size_t i) REALM_NOEXCEPT
{
    if (i >= m_write_pages.size() || !m_write_pages[i])
        return;

    REALM_ASSERT(m_read_pages[i]);
    mprotect(page_addr(i), m_page_size, PROT_READ);
    m_write_pages[i] = false;
    // leave dirty bit set
}

bool EncryptedFileMapping::copy_read_page(size_t page) REALM_NOEXCEPT
{
    for (size_t i = 0; i < m_file.mappings.size(); ++i) {
        EncryptedFileMapping* m = m_file.mappings[i];
        if (m == this || page >= m->m_page_count)
            continue;

        m->mark_unwritable(page);
        if (m->m_read_pages[page]) {
            memcpy(page_addr(page), m->page_addr(page), m_page_size);
            return true;
        }
    }
    return false;
}

void EncryptedFileMapping::read_page(size_t i) REALM_NOEXCEPT
{
    char* addr = page_addr(i);
    mprotect(addr, m_page_size, PROT_READ | PROT_WRITE);

    if (!copy_read_page(i))
        m_file.cryptor.read(m_file.fd, i * m_page_size, addr, m_page_size);

    mprotect(page_addr(i), m_page_size, PROT_READ);
    m_read_pages[i] = true;
    m_write_pages[i] = false;
}

void EncryptedFileMapping::write_page(size_t page) REALM_NOEXCEPT
{
    for (size_t i = 0; i < m_file.mappings.size(); ++i) {
        EncryptedFileMapping* m = m_file.mappings[i];
        if (m != this)
            m->mark_unreadable(page);
    }

    mprotect(page_addr(page), m_page_size, PROT_READ | PROT_WRITE);
    m_write_pages[page] = true;
    m_dirty_pages[page] = true;
}

void EncryptedFileMapping::validate_page(size_t page) REALM_NOEXCEPT
{
#ifdef REALM_DEBUG
    if (!m_read_pages[page])
        return;

    if (!m_file.cryptor.read(m_file.fd, page * m_page_size, m_validate_buffer.get(), m_page_size))
        return;

    for (size_t i = 0; i < m_file.mappings.size(); ++i) {
        EncryptedFileMapping* m = m_file.mappings[i];
        if (m != this && page < m->m_page_count && m->m_dirty_pages[page]) {
            memcpy(m_validate_buffer.get(), m->page_addr(page), m_page_size);
            break;
        }
    }

    if (memcmp(m_validate_buffer.get(), page_addr(page), m_page_size)) {
        std::cerr << "mismatch " << this << ": fd(" << m_file.fd << ") page("
                  << page << "/" << m_page_count << ") " << m_validate_buffer.get() << " "
                  << page_addr(page) << std::endl;
        REALM_TERMINATE("");
    }
#else
    static_cast<void>(page);
#endif
}

void EncryptedFileMapping::validate() REALM_NOEXCEPT
{
#ifdef REALM_DEBUG
    for (size_t i = 0; i < m_page_count; ++i)
        validate_page(i);
#endif
}

void EncryptedFileMapping::flush() REALM_NOEXCEPT
{
    size_t start = 0;
    for (size_t i = 0; i < m_page_count; ++i) {
        if (!m_read_pages[i]) {
            if (start < i)
                mprotect(page_addr(start), (i - start) * m_page_size, PROT_READ);
            start = i + 1;
        }
        else if (start == i && !m_write_pages[i])
            start = i + 1;

        if (!m_dirty_pages[i]) {
            validate_page(i);
            continue;
        }

        m_file.cryptor.write(m_file.fd, i * m_page_size, page_addr(i), m_page_size);
        m_dirty_pages[i] = false;
        m_write_pages[i] = false;
    }
    if (start < m_page_count)
        mprotect(page_addr(start), (m_page_count - start) * m_page_size, PROT_READ);

    validate();
}

void EncryptedFileMapping::sync() REALM_NOEXCEPT
{
    fsync(m_file.fd);
    // FIXME: on iOS/OSX fsync may not be enough to ensure crash safety.
    // Consider adding fcntl(F_FULLFSYNC). This most likely also applies to msync.
    //
    // See description of fsync on iOS here:
    // https://developer.apple.com/library/ios/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html
    //
    // See also
    // https://developer.apple.com/library/ios/documentation/Cocoa/Conceptual/CoreData/Articles/cdPersistentStores.html
    // for a discussion of this related to core data.
}

void EncryptedFileMapping::handle_access(void* addr) REALM_NOEXCEPT
{
    size_t accessed_page = reinterpret_cast<uintptr_t>(addr) / m_page_size;

    size_t idx = accessed_page - m_first_page;
    if (!m_read_pages[idx]) {
        read_page(idx);
    }
    else if (m_access == File::access_ReadWrite) {
        write_page(idx);
    }
    else {
        REALM_TERMINATE("Attempt to write to read-only memory");
    }
}

void EncryptedFileMapping::set(void* new_addr, size_t new_size)
{
    m_file.cryptor.set_file_size(new_size);
    REALM_ASSERT(new_size % m_page_size == 0);
    REALM_ASSERT(new_size > 0);

    bool first_init = m_addr == 0;

    flush();
    m_addr = new_addr;
    m_size = new_size;

    m_first_page = reinterpret_cast<uintptr_t>(m_addr) / m_page_size;
    m_page_count = m_size / m_page_size;

    m_read_pages.clear();
    m_write_pages.clear();
    m_dirty_pages.clear();

    m_read_pages.resize(m_page_count, false);
    m_write_pages.resize(m_page_count, false);
    m_dirty_pages.resize(m_page_count, false);

    if (first_init) {
        if (!copy_read_page(0))
            m_file.cryptor.try_read(m_file.fd, 0, page_addr(0), m_page_size);
        mark_readable(0);
        if (m_page_count > 0)
            mprotect(page_addr(1), (m_page_count - 1) * m_page_size, PROT_NONE);
    }
    else
        mprotect(m_addr, m_page_count * m_page_size, PROT_NONE);
}

File::SizeType encrypted_size_to_data_size(File::SizeType size) REALM_NOEXCEPT
{
    if (size == 0)
        return 0;
    return fake_offset(size);
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) REALM_NOEXCEPT
{
    size_t ps = page_size();
    return real_offset((size + ps - 1) & ~(ps - 1));
}

#else

namespace realm {
namespace util {

File::SizeType encrypted_size_to_data_size(File::SizeType size) REALM_NOEXCEPT
{
    return size;
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) REALM_NOEXCEPT
{
    return size;
}

#endif

} // namespace util {
} // namespace realm {

