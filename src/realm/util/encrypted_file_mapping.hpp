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

#ifndef REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
#define REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP

#include <realm/util/file.hpp>

#ifdef REALM_ENABLE_ENCRYPTION

#include <vector>

#if REALM_PLATFORM_APPLE
#  include <CommonCrypto/CommonCrypto.h>
#elif !REALM_PLATFORM_WINDOWS
#  include <openssl/aes.h>
#  include <openssl/sha.h>
#else
#error Encryption is not yet implemented for this platform.
#endif

namespace realm {
namespace util {

struct iv_table;

class AESCryptor {
public:
    AESCryptor(const uint8_t* key);
    ~AESCryptor() noexcept;

    void set_file_size(off_t new_size);

    bool try_read(int fd, off_t pos, char* dst, size_t size);
    bool read(int fd, off_t pos, char* dst, size_t size) noexcept;
    void write(int fd, off_t pos, const char* src, size_t size) noexcept;

private:
    enum EncryptionMode {
#if REALM_PLATFORM_APPLE
        mode_Encrypt = kCCEncrypt,
        mode_Decrypt = kCCDecrypt
#else
        mode_Encrypt = AES_ENCRYPT,
        mode_Decrypt = AES_DECRYPT
#endif
    };

#if REALM_PLATFORM_APPLE
    CCCryptorRef m_encr;
    CCCryptorRef m_decr;
#else
    AES_KEY m_ectx;
    AES_KEY m_dctx;
#endif

    uint8_t m_hmacKey[32];
    std::vector<iv_table> m_iv_buffer;
    std::unique_ptr<char[]> m_rw_buffer;

    void calc_hmac(const void* src, size_t len, uint8_t* dst, const uint8_t* key) const;
    bool check_hmac(const void *data, size_t len, const uint8_t *hmac) const;
    void crypt(EncryptionMode mode, off_t pos, char* dst, const char* src,
               const char* stored_iv) noexcept;
    iv_table& get_iv_table(int fd, off_t data_pos) noexcept;
};

class EncryptedFileMapping;

struct SharedFileInfo {
    int fd;
    AESCryptor cryptor;
    std::vector<EncryptedFileMapping*> mappings;

    SharedFileInfo(const uint8_t* key, int fd);
};

class EncryptedFileMapping {
public:
    // Adds the newly-created object to file.mappings iff it's successfully constructed
    EncryptedFileMapping(SharedFileInfo& file, size_t file_offset, 
                         void* addr, size_t size, File::AccessMode access);
    ~EncryptedFileMapping();

    // Write all dirty pages to disk and mark them read-only
    // Does not call fsync
    void flush() noexcept;

    // Sync this file to disk
    void sync() noexcept;

    // Handle a SEGV or BUS at the given address, which must be within this
    // object's mapping
    void handle_access(void* addr) noexcept;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size, size_t new_file_offset);

private:
    SharedFileInfo& m_file;

    size_t m_page_size;
    size_t m_blocks_per_page;

    void* m_addr = nullptr;
    size_t m_file_offset = 0;

    uintptr_t m_first_page;
    size_t m_page_count = 0;

    std::vector<bool> m_read_pages;
    std::vector<bool> m_write_pages;
    std::vector<bool> m_dirty_pages;

    File::AccessMode m_access;

#ifdef REALM_DEBUG
    std::unique_ptr<char[]> m_validate_buffer;
#endif

    char* page_addr(size_t i) const noexcept;

    void mark_unreadable(size_t i) noexcept;
    void mark_readable(size_t i) noexcept;
    void mark_unwritable(size_t i) noexcept;

    bool copy_read_page(size_t i) noexcept;
    void read_page(size_t i) noexcept;
    void write_page(size_t i) noexcept;

    void validate_page(size_t i) noexcept;
    void validate() noexcept;
};

}
}

#endif // REALM_ENABLE_ENCRYPTION

namespace realm {
namespace util {

/// Thrown by EncryptedFileMapping if a file opened is non-empty and does not
/// contain valid encrypted data
struct DecryptionFailed: util::File::AccessError {
    DecryptionFailed(): util::File::AccessError("Decryption failed", std::string()) {}
};

}
}

#endif // REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
