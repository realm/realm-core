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

#include <tightdb/util/file.hpp>

#ifdef REALM_ENABLE_ENCRYPTION

#include <vector>

#ifdef __APPLE__
#include <CommonCrypto/CommonCrypto.h>
#elif defined(REALM_ANDROID)
// OpenSSL headers aren't part of the NDK, so declare the bits we need manually
#define AES_ENCRYPT	1
#define AES_DECRYPT	0
#define SHA224_DIGEST_LENGTH 28

typedef struct aes_key_st {
    unsigned long data[61];
} AES_KEY;

typedef struct SHA256state_st {
    unsigned int data[28];
} SHA256_CTX;
#elif !defined(_WIN32)
#include <openssl/aes.h>
#include <openssl/sha.h>
#else
#error Encryption is not yet implemented for this platform.
#endif

namespace realm {
namespace util {

size_t page_size();

struct iv_table;

class AESCryptor {
public:
    AESCryptor(const uint8_t* key);
    ~AESCryptor() REALM_NOEXCEPT;

    void set_file_size(off_t new_size);

    bool try_read(int fd, off_t pos, char* dst, size_t size);
    bool read(int fd, off_t pos, char* dst, size_t size) REALM_NOEXCEPT;
    void write(int fd, off_t pos, const char* src, size_t size) REALM_NOEXCEPT;

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

#ifdef REALM_ANDROID
    // Loaded at runtime with dysym
    int (*AES_set_encrypt_key)(const unsigned char *, const int, AES_KEY *);
    int (*AES_set_decrypt_key)(const unsigned char *, const int, AES_KEY *);
    void (*AES_cbc_encrypt)(const unsigned char *, unsigned char *,
                            const unsigned long, const AES_KEY *,
                            unsigned char *, const int);

    int (*SHA224_Init)(SHA256_CTX *);
    int (*SHA256_Update)(SHA256_CTX *, const void *, size_t);
    int (*SHA256_Final)(unsigned char *, SHA256_CTX *);
#endif

    uint8_t m_hmacKey[32];
    std::vector<iv_table> m_iv_buffer;

    void calc_hmac(const void* src, size_t len, uint8_t* dst, const uint8_t* key) const;
    bool check_hmac(const void *data, size_t len, const uint8_t *hmac) const;
    void crypt(EncryptionMode mode, off_t pos, char* dst, const char* src,
               const char* stored_iv) REALM_NOEXCEPT;
    iv_table& get_iv_table(int fd, off_t data_pos) REALM_NOEXCEPT;
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
    EncryptedFileMapping(SharedFileInfo& file, void* addr, size_t size, File::AccessMode access);
    ~EncryptedFileMapping();

    // Write all dirty pages to disk and mark them read-only
    // Does not call fsync
    void flush() REALM_NOEXCEPT;

    // Sync this file to disk
    void sync() REALM_NOEXCEPT;

    // Handle a SEGV or BUS at the given address, which must be within this
    // object's mapping
    void handle_access(void* addr) REALM_NOEXCEPT;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size);

private:
    SharedFileInfo& m_file;

    size_t m_page_size;
    size_t m_blocks_per_page;

    void* m_addr;
    size_t m_size;

    uintptr_t m_first_page;
    size_t m_page_count;

    std::vector<bool> m_read_pages;
    std::vector<bool> m_write_pages;
    std::vector<bool> m_dirty_pages;

    File::AccessMode m_access;

#ifdef REALM_DEBUG
    UniquePtr<char[]> m_validate_buffer;
#endif

    char* page_addr(size_t i) const REALM_NOEXCEPT;

    void mark_unreadable(size_t i) REALM_NOEXCEPT;
    void mark_readable(size_t i) REALM_NOEXCEPT;
    void mark_unwritable(size_t i) REALM_NOEXCEPT;

    bool copy_read_page(size_t i) REALM_NOEXCEPT;
    void read_page(size_t i) REALM_NOEXCEPT;
    void write_page(size_t i) REALM_NOEXCEPT;

    void validate_page(size_t i) REALM_NOEXCEPT;
    void validate() REALM_NOEXCEPT;
};

}
}

#endif // REALM_ENABLE_ENCRYPTION

namespace realm {
namespace util {

/// Thrown by EncryptedFileMapping if a file opened is non-empty and does not
/// contain valid encrypted data
struct DecryptionFailed: util::File::AccessError {
    DecryptionFailed(): util::File::AccessError("Decryption failed") {}
};

}
}

#endif // REALM_UTIL_ENCRYPTED_FILE_MAPPING_HPP
