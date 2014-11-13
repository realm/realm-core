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

#ifndef TIGHTDB_UTIL_ENCRYPTED_FILE_MAPPING_HPP
#define TIGHTDB_UTIL_ENCRYPTED_FILE_MAPPING_HPP

#include <tightdb/util/file.hpp>

#ifdef TIGHTDB_ENABLE_ENCRYPTION

#include <vector>

#ifdef __APPLE__
#include <CommonCrypto/CommonCrypto.h>
#else
#include <openssl/aes.h>
#include <openssl/sha.h>
#endif

namespace tightdb {
namespace util {

struct iv_table;

class AESCryptor {
public:
    AESCryptor(const uint8_t* key);
    ~AESCryptor() TIGHTDB_NOEXCEPT;

    void set_file_size(off_t new_size);

    void try_read(int fd, off_t pos, char* dst);
    void read(int fd, off_t pos, char* dst) TIGHTDB_NOEXCEPT;
    void write(int fd, off_t pos, const char* src) TIGHTDB_NOEXCEPT;

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

    uint8_t m_hmacKey[32];
    std::vector<iv_table> m_iv_buffer;

    bool check_hmac(const void *data, size_t len, const uint8_t *hmac) const;
    void crypt(EncryptionMode mode, off_t pos, char* dst, const char* src,
               const char* stored_iv) TIGHTDB_NOEXCEPT;
    iv_table& get_iv_table(int fd, off_t data_pos) TIGHTDB_NOEXCEPT;
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
    void flush() TIGHTDB_NOEXCEPT;

    // Sync this file to disk
    void sync() TIGHTDB_NOEXCEPT;

    // Handle a SEGV or BUS at the given address, which must be within this
    // object's mapping
    void handle_access(void* addr) TIGHTDB_NOEXCEPT;

    // Set this mapping to a new address and size
    // Flushes any remaining dirty pages from the old mapping
    void set(void* new_addr, size_t new_size);

private:
    SharedFileInfo& m_file;

    void* m_addr;
    size_t m_size;

    uintptr_t m_first_page;
    size_t m_page_count;

    std::vector<bool> m_read_pages;
    std::vector<bool> m_write_pages;
    std::vector<bool> m_dirty_pages;

    File::AccessMode m_access;

    char* page_addr(size_t i) const TIGHTDB_NOEXCEPT;

    void mark_unreadable(size_t i) TIGHTDB_NOEXCEPT;
    void mark_readable(size_t i) TIGHTDB_NOEXCEPT;
    void mark_unwritable(size_t i) TIGHTDB_NOEXCEPT;

    bool copy_read_page(size_t i) TIGHTDB_NOEXCEPT;
    void read_page(size_t i) TIGHTDB_NOEXCEPT;
    void write_page(size_t i) TIGHTDB_NOEXCEPT;

    void validate_page(size_t i) TIGHTDB_NOEXCEPT;
    void validate() TIGHTDB_NOEXCEPT;
};

}
}

#endif // TIGHTDB_ENABLE_ENCRYPTION
#endif // TIGHTDB_UTIL_ENCRYPTED_FILE_MAPPING_HPP
