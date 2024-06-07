/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_AES_CRYPTOR_HPP
#define REALM_AES_CRYPTOR_HPP

#include <realm/util/features.h>
#include <realm/util/file.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace realm::util {
class WriteObserver {
public:
    virtual bool no_concurrent_writer_seen() = 0;

protected:
    ~WriteObserver() = default;
};

class WriteMarker {
public:
    virtual void mark(uint64_t page_offset) = 0;
    virtual void unmark() = 0;

protected:
    ~WriteMarker() = default;
};
} // namespace realm::util

#if REALM_ENABLE_ENCRYPTION

#if REALM_PLATFORM_APPLE
#include <CommonCrypto/CommonCrypto.h>
#elif defined(_WIN32)
#include <windows.h>
#include <stdio.h>
#include <bcrypt.h>
#pragma comment(lib, "bcrypt.lib")
#else
#include <openssl/sha.h>
#include <openssl/evp.h>
#endif

namespace realm::util {

struct IVTable;
class EncryptedFileMapping;

class AESCryptor {
public:
    AESCryptor(const char* key);
    ~AESCryptor() noexcept;

    void set_data_size(File::SizeType new_size);

    enum class ReadResult { Eof, Uninitialized, InterruptedFirstWrite, StaleHmac, Failed, Success };
    ReadResult read(FileDesc fd, File::SizeType pos, char* dst, WriteObserver* observer = nullptr);
    void try_read_block(FileDesc fd, File::SizeType pos, char* dst) noexcept;
    void write(FileDesc fd, File::SizeType pos, const char* src, WriteMarker* marker = nullptr) noexcept;
    bool refresh_iv(FileDesc fd, size_t page_ndx);
    void invalidate_ivs() noexcept;

    const char* get_key() const noexcept
    {
        return reinterpret_cast<const char*>(m_key.data());
    }

private:
    enum EncryptionMode {
#if REALM_PLATFORM_APPLE
        mode_Encrypt = kCCEncrypt,
        mode_Decrypt = kCCDecrypt
#elif defined(_WIN32)
        mode_Encrypt = 0,
        mode_Decrypt = 1
#else
        mode_Encrypt = 1,
        mode_Decrypt = 0
#endif
    };

    enum class IVLookupMode { UseCache, Refetch };
    using Hmac = std::array<uint8_t, 28>;

#if REALM_PLATFORM_APPLE
    CCCryptorRef m_encr;
    CCCryptorRef m_decr;
#elif defined(_WIN32)
    BCRYPT_KEY_HANDLE m_aes_key_handle;
#else
    EVP_CIPHER_CTX* m_ctx;
#endif

    const std::array<uint8_t, 64> m_key;
    std::vector<IVTable> m_iv_buffer;
    std::vector<IVTable> m_iv_buffer_cache;
    std::vector<bool> m_iv_blocks_read;
    std::unique_ptr<char[]> m_rw_buffer;
    std::unique_ptr<char[]> m_dst_buffer;

    bool constant_time_equals(const Hmac&, const Hmac&) const;
    void calculate_hmac(Hmac&) const;
    void crypt(EncryptionMode mode, File::SizeType pos, char* dst, const char* src, const char* stored_iv) noexcept;
    IVTable& get_iv_table(FileDesc fd, File::SizeType data_pos, IVLookupMode mode = IVLookupMode::UseCache) noexcept;
    void handle_error();
    void read_iv_block(FileDesc fd, File::SizeType data_pos);
    ReadResult attempt_read(FileDesc fd, File::SizeType pos, char* dst, IVLookupMode iv_mode, uint32_t& iv,
                            Hmac& hmac);
};
} // namespace realm::util

#endif // REALM_ENABLE_ENCRYPTION
#endif // REALM_AES_CRYPTOR_HPP
