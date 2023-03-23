/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm/util/sha_crypto.hpp>
#include <realm/util/backtrace.hpp>
#include <realm/util/assert.hpp>

#if REALM_PLATFORM_APPLE
#include <CommonCrypto/CommonCrypto.h>
#elif defined(_WIN32)
#include <windows.h>
#include <stdio.h>
#include <bcrypt.h>
#pragma comment(lib, "bcrypt.lib")
#define REALM_USE_BUNDLED_SHA2 1
#elif REALM_HAVE_OPENSSL
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#else
#include <sha1.h>
#define REALM_USE_BUNDLED_SHA2 1
#endif

#ifdef REALM_USE_BUNDLED_SHA2
#include <sha224.hpp>
#include <sha256.hpp>
#include <cstring>
#endif

namespace {

// The message digest of the input is calculated. The output is placed in
// out_buffer, and the size of the output is placed in out_size. The caller
// guarantees that out_buffer is large enough, which is always possible for
// message digests with a maximum output size.
#if REALM_PLATFORM_APPLE
#elif defined(_WIN32)
struct Algorithm {
    Algorithm(LPCWSTR alg_id)
    {
        if (BCryptOpenAlgorithmProvider(&hAlg, alg_id, NULL, 0) < 0) {
            throw realm::util::runtime_error("BCryptOpenAlgorithmProvider() failed");
        }
    }
    ~Algorithm()
    {
        if (hAlg) {
            BCryptCloseAlgorithmProvider(hAlg, 0);
        }
    }
    DWORD obj_length()
    {
        DWORD len;
        ULONG dummy;
        BCryptGetProperty(hAlg, BCRYPT_OBJECT_LENGTH, (PBYTE)&len, sizeof(DWORD), &dummy, 0);
        return len;
    }
    DWORD hash_length()
    {
        DWORD len;
        ULONG dummy;
        BCryptGetProperty(hAlg, BCRYPT_HASH_LENGTH, (PBYTE)&len, sizeof(DWORD), &dummy, 0);
        return len;
    }

    BCRYPT_ALG_HANDLE hAlg = NULL;
};
struct Hash {
    Hash(Algorithm& a, DWORD output_size)
        : alg(a)
        , hash_size(output_size)
    {
        REALM_ASSERT(alg.obj_length() < 512);
        REALM_ASSERT(alg.hash_length() == hash_size);
        if (BCryptCreateHash(alg.hAlg, &hHash, hash_object_buffer, 515, NULL, 0, 0) < 0) {
            throw realm::util::runtime_error("BCryptCreateHash() failed");
        }
    }
    ~Hash()
    {
        if (hHash) {
            BCryptDestroyHash(hHash);
        }
    }
    void get_hash(PUCHAR in_buffer, DWORD in_buffer_size, PUCHAR out_buffer)
    {
        if (BCryptHashData(hHash, in_buffer, in_buffer_size, 0) < 0) {
            throw realm::util::runtime_error("BCryptHashData() failed");
        }

        BCryptFinishHash(hHash, out_buffer, hash_size, 0);
    }
    Algorithm& alg;
    BCRYPT_HASH_HANDLE hHash = NULL;
    UCHAR hash_object_buffer[512];
    DWORD hash_size;
};
#elif REALM_HAVE_OPENSSL
void message_digest(const EVP_MD* digest_type, const char* in_buffer, size_t in_buffer_size,
                    unsigned char* out_buffer, unsigned int* output_size)
{
    EVP_MD_CTX* ctx = EVP_MD_CTX_create();

    int rc = EVP_DigestInit_ex(ctx, digest_type, nullptr);
    if (rc == 0) {
        EVP_MD_CTX_destroy(ctx);
        throw realm::util::runtime_error("EVP_DigestInit() failed");
    }

    rc = EVP_DigestUpdate(ctx, in_buffer, in_buffer_size);
    if (rc == 0) {
        EVP_MD_CTX_destroy(ctx);
        throw realm::util::runtime_error("EVP_DigestUpdate() failed");
    }

    rc = EVP_DigestFinal_ex(ctx, out_buffer, output_size);

    EVP_MD_CTX_destroy(ctx);

    if (rc == 0)
        throw realm::util::runtime_error("EVP_DigestFinal_ex() failed");
}
#endif

#ifdef REALM_USE_BUNDLED_SHA2
using namespace realm::util;
template <typename ShaState, size_t digest_length>
void hmac(Span<const uint8_t> in_buffer, Span<uint8_t, digest_length> out_buffer, Span<const uint8_t, 32> key)
{
    uint8_t ipad[64];
    for (size_t i = 0; i < 32; ++i)
        ipad[i] = key[i] ^ 0x36;
    memset(ipad + 32, 0x36, 32);

    uint8_t opad[64] = {0};
    for (size_t i = 0; i < 32; ++i)
        opad[i] = key[i] ^ 0x5C;
    memset(opad + 32, 0x5C, 32);

    // Full hmac operation is sha_alg(opad + sha_alg(ipad + data))
    ShaState s;
    sha_init(s);
    sha_process(s, ipad, 64);
    sha_process(s, in_buffer.data(), std::uint32_t(in_buffer.size()));
    sha_done(s, out_buffer.data());

    sha_init(s);
    sha_process(s, opad, 64);
    sha_process(s, out_buffer.data(), std::uint32_t(digest_length));
    sha_done(s, out_buffer.data());
}
#endif
} // namespace

namespace realm {
namespace util {

void sha1(const char* in_buffer, size_t in_buffer_size, unsigned char* out_buffer)
{
#if REALM_PLATFORM_APPLE
    CC_SHA1(in_buffer, CC_LONG(in_buffer_size), out_buffer);
#elif defined(_WIN32)
    Algorithm alg(BCRYPT_SHA1_ALGORITHM);
    Hash hash(alg, 20);
    hash.get_hash(reinterpret_cast<PUCHAR>(const_cast<char*>(in_buffer)), DWORD(in_buffer_size), out_buffer);
#elif REALM_HAVE_OPENSSL
    const EVP_MD* digest_type = EVP_sha1();
    unsigned int output_size;
    message_digest(digest_type, in_buffer, in_buffer_size, out_buffer, &output_size);
    REALM_ASSERT(output_size == 20);
#else
    SHA1(reinterpret_cast<char*>(out_buffer), in_buffer, in_buffer_size);
#endif
}

void sha256(const char* in_buffer, size_t in_buffer_size, unsigned char* out_buffer)
{
#if REALM_PLATFORM_APPLE
    CC_SHA256(in_buffer, CC_LONG(in_buffer_size), out_buffer);
#elif defined(_WIN32)
    Algorithm alg(BCRYPT_SHA256_ALGORITHM);
    Hash hash(alg, 32);
    hash.get_hash(reinterpret_cast<PUCHAR>(const_cast<char*>(in_buffer)), DWORD(in_buffer_size), out_buffer);
#elif REALM_HAVE_OPENSSL
    const EVP_MD* digest_type = EVP_sha256();
    unsigned int output_size;
    message_digest(digest_type, in_buffer, in_buffer_size, out_buffer, &output_size);
    REALM_ASSERT(output_size == 32);
#else
    sha256_state s;
    sha_init(s);
    sha_process(s, in_buffer, uint32_t(in_buffer_size));
    sha_done(s, out_buffer);
#endif
}

void hmac_sha224(Span<const uint8_t> in_buffer, Span<uint8_t, 28> out_buffer, Span<const uint8_t, 32> key)
{
#if REALM_PLATFORM_APPLE
    static_assert(CC_SHA224_DIGEST_LENGTH == out_buffer.size());
    CCHmac(kCCHmacAlgSHA224, key.data(), key.size(), in_buffer.data(), in_buffer.size(), out_buffer.data());
#elif defined(REALM_USE_BUNDLED_SHA2)
    static_assert(28 == out_buffer.size());
    hmac<sha224_state>(in_buffer, out_buffer, key);
#elif REALM_HAVE_OPENSSL
    static_assert(SHA224_DIGEST_LENGTH == out_buffer.size());
    unsigned int hashLen;
    HMAC(EVP_sha224(), key.data(), static_cast<int>(key.size()), in_buffer.data(), in_buffer.size(),
         out_buffer.data(), &hashLen);
    REALM_ASSERT_DEBUG(hashLen == out_buffer.size());
#else
#error "No SHA224 digest implementation on this platform."
#endif
}

void hmac_sha256(Span<const uint8_t> in_buffer, Span<uint8_t, 32> out_buffer, Span<const uint8_t, 32> key)
{
#if REALM_PLATFORM_APPLE
    static_assert(CC_SHA256_DIGEST_LENGTH == out_buffer.size());
    CCHmac(kCCHmacAlgSHA256, key.data(), key.size(), in_buffer.data(), in_buffer.size(), out_buffer.data());
#elif defined(REALM_USE_BUNDLED_SHA2)
    static_assert(32 == out_buffer.size());
    hmac<sha256_state>(in_buffer, out_buffer, key);
#elif REALM_HAVE_OPENSSL
    static_assert(SHA256_DIGEST_LENGTH == out_buffer.size());
    unsigned int hashLen;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()), in_buffer.data(), in_buffer.size(),
         out_buffer.data(), &hashLen);
    REALM_ASSERT_DEBUG(hashLen == out_buffer.size());
#else
#error "No SHA56 digest implementation on this platform."
#endif
}

} // namespace util
} // namespace realm
