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
#else
#include <openssl/sha.h>
#include <openssl/evp.h>
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
#else
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
#else
    const EVP_MD* digest_type = EVP_sha1();
    unsigned int output_size;
    message_digest(digest_type, in_buffer, in_buffer_size, out_buffer, &output_size);
    REALM_ASSERT(output_size == 20);
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
#else
    const EVP_MD* digest_type = EVP_sha256();
    unsigned int output_size;
    message_digest(digest_type, in_buffer, in_buffer_size, out_buffer, &output_size);
    REALM_ASSERT(output_size == 32);
#endif
}

} // namespace util
} // namespace realm
