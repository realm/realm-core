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
#include <wincrypt.h>
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
void message_digest(ALG_ID digest_type, const BYTE* in_buffer, DWORD in_buffer_size, BYTE* out_buffer,
                    DWORD* output_size)
{
    HCRYPTPROV hProv = 0;
    HCRYPTHASH hHash = 0;

    if (!CryptAcquireContext(&hProv, NULL, NULL, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT)) {
        throw realm::util::runtime_error("CryptAcquireContext() failed");
    }

    if (!CryptCreateHash(hProv, digest_type, 0, 0, &hHash)) {
        CryptReleaseContext(hProv, 0);
        throw realm::util::runtime_error("CryptCreateHash() failed");
    }

    if (!CryptHashData(hHash, in_buffer, in_buffer_size, 0)) {
        CryptReleaseContext(hProv, 0);
        CryptDestroyHash(hHash);
        throw realm::util::runtime_error("CryptCreateHash() failed");
    }

    CryptGetHashParam(hHash, HP_HASHVAL, out_buffer, output_size, 0);

    CryptDestroyHash(hHash);
    CryptReleaseContext(hProv, 0);
}
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
    DWORD output_size = 20;
    message_digest(CALG_SHA1, reinterpret_cast<const BYTE*>(in_buffer), DWORD(in_buffer_size),
                   reinterpret_cast<BYTE*>(out_buffer), &output_size);
    REALM_ASSERT(output_size == 20);
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
    DWORD output_size = 32;
    message_digest(CALG_SHA_256, reinterpret_cast<const BYTE*>(in_buffer), DWORD(in_buffer_size),
                   reinterpret_cast<BYTE*>(out_buffer), &output_size);
    REALM_ASSERT(output_size == 32);
#else
    const EVP_MD* digest_type = EVP_sha256();
    unsigned int output_size;
    message_digest(digest_type, in_buffer, in_buffer_size, out_buffer, &output_size);
    REALM_ASSERT(output_size == 32);
#endif
}

} // namespace util
} // namespace realm
