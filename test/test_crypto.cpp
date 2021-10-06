#include <realm/string_data.hpp>
#include <realm/util/sha_crypto.hpp>
#include <realm/sync/noinst/server/crypto_server.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::sync;

static const char test_crypto_pubkey[] = "test_pubkey.pem";

static const StringData test_crypto_pubkey_data = "-----BEGIN PUBLIC KEY-----\n"
                                                  "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3Rml+lxaRRJhQRak6kSC\n"
                                                  "/mXLFE6QOoX+fIZ+0nzZLvSZpa5lfhdPSm2DrCX+zs8pY1cupZ4tAWJxe4m91f04\n"
                                                  "bow3jnxd2s4UkXAxPBZUQEW0ZhUhNzbvwVht03fqIGi+tMDn4R0cxrtCFvkFwJ1g\n"
                                                  "S+fLHOpesdg51taGNWiAFW73yWYwVGHY0x+0GsRNL5UjSZ1nnajt29CUK7QdA2gp\n"
                                                  "tPwGShY/T8VaEPmLuwtWZ8lM0vlqOg/PHDFLnu+VMBSKB6EZOnRov/o5DC4e4Hhn\n"
                                                  "UchmrYQtp4aNXMrw5klkf0WjA8JK6q6KYbubQZ1UzoLa1Wzgi4pXJgPBodaUKr5g\n"
                                                  "cQIDAQAB\n"
                                                  "-----END PUBLIC KEY-----";

static const char test_message[] = "Hello, World";

// This must be the contents of "test_message" signed with the private key
// corresponding to the public key named by test_crypto_pubkey.
static const char test_signature[] = "\x21\xbc\x92\x5e\x1e\x63\x04\xe3\x51\x75\xcb\xe5\x94\x82\xf1\xbe"
                                     "\x48\xe3\xd7\x26\xe2\x81\x04\x07\x0b\x30\x0d\x99\x90\x02\xc6\x5d"
                                     "\x5d\x9a\x87\x14\x13\x0b\x9b\xa1\xc3\x7f\xb0\x2e\xf6\xfa\xda\xdd"
                                     "\x54\xa3\xfd\xf9\xce\x2e\xeb\xab\x1b\xda\xb4\x44\x27\x13\xcb\x54"
                                     "\x88\x37\xf9\xd6\xbe\x82\x8b\x60\xa4\xc7\xa4\x0f\xed\x2e\xb3\x2b"
                                     "\x7d\x29\xb4\x63\x36\xff\x7e\xed\x3a\x0f\x43\x17\x94\x35\xb4\x0d"
                                     "\x0f\xec\xcd\x8b\x38\x6a\x4b\x42\x79\xbe\xf3\x81\xe7\xec\x1b\xe6"
                                     "\xbb\xc2\xc8\xdb\xad\xa3\x92\x60\xcb\x7f\xdd\x21\x07\xae\x1e\xba"
                                     "\x1f\x4a\xe0\x60\x66\xaa\xf8\x6f\x05\xc6\x2b\x1f\xb1\xe2\x59\xda"
                                     "\x5e\x3f\xcb\xea\xae\xd5\x50\x68\xfa\xe1\xd3\x8b\xcb\x5e\x08\xb8"
                                     "\x72\x3c\xf6\xc8\xff\x92\x71\xc4\x91\x01\x61\x82\x25\xd0\xd3\xce"
                                     "\x18\x13\xf2\x85\xb6\x9f\xea\xb4\xda\x7e\xc8\xd3\x19\xcf\x9d\xe8"
                                     "\x95\xcd\xae\xb0\x77\x86\xa5\x45\x36\x1b\x3e\x5c\x6f\x1b\xf8\x01"
                                     "\x3e\x5d\x68\xf6\x97\x6e\x3b\x67\x4a\xd9\x55\xaa\xca\xc2\x0c\x8d"
                                     "\x1b\xe3\x15\x47\xf8\x4c\x6b\x72\xee\xd5\x60\x59\xa7\x56\xf8\x8a"
                                     "\xc0\x91\x9a\xd9\x29\xa0\x5e\x85\xac\x0f\x5d\x41\x1f\x8e\x6e\xc7";

TEST(Crypto_LoadPublicKey)
{
    auto key = PKey::load_public(test_crypto_pubkey);
    CHECK(key.can_verify());
    CHECK(!key.can_sign());
}

TEST(Crypto_LoadPublicKeyFromBuffer)
{
    auto key = PKey::load_public(BinaryData(test_crypto_pubkey_data.data(), test_crypto_pubkey_data.size()));
    CHECK(key.can_verify());
    CHECK(!key.can_sign());
}

TEST(Crypto_Verify_WithKeyFromFile)
{
    auto key = PKey::load_public(test_crypto_pubkey);
    CHECK(key.can_verify());

    BinaryData msg{test_message, sizeof test_message - 1};
    BinaryData sig{test_signature, sizeof test_signature - 1};
    CHECK(key.verify(msg, sig));
}

TEST(Crypto_Verify_WithKeyFromBuffer)
{
    auto key = PKey::load_public(BinaryData(test_crypto_pubkey_data.data(), test_crypto_pubkey_data.size()));
    CHECK(key.can_verify());

    BinaryData msg{test_message, sizeof test_message - 1};
    BinaryData sig{test_signature, sizeof test_signature - 1};
    CHECK(key.verify(msg, sig));
}

TEST(Crypto_SHA1)
{
    char in_buffer[] = "abc";
    const unsigned char expected_hash[] = "\xa9\x99\x3e\x36\x47\x06\x81\x6a\xba\x3e"
                                          "\x25\x71\x78\x50\xc2\x6c\x9c\xd0\xd8\x9d";

    unsigned char out_buffer[20];

    util::sha1(in_buffer, 3, out_buffer);

    CHECK(!std::memcmp(expected_hash, out_buffer, 20));
}

TEST(Crypto_SHA256)
{
    char in_buffer[] = "abc";
    const unsigned char expected_hash[] = "\xba\x78\x16\xbf\x8f\x01\xcf\xea"
                                          "\x41\x41\x40\xde\x5d\xae\x22\x23"
                                          "\xb0\x03\x61\xa3\x96\x17\x7a\x9c"
                                          "\xb4\x10\xff\x61\xf2\x00\x15\xad";

    unsigned char out_buffer[32];

    util::sha256(in_buffer, 3, out_buffer);

    CHECK(!std::memcmp(expected_hash, out_buffer, 32));
}
