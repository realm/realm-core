#include "test.hpp"

#include <realm/sync/noinst/server/encrypt_fingerprint.hpp>

using namespace realm;
using namespace realm::util;
using namespace realm::encrypt;

TEST(Encrypt_Fingerprint)
{
    // No encryption.
    Optional<std::array<char, 64>> encryption_key_0 = none;

    // A random key.
    const unsigned char data_1[64] = {101, 152, 243, 182, 36,  180, 180, 251, 113, 140, 41,  21,  80,  150, 64,  224,
                                      194, 231, 10,  135, 164, 225, 74,  221, 15,  250, 180, 232, 159, 9,   184, 77,
                                      127, 27,  111, 111, 103, 234, 123, 58,  136, 112, 114, 216, 138, 104, 115, 91,
                                      211, 171, 156, 11,  96,  4,   70,  215, 160, 22,  43,  187, 225, 127, 169, 242};

    std::array<char, 64> encryption_key_1;
    std::memcpy(encryption_key_1.data(), data_1, 64);

    // Another key that is very similar to the previous key.
    std::array<char, 64> encryption_key_2 = encryption_key_1;
    encryption_key_2[63] = encryption_key_1[63] + 1;

    const std::string fingerprint_0 = calculate_fingerprint(encryption_key_0);
    const std::string fingerprint_1 = calculate_fingerprint(encryption_key_1);
    const std::string fingerprint_2 = calculate_fingerprint(encryption_key_2);

    CHECK(verify_fingerprint(fingerprint_0, encryption_key_0));
    CHECK_NOT(verify_fingerprint(fingerprint_0, encryption_key_1));
    CHECK_NOT(verify_fingerprint(fingerprint_0, encryption_key_2));

    CHECK_NOT(verify_fingerprint(fingerprint_1, encryption_key_0));
    CHECK(verify_fingerprint(fingerprint_1, encryption_key_1));
    CHECK_NOT(verify_fingerprint(fingerprint_1, encryption_key_2));

    CHECK_NOT(verify_fingerprint(fingerprint_2, encryption_key_0));
    CHECK_NOT(verify_fingerprint(fingerprint_2, encryption_key_1));
    CHECK(verify_fingerprint(fingerprint_2, encryption_key_2));

    // Here we check the explicit SHA256 implementation.
    const std::string expected_0 = "e3:b0:c4:42:98:fc:1c:14:"
                                   "9a:fb:f4:c8:99:6f:b9:24:"
                                   "27:ae:41:e4:64:9b:93:4c:"
                                   "a4:95:99:1b:78:52:b8:55";

    const std::string expected_1 = "84:60:75:ba:c8:5d:ff:da:"
                                   "b9:11:2b:80:14:ef:51:1b:"
                                   "56:0b:72:a8:b9:aa:8c:39:"
                                   "f0:c2:c7:79:49:e8:5a:55";

    const std::string expected_2 = "36:01:da:eb:09:1e:c0:57:"
                                   "9b:d8:73:3e:fa:fe:97:4e:"
                                   "f8:71:1b:81:f9:6d:3a:ca:"
                                   "20:e4:2d:4a:4f:18:67:e0";

    CHECK_EQUAL(expected_0, fingerprint_0);
    CHECK_EQUAL(expected_1, fingerprint_1);
    CHECK_EQUAL(expected_2, fingerprint_2);
}
