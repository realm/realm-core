#include "testsettings.hpp"
#ifdef TEST_ENCRYPTED_FILE_MAPPING

#include <tightdb/util/encrypted_file_mapping.hpp>

#include "test.hpp"

#include <fcntl.h>
#include <unistd.h>

// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.

#ifdef TIGHTDB_ENABLE_ENCRYPTION

using namespace tightdb::util;

TEST(EncryptedFile_CryptorBasic)
{
    TEST_PATH(path);

    AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
    cryptor.set_file_size(16);
    const char data[4096] = "test data";
    char buffer[4096];

    int fd = open(path.c_str(), O_CREAT|O_RDWR);
    cryptor.write(fd, 0, data);
    cryptor.read(fd, 0, buffer);
    CHECK(memcmp(buffer, data, strlen(data)) == 0);
    close(fd);
}

TEST(EncryptedFile_CryptorRepeatedWrites)
{
    TEST_PATH(path);
    AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
    cryptor.set_file_size(16);

    const char data[4096] = "test data";
    char raw_buffer_1[8192] = {0}, raw_buffer_2[9192] = {0};
    int fd = open(path.c_str(), O_CREAT|O_RDWR);

    cryptor.write(fd, 0, data);
    lseek(fd, 0, SEEK_SET);
    read(fd, raw_buffer_1, sizeof(raw_buffer_1));

    cryptor.write(fd, 0, data);
    lseek(fd, 0, SEEK_SET);
    read(fd, raw_buffer_2, sizeof(raw_buffer_2));

    CHECK(memcmp(raw_buffer_1, raw_buffer_2, sizeof(raw_buffer_1)) != 0);

    close(fd);
}

TEST(EncryptedFile_SeparateCryptors)
{
    TEST_PATH(path);

    const char data[4096] = "test data";
    char buffer[4096];

    int fd = open(path.c_str(), O_CREAT|O_RDWR);
    {
        AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
        cryptor.set_file_size(16);
        cryptor.write(fd, 0, data);
    }
    {
        AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
        cryptor.set_file_size(16);
        cryptor.read(fd, 0, buffer);
    }

    CHECK(memcmp(buffer, data, strlen(data)) == 0);
    close(fd);
}

TEST(EncryptedFile_InterruptedWrite)
{
    TEST_PATH(path);

    const char data[] = "test data";

    int fd = open(path.c_str(), O_CREAT|O_RDWR);
    {
        AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
        cryptor.set_file_size(16);
        cryptor.write(fd, 0, data);
    }

    // Fake an interrupted write which updates the IV table but not the data
    char buffer[4096];
    pread(fd, buffer, 64, 0);
    memcpy(buffer + 32, buffer, 32);
    buffer[5]++; // first byte of "hmac1" field in iv table
    pwrite(fd, buffer, 64, 0);

    {
        AESCryptor cryptor((const uint8_t *)"12345678901234567890123456789012");
        cryptor.set_file_size(16);
        cryptor.read(fd, 0, buffer);
        CHECK(memcmp(buffer, data, strlen(data)) == 0);
    }

    close(fd);
}

#endif // TIGHTDB_ENABLE_ENCRYPTION
#endif // TEST_ENCRYPTED_FILE_MAPPING
