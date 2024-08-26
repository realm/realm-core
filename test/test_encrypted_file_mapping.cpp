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

#include "testsettings.hpp"

#if defined(TEST_ENCRYPTED_FILE_MAPPING)

#include <realm.hpp>
#include <realm/util/aes_cryptor.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"

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

#if REALM_ENABLE_ENCRYPTION

using namespace realm;
using namespace realm::util;
using realm::FileDesc;

namespace {
const char test_key[] = "1234567890123456789012345678901123456789012345678901234567890123";
}

TEST(EncryptedFile_CryptorBasic)
{
    TEST_PATH(path);

    AESCryptor cryptor(test_key);
    cryptor.set_data_size(16);
    const char data[4096] = "test data";
    char buffer[4096];

    File file(path, realm::util::File::mode_Write);
    cryptor.write(file.get_descriptor(), 0, data);
    cryptor.read(file.get_descriptor(), 0, buffer);
    CHECK(memcmp(buffer, data, strlen(data)) == 0);
}

TEST(EncryptedFile_CryptorRepeatedWrites)
{
    TEST_PATH(path);
    AESCryptor cryptor(test_key);
    cryptor.set_data_size(16);

    const char data[4096] = "test data";
    char raw_buffer_1[8192] = {0}, raw_buffer_2[8192] = {0};
    File file(path, realm::util::File::mode_Write);

    cryptor.write(file.get_descriptor(), 0, data);
    ssize_t actual_read_1 = file.read(0, raw_buffer_1, sizeof(raw_buffer_1));
    CHECK_EQUAL(actual_read_1, sizeof(raw_buffer_1));

    cryptor.write(file.get_descriptor(), 0, data);
    ssize_t actual_read_2 = file.read(0, raw_buffer_2, sizeof(raw_buffer_2));
    CHECK_EQUAL(actual_read_2, sizeof(raw_buffer_2));

    CHECK(memcmp(raw_buffer_1, raw_buffer_2, sizeof(raw_buffer_1)) != 0);
}

TEST(EncryptedFile_SeparateCryptors)
{
    TEST_PATH(path);

    const char data[4096] = "test data";
    char buffer[4096];

    File file(path, realm::util::File::mode_Write);
    {
        AESCryptor cryptor(test_key);
        cryptor.set_data_size(16);
        cryptor.write(file.get_descriptor(), 0, data);
    }
    {
        AESCryptor cryptor(test_key);
        cryptor.set_data_size(16);
        cryptor.read(file.get_descriptor(), 0, buffer);
    }

    CHECK(memcmp(buffer, data, strlen(data)) == 0);
}

TEST(EncryptedFile_InterruptedWrite)
{
    TEST_PATH(path);

    const char data[4096] = "test data";

    File file(path, realm::util::File::mode_Write);
    {
        AESCryptor cryptor(test_key);
        cryptor.set_data_size(16);
        cryptor.write(file.get_descriptor(), 0, data);
    }

    // Fake an interrupted write which updates the IV table but not the data
    char buffer[4096];
    size_t actual_pread = file.read(0, buffer, 64);
    CHECK_EQUAL(actual_pread, 64);

    memcpy(buffer + 32, buffer, 32);
    buffer[5]++; // first byte of "hmac1" field in iv table
    file.write(0, buffer, 64);

    {
        AESCryptor cryptor(test_key);
        cryptor.set_data_size(16);
        cryptor.read(file.get_descriptor(), 0, buffer);
        CHECK(memcmp(buffer, data, strlen(data)) == 0);
    }
}

TEST(EncryptedFile_IVRefreshing)
{
    constexpr size_t page_size = 4096;
    constexpr size_t pages_per_metadata_block = 64;

    // enough data to span two metadata blocks
    constexpr size_t page_count = pages_per_metadata_block * 2;
    constexpr File::SizeType data_size = page_size * page_count;
    char data[page_size];
    std::iota(std::begin(data), std::end(data), 0);

    TEST_PATH(path);
    File file(path, realm::util::File::mode_Write);
    const FileDesc fd = file.get_descriptor();

    AESCryptor cryptor(test_key);
    cryptor.set_data_size(data_size);
    for (File::SizeType i = 0; i < data_size; i += page_size) {
        cryptor.write(fd, i, data);
    }
    // The IVs for the pages we just wrote should obviously be up to date
    for (size_t i = 0; i < page_count; ++i) {
        CHECK_NOT(cryptor.refresh_iv(fd, i));
    }
    // and we should see the same ones after rereading them
    cryptor.invalidate_ivs();
    for (size_t i = 0; i < page_count; ++i) {
        CHECK_NOT(cryptor.refresh_iv(fd, i));
    }

    AESCryptor cryptor2(test_key);
    cryptor2.set_data_size(data_size);
    for (size_t i = 0; i < page_count; ++i) {
        // Each IV should be up to date immediately after reading the page
        cryptor2.read(fd, File::SizeType(i) * page_size, data);
        CHECK_NOT(cryptor2.refresh_iv(fd, i));
    }

    // Nothing's changed so rereading them should report no refresh needed
    cryptor2.invalidate_ivs();
    for (size_t i = 0; i < page_count; ++i) {
        CHECK_NOT(cryptor2.refresh_iv(fd, i));
    }

    // Modify all pages, invalidate, verify each page needs to be refreshed
    // Note that even though this isn't changing the plaintext it does update
    // the ciphertext each time
    for (File::SizeType i = 0; i < data_size; i += page_size) {
        cryptor.write(fd, i, data);
    }
    cryptor2.invalidate_ivs();
    for (size_t i = 0; i < page_count; ++i) {
        CHECK(cryptor2.refresh_iv(fd, i));
        // refresh_iv only returns true once per page per write
        CHECK_NOT(cryptor2.refresh_iv(fd, i));
    }

    // Modify all pages, verifying that a refresh is needed after each one
    for (size_t i = 0; i < page_count; ++i) {
        cryptor.write(fd, File::SizeType(i) * page_size, data);
        cryptor2.invalidate_ivs();
        CHECK(cryptor2.refresh_iv(fd, i));
        CHECK_NOT(cryptor2.refresh_iv(fd, i));
    }

    // Same thing but in reverse. This verifies that initialization of data
    // before the earliest populated point is tracked correctly
    for (size_t i = page_count; i > 0; --i) {
        cryptor.write(fd, File::SizeType(i - 1) * page_size, data);
        cryptor2.invalidate_ivs();
        CHECK(cryptor2.refresh_iv(fd, i - 1));
        CHECK_NOT(cryptor2.refresh_iv(fd, i - 1));
    }
}

TEST(EncryptedFile_NonPageAlignedMapping)
{
    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(test_util::crypt_key(true));
        f.resize(page_size() * 2);
        // Since no power-of-two page size is a multiple of 11, one of these
        // mapping will straddle a page
        for (size_t pos = 0; pos + 10 <= page_size() * 2; pos += 11) {
            File::Map<char> map(f, pos, File::access_ReadWrite, 10);
            util::encryption_read_barrier(map, 0, 10);
            for (int i = 0; i < 10; ++i)
                map.get_addr()[i] = char(i + 1);
            util::encryption_write_barrier(map, 0, 10);
        }
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(test_util::crypt_key(true));
        for (size_t pos = 0; pos + 17 <= page_size() * 2; pos += 7) {
            File::Map<char> map(f, pos, File::access_ReadOnly, 6);
            util::encryption_read_barrier(map, 0, 6);
            for (int i = 0; i < 6; ++i)
                CHECK_EQUAL(int(map.get_addr()[i]), (pos + i + 1) % 11);
        }
    }
}

TEST(EncryptedFile_GapsOfNeverWrittenPages)
{
    constexpr size_t page_count = 128;
    TEST_PATH(path);

    // Write to every other page. Note that on 16k systems this is actually
    // writing to 4 pages and then skipping 4 pages, which achieves the same
    // goal.
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(test_util::crypt_key(true));
        f.resize(page_size() * page_count);
        for (size_t i = 0; i < page_count; i += 2) {
            File::Map<char> map(f, i * page_size(), File::access_ReadWrite, page_size());
            util::encryption_read_barrier(map, 0, page_size());
            std::fill(map.get_addr(), map.get_addr() + map.get_size(), 1);
            util::encryption_write_barrier(map, 0, page_size());
        }
    }

    // Trying to read via a single large read barrier should fail since it
    // includes never-written pages
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(test_util::crypt_key(true));
        File::Map<char> map(f, 0, File::access_ReadOnly, page_count * page_size());
        CHECK_THROW(util::encryption_read_barrier(map, 0, map.get_size()), DecryptionFailed);
    }

    // A single large read mapping that only has barriers for the written pages
    // should work
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(test_util::crypt_key(true));
        File::Map<char> map(f, 0, File::access_ReadOnly, page_count * page_size());
        for (size_t i = 0; i < page_count; i += 2) {
            util::encryption_read_barrier(map, i * page_size(), page_size());
            for (size_t j = 0; j < page_size(); ++j) {
                CHECK_EQUAL(int(map.get_addr()[i * page_size() + j]), 1);
            }
        }

        // And reading the unwritten pages should throw
        for (size_t i = 1; i < page_count; i += 2) {
            CHECK_THROW(util::encryption_read_barrier(map, 0, map.get_size()), DecryptionFailed);
        }
    }

    // Reading the whole thing via a write mapping should work, as those are
    // allowed to see uninitialized data
    {
        File f(path, File::mode_Update);
        f.set_encryption_key(test_util::crypt_key(true));
        File::Map<char> map(f, 0, File::access_ReadWrite, page_count * page_size());
        util::encryption_read_barrier(map, 0, map.get_size());

        for (size_t i = 0; i < page_count; ++i) {
            const int expected = (i + 1) % 2;
            for (size_t j = 0; j < page_size(); ++j) {
                CHECK_EQUAL(int(map.get_addr()[i * page_size() + j]), expected);
            }
        }
        util::encryption_write_barrier(map, 0, map.get_size());
    }
}

TEST(EncryptedFile_MultipleWriterMappings)
{
    const size_t count = 4096 * 64 * 2; // i.e. two metablocks of data
    const size_t increments = 100;
    TEST_PATH(path);

    {
        File w(path, File::mode_Write);
        w.set_encryption_key(test_util::crypt_key(true));
        w.resize(count);
        File::Map<char> map1(w, File::access_ReadWrite, count);
        File::Map<char> map2(w, File::access_ReadWrite, count);

        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i);
            map1.get_addr()[i] = 1;
            realm::util::encryption_write_barrier(map1, i);
        }

        // Since these are multiple mappings from one File, they should see
        // each other's writes without flushing in between
        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i, 1);
            ++map1.get_addr()[i];
            realm::util::encryption_write_barrier(map1, i);
            util::encryption_read_barrier(map2, i, 1);
            ++map2.get_addr()[i];
            realm::util::encryption_write_barrier(map2, i);
        }
    }

    File reader(path, File::mode_Read);
    reader.set_encryption_key(test_util::crypt_key(true));

    File::Map<char> read(reader, File::access_ReadOnly, count);
    util::encryption_read_barrier(read, 0, count);
    for (size_t i = 0; i < count; i += increments) {
        if (!CHECK_EQUAL(int(read.get_addr()[i]), 3))
            return;
    }
}

TEST(EncryptedFile_MultipleWriterFiles)
{
    const size_t count = 4096 * 64 * 2; // i.e. two metablocks of data
    const size_t increments = 100;
    TEST_PATH(path);

    {
        File w1(path, File::mode_Write);
        w1.set_encryption_key(test_util::crypt_key(true));
        w1.resize(count);
        File::Map<char> map1(w1, File::access_ReadWrite, count);

        File w2(path, File::mode_Update);
        w2.set_encryption_key(test_util::crypt_key(true));
        File::Map<char> map2(w2, File::access_ReadWrite, count);

        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i);
            map1.get_addr()[i] = 1;
            realm::util::encryption_write_barrier(map1, i);
        }
        map1.flush();

        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i, 1);
            ++map1.get_addr()[i];
            realm::util::encryption_write_barrier(map1, i);
            map1.flush();
            w2.get_encryption()->mark_data_as_possibly_stale();

            util::encryption_read_barrier(map2, i, 1);
            ++map2.get_addr()[i];
            realm::util::encryption_write_barrier(map2, i);
            map2.flush();
            w1.get_encryption()->mark_data_as_possibly_stale();
        }
    }

    File reader(path, File::mode_Read);
    reader.set_encryption_key(test_util::crypt_key(true));

    File::Map<char> read(reader, File::access_ReadOnly, count);
    util::encryption_read_barrier(read, 0, count);
    for (size_t i = 0; i < count; i += increments) {
        if (!CHECK_EQUAL(int(read.get_addr()[i]), 3))
            return;
    }
}

TEST(EncryptedFile_MultipleReaders)
{
    const size_t count = 4096 * 64 * 2; // i.e. two metablocks of data
    const size_t increments = 100;
    TEST_PATH(path);

    File w1(path, File::mode_Write);
    w1.set_encryption_key(test_util::crypt_key(true));
    w1.resize(count);
    File::Map<char> map1(w1, File::access_ReadWrite, count);
    File::Map<char> map2(w1, File::access_ReadOnly, count);

    File w2(path, File::mode_Read);
    w2.set_encryption_key(test_util::crypt_key(true));
    File::Map<char> map3(w2, File::access_ReadOnly, count);

    for (size_t i = 0; i < count; i += increments) {
        util::encryption_read_barrier(map1, i);
        map1.get_addr()[i] = 1;
        realm::util::encryption_write_barrier(map1, i);
    }
    map1.flush();

    // Bring both readers fully up to date
    util::encryption_read_barrier(map2, 0, count);
    util::encryption_read_barrier(map3, 0, count);

    for (size_t i = 0; i < count; i += increments) {
        util::encryption_read_barrier(map1, i, 1);
        ++map1.get_addr()[i];
        realm::util::encryption_write_barrier(map1, i);

        // map1 sees the new value because the write was performed via it
        // map2 was updated in the write barrier since it's the same File
        // map3 is viewing stale data but hasn't been told to refresh
        CHECK_EQUAL(map1.get_addr()[i], 2);
        CHECK_EQUAL(map2.get_addr()[i], 2);
        CHECK_EQUAL(map3.get_addr()[i], 1);

        // Read barrier is a no-op because of no call to mark_data_as_possibly_stale()
        util::encryption_read_barrier(map3, i, 1);
        CHECK_EQUAL(map3.get_addr()[i], 1);

        map1.flush(true);
        w2.get_encryption()->mark_data_as_possibly_stale();

        // Still see the old value since no read barrier
        CHECK_EQUAL(map3.get_addr()[i], 1);

        // Now finally brought up to date
        util::encryption_read_barrier(map3, i, 1);
        CHECK_EQUAL(map3.get_addr()[i], 2);
    }
}

TEST(EncryptedFile_IVsAreRereadOnlyWhenObserverIsPresent)
{
    TEST_PATH(path);
    const size_t page_size = 4096;
    const size_t size = page_size * 64;
    File w(path, File::mode_Write);
    w.set_encryption_key(test_util::crypt_key(true));
    w.resize(size);

    // Initialize all of the pages so iv1 is non-zero
    File::Map<char> map_w(w, File::access_ReadWrite, size);
    encryption_read_barrier(map_w, 0, size);
    encryption_write_barrier(map_w, 0, size);
    map_w.flush();

    File r(path, File::mode_Read);
    r.set_encryption_key(test_util::crypt_key(true));
    File::Map<char> map_r1(r, File::access_ReadOnly, size);
    File::Map<char> map_r2(r, File::access_ReadOnly, size);
    File::Map<char> map_r3(r, File::access_ReadOnly, size);

    struct : WriteObserver {
        bool no_concurrent_writer_seen() override
        {
            return true;
        }
    } r2_observer;
    map_r2.get_encrypted_mapping()->set_observer(&r2_observer);

    struct : WriteObserver {
        bool no_concurrent_writer_seen() override
        {
            return false;
        }
    } r3_observer;
    map_r3.get_encrypted_mapping()->set_observer(&r3_observer);

    // Reads the entire IV block and first page of data
    encryption_read_barrier(map_r1, 0, page_size);
    encryption_read_barrier(map_r2, 0, page_size);
    encryption_read_barrier(map_r3, 0, page_size);

    encryption_read_barrier(map_w, page_size, size - page_size);
    encryption_write_barrier(map_w, page_size, size - page_size);
    map_w.flush();

    // No observer, so it uses the cached IV/hmac
    CHECK_THROW(encryption_read_barrier(map_r1, page_size, 1), DecryptionFailed);
    // Observer says no concurrent writers, so it uses the cached IV/hmac
    CHECK_THROW(encryption_read_barrier(map_r2, page_size, 1), DecryptionFailed);
    // Observer says there are concurrent writers, so it rereads the IV after
    // decryption fails the first time
    encryption_read_barrier(map_r3, page_size, 1);
}

TEST(EncryptedFile_Truncation)
{
    TEST_PATH(path);
    const size_t page_size = 4096;
    const size_t size = page_size * 64;
    File w(path, File::mode_Write);
    w.set_encryption_key(test_util::crypt_key(true));
    w.resize(size);

    {
        // Initialize all of the pages so iv1 is non-zero
        File::Map<char> map(w, File::access_ReadWrite, size);
        encryption_read_barrier(map, 0, size);
        encryption_write_barrier(map, 0, size);
    }

    // Truncate and then re-expand the file
    w.resize(size / 2);
    w.resize(size);

    {
        File::Map<char> map(w, File::access_ReadOnly, size);
        // Trying to read the entire file fails because it's trying to read
        // uninitialized data
        CHECK_THROW(encryption_read_barrier(map, 0, size), DecryptionFailed);
        // Reading just the valid part works
        CHECK_NOTHROW(encryption_read_barrier(map, 0, size / 2));
    }

    {
        // Write mapping can read the entire file
        File::Map<char> map(w, File::access_ReadWrite, size);
        encryption_read_barrier(map, 0, size);
        encryption_write_barrier(map, 0, size);
    }
}

TEST(EncryptedFile_RacingReadAndWrite)
{
    TEST_PATH(path);
    static constexpr size_t page_size = 4096;
    static constexpr size_t page_count = 64;
    static constexpr size_t size = page_size * page_count;

    {
        // Initialize the file
        File w(path, File::mode_Write);
        w.set_encryption_key(test_util::crypt_key(true));
        w.resize(size);
        File::Map<char> map(w, File::access_ReadWrite, size);
        encryption_read_barrier(map, 0, size);
        encryption_write_barrier(map, 0, size);
        map.flush();
    }

    File w(path, File::mode_Update);
    // note: not setting encryption key
    // Flip some bits in the encrypted file to make it invalid
    for (File::SizeType pos = int(page_size); pos < w.get_size(); pos += page_size) {
        char c;
        w.read(pos, &c, 1);
        c = ~c;
        w.write(pos, &c, 1);
    }

    struct : WriteObserver {
        size_t page = 0;
        size_t count = 0;
        AESCryptor cryptor{test_util::crypt_key(true)};
        util::File* file;

        bool no_concurrent_writer_seen() override
        {
            // The first 15 read attempts we modify the page so that it
            // continues trying to reread past the normal limit of 5 attempts,
            // but we continue to leave the page in an invalid state
            if (++count < 15) {
                auto pos = (page + 1) * page_size + 1;
                char c;
                file->read(pos, &c, 1);
                ++c;
                file->write(pos, &c, 1);
                return false;
            }

            // Now we write valid encrypted data which will result in the
            // decryption succeeding
            count = 0;
            char buffer[page_size] = {0};
            cryptor.write(file->get_descriptor(), page * page_size, buffer);
            return false;
        }
    } observer;
    observer.file = &w;
    observer.cryptor.set_data_size(File::SizeType(size));

    File r(path, File::mode_Read);
    r.set_encryption_key(test_util::crypt_key(true));
    for (size_t i = 0; i < page_count; ++i) {
        observer.page = i;
        File::Map<char> map(r, i * page_size);
        map.get_encrypted_mapping()->set_observer(&observer);
        util::encryption_read_barrier(map, 0);
    }
}

#endif // REALM_ENABLE_ENCRYPTION
#endif // TEST_ENCRYPTED_FILE_MAPPING
