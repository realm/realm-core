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
#include "util/spawned_process.hpp"

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
const uint8_t test_key[] = "1234567890123456789012345678901123456789012345678901234567890123";
}

TEST(EncryptedFile_CryptorBasic)
{
    TEST_PATH(path);

    AESCryptor cryptor(test_key);
    cryptor.set_file_size(16);
    const char data[4096] = "test data";
    char buffer[4096];

    File file(path, realm::util::File::mode_Write);
    cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    cryptor.read(file.get_descriptor(), 0, buffer, sizeof(buffer));
    CHECK(memcmp(buffer, data, strlen(data)) == 0);
}

TEST(EncryptedFile_CryptorRepeatedWrites)
{
    TEST_PATH(path);
    AESCryptor cryptor(test_key);
    cryptor.set_file_size(16);

    const char data[4096] = "test data";
    char raw_buffer_1[8192] = {0}, raw_buffer_2[8192] = {0};
    File file(path, realm::util::File::mode_Write);

    cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    file.seek(0);
    ssize_t actual_read_1 = file.read(raw_buffer_1, sizeof(raw_buffer_1));
    CHECK_EQUAL(actual_read_1, sizeof(raw_buffer_1));

    cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    file.seek(0);
    ssize_t actual_read_2 = file.read(raw_buffer_2, sizeof(raw_buffer_2));
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
        cryptor.set_file_size(16);
        cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    }
    {
        AESCryptor cryptor(test_key);
        cryptor.set_file_size(16);
        cryptor.read(file.get_descriptor(), 0, buffer, sizeof(buffer));
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
        cryptor.set_file_size(16);
        cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    }

    // Fake an interrupted write which updates the IV table but not the data
    char buffer[4096];
    file.seek(0);
    size_t actual_pread = file.read(buffer, 64);
    CHECK_EQUAL(actual_pread, 64);

    memcpy(buffer + 32, buffer, 32);
    buffer[5]++; // first byte of "hmac1" field in iv table
    file.seek(0);
    file.write(buffer, 64);

    {
        AESCryptor cryptor(test_key);
        cryptor.set_file_size(16);
        cryptor.read(file.get_descriptor(), 0, buffer, sizeof(buffer));
        CHECK(memcmp(buffer, data, strlen(data)) == 0);
    }
}

TEST(EncryptedFile_LargePages)
{
    TEST_PATH(path);

    char data[4096 * 4];
    for (size_t i = 0; i < sizeof(data); ++i)
        data[i] = static_cast<char>(i);

    AESCryptor cryptor(test_key);
    cryptor.set_file_size(sizeof(data));
    char buffer[sizeof(data)];

    File file(path, realm::util::File::mode_Write);
    cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
    cryptor.read(file.get_descriptor(), 0, buffer, sizeof(buffer));
    CHECK(memcmp(buffer, data, sizeof(data)) == 0);
}

TEST(EncryptedFile_IVRefreshing)
{
    using IVPageStates = realm::util::FlatMap<size_t, IVRefreshState>;
    constexpr size_t block_size = 4096;
    constexpr size_t blocks_per_metadata_block = 64;
    const size_t pages_per_metadata_block = block_size * blocks_per_metadata_block / page_size();

    auto verify_page_states = [&](const IVPageStates& states, off_t data_pos,
                                  std::vector<size_t> expected_pages_refreshed) {
        size_t start_page_ndx = ((data_pos / block_size) / blocks_per_metadata_block) * blocks_per_metadata_block *
                                block_size / page_size();
        size_t end_page_ndx = (((data_pos / block_size) + blocks_per_metadata_block) / blocks_per_metadata_block) *
                              blocks_per_metadata_block * block_size / page_size();

        CHECK_EQUAL(states.size(), end_page_ndx - start_page_ndx);
        for (size_t ndx = start_page_ndx; ndx < end_page_ndx; ++ndx) {
            CHECK_EQUAL(states.count(ndx), 1);
            bool expected_refresh = std::find(expected_pages_refreshed.begin(), expected_pages_refreshed.end(),
                                              ndx) != expected_pages_refreshed.end();
            CHECK(states.at(ndx) == (expected_refresh ? IVRefreshState::RequiresRefresh : IVRefreshState::UpToDate));
        }
    };

    TEST_PATH(path);
    // enough data to span two metadata blocks
    constexpr size_t data_size = block_size * blocks_per_metadata_block * 2;
    const size_t num_pages = data_size / page_size();
    char data[block_size];
    for (size_t i = 0; i < sizeof(data); ++i)
        data[i] = static_cast<char>(i);

    AESCryptor cryptor(test_key);
    cryptor.set_file_size(off_t(data_size));
    File file(path, realm::util::File::mode_Write);
    const FileDesc fd = file.get_descriptor();

    auto make_external_write_at_pos = [&](off_t data_pos) -> size_t {
        const size_t begin_write_block = data_pos / block_size * block_size;
        const size_t ndx_in_block = data_pos % block_size;
        AESCryptor cryptor2(test_key);
        cryptor2.set_file_size(off_t(data_size));
        cryptor2.read(fd, off_t(begin_write_block), data, block_size);
        ++data[ndx_in_block];
        cryptor2.write(fd, off_t(begin_write_block), data, block_size);
        return data_pos / page_size();
    };

    for (size_t i = 0; i < data_size; i += block_size) {
        cryptor.write(fd, off_t(i), data, block_size);
    }

    IVPageStates states = cryptor.refresh_ivs(fd, 0, 0, num_pages);
    std::vector<size_t> pages_needing_refresh = {};
    for (size_t i = 0; i < pages_per_metadata_block; ++i) {
        pages_needing_refresh.push_back(i);
    }
    // initial call requires refreshing all pages in range
    verify_page_states(states, 0, pages_needing_refresh);
    states = cryptor.refresh_ivs(fd, 0, 0, num_pages);
    // subsequent call does not require refreshing anything
    verify_page_states(states, 0, {});

    pages_needing_refresh = {};
    for (size_t i = 0; i < pages_per_metadata_block; ++i) {
        pages_needing_refresh.push_back(i + pages_per_metadata_block);
    }
    off_t read_data_pos = off_t(pages_per_metadata_block * page_size());
    states = cryptor.refresh_ivs(fd, read_data_pos, pages_per_metadata_block, num_pages);
    verify_page_states(states, read_data_pos, pages_needing_refresh);
    states = cryptor.refresh_ivs(fd, read_data_pos, pages_per_metadata_block, num_pages);
    verify_page_states(states, read_data_pos, {});

    read_data_pos = off_t(data_size / 2);
    size_t read_page_ndx = read_data_pos / page_size();
    states = cryptor.refresh_ivs(fd, read_data_pos, read_page_ndx, num_pages);
    verify_page_states(states, read_data_pos, {});

    read_data_pos = off_t(data_size - 1);
    read_page_ndx = read_data_pos / page_size();
    states = cryptor.refresh_ivs(fd, read_data_pos, read_page_ndx, num_pages);
    verify_page_states(states, read_data_pos, {});

    // write at pos 0, read half way through the first page
    make_external_write_at_pos(0);
    read_data_pos = off_t(page_size() / 2);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {0});

    // write at end of first page, read half way through first page
    make_external_write_at_pos(off_t(page_size() - 1));
    read_data_pos = off_t(page_size() / 2);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {0});

    // write at beginning of second page, read in first page
    make_external_write_at_pos(off_t(page_size()));
    read_data_pos = off_t(page_size() / 2);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {1});

    // write at last page of first metadata block, read in first page
    size_t page_needing_refresh = make_external_write_at_pos(blocks_per_metadata_block * block_size - 1);
    read_data_pos = off_t(page_size() / 2);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {page_needing_refresh});

    // test truncation of end_page: write to first page, and last page of first metadata block, read in first page,
    // but set the end page index lower than the last write
    make_external_write_at_pos(0);
    page_needing_refresh = make_external_write_at_pos(blocks_per_metadata_block * block_size - 1);
    REALM_ASSERT(page_needing_refresh >= 1); // this test assumes page_size is < 64 * block_size
    read_data_pos = off_t(0);
    constexpr size_t end_page_index = 1;
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, end_page_index);
    CHECK_EQUAL(states.size(), 1);
    CHECK_EQUAL(states.count(size_t(0)), 1);
    CHECK(states[0] == IVRefreshState::RequiresRefresh);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, 0, {page_needing_refresh});

    // write to a block indexed to the second metadata block
    page_needing_refresh = make_external_write_at_pos(blocks_per_metadata_block * block_size);
    // a read anywhere in the first metadata block domain does not require refresh
    read_data_pos = off_t(page_size() / 2);
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {});
    // but a read in a page controlled by the second metadata block does require a refresh
    read_data_pos = off_t(blocks_per_metadata_block * block_size);
    states = cryptor.refresh_ivs(fd, read_data_pos, page_needing_refresh, num_pages);
    verify_page_states(states, read_data_pos, {page_needing_refresh});

    // write to the last byte of data
    page_needing_refresh = make_external_write_at_pos(data_size - 1);
    // a read anywhere in the first metadata block domain does not require refresh
    read_data_pos = 0;
    states = cryptor.refresh_ivs(fd, read_data_pos, 0, num_pages);
    verify_page_states(states, read_data_pos, {});
    // but a read in a page controlled by the second metadata block does require a refresh
    read_data_pos = off_t(data_size - 1);
    states = cryptor.refresh_ivs(fd, read_data_pos, page_needing_refresh, num_pages);
    verify_page_states(states, read_data_pos, {page_needing_refresh});
}

static void check_attach_and_read(const char* key, const std::string& path, size_t num_entries)
{
    try {
        auto hist = make_in_realm_history();
        DBOptions options(key);
        auto sg = DB::create(*hist, path, options);
        auto rt = sg->start_read();
        auto foo = rt->get_table("foo");
        auto pk_col = foo->get_primary_key_column();
        REALM_ASSERT_3(foo->size(), ==, num_entries);
        REALM_ASSERT_3(foo->where().equal(pk_col, util::format("name %1", num_entries - 1).c_str()).count(), ==, 1);
    }
    catch (const std::exception& e) {
        auto fs = File::get_size_static(path);
        util::format(std::cout, "Error for num_entries %1 with page_size of %2 on file of size %3\n%4", num_entries,
                     page_size(), fs, e.what());
        throw;
    }
}

// This test changes the global page_size() and should not run with other tests.
// It checks that an encrypted Realm is portable between systems with a different page size
NONCONCURRENT_TEST(EncryptedFile_Portablility)
{
    const char* key = test_util::crypt_key(true);
    // The idea here is to incrementally increase the allocations in the Realm
    // such that the top ref written eventually crosses over the block_size and
    // page_size() thresholds. This has caught faulty top_ref + size calculations.
    std::vector<size_t> test_sizes;
#if TEST_DURATION == 0
    test_sizes.resize(100);
    std::iota(test_sizes.begin(), test_sizes.end(), 500);
    // The allocations are not controlled, but at the time of writing this test
    // 539 objects produced a file of size 16384 while 540 objects produced a file of size 20480
    // so at least one threshold is crossed here, though this may change if the allocator changes
    // or if compression is implemented
#else
    test_sizes.resize(5000);
    std::iota(test_sizes.begin(), test_sizes.end(), 500);
#endif

    test_sizes.push_back(1); // check the lower limit
    for (auto num_entries : test_sizes) {
        TEST_PATH(path);
        {
            // create the Realm with the smallest supported page_size() of 4096
            OnlyForTestingPageSizeChange change_page_size(4096);
            Group g;
            TableRef foo = g.add_table_with_primary_key("foo", type_String, "name", false);
            for (size_t i = 0; i < num_entries; ++i) {
                foo->create_object_with_primary_key(util::format("name %1", i));
            }
            g.write(path, key);
            // size_t fs = File::get_size_static(path);
            // util::format(std::cout, "write of %1 objects produced a file of size %2\n", num_entries, fs);
        }
        {
            OnlyForTestingPageSizeChange change_page_size(8192);
            check_attach_and_read(key, path, num_entries);
        }
        {
            OnlyForTestingPageSizeChange change_page_size(16384);
            check_attach_and_read(key, path, num_entries);
        }

        // check with the native page_size (which is probably redundant with one of the above)
        // and check that a write works correctly
        auto history = make_in_realm_history();
        DBOptions options(key);
        DBRef db = DB::create(*history, path, options);
        auto wt = db->start_write();
        TableRef bar = wt->get_or_add_table_with_primary_key("bar", type_String, "pk");
        bar->create_object_with_primary_key("test");
        wt->commit();
        check_attach_and_read(key, path, num_entries);
    }
}

static void verify_data(const Group& g)
{
    g.verify();
    // not all types have implemented verify() and our goal
    // here is to access all data, so we use to_json() to make
    // sure that all pages can be decrypted
    std::stringstream ss;
    g.to_json(ss);
}

NONCONCURRENT_TEST(Encrypted_empty_blocks)
{
    const char* key = test_util::crypt_key(true);

    TEST_PATH(path);
    DBOptions options(key);
    std::string pk_longer_than_block_size = std::string(5, 'a');
    std::string more_than_1_block = std::string(5000, 'b');

    {
        // create an initial state, 1 4096 block file
        // (total file size is: 4096 metadata block + 4096 data block = 8192)
        OnlyForTestingPageSizeChange change_page_size(4096);
        auto db = DB::create(path, options);
        WriteTransaction wt(db);
        auto table = wt.get_group().add_table_with_primary_key("table_foo", type_String, "pk");
        table->create_object_with_primary_key(Mixed{pk_longer_than_block_size});
        wt.commit();
    }
    // Now extend the file, adding 3 empty blocks to a total of 16384
    // This happens without a write transaction during DB::open
    // during alloc.align_filesize_for_mmap()
    OnlyForTestingPageSizeChange change_page_size(16384);
    auto db_16 = DB::create(path, options);
    ReadTransaction rt(db_16);
    verify_data(rt.get_group());
    ReadTransaction pin(db_16);

    {
        // open with 4k page size again, the file cannot be truncated, because
        // the presence of the previous reader means we are not the session initiator
        // write at least one extra block so we can verify it is read later on
        OnlyForTestingPageSizeChange change_again_size(4096);
        auto db_4k = DB::create(path, options);
        WriteTransaction wt(db_4k);
        auto table = wt.get_group().get_table("table_foo");
        table->create_object_with_primary_key(Mixed{more_than_1_block});
        verify_data(wt.get_group());
        wt.commit();
    }

    // change back to 16k page size and advance the reader
    // this requires the new block to be decrypted
    OnlyForTestingPageSizeChange change_third(16384);
    pin.get_group().verify();
    // advance and make sure the new page is read correctly from disk
    pin = ReadTransaction(db_16);
    verify_data(pin.get_group());

    // open a third instance with 16k page size
    // to verify that an external reader can also see all the data
    auto another_db = DB::create(path, options);
    WriteTransaction wt16(another_db);
    auto table = wt16.get_group().get_table("table_foo");
    table->create_object_with_primary_key(Mixed{"baz"});
    verify_data(wt16.get_group());
    wt16.commit();
}

#if !REALM_ANDROID && !REALM_IOS
// spawn process test is not supported on ios/android
NONCONCURRENT_TEST_IF(Encrypted_multiprocess_different_page_sizes, testing_supports_spawn_process)
{
    const char* key = test_util::crypt_key(true);
    DBOptions options(key);
    SHARED_GROUP_TEST_PATH(path);
    constexpr size_t num_transactions = 100;
    constexpr size_t num_objs_per_transaction = 10;

    if (test_util::SpawnedProcess::is_parent()) {
        std::unique_ptr<Replication> hist = make_in_realm_history();
        DBRef sg = DB::create(*hist, path, options);
        if (test_util::SpawnedProcess::is_parent()) {
            WriteTransaction wt(sg);
            TableRef tr = wt.get_group().add_table_with_primary_key("Foo", type_String, "pk");
            for (size_t j = 0; j < num_objs_per_transaction; j++) {
                tr->create_object_with_primary_key(util::format("parent_init_%1", j));
            }
            wt.commit();
        }
        auto process = test_util::spawn_process(test_context.test_details.test_name, "external_writer");
        for (size_t i = 0; i < num_transactions; ++i) {
            WriteTransaction wt(sg);
            TableRef tr = wt.get_table("Foo");
            for (size_t j = 0; j < num_objs_per_transaction; j++) {
                tr->create_object_with_primary_key(util::format("parent_%1_%2", i, j));
            }
            verify_data(wt.get_group());
            wt.commit();
        }
        process->wait_for_child_to_finish();
        ReadTransaction rt(sg);
        verify_data(rt.get_group());
    }
    else {
        OnlyForTestingPageSizeChange change_page(4096);

        try {
            std::unique_ptr<Replication> hist = make_in_realm_history();
            DBRef sg = DB::create(*hist, path, options);
            for (size_t i = 0; i < num_transactions; ++i) {
                WriteTransaction wt(sg);
                TableRef tr = wt.get_table("Foo");
                for (size_t j = 0; j < num_objs_per_transaction; j++) {
                    tr->create_object_with_primary_key(util::format("child_%1_%2", i, j));
                }
                verify_data(wt.get_group());
                wt.commit();
            }
        }
        catch (const std::exception& e) {
            REALM_ASSERT_EX(false, e.what());
            static_cast<void>(e); // e is unused without assertions on
        }

        exit(0);
    }
}

#endif // !REALM_ANDROID && !REALM_IOS

TEST_TYPES(Encrypted_truncation_is_not_an_error, std::true_type, std::false_type)
{
    class TestWriteObserver : public WriteObserver {
    public:
        bool no_concurrent_writer_seen() override
        {
            if (m_callback) {
                m_callback();
            }
            // simulate that a concurrent writer is seen
            // this forces the retry to wait for the max period
            // before ultimately giving up
            return false;
        }
        util::UniqueFunction<void()> m_callback;
    };

    std::unique_ptr<TestWriteObserver> observer;
    if (TEST_TYPE::value) {
        observer = std::make_unique<TestWriteObserver>();
    }

    TEST_PATH(path);
    constexpr size_t block_size = 4096;

    char data[block_size * 3];
    for (size_t i = 0; i < sizeof(data); ++i)
        data[i] = static_cast<char>(i);

    AESCryptor cryptor(test_key);
    cryptor.set_file_size(sizeof(data));
    char buffer[sizeof(data)];
    File file(path, realm::util::File::mode_Write);

    auto verify_blocks_with_middle_zerod = [&]() {
        for (size_t i = 0; i < 3 * block_size; ++i) {
            // middle block is zero'd out
            if (i >= block_size && i < 2 * block_size) {
                CHECK_EQUAL(buffer[i], 0);
                continue;
            }
            // but the blocks on either side are valid
            CHECK_EQUAL(buffer[i], static_cast<char>(i));
        }
    };
    auto read_to_buffer = [&]() -> size_t {
        return cryptor.read(file.get_descriptor(), 0, buffer, sizeof(buffer), observer.get());
    };

    auto write_encrypted_data = [&]() {
        cryptor.write(file.get_descriptor(), 0, data, sizeof(data));
        size_t bytes_read = read_to_buffer();
        CHECK_EQUAL(bytes_read, 3 * block_size);
        CHECK(memcmp(buffer, data, sizeof(data)) == 0);
    };

    // first write: iv2 is still zero, only the first block can be read
    write_encrypted_data();
    // zero out the second block
    file.seek(2 * block_size);
    memset(buffer, '\0', block_size);
    file.write(buffer, block_size);
    // this hits the case of "the very first write was interrupted"
    // we know this because iv2 is still 0
    size_t bytes_read = read_to_buffer();
    // reading the second block failed, but it was not a decryption error
    // this treats the second block as uninitialized data
    CHECK_EQUAL(bytes_read, block_size);

    // second write: iv2 is set now, but still only the first block can be read
    write_encrypted_data();
    // zero out the second block
    file.seek(2 * block_size);
    memset(buffer, '\0', block_size);
    file.write(buffer, block_size);
    bytes_read = read_to_buffer();
    // reading the second block failed, but it was not a decryption error
    // this treats the second block as uninitialized data
    CHECK_EQUAL(bytes_read, 3 * block_size);
    verify_blocks_with_middle_zerod();

    // third write: on a hmac check failure and no iv2 fallback, throw an exception
    write_encrypted_data();
    // zero out the second block, except for one byte
    file.seek(2 * block_size);
    memset(buffer, '\0', block_size);
    buffer[block_size - 1] = '\1';
    file.write(buffer, block_size);
    // this is a decryption error because the hmac check fails and it is not an entirely zero'd block
    // with an observer present, this takes an entire 5 seconds
    CHECK_THROW(read_to_buffer(), DecryptionFailed);

    // fourth write: if an uninitialized block is in the middle of
    // two valid blocks but it also has an iv1=0 all 3 blocks can be read
    write_encrypted_data();
    // zero out the second block
    file.seek(2 * block_size);
    memset(buffer, '\0', block_size);
    file.write(buffer, block_size);
    // now set the ivs of block 2 to 0
    constexpr size_t metadata_size = 64; // sizeof(iv_table)
    file.seek(1 * metadata_size);
    file.write(buffer, metadata_size);
    // force refresh the iv cache
    auto refresh_states = cryptor.refresh_ivs(file.get_descriptor(), 0, 0, 1);
    CHECK_EQUAL(refresh_states.size(), 1);
    CHECK_EQUAL(refresh_states.count(size_t(0)), 1);
    CHECK(refresh_states[0] == IVRefreshState::RequiresRefresh);
    bytes_read = read_to_buffer();
    CHECK_EQUAL(bytes_read, 3 * block_size);
    verify_blocks_with_middle_zerod();

    if (observer) {
        // simulate a second writer finally finishing the page write that matches the written iv
        char encrypted_block_copy[block_size];
        write_encrypted_data();
        file.seek(2 * block_size);
        file.read(encrypted_block_copy, block_size);
        file.seek(2 * block_size);
        file.write("data corruption!", 16);

        size_t num_retries = 0;
        bool did_write = false;
        observer->m_callback = [&]() {
            if (++num_retries >= 10 && !did_write) {
                file.seek(2 * block_size);
                file.write(encrypted_block_copy, block_size);
                did_write = true;
            }
        };
        // the retry logic does eventually succeed once the page data is restored
        bytes_read = read_to_buffer();
        CHECK_EQUAL(bytes_read, 3 * block_size);
        CHECK(memcmp(buffer, data, sizeof(data)) == 0);
    }
}


// The following can be used to debug encrypted customer Realm files
/*
static unsigned int hex_char_to_bin(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    throw std::invalid_argument("Illegal key (not a hex digit)");
}

static unsigned int hex_to_bin(char first, char second)
{
    return (hex_char_to_bin(first) << 4) | hex_char_to_bin(second);
}

ONLY(OpenEncrypted)
{
    std::string path = "my_path.realm";
    const char* hex_key = "792cddc553a0452ee893e7583735d4e9f0942d6c8404398f612fd5e415e115e19443aa3be112e072f5fc22b7e2"
                          "471a11ad2924c19413d84d19c32179c1063bd2";
    char crypt_key[64];
    for (int idx = 0; idx < 64; ++idx) {
        crypt_key[idx] = hex_to_bin(hex_key[idx * 2], hex_key[idx * 2 + 1]);
    }
    std::unique_ptr<Replication> hist_w(make_in_realm_history());

    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);
    DBOptions options(crypt_key);
    DBRef db = DB::create(*hist_w, temp_copy, options);

    TransactionRef rt = db->start_read();
    TableKeys table_keys = rt->get_table_keys();
    for (auto key : table_keys) {
        TableRef table = rt->get_table(key);
        util::format(std::cout, "table %1 has %2 rows\n", table->get_name(), table->size());
        table->to_json(std::cout);
        table->verify();
        std::cout << std::endl;
    }
}
*/

#endif // REALM_ENABLE_ENCRYPTION
#endif // TEST_ENCRYPTED_FILE_MAPPING
