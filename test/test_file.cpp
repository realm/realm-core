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
#ifdef TEST_FILE

#include <map>
#include <ostream>
#include <sstream>

#include <realm/util/encrypted_file_mapping.hpp>
#include <realm/util/file.hpp>
#include <realm/util/file_mapper.hpp>

#include "test.hpp"

#if REALM_PLATFORM_APPLE
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


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


TEST(File_ExistsAndRemove)
{
    TEST_PATH(path);
    File(path, File::mode_Write);
    CHECK(File::exists(path));
    CHECK(File::try_remove(path));
    CHECK(!File::exists(path));
    CHECK(!File::try_remove(path));
}

TEST(File_IsSame)
{
    TEST_PATH(path_1);
    TEST_PATH(path_2);

    // exFAT does not allocate inode numbers until the file is first non-empty,
    // so all never-written-to files appear to be the same file
    File(path_1, File::mode_Write).resize(1);
    File(path_2, File::mode_Write).resize(1);

    File f1(path_1, File::mode_Append);
    File f2(path_1, File::mode_Read);
    File f3(path_2, File::mode_Append);

    CHECK(f1.is_same_file(f1));
    CHECK(f1.is_same_file(f2));
    CHECK(!f1.is_same_file(f3));
    CHECK(!f2.is_same_file(f3));
}


TEST(File_Streambuf)
{
    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        File::Streambuf b(&f);
        std::ostream out(&b);
        out << "Line " << 1 << std::endl;
        out << "Line " << 2 << std::endl;
    }
    {
        File f(path, File::mode_Read);
        char buffer[256];
        size_t n = f.read(0, buffer);
        std::string s_1(buffer, buffer + n);
        std::ostringstream out;
        out << "Line " << 1 << std::endl;
        out << "Line " << 2 << std::endl;
        std::string s_2 = out.str();
        CHECK(s_1 == s_2);
    }
}

TEST_TYPES(File_Map, std::true_type, std::false_type)
{
    TEST_PATH(path);
    const char data[4096] = "12345678901234567890";
    size_t len = strlen(data);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        f.resize(len);

        File::Map<char> map(f, File::access_ReadWrite, len);
        util::encryption_read_barrier(map, 0, len);
        memcpy(map.get_addr(), data, len);
        realm::util::encryption_write_barrier(map, 0, len);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        File::Map<char> map(f, File::access_ReadOnly, len);
        util::encryption_read_barrier(map, 0, len);
        CHECK(memcmp(map.get_addr(), data, len) == 0);
    }
}


TEST_TYPES(File_MapMultiplePages, std::true_type, std::false_type)
{
    // two blocks of IV tables
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        f.resize(count * sizeof(size_t));

        File::Map<size_t> map(f, File::access_ReadWrite, count * sizeof(size_t));
        util::encryption_read_barrier(map, 0, count);
        for (size_t i = 0; i < count; ++i)
            map.get_addr()[i] = i;
        realm::util::encryption_write_barrier(map, 0, count);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        File::Map<size_t> map(f, File::access_ReadOnly, count * sizeof(size_t));
        util::encryption_read_barrier(map, 0, count);
        for (size_t i = 0; i < count; ++i) {
            CHECK_EQUAL(map.get_addr()[i], i);
            if (map.get_addr()[i] != i)
                return;
        }
    }
}

TEST_TYPES(File_ReaderAndWriter_SingleFile, std::true_type, std::false_type)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);

    File file(path, File::mode_Write);
    file.set_encryption_key(crypt_key(TEST_TYPE::value));
    file.resize(count * sizeof(size_t));

    File::Map<size_t> write(file, File::access_ReadWrite, count * sizeof(size_t));
    File::Map<size_t> read(file, File::access_ReadOnly, count * sizeof(size_t));

    for (size_t i = 0; i < count; i += 100) {
        util::encryption_read_barrier(write, i, 1);
        write.get_addr()[i] = i;
        realm::util::encryption_write_barrier(write, i);
        util::encryption_read_barrier(read, i);
        if (!CHECK_EQUAL(read.get_addr()[i], i))
            return;
    }
}

TEST_TYPES(File_ReaderAndWriter_MulitpleFiles, std::true_type, std::false_type)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);

    File writer(path, File::mode_Write);
    writer.set_encryption_key(crypt_key(TEST_TYPE::value));
    writer.resize(count * sizeof(size_t));

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key(TEST_TYPE::value));
    CHECK_EQUAL(writer.get_size(), reader.get_size());

    File::Map<size_t> write(writer, File::access_ReadWrite, count * sizeof(size_t));
    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));

    for (size_t i = 0; i < count; i += 100) {
        util::encryption_read_barrier(write, i, 1);
        write.get_addr()[i] = i;
        realm::util::encryption_write_barrier(write, i);
        write.flush(true);
        if (auto encryption = reader.get_encryption())
            encryption->mark_data_as_possibly_stale();
        util::encryption_read_barrier(read, i);
        if (!CHECK_EQUAL(read.get_addr()[i], i))
            return;
    }
}

TEST_TYPES(File_Offset, std::true_type, std::false_type)
{
    const size_t size = page_size();
    const size_t count_per_page = size / sizeof(size_t);
    // two blocks of IV tables
    const size_t page_count = 256 * 2 / (size / 4096);

    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        f.resize(page_count * size);

        for (size_t i = 0; i < page_count; ++i) {
            File::Map<size_t> map(f, i * size, File::access_ReadWrite, size);
            for (size_t j = 0; j < count_per_page; ++j) {
                util::encryption_read_barrier(map, j, 1);
                map.get_addr()[j] = i * size + j;
                realm::util::encryption_write_barrier(map, j);
            }
        }
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key(TEST_TYPE::value));
        for (size_t i = 0; i < page_count; ++i) {
            File::Map<size_t> map(f, i * size, File::access_ReadOnly, size);
            for (size_t j = 0; j < count_per_page; ++j) {
                util::encryption_read_barrier(map, j);
                CHECK_EQUAL(map.get_addr()[j], i * size + j);
                if (map.get_addr()[j] != i * size + j)
                    return;
            }
        }
    }
}

TEST_TYPES(File_MultipleWriters_SingleFile, std::true_type, std::false_type)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;
    const size_t increments = 100;
    TEST_PATH(path);

    {
        File w(path, File::mode_Write);
        w.set_encryption_key(crypt_key(TEST_TYPE::value));
        w.resize(count * sizeof(size_t));
        File::Map<size_t> map1(w, File::access_ReadWrite, count * sizeof(size_t));
        File::Map<size_t> map2(w, File::access_ReadWrite, count * sizeof(size_t));

        // Place zeroes in selected places
        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i);
            map1.get_addr()[i] = 0;
            realm::util::encryption_write_barrier(map1, i);
        }

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
    reader.set_encryption_key(crypt_key(TEST_TYPE::value));

    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));
    util::encryption_read_barrier(read, 0, count);
    for (size_t i = 0; i < count; i += increments) {
        if (!CHECK_EQUAL(read.get_addr()[i], 2))
            return;
    }
}

TEST_TYPES(File_MultipleWriters_MultipleFiles, std::true_type, std::false_type)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;
    const size_t increments = 100;
    TEST_PATH(path);

    {
        File w1(path, File::mode_Write);
        w1.set_encryption_key(crypt_key(TEST_TYPE::value));
        w1.resize(count * sizeof(size_t));

        File w2(path, File::mode_Write);
        w2.set_encryption_key(crypt_key(TEST_TYPE::value));
        w2.resize(count * sizeof(size_t));

        File::Map<size_t> map1(w1, File::access_ReadWrite, count * sizeof(size_t));
        File::Map<size_t> map2(w2, File::access_ReadWrite, count * sizeof(size_t));

        // Place zeroes in selected places
        for (size_t i = 0; i < count; i += increments) {
            encryption_read_barrier(map1, i);
            map1.get_addr()[i] = 0;
            encryption_write_barrier(map1, i);
        }
        map1.flush();

        for (size_t i = 0; i < count; i += increments) {
            util::encryption_read_barrier(map1, i, 1);
            ++map1.get_addr()[i];
            encryption_write_barrier(map1, i);
            map1.flush(true);
            if (auto encryption = w2.get_encryption())
                encryption->mark_data_as_possibly_stale();

            util::encryption_read_barrier(map2, i, 1);
            ++map2.get_addr()[i];
            encryption_write_barrier(map2, i);
            map2.flush(true);
            if (auto encryption = w1.get_encryption())
                encryption->mark_data_as_possibly_stale();
        }
    }

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key(TEST_TYPE::value));

    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));
    util::encryption_read_barrier(read, 0, count);
    for (size_t i = 0; i < count; i += increments) {
        if (!CHECK_EQUAL(read.get_addr()[i], 2))
            return;
    }
}

TEST(File_SetEncryptionKey)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    const char key[64] = {0};

#if REALM_ENABLE_ENCRYPTION
    f.set_encryption_key(key); // should not throw
#else
    CHECK_THROW_EX(f.set_encryption_key(key), Exception, (e.code() == ErrorCodes::NotSupported));
#endif
}


TEST(File_ReadWrite)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key());
    f.resize(100);

    for (char i = 0; i < 100; ++i)
        f.write(i, &i, 1);
    for (char i = 0; i < 100; ++i) {
        char read;
        f.read(i, &read, 1);
        CHECK_EQUAL(i, read);
    }
}


TEST_TYPES(File_Resize, std::true_type, std::false_type)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key(TEST_TYPE::value));

    f.resize(page_size() * 2);
    CHECK_EQUAL(page_size() * 2, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, page_size() * 2);
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            util::encryption_read_barrier(m, i, 1);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }

        // Resizing away the first write is indistinguishable in encrypted files
        // from the process being interrupted before it does the first write,
        // but with subsequent writes it can tell that there was once valid
        // encrypted data there, so flush and write a second time
        m.sync();
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            util::encryption_read_barrier(m, i, 1);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }
    }

    f.resize(page_size());
    CHECK_EQUAL(page_size(), f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadOnly, page_size());
        for (unsigned int i = 0; i < page_size(); ++i) {
            util::encryption_read_barrier(m, i);
            CHECK_EQUAL(static_cast<unsigned char>(i), m.get_addr()[i]);
            if (static_cast<unsigned char>(i) != m.get_addr()[i])
                return;
        }
    }

    f.resize(page_size() * 2);
    CHECK_EQUAL(page_size() * 2, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, page_size() * 2);
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            util::encryption_read_barrier(m, i, 1);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }
    }
    {
        File::Map<unsigned char> m(f, File::access_ReadOnly, page_size() * 2);
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            util::encryption_read_barrier(m, i);
            CHECK_EQUAL(static_cast<unsigned char>(i), m.get_addr()[i]);
            if (static_cast<unsigned char>(i) != m.get_addr()[i])
                return;
        }
    }
}


TEST(File_NotFound)
{
    TEST_PATH(path);
    File file;
    CHECK_THROW_EX(file.open(path), FileAccessError, e.get_path() == std::string(path));
}


TEST(File_PathNotFound)
{
    File file;
    CHECK_THROW_EX(file.open(""), FileAccessError, e.code() == ErrorCodes::FileNotFound);
}


TEST(File_Exists)
{
    TEST_PATH(path);
    File file;
    file.open(path, File::mode_Write); // Create the file
    file.close();
    CHECK_THROW_EX(file.open(path, File::access_ReadWrite, File::create_Must, File::flag_Trunc), FileAccessError,
                   e.get_path() == std::string(path) && e.code() == ErrorCodes::FileAlreadyExists);
}


TEST(File_Move)
{
    TEST_PATH(path);
    File file_1(path, File::mode_Write);
    CHECK(file_1.is_attached());
    File file_2(std::move(file_1));
    CHECK_NOT(file_1.is_attached());
    CHECK(file_2.is_attached());
    file_1 = std::move(file_2);
    CHECK(file_1.is_attached());
    CHECK_NOT(file_2.is_attached());
}

TEST(File_PreallocResizingAPFSBug)
{
    TEST_PATH(path);
    File file(path, File::mode_Write);
    CHECK(file.is_attached());
    file.write(0, "aaaaaaaaaaaaaaaaaaaa"); // 20 a's
    // calling prealloc on a newly created file would sometimes fail on APFS with EINVAL via fcntl(F_PREALLOCATE)
    // this may not be the only way to trigger the error, but it does seem to be timing dependant.
    file.prealloc(100);
    CHECK_EQUAL(file.get_size(), 100);

    // this will change the file size, but likely won't preallocate more space since the first call to prealloc
    // will probably have allocated a whole 4096 block.
    file.prealloc(200);
    CHECK_EQUAL(file.get_size(), 200);
    file.write(22, "aa");
    file.prealloc(5020); // expands to another 4096 block
    constexpr size_t insert_pos = 5000;
    const char* insert_str = "hello";
    file.write(insert_pos, insert_str);
    CHECK_EQUAL(file.get_size(), 5020);
    constexpr size_t input_size = 6;
    char input[input_size];
    file.read(insert_pos, input, input_size);
    CHECK_EQUAL(strncmp(input, insert_str, input_size), 0);
}

TEST(File_parent_dir)
{
    std::map<std::string, std::string> mappings = {{"Unicorn🦄/file.cpp", "Unicorn🦄"},
                                                   {"", ""},
                                                   {"asdf", ""},
                                                   {"file.cpp", ""},
                                                   {"Unicorn🦄", ""},
                                                   {"parent/file.cpp", "parent"},
                                                   {"parent//file.cpp", "parent"},
                                                   {"parent///file.cpp", "parent"},
                                                   {"parent////file.cpp", "parent"},
                                                   {"1/2/3/4.cpp", "1/2/3"},
                                                   {"/1/2/3/4", "/1/2/3"}};
    for (auto [input, expected] : mappings) {
        std::string actual = File::parent_dir(input);
        CHECK_EQUAL(actual, expected);
        if (actual != expected) {
            realm::util::format(std::cout, "unexpected result '%1' for input '%2'", actual, input);
        }
    }
}

TEST(File_Temp)
{
    auto tmp_file_name = make_temp_file("foo");
    {
        File file1;
        file1.open(tmp_file_name, File::mode_Write);
        CHECK(file1.is_attached());
    }
    remove(tmp_file_name.c_str());
}

#endif // TEST_FILE
