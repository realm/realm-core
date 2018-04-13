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

#include <ostream>
#include <sstream>

#include <realm/util/file.hpp>
#include <realm/util/file_mapper.hpp>

#include "test.hpp"

#if REALM_PLATFORM_APPLE
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif


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
    {
        File f1(path_1, File::mode_Write);
        File f2(path_1, File::mode_Read);
        File f3(path_2, File::mode_Write);

        CHECK(f1.is_same_file(f1));
        CHECK(f1.is_same_file(f2));
        CHECK(!f1.is_same_file(f3));
        CHECK(!f2.is_same_file(f3));
    }
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
        size_t n = f.read(buffer);
        std::string s_1(buffer, buffer + n);
        std::ostringstream out;
        out << "Line " << 1 << std::endl;
        out << "Line " << 2 << std::endl;
        std::string s_2 = out.str();
        CHECK(s_1 == s_2);
    }
}


TEST(File_Map)
{
    TEST_PATH(path);
    const char data[4096] = "12345678901234567890";
    size_t len = strlen(data);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key());
        f.resize(len);

        File::Map<char> map(f, File::access_ReadWrite, len);
        realm::util::encryption_read_barrier(map, 0, len);
        memcpy(map.get_addr(), data, len);
        realm::util::encryption_write_barrier(map, 0, len);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key());
        File::Map<char> map(f, File::access_ReadOnly, len);
        realm::util::encryption_read_barrier(map, 0, len);
        CHECK(memcmp(map.get_addr(), data, len) == 0);
    }
}


TEST(File_MapMultiplePages)
{
    // two blocks of IV tables
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key());
        f.resize(count * sizeof(size_t));

        File::Map<size_t> map(f, File::access_ReadWrite, count * sizeof(size_t));
        realm::util::encryption_read_barrier(map, 0, count);
        for (size_t i = 0; i < count; ++i)
            map.get_addr()[i] = i;
        realm::util::encryption_write_barrier(map, 0, count);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key());
        File::Map<size_t> map(f, File::access_ReadOnly, count * sizeof(size_t));
        realm::util::encryption_read_barrier(map, 0, count);
        for (size_t i = 0; i < count; ++i) {
            CHECK_EQUAL(map.get_addr()[i], i);
            if (map.get_addr()[i] != i)
                return;
        }
    }
}

TEST(File_ReaderAndWriter)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);

    File writer(path, File::mode_Write);
    writer.set_encryption_key(crypt_key());
    writer.resize(count * sizeof(size_t));

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key());
    CHECK_EQUAL(writer.get_size(), reader.get_size());

    File::Map<size_t> write(writer, File::access_ReadWrite, count * sizeof(size_t));
    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));

    for (size_t i = 0; i < count; i += 100) {
        realm::util::encryption_read_barrier(write, i);
        write.get_addr()[i] = i;
        realm::util::encryption_write_barrier(write, i);
        realm::util::encryption_read_barrier(read, i);
        CHECK_EQUAL(read.get_addr()[i], i);
        if (read.get_addr()[i] != i)
            return;
    }
}

TEST(File_Offset)
{
    const size_t size = page_size();
    const size_t count_per_page = size / sizeof(size_t);
    // two blocks of IV tables
    const size_t page_count = 256 * 2 / (size / 4096);

    TEST_PATH(path);
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(crypt_key());
        f.resize(page_count * size);

        for (size_t i = 0; i < page_count; ++i) {
            File::Map<size_t> map(f, i * size, File::access_ReadWrite, size);
            for (size_t j = 0; j < count_per_page; ++j) {
                realm::util::encryption_read_barrier(map, j);
                map.get_addr()[j] = i * size + j;
                realm::util::encryption_write_barrier(map, j);
            }
        }
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key());
        for (size_t i = 0; i < page_count; ++i) {
            File::Map<size_t> map(f, i * size, File::access_ReadOnly, size);
            for (size_t j = 0; j < count_per_page; ++j) {
                realm::util::encryption_read_barrier(map, j);
                CHECK_EQUAL(map.get_addr()[j], i * size + j);
                if (map.get_addr()[j] != i * size + j)
                    return;
            }
        }
    }
}


TEST(File_MultipleWriters)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;
#if defined(_WIN32) && defined(REALM_ENABLE_ENCRYPTION)
    // This test runs really slow on Windows with encryption
    const size_t increments = 3000;
#else
    const size_t increments = 100;
#endif
    TEST_PATH(path);

    {
        File w1(path, File::mode_Write);
        w1.set_encryption_key(crypt_key());
        w1.resize(count * sizeof(size_t));

        File w2(path, File::mode_Write);
        w2.set_encryption_key(crypt_key());
        w2.resize(count * sizeof(size_t));

        File::Map<size_t> map1(w1, File::access_ReadWrite, count * sizeof(size_t));
        File::Map<size_t> map2(w2, File::access_ReadWrite, count * sizeof(size_t));

        for (size_t i = 0; i < count; i += increments) {
            realm::util::encryption_read_barrier(map1, i);
            ++map1.get_addr()[i];
            realm::util::encryption_write_barrier(map1, i);
            realm::util::encryption_read_barrier(map2, i);
            ++map2.get_addr()[i];
            realm::util::encryption_write_barrier(map2, i);
        }
    }

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key());

    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));
    realm::util::encryption_read_barrier(read, 0, count);
    for (size_t i = 0; i < count; i += increments) {
        CHECK_EQUAL(read.get_addr()[i], 2);
        if (read.get_addr()[i] != 2)
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
    CHECK_THROW(f.set_encryption_key(key), std::runtime_error);
#endif
}


TEST(File_ReadWrite)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key());
    f.resize(100);

    for (char i = 0; i < 100; ++i)
        f.write(&i, 1);
    f.seek(0);
    for (char i = 0; i < 100; ++i) {
        char read;
        f.read(&read, 1);
        CHECK_EQUAL(i, read);
    }
}


TEST(File_Resize)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key());

    f.resize(page_size() * 2);
    CHECK_EQUAL(page_size() * 2, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, page_size() * 2);
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            realm::util::encryption_read_barrier(m, i);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }

        // Resizing away the first write is indistinguishable in encrypted files
        // from the process being interrupted before it does the first write,
        // but with subsequent writes it can tell that there was once valid
        // encrypted data there, so flush and write a second time
        m.sync();
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            realm::util::encryption_read_barrier(m, i);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }
    }

    f.resize(page_size());
    CHECK_EQUAL(page_size(), f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadOnly, page_size());
        for (unsigned int i = 0; i < page_size(); ++i) {
            realm::util::encryption_read_barrier(m, i);
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
            realm::util::encryption_read_barrier(m, i);
            m.get_addr()[i] = static_cast<unsigned char>(i);
            realm::util::encryption_write_barrier(m, i);
        }
    }
    {
        File::Map<unsigned char> m(f, File::access_ReadOnly, page_size() * 2);
        for (unsigned int i = 0; i < page_size() * 2; ++i) {
            realm::util::encryption_read_barrier(m, i);
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
    CHECK_THROW_EX(file.open(path), File::NotFound, e.get_path() == std::string(path));
}


TEST(File_Exists)
{
    TEST_PATH(path);
    File file;
    file.open(path, File::mode_Write); // Create the file
    file.close();
    CHECK_THROW_EX(file.open(path, File::access_ReadWrite, File::create_Must, File::flag_Trunc), File::Exists,
                   e.get_path() == std::string(path));
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


TEST(File_PreallocResizing)
{
    TEST_PATH(path);
    File file(path, File::mode_Write);
    CHECK(file.is_attached());
    file.set_encryption_key(crypt_key());
    file.prealloc(0); // this is allowed
    CHECK_EQUAL(file.get_size(), 0);
    file.prealloc(100);
    CHECK_EQUAL(file.get_size(), 100);
    file.prealloc(50);
    CHECK_EQUAL(file.get_size(), 100); // prealloc does not reduce size

    // To expose the preallocation bug, we need to iterate over a large numbers, less than 4096.
    // If the bug is present, we will allocate additional space to the file on every call, but if it is
    // not present, the OS will preallocate 4096 only on the first call.
    constexpr size_t init_size = 2048;
    constexpr size_t dest_size = 3000;
    for (size_t prealloc_space = init_size; prealloc_space <= dest_size; ++prealloc_space) {
        file.prealloc(prealloc_space);
        CHECK_EQUAL(file.get_size(), prealloc_space);
    }

#if REALM_PLATFORM_APPLE
    int fd = ::open(path.c_str(), O_RDONLY);
    CHECK(fd >= 0);
    struct stat statbuf;
    CHECK(fstat(fd, &statbuf) == 0);
    size_t allocated_size = statbuf.st_blocks;
    CHECK_EQUAL(statbuf.st_size, dest_size);
    CHECK(!int_multiply_with_overflow_detect(allocated_size, S_BLKSIZE));

    // When performing prealloc, the OS has the option to preallocate more than the requeted space
    // but we need to check that the preallocated space is within a reasonable bound.
    // If space is being incorrectly preallocated (growing on each call) then we will have more than 3000KB
    // of preallocated space, but if it is being allocated correctly (only when we need to expand) then we'll have
    // a multiple of the optimal file system I/O operation (`stat -f %k .`) which is 4096 on HSF+.
    // To give flexibility for file system prealloc implementations we check that the preallocated space is within
    // at least 16 times the nominal requested size.
    CHECK_LESS(allocated_size, 4096 * 16);

    CHECK(::close(fd) == 0);
#endif
}


#ifndef _WIN32
TEST(File_GetUniqueID)
{
    TEST_PATH(path_1);
    TEST_PATH(path_2);
    TEST_PATH(path_3);

    File file1_1;
    File file1_2;
    File file2_1;
    file1_1.open(path_1, File::mode_Write);
    file1_2.open(path_1, File::mode_Read);
    file2_1.open(path_2, File::mode_Write);

    File::UniqueID uid1_1 = file1_1.get_unique_id();
    File::UniqueID uid1_2 = file1_2.get_unique_id();
    File::UniqueID uid2_1 = file2_1.get_unique_id();
    File::UniqueID uid2_2;
    CHECK(File::get_unique_id(path_2, uid2_2));

    CHECK(uid1_1 == uid1_2);
    CHECK(uid2_1 == uid2_2);
    CHECK(uid1_1 != uid2_1);

    // Path doesn't exist
    File::UniqueID uid3_1;
    CHECK_NOT(File::get_unique_id(path_3, uid3_1));

    // Test operator<
    File::UniqueID uid4_1{0, 5};
    File::UniqueID uid4_2{1, 42};
    CHECK(uid4_1 < uid4_2);
    CHECK_NOT(uid4_2 < uid4_1);

    uid4_1 = {0, 1};
    uid4_2 = {0, 2};
    CHECK(uid4_1 < uid4_2);
    CHECK_NOT(uid4_2 < uid4_1);

    uid4_1 = uid4_2;
    CHECK_NOT(uid4_1 < uid4_2);
    CHECK_NOT(uid4_2 < uid4_1);
}
#endif

#endif // TEST_FILE
