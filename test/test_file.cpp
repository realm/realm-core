#include "testsettings.hpp"
#ifdef TEST_FILE

#include <sstream>
#include <ostream>

#include <realm/util/file.hpp>

#include "test.hpp"
#include "crypt_key.hpp"

using namespace realm::util;


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
        std::string s_1(buffer, buffer+n);
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
        f.set_encryption_key(crypt_key(true));
        f.resize(len);

        File::Map<char> map(f, File::access_ReadWrite, len);
        memcpy(map.get_addr(), data, len);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key(true));
        File::Map<char> map(f, File::access_ReadOnly, len);
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
        f.set_encryption_key(crypt_key(true));
        f.resize(count * sizeof(size_t));

        File::Map<size_t> map(f, File::access_ReadWrite, count * sizeof(size_t));
        for (size_t i = 0; i < count; ++i)
            map.get_addr()[i] = i;
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(crypt_key(true));
        File::Map<size_t> map(f, File::access_ReadOnly, count * sizeof(size_t));
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
    writer.set_encryption_key(crypt_key(true));
    writer.resize(count * sizeof(size_t));

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key(true));
    CHECK_EQUAL(writer.get_size(), reader.get_size());

    File::Map<size_t> write(writer, File::access_ReadWrite, count * sizeof(size_t));
    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));

    for (size_t i = 0; i < count; i += 100) {
        write.get_addr()[i] = i;
        CHECK_EQUAL(read.get_addr()[i], i);
        if (read.get_addr()[i] != i)
            return;
    }
}

TEST(File_MultipleWriters)
{
    const size_t count = 4096 / sizeof(size_t) * 256 * 2;

    TEST_PATH(path);

    {
        File w1(path, File::mode_Write);
        w1.set_encryption_key(crypt_key(true));
        w1.resize(count * sizeof(size_t));

        File w2(path, File::mode_Write);
        w2.set_encryption_key(crypt_key(true));
        w2.resize(count * sizeof(size_t));

        File::Map<size_t> map1(w1, File::access_ReadWrite, count * sizeof(size_t));
        File::Map<size_t> map2(w2, File::access_ReadWrite, count * sizeof(size_t));

        for (size_t i = 0; i < count; i += 100) {
            ++map1.get_addr()[i];
            ++map2.get_addr()[i];
        }
    }

    File reader(path, File::mode_Read);
    reader.set_encryption_key(crypt_key(true));

    File::Map<size_t> read(reader, File::access_ReadOnly, count * sizeof(size_t));

    for (size_t i = 0; i < count; i += 100) {
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

#ifdef REALM_ENABLE_ENCRYPTION
    f.set_encryption_key(key); // should not throw
#else
    CHECK_THROW(f.set_encryption_key(key), std::runtime_error);
#endif
}

#ifndef _WIN32

TEST(File_ReadWrite)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key(true));
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

#endif

TEST(File_Resize)
{
    TEST_PATH(path);
    File f(path, File::mode_Write);
    f.set_encryption_key(crypt_key(true));

    f.resize(8192);
    CHECK_EQUAL(8192, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, 8192);
        for (int i = 0; i < 8192; ++i)
            m.get_addr()[i] = static_cast<unsigned char>(i);

        // Resizing away the first write is indistinguishable in encrypted files
        // from the process being interrupted before it does the first write,
        // but with subsequent writes it can tell that there was once valid
        // encrypted data there, so flush and write a second time
        m.sync(f);
        for (int i = 0; i < 8192; ++i)
            m.get_addr()[i] = static_cast<unsigned char>(i);
    }

    f.resize(4096);
    CHECK_EQUAL(4096, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, 4096);
        for (int i = 0; i < 4096; ++i) {
            CHECK_EQUAL(static_cast<unsigned char>(i), m.get_addr()[i]);
            if (static_cast<unsigned char>(i) != m.get_addr()[i])
                return;
        }
    }

    f.resize(8192);
    CHECK_EQUAL(8192, f.get_size());
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, 8192);
        for (int i = 0; i < 8192; ++i)
            m.get_addr()[i] = static_cast<unsigned char>(i);
    }
    {
        File::Map<unsigned char> m(f, File::access_ReadWrite, 8192);
        for (int i = 0; i < 8192; ++i) {
            CHECK_EQUAL(static_cast<unsigned char>(i), m.get_addr()[i]);
            if (static_cast<unsigned char>(i) != m.get_addr()[i])
                return;
        }
    }
}

#endif // TEST_FILE
