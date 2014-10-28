#include "testsettings.hpp"
#ifdef TEST_FILE

#include <sstream>
#include <ostream>

#include <tightdb/util/file.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb::util;


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
        ostream out(&b);
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
    }
    {
        File f(path, File::mode_Read);
        char buffer[256];
        size_t n = f.read(buffer);
        string s_1(buffer, buffer+n);
        ostringstream out;
        out << "Line " << 1 << endl;
        out << "Line " << 2 << endl;
        string s_2 = out.str();
        CHECK(s_1 == s_2);
    }
}

TEST(File_Map)
{
    TEST_PATH(path);
    const char data[] = "hello";
    {
        File f(path, File::mode_Write);
        f.set_encryption_key(tightdb::test_util::key);
        f.resize(sizeof(data));

        File::Map<char> map(f, File::access_ReadWrite, 1);
        *map.get_addr() = data[0];

        map.remap(f, File::access_ReadWrite, sizeof(data));
        memcpy(map.get_addr() + 1, data + 1, map.get_size() - 1);
    }
    {
        File f(path, File::mode_Read);
        f.set_encryption_key(tightdb::test_util::key);
        File::Map<char> map(f, File::access_ReadOnly, sizeof(data));
        CHECK(memcmp(map.get_addr(), data, map.get_size()) == 0);
    }
}

#endif // TEST_FILE
