#include "testsettings.hpp"
#ifdef TEST_UTIL_FILE

#include <realm/util/file.hpp>

#include "test.hpp"
#include <cstdio>

using namespace realm;
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

TEST(Utils_File_dir)
{
    std::string dir_name = File::resolve("tempdir", test_util::get_test_path_prefix());

    // Create directory
    bool dir_exists = File::is_dir(dir_name);
    CHECK_EQUAL(dir_exists, false);

    make_dir(dir_name);
    try {
        make_dir(dir_name);
    }
    catch (const File::Exists& e) {
        CHECK_EQUAL(e.get_path(), dir_name);
        dir_exists = File::is_dir(dir_name);
    }
    CHECK_EQUAL(dir_exists, true);

    // Remove directory
    remove_dir(dir_name);
    try {
        remove_dir(dir_name);
    }
    catch (const File::NotFound& e) {
        CHECK_EQUAL(e.get_path(), dir_name);
        dir_exists = false;
    }
    CHECK_EQUAL(dir_exists, false);

}

TEST(Utils_File_resolve)
{
    std::string res;
    res = File::resolve("", "");
    CHECK_EQUAL(res, ".");

    res = File::resolve("/foo/bar", "dir");
    CHECK_EQUAL(res, "/foo/bar");

    res = File::resolve("foo/bar", "");
    CHECK_EQUAL(res, "foo/bar");

    res = File::resolve("file", "dir");
    CHECK_EQUAL(res, "dir/file");

    res = File::resolve("file/", "dir");
    CHECK_EQUAL(res, "dir/file/");

    /* Function does not work as specified - but not used
    res = File::resolve("../baz", "/foo/bar");
    CHECK_EQUAL(res, "/foo/baz");
    */
}

TEST(Utils_File_remove_open)
{
    std::string file_name = File::resolve("FooBar", test_util::get_test_path_prefix());
    File f(file_name, File::mode_Write);

    CHECK_EQUAL(f.is_removed(), false);
    std::remove(file_name.c_str());
    CHECK_EQUAL(f.is_removed(), true);
}

#endif
