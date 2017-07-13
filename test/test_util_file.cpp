#include "testsettings.hpp"
#ifdef TEST_UTIL_FILE

#include <realm/util/file.hpp>

#include "test.hpp"
#include <cstdio>

#ifndef _WIN32
#include <unistd.h>
#endif

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
#ifndef _WIN32
    if (getuid() == 0) {
        std::cout << "Utils_File_dir test skipped because you are running it as root\n\n";
        return;
    }
#endif

    std::string dir_name = File::resolve("tempdir", test_util::get_test_path_prefix());

    // Create directory
    bool dir_exists = File::is_dir(dir_name);
    CHECK_NOT(dir_exists);

    make_dir(dir_name);
    try {
        make_dir(dir_name);
    }
    catch (const File::Exists& e) {
        CHECK_EQUAL(e.get_path(), dir_name);
        dir_exists = File::is_dir(dir_name);
    }
    CHECK(dir_exists);

    bool perm_denied = false;
    try {
        make_dir("/foobar");
    }
    catch (const File::AccessError& e) {
        CHECK_EQUAL(e.get_path(), "/foobar");
        perm_denied = true;
    }
    CHECK(perm_denied);

    perm_denied = false;
    try {
        remove_dir("/usr");
    }
    catch (const File::AccessError& e) {
        CHECK_EQUAL(e.get_path(), "/usr");
        perm_denied = true;
    }
    CHECK(perm_denied);

    // Remove directory
    remove_dir(dir_name);
    try {
        remove_dir(dir_name);
    }
    catch (const File::NotFound& e) {
        CHECK_EQUAL(e.get_path(), dir_name);
        dir_exists = false;
    }
    CHECK_NOT(dir_exists);
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

TEST(Utils_File_RemoveDirRecursive)
{
    TEST_DIR(dir_0);
    auto touch = [](const std::string& path) {
        File(path, File::mode_Write);
    };
    std::string dir_1  = File::resolve("dir_1",  dir_0);
    make_dir(dir_1);
    std::string dir_2  = File::resolve("dir_2",  dir_1);
    make_dir(dir_2);
    std::string dir_3  = File::resolve("dir_3",  dir_2);
    make_dir(dir_3);
    std::string file_1 = File::resolve("file_1", dir_2);
    touch(file_1);
    std::string dir_4  = File::resolve("dir_4",  dir_2);
    make_dir(dir_4);
    std::string file_2 = File::resolve("file_2", dir_2);
    touch(file_2);
    std::string file_3 = File::resolve("file_3", dir_3);
    touch(file_3);
    std::string file_4 = File::resolve("file_4", dir_4);
    touch(file_4);
    remove_dir_recursive(dir_1);
    remove_dir(dir_0);
}

TEST(Utils_File_ForEach)
{
    TEST_DIR(dir_0);
    auto touch = [](const std::string& path) {
        File(path, File::mode_Write);
    };
    std::string dir_1  = File::resolve("dir_1",  dir_0);
    make_dir(dir_1);
    std::string file_1 = File::resolve("file_1", dir_0);
    touch(file_1);
    std::string dir_2  = File::resolve("dir_2",  dir_0);
    make_dir(dir_2);
    std::string file_2 = File::resolve("file_2", dir_0);
    touch(file_2);
    std::string dir_3  = File::resolve("dir_3",  dir_1);
    make_dir(dir_3);
    std::string file_3 = File::resolve("file_3", dir_1);
    touch(file_3);
    std::string dir_4  = File::resolve("dir_4",  dir_2);
    make_dir(dir_4);
    std::string file_4 = File::resolve("file_4", dir_2);
    touch(file_4);
    std::string file_5 = File::resolve("file_5", dir_3);
    touch(file_5);
    std::string file_6 = File::resolve("file_6", dir_4);
    touch(file_6);
    std::vector<std::pair<std::string, std::string>> files;
    auto handler = [&](const std::string& file, const std::string& dir) {
        files.emplace_back(dir, file);
        return true;
    };
    File::for_each(dir_0, handler);
    std::sort(files.begin(), files.end());
    std::string dir_1_3 = File::resolve("dir_3", "dir_1");
    std::string dir_2_4 = File::resolve("dir_4", "dir_2");
    if (CHECK_EQUAL(6, files.size())) {
        CHECK_EQUAL("",       files[0].first);
        CHECK_EQUAL("file_1", files[0].second);
        CHECK_EQUAL("",       files[1].first);
        CHECK_EQUAL("file_2", files[1].second);
        CHECK_EQUAL("dir_1",  files[2].first);
        CHECK_EQUAL("file_3", files[2].second);
        CHECK_EQUAL(dir_1_3,  files[3].first);
        CHECK_EQUAL("file_5", files[3].second);
        CHECK_EQUAL("dir_2",  files[4].first);
        CHECK_EQUAL("file_4", files[4].second);
        CHECK_EQUAL(dir_2_4,  files[5].first);
        CHECK_EQUAL("file_6", files[5].second);
    }
}

#endif
