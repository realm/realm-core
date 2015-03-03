#include "testsettings.hpp"
#ifdef TEST_GROUP

#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/types.h>
#endif

#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

#include "test.hpp"
#include "crypt_key.hpp"

using namespace std;
using namespace tightdb;
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


ONLY(Upgrade_Database_2_3)
{
    // Test upgrading the database file format from version 2 to 3. When opening a version 2 file, you must, as the 
    // very first action, call Group::upgrade_file_format() on it. You must not call any reading or modifying 
    // method on it prior to that, because core is not backwards compatible with the version 2 file format!

    // Copy/paste the below commented-away unit test into test_group.cpp of Realm Core 0.84 or older to create a
    // version 2 database file. Then copy it into the /test directory.

    /*
    ONLY(CreateDatabase)
    {
        string path = test_util::get_test_path_prefix() + "version_2_database.tightdb";
        File::try_remove(path);

        Group g;
        TableRef t = g.add_table("table");
        t->add_column(type_String, "string");
        t->add_column(type_Int, "integer");

        t->add_search_index(0);
        t->add_search_index(1);

        for (size_t i = 0; i < 1000; i++) {
            t->add_empty_row();
            char tmp[20];
            sprintf(tmp, "%d", i);
            t->set_string(0, i, tmp);
            t->set_int(1, i, i);
        }
        g.write(path);
    }
    */

    
    // Make a copy of the version 2 database so that we keep the original file intact and unmodified
    string path = test_util::get_test_path_prefix() + "version_2_database.tightdb";
    File::copy(path, path + ".tmp");

    // Open copy
    Group g(path + ".tmp");
    TableRef t = g.get_table("table");

    CHECK_EQUAL(g.get_file_format(), 2);
    // Upgrade from version 2 to version 3
    g.upgrade_file_format();
    CHECK_EQUAL(g.get_file_format(), 3);

    CHECK(t->has_search_index(0));
    CHECK(t->has_search_index(1));

    for (size_t i = 0; i < 1000; i++) {
        t->add_empty_row();
        char tmp[20];
        sprintf(tmp, "%d", i);

        // These tests utilize the Integer and String index. That will crash if the database is still
        // in version 2 format, because the on-disk format of index has changed in version 3.
        StringData sd(tmp);
        size_t f = t->find_first_string(0, sd);
        CHECK_EQUAL(f, i);

        f = t->find_first_int(1, i);
        CHECK_EQUAL(f, i);
    }
}


#endif // TEST_GROUP
