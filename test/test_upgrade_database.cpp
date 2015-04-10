#include "testsettings.hpp"
#ifdef TEST_GROUP

#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/types.h>
#endif

#include <realm.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"

using namespace std;
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


TEST(Upgrade_Database_2_3)
{
    // Test upgrading the database file format from version 2 to 3. When opening a version 2 file, you must, as the 
    // very first action, call Group::upgrade_file_format() on it. You must not call any reading or modifying 
    // method on it prior to that, because core is not backwards compatible with the version 2 file format!

    // Copy/paste the bottommost commented-away unit test into test_group.cpp of Realm Core 0.84 or older to create a
    // version 2 database file. Then copy it into the /test directory of this current Realm core.

    // If REALM_NULL_STRINGS is NOT defined, then this Realm core still operates in format 2 (null not supported) and
    // this unit test will not upgrade the file. The REALM_NULL_STRINGS flag was introduced to be able to merge
    // null branch into master but without activating version 3 yet.
#if 1
    // Automatic upgrade from Group
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        string path = test_util::get_test_path_prefix() + "version_2_database_" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";

        File::copy(path, path + ".tmp");

        // Open copy. Group constructor will upgrade automatically if needed, also even though user requested ReadOnly. Todo,
        // discuss if this is OK.
        Group g(path + ".tmp", 0, Group::mode_ReadOnly);
        
        TableRef t = g.get_table("table");

#ifdef REALM_NULL_STRINGS
        CHECK_EQUAL(g.get_file_format(), 3);
#else
        CHECK_EQUAL(g.get_file_format(), 2);
#endif

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = std::to_string(i);
            StringData sd(str);
            size_t f = t->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }

        g.commit();

        // Test an assert that guards against writing version 2 file to disk
        File::try_remove(path + ".tmp2");
        g.write(path + ".tmp2");

    }

    // Automatic upgrade from SharedGroup
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        string path = test_util::get_test_path_prefix() + "version_2_database_" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";

        File::copy(path, path + ".tmp");

        SharedGroup sg(path + ".tmp");
        ReadTransaction rt(sg);
        ConstTableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = std::to_string(i);
            StringData sd(str);
            size_t f = t->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }
    }


    // Now see if we can open the upgraded file and also commit to it
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        string path = test_util::get_test_path_prefix() + "version_2_database_" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";

        SharedGroup sg(path + ".tmp");
        WriteTransaction rt(sg);
        TableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = std::to_string(i);
            StringData sd(str);
            size_t f = t->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }

        sg.commit();
    }
    
    
    // Begin from scratch; see if we can upgrade file and then use a write transaction
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        string path = test_util::get_test_path_prefix() + "version_2_database_" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";

        File::copy(path, path + ".tmp");

        SharedGroup sg(path + ".tmp");
        WriteTransaction rt(sg);
        TableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = std::to_string(i);
            StringData sd(str);
            size_t f = t->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }

        sg.commit();

        WriteTransaction rt2(sg);
        TableRef t2 = rt.get_table("table");

        CHECK(t2->has_search_index(0));
        CHECK(t2->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = std::to_string(i);
            StringData sd(str);
            size_t f = t2->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t2->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }
    }



#else   
    // For creating a version 2 database; use in OLD (0.84) core
    char leafsize[20];
    sprintf(leafsize, "%d", REALM_MAX_BPNODE_SIZE);
    string path = test_util::get_test_path_prefix() + "version_2_database_" + leafsize + ".realm";
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
#endif    
}


// Same as above test, just with different string lengths to get better coverage of the different String array types
// that all have been modified by null support
TEST(Upgrade_Database_2_Backwards_Compatible)
{
    // Copy/paste the bottommost commented-away unit test into test_group.cpp of Realm Core 0.84 or older to create a
    // version 2 database file. Then copy it into the /test directory of this current Realm core.

#if 1
    // Make a copy of the database so that we keep the original file intact and unmodified
    string path = test_util::get_test_path_prefix() + "version_2_database_backwards_compatible_" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";
    File::copy(path, path + ".tmp");
    Group g(path + ".tmp", 0, Group::mode_ReadOnly);

    TableRef t = g.get_table("table");

#ifdef REALM_NULL_STRINGS
    CHECK_EQUAL(g.get_file_format(), 3);
#else
    CHECK_EQUAL(g.get_file_format(), 2);
#endif

    size_t f;

    for (int i = 0; i < 9; i++) {
        f = t->find_first_string(0, std::string(""));
        CHECK_EQUAL(f, 0);
        f = (t->column<String>(0) == "").find();
        CHECK_EQUAL(f, 0);

        f = t->find_first_string(1, std::string(5, char(i + 'a')));
        CHECK_EQUAL(f, i);
        f = (t->column<String>(1) == std::string(5, char(i + 'a'))).find();
        CHECK_EQUAL(f, i);

        f = t->find_first_string(2, std::string(40, char(i + 'a')));
        CHECK_EQUAL(f, i);
        f = (t->column<String>(2) == std::string(40, char(i + 'a'))).find();
        CHECK_EQUAL(f, i);

        f = t->find_first_string(3, std::string(200, char(i + 'a')));
        CHECK_EQUAL(f, i);
        f = (t->column<String>(3) == std::string(200, char(i + 'a'))).find();
        CHECK_EQUAL(f, i);
    }

    f = t->find_first_string(4, "");
    CHECK_EQUAL(f, 0);
    f = (t->column<String>(4) == "").find();
    CHECK_EQUAL(f, 0);

    f = t->find_first_string(5, "");
    CHECK_EQUAL(f, 0);
    f = (t->column<String>(5) == "").find();
    CHECK_EQUAL(f, 0);

    f = t->find_first_string(6, "");
    CHECK_EQUAL(f, 0);
    f = (t->column<String>(6) == "").find();
    CHECK_EQUAL(f, 0);

#else
    // Create database file
    string path = test_util::get_test_path_prefix() + "version_2_database_backwards_compatible" + std::to_string(REALM_MAX_BPNODE_SIZE) + ".realm";
    File::try_remove(path);

    Group g;
    TableRef t = g.add_table("table");
    t->add_column(type_String, "empty");
    t->add_column(type_String, "short");
    t->add_column(type_String, "medium");
    t->add_column(type_String, "long");

    for (size_t i = 0; i < 1000; i++) {
        t->add_empty_row();
        t->set_string(0, i, std::string(""));
        t->set_string(1, i, std::string(5, char(i + 'a')));
        t->set_string(2, i, std::string(40, char(i + 'a')));
        t->set_string(0, i, std::string(200, char(i + 'a')));
    }
    g.write(path);
#endif
}

#endif 
