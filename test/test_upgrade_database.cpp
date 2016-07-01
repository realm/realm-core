#include "testsettings.hpp"
#ifdef TEST_GROUP

#include <stdint.h>
#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/types.h>
#endif

#include <realm.hpp>
#include <realm/util/to_string.hpp>
#include <realm/util/file.hpp>
#include <realm/commit_log.hpp>
#include <realm/version.hpp>
#include <realm/history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;

#define TEST_READ_UPGRADE_MODE 1 // set to 0 when using this in an older version of core to write new tests files

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
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_1.realm";

    // Test upgrading the database file format from version 2 to 3. When you open a version 2 file using SharedGroup
    // it gets converted automatically by Group::upgrade_file_format(). Files cannot be read or written (you cannot
    // even read using Get()) without upgrading the database first.

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);

#if 0 // Not possible to upgrade from Group (needs write access to file)
    // Automatic upgrade from Group
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        CHECK_OR_RETURN(File::copy(path, temp_copy));

        // Open copy. Group constructor will upgrade automatically if needed, also even though user requested ReadOnly. Todo,
        // discuss if this is OK.
        Group g(temp_copy, 0, Group::mode_ReadOnly);

        TableRef t = g.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            string str = util::to_string(i);
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
#endif

    // Prohibit automatic upgrade by SharedGroup
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        CHECK_OR_RETURN(File::copy(path, temp_copy));

        bool no_create = false;
        SharedGroup::DurabilityLevel durability = SharedGroup::durability_Full;
        const char* encryption_key = nullptr;
        bool allow_upgrade = false;

        CHECK_THROW_WHAT(
            SharedGroup(temp_copy, no_create, durability, encryption_key, allow_upgrade),
            FileFormatUpgradeRequired, "Database upgrade required but prohibited");
    }

    // Automatic upgrade from SharedGroup
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        CHECK_OR_RETURN(File::copy(path, temp_copy));

        SharedGroup sg(temp_copy);
        ReadTransaction rt(sg);
        ConstTableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            std::string str = util::to_string(i);
            StringData sd(str);
            size_t f = t->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }
    }

    // Now see if we can open the upgraded file and also commit to it
    {
        SharedGroup sg(temp_copy);
        WriteTransaction rt(sg);
        TableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            std::string str = util::to_string(i);
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
        CHECK_OR_RETURN(File::copy(path, temp_copy));

        SharedGroup sg(temp_copy);
        WriteTransaction rt(sg);
        TableRef t = rt.get_table("table");

        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));

        for (int i = 0; i < 1000; i++) {
            // These tests utilize the Integer and String index. That will crash if the database is still
            // in version 2 format, because the on-disk format of index has changed in version 3.
            std::string str = util::to_string(i);
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
            std::string str = util::to_string(i);
            StringData sd(str);
            size_t f = t2->find_first_string(0, sd);
            CHECK_EQUAL(f, i);

            f = t2->find_first_int(1, i);
            CHECK_EQUAL(f, i);
        }
    }

    // Automatic upgrade from SharedGroup with replication
    {
      CHECK_OR_RETURN(File::copy(path, temp_copy));

      std::unique_ptr<Replication> hist = make_client_history(temp_copy);
      SharedGroup sg(*hist);
      ReadTransaction rt(sg);
      ConstTableRef t = rt.get_table("table");

      CHECK(t->has_search_index(0));
      CHECK(t->has_search_index(1));

      for (int i = 0; i < 1000; i++) {
          // These tests utilize the Integer and String index. That will crash if the database is still
          // in version 2 format, because the on-disk format of index has changed in version 3.
          std::string str = util::to_string(i);
          StringData sd(str);
          size_t f = t->find_first_string(0, sd);
          CHECK_EQUAL(f, i);

          f = t->find_first_int(1, i);
          CHECK_EQUAL(f, i);
      }
    }

#else // test write mode
    char leafsize[20];
    sprintf(leafsize, "%d", REALM_MAX_BPNODE_SIZE);
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
#endif // TEST_READ_UPGRADE_MODE
}


// Same as above test, just with different string lengths to get better coverage of the different String array types
// that all have been modified by null support
TEST(Upgrade_Database_2_Backwards_Compatible)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    CHECK_OR_RETURN(File::copy(path, temp_copy));
    SharedGroup g(temp_copy, 0);

    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(5, sgf::get_file_format_version(g));

    // First table is non-indexed for all columns, second is indexed for all columns
    for (size_t tbl = 0; tbl < 2; tbl++) {
        ReadTransaction rt(g);

        ConstTableRef t = rt.get_table(tbl);

        size_t f;

        for (int i = 0; i < 9; i++) {
            f = t->find_first_string(0, "");
            CHECK_EQUAL(f, 0);
            f = t->where().equal(0, "").find();
            CHECK_EQUAL(f, 0);
            CHECK(t->get_string(0, 0) == "");

            f = t->where().equal(0, "").find();
            std::string s5ia(5, char(i + 'a'));
            f = t->find_first_string(1, s5ia);
            CHECK_EQUAL(f, i);
            f = t->where().equal(1, s5ia).find();
            CHECK_EQUAL(f, i);

            std::string s40ia(40, char(i + 'a'));
            f = t->find_first_string(2, s40ia);
            CHECK_EQUAL(f, i);
            f = t->where().equal(2, s40ia).find();
            CHECK_EQUAL(f, i);

            std::string s200ia(200, char(i + 'a'));
            f = t->find_first_string(3, s200ia);
            CHECK_EQUAL(f, i);
            f = t->where().equal(3, s200ia).find();
            CHECK_EQUAL(f, i);
        }

        f = t->find_first_string(4, "");
        CHECK_EQUAL(f, 0);

        f = t->where().equal(4, "").find();

        CHECK_EQUAL(f, 0);

        f = t->where().not_equal(4, "").find();

        CHECK(f != 0);
        CHECK(t->get_string(4, 0) == "");
        CHECK(!(t->get_string(4, 0) != ""));

        f = t->find_first_string(5, "");
        CHECK_EQUAL(f, 0);

        f = t->where().equal(5, "").find();

        CHECK_EQUAL(f, 0);

        f = t->where().not_equal(5, "").find();

        CHECK(f != 0);
        CHECK(t->get_string(5, 0) == "");
        CHECK(!(t->get_string(5, 0) != ""));

        f = t->find_first_string(6, "");
        CHECK_EQUAL(f, 0);

        f = t->where().equal(6, "").find();

        CHECK_EQUAL(f, 0);

        f = t->where().not_equal(6, "").find();

        CHECK(f != 0);
        CHECK(t->get_string(6, 0) == "");
        CHECK(!(t->get_string(6, 0) != ""));

    }
#else // test write mode
    File::try_remove(path);

    Group g;
    TableRef t[2];
    t[0] = g.add_table("table");
    t[1] = g.add_table("table_indexed");

    for (int tbl = 0; tbl < 2; tbl++) {
        t[tbl]->add_column(type_String, "empty");
        t[tbl]->add_column(type_String, "short");
        t[tbl]->add_column(type_String, "medium");
        t[tbl]->add_column(type_String, "long");

        t[tbl]->add_column(type_String, "short_empty_string");
        t[tbl]->add_column(type_String, "medium_empty_string");
        t[tbl]->add_column(type_String, "long_empty_string");

        for (size_t i = 0; i < 9; i++) {
            t[tbl]->add_empty_row();
            t[tbl]->set_string(0, i, std::string(""));
            t[tbl]->set_string(1, i, std::string(5, char(i + 'a')));
            t[tbl]->set_string(2, i, std::string(40, char(i + 'a')));
            t[tbl]->set_string(3, i, std::string(200, char(i + 'a')));
        }

        // Upgrade leaf to short, medium, long
        t[tbl]->set_string(4, 0, std::string(5, 'a'));
        t[tbl]->set_string(5, 0, std::string(40, 'a'));
        t[tbl]->set_string(6, 0, std::string(200, 'a'));
        // Set contents to empty string
        t[tbl]->set_string(4, 0, std::string(""));
        t[tbl]->set_string(5, 0, std::string(""));
        t[tbl]->set_string(6, 0, std::string(""));
    }

    t[1]->add_search_index(0);
    t[1]->add_search_index(1);
    t[1]->add_search_index(2);
    t[1]->add_search_index(3);
    t[1]->add_search_index(4);
    t[1]->add_search_index(5);
    t[1]->add_search_index(6);

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}



// Same as above test, but upgrading through WriteTransaction instead of ReadTransaction
TEST(Upgrade_Database_2_Backwards_Compatible_WriteTransaction)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified

    SHARED_GROUP_TEST_PATH(temp_copy);

    CHECK_OR_RETURN(File::copy(path, temp_copy));
    SharedGroup g(temp_copy, 0);

    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(5, sgf::get_file_format_version(g));

    // First table is non-indexed for all columns, second is indexed for all columns
    for (size_t tbl = 0; tbl < 2; tbl++) {
        WriteTransaction wt(g);
        TableRef t = wt.get_table(tbl);

        size_t f;

        for (int mode = 0; mode < 2; mode++) {
            if (mode == 1) {
                // Add search index (no-op for second table in this group because it already has indexes on all cols)
                t->add_search_index(0);
                t->add_search_index(1);
                t->add_search_index(2);
                t->add_search_index(3);
                t->add_search_index(4);
                t->add_search_index(5);
                t->add_search_index(6);
            }

            for (int i = 0; i < 9; i++) {
                f = t->find_first_string(0, "");
                CHECK_EQUAL(f, 0);
                f = (t->column<String>(0) == "").find();
                CHECK_EQUAL(f, 0);
                CHECK(t->get_string(0, 0) == "");

                std::string s5ia(5, char(i + 'a'));
                f = t->find_first_string(1, s5ia);
                CHECK_EQUAL(f, i);
                f = (t->column<String>(1) == s5ia).find();
                CHECK_EQUAL(f, i);

                std::string s40ia(40, char(i + 'a'));
                f = t->find_first_string(2, s40ia);
                CHECK_EQUAL(f, i);
                f = (t->column<String>(2) == s40ia).find();
                CHECK_EQUAL(f, i);

                std::string s200ia(200, char(i + 'a'));
                f = t->find_first_string(3, s200ia);
                CHECK_EQUAL(f, i);
                f = (t->column<String>(3) == s200ia).find();
                CHECK_EQUAL(f, i);
            }

            f = t->find_first_string(4, "");
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(4) == "").find();
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(4) != "").find();
            CHECK(f != 0);
            CHECK(t->get_string(4, 0) == "");
            CHECK(!(t->get_string(4, 0) != ""));

            f = t->find_first_string(5, "");
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(5) == "").find();
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(5) != "").find();
            CHECK(f != 0);
            CHECK(t->get_string(5, 0) == "");
            CHECK(!(t->get_string(5, 0) != ""));

            f = t->find_first_string(6, "");
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(6) == "").find();
            CHECK_EQUAL(f, 0);
            f = (t->column<String>(6) != "").find();
            CHECK(f != 0);
            CHECK(t->get_string(6, 0) == "");
            CHECK(!(t->get_string(6, 0) != ""));
        }
    }
#else // test write mode
    File::try_remove(path);

    Group g;
    TableRef t[2];
    t[0] = g.add_table("table");
    t[1] = g.add_table("table_indexed");

    for (int tbl = 0; tbl < 2; tbl++) {
        t[tbl]->add_column(type_String, "empty");
        t[tbl]->add_column(type_String, "short");
        t[tbl]->add_column(type_String, "medium");
        t[tbl]->add_column(type_String, "long");

        t[tbl]->add_column(type_String, "short_empty_string");
        t[tbl]->add_column(type_String, "medium_empty_string");
        t[tbl]->add_column(type_String, "long_empty_string");

        for (size_t i = 0; i < 9; i++) {
            t[tbl]->add_empty_row();
            t[tbl]->set_string(0, i, std::string(""));
            t[tbl]->set_string(1, i, std::string(5, char(i + 'a')));
            t[tbl]->set_string(2, i, std::string(40, char(i + 'a')));
            t[tbl]->set_string(3, i, std::string(200, char(i + 'a')));
        }

        // Upgrade leaf to short, medium, long
        t[tbl]->set_string(4, 0, std::string(5, 'a'));
        t[tbl]->set_string(5, 0, std::string(40, 'a'));
        t[tbl]->set_string(6, 0, std::string(200, 'a'));
        // Set contents to empty string
        t[tbl]->set_string(4, 0, std::string(""));
        t[tbl]->set_string(5, 0, std::string(""));
        t[tbl]->set_string(6, 0, std::string(""));
    }

    t[1]->add_search_index(0);
    t[1]->add_search_index(1);
    t[1]->add_search_index(2);
    t[1]->add_search_index(3);
    t[1]->add_search_index(4);
    t[1]->add_search_index(5);
    t[1]->add_search_index(6);

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}



// Test reading/writing of old version 2 BinaryColumn.
TEST(Upgrade_Database_Binary)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_3.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    size_t f;

    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    CHECK_OR_RETURN(File::copy(path, temp_copy));
    SharedGroup g(temp_copy, 0);

    WriteTransaction wt(g);
    TableRef t = wt.get_table(0);

    // small blob (< 64 bytes)
    f = t->find_first_binary(0, BinaryData("", 0));
    CHECK_EQUAL(f, 0);
    f = t->where().equal(0, BinaryData("", 0)).find();
    CHECK_EQUAL(f, 0);
    CHECK(t->get_binary(0, 0) == BinaryData("", 0));
    f = t->where().not_equal(0, BinaryData("", 0)).find();
    CHECK(f == 1);
    f = t->where().not_equal(0, BinaryData("foo")).find();
    CHECK(f == 0);

    // make small blob expand, to see if expansion works
    t->add_empty_row();
    t->set_binary(0, 2, BinaryData("1234567890123456789012345678901234567890123456789012345678901234567890"));

    // repeat all previous tests again on new big blob
    f = t->find_first_binary(0, BinaryData("", 0));
    CHECK_EQUAL(f, 0);
    f = t->where().equal(0, BinaryData("", 0)).find();
    CHECK_EQUAL(f, 0);
    CHECK(t->get_binary(0, 0) == BinaryData("", 0));
    f = t->where().not_equal(0, BinaryData("", 0)).find();
    CHECK(f == 1);
    f = t->where().not_equal(0, BinaryData("foo")).find();
    CHECK(f == 0);


    // long blobs
    t = wt.get_table(1);
    f = t->find_first_binary(0, BinaryData("", 0));
    CHECK_EQUAL(f, 0);
    f = t->where().equal(0, BinaryData("", 0)).find();
    CHECK_EQUAL(f, 0);
    CHECK(t->get_binary(0, 0) == BinaryData("", 0));
    f = t->where().not_equal(0, BinaryData("", 0)).find();
    CHECK(f == 1);
    f = t->where().not_equal(0, BinaryData("foo")).find();
    CHECK(f == 0);


#else // test write mode
    File::try_remove(path);

    Group g;
    TableRef t;

    // small blob size (< 64 bytes)
    t = g.add_table("short");
    t->add_column(type_Binary, "bin");
    t->add_empty_row(2);
    t->set_binary(0, 0, BinaryData("", 0)); // Empty string. Remember 0, else it will take up 1 byte!
    t->set_binary(0, 1, BinaryData("foo"));

    // long blocs
    t = g.add_table("long");
    t->add_column(type_Binary, "bin");
    t->add_empty_row(2);
    t->set_binary(0, 0, BinaryData("", 0)); // Empty string. Remember 0, else it will take up 1 byte!
    t->set_binary(0, 0, BinaryData("foo")); // Empty string. Remember 0, else it will take up 1 byte!
    t->set_binary(0, 1, BinaryData("1234567890123456789012345678901234567890123456789012345678901234567890"));

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}



// Test upgrading a database with single column containing strings with embedded NULs
TEST(Upgrade_Database_Strings_With_NUL)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_4.realm";

    // entries in this array must have length == index
    const char* const nul_strings[] = {
        "", // length == 0
        "\0",  // length == 1 etc.
        "\0\0",
        "\0\0\0",
        "\0\0\0\0",
    };
    constexpr size_t num_nul_strings = sizeof(nul_strings) / sizeof(nul_strings[0]);

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    CHECK_OR_RETURN(File::copy(path, temp_copy));
    SharedGroup g(temp_copy, 0);

    WriteTransaction wt(g);
    TableRef t = wt.get_table("table");
    size_t reserved_row_index = t->add_empty_row(); // reserved for "upgrading" entry

    // Check if the previously added strings are in the column, 3 times:
    // 0) as is (with ArrayString leafs)
    // 1) after upgrading to ArrayStringLong
    // 2) after upgrading to ArrayBigBlobs
    for (int test_num = 0; test_num < 3; ++test_num) {
        for (size_t j = 0; j < num_nul_strings; ++j) {
            size_t f = t->find_first_string(0, StringData(nul_strings[j], j));
            CHECK_EQUAL(f, j);
            f = t->where().equal(0, StringData(nul_strings[j], j)).find();
            CHECK_EQUAL(f, j);
            CHECK(t->get_string(0, j) == StringData(nul_strings[j], j));
        }

        t->add_search_index(0);

        size_t f = t->where().not_equal(0, StringData(nul_strings[0], 0)).find();
        CHECK(f == 1);
        f = t->where().not_equal(0, StringData(nul_strings[1], 1)).find();
        CHECK(f == 0);

        switch (test_num) {
            case 0:
                t->set_string(0, reserved_row_index, StringData("12345678901234567890")); // length == 20
            case 1:
                t->set_string(0, reserved_row_index, StringData("1234567890123456789012345678901234567890123456789012345678901234567890")); // length == 70
            default:
                break;
        }
    }

#else // test write mode
    File::try_remove(path);

    Group g;

    TableRef t = g.add_table("table");
    t->add_column(type_String, "strings_with_nul_bytes");
    t->add_empty_row(num_nul_strings);
    for (size_t i = 0; i < num_nul_strings; ++i) {
        t->set_string(0, i, StringData(nul_strings[i], i));
    }

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}

#if TEST_READ_UPGRADE_MODE
TEST(Upgrade_Database_2_3_Writes_New_File_Format)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_1.realm";
    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);
    CHECK_OR_RETURN(File::copy(path, temp_copy));
    SharedGroup sg1(temp_copy);
    SharedGroup sg2(temp_copy); // verify that the we can open another shared group, and it won't deadlock
    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(sgf::get_file_format_version(sg1), sgf::get_file_format_version(sg2));
}

TEST(Upgrade_Database_2_3_Writes_New_File_Format_new)
{
    // The method `inline void SharedGroup::upgrade_file_format()` will first have a fast non-threadsafe
    // test for seeing if the file needs to be upgraded. Then it will make a slower thread-safe check inside a
    // write transaction (transaction acts like a mutex). In debug mode, the `inline void SharedGroup::upgrade_file_format()`
    // method will sleep 0.2 second between the non-threadsafe and the threadsafe test, to ensure that two threads opening
    // the same database file will both think the database needs upgrade in the first check.

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_1.realm";
    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);
    CHECK_OR_RETURN(File::copy(path, temp_copy));

    util::Thread t[10];

    for (auto& tt : t) {
        tt.start([&]() {
            SharedGroup sg(temp_copy);
        });
    }

    for (auto& tt : t)
        tt.join();
}

#endif


TEST(Upgrade_InRealmHistory)
{
    // When requesting a in-Realm history, an upgrade to at least file format
    // version 4 is necessary.

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_path);

    {
        CHECK_OR_RETURN(File::copy(path, temp_path));
        std::unique_ptr<Replication> hist = make_in_realm_history(temp_path);
        SharedGroup sg(*hist);
        using sgf = _impl::SharedGroupFriend;
        CHECK_LESS_EQUAL(4, sgf::get_file_format_version(sg));
    }

    // Try again, but do it in two steps (2->3, 3->5).
    {
        File::remove(temp_path);
        CHECK_OR_RETURN(File::copy(path, temp_path));
        bool no_create = true;
        {
            SharedGroup sg(temp_path, no_create);
            using sgf = _impl::SharedGroupFriend;
            CHECK_EQUAL(5, sgf::get_file_format_version(sg));
        }
        {
            std::unique_ptr<Replication> hist = make_in_realm_history(temp_path);
            SharedGroup sg(*hist);
            using sgf = _impl::SharedGroupFriend;
            CHECK_LESS_EQUAL(4, sgf::get_file_format_version(sg));
        }
    }
}

TEST(Upgrade_DatabaseWithCallback)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
    util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the version 4 database so that we keep the original file intact and unmodified
    CHECK_OR_RETURN(File::copy(path, temp_copy));

    // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
    // in version 3
    bool no_create = false;
    SharedGroup::DurabilityLevel durability = SharedGroup::DurabilityLevel::durability_Full;
    const char* encryption_key = nullptr;
    bool allow_file_format_upgrade = true;
    std::function<void(int,int)> upgrade_callback;

    bool did_upgrade = false;
    int old_version, new_version;
    auto callback = [&](int from, int to)
    {
        did_upgrade = true;
        old_version = from;
        new_version = to;
    };

    upgrade_callback = callback;

    SharedGroup sg(temp_copy,
                   no_create,
                   durability,
                   encryption_key,
                   allow_file_format_upgrade,
                   upgrade_callback);

    CHECK(did_upgrade);
    CHECK_EQUAL(old_version, 3);
    CHECK(new_version >= 5);
}

TEST(Upgrade_DatabaseWithCallbackWithException)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
    util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the version 4 database so that we keep the original file intact and unmodified
    CHECK_OR_RETURN(File::copy(path, temp_copy));

    // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
    // in version 3
    bool no_create = false;
    SharedGroup::DurabilityLevel durability = SharedGroup::DurabilityLevel::durability_Full;
    const char* encryption_key = nullptr;
    bool allow_file_format_upgrade = true;
    std::function<void(int,int)> upgrade_callback;

    bool did_upgrade = false;
    int old_version, new_version;
    auto exception_callback = [&](int, int)
    {
        throw std::exception();
    };
    auto successful_callback = [&](int from, int to)
    {
        did_upgrade = true;
        old_version = from;
        new_version = to;
    };

    // Callback that throws should revert the upgrade
    upgrade_callback = exception_callback;
    bool exception_thrown = false;
    try {
        SharedGroup sg1(temp_copy,
                        no_create,
                        durability,
                        encryption_key,
                        allow_file_format_upgrade,
                        upgrade_callback);
    }
    catch(...) {
        exception_thrown = true;
    }
    CHECK(exception_thrown);
    CHECK(!did_upgrade);

    // Callback should be triggered here because the file still needs to be upgraded
    upgrade_callback = successful_callback;
    SharedGroup sg2(temp_copy,
                   no_create,
                   durability,
                   encryption_key,
                   allow_file_format_upgrade,
                   upgrade_callback);
    CHECK(did_upgrade);
    CHECK_EQUAL(old_version, 3);
    CHECK(new_version >= 5);

    // Callback should not be triggered here because the file is already upgraded
    did_upgrade = false;
    SharedGroup sg3(temp_copy,
                    no_create,
                    durability,
                    encryption_key,
                    allow_file_format_upgrade,
                    upgrade_callback);
    CHECK(!did_upgrade);
}

// Open an existing database-file-format-version 4 file and check that it automatically upgrades to version 5.
// The upgrade will change all OldDateTime columns into TimeStamp columns.
TEST(Upgrade_Database_4_5_DateTime1)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
        util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the version 4 database so that we keep the original file intact and unmodified
        CHECK_OR_RETURN(File::copy(path, temp_copy));

        // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
        // in version 4
        SharedGroup sg(temp_copy);

        WriteTransaction rt(sg);
        TableRef t = rt.get_table("table");
        
        CHECK(t->has_search_index(0));
        CHECK(t->has_search_index(1));
        CHECK(!t->has_search_index(2));
        CHECK(!t->has_search_index(3));
        
        CHECK(!t->is_null(0, 0));
        CHECK(!t->is_null(1, 0));
        CHECK(!t->is_null(2, 0));
        CHECK(!t->is_null(3, 0));

        CHECK(!t->is_null(0, 1));
        CHECK(!t->is_null(1, 1));
        CHECK(!t->is_null(2, 1));
        CHECK(!t->is_null(3, 1));

        CHECK(t->is_null(0, 2));
        CHECK(!t->is_null(1, 2));
        CHECK(t->is_null(2, 2));
        CHECK(!t->is_null(3, 2));

        CHECK_EQUAL(t->get_timestamp(0, 0), Timestamp(1234, 0));
        CHECK_EQUAL(t->get_timestamp(1, 0), Timestamp(1234, 0));
        CHECK_EQUAL(t->get_timestamp(2, 0), Timestamp(1234, 0));
        CHECK_EQUAL(t->get_timestamp(3, 0), Timestamp(1234, 0));

        CHECK_EQUAL(t->get_timestamp(0, 1), Timestamp(0, 0));
        CHECK_EQUAL(t->get_timestamp(1, 1), Timestamp(0, 0));
        CHECK_EQUAL(t->get_timestamp(2, 1), Timestamp(0, 0));
        CHECK_EQUAL(t->get_timestamp(3, 1), Timestamp(0, 0));

        CHECK_EQUAL(t->get_timestamp(1, 2), Timestamp(0, 0));
        CHECK_EQUAL(t->get_timestamp(3, 2), Timestamp(0, 0));

        CHECK_EQUAL(t->size(), 3);
    }

#else // test write mode
    // NOTE: This code must be executed from an old file-format-version 4 core in order to create
    // a file-format-version 4 test file!
    char leafsize[20];
    sprintf(leafsize, "%d", REALM_MAX_BPNODE_SIZE);
    File::try_remove(path);

    Group g;
    TableRef t = g.add_table("table");

    // No index
    t->add_column(type_OldDateTime, "dt1", true);  // nullable
    t->add_column(type_OldDateTime, "dt2", false); // nonnullable
                                                // No index
    t->add_column(type_OldDateTime, "dt3", true);  // nullable
    t->add_column(type_OldDateTime, "dt4", false); // nonnullable

    t->add_search_index(0);
    t->add_search_index(1);

    t->add_empty_row();
    t->set_olddatetime(0, 0, OldDateTime(1234));
    t->set_olddatetime(1, 0, OldDateTime(1234));
    t->set_olddatetime(2, 0, OldDateTime(1234));
    t->set_olddatetime(3, 0, OldDateTime(1234));

    t->add_empty_row();
    t->set_olddatetime(0, 1, OldDateTime(0));
    t->set_olddatetime(1, 1, OldDateTime(0));
    t->set_olddatetime(2, 1, OldDateTime(0));
    t->set_olddatetime(3, 1, OldDateTime(0));

    t->add_empty_row();
    t->set_null(0, 2);
    t->set_olddatetime(1, 2, OldDateTime(0));
    t->set_null(2, 2);
    t->set_olddatetime(3, 2, OldDateTime(0));

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE

}

#endif // TEST_GROUP
