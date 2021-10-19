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
#ifdef TEST_UPGRADE

#include <cstdint>
#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/types.h>
#endif

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/query_expression.hpp>
#include <realm/util/file.hpp>
#include <realm/util/to_string.hpp>
#include <realm/version.hpp>
#include "test.hpp"
#include "test_table_helper.hpp"

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

#ifdef LEGACY_TESTS

TEST_IF(Upgrade_Database_2_3, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
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
        File::copy(path, temp_copy);

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
        File::copy(path, temp_copy);

        bool no_create = false;
        SharedGroupOptions::Durability durability = SharedGroupOptions::Durability::Full;
        const char* encryption_key = nullptr;
        bool allow_upgrade = false;

        CHECK_THROW(DB(temp_copy, no_create, SharedGroupOptions(durability, encryption_key, allow_upgrade)),
                    FileFormatUpgradeRequired);
    }

    // Automatic upgrade from SharedGroup
    {
        // Make a copy of the version 2 database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        DB sg(temp_copy);
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
        DB sg(temp_copy);
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
        File::copy(path, temp_copy);

        DB sg(temp_copy);
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
        File::copy(path, temp_copy);

        std::unique_ptr<Replication> hist = make_in_realm_history();
        DB sg(*hist, temp_copy);
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

#else  // test write mode
    char leafsize[20];
    sprintf(leafsize, "%d", REALM_MAX_BPNODE_SIZE);
    File::try_remove(path);

    Group g;
    TableRef t = g.add_table("table");
    t->add_column(type_String, "string");
    t->add_column(type_Int, "integer");

    t->add_search_index(0);
    t->add_search_index(1);

    for (int i = 0; i < 1000; i++) {
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
TEST_IF(Upgrade_Database_2_Backwards_Compatible, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    File::copy(path, temp_copy);
    DB g(temp_copy, 0);

    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(9, sgf::get_file_format_version(g));

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
#else  // test write mode
#endif // TEST_READ_UPGRADE_MODE
}


// Same as above test, but upgrading through WriteTransaction instead of ReadTransaction
TEST_IF(Upgrade_Database_2_Backwards_Compatible_WriteTransaction,
        REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified

    SHARED_GROUP_TEST_PATH(temp_copy);

    File::copy(path, temp_copy);
    DB g(temp_copy, 0);

    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(9, sgf::get_file_format_version(g));

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
#else  // test write mode
#endif // TEST_READ_UPGRADE_MODE
}


// Test reading/writing of old version 2 BinaryColumn.
TEST_IF(Upgrade_Database_Binary, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_3.realm";

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));
    size_t f;

    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    File::copy(path, temp_copy);
    DB g(temp_copy, 0);

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


#else  // test write mode
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
TEST_IF(Upgrade_Database_Strings_With_NUL, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_4.realm";

    // entries in this array must have length == index
    const char* const nul_strings[] = {
        "",   // length == 0
        "\0", // length == 1 etc.
        "\0\0", "\0\0\0", "\0\0\0\0",
    };
    constexpr size_t num_nul_strings = sizeof(nul_strings) / sizeof(nul_strings[0]);

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_copy);

    File::copy(path, temp_copy);
    DB g(temp_copy, 0);

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
                break;
            case 1:
                t->set_string(
                    0, reserved_row_index,
                    StringData(
                        "1234567890123456789012345678901234567890123456789012345678901234567890")); // length == 70
                break;
            default:
                break;
        }
    }

#else  // test write mode
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
TEST_IF(Upgrade_Database_2_3_Writes_New_File_Format, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_1.realm";
    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);
    DB sg1(temp_copy);
    DB sg2(temp_copy); // verify that the we can open another shared group, and it won't deadlock
    using sgf = _impl::SharedGroupFriend;
    CHECK_EQUAL(sgf::get_file_format_version(sg1), sgf::get_file_format_version(sg2));
}

TEST_IF(Upgrade_Database_2_3_Writes_New_File_Format_new, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    // The method `inline void SharedGroup::upgrade_file_format()` will first have a fast non-threadsafe
    // test for seeing if the file needs to be upgraded. Then it will make a slower thread-safe check inside a
    // write transaction (transaction acts like a mutex). In debug mode, the `inline void
    // SharedGroup::upgrade_file_format()` method will sleep 0.2 second between the non-threadsafe and the threadsafe
    // test, to ensure that two threads opening the same database file will both think the database needs upgrade in
    // the first check.

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_1.realm";
    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);

    util::Thread t[10];

    for (auto& tt : t) {
        tt.start([&]() {
            DB sg(temp_copy);
        });
    }

    for (auto& tt : t)
        tt.join();
}

#endif


TEST_IF(Upgrade_InRealmHistory, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    // When requesting a in-Realm history, an upgrade to at least file format
    // version 4 is necessary.

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_2.realm";

    CHECK_OR_RETURN(File::exists(path));
    // Make a copy of the database so that we keep the original file intact and unmodified
    SHARED_GROUP_TEST_PATH(temp_path);

    {
        File::copy(path, temp_path);
        std::unique_ptr<Replication> hist = make_in_realm_history();
        DB sg(*hist, temp_path);
        using sgf = _impl::SharedGroupFriend;
        CHECK_LESS_EQUAL(4, sgf::get_file_format_version(sg));
    }

    // Try again, but do it in two steps (2->3, 3->6).
    {
        File::remove(temp_path);
        File::copy(path, temp_path);
        bool no_create = true;
        {
            DB sg(temp_path, no_create);
            using sgf = _impl::SharedGroupFriend;
            CHECK_EQUAL(9, sgf::get_file_format_version(sg));
        }
        {
            std::unique_ptr<Replication> hist = make_in_realm_history();
            DB sg(*hist, temp_path);
            using sgf = _impl::SharedGroupFriend;
            CHECK_LESS_EQUAL(4, sgf::get_file_format_version(sg));
        }
    }
}

TEST_IF(Upgrade_DatabaseWithCallback, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the version 4 database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);

    // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
    // in version 3
    bool no_create = false;
    SharedGroupOptions::Durability durability = SharedGroupOptions::Durability::Full;
    const char* encryption_key = nullptr;
    bool allow_file_format_upgrade = true;
    std::function<void(int, int)> upgrade_callback;

    bool did_upgrade = false;
    int old_version, new_version;
    auto callback = [&](int from, int to) {
        did_upgrade = true;
        old_version = from;
        new_version = to;
    };

    upgrade_callback = callback;

    DB sg(temp_copy, no_create,
          SharedGroupOptions(durability, encryption_key, allow_file_format_upgrade, upgrade_callback));

    CHECK(did_upgrade);
    CHECK_EQUAL(old_version, 3);
    CHECK(new_version >= 5);
}

TEST_IF(Upgrade_DatabaseWithCallbackWithException, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

    CHECK_OR_RETURN(File::exists(path));
    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the version 4 database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);

    // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
    // in version 3
    bool no_create = false;
    SharedGroupOptions::Durability durability = SharedGroupOptions::Durability::Full;
    const char* encryption_key = nullptr;
    bool allow_file_format_upgrade = true;
    std::function<void(int, int)> upgrade_callback;

    bool did_upgrade = false;
    int old_version, new_version;
    auto exception_callback = [&](int, int) {
        throw std::exception();
    };
    auto successful_callback = [&](int from, int to) {
        did_upgrade = true;
        old_version = from;
        new_version = to;
    };

    // Callback that throws should revert the upgrade
    upgrade_callback = exception_callback;
    bool exception_thrown = false;
    try {
        DB sg1(temp_copy, no_create,
               SharedGroupOptions(durability, encryption_key, allow_file_format_upgrade, upgrade_callback));
    }
    catch (...) {
        exception_thrown = true;
    }
    CHECK(exception_thrown);
    CHECK(!did_upgrade);

    // Callback should be triggered here because the file still needs to be upgraded
    upgrade_callback = successful_callback;
    DB sg2(temp_copy, no_create,
           SharedGroupOptions(durability, encryption_key, allow_file_format_upgrade, upgrade_callback));
    CHECK(did_upgrade);
    CHECK_EQUAL(old_version, 3);
    CHECK(new_version >= 5);

    // Callback should not be triggered here because the file is already upgraded
    did_upgrade = false;
    DB sg3(temp_copy, no_create,
           SharedGroupOptions(durability, encryption_key, allow_file_format_upgrade, upgrade_callback));
    CHECK(!did_upgrade);
}

// Open an existing database-file-format-version 4 file and check that it automatically upgrades to version 5.
// The upgrade will change all OldDateTime columns into TimeStamp columns.
TEST_IF(Upgrade_Database_4_5_DateTime1, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_4_to_5_datetime1.realm";

#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the version 4 database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        // Constructing this SharedGroup will trigger Table::upgrade_olddatetime() for all tables because the file is
        // in version 4
        DB sg(temp_copy);

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

#else  // test write mode
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
#endif


// Open an existing database-file-format-version 5 file and
// check that it automatically upgrades to version 6.
TEST_IF(Upgrade_Database_5_6_StringIndex, REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_5_to_6_stringindex.realm";

    // use a common prefix which will not cause a stack overflow but is larger
    // than StringIndex::s_max_offset
    const int common_prefix_length = 500;
    std::string std_base2(common_prefix_length, 'a');
    std::string std_base2_b = std_base2 + "b";
    std::string std_base2_c = std_base2 + "c";
    StringData base2(std_base2);
    StringData base2_b(std_base2_b);
    StringData base2_c(std_base2_c);


#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        // Constructing this SharedGroup will trigger an upgrade
        // for all tables because the file is in version 4
        auto sg = DB::create(temp_copy);

        WriteTransaction wt(sg);
        TableRef t = wt.get_table("t1");
        TableRef t2 = wt.get_table("t2");

        auto int_ndx = t->get_column_key("int");
        auto bool_ndx = t->get_column_key("bool");
        auto str_ndx = t->get_column_key("string");
        auto ts_ndx = t->get_column_key("timestamp");

        auto null_int_ndx = t->get_column_key("nullable int");
        auto null_bool_ndx = t->get_column_key("nullable bool");
        auto null_str_ndx = t->get_column_key("nullable string");
        auto null_ts_ndx = t->get_column_key("nullable timestamp");

        size_t num_rows = 6;

        CHECK_EQUAL(t->size(), num_rows);
        CHECK(t->has_search_index(int_ndx));
        CHECK(t->has_search_index(bool_ndx));
        CHECK(t->has_search_index(str_ndx));
        CHECK(t->has_search_index(ts_ndx));

        CHECK(t->has_search_index(null_int_ndx));
        CHECK(t->has_search_index(null_bool_ndx));
        CHECK(t->has_search_index(null_str_ndx));
        CHECK(t->has_search_index(null_ts_ndx));

        CHECK_EQUAL(t->find_first_string(str_ndx, base2_b), ObjKey(0));
        CHECK_EQUAL(t->find_first_string(str_ndx, base2_c), ObjKey(1));
        CHECK_EQUAL(t->find_first_string(str_ndx, base2), ObjKey(4));
        // CHECK_EQUAL(t->get_distinct_view(str_ndx).size(), 4);
        CHECK_EQUAL(t->size(), 6);

        CHECK_EQUAL(t->find_first_string(null_str_ndx, base2_b), ObjKey(0));
        CHECK_EQUAL(t->find_first_string(null_str_ndx, base2_c), ObjKey(1));
        CHECK_EQUAL(t->find_first_string(null_str_ndx, base2), ObjKey(4));
        // CHECK_EQUAL(t->get_distinct_view(null_str_ndx).size(), 4);

        // If the StringIndexes were not updated we couldn't do this
        // on a format 5 file and find it again.
        std::string std_base2_d = std_base2 + "d";
        StringData base2_d(std_base2_d);
        auto obj = t->create_object().set(str_ndx, base2_d);
        CHECK_EQUAL(t->find_first_string(str_ndx, base2_d), ObjKey(6));
        obj.set(null_str_ndx, base2_d);
        CHECK_EQUAL(t->find_first_string(null_str_ndx, base2_d), ObjKey(6));

        // And if the indexes were using the old format, adding a long
        // prefix string would cause a stack overflow.
        std::string big_base(90000, 'a');
        std::string big_base_b = big_base + "b";
        std::string big_base_c = big_base + "c";
        StringData b(big_base_b);
        StringData c(big_base_c);
        t->create_object().set(str_ndx, b).set(null_str_ndx, b);
        t->create_object().set(str_ndx, c).set(null_str_ndx, c);

        t->verify();
        t2->verify();
    }

#else  // test write mode
#ifndef REALM_CLUSTER_IF
    // NOTE: This code must be executed from an old file-format-version 5
    // core in order to create a file-format-version 5 test file!
    char leafsize[20];
    sprintf(leafsize, "%d", REALM_MAX_BPNODE_SIZE);
    File::try_remove(path);

    Group g;
    TableRef t = g.add_table("t1");
    TableRef t2 = g.add_table("t2");

    size_t int_ndx = t->add_column(type_Int, "int");
    size_t bool_ndx = t->add_column(type_Bool, "bool");
    t->add_column(type_Float, "float");
    t->add_column(type_Double, "double");
    size_t str_ndx = t->add_column(type_String, "string");
    t->add_column(type_Binary, "binary");
    size_t ts_ndx = t->add_column(type_Timestamp, "timestamp");
    t->add_column(type_Table, "table");
    t->add_column(type_Mixed, "mixed");
    t->add_column_link(type_Link, "link", *t2);
    t->add_column_link(type_LinkList, "linklist", *t2);

    size_t null_int_ndx = t->add_column(type_Int, "nullable int", true);
    size_t null_bool_ndx = t->add_column(type_Bool, "nullable bool", true);
    size_t null_str_ndx = t->add_column(type_String, "nullable string", true);
    size_t null_ts_ndx = t->add_column(type_Timestamp, "nullable timestamp", true);

    t->add_search_index(bool_ndx);
    t->add_search_index(int_ndx);
    t->add_search_index(str_ndx);
    t->add_search_index(ts_ndx);
    t->add_search_index(null_bool_ndx);
    t->add_search_index(null_int_ndx);
    t->add_search_index(null_str_ndx);
    t->add_search_index(null_ts_ndx);

    t->add_empty_row(6);
    t->set_string(str_ndx, 0, base2_b);
    t->set_string(str_ndx, 1, base2_c);
    t->set_string(str_ndx, 2, "aaaaaaaaaa");
    t->set_string(str_ndx, 3, "aaaaaaaaaa");
    t->set_string(str_ndx, 4, base2);
    t->set_string(str_ndx, 5, base2);

    t->set_string(null_str_ndx, 0, base2_b);
    t->set_string(null_str_ndx, 1, base2_c);
    t->set_string(null_str_ndx, 2, "aaaaaaaaaa");
    t->set_string(null_str_ndx, 3, "aaaaaaaaaa");
    t->set_string(null_str_ndx, 4, base2);
    t->set_string(null_str_ndx, 5, base2);

    g.write(path);
#endif // REALM_CLUSTER_IF
#endif // TEST_READ_UPGRADE_MODE
}


TEST_IF(Upgrade_Database_6_7, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_6_to_7.realm";

#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        // Constructing this SharedGroup will trigger an upgrade
        auto hist = make_in_realm_history();
        DBRef sg = DB::create(*hist, temp_copy);

        auto rt = sg->start_read();
        CHECK_EQUAL(rt->get_history_schema_version(), hist->get_history_schema_version());

        ConstTableRef t = rt->get_table("table");
        auto col = t->get_column_key("value");
        CHECK(t);
        CHECK_EQUAL(t->size(), 1);
        CHECK_EQUAL(t->begin()->get<Int>(col), 123);
    }
#else  // test write mode
#ifndef REALM_CLUSTER_IF
    // NOTE: This code must be executed from an old file-format-version 6
    // core in order to create a file-format-version 6 test file!

    Group g;
    TableRef t = g.add_table("table");
    size_t col = t->add_column(type_Int, "value");
    size_t row = t->add_empty_row();
    t->set_int(col, row, 123);
    g.write(path);
#endif // REALM_CLUSTER_IF
#endif // TEST_READ_UPGRADE_MODE
}

TEST_IF(Upgrade_Database_7_8, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_7_to_8.realm";

#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        // Constructing this SharedGroup will trigger an upgrade
        auto hist = make_in_realm_history();
        DBRef sg = DB::create(*hist, temp_copy);

        auto rt = sg->start_read();
        CHECK_EQUAL(rt->get_history_schema_version(), hist->get_history_schema_version());

        ConstTableRef t = rt->get_table("table");
        auto col = t->get_column_key("value");
        CHECK(t);
        CHECK_EQUAL(t->size(), 1);
        CHECK_EQUAL(t->begin()->get<Int>(col), 123);
    }
#else  // test write mode
#ifndef REALM_CLUSTER_IF
    // NOTE: This code must be executed from an old file-format-version 7
    // core in order to create a file-format-version 7 test file!

    Group g;
    TableRef t = g.add_table("table");
    size_t col = t->add_column(type_Int, "value");
    size_t row = t->add_empty_row();
    t->set_int(col, row, 123);
    g.write(path);
#endif // REALM_CLUSTER_IF
#endif // TEST_READ_UPGRADE_MODE
}


TEST_IF(Upgrade_Database_8_9, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_8_to_9.realm";
    std::string validation_str = "test string";
#if TEST_READ_UPGRADE_MODE

    // Automatic upgrade from SharedGroup
    {
        CHECK_OR_RETURN(File::exists(path));
        SHARED_GROUP_TEST_PATH(temp_copy);

        // Make a copy of the database so that we keep the original file intact and unmodified
        File::copy(path, temp_copy);

        // Constructing this SharedGroup will trigger an upgrade
        auto hist = make_in_realm_history();
        DBRef sg = DB::create(*hist, temp_copy);

        auto rt = sg->start_read();
        CHECK_EQUAL(rt->get_history_schema_version(), hist->get_history_schema_version());

        ConstTableRef t = rt->get_table("table");
        auto col_int = t->get_column_key("value");
        auto col_str = t->get_column_key("str_col");
        CHECK(t);
        CHECK_EQUAL(t->size(), 1);
        CHECK_EQUAL(t->begin()->get<Int>(col_int), 123);
        CHECK_EQUAL(t->begin()->get<String>(col_str), validation_str);
    }
#else  // test write mode
#ifndef REALM_CLUSTER_IF
    // NOTE: This code must be executed from an old file-format-version 8
    // core in order to create a file-format-version 8 test file!

    Group g;
    TableRef t = g.add_table("table");
    size_t col = t->add_column(type_Int, "value");
    size_t str_col = t->add_column(type_String, "str_col", true);
    t->add_search_index(str_col);
    size_t row = t->add_empty_row();
    t->set_int(col, row, 123);
    t->set_string(str_col, row, validation_str);
    g.write(path);
#endif // REALM_CLUSTER_IF
#endif // TEST_READ_UPGRADE_MODE
}

TEST(Upgrade_Database_6_10)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_6.realm";
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    DBRef sg = DB::create(*hist, temp_copy);
    ReadTransaction rt(sg);
    auto t = rt.get_table("table");
    CHECK(t);
}

namespace {
constexpr bool generate_json = false;
}

TEST(Upgrade_Database_9_10_with_pk_table)
{
    /* File has this content:
    {
    "pk":[
        {"_key":0,"pk_table":"link origin","pk_property":"pk"}
        {"_key":1,"pk_table":"object","pk_property":"pk"}
    ],
    "metadata":[
        {"_key":0,"version":0}
    ],
    "class_dog":[
    ],
    "class_link origin":[
        {"_key":0,"pk":5,"object":null,"array":{"table": "class_object", "keys": []}},
        {"_key":1,"pk":6,"object":{"table": "class_object", "key": 0},"array":{"table": "class_object", "keys": []}},
        {"_key":2,"pk":7,"object":null,"array":{"table": "class_object", "keys": [1,2]}}
    ],
    "class_object":[
        {"_key":0,"pk":"hello","value":7,"enum":"red","list":[""],"optional":null},
        {"_key":1,"pk":"world","value":35,"enum":"blue","list":[],"optional":null},
        {"_key":2,"pk":"goodbye","value":800,"enum":"red","list":[],"optional":-87}
    ]
    }
    */

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_9_to_10_pk_table.realm";
    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    auto sg = DB::create(*hist, temp_copy);
    ReadTransaction rt(sg);
    rt.get_group().verify();
    CHECK_EQUAL(rt.get_group().size(), 4);
    // rt.get_group().to_json(std::cout);

    ConstTableRef t_object = rt.get_table("class_object");
    ConstTableRef t_origin = rt.get_table("class_link origin");
    ConstTableRef t_dog = rt.get_table("class_dog");

    auto pk_col = t_object->get_primary_key_column();
    CHECK(pk_col);
    CHECK_EQUAL(t_object->get_column_name(pk_col), "pk");
    auto hello_key = t_object->find_first_string(pk_col, "hello");
    auto obj1 = t_object->get_object(hello_key);
    CHECK_EQUAL(obj1.get<Int>("value"), 7);
    auto enum_col_key = t_object->get_column_key("enum");
    CHECK(t_object->is_enumerated(enum_col_key));
    CHECK_EQUAL(obj1.get<String>(enum_col_key), "red");
    auto list = obj1.get_list<String>(t_object->get_column_key("list"));
    CHECK_EQUAL(list.size(), 1);
    CHECK_EQUAL(list.get(0), "");

    pk_col = t_origin->get_primary_key_column();
    CHECK(pk_col);
    CHECK_EQUAL(t_origin->get_column_name(pk_col), "pk");
    auto key_6 = t_origin->find_first_int(pk_col, 6);
    auto obj2 = t_origin->get_object(key_6);
    CHECK_EQUAL(obj2.get<ObjKey>("object"), hello_key);

    pk_col = t_dog->get_primary_key_column();
    CHECK(pk_col);
}

TEST_IF(Upgrade_Database_9_10, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    size_t nb_rows = (REALM_MAX_BPNODE_SIZE == 4) ? 50 : 500;
    size_t insert_pos = (REALM_MAX_BPNODE_SIZE == 4) ? 40 : 177;

    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_9_to_10.realm";
#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();

    int iter = 2;
    while (iter) {
        int max_try = 10;
        DBRef sg;
        while (max_try--) {
            try {
                // Constructing this SharedGroup will trigger an upgrade first time around
                sg = DB::create(*hist, temp_copy);
                break;
            }
            catch (...) {
                // Upgrade failed - try again
            }
        }
        ReadTransaction rt(sg);

        ConstTableRef t = rt.get_table("table");
        ConstTableRef o = rt.get_table("other");
        ConstTableRef e = rt.get_table("empty");
        rt.get_group().verify();

        CHECK(t);
        CHECK(o);
        CHECK_EQUAL(t->size(), nb_rows + 1);
        CHECK_EQUAL(o->size(), 25);
        CHECK_EQUAL(e->size(), 0);

        auto t_col_keys = t->get_column_keys();
        CHECK_EQUAL(t_col_keys.size(), 14);
        auto o_col_keys = o->get_column_keys();
        CHECK_EQUAL(o_col_keys.size(), 2);
        auto e_col_keys = e->get_column_keys();
        CHECK_EQUAL(e_col_keys.size(), 0);

        auto col_int = t_col_keys[0];
        auto col_int_null = t_col_keys[1];
        auto col_bool = t_col_keys[2];
        auto col_bool_null = t_col_keys[3];
        auto col_float = t_col_keys[4];
        auto col_double = t_col_keys[5];
        auto col_string = t_col_keys[6];
        auto col_string_i = t_col_keys[7];
        auto col_binary = t_col_keys[8];
        auto col_date = t_col_keys[9];
        auto col_link = t_col_keys[10];
        auto col_linklist = t_col_keys[11];
        auto col_int_list = t_col_keys[12];
        auto col_int_null_list = t_col_keys[13];
        auto col_val = o_col_keys[0];
        auto col_str_list = o_col_keys[1];

        CHECK_EQUAL(t->get_column_name(col_int), "int");
        CHECK_EQUAL(t->get_column_name(col_int_null), "int_1");
        CHECK_EQUAL(t->get_column_name(col_bool), "col_2");
        CHECK_EQUAL(t->get_column_name(col_bool_null), "bool_null");
        CHECK_EQUAL(t->get_column_name(col_float), "float");
        CHECK_EQUAL(t->get_column_name(col_double), "double");
        CHECK_EQUAL(t->get_column_name(col_string), "string");
        CHECK_EQUAL(t->get_column_name(col_string_i), "string_i");
        CHECK_EQUAL(t->get_column_name(col_binary), "binary");
        CHECK_EQUAL(t->get_column_name(col_date), "date");
        CHECK_EQUAL(t->get_column_name(col_link), "link");
        CHECK_EQUAL(t->get_column_name(col_linklist), "linklist");
        CHECK_EQUAL(t->get_column_name(col_int_list), "integers");
        CHECK_EQUAL(o->get_column_name(col_val), "val");
        CHECK_EQUAL(o->get_column_name(col_str_list), "strings");

        CHECK_EQUAL(t->get_column_type(col_int_null), type_Int);
        CHECK_EQUAL(t->get_column_type(col_bool), type_Bool);
        CHECK_EQUAL(t->get_column_type(col_bool_null), type_Bool);
        CHECK_EQUAL(t->get_column_type(col_float), type_Float);
        CHECK_EQUAL(t->get_column_type(col_double), type_Double);
        CHECK_EQUAL(t->get_column_type(col_string), type_String);
        CHECK_EQUAL(t->get_column_type(col_string_i), type_String);
        CHECK_EQUAL(t->get_column_type(col_binary), type_Binary);
        CHECK_EQUAL(t->get_column_type(col_date), type_Timestamp);
        CHECK_EQUAL(t->get_column_type(col_link), type_Link);
        CHECK_EQUAL(t->get_column_type(col_linklist), type_LinkList);
        CHECK_EQUAL(t->get_column_type(col_int_list), type_Int);
        CHECK(t->get_column_attr(col_int_list).test(col_attr_List));
        CHECK_EQUAL(t->get_column_type(col_int_null_list), type_Int);
        CHECK(t->get_column_attr(col_int_null_list).test(col_attr_List));
        CHECK_EQUAL(o->get_column_type(col_val), type_Int);
        CHECK_EQUAL(o->get_column_type(col_str_list), type_String);
        CHECK(col_str_list.get_attrs().test(col_attr_List));

        CHECK_EQUAL(t->is_nullable(col_int), false);
        CHECK_EQUAL(t->is_nullable(col_int_null), true);
        CHECK_EQUAL(t->is_nullable(col_bool), false);
        CHECK_EQUAL(t->is_nullable(col_bool_null), true);
        CHECK_EQUAL(t->is_nullable(col_float), false);
        CHECK_EQUAL(t->is_nullable(col_double), false);
        CHECK_EQUAL(t->is_nullable(col_string), false);
        CHECK_EQUAL(t->is_nullable(col_string_i), true);
        CHECK_EQUAL(t->is_nullable(col_binary), false);
        CHECK_EQUAL(t->is_nullable(col_date), false);
        CHECK_EQUAL(t->is_nullable(col_link), true);
        CHECK_EQUAL(t->is_nullable(col_linklist), false);
        CHECK_EQUAL(t->is_nullable(col_int_list), false);
        CHECK_EQUAL(t->is_nullable(col_int_null_list), true);

        CHECK_EQUAL(t->has_search_index(col_string), false);
        CHECK_EQUAL(t->has_search_index(col_string_i), true);

        const Obj obj0 = t->get_object(ObjKey(0));
        CHECK(obj0.is_null(col_int_null));
        CHECK(obj0.is_null(col_bool_null));

        const Obj obj17 = t->get_object(ObjKey(17));
        const Obj obj18 = t->get_object(ObjKey(18));
        const Obj obj23 = t->get_object(ObjKey(23));
        const Obj obj27 = t->get_object(ObjKey(27));
        const Obj obj = t->get_object(ObjKey(insert_pos));

        CHECK_EQUAL(obj17.get<Int>(col_int), 17);
        CHECK_EQUAL(obj17.get<util::Optional<Int>>(col_int_null), 17);
        CHECK_EQUAL(obj17.get<Bool>(col_bool), false);
        CHECK_EQUAL(obj17.get<util::Optional<Bool>>(col_bool_null), false);
        CHECK_EQUAL(obj17.get<Float>(col_float), 17 * 1.5f);
        CHECK_EQUAL(obj17.get<Double>(col_double), 17 * 2.5);
        CHECK_EQUAL(obj17.get<String>(col_string), "This is a medium long string");
        CHECK_EQUAL(t->find_first(col_string_i, StringData("This is a somewhat longer string17")), obj17.get_key());
        std::string bigbin(1000, 'x');
        CHECK_EQUAL(obj17.get<Binary>(col_binary), BinaryData(bigbin));
        CHECK_EQUAL(obj17.get<Timestamp>(col_date), Timestamp(1700, 17));
        CHECK_EQUAL(obj17.get<ObjKey>(col_link), obj27.get_key());
        auto int_list_null = obj17.get_list<Int>(col_int_list);
        CHECK(int_list_null.is_empty());

        CHECK_EQUAL(obj18.get<String>(col_string), "");
        CHECK_EQUAL(obj18.get<String>(col_string_i), StringData());

        auto int_list = obj23.get_list<Int>(col_int_list);
        CHECK(!int_list.is_empty());
        CHECK_EQUAL(int_list.size(), 18);
        CHECK_EQUAL(int_list.get(0), 24);
        CHECK_EQUAL(int_list.get(17), 41);

        auto int_null_list = obj23.get_list<util::Optional<Int>>(col_int_null_list);
        CHECK(!int_null_list.is_empty());
        CHECK_EQUAL(int_null_list.size(), 10);
        CHECK_EQUAL(int_null_list.get(1), 5);
        CHECK_NOT(int_null_list.get(5));

        CHECK_EQUAL(obj27.get<Bool>(col_bool), true);
        std::string bin("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwx");
        CHECK_EQUAL(obj27.get<Binary>(col_binary), BinaryData(bin));
        CHECK_EQUAL(obj27.get_backlink_count(*t, col_link), 1);
        CHECK_EQUAL(obj27.get_backlink(*t, col_link, 0), obj17.get_key());
        auto ll = obj27.get_linklist(col_linklist);
        CHECK_EQUAL(ll.size(), 7);
        const Obj linked_obj = ll.get_object(0);
        CHECK_EQUAL(linked_obj.get_key(), ObjKey(12));
        size_t expected_back_link_count = (REALM_MAX_BPNODE_SIZE == 4) ? 1 : 4;
        CHECK_EQUAL(linked_obj.get_backlink_count(*t, col_linklist), expected_back_link_count);

        CHECK_EQUAL(obj.get<String>(col_string_i),
                    "This is a rather long string, that should not be very much shorter");
        CHECK_EQUAL(obj.get<Binary>(col_binary), BinaryData("", 0));

        const Obj obj14 = o->get_object(14);

        auto str_list = obj14.get_list<String>(col_str_list);
        CHECK_EQUAL(str_list.size(), 3);
        CHECK_EQUAL(str_list.get(0), StringData("Medium_Sized_String_0"));
        CHECK_EQUAL(str_list.get(1), StringData("Medium_Sized_String_1"));
        CHECK(str_list.is_null(2));

        --iter;
    }
    if (REALM_MAX_BPNODE_SIZE == 1000) {
        auto sg = DB::create(*hist, temp_copy);
        if (generate_json) {
            std::ofstream expect("expect_test_upgrade_database_9_to_10.json");
            sg->start_read()->to_json(expect, 0);
        }
        std::stringstream out;
        sg->start_read()->to_json(out, 0);
        std::ifstream expect("expect_test_upgrade_database_9_to_10.json");
        std::string str1;
        std::string str2;
        expect >> str1;
        out >> str2;
        while (expect) {
            if (str1 != str2) {
                CHECK_EQUAL(str1, str2);
                break;
            }
            expect >> str1;
            out >> str2;
        }
    }

#else
    // NOTE: This code must be executed from an old file-format-version 9
    // core in order to create a file-format-version 9 test file!

#ifndef REALM_CLUSTER_IF
    Group g;
    TableRef t = g.add_table("table");
    TableRef o = g.add_table("other");
    g.add_table("empty");
    size_t col_int = t->add_column(type_Int, "int");
    size_t col_int_null = t->add_column(type_Int, "int", true); // Duplicate name
    size_t col_bool = t->add_column(type_Bool, "");             // Missing name
    size_t col_bool_null = t->add_column(type_Bool, "bool_null", true);
    size_t col_float = t->add_column(type_Float, "float");
    size_t col_double = t->add_column(type_Double, "double");
    size_t col_string = t->add_column(type_String, "string");
    size_t col_string_i = t->add_column(type_String, "string_i", true);
    size_t col_binary = t->add_column(type_Binary, "binary");
    size_t col_date = t->add_column(type_Timestamp, "date");
    size_t col_link = t->add_column_link(type_Link, "link", *t);
    size_t col_linklist = t->add_column_link(type_LinkList, "linklist", *o);

    DescriptorRef subdesc;
    size_t col_int_list = t->add_column(type_Table, "integers", false, &subdesc);
    subdesc->add_column(type_Int, "list");

    size_t col_int_null_list = t->add_column(type_Table, "intnulls", false, &subdesc);
    subdesc->add_column(type_Int, "list", nullptr, true);

    size_t col_val = o->add_column(type_Int, "val");
    size_t col_string_list = o->add_column(type_Table, "strings", false, &subdesc);
    subdesc->add_column(type_String, "list", nullptr, true);

    t->add_search_index(col_string_i);

    t->add_empty_row(nb_rows);
    o->add_empty_row(25);
    for (size_t i = 0; i < nb_rows; i++) {
        if (i % 2) {
            t->set_int(col_int, i, i);
            t->set_int(col_int_null, i, i);
            t->set_bool(col_bool, i, (i % 3) == 0);
            t->set_bool(col_bool_null, i, (i % 3) == 0);
            t->set_float(col_float, i, i * 1.5f);
            t->set_double(col_double, i, i * 2.5);

            // String
            std::string str = "foo ";
            str += util::to_string(i);
            t->set_string(col_string, i, str);
            str = "This is a somewhat longer string";
            str += util::to_string(i);
            t->set_string(col_string_i, i, str);

            // Binary
            if (i % 9 == 0) {
                const char bin[] = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
                t->set_binary(col_binary, i, BinaryData(bin, strlen(bin) - i / 10));
            }

            // Timestamp
            t->set_timestamp(col_date, i, Timestamp(100 * i, i));

            // Link
            if ((i % 17) == 0) {
                t->set_link(col_link, i, (i + 10) % nb_rows);
            }

            // LinkList
            if ((i % 27) == 0) {
                auto lv = t->get_linklist(col_linklist, i);
                for (size_t j = 0; j < i % 10; j++) {
                    lv->add(j + i % 15);
                }
            }
            // ListOfPrimitives
            if ((i % 23) == 0) {
                auto st = t->get_subtable(col_int_list, i);
                int nb_elements = 16 + i % 20;
                st->add_empty_row(nb_elements);
                for (int j = 0; j < nb_elements; j++) {
                    st->set_int(0, j, j + i % 30);
                }
                st->remove(0);
            }
            // ListOfOptionals
            if (i == 23) {
                auto st = t->get_subtable(col_int_null_list, i);
                st->add_empty_row(10);
                for (int j = 0; j < 10; j++) {
                    if (j != 5)
                        st->set_int(0, j, 5 * j);
                }
            }
        }
    }

    for (size_t i = 0; i < 25; i++) {
        o->set_int(col_val, i, i);
        auto st = o->get_subtable(col_string_list, i);
        if (i % 5 == 0) {
            for (size_t j = 0; j < i / 5; j++) {
                st->add_empty_row();
                std::string str = "String_" + std::to_string(j);
                st->set_string(0, j, str);
            }
        }
        if (i % 7 == 0) {
            for (size_t j = 0; j < i / 7; j++) {
                st->add_empty_row();
                std::string str = "Medium_Sized_String_" + std::to_string(j);
                st->set_string(0, j, str);
            }
            st->add_empty_row();
        }
    }

    t->set_string(col_string, 17, "This is a medium long string");
    std::string bigbin(1000, 'x');
    t->set_binary(col_binary, 17, BinaryData(bigbin));
    t->insert_empty_row(insert_pos);
    t->set_string(col_string_i, insert_pos, "This is a rather long string, that should not be very much shorter");

    g.write(path);
#endif
#endif // TEST_READ_UPGRADE_MODE
}

TEST_IF(Upgrade_Database_10_11, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_10_to_11.realm";
    std::vector<int64_t> ids = {0, 2, 3, 15, 42, 100, 7000};
#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    auto sg = DB::create(*hist, temp_copy);
    auto rt = sg->start_read();

    auto t = rt->get_table("table");
    auto o = rt->get_table("origin");
    auto col = o->get_column_key("link");

    auto it = o->begin();
    for (auto id : ids) {
        auto obj = t->get_object_with_primary_key(id);
        CHECK_EQUAL(obj.get_backlink_count(), 1);
        CHECK_EQUAL(it->get<ObjKey>(col), obj.get_key());
        ++it;
    }

#else
    // NOTE: This code must be executed from an old file-format-version 10
    // core in order to create a file-format-version 10 test file!

    Group g;
    TableRef t = g.add_table_with_primary_key("table", type_Int, "id", false);
    TableRef o = g.add_table("origin");
    auto col = o->add_column(*t, "link");

    for (auto id : ids) {
        auto obj = t->create_object_with_primary_key(id);
        o->create_object().set(col, obj.get_key());
    }

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}

TEST_TYPES(Upgrade_Database_11, std::true_type, std::false_type)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_11.realm";
    std::vector<int64_t> ids = {0, 2, 3, 15, 42, 100, 7000};
    std::vector<std::string> names = {"adam", "bart", "carlo", "david", "eric", "frank", "gary"};
#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    DBOptions options;
    options.is_immutable = TEST_TYPE::value;
    auto sg = DB::create(*hist, temp_copy, options);
    auto rt = sg->start_read();

    auto foo = rt->get_table("foo");
    auto bar = rt->get_table("bar");
    auto o = rt->get_table("origin");
    auto col1 = o->get_column_key("link1");
    auto col2 = o->get_column_key("link2");
#else
    // NOTE: This code must be executed from an old file-format-version 20
    // core in order to create a file-format-version 20 test file!

    Group g;
    TableRef foo = g.add_table_with_primary_key("foo", type_Int, "id", false);
    TableRef bar = g.add_table_with_primary_key("bar", type_String, "name", false);
    TableRef o = g.add_table("origin");
    auto col1 = o->add_column_link(type_Link, "link1", *foo);
    auto col2 = o->add_column_link(type_Link, "link2", *bar);

    for (auto id : ids) {
        auto obj = foo->create_object_with_primary_key(id);
        o->create_object().set(col1, obj.get_key());
    }

    for (auto name : names) {
        auto obj = bar->create_object_with_primary_key(name);
        o->create_object().set(col2, obj.get_key());
    }

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE

    auto get_object = [](TableRef table, auto pk) {
        auto col = table->get_primary_key_column();
        auto key = table->find_first(col, pk);
        return table->get_object(key);
    };

    auto it = o->begin();
    for (auto id : ids) {
        auto obj = get_object(foo, id);
        CHECK_EQUAL(obj.get_backlink_count(), 1);
        CHECK_EQUAL(it->get<ObjKey>(col1), obj.get_key());
        ++it;
    }
    for (auto name : names) {
        auto obj = get_object(bar, StringData(name));
        CHECK_EQUAL(obj.get_backlink_count(), 1);
        CHECK_EQUAL(it->get<ObjKey>(col2), obj.get_key());
        ++it;
    }
}

TEST_IF(Upgrade_Database_20, REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_20.realm";
    std::vector<int64_t> ids = {0, 2, 3, 15, 42, 100, 7000};
    std::vector<std::string> names = {"adam", "bart", "carlo", "david", "eric", "frank", "gary"};
#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    auto sg = DB::create(*hist, temp_copy);
    auto wt = sg->start_write();

    auto foo = wt->get_table("foo");
    auto bar = wt->get_table("bar");
    auto o = wt->get_table("origin");
    auto col1 = o->get_column_key("link1");
    auto col2 = o->get_column_key("link2");
    ObjKey bl1(4);
    ObjKey bl2(11);

#else
    // NOTE: This code must be executed from an old file-format-version 20
    // core in order to create a file-format-version 20 test file!

    Group g;
    TableRef foo = g.add_table_with_primary_key("foo", type_Int, "id", false);
    TableRef bar = g.add_table_with_primary_key("bar", type_String, "name", false);
    TableRef o = g.add_table("origin");
    auto col1 = o->add_column(*foo, "link1");
    auto col2 = o->add_column(*bar, "link2");

    for (auto id : ids) {
        auto obj = foo->create_object_with_primary_key(id);
        o->create_object().set(col1, obj.get_key());
    }

    for (auto name : names) {
        auto obj = bar->create_object_with_primary_key(name);
        o->create_object().set(col2, obj.get_key());
    }

    auto o1 = foo->get_object_with_primary_key(ids[4]);
    auto bl1 = o1.get_backlink(*o, col1, 0);
    o1.invalidate();
    auto o2 = bar->get_object_with_primary_key(names[4]);
    auto bl2 = o2.get_backlink(*o, col2, 0);
    o2.invalidate();

    g.write(path);
#endif // TEST_READ_UPGRADE_MODE

    CHECK_EQUAL(foo->nb_unresolved(), 1);
    CHECK_EQUAL(bar->nb_unresolved(), 1);
    CHECK_NOT(o->get_object(bl1).get<ObjKey>(col1));
    CHECK_NOT(o->get_object(bl2).get<ObjKey>(col2));
    // We test that we are able to identify the tombstones for these primary keys
    foo->create_object_with_primary_key(ids[4]);
    bar->create_object_with_primary_key(names[4]);
    CHECK_EQUAL(foo->nb_unresolved(), 0);
    CHECK_EQUAL(bar->nb_unresolved(), 0);
    CHECK(o->get_object(bl1).get<ObjKey>(col1));
    CHECK(o->get_object(bl2).get<ObjKey>(col2));

    auto it = o->begin();
    for (auto id : ids) {
        auto obj = foo->get_object_with_primary_key(id);
        CHECK_EQUAL(obj.get_backlink_count(), 1);
        CHECK_EQUAL(it->get<ObjKey>(col1), obj.get_key());
        ++it;
    }
    for (auto name : names) {
        auto obj = bar->get_object_with_primary_key(name);
        CHECK_EQUAL(obj.get_backlink_count(), 1);
        CHECK_EQUAL(it->get<ObjKey>(col2), obj.get_key());
        ++it;
    }
}

TEST(Upgrade_progress)
{
    SHARED_GROUP_TEST_PATH(temp_copy);
    auto hist = make_in_realm_history();

    for (int i = 1; i <= 7; i++) {
        auto fn = test_util::get_test_resource_path() + "test_upgrade_progress_" + util::to_string(i) + ".realm";
        File::copy(fn, temp_copy);
        DB::create(*hist, temp_copy)->start_read()->verify();
    }
}

TEST(Upgrade_FixColumnKeys)
{
    SHARED_GROUP_TEST_PATH(temp_copy);
    // The "object" table in this file contains an m_keys array where the keys for the
    // backlink columns are wrong.
    auto fn = test_util::get_test_resource_path() + "test_upgrade_colkey_error.realm";
    File::copy(fn, temp_copy);

    auto hist = make_in_realm_history();
    DB::create(*hist, temp_copy)->start_read()->verify();
}

NONCONCURRENT_TEST(Upgrade_BackupAtoBtoAtoC)
{
    SHARED_GROUP_TEST_PATH(path);
    std::string prefix = realm::BackupHandler::get_prefix_from_path(path);
    // clear out any leftovers from potential earlier crash of unittest
    File::try_remove(prefix + "v200.backup.realm");

    // Build a realm file with format 200
    _impl::GroupFriend::fake_target_file_format(200);
    {
        DBOptions options;
        options.accepted_versions = {200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->add_table("MyTable");
        table->add_column(type_String, "names");
        tr->commit();
    }

    // upgrade to format 201
    _impl::GroupFriend::fake_target_file_format(201);
    {
        DBOptions options;
        options.accepted_versions = {201, 200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->get_table("MyTable");
        auto col = table->get_column_key("names");
        table->create_object().set(col, "hr hansen");
        tr->commit();
    }
    CHECK(File::exists(prefix + "v200.backup.realm"));

    // downgrade/restore backup of format 200
    _impl::GroupFriend::fake_target_file_format(200);
    {
        DBOptions options;
        options.accepted_versions = {200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->get_table("MyTable");
        auto col = table->get_column_key("names");
        CHECK(table->size() == 0); // no sign of "hr hansen"
        table->create_object().set(col, "mr Kirby");
        tr->commit();
    }
    CHECK(!File::exists(prefix + "v200.backup.realm"));

    // move forward to version 202, bypassing the outlawed 201
    _impl::GroupFriend::fake_target_file_format(202);
    {
        DBOptions options;
        options.accepted_versions = {202, 200};
        options.to_be_deleted = {{201, 0}};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->get_table("MyTable");
        CHECK(table->size() == 1);
        tr->commit();
    }
    CHECK(File::exists(prefix + "v200.backup.realm"));

    // Cleanup file and disable mockup versioning
    File::try_remove(prefix + "v200.backup.realm");
    _impl::GroupFriend::fake_target_file_format({});
}

NONCONCURRENT_TEST(Upgrade_BackupAtoBbypassAtoC)
{
    SHARED_GROUP_TEST_PATH(path);
    std::string prefix = realm::BackupHandler::get_prefix_from_path(path);
    // clear out any leftovers from potential earlier crash of unittest
    File::try_remove(prefix + "v200.backup.realm");
    File::try_remove(prefix + "v201.backup.realm");

    // Build a realm file with format 200
    _impl::GroupFriend::fake_target_file_format(200);
    {
        DBOptions options;
        options.accepted_versions = {200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->add_table("MyTable");
        table->add_column(type_String, "names");
        tr->commit();
    }

    // upgrade to format 201
    _impl::GroupFriend::fake_target_file_format(201);
    {
        DBOptions options;
        options.accepted_versions = {201, 200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->get_table("MyTable");
        auto col = table->get_column_key("names");
        table->create_object().set(col, "hr hansen");
        tr->commit();
    }
    CHECK(File::exists(prefix + "v200.backup.realm"));

    // upgrade further to 202, based on 201, to create a v201 backup
    _impl::GroupFriend::fake_target_file_format(202);
    {
        DBOptions options;
        options.accepted_versions = {202, 201, 200};
        options.to_be_deleted = {};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
    }
    CHECK(File::exists(prefix + "v200.backup.realm"));
    CHECK(File::exists(prefix + "v201.backup.realm"));

    // downgrade/restore backup of format 200, but simulate that no downgrade
    // is actually shipped. Instead directly move forward to version 203,
    // bypassing the outlawed 201 and 202. Set an age limit of 2 seconds for any backuo
    // of version 201 to prevent it from being deleted
    _impl::GroupFriend::fake_target_file_format(203);
    {
        DBOptions options;
        options.accepted_versions = {203, 200};
        options.to_be_deleted = {{201, 2}};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
        auto tr = db->start_write();
        auto table = tr->get_table("MyTable");
        CHECK(table->size() == 0);
        tr->commit();
    }

    CHECK(File::exists(prefix + "v200.backup.realm"));
    CHECK(File::exists(prefix + "v201.backup.realm"));
    // Ask for v201 to have a max age of one sec.
    // When opened more than a sec later,
    // the v201 backup will be too old and automagically removed
    millisleep(2000);
    {
        DBOptions options;
        options.accepted_versions = {203, 200};
        options.to_be_deleted = {{201, 1}};
        auto hist = make_in_realm_history();
        auto db = DB::create(*hist, path, options);
    }
    CHECK(File::exists(prefix + "v200.backup.realm"));
    CHECK(!File::exists(prefix + "v201.backup.realm"));

    // Cleanup file and disable mockup versioning
    File::try_remove(prefix + "v200.backup.realm");
    _impl::GroupFriend::fake_target_file_format({});
}

#endif // TEST_GROUP
