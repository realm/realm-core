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
#include <realm/query_expression.hpp>
#include <realm/util/file.hpp>
#include <realm/util/to_string.hpp>
#include <realm/version.hpp>
#include "test.hpp"
#include "test_table_helper.hpp"

#include <external/json/json.hpp>

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

TEST_IF(Upgrade_DatabaseWithCallback, REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_20.realm";
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);

    bool did_upgrade = false;
    DBOptions options;
    options.no_create = true;
    options.upgrade_callback = [&](int old_version, int new_version) {
        did_upgrade = true;
        CHECK_EQUAL(old_version, 20);
        CHECK_EQUAL(new_version, Group::get_current_file_format_version());
    };
    DB::create(temp_copy, options);
    CHECK(did_upgrade);
}

TEST_IF(Upgrade_DatabaseWithCallbackWithException, REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_20.realm";
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);

    // Callback that throws should revert the upgrade
    DBOptions options;
    options.no_create = true;
    options.upgrade_callback = [&](int, int) {
        throw 123;
    };
    CHECK_THROW(DB::create(temp_copy, options), int);

    // Callback should be triggered here because the file still needs to be upgraded
    bool did_upgrade = false;
    options.upgrade_callback = [&](int old_version, int new_version) {
        did_upgrade = true;
        CHECK_EQUAL(old_version, 20);
        CHECK_EQUAL(new_version, Group::get_current_file_format_version());
    };
    DB::create(temp_copy, options);
    CHECK(did_upgrade);

    // Callback should not be triggered here because the file is already upgraded
    did_upgrade = false;
    DB::create(temp_copy, options);
    CHECK_NOT(did_upgrade);
}

static void compare_files(test_util::unit_test::TestContext& test_context, const std::string& old_path,
                          const std::string& new_path)
{
    // We might expand the file to the page size so that we can mmap it, but
    // otherwise the file should be left unchanged
    File old_file(old_path);
    File new_file(new_path);
    size_t old_size = size_t(old_file.get_size());
    size_t new_size = size_t(new_file.get_size());
    if (new_size != old_size) {
        size_t rounded_up_to_page_size = round_up_to_page_size(old_size);
        if (!CHECK_EQUAL(new_size, rounded_up_to_page_size)) {
            return;
        }
    }

    auto old_buffer = std::make_unique<char[]>(old_size);
    auto new_buffer = std::make_unique<char[]>(old_size);
    old_file.read(0, old_buffer.get(), old_size);
    new_file.read(0, new_buffer.get(), old_size);
    CHECK_NOT(memcmp(old_buffer.get(), new_buffer.get(), old_size));
}

TEST(Upgrade_Disabled)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_20.realm";
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);

    DBOptions options;
    options.allow_file_format_upgrade = false;
    CHECK_THROW_ANY(DB::create(make_in_realm_history(), temp_copy, options));

    // Verify that we didn't modify the input file
    compare_files(test_context, path, temp_copy);
}

TEST(Upgrade_DatabaseWithUnsupportedOldFileFormat)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_1000_1.realm";
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);

    // Should fail to upgrade because it's too old
    CHECK_THROW(DB::create(temp_copy), UnsupportedFileFormatVersion);

    // Verify that we didn't modify the input file
    compare_files(test_context, path, temp_copy);
}

NONCONCURRENT_TEST(Upgrade_DatabaseWithUnsupportedNewFileFormat)
{
    // Build a realm file with format 200
    SHARED_GROUP_TEST_PATH(path);
    _impl::GroupFriend::fake_target_file_format(200);
    Group().write(path);
    _impl::GroupFriend::fake_target_file_format({});

    SHARED_GROUP_TEST_PATH(path_2);
    File::copy(path, path_2);

    // Should fail to upgrade because it's too new
    CHECK_THROW(DB::create(path_2), UnsupportedFileFormatVersion);

    // Verify that we didn't modify the input file
    compare_files(test_context, path, path_2);
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

TEST(Upgrade_Database_11)
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
    auto sg = DB::create(*hist, temp_copy);
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

TEST(Upgrade_RecoverAsymmetricTables)
{
    // This file has a table that is marked as asymmetric, but has 3 objects in it
    std::string path = test_util::get_test_resource_path() + "downgrade_asymmetric.realm";
    SHARED_GROUP_TEST_PATH(temp_copy);
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    auto db = DB::create(*hist, temp_copy);
    auto rt = db->start_read();
    auto t = rt->get_table("ephemeral");
    CHECK(t->is_asymmetric());
    CHECK_EQUAL(t->size(), 0);
    t = rt->get_table("persistent");
    CHECK(!t->is_asymmetric());
    CHECK_EQUAL(t->size(), 3);
}

TEST_IF(Upgrade_Database_22, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_22.realm";

    std::map<std::string, Mixed> dict_values = {
        {"one", 1},         {"two", 2.},      {"three", "John"},   {"four", "Paul"},
        {"five", "George"}, {"six", "Ringo"}, {"seven", "Morgan"}, {"eight", Timestamp(4, 5)}};
    // The string an binary values will be intermixed using the old algorithm
    std::vector<Mixed> set_values = {1, 2., "John", "Ringo", BinaryData("Paul"), BinaryData("George"), "Beatles"};

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);

    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    CHECK(DB::needs_file_format_upgrade(temp_copy, {}));
    auto sg = DB::create(*hist, temp_copy);
    auto rt = sg->start_read();

    auto t = rt->get_table("table");
    auto target = rt->get_table("target");
    auto col_dict = t->get_column_key("dict");
    auto col_set = t->get_column_key("set");

    auto obj = t->get_object_with_primary_key(1);
    auto obj1 = target->get_object_with_primary_key(47);

    auto dict = obj.get_dictionary(col_dict);
    CHECK_EQUAL(dict.size(), dict_values.size() + 1);
    for (auto& entry : dict_values) {
        auto val = dict.get(entry.first);
        CHECK_EQUAL(val, entry.second);
    }
    auto link = dict["nine"].get_link();
    CHECK_EQUAL(link, obj1.get_link());

    auto set = obj.get_set<Mixed>(col_set);
    CHECK_EQUAL(set.size(), set_values.size() + 1);
    for (auto& val : set_values) {
        CHECK(set.find(val) != realm::npos);
    }
    CHECK(set.find(obj1.get_link()) != realm::npos);

    CHECK_EQUAL(obj1.get_backlink_count(), 2);

    rt->verify();

#else
    // NOTE: This code must be executed from an old file-format-version 22
    // core in order to create a file-format-version 10 test file!

    Group g;
    TableRef t = g.add_table_with_primary_key("table", type_Int, "id", false);
    TableRef target = g.add_table_with_primary_key("target", type_Int, "id", false);
    auto col_dict = t->add_column_dictionary(type_Mixed, "dict", true, type_String);
    auto col_set = t->add_column_set(type_Mixed, "set", true);

    auto obj = t->create_object_with_primary_key(1);
    auto obj1 = target->create_object_with_primary_key(47);
    auto dict = obj.get_dictionary(col_dict);
    for (auto& entry : dict_values) {
        dict.insert(entry.first, entry.second);
    }
    dict.insert("nine", obj1);
    auto set = obj.get_set<Mixed>(col_set);
    for (auto& val : set_values) {
        set.insert(val);
    }
    set.insert(obj1.get_link());
    g.to_json(std::cout);
    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}

// FIXME: add migration test for decimal128
TEST_IF(Upgrade_Database_23, REALM_MAX_BPNODE_SIZE == 4 || REALM_MAX_BPNODE_SIZE == 1000)
{
    // We should test that we can convert backlink arrays bigger that node size to BPlusTrees
    std::string path = test_util::get_test_resource_path() + "test_upgrade_database_" +
                       util::to_string(REALM_MAX_BPNODE_SIZE) + "_23.realm";

    const size_t cnt = 10 * REALM_MAX_BPNODE_SIZE;
    // string order changed in sets, and string indexes
    // The strings will be sorted according to the old ordering using signed chars
    std::string base(StringIndex::s_max_offset, 'a');

    // include long prefix strings > s_max_offset with non-ascii suffixes to test string ordering in the index
    std::vector<std::string> string_storage = {base + "aaaaa", base + "aaaaA", base + "aaaaʄ", base + "aaaaÆ",
                                               base + "aaaaæ"};
    std::vector<Mixed> set_values = {1,
                                     2.,
                                     "John",
                                     "Ringo",
                                     BinaryData("Paul"),
                                     BinaryData("George"),
                                     Timestamp(0, 0),
                                     ObjectId("abcdefabcdefabcdefabcdef"),
                                     UUID(),
                                     "Æ",
                                     "æ",
                                     "ÿ",
                                     "ʄ",
                                     "Beatles",
                                     "john",
                                     "ringo",
                                     {},
                                     "",
                                     StringData("\0", 1),
                                     StringData("\0\1", 2),
                                     StringData("\0\0\1", 3),
                                     StringData("\1\0", 2)};
    for (auto& str : string_storage) {
        set_values.push_back(StringData(str));
        set_values.push_back(BinaryData(str.data(), str.size()));
    }
    std::sort(set_values.begin(), set_values.end()); // using default Mixed::operator<

#if TEST_READ_UPGRADE_MODE
    CHECK_OR_RETURN(File::exists(path));

    SHARED_GROUP_TEST_PATH(temp_copy);


    // Make a copy of the database so that we keep the original file intact and unmodified
    File::copy(path, temp_copy);
    auto hist = make_in_realm_history();
    DBOptions options;
    options.logger = test_context.logger;
    auto sg = DB::create(*hist, temp_copy, options);
    auto rt = sg->start_write();
    // rt->to_json(std::cout);
    rt->verify();

    {
        // checks on many backlinks
        auto t = rt->get_table("table");
        auto target = rt->get_table("target");
        auto col_link = t->get_column_key("link");

        auto target_obj = target->get_object_with_primary_key(1);
        CHECK_EQUAL(target_obj.get_backlink_count(*t, col_link), cnt);
        // This should cause an upgrade
        auto o = t->create_object_with_primary_key(20000);
        o.set(col_link, target_obj.get_key());
        t->verify();
        // Check that we can delete all source objects
        for (auto& obj : *t) {
            obj.remove();
        }
        CHECK_EQUAL(target_obj.get_backlink_count(*t, col_link), 0);
    }

    {
        // checks on string sorting and ordering
        auto t = rt->get_table("table_for_mixed");
        auto col_mixed_set = t->get_column_key("mixedSet");
        auto col_string_set = t->get_column_key("stringSet");
        auto col_binary_set = t->get_column_key("binarySet");

        auto obj1 = t->get_object_with_primary_key(1);
        auto mixed_set1 = obj1.get_set<Mixed>(col_mixed_set);
        auto string_set1 = obj1.get_set<StringData>(col_string_set);
        auto binary_set1 = obj1.get_set<BinaryData>(col_binary_set);
        CHECK_EQUAL(mixed_set1.size(), set_values.size());

        // We will create some new sets with the same values as the old ones
        // The values should be in the same order

        auto obj2 = t->create_object_with_primary_key(2);
        auto mixed_set2 = obj2.get_set<Mixed>(col_mixed_set);
        auto string_set2 = obj2.get_set<StringData>(col_string_set);
        auto binary_set2 = obj2.get_set<BinaryData>(col_binary_set);
        for (auto& val : set_values) {
            mixed_set2.insert(val);
            if (val.is_type(type_String) || val.is_null()) {
                string_set2.insert(val.get<StringData>());
            }
            if (val.is_type(type_Binary) || val.is_null()) {
                binary_set2.insert(val.get<BinaryData>());
            }
        }

        CHECK_EQUAL(mixed_set1.size(), mixed_set2.size());
        CHECK_EQUAL(string_set1.size(), string_set2.size());
        CHECK_EQUAL(binary_set1.size(), binary_set2.size());
        for (size_t i = 0; i < set_values.size(); ++i) {
            CHECK_EQUAL(mixed_set1.get(i), mixed_set2.get(i));
            CHECK_NOT_EQUAL(mixed_set1.find(set_values[i]), realm::not_found);
            if (set_values[i].is_type(type_String) || set_values[i].is_null()) {
                CHECK_NOT_EQUAL(string_set1.find(set_values[i].get<StringData>()), realm::not_found);
            }
            if (set_values[i].is_type(type_Binary) || set_values[i].is_null()) {
                CHECK_NOT_EQUAL(binary_set1.find(set_values[i].get<BinaryData>()), realm::not_found);
            }
        }
        for (size_t i = 0; i < string_set1.size(); ++i) {
            CHECK_EQUAL(string_set1.get(i), string_set2.get(i));
        }
        for (size_t i = 0; i < binary_set1.size(); ++i) {
            CHECK_EQUAL(binary_set1.get(i), binary_set2.get(i));
        }
        for (size_t i = 1; i < mixed_set1.size(); ++i) {
            auto prev = mixed_set1.get_any(i - 1);
            auto cur = mixed_set1.get_any(i);
            CHECK_LESS(prev, cur);
        }
        for (size_t i = 1; i < string_set1.size(); ++i) {
            auto prev = string_set1.get_any(i - 1);
            auto cur = string_set1.get_any(i);
            CHECK_LESS(prev, cur);
        }
        for (size_t i = 1; i < binary_set1.size(); ++i) {
            auto prev = binary_set1.get_any(i - 1);
            auto cur = binary_set1.get_any(i);
            CHECK_LESS(prev, cur);
        }

        auto t2 = rt->get_table("table2");
        auto t2_col_id = t2->get_column_key("id");
        auto t2_mixed_col = t2->get_column_key("mixed");

        CHECK(t2->has_search_index(t2_col_id));
        CHECK(t2->has_search_index(t2_mixed_col));
        // add duplicates to the mixed
        std::vector<std::string> new_values = {base + "aaaa🎈", base + "aaaa🧵", base + "aaaa🧶", base + "aaaa🪢"};
        for (auto& val : new_values) {
            t2->create_object_with_primary_key(val).set(t2_mixed_col, Mixed{val});
        }
        set_values.insert(set_values.end(), new_values.begin(), new_values.end());
        size_t pk = 0;
        for (auto& val : set_values) {
            t2->create_object_with_primary_key(util::format("pk %1", ++pk)).set(t2_mixed_col, val);
        }

        for (auto& val : set_values) {
            if (val.is_type(type_String) || val.is_null()) {
                StringData str = val.get<StringData>();
                CHECK_EQUAL(t2->where().equal(t2_col_id, str).count(), 1);
                CHECK_EQUAL(t2->where().equal(t2_mixed_col, val).count(), 2);
                // Check that the search index finds us the right object
                auto k1 = t2->find_first(t2_col_id, str);
                CHECK_EQUAL(t2->get_object(k1).get<String>(t2_col_id), str);
                auto k2 = t2->find_first(t2_mixed_col, val);
                CHECK_EQUAL(t2->get_object(k2).get_any(t2_mixed_col), val);
                auto tv = t2->where().equal(t2_col_id, str).find_all();
                CHECK_EQUAL(tv.size(), 1);
                CHECK(k1 == tv.get_key(0));
                auto tv2 = t2->where().equal(t2_mixed_col, val).find_all();
                CHECK_EQUAL(tv2.size(), 2);
                CHECK(k2 == tv2.get_key(0) || k2 == tv2.get_key(1));
            }
        }
        t2->verify();
    }

#else
    // NOTE: This code must be executed from an old file-format-version 23
    // core in order to create a file-format-version 10 test file!

    // to test the migration of StringIndexes data must be persisted using signed char comparison
    static_assert(std::is_signed<char>(), "these files must be generated on a machine that uses signed chars");

    Group g;
    {
        TableRef t = g.add_table_with_primary_key("table", type_Int, "id", false);
        TableRef target = g.add_table_with_primary_key("target", type_Int, "id", false);
        auto col_link = t->add_column(*target, "link");

        auto target_key = target->create_object_with_primary_key(1).get_key();
        for (size_t i = 0; i < cnt; i++) {

            auto o = t->create_object_with_primary_key(int64_t(i));
            o.set(col_link, target_key);
        }
    }

    {
        TableRef t = g.add_table_with_primary_key("table_for_mixed", type_Int, "id", true);
        auto col_mixed_set = t->add_column_set(type_Mixed, "mixedSet", true);
        auto col_string_set = t->add_column_set(type_String, "stringSet", true);
        auto col_binary_set = t->add_column_set(type_Binary, "binarySet", true);
        Obj obj = t->create_object_with_primary_key(1);
        auto mixed_set = obj.get_set<Mixed>(col_mixed_set);
        auto string_set = obj.get_set<StringData>(col_string_set);
        auto binary_set = obj.get_set<BinaryData>(col_binary_set);
        for (auto& val : set_values) {
            mixed_set.insert(val);
            if (val.is_type(type_String) || val.is_null()) {
                string_set.insert(val.get<StringData>());
            }
            if (val.is_type(type_Binary) || val.is_null()) {
                binary_set.insert(val.get<BinaryData>());
            }
        }

        TableRef t2 = g.add_table_with_primary_key("table2", type_String, "id", true);
        auto col_mixed = t2->add_column(type_Mixed, "mixed");
        t2->add_search_index(col_mixed);
        for (auto& val : set_values) {
            if (val.is_type(type_String) || val.is_null()) {
                auto obj = t2->create_object_with_primary_key(val.get<StringData>());
                obj.set<Mixed>(col_mixed, val);
            }
        }
    }
    g.write(path);
#endif // TEST_READ_UPGRADE_MODE
}
#endif // TEST_GROUP
