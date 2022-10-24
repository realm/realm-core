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
#ifdef TEST_GROUP

#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#include <sys/types.h>
#endif

// File permissions for Windows
// http://stackoverflow.com/questions/592448/c-how-to-set-file-permissions-cross-platform
#ifdef _WIN32
#include <io.h>
typedef int mode_t2;
static const mode_t2 S_IWUSR = mode_t2(_S_IWRITE);
static const mode_t2 MS_MODE_MASK = 0x0000ffff;
#endif

#include <realm.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
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


namespace {

template <class T>
void test_table_add_columns(T t)
{
    t->add_column(type_String, "first");
    t->add_column(type_Int, "second");
    t->add_column(type_Bool, "third");
    t->add_column(type_Int, "fourth");
}

template <class T>
void setup_table(T t)
{
    t->create_object().set_all("a", 1, true, int(Wed));
    t->create_object().set_all("b", 15, true, int(Wed));
    t->create_object().set_all("ccc", 10, true, int(Wed));
    t->create_object().set_all("dddd", 20, true, int(Wed));
}

} // Anonymous namespace


#if REALM_ENABLE_ENCRYPTION
TEST(Group_OpenUnencryptedFileWithKey_SinglePage)
{
    // This results in a file which is too small to be an encrypted file
    GROUP_TEST_PATH(path);
    Group().write(path);
    CHECK_THROW(Group(path, crypt_key(true)), InvalidDatabase);
}

TEST(Group_OpenUnencryptedFileWithKey_TwoPages)
{
    // This results in a file which is large enough to be an encrypted file,
    // but the first four bytes will be zero, making the encryption layer think
    // it hasn't been written to
    GROUP_TEST_PATH(path);
    {
        Group group;
        TableRef table = group.get_or_add_table("table");
        auto col = table->add_column(type_String, "str");
        table->create_object().set(col, std::string(page_size() - 100, '\1'));
        group.write(path);
    }

    CHECK_THROW(Group(path, crypt_key(true)), InvalidDatabase);
}

TEST(Group_OpenUnencryptedFileWithKey_ThreePages)
{
    // This creates a three-page file so that the footer's IV field is the first
    // non-zero field in an unencrypted streaming format file
    GROUP_TEST_PATH(path);
    {
        Group group;
        TableRef table = group.get_or_add_table("table");
        auto col = table->add_column(type_String, "str");
        std::string data(page_size() - 100, '\1');
        table->create_object().set<String>(col, data);
        table->create_object().set<String>(col, data);
        group.write(path);
    }

    CHECK_THROW(Group(path, crypt_key(true)), InvalidDatabase);
}
#endif // REALM_ENABLE_ENCRYPTION

#ifndef _WIN32
TEST(Group_Permissions)
{
    if (getuid() == 0) {
        std::cout << "Group_Permissions test skipped because you are running it as root\n\n";
        return;
    }
    if (test_util::test_dir_is_exfat()) {
        return;
    }

    GROUP_TEST_PATH(path);
    {
        Group group1;
        TableRef t1 = group1.add_table("table1");
        t1->add_column(type_String, "s");
        t1->add_column(type_Int, "i");
        for (size_t i = 0; i < 4; ++i) {
            t1->create_object().set_all("a", 3);
        }
        group1.write(path, crypt_key());
    }

#ifdef _WIN32
    _chmod(path.c_str(), S_IWUSR & MS_MODE_MASK);
#else
    chmod(path.c_str(), S_IWUSR);
#endif

    CHECK_THROW(Group(path, crypt_key()), File::PermissionDenied);
}
#endif

TEST(Group_BadFile)
{
    GROUP_TEST_PATH(path_1);

    {
        File file(path_1, File::mode_Append);
        file.write("foo");
    }

    {
        auto group_open = [&] {
            Group group(path_1, crypt_key());
        };

        CHECK_THROW(group_open(), InvalidDatabase);
        CHECK_THROW(group_open(), InvalidDatabase); // Again
    }
}


TEST(Group_OpenBuffer)
{
    // Produce a valid buffer
    std::unique_ptr<char[]> buffer;
    size_t buffer_size;
    {
        GROUP_TEST_PATH(path);
        {
            Group group;
            group.write(path);
        }
        {
            File file(path);
            buffer_size = size_t(file.get_size());
            buffer.reset(new char[buffer_size]);
            CHECK(bool(buffer));
            file.read(buffer.get(), buffer_size);
        }
    }

    // Keep ownership of buffer
    {
        bool take_ownership = false;
        Group group(BinaryData(buffer.get(), buffer_size), take_ownership);
        CHECK(group.is_attached());
    }

    // Pass ownership of buffer
    {
        bool take_ownership = true;
        Group group(BinaryData(buffer.get(), buffer_size), take_ownership);
        CHECK(group.is_attached());
        buffer.release();
    }
}


TEST(Group_Size)
{
    Group group;
    CHECK(group.is_attached());
    CHECK(group.is_empty());

    group.add_table("a");
    CHECK_NOT(group.is_empty());
    CHECK_EQUAL(1, group.size());

    group.add_table("b");
    CHECK_NOT(group.is_empty());
    CHECK_EQUAL(2, group.size());
}


TEST(Group_AddTable)
{
    Group group;
    group.add_table("a");
    group.add_table("b");
    CHECK_EQUAL(2, group.size());
    CHECK_THROW(group.add_table("b"), TableNameInUse);
    CHECK_EQUAL(2, group.size());
}


TEST(Group_AddTableWithLinks)
{
    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    auto c0 = a->add_column(type_Int, "foo");
    auto c1 = b->add_column(*a, "bar");

    auto a_key = a->get_key();
    CHECK_EQUAL(b->get_opposite_table_key(c1), a_key);
    CHECK_EQUAL(a->get_opposite_table_key(c0), TableKey());

    group.add_table("c");

    CHECK_EQUAL(b->get_opposite_table_key(c1), a_key);
    CHECK_EQUAL(a->get_opposite_table_key(c0), TableKey());
}


TEST(Group_TableNameTooLong)
{
    Group group;
    size_t buf_len = 64;
    std::unique_ptr<char[]> buf(new char[buf_len]);
    CHECK_LOGIC_ERROR(group.add_table(StringData(buf.get(), buf_len)), LogicError::table_name_too_long);
    group.add_table(StringData(buf.get(), buf_len - 1));
}


TEST(Group_TableKey)
{
    Group group;
    TableRef moja = group.add_table("moja");
    TableRef mbili = group.add_table("mbili");
    TableRef tatu = group.add_table("tatu");
    CHECK_EQUAL(3, group.size());
    CHECK_EQUAL(moja, group.get_table(moja->get_key()));
    CHECK_EQUAL(mbili, group.get_table(mbili->get_key()));
    CHECK_EQUAL(tatu, group.get_table(tatu->get_key()));
    CHECK_EQUAL("moja", group.get_table_name(moja->get_key()));
    CHECK_EQUAL("mbili", group.get_table_name(mbili->get_key()));
    CHECK_EQUAL("tatu", group.get_table_name(tatu->get_key()));
    CHECK_EQUAL(group.find_table("moja"), moja->get_key());
    CHECK_NOT(group.find_table("hello"));

    auto all_table_keys = group.get_table_keys();
    CHECK_EQUAL(all_table_keys.size(), 3);
    int cnt = 0;
    for (auto key : group.get_table_keys()) {
        CHECK_EQUAL(key, all_table_keys[cnt]);
        cnt++;
    }
    CHECK_EQUAL(cnt, 3);
}


TEST(Group_GetTable)
{
    Group group;
    const Group& cgroup = group;

    TableRef table_1 = group.add_table("table_1");
    TableRef table_2 = group.add_table("table_2");

    CHECK_NOT(group.get_table("foo"));
    CHECK_NOT(cgroup.get_table("foo"));
    CHECK_EQUAL(table_1, group.get_table("table_1"));
    CHECK_EQUAL(table_1, cgroup.get_table("table_1"));
    CHECK_EQUAL(table_2, group.get_table("table_2"));
    CHECK_EQUAL(table_2, cgroup.get_table("table_2"));
}


TEST(Group_GetOrAddTable)
{
    Group group;
    CHECK_EQUAL(0, group.size());
    group.get_or_add_table("a");
    CHECK_EQUAL(1, group.size());
    group.get_or_add_table("a");
    CHECK_EQUAL(1, group.size());

    bool was_created = false;
    group.get_or_add_table("foo", Table::Type::TopLevel, &was_created);
    CHECK(was_created);
    CHECK_EQUAL(2, group.size());
    group.get_or_add_table("foo", Table::Type::TopLevel, &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(2, group.size());
    group.get_or_add_table("bar", Table::Type::TopLevel, &was_created);
    CHECK(was_created);
    CHECK_EQUAL(3, group.size());
    group.get_or_add_table("baz", Table::Type::TopLevel, &was_created);
    CHECK(was_created);
    CHECK_EQUAL(4, group.size());
    group.get_or_add_table("bar", Table::Type::TopLevel, &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(4, group.size());
    group.get_or_add_table("baz", Table::Type::TopLevel, &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(4, group.size());
}


TEST(Group_GetOrAddTable2)
{
    Group group;
    bool was_inserted;
    group.get_or_add_table("foo", Table::Type::TopLevel, &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK(was_inserted);
    group.get_or_add_table("foo", Table::Type::TopLevel, &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK_NOT(was_inserted);
}


TEST(Group_BasicRemoveTable)
{
    Group group;
    TableRef alpha = group.add_table("alpha");
    TableRef beta = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    TableRef delta = group.add_table("delta");
    CHECK_EQUAL(4, group.size());
    group.verify();
    group.remove_table(gamma->get_key()); // By key
    CHECK_EQUAL(3, group.size());
    CHECK(alpha);
    CHECK(beta);
    CHECK_NOT(gamma);
    CHECK(delta);
    CHECK_EQUAL("alpha", group.get_table_name(alpha->get_key()));
    CHECK_EQUAL("beta", group.get_table_name(beta->get_key()));
    CHECK_EQUAL("delta", group.get_table_name(delta->get_key()));
    group.remove_table(alpha->get_key()); // By key
    CHECK_EQUAL(2, group.size());
    CHECK_NOT(alpha);
    CHECK(beta);
    CHECK_NOT(gamma);
    CHECK(delta);
    CHECK_EQUAL("beta", group.get_table_name(beta->get_key()));
    CHECK_EQUAL("delta", group.get_table_name(delta->get_key()));
    group.remove_table("delta"); // By name
    CHECK_EQUAL(1, group.size());
    CHECK_NOT(alpha);
    CHECK(beta);
    CHECK_NOT(gamma);
    CHECK_NOT(delta);
    CHECK_EQUAL("beta", group.get_table_name(beta->get_key()));
    CHECK_THROW(group.remove_table("epsilon"), NoSuchTable);
    group.verify();
}


TEST(Group_ObjUseAfterTableDetach)
{
    Obj obj;
    ColKey col;
    {
        Group group;
        TableRef alpha = group.add_table("alpha");
        col = alpha->add_column(type_Int, "first");
        obj = alpha->create_object();
        obj.set(col, 42);
        CHECK_EQUAL(obj.get<int64_t>(col), 42);
    }
    CHECK_THROW(obj.get<int64_t>(col), realm::InvalidTableRef);
}

TEST(Group_RemoveTableWithColumns)
{
    Group group;

    TableRef alpha = group.add_table("alpha");
    TableRef beta = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    TableRef delta = group.add_table("delta");
    TableRef epsilon = group.add_table("epsilon");
    CHECK_EQUAL(5, group.size());

    alpha->add_column(type_Int, "alpha-1");
    auto col_link = beta->add_column(*delta, "beta-1");
    gamma->add_column(*gamma, "gamma-1");
    delta->add_column(type_Int, "delta-1");
    epsilon->add_column(*delta, "epsilon-1");

    Obj obj = delta->create_object();
    ObjKey k = obj.get_key();
    beta->create_object().set<ObjKey>(col_link, k);
    auto view = obj.get_backlink_view(beta, col_link);
    CHECK_EQUAL(view.size(), 1);

    // Remove table with columns, but no link columns, and table is not a link
    // target.
    group.remove_table("alpha");
    CHECK_EQUAL(4, group.size());
    CHECK_NOT(alpha);
    CHECK(beta);
    CHECK(gamma);
    CHECK(delta);
    CHECK(epsilon);

    // Remove table with link column, and table is not a link target.
    group.remove_table("beta");
    CHECK_EQUAL(3, group.size());
    CHECK_NOT(beta);
    CHECK(gamma);
    CHECK(delta);
    CHECK(epsilon);
    view.sync_if_needed();
    CHECK_EQUAL(view.size(), 0);

    // Remove table with self-link column, and table is not a target of link
    // columns of other tables.
    group.remove_table("gamma");
    CHECK_EQUAL(2, group.size());
    CHECK_NOT(gamma);
    CHECK(delta);
    CHECK(epsilon);

    // Try, but fail to remove table which is a target of link columns of other
    // tables.
    CHECK_THROW(group.remove_table("delta"), CrossTableLinkTarget);
    CHECK_EQUAL(2, group.size());
    CHECK(delta);
    CHECK(epsilon);
}


TEST(Group_RemoveTableMovesTableWithLinksOver)
{
    // Create a scenario where a table is removed from the group, and the last
    // table in the group (which will be moved into the vacated slot) has both
    // link and backlink columns.

    Group group;
    TableRef first = group.add_table("alpha");
    TableRef second = group.add_table("beta");
    TableRef third = group.add_table("gamma");
    TableRef fourth = group.add_table("delta");

    auto one = first->add_column(*third, "one");

    auto two = third->add_column(*fourth, "two");
    auto three = third->add_column(*third, "three");

    auto four = fourth->add_column(*first, "four");
    auto five = fourth->add_column(*third, "five");

    std::vector<ObjKey> first_keys;
    std::vector<ObjKey> third_keys;
    std::vector<ObjKey> fourth_keys;
    first->create_objects(2, first_keys);
    third->create_objects(2, third_keys);
    fourth->create_object();
    fourth->create_object();
    fourth->create_objects(2, fourth_keys);
    first->get_object(first_keys[0]).set(one, third_keys[0]);    // first[0].one   = third[0]
    first->get_object(first_keys[1]).set(one, third_keys[1]);    // first[1].one   = third[1]
    third->get_object(third_keys[0]).set(two, fourth_keys[1]);   // third[0].two   = fourth[1]
    third->get_object(third_keys[1]).set(two, fourth_keys[0]);   // third[1].two   = fourth[0]
    third->get_object(third_keys[0]).set(three, third_keys[1]);  // third[0].three = third[1]
    third->get_object(third_keys[1]).set(three, third_keys[1]);  // third[1].three = third[1]
    fourth->get_object(fourth_keys[0]).set(four, first_keys[0]); // fourth[0].four = first[0]
    fourth->get_object(fourth_keys[1]).set(four, first_keys[0]); // fourth[1].four = first[0]
    fourth->get_object(fourth_keys[0]).set(five, third_keys[0]); // fourth[0].five = third[0]
    fourth->get_object(fourth_keys[1]).set(five, third_keys[1]); // fourth[1].five = third[1]

    group.verify();

    group.remove_table(second->get_key()); // Second

    group.verify();

    CHECK_EQUAL(3, group.size());
    CHECK(bool(first));
    CHECK_NOT(bool(second));
    CHECK(bool(third));
    CHECK(bool(fourth));
    CHECK_EQUAL(1, first->get_column_count());
    CHECK_EQUAL("one", first->get_column_name(one));
    CHECK_EQUAL(third, first->get_link_target(one));
    CHECK_EQUAL(2, third->get_column_count());
    CHECK_EQUAL("two", third->get_column_name(two));
    CHECK_EQUAL("three", third->get_column_name(three));
    CHECK_EQUAL(fourth, third->get_link_target(two));
    CHECK_EQUAL(third, third->get_link_target(three));
    CHECK_EQUAL(2, fourth->get_column_count());
    CHECK_EQUAL("four", fourth->get_column_name(four));
    CHECK_EQUAL("five", fourth->get_column_name(five));
    CHECK_EQUAL(first, fourth->get_link_target(four));
    CHECK_EQUAL(third, fourth->get_link_target(five));

    third->get_object(third_keys[0]).set(two, fourth_keys[0]);   // third[0].two   = fourth[0]
    fourth->get_object(fourth_keys[1]).set(four, first_keys[1]); // fourth[1].four = first[1]
    first->get_object(first_keys[0]).set(one, third_keys[1]);    // first[0].one   = third[1]

    group.verify();

    CHECK_EQUAL(2, first->size());
    CHECK_EQUAL(third_keys[1], first->get_object(first_keys[0]).get<ObjKey>(one));
    CHECK_EQUAL(third_keys[1], first->get_object(first_keys[1]).get<ObjKey>(one));
    CHECK_EQUAL(1, first->get_object(first_keys[0]).get_backlink_count(*fourth, four));
    CHECK_EQUAL(1, first->get_object(first_keys[1]).get_backlink_count(*fourth, four));

    CHECK_EQUAL(2, third->size());
    CHECK_EQUAL(fourth_keys[0], third->get_object(third_keys[0]).get<ObjKey>(two));
    CHECK_EQUAL(fourth_keys[0], third->get_object(third_keys[1]).get<ObjKey>(two));
    CHECK_EQUAL(third_keys[1], third->get_object(third_keys[0]).get<ObjKey>(three));
    CHECK_EQUAL(third_keys[1], third->get_object(third_keys[1]).get<ObjKey>(three));

    CHECK_EQUAL(0, third->get_object(third_keys[0]).get_backlink_count(*first, one));
    CHECK_EQUAL(2, third->get_object(third_keys[1]).get_backlink_count(*first, one));
    CHECK_EQUAL(0, third->get_object(third_keys[0]).get_backlink_count(*third, three));
    CHECK_EQUAL(2, third->get_object(third_keys[1]).get_backlink_count(*third, three));
    CHECK_EQUAL(1, third->get_object(third_keys[0]).get_backlink_count(*fourth, five));
    CHECK_EQUAL(1, third->get_object(third_keys[1]).get_backlink_count(*fourth, five));

    CHECK_EQUAL(4, fourth->size());
    CHECK_EQUAL(first_keys[0], fourth->get_object(fourth_keys[0]).get<ObjKey>(four));
    CHECK_EQUAL(first_keys[1], fourth->get_object(fourth_keys[1]).get<ObjKey>(four));
    CHECK_EQUAL(third_keys[0], fourth->get_object(fourth_keys[0]).get<ObjKey>(five));
    CHECK_EQUAL(third_keys[1], fourth->get_object(fourth_keys[1]).get<ObjKey>(five));

    CHECK_EQUAL(2, fourth->get_object(fourth_keys[0]).get_backlink_count(*third, two));
    CHECK_EQUAL(0, fourth->get_object(fourth_keys[1]).get_backlink_count(*third, two));
}


TEST(Group_RemoveLinkTable)
{
    Group group;
    TableRef table = group.add_table("table");
    table->add_column(*table, "link");
    group.remove_table(table->get_key());
    CHECK(group.is_empty());
    CHECK(!table);
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    target->add_column(type_Int, "int");
    origin->add_column(*target, "link");
    CHECK_THROW(group.remove_table(target->get_key()), CrossTableLinkTarget);
    group.remove_table(origin->get_key());
    CHECK_EQUAL(1, group.size());
    CHECK(!origin);
    CHECK(target);
    group.verify();
}


TEST(Group_RenameTable)
{
    Group group;
    TableRef alpha = group.add_table("alpha");
    TableRef beta = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    group.rename_table(beta->get_key(), "delta");
    CHECK_EQUAL("delta", beta->get_name());
    group.rename_table("delta", "epsilon");
    CHECK_EQUAL("alpha", alpha->get_name());
    CHECK_EQUAL("epsilon", beta->get_name());
    CHECK_EQUAL("gamma", gamma->get_name());
    CHECK_THROW(group.rename_table("eta", "theta"), NoSuchTable);
    CHECK_THROW(group.rename_table("epsilon", "alpha"), TableNameInUse);
    bool require_unique_name = false;
    group.rename_table("epsilon", "alpha", require_unique_name);
    CHECK_EQUAL("alpha", alpha->get_name());
    CHECK_EQUAL("alpha", beta->get_name());
    CHECK_EQUAL("gamma", gamma->get_name());
    group.verify();
}


TEST(Group_Equal)
{
    Group g1, g2, g3;
    CHECK(g1 == g2);
    auto t1 = g1.add_table("TABLE1");
    test_table_add_columns(t1);
    CHECK_NOT(g1 == g2);
    setup_table(t1);
    auto t2 = g2.add_table("TABLE1");
    test_table_add_columns(t2);
    setup_table(t2);
    CHECK(g1 == g2);
    t2->create_object().set_all("hey", 2, false, int(Thu));
    CHECK(g1 != g2); // table size differ
    auto t3 = g3.add_table("TABLE3");
    test_table_add_columns(t3);
    setup_table(t3);
    CHECK(g1 != g3); // table names differ
}

TEST(Group_Invalid1)
{
    GROUP_TEST_PATH(path);

    // Try to open non-existing file
    // (read-only files have to exists to before opening)
    CHECK_THROW(Group(path, crypt_key()), File::NotFound);
}

TEST(Group_Invalid2)
{
    // Try to open buffer with invalid data
    const char* const str = "invalid data";
    CHECK_THROW(Group(BinaryData(str, strlen(str))), InvalidDatabase);
}

TEST(Group_Overwrite)
{
    GROUP_TEST_PATH(path);
    {
        Group g;
        g.write(path, crypt_key());
        CHECK_THROW(g.write(path, crypt_key()), File::Exists);
    }
    {
        Group g(path, crypt_key());
        CHECK_THROW(g.write(path, crypt_key()), File::Exists);
    }
    {
        Group g;
        File::try_remove(path);
        g.write(path, crypt_key());
    }
}


TEST(Group_Serialize0)
{
    GROUP_TEST_PATH(path);
    {
        // Create empty group and serialize to disk
        Group to_disk;
        to_disk.write(path, crypt_key());

        // Load the group
        Group from_disk(path, crypt_key());

        // Create new table in group
        auto t = from_disk.add_table("test");
        test_table_add_columns(t);
        auto cols = t->get_column_keys();

        CHECK_EQUAL(4, t->get_column_count());
        CHECK_EQUAL(0, t->size());

        // Modify table
        Obj obj = t->create_object();
        obj.set_all("Test", 1, true, int(Wed));

        CHECK_EQUAL("Test", obj.get<String>(cols[0]));
        CHECK_EQUAL(1, obj.get<Int>(cols[1]));
        CHECK_EQUAL(true, obj.get<Bool>(cols[2]));
        CHECK_EQUAL(Wed, obj.get<Int>(cols[3]));
    }
    {
        // Load the group and let it clean up without loading
        // any tables
        Group g(path, crypt_key());
    }
}


TEST(Group_Serialize1)
{
    GROUP_TEST_PATH(path);
    {
        // Create group with one table
        Group to_disk;
        auto table = to_disk.add_table("test");
        test_table_add_columns(table);
        table->create_object(ObjKey(0)).set_all("", 1, true, int(Wed));
        table->create_object(ObjKey(1)).set_all("", 15, true, int(Wed));
        table->create_object(ObjKey(2)).set_all("", 10, true, int(Wed));
        table->create_object(ObjKey(3)).set_all("", 20, true, int(Wed));
        table->create_object(ObjKey(4)).set_all("", 11, true, int(Wed));

        table->create_object(ObjKey(6)).set_all("", 45, true, int(Wed));
        table->create_object(ObjKey(7)).set_all("", 10, true, int(Wed));
        table->create_object(ObjKey(8)).set_all("", 0, true, int(Wed));
        table->create_object(ObjKey(9)).set_all("", 30, true, int(Wed));
        table->create_object(ObjKey(10)).set_all("", 9, true, int(Wed));

#ifdef REALM_DEBUG
        to_disk.verify();
#endif

        // Serialize to disk
        to_disk.write(path, crypt_key());

        // Load the table
        Group from_disk(path, crypt_key());
        auto t = from_disk.get_table("test");

        CHECK_EQUAL(4, t->get_column_count());
        CHECK_EQUAL(10, t->size());
        auto cols = t->get_column_keys();
        // Verify that original values are there
        CHECK(*table == *t);

        // Modify both tables
        table->get_object(ObjKey(0)).set(cols[0], "test");
        t->get_object(ObjKey(0)).set(cols[0], "test");

        table->create_object(ObjKey(5)).set_all("hello", 100, false, int(Mon));
        t->create_object(ObjKey(5)).set_all("hello", 100, false, int(Mon));
        table->remove_object(ObjKey(1));
        t->remove_object(ObjKey(1));

        // Verify that both changed correctly
        CHECK(*table == *t);
#ifdef REALM_DEBUG
        to_disk.verify();
        from_disk.verify();
#endif
    }
    {
        // Load the group and let it clean up without loading
        // any tables
        Group g(path, crypt_key());
    }
}


TEST(Group_Serialize2)
{
    GROUP_TEST_PATH(path);

    // Create group with two tables
    Group to_disk;
    TableRef table1 = to_disk.add_table("test1");
    test_table_add_columns(table1);
    table1->create_object().set_all("", 1, true, int(Wed));
    table1->create_object().set_all("", 15, true, int(Wed));
    table1->create_object().set_all("", 10, true, int(Wed));

    TableRef table2 = to_disk.add_table("test2");
    test_table_add_columns(table2);
    table2->create_object().set_all("hey", 0, true, int(Tue));
    table2->create_object().set_all("hello", 3232, false, int(Sun));

#ifdef REALM_DEBUG
    to_disk.verify();
#endif

    // Serialize to disk
    to_disk.write(path, crypt_key());

    // Load the tables
    Group from_disk(path, crypt_key());
    TableRef t1 = from_disk.get_table("test1");
    TableRef t2 = from_disk.get_table("test2");

    // Verify that original values are there
    CHECK(*table1 == *t1);
    CHECK(*table2 == *t2);

#ifdef REALM_DEBUG
    to_disk.verify();
    from_disk.verify();
#endif
}


TEST(Group_Serialize3)
{
    GROUP_TEST_PATH(path);

    // Create group with one table (including long strings
    Group to_disk;
    TableRef table = to_disk.add_table("test");
    test_table_add_columns(table);
    table->create_object().set_all("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1", 1,
                                   true, int(Wed));
    table->create_object().set_all("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15,
                                   true, int(Wed));

#ifdef REALM_DEBUG
    to_disk.verify();
#endif

    // Serialize to disk
    to_disk.write(path, crypt_key());

    // Load the table
    Group from_disk(path, crypt_key());
    TableRef t = from_disk.get_table("test");

    // Verify that original values are there
    CHECK(*table == *t);
#ifdef REALM_DEBUG
    to_disk.verify();
    from_disk.verify();
#endif
}


TEST(Group_Serialize_Mem)
{
    // Create group with one table
    Group to_mem;
    TableRef table = to_mem.add_table("test");
    test_table_add_columns(table);
    table->create_object().set_all("", 1, true, int(Wed));
    table->create_object().set_all("", 15, true, int(Wed));
    table->create_object().set_all("", 10, true, int(Wed));
    table->create_object().set_all("", 20, true, int(Wed));
    table->create_object().set_all("", 11, true, int(Wed));
    table->create_object().set_all("", 45, true, int(Wed));
    table->create_object().set_all("", 10, true, int(Wed));
    table->create_object().set_all("", 0, true, int(Wed));
    table->create_object().set_all("", 30, true, int(Wed));
    table->create_object().set_all("", 9, true, int(Wed));

#ifdef REALM_DEBUG
    to_mem.verify();
#endif

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(10, t->size());

    // Verify that original values are there
    CHECK(*table == *t);
#ifdef REALM_DEBUG
    to_mem.verify();
    from_mem.verify();
#endif
}


TEST(Group_Close)
{
    Group to_mem;
    TableRef table = to_mem.add_table("test");
    test_table_add_columns(table);
    table->create_object().set_all("", 1, true, int(Wed));
    table->create_object().set_all("", 2, true, int(Wed));

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    Group from_mem(buffer);
}

TEST(Group_Serialize_Optimized)
{
    // Create group with one table
    Group to_mem;
    TableRef table = to_mem.add_table("test");
    test_table_add_columns(table);

    for (size_t i = 0; i < 5; ++i) {
        table->create_object().set_all("abd", 1, true, int(Mon));
        table->create_object().set_all("eftg", 2, true, int(Tue));
        table->create_object().set_all("hijkl", 5, true, int(Wed));
        table->create_object().set_all("mnopqr", 8, true, int(Thu));
        table->create_object().set_all("stuvxyz", 9, true, int(Fri));
    }

    ColKey col_string = table->get_column_keys()[0];
    table->enumerate_string_column(col_string);

#ifdef REALM_DEBUG
    to_mem.verify();
#endif

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");

    CHECK_EQUAL(4, t->get_column_count());

    // Verify that original values are there
    CHECK(*table == *t);

    // Add a row with a known (but unique) value
    auto k = table->create_object().set_all("search_target", 9, true, int(Fri)).get_key();

    const auto res = table->find_first_string(col_string, "search_target");
    CHECK_EQUAL(k, res);

#ifdef REALM_DEBUG
    to_mem.verify();
    from_mem.verify();
#endif
}


TEST(Group_Serialize_All)
{
    // Create group with one table
    Group to_mem;
    TableRef table = to_mem.add_table("test");

    table->add_column(type_Int, "int");
    table->add_column(type_Bool, "bool");
    table->add_column(type_Timestamp, "date");
    table->add_column(type_String, "string");
    table->add_column(type_Binary, "binary");

    table->create_object(ObjKey(0)).set_all(12, true, Timestamp{12345, 0}, "test", BinaryData("binary", 7));

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");

    CHECK_EQUAL(5, t->get_column_count());
    CHECK_EQUAL(1, t->size());
    auto cols = t->get_column_keys();
    Obj obj = t->get_object(ObjKey(0));
    CHECK_EQUAL(12, obj.get<Int>(cols[0]));
    CHECK_EQUAL(true, obj.get<Bool>(cols[1]));
    CHECK(obj.get<Timestamp>(cols[2]) == Timestamp(12345, 0));
    CHECK_EQUAL("test", obj.get<String>(cols[3]));
    CHECK_EQUAL(7, obj.get<Binary>(cols[4]).size());
    CHECK_EQUAL("binary", obj.get<Binary>(cols[4]).data());
}


TEST(Group_ToJSON)
{
    Group g;
    TableRef table = g.add_table("test");
    test_table_add_columns(table);

    table->create_object().set_all("plain string", 1, true, int(Wed));
    // Test that control characters are correctly escaped
    table->create_object().set_all("\"key\":\t123,\n\"value\":\tjim", 1, true, int(Wed));
    std::ostringstream out;
    g.to_json(out);
    std::string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("{\n\"test\":"
                "["
                "{\"_key\":0,\"first\":\"plain string\",\"second\":1,\"third\":true,\"fourth\":2},"
                "{\"_key\":1,\"first\":\"\\\"key\\\":\\t123,\\n\\\"value\\\":\\tjim\",\"second\":1,\"third\":true,"
                "\"fourth\":2}"
                "]\n"
                "}\n",
                str);
}

TEST(Group_IndexString)
{
    Group to_mem;
    TableRef table = to_mem.add_table("test");
    test_table_add_columns(table);

    auto k0 = table->create_object().set_all("jeff", 1, true, int(Wed)).get_key();
    auto k1 = table->create_object().set_all("jim", 1, true, int(Wed)).get_key();
    table->create_object().set_all("jennifer", 1, true, int(Wed));
    table->create_object().set_all("john", 1, true, int(Wed));
    table->create_object().set_all("jimmy", 1, true, int(Wed));
    auto k5 = table->create_object().set_all("jimbo", 1, true, int(Wed)).get_key();
    auto k6 = table->create_object().set_all("johnny", 1, true, int(Wed)).get_key();
    table->create_object().set_all("jennifer", 1, true, int(Wed)); // duplicate

    ColKey col_string = table->get_column_key("first");
    table->add_search_index(col_string);
    CHECK(table->has_search_index(col_string));

    auto r1 = table->find_first_string(col_string, "jimmi");
    CHECK_EQUAL(null_key, r1);

    auto r2 = table->find_first_string(col_string, "jeff");
    auto r3 = table->find_first_string(col_string, "jim");
    auto r4 = table->find_first_string(col_string, "jimbo");
    auto r5 = table->find_first_string(col_string, "johnny");
    CHECK_EQUAL(k0, r2);
    CHECK_EQUAL(k1, r3);
    CHECK_EQUAL(k5, r4);
    CHECK_EQUAL(k6, r5);

    size_t c1 = table->count_string(col_string, "jennifer");
    CHECK_EQUAL(2, c1);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");
    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(8, t->size());

    col_string = table->get_column_key("first");
    CHECK(t->has_search_index(col_string));

    auto m1 = t->find_first_string(col_string, "jimmi");
    CHECK_EQUAL(null_key, m1);

    auto m2 = t->find_first_string(col_string, "jeff");
    auto m3 = t->find_first_string(col_string, "jim");
    auto m4 = t->find_first_string(col_string, "jimbo");
    auto m5 = t->find_first_string(col_string, "johnny");
    CHECK_EQUAL(k0, m2);
    CHECK_EQUAL(k1, m3);
    CHECK_EQUAL(k5, m4);
    CHECK_EQUAL(k6, m5);

    size_t m6 = t->count_string(col_string, "jennifer");
    CHECK_EQUAL(2, m6);

    // Remove the search index and verify
    t->remove_search_index(col_string);
    CHECK(!t->has_search_index(col_string));
    from_mem.verify();

    auto m7 = t->find_first_string(col_string, "jimmi");
    auto m8 = t->find_first_string(col_string, "johnny");
    CHECK_EQUAL(null_key, m7);
    CHECK_EQUAL(k6, m8);
}

TEST(Group_CascadeNotify_SimpleWeak)
{
    Group g;
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column(*t, "link");
    auto col_link_list = origin->add_column_list(*t, "linklist");

    std::vector<ObjKey> t_keys;
    t->create_objects(100, t_keys);

    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification&) {
        called = true;
    });
    t->remove_object(t_keys[5]);
    t_keys.erase(t_keys.begin() + 5);
    CHECK(called);

    // remove_object() on a table with no (back)links just sends that single
    // row in the notification
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(t->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(t_keys[5], notification.rows[0].key);
    });
    t->remove_object(t_keys[5]);
    t_keys.erase(t_keys.begin() + 5);
    CHECK(called);

    std::vector<ObjKey> o_keys;
    origin->create_objects(100, o_keys);

    // remove_object() on an un-linked-to row should still just send that row
    // in the notification
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(t->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(t_keys[5], notification.rows[0].key);
    });
    t->remove_object(t_keys[5]);
    t_keys.erase(t_keys.begin() + 5);
    CHECK(called);

    // remove_object() on a linked-to row should send information about the
    // links which had linked to it
    // rows are arbitrarily different to make things less likely to pass by coincidence
    Obj obj10 = origin->get_object(o_keys[10]);
    Obj obj15 = origin->get_object(o_keys[15]);
    obj10.set(col_link, t_keys[11]);
    LnkLstPtr lv = obj15.get_linklist_ptr(col_link_list);
    lv->add(t_keys[11]);
    lv->add(t_keys[30]);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(t->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(t_keys[11], notification.rows[0].key);

        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(col_link, notification.links[0].origin_col_key);
        CHECK_EQUAL(o_keys[10], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[11], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_key);
        CHECK_EQUAL(o_keys[15], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[11], notification.links[1].old_target_key);
    });
    t->remove_object(t_keys[11]);
    t_keys.erase(t_keys.begin() + 11);
    CHECK(called);

    // remove_object() on the origin table just sends the row being removed
    // because the links are weak
    obj10.set(col_link, t_keys[11]);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(origin->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(o_keys[10], notification.rows[0].key);

        CHECK_EQUAL(0, notification.links.size());
    });
    origin->remove_object(o_keys[10]);
    o_keys.erase(o_keys.begin() + 10);
    CHECK(called);
}


TEST(Group_CascadeNotify_TableClearWeak)
{
    Group g;
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column(*t, "link");
    auto col_link_list = origin->add_column_list(*t, "linklist");

    std::vector<ObjKey> t_keys, o_keys;
    t->create_objects(10, t_keys);

    // clear() does not list the rows in the table being cleared because it
    // would be expensive and mostly pointless to do so
    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(0, notification.rows.size());
    });
    t->clear();
    t_keys.clear();
    CHECK(called);

    origin->create_objects(10, o_keys);
    t->create_objects(10, t_keys);

    // clear() does report nullified links
    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);
    origin->get_object(o_keys[5]).get_linklist(col_link_list).add(t_keys[4]);

    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.rows.size());

        CHECK_EQUAL(3, notification.links.size());
        CHECK_EQUAL(col_link, notification.links[0].origin_col_key);
        CHECK_EQUAL(o_keys[1], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[2], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_key);
        CHECK_EQUAL(o_keys[3], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[4], notification.links[1].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[2].origin_col_key);
        CHECK_EQUAL(o_keys[5], notification.links[2].origin_key);
        CHECK_EQUAL(t_keys[4], notification.links[2].old_target_key);
    });
    t->clear();
    t_keys.clear();
    CHECK(called);
    g.verify();
}


TEST(Group_CascadeNotify_TableViewClearWeak)
{
    Group g;
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column(*t, "link");
    auto col_link_list = origin->add_column_list(*t, "linklist");

    std::vector<ObjKey> t_keys, o_keys;
    t->create_objects(10, t_keys);

    // Unlike clearing a table, the rows removed by the clear() are included in
    // the notification so that cascaded deletions and direct deletions don't
    // need to be handled separately
    int calls = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        calls++;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(10, notification.rows.size());
    });
    t->where().find_all().clear();
    t_keys.clear();
    CHECK_EQUAL(calls, 1);

    origin->create_objects(10, o_keys);
    t->create_objects(10, t_keys);

    // should list which links were nullified
    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);

    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        calls++;
        CHECK_EQUAL(10, notification.rows.size());
        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(col_link, notification.links[0].origin_col_key);
        CHECK_EQUAL(o_keys[1], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[2], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_key);
        CHECK_EQUAL(o_keys[3], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[4], notification.links[1].old_target_key);
    });
    t->where().find_all().clear();
    t_keys.clear();
    CHECK_EQUAL(calls, 2);
}


// more levels of cascade delete.... this does not seem to add any additional coverage
static void make_tree(Table& table, Obj& obj, ColKey left, ColKey right, int depth)
{
    if (depth < 4) {
        auto o_l = obj.create_and_set_linked_object(left);
        auto o_r = obj.create_and_set_linked_object(right);
        make_tree(table, o_l, left, right, depth + 1);
        make_tree(table, o_r, left, right, depth + 1);
    }
}

TEST(Group_CascadeNotify_TreeCascade)
{
    Group g;
    TableRef t = g.add_table("table", Table::Type::Embedded);
    TableRef parent = g.add_table("parent");
    auto left = t->add_column(*t, "left");
    auto right = t->add_column(*t, "right");
    auto col = parent->add_column(*t, "root");
    auto outer_root = parent->create_object();
    auto root = outer_root.create_and_set_linked_object(col);
    make_tree(*t, root, left, right, 0);
    CHECK_EQUAL(t->size(), 31);

    int calls = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        // Each notification reports removing one more level of the tree, so
        // number of rows and links nullfied doubles each time
        CHECK_EQUAL(notification.rows.size(), 1 << calls);
        CHECK_EQUAL(notification.links.size(), 0);
        CHECK_EQUAL(t->size(), 32 - (1 << calls));
        calls++;
    });
    parent->clear();
    CHECK_EQUAL(calls, 5);
    CHECK_EQUAL(t->size(), 0);
}


TEST(Group_ChangeEmbeddedness)
{
    Group g;
    TableRef t = g.add_table("table");
    TableRef parent = g.add_table("parent");
    auto col = parent->add_column(*t, "child");
    auto p1 = parent->create_object();
    auto p2 = parent->create_object();
    auto p3 = parent->create_object();
    auto obj1 = t->create_object();
    auto obj2 = t->create_object();
    auto obj3 = t->create_object();
    p1.set(col, obj1.get_key());
    p2.set(col, obj2.get_key());

    // obj3 has no owner, so we can't make the table embedded
    CHECK_THROW_CONTAINING_MESSAGE(
        t->set_table_type(Table::Type::Embedded),
        "Cannot convert 'table' to embedded: at least one object has no incoming links and would be deleted.");
    CHECK_NOT(t->is_embedded());

    // Now it has owner
    p3.set(col, obj3.get_key());
    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());

    CHECK_NOTHROW(t->set_table_type(Table::Type::TopLevel));
    p3.set(col, obj2.get_key());
    obj3.remove();

    // Now obj2 has 2 parents
    CHECK_EQUAL(obj2.get_backlink_count(), 2);
    CHECK_THROW_CONTAINING_MESSAGE(
        t->set_table_type(Table::Type::Embedded),
        "Cannot convert 'table' to embedded: at least one object has more than one incoming link.");
    CHECK_NOT(t->is_embedded());
}

TEST(Group_MakeEmbedded_PrimaryKey)
{
    Group g;
    TableRef t = g.add_table_with_primary_key("child", type_Int, "_id");
    CHECK_THROW_CONTAINING_MESSAGE(t->set_table_type(Table::Type::Embedded),
                                   "Cannot change 'child' to embedded when using a primary key.");
    CHECK_NOT(t->is_embedded());
}

TEST(Group_MakeEmbedded_NoIncomingLinks_Empty)
{
    Group g;
    TableRef t = g.add_table("child");
    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 0);
}

TEST(Group_MakeEmbedded_NoIncomingLinks_Nonempty)
{
    Group g;
    TableRef t = g.add_table("child");
    auto child_obj = t->create_object();
    CHECK_THROW_CONTAINING_MESSAGE(
        t->set_table_type(Table::Type::Embedded),
        "Cannot convert 'child' to embedded: at least one object has no incoming links and would be deleted.");
    CHECK_NOT(t->is_embedded());

    // Add an incoming link and it should now work
    TableRef parent = g.add_table("parent");
    parent->add_column(*t, "child");
    parent->create_object().set_all(child_obj.get_key());

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_MultipleIncomingLinks_OneProperty)
{
    Group g;
    TableRef t = g.add_table("child");
    TableRef parent = g.add_table("parent");

    auto child_obj = t->create_object();

    parent->add_column(*t, "child");
    parent->create_object().set_all(child_obj.get_key());
    parent->create_object().set_all(child_obj.get_key());

    CHECK_THROW_CONTAINING_MESSAGE(
        t->set_table_type(Table::Type::Embedded),
        "Cannot convert 'child' to embedded: at least one object has more than one incoming link.");
    CHECK_NOT(t->is_embedded());

    // Should work after deleting one of the incoming links
    parent->begin()->remove();

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_MultipleIncomingLinks_MultipleProperties)
{
    Group g;
    TableRef t = g.add_table("child");
    TableRef parent = g.add_table("parent");

    auto child_obj = t->create_object();

    parent->add_column(*t, "link 1");
    parent->add_column(*t, "link 2");
    parent->create_object().set_all(child_obj.get_key(), child_obj.get_key());

    CHECK_THROW_CONTAINING_MESSAGE(
        t->set_table_type(Table::Type::Embedded),
        "Cannot convert 'child' to embedded: at least one object has more than one incoming link.");
    CHECK_NOT(t->is_embedded());

    // Should work after deleting one of the incoming links
    parent->begin()->set_all(ObjKey());

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_IncomingMixed_Single)
{
    Group g;
    TableRef t = g.add_table("child");
    auto child_obj = t->create_object();

    TableRef parent = g.add_table("parent");
    parent->add_column(type_Mixed, "mixed");
    parent->add_column(*t, "link");
    parent->create_object().set_all(Mixed(child_obj.get_link()), child_obj.get_key());

    CHECK_THROW_CONTAINING_MESSAGE(t->set_table_type(Table::Type::Embedded),
                                   "Cannot convert 'child' to embedded: there is an incoming link from the Mixed "
                                   "property 'parent.mixed', which does not support linking to embedded objects.");
    CHECK_NOT(t->is_embedded());

    // Should work after removing the Mixed link
    parent->begin()->set_all(Mixed());

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_IncomingMixed_List)
{
    Group g;
    TableRef t = g.add_table("child");
    auto child_obj = t->create_object();

    TableRef parent = g.add_table("parent");
    parent->add_column(*t, "link");
    auto col = parent->add_column_list(type_Mixed, "mixed");
    auto obj = parent->create_object().set_all(child_obj.get_key());
    obj.get_list<Mixed>(col).add(child_obj.get_link());

    CHECK_THROW_CONTAINING_MESSAGE(t->set_table_type(Table::Type::Embedded),
                                   "Cannot convert 'child' to embedded: there is an incoming link from the Mixed "
                                   "property 'parent.mixed', which does not support linking to embedded objects.");
    CHECK_NOT(t->is_embedded());

    // Should work after removing the Mixed link
    obj.get_list<Mixed>(col).clear();

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_IncomingMixed_Set)
{
    Group g;
    TableRef t = g.add_table("child");
    auto child_obj = t->create_object();

    TableRef parent = g.add_table("parent");
    parent->add_column(*t, "link");
    auto col = parent->add_column_set(type_Mixed, "mixed");
    auto obj = parent->create_object().set_all(child_obj.get_key());
    obj.get_set<Mixed>(col).insert(child_obj.get_link());

    CHECK_THROW_CONTAINING_MESSAGE(t->set_table_type(Table::Type::Embedded),
                                   "Cannot convert 'child' to embedded: there is an incoming link from the Mixed "
                                   "property 'parent.mixed', which does not support linking to embedded objects.");
    CHECK_NOT(t->is_embedded());

    // Should work after removing the Mixed link
    obj.get_set<Mixed>(col).clear();

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_IncomingMixed_Dictionary)
{
    Group g;
    TableRef t = g.add_table("child");
    auto child_obj = t->create_object();

    TableRef parent = g.add_table("parent");
    parent->add_column(*t, "link");
    auto col = parent->add_column_dictionary(type_Mixed, "mixed");
    auto obj = parent->create_object().set_all(child_obj.get_key());
    obj.get_dictionary(col).insert("foo", child_obj.get_link());

    CHECK_THROW_CONTAINING_MESSAGE(t->set_table_type(Table::Type::Embedded),
                                   "Cannot convert 'child' to embedded: there is an incoming link from the Mixed "
                                   "property 'parent.mixed', which does not support linking to embedded objects.");
    CHECK_NOT(t->is_embedded());

    // Should work after removing the Mixed link
    obj.get_dictionary(col).clear();

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded));
    CHECK(t->is_embedded());
    CHECK(t->size() == 1);
}

TEST(Group_MakeEmbedded_DeleteOrphans)
{
    Group g;
    TableRef t = g.add_table("child");
    auto col = t->add_column(type_Int, "value");
    std::vector<ObjKey> children;
    t->create_objects(10000, children);

    TableRef parent = g.add_table("parent");
    parent->add_column(*t, "link");
    for (int i = 0; i < 100; ++i) {
        t->get_object(children[i * 100]).set_all(i + 1);
        parent->create_object().set_all(children[i * 100]);
    }

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));
    CHECK(t->is_embedded());
    CHECK(t->size() == 100);
    for (int i = 0; i < 100; ++i) {
        Obj obj;
        if (CHECK_NOTHROW(obj = t->get_object(children[i * 100]))) {
            CHECK_EQUAL(obj.get<int64_t>(col), i + 1);
        }
    }
}

TEST(Group_MakeEmbedded_DuplicateObjectsForMultipleParentObjects)
{
    Group g;
    TableRef t = g.add_table("child");
    auto col = t->add_column(type_Int, "value");
    auto child = t->create_object().set_all(10);

    TableRef parent = g.add_table("parent");
    auto link_col = parent->add_column(*t, "link");
    for (int i = 0; i < 10000; ++i) {
        parent->create_object().set_all(child.get_key());
    }

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));
    CHECK(t->is_embedded());
    CHECK(t->size() == 10000);
    CHECK(parent->size() == 10000);
    CHECK_NOT(child.is_valid()); // original object should have been deleted

    for (auto parent_obj : *parent) {
        CHECK_EQUAL(t->get_object(parent_obj.get<ObjKey>(link_col)).get<int64_t>(col), 10);
    }
}

TEST(Group_MakeEmbedded_DuplicateObjectsForMultipleColumns)
{
    Group g;
    TableRef t = g.add_table("child");
    auto col = t->add_column(type_Int, "value");
    auto child = t->create_object().set_all(10);

    TableRef parent = g.add_table("parent");
    auto parent_obj = parent->create_object();
    for (int i = 0; i < 10; ++i) {
        auto col = parent->add_column(*t, util::format("link %1", i));
        parent_obj.set(col, child.get_key());
    }

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));
    CHECK(t->is_embedded());
    CHECK(t->size() == 10);
    CHECK(parent->size() == 1);
    CHECK_NOT(child.is_valid()); // original object should have been deleted

    for (auto link_col : parent->get_column_keys()) {
        CHECK_EQUAL(t->get_object(parent_obj.get<ObjKey>(link_col)).get<int64_t>(col), 10);
    }
}

TEST(Group_MakeEmbedded_DuplicateObjectsForList)
{
    Group g;
    TableRef t = g.add_table("child");
    auto col = t->add_column(type_Int, "value");
    auto child = t->create_object().set_all(10);

    TableRef parent = g.add_table("parent");
    auto parent_obj = parent->create_object();
    auto list = parent_obj.get_linklist(parent->add_column_list(*t, "link"));
    for (int i = 0; i < 10; ++i)
        list.add(child.get_key());

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));
    CHECK(t->is_embedded());
    CHECK(t->size() == 10);
    CHECK(parent->size() == 1);
    CHECK_NOT(child.is_valid()); // original object should have been deleted

    CHECK_EQUAL(list.size(), 10);
    for (int i = 0; i < 10; ++i)
        CHECK_EQUAL(t->get_object(list.get(i)).get<int64_t>(col), 10);
}

TEST(Group_MakeEmbedded_DuplicateObjectsForDictionary)
{
    Group g;
    TableRef t = g.add_table("child");
    auto col = t->add_column(type_Int, "value");
    auto child = t->create_object().set_all(10);

    TableRef parent = g.add_table("parent");
    auto parent_obj = parent->create_object();
    auto dict = parent_obj.get_dictionary(parent->add_column_dictionary(*t, "link"));
    for (int i = 0; i < 10; ++i)
        dict.insert(util::to_string(i), child.get_link());

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));
    CHECK(t->is_embedded());
    CHECK(t->size() == 10);
    CHECK(parent->size() == 1);
    CHECK_NOT(child.is_valid()); // original object should have been deleted

    CHECK_EQUAL(dict.size(), 10);
    for (auto [key, value] : dict)
        CHECK_EQUAL(g.get_object(value.get_link()).get<int64_t>(col), 10);
}

TEST(Group_MakeEmbedded_DeepCopy)
{
    Group g;
    TableRef t = g.add_table("table");
    auto obj = t->create_object();

    // Single/List/Set/Dictionary of a primitive value
    obj.set(t->add_column(type_Int, "int"), 1);
    auto int_list = obj.get_list<int64_t>(t->add_column_list(type_Int, "int list"));
    int_list.add(1);
    int_list.add(2);
    int_list.add(3);
    auto int_set = obj.get_set<int64_t>(t->add_column_set(type_Int, "int set"));
    int_set.insert(4);
    int_set.insert(5);
    int_set.insert(6);
    auto int_dict = obj.get_dictionary(t->add_column_dictionary(type_Int, "int dict"));
    int_dict.insert("a", 7);
    int_dict.insert("b", 8);
    int_dict.insert("c", 9);

    // Single/List/Set/Dictionary of a link to a top-level object
    TableRef target = g.add_table("target");
    target->add_column(type_Int, "value");
    auto target_obj1 = target->create_object().set_all(123);
    auto target_obj2 = target->create_object().set_all(456);
    auto target_obj3 = target->create_object().set_all(789);
    auto target_objkey1 = target_obj1.get_key();
    auto target_objkey2 = target_obj2.get_key();
    auto target_objkey3 = target_obj3.get_key();

    obj.set(t->add_column(*target, "link"), target_objkey1);
    auto obj_list = obj.get_linklist(t->add_column_list(*target, "obj list"));
    obj_list.add(target_objkey1);
    obj_list.add(target_objkey2);
    obj_list.add(target_objkey3);
    obj_list.add(target_objkey1);
    obj_list.add(target_objkey2);
    obj_list.add(target_objkey3);
    auto obj_set = obj.get_linkset(t->add_column_set(*target, "obj set"));
    obj_set.insert(target_objkey1);
    obj_set.insert(target_objkey2);
    obj_set.insert(target_objkey3);
    auto obj_dict = obj.get_dictionary(t->add_column_dictionary(*target, "obj dict"));
    obj_dict.insert("a", target_objkey1);
    obj_dict.insert("b", target_objkey2);
    obj_dict.insert("c", target_objkey3);
    obj_dict.insert("d", target_objkey1);
    obj_dict.insert("e", target_objkey2);
    obj_dict.insert("f", target_objkey3);

    // Single/List/Dictionary of a link to an embedded object
    TableRef embedded = g.add_table("embedded", Table::Type::Embedded);
    embedded->add_column(type_Int, "value");

    obj.create_and_set_linked_object(t->add_column(*embedded, "embedded link")).set_all(1);
    auto embedded_list = obj.get_linklist(t->add_column_list(*embedded, "embedded list"));
    embedded_list.create_and_insert_linked_object(0).set_all(2);
    embedded_list.create_and_insert_linked_object(1).set_all(3);
    embedded_list.create_and_insert_linked_object(2).set_all(4);
    auto embedded_dict = obj.get_dictionary(t->add_column_dictionary(*embedded, "embedded dict"));
    embedded_dict.create_and_insert_linked_object("a").set_all(5);
    embedded_dict.create_and_insert_linked_object("b").set_all(6);
    embedded_dict.create_and_insert_linked_object("c").set_all(7);

    // Parent object type for the table which'll be made embedded
    auto parent = g.add_table("parent");
    parent->add_column(*t, "link");
    parent->create_object().set_all(obj.get_key());
    parent->create_object().set_all(obj.get_key());

    // Pre-conversion sanity check
    CHECK_EQUAL(t->size(), 1);
    CHECK_EQUAL(target->size(), 3);
    CHECK_EQUAL(embedded->size(), 7);
    CHECK_EQUAL(parent->get_object(0).get<ObjKey>("link"), parent->get_object(1).get<ObjKey>("link"));

    CHECK_NOTHROW(t->set_table_type(Table::Type::Embedded, true));

    CHECK_EQUAL(t->size(), 2);
    CHECK_EQUAL(target->size(), 3);
    CHECK_EQUAL(embedded->size(), 14);
    CHECK_NOT_EQUAL(parent->get_object(0).get<ObjKey>("link"), parent->get_object(1).get<ObjKey>("link"));

    // The original objects should be deleted, except for target since that's still top-level
    CHECK_NOT(obj.is_valid());
    CHECK(target_obj1.is_valid());
    CHECK(target_obj2.is_valid());
    CHECK(target_obj3.is_valid());
    CHECK_NOT(int_list.is_attached());
    CHECK_NOT(int_set.is_attached());
    CHECK_NOT(int_dict.is_attached());
    CHECK_NOT(obj_list.is_attached());
    CHECK_NOT(obj_set.is_attached());
    CHECK_NOT(obj_dict.is_attached());
    CHECK_NOT(embedded_list.is_attached());
    CHECK_NOT(embedded_dict.is_attached());

    for (auto parent_obj : *parent) {
        auto obj = parent_obj.get_linked_object("link");
        CHECK_EQUAL(obj.get<int64_t>("int"), 1);

        auto int_list = obj.get_list<int64_t>("int list");
        CHECK_EQUAL(int_list.size(), 3);
        CHECK_EQUAL(int_list.get(0), 1);
        CHECK_EQUAL(int_list.get(1), 2);
        CHECK_EQUAL(int_list.get(2), 3);

        auto int_set = obj.get_set<int64_t>("int set");
        CHECK_EQUAL(int_set.size(), 3);
        CHECK_NOT_EQUAL(int_set.find(4), -1);
        CHECK_NOT_EQUAL(int_set.find(5), -1);
        CHECK_NOT_EQUAL(int_set.find(6), -1);

        auto int_dict = obj.get_dictionary("int dict");
        CHECK_EQUAL(int_dict.size(), 3);
        CHECK_EQUAL(int_dict.get("a"), 7);
        CHECK_EQUAL(int_dict.get("b"), 8);
        CHECK_EQUAL(int_dict.get("c"), 9);

        CHECK_EQUAL(obj.get<ObjKey>("link"), target_objkey1);

        auto obj_list = obj.get_linklist("obj list");
        CHECK_EQUAL(obj_list.size(), 6);
        CHECK_EQUAL(obj_list.get(0), target_objkey1);
        CHECK_EQUAL(obj_list.get(1), target_objkey2);
        CHECK_EQUAL(obj_list.get(2), target_objkey3);
        CHECK_EQUAL(obj_list.get(3), target_objkey1);
        CHECK_EQUAL(obj_list.get(4), target_objkey2);
        CHECK_EQUAL(obj_list.get(5), target_objkey3);

        auto obj_set = obj.get_linkset("obj set");
        CHECK_EQUAL(obj_set.size(), 3);
        CHECK_NOT_EQUAL(obj_set.find(target_objkey1), -1);
        CHECK_NOT_EQUAL(obj_set.find(target_objkey2), -1);
        CHECK_NOT_EQUAL(obj_set.find(target_objkey3), -1);

        auto obj_dict = obj.get_dictionary("obj dict");
        CHECK_EQUAL(obj_dict.size(), 6);
        CHECK_EQUAL(obj_dict.get("a"), target_obj1.get_link());
        CHECK_EQUAL(obj_dict.get("b"), target_obj2.get_link());
        CHECK_EQUAL(obj_dict.get("c"), target_obj3.get_link());
        CHECK_EQUAL(obj_dict.get("d"), target_obj1.get_link());
        CHECK_EQUAL(obj_dict.get("e"), target_obj2.get_link());
        CHECK_EQUAL(obj_dict.get("f"), target_obj3.get_link());

        CHECK_EQUAL(obj.get_linked_object("embedded link").get<int64_t>("value"), 1);

        auto embedded_list = obj.get_linklist("embedded list");
        CHECK_EQUAL(embedded_list[0].get<int64_t>("value"), 2);
        CHECK_EQUAL(embedded_list[1].get<int64_t>("value"), 3);
        CHECK_EQUAL(embedded_list[2].get<int64_t>("value"), 4);

        auto embedded_dict = obj.get_dictionary("embedded dict");
        CHECK_EQUAL(embedded_dict.get_object("a").get<int64_t>("value"), 5);
        CHECK_EQUAL(embedded_dict.get_object("b").get<int64_t>("value"), 6);
        CHECK_EQUAL(embedded_dict.get_object("c").get<int64_t>("value"), 7);
    }
}

TEST(Group_WriteEmpty)
{
    SHARED_GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);
    auto db = DB::create(path_1);
    {
        Group group;
        group.write(path_2);
    }
    File::remove(path_2);
    {
        Group group(path_1, 0);
        group.write(path_2);
    }
}


#ifdef REALM_DEBUG
#ifdef REALM_TO_DOT

TEST(Group_ToDot)
{
    // Create group with one table
    Group mygroup;

    // Create table with all column types
    TableRef table = mygroup.add_table("test");
    DescriptorRef subdesc;
    table->add_column(type_Int, "int");
    table->add_column(type_Bool, "bool");
    table->add_column(type_String, "string");
    table->add_column(type_String, "string_long");
    table->add_column(type_String, "string_enum"); // becomes StringEnumColumn
    table->add_column(type_Binary, "binary");
    table->add_column(type_Mixed, "mixed");
    subdesc->add_column(type_Int, "sub_first");
    subdesc->add_column(type_String, "sub_second");
    subdesc.reset();

    // Add some rows
    for (size_t i = 0; i < 15; ++i) {
        table->insert_empty_row(i);
        table->set_int(0, i, i);
        table->set_bool(1, i, (i % 2 ? true : false));
        table->set_olddatetime(2, i, 12345);

        std::stringstream ss;
        ss << "string" << i;
        table->set_string(3, i, ss.str().c_str());

        ss << " very long string.........";
        table->set_string(4, i, ss.str().c_str());

        switch (i % 3) {
            case 0:
                table->set_string(5, i, "test1");
                break;
            case 1:
                table->set_string(5, i, "test2");
                break;
            case 2:
                table->set_string(5, i, "test3");
                break;
        }

        table->set_binary(6, i, BinaryData("binary", 7));

        switch (i % 3) {
            case 0:
                table->set_mixed(7, i, false);
                break;
            case 1:
                table->set_mixed(7, i, (int64_t)i);
                break;
            case 2:
                table->set_mixed(7, i, "string");
                break;
        }

        // Add sub-tables
        if (i == 2) {
            // To mixed column
            table->set_mixed(7, i, Mixed::subtable_tag());
            TableRef st = table->get_subtable(7, i);

            st->add_column(type_Int, "first");
            st->add_column(type_String, "second");

            st->insert_empty_row(0);
            st->set_int(0, 0, 42);
            st->set_string(1, 0, "meaning");

            // To table column
            TableRef subtable2 = table->get_subtable(8, i);
            subtable2->add_empty_row();
            subtable2->set_int(0, 0, 42);
            subtable2->set_string(1, 0, "meaning");
        }
    }

    // We also want StringEnumColumn's
    table->optimize();

#if 1
    // Write array graph to std::cout
    std::stringstream ss;
    mygroup.to_dot(ss);
    std::cout << ss.str() << std::endl;
#endif

    // Write array graph to file in dot format
    std::ofstream fs("realm_graph.dot", std::ios::out | std::ios::binary);
    if (!fs.is_open())
        std::cout << "file open error " << strerror(errno) << std::endl;
    mygroup.to_dot(fs);
    fs.close();
}

#endif // REALM_TO_DOT
#endif // REALM_DEBUG

TEST_TYPES(Group_TimestampAddAIndexAndThenInsertEmptyRows, std::true_type, std::false_type)
{
    constexpr bool nullable = TEST_TYPE::value;
    Group g;
    TableRef table = g.add_table("");
    auto col = table->add_column(type_Timestamp, "date", nullable);
    table->add_search_index(col);
    std::vector<ObjKey> keys;
    table->create_objects(5, keys);
    CHECK_EQUAL(table->size(), 5);
}

TEST(Group_SharedMappingsForReadOnlyStreamingForm)
{
    GROUP_TEST_PATH(path);
    {
        Group g;
        auto table = g.add_table("table");
        table->add_column(type_Int, "col");
        table->create_object();
        g.write(path, crypt_key());
    }

    {
        Group g1(path, crypt_key());
        auto table1 = g1.get_table("table");
        CHECK(table1 && table1->size() == 1);

        Group g2(path, crypt_key());
        auto table2 = g2.get_table("table");
        CHECK(table2 && table2->size() == 1);
    }
}


// This test ensures that cascading delete works by testing that
// a linked row is deleted when the parent row is deleted, but only
// if it is the only parent row. It is also tested that an optional
// notification handler is called appropriately.
// It is tested that it works both with one and two levels of links
// and also if the links creates a cycle
TEST(Group_RemoveRecursive)
{
    Group g;
    TableRef target = g.add_table("target");
    TableRef origin = g.add_table("origin");
    TableKey target_key = target->get_key();

    target->add_column(type_Int, "integers", true);
    auto link_col_t = target->add_column(*target, "links");
    auto link_col_o = origin->add_column(*target, "links");

    // Delete one at a time
    ObjKey key_target = target->create_object().get_key();
    ObjKey k0 = origin->create_object().set(link_col_o, key_target).get_key();
    ObjKey k1 = origin->create_object().set(link_col_o, key_target).get_key();
    CHECK_EQUAL(target->size(), 1);
    origin->remove_object_recursive(k0);
    // Should not have deleted child
    CHECK_EQUAL(target->size(), 1);
    // Delete last link
    origin->remove_object_recursive(k1);
    // Now it should be gone
    CHECK_EQUAL(target->size(), 0);

    // 3 rows linked together in a list
    std::vector<ObjKey> keys;
    target->create_objects(3, keys);
    target->get_object(keys[0]).set(link_col_t, keys[1]);
    target->get_object(keys[1]).set(link_col_t, keys[2]);
    int calls = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        CHECK_EQUAL(notification.rows.size(), 1);
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(target_key, notification.rows[0].table_key);
        CHECK_EQUAL(keys[calls], notification.rows[0].key);
        calls++;
    });
    target->remove_object_recursive(keys[0]);
    CHECK_EQUAL(calls, 3);
    CHECK_EQUAL(target->size(), 0);

    // 3 rows linked together in circle
    keys.clear();
    target->create_objects(3, keys);
    target->get_object(keys[0]).set(link_col_t, keys[1]);
    target->get_object(keys[1]).set(link_col_t, keys[2]);
    target->get_object(keys[2]).set(link_col_t, keys[0]);

    calls = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        // First deletion nullifies the link from 2 -> 0, none others do
        CHECK_EQUAL(calls == 0, notification.links.size());
        CHECK_EQUAL(notification.rows.size(), 1);
        CHECK_EQUAL(target_key, notification.rows[0].table_key);
        CHECK_EQUAL(keys[calls], notification.rows[0].key);
        calls++;
    });
    target->remove_object_recursive(keys[0]);
    CHECK_EQUAL(calls, 3);
    CHECK_EQUAL(target->size(), 0);

    // Object linked to itself
    k0 = target->create_object().get_key();
    target->get_object(k0).set(link_col_t, k0);
    g.set_cascade_notification_handler(nullptr);
    target->remove_object_recursive(k0);
    CHECK_EQUAL(target->size(), 0);
}

TEST(Group_IntPrimaryKeyCol)
{
    Group g;
    TableRef table = g.add_table_with_primary_key("class_foo", type_Int, "primary", true);
    ColKey primary_key_column = table->get_primary_key_column();
    CHECK(primary_key_column);
    CHECK(table->has_search_index(primary_key_column));

    auto obj = table->create_object_with_primary_key({1});
    CHECK_EQUAL(obj.get<Int>(primary_key_column), 1);
    auto obj1 = table->create_object_with_primary_key({});
    CHECK(obj1.is_null(primary_key_column));
    CHECK_EQUAL(table->size(), 2);

    table->set_primary_key_column(ColKey{});
    table->add_search_index(primary_key_column);
    CHECK(table->get_primary_key_column() == ColKey{});
    CHECK(table->has_search_index(primary_key_column));

    table->set_primary_key_column(primary_key_column);
    CHECK(table->get_primary_key_column() == primary_key_column);
    CHECK(table->has_search_index(primary_key_column));
}

TEST(Group_StringPrimaryKeyCol)
{
    Group g;
    TableRef table = g.add_table_with_primary_key("class_foo", type_String, "primary");
    ColKey primary_key_column = table->get_primary_key_column();
    CHECK(primary_key_column);
    ColKey col1 = primary_key_column;
    ColKey col2 = table->add_column(type_String, "secondary");
    ColKey col3 = table->add_column(type_Int, "another");
    ColKey list_col = table->add_column_list(type_Float, "floats");
    CHECK_NOT(table->find_first(primary_key_column, StringData("Exactly!")));
    CHECK(table->has_search_index(primary_key_column));

    auto obj1 = table->create_object_with_primary_key("Exactly!", {{col3, 5}, {col2, "last"}});
    CHECK_THROW_ANY(table->create_object_with_primary_key("Exactly!", {{}}, Table::UpdateMode::never));
    table->create_object_with_primary_key("Exactly!", {{col2, "first"}}, Table::UpdateMode::changed);

    table->create_object_with_primary_key("Paul", {{col2, "John"}});
    table->create_object_with_primary_key("John", {{col2, "Paul"}});
    table->create_object_with_primary_key("George", {{col2, "George"}});
    CHECK_EQUAL(obj1.get<String>(primary_key_column), "Exactly!");
    auto k = table->find_first(primary_key_column, StringData("Exactly!"));
    CHECK_EQUAL(k, obj1.get_key());
    auto list = obj1.get_list<Float>(list_col);
    for (int f = 0; f < 10; f++) {
        list.add(float(f) / 2.f);
    }
    g.validate_primary_columns();

    auto table1 = g.add_table("class_bar");
    auto col_link = table1->add_column(*table, "link");
    auto col_linklist = table1->add_column_list(*table, "linklist");
    Obj origin_obj = table1->create_object();
    origin_obj.set(col_link, k);
    auto ll = origin_obj.get_linklist(col_linklist);
    for (auto o : *table) {
        ll.add(o.get_key());
    }
    // Changing PK
    table->set_primary_key_column(col2);
    g.validate_primary_columns();
    g.verify();
    CHECK(table->get_primary_key_column() == col2);
    CHECK(table->has_search_index(col2));
    CHECK_NOT(table->has_search_index(primary_key_column));

    auto obj2 = table->create_object_with_primary_key({"FooBar"}).set(col1, "second");
    k = table->find_first(col2, StringData("FooBar"));
    CHECK_EQUAL(k, obj2.get_key());
    k = table->find_first(col2, StringData("first"));
    CHECK(obj1.is_valid());
    obj1 = table->get_object(k);
    CHECK_EQUAL(obj1.get<String>(col1), "Exactly!");
    CHECK_EQUAL(origin_obj.get<ObjKey>(col_link), k);
    CHECK_EQUAL(ll.size(), 4);
    CHECK(table->is_valid(ll.get(0)));
    list = obj1.get_list<Float>(list_col);
    CHECK_EQUAL(list.size(), 10);
    CHECK_EQUAL(list.get(5), 2.5f);
    CHECK_EQUAL(table->size(), 5);
    CHECK(table->find_first(col2, StringData("Paul")));
    CHECK(table->find_first(col2, StringData("John")));
    CHECK(table->find_first(col2, StringData("George")));

    // Changing PK back
    table->add_search_index(primary_key_column);
    CHECK(table->get_primary_key_column() == col2);
    CHECK(table->has_search_index(primary_key_column));
    CHECK(table->has_search_index(col2));

    table->set_primary_key_column(primary_key_column);
    g.validate_primary_columns();
    CHECK(table->get_primary_key_column() == primary_key_column);
    CHECK(table->has_search_index(primary_key_column));
    CHECK_NOT(table->has_search_index(col2));
}

TEST(Group_SetColumnWithDuplicateValuesToPrimaryKey)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey string_col = table->add_column(type_String, "string");
    ColKey int_col = table->add_column(type_Int, "int");

    std::vector<ObjKey> keys;
    table->create_objects(2, keys);

    CHECK_THROW(table->set_primary_key_column(string_col), DuplicatePrimaryKeyValueException);
    CHECK_EQUAL(table->get_primary_key_column(), ColKey());
    CHECK_THROW(table->set_primary_key_column(int_col), DuplicatePrimaryKeyValueException);
    CHECK_EQUAL(table->get_primary_key_column(), ColKey());
}

TEST(Group_SetColumnWithNullPrimaryKeyy)
{
    Group g;
    TableRef table = g.add_table("table");
    ColKey string_col = table->add_column(type_String, "string", true);

    std::vector<ObjKey> keys;
    table->create_objects(2, keys);
    table->get_object(keys[0]).set_any(string_col, {"first"});
    table->get_object(keys[1]).set_any(string_col, {});
    CHECK(!table->get_object(keys[0]).is_null(string_col));
    CHECK_EQUAL(table->get_object(keys[0]).get<StringData>(string_col), "first");
    CHECK(table->get_object(keys[1]).is_null(string_col));
    CHECK(table->get_primary_key_column() != string_col);
    CHECK_EQUAL(table->size(), 2);
    table->set_primary_key_column(string_col);
    CHECK_EQUAL(table->get_primary_key_column(), string_col);
    ObjKey first = table->find_first_string(string_col, "first");
    ObjKey second = table->find_first_null(string_col);
    ObjKey third = table->find_first_string(string_col, "not found");
    CHECK(bool(first));
    CHECK(bool(second));
    CHECK(!bool(third));
    CHECK_EQUAL(table->size(), 2);
}

TEST(Group_ChangeIntPrimaryKeyValuesInMigration)
{
    Group g;
    TableRef table = g.add_table_with_primary_key("table", type_Int, "pk");
    ColKey value_col = table->add_column(type_Int, "value");
    ColKey pk_col = table->get_primary_key_column();

    for (int i = 0; i < 10; ++i) {
        table->create_object_with_primary_key(i).set<int64_t>(value_col, i + 10);
    }

    TableView tv = table->where().find_all();
    for (size_t i = 0; i < tv.size(); ++i) {
        Obj obj = tv.get_object(i);
        obj.set(pk_col, obj.get<int64_t>(value_col));
    }
    table->validate_primary_column();

    for (int64_t i = 0; i < 10; ++i) {
        ObjKey key = table->find_first(pk_col, i + 10);
        CHECK(key);
        Obj obj = table->get_object(key);
        CHECK_EQUAL(obj.get<int64_t>(pk_col), i + 10);
        CHECK_EQUAL(obj.get<int64_t>(value_col), i + 10);
    }
}

TEST(Group_ChangeStringPrimaryKeyValuesInMigration)
{
    Group g;
    TableRef table = g.add_table_with_primary_key("table", type_String, "pk");
    ColKey value_col = table->add_column(type_String, "value");
    ColKey pk_col = table->get_primary_key_column();

    for (int i = 0; i < 10; ++i) {
        table->create_object_with_primary_key(util::to_string(i)).set(value_col, util::to_string(i + 10));
    }

    TableView tv = table->where().find_all();
    for (size_t i = 0; i < tv.size(); ++i) {
        Obj obj = tv.get_object(i);
        obj.set(pk_col, obj.get<StringData>(value_col));
    }
    table->validate_primary_column();

    for (int64_t i = 0; i < 10; ++i) {
        auto str = util::to_string(i + 10);
        ObjKey key = table->find_first<StringData>(pk_col, str);
        CHECK(key);
        Obj obj = table->get_object(key);
        CHECK_EQUAL(obj.get<StringData>(pk_col), str);
        CHECK_EQUAL(obj.get<StringData>(value_col), str);
    }
}

TEST(Group_UniqueColumnKeys)
{
    // Table key is shifted 30 bits before added to ColKey, so when handled as
    // 32 bit value, only the two LSB has effect
    Group g;
    g.add_table("0");
    auto foo = g.add_table("foo"); // TableKey == 1
    g.add_table("2");
    g.add_table("3");
    g.add_table("4");
    auto bar = g.add_table("bar"); // Tablekey == 5. Upper bit stripped off before fix, so equal to 1
    auto col_foo = foo->add_column(type_Int, "ints");
    auto col_bar = bar->add_column(type_Int, "ints");
    CHECK_NOT_EQUAL(col_foo, col_bar);
}

#endif // TEST_GROUP
