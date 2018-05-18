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


TEST(Group_Unattached)
{
    Group group((Group::unattached_tag()));

    CHECK(!group.is_attached());
}


TEST(Group_UnattachedErrorHandling)
{
    Group group((Group::unattached_tag()));

    CHECK_EQUAL(false, group.is_empty());
    CHECK_EQUAL(TableKey(), group.find_table("foo"));
    CHECK_LOGIC_ERROR(group.get_table(TableKey()), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table("foo"), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.add_table("foo", false), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table(TableKey()), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table("foo"), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.add_table("foo", false), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.remove_table("foo"), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.remove_table(TableKey()), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.rename_table("foo", "bar", false), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.rename_table(TableKey(), "bar", false), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.commit(), LogicError::detached_accessor);

    {
        const Group& const_group = group;
        CHECK_LOGIC_ERROR(const_group.get_table(TableKey()), LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(const_group.get_table("foo"), LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(const_group.get_table(TableKey()), LogicError::detached_accessor);
    }

    {
        bool f = false;
        CHECK_LOGIC_ERROR(group.get_or_add_table("foo", &f), LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(group.get_or_add_table("foo", &f), LogicError::detached_accessor);
    }
    {
        std::ostringstream out;
        size_t link_depth = 0;
        std::map<std::string, std::string> renames;
        CHECK_LOGIC_ERROR(group.to_json(out, link_depth, &renames), LogicError::detached_accessor);
    }
}


TEST(Group_OpenFile)
{
    GROUP_TEST_PATH(path);

    {
        Group group((Group::unattached_tag()));
        group.open(path, crypt_key(), Group::mode_ReadWrite);
        CHECK(group.is_attached());
    }

    {
        Group group((Group::unattached_tag()));
        group.open(path, crypt_key(), Group::mode_ReadWriteNoCreate);
        CHECK(group.is_attached());
    }

    {
        Group group((Group::unattached_tag()));
        group.open(path, crypt_key(), Group::mode_ReadOnly);
        CHECK(group.is_attached());
    }
}

// Ensure that Group throws when you attempt to attach it twice in a row
TEST(Group_DoubleOpening)
{
    // File-based open()
    {
        GROUP_TEST_PATH(path);
        Group group((Group::unattached_tag()));

        group.open(path, crypt_key(), Group::mode_ReadWrite);
        CHECK_LOGIC_ERROR(group.open(path, crypt_key(), Group::mode_ReadWrite), LogicError::wrong_group_state);
    }

    // Buffer-based open()
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

        Group group((Group::unattached_tag()));
        bool take_ownership = false;

        group.open(BinaryData(buffer.get(), buffer_size), take_ownership);
        CHECK_LOGIC_ERROR(group.open(BinaryData(buffer.get(), buffer_size), take_ownership),
                          LogicError::wrong_group_state);
    }
}

#if REALM_ENABLE_ENCRYPTION
TEST(Group_OpenUnencryptedFileWithKey)
{
    GROUP_TEST_PATH(path);
    {
        Group group(path, nullptr, Group::mode_ReadWrite);

        // We want the file to be exactly three pages in size so that trying to
        // read the footer would use the first non-zero field of the header as
        // the IV
        TableRef table = group.get_or_add_table("table");
        auto col = table->add_column(type_String, "str");
        std::string data(page_size() - 100, '\1');
        table->create_object().set<String>(col, data);
        table->create_object().set<String>(col, data);

        group.commit();
    }

    {
        Group group((Group::unattached_tag()));
        CHECK_THROW(group.open(path, crypt_key(true), Group::mode_ReadWrite), InvalidDatabase);
    }
}
#endif // REALM_ENABLE_ENCRYPTION

#ifndef _WIN32
TEST(Group_Permissions)
{
    if (getuid() == 0) {
        std::cout << "Group_Permissions test skipped because you are running it as root\n\n";
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

    {
        Group group2((Group::unattached_tag()));

        // Following two lines fail under Windows, fixme
        CHECK_THROW(group2.open(path, crypt_key(), Group::mode_ReadOnly), File::PermissionDenied);
        CHECK(!group2.is_attached());
    }
}
#endif

TEST(Group_BadFile)
{
    GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);

    {
        File file(path_1, File::mode_Append);
        file.write("foo");
    }

    {
        Group group((Group::unattached_tag()));
        CHECK_THROW(group.open(path_1, crypt_key(), Group::mode_ReadOnly), InvalidDatabase);
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, crypt_key(), Group::mode_ReadOnly), InvalidDatabase); // Again
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, crypt_key(), Group::mode_ReadWrite), InvalidDatabase);
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, crypt_key(), Group::mode_ReadWriteNoCreate), InvalidDatabase);
        CHECK(!group.is_attached());
        group.open(path_2, crypt_key(), Group::mode_ReadWrite); // This one must work
        CHECK(group.is_attached());
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
        Group group((Group::unattached_tag()));
        bool take_ownership = false;
        group.open(BinaryData(buffer.get(), buffer_size), take_ownership);
        CHECK(group.is_attached());
    }

    // Pass ownership of buffer
    {
        Group group((Group::unattached_tag()));
        bool take_ownership = true;
        group.open(BinaryData(buffer.get(), buffer_size), take_ownership);
        CHECK(group.is_attached());
        buffer.release();
    }
}


TEST(Group_BadBuffer)
{
    GROUP_TEST_PATH(path);

    // Produce an invalid buffer
    char buffer[32];
    for (size_t i = 0; i < sizeof buffer; ++i)
        buffer[i] = char((i + 192) % 128);

    {
        Group group((Group::unattached_tag()));
        bool take_ownership = false;
        CHECK_THROW(group.open(BinaryData(buffer), take_ownership), InvalidDatabase);
        CHECK(!group.is_attached());
        // Check that ownership is not passed on failure during
        // open. If it was, we would get a bad delete on stack
        // allocated memory wich would at least be caught by Valgrind.
        take_ownership = true;
        CHECK_THROW(group.open(BinaryData(buffer), take_ownership), InvalidDatabase);
        CHECK(!group.is_attached());
        // Check that the group is still able to attach to a file,
        // even after failures.
        group.open(path, crypt_key(), Group::mode_ReadWrite);
        CHECK(group.is_attached());
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
    TableRef foo_1 = group.add_table("foo");
    CHECK_EQUAL(1, group.size());
    CHECK_THROW(group.add_table("foo"), TableNameInUse);
    CHECK_EQUAL(1, group.size());
    bool require_unique_name = false;
    TableRef foo_2 = group.add_table("foo", require_unique_name);
    CHECK_EQUAL(2, group.size());
    CHECK_NOT_EQUAL(foo_1, foo_2);
}


TEST(Group_AddTable2)
{
    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    CHECK_EQUAL(2, group.size());
    CHECK_THROW(group.add_table("b"), TableNameInUse);
    CHECK_EQUAL(2, group.size());
}


TEST(Group_AddTableWithLinks)
{
    using tf = _impl::TableFriend;

    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    a->add_column(type_Int, "foo");
    b->add_column_link(type_Link, "bar", *a);

    auto& a_spec = tf::get_spec(*a);
    auto& b_spec = tf::get_spec(*b);
    auto a_key = a->get_key();
    auto b_key = b->get_key();
    CHECK_EQUAL(b_spec.get_opposite_link_table_key(0), a_key);
    CHECK_EQUAL(a_spec.get_opposite_link_table_key(1), b_key);

    group.add_table("c");

    CHECK_EQUAL(b_spec.get_opposite_link_table_key(0), a_key);
    CHECK_EQUAL(a_spec.get_opposite_link_table_key(1), b_key);
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
    group.get_or_add_table("foo", &was_created);
    CHECK(was_created);
    CHECK_EQUAL(2, group.size());
    group.get_or_add_table("foo", &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(2, group.size());
    group.get_or_add_table("bar", &was_created);
    CHECK(was_created);
    CHECK_EQUAL(3, group.size());
    group.get_or_add_table("baz", &was_created);
    CHECK(was_created);
    CHECK_EQUAL(4, group.size());
    group.get_or_add_table("bar", &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(4, group.size());
    group.get_or_add_table("baz", &was_created);
    CHECK_NOT(was_created);
    CHECK_EQUAL(4, group.size());
}


TEST(Group_GetOrAddTable2)
{
    Group group;
    bool was_inserted;
    group.get_or_add_table("foo", &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK(was_inserted);
    group.get_or_add_table("foo", &was_inserted);
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
    CHECK_THROW(obj.get<int64_t>(col), realm::LogicError);
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
    auto col_link = beta->add_column_link(type_Link, "beta-1", *delta);
    gamma->add_column_link(type_Link, "gamma-1", *gamma);
    delta->add_column(type_Int, "delta-1");
    epsilon->add_column_link(type_Link, "epsilon-1", *delta);

    ObjKey k = delta->create_object().get_key();
    beta->create_object().set<ObjKey>(col_link, k);
    auto view = delta->get_backlink_view(k, beta, col_link);
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

    auto one = first->add_column_link(type_Link, "one", *third);

    auto two = third->add_column_link(type_Link, "two", *fourth);
    auto three = third->add_column_link(type_Link, "three", *third);

    auto four = fourth->add_column_link(type_Link, "four", *first);
    auto five = fourth->add_column_link(type_Link, "five", *third);

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
    table->add_column_link(type_Link, "link", *table);
    group.remove_table(table->get_key());
    CHECK(group.is_empty());
    CHECK(!table);
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    target->add_column(type_Int, "int");
    origin->add_column_link(type_Link, "link", *target);
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

TEST(Group_Persist)
{
    GROUP_TEST_PATH(path);

    // Create new database
    Group db(path, crypt_key(), Group::mode_ReadWrite);

    // Insert some data
    TableRef table = db.add_table("test");
    table->add_column(type_Int, "int");
    table->add_column(type_Bool, "bool");
    table->add_column(type_String, "string");
    table->add_column(type_Binary, "binary");
    table->add_column(type_Timestamp, "timestamp");
    table->create_object(ObjKey(0)).set_all(12, true, "test", BinaryData("binary", 7), Timestamp{111, 222});

    // Write changes to file
    db.commit();

#ifdef REALM_DEBUG
    db.verify();
#endif

    {
        CHECK_EQUAL(5, table->get_column_count());
        CHECK_EQUAL(1, table->size());
        auto cols = table->get_column_keys();
        Obj obj = table->get_object(ObjKey(0));
        CHECK_EQUAL(12, obj.get<Int>(cols[0]));
        CHECK_EQUAL(true, obj.get<Bool>(cols[1]));
        CHECK_EQUAL("test", obj.get<String>(cols[2]));
        CHECK_EQUAL(7, obj.get<Binary>(cols[3]).size());
        CHECK_EQUAL("binary", obj.get<Binary>(cols[3]).data());
        CHECK(obj.get<Timestamp>(cols[4]) == Timestamp(111, 222));

        // Change a bit
        obj.set(cols[2], "Changed!");

        // Write changes to file
        db.commit();
    }

#ifdef REALM_DEBUG
    db.verify();
#endif

    {
        CHECK_EQUAL(5, table->get_column_count());
        CHECK_EQUAL(1, table->size());
        auto cols = table->get_column_keys();
        Obj obj = table->get_object(ObjKey(0));
        CHECK_EQUAL(12, obj.get<Int>(cols[0]));
        CHECK_EQUAL(true, obj.get<Bool>(cols[1]));
        CHECK_EQUAL("Changed!", obj.get<String>(cols[2]));
        CHECK_EQUAL(7, obj.get<Binary>(cols[3]).size());
        CHECK_EQUAL("binary", obj.get<Binary>(cols[3]).data());
        CHECK(obj.get<Timestamp>(cols[4]) == Timestamp(111, 222));
    }
}

#ifdef LEGACY_TESTS
TEST(Group_ToJSON)
{
    Group g;
    TableRef table = g.add_table("test");
    test_table_add_columns(table);

    add(table, "jeff", 1, true, int(Wed));
    add(table, "jim", 1, true, int(Wed));
    std::ostringstream out;
    g.to_json(out);
    std::string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("{\"test\":[{\"first\":\"jeff\",\"second\":1,\"third\":true,\"fourth\":2},{\"first\":\"jim\","
                "\"second\":1,\"third\":true,\"fourth\":2}]}",
                str);
}

TEST(Group_ToString)
{
    Group g;
    TableRef table = g.add_table("test");
    test_table_add_columns(table);

    add(table, "jeff", 1, true, int(Wed));
    add(table, "jim", 1, true, int(Wed));
    std::ostringstream out;
    g.to_string(out);
    std::string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("     tables     rows  \n   0 test       2     \n", str.c_str());
}
#endif // LEGACY_TESTS

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

TEST(Group_StockBug)
{
    // This test is a regression test - it once triggered a bug.
    // the bug was fixed in pr 351. In release mode, it crashes
    // the application. To get an assert in debug mode, the max
    // list size should be set to 1000.
    GROUP_TEST_PATH(path);
    Group group(path, crypt_key(), Group::mode_ReadWrite);

    TableRef table = group.add_table("stocks");
    auto col = table->add_column(type_String, "ticker");

    for (size_t i = 0; i < 100; ++i) {
        table->verify();
        table->create_object().set(col, "123456789012345678901234567890123456789");
        table->verify();
        group.commit();
    }
}

TEST(Group_CommitLinkListChange)
{
    GROUP_TEST_PATH(path);
    Group group(path, crypt_key(), Group::mode_ReadWrite);
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);
    target->add_column(type_Int, "integers");
    auto k = target->create_object().get_key();
    origin->create_object().get_linklist(col_link).add(k);
    group.commit();
    group.verify();
}

TEST(Group_Commit_Update_Integer_Index)
{
    // This reproduces a bug where a commit would fail to update the Column::m_search_index pointer
    // and hence crash or behave erratic for subsequent index operations
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("table");
    auto col = t->add_column(type_Int, "integer");

    auto k0 = t->create_object().set<Int>(col, (0 + 1) * 0xeeeeeeeeeeeeeeeeULL).get_key();
    for (size_t i = 1; i < 200; i++) {
        t->create_object().set<Int>(col, (i + 1) * 0xeeeeeeeeeeeeeeeeULL);
    }

    t->add_search_index(col);

    // This would always work
    CHECK(t->find_first_int(col, (0 + 1) * 0xeeeeeeeeeeeeeeeeULL) == k0);

    g.commit();

    // This would fail (sometimes return not_found, sometimes crash)
    CHECK(t->find_first_int(col, (0 + 1) * 0xeeeeeeeeeeeeeeeeULL) == k0);
}


TEST(Group_CascadeNotify_Simple)
{
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");

    // Add some extra rows so that the indexes being tested aren't all 0
    std::vector<ObjKey> t_keys;
    t->create_objects(100, t_keys);

    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification&) { called = true; });
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

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column_link(type_Link, "link", *t);
    auto col_link_list = origin->add_column_link(type_LinkList, "linklist", *t);

    std::vector<ObjKey> o_keys;
    origin->create_objects(100, o_keys);

    // move_last_over() on an un-linked-to row should still just send that row
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

    // move_last_over() on a linked-to row should send information about the
    // links which had linked to it
    // rows are arbitrarily different to make things less likely to pass by coincidence
    Obj obj10 = origin->get_object(o_keys[10]);
    Obj obj15 = origin->get_object(o_keys[15]);
    obj10.set(col_link, t_keys[11]);
    LinkListPtr lv = obj15.get_linklist_ptr(col_link_list);
    lv->add(t_keys[11]);
    lv->add(t_keys[30]);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(t->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(t_keys[11], notification.rows[0].key);

        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(col_link, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(o_keys[10], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[11], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(o_keys[15], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[11], notification.links[1].old_target_key);
    });
    t->remove_object(t_keys[11]);
    t_keys.erase(t_keys.begin() + 11);
    CHECK(called);

    // move_last_over() on the origin table just sends the row being removed
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

    // move_last_over() on the origin table with strong links lists the target
    // rows that are removed
    origin->set_link_type(col_link, link_Strong);
    origin->set_link_type(col_link_list, link_Strong);

    Obj obj12 = origin->get_object(o_keys[12]);
    Obj obj13 = origin->get_object(o_keys[13]);
    obj12.set(col_link, t_keys[50]);
    lv = obj12.get_linklist_ptr(col_link_list);
    lv->add(t_keys[60]);
    lv->add(t_keys[61]);
    lv->add(t_keys[61]);
    lv->add(t_keys[62]);
    obj13.set(col_link, t_keys[62]); // hold on to 62
    // 50, 60 and 61 should be removed; 62 should not as there's still a strong link
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(4, notification.rows.size());
        CHECK_EQUAL(origin->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(o_keys[12], notification.rows[0].key);
        CHECK_EQUAL(t->get_key(), notification.rows[1].table_key);
        CHECK_EQUAL(t_keys[50], notification.rows[1].key);
        CHECK_EQUAL(t->get_key(), notification.rows[2].table_key);
        CHECK_EQUAL(t_keys[60], notification.rows[2].key);
        CHECK_EQUAL(t->get_key(), notification.rows[3].table_key);
        CHECK_EQUAL(t_keys[61], notification.rows[3].key);

        CHECK_EQUAL(0, notification.links.size());
    });
    origin->remove_object(o_keys[12]);
    CHECK(called);

    // Indirect nullifications: move_last_over() on a row with the last strong
    // links to a row that still has weak links to it
    auto col_link_weak = origin->add_column_link(type_Link, "link2", *t);
    auto col_link_list_weak = origin->add_column_link(type_LinkList, "linklist2", *t);

    Obj obj30 = t->get_object(t_keys[30]);
    Obj obj31 = t->get_object(t_keys[31]);
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link_list));
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link_weak));
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link_list_weak));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link_list));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link_weak));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link_list_weak));

    Obj obj20 = origin->get_object(o_keys[20]);
    Obj obj25 = origin->get_object(o_keys[25]);
    obj20.set(col_link, t_keys[30]);
    obj20.get_linklist(col_link_list).add(t_keys[31]);
    obj25.set(col_link_weak, t_keys[31]);
    obj25.get_linklist(col_link_list_weak).add(t_keys[30]);

    CHECK_EQUAL(1, obj30.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link_list));
    CHECK_EQUAL(0, obj30.get_backlink_count(*origin, col_link_weak));
    CHECK_EQUAL(1, obj30.get_backlink_count(*origin, col_link_list_weak));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(1, obj31.get_backlink_count(*origin, col_link_list));
    CHECK_EQUAL(1, obj31.get_backlink_count(*origin, col_link_weak));
    CHECK_EQUAL(0, obj31.get_backlink_count(*origin, col_link_list_weak));

    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(3, notification.rows.size());

        CHECK_EQUAL(origin->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(o_keys[20], notification.rows[0].key);
        CHECK_EQUAL(t->get_key(), notification.rows[1].table_key);
        CHECK_EQUAL(t_keys[30], notification.rows[1].key);
        CHECK_EQUAL(t->get_key(), notification.rows[2].table_key);
        CHECK_EQUAL(t_keys[31], notification.rows[2].key);

        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(col_link_list_weak, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(o_keys[25], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[30], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_weak, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(o_keys[25], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[31], notification.links[1].old_target_key);
    });
    origin->remove_object(o_keys[20]);
    CHECK(called);
}


TEST(Group_CascadeNotify_TableClear)
{
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");

    std::vector<ObjKey> t_keys;
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

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column_link(type_Link, "link", *t);
    auto col_link_list = origin->add_column_link(type_LinkList, "linklist", *t);

    std::vector<ObjKey> o_keys;
    origin->create_objects(10, o_keys);
    t->create_objects(10, t_keys);

    // clear() does report nullified links
    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);

    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.rows.size());

        CHECK_EQUAL(2, notification.links.size());
        CHECK_EQUAL(col_link, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(o_keys[1], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[2], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(o_keys[3], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[4], notification.links[1].old_target_key);
    });
    t->clear();
    t_keys.clear();
    CHECK(called);

    t->create_objects(10, t_keys);

    // and cascaded deletions
    origin->set_link_type(col_link, link_Strong);
    origin->set_link_type(col_link_list, link_Strong);

    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);

    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(2, notification.rows.size());
        CHECK_EQUAL(t->get_key(), notification.rows[0].table_key);
        CHECK_EQUAL(t_keys[2], notification.rows[0].key);
        CHECK_EQUAL(t->get_key(), notification.rows[1].table_key);
        CHECK_EQUAL(t_keys[4], notification.rows[1].key);

        CHECK_EQUAL(0, notification.links.size());
    });
    origin->clear();
    CHECK(called);
}

TEST(Group_CascadeNotify_TableViewClear)
{
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");

    std::vector<ObjKey> t_keys;
    t->create_objects(10, t_keys);

    // No link columns, so remove() is used
    // Unlike clearing a table, the rows removed by the clear() are included in
    // the notification so that cascaded deletions and direct deletions don't
    // need to be handled separately
    int called = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called++;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(10, notification.rows.size());
    });
    t->where().find_all().clear();
    t_keys.clear();
    CHECK_EQUAL(called, 1);

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    auto col_link = origin->add_column_link(type_Link, "link", *t);
    auto col_link_list = origin->add_column_link(type_LinkList, "linklist", *t);

    std::vector<ObjKey> o_keys;
    origin->create_objects(10, o_keys);
    t->create_objects(10, t_keys);

    // should list which links were nullified
    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);

    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called++;
        CHECK_EQUAL(10, notification.rows.size());
        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(col_link, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(o_keys[1], notification.links[0].origin_key);
        CHECK_EQUAL(t_keys[2], notification.links[0].old_target_key);

        CHECK_EQUAL(col_link_list, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(o_keys[3], notification.links[1].origin_key);
        CHECK_EQUAL(t_keys[4], notification.links[1].old_target_key);
    });
    t->where().find_all().clear();
    t_keys.clear();
    CHECK_EQUAL(called, 2);

    t->create_objects(10, t_keys);

    // should included cascaded deletions
    origin->set_link_type(col_link, link_Strong);
    origin->set_link_type(col_link_list, link_Strong);

    origin->get_object(o_keys[1]).set(col_link, t_keys[2]);
    origin->get_object(o_keys[3]).get_linklist(col_link_list).add(t_keys[4]);

    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called++;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(12, notification.rows.size()); // 10 from origin, 2 from target
        CHECK_EQUAL(t->get_key(), notification.rows[10].table_key);
        CHECK_EQUAL(t_keys[2], notification.rows[10].key);
        CHECK_EQUAL(t->get_key(), notification.rows[11].table_key);
        CHECK_EQUAL(t_keys[4], notification.rows[11].key);
    });
    origin->where().find_all().clear();
    CHECK_EQUAL(called, 3);
}

TEST(Group_WriteEmpty)
{
    GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);
    {
        Group group;
        group.write(path_2);
    }
    File::remove(path_2);
    {
        Group group(path_1, 0, Group::mode_ReadWrite);
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
    table->add_column(type_OldDateTime, "date");
    table->add_column(type_String, "string");
    table->add_column(type_String, "string_long");
    table->add_column(type_String, "string_enum"); // becomes StringEnumColumn
    table->add_column(type_Binary, "binary");
    table->add_column(type_Mixed, "mixed");
    table->add_column(type_Table, "tables", &subdesc);
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
        Group g1(path, crypt_key(), Group::mode_ReadOnly);
        auto table1 = g1.get_table("table");
        CHECK(table1 && table1->size() == 1);

        Group g2(path, crypt_key(), Group::mode_ReadOnly);
        auto table2 = g2.get_table("table");
        CHECK(table2 && table2->size() == 1);
    }
}


#ifdef LEGACY_TESTS
// This test embodies a current limitation of our merge algorithm. If this
// limitation is lifted, the code for the SET_UNIQUE instruction in
// fuzz_group.cpp should be strengthened to reflect this.
// (i.e. remove the try / catch for LogicError of kind illegal_combination)
TEST(Group_SetNullUniqueLimitation)
{
    Group g;
    TableRef t = g.add_table("t0");
    t->add_column(type_Int, "", true);
    t->add_search_index(0);
    t->add_column_link(type_LinkList, "", *t);
    t->add_empty_row();
    t->get_linklist(1, 0)->add(0);
    try {
        t->set_null_unique(0, 0);
    }
    catch (const LogicError& le) {
        CHECK(le.kind() == LogicError::illegal_combination);
    }
}
#endif // LEGACY_TESTS

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
    auto link_col_t = target->add_column_link(type_Link, "links", *target);
    auto link_col_o = origin->add_column_link(type_Link, "links", *target);

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
    int called = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called++;
        size_t sz = notification.rows.size();
        CHECK_EQUAL(3, sz);
        for (size_t i = 0; i < sz; i++) {
            CHECK_EQUAL(target_key, notification.rows[i].table_key);
            CHECK_EQUAL(keys[i], notification.rows[i].key);
        }

        CHECK_EQUAL(0, notification.links.size());
    });
    target->remove_object_recursive(keys[0]);
    CHECK_EQUAL(called, 1);
    CHECK_EQUAL(target->size(), 0);

    // 3 rows linked together in circle
    keys.clear();
    target->create_objects(3, keys);
    target->get_object(keys[0]).set(link_col_t, keys[1]);
    target->get_object(keys[1]).set(link_col_t, keys[2]);
    target->get_object(keys[2]).set(link_col_t, keys[0]);

    called = 0;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called++;
        size_t sz = notification.rows.size();
        CHECK_EQUAL(3, sz);
        for (size_t i = 0; i < sz; i++) {
            CHECK_EQUAL(target_key, notification.rows[i].table_key);
            CHECK_EQUAL(keys[i], notification.rows[i].key);
        }

        CHECK_EQUAL(0, notification.links.size());
    });
    target->remove_object_recursive(keys[0]);
    CHECK_EQUAL(called, 1);
    CHECK_EQUAL(target->size(), 0);

    // Object linked to itself
    k0 = target->create_object().get_key();
    target->get_object(k0).set(link_col_t, k0);
    g.set_cascade_notification_handler(nullptr);
    target->remove_object_recursive(k0);
    CHECK_EQUAL(target->size(), 0);
}

#endif // TEST_GROUP
