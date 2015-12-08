#include "testsettings.hpp"
#ifdef TEST_GROUP

#include <algorithm>
#include <fstream>

#include <sys/stat.h>
#ifndef _WIN32
#  include <unistd.h>
#  include <sys/types.h>
#endif

// File permissions for Windows
// http://stackoverflow.com/questions/592448/c-how-to-set-file-permissions-cross-platform
#ifdef _WIN32
#  include <io.h>
typedef int mode_t2;
static const mode_t2 S_IWUSR = mode_t2(_S_IWRITE);
static const mode_t2 MS_MODE_MASK = 0x0000ffff;
#endif

#include <realm.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"
#include "crypt_key.hpp"

using namespace realm;
using namespace realm::util;
using namespace std;

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

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

REALM_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

REALM_TABLE_3(TestTableGroup2,
                first,  Mixed,
                second, Subtable<TestTableGroup>,
                third,  Subtable<TestTableGroup>)

} // Anonymous namespace


TEST(Group_Unattached)
{
    Group group((Group::unattached_tag()));

    CHECK(!group.is_attached());
}


TEST(Group_UnattachedErrorHandling)
{
    Group group((Group::unattached_tag()));

    // FIXME: Uncomment the two commented lines below once #935 is fixed.

//  CHECK_LOGIC_ERROR(group.is_empty(),                              LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.size(),                                  LogicError::detached_accessor);
//  CHECK_LOGIC_ERROR(group.find_table("foo"),                       LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table(0),                            LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table("foo"),                        LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.add_table("foo", false),                 LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table<TestTableGroup>(0),            LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.get_table<TestTableGroup>("foo"),        LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(group.add_table<TestTableGroup>("foo", false), LogicError::detached_accessor);

    {
        const auto& const_group = group;
        CHECK_LOGIC_ERROR(const_group.get_table(0),                 LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(const_group.get_table("foo"),             LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(const_group.get_table<TestTableGroup>(0), LogicError::detached_accessor);
    }

    {
        bool f = false;
        CHECK_LOGIC_ERROR(group.get_or_add_table("foo", &f),                 LogicError::detached_accessor);
        CHECK_LOGIC_ERROR(group.get_or_add_table<TestTableGroup>("foo", &f), LogicError::detached_accessor);
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
        using Deleter = decltype(::free)*;
        char* dummy = nullptr; // to make it compile on some early Visual Studio 2015 RC (now fixed properly in VC)
        std::unique_ptr<char[], Deleter> buffer(dummy, ::free);
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
                buffer.reset(static_cast<char*>(malloc(buffer_size)));
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

#ifndef _WIN32
TEST(Group_Permissions)
{
    if(getuid() == 0) {
        std::cout << "Group_Permissions test skipped because you are running it as root\n\n";
        return;
    }

    GROUP_TEST_PATH(path);
    {
        Group group1;
        TableRef t1 = group1.add_table("table1");
        t1->add_column(type_String, "s");
        t1->add_column(type_Int,    "i");
        for(size_t i=0; i<4; ++i) {
            t1->insert_empty_row(i);
            t1->set_string(0, i, "a");
            t1->set_int(1, i, 3);
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
            buffer.reset(static_cast<char*>(malloc(buffer_size)));
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
    for (size_t i=0; i<sizeof buffer; ++i)
        buffer[i] = char((i+192)%128);

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


TEST(Group_InsertTable)
{
    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.insert_table(0, "b");
    CHECK_EQUAL(2, group.size());
    CHECK_THROW(group.insert_table(2, "b"), TableNameInUse);
    CHECK_EQUAL(2, group.size());
    CHECK_EQUAL(a->get_index_in_group(), 1);
    CHECK_EQUAL(b->get_index_in_group(), 0);
}


TEST(Group_InsertTableWithLinks)
{
    using df = _impl::DescriptorFriend;

    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    a->add_column(type_Int, "foo");
    b->add_column_link(type_Link, "bar", *a);

    auto& a_spec = df::get_spec(*a->get_descriptor());
    auto& b_spec = df::get_spec(*b->get_descriptor());
    CHECK_EQUAL(b_spec.get_opposite_link_table_ndx(0), 0);
    CHECK_EQUAL(a_spec.get_opposite_link_table_ndx(1), 1);

    group.insert_table(0, "c");

    CHECK_EQUAL(b_spec.get_opposite_link_table_ndx(0), 1);
    CHECK_EQUAL(a_spec.get_opposite_link_table_ndx(1), 2);
}


TEST(Group_TableNameTooLong)
{
    Group group;
    size_t buf_len = 64;
    std::unique_ptr<char[]> buf(new char[buf_len]);
    CHECK_LOGIC_ERROR(group.add_table(StringData(buf.get(), buf_len)),
                      LogicError::table_name_too_long);
    group.add_table(StringData(buf.get(), buf_len - 1));
}


TEST(Group_TableIndex)
{
    Group group;
    TableRef moja  = group.add_table("moja");
    TableRef mbili = group.add_table("mbili");
    TableRef tatu  = group.add_table("tatu");
    CHECK_EQUAL(3, group.size());
    std::vector<size_t> indexes;
    indexes.push_back(moja->get_index_in_group());
    indexes.push_back(mbili->get_index_in_group());
    indexes.push_back(tatu->get_index_in_group());
    sort(indexes.begin(), indexes.end());
    CHECK_EQUAL(0, indexes[0]);
    CHECK_EQUAL(1, indexes[1]);
    CHECK_EQUAL(2, indexes[2]);
    CHECK_EQUAL(moja,  group.get_table(moja->get_index_in_group()));
    CHECK_EQUAL(mbili, group.get_table(mbili->get_index_in_group()));
    CHECK_EQUAL(tatu,  group.get_table(tatu->get_index_in_group()));
    CHECK_EQUAL("moja",  group.get_table_name(moja->get_index_in_group()));
    CHECK_EQUAL("mbili", group.get_table_name(mbili->get_index_in_group()));
    CHECK_EQUAL("tatu",  group.get_table_name(tatu->get_index_in_group()));
    CHECK_LOGIC_ERROR(group.get_table(4), LogicError::table_index_out_of_range);
    CHECK_LOGIC_ERROR(group.get_table_name(4), LogicError::table_index_out_of_range);
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


TEST(Group_GetOrInsertTable)
{
    Group group;
    bool was_inserted;
    group.get_or_insert_table(0, "foo", &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK(was_inserted);
    group.get_or_insert_table(0, "foo", &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK_NOT(was_inserted);
    group.get_or_insert_table(1, "foo", &was_inserted);
    CHECK_EQUAL(1, group.size());
    CHECK_NOT(was_inserted);
}


TEST(Group_StaticallyTypedTables)
{
    Group group;
    const Group& cgroup = group;

    TestTableGroup::Ref  table_1 = group.add_table<TestTableGroup>("table_1");
    TestTableGroup2::Ref table_2 = group.add_table<TestTableGroup2>("table_2");

    CHECK_THROW(group.add_table("table_2"), TableNameInUse);
    CHECK_THROW(group.add_table<TestTableGroup>("table_2"),  TableNameInUse);
    CHECK_THROW(group.add_table<TestTableGroup2>("table_2"), TableNameInUse);

    CHECK_NOT(group.get_table("foo"));
    CHECK_NOT(cgroup.get_table("foo"));
    CHECK_NOT(group.get_table<TestTableGroup>("foo"));
    CHECK_NOT(cgroup.get_table<TestTableGroup>("foo"));
    CHECK_NOT(group.get_table<TestTableGroup2>("foo"));
    CHECK_NOT(cgroup.get_table<TestTableGroup2>("foo"));

    CHECK_EQUAL(table_1, group.get_table<TestTableGroup>(table_1->get_index_in_group()));
    CHECK_EQUAL(table_1, cgroup.get_table<TestTableGroup>(table_1->get_index_in_group()));
    CHECK_EQUAL(table_2, group.get_table<TestTableGroup2>(table_2->get_index_in_group()));
    CHECK_EQUAL(table_2, cgroup.get_table<TestTableGroup2>(table_2->get_index_in_group()));
    CHECK_THROW(group.get_table<TestTableGroup2>(table_1->get_index_in_group()),  DescriptorMismatch);
    CHECK_THROW(cgroup.get_table<TestTableGroup2>(table_1->get_index_in_group()), DescriptorMismatch);
    CHECK_THROW(group.get_table<TestTableGroup>(table_2->get_index_in_group()),   DescriptorMismatch);
    CHECK_THROW(cgroup.get_table<TestTableGroup>(table_2->get_index_in_group()),  DescriptorMismatch);

    CHECK_EQUAL(table_1, group.get_table<TestTableGroup>("table_1"));
    CHECK_EQUAL(table_1, cgroup.get_table<TestTableGroup>("table_1"));
    CHECK_EQUAL(table_2, group.get_table<TestTableGroup2>("table_2"));
    CHECK_EQUAL(table_2, cgroup.get_table<TestTableGroup2>("table_2"));
    CHECK_THROW(group.get_table<TestTableGroup2>("table_1"),  DescriptorMismatch);
    CHECK_THROW(cgroup.get_table<TestTableGroup2>("table_1"), DescriptorMismatch);
    CHECK_THROW(group.get_table<TestTableGroup>("table_2"),   DescriptorMismatch);
    CHECK_THROW(cgroup.get_table<TestTableGroup>("table_2"),  DescriptorMismatch);

    CHECK_EQUAL(table_1, group.get_or_add_table<TestTableGroup>("table_1"));
    CHECK_EQUAL(table_2, group.get_or_add_table<TestTableGroup2>("table_2"));
    CHECK_THROW(group.get_or_add_table<TestTableGroup2>("table_1"), DescriptorMismatch);
    CHECK_THROW(group.get_or_add_table<TestTableGroup>("table_2"),  DescriptorMismatch);

    CHECK_LOGIC_ERROR(group.get_table<TestTableGroup>(3), LogicError::table_index_out_of_range);
}


TEST(Group_BasicRemoveTable)
{
    Group group;
    TableRef alpha = group.add_table("alpha");
    TableRef beta  = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    TableRef delta = group.add_table("delta");
    CHECK_EQUAL(4, group.size());
    group.remove_table(gamma->get_index_in_group()); // By index
    CHECK_EQUAL(3, group.size());
    CHECK(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK_NOT(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK_EQUAL("alpha", group.get_table_name(alpha->get_index_in_group()));
    CHECK_EQUAL("beta",  group.get_table_name(beta->get_index_in_group()));
    CHECK_EQUAL("delta", group.get_table_name(delta->get_index_in_group()));
    group.remove_table(alpha->get_index_in_group()); // By index
    CHECK_EQUAL(2, group.size());
    CHECK_NOT(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK_NOT(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK_EQUAL("beta",  group.get_table_name(beta->get_index_in_group()));
    CHECK_EQUAL("delta", group.get_table_name(delta->get_index_in_group()));
    group.remove_table("delta"); // By name
    CHECK_EQUAL(1, group.size());
    CHECK_NOT(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK_NOT(gamma->is_attached());
    CHECK_NOT(delta->is_attached());
    CHECK_EQUAL("beta",  group.get_table_name(beta->get_index_in_group()));
    CHECK_LOGIC_ERROR(group.remove_table(1), LogicError::table_index_out_of_range);
    CHECK_THROW(group.remove_table("epsilon"), NoSuchTable);
    group.verify();
}


TEST(Group_RemoveTableWithColumns)
{
    Group group;

    TableRef alpha   = group.add_table("alpha");
    TableRef beta    = group.add_table("beta");
    TableRef gamma   = group.add_table("gamma");
    TableRef delta   = group.add_table("delta");
    TableRef epsilon = group.add_table("epsilon");
    CHECK_EQUAL(5, group.size());

    alpha->add_column(type_Int, "alpha-1");
    beta->add_column_link(type_Link, "beta-1", *delta);
    gamma->add_column_link(type_Link, "gamma-1", *gamma);
    delta->add_column(type_Int, "delta-1");
    epsilon->add_column_link(type_Link, "epsilon-1", *delta);

    // Remove table with columns, but no link columns, and table is not a link
    // target.
    group.remove_table("alpha");
    CHECK_EQUAL(4, group.size());
    CHECK_NOT(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Remove table with link column, and table is not a link target.
    group.remove_table("beta");
    CHECK_EQUAL(3, group.size());
    CHECK_NOT(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Remove table with self-link column, and table is not a target of link
    // columns of other tables.
    group.remove_table("gamma");
    CHECK_EQUAL(2, group.size());
    CHECK_NOT(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());

    // Try, but fail to remove table which is a target of link columns of other
    // tables.
    CHECK_THROW(group.remove_table("delta"), CrossTableLinkTarget);
    CHECK_EQUAL(2, group.size());
    CHECK(delta->is_attached());
    CHECK(epsilon->is_attached());
}


TEST(Group_RemoveTableMovesTableWithLinksOver)
{
    // Create a scenario where a table is removed from the group, and the last
    // table in the group (which will be moved into the vacated slot) has both
    // link and backlink columns.

    Group group;
    group.add_table("alpha");
    group.add_table("beta");
    group.add_table("gamma");
    group.add_table("delta");
    TableRef first  = group.get_table(0);
    TableRef second = group.get_table(1);
    TableRef third  = group.get_table(2);
    TableRef fourth = group.get_table(3);

    first->add_column_link(type_Link,  "one",   *third);
    third->add_column_link(type_Link,  "two",   *fourth);
    third->add_column_link(type_Link,  "three", *third);
    fourth->add_column_link(type_Link, "four",  *first);
    fourth->add_column_link(type_Link, "five",  *third);
    first->add_empty_row(2);
    third->add_empty_row(2);
    fourth->add_empty_row(2);
    first->set_link(0,0,0);  // first[0].one   = third[0]
    first->set_link(0,1,1);  // first[1].one   = third[1]
    third->set_link(0,0,1);  // third[0].two   = fourth[1]
    third->set_link(0,1,0);  // third[1].two   = fourth[0]
    third->set_link(1,0,1);  // third[0].three = third[1]
    third->set_link(1,1,1);  // third[1].three = third[1]
    fourth->set_link(0,0,0); // fourth[0].four = first[0]
    fourth->set_link(0,1,0); // fourth[1].four = first[0]
    fourth->set_link(1,0,0); // fourth[0].five = third[0]
    fourth->set_link(1,1,1); // fourth[1].five = third[1]

    group.verify();

    group.remove_table(1); // Second

    group.verify();

    CHECK_EQUAL(3, group.size());
    CHECK(first->is_attached());
    CHECK_NOT(second->is_attached());
    CHECK(third->is_attached());
    CHECK(fourth->is_attached());
    CHECK_EQUAL(1, first->get_column_count());
    CHECK_EQUAL("one", first->get_column_name(0));
    CHECK_EQUAL(third, first->get_link_target(0));
    CHECK_EQUAL(2, third->get_column_count());
    CHECK_EQUAL("two",   third->get_column_name(0));
    CHECK_EQUAL("three", third->get_column_name(1));
    CHECK_EQUAL(fourth, third->get_link_target(0));
    CHECK_EQUAL(third,  third->get_link_target(1));
    CHECK_EQUAL(2, fourth->get_column_count());
    CHECK_EQUAL("four", fourth->get_column_name(0));
    CHECK_EQUAL("five", fourth->get_column_name(1));
    CHECK_EQUAL(first, fourth->get_link_target(0));
    CHECK_EQUAL(third, fourth->get_link_target(1));

    third->set_link(0,0,0);  // third[0].two   = fourth[0]
    fourth->set_link(0,1,1); // fourth[1].four = first[1]
    first->set_link(0,0,1);  // first[0].one   = third[1]

    group.verify();

    CHECK_EQUAL(2, first->size());
    CHECK_EQUAL(1, first->get_link(0,0));
    CHECK_EQUAL(1, first->get_link(0,1));
    CHECK_EQUAL(1, first->get_backlink_count(0, *fourth, 0));
    CHECK_EQUAL(1, first->get_backlink_count(1, *fourth, 0));
    CHECK_EQUAL(2, third->size());
    CHECK_EQUAL(0, third->get_link(0,0));
    CHECK_EQUAL(0, third->get_link(0,1));
    CHECK_EQUAL(1, third->get_link(1,0));
    CHECK_EQUAL(1, third->get_link(1,1));
    CHECK_EQUAL(0, third->get_backlink_count(0, *first,  0));
    CHECK_EQUAL(2, third->get_backlink_count(1, *first,  0));
    CHECK_EQUAL(0, third->get_backlink_count(0, *third,  1));
    CHECK_EQUAL(2, third->get_backlink_count(1, *third,  1));
    CHECK_EQUAL(1, third->get_backlink_count(0, *fourth, 1));
    CHECK_EQUAL(1, third->get_backlink_count(1, *fourth, 1));
    CHECK_EQUAL(2, fourth->size());
    CHECK_EQUAL(0, fourth->get_link(0,0));
    CHECK_EQUAL(1, fourth->get_link(0,1));
    CHECK_EQUAL(0, fourth->get_link(1,0));
    CHECK_EQUAL(1, fourth->get_link(1,1));
    CHECK_EQUAL(2, fourth->get_backlink_count(0, *third, 0));
    CHECK_EQUAL(0, fourth->get_backlink_count(1, *third, 0));
}


TEST(Group_RemoveLinkTable)
{
    Group group;
    TableRef table = group.add_table("table");
    table->add_column_link(type_Link, "", *table);
    group.remove_table(table->get_index_in_group());
    CHECK(group.is_empty());
    CHECK(!table->is_attached());
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    target->add_column(type_Int, "");
    origin->add_column_link(type_Link, "", *target);
    CHECK_THROW(group.remove_table(target->get_index_in_group()), CrossTableLinkTarget);
    group.remove_table(origin->get_index_in_group());
    CHECK_EQUAL(1, group.size());
    CHECK(!origin->is_attached());
    CHECK(target->is_attached());
    group.verify();
}


TEST(Group_RenameTable)
{
    Group group;
    TableRef alpha = group.add_table("alpha");
    TableRef beta  = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    group.rename_table(beta->get_index_in_group(), "delta");
    CHECK_EQUAL("delta", beta->get_name());
    group.rename_table("delta", "epsilon");
    CHECK_EQUAL("alpha",   alpha->get_name());
    CHECK_EQUAL("epsilon", beta->get_name());
    CHECK_EQUAL("gamma",   gamma->get_name());
    CHECK_LOGIC_ERROR(group.rename_table(3, "zeta"), LogicError::table_index_out_of_range);
    CHECK_THROW(group.rename_table("eta", "theta"), NoSuchTable);
    CHECK_THROW(group.rename_table("epsilon", "alpha"), TableNameInUse);
    bool require_unique_name = false;
    group.rename_table("epsilon", "alpha", require_unique_name);
    CHECK_EQUAL("alpha", alpha->get_name());
    CHECK_EQUAL("alpha", beta->get_name());
    CHECK_EQUAL("gamma", gamma->get_name());
    group.verify();
}


TEST(Group_BasicMoveTable)
{
    Group group;
    TableRef alpha = group.add_table("alpha");
    TableRef beta  = group.add_table("beta");
    TableRef gamma = group.add_table("gamma");
    TableRef delta = group.add_table("delta");
    CHECK_EQUAL(4, group.size());

    // Move up:
    group.move_table(1, 3);
    CHECK_EQUAL(4, group.size());
    CHECK(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK_EQUAL(0, alpha->get_index_in_group());
    CHECK_EQUAL(3, beta->get_index_in_group());
    CHECK_EQUAL(1, gamma->get_index_in_group());
    CHECK_EQUAL(2, delta->get_index_in_group());

    group.verify();

    // Move down:
    group.move_table(2, 0);
    CHECK_EQUAL(4, group.size());
    CHECK(alpha->is_attached());
    CHECK(beta->is_attached());
    CHECK(gamma->is_attached());
    CHECK(delta->is_attached());
    CHECK_EQUAL(1, alpha->get_index_in_group());
    CHECK_EQUAL(3, beta->get_index_in_group());
    CHECK_EQUAL(2, gamma->get_index_in_group());
    CHECK_EQUAL(0, delta->get_index_in_group());

    group.verify();
}

TEST(Group_MoveTableWithLinks)
{
    using df = _impl::DescriptorFriend;
    Group group;
    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    TableRef c = group.add_table("c");
    TableRef d = group.add_table("d");
    CHECK_EQUAL(4, group.size());
    a->add_column_link(type_Link, "link_to_b", *b);
    b->add_column_link(type_LinkList, "link_to_c", *c);
    c->add_column_link(type_Link, "link_to_d", *d);
    d->add_column_link(type_LinkList, "link_to_a", *a);

    auto& a_spec = df::get_spec(*a->get_descriptor());
    auto& b_spec = df::get_spec(*b->get_descriptor());
    auto& c_spec = df::get_spec(*c->get_descriptor());
    auto& d_spec = df::get_spec(*d->get_descriptor());

    // Move up:
    group.move_table(1, 3);
    CHECK(a->is_attached());
    CHECK(b->is_attached());
    CHECK(c->is_attached());
    CHECK(d->is_attached());
    CHECK_EQUAL(a->get_link_target(0), b);
    CHECK_EQUAL(b->get_link_target(0), c);
    CHECK_EQUAL(c->get_link_target(0), d);
    CHECK_EQUAL(d->get_link_target(0), a);
    // Check backlink columns
    CHECK_EQUAL(a_spec.get_opposite_link_table_ndx(1), d->get_index_in_group());
    CHECK_EQUAL(b_spec.get_opposite_link_table_ndx(1), a->get_index_in_group());
    CHECK_EQUAL(c_spec.get_opposite_link_table_ndx(1), b->get_index_in_group());
    CHECK_EQUAL(d_spec.get_opposite_link_table_ndx(1), c->get_index_in_group());

    // Move down:
    group.move_table(2, 0);
    CHECK(a->is_attached());
    CHECK(b->is_attached());
    CHECK(c->is_attached());
    CHECK(d->is_attached());
    CHECK_EQUAL(a->get_link_target(0), b);
    CHECK_EQUAL(b->get_link_target(0), c);
    CHECK_EQUAL(c->get_link_target(0), d);
    CHECK_EQUAL(d->get_link_target(0), a);
    // Check backlink columns
    CHECK_EQUAL(a_spec.get_opposite_link_table_ndx(1), d->get_index_in_group());
    CHECK_EQUAL(b_spec.get_opposite_link_table_ndx(1), a->get_index_in_group());
    CHECK_EQUAL(c_spec.get_opposite_link_table_ndx(1), b->get_index_in_group());
    CHECK_EQUAL(d_spec.get_opposite_link_table_ndx(1), c->get_index_in_group());
}


namespace {

void setup_table(TestTableGroup::Ref t)
{
    t->add("a",  1, true, Wed);
    t->add("b", 15, true, Wed);
    t->add("ccc", 10, true, Wed);
    t->add("dddd", 20, true, Wed);
}

} // anonymous namespace


TEST(Group_Equal)
{
    Group g1, g2;
    CHECK(g1 == g2);
    TestTableGroup::Ref t1 = g1.add_table<TestTableGroup>("TABLE1");
    CHECK_NOT(g1 == g2);
    setup_table(t1);
    TestTableGroup::Ref t2 = g2.add_table<TestTableGroup>("TABLE1");
    setup_table(t2);
    CHECK(g1 == g2);
    t2->add("hey", 2, false, Thu);
    CHECK(g1 != g2);
}


TEST(Group_TableAccessorLeftBehind)
{
    TableRef table;
    TableRef subtable;
    {
        Group group;
        table = group.add_table("test");
        CHECK(table->is_attached());
        table->add_column(type_Table, "sub");
        table->add_empty_row();
        subtable = table->get_subtable(0,0);
        CHECK(subtable->is_attached());
    }
    CHECK(!table->is_attached());
    CHECK(!subtable->is_attached());
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
    const size_t size = strlen(str);
    char* const data = new char[strlen(str)];
    std::copy(str, str+size, data);
    CHECK_THROW(Group(BinaryData(data, size)), InvalidDatabase);
    delete[] data;
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
        TestTableGroup::Ref t = from_disk.add_table<TestTableGroup>("test");

        CHECK_EQUAL(4, t->get_column_count());
        CHECK_EQUAL(0, t->size());

        // Modify table
        t->add("Test",  1, true, Wed);

        CHECK_EQUAL("Test", t[0].first);
        CHECK_EQUAL(1,      t[0].second);
        CHECK_EQUAL(true,   t[0].third);
        CHECK_EQUAL(Wed,    t[0].fourth);
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
        TestTableGroup::Ref table = to_disk.add_table<TestTableGroup>("test");
        table->add("",  1, true, Wed);
        table->add("", 15, true, Wed);
        table->add("", 10, true, Wed);
        table->add("", 20, true, Wed);
        table->add("", 11, true, Wed);
        table->add("", 45, true, Wed);
        table->add("", 10, true, Wed);
        table->add("",  0, true, Wed);
        table->add("", 30, true, Wed);
        table->add("",  9, true, Wed);

#ifdef REALM_DEBUG
        to_disk.verify();
#endif

        // Serialize to disk
        to_disk.write(path, crypt_key());

        // Load the table
        Group from_disk(path, crypt_key());
        TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");

        CHECK_EQUAL(4, t->get_column_count());
        CHECK_EQUAL(10, t->size());

        // Verify that original values are there
        CHECK(*table == *t);

        // Modify both tables
        table[0].first = "test";
        t[0].first = "test";
        table->insert(5, "hello", 100, false, Mon);
        t->insert(5, "hello", 100, false, Mon);
        table->remove(1);
        t->remove(1);

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
    TestTableGroup::Ref table1 = to_disk.add_table<TestTableGroup>("test1");
    table1->add("",  1, true, Wed);
    table1->add("", 15, true, Wed);
    table1->add("", 10, true, Wed);

    TestTableGroup::Ref table2 = to_disk.add_table<TestTableGroup>("test2");
    table2->add("hey",  0, true, Tue);
    table2->add("hello", 3232, false, Sun);

#ifdef REALM_DEBUG
    to_disk.verify();
#endif

    // Serialize to disk
    to_disk.write(path, crypt_key());

    // Load the tables
    Group from_disk(path, crypt_key());
    TestTableGroup::Ref t1 = from_disk.get_table<TestTableGroup>("test1");
    TestTableGroup::Ref t2 = from_disk.get_table<TestTableGroup>("test2");

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
    TestTableGroup::Ref table = to_disk.add_table<TestTableGroup>("test");
    table->add("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1",  1, true, Wed);
    table->add("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15, true, Wed);

#ifdef REALM_DEBUG
    to_disk.verify();
#endif

    // Serialize to disk
    to_disk.write(path, crypt_key());

    // Load the table
    Group from_disk(path, crypt_key());
    TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");

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
    TestTableGroup::Ref table = to_mem.add_table<TestTableGroup>("test");
    table->add("",  1, true, Wed);
    table->add("", 15, true, Wed);
    table->add("", 10, true, Wed);
    table->add("", 20, true, Wed);
    table->add("", 11, true, Wed);
    table->add("", 45, true, Wed);
    table->add("", 10, true, Wed);
    table->add("",  0, true, Wed);
    table->add("", 30, true, Wed);
    table->add("",  9, true, Wed);

#ifdef REALM_DEBUG
    to_mem.verify();
#endif

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");

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
    TestTableGroup::Ref table = to_mem.add_table<TestTableGroup>("test");
    table->add("",  1, true, Wed);
    table->add("",  2, true, Wed);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    Group from_mem(buffer);
}


TEST(Group_Serialize_Optimized)
{
    // Create group with one table
    Group to_mem;
    TestTableGroup::Ref table = to_mem.add_table<TestTableGroup>("test");

    for (size_t i = 0; i < 5; ++i) {
        table->add("abd",     1, true, Mon);
        table->add("eftg",    2, true, Tue);
        table->add("hijkl",   5, true, Wed);
        table->add("mnopqr",  8, true, Thu);
        table->add("stuvxyz", 9, true, Fri);
    }

    table->optimize();

#ifdef REALM_DEBUG
    to_mem.verify();
#endif

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());

    // Verify that original values are there
    CHECK(*table == *t);

    // Add a row with a known (but unique) value
    table->add("search_target", 9, true, Fri);

    const size_t res = table->column().first.find_first("search_target");
    CHECK_EQUAL(table->size()-1, res);

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

    table->add_column(type_Int,      "int");
    table->add_column(type_Bool,     "bool");
    table->add_column(type_DateTime, "date");
    table->add_column(type_String,   "string");
    table->add_column(type_Binary,   "binary");
    table->add_column(type_Mixed,    "mixed");

    table->insert_empty_row(0);
    table->set_int(0, 0, 12);
    table->set_bool(1, 0, true);
    table->set_datetime(2, 0, 12345);
    table->set_string(3, 0, "test");
    table->set_binary(4, 0, BinaryData("binary", 7));
    table->set_mixed(5, 0, false);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");

    CHECK_EQUAL(6, t->get_column_count());
    CHECK_EQUAL(1, t->size());
    CHECK_EQUAL(12, t->get_int(0, 0));
    CHECK_EQUAL(true, t->get_bool(1, 0));
    CHECK_EQUAL(12345, t->get_datetime(2, 0));
    CHECK_EQUAL("test", t->get_string(3, 0));
    CHECK_EQUAL(7, t->get_binary(4, 0).size());
    CHECK_EQUAL("binary", t->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, t->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, t->get_mixed(5, 0).get_bool());
}

TEST(Group_Persist)
{
    GROUP_TEST_PATH(path);

    // Create new database
    Group db(path, crypt_key(), Group::mode_ReadWrite);

    // Insert some data
    TableRef table = db.add_table("test");
    table->add_column(type_Int,      "int");
    table->add_column(type_Bool,     "bool");
    table->add_column(type_DateTime, "date");
    table->add_column(type_String,   "string");
    table->add_column(type_Binary,   "binary");
    table->add_column(type_Mixed,    "mixed");
    table->insert_empty_row(0);
    table->set_int(0, 0, 12);
    table->set_bool(1, 0, true);
    table->set_datetime(2, 0, 12345);
    table->set_string(3, 0, "test");
    table->set_binary(4, 0, BinaryData("binary", 7));
    table->set_mixed(5, 0, false);

    // Write changes to file
    db.commit();

#ifdef REALM_DEBUG
    db.verify();
#endif

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(12345, table->get_datetime(2, 0));
    CHECK_EQUAL("test", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).size());
    CHECK_EQUAL("binary", table->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());

    // Change a bit
    table->set_string(3, 0, "Changed!");

    // Write changes to file
    db.commit();

#ifdef REALM_DEBUG
    db.verify();
#endif

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(12345, table->get_datetime(2, 0));
    CHECK_EQUAL("Changed!", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).size());
    CHECK_EQUAL("binary", table->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());
}


TEST(Group_Subtable)
{
    GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);

    int n = 1;

    Group g;
    TableRef table = g.add_table("test");
    DescriptorRef sub;
    table->add_column(type_Int,   "foo");
    table->add_column(type_Table, "sub", &sub);
    table->add_column(type_Mixed, "baz");
    sub->add_column(type_Int, "bar");
    sub.reset();

    for (int i=0; i<n; ++i) {
        table->add_empty_row();
        table->set_int(0, i, 100+i);
        if (i%2 == 0) {
            TableRef st = table->get_subtable(1,i);
            st->add_empty_row();
            st->set_int(0, 0, 200+i);
        }
        if (i%3 == 1) {
            table->set_mixed(2, i, Mixed::subtable_tag());
            TableRef st = table->get_subtable(2,i);
            st->add_column(type_Int, "banach");
            st->add_empty_row();
            st->set_int(0, 0, 700+i);
        }
    }

    CHECK_EQUAL(n, table->size());

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(100+i, table->get_int(0,i));
        {
            TableRef st = table->get_subtable(1,i);
            CHECK_EQUAL(i%2 == 0 ? 1 : 0, st->size());
            if (i%2 == 0)
                CHECK_EQUAL(200+i, st->get_int(0,0));
            if (i%3 == 0) {
                st->add_empty_row();
                st->set_int(0, st->size()-1, 300+i);
            }
        }
        CHECK_EQUAL(i%3 == 1 ? type_Table : type_Int, table->get_mixed_type(2,i));
        if (i%3 == 1) {
            TableRef st = table->get_subtable(2,i);
            CHECK_EQUAL(1, st->size());
            CHECK_EQUAL(700+i, st->get_int(0,0));
        }
        if (i%8 == 3) {
            if (i%3 != 1)
                table->set_mixed(2, i, Mixed::subtable_tag());
            TableRef st = table->get_subtable(2,i);
            if (i%3 != 1)
                st->add_column(type_Int, "banach");
            st->add_empty_row();
            st->set_int(0, st->size()-1, 800+i);
        }
    }

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(100+i, table->get_int(0,i));
        {
            TableRef st = table->get_subtable(1,i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(200+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(300+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
        CHECK_EQUAL(i%3 == 1 || i%8 == 3 ? type_Table : type_Int, table->get_mixed_type(2,i));
        if (i%3 == 1 || i%8 == 3) {
            TableRef st = table->get_subtable(2,i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(700+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(800+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
    }

    g.write(path_1, crypt_key());

    // Read back tables
    Group g2(path_1, crypt_key());
    TableRef table2 = g2.get_table("test");

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(100+i, table2->get_int(0,i));
        {
            TableRef st = table2->get_subtable(1,i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(200+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(300+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%5 == 0) {
                st->add_empty_row();
                st->set_int(0, st->size()-1, 400+i);
            }
        }
        CHECK_EQUAL(i%3 == 1 || i%8 == 3 ? type_Table : type_Int, table2->get_mixed_type(2,i));
        if (i%3 == 1 || i%8 == 3) {
            TableRef st = table2->get_subtable(2,i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(700+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(800+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
        if (i%7 == 4) {
            if (i%3 != 1 && i%8 != 3)
                table2->set_mixed(2, i, Mixed::subtable_tag());
            TableRef st = table2->get_subtable(2,i);
            if (i%3 != 1 && i%8 != 3)
                st->add_column(type_Int, "banach");
            st->add_empty_row();
            st->set_int(0, st->size()-1, 900+i);
        }
    }

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(100+i, table2->get_int(0,i));
        {
            TableRef st = table2->get_subtable(1,i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(200+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(300+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%5 == 0) {
                CHECK_EQUAL(400+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
        CHECK_EQUAL(i%3 == 1 || i%8 == 3 || i%7 == 4 ? type_Table : type_Int, table2->get_mixed_type(2,i));
        if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
            TableRef st = table2->get_subtable(2,i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(700+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(800+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%7 == 4) {
                CHECK_EQUAL(900+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
    }

    g2.write(path_2, crypt_key());

    // Read back tables
    Group g3(path_2, crypt_key());
    TableRef table3 = g2.get_table("test");

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(100+i, table3->get_int(0,i));
        {
            TableRef st = table3->get_subtable(1,i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(200+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(300+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%5 == 0) {
                CHECK_EQUAL(400+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
        CHECK_EQUAL(i%3 == 1 || i%8 == 3 || i%7 == 4 ? type_Table : type_Int, table3->get_mixed_type(2,i));
        if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
            TableRef st = table3->get_subtable(2,i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
            CHECK_EQUAL(expected_size, st->size());
            size_t ndx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(700+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(800+i, st->get_int(0, ndx));
                ++ndx;
            }
            if (i%7 == 4) {
                CHECK_EQUAL(900+i, st->get_int(0, ndx));
                ++ndx;
            }
        }
    }
}


TEST(Group_MultiLevelSubtables)
{
    GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);
    GROUP_TEST_PATH(path_3);
    GROUP_TEST_PATH(path_4);
    GROUP_TEST_PATH(path_5);

    {
        Group g;
        TableRef table = g.add_table("test");
        {
            DescriptorRef sub_1, sub_2;
            table->add_column(type_Int,   "int");
            table->add_column(type_Table, "tab", &sub_1);
            table->add_column(type_Mixed, "mix");
            sub_1->add_column(type_Int,   "int");
            sub_1->add_column(type_Table, "tab", &sub_2);
            sub_2->add_column(type_Int,   "int");
        }
        table->add_empty_row();
        {
            TableRef a = table->get_subtable(1, 0);
            a->add_empty_row();
            TableRef b = a->get_subtable(1, 0);
            b->add_empty_row();
        }
        {
            table->set_mixed(2, 0, Mixed::subtable_tag());
            TableRef a = table->get_subtable(2, 0);
            a->add_column(type_Int,   "int");
            a->add_column(type_Mixed, "mix");
            a->add_empty_row();
            a->set_mixed(1, 0, Mixed::subtable_tag());
            TableRef b = a->get_subtable(1, 0);
            b->add_column(type_Int, "int");
            b->add_empty_row();
        }
        g.write(path_1, crypt_key());
    }

    // Non-mixed
    {
        Group g(path_1, crypt_key());
        TableRef table = g.get_table("test");
        // Get A as subtable
        TableRef a = table->get_subtable(1, 0);
        // Get B as subtable from A
        TableRef b = a->get_subtable(1, 0);
        // Modify B
        b->set_int(0, 0, 6661012);
        // Modify A
        a->set_int(0, 0, 6661011);
        // Modify top
        table->set_int(0, 0, 6661010);
        // Get a second ref to A (compare)
        CHECK_EQUAL(table->get_subtable(1, 0), a);
        CHECK_EQUAL(table->get_subtable(1, 0)->get_int(0,0), 6661011);
        // get a second ref to B (compare)
        CHECK_EQUAL(a->get_subtable(1, 0), b);
        CHECK_EQUAL(a->get_subtable(1, 0)->get_int(0,0), 6661012);
        g.write(path_2, crypt_key());
    }
    {
        Group g(path_2, crypt_key());
        TableRef table = g.get_table("test");
        // Get A as subtable
        TableRef a = table->get_subtable(1, 0);
        // Get B as subtable from A
        TableRef b = a->get_subtable(1, 0);
        // Drop reference to A
        a = TableRef();
        // Modify B
        b->set_int(0, 0, 6661013);
        // Get a third ref to A (compare)
        a = table->get_subtable(1, 0);
        CHECK_EQUAL(table->get_subtable(1, 0)->get_int(0,0), 6661011);
        // Get third ref to B and verify last mod
        b = a->get_subtable(1, 0);
        CHECK_EQUAL(a->get_subtable(1, 0)->get_int(0,0), 6661013);
        g.write(path_3, crypt_key());
    }

    // Mixed
    {
        Group g(path_3, crypt_key());
        TableRef table = g.get_table("test");
        // Get A as subtable
        TableRef a = table->get_subtable(2, 0);
        // Get B as subtable from A
        TableRef b = a->get_subtable(1, 0);
        // Modify B
        b->set_int(0, 0, 6661012);
        // Modify A
        a->set_int(0, 0, 6661011);
        // Modify top
        table->set_int(0, 0, 6661010);
        // Get a second ref to A (compare)
        CHECK_EQUAL(table->get_subtable(2, 0), a);
        CHECK_EQUAL(table->get_subtable(2, 0)->get_int(0,0), 6661011);
        // get a second ref to B (compare)
        CHECK_EQUAL(a->get_subtable(1, 0), b);
        CHECK_EQUAL(a->get_subtable(1, 0)->get_int(0,0), 6661012);
        g.write(path_4, crypt_key());
    }
    {
        Group g(path_4, crypt_key());
        TableRef table = g.get_table("test");
        // Get A as subtable
        TableRef a = table->get_subtable(2, 0);
        // Get B as subtable from A
        TableRef b = a->get_subtable(1, 0);
        // Drop reference to A
        a = TableRef();
        // Modify B
        b->set_int(0, 0, 6661013);
        // Get a third ref to A (compare)
        a = table->get_subtable(2, 0);
        CHECK_EQUAL(table->get_subtable(2, 0)->get_int(0,0), 6661011);
        // Get third ref to B and verify last mod
        b = a->get_subtable(1, 0);
        CHECK_EQUAL(a->get_subtable(1, 0)->get_int(0,0), 6661013);
        g.write(path_5, crypt_key());
    }
}


TEST(Group_CommitSubtable)
{
    GROUP_TEST_PATH(path);
    Group group(path, crypt_key(), Group::mode_ReadWrite);

    TableRef table = group.add_table("test");
    DescriptorRef sub_1;
    table->add_column(type_Table, "subtable", &sub_1);
    sub_1->add_column(type_Int,   "int");
    sub_1.reset();
    table->add_empty_row();

    TableRef subtable = table->get_subtable(0,0);
    subtable->add_empty_row();

    group.commit();

    table->add_empty_row();
    group.commit();

    subtable = table->get_subtable(0,0);
    subtable->add_empty_row();
    group.commit();

    table->add_empty_row();
    subtable = table->get_subtable(0,0);
    subtable->add_empty_row();
    group.commit();
}


TEST(Group_CommitSubtableMixed)
{
    GROUP_TEST_PATH(path);
    Group group(path, crypt_key(), Group::mode_ReadWrite);

    TableRef table = group.add_table("test");
    table->add_column(type_Mixed, "mixed");

    table->add_empty_row();

    table->clear_subtable(0,0);
    TableRef subtable = table->get_subtable(0,0);
    subtable->add_column(type_Int, "int");
    subtable->add_empty_row();

    group.commit();

    table->add_empty_row();
    group.commit();

    subtable = table->get_subtable(0,0);
    subtable->add_empty_row();
    group.commit();

    table->add_empty_row();
    subtable = table->get_subtable(0,0);
    subtable->add_empty_row();
    group.commit();
}


TEST(Group_CommitDegenerateSubtable)
{
    GROUP_TEST_PATH(path);
    Group group(path, crypt_key(), Group::mode_ReadWrite);
    TableRef table = group.add_table("parent");
    table->add_column(type_Table, "");
    table->get_subdescriptor(0)->add_column(type_Int, "");
    table->add_empty_row();
    TableRef subtab = table->get_subtable(0,0);
    CHECK(subtab->is_degenerate());
    group.commit();
    CHECK(subtab->is_degenerate());
}


TEST(Group_InvalidateTables)
{
    TestTableGroup2::Ref table;
    TableRef             subtable1;
    TestTableGroup::Ref  subtable2;
    TestTableGroup::Ref  subtable3;
    {
        Group group;
        table = group.add_table<TestTableGroup2>("foo");
        CHECK(table->is_attached());
        table->add(Mixed::subtable_tag(), 0, 0);
        CHECK(table->is_attached());
        subtable1 = table[0].first.get_subtable();
        CHECK(table->is_attached());
        CHECK(subtable1);
        CHECK(subtable1->is_attached());
        subtable2 = table[0].second;
        CHECK(table->is_attached());
        CHECK(subtable1->is_attached());
        CHECK(subtable2);
        CHECK(subtable2->is_attached());
        subtable3 = table[0].third;
        CHECK(table->is_attached());
        CHECK(subtable1->is_attached());
        CHECK(subtable2->is_attached());
        CHECK(subtable3);
        CHECK(subtable3->is_attached());
        subtable3->add("alpha", 79542, true,  Wed);
        subtable3->add("beta",     97, false, Mon);
        CHECK(table->is_attached());
        CHECK(subtable1->is_attached());
        CHECK(subtable2->is_attached());
        CHECK(subtable3->is_attached());
    }
    CHECK(!table->is_attached());
    CHECK(!subtable1->is_attached());
    CHECK(!subtable2->is_attached());
    CHECK(!subtable3->is_attached());
}


TEST(Group_ToJSON)
{
    Group g;
    TestTableGroup::Ref table = g.add_table<TestTableGroup>("test");

    table->add("jeff", 1, true, Wed);
    table->add("jim",  1, true, Wed);
    std::ostringstream out;
    g.to_json(out);
    std::string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("{\"test\":[{\"first\":\"jeff\",\"second\":1,\"third\":true,\"fourth\":2},{\"first\":\"jim\",\"second\":1,\"third\":true,\"fourth\":2}]}", str);
}


TEST(Group_ToString)
{
    Group g;
    TestTableGroup::Ref table = g.add_table<TestTableGroup>("test");

    table->add("jeff", 1, true, Wed);
    table->add("jim",  1, true, Wed);
    std::ostringstream out;
    g.to_string(out);
    std::string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("     tables     rows  \n   0 test       2     \n", str.c_str());
}


TEST(Group_IndexString)
{
    Group to_mem;
    TestTableGroup::Ref table = to_mem.add_table<TestTableGroup>("test");

    table->add("jeff",     1, true, Wed);
    table->add("jim",      1, true, Wed);
    table->add("jennifer", 1, true, Wed);
    table->add("john",     1, true, Wed);
    table->add("jimmy",    1, true, Wed);
    table->add("jimbo",    1, true, Wed);
    table->add("johnny",   1, true, Wed);
    table->add("jennifer", 1, true, Wed); //duplicate

    table->column().first.add_search_index();
    CHECK(table->column().first.has_search_index());

    size_t r1 = table->column().first.find_first("jimmi");
    CHECK_EQUAL(not_found, r1);

    size_t r2 = table->column().first.find_first("jeff");
    size_t r3 = table->column().first.find_first("jim");
    size_t r4 = table->column().first.find_first("jimbo");
     size_t r5 = table->column().first.find_first("johnny");
    CHECK_EQUAL(0, r2);
    CHECK_EQUAL(1, r3);
    CHECK_EQUAL(5, r4);
    CHECK_EQUAL(6, r5);

    size_t c1 = table->column().first.count("jennifer");
    CHECK_EQUAL(2, c1);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");
    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(8, t->size());

    CHECK(t->column().first.has_search_index());

    size_t m1 = t->column().first.find_first("jimmi");
    CHECK_EQUAL(not_found, m1);

    size_t m2 = t->column().first.find_first("jeff");
    size_t m3 = t->column().first.find_first("jim");
    size_t m4 = t->column().first.find_first("jimbo");
    size_t m5 = t->column().first.find_first("johnny");
    CHECK_EQUAL(0, m2);
    CHECK_EQUAL(1, m3);
    CHECK_EQUAL(5, m4);
    CHECK_EQUAL(6, m5);

    size_t m6 = t->column().first.count("jennifer");
    CHECK_EQUAL(2, m6);

    // Remove the search index and verify
    t->column().first.remove_search_index();
    CHECK(!t->column().first.has_search_index());
    from_mem.verify();

    size_t m7 = t->column().first.find_first("jimmi");
    size_t m8 = t->column().first.find_first("johnny");
    CHECK_EQUAL(not_found, m7);
    CHECK_EQUAL(6, m8);
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
    table->add_column(type_String, "ticker");

    for (size_t i = 0; i < 100; ++i) {
        table->verify();
        table->insert_empty_row(i);
        table->set_string(0, i, "123456789012345678901234567890123456789");
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
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0,0);
    link_list->add(0);
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
    t->add_column(type_Int, "integer");

    for (size_t i = 0; i < 200; i++) {
        t->add_empty_row();
        t->set_int(0, i, (i + 1) * 0xeeeeeeeeeeeeeeeeULL);
    }

    t->add_search_index(0);

    // This would always work
    CHECK(t->find_first_int(0, (0 + 1) * 0xeeeeeeeeeeeeeeeeULL) == 0);

    g.commit();

    // This would fail (sometimes return not_found, sometimes crash)
    CHECK(t->find_first_int(0, (0 + 1) * 0xeeeeeeeeeeeeeeeeULL) == 0);
}


TEST(Group_CascadeNotify_Simple)
{
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");

    // Add some extra rows so that the indexes being tested aren't all 0
    t->add_empty_row(100);

    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification&) {
        called = true;
    });
    t->remove(5);
    CHECK(called);

    // move_last_over() on a table with no (back)links just sends that single
    // row in the notification
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(5, notification.rows[0].row_ndx);
    });
    t->move_last_over(5);
    CHECK(called);

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *t);
    origin->add_column_link(type_LinkList, "linklist", *t);

    origin->add_empty_row(100);

    // calling remove() is now an error, so no more tests of it

    // move_last_over() on an un-linked-to row should still just send that row
    // in the notification
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(5, notification.rows[0].row_ndx);
    });
    t->move_last_over(5);
    CHECK(called);

    // move_last_over() on a linked-to row should send information about the
    // links which had linked to it
    origin->set_link(0, 10, 11); // rows are arbitrarily different to make things less likely to pass by coincidence
    LinkViewRef lv = origin->get_linklist(1, 15);
    lv->add(11);
    lv->add(30);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(11, notification.rows[0].row_ndx);

        CHECK_EQUAL(2, notification.links.size());
        CHECK_EQUAL(0, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(10, notification.links[0].origin_row_ndx);
        CHECK_EQUAL(11, notification.links[0].old_target_row_ndx);

        CHECK_EQUAL(1, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(15, notification.links[1].origin_row_ndx);
        CHECK_EQUAL(11, notification.links[1].old_target_row_ndx);
    });
    t->move_last_over(11);
    CHECK(called);

    // move_last_over() on the origin table just sends the row being removed
    // because the links are weak
    origin->set_link(0, 10, 11);
    origin->get_linklist(1, 10)->add(11);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(1, notification.rows.size());
        CHECK_EQUAL(1, notification.rows[0].table_ndx);
        CHECK_EQUAL(10, notification.rows[0].row_ndx);

        CHECK_EQUAL(0, notification.links.size());
    });
    origin->move_last_over(10);
    CHECK(called);

    // move_last_over() on the origin table with strong links lists the target
    // rows that are removed
    origin->get_descriptor()->set_link_type(0, link_Strong);
    origin->get_descriptor()->set_link_type(1, link_Strong);

    origin->set_link(0, 10, 50);
    origin->set_link(0, 11, 62);
    lv = origin->get_linklist(1, 10);
    lv->add(60);
    lv->add(61);
    lv->add(61);
    lv->add(62);
    // 50, 60 and 61 should be removed; 62 should not as there's still a strong link
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(4, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(50, notification.rows[0].row_ndx);
        CHECK_EQUAL(0, notification.rows[1].table_ndx);
        CHECK_EQUAL(60, notification.rows[1].row_ndx);
        CHECK_EQUAL(0, notification.rows[2].table_ndx);
        CHECK_EQUAL(61, notification.rows[2].row_ndx);
        CHECK_EQUAL(1, notification.rows[3].table_ndx);
        CHECK_EQUAL(10, notification.rows[3].row_ndx);

        CHECK_EQUAL(0, notification.links.size());
    });
    origin->move_last_over(10);
    CHECK(called);

    g.set_cascade_notification_handler(nullptr);
    t->clear();
    origin->clear();
    t->add_empty_row(100);
    origin->add_empty_row(100);

    // Indirect nullifications: move_last_over() on a row with the last strong
    // links to a row that still has weak links to it
    origin->add_column_link(type_Link, "link2", *t);
    origin->add_column_link(type_LinkList, "linklist2", *t);

    CHECK_EQUAL(0, t->get_backlink_count(30, *origin, 0));
    CHECK_EQUAL(0, t->get_backlink_count(30, *origin, 1));
    CHECK_EQUAL(0, t->get_backlink_count(30, *origin, 2));
    CHECK_EQUAL(0, t->get_backlink_count(30, *origin, 3));
    origin->set_link(0, 20, 30);
    origin->get_linklist(1, 20)->add(31);
    origin->set_link(2, 25, 31);
    origin->get_linklist(3, 25)->add(30);
    CHECK_EQUAL(1, t->get_backlink_count(30, *origin, 0));
    CHECK_EQUAL(1, t->get_backlink_count(31, *origin, 1));
    CHECK_EQUAL(1, t->get_backlink_count(31, *origin, 2));
    CHECK_EQUAL(1, t->get_backlink_count(30, *origin, 3));

    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(3, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(30, notification.rows[0].row_ndx);
        CHECK_EQUAL(0, notification.rows[1].table_ndx);
        CHECK_EQUAL(31, notification.rows[1].row_ndx);
        CHECK_EQUAL(1, notification.rows[2].table_ndx);
        CHECK_EQUAL(20, notification.rows[2].row_ndx);

        CHECK_EQUAL(2, notification.links.size());
        CHECK_EQUAL(3, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(25, notification.links[0].origin_row_ndx);
        CHECK_EQUAL(30, notification.links[0].old_target_row_ndx);

        CHECK_EQUAL(2, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(25, notification.links[1].origin_row_ndx);
        CHECK_EQUAL(31, notification.links[1].old_target_row_ndx);
    });
    origin->move_last_over(20);
    CHECK(called);
}


TEST(Group_CascadeNotify_TableClear)
{
    GROUP_TEST_PATH(path);

    Group g(path, 0, Group::mode_ReadWrite);
    TableRef t = g.add_table("target");
    t->add_column(type_Int, "int");

    t->add_empty_row(10);

    // clear() does not list the rows in the table being cleared because it
    // would be expensive and mostly pointless to do so
    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(0, notification.rows.size());
    });
    t->clear();
    CHECK(called);

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *t);
    origin->add_column_link(type_LinkList, "linklist", *t);

    t->add_empty_row(10);
    origin->add_empty_row(10);

    // clear() does report nullified links
    origin->set_link(0, 1, 2);
    origin->get_linklist(1, 3)->add(4);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.rows.size());

        CHECK_EQUAL(2, notification.links.size());
        CHECK_EQUAL(0, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(1, notification.links[0].origin_row_ndx);
        CHECK_EQUAL(2, notification.links[0].old_target_row_ndx);

        CHECK_EQUAL(1, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(3, notification.links[1].origin_row_ndx);
        CHECK_EQUAL(4, notification.links[1].old_target_row_ndx);
    });
    t->clear();
    CHECK(called);

    t->add_empty_row(10);
    origin->add_empty_row(10);

    // and cascaded deletions
    origin->get_descriptor()->set_link_type(0, link_Strong);
    origin->get_descriptor()->set_link_type(1, link_Strong);
    origin->set_link(0, 1, 2);
    origin->get_linklist(1, 3)->add(4);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(2, notification.rows.size());
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(2, notification.rows[0].row_ndx);
        CHECK_EQUAL(0, notification.rows[1].table_ndx);
        CHECK_EQUAL(4, notification.rows[1].row_ndx);
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

    t->add_empty_row(10);

    // No link columns, so remove() is used
    // Unlike clearing a table, the rows removed by the clear() are included in
    // the notification so that cascaded deletions and direct deletions don't
    // need to be handled separately
    bool called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(10, notification.rows.size());
    });
    t->where().find_all().clear();
    CHECK(called);

    // Add another table which links to the target table
    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *t);
    origin->add_column_link(type_LinkList, "linklist", *t);

    // Now has backlinks, so move_last_over() is used
    t->add_empty_row(10);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(10, notification.rows.size());
    });
    t->where().find_all().clear();
    CHECK(called);

    t->add_empty_row(10);
    origin->add_empty_row(10);

    // should list which links were nullified
    origin->set_link(0, 1, 2);
    origin->get_linklist(1, 3)->add(4);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(10, notification.rows.size());
        CHECK_EQUAL(2, notification.links.size());

        CHECK_EQUAL(0, notification.links[0].origin_col_ndx);
        CHECK_EQUAL(1, notification.links[0].origin_row_ndx);
        CHECK_EQUAL(2, notification.links[0].old_target_row_ndx);

        CHECK_EQUAL(1, notification.links[1].origin_col_ndx);
        CHECK_EQUAL(3, notification.links[1].origin_row_ndx);
        CHECK_EQUAL(4, notification.links[1].old_target_row_ndx);
    });
    t->where().find_all().clear();
    CHECK(called);

    g.set_cascade_notification_handler(nullptr);
    origin->clear();
    t->add_empty_row(10);
    origin->add_empty_row(10);

    // should included cascaded deletions
    origin->get_descriptor()->set_link_type(0, link_Strong);
    origin->get_descriptor()->set_link_type(1, link_Strong);
    origin->set_link(0, 1, 2);
    origin->get_linklist(1, 3)->add(4);
    called = false;
    g.set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        called = true;
        CHECK_EQUAL(0, notification.links.size());
        CHECK_EQUAL(12, notification.rows.size()); // 10 from origin, 2 from target
        CHECK_EQUAL(0, notification.rows[0].table_ndx);
        CHECK_EQUAL(2, notification.rows[0].row_ndx);
        CHECK_EQUAL(0, notification.rows[1].table_ndx);
        CHECK_EQUAL(4, notification.rows[1].row_ndx);
    });
    origin->where().find_all().clear();
    CHECK(called);
}


TEST(Group_Fuzzy)
{
    // Either provide a crash file generated by AFL to reproduce a crash, or leave it blank in order to run
    // a very simple fuzz test that just uses a random generator for generating Realm actions.
    string filename = "";
    //string filename = "/findings/hangs/id:000041,src:000000,op:havoc,rep:64";
//    string filename = "d:/crash3";

    // Predeclarations for methods in fuzz_group.cpp
    int run_fuzzy(int argc, const char* argv[]);
    void parse_and_apply_instructions(string& in, Group& g, util::Optional<std::ostream&> log);

    string instr;
    if (filename != "") {
        const char* tmp[] = { "", filename.c_str(), "--log" };
        run_fuzzy(sizeof(tmp) / sizeof(tmp[0]), tmp);
    }
    else {
        // Number of fuzzy tests
        const size_t iterations = 10;

        // Number of instructions in each test
        const size_t instructions = 100;

        srand(time(0));
        for (int64_t counter = 0; counter < iterations; counter++)
        {
            instr = "";
            for (size_t t = 0; t < instructions; t++) {
                instr += static_cast<char>(fastrand());
            }
            Group g;
            parse_and_apply_instructions(instr, g, util::none);
        }
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
    s.add_column(type_Int,      "int");
    s.add_column(type_Bool,     "bool");
    s.add_column(type_DateTime, "date");
    s.add_column(type_String,   "string");
    s.add_column(type_String,   "string_long");
    s.add_column(type_String,   "string_enum"); // becomes StringEnumColumn
    s.add_column(type_Binary,   "binary");
    s.add_column(type_Mixed,    "mixed");
    s.add_column(type_Table,    "tables", &subdesc);
    subdesc->add_column(type_Int,    "sub_first");
    subdesc->add_column(type_String, "sub_second");

    // Add some rows
    for (size_t i = 0; i < 15; ++i) {
        table->insert_empty_row(i);
        table->set_int(0, i, i);
        table->set_bool(1, i, (i % 2 ? true : false));
        table->set_datetime(2, i, 12345);

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

        table->set_binary(6, i, "binary", 7);

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

        table->set_subtable(8, i);

        // Add sub-tables
        if (i == 2) {
            // To mixed column
            table->set_mixed(7, i, Mixed(type_Table));
            Table subtable = table->GetMixedTable(7, i);

            Spec s = subtable->get_spec();
            s.add_column(type_Int,    "first");
            s.add_column(type_String, "second");
            subtable->UpdateFromSpec(s.get_ref());

            subtable->insert_empty_row(0);
            subtable->set_int(0, 0, 42);
            subtable->set_string(1, 0, "meaning");

            // To table column
            Table subtable2 = table->get_subtable(8, i);
            subtable2->set_empty_row(0);
            subtable2->set_int(0, 0, 42);
            subtable2->set_string(1, 0, "meaning");
        }
    }

    // We also want StringEnumColumn's
    table->optimize();

#if 1
    // Write array graph to std::cout
    std::stringstream ss;
    mygroup.ToDot(ss);
    std::cout << ss.str() << std::endl;
#endif

    // Write array graph to file in dot format
    std::ofstream fs("realm_graph.dot", ios::out | ios::binary);
    if (!fs.is_open())
        std::cout << "file open error " << strerror << std::endl;
    mygroup.to_dot(fs);
    fs.close();
}

#endif // REALM_TO_DOT
#endif // REALM_DEBUG

#endif // TEST_GROUP
