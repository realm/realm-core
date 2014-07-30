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
typedef int mode_t;
static const mode_t S_IWUSR = mode_t(_S_IWRITE);
static const mode_t MS_MODE_MASK = 0x0000ffff;
#endif

#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

#include "test.hpp"

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


namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

TIGHTDB_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

} // Anonymous namespace


TEST(Group_Unattached)
{
    Group group((Group::unattached_tag()));
    CHECK(!group.is_attached());
}


TEST(Group_OpenFile)
{
    GROUP_TEST_PATH(path);

    {
        Group group((Group::unattached_tag()));
        group.open(path, Group::mode_ReadWrite);
        CHECK(group.is_attached());
    }

    {
        Group group((Group::unattached_tag()));
        group.open(path, Group::mode_ReadWriteNoCreate);
        CHECK(group.is_attached());
    }

    {
        Group group((Group::unattached_tag()));
        group.open(path, Group::mode_ReadOnly);
        CHECK(group.is_attached());
    }
}


TEST(Group_Permissions)
{
#ifndef _WIN32
    if(getuid() == 0) {
        cout << "Group_Permissions test skipped because you are running it as root\n\n";
        return;
    }
#endif

    GROUP_TEST_PATH(path);
    {
        Group group1;
        TableRef t1 = group1.get_table("table1");
        t1->add_column(type_String, "s");
        t1->add_column(type_Int,    "i");
        for(size_t i=0; i<4; ++i) {
            t1->insert_string(0, i, "a");
            t1->insert_int(1, i, 3);
            t1->insert_done();
        }
        group1.write(path);
    }

#ifdef _WIN32
    _chmod(path.c_str(), S_IWUSR & MS_MODE_MASK);
#else
    chmod(path.c_str(), S_IWUSR);
#endif

    {
        Group group2((Group::unattached_tag()));
        CHECK_THROW(group2.open(path, Group::mode_ReadOnly), File::PermissionDenied);
        CHECK(!group2.has_table("table1"));  // is not attached
    }
}



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
        CHECK_THROW(group.open(path_1, Group::mode_ReadOnly), InvalidDatabase);
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, Group::mode_ReadOnly), InvalidDatabase); // Again
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, Group::mode_ReadWrite), InvalidDatabase);
        CHECK(!group.is_attached());
        CHECK_THROW(group.open(path_1, Group::mode_ReadWriteNoCreate), InvalidDatabase);
        CHECK(!group.is_attached());
        group.open(path_2, Group::mode_ReadWrite); // This one must work
        CHECK(group.is_attached());
    }
}


TEST(Group_OpenBuffer)
{
    // Produce a valid buffer
    UniquePtr<char[]> buffer;
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
        group.open(path, Group::mode_ReadWrite);
        CHECK(group.is_attached());
    }
}


TEST(Group_Size)
{
    Group g;
    CHECK(g.is_attached());
    CHECK(g.is_empty());

    TableRef t = g.get_table("a");
    CHECK(!g.is_empty());
    CHECK_EQUAL(1, g.size());

    TableRef t1 = g.get_table("b");
    CHECK(!g.is_empty());
    CHECK_EQUAL(2, g.size());
}

TEST(Group_RemoveTable)
{
    Group g;
    TableRef t1 = g.get_table("alpha");
    TableRef t2 = g.get_table("beta");
    TableRef t3 = g.get_table("tau");
    TableRef t4 = g.get_table("sigma");
    CHECK_EQUAL(4, g.size());
    g.rename_table(1, "grappa");
    CHECK_EQUAL(4, g.size());
    g.remove_table(1);
    CHECK_EQUAL(3, g.size());
    CHECK_EQUAL(StringData("alpha"), g.get_table_name(0));
    CHECK_EQUAL(StringData("tau"), g.get_table_name(2));
    CHECK_EQUAL(StringData("sigma"), g.get_table_name(3));
    TableRef t5 = g.get_table("beta");
    CHECK_EQUAL(4, g.size());
    CHECK_EQUAL(StringData("alpha"), g.get_table_name(0));
    CHECK_EQUAL(StringData("beta"), g.get_table_name(1));
    CHECK_EQUAL(StringData("tau"), g.get_table_name(2));
    CHECK_EQUAL(StringData("sigma"), g.get_table_name(3));
}


TEST(Group_RemoveLinkTable)
{
    Group group;
    TableRef table = group.get_table("table");
    table->add_column_link(type_Link, "", *table);
    group.remove_table(table->get_index_in_parent());
    CHECK(group.is_empty());
    CHECK(!table->is_attached());
    TableRef origin = group.get_table("origin");
    TableRef target = group.get_table("target");
    target->add_column(type_Int, "");
    origin->add_column_link(type_Link, "", *target);
    CHECK_THROW(group.remove_table(target->get_index_in_parent()), CrossTableLinkTarget);
    group.remove_table(origin->get_index_in_parent());
    CHECK_EQUAL(1, group.size());
    CHECK(!origin->is_attached());
    CHECK(target->is_attached());
}


TEST(Group_GetTable)
{
    Group g;
    const Group& cg = g;
    TableRef t1 = g.get_table("alpha");
    ConstTableRef t2 = cg.get_table("alpha");
    CHECK_EQUAL(t1, t2);
    TestTableGroup::Ref t3 = g.get_table<TestTableGroup>("beta");
    TestTableGroup::ConstRef t4 = cg.get_table<TestTableGroup>("beta");
    CHECK_EQUAL(t3, t4);
}

TEST(Group_GetTableWasCreated)
{
    Group group;
    bool was_created = false;
    group.get_table("foo", was_created);
    CHECK(was_created);
    group.get_table("foo", was_created);
    CHECK(!was_created);
    group.get_table("bar", was_created);
    CHECK(was_created);
    group.get_table("baz", was_created);
    CHECK(was_created);
    group.get_table("bar", was_created);
    CHECK(!was_created);
    group.get_table("baz", was_created);
    CHECK(!was_created);
}

namespace {
void setup_table(TestTableGroup::Ref t)
{
    t->add("a",  1, true, Wed);
    t->add("b", 15, true, Wed);
    t->add("ccc", 10, true, Wed);
    t->add("dddd", 20, true, Wed);
}
}

TEST(Group_Equal)
{
    Group g1, g2;
    TestTableGroup::Ref t1 = g1.get_table<TestTableGroup>("TABLE1");
    setup_table(t1);
    TestTableGroup::Ref t2 = g2.get_table<TestTableGroup>("TABLE1");
    setup_table(t2);
    CHECK_EQUAL(true, g1 == g2);

    t2->add("hey", 2, false, Thu);
    CHECK_EQUAL(true, g1 != g2);
}

TEST(Group_TableAccessorLeftBehind)
{
    TableRef table;
    TableRef subtable;
    {
        Group group;
        table = group.get_table("test");
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
    CHECK_THROW(Group(path), File::NotFound);
}

TEST(Group_Invalid2)
{
    // Try to open buffer with invalid data
    const char* const str = "invalid data";
    const size_t size = strlen(str);
    char* const data = new char[strlen(str)];
    copy(str, str+size, data);
    CHECK_THROW(Group(BinaryData(data, size)), InvalidDatabase);
    delete[] data;
}

TEST(Group_Overwrite)
{
    GROUP_TEST_PATH(path);
    {
        Group g;
        g.write(path);
        CHECK_THROW(g.write(path), File::Exists);
    }
    {
        Group g(path);
        CHECK_THROW(g.write(path), File::Exists);
    }
    {
        Group g;
        File::try_remove(path);
        g.write(path);
    }
}

TEST(Group_Serialize0)
{
    GROUP_TEST_PATH(path);
    {
        // Create empty group and serialize to disk
        Group to_disk;
        to_disk.write(path);

        // Load the group
        Group from_disk(path);

        // Create new table in group
        TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");

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
        Group g(path);
    }
}


TEST(Group_Serialize1)
{
    GROUP_TEST_PATH(path);
    {
        // Create group with one table
        Group to_disk;
        TestTableGroup::Ref table = to_disk.get_table<TestTableGroup>("test");
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

#ifdef TIGHTDB_DEBUG
        to_disk.Verify();
#endif

        // Serialize to disk
        to_disk.write(path);

        // Load the table
        Group from_disk(path);
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
#ifdef TIGHTDB_DEBUG
        to_disk.Verify();
        from_disk.Verify();
#endif
    }
    {
        // Load the group and let it clean up without loading
        // any tables
        Group g(path);
    }
}

TEST(Group_Serialize2)
{
    GROUP_TEST_PATH(path);

    // Create group with two tables
    Group to_disk;
    TestTableGroup::Ref table1 = to_disk.get_table<TestTableGroup>("test1");
    table1->add("",  1, true, Wed);
    table1->add("", 15, true, Wed);
    table1->add("", 10, true, Wed);

    TestTableGroup::Ref table2 = to_disk.get_table<TestTableGroup>("test2");
    table2->add("hey",  0, true, Tue);
    table2->add("hello", 3232, false, Sun);

#ifdef TIGHTDB_DEBUG
    to_disk.Verify();
#endif

    // Serialize to disk
    to_disk.write(path);

    // Load the tables
    Group from_disk(path);
    TestTableGroup::Ref t1 = from_disk.get_table<TestTableGroup>("test1");
    TestTableGroup::Ref t2 = from_disk.get_table<TestTableGroup>("test2");

    // Verify that original values are there
    CHECK(*table1 == *t1);
    CHECK(*table2 == *t2);

#ifdef TIGHTDB_DEBUG
    to_disk.Verify();
    from_disk.Verify();
#endif
}

TEST(Group_Serialize3)
{
    GROUP_TEST_PATH(path);

    // Create group with one table (including long strings
    Group to_disk;
    TestTableGroup::Ref table = to_disk.get_table<TestTableGroup>("test");
    table->add("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1",  1, true, Wed);
    table->add("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15, true, Wed);

#ifdef TIGHTDB_DEBUG
    to_disk.Verify();
#endif

    // Serialize to disk
    to_disk.write(path);

    // Load the table
    Group from_disk(path);
    TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");

    // Verify that original values are there
    CHECK(*table == *t);
#ifdef TIGHTDB_DEBUG
    to_disk.Verify();
    from_disk.Verify();
#endif
}

TEST(Group_Serialize_Mem)
{
    // Create group with one table
    Group to_mem;
    TestTableGroup::Ref table = to_mem.get_table<TestTableGroup>("test");
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

#ifdef TIGHTDB_DEBUG
    to_mem.Verify();
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
#ifdef TIGHTDB_DEBUG
    to_mem.Verify();
    from_mem.Verify();
#endif
}

TEST(Group_Close)
{
    Group to_mem;
    TestTableGroup::Ref table = to_mem.get_table<TestTableGroup>("test");
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
    TestTableGroup::Ref table = to_mem.get_table<TestTableGroup>("test");

    for (size_t i = 0; i < 5; ++i) {
        table->add("abd",     1, true, Mon);
        table->add("eftg",    2, true, Tue);
        table->add("hijkl",   5, true, Wed);
        table->add("mnopqr",  8, true, Thu);
        table->add("stuvxyz", 9, true, Fri);
    }

    table->optimize();

#ifdef TIGHTDB_DEBUG
    to_mem.Verify();
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

#ifdef TIGHTDB_DEBUG
    to_mem.Verify();
    from_mem.Verify();
#endif
}

TEST(Group_Serialize_All)
{
    // Create group with one table
    Group to_mem;
    TableRef table = to_mem.get_table("test");

    table->add_column(type_Int,      "int");
    table->add_column(type_Bool,     "bool");
    table->add_column(type_DateTime, "date");
    table->add_column(type_String,   "string");
    table->add_column(type_Binary,   "binary");
    table->add_column(type_Mixed,    "mixed");

    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_datetime(2, 0, 12345);
    table->insert_string(3, 0, "test");
    table->insert_binary(4, 0, BinaryData("binary", 7));
    table->insert_mixed(5, 0, false);
    table->insert_done();

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TableRef t = from_mem.get_table("test");

    CHECK_EQUAL(6, t->get_column_count());
    CHECK_EQUAL(1, t->size());
    CHECK_EQUAL(12, t->get_int(0, 0));
    CHECK_EQUAL(true, t->get_bool(1, 0));
    CHECK_EQUAL(time_t(12345), t->get_datetime(2, 0));
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
    Group db(path, Group::mode_ReadWrite);

    // Insert some data
    TableRef table = db.get_table("test");
    table->add_column(type_Int,      "int");
    table->add_column(type_Bool,     "bool");
    table->add_column(type_DateTime, "date");
    table->add_column(type_String,   "string");
    table->add_column(type_Binary,   "binary");
    table->add_column(type_Mixed,    "mixed");
    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_datetime(2, 0, 12345);
    table->insert_string(3, 0, "test");
    table->insert_binary(4, 0, BinaryData("binary", 7));
    table->insert_mixed(5, 0, false);
    table->insert_done();

    // Write changes to file
    db.commit();

#ifdef TIGHTDB_DEBUG
    db.Verify();
#endif

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(time_t(12345), table->get_datetime(2, 0));
    CHECK_EQUAL("test", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).size());
    CHECK_EQUAL("binary", table->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());

    // Change a bit
    table->set_string(3, 0, "Changed!");

    // Write changes to file
    db.commit();

#ifdef TIGHTDB_DEBUG
    db.Verify();
#endif

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(time_t(12345), table->get_datetime(2, 0));
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
    TableRef table = g.get_table("test");
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

    g.write(path_1);

    // Read back tables
    Group g2(path_1);
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

    g2.write(path_2);

    // Read back tables
    Group g3(path_2);
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
        TableRef table = g.get_table("test");
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
        g.write(path_1);
    }

    // Non-mixed
    {
        Group g(path_1);
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
        g.write(path_2);
    }
    {
        Group g(path_2);
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
        g.write(path_3);
    }

    // Mixed
    {
        Group g(path_3);
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
        g.write(path_4);
    }
    {
        Group g(path_4);
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
        g.write(path_5);
    }
}


TEST(Group_CommitSubtable)
{
    GROUP_TEST_PATH(path);
    Group group(path, Group::mode_ReadWrite);

    TableRef table = group.get_table("test");
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
    Group group(path, Group::mode_ReadWrite);

    TableRef table = group.get_table("test");
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
    Group group(path, Group::mode_ReadWrite);
    TableRef table = group.get_table("parent");
    table->add_column(type_Table, "");
    table->get_subdescriptor(0)->add_column(type_Int, "");
    table->add_empty_row();
    TableRef subtab = table->get_subtable(0,0);
    CHECK(subtab->is_degenerate());
    group.commit();
    CHECK(subtab->is_degenerate());
}


namespace {

TIGHTDB_TABLE_3(TestTableGroup2,
                first,  Mixed,
                second, Subtable<TestTableGroup>,
                third,  Subtable<TestTableGroup>)

} // anonymous namespace

TEST(Group_InvalidateTables)
{
    TestTableGroup2::Ref table;
    TableRef             subtable1;
    TestTableGroup::Ref  subtable2;
    TestTableGroup::Ref  subtable3;
    {
        Group group;
        table = group.get_table<TestTableGroup2>("foo");
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
    TestTableGroup::Ref table = g.get_table<TestTableGroup>("test");

    table->add("jeff", 1, true, Wed);
    table->add("jim",  1, true, Wed);
    ostringstream out;
    g.to_json(out);
    string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("{\"test\":[{\"first\":\"jeff\",\"second\":1,\"third\":true,\"fourth\":2},{\"first\":\"jim\",\"second\":1,\"third\":true,\"fourth\":2}]}", str);
}

TEST(Group_ToString)
{
    Group g;
    TestTableGroup::Ref table = g.get_table<TestTableGroup>("test");

    table->add("jeff", 1, true, Wed);
    table->add("jim",  1, true, Wed);
    ostringstream out;
    g.to_string(out);
    string str = out.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("     tables     rows  \n   0 test       2     \n", str.c_str());
}

TEST(Group_IndexString)
{
    Group to_mem;
    TestTableGroup::Ref table = to_mem.get_table<TestTableGroup>("test");

    table->add("jeff",     1, true, Wed);
    table->add("jim",      1, true, Wed);
    table->add("jennifer", 1, true, Wed);
    table->add("john",     1, true, Wed);
    table->add("jimmy",    1, true, Wed);
    table->add("jimbo",    1, true, Wed);
    table->add("johnny",   1, true, Wed);
    table->add("jennifer", 1, true, Wed); //duplicate

    table->column().first.set_index();
    CHECK(table->column().first.has_index());

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

    CHECK(t->column().first.has_index());

    size_t m1 = table->column().first.find_first("jimmi");
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
}


TEST(Group_StockBug)
{
    // This test is a regression test - it once triggered a bug.
    // the bug was fixed in pr 351. In release mode, it crashes
    // the application. To get an assert in debug mode, the max
    // list size should be set to 1000.
    GROUP_TEST_PATH(path);
    Group group(path, Group::mode_ReadWrite);

    TableRef table = group.get_table("stocks");
    table->add_column(type_String, "ticker");

    for (size_t i = 0; i < 100; ++i) {
        table->Verify();
        table->insert_string(0, i, "123456789012345678901234567890123456789");
        table->insert_done();
        table->Verify();
        group.commit();
    }
}


TEST(Group_CommitLinkListChange)
{
    GROUP_TEST_PATH(path);
    Group group(path, Group::mode_ReadWrite);
    TableRef origin = group.get_table("origin");
    TableRef target = group.get_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_column(type_Int, "");
    origin->add_empty_row();
    target->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0,0);
    link_list->add(0);
    group.commit();
    group.Verify();
}


#ifdef TIGHTDB_DEBUG
#ifdef TIGHTDB_TO_DOT

TEST(Group_ToDot)
{
    // Create group with one table
    Group mygroup;

    // Create table with all column types
    TableRef table = mygroup.get_table("test");
    DescriptorRef subdesc;
    s.add_column(type_Int,      "int");
    s.add_column(type_Bool,     "bool");
    s.add_column(type_DateTime, "date");
    s.add_column(type_String,   "string");
    s.add_column(type_String,   "string_long");
    s.add_column(type_String,   "string_enum"); // becomes ColumnStringEnum
    s.add_column(type_Binary,   "binary");
    s.add_column(type_Mixed,    "mixed");
    s.add_column(type_Table,    "tables", &subdesc);
    subdesc->add_column(type_Int,    "sub_first");
    subdesc->add_column(type_String, "sub_second");

    // Add some rows
    for (size_t i = 0; i < 15; ++i) {
        table->insert_int(0, i, i);
        table->insert_bool(1, i, (i % 2 ? true : false));
        table->insert_datetime(2, i, 12345);

        stringstream ss;
        ss << "string" << i;
        table->insert_string(3, i, ss.str().c_str());

        ss << " very long string.........";
        table->insert_string(4, i, ss.str().c_str());

        switch (i % 3) {
            case 0:
                table->insert_string(5, i, "test1");
                break;
            case 1:
                table->insert_string(5, i, "test2");
                break;
            case 2:
                table->insert_string(5, i, "test3");
                break;
        }

        table->insert_binary(6, i, "binary", 7);

        switch (i % 3) {
            case 0:
                table->insert_mixed(7, i, false);
                break;
            case 1:
                table->insert_mixed(7, i, (int64_t)i);
                break;
            case 2:
                table->insert_mixed(7, i, "string");
                break;
        }

        table->insert_subtable(8, i);
        table->insert_done();

        // Add sub-tables
        if (i == 2) {
            // To mixed column
            table->set_mixed(7, i, Mixed(type_Table));
            Table subtable = table->GetMixedTable(7, i);

            Spec s = subtable->get_spec();
            s.add_column(type_Int,    "first");
            s.add_column(type_String, "second");
            subtable->UpdateFromSpec(s.get_ref());

            subtable->insert_int(0, 0, 42);
            subtable->insert_string(1, 0, "meaning");
            subtable->insert_done();

            // To table column
            Table subtable2 = table->get_subtable(8, i);
            subtable2->insert_int(0, 0, 42);
            subtable2->insert_string(1, 0, "meaning");
            subtable2->insert_done();
        }
    }

    // We also want ColumnStringEnum's
    table->optimize();

#if 1
    // Write array graph to cout
    stringstream ss;
    mygroup.ToDot(ss);
    cout << ss.str() << endl;
#endif

    // Write array graph to file in dot format
    ofstream fs("tightdb_graph.dot", ios::out | ios::binary);
    if (!fs.is_open())
        cout << "file open error " << strerror << endl;
    mygroup.to_dot(fs);
    fs.close();
}

#endif // TIGHTDB_TO_DOT
#endif // TIGHTDB_DEBUG

#endif // TEST_GROUP
