#include <algorithm>
#include <fstream>

#include <UnitTest++.h>

#include <tightdb.hpp>
#include <tightdb/file.hpp>

using namespace std;
using namespace tightdb;

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

TIGHTDB_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

} // Anonymous namespace

TEST(Group_Size)
{
    Group g;

    CHECK_EQUAL(true, g.is_empty());

    TableRef t = g.get_table("a");
    CHECK_EQUAL(false, g.is_empty());
    CHECK_EQUAL(1, g.size());

    TableRef t1 = g.get_table("b");
    CHECK_EQUAL(false, g.is_empty());
    CHECK_EQUAL(2, g.size());
}

TEST(Group_GetTable)
{
    Group g;
    const Group &cg = g;
    TableRef t1 = g.get_table("alpha");
    ConstTableRef t2 = cg.get_table("alpha");
    CHECK_EQUAL(t1, t2);
    TestTableGroup::Ref t3 = g.get_table<TestTableGroup>("beta");
    TestTableGroup::ConstRef t4 = cg.get_table<TestTableGroup>("beta");
    CHECK_EQUAL(t3, t4);
}

TEST(Group_Invalid1)
{
    File::try_remove("table_test.tightdb");

    // Try to open non-existing file
    // (read-only files have to exists to before opening)
    CHECK_THROW(Group("table_test.tightdb"), File::NotFound);
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
    File::try_remove("test_overwrite.tightdb");
    {
        Group g;
        g.write("test_overwrite.tightdb");
        CHECK_THROW(g.write("test_overwrite.tightdb"), File::Exists);
    }
    {
        Group g("test_overwrite.tightdb");
        CHECK_THROW(g.write("test_overwrite.tightdb"), File::Exists);
    }
    {
        Group g;
        File::try_remove("test_overwrite.tightdb");
        g.write("test_overwrite.tightdb");
    }
}

TEST(Group_Serialize0)
{
    File::try_remove("table_test.tightdb");

    // Create empty group and serialize to disk
    Group to_disk;
    to_disk.write("table_test.tightdb");

    // Load the group
    Group from_disk("table_test.tightdb");

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

TEST(Group_Read0)
{
    // Load the group and let it clean up without loading
    // any tables
    Group g("table_test.tightdb");
}

TEST(Group_Serialize1)
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
#endif // TIGHTDB_DEBUG

    // Delete old file if there
    File::try_remove("table_test.tightdb");

    // Serialize to disk
    to_disk.write("table_test.tightdb");

    // Load the table
    Group from_disk("table_test.tightdb");
    TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(10, t->size());

#ifdef TIGHTDB_DEBUG
    // Verify that original values are there
    CHECK(*table == *t);
#endif

    // Modify both tables
    table[0].first = "test";
    t[0].first = "test";
    table->insert(5, "hello", 100, false, Mon);
    t->insert(5, "hello", 100, false, Mon);
    table->remove(1);
    t->remove(1);

#ifdef TIGHTDB_DEBUG
    // Verify that both changed correctly
    CHECK(*table == *t);
    to_disk.Verify();
    from_disk.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Group_Read1)
{
    // Load the group and let it clean up without loading
    // any tables
    Group g("table_test.tightdb");
}

TEST(Group_Serialize2)
{
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
#endif // TIGHTDB_DEBUG

    // Delete old file if there
    File::try_remove("table_test.tightdb");

    // Serialize to disk
    to_disk.write("table_test.tightdb");

    // Load the tables
    Group from_disk("table_test.tightdb");
    TestTableGroup::Ref t1 = from_disk.get_table<TestTableGroup>("test1");
    TestTableGroup::Ref t2 = from_disk.get_table<TestTableGroup>("test2");
    static_cast<void>(t2);
    static_cast<void>(t1);

#ifdef TIGHTDB_DEBUG
    // Verify that original values are there
    CHECK(*table1 == *t1);
    CHECK(*table2 == *t2);
    to_disk.Verify();
    from_disk.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Group_Serialize3)
{
    // Create group with one table (including long strings
    Group to_disk;
    TestTableGroup::Ref table = to_disk.get_table<TestTableGroup>("test");
    table->add("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1",  1, true, Wed);
    table->add("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15, true, Wed);

#ifdef TIGHTDB_DEBUG
    to_disk.Verify();
#endif // TIGHTDB_DEBUG

    // Delete old file if there
    File::try_remove("table_test.tightdb");

    // Serialize to disk
    to_disk.write("table_test.tightdb");

    // Load the table
    Group from_disk("table_test.tightdb");
    TestTableGroup::Ref t = from_disk.get_table<TestTableGroup>("test");
    static_cast<void>(t);


#ifdef TIGHTDB_DEBUG
    // Verify that original values are there
    CHECK(*table == *t);
    to_disk.Verify();
    from_disk.Verify();
#endif // TIGHTDB_DEBUG}
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
#endif // TIGHTDB_DEBUG

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(10, t->size());

#ifdef TIGHTDB_DEBUG
    // Verify that original values are there
    CHECK(*table == *t);
    to_mem.Verify();
    from_mem.Verify();
#endif //_DEBUG
}

TEST(Group_Close)
{
    Group* to_mem = new Group();
    TestTableGroup::Ref table = to_mem->get_table<TestTableGroup>("test");
    table->add("",  1, true, Wed);
    table->add("",  2, true, Wed);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem->write_to_mem();

    Group* from_mem = new Group(buffer);
    delete to_mem;
    delete from_mem;
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
#endif // TIGHTDB_DEBUG

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());

    // Verify that original values are there
#ifdef TIGHTDB_DEBUG
    CHECK(*table == *t);
#endif

    // Add a row with a known (but unique) value
    table->add("search_target", 9, true, Fri);

    const size_t res = table->column().first.find_first("search_target");
    CHECK_EQUAL(table->size()-1, res);

#ifdef TIGHTDB_DEBUG
    to_mem.Verify();
    from_mem.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Group_Serialize_All)
{
    // Create group with one table
    Group to_mem;
    TableRef table = to_mem.get_table("test");

    table->add_column(type_Int,    "int");
    table->add_column(type_Bool,   "bool");
    table->add_column(type_Date,   "date");
    table->add_column(type_String, "string");
    table->add_column(type_Binary, "binary");
    table->add_column(type_Mixed,  "mixed");

    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_date(2, 0, 12345);
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
    CHECK_EQUAL(time_t(12345), t->get_date(2, 0));
    CHECK_EQUAL("test", t->get_string(3, 0));
    CHECK_EQUAL(7, t->get_binary(4, 0).size());
    CHECK_EQUAL("binary", t->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, t->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, t->get_mixed(5, 0).get_bool());
}

TEST(Group_Persist)
{
    // Delete old file if there
    File::try_remove("testdb.tightdb");

    // Create new database
    Group db("testdb.tightdb", Group::mode_ReadWrite);

    // Insert some data
    TableRef table = db.get_table("test");
    table->add_column(type_Int,    "int");
    table->add_column(type_Bool,   "bool");
    table->add_column(type_Date,   "date");
    table->add_column(type_String, "string");
    table->add_column(type_Binary, "binary");
    table->add_column(type_Mixed,  "mixed");
    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_date(2, 0, 12345);
    table->insert_string(3, 0, "test");
    table->insert_binary(4, 0, BinaryData("binary", 7));
    table->insert_mixed(5, 0, false);
    table->insert_done();

    // Write changes to file
    db.commit();

#ifdef TIGHTDB_DEBUG
    db.Verify();
#endif // TIGHTDB_DEBUG

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(time_t(12345), table->get_date(2, 0));
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
#endif // TIGHTDB_DEBUG

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL(time_t(12345), table->get_date(2, 0));
    CHECK_EQUAL("Changed!", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).size());
    CHECK_EQUAL("binary", table->get_binary(4, 0).data());
    CHECK_EQUAL(type_Bool, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());
}

TEST(Group_Subtable)
{
    int n = 1;

    Group g;
    TableRef table = g.get_table("test");
    Spec& s = table->get_spec();
    s.add_column(type_Int, "foo");
    Spec sub = s.add_subtable_column("sub");
    sub.add_column(type_Int, "bar");
    s.add_column(type_Mixed, "baz");
    table->update_from_spec();

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

    File::try_remove("subtables.tightdb");
    g.write("subtables.tightdb");

    // Read back tables
    Group g2("subtables.tightdb");
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

    File::try_remove("subtables2.tightdb");
    g2.write("subtables2.tightdb");

    // Read back tables
    Group g3("subtables2.tightdb");
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
    {
        Group g;
        TableRef table = g.get_table("test");
        {
            Spec& s = table->get_spec();
            s.add_column(type_Int, "int");
            {
                Spec sub = s.add_subtable_column("tab");
                sub.add_column(type_Int, "int");
                {
                    Spec subsub = sub.add_subtable_column("tab");
                    subsub.add_column(type_Int, "int");
                }
            }
            s.add_column(type_Mixed, "mix");
            table->update_from_spec();
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
            {
                Spec& s = a->get_spec();
                s.add_column(type_Int, "int");
                s.add_column(type_Mixed, "mix");
                a->update_from_spec();
            }
            a->add_empty_row();
            a->set_mixed(1, 0, Mixed::subtable_tag());
            TableRef b = a->get_subtable(1, 0);
            {
                Spec& s = b->get_spec();
                s.add_column(type_Int, "int");
                b->update_from_spec();
            }
            b->add_empty_row();
        }
        File::try_remove("subtables.tightdb");
        g.write("subtables.tightdb");
    }

    // Non-mixed
    {
        Group g("subtables.tightdb");
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
        File::try_remove("subtables2.tightdb");
        g.write("subtables2.tightdb");
    }
    {
        Group g("subtables2.tightdb");
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
        File::try_remove("subtables3.tightdb");
        g.write("subtables3.tightdb");
    }

    // Mixed
    {
        Group g("subtables3.tightdb");
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
        File::try_remove("subtables4.tightdb");
        g.write("subtables4.tightdb");
    }
    {
        Group g("subtables4.tightdb");
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
        File::try_remove("subtables5.tightdb");
        g.write("subtables5.tightdb");
    }
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
        CHECK(table->is_valid());
        table->add(Mixed::subtable_tag(), 0, 0);
        CHECK(table->is_valid());
        subtable1 = table[0].first.get_subtable();
        CHECK(table->is_valid());
        CHECK(subtable1);
        CHECK(subtable1->is_valid());
        subtable2 = table[0].second;
        CHECK(table->is_valid());
        CHECK(subtable1->is_valid());
        CHECK(subtable2);
        CHECK(subtable2->is_valid());
        subtable3 = table[0].third;
        CHECK(table->is_valid());
        CHECK(subtable1->is_valid());
        CHECK(subtable2->is_valid());
        CHECK(subtable3);
        CHECK(subtable3->is_valid());
        subtable3->add("alpha", 79542, true,  Wed);
        subtable3->add("beta",     97, false, Mon);
        CHECK(table->is_valid());
        CHECK(subtable1->is_valid());
        CHECK(subtable2->is_valid());
        CHECK(subtable3->is_valid());
    }
    CHECK(!table->is_valid());
    CHECK(!subtable1->is_valid());
    CHECK(!subtable2->is_valid());
    CHECK(!subtable3->is_valid());
}

TEST(Group_toJSON)
{
    Group g;
    TestTableGroup::Ref table = g.get_table<TestTableGroup>("test");

    table->add("jeff",     1, true, Wed);
    table->add("jim",      1, true, Wed);
    std::ostringstream ss;
    ss.sync_with_stdio(false); // for performance
    g.to_json(ss);
    const std::string str = ss.str();
    CHECK(str.length() > 0);
}

TEST(Group_toString)
{
    Group g;
    TestTableGroup::Ref table = g.get_table<TestTableGroup>("test");

    table->add("jeff",     1, true, Wed);
    table->add("jim",      1, true, Wed);
    std::ostringstream ss;
    ss.sync_with_stdio(false); // for performance
    g.to_string(ss);
    const std::string str = ss.str();
    CHECK(str.length() > 0);
    CHECK_EQUAL("     tables     rows  \n   0 test       2     \n", str.c_str());
}

TEST(Group_Index_String)
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

    const size_t r1 = table->column().first.find_first("jimmi");
    CHECK_EQUAL(not_found, r1);

    const size_t r2 = table->column().first.find_first("jeff");
    const size_t r3 = table->column().first.find_first("jim");
    const size_t r4 = table->column().first.find_first("jimbo");
    const size_t r5 = table->column().first.find_first("johnny");
    CHECK_EQUAL(0, r2);
    CHECK_EQUAL(1, r3);
    CHECK_EQUAL(5, r4);
    CHECK_EQUAL(6, r5);

    const size_t c1 = table->column().first.count("jennifer");
    CHECK_EQUAL(2, c1);

    // Serialize to memory (we now own the buffer)
    BinaryData buffer = to_mem.write_to_mem();

    // Load the table
    Group from_mem(buffer);
    TestTableGroup::Ref t = from_mem.get_table<TestTableGroup>("test");
    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(8, t->size());

    CHECK(t->column().first.has_index());

    const size_t m1 = table->column().first.find_first("jimmi");
    CHECK_EQUAL(not_found, m1);

    const size_t m2 = t->column().first.find_first("jeff");
    const size_t m3 = t->column().first.find_first("jim");
    const size_t m4 = t->column().first.find_first("jimbo");
    const size_t m5 = t->column().first.find_first("johnny");
    CHECK_EQUAL(0, m2);
    CHECK_EQUAL(1, m3);
    CHECK_EQUAL(5, m4);
    CHECK_EQUAL(6, m5);

    const size_t m6 = t->column().first.count("jennifer");
    CHECK_EQUAL(2, m6);
}

#ifdef TIGHTDB_DEBUG
#ifdef TIGHTDB_TO_DOT

TEST(Group_ToDot)
{
    // Create group with one table
    Group mygroup;

    // Create table with all column types
    TableRef table = mygroup.get_table("test");
    Spec s = table->get_spec();
    s.add_column(type_Int,    "int");
    s.add_column(type_Bool,   "bool");
    s.add_column(type_Date,   "date");
    s.add_column(type_String, "string");
    s.add_column(type_String, "string_long");
    s.add_column(type_String, "string_enum"); // becomes ColumnStringEnum
    s.add_column(type_Binary, "binary");
    s.add_column(type_Mixed,  "mixed");
    Spec sub = s.add_subtable_column("tables");
    sub.add_column(type_Int,  "sub_first");
    sub.add_column(type_String, "sub_second");
    table->UpdateFromSpec(s.GetRef());

    // Add some rows
    for (size_t i = 0; i < 15; ++i) {
        table->insert_int(0, i, i);
        table->insert_bool(1, i, (i % 2 ? true : false));
        table->insert_date(2, i, 12345);

        std::stringstream ss;
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
            subtable->UpdateFromSpec(s.GetRef());

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
    std::stringstream ss;
    mygroup.ToDot(ss);
    cout << ss.str() << endl;
#endif

    // Write array graph to file in dot format
    std::ofstream fs("tightdb_graph.dot", ios::out | ios::binary);
    if (!fs.is_open()) cout << "file open error " << strerror << endl;
    mygroup.to_dot(fs);
    fs.close();
}

#endif //TIGHTDB_TO_DOT
#endif // TIGHTDB_DEBUG
