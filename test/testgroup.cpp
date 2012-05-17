#include "group.hpp"
#include "tightdb.hpp"
#include <UnitTest++.h>

using namespace tightdb;

enum Days {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun
};

TIGHTDB_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

// Windows version of serialization is not implemented yet
#if 1 //_MSC_VER

TEST(Group_Invalid1)
{
    // Delete old file if there
    remove("table_test.tbl");

    // Try to open non-existing file
    Group fromDisk("table_test.tbl");
    CHECK(!fromDisk.is_valid());
}

TEST(Group_Invalid2)
{
    // Try to open buffer with invalid data
    const char* const buffer = "invalid data";
    Group fromMen(buffer, strlen(buffer));
    CHECK(!fromMen.is_valid());
}

TEST(Group_Serialize0)
{
    // Create empty group and serialize to disk
    Group toDisk;
    toDisk.write("table_test.tbl");

    // Load the group
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());

    // Create new table in group
    BasicTableRef<TestTableGroup> t = fromDisk.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(0, t->size());

    // Modify table
    t->add("Test",  1, true, Wed);

    CHECK_EQUAL("Test", static_cast<const char*>(t[0].first));
    CHECK_EQUAL(1,      t[0].second);
    CHECK_EQUAL(true,   t[0].third);
    CHECK_EQUAL(Wed,    t[0].fourth);
}

TEST(Group_Read0)
{
    // Load the group and let it clean up without loading
    // any tables
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());
}

TEST(Group_Serialize1)
{
    // Create group with one table
    Group toDisk;
    BasicTableRef<TestTableGroup> table = toDisk.get_table<TestTableGroup>("test");
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

#ifdef _DEBUG
    toDisk.verify();
#endif //_DEBUG

    // Delete old file if there
    remove("table_test.tbl");

    // Serialize to disk
    toDisk.write("table_test.tbl");

    // Load the table
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());
    BasicTableRef<TestTableGroup> t = fromDisk.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(10, t->size());

#ifdef _DEBUG
    // Verify that original values are there
    CHECK(table->Compare(*t));
#endif

    // Modify both tables
    table[0].first = "test";
    t[0].first = "test";
    table->insert(5, "hello", 100, false, Mon);
    t->insert(5, "hello", 100, false, Mon);
    table->remove(1);
    t->remove(1);

#ifdef _DEBUG
    // Verify that both changed correctly
    CHECK(table->Compare(*t));
    toDisk.verify();
    fromDisk.verify();
#endif //_DEBUG
}

TEST(Group_Read1)
{
    // Load the group and let it clean up without loading
    // any tables
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());
}

TEST(Group_Serialize2)
{
    // Create group with two tables
    Group toDisk;
    BasicTableRef<TestTableGroup> table1 = toDisk.get_table<TestTableGroup>("test1");
    table1->add("",  1, true, Wed);
    table1->add("", 15, true, Wed);
    table1->add("", 10, true, Wed);

    BasicTableRef<TestTableGroup> table2 = toDisk.get_table<TestTableGroup>("test2");
    table2->add("hey",  0, true, Tue);
    table2->add("hello", 3232, false, Sun);

#ifdef _DEBUG
    toDisk.verify();
#endif //_DEBUG

    // Delete old file if there
    remove("table_test.tbl");

    // Serialize to disk
    toDisk.write("table_test.tbl");

    // Load the tables
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());
    BasicTableRef<TestTableGroup> t1 = fromDisk.get_table<TestTableGroup>("test1");
    BasicTableRef<TestTableGroup> t2 = fromDisk.get_table<TestTableGroup>("test2");
    (void)t2;
    (void)t1;

#ifdef _DEBUG
    // Verify that original values are there
    CHECK(table1->Compare(*t1));
    CHECK(table2->Compare(*t2));
    toDisk.verify();
    fromDisk.verify();
#endif //_DEBUG
}

TEST(Group_Serialize3)
{
    // Create group with one table (including long strings
    Group toDisk;
    BasicTableRef<TestTableGroup> table = toDisk.get_table<TestTableGroup>("test");
    table->add("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1",  1, true, Wed);
    table->add("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15, true, Wed);

#ifdef _DEBUG
    toDisk.verify();
#endif //_DEBUG

    // Delete old file if there
    remove("table_test.tbl");

    // Serialize to disk
    toDisk.write("table_test.tbl");

    // Load the table
    Group fromDisk("table_test.tbl");
    CHECK(fromDisk.is_valid());
    BasicTableRef<TestTableGroup> t = fromDisk.get_table<TestTableGroup>("test");
    (void)t;


#ifdef _DEBUG
    // Verify that original values are there
    CHECK(table->Compare(*t));
    toDisk.verify();
    fromDisk.verify();
#endif //_DEBUG}
}

TEST(Group_Serialize_Men)
{
    // Create group with one table
    Group toMem;
    BasicTableRef<TestTableGroup> table = toMem.get_table<TestTableGroup>("test");
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

#ifdef _DEBUG
    toMem.verify();
#endif //_DEBUG

    // Serialize to memory (we now own the buffer)
    size_t len;
    const char* const buffer = toMem.write_to_mem(len);

    // Load the table
    Group fromMem(buffer, len);
    CHECK(fromMem.is_valid());
    BasicTableRef<TestTableGroup> t = fromMem.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());
    CHECK_EQUAL(10, t->size());


#ifdef _DEBUG
    // Verify that original values are there
    CHECK(table->Compare(*t));
    toMem.verify();
    fromMem.verify();
#endif //_DEBUG
}

TEST(Group_Serialize_Optimized)
{
    // Create group with one table
    Group toMem;
    BasicTableRef<TestTableGroup> table = toMem.get_table<TestTableGroup>("test");

    for (size_t i = 0; i < 5; ++i) {
        table->add("abd",     1, true, Mon);
        table->add("eftg",    2, true, Tue);
        table->add("hijkl",   5, true, Wed);
        table->add("mnopqr",  8, true, Thu);
        table->add("stuvxyz", 9, true, Fri);
    }

    table->optimize();

#ifdef _DEBUG
    toMem.verify();
#endif //_DEBUG

    // Serialize to memory (we now own the buffer)
    size_t len;
    const char* const buffer = toMem.write_to_mem(len);

    // Load the table
    Group fromMem(buffer, len);
    CHECK(fromMem.is_valid());
    BasicTableRef<TestTableGroup> t = fromMem.get_table<TestTableGroup>("test");

    CHECK_EQUAL(4, t->get_column_count());

    // Verify that original values are there
#ifdef _DEBUG
    CHECK(table->Compare(*t));
#endif

    // Add a row with a known (but unique) value
    table->add("search_target", 9, true, Fri);

    const size_t res = table->cols().first.find_first("search_target");
    CHECK_EQUAL(table->size()-1, res);

#ifdef _DEBUG
    toMem.verify();
    fromMem.verify();
#endif //_DEBUG
}

TEST(Group_Serialize_All)
{
    // Create group with one table
    Group toMem;
    TableRef table = toMem.get_table("test");

    table->add_column(COLUMN_TYPE_INT,    "int");
    table->add_column(COLUMN_TYPE_BOOL,   "bool");
    table->add_column(COLUMN_TYPE_DATE,   "date");
    table->add_column(COLUMN_TYPE_STRING, "string");
    table->add_column(COLUMN_TYPE_BINARY, "binary");
    table->add_column(COLUMN_TYPE_MIXED,  "mixed");

    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_date(2, 0, 12345);
    table->insert_string(3, 0, "test");
    table->insert_binary(4, 0, "binary", 7);
    table->insert_mixed(5, 0, false);
    table->insert_done();

    // Serialize to memory (we now own the buffer)
    size_t len;
    const char* const buffer = toMem.write_to_mem(len);

    // Load the table
    Group fromMem(buffer, len);
    CHECK(fromMem.is_valid());
    TableRef t = fromMem.get_table("test");

    CHECK_EQUAL(6, t->get_column_count());
    CHECK_EQUAL(1, t->size());
    CHECK_EQUAL(12, t->get_int(0, 0));
    CHECK_EQUAL(true, t->get_bool(1, 0));
    CHECK_EQUAL((time_t)12345, t->get_date(2, 0));
    CHECK_EQUAL("test", t->get_string(3, 0));
    CHECK_EQUAL(7, t->get_binary(4, 0).len);
    CHECK_EQUAL("binary", (const char*)t->get_binary(4, 0).pointer);
    CHECK_EQUAL(COLUMN_TYPE_BOOL, t->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, t->get_mixed(5, 0).get_bool());
}

#if !defined(_MSC_VER) // write persistence

TEST(Group_Persist) {
    // Delete old file if there
    remove("testdb.tdb");

    // Create new database
    Group db("testdb.tdb", false);

    // Insert some data
    TableRef table = db.get_table("test");
    table->add_column(COLUMN_TYPE_INT,    "int");
    table->add_column(COLUMN_TYPE_BOOL,   "bool");
    table->add_column(COLUMN_TYPE_DATE,   "date");
    table->add_column(COLUMN_TYPE_STRING, "string");
    table->add_column(COLUMN_TYPE_BINARY, "binary");
    table->add_column(COLUMN_TYPE_MIXED,  "mixed");
    table->insert_int(0, 0, 12);
    table->insert_bool(1, 0, true);
    table->insert_date(2, 0, 12345);
    table->insert_string(3, 0, "test");
    table->insert_binary(4, 0, "binary", 7);
    table->insert_mixed(5, 0, false);
    table->insert_done();

    // Write changes to file
    db.commit();

#ifdef _DEBUG
    db.verify();
#endif //_DEBUG

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL((time_t)12345, table->get_date(2, 0));
    CHECK_EQUAL("test", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).len);
    CHECK_EQUAL("binary", (const char*)table->get_binary(4, 0).pointer);
    CHECK_EQUAL(COLUMN_TYPE_BOOL, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());

    // Change a bit
    table->set_string(3, 0, "Changed!");

    // Write changes to file
    db.commit();

#ifdef _DEBUG
    db.verify();
#endif //_DEBUG

    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(1, table->size());
    CHECK_EQUAL(12, table->get_int(0, 0));
    CHECK_EQUAL(true, table->get_bool(1, 0));
    CHECK_EQUAL((time_t)12345, table->get_date(2, 0));
    CHECK_EQUAL("Changed!", table->get_string(3, 0));
    CHECK_EQUAL(7, table->get_binary(4, 0).len);
    CHECK_EQUAL("binary", (const char*)table->get_binary(4, 0).pointer);
    CHECK_EQUAL(COLUMN_TYPE_BOOL, table->get_mixed(5, 0).get_type());
    CHECK_EQUAL(false, table->get_mixed(5, 0).get_bool());
}
#endif

TEST(Group_Subtable)
{
    int n = 1;

    Group g;
    TableRef table = g.get_table("test");
    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_INT, "foo");
    Spec sub = s.add_subtable_column("sub");
    sub.add_column(COLUMN_TYPE_INT, "bar");
    s.add_column(COLUMN_TYPE_MIXED, "baz");
    table->update_from_spec();

    for (int i=0; i<n; ++i) {
        table->add_empty_row();
        table->set_int(0, i, 100+i);
        if (i%2 == 0) {
            TableRef st = table->get_subtable(1, i);
            st->add_empty_row();
            st->set_int(0, 0, 200+i);
        }
        if (i%3 == 1) {
            table->set_mixed(2, i, Mixed(COLUMN_TYPE_TABLE));
            TableRef st = table->get_subtable(2, i);
            st->add_column(COLUMN_TYPE_INT, "banach");
            st->add_empty_row();
            st->set_int(0, 0, 700+i);
        }
    }

    CHECK_EQUAL(table->size(), n);

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(table->get_int(0, i), 100+i);
        {
            TableRef st = table->get_subtable(1, i);
            CHECK_EQUAL(st->size(), i%2 == 0 ? 1 : 0);
            if (i%2 == 0) CHECK_EQUAL(st->get_int(0,0), 200+i);
            if (i%3 == 0) {
                st->add_empty_row();
                st->set_int(0, st->size()-1, 300+i);
            }
        }
        CHECK_EQUAL(table->get_mixed_type(2,i), i%3 == 1 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT);
        if (i%3 == 1) {
            TableRef st = table->get_subtable(2, i);
            CHECK_EQUAL(st->size(), 1);
            CHECK_EQUAL(st->get_int(0,0), 700+i);
        }
        if (i%8 == 3) {
            if (i%3 != 1) table->set_mixed(2, i, Mixed(COLUMN_TYPE_TABLE));
            TableRef st = table->get_subtable(2, i);
            if (i%3 != 1) st->add_column(COLUMN_TYPE_INT, "banach");
            st->add_empty_row();
            st->set_int(0, st->size()-1, 800+i);
        }
    }

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(table->get_int(0, i), 100+i);
        {
            TableRef st = table->get_subtable(1, i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 200+i);
                ++idx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 300+i);
                ++idx;
            }
        }
        CHECK_EQUAL(table->get_mixed_type(2,i), i%3 == 1 || i%8 == 3 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT);
        if (i%3 == 1 || i%8 == 3) {
            TableRef st = table->get_subtable(2, i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(st->get_int(0, idx), 700+i);
                ++idx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(st->get_int(0, idx), 800+i);
                ++idx;
            }
        }
    }

    g.write("subtables.tdb");

    // Read back tables
    Group g2("subtables.tdb");
    TableRef table2 = g2.get_table("test");

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(table2->get_int(0, i), 100+i);
        {
            TableRef st = table2->get_subtable(1, i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 200+i);
                ++idx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 300+i);
                ++idx;
            }
            if (i%5 == 0) {
                st->add_empty_row();
                st->set_int(0, st->size()-1, 400+i);
            }
        }
        CHECK_EQUAL(table2->get_mixed_type(2,i), i%3 == 1 || i%8 == 3 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT);
        if (i%3 == 1 || i%8 == 3) {
            TableRef st = table2->get_subtable(2, i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(st->get_int(0, idx), 700+i);
                ++idx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(st->get_int(0, idx), 800+i);
                ++idx;
            }
        }
        if (i%7 == 4) {
            if (i%3 != 1 && i%8 != 3) table2->set_mixed(2, i, Mixed(COLUMN_TYPE_TABLE));
            TableRef st = table2->get_subtable(2, i);
            if (i%3 != 1 && i%8 != 3) st->add_column(COLUMN_TYPE_INT, "banach");
            st->add_empty_row();
            st->set_int(0, st->size()-1, 900+i);
        }
    }

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(table2->get_int(0, i), 100+i);
        {
            TableRef st = table2->get_subtable(1, i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 200+i);
                ++idx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 300+i);
                ++idx;
            }
            if (i%5 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 400+i);
                ++idx;
            }
        }
        CHECK_EQUAL(table2->get_mixed_type(2,i), i%3 == 1 || i%8 == 3 || i%7 == 4 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT);
        if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
            TableRef st = table2->get_subtable(2, i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(st->get_int(0, idx), 700+i);
                ++idx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(st->get_int(0, idx), 800+i);
                ++idx;
            }
            if (i%7 == 4) {
                CHECK_EQUAL(st->get_int(0, idx), 900+i);
                ++idx;
            }
        }
    }

    g2.write("subtables2.tdb");

    // Read back tables
    Group g3("subtables2.tdb");
    TableRef table3 = g2.get_table("test");

    for (int i=0; i<n; ++i) {
        CHECK_EQUAL(table3->get_int(0, i), 100+i);
        {
            TableRef st = table3->get_subtable(1, i);
            size_t expected_size = (i%2 == 0 ? 1 : 0) + (i%3 == 0 ? 1 : 0) + (i%5 == 0 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%2 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 200+i);
                ++idx;
            }
            if (i%3 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 300+i);
                ++idx;
            }
            if (i%5 == 0) {
                CHECK_EQUAL(st->get_int(0, idx), 400+i);
                ++idx;
            }
        }
        CHECK_EQUAL(table3->get_mixed_type(2,i), i%3 == 1 || i%8 == 3 || i%7 == 4 ? COLUMN_TYPE_TABLE : COLUMN_TYPE_INT);
        if (i%3 == 1 || i%8 == 3 || i%7 == 4) {
            TableRef st = table3->get_subtable(2, i);
            size_t expected_size = (i%3 == 1 ? 1 : 0) + (i%8 == 3 ? 1 : 0) + (i%7 == 4 ? 1 : 0);
            CHECK_EQUAL(st->size(), expected_size);
            size_t idx = 0;
            if (i%3 == 1) {
                CHECK_EQUAL(st->get_int(0, idx), 700+i);
                ++idx;
            }
            if (i%8 == 3) {
                CHECK_EQUAL(st->get_int(0, idx), 800+i);
                ++idx;
            }
            if (i%7 == 4) {
                CHECK_EQUAL(st->get_int(0, idx), 900+i);
                ++idx;
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
            s.add_column(COLUMN_TYPE_INT, "int");
            {
                Spec sub = s.add_subtable_column("tab");
                sub.add_column(COLUMN_TYPE_INT, "int");
                {
                    Spec subsub = sub.add_subtable_column("tab");
                    subsub.add_column(COLUMN_TYPE_INT, "int");
                }
            }
            s.add_column(COLUMN_TYPE_MIXED, "mix");
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
            table->set_mixed(2, 0, Mixed(COLUMN_TYPE_TABLE));
            TableRef a = table->get_subtable(2, 0);
            {
                Spec& s = a->get_spec();
                s.add_column(COLUMN_TYPE_INT, "int");
                s.add_column(COLUMN_TYPE_MIXED, "mix");
                a->update_from_spec();
            }
            a->add_empty_row();
            a->set_mixed(1, 0, Mixed(COLUMN_TYPE_TABLE));
            TableRef b = a->get_subtable(1, 0);
            {
                Spec& s = b->get_spec();
                s.add_column(COLUMN_TYPE_INT, "int");
                b->update_from_spec();
            }
            b->add_empty_row();
        }
        g.write("subtables.tdb");
    }

    // Non-mixed
    {
        Group g("subtables.tdb");
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
        g.write("subtables2.tdb");
    }
    {
        Group g("subtables2.tdb");
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
        g.write("subtables3.tdb");
    }

    // Mixed
    {
        Group g("subtables3.tdb");
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
        g.write("subtables4.tdb");
    }
    {
        Group g("subtables4.tdb");
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
        g.write("subtables5.tdb");
    }
}



#ifdef _DEBUG
#ifdef TIGHTDB_TO_DOT

#include <fstream>
TEST(Group_ToDot)
{
    // Create group with one table
    Group mygroup;

    // Create table with all column types
    TableRef table = mygroup.get_table("test");
    Spec s = table->get_spec();
    s.add_column(COLUMN_TYPE_INT,    "int");
    s.add_column(COLUMN_TYPE_BOOL,   "bool");
    s.add_column(COLUMN_TYPE_DATE,   "date");
    s.add_column(COLUMN_TYPE_STRING, "string");
    s.add_column(COLUMN_TYPE_STRING, "string_long");
    s.add_column(COLUMN_TYPE_STRING, "string_enum"); // becomes ColumnStringEnum
    s.add_column(COLUMN_TYPE_BINARY, "binary");
    s.add_column(COLUMN_TYPE_MIXED,  "mixed");
    Spec sub = s.add_subtable_column("tables");
    sub.add_column(COLUMN_TYPE_INT,  "sub_first");
    sub.add_column(COLUMN_TYPE_STRING, "sub_second");
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

        table->insert_table(8, i);
        table->insert_done();

        // Add sub-tables
        if (i == 2) {
            // To mixed column
            table->set_mixed(7, i, Mixed(COLUMN_TYPE_TABLE));
            Table subtable = table->GetMixedTable(7, i);

            Spec s = subtable->get_spec();
            s.add_column(COLUMN_TYPE_INT,    "first");
            s.add_column(COLUMN_TYPE_STRING, "second");
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
#endif //_DEBUG
#endif
