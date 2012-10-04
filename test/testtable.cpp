#include <algorithm>
#include <sstream>
#include <UnitTest++.h>
#include <tightdb/table_macros.hpp>

using namespace tightdb;

TEST(Table1)
{
    Table table;
    table.add_column(COLUMN_TYPE_INT, "first");
    table.add_column(COLUMN_TYPE_INT, "second");

    CHECK_EQUAL(COLUMN_TYPE_INT, table.get_column_type(0));
    CHECK_EQUAL(COLUMN_TYPE_INT, table.get_column_type(1));
    CHECK_EQUAL("first", table.get_column_name(0));
    CHECK_EQUAL("second", table.get_column_name(1));

    // Test adding a single empty row
    // and filling it with values
    size_t ndx = table.add_empty_row();
    table.set_int(0, ndx, 0);
    table.set_int(1, ndx, 10);

    CHECK_EQUAL(0, table.get_int(0, ndx));
    CHECK_EQUAL(10, table.get_int(1, ndx));

    // Test adding multiple rows
    ndx = table.add_empty_row(7);
    for (size_t i = ndx; i < 7; ++i) {
        table.set_int(0, i, 2*i);
        table.set_int(1, i, 20*i);
    }

    for (size_t i = ndx; i < 7; ++i) {
        const int64_t v1 = 2 * i;
        const int64_t v2 = 20 * i;
        CHECK_EQUAL(v1, table.get_int(0, i));
        CHECK_EQUAL(v2, table.get_int(1, i));
    }

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

enum Days {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun
};

TIGHTDB_TABLE_4(TestTable,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

TEST(Table2)
{
    TestTable table;

    table.add(0, 10, true, Wed);
    const TestTable::Cursor r = table.back(); // last item

    CHECK_EQUAL(0, r.first);
    CHECK_EQUAL(10, r.second);
    CHECK_EQUAL(true, r.third);
    CHECK_EQUAL(Wed, r.fourth);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table3)
{
    TestTable table;

    for (size_t i = 0; i < 100; ++i) {
        table.add(0, 10, true, Wed);
    }

    // Test column searching
    CHECK_EQUAL(size_t(0),  table.column().first.find_first(0));
    CHECK_EQUAL(size_t(-1), table.column().first.find_first(1));
    CHECK_EQUAL(size_t(0),  table.column().second.find_first(10));
    CHECK_EQUAL(size_t(-1), table.column().second.find_first(100));
    CHECK_EQUAL(size_t(0),  table.column().third.find_first(true));
    CHECK_EQUAL(size_t(-1), table.column().third.find_first(false));
    CHECK_EQUAL(size_t(0) , table.column().fourth.find_first(Wed));
    CHECK_EQUAL(size_t(-1), table.column().fourth.find_first(Mon));

    // Test column incrementing
    table.column().first += 3;
    CHECK_EQUAL(3, table[0].first);
    CHECK_EQUAL(3, table[99].first);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TIGHTDB_TABLE_2(TestTableEnum,
                first,      Enum<Days>,
                second,     String)

TEST(Table4)
{
    TestTableEnum table;

    table.add(Mon, "Hello");
    table.add(Mon, "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello");
    const TestTableEnum::Cursor r = table.back(); // last item

    CHECK_EQUAL(Mon, r.first);
    CHECK_EQUAL("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello", (const char*)r.second);

    // Test string column searching
    CHECK_EQUAL(size_t(1),  table.column().second.find_first("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello"));
    CHECK_EQUAL(size_t(-1), table.column().second.find_first("Foo"));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table_Delete)
{
    TestTable table;

    for (size_t i = 0; i < 10; ++i) {
        table.add(0, i, true, Wed);
    }

    table.remove(0);
    table.remove(4);
    table.remove(7);

    CHECK_EQUAL(1, table[0].second);
    CHECK_EQUAL(2, table[1].second);
    CHECK_EQUAL(3, table[2].second);
    CHECK_EQUAL(4, table[3].second);
    CHECK_EQUAL(6, table[4].second);
    CHECK_EQUAL(7, table[5].second);
    CHECK_EQUAL(8, table[6].second);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG

    // Delete all items one at a time
    for (size_t i = 0; i < 7; ++i) {
        table.remove(0);
    }

    CHECK(table.is_empty());
    CHECK_EQUAL(0, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table_Delete_All_Types)
{
    // Create table with all column types
    Table table;
    Spec& s = table.get_spec();
    s.add_column(COLUMN_TYPE_INT,    "int");
    s.add_column(COLUMN_TYPE_BOOL,   "bool");
    s.add_column(COLUMN_TYPE_DATE,   "date");
    s.add_column(COLUMN_TYPE_STRING, "string");
    s.add_column(COLUMN_TYPE_STRING, "string_long");
    s.add_column(COLUMN_TYPE_STRING, "string_enum"); // becomes ColumnStringEnum
    s.add_column(COLUMN_TYPE_BINARY, "binary");
    s.add_column(COLUMN_TYPE_MIXED,  "mixed");
    Spec sub = s.add_subtable_column("tables");
    sub.add_column(COLUMN_TYPE_INT,    "sub_first");
    sub.add_column(COLUMN_TYPE_STRING, "sub_second");
    table.update_from_spec();

    // Add some rows
    for (size_t i = 0; i < 15; ++i) {
        table.insert_int(0, i, i);
        table.insert_bool(1, i, (i % 2 ? true : false));
        table.insert_date(2, i, 12345);

        std::stringstream ss;
        ss << "string" << i;
        table.insert_string(3, i, ss.str().c_str());

        ss << " very long string.........";
        table.insert_string(4, i, ss.str().c_str());

        switch (i % 3) {
            case 0:
                table.insert_string(5, i, "test1");
                break;
            case 1:
                table.insert_string(5, i, "test2");
                break;
            case 2:
                table.insert_string(5, i, "test3");
                break;
        }

        table.insert_binary(6, i, "binary", 7);

        switch (i % 4) {
            case 0:
                table.insert_mixed(7, i, false);
                break;
            case 1:
                table.insert_mixed(7, i, (int64_t)i);
                break;
            case 2:
                table.insert_mixed(7, i, "string");
                break;
            case 3:
            {
                // Add subtable to mixed column
                // We can first set schema and contents when the entire
                // row has been inserted
                table.insert_mixed(7, i, Mixed::subtable_tag());
                break;
            }
        }

        table.insert_subtable(8, i);
        table.insert_done();

        // Add subtable to mixed column
        if (i % 4 == 3) {
            TableRef subtable = table.get_subtable(7, i);
            subtable->add_column(COLUMN_TYPE_INT,    "first");
            subtable->add_column(COLUMN_TYPE_STRING, "second");
            subtable->insert_int(0, 0, 42);
            subtable->insert_string(1, 0, "meaning");
            subtable->insert_done();
        }

        // Add sub-tables to table column
        TableRef subtable = table.get_subtable(8, i);
        subtable->insert_int(0, 0, 42);
        subtable->insert_string(1, 0, "meaning");
        subtable->insert_done();
    }

    // We also want a ColumnStringEnum
    table.optimize();

    // Test Deletes
    table.remove(14);
    table.remove(0);
    table.remove(5);

    CHECK_EQUAL(12, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG

    // Test Clear
    table.clear();
    CHECK_EQUAL(0, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table_Find_Int)
{
    TestTable table;

    for (int i = 1000; i >= 0; --i) {
        table.add(0, i, true, Wed);
    }

    CHECK_EQUAL(size_t(0),    table.column().second.find_first(1000));
    CHECK_EQUAL(size_t(1000), table.column().second.find_first(0));
    CHECK_EQUAL(size_t(-1),   table.column().second.find_first(1001));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}


/*
TEST(Table6)
{
    TestTableEnum table;

    TDB_QUERY(TestQuery, TestTableEnum) {
    //  first.between(Mon, Thu);
        second == "Hello" || (second == "Hey" && first == Mon);
    }};

    TDB_QUERY_OPT(TestQuery2, TestTableEnum) (Days a, Days b, const char* str) {
        (void)b;
        (void)a;
        //first.between(a, b);
        second == str || second.MatchRegEx(".*");
    }};

    //TestTableEnum result = table.find_all(TestQuery2(Mon, Tue, "Hello")).sort().Limit(10);
    //size_t result2 = table.Range(10, 200).find_first(TestQuery());
    //CHECK_EQUAL((size_t)-1, result2);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}
*/


TEST(Table_FindAll_Int)
{
    TestTable table;

    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);

    // Search for a value that does not exits
    const TestTable::View v0 = table.column().second.find_all(5);
    CHECK_EQUAL(0, v0.size());

    // Search for a value with several matches
    const TestTable::View v = table.column().second.find_all(20);

    CHECK_EQUAL(5, v.size());
    CHECK_EQUAL(1, v.get_source_ndx(0));
    CHECK_EQUAL(3, v.get_source_ndx(1));
    CHECK_EQUAL(5, v.get_source_ndx(2));
    CHECK_EQUAL(7, v.get_source_ndx(3));
    CHECK_EQUAL(9, v.get_source_ndx(4));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table_Sorted_Int)
{
    TestTable table;

    table.add(0, 10, true, Wed); // 0: 4
    table.add(0, 20, true, Wed); // 1: 7
    table.add(0,  0, true, Wed); // 2: 0
    table.add(0, 40, true, Wed); // 3: 8
    table.add(0, 15, true, Wed); // 4: 6
    table.add(0, 11, true, Wed); // 5: 5
    table.add(0,  6, true, Wed); // 6: 3
    table.add(0,  4, true, Wed); // 7: 2
    table.add(0, 99, true, Wed); // 8: 9
    table.add(0,  2, true, Wed); // 9: 1

    // Search for a value that does not exits
    TestTable::View v = table.column().second.get_sorted_view();
    CHECK_EQUAL(table.size(), v.size());

    CHECK_EQUAL(2, v.get_source_ndx(0));
    CHECK_EQUAL(9, v.get_source_ndx(1));
    CHECK_EQUAL(7, v.get_source_ndx(2));
    CHECK_EQUAL(6, v.get_source_ndx(3));
    CHECK_EQUAL(0, v.get_source_ndx(4));
    CHECK_EQUAL(5, v.get_source_ndx(5));
    CHECK_EQUAL(4, v.get_source_ndx(6));
    CHECK_EQUAL(1, v.get_source_ndx(7));
    CHECK_EQUAL(3, v.get_source_ndx(8));
    CHECK_EQUAL(8, v.get_source_ndx(9));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TEST(Table_Index_String)
{
    TestTableEnum table;
    
    table.add(Mon, "jeff");
    table.add(Tue, "jim");
    table.add(Wed, "jennifer");
    table.add(Thu, "john");
    table.add(Fri, "jimmy");
    table.add(Sat, "jimbo");
    table.add(Sun, "johnny");
    table.add(Mon, "jennifer"); //duplicate
    
    table.column().second.set_index();
    CHECK(table.column().second.has_index());
    
    const size_t r1 = table.column().second.find_first("jimmi");
    CHECK_EQUAL(not_found, r1);
    
    const size_t r2 = table.column().second.find_first("jeff");
    const size_t r3 = table.column().second.find_first("jim");
    const size_t r4 = table.column().second.find_first("jimbo");
    const size_t r5 = table.column().second.find_first("johnny");
    CHECK_EQUAL(0, r2);
    CHECK_EQUAL(1, r3);
    CHECK_EQUAL(5, r4);
    CHECK_EQUAL(6, r5);
    
    const size_t c1 = table.column().second.count("jennifer");
    CHECK_EQUAL(2, c1);
}

TIGHTDB_TABLE_2(LookupTable,
                first,  String,
                second, Int)

TEST(Table_Lookup)
{
    LookupTable table;

    table.add("jeff",     0);
    table.add("jim",      1);
    table.add("jennifer", 2);
    table.add("john",     3);
    table.add("jimmy",    4);
    table.add("jimbo",    5);
    table.add("johnny",   6);
    table.add("jennifer", 7); //duplicate

    // Do lookups with manual search
    const size_t a0 = table.lookup("jeff");
    const size_t a1 = table.lookup("jim");
    const size_t a2 = table.lookup("jennifer");
    const size_t a3 = table.lookup("john");
    const size_t a4 = table.lookup("jimmy");
    const size_t a5 = table.lookup("jimbo");
    const size_t a6 = table.lookup("johnny");
    const size_t a7 = table.lookup("jerry");
    CHECK_EQUAL(0, a0);
    CHECK_EQUAL(1, a1);
    CHECK_EQUAL(2, a2);
    CHECK_EQUAL(3, a3);
    CHECK_EQUAL(4, a4);
    CHECK_EQUAL(5, a5);
    CHECK_EQUAL(6, a6);
    CHECK_EQUAL(not_found, a7);

    table.column().first.set_index();
    CHECK(table.column().first.has_index());

    // Do lookups using (cached) index
    const size_t b0 = table.lookup("jeff");
    const size_t b1 = table.lookup("jim");
    const size_t b2 = table.lookup("jennifer");
    const size_t b3 = table.lookup("john");
    const size_t b4 = table.lookup("jimmy");
    const size_t b5 = table.lookup("jimbo");
    const size_t b6 = table.lookup("johnny");
    const size_t b7 = table.lookup("jerry");
    CHECK_EQUAL(0, b0);
    CHECK_EQUAL(1, b1);
    CHECK_EQUAL(2, b2);
    CHECK_EQUAL(3, b3);
    CHECK_EQUAL(4, b4);
    CHECK_EQUAL(5, b5);
    CHECK_EQUAL(6, b6);
    CHECK_EQUAL(not_found, b7);
}

TEST(Table_Distinct)
{
    TestTableEnum table;

    table.add(Mon, "A");
    table.add(Tue, "B");
    table.add(Wed, "C");
    table.add(Thu, "B");
    table.add(Fri, "C");
    table.add(Sat, "D");
    table.add(Sun, "D");
    table.add(Mon, "D");

    table.column().second.set_index();
    CHECK(table.column().second.has_index());

    TestTableEnum::View view = table.column().second.distinct();

    CHECK_EQUAL(4, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(0));
    CHECK_EQUAL(1, view.get_source_ndx(1));
    CHECK_EQUAL(2, view.get_source_ndx(2));
    CHECK_EQUAL(5, view.get_source_ndx(3));
}

/*
TEST(Table_Index_Int)
{
    TestTable table;

    table.add(0,  1, true, Wed);
    table.add(0, 15, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0, 20, true, Wed);
    table.add(0, 11, true, Wed);
    table.add(0, 45, true, Wed);
    table.add(0, 10, true, Wed);
    table.add(0,  0, true, Wed);
    table.add(0, 30, true, Wed);
    table.add(0,  9, true, Wed);

    // Create index for column two
//    table.cols().second.set_index();

    // Search for a value that does not exits
    const size_t r1 = table.column().second.find_first(2);
    CHECK_EQUAL(-1, r1);

    // Find existing values
    CHECK_EQUAL(0, table.column().second.find_first(1));
    CHECK_EQUAL(1, table.column().second.find_first(15));
    CHECK_EQUAL(2, table.column().second.find_first(10));
    CHECK_EQUAL(3, table.column().second.find_first(20));
    CHECK_EQUAL(4, table.column().second.find_first(11));
    CHECK_EQUAL(5, table.column().second.find_first(45));
    //CHECK_EQUAL(6, table.column().second.find_first(10)); // only finds first match
    CHECK_EQUAL(7, table.column().second.find_first(0));
    CHECK_EQUAL(8, table.column().second.find_first(30));
    CHECK_EQUAL(9, table.column().second.find_first(9));

    // Change some values
    table[2].second = 13;
    table[9].second = 100;

    CHECK_EQUAL(0, table.column().second.find_first(1));
    CHECK_EQUAL(1, table.column().second.find_first(15));
    CHECK_EQUAL(2, table.column().second.find_first(13));
    CHECK_EQUAL(3, table.column().second.find_first(20));
    CHECK_EQUAL(4, table.column().second.find_first(11));
    CHECK_EQUAL(5, table.column().second.find_first(45));
    CHECK_EQUAL(6, table.column().second.find_first(10));
    CHECK_EQUAL(7, table.column().second.find_first(0));
    CHECK_EQUAL(8, table.column().second.find_first(30));
    CHECK_EQUAL(9, table.column().second.find_first(100));

    // Insert values
    table.add(0, 29, true, Wed);
    //TODO: More than add

    CHECK_EQUAL(0, table.column().second.find_first(1));
    CHECK_EQUAL(1, table.column().second.find_first(15));
    CHECK_EQUAL(2, table.column().second.find_first(13));
    CHECK_EQUAL(3, table.column().second.find_first(20));
    CHECK_EQUAL(4, table.column().second.find_first(11));
    CHECK_EQUAL(5, table.column().second.find_first(45));
    CHECK_EQUAL(6, table.column().second.find_first(10));
    CHECK_EQUAL(7, table.column().second.find_first(0));
    CHECK_EQUAL(8, table.column().second.find_first(30));
    CHECK_EQUAL(9, table.column().second.find_first(100));
    CHECK_EQUAL(10, table.column().second.find_first(29));

    // Delete some values
    table.remove(0);
    table.remove(5);
    table.remove(8);

    CHECK_EQUAL(0, table.column().second.find_first(15));
    CHECK_EQUAL(1, table.column().second.find_first(13));
    CHECK_EQUAL(2, table.column().second.find_first(20));
    CHECK_EQUAL(3, table.column().second.find_first(11));
    CHECK_EQUAL(4, table.column().second.find_first(45));
    CHECK_EQUAL(5, table.column().second.find_first(0));
    CHECK_EQUAL(6, table.column().second.find_first(30));
    CHECK_EQUAL(7, table.column().second.find_first(100));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}
*/

TIGHTDB_TABLE_4(TestTableAE,
                first,  Int,
                second, String,
                third,  Bool,
                fourth, Enum<Days>)

TEST(TableAutoEnumeration)
{
    TestTableAE table;

    for (size_t i = 0; i < 5; ++i) {
        table.add(1, "abd",     true, Mon);
        table.add(2, "eftg",    true, Tue);
        table.add(5, "hijkl",   true, Wed);
        table.add(8, "mnopqr",  true, Thu);
        table.add(9, "stuvxyz", true, Fri);
    }

    table.optimize();

    for (size_t i = 0; i < 5; ++i) {
        const size_t n = i * 5;
        CHECK_EQUAL(1, table[0+n].first);
        CHECK_EQUAL(2, table[1+n].first);
        CHECK_EQUAL(5, table[2+n].first);
        CHECK_EQUAL(8, table[3+n].first);
        CHECK_EQUAL(9, table[4+n].first);

        CHECK_EQUAL("abd",     (const char*)table[0+n].second);
        CHECK_EQUAL("eftg",    (const char*)table[1+n].second);
        CHECK_EQUAL("hijkl",   (const char*)table[2+n].second);
        CHECK_EQUAL("mnopqr",  (const char*)table[3+n].second);
        CHECK_EQUAL("stuvxyz", (const char*)table[4+n].second);

        CHECK_EQUAL(true, table[0+n].third);
        CHECK_EQUAL(true, table[1+n].third);
        CHECK_EQUAL(true, table[2+n].third);
        CHECK_EQUAL(true, table[3+n].third);
        CHECK_EQUAL(true, table[4+n].third);

        CHECK_EQUAL(Mon, table[0+n].fourth);
        CHECK_EQUAL(Tue, table[1+n].fourth);
        CHECK_EQUAL(Wed, table[2+n].fourth);
        CHECK_EQUAL(Thu, table[3+n].fourth);
        CHECK_EQUAL(Fri, table[4+n].fourth);
    }

    // Verify counts
    const size_t count1 = table.column().second.count("abd");
    const size_t count2 = table.column().second.count("eftg");
    const size_t count3 = table.column().second.count("hijkl");
    const size_t count4 = table.column().second.count("mnopqr");
    const size_t count5 = table.column().second.count("stuvxyz");
    CHECK_EQUAL(5, count1);
    CHECK_EQUAL(5, count2);
    CHECK_EQUAL(5, count3);
    CHECK_EQUAL(5, count4);
    CHECK_EQUAL(5, count5);
}


TEST(TableAutoEnumerationFindFindAll)
{
    TestTableAE table;

    for (size_t i = 0; i < 5; ++i) {
        table.add(1, "abd",     true, Mon);
        table.add(2, "eftg",    true, Tue);
        table.add(5, "hijkl",   true, Wed);
        table.add(8, "mnopqr",  true, Thu);
        table.add(9, "stuvxyz", true, Fri);
    }

    table.optimize();

    size_t t = table.column().second.find_first("eftg");
    CHECK_EQUAL(1, t);

    TestTableAE::View tv = table.column().second.find_all("eftg");
    CHECK_EQUAL(5, tv.size());
    CHECK_EQUAL("eftg", static_cast<const char*>(tv[0].second));
    CHECK_EQUAL("eftg", static_cast<const char*>(tv[1].second));
    CHECK_EQUAL("eftg", static_cast<const char*>(tv[2].second));
    CHECK_EQUAL("eftg", static_cast<const char*>(tv[3].second));
    CHECK_EQUAL("eftg", static_cast<const char*>(tv[4].second));
}

#include <tightdb/alloc_slab.hpp>
TEST(Table_SlabAlloc)
{
    SlabAlloc alloc;
    TestTable table(alloc);

    table.add(0, 10, true, Wed);
    const TestTable::Cursor r = table.back(); // last item

    CHECK_EQUAL(   0, r.first);
    CHECK_EQUAL(  10, r.second);
    CHECK_EQUAL(true, r.third);
    CHECK_EQUAL( Wed, r.fourth);

    // Add some more rows
    table.add(1, 10, true, Wed);
    table.add(2, 20, true, Wed);
    table.add(3, 10, true, Wed);
    table.add(4, 20, true, Wed);
    table.add(5, 10, true, Wed);

    // Delete some rows
    table.remove(2);
    table.remove(4);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

#include <tightdb/group.hpp>

#ifndef _MSC_VER

TEST(Table_Spec)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_INT,    "first");
    s.add_column(COLUMN_TYPE_STRING, "second");
    Spec sub = s.add_subtable_column("third");
        sub.add_column(COLUMN_TYPE_INT,    "sub_first");
        sub.add_column(COLUMN_TYPE_STRING, "sub_second");
    table->update_from_spec();

    CHECK_EQUAL(3, table->get_column_count());

    // Add a row
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);
    table->insert_done();

    CHECK_EQUAL(0, table->get_subtable_size(2, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_int(0, 0, 42);
        subtable->insert_string(1, 0, "test");
        subtable->insert_done();

        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    CHECK_EQUAL(1, table->get_subtable_size(2, 0));

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);

        CHECK_EQUAL(1,      subtable->size());
        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    // Write the group to disk
    group.write("subtables.tightdb");

    // Read back tables
    {
        Group fromDisk("subtables.tightdb", GROUP_READONLY);
        TableRef fromDiskTable = fromDisk.get_table("test");

        TableRef subtable2 = fromDiskTable->get_subtable(2, 0);

        CHECK_EQUAL(1,      subtable2->size());
        CHECK_EQUAL(42,     subtable2->get_int(0, 0));
        CHECK_EQUAL("test", subtable2->get_string(1, 0));
    }
}

#endif

TEST(Table_Mixed)
{
    Table table;
    table.add_column(COLUMN_TYPE_INT, "first");
    table.add_column(COLUMN_TYPE_MIXED, "second");

    CHECK_EQUAL(COLUMN_TYPE_INT, table.get_column_type(0));
    CHECK_EQUAL(COLUMN_TYPE_MIXED, table.get_column_type(1));
    CHECK_EQUAL("first", table.get_column_name(0));
    CHECK_EQUAL("second", table.get_column_name(1));

    const size_t ndx = table.add_empty_row();
    table.set_int(0, ndx, 0);
    table.set_mixed(1, ndx, true);

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(COLUMN_TYPE_BOOL, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());

    table.insert_int(0, 1, 43);
    table.insert_mixed(1, 1, (int64_t)12);
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, ndx));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(COLUMN_TYPE_BOOL, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(COLUMN_TYPE_INT,  table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,   table.get_mixed(1, 1).get_int());

    table.insert_int(0, 2, 100);
    table.insert_mixed(1, 2, "test");
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(COLUMN_TYPE_INT,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(COLUMN_TYPE_STRING, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());

    table.insert_int(0, 3, 0);
    table.insert_mixed(1, 3, Date(324234));
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(COLUMN_TYPE_INT,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(COLUMN_TYPE_STRING, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(COLUMN_TYPE_DATE,   table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_date());

    table.insert_int(0, 4, 43);
    table.insert_mixed(1, 4, Mixed("binary", 7));
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(COLUMN_TYPE_INT,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(COLUMN_TYPE_STRING, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(COLUMN_TYPE_DATE,   table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(COLUMN_TYPE_BINARY, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_date());
    CHECK_EQUAL("binary", (const char*)table.get_mixed(1, 4).get_binary().pointer);
    CHECK_EQUAL(7,      table.get_mixed(1, 4).get_binary().len);

    table.insert_int(0, 5, 0);
    table.insert_mixed(1, 5, Mixed::subtable_tag());
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(0,  table.get_int(0, 5));
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(COLUMN_TYPE_INT,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(COLUMN_TYPE_STRING, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(COLUMN_TYPE_DATE,   table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(COLUMN_TYPE_BINARY, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(COLUMN_TYPE_TABLE,  table.get_mixed(1, 5).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_date());
    CHECK_EQUAL("binary", (const char*)table.get_mixed(1, 4).get_binary().pointer);
    CHECK_EQUAL(7,      table.get_mixed(1, 4).get_binary().len);

    // Get table from mixed column and add schema and some values
    TableRef subtable = table.get_subtable(1, 5);
    subtable->add_column(COLUMN_TYPE_STRING, "name");
    subtable->add_column(COLUMN_TYPE_INT,    "age");

    subtable->insert_string(0, 0, "John");
    subtable->insert_int(1, 0, 40);
    subtable->insert_done();

    // Get same table again and verify values
    TableRef subtable2 = table.get_subtable(1, 5);
    CHECK_EQUAL(1, subtable2->size());
    CHECK_EQUAL("John", subtable2->get_string(0, 0));
    CHECK_EQUAL(40, subtable2->get_int(1, 0));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif // TIGHTDB_DEBUG
}

TIGHTDB_TABLE_1(TestTableMX,
                first, Mixed)


TEST(Table_Mixed2)
{
    TestTableMX table;

    table.add(int64_t(1));
    table.add(true);
    table.add(Date(1234));
    table.add("test");

    CHECK_EQUAL(COLUMN_TYPE_INT,    table[0].first.get_type());
    CHECK_EQUAL(COLUMN_TYPE_BOOL,   table[1].first.get_type());
    CHECK_EQUAL(COLUMN_TYPE_DATE,   table[2].first.get_type());
    CHECK_EQUAL(COLUMN_TYPE_STRING, table[3].first.get_type());

    CHECK_EQUAL(1,            table[0].first.get_int());
    CHECK_EQUAL(true,         table[1].first.get_bool());
    CHECK_EQUAL(time_t(1234), table[2].first.get_date());
    CHECK_EQUAL("test",       table[3].first.get_string());
}


TEST(Table_SubtableSizeAndClear)
{
    Table table;
    Spec& spec = table.get_spec();
    {
        Spec subspec = spec.add_subtable_column("subtab");
        subspec.add_column(COLUMN_TYPE_INT, "int");
    }
    spec.add_column(COLUMN_TYPE_MIXED, "mixed");
    table.update_from_spec();

    table.insert_subtable(0, 0);
    table.insert_mixed(1, 0, false);
    table.insert_done();

    table.insert_subtable(0, 1);
    table.insert_mixed(1, 1, Mixed::subtable_tag());
    table.insert_done();

    CHECK_EQUAL(table.get_subtable_size(0,0), 0); // Subtable column
    CHECK_EQUAL(table.get_subtable_size(1,0), 0); // Mixed column, bool value
    CHECK_EQUAL(table.get_subtable_size(1,1), 0); // Mixed column, table value

    CHECK(table.get_subtable(0, 0));  // Subtable column
    CHECK(!table.get_subtable(1, 0)); // Mixed column, bool value, must return NULL
    CHECK(table.get_subtable(1, 1));  // Mixed column, table value

    table.set_mixed(1, 0, Mixed::subtable_tag());
    table.set_mixed(1, 1, false);
    CHECK(table.get_subtable(1, 0));
    CHECK(!table.get_subtable(1, 1));

    TableRef subtab1 = table.get_subtable(0, 0);
    TableRef subtab2 = table.get_subtable(1, 0);
    {
        Spec& subspec = subtab2->get_spec();
        subspec.add_column(COLUMN_TYPE_INT, "int");
        subtab2->update_from_spec();
    }

    CHECK_EQUAL(table.get_subtable_size(1, 0), 0);
    CHECK(table.get_subtable(1, 0));

    subtab1->insert_int(0, 0, 0);
    subtab1->insert_done();

    subtab2->insert_int(0, 0, 0);
    subtab2->insert_done();

    CHECK_EQUAL(table.get_subtable_size(0,0), 1);
    CHECK_EQUAL(table.get_subtable_size(1,0), 1);

    table.clear_subtable(0, 0);
    table.clear_subtable(1, 0);

    CHECK_EQUAL(table.get_subtable_size(0,0), 0);
    CHECK_EQUAL(table.get_subtable_size(1,0), 0);

    CHECK(table.get_subtable(1, 0));
}


namespace
{
    TIGHTDB_TABLE_2(MyTable1,
                    val, Int,
                    val2, Int)

    TIGHTDB_TABLE_2(MyTable2,
                    val, Int,
                    subtab, Subtable<MyTable1>)

    TIGHTDB_TABLE_1(MyTable3,
                    subtab, Subtable<MyTable2>)
}


TEST(Table_SetMethod)
{
    MyTable1 t;
    t.add(8, 9);
    CHECK_EQUAL(t[0].val,  8);
    CHECK_EQUAL(t[0].val2, 9);
    t.set(0, 2, 4);
    CHECK_EQUAL(t[0].val,  2);
    CHECK_EQUAL(t[0].val2, 4);
}


TEST(Table_HighLevelSubtables)
{
    MyTable3 t;
    {
        MyTable3::Ref r1 = t.get_table_ref();
        MyTable3::ConstRef r2 = t.get_table_ref();
        MyTable3::ConstRef r3 = r2->get_table_ref();
        r3 = t.get_table_ref(); // Also test assigment that converts to const
        static_cast<void>(r1);
        static_cast<void>(r3);
    }

    t.add();
    const MyTable3& ct = t;
    {
        MyTable2::Ref       s1 = t[0].subtab;
        MyTable2::ConstRef  s2 = t[0].subtab;
        MyTable2::Ref       s3 = t[0].subtab->get_table_ref();
        MyTable2::ConstRef  s4 = t[0].subtab->get_table_ref();
        MyTable2::Ref       s5 = t.column().subtab[0];
        MyTable2::ConstRef  s6 = t.column().subtab[0];
        MyTable2::Ref       s7 = t.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef  s8 = t.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef cs1 = ct[0].subtab;
        MyTable2::ConstRef cs2 = ct[0].subtab->get_table_ref();
        MyTable2::ConstRef cs3 = ct.column().subtab[0];
        MyTable2::ConstRef cs4 = ct.column().subtab[0]->get_table_ref();
        s1 = t[0].subtab;
        s2 = t[0].subtab; // Also test assigment that converts to const
        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(s5);
        static_cast<void>(s6);
        static_cast<void>(s7);
        static_cast<void>(s8);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
        static_cast<void>(cs3);
        static_cast<void>(cs4);
    }

    t[0].subtab->add();
    {
        MyTable1::Ref       s1 = t[0].subtab[0].subtab;
        MyTable1::ConstRef  s2 = t[0].subtab[0].subtab;
        MyTable1::Ref       s3 = t[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef  s4 = t[0].subtab[0].subtab->get_table_ref();
        MyTable1::Ref       s5 = t.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef  s6 = t.column().subtab[0]->column().subtab[0];
        MyTable1::Ref       s7 = t.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef  s8 = t.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef cs1 = ct[0].subtab[0].subtab;
        MyTable1::ConstRef cs2 = ct[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef cs3 = ct.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef cs4 = ct.column().subtab[0]->column().subtab[0]->get_table_ref();
        s1 = t[0].subtab[0].subtab;
        s2 = t[0].subtab[0].subtab; // Also test assigment that converts to const
        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(s5);
        static_cast<void>(s6);
        static_cast<void>(s7);
        static_cast<void>(s8);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
        static_cast<void>(cs3);
        static_cast<void>(cs4);
    }

    t[0].subtab[0].val = 1;
    CHECK_EQUAL(t[0].subtab[0].val,                     1);
    CHECK_EQUAL(t.column().subtab[0]->column().val[0],  1);
    CHECK_EQUAL(t[0].subtab->column().val[0],           1);
    CHECK_EQUAL(t.column().subtab[0][0].val,            1);

    t.column().subtab[0]->column().val[0] = 2;
    CHECK_EQUAL(t[0].subtab[0].val,                     2);
    CHECK_EQUAL(t.column().subtab[0]->column().val[0],  2);
    CHECK_EQUAL(t[0].subtab->column().val[0],           2);
    CHECK_EQUAL(t.column().subtab[0][0].val,            2);

    t[0].subtab->column().val[0] = 3;
    CHECK_EQUAL(t[0].subtab[0].val,                     3);
    CHECK_EQUAL(t.column().subtab[0]->column().val[0],  3);
    CHECK_EQUAL(t[0].subtab->column().val[0],           3);
    CHECK_EQUAL(t.column().subtab[0][0].val,            3);

    t.column().subtab[0][0].val = 4;
    CHECK_EQUAL(t[0].subtab[0].val,                     4);
    CHECK_EQUAL(t.column().subtab[0]->column().val[0],  4);
    CHECK_EQUAL(t[0].subtab->column().val[0],           4);
    CHECK_EQUAL(t.column().subtab[0][0].val,            4);
    CHECK_EQUAL(ct[0].subtab[0].val,                    4);
    CHECK_EQUAL(ct.column().subtab[0]->column().val[0], 4);
    CHECK_EQUAL(ct[0].subtab->column().val[0],          4);
    CHECK_EQUAL(ct.column().subtab[0][0].val,           4);

    t[0].subtab[0].subtab->add();
    t[0].subtab[0].subtab[0].val = 5;
    CHECK_EQUAL(t[0].subtab[0].subtab[0].val,                               5);
    CHECK_EQUAL(t.column().subtab[0]->column().subtab[0]->column().val[0],  5);
    CHECK_EQUAL(ct[0].subtab[0].subtab[0].val,                              5);
    CHECK_EQUAL(ct.column().subtab[0]->column().subtab[0]->column().val[0], 5);

    t.column().subtab[0]->column().subtab[0]->column().val[0] = 6;
    CHECK_EQUAL(t[0].subtab[0].subtab[0].val,                               6);
    CHECK_EQUAL(t.column().subtab[0]->column().subtab[0]->column().val[0],  6);
    CHECK_EQUAL(ct[0].subtab[0].subtab[0].val,                              6);
    CHECK_EQUAL(ct.column().subtab[0]->column().subtab[0]->column().val[0], 6);

/*
  Idea for compile time failure tests:

    const MyTable2 t;
#if    TEST_INDEX == 0
    t[0].val = 7;
#elsif TEST_INDEX == 1
    t.column().val[0] = 7;
#elsif TEST_INDEX == 2
    t[0].subtab[0].val = 7;
#elsif TEST_INDEX == 3
    t[0].subtab->column().val[0] = 7;
#endif
*/
}


namespace
{
    TIGHTDB_TABLE_2(TableDateAndBinary,
                    date, Date,
                    bin, Binary)
}

TEST(Table_DateAndBinary)
{
    TableDateAndBinary t;

    const size_t size = 10;
    char data[size];
    for (size_t i=0; i<size; ++i) data[i] = (char)i;
    t.add(8, BinaryData(data, size));
    CHECK_EQUAL(t[0].date, 8);
    CHECK_EQUAL(t[0].bin.get_len(), size);
    CHECK(std::equal(t[0].bin.get_pointer(), t[0].bin.get_pointer()+size, data));
}

// Test for a specific bug found: Calling clear on a group with a table with a subtable
TEST(Table_Test_Clear_With_Subtable_AND_Group)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_STRING, "name");
    Spec sub = s.add_subtable_column("sub");
        sub.add_column(COLUMN_TYPE_INT, "num");
    table->update_from_spec();

    CHECK_EQUAL(2, table->get_column_count());

    // Add a row
    table->insert_string(0, 0, "Foo");
    table->insert_subtable(1, 0);
    table->insert_done();

    CHECK_EQUAL(0, table->get_subtable_size(1, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(1, 0);
        CHECK(subtable->is_empty());

        subtable->insert_int(0, 0, 123);
        subtable->insert_done();

        CHECK_EQUAL(123, subtable->get_int(0, 0));
    }

    CHECK_EQUAL(1, table->get_subtable_size(1, 0));

    table->clear();
}


TEST(Table_SubtableWithParentChange)
{
    // FIXME: Also check that when a freestanding table is destroyed, it invalidates all its subtable wrappers.
    // FIXME: Also check that there is no memory corruption or bad read if a non-null TableRef outlives its root table or group.
    MyTable3 table;
    table.add();
    table.add();
    MyTable2::Ref subtab = table[1].subtab;
    subtab->add(7, 0);
    CHECK(table.is_valid());
    CHECK(subtab->is_valid());
    CHECK_EQUAL(subtab, MyTable2::Ref(table[1].subtab));
    CHECK_EQUAL(table[1].subtab[0].val, 7);
    CHECK_EQUAL(subtab[0].val, 7);
    CHECK(subtab->is_valid());
#ifdef TIGHTDB_DEBUG
    table.Verify();
    subtab->Verify();
#endif
    CHECK(table.is_valid());
    CHECK(subtab->is_valid());
    table.insert(0, 0);
    CHECK(table.is_valid());
    CHECK(!subtab->is_valid());
    subtab = table[2].subtab;
    CHECK(subtab->is_valid());
    table.remove(1);
    CHECK(!subtab->is_valid());
    subtab = table[1].subtab;
    CHECK(table.is_valid());
    CHECK(subtab->is_valid());
}

#include <tightdb/lang_bind_helper.hpp>

TEST(Table_LanguageBindings)
{
   Table* table = LangBindHelper::new_table();
   CHECK(table->is_valid());
   LangBindHelper::unbind_table_ref(table);
}
