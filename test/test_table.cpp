#include "testsettings.hpp"
#ifdef TEST_TABLE

#include <algorithm>
#include <string>
#include <fstream>
#include <ostream>


#include <UnitTest++.h>

#include <tightdb/table_macros.hpp>
#include <tightdb/lang_bind_helper.hpp>
#include <tightdb/alloc_slab.hpp>
#include <tightdb/group.hpp>

#include "testsettings.hpp"
#include "util/misc.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace test_util;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

namespace {
TIGHTDB_TABLE_2(TupleTableType,
                first,  Int,
                second, String)
}

#ifdef JAVA_MANY_COLUMNS_CRASH

TIGHTDB_TABLE_3(SubtableType,
                year,  Int,
                daysSinceLastVisit, Int,
                conceptId, String)

TIGHTDB_TABLE_7(MainTableType,
                patientId, String,
                gender, Int,
                ethnicity, Int,
                yearOfBirth, Int,
                yearOfDeath, Int,
                zipCode, String,
                events, Subtable<SubtableType>)

TEST(ManyColumnsCrash2)
{
    // Trying to reproduce Java crash. It currently fails to trigger the bug, though.
    for(int a = 0; a < 10; a++)
    {
        Group group;

        MainTableType::Ref mainTable = group.get_table<MainTableType>("PatientTable");
        TableRef dynPatientTable = group.get_table("PatientTable");
        dynPatientTable->add_empty_row();

        for (int counter = 0; counter < 20000; counter++)
        {
#if 0
            // Add row to subtable through typed interface
            SubtableType::Ref subtable = mainTable[0].events->get_table_ref();
            TIGHTDB_ASSERT(subtable->is_attached());
            subtable->add(0, 0, "");
            TIGHTDB_ASSERT(subtable->is_attached());

#else
            // Add row to subtable through dynamic interface. This mimics Java closest
            TableRef subtable2 = dynPatientTable->get_subtable(6, 0);
            TIGHTDB_ASSERT(subtable2->is_attached());
            size_t subrow = subtable2->add_empty_row();
            TIGHTDB_ASSERT(subtable2->is_attached());

#endif
            if((counter % 1000) == 0){
           //     cerr << counter << "\n";
            }
        }
    }
}

#if 0
ONLY(ManyColumnsCrash) {
    // Trying to reproduce crash in Java code. This test has been disabled because it fails to crash, and because a
    // much simpler Java snippet also makes it crash (see above test).
    for(int a = 0; a < 100; a++)
    {

        Group* group = new Group("d:/master/pfm.tightdb");
        TableRef dynPatientTable = group->get_table("PatientTable");

        for (int counter =0;counter<70000;  counter++)
        {

            int obfuscatedYear = (counter % 5);
            int daysSinceLastVisit = (counter % 5);
            char buf[100];
            sprintf(buf, "CC%d", counter % 1000);
            StringData conceptId = buf;


            // check if the patient exists
            size_t patient = counter % 100;
            size_t t = dynPatientTable->get_column_index(conceptId);
            if(t == -1)
            {
                // create the event
#if 1

                PatientTableType::Ref table = group->get_table<PatientTableType>("events");
                table->add(obfuscatedYear, daysSinceLastVisit, conceptId);
#else
                TableRef subtable = dynPatientTable->get_subtable(6, patient);
                size_t subrow = subtable->add_empty_row();

                subtable->set_int(0, subrow, obfuscatedYear);
                subtable->set_int(1, subrow, daysSinceLastVisit);
                subtable->set_string(2, subrow, conceptId);
#endif
            }

            // update the patient bitmap
            size_t conceptColIndex = dynPatientTable->add_column(type_Bool, conceptId);

            if((counter % 1000) == 0){
                cerr << counter << "\n";
            }
        }

    }
}
#endif
#endif

TEST(DeleteCrash)
{
    Group group;
    TableRef table = group.get_table("test");

    table->add_column(type_String, "name");
    table->add_column(type_Int,    "age");

    table->add_empty_row(3);
    table->set_string(0, 0, "Alice");
    table->set_int(1, 0, 27);

    table->set_string(0, 1, "Bob");
    table->set_int(1, 1, 50);

    table->set_string(0, 2, "Peter");
    table->set_int(1, 2, 44);

    table->remove(0);

    table->remove(1);
}


TEST(TestOptimizeCrash)
{
    // This will crash at the .add() method
    TupleTableType ttt;
    ttt.optimize();
    ttt.column().second.set_index();
    ttt.clear();
    ttt.add(1, "AA");
}

TEST(Table1)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    CHECK_EQUAL(type_Int, table.get_column_type(0));
    CHECK_EQUAL(type_Int, table.get_column_type(1));
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
#endif
}

TEST(Table_floats)
{
    Table table;
    table.add_column(type_Float, "first");
    table.add_column(type_Double, "second");

    CHECK_EQUAL(type_Float, table.get_column_type(0));
    CHECK_EQUAL(type_Double, table.get_column_type(1));
    CHECK_EQUAL("first", table.get_column_name(0));
    CHECK_EQUAL("second", table.get_column_name(1));

    // Test adding a single empty row
    // and filling it with values
    size_t ndx = table.add_empty_row();
    table.set_float(0, ndx, float(1.12));
    table.set_double(1, ndx, double(102.13));

    CHECK_EQUAL(float(1.12), table.get_float(0, ndx));
    CHECK_EQUAL(double(102.13), table.get_double(1, ndx));

    // Test adding multiple rows
    ndx = table.add_empty_row(7);
    for (size_t i = ndx; i < 7; ++i) {
        table.set_float(0, i, float(1.12) + 100*i);
        table.set_double(1, i, double(102.13)*200*i);
    }

    for (size_t i = ndx; i < 7; ++i) {
        const float v1  = float(1.12) + 100*i;
        const double v2 = double(102.13)*200*i;
        CHECK_EQUAL(v1, table.get_float(0, i));
        CHECK_EQUAL(v2, table.get_double(1, i));
    }

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
}

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

TIGHTDB_TABLE_4(TestTable,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

} // anonymous namespace

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
#endif
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
#endif
}

namespace {
TIGHTDB_TABLE_2(TestTableEnum,
                first,      Enum<Days>,
                second,     String)
} // anonymous namespace

TEST(Table4)
{
    TestTableEnum table;

    table.add(Mon, "Hello");
    table.add(Mon, "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello");
    const TestTableEnum::Cursor r = table.back(); // last item

    CHECK_EQUAL(Mon, r.first);
    CHECK_EQUAL("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello", r.second);

    // Test string column searching
    CHECK_EQUAL(size_t(1),  table.column().second.find_first("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello"));
    CHECK_EQUAL(size_t(-1), table.column().second.find_first("Foo"));

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
}

namespace {
TIGHTDB_TABLE_2(TestTableFloats,
                first,      Float,
                second,     Double)
} // anonymous namespace

TEST(Table_float2)
{
    TestTableFloats table;

    table.add(1.1f, 2.2);
    table.add(1.1f, 2.2);
    const TestTableFloats::Cursor r = table.back(); // last item

    CHECK_EQUAL(1.1f, r.first);
    CHECK_EQUAL(2.2, r.second);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
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
#endif

    // Delete all items one at a time
    for (size_t i = 0; i < 7; ++i) {
        table.remove(0);
    }

    CHECK(table.is_empty());
    CHECK_EQUAL(0, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
}


TEST(Table_LowLevelCopy)
{
    Table table;
    table.add_column(type_Int, "i1");
    table.add_column(type_Int, "i2");
    table.add_column(type_Int, "xxxxxxxxxxxxxxxxxxxxxxx");

    table.insert_int(0, 0, 10);
    table.insert_int(1, 0, 120);
    table.insert_int(2, 0, 1230);
    table.insert_done();
    table.insert_int(0, 1, 12);
    table.insert_int(1, 1, 100);
    table.insert_int(2, 1, 1300);
    table.insert_done();

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif

    Table table2 = table;

#ifdef TIGHTDB_DEBUG
    table2.Verify();
#endif

    CHECK(table2 == table);

    TableRef table3 = table.copy();

#ifdef TIGHTDB_DEBUG
    table3->Verify();
#endif

    CHECK(*table3 == table);
}


TEST(Table_HighLevelCopy)
{
    TestTable table;
    table.add(10, 120, false, Mon);
    table.add(12, 100, true,  Tue);

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif

    TestTable table2 = table;

#ifdef TIGHTDB_DEBUG
    table2.Verify();
#endif

    CHECK(table2 == table);

    TestTable::Ref table3 = table.copy();

#ifdef TIGHTDB_DEBUG
    table3->Verify();
#endif

    CHECK(*table3 == table);
}


namespace {

void setup_multi_table(Table& table, size_t rows, size_t sub_rows)
{
    // Create table with all column types
    table.add_column(type_Int,      "int");
    table.add_column(type_Bool,     "bool");
    table.add_column(type_DateTime, "date");
    table.add_column(type_Float,    "float");
    table.add_column(type_Double,   "double");
    table.add_column(type_String,   "string");
    table.add_column(type_String,   "string_long");
    table.add_column(type_String,   "string_enum"); // becomes ColumnStringEnum
    table.add_column(type_Binary,   "binary");
    table.add_column(type_Mixed,    "mixed");
    table.add_column(type_Table,    "tables");
    table.add_subcolumn(tuple(10), type_Int,    "sub_first");
    table.add_subcolumn(tuple(10), type_String, "sub_second");

    // Add some rows
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        table.insert_int(0, i, int64_t(i*sign));
        table.insert_bool(1, i, (i % 2 ? true : false));
        table.insert_datetime(2, i, 12345);
        table.insert_float(3, i, 123.456f*sign);
        table.insert_double(4, i, 9876.54321*sign);

        stringstream ss;
        ss << "string" << i;
        table.insert_string(5, i, ss.str().c_str());

        ss << " very long string.........";
        table.insert_string(6, i, ss.str().c_str());

        switch (i % 3) {
            case 0:
                table.insert_string(7, i, "enum1");
                break;
            case 1:
                table.insert_string(7, i, "enum2");
                break;
            case 2:
                table.insert_string(7, i, "enum3");
                break;
        }

        table.insert_binary(8, i, BinaryData("binary", 7));

        switch (i % 8) {
            case 0:
                table.insert_mixed(9, i, false);
                break;
            case 1:
                table.insert_mixed(9, i, int64_t(i*i*sign));
                break;
            case 2:
                table.insert_mixed(9, i, "string");
                break;
            case 3:
                table.insert_mixed(9, i, DateTime(123456789));
                break;
            case 4:
                table.insert_mixed(9, i, BinaryData("binary", 7));
                break;
            case 5:
            {
                // Add subtable to mixed column
                // We can first set schema and contents when the entire
                // row has been inserted
                table.insert_mixed(9, i, Mixed::subtable_tag());
                break;
            }
            case 6:
                table.insert_mixed(9, i, float(123.1*i*sign));
                break;
            case 7:
                table.insert_mixed(9, i, double(987.65*i*sign));
                break;
        }

        table.insert_subtable(10, i);
        table.insert_done();

        // Add subtable to mixed column
        if (i % 8 == 5) {
            TableRef subtable = table.get_subtable(9, i);
            subtable->add_column(type_Int,    "first");
            subtable->add_column(type_String, "second");
            for (size_t j=0; j<2; j++) {
                subtable->insert_int(0, j, i*i*j*sign);
                subtable->insert_string(1, j, "mixed sub");
                subtable->insert_done();
            }
        }

        // Add sub-tables to table column
        for (size_t j = 0; j != sub_rows+i; ++j) {
            TableRef subtable = table.get_subtable(10, i);
            int64_t val = -123+i*j*1234*sign;
            subtable->insert_int(0, j, val);
            subtable->insert_string(1, j, "sub");
            subtable->insert_done();
        }
    }
    // We also want a ColumnStringEnum
    table.optimize();
}

} // anonymous namespace


TEST(Table_Delete_All_Types)
{
    Table table;
    setup_multi_table(table, 15, 2);

    // Test Deletes
    table.remove(14);
    table.remove(0);
    table.remove(5);

    CHECK_EQUAL(12, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif

    // Test Clear
    table.clear();
    CHECK_EQUAL(0, table.size());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
}

TEST(Table_Move_All_Types)
{
    Table table;
    setup_multi_table(table, 15, 2);
    table.set_index(6);

    while (table.size() > 1) {
        size_t size = table.size();
        size_t ndx = size_t(rand()) % (size-1);

        table.move_last_over(ndx);

#ifdef TIGHTDB_DEBUG
        table.Verify();
#endif
    }
}


TEST(Table_DegenerateSubtableSearchAndAggregate)
{
    Table parent;

    // Add all column types
    parent.add_column(type_Table, "child");
    parent.add_subcolumn(tuple(0), type_Int,      "int");    // 0
    parent.add_subcolumn(tuple(0), type_Bool,     "bool");   // 1
    parent.add_subcolumn(tuple(0), type_Float,    "float");  // 2
    parent.add_subcolumn(tuple(0), type_Double,   "double"); // 3
    parent.add_subcolumn(tuple(0), type_DateTime, "date");   // 4
    parent.add_subcolumn(tuple(0), type_String,   "string"); // 5
    parent.add_subcolumn(tuple(0), type_Binary,   "binary"); // 6
    parent.add_subcolumn(tuple(0), type_Table,    "table");  // 7
    parent.add_subcolumn(tuple(0), type_Mixed,    "mixed");  // 8
    parent.add_subcolumn(tuple(0,7), type_Int, "i");

    parent.add_empty_row(); // Create a degenerate subtable

    ConstTableRef degen_child = parent.get_subtable(0,0); // NOTE: Constness is essential here!!!

    CHECK_EQUAL(0, degen_child->size());
    CHECK_EQUAL(9, degen_child->get_column_count());

    // Searching:

    CHECK_EQUAL(not_found, degen_child->lookup(StringData()));
//    CHECK_EQUAL(0, degen_child->distinct(0).size()); // needs index but you cannot set index on ConstTableRef
    CHECK_EQUAL(0, degen_child->get_sorted_view(0).size());

    CHECK_EQUAL(not_found, degen_child->find_first_int(0, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_bool(1, false));
    CHECK_EQUAL(not_found, degen_child->find_first_float(2, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_double(3, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_datetime(4, DateTime()));
    CHECK_EQUAL(not_found, degen_child->find_first_string(5, StringData()));
//    CHECK_EQUAL(not_found, degen_child->find_first_binary(6, BinaryData())); // Exists but not yet implemented
//    CHECK_EQUAL(not_found, degen_child->find_first_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(not_found, degen_child->find_first_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->find_all_int(0, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_bool(1, false).size());
    CHECK_EQUAL(0, degen_child->find_all_float(2, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_double(3, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_datetime(4, DateTime()).size());
    CHECK_EQUAL(0, degen_child->find_all_string(5, StringData()).size());
//    CHECK_EQUAL(0, degen_child->find_all_binary(6, BinaryData()).size()); // Exists but not yet implemented
//    CHECK_EQUAL(0, degen_child->find_all_subtable(7, subtab).size()); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->find_all_mixed(8, Mixed()).size()); // Not yet implemented

    CHECK_EQUAL(0, degen_child->lower_bound_int(0, 0));
    CHECK_EQUAL(0, degen_child->lower_bound_bool(1, false));
    CHECK_EQUAL(0, degen_child->lower_bound_float(2, 0));
    CHECK_EQUAL(0, degen_child->lower_bound_double(3, 0));
//    CHECK_EQUAL(0, degen_child->lower_bound_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->lower_bound_string(5, StringData()));
//    CHECK_EQUAL(0, degen_child->lower_bound_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->lower_bound_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->lower_bound_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->upper_bound_int(0, 0));
    CHECK_EQUAL(0, degen_child->upper_bound_bool(1, false));
    CHECK_EQUAL(0, degen_child->upper_bound_float(2, 0));
    CHECK_EQUAL(0, degen_child->upper_bound_double(3, 0));
//    CHECK_EQUAL(0, degen_child->upper_bound_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->upper_bound_string(5, StringData()));
//    CHECK_EQUAL(0, degen_child->upper_bound_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->upper_bound_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->upper_bound_mixed(8, Mixed())); // Not yet implemented


    // Aggregates:

    CHECK_EQUAL(0, degen_child->count_int(0, 0));
//    CHECK_EQUAL(0, degen_child->count_bool(1, false)); // Not yet implemented
    CHECK_EQUAL(0, degen_child->count_float(2, 0));
    CHECK_EQUAL(0, degen_child->count_double(3, 0));
//    CHECK_EQUAL(0, degen_child->count_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->count_string(5, StringData()));
//    CHECK_EQUAL(0, degen_child->count_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->count_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->count_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->minimum_int(0));
    CHECK_EQUAL(0, degen_child->minimum_float(2));
    CHECK_EQUAL(0, degen_child->minimum_double(3));
//    CHECK_EQUAL(Date(), degen_child->minimum_date(4, Date())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->maximum_int(0));
    CHECK_EQUAL(0, degen_child->maximum_float(2));
    CHECK_EQUAL(0, degen_child->maximum_double(3));
//    CHECK_EQUAL(Date(), degen_child->maximum_date(4, Date())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->sum_int(0));
    CHECK_EQUAL(0, degen_child->sum_float(2));
    CHECK_EQUAL(0, degen_child->sum_double(3));

    CHECK_EQUAL(0, degen_child->average_int(0));
    CHECK_EQUAL(0, degen_child->average_float(2));
    CHECK_EQUAL(0, degen_child->average_double(3));


    // Queries:
    CHECK_EQUAL(not_found, degen_child->where().equal(0, int64_t()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(1, false).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(2, float()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(3, double()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal_datetime(4, DateTime()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(5, StringData()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(6, BinaryData()).find());
//    CHECK_EQUAL(not_found, degen_child->where().equal(7, subtab).find()); // Not yet implemented
//    CHECK_EQUAL(not_found, degen_child->where().equal(8, Mixed()).find()); // Not yet implemented

    CHECK_EQUAL(not_found, degen_child->where().not_equal(0, int64_t()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(2, float()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(3, double()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal_datetime(4, DateTime()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(5, StringData()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(6, BinaryData()).find());
//    CHECK_EQUAL(not_found, degen_child->where().not_equal(7, subtab).find()); // Not yet implemented
//    CHECK_EQUAL(not_found, degen_child->where().not_equal(8, Mixed()).find()); // Not yet implemented

    TableView v = degen_child->where().equal(0, int64_t()).find_all();
    CHECK_EQUAL(0, v.size());

    v = degen_child->where().equal(5, "hello").find_all();
    CHECK_EQUAL(0, v.size());

    size_t r = degen_child->where().equal(5, "hello").count();
    CHECK_EQUAL(0, r);

    r = degen_child->where().equal(5, "hello").remove();
    CHECK_EQUAL(0, r);

    size_t res;
    degen_child->where().equal(5, "hello").average_int(0, &res);
    CHECK_EQUAL(0, res);
}


// enable to generate testfiles for to_string and json below
#define GENERATE 0

TEST(Table_test_to_string)
{
    Table table;
    setup_multi_table(table, 15, 6);

    stringstream ss;
    table.to_string(ss);
    const string result = ss.str();
#if _MSC_VER
    const char* filename = "expect_string-win.txt";
#else
    const char* filename = "expect_string.txt";
#endif
#if GENERATE   // enable to generate testfile - check it manually
    ofstream testFile(filename, ios::out);
    testFile << result;
    cerr << "to_string() test:\n" << result << endl;
#else
    ifstream testFile(filename, ios::in);
    CHECK(!testFile.fail());
    string expected;
    expected.assign( istreambuf_iterator<char>(testFile),
                     istreambuf_iterator<char>() );
    bool test_ok = test_util::equal_without_cr(result, expected);
    CHECK_EQUAL(true, test_ok);
    if (!test_ok) {
        ofstream testFile("expect_string.error.txt", ios::out | ios::binary);
        testFile << result;
        cerr << "\n error result in 'expect_string.error.txt'\n";
    }
#endif
}

TEST(Table_test_json_all_data)
{
    Table table;
    setup_multi_table(table, 15, 2);

    stringstream ss;
    table.to_json(ss);
    const string json = ss.str();
#if _MSC_VER
    const char* filename = "expect_json-win.json";
#else
    const char* filename = "expect_json.json";
#endif
#if GENERATE
        // Generate the testdata to compare. After doing this,
        // verify that the output is correct with a json validator:
        // http://jsonformatter.curiousconcept.com/
    cerr << "JSON:" << json << "\n";
    ofstream testFile(filename, ios::out | ios::binary);
    testFile << json;
#else
    string expected;
    ifstream testFile(filename, ios::in | ios::binary);
    CHECK(!testFile.fail());
    getline(testFile,expected);
    CHECK_EQUAL(true, json == expected);
    if (json != expected) {
        ofstream testFile("expect_json.error.json", ios::out | ios::binary);
        testFile << json;
        cerr << "\n error result in 'expect_json.error.json'\n";
    }
#endif
}


/* DISABLED BECAUSE IT FAILS - A PULL REQUEST WILL BE MADE WHERE IT IS REENABLED!
TEST(Table_test_row_to_string)
{
    // Create table with all column types
    Table table;
    setup_multi_table(table, 2, 2);

    stringstream ss;
    table.row_to_string(1, ss);
    const string row_str = ss.str();
#if 0
    ofstream testFile("row_to_string.txt", ios::out);
    testFile << row_str;
#endif

    string expected = "    int   bool                 date           float          double   string              string_long  string_enum     binary  mixed  tables\n"
                      "1:   -1   true  1970-01-01 03:25:45  -1.234560e+002  -9.876543e+003  string1  string1 very long st...  enum2          7 bytes     -1     [3]\n";
    bool test_ok = test_util::equal_without_cr(row_str, expected);
    CHECK_EQUAL(true, test_ok);
    if (!test_ok) {
        cerr << "row_to_string() failed\n"
             << "Expected: " << expected << "\n"
             << "Got     : " << row_str << endl;
    }
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
#endif
}
*/


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
#endif
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
#endif
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
#endif
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

TEST(Table_Index_String_Twice)
{
    TestTableEnum table;

    table.add(Mon, "jeff");
    table.add(Tue, "jim");
    table.add(Wed, "jennifer");
    table.add(Thu, "john");
    table.add(Fri, "jimmy");
    table.add(Sat, "jimbo");
    table.add(Sun, "johnny");
    table.add(Mon, "jennifer"); // duplicate

    table.column().second.set_index();
    CHECK_EQUAL(true, table.column().second.has_index());
    table.column().second.set_index();
    CHECK_EQUAL(true, table.column().second.has_index());
}

namespace {
TIGHTDB_TABLE_2(LookupTable,
                first,  String,
                second, Int)
} // anonymous namespace

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

namespace {
TIGHTDB_TABLE_1(TestSubtableLookup2,
                str, String)
TIGHTDB_TABLE_1(TestSubtableLookup1,
                subtab, Subtable<TestSubtableLookup2>)
} // anonymous namespace


TEST(Table_SubtableLookup)
{
    TestSubtableLookup1 t;
    t.add();
    t.add();
    {
        TestSubtableLookup2::Ref r0 = t[0].subtab;
        r0->add("foo");
        r0->add("bar");
        size_t i1 = r0->lookup("bar");
        CHECK_EQUAL(1, i1);
        size_t i2 = r0->lookup("foobar");
        CHECK_EQUAL(not_found, i2);
    }

    {
        TestSubtableLookup2::Ref r1 = t[1].subtab;
        size_t i3 = r1->lookup("bar");
        CHECK_EQUAL(not_found, i3);
    }
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

    TestTableEnum::View view = table.column().second.get_distinct_view();

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
#endif
}
*/

namespace {
TIGHTDB_TABLE_4(TestTableAE,
                first,  Int,
                second, String,
                third,  Bool,
                fourth, Enum<Days>)
} // anonymous namespace

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

        CHECK_EQUAL("abd",     table[0+n].second);
        CHECK_EQUAL("eftg",    table[1+n].second);
        CHECK_EQUAL("hijkl",   table[2+n].second);
        CHECK_EQUAL("mnopqr",  table[3+n].second);
        CHECK_EQUAL("stuvxyz", table[4+n].second);

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
    CHECK_EQUAL("eftg", tv[0].second);
    CHECK_EQUAL("eftg", tv[1].second);
    CHECK_EQUAL("eftg", tv[2].second);
    CHECK_EQUAL("eftg", tv[3].second);
    CHECK_EQUAL("eftg", tv[4].second);
}

namespace {
TIGHTDB_TABLE_4(TestTableEnum4,
                col1, String,
                col2, String,
                col3, String,
                col4, String)
} // anonymous namespace

TEST(TableAutoEnumerationOptimize)
{
    TestTableEnum4 t;

    // Insert non-optimzable strings
    string s;
    for (size_t i = 0; i < 10; ++i) {
        t.add(s.c_str(), s.c_str(), s.c_str(), s.c_str());
        s += "x";
    }
    t.optimize();

    // AutoEnumerate in reverse order
    for (size_t i = 0; i < 10; ++i) {
        t[i].col4 = "test";
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t[i].col3 = "test";
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t[i].col2 = "test";
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t[i].col1 = "test";
    }
    t.optimize();

    for (size_t i = 0; i < 10; ++i) {
        CHECK_EQUAL("test", t[i].col1);
        CHECK_EQUAL("test", t[i].col2);
        CHECK_EQUAL("test", t[i].col3);
        CHECK_EQUAL("test", t[i].col4);
    }

#ifdef TIGHTDB_DEBUG
    t.Verify();
#endif
}

namespace {
TIGHTDB_TABLE_1(TestSubtabEnum2,
                str, String)
TIGHTDB_TABLE_1(TestSubtabEnum1,
                subtab, Subtable<TestSubtabEnum2>)
} // anonymous namespace

TEST(Table_OptimizeSubtable)
{
    TestSubtabEnum1 t;
    t.add();
    t.add();

    {
        // Non-enumerable
        TestSubtabEnum2::Ref r = t[0].subtab;
        string s;
        for (int i=0; i<100; ++i) {
            r->add(s.c_str());
            s += 'x';
        }
    }

    {
        // Enumerable
        TestSubtabEnum2::Ref r = t[1].subtab;
        for (int i=0; i<100; ++i) {
            r->add("foo");
        }
        r->optimize();
    }

    // Verify
    {
        // Non-enumerable
        TestSubtabEnum2::Ref r = t[0].subtab;
        string s;
        for (size_t i = 0; i < r->size(); ++i) {
            CHECK_EQUAL(s.c_str(), r[i].str);
            s += 'x';
        }
    }
    {
        // Non-enumerable
        TestSubtabEnum2::Ref r = t[1].subtab;
        for (size_t i = 0; i < r->size(); ++i) {
            CHECK_EQUAL("foo", r[i].str);
        }
    }
}

TEST(Table_OptimizeCompare)
{
    TestSubtabEnum2 t1, t2;
    for (int i=0; i<100; ++i) {
        t1.add("foo");
    }
    for (int i=0; i<100; ++i) {
        t2.add("foo");
    }
    t1.optimize();
    CHECK(t1 == t2);
    t1[50].str = "bar";
    CHECK(t1 != t2);
    t1[50].str = "foo";
    CHECK(t1 == t2);
    t2[50].str = "bar";
    CHECK(t1 != t2);
    t2[50].str = "foo";
    CHECK(t1 == t2);
}


TEST(Table_SlabAlloc)
{
    SlabAlloc alloc;
    alloc.attach_empty();
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
#endif
}


TEST(Table_Spec)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,   "third");
    table->add_subcolumn(tuple(2), type_Int,    "sub_first");
    table->add_subcolumn(tuple(2), type_String, "sub_second");

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
    File::try_remove("subtables.tightdb");
    group.write("subtables.tightdb");

    // Read back tables
    {
        Group fromDisk("subtables.tightdb", Group::mode_ReadOnly);
        TableRef fromDiskTable = fromDisk.get_table("test");

        TableRef subtable2 = fromDiskTable->get_subtable(2, 0);

        CHECK_EQUAL(1,      subtable2->size());
        CHECK_EQUAL(42,     subtable2->get_int(0, 0));
        CHECK_EQUAL("test", subtable2->get_string(1, 0));
    }
}

TEST(Table_Spec_ColumnPath)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create path to sub-table column (starting with root)
    vector<size_t> column_path;

    // Create specification with sub-table
    table->add_subcolumn(column_path, type_Int,    "first");
    table->add_subcolumn(column_path, type_String, "second");
    table->add_subcolumn(column_path, type_Table,  "third");

    column_path.push_back(2); // third column (which is a sub-table col)

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);
    table->insert_done();

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
}

TEST(Table_Spec_RenameColumns)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);
    table->insert_done();

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

    // Rename first column
    table->rename_column(0, "1st");
    CHECK_EQUAL(0, table->get_column_index("1st"));

    // Rename sub-column
    column_path.push_back(0); // third
    table->rename_subcolumn(column_path, "sub_1st");

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK_EQUAL(0, subtable->get_column_index("sub_1st"));
    }
}

TEST(Table_Spec_DeleteColumns)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");
    table->add_column(type_String, "fourth"); // will be auto-enumerated

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Put in an index as well
    table->set_index(1);

    CHECK_EQUAL(4, table->get_column_count());

    // Add a few rows
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);
    table->insert_string(3, 0, "X");
    table->insert_done();

    table->insert_int(0, 1, 4);
    table->insert_string(1, 1, "World");
    table->insert_subtable(2, 1);
    table->insert_string(3, 1, "X");
    table->insert_done();

    table->insert_int(0, 2, 4);
    table->insert_string(1, 2, "Goodbye");
    table->insert_subtable(2, 2);
    table->insert_string(3, 2, "X");
    table->insert_done();

    // We want the last column to be StringEnum column
    table->optimize();

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

    // Remove the first column
    table->remove_column(0);
    CHECK_EQUAL(3, table->get_column_count());
    CHECK_EQUAL("Hello", table->get_string(0, 0));
    CHECK_EQUAL("X", table->get_string(2, 0));

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(1, 0);

        CHECK_EQUAL(2,      subtable->get_column_count());
        CHECK_EQUAL(1,      subtable->size());
        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    // Create path to column in sub-table
    column_path.clear();
    column_path.push_back(1); // third
    column_path.push_back(1); // sub_second

    // Remove a column in sub-table
    table->remove_subcolumn(column_path);

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(1, 0);

        CHECK_EQUAL(1,      subtable->get_column_count());
        CHECK_EQUAL(1,      subtable->size());
        CHECK_EQUAL(42,     subtable->get_int(0, 0));
    }

    // Remove sub-table column (with all members)
    table->remove_column(1);
    CHECK_EQUAL(2, table->get_column_count());
    CHECK_EQUAL("Hello", table->get_string(0, 0));
    CHECK_EQUAL("X", table->get_string(1, 0));

    // Remove optimized string column
    table->remove_column(1);
    CHECK_EQUAL(1, table->get_column_count());
    CHECK_EQUAL("Hello", table->get_string(0, 0));

    // Remove last column
    table->remove_column(0);
    CHECK_EQUAL(0, table->get_column_count());
    CHECK(table->is_empty());

#ifdef TIGHTDB_DEBUG
    table->Verify();
#endif
}


TEST(Table_Spec_AddColumns)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Put in an index as well
    table->set_index(1);

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

    // Add a new bool column
    table->add_column(type_Bool, "fourth");
    CHECK_EQUAL(4, table->get_column_count());
    CHECK_EQUAL(false, table->get_bool(3, 0));

    // Add a new string column
    table->add_column(type_String, "fifth");
    CHECK_EQUAL(5, table->get_column_count());
    CHECK_EQUAL("", table->get_string(4, 0));

    // Add a new table column
    table->add_column(type_Table, "sixth");
    CHECK_EQUAL(6, table->get_column_count());
    CHECK_EQUAL(0, table->get_subtable_size(5, 0));

    // Add a new mixed column
    table->add_column(type_Mixed, "seventh");
    CHECK_EQUAL(7, table->get_column_count());
    CHECK_EQUAL(0, table->get_mixed(6, 0).get_int());

    // Create path to column in sub-table
    column_path.clear();
    column_path.push_back(2); // third

    // Add new int column to sub-table
    table->add_subcolumn(column_path, type_Int, "sub_third");

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);

        CHECK_EQUAL(3,      subtable->get_column_count());
        CHECK_EQUAL(1,      subtable->size());
        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
        CHECK_EQUAL(0,      subtable->get_int(2, 0));
    }

    // Add new table column to sub-table
    table->add_subcolumn(column_path, type_Table, "sub_fourth");

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);

        CHECK_EQUAL(4,      subtable->get_column_count());
        CHECK_EQUAL(1,      subtable->size());
        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
        CHECK_EQUAL(0,      subtable->get_int(2, 0));
        CHECK_EQUAL(0,      subtable->get_subtable_size(3, 0));
        CHECK_EQUAL(1,      table->get_subtable_size(2, 0));
    }

    // Add new column to new sub-table
    column_path.push_back(3); // sub_forth
    table->add_subcolumn(column_path, type_String, "first");

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK_EQUAL(4,      subtable->get_column_count());

        TableRef subsubtable = subtable->get_subtable(3, 0);
        CHECK_EQUAL(1,      subsubtable->get_column_count());
    }

    // Add a new mixed column
    table->add_column(type_Mixed, "eighth");
    CHECK_EQUAL(8, table->get_column_count());
    table->set_mixed(7, 0, Mixed::subtable_tag());
    TableRef stab = table->get_subtable(7, 0);
    stab->add_column(type_Int, "smurf");
    stab->insert_int(0, 0, 1);
    stab->insert_done();
    stab->insert_int(0, 1, 2);
    stab->insert_done();
    CHECK_EQUAL(2, table->get_subtable_size(7, 0));

#ifdef TIGHTDB_DEBUG
    table->Verify();
#endif
}


TEST(Table_Spec_DeleteColumnsBug)
{
    TableRef table = Table::create();

    // Create specification with sub-table
    table->add_column(type_String, "name");
    table->set_index(0);
    table->add_column(type_Int,    "age");
    table->add_column(type_Bool,   "hired");
    table->add_column(type_Table,  "phones");

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(3); // phones

    table->add_subcolumn(column_path, type_String, "type");
    table->add_subcolumn(column_path, type_String, "number");

    // Add rows
    table->add_empty_row();
    table->set_string(0, 0, "jessica");
    table->set_int(1, 0, 22);
    table->set_bool(2, 0, true);
    {
        TableRef phones = table->get_subtable(3, 0);
        phones->add_empty_row();
        phones->set_string(0, 0, "home");
        phones->set_string(1, 0, "232-323-3242");
    }

    table->add_empty_row();
    table->set_string(0, 1, "joe");
    table->set_int(1, 1, 42);
    table->set_bool(2, 1, false);
    {
        TableRef phones = table->get_subtable(3, 0);
        phones->add_empty_row();
        phones->set_string(0, 0, "work");
        phones->set_string(1, 0, "434-434-4343");
    }

    table->add_empty_row();
    table->set_string(0, 1, "jared");
    table->set_int(1, 1, 35);
    table->set_bool(2, 1, true);
    {
        TableRef phones = table->get_subtable(3, 0);
        phones->add_empty_row();
        phones->set_string(0, 0, "home");
        phones->set_string(1, 0, "342-323-3242");

        phones->add_empty_row();
        phones->set_string(0, 0, "school");
        phones->set_string(1, 0, "434-432-5433");
    }

    // Add new column
    table->add_column(type_Mixed, "extra");
    table->set_mixed(4, 0, true);
    table->set_mixed(4, 2, "Random string!");

    // Remove some columns
    table->remove_column(1); // age
    table->remove_column(3); // extra

#ifdef TIGHTDB_DEBUG
    table->Verify();
#endif
}


TEST(Table_Mixed)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Mixed, "second");

    CHECK_EQUAL(type_Int, table.get_column_type(0));
    CHECK_EQUAL(type_Mixed, table.get_column_type(1));
    CHECK_EQUAL("first", table.get_column_name(0));
    CHECK_EQUAL("second", table.get_column_name(1));

    const size_t ndx = table.add_empty_row();
    table.set_int(0, ndx, 0);
    table.set_mixed(1, ndx, true);

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());

    table.insert_int(0, 1, 43);
    table.insert_mixed(1, 1, (int64_t)12);
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, ndx));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,  table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,   table.get_mixed(1, 1).get_int());

    table.insert_int(0, 2, 100);
    table.insert_mixed(1, 2, "test");
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());

    table.insert_int(0, 3, 0);
    table.insert_mixed(1, 3, DateTime(324234));
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(type_Bool,    table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,     table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String,  table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_DateTime,table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_datetime());

    table.insert_int(0, 4, 43);
    table.insert_mixed(1, 4, Mixed(BinaryData("binary", 7)));
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(type_Bool,     table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,      table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String,   table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_DateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_datetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7,      table.get_mixed(1, 4).get_binary().size());

    table.insert_int(0, 5, 0);
    table.insert_mixed(1, 5, Mixed::subtable_tag());
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(0,  table.get_int(0, 5));
    CHECK_EQUAL(type_Bool,     table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,      table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String,   table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_DateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(type_Table,  table.get_mixed(1, 5).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_datetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7,      table.get_mixed(1, 4).get_binary().size());

    // Get table from mixed column and add schema and some values
    TableRef subtable = table.get_subtable(1, 5);
    subtable->add_column(type_String, "name");
    subtable->add_column(type_Int,    "age");

    subtable->insert_string(0, 0, "John");
    subtable->insert_int(1, 0, 40);
    subtable->insert_done();

    // Get same table again and verify values
    TableRef subtable2 = table.get_subtable(1, 5);
    CHECK_EQUAL(1, subtable2->size());
    CHECK_EQUAL("John", subtable2->get_string(0, 0));
    CHECK_EQUAL(40, subtable2->get_int(1, 0));

    // Insert float, double
    table.insert_int(0, 6, 31);
    table.insert_mixed(1, 6, float(1.123));
    table.insert_done();
    table.insert_int(0, 7, 0);
    table.insert_mixed(1, 7, double(2.234));
    table.insert_done();

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0,  table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(0,  table.get_int(0, 5));
    CHECK_EQUAL(31, table.get_int(0, 6));
    CHECK_EQUAL(0,  table.get_int(0, 7));
    CHECK_EQUAL(type_Bool,     table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,      table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String,   table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_DateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(type_Table,  table.get_mixed(1, 5).get_type());
    CHECK_EQUAL(type_Float,  table.get_mixed(1, 6).get_type());
    CHECK_EQUAL(type_Double, table.get_mixed(1, 7).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_datetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7,      table.get_mixed(1, 4).get_binary().size());
    CHECK_EQUAL(float(1.123),  table.get_mixed(1, 6).get_float());
    CHECK_EQUAL(double(2.234), table.get_mixed(1, 7).get_double());

#ifdef TIGHTDB_DEBUG
    table.Verify();
#endif
}


namespace {
TIGHTDB_TABLE_1(TestTableMX,
                first, Mixed)
} // anonymous namespace

TEST(Table_Mixed2)
{
    TestTableMX table;

    table.add(int64_t(1));
    table.add(true);
    table.add(DateTime(1234));
    table.add("test");

    CHECK_EQUAL(type_Int,      table[0].first.get_type());
    CHECK_EQUAL(type_Bool,     table[1].first.get_type());
    CHECK_EQUAL(type_DateTime, table[2].first.get_type());
    CHECK_EQUAL(type_String,   table[3].first.get_type());

    CHECK_EQUAL(1,            table[0].first.get_int());
    CHECK_EQUAL(true,         table[1].first.get_bool());
    CHECK_EQUAL(time_t(1234), table[2].first.get_datetime());
    CHECK_EQUAL("test",       table[3].first.get_string());
}


TEST(Table_SubtableSizeAndClear)
{
    Table table;
    table.add_column(type_Table, "subtab");
    table.add_column(type_Mixed, "mixed");
    table.add_subcolumn(tuple(0), type_Int, "int");

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
    subtab2->add_column(type_Int, "int");

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


TEST(Table_LowLevelSubtables)
{
    Table table;
    vector<size_t> column_path;
    table.add_column(type_Table, "subtab");
    table.add_column(type_Mixed, "mixed");
    column_path.push_back(0);
    table.add_subcolumn(column_path, type_Table, "subtab");
    table.add_subcolumn(column_path, type_Mixed, "mixed");
    column_path.push_back(0);
    table.add_subcolumn(column_path, type_Table, "subtab");
    table.add_subcolumn(column_path, type_Mixed, "mixed");

    table.add_empty_row(2);
    CHECK_EQUAL(2, table.size());
    for (int i_1 = 0; i_1 != 2; ++i_1) {
        TableRef subtab = table.get_subtable(0, i_1);
        subtab->add_empty_row(2 + i_1);
        CHECK_EQUAL(2 + i_1, subtab->size());
        {
            TableRef subsubtab = subtab->get_subtable(0, 0 + i_1);
            subsubtab->add_empty_row(3 + i_1);
            CHECK_EQUAL(3 + i_1, subsubtab->size());

            for (int i_3 = 0; i_3 != 3 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubtab->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab->get_subtable_size(1, i_3)); // Mixed
            }

            subtab->clear_subtable(1, 1 + i_1); // Mixed
            TableRef subsubtab_mix = subtab->get_subtable(1, 1 + i_1);
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(1 + i_1);
            CHECK_EQUAL(1 + i_1, subsubtab_mix->size());

            for (int i_3 = 0; i_3 != 1 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubtab_mix->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab_mix->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(1, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(subtab->get_subtable(0, i_2)));
            CHECK_EQUAL(i_2 == 1 + i_1, bool(subtab->get_subtable(1, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 0 + i_1 ? 3 + i_1 : 0, subtab->get_subtable_size(0, i_2));
            CHECK_EQUAL(i_2 == 1 + i_1 ? 1 + i_1 : 0, subtab->get_subtable_size(1, i_2)); // Mixed
        }

        table.clear_subtable(1, i_1); // Mixed
        TableRef subtab_mix = table.get_subtable(1, i_1);
        vector<size_t> subcol_path;
        subtab_mix->add_column(type_Table, "subtab");
        subtab_mix->add_column(type_Mixed, "mixed");
        subcol_path.push_back(0);
        subtab_mix->add_subcolumn(subcol_path, type_Table, "subtab");
        subtab_mix->add_subcolumn(subcol_path, type_Mixed, "mixed");
        subtab_mix->add_empty_row(3 + i_1);
        CHECK_EQUAL(3 + i_1, subtab_mix->size());
        {
            TableRef subsubtab = subtab_mix->get_subtable(0, 1 + i_1);
            subsubtab->add_empty_row(7 + i_1);
            CHECK_EQUAL(7 + i_1, subsubtab->size());

            for (int i_3 = 0; i_3 != 7 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubtab->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab->get_subtable_size(1, i_3)); // Mixed
            }

            subtab_mix->clear_subtable(1, 2 + i_1); // Mixed
            TableRef subsubtab_mix = subtab_mix->get_subtable(1, 2 + i_1);
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(5 + i_1);
            CHECK_EQUAL(5 + i_1, subsubtab_mix->size());

            for (int i_3 = 0; i_3 != 5 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubtab_mix->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab_mix->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(1, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(subtab_mix->get_subtable(0, i_2)));
            CHECK_EQUAL(i_2 == 2 + i_1, bool(subtab_mix->get_subtable(1, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 1 + i_1 ? 7 + i_1 : 0, subtab_mix->get_subtable_size(0, i_2));
            CHECK_EQUAL(i_2 == 2 + i_1 ? 5 + i_1 : 0, subtab_mix->get_subtable_size(1, i_2)); // Mixed
        }

        CHECK_EQUAL(true, bool(table.get_subtable(0, i_1)));
        CHECK_EQUAL(true, bool(table.get_subtable(1, i_1))); // Mixed
        CHECK_EQUAL(2 + i_1, table.get_subtable_size(0, i_1));
        CHECK_EQUAL(3 + i_1, table.get_subtable_size(1, i_1)); // Mixed
    }
}


namespace {
TIGHTDB_TABLE_2(MyTable1,
                val, Int,
                val2, Int)

TIGHTDB_TABLE_2(MyTable2,
                val, Int,
                subtab, Subtable<MyTable1>)

TIGHTDB_TABLE_1(MyTable3,
                subtab, Subtable<MyTable2>)

TIGHTDB_TABLE_1(MyTable4,
                mix, Mixed)
} // anonymous namespace


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


TEST(Table_SubtableCopyOnSetAndInsert)
{
    MyTable1 t1;
    t1.add(7, 8);
    MyTable2 t2;
    t2.add(9, &t1);
    MyTable1::Ref r1 = t2[0].subtab;
    CHECK(t1 == *r1);
    MyTable4 t4;
    t4.add();
    t4[0].mix.set_subtable(t2);
    MyTable2::Ref r2 = unchecked_cast<MyTable2>(t4[0].mix.get_subtable());
    CHECK(t2 == *r2);
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


namespace {
TIGHTDB_TABLE_2(TableDateAndBinary,
                date, DateTime,
                bin, Binary)
} // anonymous namespace

TEST(Table_DateAndBinary)
{
    TableDateAndBinary t;

    const size_t size = 10;
    char data[size];
    for (size_t i=0; i<size; ++i) data[i] = (char)i;
    t.add(8, BinaryData(data, size));
    CHECK_EQUAL(t[0].date, 8);
    CHECK_EQUAL(t[0].bin.size(), size);
    CHECK(equal(t[0].bin.data(), t[0].bin.data()+size, data));
}

// Test for a specific bug found: Calling clear on a group with a table with a subtable
TEST(Table_Test_Clear_With_Subtable_AND_Group)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_String, "name");
    table->add_column(type_Table,  "sub");
    table->add_subcolumn(tuple(1), type_Int, "num");

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


//set a subtable in an already exisitng row by providing an existing subtable as the example to copy
TEST(Table_SetSubTableByExample)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);
    table->insert_done();

    // create a freestanding table to be used as a source by set_subtable

    Table  sub = Table();
    sub.add_column(type_Int,"sub_first");
    sub.add_column(type_String,"sub_second");
    sub.add_empty_row();
    sub.set_int(0,0,42);
    sub.set_string(1,0,"forty two");
    sub.add_empty_row();
    sub.set_int(0,1,3);
    sub.set_string(1,1,"PI");

    // Get the sub-table back for inspection
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        //add a subtable into the row, resembling the sub we just created
        table->set_subtable(2,0,&sub);

        TableRef subtable2 = table->get_subtable(2, 0);

        CHECK_EQUAL(42,     subtable2->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable2->get_string(1, 0));
        CHECK_EQUAL(3,     subtable2->get_int(0, 1));
        CHECK_EQUAL("PI", subtable2->get_string(1,1));
    }
}

//In the tableview class, set a subtable in an already exisitng row by providing an existing subtable as the example to copy
TEST(TableView_SetSubTableByExample)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add two rows
    table->insert_int(0, 0, 4);
    table->insert_string(1, 0, "Hello");
    table->insert_subtable(2, 0);// create a freestanding table to be used as a source by set_subtable
    table->insert_done();

    table->insert_int(0, 1, 8);
    table->insert_string(1, 1, "Hi!, Hello?");
    table->insert_subtable(2, 1);
    table->insert_done();

    Table  sub = Table();
    sub.add_column(type_Int,"sub_first");
    sub.add_column(type_String,"sub_second");
    sub.add_empty_row();
    sub.set_int(0,0,42);
    sub.set_string(1,0,"forty two");
    sub.add_empty_row();
    sub.set_int(0,1,3);
    sub.set_string(1,1,"PI");

    //create a tableview with the table as source

    TableView view = table->find_all_int(0,8);//select the second of the two rows

    // Verify the sub table is empty
    {
        TableRef subtable = view.get_subtable(2, 0);
        CHECK(subtable->is_empty());

        //add a subtable into the second table row (first view row), resembling the sub we just created
        view.set_subtable(2,0,&sub);

        TableRef subtable2 = view.get_subtable(2, 0);//fetch back the subtable from the view

        CHECK_EQUAL(42,     subtable2->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable2->get_string(1, 0));
        CHECK_EQUAL(3,     subtable2->get_int(0, 1));
        CHECK_EQUAL("PI", subtable2->get_string(1,1));

        TableRef subtable3 = table->get_subtable(2, 1);//fetch back the subtable from the table.

        CHECK_EQUAL(42,     subtable3->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable3->get_string(1, 0));
        CHECK_EQUAL(3,     subtable3->get_int(0, 1));
        CHECK_EQUAL("PI", subtable3->get_string(1,1));
    }
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
    CHECK(table.is_attached());
    CHECK(subtab->is_attached());
    CHECK_EQUAL(subtab, MyTable2::Ref(table[1].subtab));
    CHECK_EQUAL(table[1].subtab[0].val, 7);
    CHECK_EQUAL(subtab[0].val, 7);
    CHECK(subtab->is_attached());
#ifdef TIGHTDB_DEBUG
    table.Verify();
    subtab->Verify();
#endif
    CHECK(table.is_attached());
    CHECK(subtab->is_attached());
    table.insert(0, 0);
    CHECK(table.is_attached());
    CHECK(!subtab->is_attached());
    subtab = table[2].subtab;
    CHECK(subtab->is_attached());
    table.remove(1);
    CHECK(!subtab->is_attached());
    subtab = table[1].subtab;
    CHECK(table.is_attached());
    CHECK(subtab->is_attached());
}

TEST(Table_HasSharedSpec)
{
    MyTable2 table1;
    CHECK(!table1.has_shared_spec());
    Group g;
    MyTable2::Ref table2 = g.get_table<MyTable2>("foo");
    CHECK(!table2->has_shared_spec());
    table2->add();
    CHECK(table2[0].subtab->has_shared_spec());

    // Subtable in mixed column
    TestTableMX::Ref table3 = g.get_table<TestTableMX>("bar");
    CHECK(!table3->has_shared_spec());
    table3->add();
    table3[0].first.set_subtable<MyTable2>();
    MyTable2::Ref table4 = table3[0].first.get_subtable<MyTable2>();
    CHECK(table4);
    CHECK(!table4->has_shared_spec());
    table4->add();
    CHECK(!table4->has_shared_spec());
    CHECK(table4[0].subtab->has_shared_spec());
}


namespace {
TIGHTDB_TABLE_3(TableAgg,
                c_int,   Int,
                c_float, Float,
                c_double, Double)

                // TODO: Bool? DateTime
} // anonymous namespace

#if TEST_DURATION > 0
#define TBL_SIZE TIGHTDB_MAX_LIST_SIZE*10
#else
#define TBL_SIZE 10
#endif

TEST(Table_Aggregates)
{
    TableAgg table;
    int64_t i_sum = 0;
    double f_sum = 0;
    double d_sum = 0;

    for (int i = 0; i < TBL_SIZE; i++) {
        table.add(5987654, 4.0f, 3.0);
        i_sum += 5987654;
        f_sum += double(4.0f);
        d_sum += 3.0;
    }
    table.add(1, 1.1f, 1.2);
    table.add(987654321, 11.0f, 12.0);
    table.add(5, 4.0f, 3.0);
    i_sum += 1 + 987654321 + 5;
    f_sum += double(1.1f) + double(11.0f) + double(4.0f);
    d_sum += 1.2 + 12.0 + 3.0;
    double size = TBL_SIZE + 3;

    // minimum
    CHECK_EQUAL(1, table.column().c_int.minimum());
    CHECK_EQUAL(1.1f, table.column().c_float.minimum());
    CHECK_EQUAL(1.2, table.column().c_double.minimum());
    // maximum
    CHECK_EQUAL(987654321, table.column().c_int.maximum());
    CHECK_EQUAL(11.0f, table.column().c_float.maximum());
    CHECK_EQUAL(12.0, table.column().c_double.maximum());
    // sum
    CHECK_EQUAL(i_sum, table.column().c_int.sum());
    CHECK_EQUAL(f_sum, table.column().c_float.sum());
    CHECK_EQUAL(d_sum, table.column().c_double.sum());
    // average
    CHECK_EQUAL(double(i_sum)/size, table.column().c_int.average());
    CHECK_EQUAL(double(f_sum)/size, table.column().c_float.average());
    // almost_equal because of double/float imprecision
    CHECK(almost_equal(double(d_sum)/size, table.column().c_double.average()));
}

namespace {
TIGHTDB_TABLE_1(TableAgg2,
                c_count, Int)
} // anonymous namespace


TEST(Table_Aggregates2)
{
    TableAgg2 table;
    int c = -420;
    int s = 0;
    while (c < -20) {
        table.add(c);
        s += c;
        c++;
    }

    CHECK_EQUAL(-420, table.column().c_count.minimum());
    CHECK_EQUAL(-21, table.column().c_count.maximum());
    CHECK_EQUAL(s, table.column().c_count.sum());
}

TEST(Table_LanguageBindings)
{
   Table* table = LangBindHelper::new_table();
   CHECK(table->is_attached());

   table->add_column(type_Int, "i");
   table->insert_int(0, 0, 10);
   table->insert_done();
   table->insert_int(0, 1, 12);
   table->insert_done();

   Table* table2 = LangBindHelper::copy_table(*table);
   CHECK(table2->is_attached());

   CHECK(*table == *table2);

   LangBindHelper::unbind_table_ref(table);
   LangBindHelper::unbind_table_ref(table2);
}

TEST(Table_MultipleColumn)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "first");
    CHECK_EQUAL(table.get_column_count(), 2);
    CHECK_EQUAL(table.get_column_index("first"), 0);
}


TEST(Table_FormerLeakCase)
{
    Table sub;
    sub.add_column(type_Int, "a");

    Table root;
    root.add_column(type_Table, "b");
    root.add_subcolumn(tuple(0), type_Int, "a");
    root.add_empty_row(1);
    root.set_subtable(0, 0, &sub);
    root.set_subtable(0, 0, 0);
}

TIGHTDB_TABLE_3(TablePivotAgg,
                sex,   String,
                age,   Int,
                hired, Bool)

TEST(Table_pivot)
{
    TablePivotAgg table;
    for (size_t i = 0; i < 500000; ++i) {
        StringData sex = i % 2 ? "Male" : "Female";
        table.add(sex, 20 + (i%20), true);
    }

    UnitTest::Timer timer;
    stringstream ss;

    Table result_count;
    timer.Start();
    table.aggregate(0, 1, Table::aggr_count, result_count);
    printf("pivot_count1: %d\n", timer.GetTimeInMs());

    result_count.to_string(ss);
    cout << ss.str();

    Table result_sum;
    table.aggregate(0, 1, Table::aggr_sum, result_sum);

    ss.str("");
    result_sum.to_string(ss);
    cout << ss.str();

    Table result_avg;
    table.aggregate(0, 1, Table::aggr_avg, result_avg);

    ss.str("");
    result_avg.to_string(ss);
    cout << ss.str();

    // Test with enumerated strings
    table.optimize();

    Table result_count2;
    timer.Start();
    table.aggregate(0, 1, Table::aggr_count, result_count2);
    printf("pivot_count2: %d\n", timer.GetTimeInMs());
    ss.str("");
    result_count2.to_string(ss);
    cout << ss.str();
}

#endif // TEST_TABLE
