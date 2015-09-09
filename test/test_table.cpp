#include "testsettings.hpp"
#ifdef TEST_TABLE

#include <algorithm>
#include <limits>
#include <string>
#include <fstream>
#include <ostream>

#include <realm.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/util/buffer.hpp>

#include "util/misc.hpp"

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestResults;


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

REALM_TABLE_2(TupleTableType,
                first,  Int,
                second, String)

} // anonymous namespace


#ifdef JAVA_MANY_COLUMNS_CRASH

REALM_TABLE_3(SubtableType,
                year,  Int,
                daysSinceLastVisit, Int,
                conceptId, String)

REALM_TABLE_7(MainTableType,
                patientId, String,
                gender, Int,
                ethnicity, Int,
                yearOfBirth, Int,
                yearOfDeath, Int,
                zipCode, String,
                events, Subtable<SubtableType>)

TEST(Table_ManyColumnsCrash2)
{
    // Trying to reproduce Java crash.
    for (int a = 0; a < 10; a++)
    {
        Group group;

        MainTableType::Ref mainTable = group.add_table<MainTableType>("PatientTable");
        TableRef dynPatientTable = group.add_table("PatientTable");
        dynPatientTable->add_empty_row();

        for (int counter = 0; counter < 20000; counter++)
        {
#if 0
            // Add row to subtable through typed interface
            SubtableType::Ref subtable = mainTable[0].events->get_table_ref();
            REALM_ASSERT(subtable->is_attached());
            subtable->add(0, 0, "");
            REALM_ASSERT(subtable->is_attached());

#else
            // Add row to subtable through dynamic interface. This mimics Java closest
            TableRef subtable2 = dynPatientTable->get_subtable(6, 0);
            REALM_ASSERT(subtable2->is_attached());
            size_t subrow = subtable2->add_empty_row();
            REALM_ASSERT(subtable2->is_attached());

#endif
            if((counter % 1000) == 0){
           //     std::cerr << counter << "\n";
            }
        }
    }
}

#endif // JAVA_MANY_COLUMNS_CRASH

TEST(Table_Null)
{
    {
        // Check that add_empty_row() adds NULL string as default
        Group group;
        TableRef table = group.add_table("test");

        table->add_column(type_String, "name", true); // nullable = true
        table->add_empty_row();

        CHECK(table->get_string(0, 0).is_null());
    }

    {
        // Check that add_empty_row() adds empty string as default
        Group group;
        TableRef table = group.add_table("test");

        table->add_column(type_String, "name");
        table->add_empty_row();

        CHECK(!table->get_string(0, 0).is_null());

        // Test that inserting null in non-nullable column will throw
        CHECK_LOGIC_ERROR(table->set_string(0, 0, realm::null()), LogicError::column_not_nullable);
    }

    {
        // Check that add_empty_row() adds null integer as default
        Group group;
        TableRef table = group.add_table("table");
        table->add_column(type_Int, "name", true /*nullable*/);
        table->add_empty_row();
        CHECK(table->is_null(0, 0));
    }

    {
        // Check that add_empty_row() adds 0 integer as default.
        Group group;
        TableRef table = group.add_table("test");
        table->add_column(type_Int, "name");
        table->add_empty_row();
        CHECK(!table->is_null(0, 0));
        CHECK_EQUAL(0, table->get_int(0, 0));

        // Check that inserting null in non-nullable column will throw
        CHECK_LOGIC_ERROR(table->set_null(0, 0), LogicError::column_not_nullable);
    }

    {
        // Check that add_empty_row() adds NULL binary as default
        Group group;
        TableRef table = group.add_table("test");

        table->add_column(type_Binary, "name", true /*nullable*/);
        table->add_empty_row();

        CHECK(table->get_binary(0, 0).is_null());
    }

    {
        // Check that add_empty_row() adds empty binary as default
        Group group;
        TableRef table = group.add_table("test");

        table->add_column(type_Binary, "name");
        table->add_empty_row();

        CHECK(!table->get_binary(0, 0).is_null());

        // Test that inserting null in non-nullable column will throw
        CHECK_THROW_ANY(table->set_binary(0, 0, BinaryData()));
    }

}

TEST(Table_DeleteCrash)
{
    Group group;
    TableRef table = group.add_table("test");

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


TEST(Table_OptimizeCrash)
{
    // This will crash at the .add() method
    TupleTableType ttt;
    ttt.optimize();
    ttt.column().second.add_search_index();
    ttt.clear();
    ttt.add(1, "AA");
}


TEST(Table_1)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_ColumnNameTooLong)
{
    Group group;
    TableRef table = group.add_table("foo");
    const size_t buf_size = 64;
    std::unique_ptr<char[]> buf(new char[buf_size]);
    CHECK_LOGIC_ERROR(table->add_column(type_Int, StringData(buf.get(), buf_size)),
                      LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->insert_column(0, type_Int, StringData(buf.get(), buf_size)),
                      LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->add_column_link(type_Link,
                                             StringData(buf.get(), buf_size),
                                             *table),
                      LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->insert_column_link(0, type_Link,
                                                StringData(buf.get(), buf_size),
                                                *table),
                      LogicError::column_name_too_long);

    table->add_column(type_Int, StringData(buf.get(), buf_size - 1));
    table->insert_column(0, type_Int, StringData(buf.get(), buf_size - 1));
    table->add_column_link(type_Link, StringData(buf.get(), buf_size - 1), *table);
    table->insert_column_link(0, type_Link, StringData(buf.get(), buf_size - 1), *table);
}


TEST(Table_StringOrBinaryTooBig)
{
    Table table;
    table.add_column(type_String, "s");
    table.add_column(type_Binary, "b");
    table.add_column(type_Mixed,  "m1");
    table.add_column(type_Mixed,  "m2");
    table.add_empty_row();

    table.set_string(0, 0, "01234567");

    size_t large_bin_size = 0xFFFFF1;
    size_t large_str_size = 0xFFFFF0; // null-terminate reduces max size by 1
    std::unique_ptr<char[]> large_buf(new char[large_bin_size]);
    CHECK_LOGIC_ERROR(table.set_string(0, 0, StringData(large_buf.get(), large_str_size)),
                      LogicError::string_too_big);
    CHECK_LOGIC_ERROR(table.set_binary(1, 0, BinaryData(large_buf.get(), large_bin_size)),
                      LogicError::binary_too_big);
    CHECK_LOGIC_ERROR(table.set_mixed(2, 0, Mixed(StringData(large_buf.get(), large_str_size))),
                      LogicError::string_too_big);
    CHECK_LOGIC_ERROR(table.set_mixed(3, 0, Mixed(BinaryData(large_buf.get(), large_bin_size))),
                      LogicError::binary_too_big);
    table.set_string(0, 0, StringData(large_buf.get(), large_str_size - 1));
    table.set_binary(1, 0, BinaryData(large_buf.get(), large_bin_size - 1));
    table.set_mixed(2, 0, Mixed(StringData(large_buf.get(), large_str_size - 1)));
    table.set_mixed(3, 0, Mixed(BinaryData(large_buf.get(), large_bin_size - 1)));
}


TEST(Table_Floats)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

REALM_TABLE_4(TestTable,
                first,  Int,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

} // anonymous namespace

TEST(Table_2)
{
    TestTable table;

    table.add(0, 10, true, Wed);
    const TestTable::Cursor r = table.back(); // last item

    CHECK_EQUAL(0, r.first);
    CHECK_EQUAL(10, r.second);
    CHECK_EQUAL(true, r.third);
    CHECK_EQUAL(Wed, r.fourth);

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_3)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

REALM_TABLE_2(TestTableEnum,
                first,      Enum<Days>,
                second,     String)

} // anonymous namespace

TEST(Table_4)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

REALM_TABLE_2(TestTableFloats,
                first,      Float,
                second,     Double)

} // anonymous namespace

TEST(Table_Float2)
{
    TestTableFloats table;

    table.add(1.1f, 2.2);
    table.add(1.1f, 2.2);
    const TestTableFloats::Cursor r = table.back(); // last item

    CHECK_EQUAL(1.1f, r.first);
    CHECK_EQUAL(2.2, r.second);

#ifdef REALM_DEBUG
    table.verify();
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

#ifdef REALM_DEBUG
    table.verify();
#endif

    // Delete all items one at a time
    for (size_t i = 0; i < 7; ++i) {
        table.remove(0);
    }

    CHECK(table.is_empty());
    CHECK_EQUAL(0, table.size());

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_GetName)
{
    // Freestanding tables have no names
    {
        Table table;
        CHECK_EQUAL("", table.get_name());
    }
    // ... regardless of how they are created
    {
        TableRef table = Table::create();
        CHECK_EQUAL("", table->get_name());
    }

    // Direct members of groups do have names
    {
        Group group;
        TableRef table = group.add_table("table");
        CHECK_EQUAL("table", table->get_name());
    }
    {
        Group group;
        TableRef foo = group.add_table("foo");
        TableRef bar = group.add_table("bar");
        CHECK_EQUAL("foo", foo->get_name());
        CHECK_EQUAL("bar", bar->get_name());
    }

    // Subtables should never have names
    {
        Table table;
        DescriptorRef subdesc;
        table.add_column(type_Table, "sub", &subdesc);
        table.add_empty_row();
        TableRef subtab = table.get_subtable(0,0);
        CHECK_EQUAL("", table.get_name());
        CHECK_EQUAL("", subtab->get_name());
    }
    // ... not even when the parent is a member of a group
    {
        Group group;
        TableRef table = group.add_table("table");
        DescriptorRef subdesc;
        table->add_column(type_Table, "sub", &subdesc);
        table->add_empty_row();
        TableRef subtab = table->get_subtable(0,0);
        CHECK_EQUAL("table", table->get_name());
        CHECK_EQUAL("", subtab->get_name());
    }
}


namespace {

void setup_multi_table(Table& table, size_t rows, size_t sub_rows,
                       bool fixed_subtab_sizes = false)
{
    // Create table with all column types
    {
        DescriptorRef sub1;
        table.add_column(type_Int,      "int");              //  0
        table.add_column(type_Bool,     "bool");             //  1
        table.add_column(type_DateTime, "date");             //  2
        table.add_column(type_Float,    "float");            //  3
        table.add_column(type_Double,   "double");           //  4
        table.add_column(type_String,   "string");           //  5
        table.add_column(type_String,   "string_long");      //  6
        table.add_column(type_String,   "string_big_blobs"); //  7
        table.add_column(type_String,   "string_enum");      //  8 - becomes StringEnumColumn
        table.add_column(type_Binary,   "binary");           //  9
        table.add_column(type_Table,    "tables", &sub1);    // 10
        table.add_column(type_Mixed,    "mixed");            // 11
        table.add_column(type_Int,      "int_null", true);   // 12, nullable = true
        sub1->add_column(type_Int,        "sub_first");
        sub1->add_column(type_String,     "sub_second");
    }

    table.add_empty_row(rows);

    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        table.set_int(0, i, int64_t(i*sign));

        if (i % 4 == 0) {
            table.set_null(12, i);
        }
        else {
            table.set_int(12, i, int64_t(i*sign));
        }
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_bool(1, i, (i % 2 ? true : false));
    for (size_t i = 0; i < rows; ++i)
        table.set_datetime(2, i, 12345);
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        table.set_float(3, i, 123.456f*sign);
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        table.set_double(4, i, 9876.54321*sign);
    }
    std::vector<std::string> strings;
    for (size_t i = 0; i < rows; ++i) {
        std::stringstream out;
        out << "string" << i;
        strings.push_back(out.str());
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_string(5, i, strings[i]);
    for (size_t i = 0; i < rows; ++i)
        table.set_string(6, i, strings[i] + " very long string.........");
    for (size_t i = 0; i < rows; ++i) {
        switch (i % 2) {
            case 0: {
                std::string s = strings[i];
                s += " very long string.........";
                for (int j = 0; j != 4; ++j)
                    s += " big blobs big blobs big blobs"; // +30
                table.set_string(7, i, s);
                break;
            }
            case 1:
                table.set_string(7, i, "");
                break;
        }
    }
    for (size_t i = 0; i < rows; ++i) {
        switch (i % 3) {
            case 0:
                table.set_string(8, i, "enum1");
                break;
            case 1:
                table.set_string(8, i, "enum2");
                break;
            case 2:
                table.set_string(8, i, "enum3");
                break;
        }
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_binary(9, i, BinaryData("binary", 7));
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        size_t n = sub_rows;
        if (!fixed_subtab_sizes)
            n += i;
        for (size_t j = 0; j != n; ++j) {
            TableRef subtable = table.get_subtable(10, i);
            int64_t val = -123+i*j*1234*sign;
            subtable->insert_empty_row(j);
            subtable->set_int(0, j, val);
            subtable->set_string(1, j, "sub");
        }
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i%2 == 0) ? 1 : -1;
        switch (i % 8) {
            case 0:
                table.set_mixed(11, i, false);
                break;
            case 1:
                table.set_mixed(11, i, int64_t(i*i*sign));
                break;
            case 2:
                table.set_mixed(11, i, "string");
                break;
            case 3:
                table.set_mixed(11, i, DateTime(123456789));
                break;
            case 4:
                table.set_mixed(11, i, BinaryData("binary", 7));
                break;
            case 5: {
                // Add subtable to mixed column
                // We can first set schema and contents when the entire
                // row has been inserted
                table.set_mixed(11, i, Mixed::subtable_tag());
                TableRef subtable = table.get_subtable(11, i);
                subtable->add_column(type_Int,    "first");
                subtable->add_column(type_String, "second");
                for (size_t j = 0; j != 2; ++j) {
                    subtable->insert_empty_row(j);
                    subtable->set_int(0, j, i*i*j*sign);
                    subtable->set_string(1, j, "mixed sub");
                }
                break;
            }
            case 6:
                table.set_mixed(11, i, float(123.1*i*sign));
                break;
            case 7:
                table.set_mixed(11, i, double(987.65*i*sign));
                break;
        }
    }

    // We also want a StringEnumColumn
    table.optimize();
}

} // anonymous namespace


TEST(Table_LowLevelCopy)
{
    Table table;
    setup_multi_table(table, 15, 2);

#ifdef REALM_DEBUG
    table.verify();
#endif

    Table table2 = table;

#ifdef REALM_DEBUG
    table2.verify();
#endif

    CHECK(table2 == table);

    TableRef table3 = table.copy();

#ifdef REALM_DEBUG
    table3->verify();
#endif

    CHECK(*table3 == table);
}


TEST(Table_HighLevelCopy)
{
    TestTable table;
    table.add(10, 120, false, Mon);
    table.add(12, 100, true,  Tue);

#ifdef REALM_DEBUG
    table.verify();
#endif

    TestTable table2 = table;

#ifdef REALM_DEBUG
    table2.verify();
#endif

    CHECK(table2 == table);

    TestTable::Ref table3 = table.copy();

#ifdef REALM_DEBUG
    table3->verify();
#endif

    CHECK(*table3 == table);
}


TEST(Table_DeleteAllTypes)
{
    Table table;
    setup_multi_table(table, 15, 2);

    // Test Deletes
    table.remove(14);
    table.remove(0);
    table.remove(5);

    CHECK_EQUAL(12, table.size());

#ifdef REALM_DEBUG
    table.verify();
#endif

    // Test Clear
    table.clear();
    CHECK_EQUAL(0, table.size());

#ifdef REALM_DEBUG
    table.verify();
#endif
}


// Triggers a bug that would make Realm crash if you run optimize() followed by add_search_index()
TEST(Table_Optimize_SetIndex_Crash)
{
    Table table;
    table.add_column(type_String, "first");
    table.add_empty_row(3);
    table.set_string(0, 0, "string0");
    table.set_string(0, 1, "string1");
    table.set_string(0, 2, "string1");

    table.optimize();
    CHECK_NOT_EQUAL(0, table.get_descriptor()->get_num_unique_values(0));

    table.set_string(0, 2, "string2");

    table.add_search_index(0);

    table.move_last_over(1);
    table.move_last_over(1);
}


TEST(Table_MoveAllTypes)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    Table table;
    setup_multi_table(table, 15, 2);
    table.add_search_index(6);

    while (!table.is_empty()) {
        size_t size = table.size();
        size_t target_row_ndx = random.draw_int_mod(size);
        table.move_last_over(target_row_ndx);
        table.verify();
    }
}


TEST(Table_DegenerateSubtableSearchAndAggregate)
{
    Table parent;

    // Add all column types
    {
        DescriptorRef sub_1, sub_2;
        parent.add_column(type_Table,  "child", &sub_1);
        sub_1->add_column(type_Int,      "int");           // 0
        sub_1->add_column(type_Bool,     "bool");          // 1
        sub_1->add_column(type_Float,    "float");         // 2
        sub_1->add_column(type_Double,   "double");        // 3
        sub_1->add_column(type_DateTime, "date");          // 4
        sub_1->add_column(type_String,   "string");        // 5
        sub_1->add_column(type_Binary,   "binary");        // 6
        sub_1->add_column(type_Table,    "table", &sub_2); // 7
        sub_1->add_column(type_Mixed,    "mixed");         // 8
        sub_1->add_column(type_Int,      "int_null", nullptr, true); // 9, nullable = true
        sub_2->add_column(type_Int,        "i");
    }

    parent.add_empty_row(); // Create a degenerate subtable

    ConstTableRef degen_child = parent.get_subtable(0,0); // NOTE: Constness is essential here!!!

    CHECK_EQUAL(0, degen_child->size());
    CHECK_EQUAL(10, degen_child->get_column_count());

    // Searching:

    CHECK_LOGIC_ERROR(degen_child->find_pkey_string(""), LogicError::no_primary_key);
//    CHECK_EQUAL(0, degen_child->distinct(0).size()); // needs index but you cannot set index on ConstTableRef
    CHECK_EQUAL(0, degen_child->get_sorted_view(0).size());

    CHECK_EQUAL(not_found, degen_child->find_first_int(0, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_bool(1, false));
    CHECK_EQUAL(not_found, degen_child->find_first_float(2, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_double(3, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_datetime(4, DateTime()));
    CHECK_EQUAL(not_found, degen_child->find_first_string(5, StringData("")));
//    CHECK_EQUAL(not_found, degen_child->find_first_binary(6, BinaryData())); // Exists but not yet implemented
//    CHECK_EQUAL(not_found, degen_child->find_first_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(not_found, degen_child->find_first_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->find_all_int(0, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_bool(1, false).size());
    CHECK_EQUAL(0, degen_child->find_all_float(2, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_double(3, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_datetime(4, DateTime()).size());
    CHECK_EQUAL(0, degen_child->find_all_string(5, StringData("")).size());
//    CHECK_EQUAL(0, degen_child->find_all_binary(6, BinaryData()).size()); // Exists but not yet implemented
//    CHECK_EQUAL(0, degen_child->find_all_subtable(7, subtab).size()); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->find_all_mixed(8, Mixed()).size()); // Not yet implemented

    CHECK_EQUAL(0, degen_child->lower_bound_int(0, 0));
    CHECK_EQUAL(0, degen_child->lower_bound_bool(1, false));
    CHECK_EQUAL(0, degen_child->lower_bound_float(2, 0));
    CHECK_EQUAL(0, degen_child->lower_bound_double(3, 0));
//    CHECK_EQUAL(0, degen_child->lower_bound_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->lower_bound_string(5, StringData("")));
//    CHECK_EQUAL(0, degen_child->lower_bound_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->lower_bound_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->lower_bound_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->upper_bound_int(0, 0));
    CHECK_EQUAL(0, degen_child->upper_bound_bool(1, false));
    CHECK_EQUAL(0, degen_child->upper_bound_float(2, 0));
    CHECK_EQUAL(0, degen_child->upper_bound_double(3, 0));
//    CHECK_EQUAL(0, degen_child->upper_bound_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->upper_bound_string(5, StringData("")));
//    CHECK_EQUAL(0, degen_child->upper_bound_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->upper_bound_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->upper_bound_mixed(8, Mixed())); // Not yet implemented


    // Aggregates:

    CHECK_EQUAL(0, degen_child->count_int(0, 0));
//    CHECK_EQUAL(0, degen_child->count_bool(1, false)); // Not yet implemented
    CHECK_EQUAL(0, degen_child->count_float(2, 0));
    CHECK_EQUAL(0, degen_child->count_double(3, 0));
//    CHECK_EQUAL(0, degen_child->count_date(4, Date())); // Not yet implemented
    CHECK_EQUAL(0, degen_child->count_string(5, StringData("")));
//    CHECK_EQUAL(0, degen_child->count_binary(6, BinaryData())); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->count_subtable(7, subtab)); // Not yet implemented
//    CHECK_EQUAL(0, degen_child->count_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->minimum_int(0));
    CHECK_EQUAL(0, degen_child->minimum_float(2));
    CHECK_EQUAL(0, degen_child->minimum_double(3));
    CHECK_EQUAL(0, degen_child->minimum_datetime(4));

    CHECK_EQUAL(0, degen_child->maximum_int(0));
    CHECK_EQUAL(0, degen_child->maximum_float(2));
    CHECK_EQUAL(0, degen_child->maximum_double(3));
    CHECK_EQUAL(0, degen_child->maximum_datetime(4));

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
    CHECK_EQUAL(not_found, degen_child->where().equal(5, StringData("")).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(6, BinaryData()).find());
//    CHECK_EQUAL(not_found, degen_child->where().equal(7, subtab).find()); // Not yet implemented
//    CHECK_EQUAL(not_found, degen_child->where().equal(8, Mixed()).find()); // Not yet implemented

    CHECK_EQUAL(not_found, degen_child->where().not_equal(0, int64_t()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(2, float()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(3, double()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal_datetime(4, DateTime()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(5, StringData("")).find());
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

TEST(Table_Range)
{
    Table table;
    table.add_column(type_Int, "int");
    table.add_empty_row(100);
    for (size_t i = 0 ; i < 100; ++i)
        table.set_int(0, i, i);
    TableView tv = table.get_range_view(10, 20);
    CHECK_EQUAL(10, tv.size());
    for (size_t i = 0; i < tv.size(); ++i)
        CHECK_EQUAL(int64_t(i+10), tv.get_int(0, i));
}

TEST(Table_RangeConst)
{
    Group group;
    {
        TableRef table = group.add_table("test");
        table->add_column(type_Int, "int");
        table->add_empty_row(100);
        for (int i = 0 ; i < 100; ++i)
            table->set_int(0, i, i);
    }
    ConstTableRef ctable = group.get_table("test");
    ConstTableView tv = ctable->get_range_view(10, 20);
    CHECK_EQUAL(10, tv.size());
    for (size_t i = 0; i<tv.size(); ++i)
        CHECK_EQUAL(int64_t(i+10), tv.get_int(0, i));
}


// enable to generate testfiles for to_string below
#define GENERATE 0

TEST(Table_ToString)
{
    Table table;
    setup_multi_table(table, 15, 6);

    std::stringstream ss;
    table.to_string(ss);
    const std::string result = ss.str();
    std::string file_name = get_test_resource_path();
    file_name += "expect_string.txt";
#if GENERATE   // enable to generate testfile - check it manually
    std::ofstream test_file(file_name.c_str(), std::ios::out);
    test_file << result;
    std::cerr << "to_string() test:\n" << result << std::endl;
#else
    std::ifstream test_file(file_name.c_str(), std::ios::in);
    CHECK(!test_file.fail());
    std::string expected;
    expected.assign( std::istreambuf_iterator<char>(test_file),
                     std::istreambuf_iterator<char>() );
    bool test_ok = test_util::equal_without_cr(result, expected);
    CHECK_EQUAL(true, test_ok);
    if (!test_ok) {
        TEST_PATH(path);
        File out(path, File::mode_Write);
        out.write(result);
        std::cerr << "\n error result in '" << std::string(path) << "'\n";
    }
#endif
}

/* DISABLED BECAUSE IT FAILS - A PULL REQUEST WILL BE MADE WHERE IT IS REENABLED!
TEST(Table_RowToString)
{
    // Create table with all column types
    Table table;
    setup_multi_table(table, 2, 2);

    std::stringstream ss;
    table.row_to_string(1, ss);
    const std::string row_str = ss.str();
#if 0
    std::ofstream test_file("row_to_string.txt", ios::out);
    test_file << row_str;
#endif

    std::string expected = "    int   bool                 date           float          double   string              string_long  string_enum     binary  mixed  tables\n"
                      "1:   -1   true  1970-01-01 03:25:45  -1.234560e+002  -9.876543e+003  string1  string1 very long st...  enum2          7 bytes     -1     [3]\n";
    bool test_ok = test_util::equal_without_cr(row_str, expected);
    CHECK_EQUAL(true, test_ok);
    if (!test_ok) {
        std::cerr << "row_to_string() failed\n"
             << "Expected: " << expected << "\n"
             << "Got     : " << row_str << std::endl;
    }
}


TEST(Table_FindInt)
{
    TestTable table;

    for (int i = 1000; i >= 0; --i) {
        table.add(0, i, true, Wed);
    }

    CHECK_EQUAL(size_t(0),    table.column().second.find_first(1000));
    CHECK_EQUAL(size_t(1000), table.column().second.find_first(0));
    CHECK_EQUAL(size_t(-1),   table.column().second.find_first(1001));

#ifdef REALM_DEBUG
    table.verify();
#endif
}
*/


/*
TEST(Table_6)
{
    TestTableEnum table;

    RLM_QUERY(TestQuery, TestTableEnum) {
    //  first.between(Mon, Thu);
        second == "Hello" || (second == "Hey" && first == Mon);
    }};

    RLM_QUERY_OPT(TestQuery2, TestTableEnum) (Days a, Days b, const char* str) {
        (void)b;
        (void)a;
        //first.between(a, b);
        second == str || second.MatchRegEx(".*");
    }};

    //TestTableEnum result = table.find_all(TestQuery2(Mon, Tue, "Hello")).sort().Limit(10);
    //size_t result2 = table.Range(10, 200).find_first(TestQuery());
    //CHECK_EQUAL((size_t)-1, result2);

#ifdef REALM_DEBUG
    table.verify();
#endif
}
*/


TEST(Table_FindAllInt)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_SortedInt)
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_Sorted_Query_where)
{
    // Using where(tv) instead of tableview(tv)
    TestTable table;

    table.add(0, 10, true, Wed); // 0: 4
    table.add(0, 20, false, Wed); // 1: 7
    table.add(0, 0, false, Wed); // 2: 0
    table.add(0, 40, false, Wed); // 3: 8
    table.add(0, 15, false, Wed); // 4: 6
    table.add(0, 11, true, Wed); // 5: 5
    table.add(0, 6, true, Wed); // 6: 3
    table.add(0, 4, true, Wed); // 7: 2
    table.add(0, 99, true, Wed); // 8: 9
    table.add(0, 2, true, Wed); // 9: 1

    // Count booleans
    size_t count_original = table.where().third.equal(false).count();
    CHECK_EQUAL(4, count_original);

    // Get a view containing the complete table
    TestTable::View v = table.column().first.find_all(0);
    CHECK_EQUAL(table.size(), v.size());

    // Count booleans
    size_t count_view = table.where(&v).third.equal(false).count();
    CHECK_EQUAL(4, count_view);

    TestTable::View v_sorted = table.column().second.get_sorted_view();
    CHECK_EQUAL(table.size(), v_sorted.size());

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_Multi_Sort)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    table.add_empty_row(5);

    // 1, 10
    table.set_int(0, 0, 1);
    table.set_int(1, 0, 10);

    // 2, 10
    table.set_int(0, 1, 2);
    table.set_int(1, 1, 10);

    // 0, 10
    table.set_int(0, 2, 0);
    table.set_int(1, 2, 10);

    // 2, 14
    table.set_int(0, 3, 2);
    table.set_int(1, 3, 14);

    // 1, 14
    table.set_int(0, 4, 1);
    table.set_int(1, 4, 14);

    std::vector<size_t> col_ndx1;
    col_ndx1.push_back(0);
    col_ndx1.push_back(1);

    std::vector<bool> asc;
    asc.push_back(true);
    asc.push_back(true);

    // (0, 10); (1, 10); (1, 14); (2, 10); (2; 14)
    TableView v_sorted1 = table.get_sorted_view(col_ndx1, asc);
    CHECK_EQUAL(table.size(), v_sorted1.size());
    CHECK_EQUAL(2, v_sorted1.get_source_ndx(0));
    CHECK_EQUAL(0, v_sorted1.get_source_ndx(1));
    CHECK_EQUAL(4, v_sorted1.get_source_ndx(2));
    CHECK_EQUAL(1, v_sorted1.get_source_ndx(3));
    CHECK_EQUAL(3, v_sorted1.get_source_ndx(4));

    std::vector<size_t> col_ndx2;
    col_ndx2.push_back(1);
    col_ndx2.push_back(0);

    // (0, 10); (1, 10); (2, 10); (1, 14); (2, 14)
    TableView v_sorted2 = table.get_sorted_view(col_ndx2, asc);
    CHECK_EQUAL(table.size(), v_sorted2.size());
    CHECK_EQUAL(2, v_sorted2.get_source_ndx(0));
    CHECK_EQUAL(0, v_sorted2.get_source_ndx(1));
    CHECK_EQUAL(1, v_sorted2.get_source_ndx(2));
    CHECK_EQUAL(4, v_sorted2.get_source_ndx(3));
    CHECK_EQUAL(3, v_sorted2.get_source_ndx(4));
}


TEST(Table_IndexString)
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

    table.column().second.add_search_index();
    CHECK(table.column().second.has_search_index());

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


TEST(Table_IndexStringTwice)
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

    table.column().second.add_search_index();
    CHECK_EQUAL(true, table.column().second.has_search_index());
    table.column().second.add_search_index();
    CHECK_EQUAL(true, table.column().second.has_search_index());
}


// Tests Table part of index on Int, DateTime and Bool columns. For a more exhaustive 
// test of the integer index (bypassing Table), see test_index_string.cpp)
TEST(Table_IndexInteger)
{
    Table table;
    size_t r;

    table.add_column(type_Int, "ints");
    table.add_column(type_DateTime, "date");
    table.add_column(type_Bool, "date");

    table.add_empty_row(13);

    table.set_int(0, 0, 3); // 0
    table.set_int(0, 1, 1); // 1
    table.set_int(0, 2, 2); // 2
    table.set_int(0, 3, 2); // 3
    table.set_int(0, 4, 2); // 4
    table.set_int(0, 5, 3); // 5
    table.set_int(0, 6, 3); // 6
    table.set_int(0, 7, 2); // 7
    table.set_int(0, 8, 4); // 8
    table.set_int(0, 9, 2); // 9
    table.set_int(0, 10, 6); // 10
    table.set_int(0, 11, 2); // 11
    table.set_int(0, 12, 3); // 12

    table.add_search_index(0);
    CHECK(table.has_search_index(0));
    table.add_search_index(1);
    CHECK(table.has_search_index(1));
    table.add_search_index(2);
    CHECK(table.has_search_index(2));

    table.set_datetime(1, 10, DateTime(43));
    r = table.find_first_datetime(1, DateTime(43));
    CHECK_EQUAL(10, r);

    table.set_bool(2, 11, true);
    r = table.find_first_bool(2, true);
    CHECK_EQUAL(11, r);

    r = table.find_first_int(0, 11);
    CHECK_EQUAL(not_found, r);

    r = table.find_first_int(0, 3);
    CHECK_EQUAL(0, r);

    r = table.find_first_int(0, 4);
    CHECK_EQUAL(8, r);

    TableView tv = table.find_all_int(0, 2);
    CHECK_EQUAL(6, tv.size());

    CHECK_EQUAL(2, tv[0].get_index());
    CHECK_EQUAL(3, tv[1].get_index());
    CHECK_EQUAL(4, tv[2].get_index());
    CHECK_EQUAL(7, tv[3].get_index());
    CHECK_EQUAL(9, tv[4].get_index());
    CHECK_EQUAL(11, tv[5].get_index());
}


TEST(Table_PrimaryKeyBasics)
{
    // Note: Formally, member functions of Table are not required to leave the
    // table in a valid state when they throw LogicError. In the cases below,
    // however, informed by the actual implementation of these functions, we
    // assume that they do allow us to continue, but please remember that this
    // is not generally the case.

    Table table;
    table.add_column(type_String, "");

    // Empty table
    CHECK_NOT(table.has_primary_key());
    CHECK_LOGIC_ERROR(table.find_pkey_string("foo"), LogicError::no_primary_key);
    CHECK_LOGIC_ERROR(table.try_add_primary_key(0), LogicError::no_search_index);
    table.add_search_index(0);
    CHECK_NOT(table.has_primary_key());
    CHECK_LOGIC_ERROR(table.find_pkey_string("foo"), LogicError::no_primary_key);
    CHECK(table.try_add_primary_key(0));
    CHECK(table.has_primary_key());
    CHECK_NOT(table.find_pkey_string("foo"));

    // One row
    table.remove_primary_key();
    table.add_empty_row();
    table.set_string(0, 0, "foo");
    CHECK_LOGIC_ERROR(table.find_pkey_string("foo"), LogicError::no_primary_key);
    CHECK(table.try_add_primary_key(0));
    CHECK_EQUAL(0, table.find_pkey_string("foo").get_index());
    CHECK_NOT(table.find_pkey_string("bar"));

    // Two rows
    table.remove_primary_key();
    table.add_empty_row();
    table.set_string(0, 1, "bar");
    CHECK(table.try_add_primary_key(0));
    CHECK_EQUAL(0, table.find_pkey_string("foo").get_index());
    CHECK_EQUAL(1, table.find_pkey_string("bar").get_index());

    // Modify primary key
    CHECK_LOGIC_ERROR(table.set_string(0, 1, "foo"), LogicError::unique_constraint_violation);
    table.set_string(0, 1, "bar");
    table.set_string(0, 1, "baz");
    CHECK_EQUAL(0, table.find_pkey_string("foo").get_index());
    CHECK_NOT(table.find_pkey_string("bar"));
    CHECK_EQUAL(1, table.find_pkey_string("baz").get_index());

    // Insert row
    // Unfortunately, we could not have recovered and continued if we had let
    // Table::insert_string() throw.
//    CHECK_LOGIC_ERROR(table.insert_string(0, 2, "foo"), LogicError::unique_constraint_violation);
    table.verify();
    table.insert_empty_row(2);
    table.set_string(0, 2, "bar");
    table.verify();
    table.add_empty_row();
    table.verify();
    // Unfortunately, we could not have recovered and continued if we had let
    // Table::add_empty_row() throw.
//    CHECK_LOGIC_ERROR(table.add_empty_row(), LogicError::unique_constraint_violation);

    // Duplicate key value
    table.remove_primary_key();
    table.set_string(0, 1, "foo");
    CHECK_NOT(table.try_add_primary_key(0));
}


TEST(Table_PrimaryKeyLargeCommonPrefix)
{
    Table table;
    table.add_column(type_String, "");
    table.add_empty_row(2);
    table.set_string(0, 0, "metasyntactic variable 1");
    table.set_string(0, 1, "metasyntactic variable 2");
    table.add_search_index(0);
    CHECK(table.try_add_primary_key(0));
    CHECK_LOGIC_ERROR(table.set_string(0, 1, "metasyntactic variable 1"),
                      LogicError::unique_constraint_violation);
    table.set_string(0, 1, "metasyntactic variable 2");
    table.set_string(0, 1, "metasyntactic variable 3");
}


TEST(Table_PrimaryKeyExtra)
{
    Table table;
    table.add_column(type_String, "");
    table.add_column(type_Int, "");
    table.add_empty_row(8);

    table.set_string(0, 0, "jeff");
    table.set_string(0, 1, "jim");
    table.set_string(0, 2, "jennifer");
    table.set_string(0, 3, "john");
    table.set_string(0, 4, "jimmy");
    table.set_string(0, 5, "jimbo");
    table.set_string(0, 6, "johnny");
    table.set_string(0, 7, "jennifer"); // Duplicate primary key

    table.set_int(1, 0, 0);
    table.set_int(1, 1, 1);
    table.set_int(1, 2, 2);
    table.set_int(1, 3, 3);
    table.set_int(1, 4, 4);
    table.set_int(1, 5, 5);
    table.set_int(1, 6, 6);
    table.set_int(1, 7, 7);

    CHECK_LOGIC_ERROR(table.find_pkey_string("jeff"), LogicError::no_primary_key);

    CHECK_LOGIC_ERROR(table.try_add_primary_key(0), LogicError::no_search_index);
    CHECK_NOT(table.has_primary_key());

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    CHECK_NOT(table.try_add_primary_key(0));
    CHECK_NOT(table.has_primary_key());

    table.set_string(0, 7, "jennifer 8");
    CHECK(table.try_add_primary_key(0));
    CHECK(table.has_primary_key());

    table.verify();

    Row a0 = table.find_pkey_string("jeff");
    Row a1 = table.find_pkey_string("jim");
    Row a2 = table.find_pkey_string("jennifer");
    Row a3 = table.find_pkey_string("john");
    Row a4 = table.find_pkey_string("jimmy");
    Row a5 = table.find_pkey_string("jimbo");
    Row a6 = table.find_pkey_string("johnny");
    Row a7 = table.find_pkey_string("jerry");
    CHECK(a0);
    CHECK(a1);
    CHECK(a2);
    CHECK(a3);
    CHECK(a4);
    CHECK(a5);
    CHECK(a6);
    CHECK_NOT(a7);
    CHECK_EQUAL(0, a0.get_index());
    CHECK_EQUAL(1, a1.get_index());
    CHECK_EQUAL(2, a2.get_index());
    CHECK_EQUAL(3, a3.get_index());
    CHECK_EQUAL(4, a4.get_index());
    CHECK_EQUAL(5, a5.get_index());
    CHECK_EQUAL(6, a6.get_index());
}


TEST(Table_SubtablePrimaryKey)
{
    Table parent;
    parent.add_column(type_Table, "");
    parent.get_subdescriptor(0)->add_column(type_String, "");
    parent.add_empty_row();
    TableRef child = parent[0].get_subtable(0);
    CHECK_LOGIC_ERROR(child->find_pkey_string("foo"), LogicError::no_primary_key);
    CHECK_LOGIC_ERROR(child->add_search_index(0), LogicError::wrong_kind_of_table);
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

    table.column().second.add_search_index();
    CHECK(table.column().second.has_search_index());

    TestTableEnum::View view = table.column().second.get_distinct_view();

    CHECK_EQUAL(4, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(0));
    CHECK_EQUAL(1, view.get_source_ndx(1));
    CHECK_EQUAL(2, view.get_source_ndx(2));
    CHECK_EQUAL(5, view.get_source_ndx(3));
}


TEST(Table_DistinctEnums)
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

    table.column().first.add_search_index();
    CHECK(table.column().first.has_search_index());

    TestTableEnum::View view = table.column().first.get_distinct_view();

    CHECK_EQUAL(7, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(0));
    CHECK_EQUAL(1, view.get_source_ndx(1));
    CHECK_EQUAL(2, view.get_source_ndx(2));
    CHECK_EQUAL(3, view.get_source_ndx(3));
    CHECK_EQUAL(4, view.get_source_ndx(4));
    CHECK_EQUAL(5, view.get_source_ndx(5));
    CHECK_EQUAL(6, view.get_source_ndx(6));
}


TEST(Table_DistinctIntegers)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row(4);
    table.set_int(0, 0, 1);
    table.set_int(0, 1, 2);
    table.set_int(0, 2, 3);
    table.set_int(0, 3, 3);

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);

    CHECK_EQUAL(3, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(0));
    CHECK_EQUAL(1, view.get_source_ndx(1));
    CHECK_EQUAL(2, view.get_source_ndx(2));
}


TEST(Table_DistinctBool)
{
    Table table;
    table.add_column(type_Bool, "first");
    table.add_empty_row(4);
    table.set_bool(0, 0, true);
    table.set_bool(0, 1, false);
    table.set_bool(0, 2, true);
    table.set_bool(0, 3, false);

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);

    CHECK_EQUAL(2, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(1));
    CHECK_EQUAL(1, view.get_source_ndx(0));
}


/*
// FIXME Commented out because indexes on floats and doubles are not supported (yet).

TEST(Table_DistinctFloat)
{
    Table table;
    table.add_column(type_Float, "first");
    table.add_empty_row(12);
    for (size_t i = 0; i < 10; ++i) {
        table.set_float(0, i, static_cast<float>(i) + 0.5f);
    }
    table.set_float(0, 10, 0.5f);
    table.set_float(0, 11, 1.5f);

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);
    CHECK_EQUAL(10, view.size());
}


TEST(Table_DistinctDouble)
{
    Table table;
    table.add_column(type_Double, "first");
    table.add_empty_row(12);
    for (size_t i = 0; i < 10; ++i) {
        table.set_double(0, i, static_cast<double>(i) + 0.5);
    }
    table.set_double(0, 10, 0.5);
    table.set_double(0, 11, 1.5);

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);
    CHECK_EQUAL(10, view.size());
}
*/


TEST(Table_DistinctDateTime)
{
    Table table;
    table.add_column(type_DateTime, "first");
    table.add_empty_row(4);
    table.set_datetime(0, 0, DateTime(0));
    table.set_datetime(0, 1, DateTime(1));
    table.set_datetime(0, 2, DateTime(3));
    table.set_datetime(0, 3, DateTime(3));

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    TableView view = table.get_distinct_view(0);
    CHECK_EQUAL(3, view.size());
}


TEST(Table_DistinctFromPersistedTable)
{
    GROUP_TEST_PATH(path);

    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column(type_Int, "first");
        table->add_empty_row(4);
        table->set_int(0, 0, 1);
        table->set_int(0, 1, 2);
        table->set_int(0, 2, 3);
        table->set_int(0, 3, 3);

        table->add_search_index(0);
        CHECK(table->has_search_index(0));
        group.write(path);
    }

    {
        Group group(path, 0, Group::mode_ReadOnly);
        TableRef table = group.get_table("table");
        TableView view = table->get_distinct_view(0);

        CHECK_EQUAL(3, view.size());
        CHECK_EQUAL(0, view.get_source_ndx(0));
        CHECK_EQUAL(1, view.get_source_ndx(1));
        CHECK_EQUAL(2, view.get_source_ndx(2));
    }
}



TEST(Table_IndexInt)
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
    table.column().second.add_search_index();

    // Search for a value that does not exits
    const size_t r1 = table.column().second.find_first(2);
    CHECK_EQUAL(npos, r1);

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

#ifdef REALM_DEBUG
    table.verify();
#endif
}



namespace {

REALM_TABLE_4(TestTableAE,
                first,  Int,
                second, String,
                third,  Bool,
                fourth, Enum<Days>)

} // anonymous namespace

TEST(Table_AutoEnumeration)
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


TEST(Table_AutoEnumerationFindFindAll)
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

REALM_TABLE_4(TestTableEnum4,
                col1, String,
                col2, String,
                col3, String,
                col4, String)

} // anonymous namespace

TEST(Table_AutoEnumerationOptimize)
{
    TestTableEnum4 t;

    // Insert non-optimzable strings
    std::string s;
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

#ifdef REALM_DEBUG
    t.verify();
#endif
}

namespace {

REALM_TABLE_1(TestSubtabEnum2,
                str, String)
REALM_TABLE_1(TestSubtabEnum1,
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
        std::string s;
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
        std::string s;
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

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_Spec)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    {
        DescriptorRef sub_1;
        table->add_column(type_Int,    "first");
        table->add_column(type_String, "second");
        table->add_column(type_Table,  "third", &sub_1);
        sub_1->add_column(type_Int,      "sub_first");
        sub_1->add_column(type_String,   "sub_second");
    }

    CHECK_EQUAL(3, table->get_column_count());

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    CHECK_EQUAL(0, table->get_subtable_size(2, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 42);
        subtable->set_string(1, 0, "test");

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
    GROUP_TEST_PATH(path);
    group.write(path);

    // Read back tables
    {
        Group from_disk(path, 0, Group::mode_ReadOnly);
        TableRef from_disk_table = from_disk.get_table("test");

        TableRef subtable2 = from_disk_table->get_subtable(2, 0);

        CHECK_EQUAL(1,      subtable2->size());
        CHECK_EQUAL(42,     subtable2->get_int(0, 0));
        CHECK_EQUAL("test", subtable2->get_string(1, 0));
    }
}

TEST(Table_SpecColumnPath)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create path to sub-table column (starting with root)
    std::vector<size_t> column_path;

    // Create specification with sub-table
    table->add_subcolumn(column_path, type_Int,    "first");
    table->add_subcolumn(column_path, type_String, "second");
    table->add_subcolumn(column_path, type_Table,  "third");

    column_path.push_back(2); // third column (which is a sub-table col)

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 42);
        subtable->set_string(1, 0, "test");

        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }
}

TEST(Table_SpecRenameColumns)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 42);
        subtable->set_string(1, 0, "test");

        CHECK_EQUAL(42,     subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    // Rename first column
    table->rename_column(0, "1st");
    CHECK_EQUAL(0, table->get_column_index("1st"));

    // Rename sub-column
    table->rename_subcolumn(column_path, 0, "sub_1st"); // third

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK_EQUAL(0, subtable->get_column_index("sub_1st"));
    }
}

TEST(Table_SpecDeleteColumns)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");
    table->add_column(type_String, "fourth"); // will be auto-enumerated

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Put in an index as well
    table->add_search_index(1);

    CHECK_EQUAL(4, table->get_column_count());

    // Add a few rows
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");
    table->set_string(3, 0, "X");

    table->insert_empty_row(1);
    table->set_int(0, 1, 4);
    table->set_string(1, 1, "World");
    table->set_string(3, 1, "X");

    table->insert_empty_row(2);
    table->set_int(0, 2, 4);
    table->set_string(1, 2, "Goodbye");
    table->set_string(3, 2, "X");

    // We want the last column to be StringEnum column
    table->optimize();

    CHECK_EQUAL(0, table->get_subtable_size(2, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 42);
        subtable->set_string(1, 0, "test");

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

    // Remove a column in sub-table
    table->remove_subcolumn(column_path, 1);  // sub_second

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

#ifdef REALM_DEBUG
    table->verify();
#endif
}

TEST(Table_NullInEnum)
{
    Group group;
    TableRef table = group.add_table("test");
    table->add_column(type_String, "second", true);

    for (size_t c = 0; c < 100; c++) {
        table->insert_empty_row(c);
        table->set_string(0, c, "hello");
    }

    size_t r;

    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(100, r);

    table->set_string(0, 50, realm::null());
    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(99, r);

    table->optimize();

    table->set_string(0, 50, realm::null());
    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(99, r);

    table->set_string(0, 50, "hello");
    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(100, r);

    table->set_string(0, 50, realm::null());
    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(99, r);

    r = table->where().equal(0, realm::null()).count();
    CHECK_EQUAL(1, r);

    table->set_string(0, 55, realm::null());
    r = table->where().equal(0, realm::null()).count();
    CHECK_EQUAL(2, r);

    r = table->where().equal(0, "hello").count();
    CHECK_EQUAL(98, r);

    table->remove(55);
    r = table->where().equal(0, realm::null()).count();
    CHECK_EQUAL(1, r);
}

TEST(Table_SpecAddColumns)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Put in an index as well
    table->add_search_index(1);

    CHECK_EQUAL(3, table->get_column_count());

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    CHECK_EQUAL(0, table->get_subtable_size(2, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 42);
        subtable->set_string(1, 0, "test");

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
    stab->insert_empty_row(0);
    stab->set_int(0, 0, 1);
    stab->insert_empty_row(1);
    stab->set_int(0, 1, 2);
    CHECK_EQUAL(2, table->get_subtable_size(7, 0));

#ifdef REALM_DEBUG
    table->verify();
#endif
}


TEST(Table_SpecDeleteColumnsBug)
{
    TableRef table = Table::create();

    // Create specification with sub-table
    table->add_column(type_String, "name");
    table->add_search_index(0);
    table->add_column(type_Int,    "age");
    table->add_column(type_Bool,   "hired");
    table->add_column(type_Table,  "phones");

    // Create path to sub-table column
    std::vector<size_t> column_path;
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

#ifdef REALM_DEBUG
    table->verify();
#endif
}


TEST(Table_Mixed)
{
    Table table;
    table.add_column(type_Int,   "first");
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

    table.insert_empty_row(1);
    table.set_int(0, 1, 43);
    table.set_mixed(1, 1, (int64_t)12);

    CHECK_EQUAL(0,  table.get_int(0, ndx));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,  table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,   table.get_mixed(1, 1).get_int());

    table.insert_empty_row(2);
    table.set_int(0, 2, 100);
    table.set_mixed(1, 2, "test");

    CHECK_EQUAL(0,  table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool,   table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int,    table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(true,   table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12,     table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());

    table.insert_empty_row(3);
    table.set_int(0, 3, 0);
    table.set_mixed(1, 3, DateTime(324234));

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

    table.insert_empty_row(4);
    table.set_int(0, 4, 43);
    table.set_mixed(1, 4, Mixed(BinaryData("binary", 7)));

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

    table.insert_empty_row(5);
    table.set_int(0, 5, 0);
    table.set_mixed(1, 5, Mixed::subtable_tag());

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

    subtable->insert_empty_row(0);
    subtable->set_string(0, 0, "John");
    subtable->set_int(1, 0, 40);

    // Get same table again and verify values
    TableRef subtable2 = table.get_subtable(1, 5);
    CHECK_EQUAL(1, subtable2->size());
    CHECK_EQUAL("John", subtable2->get_string(0, 0));
    CHECK_EQUAL(40, subtable2->get_int(1, 0));

    // Insert float, double
    table.insert_empty_row(6);
    table.set_int(0, 6, 31);
    table.set_mixed(1, 6, float(1.123));
    table.insert_empty_row(7);
    table.set_int(0, 7, 0);
    table.set_mixed(1, 7, double(2.234));

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

#ifdef REALM_DEBUG
    table.verify();
#endif
}


namespace {
REALM_TABLE_1(TestTableMX,
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
    CHECK_EQUAL(1234,         table[2].first.get_datetime());
    CHECK_EQUAL("test",       table[3].first.get_string());
}


TEST(Table_SubtableSizeAndClear)
{
    Table table;
    DescriptorRef subdesc;
    table.add_column(type_Table, "subtab", &subdesc);
    table.add_column(type_Mixed, "mixed");
    subdesc->add_column(type_Int,  "int");

    table.insert_empty_row(0);
    table.insert_empty_row(1);
    Table subtable;
    table.set_mixed_subtable(1, 1, &subtable);

    CHECK_EQUAL(0, table.get_subtable_size(0,0)); // Subtable column
    CHECK_EQUAL(0, table.get_subtable_size(1,0)); // Mixed column, bool value
    CHECK_EQUAL(0, table.get_subtable_size(1,1)); // Mixed column, table value

    CHECK(table.get_subtable(0,0));  // Subtable column
    CHECK(!table.get_subtable(1,0)); // Mixed column, bool value, must return nullptr
    CHECK(table.get_subtable(1,1));  // Mixed column, table value

    table.set_mixed(1, 0, Mixed::subtable_tag());
    table.set_mixed(1, 1, false);
    CHECK(table.get_subtable(1,0));
    CHECK(!table.get_subtable(1,1));

    TableRef subtab1 = table.get_subtable(0,0);
    TableRef subtab2 = table.get_subtable(1,0);
    subtab2->add_column(type_Int, "int");

    CHECK_EQUAL(0, table.get_subtable_size(1,0));
    CHECK(table.get_subtable(1,0));

    subtab1->insert_empty_row(0);
    subtab2->insert_empty_row(0);

    CHECK_EQUAL(1, table.get_subtable_size(0,0));
    CHECK_EQUAL(1, table.get_subtable_size(1,0));

    table.clear_subtable(0,0);
    table.clear_subtable(1,0);

    CHECK_EQUAL(0, table.get_subtable_size(0,0));
    CHECK_EQUAL(0, table.get_subtable_size(1,0));

    CHECK(table.get_subtable(1,0));
}


TEST(Table_LowLevelSubtables)
{
    Table table;
    std::vector<size_t> column_path;
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
        std::vector<size_t> subcol_path;
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
REALM_TABLE_2(MyTable1,
                val, Int,
                val2, Int)

REALM_TABLE_2(MyTable2,
                val, Int,
                subtab, Subtable<MyTable1>)

REALM_TABLE_1(MyTable3,
                subtab, Subtable<MyTable2>)

REALM_TABLE_1(MyTable4,
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
REALM_TABLE_2(TableDateAndBinary,
                date, DateTime,
                bin, Binary)
} // anonymous namespace

TEST(Table_DateAndBinary)
{
    {
        TableDateAndBinary t;

        const size_t size = 10;
        char data[size];
        for (size_t i=0; i<size; ++i) data[i] = (char)i;
        t.add(8, BinaryData(data, size));
        CHECK_EQUAL(t[0].date, 8);
        CHECK_EQUAL(t[0].bin.size(), size);
        CHECK(std::equal(t[0].bin.data(), t[0].bin.data()+size, data));
    }

    // Test that 64-bit dates are preserved
    {
        TableDateAndBinary t;

        int64_t date = std::numeric_limits<int64_t>::max() - 400;

        t.add(date, BinaryData(""));
        CHECK_EQUAL(t[0].date.get().get_datetime(), date);
    }
}

// Test for a specific bug found: Calling clear on a group with a table with a subtable
TEST(Table_ClearWithSubtableAndGroup)
{
    Group group;
    TableRef table = group.add_table("test");
    DescriptorRef sub_1;

    // Create specification with sub-table
    table->add_column(type_String, "name");
    table->add_column(type_Table,  "sub", &sub_1);
    sub_1->add_column(type_Int,      "num");

    CHECK_EQUAL(2, table->get_column_count());

    // Add a row
    table->insert_empty_row(0);
    table->set_string(0, 0, "Foo");

    CHECK_EQUAL(0, table->get_subtable_size(1, 0));

    // Get the sub-table
    {
        TableRef subtable = table->get_subtable(1, 0);
        CHECK(subtable->is_empty());

        subtable->insert_empty_row(0);
        subtable->set_int(0, 0, 123);

        CHECK_EQUAL(123, subtable->get_int(0, 0));
    }

    CHECK_EQUAL(1, table->get_subtable_size(1, 0));

    table->clear();
}


//set a subtable in an already exisitng row by providing an existing subtable as the example to copy
// FIXME: Do we need both this one and Table_SetSubTableByExample2?
TEST(Table_SetSubTableByExample1)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

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
// FIXME: Do we need both this one and Table_SetSubTableByExample1?
TEST(Table_SetSubTableByExample2)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int,    "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table,  "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int,    "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add two rows
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    table->insert_empty_row(1);
    table->set_int(0, 1, 8);
    table->set_string(1, 1, "Hi!, Hello?");

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

        CHECK_EQUAL(false, subtable->is_empty());
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


TEST(Table_HasSharedSpec)
{
    MyTable2 table1;
    CHECK(!table1.has_shared_type());
    Group g;
    MyTable2::Ref table2 = g.add_table<MyTable2>("foo");
    CHECK(!table2->has_shared_type());
    table2->add();
    CHECK(table2[0].subtab->has_shared_type());

    // Subtable in mixed column
    TestTableMX::Ref table3 = g.add_table<TestTableMX>("bar");
    CHECK(!table3->has_shared_type());
    table3->add();
    table3[0].first.set_subtable<MyTable2>();
    MyTable2::Ref table4 = table3[0].first.get_subtable<MyTable2>();
    CHECK(table4);
    CHECK(!table4->has_shared_type());
    table4->add();
    CHECK(!table4->has_shared_type());
    CHECK(table4[0].subtab->has_shared_type());
}


namespace {
REALM_TABLE_3(TableAgg,
                c_int,   Int,
                c_float, Float,
                c_double, Double)

                // TODO: Bool? DateTime
} // anonymous namespace

#if TEST_DURATION > 0
#define TBL_SIZE REALM_MAX_BPNODE_SIZE*10
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
        f_sum += 4.0f;
        d_sum += 3.0;
    }
    table.add(1, 1.1f, 1.2);
    table.add(987654321, 11.0f, 12.0);
    table.add(5, 4.0f, 3.0);
    i_sum += 1 + 987654321 + 5;
    f_sum += double(1.1f) + double(11.0f) + double(4.0f);
    d_sum += 1.2 + 12.0 + 3.0;
    double size = TBL_SIZE + 3;

    double epsilon = std::numeric_limits<double>::epsilon();

    // minimum
    CHECK_EQUAL(1, table.column().c_int.minimum());
    CHECK_EQUAL(1.1f, table.column().c_float.minimum());
    CHECK_EQUAL(1.2, table.column().c_double.minimum());
    // maximum
    CHECK_EQUAL(987654321, table.column().c_int.maximum());
    CHECK_EQUAL(11.0f, table.column().c_float.maximum());
    CHECK_EQUAL(12.0, table.column().c_double.maximum());
    // sum
    CHECK_APPROXIMATELY_EQUAL(double(i_sum), double(table.column().c_int.sum()), 10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum, table.column().c_float.sum(),  10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum, table.column().c_double.sum(), 10*epsilon);
    // average
    CHECK_APPROXIMATELY_EQUAL(i_sum/size, table.column().c_int.average(),    10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum/size, table.column().c_float.average(),  10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum/size, table.column().c_double.average(), 10*epsilon);
}

namespace {
REALM_TABLE_1(TableAgg2,
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

// Test Table methods max, min, avg, sum, on both nullable and non-nullable columns
TEST(Table_Aggregates3)
{
    bool nullable = false;
    
    for (int i = 0; i < 2; i++) {
        // First we test everything with columns being nullable and with each column having at least 1 null
        // Then we test everything with non-nullable columns where the null entries will instead be just
        // 0, 0.0, etc.
        nullable = (i == 1);

        Group g;
        TableRef table = g.add_table("Inventory");

        table->insert_column(0, type_Int, "Price", nullable);
        table->insert_column(1, type_Float, "Shipping", nullable);
        table->insert_column(2, type_Double, "Rating", nullable);
        table->insert_column(3, type_DateTime, "Delivery date", nullable);

        table->add_empty_row(3);

        table->set_int(0, 0, 1);
        // table->set_null(0, 1);
        table->set_int(0, 2, 3);

        // table->set_null(1, 0);
        // table->set_null(1, 1);
        table->set_float(1, 2, 30.f);

        table->set_double(2, 0, 1.1);
        table->set_double(2, 1, 2.2);
        // table->set_null(2, 2);

        table->set_datetime(3, 0, DateTime(2016, 2, 2));
        // table->set_null(3, 1);
        table->set_datetime(3, 2, DateTime(2016, 6, 6));

        size_t count;
        size_t pos;
        if (i == 1) {
            // This i == 1 is the NULLABLE case where columns are nullable
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(0, &pos), 3);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_float(1, &pos), 30.f);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_double(2, &pos), 2.2);
            CHECK_EQUAL(pos, 1);

            pos = 123;
            CHECK_EQUAL(table->maximum_datetime(3, &pos), DateTime(2016, 6, 6));
            CHECK_EQUAL(pos, 2);

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(0, &pos), 1);
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_float(1, &pos), 30.f);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->minimum_double(2, &pos), 1.1);
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_datetime(3, &pos), DateTime(2016, 2, 2));
            CHECK_EQUAL(pos, 0);

            // average
            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_int(0, &count), (1 + 3) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            count = 123;
            CHECK_EQUAL(table->average_float(1, &count), 30.f);
            CHECK_EQUAL(count, 1);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_double(2, &count), (1.1 + 2.2) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            // sum
            CHECK_EQUAL(table->sum_int(0), 4);
            CHECK_EQUAL(table->sum_float(1), 30.f);
            CHECK_APPROXIMATELY_EQUAL(table->sum_double(2), 1.1 + 2.2, 0.01);
        }
        else {
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(0, &pos), 3);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_float(1, &pos), 30.f);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_double(2, &pos), 2.2);
            CHECK_EQUAL(pos, 1);

            pos = 123;
            CHECK_EQUAL(table->maximum_datetime(3, &pos), DateTime(2016, 6, 6));
            CHECK_EQUAL(pos, 2);

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(0, &pos), 0);
            CHECK_EQUAL(pos, 1);

            pos = 123;
            CHECK_EQUAL(table->minimum_float(1, &pos), 0.f);
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_double(2, &pos), 0.);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->minimum_datetime(3, &pos), DateTime(0));
            CHECK_EQUAL(pos, 1);

            // average
            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_int(0, &count), (1 + 3 + 0) / 3., 0.01);
            CHECK_EQUAL(count, 3);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_float(1, &count), 30.f / 3., 0.01);
            CHECK_EQUAL(count, 3);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_double(2, &count), (1.1 + 2.2 + 0.) / 3., 0.01);
            CHECK_EQUAL(count, 3);

            // sum
            CHECK_EQUAL(table->sum_int(0), 4);
            CHECK_EQUAL(table->sum_float(1), 30.f);
            CHECK_APPROXIMATELY_EQUAL(table->sum_double(2), 1.1 + 2.2, 0.01);
        }
    }
}

TEST(Table_LanguageBindings)
{
   Table* table = LangBindHelper::new_table();
   CHECK(table->is_attached());

   table->add_column(type_Int, "i");
   table->insert_empty_row(0);
   table->set_int(0, 0, 10);
   table->insert_empty_row(1);
   table->set_int(0, 1, 12);

   Table* table2 = LangBindHelper::copy_table(*table);
   CHECK(table2->is_attached());

   CHECK(*table == *table2);

   LangBindHelper::unbind_table_ptr(table);
   LangBindHelper::unbind_table_ptr(table2);
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
    DescriptorRef subdesc;
    root.add_column(type_Table, "b", &subdesc);
    subdesc->add_column(type_Int,  "a");
    root.add_empty_row(1);
    root.set_subtable(0, 0, &sub);
    root.set_subtable(0, 0, 0);
}


namespace {

REALM_TABLE_3(TablePivotAgg,
                sex,   String,
                age,   Int,
                hired, Bool)

} // anonymous namespace

TEST(Table_Pivot)
{
    size_t count = 1717;
    TablePivotAgg table;
    int64_t age_sum[2] = {0, 0};
    int64_t age_cnt[2] = {0, 0};
    int64_t age_min[2];
    int64_t age_max[2];
    double age_avg[2];

    for (size_t i = 0; i < count; ++i) {
        size_t sex = i % 2;
        int64_t age =  3 + (i%117);
        table.add((sex==0) ? "Male" : "Female", age, true);

        age_sum[sex] += age;
        age_cnt[sex] += 1;
        if ((i < 2) || age < age_min[sex])
            age_min[sex] = age;
        if ((i < 2) || age > age_max[sex])
            age_max[sex] = age;
    }
    for (size_t sex = 0; sex < 2; ++sex) {
        age_avg[sex] = double(age_sum[sex]) / double(age_cnt[sex]);
    }


    for (int i = 0; i < 2; ++i) {
        Table result_count;
        table.aggregate(0, 1, Table::aggr_count, result_count);
        CHECK_EQUAL(2, result_count.get_column_count());
        CHECK_EQUAL(2, result_count.size());
        for (size_t sex = 0; sex < 2; ++sex) {
            CHECK_EQUAL(age_cnt[sex], result_count.get_int(1, sex));
        }

        Table result_sum;
        table.aggregate(0, 1, Table::aggr_sum, result_sum);
        for (size_t sex = 0; sex < 2; ++sex) {
            CHECK_EQUAL(age_sum[sex], result_sum.get_int(1, sex));
        }

        Table result_avg;
        table.aggregate(0, 1, Table::aggr_avg, result_avg);
        if ((false)) {
            std::ostringstream ss;
            result_avg.to_string(ss);
            std::cerr << "\nMax:\n" << ss.str();
        }
        CHECK_EQUAL(2, result_avg.get_column_count());
        CHECK_EQUAL(2, result_avg.size());
        for (size_t sex = 0; sex < 2; ++sex) {
            CHECK_EQUAL(age_avg[sex], result_avg.get_double(1, sex));
        }

        Table result_min;
        table.aggregate(0, 1, Table::aggr_min, result_min);
        CHECK_EQUAL(2, result_min.get_column_count());
        CHECK_EQUAL(2, result_min.size());
        for (size_t sex = 0; sex < 2; ++sex) {
            CHECK_EQUAL(age_min[sex], result_min.get_int(1, sex));
        }

        Table result_max;
        table.aggregate(0, 1, Table::aggr_max, result_max);
        CHECK_EQUAL(2, result_max.get_column_count());
        CHECK_EQUAL(2, result_max.size());
        for (size_t sex = 0; sex < 2; ++sex) {
            CHECK_EQUAL(age_max[sex], result_max.get_int(1, sex));
        }

        // Test with enumerated strings in second loop
        table.optimize();
    }
}


namespace {

void compare_table_with_slice(TestResults& test_results, const Table& table,
                              const Table& slice, size_t offset, size_t size)
{
    ConstDescriptorRef table_desc = table.get_descriptor();
    ConstDescriptorRef slice_desc = slice.get_descriptor();
    CHECK(*table_desc == *slice_desc);
    if (*table_desc != *slice_desc)
        return;

    size_t num_cols = table.get_column_count();
    for (size_t col_i = 0; col_i != num_cols; ++col_i) {
        DataType type = table.get_column_type(col_i);
        switch (type) {
            case type_Int:
            case type_Link:
                for (size_t i = 0; i != size; ++i) {
                    int_fast64_t v_1 = table.get_int(col_i, offset + i);
                    int_fast64_t v_2 = slice.get_int(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Bool:
                for (size_t i = 0; i != size; ++i) {
                    bool v_1 = table.get_bool(col_i, offset + i);
                    bool v_2 = slice.get_bool(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Float:
                for (size_t i = 0; i != size; ++i) {
                    float v_1 = table.get_float(col_i, offset + i);
                    float v_2 = slice.get_float(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Double:
                for (size_t i = 0; i != size; ++i) {
                    double v_1 = table.get_double(col_i, offset + i);
                    double v_2 = slice.get_double(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_String:
                for (size_t i = 0; i != size; ++i) {
                    StringData v_1 = table.get_string(col_i, offset + i);
                    StringData v_2 = slice.get_string(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Binary:
                for (size_t i = 0; i != size; ++i) {
                    BinaryData v_1 = table.get_binary(col_i, offset + i);
                    BinaryData v_2 = slice.get_binary(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_DateTime:
                for (size_t i = 0; i != size; ++i) {
                    DateTime v_1 = table.get_datetime(col_i, offset + i);
                    DateTime v_2 = slice.get_datetime(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Table:
                for (size_t i = 0; i != size; ++i) {
                    ConstTableRef t_1 = table.get_subtable(col_i, offset + i);
                    ConstTableRef t_2 = slice.get_subtable(col_i, i);
                    CHECK(*t_1 == *t_2);
                }
                break;
            case type_Mixed:
                for (size_t i = 0; i != size; ++i) {
                    Mixed v_1 = table.get_mixed(col_i, offset + i);
                    Mixed v_2 = slice.get_mixed(col_i, i);
                    CHECK_EQUAL(v_1.get_type(), v_2.get_type());
                    if (v_1.get_type() == v_2.get_type()) {
                        switch (v_1.get_type()) {
                            case type_Int:
                                CHECK_EQUAL(v_1.get_int(), v_2.get_int());
                                break;
                            case type_Bool:
                                CHECK_EQUAL(v_1.get_bool(), v_2.get_bool());
                                break;
                            case type_Float:
                                CHECK_EQUAL(v_1.get_float(), v_2.get_float());
                                break;
                            case type_Double:
                                CHECK_EQUAL(v_1.get_double(), v_2.get_double());
                                break;
                            case type_String:
                                CHECK_EQUAL(v_1.get_string(), v_2.get_string());
                                break;
                            case type_Binary:
                                CHECK_EQUAL(v_1.get_binary(), v_2.get_binary());
                                break;
                            case type_DateTime:
                                CHECK_EQUAL(v_1.get_datetime(), v_2.get_datetime());
                                break;
                            case type_Table: {
                                ConstTableRef t_1 = table.get_subtable(col_i, offset + i);
                                ConstTableRef t_2 = slice.get_subtable(col_i, i);
                                CHECK(*t_1 == *t_2);
                                break;
                            }
                            case type_Mixed:
                            case type_Link:
                            case type_LinkList:
                                REALM_ASSERT(false);
                        }
                    }
                }
                break;
            case type_LinkList:
                break;
        }
    }
}


void test_write_slice_name(TestResults& test_results, const Table& table,
                           StringData expect_name, bool override_name)
{
    size_t offset = 0, size = 0;
    std::ostringstream out;
    if (override_name) {
        table.write(out, offset, size, expect_name);
    }
    else {
        table.write(out, offset, size);
    }
    std::string str = out.str();
    BinaryData buffer(str.data(), str.size());
    bool take_ownership = false;
    Group group(buffer, take_ownership);
    TableRef slice = group.get_table(expect_name);
    CHECK(slice);
}

void test_write_slice_contents(TestResults& test_results, const Table& table,
                               size_t offset, size_t size)
{
    std::ostringstream out;
    table.write(out, offset, size);
    std::string str = out.str();
    BinaryData buffer(str.data(), str.size());
    bool take_ownership = false;
    Group group(buffer, take_ownership);
    TableRef slice = group.get_table("test");
    CHECK(slice);
    if (slice) {
        size_t remaining_size = table.size() - offset;
        size_t size_2 = size;
        if (size_2 > remaining_size)
            size_2 = remaining_size;
        CHECK_EQUAL(size_2, slice->size());
        if (size_2 == slice->size())
            compare_table_with_slice(test_results, table, *slice, offset, size_2);
    }
}

} // anonymous namespace


TEST(Table_WriteSlice)
{
    // check that the name of the written table is as expected
    {
        Table table;
        test_write_slice_name(test_results, table, "",    false);
        test_write_slice_name(test_results, table, "foo", true); // Override
        test_write_slice_name(test_results, table, "",    true); // Override
    }
    {
        Group group;
        TableRef table = group.add_table("test");
        test_write_slice_name(test_results, *table, "test", false);
        test_write_slice_name(test_results, *table, "foo",  true); // Override
        test_write_slice_name(test_results, *table, "",     true); // Override
    }

    // Run through a 3-D matrix of table sizes, slice offsets, and
    // slice sizes. Each test involves a table with columns of each
    // possible type.
#ifdef REALM_DEBUG
    int table_sizes[] = { 0, 1, 2, 3, 5, 9, 27, 81, 82, 135 };
#else
    int table_sizes[] = { 0, 1, 2, 3, 5, 9, 27, 81, 82, 243, 729, 2187, 6561 };
#endif
    int num_sizes = sizeof table_sizes / sizeof *table_sizes;
    for (int table_size_i = 0; table_size_i != num_sizes; ++table_size_i) {
        int table_size = table_sizes[table_size_i];
        Group group;
        TableRef table = group.add_table("test");
        bool fixed_subtab_sizes = true;
        setup_multi_table(*table, table_size, 1, fixed_subtab_sizes);
        for (int offset_i = 0; offset_i != num_sizes; ++offset_i) {
            int offset = table_sizes[offset_i];
            if (offset > table_size)
                break;
            for (int size_i = 0; size_i != num_sizes; ++size_i) {
                int size = table_sizes[size_i];
                // This also checks that the range can extend beyond
                // end of table
                test_write_slice_contents(test_results, *table, offset, size);
                if (offset + size > table_size)
                    break;
            }
        }
    }
}


TEST(Table_Parent)
{
    TableRef table = Table::create();
    CHECK_EQUAL(TableRef(), table->get_parent_table());
    CHECK_EQUAL(realm::npos, table->get_parent_row_index()); // Not a subtable
    CHECK_EQUAL(realm::npos, table->get_index_in_group()); // Not a group-level table

    DescriptorRef subdesc;
    table->add_column(type_Table, "", &subdesc);
    table->add_column(type_Mixed, "");
    subdesc->add_column(type_Int, "");
    table->add_empty_row(2);
    table->set_mixed(1, 0, Mixed::subtable_tag());
    table->set_mixed(1, 1, Mixed::subtable_tag());

    TableRef subtab;
    size_t column_ndx = 0;

    subtab = table->get_subtable(0,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(0,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(1,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    // Check that column indexes are properly adjusted after new
    // column is insert.
    table->insert_column(0, type_Int, "");

    subtab = table->get_subtable(1,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(2,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(2, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(2,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(2, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    // Check that column indexes are properly adjusted after inserted
    // column is removed.
    table->remove_column(0);

    subtab = table->get_subtable(0,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(0,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(1,0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1,1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());
}


TEST(Table_RegularSubtablesRetain)
{
    // Create one degenerate subtable
    TableRef parent = Table::create();
    DescriptorRef subdesc;
    parent->add_column(type_Table, "a", &subdesc);
    subdesc->add_column(type_Int, "x");
    parent->add_empty_row();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(1, parent->size());
    TableRef subtab_0_0 = parent->get_subtable(0,0);
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_0->size());

    // Expand to 4 subtables in a 2-by-2 parent.
    parent->add_column(type_Table, "b", &subdesc);
    subdesc->add_column(type_Int, "x");
    parent->add_empty_row();
    subtab_0_0->add_empty_row();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    TableRef subtab_0_1 = parent->get_subtable(0,1);
    CHECK_EQUAL(1, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_1->size());
    TableRef subtab_1_0 = parent->get_subtable(1,0);
    CHECK_EQUAL(1, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_0->size());
    TableRef subtab_1_1 = parent->get_subtable(1,1);
    CHECK_EQUAL(1, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_1->size());

    // Check that subtables get their specs correctly updated
    subdesc = parent->get_subdescriptor(0);
    subdesc->add_column(type_Float, "f");
    subdesc = parent->get_subdescriptor(1);
    subdesc->add_column(type_Double, "d");
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    subtab_1_1->add_empty_row();
    subtab_0_0->set_int    (0, 0, 10000);
    subtab_0_0->set_float  (1, 0, 10010.0f);
    subtab_1_1->set_int    (0, 0, 11100);
    subtab_1_1->set_double (1, 0, 11110.0);
    parent->add_empty_row();
    CHECK_EQUAL(3, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10000,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11100,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11110.0,  subtab_1_1->get_double (1,0));

    // Insert a row and a column before all the subtables
    parent->insert_column(0, type_Table, "dummy_1");
    parent->insert_empty_row(0);
    subtab_0_0->set_int    (0, 0, 10001);
    subtab_0_0->set_float  (1, 0, 10011.0f);
    subtab_1_1->set_int    (0, 0, 11101);
    subtab_1_1->set_double (1, 0, 11111.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(4, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10001,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11101,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11111.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Insert a row and a column between the subtables
    parent->insert_column(2, type_Int, "dummy_2");
    parent->insert_empty_row(2);
    subtab_0_0->set_int    (0, 0, 10002);
    subtab_0_0->set_float  (1, 0, 10012.0f);
    subtab_1_1->set_int    (0, 0, 11102);
    subtab_1_1->set_double (1, 0, 11112.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10002,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11102,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11112.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Insert a column after the subtables
    parent->insert_column(4, type_Table, "dummy_3");
    subtab_0_0->set_int    (0, 0, 10003);
    subtab_0_0->set_float  (1, 0, 10013.0f);
    subtab_1_1->set_int    (0, 0, 11103);
    subtab_1_1->set_double (1, 0, 11113.0);
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(type_Table, parent->get_column_type(4));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10003,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11103,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11113.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Remove the row and the column between the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int    (0, 0, 10004);
    subtab_0_0->set_float  (1, 0, 10014.0f);
    subtab_1_1->set_int    (0, 0, 11104);
    subtab_1_1->set_double (1, 0, 11114.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(4, parent->size());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10004,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11104,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11114.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Remove the row and the column before the subtables
    parent->remove_column(0);
    parent->remove(0);
    subtab_0_0->set_int    (0, 0, 10005);
    subtab_0_0->set_float  (1, 0, 10015.0f);
    subtab_1_1->set_int    (0, 0, 11105);
    subtab_1_1->set_double (1, 0, 11115.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11105,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11115.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Remove the row and the column after the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int    (0, 0, 10006);
    subtab_0_0->set_float  (1, 0, 10016.0f);
    subtab_1_1->set_int    (0, 0, 11106);
    subtab_1_1->set_double (1, 0, 11116.0);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11106,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11116.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Check that subtable accessors are detached when the subtables are removed
    parent->remove(1);
    subtab_0_0->set_int   (0, 0, 10007);
    subtab_0_0->set_float (1, 0, 10017.0f);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK( subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    parent->remove_column(1);
    subtab_0_0->set_int   (0, 0, 10008);
    subtab_0_0->set_float (1, 0, 10018.0f);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Clear subtable
    parent->clear_subtable(0,0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Clear parent table
    parent->clear();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 4 new subtables, then remove some of them in a different way
    parent->add_column(type_Table, "c", &subdesc);
    subdesc->add_column(type_String, "x");
    parent->add_empty_row(2);
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    subtab_1_1->add_empty_row();
    subtab_1_1->set_string(0, 0, "pneumonoultramicroscopicsilicovolcanoconiosis");
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));
    parent->remove(0);
    parent->remove_column(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0,0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK( subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));

    // Insert 2x2 new subtables, then remove them all together
    parent->add_column(type_Table, "d", &subdesc);
    subdesc->add_column(type_String, "x");
    parent->add_empty_row(2);
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    subtab_1_1->add_empty_row();
    subtab_1_1->set_string(0, 0, "supercalifragilisticexpialidocious");
    parent->clear();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last row
    parent->add_empty_row(1);
    parent->remove_column(0);
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_0->add_empty_row(1);
    subtab_0_0->set_string(0, 0, "brahmaputra");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0,0));
    parent->remove(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    parent->add_empty_row(1);
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_0->add_empty_row(1);
    subtab_0_0->set_string(0, 0, "baikonur");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0,0));
    parent->remove_column(0);
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(Table_MixedSubtablesRetain)
{
    // Create one degenerate subtable
    TableRef parent = Table::create();
    parent->add_column(type_Mixed, "a");
    parent->add_empty_row();
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    TableRef subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_0->add_column(type_Int, "x");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(1, parent->size());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_0->size());

    // Expand to 4 subtables in a 2-by-2 parent.
    subtab_0_0->add_empty_row();
    parent->add_column(type_Mixed, "b");
    parent->set_mixed(1, 0, Mixed::subtable_tag());
    TableRef subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_0->add_column(type_Int, "x");
    parent->add_empty_row();
    parent->set_mixed(0, 1, Mixed::subtable_tag());
    TableRef subtab_0_1 = parent->get_subtable(0,1);
    subtab_0_1->add_column(type_Int, "x");
    parent->set_mixed(1, 1, Mixed::subtable_tag());
    TableRef subtab_1_1 = parent->get_subtable(1,1);
    subtab_1_1->add_column(type_Int, "x");
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(1, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(1, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_1->size());

    // Check that subtables get their specs correctly updated
    subtab_0_0->add_column(type_Float,  "f");
    subtab_0_1->add_column(type_Float,  "f");
    subtab_1_0->add_column(type_Double, "d");
    subtab_1_1->add_column(type_Double, "d");
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int,   subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int,    subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    subtab_1_1->add_empty_row();
    subtab_0_0->set_int    (0, 0, 10000);
    subtab_0_0->set_float  (1, 0, 10010.0f);
    subtab_1_1->set_int    (0, 0, 11100);
    subtab_1_1->set_double (1, 0, 11110.0);
    parent->add_empty_row();
    CHECK_EQUAL(3, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10000,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11100,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11110.0,  subtab_1_1->get_double (1,0));

    // Insert a row and a column before all the subtables
    parent->insert_column(0, type_Table, "dummy_1");
    parent->insert_empty_row(0);
    subtab_0_0->set_int    (0, 0, 10001);
    subtab_0_0->set_float  (1, 0, 10011.0f);
    subtab_1_1->set_int    (0, 0, 11101);
    subtab_1_1->set_double (1, 0, 11111.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(2));
    CHECK_EQUAL(4, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10001,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11101,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11111.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Insert a row and a column between the subtables
    parent->insert_column(2, type_Int, "dummy_2");
    parent->insert_empty_row(2);
    parent->set_mixed(3, 2, "Lopadotemachoselachogaleokranioleipsanodrimhypotrimmatosilphio"
                        "paraomelitokatakechy­menokichlepikossyphophattoperisteralektryonopte"
                        "kephalliokigklopeleiolagoiosiraiobaphetraganopterygon");
    subtab_0_0->set_int    (0, 0, 10002);
    subtab_0_0->set_float  (1, 0, 10012.0f);
    subtab_1_1->set_int    (0, 0, 11102);
    subtab_1_1->set_double (1, 0, 11112.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(3));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10002,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11102,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11112.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Insert a column after the subtables
    parent->insert_column(4, type_Table, "dummy_3");
    subtab_0_0->set_int    (0, 0, 10003);
    subtab_0_0->set_float  (1, 0, 10013.0f);
    subtab_1_1->set_int    (0, 0, 11103);
    subtab_1_1->set_double (1, 0, 11113.0);
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int,   parent->get_column_type(2));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(3));
    CHECK_EQUAL(type_Table, parent->get_column_type(4));
    CHECK_EQUAL(5, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10003,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11103,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11113.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3,3));

    // Remove the row and the column between the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int    (0, 0, 10004);
    subtab_0_0->set_float  (1, 0, 10014.0f);
    subtab_1_1->set_int    (0, 0, 11104);
    subtab_1_1->set_double (1, 0, 11114.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(2));
    CHECK_EQUAL(type_Table, parent->get_column_type(3));
    CHECK_EQUAL(4, parent->size());
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL(10004,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11104,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11114.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1,1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1,2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2,1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2,2));

    // Remove the row and the column before the subtables
    parent->remove_column(0);
    parent->remove(0);
    subtab_0_0->set_int    (0, 0, 10005);
    subtab_0_0->set_float  (1, 0, 10015.0f);
    subtab_1_1->set_int    (0, 0, 11105);
    subtab_1_1->set_double (1, 0, 11115.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11105,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11115.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Remove the row and the column after the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int    (0, 0, 10006);
    subtab_0_0->set_float  (1, 0, 10016.0f);
    subtab_1_1->set_int    (0, 0, 11106);
    subtab_1_1->set_double (1, 0, 11116.0);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006,    subtab_0_0->get_int    (0,0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float  (1,0));
    CHECK_EQUAL(11106,    subtab_1_1->get_int    (0,0));
    CHECK_EQUAL(11116.0,  subtab_1_1->get_double (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0,1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1,1));

    // Check that subtable accessors are detached when the subtables are removed
    parent->remove(1);
    subtab_0_0->set_int   (0, 0, 10007);
    subtab_0_0->set_float (1, 0, 10017.0f);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK( subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1,0));
    parent->remove_column(1);
    subtab_0_0->set_int   (0, 0, 10008);
    subtab_0_0->set_float (1, 0, 10018.0f);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK( subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008,    subtab_0_0->get_int   (0,0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float (1,0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0,0));

    // Remove subtable
    parent->clear_subtable(0,0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Clear parent table
    parent->clear();
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 4 new subtables, then remove some of them in a different way
    parent->add_column(type_Mixed, "c");
    parent->add_empty_row(2);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    parent->set_mixed(0, 1, Mixed::subtable_tag());
    parent->set_mixed(1, 0, Mixed::subtable_tag());
    parent->set_mixed(1, 1, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    CHECK(subtab_0_0);
    CHECK(subtab_0_1);
    CHECK(subtab_1_0);
    CHECK(subtab_1_1);
    subtab_1_1->add_column(type_String, "x");
    subtab_1_1->add_empty_row();
    subtab_1_1->set_string(0, 0, "pneumonoultramicroscopicsilicovolcanoconiosis");
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(2, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(0, subtab_0_1->size());
    CHECK_EQUAL(0, subtab_1_0->size());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));
    parent->remove(0);
    parent->remove_column(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0,0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK( subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0,0));

    // Insert 2x2 new subtables, then remove them all together
    parent->add_column(type_Mixed, "d");
    parent->add_empty_row(2);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    parent->set_mixed(0, 1, Mixed::subtable_tag());
    parent->set_mixed(1, 0, Mixed::subtable_tag());
    parent->set_mixed(1, 1, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_1 = parent->get_subtable(0,1);
    subtab_1_0 = parent->get_subtable(1,0);
    subtab_1_1 = parent->get_subtable(1,1);
    subtab_1_1->add_column(type_String, "x");
    subtab_1_1->add_empty_row();
    subtab_1_1->set_string(0, 0, "supercalifragilisticexpialidocious");
    parent->clear();
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last row
    parent->add_empty_row(1);
    parent->remove_column(0);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_0->add_column(type_String, "x");
    subtab_0_0->add_empty_row(1);
    subtab_0_0->set_string(0, 0, "brahmaputra");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0,0));
    parent->remove(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    parent->add_empty_row(1);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0,0);
    subtab_0_0->add_column(type_String, "x");
    subtab_0_0->add_empty_row(1);
    subtab_0_0->set_string(0, 0, "baikonur");
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL("d", parent->get_column_name(0));
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(1, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_String, subtab_0_0->get_column_type(0));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL(1, subtab_0_0->size());
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0,0));
    parent->remove_column(0);
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(Table_RowAccessor)
{
    Table table;
    DescriptorRef subdesc;
    table.add_column(type_Int,      "int");
    table.add_column(type_Bool,     "bool");
    table.add_column(type_Float,    "");
    table.add_column(type_Double,   "");
    table.add_column(type_String,   "");
    table.add_column(type_Binary,   "", true);
    table.add_column(type_DateTime, "");
    table.add_column(type_Table,    "", &subdesc);
    table.add_column(type_Mixed,    "");
    subdesc->add_column(type_Int, "i");
    table.add_empty_row(2);

    BinaryData bin("bin", 3);

    Table empty_subtab;
    empty_subtab.add_column(type_Int, "i");

    Table one_subtab;
    one_subtab.add_column(type_Int, "i");
    one_subtab.add_empty_row(1);
    one_subtab.set_int(0, 0, 19);

    Table two_subtab;
    two_subtab.add_column(type_Int, "i");
    two_subtab.add_empty_row(1);
    two_subtab.set_int(0, 0, 29);

    table.set_int      (0, 1, 4923);
    table.set_bool     (1, 1, true);
    table.set_float    (2, 1, 5298.0f);
    table.set_double   (3, 1, 2169.0);
    table.set_string   (4, 1, "str");
    table.set_binary   (5, 1, bin);
    table.set_datetime (6, 1, 7739);
    table.set_subtable (7, 1, &one_subtab);
    table.set_mixed    (8, 1, Mixed("mix"));

    // Check getters for `RowExpr`
    {
        CHECK_EQUAL(9,         table[0].get_column_count());
        CHECK_EQUAL(type_Int,  table[0].get_column_type(0));
        CHECK_EQUAL(type_Bool, table[0].get_column_type(1));
        CHECK_EQUAL("int",     table[0].get_column_name(0));
        CHECK_EQUAL("bool",    table[0].get_column_name(1));
        CHECK_EQUAL(0,         table[0].get_column_index("int"));
        CHECK_EQUAL(1,         table[0].get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(),  table[0].get_int           (0));
        CHECK_EQUAL(bool(),          table[0].get_bool          (1));
        CHECK_EQUAL(float(),         table[0].get_float         (2));
        CHECK_EQUAL(double(),        table[0].get_double        (3));
        CHECK_EQUAL(StringData(""),    table[0].get_string        (4));
        CHECK_EQUAL(BinaryData(),    table[0].get_binary        (5));
        CHECK_EQUAL(DateTime(),      table[0].get_datetime      (6));
        CHECK_EQUAL(0,               table[0].get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  table[0].get_mixed         (8));
        CHECK_EQUAL(type_Int,        table[0].get_mixed_type    (8));

        CHECK_EQUAL(4923,            table[1].get_int           (0));
        CHECK_EQUAL(true,            table[1].get_bool          (1));
        CHECK_EQUAL(5298.0f,         table[1].get_float         (2));
        CHECK_EQUAL(2169.0,          table[1].get_double        (3));
        CHECK_EQUAL("str",           table[1].get_string        (4));
        CHECK_EQUAL(bin,             table[1].get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  table[1].get_datetime      (6));
        CHECK_EQUAL(1,               table[1].get_subtable_size (7));
        CHECK_EQUAL("mix",           table[1].get_mixed         (8));
        CHECK_EQUAL(type_String,     table[1].get_mixed_type    (8));

        TableRef subtab_0 = table[0].get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = table[1].get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `ConstRowExpr`
    {
        const Table& const_table = table;

        CHECK_EQUAL(9,         const_table[0].get_column_count());
        CHECK_EQUAL(type_Int,  const_table[0].get_column_type(0));
        CHECK_EQUAL(type_Bool, const_table[0].get_column_type(1));
        CHECK_EQUAL("int",     const_table[0].get_column_name(0));
        CHECK_EQUAL("bool",    const_table[0].get_column_name(1));
        CHECK_EQUAL(0,         const_table[0].get_column_index("int"));
        CHECK_EQUAL(1,         const_table[0].get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(),  const_table[0].get_int           (0));
        CHECK_EQUAL(bool(),          const_table[0].get_bool          (1));
        CHECK_EQUAL(float(),         const_table[0].get_float         (2));
        CHECK_EQUAL(double(),        const_table[0].get_double        (3));
        CHECK_EQUAL(StringData(""),  const_table[0].get_string        (4));
        CHECK_EQUAL(BinaryData(),  const_table[0].get_binary        (5));
        CHECK_EQUAL(DateTime(),      const_table[0].get_datetime      (6));
        CHECK_EQUAL(0,               const_table[0].get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  const_table[0].get_mixed         (8));
        CHECK_EQUAL(type_Int,        const_table[0].get_mixed_type    (8));

        CHECK_EQUAL(4923,            const_table[1].get_int           (0));
        CHECK_EQUAL(true,            const_table[1].get_bool          (1));
        CHECK_EQUAL(5298.0f,         const_table[1].get_float         (2));
        CHECK_EQUAL(2169.0,          const_table[1].get_double        (3));
        CHECK_EQUAL("str",           const_table[1].get_string        (4));
        CHECK_EQUAL(bin,             const_table[1].get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  const_table[1].get_datetime      (6));
        CHECK_EQUAL(1,               const_table[1].get_subtable_size (7));
        CHECK_EQUAL("mix",           const_table[1].get_mixed         (8));
        CHECK_EQUAL(type_String,     const_table[1].get_mixed_type    (8));

        ConstTableRef subtab_0 = const_table[0].get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = const_table[1].get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `Row`
    {
        Row row_0 = table[0];
        Row row_1 = table[1];

        CHECK_EQUAL(9,         row_0.get_column_count());
        CHECK_EQUAL(type_Int,  row_0.get_column_type(0));
        CHECK_EQUAL(type_Bool, row_0.get_column_type(1));
        CHECK_EQUAL("int",     row_0.get_column_name(0));
        CHECK_EQUAL("bool",    row_0.get_column_name(1));
        CHECK_EQUAL(0,         row_0.get_column_index("int"));
        CHECK_EQUAL(1,         row_0.get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(),  row_0.get_int           (0));
        CHECK_EQUAL(bool(),          row_0.get_bool          (1));
        CHECK_EQUAL(float(),         row_0.get_float         (2));
        CHECK_EQUAL(double(),        row_0.get_double        (3));
        CHECK_EQUAL(StringData(""),  row_0.get_string        (4));
        CHECK_EQUAL(BinaryData(),  row_0.get_binary        (5));
        CHECK_EQUAL(DateTime(),      row_0.get_datetime      (6));
        CHECK_EQUAL(0,               row_0.get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  row_0.get_mixed         (8));
        CHECK_EQUAL(type_Int,        row_0.get_mixed_type    (8));

        CHECK_EQUAL(4923,            row_1.get_int           (0));
        CHECK_EQUAL(true,            row_1.get_bool          (1));
        CHECK_EQUAL(5298.0f,         row_1.get_float         (2));
        CHECK_EQUAL(2169.0,          row_1.get_double        (3));
        CHECK_EQUAL("str",           row_1.get_string        (4));
        CHECK_EQUAL(bin,             row_1.get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  row_1.get_datetime      (6));
        CHECK_EQUAL(1,               row_1.get_subtable_size (7));
        CHECK_EQUAL("mix",           row_1.get_mixed         (8));
        CHECK_EQUAL(type_String,     row_1.get_mixed_type    (8));

        TableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `const Row`
    {
        const Row row_0 = table[0];
        const Row row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(),  row_0.get_int           (0));
        CHECK_EQUAL(bool(),          row_0.get_bool          (1));
        CHECK_EQUAL(float(),         row_0.get_float         (2));
        CHECK_EQUAL(double(),        row_0.get_double        (3));
        CHECK_EQUAL(StringData(""),  row_0.get_string        (4));
        CHECK_EQUAL(BinaryData(),  row_0.get_binary        (5));
        CHECK_EQUAL(DateTime(),      row_0.get_datetime      (6));
        CHECK_EQUAL(0,               row_0.get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  row_0.get_mixed         (8));
        CHECK_EQUAL(type_Int,        row_0.get_mixed_type    (8));

        CHECK_EQUAL(4923,            row_1.get_int           (0));
        CHECK_EQUAL(true,            row_1.get_bool          (1));
        CHECK_EQUAL(5298.0f,         row_1.get_float         (2));
        CHECK_EQUAL(2169.0,          row_1.get_double        (3));
        CHECK_EQUAL("str",           row_1.get_string        (4));
        CHECK_EQUAL(bin,             row_1.get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  row_1.get_datetime      (6));
        CHECK_EQUAL(1,               row_1.get_subtable_size (7));
        CHECK_EQUAL("mix",           row_1.get_mixed         (8));
        CHECK_EQUAL(type_String,     row_1.get_mixed_type    (8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `ConstRow`
    {
        ConstRow row_0 = table[0];
        ConstRow row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(),  row_0.get_int           (0));
        CHECK_EQUAL(bool(),          row_0.get_bool          (1));
        CHECK_EQUAL(float(),         row_0.get_float         (2));
        CHECK_EQUAL(double(),        row_0.get_double        (3));
        CHECK_EQUAL(StringData(""),  row_0.get_string        (4));
        CHECK_EQUAL(BinaryData(),  row_0.get_binary        (5));
        CHECK_EQUAL(DateTime(),      row_0.get_datetime      (6));
        CHECK_EQUAL(0,               row_0.get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  row_0.get_mixed         (8));
        CHECK_EQUAL(type_Int,        row_0.get_mixed_type    (8));

        CHECK_EQUAL(4923,            row_1.get_int           (0));
        CHECK_EQUAL(true,            row_1.get_bool          (1));
        CHECK_EQUAL(5298.0f,         row_1.get_float         (2));
        CHECK_EQUAL(2169.0,          row_1.get_double        (3));
        CHECK_EQUAL("str",           row_1.get_string        (4));
        CHECK_EQUAL(bin,             row_1.get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  row_1.get_datetime      (6));
        CHECK_EQUAL(1,               row_1.get_subtable_size (7));
        CHECK_EQUAL("mix",           row_1.get_mixed         (8));
        CHECK_EQUAL(type_String,     row_1.get_mixed_type    (8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `const ConstRow` (double constness)
    {
        const ConstRow row_0 = table[0];
        const ConstRow row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(),  row_0.get_int           (0));
        CHECK_EQUAL(bool(),          row_0.get_bool          (1));
        CHECK_EQUAL(float(),         row_0.get_float         (2));
        CHECK_EQUAL(double(),        row_0.get_double        (3));
        CHECK_EQUAL(StringData(""),  row_0.get_string        (4));
        CHECK_EQUAL(BinaryData(),  row_0.get_binary        (5));
        CHECK_EQUAL(DateTime(),      row_0.get_datetime      (6));
        CHECK_EQUAL(0,               row_0.get_subtable_size (7));
        CHECK_EQUAL(int_fast64_t(),  row_0.get_mixed         (8));
        CHECK_EQUAL(type_Int,        row_0.get_mixed_type    (8));

        CHECK_EQUAL(4923,            row_1.get_int           (0));
        CHECK_EQUAL(true,            row_1.get_bool          (1));
        CHECK_EQUAL(5298.0f,         row_1.get_float         (2));
        CHECK_EQUAL(2169.0,          row_1.get_double        (3));
        CHECK_EQUAL("str",           row_1.get_string        (4));
        CHECK_EQUAL(bin,             row_1.get_binary        (5));
        CHECK_EQUAL(DateTime(7739),  row_1.get_datetime      (6));
        CHECK_EQUAL(1,               row_1.get_subtable_size (7));
        CHECK_EQUAL("mix",           row_1.get_mixed         (8));
        CHECK_EQUAL(type_String,     row_1.get_mixed_type    (8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check setters for `Row`
    {
        Row row_0 = table[0];
        Row row_1 = table[1];

        row_0.set_int      (0, 5651);
        row_0.set_bool     (1, true);
        row_0.set_float    (2, 8397.0f);
        row_0.set_double   (3, 1937.0);
        row_0.set_string   (4, "foo");
        row_0.set_binary   (5, bin);
        row_0.set_datetime (6, DateTime(9992));
        row_0.set_subtable (7, &one_subtab);
        row_0.set_mixed    (8, Mixed(3637.0f));

        row_1.set_int      (0, int_fast64_t());
        row_1.set_bool     (1, bool());
        row_1.set_float    (2, float());
        row_1.set_double   (3, double());
        row_1.set_string   (4, StringData(""));
        row_1.set_binary   (5, BinaryData());
        row_1.set_datetime (6, DateTime());
        row_1.set_subtable (7, 0);
        row_1.set_mixed    (8, Mixed());

        Mixed mix_subtab((Mixed::subtable_tag()));

        CHECK_EQUAL(5651,            table.get_int      (0,0));
        CHECK_EQUAL(true,            table.get_bool     (1,0));
        CHECK_EQUAL(8397.0f,         table.get_float    (2,0));
        CHECK_EQUAL(1937.0,          table.get_double   (3,0));
        CHECK_EQUAL("foo",           table.get_string   (4,0));
        CHECK_EQUAL(bin,             table.get_binary   (5,0));
        CHECK_EQUAL(DateTime(9992),  table.get_datetime (6,0));
        CHECK_EQUAL(3637.0f,         table.get_mixed    (8,0));

        CHECK_EQUAL(int_fast64_t(),  table.get_int      (0,1));
        CHECK_EQUAL(bool(),          table.get_bool     (1,1));
        CHECK_EQUAL(float(),         table.get_float    (2,1));
        CHECK_EQUAL(double(),        table.get_double   (3,1));
        CHECK_EQUAL(StringData(""),  table.get_string   (4,1));
        CHECK_EQUAL(BinaryData(),  table.get_binary   (5,1));
        CHECK_EQUAL(DateTime(),      table.get_datetime (6,1));
        CHECK_EQUAL(int_fast64_t(),  table.get_mixed    (8,1));

        TableRef subtab_0 = table.get_subtable(7,0);
        CHECK_EQUAL(19, subtab_0->get_int(0,0));
        CHECK(*subtab_0 == one_subtab);
        TableRef subtab_1 = table.get_subtable(7,1);
        CHECK(*subtab_1 == empty_subtab);

        row_0.set_mixed_subtable(8, 0);
        row_1.set_mixed_subtable(8, &two_subtab);
        subtab_0 = table.get_subtable(8,0);
        subtab_1 = table.get_subtable(8,1);
        CHECK(subtab_0);
        CHECK(subtab_1);
        CHECK(subtab_0->is_attached());
        CHECK(subtab_1->is_attached());
        CHECK(*subtab_0 == Table());
        CHECK_EQUAL(29, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == two_subtab);
    }

    // Check setters for `RowExpr`
    {
        table[0].set_int      (0, int_fast64_t());
        table[0].set_bool     (1, bool());
        table[0].set_float    (2, float());
        table[0].set_double   (3, double());
        table[0].set_string   (4, StringData(""));
        table[0].set_binary   (5, BinaryData());
        table[0].set_datetime (6, DateTime());
        table[0].set_subtable (7, 0);
        table[0].set_mixed    (8, Mixed());

        table[1].set_int      (0, 5651);
        table[1].set_bool     (1, true);
        table[1].set_float    (2, 8397.0f);
        table[1].set_double   (3, 1937.0);
        table[1].set_string   (4, "foo");
        table[1].set_binary   (5, bin);
        table[1].set_datetime (6, DateTime(9992));
        table[1].set_subtable (7, &one_subtab);
        table[1].set_mixed    (8, Mixed(3637.0f));

        Mixed mix_subtab((Mixed::subtable_tag()));

        CHECK_EQUAL(int_fast64_t(),  table.get_int      (0,0));
        CHECK_EQUAL(bool(),          table.get_bool     (1,0));
        CHECK_EQUAL(float(),         table.get_float    (2,0));
        CHECK_EQUAL(double(),        table.get_double   (3,0));
        CHECK_EQUAL(StringData(""),  table.get_string   (4,0));
        CHECK_EQUAL(BinaryData(),  table.get_binary   (5,0));
        CHECK_EQUAL(DateTime(),      table.get_datetime (6,0));
        CHECK_EQUAL(int_fast64_t(),  table.get_mixed    (8,0));

        CHECK_EQUAL(5651,            table.get_int      (0,1));
        CHECK_EQUAL(true,            table.get_bool     (1,1));
        CHECK_EQUAL(8397.0f,         table.get_float    (2,1));
        CHECK_EQUAL(1937.0,          table.get_double   (3,1));
        CHECK_EQUAL("foo",           table.get_string   (4,1));
        CHECK_EQUAL(bin,             table.get_binary   (5,1));
        CHECK_EQUAL(DateTime(9992),  table.get_datetime (6,1));
        CHECK_EQUAL(3637.0f,         table.get_mixed    (8,1));

        TableRef subtab_0 = table.get_subtable(7,0);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = table.get_subtable(7,1);
        CHECK_EQUAL(19, subtab_1->get_int(0,0));
        CHECK(*subtab_1 == one_subtab);

        table[0].set_mixed_subtable(8, &two_subtab);
        table[1].set_mixed_subtable(8, 0);
        subtab_0 = table.get_subtable(8,0);
        subtab_1 = table.get_subtable(8,1);
        CHECK(subtab_0);
        CHECK(subtab_1);
        CHECK(subtab_0->is_attached());
        CHECK(subtab_1->is_attached());
        CHECK_EQUAL(29, subtab_0->get_int(0,0));
        CHECK(*subtab_0 == two_subtab);
        CHECK(*subtab_1 == Table());
    }

    // Check that we can also create ConstRow's from `const Table`
    {
        const Table& const_table = table;
        ConstRow row_0 = const_table[0];
        ConstRow row_1 = const_table[1];
        CHECK_EQUAL(0,    row_0.get_int(0));
        CHECK_EQUAL(5651, row_1.get_int(0));
    }

    // Check that we can get the table and the row index from a Row
    {
        Row row_0 = table[0];
        Row row_1 = table[1];
        CHECK_EQUAL(&table, row_0.get_table());
        CHECK_EQUAL(&table, row_1.get_table());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
    }
}


TEST(Table_RowAccessorLinks)
{
    Group group;
    TableRef target_table = group.add_table("target");
    target_table->add_column(type_Int, "");
    target_table->add_empty_row(16);
    TableRef origin_table = group.add_table("origin");
    origin_table->add_column_link(type_Link, "", *target_table);
    origin_table->add_column_link(type_LinkList, "", *target_table);
    origin_table->add_empty_row(2);

    Row source_row_1 = origin_table->get(0);
    Row source_row_2 = origin_table->get(1);
    CHECK(source_row_1.is_null_link(0));
    CHECK(source_row_2.is_null_link(0));
    CHECK(source_row_1.linklist_is_empty(1));
    CHECK(source_row_2.linklist_is_empty(1));
    CHECK_EQUAL(0, source_row_1.get_link_count(1));
    CHECK_EQUAL(0, source_row_2.get_link_count(1));
    CHECK_EQUAL(0, target_table->get(7).get_backlink_count(*origin_table, 0));
    CHECK_EQUAL(0, target_table->get(13).get_backlink_count(*origin_table, 0));
    CHECK_EQUAL(0, target_table->get(11).get_backlink_count(*origin_table, 1));
    CHECK_EQUAL(0, target_table->get(15).get_backlink_count(*origin_table, 1));

    // Set links
    source_row_1.set_link(0, 7);
    source_row_2.set_link(0, 13);
    CHECK(!source_row_1.is_null_link(0));
    CHECK(!source_row_2.is_null_link(0));
    CHECK_EQUAL(7,  source_row_1.get_link(0));
    CHECK_EQUAL(13, source_row_2.get_link(0));
    CHECK_EQUAL(1, target_table->get(7).get_backlink_count(*origin_table, 0));
    CHECK_EQUAL(1, target_table->get(13).get_backlink_count(*origin_table, 0));
    CHECK_EQUAL(0, target_table->get(7).get_backlink(*origin_table, 0, 0));
    CHECK_EQUAL(1, target_table->get(13).get_backlink(*origin_table, 0, 0));

    // Nullify links
    source_row_1.nullify_link(0);
    source_row_2.nullify_link(0);
    CHECK(source_row_1.is_null_link(0));
    CHECK(source_row_2.is_null_link(0));
    CHECK_EQUAL(0, target_table->get(7).get_backlink_count(*origin_table, 0));
    CHECK_EQUAL(0, target_table->get(13).get_backlink_count(*origin_table, 0));

    // Add stuff to link lists
    LinkViewRef link_list_1 = source_row_1.get_linklist(1);
    LinkViewRef link_list_2 = source_row_2.get_linklist(1);
    link_list_1->add(15);
    link_list_2->add(11);
    link_list_2->add(15);
    CHECK(!source_row_1.linklist_is_empty(1));
    CHECK(!source_row_2.linklist_is_empty(1));
    CHECK_EQUAL(1, source_row_1.get_link_count(1));
    CHECK_EQUAL(2, source_row_2.get_link_count(1));
    CHECK_EQUAL(1, target_table->get(11).get_backlink_count(*origin_table, 1));
    CHECK_EQUAL(2, target_table->get(15).get_backlink_count(*origin_table, 1));
    CHECK_EQUAL(1, target_table->get(11).get_backlink(*origin_table, 1, 0));
    size_t back_link_1 = target_table->get(15).get_backlink(*origin_table, 1, 0);
    size_t back_link_2 = target_table->get(15).get_backlink(*origin_table, 1, 1);
    CHECK((back_link_1 == 0 && back_link_2 == 1) || (back_link_1 == 1 && back_link_2 == 0));

    // Clear link lists
    link_list_1->clear();
    link_list_2->clear();
    CHECK(source_row_1.linklist_is_empty(1));
    CHECK(source_row_2.linklist_is_empty(1));
    CHECK_EQUAL(0, source_row_1.get_link_count(1));
    CHECK_EQUAL(0, source_row_2.get_link_count(1));
    CHECK_EQUAL(0, target_table->get(11).get_backlink_count(*origin_table, 1));
    CHECK_EQUAL(0, target_table->get(15).get_backlink_count(*origin_table, 1));
}


TEST(Table_RowAccessorDetach)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    Row row = table[0];
    CHECK(row.is_attached());
    row.detach();
    CHECK(!row.is_attached());
    row = table[0];
    CHECK(row.is_attached());
}


TEST(Table_RowAccessorCopyAndAssign)
{
    Table table;
    const Table& ctable = table;
    table.add_column(type_Int, "");
    table.add_empty_row(3);
    table.set_int(0, 0, 750);
    table.set_int(0, 1, 751);
    table.set_int(0, 2, 752);

    {
        // Check copy construction of row accessor from row expression
        Row       row_1 =  table[0]; // Copy construct `Row` from `RowExpr`
        ConstRow crow_1 =  table[1]; // Copy construct `ConstRow` from `RowExpr`
        ConstRow crow_2 = ctable[2]; // Copy construct `ConstRow` from `ConstRowExpr`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table,  row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0,  row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());

        // Check copy construction of row accessor from other row accessor
        Row drow_1;
        ConstRow dcrow_1;
        CHECK(!drow_1.is_attached());
        CHECK(!dcrow_1.is_attached());
        Row      drow_2  = drow_1;  // Copy construct `Row` from detached `Row`
        ConstRow dcrow_2 = drow_1;  // Copy construct `ConstRow` from detached `Row`
        ConstRow dcrow_3 = dcrow_1; // Copy construct `ConstRow` from detached `ConstRow`
        Row      row_2   = row_1;   // Copy construct `Row` from attached `Row`
        ConstRow crow_3  = row_1;   // Copy construct `ConstRow` from attached `Row`
        ConstRow crow_4  = crow_1;  // Copy construct `ConstRow` from attached `ConstRow`
        CHECK(!drow_2.is_attached());
        CHECK(!dcrow_2.is_attached());
        CHECK(!dcrow_3.is_attached());
        CHECK(row_2.is_attached());
        CHECK(crow_3.is_attached());
        CHECK(crow_4.is_attached());
        CHECK(!drow_2.get_table());
        CHECK(!dcrow_2.get_table());
        CHECK(!dcrow_3.get_table());
        CHECK_EQUAL(&table, row_2.get_table());
        CHECK_EQUAL(&table, crow_3.get_table());
        CHECK_EQUAL(&table, crow_4.get_table());
        CHECK_EQUAL(0, row_2.get_index());
        CHECK_EQUAL(0, crow_3.get_index());
        CHECK_EQUAL(1, crow_4.get_index());
    }
    table.verify();

    // Check assignment of row expression to row accessor
    {
        Row row;
        ConstRow crow_1, crow_2;
        row    =  table[0]; // Assign `RowExpr` to detached `Row`
        crow_1 =  table[1]; // Assign `RowExpr` to detached `ConstRow`
        crow_2 = ctable[2]; // Assign `ConstRowExpr` to detached `ConstRow`
        CHECK(row.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table,  row.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0,  row.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());
        row    =  table[1]; // Assign `RowExpr` to attached `Row`
        crow_1 =  table[2]; // Assign `RowExpr` to attached `ConstRow`
        crow_2 = ctable[0]; // Assign `ConstRowExpr` to attached `ConstRow`
        CHECK(row.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table,  row.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(1,  row.get_index());
        CHECK_EQUAL(2, crow_1.get_index());
        CHECK_EQUAL(0, crow_2.get_index());
    }

    // Check assignment of row accessor to row accessor
    {
        Row drow, row_1;
        ConstRow dcrow, crow_1, crow_2;
        row_1  = row_1;  // Assign detached `Row` to self
        crow_1 = crow_1; // Assign detached `ConstRow` to self
        CHECK(!row_1.is_attached());
        CHECK(!crow_1.is_attached());
        row_1  = drow;  // Assign detached `Row` to detached `Row`
        crow_1 = drow;  // Assign detached `Row` to detached `ConstRow`
        crow_2 = dcrow; // Assign detached `ConstRow` to detached `ConstRow`
        CHECK(!row_1.is_attached());
        CHECK(!crow_1.is_attached());
        CHECK(!crow_2.is_attached());
        Row       row_2 = table[0];
        Row       row_3 = table[1];
        ConstRow crow_3 = table[2];
        CHECK(row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK(crow_3.is_attached());
        CHECK_EQUAL(&table,  row_2.get_table());
        CHECK_EQUAL(&table,  row_3.get_table());
        CHECK_EQUAL(&table, crow_3.get_table());
        CHECK_EQUAL(0,  row_2.get_index());
        CHECK_EQUAL(1,  row_3.get_index());
        CHECK_EQUAL(2, crow_3.get_index());
        row_1  =  row_2; // Assign attached `Row` to detached `Row`
        crow_1 =  row_3; // Assign attached `Row` to detached `ConstRow`
        crow_2 = crow_3; // Assign attached `ConstRow` to detached `ConstRow`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table,  row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0,  row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());
        row_1  = row_1;  // Assign attached `Row` to self
        crow_1 = crow_1; // Assign attached `ConstRow` to self
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK_EQUAL(&table,  row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(0,  row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        Row       row_4 = table[2];
        Row       row_5 = table[0];
        ConstRow crow_4 = table[1];
        row_1  =  row_4; // Assign attached `Row` to attached `Row`
        crow_1 =  row_5; // Assign attached `Row` to attached `ConstRow`
        crow_2 = crow_4; // Assign attached `ConstRow` to attached `ConstRow`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table,  row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(2,  row_1.get_index());
        CHECK_EQUAL(0, crow_1.get_index());
        CHECK_EQUAL(1, crow_2.get_index());
        row_1  = drow;  // Assign detached `Row` to attached `Row`
        crow_1 = drow;  // Assign detached `Row` to attached `ConstRow`
        crow_2 = dcrow; // Assign detached `ConstRow` to attached `ConstRow`
        CHECK(!row_1.is_attached());
        CHECK(!crow_1.is_attached());
        CHECK(!crow_2.is_attached());
    }
}

TEST(Table_RowAccessorCopyConstructionBug)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();

    BasicRowExpr<Table> row_expr(table[0]);
    BasicRow<Table> row_from_expr(row_expr);
    BasicRow<Table> row_copy(row_from_expr);

    table.remove(0);

    CHECK_NOT(row_from_expr.is_attached());
    CHECK_NOT(row_copy.is_attached());
}

TEST(Table_RowAccessorAssignMultipleTables)
{
    Table tables[2];
    for (int i = 0; i < 2; ++i) {
        tables[i].add_column(type_Int, "");
        tables[i].add_empty_row(3);
        tables[i].set_int(0, 0, 750);
        tables[i].set_int(0, 1, 751);
        tables[i].set_int(0, 2, 752);
    }

    Row row_1 = tables[0][2];
    Row row_2 = tables[1][2];
    Row row_3 = tables[0][2];
    row_1 = tables[1][2]; // Assign attached `Row` to a different table via RowExpr

    // Veriy that the correct accessors are updated when removing from a table
    tables[0].remove(0);
    CHECK_EQUAL(row_1.get_index(), 2);
    CHECK_EQUAL(row_2.get_index(), 2);
    CHECK_EQUAL(row_3.get_index(), 1);

    row_1 = row_3; // Assign attached `Row` to a different table via Row

    // Veriy that the correct accessors are updated when removing from a table
    tables[0].remove(0);
    CHECK_EQUAL(row_1.get_index(), 0);
    CHECK_EQUAL(row_2.get_index(), 2);
    CHECK_EQUAL(row_3.get_index(), 0);
}

TEST(Table_RowAccessorRetain)
{
    // Create a table with two rows
    TableRef parent = Table::create();
    parent->add_column(type_Int, "a");
    parent->add_empty_row(2);
    parent->set_int(0, 0, 27);
    parent->set_int(0, 1, 227);
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    ConstRow row_1 = (*parent)[0];
    ConstRow row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());

    // Check that row insertion does not detach the row accessors, and that the
    // row indexes is properly adjusted
    parent->insert_empty_row(1); // Between
    parent->add_empty_row();     // After
    parent->insert_empty_row(0); // Before
    parent->verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(1, row_1.get_index());
    CHECK_EQUAL(3, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));
    parent->insert_empty_row(1); // Immediately before row_1
    parent->insert_empty_row(5); // Immediately after  row_2
    parent->insert_empty_row(3); // Immediately after  row_1
    parent->insert_empty_row(5); // Immediately before row_2
    parent->verify();
    CHECK_EQUAL(9, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(2, row_1.get_index());
    CHECK_EQUAL(6, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of rows (other than row_1 and row_2) does not detach
    // the row accessors, and that the row indexes is properly adjusted
    parent->remove(3); // Immediately after  row_1
    parent->remove(1); // Immediately before row_1
    parent->remove(3); // Immediately before row_2
    parent->remove(4); // Immediately after  row_2
    parent->verify();
    CHECK_EQUAL(5, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(1, row_1.get_index());
    CHECK_EQUAL(3, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));
    parent->remove(4); // After
    parent->remove(0); // Before
    parent->remove(1); // Between
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of first row detaches row_1
    parent->remove(0);
    parent->verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_2.get_index());
    CHECK_EQUAL(227, row_2.get_int(0));
    // Restore first row and recover row_1
    parent->insert_empty_row(0);
    parent->set_int(0, 0, 27);
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of second row detaches row_2
    parent->remove(1);
    parent->verify();
    CHECK_EQUAL(1, parent->size());
    CHECK(row_1.is_attached());
    CHECK(!row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(27, row_1.get_int(0));
    // Restore second row and recover row_2
    parent->add_empty_row();
    parent->set_int(0, 1, 227);
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that descriptor modifications do not affect the row accessors (as
    // long as we do not remove the last column)
    parent->add_column(type_String, "x");
    parent->insert_column(0, type_Float, "y");
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(1));
    CHECK_EQUAL(227, row_2.get_int(1));
    parent->remove_column(0);
    parent->remove_column(1);
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());
    CHECK_EQUAL(27,  row_1.get_int(0));
    CHECK_EQUAL(227, row_2.get_int(0));

    // Check that removal of the last column detaches all row accessors
    parent->remove_column(0);
    parent->verify();
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(!row_2.is_attached());
    // Restore rows and recover row accessors
    parent->add_column(type_Int, "a");
    parent->add_empty_row(2);
    parent->set_int(0, 0, 27);
    parent->set_int(0, 1, 227);
    parent->verify();
    CHECK_EQUAL(2, parent->size());
    row_1 = (*parent)[0];
    row_2 = (*parent)[1];
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(parent.get(), row_1.get_table());
    CHECK_EQUAL(parent.get(), row_2.get_table());
    CHECK_EQUAL(0, row_1.get_index());
    CHECK_EQUAL(1, row_2.get_index());

    // Check that clearing of the table detaches all row accessors
    parent->clear();
    parent->verify();
    CHECK_EQUAL(0, parent->size());
    CHECK(!row_1.is_attached());
    CHECK(!row_2.is_attached());
}


TEST(Table_SubtableRowAccessorsRetain)
{
    // Create a mixed and a regular subtable each with one row
    TableRef parent = Table::create();
    parent->add_column(type_Mixed, "a");
    parent->add_column(type_Table, "b");
    DescriptorRef subdesc = parent->get_subdescriptor(1);
    subdesc->add_column(type_Int, "regular");
    parent->add_empty_row();
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    TableRef mixed = parent->get_subtable(0,0);
    CHECK(mixed && mixed->is_attached());
    mixed->add_column(type_Int, "mixed");
    mixed->add_empty_row();
    mixed->set_int(0, 0, 19);
    TableRef regular = parent->get_subtable(1,0);
    CHECK(regular && regular->is_attached());
    regular->add_empty_row();
    regular->set_int(0, 0, 29);
    CHECK(mixed->size()   == 1);
    CHECK(regular->size() == 1);
    ConstRow row_m = (*mixed)[0];
    ConstRow row_r = (*regular)[0];
    CHECK_EQUAL(19, row_m.get_int(0));
    CHECK_EQUAL(29, row_r.get_int(0));

    // Check that all row accessors in a mixed subtable are detached if the
    // subtable is overridden
    parent->set_mixed(0, 0, Mixed("foo"));
    CHECK(!mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK(!row_m.is_attached());
    CHECK(row_r.is_attached());
    // Restore mixed
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    mixed = parent->get_subtable(0,0);
    CHECK(mixed);
    CHECK(mixed->is_attached());
    mixed->add_column(type_Int, "mixed_2");
    mixed->add_empty_row();
    mixed->set_int(0, 0, 19);
    CHECK(regular->is_attached());
    CHECK_EQUAL(1, mixed->size());
    CHECK_EQUAL(1, regular->size());
    row_m = (*mixed)[0];
    CHECK_EQUAL(19, row_m.get_int(0));
    CHECK_EQUAL(29, row_r.get_int(0));

    // Check that all row accessors in a regular subtable are detached if the
    // subtable is overridden
    parent->set_subtable(1, 0, 0); // Clear
    CHECK(mixed->is_attached());
    CHECK(regular->is_attached());
    CHECK(row_m.is_attached());
    CHECK(!row_r.is_attached());
}


TEST(Table_MoveLastOverRetain)
{
    // Create three parent tables, each with with 5 rows, and each row
    // containing one regular and one mixed subtable
    TableRef parent_1, parent_2, parent_3;
    for (int i = 0; i < 3; ++i) {
        TableRef& parent = i == 0 ? parent_1 : i == 1 ? parent_2 : parent_3;
        parent = Table::create();
        parent->add_column(type_Table, "a");
        parent->add_column(type_Mixed, "b");
        DescriptorRef subdesc = parent->get_subdescriptor(0);
        subdesc->add_column(type_Int, "regular");
        parent->add_empty_row(5);
        for (int row_ndx = 0; row_ndx < 5; ++row_ndx) {
            TableRef regular = parent->get_subtable(0, row_ndx);
            regular->add_empty_row();
            regular->set_int(0, 0, 10 + row_ndx);
            parent->set_mixed(1, row_ndx, Mixed::subtable_tag());
            TableRef mixed = parent->get_subtable(1, row_ndx);
            mixed->add_column(type_Int, "mixed");
            mixed->add_empty_row();
            mixed->set_int(0, 0, 20 + row_ndx);
        }
    }

    // Use first table to check with accessors on row indexes 0, 1, and 4, but
    // none at index 2 and 3.
    {
        TableRef parent = parent_1;
        ConstRow row_0 = (*parent)[0];
        ConstRow row_1 = (*parent)[1];
        ConstRow row_4 = (*parent)[4];
        TableRef regular_0 = parent->get_subtable(0,0);
        TableRef regular_1 = parent->get_subtable(0,1);
        TableRef regular_4 = parent->get_subtable(0,4);
        TableRef   mixed_0 = parent->get_subtable(1,0);
        TableRef   mixed_1 = parent->get_subtable(1,1);
        TableRef   mixed_4 = parent->get_subtable(1,4);
        CHECK(row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(4, row_4.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_4 && mixed_4->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        parent->move_last_over(2); // Move row at index 4 to index 2
        parent->move_last_over(0); // Move row at index 3 to index 0
        CHECK(!row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(2, row_4.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0,1));
        CHECK_EQUAL(regular_4, parent->get_subtable(0,2));
        CHECK(!mixed_0->is_attached());
        CHECK(mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(24, mixed_4->get_int(0,0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1,1));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1,2));

        // Perform two more 'move last over' operations which brings the number
        // of rows down from 3 to 1
        parent->move_last_over(1); // Move row at index 2 to index 1
        parent->move_last_over(0); // Move row at index 1 to index 0
        CHECK(!row_0.is_attached());
        CHECK(!row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_4.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(14, regular_4->get_int(0,0));
        CHECK_EQUAL(regular_4, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(24, mixed_4->get_int(0,0));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1,0));
    }

    // Use second table to check with accessors on row indexes 0, 2, and 3, but
    // none at index 1 and 4.
    {
        TableRef parent = parent_2;
        ConstRow row_0 = (*parent)[0];
        ConstRow row_2 = (*parent)[2];
        ConstRow row_3 = (*parent)[3];
        TableRef regular_0 = parent->get_subtable(0,0);
        TableRef regular_2 = parent->get_subtable(0,2);
        TableRef regular_3 = parent->get_subtable(0,3);
        TableRef   mixed_0 = parent->get_subtable(1,0);
        TableRef   mixed_2 = parent->get_subtable(1,2);
        TableRef   mixed_3 = parent->get_subtable(1,3);
        CHECK(row_0.is_attached());
        CHECK(row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(2, row_2.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0,0));
        CHECK_EQUAL(12, regular_2->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_2 && mixed_2->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0,0));
        CHECK_EQUAL(22, mixed_2->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        parent->move_last_over(2); // Move row at index 4 to index 2
        parent->move_last_over(0); // Move row at index 3 to index 0
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one more 'move last over' operation which brings the number
        // of rows down from 3 to 2
        parent->move_last_over(1); // Move row at index 2 to index 1
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one final 'move last over' operation which brings the number
        // of rows down from 2 to 1
        parent->move_last_over(0); // Move row at index 1 to index 0
        CHECK(!row_0.is_attached());
        CHECK(!row_2.is_attached());
        CHECK(!row_3.is_attached());
        CHECK(!regular_0->is_attached());
        CHECK(!regular_2->is_attached());
        CHECK(!regular_3->is_attached());
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(!mixed_3->is_attached());
    }

    // Use third table to check with accessors on row indexes 1 and 3, but none
    // at index 0, 2, and 4.
    {
        TableRef parent = parent_3;
        ConstRow row_1 = (*parent)[1];
        ConstRow row_3 = (*parent)[3];
        TableRef regular_1 = parent->get_subtable(0,1);
        TableRef regular_3 = parent->get_subtable(0,3);
        TableRef   mixed_1 = parent->get_subtable(1,1);
        TableRef   mixed_3 = parent->get_subtable(1,3);
        CHECK(row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));

        // Perform two 'move last over' operations which brings the number of
        // rows down from 5 to 3
        parent->move_last_over(2); // Move row at index 4 to index 2
        parent->move_last_over(0); // Move row at index 3 to index 0
        CHECK(row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0,0));
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0,1));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0,0));
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1,1));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one more 'move last over' operation which brings the number
        // of rows down from 3 to 2
        parent->move_last_over(1); // Move row at index 2 to index 1
        CHECK(!row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0,0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0,0));
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0,0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1,0));

        // Perform one final 'move last over' operation which brings the number
        // of rows down from 2 to 1
        parent->move_last_over(0); // Move row at index 1 to index 0
        CHECK(!row_1.is_attached());
        CHECK(!row_3.is_attached());
        CHECK(!regular_1->is_attached());
        CHECK(!regular_3->is_attached());
        CHECK(!mixed_1->is_attached());
        CHECK(!mixed_3->is_attached());
    }
}


TEST(Table_EnumStringInsertEmptyRow)
{
    Table table;
    table.add_column(type_String, "");
    table.add_empty_row(128);
    for (int i = 0; i < 128; ++i)
        table.set_string(0, i, "foo");
    DescriptorRef desc = table.get_descriptor();
    CHECK_EQUAL(0, desc->get_num_unique_values(0));
    table.optimize();
    // Make sure we now have an enumerated strings column
    CHECK_EQUAL(1, desc->get_num_unique_values(0));
    table.add_empty_row();
    CHECK_EQUAL("", table.get_string(0, 128));
}


TEST(Table_AddColumnWithThreeLevelBptree)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row(REALM_MAX_BPNODE_SIZE*REALM_MAX_BPNODE_SIZE+1);
    table.add_column(type_Int, "");
    table.verify();
}


TEST(Table_ClearWithTwoLevelBptree)
{
    Table table;
    table.add_column(type_Mixed, "");
    table.add_empty_row(REALM_MAX_BPNODE_SIZE+1);
    table.clear();
    table.verify();
}


TEST(Table_IndexStringDelete)
{
    Table t;
    t.add_column(type_String, "str");
    t.add_search_index(0);

    std::ostringstream out;

    for (size_t i = 0; i < 1000; ++i) {
        t.add_empty_row();
        out.str(std::string());
        out << i;
        t.set_string(0, i, out.str());
    }

    t.clear();

    for (size_t i = 0; i < 1000; ++i) {
        t.add_empty_row();
        out.str(std::string());
        out << i;
        t.set_string(0, i, out.str());
    }
}

#if REALM_NULL_STRINGS == 1
TEST(Table_Nulls)
{
    // 'round' lets us run this entire test both with and without index and with/without optimize/enum
    for (size_t round = 0; round < 5; round++) {
        Table t;
        TableView tv;
        t.add_column(type_String, "str", true /*nullable*/ );

        if (round == 1)
            t.add_search_index(0);
        else if (round == 2)
            t.optimize(true);
        else if (round == 3) {
            t.add_search_index(0);
            t.optimize(true);
        }
        else if (round == 4) {
            t.optimize(true);
            t.add_search_index(0);
        }

        t.add_empty_row(3);
        t.set_string(0, 0, "foo"); // short strings
        t.set_string(0, 1, "");
        t.set_string(0, 2, realm::null());

        CHECK_EQUAL(1, t.count_string(0, "foo"));
        CHECK_EQUAL(1, t.count_string(0, ""));
        CHECK_EQUAL(1, t.count_string(0, realm::null()));

        CHECK_EQUAL(0, t.find_first_string(0, "foo"));
        CHECK_EQUAL(1, t.find_first_string(0, ""));
        CHECK_EQUAL(2, t.find_first_string(0, realm::null()));

        tv = t.find_all_string(0, "foo");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(0, tv.get_source_ndx(0));
        tv = t.find_all_string(0, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(1, tv.get_source_ndx(0));
        tv = t.find_all_string(0, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(2, tv.get_source_ndx(0));

        t.set_string(0, 0, "xxxxxxxxxxYYYYYYYYYY"); // medium strings (< 64)

        CHECK_EQUAL(1, t.count_string(0, "xxxxxxxxxxYYYYYYYYYY"));
        CHECK_EQUAL(1, t.count_string(0, ""));
        CHECK_EQUAL(1, t.count_string(0, realm::null()));

        CHECK_EQUAL(0, t.find_first_string(0, "xxxxxxxxxxYYYYYYYYYY"));
        CHECK_EQUAL(1, t.find_first_string(0, ""));
        CHECK_EQUAL(2, t.find_first_string(0, realm::null()));

        tv = t.find_all_string(0, "xxxxxxxxxxYYYYYYYYYY");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(0, tv.get_source_ndx(0));
        tv = t.find_all_string(0, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(1, tv.get_source_ndx(0));
        tv = t.find_all_string(0, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(2, tv.get_source_ndx(0));


        // long strings (>= 64)
        t.set_string(0, 0, "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx");

        CHECK_EQUAL(1, t.count_string(0,
            "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx"));
        CHECK_EQUAL(1, t.count_string(0, ""));
        CHECK_EQUAL(1, t.count_string(0, realm::null()));

        CHECK_EQUAL(0, t.find_first_string(0,
            "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx"));
        CHECK_EQUAL(1, t.find_first_string(0, ""));
        CHECK_EQUAL(2, t.find_first_string(0, realm::null()));

        tv = t.find_all_string(0, "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(0, tv.get_source_ndx(0));
        tv = t.find_all_string(0, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(1, tv.get_source_ndx(0));
        tv = t.find_all_string(0, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(2, tv.get_source_ndx(0));
    }

    {
        Table t;
        t.add_column(type_Int, "int", true);        // nullable = true
        t.add_column(type_Bool, "bool", true);      // nullable = true
        t.add_column(type_DateTime, "bool", true);  // nullable = true

        t.add_empty_row(2);

        t.set_int(0, 0, 65);
        t.set_bool(1, 0, false);
        t.set_datetime(2, 0, DateTime(3));

        CHECK_EQUAL(65, t.get_int(0, 0));
        CHECK_EQUAL(false, t.get_bool(1, 0));
        CHECK_EQUAL(DateTime(3), t.get_datetime(2, 0));

        CHECK_EQUAL(65, t.maximum_int(0));
        CHECK_EQUAL(65, t.minimum_int(0));
        CHECK_EQUAL(DateTime(3), t.maximum_datetime(2));
        CHECK_EQUAL(DateTime(3), t.minimum_datetime(2));

        CHECK(!t.is_null(0, 0));
        CHECK(!t.is_null(1, 0));
        CHECK(!t.is_null(2, 0));

        CHECK(t.is_null(0, 1));
        CHECK(t.is_null(1, 1));
        CHECK(t.is_null(2, 1));

        CHECK_EQUAL(1, t.find_first_null(0));
        CHECK_EQUAL(1, t.find_first_null(1));
        CHECK_EQUAL(1, t.find_first_null(2));

        CHECK_EQUAL(not_found, t.find_first_int(0, -1));
        CHECK_EQUAL(not_found, t.find_first_bool(1, true));
        CHECK_EQUAL(not_found, t.find_first_datetime(2, DateTime(5)));

        CHECK_EQUAL(0, t.find_first_int(0, 65));
        CHECK_EQUAL(0, t.find_first_bool(1, false));
        CHECK_EQUAL(0, t.find_first_datetime(2, DateTime(3)));

        t.set_null(0, 0);
        t.set_null(1, 0);
        t.set_null(2, 0);

        CHECK(t.is_null(0, 0));
        CHECK(t.is_null(1, 0));
        CHECK(t.is_null(2, 0));
    }
    {
        Table t;
        t.add_column(type_Float, "float", true);        // nullable = true
        t.add_column(type_Double, "double", true);      // nullable = true

        t.add_empty_row(2);

        t.set_float(0, 0, 1.23f);
        t.set_double(1, 0, 12.3);

        CHECK_EQUAL(1.23f, t.get_float(0, 0));
        CHECK_EQUAL(12.3, t.get_double(1, 0));

        CHECK_EQUAL(1.23f, t.maximum_float(0));
        CHECK_EQUAL(1.23f, t.minimum_float(0));
        CHECK_EQUAL(12.3, t.maximum_double(1));
        CHECK_EQUAL(12.3, t.minimum_double(1));

        CHECK(!t.is_null(0, 0));
        CHECK(!t.is_null(1, 0));

        CHECK(t.is_null(0, 1));
        CHECK(t.is_null(1, 1));

        CHECK_EQUAL(1, t.find_first_null(0));
        CHECK_EQUAL(1, t.find_first_null(1));

        CHECK_EQUAL(not_found, t.find_first_float(0, 2.22f));
        CHECK_EQUAL(not_found, t.find_first_double(1, 2.22));

        CHECK_EQUAL(0, t.find_first_float(0, 1.23f));
        CHECK_EQUAL(0, t.find_first_double(1, 12.3));

        t.set_null(0, 0);
        t.set_null(1, 0);

        CHECK(t.is_null(0, 0));
        CHECK(t.is_null(1, 0));
    }
}
#endif

TEST(Table_RowAccessor_Null)
{
    Table table;
    size_t col_bool   = table.add_column(type_Bool,     "bool",   true);
    size_t col_int    = table.add_column(type_Int,      "int",    true);
    size_t col_string = table.add_column(type_String,   "string", true);
    size_t col_float  = table.add_column(type_Float,    "float",  true);
    size_t col_double = table.add_column(type_Double,   "double", true);    
    size_t col_date   = table.add_column(type_DateTime, "date",   true);
    size_t col_binary = table.add_column(type_Binary,   "binary", true);

    {
        table.add_empty_row();
        Row row = table[0];
        row.set_null(col_bool);
        row.set_null(col_int);
        row.set_string(col_string, realm::null());
        row.set_null(col_float);
        row.set_null(col_double);
        row.set_null(col_date);
        row.set_binary(col_binary, BinaryData());
    }
    {
        table.add_empty_row();
        Row row = table[1];
        row.set_bool(col_bool, true);
        row.set_int(col_int, 1);
        row.set_string(col_string, "1");
        row.set_float(col_float, 1.0);
        row.set_double(col_double, 1.0);
        row.set_datetime(col_date, DateTime(1));
        row.set_binary(col_binary, BinaryData("a"));
    }

    {
        Row row = table[0];
        CHECK(row.is_null(col_bool));
        CHECK(row.is_null(col_int));
        CHECK(row.is_null(col_string));
        CHECK(row.is_null(col_float));
        CHECK(row.is_null(col_double));
        CHECK(row.is_null(col_date));
        CHECK(row.is_null(col_binary));
    }

    {
        Row row = table[1];
        CHECK_EQUAL(true,            row.get_bool(col_bool));
        CHECK_EQUAL(1,               row.get_int(col_int));
        CHECK_EQUAL("1",             row.get_string(col_string));
        CHECK_EQUAL(1.0,             row.get_float(col_float));
        CHECK_EQUAL(1.0,             row.get_double(col_double));
        CHECK_EQUAL(DateTime(1),     row.get_datetime(col_date));
        CHECK_EQUAL(BinaryData("a"), row.get_binary(col_binary));
    }
}


// This triggers a severe bug in the Array::alloc() allocator in which its capacity-doubling
// scheme forgets to test of the doubling has overflowed the maximum allowed size of an
// array which is 2^20 - 1 bytes
TEST(Table_AllocatorCapacityBug)
{
    char* buf = new char[20000000];

    // First a simple trigger of `Assertion failed: value <= 0xFFFFFL [26000016, 16777215]`
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default());
        BinaryColumn c(Allocator::get_default(), ref, true);

        c.add(BinaryData(buf, 13000000));
        c.set(0, BinaryData(buf, 14000000));
    }

    // Now a small fuzzy test to catch other such bugs
    {
        Table t;
        t.add_column(type_Binary, "", true);

        for (size_t j = 0; j < 100; j++) {
            size_t r = (j * 123456789 + 123456789) % 100;
            if (r < 20) {
                t.add_empty_row();
            }
            else if (t.size() > 0 && t.size() < 5) {
                // Set only if there are no more than 4 rows, else it takes up too much space on devices (4 * 16 MB 
                // worst case now)
                size_t row = (j * 123456789 + 123456789) % t.size();
                size_t len = (j * 123456789 + 123456789) % 16000000;
                BinaryData bd;
                bd = BinaryData(buf, len);
                t.set_binary(0, row, bd);
            }
            else if (t.size() >= 4) {
                t.clear();
            }
        }
    }
    delete buf;  
}



#endif // TEST_TABLE
