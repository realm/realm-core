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
#ifdef TEST_TABLE

#include <algorithm>
#include <limits>
#include <string>
#include <fstream>
#include <ostream>

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/to_string.hpp>

#include "util/misc.hpp"

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;


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


#ifdef JAVA_MANY_COLUMNS_CRASH

REALM_TABLE_3(SubtableType, year, Int, daysSinceLastVisit, Int, conceptId, String)

REALM_TABLE_7(MainTableType, patientId, String, gender, Int, ethnicity, Int, yearOfBirth, Int, yearOfDeath, Int,
              zipCode, String, events, Subtable<SubtableType>)

TEST(Table_ManyColumnsCrash2)
{
    // Trying to reproduce Java crash.
    for (int a = 0; a < 10; a++) {
        Group group;

        MainTableType::Ref mainTable = group.add_table<MainTableType>("PatientTable");
        TableRef dynPatientTable = group.add_table("PatientTable");
        dynPatientTable->add_empty_row();

        for (int counter = 0; counter < 20000; counter++) {
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
            if ((counter % 1000) == 0) {
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
        CHECK(!table->is_nullable(0));

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
        CHECK(table->is_nullable(0));
        table->add_empty_row();
        CHECK(table->is_null(0, 0));
    }

    {
        // Check that add_empty_row() adds 0 integer as default.
        Group group;
        TableRef table = group.add_table("test");
        table->add_column(type_Int, "name");
        CHECK(!table->is_nullable(0));
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
        CHECK(table->is_nullable(0));

        table->add_empty_row();
        CHECK(table->get_binary(0, 0).is_null());
    }

    {
        // Check that add_empty_row() adds empty binary as default
        Group group;
        TableRef table = group.add_table("test");

        table->add_column(type_Binary, "name");
        CHECK(!table->is_nullable(0));

        table->add_empty_row();
        CHECK(!table->get_binary(0, 0).is_null());

        // Test that inserting null in non-nullable column will throw
        CHECK_THROW_ANY(table->set_binary(0, 0, BinaryData()));
    }

    {
        // Check that link columns are nullable.
        Group group;
        TableRef target = group.add_table("target");
        TableRef table = group.add_table("table");

        target->add_column(type_Int, "int");
        table->add_column_link(type_Link, "link", *target);
        CHECK(table->is_nullable(0));
        CHECK(!target->is_nullable(0));
    }

    {
        // Check that linklist columns are not nullable.
        Group group;
        TableRef target = group.add_table("target");
        TableRef table = group.add_table("table");

        target->add_column(type_Int, "int");
        table->add_column_link(type_LinkList, "link", *target);
        CHECK(!table->is_nullable(0));
        CHECK(!target->is_nullable(0));
    }
}

TEST(Table_DeleteCrash)
{
    Group group;
    TableRef table = group.add_table("test");

    table->add_column(type_String, "name");
    table->add_column(type_Int, "age");

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
    Table ttt;
    ttt.add_column(type_Int, "first");
    ttt.add_column(type_String, "second");
    ttt.optimize();
    ttt.add_search_index(1);
    ttt.clear();
    ttt.add_empty_row(1);
    ttt.set_int(0, 0, 1);
    ttt.set_string(1, 0, "AA");
}

TEST(Table_DateTimeMinMax)
{
    Group g;
    TableRef table = g.add_table("test_table");

    table->insert_column(0, type_Timestamp, "time", true);

    // We test different code paths of the internal Core minmax method. First a null value as initial "best candidate",
    // then non-null first. For each case we then try both a substitution of best candidate, then non-substitution. 4
    // permutations in total.
    
    table->add_empty_row(3);
    table->set_null(0, 0);
    table->set_timestamp(0, 1, {0, 0});
    table->set_timestamp(0, 2, {2, 2});

    CHECK_EQUAL(table->maximum_timestamp(0), Timestamp(2, 2));
    CHECK_EQUAL(table->minimum_timestamp(0), Timestamp(0, 0));

    table->clear();
    table->insert_column(0, type_Timestamp, "time", true);
    table->add_empty_row(3);
    table->set_null(0, 0);
    table->set_timestamp(0, 1, {0, 0});
    table->set_timestamp(0, 2, {2, 2});

    size_t idx; // tableview entry that points at the max/min value

    CHECK_EQUAL(table->maximum_timestamp(0, &idx), Timestamp(2, 2));
    CHECK_EQUAL(idx, 2);
    CHECK_EQUAL(table->minimum_timestamp(0, &idx), Timestamp(0, 0));
    CHECK_EQUAL(idx, 1);

    table->clear();
    table->insert_column(0, type_Timestamp, "time", true);
    table->add_empty_row(3);
    table->set_null(0, 0);
    table->set_timestamp(0, 1, {0, 0});
    table->set_timestamp(0, 2, {2, 2});

    CHECK_EQUAL(table->maximum_timestamp(0), Timestamp(2, 2));
    CHECK_EQUAL(table->minimum_timestamp(0), Timestamp(0, 0));

    table->clear();
    table->insert_column(0, type_Timestamp, "time", true);
    table->add_empty_row(3);
    table->set_null(0, 0);
    table->set_timestamp(0, 1, {0, 0});
    table->set_timestamp(0, 2, {2, 2});

    CHECK_EQUAL(table->maximum_timestamp(0, &idx), Timestamp(2, 2));
    CHECK_EQUAL(idx, 2);
    CHECK_EQUAL(table->minimum_timestamp(0, &idx), Timestamp(0, 0));
    CHECK_EQUAL(idx, 1);
}

TEST(Table_MinMaxSingleNullRow)
{
    // To illustrate/document behaviour
    Group g;
    TableRef table = g.add_table("test_table");

    table->insert_column(0, type_Timestamp, "time", true);
    table->insert_column(1, type_Int, "int", true);
    table->insert_column(2, type_Float, "float", true);
    table->add_empty_row();

    size_t ret;

    // NOTE: Return-values of method calls are undefined if you have only null-entries in the table.
    // The return-value is not necessarily a null-object. Always test the return_ndx argument!

    // Maximum
    {
        table->maximum_timestamp(0, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().maximum_timestamp(0, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().maximum_timestamp(0, &ret); // max on query
        CHECK(ret == npos);

        table->maximum_int(1, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().maximum_int(1, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().maximum_int(1, nullptr, 0, npos, npos, &ret); // max on query
        CHECK(ret == npos);

        table->maximum_float(2, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().maximum_float(2, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().maximum_float(2, nullptr, 0, npos, npos, &ret); // max on query
        CHECK(ret == npos);

        table->add_empty_row();

        CHECK(table->maximum_timestamp(0).is_null()); // max on table
        table.get()->where().find_all().maximum_timestamp(0, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().maximum_timestamp(0, &ret); // max on query
        CHECK(ret == npos);
    }

    // Minimum
    {
        table->minimum_timestamp(0, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().minimum_timestamp(0, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().minimum_timestamp(0, &ret); // max on query
        CHECK(ret == npos);

        table->minimum_int(1, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().minimum_int(1, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().minimum_int(1, nullptr, 0, npos, npos, &ret); // max on query
        CHECK(ret == npos);

        table->minimum_float(2, &ret); // max on table
        CHECK(ret == npos);
        table.get()->where().find_all().minimum_float(2, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().minimum_float(2, nullptr, 0, npos, npos, &ret); // max on query
        CHECK(ret == npos);

        table->add_empty_row();

        CHECK(table->minimum_timestamp(0).is_null()); // max on table
        table.get()->where().find_all().minimum_timestamp(0, &ret); // max on tableview
        CHECK(ret == npos);
        table.get()->where().minimum_timestamp(0, &ret); // max on query
        CHECK(ret == npos);
    }


}

TEST(TableView_AggregateBugs)
{
    // Tests against various aggregate bugs on TableViews: https://github.com/realm/realm-core/pull/2360
    {
        Table table;
        table.add_column(type_Int, "ints", true);
        table.add_empty_row(4);

        table.set_int(0, 0, 1);
        table.set_int(0, 1, 2);
        table.set_null(0, 2);
        table.set_int(0, 3, 42);

        table.add_column(type_Double, "doubles", true);
        table.set_double(1, 0, 1.);
        table.set_double(1, 1, 2.);
        table.set_null(1, 2);
        table.set_double(1, 3, 42.);

        auto tv = table.where().not_equal(0, 42).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_int(0), 2);

        // average == sum / rows, where rows does *not* include values with null.
        size_t vc; // number of non-null values that the average was computed from
        CHECK_APPROXIMATELY_EQUAL(table.average_int(0, &vc), double(1 + 2 + 42) / 3, 0.001);
        CHECK_EQUAL(vc, 3);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_EQUAL(table.average_int(0), table.where().average_int(0, &vc));
        CHECK_EQUAL(vc, 3);
        CHECK_EQUAL(table.average_int(0), table.where().find_all().average_int(0, &vc));
        CHECK_EQUAL(vc, 3);

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(0, 1).find_all().average_int(0, &vc), double(2 + 42) / 2, 0.001);
        CHECK_EQUAL(vc, 2);

        // Add Double column and do same tests on that
        table.add_column(type_Double, "doubles", true);
        table.set_double(1, 0, 1.);
        table.set_double(1, 1, 2.);
        table.set_null(1, 2);
        table.set_double(1, 3, 42.);

        tv = table.where().not_equal(1, 42.).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_double(1), 2.);

        // average == sum / rows, where rows does *not* include values with null.
        CHECK_APPROXIMATELY_EQUAL(table.average_double(1, &vc), double(1. + 2. + 42.) / 3, 0.001);
        CHECK_EQUAL(vc, 3);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_APPROXIMATELY_EQUAL(table.average_double(1), table.where().average_double(1, &vc), 0.001);
        CHECK_EQUAL(vc, 3);

        CHECK_APPROXIMATELY_EQUAL(table.average_double(1), table.where().find_all().average_double(1, &vc), 0.001);
        CHECK_EQUAL(vc, 3);

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(1, 1.).find_all().average_double(1, &vc), (2. + 42.) / 2, 0.001);
        CHECK_EQUAL(vc, 2);
    }

    // Same as above, with null entry first
    {
        Table table;
        table.add_column(type_Int, "value", true);
        table.add_empty_row(4);
        table.set_null(0, 0);
        table.set_int(0, 1, 1);
        table.set_int(0, 2, 2);
        table.set_int(0, 3, 42);

        auto tv = table.where().not_equal(0, 42).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_int(0), 2);

        // average == sum / rows, where rows does *not* include values with null.
        CHECK_APPROXIMATELY_EQUAL(table.average_int(0), double(1 + 2 + 42) / 3, 0.001);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_EQUAL(table.average_int(0), table.where().average_int(0));
        CHECK_EQUAL(table.average_int(0), table.where().find_all().average_int(0));

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(0, 1).find_all().average_int(0), double(2 + 42) / 2, 0.001);
    }
}


TEST(Table_AggregateFuzz)
{
    // Tests sum, avg, min, max on Table, TableView, Query, for types float, Timestamp, int
    for(int iter = 0; iter < 50 + 1000 * TEST_DURATION; iter++)
    {
        Group g;
        TableRef table = g.add_table("test_table");

        table->insert_column(0, type_Timestamp, "time", true);
        table->insert_column(1, type_Int, "int", true);
        table->insert_column(2, type_Float, "float", true);

        size_t rows = size_t(fastrand(10));
        table->add_empty_row(rows);
        int64_t largest = 0;
        int64_t smallest = 0;
        size_t largest_pos = npos;
        size_t smallest_pos = npos;

        double avg = 0;
        int64_t sum = 0;
        size_t nulls = 0;

        // Create some rows with values and some rows with just nulls
        for (size_t t = 0; t < rows; t++) {
            bool null = (fastrand(1) == 0);
            if (!null) {
                int64_t value = fastrand(10);
                sum += value;
                if (largest_pos == npos || value > largest) {
                    largest = value;
                    largest_pos = t;
                }
                if (smallest_pos == npos || value < smallest) {
                    smallest = value;
                    smallest_pos = t;
                }
                table.get()->set_timestamp(0, t, Timestamp(value, 0));
                table.get()->set_int(1, t, value);
                table.get()->set_float(2, t, float(value));
            }
            else {
                nulls++;
            }
        }

        avg = double(sum) / (rows - nulls == 0 ? 1 : rows - nulls);

        size_t ret;
        float f;
        int64_t i;
        Timestamp ts;

        // Test methods on Table
        {
            // Table::max
            ret = 123;
            f = table.get()->maximum_float(2, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, largest_pos));

            ret = 123;
            i = table.get()->maximum_int(1, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, largest_pos));

            ret = 123;
            ts = table.get()->maximum_timestamp(0, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, largest_pos));

            // Table::min
            ret = 123;
            f = table.get()->minimum_float(2, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, smallest_pos));

            ret = 123;
            i = table.get()->minimum_int(1, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, smallest_pos));

            ret = 123;
            ts = table.get()->minimum_timestamp(0, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, smallest_pos));

            // Table::avg
            double d;

            // number of non-null values used in computing the avg or sum
            ret = 123;

            // Table::avg
            d = table.get()->average_float(2, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            ret = 123;
            d = table.get()->average_int(1, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // Table::sum
            d = table.get()->sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table.get()->sum_int(1);
            CHECK_EQUAL(i, sum);
        }

        // Test methods on TableView
        {
            // TableView::max
            ret = 123;
            f = table.get()->where().find_all().maximum_float(2, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, largest_pos));

            ret = 123;
            i = table.get()->where().find_all().maximum_int(1, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, largest_pos));

            ret = 123;
            ts = table.get()->where().find_all().maximum_timestamp(0, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, largest_pos));

            // TableView::min
            ret = 123;
            f = table.get()->where().find_all().minimum_float(2, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, smallest_pos));

            ret = 123;
            i = table.get()->where().find_all().minimum_int(1, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, smallest_pos));

            ret = 123;
            ts = table.get()->where().find_all().minimum_timestamp(0, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, smallest_pos));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            ret = 123;

            // TableView::avg
            d = table.get()->where().find_all().average_float(2, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            ret = 123;
            d = table.get()->where().find_all().average_int(1, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table.get()->where().find_all().sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table.get()->where().find_all().sum_int(1);
            CHECK_EQUAL(i, sum);

        }


        // Test methods on Query
        {
            // TableView::max
            ret = 123;
            f = table.get()->where().maximum_float(2, nullptr, 0, npos, npos, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, largest_pos));

            ret = 123;
            i = table.get()->where().maximum_int(1, nullptr, 0, npos, npos, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, largest_pos));

            ret = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table.get()->where().maximum_timestamp(0, &ret);
            CHECK_EQUAL(ret, largest_pos);
            if (largest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, largest_pos)); 

            // TableView::min
            ret = 123;
            f = table.get()->where().minimum_float(2, nullptr, 0, npos, npos, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(f, table.get()->get_float(2, smallest_pos));

            ret = 123;
            i = table.get()->where().minimum_int(1, nullptr, 0, npos, npos, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(i, table.get()->get_int(1, smallest_pos));

            ret = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table.get()->where().minimum_timestamp(0, &ret);
            CHECK_EQUAL(ret, smallest_pos);
            if (smallest_pos != npos)
                CHECK_EQUAL(ts, table.get()->get_timestamp(0, smallest_pos));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            ret = 123;

            // TableView::avg
            d = table.get()->where().average_float(2, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            ret = 123;
            d = table.get()->where().average_int(1, &ret);
            CHECK_EQUAL(ret, (rows - nulls));
            if (ret != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table.get()->where().sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table.get()->where().sum_int(1);
            CHECK_EQUAL(i, sum);
        }
    }
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
        table.set_int(0, i, 2 * i);
        table.set_int(1, i, 20 * i);
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
    CHECK_LOGIC_ERROR(table->add_column(type_Int, StringData(buf.get(), buf_size)), LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->insert_column(0, type_Int, StringData(buf.get(), buf_size)),
                      LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->add_column_link(type_Link, StringData(buf.get(), buf_size), *table),
                      LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->insert_column_link(0, type_Link, StringData(buf.get(), buf_size), *table),
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
    table.add_column(type_Mixed, "m1");
    table.add_column(type_Mixed, "m2");
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
    table.set_binary_big(1, 0, BinaryData(large_buf.get(), large_bin_size));
    size_t pos = 0;
    table.get_binary_at(1, 0, pos);
    CHECK_EQUAL(pos, 0xFFFFF0);
    table.get_binary_at(1, 0, pos);
    CHECK_EQUAL(pos, 0);
}


TEST(Table_SetBinaryLogicErrors)
{
    Group group;
    TableRef table = group.add_table("table");
    table->add_column(type_Binary, "a");
    table->add_column(type_Int, "b");
    table->add_empty_row();

    BinaryData bd;
    CHECK_LOGIC_ERROR(table->set_binary(2, 0, bd), LogicError::column_index_out_of_range);
    CHECK_LOGIC_ERROR(table->set_binary(0, 1, bd), LogicError::row_index_out_of_range);
    CHECK_LOGIC_ERROR(table->set_null(0, 0), LogicError::column_not_nullable);

    // FIXME: Must also check that Logic::type_mismatch is thrown on column type mismatch, but Table::set_binary()
    // does not properly check it yet.

    group.remove_table("table");
    CHECK_LOGIC_ERROR(table->set_binary(0, 0, bd), LogicError::detached_accessor);

    // Logic error LogicError::binary_too_big checked in Table_StringOrBinaryTooBig
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
    table.set_float(0, ndx, 1.12f);
    table.set_double(1, ndx, 102.13);

    CHECK_EQUAL(1.12f, table.get_float(0, ndx));
    CHECK_EQUAL(102.13, table.get_double(1, ndx));

    // Test adding multiple rows
    ndx = table.add_empty_row(7);
    for (size_t i = ndx; i < 7; ++i) {
        table.set_float(0, i, 1.12f + 100 * i);
        table.set_double(1, i, 102.13 * 200 * i);
    }

    for (size_t i = ndx; i < 7; ++i) {
        const float v1 = 1.12f + 100 * i;
        const double v2 = 102.13 * 200 * i;
        CHECK_EQUAL(v1, table.get_float(0, i));
        CHECK_EQUAL(v2, table.get_double(1, i));
    }

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

class TestTable01 : public TestTable {
public:
    TestTable01(Allocator& a)
        : TestTable(a)
    {
        init();
    }
    TestTable01()
    {
        init();
    }
    void init()
    {
        add_column(type_Int, "first");
        add_column(type_Int, "second");
        add_column(type_Bool, "third");
        add_column(type_Int, "fourth");
    }
};

} // anonymous namespace

TEST(Table_2)
{
    TestTable01 table;
    add(table, 0, 10, true, Wed);

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(10, table.get_int(1, 0));
    CHECK_EQUAL(true, table.get_bool(2, 0));
    CHECK_EQUAL(Wed, table.get_int(3, 0));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_3)
{
    TestTable01 table;

    for (size_t i = 0; i < 100; ++i) {
        add(table, 0, 10, true, Wed);
    }

    // Test column searching
    CHECK_EQUAL(size_t(0), table.find_first_int(0, 0));
    CHECK_EQUAL(size_t(-1), table.find_first_int(0, 1));
    CHECK_EQUAL(size_t(0), table.find_first_int(1, 10));
    CHECK_EQUAL(size_t(-1), table.find_first_int(1, 100));
    CHECK_EQUAL(size_t(0), table.find_first_bool(2, true));
    CHECK_EQUAL(size_t(-1), table.find_first_bool(2, false));
    CHECK_EQUAL(size_t(0), table.find_first_int(3, Wed));
    CHECK_EQUAL(size_t(-1), table.find_first_int(3, Mon));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

class TestTableEnum : public TestTable {
public:
    TestTableEnum()
    {
        add_column(type_Int, "first");
        add_column(type_String, "second");
    }
};

} // anonymous namespace

TEST(Table_4)
{
    TestTableEnum table;

    add(table, Mon, "Hello");
    add(table, Mon, "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello");

    CHECK_EQUAL(Mon, table.get_int(0, 0));
    CHECK_EQUAL("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello",
                table.get_string(1, 1));

    // Test string column searching
    CHECK_EQUAL(size_t(1), table.find_first_string(
                               1, "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello"));
    CHECK_EQUAL(size_t(-1), table.find_first_string(1, "Foo"));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

namespace {

class TestTableFloats : public TestTable {
public:
    TestTableFloats()
    {
        add_column(type_Float, "first");
        add_column(type_Double, "second");
    }
};

} // anonymous namespace

TEST(Table_Float2)
{
    TestTableFloats table;

    add(table, 1.1f, 2.2);
    add(table, 1.1f, 2.2);

    CHECK_EQUAL(1.1f, table.get_float(0, 0));
    CHECK_EQUAL(2.2, table.get_double(1, 1));

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_Delete)
{
    TestTable01 table;

    for (int i = 0; i < 10; ++i) {
        add(table, 0, i, true, Wed);
    }

    table.remove(0);
    table.remove(4);
    table.remove(7);

    CHECK_EQUAL(1, table.get_int(1, 0));
    CHECK_EQUAL(2, table.get_int(1, 1));
    CHECK_EQUAL(3, table.get_int(1, 2));
    CHECK_EQUAL(4, table.get_int(1, 3));
    CHECK_EQUAL(6, table.get_int(1, 4));
    CHECK_EQUAL(7, table.get_int(1, 5));
    CHECK_EQUAL(8, table.get_int(1, 6));

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
        TableRef subtab = table.get_subtable(0, 0);
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
        TableRef subtab = table->get_subtable(0, 0);
        CHECK_EQUAL("table", table->get_name());
        CHECK_EQUAL("", subtab->get_name());
    }
}


namespace {

void setup_multi_table(Table& table, size_t rows, size_t sub_rows, bool fixed_subtab_sizes = false)
{
    // Create table with all column types
    {
        DescriptorRef sub1;
        table.add_column(type_Int, "int");                 //  0
        table.add_column(type_Bool, "bool");               //  1
        table.add_column(type_OldDateTime, "date");        //  2
        table.add_column(type_Float, "float");             //  3
        table.add_column(type_Double, "double");           //  4
        table.add_column(type_String, "string");           //  5
        table.add_column(type_String, "string_long");      //  6
        table.add_column(type_String, "string_big_blobs"); //  7
        table.add_column(type_String, "string_enum");      //  8 - becomes StringEnumColumn
        table.add_column(type_Binary, "binary");           //  9
        table.add_column(type_Table, "tables", &sub1);     // 10
        table.add_column(type_Mixed, "mixed");             // 11
        table.add_column(type_Int, "int_null", true);      // 12, nullable = true
        sub1->add_column(type_Int, "sub_first");
        sub1->add_column(type_String, "sub_second");
    }

    table.add_empty_row(rows);

    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_int(0, i, int64_t(i * sign));

        if (i % 4 == 0) {
            table.set_null(12, i);
        }
        else {
            table.set_int(12, i, int64_t(i * sign));
        }
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_bool(1, i, (i % 2 ? true : false));
    for (size_t i = 0; i < rows; ++i)
        table.set_olddatetime(2, i, 12345);
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_float(3, i, 123.456f * sign);
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        table.set_double(4, i, 9876.54321 * sign);
    }
    std::vector<std::string> strings;
    for (size_t i = 0; i < rows; ++i) {
        std::stringstream out;
        out << "string" << i;
        strings.push_back(out.str());
    }
    for (size_t i = 0; i < rows; ++i)
        table.set_string(5, i, strings[i]);
    for (size_t i = 0; i < rows; ++i) {
        std::string str_i(strings[i] + " very long string.........");
        table.set_string(6, i, str_i);
    }
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
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        size_t n = sub_rows;
        if (!fixed_subtab_sizes)
            n += i;
        for (size_t j = 0; j != n; ++j) {
            TableRef subtable = table.get_subtable(10, i);
            int64_t val = -123 + i * j * 1234 * sign;
            subtable->insert_empty_row(j);
            subtable->set_int(0, j, val);
            subtable->set_string(1, j, "sub");
        }
    }
    for (size_t i = 0; i < rows; ++i) {
        int64_t sign = (i % 2 == 0) ? 1 : -1;
        switch (i % 8) {
            case 0:
                table.set_mixed(11, i, false);
                break;
            case 1:
                table.set_mixed(11, i, int64_t(i * i * sign));
                break;
            case 2:
                table.set_mixed(11, i, "string");
                break;
            case 3:
                table.set_mixed(11, i, OldDateTime(123456789));
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
                subtable->add_column(type_Int, "first");
                subtable->add_column(type_String, "second");
                for (size_t j = 0; j != 2; ++j) {
                    subtable->insert_empty_row(j);
                    subtable->set_int(0, j, i * i * j * sign);
                    subtable->set_string(1, j, "mixed sub");
                }
                break;
            }
            case 6:
                table.set_mixed(11, i, float(123.1 * i * sign));
                break;
            case 7:
                table.set_mixed(11, i, double(987.65 * i * sign));
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
    TestTable01 table;
    add(table, 10, 120, false, Mon);
    add(table, 12, 100, true, Tue);

#ifdef REALM_DEBUG
    table.verify();
#endif

    TestTable01 table2 = table;

#ifdef REALM_DEBUG
    table2.verify();
#endif

    CHECK(table2 == table);

    auto table3 = table.copy();

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
        parent.add_column(type_Table, "child", &sub_1);
        sub_1->add_column(type_Int, "int");                     // 0
        sub_1->add_column(type_Bool, "bool");                   // 1
        sub_1->add_column(type_Float, "float");                 // 2
        sub_1->add_column(type_Double, "double");               // 3
        sub_1->add_column(type_OldDateTime, "date");            // 4
        sub_1->add_column(type_String, "string");               // 5
        sub_1->add_column(type_Binary, "binary");               // 6
        sub_1->add_column(type_Table, "table", &sub_2);         // 7
        sub_1->add_column(type_Mixed, "mixed");                 // 8
        sub_1->add_column(type_Int, "int_null", nullptr, true); // 9, nullable = true
        sub_2->add_column(type_Int, "i");
    }

    parent.add_empty_row(); // Create a degenerate subtable

    ConstTableRef degen_child = parent.get_subtable(0, 0); // NOTE: Constness is essential here!!!

    CHECK_EQUAL(0, degen_child->size());
    CHECK_EQUAL(10, degen_child->get_column_count());

    // Searching:

    //    CHECK_EQUAL(0, degen_child->distinct(0).size()); // needs index but you cannot set index on ConstTableRef
    CHECK_EQUAL(0, degen_child->get_sorted_view(0).size());

    CHECK_EQUAL(not_found, degen_child->find_first_int(0, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_bool(1, false));
    CHECK_EQUAL(not_found, degen_child->find_first_float(2, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_double(3, 0));
    CHECK_EQUAL(not_found, degen_child->find_first_olddatetime(4, OldDateTime()));
    CHECK_EQUAL(not_found, degen_child->find_first_string(5, StringData("")));
    //    CHECK_EQUAL(not_found, degen_child->find_first_binary(6, BinaryData())); // Exists but not yet implemented
    //    CHECK_EQUAL(not_found, degen_child->find_first_subtable(7, subtab)); // Not yet implemented
    //    CHECK_EQUAL(not_found, degen_child->find_first_mixed(8, Mixed())); // Not yet implemented

    CHECK_EQUAL(0, degen_child->find_all_int(0, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_bool(1, false).size());
    CHECK_EQUAL(0, degen_child->find_all_float(2, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_double(3, 0).size());
    CHECK_EQUAL(0, degen_child->find_all_olddatetime(4, OldDateTime()).size());
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
    CHECK_EQUAL(0, degen_child->minimum_olddatetime(4));

    CHECK_EQUAL(0, degen_child->maximum_int(0));
    CHECK_EQUAL(0, degen_child->maximum_float(2));
    CHECK_EQUAL(0, degen_child->maximum_double(3));
    CHECK_EQUAL(0, degen_child->maximum_olddatetime(4));

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
    CHECK_EQUAL(not_found, degen_child->where().equal_olddatetime(4, OldDateTime()).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(5, StringData("")).find());
    CHECK_EQUAL(not_found, degen_child->where().equal(6, BinaryData()).find());
    //    CHECK_EQUAL(not_found, degen_child->where().equal(7, subtab).find()); // Not yet implemented
    //    CHECK_EQUAL(not_found, degen_child->where().equal(8, Mixed()).find()); // Not yet implemented

    CHECK_EQUAL(not_found, degen_child->where().not_equal(0, int64_t()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(2, float()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal(3, double()).find());
    CHECK_EQUAL(not_found, degen_child->where().not_equal_olddatetime(4, OldDateTime()).find());
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
    for (size_t i = 0; i < 100; ++i)
        table.set_int(0, i, i);
    TableView tv = table.get_range_view(10, 20);
    CHECK_EQUAL(10, tv.size());
    for (size_t i = 0; i < tv.size(); ++i)
        CHECK_EQUAL(int64_t(i + 10), tv.get_int(0, i));

    for (size_t i = 0; i < 5; ++i)
        table.insert_empty_row(0);

    CHECK(tv.sync_if_needed());
    for (size_t i = 0; i < tv.size(); ++i)
        CHECK_EQUAL(int64_t(i + 5), tv.get_int(0, i));
}

TEST(Table_RangeConst)
{
    Group group;
    {
        TableRef table = group.add_table("test");
        table->add_column(type_Int, "int");
        table->add_empty_row(100);
        for (int i = 0; i < 100; ++i)
            table->set_int(0, i, i);
    }
    ConstTableRef ctable = group.get_table("test");
    ConstTableView tv = ctable->get_range_view(10, 20);
    CHECK_EQUAL(10, tv.size());
    for (size_t i = 0; i < tv.size(); ++i)
        CHECK_EQUAL(int64_t(i + 10), tv.get_int(0, i));
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
#if GENERATE // enable to generate testfile - check it manually
    std::ofstream test_file(file_name.c_str(), std::ios::out);
    test_file << result;
    std::cerr << "to_string() test:\n" << result << std::endl;
#else
    std::ifstream test_file(file_name.c_str(), std::ios::in);
    CHECK(!test_file.fail());
    std::string expected;
    expected.assign(std::istreambuf_iterator<char>(test_file), std::istreambuf_iterator<char>());
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

    std::string expected = "    int   bool                 date           float          double   string
string_long  string_enum     binary  mixed  tables\n"
                      "1:   -1   true  1970-01-01 03:25:45  -1.234560e+002  -9.876543e+003  string1  string1 very long
st...  enum2          7 bytes     -1     [3]\n";
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
    TestTable01 table;

    for (int i = 1000; i >= 0; --i) {
        add(table, 0, i, true, Wed);
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
        static_cast<void>(b);
        static_cast<void>(a);
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
    TestTable01 table;

    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);

    // Search for a value that does not exits
    auto v0 = table.find_all_int(1, 5);
    CHECK_EQUAL(0, v0.size());

    // Search for a value with several matches
    auto v = table.find_all_int(1, 20);

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
    TestTable01 table;

    add(table, 0, 10, true, Wed); // 0: 4
    add(table, 0, 20, true, Wed); // 1: 7
    add(table, 0, 0, true, Wed);  // 2: 0
    add(table, 0, 40, true, Wed); // 3: 8
    add(table, 0, 15, true, Wed); // 4: 6
    add(table, 0, 11, true, Wed); // 5: 5
    add(table, 0, 6, true, Wed);  // 6: 3
    add(table, 0, 4, true, Wed);  // 7: 2
    add(table, 0, 99, true, Wed); // 8: 9
    add(table, 0, 2, true, Wed);  // 9: 1

    // Search for a value that does not exits
    auto v = table.get_sorted_view(1);
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
    TestTable01 table;

    add(table, 0, 10, true, Wed);  // 0: 4
    add(table, 0, 20, false, Wed); // 1: 7
    add(table, 0, 0, false, Wed);  // 2: 0
    add(table, 0, 40, false, Wed); // 3: 8
    add(table, 0, 15, false, Wed); // 4: 6
    add(table, 0, 11, true, Wed);  // 5: 5
    add(table, 0, 6, true, Wed);   // 6: 3
    add(table, 0, 4, true, Wed);   // 7: 2
    add(table, 0, 99, true, Wed);  // 8: 9
    add(table, 0, 2, true, Wed);   // 9: 1

    // Count booleans
    size_t count_original = table.where().equal(2, false).count();
    CHECK_EQUAL(4, count_original);

    // Get a view containing the complete table
    auto v = table.find_all_int(0, 0);
    CHECK_EQUAL(table.size(), v.size());

    // Count booleans
    size_t count_view = table.where(&v).equal(2, false).count();
    CHECK_EQUAL(4, count_view);

    auto v_sorted = table.get_sorted_view(1);
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

    std::vector<std::vector<size_t>> col_ndx1 = {{0}, {1}};
    std::vector<bool> asc = {true, true};

    // (0, 10); (1, 10); (1, 14); (2, 10); (2; 14)
    TableView v_sorted1 = table.get_sorted_view(SortDescriptor{table, col_ndx1, asc});
    CHECK_EQUAL(table.size(), v_sorted1.size());
    CHECK_EQUAL(2, v_sorted1.get_source_ndx(0));
    CHECK_EQUAL(0, v_sorted1.get_source_ndx(1));
    CHECK_EQUAL(4, v_sorted1.get_source_ndx(2));
    CHECK_EQUAL(1, v_sorted1.get_source_ndx(3));
    CHECK_EQUAL(3, v_sorted1.get_source_ndx(4));

    std::vector<std::vector<size_t>> col_ndx2 = {{1}, {0}};

    // (0, 10); (1, 10); (2, 10); (1, 14); (2, 14)
    TableView v_sorted2 = table.get_sorted_view(SortDescriptor{table, col_ndx2, asc});
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

    add(table, Mon, "jeff");
    add(table, Tue, "jim");
    add(table, Wed, "jennifer");
    add(table, Thu, "john");
    add(table, Fri, "jimmy");
    add(table, Sat, "jimbo");
    add(table, Sun, "johnny");
    add(table, Mon, "jennifer"); // duplicate

    table.add_search_index(1);
    CHECK(table.has_search_index(1));

    const size_t r1 = table.find_first_string(1, "jimmi");
    CHECK_EQUAL(not_found, r1);

    const size_t r2 = table.find_first_string(1, "jeff");
    const size_t r3 = table.find_first_string(1, "jim");
    const size_t r4 = table.find_first_string(1, "jimbo");
    const size_t r5 = table.find_first_string(1, "johnny");
    CHECK_EQUAL(0, r2);
    CHECK_EQUAL(1, r3);
    CHECK_EQUAL(5, r4);
    CHECK_EQUAL(6, r5);

    const size_t c1 = table.count_string(1, "jennifer");
    CHECK_EQUAL(2, c1);
}


TEST(Table_IndexStringTwice)
{
    TestTableEnum table;

    add(table, Mon, "jeff");
    add(table, Tue, "jim");
    add(table, Wed, "jennifer");
    add(table, Thu, "john");
    add(table, Fri, "jimmy");
    add(table, Sat, "jimbo");
    add(table, Sun, "johnny");
    add(table, Mon, "jennifer"); // duplicate

    table.add_search_index(1);
    CHECK_EQUAL(true, table.has_search_index(1));
    table.add_search_index(1);
    CHECK_EQUAL(true, table.has_search_index(1));
}


// Tests Table part of index on Int, OldDateTime and Bool columns. For a more exhaustive
// test of the integer index (bypassing Table), see test_index_string.cpp)
TEST(Table_IndexInteger)
{
    Table table;
    size_t r;

    table.add_column(type_Int, "ints");
    table.add_column(type_OldDateTime, "date");
    table.add_column(type_Bool, "date");

    table.add_empty_row(13);

    table.set_int(0, 0, 3);  // 0
    table.set_int(0, 1, 1);  // 1
    table.set_int(0, 2, 2);  // 2
    table.set_int(0, 3, 2);  // 3
    table.set_int(0, 4, 2);  // 4
    table.set_int(0, 5, 3);  // 5
    table.set_int(0, 6, 3);  // 6
    table.set_int(0, 7, 2);  // 7
    table.set_int(0, 8, 4);  // 8
    table.set_int(0, 9, 2);  // 9
    table.set_int(0, 10, 6); // 10
    table.set_int(0, 11, 2); // 11
    table.set_int(0, 12, 3); // 12

    table.add_search_index(0);
    CHECK(table.has_search_index(0));
    table.add_search_index(1);
    CHECK(table.has_search_index(1));
    table.add_search_index(2);
    CHECK(table.has_search_index(2));

    table.set_olddatetime(1, 10, OldDateTime(43));
    r = table.find_first_olddatetime(1, OldDateTime(43));
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


TEST(Table_SetIntUnique)
{
    Table table;
    table.add_column(type_Int, "ints");
    table.add_column(type_Int, "ints_null", true);
    table.add_column(type_Int, "ints_null", true);
    table.add_empty_row(10);

    CHECK_LOGIC_ERROR(table.set_int_unique(0, 0, 123), LogicError::no_search_index);
    CHECK_LOGIC_ERROR(table.set_int_unique(1, 0, 123), LogicError::no_search_index);
    CHECK_LOGIC_ERROR(table.set_null_unique(2, 0), LogicError::no_search_index);
    table.add_search_index(0);
    table.add_search_index(1);
    table.add_search_index(2);

    table.set_int_unique(0, 0, 123);
    CHECK_EQUAL(table.size(), 10);

    table.set_int_unique(1, 0, 123);
    CHECK_EQUAL(table.size(), 10);

    table.set_int_unique(2, 0, 123);
    CHECK_EQUAL(table.size(), 10);

    // Check that conflicting SetIntUniques result in rows being deleted. First a collision in column 0:
    table.set_int_unique(0, 1, 123); // This will delete row 1
    CHECK_EQUAL(table.size(), 9);

    table.set_int_unique(1, 1, 123); // This will delete row 1
    CHECK_EQUAL(table.size(), 8);

    table.set_int_unique(1, 2, 123); // This will delete row 1
    CHECK_EQUAL(table.size(), 7);

    // Collision in column 1:
    table.set_int_unique(1, 0, 123); // no-op
    CHECK_EQUAL(table.size(), 7);
    table.set_int_unique(0, 0, 123); // no-op
    CHECK_EQUAL(table.size(), 7);
    table.set_int_unique(2, 0, 123); // no-op
    CHECK_EQUAL(table.size(), 7);

    // Collision in column 2:
    table.set_int_unique(2, 1, 123); // This will delete a row
    CHECK_EQUAL(table.size(), 6);
    table.set_int_unique(0, 1, 123); // This will delete a row
    CHECK_EQUAL(table.size(), 5);
    table.set_int_unique(1, 1, 123); // This will delete a row
    CHECK_EQUAL(table.size(), 4);

    // Since table.add_empty_row(10); filled the column with all nulls, only two rows should now remain
    table.set_null_unique(2, 1);
    CHECK_EQUAL(table.size(), 2);

    table.set_null_unique(2, 0);
    CHECK_EQUAL(table.size(), 1);
}


TEST_TYPES(Table_SetStringUnique, std::true_type, std::false_type)
{
    bool string_enum_column = TEST_TYPE::value;
    Table table;
    table.add_column(type_Int, "ints");
    table.add_column(type_String, "strings");
    table.add_column(type_String, "strings_nullable", true);
    table.add_empty_row(10); // all duplicates!

    CHECK_LOGIC_ERROR(table.set_string_unique(1, 0, "foo"), LogicError::no_search_index);
    CHECK_LOGIC_ERROR(table.set_string_unique(2, 0, "foo"), LogicError::no_search_index);
    table.add_search_index(1);
    table.add_search_index(2);

    if (string_enum_column) {
        bool force = true;
        table.optimize(force);
    }

    table.set_string_unique(1, 0, "bar");

    // Check that conflicting SetStringUniques result in rows with duplicate values being deleted.
    table.set_string_unique(1, 1, "bar");
    CHECK_EQUAL(table.size(), 9); // Only duplicates of "bar" are removed.

    table.set_string_unique(2, 0, realm::null());
    CHECK_EQUAL(table.size(), 1);
}


TEST(Table_AddInt)
{
    Table t;
    t.add_column(type_Int, "i");
    t.add_column(type_Int, "ni", /*nullable*/ true);
    t.add_empty_row(1);

    t.add_int(0, 0, 1);
    CHECK_EQUAL(t.get_int(0, 0), 1);

    // Check that signed integers wrap around. This invariant is necessary for
    // full commutativity.
    t.add_int(0, 0, Table::max_integer);
    CHECK_EQUAL(t.get_int(0, 0), Table::min_integer);
    t.add_int(0, 0, -1);
    CHECK_EQUAL(t.get_int(0, 0), Table::max_integer);

    // add_int() has no effect on a NULL
    CHECK(t.is_null(1, 0));
    CHECK_LOGIC_ERROR(t.add_int(1, 0, 123), LogicError::illegal_combination);
}


TEST(Table_SetUniqueAccessorUpdating)
{
    Group g;
    TableRef origin = g.add_table("origin");
    TableRef target = g.add_table("target");

    target->add_column(type_Int, "col");
    origin->add_column(type_Int, "pk");
    origin->add_column_link(type_LinkList, "list", *target);
    origin->add_search_index(0);

    origin->add_empty_row(2);
    origin->set_int_unique(0, 0, 1);
    origin->set_int_unique(0, 1, 2);

    Row row_0 = (*origin)[0];
    Row row_1 = (*origin)[1];
    LinkViewRef lv_0 = origin->get_linklist(1, 0);
    LinkViewRef lv_1 = origin->get_linklist(1, 1);

    // check new row number > old row number

    origin->add_empty_row(2);
    // leaves row 0 as winner, move last over of 2
    origin->set_int_unique(0, 2, 1);

    CHECK_EQUAL(origin->size(), 3);
    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK_EQUAL(row_0.get_index(), 0);
    CHECK_EQUAL(row_1.get_index(), 1);

    CHECK(lv_0->is_attached());
    CHECK(lv_1->is_attached());
    CHECK(lv_0 == origin->get_linklist(1, 0));
    CHECK(lv_1 == origin->get_linklist(1, 1));

    // check new row number < old row number

    origin->insert_empty_row(0, 2);
    CHECK_EQUAL(origin->size(), 5);
    // winner is row 3, row 0 is deleted via move_last_over(0)
    origin->set_int_unique(0, 0, 2);
    CHECK_EQUAL(origin->size(), 4);

    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK_EQUAL(row_0.get_index(), 2); // unchanged
    CHECK_EQUAL(row_1.get_index(), 3); // unchanged

    CHECK(lv_0->is_attached());
    CHECK(lv_1->is_attached());
    CHECK(lv_0 == origin->get_linklist(1, 2));
    CHECK(lv_1 == origin->get_linklist(1, 3));
}


TEST(Table_SetUniqueLoserAccessorUpdates)
{
    Group g;
    TableRef origin = g.add_table("origin");
    TableRef target = g.add_table("target");

    target->add_column(type_Int, "col");
    target->add_empty_row(6);
    size_t int_col = origin->add_column(type_Int, "pk");
    size_t ll_col = origin->add_column_link(type_LinkList, "list", *target);
    size_t str_col = origin->add_column(type_String, "description");
    origin->add_search_index(0);
    origin->add_search_index(2);

    origin->add_empty_row(4);
    origin->set_int_unique(int_col, 0, 1);
    origin->set_int_unique(int_col, 1, 2);
    origin->set_string(str_col, 0, "zero");
    origin->set_string(str_col, 1, "one");
    origin->set_string(str_col, 2, "two");
    origin->set_string(str_col, 3, "three");

    Row row_0 = (*origin)[0];
    Row row_1 = (*origin)[1];
    Row row_2 = (*origin)[2];
    LinkViewRef lv_0 = origin->get_linklist(ll_col, 0);
    LinkViewRef lv_1 = origin->get_linklist(ll_col, 1);
    lv_0->add(0); // one link
    lv_1->add(1); // two links
    lv_1->add(2);

    CHECK_EQUAL(origin->size(), 4);
    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(row_0.get_string(str_col), "zero");
    CHECK_EQUAL(row_1.get_string(str_col), "one");
    CHECK_EQUAL(row_2.get_string(str_col), "two");

    // leaves row 0 as winner, move last over of 2
    origin->set_int_unique(int_col, 2, 1);

    CHECK_EQUAL(origin->size(), 3);
    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK(row_2.is_attached());
    CHECK_EQUAL(row_0.get_index(), 0);
    CHECK_EQUAL(row_1.get_index(), 1);
    CHECK_EQUAL(row_2.get_index(), 0);
    CHECK_EQUAL(row_0.get_string(str_col), "zero");
    CHECK_EQUAL(row_1.get_string(str_col), "one");
    CHECK_EQUAL(row_2.get_string(str_col), "zero");
    CHECK_EQUAL(row_0.get_linklist(ll_col)->size(), 1);
    CHECK_EQUAL(row_1.get_linklist(ll_col)->size(), 2);
    CHECK_EQUAL(row_2.get_linklist(ll_col)->size(), 1); // subsumed
    CHECK_EQUAL(lv_0->size(), 1);
    CHECK_EQUAL(lv_1->size(), 2);

    CHECK(lv_0->is_attached());
    CHECK(lv_1->is_attached());
    CHECK(lv_0 == origin->get_linklist(1, 0));
    CHECK(lv_1 == origin->get_linklist(1, 1));
}


TEST(Table_AccessorsUpdateAfterMergeRows)
{
    Group g;
    TableRef origin = g.add_table("origin");
    TableRef target = g.add_table("target");

    target->add_column(type_Int, "col");
    target->add_empty_row(6);

    origin->add_column_link(type_Link, "link_column", *target);
    origin->add_empty_row(3);
    origin->set_link(0, 0, 0);
    origin->set_link(0, 1, 1);
    origin->set_link(0, 2, 2);

    Row row_0 = (*origin)[0];
    Row row_1 = (*origin)[1];

    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK_EQUAL(row_0.get_index(), 0);
    CHECK_EQUAL(row_1.get_index(), 1);

    origin->merge_rows(1, 2);

    CHECK(row_0.is_attached());
    CHECK(row_1.is_attached());
    CHECK_EQUAL(row_0.get_index(), 0);
    CHECK_EQUAL(row_1.get_index(), 2);
}


TEST(Table_Distinct)
{
    TestTableEnum table;

    add(table, Mon, "A");
    add(table, Tue, "B");
    add(table, Wed, "C");
    add(table, Thu, "B");
    add(table, Fri, "C");
    add(table, Sat, "D");
    add(table, Sun, "D");
    add(table, Mon, "D");

    table.add_search_index(1);
    CHECK(table.has_search_index(1));

    auto view = table.get_distinct_view(1);

    CHECK_EQUAL(4, view.size());
    CHECK_EQUAL(0, view.get_source_ndx(0));
    CHECK_EQUAL(1, view.get_source_ndx(1));
    CHECK_EQUAL(2, view.get_source_ndx(2));
    CHECK_EQUAL(5, view.get_source_ndx(3));
}


TEST(Table_DistinctEnums)
{
    TestTableEnum table;
    add(table, Mon, "A");
    add(table, Tue, "B");
    add(table, Wed, "C");
    add(table, Thu, "B");
    add(table, Fri, "C");
    add(table, Sat, "D");
    add(table, Sun, "D");
    add(table, Mon, "D");

    table.add_search_index(0);
    CHECK(table.has_search_index(0));

    auto view = table.get_distinct_view(0);

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
    table.add_column(type_OldDateTime, "first");
    table.add_empty_row(4);
    table.set_olddatetime(0, 0, OldDateTime(0));
    table.set_olddatetime(0, 1, OldDateTime(1));
    table.set_olddatetime(0, 2, OldDateTime(3));
    table.set_olddatetime(0, 3, OldDateTime(3));

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
    TestTable01 table;

    add(table, 0, 1, true, Wed);
    add(table, 0, 15, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 20, true, Wed);
    add(table, 0, 11, true, Wed);
    add(table, 0, 45, true, Wed);
    add(table, 0, 10, true, Wed);
    add(table, 0, 0, true, Wed);
    add(table, 0, 30, true, Wed);
    add(table, 0, 9, true, Wed);

    // Create index for column two
    table.add_search_index(1);

    // Search for a value that does not exits
    const size_t r1 = table.find_first_int(1, 2);
    CHECK_EQUAL(npos, r1);

    // Find existing values
    CHECK_EQUAL(0, table.find_first_int(1, 1));
    CHECK_EQUAL(1, table.find_first_int(1, 15));
    CHECK_EQUAL(2, table.find_first_int(1, 10));
    CHECK_EQUAL(3, table.find_first_int(1, 20));
    CHECK_EQUAL(4, table.find_first_int(1, 11));
    CHECK_EQUAL(5, table.find_first_int(1, 45));
    // CHECK_EQUAL(6, table.find_first_int(1, 10)); // only finds first match
    CHECK_EQUAL(7, table.find_first_int(1, 0));
    CHECK_EQUAL(8, table.find_first_int(1, 30));
    CHECK_EQUAL(9, table.find_first_int(1, 9));

    // Change some values
    table.set_int(1, 2, 13);
    table.set_int(1, 9, 100);

    CHECK_EQUAL(0, table.find_first_int(1, 1));
    CHECK_EQUAL(1, table.find_first_int(1, 15));
    CHECK_EQUAL(2, table.find_first_int(1, 13));
    CHECK_EQUAL(3, table.find_first_int(1, 20));
    CHECK_EQUAL(4, table.find_first_int(1, 11));
    CHECK_EQUAL(5, table.find_first_int(1, 45));
    CHECK_EQUAL(6, table.find_first_int(1, 10));
    CHECK_EQUAL(7, table.find_first_int(1, 0));
    CHECK_EQUAL(8, table.find_first_int(1, 30));
    CHECK_EQUAL(9, table.find_first_int(1, 100));

    // Insert values
    add(table, 0, 29, true, Wed);
    // TODO: More than add

    CHECK_EQUAL(0, table.find_first_int(1, 1));
    CHECK_EQUAL(1, table.find_first_int(1, 15));
    CHECK_EQUAL(2, table.find_first_int(1, 13));
    CHECK_EQUAL(3, table.find_first_int(1, 20));
    CHECK_EQUAL(4, table.find_first_int(1, 11));
    CHECK_EQUAL(5, table.find_first_int(1, 45));
    CHECK_EQUAL(6, table.find_first_int(1, 10));
    CHECK_EQUAL(7, table.find_first_int(1, 0));
    CHECK_EQUAL(8, table.find_first_int(1, 30));
    CHECK_EQUAL(9, table.find_first_int(1, 100));
    CHECK_EQUAL(10, table.find_first_int(1, 29));

    // Delete some values
    table.remove(0);
    table.remove(5);
    table.remove(8);

    CHECK_EQUAL(0, table.find_first_int(1, 15));
    CHECK_EQUAL(1, table.find_first_int(1, 13));
    CHECK_EQUAL(2, table.find_first_int(1, 20));
    CHECK_EQUAL(3, table.find_first_int(1, 11));
    CHECK_EQUAL(4, table.find_first_int(1, 45));
    CHECK_EQUAL(5, table.find_first_int(1, 0));
    CHECK_EQUAL(6, table.find_first_int(1, 30));
    CHECK_EQUAL(7, table.find_first_int(1, 100));

#ifdef REALM_DEBUG
    table.verify();
#endif
}


namespace {

class TestTableAE : public TestTable {
public:
    TestTableAE()
    {
        add_column(type_Int, "first");
        add_column(type_String, "second");
        add_column(type_Bool, "third");
        add_column(type_Int, "fourth");
    }
};

} // anonymous namespace

TEST(Table_AutoEnumeration)
{
    TestTableAE table;

    for (size_t i = 0; i < 5; ++i) {
        add(table, 1, "abd", true, Mon);
        add(table, 2, "eftg", true, Tue);
        add(table, 5, "hijkl", true, Wed);
        add(table, 8, "mnopqr", true, Thu);
        add(table, 9, "stuvxyz", true, Fri);
    }

    table.optimize();

    for (size_t i = 0; i < 5; ++i) {
        const size_t n = i * 5;
        CHECK_EQUAL(1, table.get_int(0, 0 + n));
        CHECK_EQUAL(2, table.get_int(0, 1 + n));
        CHECK_EQUAL(5, table.get_int(0, 2 + n));
        CHECK_EQUAL(8, table.get_int(0, 3 + n));
        CHECK_EQUAL(9, table.get_int(0, 4 + n));

        CHECK_EQUAL("abd", table.get_string(1, 0 + n));
        CHECK_EQUAL("eftg", table.get_string(1, 1 + n));
        CHECK_EQUAL("hijkl", table.get_string(1, 2 + n));
        CHECK_EQUAL("mnopqr", table.get_string(1, 3 + n));
        CHECK_EQUAL("stuvxyz", table.get_string(1, 4 + n));

        CHECK_EQUAL(true, table.get_bool(2, 0 + n));
        CHECK_EQUAL(true, table.get_bool(2, 1 + n));
        CHECK_EQUAL(true, table.get_bool(2, 2 + n));
        CHECK_EQUAL(true, table.get_bool(2, 3 + n));
        CHECK_EQUAL(true, table.get_bool(2, 4 + n));

        CHECK_EQUAL(Mon, table.get_int(3, 0 + n));
        CHECK_EQUAL(Tue, table.get_int(3, 1 + n));
        CHECK_EQUAL(Wed, table.get_int(3, 2 + n));
        CHECK_EQUAL(Thu, table.get_int(3, 3 + n));
        CHECK_EQUAL(Fri, table.get_int(3, 4 + n));
    }

    // Verify counts
    const size_t count1 = table.count_string(1, "abd");
    const size_t count2 = table.count_string(1, "eftg");
    const size_t count3 = table.count_string(1, "hijkl");
    const size_t count4 = table.count_string(1, "mnopqr");
    const size_t count5 = table.count_string(1, "stuvxyz");
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
        add(table, 1, "abd", true, Mon);
        add(table, 2, "eftg", true, Tue);
        add(table, 5, "hijkl", true, Wed);
        add(table, 8, "mnopqr", true, Thu);
        add(table, 9, "stuvxyz", true, Fri);
    }

    table.optimize();

    size_t t = table.find_first_string(1, "eftg");
    CHECK_EQUAL(1, t);

    auto tv = table.find_all_string(1, "eftg");
    CHECK_EQUAL(5, tv.size());
    CHECK_EQUAL("eftg", tv.get_string(1, 0));
    CHECK_EQUAL("eftg", tv.get_string(1, 1));
    CHECK_EQUAL("eftg", tv.get_string(1, 2));
    CHECK_EQUAL("eftg", tv.get_string(1, 3));
    CHECK_EQUAL("eftg", tv.get_string(1, 4));
}

TEST(Table_AutoEnumerationOptimize)
{
    Table t;
    t.add_column(type_String, "col1");
    t.add_column(type_String, "col2");
    t.add_column(type_String, "col3");
    t.add_column(type_String, "col4");

    // Insert non-optimzable strings
    std::string s;
    for (size_t i = 0; i < 10; ++i) {
        auto ndx = t.add_empty_row(1);
        t.set_string(0, ndx, s.c_str());
        t.set_string(1, ndx, s.c_str());
        t.set_string(2, ndx, s.c_str());
        t.set_string(3, ndx, s.c_str());
        s += "x";
    }
    t.optimize();

    // AutoEnumerate in reverse order
    for (size_t i = 0; i < 10; ++i) {
        t.set_string(3, i, "test");
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t.set_string(2, i, "test");
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t.set_string(1, i, "test");
    }
    t.optimize();
    for (size_t i = 0; i < 10; ++i) {
        t.set_string(0, i, "test");
    }
    t.optimize();

    for (size_t i = 0; i < 10; ++i) {
        CHECK_EQUAL("test", t.get_string(0, i));
        CHECK_EQUAL("test", t.get_string(1, i));
        CHECK_EQUAL("test", t.get_string(2, i));
        CHECK_EQUAL("test", t.get_string(3, i));
    }

#ifdef REALM_DEBUG
    t.verify();
#endif
}

TEST(Table_OptimizeSubtable)
{
    Table t;
    DescriptorRef descr;
    t.add_column(type_Table, "sub", false, &descr);
    descr->add_column(type_String, "str");
    t.add_empty_row();
    t.add_empty_row();

    {
        // Non-enumerable
        auto r = t.get_subtable(0, 0);
        std::string s;
        for (int i = 0; i < 100; ++i) {
            auto ndx = r->add_empty_row();
            r->set_string(0, ndx, s.c_str());
            s += 'x';
        }
    }

    {
        // Enumerable
        auto r = t.get_subtable(0, 1);
        for (int i = 0; i < 100; ++i) {
            auto ndx = r->add_empty_row();
            r->set_string(0, ndx, "foo");
        }
        r->optimize();
    }

    // Verify
    {
        // Non-enumerable
        auto r = t.get_subtable(0, 0);
        std::string s;
        for (size_t i = 0; i < r->size(); ++i) {
            CHECK_EQUAL(s.c_str(), r->get_string(0, i));
            s += 'x';
        }
    }
    {
        // Non-enumerable
        auto r = t.get_subtable(0, 1);
        for (size_t i = 0; i < r->size(); ++i) {
            CHECK_EQUAL("foo", r->get_string(0, i));
        }
    }
}

TEST(Table_OptimizeCompare)
{
    Table t1, t2;
    t1.add_column(type_String, "str");
    t2.add_column(type_String, "str");

    t1.add_empty_row(100);
    for (int i = 0; i < 100; ++i) {
        t1.set_string(0, i, "foo");
    }
    t2.add_empty_row(100);
    for (int i = 0; i < 100; ++i) {
        t2.set_string(0, i, "foo");
    }
    t1.optimize();
    CHECK(t1 == t2);
    t1.set_string(0, 50, "bar");
    CHECK(t1 != t2);
    t1.set_string(0, 50, "foo");
    CHECK(t1 == t2);
    t2.set_string(0, 50, "bar");
    CHECK(t1 != t2);
    t2.set_string(0, 50, "foo");
    CHECK(t1 == t2);
}


TEST(Table_SlabAlloc)
{
    SlabAlloc alloc;
    alloc.attach_empty();
    TestTable01 table(alloc);

    add(table, 0, 10, true, Wed);

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(10, table.get_int(1, 0));
    CHECK_EQUAL(true, table.get_bool(2, 0));
    CHECK_EQUAL(Wed, table.get_int(3, 0));

    // Add some more rows
    add(table, 1, 10, true, Wed);
    add(table, 2, 20, true, Wed);
    add(table, 3, 10, true, Wed);
    add(table, 4, 20, true, Wed);
    add(table, 5, 10, true, Wed);

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
        table->add_column(type_Int, "first");
        table->add_column(type_String, "second");
        table->add_column(type_Table, "third", &sub_1);
        sub_1->add_column(type_Int, "sub_first");
        sub_1->add_column(type_String, "sub_second");
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

        CHECK_EQUAL(42, subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    CHECK_EQUAL(1, table->get_subtable_size(2, 0));

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);

        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(42, subtable->get_int(0, 0));
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

        CHECK_EQUAL(1, subtable2->size());
        CHECK_EQUAL(42, subtable2->get_int(0, 0));
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
    table->add_subcolumn(column_path, type_Int, "first");
    table->add_subcolumn(column_path, type_String, "second");
    table->add_subcolumn(column_path, type_Table, "third");

    column_path.push_back(2); // third column (which is a sub-table col)

    table->add_subcolumn(column_path, type_Int, "sub_first");
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

        CHECK_EQUAL(42, subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }
}

TEST(Table_SpecRenameColumns)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int, "sub_first");
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

        CHECK_EQUAL(42, subtable->get_int(0, 0));
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
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third");
    table->add_column(type_String, "fourth"); // will be auto-enumerated

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int, "sub_first");
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

        CHECK_EQUAL(42, subtable->get_int(0, 0));
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

        CHECK_EQUAL(2, subtable->get_column_count());
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(42, subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
    }

    // Create path to column in sub-table
    column_path.clear();
    column_path.push_back(1); // third

    // Remove a column in sub-table
    table->remove_subcolumn(column_path, 1); // sub_second

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(1, 0);

        CHECK_EQUAL(1, subtable->get_column_count());
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(42, subtable->get_int(0, 0));
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


TEST(Table_SpecMoveColumns)
{
    using df = _impl::DescriptorFriend;

    Group group;
    TableRef foo = group.add_table("foo");
    foo->add_column(type_Int, "a");
    foo->add_column(type_Float, "b");
    foo->add_column(type_Table, "c");
    DescriptorRef foo_descriptor = foo->get_descriptor();
    DescriptorRef c_descriptor = foo_descriptor->get_subdescriptor(2);
    c_descriptor->add_column(type_Int, "c_a");
    c_descriptor->add_column(type_Float, "c_b");

    foo->add_empty_row();
    foo->add_empty_row();

    TableRef subtable0 = foo->get_subtable(2, 0);
    subtable0->add_empty_row();
    subtable0->set_int(0, 0, 123);

    df::move_column(*foo_descriptor, 0, 2);
    CHECK_EQUAL(foo_descriptor->get_column_type(1), type_Table);
    CHECK_EQUAL(foo_descriptor->get_column_name(1), "c");
    CHECK(c_descriptor->is_attached());
    CHECK(subtable0->is_attached());
    CHECK_EQUAL(123, subtable0->get_int(0, 0));

    TableRef subtable1 = foo->get_subtable(1, 1);
    subtable1->add_empty_row();
    subtable1->set_int(0, 0, 456);

    df::move_column(*c_descriptor, 0, 1);
    CHECK(subtable0->is_attached());
    CHECK(subtable1->is_attached());
    CHECK_EQUAL(subtable0->get_int(1, 0), 123);
    CHECK_EQUAL(subtable1->get_int(1, 0), 456);
}


TEST(Table_SpecMoveLinkColumn)
{
    using df = _impl::DescriptorFriend;

    Group group;
    TableRef target = group.add_table("target");
    target->add_column(type_Int, "a");

    TableRef origin = group.add_table("origin");
    origin->add_column_link(type_Link, "a", *target);
    origin->add_column(type_Int, "b");

    origin->add_empty_row(2);
    target->add_empty_row(2);
    origin->set_link(0, 0, 1);

    df::move_column(*origin->get_descriptor(), 0, 1);

    CHECK_EQUAL(origin->get_link(1, 0), 1);
    CHECK_EQUAL(target->get_backlink_count(0, *origin, 1), 0);
    CHECK_EQUAL(target->get_backlink_count(1, *origin, 1), 1);
}


TEST(Table_SpecMoveColumnsWithIndexes)
{
    using df = _impl::DescriptorFriend;
    using tf = _impl::TableFriend;

    Group group;

    TableRef foo = group.add_table("foo");
    DescriptorRef desc = foo->get_descriptor();
    foo->add_column(type_Int, "a");
    foo->add_search_index(0);
    foo->add_column(type_Int, "b");
    StringIndex* a_index = tf::get_column(*foo, 0).get_search_index();
    CHECK_EQUAL(1, a_index->get_ndx_in_parent());

    df::move_column(*desc, 0, 1);

    CHECK_EQUAL(2, a_index->get_ndx_in_parent());

    auto& spec = df::get_spec(*desc);

    CHECK(foo->has_search_index(1));
    CHECK((spec.get_column_attr(1) & col_attr_Indexed));
    CHECK(!foo->has_search_index(0));
    CHECK(!(spec.get_column_attr(0) & col_attr_Indexed));

    foo->add_column(type_Int, "c");
    foo->add_search_index(0);
    StringIndex* b_index = tf::get_column(*foo, 0).get_search_index();
    CHECK_EQUAL(1, b_index->get_ndx_in_parent());
    CHECK_EQUAL(3, a_index->get_ndx_in_parent());

    df::move_column(*desc, 0, 1);
    CHECK(foo->has_search_index(0));
    CHECK((spec.get_column_attr(0) & col_attr_Indexed));
    CHECK(foo->has_search_index(1));
    CHECK((spec.get_column_attr(1) & col_attr_Indexed));
    CHECK(!foo->has_search_index(2));
    CHECK(!(spec.get_column_attr(2) & col_attr_Indexed));
    CHECK_EQUAL(1, a_index->get_ndx_in_parent());
    CHECK_EQUAL(3, b_index->get_ndx_in_parent());

    df::move_column(*desc, 2, 0);
    CHECK(!foo->has_search_index(0));
    CHECK(!(spec.get_column_attr(0) & col_attr_Indexed));
    CHECK(foo->has_search_index(1));
    CHECK((spec.get_column_attr(1) & col_attr_Indexed));
    CHECK(foo->has_search_index(2));
    CHECK((spec.get_column_attr(2) & col_attr_Indexed));
    CHECK_EQUAL(2, a_index->get_ndx_in_parent());
    CHECK_EQUAL(4, b_index->get_ndx_in_parent());

    df::move_column(*desc, 1, 0);
    CHECK(foo->has_search_index(0));
    CHECK((spec.get_column_attr(0) & col_attr_Indexed));
    CHECK(!foo->has_search_index(1));
    CHECK(!(spec.get_column_attr(1) & col_attr_Indexed));
    CHECK(foo->has_search_index(2));
    CHECK((spec.get_column_attr(2) & col_attr_Indexed));
    CHECK_EQUAL(1, a_index->get_ndx_in_parent());
    CHECK_EQUAL(4, b_index->get_ndx_in_parent());
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
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int, "sub_first");
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

        CHECK_EQUAL(42, subtable->get_int(0, 0));
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

        CHECK_EQUAL(3, subtable->get_column_count());
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(42, subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
        CHECK_EQUAL(0, subtable->get_int(2, 0));
    }

    // Add new table column to sub-table
    table->add_subcolumn(column_path, type_Table, "sub_fourth");

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);

        CHECK_EQUAL(4, subtable->get_column_count());
        CHECK_EQUAL(1, subtable->size());
        CHECK_EQUAL(42, subtable->get_int(0, 0));
        CHECK_EQUAL("test", subtable->get_string(1, 0));
        CHECK_EQUAL(0, subtable->get_int(2, 0));
        CHECK_EQUAL(0, subtable->get_subtable_size(3, 0));
        CHECK_EQUAL(1, table->get_subtable_size(2, 0));
    }

    // Add new column to new sub-table
    column_path.push_back(3); // sub_forth
    table->add_subcolumn(column_path, type_String, "first");

    // Get the sub-table again and see if the values
    // still match.
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK_EQUAL(4, subtable->get_column_count());

        TableRef subsubtable = subtable->get_subtable(3, 0);
        CHECK_EQUAL(1, subsubtable->get_column_count());
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
    table->add_column(type_Int, "age");
    table->add_column(type_Bool, "hired");
    table->add_column(type_Table, "phones");

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

    table.insert_empty_row(1);
    table.set_int(0, 1, 43);
    table.set_mixed(1, 1, int64_t(12));

    CHECK_EQUAL(0, table.get_int(0, ndx));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());

    table.insert_empty_row(2);
    table.set_int(0, 2, 100);
    table.set_mixed(1, 2, "test");

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());

    table.insert_empty_row(3);
    table.set_int(0, 3, 0);
    table.set_mixed(1, 3, OldDateTime(324234));

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0, table.get_int(0, 3));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_OldDateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_olddatetime());

    table.insert_empty_row(4);
    table.set_int(0, 4, 43);
    table.set_mixed(1, 4, Mixed(BinaryData("binary", 7)));

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0, table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_OldDateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_olddatetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7, table.get_mixed(1, 4).get_binary().size());

    table.insert_empty_row(5);
    table.set_int(0, 5, 0);
    table.set_mixed(1, 5, Mixed::subtable_tag());

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0, table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(0, table.get_int(0, 5));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_OldDateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(type_Table, table.get_mixed(1, 5).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_olddatetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7, table.get_mixed(1, 4).get_binary().size());

    // Get table from mixed column and add schema and some values
    TableRef subtable = table.get_subtable(1, 5);
    subtable->add_column(type_String, "name");
    subtable->add_column(type_Int, "age");

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

    CHECK_EQUAL(0, table.get_int(0, 0));
    CHECK_EQUAL(43, table.get_int(0, 1));
    CHECK_EQUAL(0, table.get_int(0, 3));
    CHECK_EQUAL(43, table.get_int(0, 4));
    CHECK_EQUAL(0, table.get_int(0, 5));
    CHECK_EQUAL(31, table.get_int(0, 6));
    CHECK_EQUAL(0, table.get_int(0, 7));
    CHECK_EQUAL(type_Bool, table.get_mixed(1, 0).get_type());
    CHECK_EQUAL(type_Int, table.get_mixed(1, 1).get_type());
    CHECK_EQUAL(type_String, table.get_mixed(1, 2).get_type());
    CHECK_EQUAL(type_OldDateTime, table.get_mixed(1, 3).get_type());
    CHECK_EQUAL(type_Binary, table.get_mixed(1, 4).get_type());
    CHECK_EQUAL(type_Table, table.get_mixed(1, 5).get_type());
    CHECK_EQUAL(type_Float, table.get_mixed(1, 6).get_type());
    CHECK_EQUAL(type_Double, table.get_mixed(1, 7).get_type());
    CHECK_EQUAL(true, table.get_mixed(1, 0).get_bool());
    CHECK_EQUAL(12, table.get_mixed(1, 1).get_int());
    CHECK_EQUAL("test", table.get_mixed(1, 2).get_string());
    CHECK_EQUAL(324234, table.get_mixed(1, 3).get_olddatetime());
    CHECK_EQUAL("binary", table.get_mixed(1, 4).get_binary().data());
    CHECK_EQUAL(7, table.get_mixed(1, 4).get_binary().size());
    CHECK_EQUAL(float(1.123), table.get_mixed(1, 6).get_float());
    CHECK_EQUAL(double(2.234), table.get_mixed(1, 7).get_double());

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_Mixed2)
{
    Table table;
    table.add_column(type_Mixed, "first");

    table.add_empty_row(4);
    table.set_mixed(0, 0, int64_t(1));
    table.set_mixed(0, 1, true);
    table.set_mixed(0, 2, OldDateTime(1234));
    table.set_mixed(0, 3, "test");

    CHECK_EQUAL(type_Int, table.get_mixed_type(0, 0));
    CHECK_EQUAL(type_Bool, table.get_mixed_type(0, 1));
    CHECK_EQUAL(type_OldDateTime, table.get_mixed_type(0, 2));
    CHECK_EQUAL(type_String, table.get_mixed_type(0, 3));

    CHECK_EQUAL(1, table.get_mixed(0, 0));
    CHECK_EQUAL(true, table.get_mixed(0, 1));
    CHECK_EQUAL(OldDateTime(1234), table.get_mixed(0, 2));
    CHECK_EQUAL("test", table.get_mixed(0, 3));
}


TEST(Table_SubtableSizeAndClear)
{
    Table table;
    DescriptorRef subdesc;
    table.add_column(type_Table, "subtab", &subdesc);
    table.add_column(type_Mixed, "mixed");
    subdesc->add_column(type_Int, "int");

    table.insert_empty_row(0);
    table.insert_empty_row(1);
    Table subtable;
    table.set_mixed_subtable(1, 1, &subtable);

    CHECK_EQUAL(0, table.get_subtable_size(0, 0)); // Subtable column
    CHECK_EQUAL(0, table.get_subtable_size(1, 0)); // Mixed column, bool value
    CHECK_EQUAL(0, table.get_subtable_size(1, 1)); // Mixed column, table value

    CHECK(table.get_subtable(0, 0));  // Subtable column
    CHECK(!table.get_subtable(1, 0)); // Mixed column, bool value, must return nullptr
    CHECK(table.get_subtable(1, 1));  // Mixed column, table value

    table.set_mixed(1, 0, Mixed::subtable_tag());
    table.set_mixed(1, 1, false);
    CHECK(table.get_subtable(1, 0));
    CHECK(!table.get_subtable(1, 1));

    TableRef subtab1 = table.get_subtable(0, 0);
    TableRef subtab2 = table.get_subtable(1, 0);
    subtab2->add_column(type_Int, "int");

    CHECK_EQUAL(0, table.get_subtable_size(1, 0));
    CHECK(table.get_subtable(1, 0));

    subtab1->insert_empty_row(0);
    subtab2->insert_empty_row(0);

    CHECK_EQUAL(1, table.get_subtable_size(0, 0));
    CHECK_EQUAL(1, table.get_subtable_size(1, 0));

    table.clear_subtable(0, 0);
    table.clear_subtable(1, 0);

    CHECK_EQUAL(0, table.get_subtable_size(0, 0));
    CHECK_EQUAL(0, table.get_subtable_size(1, 0));

    CHECK(table.get_subtable(1, 0));
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
                CHECK_EQUAL(true, bool(subsubtab->get_subtable(0, i_3)));
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
                CHECK_EQUAL(true, bool(subsubtab_mix->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab_mix->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(1, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(subtab->get_subtable(0, i_2)));
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
                CHECK_EQUAL(true, bool(subsubtab->get_subtable(0, i_3)));
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
                CHECK_EQUAL(true, bool(subsubtab_mix->get_subtable(0, i_3)));
                CHECK_EQUAL(false, bool(subsubtab_mix->get_subtable(1, i_3))); // Mixed
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(0, i_3));
                CHECK_EQUAL(0, subsubtab_mix->get_subtable_size(1, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(subtab_mix->get_subtable(0, i_2)));
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

template <class T>
void my_table_1_add_columns(T t)
{
    t->add_column(type_Int, "val");
    t->add_column(type_Int, "val2");
}

template <class T>
void my_table_2_add_columns(T t)
{
    DescriptorRef sub_descr;
    t->add_column(type_Int, "val");
    t->add_column(type_Table, "subtab", &sub_descr);
    my_table_1_add_columns(sub_descr);
}

template <class T>
void my_table_3_add_columns(T t)
{
    DescriptorRef sub_descr;
    t->add_column(type_Table, "subtab", &sub_descr);
    my_table_2_add_columns(sub_descr);
}

} // anonymous namespace

TEST(Table_HighLevelSubtables)
{
    Table t;
    my_table_3_add_columns(&t);
    {
        TableRef r1 = t.get_table_ref();
        ConstTableRef r2 = t.get_table_ref();
        ConstTableRef r3 = r2->get_table_ref();
        r3 = t.get_table_ref(); // Also test assigment that converts to const
        static_cast<void>(r1);
        static_cast<void>(r3);
    }

    t.add_empty_row();
    const Table& ct = t;
    {
        TableRef s1 = t.get_subtable(0, 0);
        ConstTableRef s2 = t.get_subtable(0, 0);
        TableRef s3 = t.get_subtable(0, 0)->get_table_ref();
        ConstTableRef s4 = t.get_subtable(0, 0)->get_table_ref();

        ConstTableRef cs1 = ct.get_subtable(0, 0);
        ConstTableRef cs2 = ct.get_subtable(0, 0)->get_table_ref();

        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
    }

    t.get_subtable(0, 0)->add_empty_row();
    {
        TableRef s1 = t.get_subtable(0, 0)->get_subtable(1, 0);
        ConstTableRef s2 = t.get_subtable(0, 0)->get_subtable(1, 0);
        TableRef s3 = t.get_subtable(0, 0)->get_subtable(1, 0)->get_table_ref();
        ConstTableRef s4 = t.get_subtable(0, 0)->get_subtable(1, 0)->get_table_ref();

        ConstTableRef cs1 = ct.get_subtable(0, 0)->get_subtable(1, 0);
        ConstTableRef cs2 = ct.get_subtable(0, 0)->get_subtable(1, 0)->get_table_ref();

        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
    }

    t.get_subtable(0, 0)->set_int(0, 0, 1);
    CHECK_EQUAL(t.get_subtable(0, 0)->get_int(0, 0), 1);
}


TEST(Table_SubtableCopyOnSetAndInsert)
{
    TestTable t1;
    my_table_1_add_columns(&t1);
    add(t1, 7, 8);

    Table t2;
    my_table_2_add_columns(&t2);
    t2.add_empty_row();
    t2.set_subtable(1, 0, &t1);

    TableRef r1 = t2.get_subtable(1, 0);
    CHECK(t1 == *r1);

    Table t4;
    t4.add_column(type_Mixed, "mix");
    t4.add_empty_row();
    t4.set_mixed_subtable(0, 0, &t2);
    auto r2 = t4.get_subtable(0, 0);
    CHECK(t2 == *r2);
}


TEST(Table_SetMethod)
{
    TestTable t;
    my_table_1_add_columns(&t);
    add(t, 8, 9);
    CHECK_EQUAL(t.get_int(0, 0), 8);
    CHECK_EQUAL(t.get_int(1, 0), 9);
    set(t, 0, 2, 4);
    CHECK_EQUAL(t.get_int(0, 0), 2);
    CHECK_EQUAL(t.get_int(1, 0), 4);
}


namespace {

class TableDateAndBinary : public TestTable {
public:
    TableDateAndBinary()
    {
        add_column(type_OldDateTime, "date");
        add_column(type_Binary, "bin");
    }
};

} // anonymous namespace

TEST(Table_DateAndBinary)
{
    {
        TableDateAndBinary t;

        const size_t size = 10;
        char data[size];
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<char>(i);
        add(t, 8, BinaryData(data, size));
        CHECK_EQUAL(t.get_olddatetime(0, 0), 8);
        BinaryData bin = t.get_binary(1, 0);
        CHECK_EQUAL(bin.size(), size);
        CHECK(std::equal(bin.data(), bin.data() + size, data));
    }

    // Test that 64-bit dates are preserved
    {
        TableDateAndBinary t;

        int64_t date = std::numeric_limits<int64_t>::max() - 400;

        add(t, date, BinaryData(""));
        CHECK_EQUAL(t.get_olddatetime(0, 0), date);
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
    table->add_column(type_Table, "sub", &sub_1);
    sub_1->add_column(type_Int, "num");

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


// set a subtable in an already exisitng row by providing an existing subtable as the example to copy
// FIXME: Do we need both this one and Table_SetSubTableByExample2?
TEST(Table_SetSubTableByExample1)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int, "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add a row
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    // create a freestanding table to be used as a source by set_subtable

    Table sub = Table();
    sub.add_column(type_Int, "sub_first");
    sub.add_column(type_String, "sub_second");
    sub.add_empty_row();
    sub.set_int(0, 0, 42);
    sub.set_string(1, 0, "forty two");
    sub.add_empty_row();
    sub.set_int(0, 1, 3);
    sub.set_string(1, 1, "PI");

    // Get the sub-table back for inspection
    {
        TableRef subtable = table->get_subtable(2, 0);
        CHECK(subtable->is_empty());

        // add a subtable into the row, resembling the sub we just created
        table->set_subtable(2, 0, &sub);

        TableRef subtable2 = table->get_subtable(2, 0);

        CHECK_EQUAL(42, subtable2->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable2->get_string(1, 0));
        CHECK_EQUAL(3, subtable2->get_int(0, 1));
        CHECK_EQUAL("PI", subtable2->get_string(1, 1));
    }
}

// In the tableview class, set a subtable in an already exisitng row by providing an existing subtable as the example
// to copy
// FIXME: Do we need both this one and Table_SetSubTableByExample1?
TEST(Table_SetSubTableByExample2)
{
    Group group;
    TableRef table = group.add_table("test");

    // Create specification with sub-table
    table->add_column(type_Int, "first");
    table->add_column(type_String, "second");
    table->add_column(type_Table, "third");

    // Create path to sub-table column
    std::vector<size_t> column_path;
    column_path.push_back(2); // third

    table->add_subcolumn(column_path, type_Int, "sub_first");
    table->add_subcolumn(column_path, type_String, "sub_second");

    // Add two rows
    table->insert_empty_row(0);
    table->set_int(0, 0, 4);
    table->set_string(1, 0, "Hello");

    table->insert_empty_row(1);
    table->set_int(0, 1, 8);
    table->set_string(1, 1, "Hi!, Hello?");

    Table sub = Table();
    sub.add_column(type_Int, "sub_first");
    sub.add_column(type_String, "sub_second");
    sub.add_empty_row();
    sub.set_int(0, 0, 42);
    sub.set_string(1, 0, "forty two");
    sub.add_empty_row();
    sub.set_int(0, 1, 3);
    sub.set_string(1, 1, "PI");

    // create a tableview with the table as source

    TableView view = table->find_all_int(0, 8); // select the second of the two rows

    // Verify the sub table is empty
    {
        TableRef subtable = view.get_subtable(2, 0);
        CHECK(subtable->is_empty());

        // add a subtable into the second table row (first view row), resembling the sub we just created
        view.set_subtable(2, 0, &sub);

        TableRef subtable2 = view.get_subtable(2, 0); // fetch back the subtable from the view

        CHECK_EQUAL(false, subtable->is_empty());
        CHECK_EQUAL(42, subtable2->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable2->get_string(1, 0));
        CHECK_EQUAL(3, subtable2->get_int(0, 1));
        CHECK_EQUAL("PI", subtable2->get_string(1, 1));

        TableRef subtable3 = table->get_subtable(2, 1); // fetch back the subtable from the table.

        CHECK_EQUAL(42, subtable3->get_int(0, 0));
        CHECK_EQUAL("forty two", subtable3->get_string(1, 0));
        CHECK_EQUAL(3, subtable3->get_int(0, 1));
        CHECK_EQUAL("PI", subtable3->get_string(1, 1));
    }
}


TEST(Table_HasSharedSpec)
{
    Group g;
    auto table2 = g.add_table("foo");
    my_table_2_add_columns(table2);
    CHECK(!table2->has_shared_type());
    table2->add_empty_row();
    CHECK(table2->get_subtable(1, 0)->has_shared_type());

    // Subtable in mixed column
    auto table3 = g.add_table("bar");
    table3->add_column(type_Mixed, "first");

    CHECK(!table3->has_shared_type());
    table3->add_empty_row();
    table3->clear_subtable(0, 0);
    TableRef table4 = table3->get_subtable(0, 0);
    CHECK(table4);
    CHECK(!table4->has_shared_type());
    my_table_2_add_columns(table4);
    table4->add_empty_row();
    CHECK(!table4->has_shared_type());
    CHECK(table4->get_subtable(1, 0)->has_shared_type());
}

#if TEST_DURATION > 0
#define TBL_SIZE REALM_MAX_BPNODE_SIZE * 10
#else
#define TBL_SIZE 10
#endif

TEST(Table_Aggregates)
{
    TestTable table;
    table.add_column(type_Int, "c_int");
    table.add_column(type_Float, "c_float");
    table.add_column(type_Double, "c_double");
    int64_t i_sum = 0;
    double f_sum = 0;
    double d_sum = 0;

    for (int i = 0; i < TBL_SIZE; i++) {
        add(table, 5987654, 4.0f, 3.0);
        i_sum += 5987654;
        f_sum += 4.0f;
        d_sum += 3.0;
    }
    add(table, 1, 1.1f, 1.2);
    add(table, 987654321, 11.0f, 12.0);
    add(table, 5, 4.0f, 3.0);
    i_sum += 1 + 987654321 + 5;
    f_sum += double(1.1f) + double(11.0f) + double(4.0f);
    d_sum += 1.2 + 12.0 + 3.0;
    double size = TBL_SIZE + 3;

    double epsilon = std::numeric_limits<double>::epsilon();

    // minimum
    CHECK_EQUAL(1, table.minimum_int(0));
    CHECK_EQUAL(1.1f, table.minimum_float(1));
    CHECK_EQUAL(1.2, table.minimum_double(2));
    // maximum
    CHECK_EQUAL(987654321, table.maximum_int(0));
    CHECK_EQUAL(11.0f, table.maximum_float(1));
    CHECK_EQUAL(12.0, table.maximum_double(2));
    // sum
    CHECK_APPROXIMATELY_EQUAL(double(i_sum), double(table.sum_int(0)), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum, table.sum_float(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum, table.sum_double(2), 10 * epsilon);
    // average
    CHECK_APPROXIMATELY_EQUAL(i_sum / size, table.average_int(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum / size, table.average_float(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum / size, table.average_double(2), 10 * epsilon);
}


TEST(Table_Aggregates2)
{
    TestTable table;
    table.add_column(type_Int, "c_count");
    int c = -420;
    int s = 0;
    while (c < -20) {
        add(table, c);
        s += c;
        c++;
    }

    CHECK_EQUAL(-420, table.minimum_int(0));
    CHECK_EQUAL(-21, table.maximum_int(0));
    CHECK_EQUAL(s, table.sum_int(0));
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
        table->insert_column(3, type_OldDateTime, "Delivery date", nullable);
        table->insert_column(4, type_Timestamp, "Delivery date 2", nullable);

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

        table->set_olddatetime(3, 0, OldDateTime(2016, 2, 2));
        // table->set_null(3, 1);
        table->set_olddatetime(3, 2, OldDateTime(2016, 6, 6));

        table->set_timestamp(4, 0, Timestamp(2, 2));
        // table->set_null(4, 1);
        table->set_timestamp(4, 2, Timestamp(6, 6));

        size_t count;
        size_t pos;
        if (nullable) {
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(0), 3);
            CHECK_EQUAL(table->maximum_int(0, &pos), 3);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_float(1), 30.f);
            CHECK_EQUAL(table->maximum_float(1, &pos), 30.f);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_double(2), 2.2);
            CHECK_EQUAL(table->maximum_double(2, &pos), 2.2);
            CHECK_EQUAL(pos, 1);

            pos = 123;
            CHECK_EQUAL(table->maximum_olddatetime(3), OldDateTime(2016, 6, 6));
            CHECK_EQUAL(table->maximum_olddatetime(3, &pos), OldDateTime(2016, 6, 6));
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(4), Timestamp(6, 6));
            CHECK_EQUAL(table->maximum_timestamp(4, &pos), Timestamp(6, 6));
            CHECK_EQUAL(pos, 2);

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(0), 1);
            CHECK_EQUAL(table->minimum_int(0, &pos), 1);
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_float(1), 30.f);
            CHECK_EQUAL(table->minimum_float(1, &pos), 30.f);
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->minimum_double(2), 1.1);
            CHECK_EQUAL(table->minimum_double(2, &pos), 1.1);
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_olddatetime(3), OldDateTime(2016, 2, 2));
            CHECK_EQUAL(table->minimum_olddatetime(3, &pos), OldDateTime(2016, 2, 2));
            CHECK_EQUAL(pos, 0);

            pos = 123;
            CHECK_EQUAL(table->minimum_timestamp(4), Timestamp(2, 2));
            CHECK_EQUAL(table->minimum_timestamp(4, &pos), Timestamp(2, 2));
            CHECK_EQUAL(pos, 0);

            // average
            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_int(0), (1 + 3) / 2., 0.01);
            CHECK_APPROXIMATELY_EQUAL(table->average_int(0, &count), (1 + 3) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            count = 123;
            CHECK_EQUAL(table->average_float(1), 30.f);
            CHECK_EQUAL(table->average_float(1, &count), 30.f);
            CHECK_EQUAL(count, 1);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_double(2), (1.1 + 2.2) / 2., 0.01);
            CHECK_APPROXIMATELY_EQUAL(table->average_double(2, &count), (1.1 + 2.2) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            // sum
            CHECK_EQUAL(table->sum_int(0), 4);
            CHECK_EQUAL(table->sum_float(1), 30.f);
            CHECK_APPROXIMATELY_EQUAL(table->sum_double(2), 1.1 + 2.2, 0.01);
        }
        else { // not nullable
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
            CHECK_EQUAL(table->maximum_olddatetime(3, &pos), OldDateTime(2016, 6, 6));
            CHECK_EQUAL(pos, 2);

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(4, &pos), Timestamp(6, 6));
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
            CHECK_EQUAL(table->minimum_olddatetime(3, &pos), OldDateTime(0));
            CHECK_EQUAL(pos, 1);

            pos = 123;
            // Timestamp(0, 0) is default value for non-nullable column
            CHECK_EQUAL(table->minimum_timestamp(4, &pos), Timestamp(0, 0));
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


TEST(Table_EmptyMinmax)
{
    Group g;
    TableRef table = g.add_table("");
    table->add_column(type_Timestamp, "");

    size_t min_index;
    Timestamp min_ts = table->minimum_timestamp(0, &min_index);
    CHECK_EQUAL(min_index, realm::npos);
    CHECK(min_ts.is_null());

    size_t max_index;
    Timestamp max_ts = table->maximum_timestamp(0, &max_index);
    CHECK_EQUAL(max_index, realm::npos);
    CHECK(max_ts.is_null());
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
    subdesc->add_column(type_Int, "a");
    root.add_empty_row(1);
    root.set_subtable(0, 0, &sub);
    root.set_subtable(0, 0, nullptr);
}


TEST(Table_Pivot)
{
    size_t count = 1717;
    TestTable table;
    table.add_column(type_String, "sex");
    table.add_column(type_Int, "age");
    table.add_column(type_Bool, "hired");
    int64_t age_sum[2] = {0, 0};
    int64_t age_cnt[2] = {0, 0};
    int64_t age_min[2];
    int64_t age_max[2];
    double age_avg[2];

    for (size_t i = 0; i < count; ++i) {
        size_t sex = i % 2;
        int64_t age = 3 + (i % 117);
        add(table, (sex == 0) ? "Male" : "Female", age, true);

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

void compare_table_with_slice(TestContext& test_context, const Table& table, const Table& slice, size_t offset,
                              size_t size)
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
            case type_OldDateTime:
                for (size_t i = 0; i != size; ++i) {
                    OldDateTime v_1 = table.get_olddatetime(col_i, offset + i);
                    OldDateTime v_2 = slice.get_olddatetime(col_i, i);
                    CHECK_EQUAL(v_1, v_2);
                }
                break;
            case type_Timestamp:
                for (size_t i = 0; i != size; ++i) {
                    Timestamp v_1 = table.get_timestamp(col_i, offset + i);
                    Timestamp v_2 = slice.get_timestamp(col_i, i);
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
                            case type_OldDateTime:
                                CHECK_EQUAL(v_1.get_olddatetime(), v_2.get_olddatetime());
                                break;
                            case type_Timestamp:
                                CHECK_EQUAL(v_1.get_timestamp(), v_2.get_timestamp());
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


void test_write_slice_name(TestContext& test_context, const Table& table, StringData expect_name, bool override_name)
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

void test_write_slice_contents(TestContext& test_context, const Table& table, size_t offset, size_t size)
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
            compare_table_with_slice(test_context, table, *slice, offset, size_2);
    }
}

} // anonymous namespace


TEST(Table_WriteSlice)
{
    // check that the name of the written table is as expected
    {
        Table table;
        test_write_slice_name(test_context, table, "", false);
        test_write_slice_name(test_context, table, "foo", true); // Override
        test_write_slice_name(test_context, table, "", true);    // Override
    }
    {
        Group group;
        TableRef table = group.add_table("test");
        test_write_slice_name(test_context, *table, "test", false);
        test_write_slice_name(test_context, *table, "foo", true); // Override
        test_write_slice_name(test_context, *table, "", true);    // Override
    }

// Run through a 3-D matrix of table sizes, slice offsets, and
// slice sizes. Each test involves a table with columns of each
// possible type.
#if TEST_DURATION > 0
    int table_sizes[] = {0, 1, 2, 3, 5, 9, 27, 81, 82, 243, 729, 2187, 6561};
#else
    int table_sizes[] = {0, 1, 2, 3, 5, 9, 27, 81, 82, 243, 729, 2187};
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
                test_write_slice_contents(test_context, *table, offset, size);
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
    CHECK_EQUAL(realm::npos, table->get_index_in_group());   // Not a group-level table

    DescriptorRef subdesc;
    table->add_column(type_Table, "", &subdesc);
    table->add_column(type_Mixed, "");
    subdesc->add_column(type_Int, "");
    table->add_empty_row(2);
    table->set_mixed(1, 0, Mixed::subtable_tag());
    table->set_mixed(1, 1, Mixed::subtable_tag());

    TableRef subtab;
    size_t column_ndx = 0;

    subtab = table->get_subtable(0, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(0, 1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(1, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1, 1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    // Check that column indexes are properly adjusted after new
    // column is insert.
    table->insert_column(0, type_Int, "");

    subtab = table->get_subtable(1, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1, 1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(2, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(2, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(2, 1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(2, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    // Check that column indexes are properly adjusted after inserted
    // column is removed.
    table->remove_column(0);

    subtab = table->get_subtable(0, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(0, 1);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(0, column_ndx);
    CHECK_EQUAL(1, subtab->get_parent_row_index());

    subtab = table->get_subtable(1, 0);
    CHECK_EQUAL(table, subtab->get_parent_table(&column_ndx));
    CHECK_EQUAL(1, column_ndx);
    CHECK_EQUAL(0, subtab->get_parent_row_index());

    subtab = table->get_subtable(1, 1);
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
    TableRef subtab_0_0 = parent->get_subtable(0, 0);
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
    TableRef subtab_0_1 = parent->get_subtable(0, 1);
    CHECK_EQUAL(1, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_0_1->size());
    TableRef subtab_1_0 = parent->get_subtable(1, 0);
    CHECK_EQUAL(1, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_0->size());
    TableRef subtab_1_1 = parent->get_subtable(1, 1);
    CHECK_EQUAL(1, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(0, subtab_1_1->size());

    // Check that subtables get their specs correctly updated
    subdesc = parent->get_subdescriptor(0);
    subdesc->add_column(type_Float, "f");
    subdesc = parent->get_subdescriptor(1);
    subdesc->add_column(type_Double, "d");
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    subtab_1_1->add_empty_row();
    subtab_0_0->set_int(0, 0, 10000);
    subtab_0_0->set_float(1, 0, 10010.0f);
    subtab_1_1->set_int(0, 0, 11100);
    subtab_1_1->set_double(1, 0, 11110.0);
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
    CHECK_EQUAL(10000, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11100, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11110.0, subtab_1_1->get_double(1, 0));

    // Insert a row and a column before all the subtables
    parent->insert_column(0, type_Table, "dummy_1");
    parent->insert_empty_row(0);
    subtab_0_0->set_int(0, 0, 10001);
    subtab_0_0->set_float(1, 0, 10011.0f);
    subtab_1_1->set_int(0, 0, 11101);
    subtab_1_1->set_double(1, 0, 11111.0);
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
    CHECK_EQUAL(10001, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11101, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11111.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2, 2));

    // Insert a row and a column between the subtables
    parent->insert_column(2, type_Int, "dummy_2");
    parent->insert_empty_row(2);
    subtab_0_0->set_int(0, 0, 10002);
    subtab_0_0->set_float(1, 0, 10012.0f);
    subtab_1_1->set_int(0, 0, 11102);
    subtab_1_1->set_double(1, 0, 11112.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int, parent->get_column_type(2));
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
    CHECK_EQUAL(10002, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11102, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11112.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3, 3));

    // Insert a column after the subtables
    parent->insert_column(4, type_Table, "dummy_3");
    subtab_0_0->set_int(0, 0, 10003);
    subtab_0_0->set_float(1, 0, 10013.0f);
    subtab_1_1->set_int(0, 0, 11103);
    subtab_1_1->set_double(1, 0, 11113.0);
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Int, parent->get_column_type(2));
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
    CHECK_EQUAL(10003, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11103, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11113.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3, 3));

    // Remove the row and the column between the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int(0, 0, 10004);
    subtab_0_0->set_float(1, 0, 10014.0f);
    subtab_1_1->set_int(0, 0, 11104);
    subtab_1_1->set_double(1, 0, 11114.0);
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
    CHECK_EQUAL(10004, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11104, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11114.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2, 2));

    // Remove the row and the column before the subtables
    parent->remove_column(0);
    parent->remove(0);
    subtab_0_0->set_int(0, 0, 10005);
    subtab_0_0->set_float(1, 0, 10015.0f);
    subtab_1_1->set_int(0, 0, 11105);
    subtab_1_1->set_double(1, 0, 11115.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11105, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11115.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0, 1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1, 1));

    // Remove the row and the column after the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int(0, 0, 10006);
    subtab_0_0->set_float(1, 0, 10016.0f);
    subtab_1_1->set_int(0, 0, 11106);
    subtab_1_1->set_double(1, 0, 11116.0);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Table, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11106, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11116.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0, 1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1, 1));

    // Check that subtable accessors are detached when the subtables are removed
    parent->remove(1);
    subtab_0_0->set_int(0, 0, 10007);
    subtab_0_0->set_float(1, 0, 10017.0f);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    parent->remove_column(1);
    subtab_0_0->set_int(0, 0, 10008);
    subtab_0_0->set_float(1, 0, 10018.0f);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));

    // Clear subtable
    parent->clear_subtable(0, 0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(0, subtab_0_0->size());
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));

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
    subtab_0_0 = parent->get_subtable(0, 0);
    subtab_0_1 = parent->get_subtable(0, 1);
    subtab_1_0 = parent->get_subtable(1, 0);
    subtab_1_1 = parent->get_subtable(1, 1);
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
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0, 0));
    parent->remove(0);
    parent->remove_column(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0, 0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0, 0));

    // Insert 2x2 new subtables, then remove them all together
    parent->add_column(type_Table, "d", &subdesc);
    subdesc->add_column(type_String, "x");
    parent->add_empty_row(2);
    subtab_0_0 = parent->get_subtable(0, 0);
    subtab_0_1 = parent->get_subtable(0, 1);
    subtab_1_0 = parent->get_subtable(1, 0);
    subtab_1_1 = parent->get_subtable(1, 1);
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
    subtab_0_0 = parent->get_subtable(0, 0);
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
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0, 0));
    parent->remove(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    parent->add_empty_row(1);
    subtab_0_0 = parent->get_subtable(0, 0);
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
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0, 0));
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
    TableRef subtab_0_0 = parent->get_subtable(0, 0);
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
    TableRef subtab_1_0 = parent->get_subtable(1, 0);
    subtab_1_0->add_column(type_Int, "x");
    parent->add_empty_row();
    parent->set_mixed(0, 1, Mixed::subtable_tag());
    TableRef subtab_0_1 = parent->get_subtable(0, 1);
    subtab_0_1->add_column(type_Int, "x");
    parent->set_mixed(1, 1, Mixed::subtable_tag());
    TableRef subtab_1_1 = parent->get_subtable(1, 1);
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
    subtab_0_0->add_column(type_Float, "f");
    subtab_0_1->add_column(type_Float, "f");
    subtab_1_0->add_column(type_Double, "d");
    subtab_1_1->add_column(type_Double, "d");
    CHECK_EQUAL(2, subtab_0_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_0->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_0->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_0_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_0_1->get_column_type(0));
    CHECK_EQUAL(type_Float, subtab_0_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_0_1->get_column_name(0));
    CHECK_EQUAL("f", subtab_0_1->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_0->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_0->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_0->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_0->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_0->get_column_name(1));
    CHECK_EQUAL(2, subtab_1_1->get_column_count());
    CHECK_EQUAL(type_Int, subtab_1_1->get_column_type(0));
    CHECK_EQUAL(type_Double, subtab_1_1->get_column_type(1));
    CHECK_EQUAL("x", subtab_1_1->get_column_name(0));
    CHECK_EQUAL("d", subtab_1_1->get_column_name(1));

    // Check that cell changes in subtables are visible
    subtab_1_1->add_empty_row();
    subtab_0_0->set_int(0, 0, 10000);
    subtab_0_0->set_float(1, 0, 10010.0f);
    subtab_1_1->set_int(0, 0, 11100);
    subtab_1_1->set_double(1, 0, 11110.0);
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
    CHECK_EQUAL(10000, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10010.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11100, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11110.0, subtab_1_1->get_double(1, 0));

    // Insert a row and a column before all the subtables
    parent->insert_column(0, type_Table, "dummy_1");
    parent->insert_empty_row(0);
    subtab_0_0->set_int(0, 0, 10001);
    subtab_0_0->set_float(1, 0, 10011.0f);
    subtab_1_1->set_int(0, 0, 11101);
    subtab_1_1->set_double(1, 0, 11111.0);
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
    CHECK_EQUAL(10001, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10011.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11101, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11111.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2, 2));

    // Insert a row and a column between the subtables
    parent->insert_column(2, type_Int, "dummy_2");
    parent->insert_empty_row(2);
    parent->set_mixed(3, 2, "Lopadotemachoselachogaleokranioleipsanodrimhypotrimmatosilphio"
                            "paraomelitokatakechy­menokichlepikossyphophattoperisteralektryonopte"
                            "kephalliokigklopeleiolagoiosiraiobaphetraganopterygon");
    subtab_0_0->set_int(0, 0, 10002);
    subtab_0_0->set_float(1, 0, 10012.0f);
    subtab_1_1->set_int(0, 0, 11102);
    subtab_1_1->set_double(1, 0, 11112.0);
    CHECK_EQUAL(4, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int, parent->get_column_type(2));
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
    CHECK_EQUAL(10002, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10012.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11102, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11112.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3, 3));

    // Insert a column after the subtables
    parent->insert_column(4, type_Table, "dummy_3");
    subtab_0_0->set_int(0, 0, 10003);
    subtab_0_0->set_float(1, 0, 10013.0f);
    subtab_1_1->set_int(0, 0, 11103);
    subtab_1_1->set_double(1, 0, 11113.0);
    CHECK_EQUAL(5, parent->get_column_count());
    CHECK_EQUAL(type_Table, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Int, parent->get_column_type(2));
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
    CHECK_EQUAL(10003, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10013.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11103, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11113.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 3));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(3, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(3, 3));

    // Remove the row and the column between the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int(0, 0, 10004);
    subtab_0_0->set_float(1, 0, 10014.0f);
    subtab_1_1->set_int(0, 0, 11104);
    subtab_1_1->set_double(1, 0, 11114.0);
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
    CHECK_EQUAL(10004, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10014.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11104, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11114.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(1, 1));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(1, 2));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(2, 1));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(2, 2));

    // Remove the row and the column before the subtables
    parent->remove_column(0);
    parent->remove(0);
    subtab_0_0->set_int(0, 0, 10005);
    subtab_0_0->set_float(1, 0, 10015.0f);
    subtab_1_1->set_int(0, 0, 11105);
    subtab_1_1->set_double(1, 0, 11115.0);
    CHECK_EQUAL(3, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(type_Table, parent->get_column_type(2));
    CHECK_EQUAL(3, parent->size());
    CHECK_EQUAL(10005, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10015.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11105, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11115.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0, 1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1, 1));

    // Remove the row and the column after the subtables
    parent->remove_column(2);
    parent->remove(2);
    subtab_0_0->set_int(0, 0, 10006);
    subtab_0_0->set_float(1, 0, 10016.0f);
    subtab_1_1->set_int(0, 0, 11106);
    subtab_1_1->set_double(1, 0, 11116.0);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(type_Mixed, parent->get_column_type(0));
    CHECK_EQUAL(type_Mixed, parent->get_column_type(1));
    CHECK_EQUAL(2, parent->size());
    CHECK_EQUAL(10006, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10016.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(11106, subtab_1_1->get_int(0, 0));
    CHECK_EQUAL(11116.0, subtab_1_1->get_double(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_0_1, parent->get_subtable(0, 1));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    CHECK_EQUAL(subtab_1_1, parent->get_subtable(1, 1));

    // Check that subtable accessors are detached when the subtables are removed
    parent->remove(1);
    subtab_0_0->set_int(0, 0, 10007);
    subtab_0_0->set_float(1, 0, 10017.0f);
    CHECK_EQUAL(2, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10007, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10017.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));
    CHECK_EQUAL(subtab_1_0, parent->get_subtable(1, 0));
    parent->remove_column(1);
    subtab_0_0->set_int(0, 0, 10008);
    subtab_0_0->set_float(1, 0, 10018.0f);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    CHECK(subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(!subtab_1_1->is_attached());
    CHECK_EQUAL(10008, subtab_0_0->get_int(0, 0));
    CHECK_EQUAL(10018.0f, subtab_0_0->get_float(1, 0));
    CHECK_EQUAL(subtab_0_0, parent->get_subtable(0, 0));

    // Remove subtable
    parent->clear_subtable(0, 0);
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
    subtab_0_0 = parent->get_subtable(0, 0);
    subtab_0_1 = parent->get_subtable(0, 1);
    subtab_1_0 = parent->get_subtable(1, 0);
    subtab_1_1 = parent->get_subtable(1, 1);
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
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0, 0));
    parent->remove(0);
    parent->remove_column(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(1, parent->size());
    subtab_1_1 = parent->get_subtable(0, 0);
    CHECK(!subtab_0_0->is_attached());
    CHECK(!subtab_0_1->is_attached());
    CHECK(!subtab_1_0->is_attached());
    CHECK(subtab_1_1->is_attached());
    CHECK_EQUAL(1, subtab_1_1->size());
    CHECK_EQUAL("pneumonoultramicroscopicsilicovolcanoconiosis", subtab_1_1->get_string(0, 0));

    // Insert 2x2 new subtables, then remove them all together
    parent->add_column(type_Mixed, "d");
    parent->add_empty_row(2);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    parent->set_mixed(0, 1, Mixed::subtable_tag());
    parent->set_mixed(1, 0, Mixed::subtable_tag());
    parent->set_mixed(1, 1, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0, 0);
    subtab_0_1 = parent->get_subtable(0, 1);
    subtab_1_0 = parent->get_subtable(1, 0);
    subtab_1_1 = parent->get_subtable(1, 1);
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
    subtab_0_0 = parent->get_subtable(0, 0);
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
    CHECK_EQUAL("brahmaputra", subtab_0_0->get_string(0, 0));
    parent->remove(0);
    CHECK_EQUAL(1, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());

    // Insert 1x1 new subtable, then remove it by removing the last column
    parent->add_empty_row(1);
    parent->set_mixed(0, 0, Mixed::subtable_tag());
    subtab_0_0 = parent->get_subtable(0, 0);
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
    CHECK_EQUAL("baikonur", subtab_0_0->get_string(0, 0));
    parent->remove_column(0);
    CHECK_EQUAL(0, parent->get_column_count());
    CHECK_EQUAL(0, parent->size());
    CHECK(!subtab_0_0->is_attached());
}


TEST(Table_RowAccessor)
{
    Table table;
    DescriptorRef subdesc;
    table.add_column(type_Int, "int");
    table.add_column(type_Bool, "bool");
    table.add_column(type_Float, "");
    table.add_column(type_Double, "");
    table.add_column(type_String, "");
    table.add_column(type_Binary, "", true);
    table.add_column(type_OldDateTime, "");
    table.add_column(type_Table, "", &subdesc);
    table.add_column(type_Mixed, "");
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

    table.set_int(0, 1, 4923);
    table.set_bool(1, 1, true);
    table.set_float(2, 1, 5298.0f);
    table.set_double(3, 1, 2169.0);
    table.set_string(4, 1, "str");
    table.set_binary(5, 1, bin);
    table.set_olddatetime(6, 1, 7739);
    table.set_subtable(7, 1, &one_subtab);
    table.set_mixed(8, 1, Mixed("mix"));

    // Check getters for `RowExpr`
    {
        CHECK_EQUAL(9, table[0].get_column_count());
        CHECK_EQUAL(type_Int, table[0].get_column_type(0));
        CHECK_EQUAL(type_Bool, table[0].get_column_type(1));
        CHECK_EQUAL("int", table[0].get_column_name(0));
        CHECK_EQUAL("bool", table[0].get_column_name(1));
        CHECK_EQUAL(0, table[0].get_column_index("int"));
        CHECK_EQUAL(1, table[0].get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(), table[0].get_int(0));
        CHECK_EQUAL(bool(), table[0].get_bool(1));
        CHECK_EQUAL(float(), table[0].get_float(2));
        CHECK_EQUAL(double(), table[0].get_double(3));
        CHECK_EQUAL(StringData(""), table[0].get_string(4));
        CHECK_EQUAL(BinaryData(), table[0].get_binary(5));
        CHECK_EQUAL(OldDateTime(), table[0].get_olddatetime(6));
        CHECK_EQUAL(0, table[0].get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), table[0].get_mixed(8));
        CHECK_EQUAL(type_Int, table[0].get_mixed_type(8));

        CHECK_EQUAL(4923, table[1].get_int(0));
        CHECK_EQUAL(true, table[1].get_bool(1));
        CHECK_EQUAL(5298.0f, table[1].get_float(2));
        CHECK_EQUAL(2169.0, table[1].get_double(3));
        CHECK_EQUAL("str", table[1].get_string(4));
        CHECK_EQUAL(bin, table[1].get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), table[1].get_olddatetime(6));
        CHECK_EQUAL(1, table[1].get_subtable_size(7));
        CHECK_EQUAL("mix", table[1].get_mixed(8));
        CHECK_EQUAL(type_String, table[1].get_mixed_type(8));

        TableRef subtab_0 = table[0].get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = table[1].get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `ConstRowExpr`
    {
        const Table& const_table = table;

        CHECK_EQUAL(9, const_table[0].get_column_count());
        CHECK_EQUAL(type_Int, const_table[0].get_column_type(0));
        CHECK_EQUAL(type_Bool, const_table[0].get_column_type(1));
        CHECK_EQUAL("int", const_table[0].get_column_name(0));
        CHECK_EQUAL("bool", const_table[0].get_column_name(1));
        CHECK_EQUAL(0, const_table[0].get_column_index("int"));
        CHECK_EQUAL(1, const_table[0].get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(), const_table[0].get_int(0));
        CHECK_EQUAL(bool(), const_table[0].get_bool(1));
        CHECK_EQUAL(float(), const_table[0].get_float(2));
        CHECK_EQUAL(double(), const_table[0].get_double(3));
        CHECK_EQUAL(StringData(""), const_table[0].get_string(4));
        CHECK_EQUAL(BinaryData(), const_table[0].get_binary(5));
        CHECK_EQUAL(OldDateTime(), const_table[0].get_olddatetime(6));
        CHECK_EQUAL(0, const_table[0].get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), const_table[0].get_mixed(8));
        CHECK_EQUAL(type_Int, const_table[0].get_mixed_type(8));

        CHECK_EQUAL(4923, const_table[1].get_int(0));
        CHECK_EQUAL(true, const_table[1].get_bool(1));
        CHECK_EQUAL(5298.0f, const_table[1].get_float(2));
        CHECK_EQUAL(2169.0, const_table[1].get_double(3));
        CHECK_EQUAL("str", const_table[1].get_string(4));
        CHECK_EQUAL(bin, const_table[1].get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), const_table[1].get_olddatetime(6));
        CHECK_EQUAL(1, const_table[1].get_subtable_size(7));
        CHECK_EQUAL("mix", const_table[1].get_mixed(8));
        CHECK_EQUAL(type_String, const_table[1].get_mixed_type(8));

        ConstTableRef subtab_0 = const_table[0].get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = const_table[1].get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `Row`
    {
        Row row_0 = table[0];
        Row row_1 = table[1];

        CHECK_EQUAL(9, row_0.get_column_count());
        CHECK_EQUAL(type_Int, row_0.get_column_type(0));
        CHECK_EQUAL(type_Bool, row_0.get_column_type(1));
        CHECK_EQUAL("int", row_0.get_column_name(0));
        CHECK_EQUAL("bool", row_0.get_column_name(1));
        CHECK_EQUAL(0, row_0.get_column_index("int"));
        CHECK_EQUAL(1, row_0.get_column_index("bool"));

        CHECK_EQUAL(int_fast64_t(), row_0.get_int(0));
        CHECK_EQUAL(bool(), row_0.get_bool(1));
        CHECK_EQUAL(float(), row_0.get_float(2));
        CHECK_EQUAL(double(), row_0.get_double(3));
        CHECK_EQUAL(StringData(""), row_0.get_string(4));
        CHECK_EQUAL(BinaryData(), row_0.get_binary(5));
        CHECK_EQUAL(OldDateTime(), row_0.get_olddatetime(6));
        CHECK_EQUAL(0, row_0.get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), row_0.get_mixed(8));
        CHECK_EQUAL(type_Int, row_0.get_mixed_type(8));

        CHECK_EQUAL(4923, row_1.get_int(0));
        CHECK_EQUAL(true, row_1.get_bool(1));
        CHECK_EQUAL(5298.0f, row_1.get_float(2));
        CHECK_EQUAL(2169.0, row_1.get_double(3));
        CHECK_EQUAL("str", row_1.get_string(4));
        CHECK_EQUAL(bin, row_1.get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), row_1.get_olddatetime(6));
        CHECK_EQUAL(1, row_1.get_subtable_size(7));
        CHECK_EQUAL("mix", row_1.get_mixed(8));
        CHECK_EQUAL(type_String, row_1.get_mixed_type(8));

        TableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `const Row`
    {
        const Row row_0 = table[0];
        const Row row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(), row_0.get_int(0));
        CHECK_EQUAL(bool(), row_0.get_bool(1));
        CHECK_EQUAL(float(), row_0.get_float(2));
        CHECK_EQUAL(double(), row_0.get_double(3));
        CHECK_EQUAL(StringData(""), row_0.get_string(4));
        CHECK_EQUAL(BinaryData(), row_0.get_binary(5));
        CHECK_EQUAL(OldDateTime(), row_0.get_olddatetime(6));
        CHECK_EQUAL(0, row_0.get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), row_0.get_mixed(8));
        CHECK_EQUAL(type_Int, row_0.get_mixed_type(8));

        CHECK_EQUAL(4923, row_1.get_int(0));
        CHECK_EQUAL(true, row_1.get_bool(1));
        CHECK_EQUAL(5298.0f, row_1.get_float(2));
        CHECK_EQUAL(2169.0, row_1.get_double(3));
        CHECK_EQUAL("str", row_1.get_string(4));
        CHECK_EQUAL(bin, row_1.get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), row_1.get_olddatetime(6));
        CHECK_EQUAL(1, row_1.get_subtable_size(7));
        CHECK_EQUAL("mix", row_1.get_mixed(8));
        CHECK_EQUAL(type_String, row_1.get_mixed_type(8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `ConstRow`
    {
        ConstRow row_0 = table[0];
        ConstRow row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(), row_0.get_int(0));
        CHECK_EQUAL(bool(), row_0.get_bool(1));
        CHECK_EQUAL(float(), row_0.get_float(2));
        CHECK_EQUAL(double(), row_0.get_double(3));
        CHECK_EQUAL(StringData(""), row_0.get_string(4));
        CHECK_EQUAL(BinaryData(), row_0.get_binary(5));
        CHECK_EQUAL(OldDateTime(), row_0.get_olddatetime(6));
        CHECK_EQUAL(0, row_0.get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), row_0.get_mixed(8));
        CHECK_EQUAL(type_Int, row_0.get_mixed_type(8));

        CHECK_EQUAL(4923, row_1.get_int(0));
        CHECK_EQUAL(true, row_1.get_bool(1));
        CHECK_EQUAL(5298.0f, row_1.get_float(2));
        CHECK_EQUAL(2169.0, row_1.get_double(3));
        CHECK_EQUAL("str", row_1.get_string(4));
        CHECK_EQUAL(bin, row_1.get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), row_1.get_olddatetime(6));
        CHECK_EQUAL(1, row_1.get_subtable_size(7));
        CHECK_EQUAL("mix", row_1.get_mixed(8));
        CHECK_EQUAL(type_String, row_1.get_mixed_type(8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check getters for `const ConstRow` (double constness)
    {
        const ConstRow row_0 = table[0];
        const ConstRow row_1 = table[1];

        CHECK_EQUAL(int_fast64_t(), row_0.get_int(0));
        CHECK_EQUAL(bool(), row_0.get_bool(1));
        CHECK_EQUAL(float(), row_0.get_float(2));
        CHECK_EQUAL(double(), row_0.get_double(3));
        CHECK_EQUAL(StringData(""), row_0.get_string(4));
        CHECK_EQUAL(BinaryData(), row_0.get_binary(5));
        CHECK_EQUAL(OldDateTime(), row_0.get_olddatetime(6));
        CHECK_EQUAL(0, row_0.get_subtable_size(7));
        CHECK_EQUAL(int_fast64_t(), row_0.get_mixed(8));
        CHECK_EQUAL(type_Int, row_0.get_mixed_type(8));

        CHECK_EQUAL(4923, row_1.get_int(0));
        CHECK_EQUAL(true, row_1.get_bool(1));
        CHECK_EQUAL(5298.0f, row_1.get_float(2));
        CHECK_EQUAL(2169.0, row_1.get_double(3));
        CHECK_EQUAL("str", row_1.get_string(4));
        CHECK_EQUAL(bin, row_1.get_binary(5));
        CHECK_EQUAL(OldDateTime(7739), row_1.get_olddatetime(6));
        CHECK_EQUAL(1, row_1.get_subtable_size(7));
        CHECK_EQUAL("mix", row_1.get_mixed(8));
        CHECK_EQUAL(type_String, row_1.get_mixed_type(8));

        ConstTableRef subtab_0 = row_0.get_subtable(7);
        CHECK(*subtab_0 == empty_subtab);
        ConstTableRef subtab_1 = row_1.get_subtable(7);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);
    }

    // Check setters for `Row`
    {
        Row row_0 = table[0];
        Row row_1 = table[1];

        row_0.set_int(0, 5651);
        row_0.set_bool(1, true);
        row_0.set_float(2, 8397.0f);
        row_0.set_double(3, 1937.0);
        row_0.set_string(4, "foo");
        row_0.set_binary(5, bin);
        row_0.set_olddatetime(6, OldDateTime(9992));
        row_0.set_subtable(7, &one_subtab);
        row_0.set_mixed(8, Mixed(3637.0f));

        row_1.set_int(0, int_fast64_t());
        row_1.set_bool(1, bool());
        row_1.set_float(2, float());
        row_1.set_double(3, double());
        row_1.set_string(4, StringData(""));
        row_1.set_binary(5, BinaryData());
        row_1.set_olddatetime(6, OldDateTime());
        row_1.set_subtable(7, nullptr);
        row_1.set_mixed(8, Mixed());

        Mixed mix_subtab((Mixed::subtable_tag()));

        CHECK_EQUAL(5651, table.get_int(0, 0));
        CHECK_EQUAL(true, table.get_bool(1, 0));
        CHECK_EQUAL(8397.0f, table.get_float(2, 0));
        CHECK_EQUAL(1937.0, table.get_double(3, 0));
        CHECK_EQUAL("foo", table.get_string(4, 0));
        CHECK_EQUAL(bin, table.get_binary(5, 0));
        CHECK_EQUAL(OldDateTime(9992), table.get_olddatetime(6, 0));
        CHECK_EQUAL(3637.0f, table.get_mixed(8, 0));

        CHECK_EQUAL(int_fast64_t(), table.get_int(0, 1));
        CHECK_EQUAL(bool(), table.get_bool(1, 1));
        CHECK_EQUAL(float(), table.get_float(2, 1));
        CHECK_EQUAL(double(), table.get_double(3, 1));
        CHECK_EQUAL(StringData(""), table.get_string(4, 1));
        CHECK_EQUAL(BinaryData(), table.get_binary(5, 1));
        CHECK_EQUAL(OldDateTime(), table.get_olddatetime(6, 1));
        CHECK_EQUAL(int_fast64_t(), table.get_mixed(8, 1));

        TableRef subtab_0 = table.get_subtable(7, 0);
        CHECK_EQUAL(19, subtab_0->get_int(0, 0));
        CHECK(*subtab_0 == one_subtab);
        TableRef subtab_1 = table.get_subtable(7, 1);
        CHECK(*subtab_1 == empty_subtab);

        row_0.set_mixed_subtable(8, 0);
        row_1.set_mixed_subtable(8, &two_subtab);
        subtab_0 = table.get_subtable(8, 0);
        subtab_1 = table.get_subtable(8, 1);
        CHECK(subtab_0);
        CHECK(subtab_1);
        CHECK(subtab_0->is_attached());
        CHECK(subtab_1->is_attached());
        CHECK(*subtab_0 == Table());
        CHECK_EQUAL(29, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == two_subtab);
    }

    // Check setters for `RowExpr`
    {
        table[0].set_int(0, int_fast64_t());
        table[0].set_bool(1, bool());
        table[0].set_float(2, float());
        table[0].set_double(3, double());
        table[0].set_string(4, StringData(""));
        table[0].set_binary(5, BinaryData());
        table[0].set_olddatetime(6, OldDateTime());
        table[0].set_subtable(7, nullptr);
        table[0].set_mixed(8, Mixed());

        table[1].set_int(0, 5651);
        table[1].set_bool(1, true);
        table[1].set_float(2, 8397.0f);
        table[1].set_double(3, 1937.0);
        table[1].set_string(4, "foo");
        table[1].set_binary(5, bin);
        table[1].set_olddatetime(6, OldDateTime(9992));
        table[1].set_subtable(7, &one_subtab);
        table[1].set_mixed(8, Mixed(3637.0f));

        Mixed mix_subtab((Mixed::subtable_tag()));

        CHECK_EQUAL(int_fast64_t(), table.get_int(0, 0));
        CHECK_EQUAL(bool(), table.get_bool(1, 0));
        CHECK_EQUAL(float(), table.get_float(2, 0));
        CHECK_EQUAL(double(), table.get_double(3, 0));
        CHECK_EQUAL(StringData(""), table.get_string(4, 0));
        CHECK_EQUAL(BinaryData(), table.get_binary(5, 0));
        CHECK_EQUAL(OldDateTime(), table.get_olddatetime(6, 0));
        CHECK_EQUAL(int_fast64_t(), table.get_mixed(8, 0));

        CHECK_EQUAL(5651, table.get_int(0, 1));
        CHECK_EQUAL(true, table.get_bool(1, 1));
        CHECK_EQUAL(8397.0f, table.get_float(2, 1));
        CHECK_EQUAL(1937.0, table.get_double(3, 1));
        CHECK_EQUAL("foo", table.get_string(4, 1));
        CHECK_EQUAL(bin, table.get_binary(5, 1));
        CHECK_EQUAL(OldDateTime(9992), table.get_olddatetime(6, 1));
        CHECK_EQUAL(3637.0f, table.get_mixed(8, 1));

        TableRef subtab_0 = table.get_subtable(7, 0);
        CHECK(*subtab_0 == empty_subtab);
        TableRef subtab_1 = table.get_subtable(7, 1);
        CHECK_EQUAL(19, subtab_1->get_int(0, 0));
        CHECK(*subtab_1 == one_subtab);

        table[0].set_mixed_subtable(8, &two_subtab);
        table[1].set_mixed_subtable(8, 0);
        subtab_0 = table.get_subtable(8, 0);
        subtab_1 = table.get_subtable(8, 1);
        CHECK(subtab_0);
        CHECK(subtab_1);
        CHECK(subtab_0->is_attached());
        CHECK(subtab_1->is_attached());
        CHECK_EQUAL(29, subtab_0->get_int(0, 0));
        CHECK(*subtab_0 == two_subtab);
        CHECK(*subtab_1 == Table());
    }

    // Check that we can also create ConstRow's from `const Table`
    {
        const Table& const_table = table;
        ConstRow row_0 = const_table[0];
        ConstRow row_1 = const_table[1];
        CHECK_EQUAL(0, row_0.get_int(0));
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
    CHECK_EQUAL(7, source_row_1.get_link(0));
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


TEST(Table_RowAccessor_DetachedRowExpr)
{
    // Check that it is possible to create a detached RowExpr from scratch.
    BasicRowExpr<Table> row;
    CHECK_NOT(row.is_attached());
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
        Row row_1 = table[0];        // Copy construct `Row` from `RowExpr`
        ConstRow crow_1 = table[1];  // Copy construct `ConstRow` from `RowExpr`
        ConstRow crow_2 = ctable[2]; // Copy construct `ConstRow` from `ConstRowExpr`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table, row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0, row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());

        // Check copy construction of row accessor from other row accessor
        Row drow_1;
        ConstRow dcrow_1;
        CHECK(!drow_1.is_attached());
        CHECK(!dcrow_1.is_attached());
        Row drow_2 = drow_1;        // Copy construct `Row` from detached `Row`
        ConstRow dcrow_2 = drow_1;  // Copy construct `ConstRow` from detached `Row`
        ConstRow dcrow_3 = dcrow_1; // Copy construct `ConstRow` from detached `ConstRow`
        Row row_2 = row_1;          // Copy construct `Row` from attached `Row`
        ConstRow crow_3 = row_1;    // Copy construct `ConstRow` from attached `Row`
        ConstRow crow_4 = crow_1;   // Copy construct `ConstRow` from attached `ConstRow`
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
        row = table[0];     // Assign `RowExpr` to detached `Row`
        crow_1 = table[1];  // Assign `RowExpr` to detached `ConstRow`
        crow_2 = ctable[2]; // Assign `ConstRowExpr` to detached `ConstRow`
        CHECK(row.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table, row.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0, row.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());
        row = table[1];     // Assign `RowExpr` to attached `Row`
        crow_1 = table[2];  // Assign `RowExpr` to attached `ConstRow`
        crow_2 = ctable[0]; // Assign `ConstRowExpr` to attached `ConstRow`
        CHECK(row.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table, row.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(1, row.get_index());
        CHECK_EQUAL(2, crow_1.get_index());
        CHECK_EQUAL(0, crow_2.get_index());
    }

    // Check assignment of row accessor to row accessor
    {
        Row drow, row_1;
        ConstRow dcrow, crow_1, crow_2;
        row_1 = row_1;   // Assign detached `Row` to self
        crow_1 = crow_1; // Assign detached `ConstRow` to self
        CHECK(!row_1.is_attached());
        CHECK(!crow_1.is_attached());
        row_1 = drow;   // Assign detached `Row` to detached `Row`
        crow_1 = drow;  // Assign detached `Row` to detached `ConstRow`
        crow_2 = dcrow; // Assign detached `ConstRow` to detached `ConstRow`
        CHECK(!row_1.is_attached());
        CHECK(!crow_1.is_attached());
        CHECK(!crow_2.is_attached());
        Row row_2 = table[0];
        Row row_3 = table[1];
        ConstRow crow_3 = table[2];
        CHECK(row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK(crow_3.is_attached());
        CHECK_EQUAL(&table, row_2.get_table());
        CHECK_EQUAL(&table, row_3.get_table());
        CHECK_EQUAL(&table, crow_3.get_table());
        CHECK_EQUAL(0, row_2.get_index());
        CHECK_EQUAL(1, row_3.get_index());
        CHECK_EQUAL(2, crow_3.get_index());
        row_1 = row_2;   // Assign attached `Row` to detached `Row`
        crow_1 = row_3;  // Assign attached `Row` to detached `ConstRow`
        crow_2 = crow_3; // Assign attached `ConstRow` to detached `ConstRow`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table, row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(0, row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        CHECK_EQUAL(2, crow_2.get_index());
        row_1 = row_1;   // Assign attached `Row` to self
        crow_1 = crow_1; // Assign attached `ConstRow` to self
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK_EQUAL(&table, row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(0, row_1.get_index());
        CHECK_EQUAL(1, crow_1.get_index());
        Row row_4 = table[2];
        Row row_5 = table[0];
        ConstRow crow_4 = table[1];
        row_1 = row_4;   // Assign attached `Row` to attached `Row`
        crow_1 = row_5;  // Assign attached `Row` to attached `ConstRow`
        crow_2 = crow_4; // Assign attached `ConstRow` to attached `ConstRow`
        CHECK(row_1.is_attached());
        CHECK(crow_1.is_attached());
        CHECK(crow_2.is_attached());
        CHECK_EQUAL(&table, row_1.get_table());
        CHECK_EQUAL(&table, crow_1.get_table());
        CHECK_EQUAL(&table, crow_2.get_table());
        CHECK_EQUAL(2, row_1.get_index());
        CHECK_EQUAL(0, crow_1.get_index());
        CHECK_EQUAL(1, crow_2.get_index());
        row_1 = drow;   // Assign detached `Row` to attached `Row`
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    CHECK_EQUAL(27, row_1.get_int(1));
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
    CHECK_EQUAL(27, row_1.get_int(0));
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
    TableRef mixed = parent->get_subtable(0, 0);
    CHECK(mixed && mixed->is_attached());
    mixed->add_column(type_Int, "mixed");
    mixed->add_empty_row();
    mixed->set_int(0, 0, 19);
    TableRef regular = parent->get_subtable(1, 0);
    CHECK(regular && regular->is_attached());
    regular->add_empty_row();
    regular->set_int(0, 0, 29);
    CHECK(mixed->size() == 1);
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
    mixed = parent->get_subtable(0, 0);
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
    parent->set_subtable(1, 0, nullptr); // Clear
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
        TableRef regular_0 = parent->get_subtable(0, 0);
        TableRef regular_1 = parent->get_subtable(0, 1);
        TableRef regular_4 = parent->get_subtable(0, 4);
        TableRef mixed_0 = parent->get_subtable(1, 0);
        TableRef mixed_1 = parent->get_subtable(1, 1);
        TableRef mixed_4 = parent->get_subtable(1, 4);
        CHECK(row_0.is_attached());
        CHECK(row_1.is_attached());
        CHECK(row_4.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(4, row_4.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_1->is_attached());
        CHECK(regular_4->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0, 0));
        CHECK_EQUAL(11, regular_1->get_int(0, 0));
        CHECK_EQUAL(14, regular_4->get_int(0, 0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_4 && mixed_4->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0, 0));
        CHECK_EQUAL(21, mixed_1->get_int(0, 0));
        CHECK_EQUAL(24, mixed_4->get_int(0, 0));

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
        CHECK_EQUAL(11, regular_1->get_int(0, 0));
        CHECK_EQUAL(14, regular_4->get_int(0, 0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0, 1));
        CHECK_EQUAL(regular_4, parent->get_subtable(0, 2));
        CHECK(!mixed_0->is_attached());
        CHECK(mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0, 0));
        CHECK_EQUAL(24, mixed_4->get_int(0, 0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1, 1));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1, 2));

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
        CHECK_EQUAL(14, regular_4->get_int(0, 0));
        CHECK_EQUAL(regular_4, parent->get_subtable(0, 0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_4->is_attached());
        CHECK_EQUAL(24, mixed_4->get_int(0, 0));
        CHECK_EQUAL(mixed_4, parent->get_subtable(1, 0));
    }

    // Use second table to check with accessors on row indexes 0, 2, and 3, but
    // none at index 1 and 4.
    {
        TableRef parent = parent_2;
        ConstRow row_0 = (*parent)[0];
        ConstRow row_2 = (*parent)[2];
        ConstRow row_3 = (*parent)[3];
        TableRef regular_0 = parent->get_subtable(0, 0);
        TableRef regular_2 = parent->get_subtable(0, 2);
        TableRef regular_3 = parent->get_subtable(0, 3);
        TableRef mixed_0 = parent->get_subtable(1, 0);
        TableRef mixed_2 = parent->get_subtable(1, 2);
        TableRef mixed_3 = parent->get_subtable(1, 3);
        CHECK(row_0.is_attached());
        CHECK(row_2.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_0.get_index());
        CHECK_EQUAL(2, row_2.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_0->is_attached());
        CHECK(regular_2->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(10, regular_0->get_int(0, 0));
        CHECK_EQUAL(12, regular_2->get_int(0, 0));
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK(mixed_0 && mixed_0->is_attached());
        CHECK(mixed_2 && mixed_2->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(20, mixed_0->get_int(0, 0));
        CHECK_EQUAL(22, mixed_2->get_int(0, 0));
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));

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
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0, 0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1, 0));

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
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0, 0));
        CHECK(!mixed_0->is_attached());
        CHECK(!mixed_2->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1, 0));

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
        TableRef regular_1 = parent->get_subtable(0, 1);
        TableRef regular_3 = parent->get_subtable(0, 3);
        TableRef mixed_1 = parent->get_subtable(1, 1);
        TableRef mixed_3 = parent->get_subtable(1, 3);
        CHECK(row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(1, row_1.get_index());
        CHECK_EQUAL(3, row_3.get_index());
        CHECK(regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(11, regular_1->get_int(0, 0));
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK(mixed_1 && mixed_1->is_attached());
        CHECK(mixed_3 && mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0, 0));
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));

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
        CHECK_EQUAL(11, regular_1->get_int(0, 0));
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK_EQUAL(regular_1, parent->get_subtable(0, 1));
        CHECK_EQUAL(regular_3, parent->get_subtable(0, 0));
        CHECK(mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(21, mixed_1->get_int(0, 0));
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));
        CHECK_EQUAL(mixed_1, parent->get_subtable(1, 1));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1, 0));

        // Perform one more 'move last over' operation which brings the number
        // of rows down from 3 to 2
        parent->move_last_over(1); // Move row at index 2 to index 1
        CHECK(!row_1.is_attached());
        CHECK(row_3.is_attached());
        CHECK_EQUAL(0, row_3.get_index());
        CHECK(!regular_1->is_attached());
        CHECK(regular_3->is_attached());
        CHECK_EQUAL(13, regular_3->get_int(0, 0));
        CHECK_EQUAL(regular_3, parent->get_subtable(0, 0));
        CHECK(!mixed_1->is_attached());
        CHECK(mixed_3->is_attached());
        CHECK_EQUAL(23, mixed_3->get_int(0, 0));
        CHECK_EQUAL(mixed_3, parent->get_subtable(1, 0));

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


TEST(Table_InsertColumnMaintainsBacklinkIndices)
{
    Group g;

    TableRef t0 = g.add_table("hrnetprsafd");
    TableRef t1 = g.add_table("qrsfdrpnkd");

    t1->add_column_link(type_Link, "bbb", *t0);
    t1->add_column_link(type_Link, "ccc", *t0);
    t1->insert_column(0, type_Int, "aaa");

    t1->add_empty_row();

    t0->add_column(type_Int, "foo");
    t0->add_empty_row();

    t1->remove_column(0);
    t1->set_link(0, 0, 0);
    t1->remove_column(0);
    t1->set_link(0, 0, 0);
}


TEST(Table_MultipleLinkColumnsToSelf)
{
    Group g;
    TableRef t = g.add_table("A");
    t->insert_column_link(0, type_Link, "e", *t);
    t->insert_column_link(1, type_LinkList, "f", *t);
    t->add_empty_row();
    t->get_linklist(1, 0)->add(0);
    _impl::TableFriend::move_column(*t->get_descriptor(), 0, 1);
    g.verify();
    t->get_linklist(0, 0)->add(0);
    g.verify();
}


TEST(Table_MultipleLinkColumnsToOther)
{
    Group g;
    TableRef t = g.add_table("A");
    TableRef t2 = g.add_table("B");
    t->insert_column_link(0, type_Link, "e", *t2);
    t->insert_column_link(1, type_LinkList, "f", *t);
    t->add_empty_row();
    t->get_linklist(1, 0)->add(0);
    _impl::TableFriend::move_column(*t->get_descriptor(), 0, 1);
    g.verify();
    t->get_linklist(0, 0)->add(0);
    g.verify();
}


TEST(Table_MultipleLinkColumnsMoveTables)
{
    Group g;
    TableRef t = g.add_table("A");
    TableRef t2 = g.add_table("B");
    t->insert_column_link(0, type_Link, "e", *t);
    t->insert_column_link(1, type_LinkList, "f", *t);
    t->add_empty_row();
    t->get_linklist(1, 0)->add(0);
    _impl::TableFriend::move_column(*t->get_descriptor(), 0, 1);
    g.verify();
    t->get_linklist(0, 0)->add(0);
    g.verify();
    g.move_table(0, 1);
    g.verify();
    g.move_table(1, 0);
    g.verify();
}


TEST(Table_MultipleLinkColumnsMoveTablesCrossLinks)
{
    Group g;
    TableRef t = g.add_table("A");
    TableRef t2 = g.add_table("B");
    t->insert_column_link(0, type_Link, "e", *t2);
    t->insert_column_link(1, type_LinkList, "f", *t);
    t->insert_column_link(2, type_Link, "g", *t2);
    t->add_empty_row();
    t->get_linklist(1, 0)->add(0);
    g.move_table(0, 1);
    g.verify();
    _impl::TableFriend::move_column(*t->get_descriptor(), 1, 2);
    g.verify();
    t->get_linklist(2, 0)->add(0);
    g.verify();
    g.move_table(1, 0);
    g.verify();
    _impl::TableFriend::move_column(*t->get_descriptor(), 1, 0);
    g.verify();
}


TEST(Table_AddColumnWithThreeLevelBptree)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row(REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1);
    table.add_column(type_Int, "");
    table.verify();
}


TEST(Table_ClearWithTwoLevelBptree)
{
    Table table;
    table.add_column(type_Mixed, "");
    table.add_empty_row(REALM_MAX_BPNODE_SIZE + 1);
    table.clear();
    table.verify();
}


TEST(Table_IndexStringDelete)
{
    Table t;
    t.add_column(type_String, "str");
    t.add_search_index(0);

    for (size_t i = 0; i < 1000; ++i) {
        t.add_empty_row();
        std::string out(util::to_string(i));
        t.set_string(0, i, out);
    }

    t.clear();

    for (size_t i = 0; i < 1000; ++i) {
        t.add_empty_row();
        std::string out(util::to_string(i));
        t.set_string(0, i, out);
    }
}


TEST(Table_Nulls)
{
    // 'round' lets us run this entire test both with and without index and with/without optimize/enum
    for (size_t round = 0; round < 5; round++) {
        Table t;
        TableView tv;
        t.add_column(type_String, "str", true /*nullable*/);

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

        CHECK_EQUAL(1, t.count_string(0, "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx"));
        CHECK_EQUAL(1, t.count_string(0, ""));
        CHECK_EQUAL(1, t.count_string(0, realm::null()));

        CHECK_EQUAL(0,
                    t.find_first_string(0, "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx"));
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
        t.add_column(type_Int, "int", true);          // nullable = true
        t.add_column(type_Bool, "bool", true);        // nullable = true
        t.add_column(type_OldDateTime, "bool", true); // nullable = true

        t.add_empty_row(2);

        t.set_int(0, 0, 65);
        t.set_bool(1, 0, false);
        t.set_olddatetime(2, 0, OldDateTime(3));

        CHECK_EQUAL(65, t.get_int(0, 0));
        CHECK_EQUAL(false, t.get_bool(1, 0));
        CHECK_EQUAL(OldDateTime(3), t.get_olddatetime(2, 0));

        CHECK_EQUAL(65, t.maximum_int(0));
        CHECK_EQUAL(65, t.minimum_int(0));
        CHECK_EQUAL(OldDateTime(3), t.maximum_olddatetime(2));
        CHECK_EQUAL(OldDateTime(3), t.minimum_olddatetime(2));

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
        CHECK_EQUAL(not_found, t.find_first_olddatetime(2, OldDateTime(5)));

        CHECK_EQUAL(0, t.find_first_int(0, 65));
        CHECK_EQUAL(0, t.find_first_bool(1, false));
        CHECK_EQUAL(0, t.find_first_olddatetime(2, OldDateTime(3)));

        t.set_null(0, 0);
        t.set_null(1, 0);
        t.set_null(2, 0);

        CHECK(t.is_null(0, 0));
        CHECK(t.is_null(1, 0));
        CHECK(t.is_null(2, 0));
    }
    {
        Table t;
        t.add_column(type_Float, "float", true);   // nullable = true
        t.add_column(type_Double, "double", true); // nullable = true

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


TEST(Table_InsertSubstring)
{
    struct Fixture {
        Table table;
        Fixture()
        {
            table.add_column(type_String, "");
            table.add_empty_row();
            table.set_string(0, 0, "0123456789");
        }
    };
    {
        Fixture f;
        f.table.insert_substring(0, 0, 0, "x");
        CHECK_EQUAL("x0123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.insert_substring(0, 0, 5, "x");
        CHECK_EQUAL("01234x56789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.insert_substring(0, 0, 10, "x");
        CHECK_EQUAL("0123456789x", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.insert_substring(0, 0, 5, "");
        CHECK_EQUAL("0123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.insert_substring(1, 0, 5, "x"), LogicError::column_index_out_of_range);
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.insert_substring(0, 1, 5, "x"), LogicError::row_index_out_of_range);
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.insert_substring(0, 0, 11, "x"), LogicError::string_position_out_of_range);
    }
}


TEST(Table_RemoveSubstring)
{
    struct Fixture {
        Table table;
        Fixture()
        {
            table.add_column(type_String, "");
            table.add_empty_row();
            table.set_string(0, 0, "0123456789");
        }
    };
    {
        Fixture f;
        f.table.remove_substring(0, 0, 0, 1);
        CHECK_EQUAL("123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 9, 1);
        CHECK_EQUAL("012345678", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 0);
        CHECK_EQUAL("", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 5);
        CHECK_EQUAL("01234", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 10);
        CHECK_EQUAL("0123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 5, 1000);
        CHECK_EQUAL("01234", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 10, 0);
        CHECK_EQUAL("0123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        f.table.remove_substring(0, 0, 10, 1);
        CHECK_EQUAL("0123456789", f.table.get_string(0, 0));
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.remove_substring(1, 0, 5, 1), LogicError::column_index_out_of_range);
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.remove_substring(0, 1, 5, 1), LogicError::row_index_out_of_range);
    }
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.table.remove_substring(0, 0, 11, 1), LogicError::string_position_out_of_range);
    }
}


TEST(Table_SwapRowsThenMoveLastOverWithBacklinks)
{
    // Attempts to trigger bug where LinkColumn::swap_rows() would only swap its backlinks but forgot
    // to swap its own values
    Group g;
    TableRef t1 = g.add_table("t1");
    TableRef t2 = g.add_table("t2");
    t1->add_column(type_Int, "i");
    t2->add_column_link(type_Link, "l", *t1);

    t1->add_empty_row(2);
    t2->add_empty_row(2);

    t2->set_link(0, 0, 0);
    t2->set_link(0, 1, 1);

    t2->swap_rows(0, 1);
    t2->verify();
    t2->move_last_over(0);
    t2->verify();
}


TEST(Table_RowAccessor_Null)
{
    Table table;
    size_t col_bool = table.add_column(type_Bool, "bool", true);
    size_t col_int = table.add_column(type_Int, "int", true);
    size_t col_string = table.add_column(type_String, "string", true);
    size_t col_float = table.add_column(type_Float, "float", true);
    size_t col_double = table.add_column(type_Double, "double", true);
    size_t col_date = table.add_column(type_OldDateTime, "date", true);
    size_t col_binary = table.add_column(type_Binary, "binary", true);
    size_t col_timestamp = table.add_column(type_Timestamp, "timestamp", true);

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
        row.set_null(col_timestamp);
    }
    {
        table.add_empty_row();
        Row row = table[1];
        row.set_bool(col_bool, true);
        row.set_int(col_int, 1);
        row.set_string(col_string, "1");
        row.set_float(col_float, 1.0f);
        row.set_double(col_double, 1.0);
        row.set_olddatetime(col_date, OldDateTime(1));
        row.set_binary(col_binary, BinaryData("a"));
        row.set_timestamp(col_timestamp, Timestamp(1, 2));
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
        CHECK(row.is_null(col_timestamp));
    }

    {
        Row row = table[1];
        CHECK_EQUAL(true, row.get_bool(col_bool));
        CHECK_EQUAL(1, row.get_int(col_int));
        CHECK_EQUAL("1", row.get_string(col_string));
        CHECK_EQUAL(1.0, row.get_float(col_float));
        CHECK_EQUAL(1.0, row.get_double(col_double));
        CHECK_EQUAL(OldDateTime(1), row.get_olddatetime(col_date));
        CHECK_EQUAL(BinaryData("a"), row.get_binary(col_binary));
        CHECK_EQUAL(Timestamp(1, 2), row.get_timestamp(col_timestamp));
    }
}


// This triggers a severe bug in the Array::alloc() allocator in which its capacity-doubling
// scheme forgets to test of the doubling has overflowed the maximum allowed size of an
// array which is 2^24 - 1 bytes
TEST(Table_AllocatorCapacityBug)
{
    std::unique_ptr<char[]> buf(new char[20000000]);

    // First a simple trigger of `Assertion failed: value <= 0xFFFFFFL [26000016, 16777215]`
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref, true);

        c.add(BinaryData(buf.get(), 13000000));
        c.set(0, BinaryData(buf.get(), 14000000));

        c.destroy();
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
                bd = BinaryData(buf.get(), len);
                t.set_binary(0, row, bd);
            }
            else if (t.size() >= 4) {
                t.clear();
            }
        }
    }
}


// Exposes crash when setting a int, float or double that has its least significant bit set
TEST(Table_MixedCrashValues)
{
    GROUP_TEST_PATH(path);
    const char* encryption_key = nullptr;
    Group group(path, encryption_key, Group::mode_ReadWrite);
    TableRef table = group.add_table("t");
    table->add_column(type_Mixed, "m");
    table->add_empty_row(3);

    table->set_mixed(0, 0, Mixed(int64_t(-1)));
    table->set_mixed(0, 1, Mixed(2.0f));
    table->set_mixed(0, 2, Mixed(2.0));

    CHECK_EQUAL(table->get_mixed(0, 0).get_int(), int64_t(-1));
    CHECK_EQUAL(table->get_mixed(0, 1).get_float(), 2.0f);
    CHECK_EQUAL(table->get_mixed(0, 2).get_double(), 2.0);

    group.verify();
}


TEST(Table_MergeRows_Links)
{
    Group g;

    TableRef t0 = g.add_table("t0");
    TableRef t1 = g.add_table("t1");
    t0->add_column_link(type_Link, "link", *t1);
    t1->add_column(type_Int, "int");
    t0->add_empty_row(2);
    t1->add_empty_row(2);
    for (int i = 0; i < 2; ++i) {
        t0->set_link(0, i, i);
        t1->set_int(0, i, i);
    }
    t1->add_empty_row();

    Row replaced_row = t1->get(0);
    CHECK_EQUAL(t1->get_backlink_count(0, *t0, 0), 1);
    t1->merge_rows(0, 2);
    CHECK(replaced_row.is_attached());
    CHECK_EQUAL(t0->get_link(0, 0), 2);
    CHECK_EQUAL(t1->get_backlink_count(0, *t0, 0), 0);
}


TEST(Table_MergeRows_LinkLists)
{
    Group g;

    TableRef t0 = g.add_table("t0");
    TableRef t1 = g.add_table("t1");
    t0->add_column_link(type_LinkList, "linklist", *t1);
    t1->add_column(type_Int, "int");
    t0->add_empty_row(10);
    t1->add_empty_row(10);
    for (int i = 0; i < 10; ++i) {
        auto links = t0->get_linklist(0, i);
        links->add(i);
        links->add((i + 1) % 10);
        t1->set_int(0, i, i);
    }
    t1->add_empty_row();

    Row replaced_row = t1->get(0);
    CHECK_EQUAL(t1->get_backlink_count(0, *t0, 0), 2);
    t1->merge_rows(0, 10);
    CHECK(replaced_row.is_attached());
    CHECK_EQUAL(t1->get_backlink_count(0, *t0, 0), 0);
    CHECK_EQUAL(t0->get_linklist(0, 0)->size(), 2);
    CHECK_EQUAL(t0->get_linklist(0, 0)->get(0).get_index(), 10);
    CHECK_EQUAL(t0->get_linklist(0, 0)->get(1).get_index(), 1);
    CHECK_EQUAL(t0->get_linklist(0, 9)->size(), 2);
    CHECK_EQUAL(t0->get_linklist(0, 9)->get(0).get_index(), 9);
    CHECK_EQUAL(t0->get_linklist(0, 9)->get(1).get_index(), 10);
}

// Minimal test case causing an assertion error because
// backlink columns are storing stale values referencing
// their respective link column index. If a link column
// index changes, the backlink column accessors must also
// be updated.
TEST(Table_MinimalStaleLinkColumnIndex)
{
    Group g;
    TableRef t = g.add_table("table");
    t->add_column(type_Int, "int1");
    t->add_search_index(0);
    t->add_empty_row(2);
    t->set_int(0, 1, 4444);

    TableRef t2 = g.add_table("table2");
    t2->add_column(type_Int, "int_col");
    t2->add_column_link(type_Link, "link", *t);
    t2->remove_column(0);

    t->set_int_unique(0, 0, 4444); // crashed here

    CHECK_EQUAL(t->get_int(0, 0), 4444);
    CHECK_EQUAL(t->size(), 1);
}

// This test case is a simplified version of a bug revealed by fuzz testing
// set_int_unique triggers backlinks to update if the element to insert is
// not unique. The expected behaviour is that the new row containing the
// unique int will be removed and the old row will remain; this ensures
// uniques without throwing errors. This test was crashing (assert failed)
// when inserting a unique duplicate because backlink indices hadn't been
// updated after a column had been removed from the table containing the link.
TEST(Table_FuzzTestRevealed_SetUniqueAssert)
{
    Group g;
    g.add_table("string_index_test_table");
    g.get_table(0)->add_search_index(g.get_table(0)->add_column(DataType(0), "aa", true));
    g.get_table(0)->add_search_index(g.get_table(0)->add_column(DataType(0), "bb", true));
    g.get_table(0)->insert_column(0, DataType(0), "cc", true);
    g.get_table(0)->add_search_index(0);
    g.get_table(0)->insert_column_link(3, type_Link, "dd", *g.get_table(0));
    g.get_table(0)->add_empty_row(225);
    {
        TableRef t = g.get_table(0);
        t->remove_column(1);
    }
    {
        TableRef t = g.get_table(0);
        t->remove_column(0);
    }
    g.get_table(0)->add_empty_row(186);
    g.get_table(0)->find_first_int(0, 0);
    g.get_table(0)->set_int_unique(0, 255, 1);
    g.get_table(0)->find_first_int(0, 0);
    g.get_table(0)->set_null(0, 53);
    g.get_table(0)->set_int_unique(0, 97, 'l');
    g.get_table(0)->add_empty_row(85);
    g.get_table(0)->set_int_unique(0, 100, 'l'); // duplicate
    CHECK_EQUAL(g.get_table(0)->get_int(0, 97), 'l');
    CHECK_EQUAL(g.get_table(0)->get_int(0, 100), 0);
}

TEST(Table_InsertUniqueDuplicate_LinkedColumns)
{
    Group g;
    TableRef t = g.add_table("table");
    t->add_column(type_Int, "int1");
    t->add_search_index(0);
    t->add_empty_row(2);
    t->set_int_unique(0, 0, 42);
    t->set_int_unique(0, 1, 42);
    CHECK_EQUAL(t->size(), 1);
    CHECK_EQUAL(t->get_int(0, 0), 42);

    t->insert_column(0, type_String, "string1");
    t->add_search_index(0);
    t->add_empty_row(1);
    t->set_string_unique(0, 0, "fourty-two");
    t->set_string_unique(0, 1, "fourty-two");
    CHECK_EQUAL(t->size(), 1);
    CHECK_EQUAL(t->get_string(0, 0), "fourty-two");
    CHECK_EQUAL(t->get_int(1, 0), 42);

    TableRef t2 = g.add_table("table2");
    t2->add_column(type_Int, "int_col");
    t2->add_column(type_String, "string_col");
    t2->add_column_link(type_Link, "link", *t);
    t2->add_search_index(0);
    t2->add_search_index(1);
    t2->add_empty_row(2);
    t2->set_int_unique(0, 0, 43);
    t2->set_string_unique(1, 0, "fourty-three");
    t2->set_string_unique(1, 1, "FOURTY_THREE");
    t2->set_link(2, 0, 0);
    t2->set_int_unique(0, 1, 43); // deletes row 1, row 0 is winner

    CHECK_EQUAL(t2->size(), 1);
    CHECK_EQUAL(t2->get_int(0, 0), 43);
    CHECK_EQUAL(t2->get_string(1, 0), "fourty-three");
    CHECK_EQUAL(t2->get_link(2, 0), 0);

    t2->remove_column(0);
    t->insert_empty_row(0); // update t2 link through backlinks
    t->set_int(1, 0, 333);
    CHECK_EQUAL(t->get_int(1, 0), 333);
    CHECK_EQUAL(t->get_int(1, 1), 42);
    CHECK_EQUAL(t2->get_link(1, 0), 1); // bumped forward by insert at t(0), updated through backlinks

    using df = _impl::DescriptorFriend;
    DescriptorRef t2_descriptor = t2->get_descriptor();
    df::move_column(*t2_descriptor, 0, 1);
    CHECK_EQUAL(t2->get_link(0, 0), 1); // unchanged
    t->insert_empty_row(0);
    t->set_int(1, 0, 4444);
    CHECK_EQUAL(t2->get_link(0, 0), 2); // bumped forward via backlinks
    t2->remove_column(1);
    CHECK_EQUAL(t2->get_link(0, 0), 2); // unchanged
    t->insert_empty_row(0);             // update through backlinks
    t->set_int(1, 0, 55555);
    CHECK_EQUAL(t2->get_link(0, 0), 3);

    t->set_int_unique(1, 0, 4444);      // duplicate, row 1 wins, move_last_over(0)
    CHECK_EQUAL(t2->get_link(0, 0), 0); // changed by duplicate overwrite in linked table via backlinks

    t2->insert_column(0, type_Int, "type_Int col");
    CHECK_EQUAL(t2->get_link(1, 0), 0); // no change after insert col
    t->insert_empty_row(0);
    t->set_int(1, 0, 666666);
    CHECK_EQUAL(t2->get_link(1, 0), 1); // bumped forward via backlinks

    df::move_column(*t2_descriptor, 1, 0); // move backwards
    CHECK_EQUAL(t2->get_link(0, 0), 1);    // no change
    t->insert_empty_row(0);
    t->set_int(1, 0, 7777777);
    CHECK_EQUAL(t2->get_link(0, 0), 2); // bumped forward via backlinks
    t->remove(0);
    CHECK_EQUAL(t2->get_link(0, 0), 1); // bumped back via backlinks
}


TEST(Table_DetachedAccessor)
{
    Group group;
    TableRef table = group.add_table("table");
    table->add_column(type_Int, "i");
    table->add_column(type_String, "s");
    table->add_column(type_Binary, "b");
    table->add_column_link(type_Link, "l", *table);
    table->add_empty_row(2);
    group.remove_table("table");

    CHECK_LOGIC_ERROR(table->clear(), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->add_search_index(0), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->remove_search_index(0), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->merge_rows(0, 1), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->swap_rows(0, 1), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->set_string(1, 0, ""), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->set_string_unique(1, 0, ""), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->insert_substring(1, 0, 0, "x"), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->remove_substring(1, 0, 0), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->set_binary(2, 0, BinaryData()), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->set_link(3, 0, 0), LogicError::detached_accessor);
}

// This test reproduces a user reported assertion failure. The problem was
// due to BacklinkColumn::m_origin_column_ndx not being updated when the
// linked table removed/inserted columns (this happened on a migration)
TEST(Table_StaleLinkIndexOnTableRemove)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(realm::make_in_realm_history(path));
    SharedGroup sg_w(*hist, SharedGroupOptions(crypt_key()));
    Group& group_w = const_cast<Group&>(sg_w.begin_read());

    LangBindHelper::promote_to_write(sg_w);
    TableRef t = group_w.add_table("table1");
    t->add_column(type_Int, "int1");
    t->add_empty_row(2);

    TableRef t2 = group_w.add_table("table2");
    t2->add_column(type_Int, "int_col");
    t2->add_column_link(type_Link, "link", *t);
    t2->add_empty_row();
    t2->set_link(1, 0, 1);
    t2->remove_column(0); // after this call LinkColumnBase::m_column_ndx was incorrect
    t2->add_column(type_Int, "int_col2");

    // The stale backlink index would still be "1" which is now an integer column in t2
    // so the assertion in Spec::get_opposite_link_table() would fail when removing a link
    t->remove(1);

    CHECK_EQUAL(t->size(), 1);
    CHECK_EQUAL(t2->get_link(0, 0), realm::npos); // no link
}

TEST(Table_ColumnsSupportStringIndex)
{
    std::vector<DataType> all_types{type_Int,    type_Bool,        type_Float,     type_Double, type_String,
                                    type_Binary, type_OldDateTime, type_Timestamp, type_Table,  type_Mixed};

    std::vector<DataType> supports_index{type_Int, type_Bool, type_String, type_OldDateTime, type_Timestamp};

    Group g; // type_Link must be part of a group
    TableRef t = g.add_table("t1");
    for (auto it = all_types.begin(); it != all_types.end(); ++it) {
        t->add_column(*it, "");
        ColumnBase& col = _impl::TableFriend::get_column(*t, 0);
        bool does_support_index = col.supports_search_index();
        auto found_pos = std::find(supports_index.begin(), supports_index.end(), *it);
        CHECK_EQUAL(does_support_index, (found_pos != supports_index.end()));
        CHECK_EQUAL(does_support_index, (col.create_search_index() != nullptr));
        CHECK_EQUAL(does_support_index, col.has_search_index());
        col.destroy_search_index();
        CHECK(!col.has_search_index());
        if (does_support_index) {
            t->add_search_index(0);
        }
        else {
            CHECK_LOGIC_ERROR(t->add_search_index(0), LogicError::illegal_combination);
        }
        CHECK_EQUAL(does_support_index, t->has_search_index(0));
        t->remove_column(0);
    }

    // Check type_Link
    t->add_column_link(type_Link, "", *t);
    ColumnBase& link_col = _impl::TableFriend::get_column(*t, 0);
    CHECK(!link_col.supports_search_index());
    CHECK(link_col.create_search_index() == nullptr);
    CHECK(!link_col.has_search_index());
    CHECK_LOGIC_ERROR(t->add_search_index(0), LogicError::illegal_combination);
    t->remove_column(0);

    // Check type_LinkList
    t->add_column_link(type_LinkList, "", *t);
    ColumnBase& linklist_col = _impl::TableFriend::get_column(*t, 0);
    CHECK(!linklist_col.supports_search_index());
    CHECK(linklist_col.create_search_index() == nullptr);
    CHECK(!linklist_col.has_search_index());
    CHECK_LOGIC_ERROR(t->add_search_index(0), LogicError::illegal_combination);
    t->remove_column(0);

    // Check StringEnum
    t->add_column(type_String, "");
    bool force = true;
    t->optimize(force);
    ColumnBase& enum_col = _impl::TableFriend::get_column(*t, 0);
    CHECK(enum_col.supports_search_index());
    CHECK(enum_col.create_search_index() != nullptr);
    CHECK(enum_col.has_search_index());
    enum_col.destroy_search_index();
    CHECK(!enum_col.has_search_index());
    t->add_search_index(0);
    CHECK(enum_col.has_search_index());
    t->remove_column(0);
}

TEST(Table_addRowsToTableWithNoColumns)
{
    Group g; // type_Link must be part of a group
    TableRef t = g.add_table("t");

    CHECK_LOGIC_ERROR(t->add_empty_row(1), LogicError::table_has_no_columns);
    CHECK_LOGIC_ERROR(t->insert_empty_row(0), LogicError::table_has_no_columns);
    CHECK_EQUAL(t->size(), 0);
    t->add_column(type_String, "str_col");
    t->add_empty_row(1);
    CHECK_EQUAL(t->size(), 1);
    t->add_search_index(0);
    t->insert_empty_row(0);
    CHECK_EQUAL(t->size(), 2);
    t->remove_column(0);
    CHECK_EQUAL(t->size(), 0);
    CHECK_LOGIC_ERROR(t->add_empty_row(1), LogicError::table_has_no_columns);

    // Can add rows to a table with backlinks
    TableRef u = g.add_table("u");
    u->add_column_link(type_Link, "link from u to t", *t);
    CHECK_EQUAL(u->size(), 0);
    CHECK_EQUAL(t->size(), 0);
    t->add_empty_row(1);
    CHECK_EQUAL(t->size(), 1);
    u->remove_column(0);
    CHECK_EQUAL(u->size(), 0);
    CHECK_EQUAL(t->size(), 0);
    CHECK_LOGIC_ERROR(t->add_empty_row(1), LogicError::table_has_no_columns);

    // Do the exact same as above but with LinkLists
    u->add_column_link(type_LinkList, "link list from u to t", *t);
    CHECK_EQUAL(u->size(), 0);
    CHECK_EQUAL(t->size(), 0);
    t->add_empty_row(1);
    CHECK_EQUAL(t->size(), 1);
    u->remove_column(0);
    CHECK_EQUAL(u->size(), 0);
    CHECK_EQUAL(t->size(), 0);
    CHECK_LOGIC_ERROR(t->add_empty_row(1), LogicError::table_has_no_columns);

    // Check that links are nulled when connected table is cleared
    u->add_column_link(type_Link, "link from u to t", *t);
    u->add_empty_row(1);
    CHECK_EQUAL(u->size(), 1);
    CHECK_EQUAL(t->size(), 0);
    CHECK_LOGIC_ERROR(u->set_link(0, 0, 0), LogicError::target_row_index_out_of_range);
    CHECK(u->is_null_link(0, 0));
    CHECK_EQUAL(t->size(), 0);
    t->add_empty_row();
    u->set_link(0, 0, 0);
    CHECK_EQUAL(u->get_link(0, 0), 0);
    CHECK(!u->is_null_link(0, 0));
    CHECK_EQUAL(t->size(), 1);
    t->add_column(type_Int, "int column");
    CHECK_EQUAL(t->size(), 1);
    t->remove_column(0);
    CHECK_EQUAL(t->size(), 0);
    CHECK_EQUAL(u->size(), 1);
    CHECK(u->is_null_link(0, 0));
}

TEST(Table_getVersionCounterAfterRowAccessor)
{
    Table t;
    size_t col_bool = t.add_column(type_Bool, "bool", true);
    size_t col_int = t.add_column(type_Int, "int", true);
    size_t col_string = t.add_column(type_String, "string", true);
    size_t col_float = t.add_column(type_Float, "float", true);
    size_t col_double = t.add_column(type_Double, "double", true);
    size_t col_date = t.add_column(type_OldDateTime, "date", true);
    size_t col_binary = t.add_column(type_Binary, "binary", true);
    size_t col_timestamp = t.add_column(type_Timestamp, "timestamp", true);

    t.add_empty_row(1);

    int_fast64_t ver = t.get_version_counter();
    int_fast64_t newVer;

    auto check_ver_bump = [&]() {
        newVer = t.get_version_counter();
        CHECK_GREATER(newVer, ver);
        ver = newVer;
    };

    t.set_bool(col_bool, 0, true);
    check_ver_bump();

    t.set_int(col_int, 0, 42);
    check_ver_bump();

    t.set_string(col_string, 0, "foo");
    check_ver_bump();

    t.set_float(col_float, 0, 0.42f);
    check_ver_bump();

    t.set_double(col_double, 0, 0.42);
    check_ver_bump();

    t.set_olddatetime(col_date, 0, 1234);
    check_ver_bump();

    t.set_binary(col_binary, 0, BinaryData("binary", 7));
    check_ver_bump();

    t.set_timestamp(col_timestamp, 0, Timestamp(777, 888));
    check_ver_bump();

    t.set_null(0, 0);
    check_ver_bump();
}


// This test a bug where get_size_from_type_and_ref() returned off-by-one on nullable integer columns.
// It seems to be only invoked from Table::get_size_from_ref() which is fast static method that lets
// you find the size of a Table without having to create an instance of it. This seems to be only done
// on subtables, so the bug has not been triggered in public.
TEST_TYPES(Table_ColumnSizeFromRef, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    Group g;
    TableRef t = g.add_table("table");
    t->add_column(type_Int, "int", nullable_toggle);
    t->add_column(type_Bool, "bool", nullable_toggle);
    t->add_column(type_String, "string", nullable_toggle);
    t->add_column(type_Binary, "binary", nullable_toggle);
    t->add_column(type_Double, "double");
    t->add_column(type_Float, "float");
    t->add_column(type_Mixed, "mixed");
    t->add_column(type_Timestamp, "timestamp");
    t->add_column_link(type_Link, "link", *t);
    t->add_column_link(type_LinkList, "LinkList", *t);

    auto check_column_sizes = [this, &t](size_t num_rows) {
        t->clear();
        t->add_empty_row(num_rows);
        CHECK_EQUAL(t->size(), num_rows);
        using tf = _impl::TableFriend;
        Spec& t_spec = tf::get_spec(*t);
        size_t actual_num_cols = t_spec.get_column_count();
        for (size_t col_ndx = 0; col_ndx < actual_num_cols; ++col_ndx) {
            ColumnType col_type = t_spec.get_column_type(col_ndx);
            ColumnBase& base = tf::get_column(*t, col_ndx);
            ref_type col_ref = base.get_ref();
            bool nullable = (t_spec.get_column_attr(col_ndx) & col_attr_Nullable) == col_attr_Nullable;
            size_t col_size = ColumnBase::get_size_from_type_and_ref(col_type, col_ref, base.get_alloc(), nullable);
            CHECK_EQUAL(col_size, num_rows);
        }
    };

    // Test leafs
    check_column_sizes(REALM_MAX_BPNODE_SIZE - 1);

    // Test empty
    check_column_sizes(0);

    // Test internal nodes
    check_column_sizes(REALM_MAX_BPNODE_SIZE + 1);

    // Test on boundary for good measure
    check_column_sizes(REALM_MAX_BPNODE_SIZE);

    // Try with more levels in the tree
    check_column_sizes(10 * REALM_MAX_BPNODE_SIZE);
}


#endif // TEST_TABLE
