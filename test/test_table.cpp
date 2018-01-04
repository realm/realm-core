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
#include <set>
#include <chrono>

using namespace std::chrono;

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/lang_bind_helper.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/to_string.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_string.hpp>
#include <realm/array_timestamp.hpp>

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

namespace {

class TestTable01 : public realm::Table {
public:
    TestTable01(Allocator& a)
        : Table(a)
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
        Obj obj = table->create_object();

        CHECK(obj.get<String>("name").is_null());
    }

    {
        // Check that add_empty_row() adds empty string as default
        Group group;
        TableRef table = group.add_table("test");

        auto col = table->add_column(type_String, "name");
        CHECK(!table->is_nullable(col));

        Obj obj = table->create_object();
        CHECK(!obj.get<String>(col).is_null());

        // Test that inserting null in non-nullable column will throw
        CHECK_LOGIC_ERROR(obj.set_null(col), LogicError::column_not_nullable);
    }

    {
        // Check that add_empty_row() adds null integer as default
        Group group;
        TableRef table = group.add_table("table");
        auto col = table->add_column(type_Int, "age", true /*nullable*/);
        CHECK(table->is_nullable(col));

        Obj obj = table->create_object();
        CHECK(obj.is_null(col));
    }

    {
        // Check that add_empty_row() adds 0 integer as default.
        Group group;
        TableRef table = group.add_table("test");
        auto col = table->add_column(type_Int, "age");
        CHECK(!table->is_nullable(col));

        Obj obj = table->create_object();
        CHECK(!obj.is_null(col));
        CHECK_EQUAL(0, obj.get<Int>(col));

        // Check that inserting null in non-nullable column will throw
        CHECK_LOGIC_ERROR(obj.set_null(col), LogicError::column_not_nullable);
    }

    {
        // Check that add_empty_row() adds NULL binary as default
        Group group;
        TableRef table = group.add_table("test");

        auto col = table->add_column(type_Binary, "bin", true /*nullable*/);
        CHECK(table->is_nullable(col));

        Obj obj = table->create_object();
        CHECK(obj.is_null(col));
    }

    {
        // Check that add_empty_row() adds empty binary as default
        Group group;
        TableRef table = group.add_table("test");

        auto col = table->add_column(type_Binary, "name");
        CHECK(!table->is_nullable(col));

        Obj obj = table->create_object();
        CHECK(!obj.get<Binary>(col).is_null());

        // Test that inserting null in non-nullable column will throw
        CHECK_THROW_ANY(obj.set_null(col));
    }

    {
        // Check that link columns are nullable.
        Group group;
        TableRef target = group.add_table("target");
        TableRef table = group.add_table("table");

        auto col_int = target->add_column(type_Int, "int");
        auto col_link = table->add_column_link(type_Link, "link", *target);
        CHECK(table->is_nullable(col_link));
        CHECK(!target->is_nullable(col_int));
    }

    {
        // Check that linklist columns are not nullable.
        Group group;
        TableRef target = group.add_table("target");
        TableRef table = group.add_table("table");

        auto col_int = target->add_column(type_Int, "int");
        auto col_link = table->add_column_link(type_LinkList, "link", *target);
        CHECK(!table->is_nullable(col_link));
        CHECK(!target->is_nullable(col_int));
    }
}

TEST(Table_DeleteCrash)
{
    Group group;
    TableRef table = group.add_table("test");

    table->add_column(type_String, "name");
    table->add_column(type_Int, "age");

    Key k0 = table->create_object().set_all("Alice", 17).get_key();
    Key k1 = table->create_object().set_all("Bob", 50).get_key();
    table->create_object().set_all("Peter", 44);

    table->remove_object(k0);

    table->remove_object(k1);
}

TEST(Table_OptimizeCrash)
{
    // This will crash at the .add() method
    Table ttt;
    ttt.add_column(type_Int, "first");
    auto col = ttt.add_column(type_String, "second");
#ifdef LEGACY_TESTS
    ttt.optimize();
#endif // LEGACY_TESTS
    ttt.add_search_index(col);
    ttt.clear();
    ttt.create_object().set_all(1, "AA");
}

TEST(Table_DateTimeMinMax)
{
    Group g;
    TableRef table = g.add_table("test_table");

    auto col = table->add_column(type_Timestamp, "time", true);

    // We test different code paths of the internal Core minmax method. First a null value as initial "best candidate",
    // then non-null first. For each case we then try both a substitution of best candidate, then non-substitution. 4
    // permutations in total.

    std::vector<Obj> objs(3);
    objs[0] = table->create_object();
    objs[1] = table->create_object();
    objs[2] = table->create_object();

    objs[0].set_null(col);
    objs[1].set(col, Timestamp{0, 0});
    objs[2].set(col, Timestamp{2, 2});

    CHECK_EQUAL(table->maximum_timestamp(col), Timestamp(2, 2));
    CHECK_EQUAL(table->minimum_timestamp(col), Timestamp(0, 0));

    objs[0].set(col, Timestamp{0, 0});
    objs[1].set_null(col);
    objs[2].set(col, Timestamp{2, 2});

    Key idx; // tableview entry that points at the max/min value

    CHECK_EQUAL(table->maximum_timestamp(col, &idx), Timestamp(2, 2));
    CHECK_EQUAL(idx, objs[2].get_key());
    CHECK_EQUAL(table->minimum_timestamp(col, &idx), Timestamp(0, 0));
    CHECK_EQUAL(idx, objs[0].get_key());

    objs[0].set_null(col);
    objs[1].set(col, Timestamp{2, 2});
    objs[2].set(col, Timestamp{0, 0});

    CHECK_EQUAL(table->maximum_timestamp(col), Timestamp(2, 2));
    CHECK_EQUAL(table->minimum_timestamp(col), Timestamp(0, 0));

    objs[0].set(col, Timestamp{2, 2});
    objs[1].set_null(col);
    objs[2].set(col, Timestamp{0, 0});

    CHECK_EQUAL(table->maximum_timestamp(col, &idx), Timestamp(2, 2));
    CHECK_EQUAL(idx, objs[0].get_key());
    CHECK_EQUAL(table->minimum_timestamp(col, &idx), Timestamp(0, 0));
    CHECK_EQUAL(idx, objs[2].get_key());
}


TEST(Table_MinMaxSingleNullRow)
{
    // To illustrate/document behaviour
    Group g;
    TableRef table = g.add_table("test_table");

    auto date_col = table->add_column(type_Timestamp, "time", true);
    auto int_col = table->add_column(type_Int, "int", true);
    auto float_col = table->add_column(type_Float, "float", true);
    table->create_object();

    Key key;

    // NOTE: Return-values of method calls are undefined if you have only null-entries in the table.
    // The return-value is not necessarily a null-object. Always test the return_ndx argument!

    // Maximum
    {
        table->maximum_timestamp(date_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().maximum_timestamp(date_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().maximum_timestamp(date_col, &key); // max on query
        CHECK(key == null_key);

        table->maximum_int(int_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().maximum_int(int_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().maximum_int(int_col, &key); // max on query
        CHECK(key == null_key);

        table->maximum_float(float_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().maximum_float(float_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().maximum_float(float_col, &key); // max on query
        CHECK(key == null_key);

        table->create_object();

        CHECK(table->maximum_timestamp(date_col).is_null());               // max on table
        table->where().find_all().maximum_timestamp(date_col, &key);       // max on tableview
        CHECK(key == null_key);
        table->where().maximum_timestamp(date_col, &key); // max on query
        CHECK(key == null_key);
    }

    // Minimum
    {
        table->minimum_timestamp(date_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().minimum_timestamp(date_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().minimum_timestamp(date_col, &key); // max on query
        CHECK(key == null_key);

        table->minimum_int(int_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().minimum_int(int_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().minimum_int(int_col, &key); // max on query
        CHECK(key == null_key);

        table->minimum_float(float_col, &key); // max on table
        CHECK(key == null_key);
        table->where().find_all().minimum_float(float_col, &key); // max on tableview
        CHECK(key == null_key);
        table->where().minimum_float(float_col, &key); // max on query
        CHECK(key == null_key);

        table->create_object();

        CHECK(table->minimum_timestamp(date_col).is_null());               // max on table
        table->where().find_all().minimum_timestamp(date_col, &key);       // max on tableview
        CHECK(key == null_key);
        table->where().minimum_timestamp(date_col, &key); // max on query
        CHECK(key == null_key);
    }
}


TEST(TableView_AggregateBugs)
{
    // Tests against various aggregate bugs on TableViews: https://github.com/realm/realm-core/pull/2360
    {
        Table table;
        auto int_col = table.add_column(type_Int, "ints", true);
        auto double_col = table.add_column(type_Double, "doubles", true);

        table.create_object().set_all(1, 1.);
        table.create_object().set_all(2, 2.);
        table.create_object();
        table.create_object().set_all(42, 42.);

        auto tv = table.where().not_equal(int_col, 42).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_int(int_col), 2);

        // average == sum / rows, where rows does *not* include values with null.
        size_t vc; // number of non-null values that the average was computed from
        CHECK_APPROXIMATELY_EQUAL(table.average_int(int_col, &vc), double(1 + 2 + 42) / 3, 0.001);
        CHECK_EQUAL(vc, 3);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_EQUAL(table.average_int(int_col), table.where().average_int(int_col, &vc));
        CHECK_EQUAL(vc, 3);
        CHECK_EQUAL(table.average_int(int_col), table.where().find_all().average_int(int_col, &vc));
        CHECK_EQUAL(vc, 3);

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(int_col, 1).find_all().average_int(int_col, &vc),
                                  double(2 + 42) / 2, 0.001);
        CHECK_EQUAL(vc, 2);

        // Now doubles
        tv = table.where().not_equal(double_col, 42.).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_double(double_col), 2.);

        // average == sum / rows, where rows does *not* include values with null.
        CHECK_APPROXIMATELY_EQUAL(table.average_double(double_col, &vc), double(1. + 2. + 42.) / 3, 0.001);
        CHECK_EQUAL(vc, 3);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_APPROXIMATELY_EQUAL(table.average_double(double_col), table.where().average_double(double_col, &vc),
                                  0.001);
        CHECK_EQUAL(vc, 3);

        CHECK_APPROXIMATELY_EQUAL(table.average_double(double_col),
                                  table.where().find_all().average_double(double_col, &vc), 0.001);
        CHECK_EQUAL(vc, 3);

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(double_col, 1.).find_all().average_double(double_col, &vc),
                                  (2. + 42.) / 2, 0.001);
        CHECK_EQUAL(vc, 2);
    }

    // Same as above, with null entry first
    {
        Table table;
        auto int_col = table.add_column(type_Int, "ints", true);

        table.create_object();
        table.create_object().set_all(1);
        table.create_object().set_all(2);
        table.create_object().set_all(42);

        auto tv = table.where().not_equal(int_col, 42).find_all();
        CHECK_EQUAL(tv.size(), 3);
        CHECK_EQUAL(tv.maximum_int(int_col), 2);

        // average == sum / rows, where rows does *not* include values with null.
        CHECK_APPROXIMATELY_EQUAL(table.average_int(int_col), double(1 + 2 + 42) / 3, 0.001);

        // There are currently 3 ways of doing average: on tableview, table and query:
        CHECK_EQUAL(table.average_int(int_col), table.where().average_int(int_col));
        CHECK_EQUAL(table.average_int(int_col), table.where().find_all().average_int(int_col));

        // Core has an optimization where it executes average directly on the column if there
        // are no query conditions. Bypass that here.
        CHECK_APPROXIMATELY_EQUAL(table.where().not_equal(int_col, 1).find_all().average_int(int_col),
                                  double(2 + 42) / 2, 0.001);
    }
}


#ifdef LEGACY_TESTS
TEST(Table_AggregateFuzz)
{
    // Tests sum, avg, min, max on Table, TableView, Query, for types float, Timestamp, int
    for(int iter = 0; iter < 50 + 1000 * TEST_DURATION; iter++)
    {
        Group g;
        TableRef table = g.add_table("test_table");

        auto date_col = table->add_column(type_Timestamp, "time", true);
        auto int_col = table->add_column(type_Int, "int", true);
        auto float_col = table->add_column(type_Float, "float", true);

        size_t rows = size_t(fastrand(10));
        std::vector<Key> keys;
        table->create_objects(rows, keys);
        int64_t largest = 0;
        int64_t smallest = 0;
        Key largest_pos = null_key;
        Key smallest_pos = null_key;

        double avg = 0;
        int64_t sum = 0;
        size_t nulls = 0;

        // Create some rows with values and some rows with just nulls
        for (size_t t = 0; t < rows; t++) {
            bool null = (fastrand(1) == 0);
            if (!null) {
                int64_t value = fastrand(10);
                sum += value;
                if (largest_pos == null_key || value > largest) {
                    largest = value;
                    largest_pos = keys[t];
                }
                if (smallest_pos == null_key || value < smallest) {
                    smallest = value;
                    smallest_pos = keys[t];
                }
                table->get_object(keys[t]).set_all(Timestamp(value, 0), value, float(value));
            }
            else {
                nulls++;
            }
        }

        avg = double(sum) / (rows - nulls == 0 ? 1 : rows - nulls);

        Key key;
        size_t cnt;
        float f;
        int64_t i;
        Timestamp ts;

        // Test methods on Table
        {
            // Table::max
            key = 123;
            f = table->maximum_float(2, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->maximum_int(1, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->maximum_timestamp(0, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // Table::min
            key = 123;
            f = table->minimum_float(2, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->minimum_int(1, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->minimum_timestamp(0, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // Table::avg
            double d;

            // number of non-null values used in computing the avg or sum
            cnt = 123;

            // Table::avg
            d = table->average_float(2, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->average_int(1, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // Table::sum
            d = table->sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->sum_int(1);
            CHECK_EQUAL(i, sum);
        }

        // Test methods on TableView
        {
            // TableView::max
            key = 123;
            f = table->where().find_all().maximum_float(2, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->where().find_all().maximum_int(1, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->where().find_all().maximum_timestamp(0, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // TableView::min
            key = 123;
            f = table->where().find_all().minimum_float(2, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->where().find_all().minimum_int(1, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->where().find_all().minimum_timestamp(0, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            key = 123;

            // TableView::avg
            d = table->where().find_all().average_float(2, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->where().find_all().average_int(1, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table->where().find_all().sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->where().find_all().sum_int(1);
            CHECK_EQUAL(i, sum);

        }

        // Test methods on Query
        {
            // TableView::max
            key = 123;
            f = table->where().maximum_float(2, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->where().maximum_int(1, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table->where().maximum_timestamp(0, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // TableView::min
            key = 123;
            f = table->where().minimum_float(2, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->where().minimum_int(1, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table->where().minimum_timestamp(0, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            cnt = 123;

            // TableView::avg
            d = table->where().average_float(2, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->where().average_int(1, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table->where().sum_float(2);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->where().sum_int(1);
            CHECK_EQUAL(i, sum);
        }
    }
}
#endif

TEST(Table_ColumnNameTooLong)
{
    Group group;
    TableRef table = group.add_table("foo");
    const size_t buf_size = 64;
    char buf[buf_size];
    memset(buf, 'A', buf_size);
    CHECK_LOGIC_ERROR(table->add_column(type_Int, StringData(buf, buf_size)), LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->add_column_list(type_Int, StringData(buf, buf_size)), LogicError::column_name_too_long);
    CHECK_LOGIC_ERROR(table->add_column_link(type_Link, StringData(buf, buf_size), *table),
                      LogicError::column_name_too_long);

    table->add_column(type_Int, StringData(buf, buf_size - 1));
    memset(buf, 'B', buf_size); // Column names must be unique
    table->add_column_list(type_Int, StringData(buf, buf_size - 1));
    memset(buf, 'C', buf_size);
    table->add_column_link(type_Link, StringData(buf, buf_size - 1), *table);
}

#ifdef LEGACY_TESTS
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
#endif

TEST(Table_Floats)
{
    Table table;
    auto float_col = table.add_column(type_Float, "first");
    auto double_col = table.add_column(type_Double, "second");

    CHECK_EQUAL(type_Float, table.get_column_type(float_col));
    CHECK_EQUAL(type_Double, table.get_column_type(double_col));
    CHECK_EQUAL("first", table.get_column_name(float_col));
    CHECK_EQUAL("second", table.get_column_name(double_col));

    // Test adding a single empty row
    // and filling it with values
    Obj obj = table.create_object().set_all(1.12f, 102.13);

    CHECK_EQUAL(1.12f, obj.get<float>(float_col));
    CHECK_EQUAL(102.13, obj.get<double>(double_col));

    // Test adding multiple rows
    std::vector<Key> keys;
    table.create_objects(7, keys);
    for (size_t i = 0; i < 7; ++i) {
        table.get_object(keys[i]).set(float_col, 1.12f + 100 * i).set(double_col, 102.13 * 200 * i);
    }

    for (size_t i = 0; i < 7; ++i) {
        const float v1 = 1.12f + 100 * i;
        const double v2 = 102.13 * 200 * i;
        Obj o = table.get_object(keys[i]);
        CHECK_EQUAL(v1, o.get<float>(float_col));
        CHECK_EQUAL(v2, o.get<double>(double_col));
    }

    table.verify();
}

TEST(Table_Delete)
{
    Table table;

    auto col_int = table.add_column(type_Int, "ints");

    for (int i = 0; i < 10; ++i) {
        table.create_object(Key(i)).set(col_int, i);
    }

    table.remove_object(Key(0));
    table.remove_object(Key(4));
    table.remove_object(Key(7));

    CHECK_EQUAL(1, table.get_object(Key(1)).get<int64_t>(col_int));
    CHECK_EQUAL(2, table.get_object(Key(2)).get<int64_t>(col_int));
    CHECK_EQUAL(3, table.get_object(Key(3)).get<int64_t>(col_int));
    CHECK_EQUAL(5, table.get_object(Key(5)).get<int64_t>(col_int));
    CHECK_EQUAL(6, table.get_object(Key(6)).get<int64_t>(col_int));
    CHECK_EQUAL(8, table.get_object(Key(8)).get<int64_t>(col_int));
    CHECK_EQUAL(9, table.get_object(Key(9)).get<int64_t>(col_int));

#ifdef REALM_DEBUG
    table.verify();
#endif

    // Delete all items one at a time
    for (size_t i = 0; i < 10; ++i) {
        try {
            table.remove_object(Key(i));
        }
        catch (...) {
        }
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
}


namespace {

void setup_multi_table(Table& table, size_t rows, std::vector<Key>& keys, std::vector<ColKey>& column_keys)
{
    // Create table with all column types
    auto int_col = table.add_column(type_Int, "int");                        //  0
    column_keys.push_back(int_col);
    auto bool_col = table.add_column(type_Bool, "bool");                     //  1
    column_keys.push_back(bool_col);
    auto float_col = table.add_column(type_Float, "float");                  //  3
    column_keys.push_back(float_col);
    auto double_col = table.add_column(type_Double, "double");               //  4
    column_keys.push_back(double_col);
    auto string_col = table.add_column(type_String, "string");               //  5
    column_keys.push_back(string_col);
    auto string_long_col = table.add_column(type_String, "string_long");     //  6
    column_keys.push_back(string_long_col);
    auto string_big_col = table.add_column(type_String, "string_big_blobs"); //  7
    column_keys.push_back(string_big_col);
    auto string_enum_col = table.add_column(type_String, "string_enum");     //  8 - becomes StringEnumColumn
    column_keys.push_back(string_enum_col);
    auto bin_col = table.add_column(type_Binary, "binary");                  //  9
    column_keys.push_back(bin_col);
    auto int_null_col = table.add_column(type_Int, "int_null", true);        // 12, nullable = true
    column_keys.push_back(int_null_col);

    std::vector<std::string> strings;
    for (size_t i = 0; i < rows; ++i) {
        std::stringstream out;
        out << "string" << i;
        strings.push_back(out.str());
    }

    for (size_t i = 0; i < rows; ++i) {
        Obj obj = table.create_object();
        keys.push_back(obj.get_key());

        int64_t sign = (i % 2 == 0) ? 1 : -1;

        // int
        obj.set(int_col, int64_t(i * sign));

        if (i % 4 == 0) {
            obj.set_null(int_null_col);
        }
        else {
            obj.set(int_null_col, int64_t(i * sign));
        }
        // bool
        obj.set(bool_col, (i % 2 ? true : false));
        // float
        obj.set(float_col, 123.456f * sign);
        // double
        obj.set(double_col, 9876.54321 * sign);
        // strings
        std::string str_i(strings[i] + " very long string.........");
        obj.set(string_col, StringData(strings[i]));
        obj.set(string_long_col, StringData(str_i));
        switch (i % 2) {
            case 0: {
                std::string s = strings[i];
                s += " very long string.........";
                for (int j = 0; j != 4; ++j)
                    s += " big blobs big blobs big blobs"; // +30
                obj.set(string_big_col, StringData(s));
                break;
            }
            case 1:
                obj.set(string_big_col, StringData(""));
                break;
        }
        // enum
        switch (i % 3) {
            case 0:
                obj.set(string_enum_col, "enum1");
                break;
            case 1:
                obj.set(string_enum_col, "enum2");
                break;
            case 2:
                obj.set(string_enum_col, "enum3");
                break;
        }
        obj.set(bin_col, BinaryData("binary", 7));
    }

#ifdef LEGACY_TESTS
    // We also want a StringEnumColumn
    table.optimize();
#endif
}

} // anonymous namespace


TEST(Table_DeleteAllTypes)
{
    Table table;
    std::vector<Key> keys;
    std::vector<ColKey> column_keys;
    setup_multi_table(table, 15, keys, column_keys);

    // Test Deletes
    table.remove_object(keys[14]);
    table.remove_object(keys[0]);
    table.remove_object(keys[5]);

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


#ifdef LEGACY_TESTS
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
#endif

TEST(Table_MoveAllTypes)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    Table table;
    std::vector<Key> keys;
    std::vector<ColKey> column_keys;
    setup_multi_table(table, 15, keys, column_keys);
    table.add_search_index(column_keys[6]);
    while (!table.is_empty()) {
        size_t size = keys.size();
        auto it = keys.begin() + random.draw_int_mod(size);
        table.remove_object(*it);
        keys.erase(it);
        table.verify();
    }
}


#ifdef LEGACY_TESTS

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
#endif

TEST(Table_FindAllInt)
{
    Table table;

    auto col_int = table.add_column(type_Int, "integers");

    table.create_object(Key(0)).set(col_int, 10);
    table.create_object(Key(1)).set(col_int, 20);
    table.create_object(Key(2)).set(col_int, 10);
    table.create_object(Key(3)).set(col_int, 20);
    table.create_object(Key(4)).set(col_int, 10);
    table.create_object(Key(5)).set(col_int, 20);
    table.create_object(Key(6)).set(col_int, 10);
    table.create_object(Key(7)).set(col_int, 20);
    table.create_object(Key(8)).set(col_int, 10);
    table.create_object(Key(9)).set(col_int, 20);

    // Search for a value that does not exits
    auto v0 = table.find_all_int(col_int, 5);
    CHECK_EQUAL(0, v0.size());

    // Search for a value with several matches
    auto v = table.find_all_int(col_int, 20);

    CHECK_EQUAL(5, v.size());
    CHECK_EQUAL(Key(1), v.get_key(0));
    CHECK_EQUAL(Key(3), v.get_key(1));
    CHECK_EQUAL(Key(5), v.get_key(2));
    CHECK_EQUAL(Key(7), v.get_key(3));
    CHECK_EQUAL(Key(9), v.get_key(4));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_SortedInt)
{
    Table table;

    auto col_int = table.add_column(type_Int, "integers");

    table.create_object(Key(0)).set(col_int, 10); // 0: 4
    table.create_object(Key(1)).set(col_int, 20); // 1: 7
    table.create_object(Key(2)).set(col_int, 0);  // 2: 0
    table.create_object(Key(3)).set(col_int, 40); // 3: 8
    table.create_object(Key(4)).set(col_int, 15); // 4: 6
    table.create_object(Key(5)).set(col_int, 11); // 5: 5
    table.create_object(Key(6)).set(col_int, 6);  // 6: 3
    table.create_object(Key(7)).set(col_int, 4);  // 7: 2
    table.create_object(Key(8)).set(col_int, 99); // 8: 9
    table.create_object(Key(9)).set(col_int, 2);  // 9: 1

    // Search for a value that does not exits
    auto v = table.get_sorted_view(col_int);
    CHECK_EQUAL(table.size(), v.size());

    CHECK_EQUAL(Key(2), v.get_key(0));
    CHECK_EQUAL(Key(9), v.get_key(1));
    CHECK_EQUAL(Key(7), v.get_key(2));
    CHECK_EQUAL(Key(6), v.get_key(3));
    CHECK_EQUAL(Key(0), v.get_key(4));
    CHECK_EQUAL(Key(5), v.get_key(5));
    CHECK_EQUAL(Key(4), v.get_key(6));
    CHECK_EQUAL(Key(1), v.get_key(7));
    CHECK_EQUAL(Key(3), v.get_key(8));
    CHECK_EQUAL(Key(8), v.get_key(9));

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_Sorted_Query_where)
{
    Table table;

    auto col_dummy = table.add_column(type_Int, "dummmy");
    auto col_int = table.add_column(type_Int, "integers");
    auto col_bool = table.add_column(type_Bool, "booleans");

    table.create_object(Key(0)).set(col_int, 10).set(col_bool, true);  // 0: 4
    table.create_object(Key(1)).set(col_int, 20).set(col_bool, false); // 1: 7
    table.create_object(Key(2)).set(col_int, 0).set(col_bool, false);  // 2: 0
    table.create_object(Key(3)).set(col_int, 40).set(col_bool, false); // 3: 8
    table.create_object(Key(4)).set(col_int, 15).set(col_bool, false); // 4: 6
    table.create_object(Key(5)).set(col_int, 11).set(col_bool, true);  // 5: 5
    table.create_object(Key(6)).set(col_int, 6).set(col_bool, true);   // 6: 3
    table.create_object(Key(7)).set(col_int, 4).set(col_bool, true);   // 7: 2
    table.create_object(Key(8)).set(col_int, 99).set(col_bool, true);  // 8: 9
    table.create_object(Key(9)).set(col_int, 2).set(col_bool, true);   // 9: 1

    // Get a view containing the complete table
    auto v = table.find_all_int(col_dummy, 0);
    CHECK_EQUAL(table.size(), v.size());

    // Count booleans
    size_t count_view = table.where(&v).equal(col_bool, false).count();
    CHECK_EQUAL(4, count_view);

    auto v_sorted = table.get_sorted_view(col_int);
    CHECK_EQUAL(table.size(), v_sorted.size());

#ifdef REALM_DEBUG
    table.verify();
#endif
}

#ifdef LEGACY_TESTS
TEST(Table_Multi_Sort)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_Int, "second");

    table.create_object(Key(0)).set_all(1, 10);
    table.create_object(Key(1)).set_all(2, 10);
    table.create_object(Key(2)).set_all(0, 10);
    table.create_object(Key(3)).set_all(2, 14);
    table.create_object(Key(4)).set_all(1, 14);

    std::vector<std::vector<size_t>> col_ndx1 = {{0}, {1}};
    std::vector<bool> asc = {true, true};

    // (0, 10); (1, 10); (1, 14); (2, 10); (2; 14)
    TableView v_sorted1 = table.get_sorted_view(SortDescriptor{table, col_ndx1, asc});
    CHECK_EQUAL(table.size(), v_sorted1.size());
    CHECK_EQUAL(Key(2), v_sorted1.get_key(0));
    CHECK_EQUAL(Key(0), v_sorted1.get_key(1));
    CHECK_EQUAL(Key(4), v_sorted1.get_key(2));
    CHECK_EQUAL(Key(1), v_sorted1.get_key(3));
    CHECK_EQUAL(Key(3), v_sorted1.get_key(4));

    std::vector<std::vector<size_t>> col_ndx2 = {{1}, {0}};

    // (0, 10); (1, 10); (2, 10); (1, 14); (2, 14)
    TableView v_sorted2 = table.get_sorted_view(SortDescriptor{table, col_ndx2, asc});
    CHECK_EQUAL(table.size(), v_sorted2.size());
    CHECK_EQUAL(Key(2), v_sorted2.get_key(0));
    CHECK_EQUAL(Key(0), v_sorted2.get_key(1));
    CHECK_EQUAL(Key(1), v_sorted2.get_key(2));
    CHECK_EQUAL(Key(4), v_sorted2.get_key(3));
    CHECK_EQUAL(Key(3), v_sorted2.get_key(4));
}
#endif

TEST(Table_IndexString)
{
    Table table;
    table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    Key k0 = table.create_object().set_all(int(Mon), "jeff").get_key();
    Key k1 = table.create_object().set_all(int(Tue), "jim").get_key();
    table.create_object().set_all(int(Wed), "jennifer");
    table.create_object().set_all(int(Thu), "john");
    table.create_object().set_all(int(Fri), "jimmy");
    Key k5 = table.create_object().set_all(int(Sat), "jimbo").get_key();
    Key k6 = table.create_object().set_all(int(Sun), "johnny").get_key();
    table.create_object().set_all(int(Mon), "jennifer"); // duplicate

    table.add_search_index(col_str);
    CHECK(table.has_search_index(col_str));

    Key r1 = table.find_first_string(col_str, "jimmi");
    CHECK_EQUAL(null_key, r1);

    Key r2 = table.find_first_string(col_str, "jeff");
    Key r3 = table.find_first_string(col_str, "jim");
    Key r4 = table.find_first_string(col_str, "jimbo");
    Key r5 = table.find_first_string(col_str, "johnny");
    CHECK_EQUAL(k0, r2);
    CHECK_EQUAL(k1, r3);
    CHECK_EQUAL(k5, r4);
    CHECK_EQUAL(k6, r5);

    const size_t c1 = table.count_string(col_str, "jennifer");
    CHECK_EQUAL(2, c1);
}


TEST(Table_IndexStringTwice)
{
    Table table;
    table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    table.create_object().set_all(int(Mon), "jeff");
    table.create_object().set_all(int(Tue), "jim");
    table.create_object().set_all(int(Wed), "jennifer");
    table.create_object().set_all(int(Thu), "john");
    table.create_object().set_all(int(Fri), "jimmy");
    table.create_object().set_all(int(Sat), "jimbo");
    table.create_object().set_all(int(Sun), "johnny");
    table.create_object().set_all(int(Mon), "jennifer"); // duplicate

    table.add_search_index(col_str);
    CHECK_EQUAL(true, table.has_search_index(col_str));
    table.add_search_index(col_str);
    CHECK_EQUAL(true, table.has_search_index(col_str));
}


// Tests Table part of index on Int, OldDateTime and Bool columns. For a more exhaustive
// test of the integer index (bypassing Table), see test_index_string.cpp)
TEST(Table_IndexInteger)
{
    Table table;
    Key k;

    auto col_int = table.add_column(type_Int, "ints");
    auto col_date = table.add_column(type_Timestamp, "date");
    auto col_bool = table.add_column(type_Bool, "booleans");

    std::vector<Key> keys;
    table.create_objects(13, keys);

    table.get_object(keys[0]).set(col_int, 3);  // 0
    table.get_object(keys[1]).set(col_int, 1);  // 1
    table.get_object(keys[2]).set(col_int, 2);  // 2
    table.get_object(keys[3]).set(col_int, 2);  // 3
    table.get_object(keys[4]).set(col_int, 2);  // 4
    table.get_object(keys[5]).set(col_int, 3);  // 5
    table.get_object(keys[6]).set(col_int, 3);  // 6
    table.get_object(keys[7]).set(col_int, 2);  // 7
    table.get_object(keys[8]).set(col_int, 4);  // 8
    table.get_object(keys[9]).set(col_int, 2);  // 9
    table.get_object(keys[10]).set(col_int, 6); // 10
    table.get_object(keys[11]).set(col_int, 2); // 11
    table.get_object(keys[12]).set(col_int, 3); // 12

    table.add_search_index(col_int);
    CHECK(table.has_search_index(col_int));
    table.add_search_index(col_date);
    CHECK(table.has_search_index(col_date));
    table.add_search_index(col_bool);
    CHECK(table.has_search_index(col_bool));

    table.get_object(keys[10]).set(col_date, Timestamp(43, 0));
    k = table.find_first_timestamp(col_date, Timestamp(43, 0));
    CHECK_EQUAL(keys[10], k);

    table.get_object(keys[11]).set(col_bool, true);
    k = table.find_first_bool(col_bool, true);
    CHECK_EQUAL(keys[11], k);

    k = table.find_first_int(col_int, 11);
    CHECK_EQUAL(null_key, k);

    k = table.find_first_int(col_int, 3);
    CHECK_EQUAL(keys[0], k);

    k = table.find_first_int(col_int, 4);
    CHECK_EQUAL(keys[8], k);

    TableView tv = table.find_all_int(col_int, 2);
    CHECK_EQUAL(6, tv.size());

    CHECK_EQUAL(keys[2], tv[0].get_key());
    CHECK_EQUAL(keys[3], tv[1].get_key());
    CHECK_EQUAL(keys[4], tv[2].get_key());
    CHECK_EQUAL(keys[7], tv[3].get_key());
    CHECK_EQUAL(keys[9], tv[4].get_key());
    CHECK_EQUAL(keys[11], tv[5].get_key());
}


TEST(Table_AddInt)
{
    Table t;
    auto col_int = t.add_column(type_Int, "i");
    auto col_int_null = t.add_column(type_Int, "ni", /*nullable*/ true);
    Obj obj = t.create_object();

    obj.add_int(col_int, 1);
    CHECK_EQUAL(obj.get<Int>(col_int), 1);

    // Check that signed integers wrap around. This invariant is necessary for
    // full commutativity.
    obj.add_int(col_int, Table::max_integer);
    CHECK_EQUAL(obj.get<Int>(col_int), Table::min_integer);
    obj.add_int(col_int, -1);
    CHECK_EQUAL(obj.get<Int>(col_int), Table::max_integer);

    // add_int() has no effect on a NULL
    CHECK(obj.is_null(col_int_null));
    CHECK_LOGIC_ERROR(obj.add_int(col_int_null, 123), LogicError::illegal_combination);
}

TEST(Table_Distinct)
{
    Table table;
    auto col_int = table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    Key k0 = table.create_object().set_all(int(Mon), "A").get_key();
    Key k1 = table.create_object().set_all(int(Tue), "B").get_key();
    Key k2 = table.create_object().set_all(int(Wed), "C").get_key();
    Key k3 = table.create_object().set_all(int(Thu), "B").get_key();
    Key k4 = table.create_object().set_all(int(Fri), "C").get_key();
    Key k5 = table.create_object().set_all(int(Sat), "D").get_key();
    Key k6 = table.create_object().set_all(int(Sun), "D").get_key();
    table.create_object().set_all(int(Mon), "D");

    table.add_search_index(col_int);
    CHECK(table.has_search_index(col_int));

    auto view = table.get_distinct_view(col_int);

    CHECK_EQUAL(7, view.size());
    CHECK_EQUAL(k0, view.get_key(0));
    CHECK_EQUAL(k1, view.get_key(1));
    CHECK_EQUAL(k2, view.get_key(2));
    CHECK_EQUAL(k3, view.get_key(3));
    CHECK_EQUAL(k4, view.get_key(4));
    CHECK_EQUAL(k5, view.get_key(5));
    CHECK_EQUAL(k6, view.get_key(6));

    table.add_search_index(col_str);
    CHECK(table.has_search_index(col_str));

    view = table.get_distinct_view(col_str);

    CHECK_EQUAL(4, view.size());
    CHECK_EQUAL(k0, view.get_key(0));
    CHECK_EQUAL(k1, view.get_key(1));
    CHECK_EQUAL(k2, view.get_key(2));
    CHECK_EQUAL(k5, view.get_key(3));
}


TEST(Table_DistinctBool)
{
    Table table;
    auto col_bool = table.add_column(type_Bool, "first");

    Key k0 = table.create_object().set(col_bool, true).get_key();
    Key k1 = table.create_object().set(col_bool, false).get_key();
    table.create_object().set(col_bool, true);
    table.create_object().set(col_bool, false);

    table.add_search_index(col_bool);
    CHECK(table.has_search_index(col_bool));

    TableView view = table.get_distinct_view(col_bool);

    CHECK_EQUAL(2, view.size());
    CHECK_EQUAL(k0, view.get_key(1));
    CHECK_EQUAL(k1, view.get_key(0));
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


TEST(Table_DistinctTimestamp)
{
    Table table;
    auto col_date = table.add_column(type_Timestamp, "first");

    table.create_object().set(col_date, Timestamp(0, 0));
    table.create_object().set(col_date, Timestamp(1, 0));
    table.create_object().set(col_date, Timestamp(3, 0));
    table.create_object().set(col_date, Timestamp(3, 0));

    table.add_search_index(col_date);
    CHECK(table.has_search_index(col_date));

    TableView view = table.get_distinct_view(col_date);
    CHECK_EQUAL(3, view.size());
}


TEST(Table_DistinctFromPersistedTable)
{
    GROUP_TEST_PATH(path);

    {
        Group group;
        TableRef table = group.add_table("table");
        auto col = table->add_column(type_Int, "first");

        table->create_object().set(col, 1);
        table->create_object().set(col, 2);
        table->create_object().set(col, 3);
        table->create_object().set(col, 3);

        table->add_search_index(col);
        CHECK(table->has_search_index(col));
        group.write(path);
    }

    {
        Group group(path, 0, Group::mode_ReadOnly);
        TableRef table = group.get_table("table");
        auto col = table->get_column_key("first");
        TableView view = table->get_distinct_view(col);

        CHECK_EQUAL(3, view.size());
        CHECK_EQUAL(table->get_object(view.get_key(0)).get<Int>(col), 1);
        CHECK_EQUAL(table->get_object(view.get_key(1)).get<Int>(col), 2);
        CHECK_EQUAL(table->get_object(view.get_key(2)).get<Int>(col), 3);
    }
}


TEST(Table_IndexInt)
{
    Table table;
    auto col = table.add_column(type_Int, "first");

    Key k0 = table.create_object().set(col, 1).get_key();
    Key k1 = table.create_object().set(col, 15).get_key();
    Key k2 = table.create_object().set(col, 10).get_key();
    Key k3 = table.create_object().set(col, 20).get_key();
    Key k4 = table.create_object().set(col, 11).get_key();
    Key k5 = table.create_object().set(col, 45).get_key();
    Key k6 = table.create_object().set(col, 10).get_key();
    Key k7 = table.create_object().set(col, 0).get_key();
    Key k8 = table.create_object().set(col, 30).get_key();
    Key k9 = table.create_object().set(col, 9).get_key();

    // Create index for column two
    table.add_search_index(col);

    // Search for a value that does not exits
    Key k = table.find_first_int(col, 2);
    CHECK_EQUAL(null_key, k);

    // Find existing values
    CHECK_EQUAL(k0, table.find_first_int(col, 1));
    CHECK_EQUAL(k1, table.find_first_int(col, 15));
    CHECK_EQUAL(k2, table.find_first_int(col, 10));
    CHECK_EQUAL(k3, table.find_first_int(col, 20));
    CHECK_EQUAL(k4, table.find_first_int(col, 11));
    CHECK_EQUAL(k5, table.find_first_int(col, 45));
    // CHECK_EQUAL(6, table.find_first_int(col, 10)); // only finds first match
    CHECK_EQUAL(k7, table.find_first_int(col, 0));
    CHECK_EQUAL(k8, table.find_first_int(col, 30));
    CHECK_EQUAL(k9, table.find_first_int(col, 9));

    // Change some values
    table.get_object(k2).set(col, 13);
    table.get_object(k9).set(col, 100);

    CHECK_EQUAL(k0, table.find_first_int(col, 1));
    CHECK_EQUAL(k1, table.find_first_int(col, 15));
    CHECK_EQUAL(k2, table.find_first_int(col, 13));
    CHECK_EQUAL(k3, table.find_first_int(col, 20));
    CHECK_EQUAL(k4, table.find_first_int(col, 11));
    CHECK_EQUAL(k5, table.find_first_int(col, 45));
    CHECK_EQUAL(k6, table.find_first_int(col, 10));
    CHECK_EQUAL(k7, table.find_first_int(col, 0));
    CHECK_EQUAL(k8, table.find_first_int(col, 30));
    CHECK_EQUAL(k9, table.find_first_int(col, 100));

    // Insert values
    Key k10 = table.create_object().set(col, 29).get_key();
    // TODO: More than add

    CHECK_EQUAL(k0, table.find_first_int(col, 1));
    CHECK_EQUAL(k1, table.find_first_int(col, 15));
    CHECK_EQUAL(k2, table.find_first_int(col, 13));
    CHECK_EQUAL(k3, table.find_first_int(col, 20));
    CHECK_EQUAL(k4, table.find_first_int(col, 11));
    CHECK_EQUAL(k5, table.find_first_int(col, 45));
    CHECK_EQUAL(k6, table.find_first_int(col, 10));
    CHECK_EQUAL(k7, table.find_first_int(col, 0));
    CHECK_EQUAL(k8, table.find_first_int(col, 30));
    CHECK_EQUAL(k9, table.find_first_int(col, 100));
    CHECK_EQUAL(k10, table.find_first_int(col, 29));

    // Delete some values
    table.remove_object(k0);
    table.remove_object(k5);
    table.remove_object(k8);

    CHECK_EQUAL(null_key, table.find_first_int(col, 1));
    CHECK_EQUAL(k1, table.find_first_int(col, 15));
    CHECK_EQUAL(k2, table.find_first_int(col, 13));
    CHECK_EQUAL(k3, table.find_first_int(col, 20));
    CHECK_EQUAL(k4, table.find_first_int(col, 11));
    CHECK_EQUAL(null_key, table.find_first_int(col, 45));
    CHECK_EQUAL(k6, table.find_first_int(col, 10));
    CHECK_EQUAL(k7, table.find_first_int(col, 0));
    CHECK_EQUAL(null_key, table.find_first_int(col, 30));
    CHECK_EQUAL(k9, table.find_first_int(col, 100));
    CHECK_EQUAL(k10, table.find_first_int(col, 29));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

#ifdef LEGACY_TESTS

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
#endif // LEGACY_TESTS


TEST(Table_SlabAlloc)
{
    SlabAlloc alloc;
    alloc.attach_empty();
    Table table(alloc);

    auto col_int0 = table.add_column(type_Int, "int0");
    auto col_int1 = table.add_column(type_Int, "int1");
    auto col_bool = table.add_column(type_Bool, "bool");
    auto col_int2 = table.add_column(type_Int, "int2");

    Obj obj = table.create_object().set_all(0, 10, true, int(Wed));
    CHECK_EQUAL(0, obj.get<Int>(col_int0));
    CHECK_EQUAL(10, obj.get<Int>(col_int1));
    CHECK_EQUAL(true, obj.get<Bool>(col_bool));
    CHECK_EQUAL(Wed, obj.get<Int>(col_int2));

    // Add some more rows
    table.create_object().set_all(1, 10, true, int(Wed));
    Key k0 = table.create_object().set_all(2, 20, true, int(Wed)).get_key();
    table.create_object().set_all(3, 10, true, int(Wed));
    Key k1 = table.create_object().set_all(4, 20, true, int(Wed)).get_key();
    table.create_object().set_all(5, 10, true, int(Wed));

    // Delete some rows
    table.remove_object(k0);
    table.remove_object(k1);

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_NullInEnum)
{
    Group group;
    TableRef table = group.add_table("test");
    auto col = table->add_column(type_String, "second", true);

    for (size_t c = 0; c < 100; c++) {
        table->create_object().set(col, "hello");
    }

    size_t r;

    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(100, r);

    Obj obj50 = table->get_object(Key(50));
    obj50.set<String>(col, realm::null());
    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(99, r);

#ifdef LEGACY_TESTS
    table->optimize();
#endif

    obj50.set<String>(col, realm::null());
    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(99, r);

    obj50.set<String>(col, "hello");
    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(100, r);

    obj50.set<String>(col, realm::null());
    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(99, r);

    r = table->where().equal(col, realm::null()).count();
    CHECK_EQUAL(1, r);

    table->get_object(Key(55)).set(col, realm::null());
    r = table->where().equal(col, realm::null()).count();
    CHECK_EQUAL(2, r);

    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(98, r);

    table->remove_object(Key(55));
    r = table->where().equal(col, realm::null()).count();
    CHECK_EQUAL(1, r);
}


TEST(Table_DateAndBinary)
{
    Table t;
    auto col_date = t.add_column(type_Timestamp, "date");
    auto col_bin = t.add_column(type_Binary, "bin");

    const size_t size = 10;
    char data[size];
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>(i);
    t.create_object().set_all(Timestamp(8, 0), BinaryData(data, size));
    Obj& obj = *t.begin();
    CHECK_EQUAL(obj.get<Timestamp>(col_date), Timestamp(8, 0));
    BinaryData bin = obj.get<Binary>(col_bin);
    CHECK_EQUAL(bin.size(), size);
    CHECK(std::equal(bin.data(), bin.data() + size, data));

    // Test that 64-bit dates are preserved
    Timestamp date(std::numeric_limits<int64_t>::max() - 400, 0);
    obj.set(col_date, date);
    CHECK_EQUAL(obj.get<Timestamp>(col_date), date);
}

#if TEST_DURATION > 0
#define TBL_SIZE REALM_MAX_BPNODE_SIZE * 10
#else
#define TBL_SIZE 10
#endif // TEST_DURATION

TEST(Table_Aggregates)
{
    Table table;
    auto int_col = table.add_column(type_Int, "c_int");
    auto float_col = table.add_column(type_Float, "c_float");
    auto double_col = table.add_column(type_Double, "c_double");
    auto str_col = table.add_column(type_String, "c_string");
    int64_t i_sum = 0;
    double f_sum = 0;
    double d_sum = 0;

    for (int i = 0; i < TBL_SIZE; i++) {
        table.create_object().set_all(5987654, 4.0f, 3.0, "Hello");
        i_sum += 5987654;
        f_sum += 4.0f;
        d_sum += 3.0;
    }
    table.create_object().set_all(1, 1.1f, 1.2, "Hi");
    table.create_object().set_all(987654321, 11.0f, 12.0, "Goodbye");
    table.create_object().set_all(5, 4.0f, 3.0, "Hey");
    i_sum += 1 + 987654321 + 5;
    f_sum += double(1.1f) + double(11.0f) + double(4.0f);
    d_sum += 1.2 + 12.0 + 3.0;
    double size = TBL_SIZE + 3;

    double epsilon = std::numeric_limits<double>::epsilon();

    // count
    CHECK_EQUAL(1, table.count_int(int_col, 987654321));
    CHECK_EQUAL(1, table.count_float(float_col, 11.0f));
    CHECK_EQUAL(1, table.count_double(double_col, 12.0));
    CHECK_EQUAL(1, table.count_string(str_col, "Goodbye"));
    // minimum
    CHECK_EQUAL(1, table.minimum_int(int_col));
    CHECK_EQUAL(1.1f, table.minimum_float(float_col));
    CHECK_EQUAL(1.2, table.minimum_double(double_col));
    // maximum
    CHECK_EQUAL(987654321, table.maximum_int(int_col));
    CHECK_EQUAL(11.0f, table.maximum_float(float_col));
    CHECK_EQUAL(12.0, table.maximum_double(double_col));
    // sum
    CHECK_APPROXIMATELY_EQUAL(double(i_sum), double(table.sum_int(int_col)), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum, table.sum_float(float_col), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum, table.sum_double(double_col), 10 * epsilon);
    // average
    CHECK_APPROXIMATELY_EQUAL(i_sum / size, table.average_int(int_col), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum / size, table.average_float(float_col), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum / size, table.average_double(double_col), 10 * epsilon);
}

TEST(Table_Aggregates2)
{
    Table table;
    auto int_col = table.add_column(type_Int, "c_count");
    int c = -420;
    int s = 0;
    while (c < -20) {
        table.create_object().set(int_col, c);
        s += c;
        c++;
    }

    CHECK_EQUAL(-420, table.minimum_int(int_col));
    CHECK_EQUAL(-21, table.maximum_int(int_col));
    CHECK_EQUAL(s, table.sum_int(int_col));
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

        auto col_price = table->add_column(type_Int, "Price", nullable);
        auto col_shipping = table->add_column(type_Float, "Shipping", nullable);
        auto col_rating = table->add_column(type_Double, "Rating", nullable);
        auto col_date = table->add_column(type_Timestamp, "Delivery date", nullable);

        Obj obj0 = table->create_object(Key(0));
        Obj obj1 = table->create_object(Key(1));
        Obj obj2 = table->create_object(Key(2));

        obj0.set(col_price, 1);
        // table->set_null(0, 1);
        obj2.set(col_price, 3);

        // table->set_null(1, 0);
        // table->set_null(1, 1);
        obj2.set(col_shipping, 30.f);

        obj0.set(col_rating, 1.1);
        obj1.set(col_rating, 2.2);
        // table->set_null(2, 2);

        obj0.set(col_date, Timestamp(2, 2));
        // table->set_null(4, 1);
        obj2.set(col_date, Timestamp(6, 6));

        size_t count;
        Key pos;
        if (nullable) {
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(col_price), 3);
            CHECK_EQUAL(table->maximum_int(col_price, &pos), 3);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_float(col_shipping), 30.f);
            CHECK_EQUAL(table->maximum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_double(col_rating), 2.2);
            CHECK_EQUAL(table->maximum_double(col_rating, &pos), 2.2);
            CHECK_EQUAL(pos, Key(1));

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(col_date), Timestamp(6, 6));
            CHECK_EQUAL(table->maximum_timestamp(col_date, &pos), Timestamp(6, 6));
            CHECK_EQUAL(pos, Key(2));

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(col_price), 1);
            CHECK_EQUAL(table->minimum_int(col_price, &pos), 1);
            CHECK_EQUAL(pos, Key(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_float(col_shipping), 30.f);
            CHECK_EQUAL(table->minimum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            CHECK_EQUAL(table->minimum_double(col_rating), 1.1);
            CHECK_EQUAL(table->minimum_double(col_rating, &pos), 1.1);
            CHECK_EQUAL(pos, Key(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_timestamp(col_date), Timestamp(2, 2));
            CHECK_EQUAL(table->minimum_timestamp(col_date, &pos), Timestamp(2, 2));
            CHECK_EQUAL(pos, Key(0));

            // average
            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_int(col_price), (1 + 3) / 2., 0.01);
            CHECK_APPROXIMATELY_EQUAL(table->average_int(col_price, &count), (1 + 3) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            count = 123;
            CHECK_EQUAL(table->average_float(col_shipping), 30.f);
            CHECK_EQUAL(table->average_float(col_shipping, &count), 30.f);
            CHECK_EQUAL(count, 1);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_double(col_rating), (1.1 + 2.2) / 2., 0.01);
            CHECK_APPROXIMATELY_EQUAL(table->average_double(col_rating, &count), (1.1 + 2.2) / 2., 0.01);
            CHECK_EQUAL(count, 2);

            // sum
            CHECK_EQUAL(table->sum_int(col_price), 4);
            CHECK_EQUAL(table->sum_float(col_shipping), 30.f);
            CHECK_APPROXIMATELY_EQUAL(table->sum_double(col_rating), 1.1 + 2.2, 0.01);
        }
        else { // not nullable
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(col_price, &pos), 3);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_double(col_rating, &pos), 2.2);
            CHECK_EQUAL(pos, Key(1));

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(col_date, &pos), Timestamp(6, 6));
            CHECK_EQUAL(pos, Key(2));

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(col_price, &pos), 0);
            CHECK_EQUAL(pos, Key(1));

            pos = 123;
            CHECK_EQUAL(table->minimum_float(col_shipping, &pos), 0.f);
            CHECK_EQUAL(pos, Key(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_double(col_rating, &pos), 0.);
            CHECK_EQUAL(pos, Key(2));

            pos = 123;
            // Timestamp(0, 0) is default value for non-nullable column
            CHECK_EQUAL(table->minimum_timestamp(col_date, &pos), Timestamp(0, 0));
            CHECK_EQUAL(pos, Key(1));

            // average
            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_int(col_price, &count), (1 + 3 + 0) / 3., 0.01);
            CHECK_EQUAL(count, 3);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_float(col_shipping, &count), 30.f / 3., 0.01);
            CHECK_EQUAL(count, 3);

            count = 123;
            CHECK_APPROXIMATELY_EQUAL(table->average_double(col_rating, &count), (1.1 + 2.2 + 0.) / 3., 0.01);
            CHECK_EQUAL(count, 3);

            // sum
            CHECK_EQUAL(table->sum_int(col_price), 4);
            CHECK_EQUAL(table->sum_float(col_shipping), 30.f);
            CHECK_APPROXIMATELY_EQUAL(table->sum_double(col_rating), 1.1 + 2.2, 0.01);
        }
    }
}

TEST(Table_EmptyMinmax)
{
    Group g;
    TableRef table = g.add_table("");
    auto col = table->add_column(type_Timestamp, "date");

    Key min_key;
    Timestamp min_ts = table->minimum_timestamp(col, &min_key);
    CHECK_EQUAL(min_key, null_key);
    CHECK(min_ts.is_null());

    Key max_key;
    Timestamp max_ts = table->maximum_timestamp(col, &max_key);
    CHECK_EQUAL(max_key, null_key);
    CHECK(max_ts.is_null());
}

#ifdef LEGACY_TESTS
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
#endif // LEGACY_TESTS

TEST(Table_AddColumnWithThreeLevelBptree)
{
    Table table;
    std::vector<Key> keys;
    table.add_column(type_Int, "int0");
    table.create_objects(REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1, keys);
    table.add_column(type_Int, "int1");
    table.verify();
}


TEST(Table_ClearWithTwoLevelBptree)
{
    Table table;
    std::vector<Key> keys;
    table.add_column(type_String, "strings");
    table.create_objects(REALM_MAX_BPNODE_SIZE + 1, keys);
    table.clear();
    table.verify();
}

TEST(Table_IndexStringDelete)
{
    Table t;
    auto col = t.add_column(type_String, "str");
    t.add_search_index(col);

    for (size_t i = 0; i < 1000; ++i) {
        std::string out(util::to_string(i));
        t.create_object().set<String>(col, out);
    }

    t.clear();

    for (size_t i = 0; i < 1000; ++i) {
        std::string out(util::to_string(i));
        t.create_object().set<String>(col, out);
    }
}


TEST(Table_NullableChecks)
{
    Table t;
    TableView tv;
    constexpr bool nullable = true;
    auto str_col = t.add_column(type_String, "str", nullable);
    auto int_col = t.add_column(type_Int, "int", nullable);
    auto bool_col = t.add_column(type_Bool, "bool", nullable);
    auto ts_col = t.add_column(type_Timestamp, "timestamp", nullable);
    auto float_col = t.add_column(type_Float, "float", nullable);
    auto double_col = t.add_column(type_Double, "double", nullable);
    auto binary_col = t.add_column(type_Binary, "binary", nullable);

    Obj obj = t.create_object();
    StringData sd; // construct a null reference
    Timestamp ts; // null
    BinaryData bd;; // null
    obj.set(str_col, sd);
    obj.set(int_col, realm::null());
    obj.set(bool_col, realm::null());
    obj.set(ts_col, ts);
    obj.set(float_col, realm::null());
    obj.set(double_col, realm::null());
    obj.set(binary_col, bd);

    // is_null is always reliable regardless of type
    CHECK(obj.is_null(str_col));
    CHECK(obj.is_null(int_col));
    CHECK(obj.is_null(bool_col));
    CHECK(obj.is_null(ts_col));
    CHECK(obj.is_null(float_col));
    CHECK(obj.is_null(double_col));
    CHECK(obj.is_null(binary_col));

    StringData str0 = obj.get<String>(str_col);
    CHECK(str0.is_null());
    util::Optional<int64_t> int0 = obj.get<util::Optional<int64_t>>(int_col);
    CHECK(!int0);
    util::Optional<bool> bool0 = obj.get<util::Optional<bool>>(bool_col);
    CHECK(!bool0);
    Timestamp ts0 = obj.get<Timestamp>(ts_col);
    CHECK(ts0.is_null());
    util::Optional<float> float0 = obj.get<util::Optional<float>>(float_col);
    CHECK(!float0);
    util::Optional<double> double0 = obj.get<util::Optional<double>>(double_col);
    CHECK(!double0);
    BinaryData binary0 = obj.get<Binary>(binary_col);
    CHECK(binary0.is_null());
}


#ifdef LEGACY_TESTS
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
    CHECK_LOGIC_ERROR(table->swap_rows(0, 1), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(table->move_row(0, 1), LogicError::detached_accessor);
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
#endif // LEGACY_TESTS

TEST(Table_addRowsToTableWithNoColumns)
{
    Group g; // type_Link must be part of a group
    TableRef t = g.add_table("t");

    t->create_object();
    CHECK_EQUAL(t->size(), 1);
    auto col = t->add_column(type_String, "str_col");
    t->create_object();
    CHECK_EQUAL(t->size(), 2);
    t->add_search_index(col);
    t->create_object();
    CHECK_EQUAL(t->size(), 3);
    t->remove_column(col);
    CHECK_EQUAL(t->size(), 3);

    // Check that links are nulled when connected table is cleared
    TableRef u = g.add_table("u");
    auto col_link = u->add_column_link(type_Link, "link from u to t", *t);
    Obj obj = u->create_object();
    CHECK_EQUAL(u->size(), 1);
    CHECK_EQUAL(t->size(), 3);
    CHECK_LOGIC_ERROR(obj.set(col_link, Key(45)), LogicError::target_row_index_out_of_range);
    CHECK(obj.is_null(col_link));
    CHECK_EQUAL(t->size(), 3);
    Key k = t->create_object().get_key();
    obj.set(col_link, k);
    CHECK_EQUAL(obj.get<Key>(col_link), k);
    CHECK(!obj.is_null(col_link));
    CHECK_EQUAL(t->size(), 4);
    t->clear();
    CHECK_EQUAL(t->size(), 0);
    CHECK_EQUAL(u->size(), 1);
    CHECK(obj.is_null(col_link));
    u->remove_column(col_link);
}


TEST(Table_getVersionCounterAfterRowAccessor)
{
    Table t;
    auto col_bool = t.add_column(type_Bool, "bool", true);
    auto col_int = t.add_column(type_Int, "int", true);
    auto col_string = t.add_column(type_String, "string", true);
    auto col_float = t.add_column(type_Float, "float", true);
    auto col_double = t.add_column(type_Double, "double", true);
    auto col_binary = t.add_column(type_Binary, "binary", true);
    auto col_date = t.add_column(type_Timestamp, "timestamp", true);

    Obj obj = t.create_object();

    int_fast64_t ver = t.get_content_version();
    int_fast64_t newVer;

    auto check_ver_bump = [&]() {
        newVer = t.get_content_version();
        CHECK_GREATER(newVer, ver);
        ver = newVer;
    };

    obj.set<Bool>(col_bool, true);
    check_ver_bump();

    obj.set<Int>(col_int, 42);
    check_ver_bump();

    obj.set<String>(col_string, "foo");
    check_ver_bump();

    obj.set<Float>(col_float, 0.42f);
    check_ver_bump();

    obj.set<Double>(col_double, 0.42);
    check_ver_bump();

    obj.set<Binary>(col_binary, BinaryData("binary", 7));
    check_ver_bump();

    obj.set<Timestamp>(col_date, Timestamp(777, 888));
    check_ver_bump();

    obj.set_null(col_string);
    check_ver_bump();
}


TEST(Table_object_basic)
{
    Table table;
    auto int_col = table.add_column(type_Int, "int");
    auto intnull_col = table.add_column(type_Int, "intnull", true);

    char data[10];
    memset(data, 0x5a, 10);
    BinaryData bin_data(data, 10);
    BinaryData bin_zero(data, 0);

    table.create_object(Key(5)).set_all(100, 7);
    CHECK_EQUAL(table.size(), 1);
    CHECK_THROW(table.create_object(Key(5)), InvalidKey);
    CHECK_EQUAL(table.size(), 1);
    table.create_object(Key(2));
    Obj x = table.create_object(Key(7));
    table.create_object(Key(8));
    table.create_object(Key(10));
    table.create_object(Key(6));

    Obj y = table.get_object(Key(5));

    // Int
    CHECK(!x.is_null(int_col));
    CHECK_EQUAL(0, x.get<int64_t>(int_col));
    CHECK(x.is_null(intnull_col));

    CHECK_EQUAL(100, y.get<int64_t>(int_col));
    CHECK(!y.is_null(intnull_col));
    CHECK_EQUAL(7, y.get<util::Optional<int64_t>>(intnull_col));
    y.set_null(intnull_col);
    CHECK(y.is_null(intnull_col));

    // Boolean
    auto bool_col = table.add_column(type_Bool, "bool");
    auto boolnull_col = table.add_column(type_Bool, "boolnull", true);
    y.set(bool_col, true);
    y.set(boolnull_col, false);

    CHECK(!x.is_null(bool_col));
    CHECK_EQUAL(false, x.get<Bool>(bool_col));
    CHECK(x.is_null(boolnull_col));

    CHECK_EQUAL(true, y.get<Bool>(bool_col));
    CHECK(!y.is_null(boolnull_col));
    auto bool_val = y.get<util::Optional<Bool>>(boolnull_col);
    CHECK_EQUAL(true, bool(bool_val));
    CHECK_EQUAL(false, *bool_val);
    y.set_null(boolnull_col);
    CHECK(y.is_null(boolnull_col));

    // Float
    auto float_col = table.add_column(type_Float, "float");
    auto floatnull_col = table.add_column(type_Float, "floatnull", true);
    y.set(float_col, 2.7182818f);
    y.set(floatnull_col, 3.1415927f);

    CHECK(!x.is_null(float_col));
    CHECK_EQUAL(0.0f, x.get<Float>(float_col));
    CHECK(x.is_null(floatnull_col));

    CHECK_EQUAL(2.7182818f, y.get<Float>(float_col));
    CHECK(!y.is_null(floatnull_col));
    CHECK_EQUAL(3.1415927f, y.get<util::Optional<Float>>(floatnull_col));
    y.set_null(floatnull_col);
    CHECK(y.is_null(floatnull_col));

    // Double
    auto double_col = table.add_column(type_Double, "double");
    auto doublenull_col = table.add_column(type_Double, "doublenull", true);
    y.set(double_col, 2.718281828459045);
    y.set(doublenull_col, 3.141592653589793);

    CHECK(!x.is_null(double_col));
    CHECK_EQUAL(0.0f, x.get<Double>(double_col));
    CHECK(x.is_null(doublenull_col));

    CHECK_EQUAL(2.718281828459045, y.get<Double>(double_col));
    CHECK(!y.is_null(doublenull_col));
    CHECK_EQUAL(3.141592653589793, y.get<util::Optional<Double>>(doublenull_col));
    y.set_null(doublenull_col);
    CHECK(y.is_null(doublenull_col));

    // String
    auto str_col = table.add_column(type_String, "str");
    auto strnull_col = table.add_column(type_String, "strnull", true);
    y.set(str_col, "Hello");
    y.set(strnull_col, "World");

    CHECK(!x.is_null(str_col));
    CHECK_EQUAL("", x.get<String>(str_col));
    CHECK(x.is_null(strnull_col));

    CHECK_EQUAL("Hello", y.get<String>(str_col));
    CHECK(!y.is_null(strnull_col));
    CHECK_EQUAL("World", y.get<String>(strnull_col));
    y.set_null(strnull_col);
    CHECK(y.is_null(strnull_col));

    // Upgrade to medium leaf
    y.set(str_col, "This is a fine day");
    CHECK_EQUAL("This is a fine day", y.get<String>(str_col));
    CHECK(!y.is_null(str_col));

    // Binary
    auto bin_col = table.add_column(type_Binary, "bin");
    auto binnull_col = table.add_column(type_Binary, "binnull", true);
    y.set(bin_col, bin_data);
    y.set(binnull_col, bin_data);

    CHECK(!x.is_null(bin_col));
    CHECK_EQUAL(bin_zero, x.get<Binary>(bin_col));
    CHECK(x.is_null(binnull_col));

    CHECK_EQUAL(bin_data, y.get<Binary>(bin_col));
    CHECK(!y.is_null(binnull_col));
    CHECK_EQUAL(bin_data, y.get<Binary>(binnull_col));
    y.set_null(binnull_col);
    CHECK(y.is_null(binnull_col));

    // Upgrade from small to big
    char big_data[100];
    memset(big_data, 0xa5, 100);
    BinaryData bin_data_big(big_data, 100);
    x.set(bin_col, bin_data);
    y.set(bin_col, bin_data_big);
    CHECK_EQUAL(bin_data, x.get<Binary>(bin_col));
    CHECK_EQUAL(bin_data_big, y.get<Binary>(bin_col));
    CHECK(!y.is_null(bin_col));

    // Timestamp
    auto ts_col = table.add_column(type_Timestamp, "ts");
    auto tsnull_col = table.add_column(type_Timestamp, "tsnull", true);
    y.set(ts_col, Timestamp(123, 456));
    y.set(tsnull_col, Timestamp(789, 10));

    CHECK(!x.is_null(ts_col));
    CHECK_EQUAL(Timestamp(0, 0), x.get<Timestamp>(ts_col));
    CHECK(x.is_null(tsnull_col));

    CHECK_EQUAL(Timestamp(123, 456), y.get<Timestamp>(ts_col));
    CHECK(!y.is_null(tsnull_col));
    CHECK_EQUAL(Timestamp(789, 10), y.get<Timestamp>(tsnull_col));
    y.set_null(binnull_col);
    CHECK(y.is_null(binnull_col));

    // Check that accessing a removed object will throw
    table.remove_object(Key(5));
    CHECK_THROW(y.get<int64_t>(intnull_col), InvalidKey);

    CHECK(table.get_object(Key(8)).is_null(intnull_col));

    Key k11 = table.create_object().get_key();
    Key k12 = table.create_object().get_key();
    CHECK_EQUAL(k11.value, 11);
    CHECK_EQUAL(k12.value, 12);
}

TEST(Table_remove_column)
{
    Table table;
    table.add_column(type_Int, "int1");
    auto int2_col = table.add_column(type_Int, "int2");
    table.add_column(type_Int, "int3");

    Obj obj = table.create_object(Key(5)).set_all(100, 7, 25);

    CHECK_EQUAL(obj.get<int64_t>("int1"), 100);
    CHECK_EQUAL(obj.get<int64_t>("int2"), 7);
    CHECK_EQUAL(obj.get<int64_t>("int3"), 25);

    table.remove_column(int2_col);

    CHECK_EQUAL(obj.get<int64_t>("int1"), 100);
    CHECK_THROW(obj.get<int64_t>("int2"), InvalidKey);
    CHECK_EQUAL(obj.get<int64_t>("int3"), 25);
    table.add_column(type_Int, "int4");
    CHECK_EQUAL(obj.get<int64_t>("int4"), 0);
}

TEST(Table_list_basic)
{
    Table table;
    auto list_col = table.add_column_list(type_Int, "int_list");

    {
        Obj obj = table.create_object(Key(5));
        CHECK(obj.is_null(list_col));
        auto list = obj.get_list<int64_t>(list_col);
        CHECK_NOT(obj.is_null(list_col));
        for (int i = 0; i < 100; i++) {
            list.add(i + 1000);
        }
    }
    {
        Obj obj = table.get_object(Key(5));
        auto list1 = obj.get_list<int64_t>(list_col);
        CHECK_EQUAL(list1.size(), 100);
        CHECK_EQUAL(list1.get(0), 1000);
        CHECK_EQUAL(list1.get(99), 1099);
        auto list2 = obj.get_list<int64_t>(list_col);
        list2.set(50, 747);
        CHECK_EQUAL(list1.get(50), 747);
    }
    table.remove_object(Key(5));
}

TEST(Table_StableIteration)
{
    Table table;
    auto list_col = table.add_column_list(type_Int, "int_list");
    std::vector<int64_t> values = {1, 7, 3, 5, 5, 2, 4};
    Obj obj = table.create_object(Key(5)).set_list_values(list_col, values);

    auto list = obj.get_list<int64_t>(list_col);
    auto x = list.begin();
    CHECK_EQUAL(*x, 1);
    ++x; // == 7
    ++x; // == 3
    CHECK_EQUAL(*x, 3);
    auto end = list.end();
    for (auto it = list.begin(); it != end; it++) {
        if (*it > 3) {
            list.remove(it);
            // When an element is removed, the iterator should be invalid
            CHECK_THROW_ANY(*it);
        }
        // This iterator should keep pointing to the same element
        CHECK_EQUAL(*x, 3);
    }
    // Advancing the iterator should skip the two deleted elements
    ++x; // == 2
    CHECK_EQUAL(*x, 2);
    ++x; // Past end of list
    CHECK_THROW_ANY(*x);
    CHECK_EQUAL(list.size(), 3);
    CHECK_EQUAL(list[0], 1);
    CHECK_EQUAL(list[1], 3);
    CHECK_EQUAL(list[2], 2);
}

TEST(Table_ListOfPrimitives)
{
    Group g;
    TableRef t = g.add_table("table");
    ColKey int_col = t->add_column_list(type_Int, "integers");
    ColKey bool_col = t->add_column_list(type_Bool, "booleans");
    ColKey string_col = t->add_column_list(type_String, "strings");
    ColKey double_col = t->add_column_list(type_Double, "doubles");
    ColKey timestamp_col = t->add_column_list(type_Timestamp, "timestamps");
    Obj obj = t->create_object(Key(7));

    std::vector<int64_t> integer_vector = {1, 2, 3, 4};
    obj.set_list_values(int_col, integer_vector);

    std::vector<bool> bool_vector = {false, false, true, false, true};
    obj.set_list_values(bool_col, bool_vector);

    std::vector<StringData> string_vector = {"monday", "tuesday", "thursday", "friday", "saturday", "sunday"};
    obj.set_list_values(string_col, string_vector);

    std::vector<double> double_vector = {898742.09382, 3.14159265358979, 2.71828182845904};
    obj.set_list_values(double_col, double_vector);

    time_t seconds_since_epoc = time(nullptr);
    std::vector<Timestamp> timestamp_vector = {Timestamp(seconds_since_epoc, 0),
                                               Timestamp(seconds_since_epoc + 60, 0)};
    obj.set_list_values(timestamp_col, timestamp_vector);

    auto int_list = obj.get_list<int64_t>(int_col);
    std::vector<int64_t> vec(int_list.size());
    CHECK_EQUAL(integer_vector.size(), int_list.size());
    // {1, 2, 3, 4}
    auto it = int_list.begin();
    CHECK_EQUAL(*it, 1);
    std::copy(int_list.begin(), int_list.end(), vec.begin());
    unsigned j = 0;
    for (auto i : int_list) {
        CHECK_EQUAL(vec[j], i);
        CHECK_EQUAL(integer_vector[j++], i);
    }
    auto f = std::find(int_list.begin(), int_list.end(), 3);
    CHECK_EQUAL(3, *f++);
    CHECK_EQUAL(4, *f);

    for (unsigned i = 0; i < int_list.size(); i++) {
        CHECK_EQUAL(integer_vector[i], int_list[i]);
    }

    CHECK_EQUAL(3, int_list.remove(2));
    // {1, 2, 4}
    CHECK_EQUAL(integer_vector.size() - 1, int_list.size());
    CHECK_EQUAL(4, int_list[2]);
    int_list.resize(6);
    // {1, 2, 4, 0, 0, 0}
    CHECK_EQUAL(int_list[5], 0);
    int_list.swap(0, 1);
    // {2, 1, 4, 0, 0, 0}
    CHECK_EQUAL(2, int_list[0]);
    CHECK_EQUAL(1, int_list[1]);
    int_list.move(1, 4);
    // {2, 4, 0, 0, 1, 0}
    CHECK_EQUAL(4, int_list[1]);
    CHECK_EQUAL(1, int_list[4]);
    int_list.remove(1, 3);
    // {2, 0, 1, 0}
    CHECK_EQUAL(1, int_list[2]);
    int_list.resize(2);
    // {2, 0}
    CHECK_EQUAL(2, int_list.size());
    CHECK_EQUAL(2, int_list[0]);
    CHECK_EQUAL(0, int_list[1]);

    int_list.clear();
    auto int_list2 = obj.get_list<int64_t>(int_col);
    CHECK_EQUAL(0, int_list2.size());

    auto bool_list = obj.get_list<bool>(bool_col);
    CHECK_EQUAL(bool_vector.size(), bool_list.size());
    for (unsigned i = 0; i < bool_list.size(); i++) {
        CHECK_EQUAL(bool_vector[i], bool_list[i]);
    }

    auto string_list = obj.get_list<StringData>(string_col);
    CHECK_EQUAL(string_list.begin()->size(), string_vector.begin()->size());
    CHECK_EQUAL(string_vector.size(), string_list.size());
    for (unsigned i = 0; i < string_list.size(); i++) {
        CHECK_EQUAL(string_vector[i], string_list[i]);
    }

    string_list.insert(2, "Wednesday");
    CHECK_EQUAL(string_vector.size() + 1, string_list.size());
    CHECK_EQUAL(StringData("Wednesday"), string_list.get(2));

    auto double_list = obj.get_list<double>(double_col);
    CHECK_EQUAL(double_vector.size(), double_list.size());
    for (unsigned i = 0; i < double_list.size(); i++) {
        CHECK_EQUAL(double_vector[i], double_list.get(i));
    }

    auto timestamp_list = obj.get_list<Timestamp>(timestamp_col);
    CHECK_EQUAL(timestamp_vector.size(), timestamp_list.size());
    for (unsigned i = 0; i < timestamp_list.size(); i++) {
        CHECK_EQUAL(timestamp_vector[i], timestamp_list.get(i));
    }

    t->remove_object(Key(7));
    CHECK_NOT(timestamp_list.is_valid());
}

TEST(Table_object_merge_nodes)
{
    // This test works best for REALM_MAX_BPNODE_SIZE == 8.
    // To be used mostly as a help when debugging new implementation

    int nb_rows = REALM_MAX_BPNODE_SIZE * 8;
    Table table;
    std::vector<int64_t> key_set;
    auto c0 = table.add_column(type_Int, "int1");
    auto c1 = table.add_column(type_Int, "int2", true);

    for (int i = 0; i < nb_rows; i++) {
        table.create_object(Key(i)).set_all(i << 1, i << 2);
        key_set.push_back(i);
    }

    for (int i = 0; i < nb_rows; i++) {
        auto key_index = test_util::random_int<int64_t>(0, key_set.size() - 1);
        auto it = key_set.begin() + int(key_index);

        // table.dump_objects();
        // std::cout << "Key to remove: " << std::hex << *it << std::dec << std::endl;

        table.remove_object(Key(*it));
        key_set.erase(it);
        for (unsigned j = 0; j < key_set.size(); j++) {
            int64_t key_val = key_set[j];
            Obj o = table.get_object(Key(key_val));
            CHECK_EQUAL(key_val << 1, o.get<int64_t>(c0));
            CHECK_EQUAL(key_val << 2, o.get<util::Optional<int64_t>>(c1));
        }
    }
}

TEST(Table_object_forward_iterator)
{
    int nb_rows = 1024;
    Table table;
    auto c0 = table.add_column(type_Int, "int1");
    auto c1 = table.add_column(type_Int, "int2", true);

    for (int i = 0; i < nb_rows; i++) {
        table.create_object(Key(i));
    }

    int tree_size = 0;
    table.traverse_clusters([&tree_size](const Cluster* cluster) {
        tree_size += cluster->node_size();
        return false;
    });
    CHECK_EQUAL(tree_size, nb_rows);

    for (Obj o : table) {
        int64_t key_value = o.get_key().value;
        o.set_all(key_value << 1, key_value << 2);
    }

    // table.dump_objects();

    for (Obj o : table) {
        int64_t key_value = o.get_key().value;
        // std::cout << "Key value: " << std::hex << key_value << std::dec << std::endl;
        CHECK_EQUAL(key_value << 1, o.get<int64_t>(c0));
        CHECK_EQUAL(key_value << 2, o.get<util::Optional<int64_t>>(c1));
    }

    auto it = table.begin();
    while (it != table.end()) {
        int64_t val = it->get_key().value;
        // Delete every 7th object
        if ((val % 7) == 0) {
            table.remove_object(it);
        }
        ++it;
    }
    CHECK_EQUAL(table.size(), nb_rows * 6 / 7);

    auto it1 = table.begin();
    Key key = it1->get_key();
    ++it1;
    int64_t val = it1->get<int64_t>(c0);
    table.remove_object(key);
    CHECK_EQUAL(val, it1->get<int64_t>(c0));
    table.remove_object(it1);
    CHECK_THROW_ANY(it1->get<int64_t>(c0));
}

TEST(Table_object_sequential)
{
    int nb_rows = 1024;
    Table table;
    auto c0 = table.add_column(type_Int, "int1");
    auto c1 = table.add_column(type_Int, "int2", true);

    auto t1 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        table.create_object(Key(i)).set_all(i << 1, i << 2);
    }

    auto t2 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        Obj o = table.get_object(Key(i));
        CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
        CHECK_EQUAL(i << 2, o.get<util::Optional<int64_t>>(c1));
    }

    auto t3 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        table.remove_object(Key(i));
        for (int j = i + 1; j < nb_rows; j++) {
            Obj o = table.get_object(Key(j));
            CHECK_EQUAL(j << 1, o.get<int64_t>(c0));
            CHECK_EQUAL(j << 2, o.get<util::Optional<int64_t>>(c1));
        }
    }

    auto t4 = steady_clock::now();

    std::cout << nb_rows << " rows" << std::endl;
    std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   lookup time   : " << duration_cast<nanoseconds>(t3 - t2).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   erase time    : " << duration_cast<nanoseconds>(t4 - t3).count() / nb_rows << " ns/key"
              << std::endl;
}

TEST(Table_object_random)
{
    int nb_rows = 1024;
    std::vector<int64_t> key_values;
    {
        std::set<int64_t> key_set;
        for (int i = 0; i < nb_rows; i++) {
            bool ok = false;
            while (!ok) {
                auto key_val = test_util::random_int<int64_t>(0, nb_rows * 10);
                if (key_set.count(key_val) == 0) {
                    key_values.push_back(key_val);
                    key_set.insert(key_val);
                    ok = true;
                }
            }
        }
    }

    Table table;
    auto c0 = table.add_column(type_Int, "int1");
    auto c1 = table.add_column(type_Int, "int2", true);

    auto t1 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        table.create_object(Key(key_values[i])).set_all(i << 1, i << 2);
    }

    auto t2 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        Obj o = table.get_object(Key(key_values[i]));
        CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
        CHECK_EQUAL(i << 2, o.get<util::Optional<int64_t>>(c1));
    }

    auto t3 = steady_clock::now();

    for (int i = 0; i < nb_rows; i++) {
        table.remove_object(Key(key_values[i]));
        for (int j = i + 1; j < nb_rows; j++) {
            Obj o = table.get_object(Key(key_values[j]));
            CHECK_EQUAL(j << 1, o.get<int64_t>(c0));
            CHECK_EQUAL(j << 2, o.get<util::Optional<int64_t>>(c1));
        }
    }

    auto t4 = steady_clock::now();

    std::cout << nb_rows << " rows" << std::endl;
    std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   lookup time   : " << duration_cast<nanoseconds>(t3 - t2).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   erase time    : " << duration_cast<nanoseconds>(t4 - t3).count() / nb_rows << " ns/key"
              << std::endl;
}


#ifdef LEGACY_TESTS
TEST(Table_3)
{
    TestTable01 table;

    for (int64_t i = 0; i < 100; ++i) {
        table.create_object(Key(i)).set_all(i, 10, true, int(Wed));
    }
    auto cols = table.get_key_cols();

    // Test column searching
    CHECK_EQUAL(Key(0), table.find_first_int(cols[0], 0));
    CHECK_EQUAL(Key(50), table.find_first_int(cols[0], 50));
    CHECK_EQUAL(null_key, table.find_first_int(cols[0], 500));
    CHECK_EQUAL(Key(0), table.find_first_int(cols[1], 10));
    CHECK_EQUAL(null_key, table.find_first_int(cols[1], 100));
    CHECK_EQUAL(Key(0), table.find_first_bool(cols[2], true));
    CHECK_EQUAL(null_key, table.find_first_bool(cols[2], false));
    CHECK_EQUAL(Key(0), table.find_first_int(cols[3], Wed));
    CHECK_EQUAL(null_key, table.find_first_int(cols[3], Mon));

#ifdef REALM_DEBUG
    table.verify();
#endif
}
#endif


TEST(Table_4)
{
    Table table;
    auto c0 = table.add_column(type_String, "strings");

    table.create_object(Key(5)).set(c0, "Hello");
    table.create_object(Key(7)).set(c0,
                                    "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello");

    CHECK_EQUAL("HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello",
                table.get_object(Key(7)).get<String>(c0));

    // Test string column searching
    CHECK_EQUAL(Key(7), table.find_first_string(
                            c0, "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello"));
    CHECK_EQUAL(null_key, table.find_first_string(c0, "Foo"));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

// Very basic sanity check of search index when you add, remove and set objects
TEST(Table_SearchIndexFindFirst)
{
    Table table;

    auto c1 = table.add_column(type_Int, "a");
    auto c2 = table.add_column(type_Int, "b", true);
    auto c3 = table.add_column(type_String, "c");
    auto c4 = table.add_column(type_String, "d", true);
    auto c5 = table.add_column(type_Bool, "e");
    auto c6 = table.add_column(type_Bool, "f", true);
    auto c7 = table.add_column(type_Timestamp, "g");
    auto c8 = table.add_column(type_Timestamp, "h", true);

    Obj o0 = table.create_object();
    Obj o1 = table.create_object();
    Obj o2 = table.create_object();
    Obj o3 = table.create_object();

    o0.set_all(100, 100, "100", "100", false, false, Timestamp(100, 100), Timestamp(100, 100));
    o1.set_all(200, 200, "200", "200", true, true, Timestamp(200, 200), Timestamp(200, 200));
    o2.set_all(200, 200, "200", "200", true, true, Timestamp(200, 200), Timestamp(200, 200));
    CHECK(o3.is_null(c2));
    CHECK(o3.is_null(c4));
    CHECK(o3.is_null(c6));
    CHECK(o3.is_null(c8));

    table.add_search_index(c1);
    table.add_search_index(c2);
    table.add_search_index(c3);
    table.add_search_index(c4);
    table.add_search_index(c5);
    table.add_search_index(c6);
    table.add_search_index(c7);
    table.add_search_index(c8);

    // Non-nullable integers
    CHECK_EQUAL(table.find_first_int(c1, 100), o0.get_key());
    CHECK_EQUAL(table.find_first_int(c1, 200), o1.get_key());
    // Uninitialized non-nullable integers equal 0
    CHECK_EQUAL(table.find_first_int(c1, 0), o3.get_key());

    // Nullable integers
    CHECK_EQUAL(table.find_first_int(c2, 100), o0.get_key());
    CHECK_EQUAL(table.find_first_int(c2, 200), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(1), o3.get_key());

    // Non-nullable strings
    CHECK_EQUAL(table.find_first_string(c3, "100"), o0.get_key());
    CHECK_EQUAL(table.find_first_string(c3, "200"), o1.get_key());
    // Uninitialized non-nullable strings equal ""
    CHECK_EQUAL(table.find_first_string(c3, ""), o3.get_key());

    // Nullable strings
    CHECK_EQUAL(table.find_first_string(c4, "100"), o0.get_key());
    CHECK_EQUAL(table.find_first_string(c4, "200"), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(3), o3.get_key());

    // Non-nullable bools
    CHECK_EQUAL(table.find_first_bool(c5, false), o0.get_key());
    CHECK_EQUAL(table.find_first_bool(c5, true), o1.get_key());

    // Nullable bools
    CHECK_EQUAL(table.find_first_bool(c6, false), o0.get_key());
    CHECK_EQUAL(table.find_first_bool(c6, true), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(5), o3.get_key());

    // Non-nullable Timestamp
    CHECK_EQUAL(table.find_first_timestamp(c7, Timestamp(100, 100)), o0.get_key());
    CHECK_EQUAL(table.find_first_timestamp(c7, Timestamp(200, 200)), o1.get_key());

    // Nullable Timestamp
    CHECK_EQUAL(table.find_first_timestamp(c8, Timestamp(100, 100)), o0.get_key());
    CHECK_EQUAL(table.find_first_timestamp(c8, Timestamp(200, 200)), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(7), o3.get_key());

    // Remove object and see if things still work
    // *******************************************************************************
    table.remove_object(o0.get_key());

    // Integers
    CHECK_EQUAL(table.find_first_int(c1, 100), null_key);
    CHECK_EQUAL(table.find_first_int(c1, 200), o1.get_key());
    // Uninitialized non-nullable integers equal 0
    CHECK_EQUAL(table.find_first_int(c1, 0), o3.get_key());

    CHECK_EQUAL(table.find_first_int(c2, 200), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(1), o3.get_key());

    // Non-nullable strings
    CHECK_EQUAL(table.find_first_string(c3, "100"), null_key);
    CHECK_EQUAL(table.find_first_string(c3, "200"), o1.get_key());
    // Uninitialized non-nullable strings equal ""
    CHECK_EQUAL(table.find_first_string(c3, ""), o3.get_key());

    // Nullable strings
    CHECK_EQUAL(table.find_first_string(c4, "100"), null_key);
    CHECK_EQUAL(table.find_first_string(c4, "200"), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(3), o3.get_key());

    // Non-nullable bools
    // default value for non-nullable bool is false, so o3 is a match
    CHECK_EQUAL(table.find_first_bool(c5, false), o3.get_key());
    CHECK_EQUAL(table.find_first_bool(c5, true), o1.get_key());

    // Nullable bools
    CHECK_EQUAL(table.find_first_bool(c6, false), null_key);
    CHECK_EQUAL(table.find_first_bool(c6, true), o1.get_key());

    // Call "set" and see if things still work
    // *******************************************************************************
    o1.set_all(500, 500, "500", "500");
    o2.set_all(600, 600, "600", "600");

    CHECK_EQUAL(table.find_first_int(c1, 500), o1.get_key());
    CHECK_EQUAL(table.find_first_int(c1, 600), o2.get_key());
    // Uninitialized non-nullable integers equal 0
    CHECK_EQUAL(table.find_first_int(c1, 0), o3.get_key());
    CHECK_EQUAL(table.find_first_int(c2, 500), o1.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(1), o3.get_key());

    // Non-nullable strings
    CHECK_EQUAL(table.find_first_string(c3, "500"), o1.get_key());
    CHECK_EQUAL(table.find_first_string(c3, "600"), o2.get_key());
    // Uninitialized non-nullable strings equal ""
    CHECK_EQUAL(table.find_first_string(c3, ""), o3.get_key());

    // Nullable strings
    CHECK_EQUAL(table.find_first_string(c4, "500"), o1.get_key());
    CHECK_EQUAL(table.find_first_string(c4, "600"), o2.get_key());
    // FIXME: Waiting for fix outside scope of search index PR
    // CHECK_EQUAL(table.find_first_null(3), o3.get_key());

    // Remove four of the indexes through remove_search_index() call. Let other four remain to see
    // if they leak memory when Table goes out of scope (needs leak detector)
    table.remove_search_index(c1);
    table.remove_search_index(c2);
    table.remove_search_index(c3);
    table.remove_search_index(c4);
}

TEST(Table_SearchIndexFindAll)
{
    Table table;
    auto col_int = table.add_column(type_Int, "integers");
    auto col_str = table.add_column(type_String, "strings");
    // Add index before creating objects
    table.add_search_index(col_int);
    table.add_search_index(col_str);

    std::vector<Key> keys;
    table.create_objects(100, keys);
    for (auto o : table) {
        int64_t key_value = o.get_key().value;
        o.set(col_int, key_value);
        // When node size is 4 the objects with "Hello" will be in 2 clusters
        if (key_value > 21 && key_value < 28) {
            o.set(col_str, "Hello");
        }
    }

    auto tv = table.find_all_string(col_str, "Hello");
    CHECK_EQUAL(tv.size(), 6);
}

namespace {

template <class T, bool nullable>
struct Tester {
    using T2 = typename util::RemoveOptional<T>::type;

    static ColKey col;

    static std::vector<Key> find_all_reference(Table& table, T v)
    {
        std::vector<Key> res;
        Table::Iterator it = table.begin();
        while (it != table.end()) {
            if (!it->is_null(col)) {
                T v2 = it->get<T>(col);
                if (v == v2) {
                    res.push_back(it->get_key());
                }
            }
            ++it;
        };
        // res is returned with nrvo optimization
        return res;
    }

    static void validate(Table& table)
    {
        Table::Iterator it = table.begin();

        if (it != table.end()) {
            auto v = it->get<T>(col);

            if (!it->is_null(col)) {
                std::vector<Key> res;
                table.get_search_index(col)->find_all(res, v, false);
                std::vector<Key> ref = find_all_reference(table, v);

                size_t a = ref.size();
                size_t b = res.size();

                REALM_ASSERT(a == b);
            }
        }
    }

    static void run(realm::DataType type)
    {
        Table table;
        col = table.add_column(type, "name", nullable);
        table.add_search_index(col);
        const size_t iters = 1000;

        bool add_trend = true;

        for (size_t iter = 0; iter < iters; iter++) {

            if (iter == iters / 2) {
                add_trend = false;
            }

            // Add object (with 60% probability, so we grow the object count over time)
            if (fastrand(100) < (add_trend ? 80 : 20)) {
                Obj o = table.create_object();
                bool set_to_null = fastrand(100) < 20;

                if (!set_to_null) {
                    auto t = create();
                    o.set<T2>(col, t);
                }
            }

            // Remove random object
            if (fastrand(100) < 50 && table.size() > 0) {
                Table::Iterator it = table.begin();
                auto r = fastrand(table.size() - 1);
                // FIXME: Is there a faster way to pick a random object?
                for (unsigned t = 0; t < r; t++) {
                    ++it;
                }
                Obj o = *it;
                table.remove_object(o.get_key());
            }

            // Edit random object
            if (table.size() > 0) {
                Table::Iterator it = table.begin();
                auto r = fastrand(table.size() - 1);
                // FIXME: Is there a faster way to pick a random object?
                for (unsigned t = 0; t < r; t++) {
                    ++it;
                }
                Obj o = *it;
                bool set_to_null = fastrand(100) < 20;
                if (set_to_null && table.is_nullable(col)) {
                    o.set_null(col);
                }
                else {
                    auto t = create();
                    o.set<T2>(col, t);
                }
            }

            if (iter % (iters / 1000) == 0) {
                validate(table);
            }
        }
    }


    // Create random data element of any type supported by the search index
    template <typename Type = T2>
    typename std::enable_if<std::is_same<Type, StringData>::value, std::string>::type static create()
    {
        std::string s = realm::util::to_string(fastrand(5));
        return s;
    }
    template <typename Type = T2>
    typename std::enable_if<std::is_same<Type, Timestamp>::value, T2>::type static create()
    {
        return Timestamp(fastrand(3), int32_t(fastrand(3)));
    }
    template <typename Type = T2>
    typename std::enable_if<std::is_same<Type, int64_t>::value, T2>::type static create()
    {
        return fastrand(5);
    }

    template <typename Type = T2>
    typename std::enable_if<std::is_same<Type, bool>::value, T2>::type static create()
    {
        return fastrand(100) > 50;
    }
};

template <class T, bool nullable>
ColKey Tester<T, nullable>::col;
}

// The run() method will first add lots of objects, and then remove them. This will test
// both node splits and empty leaf destruction and get good search index code coverage
TEST(Table_search_index_fuzzer)
{
    // Syntax for Tester<T, nullable>:
    // T:         Type that must be used in calls too Obj::get<T>
    // nullable:  If the columns must be is nullable or not
    // Obj::set() will be automatically be called with set<RemoveOptional<T>>()

    Tester<bool, false>::run(type_Bool);
    Tester<Optional<bool>, true>::run(type_Bool);

    Tester<int64_t, false>::run(type_Int);
    Tester<Optional<int64_t>, true>::run(type_Int);

    // Self-contained null state
    Tester<Timestamp, false>::run(type_Timestamp);
    Tester<Timestamp, true>::run(type_Timestamp);

    // Self-contained null state
    Tester<StringData, true>::run(type_String);
    Tester<StringData, false>::run(type_String);
}

TEST(Table_StaleColumnKey)
{
    Table table;

    auto col = table.add_column(type_Int, "age");

    Obj obj = table.create_object();
    obj.set(col, 5);

    table.remove_column(col);
    // col is now obsolete
    table.add_column(type_Int, "score");
    CHECK_THROW_ANY(obj.get<Int>(col));
}


#endif // TEST_TABLE
