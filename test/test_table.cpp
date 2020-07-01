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
#include <cmath>
#include <limits>
#include <string>
#include <fstream>
#include <ostream>
#include <set>
#include <chrono>

using namespace std::chrono;

#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/to_string.hpp>
#include <realm/util/base64.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_string.hpp>
#include <realm/array_timestamp.hpp>
#include <realm/index_string.hpp>

#include "util/misc.hpp"

#include "test.hpp"
#include "test_table_helper.hpp"
#include "test_types_helper.hpp"

// #include <valgrind/callgrind.h>
//#define PERFORMACE_TESTING

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;
using unit_test::TestContext;

#ifndef CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_START_INSTRUMENTATION
#endif

#ifndef CALLGRIND_STOP_INSTRUMENTATION
#define CALLGRIND_STOP_INSTRUMENTATION
#endif

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

extern unsigned int unit_test_random_seed;

namespace {

// copy and convert values between nullable/not nullable as expressed by types
// both non-nullable:
template <typename T1, typename T2>
struct value_copier {
    value_copier(bool)
    {
    }
    T2 operator()(T1 from_value, bool = false)
    {
        return from_value;
    }
};

// copy from non-nullable to nullable
template <typename T1, typename T2>
struct value_copier<T1, Optional<T2>> {
    value_copier(bool throw_on_null)
        : internal_copier(throw_on_null)
    {
    }
    value_copier<T1, T2> internal_copier; // we need state for strings and binaries.
    Optional<T2> operator()(T1 from_value, bool)
    {
        return Optional<T2>(internal_copier(from_value));
    }
};

// copy from nullable to non-nullable - nulls may trigger exception or become default value
template <typename T1, typename T2>
struct value_copier<Optional<T1>, T2> {
    value_copier(bool throw_on_null)
        : m_throw_on_null(throw_on_null)
    {
    }
    bool m_throw_on_null;
    T2 operator()(Optional<T1> from_value, bool)
    {
        if (bool(from_value))
            return from_value.value();
        else {
            if (m_throw_on_null)
                throw realm::LogicError(realm::LogicError::column_not_nullable);
            else
                return T2(); // default value for type
        }
    }
};

// identical to non-specialized case, but specialization needed to avoid capture by 2 previous decls
template <typename T1, typename T2>
struct value_copier<Optional<T1>, Optional<T2>> {
    value_copier(bool)
    {
    }
    Optional<T2> operator()(Optional<T1> from_value, bool)
    {
        return from_value;
    }
};

// Specialization for StringData, BinaryData and Timestamp.
// these types do not encode/express nullability.

template <>
struct value_copier<StringData, StringData> {
    value_copier(bool throw_on_null)
        : m_throw_on_null(throw_on_null)
    {
    }
    bool m_throw_on_null;
    std::vector<char> data; // we need to make a local copy because writing may invalidate the argument
    StringData operator()(StringData from_value, bool to_optional)
    {
        if (from_value.is_null()) {
            if (to_optional)
                return StringData();

            if (m_throw_on_null) {
                // possibly incorrect - may need to convert to default value for non-nullable entries instead
                throw realm::LogicError(realm::LogicError::column_not_nullable);
            }
            else
                return StringData("", 0);
        }
        const char* p = from_value.data();
        const char* limit = p + from_value.size();
        data.clear();
        data.reserve(from_value.size());
        while (p != limit)
            data.push_back(*p++);
        return StringData(&data[0], from_value.size());
    }
};

template <>
struct value_copier<BinaryData, BinaryData> {
    value_copier(bool throw_on_null)
        : m_throw_on_null(throw_on_null)
    {
    }
    bool m_throw_on_null;
    std::vector<char> data; // we need to make a local copy because writing may invalidate the argument
    BinaryData operator()(BinaryData from_value, bool to_optional)
    {
        if (from_value.is_null()) {
            if (to_optional)
                return BinaryData();

            if (m_throw_on_null) {
                // possibly incorrect - may need to convert to default value for non-nullable entries instead
                throw realm::LogicError(realm::LogicError::column_not_nullable);
            }
            else
                return BinaryData("", 0);
        }
        const char* p = from_value.data();
        const char* limit = p + from_value.size();
        data.clear();
        data.reserve(from_value.size());
        while (p != limit)
            data.push_back(*p++);
        return BinaryData(&data[0], from_value.size());
    }
};

template <>
struct value_copier<Timestamp, Timestamp> {
    value_copier(bool throw_on_null)
        : m_throw_on_null(throw_on_null)
    {
    }
    bool m_throw_on_null;
    Timestamp operator()(Timestamp from_value, bool to_optional)
    {
        if (from_value.is_null()) {
            if (to_optional)
                return Timestamp();

            if (m_throw_on_null)
                throw realm::LogicError(realm::LogicError::column_not_nullable);
            else
                return Timestamp(0, 0);
        }
        return from_value;
    }
};
}

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

        // Check that you can obtain a non null value through get<Int>
        obj.set(col, 7);
        CHECK_NOT(obj.is_null(col));
        CHECK_EQUAL(obj.get<Int>(col), 7);
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

    ObjKey k0 = table->create_object().set_all("Alice", 17).get_key();
    ObjKey k1 = table->create_object().set_all("Bob", 50).get_key();
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
    ttt.enumerate_string_column(col);
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

    ObjKey idx; // tableview entry that points at the max/min value

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

    ObjKey key;

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
        std::vector<ObjKey> keys;
        table->create_objects(rows, keys);
        int64_t largest = 0;
        int64_t smallest = 0;
        ObjKey largest_pos = null_key;
        ObjKey smallest_pos = null_key;

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

        ObjKey key;
        size_t cnt;
        float f;
        int64_t i;
        Timestamp ts;

        // Test methods on Table
        {
            // Table::max
            key = 123;
            f = table->maximum_float(float_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->maximum_int(int_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->maximum_timestamp(date_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // Table::min
            key = 123;
            f = table->minimum_float(float_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->minimum_int(int_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->minimum_timestamp(date_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // Table::avg
            double d;

            // number of non-null values used in computing the avg or sum
            cnt = 123;

            // Table::avg
            d = table->average_float(float_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->average_int(int_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // Table::sum
            d = table->sum_float(float_col);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->sum_int(int_col);
            CHECK_EQUAL(i, sum);
        }

        // Test methods on TableView
        {
            // TableView::max
            key = 123;
            f = table->where().find_all().maximum_float(float_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->where().find_all().maximum_int(int_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->where().find_all().maximum_timestamp(date_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // TableView::min
            key = 123;
            f = table->where().find_all().minimum_float(float_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->where().find_all().minimum_int(int_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            ts = table->where().find_all().minimum_timestamp(date_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            key = 123;

            // TableView::avg
            d = table->where().find_all().average_float(float_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->where().find_all().average_int(int_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table->where().find_all().sum_float(float_col);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->where().find_all().sum_int(int_col);
            CHECK_EQUAL(i, sum);

        }

        // Test methods on Query
        {
            // TableView::max
            key = 123;
            f = table->where().maximum_float(float_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(largest_pos).get<float>(float_col));

            key = 123;
            i = table->where().maximum_int(int_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(largest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table->where().maximum_timestamp(date_col, &key);
            CHECK_EQUAL(key, largest_pos);
            if (largest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(largest_pos).get<Timestamp>(date_col));

            // TableView::min
            key = 123;
            f = table->where().minimum_float(float_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(f, table->get_object(smallest_pos).get<float>(float_col));

            key = 123;
            i = table->where().minimum_int(int_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(i, table->get_object(smallest_pos).get<util::Optional<Int>>(int_col));

            key = 123;
            // Note: Method arguments different from metholds on other column types
            ts = table->where().minimum_timestamp(date_col, &key);
            CHECK_EQUAL(key, smallest_pos);
            if (smallest_pos != null_key)
                CHECK_EQUAL(ts, table->get_object(smallest_pos).get<Timestamp>(date_col));

            // TableView::avg
            double d;

            // number of non-null values used in computing the avg or sum
            cnt = 123;

            // TableView::avg
            d = table->where().average_float(float_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            cnt = 123;
            d = table->where().average_int(int_col, &cnt);
            CHECK_EQUAL(cnt, (rows - nulls));
            if (cnt != 0)
                CHECK_APPROXIMATELY_EQUAL(d, avg, 0.001);

            // TableView::sum
            d = table->where().sum_float(float_col);
            CHECK_APPROXIMATELY_EQUAL(d, double(sum), 0.001);

            i = table->where().sum_int(int_col);
            CHECK_EQUAL(i, sum);
        }
    }
}

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

TEST(Table_StringOrBinaryTooBig)
{
    Table table;
    auto col_string = table.add_column(type_String, "s");
    auto col_binary = table.add_column(type_Binary, "b");
    Obj obj = table.create_object();

    obj.set(col_string, "01234567");

    size_t large_bin_size = 0xFFFFF1;
    size_t large_str_size = 0xFFFFF0; // null-terminate reduces max size by 1
    std::unique_ptr<char[]> large_buf(new char[large_bin_size]);
    CHECK_LOGIC_ERROR(obj.set(col_string, StringData(large_buf.get(), large_str_size)), LogicError::string_too_big);
    CHECK_LOGIC_ERROR(obj.set(col_binary, BinaryData(large_buf.get(), large_bin_size)), LogicError::binary_too_big);
    obj.set(col_string, StringData(large_buf.get(), large_str_size - 1));
    obj.set(col_binary, BinaryData(large_buf.get(), large_bin_size - 1));
}


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
    std::vector<ObjKey> keys;
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
        table.create_object(ObjKey(i)).set(col_int, i);
    }

    table.remove_object(ObjKey(0));
    table.remove_object(ObjKey(4));
    table.remove_object(ObjKey(7));

    CHECK_EQUAL(1, table.get_object(ObjKey(1)).get<int64_t>(col_int));
    CHECK_EQUAL(2, table.get_object(ObjKey(2)).get<int64_t>(col_int));
    CHECK_EQUAL(3, table.get_object(ObjKey(3)).get<int64_t>(col_int));
    CHECK_EQUAL(5, table.get_object(ObjKey(5)).get<int64_t>(col_int));
    CHECK_EQUAL(6, table.get_object(ObjKey(6)).get<int64_t>(col_int));
    CHECK_EQUAL(8, table.get_object(ObjKey(8)).get<int64_t>(col_int));
    CHECK_EQUAL(9, table.get_object(ObjKey(9)).get<int64_t>(col_int));

#ifdef REALM_DEBUG
    table.verify();
#endif

    // Delete all items one at a time
    for (size_t i = 0; i < 10; ++i) {
        try {
            table.remove_object(ObjKey(i));
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

void setup_multi_table(Table& table, size_t rows, std::vector<ObjKey>& keys, std::vector<ColKey>& column_keys)
{
    // Create table with all column types
    auto int_col = table.add_column(type_Int, "int");                        //  0
    auto bool_col = table.add_column(type_Bool, "bool");                     //  1
    auto float_col = table.add_column(type_Float, "float");                  //  2
    auto double_col = table.add_column(type_Double, "double");               //  3
    auto string_col = table.add_column(type_String, "string");               //  4
    auto string_long_col = table.add_column(type_String, "string_long");     //  5
    auto string_big_col = table.add_column(type_String, "string_big_blobs"); //  6
    auto string_enum_col = table.add_column(type_String, "string_enum");     //  7 - becomes StringEnumColumn
    auto bin_col = table.add_column(type_Binary, "binary");                  //  8
    auto int_null_col = table.add_column(type_Int, "int_null", true);        //  9, nullable = true
    column_keys.push_back(int_col);
    column_keys.push_back(bool_col);
    column_keys.push_back(float_col);
    column_keys.push_back(double_col);
    column_keys.push_back(string_col);
    column_keys.push_back(string_long_col);
    column_keys.push_back(string_big_col);
    column_keys.push_back(string_enum_col);
    column_keys.push_back(bin_col);
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

    // We also want a StringEnumColumn
    table.enumerate_string_column(string_enum_col);
}

} // anonymous namespace


TEST(Table_DeleteAllTypes)
{
    Table table;
    std::vector<ObjKey> keys;
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


TEST(Table_MoveAllTypes)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator

    Table table;
    std::vector<ObjKey> keys;
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

TEST(Table_FindAllInt)
{
    Table table;

    auto col_int = table.add_column(type_Int, "integers");

    table.create_object(ObjKey(0)).set(col_int, 10);
    table.create_object(ObjKey(1)).set(col_int, 20);
    table.create_object(ObjKey(2)).set(col_int, 10);
    table.create_object(ObjKey(3)).set(col_int, 20);
    table.create_object(ObjKey(4)).set(col_int, 10);
    table.create_object(ObjKey(5)).set(col_int, 20);
    table.create_object(ObjKey(6)).set(col_int, 10);
    table.create_object(ObjKey(7)).set(col_int, 20);
    table.create_object(ObjKey(8)).set(col_int, 10);
    table.create_object(ObjKey(9)).set(col_int, 20);

    // Search for a value that does not exits
    auto v0 = table.find_all_int(col_int, 5);
    CHECK_EQUAL(0, v0.size());

    // Search for a value with several matches
    auto v = table.find_all_int(col_int, 20);

    CHECK_EQUAL(5, v.size());
    CHECK_EQUAL(ObjKey(1), v.get_key(0));
    CHECK_EQUAL(ObjKey(3), v.get_key(1));
    CHECK_EQUAL(ObjKey(5), v.get_key(2));
    CHECK_EQUAL(ObjKey(7), v.get_key(3));
    CHECK_EQUAL(ObjKey(9), v.get_key(4));

#ifdef REALM_DEBUG
    table.verify();
#endif
}

TEST(Table_SortedInt)
{
    Table table;

    auto col_int = table.add_column(type_Int, "integers");

    table.create_object(ObjKey(0)).set(col_int, 10); // 0: 4
    table.create_object(ObjKey(1)).set(col_int, 20); // 1: 7
    table.create_object(ObjKey(2)).set(col_int, 0);  // 2: 0
    table.create_object(ObjKey(3)).set(col_int, 40); // 3: 8
    table.create_object(ObjKey(4)).set(col_int, 15); // 4: 6
    table.create_object(ObjKey(5)).set(col_int, 11); // 5: 5
    table.create_object(ObjKey(6)).set(col_int, 6);  // 6: 3
    table.create_object(ObjKey(7)).set(col_int, 4);  // 7: 2
    table.create_object(ObjKey(8)).set(col_int, 99); // 8: 9
    table.create_object(ObjKey(9)).set(col_int, 2);  // 9: 1

    // Search for a value that does not exits
    auto v = table.get_sorted_view(col_int);
    CHECK_EQUAL(table.size(), v.size());

    CHECK_EQUAL(ObjKey(2), v.get_key(0));
    CHECK_EQUAL(ObjKey(9), v.get_key(1));
    CHECK_EQUAL(ObjKey(7), v.get_key(2));
    CHECK_EQUAL(ObjKey(6), v.get_key(3));
    CHECK_EQUAL(ObjKey(0), v.get_key(4));
    CHECK_EQUAL(ObjKey(5), v.get_key(5));
    CHECK_EQUAL(ObjKey(4), v.get_key(6));
    CHECK_EQUAL(ObjKey(1), v.get_key(7));
    CHECK_EQUAL(ObjKey(3), v.get_key(8));
    CHECK_EQUAL(ObjKey(8), v.get_key(9));

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

    table.create_object(ObjKey(0)).set(col_int, 10).set(col_bool, true);  // 0: 4
    table.create_object(ObjKey(1)).set(col_int, 20).set(col_bool, false); // 1: 7
    table.create_object(ObjKey(2)).set(col_int, 0).set(col_bool, false);  // 2: 0
    table.create_object(ObjKey(3)).set(col_int, 40).set(col_bool, false); // 3: 8
    table.create_object(ObjKey(4)).set(col_int, 15).set(col_bool, false); // 4: 6
    table.create_object(ObjKey(5)).set(col_int, 11).set(col_bool, true);  // 5: 5
    table.create_object(ObjKey(6)).set(col_int, 6).set(col_bool, true);   // 6: 3
    table.create_object(ObjKey(7)).set(col_int, 4).set(col_bool, true);   // 7: 2
    table.create_object(ObjKey(8)).set(col_int, 99).set(col_bool, true);  // 8: 9
    table.create_object(ObjKey(9)).set(col_int, 2).set(col_bool, true);   // 9: 1

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

namespace realm {
template <class T>
T nan(const char* tag)
{
    typename std::conditional<std::is_same<T, float>::value, uint32_t, uint64_t>::type i;
    uint64_t double_nan = 0x7ff8000000000000;
    i = std::is_same<T, float>::value ? 0x7fc00000 : static_cast<decltype(i)>(double_nan);
    i += *tag;
    return type_punning<T>(i);
}
template <>
Decimal128 nan(const char* init)
{
    return Decimal128::nan(init);
}

template <typename T>
inline bool isnan(T val)
{
    return std::isnan(val);
}
inline bool isnan(Decimal128 val)
{
    return val.is_nan();
}

} // namespace realm

TEST_TYPES(Table_SortFloat, float, double, Decimal128)
{
    Table table;
    DataType type = ColumnTypeTraits<TEST_TYPE>::id;
    auto col = table.add_column(type, "value", true);
    ObjKeys keys;
    table.create_objects(900, keys);
    for (size_t i = 0; i < keys.size(); i += 3) {
        table.get_object(keys[i]).set(col, TEST_TYPE(-500.0 + i));
        table.get_object(keys[i + 1]).set_null(col);
        const char nan_tag[] = {char('0' + i % 10), 0};
        table.get_object(keys[i + 2]).set(col, realm::nan<TEST_TYPE>(nan_tag));
    }

    TableView sorted = table.get_sorted_view(SortDescriptor{{{col}}, {true}});
    CHECK_EQUAL(table.size(), sorted.size());

    // nulls should appear first,
    // followed by nans, folllowed by the rest of the values in ascending order
    for (size_t i = 0; i < 300; ++i) {
        CHECK(sorted.get(i).is_null(col));
    }
    for (size_t i = 300; i < 600; ++i) {
        CHECK(realm::isnan(sorted.get(i).get<TEST_TYPE>(col)));
    }
    for (size_t i = 600; i + 1 < 900; ++i) {
        CHECK_GREATER(sorted.get(i + 1).get<TEST_TYPE>(col), sorted.get(i).get<TEST_TYPE>(col));
    }
}

TEST_TYPES(Table_Multi_Sort, int64_t, float, double, Decimal128)
{
    Table table;
    auto col_0 = table.add_column(ColumnTypeTraits<TEST_TYPE>::id, "first");
    auto col_1 = table.add_column(ColumnTypeTraits<TEST_TYPE>::id, "second");

    table.create_object(ObjKey(0)).set_all(TEST_TYPE(1), TEST_TYPE(10));
    table.create_object(ObjKey(1)).set_all(TEST_TYPE(2), TEST_TYPE(10));
    table.create_object(ObjKey(2)).set_all(TEST_TYPE(0), TEST_TYPE(10));
    table.create_object(ObjKey(3)).set_all(TEST_TYPE(2), TEST_TYPE(14));
    table.create_object(ObjKey(4)).set_all(TEST_TYPE(1), TEST_TYPE(14));

    std::vector<std::vector<ColKey>> col_ndx1 = {{col_0}, {col_1}};
    std::vector<bool> asc = {true, true};

    // (0, 10); (1, 10); (1, 14); (2, 10); (2; 14)
    TableView v_sorted1 = table.get_sorted_view(SortDescriptor{col_ndx1, asc});
    CHECK_EQUAL(table.size(), v_sorted1.size());
    CHECK_EQUAL(ObjKey(2), v_sorted1.get_key(0));
    CHECK_EQUAL(ObjKey(0), v_sorted1.get_key(1));
    CHECK_EQUAL(ObjKey(4), v_sorted1.get_key(2));
    CHECK_EQUAL(ObjKey(1), v_sorted1.get_key(3));
    CHECK_EQUAL(ObjKey(3), v_sorted1.get_key(4));

    std::vector<std::vector<ColKey>> col_ndx2 = {{col_1}, {col_0}};

    // (0, 10); (1, 10); (2, 10); (1, 14); (2, 14)
    TableView v_sorted2 = table.get_sorted_view(SortDescriptor{col_ndx2, asc});
    CHECK_EQUAL(table.size(), v_sorted2.size());
    CHECK_EQUAL(ObjKey(2), v_sorted2.get_key(0));
    CHECK_EQUAL(ObjKey(0), v_sorted2.get_key(1));
    CHECK_EQUAL(ObjKey(1), v_sorted2.get_key(2));
    CHECK_EQUAL(ObjKey(4), v_sorted2.get_key(3));
    CHECK_EQUAL(ObjKey(3), v_sorted2.get_key(4));
}

TEST(Table_IndexString)
{
    Table table;
    auto col_int = table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    table.add_search_index(col_str);
    CHECK(table.has_search_index(col_str));

    ObjKey k0 = table.create_object(ObjKey{}, {{col_int, int(Mon)}, {col_str, "jeff"}}).get_key();
    ObjKey k1 = table.create_object(ObjKey{}, {{col_str, "jim"}, {col_int, int(Tue)}}).get_key();
    table.create_object().set_all(int(Wed), "jennifer");
    table.create_object().set_all(int(Thu), "john");
    table.create_object().set_all(int(Fri), "jimmy");
    ObjKey k5 = table.create_object().set_all(int(Sat), "jimbo").get_key();
    // Use a key where the first has the the second most significant bit set.
    // When this is shifted up and down again, the most significant bit must
    // still be 0.
    ObjKey k6 = table.create_object(ObjKey(1LL << 62)).set_all(int(Sun), "johnny").get_key();
    table.create_object().set_all(int(Mon), "jennifer"); // duplicate

    ObjKey r1 = table.find_first_string(col_str, "jimmi");
    CHECK_EQUAL(null_key, r1);

    ObjKey r2 = table.find_first_string(col_str, "jeff");
    ObjKey r3 = table.find_first_string(col_str, "jim");
    ObjKey r4 = table.find_first_string(col_str, "jimbo");
    ObjKey r5 = table.find_first_string(col_str, "johnny");
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
    ObjKey k;

    auto col_int = table.add_column(type_Int, "ints");
    auto col_date = table.add_column(type_Timestamp, "date");
    auto col_bool = table.add_column(type_Bool, "booleans");

    std::vector<ObjKey> keys;
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

TEST(Table_AddIntIndexed)
{
    Table table;
    auto col = table.add_column(DataType(0), "int_1", false);
    Obj obj = table.create_object();
    table.add_search_index(col);
    obj.add_int(col, 8463800223514590069);
    obj.remove();
}

TEST(Table_Distinct)
{
    Table table;
    auto col_int = table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    ObjKey k0 = table.create_object().set_all(int(Mon), "A").get_key();
    ObjKey k1 = table.create_object().set_all(int(Tue), "B").get_key();
    ObjKey k2 = table.create_object().set_all(int(Wed), "C").get_key();
    ObjKey k3 = table.create_object().set_all(int(Thu), "B").get_key();
    ObjKey k4 = table.create_object().set_all(int(Fri), "C").get_key();
    ObjKey k5 = table.create_object().set_all(int(Sat), "D").get_key();
    ObjKey k6 = table.create_object().set_all(int(Sun), "D").get_key();
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

    ObjKey k0 = table.create_object().set(col_bool, true).get_key();
    ObjKey k1 = table.create_object().set(col_bool, false).get_key();
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


TEST(Table_DistincTBasePersistedTable)
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

    ObjKey k0 = table.create_object().set(col, 1).get_key();
    ObjKey k1 = table.create_object().set(col, 15).get_key();
    ObjKey k2 = table.create_object().set(col, 10).get_key();
    ObjKey k3 = table.create_object().set(col, 20).get_key();
    ObjKey k4 = table.create_object().set(col, 11).get_key();
    ObjKey k5 = table.create_object().set(col, 45).get_key();
    ObjKey k6 = table.create_object().set(col, 10).get_key();
    ObjKey k7 = table.create_object().set(col, 0).get_key();
    ObjKey k8 = table.create_object().set(col, 30).get_key();
    ObjKey k9 = table.create_object().set(col, 9).get_key();

    // Create index for column two
    table.add_search_index(col);

    // Search for a value that does not exits
    ObjKey k = table.find_first_int(col, 2);
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
    ObjKey k10 = table.create_object().set(col, 29).get_key();
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

TEST(Table_AutoEnumeration)
{
    Table table;

    auto col_int = table.add_column(type_Int, "first");
    auto col_str = table.add_column(type_String, "second");

    for (size_t i = 0; i < 5; ++i) {
        table.create_object().set_all(1, "abd");
        table.create_object().set_all(2, "eftg");
        table.create_object().set_all(5, "hijkl");
        table.create_object().set_all(8, "mnopqr");
        table.create_object().set_all(9, "stuvxyz");
    }

    table.enumerate_string_column(col_str);

    for (size_t i = 0; i < 5; ++i) {
        const size_t n = i * 5;
        CHECK_EQUAL(1, table.get_object(ObjKey(0 + n)).get<Int>(col_int));
        CHECK_EQUAL(2, table.get_object(ObjKey(1 + n)).get<Int>(col_int));
        CHECK_EQUAL(5, table.get_object(ObjKey(2 + n)).get<Int>(col_int));
        CHECK_EQUAL(8, table.get_object(ObjKey(3 + n)).get<Int>(col_int));
        CHECK_EQUAL(9, table.get_object(ObjKey(4 + n)).get<Int>(col_int));

        CHECK_EQUAL("abd", table.get_object(ObjKey(0 + n)).get<String>(col_str));
        CHECK_EQUAL("eftg", table.get_object(ObjKey(1 + n)).get<String>(col_str));
        CHECK_EQUAL("hijkl", table.get_object(ObjKey(2 + n)).get<String>(col_str));
        CHECK_EQUAL("mnopqr", table.get_object(ObjKey(3 + n)).get<String>(col_str));
        CHECK_EQUAL("stuvxyz", table.get_object(ObjKey(4 + n)).get<String>(col_str));
    }

    // Verify counts
    const size_t count1 = table.count_string(col_str, "abd");
    const size_t count2 = table.count_string(col_str, "eftg");
    const size_t count3 = table.count_string(col_str, "hijkl");
    const size_t count4 = table.count_string(col_str, "mnopqr");
    const size_t count5 = table.count_string(col_str, "stuvxyz");
    CHECK_EQUAL(5, count1);
    CHECK_EQUAL(5, count2);
    CHECK_EQUAL(5, count3);
    CHECK_EQUAL(5, count4);
    CHECK_EQUAL(5, count5);

    ObjKey t = table.find_first_string(col_str, "eftg");
    CHECK_EQUAL(ObjKey(1), t);

    auto tv = table.find_all_string(col_str, "eftg");
    CHECK_EQUAL(5, tv.size());
    CHECK_EQUAL("eftg", tv.get(0).get<String>(col_str));
    CHECK_EQUAL("eftg", tv.get(1).get<String>(col_str));
    CHECK_EQUAL("eftg", tv.get(2).get<String>(col_str));
    CHECK_EQUAL("eftg", tv.get(3).get<String>(col_str));
    CHECK_EQUAL("eftg", tv.get(4).get<String>(col_str));

    Obj obj = table.create_object();
    CHECK_EQUAL(0, obj.get<Int>(col_int));
    CHECK_EQUAL("", obj.get<String>(col_str));
}


TEST(Table_AutoEnumerationOptimize)
{
    Table t;
    auto col0 = t.add_column(type_String, "col1");
    auto col1 = t.add_column(type_String, "col2");
    auto col2 = t.add_column(type_String, "col3");
    auto col3 = t.add_column(type_String, "col4");

    // Insert non-optimizable strings
    std::string s;
    std::vector<ObjKey> keys;
    t.create_objects(10, keys);
    for (Obj o : t) {
        o.set_all(s.c_str(), s.c_str(), s.c_str(), s.c_str());
        s += "x";
    }

    // AutoEnumerate in reverse order
    for (Obj o : t) {
        o.set(col3, "test");
    }
    t.enumerate_string_column(col3);
    for (Obj o : t) {
        o.set(col2, "test");
    }
    t.enumerate_string_column(col2);
    for (Obj o : t) {
        o.set(col1, "test");
    }
    t.enumerate_string_column(col1);
    for (Obj o : t) {
        o.set(col0, "test");
    }
    t.enumerate_string_column(col0);

    for (Obj o : t) {
        CHECK_EQUAL("test", o.get<String>(col0));
        CHECK_EQUAL("test", o.get<String>(col1));
        CHECK_EQUAL("test", o.get<String>(col2));
        CHECK_EQUAL("test", o.get<String>(col3));
    }

#ifdef REALM_DEBUG
    t.verify();
#endif
}

TEST(Table_OptimizeCompare)
{
    Table t1, t2;
    auto col_t1 = t1.add_column(type_String, "str");
    auto col_t2 = t2.add_column(type_String, "str");

    std::vector<ObjKey> keys_t1;
    std::vector<ObjKey> keys_t2;
    t1.create_objects(100, keys_t1);
    for (Obj o : t1) {
        o.set(col_t1, "foo");
    }
    t2.create_objects(100, keys_t2);
    for (Obj o : t2) {
        o.set(col_t2, "foo");
    }
    t1.enumerate_string_column(col_t1);
    CHECK(t1 == t2);
    Obj obj1 = t1.get_object(keys_t1[50]);
    Obj obj2 = t2.get_object(keys_t2[50]);
    obj1.set(col_t1, "bar");
    CHECK(t1 != t2);
    obj1.set(col_t1, "foo");
    CHECK(t1 == t2);
    obj2.set(col_t2, "bar");
    CHECK(t1 != t2);
    obj2.set(col_t2, "foo");
    CHECK(t1 == t2);
}


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
    ObjKey k0 = table.create_object().set_all(2, 20, true, int(Wed)).get_key();
    table.create_object().set_all(3, 10, true, int(Wed));
    ObjKey k1 = table.create_object().set_all(4, 20, true, int(Wed)).get_key();
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

    Obj obj50 = table->get_object(ObjKey(50));
    obj50.set<String>(col, realm::null());
    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(99, r);

    table->enumerate_string_column(col);

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

    table->get_object(ObjKey(55)).set(col, realm::null());
    r = table->where().equal(col, realm::null()).count();
    CHECK_EQUAL(2, r);

    r = table->where().equal(col, "hello").count();
    CHECK_EQUAL(98, r);

    table->remove_object(ObjKey(55));
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
    Obj obj = *t.begin();
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
    auto decimal_col = table.add_column(type_Decimal, "c_decimal");
    int64_t i_sum = 0;
    double f_sum = 0;
    double d_sum = 0;
    Decimal128 decimal_sum(0);

    for (int i = 0; i < TBL_SIZE; i++) {
        table.create_object().set_all(5987654, 4.0f, 3.0, "Hello", Decimal128(7.7));
        i_sum += 5987654;
        f_sum += 4.0f;
        d_sum += 3.0;
        decimal_sum += Decimal128(7.7);
    }
    table.create_object().set_all(1, 1.1f, 1.2, "Hi", Decimal128(8.9));
    table.create_object().set_all(987654321, 11.0f, 12.0, "Goodbye", Decimal128(10.1));
    table.create_object().set_all(5, 4.0f, 3.0, "Hey", Decimal128("1.12e23"));
    i_sum += 1 + 987654321 + 5;
    f_sum += double(1.1f) + double(11.0f) + double(4.0f);
    d_sum += 1.2 + 12.0 + 3.0;
    decimal_sum += Decimal128(8.9) + Decimal128(10.1) + Decimal128("1.12e23");
    double size = TBL_SIZE + 3;

    double epsilon = std::numeric_limits<double>::epsilon();

    // count
    CHECK_EQUAL(1, table.count_int(int_col, 987654321));
    CHECK_EQUAL(1, table.count_float(float_col, 11.0f));
    CHECK_EQUAL(1, table.count_double(double_col, 12.0));
    CHECK_EQUAL(1, table.count_string(str_col, "Goodbye"));
    CHECK_EQUAL(1, table.count_decimal(decimal_col, Decimal128("1.12e23")));
    ObjKey ret;
    // minimum
    CHECK_EQUAL(1, table.minimum_int(int_col, &ret));
    CHECK(ret && table.get_object(ret).get<Int>(int_col) == 1);
    ret = ObjKey();
    CHECK_EQUAL(1.1f, table.minimum_float(float_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(table.get_object(ret).get<Float>(float_col), 1.1f);
    ret = ObjKey();
    CHECK_EQUAL(1.2, table.minimum_double(double_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(table.get_object(ret).get<Double>(double_col), 1.2);
    ret = ObjKey();
    CHECK_EQUAL(Decimal128(7.7), table.minimum_decimal(decimal_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(table.get_object(ret).get<Decimal128>(decimal_col), Decimal128(7.7));

    // maximum
    ret = ObjKey();
    CHECK_EQUAL(987654321, table.maximum_int(int_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(table.get_object(ret).get<Int>(int_col), 987654321);
    ret = ObjKey();
    CHECK_EQUAL(11.0f, table.maximum_float(float_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(11.0f, table.get_object(ret).get<Float>(float_col));
    ret = ObjKey();
    CHECK_EQUAL(12.0, table.maximum_double(double_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(12.0, table.get_object(ret).get<Double>(double_col));
    ret = ObjKey();
    CHECK_EQUAL(Decimal128("1.12e23"), table.maximum_decimal(decimal_col, &ret));
    CHECK(ret);
    CHECK_EQUAL(Decimal128("1.12e23"), table.get_object(ret).get<Decimal128>(decimal_col));
    // sum
    CHECK_APPROXIMATELY_EQUAL(double(i_sum), double(table.sum_int(int_col)), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(f_sum, table.sum_float(float_col), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(d_sum, table.sum_double(double_col), 10 * epsilon);
    CHECK_EQUAL(decimal_sum, table.sum_decimal(decimal_col));
    // average
    size_t count = realm::npos;
    CHECK_APPROXIMATELY_EQUAL(i_sum / size, table.average_int(int_col, &count), 10 * epsilon);
    CHECK_EQUAL(count, size);
    count = realm::npos;
    CHECK_APPROXIMATELY_EQUAL(f_sum / size, table.average_float(float_col, &count), 10 * epsilon);
    CHECK_EQUAL(count, size);
    count = realm::npos;
    CHECK_APPROXIMATELY_EQUAL(d_sum / size, table.average_double(double_col, &count), 10 * epsilon);
    CHECK_EQUAL(count, size);
    count = realm::npos;
    CHECK_EQUAL(decimal_sum / Decimal128(size), table.average_decimal(decimal_col, &count));
    CHECK_EQUAL(count, size);
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

        Obj obj0 = table->create_object(ObjKey(0));
        Obj obj1 = table->create_object(ObjKey(1));
        Obj obj2 = table->create_object(ObjKey(2));

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
        ObjKey pos;
        if (nullable) {
            // max
            pos = 123;
            CHECK_EQUAL(table->maximum_int(col_price), 3);
            CHECK_EQUAL(table->maximum_int(col_price, &pos), 3);
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_float(col_shipping), 30.f);
            CHECK_EQUAL(table->maximum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_double(col_rating), 2.2);
            CHECK_EQUAL(table->maximum_double(col_rating, &pos), 2.2);
            CHECK_EQUAL(pos, ObjKey(1));

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(col_date), Timestamp(6, 6));
            CHECK_EQUAL(table->maximum_timestamp(col_date, &pos), Timestamp(6, 6));
            CHECK_EQUAL(pos, ObjKey(2));

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(col_price), 1);
            CHECK_EQUAL(table->minimum_int(col_price, &pos), 1);
            CHECK_EQUAL(pos, ObjKey(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_float(col_shipping), 30.f);
            CHECK_EQUAL(table->minimum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            CHECK_EQUAL(table->minimum_double(col_rating), 1.1);
            CHECK_EQUAL(table->minimum_double(col_rating, &pos), 1.1);
            CHECK_EQUAL(pos, ObjKey(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_timestamp(col_date), Timestamp(2, 2));
            CHECK_EQUAL(table->minimum_timestamp(col_date, &pos), Timestamp(2, 2));
            CHECK_EQUAL(pos, ObjKey(0));

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
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_float(col_shipping, &pos), 30.f);
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            CHECK_EQUAL(table->maximum_double(col_rating, &pos), 2.2);
            CHECK_EQUAL(pos, ObjKey(1));

            pos = 123;
            CHECK_EQUAL(table->maximum_timestamp(col_date, &pos), Timestamp(6, 6));
            CHECK_EQUAL(pos, ObjKey(2));

            // min
            pos = 123;
            CHECK_EQUAL(table->minimum_int(col_price, &pos), 0);
            CHECK_EQUAL(pos, ObjKey(1));

            pos = 123;
            CHECK_EQUAL(table->minimum_float(col_shipping, &pos), 0.f);
            CHECK_EQUAL(pos, ObjKey(0));

            pos = 123;
            CHECK_EQUAL(table->minimum_double(col_rating, &pos), 0.);
            CHECK_EQUAL(pos, ObjKey(2));

            pos = 123;
            // Timestamp(0, 0) is default value for non-nullable column
            CHECK_EQUAL(table->minimum_timestamp(col_date, &pos), Timestamp(0, 0));
            CHECK_EQUAL(pos, ObjKey(1));

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

    ObjKey min_key;
    Timestamp min_ts = table->minimum_timestamp(col, &min_key);
    CHECK_EQUAL(min_key, null_key);
    CHECK(min_ts.is_null());

    ObjKey max_key;
    Timestamp max_ts = table->maximum_timestamp(col, &max_key);
    CHECK_EQUAL(max_key, null_key);
    CHECK(max_ts.is_null());
}

TEST(Table_EnumStringInsertEmptyRow)
{
    Table table;
    auto col_str = table.add_column(type_String, "strings");
    for (int i = 0; i < 128; ++i)
        table.create_object().set(col_str, "foo");

    CHECK_EQUAL(0, table.get_num_unique_values(col_str));
    table.enumerate_string_column(col_str);
    // Make sure we now have an enumerated strings column
    CHECK_EQUAL(1, table.get_num_unique_values(col_str));
    Obj obj = table.create_object();
    CHECK_EQUAL("", obj.get<String>(col_str));
    CHECK_EQUAL(2, table.get_num_unique_values(col_str));
}

TEST(Table_AddColumnWithThreeLevelBptree)
{
    Table table;
    std::vector<ObjKey> keys;
    table.add_column(type_Int, "int0");
    table.create_objects(REALM_MAX_BPNODE_SIZE * REALM_MAX_BPNODE_SIZE + 1, keys);
    table.add_column(type_Int, "int1");
    table.verify();
}

TEST(Table_DeleteObjectsInFirstCluster)
{
    // Designed to exercise logic if cluster size is 4
    Table table;
    table.add_column(type_Int, "int0");

    ObjKeys keys;
    table.create_objects(32, keys);

    // delete objects in first cluster
    table.remove_object(keys[2]);
    table.remove_object(keys[1]);
    table.remove_object(keys[3]);
    table.remove_object(keys[0]);

    table.create_object(ObjKey(1)); // Must not throw

    // Replace root node
    while (table.size() > 16)
        table.begin()->remove();

    // table.dump_objects();
    table.create_object(ObjKey(1)); // Must not throw
}

TEST(Table_ClearWithTwoLevelBptree)
{
    Table table;
    std::vector<ObjKey> keys;
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


TEST(Table_Nulls)
{
    // 'round' lets us run this entire test both with and without index and with/without optimize/enum
    for (size_t round = 0; round < 5; round++) {
        Table t;
        TableView tv;
        auto col_str = t.add_column(type_String, "str", true /*nullable*/);

        if (round == 1)
            t.add_search_index(col_str);
        else if (round == 2)
            t.enumerate_string_column(col_str);
        else if (round == 3) {
            t.add_search_index(col_str);
            t.enumerate_string_column(col_str);
        }
        else if (round == 4) {
            t.enumerate_string_column(col_str);
            t.add_search_index(col_str);
        }

        std::vector<ObjKey> keys;
        t.create_objects(3, keys);
        t.get_object(keys[0]).set(col_str, "foo"); // short strings
        t.get_object(keys[1]).set(col_str, "");
        t.get_object(keys[2]).set(col_str, StringData()); // null

        CHECK_EQUAL(1, t.count_string(col_str, "foo"));
        CHECK_EQUAL(1, t.count_string(col_str, ""));
        CHECK_EQUAL(1, t.count_string(col_str, realm::null()));

        CHECK_EQUAL(keys[0], t.find_first_string(col_str, "foo"));
        CHECK_EQUAL(keys[1], t.find_first_string(col_str, ""));
        CHECK_EQUAL(keys[2], t.find_first_string(col_str, realm::null()));

        tv = t.find_all_string(col_str, "foo");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[0], tv.get_key(0));
        tv = t.find_all_string(col_str, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[1], tv.get_key(0));
        tv = t.find_all_string(col_str, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[2], tv.get_key(0));

        const char* string_medium = "xxxxxxxxxxYYYYYYYYYY";
        t.get_object(keys[0]).set(col_str, string_medium); // medium strings (< 64)

        CHECK_EQUAL(1, t.count_string(col_str, string_medium));
        CHECK_EQUAL(1, t.count_string(col_str, ""));
        CHECK_EQUAL(1, t.count_string(col_str, realm::null()));

        CHECK_EQUAL(keys[0], t.find_first_string(col_str, string_medium));
        CHECK_EQUAL(keys[1], t.find_first_string(col_str, ""));
        CHECK_EQUAL(keys[2], t.find_first_string(col_str, realm::null()));

        tv = t.find_all_string(col_str, string_medium);
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[0], tv.get_key(0));
        tv = t.find_all_string(col_str, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[1], tv.get_key(0));
        tv = t.find_all_string(col_str, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[2], tv.get_key(0));


        // long strings (>= 64)
        const char* string_long = "xxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxxYYYYYYYYYYxxxxxxxxxx";
        t.get_object(keys[0]).set(col_str, string_long);

        CHECK_EQUAL(1, t.count_string(col_str, string_long));
        CHECK_EQUAL(1, t.count_string(col_str, ""));
        CHECK_EQUAL(1, t.count_string(col_str, realm::null()));

        CHECK_EQUAL(keys[0], t.find_first_string(col_str, string_long));
        CHECK_EQUAL(keys[1], t.find_first_string(col_str, ""));
        CHECK_EQUAL(keys[2], t.find_first_string(col_str, realm::null()));

        tv = t.find_all_string(col_str, string_long);
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[0], tv.get_key(0));
        tv = t.find_all_string(col_str, "");
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[1], tv.get_key(0));
        tv = t.find_all_string(col_str, realm::null());
        CHECK_EQUAL(1, tv.size());
        CHECK_EQUAL(keys[2], tv.get_key(0));
    }

    {
        Table t;
        auto col_int = t.add_column(type_Int, "int", true);         // nullable = true
        auto col_bool = t.add_column(type_Bool, "bool", true);      // nullable = true
        auto col_date = t.add_column(type_Timestamp, "date", true); // nullable = true

        Obj obj0 = t.create_object();
        Obj obj1 = t.create_object();
        ObjKey k0 = obj0.get_key();
        ObjKey k1 = obj1.get_key();

        obj0.set(col_int, 65);
        obj0.set(col_bool, false);
        obj0.set(col_date, Timestamp(3, 0));

        CHECK_EQUAL(65, obj0.get<Int>(col_int));
        CHECK_EQUAL(false, obj0.get<Bool>(col_bool));
        CHECK_EQUAL(Timestamp(3, 0), obj0.get<Timestamp>(col_date));

        CHECK_EQUAL(65, t.maximum_int(col_int));
        CHECK_EQUAL(65, t.minimum_int(col_int));
        CHECK_EQUAL(Timestamp(3, 0), t.maximum_timestamp(col_date));
        CHECK_EQUAL(Timestamp(3, 0), t.minimum_timestamp(col_date));

        CHECK_NOT(obj0.is_null(col_int));
        CHECK_NOT(obj0.is_null(col_bool));
        CHECK_NOT(obj0.is_null(col_date));

        CHECK_THROW_ANY(obj1.get<Int>(col_int));
        CHECK(obj1.is_null(col_int));
        CHECK(obj1.is_null(col_bool));
        CHECK(obj1.is_null(col_date));

        CHECK_EQUAL(k1, t.find_first_null(col_int));
        CHECK_EQUAL(k1, t.find_first_null(col_bool));
        CHECK_EQUAL(k1, t.find_first_null(col_date));

        CHECK_EQUAL(null_key, t.find_first_int(col_int, -1));
        CHECK_EQUAL(null_key, t.find_first_bool(col_bool, true));
        CHECK_EQUAL(null_key, t.find_first_timestamp(col_date, Timestamp(5, 0)));

        CHECK_EQUAL(k0, t.find_first_int(col_int, 65));
        CHECK_EQUAL(k0, t.find_first_bool(col_bool, false));
        CHECK_EQUAL(k0, t.find_first_timestamp(col_date, Timestamp(3, 0)));

        obj0.set_null(col_int);
        obj0.set_null(col_bool);
        obj0.set_null(col_date);

        CHECK(obj0.is_null(col_int));
        CHECK(obj0.is_null(col_bool));
        CHECK(obj0.is_null(col_date));
    }
    {
        Table t;
        auto col_float = t.add_column(type_Float, "float", true);    // nullable = true
        auto col_double = t.add_column(type_Double, "double", true); // nullable = true

        Obj obj0 = t.create_object();
        Obj obj1 = t.create_object();
        ObjKey k0 = obj0.get_key();
        ObjKey k1 = obj1.get_key();

        obj0.set_all(1.23f, 12.3);

        CHECK_EQUAL(1.23f, obj0.get<float>(col_float));
        CHECK_EQUAL(12.3, obj0.get<double>(col_double));

        CHECK_EQUAL(1.23f, t.maximum_float(col_float));
        CHECK_EQUAL(1.23f, t.minimum_float(col_float));
        CHECK_EQUAL(12.3, t.maximum_double(col_double));
        CHECK_EQUAL(12.3, t.minimum_double(col_double));

        CHECK_NOT(obj0.is_null(col_float));
        CHECK_NOT(obj0.is_null(col_double));

        CHECK(obj1.is_null(col_float));
        CHECK(obj1.is_null(col_double));

        CHECK_EQUAL(k1, t.find_first_null(col_float));
        CHECK_EQUAL(k1, t.find_first_null(col_double));

        CHECK_EQUAL(null_key, t.find_first_float(col_float, 2.22f));
        CHECK_EQUAL(null_key, t.find_first_double(col_double, 2.22));

        CHECK_EQUAL(k0, t.find_first_float(col_float, 1.23f));
        CHECK_EQUAL(k0, t.find_first_double(col_double, 12.3));

        util::Optional<Float> f_val = 5.f;
        obj0.set(col_float, f_val);
        CHECK_NOT(obj0.is_null(col_float));
        CHECK_EQUAL(obj0.get<Optional<float>>(col_float), 5.f);

        util::Optional<Double> d_val = 5.;
        obj0.set(col_double, d_val);
        CHECK_NOT(obj0.is_null(col_double));
        CHECK_EQUAL(obj0.get<Optional<double>>(col_double), 5.);

        obj0.set_null(col_float);
        obj0.set_null(col_double);

        CHECK(obj0.is_null(col_float));
        CHECK(obj0.is_null(col_double));
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
        BPlusTree<BinaryData> c(Allocator::get_default());
        c.create();

        c.add(BinaryData(buf.get(), 13000000));
        c.set(0, BinaryData(buf.get(), 14000000));

        c.destroy();
    }

    // Now a small fuzzy test to catch other such bugs
    {
        Table t;
        std::vector<ObjKey> keys;
        auto col_bin = t.add_column(type_Binary, "Binaries", true);

        for (size_t j = 0; j < 100; j++) {
            size_t r = (j * 123456789 + 123456789) % 100;
            if (r < 20) {
                keys.push_back(t.create_object().get_key());
            }
            else if (t.size() > 0 && t.size() < 5) {
                // Set only if there are no more than 4 rows, else it takes up too much space on devices (4 * 16 MB
                // worst case now)
                size_t row = (j * 123456789 + 123456789) % t.size();
                size_t len = (j * 123456789 + 123456789) % 16000000;
                BinaryData bd;
                bd = BinaryData(buf.get(), len);
                t.get_object(keys[row]).set(col_bin, bd);
            }
            else if (t.size() >= 4) {
                t.clear();
                keys.clear();
            }
        }
    }
}


TEST(Table_DetachedAccessor)
{
    Group group;
    TableRef table = group.add_table("table");
    auto col_int = table->add_column(type_Int, "i");
    auto col_str = table->add_column(type_String, "s");
    table->add_column(type_Binary, "b");
    table->add_column_link(type_Link, "l", *table);
    ObjKey key0 = table->create_object().get_key();
    Obj obj1 = table->create_object();
    group.remove_table("table");

    CHECK_THROW(table->clear(), NoSuchTable);
    CHECK_THROW(table->add_search_index(col_int), NoSuchTable);
    CHECK_THROW(table->remove_search_index(col_int), NoSuchTable);
    CHECK_THROW(table->get_object(key0), NoSuchTable);
    CHECK_THROW_ANY(obj1.set(col_str, "hello"));
}

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
    CHECK_LOGIC_ERROR(obj.set(col_link, ObjKey(45)), LogicError::target_row_index_out_of_range);
    CHECK(obj.is_null(col_link));
    CHECK_EQUAL(t->size(), 3);
    ObjKey k = t->create_object().get_key();
    obj.set(col_link, k);
    CHECK_EQUAL(obj.get<ObjKey>(col_link), k);
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

    table.create_object(ObjKey(5)).set_all(100, 7);
    CHECK_EQUAL(table.size(), 1);
    CHECK_THROW(table.create_object(ObjKey(5)), KeyAlreadyUsed);
    CHECK_EQUAL(table.size(), 1);
    table.create_object(ObjKey(2));
    Obj x = table.create_object(ObjKey(7));
    table.create_object(ObjKey(8));
    table.create_object(ObjKey(10));
    table.create_object(ObjKey(6));

    Obj y = table.get_object(ObjKey(5));

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
    table.remove_object(ObjKey(5));
    CHECK_THROW(y.get<int64_t>(intnull_col), KeyNotFound);

    CHECK(table.get_object(ObjKey(8)).is_null(intnull_col));
}


TEST(Table_ObjectsWithNoColumns)
{
    Table table;
    std::vector<ObjKey> keys;
    table.create_objects(REALM_MAX_BPNODE_SIZE * 2, keys);
    CHECK_NOT(table.is_empty());
    CHECK_EQUAL(table.size(), REALM_MAX_BPNODE_SIZE * 2);
    for (ObjKey k : keys) {
        Obj obj = table.get_object(k);
        CHECK(obj.is_valid());
        obj.remove();
        CHECK(!obj.is_valid());
    }
    CHECK(table.is_empty());
    CHECK_EQUAL(table.size(), 0);
}

TEST(Table_remove_column)
{
    Table table;
    table.add_column(type_Int, "int1");
    auto int2_col = table.add_column(type_Int, "int2");
    table.add_column(type_Int, "int3");

    Obj obj = table.create_object(ObjKey(5)).set_all(100, 7, 25);

    CHECK_EQUAL(obj.get<int64_t>("int1"), 100);
    CHECK_EQUAL(obj.get<int64_t>("int2"), 7);
    CHECK_EQUAL(obj.get<int64_t>("int3"), 25);

    table.remove_column(int2_col);

    CHECK_EQUAL(obj.get<int64_t>("int1"), 100);
    CHECK_THROW(obj.get<int64_t>("int2"), LogicError);
    CHECK_EQUAL(obj.get<int64_t>("int3"), 25);
    table.add_column(type_Int, "int4");
    CHECK_EQUAL(obj.get<int64_t>("int4"), 0);
}

TEST(Table_list_basic)
{
    Table table;
    auto list_col = table.add_column_list(type_Int, "int_list");
    int sum = 0;

    {
        Obj obj = table.create_object(ObjKey(5));
        CHECK_NOT(obj.is_null(list_col));
        auto list = obj.get_list<int64_t>(list_col);
        CHECK_NOT(obj.is_null(list_col));
        CHECK(list.is_empty());

        size_t return_cnt = 0;
        list.sum(&return_cnt);
        CHECK_EQUAL(return_cnt, 0);
        list.max(&return_cnt);
        CHECK_EQUAL(return_cnt, 0);
        list.min(&return_cnt);
        CHECK_EQUAL(return_cnt, 0);
        list.avg(&return_cnt);
        CHECK_EQUAL(return_cnt, 0);

        for (int i = 0; i < 100; i++) {
            list.add(i + 1000);
            sum += (i + 1000);
        }
    }
    {
        Obj obj = table.get_object(ObjKey(5));
        auto list1 = obj.get_list<int64_t>(list_col);
        CHECK_EQUAL(list1.size(), 100);
        CHECK_EQUAL(list1.get(0), 1000);
        CHECK_EQUAL(list1.get(99), 1099);
        auto list_base = obj.get_listbase_ptr(list_col);
        CHECK_EQUAL(list_base->size(), 100);
        CHECK(dynamic_cast<Lst<Int>*>(list_base.get()));

        CHECK_EQUAL(list1.sum(), sum);
        CHECK_EQUAL(list1.max(), 1099);
        CHECK_EQUAL(list1.min(), 1000);
        CHECK_EQUAL(list1.avg(), double(sum) / 100);

        auto list2 = obj.get_list<int64_t>(list_col);
        list2.set(50, 747);
        CHECK_EQUAL(list1.get(50), 747);
        list1.resize(101);
        CHECK_EQUAL(list1.get(100), 0);
        list1.resize(50);
        CHECK_EQUAL(list1.size(), 50);
    }
    {
        Obj obj = table.create_object(ObjKey(7));
        auto list = obj.get_list<int64_t>(list_col);
        list.resize(10);
        CHECK_EQUAL(list.size(), 10);
        for (int i = 0; i < 10; i++) {
            CHECK_EQUAL(list.get(i), 0);
        }
    }
    table.remove_object(ObjKey(5));
}

template <typename T>
struct NullableTypeConverter {
    using NullableType = util::Optional<T>;
    static bool is_null(NullableType t)
    {
        return !bool(t);
    }
};

template <>
struct NullableTypeConverter<Decimal128> {
    using NullableType = Decimal128;
    static bool is_null(Decimal128 val)
    {
        return val.is_null();
    }
};

TEST_TYPES(Table_list_nullable, int64_t, float, double, Decimal128)
{
    Table table;
    auto list_col = table.add_column_list(ColumnTypeTraits<TEST_TYPE>::id, "int_list", true);
    ColumnSumType<TEST_TYPE> sum = TEST_TYPE(0);

    {
        Obj obj = table.create_object(ObjKey(5));
        CHECK_NOT(obj.is_null(list_col));
        auto list = obj.get_list<typename NullableTypeConverter<TEST_TYPE>::NullableType>(list_col);
        CHECK_NOT(obj.is_null(list_col));
        CHECK(list.is_empty());
        for (int i = 0; i < 100; i++) {
            TEST_TYPE val = TEST_TYPE(i + 1000);
            list.add(val);
            sum += (val);
        }
    }
    {
        Obj obj = table.get_object(ObjKey(5));
        auto list1 = obj.get_list<typename NullableTypeConverter<TEST_TYPE>::NullableType>(list_col);
        CHECK_EQUAL(list1.size(), 100);
        CHECK_EQUAL(list1.get(0), TEST_TYPE(1000));
        CHECK_EQUAL(list1.get(99), TEST_TYPE(1099));
        CHECK_NOT(list1.is_null(0));
        auto list_base = obj.get_listbase_ptr(list_col);
        CHECK_EQUAL(list_base->size(), 100);
        CHECK_NOT(list_base->is_null(0));
        CHECK(dynamic_cast<Lst<typename NullableTypeConverter<TEST_TYPE>::NullableType>*>(list_base.get()));

        CHECK_EQUAL(list1.sum(), sum);
        CHECK_EQUAL(list1.max(), TEST_TYPE(1099));
        CHECK_EQUAL(list1.min(), TEST_TYPE(1000));
        CHECK_EQUAL(list1.avg(), typename ColumnTypeTraits<TEST_TYPE>::average_type(sum) / 100);

        auto list2 = obj.get_list<typename NullableTypeConverter<TEST_TYPE>::NullableType>(list_col);
        list2.set(50, TEST_TYPE(747));
        CHECK_EQUAL(list1.get(50), TEST_TYPE(747));
        list1.set_null(50);
        CHECK(NullableTypeConverter<TEST_TYPE>::is_null(list1.get(50)));
        list1.resize(101);
        CHECK(NullableTypeConverter<TEST_TYPE>::is_null(list1.get(100)));
    }
    {
        Obj obj = table.create_object(ObjKey(7));
        auto list = obj.get_list<typename NullableTypeConverter<TEST_TYPE>::NullableType>(list_col);
        list.resize(10);
        CHECK_EQUAL(list.size(), 10);
        for (int i = 0; i < 10; i++) {
            CHECK(NullableTypeConverter<TEST_TYPE>::is_null(list.get(i)));
        }
    }
    table.remove_object(ObjKey(5));
}

TEST(Table_StableIteration)
{
    Table table;
    auto list_col = table.add_column_list(type_Int, "int_list");
    std::vector<int64_t> values = {1, 7, 3, 5, 5, 2, 4};
    Obj obj = table.create_object(ObjKey(5)).set_list_values(list_col, values);

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

TEST(Table_ListOps)
{
    Table table;
    ColKey col = table.add_column_list(type_Int, "integers");

    Obj obj = table.create_object();
    Obj obj1 = obj;
    Lst<Int> list = obj.get_list<Int>(col);
    list.add(1);
    list.add(2);
    list.swap(0, 1);
    CHECK_EQUAL(list.get(0), 2);
    CHECK_EQUAL(list.get(1), 1);

    Lst<Int> list1;
    CHECK_EQUAL(list1.size(), 0);
    list1 = list;
    CHECK_EQUAL(list1.size(), 2);
    list.add(3);
    CHECK_EQUAL(list.size(), 3);
    CHECK_EQUAL(list1.size(), 3);

    Lst<Int> list2 = list;
    CHECK_EQUAL(list2.size(), 3);
}

TEST(Table_ListOfPrimitives)
{
    Group g;
    std::vector<CollectionBase*> lists;
    TableRef t = g.add_table("table");
    ColKey int_col = t->add_column_list(type_Int, "integers");
    ColKey bool_col = t->add_column_list(type_Bool, "booleans");
    ColKey string_col = t->add_column_list(type_String, "strings");
    ColKey double_col = t->add_column_list(type_Double, "doubles");
    ColKey timestamp_col = t->add_column_list(type_Timestamp, "timestamps");
    Obj obj = t->create_object(ObjKey(7));

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
    lists.push_back(&int_list);
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
    CHECK_EQUAL(lists[0]->size(), 2);
    CHECK_EQUAL(lists[0]->get_col_key(), int_col);

    int_list.clear();
    auto int_list2 = obj.get_list<int64_t>(int_col);
    CHECK_EQUAL(0, int_list2.size());

    CHECK_THROW_ANY(obj.get_list<util::Optional<int64_t>>(int_col));

    auto bool_list = obj.get_list<bool>(bool_col);
    lists.push_back(&bool_list);
    CHECK_EQUAL(bool_vector.size(), bool_list.size());
    for (unsigned i = 0; i < bool_list.size(); i++) {
        CHECK_EQUAL(bool_vector[i], bool_list[i]);
    }

    auto bool_list_nullable = obj.get_list<util::Optional<bool>>(bool_col);
    CHECK_THROW_ANY(bool_list_nullable.set(0, util::none));

    auto string_list = obj.get_list<StringData>(string_col);
    auto str_min = string_list.min();
    CHECK(str_min.is_null());
    CHECK_EQUAL(string_list.begin()->size(), string_vector.begin()->size());
    CHECK_EQUAL(string_vector.size(), string_list.size());
    for (unsigned i = 0; i < string_list.size(); i++) {
        CHECK_EQUAL(string_vector[i], string_list[i]);
    }

    string_list.insert(2, "Wednesday");
    CHECK_EQUAL(string_vector.size() + 1, string_list.size());
    CHECK_EQUAL(StringData("Wednesday"), string_list.get(2));
    CHECK_THROW_ANY(string_list.set(2, StringData{}));
    CHECK_THROW_ANY(string_list.add(StringData{}));
    CHECK_THROW_ANY(string_list.insert(2, StringData{}));

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
    size_t return_ndx = 7;
    timestamp_list.min(&return_ndx);
    CHECK_EQUAL(return_ndx, 0);
    timestamp_list.max(&return_ndx);
    CHECK_EQUAL(return_ndx, 1);

    t->remove_object(ObjKey(7));
    CHECK_NOT(timestamp_list.is_attached());
}

TEST_TYPES(Table_ListOfPrimitivesSort, int64_t, float, double, Decimal128, ObjectId, Timestamp, Optional<int64_t>,
           Optional<float>, Optional<double>, Optional<ObjectId>)
{
    using underlying_type = typename util::RemoveOptional<TEST_TYPE>::type;
    constexpr bool is_optional = !std::is_same<underlying_type, TEST_TYPE>::value;

    Group g;
    TableRef t = g.add_table("table");
    ColKey col = t->add_column_list(ColumnTypeTraits<TEST_TYPE>::id, "values", is_optional);

    auto obj = t->create_object();
    auto list = obj.get_list<TEST_TYPE>(col);

    std::vector<TEST_TYPE> values =
        values_from_int<TEST_TYPE, underlying_type>({9, 4, 2, 7, 4, 1, 8, 11, 3, 4, 5, 22});
    std::vector<size_t> indices;
    obj.set_list_values(col, values);

    CHECK(list.has_changed());
    CHECK_NOT(list.has_changed());

    auto cmp = [&]() {
        CHECK_EQUAL(values.size(), indices.size());
        for (size_t i = 0; i < values.size(); i++) {
            CHECK_EQUAL(values[i], list.get(indices[i]));
        }
    };
    std::sort(values.begin(), values.end(), ::less());
    list.sort(indices);
    cmp();
    std::sort(values.begin(), values.end(), ::greater());
    list.sort(indices, false);
    cmp();
    CHECK_NOT(list.has_changed());

    TEST_TYPE new_value = convert_for_test<underlying_type>(6);
    values.push_back(new_value);
    list.add(new_value);
    CHECK(list.has_changed());
    std::sort(values.begin(), values.end(), ::less());
    list.sort(indices);
    cmp();

    values.resize(7);
    obj.set_list_values(col, values);
    std::sort(values.begin(), values.end(), ::greater());
    list.sort(indices, false);
    cmp();
}

TEST_TYPES(Table_ListOfPrimitivesDistinct, int64_t, float, double, Decimal128, ObjectId, Timestamp, Optional<int64_t>,
           Optional<float>, Optional<double>, Optional<ObjectId>)
{
    using underlying_type = typename util::RemoveOptional<TEST_TYPE>::type;
    constexpr bool is_optional = !std::is_same<underlying_type, TEST_TYPE>::value;
    Group g;
    TableRef t = g.add_table("table");
    ColKey col = t->add_column_list(ColumnTypeTraits<underlying_type>::id, "values", is_optional);

    auto obj = t->create_object();
    auto list = obj.get_list<TEST_TYPE>(col);

    std::vector<TEST_TYPE> values = values_from_int<TEST_TYPE, underlying_type>({9, 4, 2, 7, 4, 9, 8, 11, 2, 4, 5});
    std::vector<TEST_TYPE> distinct_values = values_from_int<TEST_TYPE, underlying_type>({9, 4, 2, 7, 8, 11, 5});
    std::vector<size_t> indices;
    obj.set_list_values(col, values);

    auto cmp = [&]() {
        CHECK_EQUAL(distinct_values.size(), indices.size());
        for (size_t i = 0; i < distinct_values.size(); i++) {
            CHECK_EQUAL(distinct_values[i], list.get(indices[i]));
        }
    };

    list.distinct(indices);
    cmp();
    list.distinct(indices, true);
    std::sort(distinct_values.begin(), distinct_values.end(), std::less<TEST_TYPE>());
    cmp();
    list.distinct(indices, false);
    std::sort(distinct_values.begin(), distinct_values.end(), std::greater<TEST_TYPE>());
    cmp();
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
        table.create_object(ObjKey(i)).set_all(i << 1, i << 2);
        key_set.push_back(i);
    }

    for (int i = 0; i < nb_rows; i++) {
        auto key_index = test_util::random_int<int64_t>(0, key_set.size() - 1);
        auto it = key_set.begin() + int(key_index);

        // table.dump_objects();
        // std::cout << "Key to remove: " << std::hex << *it << std::dec << std::endl;

        table.remove_object(ObjKey(*it));
        key_set.erase(it);
        for (unsigned j = 0; j < key_set.size(); j += 23) {
            int64_t key_val = key_set[j];
            Obj o = table.get_object(ObjKey(key_val));
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
        table.create_object(ObjKey(i));
    }

    size_t tree_size = 0;
    auto f = [&tree_size](const Cluster* cluster) {
        tree_size += cluster->node_size();
        return false;
    };
    table.traverse_clusters(f);
    CHECK_EQUAL(tree_size, size_t(nb_rows));

    for (Obj o : table) {
        int64_t key_value = o.get_key().value;
        o.set_all(key_value << 1, key_value << 2);
    }

    // table.dump_objects();

    size_t ndx = 0;
    for (Obj o : table) {
        int64_t key_value = o.get_key().value;
        // std::cout << "Key value: " << std::hex << key_value << std::dec << std::endl;
        CHECK_EQUAL(key_value << 1, o.get<int64_t>(c0));
        CHECK_EQUAL(key_value << 2, o.get<util::Optional<int64_t>>(c1));

        Obj x = table.get_object(ndx);
        CHECK_EQUAL(o.get_key(), x.get_key());
        CHECK_EQUAL(o.get<int64_t>(c0), x.get<int64_t>(c0));
        ndx++;
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
    ObjKey key = it1->get_key();
    ++it1;
    int64_t val = it1->get<int64_t>(c0);
    table.remove_object(key);
    CHECK_EQUAL(val, it1->get<int64_t>(c0));

    val = (it1 + 2)->get<int64_t>(c0);
    table.remove_object(it1);
    CHECK_THROW_ANY(it1->get<int64_t>(c0));
    // Still invalid
    CHECK_THROW_ANY(it1->get<int64_t>(c0));
    it1 += 0;
    // Still invalid
    CHECK_THROW_ANY(it1->get<int64_t>(c0));
    it1 += 2;
    CHECK_EQUAL(val, it1->get<int64_t>(c0));
}

TEST(Table_object_by_index)
{
    Table table;

    ObjKeys keys({17, 4, 345, 65, 1, 46, 93, 43, 76, 123, 33, 42, 99, 53, 52, 256, 2}); // 17 elements
    std::map<ObjKey, size_t> positions;
    table.create_objects(keys);
    size_t sz = table.size();
    CHECK_EQUAL(sz, keys.size());
    for (size_t i = 0; i < sz; i++) {
        Obj o = table.get_object(i);
        auto it = std::find(keys.begin(), keys.end(), o.get_key());
        CHECK(it != keys.end());
        positions.emplace(o.get_key(), i);
    }
    for (auto k : keys) {
        size_t ndx = table.get_object_ndx(k);
        CHECK_EQUAL(ndx, positions[k]);
    }
}

// String query benchmark
TEST(Table_QuickSort2)
{
    Table ttt;
    auto strings = ttt.add_column(type_String, "2");

    for (size_t t = 0; t < 1000; t++) {
        Obj o = ttt.create_object();
        std::string s = util::to_string(t % 100);
        o.set<StringData>(strings, s);
    }

    Query q = ttt.where().equal(strings, "10");

    auto t1 = steady_clock::now();

    CALLGRIND_START_INSTRUMENTATION;

    size_t nb_reps = 1000;
    for (size_t t = 0; t < nb_reps; t++) {
        TableView tv = q.find_all();
        CHECK_EQUAL(tv.size(), 10);
    }

    CALLGRIND_STOP_INSTRUMENTATION;

    auto t2 = steady_clock::now();

    std::cout << nb_reps << " repetitions of find_all" << std::endl;
    std::cout << "    time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_reps << " ns/rep" << std::endl;
}

TEST(Table_object_sequential)
{
#ifdef PERFORMACE_TESTING
    int nb_rows = 10'000'000;
    int num_runs = 1;
#else
    int nb_rows = 100'000;
    int num_runs = 1;
#endif
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey c0;
    ColKey c1;

    CALLGRIND_START_INSTRUMENTATION;

    std::cout << nb_rows << " rows - sequential" << std::endl;

    {
        WriteTransaction wt(sg);
        auto table = wt.add_table("test");

        c0 = table->add_column(type_Int, "int1");
        c1 = table->add_column(type_Int, "int2", true);


        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            table->create_object(ObjKey(i)).set_all(i << 1, i << 2);
        }

        auto t2 = steady_clock::now();
        std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;

        CHECK_EQUAL(table->size(), nb_rows);
        wt.commit();
    }
    {
        auto t1 = steady_clock::now();
        sg->compact();
        auto t2 = steady_clock::now();
        std::cout << "  compaction time: " << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    }
    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(i));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup obj    : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(i));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup field  : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(i));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup same   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        WriteTransaction wt(sg);
        auto table = wt.get_table("test");

        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            Obj o = table->get_object(ObjKey(i));
            o.set(c0, i << 2).set(c1, i << 1);
        }

        auto t2 = steady_clock::now();

        std::cout << "   update time   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        auto table = wt.get_table("test");

        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            table->remove_object(ObjKey(i));
#ifdef REALM_DEBUG
            CHECK_EQUAL(table->size(), nb_rows - i - 1);

            for (int j = i + 1; j < nb_rows; j += nb_rows / 100) {
                Obj o = table->get_object(ObjKey(j));
                CHECK_EQUAL(j << 2, o.get<int64_t>(c0));
                CHECK_EQUAL(j << 1, o.get<util::Optional<int64_t>>(c1));
            }

#endif
        }
        auto t2 = steady_clock::now();
        std::cout << "   erase time    : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;

        wt.commit();
    }

    CALLGRIND_STOP_INSTRUMENTATION;
}

TEST(Table_object_seq_rnd)
{
#ifdef PERFORMACE_TESTING
    size_t rows = 1'000'000;
    int runs = 100;     // runs for building scenario
#else
    size_t rows = 100'000;
    int runs = 100;
#endif
    int64_t next_key = 0;
    std::vector<int64_t> key_values;
    std::set<int64_t> key_set;
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey c0;
    {
        std::cout << "Establishing scenario seq ins/rnd erase " << std::endl;
        WriteTransaction wt(sg);
        auto table = wt.add_table("test");
        c0 = table->add_column(type_Int, "int1");

        for (int run = 0; run < runs; ++run) {
            if (key_values.size() < rows) { // expanding by 2%!
                for (size_t n = 0; n < rows / 50; ++n) {
                    auto key_val = next_key++;
                    key_values.push_back(key_val);
                    key_set.insert(key_val);
                    table->create_object(ObjKey(key_val)).set_all(key_val << 1);
                }
            }
            // do 1% random deletions
            for (size_t n = 0; n < rows / 100; ++n) {
                auto index = test_util::random_int<size_t>(0, key_values.size() - 1);
                auto key_val = key_values[index];
                if (index < key_values.size() - 1)
                    key_values[index] = key_values.back();
                key_values.pop_back();
                table->remove_object(ObjKey(key_val));
            }
        }
        wt.commit();
    }
    // scenario established!
    int nb_rows = int(key_values.size());
#ifdef PERFORMACE_TESTING
    int num_runs = 10; // runs for timing access
#else
    int num_runs = 1; // runs for timing access
#endif
    {
        auto t1 = steady_clock::now();
        sg->compact();
        auto t2 = steady_clock::now();
        std::cout << "  compaction time: " << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    }
    std::cout << "Scenario has " << nb_rows << " rows. Timing...." << std::endl;
    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup obj    : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
                CHECK_EQUAL(key_values[i] << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup field  : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
                CHECK_EQUAL(key_values[i] << 1, o.get<int64_t>(c0));
                CHECK_EQUAL(key_values[i] << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup same   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }
}


TEST(Table_object_random)
{
#ifdef PERFORMACE_TESTING
    int nb_rows = 1'000'000;
    int num_runs = 10;
#else
    int nb_rows = 100'000;
    int num_runs = 1;
#endif
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    ColKey c0;
    ColKey c1;
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

    CALLGRIND_START_INSTRUMENTATION;

    std::cout << nb_rows << " rows - random" << std::endl;

    {
        WriteTransaction wt(sg);
        auto table = wt.add_table("test");

        c0 = table->add_column(type_Int, "int1");
        c1 = table->add_column(type_Int, "int2", true);


        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            table->create_object(ObjKey(key_values[i])).set_all(i << 1, i << 2);
        }


        auto t2 = steady_clock::now();
        std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;

        CHECK_EQUAL(table->size(), nb_rows);
        wt.commit();
    }
    {
        auto t1 = steady_clock::now();
        sg->compact();
        auto t2 = steady_clock::now();
        std::cout << "  compaction time: " << duration_cast<milliseconds>(t2 - t1).count() << " ms" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup obj    : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup field  : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        ReadTransaction rt(sg);
        auto table = rt.get_table("test");

        auto t1 = steady_clock::now();

        for (int j = 0; j < num_runs; ++j) {
            for (int i = 0; i < nb_rows; i++) {
                const Obj o = table->get_object(ObjKey(key_values[i]));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
                CHECK_EQUAL(i << 1, o.get<int64_t>(c0));
            }
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup same   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows / num_runs
                  << " ns/key" << std::endl;
    }

    {
        WriteTransaction wt(sg);
        auto table = wt.get_table("test");

        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            Obj o = table->get_object(ObjKey(key_values[i]));
            o.set(c0, i << 2).set(c1, i << 1);
        }

        auto t2 = steady_clock::now();

        std::cout << "   update time   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;
        wt.commit();
    }

    {
        WriteTransaction wt(sg);
        auto table = wt.get_table("test");

        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            table->remove_object(ObjKey(key_values[i]));
#ifdef REALM_DEBUG
            CHECK_EQUAL(table->size(), nb_rows - i - 1);
            for (int j = i + 1; j < nb_rows; j += nb_rows / 100) {
                Obj o = table->get_object(ObjKey(key_values[j]));
                CHECK_EQUAL(j << 2, o.get<int64_t>(c0));
                CHECK_EQUAL(j << 1, o.get<util::Optional<int64_t>>(c1));
            }
#endif
        }
        auto t2 = steady_clock::now();
        std::cout << "   erase time    : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
                  << std::endl;

        wt.commit();
    }

    CALLGRIND_STOP_INSTRUMENTATION;
}

TEST(Table_CollisionMapping)
{

#if REALM_EXERCISE_OBJECT_ID_COLLISION
    bool expect_collisions = true;
#else
    bool expect_collisions = false;
#endif

    // This number corresponds to the mask used to calculate "optimistic"
    // object IDs. See `ObjectIDProvider::get_optimistic_local_id_hashed`.
    const size_t num_objects_with_guaranteed_collision = 0xff;

    SHARED_GROUP_TEST_PATH(path);

    {
        DBRef sg = DB::create(path);
        {
            auto wt = sg->start_write();
            TableRef t0 = wt->add_table_with_primary_key("class_t0", type_String, "pk");

            char buffer[12];
            for (size_t i = 0; i < num_objects_with_guaranteed_collision; ++i) {
                const char* in = reinterpret_cast<char*>(&i);
                size_t len = base64_encode(in, sizeof(i), buffer, sizeof(buffer));

                t0->create_object_with_primary_key(StringData{buffer, len});
            }
            wt->commit();
        }

        {
            ReadTransaction rt{sg};
            ConstTableRef t0 = rt.get_table("class_t0");
            // Check that at least one object exists where the 63rd bit is set.
            size_t num_object_keys_with_63rd_bit_set = 0;
            uint64_t bit63 = 0x4000000000000000;
            for (Obj obj : *t0) {
                if (obj.get_key().value & bit63)
                    ++num_object_keys_with_63rd_bit_set;
            }
            CHECK(!expect_collisions || num_object_keys_with_63rd_bit_set > 0);
        }
    }

    // Check that locally allocated IDs are properly persisted
    {
        DBRef sg_2 = DB::create(path);
        {
            WriteTransaction wt{sg_2};
            TableRef t0 = wt.get_table("class_t0");

            // Make objects with primary keys that do not already exist but are guaranteed
            // to cause further collisions.
            char buffer[12];
            for (size_t i = 0; i < num_objects_with_guaranteed_collision; ++i) {
                size_t foo = num_objects_with_guaranteed_collision + i;
                const char* in = reinterpret_cast<char*>(&foo);
                size_t len = base64_encode(in, sizeof(foo), buffer, sizeof(buffer));

                t0->create_object_with_primary_key(StringData{buffer, len});
            }
            wt.commit();
        }
        {
            WriteTransaction wt{sg_2};
            TableRef t0 = wt.get_table("class_t0");

            // Find an object with collision key
            std::string pk;
            ObjKey key;
            uint64_t bit63 = 0x4000000000000000;
            for (Obj obj : *t0) {
                if (obj.get_key().value & bit63) {
                    key = obj.get_key();
                    pk = obj.get<String>("pk");
                    obj.remove();
                    break;
                }
            }

            if (key) {
                // Insert object again - should get a different key
                auto obj = t0->create_object_with_primary_key(pk);
                CHECK_NOT_EQUAL(obj.get_key(), key);
            }

            wt.commit();
        }
    }
}

TEST(Table_PrimaryKeyString)
{
#ifdef REALM_DEBUG
    int nb_rows = 1000;
#else
    int nb_rows = 100000;
#endif
    SHARED_GROUP_TEST_PATH(path);

    DBRef sg = DB::create(path);
    auto wt = sg->start_write();
    TableRef t0 = wt->add_table_with_primary_key("class_t0", type_String, "pk");
    auto pk_col = t0->get_primary_key_column();

    auto t1 = steady_clock::now();
    CALLGRIND_START_INSTRUMENTATION;

    for (int i = 0; i < nb_rows; ++i) {
        std::string pk = "KEY_" + util::to_string(i);
        t0->create_object_with_primary_key(pk);
    }

    auto t2 = steady_clock::now();

    for (int i = 0; i < nb_rows; ++i) {
        std::string pk = "KEY_" + util::to_string(i);
        ObjKey k = t0->find_first(pk_col, StringData(pk));
#ifdef REALM_DEBUG
        CHECK(t0->is_valid(k));
#else
        CHECK(k);
#endif
    }

    CALLGRIND_STOP_INSTRUMENTATION;
    auto t3 = steady_clock::now();
    std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/key"
              << std::endl;
    std::cout << "   lookup time: " << duration_cast<nanoseconds>(t3 - t2).count() / nb_rows << " ns/key"
              << std::endl;
    wt->commit();
}

TEST(Table_3)
{
    Table table;

    auto col_int0 = table.add_column(type_Int, "first");
    auto col_int1 = table.add_column(type_Int, "second");
    auto col_bool2 = table.add_column(type_Bool, "third");
    auto col_int3 = table.add_column(type_Int, "fourth");

    for (int64_t i = 0; i < 100; ++i) {
        table.create_object(ObjKey(i)).set_all(i, 10, true, int(Wed));
    }

    // Test column searching
    CHECK_EQUAL(ObjKey(0), table.find_first_int(col_int0, 0));
    CHECK_EQUAL(ObjKey(50), table.find_first_int(col_int0, 50));
    CHECK_EQUAL(null_key, table.find_first_int(col_int0, 500));
    CHECK_EQUAL(ObjKey(0), table.find_first_int(col_int1, 10));
    CHECK_EQUAL(null_key, table.find_first_int(col_int1, 100));
    CHECK_EQUAL(ObjKey(0), table.find_first_bool(col_bool2, true));
    CHECK_EQUAL(null_key, table.find_first_bool(col_bool2, false));
    CHECK_EQUAL(ObjKey(0), table.find_first_int(col_int3, Wed));
    CHECK_EQUAL(null_key, table.find_first_int(col_int3, Mon));

#ifdef REALM_DEBUG
    table.verify();
#endif
}


TEST(Table_4)
{
    Table table;
    auto c0 = table.add_column(type_String, "strings");
    const char* hello_hello = "HelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHelloHello";

    table.create_object(ObjKey(5)).set(c0, "Hello");
    table.create_object(ObjKey(7)).set(c0, hello_hello);

    CHECK_EQUAL(hello_hello, table.get_object(ObjKey(7)).get<String>(c0));

    // Test string column searching
    CHECK_EQUAL(ObjKey(7), table.find_first_string(c0, hello_hello));
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

    ObjKeys keys;
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

    static std::vector<ObjKey> find_all_reference(TableRef table, T v)
    {
        std::vector<ObjKey> res;
        Table::Iterator it = table->begin();
        while (it != table->end()) {
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

    static void validate(TableRef table)
    {
        Table::Iterator it = table->begin();

        if (it != table->end()) {
            auto v = it->get<T>(col);

            if (!it->is_null(col)) {
                std::vector<ObjKey> res;
                table->get_search_index(col)->find_all(res, v, false);
                std::vector<ObjKey> ref = find_all_reference(table, v);

                size_t a = ref.size();
                size_t b = res.size();

                REALM_ASSERT(a == b);
            }
        }
    }

    static void run(DBRef db, realm::DataType type)
    {
        auto trans = db->start_write();
        auto table = trans->add_table("my_table");
        col = table->add_column(type, "name", nullable);
        table->add_search_index(col);
        const size_t iters = 1000;

        bool add_trend = true;

        for (size_t iter = 0; iter < iters; iter++) {

            if (iter == iters / 2) {
                add_trend = false;
            }

            // Add object (with 60% probability, so we grow the object count over time)
            if (fastrand(100) < (add_trend ? 80 : 20)) {
                Obj o = table->create_object();
                bool set_to_null = fastrand(100) < 20;

                if (!set_to_null) {
                    auto t = create();
                    o.set<T2>(col, t);
                }
            }

            // Remove random object
            if (fastrand(100) < 50 && table->size() > 0) {
                Table::Iterator it = table->begin();
                auto r = fastrand(table->size() - 1);
                // FIXME: Is there a faster way to pick a random object?
                for (unsigned t = 0; t < r; t++) {
                    ++it;
                }
                Obj o = *it;
                table->remove_object(o.get_key());
            }

            // Edit random object
            if (table->size() > 0) {
                Table::Iterator it = table->begin();
                auto r = fastrand(table->size() - 1);
                // FIXME: Is there a faster way to pick a random object?
                for (unsigned t = 0; t < r; t++) {
                    ++it;
                }
                Obj o = *it;
                bool set_to_null = fastrand(100) < 20;
                if (set_to_null && table->is_nullable(col)) {
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
        trans->rollback();
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

    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    auto db = DB::create(*hist);
    Tester<bool, false>::run(db, type_Bool);
    Tester<Optional<bool>, true>::run(db, type_Bool);

    Tester<int64_t, false>::run(db, type_Int);
    Tester<Optional<int64_t>, true>::run(db, type_Int);

    // Self-contained null state
    Tester<Timestamp, false>::run(db, type_Timestamp);
    Tester<Timestamp, true>::run(db, type_Timestamp);

    // Self-contained null state
    Tester<StringData, true>::run(db, type_String);
    Tester<StringData, false>::run(db, type_String);
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

TEST(Table_KeysRow)
{
    Table table;
    auto col_int = table.add_column(type_Int, "int");
    auto col_string = table.add_column(type_String, "string", true);
    table.add_search_index(col_int);
    table.add_search_index(col_string);

    table.create_object(ObjKey(7), {{col_int, 123}, {col_string, "Hello, "}});
    table.create_object(ObjKey(9), {{col_int, 456}, {col_string, StringData()}});

    auto i = table.find_first_int(col_int, 123);
    CHECK_EQUAL(i, ObjKey(7));
    i = table.find_first_int(col_int, 456);
    CHECK_EQUAL(i, ObjKey(9));

    i = table.find_first_string(col_string, "Hello, ");
    CHECK_EQUAL(i, ObjKey(7));
    i = table.find_first_string(col_string, StringData());
    CHECK_EQUAL(i, ObjKey(9));
}

template <typename T>
T generate_value()
{
    return test_util::random_int<T>();
}

template <>
std::string generate_value()
{
    std::string str;
    str.resize(31);
    std::generate<std::string::iterator, char (*)()>(str.begin(), str.end(), &test_util::random_int<char>);
    return str;
}

template<> bool generate_value() { return test_util::random_int<int>() & 0x1; }
template<> float generate_value() { return float(1.0 * test_util::random_int<int>() / (test_util::random_int<int>(1, 1000))); }
template<> double generate_value() { return 1.0 * test_util::random_int<int>() / (test_util::random_int<int>(1, 1000)); }
template<> Timestamp generate_value() { return Timestamp(test_util::random_int<int>(0, 1000000), test_util::random_int<int>(0, 1000000000)); }
template <>
Decimal128 generate_value()
{
    return Decimal128(test_util::random_int<int>(-100000, 100000));
}
template <>
ObjectId generate_value()
{
    return ObjectId::gen();
}

// helper object taking care of destroying memory underlying StringData and BinaryData
// just a passthrough for other types
template <typename T>
struct managed {
    T value;
};

template <typename T>
struct ManagedStorage {
    std::string storage;
    T value;

    ManagedStorage() {}
    ManagedStorage(null) {}
    ManagedStorage(std::string&& v)
        : storage(std::move(v))
        , value(storage)
    {
    }
    ManagedStorage(const ManagedStorage& other)
    {
        *this = other;
    }
    ManagedStorage(ManagedStorage&& other)
    {
        *this = std::move(other);
    }

    ManagedStorage(T v)
    {
        if (v) {
            if (v.size()) {
                storage.assign(v.data(), v.data() + v.size());
            }
            value = T(storage);
        }
    }
    ManagedStorage& operator=(const ManagedStorage& other)
    {
        storage = other.storage;
        value = other.value ? T(storage) : T();
        return *this;
    }
    ManagedStorage& operator=(ManagedStorage&& other)
    {
        storage = std::move(other.storage);
        value = other.value ? T(storage) : T();
        return *this;
    }
};

template <>
struct managed<StringData> : ManagedStorage<StringData> {
    using ManagedStorage::ManagedStorage;
};
template <>
struct managed<BinaryData> : ManagedStorage<BinaryData> {
    using ManagedStorage::ManagedStorage;
};


template <typename T>
void check_values(TestContext& test_context, Lst<T>& lst, std::vector<managed<T>>& reference)
{
    CHECK_EQUAL(lst.size(), reference.size());
    for (unsigned j = 0; j < reference.size(); ++j)
        CHECK_EQUAL(lst.get(j), reference[j].value);
}

template <typename T>
struct generator {
    static managed<T> get(bool optional)
    {
        if (optional && (test_util::random_int<int>() % 10) == 0) {
            return managed<T>{T()};
        }
        else {
            return managed<T>{generate_value<T>()};
        }
    }
};

template <>
struct generator<StringData> {
    static managed<StringData> get(bool optional)
    {
        if (optional && (test_util::random_int<int>() % 10) == 0) {
            return managed<StringData>(null());
        }
        else {
            return generate_value<std::string>();
        }
    }
};

template <>
struct generator<BinaryData> {
    static managed<BinaryData> get(bool optional)
    {
        if (optional && (test_util::random_int<int>() % 10) == 0) {
            return managed<BinaryData>(null());
        }
        else {
            return generate_value<std::string>();
        }
    }
};

template <>
struct generator<ObjectId> {
    static managed<ObjectId> get(bool)
    {
        return managed<ObjectId>{generate_value<ObjectId>()};
    }
};

template <typename T>
struct generator<Optional<T>> {
    static managed<Optional<T>> get(bool)
    {
        if ((test_util::random_int<int>() % 10) == 0)
            return managed<Optional<T>>{Optional<T>()};
        else
            return managed<Optional<T>>{generate_value<T>()};
    }
};

// specialize for Optional<StringData> and Optional<BinaryData> just to trigger errors if ever used
template <>
struct generator<Optional<StringData>> {
};
template <>
struct generator<Optional<BinaryData>> {
};

template <typename T>
void test_lists(TestContext& test_context, DBRef sg, const realm::DataType type_id, bool optional = false)
{
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col = table->add_column_list(type_id, "the column", optional);
    Obj o = table->create_object();
    Lst<T> lst = o.get_list<T>(col);
    std::vector<managed<T>> reference;
    for (int j = 0; j < 1000; ++j) {
        managed<T> value = generator<T>::get(optional);
        lst.add(value.value);
        reference.push_back(value);
    }
    check_values(test_context, lst, reference);
    for (int j = 0; j < 100; ++j) {
        managed<T> value = generator<T>::get(optional);
        lst.insert(493, value.value);
        value = generator<T>::get(optional);
        lst.set(493, value.value);
        reference.insert(reference.begin() + 493, value);
    }
    check_values(test_context, lst, reference);
    for (int j = 0; j < 100; ++j) {
        lst.remove(142);
        reference.erase(reference.begin() + 142);
    }
    check_values(test_context, lst, reference);
    for (int disp = 0; disp < 4; ++disp) {
        for (int j = 250 + disp; j > 50; j -= 3) {
            lst.remove(j);
            reference.erase(reference.begin() + j);
        }
        check_values(test_context, lst, reference);
    }
    auto it = reference.begin();
    for (auto value : lst) {
        CHECK(value == it->value);
        ++it;
    }
    for (size_t j = lst.size(); j >= 100; --j) {
        lst.remove(j - 1);
        reference.pop_back();
    }
    check_values(test_context, lst, reference);
    while (size_t sz = lst.size()) {
        lst.remove(sz - 1);
        reference.pop_back();
    }
    CHECK_EQUAL(0, reference.size());
    t->rollback();
}

TEST(List_Ops)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    test_lists<int64_t>(test_context, sg, type_Int);
    test_lists<StringData>(test_context, sg, type_String);
    test_lists<BinaryData>(test_context, sg, type_Binary);
    test_lists<bool>(test_context, sg, type_Bool);
    test_lists<float>(test_context, sg, type_Float);
    test_lists<double>(test_context, sg, type_Double);
    test_lists<Timestamp>(test_context, sg, type_Timestamp);
    test_lists<Decimal128>(test_context, sg, type_Decimal);
    test_lists<ObjectId>(test_context, sg, type_ObjectId);

    test_lists<Optional<int64_t>>(test_context, sg, type_Int, true);
    test_lists<StringData>(test_context, sg, type_String, true); // always Optional?
    test_lists<BinaryData>(test_context, sg, type_Binary, true); // always Optional?
    test_lists<Optional<bool>>(test_context, sg, type_Bool, true);
    test_lists<Optional<float>>(test_context, sg, type_Float, true);
    test_lists<Optional<double>>(test_context, sg, type_Double, true);
    test_lists<Timestamp>(test_context, sg, type_Timestamp, true); // always Optional?
    test_lists<Decimal128>(test_context, sg, type_Decimal, true);
    test_lists<ObjectId>(test_context, sg, type_ObjectId, true);
}

template <typename T, typename U = T>
void test_lists_numeric_agg(TestContext& test_context, DBRef sg, const realm::DataType type_id, U null_value = U{},
                            bool optional = false)
{
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col = table->add_column_list(type_id, "the column", optional);
    Obj o = table->create_object();
    Lst<T> lst = o.get_list<T>(col);
    for (int j = -1000; j < 1000; ++j) {
        T value = T(j);
        lst.add(value);
    }
    if (optional) {
        // given that sum/avg do not count nulls and min/max ignore nulls,
        // adding any number of null values should not affect the results of any aggregates
        for (size_t i = 0; i < 1000; ++i) {
            lst.add(null_value);
        }
    }
    for (int j = -1000; j < 1000; ++j) {
        CHECK_EQUAL(lst.get(j + 1000), T(j));
    }
    {
        size_t ret_ndx = realm::npos;
        Mixed min = lst.min(&ret_ndx);
        CHECK(!min.is_null());
        CHECK_EQUAL(ret_ndx, 0);
        CHECK_EQUAL(min.get<ColumnMinMaxType<T>>(), ColumnMinMaxType<T>(-1000));
        Mixed max = lst.max(&ret_ndx);
        CHECK(!max.is_null());
        CHECK_EQUAL(ret_ndx, 1999);
        CHECK_EQUAL(max.get<ColumnMinMaxType<T>>(), ColumnMinMaxType<T>(999));
        size_t ret_count = 0;
        Mixed sum = lst.sum(&ret_count);
        CHECK(!sum.is_null());
        CHECK_EQUAL(ret_count, 2000);
        CHECK_EQUAL(sum.get<ColumnSumType<T>>(), ColumnSumType<T>(-1000));
        Mixed avg = lst.avg(&ret_count);
        CHECK(!avg.is_null());
        CHECK_EQUAL(ret_count, 2000);
        CHECK_EQUAL(avg.get<ColumnAverageType<T>>(), (ColumnAverageType<T>(-1000) / ColumnAverageType<T>(2000)));
    }

    lst.clear();
    CHECK_EQUAL(lst.size(), 0);
    {
        size_t ret_ndx = realm::npos;
        Mixed min = lst.min(&ret_ndx);
        CHECK_EQUAL(ret_ndx, realm::npos);
        ret_ndx = realm::npos;
        Mixed max = lst.max(&ret_ndx);
        CHECK_EQUAL(ret_ndx, realm::npos);
        size_t ret_count = realm::npos;
        Mixed sum = lst.sum(&ret_count);
        CHECK_EQUAL(ret_count, 0);
        ret_count = realm::npos;
        Mixed avg = lst.avg(&ret_count);
        CHECK_EQUAL(ret_count, 0);
    }

    lst.add(T(1));
    {
        size_t ret_ndx = realm::npos;
        Mixed min = lst.min(&ret_ndx);
        CHECK(!min.is_null());
        CHECK_EQUAL(ret_ndx, 0);
        CHECK_EQUAL(min.get<ColumnMinMaxType<T>>(), ColumnMinMaxType<T>(1));
        Mixed max = lst.max(&ret_ndx);
        CHECK(!max.is_null());
        CHECK_EQUAL(ret_ndx, 0);
        CHECK_EQUAL(max.get<ColumnMinMaxType<T>>(), ColumnMinMaxType<T>(1));
        size_t ret_count = 0;
        Mixed sum = lst.sum(&ret_count);
        CHECK(!sum.is_null());
        CHECK_EQUAL(ret_count, 1);
        CHECK_EQUAL(sum.get<ColumnSumType<T>>(), ColumnSumType<T>(1));
        Mixed avg = lst.avg(&ret_count);
        CHECK(!avg.is_null());
        CHECK_EQUAL(ret_count, 1);
        CHECK_EQUAL(avg.get<ColumnAverageType<T>>(), ColumnAverageType<T>(1));
    }

    t->rollback();
}

TEST(List_AggOps)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    test_lists_numeric_agg<int64_t>(test_context, sg, type_Int);
    test_lists_numeric_agg<float>(test_context, sg, type_Float);
    test_lists_numeric_agg<double>(test_context, sg, type_Double);
    test_lists_numeric_agg<Decimal128>(test_context, sg, type_Decimal);

    test_lists_numeric_agg<Optional<int64_t>>(test_context, sg, type_Int, Optional<int64_t>{}, true);
    test_lists_numeric_agg<float>(test_context, sg, type_Float, realm::null::get_null_float<float>(), true);
    test_lists_numeric_agg<double>(test_context, sg, type_Double, realm::null::get_null_float<double>(), true);
    test_lists_numeric_agg<Decimal128>(test_context, sg, type_Decimal, Decimal128(realm::null()), true);
}

TEST(List_DecimalMinMax)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col = table->add_column_list(type_Decimal, "the column");
    Obj o = table->create_object();
    Lst<Decimal128> lst = o.get_list<Decimal128>(col);
    std::string larger_than_max_int64_t = "123.45e99";
    lst.add(Decimal128(larger_than_max_int64_t));
    CHECK_EQUAL(lst.size(), 1);
    CHECK_EQUAL(lst.get(0), Decimal128(larger_than_max_int64_t));
    size_t min_ndx = realm::npos;
    Mixed min = lst.min(&min_ndx);
    CHECK_EQUAL(min_ndx, 0);
    CHECK_EQUAL(min.get<Decimal128>(), Decimal128(larger_than_max_int64_t));
    lst.clear();
    CHECK_EQUAL(lst.size(), 0);
    std::string smaller_than_min_int64_t = "-123.45e99";
    lst.add(Decimal128(smaller_than_min_int64_t));
    CHECK_EQUAL(lst.size(), 1);
    CHECK_EQUAL(lst.get(0), Decimal128(smaller_than_min_int64_t));
    size_t max_ndx = realm::npos;
    Mixed max = lst.max(&max_ndx);
    CHECK_EQUAL(max_ndx, 0);
    CHECK_EQUAL(max.get<Decimal128>(), Decimal128(smaller_than_min_int64_t));
}

template <typename T>
void check_table_values(TestContext& test_context, TableRef t, ColKey col, std::map<int, managed<T>>& reference)
{
    if (t->size() != reference.size()) {
        std::cout << "gah" << std::endl;
    }
    CHECK_EQUAL(t->size(), reference.size());
    for (auto it : reference) {
        T value = it.second.value;
        Obj o = t->get_object(ObjKey(it.first));
        CHECK_EQUAL(o.get<T>(col), value);
    }
}

template <typename T>
void test_tables(TestContext& test_context, DBRef sg, const realm::DataType type_id, bool optional = false)
{
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col = table->add_column(type_id, "the column", optional);
    std::map<int, managed<T>> reference;

    // insert elements 0 - 999
    for (int j = 0; j < 1000; ++j) {
        managed<T> value = generator<T>::get(optional);
        Obj o = table->create_object(ObjKey(j)).set_all(value.value);
        reference[j] = std::move(value);
    }
    // insert elements 10000 - 10999
    for (int j = 10000; j < 11000; ++j) {
        managed<T> value = generator<T>::get(optional);
        Obj o = table->create_object(ObjKey(j)).set_all(value.value);
        reference[j] = std::move(value);
    }
    // insert in between previous groups
    for (int j = 4000; j < 7000; ++j) {
        managed<T> value = generator<T>::get(optional);
        Obj o = table->create_object(ObjKey(j)).set_all(value.value);
        reference[j] = std::move(value);
    }
    check_table_values(test_context, table, col, reference);

    // modify values
    for (int j = 0; j < 11000; j += 100) {
        auto it = reference.find(j);
        if (it == reference.end()) // skip over holes in the key range
            continue;
        managed<T> value = generator<T>::get(optional);
        Obj o = table->get_object(ObjKey(j));
        o.set<T>(col, value.value);
        it->second = value;
    }
    check_table_values(test_context, table, col, reference);

    // remove chunk in the middle
    for (int j = 1000; j < 10000; ++j) {
        auto it = reference.find(j);
        if (it == reference.end()) // skip over holes in the key range
            continue;
        table->remove_object(ObjKey(j));
        reference.erase(it);
    }
    check_table_values(test_context, table, col, reference);
    t->rollback();
}

TEST(Table_Ops)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    test_tables<int64_t>(test_context, sg, type_Int);
    test_tables<StringData>(test_context, sg, type_String);
    test_tables<BinaryData>(test_context, sg, type_Binary);
    test_tables<bool>(test_context, sg, type_Bool);
    test_tables<float>(test_context, sg, type_Float);
    test_tables<double>(test_context, sg, type_Double);
    test_tables<Timestamp>(test_context, sg, type_Timestamp);
    test_tables<Decimal128>(test_context, sg, type_Decimal);
    test_tables<ObjectId>(test_context, sg, type_ObjectId);

    test_tables<Optional<int64_t>>(test_context, sg, type_Int, true);
    test_tables<StringData>(test_context, sg, type_String, true); // always Optional?
    test_tables<BinaryData>(test_context, sg, type_Binary, true); // always Optional?
    test_tables<Optional<bool>>(test_context, sg, type_Bool, true);
    test_tables<Optional<float>>(test_context, sg, type_Float, true);
    test_tables<Optional<double>>(test_context, sg, type_Double, true);
    test_tables<Timestamp>(test_context, sg, type_Timestamp, true); // always Optional?
    test_tables<Decimal128>(test_context, sg, type_Decimal, true);
    test_tables<Optional<ObjectId>>(test_context, sg, type_ObjectId, true);
}

template <typename TFrom, typename TTo>
void test_dynamic_conversion(TestContext& test_context, DBRef sg, realm::DataType type_id, bool from_nullable,
                             bool to_nullable)
{
    // Create values of type TFrom and ask for dynamic conversion to TTo
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col_from = table->add_column(type_id, "the column", from_nullable);
    if (type_id == type_String) {
        table->add_search_index(col_from);
    }
    std::map<int, managed<TTo>> reference;
    value_copier<TFrom, TTo> copier(false);
    for (int j = 0; j < 10; ++j) {
        managed<TFrom> value = generator<TFrom>::get(from_nullable);
        Obj o =
            table->create_object(ObjKey(j)).set_all(value.value); // <-- so set_all works even if it doesn't set all?
        TTo conv_value = copier(
            value.value, to_nullable); // one may argue that using the same converter for ref and dut is.. mmmh...
        reference[j] = managed<TTo>{conv_value};
    }
    auto col_to = table->set_nullability(col_from, to_nullable, false);
    if (type_id == type_String) {
        CHECK(table->has_search_index(col_to));
    }
    check_table_values(test_context, table, col_to, reference);
    t->rollback();
}

template <typename TFrom, typename TTo>
void test_dynamic_conversion_list(TestContext& test_context, DBRef sg, realm::DataType type_id, bool from_nullable,
                                  bool to_nullable)
{
    // Create values of type TFrom and ask for dynamic conversion to TTo
    auto t = sg->start_write();
    auto table = t->add_table("the_table");
    auto col_from = table->add_column_list(type_id, "the column", from_nullable);
    Obj o = table->create_object();
    table->create_object(); // This object will have an empty list
    Lst<TFrom> from_lst = o.get_list<TFrom>(col_from);
    std::vector<managed<TTo>> reference;
    value_copier<TFrom, TTo> copier(false);
    for (int j = 0; j < 1000; ++j) {
        managed<TFrom> value = generator<TFrom>::get(from_nullable);
        from_lst.add(value.value);
        TTo conv_value = copier(value.value, to_nullable);
        reference.push_back(managed<TTo>{conv_value});
    }
    auto col_to = table->set_nullability(col_from, to_nullable, false);
    Lst<TTo> to_lst = o.get_list<TTo>(col_to);
    check_values(test_context, to_lst, reference);
    t->rollback();
}

template <typename T>
void test_dynamic_conversion_combi(TestContext& test_context, DBRef sg, realm::DataType type_id)
{
    test_dynamic_conversion<T, Optional<T>>(test_context, sg, type_id, false, true);
    test_dynamic_conversion<Optional<T>, T>(test_context, sg, type_id, true, false);
    test_dynamic_conversion<T, T>(test_context, sg, type_id, false, false);
    test_dynamic_conversion<Optional<T>, Optional<T>>(test_context, sg, type_id, true, true);
}

template <typename T>
void test_dynamic_conversion_combi_sametype(TestContext& test_context, DBRef sg, realm::DataType type_id)
{
    test_dynamic_conversion<T, T>(test_context, sg, type_id, false, true);
    test_dynamic_conversion<T, T>(test_context, sg, type_id, true, false);
    test_dynamic_conversion<T, T>(test_context, sg, type_id, false, false);
    test_dynamic_conversion<T, T>(test_context, sg, type_id, true, true);
}

template <typename T>
void test_dynamic_conversion_list_combi(TestContext& test_context, DBRef sg, realm::DataType type_id)
{
    test_dynamic_conversion_list<T, Optional<T>>(test_context, sg, type_id, false, true);
    test_dynamic_conversion_list<Optional<T>, T>(test_context, sg, type_id, true, false);
    test_dynamic_conversion_list<T, T>(test_context, sg, type_id, false, false);
    test_dynamic_conversion_list<Optional<T>, Optional<T>>(test_context, sg, type_id, true, true);
}

template <typename T>
void test_dynamic_conversion_list_combi_sametype(TestContext& test_context, DBRef sg, realm::DataType type_id)
{
    test_dynamic_conversion_list<T, T>(test_context, sg, type_id, false, true);
    test_dynamic_conversion_list<T, T>(test_context, sg, type_id, true, false);
    test_dynamic_conversion_list<T, T>(test_context, sg, type_id, false, false);
    test_dynamic_conversion_list<T, T>(test_context, sg, type_id, true, true);
}

TEST(Table_Column_DynamicConversions)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    test_dynamic_conversion_combi<int64_t>(test_context, sg, type_Int);
    test_dynamic_conversion_combi<float>(test_context, sg, type_Float);
    test_dynamic_conversion_combi<double>(test_context, sg, type_Double);
    test_dynamic_conversion_combi<bool>(test_context, sg, type_Bool);
    test_dynamic_conversion_combi<ObjectId>(test_context, sg, type_ObjectId);

    test_dynamic_conversion_combi_sametype<StringData>(test_context, sg, type_String);
    test_dynamic_conversion_combi_sametype<BinaryData>(test_context, sg, type_Binary);
    test_dynamic_conversion_combi_sametype<Timestamp>(test_context, sg, type_Timestamp);
    test_dynamic_conversion_combi_sametype<Decimal128>(test_context, sg, type_Decimal);
    // lists...:
    test_dynamic_conversion_list_combi<int64_t>(test_context, sg, type_Int);
    test_dynamic_conversion_list_combi<float>(test_context, sg, type_Float);
    test_dynamic_conversion_list_combi<double>(test_context, sg, type_Double);
    test_dynamic_conversion_list_combi<bool>(test_context, sg, type_Bool);
    test_dynamic_conversion_list_combi<ObjectId>(test_context, sg, type_ObjectId);

    test_dynamic_conversion_list_combi_sametype<StringData>(test_context, sg, type_String);
    test_dynamic_conversion_list_combi_sametype<BinaryData>(test_context, sg, type_Binary);
    test_dynamic_conversion_list_combi_sametype<Timestamp>(test_context, sg, type_Timestamp);
    test_dynamic_conversion_list_combi_sametype<Decimal128>(test_context, sg, type_Decimal);
}

/*
TEST(Table_Column_Conversions)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    test_column_conversion<int64_t, Optional<int64_t>>(test_context, sg, type_Int);
    test_column_conversion<float, Optional<float>>(test_context, sg, type_Float);
    test_column_conversion<double, Optional<double>>(test_context, sg, type_Double);
    test_column_conversion<bool, Optional<bool>>(test_context, sg, type_Bool);
    test_column_conversion<StringData, StringData>(test_context, sg, type_String);
    test_column_conversion<BinaryData, BinaryData>(test_context, sg, type_Binary);
    test_column_conversion<Timestamp, Timestamp>(test_context, sg, type_Timestamp);

    test_column_conversion_optional<int64_t>(test_context, sg, type_Int);
    test_column_conversion_optional<float>(test_context, sg, type_Float);
    test_column_conversion_optional<double>(test_context, sg, type_Double);
    test_column_conversion_optional<bool>(test_context, sg, type_Bool);

    test_column_conversion_sametype<StringData>(test_context, sg, type_String);
    test_column_conversion_sametype<BinaryData>(test_context, sg, type_Binary);
    test_column_conversion_sametype<Timestamp>(test_context, sg, type_Timestamp);

}
*/
TEST(Table_MultipleObjs) {
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_table("my_table");
    auto col = table->add_column_link(type_LinkList, "the links", *table);
    auto col_int = table->add_column_list(type_String, "the integers");
    auto obj_key = table->create_object().get_key();
    tr->commit();
    tr = sg->start_write();
    table = tr->get_table("my_table");
    auto obj = table->get_object(obj_key);
    auto list_1 = obj.get_linklist(col);
    auto list_2 = obj.get_linklist(col);

    auto list_3 = obj.get_list<StringData>(col_int);
    auto list_4 = obj.get_list<StringData>(col_int);
    std::string s = "42";
    StringData ss(s.data(), s.size());
    list_3.add(ss);
    CHECK_EQUAL(list_4.get(0), ss);

    list_1.add(obj_key);
    CHECK_EQUAL(list_1.get(0), obj_key);
    CHECK_EQUAL(list_2.get(0), obj_key);
}

TEST(Table_IteratorRandomAccess)
{
    Table t;

    ObjKeys keys;
    t.create_objects(1000, keys);

    auto key = keys.begin();
    auto iter = t.begin();
    auto end = t.end();
    for (size_t pos = 0; (pos + 3) < 1000; pos += 3) {
        CHECK_EQUAL(iter->get_key(), *key);
        iter += 3;
        key += 3;
    }

    // random access
    for (int j = 0; j < 5; j++) {
        std::vector<size_t> random_idx(keys.size());
        std::iota(random_idx.begin(), random_idx.end(), 0);
        // unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
        // std::cout << "Seed " << seed << std::endl;
        std::shuffle(random_idx.begin(), random_idx.end(), std::mt19937(unit_test_random_seed));
        iter = t.begin();
        int i = 0;
        for (auto index : random_idx) {
            if (index < keys.size()) {
                auto k = keys[index];
                if (i == 4) {
                    t.remove_object(k);
                    keys.erase(keys.begin() + index);
                    if (index == 0)
                        iter = t.begin();
                    i = 0;
                }
                else {
                    CHECK_EQUAL(k, iter[index].get_key());
                }
                i++;
            }
        }
    }

    auto iter200 = iter + 200;
    CHECK_EQUAL(keys[200], iter200->get_key());
    ++iter; // Now points to element 1
    CHECK_EQUAL(keys[201], iter[200].get_key());
    CHECK_EQUAL(keys[201], iter200[1].get_key());
    CHECK_EQUAL(keys[1], iter->get_key());
}

TEST(Table_EmbeddedObjects)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_embedded_table("mytable");
    tr->commit_and_continue_as_read();
    tr->promote_to_write();
    CHECK(table->is_embedded());
    CHECK_THROW(table->create_object(), LogicError);
    tr->rollback();

    tr = sg->start_read();
    table = tr->get_table("mytable");
    CHECK(table->is_embedded());
}

TEST(Table_EmbeddedObjectCreateAndDestroy)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    {
        auto tr = sg->start_write();
        auto table = tr->add_embedded_table("myEmbeddedStuff");
        auto col_recurse = table->add_column_link(type_Link, "theRecursiveBit", *table);
        CHECK_THROW(table->create_object(), LogicError);
        auto parent = tr->add_table("myParentStuff");
        auto ck = parent->add_column_link(type_Link, "theGreatColumn", *table);
        Obj o = parent->create_object();
        Obj o2 = o.create_and_set_linked_object(ck);
        o2.create_and_set_linked_object(col_recurse);
        CHECK(table->size() == 2);
        tr->commit();
    }
    {
        auto tr = sg->start_write();
        auto table = tr->get_table("myEmbeddedStuff");
        auto parent = tr->get_table("myParentStuff");
        CHECK(table->size() == 2);
        auto first = parent->begin();
        first->set("theGreatColumn", ObjKey());
        CHECK(table->size() == 0);
        // do not commit
    }
    {
        auto tr = sg->start_write();
        auto table = tr->get_table("myEmbeddedStuff");
        auto parent = tr->get_table("myParentStuff");
        CHECK(table->size() == 2);
        auto first = parent->begin();
        first->remove();
        CHECK(table->size() == 0);
        // do not commit
    }
}

TEST(Table_EmbeddedObjectCreateAndDestroyList)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_embedded_table("myEmbeddedStuff");
    auto col_recurse = table->add_column_link(type_LinkList, "theRecursiveBit", *table);
    CHECK_THROW(table->create_object(), LogicError);
    auto parent = tr->add_table("myParentStuff");
    auto ck = parent->add_column_link(type_LinkList, "theGreatColumn", *table);
    Obj o = parent->create_object();
    auto parent_ll = o.get_linklist(ck);
    Obj o2 = parent_ll.create_and_insert_linked_object(0);
    Obj o3 = parent_ll.create_and_insert_linked_object(1);
    Obj o4 = parent_ll.create_and_insert_linked_object(0);
    auto o2_ll = o2.get_linklist(col_recurse);
    auto o3_ll = o3.get_linklist(col_recurse);
    o2_ll.create_and_insert_linked_object(0);
    o2_ll.create_and_insert_linked_object(0);
    o3_ll.create_and_insert_linked_object(0);
    CHECK(table->size() == 6);
    parent_ll.create_and_set_linked_object(1); // implicitly remove entry for 02
    CHECK(!o2.is_valid());
    CHECK(table->size() == 4);
    parent_ll.clear();
    CHECK(table->size() == 0);
    parent_ll.create_and_insert_linked_object(0);
    parent_ll.create_and_insert_linked_object(1);
    CHECK(table->size() == 2);
    o.remove();
    CHECK(table->size() == 0);
    tr->commit();
}

TEST(Table_EmbeddedObjectNotifications)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_embedded_table("myEmbeddedStuff");
    auto col_recurse = table->add_column_link(type_LinkList, "theRecursiveBit", *table);
    CHECK_THROW(table->create_object(), LogicError);
    auto parent = tr->add_table("myParentStuff");
    auto ck = parent->add_column_link(type_LinkList, "theGreatColumn", *table);
    Obj o = parent->create_object();
    auto parent_ll = o.get_linklist(ck);
    Obj o2 = parent_ll.create_and_insert_linked_object(0);
    Obj o3 = parent_ll.create_and_insert_linked_object(1);
    Obj o4 = parent_ll.create_and_insert_linked_object(0);
    auto o2_ll = o2.get_linklist(col_recurse);
    auto o3_ll = o3.get_linklist(col_recurse);
    o2_ll.create_and_insert_linked_object(0);
    o2_ll.create_and_insert_linked_object(0);
    o3_ll.create_and_insert_linked_object(0);
    CHECK(table->size() == 6);
    Obj o5 = parent_ll.create_and_set_linked_object(1); // implicitly remove entry for 02
    CHECK(!o2.is_valid());
    CHECK(table->size() == 4);
    // now the notifications...
    int calls = 0;
    tr->set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        CHECK_EQUAL(0, notification.links.size());
        if (calls == 0) {
            CHECK_EQUAL(1, notification.rows.size());
            CHECK_EQUAL(parent->get_key(), notification.rows[0].table_key);
            CHECK_EQUAL(o.get_key(), notification.rows[0].key);
        }
        else if (calls == 1) {
            CHECK_EQUAL(3, notification.rows.size());
            for (auto& row : notification.rows)
                CHECK_EQUAL(table->get_key(), row.table_key);
            CHECK_EQUAL(o4.get_key(), notification.rows[0].key);
            CHECK_EQUAL(o5.get_key(), notification.rows[1].key);
            CHECK_EQUAL(o3.get_key(), notification.rows[2].key);
        }
        else if (calls == 2) {
            CHECK_EQUAL(1, notification.rows.size()); // from o3
            for (auto& row : notification.rows)
                CHECK_EQUAL(table->get_key(), row.table_key);
            // don't bother checking the keys...
        }
        ++calls;
    });

    o.remove();
    CHECK(calls == 3);
    tr->commit();
}
TEST(Table_EmbeddedObjectTableClearNotifications)
{
    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_embedded_table("myEmbeddedStuff");
    auto col_recurse = table->add_column_link(type_LinkList, "theRecursiveBit", *table);
    CHECK_THROW(table->create_object(), LogicError);
    auto parent = tr->add_table("myParentStuff");
    auto ck = parent->add_column_link(type_LinkList, "theGreatColumn", *table);
    Obj o = parent->create_object();
    auto parent_ll = o.get_linklist(ck);
    Obj o2 = parent_ll.create_and_insert_linked_object(0);
    Obj o3 = parent_ll.create_and_insert_linked_object(1);
    Obj o4 = parent_ll.create_and_insert_linked_object(0);
    auto o2_ll = o2.get_linklist(col_recurse);
    auto o3_ll = o3.get_linklist(col_recurse);
    o2_ll.create_and_insert_linked_object(0);
    o2_ll.create_and_insert_linked_object(0);
    o3_ll.create_and_insert_linked_object(0);
    CHECK(table->size() == 6);
    Obj o5 = parent_ll.create_and_set_linked_object(1); // implicitly remove entry for 02
    CHECK(!o2.is_valid());
    CHECK(table->size() == 4);
    // now the notifications...
    int calls = 0;
    tr->set_cascade_notification_handler([&](const Group::CascadeNotification& notification) {
        if (calls == 0) {
            CHECK_EQUAL(3, notification.rows.size());
            for (auto& row : notification.rows)
                CHECK_EQUAL(table->get_key(), row.table_key);
            CHECK_EQUAL(o4.get_key(), notification.rows[0].key);
            CHECK_EQUAL(o5.get_key(), notification.rows[1].key);
            CHECK_EQUAL(o3.get_key(), notification.rows[2].key);
        }
        else if (calls == 1) {
            CHECK_EQUAL(1, notification.rows.size()); // from o3
            for (auto& row : notification.rows)
                CHECK_EQUAL(table->get_key(), row.table_key);
            // don't bother checking the keys...
        }
        ++calls;
    });

    parent->clear();
    CHECK(calls == 2);
    CHECK_EQUAL(parent->size(), 0);
    tr->commit();
}

TEST(Table_EmbeddedObjectPath)
{
    auto collect_path = [](const Obj& o) {
        return o.get_fat_path();
    };

    SHARED_GROUP_TEST_PATH(path);

    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef sg = DB::create(*hist, DBOptions(crypt_key()));

    auto tr = sg->start_write();
    auto table = tr->add_embedded_table("myEmbeddedStuff");
    auto col_recurse = table->add_column_link(type_LinkList, "theRecursiveBit", *table);
    CHECK_THROW(table->create_object(), LogicError);
    auto parent = tr->add_table("myParentStuff");
    auto ck = parent->add_column_link(type_LinkList, "theGreatColumn", *table);
    Obj o = parent->create_object();
    auto gch = collect_path(o);
    CHECK(gch.size() == 0);
    auto parent_ll = o.get_linklist(ck);
    Obj o2 = parent_ll.create_and_insert_linked_object(0);
    auto gbh = collect_path(o2);
    CHECK(gbh.size() == 1);
    CHECK(gbh[0].obj.get_key() == o.get_key());
    CHECK(gbh[0].col_key == ck);
    CHECK(gbh[0].index == 0);
    Obj o3 = parent_ll.create_and_insert_linked_object(1);
    Obj o4 = parent_ll.create_and_insert_linked_object(0);
    auto gah = collect_path(o4);
    CHECK(gah.size() == 1);
    CHECK(gah[0].obj.get_key() == o.get_key());
    CHECK(gah[0].col_key == ck);
    CHECK(gah[0].index == 0);
    auto gzh = collect_path(o3);
    CHECK(gzh.size() == 1);
    CHECK(gzh[0].obj.get_key() == o.get_key());
    CHECK(gzh[0].col_key == ck);
    CHECK(gzh[0].index == 2);
    auto o2_ll = o2.get_linklist(col_recurse);
    auto o3_ll = o3.get_linklist(col_recurse);
    o2_ll.create_and_insert_linked_object(0);
    o2_ll.create_and_insert_linked_object(0);
    o3_ll.create_and_insert_linked_object(0);
    CHECK(table->size() == 6);
    auto gyh = collect_path(o3_ll.get_object(0));
    CHECK(gyh.size() == 2);
    CHECK(gyh[0].obj.get_key() == o.get_key());
    CHECK(gyh[0].col_key == ck);
    CHECK(gyh[0].index == 2);
    CHECK(gyh[1].obj.get_key() == o3.get_key());
    CHECK(gyh[1].col_key = col_recurse);
    CHECK(gyh[1].index == 0);
}


#endif // TEST_TABLE
