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
#ifdef TEST_TABLE_VIEW

#include <limits>
#include <string>
#include <sstream>
#include <ostream>
#include <cwchar>

#include <realm/table_view.hpp>
#include <realm/query_expression.hpp>

#include "util/misc.hpp"

#include "test.hpp"
#include "test_table_helper.hpp"

using namespace realm;
using namespace test_util;

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

TEST(TableView_Json)
{
    Table table;
    table.add_column(type_Int, "first");

    size_t ndx = table.add_empty_row();
    table.set_int(0, ndx, 1);
    ndx = table.add_empty_row();
    table.set_int(0, ndx, 2);
    ndx = table.add_empty_row();
    table.set_int(0, ndx, 3);

    TableView v = table.where().find_all(1);
    std::stringstream ss;
    v.to_json(ss);
    const std::string json = ss.str();
    CHECK_EQUAL(true, json.length() > 0);
    CHECK_EQUAL("[{\"first\":2},{\"first\":3}]", json);
}


TEST(TableView_TimestampMaxMinCount)
{
    Table t;
    t.add_column(type_Timestamp, "ts", true);
    t.add_empty_row();
    t.set_timestamp(0, 0, Timestamp(300, 300));

    t.add_empty_row();
    t.set_timestamp(0, 1, Timestamp(100, 100));

    t.add_empty_row();
    t.set_timestamp(0, 2, Timestamp(200, 200));

    // Add row with null. For max(), any non-null is greater, and for min() any non-null is less
    t.add_empty_row();

    TableView tv = t.where().find_all();
    Timestamp ts;

    ts = tv.maximum_timestamp(0, nullptr);
    CHECK_EQUAL(ts, Timestamp(300, 300));
    ts = tv.minimum_timestamp(0, nullptr);
    CHECK_EQUAL(ts, Timestamp(100, 100));

    size_t index;
    ts = tv.maximum_timestamp(0, &index);
    CHECK_EQUAL(index, 0);
    ts = tv.minimum_timestamp(0, &index);
    CHECK_EQUAL(index, 1);

    size_t cnt;
    cnt = tv.count_timestamp(0, Timestamp(100, 100));
    CHECK_EQUAL(cnt, 1);

    cnt = tv.count_timestamp(0, Timestamp{});
    CHECK_EQUAL(cnt, 1);
}

TEST(TableView_TimestampGetSet)
{
    Table t;
    t.add_column(type_Timestamp, "ts", true);
    t.add_empty_row(3);
    t.set_timestamp(0, 0, Timestamp(000, 010));
    t.set_timestamp(0, 1, Timestamp(100, 110));
    t.set_timestamp(0, 2, Timestamp(200, 210));

    TableView tv = t.where().find_all();
    CHECK_EQUAL(tv.get_timestamp(0, 0), Timestamp(000, 010));
    CHECK_EQUAL(tv.get_timestamp(0, 1), Timestamp(100, 110));
    CHECK_EQUAL(tv.get_timestamp(0, 2), Timestamp(200, 210));

    tv.set_timestamp(0, 0, Timestamp(1000, 1010));
    tv.set_timestamp(0, 1, Timestamp(1100, 1110));
    tv.set_timestamp(0, 2, Timestamp(1200, 1210));
    CHECK_EQUAL(tv.get_timestamp(0, 0), Timestamp(1000, 1010));
    CHECK_EQUAL(tv.get_timestamp(0, 1), Timestamp(1100, 1110));
    CHECK_EQUAL(tv.get_timestamp(0, 2), Timestamp(1200, 1210));
}

TEST(TableView_GetSetInteger)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 1);
    add(table, 2);
    add(table, 3);
    add(table, 1);
    add(table, 2);

    TableView v;                 // Test empty construction
    v = table.find_all_int(0, 2); // Test assignment

    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2, v[0].get_int(0));
    CHECK_EQUAL(2, v[1].get_int(0));

    // Test of Set
    v[0].set_int(0, 123);
    CHECK_EQUAL(123, v[0].get_int(0));
}


TEST(TableView_FloatsGetSet)
{
    TestTable table;
    table.add_column(type_Float, "1");
    table.add_column(type_Double, "2");
    table.add_column(type_Int, "3");

    float f_val[] = {1.1f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f};
    double d_val[] = {1.2, 2.2, 3.2, -1.2, 2.3, 0.0};

    CHECK_EQUAL(true, table.is_empty());

    // Test add(?,?) with parameters
    for (size_t i = 0; i < 5; ++i)
        add(table, f_val[i], d_val[i], int64_t(i));

    table.add_empty_row();

    CHECK_EQUAL(6, table.size());
    for (size_t i = 0; i < 6; ++i) {
        CHECK_EQUAL(f_val[i], table.get_float(0, i));
        CHECK_EQUAL(d_val[i], table.get_double(1, i));
    }

    TableView v;                         // Test empty construction
    v = table.find_all_float(0, 2.1f); // Test assignment
    CHECK_EQUAL(2, v.size());

    TableView v2(v);


    // Test of Get
    CHECK_EQUAL(2.1f, v[0].get_float(0));
    CHECK_EQUAL(2.1f, v[1].get_float(0));
    CHECK_EQUAL(2.2, v[0].get_double(1));
    CHECK_EQUAL(2.3, v[1].get_double(1));

    // Test of Set
    v[0].set_float(0, 123.321f);
    CHECK_EQUAL(123.321f, v[0].get_float(0));
    v[0].set_double(1, 123.3219);
    CHECK_EQUAL(123.3219, v[0].get_double(1));
}

TEST(TableView_FloatsFindAndAggregations)
{
    TestTable table;
    table.add_column(type_Float, "1");
    table.add_column(type_Double, "2");
    table.add_column(type_Int, "3");

    float f_val[] = {1.2f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f};
    double d_val[] = {-1.2, 2.2, 3.2, -1.2, 2.3, 0.0};
    // v_some =       ^^^^            ^^^^
    double sum_f = 0.0;
    double sum_d = 0.0;
    for (size_t i = 0; i < 6; ++i) {
        add(table, f_val[i], d_val[i], 1);
        sum_d += d_val[i];
        sum_f += f_val[i];
    }

    // Test find_all()
    TableView v_all = table.find_all_int(2, 1);
    CHECK_EQUAL(6, v_all.size());

    TableView v_some = table.find_all_double(1, -1.2);
    CHECK_EQUAL(2, v_some.size());
    CHECK_EQUAL(0, v_some.get_source_ndx(0));
    CHECK_EQUAL(3, v_some.get_source_ndx(1));

    // Test find_first
    CHECK_EQUAL(0, v_all.find_first_double(1, -1.2));
    CHECK_EQUAL(5, v_all.find_first_double(1, 0.0));
    CHECK_EQUAL(2, v_all.find_first_double(1, 3.2));

    CHECK_EQUAL(1, v_all.find_first_float(0, 2.1f));
    CHECK_EQUAL(5, v_all.find_first_float(0, 0.0f));
    CHECK_EQUAL(2, v_all.find_first_float(0, 3.1f));

    // TODO: add for float as well

    double epsilon = std::numeric_limits<double>::epsilon();

    // Test sum
    CHECK_APPROXIMATELY_EQUAL(sum_d, v_all.sum_double(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f, v_all.sum_float(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(-1.2 + -1.2, v_some.sum_double(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(double(1.2f) + double(-1.1f), v_some.sum_float(0), 10 * epsilon);

    size_t ndx = not_found;

    // Test max
    CHECK_EQUAL(3.2, v_all.maximum_double(1, &ndx));
    CHECK_EQUAL(2, ndx);

    CHECK_EQUAL(-1.2, v_some.maximum_double(1, &ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.1f, v_all.maximum_float(0, &ndx));
    CHECK_EQUAL(2, ndx);

    CHECK_EQUAL(1.2f, v_some.maximum_float(0, &ndx));
    CHECK_EQUAL(0, ndx);

    // Max without ret_index
    CHECK_EQUAL(3.2, v_all.maximum_double(1));
    CHECK_EQUAL(-1.2, v_some.maximum_double(1));
    CHECK_EQUAL(3.1f, v_all.maximum_float(0));
    CHECK_EQUAL(1.2f, v_some.maximum_float(0));

    // Test min
    CHECK_EQUAL(-1.2, v_all.minimum_double(1));
    CHECK_EQUAL(-1.2, v_some.minimum_double(1));
    CHECK_EQUAL(-1.1f, v_all.minimum_float(0));
    CHECK_EQUAL(-1.1f, v_some.minimum_float(0));

    // min with ret_ndx
    CHECK_EQUAL(-1.2, v_all.minimum_double(1, &ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(-1.2, v_some.minimum_double(1, &ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(-1.1f, v_all.minimum_float(0, &ndx));
    CHECK_EQUAL(3, ndx);

    CHECK_EQUAL(-1.1f, v_some.minimum_float(0, &ndx));
    CHECK_EQUAL(1, ndx);

    // Test avg
    CHECK_APPROXIMATELY_EQUAL(sum_d / 6.0, v_all.average_double(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL((-1.2 + -1.2) / 2.0, v_some.average_double(1), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f / 6.0, v_all.average_float(0), 10 * epsilon);
    CHECK_APPROXIMATELY_EQUAL((double(1.2f) + double(-1.1f)) / 2, v_some.average_float(0), 10 * epsilon);

    CHECK_EQUAL(1, v_some.count_float(0, 1.2f));
    CHECK_EQUAL(2, v_some.count_double(1, -1.2));
    CHECK_EQUAL(2, v_some.count_int(2, 1));

    CHECK_EQUAL(2, v_all.count_float(0, 2.1f));
    CHECK_EQUAL(2, v_all.count_double(1, -1.2));
    CHECK_EQUAL(6, v_all.count_int(2, 1));
}

TEST(TableView_Sum)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 2);
    add(table, 2);
    add(table, 2);
    add(table, 2);
    add(table, 2);

    TableView v = table.find_all_int(0, 2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.sum_int(0);
    CHECK_EQUAL(10, sum);
}

TEST(TableView_Average)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 2);
    add(table, 2);
    add(table, 2);
    add(table, 2);
    add(table, 2);

    TableView v = table.find_all_int(0, 2);
    CHECK_EQUAL(5, v.size());

    double sum = v.average_int(0);
    CHECK_APPROXIMATELY_EQUAL(2., sum, 0.00001);
}

TEST(TableView_SumNegative)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, 11);
    v[2].set_int(0, -20);

    int64_t sum = v.sum_int(0);
    CHECK_EQUAL(-9, sum);
}

TEST(TableView_IsAttached)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    TableView v2 = table.find_all_int(0, 0);
    v[0].set_int(0, 11);
    CHECK_EQUAL(true, v.is_attached());
    CHECK_EQUAL(true, v2.is_attached());
    v.remove_last();
    CHECK_EQUAL(true, v.is_attached());
    CHECK_EQUAL(true, v2.is_attached());

    table.remove_last();
    CHECK_EQUAL(true, v.is_attached());
    CHECK_EQUAL(true, v2.is_attached());
}

TEST(TableView_Max)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, -1);
    v[1].set_int(0, 2);
    v[2].set_int(0, 1);

    int64_t max = v.maximum_int(0);
    CHECK_EQUAL(2, max);
}

TEST(TableView_Max2)
{
    TestTable table;
    table.add_column(type_Int, "1");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, -1);
    v[1].set_int(0, -2);
    v[2].set_int(0, -3);

    int64_t max = v.maximum_int(0, 0);
    CHECK_EQUAL(-1, max);
}


TEST(TableView_Min)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, -1);
    v[1].set_int(0, 2);
    v[2].set_int(0, 1);

    int64_t min = v.minimum_int(0);
    CHECK_EQUAL(-1, min);

    size_t ndx = not_found;
    min = v.minimum_int(0, &ndx);
    CHECK_EQUAL(-1, min);
    CHECK_EQUAL(0, ndx);
}

TEST(TableView_Min2)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, -1);
    v[1].set_int(0, -2);
    v[2].set_int(0, -3);

    int64_t min = v.minimum_int(0);
    CHECK_EQUAL(-3, min);

    size_t ndx = not_found;
    min = v.minimum_int(0 ,&ndx);
    CHECK_EQUAL(-3, min);
    CHECK_EQUAL(2, ndx);
}


TEST(TableView_Find)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    v[0].set_int(0, 5);
    v[1].set_int(0, 4);
    v[2].set_int(0, 4);

    size_t r = v.find_first_int(0, 4);
    CHECK_EQUAL(1, r);
}


TEST(TableView_Follows_Changes)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0, 0, 1);
    Query q = table.where().equal(0, 1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));

    // low level sanity check that we can copy a query and run the copy:
    Query q2 = q;
    TableView v2 = q2.find_all();

    // now the fun begins
    CHECK_EQUAL(1, v.size());
    table.add_empty_row();
    CHECK_EQUAL(1, v.size());
    table.set_int(0, 1, 1);
    v.sync_if_needed();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
    CHECK_EQUAL(1, v.get_int(0, 1));
    table.set_int(0, 0, 7);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
    table.set_int(0, 1, 7);
    v.sync_if_needed();
    CHECK_EQUAL(0, v.size());
    table.set_int(0, 1, 1);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));
}


TEST(TableView_Distinct_Follows_Changes)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_column(type_String, "second");
    table.add_search_index(0);

    table.add_empty_row(5);
    for (int i = 0; i < 5; ++i) {
        table.set_int(0, i, i);
        table.set_string(1, i, "Foo");
    }

    TableView distinct_ints = table.get_distinct_view(0);
    CHECK_EQUAL(5, distinct_ints.size());
    CHECK(distinct_ints.is_in_sync());

    // Check that adding a value that doesn't actually impact the
    // view still invalidates the view (which is inspected for now).
    table.add_empty_row();
    table.set_int(0, 5, 4);
    table.set_string(1, 5, "Foo");
    CHECK(!distinct_ints.is_in_sync());
    distinct_ints.sync_if_needed();
    CHECK(distinct_ints.is_in_sync());
    CHECK_EQUAL(5, distinct_ints.size());

    // Check that adding a value that impacts the view invalidates the view.
    distinct_ints.sync_if_needed();
    table.add_empty_row();
    table.set_int(0, 6, 10);
    table.set_string(1, 6, "Foo");
    CHECK(!distinct_ints.is_in_sync());
    distinct_ints.sync_if_needed();
    CHECK(distinct_ints.is_in_sync());
    CHECK_EQUAL(6, distinct_ints.size());
}


TEST(TableView_SyncAfterCopy)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0, 0, 1);

    // do initial query
    Query q = table.where().equal(0, 1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0, 0));

    // move the tableview
    TableView v2 = v;
    CHECK_EQUAL(1, v2.size());

    // make a change
    size_t ndx2 = table.add_empty_row();
    table.set_int(0, ndx2, 1);

    // verify that the copied view sees the change
    v2.sync_if_needed();
    CHECK_EQUAL(2, v2.size());
}

TEST(TableView_FindAll)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 0);
    add(table, 0);
    add(table, 0);

    TableView v = table.find_all_int(0, 0);
    CHECK_EQUAL(3, v.size());
    v[0].set_int(0, 5);
    v[1].set_int(0, 4); // match
    v[2].set_int(0, 4); // match

    // todo, add creation to wrapper function in table.h
    TableView v2 = v.find_all_int(0, 4);
    CHECK_EQUAL(2, v2.size());
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}


TEST(TableView_FindAllString)
{
    TestTable table;
    table.add_column(type_String, "1");

    add(table, "a");
    add(table, "a");
    add(table, "a");

    TableView v = table.find_all_string(0, "a");
    v[0].set_string(0, "foo");
    v[1].set_string(0, "bar"); // match
    v[2].set_string(0, "bar"); // match

    // todo, add creation to wrapper function in table.h
    TableView v2 = v.find_all_string(0, "bar");
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}


NONCONCURRENT_TEST(TableView_StringSort)
{
    // WARNING: Do not use the C++11 method (set_string_compare_method(1)) on Windows 8.1 because it has a bug that
    // takes length in count when sorting ("b" comes before "aaaa"). Bug is not present in Windows 7.

    // Test of handling of unicode takes place in test_utf8.cpp
    TestTable table;
    table.add_column(type_String, "1");

    add(table, "alpha");
    add(table, "zebra");
    add(table, "ALPHA");
    add(table, "ZEBRA");

    // Core-only is default comparer
    TableView v = table.where().find_all();
    v.sort(0);
    CHECK_EQUAL("alpha", v[0].get_string(0));
    CHECK_EQUAL("ALPHA", v[1].get_string(0));
    CHECK_EQUAL("zebra", v[2].get_string(0));
    CHECK_EQUAL("ZEBRA", v[3].get_string(0));

    // Should be exactly the same as above because 0 was default already
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
    v.sort(0);
    CHECK_EQUAL("alpha", v[0].get_string(0));
    CHECK_EQUAL("ALPHA", v[1].get_string(0));
    CHECK_EQUAL("zebra", v[2].get_string(0));
    CHECK_EQUAL("ZEBRA", v[3].get_string(0));

    // Test descending mode
    v.sort(0, false);
    CHECK_EQUAL("alpha", v[3].get_string(0));
    CHECK_EQUAL("ALPHA", v[2].get_string(0));
    CHECK_EQUAL("zebra", v[1].get_string(0));
    CHECK_EQUAL("ZEBRA", v[0].get_string(0));

    // primitive C locale comparer. But that's OK since all we want to test is
    // if the callback is invoked
    bool got_called = false;
    auto comparer = [&](const char* s1, const char* s2) {
        got_called = true;
        return *s1 < *s2;
    };

    // Test if callback comparer works. Our callback is a primitive dummy-comparer
    set_string_compare_method(STRING_COMPARE_CALLBACK, comparer);
    v.sort(0);
    CHECK_EQUAL("ALPHA", v[0].get_string(0));
    CHECK_EQUAL("ZEBRA", v[1].get_string(0));
    CHECK_EQUAL("alpha", v[2].get_string(0));
    CHECK_EQUAL("zebra", v[3].get_string(0));
    CHECK_EQUAL(true, got_called);

#ifdef _MSC_VER
    // Try C++11 method which uses current locale of the operating system to give precise sorting. This C++11 feature
    // is currently (mid 2014) only supported by Visual Studio
    got_called = false;
    bool available = set_string_compare_method(STRING_COMPARE_CPP11, nullptr);
    if (available) {
        v.sort(0);
        CHECK_EQUAL("alpha", v[0].get_string(0));
        CHECK_EQUAL("ALPHA", v[1].get_string(0));
        CHECK_EQUAL("zebra", v[2].get_string(0));
        CHECK_EQUAL("ZEBRA", v[3].get_string(0));
        CHECK_EQUAL(false, got_called);
    }
#endif

    // Set back to default for use by other unit tests
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


TEST(TableView_FloatDoubleSort)
{
    TestTable t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");

    add(t, 1.0f, 10.0);
    add(t, 3.0f, 30.0);
    add(t, 2.0f, 20.0);
    add(t, 0.0f, 5.0);

    TableView tv = t.where().find_all();
    tv.sort(0);

    CHECK_EQUAL(0.0f, tv[0].get_float(0));
    CHECK_EQUAL(1.0f, tv[1].get_float(0));
    CHECK_EQUAL(2.0f, tv[2].get_float(0));
    CHECK_EQUAL(3.0f, tv[3].get_float(0));

    tv.sort(1);
    CHECK_EQUAL(5.0f, tv[0].get_double(1));
    CHECK_EQUAL(10.0f, tv[1].get_double(1));
    CHECK_EQUAL(20.0f, tv[2].get_double(1));
    CHECK_EQUAL(30.0f, tv[3].get_double(1));
}

TEST(TableView_DoubleSortPrecision)
{
    // Detect if sorting algorithm accidentially casts doubles to float somewhere so that precision gets lost
    TestTable t;
    t.add_column(type_Float, "1");
    t.add_column(type_Double, "2");

    double d1 = 100000000000.0;
    double d2 = 100000000001.0;

    // When casted to float, they are equal
    float f1 = static_cast<float>(d1);
    float f2 = static_cast<float>(d2);

    // If this check fails, it's a bug in this unit test, not in Realm
    CHECK_EQUAL(f1, f2);

    // First verify that our unit is guaranteed to find such a bug; that is, test if such a cast is guaranteed to give
    // bad sorting order. This is not granted, because an unstable sorting algorithm could *by chance* give the
    // correct sorting order. Fortunatly we use std::stable_sort which must maintain order on draws.
    add(t, f2, d2);
    add(t, f1, d1);

    TableView tv = t.where().find_all();
    tv.sort(0);

    // Sort should be stable
    CHECK_EQUAL(f2, tv[0].get_float(0));
    CHECK_EQUAL(f1, tv[1].get_float(0));

    // If sort is stable, and compare makes a draw because the doubles are accidentially casted to float in Realm,
    // then
    // original order would be maintained. Check that it's not maintained:
    tv.sort(1);
    CHECK_EQUAL(d1, tv[0].get_double(1));
    CHECK_EQUAL(d2, tv[1].get_double(1));
}

TEST(TableView_SortNullString)
{
    Table t;
    t.add_column(type_String, "s", true);
    t.add_empty_row(4);
    t.set_string(0, 0, StringData("")); // empty string
    t.set_string(0, 1, realm::null());  // realm::null()
    t.set_string(0, 2, StringData("")); // empty string
    t.set_string(0, 3, realm::null());  // realm::null()

    TableView tv;

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());

    t.set_string(0, 0, StringData("medium medium medium medium"));

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());

    t.set_string(0, 0, StringData("long long long long long long long long long long long long long long"));

    tv = t.where().find_all();
    tv.sort(0);
    CHECK(tv.get_string(0, 0).is_null());
    CHECK(tv.get_string(0, 1).is_null());
    CHECK(!tv.get_string(0, 2).is_null());
    CHECK(!tv.get_string(0, 3).is_null());
}

TEST(TableView_Delete)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 1);
    add(table, 2);
    add(table, 1);
    add(table, 3);
    add(table, 1);

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(3, v.size());

    v.remove(1);
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_source_ndx(0));
    CHECK_EQUAL(3, v.get_source_ndx(1));

    CHECK_EQUAL(4, table.size());
    CHECK_EQUAL(1, table[0].get_int(0));
    CHECK_EQUAL(2, table[1].get_int(0));
    CHECK_EQUAL(3, table[2].get_int(0));
    CHECK_EQUAL(1, table[3].get_int(0));

    v.remove(0);
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    CHECK_EQUAL(3, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
    CHECK_EQUAL(1, table[2].get_int(0));

    v.remove(0);
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
}

TEST(TableView_Clear)
{
    TestTable table;
    table.add_column(type_Int, "first");

    add(table, 1);
    add(table, 2);
    add(table, 1);
    add(table, 3);
    add(table, 1);

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(3, v.size());

    v.clear();
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].get_int(0));
    CHECK_EQUAL(3, table[1].get_int(0));
}


// Verify that TableView::clear() can handle a detached ref,
// so that it can be used in an imperative setting
TEST(TableView_Imperative_Clear)
{
    Table t;
    t.add_column(type_Int, "i1");
    t.add_empty_row(3);
    t.set_int(0, 0, 7);
    t.set_int(0, 1, 13);
    t.set_int(0, 2, 29);

    TableView v = t.where().less(0, 20).find_all();
    CHECK_EQUAL(2, v.size());
    // remove the underlying entry in the table, introducing a detached ref
    t.move_last_over(v.get_source_ndx(0));
    // the detached ref still counts as an entry when calling size()
    CHECK_EQUAL(2, v.size());
    // but is does not count as attached anymore:
    CHECK_EQUAL(1, v.num_attached_rows());
    v.clear();
    CHECK_EQUAL(0, v.size());
    CHECK_EQUAL(1, t.size());
}

// exposes a bug in stacked tableview:
// view V1 selects a subset of rows from Table T1
// View V2 selects rows from  view V1
// Then, some rows in V2 can be found, that are not in V1
TEST(TableView_Stacked)
{
    Table t;
    t.add_column(type_Int, "i1");
    t.add_column(type_Int, "i2");
    t.add_column(type_String, "S1");
    t.add_empty_row(2);
    t.set_int(0, 0, 1);      // 1
    t.set_int(1, 0, 2);      // 2
    t.set_string(2, 0, "A"); // "A"
    t.set_int(0, 1, 2);      // 2
    t.set_int(1, 1, 2);      // 2
    t.set_string(2, 1, "B"); // "B"

    TableView tv = t.find_all_int(0, 2);
    TableView tv2 = tv.find_all_int(1, 2);
    CHECK_EQUAL(1, tv2.size());             // evaluates tv2.size to 1 which is expected
    CHECK_EQUAL("B", tv2.get_string(2, 0)); // evalates get_string(2,0) to "A" which is not expected
}


TEST(TableView_ClearNone)
{
    Table table;
    table.add_column(type_Int, "first");

    TableView v = table.find_all_int(0, 1);
    CHECK_EQUAL(0, v.size());

    v.clear();
}

TEST(TableView_FindAllStacked)
{
    TestTable table;
    table.add_column(type_Int, "1");
    table.add_column(type_Int, "2");	

    add(table, 0, 1);
    add(table, 0, 2);
    add(table, 0, 3);
    add(table, 1, 1);
    add(table, 1, 2);
    add(table, 1, 3);

    TableView v = table.find_all_int(0, 0);
    CHECK_EQUAL(3, v.size());

    TableView v2 = v.find_all_int(1, 2);
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(0, v2[0].get_int(0));
    CHECK_EQUAL(2, v2[0].get_int(1));
    CHECK_EQUAL(1, v2.get_source_ndx(0));
}


TEST(TableView_LowLevelSubtables)
{
    Table table;
    std::vector<size_t> column_path;
    table.add_column(type_Bool, "enable");
    table.add_column(type_Table, "subtab");
    table.add_column(type_Mixed, "mixed");
    column_path.push_back(1);
    table.add_subcolumn(column_path, type_Bool, "enable");
    table.add_subcolumn(column_path, type_Table, "subtab");
    table.add_subcolumn(column_path, type_Mixed, "mixed");
    column_path.push_back(1);
    table.add_subcolumn(column_path, type_Bool, "enable");
    table.add_subcolumn(column_path, type_Table, "subtab");
    table.add_subcolumn(column_path, type_Mixed, "mixed");

    table.add_empty_row(2 * 2);
    table.set_bool(0, 1, true);
    table.set_bool(0, 3, true);
    TableView view = table.where().equal(0, true).find_all();
    CHECK_EQUAL(2, view.size());
    for (int i_1 = 0; i_1 != 2; ++i_1) {
        TableRef subtab = view.get_subtable(1, i_1);
        subtab->add_empty_row(2 * (2 + i_1));
        for (int i_2 = 0; i_2 != 2 * (2 + i_1); ++i_2)
            subtab->set_bool(0, i_2, i_2 % 2 == 0);
        TableView subview = subtab->where().equal(0, true).find_all();
        CHECK_EQUAL(2 + i_1, subview.size());
        {
            TableRef subsubtab = subview.get_subtable(1, 0 + i_1);
            subsubtab->add_empty_row(2 * (3 + i_1));
            for (int i_3 = 0; i_3 != 2 * (3 + i_1); ++i_3)
                subsubtab->set_bool(0, i_3, i_3 % 2 == 1);
            TableView subsubview = subsubtab->where().equal(0, true).find_all();
            CHECK_EQUAL(3 + i_1, subsubview.size());

            for (int i_3 = 0; i_3 != 3 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            subview.clear_subtable(2, 1 + i_1); // Mixed
            TableRef subsubtab_mix = subview.get_subtable(2, 1 + i_1);
            subsubtab_mix->add_column(type_Bool, "enable");
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(2 * (1 + i_1));
            for (int i_3 = 0; i_3 != 2 * (1 + i_1); ++i_3)
                subsubtab_mix->set_bool(0, i_3, i_3 % 2 == 0);
            TableView subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(1 + i_1, subsubview_mix.size());

            for (int i_3 = 0; i_3 != 1 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(subview.get_subtable(1, i_2)));
            CHECK_EQUAL(i_2 == 1 + i_1, bool(subview.get_subtable(2, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 0 + i_1 ? 2 * (3 + i_1) : 0, subview.get_subtable_size(1, i_2));
            CHECK_EQUAL(i_2 == 1 + i_1 ? 2 * (1 + i_1) : 0, subview.get_subtable_size(2, i_2)); // Mixed
        }

        view.clear_subtable(2, i_1); // Mixed
        TableRef subtab_mix = view.get_subtable(2, i_1);
        std::vector<size_t> subcol_path;
        subtab_mix->add_column(type_Bool, "enable");
        subtab_mix->add_column(type_Table, "subtab");
        subtab_mix->add_column(type_Mixed, "mixed");
        subcol_path.push_back(1);
        subtab_mix->add_subcolumn(subcol_path, type_Bool, "enable");
        subtab_mix->add_subcolumn(subcol_path, type_Table, "subtab");
        subtab_mix->add_subcolumn(subcol_path, type_Mixed, "mixed");
        subtab_mix->add_empty_row(2 * (3 + i_1));
        for (int i_2 = 0; i_2 != 2 * (3 + i_1); ++i_2)
            subtab_mix->set_bool(0, i_2, i_2 % 2 == 1);
        TableView subview_mix = subtab_mix->where().equal(0, true).find_all();
        CHECK_EQUAL(3 + i_1, subview_mix.size());
        {
            TableRef subsubtab = subview_mix.get_subtable(1, 1 + i_1);
            subsubtab->add_empty_row(2 * (7 + i_1));
            for (int i_3 = 0; i_3 != 2 * (7 + i_1); ++i_3)
                subsubtab->set_bool(0, i_3, i_3 % 2 == 1);
            TableView subsubview = subsubtab->where().equal(0, true).find_all();
            CHECK_EQUAL(7 + i_1, subsubview.size());

            for (int i_3 = 0; i_3 != 7 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            subview_mix.clear_subtable(2, 2 + i_1); // Mixed
            TableRef subsubtab_mix = subview_mix.get_subtable(2, 2 + i_1);
            subsubtab_mix->add_column(type_Bool, "enable");
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(2 * (5 + i_1));
            for (int i_3 = 0; i_3 != 2 * (5 + i_1); ++i_3)
                subsubtab_mix->set_bool(0, i_3, i_3 % 2 == 0);
            TableView subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(5 + i_1, subsubview_mix.size());

            for (int i_3 = 0; i_3 != 5 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(subview_mix.get_subtable(1, i_2)));
            CHECK_EQUAL(i_2 == 2 + i_1, bool(subview_mix.get_subtable(2, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 1 + i_1 ? 2 * (7 + i_1) : 0, subview_mix.get_subtable_size(1, i_2));
            CHECK_EQUAL(i_2 == 2 + i_1 ? 2 * (5 + i_1) : 0, subview_mix.get_subtable_size(2, i_2)); // Mixed
        }

        CHECK_EQUAL(true, bool(view.get_subtable(1, i_1)));
        CHECK_EQUAL(true, bool(view.get_subtable(2, i_1))); // Mixed
        CHECK_EQUAL(2 * (2 + i_1), view.get_subtable_size(1, i_1));
        CHECK_EQUAL(2 * (3 + i_1), view.get_subtable_size(2, i_1)); // Mixed
    }


    ConstTableView const_view = table.where().equal(0, true).find_all();
    CHECK_EQUAL(2, const_view.size());
    for (int i_1 = 0; i_1 != 2; ++i_1) {
        ConstTableRef subtab = const_view.get_subtable(1, i_1);
        ConstTableView const_subview = subtab->where().equal(0, true).find_all();
        CHECK_EQUAL(2 + i_1, const_subview.size());
        {
            ConstTableRef subsubtab = const_subview.get_subtable(1, 0 + i_1);
            ConstTableView const_subsubview = subsubtab->where().equal(0, true).find_all();
            CHECK_EQUAL(3 + i_1, const_subsubview.size());
            for (int i_3 = 0; i_3 != 3 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(const_subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            ConstTableRef subsubtab_mix = const_subview.get_subtable(2, 1 + i_1);
            ConstTableView const_subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(1 + i_1, const_subsubview_mix.size());
            for (int i_3 = 0; i_3 != 1 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(const_subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(const_subview.get_subtable(1, i_2)));
            CHECK_EQUAL(i_2 == 1 + i_1, bool(const_subview.get_subtable(2, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 0 + i_1 ? 2 * (3 + i_1) : 0, const_subview.get_subtable_size(1, i_2));
            CHECK_EQUAL(i_2 == 1 + i_1 ? 2 * (1 + i_1) : 0, const_subview.get_subtable_size(2, i_2)); // Mixed
        }

        ConstTableRef subtab_mix = const_view.get_subtable(2, i_1);
        ConstTableView const_subview_mix = subtab_mix->where().equal(0, true).find_all();
        CHECK_EQUAL(3 + i_1, const_subview_mix.size());
        {
            ConstTableRef subsubtab = const_subview_mix.get_subtable(1, 1 + i_1);
            ConstTableView const_subsubview = subsubtab->where().equal(0, true).find_all();
            CHECK_EQUAL(7 + i_1, const_subsubview.size());
            for (int i_3 = 0; i_3 != 7 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(const_subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            ConstTableRef subsubtab_mix = const_subview_mix.get_subtable(2, 2 + i_1);
            ConstTableView const_subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(5 + i_1, const_subsubview_mix.size());
            for (int i_3 = 0; i_3 != 5 + i_1; ++i_3) {
                CHECK_EQUAL(true, bool(const_subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true, bool(const_subview_mix.get_subtable(1, i_2)));
            CHECK_EQUAL(i_2 == 2 + i_1, bool(const_subview_mix.get_subtable(2, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 1 + i_1 ? 2 * (7 + i_1) : 0, const_subview_mix.get_subtable_size(1, i_2));
            CHECK_EQUAL(i_2 == 2 + i_1 ? 2 * (5 + i_1) : 0, const_subview_mix.get_subtable_size(2, i_2)); // Mixed
        }

        CHECK_EQUAL(true, bool(const_view.get_subtable(1, i_1)));
        CHECK_EQUAL(true, bool(const_view.get_subtable(2, i_1))); // Mixed
        CHECK_EQUAL(2 * (2 + i_1), const_view.get_subtable_size(1, i_1));
        CHECK_EQUAL(2 * (3 + i_1), const_view.get_subtable_size(2, i_1)); // Mixed
    }
}


TEST(TableView_HighLevelSubtables)
{
    Table t;
    t.add_column(type_Int, "val");
    DescriptorRef sub;
    t.add_column(type_Table, "subtab", &sub);
    sub->add_column(type_Int, "val");
    DescriptorRef subsub;
    sub->add_column(type_Table, "subtab", &subsub);
    subsub->add_column(type_Int, "value");

    const Table& ct = t;

    t.add_empty_row();
    TableView v = t.find_all_int(0, 0);
    ConstTableView cv = ct.find_all_int(0, 0);

    {
        TableView v2 = v.find_all_int(0, 0);
        ConstTableView cv2 = cv.find_all_int(0, 0);

        ConstTableView cv3 = t.find_all_int(0, 0);
        ConstTableView cv4 = v.find_all_int(0, 0);

        // Also test assigment that converts to const
        cv3 = t.find_all_int(0, 0);
        cv4 = v.find_all_int(0, 0);

        static_cast<void>(v2);
        static_cast<void>(cv2);
        static_cast<void>(cv3);
        static_cast<void>(cv4);
    }

    v[0].get_subtable(1).get()->add_empty_row();
    v[0].get_subtable(1).get()->get_subtable(1, 0).get()->add_empty_row();

    v[0].get_subtable(1).get()->set_int(0, 0, 1);
    v[0].get_subtable(1).get()->get_subtable(1, 0).get()->set_int(0, 0, 2);

    CHECK_EQUAL(v[0].get_subtable(1).get()->get_int(0, 0), 1);
    CHECK_EQUAL(v.get_subtable(1, 0).get()->get_subtable(1, 0).get()->get_int(0, 0), 2);

    CHECK_EQUAL(cv[0].get_subtable(1).get()->get_int(0, 0), 1);
    CHECK_EQUAL(cv.get_subtable(1, 0).get()->get_subtable(1, 0).get()->get_int(0, 0), 2);
}


TEST(TableView_ToString)
{
    TestTable tbl;
    tbl.add_column(type_Int, "first");
    tbl.add_column(type_Int, "second");	

    add(tbl, 2, 123456);
    add(tbl, 4, 1234567);
    add(tbl, 6, 12345678);
    add(tbl, 4, 12345678);

    std::string s = "    first    second\n";
    std::string s0 = "0:      2    123456\n";
    std::string s1 = "1:      4   1234567\n";
    std::string s2 = "2:      6  12345678\n";
    std::string s3 = "3:      4  12345678\n";

    // Test full view
    std::stringstream ss;
    TableView tv = tbl.where().find_all();
    tv.to_string(ss);
    CHECK_EQUAL(s + s0 + s1 + s2 + s3, ss.str());

    // Find partial view: row 1+3
    std::stringstream ss2;
    tv = tbl.where().equal(0, 4).find_all();
    tv.to_string(ss2);
    CHECK_EQUAL(s + s1 + s3, ss2.str());

    // test row_to_string. get row 0 of previous view - i.e. row 1 in tbl
    std::stringstream ss3;
    tv.row_to_string(0, ss3);
    CHECK_EQUAL(s + s1, ss3.str());
}


TEST(TableView_RefCounting)
{
    TableView tv, tv2;
    {
        TableRef t = Table::create();
        t->add_column(type_Int, "myint");
        t->add_empty_row();
        t->set_int(0, 0, 12);
        tv = t->where().find_all();
    }

    {
        TableRef t2 = Table::create();
        t2->add_column(type_String, "mystr");
        t2->add_empty_row();
        t2->set_string(0, 0, "just a test string");
        tv2 = t2->where().find_all();
    }

    // Now try to access TableView and see that the Table is still alive
    int64_t i = tv.get_int(0, 0);
    CHECK_EQUAL(i, 12);
    std::string s = tv2.get_string(0, 0);
    CHECK_EQUAL(s, "just a test string");
}


TEST(TableView_DynPivot)
{
    TableRef table = Table::create();
    size_t column_ndx_sex = table->add_column(type_String, "sex");
    size_t column_ndx_age = table->add_column(type_Int, "age");
    table->add_column(type_Bool, "hired");

    size_t count = 5000;
    for (size_t i = 0; i < count; ++i) {
        StringData sex = i % 2 ? "Male" : "Female";
        table->insert_empty_row(i);
        table->set_string(0, i, sex);
        table->set_int(1, i, 20 + (i % 20));
        table->set_bool(2, i, true);
    }

    TableView tv = table->where().find_all();

    Table result_count;
    tv.aggregate(0, 1, Table::aggr_count, result_count);
    int64_t half = count / 2;
    CHECK_EQUAL(2, result_count.get_column_count());
    CHECK_EQUAL(2, result_count.size());
    CHECK_EQUAL(half, result_count.get_int(1, 0));
    CHECK_EQUAL(half, result_count.get_int(1, 1));

    Table result_sum;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_sum, result_sum);

    Table result_avg;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_avg, result_avg);

    Table result_min;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_min, result_min);

    Table result_max;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_max, result_max);


    // Test with enumerated strings
    table->optimize();

    Table result_count2;
    tv.aggregate(column_ndx_sex, column_ndx_age, Table::aggr_count, result_count2);
    CHECK_EQUAL(2, result_count2.get_column_count());
    CHECK_EQUAL(2, result_count2.size());
    CHECK_EQUAL(half, result_count2.get_int(1, 0));
    CHECK_EQUAL(half, result_count2.get_int(1, 1));
}


TEST(TableView_RowAccessor)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.set_int(0, 0, 703);
    TableView tv = table.where().find_all();
    Row row = tv[0];
    CHECK_EQUAL(703, row.get_int(0));
    ConstRow crow = tv[0];
    CHECK_EQUAL(703, crow.get_int(0));
    ConstTableView ctv = table.where().find_all();
    ConstRow crow_2 = ctv[0];
    CHECK_EQUAL(703, crow_2.get_int(0));
}

TEST(TableView_FindBySourceNdx)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 2);
    TableView tv = table.where().find_all();
    tv.sort(0, false);
    CHECK_EQUAL(0, tv.find_by_source_ndx(2));
    CHECK_EQUAL(1, tv.find_by_source_ndx(1));
    CHECK_EQUAL(2, tv.find_by_source_ndx(0));
}

TEST(TableView_MultiColSort)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_column(type_Float, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 1);

    table[0].set_float(1, 0.f);
    table[1].set_float(1, 2.f);
    table[2].set_float(1, 1.f);

    TableView tv = table.where().find_all();

    std::vector<std::vector<size_t>> v = {{0}, {1}};
    std::vector<bool> a = {true, true};

    tv.sort(SortDescriptor{table, v, a});

    CHECK_EQUAL(tv.get_float(1, 0), 0.f);
    CHECK_EQUAL(tv.get_float(1, 1), 1.f);
    CHECK_EQUAL(tv.get_float(1, 2), 2.f);

    std::vector<bool> a_descending = {false, false};
    tv.sort(SortDescriptor{table, v, a_descending});

    CHECK_EQUAL(tv.get_float(1, 0), 2.f);
    CHECK_EQUAL(tv.get_float(1, 1), 1.f);
    CHECK_EQUAL(tv.get_float(1, 2), 0.f);

    std::vector<bool> a_ascdesc = {true, false};
    tv.sort(SortDescriptor{table, v, a_ascdesc});

    CHECK_EQUAL(tv.get_float(1, 0), 0.f);
    CHECK_EQUAL(tv.get_float(1, 1), 2.f);
    CHECK_EQUAL(tv.get_float(1, 2), 1.f);
}

TEST(TableView_QueryCopy)
{
    Table table;
    table.add_column(type_Int, "");
    table.add_empty_row();
    table.add_empty_row();
    table.add_empty_row();
    table[0].set_int(0, 0);
    table[1].set_int(0, 1);
    table[2].set_int(0, 2);

    // Test if copy-assign of Query in TableView works
    TableView tv = table.where().find_all();

    Query q = table.where();

    q.group();
    q.equal(0, 1);
    q.Or();
    q.equal(0, 2);
    q.end_group();

    q.count();

    Query q2;
    q2 = table.where().equal(0, 1234);

    q2 = q;
    size_t t = q2.count();

    CHECK_EQUAL(t, 2);
}

TEST(TableView_SortEnum)
{
    Table table;
    table.add_column(type_String, "str");
    table.add_empty_row(3);
    table[0].set_string(0, "foo");
    table[1].set_string(0, "foo");
    table[2].set_string(0, "foo");

    table.optimize();

    table.add_empty_row(3);
    table[3].set_string(0, "bbb");
    table[4].set_string(0, "aaa");
    table[5].set_string(0, "baz");

    TableView tv = table.where().find_all();
    tv.sort(0);

    CHECK_EQUAL(tv[0].get_string(0), "aaa");
    CHECK_EQUAL(tv[1].get_string(0), "baz");
    CHECK_EQUAL(tv[2].get_string(0), "bbb");
    CHECK_EQUAL(tv[3].get_string(0), "foo");
    CHECK_EQUAL(tv[4].get_string(0), "foo");
    CHECK_EQUAL(tv[5].get_string(0), "foo");
}


TEST(TableView_UnderlyingRowRemoval)
{
    struct Fixture {
        Table table;
        TableView view;
        Fixture()
        {
            table.add_column(type_Int, "a");
            table.add_column(type_Int, "b");
            table.add_empty_row(5);

            table.set_int(0, 0, 0);
            table.set_int(0, 1, 1);
            table.set_int(0, 2, 2);
            table.set_int(0, 3, 3);
            table.set_int(0, 4, 4);

            table.set_int(1, 0, 0);
            table.set_int(1, 1, 1);
            table.set_int(1, 2, 0);
            table.set_int(1, 3, 1);
            table.set_int(1, 4, 1);

            view = table.find_all_int(1, 0);
        }
    };

    // Sanity
    {
        Fixture f;
        CHECK_EQUAL(2, f.view.size());
        CHECK_EQUAL(0, f.view.get_source_ndx(0));
        CHECK_EQUAL(2, f.view.get_source_ndx(1));
    }

    // The following checks assume that unordered row removal in the underlying
    // table is done using `Table::move_last_over()`, and that Table::clear()
    // does that in reverse order of rows in the view.

    // Ordered remove()
    {
        Fixture f;
        f.view.remove(0);
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(1, f.table.get_int(0, 0));
        CHECK_EQUAL(2, f.table.get_int(0, 1));
        CHECK_EQUAL(3, f.table.get_int(0, 2));
        CHECK_EQUAL(4, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(1, f.view.get_source_ndx(0));
    }
    {
        Fixture f;
        f.view.remove(1);
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(0, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(3, f.table.get_int(0, 2));
        CHECK_EQUAL(4, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(0, f.view.get_source_ndx(0));
    }

    // Unordered remove()
    {
        Fixture f;
        f.view.remove(0, RemoveMode::unordered);
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(4, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(2, f.table.get_int(0, 2));
        CHECK_EQUAL(3, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(2, f.view.get_source_ndx(0));
    }
    {
        Fixture f;
        f.view.remove(1, RemoveMode::unordered);
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(0, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(4, f.table.get_int(0, 2));
        CHECK_EQUAL(3, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(0, f.view.get_source_ndx(0));
    }

    // Ordered remove_last()
    {
        Fixture f;
        f.view.remove_last();
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(0, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(3, f.table.get_int(0, 2));
        CHECK_EQUAL(4, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(0, f.view.get_source_ndx(0));
    }

    // Unordered remove_last()
    {
        Fixture f;
        f.view.remove_last(RemoveMode::unordered);
        CHECK_EQUAL(4, f.table.size());
        CHECK_EQUAL(0, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(4, f.table.get_int(0, 2));
        CHECK_EQUAL(3, f.table.get_int(0, 3));
        CHECK_EQUAL(1, f.view.size());
        CHECK_EQUAL(0, f.view.get_source_ndx(0));
    }

    // Ordered clear()
    {
        Fixture f;
        f.view.clear();
        CHECK_EQUAL(3, f.table.size());
        CHECK_EQUAL(1, f.table.get_int(0, 0));
        CHECK_EQUAL(3, f.table.get_int(0, 1));
        CHECK_EQUAL(4, f.table.get_int(0, 2));
        CHECK_EQUAL(0, f.view.size());
    }

    // Unordered clear()
    {
        Fixture f;
        f.view.clear(RemoveMode::unordered);
        CHECK_EQUAL(3, f.table.size());
        CHECK_EQUAL(3, f.table.get_int(0, 0));
        CHECK_EQUAL(1, f.table.get_int(0, 1));
        CHECK_EQUAL(4, f.table.get_int(0, 2));
        CHECK_EQUAL(0, f.view.size());
    }
}

TEST(TableView_Backlinks)
{
    Group group;

    TableRef source = group.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group.add_table("links");
    links->add_column_link(type_Link, "link", *source);
    links->add_column_link(type_LinkList, "link_list", *source);

    source->add_empty_row(3);

    {
        // Links
        TableView tv = source->get_backlink_view(2, links.get(), 0);

        CHECK_EQUAL(tv.size(), 0);

        links->add_empty_row();
        links->set_link(0, 0, 2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 1);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
    {
        // LinkViews
        TableView tv = source->get_backlink_view(2, links.get(), 1);

        CHECK_EQUAL(tv.size(), 0);

        auto ll = links->get_linklist(1, 0);
        ll->add(2);
        ll->add(0);
        ll->add(2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 2);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
}

// Verify that a TableView that represents backlinks to a row functions correctly
// after being move-assigned.
TEST(TableView_BacklinksAfterMoveAssign)
{
    Group group;

    TableRef source = group.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group.add_table("links");
    links->add_column_link(type_Link, "link", *source);
    links->add_column_link(type_LinkList, "link_list", *source);

    source->add_empty_row(3);

    {
        // Links
        TableView tv_source = source->get_backlink_view(2, links.get(), 0);
        TableView tv;
        tv = std::move(tv_source);

        CHECK_EQUAL(tv.size(), 0);

        links->add_empty_row();
        links->set_link(0, 0, 2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 1);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
    {
        // LinkViews
        TableView tv_source = source->get_backlink_view(2, links.get(), 1);
        TableView tv;
        tv = std::move(tv_source);

        CHECK_EQUAL(tv.size(), 0);

        auto ll = links->get_linklist(1, 0);
        ll->add(2);
        ll->add(0);
        ll->add(2);

        tv.sync_if_needed();
        CHECK_EQUAL(tv.size(), 2);
        CHECK_EQUAL(tv[0].get_index(), links->get(0).get_index());
    }
}

// Verify that a TableView that represents backlinks continues to track the correct row
// when it moves within a table or is deleted.
TEST(TableView_BacklinksWhenTargetRowMovedOrDeleted)
{
    Group group;

    TableRef source = group.add_table("source");
    source->add_column(type_Int, "int");

    TableRef links = group.add_table("links");
    size_t col_link = links->add_column_link(type_Link, "link", *source);
    size_t col_linklist = links->add_column_link(type_LinkList, "link_list", *source);

    source->add_empty_row(3);

    links->add_empty_row(3);
    links->set_link(col_link, 0, 1);
    LinkViewRef ll = links->get_linklist(col_linklist, 0);
    ll->add(1);
    ll->add(0);

    links->set_link(col_link, 1, 1);
    ll = links->get_linklist(col_linklist, 1);
    ll->add(1);

    links->set_link(col_link, 2, 0);

    TableView tv_link = source->get_backlink_view(1, links.get(), col_link);
    TableView tv_linklist = source->get_backlink_view(1, links.get(), col_linklist);

    CHECK_EQUAL(tv_link.size(), 2);
    CHECK_EQUAL(tv_linklist.size(), 2);

    source->swap_rows(1, 0);
    tv_link.sync_if_needed();
    tv_linklist.sync_if_needed();

    CHECK_EQUAL(tv_link.size(), 2);
    CHECK_EQUAL(tv_linklist.size(), 2);

    CHECK(!tv_link.depends_on_deleted_object());
    CHECK(!tv_linklist.depends_on_deleted_object());

    source->move_last_over(0);

    CHECK(tv_link.depends_on_deleted_object());
    CHECK(tv_linklist.depends_on_deleted_object());

    CHECK(!tv_link.is_in_sync());
    CHECK(!tv_linklist.is_in_sync());

    tv_link.sync_if_needed();
    tv_linklist.sync_if_needed();

    CHECK(tv_link.is_in_sync());
    CHECK(tv_linklist.is_in_sync());

    CHECK_EQUAL(tv_link.size(), 0);
    CHECK_EQUAL(tv_linklist.size(), 0);

    source->add_empty_row();

    // TableViews that depend on a deleted row will stay in sync despite modifications to their table.
    CHECK(tv_link.is_in_sync());
    CHECK(tv_linklist.is_in_sync());
}

TEST(TableView_BacklinksWithColumnInsertion)
{
    Group g;
    TableRef target = g.add_table("target");
    target->add_column(type_Int, "int");
    target->add_empty_row(2);
    target->set_int(0, 1, 10);

    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *target);
    origin->add_column_link(type_LinkList, "linklist", *target);
    origin->add_empty_row(2);
    origin->set_link(0, 1, 1);
    origin->get_linklist(1, 1)->add(1);

    auto tv1 = target->get_backlink_view(1, origin.get(), 0);
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get_source_ndx(0), 1);

    auto tv2 = target->get_backlink_view(1, origin.get(), 1);
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(tv1.get_source_ndx(0), 1);

    target->insert_column(0, type_String, "string");
    target->insert_empty_row(0);

    tv1.sync_if_needed();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get_source_ndx(0), 1);

    tv2.sync_if_needed();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(tv2.get_source_ndx(0), 1);

    origin->insert_column(0, type_String, "string");
    target->insert_empty_row(0);
    origin->insert_empty_row(0);

    tv1.sync_if_needed();
    CHECK_EQUAL(tv1.size(), 1);
    CHECK_EQUAL(tv1.get_source_ndx(0), 2);

    tv2.sync_if_needed();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(tv2.get_source_ndx(0), 2);
}

namespace {
struct DistinctDirect {
    Table& table;
    DistinctDirect(TableRef, TableRef t)
        : table(*t)
    {
    }

    SortDescriptor operator()(std::initializer_list<size_t> columns, std::vector<bool> ascending = {}) const
    {
        std::vector<std::vector<size_t>> column_indices;
        for (size_t col : columns)
            column_indices.push_back({col});
        return SortDescriptor(table, column_indices, ascending);
    }

    size_t get_source_ndx(const TableView& tv, size_t ndx) const
    {
        return tv.get_source_ndx(ndx);
    }

    StringData get_string(const TableView& tv, size_t col, size_t row) const
    {
        return tv.get_string(col, row);
    }

    TableView find_all() const
    {
        return table.where().find_all();
    }
};

struct DistinctOverLink {
    Table& table;
    DistinctOverLink(TableRef t, TableRef)
        : table(*t)
    {
    }

    SortDescriptor operator()(std::initializer_list<size_t> columns, std::vector<bool> ascending = {}) const
    {
        std::vector<std::vector<size_t>> column_indices;
        for (size_t col : columns)
            column_indices.push_back({0, col});
        return SortDescriptor(table, column_indices, ascending);
    }

    size_t get_source_ndx(const TableView& tv, size_t ndx) const
    {
        return tv.get_link(0, ndx);
    }

    StringData get_string(const TableView& tv, size_t col, size_t row) const
    {
        return tv.get_link_target(0)->get_string(col, tv.get_link(0, row));
    }

    TableView find_all() const
    {
        return table.where().find_all();
    }
};
} // anonymous namespace

TEST_TYPES(TableView_Distinct, DistinctDirect, DistinctOverLink)
{
    // distinct() will preserve the original order of the row pointers, also if the order is a result of sort()
    // If multiple rows are indentical for the given set of distinct-columns, then only the first is kept.
    // You can call sync_if_needed() to update the distinct view, just like you can for a sorted view.
    // Each time you call distinct() it will first fetch the full original TableView contents and then apply
    // distinct() on that. So it distinct() does not filter the result of the previous distinct().

    // distinct() is internally based on the existing sort() method which is well tested. Hence it's not required
    // to test distinct() with all possible Realm data types.


    Group g;
    TableRef target = g.add_table("target");
    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *target);

    Table& t = *target;
    t.add_column(type_String, "s", true);
    t.add_column(type_Int, "i", true);
    t.add_column(type_Float, "f", true);

    t.add_empty_row(7);
    t.set_string(0, 0, StringData(""));
    t.set_int(1, 0, 100);
    t.set_float(2, 0, 100.f);

    t.set_string(0, 1, realm::null());
    t.set_int(1, 1, 200);
    t.set_float(2, 1, 200.f);

    t.set_string(0, 2, StringData(""));
    t.set_int(1, 2, 100);
    t.set_float(2, 2, 100.f);

    t.set_string(0, 3, realm::null());
    t.set_int(1, 3, 200);
    t.set_float(2, 3, 200.f);

    t.set_string(0, 4, "foo");
    t.set_int(1, 4, 300);
    t.set_float(2, 4, 300.f);

    t.set_string(0, 5, "foo");
    t.set_int(1, 5, 400);
    t.set_float(2, 5, 400.f);

    t.set_string(0, 6, "bar");
    t.set_int(1, 6, 500);
    t.set_float(2, 6, 500.f);

    origin->add_empty_row(t.size());
    for (size_t i = 0; i < t.size(); ++i)
        origin->set_link(0, i, i);

    TEST_TYPE h(origin, target);

    TableView tv;
    tv = h.find_all();
    tv.distinct(h({0}));
    CHECK_EQUAL(tv.size(), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 1);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 6);

    tv = h.find_all();
    tv.sort(h({0}));
    tv.distinct(h({0}));
    CHECK_EQUAL(tv.size(), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 1);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 6);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 4);

    tv = h.find_all();
    tv.sort(h({0}, {false}));
    tv.distinct(h({0}));
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 6);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 1);

    // Note here that our stable sort will sort the two "foo"s like row {4, 5}
    tv = h.find_all();
    tv.sort(h({0}, {false}));
    tv.distinct(h({0, 1}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 6);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 4), 1);


    // Now try distinct on string+float column. The float column has the same values as the int column
    // so the result should equal the test above
    tv = h.find_all();
    tv.sort(h({0}, {false}));
    tv.distinct(h({0, 1}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 6);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 4), 1);


    // Same as previous test, but with string column being Enum
    t.optimize(true); // true = enforce regardless if Realm thinks it pays off or not
    tv = h.find_all();
    tv.sort(h({0}, {false}));
    tv.distinct(h({0, 1}));
    CHECK_EQUAL(tv.size(), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 0), 4);
    CHECK_EQUAL(h.get_source_ndx(tv, 1), 5);
    CHECK_EQUAL(h.get_source_ndx(tv, 2), 6);
    CHECK_EQUAL(h.get_source_ndx(tv, 3), 0);
    CHECK_EQUAL(h.get_source_ndx(tv, 4), 1);


    // Now test sync_if_needed()
    tv = h.find_all();
    // "", null, "", null, "foo", "foo", "bar"

    tv.sort(h({0}, {false}));
    // "foo", "foo", "bar", "", "", null, null

    CHECK_EQUAL(tv.size(), 7);
    CHECK_EQUAL(h.get_string(tv, 0, 0), "foo");
    CHECK_EQUAL(h.get_string(tv, 0, 1), "foo");
    CHECK_EQUAL(h.get_string(tv, 0, 2), "bar");
    CHECK_EQUAL(h.get_string(tv, 0, 3), "");
    CHECK_EQUAL(h.get_string(tv, 0, 4), "");
    CHECK(h.get_string(tv, 0, 5).is_null());
    CHECK(h.get_string(tv, 0, 6).is_null());

    tv.distinct(h({0}));
    // "foo", "bar", "", null

    // remove "bar"
    origin->remove(6);
    target->remove(6);
    // access to tv undefined; may crash

    tv.sync_if_needed();
    // "foo", "", null

    CHECK_EQUAL(tv.size(), 3);
    CHECK_EQUAL(h.get_string(tv, 0, 0), "foo");
    CHECK_EQUAL(h.get_string(tv, 0, 1), "");
    CHECK(h.get_string(tv, 0, 2).is_null());

    // Remove distinct property by providing empty column list. Now TableView should look like it
    // did just after our last tv.sort(0, false) above, but after having executed table.remove(6)
    tv.distinct(SortDescriptor{});
    // "foo", "foo", "", "", null, null
    CHECK_EQUAL(tv.size(), 6);
    CHECK_EQUAL(h.get_string(tv, 0, 0), "foo");
    CHECK_EQUAL(h.get_string(tv, 0, 1), "foo");
    CHECK_EQUAL(h.get_string(tv, 0, 2), "");
    CHECK_EQUAL(h.get_string(tv, 0, 3), "");
    CHECK(h.get_string(tv, 0, 4).is_null());
    CHECK(h.get_string(tv, 0, 5).is_null());
}

TEST(TableView_DistinctOverNullLink)
{
    Group g;
    TableRef target = g.add_table("target");
    target->add_column(type_Int, "value");
    target->add_empty_row(2);
    target->set_int(0, 0, 1);
    target->set_int(0, 0, 2);

    TableRef origin = g.add_table("origin");
    origin->add_column_link(type_Link, "link", *target);
    origin->add_empty_row(5);
    origin->set_link(0, 0, 0);
    origin->set_link(0, 1, 1);
    origin->set_link(0, 2, 0);
    origin->set_link(0, 3, 1);
    // 4 is null

    auto tv = origin->where().find_all();
    tv.distinct(SortDescriptor(*origin, {{0, 0}}));
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get_source_ndx(0), 0);
    CHECK_EQUAL(tv.get_source_ndx(1), 1);
}

TEST(TableView_IsRowAttachedAfterClear)
{
    Table t;
    size_t col_id = t.add_column(type_Int, "id");

    t.add_empty_row(2);
    t.set_int(col_id, 0, 0);
    t.set_int(col_id, 1, 1);

    TableView tv = t.where().find_all();
    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_row_attached(0));
    CHECK(tv.is_row_attached(1));

    t.move_last_over(1);
    CHECK_EQUAL(2, tv.size());
    CHECK(tv.is_row_attached(0));
    CHECK(!tv.is_row_attached(1));

    t.clear();
    CHECK_EQUAL(2, tv.size());
    CHECK(!tv.is_row_attached(0));
    CHECK(!tv.is_row_attached(1));
}

TEST(TableView_IsInTableOrder)
{
    Group g;

    TableRef source = g.add_table("source");
    TableRef target = g.add_table("target");

    size_t col_link = source->add_column_link(type_LinkList, "link", *target);
    size_t col_name = source->add_column(type_String, "name");
    size_t col_id = target->add_column(type_Int, "id");
    target->add_search_index(col_id);

    source->add_empty_row();
    target->add_empty_row();

    // Detached views are in table order.
    TableView tv;
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Queries not restricted by views are in table order.
    tv = target->where().find_all();
    CHECK_EQUAL(true, tv.is_in_table_order());

    // Views that have a distinct filter remain in table order.
    tv.distinct(col_id);
    CHECK_EQUAL(true, tv.is_in_table_order());

    // Views that are sorted are not guaranteed to be in table order.
    tv.sort(col_id, true);
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Queries restricted by views are not guaranteed to be in table order.
    TableView restricting_view = target->where().equal(col_id, 0).find_all();
    tv = target->where(&restricting_view).find_all();
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Backlinks are not guaranteed to be in table order.
    tv = target->get_backlink_view(0, source.get(), col_link);
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Views derived from a LinkView are not guaranteed to be in table order.
    LinkViewRef ll = source->get_linklist(col_link, 0);
    tv = ll->get_sorted_view(col_name);
    CHECK_EQUAL(false, tv.is_in_table_order());

    // Views based directly on a table are in table order.
    tv = target->get_range_view(0, 1);
    CHECK_EQUAL(true, tv.is_in_table_order());
    tv = target->get_distinct_view(col_id);
    CHECK_EQUAL(true, tv.is_in_table_order());

    // … unless sorted.
    tv = target->get_sorted_view(col_id);
    CHECK_EQUAL(false, tv.is_in_table_order());
}


NONCONCURRENT_TEST(TableView_SortOrder_Similiar)
{
    TestTable table;
    table.add_column(type_String, "1");

    // This tests the expected sorting order with STRING_COMPARE_CORE_SIMILAR. See utf8_compare() in unicode.cpp. Only
    // characters
    // that have a visual representation are tested (control characters such as line feed are omitted).
    //
    // NOTE: Your editor must assume that Core source code is in utf8, and it must save as utf8, else this unit
    // test will fail.

    /*
    // This code snippet can be used to produce a list of *all* unicode characters in sorted order.
    //
    std::vector<int> original(collation_order, collation_order + sizeof collation_order / sizeof collation_order[0]);
    std::vector<int> sorted = original;
    std::sort(sorted.begin(), sorted.end());
    size_t highest_rank = sorted[sorted.size() - 1];

    std::wstring ws;
    for (size_t rank = 0; rank <= highest_rank; rank++) {
        size_t unicode = std::find(original.begin(), original.end(), rank) - original.begin();
        if (unicode != original.size()) {
            std::wcout << wchar_t(unicode) << "\n";
            std::cout << unicode << ", ";
            ws += wchar_t(unicode);
        }
    }
    */

    set_string_compare_method(STRING_COMPARE_CORE_SIMILAR, nullptr);

    add(table, " ");
    add(table, "!");
    add(table, "\"");
    add(table, "#");
    add(table, "%");
    add(table, "&");
    add(table, "'");
    add(table, "(");
    add(table, ")");
    add(table, "*");
    add(table, "+");
    add(table, ",");
    add(table, "-");
    add(table, ".");
    add(table, "/");
    add(table, ":");
    add(table, ";");
    add(table, "<");
    add(table, "=");
    add(table, ">");
    add(table, "?");
    add(table, "@");
    add(table, "[");
    add(table, "\\");
    add(table, "]");
    add(table, "^");
    add(table, "_");
    add(table, "`");
    add(table, "{");
    add(table, "|");
    add(table, "}");
    add(table, "~");
    add(table, " ");
    add(table, "¡");
    add(table, "¦");
    add(table, "§");
    add(table, "¨");
    add(table, "©");
    add(table, "«");
    add(table, "¬");
    add(table, "®");
    add(table, "¯");
    add(table, "°");
    add(table, "±");
    add(table, "´");
    add(table, "¶");
    add(table, "·");
    add(table, "¸");
    add(table, "»");
    add(table, "¿");
    add(table, "×");
    add(table, "÷");
    add(table, "¤");
    add(table, "¢");
    add(table, "$");
    add(table, "£");
    add(table, "¥");
    add(table, "0");
    add(table, "1");
    add(table, "¹");
    add(table, "½");
    add(table, "¼");
    add(table, "2");
    add(table, "²");
    add(table, "3");
    add(table, "³");
    add(table, "¾");
    add(table, "4");
    add(table, "5");
    add(table, "6");
    add(table, "7");
    add(table, "8");
    add(table, "9");
    add(table, "a");
    add(table, "A");
    add(table, "ª");
    add(table, "á");
    add(table, "Á");
    add(table, "à");
    add(table, "À");
    add(table, "ă");
    add(table, "Ă");
    add(table, "â");
    add(table, "Â");
    add(table, "ǎ");
    add(table, "Ǎ");
    add(table, "å");
    add(table, "Å");
    add(table, "ǻ");
    add(table, "Ǻ");
    add(table, "ä");
    add(table, "Ä");
    add(table, "ǟ");
    add(table, "Ǟ");
    add(table, "ã");
    add(table, "Ã");
    add(table, "ȧ");
    add(table, "Ȧ");
    add(table, "ǡ");
    add(table, "Ǡ");
    add(table, "ą");
    add(table, "Ą");
    add(table, "ā");
    add(table, "Ā");
    add(table, "ȁ");
    add(table, "Ȁ");
    add(table, "ȃ");
    add(table, "Ȃ");
    add(table, "æ");
    add(table, "Æ");
    add(table, "ǽ");
    add(table, "Ǽ");
    add(table, "ǣ");
    add(table, "Ǣ");
    add(table, "Ⱥ");
    add(table, "b");
    add(table, "B");
    add(table, "ƀ");
    add(table, "Ƀ");
    add(table, "Ɓ");
    add(table, "ƃ");
    add(table, "Ƃ");
    add(table, "c");
    add(table, "C");
    add(table, "ć");
    add(table, "Ć");
    add(table, "ĉ");
    add(table, "Ĉ");
    add(table, "č");
    add(table, "Č");
    add(table, "ċ");
    add(table, "Ċ");
    add(table, "ç");
    add(table, "Ç");
    add(table, "ȼ");
    add(table, "Ȼ");
    add(table, "ƈ");
    add(table, "Ƈ");
    add(table, "d");
    add(table, "D");
    add(table, "ď");
    add(table, "Ď");
    add(table, "đ");
    add(table, "Đ");
    add(table, "ð");
    add(table, "Ð");
    add(table, "ȸ");
    add(table, "ǳ");
    add(table, "ǲ");
    add(table, "Ǳ");
    add(table, "ǆ");
    add(table, "ǅ");
    add(table, "Ǆ");
    add(table, "Ɖ");
    add(table, "Ɗ");
    add(table, "ƌ");
    add(table, "Ƌ");
    add(table, "ȡ");
    add(table, "e");
    add(table, "E");
    add(table, "é");
    add(table, "É");
    add(table, "è");
    add(table, "È");
    add(table, "ĕ");
    add(table, "Ĕ");
    add(table, "ê");
    add(table, "Ê");
    add(table, "ě");
    add(table, "Ě");
    add(table, "ë");
    add(table, "Ë");
    add(table, "ė");
    add(table, "Ė");
    add(table, "ȩ");
    add(table, "Ȩ");
    add(table, "ę");
    add(table, "Ę");
    add(table, "ē");
    add(table, "Ē");
    add(table, "ȅ");
    add(table, "Ȅ");
    add(table, "ȇ");
    add(table, "Ȇ");
    add(table, "ɇ");
    add(table, "Ɇ");
    add(table, "ǝ");
    add(table, "Ǝ");
    add(table, "Ə");
    add(table, "Ɛ");
    add(table, "f");
    add(table, "F");
    add(table, "ƒ");
    add(table, "Ƒ");
    add(table, "g");
    add(table, "G");
    add(table, "ǵ");
    add(table, "Ǵ");
    add(table, "ğ");
    add(table, "Ğ");
    add(table, "ĝ");
    add(table, "Ĝ");
    add(table, "ǧ");
    add(table, "Ǧ");
    add(table, "ġ");
    add(table, "Ġ");
    add(table, "ģ");
    add(table, "Ģ");
    add(table, "ǥ");
    add(table, "Ǥ");
    add(table, "Ɠ");
    add(table, "Ɣ");
    add(table, "ƣ");
    add(table, "Ƣ");
    add(table, "h");
    add(table, "H");
    add(table, "ĥ");
    add(table, "Ĥ");
    add(table, "ȟ");
    add(table, "Ȟ");
    add(table, "ħ");
    add(table, "Ħ");
    add(table, "ƕ");
    add(table, "Ƕ");
    add(table, "i");
    add(table, "I");
    add(table, "í");
    add(table, "Í");
    add(table, "ì");
    add(table, "Ì");
    add(table, "ĭ");
    add(table, "Ĭ");
    add(table, "î");
    add(table, "Î");
    add(table, "ǐ");
    add(table, "Ǐ");
    add(table, "ï");
    add(table, "Ï");
    add(table, "ĩ");
    add(table, "Ĩ");
    add(table, "İ");
    add(table, "į");
    add(table, "Į");
    add(table, "ī");
    add(table, "Ī");
    add(table, "ȉ");
    add(table, "Ȉ");
    add(table, "ȋ");
    add(table, "Ȋ");
    add(table, "ĳ");
    add(table, "Ĳ");
    add(table, "ı");
    add(table, "Ɨ");
    add(table, "Ɩ");
    add(table, "j");
    add(table, "J");
    add(table, "ĵ");
    add(table, "Ĵ");
    add(table, "ǰ");
    add(table, "ȷ");
    add(table, "ɉ");
    add(table, "Ɉ");
    add(table, "k");
    add(table, "K");
    add(table, "ǩ");
    add(table, "Ǩ");
    add(table, "ķ");
    add(table, "Ķ");
    add(table, "ƙ");
    add(table, "Ƙ");
    add(table, "ĺ");
    add(table, "Ĺ");
    add(table, "ľ");
    add(table, "Ľ");
    add(table, "ļ");
    add(table, "Ļ");
    add(table, "ł");
    add(table, "Ł");
    add(table, "ŀ");
    add(table, "l");
    add(table, "Ŀ");
    add(table, "L");
    add(table, "ǉ");
    add(table, "ǈ");
    add(table, "Ǉ");
    add(table, "ƚ");
    add(table, "Ƚ");
    add(table, "ȴ");
    add(table, "ƛ");
    add(table, "m");
    add(table, "M");
    add(table, "n");
    add(table, "N");
    add(table, "ń");
    add(table, "Ń");
    add(table, "ǹ");
    add(table, "Ǹ");
    add(table, "ň");
    add(table, "Ň");
    add(table, "ñ");
    add(table, "Ñ");
    add(table, "ņ");
    add(table, "Ņ");
    add(table, "ǌ");
    add(table, "ǋ");
    add(table, "Ǌ");
    add(table, "Ɲ");
    add(table, "ƞ");
    add(table, "Ƞ");
    add(table, "ȵ");
    add(table, "ŋ");
    add(table, "Ŋ");
    add(table, "o");
    add(table, "O");
    add(table, "º");
    add(table, "ó");
    add(table, "Ó");
    add(table, "ò");
    add(table, "Ò");
    add(table, "ŏ");
    add(table, "Ŏ");
    add(table, "ô");
    add(table, "Ô");
    add(table, "ǒ");
    add(table, "Ǒ");
    add(table, "ö");
    add(table, "Ö");
    add(table, "ȫ");
    add(table, "Ȫ");
    add(table, "ő");
    add(table, "Ő");
    add(table, "õ");
    add(table, "Õ");
    add(table, "ȭ");
    add(table, "Ȭ");
    add(table, "ȯ");
    add(table, "Ȯ");
    add(table, "ȱ");
    add(table, "Ȱ");
    add(table, "ø");
    add(table, "Ø");
    add(table, "ǿ");
    add(table, "Ǿ");
    add(table, "ǫ");
    add(table, "Ǫ");
    add(table, "ǭ");
    add(table, "Ǭ");
    add(table, "ō");
    add(table, "Ō");
    add(table, "ȍ");
    add(table, "Ȍ");
    add(table, "ȏ");
    add(table, "Ȏ");
    add(table, "ơ");
    add(table, "Ơ");
    add(table, "œ");
    add(table, "Œ");
    add(table, "Ɔ");
    add(table, "Ɵ");
    add(table, "ȣ");
    add(table, "Ȣ");
    add(table, "p");
    add(table, "P");
    add(table, "ƥ");
    add(table, "Ƥ");
    add(table, "q");
    add(table, "Q");
    add(table, "ȹ");
    add(table, "ɋ");
    add(table, "Ɋ");
    add(table, "ĸ");
    add(table, "r");
    add(table, "R");
    add(table, "ŕ");
    add(table, "Ŕ");
    add(table, "ř");
    add(table, "Ř");
    add(table, "ŗ");
    add(table, "Ŗ");
    add(table, "ȑ");
    add(table, "Ȑ");
    add(table, "ȓ");
    add(table, "Ȓ");
    add(table, "Ʀ");
    add(table, "ɍ");
    add(table, "Ɍ");
    add(table, "s");
    add(table, "S");
    add(table, "ś");
    add(table, "Ś");
    add(table, "ŝ");
    add(table, "Ŝ");
    add(table, "š");
    add(table, "Š");
    add(table, "ş");
    add(table, "Ş");
    add(table, "ș");
    add(table, "Ș");
    add(table, "ſ");
    add(table, "ß");
    add(table, "ȿ");
    add(table, "Ʃ");
    add(table, "ƪ");
    add(table, "t");
    add(table, "T");
    add(table, "ť");
    add(table, "Ť");
    add(table, "ţ");
    add(table, "Ţ");
    add(table, "ț");
    add(table, "Ț");
    add(table, "ƾ");
    add(table, "ŧ");
    add(table, "Ŧ");
    add(table, "Ⱦ");
    add(table, "ƫ");
    add(table, "ƭ");
    add(table, "Ƭ");
    add(table, "Ʈ");
    add(table, "ȶ");
    add(table, "u");
    add(table, "U");
    add(table, "ú");
    add(table, "Ú");
    add(table, "ù");
    add(table, "Ù");
    add(table, "ŭ");
    add(table, "Ŭ");
    add(table, "û");
    add(table, "Û");
    add(table, "ǔ");
    add(table, "Ǔ");
    add(table, "ů");
    add(table, "Ů");
    add(table, "ü");
    add(table, "Ü");
    add(table, "ǘ");
    add(table, "Ǘ");
    add(table, "ǜ");
    add(table, "Ǜ");
    add(table, "ǚ");
    add(table, "Ǚ");
    add(table, "ǖ");
    add(table, "Ǖ");
    add(table, "ű");
    add(table, "Ű");
    add(table, "ũ");
    add(table, "Ũ");
    add(table, "ų");
    add(table, "Ų");
    add(table, "ū");
    add(table, "Ū");
    add(table, "ȕ");
    add(table, "Ȕ");
    add(table, "ȗ");
    add(table, "Ȗ");
    add(table, "ư");
    add(table, "Ư");
    add(table, "Ʉ");
    add(table, "Ɯ");
    add(table, "Ʊ");
    add(table, "v");
    add(table, "V");
    add(table, "Ʋ");
    add(table, "Ʌ");
    add(table, "w");
    add(table, "W");
    add(table, "ŵ");
    add(table, "Ŵ");
    add(table, "x");
    add(table, "X");
    add(table, "y");
    add(table, "Y");
    add(table, "ý");
    add(table, "Ý");
    add(table, "ŷ");
    add(table, "Ŷ");
    add(table, "ÿ");
    add(table, "Ÿ");
    add(table, "ȳ");
    add(table, "Ȳ");
    add(table, "ɏ");
    add(table, "Ɏ");
    add(table, "ƴ");
    add(table, "Ƴ");
    add(table, "ȝ");
    add(table, "Ȝ");
    add(table, "z");
    add(table, "Z");
    add(table, "ź");
    add(table, "Ź");
    add(table, "ž");
    add(table, "Ž");
    add(table, "ż");
    add(table, "Ż");
    add(table, "ƍ");
    add(table, "ƶ");
    add(table, "Ƶ");
    add(table, "ȥ");
    add(table, "Ȥ");
    add(table, "ɀ");
    add(table, "Ʒ");
    add(table, "ǯ");
    add(table, "Ǯ");
    add(table, "ƹ");
    add(table, "Ƹ");
    add(table, "ƺ");
    add(table, "þ");
    add(table, "Þ");
    add(table, "ƿ");
    add(table, "Ƿ");
    add(table, "ƻ");
    add(table, "ƨ");
    add(table, "Ƨ");
    add(table, "ƽ");
    add(table, "Ƽ");
    add(table, "ƅ");
    add(table, "Ƅ");
    add(table, "ɂ");
    add(table, "Ɂ");
    add(table, "ŉ");
    add(table, "ǀ");
    add(table, "ǁ");
    add(table, "ǂ");
    add(table, "ǃ");
    add(table, "µ");

    // Core-only is default comparer
    TableView v1 = table.where().find_all();
    TableView v2 = table.where().find_all();

    v2.sort(0);

    for (size_t t = 0; t < v1.size(); t++) {
        CHECK_EQUAL(v1.get_source_ndx(t), v2.get_source_ndx(t));
    }

    // Set back to default in case other tests rely on this
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


NONCONCURRENT_TEST(TableView_SortOrder_Core)
{
    TestTable table;
    table.add_column(type_String, "1");

    // This tests the expected sorting order with STRING_COMPARE_CORE. See utf8_compare() in unicode.cpp. Only
    // characters
    // that have a visual representation are tested (control characters such as line feed are omitted).
    //
    // NOTE: Your editor must assume that Core source code is in utf8, and it must save as utf8, else this unit
    // test will fail.

    set_string_compare_method(STRING_COMPARE_CORE, nullptr);

    add(table, "'");
    add(table, "-");
    add(table, " ");
    add(table, " ");
    add(table, "!");
    add(table, "\"");
    add(table, "#");
    add(table, "$");
    add(table, "%");
    add(table, "&");
    add(table, "(");
    add(table, ")");
    add(table, "*");
    add(table, ",");
    add(table, ".");
    add(table, "/");
    add(table, ":");
    add(table, ";");
    add(table, "?");
    add(table, "@");
    add(table, "[");
    add(table, "\\");
    add(table, "^");
    add(table, "_");
    add(table, "`");
    add(table, "{");
    add(table, "|");
    add(table, "}");
    add(table, "~");
    add(table, "¡");
    add(table, "¦");
    add(table, "¨");
    add(table, "¯");
    add(table, "´");
    add(table, "¸");
    add(table, "¿");
    add(table, "ǃ");
    add(table, "¢");
    add(table, "£");
    add(table, "¤");
    add(table, "¥");
    add(table, "+");
    add(table, "<");
    add(table, "=");
    add(table, ">");
    add(table, "±");
    add(table, "«");
    add(table, "»");
    add(table, "×");
    add(table, "÷");
    add(table, "ǀ");
    add(table, "ǁ");
    add(table, "ǂ");
    add(table, "§");
    add(table, "©");
    add(table, "¬");
    add(table, "®");
    add(table, "°");
    add(table, "µ");
    add(table, "¶");
    add(table, "·");
    add(table, "0");
    add(table, "¼");
    add(table, "½");
    add(table, "¾");
    add(table, "1");
    add(table, "¹");
    add(table, "2");
    add(table, "ƻ");
    add(table, "²");
    add(table, "3");
    add(table, "³");
    add(table, "4");
    add(table, "5");
    add(table, "ƽ");
    add(table, "Ƽ");
    add(table, "6");
    add(table, "7");
    add(table, "8");
    add(table, "9");
    add(table, "a");
    add(table, "A");
    add(table, "ª");
    add(table, "á");
    add(table, "Á");
    add(table, "à");
    add(table, "À");
    add(table, "ȧ");
    add(table, "Ȧ");
    add(table, "â");
    add(table, "Â");
    add(table, "ǎ");
    add(table, "Ǎ");
    add(table, "ă");
    add(table, "Ă");
    add(table, "ā");
    add(table, "Ā");
    add(table, "ã");
    add(table, "Ã");
    add(table, "ą");
    add(table, "Ą");
    add(table, "Ⱥ");
    add(table, "ǡ");
    add(table, "Ǡ");
    add(table, "ǻ");
    add(table, "Ǻ");
    add(table, "ǟ");
    add(table, "Ǟ");
    add(table, "ȁ");
    add(table, "Ȁ");
    add(table, "ȃ");
    add(table, "Ȃ");
    add(table, "ǽ");
    add(table, "Ǽ");
    add(table, "b");
    add(table, "B");
    add(table, "ƀ");
    add(table, "Ƀ");
    add(table, "Ɓ");
    add(table, "ƃ");
    add(table, "Ƃ");
    add(table, "ƅ");
    add(table, "Ƅ");
    add(table, "c");
    add(table, "C");
    add(table, "ć");
    add(table, "Ć");
    add(table, "ċ");
    add(table, "Ċ");
    add(table, "ĉ");
    add(table, "Ĉ");
    add(table, "č");
    add(table, "Č");
    add(table, "ç");
    add(table, "Ç");
    add(table, "ȼ");
    add(table, "Ȼ");
    add(table, "ƈ");
    add(table, "Ƈ");
    add(table, "Ɔ");
    add(table, "d");
    add(table, "D");
    add(table, "ď");
    add(table, "Ď");
    add(table, "đ");
    add(table, "Đ");
    add(table, "ƌ");
    add(table, "Ƌ");
    add(table, "Ɗ");
    add(table, "ð");
    add(table, "Ð");
    add(table, "ƍ");
    add(table, "ȸ");
    add(table, "ǳ");
    add(table, "ǲ");
    add(table, "Ǳ");
    add(table, "ǆ");
    add(table, "ǅ");
    add(table, "Ǆ");
    add(table, "Ɖ");
    add(table, "ȡ");
    add(table, "e");
    add(table, "E");
    add(table, "é");
    add(table, "É");
    add(table, "è");
    add(table, "È");
    add(table, "ė");
    add(table, "Ė");
    add(table, "ê");
    add(table, "Ê");
    add(table, "ë");
    add(table, "Ë");
    add(table, "ě");
    add(table, "Ě");
    add(table, "ĕ");
    add(table, "Ĕ");
    add(table, "ē");
    add(table, "Ē");
    add(table, "ę");
    add(table, "Ę");
    add(table, "ȩ");
    add(table, "Ȩ");
    add(table, "ɇ");
    add(table, "Ɇ");
    add(table, "ȅ");
    add(table, "Ȅ");
    add(table, "ȇ");
    add(table, "Ȇ");
    add(table, "ǝ");
    add(table, "Ǝ");
    add(table, "Ə");
    add(table, "Ɛ");
    add(table, "ȝ");
    add(table, "Ȝ");
    add(table, "f");
    add(table, "F");
    add(table, "ƒ");
    add(table, "Ƒ");
    add(table, "g");
    add(table, "G");
    add(table, "ǵ");
    add(table, "Ǵ");
    add(table, "ġ");
    add(table, "Ġ");
    add(table, "ĝ");
    add(table, "Ĝ");
    add(table, "ǧ");
    add(table, "Ǧ");
    add(table, "ğ");
    add(table, "Ğ");
    add(table, "ģ");
    add(table, "Ģ");
    add(table, "ǥ");
    add(table, "Ǥ");
    add(table, "Ɠ");
    add(table, "Ɣ");
    add(table, "h");
    add(table, "H");
    add(table, "ĥ");
    add(table, "Ĥ");
    add(table, "ȟ");
    add(table, "Ȟ");
    add(table, "ħ");
    add(table, "Ħ");
    add(table, "ƕ");
    add(table, "Ƕ");
    add(table, "i");
    add(table, "I");
    add(table, "ı");
    add(table, "í");
    add(table, "Í");
    add(table, "ì");
    add(table, "Ì");
    add(table, "İ");
    add(table, "î");
    add(table, "Î");
    add(table, "ï");
    add(table, "Ï");
    add(table, "ǐ");
    add(table, "Ǐ");
    add(table, "ĭ");
    add(table, "Ĭ");
    add(table, "ī");
    add(table, "Ī");
    add(table, "ĩ");
    add(table, "Ĩ");
    add(table, "į");
    add(table, "Į");
    add(table, "Ɨ");
    add(table, "ȉ");
    add(table, "Ȉ");
    add(table, "ȋ");
    add(table, "Ȋ");
    add(table, "Ɩ");
    add(table, "ĳ");
    add(table, "Ĳ");
    add(table, "j");
    add(table, "J");
    add(table, "ȷ");
    add(table, "ĵ");
    add(table, "Ĵ");
    add(table, "ǰ");
    add(table, "ɉ");
    add(table, "Ɉ");
    add(table, "k");
    add(table, "K");
    add(table, "ǩ");
    add(table, "Ǩ");
    add(table, "ķ");
    add(table, "Ķ");
    add(table, "ƙ");
    add(table, "Ƙ");
    add(table, "l");
    add(table, "L");
    add(table, "ĺ");
    add(table, "Ĺ");
    add(table, "ŀ");
    add(table, "Ŀ");
    add(table, "ľ");
    add(table, "Ľ");
    add(table, "ļ");
    add(table, "Ļ");
    add(table, "ƚ");
    add(table, "Ƚ");
    add(table, "ł");
    add(table, "Ł");
    add(table, "ƛ");
    add(table, "ǉ");
    add(table, "ǈ");
    add(table, "Ǉ");
    add(table, "ȴ");
    add(table, "m");
    add(table, "M");
    add(table, "Ɯ");
    add(table, "n");
    add(table, "N");
    add(table, "ń");
    add(table, "Ń");
    add(table, "ǹ");
    add(table, "Ǹ");
    add(table, "ň");
    add(table, "Ň");
    add(table, "ñ");
    add(table, "Ñ");
    add(table, "ņ");
    add(table, "Ņ");
    add(table, "Ɲ");
    add(table, "ŉ");
    add(table, "ƞ");
    add(table, "Ƞ");
    add(table, "ǌ");
    add(table, "ǋ");
    add(table, "Ǌ");
    add(table, "ȵ");
    add(table, "ŋ");
    add(table, "Ŋ");
    add(table, "o");
    add(table, "O");
    add(table, "º");
    add(table, "ó");
    add(table, "Ó");
    add(table, "ò");
    add(table, "Ò");
    add(table, "ȯ");
    add(table, "Ȯ");
    add(table, "ô");
    add(table, "Ô");
    add(table, "ǒ");
    add(table, "Ǒ");
    add(table, "ŏ");
    add(table, "Ŏ");
    add(table, "ō");
    add(table, "Ō");
    add(table, "õ");
    add(table, "Õ");
    add(table, "ǫ");
    add(table, "Ǫ");
    add(table, "Ɵ");
    add(table, "ȱ");
    add(table, "Ȱ");
    add(table, "ȫ");
    add(table, "Ȫ");
    add(table, "ǿ");
    add(table, "Ǿ");
    add(table, "ȭ");
    add(table, "Ȭ");
    add(table, "ǭ");
    add(table, "Ǭ");
    add(table, "ȍ");
    add(table, "Ȍ");
    add(table, "ȏ");
    add(table, "Ȏ");
    add(table, "ơ");
    add(table, "Ơ");
    add(table, "ƣ");
    add(table, "Ƣ");
    add(table, "œ");
    add(table, "Œ");
    add(table, "ȣ");
    add(table, "Ȣ");
    add(table, "p");
    add(table, "P");
    add(table, "ƥ");
    add(table, "Ƥ");
    add(table, "q");
    add(table, "Q");
    add(table, "ĸ");
    add(table, "ɋ");
    add(table, "Ɋ");
    add(table, "ȹ");
    add(table, "r");
    add(table, "R");
    add(table, "Ʀ");
    add(table, "ŕ");
    add(table, "Ŕ");
    add(table, "ř");
    add(table, "Ř");
    add(table, "ŗ");
    add(table, "Ŗ");
    add(table, "ɍ");
    add(table, "Ɍ");
    add(table, "ȑ");
    add(table, "Ȑ");
    add(table, "ȓ");
    add(table, "Ȓ");
    add(table, "s");
    add(table, "S");
    add(table, "ś");
    add(table, "Ś");
    add(table, "ŝ");
    add(table, "Ŝ");
    add(table, "š");
    add(table, "Š");
    add(table, "ş");
    add(table, "Ş");
    add(table, "ș");
    add(table, "Ș");
    add(table, "ȿ");
    add(table, "Ʃ");
    add(table, "ƨ");
    add(table, "Ƨ");
    add(table, "ƪ");
    add(table, "ß");
    add(table, "ſ");
    add(table, "t");
    add(table, "T");
    add(table, "ť");
    add(table, "Ť");
    add(table, "ţ");
    add(table, "Ţ");
    add(table, "ƭ");
    add(table, "Ƭ");
    add(table, "ƫ");
    add(table, "Ʈ");
    add(table, "ț");
    add(table, "Ț");
    add(table, "Ⱦ");
    add(table, "ȶ");
    add(table, "þ");
    add(table, "Þ");
    add(table, "ŧ");
    add(table, "Ŧ");
    add(table, "u");
    add(table, "U");
    add(table, "ú");
    add(table, "Ú");
    add(table, "ù");
    add(table, "Ù");
    add(table, "û");
    add(table, "Û");
    add(table, "ǔ");
    add(table, "Ǔ");
    add(table, "ŭ");
    add(table, "Ŭ");
    add(table, "ū");
    add(table, "Ū");
    add(table, "ũ");
    add(table, "Ũ");
    add(table, "ů");
    add(table, "Ů");
    add(table, "ų");
    add(table, "Ų");
    add(table, "Ʉ");
    add(table, "ǘ");
    add(table, "Ǘ");
    add(table, "ǜ");
    add(table, "Ǜ");
    add(table, "ǚ");
    add(table, "Ǚ");
    add(table, "ǖ");
    add(table, "Ǖ");
    add(table, "ȕ");
    add(table, "Ȕ");
    add(table, "ȗ");
    add(table, "Ȗ");
    add(table, "ư");
    add(table, "Ư");
    add(table, "Ʊ");
    add(table, "v");
    add(table, "V");
    add(table, "Ʋ");
    add(table, "Ʌ");
    add(table, "w");
    add(table, "W");
    add(table, "ŵ");
    add(table, "Ŵ");
    add(table, "ƿ");
    add(table, "Ƿ");
    add(table, "x");
    add(table, "X");
    add(table, "y");
    add(table, "Y");
    add(table, "ý");
    add(table, "Ý");
    add(table, "ŷ");
    add(table, "Ŷ");
    add(table, "ÿ");
    add(table, "Ÿ");
    add(table, "ȳ");
    add(table, "Ȳ");
    add(table, "ű");
    add(table, "Ű");
    add(table, "ɏ");
    add(table, "Ɏ");
    add(table, "ƴ");
    add(table, "Ƴ");
    add(table, "ü");
    add(table, "Ü");
    add(table, "z");
    add(table, "Z");
    add(table, "ź");
    add(table, "Ź");
    add(table, "ż");
    add(table, "Ż");
    add(table, "ž");
    add(table, "Ž");
    add(table, "ƶ");
    add(table, "Ƶ");
    add(table, "ȥ");
    add(table, "Ȥ");
    add(table, "ɀ");
    add(table, "æ");
    add(table, "Æ");
    add(table, "Ʒ");
    add(table, "ǣ");
    add(table, "Ǣ");
    add(table, "ä");
    add(table, "Ä");
    add(table, "ǯ");
    add(table, "Ǯ");
    add(table, "ƹ");
    add(table, "Ƹ");
    add(table, "ƺ");
    add(table, "ø");
    add(table, "Ø");
    add(table, "ö");
    add(table, "Ö");
    add(table, "ő");
    add(table, "Ő");
    add(table, "å");
    add(table, "Å");
    add(table, "ƾ");
    add(table, "ɂ");
    add(table, "Ɂ");

    // Core-only is default comparer
    TableView v1 = table.where().find_all();
    TableView v2 = table.where().find_all();

    v2.sort(0);

    for (size_t t = 0; t < v1.size(); t++) {
        CHECK_EQUAL(v1.get_source_ndx(t), v2.get_source_ndx(t));
    }

    // Set back to default in case other tests rely on this
    set_string_compare_method(STRING_COMPARE_CORE, nullptr);
}


// Verify that copy-constructed and copy-assigned TableViews work normally.
TEST(TableView_Copy)
{
    Table table;
    size_t col_id = table.add_column(type_Int, "id");
    for (size_t i = 0; i < 3; ++i)
        table.set_int(col_id, table.add_empty_row(), i);

    TableView tv = (table.column<Int>(col_id) > 0).find_all();
    CHECK_EQUAL(2, tv.size());

    TableView copy_1(tv);
    TableView copy_2;
    copy_2 = tv;

    CHECK_EQUAL(2, copy_1.size());
    CHECK_EQUAL(1, copy_1.get_source_ndx(0));
    CHECK_EQUAL(2, copy_1.get_source_ndx(1));

    CHECK_EQUAL(2, copy_2.size());
    CHECK_EQUAL(1, copy_2.get_source_ndx(0));
    CHECK_EQUAL(2, copy_2.get_source_ndx(1));

    table.move_last_over(1);

    CHECK(!copy_1.is_in_sync());
    CHECK(!copy_2.is_in_sync());

    copy_1.sync_if_needed();
    CHECK_EQUAL(1, copy_1.size());
    CHECK_EQUAL(1, copy_1.get_source_ndx(0));

    copy_2.sync_if_needed();
    CHECK_EQUAL(1, copy_2.size());
    CHECK_EQUAL(1, copy_2.get_source_ndx(0));
}

TEST(TableView_InsertColumnsAfterSort)
{
    Table table;
    table.add_column(type_Int, "value");
    table.add_empty_row(10);
    for (size_t i = 0; i < 10; ++i)
        table.set_int(0, i, i);

    SortDescriptor desc(table, {{0}}, {false}); // sort by the one column in descending order

    table.insert_column(0, type_String, "0");
    auto tv = table.get_sorted_view(desc);
    CHECK_EQUAL(tv.get_int(1, 0), 9);
    CHECK_EQUAL(tv.get_int(1, 9), 0);

    table.insert_column(0, type_String, "1");
    table.add_empty_row();
    tv.sync_if_needed();
    CHECK_EQUAL(tv.get_int(2, 0), 9);
    CHECK_EQUAL(tv.get_int(2, 10), 0);
}

TEST(TableView_TimestampMaxRemoveRow)
{
    Table table;
    table.add_column(type_Timestamp, "time");
    for (size_t i = 0; i < 10; ++i) {
        table.add_empty_row();
        table.set_timestamp(0, i, Timestamp(i, 0));
    }

    TableView tv = table.where().find_all();
    CHECK_EQUAL(tv.size(), 10);
    CHECK_EQUAL(tv.maximum_timestamp(0), Timestamp(9, 0));

    table.move_last_over(9);
    CHECK_EQUAL(tv.size(), 10);                            // not changed since sync_if_needed hasn't been called
    CHECK_EQUAL(tv.maximum_timestamp(0), Timestamp(8, 0)); // but aggregate functions skip removed rows

    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 9);
    CHECK_EQUAL(tv.maximum_timestamp(0), Timestamp(8, 0));
}

#endif // TEST_TABLE_VIEW
