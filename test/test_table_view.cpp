#include "testsettings.hpp"
#ifdef TEST_TABLE_VIEW

#include <limits>
#include <string>
#include <sstream>
#include <ostream>

#include <tightdb/table_macros.hpp>

#include "util/misc.hpp"

#include "test.hpp"

using namespace std;
using namespace tightdb;
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


namespace {

TIGHTDB_TABLE_1(TestTableInt,
                first, Int)

TIGHTDB_TABLE_2(TestTableInt2,
                first,  Int,
                second, Int)

TIGHTDB_TABLE_2(TestTableDate,
                first, DateTime,
                second, Int)

TIGHTDB_TABLE_2(TestTableFloatDouble,
                first, Float,
                second, Double)


} // anonymous namespace


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


TEST(TableView_DateMaxMin)
{
    TestTableDate ttd;

    ttd.add(DateTime(2014, 7, 10), 1);
    ttd.add(DateTime(2013, 7, 10), 1);
    ttd.add(DateTime(2015, 8, 10), 1);
    ttd.add(DateTime(2015, 7, 10), 1);

    TestTableDate::View v = ttd.column().second.find_all(1);
    size_t ndx = not_found;

    CHECK_EQUAL(DateTime(2015, 8, 10), v.column().first.maximum(&ndx));
    CHECK_EQUAL(2, ndx);

    CHECK_EQUAL(DateTime(2013, 7, 10), v.column().first.minimum(&ndx));
    CHECK_EQUAL(1, ndx);
}

TEST(TableView_GetSetInteger)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(3);
    table.add(1);
    table.add(2);

    TestTableInt::View v; // Test empty construction
    v = table.column().first.find_all(2); // Test assignment

    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2, v[0].first);
    CHECK_EQUAL(2, v[1].first);

    // Test of Set
    v[0].first = 123;
    CHECK_EQUAL(123, v[0].first);
}



namespace {
TIGHTDB_TABLE_3(TableFloats,
                col_float, Float,
                col_double, Double,
                col_int, Int)
}

TEST(TableView_FloatsGetSet)
{
    TableFloats table;

    float  f_val[] = { 1.1f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f };
    double d_val[] = { 1.2 , 2.2 , 3.2 , -1.2 , 2.3, 0.0  };

    CHECK_EQUAL(true, table.is_empty());

    // Test add(?,?) with parameters
    for (size_t i=0; i<5; ++i)
        table.add(f_val[i], d_val[i], i);
    table.add();
    CHECK_EQUAL(6, table.size());
    for (size_t i=0; i<6; ++i) {
        CHECK_EQUAL(f_val[i], table.column().col_float[i]);
        CHECK_EQUAL(d_val[i], table.column().col_double[i]);
    }

    TableFloats::View v; // Test empty construction
    v = table.column().col_float.find_all(2.1f); // Test assignment
    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2.1f, v[0].col_float);
    CHECK_EQUAL(2.1f, v[1].col_float);
    CHECK_EQUAL(2.2, v[0].col_double);
    CHECK_EQUAL(2.3, v[1].col_double);

    // Test of Set
    v[0].col_float = 123.321f;
    CHECK_EQUAL(123.321f, v[0].col_float);
    v[0].col_double = 123.3219;
    CHECK_EQUAL(123.3219, v[0].col_double);
}

TEST(TableView_FloatsFindAndAggregations)
{
    TableFloats table;
    float  f_val[] = { 1.2f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f };
    double d_val[] = { -1.2, 2.2 , 3.2 , -1.2 , 2.3 , 0.0  };
    // v_some =        ^^^^              ^^^^
    double sum_f = 0.0;
    double sum_d = 0.0;
    for (size_t i=0; i<6; ++i) {
        table.add(f_val[i], d_val[i], 1);
        sum_d += d_val[i];
        sum_f += f_val[i];
    }

    // Test find_all()
    TableFloats::View v_all = table.column().col_int.find_all(1);
    CHECK_EQUAL(6, v_all.size());

    TableFloats::View v_some = table.column().col_double.find_all(-1.2);
    CHECK_EQUAL(2, v_some.size());
    CHECK_EQUAL(0, v_some.get_source_ndx(0));
    CHECK_EQUAL(3, v_some.get_source_ndx(1));

    // Test find_first
    CHECK_EQUAL(0, v_all.column().col_double.find_first(-1.2) );
    CHECK_EQUAL(5, v_all.column().col_double.find_first(0.0) );
    CHECK_EQUAL(2, v_all.column().col_double.find_first(3.2) );

    CHECK_EQUAL(1, v_all.column().col_float.find_first(2.1f) );
    CHECK_EQUAL(5, v_all.column().col_float.find_first(0.0f) );
    CHECK_EQUAL(2, v_all.column().col_float.find_first(3.1f) );

    // TODO: add for float as well

    double epsilon = numeric_limits<double>::epsilon();

    // Test sum
    CHECK_APPROXIMATELY_EQUAL(sum_d,
                              v_all.column().col_double.sum(),  10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f,
                              v_all.column().col_float.sum(),   10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(-1.2 + -1.2,
                              v_some.column().col_double.sum(), 10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(double(1.2f) + double(-1.1f),
                              v_some.column().col_float.sum(),  10*epsilon);

    size_t ndx = not_found;

    // Test max
    CHECK_EQUAL(3.2, v_all.column().col_double.maximum(&ndx));
    CHECK_EQUAL(2, ndx);

    CHECK_EQUAL(-1.2, v_some.column().col_double.maximum(&ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(3.1f, v_all.column().col_float.maximum(&ndx));
    CHECK_EQUAL(2, ndx);

    CHECK_EQUAL(1.2f, v_some.column().col_float.maximum(&ndx));
    CHECK_EQUAL(0, ndx);

    // Max without ret_index
    CHECK_EQUAL(3.2, v_all.column().col_double.maximum());
    CHECK_EQUAL(-1.2, v_some.column().col_double.maximum());
    CHECK_EQUAL(3.1f, v_all.column().col_float.maximum());
    CHECK_EQUAL(1.2f, v_some.column().col_float.maximum());

    // Test min
    CHECK_EQUAL(-1.2, v_all.column().col_double.minimum());
    CHECK_EQUAL(-1.2, v_some.column().col_double.minimum());
    CHECK_EQUAL(-1.1f, v_all.column().col_float.minimum());
    CHECK_EQUAL(-1.1f, v_some.column().col_float.minimum());

    // min with ret_ndx
    CHECK_EQUAL(-1.2, v_all.column().col_double.minimum(&ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(-1.2, v_some.column().col_double.minimum(&ndx));
    CHECK_EQUAL(0, ndx);

    CHECK_EQUAL(-1.1f, v_all.column().col_float.minimum(&ndx));
    CHECK_EQUAL(3, ndx);

    CHECK_EQUAL(-1.1f, v_some.column().col_float.minimum(&ndx));
    CHECK_EQUAL(1, ndx);

    // Test avg
    CHECK_APPROXIMATELY_EQUAL(sum_d / 6.0,
                              v_all.column().col_double.average(),  10*epsilon);
    CHECK_APPROXIMATELY_EQUAL((-1.2 + -1.2) / 2.0,
                              v_some.column().col_double.average(), 10*epsilon);
    CHECK_APPROXIMATELY_EQUAL(sum_f / 6.0,
                              v_all.column().col_float.average(),   10*epsilon);
    CHECK_APPROXIMATELY_EQUAL((double(1.2f) + double(-1.1f)) / 2,
                              v_some.column().col_float.average(), 10*epsilon);

    CHECK_EQUAL(1, v_some.column().col_float.count(1.2f));
    CHECK_EQUAL(2, v_some.column().col_double.count(-1.2));
    CHECK_EQUAL(2, v_some.column().col_int.count(1));

    CHECK_EQUAL(2, v_all.column().col_float.count(2.1f));
    CHECK_EQUAL(2, v_all.column().col_double.count(-1.2));
    CHECK_EQUAL(6, v_all.column().col_int.count(1));
}

TEST(TableView_Sum)
{
    TestTableInt table;

    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);

    TestTableInt::View v = table.column().first.find_all(2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.column().first.sum();
    CHECK_EQUAL(10, sum);
}

TEST(TableView_SumNegative)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = 11;
    v[2].first = -20;

    int64_t sum = v.column().first.sum();
    CHECK_EQUAL(-9, sum);
}

TEST(TableView_IsAttached)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    TestTableInt::View v2 = table.column().first.find_all(0);
    v[0].first = 11;
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
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t max = v.column().first.maximum();
    CHECK_EQUAL(2, max);
}

TEST(TableView_Max2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t max = v.column().first.maximum();
    CHECK_EQUAL(-1, max);
}


TEST(TableView_Min)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t min = v.column().first.minimum();
    CHECK_EQUAL(-1, min);

    size_t ndx = not_found;
    min = v.column().first.minimum(&ndx);
    CHECK_EQUAL(-1, min);
    CHECK_EQUAL(0, ndx);
}

TEST(TableView_Min2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t min = v.column().first.minimum();
    CHECK_EQUAL(-3, min);

    size_t ndx = not_found;
    min = v.column().first.minimum(&ndx);
    CHECK_EQUAL(-3, min);
    CHECK_EQUAL(2, ndx);

}


TEST(TableView_Find)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4;
    v[2].first = 4;

    size_t r = v.column().first.find_first(4);
    CHECK_EQUAL(1, r);
}


TEST(TableView_Follows_Changes)
{
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0,0,1);
    Query q = table.where().equal(0,1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0,0));

    // low level sanity check that we can copy a query and run the copy:
    Query q2 = q;
    TableView v2 = q2.find_all();

    // now the fun begins
    CHECK_EQUAL(1, v.size());
    table.add_empty_row();
    CHECK_EQUAL(1, v.size());
    table.set_int(0,1,1);
    v.sync_if_needed();
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(1, v.get_int(0,0));
    CHECK_EQUAL(1, v.get_int(0,1));
    table.set_int(0,0,7);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0,0));
    table.set_int(0,1,7);
    v.sync_if_needed();
    CHECK_EQUAL(0, v.size());
    table.set_int(0,1,1);
    v.sync_if_needed();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0,0));
}

TEST(TableView_SyncAfterCopy) {
    Table table;
    table.add_column(type_Int, "first");
    table.add_empty_row();
    table.set_int(0,0,1);

    // do initial query
    Query q = table.where().equal(0,1);
    TableView v = q.find_all();
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(1, v.get_int(0,0));

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
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    CHECK_EQUAL(3, v.size());
    v[0].first = 5;
    v[1].first = 4; // match
    v[2].first = 4; // match

    // todo, add creation to wrapper function in table.h
    TestTableInt::View v2 = v.column().first.find_all(4);
    CHECK_EQUAL(2, v2.size());
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}

namespace {

TIGHTDB_TABLE_1(TestTableString,
                first, String)

} // anonymous namespace

TEST(TableView_FindAllString)
{
    TestTableString table;

    table.add("a");
    table.add("a");
    table.add("a");

    TestTableString::View v = table.column().first.find_all("a");
    v[0].first = "foo";
    v[1].first = "bar"; // match
    v[2].first = "bar"; // match

    // todo, add creation to wrapper function in table.h
    TestTableString::View v2 = v.column().first.find_all("bar");
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}


// primitive C locale comparer. But that's OK since all we want to test is if the callback is invoked
bool got_called = false;
bool comparer(const char* s1, const char* s2)
{
    got_called = true;
    return *s1 < *s2;
}

TEST(TableView_StringSort)
{
    // WARNING: Do not use the C++11 method (set_string_compare_method(1)) on Windows 8.1 because it has a bug that
    // takes length in count when sorting ("b" comes before "aaaa"). Bug is not present in Windows 7.

    // Test of handling of unicode takes place in test_utf8.cpp
    TestTableString table;

    table.add("alpha");
    table.add("zebra");
    table.add("ALPHA");
    table.add("ZEBRA");

    // Core-only is default comparer
    TestTableString::View v = table.where().find_all();
    v.column().first.sort();
    CHECK_EQUAL("alpha", v[0].first);
    CHECK_EQUAL("ALPHA", v[1].first);
    CHECK_EQUAL("zebra", v[2].first);
    CHECK_EQUAL("ZEBRA", v[3].first);

    // Should be exactly the same as above because 0 was default already
    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);
    v.column().first.sort();
    CHECK_EQUAL("alpha", v[0].first);
    CHECK_EQUAL("ALPHA", v[1].first);
    CHECK_EQUAL("zebra", v[2].first);
    CHECK_EQUAL("ZEBRA", v[3].first);

    // Test descending mode
    v.column().first.sort(false);
    CHECK_EQUAL("alpha", v[3].first);
    CHECK_EQUAL("ALPHA", v[2].first);
    CHECK_EQUAL("zebra", v[1].first);
    CHECK_EQUAL("ZEBRA", v[0].first);

    // Test if callback comparer works. Our callback is a primitive dummy-comparer
    set_string_compare_method(STRING_COMPARE_CALLBACK, &comparer);
    v.column().first.sort();
    CHECK_EQUAL("ALPHA", v[0].first);
    CHECK_EQUAL("ZEBRA", v[1].first);
    CHECK_EQUAL("alpha", v[2].first);
    CHECK_EQUAL("zebra", v[3].first);
    CHECK_EQUAL(true, got_called);

#ifdef _MSC_VER
    // Try C++11 method which uses current locale of the operating system to give precise sorting. This C++11 feature
    // is currently (mid 2014) only supported by Visual Studio
    got_called = false;
    bool available = set_string_compare_method(STRING_COMPARE_CPP11, null_ptr);
    if (available) {
        v.column().first.sort();
        CHECK_EQUAL("alpha", v[0].first);
        CHECK_EQUAL("ALPHA", v[1].first);
        CHECK_EQUAL("zebra", v[2].first);
        CHECK_EQUAL("ZEBRA", v[3].first);
        CHECK_EQUAL(false, got_called);
    }
#endif

    // Set back to default for use by other unit tests
    set_string_compare_method(STRING_COMPARE_CORE, null_ptr);
}

TEST(TableView_FloatDoubleSort)
{
    TestTableFloatDouble t;

    t.add(1.0f, 10.0);
    t.add(3.0f, 30.0);
    t.add(2.0f, 20.0);
    t.add(0.0f, 5.0);

    TestTableFloatDouble::View tv = t.where().find_all();
    tv.column().first.sort();

    CHECK_EQUAL(0.0f, tv[0].first);
    CHECK_EQUAL(1.0f, tv[1].first);
    CHECK_EQUAL(2.0f, tv[2].first);
    CHECK_EQUAL(3.0f, tv[3].first);

    tv.column().second.sort();
    CHECK_EQUAL(5.0f, tv[0].second);
    CHECK_EQUAL(10.0f, tv[1].second);
    CHECK_EQUAL(20.0f, tv[2].second);
    CHECK_EQUAL(30.0f, tv[3].second);
}

TEST(TableView_DoubleSortPrecision)
{
    // Detect if sorting algorithm accidentially casts doubles to float somewhere so that precision gets lost
    TestTableFloatDouble t;

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
    t.add(f2, d2);
    t.add(f1, d1);

    TestTableFloatDouble::View tv = t.where().find_all();
    tv.column().first.sort();

    // Sort should be stable
    CHECK_EQUAL(f2, tv[0].first);
    CHECK_EQUAL(f1, tv[1].first);

    // If sort is stable, and compare makes a draw because the doubles are accidentially casted to float in Realm, then
    // original order would be maintained. Check that it's not maintained:
    tv.column().second.sort();
    CHECK_EQUAL(d1, tv[0].second);
    CHECK_EQUAL(d2, tv[1].second);
}


TEST(TableView_Delete)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(1);
    table.add(3);
    table.add(1);

    TestTableInt::View v = table.column().first.find_all(1);
    CHECK_EQUAL(3, v.size());

    v.remove(1);
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_source_ndx(0));
    CHECK_EQUAL(3, v.get_source_ndx(1));

    CHECK_EQUAL(4, table.size());
    CHECK_EQUAL(1, table[0].first);
    CHECK_EQUAL(2, table[1].first);
    CHECK_EQUAL(3, table[2].first);
    CHECK_EQUAL(1, table[3].first);

    v.remove(0);
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    CHECK_EQUAL(3, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
    CHECK_EQUAL(1, table[2].first);

    v.remove(0);
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
}

TEST(TableView_Clear)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(1);
    table.add(3);
    table.add(1);

    TestTableInt::View v = table.column().first.find_all(1);
    CHECK_EQUAL(3, v.size());

    v.clear();
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
}


//exposes a bug in stacked tableview:
//view V1 selects a subset of rows from Table T1
//View V2 selects rows from  view V1
//Then, some rows in V2 can be found, that are not in V1
TEST(TableView_Stacked)
{
    Table t;
    t.add_column(type_Int,"i1");
    t.add_column(type_Int,"i2");
    t.add_column(type_String,"S1");
    t.add_empty_row(2);
    t.set_int(0,0,1);    t.set_int(1,0,2); t.set_string(2,0,"A");    //   1 2   "A"
    t.set_int(0,1,2);    t.set_int(1,1,2); t.set_string(2,1,"B");    //   2 2   "B"

    TableView tv = t.find_all_int(0,2);
    TableView tv2 = tv.find_all_int(1,2);
    CHECK_EQUAL(1,tv2.size()); //evaluates tv2.size to 1 which is expected
    CHECK_EQUAL("B",tv2.get_string(2,0)); //evalates get_string(2,0) to "A" which is not expected


    

}


TEST(TableView_ClearNone)
{
    TestTableInt table;

    TestTableInt::View v = table.column().first.find_all(1);
    CHECK_EQUAL(0, v.size());

    v.clear();
}


TEST(TableView_FindAllStacked)
{
    TestTableInt2 table;

    table.add(0, 1);
    table.add(0, 2);
    table.add(0, 3);
    table.add(1, 1);
    table.add(1, 2);
    table.add(1, 3);

    TestTableInt2::View v = table.column().first.find_all(0);
    CHECK_EQUAL(3, v.size());

    TestTableInt2::View v2 = v.column().second.find_all(2);
    CHECK_EQUAL(1, v2.size());
    CHECK_EQUAL(0, v2[0].first);
    CHECK_EQUAL(2, v2[0].second);
    CHECK_EQUAL(1, v2.get_source_ndx(0));
}


TEST(TableView_LowLevelSubtables)
{
    Table table;
    vector<size_t> column_path;
    table.add_column(type_Bool,  "enable");
    table.add_column(type_Table, "subtab");
    table.add_column(type_Mixed, "mixed");
    column_path.push_back(1);
    table.add_subcolumn(column_path, type_Bool,  "enable");
    table.add_subcolumn(column_path, type_Table, "subtab");
    table.add_subcolumn(column_path, type_Mixed, "mixed");
    column_path.push_back(1);
    table.add_subcolumn(column_path, type_Bool,  "enable");
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
                CHECK_EQUAL(true,  bool(subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            subview.clear_subtable(2, 1 + i_1); // Mixed
            TableRef subsubtab_mix = subview.get_subtable(2, 1 + i_1);
            subsubtab_mix->add_column(type_Bool,  "enable");
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(2 * (1 + i_1));
            for (int i_3 = 0; i_3 != 2 * (1 + i_1); ++i_3)
                subsubtab_mix->set_bool(0, i_3, i_3 % 2 == 0);
            TableView subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(1 + i_1, subsubview_mix.size());

            for (int i_3 = 0; i_3 != 1 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(subview.get_subtable(1, i_2)));
            CHECK_EQUAL(i_2 == 1 + i_1, bool(subview.get_subtable(2, i_2))); // Mixed
            CHECK_EQUAL(i_2 == 0 + i_1 ? 2 * (3 + i_1) : 0, subview.get_subtable_size(1, i_2));
            CHECK_EQUAL(i_2 == 1 + i_1 ? 2 * (1 + i_1) : 0, subview.get_subtable_size(2, i_2)); // Mixed
        }

        view.clear_subtable(2, i_1); // Mixed
        TableRef subtab_mix = view.get_subtable(2, i_1);
        vector<size_t> subcol_path;
        subtab_mix->add_column(type_Bool,  "enable");
        subtab_mix->add_column(type_Table, "subtab");
        subtab_mix->add_column(type_Mixed, "mixed");
        subcol_path.push_back(1);
        subtab_mix->add_subcolumn(subcol_path, type_Bool,  "enable");
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
                CHECK_EQUAL(true,  bool(subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            subview_mix.clear_subtable(2, 2 + i_1); // Mixed
            TableRef subsubtab_mix = subview_mix.get_subtable(2, 2 + i_1);
            subsubtab_mix->add_column(type_Bool,  "enable");
            subsubtab_mix->add_column(type_Table, "subtab");
            subsubtab_mix->add_column(type_Mixed, "mixed");
            subsubtab_mix->add_empty_row(2 * (5 + i_1));
            for (int i_3 = 0; i_3 != 2 * (5 + i_1); ++i_3)
                subsubtab_mix->set_bool(0, i_3, i_3 % 2 == 0);
            TableView subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(5 + i_1, subsubview_mix.size());

            for (int i_3 = 0; i_3 != 5 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(subview_mix.get_subtable(1, i_2)));
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
                CHECK_EQUAL(true,  bool(const_subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            ConstTableRef subsubtab_mix = const_subview.get_subtable(2, 1 + i_1);
            ConstTableView const_subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(1 + i_1, const_subsubview_mix.size());
            for (int i_3 = 0; i_3 != 1 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(const_subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(const_subview.get_subtable(1, i_2)));
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
                CHECK_EQUAL(true,  bool(const_subsubview.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview.get_subtable_size(2, i_3)); // Mixed
            }

            ConstTableRef subsubtab_mix = const_subview_mix.get_subtable(2, 2 + i_1);
            ConstTableView const_subsubview_mix = subsubtab_mix->where().equal(0, true).find_all();
            CHECK_EQUAL(5 + i_1, const_subsubview_mix.size());
            for (int i_3 = 0; i_3 != 5 + i_1; ++i_3) {
                CHECK_EQUAL(true,  bool(const_subsubview_mix.get_subtable(1, i_3)));
                CHECK_EQUAL(false, bool(const_subsubview_mix.get_subtable(2, i_3))); // Mixed
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(1, i_3));
                CHECK_EQUAL(0, const_subsubview_mix.get_subtable_size(2, i_3)); // Mixed
            }
        }
        for (int i_2 = 0; i_2 != 2 + i_1; ++i_2) {
            CHECK_EQUAL(true,           bool(const_subview_mix.get_subtable(1, i_2)));
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


namespace {

TIGHTDB_TABLE_1(MyTable1,
                val, Int)

TIGHTDB_TABLE_2(MyTable2,
                val, Int,
                subtab, Subtable<MyTable1>)

TIGHTDB_TABLE_2(MyTable3,
                val, Int,
                subtab, Subtable<MyTable2>)

} // anonymous namespace

TEST(TableView_HighLevelSubtables)
{
    MyTable3 t;
    const MyTable3& ct = t;

    t.add();
    MyTable3::View v = t.column().val.find_all(0);
    MyTable3::ConstView cv = ct.column().val.find_all(0);

    {
        MyTable3::View v2 = v.column().val.find_all(0);
        MyTable3::ConstView cv2 = cv.column().val.find_all(0);

        MyTable3::ConstView cv3 = t.column().val.find_all(0);
        MyTable3::ConstView cv4 = v.column().val.find_all(0);

        // Also test assigment that converts to const
        cv3 = t.column().val.find_all(0);
        cv4 = v.column().val.find_all(0);

        static_cast<void>(v2);
        static_cast<void>(cv2);
        static_cast<void>(cv3);
        static_cast<void>(cv4);
    }

    {
        MyTable2::Ref       s1 = v[0].subtab;
        MyTable2::ConstRef  s2 = v[0].subtab;
        MyTable2::Ref       s3 = v[0].subtab->get_table_ref();
        MyTable2::ConstRef  s4 = v[0].subtab->get_table_ref();
        MyTable2::Ref       s5 = v.column().subtab[0];
        MyTable2::ConstRef  s6 = v.column().subtab[0];
        MyTable2::Ref       s7 = v.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef  s8 = v.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef cs1 = cv[0].subtab;
        MyTable2::ConstRef cs2 = cv[0].subtab->get_table_ref();
        MyTable2::ConstRef cs3 = cv.column().subtab[0];
        MyTable2::ConstRef cs4 = cv.column().subtab[0]->get_table_ref();
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
        MyTable1::Ref       s1 = v[0].subtab[0].subtab;
        MyTable1::ConstRef  s2 = v[0].subtab[0].subtab;
        MyTable1::Ref       s3 = v[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef  s4 = v[0].subtab[0].subtab->get_table_ref();
        MyTable1::Ref       s5 = v.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef  s6 = v.column().subtab[0]->column().subtab[0];
        MyTable1::Ref       s7 = v.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef  s8 = v.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef cs1 = cv[0].subtab[0].subtab;
        MyTable1::ConstRef cs2 = cv[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef cs3 = cv.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef cs4 = cv.column().subtab[0]->column().subtab[0]->get_table_ref();
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

    v[0].subtab[0].val = 1;
    CHECK_EQUAL(v[0].subtab[0].val,                     1);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  1);
    CHECK_EQUAL(v[0].subtab->column().val[0],           1);
    CHECK_EQUAL(v.column().subtab[0][0].val,            1);

    v.column().subtab[0]->column().val[0] = 2;
    CHECK_EQUAL(v[0].subtab[0].val,                     2);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  2);
    CHECK_EQUAL(v[0].subtab->column().val[0],           2);
    CHECK_EQUAL(v.column().subtab[0][0].val,            2);

    v[0].subtab->column().val[0] = 3;
    CHECK_EQUAL(v[0].subtab[0].val,                     3);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  3);
    CHECK_EQUAL(v[0].subtab->column().val[0],           3);
    CHECK_EQUAL(v.column().subtab[0][0].val,            3);

    v.column().subtab[0][0].val = 4;
    CHECK_EQUAL(v[0].subtab[0].val,                     4);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  4);
    CHECK_EQUAL(v[0].subtab->column().val[0],           4);
    CHECK_EQUAL(v.column().subtab[0][0].val,            4);
    CHECK_EQUAL(cv[0].subtab[0].val,                    4);
    CHECK_EQUAL(cv.column().subtab[0]->column().val[0], 4);
    CHECK_EQUAL(cv[0].subtab->column().val[0],          4);
    CHECK_EQUAL(cv.column().subtab[0][0].val,           4);

    v[0].subtab[0].subtab->add();
    v[0].subtab[0].subtab[0].val = 5;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                               5);
    CHECK_EQUAL(v.column().subtab[0]->column().subtab[0]->column().val[0],  5);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                              5);
    CHECK_EQUAL(cv.column().subtab[0]->column().subtab[0]->column().val[0], 5);

    v.column().subtab[0]->column().subtab[0]->column().val[0] = 6;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                               6);
    CHECK_EQUAL(v.column().subtab[0]->column().subtab[0]->column().val[0],  6);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                              6);
    CHECK_EQUAL(cv.column().subtab[0]->column().subtab[0]->column().val[0], 6);
}


TEST(TableView_ToString)
{
    TestTableInt2 tbl;

    tbl.add(2, 123456);
    tbl.add(4, 1234567);
    tbl.add(6, 12345678);
    tbl.add(4, 12345678);

    string s  = "    first    second\n";
    string s0 = "0:      2    123456\n";
    string s1 = "1:      4   1234567\n";
    string s2 = "2:      6  12345678\n";
    string s3 = "3:      4  12345678\n";

    // Test full view
    stringstream ss;
    TestTableInt2::View tv = tbl.where().find_all();
    tv.to_string(ss);
    CHECK_EQUAL(s+s0+s1+s2+s3, ss.str());

    // Find partial view: row 1+3
    stringstream ss2;
    tv = tbl.where().first.equal(4).find_all();
    tv.to_string(ss2);
    CHECK_EQUAL(s+s1+s3, ss2.str());

    // test row_to_string. get row 0 of previous view - i.e. row 1 in tbl
    stringstream ss3;
    tv.row_to_string(0,ss3);
    CHECK_EQUAL(s+s1, ss3.str());
}


TEST(TableView_RefCounting)
{
    TableView tv, tv2;
    {
        TableRef t = Table::create();
        t->add_column(type_Int, "myint");
        t->insert_int(0, 0, 12);
        t->insert_done();
        tv = t->where().find_all();
    }

    {
        TableRef t2 = Table::create();
        t2->add_column(type_String, "mystr");
        t2->insert_string(0, 0, "just a test string");
        t2->insert_done();
        tv2 = t2->where().find_all();
    }

    // Now try to access TableView and see that the Table is still alive
    int64_t i = tv.get_int(0, 0);
    CHECK_EQUAL(i, 12);
    string s = tv2.get_string(0, 0);
    CHECK_EQUAL(s, "just a test string");
}


TEST(TableView_DynPivot)
{
    TableRef table = Table::create();
    size_t column_ndx_sex = table->add_column(type_String, "sex");
    size_t column_ndx_age = table->add_column(type_Int,    "age");
    table->add_column(type_Bool, "hired");

    size_t count = 5000;
    for (size_t i = 0; i < count; ++i) {
        StringData sex = i % 2 ? "Male" : "Female";
        table->insert_string(0, i, sex);
        table->insert_int(1, i, 20 + (i%20));
        table->insert_bool(2, i, true);
        table->insert_done();
    }

    TableView tv = table->where().find_all();

    Table result_count;
    tv.aggregate(0, 1, Table::aggr_count, result_count);
    int64_t half = count/2;
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


    Query q = table.where();
    Query q2 = table.where().equal(0, 123);

    Query q3 = Query(q2, Query::TCopyExpressionTag());

    q2 = q;

    q2.find();








}


#endif // TEST_TABLE_VIEW
