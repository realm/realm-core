#include "testsettings.hpp"
#ifdef TEST_COLUMN_MIXED

#include <limits>

#include <realm/util/unique_ptr.hpp>
#include <realm/column_mixed.hpp>

#include "test.hpp"

using namespace std;
using namespace realm;
using namespace realm::util;


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


TEST(ColumnMixed_Int)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    int64_t max_val = numeric_limits<int64_t>::max();
    int64_t min_val = numeric_limits<int64_t>::min();
    int64_t all_bit = 0xFFFFFFFFFFFFFFFFULL; // FIXME: Undefined cast from unsigned to signed

    c.insert_int(0,       2);
    c.insert_int(1, min_val);
    c.insert_int(2, max_val);
    c.insert_int(3, all_bit);
    CHECK_EQUAL(4, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Int, c.get_type(i));

    CHECK_EQUAL(      2, c.get_int(0));
    CHECK_EQUAL(min_val, c.get_int(1));
    CHECK_EQUAL(max_val, c.get_int(2));
    CHECK_EQUAL(all_bit, c.get_int(3));

    c.set_int(0,    400);
    c.set_int(1,      0);
    c.set_int(2, -99999);
    c.set_int(3,      1);

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Int, c.get_type(i));

    CHECK_EQUAL(   400, c.get_int(0));
    CHECK_EQUAL(     0, c.get_int(1));
    CHECK_EQUAL(-99999, c.get_int(2));
    CHECK_EQUAL(     1, c.get_int(3));
    CHECK_EQUAL(4, c.size());

    c.destroy();
}


TEST(ColumnMixed_Float)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    uint32_t v = 0xFFFFFFFF;
    float f = float(v);
    float fval1[] = { 0.0f, 100.123f, -111.222f, f };
    float fval2[] = { -0.0f, -100.123f, numeric_limits<float>::max(),
                      numeric_limits<float>::min() };

    // Test insert
    for (size_t i=0; i<4; ++i)
        c.insert_float(i, fval1[i]);
    CHECK_EQUAL(4, c.size());

    for (size_t i = 0; i < c.size(); ++i) {
        CHECK_EQUAL(type_Float, c.get_type(i));
        CHECK_EQUAL(fval1[i], c.get_float(i));
    }

    // Set to new values - ensure sign is changed
    for (size_t i=0; i<4; ++i)
        c.set_float(i, fval2[i]);

    for (size_t i = 0; i < c.size(); ++i) {
        CHECK_EQUAL(type_Float, c.get_type(i));
        CHECK_EQUAL(fval2[i], c.get_float(i));
    }
    CHECK_EQUAL(4, c.size());

    c.destroy();
}


TEST(ColumnMixed_Double)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    uint64_t v = 0xFFFFFFFFFFFFFFFFULL;
    double d = double(v);
    double fval1[] = {1.0, 200.123, -111.222, d};
    double fval2[] = {-1.0, -100.123, numeric_limits<double>::max(), numeric_limits<double>::min()};

    // Test insert
    for (size_t i=0; i<4; ++i)
        c.insert_double(i, fval1[i]);
    CHECK_EQUAL(4, c.size());

    for (size_t i = 0; i < c.size(); ++i) {
        CHECK_EQUAL(type_Double, c.get_type(i));
        double v = c.get_double(i);
        CHECK_EQUAL(fval1[i], v);
    }

    // Set to new values - ensure sign is changed
    for (size_t i=0; i<4; ++i)
        c.set_double(i, fval2[i]);

    CHECK_EQUAL(4, c.size());
    for (size_t i = 0; i < c.size(); ++i) {
        CHECK_EQUAL(type_Double, c.get_type(i));
        CHECK_EQUAL(fval2[i], c.get_double(i));
    }

    c.destroy();
}


TEST(ColumnMixed_Bool)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_bool(0, true);
    c.insert_bool(1, false);
    c.insert_bool(2, true);
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Bool, c.get_type(i));

    CHECK_EQUAL(true,  c.get_bool(0));
    CHECK_EQUAL(false, c.get_bool(1));
    CHECK_EQUAL(true,  c.get_bool(2));

    c.set_bool(0, false);
    c.set_bool(1, true);
    c.set_bool(2, false);
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Bool, c.get_type(i));

    CHECK_EQUAL(false, c.get_bool(0));
    CHECK_EQUAL(true,  c.get_bool(1));
    CHECK_EQUAL(false, c.get_bool(2));

    c.destroy();
}


TEST(ColumnMixed_Date)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_datetime(0,     2);
    c.insert_datetime(1,   100);
    c.insert_datetime(2, 20000);
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_DateTime, c.get_type(i));

    CHECK_EQUAL(    2, c.get_datetime(0));
    CHECK_EQUAL(  100, c.get_datetime(1));
    CHECK_EQUAL(20000, c.get_datetime(2));

    c.set_datetime(0,   400);
    c.set_datetime(1,     0);
    c.set_datetime(2, 99999);

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_DateTime, c.get_type(i));

    CHECK_EQUAL(  400, c.get_datetime(0));
    CHECK_EQUAL(    0, c.get_datetime(1));
    CHECK_EQUAL(99999, c.get_datetime(2));
    CHECK_EQUAL(3, c.size());

    c.destroy();
}


TEST(ColumnMixed_String)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_string(0, "aaa");
    c.insert_string(1, "bbbbb");
    c.insert_string(2, "ccccccc");
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_String, c.get_type(i));

    CHECK_EQUAL("aaa",     c.get_string(0));
    CHECK_EQUAL("bbbbb",   c.get_string(1));
    CHECK_EQUAL("ccccccc", c.get_string(2));

    c.set_string(0, "dd");
    c.set_string(1, "");
    c.set_string(2, "eeeeeeeee");
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_String, c.get_type(i));

    CHECK_EQUAL("dd",        c.get_string(0));
    CHECK_EQUAL("",          c.get_string(1));
    CHECK_EQUAL("eeeeeeeee", c.get_string(2));

    c.destroy();
}


TEST(ColumnMixed_Binary)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_binary(0, BinaryData("aaa", 4));
    c.insert_binary(1, BinaryData("bbbbb", 6));
    c.insert_binary(2, BinaryData("ccccccc", 8));
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Binary, c.get_type(i));

    CHECK_EQUAL("aaa",     c.get_binary(0).data());
    CHECK_EQUAL("bbbbb",   c.get_binary(1).data());
    CHECK_EQUAL("ccccccc", c.get_binary(2).data());

    c.set_binary(0, BinaryData("dd", 3));
    c.set_binary(1, BinaryData("", 1));
    c.set_binary(2, BinaryData("eeeeeeeee", 10));
    CHECK_EQUAL(3, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Binary, c.get_type(i));

    CHECK_EQUAL("dd",        c.get_binary(0).data());
    CHECK_EQUAL("",          c.get_binary(1).data());
    CHECK_EQUAL("eeeeeeeee", c.get_binary(2).data());

    c.destroy();
}


TEST(ColumnMixed_Table)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_subtable(0, 0);
    c.insert_subtable(1, 0);
    CHECK_EQUAL(2, c.size());

    for (size_t i = 0; i < c.size(); ++i)
        CHECK_EQUAL(type_Table, c.get_type(i));

    UniquePtr<Table> t1(c.get_subtable_ptr(0));
    UniquePtr<Table> t2(c.get_subtable_ptr(1));
    CHECK(t1->is_empty());
    CHECK(t2->is_empty());

    c.destroy();
}


TEST(ColumnMixed_Mixed)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    // Insert mixed types
    c.insert_int(0, 23);
    c.insert_bool(0, false);
    c.insert_datetime(0, 23423);
    c.insert_string(0, "Hello");
    c.insert_binary(0, BinaryData("binary"));
    c.insert_subtable(0, 0);
    c.insert_float(0, 1.124f);
    c.insert_double(0, 1234.124);
    CHECK_EQUAL(8, c.size());

    CHECK_EQUAL(type_Double, c.get_type(0));
    CHECK_EQUAL(type_Float,  c.get_type(1));
    CHECK_EQUAL(type_Table,  c.get_type(2));
    CHECK_EQUAL(type_Binary, c.get_type(3));
    CHECK_EQUAL(type_String, c.get_type(4));
    CHECK_EQUAL(type_DateTime,   c.get_type(5));
    CHECK_EQUAL(type_Bool,   c.get_type(6));
    CHECK_EQUAL(type_Int,    c.get_type(7));

    // Change all entries to new types
    c.set_int(0, 23);
    c.set_bool(1, false);
    c.set_datetime(2, 23423);
    c.set_string(3, "Hello");
    c.set_binary(4, BinaryData("binary"));
    c.set_subtable(5, 0);
    c.set_float(6, 1.124f);
    c.set_double(7, 1234.124);
    CHECK_EQUAL(8, c.size());

    CHECK_EQUAL(type_Double, c.get_type(7));
    CHECK_EQUAL(type_Float,  c.get_type(6));
    CHECK_EQUAL(type_Table,  c.get_type(5));
    CHECK_EQUAL(type_Binary, c.get_type(4));
    CHECK_EQUAL(type_String, c.get_type(3));
    CHECK_EQUAL(type_DateTime,   c.get_type(2));
    CHECK_EQUAL(type_Bool,   c.get_type(1));
    CHECK_EQUAL(type_Int,    c.get_type(0));

    c.destroy();
}


TEST(ColumnMixed_SubtableSize)
{
    ref_type ref = ColumnMixed::create(Allocator::get_default());
    ColumnMixed c(Allocator::get_default(), ref, 0, 0);

    c.insert_subtable(0, 0);
    c.insert_subtable(1, 0);
    c.insert_subtable(2, 0);
    c.insert_subtable(3, 0);
    c.insert_subtable(4, 0);

    // No table instantiated yet (zero ref)
    CHECK_EQUAL( 0, c.get_subtable_size(0));

    {    // Empty table (no columns)
        TableRef t1 = c.get_subtable_ptr(1)->get_table_ref();
        CHECK(t1->is_empty());
        CHECK_EQUAL( 0, c.get_subtable_size(1));
    }

    {   // Empty table (1 column, no rows)
        TableRef t2 = c.get_subtable_ptr(2)->get_table_ref();
        CHECK(t2->is_empty());
        t2->add_column(type_Int, "col1");
        CHECK_EQUAL( 0, c.get_subtable_size(2));
    }

    {   // Table with rows
        TableRef t3 = c.get_subtable_ptr(3)->get_table_ref();
        CHECK(t3->is_empty());
        t3->add_column(type_Int, "col1");
        t3->add_empty_row(10);
        CHECK_EQUAL(10, c.get_subtable_size(3));
    }

    {   // Table with mixed column first
        TableRef t4 = c.get_subtable_ptr(4)->get_table_ref();
        CHECK(t4->is_empty());
        t4->add_column(type_Mixed, "col1");
        t4->add_empty_row(10);
        // FAILS because it tries to manually get size from first column
        // which is topped by a node with two subentries.
        CHECK_EQUAL(10, c.get_subtable_size(4));
    }

    c.destroy();
}


#endif // TEST_COLUMN_MIXED
