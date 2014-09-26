#include "testsettings.hpp"
#ifdef TEST_COLUMN_FLOAT

#include <iostream>

#include <tightdb/column_basic.hpp>

#include "test.hpp"

using namespace tightdb;
using test_util::unit_test::TestResults;


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

template<class T, size_t N> inline size_t size_of_array(T(&)[N])
{
    return N;
}

// Article about comparing floats:
// http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/

float float_values[] = {
    0.0f,
    1.0f,
    2.12345f,
    12345.12f,
    -12345.12f
};
const size_t num_float_values = size_of_array(float_values);

double double_values[] = {
    0.0,
    1.0,
    2.12345,
    12345.12,
    -12345.12
};
const size_t num_double_values = size_of_array(double_values);

} // anonymous namespace


template <class C>
void BasicColumn_IsEmpty(TestResults& test_results)
{
    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    CHECK(c.is_empty());
    CHECK_EQUAL(0U, c.size());
    c.destroy();
}
TEST(ColumnFloat_IsEmpty)
{
    BasicColumn_IsEmpty<ColumnFloat>(test_results);
}
TEST(ColumnDouble_IsEmpty)
{
    BasicColumn_IsEmpty<ColumnDouble>(test_results);
}


template <class C, typename T>
void BasicColumn_AddGet(TestResults& test_results, T values[], size_t num_values)
{
    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    for (size_t i = 0; i < num_values; ++i) {
        c.add(values[i]);

        CHECK_EQUAL(i+1, c.size());

        for (size_t j = 0; j < i; ++j)
            CHECK_EQUAL(values[j], c.get(j));
    }

    c.destroy();
}
TEST(ColumnFloat_AddGet)
{
    BasicColumn_AddGet<ColumnFloat, float>(test_results, float_values, num_float_values);
}
TEST(ColumnDouble_AddGet)
{
    BasicColumn_AddGet<ColumnDouble, double>(test_results, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Clear(TestResults& test_results)
{
    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    CHECK(c.is_empty());

    for (size_t i = 0; i < 100; ++i)
        c.add();
    CHECK(!c.is_empty());

    c.clear();
    CHECK(c.is_empty());

    c.destroy();
}
TEST(ColumnFloat_Clear)
{
    BasicColumn_Clear<ColumnFloat, float>(test_results);
}
TEST(ColumnDouble_Clear)
{
    BasicColumn_Clear<ColumnDouble, double>(test_results);
}


template <class C, typename T>
void BasicColumn_Set(TestResults& test_results, T values[], size_t num_values)
{
    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    for (size_t i = 0; i < num_values; ++i)
        c.add(values[i]);
    CHECK_EQUAL(num_values, c.size());

    T v0 = T(1.6);
    T v3 = T(-987.23);
    c.set(0, v0);
    CHECK_EQUAL(v0, c.get(0));
    c.set(3, v3);
    CHECK_EQUAL(v3, c.get(3));

    CHECK_EQUAL(values[1], c.get(1));
    CHECK_EQUAL(values[2], c.get(2));
    CHECK_EQUAL(values[4], c.get(4));

    c.destroy();
}
TEST(ColumnFloat_Set)
{
    BasicColumn_Set<ColumnFloat, float>(test_results, float_values, num_float_values);
}
TEST(ColumnDouble_Set)
{
    BasicColumn_Set<ColumnDouble, double>(test_results, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Insert(TestResults& test_results, T values[], size_t num_values)
{
    static_cast<void>(num_values);

    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    // Insert in empty column
    c.insert(0, values[0]);
    CHECK_EQUAL(values[0], c.get(0));
    CHECK_EQUAL(1, c.size());

    // Insert in top
    c.insert(0, values[1]);
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[0], c.get(1));
    CHECK_EQUAL(2, c.size());

    // Insert in middle
    c.insert(1, values[2]);
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[0], c.get(2));
    CHECK_EQUAL(3, c.size());

    // Insert at buttom
    c.insert(3, values[3]);
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[0], c.get(2));
    CHECK_EQUAL(values[3], c.get(3));
    CHECK_EQUAL(4, c.size());

    // Insert at top
    c.insert(0, values[4]);
    CHECK_EQUAL(values[4], c.get(0));
    CHECK_EQUAL(values[1], c.get(1));
    CHECK_EQUAL(values[2], c.get(2));
    CHECK_EQUAL(values[0], c.get(3));
    CHECK_EQUAL(values[3], c.get(4));
    CHECK_EQUAL(5, c.size());

    c.destroy();
}
TEST(ColumnFloat_Insert)
{
    BasicColumn_Insert<ColumnFloat, float>(test_results, float_values, num_float_values);
}
TEST(ColumnDouble_Insert)
{
    BasicColumn_Insert<ColumnDouble, double>(test_results, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Aggregates(TestResults& test_results, T values[], size_t num_values)
{
    static_cast<void>(test_results);
    static_cast<void>(num_values);
    static_cast<void>(values);

    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

//    double sum = c.sum();
//    CHECK_EQUAL(0, sum);

    // todo: add tests for minimum, maximum,
    // todo !!!

   c.destroy();
}
TEST(ColumnFloat_Aggregates)
{
    BasicColumn_Aggregates<ColumnFloat, float>(test_results, float_values, num_float_values);
}
TEST(ColumnDouble_Aggregates)
{
    BasicColumn_Aggregates<ColumnDouble, double>(test_results, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Delete(TestResults& test_results, T values[], size_t num_values)
{
    ref_type ref = C::create(Allocator::get_default());
    C c(Allocator::get_default(), ref);

    for (size_t i = 0; i < num_values; ++i)
        c.add(values[i]);
    CHECK_EQUAL(5, c.size());
    CHECK_EQUAL(values[0], c.get(0));
    CHECK_EQUAL(values[1], c.get(1));
    CHECK_EQUAL(values[2], c.get(2));
    CHECK_EQUAL(values[3], c.get(3));
    CHECK_EQUAL(values[4], c.get(4));

    // Delete first
    c.erase(0, 0 == c.size()-1);
    CHECK_EQUAL(4, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[3], c.get(2));
    CHECK_EQUAL(values[4], c.get(3));

    // Delete middle
    c.erase(2, 2 == c.size()-1);
    CHECK_EQUAL(3, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[4], c.get(2));

    // Delete last
    c.erase(2, 2 == c.size()-1);
    CHECK_EQUAL(2, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));

    // Delete single
    c.erase(0, 0 == c.size()-1);
    CHECK_EQUAL(1, c.size());
    CHECK_EQUAL(values[2], c.get(0));

    // Delete all
    c.erase(0, 0 == c.size()-1);
    CHECK_EQUAL(0, c.size());

    c.destroy();
}
TEST(ColumnFloat_Delete)
{
    BasicColumn_Delete<ColumnFloat, float>(test_results, float_values, num_float_values);
}
TEST(ColumnDouble_Delete)
{
    BasicColumn_Delete<ColumnDouble, double>(test_results, double_values, num_double_values);
}

TEST(ColumnDouble_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Double, "works");
    t.add_column(type_Double, "works also");
    t.add_empty_row();
    t.set_double(0,0,1.1);
    t.set_double(1,0,2.2);
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Double, "doesn't work");
    CHECK_EQUAL(0.0, t.get_double(1,0));
}

TEST(ColumnFloat_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Float, "works");
    t.add_column(type_Float, "works also");
    t.add_empty_row();
    t.set_float(0,0,1.1f);
    t.set_float(1,0,2.2f);
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Float, "doesn't work");
    CHECK_EQUAL(0.0, t.get_float(1,0));
}

TEST(ColumnInt_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Int, "works");
    t.add_column(type_Int, "works also");
    t.add_empty_row();
    t.set_int(0,0,1);
    t.set_int(1,0,2);
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Int, "doesn't work");
    CHECK_EQUAL(0, t.get_int(1,0));
}

TEST(ColumnString_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_String, "works");
    t.add_column(type_String, "works also");
    t.add_empty_row();
    t.set_string(0,0, "yellow");
    t.set_string(1,0, "very bright");
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_String, "doesn't work");
    CHECK_EQUAL("", t.get_string(1,0));
}

TEST(ColumnBinary_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Binary, "works");
    t.add_column(type_Binary, "works also");
    t.add_empty_row();
    t.set_binary(0,0, BinaryData("yellow"));
    t.set_binary(1,0, BinaryData("very bright"));
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Binary, "doesn't work");
    CHECK_EQUAL(BinaryData(), t.get_binary(1,0));
}

TEST(ColumnBool_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Bool, "works");
    t.add_column(type_Bool, "works also");
    t.add_empty_row();
    t.set_bool(0,0, true);
    t.set_bool(1,0, true);
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Bool, "doesn't work");
    CHECK_EQUAL(false, t.get_bool(1,0));
}

TEST(ColumnMixed_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Mixed, "works");
    t.add_column(type_Mixed, "works also");
    t.add_empty_row();
    t.set_mixed(0,0, Mixed(1.1));
    t.set_mixed(1,0, Mixed(2.2));
    t.remove_column(1);
    t.add_empty_row();
    t.add_column(type_Mixed, "doesn't work");
    CHECK_EQUAL(0, t.get_mixed(1,0));
}


#endif // TEST_COLUMN_FLOAT
