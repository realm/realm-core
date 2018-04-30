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
#ifdef TEST_COLUMN_FLOAT

#include <iostream>

#include "test.hpp"
#include <realm/column_integer.hpp>
#include <realm/table.hpp>

using namespace realm;
using test_util::unit_test::TestContext;


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

template <class T, size_t N>
inline size_t size_of_array(T (&)[N])
{
    return N;
}

// Article about comparing floats:
// http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/

float float_values[] = {0.0f, 1.0f, 2.12345f, 12345.12f, -12345.12f};
const size_t num_float_values = size_of_array(float_values);

double double_values[] = {0.0, 1.0, 2.12345, 12345.12, -12345.12};
const size_t num_double_values = size_of_array(double_values);

} // anonymous namespace


template <class C>
void BasicColumn_IsEmpty(TestContext& test_context)
{
    C c(Allocator::get_default());
    c.create();

    CHECK(c.is_empty());
    CHECK_EQUAL(0U, c.size());
    c.destroy();
}
TEST(FloatColumn_IsEmpty)
{
    BasicColumn_IsEmpty<FloatColumn>(test_context);
}
TEST(DoubleColumn_IsEmpty)
{
    BasicColumn_IsEmpty<DoubleColumn>(test_context);
}


template <class C, typename T>
void BasicColumn_AddGet(TestContext& test_context, T values[], size_t num_values)
{
    C c(Allocator::get_default());
    c.create();

    for (size_t i = 0; i < num_values; ++i) {
        c.add(values[i]);

        CHECK_EQUAL(i + 1, c.size());

        for (size_t j = 0; j < i; ++j)
            CHECK_EQUAL(values[j], c.get(j));
    }

    c.destroy();
}
TEST(FloatColumn_AddGet)
{
    BasicColumn_AddGet<FloatColumn, float>(test_context, float_values, num_float_values);
}
TEST(DoubleColumn_AddGet)
{
    BasicColumn_AddGet<DoubleColumn, double>(test_context, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Clear(TestContext& test_context)
{
    C c(Allocator::get_default());
    c.create();

    CHECK(c.is_empty());

    for (size_t i = 0; i < 100; ++i)
        c.add(T());
    CHECK(!c.is_empty());

    c.clear();
    CHECK(c.is_empty());

    c.destroy();
}
TEST(FloatColumn_Clear)
{
    BasicColumn_Clear<FloatColumn, float>(test_context);
}
TEST(DoubleColumn_Clear)
{
    BasicColumn_Clear<DoubleColumn, double>(test_context);
}


template <class C, typename T>
void BasicColumn_Set(TestContext& test_context, T values[], size_t num_values)
{
    C c(Allocator::get_default());
    c.create();

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
TEST(FloatColumn_Set)
{
    BasicColumn_Set<FloatColumn, float>(test_context, float_values, num_float_values);
}
TEST(DoubleColumn_Set)
{
    BasicColumn_Set<DoubleColumn, double>(test_context, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Insert(TestContext& test_context, T values[], size_t num_values)
{
    static_cast<void>(num_values);

    C c(Allocator::get_default());
    c.create();

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
TEST(FloatColumn_Insert)
{
    BasicColumn_Insert<FloatColumn, float>(test_context, float_values, num_float_values);
}
TEST(DoubleColumn_Insert)
{
    BasicColumn_Insert<DoubleColumn, double>(test_context, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Aggregates(TestContext& test_context, T values[], size_t num_values)
{
    static_cast<void>(test_context);
    static_cast<void>(num_values);
    static_cast<void>(values);

    C c(Allocator::get_default());
    c.create();

    //    double sum = c.sum();
    //    CHECK_EQUAL(0, sum);

    // todo: add tests for minimum, maximum,
    // todo !!!

    c.destroy();
}
TEST(FloatColumn_Aggregates)
{
    BasicColumn_Aggregates<FloatColumn, float>(test_context, float_values, num_float_values);
}
TEST(DoubleColumn_Aggregates)
{
    BasicColumn_Aggregates<DoubleColumn, double>(test_context, double_values, num_double_values);
}


template <class C, typename T>
void BasicColumn_Delete(TestContext& test_context, T values[], size_t num_values)
{
    C c(Allocator::get_default());
    c.create();

    for (size_t i = 0; i < num_values; ++i)
        c.add(values[i]);
    CHECK_EQUAL(5, c.size());
    CHECK_EQUAL(values[0], c.get(0));
    CHECK_EQUAL(values[1], c.get(1));
    CHECK_EQUAL(values[2], c.get(2));
    CHECK_EQUAL(values[3], c.get(3));
    CHECK_EQUAL(values[4], c.get(4));

    // Delete first
    c.erase(0);
    CHECK_EQUAL(4, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[3], c.get(2));
    CHECK_EQUAL(values[4], c.get(3));

    // Delete middle
    c.erase(2);
    CHECK_EQUAL(3, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));
    CHECK_EQUAL(values[4], c.get(2));

    // Delete last
    c.erase(2);
    CHECK_EQUAL(2, c.size());
    CHECK_EQUAL(values[1], c.get(0));
    CHECK_EQUAL(values[2], c.get(1));

    // Delete single
    c.erase(0);
    CHECK_EQUAL(1, c.size());
    CHECK_EQUAL(values[2], c.get(0));

    // Delete all
    c.erase(0);
    CHECK_EQUAL(0, c.size());

    c.destroy();
}
TEST(FloatColumn_Delete)
{
    BasicColumn_Delete<FloatColumn, float>(test_context, float_values, num_float_values);
}
TEST(DoubleColumn_Delete)
{
    BasicColumn_Delete<DoubleColumn, double>(test_context, double_values, num_double_values);
}

TEST(FloatColumn_SwapRows)
{
    auto epsilon = std::numeric_limits<float>::epsilon();

    // Normal case
    {
        FloatColumn c(Allocator::get_default());
        c.create();

        c.add(-21.389f);
        c.add(30.221f);
        c.add(10.93f);
        c.add(5.0099f);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 30.221, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 10.93, epsilon);
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.swap(1, 2);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 4);

        c.destroy();
    }

    // First two elements
    {
        FloatColumn c(Allocator::get_default());
        c.create();

        c.add(30.221f);
        c.add(10.93f);
        c.add(5.0099f);

        c.swap(0, 1);

        CHECK_APPROXIMATELY_EQUAL(c.get(0), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(1), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Last two elements
    {
        FloatColumn c(Allocator::get_default());
        c.create();

        c.add(5.0099f);
        c.add(30.221f);
        c.add(10.93f);

        c.swap(1, 2);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Indices in wrong order
    {
        FloatColumn c(Allocator::get_default());
        c.create();

        c.add(5.0099f);
        c.add(30.221f);
        c.add(10.93f);

        c.swap(2, 1);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }
}

TEST(DoubleColumn_SwapRows)
{
    auto epsilon = std::numeric_limits<double>::epsilon();

    // Normal case
    {
        DoubleColumn c(Allocator::get_default());
        c.create();

        c.add(-21.389);
        c.add(30.221);
        c.add(10.93);
        c.add(5.0099);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 30.221, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 10.93, epsilon);
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.swap(1, 2);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 4);

        c.destroy();
    }

    // First two elements
    {
        DoubleColumn c(Allocator::get_default());
        c.create();

        c.add(30.221);
        c.add(10.93);
        c.add(5.0099);

        c.swap(0, 1);

        CHECK_APPROXIMATELY_EQUAL(c.get(0), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(1), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Last two elements
    {
        DoubleColumn c(Allocator::get_default());
        c.create();

        c.add(5.0099);
        c.add(30.221);
        c.add(10.93);

        c.swap(1, 2);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Indices in wrong order
    {
        DoubleColumn c(Allocator::get_default());
        c.create();

        c.add(5.0099);
        c.add(30.221);
        c.add(10.93);

        c.swap(2, 1);

        CHECK_APPROXIMATELY_EQUAL(c.get(1), 10.93, epsilon);
        CHECK_APPROXIMATELY_EQUAL(c.get(2), 30.221, epsilon);
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }
}

#ifdef LEGACY_TESTS
TEST(DoubleColumn_InitOfEmptyColumn)
{
    Table t;
    auto col_0 = t.add_column(type_Double, "works");
    auto col_1 = t.add_column(type_Double, "works also");
    t.create_object().set_all(1.1, 2.2);
    t.remove_column(col_1);
    Obj obj = t.create_object();
    t.add_column(type_Double, "doesn't work");
    CHECK_EQUAL(0.0, obj.get<Double>(col_0));
}

// Test for a bug where default values of newly added float/double columns did not obey their nullability
TEST_TYPES(DoubleFloatColumn_InitOfEmptyColumnNullable, std::true_type, std::false_type)
{
    constexpr bool nullable_toggle = TEST_TYPE::value;
    Table t;
    t.add_column(type_Int, "unused");
    Obj obj = t.create_object();
    auto col_0 = t.add_column(type_Double, "d", nullable_toggle);
    auto col_1 = t.add_column(type_Float, "f", nullable_toggle);
    CHECK(obj.is_null(col_0) == nullable_toggle);
    CHECK(obj.is_null(col_1) == nullable_toggle);
    if (nullable_toggle) {
        obj.set_null(col_0);
        obj.set_null(col_1);
        CHECK(obj.is_null(col_0));
        CHECK(obj.is_null(col_1));
    }
}

TEST(FloatColumn_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Float, "works");
    auto col_1 = t.add_column(type_Float, "works also");
    t.create_object().set_all(1.1f, 2.2f);
    t.remove_column(col_1);
    Obj obj = t.create_object();
    auto col_2 = t.add_column(type_Float, "doesn't work");
    CHECK_EQUAL(0.0, obj.get<Float>(col_2));
}

TEST(ColumnInt_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Int, "works");
    auto col_1 = t.add_column(type_Int, "works also");
    t.create_object().set_all(1, 2);
    t.remove_column(col_1);
    Obj obj = t.create_object();
    auto col_2 = t.add_column(type_Int, "doesn't work");
    CHECK_EQUAL(0, obj.get<Int>(col_2));
}

TEST(ColumnString_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_String, "works");
    auto col_1 = t.add_column(type_String, "works also", false);
    t.create_object().set_all("yellow", "very bright");
    t.remove_column(col_1);
    Obj obj = t.create_object();
    auto col_2 = t.add_column(type_String, "doesn't work");
    CHECK_EQUAL("", obj.get<String>(col_2));
}

TEST(ColumnBinary_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Binary, "works");
    auto col_1 = t.add_column(type_Binary, "works also");
    t.create_object().set_all(BinaryData("yellow"), BinaryData("very bright"));
    t.remove_column(col_1);
    Obj obj = t.create_object();
    auto col_2 = t.add_column(type_Binary, "doesn't work");
    CHECK_NOT_EQUAL(BinaryData(), obj.get<Binary>(col_2));
}

TEST(ColumnBool_InitOfEmptyColumn)
{
    Table t;
    t.add_column(type_Bool, "works");
    auto col_1 = t.add_column(type_Bool, "works also");
    t.create_object().set_all(true, true);
    t.remove_column(col_1);
    Obj obj = t.create_object();
    auto col_2 = t.add_column(type_Bool, "doesn't work");
    CHECK_EQUAL(false, obj.get<Bool>(col_2));
}
#endif

#endif // TEST_COLUMN_FLOAT
