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
#ifdef TEST_ARRAY_FLOAT

#include <realm/array_basic.hpp>
#include <realm/column_integer.hpp>

#include "test.hpp"

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


// TODO: Add test of full range of floats.

template <class A, typename T>
void BasicArray_AddGet(TestContext& test_context, T values[], size_t num_values)
{
    A f(Allocator::get_default());
    f.create();
    for (size_t i = 0; i < num_values; ++i) {
        f.add(values[i]);

        CHECK_EQUAL(i + 1, f.size());

        for (size_t j = 0; j < i; ++j)
            CHECK_EQUAL(values[j], f.get(j));
    }

    f.clear();
    CHECK_EQUAL(0, f.size());

    f.destroy(); // cleanup
}
TEST(ArrayFloat_AddGet)
{
    BasicArray_AddGet<ArrayFloat, float>(test_context, float_values, num_float_values);
}
TEST(ArrayDouble_AddGet)
{
    BasicArray_AddGet<ArrayDouble, double>(test_context, double_values, num_double_values);
}


template <class A, typename T>
void BasicArray_AddManyValues(TestContext& test_context)
{
    A f(Allocator::get_default());
    f.create();
    size_t repeats = 1100;
    for (size_t i = 0; i < repeats; ++i) {
        f.add(T(i));
        T val = f.get(i);
        CHECK_EQUAL(T(i), val);
        CHECK_EQUAL(i + 1, f.size());
    }
    for (size_t i = 0; i < repeats; ++i) {
        T val = f.get(i);
        CHECK_EQUAL(T(i), val);
    }

    f.clear();
    CHECK_EQUAL(0, f.size());

    f.destroy(); // cleanup
}
TEST(ArrayFloat_AddManyValues)
{
    BasicArray_AddManyValues<ArrayFloat, float>(test_context);
}
TEST(ArrayDouble_AddManyValues)
{
    BasicArray_AddManyValues<ArrayDouble, double>(test_context);
}

template <class A, typename T>
void BasicArray_Delete(TestContext& test_context)
{
    A f(Allocator::get_default());
    f.create();
    for (size_t i = 0; i < 5; ++i)
        f.add(T(i));

    // Delete first
    f.erase(0);
    CHECK_EQUAL(4, f.size());
    CHECK_EQUAL(1, f.get(0));
    CHECK_EQUAL(2, f.get(1));
    CHECK_EQUAL(3, f.get(2));
    CHECK_EQUAL(4, f.get(3));

    // Delete last
    f.erase(3);
    CHECK_EQUAL(3, f.size());
    CHECK_EQUAL(1, f.get(0));
    CHECK_EQUAL(2, f.get(1));
    CHECK_EQUAL(3, f.get(2));

    // Delete middle
    f.erase(1);
    CHECK_EQUAL(2, f.size());
    CHECK_EQUAL(1, f.get(0));
    CHECK_EQUAL(3, f.get(1));

    // Delete all
    f.erase(0);
    CHECK_EQUAL(1, f.size());
    CHECK_EQUAL(3, f.get(0));
    f.erase(0);
    CHECK_EQUAL(0, f.size());
    CHECK(f.is_empty());

    f.destroy(); // cleanup
}
TEST(ArrayFloat_Delete)
{
    BasicArray_Delete<ArrayFloat, float>(test_context);
}
TEST(ArrayDouble_Delete)
{
    BasicArray_Delete<ArrayDouble, double>(test_context);
}


template <class A, typename T>
void BasicArray_Set(TestContext& test_context, T values[], size_t num_values)
{
    A f(Allocator::get_default());
    f.create();

    CHECK_EQUAL(0, f.size());
    for (size_t i = 0; i < num_values; ++i)
        f.add(values[i]);
    CHECK_EQUAL(num_values, f.size());

    f.set(0, T(1.6));
    CHECK_EQUAL(T(1.6), f.get(0));
    f.set(3, T(987.23));
    CHECK_EQUAL(T(987.23), f.get(3));

    CHECK_EQUAL(values[1], f.get(1));
    CHECK_EQUAL(values[2], f.get(2));
    CHECK_EQUAL(values[4], f.get(4));
    CHECK_EQUAL(num_values, f.size());

    f.destroy(); // cleanup
}
TEST(ArrayFloat_Set)
{
    BasicArray_Set<ArrayFloat, float>(test_context, float_values, num_float_values);
}
TEST(ArrayDouble_Set)
{
    BasicArray_Set<ArrayDouble, double>(test_context, double_values, num_double_values);
}


template <class A, typename T>
void BasicArray_Insert(TestContext& test_context)
{
    A f(Allocator::get_default());
    f.create();

    T v0 = T(123.970);
    T v1 = T(-321.971);
    T v2 = T(555.972);
    T v3 = T(-999.973);

    // Insert in empty array
    f.insert(0, v0);
    CHECK_EQUAL(v0, f.get(0));
    CHECK_EQUAL(1, f.size());

    // Insert in top
    f.insert(0, v1);
    CHECK_EQUAL(v1, f.get(0));
    CHECK_EQUAL(v0, f.get(1));
    CHECK_EQUAL(2, f.size());

    // Insert in middle
    f.insert(1, v2);
    CHECK_EQUAL(v1, f.get(0));
    CHECK_EQUAL(v2, f.get(1));
    CHECK_EQUAL(v0, f.get(2));
    CHECK_EQUAL(3, f.size());

    // Insert at buttom
    f.insert(3, v3);
    CHECK_EQUAL(v1, f.get(0));
    CHECK_EQUAL(v2, f.get(1));
    CHECK_EQUAL(v0, f.get(2));
    CHECK_EQUAL(v3, f.get(3));
    CHECK_EQUAL(4, f.size());

    f.destroy(); // cleanup
}
TEST(ArrayFloat_Insert)
{
    BasicArray_Insert<ArrayFloat, float>(test_context);
}
TEST(ArrayDouble_Insert)
{
    BasicArray_Insert<ArrayDouble, double>(test_context);
}

#endif // TEST_ARRAY_FLOAT
