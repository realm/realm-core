#include "testsettings.hpp"
#ifdef TEST_ARRAY_FLOAT

#include <tightdb/array_basic.hpp>
#include <tightdb/column.hpp>

#include "test.hpp"

using namespace realm;
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


// TODO: Add test of full range of floats.

template <class A, typename T>
void BasicArray_AddGet(TestResults& test_results, T values[], size_t num_values)
{
    A f(Allocator::get_default());
    f.create();
    for (size_t i = 0; i < num_values; ++i) {
        f.add(values[i]);

        CHECK_EQUAL(i+1, f.size());

        for (size_t j=0; j<i; ++j)
            CHECK_EQUAL(values[j], f.get(j));
    }

    f.clear();
    CHECK_EQUAL(0, f.size());

    f.destroy();    // cleanup
}
TEST(ArrayFloat_AddGet)
{
    BasicArray_AddGet<ArrayFloat, float>(test_results, float_values, num_float_values);
}
TEST(ArrayDouble_AddGet)
{
    BasicArray_AddGet<ArrayDouble, double>(test_results, double_values, num_double_values);
}


template <class A, typename T>
void BasicArray_AddManyValues(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();
    size_t repeats = 1100;
    for (size_t i = 0; i < repeats; ++i) {
        f.add(T(i));
        T val = f.get(i);
        CHECK_EQUAL(T(i), val);
        CHECK_EQUAL(i+1, f.size());
    }
    for (size_t i = 0; i < repeats; ++i) {
        T val = f.get(i);
        CHECK_EQUAL(T(i), val);
    }

    f.clear();
    CHECK_EQUAL(0, f.size());

    f.destroy();    // cleanup
}
TEST(ArrayFloat_AddManyValues)
{
    BasicArray_AddManyValues<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_AddManyValues)
{
    BasicArray_AddManyValues<ArrayDouble, double>(test_results);
}

template <class A, typename T>
void BasicArray_Delete(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();
    for (size_t i = 0; i < 5; ++i)
        f.add( T(i) );

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

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Delete)
{
    BasicArray_Delete<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Delete)
{
    BasicArray_Delete<ArrayDouble, double>(test_results);
}


template <class A, typename T>
void BasicArray_Set(TestResults& test_results, T values[], size_t num_values)
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

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Set)
{
    BasicArray_Set<ArrayFloat, float>(test_results, float_values, num_float_values);
}
TEST(ArrayDouble_Set)
{
    BasicArray_Set<ArrayDouble, double>(test_results, double_values, num_double_values);
}


template <class A, typename T>
void BasicArray_Insert(TestResults& test_results)
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

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Insert)
{
    BasicArray_Insert<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Insert)
{
    BasicArray_Insert<ArrayDouble, double>(test_results);
}

#if 0
// sum() is unused by other classes
template <class A, typename T>
void BasicArray_Sum(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();

    T values[] = { T(1.1), T(2.2), T(3.3), T(4.4), T(5.5)};
    double sum = 0.0;
    for (size_t i = 0; i < 5; ++i) {
        f.add(values[i]);
        sum += values[i];
    }
    CHECK_EQUAL(5, f.size());

    // all
    CHECK_EQUAL(sum, f.sum());
    // first
    CHECK_EQUAL(double(values[0]), f.sum(0, 1));
    // last
    CHECK_EQUAL(double(values[4]), f.sum(4, 5));
    // middle range
    CHECK_EQUAL(double(values[2]) + double(values[3]) + double(values[4]), f.sum(2));
    // single middle
    CHECK_EQUAL(double(values[2]), f.sum(2, 3));
    f.destroy();    // cleanup
}
TEST(ArrayFloat_Sum)
{
    BasicArray_Sum<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Sum)
{
    BasicArray_Sum<ArrayDouble, double>(test_results);
}
#endif

template <class A, typename T>
void BasicArray_Minimum(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();

    T res = T();

    CHECK_EQUAL(false, f.minimum(res));

    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(4.4)};
    for (size_t i = 0; i < 5; ++i)
        f.add(values[i]);
    CHECK_EQUAL(5, f.size());

    // middle match in all
    CHECK_EQUAL(true, f.minimum(res));
    CHECK_EQUAL(values[2], res);
    // first match
    CHECK_EQUAL(true, f.minimum(res, 0, 2));
    CHECK_EQUAL(values[0], res);
    // middle range, last match
    CHECK_EQUAL(true, f.minimum(res, 1, 3));
    CHECK_EQUAL(values[2], res);
    // single middle
    CHECK_EQUAL(true, f.minimum(res, 3, 4));
    CHECK_EQUAL(values[3], res);
    // first match in range
    CHECK_EQUAL(true, f.minimum(res, 3, size_t(-1)));
    CHECK_EQUAL(values[4], res);

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Minimum)
{
    BasicArray_Minimum<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Minimum)
{
    BasicArray_Minimum<ArrayDouble, double>(test_results);
}


template <class A, typename T>
void BasicArray_Maximum(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();

    T res = T();

    CHECK_EQUAL(false, f.maximum(res));

    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(4.4)};
    for (size_t i = 0; i < 5; ++i)
        f.add(values[i]);
    CHECK_EQUAL(5, f.size());

    // middle match in all
    CHECK_EQUAL(true, f.maximum(res));
    CHECK_EQUAL(values[3], res);
    // last match
    CHECK_EQUAL(true, f.maximum(res, 0, 2));
    CHECK_EQUAL(values[1], res);
    // middle range, last match
    CHECK_EQUAL(true, f.maximum(res, 1, 4));
    CHECK_EQUAL(values[3], res);
    // single middle
    CHECK_EQUAL(true, f.maximum(res, 3, 4));
    CHECK_EQUAL(values[3], res);
    // first match in range
    CHECK_EQUAL(true, f.maximum(res, 3, size_t(-1)));
    CHECK_EQUAL(values[3], res);

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Maximum)
{
    BasicArray_Maximum<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Maximum)
{
    BasicArray_Maximum<ArrayDouble, double>(test_results);
}


template <class A, typename T>
void BasicArray_Find(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();

    // Empty list
    CHECK_EQUAL(size_t(-1), f.find_first(0));

    // Add some values
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4) };
    for (size_t i = 0; i < 6; ++i)
        f.add(values[i]);

    // Find (full range: start=0, end=-1)
    CHECK_EQUAL(0, f.find_first(T(1.1)));
    CHECK_EQUAL(5, f.find_first(T(4.4)));
    CHECK_EQUAL(2, f.find_first(T(-1.0)));

    // non-existing
    CHECK_EQUAL(size_t(-1), f.find_first(T(0)));

    // various range limitations
    CHECK_EQUAL(1,          f.find_first(T(2.2), 1, 2));    // ok
    CHECK_EQUAL(1,          f.find_first(T(2.2), 1, 3));
    CHECK_EQUAL(5,          f.find_first(T(4.4), 1));       // defaul end=all
    CHECK_EQUAL(size_t(-1), f.find_first(T(2.2), 1, 1));    // start=end
    CHECK_EQUAL(size_t(-1), f.find_first(T(1.1), 1, 4));    // no match .end 1 too little
    CHECK_EQUAL(4,          f.find_first(T(1.1), 1, 5));    // skip first match, end at last match

    // Find all
    ref_type results_ref = Column::create(Allocator::get_default());
    Column results(Allocator::get_default(), results_ref);
    f.find_all(&results, T(1.1), 0);
    CHECK_EQUAL(2, results.size());
    CHECK_EQUAL(0, results.get(0));
    CHECK_EQUAL(4, results.get(1));
    // Find all, range limited -> no match
    results.clear();
    f.find_all(&results, T(1.1), 0, 1, 4);
    CHECK_EQUAL(0, results.size());
    results.destroy();

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Find)
{
    BasicArray_Find<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Find)
{
    BasicArray_Find<ArrayDouble, double>(test_results);
}


template <class A, typename T>
void BasicArray_Count(TestResults& test_results)
{
    A f(Allocator::get_default());
    f.create();

    // Empty list
    CHECK_EQUAL(0, f.count(0));

    // Add some values
    //                0       1        2       3       4       5
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4)};
    for (size_t i = 0; i < 6; ++i)
        f.add(values[i]);

    // count full range
    CHECK_EQUAL(0, f.count(T(0.0)));
    CHECK_EQUAL(1, f.count(T(4.4)));
    CHECK_EQUAL(1, f.count(T(-1.0)));
    CHECK_EQUAL(2, f.count(T(1.1)));

    // limited range
    CHECK_EQUAL(0, f.count(T(4.4), 0, 5));
    CHECK_EQUAL(1, f.count(T(4.4), 0, 6));
    CHECK_EQUAL(1, f.count(T(4.4), 5, 6));

    CHECK_EQUAL(0, f.count(T(-1.0), 1, 2));
    CHECK_EQUAL(0, f.count(T(-1.0), 3, 4));
    CHECK_EQUAL(1, f.count(T(-1.0), 2, 4));
    CHECK_EQUAL(1, f.count(T(-1.0), 1));

    f.destroy();    // cleanup
}
TEST(ArrayFloat_Count)
{
    BasicArray_Count<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Count)
{
    BasicArray_Count<ArrayDouble, double>(test_results);
}


template <class A, typename T>
void BasicArray_Compare(TestResults& test_results)
{
    A f1(Allocator::get_default()), f2(Allocator::get_default());
    f1.create();
    f2.create();

    // Empty list
    CHECK_EQUAL(true, f1.compare(f2));
    CHECK_EQUAL(true, f2.compare(f1));

    // Add some values
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4)};
    for (size_t i = 0; i < 6; ++i) {
        f1.add(values[i]);
        f2.add(values[i]);
        CHECK_EQUAL(true, f1.compare(f2));
    }

    f1.erase(0);
    CHECK_EQUAL(false, f1.compare(f2));

    f2.erase(0);
    CHECK_EQUAL(true, f1.compare(f2));

    f1.destroy();    // cleanup
    f2.destroy();
}
TEST(ArrayFloat_Compare)
{
    BasicArray_Compare<ArrayFloat, float>(test_results);
}
TEST(ArrayDouble_Compare)
{
    BasicArray_Compare<ArrayDouble, double>(test_results);
}

#endif // TEST_ARRAY_FLOAT
