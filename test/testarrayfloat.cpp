#include <UnitTest++.h>
#include <tightdb/array_float.hpp>
#include <tightdb/array_double.hpp>
#include <tightdb/column.hpp>

template <typename T, size_t N> inline
size_t SizeOfArray( const T(&)[ N ] )
{
  return N;
}

using namespace tightdb;

// Article about comparing floats:
// http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/

namespace {
float floatVal[] = {0.0f,
                   1.0f,
                   2.12345f,
                   12345.12f,
                   -12345.12f
                  };
const size_t floatValLen = SizeOfArray(floatVal);

double doubleVal[] = {0.0,
                      1.0,
                      2.12345,
                      12345.12,
                      -12345.12
                     };
const size_t doubleValLen = SizeOfArray(doubleVal);
}

// Add test of full range of floats.

template <class C, typename T>
void ArrayBasic_AddGet(T val[], size_t valLen)
{
    C f;
    for (int i=0; i<valLen; ++i) {
        f.add(val[i]);

        CHECK_EQUAL(i+1, f.Size());

        for (int j=0; j<i; ++j) {
            CHECK_EQUAL(val[j], f.Get(j));
        }
    }

    f.Clear();
    CHECK_EQUAL(0, f.Size());
}
TEST(ArrayFloat_AddGet) { ArrayBasic_AddGet<ArrayFloat, float>(floatVal, floatValLen); }
TEST(ArrayDouble_AddGet){ ArrayBasic_AddGet<ArrayDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void ArrayBasic_AddManyValues()
{
    C f;
    const size_t repeats = 1100;
    for (size_t i=0; i<repeats; ++i) {
        f.add(T(i));
        T val = f.Get(i);
        CHECK_EQUAL(T(i), val);
        CHECK_EQUAL(i+1, f.Size());
    }
    for (size_t i=0; i<repeats; ++i) {
        T val = f.Get(i);
        CHECK_EQUAL(T(i), val);
    }

    f.Clear();
    CHECK_EQUAL(0, f.Size());
}
TEST(ArrayFloat_AddManyValues) { ArrayBasic_AddManyValues<ArrayFloat, float>(); }
TEST(ArrayDouble_AddManyValues){ ArrayBasic_AddManyValues<ArrayDouble, double>(); }


template <class C, typename T>
void ArrayBasic_Set(T val[], size_t valLen)
{
    C f;
    CHECK_EQUAL(0, f.Size());
    for (int i=0; i<valLen; ++i)
        f.add(val[i]);
    CHECK_EQUAL(valLen, f.Size());

    f.Set(0, T(1.6));
    CHECK_EQUAL(T(1.6), f.Get(0));
    f.Set(3, T(987.23));
    CHECK_EQUAL(T(987.23), f.Get(3));

    CHECK_EQUAL(val[1], f.Get(1));
    CHECK_EQUAL(val[2], f.Get(2));
    CHECK_EQUAL(val[4], f.Get(4));
    CHECK_EQUAL(valLen, f.Size());
}
TEST(ArrayFloat_Set) { ArrayBasic_Set<ArrayFloat, float>(floatVal, floatValLen); }
TEST(ArrayDouble_Set){ ArrayBasic_Set<ArrayDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void ArrayBasic_Insert(T val[])
{
    C f;
    const T v0 = T(123.970);
    const T v1 = T(-321.971);
    T v2 = T(555.972);
    T v3 = T(-999.973);

    // Insert in empty array
    f.Insert(0, v0);
    CHECK_EQUAL(v0, f.Get(0));
    CHECK_EQUAL(1, f.Size());

    // Insert in top
    f.Insert(0, v1);
    CHECK_EQUAL(v1, f.Get(0));
    CHECK_EQUAL(v0, f.Get(1));
    CHECK_EQUAL(2, f.Size());

    // Insert in middle
    f.Insert(1, v2);
    CHECK_EQUAL(v1, f.Get(0));
    CHECK_EQUAL(v2, f.Get(1));
    CHECK_EQUAL(v0, f.Get(2));
    CHECK_EQUAL(3, f.Size());

    // Insert at buttom
    f.Insert(3, v3);
    CHECK_EQUAL(v1, f.Get(0));
    CHECK_EQUAL(v2, f.Get(1));
    CHECK_EQUAL(v0, f.Get(2));
    CHECK_EQUAL(v3, f.Get(3));
    CHECK_EQUAL(4, f.Size());
}
TEST(ArrayFloat_Insert) { ArrayBasic_Insert<ArrayFloat, float>(floatVal); }
TEST(ArrayDouble_Insert){ ArrayBasic_Insert<ArrayDouble, double>(doubleVal); }


template <class C, typename T>
void ArrayBasic_Delete(T val[])
{
    C f;
    for (int i=0; i<5; ++i)
        f.add(val[i]);

    // Delete first
    f.Delete(0);
    CHECK_EQUAL(4, f.Size());
    CHECK_EQUAL(val[1], f.Get(0));
    CHECK_EQUAL(val[2], f.Get(1));
    CHECK_EQUAL(val[3], f.Get(2));
    CHECK_EQUAL(val[4], f.Get(3));

    // Delete middle
    f.Delete(2);
    CHECK_EQUAL(3, f.Size());
    CHECK_EQUAL(val[1], f.Get(0));
    CHECK_EQUAL(val[2], f.Get(1));
    CHECK_EQUAL(val[4], f.Get(2));

    // Delete buttom
    f.Delete(2);
    CHECK_EQUAL(2, f.Size());
    CHECK_EQUAL(val[1], f.Get(0));
    CHECK_EQUAL(val[2], f.Get(1));
}
TEST(ArrayFloat_Delete) { ArrayBasic_Delete<ArrayFloat, float>(floatVal); }
TEST(ArrayDouble_Delete){ ArrayBasic_Delete<ArrayDouble, double>(doubleVal); }


template <class C, typename T>
void ArrayBasic_Sum(T val[])
{
    C f;

    T values[] = { T(1.1), T(2.2), T(3.3), T(4.4), T(5.5)};
    double sum = 0.0;
    for (size_t i=0; i<5; ++i) {
        f.add(values[i]);
        sum += values[i];
    }
    CHECK_EQUAL(5, f.Size());

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
}
TEST(ArrayFloat_Sum) { ArrayBasic_Sum<ArrayFloat, float>(floatVal); }
TEST(ArrayDouble_Sum){ ArrayBasic_Sum<ArrayDouble, double>(doubleVal); }


template <class C, typename T>
void ArrayBasic_Minimum(T val[])
{
    C f;
    T res;

    CHECK_EQUAL(false, f.minimum(res));

    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(4.4)};
    for (size_t i=0; i<5; ++i) {
        f.add(values[i]);
    }
    CHECK_EQUAL(5, f.Size());

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
}
TEST(ArrayFloat_Minimum) { ArrayBasic_Minimum<ArrayFloat, float>(floatVal); }
TEST(ArrayDouble_Minimum){ ArrayBasic_Minimum<ArrayDouble, double>(doubleVal); }


template <class C, typename T>
void ArrayBasic_Maximum(T val[])
{
    C f;
    T res;

    CHECK_EQUAL(false, f.maximum(res));

    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(4.4)};
    for (size_t i=0; i<5; ++i) {
        f.add(values[i]);
    }
    CHECK_EQUAL(5, f.Size());

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
}
TEST(ArrayFloat_Maximum) { ArrayBasic_Maximum<ArrayFloat, float>(floatVal); }
TEST(ArrayDouble_Maximum){ ArrayBasic_Maximum<ArrayDouble, double>(doubleVal); }

// TODO: count, find, fird_first, find_all, Compare