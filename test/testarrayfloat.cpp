#include <UnitTest++.h>
#include <tightdb/array_basic.hpp>
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

// TODO: Add test of full range of floats.

template <class C, typename T>
void BasicArray_AddGet(T val[], size_t valLen)
{
    C f;
    for (size_t i=0; i<valLen; ++i) {
        f.add(val[i]);

        CHECK_EQUAL(i+1, f.Size());

        for (size_t j=0; j<i; ++j) {
            CHECK_EQUAL(val[j], f.Get(j));
        }
    }

    f.Clear();
    CHECK_EQUAL(0, f.Size());
}
TEST(ArrayFloat_AddGet) { BasicArray_AddGet<ArrayFloat, float>(floatVal, floatValLen); }
TEST(ArrayDouble_AddGet){ BasicArray_AddGet<ArrayDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void BasicArray_AddManyValues()
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
TEST(ArrayFloat_AddManyValues) { BasicArray_AddManyValues<ArrayFloat, float>(); }
TEST(ArrayDouble_AddManyValues){ BasicArray_AddManyValues<ArrayDouble, double>(); }

template <class C, typename T>
void BasicArray_Delete()
{
    C f;
    for (size_t i=0; i<5; ++i)
        f.add( T(i) );

    // Delete first
    f.Delete(0);
    CHECK_EQUAL(4, f.Size());
    CHECK_EQUAL(1, f.Get(0));
    CHECK_EQUAL(2, f.Get(1));
    CHECK_EQUAL(3, f.Get(2));
    CHECK_EQUAL(4, f.Get(3));

    // Delete last
    f.Delete(3);
    CHECK_EQUAL(3, f.Size());
    CHECK_EQUAL(1, f.Get(0));
    CHECK_EQUAL(2, f.Get(1));
    CHECK_EQUAL(3, f.Get(2));
        
    // Delete middle
    f.Delete(1);
    CHECK_EQUAL(2, f.Size());
    CHECK_EQUAL(1, f.Get(0));
    CHECK_EQUAL(3, f.Get(1));

    // Delete all
    f.Delete(0);
    CHECK_EQUAL(1, f.Size());
    CHECK_EQUAL(3, f.Get(0));
    f.Delete(0);
    CHECK_EQUAL(0, f.Size());
    CHECK(f.is_empty());
}
TEST(ArrayFloat_Delete) { BasicArray_Delete<ArrayFloat, float>(); }
TEST(ArrayDouble_Delete){ BasicArray_Delete<ArrayDouble, double>(); }


template <class C, typename T>
void BasicArray_Set(T val[], size_t valLen)
{
    C f;
    CHECK_EQUAL(0, f.Size());
    for (size_t i=0; i<valLen; ++i)
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
TEST(ArrayFloat_Set) { BasicArray_Set<ArrayFloat, float>(floatVal, floatValLen); }
TEST(ArrayDouble_Set){ BasicArray_Set<ArrayDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void BasicArray_Insert()
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
TEST(ArrayFloat_Insert) { BasicArray_Insert<ArrayFloat, float>(); }
TEST(ArrayDouble_Insert){ BasicArray_Insert<ArrayDouble, double>(); }

#if 0
// sum() is unused by other classes
template <class C, typename T>
void BasicArray_Sum()
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
TEST(ArrayFloat_Sum) { BasicArray_Sum<ArrayFloat, float>(); }
TEST(ArrayDouble_Sum){ BasicArray_Sum<ArrayDouble, double>(); }
#endif

template <class C, typename T>
void BasicArray_Minimum()
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
TEST(ArrayFloat_Minimum) { BasicArray_Minimum<ArrayFloat, float>(); }
TEST(ArrayDouble_Minimum){ BasicArray_Minimum<ArrayDouble, double>(); }


template <class C, typename T>
void BasicArray_Maximum()
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
TEST(ArrayFloat_Maximum) { BasicArray_Maximum<ArrayFloat, float>(); }
TEST(ArrayDouble_Maximum){ BasicArray_Maximum<ArrayDouble, double>(); }


template <class C, typename T>
void BasicArray_Find()
{
    C f;

    // Empty list
    CHECK_EQUAL(-1, f.find_first(0));
    
    // Add some values
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4)};
    for (size_t i=0; i<6; ++i) {
        f.add(values[i]);
    }

    // Find (full range: start=0, end=-1)
    CHECK_EQUAL(0, f.find_first(T(1.1)));
    CHECK_EQUAL(5, f.find_first(T(4.4)));
    CHECK_EQUAL(2, f.find_first(T(-1.0)));

    // non-existing
    CHECK_EQUAL(-1, f.find_first(T(0)));
   
    // various range limitations
    CHECK_EQUAL( 1, f.find_first(T(2.2), 1, 2));    // ok
    CHECK_EQUAL( 1, f.find_first(T(2.2), 1, 3));    
    CHECK_EQUAL( 5, f.find_first(T(4.4), 1));       // defaul end=all
    CHECK_EQUAL(-1, f.find_first(T(2.2), 1, 1));    // start=end
    CHECK_EQUAL(-1, f.find_first(T(1.1), 1, 4));    // no match .end 1 too little
    CHECK_EQUAL( 4, f.find_first(T(1.1), 1, 5));    // skip first match, end at last match

    // Find all
    Array resArr;
    f.find_all(resArr, T(1.1), 0);
    CHECK_EQUAL(2, resArr.Size());
    CHECK_EQUAL(0, resArr.Get(0));
    CHECK_EQUAL(4, resArr.Get(1));
    // Find all, range limited -> no match
    resArr.Clear();
    f.find_all(resArr, T(1.1), 0, 1, 4);
    CHECK_EQUAL(0, resArr.Size());

}
TEST(ArrayFloat_Find) { BasicArray_Find<ArrayFloat, float>(); }
TEST(ArrayDouble_Find){ BasicArray_Find<ArrayDouble, double>(); }


template <class C, typename T>
void BasicArray_Count()
{
    C f;

    // Empty list
    CHECK_EQUAL(0, f.count(0));
    
    // Add some values
    //                0       1        2       3       4       5
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4)};
    for (size_t i=0; i<6; ++i) {
        f.add(values[i]);
    }
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

}
TEST(ArrayFloat_Count) { BasicArray_Count<ArrayFloat, float>(); }
TEST(ArrayDouble_Count){ BasicArray_Count<ArrayDouble, double>(); }


template <class C, typename T>
void BasicArray_Compare()
{
    C f1, f2;

    // Empty list
    CHECK_EQUAL(true, f1.Compare(f2));
    CHECK_EQUAL(true, f2.Compare(f1));
    
    // Add some values
    T values[] = { T(1.1), T(2.2), T(-1.0), T(5.5), T(1.1), T(4.4)};
    for (size_t i=0; i<6; ++i) {
        f1.add(values[i]);
        f2.add(values[i]);
        CHECK_EQUAL(true, f1.Compare(f2));
    }

    f1.Delete(0);
    CHECK_EQUAL(false, f1.Compare(f2));

    f2.Delete(0);
    CHECK_EQUAL(true, f1.Compare(f2));
}
TEST(ArrayFloat_Compare) { BasicArray_Compare<ArrayFloat, float>(); }
TEST(ArrayDouble_Compare){ BasicArray_Compare<ArrayDouble, double>(); }

