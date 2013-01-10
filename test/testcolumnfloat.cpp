#include <cstdio>
#include <UnitTest++.h>
#include <tightdb/column_float.hpp>
#include <tightdb/column_double.hpp>

template <typename T, size_t N> inline
size_t SizeOfArray( const T(&)[ N ] )
{
  return N;
}

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

void printCol(ColumnFloat& c)
{
    for (int i=0; i < c.Size(); ++i) {
        fprintf(stderr, " Col[%d] = %f \n", i, c.Get(i));
    }
}

using namespace tightdb;


template <class C>
void ColumnBasic_IsEmpty()
{
    C c;
    CHECK(c.is_empty());
    CHECK_EQUAL(c.Size(), (size_t)0);
}
TEST(ColumnFloat_IsEmpty) { ColumnBasic_IsEmpty<ColumnFloat>(); }
TEST(ColumnDouble_IsEmpty){ ColumnBasic_IsEmpty<ColumnDouble>(); }


template <class C, typename T>
void ColumnBasic_AddGet(T val[], size_t valLen)
{
    C c;
    for (int i=0; i<valLen; ++i) {
        c.add(val[i]);

        CHECK_EQUAL(i+1, c.Size());

        for (int j=0; j<i; ++j) {
            CHECK_EQUAL(val[j], c.Get(j));
        }
    }
}
TEST(ColumnFloat_AddGet) { ColumnBasic_AddGet<ColumnFloat, float>(floatVal, floatValLen); }
TEST(ColumnDouble_AddGet){ ColumnBasic_AddGet<ColumnDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void ColumnBasic_Clear()
{
    C c;
    CHECK(c.is_empty());

    for (size_t i=0; i<100; ++i)
        c.add();
    CHECK(!c.is_empty());

    c.Clear();
    CHECK(c.is_empty());
}
TEST(ColumnFloat_Clear) { ColumnBasic_Clear<ColumnFloat, float>(); }
TEST(ColumnDouble_Clear){ ColumnBasic_Clear<ColumnDouble, double>(); }


template <class C, typename T>
void ColumnBasic_Set(T val[], size_t valLen)
{
    C c;
    for (int i=0; i<valLen; ++i)
        c.add(val[i]);
    CHECK_EQUAL(valLen, c.Size());
    
    T v0 = T(1.6);
    T v3 = T(-987.23);
    c.Set(0, v0);
    CHECK_EQUAL(v0, c.Get(0));
    c.Set(3, v3);
    CHECK_EQUAL(v3, c.Get(3));

    CHECK_EQUAL(val[1], c.Get(1));
    CHECK_EQUAL(val[2], c.Get(2));
    CHECK_EQUAL(val[4], c.Get(4));
}
TEST(ColumnFloat_Set) { ColumnBasic_Set<ColumnFloat, float>(floatVal, floatValLen); }
TEST(ColumnDouble_Set){ ColumnBasic_Set<ColumnDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void ColumnBasic_Insert(T val[], size_t valLen)
{
    C c;
    
    // Insert in empty column
    c.Insert(0, val[0]);
    CHECK_EQUAL(val[0], c.Get(0));
    CHECK_EQUAL(1, c.Size());

    // Insert in top
    c.Insert(0, val[1]);
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[0], c.Get(1));
    CHECK_EQUAL(2, c.Size());

    // Insert in middle
    c.Insert(1, val[2]);
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[2], c.Get(1));
    CHECK_EQUAL(val[0], c.Get(2));
    CHECK_EQUAL(3, c.Size());

    // Insert at buttom
    c.Insert(3, val[3]);
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[2], c.Get(1));
    CHECK_EQUAL(val[0], c.Get(2));
    CHECK_EQUAL(val[3], c.Get(3));
    CHECK_EQUAL(4, c.Size());   

    // Insert at top
    c.Insert(0, val[4]);
    CHECK_EQUAL(val[4], c.Get(0));
    CHECK_EQUAL(val[1], c.Get(1));
    CHECK_EQUAL(val[2], c.Get(2));
    CHECK_EQUAL(val[0], c.Get(3));
    CHECK_EQUAL(val[3], c.Get(4));
    CHECK_EQUAL(5, c.Size());   
}
TEST(ColumnFloat_Insert) { ColumnBasic_Insert<ColumnFloat, float>(floatVal, floatValLen); }
TEST(ColumnDouble_Insert){ ColumnBasic_Insert<ColumnDouble, double>(doubleVal, doubleValLen); }


template <class C, typename T>
void ColumnBasic_Delete(T val[], size_t valLen)
{
    C c;
    for (int i=0; i<valLen; ++i)
        c.add(val[i]);
    CHECK_EQUAL(5, c.Size());
    CHECK_EQUAL(val[0], c.Get(0));
    CHECK_EQUAL(val[1], c.Get(1));
    CHECK_EQUAL(val[2], c.Get(2));
    CHECK_EQUAL(val[3], c.Get(3));
    CHECK_EQUAL(val[4], c.Get(4));

    // Delete first
    c.Delete(0);
    CHECK_EQUAL(4, c.Size());
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[2], c.Get(1));
    CHECK_EQUAL(val[3], c.Get(2));
    CHECK_EQUAL(val[4], c.Get(3));

    // Delete middle
    c.Delete(2);
    CHECK_EQUAL(3, c.Size());
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[2], c.Get(1));
    CHECK_EQUAL(val[4], c.Get(2));

    // Delete last
    c.Delete(2);
    CHECK_EQUAL(2, c.Size());
    CHECK_EQUAL(val[1], c.Get(0));
    CHECK_EQUAL(val[2], c.Get(1));

    // Delete single
    c.Delete(0);
    CHECK_EQUAL(1, c.Size());
    CHECK_EQUAL(val[2], c.Get(0));

    // Delete all
    c.Delete(0);
    CHECK_EQUAL(0, c.Size());
}
TEST(ColumnFloat_Delete) { ColumnBasic_Delete<ColumnFloat, float>(floatVal, floatValLen); }
TEST(ColumnDouble_Delete){ ColumnBasic_Delete<ColumnDouble, double>(doubleVal, doubleValLen); }

