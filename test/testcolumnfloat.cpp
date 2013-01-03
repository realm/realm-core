#include <cstdio>
#include <UnitTest++.h>
#include <tightdb/column_float.hpp>

template <typename T, size_t N> inline
size_t SizeOfArray( const T(&)[ N ] )
{
  return N;
}

#if 1

using namespace tightdb;

struct db_setup_column_float {
    static ColumnFloat c;
};

ColumnFloat db_setup_column_float::c;

TEST_FIXTURE(db_setup_column_float, ColumnFloat_IsEmpty)
{
    CHECK(c.is_empty());
    CHECK_EQUAL(c.Size(), (size_t)0);
}

namespace {
float testval[] = {float(0.0),
                   float(1.0),
                   float(2.12345),
                   float(12345.12),
                   float(-12345.12)
                  };
const size_t testvalLen = SizeOfArray(testval);
}

TEST_FIXTURE(db_setup_column_float, ColumnFloat_AddGetValues)
{
    for (int i=0; i<testvalLen; ++i) {
        c.add(testval[i]);

        CHECK_EQUAL(i+1, c.Size());

        for (int j=0; j<i; ++j) {
            float val = c.Get(j);
            CHECK_EQUAL(testval[j], val);
        }
    }
}

TEST_FIXTURE(db_setup_column_float, ColumnFloat_Set)
{
    CHECK_EQUAL(testvalLen, c.Size());

    c.Set(0, float(1.6));
    CHECK_EQUAL(float(1.6), c.Get(0));
    c.Set(3, float(987.23));
    CHECK_EQUAL(float(987.23), c.Get(3));

    CHECK_EQUAL(testval[1], c.Get(1));
    CHECK_EQUAL(testval[2], c.Get(2));
    CHECK_EQUAL(testval[4], c.Get(4));
}

TEST_FIXTURE(db_setup_column_float, ColumnFloat_Clear)
{
    CHECK(!c.is_empty());
    c.Clear();
    CHECK(c.is_empty());
}
void printCol(ColumnFloat& c)
{
    for (int i=0; i < c.Size(); ++i) {
        fprintf(stderr, " Col[%d] = %f \n", i, c.Get(i));
    }
}


TEST_FIXTURE(db_setup_column_float, ColumnFloatInsert)
{
    CHECK(c.is_empty());
    
    // Insert in empty column
    c.Insert(0, 123.91f);
    CHECK_EQUAL(123.91f, c.Get(0));
    CHECK_EQUAL(1, c.Size());

    // Insert in top
    c.Insert(0, 321.93f);
    CHECK_EQUAL(321.93f, c.Get(0));
    CHECK_EQUAL(123.91f, c.Get(1));
    CHECK_EQUAL(2, c.Size());

    // Insert in middle
    c.Insert(1, 555.95f);
    CHECK_EQUAL(321.93f, c.Get(0));
    CHECK_EQUAL(555.95f, c.Get(1));
    CHECK_EQUAL(123.91f, c.Get(2));
    CHECK_EQUAL(3, c.Size());

    // Insert at buttom
    c.Insert(3, 999.99f);
    CHECK_EQUAL(321.93f, c.Get(0));
    CHECK_EQUAL(555.95f, c.Get(1));
    CHECK_EQUAL(123.91f, c.Get(2));
    CHECK_EQUAL(999.99f, c.Get(3));
    CHECK_EQUAL(4, c.Size());   

    // Insert at top
    c.Insert(0, 888.98f);
    CHECK_EQUAL(888.98f, c.Get(0));
    CHECK_EQUAL(321.93f, c.Get(1));
    CHECK_EQUAL(555.95f, c.Get(2));
    CHECK_EQUAL(123.91f, c.Get(3));
    CHECK_EQUAL(999.99f, c.Get(4));
    CHECK_EQUAL(5, c.Size());   
}


TEST_FIXTURE(db_setup_column_float, ColumnFloatDelete)
{
    c.Clear();
    for (int i=0; i<5; ++i)
        c.add(testval[i]);
    CHECK_EQUAL(5, c.Size());
    CHECK_EQUAL(testval[0], c.Get(0));
    CHECK_EQUAL(testval[1], c.Get(1));
    CHECK_EQUAL(testval[2], c.Get(2));
    CHECK_EQUAL(testval[3], c.Get(3));
    CHECK_EQUAL(testval[4], c.Get(4));

    // Delete first
    c.Delete(0);
    CHECK_EQUAL(4, c.Size());
    CHECK_EQUAL(testval[1], c.Get(0));
    CHECK_EQUAL(testval[2], c.Get(1));
    CHECK_EQUAL(testval[3], c.Get(2));
    CHECK_EQUAL(testval[4], c.Get(3));

    // Delete middle
    c.Delete(2);
    CHECK_EQUAL(3, c.Size());
    CHECK_EQUAL(testval[1], c.Get(0));
    CHECK_EQUAL(testval[2], c.Get(1));
    CHECK_EQUAL(testval[4], c.Get(2));

    // Delete last
    c.Delete(2);
    CHECK_EQUAL(2, c.Size());
    CHECK_EQUAL(testval[1], c.Get(0));
    CHECK_EQUAL(testval[2], c.Get(1));

    // Delete single
    c.Delete(0);
    CHECK_EQUAL(1, c.Size());
    CHECK_EQUAL(testval[2], c.Get(0));

    // Delete all
    c.Delete(0);
    CHECK_EQUAL(0, c.Size());
}


// clean up (ALWAYS PUT THIS LAST)
TEST_FIXTURE(db_setup_column_float, ColumnFloat_Destroy)
{
    c.Destroy();
}

#endif