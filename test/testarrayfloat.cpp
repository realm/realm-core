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

struct db_setup_float {
    static ArrayFloat f;
};
ArrayFloat db_setup_float::f;


// NOTE: Comapring floats is difficult. Strait comparison is usually wrong 
// unless you know the numbers excactly and the precision they are can represent.
// See also this article about comparing floats: 
// http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/

namespace {
float testval[] = {float(0.0),
                   float(1.0),
                   float(2.12345),
                   float(12345.12),
                   float(-12345.12)
                  };
const size_t testvalLen = SizeOfArray(testval);
}

TEST_FIXTURE(db_setup_float, ArrayFloat_AddGet)
{
    for (int i=0; i<testvalLen; ++i) {
        f.add(testval[i]);

        CHECK_EQUAL(i+1, f.Size());

        for (int j=0; j<i; ++j) {
            float val = f.Get(j);
            CHECK_EQUAL(testval[j], val);
        }
    }

    f.Clear();
    CHECK_EQUAL(0, f.Size());
}

TEST_FIXTURE(db_setup_float, ArrayFloat_AddManyValues)
{
    const size_t repeats = 1100;
    for (size_t i=0; i<repeats; ++i) {
        f.add(float(i));
        float val = f.Get(i);
        //printf("%d: %f\n", i, val);
        CHECK_EQUAL(i, val);
        CHECK_EQUAL(i+1, f.Size());
    }
    for (size_t i=0; i<repeats; ++i) {
        float val = f.Get(i);
        CHECK_EQUAL(i, val);
    }

    f.Clear();
    CHECK_EQUAL(0, f.Size());
}

TEST_FIXTURE(db_setup_float, ArrayFloat_Set)
{
    CHECK_EQUAL(0, f.Size());
    for (int i=0; i<testvalLen; ++i)
        f.add(testval[i]);
    CHECK_EQUAL(testvalLen, f.Size());

    f.Set(0, float(1.6));
    CHECK_EQUAL(float(1.6), f.Get(0));
    f.Set(3, float(987.23));
    CHECK_EQUAL(float(987.23), f.Get(3));

    CHECK_EQUAL(testval[1], f.Get(1));
    CHECK_EQUAL(testval[2], f.Get(2));
    CHECK_EQUAL(testval[4], f.Get(4));
}

TEST_FIXTURE(db_setup_float, ArrayFloat_Insert)
{
    f.Clear();
    CHECK(f.is_empty());
    
    // Insert in empty array
    f.Insert(0, 123.97f);
    CHECK_EQUAL(123.97f, f.Get(0));
    CHECK_EQUAL(1, f.Size());

    // Insert in top
    f.Insert(0, 321.97f);
    CHECK_EQUAL(321.97f, f.Get(0));
    CHECK_EQUAL(123.97f, f.Get(1));
    CHECK_EQUAL(2, f.Size());

    // Insert in middle
    f.Insert(1, 555.97f);
    CHECK_EQUAL(321.97f, f.Get(0));
    CHECK_EQUAL(555.97f, f.Get(1));
    CHECK_EQUAL(123.97f, f.Get(2));
    CHECK_EQUAL(3, f.Size());

    // Insert at buttom
    f.Insert(3, 999.97f);
    CHECK_EQUAL(321.97f, f.Get(0));
    CHECK_EQUAL(555.97f, f.Get(1));
    CHECK_EQUAL(123.97f, f.Get(2));
    CHECK_EQUAL(999.97f, f.Get(3));
    CHECK_EQUAL(4, f.Size());
}

TEST_FIXTURE(db_setup_float, ArrayFloat_Delete)
{
    f.Clear();
    CHECK_EQUAL(0, f.Size());

    f.add(float(1.1));
    f.add(float(2.2));
    f.add(float(3.3));
    f.add(float(4.4));
    f.add(float(5.5));
    CHECK_EQUAL(5, f.Size());
    CHECK_EQUAL(1.1f, f.Get(0));
    CHECK_EQUAL(2.2f, f.Get(1));
    CHECK_EQUAL(3.3f, f.Get(2));
    CHECK_EQUAL(4.4f, f.Get(3));
    CHECK_EQUAL(5.5f, f.Get(4));

    // Delete first
    f.Delete(0);
    CHECK_EQUAL(4, f.Size());
    CHECK_EQUAL(2.2f, f.Get(0));
    CHECK_EQUAL(3.3f, f.Get(1));
    CHECK_EQUAL(4.4f, f.Get(2));
    CHECK_EQUAL(5.5f, f.Get(3));

    // Delete middle
    f.Delete(2);
    CHECK_EQUAL(3, f.Size());
    CHECK_EQUAL(2.2f, f.Get(0));
    CHECK_EQUAL(3.3f, f.Get(1));
    CHECK_EQUAL(5.5f, f.Get(2));

    // Delete buttom
    f.Delete(2);
    CHECK_EQUAL(2, f.Size());
    CHECK_EQUAL(2.2f, f.Get(0));
    CHECK_EQUAL(3.3f, f.Get(1));
}



#if 1

struct db_setup_double {
    static ArrayDouble d;
};
ArrayDouble db_setup_double::d;


TEST_FIXTURE(db_setup_double, ArrayDoubleStoreRetrieveValues)
{
    double test[] = {double(0.0),
                     double(1.0),
                     double(2.12345),
                     double(12345.12),
                     double(-12345.12)
                    };

    for (int i=0; i<5; ++i) {
        d.add(test[i]);
    }
    CHECK_EQUAL(5, d.Size());

    for (int i=0; i<5; ++i) {
        double val = d.Get(i);
        CHECK_EQUAL(test[i], val);
    }

    d.Clear();
    CHECK_EQUAL(0, d.Size());
}

#endif