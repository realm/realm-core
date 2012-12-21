#include <UnitTest++.h>
#include <tightdb/array_float.hpp>
#include <tightdb/column.hpp>

using namespace tightdb;

struct db_setup_float {
    static ArrayFloat c;
};

ArrayFloat db_setup_float::c;

// NOTE: Comapring floats is difficult. Strait comparison is usually wrong 
// unless you know the numbers excactly and the precision they are can represent.
// See also this article about comparing floats: 
// http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/


TEST_FIXTURE(db_setup_float, ArrayFloatStoreRetrieveValues)
{
    float f[] = {float(0.0),
                 float(1.0),
                 float(2.12345),
                 float(12345.12),
                 float(-12345.12)
                };

    for (int i=0; i<5; ++i) {
        c.add(f[i]);
    }
    CHECK_EQUAL(5, c.Size());

    for (int i=0; i<5; ++i) {
        float val = c.Get(i);
        CHECK_EQUAL(f[i], val);
    }

    c.Clear();
    CHECK_EQUAL(0, c.Size());
}

TEST_FIXTURE(db_setup_float, ArrayFloatStoreManyValues)
{
    const size_t repeats = 1100;
    for (size_t i=0; i<repeats; ++i) {
        c.add(i);
        float val = c.Get(i);
        //printf("%d: %f\n", i, val);
        CHECK_EQUAL(i, val);
        CHECK_EQUAL(i+1, c.Size());
    }
    for (size_t i=0; i<repeats; ++i) {
        float val = c.Get(i);
        CHECK_EQUAL(i, val);
    }

    c.Clear();
    CHECK_EQUAL(0, c.Size());
}

// FIXME: Test Set(), test Delete()