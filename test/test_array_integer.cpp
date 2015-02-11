#include "testsettings.hpp"

#include <tightdb/array_integer.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::test_util;


TEST(ArrayInteger_Sort)
{
    // Create array with random values
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.add(25);
    a.add(12);
    a.add(50);
    a.add(3);
    a.add(34);
    a.add(0);
    a.add(17);
    a.add(51);
    a.add(2);
    a.add(40);

    a.sort();

    CHECK_EQUAL(0, a.get(0));
    CHECK_EQUAL(2, a.get(1));
    CHECK_EQUAL(3, a.get(2));
    CHECK_EQUAL(12, a.get(3));
    CHECK_EQUAL(17, a.get(4));
    CHECK_EQUAL(25, a.get(5));
    CHECK_EQUAL(34, a.get(6));
    CHECK_EQUAL(40, a.get(7));
    CHECK_EQUAL(50, a.get(8));
    CHECK_EQUAL(51, a.get(9));

    // Cleanup
    a.destroy();
}

TEST(ArrayInteger_Sort1)
{
    // negative values
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 400; ++t)
        a.add(random.draw_int(-100, 199));

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}


TEST(ArrayInteger_Sort2)
{
    // 64 bit values
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 400; ++t) {
        int_fast64_t v = 1;
        for (int i = 0; i != 8; ++i)
            v *= int64_t(random.draw_int_max(RAND_MAX));
        a.add(v);
    }

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}

TEST(ArrayInteger_Sort3)
{
    // many values
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    Random random(random_int<unsigned long>()); // Seed from slow global generator
    for (size_t t = 0; t < 1000; ++t)
        a.add(random.draw_int_max(200)); // 200 will make some duplicate values which is good

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) >= a.get(t - 1));

    a.destroy();
}


TEST(ArrayInteger_Sort4)
{
    // same values
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    for (size_t t = 0; t < 1000; ++t)
        a.add(0);

    size_t orig_size = a.size();
    a.sort();

    CHECK(a.size() == orig_size);
    for (size_t t = 1; t < a.size(); ++t)
        CHECK(a.get(t) == 0);

    a.destroy();
}

TEST(ArrayInteger_Sum0)
{
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    for (int i = 0; i < 64 + 7; ++i) {
        a.add(0);
    }
    CHECK_EQUAL(0, a.sum(0, a.size()));
    a.destroy();
}

TEST(ArrayInteger_Sum1)
{
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    int64_t s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 2);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(ArrayInteger_Sum2)
{
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    int64_t s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 4);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}


TEST(ArrayInteger_Sum4)
{
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    int64_t s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 16);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(ArrayInteger_Sum16)
{
    ArrayInteger a(Allocator::get_default());
    a.create(Array::type_Normal);

    int64_t s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        a.add(i % 30000);

    s1 = 0;
    for (int i = 0; i < 256 + 7; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(0, a.size()));

    s1 = 0;
    for (int i = 3; i < 100; ++i)
        s1 += a.get(i);
    CHECK_EQUAL(s1, a.sum(3, 100));

    a.destroy();
}

TEST(ArrayIntNull_SetNull) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.add(0);
    a.set_null(0);
    CHECK(a.is_null(0));

    a.add(128);
    CHECK(a.is_null(0));

    a.add(120000);
    CHECK(a.is_null(0));

    a.destroy();
}

TEST(ArrayIntNull_SetIntegerToPreviousNullValueChoosesNewNull) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.add(126);
    // NULL value should be 127
    a.add(0);
    a.set_null(1);
    a.set(0, 127);
    // array should be upgraded now
    CHECK(a.is_null(1));

    a.add(1000000000000); // upgrade to 64-bit, null should now be a "random" value
    CHECK(a.is_null(1));
    int64_t old_null = a.null_value();
    a.add(old_null);
    CHECK(a.is_null(1));
    CHECK_NOT_EQUAL(a.null_value(), old_null);

    a.destroy();
}

