#include "testsettings.hpp"

#include <tightdb/array_integer.hpp>

#include "test.hpp"

using namespace std;
using namespace realm;
using namespace realm::test_util;

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
