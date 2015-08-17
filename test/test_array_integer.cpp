#include "testsettings.hpp"

#include <limits>

#include <realm/array_integer.hpp>
#include <realm/column.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;

TEST_TYPES(ArrayInteger_Sum0, ArrayInteger, ArrayIntNull)
{
    TEST_TYPE a(Allocator::get_default());
    a.create(Array::type_Normal);

    for (int i = 0; i < 64 + 7; ++i) {
        a.add(0);
    }
    CHECK_EQUAL(0, a.sum(0, a.size()));
    a.destroy();
}

TEST_TYPES(ArrayInteger_Sum1, ArrayInteger, ArrayIntNull)
{
    TEST_TYPE a(Allocator::get_default());
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

TEST_TYPES(ArrayInteger_Sum2, ArrayInteger, ArrayIntNull)
{
    TEST_TYPE a(Allocator::get_default());
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


TEST_TYPES(ArrayInteger_Sum4, ArrayInteger, ArrayIntNull)
{
    TEST_TYPE a(Allocator::get_default());
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

TEST_TYPES(ArrayInteger_Sum16, ArrayInteger, ArrayIntNull)
{
    TEST_TYPE a(Allocator::get_default());
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

TEST(ArrayIntNull_InitFromTruncatedRef)
{
    // This is used when clearing/truncating the B+tree.

    Array inner_node(Allocator::get_default());
    inner_node.create(Array::type_Normal);
    inner_node.add(123);
    inner_node.add(456);

    inner_node.clear();
    CHECK_EQUAL(0, inner_node.size());

    ArrayIntNull new_leaf(Allocator::get_default());
    new_leaf.init_from_ref(inner_node.get_ref()); // ownership transferred
    CHECK_EQUAL(0, new_leaf.size());
    new_leaf.destroy();
}

TEST(ArrayIntNull_InitFromParent)
{
    Array inner_node(Allocator::get_default());
    inner_node.create(Array::type_HasRefs);

    {
        ArrayIntNull leaf(Allocator::get_default());
        leaf.create(Array::type_Normal);
        leaf.add(123);
        inner_node.add(0);
        inner_node.set_as_ref(0, leaf.get_ref());
    }

    ArrayIntNull leaf2(Allocator::get_default());
    leaf2.set_parent(&inner_node, 0);
    leaf2.init_from_parent();
    CHECK_EQUAL(123, leaf2.get(0));
    inner_node.clear_and_destroy_children();
    inner_node.destroy();
}

TEST(ArrayIntNull_SetNull) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.add(0);
    CHECK(!a.is_null(0));
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

    a.add(1000000000000LL); // upgrade to 64-bit, null should now be a "random" value
    CHECK(a.is_null(1));
    int64_t old_null = a.null_value();
    a.add(old_null);
    CHECK(a.is_null(1));
    CHECK_NOT_EQUAL(a.null_value(), old_null);

    a.destroy();
}

TEST(ArrayIntNull_Boundaries) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);
    a.add(0);
    a.set_null(0);
    a.add(0);
    CHECK(a.is_null(0));
    CHECK(!a.is_null(1));
    CHECK_EQUAL(a.get_width(), 1); // not sure if this should stay. Makes assumtions about implementation details.


    // consider turning this into a array + loop
    a.add(0);
    CHECK_EQUAL(0, a.back());
    CHECK(a.is_null(0));

    a.add(1);
    CHECK_EQUAL(1, a.back());
    CHECK(a.is_null(0));

    a.add(3);
    CHECK_EQUAL(3, a.back());
    CHECK(a.is_null(0));

    a.add(15);
    CHECK_EQUAL(15, a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int8_t>::max());
    CHECK_EQUAL(std::numeric_limits<int8_t>::max(), a.back());
    CHECK(a.is_null(0));

    a.add(std::numeric_limits<int8_t>::min());
    CHECK_EQUAL(std::numeric_limits<int8_t>::min(), a.back());
    CHECK(a.is_null(0));

    a.add(std::numeric_limits<uint8_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint8_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int16_t>::max());
    CHECK_EQUAL(std::numeric_limits<int16_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int16_t>::min());
    CHECK_EQUAL(std::numeric_limits<int16_t>::min(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<uint16_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint16_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int32_t>::max());
    CHECK_EQUAL(std::numeric_limits<int32_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int32_t>::min());
    CHECK_EQUAL(std::numeric_limits<int32_t>::min(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<uint32_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint32_t>::max(), a.back());
    CHECK(a.is_null(0));


    a.add(std::numeric_limits<int_fast64_t>::max());
    CHECK_EQUAL(std::numeric_limits<int_fast64_t>::max(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<int_fast64_t>::min());
    CHECK_EQUAL(std::numeric_limits<int_fast64_t>::min(), a.back());
    CHECK(a.is_null(0));
    a.add(std::numeric_limits<uint_fast64_t>::max());
    CHECK_EQUAL(std::numeric_limits<uint_fast64_t>::max(), a.get_uint(a.size()-1));
    CHECK(a.is_null(0));


    a.destroy();
}

TEST(ArrayIntNull_SetUint0) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);
    a.add(0);
    a.add(0);

    a.set_uint(0, 0);
    a.set_null(1);

    CHECK(!a.is_null(0));
    CHECK(a.is_null(1));

    a.destroy();
}

// Test if allocator relocation preserves null and non-null
TEST(ArrayIntNull_Relocate) {
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);

    // Enforce 64 bits and hence use magic value
    a.add(0x1000000000000000LL);
    a.add(0);
    a.set_null(1);

    // Add values until relocation has happend multiple times (80 kilobyte payload in total)
    for (size_t t = 0; t < 10000; t++)
        a.add(0);

    CHECK(!a.is_null(0));
    CHECK(a.is_null(1));
    a.destroy();
}

TEST(ArrayIntNull_Find)
{
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);

    a.clear();
    for (size_t i = 0; i < 100; ++i) {
        a.add(0x33);
    }
    a.add(0x100);
    a.set(50, 0x44);
    a.set(60, 0x44);
    a.set_null(51);

    size_t t0 = a.find_first<NotEqual>(0x33);
    CHECK_EQUAL(50, t0);

    size_t t1 = a.find_first<NotEqual>(0x33, 0, 50);
    CHECK_EQUAL(not_found, t1);

    size_t t2 = a.find_first(0x44);
    CHECK_EQUAL(50, t2);

    size_t t3 = a.find_first(0);
    CHECK_EQUAL(not_found, t3);

    int64_t t4;
    a.minimum(t4);
    CHECK_EQUAL(0x33, t4);

    int64_t t5;
    a.maximum(t5);
    CHECK_EQUAL(0x100, t5);

    int64_t t6;
    size_t i6;
    bool found = a.maximum(t6, 0 , npos, &i6);
    CHECK_EQUAL(100, i6);
    CHECK_EQUAL(0x100, t6);
    CHECK_EQUAL(found, true);

    {
        ref_type column_ref = IntegerColumn::create(Allocator::get_default());
        IntegerColumn col(Allocator::get_default(), column_ref);

        a.find_all(&col, 0x44);

        CHECK_EQUAL(2, col.size());
        CHECK_EQUAL(a[col.get(0)], 0x44);
        CHECK_EQUAL(a[col.get(1)], 0x44);

        col.destroy();

    }
    a.destroy();
}

TEST(ArrayIntNull_MinMaxOfNegativeIntegers)
{
    ArrayIntNull a(Allocator::get_default());
    a.create(Array::type_Normal);
    a.clear();
    a.add(-1);
    a.add(-2);
    a.add(-3);
    a.add(-128);
    a.add(0);
    a.set_null(4);

    int64_t t0;
    a.minimum(t0);
    CHECK_EQUAL(-128, t0);

    int64_t t1;
    a.maximum(t1);
    CHECK_EQUAL(-1, t1);

    a.clear();
    a.add(std::numeric_limits<int_fast64_t>::max());
    a.add(0);
    a.set_null(1);

    int64_t t2;
    size_t i2;
    bool found = a.minimum(t2, 0, npos, &i2);
    CHECK_EQUAL(i2, 0);
    CHECK_EQUAL(t2, std::numeric_limits<int_fast64_t>::max());
    CHECK_EQUAL(found, true);

    a.destroy();
}
