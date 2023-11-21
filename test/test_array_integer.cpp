/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"

#include <limits>
#include <iostream>

#include <realm/array_integer.hpp>
#include <realm/array_ref.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_integer_tpl.hpp>
#include <realm/query_conditions.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;

TEST(Test_ArrayInt_no_encode)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(10);
    a.add(11);
    a.add(12);
    CHECK_NOT(a.try_encode());
    CHECK_NOT(a.is_encoded());
    CHECK(a.get(0) == 10);
    CHECK(a.get(1) == 11);
    CHECK(a.get(2) == 12);
}

TEST(Test_array_same_size_less_bits)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(24);
    a.add(240);
    a.add(545);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get_any(0) == 24);
    CHECK(a.get_any(1) == 240);
    CHECK(a.get_any(2) == 545);
}

TEST(Test_ArrayInt_encode_decode_need)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(10);
    a.add(5);
    a.add(5);
    // we allocate 16 bits for the compressed version which is not giving us any gain (15bits needed for data at this
    // stage)
    CHECK_NOT(a.try_encode());
    CHECK_NOT(a.is_encoded());
    CHECK_NOT(a.try_decode());
    CHECK_NOT(a.is_encoded());
    a.add(10);
    a.add(15);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == 10);
    CHECK(a.get(1) == 5);
    CHECK(a.get(2) == 5);
    CHECK(a.get(3) == 10);
    CHECK(a.get(4) == 15);
    a.destroy();
}

TEST(Test_ArrayInt_negative_nums)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(-1000000);
    a.add(0);
    a.add(1000000);
    CHECK(!a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    a.add(-1000000);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.get(3) == -1000000);
    CHECK(a.try_decode());
    a.add(0);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.get(3) == -1000000);
    CHECK(a.get(4) == 0);
    CHECK(a.try_decode());
    a.add(1000000);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.try_decode());
    a.add(-1000000);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.try_decode());
    a.add(0);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.try_decode());
    a.add(1000000);
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.size() == 9);
    CHECK(a.is_encoded());
    CHECK(a.get(0) == -1000000);
    CHECK(a.get(1) == 0);
    CHECK(a.get(2) == 1000000);
    CHECK(a.get(3) == -1000000);
    CHECK(a.get(4) == 0);
    CHECK(a.get(5) == 1000000);
    CHECK(a.get(6) == -1000000);
    CHECK(a.get(7) == 0);
    CHECK(a.get(8) == 1000000);
}

TEST(Test_ArrayInt_compress_data)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(16388);
    a.add(409);
    a.add(16388);
    a.add(16388);
    a.add(409);
    a.add(16388);
    CHECK(a.size() == 6);
    // Current: [16388:16, 409:16, 16388:16, 16388:16, 409:16, 16388:16], space needed: 6*16 bits = 96 bits +
    // header
    // compress the array is a good option.
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    // Compressed: [409:16, 16388:16][1:1,0:1,1:1,1:1,0:1,1:1], space needed: 2*16 bits + 6 * 1 bit = 38 bits +
    // header
    CHECK(a.size() == 6);
    CHECK(a.get(0) == 16388);
    CHECK(a.get(1) == 409);
    CHECK(a.get(2) == 16388);
    CHECK(a.get(3) == 16388);
    CHECK(a.get(4) == 409);
    CHECK(a.get(5) == 16388);
    // decompress
    CHECK(a.try_decode());
    a.add(20);
    // compress again, it should be a viable option
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    CHECK(a.size() == 7);
    CHECK(a.get(0) == 16388);
    CHECK(a.get(1) == 409);
    CHECK(a.get(2) == 16388);
    CHECK(a.get(3) == 16388);
    CHECK(a.get(4) == 409);
    CHECK(a.get(5) == 16388);
    CHECK(a.get(6) == 20);
    CHECK(a.try_decode());
    CHECK_NOT(a.is_encoded());
    CHECK(a.get(0) == 16388);
    CHECK(a.get(1) == 409);
    CHECK(a.get(2) == 16388);
    CHECK(a.get(3) == 16388);
    CHECK(a.get(4) == 409);
    CHECK(a.get(5) == 16388);
    CHECK(a.get(6) == 20);
    a.destroy();
}

TEST(Test_ArrayInt_compress_data_init_from_mem)
{
    ArrayInteger a(Allocator::get_default());
    a.create();
    a.add(16388);
    a.add(409);
    a.add(16388);
    a.add(16388);
    a.add(409);
    a.add(16388);
    const auto sz = a.size();
    CHECK(sz == 6);
    // Current: [16388:16, 409:16, 16388:16, 16388:16, 409:16, 16388:16],
    // space needed: 6*16 bits = 96 bits + header
    // compress the array is a good option (it should already be compressed).
    CHECK(a.try_encode());
    CHECK(a.is_encoded());
    //  Array should be in compressed form now
    auto mem = a.get_mem();
    ArrayInteger a1(Allocator::get_default());
    a1.init_from_mem(mem); // initialise a1 with a
    // check a1
    CHECK(a1.is_encoded());
    const auto sz1 = a1.size();
    CHECK(sz1 == 6);
    CHECK(a1.get(0) == 16388);
    CHECK(a1.get(1) == 409);
    CHECK(a1.get(2) == 16388);
    CHECK(a1.get(3) == 16388);
    CHECK(a1.get(4) == 409);
    CHECK(a1.get(5) == 16388);
    // decompress a1 and compresses again
    CHECK(a1.is_encoded());
    CHECK(a1.try_decode());
    CHECK_NOT(a1.is_encoded());
    a1.add(20);
    CHECK(a1.try_encode());
    CHECK(a1.is_encoded());
    CHECK(a1.size() == 7);
    CHECK(a1.get(0) == 16388);
    CHECK(a1.get(1) == 409);
    CHECK(a1.get(2) == 16388);
    CHECK(a1.get(3) == 16388);
    CHECK(a1.get(4) == 409);
    CHECK(a1.get(5) == 16388);
    CHECK(a1.get(6) == 20);
    CHECK(a1.try_decode());
    // a1 is now in charge of destroying the memory
    a1.destroy();
    CHECK_NOT(a1.is_attached());
}

TEST(ArrayIntNull_SetNull)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

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

TEST(ArrayIntNull_SetIntegerToPreviousNullValueChoosesNewNull)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

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

TEST(ArrayIntNull_Boundaries)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();
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

    a.destroy();
}

// Test if allocator relocation preserves null and non-null
TEST(ArrayIntNull_Relocate)
{
    ArrayIntNull a(Allocator::get_default());
    a.create();

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
    a.create();

    a.clear();
    for (size_t i = 0; i < 100; ++i) {
        a.add(0x33);
    }
    a.add(0x100);
    a.set(50, 0x44);
    a.set_null(51);
    a.set(60, 0x44);

    size_t t = a.find_first<NotEqual>(0x33);
    CHECK_EQUAL(50, t);

    t = a.find_first<NotEqual>(0x33, 0, 50);
    CHECK_EQUAL(not_found, t);

    t = a.find_first<NotEqual>(null());
    CHECK_EQUAL(0, t);

    t = a.find_first<NotEqual>(null(), 51);
    CHECK_EQUAL(52, t);

    size_t t2 = a.find_first(0x44);
    CHECK_EQUAL(50, t2);

    t2 = a.find_first(null());
    CHECK_EQUAL(51, t2);

    size_t t3 = a.find_first(0);
    CHECK_EQUAL(not_found, t3);

    size_t t22 = a.find_first<Greater>(0x100);
    CHECK_EQUAL(t22, not_found);

    {
        IntegerColumn col(Allocator::get_default());
        col.create();

        a.find_all(&col, 0x44);

        CHECK_EQUAL(2, col.size());
        CHECK_EQUAL(a.get(static_cast<size_t>(col.get(0))), 0x44);
        CHECK_EQUAL(a.get(static_cast<size_t>(col.get(1))), 0x44);

        col.destroy();
    }
    a.destroy();
}

TEST(ArrayRef_Basic)
{
    ArrayRef a(Allocator::get_default());
    a.create();
    CHECK(a.has_refs());

    ref_type ref = 8;
    a.insert(0, ref);
    CHECK_EQUAL(a.get(0), ref);
    a.insert(0, 16);
    CHECK_EQUAL(a.get(0), 16);
    CHECK_EQUAL(a.get(1), ref);
    a.set(0, 32);
    CHECK_EQUAL(a.get(0), 32);
    CHECK_EQUAL(a.get(1), ref);
    a.add(16);
    CHECK_EQUAL(a.get(0), 32);
    CHECK_EQUAL(a.get(1), ref);
    CHECK_EQUAL(a.get(2), 16);

    a.destroy();
}
