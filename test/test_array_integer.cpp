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

#include <realm/array_integer.hpp>
#include <realm/array_ref.hpp>
#include <realm/column_integer.hpp>
#include <realm/array_integer_tpl.hpp>
#include <realm/query_conditions.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;

using realm::util::unwrap;

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
