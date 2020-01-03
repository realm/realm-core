/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm.hpp>
#include <realm/array_decimal128.hpp>

#include "test.hpp"


using namespace realm;

TEST(Decimal_Basics)
{
    auto test_str = [&](const std::string& str, const std::string& ref) {
        Decimal128 d(str);
        CHECK_EQUAL(d.to_string(), ref);
        auto x = d.to_bid64();
        Decimal128 d1(x);
        CHECK_EQUAL(d, d1);
    };
    test_str("0", "0");
    test_str("0.000", "0E-3");
    test_str("0E-3", "0E-3");
    test_str("3.1416", "3.1416");
    test_str("3.1416e-4", "3.1416E-4");
    test_str("-3.1416e-4", "-3.1416E-4");
    test_str("10e2", "10E2");
    test_str("10e+2", "10E2");
    test_str("1e-00021", "1E-21");
    test_str("10.100e2", "1010.0");
    test_str(".00000001", "1E-8");
    test_str(".00000001000000000", "1.000000000E-8");

    Decimal128 d("-10.5");
    Decimal128 d1("20.25");
    CHECK(d < d1);
    Decimal128 d2("100");
    CHECK(d1 < d2);

    Decimal128 y;
    CHECK(y.is_null());
    y = d1;

    Decimal128 d10(10);
    CHECK(d10 < d2);
    CHECK(d10 >= d);
}

TEST(Decimal_Array)
{
    const char str0[] = "12345.67";
    const char str1[] = "1000.00";
    const char str2[] = "-45";

    ArrayDecimal128 arr(Allocator::get_default());
    arr.create();

    arr.add({str0});
    arr.add({str1});
    arr.insert(1, {str2});

    Decimal128 id2(str2);
    CHECK_EQUAL(arr.get(0), Decimal128(str0));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK_EQUAL(arr.get(2), Decimal128(str1));
    CHECK_EQUAL(arr.find_first(id2), 1);

    arr.erase(1);
    CHECK_EQUAL(arr.get(1), Decimal128(str1));

    ArrayDecimal128 arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 1);
    CHECK_EQUAL(arr1.get(0), Decimal128(str1));

    arr.destroy();
    arr1.destroy();
}

TEST(Decimal128_Table)
{
    const char str0[] = "12345.67";
    const char str1[] = "1000.00";

    Table t;
    auto col_price = t.add_column(type_Decimal, "id");
    auto obj0 = t.create_object().set(col_price, Decimal128(str0));
    auto obj1 = t.create_object().set(col_price, Decimal128(str1));
    CHECK_EQUAL(obj0.get<Decimal128>(col_price), Decimal128(str0));
    CHECK_EQUAL(obj1.get<Decimal128>(col_price), Decimal128(str1));
    auto key = t.find_first(col_price, Decimal128(str1));
    CHECK_EQUAL(key, obj1.get_key());
    auto d = obj1.get_any(col_price);
    CHECK_EQUAL(d.get<Decimal128>().to_string(), "1000.00");
}
