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

#include <realm/decimal128.hpp>

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
    test_str("3.1416", "3.1416");
    test_str("3.1416e-4", "3.1416E-4");
    test_str("-3.1416e-4", "-3.1416E-4");
    test_str("10e2", "10E2");
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
