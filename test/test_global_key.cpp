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

#include <realm/global_key.hpp>

#include "test.hpp"

#include <realm/mixed.hpp>
#include <realm/string_data.hpp>

using namespace realm;

TEST(GlobalKey_ToString)
{
    CHECK_EQUAL(GlobalKey(0xabc, 0xdef).to_string(), "{0abc-0def}");
    CHECK_EQUAL(GlobalKey(0x11abc, 0x999def).to_string(), "{11abc-999def}");
    CHECK_EQUAL(GlobalKey(0, 0).to_string(), "{0000-0000}");
}

TEST(GlobalKey_FromString)
{
    CHECK_EQUAL(GlobalKey::from_string("{0-0}"), GlobalKey(0, 0));
    CHECK_EQUAL(GlobalKey::from_string("{aaaabbbbccccdddd-eeeeffff00001111}"),
                GlobalKey(0xaaaabbbbccccddddULL, 0xeeeeffff00001111ULL));
    CHECK_THROW(GlobalKey::from_string(""), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("0"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("-"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("0-"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0-0"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0-0-0}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{aaaabbbbccccdddde-0}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0g-0}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0-0g}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{0-aaaabbbbccccdddde}"), std::invalid_argument);
    CHECK_THROW(GlobalKey::from_string("{-}"), std::invalid_argument);

    // std::strtoull accepts the "0x" prefix. We don't.
    CHECK_THROW(GlobalKey::from_string("{0x0-0x0}"), std::invalid_argument);
    {
        std::istringstream istr("{1-2}");
        GlobalKey oid;
        istr >> oid;
        CHECK_EQUAL(oid, GlobalKey(1, 2));
    }
    {
        std::istringstream istr("{1-2");
        GlobalKey oid;
        istr >> oid;
        CHECK(istr.rdstate() & std::istream::failbit);
        CHECK_EQUAL(oid, GlobalKey());
    }
}

TEST(GlobalKey_Compare)
{
    CHECK_LESS(GlobalKey(0, 0), GlobalKey(0, 1));
    CHECK_LESS(GlobalKey(0, 0), GlobalKey(1, 0));
}
