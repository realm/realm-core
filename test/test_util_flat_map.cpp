/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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
#ifdef TEST_UTIL_FLAT_MAP

#include <realm/util/flat_map.hpp>

#include <string>

#include "test.hpp"

using namespace realm;

TEST(Util_FlatMap_Basic)
{
    util::FlatMap<std::string, size_t> map;
    CHECK_EQUAL(map.size(), 0);
    CHECK(map.empty());
    map.clear();
    CHECK(map.empty());
    auto it_and_inserted = map.insert({"hello", 1});
    CHECK(it_and_inserted.first == map.begin());
    CHECK(it_and_inserted.second == true);
    map.insert({"two", 2});
    CHECK(!map.empty());
    CHECK_EQUAL(map.size(), 2);
    it_and_inserted = map.insert({"two", 22}); // due to an existing item, this does not insert
    CHECK(it_and_inserted.second == false);
    CHECK_EQUAL(map["hello"], 1);
    CHECK_EQUAL(map["two"], 2);
    CHECK_EQUAL(map.count("three"), 0);
    CHECK(map.find("three") == map.end());
    CHECK_EQUAL(map["three"], 0);
    CHECK_EQUAL(map.count("three"), 1);
    CHECK_EQUAL(map.erase("hello"), 1);
    CHECK_EQUAL(map.erase("three"), 1);
    CHECK_EQUAL(map.erase("unknown"), 0);
    CHECK_EQUAL(map.size(), 1);
    map.clear();
    CHECK(map.empty());
}

TEST(Util_FlatMap_Construct)
{
    util::FlatMap<std::string, size_t> map({{"foo", 1}, {"bar", 2}, {"ape", 3}});
    CHECK_EQUAL(map.size(), 3);
    CHECK_EQUAL(map.count("foo"), 1);
    CHECK_EQUAL(map.count("bar"), 1);
    CHECK_EQUAL(map.count("ape"), 1);
}

#endif // TEST_UTIL_FLAT_MAP
