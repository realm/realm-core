/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#include <realm.hpp>
#include <realm/array_mixed.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

TEST(Set_Basics)
{
    Group g;

    auto t = g.add_table("foo");
    auto col_int = t->add_column_set(type_Int, "ints");
    auto col_str = t->add_column_set(type_String, "strings");
    auto col_any = t->add_column_set(type_Mixed, "any");
    CHECK(col_int.is_set());
    CHECK(col_str.is_set());
    CHECK(col_any.is_set());

    auto obj = t->create_object();
    {
        auto s = obj.get_set<Int>(col_int);
        s.insert(5);
        CHECK_EQUAL(s.size(), 1);
        s.insert(10);
        CHECK_EQUAL(s.size(), 2);
        s.insert(5);
        CHECK_EQUAL(s.size(), 2);
        auto ndx = s.find(5);
        CHECK_NOT_EQUAL(ndx, realm::npos);
        auto erased = s.erase(5);
        CHECK_EQUAL(ndx, erased);
        CHECK_EQUAL(s.size(), 1);
    }

    {
        auto s = obj.get_set<String>(col_str);
        s.insert("Hello");
        CHECK_EQUAL(s.size(), 1);
        s.insert("World");
        CHECK_EQUAL(s.size(), 2);
        s.insert("Hello");
        CHECK_EQUAL(s.size(), 2);
        auto ndx = s.find("Hello");
        CHECK_NOT_EQUAL(ndx, realm::npos);
        auto erased = s.erase("Hello");
        CHECK_EQUAL(ndx, erased);
        CHECK_EQUAL(s.size(), 1);
    }
    {
        auto s = obj.get_set<Mixed>(col_any);
        s.insert(Mixed("Hello"));
        CHECK_EQUAL(s.size(), 1);
        s.insert(Mixed(10));
        CHECK_EQUAL(s.size(), 2);
        s.insert(Mixed("Hello"));
        CHECK_EQUAL(s.size(), 2);
        auto ndx = s.find(Mixed("Hello"));
        CHECK_NOT_EQUAL(ndx, realm::npos);
        auto erased = s.erase(Mixed("Hello"));
        CHECK_EQUAL(ndx, erased);
        CHECK_EQUAL(s.size(), 1);
    }
}


TEST(Set_Mixed)
{
    Group g;

    auto t = g.add_table("foo");
    t->add_column_set(type_Mixed, "mixeds");
    auto obj = t->create_object();

    auto set = obj.get_set<Mixed>("mixeds");
    set.insert(123);
    set.insert(123);
    set.insert(123);
    CHECK_EQUAL(set.size(), 1);
    CHECK_EQUAL(set.get(0), Mixed(123));

    // Sets of Mixed should be ordered by their type index (as specified by the `DataType` enum).
    set.insert(56.f);
    set.insert("Hello, World!");
    set.insert(util::none);
    set.insert(util::none);
    set.insert("Hello, World!");
    CHECK_EQUAL(set.size(), 4);

    CHECK_EQUAL(set.get(0), Mixed{});
    CHECK_EQUAL(set.get(1), Mixed{123});
    CHECK_EQUAL(set.get(2), Mixed{"Hello, World!"});
    CHECK_EQUAL(set.get(3), Mixed{56.f});

    // Sets of Mixed can be sorted.
    std::vector<Mixed> sorted;
    std::vector<size_t> sorted_indices;
    set.sort(sorted_indices);
    std::transform(begin(sorted_indices), end(sorted_indices), std::back_inserter(sorted), [&](size_t index) {
        return set.get(index);
    });
    CHECK(std::equal(begin(sorted), end(sorted), set.begin()));
    auto sorted2 = sorted;
    std::sort(begin(sorted2), end(sorted2), SetElementLessThan<Mixed>{});
    CHECK(sorted2 == sorted);
}

TEST(Set_Links)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    auto bars = g.add_table("class_Bar");
    auto cabs = g.add_table("class_Cab");

    ColKey col_links = foos->add_column_set(*bars, "links");
    ColKey col_typed_links = foos->add_column_set(type_TypedLink, "typed_links");
    ColKey col_mixeds = foos->add_column_set(type_Mixed, "mixeds");

    auto foo = foos->create_object();

    auto bar1 = bars->create_object();
    auto bar2 = bars->create_object();
    auto bar3 = bars->create_object();
    auto bar4 = bars->create_object();

    auto cab1 = cabs->create_object();
    auto cab2 = cabs->create_object();
    auto cab3 = cabs->create_object();

    auto set_links = foo.get_set<ObjKey>(col_links);
    auto set_typed_links = foo.get_set<ObjLink>(col_typed_links);
    auto set_mixeds = foo.get_set<Mixed>(col_mixeds);

    set_links.insert(bar1.get_key());
    set_links.insert(bar2.get_key());
    set_links.insert(bar3.get_key());
    set_links.insert(bar1.get_key());
    set_links.insert(bar2.get_key());
    set_links.insert(bar3.get_key());

    CHECK_EQUAL(set_links.size(), 3);
    CHECK_NOT_EQUAL(set_links.find(bar1.get_key()), realm::npos);
    CHECK_NOT_EQUAL(set_links.find(bar2.get_key()), realm::npos);
    CHECK_NOT_EQUAL(set_links.find(bar3.get_key()), realm::npos);
    CHECK_EQUAL(set_links.find(bar4.get_key()), realm::npos);

    set_typed_links.insert(bar1.get_link());
    set_typed_links.insert(bar2.get_link());
    set_typed_links.insert(cab1.get_link());
    set_typed_links.insert(cab2.get_link());
    CHECK_EQUAL(set_typed_links.size(), 4);

    set_typed_links.insert(bar1.get_link());
    CHECK_EQUAL(set_typed_links.size(), 4);
    set_typed_links.insert(bar2.get_link());
    CHECK_EQUAL(set_typed_links.size(), 4);
    set_typed_links.insert(cab1.get_link());
    CHECK_EQUAL(set_typed_links.size(), 4);
    set_typed_links.insert(cab2.get_link());
    CHECK_EQUAL(set_typed_links.size(), 4);

    CHECK_NOT_EQUAL(set_typed_links.find(bar1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_typed_links.find(bar2.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_typed_links.find(cab1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_typed_links.find(cab2.get_link()), realm::npos);
    CHECK_EQUAL(set_typed_links.find(bar3.get_link()), realm::npos);

    set_mixeds.insert(bar1.get_link());
    set_mixeds.insert(bar2.get_link());
    set_mixeds.insert(cab1.get_link());
    set_mixeds.insert(cab2.get_link());
    set_mixeds.insert(bar1.get_link());
    set_mixeds.insert(bar2.get_link());
    set_mixeds.insert(cab1.get_link());
    set_mixeds.insert(cab2.get_link());

    CHECK_EQUAL(set_mixeds.size(), 4);
    CHECK_NOT_EQUAL(set_mixeds.find(bar1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(bar2.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(cab1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(cab2.get_link()), realm::npos);
    CHECK_EQUAL(set_mixeds.find(bar3.get_link()), realm::npos);

    bar1.invalidate();

    CHECK_EQUAL(set_links.size(), 3);
    CHECK_EQUAL(set_typed_links.size(), 4);
    CHECK_EQUAL(set_mixeds.size(), 4);

    CHECK_NOT_EQUAL(set_links.find(bar1.get_key()), realm::npos);
    CHECK_NOT_EQUAL(set_typed_links.find(bar1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(bar1.get_link()), realm::npos);
}