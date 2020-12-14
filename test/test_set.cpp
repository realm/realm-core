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
#include "test_types_helper.hpp"

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
        auto [erased_ndx, erased] = s.erase(5);
        CHECK(erased);
        CHECK_EQUAL(erased_ndx, 0);
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
        auto [erased_ndx, erased] = s.erase("Hello");
        CHECK(erased);
        CHECK_EQUAL(erased_ndx, 0);
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
        auto [erased_ndx, erased] = s.erase(Mixed("Hello"));
        CHECK(erased);
        CHECK_EQUAL(erased_ndx, 1);
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
    auto lnkset_links = foo.get_setbase_ptr(col_links);
    auto set_typed_links = foo.get_set<ObjLink>(col_typed_links);
    auto set_mixeds = foo.get_set<Mixed>(col_mixeds);

    set_links.insert(bar1.get_key());
    set_links.insert(bar2.get_key());
    set_links.insert(bar3.get_key());
    set_links.insert(bar1.get_key());
    set_links.insert(bar2.get_key());
    set_links.insert(bar3.get_key());

    CHECK_EQUAL(set_links.size(), 3);
    CHECK_EQUAL(bar1.get_backlink_count(), 1);
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

    CHECK_EQUAL(bar1.get_backlink_count(), 2);
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
    CHECK_EQUAL(bar1.get_backlink_count(), 3);
    CHECK_NOT_EQUAL(set_mixeds.find(bar1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(bar2.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(cab1.get_link()), realm::npos);
    CHECK_NOT_EQUAL(set_mixeds.find(cab2.get_link()), realm::npos);
    CHECK_EQUAL(set_mixeds.find(bar3.get_link()), realm::npos);

    bar1.remove();

    CHECK_EQUAL(set_links.size(), 2);
    CHECK_EQUAL(set_typed_links.size(), 3);
    CHECK_EQUAL(set_mixeds.size(), 3);

    CHECK_EQUAL(set_links.find(bar1.get_key()), realm::npos);
    CHECK_EQUAL(set_typed_links.find(bar1.get_link()), realm::npos);
    CHECK_EQUAL(set_mixeds.find(bar1.get_link()), realm::npos);

    auto bar2_key = bar2.get_key();
    auto bar2_link = bar2.get_link();
    bar2.invalidate();

    CHECK_EQUAL(set_links.size(), 2);
    CHECK_EQUAL(lnkset_links->size(), 1); // Unresolved link was hidden from LnkSet
    CHECK_EQUAL(set_typed_links.size(), 3);
    CHECK_EQUAL(set_mixeds.size(), 3);

    CHECK_EQUAL(set_links.find(bar2_key), realm::npos);               // The original bar2 key is no longer in the set
    CHECK_NOT_EQUAL(set_links.find(bar2.get_key()), realm::npos);     // The unresolved bar2 key is in the set
    CHECK_EQUAL(lnkset_links->find_any(bar2.get_key()), realm::npos); // The unresolved bar2 key is hidden by LnkSet
    CHECK_EQUAL(set_typed_links.find(bar2_link), realm::npos);
    CHECK_EQUAL(set_mixeds.find(bar2_link), realm::npos);

    // g.to_json(std::cout);
    foos->clear();
    g.verify();
}

TEST_TYPES(Set_Types, Prop<Int>, Prop<String>, Prop<Float>, Prop<Double>, Prop<Timestamp>, Prop<UUID>, Prop<ObjectId>,
           Prop<Decimal128>, Prop<BinaryData>, Nullable<Int>, Nullable<String>, Nullable<Float>, Nullable<Double>,
           Nullable<Timestamp>, Nullable<UUID>, Nullable<ObjectId>, Nullable<Decimal128>, Nullable<BinaryData>)
{
    using type = typename TEST_TYPE::type;
    TestValueGenerator gen;
    Group g;

    auto t = g.add_table("foo");
    auto col = t->add_column_set(TEST_TYPE::data_type, "values", TEST_TYPE::is_nullable);
    CHECK(col.is_set());

    auto obj = t->create_object();
    {
        auto s = obj.get_set<type>(col);
        auto values = gen.values_from_int<type>({0, 1, 2, 3});
        for (auto v : values) {
            s.insert(v);
        }
        CHECK_EQUAL(s.size(), values.size());
        for (auto v : values) {
            auto ndx = s.find(v);
            CHECK_NOT_EQUAL(ndx, realm::npos);
        }
        auto [erased_ndx, erased] = s.erase(values[0]);
        CHECK(erased);
        CHECK_EQUAL(erased_ndx, 0);
        CHECK_EQUAL(s.size(), values.size() - 1);

        s.clear();
        CHECK_EQUAL(s.size(), 0);

        if (TEST_TYPE::is_nullable) {
            s.insert_null();
            CHECK_EQUAL(s.size(), 1);
            auto null_value = TEST_TYPE::default_value();
            CHECK(value_is_null(null_value));
            auto ndx = s.find(null_value);
            CHECK_NOT_EQUAL(ndx, realm::npos);
            s.erase_null();
            CHECK_EQUAL(s.size(), 0);
            ndx = s.find(null_value);
            CHECK_EQUAL(ndx, realm::npos);
        }
    }
}

TEST(Set_LnkSetUnresolved)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    auto bars = g.add_table("class_Bar");

    ColKey col_links = foos->add_column_set(*bars, "links");
    auto foo = foos->create_object();
    auto bar1 = bars->create_object();
    auto bar2 = bars->create_object();
    auto bar3 = bars->create_object();

    auto key_set = foo.get_set<ObjKey>(col_links);
    auto link_set = foo.get_linkset(col_links);

    link_set.insert(bar1.get_key());
    link_set.insert(bar2.get_key());
    link_set.insert(bar1.get_key());
    link_set.insert(bar2.get_key());

    CHECK_EQUAL(key_set.size(), 2);
    CHECK_EQUAL(link_set.size(), 2);
    CHECK_EQUAL(key_set.find(bar1.get_key()), 0);
    CHECK_EQUAL(key_set.find(bar2.get_key()), 1);
    CHECK_EQUAL(link_set.find(bar1.get_key()), 0);
    CHECK_EQUAL(link_set.find(bar2.get_key()), 1);

    bar2.invalidate();

    CHECK_EQUAL(key_set.size(), 2);
    CHECK_EQUAL(link_set.size(), 1);
    CHECK_EQUAL(key_set.find(bar2.get_key()), 0);
    CHECK_EQUAL(key_set.find(bar1.get_key()), 1);
    CHECK_EQUAL(link_set.find(bar1.get_key()), 0);
    CHECK_EQUAL(link_set.find(bar2.get_key()), not_found);

    link_set.insert(bar3.get_key());

    CHECK_EQUAL(key_set.size(), 3);
    CHECK_EQUAL(link_set.size(), 2);

    CHECK_EQUAL(key_set.find(bar2.get_key()), 0);
    CHECK_EQUAL(key_set.find(bar1.get_key()), 1);
    CHECK_EQUAL(key_set.find(bar3.get_key()), 2);

    CHECK_EQUAL(link_set.find(bar1.get_key()), 0);
    CHECK_EQUAL(link_set.find(bar2.get_key()), not_found);
    CHECK_EQUAL(link_set.find(bar3.get_key()), 1);

    CHECK_EQUAL(link_set.get(0), bar1.get_key());
    CHECK_EQUAL(link_set.get(1), bar3.get_key());

    std::vector<size_t> found;
    link_set.find_all(bar3.get_key(), [&](size_t ndx) {
        found.push_back(ndx);
    });
    CHECK_EQUAL(found.size(), 1);
    CHECK_EQUAL(found[0], 1);
}

TEST(Set_Union)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_ints = foos->add_column_set(type_Int, "ints");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_ints);
    auto set2 = obj2.get_set<int64_t>(col_ints);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    set1.assign_union(set2);
    CHECK_EQUAL(set1.size(), 5);
    CHECK_EQUAL(set1.get(0), 1);
    CHECK_EQUAL(set1.get(1), 2);
    CHECK_EQUAL(set1.get(2), 3);
    CHECK_EQUAL(set1.get(3), 4);
    CHECK_EQUAL(set1.get(4), 5);
}

TEST(Set_Intersection)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_ints = foos->add_column_set(type_Int, "ints");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_ints);
    auto set2 = obj2.get_set<int64_t>(col_ints);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    CHECK(set1.intersects(set2));
    CHECK(set2.intersects(set1));
    CHECK(!set1.is_subset_of(set2));
    CHECK(!set2.is_subset_of(set1));
    CHECK(!set1.is_superset_of(set2));
    CHECK(!set2.is_superset_of(set1));
    std::vector<int64_t> superset{{1, 2, 3, 4, 5}};
    std::vector<int64_t> subset{{1, 2}};
    CHECK(set1.is_subset_of(superset));
    CHECK(set1.is_superset_of(subset));

    set1.assign_intersection(set2);
    CHECK_EQUAL(set1.size(), 2);
    CHECK_EQUAL(set1.get(0), 4);
    CHECK_EQUAL(set1.get(1), 5);
}

TEST(Set_Difference)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_ints = foos->add_column_set(type_Int, "ints");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_ints);
    auto set2 = obj2.get_set<int64_t>(col_ints);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    set1.assign_difference(set2);
    CHECK_EQUAL(set1.size(), 2);
    CHECK_EQUAL(set1.get(0), 1);
    CHECK_EQUAL(set1.get(1), 2);
}

TEST(Set_SymmetricDifference)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_ints = foos->add_column_set(type_Int, "ints");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_ints);
    auto set2 = obj2.get_set<int64_t>(col_ints);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    set1.assign_symmetric_difference(set2);
    CHECK_EQUAL(set1.size(), 3);
    CHECK_EQUAL(set1.get(0), 1);
    CHECK_EQUAL(set1.get(1), 2);
    CHECK_EQUAL(set1.get(2), 3);
}
