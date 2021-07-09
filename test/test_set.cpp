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

extern unsigned int unit_test_random_seed;

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
        CHECK_THROW_ANY(s.insert(StringData{}));
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

    Obj bar = g.add_table("bar")->create_object();
    auto t = g.add_table("foo");
    t->add_column_set(type_Mixed, "mixeds");
    auto obj = t->create_object();

    // Check that different typed with same numeric value are treated as the same
    auto set = obj.get_set<Mixed>("mixeds");
    set.insert(123);
    set.insert(123.f);
    set.insert(123.);
    set.insert(Decimal128("123"));
    CHECK_EQUAL(set.size(), 1);
    CHECK_EQUAL(set.get(0), Mixed(123));
    set.clear();

    std::vector<Mixed> ref_values{{},
                                  false,
                                  true,
                                  Decimal128("-123"),
                                  25,
                                  56.f,
                                  88.,
                                  "Hello, World!",
                                  "æbler", // Apples
                                  "ørken", // Dessert
                                  "ådsel", // Carrion
                                  Timestamp(1, 2),
                                  ObjectId::gen(),
                                  UUID("01234567-9abc-4def-9012-3456789abcde"),
                                  bar.get_link()};
    // Sets of Mixed should be ordered by the rules defined for Set<Mixed>. Refer to "realm/set.hpp".
    std::vector<size_t> indices(ref_values.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), std::mt19937(unit_test_random_seed));
    for (auto i : indices) {
        set.insert(ref_values[i]);
    }
    std::vector<Mixed> actuals;
    std::for_each(set.begin(), set.end(), [&actuals](auto v) {
        actuals.push_back(v);
    });
    CHECK(ref_values == actuals);
    actuals.clear();

    // Sets of Mixed can be sorted. Should sort according to the comparison rules defined for Mixed, which
    // currently
    set.sort(indices);
    std::transform(begin(indices), end(indices), std::back_inserter(actuals), [&](size_t index) {
        return set.get(index);
    });
    std::sort(begin(ref_values), end(ref_values), [](auto v1, auto v2) {
        return v1 < v2;
    });
    CHECK(ref_values == actuals);
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
    CHECK_THROW_ANY(set_links.insert({}));
    set_links.erase(bar1.get_key());
    CHECK_EQUAL(bar1.get_backlink_count(), 0);
    set_links.insert(bar1.get_key());

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
           Prop<Decimal128>, Prop<BinaryData>, Prop<Mixed>, Nullable<Int>, Nullable<String>, Nullable<Float>,
           Nullable<Double>, Nullable<Timestamp>, Nullable<UUID>, Nullable<ObjectId>, Nullable<Decimal128>,
           Nullable<BinaryData>)
{
    using type = typename TEST_TYPE::type;
    TestValueGenerator gen;
    Group g;

    auto t = g.add_table("foo");
    auto col = t->add_column_set(TEST_TYPE::data_type, "values", TEST_TYPE::is_nullable);
    auto col_list = t->add_column_list(TEST_TYPE::data_type, "list", TEST_TYPE::is_nullable);
    CHECK(col.is_set());

    auto obj = t->create_object();
    {
        auto s = obj.get_set<type>(col);
        auto l = obj.get_list<type>(col_list);
        auto values = gen.values_from_int<type>({0, 1, 2, 3});
        for (auto v : values) {
            s.insert(v);
            l.add(v);
        }
        CHECK_EQUAL(s.size(), values.size());
        CHECK(s.set_equals(l));
        for (auto v : values) {
            auto ndx = s.find(v);
            CHECK_NOT_EQUAL(ndx, realm::npos);
        }
        auto [erased_ndx, erased] = s.erase(values[0]);
        CHECK(erased);
        CHECK_EQUAL(erased_ndx, 0);
        CHECK_EQUAL(s.size(), values.size() - 1);
        CHECK(s.is_subset_of(l));

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

TEST(Set_BinarySets)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);

    {
        WriteTransaction wt{sg};
        TableRef foo = wt.add_table("class_foo");
        auto set1 = foo->add_column_set(type_Binary, "bin1");
        auto set2 = foo->add_column_set(type_Binary, "bin2");

        auto obj = foo->create_object();
        auto s1 = obj.get_set<Binary>(set1);
        auto s2 = obj.get_set<Binary>(set2);
        wt.commit();
    }
    {
        WriteTransaction wt{sg};
        TableRef foo = wt.get_or_add_table("class_foo");
        auto set1 = foo->get_column_key("bin1");
        auto set2 = foo->get_column_key("bin2");

        auto s1 = foo->begin()->get_set<Binary>(set1);
        auto s2 = foo->begin()->get_set<Binary>(set2);

        std::string str1(1024, 'a');
        std::string str2(256, 'b');
        std::string str3(64, 'c');
        std::string str4(256, 'd');
        std::string str5(1024, 'e');

        s1.insert(BinaryData(str1.data(), str1.size()));
        s1.insert(BinaryData(str2.data(), str2.size()));
        s1.insert(BinaryData(str3.data(), str3.size()));

        s2.insert(BinaryData(str3.data(), str3.size()));
        s2.insert(BinaryData(str4.data(), str4.size()));
        s2.insert(BinaryData(str5.data(), str5.size()));
        wt.commit();
    }
    {
        WriteTransaction wt{sg};
        TableRef foo = wt.get_or_add_table("class_foo");
        auto set1 = foo->get_column_key("bin1");
        auto set2 = foo->get_column_key("bin2");

        auto s1 = foo->begin()->get_set<Binary>(set1);
        auto s2 = foo->begin()->get_set<Binary>(set2);

        s1.assign_intersection(s2);
        wt.commit();
    }

    sg->start_read()->verify();
}

TEST(Set_TableClear)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef sg = DB::create(path);

    {
        WriteTransaction wt{sg};
        TableRef foo = wt.add_table("class_foo");
        TableRef origin = wt.add_table("class_origin");
        auto set = origin->add_column_set(*foo, "set");

        auto obj = foo->create_object();
        auto s1 = origin->create_object().get_linkset(set);
        s1.insert(obj.get_key());
        wt.commit();
    }
    {
        WriteTransaction wt{sg};
        TableRef origin = wt.get_or_add_table("class_origin");
        auto set = origin->get_column_key("set");
        auto s1 = origin->begin()->get_linkset(set);
        s1.clear();
        wt.commit();
    }
    {
        WriteTransaction wt{sg};
        TableRef foo = wt.get_or_add_table("class_foo");
        foo->clear();
        wt.commit();
    }

    sg->start_read()->verify();
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
    ColKey col_set = foos->add_column_set(type_Int, "int_set");
    ColKey col_list = foos->add_column_list(type_Int, "int_list");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_set);
    auto set2 = obj2.get_set<int64_t>(col_set);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    auto list = obj1.get_list<int64_t>(col_list);

    for (int64_t x : {11, 3, 7, 5, 14, 7}) {
        list.add(x);
    }

    set1.assign_union(set2);
    CHECK_EQUAL(set1.size(), 5);
    CHECK_EQUAL(set1.get(0), 1);
    CHECK_EQUAL(set1.get(1), 2);
    CHECK_EQUAL(set1.get(2), 3);
    CHECK_EQUAL(set1.get(3), 4);
    CHECK_EQUAL(set1.get(4), 5);
    set2.assign_union(list);
    CHECK_EQUAL(set2.size(), 6);
    CHECK_EQUAL(set2.get(0), 3);
    CHECK_EQUAL(set2.get(5), 14);
}

TEST(Set_UnionString)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_set = foos->add_column_set(type_String, "string set", true);
    ColKey col_list = foos->add_column_list(type_String, "string list", true);

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<String>(col_set);
    auto set2 = obj2.get_set<String>(col_set);

    set1.insert("FooBar");
    set1.insert("A");
    set1.insert("The fox jumps over the lazy dog");

    set2.insert("FooBar");
    set2.insert("World");
    set2.insert("Atomic");

    auto list1 = obj1.get_list<String>(col_list);
    list1.add("FooBar");
    list1.add("World");
    list1.add({});
    list1.add("Atomic");

    set1.assign_union(set2);
    CHECK_EQUAL(set1.size(), 5);
    CHECK_EQUAL(set1.get(0), "A");
    CHECK_EQUAL(set1.get(1), "Atomic");
    CHECK_EQUAL(set1.get(2), "FooBar");
    CHECK_EQUAL(set1.get(3), "The fox jumps over the lazy dog");
    CHECK_EQUAL(set1.get(4), "World");

    set1.assign_union(list1);
    CHECK_EQUAL(set1.size(), 6);
    CHECK_EQUAL(set1.get(0), StringData());
    CHECK_EQUAL(set1.get(1), "A");
    CHECK_EQUAL(set1.get(2), "Atomic");
    CHECK_EQUAL(set1.get(3), "FooBar");
    CHECK_EQUAL(set1.get(4), "The fox jumps over the lazy dog");
    CHECK_EQUAL(set1.get(5), "World");
}

TEST(Set_Intersection)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_set = foos->add_column_set(type_Int, "int set", true);
    ColKey col_list = foos->add_column_list(type_Int, "int list", true);

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();
    auto obj3 = foos->create_object();

    auto set1 = obj1.get_set<util::Optional<int64_t>>(col_set);
    auto set2 = obj2.get_set<util::Optional<int64_t>>(col_set);

    for (int64_t x : {1, 2, 4, 5}) {
        set1.insert(x);
    }

    for (int64_t x : {3, 4, 5}) {
        set2.insert(x);
    }

    auto superset = obj1.get_list<util::Optional<int64_t>>(col_list);
    auto subset = obj2.get_list<util::Optional<int64_t>>(col_list);
    auto same_set = obj3.get_list<util::Optional<int64_t>>(col_list);

    for (int64_t x : {3, 4, 5, 1, 2}) {
        superset.add(x);
    }

    for (int64_t x : {1, 2}) {
        subset.add(x);
    }

    for (int64_t x : {1, 4, 2, 5}) {
        same_set.add(x);
    }

    CHECK(set1.intersects(set2));
    CHECK(set2.intersects(set1));
    CHECK(!set1.is_subset_of(set2));
    CHECK(!set2.is_subset_of(set1));
    CHECK(!set1.is_superset_of(set2));
    CHECK(!set2.is_superset_of(set1));
    CHECK(!set1.is_strict_superset_of(set1));
    CHECK(!set1.is_strict_subset_of(set1));
    CHECK(set1.is_subset_of(superset));
    CHECK(set1.is_superset_of(subset));
    CHECK(set1.is_strict_superset_of(subset));
    CHECK(set1.is_strict_subset_of(superset));
    CHECK(set1.is_subset_of(same_set));
    CHECK(set1.is_superset_of(same_set));
    CHECK(!set1.is_strict_superset_of(same_set));
    CHECK(!set1.is_strict_subset_of(same_set));
    CHECK(set1.set_equals(set1));
    CHECK(set1.set_equals(same_set));
    CHECK(!set1.set_equals(superset));
    CHECK(!set1.set_equals(superset));
    CHECK(!set1.set_equals(subset));
    CHECK(!set1.set_equals(subset));

    set1.assign_intersection(set2);
    CHECK_EQUAL(set1.size(), 2);
    CHECK_EQUAL(set1.get(0), 4);
    CHECK_EQUAL(set1.get(1), 5);
}

TEST(Set_IntersectionString)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_strings = foos->add_column_set(type_String, "strings");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<String>(col_strings);
    auto set2 = obj2.get_set<String>(col_strings);

    set1.insert("FooBar");
    set1.insert("A");
    set1.insert("Hello");
    set1.insert("The fox jumps over the lazy dog");

    set2.insert("FooBar");
    set2.insert("World");
    set2.insert("The fox jumps over the lazy dog");

    CHECK(set1.intersects(set2));
    CHECK(set2.intersects(set1));
    CHECK(!set1.is_subset_of(set2));
    CHECK(!set2.is_subset_of(set1));
    CHECK(!set1.is_superset_of(set2));
    CHECK(!set2.is_superset_of(set1));

    set1.assign_intersection(set2);
    CHECK_EQUAL(set1.size(), 2);
    CHECK_EQUAL(set1.get(0), "FooBar");
    CHECK_EQUAL(set1.get(1), "The fox jumps over the lazy dog");
}

TEST(Set_Difference)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_set = foos->add_column_set(type_Int, "int set");
    ColKey col_list = foos->add_column_list(type_Int, "int list");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_set);
    auto set2 = obj2.get_set<int64_t>(col_set);

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

    set1.assign_union(set2);
    auto list = obj2.get_list<int64_t>(col_list);
    for (int64_t x : {4, 5, 1, 27}) {
        list.add(x);
    }
    set1.assign_difference(list);
    CHECK_EQUAL(set1.size(), 2);
    CHECK_EQUAL(set1.get(0), 2);
    CHECK_EQUAL(set1.get(1), 3);
}

TEST(Set_SymmetricDifference)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_set = foos->add_column_set(type_Int, "int set");
    ColKey col_list = foos->add_column_list(type_Int, "int list");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<int64_t>(col_set);
    auto set2 = obj2.get_set<int64_t>(col_set);

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

    set1.assign_union(set2);
    auto list = obj2.get_list<int64_t>(col_list);
    for (int64_t x : {4, 5, 1, 27}) {
        list.add(x);
    }
    set1.assign_symmetric_difference(list);
    CHECK_EQUAL(set1.size(), 3);
    CHECK_EQUAL(set1.get(0), 2);
    CHECK_EQUAL(set1.get(1), 3);
    CHECK_EQUAL(set1.get(2), 27);
}

TEST(Set_SymmetricDifferenceString)
{
    Group g;
    auto foos = g.add_table("class_Foo");
    ColKey col_strings = foos->add_column_set(type_String, "strings");

    auto obj1 = foos->create_object();
    auto obj2 = foos->create_object();

    auto set1 = obj1.get_set<String>(col_strings);
    auto set2 = obj2.get_set<String>(col_strings);

    set1.insert("FooBar");
    set1.insert("A");
    set1.insert("The fox jumps over the lazy dog");

    set2.insert("FooBar");
    set2.insert("World");
    set2.insert("Cosmic love");

    set1.assign_symmetric_difference(set2);
    CHECK_EQUAL(set1.size(), 4);
    CHECK_EQUAL(set1.get(0), "A");
    CHECK_EQUAL(set1.get(1), "Cosmic love");
    CHECK_EQUAL(set1.get(2), "The fox jumps over the lazy dog");
    CHECK_EQUAL(set1.get(3), "World");
}
