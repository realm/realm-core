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

/*************************************************************************
 *
 * This test set validate that sets and lists dont hit an assert exception
 * when operating with Mixed.
 *
 * See: https://github.com/realm/realm-core/issues/4304
 *
 **************************************************************************/

TEST(Set_Mixed_do_erase)
{
    Group g;

    auto t = g.add_table("foo");
    t->add_column_set(type_Mixed, "mixeds");
    auto obj = t->create_object();

    auto set = obj.get_set<Mixed>("mixeds");

    set.insert(util::none);
    set.erase_null();
}

TEST(List_Mixed_do_set)
{
    Group g;

    auto t = g.add_table("foo");
    t->add_column_list(type_Mixed, "mixeds");
    auto obj = t->create_object();

    auto set = obj.get_list<Mixed>("mixeds");

    set.insert_null(0);
    set.set(0, Mixed("hello world"));
}

TEST(List_Mixed_do_insert)
{
    Group g;

    auto t = g.add_table("foo");
    t->add_column_list(type_Mixed, "mixeds");
    auto obj = t->create_object();

    auto list = obj.get_list<Mixed>("mixeds");

    list.insert_null(0);
    list.insert(0, Mixed("hello world"));
}

TEST(Mixed_List_unresolved_as_null)
{
    Group g;
    auto t = g.add_table("foo");
    t->add_column_list(type_Mixed, "mixeds");
    auto obj = t->create_object();
    auto obj1 = t->create_object();

    auto list = obj.get_list<Mixed>("mixeds");

    list.insert_null(0);
    list.insert(1, Mixed{"test"});
    obj1.invalidate();
    list.insert(2, obj1);

    CHECK(list.size() == 3);

    {
        // find all mixed nulls or unresolved link should work the same way
        int cnt = 0;
        list.find_all(realm::null(), [this, &cnt](size_t pos) {
            if (cnt == 0)
                CHECK(pos == 0);
            else if (cnt == 1)
                CHECK(pos == 2);
            cnt += 1;
        });
        cnt = 0;
        list.find_all(obj1, [this, &cnt](size_t pos) {
            if (cnt == 0)
                CHECK(pos == 0);
            else if (cnt == 1)
                CHECK(pos == 2);
            cnt += 1;
        });
    }

    {
        // find null or unresolved link must work the same way
        auto index = list.find_any(realm::null());
        CHECK(index == 0);
        index = list.find_first(obj1);
        CHECK(index == 0);
    }

    {
        // is null for unresolved links and null must behave the same way
        CHECK(list.is_null(0));
        CHECK(list.is_null(2));
    }

    {
        std::vector<size_t> indices{0, 1, 2};
        list.sort(indices);
        CHECK(indices.size() == 3);
        CHECK(indices.at(0) == 0);
        CHECK(indices.at(1) == 2);
        CHECK(indices.at(2) == 1);
        CHECK(list.is_null(indices[0]));
        CHECK(list.is_null(indices[1]));
        CHECK(!list.is_null(indices[2]));
    }

    {
        std::vector<size_t> indices{0, 1, 2};
        list.distinct(indices);
        CHECK(indices.size() == 2);
        CHECK(indices.at(0) == 0);
        CHECK(indices.at(1) == 1);
        CHECK(list.is_null(indices[0]));
        CHECK(!list.is_null(indices[1]));
        CHECK(list.find_any(realm::null()) == 0);
    }

    {
        list.remove(0);
        CHECK(list.find_any(realm::null()) == 1);
        list.remove(1);
        CHECK(list.find_any(realm::null()) == npos);
        CHECK(list.size() == 1);
    }
}

TEST(Mixed_Set_unresolved_as_null)
{
    Group g;

    auto t = g.add_table("foo");
    t->add_column_set(type_Mixed, "mixeds");
    auto obj = t->create_object();
    auto obj1 = t->create_object();
    auto obj2 = t->create_object();
    obj1.invalidate();
    obj2.invalidate();

    auto set = obj.get_set<Mixed>("mixeds");
    auto [it, success] = set.insert(Mixed{obj1});
    CHECK(success);
    auto [it1, success1] = set.insert(Mixed{"test"});
    CHECK(success1);

    {
        // null treated as invalalid link
        CHECK(set.size() == 2);
        auto [it, success] = set.insert(Mixed{});
        CHECK(!success);
        auto [it1, success1] = set.insert_null();
        CHECK(!success1);
        CHECK(set.size() == 2);
    }

    {
        // find all mixed nulls or unresolved link should work the same way
        // there can only be 1 NULL
        int cnt = 0;
        set.find_all(realm::null(), [this, &set, &cnt](size_t pos) {
            CHECK(pos != not_found);
            CHECK(set.is_null(pos));
            cnt += 1;
        });
        CHECK(cnt == 1);
    }

    {
        auto index = set.find_any(realm::null());
        CHECK(index != not_found);
        CHECK(set.is_null(index));
    }

    {
        auto [it, success] = set.insert(Mixed{obj2});
        CHECK(!success);
        CHECK(set.size() == 2);
        std::vector<size_t> indices{0, 1};
        set.sort(indices);
        CHECK(indices.size() == 2);
        CHECK(indices[0] == 0);
        CHECK(indices[1] == 1);
    }

    {
        // trigger interface exception, we ended up with multiple nulls
        Group g;
        auto t = g.add_table("foo");
        t->add_column_set(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto obj2 = t->create_object();
        auto set = obj.get_set<Mixed>("mixeds");
        auto [it, success] = set.insert(obj1);
        auto [it1, success1] = set.insert(obj2);
        CHECK(success);
        CHECK(success1);
        CHECK(set.size() == 2);
        CHECK(!set.is_null(0));
        CHECK(!set.is_null(1));
        obj1.invalidate();
        CHECK(set.is_null(0));
        CHECK(!set.is_null(1));
        obj2.invalidate();
        // this is the only violation we allow right now, we have ended up with 2 nulls
        CHECK(set.is_null(0));
        CHECK(set.is_null(1));
        auto obj3 = t->create_object();
        set.insert(obj3);
        CHECK(set.size() == 3);
        int cnt = 0;
        // we can now do find_all for nulls
        set.find_all(Mixed{}, [this, &set, &cnt](size_t index) {
            CHECK(index == 0 || index == 1);
            CHECK(set.is_null(index));
            cnt += 1;
        });
        CHECK(cnt == 2);
        // erase null will erase all the nulls
        set.erase_null();
        CHECK(set.size() == 1);
        auto obj4 = t->create_object();
        auto obj5 = t->create_object();
        set.insert(obj4);
        set.insert(obj5);
        CHECK(set.size() == 3);
        // erase all the nulls by default
        obj4.invalidate();
        obj5.invalidate();
        set.erase(Mixed{});
        CHECK(set.size() == 1);
        auto obj6 = t->create_object();
        auto obj7 = t->create_object();
        set.insert(obj6);
        set.insert(obj7);
        CHECK(set.size() == 3);
        obj6.invalidate();
        // remove only the first null
        set.erase<false>(Mixed{});
        CHECK(set.size() == 2);
    }
}
