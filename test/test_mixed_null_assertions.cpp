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
        list.find_all(realm::null(), [this](size_t pos) {
            CHECK(pos == 0 || pos == 2);
        });
        list.find_all(obj1, [this](size_t pos) {
            CHECK(pos == 0 || pos == 2);
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
        CHECK(set.size() == 2);
        set.insert_null();
        // unresolved treated like nulls
        CHECK(set.size() == 2);
    }

    {
        // find all mixed nulls or unresolved link should work the same way
        set.find_all(realm::null(), [this, &set](size_t pos) {
            CHECK(pos != not_found);
            CHECK(set.is_null(pos));
        });
    }

    {
        auto index = set.find_any(realm::null());
        CHECK(index != not_found);
        CHECK(set.is_null(index));
    }

    {
        set.insert(Mixed{obj2});
        CHECK(set.size() == 2);
        std::vector<size_t> indices{0, 1};
        set.sort(indices);
        CHECK(indices.size() == 2);
        CHECK(indices[0] == 0);
        CHECK(indices[1] == 1);
    }
}
