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
    list.insert(2, obj1);
    obj1.invalidate();

    CHECK(list.size() == 3);

    {
        // find all mixed nulls or unresolved link should work the same way
        std::vector<size_t> found;
        auto check_results = [&](std::vector<size_t> expected) -> bool {
            if (found.size() != expected.size())
                return false;

            std::sort(found.begin(), found.end());
            std::sort(expected.begin(), expected.end());
            for (size_t i = 0; i < found.size(); ++i) {
                if (found[i] != expected[i])
                    return false;
            }
            return true;
        };
        list.find_all(realm::null(), [&](size_t pos) {
            found.push_back(pos);
        });
        CHECK(check_results({0, 2}));
        found = {};
        list.find_all(obj1, [&](size_t pos) {
            found.push_back(pos);
        });
        CHECK(check_results({2}));
    }

    {
        // find null or find unresolved link diverge, different objects should be returned
        auto index = list.find_any(realm::null());
        CHECK(index == 0);
        index = list.find_first(obj1);
        CHECK(index == 2);
        // but both should look like nulls
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
        CHECK(list.find_any(obj1) == 1);
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
    auto set = obj.get_set<Mixed>("mixeds");
    auto [it, success] = set.insert(Mixed{obj1});
    obj1.invalidate();

    CHECK(success);
    auto [it1, success1] = set.insert(Mixed{"test"});
    CHECK(success1);

    {
        // null can be inserted in the set
        CHECK(set.size() == 2);
        auto [it, success] = set.insert(Mixed{});
        CHECK(success);
        auto [it1, success1] = set.insert_null();
        CHECK(!success1);
        CHECK(set.size() == 3);
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
        obj2.invalidate();
        CHECK(success);
        CHECK(set.size() == 4);
        std::vector<size_t> indices{1, 0, 2, 3};
        set.sort(indices);
        CHECK(indices.size() == 4);
        CHECK(indices[0] == 0);
        CHECK(indices[1] == 1);
        CHECK(indices[2] == 2);
        CHECK(indices[3] == 3);
    }

    {
        // erase null but there are only unresolved links in the set
        Group g;
        auto t = g.add_table("foo");
        t->add_column_set(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto obj2 = t->create_object();
        auto set = obj.get_set<Mixed>("mixeds");
        set.insert(obj1);
        set.insert(obj2);
        CHECK_EQUAL(set.size(), 2);
        obj1.invalidate();
        obj2.invalidate();
        CHECK(set.is_null(0));
        CHECK(set.is_null(1));
        set.insert(Mixed{1});
        CHECK_EQUAL(set.size(), 3);
        set.erase_null();
        CHECK_EQUAL(set.size(), 3);
        set.erase(Mixed{});
        CHECK_EQUAL(set.size(), 3);
    }

    {
        // erase null when there are unresolved and nulls
        Group g;
        auto t = g.add_table("foo");
        t->add_column_set(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto obj2 = t->create_object();
        auto set = obj.get_set<Mixed>("mixeds");
        set.insert(obj1);
        set.insert(obj2);
        set.insert(Mixed{});
        CHECK_EQUAL(set.size(), 3);
        obj1.invalidate();
        obj2.invalidate();
        size_t cnt = 0;
        set.find_all(Mixed{}, [this, &set, &cnt](size_t index) {
            CHECK(index == 0);
            CHECK(set.is_null(index));
            cnt += 1;
        });
        CHECK_EQUAL(cnt, 1);
        set.erase(Mixed{});
        CHECK_EQUAL(set.size(), 2);
    }

    {
        // assure that random access iterator does not return an unresolved link
        Group g;
        auto t = g.add_table("foo");
        t->add_column_set(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto obj2 = t->create_object();
        auto set = obj.get_set<Mixed>("mixeds");
        set.insert(obj1);
        set.insert(obj2);
        obj1.invalidate();
        obj2.invalidate();
        set.insert(Mixed{});
        size_t unresolved = 0;
        size_t null = 0;
        for (auto& mixed : set) {
            if (mixed.is_null())
                null += 1;
            if (mixed.is_unresolved_link())
                unresolved += 1;
        }
        CHECK_EQUAL(null, 1);
        CHECK_EQUAL(unresolved,
                    2); // this is a limitation, iterating through the set we can still expose unresolved links
    }
}
