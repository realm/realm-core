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

    CHECK_EQUAL(list.size(), 3);

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
        CHECK_EQUAL(check_results({0, 2}), true);
    }

    {
        // find null or find unresolved link diverge, different objects should be returned
        auto index = list.find_any(realm::null());
        CHECK_EQUAL(index, 0);
        index = list.find_first(obj1);
        CHECK_EQUAL(index, 2);
        // but both should look like nulls
        CHECK_EQUAL(list.is_null(0), true);
        CHECK_EQUAL(list.is_null(2), true);
    }

    {
        std::vector<size_t> indices{0, 1, 2};
        list.sort(indices);
        CHECK_EQUAL(indices.size(), 3);
        CHECK_EQUAL(indices.at(0), 0);
        CHECK_EQUAL(indices.at(1), 2);
        CHECK_EQUAL(indices.at(2), 1);
        CHECK_EQUAL(list.is_null(indices[0]), true);
        CHECK_EQUAL(list.is_null(indices[1]), true);
        CHECK_EQUAL(list.is_null(indices[2]), false);
    }

    {
        std::vector<size_t> indices{0, 1, 2};
        list.distinct(indices);
        CHECK_EQUAL(indices.size(), 2);
        CHECK_EQUAL(indices.at(0), 0);
        CHECK_EQUAL(indices.at(1), 1);
        CHECK_EQUAL(list.is_null(indices[0]), true);
        CHECK_EQUAL(list.is_null(indices[1]), false);
        CHECK_EQUAL(list.find_any(realm::null()), 0);
    }

    {
        list.remove(0);
        CHECK_EQUAL(list.find_any(obj1), 1);
        list.remove(1);
        CHECK_EQUAL(list.find_any(realm::null()), npos);
        CHECK_EQUAL(list.size(), 1);
    }

    {
        Group g;
        auto t = g.add_table("foo");
        t->add_column_list(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto list = obj.get_list<Mixed>("mixeds");

        list.insert(0, obj1);
        list.insert_null(1);
        obj1.invalidate();

        auto index_any = list.find_any(realm::null());
        auto index_first = list.find_first(realm::null());
        CHECK_EQUAL(index_any, 0);
        CHECK_EQUAL(index_first, 0);
    }

    {
        Group g;
        auto t = g.add_table("foo");
        t->add_column_list(type_Mixed, "mixeds");
        auto obj = t->create_object();
        auto obj1 = t->create_object();
        auto list = obj.get_list<Mixed>("mixeds");

        list.insert(0, obj1);
        obj1.invalidate();
        auto index_any = list.find_any(realm::null());
        auto index_first = list.find_first(realm::null());
        CHECK_EQUAL(index_any, 0);
        CHECK_EQUAL(index_first, 0);
    }
}

TEST(Mixed_Set_unresolved_links)
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

    CHECK_EQUAL(success, true);
    auto [it1, success1] = set.insert(Mixed{"test"});
    CHECK_EQUAL(success1, true);

    {
        // null can be inserted in the set
        CHECK_EQUAL(set.size(), 2);
        auto [it, success] = set.insert(Mixed{});
        CHECK_EQUAL(success, true);
        auto [it1, success1] = set.insert_null();
        CHECK_EQUAL(success1, false);
        CHECK_EQUAL(set.size(), 3);
    }

    {
        int cnt = 0;
        set.find_all(realm::null(), [this, &set, &cnt](size_t pos) {
            CHECK(pos != not_found);
            CHECK_EQUAL(set.is_null(pos), true);
            cnt += 1;
        });
        CHECK_EQUAL(cnt, 1);
    }

    {
        auto index = set.find_any(realm::null());
        CHECK(index != not_found);
        CHECK_EQUAL(set.is_null(index), true);
    }

    {
        auto [it, success] = set.insert(Mixed{obj2});
        obj2.invalidate();
        CHECK_EQUAL(success, true);
        CHECK_EQUAL(set.size(), 4);
        std::vector<size_t> indices{1, 0, 2, 3};
        set.sort(indices);
        CHECK_EQUAL(indices.size(), 4);
        CHECK_EQUAL(indices[0], 0);
        CHECK_EQUAL(indices[1], 1);
        CHECK_EQUAL(indices[2], 2);
        CHECK_EQUAL(indices[3], 3);
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
        // this should be treated as null, but for set of mixed we decided to leave unresolved exposed
        CHECK_EQUAL(set.is_null(0), false);
        CHECK_EQUAL(set.is_null(1), false);
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
            CHECK_EQUAL(index, 0);
            CHECK_EQUAL(set.is_null(index), true);
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
        CHECK_EQUAL(unresolved, 2);
    }
}

TEST(Mixed_nullify_removes_backlinks_crash)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set_null(mixed_col); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    dest_obj.remove(); // triggers an assertion failure if the backlink was not removed
    source_obj.remove();
}

TEST(Mixed_nullify_removes_backlinks_exception)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set_null(mixed_col); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.remove();
    dest_obj.remove(); // if the backlink was not removed, this creates an exception "key not found"
}

TEST(Mixed_nullify_and_invalidate_crash)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set_null(mixed_col); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    dest_obj.invalidate(); // triggers an assertion failure if the backlink was not removed
    auto resurrected = dest_table->create_object_with_primary_key({1});
    CHECK(source_obj.is_null(mixed_col));
    source_obj.remove();
    resurrected.remove();
}

TEST(Mixed_nullify_and_invalidate_exception)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set_null(mixed_col); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    dest_obj.invalidate(); // triggers an exception "key not found" if the backlink was not removed
    auto resurrected = dest_table->create_object_with_primary_key({1});
    CHECK(source_obj.is_null(mixed_col));
    CHECK(resurrected.get_backlink_count() == 0);
    resurrected.remove();
}

TEST(Mixed_set_non_link_exception)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set(mixed_col, Mixed{0}); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.remove();
    dest_obj.remove(); // triggers an exception "key not found" if the backlink was not removed
}

TEST(Mixed_set_non_link_assertion)
{
    Group g;
    auto source_table = g.add_table_with_primary_key("source", type_Int, "_id");
    auto dest_table = g.add_table_with_primary_key("dest", type_Int, "_id");
    ColKey mixed_col = source_table->add_column(type_Mixed, "mixed");
    auto source_obj = source_table->create_object_with_primary_key({0});
    auto dest_obj = dest_table->create_object_with_primary_key({1});
    CHECK(dest_obj.get_backlink_count() == 0);
    source_obj.set(mixed_col, Mixed{ObjLink{dest_table->get_key(), dest_obj.get_key()}});
    CHECK(dest_obj.get_backlink_count() == 1);
    source_obj.set(mixed_col, Mixed{0}); // needs to remove backlinks!
    CHECK(dest_obj.get_backlink_count() == 0);
    dest_obj.remove(); // triggers an assertion failure if the backlink was not removed
    source_obj.remove();
}
