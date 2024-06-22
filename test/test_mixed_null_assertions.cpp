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
    auto val = set.get(0);
    CHECK(val.is_type(type_String));
    CHECK_EQUAL(val.get_string(), "hello world");
    set.set(0, Mixed(BinaryData("hello world", 11)));
    val = set.get(0);
    CHECK(val.is_type(type_Binary));
    CHECK_EQUAL(val.get_binary(), BinaryData("hello world", 11));
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

TEST(Mixed_LinkSelfAssignment)
{
    Group g;
    auto source = g.add_table("source");
    auto dest = g.add_table("dest");
    ColKey mixed_col = source->add_column(type_Mixed, "mixed");
    auto source_obj = source->create_object();
    auto dest_obj = dest->create_object();

    CHECK_EQUAL(dest_obj.get_backlink_count(), 0);

    source_obj.set(mixed_col, Mixed{ObjLink{dest->get_key(), dest_obj.get_key()}});
    CHECK_EQUAL(dest_obj.get_backlink_count(), 1);

    // Re-assign the same link, which should not update backlinks
    source_obj.set(mixed_col, Mixed{ObjLink{dest->get_key(), dest_obj.get_key()}});
    CHECK_EQUAL(dest_obj.get_backlink_count(), 1);

    dest_obj.remove();
    CHECK_EQUAL(source_obj.get<Mixed>(mixed_col), Mixed());
    source_obj.remove();
}

TEST(Mixed_EmbeddedLstMixedRecursiveDelete)
{
    Group g;
    auto top1 = g.add_table_with_primary_key("source", type_String, "_id");
    auto embedded = g.add_table("embedded", Table::Type::Embedded);
    auto top2 = g.add_table_with_primary_key("top2", type_String, "_id");
    auto top3 = g.add_table_with_primary_key("top3", type_String, "_id");

    ColKey top1_lst_col = top1->add_column_list(*embedded, "groups");
    ColKey embedded_lst_col = embedded->add_column_list(type_Mixed, "items");
    auto source_obj1 = top1->create_object_with_primary_key("top1_obj1");

    auto top2_obj1 = top2->create_object_with_primary_key("top2_obj1");
    auto top2_obj2 = top2->create_object_with_primary_key("top2_obj2");
    auto top2_obj3 = top2->create_object_with_primary_key("top2_obj3");
    auto top2_obj4 = top2->create_object_with_primary_key("top2_obj4");

    auto top3_obj1 = top3->create_object_with_primary_key("top3_obj1");
    auto top3_obj2 = top3->create_object_with_primary_key("top3_obj2");
    auto top3_obj3 = top3->create_object_with_primary_key("top3_obj3");
    auto top3_obj4 = top3->create_object_with_primary_key("top3_obj4");

    {
        LnkLst top1_lst = source_obj1.get_linklist(top1_lst_col);
        auto embedded1 = top1_lst.create_and_insert_linked_object(0);
        auto embedded2 = top1_lst.create_and_insert_linked_object(0);
        auto embedded3 = top1_lst.create_and_insert_linked_object(0);

        auto e1_lst = embedded1.get_list<Mixed>(embedded_lst_col);
        e1_lst.add(ObjLink(top2->get_key(), top2_obj1.get_key()));
        e1_lst.add(ObjLink(top2->get_key(), top2_obj2.get_key()));
        e1_lst.add(ObjLink(top2->get_key(), top2_obj3.get_key()));
        e1_lst.add(ObjLink(top2->get_key(), top2_obj4.get_key()));

        auto e2_lst = embedded2.get_list<Mixed>(embedded_lst_col);
        e2_lst.add(ObjLink(top3->get_key(), top3_obj1.get_key()));
        e2_lst.add(ObjLink(top3->get_key(), top3_obj2.get_key()));
        e2_lst.add(ObjLink(top3->get_key(), top3_obj3.get_key()));
        e2_lst.add(ObjLink(top3->get_key(), top3_obj4.get_key()));

        auto e3_lst = embedded3.get_list<Mixed>(embedded_lst_col);
        e3_lst.add(ObjLink(top2->get_key(), top2_obj1.get_key()));
        e3_lst.add(ObjLink(top2->get_key(), top2_obj2.get_key()));
        e3_lst.add(ObjLink(top2->get_key(), top2_obj3.get_key()));
        e3_lst.add(ObjLink(top2->get_key(), top2_obj4.get_key()));
    }
    std::vector<ObjKey> keys_to_delete = {source_obj1.get_key()};

    CHECK_EQUAL(top2_obj1.get_backlink_count(), 2);
    CHECK_EQUAL(top2_obj2.get_backlink_count(), 2);
    CHECK_EQUAL(top2_obj3.get_backlink_count(), 2);
    CHECK_EQUAL(top2_obj4.get_backlink_count(), 2);

    CHECK_EQUAL(top3_obj1.get_backlink_count(), 1);
    CHECK_EQUAL(top3_obj2.get_backlink_count(), 1);
    CHECK_EQUAL(top3_obj3.get_backlink_count(), 1);
    CHECK_EQUAL(top3_obj4.get_backlink_count(), 1);

    top2_obj3.invalidate();

    _impl::TableFriend::batch_erase_objects(*top1, keys_to_delete);

    CHECK(top2_obj1.is_valid());
    CHECK(top2_obj2.is_valid());
    CHECK_NOT(top2_obj3.is_valid());
    CHECK(top2_obj4.is_valid());

    CHECK_EQUAL(top2_obj1.get_backlink_count(), 0);
    CHECK_EQUAL(top2_obj2.get_backlink_count(), 0);
    CHECK_EQUAL(top2_obj4.get_backlink_count(), 0);

    CHECK(top3_obj1.is_valid());
    CHECK(top3_obj2.is_valid());
    CHECK(top3_obj3.is_valid());
    CHECK(top3_obj4.is_valid());

    CHECK_EQUAL(top3_obj1.get_backlink_count(), 0);
    CHECK_EQUAL(top3_obj2.get_backlink_count(), 0);
    CHECK_EQUAL(top3_obj3.get_backlink_count(), 0);
    CHECK_EQUAL(top3_obj4.get_backlink_count(), 0);
}

TEST(Mixed_SingleLinkRecursiveDelete)
{
    Group g;
    auto top1 = g.add_table_with_primary_key("source", type_String, "_id");
    auto top2 = g.add_table_with_primary_key("top2", type_String, "_id");

    ColKey top1_mixed_col = top1->add_column(type_Mixed, "mixed");
    auto top1_obj1 = top1->create_object_with_primary_key("top1_obj1");
    auto top2_obj1 = top2->create_object_with_primary_key("top2_obj1");

    top1_obj1.set<Mixed>(top1_mixed_col, ObjLink{top2->get_key(), top2_obj1.get_key()});

    CHECK_EQUAL(top2_obj1.get_backlink_count(), 1);

    top1->remove_object_recursive(top1_obj1.get_key());

    CHECK_NOT(top1_obj1.is_valid());
    CHECK_EQUAL(top1->size(), 0);
    CHECK_NOT(top2_obj1.is_valid());
    CHECK_EQUAL(top2->size(), 0);
}

static void find_nested_links(Lst<Mixed>&, std::vector<ObjLink>&);

static void find_nested_links(Dictionary& dict, std::vector<ObjLink>& links)
{
    for (size_t i = 0; i < dict.size(); ++i) {
        auto [key, val] = dict.get_pair(i);
        if (val.is_type(type_TypedLink)) {
            links.push_back(val.get_link());
        }
        else if (val.is_type(type_List)) {
            std::shared_ptr<Lst<Mixed>> list_i = dict.get_list(i);
            find_nested_links(*list_i, links);
        }
        else if (val.is_type(type_Dictionary)) {
            DictionaryPtr dict_i = dict.get_dictionary(key.get_string());
            find_nested_links(*dict_i, links);
        }
    }
}

static void find_nested_links(Lst<Mixed>& list, std::vector<ObjLink>& links)
{
    for (size_t i = 0; i < list.size(); ++i) {
        Mixed val = list.get(i);
        if (val.is_type(type_TypedLink)) {
            links.push_back(val.get_link());
        }
        else if (val.is_type(type_List)) {
            std::shared_ptr<Lst<Mixed>> list_i = list.get_list(i);
            find_nested_links(*list_i, links);
        }
        else if (val.is_type(type_Dictionary)) {
            DictionaryPtr dict_i = list.get_dictionary(i);
            find_nested_links(*dict_i, links);
        }
    }
}

struct ListOfMixedLinks {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column_list(type_Mixed, "list_of_mixed");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        Lst<Mixed> lst = from.get_list<Mixed>(m_col_key);
        for (ObjLink& link : to) {
            lst.add(link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Lst<Mixed> lst = from.get_list<Mixed>(m_col_key);
        find_nested_links(lst, links);
    }
    ColKey m_col_key;
};

struct DictionaryOfMixedLinks {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column_dictionary(type_Mixed, "dict_of_mixed");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        size_t count = 0;
        Dictionary dict = from.get_dictionary(m_col_key);
        for (ObjLink& link : to) {
            dict.insert(util::format("key_%1", count++), link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Dictionary dict = from.get_dictionary(m_col_key);
        find_nested_links(dict, links);
    }
    ColKey m_col_key;
};

struct NestedDictionary {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column(type_Mixed, "nested_dictionary");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        from.set_collection(m_col_key, CollectionType::Dictionary);
        size_t count = 0;
        Dictionary dict = from.get_dictionary(m_col_key);
        for (ObjLink& link : to) {
            dict.insert(util::format("key_%1", count++), link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Dictionary dict = from.get_dictionary(m_col_key);
        find_nested_links(dict, links);
    }
    ColKey m_col_key;
};

struct NestedList {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column(type_Mixed, "nested_list");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        from.set_collection(m_col_key, CollectionType::List);
        Lst<Mixed> list = from.get_list<Mixed>(m_col_key);
        for (ObjLink& link : to) {
            list.add(link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Lst<Mixed> list = from.get_list<Mixed>(m_col_key);
        find_nested_links(list, links);
    }
    ColKey m_col_key;
};

struct NestedListOfLists {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column(type_Mixed, "nested_lols");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        from.set_collection(m_col_key, CollectionType::List);
        Lst<Mixed> list = from.get_list<Mixed>(m_col_key);
        list.add("lol_0");
        list.add("lol_1");

        for (ObjLink& link : to) {
            size_t list_ndx = list.size();
            list.insert_collection(list_ndx, CollectionType::List);

            std::shared_ptr<Lst<Mixed>> list_n = list.get_list(list_ndx);
            list_n->add(util::format("lol_%1_0", list_ndx));
            list_n->add(util::format("lol_%1_1", list_ndx));
            list_n->add(link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Lst<Mixed> list = from.get_list<Mixed>(m_col_key);
        find_nested_links(list, links);
    }
    ColKey m_col_key;
};

struct NestedDictOfDicts {
    void init_table(TableRef table)
    {
        m_col_key = table->add_column(type_Mixed, "nested_dods");
    }
    void set_links(Obj from, std::vector<ObjLink> to)
    {
        from.set_collection(m_col_key, CollectionType::Dictionary);
        Dictionary dict = from.get_dictionary(m_col_key);
        dict.insert("dict_0", 0);
        dict.insert("dict_1", 1);

        for (ObjLink& link : to) {
            std::string key = util::format("dict_%1", dict.size());
            dict.insert_collection(key, CollectionType::Dictionary);

            DictionaryPtr dict_n = dict.get_dictionary(key);
            dict_n->insert("key0", 0);
            dict_n->insert("key1", "value 1");
            dict_n->insert("link", link);
        }
    }
    void get_links(Obj from, std::vector<ObjLink>& links)
    {
        Dictionary dict = from.get_dictionary(m_col_key);
        find_nested_links(dict, links);
    }
    ColKey m_col_key;
};

TEST_TYPES(Mixed_ContainerOfLinksFromLargeCluster, ListOfMixedLinks, DictionaryOfMixedLinks, NestedDictionary,
           NestedList, NestedListOfLists, NestedDictOfDicts)
{
    Group g;
    auto top1 = g.add_table_with_primary_key("top1", type_String, "_id");
    auto top2 = g.add_table_with_primary_key("top2", type_String, "_id");
    TEST_TYPE type;
    type.init_table(top1);

    constexpr size_t num_objects = 2000; // more than BPNODE_SIZE

    for (size_t i = 0; i < num_objects; ++i) {
        auto top1_obj = top1->create_object_with_primary_key(util::format("top1_%1", i));
        auto top2_obj1 = top2->create_object_with_primary_key(util::format("top2_1_%1", i));
        auto top2_obj2 = top2->create_object_with_primary_key(util::format("top2_2_%1", i));

        std::vector<ObjLink> dest_links = {ObjLink(top2->get_key(), top2_obj1.get_key()),
                                           ObjLink(top2->get_key(), top2_obj2.get_key())};
        type.set_links(top1_obj, dest_links);
    }

    auto remove_one_object = [&](size_t ndx) {
        Obj obj_to_remove = top1->get_object(ndx);
        std::vector<ObjLink> links;
        type.get_links(obj_to_remove, links);
        CHECK_EQUAL(links.size(), 2);
        ObjLink link_1 = links[0];
        ObjLink link_2 = links[1];
        CHECK_EQUAL(link_1.get_table_key(), top2->get_key());
        CHECK_EQUAL(link_2.get_table_key(), top2->get_key());

        Obj obj_linked1 = top2->get_object(link_1.get_obj_key());
        Obj obj_linked2 = top2->get_object(link_2.get_obj_key());
        CHECK_EQUAL(obj_linked1.get_backlink_count(), 1);
        CHECK_EQUAL(obj_linked2.get_backlink_count(), 1);

        obj_to_remove.remove();
        CHECK_NOT(obj_to_remove.is_valid());
        CHECK_EQUAL(obj_linked1.get_backlink_count(), 0);
        CHECK_EQUAL(obj_linked2.get_backlink_count(), 0);
    };

    // erase at random, to exercise the collapse/join of cluster leafs
    Random random(test_util::random_int<unsigned long>()); // Seed from slow global generator
    while (!top1->is_empty()) {
        size_t rnd = random.draw_int_mod(top1->size());
        remove_one_object(rnd);
    }
}
