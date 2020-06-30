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

#include "testsettings.hpp"
#ifdef TEST_LINKS


#include <realm.hpp>
#include <realm/util/file.hpp>
#include <realm/array_key.hpp>
#include <realm/history.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

template <class T>
void test_table_add_columns(T t)
{
    t->add_column(type_String, "first");
    t->add_column(type_Int, "second");
    t->add_column(type_Bool, "third");
    t->add_column(type_Int, "fourth");
}

} // Anonymous namespace

TEST(Links_Columns)
{
    // Test adding and removing columns with links

    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // table1 can link to table2
    table2->add_column_link(type_Link, "link", *table1);

    // add some more columns to table1 and table2
    auto col_1 = table1->add_column(type_String, "col1");
    table2->add_column(type_String, "col2");

    // Use a key where the first has the the second most significant bit set.
    // When this is shifted up and down again, the most significant bit must
    // still be 0.
    ObjKeys table_1_keys({5, 1LL << 62});
    ObjKeys table_2_keys({2, 7});
    // add some rows
    table1->create_objects(table_1_keys);
    table2->create_objects(table_2_keys);

    table1->get_object(table_1_keys[0]).set<String>(col_1, "string1");
    auto col_link2 = table1->add_column_link(type_Link, "link", *table2);

    // set some links
    table1->get_object(table_1_keys[0]).set(col_link2, table_2_keys[1]);
    Obj obj1 = table2->get_object(table_2_keys[1]);
    table1->get_object(table_1_keys[1]).set(col_link2, table_2_keys[0]);
    Obj obj0 = table2->get_object(table_2_keys[0]);

    CHECK_EQUAL(1, obj0.get_backlink_count(*table1, col_link2));
    CHECK_EQUAL(table_1_keys[1], obj0.get_backlink(*table1, col_link2, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*table1, col_link2));
    CHECK_EQUAL(table_1_keys[0], obj1.get_backlink(*table1, col_link2, 0));

    auto tv = obj1.get_backlink_view(table1, col_link2);
    CHECK_EQUAL(tv.size(), 1);

    // remove a column (moving link column back)'
    table1->remove_column(col_1);
    CHECK_EQUAL(1, table2->get_object(table_2_keys[1]).get_backlink_count(*table1, col_link2));
    CHECK_EQUAL(table_1_keys[0], table2->get_object(table_2_keys[1]).get_backlink(*table1, col_link2, 0));

    table1->remove_column(col_link2);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 0);
}


TEST(Links_Basic)
{
    GROUP_TEST_PATH(path);
    ObjKey key_origin;
    ObjKey key_target;
    ColKey col_link;

    // Test basic link operations
    {
        Group group;

        auto table1 = group.add_table("table1");
        table1->add_column(type_String, "first");
        table1->add_column(type_Int, "second");
        table1->add_column(type_Bool, "third");
        table1->add_column(type_Int, "fourth");

        Obj obj0 = table1->create_object().set_all("test1", 1, true, int64_t(Mon));
        Obj obj1 = table1->create_object().set_all("test2", 2, false, int64_t(Tue));
        Obj obj2 = table1->create_object().set_all("test3", 3, true, int64_t(Wed));
        ObjKey key0 = obj0.get_key();
        ObjKey key1 = obj1.get_key();
        ObjKey key2 = obj2.get_key();

        // create table with links to table1
        TableRef table2 = group.add_table("table2");
        col_link = table2->add_column_link(type_Link, "link", *table1);
        CHECK_EQUAL(table1, table2->get_link_target(col_link));

        // add a few links
        Obj obj3 = table2->create_object(ObjKey{}, {{col_link, key1}});
        Obj obj4 = table2->create_object(ObjKey{}, {{col_link, key0}});
        ObjKey key3 = obj3.get_key();
        key_origin = obj4.get_key();

        // Verify that links were set correctly
        ObjKey link3 = obj3.get<ObjKey>(col_link);
        ObjKey link4 = obj4.get<ObjKey>(col_link);
        CHECK_EQUAL(key1, link3);
        CHECK_EQUAL(key0, link4);

        // Verify backlinks
        CHECK_EQUAL(1, obj0.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(key_origin, obj0.get_backlink(*table2, col_link, 0));
        CHECK_EQUAL(1, obj1.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(key3, obj1.get_backlink(*table2, col_link, 0));
        CHECK_EQUAL(0, obj2.get_backlink_count(*table2, col_link));

        // Change a link to point to a new location
        obj4.set(col_link, key2);

        link4 = obj4.get<ObjKey>(col_link);
        CHECK_EQUAL(key2, link4);
        CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(1, obj2.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(key_origin, obj2.get_backlink(*table2, col_link, 0));

        // Delete an object.
        table2->remove_object(key3);

        // Verify that delete went correctly
        CHECK_EQUAL(1, table2->size());
        CHECK_EQUAL(key2, obj4.get<ObjKey>(col_link));

        CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(0, obj1.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(1, obj2.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(key_origin, obj2.get_backlink(*table2, col_link, 0));

        // Nullify a link
        obj4.set(col_link, null_key);
        CHECK(obj4.is_null(col_link));
        CHECK_EQUAL(0, obj2.get_backlink_count(*table2, col_link));

        // Add a new row to target table and verify that backlinks are
        // tracked for it as well
        Obj obj5 = table1->create_object().set_all("test4", 4, false, int64_t(Thu));
        key_target = obj5.get_key();
        CHECK_EQUAL(0, obj5.get_backlink_count(*table2, col_link));

        obj4.set(col_link, key_target);
        CHECK_EQUAL(1, obj5.get_backlink_count(*table2, col_link));

        group.write(path);
    }

    // Reopen same group from disk
    {
        Group group(path);

        TableRef table1 = group.get_table("table1");
        TableRef table2 = group.get_table("table2");

        // Verify that we are pointing to the right table
        CHECK_EQUAL(table1, table2->get_link_target(col_link));

        // Verify that links are still correct
        CHECK_EQUAL(key_target, table2->get_object(key_origin).get<ObjKey>(col_link));
    }
}

TEST(Group_LinksToSameTable)
{
    Group g;
    TableRef table = g.add_table("target");

    table->add_column(type_Int, "integers", true);
    auto link_col = table->add_column_link(type_Link, "links", *table);

    // 3 rows linked together in a list
    std::vector<ObjKey> keys;
    table->create_objects(3, keys);
    table->get_object(keys[0]).set(link_col, keys[1]);
    table->get_object(keys[1]).set(link_col, keys[2]);
    table->remove_object(keys[0]);
    CHECK_EQUAL(table->size(), 2);
    CHECK_EQUAL(table->get_object(keys[1]).get_backlink_count(*table, link_col), 0);
    table->remove_object(keys[2]);
    CHECK_EQUAL(table->size(), 1);
    CHECK(table->get_object(keys[1]).is_null(link_col));
}

TEST(Links_SetLinkLogicErrors)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    auto col0 = origin->add_column_link(type_Link, "a", *target);
    origin->add_column(type_Int, "b");
    Obj obj = origin->create_object();
    target->create_object(ObjKey(10));

    // FIXME: Not really possible with column keys?
    // CHECK_LOGIC_ERROR(obj.set(2, Key(10)), LogicError::column_index_out_of_range);
    CHECK_LOGIC_ERROR(obj.set(col0, ObjKey(5)), LogicError::target_row_index_out_of_range);

    // FIXME: Must also check that Logic::type_mismatch is thrown on column type mismatch, but Table::set_link() does
    // not properly check it yet.

    origin->remove_object(obj.get_key());
    CHECK_THROW(obj.set(col0, ObjKey(10)), KeyNotFound);

    group.remove_table("origin");
    CHECK_THROW(obj.set(col0, ObjKey(10)), realm::NoSuchTable);
}


TEST(Links_Deletes)
{
    Group group;

    auto table1 = group.add_table("table1");
    table1->add_column(type_String, "first");
    table1->add_column(type_Int, "second");
    table1->add_column(type_Bool, "third");
    table1->add_column(type_Int, "fourth");

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    auto col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    Obj obj0 = table1->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = table1->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = table1->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key0 = obj0.get_key();
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    {
        // add a few links
        Obj obj3 = table2->create_object().set(col_link, key1);
        Obj obj4 = table2->create_object().set(col_link, key0);
        ObjKey key3 = obj3.get_key();
        ObjKey key4 = obj4.get_key();

        table2->remove_object(key3);
        table2->remove_object(key4);
    }
    CHECK(table2->is_empty());
    CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link));
    CHECK_EQUAL(0, obj1.get_backlink_count(*table2, col_link));

    // add links again
    table2->create_object().set(col_link, key1);
    table2->create_object().set(col_link, key0);

    // remove all rows in target table
    table1->remove_object(key0);
    table1->remove_object(key1);
    table1->remove_object(key2);

    // verify that originating links was nullified
    for (auto o : *table2) {
        CHECK(o.is_null(col_link));
    }

    // add target rows again with links
    table1->create_object().set_all("test1", 1, true, int64_t(Mon));
    table1->create_object().set_all("test2", 2, false, int64_t(Tue));
    table1->create_object().set_all("test3", 3, true, int64_t(Wed));

    auto it = table1->begin();
    for (auto o : *table2) {
        o.set(col_link, it->get_key());
        ++it;
    }

    // clear entire table and make sure backlinks are removed as well
    table2->clear();
    for (auto o : *table1) {
        CHECK_EQUAL(0, o.get_backlink_count(*table2, col_link));
    }

    // add links again
    for (auto o : *table1) {
        table2->create_object().set(col_link, o.get_key());
    }

    // clear target table and make sure links are nullified
    table1->clear();
    for (auto o : *table2) {
        CHECK(o.is_null(col_link));
    }
}

TEST(Links_Multi)
{
    // Multiple links to same rows
    Group group;

    auto table1 = group.add_table("target");
    test_table_add_columns(table1);
    table1->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = table1->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = table1->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    auto col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links pointing to same row
    auto k0 = table2->create_object().set(col_link, key1).get_key();
    auto k1 = table2->create_object().set(col_link, key1).get_key();
    auto k2 = table2->create_object().set(col_link, key1).get_key();

    CHECK_EQUAL(3, obj1.get_backlink_count(*table2, col_link));
    CHECK_EQUAL(k0, obj1.get_backlink(*table2, col_link, 0));
    CHECK_EQUAL(k1, obj1.get_backlink(*table2, col_link, 1));
    CHECK_EQUAL(k2, obj1.get_backlink(*table2, col_link, 2));

    // nullify a link
    table2->get_object(k1).set_null(col_link);
    CHECK_EQUAL(2, obj1.get_backlink_count(*table2, col_link));
    CHECK_EQUAL(k0, obj1.get_backlink(*table2, col_link, 0));
    CHECK_EQUAL(k2, obj1.get_backlink(*table2, col_link, 1));

    // nullify one more to reduce to one link (test re-inlining)
    table2->get_object(k0).set_null(col_link);
    CHECK_EQUAL(1, obj1.get_backlink_count(*table2, col_link));
    CHECK_EQUAL(k2, obj1.get_backlink(*table2, col_link, 0));

    // re-add links
    table2->get_object(k0).set(col_link, k1);
    table2->get_object(k1).set(col_link, k1);

    // remove a row
    table2->remove_object(k0);
    CHECK_EQUAL(2, obj1.get_backlink_count(*table2, col_link));
    CHECK_EQUAL(k2, obj1.get_backlink(*table2, col_link, 0));
    CHECK_EQUAL(k1, obj1.get_backlink(*table2, col_link, 1));

    // add some more links and see that they get nullified when the target
    // is removed
    auto k3 = table2->create_object().set(col_link, key2).get_key();
    auto k4 = table2->create_object().set(col_link, key2).get_key();
    CHECK_EQUAL(2, obj2.get_backlink_count(*table2, col_link));

    obj1.remove();
    CHECK(table2->get_object(k1).is_null(col_link));
    CHECK(table2->get_object(k2).is_null(col_link));
    CHECK_NOT(table2->get_object(k3).is_null(col_link));
    CHECK_NOT(table2->get_object(k4).is_null(col_link));

    // remove all rows from target and verify that links get nullified
    table1->clear();
    for (auto o : *table2) {
        CHECK(o.is_null(col_link));
    }
}


TEST(Links_MultiToSame)
{
    Group group;

    auto table1 = group.add_table("target");
    test_table_add_columns(table1);
    Obj obj0 = table1->create_object().set_all("test1", 1, true, int64_t(Mon));
    ObjKey key0 = obj0.get_key();

    // create table with multiple links to table1
    TableRef table2 = group.add_table("table2");
    auto col_link1 = table2->add_column_link(type_Link, "link1", *table1);
    auto col_link2 = table2->add_column_link(type_Link, "link2", *table1);
    CHECK_EQUAL(table1, table2->get_link_target(col_link1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link2));

    table2->create_object().set_all(key0, key0);
    CHECK_EQUAL(1, obj0.get_backlink_count(*table2, col_link1));
    CHECK_EQUAL(1, obj0.get_backlink_count(*table2, col_link2));

    table2->begin()->remove();
    CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link1));
    CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link2));
}

TEST(Links_LinkList_TableOps)
{
    Group group;

    auto target = group.add_table("target");
    target->add_column(type_String, "first");
    target->add_column(type_Int, "second");
    target->add_column(type_Bool, "third");
    target->add_column(type_Int, "fourth");

    // create table with links to table1
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    target->create_object().set_all("test1", 1, true, int64_t(Mon));
    target->create_object().set_all("test2", 2, false, int64_t(Tue));
    target->create_object().set_all("test3", 3, true, int64_t(Wed));

    const Obj obj1 = origin->create_object();
    CHECK(obj1.get_list<ObjKey>(col_link).is_empty());
    CHECK_EQUAL(0, obj1.get_link_count(col_link));

    // add some more rows and test that they can be deleted
    origin->create_object();
    origin->create_object();
    origin->create_object();

    while (!origin->is_empty()) {
        origin->remove_object(origin->begin()->get_key());
    }

    // add some more rows and clear
    origin->create_object();
    origin->create_object();
    origin->create_object();
    origin->clear();
}


TEST(Links_LinkList_Construction)
{
    Group group;

    auto table1 = group.add_table("target");

    // create table with links to table1
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *table1);
    CHECK_EQUAL(table1, origin->get_link_target(col_link));

    ObjKeys target_keys({4, 5, 6});
    table1->create_objects(target_keys);

    auto links0 = origin->create_object().get_linklist(col_link);
    auto links1 = origin->create_object().get_linklist(col_link);

    // add several links to a single linklist
    links1.add(target_keys[0]);
    links1.add(target_keys[1]);
    links1.add(target_keys[2]);

    CHECK_EQUAL(0, links0.size());
    CHECK_EQUAL(3, links1.size());

    LnkLst default_list; // Default constructed list
    CHECK_EQUAL(0, default_list.size());

    LnkLst list0(links0); // Constructed from empty list
    CHECK_EQUAL(0, list0.size());
    list0.add(target_keys[0]); // Ensure usability
    list0.clear();             // Make it empty again

    LnkLst list1(links1); // Constructed from not empty list
    CHECK_EQUAL(3, list1.size());
    list1.clear();
    CHECK_EQUAL(0, list1.size());
    list1.add(target_keys[0]);
    list1.add(target_keys[1]);
    list1.add(target_keys[2]);

    LnkLst list2 = default_list; // Constructed from default object
    CHECK_EQUAL(0, list2.size());

    list1 = default_list; // Assignment from default
    CHECK_EQUAL(0, list1.size());

    list1 = links0; // Assignment from empty list
    CHECK_EQUAL(0, list1.size());
    list1.add(target_keys[0]);
    list1.clear();

    list2 = links1; // Assignment from non empty list
    CHECK_EQUAL(3, list2.size());
    list2.clear();
    CHECK_EQUAL(0, list2.size());
}

TEST(Links_LinkList_Basics)
{
    Group group;

    auto target = group.add_table("target");
    target->add_column(type_String, "first");
    target->add_column(type_Int, "second");
    target->add_column(type_Bool, "third");
    auto day_col = target->add_column(type_Int, "fourth");

    // create table with links to table1
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    origin->add_column(type_Int, "integers"); // Make sure the link column is not the only column
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    Obj obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key0 = obj0.get_key();
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    Obj obj3 = origin->create_object(ObjKey(0));
    ObjKey key3 = obj3.get_key();
    auto links = obj3.get_linklist(col_link);

    // add several links to a single linklist
    links.add(key2);
    links.add(key1);
    links.add(key0);

    CHECK_EQUAL(3, obj3.get_link_count(col_link));
    CHECK_EQUAL(key2, links.get(0));
    CHECK_EQUAL(key1, links.get(1));
    CHECK_EQUAL(key0, links.get(2));
    CHECK_EQUAL(Wed, Days(links[0].get<Int>(day_col)));

    LnkLst links2 = links;

    CHECK_EQUAL(3, links2.size());
    ObjList* obj_list = &links2;
    CHECK_EQUAL(key2, obj_list->get_key(0));

    // verify that backlinks was set correctly
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 0));

    // insert a link at a specific position in the linklist
    links.insert(1, key2);
    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key2, links.get(0));
    CHECK_EQUAL(key2, links.get(1));
    CHECK_EQUAL(key1, links.get(2));
    CHECK_EQUAL(key0, links.get(3));

    CHECK_EQUAL(2, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 1));

    // change one link to another (replace key2 with key1)
    links.set(0, key1);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key1, links.get(0));
    CHECK_EQUAL(key2, links.get(1));
    CHECK_EQUAL(key1, links.get(2));
    CHECK_EQUAL(key0, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key1, links2.get(0));
    CHECK_EQUAL(key2, links2.get(1));
    CHECK_EQUAL(key1, links2.get(2));
    CHECK_EQUAL(key0, links2.get(3));

    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(2, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(key3, obj1.get_backlink(*origin, col_link, 1));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 0));

    // move a link
    links.move(3, 0);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0));
    CHECK_EQUAL(key1, links.get(1));
    CHECK_EQUAL(key2, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key0, links2.get(0));
    CHECK_EQUAL(key1, links2.get(1));
    CHECK_EQUAL(key2, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    links.move(0, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key1, links.get(0));
    CHECK_EQUAL(key2, links.get(1));
    CHECK_EQUAL(key0, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key1, links2.get(0));
    CHECK_EQUAL(key2, links2.get(1));
    CHECK_EQUAL(key0, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    links.move(2, 0);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0));
    CHECK_EQUAL(key1, links.get(1));
    CHECK_EQUAL(key2, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key0, links2.get(0));
    CHECK_EQUAL(key1, links2.get(1));
    CHECK_EQUAL(key2, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    links.move(2, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0));
    CHECK_EQUAL(key1, links.get(1));
    CHECK_EQUAL(key2, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key0, links2.get(0));
    CHECK_EQUAL(key1, links2.get(1));
    CHECK_EQUAL(key2, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    // swap two links
    links.swap(1, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0));
    CHECK_EQUAL(key2, links.get(1));
    CHECK_EQUAL(key1, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key0, links2.get(0));
    CHECK_EQUAL(key2, links2.get(1));
    CHECK_EQUAL(key1, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    // swap a link with itself
    links.swap(2, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0));
    CHECK_EQUAL(key2, links.get(1));
    CHECK_EQUAL(key1, links.get(2));
    CHECK_EQUAL(key1, links.get(3));

    CHECK_EQUAL(4, links2.size());
    CHECK_EQUAL(key0, links2.get(0));
    CHECK_EQUAL(key2, links2.get(1));
    CHECK_EQUAL(key1, links2.get(2));
    CHECK_EQUAL(key1, links2.get(3));

    // remove a link
    links.remove(0);
    CHECK_EQUAL(3, links2.size());
    CHECK_EQUAL(3, obj3.get_link_count(col_link));
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));

    // remove all links
    links.clear();
    CHECK_EQUAL(0, links2.size());
    CHECK_EQUAL(0, obj3.get_link_count(col_link));
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));

    // Add links again
    links.add(key2);
    links.add(key1);
    links.add(key0);

    CHECK_EQUAL(3, links2.size());

    // verify that backlinks were set
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));

    origin->remove_object(key3);
    // verify that backlinks were removed
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));
}

TEST(ListList_Clear)
{
    SHARED_GROUP_TEST_PATH(path);
    std::unique_ptr<Replication> hist(make_in_realm_history(path));
    DBRef db = DB::create(*hist);
    auto group = db->start_write();

    auto target = group->add_table("target");
    target->add_column(type_Int, "value");

    TableRef origin = group->add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);

    ObjKey key0 = target->create_object().set_all(1).get_key();
    ObjKey key1 = target->create_object().set_all(2).get_key();

    Obj obj3 = origin->create_object(ObjKey(0));
    auto links = obj3.get_linklist(col_link);

    links.add(key0);
    links.add(key1);

    group->commit_and_continue_as_read();
    group->promote_to_write();

    if (links.size() > 1)
        links.set(1, key0);
    links.clear();
}

TEST(Links_AddBacklinkToTableWithEnumColumns)
{
    Group g;
    auto table = g.add_table("fshno");
    auto col = table->add_column(type_String, "strings", false);
    table->create_object();
    table->add_column_link(type_Link, "link1", *table);
    table->enumerate_string_column(col);
    table->add_column_link(type_Link, "link2", *table);
}

TEST(Links_LinkList_Inserts)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    auto col_date = target->get_column_key("fourth");
    Obj obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key0 = obj0.get_key();
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    auto obj = origin->create_object();
    auto links = obj.get_linklist_ptr(col_link);
    auto links2 = obj.get_linklist_ptr(col_link);
    auto k0 = links->CollectionBase::get_key();

    CHECK_EQUAL(0, links->size());
    CHECK_EQUAL(0, links2->size());

    // add several links to a single linklist
    links->add(key2);
    links->add(key1);
    links->add(key0);

    CHECK_EQUAL(3, links->size());
    CHECK_EQUAL(key2, links->get(0));
    CHECK_EQUAL(key1, links->get(1));
    CHECK_EQUAL(key0, links->get(2));
    CHECK_EQUAL(Wed, Days((*links)[0].get<Int>(col_date)));

    CHECK_EQUAL(3, links2->size());
    CHECK_EQUAL(key2, links2->get(0));
    CHECK_EQUAL(key1, links2->get(1));
    CHECK_EQUAL(key0, links2->get(2));
    CHECK_EQUAL(Wed, Days((*links2)[0].get<Int>(col_date)));

    // verify that backlinks was set correctly
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k0, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k0, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k0, obj2.get_backlink(*origin, col_link, 0));
}

TEST(Links_LinkList_Backlinks)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    Obj obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key0 = obj0.get_key();
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    Obj origin_obj = origin->create_object();
    auto links = origin_obj.get_linklist_ptr(col_link);
    auto k0 = links->CollectionBase::get_key();

    // add several links to a single linklist
    links->add(key2);
    links->add(key1);
    links->add(key0);
    links->add(key2);
    links->add(key1);
    links->add(key0);

    group.verify();

    // remove a target row and check that origin links are removed as well
    target->remove_object(key1);
    CHECK_EQUAL(4, origin_obj.get_link_count(col_link));
    CHECK_EQUAL(key2, links->get(0));
    CHECK_EQUAL(key0, links->get(1));

    // remove all
    target->clear();
    CHECK_EQUAL(0, origin_obj.get_link_count(col_link));
    CHECK(links->is_empty());

    // re-add rows to target
    obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));

    // add more rows with links
    links->add(obj2.get_key());
    auto links1 = origin->create_object().get_linklist_ptr(col_link);
    auto links2 = origin->create_object().get_linklist_ptr(col_link);
    links1->add(obj1.get_key());
    links2->add(obj0.get_key());
    auto k1 = links1->CollectionBase::get_key();
    auto k2 = links2->CollectionBase::get_key();

    // Verify backlinks
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k2, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k1, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k0, obj2.get_backlink(*origin, col_link, 0));

    // delete a row and make sure backlinks are updated
    origin->remove_object(k0);
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k2, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k1, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));

    // delete last row and make sure backlinks are updated
    origin->remove_object(k2);
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(k1, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));

    // remove all link lists and make sure backlinks are updated
    origin->clear();
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));
}


TEST(Links_LinkList_FindByOrigin)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    Obj obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));
    ObjKey key0 = obj0.get_key();
    ObjKey key1 = obj1.get_key();
    ObjKey key2 = obj2.get_key();

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    auto obj = origin->create_object();
    auto links = obj.get_linklist_ptr(col_link);
    auto links2 = obj.get_linklist_ptr(col_link);
    CHECK_EQUAL(not_found, links->find_first(key2));
    CHECK_EQUAL(not_found, links2->find_first(key2));

    links->find_all(key2, [&](size_t) { CHECK(false); });
    links2->find_all(key2, [&](size_t) { CHECK(false); });

    links->add(key2);
    links->add(key1);
    links->add(key0);

    CHECK_EQUAL(0, links->find_first(key2));
    CHECK_EQUAL(1, links->find_first(key1));
    CHECK_EQUAL(2, links->find_first(key0));
    CHECK_EQUAL(0, links2->find_first(key2));
    CHECK_EQUAL(1, links2->find_first(key1));
    CHECK_EQUAL(2, links2->find_first(key0));

    int calls = 0;
    links->find_all(key2, [&](size_t i) { CHECK_EQUAL(i, 0); ++calls; });
    CHECK_EQUAL(calls, 1);
    calls = 0;
    links2->find_all(key0, [&](size_t i) { CHECK_EQUAL(i, 2); ++calls; });
    CHECK_EQUAL(calls, 1);

    links->remove(0);

    CHECK_EQUAL(not_found, links->find_first(key2));
    CHECK_EQUAL(not_found, links2->find_first(key2));

    links->add(key0);
    links->add(key0);

    calls = 0;
    links->find_all(key0, [&](size_t i) { CHECK(i >= 1); ++calls; });
    CHECK_EQUAL(calls, 3);
}


TEST(Links_CircularAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    auto db = DB::create(path);
    ColKey col1;
    ColKey col2;
    {
        WriteTransaction wt(db);
        TableRef table1 = wt.add_table("table1");
        TableRef table2 = wt.add_table("table2");
        col1 = table1->add_column_link(type_Link, "link", *table2);
        col2 = table2->add_column_link(type_Link, "link", *table1);
        CHECK_EQUAL(table1, table2->get_link_target(col1));
        CHECK_EQUAL(table2, table1->get_link_target(col2));
        wt.commit();
    }
    {
        WriteTransaction wt(db);
        TableRef table1 = wt.get_table("table1");
        TableRef table2 = wt.get_table("table2");
        CHECK_EQUAL(table1, table2->get_link_target(col1));
        CHECK_EQUAL(table2, table1->get_link_target(col2));
    }
}


TEST(Links_Transactions)
{
    SHARED_GROUP_TEST_PATH(path);
    auto hist = make_in_realm_history(path);
    auto db = DB::create(*hist);

    ColKey dog_col;
    ObjKey tim_key;
    ObjKey harvey_key;

    {
        WriteTransaction group(db);

        // Create dogs table
        TableRef dogs = group.add_table("dogs");
        dogs->add_column(type_String, "dogName");

        // Create owners table
        TableRef owners = group.add_table("owners");
        owners->add_column(type_String, "name");
        dog_col = owners->add_column_link(type_Link, "dog", *dogs);

        // Insert a single dog
        harvey_key = dogs->create_object().set_all("Harvey").get_key();

        // Insert an owner with link to dog
        tim_key = owners->create_object().set_all("Tim", harvey_key).get_key();

        group.commit();
    }

    auto rt = db->start_read();
    ConstTableRef owners = rt->get_table("owners");
    ConstTableRef dogs = rt->get_table("dogs");
    const Obj tim = owners->get_object(tim_key);
    CHECK_NOT(tim.is_null(dog_col));
    CHECK_EQUAL(harvey_key, tim.get<ObjKey>(dog_col));
    const Obj harvey = dogs->get_object(harvey_key);
    CHECK_EQUAL(harvey.get_backlink_count(), 1);

    {
        // Add another another owner for Harvey
        WriteTransaction wt(db);
        wt.get_table("owners")->create_object().set_all("Tim", harvey_key);
        wt.commit();
    }

    rt->advance_read();
    CHECK_EQUAL(harvey.get_backlink_count(), 2);

    {
        // Delete dog
        WriteTransaction wt(db);
        wt.get_table("dogs")->remove_object(harvey_key);
        wt.commit();
    }

    // Verify that link from owner was nullified
    rt->advance_read();
    CHECK(tim.is_null(dog_col));
}

#if !REALM_ANDROID // FIXME
// When compiling for Android (armeabi-v7a) you will get this error:
// internal compiler error: in possible_polymorphic_call_targets, at ipa-devirt.c:1556
TEST(Links_RemoveTargetRows)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    auto k0 = target->create_object().set_all("test1", 1, true, int(Mon)).get_key();
    auto k1 = target->create_object().set_all("test2", 2, false, int(Tue)).get_key();
    auto k2 = target->create_object().set_all("test3", 3, true, int(Wed)).get_key();

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    auto col_link = origin->add_column_link(type_LinkList, "links", *target);

    Obj obj = origin->create_object();
    auto links = obj.get_linklist(col_link);
    links.add(k2);
    links.add(k1);
    links.add(k0);

    // delete target rows through the links one at a time
    links.remove_target_row(0);
    CHECK_EQUAL(2, target->size());
    CHECK_EQUAL(2, links.size());

    links.remove_target_row(1);
    CHECK_EQUAL(1, target->size());
    CHECK_EQUAL(1, links.size());

    links.remove_target_row(0);
    CHECK_EQUAL(0, target->size());
    CHECK_EQUAL(0, links.size());

    // re-add targets and links
    k0 = target->create_object().set_all("test1", 1, true, int(Mon)).get_key();
    k1 = target->create_object().set_all("test2", 2, false, int(Tue)).get_key();
    k2 = target->create_object().set_all("test3", 3, true, int(Wed)).get_key();
    links.add(k2);
    links.add(k1);
    links.add(k0);

    // Remove all targets through the links
    links.remove_all_target_rows();
    CHECK(target->is_empty());
    CHECK(links.is_empty());
}
#endif

TEST(Links_ClearColumnWithTwoLevelBptree)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");

    // The extra columns beyond the first one increase the likelihood of
    // getting unambiguously bad ref
    target->add_column(type_Int, "i1");
    target->add_column(type_Int, "i2");
    target->add_column(type_Int, "i3");
    target->add_column(type_Int, "i4");
    target->add_column(type_Int, "i5");
    Obj obj = target->create_object();

    auto col = origin->add_column_link(type_LinkList, "", *target);
    std::vector<ObjKey> keys;
    origin->create_objects(REALM_MAX_BPNODE_SIZE + 1, keys);
    origin->clear();
    origin->create_object().get_linklist(col).add(obj.get_key());
    group.verify();
}


TEST(Links_ClearLinkListWithTwoLevelBptree)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    auto col_link = origin->add_column_link(type_LinkList, "", *target);
    ObjKey k = target->create_object().get_key();
    auto ll = origin->create_object().get_linklist(col_link);
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
        ll.add(k);
    ll.clear();
    group.verify();
}


TEST(Links_FormerMemLeakCase)
{
    SHARED_GROUP_TEST_PATH(path);
    auto sg_w = DB::create(path);
    {
        WriteTransaction wt(sg_w);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        target->add_column(type_Int, "int");
        auto k = target->create_object().get_key();
        auto col = origin->add_column_link(type_Link, "link", *target);
        origin->create_object().set(col, k);
        origin->create_object().set(col, k);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        TableRef target = wt.get_table("target");
        target->begin()->remove();
        wt.get_group().verify();
        wt.commit();
    }
}

TEST(Links_CascadeRemove_ColumnLink)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_embedded_table("target");
        std::vector<ObjKey> origin_keys;
        std::vector<ObjKey> target_keys;
        ColKey col_link;
        Fixture()
        {
            target->add_column(type_Int, "t_1");
            col_link = origin->add_column_link(type_Link, "o_1", *target);
            for (int i = 0; i < 3; ++i) {
                auto oo = origin->create_object();
                auto to = oo.create_and_set_linked_object(col_link);
                origin_keys.push_back(oo.get_key());
                target_keys.push_back(to.get_key());
            }
        }
        Obj get_origin_obj(int i)
        {
            return origin->get_object(origin_keys[i]);
        }
        Obj get_target_obj(int i)
        {
            return target->get_object(origin_keys[i]);
        }
    };

    // Break link by nullifying
    {
        Fixture f;
        f.get_origin_obj(0).set(f.col_link, null_key); // origin[0].o_1 -> realm::null()
        // Cascade: target->remove_object(key[0])
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).set(f.col_link, null_key); // origin[1].o_1 -> realm::null()
        // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).set(f.col_link, null_key); // origin[0].o_1 -> realm::null()
        // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
    }
    // Break link by reassign
    {
        Fixture f;
        f.get_origin_obj(0).create_and_set_linked_object(f.col_link);
        // Cascade: target->remove_object(key[0])
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).create_and_set_linked_object(f.col_link);
        // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).create_and_set_linked_object(f.col_link);
        // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
    }
    // Break link by explicit object removal
    {
        Fixture f;
        f.get_origin_obj(0).remove(); // Cascade: target->remove_object(key[0])
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).remove(); // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<ObjKey>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).remove(); // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<ObjKey>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<ObjKey>(f.col_link));
    }

    // Break link by clearing table
    {
        Fixture f;
        f.origin->clear();
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(!f.target->is_valid(f.target_keys[2]));
    }
}


TEST(Links_CascadeRemove_ColumnLinkList)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_embedded_table("target");
        std::vector<ObjKey> origin_keys;
        std::vector<ObjKey> target_keys;
        std::vector<LnkLstPtr> linklists;
        ColKey col_link;
        Fixture()
        {
            target->add_column(type_Int, "t_1");
            col_link = origin->add_column_link(type_LinkList, "o_1", *target);
            origin->create_objects(3, origin_keys);
            linklists.emplace_back(origin->get_object(origin_keys[0]).get_linklist_ptr(col_link));
            linklists.emplace_back(origin->get_object(origin_keys[1]).get_linklist_ptr(col_link));
            linklists.emplace_back(origin->get_object(origin_keys[2]).get_linklist_ptr(col_link));
            target_keys.emplace_back(linklists[0]->create_and_insert_linked_object(0).get_key());
            target_keys.emplace_back(linklists[1]->create_and_insert_linked_object(0).get_key());
            target_keys.emplace_back(linklists[1]->create_and_insert_linked_object(1).get_key());
            target_keys.emplace_back(linklists[2]->create_and_insert_linked_object(0).get_key());
            target_keys.emplace_back(linklists[2]->create_and_insert_linked_object(1).get_key());
            target_keys.emplace_back(linklists[2]->create_and_insert_linked_object(2).get_key());
        }
        Obj get_origin_obj(int i)
        {
            return origin->get_object(origin_keys[i]);
        }
        Obj get_target_obj(int i)
        {
            return target->get_object(target_keys[i]);
        }
    };
    // Break links by clearing list
    {
        Fixture f;
        f.linklists[0]->clear();
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(5, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[1]->clear();
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(4, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[2]->clear(); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(!f.target->is_valid(f.target_keys[3]));
        CHECK(!f.target->is_valid(f.target_keys[4]));
        CHECK(!f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }
    // Break links by removal from list
    {
        Fixture f;
        f.linklists[0]->remove(0); // Cascade: Nothing
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(5, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[1]->remove(0);
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(5, f.target->size());
        f.group.verify();
    }

    // Break links by reassign
    {
        Fixture f;
        f.target_keys.emplace_back(f.linklists[0]->create_and_set_linked_object(0).get_key());
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[6]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[6], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(6, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.target_keys.emplace_back(f.linklists[1]->create_and_set_linked_object(0).get_key());
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[6]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[6], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        CHECK_EQUAL(6, f.target->size());
        f.group.verify();
    }

    // Break links by explicit ordered row removal
    {
        Fixture f;
        f.get_origin_obj(0).remove();
        CHECK_EQUAL(2, f.origin->size());
        CHECK_EQUAL(5, f.target->size());
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        // CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        f.group.verify();
    }
    {
        Fixture f;
        f.get_origin_obj(1).remove();
        CHECK_EQUAL(2, f.origin->size());
        CHECK_EQUAL(4, f.target->size());
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[3]));
        CHECK(f.target->is_valid(f.target_keys[4]));
        CHECK(f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        // CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        // CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        f.group.verify();
    }
    {
        Fixture f;
        f.get_origin_obj(2).remove();
        CHECK_EQUAL(2, f.origin->size());
        CHECK_EQUAL(3, f.target->size());
        CHECK(f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[2]));
        CHECK(!f.target->is_valid(f.target_keys[3]));
        CHECK(!f.target->is_valid(f.target_keys[4]));
        CHECK(!f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[0]->get(0));
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(0));
        CHECK_EQUAL(f.target_keys[2], f.linklists[1]->get(1));
        // CHECK_EQUAL(f.target_keys[3], f.linklists[2]->get(0));
        // CHECK_EQUAL(f.target_keys[4], f.linklists[2]->get(1));
        // CHECK_EQUAL(f.target_keys[5], f.linklists[2]->get(2));
        f.group.verify();
    }
    // Break link by clearing table
    {
        Fixture f;
        f.origin->clear();
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(!f.target->is_valid(f.target_keys[3]));
        CHECK(!f.target->is_valid(f.target_keys[4]));
        CHECK(!f.target->is_valid(f.target_keys[5]));
        CHECK_EQUAL(0, f.target->size());
        f.group.verify();
    }
}

TEST(Links_LinkList_Swap)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        LnkLstPtr link_list_1;
        LnkLstPtr link_list_2;
        ObjKeys okeys;
        ObjKeys tkeys;
        Fixture()
        {
            auto col_link = origin->add_column_link(type_LinkList, "", *target);
            target->add_column(type_Int, "");
            origin->create_objects(2, okeys);
            target->create_objects(2, tkeys);
            link_list_1 = origin->get_object(okeys[0]).get_linklist_ptr(col_link);
            link_list_1->add(tkeys[0]);
            link_list_1->add(tkeys[1]);
            link_list_2 = origin->get_object(okeys[1]).get_linklist_ptr(col_link); // Leave it empty
        }
    };

    // Sanity
    {
        Fixture f;
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(f.tkeys[0], f.link_list_1->get(0));
        CHECK_EQUAL(f.tkeys[1], f.link_list_1->get(1));
        CHECK_EQUAL(0, f.link_list_2->size());
        f.group.verify();
    }

    // No-op
    {
        Fixture f;
        f.link_list_1->swap(0, 0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(f.tkeys[0], f.link_list_1->get(0));
        CHECK_EQUAL(f.tkeys[1], f.link_list_1->get(1));
        f.link_list_1->swap(1, 1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(f.tkeys[0], f.link_list_1->get(0));
        CHECK_EQUAL(f.tkeys[1], f.link_list_1->get(1));
        f.group.verify();
    }

    // Both orders of arguments mean the same this
    {
        Fixture f;
        f.link_list_1->swap(0, 1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(f.tkeys[1], f.link_list_1->get(0));
        CHECK_EQUAL(f.tkeys[0], f.link_list_1->get(1));
        f.link_list_1->swap(1, 0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(f.tkeys[0], f.link_list_1->get(0));
        CHECK_EQUAL(f.tkeys[1], f.link_list_1->get(1));
        f.group.verify();
    }
}


TEST(Links_DetachedAccessor)
{
    Group group;
    TableRef table = group.add_table("table");
    auto col = table->add_column_link(type_LinkList, "l", *table);
    Obj obj = table->create_object();
    auto link_list = obj.get_linklist(col);
    link_list.add(obj.get_key());
    link_list.add(obj.get_key());
    group.remove_table("table");

    CHECK_EQUAL(link_list.size(), 0);
    CHECK_NOT(link_list.is_attached());
}

#endif // TEST_LINKS
