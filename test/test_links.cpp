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

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

#ifdef LEGACY_TESTS
void test_table_add_row(TableRef t, std::string first, int second, bool third, Days forth)
{
    t->create_object().set_all(first.c_str(), second, third, int(forth));
}
#endif

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

    std::vector<Key> table_1_keys;
    std::vector<Key> table_2_keys;
    // add some rows
    table1->create_objects(2, table_1_keys);
    table2->create_objects(2, table_2_keys);

    table1->get_object(table_1_keys[0]).set<String>(col_1, "string1");
    auto col_link2 = table1->add_column_link(type_Link, "link", *table2);

    // set some links
    table1->get_object(table_1_keys[0]).set(col_link2, table_2_keys[1]);
    CHECK_EQUAL(1, table2->get_object(table_2_keys[1]).get_backlink_count(*table1, col_link2));
    CHECK_EQUAL(table_1_keys[0], table2->get_object(table_2_keys[1]).get_backlink(*table1, col_link2, 0));

    // remove a column (moving link column back)
    col_link2 -= 1; // TODO: When we have stable col ids, this should be removed
    table1->remove_column(col_1);
    CHECK_EQUAL(1, table2->get_object(table_2_keys[1]).get_backlink_count(*table1, col_link2));
    CHECK_EQUAL(table_1_keys[0], table2->get_object(table_2_keys[1]).get_backlink(*table1, col_link2, 0));
}


TEST(Links_Basic)
{
    GROUP_TEST_PATH(path);
    Key key_origin;
    Key key_target;
    size_t col_link;

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
        Key key0 = obj0.get_key();
        Key key1 = obj1.get_key();
        Key key2 = obj2.get_key();

        // create table with links to table1
        TableRef table2 = group.add_table("table2");
        col_link = table2->add_column_link(type_Link, "link", *table1);
        CHECK_EQUAL(table1, table2->get_link_target(col_link));

        // add a few links
        Obj obj3 = table2->create_object().set(col_link, key1);
        Obj obj4 = table2->create_object().set(col_link, key0);
        Key key3 = obj3.get_key();
        key_origin = obj4.get_key();

        // Verify that links were set correctly
        Key link3 = obj3.get<Key>(col_link);
        Key link4 = obj4.get<Key>(col_link);
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

        link4 = obj4.get<Key>(col_link);
        CHECK_EQUAL(key2, link4);
        CHECK_EQUAL(0, obj0.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(1, obj2.get_backlink_count(*table2, col_link));
        CHECK_EQUAL(key_origin, obj2.get_backlink(*table2, col_link, 0));

        // Delete an object.
        table2->remove_object(key3);

        // Verify that delete went correctly
        CHECK_EQUAL(1, table2->size());
        CHECK_EQUAL(key2, obj4.get<Key>(col_link));

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
        CHECK_EQUAL(table1, table2->get_link_target(0));

        // Verify that links are still correct
        CHECK_EQUAL(key_target, table2->get_object(key_origin).get<Key>(col_link));
    }
}

TEST(Group_LinksToSameTable)
{
    Group g;
    TableRef table = g.add_table("target");

    table->add_column(type_Int, "integers", true);
    auto link_col = table->add_column_link(type_Link, "links", *table);

    // 3 rows linked together in a list
    std::vector<Key> keys;
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
    origin->add_column_link(type_Link, "a", *target);
    origin->add_column(type_Int, "b");
    Obj obj = origin->create_object();
    target->create_object(Key(10));

    CHECK_LOGIC_ERROR(obj.set(2, Key(10)), LogicError::column_index_out_of_range);
    CHECK_LOGIC_ERROR(obj.set(0, Key(5)), LogicError::target_row_index_out_of_range);

    // FIXME: Must also check that Logic::type_mismatch is thrown on column type mismatch, but Table::set_link() does
    // not properly check it yet.

    origin->remove_object(obj.get_key());
    CHECK_THROW(obj.set(0, Key(10)), InvalidKey);
#ifdef LEGACY_TESTS
    group.remove_table("origin");
    CHECK_LOGIC_ERROR(obj.set(0, Key(10)), LogicError::detached_accessor);
#endif
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
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    Obj obj0 = table1->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = table1->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = table1->create_object().set_all("test3", 3, true, int64_t(Wed));
    Key key0 = obj0.get_key();
    Key key1 = obj1.get_key();
    Key key2 = obj2.get_key();

    {
        // add a few links
        Obj obj3 = table2->create_object().set(col_link, key1);
        Obj obj4 = table2->create_object().set(col_link, key0);
        Key key3 = obj3.get_key();
        Key key4 = obj4.get_key();

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
#ifdef LEGACY_TESTS
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
    it = table1->begin();
    for (auto o : *table2) {
        o.set(col_link, it->get_key());
        ++it;
    }

    // clear target table and make sure links are nullified
    table1->clear();
    for (auto o : *table2) {
        CHECK(o.is_null(col_link));
    }
#endif
}


#ifdef LEGACY_TESTS
TEST(Links_Inserts)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    test_table_add_columns(table1);
    test_table_add_row(table1, "test1", 1, true, Mon);
    test_table_add_row(table1, "test2", 2, false, Tue);
    test_table_add_row(table1, "test3", 3, true, Wed);

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 0);
    table2->insert_empty_row(2);
    table2->set_link(col_link, 2, 2);

    table1->insert_empty_row(0);
    table1->insert_empty_row(0);
    table1->insert_empty_row(0);

    CHECK_EQUAL(4, table2->get_link(col_link, 0));
    CHECK_EQUAL(3, table2->get_link(col_link, 1));
    CHECK_EQUAL(5, table2->get_link(col_link, 2));
}

TEST(Links_InsertTrackedByBacklinks)
{
    Group group;

    auto table1 = group.add_table("target");
    test_table_add_columns(table1);
    test_table_add_row(table1, "test1", 1, true, Mon);
    test_table_add_row(table1, "test2", 2, false, Tue);
    test_table_add_row(table1, "test3", 3, true, Wed);

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 0);
    table2->insert_empty_row(2);
    table2->set_link(col_link, 2, 2);

    // verify backlinks
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(1, table1->get_backlink(0, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(2, table1->get_backlink(2, *table2, col_link, 0));

    // insert in table 2, verify that backlinks are updated
    table2->insert_empty_row(0);
    table2->insert_empty_row(0);
    table2->insert_empty_row(0);

    // verify
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(4, table1->get_backlink(0, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(3, table1->get_backlink(1, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(5, table1->get_backlink(2, *table2, col_link, 0));
}


TEST(Links_Multi)
{
    // Multiple links to same rows
    Group group;

    auto table1 = group.add_table("target");
    test_table_add_columns(table1);
    test_table_add_row(table1, "test1", 1, true, Mon);
    test_table_add_row(table1, "test2", 2, false, Tue);
    test_table_add_row(table1, "test3", 3, true, Wed);

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links pointing to same row
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 1);
    table2->insert_empty_row(2);
    table2->set_link(col_link, 2, 1);

    CHECK_EQUAL(3, table1->get_backlink_count(1, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink(1, *table2, col_link, 1));
    CHECK_EQUAL(2, table1->get_backlink(1, *table2, col_link, 2));

    // nullify a link
    table2->nullify_link(col_link, 1);
    CHECK_EQUAL(2, table1->get_backlink_count(1, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, *table2, col_link, 0));
    CHECK_EQUAL(2, table1->get_backlink(1, *table2, col_link, 1));

    // nullify one more to reduce to one link (test re-inlining)
    table2->nullify_link(col_link, 0);
    CHECK_EQUAL(1, table1->get_backlink_count(1, *table2, col_link));
    CHECK_EQUAL(2, table1->get_backlink(1, *table2, col_link, 0));

    // re-add links
    table2->set_link(col_link, 0, 1);
    table2->set_link(col_link, 1, 1);

    // remove a row
    table2->move_last_over(0);
    CHECK_EQUAL(2, table1->get_backlink_count(1, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, *table2, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink(1, *table2, col_link, 1));

    // add some more links and see that they get nullified when the target
    // is removed
    table2->insert_empty_row(2);
    table2->set_link(col_link, 2, 2);
    table2->insert_empty_row(3);
    table2->set_link(col_link, 3, 2);
    CHECK_EQUAL(2, table1->get_backlink_count(2, *table2, col_link));

    table1->move_last_over(1);
    CHECK(table2->is_null_link(col_link, 0));
    CHECK(table2->is_null_link(col_link, 1));
    CHECK(!table2->is_null_link(col_link, 2));
    CHECK(!table2->is_null_link(col_link, 3));

    // remove all rows from target and verify that links get nullified
    table1->clear();
    CHECK(table2->is_null_link(col_link, 2));
    CHECK(table2->is_null_link(col_link, 3));
}


TEST(Links_MultiToSame)
{
    Group group;

    auto table1 = group.add_table("target");
    test_table_add_columns(table1);
    test_table_add_row(table1, "test1", 1, true, Mon);
    test_table_add_row(table1, "test2", 2, false, Tue);
    test_table_add_row(table1, "test3", 3, true, Wed);

    // create table with multiple links to table1
    TableRef table2 = group.add_table("table2");
    size_t col_link1 = table2->add_column_link(type_Link, "link1", *TableRef(table1));
    size_t col_link2 = table2->add_column_link(type_Link, "link2", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link2));

    table2->add_empty_row();
    table2->set_link(col_link1, 0, 0);
    table2->set_link(col_link2, 0, 0);
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link1));
    CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link2));

    table2->move_last_over(0);
    CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link1));
    CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link2));
}
#endif

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
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    target->create_object().set_all("test1", 1, true, int64_t(Mon));
    target->create_object().set_all("test2", 2, false, int64_t(Tue));
    target->create_object().set_all("test3", 3, true, int64_t(Wed));

    ConstObj obj1 = origin->create_object(Key(0));
    CHECK(obj1.get_list<Key>(col_link).is_null());
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
#ifdef LEGACY_TESTS
    origin->clear();
#endif
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
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    origin->add_column(type_Int, "integers"); // Make sure the link column is not the only column
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    Obj obj0 = target->create_object().set_all("test1", 1, true, int64_t(Mon));
    Obj obj1 = target->create_object().set_all("test2", 2, false, int64_t(Tue));
    Obj obj2 = target->create_object().set_all("test3", 3, true, int64_t(Wed));
    Key key0 = obj0.get_key();
    Key key1 = obj1.get_key();
    Key key2 = obj2.get_key();

    Obj obj3 = origin->create_object(Key(0));
    Key key3 = obj3.get_key();
    auto links = obj3.get_linklist(col_link);

    // add several links to a single linklist
    links.add(key2);
    links.add(key1);
    links.add(key0);

    CHECK_EQUAL(3, obj3.get_link_count(col_link));
    CHECK_EQUAL(key2, links.get(0).get_key());
    CHECK_EQUAL(key1, links.get(1).get_key());
    CHECK_EQUAL(key0, links.get(2).get_key());
    CHECK_EQUAL(Wed, Days(links[0].get<Int>(day_col)));

    // verify that backlinks was set correctly
    CHECK_EQUAL(1, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj0.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj1.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(1, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 0));

    // insert a link at a specific position in the linklist
    links.insert(1, key2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key2, links.get(0).get_key());
    CHECK_EQUAL(key2, links.get(1).get_key());
    CHECK_EQUAL(key1, links.get(2).get_key());
    CHECK_EQUAL(key0, links.get(3).get_key());

    CHECK_EQUAL(2, obj2.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 0));
    CHECK_EQUAL(key3, obj2.get_backlink(*origin, col_link, 1));

    // change one link to another (replace key2 with key1)
    links.set(0, key1);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key1, links.get(0).get_key());
    CHECK_EQUAL(key2, links.get(1).get_key());
    CHECK_EQUAL(key1, links.get(2).get_key());
    CHECK_EQUAL(key0, links.get(3).get_key());

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
    CHECK_EQUAL(key0, links.get(0).get_key());
    CHECK_EQUAL(key1, links.get(1).get_key());
    CHECK_EQUAL(key2, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    links.move(0, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key1, links.get(0).get_key());
    CHECK_EQUAL(key2, links.get(1).get_key());
    CHECK_EQUAL(key0, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    links.move(2, 0);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0).get_key());
    CHECK_EQUAL(key1, links.get(1).get_key());
    CHECK_EQUAL(key2, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    links.move(2, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0).get_key());
    CHECK_EQUAL(key1, links.get(1).get_key());
    CHECK_EQUAL(key2, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    // swap two links
    links.swap(1, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0).get_key());
    CHECK_EQUAL(key2, links.get(1).get_key());
    CHECK_EQUAL(key1, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    // swap a link with itself
    links.swap(2, 2);
    CHECK_EQUAL(4, obj3.get_link_count(col_link));
    CHECK_EQUAL(key0, links.get(0).get_key());
    CHECK_EQUAL(key2, links.get(1).get_key());
    CHECK_EQUAL(key1, links.get(2).get_key());
    CHECK_EQUAL(key1, links.get(3).get_key());

    // remove a link
    links.remove(0);
    CHECK_EQUAL(3, obj3.get_link_count(col_link));
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));

    // remove all links
    links.clear();
    CHECK_EQUAL(0, obj3.get_link_count(col_link));
    CHECK_EQUAL(0, obj0.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj1.get_backlink_count(*origin, col_link));
    CHECK_EQUAL(0, obj2.get_backlink_count(*origin, col_link));

    // Add links again
    links.add(key2);
    links.add(key1);
    links.add(key0);

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


#ifdef LEGACY_TESTS
TEST(Links_LinkList_Inserts)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);

    LinkViewRef links = origin->get_linklist(col_link, 0);

    // add several links to a single linklist
    links->add(2);
    links->add(1);
    links->add(0);
    CHECK(!origin->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(3, links->size());
    CHECK_EQUAL(2, links->get(0).get_index());
    CHECK_EQUAL(1, links->get(1).get_index());
    CHECK_EQUAL(0, links->get(2).get_index());
    CHECK_EQUAL(Wed, Days((*links)[0].get_int(3)));

    // verify that backlinks was set correctly
    CHECK_EQUAL(1, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));

    target->insert_empty_row(0);

    // verify that all links in the linklist has tracked the movement
    CHECK_EQUAL(3, links->size());
    CHECK_EQUAL(3, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(1, links->get(2).get_index());
}


TEST(Links_LinkList_Backlinks)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);

    LinkViewRef links = origin->get_linklist(col_link, 0);
    links->add(2);
    links->add(1);
    links->add(0);

    // remove a target row and check that origin links are removed as well
    target->move_last_over(1);
    CHECK_EQUAL(2, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(1, links->get(0).get_index());
    CHECK_EQUAL(0, links->get(1).get_index());

    // remove all
    target->clear();
    CHECK_EQUAL(0, origin->get_link_count(col_link, 0));
    CHECK(links->is_empty());

    // re-add rows to target
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // add more rows with links
    origin->add_empty_row();
    origin->add_empty_row();
    LinkViewRef links1 = origin->get_linklist(col_link, 1);
    LinkViewRef links2 = origin->get_linklist(col_link, 2);

    // add links from each row
    links->add(2);
    links1->add(1);
    links2->add(0);

    // Verify backlinks
    CHECK_EQUAL(1, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(2, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(1, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));

    // delete a row and make sure backlinks are updated
    origin->move_last_over(0);
    CHECK_EQUAL(1, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(1, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(2, *origin, col_link));

    // delete last row and make sure backlinks are updated
    origin->move_last_over(1);
    CHECK_EQUAL(1, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, *origin, col_link));

    // remove all link lists and make sure backlinks are updated
    origin->clear();
    CHECK_EQUAL(0, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, *origin, col_link));
}


TEST(Links_LinkList_AccessorUpdates)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);

    LinkViewRef links0 = origin->get_linklist(col_link, 0);
    links0->add(2);
    links0->add(1);
    links0->add(0);

    LinkViewRef links1 = origin->get_linklist(col_link, 1);
    links1->add(2);
    links1->add(1);
    links1->add(0);

    LinkViewRef links2 = origin->get_linklist(col_link, 2);
    links2->add(2);
    links2->add(1);
    links2->add(0);

    CHECK_EQUAL(0, links0->get_origin_row_index());
    CHECK_EQUAL(1, links1->get_origin_row_index());
    CHECK_EQUAL(2, links2->get_origin_row_index());

    // get the same linkview twice
    LinkViewRef links2again = origin->get_linklist(col_link, 2);
    CHECK_EQUAL(links2->get_origin_row_index(), links2again->get_origin_row_index());

    // delete a row and make sure involved accessors are updated
    origin->move_last_over(0);
    CHECK_EQUAL(false, links0->is_attached());
    CHECK_EQUAL(0, links2->get_origin_row_index());
    CHECK_EQUAL(0, links2again->get_origin_row_index());

    // clear and make sure all accessors get detached
    origin->clear();
    CHECK_EQUAL(false, links1->is_attached());
    CHECK_EQUAL(false, links2->is_attached());
    CHECK_EQUAL(false, links2again->is_attached());
}


TEST(Links_LinkListInsert_AccessorUpdates)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);

    LinkViewRef links0 = origin->get_linklist(col_link, 0);
    links0->add(2);
    links0->add(1);
    links0->add(0);

    LinkViewRef links1 = origin->get_linklist(col_link, 1);
    links1->add(2);
    links1->add(1);
    links1->add(0);

    LinkViewRef links2 = origin->get_linklist(col_link, 2);
    links2->add(2);
    links2->add(1);
    links2->add(0);

    CHECK_EQUAL(0, links0->get_origin_row_index());
    CHECK_EQUAL(1, links1->get_origin_row_index());
    CHECK_EQUAL(2, links2->get_origin_row_index());

    // verify that backlinks was set correctly
    CHECK_EQUAL(3, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink(0, *origin, col_link, 1));
    CHECK_EQUAL(2, target->get_backlink(0, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink(1, *origin, col_link, 1));
    CHECK_EQUAL(2, target->get_backlink(1, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink(2, *origin, col_link, 1));
    CHECK_EQUAL(2, target->get_backlink(2, *origin, col_link, 2));

    // accessors follow movement of linklist entries
    origin->insert_empty_row(0);
    CHECK_EQUAL(1, links0->get_origin_row_index());
    CHECK_EQUAL(2, links1->get_origin_row_index());
    CHECK_EQUAL(3, links2->get_origin_row_index());

    // verify that backlinks was updated correctly
    CHECK_EQUAL(3, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(1, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(0, *origin, col_link, 1));
    CHECK_EQUAL(3, target->get_backlink(0, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(1, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(1, *origin, col_link, 1));
    CHECK_EQUAL(3, target->get_backlink(1, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(1, target->get_backlink(2, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(2, *origin, col_link, 1));
    CHECK_EQUAL(3, target->get_backlink(2, *origin, col_link, 2));


    // and changes of refs are visible through the accessors
    target->insert_empty_row(0);
    CHECK_EQUAL(1, links0->get_origin_row_index());
    CHECK_EQUAL(2, links1->get_origin_row_index());
    CHECK_EQUAL(3, links2->get_origin_row_index());
}

TEST(Links_LinkList_SwapRows)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);

    LinkViewRef links0 = origin->get_linklist(col_link, 0);
    links0->add(2);
    links0->add(1);
    links0->add(0);

    LinkViewRef links1 = origin->get_linklist(col_link, 1);
    links1->add(2);
    links1->add(1);
    links1->add(0);

    LinkViewRef links2 = origin->get_linklist(col_link, 2);
    links2->add(2);
    links2->add(1);
    links2->add(0);

    CHECK_EQUAL(0, links0->get_origin_row_index());
    CHECK_EQUAL(1, links1->get_origin_row_index());
    CHECK_EQUAL(2, links2->get_origin_row_index());

    origin->swap_rows(1, 2);

    // Check that accessors were updated
    CHECK_EQUAL(0, links0->get_origin_row_index());
    CHECK_EQUAL(2, links1->get_origin_row_index());
    CHECK_EQUAL(1, links2->get_origin_row_index());

    // verify that backlinks was updated correctly
    CHECK_EQUAL(3, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(0, *origin, col_link, 1));
    CHECK_EQUAL(1, target->get_backlink(0, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(1, *origin, col_link, 1));
    CHECK_EQUAL(1, target->get_backlink(1, *origin, col_link, 2));
    CHECK_EQUAL(3, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink(2, *origin, col_link, 1));
    CHECK_EQUAL(1, target->get_backlink(2, *origin, col_link, 2));

    // Release the accessor so we can test swapping when only one of
    // the two rows has an accessor.
    links0.reset();

    // Row 0 has no accessor.
    origin->swap_rows(0, 1);
    CHECK_EQUAL(2, links1->get_origin_row_index());
    CHECK_EQUAL(0, links2->get_origin_row_index());

    // Row 1 has no accessor.
    origin->swap_rows(0, 1);
    CHECK_EQUAL(2, links1->get_origin_row_index());
    CHECK_EQUAL(1, links2->get_origin_row_index());
}

TEST(Links_LinkList_TargetSwapRows)
{
    Group group;

    TableRef target = group.add_table("target");
    target->add_column(type_String, "string");
    target->add_empty_row(3);
    target->set_string(0, 0, "test1");
    target->set_string(0, 1, "test2");
    target->set_string(0, 2, "test3");

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *target);
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);

    LinkViewRef links0 = origin->get_linklist(col_link, 0);
    links0->add(2);
    links0->add(1);
    links0->add(0);

    LinkViewRef links1 = origin->get_linklist(col_link, 1);
    links1->add(2);
    links1->add(1);
    links1->add(1);

    LinkViewRef links2 = origin->get_linklist(col_link, 2);
    links2->add(2);
    links2->add(2);
    links2->add(0);

    CHECK_EQUAL(0, links0->get_origin_row_index());
    CHECK_EQUAL(1, links1->get_origin_row_index());
    CHECK_EQUAL(2, links2->get_origin_row_index());

    // FIXME: Table::swap_rows does not currently exist, so call through the
    // private API for now.
    _impl::TableFriend::do_swap_rows(*target, 1, 2);

    // Check that the String column did the swap
    CHECK_EQUAL(target->get_string(0, 1), "test3");
    CHECK_EQUAL(target->get_string(0, 2), "test2");

    // Check that links in the linklist were updated
    CHECK_EQUAL(links0->get(0).get_index(), 1);
    CHECK_EQUAL(links0->get(1).get_index(), 2);
    CHECK_EQUAL(links0->get(2).get_index(), 0);
    CHECK_EQUAL(links1->get(0).get_index(), 1);
    CHECK_EQUAL(links1->get(1).get_index(), 2);
    CHECK_EQUAL(links1->get(2).get_index(), 2);
    CHECK_EQUAL(links2->get(0).get_index(), 1);
    CHECK_EQUAL(links2->get(1).get_index(), 1);
    CHECK_EQUAL(links2->get(2).get_index(), 0);
}

TEST(Links_LinkList_FindByOrigin)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));

    origin->add_empty_row();
    LinkViewRef links = origin->get_linklist(col_link, 0);
    links->add(2);
    links->add(1);
    links->add(0);

    CHECK_EQUAL(0, links->find(2));
    CHECK_EQUAL(1, links->find(1));
    CHECK_EQUAL(2, links->find(0));

    links->remove(0);
    CHECK_EQUAL(not_found, links->find(2));
}


TEST(Links_CircularAccessors)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    {
        WriteTransaction wt(sg);
        TableRef table1 = wt.add_table("table1");
        TableRef table2 = wt.add_table("table2");
        table1->add_column_link(type_Link, "link", *table2);
        table2->add_column_link(type_Link, "link", *table1);
        CHECK_EQUAL(table1, table2->get_link_target(0));
        CHECK_EQUAL(table2, table1->get_link_target(0));
        wt.commit();
    }
    {
        WriteTransaction wt(sg);
        TableRef table1 = wt.get_table("table1");
        TableRef table2 = wt.get_table("table2");
        CHECK_EQUAL(table1, table2->get_link_target(0));
        CHECK_EQUAL(table2, table1->get_link_target(0));
    }
}


TEST(Links_Transactions)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);

    size_t name_col = 0;
    size_t dog_col = 1;
    size_t tim_row = 0;
    size_t harvey_row = 0;

    {
        WriteTransaction group(sg);

        // Create dogs table
        TableRef dogs = group.add_table("dogs");
        dogs->add_column(type_String, "dogName");

        // Create owners table
        TableRef owners = group.add_table("owners");
        owners->add_column(type_String, "name");
        owners->add_column_link(type_Link, "dog", *dogs);

        // Insert a single dog
        dogs->insert_empty_row(harvey_row);
        dogs->set_string(name_col, harvey_row, "Harvey");

        // Insert an owner with link to dog
        owners->insert_empty_row(tim_row);
        owners->set_string(name_col, tim_row, "Tim");
        owners->set_link(dog_col, tim_row, harvey_row);

        group.commit();
    }

    {
        ReadTransaction group(sg);

        // Verify that owner links to dog
        ConstTableRef owners = group.get_table("owners");
        CHECK(!owners->is_null_link(dog_col, tim_row));
        CHECK_EQUAL(harvey_row, owners->get_link(dog_col, tim_row));
    }

    {
        WriteTransaction group(sg);

        // Delete dog
        TableRef dogs = group.get_table("dogs");
        dogs->move_last_over(harvey_row);

        group.commit();
    }

    {
        ReadTransaction group(sg);

        // Verify that link from owner was nullified
        ConstTableRef owners = group.get_table("owners");
        CHECK(owners->is_null_link(dog_col, tim_row));
    }
}


TEST(Links_RemoveTargetRows)
{
    Group group;

    auto target = group.add_table("target");
    test_table_add_columns(target);
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));

    origin->add_empty_row();
    LinkViewRef links = origin->get_linklist(col_link, 0);
    links->add(2);
    links->add(1);
    links->add(0);

    // delete target rows through the links one at a time
    links->remove_target_row(0);
    CHECK_EQUAL(2, target->size());
    CHECK_EQUAL(2, links->size());

    links->remove_target_row(1);
    CHECK_EQUAL(1, target->size());
    CHECK_EQUAL(1, links->size());

    links->remove_target_row(0);
    CHECK_EQUAL(0, target->size());
    CHECK_EQUAL(0, links->size());

    // re-add targets and links
    test_table_add_row(target, "test1", 1, true, Mon);
    test_table_add_row(target, "test2", 2, false, Tue);
    test_table_add_row(target, "test3", 3, true, Wed);
    links->add(2);
    links->add(1);
    links->add(0);

    // Remove all targets through the links
    links->remove_all_target_rows();
    CHECK(target->is_empty());
    CHECK(links->is_empty());
}


TEST(Links_RemoveLastTargetColumn)
{
    // When the last ordinary column is removed from a table, its size (number
    // of rows) must "jump" to zero, even when the table continues to have
    // "hidden" backlick columns.

    Group group_1;
    TableRef table = group_1.add_table("table");
    table->add_column_link(type_Link, "t", *table);
    table->remove_column(0);

    Group group_2;
    TableRef origin = group_2.add_table("origin");
    TableRef target = group_2.add_table("target");
    target->add_column(type_Int, "t");
    target->add_empty_row();
    origin->add_column_link(type_Link, "o_1", *target);
    origin->add_column_link(type_LinkList, "o_2", *target);
    origin->add_empty_row();
    origin->set_link(0, 0, 0);
    LinkViewRef link_list = origin->get_linklist(1, 0);
    link_list->add(0);
    Row target_row_1 = target->get(0);
    Row target_row_2 = link_list->get(0);

    CHECK_EQUAL(1, target->size());
    target->remove_column(0);
    CHECK_EQUAL(0, target->get_column_count());
    CHECK(target->is_empty());
    CHECK(origin->is_null_link(0, 0));
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, origin->get_linklist(1, 0));
    CHECK_EQUAL(origin, &link_list->get_origin_table());
    CHECK_EQUAL(target, &link_list->get_target_table());
    CHECK_EQUAL(0, link_list->size());
    CHECK(!target_row_1.is_attached());
    CHECK(!target_row_2.is_attached());
}


TEST(Links_ClearColumnWithTwoLevelBptree)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");

    // The extra columns beyond the first one increase the likelihood of
    // getting unambiguously bad ref
    target->add_column(type_Int, "");
    target->add_column(type_Int, "");
    target->add_column(type_Int, "");
    target->add_column(type_Int, "");
    target->add_column(type_Int, "");
    target->add_empty_row();

    origin->add_column_link(type_LinkList, "", *target);
    origin->add_empty_row(REALM_MAX_BPNODE_SIZE + 1);
    origin->clear();
    origin->add_empty_row();
    origin->get_linklist(0, 0)->add(0);
    group.verify();
}


TEST(Links_ClearLinkListWithTwoLevelBptree)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    target->add_empty_row();
    origin->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0, 0);
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE + 1; ++i)
        link_list->add(0);
    link_list->clear();
    group.verify();
}


TEST(Links_FormerMemLeakCase)
{
    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg_w(path);
    {
        WriteTransaction wt(sg_w);
        TableRef origin = wt.add_table("origin");
        TableRef target = wt.add_table("target");
        target->add_column(type_Int, "");
        target->add_empty_row();
        origin->add_column_link(type_Link, "", *target);
        origin->add_empty_row(2);
        origin->set_link(0, 0, 0);
        origin->set_link(0, 1, 0);
        wt.commit();
    }
    {
        WriteTransaction wt(sg_w);
        TableRef target = wt.get_table("target");
        target->move_last_over(0);
        wt.get_group().verify();
        wt.commit();
    }
}


TEST(Links_RandomizedOperations)
{
    const size_t tests = 30;
    Random rnd;
    rnd.seed(random_int<unsigned long>()); // Seed from slow global generator

    for (size_t outer_iter = 0; outer_iter < 1000; outer_iter++) {
        Group group;
        TableRef refs[tests]; // 'tests' is max number of tables that can be produced

        std::vector<std::vector<size_t>> tables;

        for (size_t inner_iter = 0; inner_iter < tests; inner_iter++) {
            int action = rnd.draw_int_mod(100);

            if (action < 33 && tables.size() > 0) {
                // create link
                size_t from = rnd.draw_int_mod(tables.size());
                size_t to = rnd.draw_int_mod(tables.size());
                tables[from].push_back(to);

                int type = rnd.draw_int_mod(2);
                if (type == 0)
                    refs[from]->add_column_link(type_Link, "link", *refs[to]);
                else
                    refs[from]->add_column_link(type_LinkList, "link", *refs[to]);
            }
            else if (action < 66 && tables.size() > 0) {
                // delete link
                size_t from = rnd.draw_int_mod(tables.size());

                if (tables[from].size() > 0) {
                    size_t to = rnd.draw_int_mod(tables[from].size());
                    tables[from].erase(tables[from].begin() + to);
                    refs[from]->remove_column(to);
                }
            }
            else if (tables.size() < 10) {
                // create table
                refs[tables.size()] = group.get_or_add_table(
                    "table"); // FIXME: Lasse, did you really want to re-get the same table every time?
                tables.push_back(std::vector<size_t>());
            }
        }
    }
}
#endif

TEST(Links_CascadeRemove_ColumnLink)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        std::vector<Key> origin_keys;
        std::vector<Key> target_keys;
        size_t col_link;
        Fixture()
        {
            col_link = origin->add_column_link(type_Link, "o_1", *target, link_Strong);
            target->add_column(type_Int, "t_1");
            origin->create_objects(3, origin_keys);
            target->create_objects(3, target_keys);
            origin->get_object(origin_keys[0]).set(col_link, target_keys[0]); // origin[0].o_1 -> target[0]
            origin->get_object(origin_keys[1]).set(col_link, target_keys[1]); // origin[1].o_1 -> target[1]
            origin->get_object(origin_keys[2]).set(col_link, target_keys[2]); // origin[2].o_1 -> target[2]
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
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).set(f.col_link, null_key); // origin[1].o_1 -> realm::null()
        // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).set(f.col_link, null_key); // origin[0].o_1 -> realm::null()
        // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
    }

    // Break link by reassign
    {
        Fixture f;
        f.get_origin_obj(0).set(f.col_link, f.target_keys[2]); // origin[0].o_1 -> target[2]
        // Cascade: target->remove_object(key[0])
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).set(f.col_link, f.target_keys[0]); // origin[0].o_1 -> target[0]
        // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).set(f.col_link, f.target_keys[1]); // origin[2].o_1 -> target[1]
        // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(2).get<Key>(f.col_link));
    }

    // Avoid breaking link by reassigning self
    {
        Fixture f;
        f.get_origin_obj(0).set(f.col_link, f.target_keys[0]); // No effective change!
        // Cascade: target->remove_object(key[2])
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).set(f.col_link, f.target_keys[1]); // No effective change!
        // Cascade: target->remove_object(key[2])
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).set(f.col_link, f.target_keys[2]); // No effective change!
        // Cascade: target->remove_object(key[2])
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }

    // Break link by explicit object removal
    {
        Fixture f;
        f.get_origin_obj(0).remove(); // Cascade: target->remove_object(key[0])
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(1).remove(); // Cascade: target->remove_object(key[1])
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[2], f.get_origin_obj(2).get<Key>(f.col_link));
    }
    {
        Fixture f;
        f.get_origin_obj(2).remove(); // Cascade: target->remove_object(key[2])
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(f.target_keys[0], f.get_origin_obj(0).get<Key>(f.col_link));
        CHECK_EQUAL(f.target_keys[1], f.get_origin_obj(1).get<Key>(f.col_link));
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
        TableRef target = group.add_table("target");
        std::vector<Key> origin_keys;
        std::vector<Key> target_keys;
        std::vector<LinkListPtr> linklists;
        size_t col_link;
        Fixture()
        {
            col_link = origin->add_column_link(type_LinkList, "o_1", *target, link_Strong);
            target->add_column(type_Int, "t_1");
            origin->create_objects(3, origin_keys);
            target->create_objects(3, target_keys);
            linklists.emplace_back(origin->get_object(origin_keys[0]).get_linklist_ptr(col_link));
            linklists.emplace_back(origin->get_object(origin_keys[1]).get_linklist_ptr(col_link));
            linklists.emplace_back(origin->get_object(origin_keys[2]).get_linklist_ptr(col_link));
            linklists[0]->add(target_keys[1]); // origin[0].o_1 -> [ target[1] ]
            linklists[1]->add(target_keys[0]);
            linklists[1]->add(target_keys[1]); // origin[1].o_1 -> [ target[0], target[1] ]
            linklists[2]->add(target_keys[2]);
            linklists[2]->add(target_keys[1]);
            linklists[2]->add(target_keys[2]); // origin[1].o_1 -> [ target[2], target[1], target[2] ]
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

    // Break links by clearing list
    {
        Fixture f;
        f.linklists[0]->clear(); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(f.target_keys[0], f.linklists[1]->get(0).get_key());
        CHECK_EQUAL(f.target_keys[1], f.linklists[1]->get(1).get_key());
        CHECK_EQUAL(f.target_keys[2], f.linklists[2]->get(0).get_key());
        CHECK_EQUAL(f.target_keys[1], f.linklists[2]->get(1).get_key());
        CHECK_EQUAL(f.target_keys[2], f.linklists[2]->get(2).get_key());
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[1]->clear(); // Cascade: target->remove_object(0)
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[2]->clear(); // Cascade: target->remove_object(2)
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }

    // Break links by removal from list
    {
        Fixture f;
        f.linklists[0]->remove(0); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[1]->remove(0); // Cascade: target->remove_object(0)
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }

    // Break links by reassign
    {
        Fixture f;
        f.linklists[0]->set(0, f.target_keys[0]); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.linklists[1]->set(0, f.target_keys[1]); // Cascade: target->remove_object(0)
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }

    // Avoid breaking links by reassigning self
    {
        Fixture f;
        f.linklists[1]->set(0, f.target_keys[0]); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }

    // Break links by explicit ordered row removal
    {
        Fixture f;
        f.get_origin_obj(0).remove(); // Cascade: Nothing
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]) &&
              f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(3, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.get_origin_obj(1).remove(); // Cascade: target->remove_object(0)
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(f.target->is_valid(f.target_keys[1]) && f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }
    {
        Fixture f;
        f.get_origin_obj(2).remove(); // Cascade: target->remove_object(2)
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK(f.target->is_valid(f.target_keys[0]) && f.target->is_valid(f.target_keys[1]));
        CHECK_EQUAL(2, f.target->size());
        f.group.verify();
    }

    // Break link by clearing table
    {
        Fixture f;
        f.origin->clear();
        CHECK(!f.target->is_valid(f.target_keys[0]));
        CHECK(!f.target->is_valid(f.target_keys[1]));
        CHECK(!f.target->is_valid(f.target_keys[2]));
        CHECK_EQUAL(0, f.target->size());
        f.group.verify();
    }
}


TEST(Links_CascadeRemove_MultiLevel)
{
}


TEST(Links_CascadeRemove_Cycles)
{
}

#ifdef LEGACY_TESTS
TEST(Links_OrderedRowRemoval)
{
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(0);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(0);
        table->get_linklist(0, 1)->add(0);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(1);
        table->get_linklist(0, 1)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(0);
        table->get_linklist(0, 1)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0, 0)->add(1);
        table->get_linklist(0, 1)->add(0);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        origin->add_column_link(type_LinkList, "", *target);
        origin->add_empty_row();
        target->add_empty_row();
        origin->get_linklist(0, 0)->add(0);
        origin->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        origin->add_column_link(type_LinkList, "", *target);
        target->add_column(type_Int, "");
        origin->add_empty_row();
        target->add_empty_row();
        origin->get_linklist(0, 0)->add(0);
        origin->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        origin->add_column_link(type_LinkList, "", *target);
        target->add_column(type_Int, "");
        origin->add_empty_row();
        target->add_empty_row();
        origin->get_linklist(0, 0)->add(0);
        target->remove(0);
        group.verify();
    }
}


TEST(Links_LinkList_Swap)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        LinkViewRef llinklists[1] link_list_2;
        Fixture()
        {
            origin->add_column_link(type_LinkList, "", *target);
            target->add_column(type_Int, "");
            origin->add_empty_row(2);
            target->add_empty_row(2);
            link_list_1 = origin->get_linklist(0, 0);
            link_list_1->add(0);
            link_list_1->add(1);
            link_list_2 = origin->get_linklist(0, 1); // Leave it empty
        }
    };

    // Sanity
    {
        Fixture f;
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->size());
        f.group.verify();
    }

    // No-op
    {
        Fixture f;
        f.link_list_1->swap(0, 0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.link_list_1->swap(1, 1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.group.verify();
    }

    // Both orders of arguments mean the same this
    {
        Fixture f;
        f.link_list_1->swap(0, 1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(1, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(1).get_index());
        f.link_list_1->swap(1, 0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.group.verify();
    }

    // Detached accessor
    {
        Fixture f;
        f.origin->remove(0);
        CHECK_LOGIC_ERROR(f.link_list_1->swap(0, 1), LogicError::detached_accessor);
        f.group.verify();
    }

    // Index out of range
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.link_list_1->swap(1, 2), LogicError::link_index_out_of_range);
        CHECK_LOGIC_ERROR(f.link_list_1->swap(2, 1), LogicError::link_index_out_of_range);
        CHECK_LOGIC_ERROR(f.link_list_2->swap(0, 0), LogicError::link_index_out_of_range);
        f.group.verify();
    }
}


TEST(Links_LinkListAccessors_MoveOver)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    origin->add_column_link(type_LinkList, "", *target);
    origin->add_empty_row(4);
    LinkViewRef links0 = origin->get_linklist(0, 0);
    LinkViewRef links3 = origin->get_linklist(0, 3);

    // FIXME: Table::move_over does not currently exist, so call through private API
    // for now. Note that this only updates accessors, not the underlying rows.
    using tf = _impl::TableFriend;

    // Move a row with an attached accessor over one without.
    tf::adj_acc_move_over(*origin, 0, 1);
    tf::refresh_accessor_tree(*origin);
    CHECK_EQUAL(1, links0->get_origin_row_index());
    CHECK_EQUAL(3, links3->get_origin_row_index());

    // Move a row without an attached accessor over a row that has one.
    tf::adj_acc_move_over(*origin, 3, 2);
    tf::refresh_accessor_tree(*origin);
    CHECK_EQUAL(1, links0->get_origin_row_index());
    CHECK_EQUAL(2, links3->get_origin_row_index());

    // Move a row with an accessor over another row with an accessor.
    tf::adj_acc_move_over(*origin, 2, 1);
    tf::refresh_accessor_tree(*origin);
    CHECK_EQUAL(false, links0->is_attached());
    CHECK_EQUAL(1, links3->get_origin_row_index());

    // Move a row with an accessor over itself.
    tf::adj_acc_move_over(*origin, 1, 1);
    tf::refresh_accessor_tree(*origin);
    CHECK_EQUAL(false, links0->is_attached());
    CHECK_EQUAL(false, links3->is_attached());
}


TEST(Links_DetachedAccessor)
{
    Group group;
    TableRef table = group.add_table("table");
    table->add_column_link(type_LinkList, "l", *table);
    table->add_empty_row();
    LinkViewRef link_list = table->get_linklist(0, 0);
    link_list->add(0);
    link_list->add(0);
    group.remove_table("table");

    CHECK_LOGIC_ERROR(link_list->move(0, 1), LogicError::detached_accessor);
    CHECK_LOGIC_ERROR(link_list->swap(0, 1), LogicError::detached_accessor);
}
#endif

#endif // TEST_LINKS
