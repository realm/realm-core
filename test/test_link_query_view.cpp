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
#ifdef TEST_LINK_VIEW

#include <limits>
#include <string>
#include <sstream>
#include <ostream>

#include <realm/query_expression.hpp>
#include <realm/table_view.hpp>
#include <realm/link_view.hpp> // lasse todo remove
#include <realm/util/to_string.hpp>
#include <realm.hpp>

#include "util/misc.hpp"

#include "test.hpp"

using namespace realm;
using namespace test_util;
using namespace realm::util;

namespace {

void check_table_view(test_util::unit_test::TestContext& test_context, const char* file, long line,
                      const TableView& tv, std::vector<size_t> expected, const std::string& tv_str,
                      const std::string& expected_str)
{
    test_context.check_equal(tv.size(), expected.size(), file, line, (tv_str + ".size()").c_str(),
                             (expected_str + ".size()").c_str());
    if (tv.size() == expected.size()) {
        for (size_t i = 0; i < expected.size(); ++i) {
            test_context.check_equal(tv.get_source_ndx(i), expected[i], file, line,
                                     (tv_str + ".get_source_ndx(" + util::to_string(i) + ")").c_str(),
                                     (expected_str + "[" + util::to_string(i) + "]").c_str());
        }
    }
}
}

// void CHECK_TABLE_VIEW(const TableView&, std::initializer_list<size_t>);
#define CHECK_TABLE_VIEW(_tv, ...)                                                                                   \
    check_table_view(test_context, __FILE__, __LINE__, _tv, __VA_ARGS__, #_tv, #__VA_ARGS__)


TEST(LinkList_Basic1)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");
    table1->add_column(type_Binary, "bin1", true /*nullable*/);

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->set_binary(2, 0, BinaryData("foo"));

    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->set_binary(2, 1, BinaryData("", 0)); // empty binary

    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");
    table1->set_binary(2, 2, BinaryData()); // null binary

    size_t col_link2 = table2->add_column_link(type_Link, "link", *table1);
    table2->add_empty_row();
    table2->add_empty_row();

    table2->set_link(col_link2, 0, 1);
    table2->set_link(col_link2, 1, 2);

    Query q = table2->link(col_link2).column<String>(1) == "!";
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_index(), 0);

    q = table2->link(col_link2).column<BinaryData>(2) == BinaryData(); // == null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_index(), 1);

    q = table2->link(col_link2).column<BinaryData>(2) == BinaryData("", 0); // == empty binary
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_index(), 0);

    q = table2->link(col_link2).column<BinaryData>(2) != BinaryData(); // != null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_index(), 0);

    Query q2 = table2->link(col_link2).column<Int>(0) == 200;
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(tv2[0].get_index(), 0);

    // Just a few tests for the new string conditions to see if they work with links too.
    // The new string conditions are tested themself in Query_NextGen_StringConditions in test_query.cpp
    Query q3 = table2->link(col_link2).column<String>(1).contains("A", false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(tv3.size(), 1);
    CHECK_EQUAL(tv3[0].get_index(), 1); // "bar" contained an "A"

    table2->add_column(type_String, "str2");
    table2->set_string(1, 0, "A");
    table2->set_string(1, 1, "A");

    Query q4 = table2->link(col_link2).column<String>(1).contains(table2->column<String>(1), false);
    TableView tv4 = q4.find_all();
    CHECK_EQUAL(tv4.size(), 1);
    CHECK_EQUAL(tv4[0].get_index(), 1); // "bar" contained an "A"
}


TEST(LinkList_MissingDeepCopy)
{
    // Attempt to test that Query makes a deep copy of user given strings.
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    size_t col_link2 = table2->add_column_link(type_Link, "link", *table1);
    table2->add_empty_row();
    table2->add_empty_row();

    table2->set_link(col_link2, 0, 1);
    table2->set_link(col_link2, 1, 2);

    char* c = new char[10000000];
    c[10000000 - 1] = '!';
    Query q = table2->link(col_link2).column<String>(1) == StringData(&c[10000000 - 1], 1);

    delete[] c;
    // If this segfaults, Query hasn't made its own deep copy of "!"
    size_t m = q.find();
    CHECK_EQUAL(0, m);
}

TEST(LinkList_Basic2)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row();
    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");

    size_t col_link2 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links
    LinkViewRef links1;

    links1 = table1->get_linklist(col_link2, 0);
    links1->add(1);

    links1 = table1->get_linklist(col_link2, 1);
    links1->add(1);
    links1->add(2);


    size_t match;

    match = (table1->link(col_link2).column<Int>(0) > 550).find();
    CHECK_EQUAL(1, match);


    match = (table2->column<String>(1) == "world").find();
    CHECK_EQUAL(1, match);

    match = (table2->column<Int>(0) == 500).find();
    CHECK_EQUAL(1, match);

    match = (table1->link(col_link2).column<String>(1) == "!").find();
    CHECK_EQUAL(1, match);

    match = (table1->link(col_link2).column<Int>(0) == 600).find();
    CHECK_EQUAL(1, match);

    match = (table1->link(col_link2).column<String>(1) == "world").find();
    CHECK_EQUAL(0, match);

    match = (table1->link(col_link2).column<Int>(0) == 500).find();
    CHECK_EQUAL(0, match);

    match = (table1->link(col_link2).column<String>(1) == "world").find(1);
    CHECK_EQUAL(1, match);

    match = (table1->link(col_link2).column<Int>(0) == 500).find(1);
    CHECK_EQUAL(1, match);

    // Test link lists with 0 entries (3'rd row has no links)
    match = (table1->link(col_link2).column<String>(1) == "foobar").find();
    CHECK_EQUAL(not_found, match);

    match = (table1->link(col_link2).column<String>(1) == table1->column<String>(1)).find();
    CHECK_EQUAL(1, match);
}


TEST(LinkList_QuerySingle)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row();

    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");

    size_t col_link2 = table1->add_column_link(type_Link, "link", *table2);

    // set some links

    table1->set_link(col_link2, 0, 1);
    table1->set_link(col_link2, 1, 2);

    size_t match;

    match = (table1->link(col_link2).column<Int>(0) > 450).find();
    CHECK_EQUAL(0, match);

    match = (table1->link(col_link2).column<String>(1) == "!").find();
    CHECK_EQUAL(1, match);

    // See if NULL-link can be handled (3'rd row doesn't have any link)
    match = (table1->link(col_link2).column<String>(1) == "foobar").find();
    CHECK_EQUAL(not_found, match);
}


TEST(LinkList_TableViewTracking)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    size_t col_link2 = table2->add_column_link(type_Link, "link", *table1);
    table2->add_empty_row();
    table2->add_empty_row();
    table2->add_empty_row();
    table2->set_link(col_link2, 0, 1);
    table2->set_link(col_link2, 1, 2);
    table2->set_link(col_link2, 2, 0);

    TableView tv = (table2->link(col_link2).column<String>(1) == "!").find_all();
    CHECK_EQUAL(1, tv.size());

    // make entry NOT satisfy criteria, validate that sync removes entry from view
    table1->set_string(1, 1, "fnyt");
    CHECK_EQUAL(1, tv.size());
    tv.sync_if_needed();
    CHECK_EQUAL(0, tv.size());

    // make it SATISFY criteria again, validate that sync puts entry back in view
    table1->set_string(1, 1, "!");
    CHECK_EQUAL(0, tv.size());
    tv.sync_if_needed();
    CHECK_EQUAL(1, tv.size());
}


// Attempts to expose a bug (it would assert) where TableView::clear() was called with an unordered m_table.
// Internally, clear() tests if TableView::m_table is unordered by testing if it has any link or backlink columns
// (asana task made with 'fixme' because it's unreliable - in the future you could have unordered tables with no
// links).
TEST(LinkList_ClearView1)
{
    // m_table has:
    //      type_Link
    //      type_BackLink
    // tv: increasing target row indexes
    {
        Group group;

        TableRef table1 = group.add_table("table1");
        TableRef table2 = group.add_table("table2");

        // add some more columns to table1 and table2
        table1->add_column(type_Int, "col1");
        table1->add_column(type_String, "str1");

        // add some rows
        table1->add_empty_row();
        table1->set_int(0, 0, 300);
        table1->set_string(1, 0, "foo");
        table1->add_empty_row();
        table1->set_int(0, 1, 200);
        table1->set_string(1, 1, "!");
        table1->add_empty_row();
        table1->set_int(0, 2, 100);
        table1->set_string(1, 2, "bar");

        size_t col_link2 = table2->add_column_link(type_Link, "link", *table1);
        table2->add_empty_row();
        table2->add_empty_row();

        table2->set_link(col_link2, 0, 1);
        table2->set_link(col_link2, 1, 2);

        TableView tv = (table2->link(col_link2).column<String>(1) != "!").find_all();

        tv.clear(RemoveMode::unordered);
        CHECK_EQUAL(1, table2->size());
    }

    // m_table has:
    //      type_LinkList
    //      type_BackLink
    // tv: increasing target row indexes
    {
        Group group;

        TableRef table1 = group.add_table("table1");
        TableRef table2 = group.add_table("table2");

        // add some more columns to table1 and table2
        table1->add_column(type_Int, "col1");
        table1->add_column(type_String, "str1");

        // add some rows
        table1->add_empty_row();
        table1->set_int(0, 0, 300);
        table1->set_string(1, 0, "foo");
        table1->add_empty_row();
        table1->set_int(0, 1, 200);
        table1->set_string(1, 1, "!");
        table1->add_empty_row();
        table1->set_int(0, 2, 100);
        table1->set_string(1, 2, "bar");

        size_t col_link2 = table2->add_column_link(type_LinkList, "link", *table1);
        table2->add_empty_row();
        table2->add_empty_row();
        table2->add_empty_row();

        LinkViewRef links1;

        links1 = table2->get_linklist(col_link2, 0);
        links1->add(0);
        links1->add(1);

        links1 = table2->get_linklist(col_link2, 2);
        links1->add(1);
        links1->add(2);

        TableView tv = (table2->link(col_link2).column<String>(1) == "!").find_all();

        tv.clear(RemoveMode::unordered);
        CHECK_EQUAL(1, table2->size());
    }


    // m_table has:
    //      type_BackLink
    // tv: random target row index order (due to sort() - this can be interesting to test because clear() performs a
    // sort internally.
    {
        Group group;

        TableRef table1 = group.add_table("table1");
        TableRef table2 = group.add_table("table2");

        // add some more columns to table1 and table2
        table1->add_column(type_Int, "col1");
        table1->add_column(type_String, "str1");

        // add some rows
        table1->add_empty_row();
        table1->set_int(0, 0, 300);
        table1->set_string(1, 0, "foo");
        table1->add_empty_row();
        table1->set_int(0, 1, 200);
        table1->set_string(1, 1, "!");
        table1->add_empty_row();
        table1->set_int(0, 2, 100);
        table1->set_string(1, 2, "bar");

        size_t col_link2 = table2->add_column_link(type_LinkList, "link", *table1);
        table2->add_empty_row();
        table2->add_empty_row();

        LinkViewRef links1;

        links1 = table2->get_linklist(col_link2, 0);
        links1->add(0);
        links1->add(1);

        links1 = table2->get_linklist(col_link2, 1);
        links1->add(1);
        links1->add(2);

        TableView tv = (table1->column<String>(1) != "!").find_all();
        tv.sort(1);
        tv.clear();
        CHECK_EQUAL(1, table1->size());
    }
}


TEST(LinkList_QueryFindLinkTarget)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    table2->add_empty_row();

    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");
    table2->add_empty_row();
    table2->set_int(0, 3, 700);
    table2->set_string(1, 3, "!!");

    size_t col_link2 = table1->add_column_link(type_Link, "link", *table2);
    size_t col_link3 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links

    table1->set_link(col_link2, 0, 1);
    table1->set_link(col_link2, 1, 2);

    LinkViewRef lvr;

    lvr = table1->get_linklist(col_link3, 0);
    lvr->add(0);
    lvr->add(1);

    lvr = table1->get_linklist(col_link3, 1);
    lvr->add(1);
    lvr->add(2);

    size_t match;

    // First we test find_*_link on Table

    // find on Link
    match = table1->link(col_link2).find_first_link(1);
    CHECK_EQUAL(0, match);

    match = table1->link(col_link2).find_first_link(2);
    CHECK_EQUAL(1, match);

    match = table1->link(col_link2).find_first_link(3);
    CHECK_EQUAL(not_found, match);

    // find on LinkList
    match = table1->link(col_link3).find_first_link(1);
    CHECK_EQUAL(0, match);

    match = table1->link(col_link3).find_first_link(2);
    CHECK_EQUAL(1, match);

    match = table1->link(col_link3).find_first_link(3);
    CHECK_EQUAL(not_found, match);


    // find_all on Link

    TableView tv;

    tv = table1->link(col_link2).find_all_link(2);
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = table1->link(col_link2).find_all_link(1);
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = table1->link(col_link2).find_all_link(3);
    CHECK_EQUAL(0, tv.size());

    // find_all on LinkList
    tv = table1->link(col_link3).find_all_link(2);
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = table1->link(col_link3).find_all_link(1);
    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));
    CHECK_EQUAL(1, tv.get_source_ndx(1));

    tv = table1->link(col_link3).find_all_link(3);
    CHECK_EQUAL(0, tv.size());


    // find on query with Link
    match = (table1->column<Link>(col_link2) == table2->get(1)).find();
    CHECK_EQUAL(0, match);

    match = (table1->column<Link>(col_link2) == table2->get(2)).find();
    CHECK_EQUAL(1, match);

    match = (table1->column<Link>(col_link2) == table2->get(3)).find();
    CHECK_EQUAL(not_found, match);


    // find_all on query with Link
    tv = (table1->column<Link>(col_link2) == table2->get(2)).find_all();
    CHECK_TABLE_VIEW(tv, {1});

    tv = (table1->column<Link>(col_link2) == table2->get(1)).find_all();
    CHECK_TABLE_VIEW(tv, {0});

    tv = (table1->column<Link>(col_link2) == table2->get(3)).find_all();
    CHECK_TABLE_VIEW(tv, {});

    tv = (table1->column<Link>(col_link2) != table2->get(2)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 2});

    tv = (table1->column<Link>(col_link2) != table2->get(1)).find_all();
    CHECK_TABLE_VIEW(tv, {1, 2});

    tv = (table1->column<Link>(col_link2) != table2->get(3)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1, 2});

    // find on query with LinkList
    match = (table1->column<LinkList>(col_link3) == table2->get(1)).find();
    CHECK_EQUAL(0, match);

    match = (table1->column<LinkList>(col_link3) == table2->get(2)).find();
    CHECK_EQUAL(1, match);

    match = (table1->column<LinkList>(col_link3) == table2->get(3)).find();
    CHECK_EQUAL(not_found, match);

    // find_all on query with LinkList
    tv = (table1->column<LinkList>(col_link3) == table2->get(2)).find_all();
    CHECK_TABLE_VIEW(tv, {1});

    tv = (table1->column<LinkList>(col_link3) == table2->get(1)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1});

    tv = (table1->column<LinkList>(col_link3) == table2->get(3)).find_all();
    CHECK_TABLE_VIEW(tv, {});

    tv = (table1->column<LinkList>(col_link3) != table2->get(2)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1});

    tv = (table1->column<LinkList>(col_link3) != table2->get(1)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1});

    tv = (table1->column<LinkList>(col_link3) != table2->get(3)).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1});

    tv = table1->where().links_to(col_link3, std::vector<ConstRow>({table2->get(0), table2->get(2)})).find_all();
    CHECK_TABLE_VIEW(tv, {0, 1});
}


// Tests chains of links, such as table->link(2).link(0)...
TEST(LinkList_MultiLinkQuery)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");
    TableRef table3 = group.add_table("table3");
    TableRef table4 = group.add_table("table4");

    size_t col_linklist2 = table1->add_column_link(type_LinkList, "link", *table2);
    size_t col_link2 = table1->add_column_link(type_Link, "link", *table2);

    size_t col_link3 = table2->add_column_link(type_Link, "link", *table3);
    size_t col_linklist3 = table2->add_column_link(type_LinkList, "link", *table3);

    table4->add_column(type_Int, "int");
    table4->add_empty_row();
    table4->set_int(0, 0, 1000);
    table4->add_empty_row();
    table4->set_int(0, 1, 2000);

    table3->add_column(type_Int, "int");
    table3->add_column(type_String, "string");
    table3->add_column(type_Float, "string");

    size_t col_link4 = table3->add_column_link(type_Link, "link", *table4);
    size_t col_linklist4 = table3->add_column_link(type_LinkList, "link", *table4);

    // add some rows
    table3->add_empty_row();
    table3->set_int(0, 0, 100);
    table3->set_string(1, 0, "foo");
    table3->set_float(2, 0, 100.0f);

    table3->add_empty_row();
    table3->set_int(0, 1, 200);
    table3->set_string(1, 1, "bar");
    table3->set_float(2, 1, 200.0f);

    table3->add_empty_row();
    table3->set_int(0, 2, 300);
    table3->set_string(1, 2, "baz");
    table3->set_float(2, 2, 300.0f);

    LinkViewRef lvr;

    table3->add_empty_row();
    table3->set_link(col_link4, 0, 0);
    lvr = table3->get_linklist(col_linklist4, 0);
    lvr->add(0);
    lvr->add(1);

    table2->add_empty_row();
    table2->set_link(col_link3, 0, 0);
    lvr = table2->get_linklist(col_linklist3, 0);
    lvr->add(0);
    lvr->add(1);

    table2->add_empty_row();
    table2->set_link(col_link3, 1, 2);
    lvr = table2->get_linklist(col_linklist3, 1);
    lvr->add(2);

    table2->add_empty_row();

    table1->add_empty_row();
    table1->set_link(col_link2, 0, 1);
    lvr = table1->get_linklist(col_linklist2, 0);
    lvr->add(0);
    lvr->add(1);

    table1->add_empty_row();
    table1->set_link(col_link2, 1, 0);
    lvr = table1->get_linklist(col_linklist2, 1);
    lvr->add(2);

    table1->add_empty_row();

    TableView tv;

    // Link -> Link
    tv = (table1->link(col_link2).link(col_link3).column<Int>(0) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<Int>(0) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<Int>(0) == 200).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_link3).column<String>(1) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<String>(1) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<String>(1) == "bar").find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_link3).column<Float>(2) == 300.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<Float>(2) == 100.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).column<Float>(2) == 200.).find_all();
    CHECK_EQUAL(0, tv.size());


    // Link -> LinkList
    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(0) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(0) < 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(0) == 400).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_linklist3).column<String>(1) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<String>(1) == "none").find_all();
    CHECK_EQUAL(0, tv.size());


    // LinkList -> Link
    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(0) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(0) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(0) == 200).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_linklist2).link(col_link3).column<String>(1) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));


    tv = (table1->link(col_linklist2).link(col_link3).column<String>(1) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<String>(1) == "bar").find_all();
    CHECK_EQUAL(0, tv.size());


    // LinkList -> LinkList
    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(0) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(0) == 200).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(0) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(0) == 400).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(1) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(1) == "bar").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(1) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(1) == "none").find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(2) == 100.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(2) == 200.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(2) == 300.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(2) == 400.).find_all();
    CHECK_EQUAL(0, tv.size());

    // 3 levels of links
    tv = (table1->link(col_linklist2).link(col_linklist3).link(col_linklist4).column<Int>(0) > 0).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).link(col_link4).column<Int>(0) == 1000).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = (table1->link(col_link2).link(col_link3).link(col_link4).column<Int>(0) == 2000).find_all();
    CHECK_EQUAL(0, tv.size());
}


TEST(LinkList_SortLinkView)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");
    table1->add_column(type_Float, "str1");
    table1->add_column(type_Double, "str1");
    table1->add_column(type_String, "str2");
    table1->add_column(type_Timestamp, "ts");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 300);
    table1->set_string(1, 0, "delta");
    table1->set_float(2, 0, 300.f);
    table1->set_double(3, 0, 300.);
    table1->set_string(4, 0, "alfa");
    table1->set_timestamp(5, 0, Timestamp(300, 300));

    table1->add_empty_row();
    table1->set_int(0, 1, 100);
    table1->set_string(1, 1, "alfa");
    table1->set_float(2, 1, 100.f);
    table1->set_double(3, 1, 100.);
    table1->set_string(4, 1, "alfa");
    table1->set_timestamp(5, 1, Timestamp(100, 100));

    table1->add_empty_row();
    table1->set_int(0, 2, 200);
    table1->set_string(1, 2, "beta");
    table1->set_float(2, 2, 200.f);
    table1->set_double(3, 2, 200.);
    table1->set_string(4, 2, "alfa");
    table1->set_timestamp(5, 2, Timestamp(200, 200));

    size_t col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);
    table2->add_empty_row();
    table2->add_empty_row();

    LinkViewRef lvr;
    TableView tv;

    lvr = table2->get_linklist(col_link2, 0);
    lvr->clear();
    lvr->add(0);
    lvr->add(1);
    lvr->add(2);

    // Sort integer column
    lvr->sort(0);
    tv = lvr->get_sorted_view(0);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1); // 2 1
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    // Sort Timestamp column
    lvr->sort(5);
    tv = lvr->get_sorted_view(0);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    lvr = table2->get_linklist(col_link2, 1);
    lvr->clear();
    lvr->add(2);
    lvr->add(1);
    lvr->add(0);

    lvr->sort(0);
    tv = lvr->get_sorted_view(0);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    lvr = table2->get_linklist(col_link2, 1);
    lvr->clear();
    lvr->add(2);
    lvr->add(0);
    lvr->add(1);

    lvr->sort(0, false);
    tv = lvr->get_sorted_view(0, false);

    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    // Floats
    lvr = table2->get_linklist(col_link2, 1);
    lvr->clear();
    lvr->add(2);
    lvr->add(0);
    lvr->add(1);

    lvr->sort(2, false);
    tv = lvr->get_sorted_view(2, false);

    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    // Doubles
    lvr = table2->get_linklist(col_link2, 1);
    lvr->clear();
    lvr->add(2);
    lvr->add(0);
    lvr->add(1);

    lvr->sort(3, false);
    tv = lvr->get_sorted_view(3, false);

    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    // String
    lvr = table2->get_linklist(col_link2, 1);
    lvr->clear();
    lvr->add(2);
    lvr->add(0);
    lvr->add(1);

    lvr->sort(1, false);
    tv = lvr->get_sorted_view(1, false);

    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    // Test multi-column sorting
    std::vector<std::vector<size_t>> v;
    std::vector<bool> a = {true, true};
    std::vector<bool> a_false = {false, false};

    v.push_back({4});
    v.push_back({1});
    lvr->sort(SortDescriptor{lvr->get_target_table(), v, a_false});
    tv = lvr->get_sorted_view(SortDescriptor{lvr->get_target_table(), v, a_false});
    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    lvr->sort(SortDescriptor{lvr->get_target_table(), v, a});
    tv = lvr->get_sorted_view(SortDescriptor{lvr->get_target_table(), v, a});
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    v.push_back({2});
    a.push_back(true);

    lvr->sort(SortDescriptor{lvr->get_target_table(), v, a});
    tv = lvr->get_sorted_view(SortDescriptor{lvr->get_target_table(), v, a});
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    table1->remove(0);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 1);
}


TEST(Link_EmptySortedView)
{
    Group group;
    TableRef source = group.add_table("source");
    TableRef destination = group.add_table("destination");

    source->add_column_link(type_LinkList, "link", *destination);
    source->add_empty_row();
    LinkViewRef lvr = source->get_linklist(0, 0);

    CHECK_EQUAL(lvr->size(), 0);
    CHECK_EQUAL(lvr->get_sorted_view(0).size(), 0);
}


TEST(Link_FindNullLink)
{
    size_t match;

    Group group;

    TableRef table0 = group.add_table("table0");
    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    table0->add_column(type_String, "str1");
    table0->add_empty_row();
    table0->set_string(0, 0, "hello");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->set_string(1, 0, "foo");
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->set_string(1, 1, "!");
    table1->add_empty_row();
    table1->set_int(0, 2, 300);
    table1->set_string(1, 2, "bar");

    size_t col_link1 = table1->add_column_link(type_Link, "link", *table1);
    table1->set_link(col_link1, 0, 0);
    table1->set_link(col_link1, 2, 0);

    size_t col_link2 = table2->add_column_link(type_Link, "link", *table1);
    size_t col_linklist2 = table2->add_column_link(type_LinkList, "link", *table1);
    table2->add_empty_row();
    table2->add_empty_row();
    table2->add_empty_row();
    table2->add_empty_row();

    table2->set_link(col_link2, 0, 1);
    table2->set_link(col_link2, 2, 2);

    LinkViewRef lvr;

    lvr = table2->get_linklist(col_linklist2, 0);
    lvr->add(0);
    lvr->add(1);
    lvr = table2->get_linklist(col_linklist2, 2);
    lvr->add(0);

    /*
        Table setup. table2 has links to table1 and table1 to table0:

        table2 -> table1:                table1 -> table0:
        Row   LinkCol  LinkListCol       Row   Link
        0     1        {0, 1}            0     0
        1     null     {}                1     null
        2     2        {0}               2     0
        3     null     {}
    */

    // Find all non-null links on Link
    Query q5 = table1->column<Link>(col_link1).is_not_null();
    TableView tv5 = q5.find_all();
    CHECK_EQUAL(2, tv5.size());
    CHECK_EQUAL(0, tv5.get_source_ndx(0));
    CHECK_EQUAL(2, tv5.get_source_ndx(1));

    // Find all non-null links on LinkList
    Query q6 = table2->column<LinkList>(col_link2).is_not_null();
    TableView tv6 = q6.find_all();
    CHECK_EQUAL(2, tv6.size());
    CHECK_EQUAL(0, tv6.get_source_ndx(0));
    CHECK_EQUAL(2, tv6.get_source_ndx(1));

    // Test find_all on Link
    Query q3 = table2->column<Link>(col_link2).is_null();
    TableView tv = q3.find_all();
    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));
    CHECK_EQUAL(3, tv.get_source_ndx(1));

    // Test find() on Link
    match = table2->column<Link>(col_link2).is_null().find();
    CHECK_EQUAL(1, match);
    match = table2->column<Link>(col_link2).is_null().find(2);
    CHECK_EQUAL(3, match);

    // Test find_all() on LinkList
    Query q4 = table2->column<LinkList>(col_linklist2).is_null();
    TableView tv2 = q4.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));

    // Test find() on LinkList
    match = table2->column<LinkList>(col_linklist2).is_null().find();
    CHECK_EQUAL(1, match);
    match = table2->column<LinkList>(col_linklist2).is_null().find(2);
    CHECK_EQUAL(3, match);

    // We have not yet defined behaviour of finding realm::null()-links in a linked-to table, so we just throw. Todo.
    CHECK_THROW_ANY(table2->link(col_linklist2).column<Link>(col_link1).is_null());
}


TEST(Link_FindNotNullLink)
{
    // Regression for HelpScout #315.

    Group g;

    TableRef t0 = g.add_table("t0");
    TableRef t1 = g.add_table("t1");
    t0->add_column_link(type_Link, "link", *t1);
    t1->add_column(type_Int, "int");

    t0->add_empty_row(6);
    t1->add_empty_row(6);
    for (size_t i = 0; i < 6; ++i) {
        t1->set_int(0, i, 123);
        t0->nullify_link(0, i);
    }

    Query q0 = t0->column<Link>(0).is_null();
    TableView tv0 = q0.find_all();
    CHECK_EQUAL(6, tv0.size());

    for (size_t i = 0; i < 6; ++i) {
        t0->set_link(0, i, i);
    }

    Query q1 = t0->column<Link>(0).is_null();
    TableView tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.size());

    Query q2 = t0->where();
    q2.Not();
    q2.and_query(q1);
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(6, tv2.size());
}


TEST(LinkList_FindNotNullLink)
{
    // Regression for HelpScout #315.

    Group g;

    TableRef lists = g.add_table("List");
    TableRef items = g.add_table("ListItem");
    TableRef datas = g.add_table("ListItemData");

    lists->add_column_link(type_LinkList, "listItems", *items);
    items->add_column_link(type_Link, "localData", *datas);
    datas->add_column(type_String, "title");

    lists->add_empty_row();
    items->add_empty_row(6);
    datas->add_empty_row(6);
    LinkViewRef ll = lists->get_linklist(0, 0);
    for (size_t i = 0; i < 6; ++i) {
        datas->set_string(0, i, "foo");
        items->set_link(0, i, 0);
        ll->insert(0, i);
    }

    // This is how the Cocoa bindings do it normally:
    Query q0 = ll->get_target_table().where(ll);
    q0.and_query(q0.get_table()->column<Link>(0).is_null());
    CHECK_EQUAL(0, q0.find_all().size());

    // This is the "correct" way to do the "Not":
    Query q2 = items->where(ll);
    q2.Not();
    q2.and_query(items->column<Link>(0).is_null());
    CHECK_EQUAL(6, q2.find_all().size());

    // This is how the Cocoa bindings to the "Not":
    Query q1 = ll->get_target_table().where(ll);
    q1.Not();
    q1.and_query(q1.get_table()->column<Link>(0).is_null());
    CHECK_EQUAL(6, q1.find_all().size());
}

TEST(Link_FirstResultPastRow1000)
{
    Group g;

    TableRef data_table = g.add_table("data_table");
    TableRef link_table = g.add_table("link_table");
    link_table->add_column_link(type_Link, "link", *data_table);

    data_table->add_empty_row();
    link_table->add_empty_row(1001);

    link_table->set_link(0, 1000, 0);

    TableView tv = (link_table->column<Link>(0) == data_table->get(0)).find_all();
    CHECK_EQUAL(1, tv.size());
}


// Tests queries on a LinkList
TEST(LinkList_QueryOnLinkList)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 300);
    table1->set_string(1, 0, "delta");

    table1->add_empty_row();
    table1->set_int(0, 1, 100);
    table1->set_string(1, 1, "alfa");

    table1->add_empty_row();
    table1->set_int(0, 2, 200);
    table1->set_string(1, 2, "beta");

    size_t col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);

    table2->add_empty_row();
    table2->add_empty_row();

    LinkViewRef lvr;
    TableView tv;

    lvr = table2->get_linklist(col_link2, 0);
    lvr->clear();
    lvr->add(0);
    lvr->add(1);
    lvr->add(2);

    // Return all rows of table1 (the linked-to-table) that match the criteria and is in the LinkList

    // q.m_table = table1
    // q.m_view = lvr
    Query q = table1->where(lvr).and_query(table1->column<Int>(0) > 100);

    // tv.m_table == table1
    tv = q.find_all(); // tv = { 0, 2 }

    TableView tv2 = lvr->get_sorted_view(0);

    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(2, tv2.get_source_ndx(1));
    CHECK_EQUAL(0, tv2.get_source_ndx(2));

    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));
    CHECK_EQUAL(2, tv.get_source_ndx(1));

    // Should of course work even if nothing has changed
    tv.sync_if_needed();

    // Modify the LinkList and see if sync_if_needed takes it in count
    lvr->remove(2); // bumps version of table2 and only table2
    tv.sync_if_needed();

    CHECK_EQUAL(1, tv.size()); // fail
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    // Now test if changes in linked-to table bumps the version of the linked-from table and that
    // the query of 'tv' is re-run
    table1->set_int(0, 2, 50); // exclude row 2 from tv because of the '> 100' condition in Query
    tv.sync_if_needed();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    // See if we can keep a LinkView alive for the lifetime of a Query (used by objc lang. binding)
    Query query2;
    {
        LinkViewRef lvr2 = table2->get_linklist(col_link2, 1);
        query2 = table1->where(lvr2);
        // lvr2 goes out of scope now but should be kept alive
    }
    query2.find_all();
    query2.find();
}

TEST(LinkList_QueryOnIndexedPropertyOfLinkListSingleMatch)
{
    Group group;

    TableRef data_table = group.add_table("data");
    data_table->add_column(type_String, "col");
    data_table->add_search_index(0);

    TableRef link_table = group.add_table("link");
    link_table->add_column_link(type_LinkList, "col", *data_table);

    data_table->add_empty_row(2);
    data_table->set_string(0, 0, "a");
    data_table->set_string(0, 1, "b");

    link_table->add_empty_row();
    LinkViewRef lvr = link_table->get_linklist(0, 0);

    // Test with LinkList order matching Table order
    lvr->add(0);
    lvr->add(1);

    CHECK_EQUAL(0, data_table->where(lvr).and_query(data_table->column<String>(0) == "a").find());
    CHECK_EQUAL(1, data_table->where(lvr).and_query(data_table->column<String>(0) == "b").find());
    CHECK_EQUAL(not_found, data_table->where(lvr).and_query(data_table->column<String>(0) == "c").find());

    // Test with LinkList being the reverse of Table order
    lvr->clear();
    lvr->add(1);
    lvr->add(0);

    CHECK_EQUAL(0, data_table->where(lvr).and_query(data_table->column<String>(0) == "a").find());
    CHECK_EQUAL(1, data_table->where(lvr).and_query(data_table->column<String>(0) == "b").find());
    CHECK_EQUAL(not_found, data_table->where(lvr).and_query(data_table->column<String>(0) == "c").find());
}

TEST(LinkList_QueryLinkNull)
{
    Group group;

    TableRef data_table = group.add_table("data");
    data_table->add_column(type_String, "string", true);
    data_table->add_column_link(type_Link, "link", *data_table);
    data_table->add_column(type_Int, "int", true);
    data_table->add_column(type_Double, "double", true);
    data_table->add_column(type_OldDateTime, "date", true);

    // +-+--------+------+------+--------+------+
    // | |   0    |  1   |   2  |  3     |  4   |
    // +-+--------+------+------+--------+------+
    // | | string | link | int  | double | date |
    // +-+--------+------+------+--------+------+
    // |0| Fish   |    0 |   1  |   1.0  |  1   |
    // |1| null   | null | null |  null  | null |
    // |2| Horse  |    1 |   2  |   2.0  |  2   |
    // +-+--------+------+------+--------+------+

    data_table->add_empty_row();
    data_table->set_string(0, 0, "Fish");
    data_table->set_link(1, 0, 0);
    data_table->set_int(2, 0, 1);
    data_table->set_double(3, 0, 1.0);
    data_table->set_olddatetime(4, 0, OldDateTime(1));

    data_table->add_empty_row();
    data_table->set_string(0, 1, realm::null());
    data_table->nullify_link(1, 1);
    data_table->set_null(2, 1);
    data_table->set_null(3, 1);
    data_table->set_null(4, 1);

    data_table->add_empty_row();
    data_table->set_string(0, 2, "Horse");
    data_table->set_link(1, 2, 1);
    data_table->set_int(2, 2, 2);
    data_table->set_double(3, 2, 2.0);
    data_table->set_olddatetime(4, 2, OldDateTime(2));

    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<String>(0) == realm::null()).count());
    CHECK_EQUAL(2, data_table->where().and_query(data_table->column<String>(0) != realm::null()).count());

    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<Int>(2) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<Double>(3) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<OldDateTime>(4) == realm::null()).count());

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(1).column<String>(0) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(1).column<String>(0) != realm::null()).count());
    CHECK_EQUAL(0, data_table->where()
                       .and_query(data_table->link(1).column<String>(0) != realm::null())
                       .find_all()
                       .get_source_ndx(0));

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(1).column<Int>(2) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(1).column<Int>(2) != realm::null()).count());

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(1).column<Double>(3) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(1).column<Double>(3) != realm::null()).count());

    CHECK_EQUAL(2,
                data_table->where().and_query(data_table->link(1).column<OldDateTime>(4) == realm::null()).count());
    CHECK_EQUAL(1,
                data_table->where().and_query(data_table->link(1).column<OldDateTime>(4) != realm::null()).count());

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(1).column<String>(0).equal(realm::null())).count());
    CHECK_EQUAL(
        1, data_table->where().and_query(data_table->link(1).column<String>(0).not_equal(realm::null())).count());

    CHECK_EQUAL(
        2,
        data_table->where().Not().and_query(data_table->link(1).column<String>(0).not_equal(realm::null())).count());
    CHECK_EQUAL(1, data_table->where()
                       .Not()
                       .and_query(data_table->link(1).column<String>(0).not_equal(realm::null()))
                       .find_all()
                       .get_source_ndx(0));
    CHECK_EQUAL(2, data_table->where()
                       .Not()
                       .and_query(data_table->link(1).column<String>(0).not_equal(realm::null()))
                       .find_all()
                       .get_source_ndx(1));

    CHECK_EQUAL(
        1, data_table->where().Not().and_query(data_table->link(1).column<String>(0).equal(realm::null())).count());
    CHECK_EQUAL(0, data_table->where()
                       .Not()
                       .and_query(data_table->link(1).column<String>(0).equal(realm::null()))
                       .find_all()
                       .get_source_ndx(0));

    CHECK_EQUAL(1, (data_table->column<Link>(1) == realm::null()).count());
    CHECK_EQUAL(2, (data_table->column<Link>(1) != realm::null()).count());

    CHECK_EQUAL(0, (data_table->column<Link>(1) == Row()).count());
    CHECK_EQUAL(3, (data_table->column<Link>(1) != Row()).count());
}


TEST(LinkList_QueryOnIndexedPropertyOfLinkListMultipleMatches)
{
    Group group;

    TableRef data_table = group.add_table("data");
    data_table->add_column(type_String, "col");
    data_table->add_search_index(0);

    TableRef link_table = group.add_table("link");
    link_table->add_column_link(type_LinkList, "col", *data_table);

    // Ensure that the results from the index don't fit in a single leaf
    const size_t count = round_up(std::max(REALM_MAX_BPNODE_SIZE * 8, 100), 4);
    data_table->add_empty_row(count);
    for (size_t i = 0; i < count; ++i) {
        char str[2]{};
        str[0] = 'a' + (i % 4);
        data_table->set_string(0, i, StringData(str, 1));
    }

    link_table->add_empty_row();
    LinkViewRef lvr = link_table->get_linklist(0, 0);

    // Add every other row to the LinkList in the same order as the table
    for (size_t i = 0; i < count; i += 2)
        lvr->add(i);

    // in table and linkview
    TableView tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "a").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(0, tv[0].get_index());
    CHECK_EQUAL(4, tv[1].get_index());

    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "c").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(2, tv[0].get_index());
    CHECK_EQUAL(6, tv[1].get_index());

    // in table, not in linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "b").find_all();
    CHECK_EQUAL(0, tv.size());

    // not in table
    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "A").find_all();
    CHECK_EQUAL(0, tv.size());

    // Add every other row to the LinkList in the opposite order as the table
    lvr->clear();
    for (size_t i = count; i > 1; i -= 2)
        lvr->add(i - 2);

    // in table and linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "a").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(count - 4, tv[0].get_index());
    CHECK_EQUAL(count - 8, tv[1].get_index());

    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "c").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(count - 2, tv[0].get_index());
    CHECK_EQUAL(count - 6, tv[1].get_index());

    // in table, not in linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "b").find_all();
    CHECK_EQUAL(0, tv.size());

    // not in table
    tv = data_table->where(lvr).and_query(data_table->column<String>(0) == "A").find_all();
    CHECK_EQUAL(0, tv.size());
}

TEST(LinkList_QueryUnsortedListWithOr)
{
    Group group;

    TableRef data_table = group.add_table("data");
    data_table->add_column(type_Int, "col");

    TableRef link_table = group.add_table("link");
    link_table->add_column_link(type_LinkList, "col", *data_table);

    const size_t count = 5;
    data_table->add_empty_row(count);
    link_table->add_empty_row();
    LinkViewRef lvr = link_table->get_linklist(0, 0);

    // Populate data and add rows to the linkview in the opposite order of the
    // table's order
    for (size_t i = 0; i < count; ++i) {
        data_table->set_int(0, i, i);
        lvr->add(count - i - 1);
    }

    // Verify that a query with Or() returns all results
    TableView tv = data_table->where(lvr).group().equal(0, 1000).Or().between(0, 2, 4).end_group().find_all();
    CHECK_EQUAL(3, tv.size());
    CHECK_EQUAL(4, tv[0].get_index());
    CHECK_EQUAL(3, tv[1].get_index());
    CHECK_EQUAL(2, tv[2].get_index());
}

TEST(LinkList_QueryDateTime)
{
    Group group;

    TableRef table1 = group.add_table("first");
    TableRef table2 = group.add_table("second");

    table1->add_column_link(type_LinkList, "link", *table2);

    table2->add_column(type_OldDateTime, "date");

    table2->add_empty_row();
    table2->set_olddatetime(0, 0, OldDateTime(1));

    table1->add_empty_row();
    LinkViewRef lvr = table1->get_linklist(0, 0);
    lvr->add(0);

    TableView tv = (table1->link(0).column<OldDateTime>(0) >= OldDateTime(1)).find_all();

    CHECK_EQUAL(1, tv.size());
}

// Check that table views created through backlinks are updated correctly
// (marked as out of sync) when the source table is modified.
TEST(BackLink_Query_TableViewSyncsWhenNeeded)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    size_t col_int = source->add_column(type_Int, "id");
    size_t col_link = source->add_column_link(type_Link, "link", *target);
    size_t col_linklist = source->add_column_link(type_LinkList, "linklist", *target);

    target->add_column(type_Int, "id");

    source->add_empty_row(3);
    source->set_int(col_int, 0, 0);
    source->set_int(col_int, 1, 0);
    source->set_int(col_int, 2, 2);

    target->add_empty_row(3);
    source->set_link(col_link, 0, 0);
    source->set_link(col_link, 1, 1);

    Query q = target->backlink(*source, col_link).column<Int>(col_int) > 0;
    TableView tv = q.find_all();
    CHECK_TABLE_VIEW(tv, {});

    source->set_int(col_int, 1, 1);
    CHECK_EQUAL(false, tv.is_in_sync());

    tv.sync_if_needed();
    CHECK_TABLE_VIEW(tv, {1});

    source->set_link(col_link, 2, 2);
    CHECK_EQUAL(false, tv.is_in_sync());

    tv.sync_if_needed();
    CHECK_TABLE_VIEW(tv, {1, 2});

    Query list_query = target->backlink(*source, col_linklist).column<Int>(col_int) > 0;
    TableView list_tv = list_query.find_all();
    CHECK_TABLE_VIEW(list_tv, {});

    CHECK_EQUAL(0, source->get_link_count(col_linklist, 0));
    LinkViewRef list = source->get_linklist(col_linklist, 0);

    list->add(0);
    list->add(0);

    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_EQUAL(2, source->get_link_count(col_linklist, 0));
    CHECK_TABLE_VIEW(list_tv, {});

    list->add(2);

    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_EQUAL(3, source->get_link_count(col_linklist, 0));
    CHECK_TABLE_VIEW(list_tv, {});

    LinkViewRef list2 = source->get_linklist(col_linklist, 2);
    list2->add(0);

    CHECK_EQUAL(1, source->get_link_count(col_linklist, 2));
    CHECK_TABLE_VIEW(list_tv, {});
    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_TABLE_VIEW(list_tv, {0});
}

// Test queries involving the backlinks of a link column.
TEST(BackLink_Query_Link)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    size_t col_id = target->add_column(type_Int, "id");

    target->add_empty_row(4);
    target->set_int(col_id, 0, 0);
    target->set_int(col_id, 1, 1);
    target->set_int(col_id, 2, 2);
    target->set_int(col_id, 3, 3);

    size_t col_link = source->add_column_link(type_Link, "link", *target);
    size_t col_int = source->add_column(type_Int, "int");
    size_t col_double = source->add_column(type_Double, "double");
    size_t col_string = source->add_column(type_String, "string");

    auto add_row = [&](util::Optional<size_t> link_target, int64_t i, double d, const char* string) {
        size_t row = source->add_empty_row();
        if (link_target)
            source->set_link(col_link, row, *link_target);
        source->set_int(col_int, row, i);
        source->set_double(col_double, row, d);
        source->set_string(col_string, row, string);
    };

    add_row(util::none, 10, 10.0, "first");
    add_row(1, 20, 20.0, "second");
    add_row(2, 30, 30.0, "third");
    add_row(3, 40, 40.0, "fourth");
    add_row(3, 50, 50.0, "fifth");

    Query q1 = target->backlink(*source, col_link).column<Int>(col_int) > 25;
    CHECK_TABLE_VIEW(q1.find_all(), {2, 3});

    Query q2 = target->backlink(*source, col_link).column<Double>(col_double) < 25.0;
    CHECK_TABLE_VIEW(q2.find_all(), {1});

    Query q3 = target->backlink(*source, col_link).column<StringData>(col_string).begins_with("f");
    CHECK_TABLE_VIEW(q3.find_all(), {3});

    Query q4 = target->column<BackLink>(*source, col_link).is_null();
    CHECK_TABLE_VIEW(q4.find_all(), {0});

    Query q5 = target->column<BackLink>(*source, col_link).count() == 0;
    CHECK_TABLE_VIEW(q5.find_all(), {0});

    Query q6 = target->column<BackLink>(*source, col_link).column<Int>(col_int).average() > 42.5;
    CHECK_TABLE_VIEW(q6.find_all(), {3});

    Query q7 = target->column<BackLink>(*source, col_link).column<Double>(col_double).min() < 30.0;
    CHECK_TABLE_VIEW(q7.find_all(), {1});

    Query q8 = target->column<BackLink>(*source, col_link).column<Int>(col_int).sum() == 0;
    CHECK_TABLE_VIEW(q8.find_all(), {0});

    Query q9 = target->column<BackLink>(*source, col_link).column<Int>(col_int).average() == null();
    CHECK_TABLE_VIEW(q9.find_all(), {0});
}

// Test queries involving the backlinks of a link list column.
TEST(BackLink_Query_LinkList)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    size_t col_id = target->add_column(type_Int, "id");

    target->add_empty_row(5);
    target->set_int(col_id, 0, 0);
    target->set_int(col_id, 1, 1);
    target->set_int(col_id, 2, 2);
    target->set_int(col_id, 3, 3);
    target->set_int(col_id, 4, 4);

    size_t col_linklist = source->add_column_link(type_LinkList, "linklist", *target);
    size_t col_int = source->add_column(type_Int, "int");
    size_t col_double = source->add_column(type_Double, "double");
    size_t col_string = source->add_column(type_String, "string");

    auto add_row = [&](std::vector<size_t> link_targets, int64_t i, double d, const char* string) {
        size_t row = source->add_empty_row();
        auto link_view = source->get_linklist(col_linklist, row);
        for (auto link_target : link_targets)
            link_view->add(link_target);
        source->set_int(col_int, row, i);
        source->set_double(col_double, row, d);
        source->set_string(col_string, row, string);
    };

    add_row({}, 10, 10.0, "first");
    add_row({1, 1}, 20, 20.0, "second");
    add_row({0, 1}, 30, 30.0, "third");
    add_row({2, 3}, 40, 40.0, "fourth");
    add_row({3}, 50, 50.0, "fifth");

    Query q1 = target->backlink(*source, col_linklist).column<Int>(col_int) > 25;
    CHECK_TABLE_VIEW(q1.find_all(), {0, 1, 2, 3});

    Query q2 = target->backlink(*source, col_linklist).column<Double>(col_double) < 25.0;
    CHECK_TABLE_VIEW(q2.find_all(), {1});

    Query q3 = target->backlink(*source, col_linklist).column<StringData>(col_string).begins_with("f");
    CHECK_TABLE_VIEW(q3.find_all(), {2, 3});

    Query q4 = target->column<BackLink>(*source, col_linklist).is_null();
    CHECK_TABLE_VIEW(q4.find_all(), {4});

    Query q5 = target->column<BackLink>(*source, col_linklist).count() == 0;
    CHECK_TABLE_VIEW(q5.find_all(), {4});

    Query q6 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).average() > 42.5;
    CHECK_TABLE_VIEW(q6.find_all(), {3});

    Query q7 = target->column<BackLink>(*source, col_linklist).column<Double>(col_double).min() < 30.0;
    CHECK_TABLE_VIEW(q7.find_all(), {1});

    Query q8 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).sum() == 0;
    CHECK_TABLE_VIEW(q8.find_all(), {4});

    Query q9 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).average() == null();
    CHECK_TABLE_VIEW(q9.find_all(), {4});

    Query q10 = target->column<BackLink>(*source, col_linklist).column<Double>(col_double).sum() == 70;
    CHECK_TABLE_VIEW(q10.find_all(), {1});

    Query q11 =
        target->column<BackLink>(*source, col_linklist, source->column<Double>(col_double) == 20.0).count() == 2;
    CHECK_TABLE_VIEW(q11.find_all(), {1});
}

// Test queries involving multiple levels of links and backlinks.
TEST(BackLink_Query_MultipleLevels)
{
    Group group;

    TableRef people = group.add_table("people");

    size_t col_name = people->add_column(type_String, "name");
    size_t col_age = people->add_column(type_Int, "age");
    size_t col_children = people->add_column_link(type_LinkList, "children", *people);

    auto add_person = [&](std::string name, int age, std::vector<size_t> children) {
        size_t row = people->add_empty_row();
        auto children_link_view = people->get_linklist(col_children, row);
        for (auto child : children)
            children_link_view->add(child);
        people->set_string(col_name, row, name);
        people->set_int(col_age, row, age);
        return row;
    };

    auto hannah = add_person("Hannah", 0, {});
    auto elijah = add_person("Elijah", 3, {});

    auto mark = add_person("Mark", 30, {hannah});
    auto jason = add_person("Jason", 31, {elijah});

    auto diane = add_person("Diane", 29, {hannah});
    auto carol = add_person("Carol", 31, {});

    auto don = add_person("Don", 64, {diane, carol});
    auto diane_sr = add_person("Diane", 60, {diane, carol});

    auto michael = add_person("Michael", 57, {jason, mark});
    auto raewynne = add_person("Raewynne", 56, {jason, mark});

    // People that have a parent with a name that starts with 'M'.
    Query q1 = people->backlink(*people, col_children).column<String>(col_name).begins_with("M");
    CHECK_TABLE_VIEW(q1.find_all(), {hannah, mark, jason});

    // People that have a grandparent with a name that starts with 'M'.
    Query q2 = people->backlink(*people, col_children)
                   .backlink(*people, col_children)
                   .column<String>(col_name)
                   .begins_with("M");
    CHECK_TABLE_VIEW(q2.find_all(), {hannah, elijah});

    // People that have children that have a parent named Diane.
    Query q3 = people->link(col_children).backlink(*people, col_children).column<String>(col_name) == "Diane";
    CHECK_TABLE_VIEW(q3.find_all(), {mark, diane, don, diane_sr});

    // People that have children that have a grandparent named Don.
    Query q4 = people->link(col_children)
                   .backlink(*people, col_children)
                   .backlink(*people, col_children)
                   .column<String>(col_name) == "Don";
    CHECK_TABLE_VIEW(q4.find_all(), {mark, diane});

    // People whose parents have an average age of < 60.
    Query q5 = people->column<BackLink>(*people, col_children).column<Int>(col_age).average() < 60;
    CHECK_TABLE_VIEW(q5.find_all(), {hannah, elijah, mark, jason});

    // People that have at least one sibling.
    Query q6 =
        people->column<BackLink>(*people, col_children, people->column<Link>(col_children).count() > 1).count() > 0;
    CHECK_TABLE_VIEW(q6.find_all(), {mark, jason, diane, carol});

    // People that have Raewynne as a parent.
    Query q7 = people->column<BackLink>(*people, col_children) == people->get(raewynne);
    CHECK_TABLE_VIEW(q7.find_all(), {mark, jason});

    // People that have Mark as a child.
    Query q8 = people->column<Link>(col_children) == people->get(mark);
    CHECK_TABLE_VIEW(q8.find_all(), {michael, raewynne});

    // People that have Michael as a grandparent.
    Query q9 =
        people->backlink(*people, col_children).column<BackLink>(*people, col_children) == people->get(michael);
    CHECK_TABLE_VIEW(q9.find_all(), {hannah, elijah});

    // People that have Hannah as a grandchild.
    Query q10 = people->link(col_children).column<Link>(col_children) == people->get(hannah);
    CHECK_TABLE_VIEW(q10.find_all(), {don, diane_sr, michael, raewynne});

    // People that have no listed parents.
    Query q11 = people->column<BackLink>(*people, col_children).is_null();
    CHECK_TABLE_VIEW(q11.find_all(), {don, diane_sr, michael, raewynne});

    // Backlinks can never contain null so this will match no rows.
    Query q12 = people->column<BackLink>(*people, col_children) == null();
    CHECK_TABLE_VIEW(q12.find_all(), {});

    // Backlinks can never contain null so this will match all rows with backlinks.
    Query q13 = people->column<BackLink>(*people, col_children) != null();
    CHECK_TABLE_VIEW(q13.find_all(), {hannah, elijah, mark, jason, diane, carol});

    // No links are equal to a detached row accessor.
    Query q14 = people->column<BackLink>(*people, col_children) == Row();
    CHECK_TABLE_VIEW(q14.find_all(), {});

    // All links are not equal to a detached row accessor so this will match all rows with backlinks.
    Query q15 = people->column<BackLink>(*people, col_children) != Row();
    CHECK_TABLE_VIEW(q15.find_all(), {hannah, elijah, mark, jason, diane, carol});
}

// Test queries involving the multiple levels of backlinks across multiple tables.
TEST(BackLink_Query_MultipleLevelsAndTables)
{
    Group group;

    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    TableRef c = group.add_table("c");
    TableRef d = group.add_table("d");

    size_t col_id = a->add_column(type_Int, "id");
    size_t col_a_to_b = a->add_column_link(type_Link, "link", *b);

    size_t col_b_to_c = b->add_column_link(type_Link, "link", *c);
    size_t col_c_to_d = c->add_column_link(type_Link, "link", *d);

    d->add_column(type_Int, "id");

    auto add_row = [&](Table& table, std::vector<size_t> values, util::Optional<size_t> link) {
        size_t row = table.add_empty_row();
        size_t i = 0;
        for (; i < values.size(); ++i)
            table.set_int(i, row, values[i]);
        if (link)
            table.set_link(i, row, *link);
    };

    add_row(*d, {0}, util::none);
    add_row(*d, {1}, util::none);

    add_row(*c, {}, 0);
    add_row(*c, {}, 1);

    add_row(*b, {}, 0);
    add_row(*b, {}, 1);

    add_row(*a, {0}, 0);
    add_row(*a, {1}, 1);

    Query q = d->backlink(*c, col_c_to_d).backlink(*b, col_b_to_c).backlink(*a, col_a_to_b).column<Int>(col_id) == 1;
    CHECK_TABLE_VIEW(q.find_all(), {1});
}

#endif
