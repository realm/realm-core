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
#include <realm/util/to_string.hpp>
#include <realm.hpp>

#include "util/misc.hpp"

#include "test.hpp"

using namespace realm;
using namespace test_util;
using namespace realm::util;


namespace {

void check_table_view(test_util::unit_test::TestContext& test_context, const char* file, long line,
                      const TableView& tv, std::vector<ObjKey> expected, const std::string& tv_str,
                      const std::string& expected_str)
{
    test_context.check_equal(tv.size(), expected.size(), file, line, (tv_str + ".size()").c_str(),
                             (expected_str + ".size()").c_str());
    if (tv.size() == expected.size()) {
        for (size_t i = 0; i < expected.size(); ++i) {
            Obj o = const_cast<TableView&>(tv)[i];
            ObjKey ok = o.get_key();
            test_context.check_equal(ok, expected[i], file, line,
                                     (tv_str + ".get_source_key(" + util::to_string(ok.value) + ")").c_str(),
                                     (expected_str + "[" + util::to_string(ok.value) + "]").c_str());
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
    auto c0 = table1->add_column(type_Int, "col1");
    auto c1 = table1->add_column(type_String, "str1");
    auto c2 = table1->add_column(type_Binary, "bin1", true /*nullable*/);

    // add some rows
    auto o0 = table1->create_object().set_all(100, "foo", BinaryData("foo"));
    auto o1 = table1->create_object().set_all(200, "!", BinaryData("", 0));
    auto o2 = table1->create_object().set_all(300, "bar", BinaryData());

    auto col_link2 = table2->add_column_link(type_Link, "link", *table1);
    auto o20 = table2->create_object().set_all(o1.get_key());
    auto o21 = table2->create_object().set_all(o2.get_key());

    Query q = table2->link(col_link2).column<String>(c1) == "!";
    TableView tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), o20.get_key());

    q = table2->link(col_link2).column<BinaryData>(c2) == BinaryData(); // == null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), o21.get_key());

    q = table2->link(col_link2).column<BinaryData>(c2) == BinaryData("", 0); // == empty binary
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), o20.get_key());

    q = table2->link(col_link2).column<BinaryData>(c2) != BinaryData(); // != null
    tv = q.find_all();
    CHECK_EQUAL(tv.size(), 1);
    CHECK_EQUAL(tv[0].get_key(), o20.get_key());

    Query q2 = table2->link(col_link2).column<Int>(c0) == 200;
    TableView tv2 = q2.find_all();
    CHECK_EQUAL(tv2.size(), 1);
    CHECK_EQUAL(tv2[0].get_key(), o20.get_key());

    // Just a few tests for the new string conditions to see if they work with links too.
    // The new string conditions are tested themself in Query_NextGen_StringConditions in test_query.cpp
    Query q3 = table2->link(col_link2).column<String>(c1).contains("A", false);
    TableView tv3 = q3.find_all();
    CHECK_EQUAL(tv3.size(), 1);
    CHECK_EQUAL(tv3[0].get_key(), o1.get_key()); // "bar" contained an "A"

    auto c22 = table2->add_column(type_String, "str2");
    o20.set(c22, "A");
    o21.set(c22, "A");
    // table2->set_string(1, 0, "A");
    // table2->set_string(1, 1, "A");

    Query q4 = table2->link(col_link2).column<String>(c1).contains(table2->column<String>(c22), false);
    TableView tv4 = q4.find_all();
    CHECK_EQUAL(tv4.size(), 1);
    CHECK_EQUAL(tv4[0].get_key(), o1.get_key()); // "bar" contained an "A"
}


TEST(LinkList_MissingDeepCopy)
{
    // Attempt to test that Query makes a deep copy of user given strings.
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    auto c1 = table1->add_column(type_String, "str1");

    // add some rows
    auto o1 = table1->create_object().set_all(100, "foo");
    auto o2 = table1->create_object().set_all(200, "!");
    auto o3 = table1->create_object().set_all(300, "bar");

    auto col_link2 = table2->add_column_link(type_Link, "link", *table1);
    auto o20 = table2->create_object().set_all(o2.get_key());
    auto o21 = table2->create_object().set_all(o3.get_key());

    char* c = new char[10000000];
    c[10000000 - 1] = '!';
    Query q = table2->link(col_link2).column<String>(c1) == StringData(&c[10000000 - 1], 1);

    delete[] c;
    // If this segfaults, Query hasn't made its own deep copy of "!"
    ObjKey m = q.find();
    CHECK_EQUAL(o20.get_key(), m);
}

TEST(LinkList_Basic2)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    auto c1 = table1->add_column(type_String, "str1");

    auto c2 = table2->add_column(type_Int, "col1");
    auto c3 = table2->add_column(type_String, "str2");

    // add some rows
    auto o10 = table1->create_object().set_all(100, "foo");
    auto o11 = table1->create_object().set_all(200, "!");
    table1->create_object().set_all(300, "bar");
    auto o20 = table2->create_object().set_all(400, "hello");
    auto o21 = table2->create_object().set_all(500, "world");
    auto o22 = table2->create_object().set_all(600, "!");

    auto col_link2 = table1->add_column_link(type_LinkList, "link", *table2);

    // set some links
    LnkLst links1;

    links1 = o10.get_linklist(col_link2); // next add segfaults
    links1.add(o21.get_key());

    links1 = o11.get_linklist(col_link2);
    links1.add(o21.get_key());
    links1.add(o22.get_key());


    ObjKey match;

    match = (table1->link(col_link2).column<Int>(c2) > 550).find();
    CHECK_EQUAL(o11.get_key(), match);


    match = (table2->column<String>(c3) == "world").find();
    CHECK_EQUAL(o21.get_key(), match);

    match = (table2->column<Int>(c2) == 500).find();
    CHECK_EQUAL(o21.get_key(), match);

    match = (table1->link(col_link2).column<String>(c3) == "!").find();
    CHECK_EQUAL(o11.get_key(), match);

    match = (table1->link(col_link2).column<Int>(c2) == 600).find();
    CHECK_EQUAL(o11.get_key(), match);

    match = (table1->link(col_link2).column<String>(c3) == "world").find();
    CHECK_EQUAL(o10.get_key(), match);

    match = (table1->link(col_link2).column<Int>(c2) == 500).find();
    CHECK_EQUAL(o10.get_key(), match);

    // Test link lists with 0 entries (3'rd row has no links)
    match = (table1->link(col_link2).column<String>(c3) == "foobar").find();
    CHECK_EQUAL(ObjKey(), match);

    match = (table1->link(col_link2).column<String>(c3) == table1->column<String>(c1)).find();
    CHECK_EQUAL(o11.get_key(), match);
}


TEST(LinkList_QuerySingle)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    table1->add_column(type_String, "str1");

    auto c0 = table2->add_column(type_Int, "col1");
    auto c1 = table2->add_column(type_String, "str2");

    // add some rows
    auto x0 = table1->create_object().set_all(100, "foo");
    auto x1 = table1->create_object().set_all(200, "!");
    table1->create_object().set_all(300, "bar");
    table2->create_object().set_all(400, "hello");
    auto o1 = table2->create_object().set_all(500, "world");
    auto o2 = table2->create_object().set_all(600, "!");

    auto col_link2 = table1->add_column_link(type_Link, "link", *table2);

    // set some links

    x0.set(col_link2, o1.get_key());
    x1.set(col_link2, o2.get_key());

    ObjKey match;

    match = (table1->link(col_link2).column<Int>(c0) > 450).find();
    CHECK_EQUAL(x0.get_key(), match);

    match = (table1->link(col_link2).column<String>(c1) == "!").find();
    CHECK_EQUAL(x1.get_key(), match);

    // See if NULL-link can be handled (3'rd row doesn't have any link)
    match = (table1->link(col_link2).column<String>(c1) == "foobar").find();
    CHECK_EQUAL(ObjKey(), match);
}


TEST(LinkList_TableViewTracking)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");
    auto c1 = table1->add_column(type_String, "str1");

    // add some rows
    auto o0 = table1->create_object().set_all(100, "foo");
    auto o1 = table1->create_object().set_all(200, "!");
    auto o2 = table1->create_object().set_all(300, "bar");

    auto col_link2 = table2->add_column_link(type_Link, "link", *table1);
    table2->create_object().set_all(o1.get_key());
    table2->create_object().set_all(o2.get_key());
    table2->create_object().set_all(o0.get_key());

    TableView tv = (table2->link(col_link2).column<String>(c1) == "!").find_all();
    CHECK_EQUAL(1, tv.size());

    // make entry NOT satisfy criteria, validate that sync removes entry from view
    o1.set(c1, "fnyt");
    CHECK_EQUAL(1, tv.size());
    tv.sync_if_needed();
    CHECK_EQUAL(0, tv.size());

    // make it SATISFY criteria again, validate that sync puts entry back in view
    o1.set(c1, "!");
    CHECK_EQUAL(0, tv.size());
    tv.sync_if_needed();
    CHECK_EQUAL(1, tv.size());
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
    auto o10 = table1->create_object().set_all(100, "foo");
    auto o11 = table1->create_object().set_all(200, "!");
    auto o12 = table1->create_object().set_all(300, "bar");
    auto o20 = table2->create_object();
    auto o21 = table2->create_object().set_all(400, "hello");
    auto o22 = table2->create_object().set_all(500, "world");
    auto o23 = table2->create_object().set_all(600, "!");

    auto col_link2 = table1->add_column_link(type_Link, "link", *table2);
    auto col_link3 = table1->add_column_link(type_LinkList, "links", *table2);

    // set some links

    o10.set(col_link2, o21.get_key());
    o11.set(col_link2, o22.get_key());

    LnkLst lvr;

    lvr = o10.get_linklist(col_link3);
    lvr.add(o20.get_key());
    lvr.add(o21.get_key());

    lvr = o11.get_linklist(col_link3);
    lvr.add(o21.get_key());
    lvr.add(o22.get_key());

    ObjKey match;

    match = table1->find_first(col_link2, o21.get_key());
    CHECK_EQUAL(o10.get_key(), match);

    match = table1->find_first(col_link2, o22.get_key());
    CHECK_EQUAL(o11.get_key(), match);

    match = table1->find_first(col_link2, o23.get_key());
    CHECK_EQUAL(ObjKey(), match);

    // find on LinkList
    match = (table1->column<Link>(col_link3) == o21).find();
    CHECK_EQUAL(o10.get_key(), match);

    match = (table1->column<Link>(col_link3) == o22).find();
    CHECK_EQUAL(o11.get_key(), match);

    match = (table1->column<Link>(col_link3) == o23).find();
    CHECK_EQUAL(ObjKey(), match);

    // find on query with Link
    match = (table1->column<Link>(col_link2) == o21).find();
    CHECK_EQUAL(o10.get_key(), match);

    match = (table1->column<Link>(col_link2) == o22).find();
    CHECK_EQUAL(o11.get_key(), match);

    match = (table1->column<Link>(col_link2) == o23).find();
    CHECK_EQUAL(ObjKey(), match);


    // find_all on query with Link
    TableView tv;
    tv = (table1->column<Link>(col_link2) == o22).find_all();
    CHECK_TABLE_VIEW(tv, {o11.get_key()});

    tv = (table1->column<Link>(col_link2) == o21).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key()});

    tv = (table1->column<Link>(col_link2) == o23).find_all();
    CHECK_TABLE_VIEW(tv, {});

    tv = (table1->column<Link>(col_link2) != o22).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o12.get_key()});

    tv = (table1->column<Link>(col_link2) != o21).find_all();
    CHECK_TABLE_VIEW(tv, {o11.get_key(), o12.get_key()});

    tv = (table1->column<Link>(col_link2) != o23).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o11.get_key(), o12.get_key()});

    // find on query with LinkList
    match = (table1->column<Link>(col_link3) == o21).find();
    CHECK_EQUAL(o10.get_key(), match);

    match = (table1->column<Link>(col_link3) == o22).find();
    CHECK_EQUAL(o11.get_key(), match);

    match = (table1->column<Link>(col_link3) == o23).find();
    CHECK_EQUAL(ObjKey(), match);

    // find_all on query with LinkList
    tv = (table1->column<Link>(col_link3) == o22).find_all();
    CHECK_TABLE_VIEW(tv, {o11.get_key()});

    tv = (table1->column<Link>(col_link3) == o21).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o11.get_key()});

    tv = (table1->column<Link>(col_link3) == o23).find_all();
    CHECK_TABLE_VIEW(tv, {});

    tv = (table1->column<Link>(col_link3) != o22).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o11.get_key()});

    tv = (table1->column<Link>(col_link3) != o21).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o11.get_key()});

    tv = (table1->column<Link>(col_link3) != o23).find_all();
    CHECK_TABLE_VIEW(tv, {o10.get_key(), o11.get_key()});
}

// Tests chains of links, such as table->link(2).link(0)...
TEST(LinkList_MultiLinkQuery)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");
    TableRef table3 = group.add_table("table3");
    TableRef table4 = group.add_table("table4");

    ColKey col_linklist2 = table1->add_column_link(type_LinkList, "link", *table2);
    ColKey col_link2 = table1->add_column_link(type_Link, "linkl", *table2);

    ColKey col_link3 = table2->add_column_link(type_Link, "link", *table3);
    ColKey col_linklist3 = table2->add_column_link(type_LinkList, "linkl", *table3);

    ColKey c40 = table4->add_column(type_Int, "int");

    table4->create_object(ObjKey(0)).set_all(1000);
    table4->create_object(ObjKey(1)).set_all(2000);

    ColKey c30 = table3->add_column(type_Int, "int");
    ColKey c31 = table3->add_column(type_String, "string");
    ColKey c32 = table3->add_column(type_Float, "float");

    ColKey col_link4 = table3->add_column_link(type_Link, "link", *table4);
    ColKey col_linklist4 = table3->add_column_link(type_LinkList, "linkl", *table4);

    // add some rows
    table3->create_object(ObjKey(0)).set_all(100, "foo", 100.0f);
    table3->create_object(ObjKey(1)).set_all(200, "bar", 200.0f);
    table3->create_object(ObjKey(2)).set_all(300, "baz", 300.0f);

    LnkLst lvr;

    table3->create_object(ObjKey(3));
    auto o30 = table3->get_object(ObjKey(0));
    o30.set(col_link4, ObjKey(0));
    lvr = o30.get_linklist(col_linklist4);
    lvr.add(ObjKey(0));
    lvr.add(ObjKey(1));

    auto o20 = table2->create_object(ObjKey(0));
    o20.set(col_link3, ObjKey(0));
    lvr = o20.get_linklist(col_linklist3);
    lvr.add(ObjKey(0));
    lvr.add(ObjKey(1));

    auto o21 = table2->create_object(ObjKey(1));
    o21.set(col_link3, ObjKey(2));
    lvr = o21.get_linklist(col_linklist3);
    lvr.add(ObjKey(2));

    auto o22 = table2->create_object(ObjKey(2));

    auto o10 = table1->create_object();
    o10.set(col_link2, ObjKey(1));
    lvr = o10.get_linklist(col_linklist2);
    lvr.add(ObjKey(0));
    lvr.add(ObjKey(1));

    auto o11 = table1->create_object();
    o11.set(col_link2, ObjKey(0));
    lvr = o11.get_linklist(col_linklist2);
    lvr.add(ObjKey(2));

    table1->create_object();

    TableView tv;

    // Link -> Link
    tv = (table1->link(col_link2).link(col_link3).column<Int>(c30) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<Int>(c30) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o11.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<Int>(c30) == 200).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_link3).column<String>(c31) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<String>(c31) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o11.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<String>(c31) == "bar").find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_link3).column<Float>(c32) == 300.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<Float>(c32) == 100.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o11.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).column<Float>(c32) == 200.).find_all();
    CHECK_EQUAL(0, tv.size());

    // Link -> LinkList
    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(c30) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(c30) < 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o11.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(c30) == 400).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_link2).link(col_linklist3).column<String>(c31) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_linklist3).column<String>(c31) == "none").find_all();
    CHECK_EQUAL(0, tv.size());

    // LinkList -> Link
    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(c30) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(c30) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<Int>(c30) == 200).find_all();
    CHECK_EQUAL(0, tv.size());


    tv = (table1->link(col_linklist2).link(col_link3).column<String>(c31) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));


    tv = (table1->link(col_linklist2).link(col_link3).column<String>(c31) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_link3).column<String>(c31) == "bar").find_all();
    CHECK_EQUAL(0, tv.size());

    // LinkList -> LinkList
    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(c30) == 100).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(c30) == 200).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(c30) == 300).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Int>(c30) == 400).find_all();
    CHECK_EQUAL(0, tv.size());

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(c31) == "foo").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(c31) == "bar").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(c31) == "baz").find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<String>(c31) == "none").find_all();
    CHECK_EQUAL(0, tv.size());

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(c32) == 100.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(c32) == 200.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(c32) == 300.).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_linklist2).link(col_linklist3).column<Float>(c32) == 400.).find_all();
    CHECK_EQUAL(0, tv.size());

    // 3 levels of links
    tv = (table1->link(col_linklist2).link(col_linklist3).link(col_linklist4).column<Int>(c40) > 0).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o10.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).link(col_link4).column<Int>(c40) == 1000).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(o11.get_key(), tv.get_key(0));

    tv = (table1->link(col_link2).link(col_link3).link(col_link4).column<Int>(c40) == 2000).find_all();
    CHECK_EQUAL(0, tv.size());
}

TEST(LinkList_SortLinkView)
{
    Group group;

    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    // add some more columns to table1 and table2
    auto col_int = table1->add_column(type_Int, "ints");
    auto col_str1 = table1->add_column(type_String, "str1");
    auto col_float = table1->add_column(type_Float, "floats");
    auto col_double = table1->add_column(type_Double, "doubles");
    auto col_str2 = table1->add_column(type_String, "str2");
    auto col_date = table1->add_column(type_Timestamp, "ts");
    auto col_link2 = table2->add_column_link(type_LinkList, "linklist", *table1);

    // add some rows - all columns in different sort order
    ObjKey key0 = table1->create_object().set_all(100, "alfa", 200.f, 200., "alfa", Timestamp(200, 300)).get_key();
    ObjKey key1 = table1->create_object().set_all(200, "charley", 100.f, 300., "beta", Timestamp(100, 200)).get_key();
    ObjKey key2 = table1->create_object().set_all(300, "beta", 100.f, 100., "beta", Timestamp(300, 100)).get_key();

    Obj obj0 = table2->create_object();

    LnkLstPtr list_ptr;
    TableView tv;

    list_ptr = obj0.get_linklist_ptr(col_link2);
    list_ptr->add(key2);
    list_ptr->add(key1);
    list_ptr->add(key0);

    tv = list_ptr->get_sorted_view(col_int);
    CHECK_EQUAL(tv.get(0).get_key(), key0);
    CHECK_EQUAL(tv.get(1).get_key(), key1);
    CHECK_EQUAL(tv.get(2).get_key(), key2);

    // String
    tv = list_ptr->get_sorted_view(col_str1);
    CHECK_EQUAL(tv.get(0).get_key(), key0);
    CHECK_EQUAL(tv.get(1).get_key(), key2);
    CHECK_EQUAL(tv.get(2).get_key(), key1);

    // Sort Timestamp column
    tv = list_ptr->get_sorted_view(col_date);
    CHECK_EQUAL(tv.get(0).get_key(), key1);
    CHECK_EQUAL(tv.get(1).get_key(), key0);
    CHECK_EQUAL(tv.get(2).get_key(), key2);

    // Sort floats
    tv = list_ptr->get_sorted_view(col_float);
    CHECK_EQUAL(tv.get(0).get_key(), key2);
    CHECK_EQUAL(tv.get(1).get_key(), key1);
    CHECK_EQUAL(tv.get(2).get_key(), key0);


    // Sort descending
    tv = list_ptr->get_sorted_view(col_int, false);
    CHECK_EQUAL(tv.get(0).get_key(), key2);
    CHECK_EQUAL(tv.get(1).get_key(), key1);
    CHECK_EQUAL(tv.get(2).get_key(), key0);

    // Floats
    tv = list_ptr->get_sorted_view(col_float, false);
    CHECK_EQUAL(tv.get(0).get_key(), key0);
    CHECK_EQUAL(tv.get(1).get_key(), key2);
    CHECK_EQUAL(tv.get(2).get_key(), key1);

    // Doubles
    tv = list_ptr->get_sorted_view(col_double, false);
    CHECK_EQUAL(tv.get(0).get_key(), key1);
    CHECK_EQUAL(tv.get(1).get_key(), key0);
    CHECK_EQUAL(tv.get(2).get_key(), key2);

    // Test multi-column sorting
    std::vector<std::vector<ColKey>> v;
    std::vector<bool> a = {true, true};
    std::vector<bool> a_false = {false, false};

    v.push_back({col_str2});
    v.push_back({col_str1});
    tv = list_ptr->get_sorted_view(SortDescriptor{v, a_false});
    CHECK_EQUAL(tv.get(0).get_key(), key1);
    CHECK_EQUAL(tv.get(1).get_key(), key2);
    CHECK_EQUAL(tv.get(2).get_key(), key0);

    tv = list_ptr->get_sorted_view(SortDescriptor{v, a});
    CHECK_EQUAL(tv.get(0).get_key(), key0);
    CHECK_EQUAL(tv.get(1).get_key(), key2);
    CHECK_EQUAL(tv.get(2).get_key(), key1);

    v.push_back({col_float});
    a.push_back(true);

    // The last added column should have no influence
    tv = list_ptr->get_sorted_view(SortDescriptor{v, a});
    CHECK_EQUAL(tv.get(0).get_key(), key0);
    CHECK_EQUAL(tv.get(1).get_key(), key2);
    CHECK_EQUAL(tv.get(2).get_key(), key1);

    table1->remove_object(key0);
    tv.sync_if_needed();
    CHECK_EQUAL(tv.size(), 2);
    CHECK_EQUAL(tv.get(0).get_key(), key2);
    CHECK_EQUAL(tv.get(1).get_key(), key1);
}

TEST(Link_EmptySortedView)
{
    Group group;
    TableRef source = group.add_table("source");
    TableRef destination = group.add_table("destination");

    auto col_link_list = source->add_column_link(type_LinkList, "link", *destination);
    auto ll = source->create_object().get_linklist(col_link_list);

    CHECK_EQUAL(ll.size(), 0);
    CHECK_EQUAL(ll.get_sorted_view(col_link_list).size(), 0);
}


TEST(Link_FindNullLink)
{
    Group group;

    TableRef table0 = group.add_table("table0");
    TableRef table1 = group.add_table("table1");
    TableRef table2 = group.add_table("table2");

    auto col_str0 = table0->add_column(type_String, "str1");
    ObjKey k0 = table0->create_object().set(col_str0, "hello").get_key();

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "int1");
    table1->add_column(type_String, "str1");

    // add some rows
    Obj obj_1_0 = table1->create_object().set_all(100, "foo");
    Obj obj_1_1 = table1->create_object().set_all(200, "!");
    Obj obj_1_2 = table1->create_object().set_all(300, "bar");

    auto col_link1 = table1->add_column_link(type_Link, "link", *table0);
    obj_1_0.set(col_link1, k0);
    obj_1_2.set(col_link1, k0);

    auto col_link2 = table2->add_column_link(type_Link, "link", *table1);
    auto col_linklist2 = table2->add_column_link(type_LinkList, "link_list", *table1);
    Obj obj_2_0 = table2->create_object();
    Obj obj_2_1 = table2->create_object();
    Obj obj_2_2 = table2->create_object();
    Obj obj_2_3 = table2->create_object();

    obj_2_0.set(col_link2, obj_1_1.get_key());
    obj_2_2.set(col_link2, obj_1_2.get_key());

    LnkLst ll = obj_2_0.get_linklist(col_linklist2);
    ll.add(obj_1_0.get_key());
    ll.add(obj_1_1.get_key());
    obj_2_2.get_linklist(col_linklist2).add(obj_1_0.get_key());

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
    CHECK_EQUAL(obj_1_0.get_key(), tv5.get_key(0));
    CHECK_EQUAL(obj_1_2.get_key(), tv5.get_key(1));

    // Find all non-null links on LinkList
    Query q6 = table2->column<Link>(col_link2).is_not_null();
    TableView tv6 = q6.find_all();
    CHECK_EQUAL(2, tv6.size());
    CHECK_EQUAL(obj_2_0.get_key(), tv6.get_key(0));
    CHECK_EQUAL(obj_2_2.get_key(), tv6.get_key(1));

    // Test find_all null on Link
    Query q3 = table2->column<Link>(col_link2).is_null();
    TableView tv = q3.find_all();
    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(obj_2_1.get_key(), tv.get_key(0));
    CHECK_EQUAL(obj_2_3.get_key(), tv.get_key(1));

    // Test find() on Link
    auto match = table2->column<Link>(col_link2).is_null().find();
    CHECK_EQUAL(obj_2_1.get_key(), match);

    // Test find_all() on LinkList
    Query q4 = table2->column<Link>(col_linklist2).is_null();
    TableView tv2 = q4.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(obj_2_1.get_key(), tv2.get_key(0));
    CHECK_EQUAL(obj_2_3.get_key(), tv2.get_key(1));

    // Test find() on LinkList
    match = table2->column<Link>(col_linklist2).is_null().find();
    CHECK_EQUAL(obj_2_1.get_key(), match);

    // We have not yet defined behaviour of finding realm::null()-links in a linked-to table, so we just throw. Todo.
    CHECK_THROW_ANY(table2->link(col_linklist2).column<Link>(col_link1).is_null());
}

TEST(Link_FindNotNullLink)
{
    // Regression for HelpScout #315.

    Group g;

    TableRef t0 = g.add_table("t0");
    TableRef t1 = g.add_table("t1");
    auto col_link = t0->add_column_link(type_Link, "link", *t1);
    auto col_int = t1->add_column(type_Int, "int");

    std::vector<ObjKey> keys0;
    std::vector<ObjKey> keys1;
    t0->create_objects(6, keys0);
    t1->create_objects(6, keys1);
    for (size_t i = 0; i < 6; ++i) {
        t1->get_object(keys1[i]).set(col_int, 123);
        t0->get_object(keys0[i]).set(col_link, null_key);
    }

    Query q0 = t0->column<Link>(col_link).is_null();
    TableView tv0 = q0.find_all();
    CHECK_EQUAL(6, tv0.size());

    for (size_t i = 0; i < 6; ++i) {
        t0->get_object(keys0[i]).set(col_link, keys1[i]);
    }

    Query q1 = t0->column<Link>(col_link).is_null();
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

    auto col_linklist = lists->add_column_link(type_LinkList, "listItems", *items);
    auto col_link = items->add_column_link(type_Link, "localData", *datas);
    auto col_str = datas->add_column(type_String, "title");

    Obj obj0 = lists->create_object();
    std::vector<ObjKey> item_keys;
    std::vector<ObjKey> data_keys;
    items->create_objects(6, item_keys);
    datas->create_objects(6, data_keys);

    auto ll = obj0.get_linklist(col_linklist);
    for (size_t i = 0; i < 6; ++i) {
        datas->get_object(data_keys[i]).set(col_str, "foo");
        items->get_object(item_keys[i]).set(col_link, data_keys[0]);
        ll.insert(0, item_keys[i]);
    }

    // This is how the Cocoa bindings do it normally:
    Query q0 = ll.get_target_table().where(ll);
    q0.and_query(q0.get_table()->column<Link>(col_link).is_null());
    CHECK_EQUAL(0, q0.find_all().size());

    // This is the "correct" way to do the "Not":
    Query q2 = items->where(ll);
    q2.Not();
    q2.and_query(items->column<Link>(col_link).is_null());
    CHECK_EQUAL(6, q2.find_all().size());

    // This is how the Cocoa bindings to the "Not":
    Query q1 = ll.get_target_table().where(ll);
    q1.Not();
    q1.and_query(q1.get_table()->column<Link>(col_link).is_null());
    CHECK_EQUAL(6, q1.find_all().size());
}

TEST(Link_FirstResultPastRow1000)
{
    Group g;

    TableRef data_table = g.add_table("data_table");
    TableRef link_table = g.add_table("link_table");
    auto col_link = link_table->add_column_link(type_Link, "link", *data_table);

    Obj obj = data_table->create_object();
    std::vector<ObjKey> keys;
    link_table->create_objects(1001, keys);

    link_table->get_object(keys[1000]).set(col_link, obj.get_key());

    TableView tv = (link_table->column<Link>(col_link) == obj).find_all();
    CHECK_EQUAL(1, tv.size());
}

// Tests queries on a LinkList
TEST(LinkList_QueryOnLinkList)
{
    Group group;

    TableRef target = group.add_table("target");
    TableRef origin = group.add_table("origin");

    // add some more columns to target and origin
    auto col_int = target->add_column(type_Int, "col1");
    target->add_column(type_String, "str1");
    auto col_link2 = origin->add_column_link(type_LinkList, "linklist", *target);

    // add some rows
    ObjKey key0 = target->create_object().set_all(300, "delta").get_key();
    ObjKey key1 = target->create_object().set_all(100, "alfa").get_key();
    ObjKey key2 = target->create_object().set_all(200, "beta").get_key();
    ObjKey key4 = target->create_object().set_all(400, "charlie").get_key();
    ObjKey key5 = target->create_object().set_all(300, "ecco").get_key();


    Obj obj0 = origin->create_object();
    Obj obj1 = origin->create_object();
    Obj obj2 = origin->create_object();

    LnkLstPtr list_ptr;
    TableView tv;
    TableView tv1;

    list_ptr = obj0.get_linklist_ptr(col_link2);
    list_ptr->add(key0);
    list_ptr->add(key1);
    list_ptr->add(key2);

    obj1.get_linklist(col_link2).add(key4);
    obj2.get_linklist(col_link2).add(key5);

    // Return all rows of target (the linked-to-table) that match the criteria and is in the LinkList

    // q.m_table = target
    // q.m_view = list_ptr
    Query q = target->where(*list_ptr).and_query(target->column<Int>(col_int) > 100);
    Query q1 = origin->link(col_link2).column<Int>(col_int) == 300;

    // tv.m_table == target
    tv = q.find_all(); // tv = { 0, 2 }
    tv1 = q1.find_all(); // tv = { 0, 2 }

    TableView tv2 = list_ptr->get_sorted_view(col_int);

    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(key1, tv2.get_key(0));
    CHECK_EQUAL(key2, tv2.get_key(1));
    CHECK_EQUAL(key0, tv2.get_key(2));

    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(key0, tv.get_key(0));
    CHECK_EQUAL(key2, tv.get_key(1));

    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(obj0.get_key(), tv1.get_key(0));
    CHECK_EQUAL(obj2.get_key(), tv1.get_key(1));

    // Should of course work even if nothing has changed
    tv.sync_if_needed();

    // Modify the LinkList and see if sync_if_needed takes it in count
    list_ptr->remove(2); // bumps version of origin and only origin
    tv.sync_if_needed();

    CHECK_EQUAL(1, tv.size()); // fail
    CHECK_EQUAL(key0, tv.get_key(0));
    // tv1 should not be affected by this as linklist still contains an entry with 300
    tv1.sync_if_needed();
    CHECK_EQUAL(2, tv1.size());

    // Now test if changes in linked-to table bumps the version of the linked-from table and that
    // the query of 'tv' is re-run
    target->get_object(key0).set(col_int, 50); // exclude row 0 from tv because of the '> 100' condition in Query
    tv.sync_if_needed();
    CHECK_EQUAL(0, tv.size());
    tv1.sync_if_needed();
    CHECK_EQUAL(1, tv1.size());

    // See if we can keep a LinkView alive for the lifetime of a Query (used by objc lang. binding)
    Query query2;
    {
        auto list_ptr2 = obj1.get_linklist_ptr(col_link2);
        query2 = target->where(*list_ptr2);
        // lvr2 goes out of scope now but should be kept alive
    }
    tv = query2.find_all();
    CHECK_EQUAL(1, tv.size());
    ObjKey match = query2.find();
    CHECK_EQUAL(key4, match);
}

TEST(LinkList_QueryOnLinkListWithDuplicates)
{
    Group group;

    TableRef target = group.add_table("target");
    auto int_col = target->add_column(type_Int, "col1");
    TableRef origin = group.add_table("origin");
    auto link_col = origin->add_column_link(type_LinkList, "linklist", *target);

    std::vector<ObjKey> target_keys;
    target->create_objects(3, target_keys);
    target->get_object(target_keys[1]).set_all(1);
    target->get_object(target_keys[2]).set_all(2);

    Obj origin_obj = origin->create_object();
    LnkLst list = origin_obj.get_linklist(link_col);

    list.add(target_keys[0]);
    list.add(target_keys[1]);
    list.add(target_keys[2]);
    list.add(target_keys[0]);
    list.add(target_keys[1]);
    list.add(target_keys[2]);

    // TableView should contain both instances of each row
    auto all_rows = target->where(list).find_all();
    CHECK_EQUAL(6, all_rows.size());
    CHECK_EQUAL(target_keys[0], all_rows.get_key(0));
    CHECK_EQUAL(target_keys[1], all_rows.get_key(1));
    CHECK_EQUAL(target_keys[2], all_rows.get_key(2));
    CHECK_EQUAL(target_keys[0], all_rows.get_key(3));
    CHECK_EQUAL(target_keys[1], all_rows.get_key(4));
    CHECK_EQUAL(target_keys[2], all_rows.get_key(5));

    // TableView should contain both instances of each row matching the query
    auto some_rows = target->where(list).and_query(target->column<Int>(int_col) != 1).find_all();
    CHECK_EQUAL(4, some_rows.size());
    CHECK_EQUAL(target_keys[0], some_rows.get_key(0));
    CHECK_EQUAL(target_keys[2], some_rows.get_key(1));
    CHECK_EQUAL(target_keys[0], some_rows.get_key(2));
    CHECK_EQUAL(target_keys[2], some_rows.get_key(3));
}

TEST(LinkList_QueryOnIndexedPropertyOfLinkListSingleMatch)
{
    Group group;

    TableRef data_table = group.add_table("data");
    auto c0 = data_table->add_column(type_String, "col");
    data_table->add_search_index(c0);

    TableRef link_table = group.add_table("link");
    auto c1 = link_table->add_column_link(type_LinkList, "col", *data_table);

    auto k0 = data_table->create_object().set_all("a").get_key();
    auto k1 = data_table->create_object().set_all("b").get_key();

    auto o = link_table->create_object();
    auto lvr = o.get_linklist(c1);

    // Test with LinkList order matching Table order
    lvr.add(k0);
    lvr.add(k1);

    CHECK_EQUAL(k0, data_table->where(lvr).and_query(data_table->column<String>(c0) == "a").find());
    CHECK_EQUAL(k1, data_table->where(lvr).and_query(data_table->column<String>(c0) == "b").find());
    CHECK_EQUAL(ObjKey(), data_table->where(lvr).and_query(data_table->column<String>(c0) == "c").find());

    // Test with LinkList being the reverse of Table order
    lvr.clear();
    lvr.add(k1);
    lvr.add(k0);

    CHECK_EQUAL(k0, data_table->where(lvr).and_query(data_table->column<String>(c0) == "a").find());
    CHECK_EQUAL(k1, data_table->where(lvr).and_query(data_table->column<String>(c0) == "b").find());
    CHECK_EQUAL(ObjKey(), data_table->where(lvr).and_query(data_table->column<String>(c0) == "c").find());
}

TEST(LinkList_QueryLinkNull)
{
    Group group;

    TableRef data_table = group.add_table("data");
    auto c0 = data_table->add_column(type_String, "string", true);
    auto c1 = data_table->add_column_link(type_Link, "link", *data_table);
    auto c2 = data_table->add_column(type_Int, "int", true);
    auto c3 = data_table->add_column(type_Double, "double", true);
    auto c4 = data_table->add_column(type_Timestamp, "date", true);

    // +-+--------+------+------+--------+------+
    // | |   0    |  1   |   2  |  3     |  4   |
    // +-+--------+------+------+--------+------+
    // | | string | link | int  | double | time |
    // +-+--------+------+------+--------+------+
    // |0| Fish   |    0 |   1  |   1.0  |  1   |
    // |1| null   | null | null |  null  | null |
    // |2| Horse  |    1 |   2  |   2.0  |  2   |
    // +-+--------+------+------+--------+------+

    auto o0 = data_table->create_object();
    o0.set_all("Fish", o0.get_key(), 1, 1.0, Timestamp(0, 1));
    auto o1 = data_table->create_object();
    auto o2 = data_table->create_object();
    o2.set_all("Horse", o1.get_key(), 2, 2.0, Timestamp(0, 2));

    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<String>(c0) == realm::null()).count());
    CHECK_EQUAL(2, data_table->where().and_query(data_table->column<String>(c0) != realm::null()).count());

    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<Int>(c2) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<Double>(c3) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->column<Timestamp>(c4) == realm::null()).count());

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(c1).column<String>(c0) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(c1).column<String>(c0) != realm::null()).count());
    CHECK_EQUAL(o0.get_key(), data_table->where()
                                  .and_query(data_table->link(c1).column<String>(c0) != realm::null())
                                  .find_all()
                                  .get_key(0));

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(c1).column<Int>(c2) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(c1).column<Int>(c2) != realm::null()).count());

    CHECK_EQUAL(2, data_table->where().and_query(data_table->link(c1).column<Double>(c3) == realm::null()).count());
    CHECK_EQUAL(1, data_table->where().and_query(data_table->link(c1).column<Double>(c3) != realm::null()).count());

    CHECK_EQUAL(2,
                data_table->where().and_query(data_table->link(c1).column<Timestamp>(c4) == realm::null()).count());
    CHECK_EQUAL(1,
                data_table->where().and_query(data_table->link(c1).column<Timestamp>(c4) != realm::null()).count());

    CHECK_EQUAL(2,
                data_table->where().and_query(data_table->link(c1).column<String>(c0).equal(realm::null())).count());
    CHECK_EQUAL(
        1, data_table->where().and_query(data_table->link(c1).column<String>(c0).not_equal(realm::null())).count());

    CHECK_EQUAL(2, data_table->where()
                       .Not()
                       .and_query(data_table->link(c1).column<String>(c0).not_equal(realm::null()))
                       .count());
    CHECK_EQUAL(o1.get_key(), data_table->where()
                                  .Not()
                                  .and_query(data_table->link(c1).column<String>(c0).not_equal(realm::null()))
                                  .find_all()
                                  .get_key(0));
    CHECK_EQUAL(o2.get_key(), data_table->where()
                                  .Not()
                                  .and_query(data_table->link(c1).column<String>(c0).not_equal(realm::null()))
                                  .find_all()
                                  .get_key(1));

    CHECK_EQUAL(
        1, data_table->where().Not().and_query(data_table->link(c1).column<String>(c0).equal(realm::null())).count());
    CHECK_EQUAL(o0.get_key(), data_table->where()
                                  .Not()
                                  .and_query(data_table->link(c1).column<String>(c0).equal(realm::null()))
                                  .find_all()
                                  .get_key(0));

    CHECK_EQUAL(1, (data_table->column<Link>(c1) == realm::null()).count());
    CHECK_EQUAL(2, (data_table->column<Link>(c1) != realm::null()).count());
}

TEST(LinkList_QueryOnIndexedPropertyOfLinkListMultipleMatches)
{
    Group group;

    TableRef data_table = group.add_table("data");
    auto c0 = data_table->add_column(type_String, "col");
    data_table->add_search_index(c0);

    TableRef link_table = group.add_table("link");
    auto c1 = link_table->add_column_link(type_LinkList, "col", *data_table);

    // Ensure that the results from the index don't fit in a single leaf
    const size_t count = round_up(std::max(REALM_MAX_BPNODE_SIZE * 8, 100), 4);
    // data_table->add_empty_row(count);
    for (size_t i = 0; i < count; ++i) {
        char str[2]{};
        str[0] = 'a' + (i % 4);
        data_table->create_object(ObjKey(i)).set_all(StringData(str, 1));
        // data_table->set_string(0, i, StringData(str, 1));
    }

    auto o = link_table->create_object();
    auto lvr = o.get_linklist(c1);

    // Add every other row to the LinkList in the same order as the table
    for (size_t i = 0; i < count; i += 2)
        lvr.add(ObjKey(i));

    // in table and linkview
    TableView tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "a").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(ObjKey(0), tv[0].get_key());
    CHECK_EQUAL(ObjKey(4), tv[1].get_key());

    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "c").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(ObjKey(2), tv[0].get_key());
    CHECK_EQUAL(ObjKey(6), tv[1].get_key());

    // in table, not in linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "b").find_all();
    CHECK_EQUAL(0, tv.size());

    // not in table
    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "A").find_all();
    CHECK_EQUAL(0, tv.size());

    // Add every other row to the LinkList in the opposite order as the table
    lvr.clear();
    for (size_t i = count; i > 1; i -= 2)
        lvr.add(ObjKey(i - 2));

    // in table and linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "a").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(ObjKey(count - 4), tv[0].get_key());
    CHECK_EQUAL(ObjKey(count - 8), tv[1].get_key());

    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "c").find_all();
    CHECK_EQUAL(count / 4, tv.size());
    CHECK_EQUAL(ObjKey(count - 2), tv[0].get_key());
    CHECK_EQUAL(ObjKey(count - 6), tv[1].get_key());

    // in table, not in linkview
    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "b").find_all();
    CHECK_EQUAL(0, tv.size());

    // not in table
    tv = data_table->where(lvr).and_query(data_table->column<String>(c0) == "A").find_all();
    CHECK_EQUAL(0, tv.size());
}


TEST(LinkList_QueryUnsortedListWithOr)
{
    Group group;

    TableRef data_table = group.add_table("data");
    auto int_col = data_table->add_column(type_Int, "col");

    TableRef link_table = group.add_table("link");
    auto link_col = link_table->add_column_link(type_LinkList, "col", *data_table);

    const size_t count = 5;
    ObjKeys data_keys;
    data_table->create_objects(count, data_keys);
    auto o = link_table->create_object();
    auto lvr = o.get_linklist(link_col);

    // Populate data and add rows to the linkview in the opposite order of the
    // table's order
    for (size_t i = 0; i < count; ++i) {
        auto oo = data_table->get_object(data_keys[i]);
        oo.set<Int>(int_col, i);
        lvr.add(data_keys[count - i - 1]);
    }

    // Verify that a query with Or() returns all results
    TableView tv =
        data_table->where(lvr).group().equal(int_col, 1000).Or().between(int_col, 2, 4).end_group().find_all();
    CHECK_EQUAL(3, tv.size());
    CHECK_EQUAL(data_keys[4], tv[0].get_key());
    CHECK_EQUAL(data_keys[3], tv[1].get_key());
    CHECK_EQUAL(data_keys[2], tv[2].get_key());
}

// Check that table views created through backlinks are updated correctly
// (marked as out of sync) when the source table is modified.
TEST(BackLink_Query_TableViewSyncsWhenNeeded)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    ColKey col_int = source->add_column(type_Int, "id");
    ColKey col_link = source->add_column_link(type_Link, "link", *target);
    ColKey col_linklist = source->add_column_link(type_LinkList, "linklist", *target);

    target->add_column(type_Int, "id");

    auto o0 = source->create_object().set_all(0);
    auto o1 = source->create_object().set_all(0);
    auto o2 = source->create_object().set_all(2);

    ObjKeys keys;
    target->create_objects(3, keys);
    o0.set(col_link, keys[0]);
    o1.set(col_link, keys[1]);

    Query q = target->backlink(*source, col_link).column<Int>(col_int) > 0;
    TableView tv = q.find_all();
    CHECK_TABLE_VIEW(tv, {});

    o1.set(col_int, 1);
    CHECK_EQUAL(false, tv.is_in_sync());

    tv.sync_if_needed();
    CHECK_TABLE_VIEW(tv, {keys[1]});

    o2.set(col_link, keys[2]);
    CHECK_EQUAL(false, tv.is_in_sync());

    tv.sync_if_needed();
    CHECK_TABLE_VIEW(tv, {keys[1], keys[2]});

    Query list_query = target->backlink(*source, col_linklist).column<Int>(col_int) > 0;
    TableView list_tv = list_query.find_all();
    CHECK_TABLE_VIEW(list_tv, {});

    CHECK_EQUAL(0, o0.get_link_count(col_linklist));
    auto list = o0.get_linklist(col_linklist);

    list.add(keys[0]);
    list.add(keys[0]);

    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_EQUAL(2, o0.get_link_count(col_linklist));
    CHECK_TABLE_VIEW(list_tv, {});

    list.add(keys[2]);

    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_EQUAL(3, o0.get_link_count(col_linklist));
    CHECK_TABLE_VIEW(list_tv, {});

    auto list2 = o2.get_linklist(col_linklist);
    list2.add(keys[0]);

    CHECK_EQUAL(1, o2.get_link_count(col_linklist));
    CHECK_TABLE_VIEW(list_tv, {});
    CHECK_EQUAL(false, list_tv.is_in_sync());
    list_tv.sync_if_needed();
    CHECK_EQUAL(true, list_tv.is_in_sync());

    CHECK_TABLE_VIEW(list_tv, {keys[0]});
}

// Test queries involving the backlinks of a link column.
TEST(BackLink_Query_Link)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");


    target->add_column(type_Int, "id");

    auto k0 = target->create_object().set_all(0).get_key();
    auto k1 = target->create_object().set_all(1).get_key();
    auto k2 = target->create_object().set_all(2).get_key();
    auto k3 = target->create_object().set_all(3).get_key();

    auto col_link = source->add_column_link(type_Link, "link", *target);
    auto col_int = source->add_column(type_Int, "int");
    auto col_double = source->add_column(type_Double, "double");
    auto col_string = source->add_column(type_String, "string");

    auto add_row = [&](std::vector<ObjKey> link_targets, int64_t i, double d, const char* string) {
        auto row = source->create_object();
        for (auto link_target : link_targets) // only 0 or 1 element
            row.set(col_link, link_target);
        row.set(col_int, i);
        row.set(col_double, d);
        row.set(col_string, string);
    };

    add_row({}, 10, 10.0, "first");
    add_row({k1}, 20, 20.0, "second");
    add_row({k2}, 30, 30.0, "third");
    add_row({k3}, 40, 40.0, "fourth");
    add_row({k3}, 50, 50.0, "fifth");

    Query q1 = target->backlink(*source, col_link).column<Int>(col_int) > 25;
    CHECK_TABLE_VIEW(q1.find_all(), {k2, k3});

    Query q2 = target->backlink(*source, col_link).column<Double>(col_double) < 25.0;
    CHECK_TABLE_VIEW(q2.find_all(), {k1});

    Query q3 = target->backlink(*source, col_link).column<StringData>(col_string).begins_with("f");
    CHECK_TABLE_VIEW(q3.find_all(), {k3});

    Query q4 = target->column<BackLink>(*source, col_link).is_null();
    CHECK_TABLE_VIEW(q4.find_all(), {k0});

    Query q5 = target->column<BackLink>(*source, col_link).count() == 0;
    CHECK_TABLE_VIEW(q5.find_all(), {k0});
    Query q6 = target->column<BackLink>(*source, col_link).column<Int>(col_int).average() > 42.5;
    CHECK_TABLE_VIEW(q6.find_all(), {k3});

    Query q7 = target->column<BackLink>(*source, col_link).column<Double>(col_double).min() < 30.0;
    CHECK_TABLE_VIEW(q7.find_all(), {k1});

    Query q8 = target->column<BackLink>(*source, col_link).column<Int>(col_int).sum() == 0;
    CHECK_TABLE_VIEW(q8.find_all(), {k0});

    Query q9 = target->column<BackLink>(*source, col_link).column<Int>(col_int).average() == null();
    CHECK_TABLE_VIEW(q9.find_all(), {k0});
}

// Test queries involving the backlinks of a link list column.
TEST(BackLink_Query_LinkList)
{
    Group group;

    TableRef source = group.add_table("source");
    TableRef target = group.add_table("target");

    target->add_column(type_Int, "id");

    auto k0 = target->create_object().set_all(0).get_key();
    auto k1 = target->create_object().set_all(1).get_key();
    auto k2 = target->create_object().set_all(2).get_key();
    auto k3 = target->create_object().set_all(3).get_key();
    auto k4 = target->create_object().set_all(4).get_key();

    auto col_linklist = source->add_column_link(type_LinkList, "linklist", *target);
    auto col_int = source->add_column(type_Int, "int");
    auto col_double = source->add_column(type_Double, "double");
    auto col_string = source->add_column(type_String, "string");

    auto add_row = [&](std::vector<ObjKey> link_targets, int64_t i, double d, const char* string) {
        auto row = source->create_object();
        auto link_view = row.get_linklist(col_linklist);
        for (auto link_target : link_targets)
            link_view.add(link_target);
        row.set(col_int, i);
        row.set(col_double, d);
        row.set(col_string, string);
    };

    add_row({}, 10, 10.0, "first");
    add_row({k1, k1}, 20, 20.0, "second");
    add_row({k0, k1}, 30, 30.0, "third");
    add_row({k2, k3}, 40, 40.0, "fourth");
    add_row({k3}, 50, 50.0, "fifth");

    Query q1 = target->backlink(*source, col_linklist).column<Int>(col_int) > 25;
    CHECK_TABLE_VIEW(q1.find_all(), {k0, k1, k2, k3});

    Query q2 = target->backlink(*source, col_linklist).column<Double>(col_double) < 25.0;
    CHECK_TABLE_VIEW(q2.find_all(), {k1});

    Query q3 = target->backlink(*source, col_linklist).column<StringData>(col_string).begins_with("f");
    CHECK_TABLE_VIEW(q3.find_all(), {k2, k3});

    Query q4 = target->column<BackLink>(*source, col_linklist).is_null();
    CHECK_TABLE_VIEW(q4.find_all(), {k4});

    Query q5 = target->column<BackLink>(*source, col_linklist).count() == 0;
    CHECK_TABLE_VIEW(q5.find_all(), {k4});

    Query q6 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).average() > 42.5;
    CHECK_TABLE_VIEW(q6.find_all(), {k3});

    Query q7 = target->column<BackLink>(*source, col_linklist).column<Double>(col_double).min() < 30.0;
    CHECK_TABLE_VIEW(q7.find_all(), {k1});

    Query q8 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).sum() == 0;
    CHECK_TABLE_VIEW(q8.find_all(), {k4});

    Query q9 = target->column<BackLink>(*source, col_linklist).column<Int>(col_int).average() == null();
    CHECK_TABLE_VIEW(q9.find_all(), {k4});

    Query q10 = target->column<BackLink>(*source, col_linklist).column<Double>(col_double).sum() == 70;
    CHECK_TABLE_VIEW(q10.find_all(), {k1});

    Query q11 =
        target->column<BackLink>(*source, col_linklist, source->column<Double>(col_double) == 20.0).count() == 2;
    CHECK_TABLE_VIEW(q11.find_all(), {k1});
}

// Test queries involving multiple levels of links and backlinks.
TEST(BackLink_Query_MultipleLevels)
{
    Group group;

    TableRef people = group.add_table("people");

    auto col_name = people->add_column(type_String, "name");
    auto col_age = people->add_column(type_Int, "age");
    auto col_children = people->add_column_link(type_LinkList, "children", *people);

    auto add_person = [&](std::string name, int age, std::vector<ObjKey> children) {
        auto row = people->create_object();
        auto children_link_view = row.get_linklist(col_children);
        for (auto child : children)
            children_link_view.add(child);
        row.set(col_name, name);
        row.set(col_age, age);
        return row.get_key();
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
    Query q7 = people->column<BackLink>(*people, col_children) == people->get_object(raewynne);
    CHECK_TABLE_VIEW(q7.find_all(), {mark, jason});

    // People that have Mark as a child.
    Query q8 = people->column<Link>(col_children) == people->get_object(mark);
    CHECK_TABLE_VIEW(q8.find_all(), {michael, raewynne});

    // People that have Michael as a grandparent.
    Query q9 = people->backlink(*people, col_children).column<BackLink>(*people, col_children) ==
               people->get_object(michael);
    CHECK_TABLE_VIEW(q9.find_all(), {hannah, elijah});

    // People that have Hannah as a grandchild.
    Query q10 = people->link(col_children).column<Link>(col_children) == people->get_object(hannah);
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
}

// Test queries involving the multiple levels of backlinks across multiple tables.
TEST(BackLink_Query_MultipleLevelsAndTables)
{
    Group group;

    TableRef a = group.add_table("a");
    TableRef b = group.add_table("b");
    TableRef c = group.add_table("c");
    TableRef d = group.add_table("d");

    auto col_id = a->add_column(type_Int, "id");
    auto col_a_to_b = a->add_column_link(type_Link, "link", *b);

    auto col_b_to_c = b->add_column_link(type_Link, "link", *c);
    auto col_c_to_d = c->add_column_link(type_Link, "link", *d);

    d->add_column(type_Int, "id");

    auto k0 = d->create_object().set_all(0).get_key();
    auto k1 = d->create_object().set_all(1).get_key();
    auto saved1 = k1; // the thing to find in the end
    k0 = c->create_object().set_all(k0).get_key();
    k1 = c->create_object().set_all(k1).get_key();
    k0 = b->create_object().set_all(k0).get_key();
    k1 = b->create_object().set_all(k1).get_key();
    k0 = a->create_object().set_all(0, k0).get_key();
    k1 = a->create_object().set_all(1, k1).get_key();
    Query q = d->backlink(*c, col_c_to_d).backlink(*b, col_b_to_c).backlink(*a, col_a_to_b).column<Int>(col_id) == 1;
    CHECK_TABLE_VIEW(q.find_all(), {saved1});
}

#endif
