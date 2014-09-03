#include "testsettings.hpp"
#ifdef TEST_LINK_VIEW

#include <limits>
#include <string>
#include <sstream>
#include <ostream>

#include <tightdb/table_macros.hpp>
#include <tightdb/link_view.hpp> // lasse todo remove
#include <tightdb.hpp>

#include "util/misc.hpp"

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace test_util;
using namespace tightdb::util;

TEST(LinkList_Basic1)
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

    table2->set_link(col_link2, 0, 1);
    table2->set_link(col_link2, 1, 2);

    Query q = table2->link(col_link2).column<String>(1) == "!";
    TableView tv = q.find_all();

    Query q2 = table2->link(col_link2).column<Int>(0) == 200;
    TableView tv2 = q2.find_all();
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

    //    match = (table1->column<String>(1) == table1->column<String>(1)  ).find(); // not yet implemented
    //    CHECK_EQUAL(1, match);
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

        tv.clear();
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

        tv.clear();
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
    match = table1->where().links_to(col_link2, 1).find();
    CHECK_EQUAL(0, match);

    match = table1->where().links_to(col_link2, 2).find();
    CHECK_EQUAL(1, match);

    match = table1->where().links_to(col_link2, 3).find();
    CHECK_EQUAL(not_found, match);


    // find_all on query with Link
    tv = table1->where().links_to(col_link2, 2).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = table1->where().links_to(col_link2, 1).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));

    tv = table1->where().links_to(col_link2, 3).find_all();
    CHECK_EQUAL(0, tv.size());


    // find on query with LinkList
    match = table1->where().links_to(col_link3, 1).find();
    CHECK_EQUAL(0, match);

    match = table1->where().links_to(col_link3, 2).find();
    CHECK_EQUAL(1, match);

    match = table1->where().links_to(col_link3, 3).find();
    CHECK_EQUAL(not_found, match);

    // find_all on query with LinkList
    tv = table1->where().links_to(col_link3, 2).find_all();
    CHECK_EQUAL(1, tv.size());
    CHECK_EQUAL(1, tv.get_source_ndx(0));

    tv = table1->where().links_to(col_link3, 1).find_all();
    CHECK_EQUAL(2, tv.size());
    CHECK_EQUAL(0, tv.get_source_ndx(0));
    CHECK_EQUAL(1, tv.get_source_ndx(1));

    tv = table1->where().links_to(col_link3, 3).find_all();
    CHECK_EQUAL(0, tv.size());
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
    table3->set_float(2, 0, 100.0);

    table3->add_empty_row();
    table3->set_int(0, 1, 200);
    table3->set_string(1, 1, "bar");
    table3->set_float(2, 1, 200.0);

    table3->add_empty_row();
    table3->set_int(0, 2, 300);
    table3->set_string(1, 2, "baz");
    table3->set_float(2, 2, 300.0);

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

    tv = (table1->link(col_link2).link(col_linklist3).column<Int>(0) == "none").find_all();
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

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 300);
    table1->set_string(1, 0, "delta");
    table1->set_float(2, 0, 300.f);
    table1->set_double(3, 0, 300.);
    table1->set_string(4, 0, "alfa");

    table1->add_empty_row();
    table1->set_int(0, 1, 100);
    table1->set_string(1, 1, "alfa");
    table1->set_float(2, 1, 100.f);
    table1->set_double(3, 1, 100.);
    table1->set_string(4, 0, "alfa");

    table1->add_empty_row();
    table1->set_int(0, 2, 200);
    table1->set_string(1, 2, "beta");
    table1->set_float(2, 2, 200.f);
    table1->set_double(3, 2, 200.);
    table1->set_string(4, 0, "alfa");

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

    lvr->sort(0);
    tv = lvr->get_sorted_view(0);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1); // 2 1
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
    vector<size_t> v;
    v.push_back(4);
    v.push_back(1);
    lvr->sort(v, false);
    tv = lvr->get_sorted_view(v, false);
    CHECK_EQUAL(lvr->get(0).get_index(), 0);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 1);
    CHECK_EQUAL(tv.get(0).get_index(), 0);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 1);

    lvr->sort(v, true);
    tv = lvr->get_sorted_view(v, true);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);

    v.push_back(2);

    lvr->sort(v, true);
    tv = lvr->get_sorted_view(v, true);
    CHECK_EQUAL(lvr->get(0).get_index(), 1);
    CHECK_EQUAL(lvr->get(1).get_index(), 2);
    CHECK_EQUAL(lvr->get(2).get_index(), 0);
    CHECK_EQUAL(tv.get(0).get_index(), 1);
    CHECK_EQUAL(tv.get(1).get_index(), 2);
    CHECK_EQUAL(tv.get(2).get_index(), 0);
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

    // We have not yet defined behaviour of finding null-links in a linked-to table, so we just throw. Todo.
    CHECK_THROW_ANY(table2->link(col_linklist2).column<Link>(col_link1).is_null());
}


#endif
