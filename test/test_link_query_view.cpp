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

TEST(LinkList_Basic1)
{
    Group group;

    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

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

    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

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

    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

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

    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

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


// Attempts to expose a bug (it would assert) where TableView::Clear() was called with an unordered m_table. 
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

        TableRef table1 = group.get_table("table1");
        TableRef table2 = group.get_table("table2");

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

        TableRef table1 = group.get_table("table1");
        TableRef table2 = group.get_table("table2");

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

        TableRef table1 = group.get_table("table1");
        TableRef table2 = group.get_table("table2");

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

#endif
