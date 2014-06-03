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

TEST(LinkView_Basic)
{
    {
        Group group;

        size_t table1_ndx = 0;
        size_t table2_ndx = 1;
        TableRef table1 = group.get_table("table1");
        TableRef table2 = group.get_table("table2");

        // add some more columns to table1 and table2
        table1->add_column(type_Int, "col1");
        table2->add_column(type_Int, "col1");

        // add some rows
        table1->add_empty_row();
        table1->set_int(0, 0, 100);
        table1->add_empty_row();
        table1->set_int(0, 1, 200);
        table1->add_empty_row();
        table1->set_int(0, 2, 300);

        table2->add_empty_row();
        table2->set_int(0, 0, 400);
        table2->add_empty_row();
        table2->set_int(0, 1, 500);
        table2->add_empty_row();
        table2->set_int(0, 2, 600);

        size_t col_link2 = table1->add_column_link(type_LinkList, "link", table2_ndx); // todo, rename to add_link_column() ?

        // set some links
        table1->linklist_add_link(col_link2, 0, 1);
        table1->linklist_add_link(col_link2, 0, 2);
        
        LinkView lv = table1->links(col_link2, 0);

        CHECK_EQUAL(500, lv.get_int(0, 0));
        CHECK_EQUAL(600, lv.get_int(0, 1));

        size_t t = lv.find_first_int(0, 600);
        CHECK_EQUAL(1, t);

        lv.remove(0);
        // 600 bumped down to row 0
        CHECK_EQUAL(600, lv.get_int(0, 0));
    }
}

TEST(LinkView_Query)
{
    Group group;

    size_t table1_ndx = 0;
    size_t table2_ndx = 1;
    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

    // add some more columns to table1 and table2
    table1->add_column(type_Int, "col1");

    table2->add_column(type_Int, "col1");
    table2->add_column(type_String, "str2");

    // add some rows
    table1->add_empty_row();
    table1->set_int(0, 0, 100);
    table1->add_empty_row();
    table1->set_int(0, 1, 200);
    table1->add_empty_row();
    table1->set_int(0, 2, 300);

    table2->add_empty_row();
    table2->set_int(0, 0, 400);
    table2->set_string(1, 0, "hello");
    table2->add_empty_row();
    table2->set_int(0, 1, 500);
    table2->set_string(1, 1, "world");
    table2->add_empty_row();
    table2->set_int(0, 2, 600);
    table2->set_string(1, 2, "!");

    size_t col_link2 = table1->add_column_link(type_LinkList, "link", table2_ndx); // todo, rename to add_link_column() ?

    // set some links

    table1->linklist_add_link(col_link2, 0, 1);

    table1->linklist_add_link(col_link2, 1, 1);
    table1->linklist_add_link(col_link2, 1, 2);

    size_t match;

    match = (table1->link(col_link2).column<Int>(0) > 550).find();
    CHECK_EQUAL(1, match);

    match = (table2->column<String>(1) == "world").find();
    CHECK_EQUAL(1, match);

    match = (table1->link(col_link2).column<String>(1) == "!").find();
    CHECK_EQUAL(1, match);


}

#endif
