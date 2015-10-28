#include "testsettings.hpp"
#ifdef TEST_LINKS


#include <realm.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

REALM_TABLE_4(TestTableLinks,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

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
    table1->add_column(type_String, "col1");
    table2->add_column(type_String, "col1");

    // add some rows
    table1->add_empty_row();
    table1->set_string(0, 0, "string1");
    table1->add_empty_row();
    table2->add_empty_row();
    table2->add_empty_row();

    size_t col_link2 = table1->add_column_link(type_Link, "link", *table2);

    // set some links
    table1->set_link(col_link2, 0, 1);
    CHECK_EQUAL(1, table2->get_backlink_count(1, *table1, col_link2));
    CHECK_EQUAL(0, table2->get_backlink(1, *table1, col_link2, 0));

    // insert new column at specific location (moving link column)
    table1->insert_column(0, type_Int, "first");
    size_t new_link_col_ndx = col_link2 + 1;
    CHECK_EQUAL(1, table2->get_backlink_count(1, *table1, new_link_col_ndx));
    CHECK_EQUAL(0, table2->get_backlink(1, *table1, new_link_col_ndx, 0));

    // add one more column (moving link column)
    table1->insert_column(1, type_Int, "second");
    size_t new_link_col_ndx2 = new_link_col_ndx + 1;
    CHECK_EQUAL(1, table2->get_backlink_count(1, *table1, new_link_col_ndx2));
    CHECK_EQUAL(0, table2->get_backlink(1, *table1, new_link_col_ndx2, 0));

    // remove a column (moving link column back)
    table1->remove_column(0);
    CHECK_EQUAL(1, table2->get_backlink_count(1, *table1, new_link_col_ndx));
    CHECK_EQUAL(0, table2->get_backlink(1, *table1, new_link_col_ndx, 0));
}


TEST(Links_Basic)
{
    GROUP_TEST_PATH(path);

    // Test basic link operations
    {
        Group group;

        TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
        table1->add("test1", 1, true, Mon);
        table1->add("test2", 2, false, Tue);
        table1->add("test3", 3, true, Wed);

        // create table with links to table1
        TableRef table2 = group.add_table("table2");
        size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
        CHECK_EQUAL(table1, table2->get_link_target(col_link));

        // add a few links
        table2->insert_empty_row(0);
        table2->set_link(col_link, 0, 1);

        table2->insert_empty_row(1);
        table2->set_link(col_link, 1, 0);

        // Verify that links were set correctly
        size_t link_ndx1 = table2->get_link(col_link, 0);
        size_t link_ndx2 = table2->get_link(col_link, 1);
        CHECK_EQUAL(1, link_ndx1);
        CHECK_EQUAL(0, link_ndx2);

        // Verify backlinks
        CHECK_EQUAL(1, table1->get_backlink_count(0, *table2, col_link));
        CHECK_EQUAL(1, table1->get_backlink(0, *table2, col_link, 0));
        CHECK_EQUAL(1, table1->get_backlink_count(1, *table2, col_link));
        CHECK_EQUAL(0, table1->get_backlink(1, *table2, col_link, 0));
        CHECK_EQUAL(0, table1->get_backlink_count(2, *table2, col_link));

        // Follow links using typed accessors
        CHECK_EQUAL("test2", table2->get_link_accessor<TestTableLinks>(0, 0).first);
        CHECK_EQUAL(2, table2->get_link_accessor<TestTableLinks>(0, 0).second);
        CHECK_EQUAL("test1", table2->get_link_accessor<TestTableLinks>(0, 1).first);
        CHECK_EQUAL(1, table2->get_link_accessor<TestTableLinks>(0, 1).second);

        // Change a link to point to a new location
        table2->set_link(col_link, 1, 2);

        size_t link_ndx3 = table2->get_link(col_link, 1);
        CHECK_EQUAL(2, link_ndx3);
        CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link));
        CHECK_EQUAL(1, table1->get_backlink_count(2, *table2, col_link));
        CHECK_EQUAL(1, table1->get_backlink(2, *table2, col_link, 0));

        // Delete a link. Note that links only work with unordered
        // top-level tables, so you have to delete with move_last_over
        table2->move_last_over(0);

        // Verify that delete went correctly
        CHECK_EQUAL(1, table2->size());
        CHECK_EQUAL(2, table2->get_link(col_link, 0));

        CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link));
        CHECK_EQUAL(0, table1->get_backlink_count(1, *table2, col_link));
        CHECK_EQUAL(1, table1->get_backlink_count(2, *table2, col_link));
        CHECK_EQUAL(0, table1->get_backlink(2, *table2, col_link, 0));

        // Nullify a link
        table2->nullify_link(col_link, 0);
        CHECK(table2->is_null_link(col_link, 0));
        CHECK_EQUAL(0, table1->get_backlink_count(2, *table2, col_link));

        // Add a new row to target table and verify that backlinks are
        // tracked for it as well
        table1->add("test4", 4, false, Thu);
        CHECK_EQUAL(0, table1->get_backlink_count(3, *table2, col_link));

        table1->add_empty_row();
        CHECK_EQUAL(0, table1->get_backlink_count(4, *table2, col_link));
        table2->set_link(col_link, 0, 4);
        CHECK_EQUAL(1, table1->get_backlink_count(4, *table2, col_link));

        group.write(path);
    }

    // Reopen same group from disk
    {
        Group group(path);

        TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
        TableRef table2 = group.get_table("table2");

        // Verify that we are pointing to the right table
        CHECK_EQUAL(table1, table2->get_link_target(0));

        // Verify that links are still correct
        size_t link_ndx1 = table2->get_link(0, 0);
        CHECK_EQUAL(4, link_ndx1);
    }
}


TEST(Links_Deletes)
{
    Group group;

    TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with links to table1
    TableRef table2 = group.add_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 0);

    while (!table2->is_empty())
        table2->move_last_over(0);

    CHECK(table2->is_empty());
    CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink_count(2, *table2, col_link));

    // add links again
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 0);

    // remove all rows in target table
    while (!table1->is_empty())
        table1->move_last_over(0);

    // verify that originating links was nullified
    CHECK(table2->is_null_link(col_link, 0));
    CHECK(table2->is_null_link(col_link, 1));

    // add target rows again with links
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);
    table2->set_link(col_link, 0, 1);
    table2->set_link(col_link, 1, 0);

    // clear entire table and make sure backlinks are removed as well
    table2->clear();
    CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink_count(2, *table2, col_link));

    // add links again
    table2->insert_empty_row(0);
    table2->set_link(col_link, 0, 1);
    table2->insert_empty_row(1);
    table2->set_link(col_link, 1, 0);

    // clear target table and make sure links are nullified
    table1->clear();
    CHECK(table2->is_null_link(col_link, 0));
    CHECK(table2->is_null_link(col_link, 1));
}


TEST(Links_Inserts)
{
    Group group;

    TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref table1 = group.add_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

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


TEST(Links_LinkList_TableOps)
{
    Group group;

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // create table with links to target table
    TableRef origin = group.add_table("origin");
    size_t col_link = origin->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, origin->get_link_target(col_link));

    origin->insert_empty_row(0);
    CHECK(origin->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(0, origin->get_link_count(col_link, 0));

    // add some more rows and test that they can be deleted
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);
    origin->insert_empty_row(3);

    while (!origin->is_empty()) {
        origin->move_last_over(0);
    }

    // add some more rows and clear
    origin->insert_empty_row(0);
    origin->insert_empty_row(1);
    origin->insert_empty_row(2);
    origin->clear();

}


TEST(Links_LinkList_Basics)
{
    Group group;

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    // insert a link at a specific position in the linklist
    links->insert(1, 2);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(2, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(1, links->get(2).get_index());
    CHECK_EQUAL(0, links->get(3).get_index());

    CHECK_EQUAL(2, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 1));

    // change one link to another
    links->set(0, 1);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(1, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(1, links->get(2).get_index());
    CHECK_EQUAL(0, links->get(3).get_index());

    CHECK_EQUAL(1, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *origin, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *origin, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink(1, *origin, col_link, 1));
    CHECK_EQUAL(1, target->get_backlink_count(2, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *origin, col_link, 0));

    // move a link
    links->move(3, 0);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get(0).get_index());
    CHECK_EQUAL(1, links->get(1).get_index());
    CHECK_EQUAL(2, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    links->move(0, 2);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(1, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(0, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    links->move(2, 0);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get(0).get_index());
    CHECK_EQUAL(1, links->get(1).get_index());
    CHECK_EQUAL(2, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    links->move(2, 2);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get(0).get_index());
    CHECK_EQUAL(1, links->get(1).get_index());
    CHECK_EQUAL(2, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    // swap two links
    links->swap(1, 2);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(1, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    // swap a link with itself
    links->swap(2, 2);
    CHECK_EQUAL(4, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get(0).get_index());
    CHECK_EQUAL(2, links->get(1).get_index());
    CHECK_EQUAL(1, links->get(2).get_index());
    CHECK_EQUAL(1, links->get(3).get_index());

    // remove a link
    links->remove(0);
    CHECK_EQUAL(3, origin->get_link_count(col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(0, *origin, col_link));

    // remove all links
    links->clear();
    CHECK(origin->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(0, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(1, *origin, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, *origin, col_link));
}


TEST(Links_LinkList_Inserts)
{
    Group group;

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    // FIXME: Table::swap_rows does not currently exist, so call through the
    // private API for now.
    _impl::TableFriend::do_swap_rows(*origin, 1, 2);

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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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

    size_t name_col   = 0;
    size_t dog_col    = 1;
    size_t tim_row    = 0;
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

    TestTableLinks::Ref target = group.add_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

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
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);
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
    origin->add_column_link(type_Link,     "o_1", *target);
    origin->add_column_link(type_LinkList, "o_2", *target);
    origin->add_empty_row();
    origin->set_link(0,0,0);
    LinkViewRef link_list = origin->get_linklist(1,0);
    link_list->add(0);
    Row target_row_1 = target->get(0);
    Row target_row_2 = link_list->get(0);

    CHECK_EQUAL(1, target->size());
    target->remove_column(0);
    CHECK_EQUAL(0, target->get_column_count());
    CHECK(target->is_empty());
    CHECK(origin->is_null_link(0,0));
    CHECK(link_list->is_attached());
    CHECK_EQUAL(link_list, origin->get_linklist(1,0));
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
    origin->add_empty_row(REALM_MAX_BPNODE_SIZE+1);
    origin->clear();
    origin->add_empty_row();
    origin->get_linklist(0,0)->add(0);
    group.verify();
}


TEST(Links_ClearLinkListWithTwoLevelBptree)
{
    Group group;
    TableRef origin = group.add_table("origin");
    TableRef target = group.add_table("target");
    target->add_empty_row();
    origin->add_column_link(type_LinkList, "", *target);
    origin->add_empty_row();
    LinkViewRef link_list = origin->get_linklist(0,0);
    for (size_t i = 0; i < REALM_MAX_BPNODE_SIZE+1; ++i)
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
        origin->set_link(0,0,0);
        origin->set_link(0,1,0);
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
                refs[tables.size()] = group.get_or_add_table("table"); // FIXME: Lasse, did you really want to re-get the same table every time?
                tables.push_back(std::vector<size_t>());
            }
        }
    }
}


TEST(Links_CascadeRemove_ColumnLink)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        Row origin_row_0, origin_row_1, origin_row_2;
        Row target_row_0, target_row_1, target_row_2;
        Fixture()
        {
            origin->add_column_link(type_Link, "o_1", *target, link_Strong);
            target->add_column(type_Int, "t_1");
            origin->add_empty_row(3);
            target->add_empty_row(3);
            origin_row_0 = origin->get(0);
            origin_row_1 = origin->get(1);
            origin_row_2 = origin->get(2);
            target_row_0 = target->get(0);
            target_row_1 = target->get(1);
            target_row_2 = target->get(2);
            origin_row_0.set_link(0,0); // origin[0].o_1 -> target[0]
            origin_row_1.set_link(0,1); // origin[1].o_1 -> target[1]
            origin_row_2.set_link(0,2); // origin[2].o_1 -> target[2]
        }
    };

    // Break link by nullifying
    {
        Fixture f;
        f.origin_row_0.nullify_link(0); // origin[0].o_1 -> realm::null()
        // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_1.nullify_link(0); // origin[1].o_1 -> realm::null()
        // Cascade: target->move_last_over(1)
        CHECK(f.target_row_0 && !f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_2.nullify_link(0); // origin[2].o_1 -> realm::null()
        // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
    }

    // Break link by reassign
    {
        Fixture f;
        f.origin_row_0.set_link(0,2); // origin[0].o_1 -> target[2]
        // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_1.set_link(0,0); // origin[1].o_1 -> target[0]
        // Cascade: target->move_last_over(1)
        CHECK(f.target_row_0 && !f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(0, f.origin_row_1.get_link(0));
        CHECK_EQUAL(1, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_2.set_link(0,1); // origin[2].o_1 -> target[1]
        // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(1, f.origin_row_2.get_link(0));
    }

    // Avoid breaking link by reassigning self
    {
        Fixture f;
        f.origin_row_0.set_link(0,0); // No effective change!
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(2, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_1.set_link(0,1); // No effective change!
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(2, f.origin_row_2.get_link(0));
    }
    {
        Fixture f;
        f.origin_row_2.set_link(0,2); // No effective change!
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(2, f.origin_row_2.get_link(0));
    }

    // Break link by explicit ordered row removal
    {
        Fixture f;
        f.origin_row_0.remove(); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_2.get_link(0));
        CHECK_EQUAL(0, f.origin_row_1.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
    }
    {
        Fixture f;
        f.origin_row_1.remove(); // Cascade: target->move_last_over(1)
        CHECK(f.target_row_0 && !f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_2.get_link(0));
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
    }
    {
        Fixture f;
        f.origin_row_2.remove(); // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_1.get_index());
    }

    // Break link by explicit unordered row removal
    {
        Fixture f;
        f.origin_row_0.move_last_over(); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_2.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_index());
        CHECK_EQUAL(0, f.origin_row_2.get_index());
    }
    {
        Fixture f;
        f.origin_row_1.move_last_over(); // Cascade: target->move_last_over(1)
        CHECK(f.target_row_0 && !f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_2.get_link(0));
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
    }
    {
        Fixture f;
        f.origin_row_2.move_last_over(); // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_EQUAL(0, f.origin_row_0.get_link(0));
        CHECK_EQUAL(1, f.origin_row_1.get_link(0));
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_1.get_index());
    }

    // Break link by clearing table
    {
        Fixture f;
        f.origin->clear();
        CHECK(!f.target_row_0 && !f.target_row_1 && !f.target_row_2);
    }
}


TEST(Links_CascadeRemove_ColumnLinkList)
{
    struct Fixture {
        Group group;
        TableRef origin = group.add_table("origin");
        TableRef target = group.add_table("target");
        Row origin_row_0, origin_row_1, origin_row_2;
        Row target_row_0, target_row_1, target_row_2;
        LinkViewRef link_list_0, link_list_1, link_list_2;
        Fixture()
        {
            origin->add_column_link(type_LinkList, "o_1", *target, link_Strong);
            target->add_column(type_Int, "t_1");
            origin->add_empty_row(3);
            target->add_empty_row(3);
            origin_row_0 = origin->get(0);
            origin_row_1 = origin->get(1);
            origin_row_2 = origin->get(2);
            target_row_0 = target->get(0);
            target_row_1 = target->get(1);
            target_row_2 = target->get(2);
            link_list_0 = origin_row_0.get_linklist(0);
            link_list_1 = origin_row_1.get_linklist(0);
            link_list_2 = origin_row_2.get_linklist(0);
            link_list_0->add(1); // origin[0].o_1 -> [ target[1] ]
            link_list_1->add(0);
            link_list_1->add(1); // origin[1].o_1 -> [ target[0], target[1] ]
            link_list_2->add(2);
            link_list_2->add(1);
            link_list_2->add(2); // origin[1].o_1 -> [ target[2], target[1], target[2] ]
        }
    };

    // Break links by clearing list
    {
        Fixture f;
        f.link_list_0->clear(); // Cascade: Nothing
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.link_list_1->clear(); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(2).get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.link_list_2->clear(); // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.group.verify();
    }

    // Break links by removal from list
    {
        Fixture f;
        f.link_list_0->remove(0); // Cascade: Nothing
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.link_list_1->remove(0); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(2).get_index());
        f.group.verify();
    }

    // Break links by reassign
    {
        Fixture f;
        f.link_list_0->set(0,0); // Cascade: Nothing
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(0, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.link_list_1->set(0,1); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(2).get_index());
        f.group.verify();
    }

    // Avoid breaking links by reassigning self
    {
        Fixture f;
        f.link_list_1->set(0,0);
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        f.group.verify();
    }

    // Break links by explicit ordered row removal
    {
        Fixture f;
        f.origin_row_0.remove(); // Cascade: Nothing
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_NOT(f.link_list_0->is_attached());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        CHECK_EQUAL(0, f.origin_row_1.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.origin_row_1.remove(); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_NOT(f.link_list_1->is_attached());
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(2).get_index());
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.origin_row_2.remove(); // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_NOT(f.link_list_2->is_attached());
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_1.get_index());
        f.group.verify();
    }

    // Break links by explicit unordered row removal
    {
        Fixture f;
        f.origin_row_0.move_last_over(); // Cascade: Nothing
        CHECK(f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_NOT(f.link_list_0->is_attached());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(2, f.link_list_2->get(2).get_index());
        CHECK_EQUAL(1, f.origin_row_1.get_index());
        CHECK_EQUAL(0, f.origin_row_2.get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.origin_row_1.move_last_over(); // Cascade: target->move_last_over(0)
        CHECK(!f.target_row_0 && f.target_row_1 && f.target_row_2);
        CHECK_NOT(f.link_list_1->is_attached());
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_2->get(1).get_index());
        CHECK_EQUAL(0, f.link_list_2->get(2).get_index());
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_2.get_index());
        f.group.verify();
    }
    {
        Fixture f;
        f.origin_row_2.move_last_over(); // Cascade: target->move_last_over(2)
        CHECK(f.target_row_0 && f.target_row_1 && !f.target_row_2);
        CHECK_NOT(f.link_list_2->is_attached());
        CHECK_EQUAL(1, f.link_list_0->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        CHECK_EQUAL(0, f.origin_row_0.get_index());
        CHECK_EQUAL(1, f.origin_row_1.get_index());
        f.group.verify();
    }

    // Break link by clearing table
    {
        Fixture f;
        f.origin->clear();
        CHECK(!f.target_row_0 && !f.target_row_1 && !f.target_row_2);
        f.group.verify();
    }
}


TEST(Links_CascadeRemove_MultiLevel)
{
}


TEST(Links_CascadeRemove_Cycles)
{
}


TEST(Links_OrderedRowRemoval)
{
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(0);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(0);
        table->get_linklist(0,1)->add(0);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(1);
        table->get_linklist(0,1)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(0);
        table->get_linklist(0,1)->add(1);
        table->remove(0);
        group.verify();
    }
    {
        Group group;
        TableRef table = group.add_table("table");
        table->add_column_link(type_LinkList, "link_list", *table);
        table->add_empty_row();
        table->add_empty_row();
        table->get_linklist(0,0)->add(1);
        table->get_linklist(0,1)->add(0);
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
        LinkViewRef link_list_1, link_list_2;
        Fixture()
        {
            origin->add_column_link(type_LinkList, "", *target);
            target->add_column(type_Int, "");
            origin->add_empty_row(2);
            target->add_empty_row(2);
            link_list_1 = origin->get_linklist(0,0);
            link_list_1->add(0);
            link_list_1->add(1);
            link_list_2 = origin->get_linklist(0,1); // Leave it empty
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
        f.link_list_1->swap(0,0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.link_list_1->swap(1,1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.group.verify();
    }

    // Both orders of arguments mean the same this
    {
        Fixture f;
        f.link_list_1->swap(0,1);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(1, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(0, f.link_list_1->get(1).get_index());
        f.link_list_1->swap(1,0);
        CHECK_EQUAL(2, f.link_list_1->size());
        CHECK_EQUAL(0, f.link_list_1->get(0).get_index());
        CHECK_EQUAL(1, f.link_list_1->get(1).get_index());
        f.group.verify();
    }

    // Detached accessor
    {
        Fixture f;
        f.origin->remove(0);
        CHECK_LOGIC_ERROR(f.link_list_1->swap(0,1), LogicError::detached_accessor);
        f.group.verify();
    }

    // Index out of range
    {
        Fixture f;
        CHECK_LOGIC_ERROR(f.link_list_1->swap(1,2), LogicError::link_index_out_of_range);
        CHECK_LOGIC_ERROR(f.link_list_1->swap(2,1), LogicError::link_index_out_of_range);
        CHECK_LOGIC_ERROR(f.link_list_2->swap(0,0), LogicError::link_index_out_of_range);
        f.group.verify();
    }
}

#endif // TEST_LINKS
