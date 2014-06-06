#include "testsettings.hpp"
#ifdef TEST_GROUP


#include <tightdb.hpp>
#include <tightdb/util/file.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

namespace {

enum Days { Mon, Tue, Wed, Thu, Fri, Sat, Sun };

TIGHTDB_TABLE_4(TestTableLinks,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

} // Anonymous namespace

TEST(Links_Columns)
{
    // Test adding and removing columns with links

    Group group;

    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

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

        TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
        table1->add("test1", 1, true, Mon);
        table1->add("test2", 2, false, Tue);
        table1->add("test3", 3, true, Wed);

        // create table with links to table1
        TableRef table2 = group.get_table("table2");
        size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
        CHECK_EQUAL(table1, table2->get_link_target(col_link));

        // add a few links
        table2->insert_link(col_link, 0, 1);
        table2->insert_done();

        table2->insert_link(col_link, 1, 0);
        table2->insert_done();

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

    TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with links to table1
    TableRef table2 = group.get_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links
    table2->insert_link(col_link, 0, 1);
    table2->insert_done();
    table2->insert_link(col_link, 1, 0);
    table2->insert_done();

    while (!table2->is_empty()) {
        table2->move_last_over(0);
    }

    CHECK(table2->is_empty());
    CHECK_EQUAL(0, table1->get_backlink_count(0, *table2, col_link));
    CHECK_EQUAL(0, table1->get_backlink_count(2, *table2, col_link));

    // add links again
    table2->insert_link(col_link, 0, 1);
    table2->insert_done();
    table2->insert_link(col_link, 1, 0);
    table2->insert_done();

    // remove all rows in target table
    while (!table1->is_empty()) {
        table1->move_last_over(0);
    }

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
    table2->insert_link(col_link, 0, 1);
    table2->insert_done();
    table2->insert_link(col_link, 1, 0);
    table2->insert_done();

    // clear target table and make sure links are nullified
    table1->clear();
    CHECK(table2->is_null_link(col_link, 0));
    CHECK(table2->is_null_link(col_link, 1));
}

TEST(Links_Multi)
{
    // Multiple links to same rows
    Group group;

    TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with links to table1
    TableRef table2 = group.get_table("table2");
    size_t col_link = table2->add_column_link(type_Link, "link", *TableRef(table1));
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links pointing to same row
    table2->insert_link(col_link, 0, 1);
    table2->insert_done();
    table2->insert_link(col_link, 1, 1);
    table2->insert_done();
    table2->insert_link(col_link, 2, 1);
    table2->insert_done();

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
    table2->insert_link(col_link, 2, 2);
    table2->insert_done();
    table2->insert_link(col_link, 3, 2);
    table2->insert_done();
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

    TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with multiple links to table1
    TableRef table2 = group.get_table("table2");
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

    TestTableLinks::Ref target = group.get_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // create table with links to target table
    TableRef source = group.get_table("source");
    size_t col_link = source->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, source->get_link_target(col_link));

    source->insert_linklist(col_link, 0);
    source->insert_done();
    CHECK(source->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(0, source->get_link_count(col_link, 0));

    // add some more rows and test that they can be deleted
    source->insert_linklist(col_link, 1);
    source->insert_done();
    source->insert_linklist(col_link, 2);
    source->insert_done();
    source->insert_linklist(col_link, 3);
    source->insert_done();

    while (!source->is_empty()) {
        source->move_last_over(0);
    }

    // add some more rows and clear
    source->insert_linklist(col_link, 0);
    source->insert_done();
    source->insert_linklist(col_link, 1);
    source->insert_done();
    source->insert_linklist(col_link, 2);
    source->insert_done();
    source->clear();

}

TEST(Links_LinkList_Basics)
{
    Group group;

    TestTableLinks::Ref target = group.get_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // create table with links to target table
    TableRef source = group.get_table("source");
    size_t col_link = source->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, source->get_link_target(col_link));

    source->insert_linklist(col_link, 0);
    source->insert_done();

    LinkViewRef links = source->get_linklist(col_link, 0);

    // add several links to a single linklist
    links->add(2);
    links->add(1);
    links->add(0);
    CHECK(!source->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(3, links->size());
    CHECK_EQUAL(2, links->get_target_row(0));
    CHECK_EQUAL(1, links->get_target_row(1));
    CHECK_EQUAL(0, links->get_target_row(2));
    CHECK_EQUAL(Wed, (Days)(*links)[0].get_int(3));

    // verify that backlinks was set correctly
    CHECK_EQUAL(1, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *source, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *source, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(2, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *source, col_link, 0));

    // insert a link at a specific position in the linklist
    links->insert(1, 2);
    CHECK_EQUAL(4, source->get_link_count(col_link, 0));
    CHECK_EQUAL(2, links->get_target_row(0));
    CHECK_EQUAL(2, links->get_target_row(1));
    CHECK_EQUAL(1, links->get_target_row(2));
    CHECK_EQUAL(0, links->get_target_row(3));

    CHECK_EQUAL(2, target->get_backlink_count(2, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *source, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink(2, *source, col_link, 1));

    // change one link to another
    links->set(0, 1);
    CHECK_EQUAL(4, source->get_link_count(col_link, 0));
    CHECK_EQUAL(1, links->get_target_row(0));
    CHECK_EQUAL(2, links->get_target_row(1));
    CHECK_EQUAL(1, links->get_target_row(2));
    CHECK_EQUAL(0, links->get_target_row(3));

    CHECK_EQUAL(1, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *source, col_link, 0));
    CHECK_EQUAL(2, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(1, *source, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink(1, *source, col_link, 1));
    CHECK_EQUAL(1, target->get_backlink_count(2, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *source, col_link, 0));

    // move a link
    links->move(3, 0);
    CHECK_EQUAL(4, source->get_link_count(col_link, 0));
    CHECK_EQUAL(0, links->get_target_row(0));
    CHECK_EQUAL(1, links->get_target_row(1));
    CHECK_EQUAL(2, links->get_target_row(2));
    CHECK_EQUAL(1, links->get_target_row(3));

    // remove a link
    links->remove(0);
    CHECK_EQUAL(3, source->get_link_count(col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(0, *source, col_link));

    // remove all links
    links->clear();
    CHECK(source->linklist_is_empty(col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, *source, col_link));
}

TEST(Links_LinkList_Backlinks)
{
    Group group;

    TestTableLinks::Ref target = group.get_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // create table with links to target table
    TableRef source = group.get_table("source");
    size_t col_link = source->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, source->get_link_target(col_link));

    source->insert_linklist(col_link, 0);
    source->insert_done();

    LinkViewRef links = source->get_linklist(col_link, 0);
    links->add(2);
    links->add(1);
    links->add(0);

    // remove a target row and check that source links are removed as well
    target->move_last_over(1);
    CHECK_EQUAL(2, source->get_link_count(col_link, 0));
    CHECK_EQUAL(1, links->get_target_row(0));
    CHECK_EQUAL(0, links->get_target_row(1));

    // remove all
    target->clear();
    CHECK_EQUAL(0, source->get_link_count(col_link, 0));
    CHECK(links->is_empty());

    // re-add rows to target
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // add more rows with links
    source->add_empty_row();
    source->add_empty_row();
    LinkViewRef links1 = source->get_linklist(col_link, 1);
    LinkViewRef links2 = source->get_linklist(col_link, 2);

    // add links from each row
    links->add(2);
    links1->add(1);
    links2->add(0);

    // Verify backlinks
    CHECK_EQUAL(1, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(2, target->get_backlink(0, *source, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(1, target->get_backlink(1, *source, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(2, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(2, *source, col_link, 0));

    // delete a row and make sure backlinks are updated
    source->move_last_over(0);
    CHECK_EQUAL(1, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, *source, col_link, 0));
    CHECK_EQUAL(1, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(1, target->get_backlink(1, *source, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(2, *source, col_link));

    // delete last row and make sure backlinks are updated
    source->move_last_over(1);
    CHECK_EQUAL(1, target->get_backlink_count(0, source_table_ndx, col_link));
    CHECK_EQUAL(0, target->get_backlink(0, source_table_ndx, col_link, 0));
    CHECK_EQUAL(0, target->get_backlink_count(1, source_table_ndx, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, source_table_ndx, col_link));

    // remove all link lists and make sure backlinks are updated
    source->clear();
    CHECK_EQUAL(0, target->get_backlink_count(0, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(1, *source, col_link));
    CHECK_EQUAL(0, target->get_backlink_count(2, *source, col_link));
}

TEST(Links_LinkList_AccessorUpdates)
{
    Group group;

    TestTableLinks::Ref target = group.get_table<TestTableLinks>("target");
    target->add("test1", 1, true,  Mon);
    target->add("test2", 2, false, Tue);
    target->add("test3", 3, true,  Wed);

    // create table with links to target table
    TableRef source = group.get_table("source");
    size_t col_link = source->add_column_link(type_LinkList, "links", *TableRef(target));
    CHECK_EQUAL(target, source->get_link_target(col_link));

    source->insert_linklist(col_link, 0);
    source->insert_done();
    source->insert_linklist(col_link, 1);
    source->insert_done();
    source->insert_linklist(col_link, 2);
    source->insert_done();

    LinkViewRef links0 = source->get_linklist(col_link, 0);
    links0->add(2);
    links0->add(1);
    links0->add(0);

    LinkViewRef links1 = source->get_linklist(col_link, 1);
    links1->add(2);
    links1->add(1);
    links1->add(0);

    LinkViewRef links2 = source->get_linklist(col_link, 2);
    links2->add(2);
    links2->add(1);
    links2->add(0);

    CHECK_EQUAL(0, links0->get_parent_row());
    CHECK_EQUAL(1, links1->get_parent_row());
    CHECK_EQUAL(2, links2->get_parent_row());

    // get the same linkview twice
    LinkViewRef links2again = source->get_linklist(col_link, 2);
    CHECK_EQUAL(links2->get_parent_row(), links2again->get_parent_row());

    // delete a row and make sure involved accessors are updated
    source->move_last_over(0);
    CHECK_EQUAL(false, links0->is_attached());
    CHECK_EQUAL(0, links2->get_parent_row());
    CHECK_EQUAL(0, links2again->get_parent_row());

    // clear and make sure all accessors get detached
    source->clear();
    CHECK_EQUAL(false, links1->is_attached());
    CHECK_EQUAL(false, links2->is_attached());
    CHECK_EQUAL(false, links2again->is_attached());
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
        TableRef dogs = group.get_table("dogs");
        dogs->add_column(type_String, "dogName");

        // Create owners table
        TableRef owners = group.get_table("owners");
        owners->add_column(type_String, "name");
        owners->add_column_link(type_Link, "dog", *dogs);

        // Insert a single dog
        dogs->insert_string(name_col, harvey_row, "Harvey");
        dogs->insert_done();

        // Insert an owner with link to dog
        owners->insert_string(name_col, tim_row, "Tim");
        owners->insert_link(dog_col, tim_row, harvey_row);
        owners->insert_done();

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


#endif // TEST_GROUP
