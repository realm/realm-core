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

    size_t table1_ndx = 0;
    size_t table2_ndx = 1;
    TableRef table1 = group.get_table("table1");
    TableRef table2 = group.get_table("table2");

    // table1 can link to table2
    table2->add_column_link("link", table1_ndx);

    // add some more columns to table1 and table2
    table1->add_column(type_String, "col1");
    table2->add_column(type_String, "col1");

    // add some rows
    table1->add_empty_row();
    table1->set_string(0, 0, "string1");
    table1->add_empty_row();
    table2->add_empty_row();
    table2->add_empty_row();

    size_t col_link2 = table1->add_column_link("link", table2_ndx);

    // set some links
    table1->set_link(col_link2, 0, 1);
    CHECK_EQUAL(1, table2->get_backlink_count(1, table1_ndx, col_link2));
    CHECK_EQUAL(0, table2->get_backlink(1, table1_ndx, col_link2, 0));

    // insert new column at specific location (moving link column)
    table1->insert_column(0, type_Int, "first");
    size_t new_link_col_ndx = col_link2 + 1;
    CHECK_EQUAL(1, table2->get_backlink_count(1, table1_ndx, new_link_col_ndx));
    CHECK_EQUAL(0, table2->get_backlink(1, table1_ndx, new_link_col_ndx, 0));
}

TEST(Links_Basic)
{
    GROUP_TEST_PATH(path);

    // Test basic link operations
    {
        Group group;

        size_t table1_ndx = 0;
        TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
        table1->add("test1", 1, true, Mon);
        table1->add("test2", 2, false, Tue);
        table1->add("test3", 3, true, Wed);

        // create table with links to table1
        size_t table2_ndx = 1;
        TableRef table2 = group.get_table("table2");
        size_t col_link = table2->add_column_link("link", table1_ndx);
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
        CHECK_EQUAL(1, table1->get_backlink_count(0, table2_ndx, col_link));
        CHECK_EQUAL(1, table1->get_backlink(0, table2_ndx, col_link, 0));
        CHECK_EQUAL(1, table1->get_backlink_count(1, table2_ndx, col_link));
        CHECK_EQUAL(0, table1->get_backlink(1, table2_ndx, col_link, 0));
        CHECK_EQUAL(0, table1->get_backlink_count(2, table2_ndx, col_link));

        // Follow links using typed accessors
        CHECK_EQUAL("test2", table2->get_link_accessor<TestTableLinks>(0, 0).first);
        CHECK_EQUAL(2, table2->get_link_accessor<TestTableLinks>(0, 0).second);
        CHECK_EQUAL("test1", table2->get_link_accessor<TestTableLinks>(0, 1).first);
        CHECK_EQUAL(1, table2->get_link_accessor<TestTableLinks>(0, 1).second);

        // Change a link to point to a new location
        table2->set_link(col_link, 1, 2);

        size_t link_ndx3 = table2->get_link(col_link, 1);
        CHECK_EQUAL(2, link_ndx3);
        CHECK_EQUAL(0, table1->get_backlink_count(0, table2_ndx, col_link));
        CHECK_EQUAL(1, table1->get_backlink_count(2, table2_ndx, col_link));
        CHECK_EQUAL(1, table1->get_backlink(2, table2_ndx, col_link, 0));

        // Delete a link. Note that links only work with unordered
        // top-level tables, so you have to delete with move_last_over
        table2->move_last_over(0);

        // Verify that delete went correctly
        CHECK_EQUAL(1, table2->size());
        CHECK_EQUAL(2, table2->get_link(col_link, 0));

        CHECK_EQUAL(0, table1->get_backlink_count(0, table2_ndx, col_link));
        CHECK_EQUAL(0, table1->get_backlink_count(1, table2_ndx, col_link));
        CHECK_EQUAL(1, table1->get_backlink_count(2, table2_ndx, col_link));
        CHECK_EQUAL(0, table1->get_backlink(2, table2_ndx, col_link, 0));

        // Nullify a link
        table2->nullify_link(col_link, 0);
        CHECK(table2->is_null_link(col_link, 0));
        CHECK_EQUAL(0, table1->get_backlink_count(2, table2_ndx, col_link));

        // Add a new row to target table and verify that backlinks are
        // tracked for it as well
        table1->add("test4", 4, false, Thu);
        CHECK_EQUAL(0, table1->get_backlink_count(3, table2_ndx, col_link));

        table1->add_empty_row();
        CHECK_EQUAL(0, table1->get_backlink_count(4, table2_ndx, col_link));
        table2->set_link(col_link, 0, 4);
        CHECK_EQUAL(1, table1->get_backlink_count(4, table2_ndx, col_link));

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

    size_t table1_ndx = 0;
    TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with links to table1
    size_t table2_ndx = 1;
    TableRef table2 = group.get_table("table2");
    size_t col_link = table2->add_column_link("link", table1_ndx);
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
    CHECK_EQUAL(0, table1->get_backlink_count(0, table2_ndx, col_link));
    CHECK_EQUAL(0, table1->get_backlink_count(2, table2_ndx, col_link));

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
    CHECK_EQUAL(0, table1->get_backlink_count(0, table2_ndx, col_link));
    CHECK_EQUAL(0, table1->get_backlink_count(2, table2_ndx, col_link));

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

    size_t table1_ndx = 0;
    TestTableLinks::Ref table1 = group.get_table<TestTableLinks>("table1");
    table1->add("test1", 1, true,  Mon);
    table1->add("test2", 2, false, Tue);
    table1->add("test3", 3, true,  Wed);

    // create table with links to table1
    size_t table2_ndx = 1;
    TableRef table2 = group.get_table("table2");
    size_t col_link = table2->add_column_link("link", table1_ndx);
    CHECK_EQUAL(table1, table2->get_link_target(col_link));

    // add a few links pointing to same row
    table2->insert_link(col_link, 0, 1);
    table2->insert_done();
    table2->insert_link(col_link, 1, 1);
    table2->insert_done();
    table2->insert_link(col_link, 2, 1);
    table2->insert_done();

    CHECK_EQUAL(3, table1->get_backlink_count(1, table2_ndx, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, table2_ndx, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink(1, table2_ndx, col_link, 1));
    CHECK_EQUAL(2, table1->get_backlink(1, table2_ndx, col_link, 2));

    // nullify a link
    table2->nullify_link(col_link, 1);
    CHECK_EQUAL(2, table1->get_backlink_count(1, table2_ndx, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, table2_ndx, col_link, 0));
    CHECK_EQUAL(2, table1->get_backlink(1, table2_ndx, col_link, 1));

    // nullify one more to reduce to one link (test re-inlining)
    table2->nullify_link(col_link, 0);
    CHECK_EQUAL(1, table1->get_backlink_count(1, table2_ndx, col_link));
    CHECK_EQUAL(2, table1->get_backlink(1, table2_ndx, col_link, 0));

    // re-add links
    table2->set_link(col_link, 0, 1);
    table2->set_link(col_link, 1, 1);

    // remove a row
    table2->move_last_over(0);
    CHECK_EQUAL(2, table1->get_backlink_count(1, table2_ndx, col_link));
    CHECK_EQUAL(0, table1->get_backlink(1, table2_ndx, col_link, 0));
    CHECK_EQUAL(1, table1->get_backlink(1, table2_ndx, col_link, 1));

    // add some more links and see that they it gets nullified when the target
    // is removed
    table2->insert_link(col_link, 2, 2);
    table2->insert_done();
    table2->insert_link(col_link, 3, 2);
    table2->insert_done();
    CHECK_EQUAL(2, table1->get_backlink_count(2, table2_ndx, col_link));

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


#endif // TEST_GROUP
