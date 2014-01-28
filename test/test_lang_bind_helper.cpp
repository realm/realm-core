#include "testsettings.hpp"
#ifdef TEST_LANG_BIND_HELPER

#include <UnitTest++.h>

#include <tightdb/lang_bind_helper.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

TEST(InsertSubtable)
{
    Table t;
    t.add_column(type_Table, "sub");
    t.add_subcolumn(tuple(0), type_Int, "i1");
    t.add_subcolumn(tuple(0), type_Int, "i2");

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    LangBindHelper::insert_subtable(t, 0, 0, t2);
    t.insert_done();

    TableRef sub = t.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}


// FIXME: Move this test to test_table.cpp
TEST(SetSubtable)
{
    Table t;
    t.add_column(type_Table, "sub");
    t.add_subcolumn(tuple(0), type_Int, "i1");
    t.add_subcolumn(tuple(0), type_Int, "i2");
    t.add_empty_row();

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    t.set_subtable( 0, 0, &t2);

    TableRef sub = t.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}

#endif
