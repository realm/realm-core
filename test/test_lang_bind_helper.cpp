#include "testsettings.hpp"
#ifdef TEST_LANG_BIND_HELPER

#include <tightdb/lang_bind_helper.hpp>
#include <tightdb/descriptor.hpp>

#include "util/unit_test.hpp"
#include "util/test_only.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

TEST(LangBindHelper_InsertSubtable)
{
    Table t1;
    DescriptorRef s;
    t1.add_column(type_Table, "sub", &s);
    s->add_column(type_Int, "i1");
    s->add_column(type_Int, "i2");
    s.reset();

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    LangBindHelper::insert_subtable(t1, 0, 0, t2);
    t1.insert_done();

    TableRef sub = t1.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}


// FIXME: Move this test to test_table.cpp
TEST(LangBindHelper_SetSubtable)
{
    Table t1;
    DescriptorRef s;
    t1.add_column(type_Table, "sub", &s);
    s->add_column(type_Int, "i1");
    s->add_column(type_Int, "i2");
    s.reset();
    t1.add_empty_row();

    Table t2;
    t2.add_column(type_Int, "i1");
    t2.add_column(type_Int, "i2");
    t2.insert_int(0, 0, 10);
    t2.insert_int(1, 0, 120);
    t2.insert_done();
    t2.insert_int(0, 1, 12);
    t2.insert_int(1, 1, 100);
    t2.insert_done();

    t1.set_subtable( 0, 0, &t2);

    TableRef sub = t1.get_subtable(0, 0);

    CHECK_EQUAL(t2.get_column_count(), sub->get_column_count());
    CHECK_EQUAL(t2.size(), sub->size());
    CHECK(t2 == *sub);
}

#endif
