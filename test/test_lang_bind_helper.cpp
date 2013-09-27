#ifdef TEST_LANG_BIND_HELPER

#include <UnitTest++.h>

#include <tightdb/lang_bind_helper.hpp>

using namespace std;
using namespace tightdb;


TEST(InsertSubtable)
{
    Table t;
    Spec& spec = t.get_spec();
    Spec subspec = spec.add_subtable_column("sub");
    subspec.add_column(type_Int, "i1");
    subspec.add_column(type_Int, "i2");
    t.update_from_spec();

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
    Spec& spec = t.get_spec();
    Spec subspec = spec.add_subtable_column("sub");
    subspec.add_column(type_Int, "i1");
    subspec.add_column(type_Int, "i2");
    t.update_from_spec();
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