#include "tightdb.hpp"
#include <UnitTest++.h>

using namespace tightdb;

TIGHTDB_TABLE_1(TestTableInt,
                first, Int)


TEST(GetSetInteger)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(3);
    table.add(1);
    table.add(2);

    TableView v = table.cols().first.find_all_int(2);

    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2, v.get_int(0, 0));
    CHECK_EQUAL(2, v.get_int(0, 1));

    // Test of Set
    v.set_int(0, 0, 123);
    CHECK_EQUAL(123, v.get_int(0, 0));

    //v.Destroy();
}


TEST(TableViewSum)
{
    TestTableInt table;

    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);

    TableView v = table.cols().first.find_all_int(2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.sum(0);
    CHECK_EQUAL(10, sum);

    //v.Destroy();
}

TEST(TableViewSumNegative)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, 11);
    v.set_int(0, 2, -20);

    int64_t sum = v.sum(0);
    CHECK_EQUAL(-9, sum);

    //v.Destroy();
}

TEST(TableViewMax)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, -1);
    v.set_int(0, 1, 2);
    v.set_int(0, 2, 1);

    int64_t max = v.maximum(0);
    CHECK_EQUAL(2, max);
    //v.Destroy();
}



TEST(TableViewMax2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, -1);
    v.set_int(0, 1, -2);
    v.set_int(0, 2, -3);

    int64_t max = v.maximum(0);
    CHECK_EQUAL(-1, max);
    //v.Destroy();
}


TEST(TableViewMin)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, -1);
    v.set_int(0, 1, 2);
    v.set_int(0, 2, 1);

    int64_t min = v.minimum(0);
    CHECK_EQUAL(-1, min);
    //v.Destroy();
}


TEST(TableViewMin2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, -1);
    v.set_int(0, 1, -2);
    v.set_int(0, 2, -3);

    int64_t min = v.minimum(0);
    CHECK_EQUAL(-3, min);
    //v.Destroy();
}



TEST(TableViewFind)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, 5);
    v.set_int(0, 1, 4);
    v.set_int(0, 2, 4);

    size_t r = v.find_first_int(0, 4);
    CHECK_EQUAL(1, r);
    //v.Destroy();
}


TEST(TableViewFindAll)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TableView v = table.cols().first.find_all_int(0);
    v.set_int(0, 0, 5);
    v.set_int(0, 1, 4); // match
    v.set_int(0, 2, 4); // match

    // todo, add creation to wrapper function in table.h
    TableView *v2 = new TableView(*v.get_table());
    v.find_all_int(*v2, 0, 4);
    CHECK_EQUAL(1, v2->get_source_ndx(0));
    CHECK_EQUAL(2, v2->get_source_ndx(1));
    //v.Destroy();
    delete v2;
}

TIGHTDB_TABLE_1(TestTableString,
                first, String)

TEST(TableViewFindAllString)
{
    TestTableString table;

    table.add("a");
    table.add("a");
    table.add("a");

    TableView v = table.cols().first.find_all_int("a");
    v.set_string(0, 0, "foo");
    v.set_string(0, 1, "bar"); // match
    v.set_string(0, 2, "bar"); // match

    // todo, add creation to wrapper function in table.h
    TableView *v2 = new TableView(*v.get_table());
    v.find_all_string(*v2, 0, "bar");
    CHECK_EQUAL(1, v2->get_source_ndx(0));
    CHECK_EQUAL(2, v2->get_source_ndx(1));
    //v.Destroy();
    delete v2;
}

TEST(TableViewDelete)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(1);
    table.add(3);
    table.add(1);

    TableView v = table.cols().first.find_all_int(1);
    CHECK_EQUAL(3, v.size());

    v.remove(1);
    CHECK_EQUAL(2, v.size());
    CHECK_EQUAL(0, v.get_source_ndx(0));
    CHECK_EQUAL(3, v.get_source_ndx(1));

    CHECK_EQUAL(4, table.size());
    CHECK_EQUAL(1, table[0].first);
    CHECK_EQUAL(2, table[1].first);
    CHECK_EQUAL(3, table[2].first);
    CHECK_EQUAL(1, table[3].first);

    v.remove(0);
    CHECK_EQUAL(1, v.size());
    CHECK_EQUAL(2, v.get_source_ndx(0));

    CHECK_EQUAL(3, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
    CHECK_EQUAL(1, table[2].first);

    v.remove(0);
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
}

TEST(TableViewClear)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(1);
    table.add(3);
    table.add(1);

    TableView v = table.cols().first.find_all_int(1);
    CHECK_EQUAL(3, v.size());

    v.clear();
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
}


TEST(TableViewClearNone)
{
    TestTableInt table;

    TableView v = table.cols().first.find_all_int(1);
    CHECK_EQUAL(0, v.size());

    v.clear();

}
