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

    BasicTableView<TestTableInt> v; // Test empty construction
    v = table.cols().first.find_all(2); // Test assignment

    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2, v[0].first);
    CHECK_EQUAL(2, v[1].first);

    // Test of Set
    v[0].first = 123;
    CHECK_EQUAL(123, v[0].first);
}


TEST(TableViewSum)
{
    TestTableInt table;

    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.cols().first.sum();
    CHECK_EQUAL(10, sum);
}

TEST(TableViewSumNegative)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = 11;
    v[2].first = -20;

    int64_t sum = v.cols().first.sum();
    CHECK_EQUAL(-9, sum);
}

TEST(TableViewMax)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t max = v.cols().first.maximum();
    CHECK_EQUAL(2, max);
}



TEST(TableViewMax2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t max = v.cols().first.maximum();
    CHECK_EQUAL(-1, max);
}


TEST(TableViewMin)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t min = v.cols().first.minimum();
    CHECK_EQUAL(-1, min);
}


TEST(TableViewMin2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t min = v.cols().first.minimum();
    CHECK_EQUAL(-3, min);
}



TEST(TableViewFind)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4;
    v[2].first = 4;

    size_t r = v.cols().first.find_first(4);
    CHECK_EQUAL(1, r);
}


TEST(TableViewFindAll)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4; // match
    v[2].first = 4; // match

    // todo, add creation to wrapper function in table.h
    BasicTableView<TestTableInt> v2 = v.cols().first.find_all(4);
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}

TIGHTDB_TABLE_1(TestTableString,
                first, String)

TEST(TableViewFindAllString)
{
    TestTableString table;

    table.add("a");
    table.add("a");
    table.add("a");

    BasicTableView<TestTableString> v = table.cols().first.find_all("a");
    v[0].first = "foo";
    v[1].first = "bar"; // match
    v[2].first = "bar"; // match

    // todo, add creation to wrapper function in table.h
    BasicTableView<TestTableString> v2 = v.cols().first.find_all("bar");
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}

TEST(TableViewDelete)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(1);
    table.add(3);
    table.add(1);

    BasicTableView<TestTableInt> v = table.cols().first.find_all(1);
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

    BasicTableView<TestTableInt> v = table.cols().first.find_all(1);
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

    BasicTableView<TestTableInt> v = table.cols().first.find_all(1);
    CHECK_EQUAL(0, v.size());

    v.clear();
}
