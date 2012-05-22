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

    TestTableInt::View v; // Test empty construction
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

    TestTableInt::View v = table.cols().first.find_all(2);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
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

    TestTableInt::View v = table.cols().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4; // match
    v[2].first = 4; // match

    // todo, add creation to wrapper function in table.h
    TestTableInt::View v2 = v.cols().first.find_all(4);
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

    TestTableString::View v = table.cols().first.find_all("a");
    v[0].first = "foo";
    v[1].first = "bar"; // match
    v[2].first = "bar"; // match

    // todo, add creation to wrapper function in table.h
    TestTableString::View v2 = v.cols().first.find_all("bar");
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

    TestTableInt::View v = table.cols().first.find_all(1);
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

    TestTableInt::View v = table.cols().first.find_all(1);
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

    TestTableInt::View v = table.cols().first.find_all(1);
    CHECK_EQUAL(0, v.size());

    v.clear();
}



namespace
{
    TIGHTDB_TABLE_1(MyTable1,
                    val, Int)

    TIGHTDB_TABLE_2(MyTable2,
                    val, Int,
                    subtab, Subtable<MyTable1>)

    TIGHTDB_TABLE_2(MyTable3,
                    val, Int,
                    subtab, Subtable<MyTable2>)
}

TEST(TableView_HighLevelSubtables)
{
    MyTable3 t;
    const MyTable3& ct = t;

    t.add();
    MyTable3::View v = t.cols().val.find_all(0);
    MyTable3::ConstView cv = ct.cols().val.find_all(0);

    {
        MyTable3::View v2 = v.cols().val.find_all(0);
        MyTable3::ConstView cv2 = cv.cols().val.find_all(0);

        MyTable3::ConstView cv3 = t.cols().val.find_all(0);
        MyTable3::ConstView cv4 = v.cols().val.find_all(0);

        // Also test assigment that converts to const
        cv3 = t.cols().val.find_all(0);
        cv4 = v.cols().val.find_all(0);

        static_cast<void>(v2);
        static_cast<void>(cv2);
        static_cast<void>(cv3);
        static_cast<void>(cv4);
    }

    {
        MyTable2::Ref       s1 = v[0].subtab;
        MyTable2::ConstRef  s2 = v[0].subtab;
        MyTable2::Ref       s3 = v[0].subtab->get_table_ref();
        MyTable2::ConstRef  s4 = v[0].subtab->get_table_ref();
        MyTable2::Ref       s5 = v.cols().subtab[0];
        MyTable2::ConstRef  s6 = v.cols().subtab[0];
        MyTable2::Ref       s7 = v.cols().subtab[0]->get_table_ref();
        MyTable2::ConstRef  s8 = v.cols().subtab[0]->get_table_ref();
        MyTable2::ConstRef cs1 = cv[0].subtab;
        MyTable2::ConstRef cs2 = cv[0].subtab->get_table_ref();
        MyTable2::ConstRef cs3 = cv.cols().subtab[0];
        MyTable2::ConstRef cs4 = cv.cols().subtab[0]->get_table_ref();
        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(s5);
        static_cast<void>(s6);
        static_cast<void>(s7);
        static_cast<void>(s8);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
        static_cast<void>(cs3);
        static_cast<void>(cs4);
    }

    t[0].subtab->add();
    {
        MyTable1::Ref       s1 = v[0].subtab[0].subtab;
        MyTable1::ConstRef  s2 = v[0].subtab[0].subtab;
        MyTable1::Ref       s3 = v[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef  s4 = v[0].subtab[0].subtab->get_table_ref();
        MyTable1::Ref       s5 = v.cols().subtab[0]->cols().subtab[0];
        MyTable1::ConstRef  s6 = v.cols().subtab[0]->cols().subtab[0];
        MyTable1::Ref       s7 = v.cols().subtab[0]->cols().subtab[0]->get_table_ref();
        MyTable1::ConstRef  s8 = v.cols().subtab[0]->cols().subtab[0]->get_table_ref();
        MyTable1::ConstRef cs1 = cv[0].subtab[0].subtab;
        MyTable1::ConstRef cs2 = cv[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef cs3 = cv.cols().subtab[0]->cols().subtab[0];
        MyTable1::ConstRef cs4 = cv.cols().subtab[0]->cols().subtab[0]->get_table_ref();
        static_cast<void>(s1);
        static_cast<void>(s2);
        static_cast<void>(s3);
        static_cast<void>(s4);
        static_cast<void>(s5);
        static_cast<void>(s6);
        static_cast<void>(s7);
        static_cast<void>(s8);
        static_cast<void>(cs1);
        static_cast<void>(cs2);
        static_cast<void>(cs3);
        static_cast<void>(cs4);
    }

    v[0].subtab[0].val = 1;
    CHECK_EQUAL(v[0].subtab[0].val,                 1);
    CHECK_EQUAL(v.cols().subtab[0]->cols().val[0],  1);
    CHECK_EQUAL(v[0].subtab->cols().val[0],         1);
    CHECK_EQUAL(v.cols().subtab[0][0].val,          1);

    v.cols().subtab[0]->cols().val[0] = 2;
    CHECK_EQUAL(v[0].subtab[0].val,                 2);
    CHECK_EQUAL(v.cols().subtab[0]->cols().val[0],  2);
    CHECK_EQUAL(v[0].subtab->cols().val[0],         2);
    CHECK_EQUAL(v.cols().subtab[0][0].val,          2);

    v[0].subtab->cols().val[0] = 3;
    CHECK_EQUAL(v[0].subtab[0].val,                 3);
    CHECK_EQUAL(v.cols().subtab[0]->cols().val[0],  3);
    CHECK_EQUAL(v[0].subtab->cols().val[0],         3);
    CHECK_EQUAL(v.cols().subtab[0][0].val,          3);

    v.cols().subtab[0][0].val = 4;
    CHECK_EQUAL(v[0].subtab[0].val,                 4);
    CHECK_EQUAL(v.cols().subtab[0]->cols().val[0],  4);
    CHECK_EQUAL(v[0].subtab->cols().val[0],         4);
    CHECK_EQUAL(v.cols().subtab[0][0].val,          4);
    CHECK_EQUAL(cv[0].subtab[0].val,                4);
    CHECK_EQUAL(cv.cols().subtab[0]->cols().val[0], 4);
    CHECK_EQUAL(cv[0].subtab->cols().val[0],        4);
    CHECK_EQUAL(cv.cols().subtab[0][0].val,         4);

    v[0].subtab[0].subtab->add();
    v[0].subtab[0].subtab[0].val = 5;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                         5);
    CHECK_EQUAL(v.cols().subtab[0]->cols().subtab[0]->cols().val[0],  5);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                        5);
    CHECK_EQUAL(cv.cols().subtab[0]->cols().subtab[0]->cols().val[0], 5);

    v.cols().subtab[0]->cols().subtab[0]->cols().val[0] = 6;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                         6);
    CHECK_EQUAL(v.cols().subtab[0]->cols().subtab[0]->cols().val[0],  6);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                        6);
    CHECK_EQUAL(cv.cols().subtab[0]->cols().subtab[0]->cols().val[0], 6);
}
