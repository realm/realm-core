#include <UnitTest++.h>
#include <tightdb/table_macros.hpp>

using namespace tightdb;

namespace {
TIGHTDB_TABLE_1(TestTableInt,
                first, Int)

TIGHTDB_TABLE_2(TestTableDate,
                first, Date,
                second, Int)

}

TEST(TableViewDateMaxMin)
{
    TestTableDate ttd;

    ttd.add(Date(2014, 7, 10), 1);
    ttd.add(Date(2013, 7, 10), 1);
    ttd.add(Date(2015, 8, 10), 1);
    ttd.add(Date(2015, 7, 10), 1);

    TestTableDate::View v = ttd.column().second.find_all(1);

    CHECK_EQUAL(Date(2015, 8, 10), v.column().first.maximum());
    CHECK_EQUAL(Date(2013, 7, 10), v.column().first.minimum());
}

TEST(GetSetInteger)
{
    TestTableInt table;

    table.add(1);
    table.add(2);
    table.add(3);
    table.add(1);
    table.add(2);

    TestTableInt::View v; // Test empty construction
    v = table.column().first.find_all(2); // Test assignment

    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2, v[0].first);
    CHECK_EQUAL(2, v[1].first);

    // Test of Set
    v[0].first = 123;
    CHECK_EQUAL(123, v[0].first);
}



namespace {
TIGHTDB_TABLE_3(TableFloats,
                col_float, Float,
                col_double, Double,
                col_int, Int)
}

TEST(TableView_Floats_GetSet)
{
    TableFloats table;

    float  f_val[] = { 1.1f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f };
    double d_val[] = { 1.2 , 2.2 , 3.2 , -1.2 , 2.3, 0.0  };

    CHECK_EQUAL(true, table.is_empty());

    // Test add(?,?) with parameters
    for (size_t i=0; i<5; ++i)
        table.add(f_val[i], d_val[i], i);
    table.add();
    CHECK_EQUAL(6, table.size());
    for (size_t i=0; i<6; ++i) {
        CHECK_EQUAL(f_val[i], table.column().col_float[i]);
        CHECK_EQUAL(d_val[i], table.column().col_double[i]);
    }

    TableFloats::View v; // Test empty construction
    v = table.column().col_float.find_all(2.1f); // Test assignment
    CHECK_EQUAL(2, v.size());

    // Test of Get
    CHECK_EQUAL(2.1f, v[0].col_float);
    CHECK_EQUAL(2.1f, v[1].col_float);
    CHECK_EQUAL(2.2, v[0].col_double);
    CHECK_EQUAL(2.3, v[1].col_double);

    // Test of Set
    v[0].col_float = 123.321f;
    CHECK_EQUAL(123.321f, v[0].col_float);
    v[0].col_double = 123.3219;
    CHECK_EQUAL(123.3219, v[0].col_double);
}

TEST(TableView_Floats_Find_and_Aggregations)
{
    TableFloats table;
    float  f_val[] = { 1.2f, 2.1f, 3.1f, -1.1f, 2.1f, 0.0f };
    double d_val[] = { -1.2, 2.2 , 3.2 ,-1.2 , 2.3 , 0.0  };
    double sum_f = 0.0;
    double sum_d = 0.0;
    for (size_t i=0; i<6; ++i) {
        table.add(f_val[i], d_val[i], 1);
        sum_d += d_val[i];
        sum_f += double(f_val[i]);
    }

    // Test find_all()
    TableFloats::View v_all = table.column().col_int.find_all(1);
    CHECK_EQUAL(6, v_all.size());

    TableFloats::View v_some = table.column().col_double.find_all(-1.2);
    CHECK_EQUAL(2, v_some.size());
    CHECK_EQUAL(0, v_some.get_source_ndx(0));
    CHECK_EQUAL(3, v_some.get_source_ndx(1));

    // Test find_first
    CHECK_EQUAL(0, v_all.column().col_double.find_first(-1.2) );
    CHECK_EQUAL(5, v_all.column().col_double.find_first(0.0) );
    CHECK_EQUAL(2, v_all.column().col_double.find_first(3.2) );

    CHECK_EQUAL(1, v_all.column().col_float.find_first(2.1f) );
    CHECK_EQUAL(5, v_all.column().col_float.find_first(0.0f) );
    CHECK_EQUAL(2, v_all.column().col_float.find_first(3.1f) );

    // TODO: add for float as well

    // Test sum
    CHECK_EQUAL(sum_d, v_all.column().col_double.sum());
    CHECK_EQUAL(sum_f, v_all.column().col_float.sum());
    CHECK_EQUAL(-1.2 -1.2, v_some.column().col_double.sum());
    CHECK_EQUAL(1.2f -1.1f, v_some.column().col_float.sum());

    // Test max
    CHECK_EQUAL(3.2, v_all.column().col_double.maximum());
    CHECK_EQUAL(-1.2, v_some.column().col_double.maximum());
    CHECK_EQUAL(3.1f, v_all.column().col_float.maximum());
    CHECK_EQUAL(1.2f, v_some.column().col_float.maximum());

    // Test min
    CHECK_EQUAL(-1.2, v_all.column().col_double.minimum());
    CHECK_EQUAL(-1.2, v_some.column().col_double.minimum());
    CHECK_EQUAL(-1.1f, v_all.column().col_float.minimum());
    CHECK_EQUAL(-1.1f, v_some.column().col_float.minimum());

    // Test avg
    CHECK_EQUAL(sum_d / 6.0, v_all.column().col_double.average());
    CHECK_EQUAL((-1.2 + -1.2) / 2.0, v_some.column().col_double.average());
    CHECK_EQUAL(sum_f / 6.0, v_all.column().col_float.average());

    // Need to test this way because items summed one at a time differ with compile-time constant by some infinitesimal
    CHECK_EQUAL(int(0.05 * 1000), int(v_some.column().col_float.average() * 1000));

    CHECK_EQUAL(1, v_some.column().col_float.count(1.2f));
    CHECK_EQUAL(2, v_some.column().col_double.count(-1.2));
    CHECK_EQUAL(2, v_some.column().col_int.count(1));

    CHECK_EQUAL(2, v_all.column().col_float.count(2.1f));
    CHECK_EQUAL(2, v_all.column().col_double.count(-1.2));
    CHECK_EQUAL(6, v_all.column().col_int.count(1));
}

TEST(TableViewSum)
{
    TestTableInt table;

    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);
    table.add(2);

    TestTableInt::View v = table.column().first.find_all(2);
    CHECK_EQUAL(5, v.size());

    int64_t sum = v.column().first.sum();
    CHECK_EQUAL(10, sum);
}

TEST(TableViewSumNegative)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = 11;
    v[2].first = -20;

    int64_t sum = v.column().first.sum();
    CHECK_EQUAL(-9, sum);
}

TEST(TableViewMax)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t max = v.column().first.maximum();
    CHECK_EQUAL(2, max);
}

TEST(TableViewMax2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t max = v.column().first.maximum();
    CHECK_EQUAL(-1, max);
}


TEST(TableViewMin)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first =  2;
    v[2].first =  1;

    int64_t min = v.column().first.minimum();
    CHECK_EQUAL(-1, min);
}

TEST(TableViewMin2)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = -1;
    v[1].first = -2;
    v[2].first = -3;

    int64_t min = v.column().first.minimum();
    CHECK_EQUAL(-3, min);
}


TEST(TableViewFind)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4;
    v[2].first = 4;

    size_t r = v.column().first.find_first(4);
    CHECK_EQUAL(1, r);
}


TEST(TableViewFindAll)
{
    TestTableInt table;

    table.add(0);
    table.add(0);
    table.add(0);

    TestTableInt::View v = table.column().first.find_all(0);
    v[0].first = 5;
    v[1].first = 4; // match
    v[2].first = 4; // match

    // todo, add creation to wrapper function in table.h
    TestTableInt::View v2 = v.column().first.find_all(4);
    CHECK_EQUAL(1, v2.get_source_ndx(0));
    CHECK_EQUAL(2, v2.get_source_ndx(1));
}

namespace {
TIGHTDB_TABLE_1(TestTableString,
                first, String)
}

TEST(TableViewFindAllString)
{
    TestTableString table;

    table.add("a");
    table.add("a");
    table.add("a");

    TestTableString::View v = table.column().first.find_all("a");
    v[0].first = "foo";
    v[1].first = "bar"; // match
    v[2].first = "bar"; // match

    // todo, add creation to wrapper function in table.h
    TestTableString::View v2 = v.column().first.find_all("bar");
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

    TestTableInt::View v = table.column().first.find_all(1);
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

    TestTableInt::View v = table.column().first.find_all(1);
    CHECK_EQUAL(3, v.size());

    v.clear();
    CHECK_EQUAL(0, v.size());

    CHECK_EQUAL(2, table.size());
    CHECK_EQUAL(2, table[0].first);
    CHECK_EQUAL(3, table[1].first);
}

//exposes a bug in stacked tableview:
//view V1 selects a subset of rows from Table T1
//View V2 selects rows from  view V1
//Then, some rows in V2 can be found, that are not in V1
TEST(TableViewStacked) {
 
    Table t;
    t.add_column(type_Int,"i1");
    t.add_column(type_Int,"i2");
    t.add_column(type_String,"S1");
    t.add_empty_row(2);
    t.set_int(0,0,1);    t.set_int(1,0,2); t.set_string(2,0,"A");    //   1 2   "A"
    t.set_int(0,1,2);    t.set_int(1,1,2); t.set_string(2,1,"B");    //   2 2   "B"

    TableView tv = t.find_all_int(0,2);
    TableView tv2 = tv.find_all_int(1,2);
    CHECK_EQUAL(1,tv2.size()); //evaluates tv2.size to 1 which is expected
    CHECK_EQUAL("B",tv2.get_string(2,0)); //evalates get_string(2,0) to "A" which is not expected
}

TEST(TableViewClearNone)
{
    TestTableInt table;

    TestTableInt::View v = table.column().first.find_all(1);
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
    MyTable3::View v = t.column().val.find_all(0);
    MyTable3::ConstView cv = ct.column().val.find_all(0);

    {
        MyTable3::View v2 = v.column().val.find_all(0);
        MyTable3::ConstView cv2 = cv.column().val.find_all(0);

        MyTable3::ConstView cv3 = t.column().val.find_all(0);
        MyTable3::ConstView cv4 = v.column().val.find_all(0);

        // Also test assigment that converts to const
        cv3 = t.column().val.find_all(0);
        cv4 = v.column().val.find_all(0);

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
        MyTable2::Ref       s5 = v.column().subtab[0];
        MyTable2::ConstRef  s6 = v.column().subtab[0];
        MyTable2::Ref       s7 = v.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef  s8 = v.column().subtab[0]->get_table_ref();
        MyTable2::ConstRef cs1 = cv[0].subtab;
        MyTable2::ConstRef cs2 = cv[0].subtab->get_table_ref();
        MyTable2::ConstRef cs3 = cv.column().subtab[0];
        MyTable2::ConstRef cs4 = cv.column().subtab[0]->get_table_ref();
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
        MyTable1::Ref       s5 = v.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef  s6 = v.column().subtab[0]->column().subtab[0];
        MyTable1::Ref       s7 = v.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef  s8 = v.column().subtab[0]->column().subtab[0]->get_table_ref();
        MyTable1::ConstRef cs1 = cv[0].subtab[0].subtab;
        MyTable1::ConstRef cs2 = cv[0].subtab[0].subtab->get_table_ref();
        MyTable1::ConstRef cs3 = cv.column().subtab[0]->column().subtab[0];
        MyTable1::ConstRef cs4 = cv.column().subtab[0]->column().subtab[0]->get_table_ref();
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
    CHECK_EQUAL(v[0].subtab[0].val,                     1);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  1);
    CHECK_EQUAL(v[0].subtab->column().val[0],           1);
    CHECK_EQUAL(v.column().subtab[0][0].val,            1);

    v.column().subtab[0]->column().val[0] = 2;
    CHECK_EQUAL(v[0].subtab[0].val,                     2);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  2);
    CHECK_EQUAL(v[0].subtab->column().val[0],           2);
    CHECK_EQUAL(v.column().subtab[0][0].val,            2);

    v[0].subtab->column().val[0] = 3;
    CHECK_EQUAL(v[0].subtab[0].val,                     3);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  3);
    CHECK_EQUAL(v[0].subtab->column().val[0],           3);
    CHECK_EQUAL(v.column().subtab[0][0].val,            3);

    v.column().subtab[0][0].val = 4;
    CHECK_EQUAL(v[0].subtab[0].val,                     4);
    CHECK_EQUAL(v.column().subtab[0]->column().val[0],  4);
    CHECK_EQUAL(v[0].subtab->column().val[0],           4);
    CHECK_EQUAL(v.column().subtab[0][0].val,            4);
    CHECK_EQUAL(cv[0].subtab[0].val,                    4);
    CHECK_EQUAL(cv.column().subtab[0]->column().val[0], 4);
    CHECK_EQUAL(cv[0].subtab->column().val[0],          4);
    CHECK_EQUAL(cv.column().subtab[0][0].val,           4);

    v[0].subtab[0].subtab->add();
    v[0].subtab[0].subtab[0].val = 5;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                               5);
    CHECK_EQUAL(v.column().subtab[0]->column().subtab[0]->column().val[0],  5);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                              5);
    CHECK_EQUAL(cv.column().subtab[0]->column().subtab[0]->column().val[0], 5);

    v.column().subtab[0]->column().subtab[0]->column().val[0] = 6;
    CHECK_EQUAL(v[0].subtab[0].subtab[0].val,                               6);
    CHECK_EQUAL(v.column().subtab[0]->column().subtab[0]->column().val[0],  6);
    CHECK_EQUAL(cv[0].subtab[0].subtab[0].val,                              6);
    CHECK_EQUAL(cv.column().subtab[0]->column().subtab[0]->column().val[0], 6);
}
