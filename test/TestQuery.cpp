#include <UnitTest++.h>
#include <tightdb.hpp>

using namespace tightdb;

namespace {

TIGHTDB_TABLE_2(TwoIntTable,
                first,  Int,
                second, Int)

TIGHTDB_TABLE_1(OneIntTable,
                first,  Int)

TIGHTDB_TABLE_2(TupleTableType,
                first,  Int,
                second, String)

TIGHTDB_TABLE_2(BoolTupleTable,
                first,  Int,
                second, Bool)

TIGHTDB_TABLE_5(PeopleTable,
                name,  String,
                age,   Int,
                male,  Bool,
                hired, Date,
                photo, Binary)

} // anonymous namespace

TEST(TestDateQuery)
{
    PeopleTable table;

    table.add("Mary",  28, false, tightdb::Date(2012,  1, 24), tightdb::BinaryData("bin \0\n data 1", 13));
    table.add("Frank", 56, true,  tightdb::Date(2008,  4, 15), tightdb::BinaryData("bin \0\n data 2", 13));
    table.add("Bob",   24, true,  tightdb::Date(2010, 12,  1), tightdb::BinaryData("bin \0\n data 3", 13));

    // Find people where hired year == 2012 (hour:minute:second is default initialized to 00:00:00)
    PeopleTable::View view5 = table.where().hired.greater_equal(tightdb::Date(2012, 1, 1).get_date())
                                           .hired.less(         tightdb::Date(2013, 1, 1).get_date()).find_all(); 

    assert(view5.size() == 1 && view5[0].name == "Mary");

}


TEST(TestQueryStrIndexed_enum)
{
    TupleTableType ttt;

    for(size_t t = 0; t < 10; t++) {
        ttt.add(1, "a");
        ttt.add(4, "b");
        ttt.add(7, "c");
        ttt.add(10, "a");
        ttt.add(1, "b");
        ttt.add(4, "c");
    }

    ttt.optimize();

    ttt.column().second.set_index();

    int64_t s = ttt.where().second.equal("a").first.sum();
    CHECK_EQUAL(10*11, s);

    s = ttt.where().second.equal("a").first.equal(10).first.sum();
    CHECK_EQUAL(100, s);

    s = ttt.where().first.equal(10).second.equal("a").first.sum();
    CHECK_EQUAL(100, s);

    TupleTableType::View tv = ttt.where().second.equal("a").find_all();
    CHECK_EQUAL(10*2, tv.size());
}


TEST(TestQueryStrIndexed_non_enum)
{
    TupleTableType ttt;

    for(size_t t = 0; t < 10; t++) {
        ttt.add(1, "a");
        ttt.add(4, "b");
        ttt.add(7, "c");
        ttt.add(10, "a");
        ttt.add(1, "b");
        ttt.add(4, "c");
    }

    ttt.column().second.set_index();

    int64_t s = ttt.where().second.equal("a").first.sum();
    CHECK_EQUAL(10*11, s);

    s = ttt.where().second.equal("a").first.equal(10).first.sum();
    CHECK_EQUAL(100, s);

    s = ttt.where().first.equal(10).second.equal("a").first.sum();
    CHECK_EQUAL(100, s);

    TupleTableType::View tv = ttt.where().second.equal("a").find_all();
    CHECK_EQUAL(10*2, tv.size());
}


TEST(TestQueryFindAll_Contains2_2)
{
    TupleTableType ttt;

    ttt.add(0, "foo");
    ttt.add(1, "foobar");
    ttt.add(2, "hellofoobar");
    ttt.add(3, "foO");
    ttt.add(4, "foObar");
    ttt.add(5, "hellofoObar");
    ttt.add(6, "hellofo");
    ttt.add(7, "fobar");
    ttt.add(8, "oobar");

// utf8 case handling is only implemented on msw for now
#if defined(_MSC_VER)
    TupleTableType::Query q1 = ttt.where().second.contains("foO", false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(6, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
    CHECK_EQUAL(4, tv1.get_source_ndx(4));
    CHECK_EQUAL(5, tv1.get_source_ndx(5));
    TupleTableType::Query q2 = ttt.where().second.contains("foO", true);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(3, tv2.size());
    CHECK_EQUAL(3, tv2.get_source_ndx(0));
    CHECK_EQUAL(4, tv2.get_source_ndx(1));
    CHECK_EQUAL(5, tv2.get_source_ndx(2));
#endif
}

TEST(TestQuery_sum_new_aggregates)
{
    // test the new ACTION_FIND_PATTERN() method in array

    OneIntTable t;
    for (size_t i = 0; i < 1000; i++) {
        t.add(1);
        t.add(2);
        t.add(4);
        t.add(6);
    }
    size_t c = t.where().first.equal(2).count();
    CHECK_EQUAL(1000, c);

    c = t.where().first.greater(2).count();
    CHECK_EQUAL(2000, c);

}

TEST(TestQuery_sum_min_max_avg_foreign_col)
{
    TwoIntTable t;
    t.add(1, 10);
    t.add(2, 20);
    t.add(2, 30);
    t.add(3, 40);

    CHECK_EQUAL(50, t.where().first.equal(2).second.sum());
}


TEST(TestAggregateSingleCond)
{
    OneIntTable ttt;

    ttt.add(1);
    ttt.add(2);
    ttt.add(2);
    ttt.add(3);
    ttt.add(3);
    ttt.add(4);

    int64_t s = ttt.where().first.equal(2).first.sum();
    CHECK_EQUAL(4, s);

    s = ttt.where().first.greater(2).first.sum();
    CHECK_EQUAL(10, s);

    s = ttt.where().first.less(3).first.sum();
    CHECK_EQUAL(5, s);

    s = ttt.where().first.not_equal(3).first.sum();
    CHECK_EQUAL(9, s);


}

TEST(TestQueryFindAll_range1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");
    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");
    ttt.add(1, "a");
    ttt.add(4, "a");
    ttt.add(7, "a");
    ttt.add(10, "a");

    TupleTableType::Query q1 = ttt.where().second.equal("a");
    TupleTableType::View tv1 = q1.find_all(4, 10);
    CHECK_EQUAL(6, tv1.size());
}


TEST(TestQueryFindAll_range_or_monkey2)
{
    const size_t ROWS = 20;
    const size_t ITER = 100;

    for (size_t u = 0; u < ITER; u++)
    {
        TwoIntTable tit;
        Array a;
        size_t start = rand() % (ROWS + 1);
        size_t end = start + rand() % (ROWS + 1);

        if (end > ROWS)
            end = ROWS;

        for (size_t t = 0; t < ROWS; t++) {
            int64_t r1 = rand() % 10;
            int64_t r2 = rand() % 10;
            tit.add(r1, r2);
        }

        TwoIntTable::Query q1 = tit.where().group().first.equal(3).Or().first.equal(7).end_group().second.greater(5);
        TwoIntTable::View tv1 = q1.find_all(start, end);

        for (size_t t = start; t < end; t++) {
            if ((tit[t].first == 3 || tit[t].first == 7) && tit[t].second > 5) {
                a.add(t);
            }
        }
        size_t s1 = a.Size();
        size_t s2 = tv1.size();

        CHECK_EQUAL(s1, s2);
        for (size_t t = 0; t < a.Size(); t++) {
            size_t i1 = a.GetAsSizeT(t);
            size_t i2 = tv1.get_source_ndx(t);
            CHECK_EQUAL(i1, i2);
        }
        a.Destroy();
    }

}



TEST(TestQueryFindAll_range_or)
{
    TupleTableType ttt;

    ttt.add(1, "b");
    ttt.add(2, "a"); //// match
    ttt.add(3, "b"); //
    ttt.add(1, "a"); //// match
    ttt.add(2, "b"); //// match
    ttt.add(3, "a");
    ttt.add(1, "b");
    ttt.add(2, "a"); //// match
    ttt.add(3, "b"); //

    TupleTableType::Query q1 = ttt.where().group().first.greater(1).Or().second.equal("a").end_group().first.less(3);
    TupleTableType::View tv1 = q1.find_all(1, 8);
    CHECK_EQUAL(4, tv1.size());

    TupleTableType::View tv2 = q1.find_all(2, 8);
    CHECK_EQUAL(3, tv2.size());

    TupleTableType::View tv3 = q1.find_all(1, 7);
    CHECK_EQUAL(3, tv3.size());
}


TEST(TestQueryDelete)
{
    TupleTableType ttt;

    ttt.add(1, "X");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "X");
    ttt.add(6, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove();

    CHECK_EQUAL(4, r);
    CHECK_EQUAL(2, ttt.size());
    CHECK_EQUAL(2, ttt[0].first);
    CHECK_EQUAL(4, ttt[1].first);
    
    // test remove of all
    ttt.clear();
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    TupleTableType::Query q2 = ttt.where().second.equal("X");
    r = q2.remove();
    CHECK_EQUAL(3, r);
    CHECK_EQUAL(0, ttt.size());
}

TEST(TestQueryDeleteRange)
{
    TupleTableType ttt;

    ttt.add(0, "X");
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    ttt.add(4, "X");
    ttt.add(5, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove(1, 4);

    CHECK_EQUAL(3, r);
    CHECK_EQUAL(3, ttt.size());
    CHECK_EQUAL(0, ttt[0].first);
    CHECK_EQUAL(4, ttt[1].first);
    CHECK_EQUAL(5, ttt[2].first);
}

TEST(TestQueryDeleteLimit)
{
    TupleTableType ttt;

    ttt.add(0, "X");
    ttt.add(1, "X");
    ttt.add(2, "X");
    ttt.add(3, "X");
    ttt.add(4, "X");
    ttt.add(5, "X");

    TupleTableType::Query q = ttt.where().second.equal("X");
    size_t r = q.remove(1, 4, 2);

    CHECK_EQUAL(2, r);
    CHECK_EQUAL(4, ttt.size());
    CHECK_EQUAL(0, ttt[0].first);
    CHECK_EQUAL(3, ttt[1].first);
    CHECK_EQUAL(4, ttt[2].first);
    CHECK_EQUAL(5, ttt[3].first);
}



TEST(TestQuerySimple)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);

    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}

TEST(TestQuerySimpleBUGdetect)
{
	TupleTableType ttt;
	ttt.add(1, "a");
	ttt.add(2, "a");

	TupleTableType::Query q1 = ttt.where();

	TupleTableType::View tv1 = q1.find_all();
	CHECK_EQUAL(2, tv1.size());
	CHECK_EQUAL(0, tv1.get_source_ndx(0));

	TupleTableType::View resView = tv1.column().second.find_all("Foo");

    // This previously crashed:
    // TableView resView = TableView(tv1);
    // tv1.find_all(resView, 1, "Foo");
}


TEST(TestQuerySubtable)
{
    Group group;
    TableRef table = group.get_table("test");

    // Create specification with sub-table
    Spec& s = table->get_spec();
    s.add_column(COLUMN_TYPE_INT,    "first");
    s.add_column(COLUMN_TYPE_STRING, "second");
    Spec sub = s.add_subtable_column("third");
        sub.add_column(COLUMN_TYPE_INT,    "sub_first");
        sub.add_column(COLUMN_TYPE_STRING, "sub_second");
    table->update_from_spec();

    CHECK_EQUAL(3, table->get_column_count());

    // Main table
    table->insert_int(0, 0, 111);
    table->insert_string(1, 0, "this");
    table->insert_subtable(2, 0);
    table->insert_done();

    table->insert_int(0, 1, 222);
    table->insert_string(1, 1, "is");
    table->insert_subtable(2, 1);
    table->insert_done();

    table->insert_int(0, 2, 333);
    table->insert_string(1, 2, "a test");
    table->insert_subtable(2, 2);
    table->insert_done();

    table->insert_int(0, 3, 444);
    table->insert_string(1, 3, "of queries");
    table->insert_subtable(2, 3);
    table->insert_done();


    // Sub tables
    TableRef subtable = table->get_subtable(2, 0);
    subtable->insert_int(0, 0, 11);
    subtable->insert_string(1, 0, "a");
    subtable->insert_done();

    subtable = table->get_subtable(2, 1);
    subtable->insert_int(0, 0, 22);
    subtable->insert_string(1, 0, "b");
    subtable->insert_done();
    subtable->insert_int(0, 1, 33);
    subtable->insert_string(1, 1, "c");
    subtable->insert_done();

    subtable = table->get_subtable(2, 2);
    subtable->insert_int(0, 0, 44);
    subtable->insert_string(1, 0, "d");
    subtable->insert_done();

    subtable = table->get_subtable(2, 3);
    subtable->insert_int(0, 0, 55);
    subtable->insert_string(1, 0, "e");
    subtable->insert_done();


    Query q1 = table->where();
    q1.greater(0, 200);
    q1.subtable(2);
    q1.less(0, 50);
    q1.end_subtable();
    TableView t1 = q1.find_all(0, (size_t)-1);
    CHECK_EQUAL(2, t1.size());
    CHECK_EQUAL(1, t1.get_source_ndx(0));
    CHECK_EQUAL(2, t1.get_source_ndx(1));


    Query q2 = table->where();
    q2.subtable(2);
    q2.greater(0, 50);
    q2.Or();
    q2.less(0, 20);
    q2.end_subtable();
    TableView t2 = q2.find_all(0, (size_t)-1);
    CHECK_EQUAL(2, t2.size());
    CHECK_EQUAL(0, t2.get_source_ndx(0));
    CHECK_EQUAL(3, t2.get_source_ndx(1));


    Query q3 = table->where();
    q3.subtable(2);
    q3.greater(0, 50);
    q3.Or();
    q3.less(0, 20);
    q3.end_subtable();
    q3.less(0, 300);
    TableView t3 = q3.find_all(0, (size_t)-1);
    CHECK_EQUAL(1, t3.size());
    CHECK_EQUAL(0, t3.get_source_ndx(0));


    Query q4 = table->where();
    q4.equal(0, (int64_t)333);
    q4.Or();
    q4.subtable(2);
    q4.greater(0, 50);
    q4.Or();
    q4.less(0, 20);
    q4.end_subtable();
    TableView t4 = q4.find_all(0, (size_t)-1);


    CHECK_EQUAL(3, t4.size());
    CHECK_EQUAL(0, t4.get_source_ndx(0));
    CHECK_EQUAL(2, t4.get_source_ndx(1));
    CHECK_EQUAL(3, t4.get_source_ndx(2));
}




TEST(TestQuerySort1)
{
    TupleTableType ttt;

    ttt.add(1, "a"); // 0
    ttt.add(2, "a"); // 1
    ttt.add(3, "X"); // 2
    ttt.add(1, "a"); // 3
    ttt.add(2, "a"); // 4
    ttt.add(3, "X"); // 5
    ttt.add(9, "a"); // 6
    ttt.add(8, "a"); // 7
    ttt.add(7, "X"); // 8

    // tv.get_source_ndx()  = 0, 2, 3, 5, 6, 7, 8
    // Vals         = 1, 3, 1, 3, 9, 8, 7
    // result       = 3, 0, 5, 2, 8, 7, 6

    TupleTableType::Query q = ttt.where().first.not_equal(2);
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 7);
    CHECK(tv[0].first == 1);
    CHECK(tv[1].first == 1);
    CHECK(tv[2].first == 3);
    CHECK(tv[3].first == 3);
    CHECK(tv[4].first == 7);
    CHECK(tv[5].first == 8);
    CHECK(tv[6].first == 9);
}



TEST(TestQuerySort_QuickSort)
{
    // Triggers QuickSort because range > len
    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(rand() % 1100, "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first >= tv[t-1].first);
    }
}

TEST(TestQuerySort_CountSort)
{
    // Triggers CountSort because range <= len
    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(rand() % 900, "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort();

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first >= tv[t-1].first);
    }
}


TEST(TestQuerySort_Descending)
{
    TupleTableType ttt;

    for (size_t t = 0; t < 1000; t++)
        ttt.add(rand() % 1100, "a"); // 0

    TupleTableType::Query q = ttt.where();
    TupleTableType::View tv = q.find_all();
    tv.column().first.sort(false);

    CHECK(tv.size() == 1000);
    for (size_t t = 1; t < tv.size(); t++) {
        CHECK(tv[t].first <= tv[t-1].first);
    }
}


TEST(TestQuerySort_Dates)
{
    Table table;
    table.add_column(COLUMN_TYPE_DATE, "first");

    table.insert_date(0, 0, 1000);
    table.insert_done();
    table.insert_date(0, 1, 3000);
    table.insert_done();
    table.insert_date(0, 2, 2000);
    table.insert_done();

    TableView tv = table.where().find_all();
    CHECK(tv.size() == 3);
    CHECK(tv.get_source_ndx(0) == 0);
    CHECK(tv.get_source_ndx(1) == 1);
    CHECK(tv.get_source_ndx(2) == 2);

    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_date(0, 0) == 1000);
    CHECK(tv.get_date(0, 1) == 2000);
    CHECK(tv.get_date(0, 2) == 3000);
}


TEST(TestQuerySort_Bools)
{
    Table table;
    table.add_column(COLUMN_TYPE_BOOL, "first");

    table.insert_bool(0, 0, true);
    table.insert_done();
    table.insert_bool(0, 0, false);
    table.insert_done();
    table.insert_bool(0, 0, true);
    table.insert_done();

    TableView tv = table.where().find_all();
    tv.sort(0);

    CHECK(tv.size() == 3);
    CHECK(tv.get_bool(0, 0) == false);
    CHECK(tv.get_bool(0, 1) == true);
    CHECK(tv.get_bool(0, 2) == true);
}



TEST(TestQueryThreads)
{
    TupleTableType ttt;

    // Spread query search hits in an odd way to test more edge cases
    // (thread job size is THREAD_CHUNK_SIZE = 10)
    for (int i = 0; i < 100; i++) {
        for (int j = 0; j < 10; j++) {
            ttt.add(5, "a");
            ttt.add(j, "b");
            ttt.add(6, "c");
            ttt.add(6, "a");
            ttt.add(6, "b");
            ttt.add(6, "c");
            ttt.add(6, "a");
        }
    }
    TupleTableType::Query q1 = ttt.where().first.equal(2).second.equal("b");

    // Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
    //q1.set_threads(5);
    TupleTableType::View tv = q1.find_all();

    CHECK_EQUAL(100, tv.size());
    for (int i = 0; i < 100; i++) {
        const size_t expected = i*7*10 + 14 + 1;
        const size_t actual   = tv.get_source_ndx(i);
        CHECK_EQUAL(expected, actual);
    }
}



TEST(TestQuerySimple2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

/*
TEST(TestQueryLimit)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");
    ttt.add(1, "a");
    ttt.add(2, "a"); //
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2);

    TupleTableType::View tv1 = q1.find_all(0, size_t(-1), 2);
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));

    TupleTableType::View tv2 = q1.find_all(tv1.get_source_ndx(tv1.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(7, tv2.get_source_ndx(0));
    CHECK_EQUAL(10, tv2.get_source_ndx(1));

    TupleTableType::View tv3 = q1.find_all(tv2.get_source_ndx(tv2.size() - 1) + 1, size_t(-1), 2);
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(13, tv3.get_source_ndx(0));


    TupleTableType::Query q2 = ttt.where();
    TupleTableType::View tv4 = q2.find_all(0, 5, 3);
    CHECK_EQUAL(3, tv4.size());

    TupleTableType::Query q3 = ttt.where();
    TupleTableType::View tv5 = q3.find_all(0, 3, 5);
    CHECK_EQUAL(3, tv5.size());
}
*/

TEST(TestQueryFindNext)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "X");
    ttt.add(7, "X");

    TupleTableType::Query q1 = ttt.where().second.equal("X").first.greater(4);

    const size_t res1 = q1.find_next();
    const size_t res2 = q1.find_next(res1);
    const size_t res3 = q1.find_next(res2);

    CHECK_EQUAL(5, res1);
    CHECK_EQUAL(6, res2);
    CHECK_EQUAL((size_t)-1, res3); // no more matches
}

TEST(TestQueryFindAll1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "X");
    ttt.add(7, "X");

    TupleTableType::Query q1 = ttt.where().second.equal("a").first.greater(2).first.not_equal(4);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.get_source_ndx(0));

    TupleTableType::Query q2 = ttt.where().second.equal("X").first.greater(4);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(5, tv2.get_source_ndx(0));
    CHECK_EQUAL(6, tv2.get_source_ndx(1));

}

TEST(TestQueryFindAll2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(0, "X");

    TupleTableType::Query q2 = ttt.where().second.not_equal("a").first.less(3);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(6, tv2.get_source_ndx(0));
}

TEST(TestQueryFindAllBetween)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(3, "X");

    TupleTableType::Query q2 = ttt.where().first.between(3, 5);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
    CHECK_EQUAL(4, tv2.get_source_ndx(2));
    CHECK_EQUAL(6, tv2.get_source_ndx(3));
}


TEST(TestQueryFindAll_Range)
{
    TupleTableType ttt;

    ttt.add(5, "a");
    ttt.add(5, "a");
    ttt.add(5, "a");

    TupleTableType::Query q1 = ttt.where().second.equal("a").first.greater(2).first.not_equal(4);
    TupleTableType::View tv1 = q1.find_all(1, 2);
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(TestQueryFindAll_Or)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(6, "a");
    ttt.add(7, "X");

    // first == 5 || second == X
    TupleTableType::Query q1 = ttt.where().first.equal(5).Or().second.equal("X");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(TestQueryFindAll_Parans1)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // first > 3 && (second == X)
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().second.equal("X").end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(6, tv1.get_source_ndx(0));
}


TEST(TestQueryFindAll_OrParan)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X"); //
    ttt.add(4, "a");
    ttt.add(5, "a"); //
    ttt.add(6, "a");
    ttt.add(7, "X"); //
    ttt.add(2, "X");

    // (first == 5 || second == X && first > 2)
    TupleTableType::Query q1 = ttt.where().group().first.equal(5).Or().second.equal("X").first.greater(2).end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
    CHECK_EQUAL(4, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}


TEST(TestQueryFindAll_OrNested0)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(8, "Y");

    // first > 3 && (first == 5 || second == X)
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().first.equal(5).Or().second.equal("X").end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
}

TEST(TestQueryFindAll_OrNested)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");
    ttt.add(8, "Y");

    // first > 3 && (first == 5 || (second == X || second == Y))
    TupleTableType::Query q1 = ttt.where().first.greater(3).group().first.equal(5).Or().group().second.equal("X").Or().second.equal("Y").end_group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(5, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(7, tv1.get_source_ndx(2));
}

TEST(TestQueryFindAll_OrPHP)
{
    TupleTableType ttt;

    ttt.add(1, "Joe");
    ttt.add(2, "Sara");
    ttt.add(3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    TupleTableType::Query q1 = ttt.where().group().second.equal("Jim").Or().second.equal("Joe").end_group().first.equal(1);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}

TEST(TestQueryFindAllOr)
{
    TupleTableType ttt;

    ttt.add(1, "Joe");
    ttt.add(2, "Sara");
    ttt.add(3, "Jim");

    // (second == Jim || second == Joe) && first = 1
    TupleTableType::Query q1 = ttt.where().group().second.equal("Jim").Or().second.equal("Joe").end_group().first.equal(3);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.get_source_ndx(0));
}





TEST(TestQueryFindAll_Parans2)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // ()((first > 3()) && (()))
    TupleTableType::Query q1 = ttt.where().group().end_group().group().group().first.greater(3).group().end_group().end_group().group().group().end_group().end_group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(3, tv1.size());
    CHECK_EQUAL(4, tv1.get_source_ndx(0));
    CHECK_EQUAL(5, tv1.get_source_ndx(1));
    CHECK_EQUAL(6, tv1.get_source_ndx(2));
}

TEST(TestQueryFindAll_Parans4)
{
    TupleTableType ttt;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");
    ttt.add(3, "X");
    ttt.add(4, "a");
    ttt.add(5, "a");
    ttt.add(11, "X");

    // ()
    TupleTableType::Query q1 = ttt.where().group().end_group();
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(7, tv1.size());
}


TEST(TestQueryFindAll_Bool)
{
    BoolTupleTable btt;

    btt.add(1, true);
    btt.add(2, false);
    btt.add(3, true);
    btt.add(3, false);

    BoolTupleTable::Query q1 = btt.where().second.equal(true);
    BoolTupleTable::View tv1 = q1.find_all();
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    BoolTupleTable::Query q2 = btt.where().second.equal(false);
    BoolTupleTable::View tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));
}

TEST(TestQueryFindAll_Begins)
{
    TupleTableType ttt;

    ttt.add(0, "fo");
    ttt.add(0, "foo");
    ttt.add(0, "foobar");

    TupleTableType::Query q1 = ttt.where().second.begins_with("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}

TEST(TestQueryFindAll_Ends)
{
    TupleTableType ttt;

    ttt.add(0, "barfo");
    ttt.add(0, "barfoo");
    ttt.add(0, "barfoobar");

    TupleTableType::Query q1 = ttt.where().second.ends_with("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
}


TEST(TestQueryFindAll_Contains)
{
    TupleTableType ttt;

    ttt.add(0, "foo");
    ttt.add(0, "foobar");
    ttt.add(0, "barfoo");
    ttt.add(0, "barfoobaz");
    ttt.add(0, "fo");
    ttt.add(0, "fobar");
    ttt.add(0, "barfo");

    TupleTableType::Query q1 = ttt.where().second.contains("foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));
}

TEST(TestQueryEnums)
{
    TupleTableType table;

    for (size_t i = 0; i < 5; ++i) {
        table.add(1, "abd");
        table.add(2, "eftg");
        table.add(5, "hijkl");
        table.add(8, "mnopqr");
        table.add(9, "stuvxyz");
    }

    table.optimize();

    TupleTableType::Query q1 = table.where().second.equal("eftg");
    TupleTableType::View tv1 = q1.find_all();

    CHECK_EQUAL(5, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(6, tv1.get_source_ndx(1));
    CHECK_EQUAL(11, tv1.get_source_ndx(2));
    CHECK_EQUAL(16, tv1.get_source_ndx(3));
    CHECK_EQUAL(21, tv1.get_source_ndx(4));
}

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

#define uY  "\x0CE\x0AB"              // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088"    // decomposed form (Y followed by two dots)
#define uy  "\x0CF\x08B"              // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088"    // decomposed form (Y followed by two dots)

TEST(TestQueryCaseSensitivity)
{
    TupleTableType ttt;

    ttt.add(1, "BLAAbaergroed");
    ttt.add(1, "BLAAbaergroedandMORE");
    ttt.add(1, "BLAAbaergroed2");

    TupleTableType::Query q1 = ttt.where().second.equal("blaabaerGROED", false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
}

TEST(TestQueryUnicode2)
{
    TupleTableType ttt;

    ttt.add(1, uY);
    ttt.add(1, uYd);
    ttt.add(1, uy);
    ttt.add(1, uyd);

    TupleTableType::Query q1 = ttt.where().second.equal(uY, false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    TupleTableType::Query q2 = ttt.where().second.equal(uYd, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
    CHECK_EQUAL(3, tv2.get_source_ndx(1));

    TupleTableType::Query q3 = ttt.where().second.equal(uYd, true);
    TupleTableType::View tv3 = q3.find_all();
    CHECK_EQUAL(1, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
}

#define uA  "\x0c3\x085"         // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a"    // decomposed form (A (41) followed by ring)
#define ua  "\x0c3\x0a5"         // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a"    // decomposed form (a (41) followed by ring)

TEST(TestQueryUnicode3)
{
    TupleTableType ttt;

    ttt.add(1, uA);
    ttt.add(1, uAd);
    ttt.add(1, ua);
    ttt.add(1, uad);

    TupleTableType::Query q1 = ttt.where().second.equal(uA, false);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));

    TupleTableType::Query q2 = ttt.where().second.equal(ua, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(2, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(2, tv2.get_source_ndx(1));


    TupleTableType::Query q3 = ttt.where().second.equal(uad, false);
    TupleTableType::View tv3 = q3.find_all();
    CHECK_EQUAL(2, tv3.size());
    CHECK_EQUAL(1, tv3.get_source_ndx(0));
    CHECK_EQUAL(3, tv3.get_source_ndx(1));

    TupleTableType::Query q4 = ttt.where().second.equal(uad, true);
    TupleTableType::View tv4 = q4.find_all();
    CHECK_EQUAL(1, tv4.size());
    CHECK_EQUAL(3, tv4.get_source_ndx(0));
}


TEST(TestQueryFindAll_BeginsUNICODE)
{
    TupleTableType ttt;

    ttt.add(0, uad "fo");
    ttt.add(0, uad "foo");
    ttt.add(0, uad "foobar");

    TupleTableType::Query q1 = ttt.where().second.begins_with(uad "foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(2, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));
    CHECK_EQUAL(2, tv1.get_source_ndx(1));
}


TEST(TestQueryFindAll_EndsUNICODE)
{
    TupleTableType ttt;

    ttt.add(0, "barfo");
    ttt.add(0, "barfoo" uad);
    ttt.add(0, "barfoobar");

    TupleTableType::Query q1 = ttt.where().second.ends_with("foo" uad);
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(1, tv1.size());
    CHECK_EQUAL(1, tv1.get_source_ndx(0));

    TupleTableType::Query q2 = ttt.where().second.ends_with("foo" uAd, false);
    TupleTableType::View tv2 = q2.find_all();
    CHECK_EQUAL(1, tv2.size());
    CHECK_EQUAL(1, tv2.get_source_ndx(0));
}


TEST(TestQueryFindAll_ContainsUNICODE)
{
    TupleTableType ttt;

    ttt.add(0, uad "foo");
    ttt.add(0, uad "foobar");
    ttt.add(0, "bar" uad "foo");
    ttt.add(0, uad "bar" uad "foobaz");
    ttt.add(0, uad "fo");
    ttt.add(0, uad "fobar");
    ttt.add(0, uad "barfo");

    TupleTableType::Query q1 = ttt.where().second.contains(uad "foo");
    TupleTableType::View tv1 = q1.find_all();
    CHECK_EQUAL(4, tv1.size());
    CHECK_EQUAL(0, tv1.get_source_ndx(0));
    CHECK_EQUAL(1, tv1.get_source_ndx(1));
    CHECK_EQUAL(2, tv1.get_source_ndx(2));
    CHECK_EQUAL(3, tv1.get_source_ndx(3));

    TupleTableType::Query q2 = ttt.where().second.contains(uAd "foo", false);
    TupleTableType::View tv2 = q1.find_all();
    CHECK_EQUAL(4, tv2.size());
    CHECK_EQUAL(0, tv2.get_source_ndx(0));
    CHECK_EQUAL(1, tv2.get_source_ndx(1));
    CHECK_EQUAL(2, tv2.get_source_ndx(2));
    CHECK_EQUAL(3, tv2.get_source_ndx(3));
}

#endif

TEST(TestQuerySyntaxCheck)
{
    TupleTableType ttt;
    std::string s;

    ttt.add(1, "a");
    ttt.add(2, "a");
    ttt.add(3, "X");

    TupleTableType::Query q1 = ttt.where().first.equal(2).end_group();
#ifdef TIGHTDB_DEBUG
    s = q1.Verify();
    CHECK(s != "");
#endif

    TupleTableType::Query q2 = ttt.where().group().group().first.equal(2).end_group();
#ifdef TIGHTDB_DEBUG
    s = q2.Verify();
    CHECK(s != "");
#endif

    TupleTableType::Query q3 = ttt.where().first.equal(2).Or();
#ifdef TIGHTDB_DEBUG
    s = q3.Verify();
    CHECK(s != "");
#endif

    TupleTableType::Query q4 = ttt.where().Or().first.equal(2);
#ifdef TIGHTDB_DEBUG
    s = q4.Verify();
    CHECK(s != "");
#endif

    TupleTableType::Query q5 = ttt.where().first.equal(2);
#ifdef TIGHTDB_DEBUG
    s = q5.Verify();
    CHECK(s == "");
#endif

    TupleTableType::Query q6 = ttt.where().group().first.equal(2);
#ifdef TIGHTDB_DEBUG
    s = q6.Verify();
    CHECK(s != "");

#endif
    TupleTableType::Query q7 = ttt.where().second.equal("\xa0", false);
#ifdef TIGHTDB_DEBUG
    s = q7.Verify();
    CHECK(s != "");
#endif
}

TEST(TestTV)
{
    TupleTableType t;
    t.add(1, "a");
    t.add(2, "a");
    t.add(3, "c");
     
    TupleTableType::View v = t.where().first.greater(1).find_all();

    TupleTableType::Query q1 = t.where().tableview(v);
    CHECK_EQUAL(2, q1.count());

    TupleTableType::Query q3 = t.where().tableview(v).second.equal("a");
    CHECK_EQUAL(1, q3.count());
}
 
TEST(TestQuery_sum_min_max_avg)
{
    TupleTableType t;
    t.add(1, "a");
    t.add(2, "b");
    t.add(3, "c");

    CHECK_EQUAL(6, t.where().first.sum());
    CHECK_EQUAL(1, t.where().first.minimum());
    CHECK_EQUAL(3, t.where().first.maximum());
    CHECK_EQUAL(2, t.where().first.average());

    size_t cnt;
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 0, 0));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 1, 1));
    CHECK_EQUAL(0, cnt);
    CHECK_EQUAL(0, t.where().first.sum(&cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(1, t.where().first.sum(&cnt, 0, 1));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(2, t.where().first.sum(&cnt, 1, 2));
    CHECK_EQUAL(1, cnt);
    CHECK_EQUAL(3, t.where().first.sum(&cnt, 2, 3));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(3, t.where().first.sum(&cnt, 0, 2));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(5, t.where().first.sum(&cnt, 1, 3));
    CHECK_EQUAL(2, cnt);

    CHECK_EQUAL(6, t.where().first.sum(&cnt, 0, 3));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(6, t.where().first.sum(&cnt, 0, size_t(-1)));
    CHECK_EQUAL(3, cnt);
}

TEST(TestQuery_avg)
{
    TupleTableType t;
    size_t cnt;
    t.add(10, "a");
    CHECK_EQUAL(10, t.where().first.average());
    t.add(30, "b");
    CHECK_EQUAL(20, t.where().first.average());

    CHECK_EQUAL(0, t.where().first.average(NULL, 0, 0));     // none
    CHECK_EQUAL(0, t.where().first.average(NULL, 1, 1));     // none
    CHECK_EQUAL(20,t.where().first.average(NULL, 0, 2));     // both
    CHECK_EQUAL(20,t.where().first.average(NULL, 0, -1));     // both

    CHECK_EQUAL(10,t.where().first.average(&cnt, 0, 1));     // first
    
    CHECK_EQUAL(30,t.where().first.sum(NULL, 1, 2));     // second
    CHECK_EQUAL(30,t.where().first.average(NULL, 1, 2));     // second
}

TEST(TestQuery_avg2)
{
    TupleTableType t;
    size_t cnt;

    t.add(10, "a");
    t.add(100, "b");
    t.add(20, "a");
    t.add(100, "b");
    t.add(100, "b");
    t.add(30, "a");
    TupleTableType::Query q = t.where().second.equal("a");
    CHECK_EQUAL(3, q.count());
    q.first.sum();

    CHECK_EQUAL(60, t.where().second.equal("a").first.sum());

    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 0, 0));
    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 1, 1));
    CHECK_EQUAL(0, t.where().second.equal("a").first.average(&cnt, 2, 2));
    CHECK_EQUAL(0, cnt);

    CHECK_EQUAL(10, t.where().second.equal("a").first.average(&cnt, 0, 1));
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 1, 5));
    CHECK_EQUAL(30, t.where().second.equal("a").first.average(&cnt, 5, 6));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(15, t.where().second.equal("a").first.average(&cnt, 0, 3));
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 2, 5));
    CHECK_EQUAL(1, cnt);

    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt));
    CHECK_EQUAL(3, cnt);
    CHECK_EQUAL(15, t.where().second.equal("a").first.average(&cnt, 0, 3));
    CHECK_EQUAL(2, cnt);
    CHECK_EQUAL(20, t.where().second.equal("a").first.average(&cnt, 0, size_t(-1)));
    CHECK_EQUAL(3, cnt);
}


TEST(TestQuery_OfByOne)
{
    TupleTableType t;
    for (size_t i = 0; i < MAX_LIST_SIZE * 2; ++i) {
        t.add(1, "a");
    }

    // Top
    t[0].first = 0;
    size_t res = t.where().first.equal(0).find_next();
    CHECK_EQUAL(0, res);
    t[0].first = 1; // reset

    // Before split
    t[MAX_LIST_SIZE-1].first = 0;
    res = t.where().first.equal(0).find_next();
    CHECK_EQUAL(MAX_LIST_SIZE-1, res);
    t[MAX_LIST_SIZE-1].first = 1; // reset

    // After split
    t[MAX_LIST_SIZE].first = 0;
    res = t.where().first.equal(0).find_next();
    CHECK_EQUAL(MAX_LIST_SIZE, res);
    t[MAX_LIST_SIZE].first = 1; // reset

    // Before end
    const size_t last_pos = (MAX_LIST_SIZE*2)-1;
    t[last_pos].first = 0;
    res = t.where().first.equal(0).find_next();
    CHECK_EQUAL(last_pos, res);
}

TEST(TestQuery_Const)
{
    TupleTableType t;
    t.add(10, "a");
    t.add(100, "b");
    t.add(20, "a");

    const TupleTableType& const_table = t;

    const size_t count = const_table.where().second.equal("a").count();
    CHECK_EQUAL(2, count);

    //TODO: Should not be possible
    const_table.where().second.equal("a").remove();
}
