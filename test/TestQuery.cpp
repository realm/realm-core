#include "tightdb.h"
#include <UnitTest++.h>

TDB_TABLE_2(TupleTableType,
	Int, first,
	String, second)

TDB_TABLE_2(BoolTupleTable,
	Int, first,
	Bool, second)

TEST(TestQuerySimple) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");

	Query q1 = ttt.GetQuery().first.Equal(2);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
}

TEST(TestQuerySimple2) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");

	Query q1 = ttt.GetQuery().first.Equal(2);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(3, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
	CHECK_EQUAL(4, tv1.GetRef(1));
	CHECK_EQUAL(7, tv1.GetRef(2));
}

TEST(TestQueryCaseSensitivity) {
	TupleTableType ttt;

	ttt.Add(1, "blåbærgrød");
	ttt.Add(2, "BLÅBÆRGRØD");
	
	Query q1 = ttt.GetQuery().second.Equal("blåbærgrød", true);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
}

TEST(TestQueryFindAll1) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(6, "X");
	ttt.Add(7, "X");

	Query q1 = ttt.GetQuery().second.Equal("a").first.Greater(2).first.NotEqual(4);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(4, tv1.GetRef(0));

	Query q2 = ttt.GetQuery().second.Equal("X").first.Greater(4);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(5, tv2.GetRef(0));
	CHECK_EQUAL(6, tv2.GetRef(1));

}

TEST(TestQueryFindAll2) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");
	ttt.Add(0, "X");

	Query q2 = ttt.GetQuery().second.NotEqual("a").first.Less(3);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(6, tv2.GetRef(0));
}

TEST(TestQueryFindAllBetween) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");
	ttt.Add(3, "X");

	Query q2 = ttt.GetQuery().first.Between(3, 5);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(2, tv2.GetRef(0));
	CHECK_EQUAL(3, tv2.GetRef(1));
	CHECK_EQUAL(4, tv2.GetRef(2));
	CHECK_EQUAL(6, tv2.GetRef(3));
}


TEST(TestQueryFindAll_Range) {
	TupleTableType ttt;

	ttt.Add(5, "a");
	ttt.Add(5, "a");
	ttt.Add(5, "a"); 

	Query q1 = ttt.GetQuery().second.Equal("a").first.Greater(2).first.NotEqual(4);
	TableView tv1 = q1.FindAll(ttt, 1, 2);
	CHECK_EQUAL(1, tv1.GetRef(0));
}


TEST(TestQueryFindAll_Or) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");

	// first > 3 && (first == 5 || second == X)
	Query q1 = ttt.GetQuery().first.Greater(3).LeftParan().first.Equal(5).Or().second.Equal("X").RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(5, tv1.GetRef(0));
	CHECK_EQUAL(6, tv1.GetRef(1));
}

TEST(TestQueryFindAll_OrNested) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");
	ttt.Add(8, "Y");
	
	// first > 3 && (first == 5 || (second == X || second == Y))
	Query q1 = ttt.GetQuery().first.Greater(3).LeftParan().first.Equal(5).Or().LeftParan().second.Equal("X").Or().second.Equal("Y").RightParan().RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(5, tv1.GetRef(0));
	CHECK_EQUAL(6, tv1.GetRef(1));
	CHECK_EQUAL(7, tv1.GetRef(2));
}


TEST(TestQueryFindAll_Bool) {
	BoolTupleTable btt;

	btt.Add(1, true);
	btt.Add(2, false);
	btt.Add(3, true);
	btt.Add(3, false); 
	
	Query q1 = btt.GetQuery().second.Equal(true);
	TableView tv1 = q1.FindAll(btt);
	CHECK_EQUAL(0, tv1.GetRef(0));
	CHECK_EQUAL(2, tv1.GetRef(1));

	Query q2 = btt.GetQuery().second.Equal(false);
	TableView tv2 = q2.FindAll(btt);
	CHECK_EQUAL(1, tv2.GetRef(0));
	CHECK_EQUAL(3, tv2.GetRef(1));
}

TEST(TestQueryFindAll_Begins) {
	TupleTableType ttt;

	ttt.Add(0, "fo");
	ttt.Add(0, "foo");
	ttt.Add(0, "foobar");

	Query q1 = ttt.GetQuery().second.BeginsWith("foo");
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
}

TEST(TestQueryFindAll_Contains) {
	TupleTableType ttt;

	ttt.Add(0, "foo");
	ttt.Add(0, "foobar");
	ttt.Add(0, "barfoo");
	ttt.Add(0, "barfoobaz");
	ttt.Add(0, "fo");
	ttt.Add(0, "fobar");
	ttt.Add(0, "barfo");

	Query q1 = ttt.GetQuery().second.Contains("foo");
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(4, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
	CHECK_EQUAL(1, tv1.GetRef(1));
	CHECK_EQUAL(2, tv1.GetRef(2));
	CHECK_EQUAL(3, tv1.GetRef(3));
}

TEST(TestQueryEnums) {
	TupleTableType table;

	for (size_t i = 0; i < 5; ++i) {
		table.Add(1, "abd");
		table.Add(2, "eftg");
		table.Add(5, "hijkl");
		table.Add(8, "mnopqr");
		table.Add(9, "stuvxyz");
	}

	table.Optimize();

	Query q1 = table.GetQuery().second.Equal("eftg");
	TableView tv1 = q1.FindAll(table);

	CHECK_EQUAL(5, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
	CHECK_EQUAL(6, tv1.GetRef(1));
	CHECK_EQUAL(11, tv1.GetRef(2));
	CHECK_EQUAL(16, tv1.GetRef(3));
	CHECK_EQUAL(21, tv1.GetRef(4));
}
