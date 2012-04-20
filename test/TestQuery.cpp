#include "tightdb.h"
#include <UnitTest++.h>
#include "Group.h"

using namespace tightdb;

TDB_TABLE_2(TupleTableType,
	Int, first,
	String, second)

TDB_TABLE_2(BoolTupleTable,
	Int, first,
	Bool, second)



TEST(TestQueryDelete) {
	TupleTableType ttt;

	ttt.Add(1, "X");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a");
	ttt.Add(5, "X");
	ttt.Add(6, "X");

	Query q = ttt.GetQuery().second.Equal("X");
	size_t r = q.Delete(ttt);

	CHECK_EQUAL(4, r);
	CHECK_EQUAL(2, ttt.GetSize());
	CHECK_EQUAL(2, ttt[0].first);
	CHECK_EQUAL(4, ttt[1].first);
}



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


TEST(TestQuerySubtable) {

	Group group;
	TopLevelTable& table = group.GetTable("test");

	// Create specification with sub-table
	Spec s = table.GetSpec();
	s.AddColumn(COLUMN_TYPE_INT,    "first");
	s.AddColumn(COLUMN_TYPE_STRING, "second");
	Spec sub = s.AddColumnTable(    "third");
		sub.AddColumn(COLUMN_TYPE_INT,    "sub_first");
		sub.AddColumn(COLUMN_TYPE_STRING, "sub_second");
	table.UpdateFromSpec(s.GetRef());

	CHECK_EQUAL(3, table.GetColumnCount());

	// Main table
	table.InsertInt(0, 0, 111);
	table.InsertString(1, 0, "this");
	table.InsertTable(2, 0);
	table.InsertDone();

	table.InsertInt(0, 1, 222);
	table.InsertString(1, 1, "is");
	table.InsertTable(2, 1);
	table.InsertDone();

	table.InsertInt(0, 2, 333);
	table.InsertString(1, 2, "a test");
	table.InsertTable(2, 2);
	table.InsertDone();

	table.InsertInt(0, 3, 444);
	table.InsertString(1, 3, "of queries");
	table.InsertTable(2, 3);
	table.InsertDone();


	// Sub tables
	TableRef subtable = table.GetTable(2, 0);
	subtable->InsertInt(0, 0, 11);
	subtable->InsertString(1, 0, "a");
	subtable->InsertDone();

	subtable = table.GetTable(2, 1);
	subtable->InsertInt(0, 0, 22);
	subtable->InsertString(1, 0, "b");
	subtable->InsertDone();
	subtable->InsertInt(0, 1, 33);
	subtable->InsertString(1, 1, "c");
	subtable->InsertDone();

	subtable = table.GetTable(2, 2);
	subtable->InsertInt(0, 0, 44);
	subtable->InsertString(1, 0, "d");
	subtable->InsertDone();

	subtable = table.GetTable(2, 3);
	subtable->InsertInt(0, 0, 55);
	subtable->InsertString(1, 0, "e");
	subtable->InsertDone();


	Query *q1 = new Query;
	q1->Greater(0, 200);
	q1->Subtable(2);
	q1->Less(0, 50);
	q1->Parent();
	TableView t1 = q1->FindAll(table, 0, (size_t)-1);
	CHECK_EQUAL(2, t1.GetSize());
	CHECK_EQUAL(1, t1.GetRef(0));
	CHECK_EQUAL(2, t1.GetRef(1));
	delete q1;


	Query *q2 = new Query;
	q2->Subtable(2);
	q2->Greater(0, 50);
	q2->Or();
	q2->Less(0, 20);
	q2->Parent();
	TableView t2 = q2->FindAll(table, 0, (size_t)-1);
	CHECK_EQUAL(2, t2.GetSize());
	CHECK_EQUAL(0, t2.GetRef(0));
	CHECK_EQUAL(3, t2.GetRef(1));
	delete q2;


	Query *q3 = new Query;
	q3->Subtable(2);
	q3->Greater(0, 50);
	q3->Or();
	q3->Less(0, 20);
	q3->Parent();
	q3->Less(0, 300);
	TableView t3 = q3->FindAll(table, 0, (size_t)-1);
	CHECK_EQUAL(1, t3.GetSize());
	CHECK_EQUAL(0, t3.GetRef(0));
	delete q3;


	Query *q4 = new Query;
	q4->Equal(0, (int64_t)333);
	q4->Or();
	q4->Subtable(2);
	q4->Greater(0, 50);
	q4->Or();
	q4->Less(0, 20);
	q4->Parent();
	TableView t4 = q4->FindAll(table, 0, (size_t)-1);
	delete q4;


	CHECK_EQUAL(3, t4.GetSize());
	CHECK_EQUAL(0, t4.GetRef(0));
	CHECK_EQUAL(2, t4.GetRef(1));
	CHECK_EQUAL(3, t4.GetRef(2));

}




TEST(TestQuerySort1) {
	TupleTableType ttt;

	ttt.Add(1, "a"); // 0
	ttt.Add(2, "a"); // 1
	ttt.Add(3, "X"); // 2
	ttt.Add(1, "a"); // 3
	ttt.Add(2, "a"); // 4
	ttt.Add(3, "X"); // 5
	ttt.Add(9, "a"); // 6
	ttt.Add(8, "a"); // 7
	ttt.Add(7, "X"); // 8

	// tv.GetRef()	= 0, 2, 3, 5, 6, 7, 8
	// Vals			= 1, 3, 1, 3, 9, 8, 7
	// result		= 3, 0, 5, 2, 8, 7, 6

	Query q = ttt.GetQuery().first.NotEqual(2);
	TableView tv = q.FindAll(ttt);
	tv.Sort(0);

	CHECK(tv.GetSize() == 7);
	CHECK(tv.Get(0, 0) == 1);
	CHECK(tv.Get(0, 1) == 1);
	CHECK(tv.Get(0, 2) == 3);
	CHECK(tv.Get(0, 3) == 3);
	CHECK(tv.Get(0, 4) == 7);
	CHECK(tv.Get(0, 5) == 8);
	CHECK(tv.Get(0, 6) == 9);
}



TEST(TestQuerySort_QuickSort) {
	// Triggers QuickSort because range > len
	TupleTableType ttt;

	for(size_t t = 0; t < 1000; t++)
		ttt.Add(rand() % 1100, "a"); // 0

	Query q = ttt.GetQuery();
	TableView tv = q.FindAll(ttt);
	tv.Sort(0);

	CHECK(tv.GetSize() == 1000);
	for(size_t t = 1; t < tv.GetSize(); t++) {
		CHECK(tv.Get(0, t - 1) <= tv.Get(0, t - 1));
	}
}

TEST(TestQuerySort_CountSort) {
	// Triggers CountSort because range <= len
	TupleTableType ttt;

	for(size_t t = 0; t < 1000; t++)
		ttt.Add(rand() % 900, "a"); // 0

	Query q = ttt.GetQuery();
	TableView tv = q.FindAll(ttt);
	tv.Sort(0);

	CHECK(tv.GetSize() == 1000);
	for(size_t t = 1; t < tv.GetSize(); t++) {
		CHECK(tv.Get(0, t - 1) <= tv.Get(0, t - 1));
	}
}


TEST(TestQuerySort_Descending) {
	TupleTableType ttt;

	for(size_t t = 0; t < 1000; t++)
		ttt.Add(rand() % 1100, "a"); // 0

	Query q = ttt.GetQuery();
	TableView tv = q.FindAll(ttt);
	tv.Sort(0, false);

	CHECK(tv.GetSize() == 1000);
	for(size_t t = 1; t < tv.GetSize(); t++) {
		CHECK(tv.Get(0, t - 1) >= tv.Get(0, t - 1));
	}
}


TEST(TestQuerySort_Dates) {
	Table table;
	table.RegisterColumn(COLUMN_TYPE_DATE, "first");

	table.InsertDate(0, 0, 1000);
	table.InsertDone();
	table.InsertDate(0, 1, 3000);
	table.InsertDone();
	table.InsertDate(0, 2, 2000);
	table.InsertDone();

	Query *q = new Query();
	TableView tv = q->FindAll(table);
	delete q;
	CHECK(tv.GetSize() == 3);
	CHECK(tv.GetRef(0) == 0);
	CHECK(tv.GetRef(1) == 1);
	CHECK(tv.GetRef(2) == 2);

  	tv.Sort(0);

	CHECK(tv.GetSize() == 3);
	CHECK(tv.GetDate(0, 0) == 1000);
	CHECK(tv.GetDate(0, 1) == 2000);
	CHECK(tv.GetDate(0, 2) == 3000);
}


TEST(TestQuerySort_Bools) {
	Table table;
	table.RegisterColumn(COLUMN_TYPE_BOOL, "first");

	table.InsertBool(0, 0, true);
	table.InsertDone();
	table.InsertBool(0, 0, false);
	table.InsertDone();
	table.InsertBool(0, 0, true);
	table.InsertDone();

	Query *q = new Query();
	TableView tv = q->FindAll(table);
	delete q;
  	tv.Sort(0);

	CHECK(tv.GetSize() == 3);
	CHECK(tv.GetBool(0, 0) == false);
	CHECK(tv.GetBool(0, 1) == true);
	CHECK(tv.GetBool(0, 2) == true);
}



TEST(TestQueryThreads) {
	TupleTableType ttt;

	// Spread query search hits in an odd way to test more edge cases
	// (thread job size is THREAD_CHUNK_SIZE = 10)
	for(int i = 0; i < 100; i++) {
		for(int j = 0; j < 10; j++) {
			ttt.Add(5, "a");
			ttt.Add(j, "b");
			ttt.Add(6, "c");
			ttt.Add(6, "a");
			ttt.Add(6, "b");
			ttt.Add(6, "c");
			ttt.Add(6, "a");
		}
	}
	Query q1 = ttt.GetQuery().first.Equal(2).second.Equal("b");

	// Note, set THREAD_CHUNK_SIZE to 1.000.000 or more for performance
	//q1.SetThreads(5);
	TableView tv = q1.FindAll(ttt);

	CHECK_EQUAL(100, tv.GetSize());
	for(int i = 0; i < 100; i++) {
		CHECK_EQUAL(i*7*10 + 14 + 1, tv.GetRef(i));
	}
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


TEST(TestQueryLimit) {
	TupleTableType ttt;
	
	ttt.Add(1, "a");
	ttt.Add(2, "a"); //
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a"); //
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a"); //
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a"); //
	ttt.Add(3, "X");
	ttt.Add(1, "a");
	ttt.Add(2, "a"); //
	ttt.Add(3, "X");
	
	Query q1 = ttt.GetQuery().first.Equal(2);
	
	TableView tv1 = q1.FindAll(ttt, 0, (size_t)-1, 2);
	CHECK_EQUAL(2, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
	CHECK_EQUAL(4, tv1.GetRef(1));

	TableView tv2 = q1.FindAll(ttt, tv1.GetRef(tv1.GetSize() - 1) + 1, (size_t)-1, 2);
	CHECK_EQUAL(2, tv2.GetSize());
	CHECK_EQUAL(7, tv2.GetRef(0));
	CHECK_EQUAL(10, tv2.GetRef(1));
	
	TableView tv3 = q1.FindAll(ttt, tv2.GetRef(tv2.GetSize() - 1) + 1, (size_t)-1, 2);
	CHECK_EQUAL(1, tv3.GetSize());
	CHECK_EQUAL(13, tv3.GetRef(0));
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
	ttt.Add(4, "a"); 
	ttt.Add(5, "a");
	ttt.Add(6, "a");
	ttt.Add(7, "X");

	// first == 5 || second == X
	Query q1 = ttt.GetQuery().first.Equal(5).Or().second.Equal("X");
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(3, tv1.GetSize());
	CHECK_EQUAL(2, tv1.GetRef(0));
	CHECK_EQUAL(4, tv1.GetRef(1));
	CHECK_EQUAL(6, tv1.GetRef(2));
}


TEST(TestQueryFindAll_Parans1) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");

	// first > 3 && (second == X)
	Query q1 = ttt.GetQuery().first.Greater(3).LeftParan().second.Equal("X").RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(6, tv1.GetRef(0));
}


TEST(TestQueryFindAll_OrParan) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a"); 
	ttt.Add(5, "a");
	ttt.Add(6, "a");
	ttt.Add(7, "X");
	ttt.Add(2, "X");

	// (first == 5 || second == X && first > 2)
	Query q1 = ttt.GetQuery().LeftParan().first.Equal(5).Or().second.Equal("X").first.Greater(2).RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(3, tv1.GetSize());
	CHECK_EQUAL(2, tv1.GetRef(0));
	CHECK_EQUAL(4, tv1.GetRef(1));
	CHECK_EQUAL(6, tv1.GetRef(2));
}


TEST(TestQueryFindAll_OrNested0) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");
	ttt.Add(8, "Y");
	
	// first > 3 && (first == 5 || second == X)
	Query q1 = ttt.GetQuery().first.Greater(3).LeftParan().first.Equal(5).Or().second.Equal("X").RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(2, tv1.GetSize());
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

TEST(TestQueryFindAll_OrPHP) {
	TupleTableType ttt;

	ttt.Add(1, "Joe");
	ttt.Add(2, "Sara");
	ttt.Add(3, "Jim");

	// (second == Jim || second == Joe) && first = 1
	Query q1 = ttt.GetQuery().LeftParan().second.Equal("Jim").Or().second.Equal("Joe").RightParan().first.Equal(1);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(0, tv1.GetRef(0));
}



TEST(TestQueryFindAll_Parans2) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");

	// ()((first > 3()) && (()))
	Query q1 = ttt.GetQuery().LeftParan().RightParan().LeftParan().LeftParan().first.Greater(3).LeftParan().RightParan().RightParan().LeftParan().LeftParan().RightParan().RightParan().RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(3, tv1.GetSize());
	CHECK_EQUAL(4, tv1.GetRef(0));
	CHECK_EQUAL(5, tv1.GetRef(1));
	CHECK_EQUAL(6, tv1.GetRef(2));
}

TEST(TestQueryFindAll_Parans4) {
	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(3, "X"); 
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(11, "X");

	// ()
	Query q1 = ttt.GetQuery().LeftParan().RightParan();
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(7, tv1.GetSize());
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
	CHECK_EQUAL(2, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
	CHECK_EQUAL(2, tv1.GetRef(1));
}

TEST(TestQueryFindAll_Ends) {
	TupleTableType ttt;

	ttt.Add(0, "barfo");
	ttt.Add(0, "barfoo");
	ttt.Add(0, "barfoobar");

	Query q1 = ttt.GetQuery().second.EndsWith("foo");
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

#if (defined(_WIN32) || defined(__WIN32__) || defined(_WIN64))

#define uY  "\x0CE\x0AB"              // greek capital letter upsilon with dialytika (U+03AB)
#define uYd "\x0CE\x0A5\x0CC\x088"    // decomposed form (Y followed by two dots)
#define uy  "\x0CF\x08B"              // greek small letter upsilon with dialytika (U+03AB)
#define uyd "\x0cf\x085\x0CC\x088"    // decomposed form (Y followed by two dots)

TEST(TestQueryCaseSensitivity) {
	TupleTableType ttt;

	ttt.Add(1, "BLAAbaergroed");

	Query q1 = ttt.GetQuery().second.Equal("blaabaerGROED", false);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
}

TEST(TestQueryUnicode2) {
	TupleTableType ttt;

	ttt.Add(1, uY);
	ttt.Add(1, uYd); 
	ttt.Add(1, uy); 
	ttt.Add(1, uyd);

	Query q1 = ttt.GetQuery().second.Equal(uY, false);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(2, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
	CHECK_EQUAL(2, tv1.GetRef(1));

	Query q2 = ttt.GetQuery().second.Equal(uYd, false);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(2, tv2.GetSize());
	CHECK_EQUAL(1, tv2.GetRef(0));
	CHECK_EQUAL(3, tv2.GetRef(1));

	Query q3 = ttt.GetQuery().second.Equal(uYd, true);
	TableView tv3 = q3.FindAll(ttt);
	CHECK_EQUAL(1, tv3.GetSize());
	CHECK_EQUAL(1, tv3.GetRef(0));
}

#define uA  "\x0c3\x085"         // danish capital A with ring above (as in BLAABAERGROED)
#define uAd "\x041\x0cc\x08a"    // decomposed form (A (41) followed by ring)
#define ua  "\x0c3\x0a5"         // danish lower case a with ring above (as in blaabaergroed)
#define uad "\x061\x0cc\x08a"    // decomposed form (a (41) followed by ring)

TEST(TestQueryUnicode3) {
	TupleTableType ttt;

	ttt.Add(1, uA);
	ttt.Add(1, uAd); 
	ttt.Add(1, ua);
	ttt.Add(1, uad);

	Query q1 = ttt.GetQuery().second.Equal(uA, false);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(2, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
	CHECK_EQUAL(2, tv1.GetRef(1));

	Query q2 = ttt.GetQuery().second.Equal(ua, false);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(2, tv2.GetSize());
	CHECK_EQUAL(0, tv2.GetRef(0));
	CHECK_EQUAL(2, tv2.GetRef(1));


	Query q3 = ttt.GetQuery().second.Equal(uad, false);
	TableView tv3 = q3.FindAll(ttt);
	CHECK_EQUAL(2, tv3.GetSize());
	CHECK_EQUAL(1, tv3.GetRef(0));
	CHECK_EQUAL(3, tv3.GetRef(1));

	Query q4 = ttt.GetQuery().second.Equal(uad, true);
	TableView tv4 = q4.FindAll(ttt);
	CHECK_EQUAL(1, tv4.GetSize());
	CHECK_EQUAL(3, tv4.GetRef(0));
}


TEST(TestQueryFindAll_BeginsUNICODE) {
	TupleTableType ttt;

	ttt.Add(0, uad "fo");
	ttt.Add(0, uad "foo");
	ttt.Add(0, uad "foobar");

	Query q1 = ttt.GetQuery().second.BeginsWith(uad "foo");
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(2, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));
	CHECK_EQUAL(2, tv1.GetRef(1));
}


TEST(TestQueryFindAll_EndsUNICODE) {
	TupleTableType ttt;

	ttt.Add(0, "barfo");
	ttt.Add(0, "barfoo" uad);
	ttt.Add(0, "barfoobar");

	Query q1 = ttt.GetQuery().second.EndsWith("foo" uad);
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(1, tv1.GetSize());
	CHECK_EQUAL(1, tv1.GetRef(0));

	Query q2 = ttt.GetQuery().second.EndsWith("foo" uAd, false);
	TableView tv2 = q2.FindAll(ttt);
	CHECK_EQUAL(1, tv2.GetSize());
	CHECK_EQUAL(1, tv2.GetRef(0));
}


TEST(TestQueryFindAll_ContainsUNICODE) {
	TupleTableType ttt;

	ttt.Add(0, uad "foo");
	ttt.Add(0, uad "foobar");
	ttt.Add(0, "bar" uad "foo");
	ttt.Add(0, uad "bar" uad "foobaz");
	ttt.Add(0, uad "fo");
	ttt.Add(0, uad "fobar");
	ttt.Add(0, uad "barfo");

	Query q1 = ttt.GetQuery().second.Contains(uad "foo");
	TableView tv1 = q1.FindAll(ttt);
	CHECK_EQUAL(4, tv1.GetSize());
	CHECK_EQUAL(0, tv1.GetRef(0));
	CHECK_EQUAL(1, tv1.GetRef(1));
	CHECK_EQUAL(2, tv1.GetRef(2));
	CHECK_EQUAL(3, tv1.GetRef(3));

	Query q2 = ttt.GetQuery().second.Contains(uAd "foo", false);
	TableView tv2 = q1.FindAll(ttt);
	CHECK_EQUAL(4, tv2.GetSize());
	CHECK_EQUAL(0, tv2.GetRef(0));
	CHECK_EQUAL(1, tv2.GetRef(1));
	CHECK_EQUAL(2, tv2.GetRef(2));
	CHECK_EQUAL(3, tv2.GetRef(3));
}

#endif

TEST(TestQuerySyntaxCheck) {
	TupleTableType ttt;
	std::string s;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");

	Query q1 = ttt.GetQuery().first.Equal(2).RightParan();
	s = q1.Verify();
	CHECK(s != "");

	Query q2 = ttt.GetQuery().LeftParan().LeftParan().first.Equal(2).RightParan();
	s = q2.Verify();
	CHECK(s != "");

	Query q3 = ttt.GetQuery().first.Equal(2).Or();
	s = q3.Verify();
	CHECK(s != "");

	Query q4 = ttt.GetQuery().Or().first.Equal(2);
	s = q4.Verify();
	CHECK(s != "");

	Query q5 = ttt.GetQuery().first.Equal(2);
	s = q5.Verify();
	CHECK(s == "");

	Query q6 = ttt.GetQuery().LeftParan().first.Equal(2);
	s = q6.Verify();
	CHECK(s != "");

	Query q7 = ttt.GetQuery().second.Equal("\xa0", false);
	s = q7.Verify();
	CHECK(s != "");
}

