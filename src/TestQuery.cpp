#include "tightdb.h"
#include <UnitTest++.h>

TDB_TABLE_2(TupleTableType,
	Int, first,
	String, second)


TEST(TestQuery) {

	TupleTableType ttt;

	ttt.Add(1, "a");
	ttt.Add(2, "a");
	ttt.Add(3, "X");
	ttt.Add(4, "a");
	ttt.Add(5, "a");
	ttt.Add(6, "a");
	ttt.Add(7, "X");
	ttt.Add(8, "X");
	ttt.Add(9, "X");
	
	//TupleTableType::TestQuery q = ttt.Query.second.NotEqual("X").first.Greater(2).first.NotEqual(4);
	Query q = ttt.Query.second.NotEqual("X").first.Greater(2).first.NotEqual(4);
	TableView tv = q.FindAll(ttt);
	CHECK_EQUAL(4, tv.GetRef(0));
}
