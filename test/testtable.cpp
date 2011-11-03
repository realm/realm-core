#include "tightdb.h"
#include <UnitTest++.h>

TEST(Table1) {
	Table table;
	table.RegisterColumn(COLUMN_TYPE_INT, "first");
	table.RegisterColumn(COLUMN_TYPE_INT, "second");

	CHECK_EQUAL(COLUMN_TYPE_INT, table.GetColumnType(0));
	CHECK_EQUAL(COLUMN_TYPE_INT, table.GetColumnType(1));
	CHECK_EQUAL("first", table.GetColumnName(0));
	CHECK_EQUAL("second", table.GetColumnName(1));

	const size_t ndx = table.AddRow();
	table.Set(0, ndx, 0);
	table.Set(1, ndx, 10);

	CHECK_EQUAL(0, table.Get(0, ndx));
	CHECK_EQUAL(10, table.Get(1, ndx));

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

enum Days {
	Mon,
	Tue,
	Wed,
	Thu,
	Fri,
	Sat,
	Sun
};

TDB_TABLE_4(TestTable,
			Int,        first,
			Int,        second,
			Bool,       third,
			Enum<Days>, fourth)

TEST(Table2) {
	TestTable table;

	table.Add(0, 10, true, Wed);
	const TestTable::Cursor r = table[-1]; // last item

	CHECK_EQUAL(0, r.first);
	CHECK_EQUAL(10, r.second);
	CHECK_EQUAL(true, r.third);
	CHECK_EQUAL(Wed, r.fourth);

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

TEST(Table3) {
	TestTable table;

	for (size_t i = 0; i < 100; ++i) {
		table.Add(0, 10, true, Wed);
	}

	// Test column searching
	CHECK_EQUAL((size_t)0, table.first.Find(0));
	CHECK_EQUAL((size_t)-1, table.first.Find(1));
	CHECK_EQUAL((size_t)0, table.second.Find(10));
	CHECK_EQUAL((size_t)-1, table.second.Find(100));
	CHECK_EQUAL((size_t)0, table.third.Find(true));
	CHECK_EQUAL((size_t)-1, table.third.Find(false));
	CHECK_EQUAL((size_t)0, table.fourth.Find(Wed));
	CHECK_EQUAL((size_t)-1, table.fourth.Find(Mon));

	// Test column incrementing
	table.first += 3;
	CHECK_EQUAL(3, table[0].first);
	CHECK_EQUAL(3, table[99].first);

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

TDB_TABLE_2(TestTableEnum,
			Enum<Days>, first,
			String, second)

TEST(Table4) {
	TestTableEnum table;

	table.Add(Mon, "Hello");
	const TestTableEnum::Cursor r = table[-1]; // last item

	CHECK_EQUAL(Mon, r.first);
	CHECK_EQUAL("Hello", (const char*)r.second);

	// Test string column searching
	CHECK_EQUAL((size_t)0, table.second.Find("Hello"));
	CHECK_EQUAL((size_t)-1, table.second.Find("Foo"));

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

TEST(Table_Delete) {
	TestTable table;

	for (size_t i = 0; i < 10; ++i) {
		table.Add(0, i, true, Wed);
	}

	table.DeleteRow(0);
	table.DeleteRow(4);
	table.DeleteRow(7);

	CHECK_EQUAL(1, table[0].second);
	CHECK_EQUAL(2, table[1].second);
	CHECK_EQUAL(3, table[2].second);
	CHECK_EQUAL(4, table[3].second);
	CHECK_EQUAL(6, table[4].second);
	CHECK_EQUAL(7, table[5].second);
	CHECK_EQUAL(8, table[6].second);

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG

	// Delete all items one at a time
	for (size_t i = 0; i < 7; ++i) {
		table.DeleteRow(0);
	}

	CHECK(table.IsEmpty());
	CHECK_EQUAL(0, table.GetSize());

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}


TEST(Table_Find_Int) {
	TestTable table;

	for (int i = 1000; i >= 0; --i) {
		table.Add(0, i, true, Wed);
	}

	CHECK_EQUAL((size_t)0, table.second.Find(1000));
	CHECK_EQUAL((size_t)1000, table.second.Find(0));
	CHECK_EQUAL((size_t)-1, table.second.Find(1001));

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

TEST(Table6) {
	TestTableEnum table;

	TDB_QUERY(TestQuery, TestTableEnum) {
		first.between(Mon, Thu);
		second == "Hello" || (second == "Hey" && first == Mon);
	}};

	TDB_QUERY_OPT(TestQuery2, TestTableEnum) (Days a, Days b, const char* str) {
		first.between(a, b);
		second == str || second.MatchRegEx(".*");
	}};

	TestTableEnum result = table.FindAll(TestQuery2(Mon, Tue, "Hello")).Sort().Limit(10);
	size_t result2 = table.Range(10, 200).Find(TestQuery());
	CHECK_EQUAL((size_t)-1, result2);

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}



TEST(Table_FindAll_Int) {
	TestTable table;

	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);

	// Search for a value that does not exits
	const TableView v0 = table.second.FindAll(5);
	CHECK_EQUAL(0, v0.GetSize());

	// Search for a value with several matches
	const TableView v = table.second.FindAll(20);

	CHECK_EQUAL(5, v.GetSize());
	CHECK_EQUAL(1, v.GetRef(0));
	CHECK_EQUAL(3, v.GetRef(1));
	CHECK_EQUAL(5, v.GetRef(2));
	CHECK_EQUAL(7, v.GetRef(3));
	CHECK_EQUAL(9, v.GetRef(4));

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}

TEST(Table_Index_Int) {
	TestTable table;

	table.Add(0,  1, true, Wed);
	table.Add(0, 15, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0, 20, true, Wed);
	table.Add(0, 11, true, Wed);
	table.Add(0, 45, true, Wed);
	table.Add(0, 10, true, Wed);
	table.Add(0,  0, true, Wed);
	table.Add(0, 30, true, Wed);
	table.Add(0,  9, true, Wed);

	// Create index for column two
	table.SetIndex(1);

	// Search for a value that does not exits
	const size_t r1 = table.second.Find(2);
	CHECK_EQUAL(-1, r1);

	// Find existing values
	CHECK_EQUAL(0, table.second.Find(1));
	CHECK_EQUAL(1, table.second.Find(15));
	CHECK_EQUAL(2, table.second.Find(10));
	CHECK_EQUAL(3, table.second.Find(20));
	CHECK_EQUAL(4, table.second.Find(11));
	CHECK_EQUAL(5, table.second.Find(45)); 
	//CHECK_EQUAL(6, table.second.Find(10)); // only finds first match
	CHECK_EQUAL(7, table.second.Find(0));
	CHECK_EQUAL(8, table.second.Find(30));
	CHECK_EQUAL(9, table.second.Find(9));

	// Change some values
	table[2].second = 13;
	table[9].second = 100;

	CHECK_EQUAL(0, table.second.Find(1));
	CHECK_EQUAL(1, table.second.Find(15));
	CHECK_EQUAL(2, table.second.Find(13));
	CHECK_EQUAL(3, table.second.Find(20));
	CHECK_EQUAL(4, table.second.Find(11));
	CHECK_EQUAL(5, table.second.Find(45)); 
	CHECK_EQUAL(6, table.second.Find(10));
	CHECK_EQUAL(7, table.second.Find(0));
	CHECK_EQUAL(8, table.second.Find(30));
	CHECK_EQUAL(9, table.second.Find(100));

	// Insert values
	table.Add(0, 29, true, Wed);
	//TODO: More than add

	CHECK_EQUAL(0, table.second.Find(1));
	CHECK_EQUAL(1, table.second.Find(15));
	CHECK_EQUAL(2, table.second.Find(13));
	CHECK_EQUAL(3, table.second.Find(20));
	CHECK_EQUAL(4, table.second.Find(11));
	CHECK_EQUAL(5, table.second.Find(45)); 
	CHECK_EQUAL(6, table.second.Find(10));
	CHECK_EQUAL(7, table.second.Find(0));
	CHECK_EQUAL(8, table.second.Find(30));
	CHECK_EQUAL(9, table.second.Find(100));
	CHECK_EQUAL(10, table.second.Find(29));

	// Delete some values
	table.DeleteRow(0);
	table.DeleteRow(5);
	table.DeleteRow(8);

	CHECK_EQUAL(0, table.second.Find(15));
	CHECK_EQUAL(1, table.second.Find(13));
	CHECK_EQUAL(2, table.second.Find(20));
	CHECK_EQUAL(3, table.second.Find(11));
	CHECK_EQUAL(4, table.second.Find(45)); 
	CHECK_EQUAL(5, table.second.Find(0));
	CHECK_EQUAL(6, table.second.Find(30));
	CHECK_EQUAL(7, table.second.Find(100));

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}



#include "AllocSlab.h"
TEST(Table_SlabAlloc) {
	SlabAlloc alloc;
	TestTable table(alloc);

	table.Add(0, 10, true, Wed);
	const TestTable::Cursor r = table[-1]; // last item

	CHECK_EQUAL(0, r.first);
	CHECK_EQUAL(10, r.second);
	CHECK_EQUAL(true, r.third);
	CHECK_EQUAL(Wed, r.fourth);

	// Add some more rows
	table.Add(1, 10, true, Wed);
	table.Add(2, 20, true, Wed);
	table.Add(3, 10, true, Wed);
	table.Add(4, 20, true, Wed);
	table.Add(5, 10, true, Wed);

	// Delete some rows
	table.DeleteRow(2);
	table.DeleteRow(4);

#ifdef _DEBUG
	table.Verify();
#endif //_DEBUG
}



