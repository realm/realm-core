#include "tightdb.h"
#include <UnitTest++.h>

TEST(Table1) {
	Table table("table1");
	table.RegisterColumn(COLUMN_TYPE_INT, "first");
	table.RegisterColumn(COLUMN_TYPE_INT, "second");

	const size_t ndx = table.AddRow();
	table.Set(0, ndx, 0);
	table.Set(1, ndx, 10);

	CHECK_EQUAL(0, table.Get(0, ndx));
	CHECK_EQUAL(10, table.Get(1, ndx));
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

	TestTable::Cursor r = table.Add(0, 10, true, Wed);

	CHECK_EQUAL(0, r.first);
	CHECK_EQUAL(10, r.second);
	CHECK_EQUAL(true, r.third);
	CHECK_EQUAL(Wed, r.fourth);
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
}

TDB_TABLE_2(TestTableEnum,
			Enum<Days>, first,
			String, second)

TEST(Table4) {
	TestTableEnum table;

	TestTableEnum::Cursor r = table.Add(Mon, "Hello");

	CHECK_EQUAL(Mon, r.first);
	CHECK_EQUAL("Hello", (const char*)r.second);

	// Test string column searching
	CHECK_EQUAL((size_t)0, table.second.Find("Hello"));
	CHECK_EQUAL((size_t)-1, table.second.Find("Foo"));
}

TEST(Table5) {
	TestTable table;

	for (int i = 1000; i >= 0; --i) {
		table.Add(0, i, true, Wed);
	}

	// Create a new index on second column
	table.SetIndex(1);

	CHECK_EQUAL((size_t)0, table.second.Find(1000));
	CHECK_EQUAL((size_t)1000, table.second.Find(0));
	CHECK_EQUAL((size_t)-1, table.second.Find(1001));
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
}
