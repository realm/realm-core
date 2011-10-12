#include "Group.h"
#include "tightdb.h"
#include <UnitTest++.h>

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

// Windows version of serialization is not implemented yet
#ifndef _MSC_VER

TEST(Group_Serialize) {
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

	// Delete old file if there
	remove("table_test.tbl");

	// Serialize to disk
	table.Write("table_test.tbl");

	Group db("table_test.tbl");
	TestTable t = db.GetTable<TestTable>();

	CHECK_EQUAL(t.GetColumnCount(), 4);
	CHECK_EQUAL(t.GetSize(), 10);

	for (size_t i = 0; i < t.GetSize(); ++i) {
		CHECK_EQUAL(t[i].first, 0);
		CHECK_EQUAL(t[i].third, true);
		CHECK_EQUAL(t[i].fourth, Wed);
	}

	CHECK_EQUAL(t[0].second,  1);
	CHECK_EQUAL(t[1].second, 15);
	CHECK_EQUAL(t[2].second, 10);
	CHECK_EQUAL(t[3].second, 20);
	CHECK_EQUAL(t[4].second, 11);
	CHECK_EQUAL(t[5].second, 45);
	CHECK_EQUAL(t[6].second, 10);
	CHECK_EQUAL(t[7].second,  0);
	CHECK_EQUAL(t[8].second, 30);
	CHECK_EQUAL(t[9].second,  9);
}

#endif