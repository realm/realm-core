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

	CHECK_EQUAL(4, t.GetColumnCount());
	CHECK_EQUAL(10, t.GetSize());

	for (size_t i = 0; i < t.GetSize(); ++i) {
		CHECK_EQUAL(t[i].first, 0);
		CHECK_EQUAL(t[i].third, true);
		CHECK_EQUAL(t[i].fourth, Wed);
	}

	CHECK_EQUAL( 1, t[0].second);
	CHECK_EQUAL(15, t[1].second);
	CHECK_EQUAL(10, t[2].second);
	CHECK_EQUAL(20, t[3].second);
	CHECK_EQUAL(11, t[4].second);
	CHECK_EQUAL(45, t[5].second);
	CHECK_EQUAL(10, t[6].second);
	CHECK_EQUAL( 0, t[7].second);
	CHECK_EQUAL(30, t[8].second);
	CHECK_EQUAL( 9, t[9].second);
}

#endif