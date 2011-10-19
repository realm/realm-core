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

TDB_TABLE_4(TestTableGroup,
			String,     first,
			Int,        second,
			Bool,       third,
			Enum<Days>, fourth)

// Windows version of serialization is not implemented yet
#ifndef _MSC_VER

TEST(Group_Serialize) {
	Group toDisk;
	TestTableGroup& table = toDisk.GetTable<TestTableGroup>("test");
	table.Add("",  1, true, Wed);
	table.Add("", 15, true, Wed);
	table.Add("", 10, true, Wed);
	table.Add("", 20, true, Wed);
	table.Add("", 11, true, Wed);
	table.Add("", 45, true, Wed);
	table.Add("", 10, true, Wed);
	table.Add("",  0, true, Wed);
	table.Add("", 30, true, Wed);
	table.Add("",  9, true, Wed);

	// Delete old file if there
	remove("table_test.tbl");

	// Serialize to disk
	toDisk.Write("table_test.tbl");

	// Load the table
	Group fromDisk("table_test.tbl");
	TestTableGroup& t = fromDisk.GetTable<TestTableGroup>("test");

	CHECK_EQUAL(4, t.GetColumnCount());
	CHECK_EQUAL(10, t.GetSize());

	// Verify that original values are there
	CHECK(table.Compare(t));

	// Modify both tables
	table[0].first = "test";
	t[0].first = "test";
	table.Insert(5, "hello", 100, false, Mon);
	t.Insert(5, "hello", 100, false, Mon);
	table.DeleteRow(1);
	t.DeleteRow(1);

	// Verify that both changed correctly
	CHECK(table.Compare(t));
}

#endif