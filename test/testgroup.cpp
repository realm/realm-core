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
	TestTableGroup table;
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
	table.Write("table_test.tbl");

	// Load the table
	Group db("table_test.tbl");
	TestTableGroup t = db.GetTable<TestTableGroup>();

	CHECK_EQUAL(4, t.GetColumnCount());
	CHECK_EQUAL(10, t.GetSize());

	// Verify that original values are there
	for (size_t i = 0; i < t.GetSize(); ++i) {
		CHECK_EQUAL("", (const char*)t[i].first);
		CHECK_EQUAL(true, t[i].third);
		CHECK_EQUAL(Wed, t[i].fourth);
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

	// Modify the backed table
	t[0].first = "test";
	t.Insert(5, "hello", 100, false, Mon);
	t.DeleteRow(1);

	CHECK_EQUAL("test", (const char*)t[0].first);
	CHECK_EQUAL("", (const char*)t[1].first);

	CHECK_EQUAL("hello", (const char*)t[4].first);
	CHECK_EQUAL(100, t[4].second);
	CHECK_EQUAL(false, t[4].third);
	CHECK_EQUAL(Mon, t[4].fourth);
}

#endif