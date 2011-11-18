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

TEST(Group_Serialize0) {
	// Delete old file if there
	remove("table_test.tbl");

	// Create empty group and serialize to disk
	Group toDisk;
	toDisk.Write("table_test.tbl");

	// Load the group
	Group fromDisk("table_test.tbl");

	// Create new table in group
	TestTableGroup& t = fromDisk.GetTable<TestTableGroup>("test");

	CHECK_EQUAL(4, t.GetColumnCount());
	CHECK_EQUAL(0, t.GetSize());

	// Modify table
	t.Add("Test",  1, true, Wed);

	CHECK_EQUAL("Test", (const char*)t[0].first);
	CHECK_EQUAL(1,      t[0].second);
	CHECK_EQUAL(true,   t[0].third);
	CHECK_EQUAL(Wed,    t[0].fourth);
}

TEST(Group_Serialize1) {
	// Create group with one table
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

#ifdef _DEBUG
	toDisk.Verify();
#endif //_DEBUG

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

#ifdef _DEBUG
	toDisk.Verify();
	fromDisk.Verify();
#endif //_DEBUG
}

TEST(Group_Serialize2) {
	// Create group with two tables
	Group toDisk;
	TestTableGroup& table1 = toDisk.GetTable<TestTableGroup>("test1");
	table1.Add("",  1, true, Wed);
	table1.Add("", 15, true, Wed);
	table1.Add("", 10, true, Wed);

	TestTableGroup& table2 = toDisk.GetTable<TestTableGroup>("test2");
	table2.Add("hey",  0, true, Tue);
	table2.Add("hello", 3232, false, Sun);

#ifdef _DEBUG
	toDisk.Verify();
#endif //_DEBUG

	// Delete old file if there
	remove("table_test.tbl");

	// Serialize to disk
	toDisk.Write("table_test.tbl");

	// Load the tables
	Group fromDisk("table_test.tbl");
	TestTableGroup& t1 = fromDisk.GetTable<TestTableGroup>("test1");
	TestTableGroup& t2 = fromDisk.GetTable<TestTableGroup>("test2");

	// Verify that original values are there
	CHECK(table1.Compare(t1));
	CHECK(table2.Compare(t2));

#ifdef _DEBUG
	toDisk.Verify();
	fromDisk.Verify();
#endif //_DEBUG
}

TEST(Group_Serialize3) {
	// Create group with one table (including long strings
	Group toDisk;
	TestTableGroup& table = toDisk.GetTable<TestTableGroup>("test");
	table.Add("1 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 1",  1, true, Wed);
	table.Add("2 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 2", 15, true, Wed);

#ifdef _DEBUG
	toDisk.Verify();
#endif //_DEBUG

	// Delete old file if there
	remove("table_test.tbl");

	// Serialize to disk
	toDisk.Write("table_test.tbl");

	// Load the table
	Group fromDisk("table_test.tbl");
	TestTableGroup& t = fromDisk.GetTable<TestTableGroup>("test");

	// Verify that original values are there
	CHECK(table.Compare(t));

#ifdef _DEBUG
	toDisk.Verify();
	fromDisk.Verify();
#endif //_DEBUG}
}

TEST(Group_Serialize_Men) {
	// Create group with one table
	Group toMem;
	TestTableGroup& table = toMem.GetTable<TestTableGroup>("test");
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

#ifdef _DEBUG
	toMem.Verify();
#endif //_DEBUG

	// Serialize to memory (we now own the buffer)
	size_t len;
	const char* const buffer = toMem.WriteToMem(len);

	// Load the table
	Group fromMem(buffer, len);
	TestTableGroup& t = fromMem.GetTable<TestTableGroup>("test");

	CHECK_EQUAL(4, t.GetColumnCount());
	CHECK_EQUAL(10, t.GetSize());

	// Verify that original values are there
	CHECK(table.Compare(t));

#ifdef _DEBUG
	toMem.Verify();
	fromMem.Verify();
#endif //_DEBUG
}

TEST(Group_Serialize_Optimized) {
	// Create group with one table
	Group toMem;
	TestTableGroup& table = toMem.GetTable<TestTableGroup>("test");

	for (size_t i = 0; i < 5; ++i) {
		table.Add("abd",     1, true, Mon);
		table.Add("eftg",    2, true, Tue);
		table.Add("hijkl",   5, true, Wed);
		table.Add("mnopqr",  8, true, Thu);
		table.Add("stuvxyz", 9, true, Fri);
	}

	table.Optimize();

#ifdef _DEBUG
	toMem.Verify();
#endif //_DEBUG

	// Serialize to memory (we now own the buffer)
	size_t len;
	const char* const buffer = toMem.WriteToMem(len);

	// Load the table
	Group fromMem(buffer, len);
	TestTableGroup& t = fromMem.GetTable<TestTableGroup>("test");

	CHECK_EQUAL(4, t.GetColumnCount());

	// Verify that original values are there
	CHECK(table.Compare(t));

	// Add a row with a known (but unique) value
	table.Add("search_target", 9, true, Fri);

	const size_t res = table.first.Find("search_target");
	CHECK_EQUAL(table.GetSize()-1, res);

#ifdef _DEBUG
	toMem.Verify();
	fromMem.Verify();
#endif //_DEBUG
}

#endif