#include "tightdb.h"
#include <UnitTest++.h>

TDB_TABLE_1(TestTableInt,
			Int,        first
)


TEST(GetSetInteger) {
	TestTableInt table;

	table.Add(1);
	table.Add(2);
	table.Add(3);
	table.Add(1);
	table.Add(2);
	
	TableView v = table.first.FindAll(2);

	CHECK_EQUAL(2, v.GetSize());

	// Test of Get
	CHECK_EQUAL(2, v.Get(0, 0));
	CHECK_EQUAL(2, v.Get(0, 1));
	
	// Test of Set
	v.Set(0, 0, 123);
	CHECK_EQUAL(123, v.Get(0, 0));

	// v.Destroy()
}


/*
TDB_TABLE_1(TestTableBool,
			Bool,        first
)


TEST(GetSetBool) {
	TestTableBool table;

	table.Add(true);
	table.Add(false);
	table.Add(true);
	table.Add(false);
	
	// Search for a value with several matches
	TableView v = table.first.FindAll(false);

	CHECK_EQUAL(2, v.GetSize());

	// Test of Get
	CHECK_EQUAL(false, v.GetBool(0, 0));
	CHECK_EQUAL(false, v.GetBool(0, 1));
	
	// Test of Set
	v.Set(0, 0, true);
	CHECK_EQUAL(true, v.Get(0, 0));

	// v.Destroy()
}
*/

/*
TDB_TABLE_1(TestTableString,
			String,        first
)


TEST(GetSetString) {
	TestTableBool table;

	table.Add("foo");
	table.Add("bar");
	table.Add("foo");
	table.Add("bar");

	TableView v = table.first.FindAll("bar");

	CHECK_EQUAL(2, v.GetSize());

	// Test of Get
	CHECK_EQUAL("bar", v.GetString(0, 0));
	CHECK_EQUAL("bar", v.GetString(0, 1));
	
	// Test of Set
	v.SetString(0, 0, "bar");
	CHECK_EQUAL("bar", v.Get(0, 0));

	// v.Destroy()
}
*/
