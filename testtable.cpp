#include "Table.h"
#include <UnitTest++.h>

TEST(Table1) {
	Table table("table1");
	table.RegisterColumn("first");
	table.RegisterColumn("second");

	const size_t ndx = table.AddRow();
	table.Set(0, ndx, 0);
	table.Set(1, ndx, 10);

	CHECK_EQUAL(0, table.Get(0, ndx));
	CHECK_EQUAL(10, table.Get(1, ndx));
}

TEST(Table2) {
	MyTable table;

	MyTable::Cursor r = table.Add();
	r.first = 0;
	r.second = 10;
	r.third = 300;
	r.fourth = true;

	CHECK_EQUAL(0, r.first);
	CHECK_EQUAL(10, r.second);
	CHECK_EQUAL(300, r.third);
	CHECK_EQUAL(true, r.fourth);
}

TEST(Table3) {
	MyTable table;

	table.Add(0, 10, 300, true);

	CHECK_EQUAL(0, table[0].first);
	CHECK_EQUAL(10, table[0].second);
	CHECK_EQUAL(300, table[0].third);
	CHECK_EQUAL(true, table[0].fourth);
}

