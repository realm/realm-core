#include "Index.h"
#include <UnitTest++.h>

TEST(Index_Test1) {
	// Create a column with random values
	Column col;
	col.Add(3);
	col.Add(100);
	col.Add(10);
	col.Add(45);
	col.Add(0);

	// Create a new index on column
	Index ndx;
	ndx.BuildIndex(col);

	CHECK_EQUAL(0, ndx.Find(3));
	CHECK_EQUAL(1, ndx.Find(100));
	CHECK_EQUAL(2, ndx.Find(10));
	CHECK_EQUAL(3, ndx.Find(45));
	CHECK_EQUAL(4, ndx.Find(0));

	// Clean up
	col.Destroy();
	ndx.Destroy();
}

