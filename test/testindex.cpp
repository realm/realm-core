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

TEST(Index_Delete) {
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

	// Delete first item (in index)
	ndx.Delete(4, 0, true); // opt for last item

	CHECK_EQUAL(0, ndx.Find(3));
	CHECK_EQUAL(1, ndx.Find(100));
	CHECK_EQUAL(2, ndx.Find(10));
	CHECK_EQUAL(3, ndx.Find(45));
	CHECK_EQUAL(-1, ndx.Find(0));

	// Delete last item (in index)
	ndx.Delete(1, 100);

	CHECK_EQUAL(0, ndx.Find(3));
	CHECK_EQUAL(1, ndx.Find(10));
	CHECK_EQUAL(2, ndx.Find(45));
	CHECK_EQUAL(-1, ndx.Find(100));

	// Delete middle item (in index)
	ndx.Delete(1, 10);

	CHECK_EQUAL(0, ndx.Find(3));
	CHECK_EQUAL(1, ndx.Find(45));
	CHECK_EQUAL(-1, ndx.Find(10));

	// Delete all items
	ndx.Delete(1, 45);
	ndx.Delete(0, 3);

	CHECK_EQUAL(-1, ndx.Find(3));
	CHECK_EQUAL(-1, ndx.Find(45));
	CHECK_EQUAL(true, ndx.IsEmpty());

	// Clean up
	col.Destroy();
	ndx.Destroy();
}

TEST(Index_Insert) {
	// Create a column with random values
	Column col;
	col.Add(3);
	col.Add(100);
	col.Add(10);
	col.Add(45);
	col.Add(1);

	// Create a new index on column
	Index ndx;
	ndx.BuildIndex(col);

	// Insert item in top of column
	ndx.Insert(0, 0);

	CHECK_EQUAL(0, ndx.Find(0));
	CHECK_EQUAL(1, ndx.Find(3));
	CHECK_EQUAL(2, ndx.Find(100));
	CHECK_EQUAL(3, ndx.Find(10));
	CHECK_EQUAL(4, ndx.Find(45));
	CHECK_EQUAL(5, ndx.Find(1));

	// Append item in end of column
	ndx.Insert(6, 300, true); // opt for last item

	CHECK_EQUAL(0, ndx.Find(0));
	CHECK_EQUAL(1, ndx.Find(3));
	CHECK_EQUAL(2, ndx.Find(100));
	CHECK_EQUAL(3, ndx.Find(10));
	CHECK_EQUAL(4, ndx.Find(45));
	CHECK_EQUAL(5, ndx.Find(1));
	CHECK_EQUAL(6, ndx.Find(300));

	// Insert item in middle
	ndx.Insert(3, 15);

	CHECK_EQUAL(0, ndx.Find(0));
	CHECK_EQUAL(1, ndx.Find(3));
	CHECK_EQUAL(2, ndx.Find(100));
	CHECK_EQUAL(3, ndx.Find(15));
	CHECK_EQUAL(4, ndx.Find(10));
	CHECK_EQUAL(5, ndx.Find(45));
	CHECK_EQUAL(6, ndx.Find(1));
	CHECK_EQUAL(7, ndx.Find(300));

	// Clean up
	col.Destroy();
	ndx.Destroy();
}

