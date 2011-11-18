#include <UnitTest++.h>
#include "AllocSlab.h"

#include "Column.h"
#include "ColumnString.h"

TEST(AdaptiveStringColumnFindAllExpand) {

	AdaptiveStringColumn asc;
	Column c;

	asc.Add("HEJ");
	asc.Add("sdfsd");
	asc.Add("HEJ");
	asc.Add("sdfsd");
	asc.Add("HEJ");

	asc.FindAll(c, "HEJ");

	CHECK_EQUAL(5, asc.Size());
	CHECK_EQUAL(3, c.Size());
	CHECK_EQUAL(0, c.Get(0));
	CHECK_EQUAL(2, c.Get(1));
	CHECK_EQUAL(4, c.Get(2));

	// Expand to ArrayStringLong
	asc.Add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfs");
	asc.Add("HEJ");
	asc.Add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfs");
	asc.Add("HEJ");
	asc.Add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfgdfg djf gjkfdghkfds");

	// Todo, should the API behaviour really require us to clear c manually?
	c.Clear();
	asc.FindAll(c, "HEJ");

	CHECK_EQUAL(10, asc.Size());
	CHECK_EQUAL(5, c.Size());
	CHECK_EQUAL(0, c.Get(0));
	CHECK_EQUAL(2, c.Get(1));
	CHECK_EQUAL(4, c.Get(2));
	CHECK_EQUAL(6, c.Get(3));
	CHECK_EQUAL(8, c.Get(4));

	asc.Destroy();
	c.Destroy();

}

TEST(AdaptiveStringColumnFindAllRanges) {
	AdaptiveStringColumn asc;
	Column c;

	// 17 elements, to test node splits with MAX_LIST_SIZE = 3 or other small number
	asc.Add("HEJSA"); // 0
	asc.Add("1");
	asc.Add("HEJSA");
	asc.Add("3");
	asc.Add("HEJSA");
	asc.Add("5");
	asc.Add("HEJSA");
	asc.Add("7");
	asc.Add("HEJSA");
	asc.Add("9");
	asc.Add("HEJSA");
	asc.Add("11");
	asc.Add("HEJSA");
	asc.Add("13");
	asc.Add("HEJSA");
	asc.Add("15");
	asc.Add("HEJSA"); // 16

	c.Clear();
	asc.FindAll(c, "HEJSA", 0, 17);
	CHECK_EQUAL(9, c.Size());
	CHECK_EQUAL(0, c.Get(0));
	CHECK_EQUAL(2, c.Get(1));
	CHECK_EQUAL(4, c.Get(2));
	CHECK_EQUAL(6, c.Get(3));
	CHECK_EQUAL(8, c.Get(4));
	CHECK_EQUAL(10, c.Get(5));
	CHECK_EQUAL(12, c.Get(6));
	CHECK_EQUAL(14, c.Get(7));
	CHECK_EQUAL(16, c.Get(8));

	c.Clear();
	asc.FindAll(c, "HEJSA", 1, 16);
	CHECK_EQUAL(7, c.Size());
	CHECK_EQUAL(2, c.Get(0));
	CHECK_EQUAL(4, c.Get(1));
	CHECK_EQUAL(6, c.Get(2));
	CHECK_EQUAL(8, c.Get(3));
	CHECK_EQUAL(10, c.Get(4));
	CHECK_EQUAL(12, c.Get(5));
	CHECK_EQUAL(14, c.Get(6));

}

