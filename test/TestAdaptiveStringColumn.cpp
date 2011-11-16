#include <UnitTest++.h>
#include "AllocSlab.h"

#include "Column.h"
#include "ColumnString.h"

TEST(AdaptiveStringColumnFindAll) {

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
