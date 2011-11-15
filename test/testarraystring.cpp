#include "ArrayString.h"
#include "Column.h"
#include <UnitTest++.h>

struct db_setup_string {
	static ArrayString c;
};

ArrayString db_setup_string::c;

TEST_FIXTURE(db_setup_string, ArrayStringMultiEmpty) {
	c.Add("");
	c.Add("");
	c.Add("");
	c.Add("");
	c.Add("");
	c.Add("");
	CHECK_EQUAL(6, c.Size());

	CHECK_EQUAL("", c.Get(0));
	CHECK_EQUAL("", c.Get(1));
	CHECK_EQUAL("", c.Get(2));
	CHECK_EQUAL("", c.Get(3));
	CHECK_EQUAL("", c.Get(4));
	CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand4) {
	c.Set(0, "hey");

	CHECK_EQUAL(6, c.Size());
	CHECK_EQUAL("hey", c.Get(0));
	CHECK_EQUAL("", c.Get(1));
	CHECK_EQUAL("", c.Get(2));
	CHECK_EQUAL("", c.Get(3));
	CHECK_EQUAL("", c.Get(4));
	CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand8) {
	c.Set(1, "test");

	CHECK_EQUAL(6, c.Size());
	CHECK_EQUAL("hey", c.Get(0));
	CHECK_EQUAL("test", c.Get(1));
	CHECK_EQUAL("", c.Get(2));
	CHECK_EQUAL("", c.Get(3));
	CHECK_EQUAL("", c.Get(4));
	CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_string, ArrayArrayStringAdd0) {
	c.Clear();
	c.Add();
	CHECK_EQUAL("", c.Get(0));
	CHECK_EQUAL(1, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd1) {
	c.Add("a");
	CHECK_EQUAL("",  c.Get(0));
	CHECK_EQUAL("a", c.Get(1));
	CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd2) {
	c.Add("bb");
	CHECK_EQUAL("",   c.Get(0));
	CHECK_EQUAL("a",  c.Get(1));
	CHECK_EQUAL("bb", c.Get(2));
	CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd3) {
	c.Add("ccc");
	CHECK_EQUAL("",    c.Get(0));
	CHECK_EQUAL("a",   c.Get(1));
	CHECK_EQUAL("bb",  c.Get(2));
	CHECK_EQUAL("ccc", c.Get(3));
	CHECK_EQUAL(4, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd4) {
	c.Add("dddd");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd8) {
	c.Add("eeeeeeee");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd16) {
	c.Add("ffffffffffffffff");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL(7, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd32) {
	c.Add("gggggggggggggggggggggggggggggggg");

	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(7));
	CHECK_EQUAL(8, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringSet1) {
	c.Set(0, "ccc");
	c.Set(1, "bb");
	c.Set(2, "a");
	c.Set(3, "");

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(7));
	CHECK_EQUAL(8, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert1) {
	// Insert in middle
	c.Insert(4, "xx", 2);

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("xx",   c.Get(4));
	CHECK_EQUAL("dddd", c.Get(5));
	CHECK_EQUAL("eeeeeeee", c.Get(6));
	CHECK_EQUAL("ffffffffffffffff", c.Get(7));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(8));
	CHECK_EQUAL(9, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete1) {
	// Delete from end
	c.Delete(8);

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("xx",   c.Get(4));
	CHECK_EQUAL("dddd", c.Get(5));
	CHECK_EQUAL("eeeeeeee", c.Get(6));
	CHECK_EQUAL("ffffffffffffffff", c.Get(7));
	CHECK_EQUAL(8, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete2) {
	// Delete from top
	c.Delete(0);

	CHECK_EQUAL("bb",   c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("",     c.Get(2));
	CHECK_EQUAL("xx",   c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL(7, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete3) {
	// Delete from middle
	c.Delete(3);

	CHECK_EQUAL("bb",   c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("",     c.Get(2));
	CHECK_EQUAL("dddd", c.Get(3));
	CHECK_EQUAL("eeeeeeee", c.Get(4));
	CHECK_EQUAL("ffffffffffffffff", c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDeleteAll) {
	// Delete all items one at a time
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);
	c.Delete(0);

	CHECK(c.IsEmpty());
	CHECK_EQUAL(0, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert2) {
	// Create new list
	c.Clear();
	c.Add("a");
	c.Add("b");
	c.Add("c");
	c.Add("d");

	// Insert in top with expansion
	c.Insert(0, "xxxxx", 5);

	CHECK_EQUAL("xxxxx", c.Get(0));
	CHECK_EQUAL("a",     c.Get(1));
	CHECK_EQUAL("b",     c.Get(2));
	CHECK_EQUAL("c",     c.Get(3));
	CHECK_EQUAL("d",     c.Get(4));
	CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert3) {
	// Insert in middle with expansion
	c.Insert(3, "xxxxxxxxxx", 10);

	CHECK_EQUAL("xxxxx", c.Get(0));
	CHECK_EQUAL("a",     c.Get(1));
	CHECK_EQUAL("b",     c.Get(2));
	CHECK_EQUAL("xxxxxxxxxx", c.Get(3));
	CHECK_EQUAL("c",     c.Get(4));
	CHECK_EQUAL("d",     c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_string, ArrayStringFind1) {
	// Create new list
	c.Clear();
	c.Add("a");
	c.Add("b");
	c.Add("c");
	c.Add("d");

	// Search for last item (4 bytes width)
	const size_t r = c.Find("d");

	CHECK_EQUAL(3, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind2) {
	// Expand to 8 bytes width
	c.Add("eeeeee");

	// Search for last item
	const size_t r = c.Find("eeeeee");

	CHECK_EQUAL(4, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind3) {
	// Expand to 16 bytes width
	c.Add("ffffffffffff");

	// Search for last item
	const size_t r = c.Find("ffffffffffff");

	CHECK_EQUAL(5, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind4) {
	// Expand to 32 bytes width
	c.Add("gggggggggggggggggggggggg");

	// Search for last item 
	const size_t r = c.Find("gggggggggggggggggggggggg");

	CHECK_EQUAL(6, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind5) {
	// Expand to 64 bytes width
	c.Add("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

	// Search for last item 
	const size_t r = c.Find("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

	CHECK_EQUAL(7, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFindAll) {
	c.Clear();
	Column col;

	// first, middle and end
	c.Add("foobar");
	c.Add("bar abc");
	c.Add("foobar");
	c.Add("baz");
	c.Add("foobar");

	c.FindAll(col, "foobar");
	CHECK_EQUAL(3, col.Size());
	CHECK_EQUAL(0, col.Get(0));
	CHECK_EQUAL(2, col.Get(1));
	CHECK_EQUAL(4, col.Get(2));

	// Cleanup
	col.Destroy();
}

TEST_FIXTURE(db_setup_string, ArrayStringDestroy) {
	// clean up (ALWAYS PUT THIS LAST)
	c.Destroy();
}
