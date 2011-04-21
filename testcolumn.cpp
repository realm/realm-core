#include "Column.h"
#include <UnitTest++.h>

struct db_setup {
	static Column c;
};

Column db_setup::c;

TEST_FIXTURE(db_setup, Add0) {
	c.Add(0);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Size(), 1);
}

TEST_FIXTURE(db_setup, Add1) {
	c.Add(1);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Size(), 2);
}

TEST_FIXTURE(db_setup, Add2) {
	c.Add(2);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Size(), 3);
}

TEST_FIXTURE(db_setup, Add3) {
	c.Add(3);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Size(), 4);
}

TEST_FIXTURE(db_setup, Add4) {
	c.Add(4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Size(), 5);
}

TEST_FIXTURE(db_setup, Add5) {
	c.Add(16);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Size(), 6);
}

TEST_FIXTURE(db_setup, Add6) {
	c.Add(256);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Size(), 7);
}

TEST_FIXTURE(db_setup, Add7) {
	c.Add(65536);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Get(7), 65536);
	CHECK_EQUAL(c.Size(), 8);
}

TEST_FIXTURE(db_setup, Add8) {
	c.Add64(4294967296);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 4);
	CHECK_EQUAL(c.Get(5), 16);
	CHECK_EQUAL(c.Get(6), 256);
	CHECK_EQUAL(c.Get(7), 65536);
	CHECK_EQUAL(c.Get64(8), 4294967296);
	CHECK_EQUAL(c.Size(), 9);
}

TEST_FIXTURE(db_setup, AddNeg1) {
	c.Clear();
	c.Add(-1);

	CHECK_EQUAL(c.Size(), 1);
	CHECK_EQUAL(c.Get(0), -1);
}

TEST_FIXTURE(db_setup, AddNeg2) {
	c.Add(-256);

	CHECK_EQUAL(c.Size(), 2);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
}

TEST_FIXTURE(db_setup, AddNeg3) {
	c.Add(-65536);

	CHECK_EQUAL(c.Size(), 3);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
}

TEST_FIXTURE(db_setup, AddNeg4) {
	c.Add64(-4294967296);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), -1);
	CHECK_EQUAL(c.Get(1), -256);
	CHECK_EQUAL(c.Get(2), -65536);
	CHECK_EQUAL(c.Get64(3), -4294967296);
}

TEST_FIXTURE(db_setup, Set) {
	c.Set(0, 3);
	c.Set(1, 2);
	c.Set(2, 1);
	c.Set(3, 0);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 3);
	CHECK_EQUAL(c.Get(1), 2);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 0);
}

TEST_FIXTURE(db_setup, Insert1) {
	// Set up some initial values
	c.Clear();
	c.Add(0);
	c.Add(1);
	c.Add(2);
	c.Add(3);

	// Insert in middle
	c.Insert(2, 16);

	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 16);
	CHECK_EQUAL(c.Get(3), 2);
	CHECK_EQUAL(c.Get(4), 3);
}

TEST_FIXTURE(db_setup, Insert2) {
	// Insert at top
	c.Insert(0, 256);

	CHECK_EQUAL(c.Size(), 6);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 16);
	CHECK_EQUAL(c.Get(4), 2);
	CHECK_EQUAL(c.Get(5), 3);
}

TEST_FIXTURE(db_setup, Insert3) {
	// Insert at bottom
	c.Insert(6, 65536);

	CHECK_EQUAL(c.Size(), 7);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 16);
	CHECK_EQUAL(c.Get(4), 2);
	CHECK_EQUAL(c.Get(5), 3);
	CHECK_EQUAL(c.Get(6), 65536);
}

TEST_FIXTURE(db_setup, Index1) {
	// Create index
	Column index;
	c.BuildIndex(index);

	CHECK_EQUAL(0, c.FindWithIndex(index, 256));
	CHECK_EQUAL(1, c.FindWithIndex(index, 0));
	CHECK_EQUAL(2, c.FindWithIndex(index, 1));
	CHECK_EQUAL(3, c.FindWithIndex(index, 16));
	CHECK_EQUAL(4, c.FindWithIndex(index, 2));
	CHECK_EQUAL(5, c.FindWithIndex(index, 3));
	CHECK_EQUAL(6, c.FindWithIndex(index, 65536));
	
	c.ClearIndex();
}

TEST_FIXTURE(db_setup, Delete1) {
	// Delete from middle
	c.Delete(3);

	CHECK_EQUAL(c.Size(), 6);
	CHECK_EQUAL(c.Get(0), 256);
	CHECK_EQUAL(c.Get(1), 0);
	CHECK_EQUAL(c.Get(2), 1);
	CHECK_EQUAL(c.Get(3), 2);
	CHECK_EQUAL(c.Get(4), 3);
	CHECK_EQUAL(c.Get(5), 65536);
}

TEST_FIXTURE(db_setup, Delete2) {
	// Delete from top
	c.Delete(0);

	CHECK_EQUAL(c.Size(), 5);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
	CHECK_EQUAL(c.Get(4), 65536);
}

TEST_FIXTURE(db_setup, Delete3) {
	// Delete from bottom
	c.Delete(4);

	CHECK_EQUAL(c.Size(), 4);
	CHECK_EQUAL(c.Get(0), 0);
	CHECK_EQUAL(c.Get(1), 1);
	CHECK_EQUAL(c.Get(2), 2);
	CHECK_EQUAL(c.Get(3), 3);
}

TEST_FIXTURE(db_setup, Find1) {
	// Look for a non-existing value
	size_t res = c.Find(10);

	CHECK_EQUAL(res, -1);
}

TEST_FIXTURE(db_setup, Find2) {
	// zero-bit width
	c.Clear();
	c.Add(0);
	c.Add(0);

	size_t res = c.Find(0);
	CHECK_EQUAL(res, 0);
}

TEST_FIXTURE(db_setup, Find3) {
	// expand to 1-bit width
	c.Add(1);

	size_t res = c.Find(1);
	CHECK_EQUAL(res, 2);
}

TEST_FIXTURE(db_setup, Find4) {
	// expand to 2-bit width
	c.Add(2);

	size_t res = c.Find(2);
	CHECK_EQUAL(res, 3);
}

TEST_FIXTURE(db_setup, Find5) {
	// expand to 4-bit width
	c.Add(4);

	size_t res = c.Find(4);
	CHECK_EQUAL(res, 4);
}

TEST_FIXTURE(db_setup, Find6) {
	// expand to 8-bit width
	c.Add(16);

	size_t res = c.Find(16);
	CHECK_EQUAL(res, 5);
}

TEST_FIXTURE(db_setup, Find7) {
	// expand to 16-bit width
	c.Add(256);

	size_t res = c.Find(256);
	CHECK_EQUAL(res, 6);
}

TEST_FIXTURE(db_setup, Find8) {
	// expand to 32-bit width
	c.Add(65536);

	size_t res = c.Find(65536);
	CHECK_EQUAL(res, 7);
}

TEST_FIXTURE(db_setup, Find9) {
	// expand to 64-bit width
	c.Add64(4294967296);

	size_t res = c.Find(4294967296);
	CHECK_EQUAL(res, 8);
}

#define PARTIAL_COUNT 100
TEST_FIXTURE(db_setup, PartialFind1) {
	c.Clear();

	for (size_t i = 0; i < PARTIAL_COUNT; ++i) {
		c.Add(i);
	}

	CHECK_EQUAL(-1, c.Find(PARTIAL_COUNT+1, 0, PARTIAL_COUNT));
	CHECK_EQUAL(-1, c.Find(0, 1, PARTIAL_COUNT));
	CHECK_EQUAL(PARTIAL_COUNT-1, c.Find(PARTIAL_COUNT-1, PARTIAL_COUNT-1, PARTIAL_COUNT));
}


TEST_FIXTURE(db_setup, HeaderParse) {
	Column column(c.GetRef(), (Column*)NULL, 0);
	const bool isEqual = (c == column);
	CHECK(isEqual);
}

TEST_FIXTURE(db_setup, Destroy) {
	// clean up (ALWAYS PUT THIS LAST)
	c.Destroy();
}

struct db_setup_string {
	static AdaptiveStringColumn c;
};

AdaptiveStringColumn db_setup_string::c;

TEST_FIXTURE(db_setup_string, StringAdd0) {
	c.Add();
	CHECK_EQUAL("", c.Get(0));
	CHECK_EQUAL(1, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd1) {
	c.Add("a");
	CHECK_EQUAL("",  c.Get(0));
	CHECK_EQUAL("a", c.Get(1));
	CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd2) {
	c.Add("bb");
	CHECK_EQUAL("",   c.Get(0));
	CHECK_EQUAL("a",  c.Get(1));
	CHECK_EQUAL("bb", c.Get(2));
	CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd3) {
	c.Add("ccc");
	CHECK_EQUAL("",    c.Get(0));
	CHECK_EQUAL("a",   c.Get(1));
	CHECK_EQUAL("bb",  c.Get(2));
	CHECK_EQUAL("ccc", c.Get(3));
	CHECK_EQUAL(4, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd4) {
	c.Add("dddd");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd8) {
	c.Add("eeeeeeee");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_string, StringAdd16) {
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

TEST_FIXTURE(db_setup_string, StringAdd32) {
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

TEST_FIXTURE(db_setup_string, StringSet1) {
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

TEST_FIXTURE(db_setup_string, StringInsert1) {
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

TEST_FIXTURE(db_setup_string, StringDelete1) {
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

TEST_FIXTURE(db_setup_string, StringDelete2) {
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

TEST_FIXTURE(db_setup_string, StringDelete3) {
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

TEST_FIXTURE(db_setup_string, StringInsert2) {
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

TEST_FIXTURE(db_setup_string, StringInsert3) {
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

TEST_FIXTURE(db_setup_string, StringFind1) {
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

TEST_FIXTURE(db_setup_string, StringFind2) {
	// Expand to 8 bytes width
	c.Add("eeeeee");

	// Search for last item
	const size_t r = c.Find("eeeeee");

	CHECK_EQUAL(4, r);
}

TEST_FIXTURE(db_setup_string, StringFind3) {
	// Expand to 16 bytes width
	c.Add("ffffffffffff");

	// Search for last item
	const size_t r = c.Find("ffffffffffff");

	CHECK_EQUAL(5, r);
}

TEST_FIXTURE(db_setup_string, StringFind4) {
	// Expand to 32 bytes width
	c.Add("gggggggggggggggggggggggg");

	// Search for last item 
	const size_t r = c.Find("gggggggggggggggggggggggg");

	CHECK_EQUAL(6, r);
}

TEST_FIXTURE(db_setup_string, StringFind5) {
	// Expand to 64 bytes width
	c.Add("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

	// Search for last item 
	const size_t r = c.Find("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

	CHECK_EQUAL(7, r);
}

