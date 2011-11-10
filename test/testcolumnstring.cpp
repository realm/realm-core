#include "ColumnString.h"
#include <UnitTest++.h>

struct db_setup_column_string {
	static AdaptiveStringColumn c;
};

AdaptiveStringColumn db_setup_column_string::c;

TEST_FIXTURE(db_setup_column_string, ColumnStringMultiEmpty) {
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


TEST_FIXTURE(db_setup_column_string, ColumnStringSetExpand4) {
	c.Set(0, "hey");

	CHECK_EQUAL(6, c.Size());
	CHECK_EQUAL("hey", c.Get(0));
	CHECK_EQUAL("", c.Get(1));
	CHECK_EQUAL("", c.Get(2));
	CHECK_EQUAL("", c.Get(3));
	CHECK_EQUAL("", c.Get(4));
	CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringSetExpand8) {
	c.Set(1, "test");

	CHECK_EQUAL(6, c.Size());
	CHECK_EQUAL("hey", c.Get(0));
	CHECK_EQUAL("test", c.Get(1));
	CHECK_EQUAL("", c.Get(2));
	CHECK_EQUAL("", c.Get(3));
	CHECK_EQUAL("", c.Get(4));
	CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd0) {
	c.Clear();
	c.Add();
	CHECK_EQUAL("", c.Get(0));
	CHECK_EQUAL(1, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd1) {
	c.Add("a");
	CHECK_EQUAL("",  c.Get(0));
	CHECK_EQUAL("a", c.Get(1));
	CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd2) {
	c.Add("bb");
	CHECK_EQUAL("",   c.Get(0));
	CHECK_EQUAL("a",  c.Get(1));
	CHECK_EQUAL("bb", c.Get(2));
	CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd3) {
	c.Add("ccc");
	CHECK_EQUAL("",    c.Get(0));
	CHECK_EQUAL("a",   c.Get(1));
	CHECK_EQUAL("bb",  c.Get(2));
	CHECK_EQUAL("ccc", c.Get(3));
	CHECK_EQUAL(4, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd4) {
	c.Add("dddd");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd8) {
	c.Add("eeeeeeee");
	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd16) {
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

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd32) {
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

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd64) {
	// Add a string longer than 64 bytes to trigger long strings
	c.Add("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");

	CHECK_EQUAL("",     c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("bb",   c.Get(2));
	CHECK_EQUAL("ccc",  c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(7));
	CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.Get(8));
	CHECK_EQUAL(9, c.Size());
}


TEST_FIXTURE(db_setup_column_string, ColumnStringSet1) {
	c.Set(0, "ccc");
	c.Set(1, "bb");
	c.Set(2, "a");
	c.Set(3, "");

	CHECK_EQUAL(9, c.Size());

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(7));
	CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.Get(8));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert1) {
	// Insert in middle
	c.Insert(4, "xx");

	CHECK_EQUAL(10, c.Size());

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("xx",   c.Get(4));
	CHECK_EQUAL("dddd", c.Get(5));
	CHECK_EQUAL("eeeeeeee", c.Get(6));
	CHECK_EQUAL("ffffffffffffffff", c.Get(7));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(8));
	CHECK_EQUAL("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx", c.Get(9));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete1) {
	// Delete from end
	c.Delete(9);

	CHECK_EQUAL(9, c.Size());

	CHECK_EQUAL("ccc",  c.Get(0));
	CHECK_EQUAL("bb",   c.Get(1));
	CHECK_EQUAL("a",    c.Get(2));
	CHECK_EQUAL("",     c.Get(3));
	CHECK_EQUAL("xx",   c.Get(4));
	CHECK_EQUAL("dddd", c.Get(5));
	CHECK_EQUAL("eeeeeeee", c.Get(6));
	CHECK_EQUAL("ffffffffffffffff", c.Get(7));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(8));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete2) {
	// Delete from top
	c.Delete(0);

	CHECK_EQUAL(8, c.Size());

	CHECK_EQUAL("bb",   c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("",     c.Get(2));
	CHECK_EQUAL("xx",   c.Get(3));
	CHECK_EQUAL("dddd", c.Get(4));
	CHECK_EQUAL("eeeeeeee", c.Get(5));
	CHECK_EQUAL("ffffffffffffffff", c.Get(6));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(7));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete3) {
	// Delete from middle
	c.Delete(3);

	CHECK_EQUAL(7, c.Size());

	CHECK_EQUAL("bb",   c.Get(0));
	CHECK_EQUAL("a",    c.Get(1));
	CHECK_EQUAL("",     c.Get(2));
	CHECK_EQUAL("dddd", c.Get(3));
	CHECK_EQUAL("eeeeeeee", c.Get(4));
	CHECK_EQUAL("ffffffffffffffff", c.Get(5));
	CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.Get(6));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringDeleteAll) {
	// Delete all items one at a time
	c.Delete(0);
	CHECK_EQUAL(6, c.Size());
	c.Delete(0);
	CHECK_EQUAL(5, c.Size());
	c.Delete(0);
	CHECK_EQUAL(4, c.Size());
	c.Delete(0);
	CHECK_EQUAL(3, c.Size());
	c.Delete(0);
	CHECK_EQUAL(2, c.Size());
	c.Delete(0);
	CHECK_EQUAL(1, c.Size());
	c.Delete(0);
	CHECK_EQUAL(0, c.Size());

	CHECK(c.IsEmpty());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert2) {
	// Create new list
	c.Clear();
	c.Add("a");
	c.Add("b");
	c.Add("c");
	c.Add("d");

	// Insert in top with expansion
	c.Insert(0, "xxxxx");

	CHECK_EQUAL("xxxxx", c.Get(0));
	CHECK_EQUAL("a",     c.Get(1));
	CHECK_EQUAL("b",     c.Get(2));
	CHECK_EQUAL("c",     c.Get(3));
	CHECK_EQUAL("d",     c.Get(4));
	CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert3) {
	// Insert in middle with expansion
	c.Insert(3, "xxxxxxxxxx");

	CHECK_EQUAL("xxxxx", c.Get(0));
	CHECK_EQUAL("a",     c.Get(1));
	CHECK_EQUAL("b",     c.Get(2));
	CHECK_EQUAL("xxxxxxxxxx", c.Get(3));
	CHECK_EQUAL("c",     c.Get(4));
	CHECK_EQUAL("d",     c.Get(5));
	CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnString_Destroy) {
	// clean up (ALWAYS PUT THIS LAST)
	c.Destroy();
}

