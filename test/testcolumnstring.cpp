#include <UnitTest++.h>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>

using namespace tightdb;

struct db_setup_column_string {
    static AdaptiveStringColumn c;
};

AdaptiveStringColumn db_setup_column_string::c;

TEST_FIXTURE(db_setup_column_string, ColumnStringMultiEmpty)
{
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL("", c.Get(0));
    CHECK_EQUAL("", c.Get(1));
    CHECK_EQUAL("", c.Get(2));
    CHECK_EQUAL("", c.Get(3));
    CHECK_EQUAL("", c.Get(4));
    CHECK_EQUAL("", c.Get(5));
}


TEST_FIXTURE(db_setup_column_string, ColumnStringSetExpand4)
{
    c.Set(0, "hey");

    CHECK_EQUAL(6, c.Size());
    CHECK_EQUAL("hey", c.Get(0));
    CHECK_EQUAL("", c.Get(1));
    CHECK_EQUAL("", c.Get(2));
    CHECK_EQUAL("", c.Get(3));
    CHECK_EQUAL("", c.Get(4));
    CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringSetExpand8)
{
    c.Set(1, "test");

    CHECK_EQUAL(6, c.Size());
    CHECK_EQUAL("hey", c.Get(0));
    CHECK_EQUAL("test", c.Get(1));
    CHECK_EQUAL("", c.Get(2));
    CHECK_EQUAL("", c.Get(3));
    CHECK_EQUAL("", c.Get(4));
    CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd0)
{
    c.Clear();
    c.add();
    CHECK_EQUAL("", c.Get(0));
    CHECK_EQUAL(1, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd1)
{
    c.add("a");
    CHECK_EQUAL("",  c.Get(0));
    CHECK_EQUAL("a", c.Get(1));
    CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd2)
{
    c.add("bb");
    CHECK_EQUAL("",   c.Get(0));
    CHECK_EQUAL("a",  c.Get(1));
    CHECK_EQUAL("bb", c.Get(2));
    CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd3)
{
    c.add("ccc");
    CHECK_EQUAL("",    c.Get(0));
    CHECK_EQUAL("a",   c.Get(1));
    CHECK_EQUAL("bb",  c.Get(2));
    CHECK_EQUAL("ccc", c.Get(3));
    CHECK_EQUAL(4, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd4)
{
    c.add("dddd");
    CHECK_EQUAL("",     c.Get(0));
    CHECK_EQUAL("a",    c.Get(1));
    CHECK_EQUAL("bb",   c.Get(2));
    CHECK_EQUAL("ccc",  c.Get(3));
    CHECK_EQUAL("dddd", c.Get(4));
    CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd8)
{
    c.add("eeeeeeee");
    CHECK_EQUAL("",     c.Get(0));
    CHECK_EQUAL("a",    c.Get(1));
    CHECK_EQUAL("bb",   c.Get(2));
    CHECK_EQUAL("ccc",  c.Get(3));
    CHECK_EQUAL("dddd", c.Get(4));
    CHECK_EQUAL("eeeeeeee", c.Get(5));
    CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd16)
{
    c.add("ffffffffffffffff");
    CHECK_EQUAL("",     c.Get(0));
    CHECK_EQUAL("a",    c.Get(1));
    CHECK_EQUAL("bb",   c.Get(2));
    CHECK_EQUAL("ccc",  c.Get(3));
    CHECK_EQUAL("dddd", c.Get(4));
    CHECK_EQUAL("eeeeeeee", c.Get(5));
    CHECK_EQUAL("ffffffffffffffff", c.Get(6));
    CHECK_EQUAL(7, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd32)
{
    c.add("gggggggggggggggggggggggggggggggg");

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

TEST_FIXTURE(db_setup_column_string, ColumnStringAdd64)
{
    // Add a string longer than 64 bytes to trigger long strings
    c.add("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");

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


TEST_FIXTURE(db_setup_column_string, ColumnStringSet1)
{
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

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert1)
{
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

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete1)
{
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

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete2)
{
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

TEST_FIXTURE(db_setup_column_string, ColumnStringDelete3)
{
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

TEST_FIXTURE(db_setup_column_string, ColumnStringDeleteAll)
{
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

    CHECK(c.is_empty());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert2)
{
    // Create new list
    c.Clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Insert in top with expansion
    c.Insert(0, "xxxxx");

    CHECK_EQUAL("xxxxx", c.Get(0));
    CHECK_EQUAL("a",     c.Get(1));
    CHECK_EQUAL("b",     c.Get(2));
    CHECK_EQUAL("c",     c.Get(3));
    CHECK_EQUAL("d",     c.Get(4));
    CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_column_string, ColumnStringInsert3)
{
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

TEST(ColumnStringFind1)
{
    AdaptiveStringColumn c;

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmop");

    size_t res1 = c.find_first("");
    CHECK_EQUAL((size_t)-1, res1);

    size_t res2 = c.find_first("xlmno hiuh iuh uih i huih i biuhui");
    CHECK_EQUAL((size_t)-1, res2);

    size_t res3 = c.find_first("klmop");
    CHECK_EQUAL(4, res3);

    // Cleanup
    c.Destroy();
}

TEST(ColumnStringFind2)
{
    AdaptiveStringColumn c;

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmop");

    // Add a string longer than 64 bytes to expand to long strings
    c.add("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");

    size_t res1 = c.find_first("");
    CHECK_EQUAL((size_t)-1, res1);

    size_t res2 = c.find_first("xlmno hiuh iuh uih i huih i biuhui");
    CHECK_EQUAL((size_t)-1, res2);

    size_t res3 = c.find_first("klmop");
    CHECK_EQUAL(4, res3);

    size_t res4 = c.find_first("xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx");
    CHECK_EQUAL(5, res4);

    // Cleanup
    c.Destroy();
}

TEST(ColumnStringAutoEnumerate)
{
    AdaptiveStringColumn c;

    // Add duplicate values
    for (size_t i = 0; i < 5; ++i) {
        c.add("a");
        c.add("bc");
        c.add("def");
        c.add("ghij");
        c.add("klmop");
    }

    // Create StringEnum
    size_t keys;
    size_t values;
    const bool res = c.AutoEnumerate(keys, values);
    CHECK(res);
    ColumnStringEnum e(keys, values);

    // Verify that all entries match source
    CHECK_EQUAL(c.Size(), e.Size());
    for (size_t i = 0; i < c.Size(); ++i) {
        const char* const s1 = c.Get(i);
        const char* const s2 = e.Get(i);
        CHECK_EQUAL(s1, s2);
    }

    // Search for a value that does not exist
    const size_t res1 = e.find_first("nonexist");
    CHECK_EQUAL((size_t)-1, res1);

    // Search for an existing value
    const size_t res2 = e.find_first("klmop");
    CHECK_EQUAL(4, res2);

    // Cleanup
    c.Destroy();
    e.Destroy();
}

// Test "Replace string array with long string array" when doing it through LeafSet()
TEST_FIXTURE(db_setup_column_string, ArrayStringSetLeafToLong2)
{
    c.Clear();
    Column col;

    c.add("foobar");
    c.add("bar abc");
    c.add("baz");

    c.Set(1, "70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");

    CHECK_EQUAL(c.Size(), c.Size());
    CHECK_EQUAL("foobar", c.Get(0));
    CHECK_EQUAL("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", c.Get(1));
    CHECK_EQUAL("baz", c.Get(2));

    // Cleanup
    col.Destroy();
}

// Test against a bug where FindWithLen() would fail finding ajacent hits
TEST_FIXTURE(db_setup_column_string, ArrayStringLongFindAjacent)
{
    c.Clear();
    Array col;

    c.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    c.add("baz");
    c.add("baz");
    c.add("foo");

    c.find_all(col, "baz");

    CHECK_EQUAL(2, col.Size());

    // Cleanup
    col.Destroy();
}

TEST(AdaptiveStringColumnFindAllExpand)
{
    AdaptiveStringColumn asc;
    Array c;

    asc.add("HEJ");
    asc.add("sdfsd");
    asc.add("HEJ");
    asc.add("sdfsd");
    asc.add("HEJ");

    asc.find_all(c, "HEJ");

    CHECK_EQUAL(5, asc.Size());
    CHECK_EQUAL(3, c.Size());
    CHECK_EQUAL(0, c.Get(0));
    CHECK_EQUAL(2, c.Get(1));
    CHECK_EQUAL(4, c.Get(2));

    // Expand to ArrayStringLong
    asc.add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfs");
    asc.add("HEJ");
    asc.add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfs");
    asc.add("HEJ");
    asc.add("dfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfsdfsdfsdkfjds gfgdfg djf gjkfdghkfds");

    // Todo, should the API behaviour really require us to clear c manually?
    c.Clear();
    asc.find_all(c, "HEJ");

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

// FindAll using ranges, when expanded ArrayStringLong
TEST(AdaptiveStringColumnFindAllRangesLong)
{
    AdaptiveStringColumn asc;
    Array c;

    // 17 elements, to test node splits with MAX_LIST_SIZE = 3 or other small number
    asc.add("HEJSA"); // 0
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA");
    asc.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
    asc.add("HEJSA"); // 16

    c.Clear();
    asc.find_all(c, "HEJSA", 0, 17);
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
    asc.find_all(c, "HEJSA", 1, 16);
    CHECK_EQUAL(7, c.Size());
    CHECK_EQUAL(2, c.Get(0));
    CHECK_EQUAL(4, c.Get(1));
    CHECK_EQUAL(6, c.Get(2));
    CHECK_EQUAL(8, c.Get(3));
    CHECK_EQUAL(10, c.Get(4));
    CHECK_EQUAL(12, c.Get(5));
    CHECK_EQUAL(14, c.Get(6));

    // Clean-up
    asc.Destroy();
    c.Destroy();
}

// FindAll using ranges, when not expanded (using ArrayString)
TEST(AdaptiveStringColumnFindAllRanges)
{
    AdaptiveStringColumn asc;
    Array c;

    // 17 elements, to test node splits with MAX_LIST_SIZE = 3 or other small number
    asc.add("HEJSA"); // 0
    asc.add("1");
    asc.add("HEJSA");
    asc.add("3");
    asc.add("HEJSA");
    asc.add("5");
    asc.add("HEJSA");
    asc.add("7");
    asc.add("HEJSA");
    asc.add("9");
    asc.add("HEJSA");
    asc.add("11");
    asc.add("HEJSA");
    asc.add("13");
    asc.add("HEJSA");
    asc.add("15");
    asc.add("HEJSA"); // 16

    c.Clear();
    asc.find_all(c, "HEJSA", 0, 17);
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
    asc.find_all(c, "HEJSA", 1, 16);
    CHECK_EQUAL(7, c.Size());
    CHECK_EQUAL(2, c.Get(0));
    CHECK_EQUAL(4, c.Get(1));
    CHECK_EQUAL(6, c.Get(2));
    CHECK_EQUAL(8, c.Get(3));
    CHECK_EQUAL(10, c.Get(4));
    CHECK_EQUAL(12, c.Get(5));
    CHECK_EQUAL(14, c.Get(6));

    // Clean-up
    asc.Destroy();
    c.Destroy();
}

TEST(AdaptiveStringColumnCount)
{
    AdaptiveStringColumn asc;
    
    // 17 elements, to test node splits with MAX_LIST_SIZE = 3 or other small number
    asc.add("HEJSA"); // 0
    asc.add("1");
    asc.add("HEJSA");
    asc.add("3");
    asc.add("HEJSA");
    asc.add("5");
    asc.add("HEJSA");
    asc.add("7");
    asc.add("HEJSA");
    asc.add("9");
    asc.add("HEJSA");
    asc.add("11");
    asc.add("HEJSA");
    asc.add("13");
    asc.add("HEJSA");
    asc.add("15");
    asc.add("HEJSA"); // 16
    
    const size_t count = asc.count("HEJSA");
    CHECK_EQUAL(9, count);
    
    // Create StringEnum
    size_t keys;
    size_t values;
    const bool res = asc.AutoEnumerate(keys, values);
    CHECK(res);
    ColumnStringEnum e(keys, values);
    
    // Check that enumerated column return same result
    const size_t ecount = e.count("HEJSA");
    CHECK_EQUAL(9, ecount);
    
    // Clean-up
    asc.Destroy();
    e.Destroy();
}

TEST_FIXTURE(db_setup_column_string, ColumnString_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
