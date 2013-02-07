#include <UnitTest++.h>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/index_string.hpp>

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

TEST(ColumnStringAutoEnumerateIndex)
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

    // Set index
    e.CreateIndex();
    CHECK(e.HasIndex());

    // Search for a value that does not exist
    const size_t res1 = e.find_first("nonexist");
    CHECK_EQUAL(not_found, res1);

    Array results;
    e.find_all(results, "nonexist");
    CHECK(results.is_empty());

    // Search for an existing value
    const size_t res2 = e.find_first("klmop");
    CHECK_EQUAL(4, res2);

    e.find_all(results, "klmop");
    CHECK_EQUAL(5, results.Size());
    CHECK_EQUAL(4, results.Get(0));
    CHECK_EQUAL(9, results.Get(1));
    CHECK_EQUAL(14, results.Get(2));
    CHECK_EQUAL(19, results.Get(3));
    CHECK_EQUAL(24, results.Get(4));

    // Set a value
    e.Set(1, "newval");
    const size_t res3 = e.count("a");
    const size_t res4 = e.count("bc");
    const size_t res5 = e.count("newval");
    CHECK_EQUAL(5, res3);
    CHECK_EQUAL(4, res4);
    CHECK_EQUAL(1, res5);

    results.Clear();
    e.find_all(results, "newval");
    CHECK_EQUAL(1, results.Size());
    CHECK_EQUAL(1, results.Get(0));

    // Insert a value
    e.Insert(4, "newval");
    const size_t res6 = e.count("newval");
    CHECK_EQUAL(2, res6);

    // Delete values
    e.Delete(1);
    e.Delete(0);
    const size_t res7 = e.count("a");
    const size_t res8 = e.count("newval");
    CHECK_EQUAL(4, res7);
    CHECK_EQUAL(1, res8);

    // Clear all
    e.Clear();
    const size_t res9 = e.count("a");
    CHECK_EQUAL(0, res9);

    // Cleanup
    c.Destroy();
    e.Destroy();
    results.Destroy();
}

TEST(ColumnStringAutoEnumerateIndexReuse)
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

    // Set index
    c.CreateIndex();
    CHECK(c.HasIndex());

    // Create StringEnum
    size_t keys;
    size_t values;
    const bool res = c.AutoEnumerate(keys, values);
    CHECK(res);
    ColumnStringEnum e(keys, values);

    // Reuse the index from original column
    StringIndex& ndx = c.PullIndex();
    e.ReuseIndex(ndx);
    CHECK(e.HasIndex());

    // Search for a value that does not exist
    const size_t res1 = e.find_first("nonexist");
    CHECK_EQUAL(not_found, res1);

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

TEST(AdaptiveStringColumnIndex)
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

    asc.CreateIndex();
    CHECK(asc.HasIndex());

    const size_t count0 = asc.count("HEJ");
    const size_t count1 = asc.count("HEJSA");
    const size_t count2 = asc.count("1");
    const size_t count3 = asc.count("15");
    CHECK_EQUAL(0, count0);
    CHECK_EQUAL(9, count1);
    CHECK_EQUAL(1, count2);
    CHECK_EQUAL(1, count3);

    const size_t ndx0 = asc.find_first("HEJS");
    const size_t ndx1 = asc.find_first("HEJSA");
    const size_t ndx2 = asc.find_first("1");
    const size_t ndx3 = asc.find_first("15");
    CHECK_EQUAL(not_found, ndx0);
    CHECK_EQUAL(0, ndx1);
    CHECK_EQUAL(1, ndx2);
    CHECK_EQUAL(15, ndx3);

    // Set some values
    asc.Set(1, "one");
    asc.Set(15, "fifteen");
    const size_t set1 = asc.find_first("1");
    const size_t set2 = asc.find_first("15");
    const size_t set3 = asc.find_first("one");
    const size_t set4 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, set1);
    CHECK_EQUAL(not_found, set2);
    CHECK_EQUAL(1, set3);
    CHECK_EQUAL(15, set4);

    // Insert some values
    asc.Insert(0, "top");
    asc.Insert(8, "middle");
    asc.add("bottom");
    const size_t ins1 = asc.find_first("top");
    const size_t ins2 = asc.find_first("middle");
    const size_t ins3 = asc.find_first("bottom");
    CHECK_EQUAL(0, ins1);
    CHECK_EQUAL(8, ins2);
    CHECK_EQUAL(19, ins3);

    // Delete some values
    asc.Delete(0);  // top
    asc.Delete(7);  // middle
    asc.Delete(17); // bottom
    const size_t del1 = asc.find_first("top");
    const size_t del2 = asc.find_first("middle");
    const size_t del3 = asc.find_first("bottom");
    const size_t del4 = asc.find_first("HEJSA");
    const size_t del5 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, del1);
    CHECK_EQUAL(not_found, del2);
    CHECK_EQUAL(not_found, del3);
    CHECK_EQUAL(0, del4);
    CHECK_EQUAL(15, del5);

    // Remove all
    asc.Clear();
    const size_t c1 = asc.find_first("HEJSA");
    const size_t c2 = asc.find_first("fifteen");
    CHECK_EQUAL(not_found, c1);
    CHECK_EQUAL(not_found, c2);

    // Clean-up
    asc.Destroy();
}

TEST_FIXTURE(db_setup_column_string, ColumnString_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
