#include <UnitTest++.h>
#include <tightdb/array_string.hpp>
#include <tightdb/column.hpp>

using namespace tightdb;

struct db_setup_string {
    static ArrayString c;
};

ArrayString db_setup_string::c;

TEST_FIXTURE(db_setup_string, ArrayStringMultiEmpty)
{
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetEmpty1)
{
    c.set(0, "");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringErase0)
{
    c.erase(5);
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert0)
{
    // Intention: Insert a non-empty string into an array that is not
    // empty but contains only empty strings (and only ever have
    // contained empty strings). The insertion is not at the end of
    // the array.
    c.insert(0, "x");
}

TEST_FIXTURE(db_setup_string, ArrayStringSetEmpty2)
{
    c.set(0, "");
    c.set(5, "");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringClear)
{
    c.clear();
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringFind1)
{
    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    // Intention: Search for strings in an array that is not empty but
    // contains only empty strings (and only ever have contained empty
    // strings).
    CHECK_EQUAL(0, c.find_first(""));
    CHECK_EQUAL(size_t(-1), c.find_first("x"));
    CHECK_EQUAL(5, c.find_first("", 5));
    CHECK_EQUAL(size_t(-1), c.find_first("", 6));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand4)
{
    c.set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringFind2)
{
    // Intention: Search for non-empty string P that is not in then
    // array, but the array does contain a string where P is a prefix.
    CHECK_EQUAL(size_t(-1), c.find_first("he"));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand8)
{
    c.set(1, "test");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("test", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayArrayStringAdd0)
{
    c.clear();
    c.add();
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL(1, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd1)
{
    c.add("a");
    CHECK_EQUAL("",  c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL(2, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd2)
{
    c.add("bb");
    CHECK_EQUAL("",   c.get(0));
    CHECK_EQUAL("a",  c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL(3, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd3)
{
    c.add("ccc");
    CHECK_EQUAL("",    c.get(0));
    CHECK_EQUAL("a",   c.get(1));
    CHECK_EQUAL("bb",  c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL(4, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd4)
{
    c.add("dddd");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL(5, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd8)
{
    c.add("eeeeeeee");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd16)
{
    c.add("ffffffffffffffff");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL(7, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd32)
{
    c.add("gggggggggggggggggggggggggggggggg");

    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringSet1)
{
    c.set(0, "ccc");
    c.set(1, "bb");
    c.set(2, "a");
    c.set(3, "");

    CHECK_EQUAL("ccc",  c.get(0));
    CHECK_EQUAL("bb",   c.get(1));
    CHECK_EQUAL("a",    c.get(2));
    CHECK_EQUAL("",     c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert1)
{
    // Insert in middle
    c.insert(4, "xx");

    CHECK_EQUAL("ccc",  c.get(0));
    CHECK_EQUAL("bb",   c.get(1));
    CHECK_EQUAL("a",    c.get(2));
    CHECK_EQUAL("",     c.get(3));
    CHECK_EQUAL("xx",   c.get(4));
    CHECK_EQUAL("dddd", c.get(5));
    CHECK_EQUAL("eeeeeeee", c.get(6));
    CHECK_EQUAL("ffffffffffffffff", c.get(7));
    CHECK_EQUAL("gggggggggggggggggggggggggggggggg", c.get(8));
    CHECK_EQUAL(9, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringErase1)
{
    // Erase from end
    c.erase(8);

    CHECK_EQUAL("ccc",  c.get(0));
    CHECK_EQUAL("bb",   c.get(1));
    CHECK_EQUAL("a",    c.get(2));
    CHECK_EQUAL("",     c.get(3));
    CHECK_EQUAL("xx",   c.get(4));
    CHECK_EQUAL("dddd", c.get(5));
    CHECK_EQUAL("eeeeeeee", c.get(6));
    CHECK_EQUAL("ffffffffffffffff", c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringErase2)
{
    // Erase from top
    c.erase(0);

    CHECK_EQUAL("bb",   c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("",     c.get(2));
    CHECK_EQUAL("xx",   c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL(7, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringErase3)
{
    // Erase from middle
    c.erase(3);

    CHECK_EQUAL("bb",   c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("",     c.get(2));
    CHECK_EQUAL("dddd", c.get(3));
    CHECK_EQUAL("eeeeeeee", c.get(4));
    CHECK_EQUAL("ffffffffffffffff", c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringEraseAll)
{
    // Erase all items one at a time
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);

    CHECK(c.is_empty());
    CHECK_EQUAL(0, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert2)
{
    // Create new list
    c.clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Insert in top with expansion
    c.insert(0, "xxxxx");

    CHECK_EQUAL("xxxxx", c.get(0));
    CHECK_EQUAL("a",     c.get(1));
    CHECK_EQUAL("b",     c.get(2));
    CHECK_EQUAL("c",     c.get(3));
    CHECK_EQUAL("d",     c.get(4));
    CHECK_EQUAL(5, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert3)
{
    // Insert in middle with expansion
    c.insert(3, "xxxxxxxxxx");

    CHECK_EQUAL("xxxxx", c.get(0));
    CHECK_EQUAL("a",     c.get(1));
    CHECK_EQUAL("b",     c.get(2));
    CHECK_EQUAL("xxxxxxxxxx", c.get(3));
    CHECK_EQUAL("c",     c.get(4));
    CHECK_EQUAL("d",     c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringFind3)
{
    // Create new list
    c.clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Search for last item (4 bytes width)
    const size_t r = c.find_first("d");

    CHECK_EQUAL(3, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind4)
{
    // Expand to 8 bytes width
    c.add("eeeeee");

    // Search for last item
    const size_t r = c.find_first("eeeeee");

    CHECK_EQUAL(4, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind5)
{
    // Expand to 16 bytes width
    c.add("ffffffffffff");

    // Search for last item
    const size_t r = c.find_first("ffffffffffff");

    CHECK_EQUAL(5, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind6)
{
    // Expand to 32 bytes width
    c.add("gggggggggggggggggggggggg");

    // Search for last item
    const size_t r = c.find_first("gggggggggggggggggggggggg");

    CHECK_EQUAL(6, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind7)
{
    // Expand to 64 bytes width
    c.add("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

    // Search for last item
    const size_t r = c.find_first("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

    CHECK_EQUAL(7, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFindAll)
{
    c.clear();
    Array col;

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    c.find_all(col, "foobar");
    CHECK_EQUAL(3, col.size());
    CHECK_EQUAL(0, col.get(0));
    CHECK_EQUAL(2, col.get(1));
    CHECK_EQUAL(4, col.get(2));

    // Cleanup
    col.destroy();
}

TEST_FIXTURE(db_setup_string, ArrayStringCount)
{
    c.clear();

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    const size_t count = c.count("foobar");
    CHECK_EQUAL(3, count);
}

TEST_FIXTURE(db_setup_string, ArrayStringWithZeroBytes)
{
    c.clear();

    const char buf_1[] = { 'a', 0, 'b', 0, 'c' };
    const char buf_2[] = { 0, 'a', 0, 'b', 0 };
    const char buf_3[] = { 0, 0, 0, 0, 0 };

    c.add(StringData(buf_1, sizeof buf_1));
    c.add(StringData(buf_2, sizeof buf_2));
    c.add(StringData(buf_3, sizeof buf_3));

    CHECK_EQUAL(5, c.get(0).size());
    CHECK_EQUAL(5, c.get(1).size());
    CHECK_EQUAL(5, c.get(2).size());

    CHECK_EQUAL(StringData(buf_1, sizeof buf_1), c.get(0));
    CHECK_EQUAL(StringData(buf_2, sizeof buf_2), c.get(1));
    CHECK_EQUAL(StringData(buf_3, sizeof buf_3), c.get(2));
}

TEST_FIXTURE(db_setup_string, ArrayStringDestroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.destroy();
}

TEST(ArrayStringCompare)
{
    ArrayString a, b;

    CHECK(a.Compare(b));
    a.add("");
    CHECK(!a.Compare(b));
    b.add("x");
    CHECK(!a.Compare(b));
    a.set(0, "x");
    CHECK(a.Compare(b));

    a.destroy();
    b.destroy();
}
