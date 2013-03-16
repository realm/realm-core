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

    CHECK_EQUAL(StringData(""), c.get(0));
    CHECK_EQUAL(StringData(""), c.get(1));
    CHECK_EQUAL(StringData(""), c.get(2));
    CHECK_EQUAL(StringData(""), c.get(3));
    CHECK_EQUAL(StringData(""), c.get(4));
    CHECK_EQUAL(StringData(""), c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand4)
{
    c.set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL(StringData("hey"), c.get(0));
    CHECK_EQUAL(StringData(""), c.get(1));
    CHECK_EQUAL(StringData(""), c.get(2));
    CHECK_EQUAL(StringData(""), c.get(3));
    CHECK_EQUAL(StringData(""), c.get(4));
    CHECK_EQUAL(StringData(""), c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayStringSetExpand8)
{
    c.set(1, "test");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL(StringData("hey"), c.get(0));
    CHECK_EQUAL(StringData("test"), c.get(1));
    CHECK_EQUAL(StringData(""), c.get(2));
    CHECK_EQUAL(StringData(""), c.get(3));
    CHECK_EQUAL(StringData(""), c.get(4));
    CHECK_EQUAL(StringData(""), c.get(5));
}

TEST_FIXTURE(db_setup_string, ArrayArrayStringAdd0)
{
    c.Clear();
    c.add();
    CHECK_EQUAL(StringData(""), c.get(0));
    CHECK_EQUAL(1, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd1)
{
    c.add("a");
    CHECK_EQUAL(StringData(""),  c.get(0));
    CHECK_EQUAL(StringData("a"), c.get(1));
    CHECK_EQUAL(2, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd2)
{
    c.add("bb");
    CHECK_EQUAL(StringData(""),   c.get(0));
    CHECK_EQUAL(StringData("a"),  c.get(1));
    CHECK_EQUAL(StringData("bb"), c.get(2));
    CHECK_EQUAL(3, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd3)
{
    c.add("ccc");
    CHECK_EQUAL(StringData(""),    c.get(0));
    CHECK_EQUAL(StringData("a"),   c.get(1));
    CHECK_EQUAL(StringData("bb"),  c.get(2));
    CHECK_EQUAL(StringData("ccc"), c.get(3));
    CHECK_EQUAL(4, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd4)
{
    c.add("dddd");
    CHECK_EQUAL(StringData(""),     c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData("bb"),   c.get(2));
    CHECK_EQUAL(StringData("ccc"),  c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(5, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd8)
{
    c.add("eeeeeeee");
    CHECK_EQUAL(StringData(""),     c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData("bb"),   c.get(2));
    CHECK_EQUAL(StringData("ccc"),  c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd16)
{
    c.add("ffffffffffffffff");
    CHECK_EQUAL(StringData(""),     c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData("bb"),   c.get(2));
    CHECK_EQUAL(StringData("ccc"),  c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(5));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(6));
    CHECK_EQUAL(7, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringAdd32)
{
    c.add("gggggggggggggggggggggggggggggggg");

    CHECK_EQUAL(StringData(""),     c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData("bb"),   c.get(2));
    CHECK_EQUAL(StringData("ccc"),  c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(5));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(6));
    CHECK_EQUAL(StringData("gggggggggggggggggggggggggggggggg"), c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringSet1)
{
    c.set(0, "ccc");
    c.set(1, "bb");
    c.set(2, "a");
    c.set(3, "");

    CHECK_EQUAL(StringData("ccc"),  c.get(0));
    CHECK_EQUAL(StringData("bb"),   c.get(1));
    CHECK_EQUAL(StringData("a"),    c.get(2));
    CHECK_EQUAL(StringData(""),     c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(5));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(6));
    CHECK_EQUAL(StringData("gggggggggggggggggggggggggggggggg"), c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert1)
{
    // Insert in middle
    c.insert(4, "xx", 2);

    CHECK_EQUAL(StringData("ccc"),  c.get(0));
    CHECK_EQUAL(StringData("bb"),   c.get(1));
    CHECK_EQUAL(StringData("a"),    c.get(2));
    CHECK_EQUAL(StringData(""),     c.get(3));
    CHECK_EQUAL(StringData("xx"),   c.get(4));
    CHECK_EQUAL(StringData("dddd"), c.get(5));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(6));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(7));
    CHECK_EQUAL(StringData("gggggggggggggggggggggggggggggggg"), c.get(8));
    CHECK_EQUAL(9, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete1)
{
    // Delete from end
    c.erase(8);

    CHECK_EQUAL(StringData("ccc"),  c.get(0));
    CHECK_EQUAL(StringData("bb"),   c.get(1));
    CHECK_EQUAL(StringData("a"),    c.get(2));
    CHECK_EQUAL(StringData(""),     c.get(3));
    CHECK_EQUAL(StringData("xx"),   c.get(4));
    CHECK_EQUAL(StringData("dddd"), c.get(5));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(6));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(7));
    CHECK_EQUAL(8, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete2)
{
    // Delete from top
    c.erase(0);

    CHECK_EQUAL(StringData("bb"),   c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData(""),     c.get(2));
    CHECK_EQUAL(StringData("xx"),   c.get(3));
    CHECK_EQUAL(StringData("dddd"), c.get(4));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(5));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(6));
    CHECK_EQUAL(7, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDelete3)
{
    // Delete from middle
    c.erase(3);

    CHECK_EQUAL(StringData("bb"),   c.get(0));
    CHECK_EQUAL(StringData("a"),    c.get(1));
    CHECK_EQUAL(StringData(""),     c.get(2));
    CHECK_EQUAL(StringData("dddd"), c.get(3));
    CHECK_EQUAL(StringData("eeeeeeee"), c.get(4));
    CHECK_EQUAL(StringData("ffffffffffffffff"), c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringDeleteAll)
{
    // Delete all items one at a time
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
    c.Clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Insert in top with expansion
    c.insert(0, "xxxxx", 5);

    CHECK_EQUAL(StringData("xxxxx"), c.get(0));
    CHECK_EQUAL(StringData("a"),     c.get(1));
    CHECK_EQUAL(StringData("b"),     c.get(2));
    CHECK_EQUAL(StringData("c"),     c.get(3));
    CHECK_EQUAL(StringData("d"),     c.get(4));
    CHECK_EQUAL(5, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringInsert3)
{
    // Insert in middle with expansion
    c.insert(3, "xxxxxxxxxx", 10);

    CHECK_EQUAL(StringData("xxxxx"), c.get(0));
    CHECK_EQUAL(StringData("a"),     c.get(1));
    CHECK_EQUAL(StringData("b"),     c.get(2));
    CHECK_EQUAL(StringData("xxxxxxxxxx"), c.get(3));
    CHECK_EQUAL(StringData("c"),     c.get(4));
    CHECK_EQUAL(StringData("d"),     c.get(5));
    CHECK_EQUAL(6, c.size());
}

TEST_FIXTURE(db_setup_string, ArrayStringFind1)
{
    // Create new list
    c.Clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Search for last item (4 bytes width)
    const size_t r = c.find_first("d");

    CHECK_EQUAL(3, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind2)
{
    // Expand to 8 bytes width
    c.add("eeeeee");

    // Search for last item
    const size_t r = c.find_first("eeeeee");

    CHECK_EQUAL(4, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind3)
{
    // Expand to 16 bytes width
    c.add("ffffffffffff");

    // Search for last item
    const size_t r = c.find_first("ffffffffffff");

    CHECK_EQUAL(5, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind4)
{
    // Expand to 32 bytes width
    c.add("gggggggggggggggggggggggg");

    // Search for last item
    const size_t r = c.find_first("gggggggggggggggggggggggg");

    CHECK_EQUAL(6, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFind5)
{
    // Expand to 64 bytes width
    c.add("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

    // Search for last item
    const size_t r = c.find_first("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

    CHECK_EQUAL(7, r);
}

TEST_FIXTURE(db_setup_string, ArrayStringFindAll)
{
    c.Clear();
    Array col;

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    c.find_all(col, "foobar");
    CHECK_EQUAL(3, col.size());
    CHECK_EQUAL(0, col.Get(0));
    CHECK_EQUAL(2, col.Get(1));
    CHECK_EQUAL(4, col.Get(2));

    // Cleanup
    col.Destroy();
}

TEST_FIXTURE(db_setup_string, ArrayStringCount)
{
    c.Clear();

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
    c.Clear();

    const char buf_1[] = { 'a', 0, 'b', 0, 'c' };
    const char buf_2[] = { 0, 'a', 0, 'b', 0 };
    const char buf_3[] = { 0, 0, 0, 0, 0 };

    c.add(buf_1, sizeof buf_1);
    c.add(buf_2, sizeof buf_2);
    c.add(buf_3, sizeof buf_3);

    CHECK_EQUAL(5, c.get(0).m_size);
    CHECK_EQUAL(5, c.get(1).m_size);
    CHECK_EQUAL(5, c.get(2).m_size);

    CHECK_EQUAL(StringData(buf_1, sizeof buf_1), c.get(0));
    CHECK_EQUAL(StringData(buf_2, sizeof buf_2), c.get(1));
    CHECK_EQUAL(StringData(buf_3, sizeof buf_3), c.get(2));
}

TEST_FIXTURE(db_setup_string, ArrayStringDestroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
