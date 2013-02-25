#include <UnitTest++.h>
#include <tightdb/column_binary.hpp>

using namespace tightdb;

struct db_setup_column_binary {
    static ColumnBinary c;
};

ColumnBinary db_setup_column_binary::c;

TEST_FIXTURE(db_setup_column_binary, ColumnBinaryMultiEmpty)
{
    c.add("", 0);
    c.add("", 0);
    c.add("", 0);
    c.add(NULL, 0);
    c.add(NULL, 0);
    c.add(NULL, 0);

    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL(0, c.get_size(0));
    CHECK_EQUAL(0, c.get_size(1));
    CHECK_EQUAL(0, c.get_size(2));
    CHECK_EQUAL(0, c.get_size(3));
    CHECK_EQUAL(0, c.get_size(4));
    CHECK_EQUAL(0, c.get_size(5));
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinarySet)
{
    c.Set(0, "hey", 4);

    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL("hey", c.get_data(0));
    CHECK_EQUAL(4, c.get_size(0));
    CHECK_EQUAL(0, c.get_size(1));
    CHECK_EQUAL(0, c.get_size(2));
    CHECK_EQUAL(0, c.get_size(3));
    CHECK_EQUAL(0, c.get_size(4));
    CHECK_EQUAL(0, c.get_size(5));
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinaryAdd)
{
    c.Clear();
    CHECK_EQUAL(0, c.Size());

    c.add("abc", 4);
    CHECK_EQUAL("abc", c.get_data(0)); // single
    CHECK_EQUAL(4, c.get_size(0));
    CHECK_EQUAL(1, c.Size());

    c.add("defg", 5); //non-empty
    CHECK_EQUAL("abc", c.get_data(0));
    CHECK_EQUAL("defg", c.get_data(1));
    CHECK_EQUAL(4, c.get_size(0));
    CHECK_EQUAL(5, c.get_size(1));
    CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinarySet2)
{
    // {shrink, grow} x {first, middle, last, single}
    c.Clear();

    c.add("abc", 4);
    c.Set(0, "de", 3); // shrink single
    CHECK_EQUAL("de", c.get_data(0));
    CHECK_EQUAL(1, c.Size());

    c.Set(0, "abcd", 5); // grow single
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL(1, c.Size());

    c.add("efg", 4);
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL("efg", c.get_data(1));
    CHECK_EQUAL(2, c.Size());

    c.Set(1, "hi", 3); // shrink last
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL("hi", c.get_data(1));
    CHECK_EQUAL(2, c.Size());

    c.Set(1, "jklmno", 7); // grow last
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL("jklmno", c.get_data(1));
    CHECK_EQUAL(2, c.Size());

    c.add("pq", 3);
    c.Set(1, "efghijkl", 9); // grow middle
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL("efghijkl", c.get_data(1));
    CHECK_EQUAL("pq", c.get_data(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(1, "x", 2); // shrink middle
    CHECK_EQUAL("abcd", c.get_data(0));
    CHECK_EQUAL("x", c.get_data(1));
    CHECK_EQUAL("pq", c.get_data(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(0, "qwertyuio", 10); // grow first
    CHECK_EQUAL("qwertyuio", c.get_data(0));
    CHECK_EQUAL("x", c.get_data(1));
    CHECK_EQUAL("pq", c.get_data(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(0, "mno", 4); // shrink first
    CHECK_EQUAL("mno", c.get_data(0));
    CHECK_EQUAL("x", c.get_data(1));
    CHECK_EQUAL("pq", c.get_data(2));
    CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinaryInsert)
{
    c.Clear();

    c.Insert(0, "abc", 4); // single
    CHECK_EQUAL("abc", c.get_data(0));
    CHECK_EQUAL(1, c.Size());

    c.Insert(1, "d", 2); // end
    CHECK_EQUAL("abc", c.get_data(0));
    CHECK_EQUAL("d", c.get_data(1));
    CHECK_EQUAL(2, c.Size());

    c.Insert(2, "ef", 3); // end
    CHECK_EQUAL("abc", c.get_data(0));
    CHECK_EQUAL("d", c.get_data(1));
    CHECK_EQUAL("ef", c.get_data(2));
    CHECK_EQUAL(3, c.Size());

    c.Insert(1, "ghij", 5); // middle
    CHECK_EQUAL("abc", c.get_data(0));
    CHECK_EQUAL("ghij", c.get_data(1));
    CHECK_EQUAL("d", c.get_data(2));
    CHECK_EQUAL("ef", c.get_data(3));
    CHECK_EQUAL(4, c.Size());

    c.Insert(0, "klmno", 6); // first
    CHECK_EQUAL("klmno", c.get_data(0));
    CHECK_EQUAL("abc", c.get_data(1));
    CHECK_EQUAL("ghij", c.get_data(2));
    CHECK_EQUAL("d", c.get_data(3));
    CHECK_EQUAL("ef", c.get_data(4));
    CHECK_EQUAL(5, c.Size());

    c.Insert(2, "as", 3); // middle again
    CHECK_EQUAL("klmno", c.get_data(0));
    CHECK_EQUAL("abc",   c.get_data(1));
    CHECK_EQUAL("as",    c.get_data(2));
    CHECK_EQUAL("ghij",  c.get_data(3));
    CHECK_EQUAL("d",     c.get_data(4));
    CHECK_EQUAL("ef",    c.get_data(5));
    CHECK_EQUAL(6, c.Size());
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinaryDelete)
{
    c.Clear();

    c.add("a", 2);
    c.add("bc", 3);
    c.add("def", 4);
    c.add("ghij", 5);
    c.add("klmno", 6);

    c.Delete(0); // first
    CHECK_EQUAL("bc", c.get_data(0));
    CHECK_EQUAL("def", c.get_data(1));
    CHECK_EQUAL("ghij", c.get_data(2));
    CHECK_EQUAL("klmno", c.get_data(3));
    CHECK_EQUAL(4, c.Size());

    c.Delete(3); // last
    CHECK_EQUAL("bc", c.get_data(0));
    CHECK_EQUAL("def", c.get_data(1));
    CHECK_EQUAL("ghij", c.get_data(2));
    CHECK_EQUAL(3, c.Size());

    c.Delete(1); // middle
    CHECK_EQUAL("bc", c.get_data(0));
    CHECK_EQUAL("ghij", c.get_data(1));
    CHECK_EQUAL(2, c.Size());

    c.Delete(0); // single
    CHECK_EQUAL("ghij", c.get_data(0));
    CHECK_EQUAL(1, c.Size());

    c.Delete(0); // all
    CHECK_EQUAL(0, c.Size());
    CHECK(c.is_empty());
}

TEST_FIXTURE(db_setup_column_binary, ColumnBinary_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
