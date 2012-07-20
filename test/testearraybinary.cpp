#include <UnitTest++.h>
#include <tightdb/array_binary.hpp>

using namespace tightdb;

struct db_setup_binary {
    static ArrayBinary c;
};

ArrayBinary db_setup_binary::c;

TEST_FIXTURE(db_setup_binary, ArrayBinaryMultiEmpty)
{
    c.add("", 0);
    c.add("", 0);
    c.add("", 0);
    c.add(NULL, 0);
    c.add(NULL, 0);
    c.add(NULL, 0);

    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL(0, c.GetLen(0));
    CHECK_EQUAL(0, c.GetLen(1));
    CHECK_EQUAL(0, c.GetLen(2));
    CHECK_EQUAL(0, c.GetLen(3));
    CHECK_EQUAL(0, c.GetLen(4));
    CHECK_EQUAL(0, c.GetLen(5));
}

TEST_FIXTURE(db_setup_binary, ArrayBinarySet)
{
    c.Set(0, "hey", 4);

    CHECK_EQUAL(6, c.Size());

    CHECK_EQUAL("hey", c.Get(0));
    CHECK_EQUAL(4, c.GetLen(0));
    CHECK_EQUAL(0, c.GetLen(1));
    CHECK_EQUAL(0, c.GetLen(2));
    CHECK_EQUAL(0, c.GetLen(3));
    CHECK_EQUAL(0, c.GetLen(4));
    CHECK_EQUAL(0, c.GetLen(5));
}

TEST_FIXTURE(db_setup_binary, ArrayBinaryAdd)
{
    c.Clear();
    CHECK_EQUAL(0, c.Size());

    c.add("abc", 4);
    CHECK_EQUAL("abc", c.Get(0)); // single
    CHECK_EQUAL(4, c.GetLen(0));
    CHECK_EQUAL(1, c.Size());

    c.add("defg", 5); //non-empty
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("defg", c.Get(1));
    CHECK_EQUAL(4, c.GetLen(0));
    CHECK_EQUAL(5, c.GetLen(1));
    CHECK_EQUAL(2, c.Size());
}

TEST_FIXTURE(db_setup_binary, ArrayBinarySet2)
{
    // {shrink, grow} x {first, middle, last, single}
    c.Clear();

    c.add("abc", 4);
    c.Set(0, "de", 3); // shrink single
    CHECK_EQUAL("de", c.Get(0));
    CHECK_EQUAL(1, c.Size());

    c.Set(0, "abcd", 5); // grow single
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL(1, c.Size());

    c.add("efg", 4);
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("efg", c.Get(1));
    CHECK_EQUAL(2, c.Size());

    c.Set(1, "hi", 3); // shrink last
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("hi", c.Get(1));
    CHECK_EQUAL(2, c.Size());

    c.Set(1, "jklmno", 7); // grow last
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("jklmno", c.Get(1));
    CHECK_EQUAL(2, c.Size());

    c.add("pq", 3);
    c.Set(1, "efghijkl", 9); // grow middle
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("efghijkl", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(1, "x", 2); // shrink middle
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(0, "qwertyuio", 10); // grow first
    CHECK_EQUAL("qwertyuio", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.Size());

    c.Set(0, "mno", 4); // shrink first
    CHECK_EQUAL("mno", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.Size());
}

TEST_FIXTURE(db_setup_binary, ArrayBinaryInsert)
{
    c.Clear();

    c.Insert(0, "abc", 4); // single
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL(1, c.Size());

    c.Insert(1, "d", 2); // end
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("d", c.Get(1));
    CHECK_EQUAL(2, c.Size());

    c.Insert(2, "ef", 3); // end
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("d", c.Get(1));
    CHECK_EQUAL("ef", c.Get(2));
    CHECK_EQUAL(3, c.Size());

    c.Insert(1, "ghij", 5); // middle
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("ghij", c.Get(1));
    CHECK_EQUAL("d", c.Get(2));
    CHECK_EQUAL("ef", c.Get(3));
    CHECK_EQUAL(4, c.Size());

    c.Insert(0, "klmno", 6); // first
    CHECK_EQUAL("klmno", c.Get(0));
    CHECK_EQUAL("abc", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL("d", c.Get(3));
    CHECK_EQUAL("ef", c.Get(4));
    CHECK_EQUAL(5, c.Size());
}

TEST_FIXTURE(db_setup_binary, ArrayBinaryDelete)
{
    c.Clear();

    c.add("a", 2);
    c.add("bc", 3);
    c.add("def", 4);
    c.add("ghij", 5);
    c.add("klmno", 6);

    c.Delete(0); // first
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("def", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL("klmno", c.Get(3));
    CHECK_EQUAL(4, c.Size());

    c.Delete(3); // last
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("def", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL(3, c.Size());

    c.Delete(1); // middle
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("ghij", c.Get(1));
    CHECK_EQUAL(2, c.Size());

    c.Delete(0); // single
    CHECK_EQUAL("ghij", c.Get(0));
    CHECK_EQUAL(1, c.Size());

    c.Delete(0); // all
    CHECK_EQUAL(0, c.Size());
    CHECK(c.is_empty());
}

TEST_FIXTURE(db_setup_binary, ArrayBinary_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
