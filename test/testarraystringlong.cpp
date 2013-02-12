#include <UnitTest++.h>
#include <tightdb/array_string_long.hpp>

using namespace tightdb;

struct db_setup_string_long {
    static ArrayStringLong c;
};

ArrayStringLong db_setup_string_long::c;

TEST_FIXTURE(db_setup_string_long, ArrayStringLongMultiEmpty)
{
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    c.add("");
    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL("", c.Get(0));
    CHECK_EQUAL("", c.Get(1));
    CHECK_EQUAL("", c.Get(2));
    CHECK_EQUAL("", c.Get(3));
    CHECK_EQUAL("", c.Get(4));
    CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongSet)
{
    c.Set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.Get(0));
    CHECK_EQUAL("", c.Get(1));
    CHECK_EQUAL("", c.Get(2));
    CHECK_EQUAL("", c.Get(3));
    CHECK_EQUAL("", c.Get(4));
    CHECK_EQUAL("", c.Get(5));
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongAdd)
{
    c.Clear();
    CHECK_EQUAL(0, c.size());

    c.add("abc");
    CHECK_EQUAL("abc", c.Get(0)); // single
    CHECK_EQUAL(1, c.size());

    c.add("defg"); //non-empty
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("defg", c.Get(1));
    CHECK_EQUAL(2, c.size());
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongSet2)
{
    // {shrink, grow} x {first, middle, last, single}
    c.Clear();

    c.add("abc");
    c.Set(0, "de"); // shrink single
    CHECK_EQUAL("de", c.Get(0));
    CHECK_EQUAL(1, c.size());

    c.Set(0, "abcd"); // grow single
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL(1, c.size());

    c.add("efg");
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("efg", c.Get(1));
    CHECK_EQUAL(2, c.size());

    c.Set(1, "hi"); // shrink last
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("hi", c.Get(1));
    CHECK_EQUAL(2, c.size());

    c.Set(1, "jklmno"); // grow last
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("jklmno", c.Get(1));
    CHECK_EQUAL(2, c.size());

    c.add("pq");
    c.Set(1, "efghijkl"); // grow middle
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("efghijkl", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.size());

    c.Set(1, "x"); // shrink middle
    CHECK_EQUAL("abcd", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.size());

    c.Set(0, "qwertyuio"); // grow first
    CHECK_EQUAL("qwertyuio", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.size());

    c.Set(0, "mno"); // shrink first
    CHECK_EQUAL("mno", c.Get(0));
    CHECK_EQUAL("x", c.Get(1));
    CHECK_EQUAL("pq", c.Get(2));
    CHECK_EQUAL(3, c.size());
}


TEST_FIXTURE(db_setup_string_long, ArrayStringLongInsert)
{
    c.Clear();

    c.Insert(0, "abc"); // single
    CHECK_EQUAL(c.Get(0), "abc");
    CHECK_EQUAL(1, c.size());

    c.Insert(1, "d"); // end
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("d", c.Get(1));
    CHECK_EQUAL(2, c.size());

    c.Insert(2, "ef"); // end
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("d", c.Get(1));
    CHECK_EQUAL("ef", c.Get(2));
    CHECK_EQUAL(3, c.size());

    c.Insert(1, "ghij"); // middle
    CHECK_EQUAL("abc", c.Get(0));
    CHECK_EQUAL("ghij", c.Get(1));
    CHECK_EQUAL("d", c.Get(2));
    CHECK_EQUAL("ef", c.Get(3));
    CHECK_EQUAL(4, c.size());

    c.Insert(0, "klmno"); // first
    CHECK_EQUAL("klmno", c.Get(0));
    CHECK_EQUAL("abc", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL("d", c.Get(3));
    CHECK_EQUAL("ef", c.Get(4));
    CHECK_EQUAL(5, c.size());
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongDelete)
{
    c.Clear();

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmno");

    c.Delete(0); // first
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("def", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL("klmno", c.Get(3));
    CHECK_EQUAL(4, c.size());

    c.Delete(3); // last
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("def", c.Get(1));
    CHECK_EQUAL("ghij", c.Get(2));
    CHECK_EQUAL(3, c.size());

    c.Delete(1); // middle
    CHECK_EQUAL("bc", c.Get(0));
    CHECK_EQUAL("ghij", c.Get(1));
    CHECK_EQUAL(2, c.size());

    c.Delete(0); // single
    CHECK_EQUAL("ghij", c.Get(0));
    CHECK_EQUAL(1, c.size());

    c.Delete(0); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongFind)
{
    c.Clear();

    c.add("a");
    c.add("bc iu");
    c.add("def");
    c.add("ghij uihi i ih iu huih ui");
    c.add("klmno hiuh iuh uih i huih i biuhui");

    size_t res1 = c.find_first("");
    CHECK_EQUAL((size_t)-1, res1);

    size_t res2 = c.find_first("xlmno hiuh iuh uih i huih i biuhui");
    CHECK_EQUAL((size_t)-1, res2);

    size_t res3 = c.find_first("ghij uihi i ih iu huih ui");
    CHECK_EQUAL(3, res3);
}

TEST_FIXTURE(db_setup_string_long, ArrayStringLongCount)
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


TEST_FIXTURE(db_setup_string_long, ArrayStringLong_Destroy)
{
    // clean up (ALWAYS PUT THIS LAST)
    c.Destroy();
}
