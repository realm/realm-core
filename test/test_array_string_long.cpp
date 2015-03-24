#include "testsettings.hpp"
#ifdef TEST_ARRAY_STRING_LONG

#include <realm/array_string_long.hpp>

#include "test.hpp"

using namespace realm;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(ArrayStringLong_Basic)
{
    ArrayStringLong c(Allocator::get_default());
    c.create();

    // TEST(ArrayStringLong_MultiEmpty)

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


    // TEST(ArrayStringLong_Set)

    c.set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ArrayStringLong_Add)

    c.clear();

    CHECK_EQUAL(0, c.size());

    c.add("abc");
    CHECK_EQUAL("abc", c.get(0)); // single
    CHECK_EQUAL(1, c.size());

    c.add("defg"); //non-empty
    CHECK_EQUAL("abc", c.get(0));
    CHECK_EQUAL("defg", c.get(1));
    CHECK_EQUAL(2, c.size());


    // TEST(ArrayStringLong_Set2)

    // {shrink, grow} x {first, middle, last, single}
    c.clear();

    c.add("abc");
    c.set(0, "de"); // shrink single
    CHECK_EQUAL("de", c.get(0));
    CHECK_EQUAL(1, c.size());

    c.set(0, "abcd"); // grow single
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL(1, c.size());

    c.add("efg");
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL("efg", c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, "hi"); // shrink last
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL("hi", c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, "jklmno"); // grow last
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL("jklmno", c.get(1));
    CHECK_EQUAL(2, c.size());

    c.add("pq");
    c.set(1, "efghijkl"); // grow middle
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL("efghijkl", c.get(1));
    CHECK_EQUAL("pq", c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(1, "x"); // shrink middle
    CHECK_EQUAL("abcd", c.get(0));
    CHECK_EQUAL("x", c.get(1));
    CHECK_EQUAL("pq", c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, "qwertyuio"); // grow first
    CHECK_EQUAL("qwertyuio", c.get(0));
    CHECK_EQUAL("x", c.get(1));
    CHECK_EQUAL("pq", c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, "mno"); // shrink first
    CHECK_EQUAL("mno", c.get(0));
    CHECK_EQUAL("x", c.get(1));
    CHECK_EQUAL("pq", c.get(2));
    CHECK_EQUAL(3, c.size());


    // TEST(ArrayStringLong_Insert)

    c.clear();

    c.insert(0, "abc"); // single
    CHECK_EQUAL(c.get(0), "abc");
    CHECK_EQUAL(1, c.size());

    c.insert(1, "d"); // end
    CHECK_EQUAL("abc", c.get(0));
    CHECK_EQUAL("d", c.get(1));
    CHECK_EQUAL(2, c.size());

    c.insert(2, "ef"); // end
    CHECK_EQUAL("abc", c.get(0));
    CHECK_EQUAL("d", c.get(1));
    CHECK_EQUAL("ef", c.get(2));
    CHECK_EQUAL(3, c.size());

    c.insert(1, "ghij"); // middle
    CHECK_EQUAL("abc", c.get(0));
    CHECK_EQUAL("ghij", c.get(1));
    CHECK_EQUAL("d", c.get(2));
    CHECK_EQUAL("ef", c.get(3));
    CHECK_EQUAL(4, c.size());

    c.insert(0, "klmno"); // first
    CHECK_EQUAL("klmno", c.get(0));
    CHECK_EQUAL("abc", c.get(1));
    CHECK_EQUAL("ghij", c.get(2));
    CHECK_EQUAL("d", c.get(3));
    CHECK_EQUAL("ef", c.get(4));
    CHECK_EQUAL(5, c.size());


    // TEST(ArrayStringLong_Delete)

    c.clear();

    c.add("a");
    c.add("bc");
    c.add("def");
    c.add("ghij");
    c.add("klmno");

    c.erase(0); // first
    CHECK_EQUAL("bc", c.get(0));
    CHECK_EQUAL("def", c.get(1));
    CHECK_EQUAL("ghij", c.get(2));
    CHECK_EQUAL("klmno", c.get(3));
    CHECK_EQUAL(4, c.size());

    c.erase(3); // last
    CHECK_EQUAL("bc", c.get(0));
    CHECK_EQUAL("def", c.get(1));
    CHECK_EQUAL("ghij", c.get(2));
    CHECK_EQUAL(3, c.size());

    c.erase(1); // middle
    CHECK_EQUAL("bc", c.get(0));
    CHECK_EQUAL("ghij", c.get(1));
    CHECK_EQUAL(2, c.size());

    c.erase(0); // single
    CHECK_EQUAL("ghij", c.get(0));
    CHECK_EQUAL(1, c.size());

    c.erase(0); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());


    // TEST(ArrayStringLong_Find)

    c.clear();

    c.add("a");
    c.add("bc iu");
    c.add("def");
    c.add("ghij uihi i ih iu huih ui");
    c.add("klmno hiuh iuh uih i huih i biuhui");

    CHECK_EQUAL(size_t(-1), c.find_first(""));

    CHECK_EQUAL(size_t(-1), c.find_first("xlmno hiuh iuh uih i huih i biuhui"));

    CHECK_EQUAL(3, c.find_first("ghij uihi i ih iu huih ui"));


    // TEST(ArrayStringLong_Count)

    c.clear();

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    CHECK_EQUAL(3, c.count("foobar"));


    // TEST(ArrayStringLong_Destroy)

    c.destroy();
}

#endif // TEST_ARRAY_STRING_LONG
