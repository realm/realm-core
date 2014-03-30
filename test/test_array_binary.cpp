#include "testsettings.hpp"
#ifdef TEST_ARRAY_BINARY

#include <tightdb/array_binary.hpp>

#include "test.hpp"

using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

namespace {

struct db_setup_binary {
    static ArrayBinary c;
};

ArrayBinary db_setup_binary::c;

} // anonnymous namespace


TEST(ArrayBinary_MultiEmpty)
{
    ArrayBinary& c = db_setup_binary::c;

    c.add(BinaryData("", 0));
    c.add(BinaryData("", 0));
    c.add(BinaryData("", 0));
    c.add(BinaryData());
    c.add(BinaryData());
    c.add(BinaryData());

    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL(0, c.get(0).size());
    CHECK_EQUAL(0, c.get(1).size());
    CHECK_EQUAL(0, c.get(2).size());
    CHECK_EQUAL(0, c.get(3).size());
    CHECK_EQUAL(0, c.get(4).size());
    CHECK_EQUAL(0, c.get(5).size());
}

TEST(ArrayBinary_Set)
{
    ArrayBinary& c = db_setup_binary::c;

    c.set(0, BinaryData("hey", 4));

    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL("hey", c.get(0).data());
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(0, c.get(1).size());
    CHECK_EQUAL(0, c.get(2).size());
    CHECK_EQUAL(0, c.get(3).size());
    CHECK_EQUAL(0, c.get(4).size());
    CHECK_EQUAL(0, c.get(5).size());
}

TEST(ArrayBinary_Add)
{
    ArrayBinary& c = db_setup_binary::c;

    c.clear();
    CHECK_EQUAL(0, c.size());

    c.add(BinaryData("abc", 4));
    CHECK_EQUAL("abc", c.get(0).data()); // single
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("defg", 5)); //non-empty
    CHECK_EQUAL("abc", c.get(0).data());
    CHECK_EQUAL("defg", c.get(1).data());
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(5, c.get(1).size());
    CHECK_EQUAL(2, c.size());
}

TEST(ArrayBinary_Set2)
{
    ArrayBinary& c = db_setup_binary::c;

    // {shrink, grow} x {first, middle, last, single}
    c.clear();

    c.add(BinaryData("abc", 4));
    c.set(0, BinaryData("de", 3)); // shrink single
    CHECK_EQUAL("de", c.get(0).data());
    CHECK_EQUAL(1, c.size());

    c.set(0, BinaryData("abcd", 5)); // grow single
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("efg", 4));
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL("efg", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("hi", 3)); // shrink last
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL("hi", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("jklmno", 7)); // grow last
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL("jklmno", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.add(BinaryData("pq", 3));
    c.set(1, BinaryData("efghijkl", 9)); // grow middle
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL("efghijkl", c.get(1).data());
    CHECK_EQUAL("pq", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.set(1, BinaryData("x", 2)); // shrink middle
    CHECK_EQUAL("abcd", c.get(0).data());
    CHECK_EQUAL("x", c.get(1).data());
    CHECK_EQUAL("pq", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("qwertyuio", 10)); // grow first
    CHECK_EQUAL("qwertyuio", c.get(0).data());
    CHECK_EQUAL("x", c.get(1).data());
    CHECK_EQUAL("pq", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("mno", 4)); // shrink first
    CHECK_EQUAL("mno", c.get(0).data());
    CHECK_EQUAL("x", c.get(1).data());
    CHECK_EQUAL("pq", c.get(2).data());
    CHECK_EQUAL(3, c.size());
}

TEST(ArrayBinary_Insert)
{
    ArrayBinary& c = db_setup_binary::c;
    c.clear();

    c.insert(0, BinaryData("abc", 4)); // single
    CHECK_EQUAL("abc", c.get(0).data());
    CHECK_EQUAL(1, c.size());

    c.insert(1, BinaryData("d", 2)); // end
    CHECK_EQUAL("abc", c.get(0).data());
    CHECK_EQUAL("d", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.insert(2, BinaryData("ef", 3)); // end
    CHECK_EQUAL("abc", c.get(0).data());
    CHECK_EQUAL("d", c.get(1).data());
    CHECK_EQUAL("ef", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.insert(1, BinaryData("ghij", 5)); // middle
    CHECK_EQUAL("abc", c.get(0).data());
    CHECK_EQUAL("ghij", c.get(1).data());
    CHECK_EQUAL("d", c.get(2).data());
    CHECK_EQUAL("ef", c.get(3).data());
    CHECK_EQUAL(4, c.size());

    c.insert(0, BinaryData("klmno", 6)); // first
    CHECK_EQUAL("klmno", c.get(0).data());
    CHECK_EQUAL("abc", c.get(1).data());
    CHECK_EQUAL("ghij", c.get(2).data());
    CHECK_EQUAL("d", c.get(3).data());
    CHECK_EQUAL("ef", c.get(4).data());
    CHECK_EQUAL(5, c.size());
}

TEST(ArrayBinary_Erase)
{
    ArrayBinary& c = db_setup_binary::c;
    c.clear();

    c.add(BinaryData("a", 2));
    c.add(BinaryData("bc", 3));
    c.add(BinaryData("def", 4));
    c.add(BinaryData("ghij", 5));
    c.add(BinaryData("klmno", 6));

    c.erase(0); // first
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("def", c.get(1).data());
    CHECK_EQUAL("ghij", c.get(2).data());
    CHECK_EQUAL("klmno", c.get(3).data());
    CHECK_EQUAL(4, c.size());

    c.erase(3); // last
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("def", c.get(1).data());
    CHECK_EQUAL("ghij", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.erase(1); // middle
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("ghij", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.erase(0); // single
    CHECK_EQUAL("ghij", c.get(0).data());
    CHECK_EQUAL(1, c.size());

    c.erase(0); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());
}

TEST(ArrayBinary_Destroy)
{
    ArrayBinary& c = db_setup_binary::c;

    // clean up (ALWAYS PUT THIS LAST)
    c.destroy();
}

#endif // TEST_ARRAY_BINARY
