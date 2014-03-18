#include <UnitTest++.h>
#include <tightdb/array_blobs_big.hpp>

using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.


namespace {

struct db_setup_big_blobs {
    static ArrayBigBlobs c;
};

ArrayBigBlobs db_setup_big_blobs::c;

} // anonymous namespace


TEST(ArrayBigBlobs_IsEmpty)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

    CHECK_EQUAL(true, c.is_empty());
}

TEST(ArrayBigBlobs_MultiEmpty)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

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

TEST(ArrayBigBlobs_Set)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

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

TEST(ArrayBigBlobs_Add)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;
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

TEST(ArrayBigBlobs_Set2)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

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

TEST(ArrayBigBlobs_Insert)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;
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

TEST(ArrayBigBlobs_Erase)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;
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

TEST(ArrayBigBlobs_Count)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;
    c.clear();

    // first, middle and end
    c.add(BinaryData("foobar", 7));
    c.add(BinaryData("bar abc", 8));
    c.add(BinaryData("foobar", 7));
    c.add(BinaryData("baz", 4));
    c.add(BinaryData("foobar", 7));

    const size_t count = c.count(BinaryData("foobar", 7));
    CHECK_EQUAL(3, count);

    // str may not be zero-terminated
    const size_t count2 = c.count(BinaryData("foobarx", 6), true);
    CHECK_EQUAL(3, count2);
}

TEST(ArrayBigBlobs_Find)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

    const size_t res = c.find_first(BinaryData("baz", 4));
    CHECK_EQUAL(3, res);

    Array results;
    c.find_all(results, BinaryData("foobar", 7));
    CHECK_EQUAL(3, results.size());

    // str may not be zero-terminated
    const size_t res2 = c.find_first(BinaryData("bazx", 3), true);
    CHECK_EQUAL(3, res2);

    results.clear();
    c.find_all(results, BinaryData("foobarx", 6), true);
    CHECK_EQUAL(3, results.size());

    results.destroy();
}

TEST(ArrayBigBlobs_Destroy)
{
    ArrayBigBlobs& c = db_setup_big_blobs::c;

    // clean up (ALWAYS PUT THIS LAST)
    c.destroy();
}
