#include "testsettings.hpp"
#ifdef TEST_COLUMN_BINARY

#include <string>

#include <tightdb/column_binary.hpp>

#include "test.hpp"

using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.


namespace {

struct db_setup_column_binary {
    static ColumnBinary c;
};

ColumnBinary db_setup_column_binary::c;

} // anonymous namespace


TEST(ColumnBinary_MultiEmpty)
{
    ColumnBinary& c = db_setup_column_binary::c;

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

TEST(ColumnBinary_Set)
{
    ColumnBinary& c = db_setup_column_binary::c;

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

TEST(ColumnBinary_Add)
{
    ColumnBinary& c = db_setup_column_binary::c;
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

TEST(ColumnBinary_Set2)
{
    ColumnBinary& c = db_setup_column_binary::c;

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

TEST(ColumnBinary_Insert)
{
    ColumnBinary& c = db_setup_column_binary::c;
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

    c.insert(2, BinaryData("as", 3)); // middle again
    CHECK_EQUAL("klmno", c.get(0).data());
    CHECK_EQUAL("abc",   c.get(1).data());
    CHECK_EQUAL("as",    c.get(2).data());
    CHECK_EQUAL("ghij",  c.get(3).data());
    CHECK_EQUAL("d",     c.get(4).data());
    CHECK_EQUAL("ef",    c.get(5).data());
    CHECK_EQUAL(6, c.size());
}

TEST(ColumnBinary_Delete)
{
    ColumnBinary& c = db_setup_column_binary::c;
    c.clear();

    c.add(BinaryData("a", 2));
    c.add(BinaryData("bc", 3));
    c.add(BinaryData("def", 4));
    c.add(BinaryData("ghij", 5));
    c.add(BinaryData("klmno", 6));

    c.erase(0, 0 == c.size()-1); // first
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("def", c.get(1).data());
    CHECK_EQUAL("ghij", c.get(2).data());
    CHECK_EQUAL("klmno", c.get(3).data());
    CHECK_EQUAL(4, c.size());

    c.erase(3, 3 == c.size()-1); // last
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("def", c.get(1).data());
    CHECK_EQUAL("ghij", c.get(2).data());
    CHECK_EQUAL(3, c.size());

    c.erase(1, 1 == c.size()-1); // middle
    CHECK_EQUAL("bc", c.get(0).data());
    CHECK_EQUAL("ghij", c.get(1).data());
    CHECK_EQUAL(2, c.size());

    c.erase(0, 0 == c.size()-1); // single
    CHECK_EQUAL("ghij", c.get(0).data());
    CHECK_EQUAL(1, c.size());

    c.erase(0, 0 == c.size()-1); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());
}

TEST(ColumnBinary_Big)
{
    ColumnBinary& c = db_setup_column_binary::c;
    c.clear();

    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", 71));
    CHECK_EQUAL("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", c.get(0).data());

    c.clear();
    c.add(BinaryData("a", 2));
    c.add(BinaryData("bc", 3));
    c.add(BinaryData("def", 4));
    c.add(BinaryData("ghij", 5));
    c.add(BinaryData("klmno", 6));
    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", 71));
    CHECK_EQUAL("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ", c.get(5).data());

    // Insert all sizes
    c.clear();
    std::string s;
    for (int i = 0; i < 100; ++i) {
        c.add(BinaryData(s.c_str(), s.size()));
        s += 'x';
    }
    s.clear();
    for (int i = 0; i < 100; ++i) {
        CHECK_EQUAL(BinaryData(s.c_str(), s.size()), c.get(i));
        s += 'x';
    }

    // Set all sizes
    c.clear();
    s.clear();
    for (int i = 0; i < 100; ++i) {
        c.add();
    }
    for (int i = 0; i < 100; ++i) {
        c.set(i, BinaryData(s.c_str(), s.size()));
        s += 'x';
    }
    s.clear();
    for (int i = 0; i < 100; ++i) {
        CHECK_EQUAL(BinaryData(s.c_str(), s.size()), c.get(i));
        s += 'x';
    }
}

TEST(ColumnBinary_Destroy)
{
    ColumnBinary& c = db_setup_column_binary::c;

    // clean up (ALWAYS PUT THIS LAST)
    c.destroy();
}

#endif // TEST_COLUMN_BINARY
