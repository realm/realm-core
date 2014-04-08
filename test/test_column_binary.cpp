#include "testsettings.hpp"
#ifdef TEST_COLUMN_BINARY

#include <string>

#include <tightdb/column_binary.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.


TEST(ColumnBinary_Basic)
{
    ColumnBinary c;

    // TEST(ColumnBinary_MultiEmpty)

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


    // TEST(ColumnBinary_Set)

    c.set(0, BinaryData("hey"));

    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL(BinaryData("hey"), c.get(0));
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(0, c.get(1).size());
    CHECK_EQUAL(0, c.get(2).size());
    CHECK_EQUAL(0, c.get(3).size());
    CHECK_EQUAL(0, c.get(4).size());
    CHECK_EQUAL(0, c.get(5).size());


    // TEST(ColumnBinary_Add)

    c.clear();

    CHECK_EQUAL(0, c.size());

    c.add(BinaryData("abc"));
    CHECK_EQUAL(BinaryData("abc"),       c.get(0)); // single
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("defg")); //non-empty
    CHECK_EQUAL(BinaryData("abc"),       c.get(0));
    CHECK_EQUAL(BinaryData("defg"),      c.get(1));
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(5, c.get(1).size());
    CHECK_EQUAL(2, c.size());


    // TEST(ColumnBinary_Set2)

    // {shrink, grow} x {first, middle, last, single}
    c.clear();

    c.add(BinaryData("abc"));
    c.set(0, BinaryData("de", 3)); // shrink single
    CHECK_EQUAL(BinaryData("de"),        c.get(0));
    CHECK_EQUAL(1, c.size());

    c.set(0, BinaryData("abcd")); // grow single
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("efg"));
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(BinaryData("efg"),       c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("hi")); // shrink last
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(BinaryData("hi"),        c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("jklmno")); // grow last
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(BinaryData("jklmno"),    c.get(1));
    CHECK_EQUAL(2, c.size());

    c.add(BinaryData("pq"));
    c.set(1, BinaryData("efghijkl")); // grow middle
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(BinaryData("efghijkl"),  c.get(1));
    CHECK_EQUAL(BinaryData("pq"),        c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(1, BinaryData("x")); // shrink middle
    CHECK_EQUAL(BinaryData("abcd"),      c.get(0));
    CHECK_EQUAL(BinaryData("x"),         c.get(1));
    CHECK_EQUAL(BinaryData("pq"),        c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("qwertyuio")); // grow first
    CHECK_EQUAL(BinaryData("qwertyuio"), c.get(0));
    CHECK_EQUAL(BinaryData("x"),         c.get(1));
    CHECK_EQUAL(BinaryData("pq"),        c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("mno")); // shrink first
    CHECK_EQUAL(BinaryData("mno"),       c.get(0));
    CHECK_EQUAL(BinaryData("x"),         c.get(1));
    CHECK_EQUAL(BinaryData("pq"),        c.get(2));
    CHECK_EQUAL(3, c.size());


    // TEST(ColumnBinary_Insert)

    c.clear();

    c.insert(0, BinaryData("abc")); // single
    CHECK_EQUAL(BinaryData("abc"),       c.get(0));
    CHECK_EQUAL(1, c.size());

    c.insert(1, BinaryData("d")); // end
    CHECK_EQUAL(BinaryData("abc"),       c.get(0));
    CHECK_EQUAL(BinaryData("d"),         c.get(1));
    CHECK_EQUAL(2, c.size());

    c.insert(2, BinaryData("ef")); // end
    CHECK_EQUAL(BinaryData("abc"),       c.get(0));
    CHECK_EQUAL(BinaryData("d"),         c.get(1));
    CHECK_EQUAL(BinaryData("ef"),        c.get(2));
    CHECK_EQUAL(3, c.size());

    c.insert(1, BinaryData("ghij")); // middle
    CHECK_EQUAL(BinaryData("abc"),       c.get(0));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(1));
    CHECK_EQUAL(BinaryData("d"),         c.get(2));
    CHECK_EQUAL(BinaryData("ef"),        c.get(3));
    CHECK_EQUAL(4, c.size());

    c.insert(0, BinaryData("klmno")); // first
    CHECK_EQUAL(BinaryData("klmno"),     c.get(0));
    CHECK_EQUAL(BinaryData("abc"),       c.get(1));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(2));
    CHECK_EQUAL(BinaryData("d"),         c.get(3));
    CHECK_EQUAL(BinaryData("ef"),        c.get(4));
    CHECK_EQUAL(5, c.size());

    c.insert(2, BinaryData("as")); // middle again
    CHECK_EQUAL(BinaryData("klmno"),     c.get(0));
    CHECK_EQUAL(BinaryData("abc"),       c.get(1));
    CHECK_EQUAL(BinaryData("as"),        c.get(2));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(3));
    CHECK_EQUAL(BinaryData("d"),         c.get(4));
    CHECK_EQUAL(BinaryData("ef"),        c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ColumnBinary_Delete)

    c.clear();

    c.add(BinaryData("a"));
    c.add(BinaryData("bc"));
    c.add(BinaryData("def"));
    c.add(BinaryData("ghij"));
    c.add(BinaryData("klmno"));

    c.erase(0, 0 == c.size()-1); // first
    CHECK_EQUAL(BinaryData("bc"),        c.get(0));
    CHECK_EQUAL(BinaryData("def"),       c.get(1));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(2));
    CHECK_EQUAL(BinaryData("klmno"),     c.get(3));
    CHECK_EQUAL(4, c.size());

    c.erase(3, 3 == c.size()-1); // last
    CHECK_EQUAL(BinaryData("bc"),        c.get(0));
    CHECK_EQUAL(BinaryData("def"),       c.get(1));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(2));
    CHECK_EQUAL(3, c.size());

    c.erase(1, 1 == c.size()-1); // middle
    CHECK_EQUAL(BinaryData("bc"),        c.get(0));
    CHECK_EQUAL(BinaryData("ghij"),      c.get(1));
    CHECK_EQUAL(2, c.size());

    c.erase(0, 0 == c.size()-1); // single
    CHECK_EQUAL(BinaryData("ghij"),      c.get(0));
    CHECK_EQUAL(1, c.size());

    c.erase(0, 0 == c.size()-1); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());


    // TEST(ColumnBinary_Big)

    c.clear();

    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  "));
    CHECK_EQUAL(BinaryData("70 chars  70 chars  70 chars  70 chars  "
                           "70 chars  70 chars  70 chars  "), c.get(0));

    c.clear();
    c.add(BinaryData("a"));
    c.add(BinaryData("bc"));
    c.add(BinaryData("def"));
    c.add(BinaryData("ghij"));
    c.add(BinaryData("klmno"));
    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  "));
    CHECK_EQUAL(BinaryData("70 chars  70 chars  70 chars  70 chars  "
                           "70 chars  70 chars  70 chars  "), c.get(5));

    // Insert all sizes
    c.clear();
    string s;
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
    for (int i = 0; i < 100; ++i)
        c.add();
    for (int i = 0; i < 100; ++i) {
        c.set(i, BinaryData(s.c_str(), s.size()));
        s += 'x';
    }
    s.clear();
    for (int i = 0; i < 100; ++i) {
        CHECK_EQUAL(BinaryData(s.c_str(), s.size()), c.get(i));
        s += 'x';
    }


    // TEST(ColumnBinary_Destroy)

    c.destroy();
}

#endif // TEST_COLUMN_BINARY
