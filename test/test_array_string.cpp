#include "testsettings.hpp"
#ifdef TEST_ARRAY_STRING

#include <vector>

#include <tightdb/array_string.hpp>
#include <tightdb/column.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;

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

TEST(ArrayString_Basic)
{
    ArrayString c(Allocator::get_default());
    c.create();

    // TEST(ArrayString_MultiEmpty)

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


    // TEST(ArrayString_SetEmpty1)

    c.set(0, "");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ArrayString_Erase0)

    c.erase(5);


    // TEST(ArrayString_Insert0)

    // Intention: Insert a non-empty string into an array that is not
    // empty but contains only empty strings (and only ever have
    // contained empty strings). The insertion is not at the end of
    // the array.
    c.insert(0, "x");


    // TEST(ArrayString_SetEmpty2)

    c.set(0, "");
    c.set(5, "");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ArrayString_Clear)

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


    // TEST(ArrayString_Find1)

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("", c.get(0));
    // Intention: Search for strings in an array that is not empty but
    // contains only empty strings (and only ever have contained empty
    // strings).
    CHECK_EQUAL(0, c.find_first(""));
    CHECK_EQUAL(size_t(-1), c.find_first("x"));
    CHECK_EQUAL(5, c.find_first("", 5));
    CHECK_EQUAL(size_t(-1), c.find_first("", 6));


    // TEST(ArrayString_SetExpand4)

    c.set(0, "hey");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ArrayString_Find2)

    // Intention: Search for non-empty string P that is not in then
    // array, but the array does contain a string where P is a prefix.
    CHECK_EQUAL(size_t(-1), c.find_first("he"));


    // TEST(ArrayString_SetExpand8)

    c.set(1, "test");

    CHECK_EQUAL(6, c.size());
    CHECK_EQUAL("hey", c.get(0));
    CHECK_EQUAL("test", c.get(1));
    CHECK_EQUAL("", c.get(2));
    CHECK_EQUAL("", c.get(3));
    CHECK_EQUAL("", c.get(4));
    CHECK_EQUAL("", c.get(5));


    // TEST(ArrayString_Add0)

    c.clear();

    c.add();
    CHECK_EQUAL("", c.get(0));
    CHECK_EQUAL(1, c.size());


    // TEST(ArrayString_Add1)

    c.add("a");
    CHECK_EQUAL("",  c.get(0));
    CHECK_EQUAL("a", c.get(1));
    CHECK_EQUAL(2, c.size());


    // TEST(ArrayString_Add2)

    c.add("bb");
    CHECK_EQUAL("",   c.get(0));
    CHECK_EQUAL("a",  c.get(1));
    CHECK_EQUAL("bb", c.get(2));
    CHECK_EQUAL(3, c.size());


    // TEST(ArrayString_Add3)

    c.add("ccc");
    CHECK_EQUAL("",    c.get(0));
    CHECK_EQUAL("a",   c.get(1));
    CHECK_EQUAL("bb",  c.get(2));
    CHECK_EQUAL("ccc", c.get(3));
    CHECK_EQUAL(4, c.size());


    // TEST(ArrayString_Add4)

    c.add("dddd");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL(5, c.size());


    // TEST(ArrayString_Add8)

    c.add("eeeeeeee");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ArrayString_Add16)

    c.add("ffffffffffffffff");
    CHECK_EQUAL("",     c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("bb",   c.get(2));
    CHECK_EQUAL("ccc",  c.get(3));
    CHECK_EQUAL("dddd", c.get(4));
    CHECK_EQUAL("eeeeeeee", c.get(5));
    CHECK_EQUAL("ffffffffffffffff", c.get(6));
    CHECK_EQUAL(7, c.size());


    // TEST(ArrayString_Add32)

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


    // TEST(ArrayString_Set1)

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


    // TEST(ArrayString_Insert1)

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


    // TEST(ArrayString_Erase1)

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


    // TEST(ArrayString_Erase2)

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


    // TEST(ArrayString_Erase3)

    // Erase from middle
    c.erase(3);

    CHECK_EQUAL("bb",   c.get(0));
    CHECK_EQUAL("a",    c.get(1));
    CHECK_EQUAL("",     c.get(2));
    CHECK_EQUAL("dddd", c.get(3));
    CHECK_EQUAL("eeeeeeee", c.get(4));
    CHECK_EQUAL("ffffffffffffffff", c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ArrayString_EraseAll)

    // Erase all items one at a time
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);
    c.erase(0);

    CHECK(c.is_empty());
    CHECK_EQUAL(0, c.size());


    // TEST(ArrayString_Insert2)

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


    // TEST(ArrayString_Insert3)

    // Insert in middle with expansion
    c.insert(3, "xxxxxxxxxx");

    CHECK_EQUAL("xxxxx", c.get(0));
    CHECK_EQUAL("a",     c.get(1));
    CHECK_EQUAL("b",     c.get(2));
    CHECK_EQUAL("xxxxxxxxxx", c.get(3));
    CHECK_EQUAL("c",     c.get(4));
    CHECK_EQUAL("d",     c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(ArrayString_Find3)

    // Create new list
    c.clear();
    c.add("a");
    c.add("b");
    c.add("c");
    c.add("d");

    // Search for last item (4 bytes width)
    CHECK_EQUAL(3, c.find_first("d"));


    // TEST(ArrayString_Find4)

    // Expand to 8 bytes width
    c.add("eeeeee");

    // Search for last item
    CHECK_EQUAL(4, c.find_first("eeeeee"));


    // TEST(ArrayString_Find5)

    // Expand to 16 bytes width
    c.add("ffffffffffff");

    // Search for last item
    CHECK_EQUAL(5, c.find_first("ffffffffffff"));


    // TEST(ArrayString_Find6)

    // Expand to 32 bytes width
    c.add("gggggggggggggggggggggggg");

    // Search for last item
    CHECK_EQUAL(6, c.find_first("gggggggggggggggggggggggg"));


    // TEST(ArrayString_Find7)

    // Expand to 64 bytes width
    c.add("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");

    // Search for last item
    CHECK_EQUAL(7, c.find_first("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"));


    // TEST(ArrayString_FindAll)

    c.clear();

    ref_type results_ref = Column::create(Allocator::get_default());
    Column results(Allocator::get_default(), results_ref);

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    c.find_all(results, "foobar");
    CHECK_EQUAL(3, results.size());
    CHECK_EQUAL(0, results.get(0));
    CHECK_EQUAL(2, results.get(1));
    CHECK_EQUAL(4, results.get(2));

    // Cleanup
    results.destroy();


    // TEST(ArrayString_Count)

    c.clear();

    // first, middle and end
    c.add("foobar");
    c.add("bar abc");
    c.add("foobar");
    c.add("baz");
    c.add("foobar");

    CHECK_EQUAL(3, c.count("foobar"));


    // TEST(ArrayString_WithZeroBytes)

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


    // TEST(ArrayString_Destroy)

    c.destroy();
}


TEST(ArrayString_Null)
{
    if (NULLS) {
        {
            ArrayString a(Allocator::get_default());
            a.create();

            a.add("");
            size_t t = a.find_first("");
            CHECK_EQUAL(t, 0);

            a.destroy();
        }

        {
        ArrayString a(Allocator::get_default());
        a.create();

        a.add("foo");
        a.add("");
        a.add(StringData(0, 0)); // add null (StringData::data() == 0)

        CHECK_EQUAL(a.is_null(0), false);
        CHECK_EQUAL(a.is_null(1), false);
        CHECK_EQUAL(a.is_null(2), true);
        CHECK(a.get(0) == "foo");

        // Test set
        a.set_null(0);
        a.set_null(1);
        a.set_null(2);
        CHECK_EQUAL(a.is_null(1), true);
        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(2), true);

        a.destroy();
    }

        {
            ArrayString a(Allocator::get_default());
            a.create();

            a.add(StringData(0, 0));  // add null (StringData::data() == 0)
            a.add("");
            a.add("foo");

            CHECK_EQUAL(a.is_null(0), true);
            CHECK_EQUAL(a.is_null(1), false);
            CHECK_EQUAL(a.is_null(2), false);
            CHECK(a.get(2) == "foo");

            // Test insert
            a.insert(0, StringData(0, 0)); // add null (StringData::data() == 0)
            a.insert(2, StringData(0, 0)); // add null (StringData::data() == 0)
            a.insert(4, StringData(0, 0)); // add null (StringData::data() == 0)

            CHECK_EQUAL(a.is_null(0), true);
            CHECK_EQUAL(a.is_null(1), true);
            CHECK_EQUAL(a.is_null(2), true);
            CHECK_EQUAL(a.is_null(3), false);
            CHECK_EQUAL(a.is_null(4), true);
            CHECK_EQUAL(a.is_null(5), false);

            a.destroy();
        }

        {
            ArrayString a(Allocator::get_default());
            a.create();

            a.add("");
            a.add(StringData(0, 0));
            a.add("foo");

            CHECK_EQUAL(a.is_null(0), false);
            CHECK_EQUAL(a.is_null(1), true);
            CHECK_EQUAL(a.is_null(2), false);
            CHECK(a.get(2) == "foo");

            a.erase(0);
            CHECK_EQUAL(a.is_null(0), true);
            CHECK_EQUAL(a.is_null(1), false);

            a.erase(0);
            CHECK_EQUAL(a.is_null(0), false);

            a.destroy();
        }

        Random random(random_int<unsigned long>());

        for (size_t t = 0; t < 50; t++) {
            ArrayString a(Allocator::get_default());
            a.create();

            // vector that is kept in sync with the ArrayString so that we can compare with it
            vector<string> v;

            // ArrayString capacity starts at 128 bytes, so we need lots of elements
            // to test if relocation works
            for (size_t i = 0; i < 100; i++) {
                unsigned char rnd = random.draw_int<unsigned char>();  //    = 1234 * ((i + 123) * (t + 432) + 423) + 543;

                // Add more often than removing, so that we grow
                if (rnd < 80 && a.size() > 0) {
                    size_t del = rnd % a.size();
                    a.erase(del);
                    v.erase(v.begin() + del);
                }
                else {
                    // Generate string with good probability of being empty or null
                    static const char str[] = "This is a test of null strings";
                    size_t len;

                    if (random.draw_int<unsigned char>() > 100)
                        len = rnd % 15;
                    else
                        len = 0;

                    StringData sd;
                    string stdstr;

                    if (random.draw_int<unsigned char>() > 100) {
                        sd = StringData(0, 0);
                        stdstr = "null";
                    }
                    else {
                        sd = StringData(str, len);
                        stdstr = string(str, len);
                    }

                    if (random.draw_int<unsigned char>() > 100) {
                        a.add(sd);
                        v.push_back(stdstr);
                    }
                    else if (a.size() > 0) {
                        size_t pos = rnd % a.size();
                        a.insert(pos, sd);
                        v.insert(v.begin() + pos, stdstr);
                    }

                    CHECK_EQUAL(a.size(), v.size());
                    for (size_t i = 0; i < a.size(); i++) {
                        if (v[i] == "null") {
                            CHECK(a.is_null(i));
                            CHECK(a.get(i).data() == 0);
                        }
                        else {
                            CHECK(a.get(i) == v[i]);
                        }
                    }
                }
            }
            a.destroy();
        }
    }
}


TEST(ArrayString_Compare)
{
    ArrayString a(Allocator::get_default()), b(Allocator::get_default());
    a.create();
    b.create();

    CHECK(a.compare_string(b));
    a.add("");
    CHECK(!a.compare_string(b));
    b.add("x");
    CHECK(!a.compare_string(b));
    a.set(0, "x");
    CHECK(a.compare_string(b));

    a.destroy();
    b.destroy();
}


#endif // TEST_ARRAY_STRING
