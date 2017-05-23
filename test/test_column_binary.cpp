/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_COLUMN_BINARY

#include <string>

#include <realm/column_binary.hpp>

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


TEST(BinaryColumn_Basic)
{
    ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
    BinaryColumn c(Allocator::get_default(), ref, true);

    // TEST(BinaryColumn_MultiEmpty)

    c.add(BinaryData());
    c.add(BinaryData());
    c.add(BinaryData());
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


    // TEST(BinaryColumn_Set)

    c.set(0, BinaryData("hey"));

    CHECK_EQUAL(6, c.size());

    CHECK_EQUAL(BinaryData("hey"), c.get(0));
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(0, c.get(1).size());
    CHECK_EQUAL(0, c.get(2).size());
    CHECK_EQUAL(0, c.get(3).size());
    CHECK_EQUAL(0, c.get(4).size());
    CHECK_EQUAL(0, c.get(5).size());


    // TEST(BinaryColumn_Add)

    c.clear();

    CHECK_EQUAL(0, c.size());

    c.add(BinaryData("abc"));
    CHECK_EQUAL(BinaryData("abc"), c.get(0)); // single
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("defg")); // non-empty
    CHECK_EQUAL(BinaryData("abc"), c.get(0));
    CHECK_EQUAL(BinaryData("defg"), c.get(1));
    CHECK_EQUAL(4, c.get(0).size());
    CHECK_EQUAL(5, c.get(1).size());
    CHECK_EQUAL(2, c.size());


    // TEST(BinaryColumn_Set2)

    // {shrink, grow} x {first, middle, last, single}
    c.clear();

    c.add(BinaryData("abc"));
    c.set(0, BinaryData("de", 3)); // shrink single
    CHECK_EQUAL(BinaryData("de"), c.get(0));
    CHECK_EQUAL(1, c.size());

    c.set(0, BinaryData("abcd")); // grow single
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(1, c.size());

    c.add(BinaryData("efg"));
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(BinaryData("efg"), c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("hi")); // shrink last
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(BinaryData("hi"), c.get(1));
    CHECK_EQUAL(2, c.size());

    c.set(1, BinaryData("jklmno")); // grow last
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(BinaryData("jklmno"), c.get(1));
    CHECK_EQUAL(2, c.size());

    c.add(BinaryData("pq"));
    c.set(1, BinaryData("efghijkl")); // grow middle
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(BinaryData("efghijkl"), c.get(1));
    CHECK_EQUAL(BinaryData("pq"), c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(1, BinaryData("x")); // shrink middle
    CHECK_EQUAL(BinaryData("abcd"), c.get(0));
    CHECK_EQUAL(BinaryData("x"), c.get(1));
    CHECK_EQUAL(BinaryData("pq"), c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("qwertyuio")); // grow first
    CHECK_EQUAL(BinaryData("qwertyuio"), c.get(0));
    CHECK_EQUAL(BinaryData("x"), c.get(1));
    CHECK_EQUAL(BinaryData("pq"), c.get(2));
    CHECK_EQUAL(3, c.size());

    c.set(0, BinaryData("mno")); // shrink first
    CHECK_EQUAL(BinaryData("mno"), c.get(0));
    CHECK_EQUAL(BinaryData("x"), c.get(1));
    CHECK_EQUAL(BinaryData("pq"), c.get(2));
    CHECK_EQUAL(3, c.size());


    // TEST(BinaryColumn_Insert)

    c.clear();

    c.insert(0, BinaryData("abc")); // single
    CHECK_EQUAL(BinaryData("abc"), c.get(0));
    CHECK_EQUAL(1, c.size());

    c.insert(1, BinaryData("d")); // end
    CHECK_EQUAL(BinaryData("abc"), c.get(0));
    CHECK_EQUAL(BinaryData("d"), c.get(1));
    CHECK_EQUAL(2, c.size());

    c.insert(2, BinaryData("ef")); // end
    CHECK_EQUAL(BinaryData("abc"), c.get(0));
    CHECK_EQUAL(BinaryData("d"), c.get(1));
    CHECK_EQUAL(BinaryData("ef"), c.get(2));
    CHECK_EQUAL(3, c.size());

    c.insert(1, BinaryData("ghij")); // middle
    CHECK_EQUAL(BinaryData("abc"), c.get(0));
    CHECK_EQUAL(BinaryData("ghij"), c.get(1));
    CHECK_EQUAL(BinaryData("d"), c.get(2));
    CHECK_EQUAL(BinaryData("ef"), c.get(3));
    CHECK_EQUAL(4, c.size());

    c.insert(0, BinaryData("klmno")); // first
    CHECK_EQUAL(BinaryData("klmno"), c.get(0));
    CHECK_EQUAL(BinaryData("abc"), c.get(1));
    CHECK_EQUAL(BinaryData("ghij"), c.get(2));
    CHECK_EQUAL(BinaryData("d"), c.get(3));
    CHECK_EQUAL(BinaryData("ef"), c.get(4));
    CHECK_EQUAL(5, c.size());

    c.insert(2, BinaryData("as")); // middle again
    CHECK_EQUAL(BinaryData("klmno"), c.get(0));
    CHECK_EQUAL(BinaryData("abc"), c.get(1));
    CHECK_EQUAL(BinaryData("as"), c.get(2));
    CHECK_EQUAL(BinaryData("ghij"), c.get(3));
    CHECK_EQUAL(BinaryData("d"), c.get(4));
    CHECK_EQUAL(BinaryData("ef"), c.get(5));
    CHECK_EQUAL(6, c.size());


    // TEST(BinaryColumn_Delete)

    c.clear();

    c.add(BinaryData("a"));
    c.add(BinaryData("bc"));
    c.add(BinaryData("def"));
    c.add(BinaryData("ghij"));
    c.add(BinaryData("klmno"));

    c.erase(0); // first
    CHECK_EQUAL(BinaryData("bc"), c.get(0));
    CHECK_EQUAL(BinaryData("def"), c.get(1));
    CHECK_EQUAL(BinaryData("ghij"), c.get(2));
    CHECK_EQUAL(BinaryData("klmno"), c.get(3));
    CHECK_EQUAL(4, c.size());

    c.erase(3); // last
    CHECK_EQUAL(BinaryData("bc"), c.get(0));
    CHECK_EQUAL(BinaryData("def"), c.get(1));
    CHECK_EQUAL(BinaryData("ghij"), c.get(2));
    CHECK_EQUAL(3, c.size());

    c.erase(1); // middle
    CHECK_EQUAL(BinaryData("bc"), c.get(0));
    CHECK_EQUAL(BinaryData("ghij"), c.get(1));
    CHECK_EQUAL(2, c.size());

    c.erase(0); // single
    CHECK_EQUAL(BinaryData("ghij"), c.get(0));
    CHECK_EQUAL(1, c.size());

    c.erase(0); // all
    CHECK_EQUAL(0, c.size());
    CHECK(c.is_empty());


    // TEST(BinaryColumn_Big)

    c.clear();

    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  "));
    CHECK_EQUAL(BinaryData("70 chars  70 chars  70 chars  70 chars  "
                           "70 chars  70 chars  70 chars  "),
                c.get(0));

    c.clear();
    c.add(BinaryData("a"));
    c.add(BinaryData("bc"));
    c.add(BinaryData("def"));
    c.add(BinaryData("ghij"));
    c.add(BinaryData("klmno"));
    c.add(BinaryData("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  "));
    CHECK_EQUAL(BinaryData("70 chars  70 chars  70 chars  70 chars  "
                           "70 chars  70 chars  70 chars  "),
                c.get(5));

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
    for (int i = 0; i < 100; ++i)
        c.add(BinaryData("", 0));
    for (int i = 0; i < 100; ++i) {
        c.set(i, BinaryData(s.c_str(), s.size()));
        s += 'x';
    }
    s.clear();
    for (int i = 0; i < 100; ++i) {
        CHECK_EQUAL(BinaryData(s.c_str(), s.size()), c.get(i));
        s += 'x';
    }


    // TEST(BinaryColumn_Destroy)

    c.destroy();
}

TEST(BinaryColumn_Nulls)
{
    ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
    BinaryColumn c(Allocator::get_default(), ref, true);

    c.add(BinaryData());
    c.add(BinaryData("", 0));
    c.add(BinaryData("foo"));

    CHECK(c.get(0).is_null());
    CHECK(c.is_null(0));
    CHECK(!c.get(1).is_null());
    CHECK(!c.is_null(1));
    CHECK(!c.get(2).is_null());

    // Contains
    //      Null
    CHECK(c.get(0).contains(c.get(0)));
    CHECK(!c.get(0).contains(c.get(1)));
    CHECK(!c.get(0).contains(c.get(2)));

    //      Empty string
    CHECK(c.get(1).contains(c.get(0)));
    CHECK(c.get(1).contains(c.get(1)));
    CHECK(!c.get(1).contains(c.get(2)));

    //      "foo"
    CHECK(c.get(2).contains(c.get(0)));
    CHECK(c.get(2).contains(c.get(1)));
    CHECK(c.get(2).contains(c.get(2)));


    // Begins with
    //      Null
    CHECK(c.get(0).begins_with(c.get(0)));
    CHECK(!c.get(0).begins_with(c.get(1)));
    CHECK(!c.get(0).begins_with(c.get(2)));

    //      Empty string
    CHECK(c.get(1).begins_with(c.get(0)));
    CHECK(c.get(1).begins_with(c.get(1)));
    CHECK(!c.get(1).begins_with(c.get(2)));

    //      "foo"
    CHECK(c.get(2).begins_with(c.get(0)));
    CHECK(c.get(2).begins_with(c.get(1)));
    CHECK(c.get(2).begins_with(c.get(2)));

    // Ends with
    //      Null
    CHECK(c.get(0).ends_with(c.get(0)));
    CHECK(!c.get(0).ends_with(c.get(1)));
    CHECK(!c.get(0).ends_with(c.get(2)));

    //      Empty string
    CHECK(c.get(1).ends_with(c.get(0)));
    CHECK(c.get(1).ends_with(c.get(1)));
    CHECK(!c.get(1).ends_with(c.get(2)));

    //      "foo"
    CHECK(c.get(2).ends_with(c.get(0)));
    CHECK(c.get(2).ends_with(c.get(1)));
    CHECK(c.get(2).ends_with(c.get(2)));

    c.destroy();
}

TEST(BinaryColumn_SwapRows)
{
    // Normal case
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref);

        c.add(BinaryData("foo"));
        c.add(BinaryData("bar"));
        c.add(BinaryData("baz"));
        c.add(BinaryData("quux"));

        CHECK_EQUAL(c.get(1), BinaryData("bar"));
        CHECK_EQUAL(c.get(2), BinaryData("baz"));
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.swap_rows(1, 2);

        CHECK_EQUAL(c.get(1), BinaryData("baz"));
        CHECK_EQUAL(c.get(2), BinaryData("bar"));
        CHECK_EQUAL(c.size(), 4);

        c.destroy();
    }

    // First two elements
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref);

        c.add(BinaryData("bar"));
        c.add(BinaryData("baz"));
        c.add(BinaryData("quux"));

        c.swap_rows(0, 1);

        CHECK_EQUAL(c.get(0), BinaryData("baz"));
        CHECK_EQUAL(c.get(1), BinaryData("bar"));
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Last two elements
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref);

        c.add(BinaryData("bar"));
        c.add(BinaryData("baz"));
        c.add(BinaryData("quux"));

        c.swap_rows(1, 2);

        CHECK_EQUAL(c.get(1), BinaryData("quux"));
        CHECK_EQUAL(c.get(2), BinaryData("baz"));
        CHECK_EQUAL(c.size(), 3); // size should not change

        // swap back
        c.swap_rows(1, 2);

        CHECK_EQUAL(c.get(1), BinaryData("baz"));
        CHECK_EQUAL(c.get(2), BinaryData("quux"));
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Indices in wrong order
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref);

        c.add(BinaryData("bar"));
        c.add(BinaryData("baz"));
        c.add(BinaryData("quux"));

        c.swap_rows(2, 1);

        CHECK_EQUAL(c.get(1), BinaryData("quux"));
        CHECK_EQUAL(c.get(2), BinaryData("baz"));
        CHECK_EQUAL(c.size(), 3); // size should not change

        c.destroy();
    }

    // Null values
    {
        bool nullable = true;
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref, nullable);

        c.add(BinaryData("foo"));
        c.add(BinaryData("bar"));
        c.add(BinaryData());
        c.add(BinaryData("baz"));

        CHECK(c.get(2).is_null());

        c.swap_rows(2, 1);

        CHECK(c.get(1).is_null());
        CHECK_EQUAL(c.get(2).data(), BinaryData("bar").data());
        CHECK_EQUAL(c.get(3).data(), BinaryData("baz").data());
        CHECK_EQUAL(c.size(), 4); // size should not change

        // swap back
        c.swap_rows(2, 1);

        CHECK(c.get(2).is_null());
        CHECK_EQUAL(c.get(1).data(), BinaryData("bar").data());
        CHECK_EQUAL(c.get(3).data(), BinaryData("baz").data());
        CHECK_EQUAL(c.size(), 4); // size should not change

        c.destroy();
    }
}

TEST(BinaryColumn_MoveLastOver)
{
    ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
    BinaryColumn c(Allocator::get_default(), ref, true);

    c.add({});
    c.add({});
    c.add({});

    c.move_last_over(0);
    CHECK(c.get(0).is_null());
    CHECK(c.get(1).is_null());

    c.destroy();
}

TEST(BinaryColumn_NonLeafRoot)
{
    std::string someBigString = "0123456789012345678901234567890123456789012345678901234567890123456789";
    std::string anotherBigString = "This is a rather long string, that should not be very much shorter";

    // Small blob
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref, true);

        for (int i = 0; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(BinaryData(s));
        }

        c.erase(0);
        CHECK_EQUAL(std::string(c.get(0)), "1");
        c.erase(c.size() - 1);
        c.erase(c.size() - 1);
        CHECK_EQUAL(std::string(c.get(c.size() - 1)), util::to_string(REALM_MAX_BPNODE_SIZE - 1));

        c.destroy();
    }

    // Big blob
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref, true);

        c.add(BinaryData(anotherBigString));
        for (int i = 1; i < (REALM_MAX_BPNODE_SIZE + 2); i++) {
            std::string s = util::to_string(i);
            c.add(BinaryData(s));
        }

        c.erase(0);
        CHECK_EQUAL(std::string(c.get(0)), "1");
        c.erase(c.size() - 1);
        c.erase(c.size() - 1);
        CHECK_EQUAL(std::string(c.get(c.size() - 1)), util::to_string(REALM_MAX_BPNODE_SIZE - 1));

        c.destroy();
    }

    // Upgrade from small to big
    {
        ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
        BinaryColumn c(Allocator::get_default(), ref, true);

        for (int i = 0; i < (REALM_MAX_BPNODE_SIZE + 1); i++) {
            std::string s = util::to_string(i);
            c.add(BinaryData(s));
        }
        c.set(1, BinaryData(someBigString)); // Upgrade when setting
        c.add(BinaryData(anotherBigString)); // Upgrade when adding

        c.erase(0);
        CHECK_EQUAL(std::string(c.get(0)), someBigString);
        c.erase(c.size() - 1);
        c.erase(c.size() - 1);
        CHECK_EQUAL(std::string(c.get(c.size() - 1)), util::to_string(REALM_MAX_BPNODE_SIZE - 1));

        c.destroy();
    }
}

TEST(BinaryColumn_get_at)
{
    BinaryData read;
    size_t get_pos;

    std::string hello = "Hello, world";
    std::string very_lazy_fox =
        "The lazy fox jumped over the quick brown dog. The quick fox jumped over the lazy brown dog. ";

    ref_type ref = BinaryColumn::create(Allocator::get_default(), 0, false);
    BinaryColumn c(Allocator::get_default(), ref, true);

    c.add(BinaryData());
    c.add(BinaryData(hello));

    // First one should be NULL
    CHECK(c.get(0).is_null());
    get_pos = 0;
    read = c.get_at(0, get_pos);
    CHECK(read.is_null());

    get_pos = 0;
    read = c.get_at(1, get_pos);
    CHECK_EQUAL(read.size(), hello.size());
    CHECK_EQUAL(std::string(read.data(), read.size()), hello);

    BinaryIterator it0;
    read = it0.get_next();
    CHECK(read.is_null());

    BinaryIterator it1(&c, 1);
    read = it1.get_next();
    CHECK_EQUAL(std::string(read.data(), read.size()), hello);
    read = it1.get_next();
    CHECK(read.is_null());

    BinaryIterator it2(c.get(1));
    read = it2.get_next();
    CHECK_EQUAL(std::string(read.data(), read.size()), hello);
    read = it2.get_next();
    CHECK(read.is_null());

    // Check BigBlob
    c.add(BinaryData(very_lazy_fox));

    get_pos = 0;
    read = c.get_at(2, get_pos);
    CHECK_EQUAL(read.size(), very_lazy_fox.size());
    CHECK_EQUAL(std::string(read.data(), read.size()), very_lazy_fox);

    // Split root
    for (unsigned i = 0; i < REALM_MAX_BPNODE_SIZE; i++) {
        c.add(BinaryData());
    }

    get_pos = 0;
    read = c.get_at(1, get_pos);
    CHECK_EQUAL(read.size(), hello.size());
    CHECK_EQUAL(std::string(read.data(), read.size()), hello);

    c.destroy();
}

#endif // TEST_COLUMN_BINARY
