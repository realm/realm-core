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
#ifdef TEST_ARRAY_BLOB

#include <cstring>

#include <realm/array_blob.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_string.hpp>
#include <realm/array_key.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

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

TEST(ArrayBlob_AddEmpty)
{
    ArrayBlob blob(Allocator::get_default());
    blob.create();

    blob.add("", 0);

    // Cleanup
    blob.destroy();
}


TEST(ArrayBlob_General)
{
    ArrayBlob blob(Allocator::get_default());
    blob.create();

    const char* t1 = "aaa";
    const char* t2 = "bbbbbb";
    const char* t3 = "ccccccccccc";
    const char* t4 = "xxx";
    size_t l1 = strlen(t1) + 1;
    size_t l2 = strlen(t2) + 1;
    size_t l3 = strlen(t3) + 1;

    // Test add
    blob.add(t1, l1);
    blob.add(t2, l2);
    blob.add(t3, l3);

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l1));
    CHECK_EQUAL(t3, blob.get(l1 + l2));

    // Test insert
    blob.insert(0, t3, l3);
    blob.insert(l3, t2, l2);

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t2, blob.get(l3));
    CHECK_EQUAL(t1, blob.get(l3 + l2));
    CHECK_EQUAL(t2, blob.get(l3 + l2 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l2 + l1 + l2));

    // Test replace
    blob.replace(l3, l3 + l2, t1, l1);                     // replace with smaller
    blob.replace(l3 + l1 + l1, l3 + l1 + l1 + l2, t3, l3); // replace with bigger
    blob.replace(l3 + l1, l3 + l1 + l1, t4, l1);           // replace with same

    CHECK_EQUAL(t3, blob.get(0));
    CHECK_EQUAL(t1, blob.get(l3));
    CHECK_EQUAL(t4, blob.get(l3 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1));
    CHECK_EQUAL(t3, blob.get(l3 + l1 + l1 + l3));

    // Test delete
    blob.erase(0, l3);                 // top
    blob.erase(l1, l1 + l1);           // middle
    blob.erase(l1 + l3, l1 + l3 + l3); // bottom

    CHECK_EQUAL(t1, blob.get(0));
    CHECK_EQUAL(t3, blob.get(l1));
    CHECK_EQUAL(l1 + l3, blob.size());

    // Delete all
    blob.erase(0, l1 + l3);
    CHECK(blob.is_empty());

    // Cleanup
    blob.destroy();
}

TEST(ArrayBlob_Null)
{
    {
        ArrayString a(Allocator::get_default());
        a.create();
        a.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
        a.clear();

        a.add("foo");
        a.add("");
        a.add(realm::null());

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
        a.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
        a.clear();

        a.add(realm::null());
        a.add("");
        a.add("foo");

        CHECK_EQUAL(a.is_null(0), true);
        CHECK_EQUAL(a.is_null(1), false);
        CHECK_EQUAL(a.is_null(2), false);
        CHECK(a.get(2) == "foo");

        // Test insert
        a.insert(0, realm::null());
        a.insert(2, realm::null());
        a.insert(4, realm::null());

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
        a.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
        a.clear();

        a.add("");
        a.add(realm::null());
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

    for (size_t t = 0; t < 2; t++) {
        ArrayString a(Allocator::get_default());
        a.create();
        a.add("70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  70 chars  ");
        a.clear();

        // vector that is kept in sync with the ArrayString so that we can compare with it
        std::vector<std::string> v;

        for (size_t i = 0; i < 2000; i++) {
            unsigned char rnd = static_cast<unsigned char>(
                random.draw_int<unsigned int>()); //    = 1234 * ((i + 123) * (t + 432) + 423) + 543;

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

                if (random.draw_int<int>() > 100)
                    len = rnd % 15;
                else
                    len = 0;

                StringData sd;
                std::string stdstr;

                if (random.draw_int<int>() > 100) {
                    sd = realm::null();
                    stdstr = "null";
                }
                else {
                    sd = StringData(str, len);
                    stdstr = std::string(str, len);
                }

                if (random.draw_int<int>() > 100) {
                    a.add(sd);
                    v.push_back(stdstr);
                }
                else if (a.size() > 0) {
                    size_t pos = rnd % a.size();
                    a.insert(pos, sd);
                    v.insert(v.begin() + pos, stdstr);
                }

                CHECK_EQUAL(a.size(), v.size());
                for (size_t a_i = 0; a_i < a.size(); a_i++) {
                    if (v[a_i] == "null") {
                        CHECK(a.is_null(a_i));
                        CHECK(a.get(a_i).data() == nullptr);
                    }
                    else {
                        CHECK(a.get(a_i) == v[a_i]);
                    }
                }
            }
        }
        a.destroy();
    }
}


#endif // TEST_ARRAY_BLOB
