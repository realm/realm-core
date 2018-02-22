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

#include "realm/bplustree.hpp"
#include "realm/array_string.hpp"
#include "realm/array_timestamp.hpp"

#include "test.hpp"

using namespace realm;
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

#ifdef TEST_BPLUS_TREE

TEST(BPlusTree_Integer)
{
    BPlusTree<Int> tree(Allocator::get_default());

    CHECK_EQUAL(tree.size(), 0);

    tree.create();

    tree.add(5);
    CHECK_EQUAL(tree.get(0), 5);

    for (int i = 0; i < 16; i++) {
        tree.add(i);
    }
    CHECK_EQUAL(tree.get(1), 0);
    CHECK_EQUAL(tree.get(10), 9);
    CHECK_EQUAL(tree.find_first(7), 8);
    tree.erase(0);
    CHECK_EQUAL(tree.find_first(7), 7);
    CHECK_EQUAL(tree.find_first(100), realm::npos);

    size_t sz = tree.size();
    while (sz) {
        sz--;
        tree.erase(sz);
    }
    tree.destroy();
}

TEST(BPlusTree_Timestamp)
{
    BPlusTree<Timestamp> tree(Allocator::get_default());

    tree.create();

    tree.add(Timestamp(5, 2));
    tree.add(Timestamp(7, 0));
    tree.add(Timestamp(7, 3));
    CHECK_EQUAL(tree.get(0), Timestamp(5, 2));
    CHECK_EQUAL(tree.find_first(Timestamp(7, 3)), 2);

    tree.destroy();
}

TEST(BPlusTree_Fuzz)
{
    const size_t iters = 500;
    std::vector<std::string> ref_arr;
    BPlusTree<StringData> tree(Allocator::get_default());

    tree.create();

    for (size_t iter = 0; iter < iters; iter++) {

        // Add
        if (fastrand(100) < ((iter < iters / 2) ? 60 : 10)) {
            std::string str = "foo ";
            str += util::to_string(iter);
            tree.add(str);
            ref_arr.push_back(str);
        }

        // Erase
        if (fastrand(100) < ((iter < iters / 2) ? 40 : 90) && tree.size() > 0) {
            size_t r = size_t(fastrand(tree.size() - 1));
            tree.erase(r);
            ref_arr.erase(ref_arr.begin() + r);
        }

        // Insert
        if (fastrand(100) < ((iter < iters / 2) ? 60 : 10)) {
            size_t r = size_t(fastrand(tree.size()));
            std::string str = "baa ";
            str += util::to_string(iter);
            tree.insert(r, str);
            ref_arr.insert(ref_arr.begin() + r, str);
        }

        // Set
        if (fastrand(100) < 20 && tree.size() > 0) {
            size_t r = size_t(fastrand(tree.size() - 1));
            std::string str = "hello cruel world ";
            str += util::to_string(iter);
            tree.set(r, str);
            ref_arr[r] = str;
        }

        size_t sz = tree.size();
        CHECK_EQUAL(sz, ref_arr.size());

        for (size_t i = 0; i < sz; i++) {
            CHECK_EQUAL(tree.get(i), ref_arr[i]);
        }
    }

    size_t sz = tree.size();
    while (sz) {
        tree.erase(sz - 1);
        ref_arr.pop_back();
        sz--;
        CHECK_EQUAL(sz, tree.size());
        for (size_t i = 0; i < sz; i++) {
            CHECK_EQUAL(tree.get(i), ref_arr[i]);
        }
    }

    tree.destroy();
}

// This test is designed to work with a node size of 4
TEST(BPlusTree_Initialization)
{
    BPlusTree<Int> tree(Allocator::get_default());
    Array parent_array(Allocator::get_default());

    tree.create();
    parent_array.create(NodeHeader::type_HasRefs);

    tree.add(5);
    CHECK_EQUAL(tree.get(0), 5);

    BPlusTree<Int> another_tree(Allocator::get_default());
    parent_array.add(0);
    another_tree.set_parent(&parent_array, 0);

    // another_tree initialized from scratch with a single leaf
    ref_type ref = tree.get_ref();
    another_tree.init_from_ref(ref);

    CHECK_EQUAL(another_tree.get(0), 5);
    CHECK_EQUAL(parent_array.get(0), ref);

    tree.erase(0);
    // expand tree
    for (int i = 0; i < 10; i++) {
        tree.add(i);
    }

    // another_tree re-initialized with an inner node - replace accessor
    ref = tree.get_ref();
    another_tree.init_from_ref(ref);

    CHECK_EQUAL(another_tree.get(5), 5);

    // expand tree further
    for (int i = 0; i < 10; i++) {
        tree.add(i + 10);
    }

    // another_tree re-initialized with an inner node - reuse accessor
    ref = tree.get_ref();
    another_tree.init_from_ref(ref);

    CHECK_EQUAL(another_tree.get(15), 15);

    tree.destroy();
    parent_array.destroy();
}

#endif // TEST_BPLUS_TREE
