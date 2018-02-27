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

#include <chrono>
// #include <valgrind/callgrind.h>

#ifndef CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_START_INSTRUMENTATION
#endif

#ifndef CALLGRIND_STOP_INSTRUMENTATION
#define CALLGRIND_STOP_INSTRUMENTATION
#endif

using namespace realm;
using namespace realm::test_util;
using namespace std::chrono;

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

    std::vector<Int> all = tree.get_all();
    size_t sz = tree.size();
    CHECK_EQUAL(all.size(), sz);
    for (size_t i = 0; i < sz; i++) {
        CHECK_EQUAL(tree.get(i), all[i]);
    }

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

    tree.clear();
    CHECK_EQUAL(tree.size(), 0);

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
    Array parent_array(Allocator::get_default());
    parent_array.create(NodeHeader::type_HasRefs);
    parent_array.add(0);

    BPlusTree<Int> tree(Allocator::get_default());
    tree.set_parent(&parent_array, 0);
    tree.create();
    CHECK_EQUAL(tree.get_ref(), parent_array.get_as_ref(0));

    tree.add(5);
    CHECK_EQUAL(tree.get(0), 5);

    BPlusTree<Int> another_tree(Allocator::get_default());
    another_tree.set_parent(&parent_array, 0);

    // another_tree initialized from scratch with a single leaf
    another_tree.init_from_parent();

    CHECK_EQUAL(another_tree.get(0), 5);

    tree.erase(0);
    // expand tree
    for (int i = 0; i < 10; i++) {
        tree.add(i);
    }

    // another_tree re-initialized with an inner node - replace accessor
    another_tree.init_from_parent();
    CHECK_EQUAL(another_tree.get(5), 5);

    // expand tree further
    for (int i = 0; i < 10; i++) {
        tree.add(i + 10);
    }

    // another_tree re-initialized with an inner node - reuse accessor
    another_tree.init_from_parent();
    CHECK_EQUAL(another_tree.get(15), 15);
    CHECK_EQUAL(another_tree.size(), 20);

    tree.clear();

    another_tree.init_from_parent();
    CHECK_EQUAL(another_tree.size(), 0);

    tree.destroy();
    parent_array.destroy();
}

namespace {
BPlusTree<Int> create_bplustree_int()
{
    BPlusTree<Int> tree(Allocator::get_default());
    tree.create();

    for (int i = 0; i < 10; i++) {
        tree.add(i);
    }

    return tree;
}
}

TEST(BPlusTree_Copy)
{
    BPlusTree<Int> tree(Allocator::get_default());

    tree.create();

    tree.add(5);
    CHECK_EQUAL(tree.get(0), 5);

    tree = create_bplustree_int();
    CHECK_EQUAL(tree.size(), 10);
    CHECK_EQUAL(tree.get(0), 0);

    BPlusTree<Int> another_tree(Allocator::get_default());
    another_tree.create();

    for (int i = 0; i < 20; i++) {
        another_tree.add(i << 1);
    }

    CHECK_EQUAL(another_tree.get(10), 20);

    tree = another_tree;

    CHECK_EQUAL(tree.get(10), 20);
    CHECK_EQUAL(another_tree.get(10), 20);

    tree.destroy();
    another_tree.destroy();
}

TEST(BPlusTree_Performance)
{
    // We try to optimize for add and sequential lookup
    int nb_rows = 5000;
    BPlusTree<Int> tree(Allocator::get_default());

    tree.create();

    CALLGRIND_START_INSTRUMENTATION;

    std::cout << nb_rows << " BPlusTree - sequential" << std::endl;

    {
        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            tree.add(i);
        }

        auto t2 = steady_clock::now();
        std::cout << "   insertion time: " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/row"
                  << std::endl;

        CHECK_EQUAL(tree.size(), nb_rows);
    }

    {
        auto t1 = steady_clock::now();

        for (int i = 0; i < nb_rows; i++) {
            CHECK_EQUAL(i, tree.get(i));
        }

        auto t2 = steady_clock::now();

        std::cout << "   lookup time   : " << duration_cast<nanoseconds>(t2 - t1).count() / nb_rows << " ns/row"
                  << std::endl;
    }

    CALLGRIND_STOP_INSTRUMENTATION;

    tree.destroy();
}

#endif // TEST_BPLUS_TREE
