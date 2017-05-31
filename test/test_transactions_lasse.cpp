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
#ifdef TEST_TRANSACTIONS_LASSE

#include <cstdlib>
#include <iostream>
#include <thread>

#ifdef _WIN32
#include <windows.h> // Sleep(), sched_yield()
#else
#include <sched.h>  // sched_yield()
#include <unistd.h> // usleep()
#endif

#include <realm.hpp>
#include <realm/column.hpp>
#include <realm/utilities.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::test_util;
using unit_test::TestContext;


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


// The tests in this file are run only if TEST_DURATION is set to 2 or
// more in testsettings.hpp

namespace {

REALM_FORCEINLINE void rand_sleep(Random& random)
{
    const int64_t ms = 500000;
    unsigned char r = static_cast<unsigned char>(random.draw_int<unsigned int>());

    if (r <= 244)
        return;
    else if (r <= 248) {
        // Busyloop for 0 - 1 ms (on a 2 ghz), probably resume in current time slice
        int64_t t = random.draw_int_mod(ms);
        for (volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if (r <= 250) {
        // Busyloop for 0 - 20 ms (on a 2 ghz), maybe resume in different time slice
        int64_t t = ms * random.draw_int_mod(20);
        for (volatile int64_t i = 0; i < t; i++) {
        }
    }
    else if (r <= 252) {
        // Release current time slice but get next available
        std::this_thread::yield();
    }
    else if (r <= 254) {
// Release current time slice and get time slice according to normal scheduling
#ifdef _MSC_VER
        Sleep(0);
#else
        usleep(0);
#endif
    }
    else {
// Release time slices for at least 200 ms
#ifdef _MSC_VER
        Sleep(200);
#else
        usleep(200);
#endif
    }
}

} // anonymous namespace


// *************************************************************************************
// *
// *        Stress test 1
// *
// *************************************************************************************

namespace {

const int ITER1 = 2000;
const int READERS1 = 10;
const int WRITERS1 = 10;

void write_thread(TestContext& test_context, std::string path, int thread_ndx)
{
    int_least64_t w = thread_ndx;
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    SharedGroup sg(path);

    for (int i = 0; i < ITER1; ++i) {
        {
            WriteTransaction wt(sg);
            TableRef table = wt.get_or_add_table("table");
            table->set_int(0, 0, w);
            rand_sleep(random);
            int64_t r = table->get_int(0, 0);
            CHECK_EQUAL(r, w);
            wt.commit();
        }

        // All writes by all threads must be unique so that it can be detected if they're spurious
        w += 1000;
    }
}

void read_thread(TestContext& test_context, std::string path)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    SharedGroup sg(path);
    for (int i = 0; i < ITER1; ++i) {
        ReadTransaction rt(sg);
        int64_t r1 = rt.get_table("table")->get_int(0, 0);
        rand_sleep(random);
        int64_t r2 = rt.get_table("table")->get_int(0, 0);
        CHECK_EQUAL(r1, r2);
    }
}

} // anonymous namespace


TEST_IF(Transactions_Stress1, TEST_DURATION >= 3)
{
    test_util::ThreadWrapper read_threads[READERS1];
    test_util::ThreadWrapper write_threads[WRITERS1];

    SHARED_GROUP_TEST_PATH(path);
    SharedGroup sg(path);
    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_or_add_table("table");
        DescriptorRef desc = table->get_descriptor();
        desc->add_column(type_Int, "row");
        table->insert_empty_row(0, 1);
        table->set_int(0, 0, 0);
        wt.commit();
    }

    for (int i = 0; i < READERS1; ++i)
        read_threads[i].start([&] { read_thread(test_context, path); });

    for (int i = 0; i < WRITERS1; ++i)
        write_threads[i].start([this, &path, i] { write_thread(test_context, path, i); });

    for (int i = 0; i < READERS1; ++i) {
        bool reader_has_thrown = read_threads[i].join();
        CHECK(!reader_has_thrown);
    }

    for (int i = 0; i < WRITERS1; ++i) {
        bool writer_has_thrown = write_threads[i].join();
        CHECK(!writer_has_thrown);
    }
}


// *************************************************************************************
// *
// *        Stress test 2
// *
// *************************************************************************************

namespace {

const int THREADS2 = 30;
const int ITER2 = 2000;
const unsigned GROUPS2 = 30;

void create_groups(std::string path)
{
    Random random(random_int<unsigned long>()); // Seed from slow global generator
    std::vector<SharedGroup*> groups;

    for (int i = 0; i < ITER2; ++i) {
        // Repeatedly create a group or destroy a group or do nothing
        int action = random.draw_int_mod(2);

        if (action == 0 && groups.size() < GROUPS2) {
            groups.push_back(new SharedGroup(path));
        }
        else if (action == 1 && groups.size() > 0) {
            size_t g = random.draw_int_mod(groups.size());
            delete groups[g];
            groups.erase(groups.begin() + g);
        }
    }

    // Delete any remaining group to avoid memory and lock file leaks
    for (size_t i = 0; i < groups.size(); ++i)
        delete groups[i];
}

} // anonymous namespace

TEST_IF(Transactions_Stress2, TEST_DURATION >= 3)
{
    test_util::ThreadWrapper threads[THREADS2];

    SHARED_GROUP_TEST_PATH(path);

    for (int i = 0; i < THREADS2; ++i)
        threads[i].start(std::bind(&create_groups, std::string(path)));

    for (int i = 0; i < THREADS2; ++i) {
        bool thread_has_thrown = threads[i].join();
        CHECK(!thread_has_thrown);
    }
}


// *************************************************************************************
// *
// *        Stress test 3
// *
// *************************************************************************************

namespace {

// Must be fast because important edge case is 0 delay.
struct FastRand {
    FastRand()
        : u(1)
        , v(1)
    {
    }
    unsigned int operator()()
    {
        v = 36969 * (v & 65535) + (v >> 16);
        u = 18000 * (u & 65535) + (u >> 16);
        return (v << 16) + u;
    }

private:
    unsigned int u;
    unsigned int v;
};

} // anonymous namespace


TEST_IF(Transactions_Stress3, TEST_DURATION >= 3)
{
    SHARED_GROUP_TEST_PATH(path);

    const int ITER = 20;
    const int WRITERS = 4;
    const int READERS = 4;
    const size_t ROWS = 1 * 1000 * 1000 + 1000; // + 1000 to add extra depth level if REALM_MAX_BPNODE_SIZE = 1000
    std::atomic<bool> terminate{false};

    auto write_thread = [&] {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        FastRand fast_rand;
        SharedGroup sg(path);

        for (int i = 0; i < ITER; ++i) {
            WriteTransaction wt(sg);
            TableRef table = wt.get_or_add_table("table");
            size_t s = table->size();

            if (random.draw_bool() && s > 0) {
                size_t from = fast_rand() % s;
                size_t n = fast_rand() % (s - from + 1);
                for (size_t t = 0; t < n; ++t)
                    table->remove(from);
            }
            else if (s < ROWS / 2) {
                size_t at = fast_rand() % (s + 1);
                size_t n = fast_rand() % ROWS;
                for (size_t t = 0; t < n; ++t) {
                    table->insert_empty_row(at);
                    table->set_int(0, at, fast_rand() % 80);
                }
            }

            wt.commit();
        }
    };

    auto read_thread = [&] {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        SharedGroup sg(path);
        while (!terminate) {
            ReadTransaction rt(sg);
            if (rt.get_table("table")->size() > 0) {
                int64_t r1 = rt.get_table("table")->get_int(0, 0);
                rand_sleep(random);
                int64_t r2 = rt.get_table("table")->get_int(0, 0);
                CHECK_EQUAL(r1, r2);
            }
        }
    };

    test_util::ThreadWrapper write_threads[WRITERS];
    test_util::ThreadWrapper read_threads[READERS];

    SharedGroup sg(path);

    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_or_add_table("table");
        DescriptorRef desc = table->get_descriptor();
        desc->add_column(type_Int, "row");
        wt.commit();
    }

    for (int i = 0; i < WRITERS; ++i)
        write_threads[i].start(write_thread);

    for (int i = 0; i < READERS; ++i)
        read_threads[i].start(read_thread);

    for (int i = 0; i < WRITERS; ++i) {
        bool writer_has_thrown = write_threads[i].join();
        CHECK(!writer_has_thrown);
    }

    // Terminate reader threads cleanly
    terminate = true; // FIXME: Oops - this 'write' participates in a data race -
    // http://stackoverflow.com/questions/12878344/volatile-in-c11
    for (int i = 0; i < READERS; ++i) {
        bool reader_has_thrown = read_threads[i].join();
        CHECK(!reader_has_thrown);
    }
}


// *************************************************************************************
// *
// *        Stress test 4. Identical to 1 except that readers keep reading until all
// *        writers are done instead of terminating prematurely ("bug" in test 1)
// *
// *************************************************************************************

TEST_IF(Transactions_Stress4, TEST_DURATION >= 3)
{
    SHARED_GROUP_TEST_PATH(path);

    const int ITER = 2000;
    const int READERS = 20;
    const int WRITERS = 20;
    std::atomic<bool> terminate{false};

    auto write_thread = [&](int thread_ndx) {
        int_least64_t w = thread_ndx;
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        SharedGroup sg(path);

        for (int i = 0; i < ITER; ++i) {
            {
                WriteTransaction wt(sg);
                TableRef table = wt.get_or_add_table("table");
                table->set_int(0, 0, w);
                rand_sleep(random);
                int64_t r = table->get_int(0, 0);
                CHECK_EQUAL(r, w);
                wt.commit();
            }

            // All writes by all threads must be unique so that it can be detected if they're spurious
            w += 1000;
        }
    };

    auto read_thread = [&] {
        Random random(random_int<unsigned long>()); // Seed from slow global generator
        SharedGroup sg(path);
        while (!terminate) {
            ReadTransaction rt(sg);
            int64_t r1 = rt.get_table("table")->get_int(0, 0);
            rand_sleep(random);
            int64_t r2 = rt.get_table("table")->get_int(0, 0);
            CHECK_EQUAL(r1, r2);
        }
    };

    test_util::ThreadWrapper read_threads[READERS];
    test_util::ThreadWrapper write_threads[WRITERS];

    SharedGroup sg(path);

    {
        WriteTransaction wt(sg);
        TableRef table = wt.get_or_add_table("table");
        DescriptorRef desc = table->get_descriptor();
        desc->add_column(type_Int, "row");
        table->insert_empty_row(0, 1);
        table->set_int(0, 0, 0);
        wt.commit();
    }

    for (int i = 0; i < READERS; ++i)
        read_threads[i].start(read_thread);

    for (int i = 0; i < WRITERS; ++i)
        write_threads[i].start([=] { write_thread(i); });

    for (int i = 0; i < WRITERS; ++i) {
        bool writer_has_thrown = write_threads[i].join();
        CHECK(!writer_has_thrown);
    }

    terminate = true;
    for (int i = 0; i < READERS; ++i) {
        bool reader_has_thrown = read_threads[i].join();
        CHECK(!reader_has_thrown);
    }
}

#endif // TEST_TRANSACTIONS_LASSE
