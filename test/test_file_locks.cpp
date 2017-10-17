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
#ifdef TEST_FILE_LOCKS

#include <cassert>
#include <map>
#include <iostream>
#include <functional>
#include <thread>

#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>
#include <realm/util/features.h>
#include "test.hpp"

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


// The assumption is that if multiple processes try to place an
// exclusive lock on a file in a non-blocking fashion, then at least
// one will succeed (assuming that no one else interferes). This test
// trys to verify that this is the case by repeatedly letting two
// treads compete for the lock. This is by no means a "water tight"
// test, but it is probably the best we can do.
TEST(File_NoSpuriousTryLockFailures)
{
#if TEST_DURATION < 1
    const int num_rounds = 1000;
#elif TEST_DURATION < 2
    const int num_rounds = 10000;
#elif TEST_DURATION < 3
    const int num_rounds = 100000;
#else
    const int num_rounds = 1000000;
#endif

    const int num_slaves = 2;


    Mutex mutex;
    CondVar cond;
    int num_slaves_ready = 0;
    int num_good_locks = 0;
    bool slaves_run[num_slaves] = {false};
    std::map<int, int> results;
    bool terminate = false;

    auto kill_em_all = [&] {
        LockGuard l(mutex);
        terminate = true;
        cond.notify_all();
    };

    auto master = [&] {
        try {
            LockGuard l(mutex);
            for (int i = 0; i != num_rounds; ++i) {
                while (num_slaves_ready != num_slaves) {
                    if (terminate)
                        return;
                    cond.wait(l);
                }
                num_slaves_ready = 0;

                ++results[num_good_locks];
                num_good_locks = 0;

                for (int j = 0; j != num_slaves; ++j)
                    slaves_run[j] = true;
                cond.notify_all();
            }
        }
        catch (...) {
            kill_em_all();
            throw;
        }
    };

    auto slave = [&](int ndx, std::string path) {
        try {
            File file(path, File::mode_Write);
            for (int i = 0; i != num_rounds; ++i) {
                bool good_lock = file.try_lock_exclusive();
                if (good_lock)
                    file.unlock();
                {
                    LockGuard l(mutex);
                    if (good_lock)
                        ++num_good_locks;
                    ++num_slaves_ready;
                    cond.notify_all();
                    while (!slaves_run[ndx]) {
                        if (terminate)
                            return;
                        cond.wait(l);
                    }
                    slaves_run[ndx] = false;
                }
            }
        }
        catch (...) {
            kill_em_all();
            throw;
        }
    };

    TEST_PATH(path);
    std::string str_path = path;
    ThreadWrapper slaves[num_slaves];
    for (int i = 0; i != num_slaves; ++i) {
        slaves[i].start([=] { slave(i, str_path); });
    }
    master();
    for (int i = 0; i != num_slaves; ++i)
        CHECK(!slaves[i].join());

    /*
    typedef std::map<int, int>::const_iterator iter;
    iter end = results.end();
    for (iter i = results.begin(); i != end; ++i)
        std::cout << i->first << " -> " << i->second << "\n";
    */

    // Check that there are no cases where no one got the lock
    CHECK_EQUAL(0, results[0]);
}

// Same as above, but with busy waiting to increase the chance that try_lock is called simultaneously from
// all the threads.
TEST(File_NoSpuriousTryLockFailures2)
{
    // Busy waiting is very slow in Valgrind, so don't run it there.. Seems like we have no ONLY_TEST_IF, 
// so we're using this return instead.
    if(running_with_valgrind) {
        return;
    }

#if TEST_DURATION < 1
    const size_t num_rounds = 20;
#elif TEST_DURATION < 2
    const int num_rounds = 1000;
#elif TEST_DURATION < 3
    const int num_rounds = 10000;
#else
    const int num_rounds = 100000;
#endif

    // More threads than cores will give OS time slice yields at random places which is good for randomness
    size_t num_slaves = 2 * std::thread::hardware_concurrency(); // The number includes HyperThread cores

    std::atomic<size_t> lock_taken { 0 };
    std::atomic<size_t> barrier_1 { 0 };
    std::atomic<size_t> barrier_2 { 0 };
    std::atomic<size_t> lock_not_taken { 0 };

    auto slave = [&](int ndx, std::string path) {
        File file(path, File::mode_Write);

        for(size_t t = 0; t < num_rounds; t++) {
            lock_taken = 0;
            lock_not_taken = 0;

            // Thread barrier
            barrier_1++;
            while(barrier_1 < num_slaves) {
            }
       
            // All threads race for the lock
            bool owns_lock = file.try_lock_exclusive();

            barrier_2 = 0;

            if(owns_lock) {
                lock_taken++;
            }
            else {
                lock_not_taken++;
            }

            // Thread barrier
            while(lock_taken + lock_not_taken < num_slaves) {
            }

            REALM_ASSERT(lock_taken == 1);

            if(owns_lock) {
                file.unlock();
            }
                   
            barrier_1 = 0;

            // Thread barrier. After this barrier, the file is guaranteed to be unlocked regardless who owned it.
            barrier_2++;
            while(barrier_2 < num_slaves) {
            }
        }
    };

    TEST_PATH(path);
    std::string str_path = path;
    ThreadWrapper slaves[100];
    for (size_t i = 0; i != num_slaves; ++i) {
        slaves[i].start([=] { slave(i, str_path); });
    }

    for (size_t i = 0; i != num_slaves; ++i)
        CHECK(!slaves[i].join());
}

#endif // TEST_FILE_LOCKS
