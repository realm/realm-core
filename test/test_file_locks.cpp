#include "testsettings.hpp"
#ifdef TEST_FILE_LOCKS

#include <cassert>
#include <map>
#include <iostream>
#include <functional>

#include <realm/util/thread.hpp>
#include <realm/util/file.hpp>
#include <realm/util/features.h>

#include "test.hpp"
#include "util/thread_wrapper.hpp"

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
    bool slaves_run[num_slaves];
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
        slaves_run[i] = false;
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

#endif // TEST_FILE_LOCKS
