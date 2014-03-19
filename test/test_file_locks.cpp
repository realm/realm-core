#include "testsettings.hpp"
#ifdef TEST_FILE_LOCKS

#include <cassert>
#include <cerrno>
#include <map>
#include <iostream>

#include <UnitTest++.h>

#include <tightdb/util/bind.hpp>
#include <tightdb/util/thread.hpp>
#include <tightdb/util/file.hpp>

using namespace std;
using namespace tightdb::util;

namespace {

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

map<int, int> results;

Mutex mutex;
CondVar cond;

int num_slaves_ready = 0;
int num_good_locks = 0;
bool slaves_run[num_slaves];

void master()
{
    LockGuard l(mutex);
    for (int i = 0; i != num_rounds; ++i) {
        while (num_slaves_ready != num_slaves)
            cond.wait(l);
        num_slaves_ready = 0;

        ++results[num_good_locks];
        num_good_locks = 0;

        for (int j = 0; j != num_slaves; ++j)
            slaves_run[j] = true;
        cond.notify_all();
    }
}

void slave(int ndx)
{
    File file("test_file_locks.test", File::mode_Write);
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
            while (!slaves_run[ndx])
                cond.wait(l);
            slaves_run[ndx] = false;
        }
    }
}

} // anonymous namespace


// The assumption is that if multiple processes try to place an
// exclusive lock on a file in a non-blocking fashion, then at least
// one will succeed (assuming that no one else interferes). This test
// trys to verify that this is the case by repeatedly letting two
// treads compete for the lock. This is by no means a "water tight"
// test, but it is probably the best we can do.
TEST(File_NoSpuriousTryLockFailures)
{
    Thread slaves[num_slaves];
    for (int i = 0; i != num_slaves; ++i) {
        slaves_run[i] = false;
        slaves[i].start(bind(&slave, i));
    }
    master();
    for (int i = 0; i != num_slaves; ++i)
        slaves[i].join();

    File::try_remove("test_file_locks.test");

/*
    typedef map<int, int>::const_iterator iter;
    iter end = results.end();
    for (iter i = results.begin(); i != end; ++i)
        cout << i->first << " -> " << i->second << "\n";
*/

    // Check that there are no cases where no one got the lock
    CHECK_EQUAL(0, results[0]);
}

#endif // TEST_FILE_LOCKS
