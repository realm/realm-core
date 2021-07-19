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
#ifdef TEST_THREAD

#include <condition_variable>
#include <cstring>
#include <algorithm>
#include <queue>
#include <functional>
#include <mutex>
#include <thread>
#include <atomic>

#ifndef _WIN32
#include <unistd.h>
#include <sys/time.h>
#include <realm/utilities.hpp> // gettimeofday()
#endif

#include <realm/db_options.hpp>
#include <realm/utilities.hpp>
#include <realm/util/features.h>
#include <realm/util/thread.hpp>
#include <realm/util/interprocess_condvar.hpp>
#include <realm/util/interprocess_mutex.hpp>

#include <iostream>
#include "test.hpp"

using namespace realm;
using namespace realm::util;


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


namespace {

void increment(int* i)
{
    ++*i;
}

struct Shared {
    Mutex m_mutex;
    int m_value;

    // 10000 takes less than 0.1 sec
    void increment_10000_times()
    {
        for (int i = 0; i < 10000; ++i) {
            LockGuard lock(m_mutex);
            ++m_value;
        }
    }

    void increment_10000_times2()
    {
        for (int i = 0; i < 10000; ++i) {
            LockGuard lock(m_mutex);
            // Create a time window where thread interference can take place. Problem with ++m_value is that it
            // could assemble into 'inc [addr]' which has very tiny gap
            double f = m_value;
            f += 1.;
            m_value = int(f);
        }
    }
};

struct SharedWithEmulated {
    InterprocessMutex m_mutex;
    InterprocessMutex::SharedPart m_shared_part;
    int m_value;

    SharedWithEmulated(std::string name)
    {
        m_mutex.set_shared_part(m_shared_part, name, "0");
    }
    ~SharedWithEmulated()
    {
        m_mutex.release_shared_part();
    }

    // 10000 takes less than 0.1 sec
    void increment_10000_times()
    {
        for (int i = 0; i < 10000; ++i) {
            std::lock_guard<InterprocessMutex> lock(m_mutex);
            ++m_value;
        }
    }

    void increment_10000_times2()
    {
        for (int i = 0; i < 10000; ++i) {
            std::lock_guard<InterprocessMutex> lock(m_mutex);
            // Create a time window where thread interference can take place. Problem with ++m_value is that it
            // could assemble into 'inc [addr]' which has very tiny gap
            double f = m_value;
            f += 1.;
            m_value = int(f);
        }
    }
};

struct Robust {
    RobustMutex m_mutex;
    bool m_recover_called;

    void simulate_death()
    {
        m_mutex.lock(std::bind(&Robust::recover, this));
        // Do not unlock
    }

    void simulate_death_during_recovery()
    {
        bool no_thread_has_died = m_mutex.low_level_lock();
        if (!no_thread_has_died)
            m_recover_called = true;
        // Do not unlock
    }

    void recover()
    {
        m_recover_called = true;
    }

    void recover_throw()
    {
        m_recover_called = true;
        throw RobustMutex::NotRecoverable();
    }
};


class QueueMonitor {
public:
    QueueMonitor()
        : m_closed(false)
    {
    }

    bool get(int& value)
    {
        LockGuard lock(m_mutex);
        for (;;) {
            if (!m_queue.empty())
                break;
            if (m_closed)
                return false;
            m_nonempty_or_closed.wait(lock); // Wait for producer
        }
        bool was_full = m_queue.size() == max_queue_size;
        value = m_queue.front();
        m_queue.pop();
        if (was_full)
            m_nonfull.notify_all(); // Resume a waiting producer
        return true;
    }

    void put(int value)
    {
        LockGuard lock(m_mutex);
        while (m_queue.size() == max_queue_size)
            m_nonfull.wait(lock); // Wait for consumer
        bool was_empty = m_queue.empty();
        m_queue.push(value);
        if (was_empty)
            m_nonempty_or_closed.notify_all(); // Resume a waiting consumer
    }

    void close()
    {
        LockGuard lock(m_mutex);
        m_closed = true;
        m_nonempty_or_closed.notify_all(); // Resume all waiting consumers
    }

private:
    Mutex m_mutex;
    CondVar m_nonempty_or_closed, m_nonfull;
    std::queue<int> m_queue;
    bool m_closed;

    static const unsigned max_queue_size = 8;
};

void producer_thread(QueueMonitor* queue, int value)
{
    for (int i = 0; i < 1000; ++i) {
        queue->put(value);
    }
}

void consumer_thread(QueueMonitor* queue, int* consumed_counts)
{
    for (;;) {
        int value = 0;
        bool closed = !queue->get(value);
        if (closed)
            return;
        ++consumed_counts[value];
    }
}


} // anonymous namespace


TEST(Thread_Join)
{
    int i = 0;
    Thread thread(std::bind(&increment, &i));
    CHECK(thread.joinable());
    thread.join();
    CHECK(!thread.joinable());
    CHECK_EQUAL(1, i);
}


TEST(Thread_Start)
{
    int i = 0;
    Thread thread;
    CHECK(!thread.joinable());
    thread.start(std::bind(&increment, &i));
    CHECK(thread.joinable());
    thread.join();
    CHECK(!thread.joinable());
    CHECK_EQUAL(1, i);
}


TEST(Thread_MutexLock)
{
    Mutex mutex;
    {
        LockGuard lock(mutex);
    }
    {
        LockGuard lock(mutex);
    }
}

#ifdef REALM_HAVE_PTHREAD_PROCESS_SHARED
TEST(Thread_ProcessSharedMutex)
{
    Mutex mutex((Mutex::process_shared_tag()));
    {
        LockGuard lock(mutex);
    }
    {
        LockGuard lock(mutex);
    }
}
#endif

TEST(Thread_CriticalSection)
{
    Shared shared;
    shared.m_value = 0;
    Thread threads[10];
    for (int i = 0; i < 10; ++i)
        threads[i].start(std::bind(&Shared::increment_10000_times, &shared));
    for (int i = 0; i < 10; ++i)
        threads[i].join();
    CHECK_EQUAL(100000, shared.m_value);
}


TEST(Thread_EmulatedMutex_CriticalSection)
{
    TEST_PATH(path);
    SharedWithEmulated shared(path);
    shared.m_value = 0;
    Thread threads[10];
    for (int i = 0; i < 10; ++i)
        threads[i].start(std::bind(&SharedWithEmulated::increment_10000_times, &shared));
    for (int i = 0; i < 10; ++i)
        threads[i].join();
    CHECK_EQUAL(100000, shared.m_value);
}


TEST(Thread_CriticalSection2)
{
    Shared shared;
    shared.m_value = 0;
    Thread threads[10];
    for (int i = 0; i < 10; ++i)
        threads[i].start(std::bind(&Shared::increment_10000_times2, &shared));
    for (int i = 0; i < 10; ++i)
        threads[i].join();
    CHECK_EQUAL(100000, shared.m_value);
}


// Todo. Not supported on Windows in particular? Keywords: winbug
TEST_IF(Thread_RobustMutex, TEST_THREAD_ROBUSTNESS)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    Robust robust;

    // Check that lock/unlock cycle works and does not involve recovery
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();

    // Check recovery by simulating a death
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // One more round of recovery
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // Simulate a case where recovery fails or is impossible
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(std::bind(&Robust::recover_throw, &robust)), RobustMutex::NotRecoverable);
    CHECK(robust.m_recover_called);

    // Check that successive attempts at locking will throw
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(std::bind(&Robust::recover, &robust)), RobustMutex::NotRecoverable);
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(std::bind(&Robust::recover, &robust)), RobustMutex::NotRecoverable);
    CHECK(!robust.m_recover_called);
}


TEST_IF(Thread_DeathDuringRecovery, TEST_THREAD_ROBUSTNESS)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    // This test checks that death during recovery causes a robust
    // mutex to stay in the 'inconsistent' state.

    Robust robust;

    // Bring the mutex into the 'inconsistent' state
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);

    // Die while recovering
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);

    // The mutex is still in the 'inconsistent' state if another
    // attempt at locking it calls the recovery function
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // Now that the mutex is fully recovered, we should be able to
    // carry out a regular round of lock/unlock
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();

    // Try a double death during recovery
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);
    robust.m_recover_called = false;
    {
        Thread thread(std::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();
    robust.m_recover_called = false;
    robust.m_mutex.lock(std::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();
}


TEST(Thread_CondVar)
{
    QueueMonitor queue;
    const int num_producers = 32;
    const int num_consumers = 32;
    Thread producers[num_producers], consumers[num_consumers];
    int consumed_counts[num_consumers][num_producers];
    memset(consumed_counts, 0, sizeof consumed_counts);

    for (int i = 0; i < num_producers; ++i)
        producers[i].start(std::bind(&producer_thread, &queue, i));
    for (int i = 0; i < num_consumers; ++i)
        consumers[i].start(std::bind(&consumer_thread, &queue, &consumed_counts[i][0]));
    for (int i = 0; i < num_producers; ++i)
        producers[i].join();
    queue.close(); // Stop consumers when queue is empty
    for (int i = 0; i < num_consumers; ++i)
        consumers[i].join();

    for (int i = 0; i < num_producers; ++i) {
        int n = 0;
        for (int j = 0; j < num_consumers; ++j)
            n += consumed_counts[j][i];
        CHECK_EQUAL(1000, n);
    }
}

TEST(Thread_MutexTryLock)
{
    Thread thread;
    Mutex base_mutex;
    std::unique_lock<Mutex> m(base_mutex, std::defer_lock);

    std::condition_variable cv;
    std::mutex cv_lock;

    // basic same thread try_lock
    CHECK(m.try_lock());
    CHECK(m.owns_lock());
    CHECK_THROW(static_cast<void>(m.try_lock()), std::system_error); // already locked: Resource deadlock avoided
    m.unlock();

    bool init_done = false;
    auto do_async = [&]() {
        std::unique_lock<Mutex> mutex2(base_mutex, std::defer_lock);
        CHECK(!mutex2.owns_lock());
        CHECK(!mutex2.try_lock());
        {
            std::lock_guard<std::mutex> guard(cv_lock);
            init_done = true;
        }
        cv.notify_one();
        while(!mutex2.try_lock()) { millisleep(1); }
        CHECK(mutex2.owns_lock());
        mutex2.unlock();
    };

    // Check basic locking across threads.
    CHECK(!m.owns_lock());
    CHECK(m.try_lock());
    CHECK(m.owns_lock());
    thread.start(do_async);
    {
        std::unique_lock<std::mutex> guard(cv_lock);
        cv.wait(guard, [&]{return init_done;});
    }
    m.unlock();
    thread.join();
}

TEST(Thread_RobustMutexTryLock)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    Thread thread;
    RobustMutex m;
    int times_recover_function_was_called = 0;

    auto recover_function = [&]() {
        ++times_recover_function_was_called;
    };
    // basic same thread try_lock
    CHECK(m.try_lock(recover_function));
    CHECK(!m.try_lock(recover_function));
    m.unlock();
    CHECK(times_recover_function_was_called == 0);

    bool init_done = false;
    std::mutex control_mutex;
    std::condition_variable control_cv;

    auto do_async = [&]() {
        CHECK(!m.try_lock(recover_function));
        {
            std::lock_guard<std::mutex> guard(control_mutex);
            init_done = true;
        }
        control_cv.notify_one();
        while(!m.try_lock(recover_function)) { millisleep(1); }
        // exit the thread with the lock held to check robustness
    };

    // Check basic locking across threads.
    CHECK(m.try_lock(recover_function));
    thread.start(do_async);
    {
        std::unique_lock<std::mutex> lock(control_mutex);
        control_cv.wait(lock, [&]{ return init_done; });
    }
    m.unlock();
    thread.join();
    CHECK(times_recover_function_was_called == 0);
    // at this point the thread that obtained the mutex is dead with the lock
    CHECK(m.try_lock(recover_function));
    CHECK(times_recover_function_was_called == 1);
    m.unlock();
}

#ifndef _WIN32 // FIXME: trylock is not supported by the win32-pthread lib on Windows. No need to fix this
               // because we are going to switch to native API soon and discard win32-pthread entirely
NONCONCURRENT_TEST(Thread_InterprocessMutexTryLock)
{
    Thread thread;
    InterprocessMutex::SharedPart mutex_part;

    InterprocessMutex m;
    TEST_PATH(path);
    std::string mutex_file_name = "Test_Thread_InterprocessMutexTryLock";
    m.set_shared_part(mutex_part, path, mutex_file_name);

    // basic same thread try_lock
    CHECK(m.try_lock());
    CHECK(!m.try_lock()); // already locked but shouldn't deadlock
    m.unlock();

    bool init_done = false;
    std::condition_variable cv;
    std::mutex cv_mutex;
    auto do_async = [&]() {
        InterprocessMutex m2;
        m2.set_shared_part(mutex_part, path, mutex_file_name);

        CHECK(!m2.try_lock());
        {
            std::lock_guard<std::mutex> guard(cv_mutex);
            init_done = true;
        }
        cv.notify_one();
        while(!m2.try_lock()) { millisleep(1); }
        m2.unlock();
    };

    // Check basic locking across threads.
    CHECK(m.try_lock());
    thread.start(do_async);
    {
        std::unique_lock<std::mutex> ul(cv_mutex);
        cv.wait(ul, [&]{return init_done;});
    }
    m.unlock();
    thread.join();
    m.release_shared_part();
}

#endif

// Detect and flag trivial implementations of condvars.
namespace {

void signaller(int* signals, InterprocessMutex* mutex, InterprocessCondVar* cv)
{
    millisleep(200);
    {
        std::lock_guard<InterprocessMutex> l(*mutex);
        *signals = 1;
        // wakeup any waiters
        cv->notify_all();
    }
    // exit scope to allow waiters to get lock
    millisleep(200);
    {
        std::lock_guard<InterprocessMutex> l(*mutex);
        *signals = 2;
        // wakeup any waiters, 2nd time
        cv->notify_all();
    }
    millisleep(200);
    {
        std::lock_guard<InterprocessMutex> l(*mutex);
        *signals = 3;
        // wakeup any waiters, 2nd time
        cv->notify_all();
    }
    millisleep(200);
    {
        std::lock_guard<InterprocessMutex> l(*mutex);
        *signals = 4;
    }
}

void wakeup_signaller(int* signal_state, InterprocessMutex* mutex, InterprocessCondVar* cv)
{
    millisleep(1000);
    *signal_state = 2;
    std::lock_guard<InterprocessMutex> l(*mutex);
    cv->notify_all();
}



void waiter(InterprocessMutex* mutex, InterprocessCondVar* cv, std::mutex* control_mutex,
            std::condition_variable* control_cv, size_t* num_threads_holding_lock)
{
    std::lock_guard<InterprocessMutex> l(*mutex);

    {
        std::lock_guard<std::mutex> guard(*control_mutex);
        *num_threads_holding_lock = (*num_threads_holding_lock) + 1;
    }
    control_cv->notify_one();

    cv->wait(*mutex, nullptr);
}
}

// Verify, that a wait on a condition variable actually waits
// - this test relies on assumptions about scheduling, which
//   may not hold on a heavily loaded system.
NONCONCURRENT_TEST(Thread_CondvarWaits)
{
    int signals = 0;
    InterprocessMutex mutex;
    InterprocessMutex::SharedPart mutex_part;
    InterprocessCondVar changed;
    InterprocessCondVar::SharedPart condvar_part;
    TEST_PATH(path);
    DBOptions default_options;
    mutex.set_shared_part(mutex_part, path, "Thread_CondvarWaits_Mutex");
    changed.set_shared_part(condvar_part, path, "Thread_CondvarWaits_CondVar", default_options.temp_dir);
    changed.init_shared_part(condvar_part);
    Thread signal_thread;
    signals = 0;
    signal_thread.start(std::bind(signaller, &signals, &mutex, &changed));
    {
        std::lock_guard<InterprocessMutex> l(mutex);
        changed.wait(mutex, nullptr);
        CHECK_EQUAL(signals, 1);
        changed.wait(mutex, nullptr);
        CHECK_EQUAL(signals, 2);
        changed.wait(mutex, nullptr);
        CHECK_EQUAL(signals, 3);
    }
    signal_thread.join();
    changed.release_shared_part();
    mutex.release_shared_part();
}

// Verify that a condition variable looses its signal if no one
// is waiting on it
NONCONCURRENT_TEST(Thread_CondvarIsStateless)
{
    int signal_state = 0;
    InterprocessMutex mutex;
    InterprocessMutex::SharedPart mutex_part;
    InterprocessCondVar changed;
    InterprocessCondVar::SharedPart condvar_part;
    InterprocessCondVar::init_shared_part(condvar_part);
    TEST_PATH(path);
    DBOptions default_options;

    // Must have names because default_options.temp_dir is empty string on Windows
    mutex.set_shared_part(mutex_part, path, "Thread_CondvarIsStateless_Mutex");
    changed.set_shared_part(condvar_part, path, "Thread_CondvarIsStateless_CondVar", default_options.temp_dir);
    Thread signal_thread;
    signal_state = 1;
    // send some signals:
    {
        std::lock_guard<InterprocessMutex> l(mutex);
        for (int i = 0; i < 10; ++i)
            changed.notify_all();
    }
    // spawn a thread which will later do one more signal in order
    // to wake us up.
    signal_thread.start(std::bind(wakeup_signaller, &signal_state, &mutex, &changed));
    // Wait for a signal - the signals sent above should be lost, so
    // that this wait will actually wait for the thread to signal.
    {
        std::lock_guard<InterprocessMutex> l(mutex);
        changed.wait(mutex, 0);
        CHECK_EQUAL(signal_state, 2);
    }
    signal_thread.join();
    changed.release_shared_part();
    mutex.release_shared_part();
}


// this test hangs, if timeout doesn't work.
NONCONCURRENT_TEST(Thread_CondvarTimeout)
{
    InterprocessMutex mutex;
    InterprocessMutex::SharedPart mutex_part;
    InterprocessCondVar changed;
    InterprocessCondVar::SharedPart condvar_part;
    InterprocessCondVar::init_shared_part(condvar_part);
    TEST_PATH(path);
    DBOptions default_options;
    mutex.set_shared_part(mutex_part, path, "Thread_CondvarTimeout_Mutex");
    changed.set_shared_part(condvar_part, path, "Thread_CondvarTimeout_CondVar", default_options.temp_dir);
    struct timespec time_limit;
    timeval tv;
    gettimeofday(&tv, nullptr);
    time_limit.tv_sec = tv.tv_sec;
    time_limit.tv_nsec = tv.tv_usec * 1000;
    time_limit.tv_nsec += 100000000;        // 100 msec wait
    if (time_limit.tv_nsec >= 1000000000) { // overflow
        time_limit.tv_nsec -= 1000000000;
        time_limit.tv_sec += 1;
    }
    {
        std::lock_guard<InterprocessMutex> l(mutex);
        for (int i = 0; i < 5; ++i)
            changed.wait(mutex, &time_limit);
    }
    changed.release_shared_part();
    mutex.release_shared_part();
}


// test that notify_all will wake up all waiting threads, if there
// are many waiters:
NONCONCURRENT_TEST(Thread_CondvarNotifyAllWakeup)
{
    InterprocessMutex mutex;
    InterprocessMutex::SharedPart mutex_part;
    InterprocessCondVar changed;
    InterprocessCondVar::SharedPart condvar_part;
    InterprocessCondVar::init_shared_part(condvar_part);
    TEST_PATH(path);
    DBOptions default_options;
    mutex.set_shared_part(mutex_part, path, "Thread_CondvarNotifyAllWakeup_Mutex");
    changed.set_shared_part(condvar_part, path, "Thread_CondvarNotifyAllWakeup_CondVar", default_options.temp_dir);

    size_t num_threads_holding_lock = 0;
    std::mutex control_mutex;
    std::condition_variable control_cv;

    const size_t num_waiters = 10;
    Thread waiters[num_waiters];
    for (size_t i = 0; i < num_waiters; ++i) {
        waiters[i].start(std::bind(waiter, &mutex, &changed, &control_mutex, &control_cv, &num_threads_holding_lock));
    }
    {
        // allow all waiters to start and obtain the InterprocessCondVar
        std::unique_lock<std::mutex> unique_lock(control_mutex);
        control_cv.wait(unique_lock, [&]{ return num_threads_holding_lock == num_waiters; });
    }

    mutex.lock();
    changed.notify_all();
    mutex.unlock();

    for (size_t i = 0; i < num_waiters; ++i) {
        waiters[i].join();
    }
    changed.release_shared_part();
    mutex.release_shared_part();
}


// Test that the unlock+wait operation of wait() takes part atomically, i.e. that there is no time
// gap between them where another thread could invoke signal() which could go undetected by the wait.
// This test takes more than 3 days with valgrind.
TEST_IF(Thread_CondvarAtomicWaitUnlock, !running_with_valgrind && TEST_DURATION > 0)
{
    SHARED_GROUP_TEST_PATH(path);

    const int iter = 10000;

    // It's nice to have many threads to trigger preemption (see notes inside the t1 thread)
    const int thread_pair_count = 2; // std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    for (int tpc = 0; tpc < thread_pair_count; tpc++) {

        threads.push_back(std::thread([&]() {
            InterprocessMutex mutex;
            InterprocessMutex::SharedPart mutex_part;
            InterprocessCondVar condvar;
            InterprocessCondVar::SharedPart condvar_part;
            DBOptions default_options;

            std::stringstream ss;
            ss << std::this_thread::get_id();
            std::string id = ss.str();

            mutex.set_shared_part(mutex_part, path, "mutex" + id);
            condvar.set_shared_part(condvar_part, path, "sema" + id, default_options.temp_dir);
            InterprocessCondVar::init_shared_part(condvar_part);

            std::atomic<bool> signal(false);

            std::thread t1([&]() {
                for (int i = 0; i < iter; i++) {
                    mutex.lock();
                    signal = true;

                    // A gap in wait() could be very tight, so we need a way to preemt it between two instructions.
                    // Problem is that we have so many/frequent operating system wait calls in this that they might 
                    // be invoked closer than a thread time slice, so preemption would never occur. So we create 
                    // some work that some times willsome times bring the current time slice close to its end.

                    // Wait between 0 and number of clocks on 100 ms on a on 3 GHz machine (100 ms is Linux default
                    // time slice)
                    uint64_t clocks_to_wait = fastrand(3ULL * 1000000000ULL / 1000000ULL * 100ULL); 

                    // This loop can wait alot more than 100 ms because each iteration takes many more clocks than 
                    // just 1. That's intentional and will cover other OS'es with bigger time slices.
                    volatile int sum = 0; // must be volatile, else it compiles into no-op
                    for (uint64_t t = 0; t < clocks_to_wait; t++) {
                        sum++;
                    }

                    condvar.wait(mutex, nullptr);
                    mutex.unlock();
                }
            });

            // This thread calls notify_all() exactly one time after the other thread has invoked wait() and has
            // released the mutex. If wait() misses the notify_all() then there is a bug, which will reveal itself
            // by both threads hanging infinitely.
            std::thread t2([&]() {
                for (int i = 0; i < iter; i++) {
                    while (!signal) {
                    }
                    signal = false;
                    mutex.lock();
                    condvar.notify_all();
                    mutex.unlock();
                }
            });
            
            t1.join();
            t2.join();
        }));
    }

    for (int i = 0; i < thread_pair_count; i++) {
        threads[i].join();
    }
}

NONCONCURRENT_TEST(Thread_Condvar_CreateDestroyDifferentThreads)
{
    auto cv = std::make_unique<InterprocessCondVar>();
    InterprocessCondVar::SharedPart condvar_part;
    InterprocessCondVar::init_shared_part(condvar_part);
    TEST_PATH(path);
    DBOptions default_options;
    cv->set_shared_part(condvar_part, path, "Thread_CondvarTimeout_CondVar", default_options.temp_dir);
    std::thread([&] {
        cv.reset();
    }).join();
}

#ifdef _WIN32
TEST(Thread_Win32InterprocessBackslashes)
{
    InterprocessMutex mutex;
    InterprocessMutex::SharedPart mutex_part;
    InterprocessCondVar condvar;
    InterprocessCondVar::SharedPart condvar_part;
    InterprocessCondVar::init_shared_part(condvar_part);
    DBOptions default_options;

    mutex.set_shared_part(mutex_part, "Path\\With\\Slashes", "my_mutex");
    condvar.set_shared_part(condvar_part, "Path\\With\\Slashes", "my_condvar", default_options.temp_dir);
}
#endif

#endif // TEST_THREAD
