#include <UnitTest++.h>

#include <tightdb/thread.hpp>
#include <tightdb/bind.hpp>

using namespace std;
using namespace tightdb;


namespace {

void increment(int* i)
{
    ++*i;
}


struct Shared {
    Mutex m_mutex;
    int m_value;

    void increment_1000_times()
    {
        for (int i=0; i<1000; ++i) {
            Mutex::Lock lock(m_mutex);
            ++m_value;
        }
    }
};


struct Robust {
    RobustMutex m_mutex;
    bool m_recover_called;

    void simulate_death()
    {
        m_mutex.lock(util::bind(&Robust::recover, this));
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

} // anonymous namespace


TEST(Thread_Join)
{
    int i = 0;
    Thread thread(util::bind(&increment, &i));
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
    thread.start(util::bind(&increment, &i));
    CHECK(thread.joinable());
    thread.join();
    CHECK(!thread.joinable());
    CHECK_EQUAL(1, i);
}


TEST(Thread_MutexLock)
{
    Mutex mutex;
    {
        Mutex::Lock lock(mutex);
    }
    {
        Mutex::Lock lock(mutex);
    }
}


TEST(Thread_ProcessSharedMutex)
{
    Mutex mutex((Mutex::process_shared_tag()));
    {
        Mutex::Lock lock(mutex);
    }
    {
        Mutex::Lock lock(mutex);
    }
}


TEST(Thread_CriticalSection)
{
    Shared shared;
    shared.m_value = 0;
    Thread threads[10];
    for (int i = 0; i < 10; ++i)
        threads[i].start(util::bind(&Shared::increment_1000_times, &shared));
    for (int i = 0; i < 10; ++i)
        threads[i].join();
    CHECK_EQUAL(10000, shared.m_value);
}


TEST(Thread_RobustMutex)
{
    // Abort if robust mutexes are not supported on the current
    // platform. Otherwise we would probably get into a dead-lock.
    if (!RobustMutex::is_robust_on_this_platform())
        return;

    Robust robust;

    // Check that lock/unlock cycle works and does not involve recovery
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();

    // Check recovery by simulating a death
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // One more round of recovery
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // Simulate a case where recovery fails or is impossible
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(util::bind(&Robust::recover_throw, &robust)),
                RobustMutex::NotRecoverable);
    CHECK(robust.m_recover_called);

    // Check that successive attempts at locking will throw
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(util::bind(&Robust::recover, &robust)),
                RobustMutex::NotRecoverable);
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    CHECK_THROW(robust.m_mutex.lock(util::bind(&Robust::recover, &robust)),
                RobustMutex::NotRecoverable);
    CHECK(!robust.m_recover_called);
}


TEST(Thread_DeathDuringRecovery)
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
        Thread thread(util::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);

    // Die while recovering
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);

    // The mutex is still in the 'inconsistent' state if another
    // attempt at locking it calls the recovery function
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();

    // Now that the mutex is fully recovered, we should be able to
    // carry out a regular round of lock/unlock
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();

    // Try a double death during recovery
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death, &robust));
        thread.join();
    }
    CHECK(!robust.m_recover_called);
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);
    robust.m_recover_called = false;
    {
        Thread thread(util::bind(&Robust::simulate_death_during_recovery, &robust));
        thread.join();
    }
    CHECK(robust.m_recover_called);
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(robust.m_recover_called);
    robust.m_mutex.unlock();
    robust.m_recover_called = false;
    robust.m_mutex.lock(util::bind(&Robust::recover, &robust));
    CHECK(!robust.m_recover_called);
    robust.m_mutex.unlock();
}
