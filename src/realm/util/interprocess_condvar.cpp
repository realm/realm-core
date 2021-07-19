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

#include <realm/util/interprocess_condvar.hpp>
#include <realm/util/fifo_helper.hpp>

#include <fcntl.h>
#include <system_error>
#include <sstream>

#ifdef REALM_CONDVAR_EMULATION
#ifndef _WIN32
#include <unistd.h>
#include <poll.h>
#endif
#endif

#ifndef _WIN32
#include <sys/time.h>
#endif

#ifdef _WIN32
#include <Windows.h>
#include <set>
#include <queue>
#include <optional>
#include <realm/util/function_ref.hpp>
#endif

// Theory of operation for windows implementation:
// The core idea is that each process[1] owns a uniquely numbered Event object that is uses for
// waiting. Prior to waiting it ensures that the event is cleared then waits for it to be signaled.
// The notifier signals all events from 0 to the maximum to wake everyone who is interested in this
// CV. The complexity comes from three sources, 1) acquiring a unique id for your process, 2)
// determining the maximum Event to notify, 3) supporting more than one waiting thread per process.
// And this all must be done in a way that is robust to both processes joining, or being arbitrarily
// killed at any time. This is simplified somewhat by the additional requirement on ICV that you
// must hold the associated InterprocessMutex while notifying, and not just while waiting as in
// std::condition_variable, and this implementation relies on that for correctness.
//
// 1) Unique ids are assigned by acquiring a named mutex that will be held for the lifetime of the
// process. This ensures that nobody else will be able to use this id. Windows will automatically
// release this mutex if the process dies or is killed, so we will not be leaking the id. We try
// to lock each number starting at 0 and going up until we are able to lock one, then use that as
// our id. This ensures we get the lowest possible id that isn't currently in use to avoid raising
// m_max_process_num unnecessarily. We use a single dedicated thread for locking and unlocking this
// mutex both to allow a different thread to destroy the ICV than created it, and to allow other
// threads to continue using the ICV if the creating thread goes away, since windows will unlock
// the mutex as soon as the thread dies, even if other threads in the process are still running.
//
// 2) The maximum process id is adjusted each time a new process joins and claims an id. If the
// current value of m_max_process_num equals our id, nothing needs to be done. If our id is higher,
// raise it directly to our id. If our id is lower, try to acquire the mutex for the maximum num. If
// we fail to lock, assume the process is still running and stop. If we succeed at locking it, that
// process must no longer be running, so we can safely lower the maximum process number and repeat
// this process until we either fail to lock the mutex for that process or hit our own mutex, since
// we know *we* are still alive at least (also windows mutexes are reentrant, so we would otherwise
// get the signal that we are dead when we most certainly are not).
//
// 3) Because we reset the Event each time we wait, this only supports a single waiter per process.
// To overcome this, we also keep some per-process state to ensure that we only have one waiter at a
// time. Each potential waiter increments m_highest_waiter claiming the new value, then checks if
// there is already a designated waiter at the moment. If there is, wait on the private CV until
// m_signaled_waiters >= my waiter number. Otherwise, wait on the shared Event, and once it is
// ready, set m_signaled_waiters to m_highest waiters and notify the CV so that anyone who started
// waiting while we were waiting is also notified.
//
// [1] I'm using "process" in this description for simplicity, but it is really per
// InterprocessCondVar instance per process. So if a process opens two ICVs to the same underlying
// shared condvar, it counts as two processes in this description. Equivalently, if the process
// destroys and recreates an ICV, it counts as the process dieing and restarting. Because windows
// mutexes are reentrant, we also keep a set of the ids we have in use within each OS process to
// ensure that don't use the same id for two ICVs even if we are able to lock the mutex for that id.
//
// Mutex order: Associated InterprocessMutex
//            > g_process_nums_in_use_mutex
//            > Mutex[i] (all of them at same level)
//            > Mutex["max_process_num"]
// This is briefly violated, but using try_lock() which avoids deadlock.

using namespace realm;
using namespace realm::util;

#ifdef REALM_CONDVAR_EMULATION

namespace {

// Return difference in miliseconds between tv1 and tv2. Positive if tv1 < tv2.
int64_t timediff(const timeval& tv1, const timespec& ts2)
{
    return int64_t(ts2.tv_sec - tv1.tv_sec) * 1000 + (ts2.tv_nsec / 1000000) - (tv1.tv_usec / 1000);
}

#ifdef _WIN32
// CV name+path -> process num
// Ensures we don't use the same process num twice for two ICVs within the same process.
std::mutex g_process_nums_in_use_mutex;
std::map<std::string, std::set<int32_t>> g_process_nums_in_use;

// Provides a way to run a callable on a dedicated thread and wait for it to finish.
// While this is general purpose functionality, the intended purpose is for owning mutexes that
// would otherwise potentially be locked and unlocked on different threads.
class MutexOwnerThread {
public:
    // Will block until task has run, and will return the result.
    template <typename Func>
    static auto run_on_thread(Func&& task) -> decltype(task())
    {
        using Result = decltype(task());
        if constexpr (!std::is_same_v<Result, void>) {
            std::optional<Result> out;
            run_on_thread_impl([&] {
                out.emplace(task());
            });
            return std::move(*out);
        }
        else {
            run_on_thread_impl(task);
        }
    }

private:
    using Task = util::FunctionRef<void()>;
    struct WorkItem {
        std::condition_variable* cv;
        bool* done;
        Task task;
    };

    static void run_on_thread_impl(Task task)
    {
        auto& instance = get();

        // In C++20, we could use std::atomic_bool::wait rather than a CV here.
        std::condition_variable my_cv;
        bool done = false;

        auto lk = std::unique_lock(instance.mx);
        instance.work.push({&my_cv, &done, task});
        instance.worker_cv.notify_one();

        my_cv.wait(lk, [&] {
            return done;
        });
    }

    MutexOwnerThread()
        : worker([this] {
            thread_loop();
        })
    {
    }

    static MutexOwnerThread& get() noexcept
    {
        // Note, intentionally not destroying at process end.
        static MutexOwnerThread* const instance = new MutexOwnerThread();
        return *instance;
    }

    void thread_loop() noexcept
    {
        auto lk = std::unique_lock(mx);
        while (true) {
            worker_cv.wait(lk, [&] {
                return !work.empty();
            });

            auto work_item = std::move(work.front());
            work.pop();
            lk.unlock();
            work_item.task();
            lk.lock();
            *work_item.done = true;
            work_item.cv->notify_one();
        }
    }

    std::queue<WorkItem> work;

    std::mutex mx; // Guards work queue, as well as objects pointed to from WorkItems in the queue.
    std::condition_variable worker_cv;
    std::thread worker;
};

#else
// Write a byte to a pipe to notify anyone waiting for data on the pipe
void notify_fd(int fd)
{
    while (true) {
        char c = 0;
        ssize_t ret = write(fd, &c, 1);
        if (ret == 1) {
            break;
        }

        // If the pipe's buffer is full, we need to wait a bit for any waiter
        // to consume data before we proceed. This situation should not arise
        // under normal circumstances (it requires more pending waits than the
        // size of the buffer, which is not a likely scenario)
        REALM_ASSERT_EX(ret == -1 && errno == EAGAIN, errno);
        millisleep(1);
        continue;
    }
}

#endif

} // anonymous namespace
#endif // REALM_CONDVAR_EMULATION

InterprocessCondVar::InterprocessCondVar() {}


void InterprocessCondVar::close() noexcept
{
#ifdef REALM_CONDVAR_EMULATION
#ifndef _WIN32
    if (m_fd_read != -1) {
        ::close(m_fd_read);
        m_fd_read = -1;
    }
    if (m_fd_write != -1) {
        ::close(m_fd_write);
        m_fd_write = -1;
    }
#else
    if (m_my_mutex.handle) {
        MutexOwnerThread::run_on_thread([&] {
            m_my_mutex.unlock();
        });
    }

    if (m_my_id != -1) {
        auto lk = std::lock_guard(g_process_nums_in_use_mutex);
        g_process_nums_in_use[m_name_with_path].erase(m_my_id);
        m_my_id = -1;
    }

    m_events.clear();
    m_my_mutex = {};
#endif
#endif
    // we don't do anything to the shared part, other CondVars may share it

    m_shared_part = nullptr;
}


InterprocessCondVar::~InterprocessCondVar() noexcept
{
    close();
}

#ifdef REALM_CONDVAR_EMULATION
#ifndef _WIN32
static void make_non_blocking(int fd)
{
    // Make reading or writing from the file descriptor return -1 when the file descriptor's buffer is empty
    // rather than blocking until there's data available.
    int ret = fcntl(fd, F_SETFL, O_NONBLOCK);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }
}
#endif
#endif

void InterprocessCondVar::set_shared_part(SharedPart& shared_part, std::string base_path, std::string condvar_name,
                                          std::string tmp_path)
{
    close();
    m_shared_part = &shared_part;
    static_cast<void>(base_path);
    static_cast<void>(condvar_name);
    static_cast<void>(tmp_path);
#ifdef REALM_CONDVAR_EMULATION

#ifndef _WIN32

#if !REALM_TVOS
    m_resource_path = base_path + "." + condvar_name + ".cv";
    if (!try_create_fifo(m_resource_path)) {
        // Filesystem doesn't support named pipes, so try putting it in tmp_dir instead
        // Hash collisions are okay here because they just result in doing
        // extra work, as opposed to correctness problems.
        std::ostringstream ss;
        ss << normalize_dir(tmp_path);
        ss << "realm_" << std::hash<std::string>()(m_resource_path) << ".cv";
        m_resource_path = ss.str();
        create_fifo(m_resource_path);
    }
    m_fd_read = open(m_resource_path.c_str(), O_RDWR);
    if (m_fd_read == -1) {
        throw std::system_error(errno, std::system_category());
    }
    m_fd_write = -1;

#else // !REALM_TVOS

    // tvOS does not support named pipes, so use an anonymous pipe instead
    int notification_pipe[2];
    int ret = pipe(notification_pipe);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }

    m_fd_read = notification_pipe[0];
    m_fd_write = notification_pipe[1];

#endif // REALM_TVOS

    if (m_fd_read != -1) {
        make_non_blocking(m_fd_read);
    }
    if (m_fd_write != -1) {
        make_non_blocking(m_fd_write);
    }

#else // _WIN32
    // If the named objects are alive in the Windows kernel space, then their handles are cloned and
    // you get returned a new HANDLE number (differs from that of other processes) which represents the
    // same object. If not then they are created. When the last process that has handles to an object
    // terminates, the objects are destructed automatically by the kernel, so there will be no handle
    // leaks or other kinds of leak.

    // replace backslashes because they're significant in object namespace names
    std::string base_path_escaped = base_path;
    std::replace(base_path_escaped.begin(), base_path_escaped.end(), '\\', '/');

    m_name_with_path = base_path_escaped + condvar_name;

    {
        // Claim our mutex (lowest available id)
        auto lk = std::unique_lock(g_process_nums_in_use_mutex);
        auto& nums_in_use = g_process_nums_in_use[m_name_with_path];
        for (int32_t i = 0; true; i++) {
            if (nums_in_use.count(i))
                continue; // There is another ICV instance in this process for this CV.

            auto mutex = open_mutex(i);
            if (MutexOwnerThread::run_on_thread([&] {
                    return mutex.try_lock();
                })) {
                // We got the lock!
                m_my_mutex = std::move(mutex);
                m_my_id = i;
                nums_in_use.insert(i);
                break;
            }
        }
    }

    {
        // Update m_shared_part->m_max_process_num.
        auto max_process_num_mutex = open_mutex("max_process_num");
        auto lk = std::unique_lock(max_process_num_mutex);
        if (m_my_id > m_shared_part->m_max_process_num) {
            // I am the new max process.
            m_shared_part->m_max_process_num = m_my_id;
        }
        else if (m_my_id < m_shared_part->m_max_process_num) {
            // Walk backwards lowering m_max_process_num to eliminate any dead processes.
            for (int32_t i = m_shared_part->m_max_process_num; i > m_my_id; i--) {
                auto mutex = open_mutex(i);
                // This *is* a cycle in the lock order since the holder of mutex[i] may be waiting
                // to acquire mutex["max_process_num"]. However, because we are using try_lock()
                // here, it will not result in a deadlock.
                if (!mutex.try_lock())
                    break; // found a process that is still alive, so can't lower num any more.
                mutex.unlock();

                // Process `i` must no longer be running if we were able to grab the mutex.
                // If a new process `i` starts up, it will raise it back after acquiring the
                // max_process_num_mutex. So this is safe even if that process starts concurrently,
                // since we hold that mutex.
                m_shared_part->m_max_process_num = i - 1;
            }
        } // Nothing to do if they are already equal.
    }

    update_event_handles();
#endif // _WIN32
#endif // REALM_CONDVAR_EMULATION
}


void InterprocessCondVar::init_shared_part(SharedPart& shared_part)
{
#ifdef REALM_CONDVAR_EMULATION
#ifdef _WIN32
    shared_part.m_max_process_num = 0;
    shared_part.m_any_waiters = false;
#else
    shared_part.wait_counter = 0;
    shared_part.signal_counter = 0;
#endif
#else
    new (&shared_part) CondVar(CondVar::process_shared_tag());
#endif // REALM_CONDVAR_EMULATION
}


void InterprocessCondVar::release_shared_part()
{
#ifdef REALM_CONDVAR_EMULATION
#ifndef _WIN32
    File::try_remove(m_resource_path);
#endif
#else
    // For future platforms, remember to check if additional code should go here.
#endif
}

// Wait/notify combined invariant:
// - (number of bytes in the fifo - number of suspended thread)
//          = (wait_counter - signal_counter)
// - holds at the point of entry/exit from the critical section.

void InterprocessCondVar::wait(InterprocessMutex& m, const struct timespec* tp)
{
    // precondition: Caller holds the mutex ensuring exclusive access to variables
    // in the shared part.
    // postcondition: regardless of cause for return (timeout or notification),
    // the lock is held.
    REALM_ASSERT(m_shared_part);

#ifdef REALM_CONDVAR_EMULATION

#ifdef _WIN32
    const auto my_waiter = ++m_highest_waiter;

    if (m_have_waiter) {
        // Someone else is waiting, so just wait for their signal.
        auto waiter_lock = std::unique_lock(m, std::adopt_lock);
        if (tp) {
            m_waiter_cv.wait_until(
                waiter_lock,
                std::chrono::system_clock::from_time_t(tp->tv_sec) + std::chrono::nanoseconds(tp->tv_nsec), [&] {
                    return m_signaled_waiters >= my_waiter;
                });
        }
        else {
            m_waiter_cv.wait(waiter_lock, [&] {
                return m_signaled_waiters >= my_waiter;
            });
        }
        waiter_lock.release(); // not unlock(). Prevents unlock() when returning.
        return;
    }

    // I am the waiter.
    m_have_waiter = true;

    DWORD wait_milliseconds = INFINITE;
    if (tp) {
        timeval tp2;
        gettimeofday(&tp2, nullptr);
        auto milliseconds = timediff(tp2, *tp);
        if (milliseconds < 0) // need to check before converting to unsigned DWORD
            milliseconds = 0;
        wait_milliseconds = DWORD(milliseconds);
    }

    m_shared_part->m_any_waiters = true;
    my_event().reset(); // Ignore any notifications from before we were waiting.
    m.unlock();
    static_assert(noexcept(my_event().wait(wait_milliseconds)));
    my_event().wait(wait_milliseconds);
    m.lock();

    if (my_waiter != m_highest_waiter) {
        // Signal everyone that tried to wait while I was waiting. Do this even if I timed out, since
        // spurious wakeups are legal and they might miss a notification otherwise.
        m_signaled_waiters = m_highest_waiter;
        m_waiter_cv.notify_all();
    }
    m_have_waiter = false;
#else
    // indicate arrival of a new waiter (me) and get our own number in the
    // line of waiters. We later use this number to determine if a wakeup
    // is done because of valid signaling or should be ignored. We also use
    // wait count in the shared part to limit the number of wakeups that a
    // signaling process can buffer up. This is needed because a condition
    // variable is supposed to be state-less, so any signals sent before a
    // waiter has arrived must be lost.
    uint64_t my_wait_counter = ++m_shared_part->wait_counter;
    for (;;) {

        struct pollfd poll_d;
        poll_d.fd = m_fd_read;
        poll_d.events = POLLIN;
        poll_d.revents = 0;

        m.unlock(); // open for race from here

        // Race: A signal may trigger a write to the fifo both before and after
        // the call to poll(). If the write occurs before the call to poll(),
        // poll() will not block. This is intended.

        // Race: Another reader may overtake this one while the mutex is lifted,
        // and thus pass through the poll() call, even though it has arrived later
        // than the current thread. If so, the ticket (my_wait_counter) is used
        // below to filter waiters for fairness. The other thread will see that
        // its ticket is newer than the head of the queue and it will retry the
        // call to poll() - eventually allowing this thread to also get through
        // poll() and complete the wait().

        int r;
        {
            if (tp) {
                // poll requires a timeout in millisecs, but we get the timeout
                // as an absolute point in time, so we need to convert.
                timeval tv;
                gettimeofday(&tv, nullptr);

                int64_t milliseconds = timediff(tv, *tp);
                if (milliseconds < 0) { // negative timeout will mean no timeout. We don't want that.
                    milliseconds = 0;
                }
                REALM_ASSERT_DEBUG(!util::int_cast_has_overflow<int64_t>(milliseconds));
                int timeout = int(milliseconds);
                r = poll(&poll_d, 1, timeout);
            }
            else
                r = poll(&poll_d, 1, -1);
        }
        m.lock(); // no race after this point.
        uint64_t my_signal_counter = m_shared_part->signal_counter;

        // if wait returns with no ready fd it's a timeout:
        if (r == 0) {
            // We've earlier indicated that we're waiting and increased
            // the wait counter. Eventually (and possibly already after the return
            // from poll() but before locking the mutex) someone will write
            // to the fifo to wake us up. To keep the balance, we fake that
            // this signaling has already been done:
            ++m_shared_part->signal_counter;
            // even though we do this, a byte may be pending on the fifo.
            // we ignore this - so it may cause another, later, waiter to pass
            // through poll and grab that byte from the fifo. This will cause
            // said waiter to do a spurious return.
            return;
        }
        if (r == -1) {
            // if wait returns due to a signal, we must retry:
            if (errno == EINTR)
                continue;
        }
        // If we've been woken up, but actually arrived later than the
        // signal sent (have a later ticket), we allow someone else to
        // wake up. This can cause spinning until the right process acts
        // on its notification. To minimize this, we explicitly yield(),
        // hopefully advancing the point in time, where the rightful reciever
        // acts on the notification.
        if (my_signal_counter < my_wait_counter) {
            sched_yield();
            continue;
        }
        // Acting on the notification:
        // We need to consume the pipe data, if not, subsequent
        // waits will have their call to poll() return immediately.
        // This would effectively turn the condition variable into
        // a spinning wait, which will have correct behavior (provided
        // the user remembers to always validate the condition and
        // potentially loop on it), but it will consume excess CPU/battery
        // and may also cause priority inversion
        char c;
        ssize_t ret = read(m_fd_read, &c, 1);
        if (ret == -1)
            continue; // FIXME: If the invariants hold, this is unreachable
        return;
    }

#endif // _WIN32

#else
    m_shared_part->wait(
        *m.m_shared_part, []() {}, tp);
#endif
}


// Notify_all:
// precondition: The caller holds the mutex guarding the condition variable.
// operation: If waiters are present, we wake them up by writing a single
// byte to the fifo for each waiter.

void InterprocessCondVar::notify_all() noexcept
{
    REALM_ASSERT(m_shared_part);
#ifdef REALM_CONDVAR_EMULATION
#ifdef _WIN32
    if (!m_shared_part->m_any_waiters)
        return;

    m_shared_part->m_any_waiters = false;

    // Note that this could change while notifying but that is ok. Because the caller must hold the
    // associated mutex when notifying (unlike with std::condition_variable), the new process cannot
    // enter a wait since that also requires holding the mutex.
    if (m_events.size() != size_t(m_shared_part->m_max_process_num) + 1)
        update_event_handles();

    for (auto&& ev : m_events) {
        // Can't skip ourself because we could have other threads waiting.
        ev.set();
    }
#else
    while (m_shared_part->wait_counter > m_shared_part->signal_counter) {
        m_shared_part->signal_counter++;
        notify_fd(m_fd_write != -1 ? m_fd_write : m_fd_read);
    }
#endif
#else
    m_shared_part->notify_all();
#endif
}

#ifdef _WIN32
void InterprocessCondVar::Event::wait(DWORD millis) noexcept
{
    auto ret = WaitForSingleObject(handle, millis);
    REALM_ASSERT_RELEASE(ret != WAIT_FAILED);
    REALM_ASSERT_RELEASE(ret == WAIT_OBJECT_0 || ret == WAIT_TIMEOUT);
}
void InterprocessCondVar::Event::set() noexcept
{
    REALM_ASSERT_RELEASE(SetEvent(handle));
}
void InterprocessCondVar::Event::reset() noexcept
{
    REALM_ASSERT_RELEASE(ResetEvent(handle));
}

void InterprocessCondVar::Mutex::lock() noexcept
{
    auto ret = WaitForSingleObject(handle, INFINITE);
    REALM_ASSERT_RELEASE(ret != WAIT_FAILED);
    REALM_ASSERT_RELEASE(ret == WAIT_OBJECT_0 || ret == WAIT_ABANDONED_0);
}
bool InterprocessCondVar::Mutex::try_lock() noexcept
{
    auto ret = WaitForSingleObject(handle, 0);
    REALM_ASSERT_RELEASE(ret != WAIT_FAILED);
    return ret != WAIT_TIMEOUT;
}
void InterprocessCondVar::Mutex::unlock() noexcept
{
    REALM_ASSERT_RELEASE(ReleaseMutex(handle));
}

void InterprocessCondVar::update_event_handles()
{
    const int32_t max_process_num = m_shared_part->m_max_process_num;
    REALM_ASSERT_RELEASE(max_process_num >= 0);
    REALM_ASSERT_RELEASE(max_process_num < 1'000'000); // Sanity check.

    const size_t old_size = m_events.size();
    m_events.resize(size_t(max_process_num) + 1);
    for (size_t i = old_size; i < m_events.size(); i++) { // skipped if not growing.
        m_events[i] = open_event(int32_t(i));
    }
}

InterprocessCondVar::Mutex InterprocessCondVar::open_mutex(int32_t n)
{
    REALM_ASSERT_RELEASE(n >= 0);
    REALM_ASSERT_RELEASE(n < 1'000'000); // Sanity check.
    return open_mutex(std::to_string(n));
}
InterprocessCondVar::Mutex InterprocessCondVar::open_mutex(std::string name)
{
    const std::string uri = "Local\\realm_cv_mutex_" + m_name_with_path + '_' + name;
    const auto wuri = std::wstring(uri.begin(), uri.end());
    HandleHolder handle = CreateMutexW(nullptr, // no security
                                       false,   // initial owner
                                       wuri.c_str());
    REALM_ASSERT_RELEASE(handle);
    return Mutex{std::move(handle)};
}
InterprocessCondVar::Event InterprocessCondVar::open_event(int32_t n)
{
    REALM_ASSERT_RELEASE(n >= 0);
    REALM_ASSERT_RELEASE(n < 1'000'000); // Sanity check.
    const std::string uri = "Local\\realm_cv_event_" + m_name_with_path + '_' + std::to_string(n);
    const auto wuri = std::wstring(uri.begin(), uri.end());

    HandleHolder handle = CreateEventW(nullptr, // no security
                                       false,   // auto-reset
                                       false,   // non-signaled initially
                                       wuri.c_str());
    REALM_ASSERT_RELEASE(handle);
    return Event{std::move(handle)};
}
#endif
