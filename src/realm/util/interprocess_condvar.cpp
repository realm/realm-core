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


using namespace realm;
using namespace realm::util;

#ifdef REALM_CONDVAR_EMULATION

namespace {

// Return difference in miliseconds between tv1 and tv2. Positive if tv1 < tv2.
int64_t timediff(const timeval& tv1, const timespec& ts2)
{
    return int64_t(ts2.tv_sec - tv1.tv_sec) * 1000 + (ts2.tv_nsec / 1000000) - (tv1.tv_usec / 1000);
}

#ifndef _WIN32

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


InterprocessCondVar::InterprocessCondVar()
{
}


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

    // Create and open the named pipe
    int ret = mkfifo(m_resource_path.c_str(), 0600);
    if (ret == -1) {
        int err = errno;
        if (err == ENOTSUP || err == EACCES || err == EPERM || err == EINVAL) {
            // Filesystem doesn't support named pipes, so try putting it in tmp instead
            // Hash collisions are okay here because they just result in doing
            // extra work, as opposed to correctness problems
            std::ostringstream ss;
            ss << tmp_path;
            ss << "realm_" << std::hash<std::string>()(m_resource_path) << ".cv";
            m_resource_path = ss.str();
            ret = mkfifo(m_resource_path.c_str(), 0600);
            err = errno;
        }

        // the fifo already existing isn't an error
        if (ret == -1 && err != EEXIST) {
            // Workaround for a mkfifo bug on Blackberry devices:
            // When the fifo already exists, mkfifo fails with error ENOSYS which is not correct.
            // In this case, we use stat to check if the path exists and it is a fifo.
            struct stat stat_buf;
            if (stat(m_resource_path.c_str(), &stat_buf) == 0) {
                if ((stat_buf.st_mode & S_IFMT) != S_IFIFO) {
                    throw std::runtime_error(m_resource_path + " exists and it is not a fifo.");
                }
            }
            else {
                throw std::system_error(err, std::system_category());
            }
        }
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
    std::string sem = "realm_sema_" + base_path + condvar_name;
    std::string eve = "realm_event_" + base_path + condvar_name;

    // UWP only has W versions of API.
    std::wstring se = std::wstring(sem.begin(), sem.end());
    std::wstring ev = std::wstring(eve.begin(), eve.end());

    m_sema = CreateSemaphoreW(
        nullptr,     // no security
        0,           // initially 0
        0x7fffffff,  // max count
        LPWSTR(se.c_str()));

    m_waiters_done = CreateEventW(
        nullptr,    // no security
        false,      // auto-reset
        false,      // non-signaled initially
        LPWSTR(ev.c_str()));

#endif // _WIN32
#endif // REALM_CONDVAR_EMULATION
}


void InterprocessCondVar::init_shared_part(SharedPart& shared_part)
{
#ifdef REALM_CONDVAR_EMULATION
#ifdef _WIN32
    shared_part.m_waiters_countlock.init_as_process_shared(true);
    shared_part.m_waiters_count = 0;
    shared_part.m_was_broadcast = 0;
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
    int64_t wait_milliseconds = INFINITE;

    if (tp) {
        timeval tp2;
        gettimeofday(&tp2, nullptr);
        wait_milliseconds = timediff(tp2, *tp);
        if (wait_milliseconds < 0)
            wait_milliseconds = 0;
    }

    m_shared_part->m_waiters_countlock.lock();
    m_shared_part->m_waiters_count++;
    m_shared_part->m_waiters_countlock.unlock();

    m.unlock();
    WaitForSingleObject(m_sema, DWORD(wait_milliseconds));

    // Reacquire lock to avoid race conditions.
    m_shared_part->m_waiters_countlock.lock();

    // We're no longer waiting...
    m_shared_part->m_waiters_count--;

    // Check to see if we're the last waiter after notify_all().
    int last_waiter = m_shared_part->m_was_broadcast && m_shared_part->m_waiters_count == 0;

    m_shared_part->m_waiters_countlock.unlock();

    // If we're the last waiter thread during this particular broadcast then let all the other threads proceed.
    if (last_waiter) {
        // This call atomically signals the <m_waiters_done> event and waits until it can acquire the 
        // external mutex. This is required to ensure fairness.
        SetEvent(m_waiters_done);
        m.lock();
    }
    else {
        // Always regain the external mutex since that's the guarantee we give to our callers. 
        m.lock();
    }
    return;
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

                long milliseconds = timediff(tv, *tp);
                if (milliseconds < 0) { // negative timeout will mean no timeout. We don't want that.
                    milliseconds = 0;
                }
                REALM_ASSERT_DEBUG(!util::int_cast_has_overflow<int>(milliseconds));
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
    m_shared_part->wait(*m.m_shared_part, []() {}, tp);
#endif

}


// Notify:
// precondition: The caller holds the mutex guarding the condition variable.
// operation: If a waiter is present, we wake her up by writing a single
// byte to the fifo.

void InterprocessCondVar::notify() noexcept
{
    REALM_ASSERT(m_shared_part);
#ifdef REALM_CONDVAR_EMULATION
#ifdef _WIN32
    m_shared_part->m_waiters_countlock.lock();
    int have_waiters = m_shared_part->m_waiters_count > 0;
    m_shared_part->m_waiters_countlock.unlock();

    // If there aren't any waiters, then this is a no-op.  
    if (have_waiters) {
        ReleaseSemaphore(m_sema, 1, 0);
    }
#else
    if (m_shared_part->wait_counter > m_shared_part->signal_counter) {
        m_shared_part->signal_counter++;
        notify_fd(m_fd_write != -1 ? m_fd_write : m_fd_read);
    }
#endif
#else
    m_shared_part->notify();
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
    // This is needed to ensure that <m_waiters_count> and <m_was_broadcast> are
    // consistent relative to each other.
    m_shared_part->m_waiters_countlock.lock();
    int have_waiters = 0;

    if (m_shared_part->m_waiters_count > 0) {
        // We are broadcasting, even if there is just one waiter...
        // Record that we are broadcasting, which helps optimize
        // wait() for the non-broadcast case.
        m_shared_part->m_was_broadcast = 1;
        have_waiters = 1;
    }

    if (have_waiters) {
        // Wake up all the waiters atomically.
        ReleaseSemaphore(m_sema, m_shared_part->m_waiters_count, 0);
        m_shared_part->m_waiters_countlock.unlock();

        // Wait for all the awakened threads to acquire the counting
        // semaphore. 
        WaitForSingleObject(m_waiters_done, INFINITE);

        // This assignment is okay, even without the <m_waiters_countlock> held 
        // because no other waiter threads can wake up to access it.
        m_shared_part->m_was_broadcast = 0;
    }
    else {
        m_shared_part->m_waiters_countlock.unlock();
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
