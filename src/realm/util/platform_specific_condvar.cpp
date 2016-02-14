/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <realm/util/platform_specific_condvar.hpp>

#include <fcntl.h>
#include <system_error>
#include <unistd.h>
#include <poll.h>

using namespace realm;
using namespace realm::util;

#ifdef REALM_CONDVAR_EMULATION

namespace {
// Write a byte to a pipe to notify anyone waiting for data on the pipe
void notify_fd(int fd)
{
    while (true) {
        char c = 0;
        ssize_t ret = write(fd, &c, 1);
        if (ret == 1) {
            break;
        }

        // If the pipe's buffer is full, we need to read some of the old data in
        // it to make space. We don't just read in the code waiting for
        // notifications so that we can notify multiple waiters with a single
        // write.
        REALM_ASSERT(ret == -1 && errno == EAGAIN);
        char buff[1024];
        int result = read(fd, buff, sizeof buff);
        static_cast<void>(result); // silence a warning
    }
}
} // anonymous namespace
#endif // REALM_CONDVAR_EMULATION


std::string PlatformSpecificCondVar::internal_naming_prefix = "/RealmsBigFriendlySemaphore";

void PlatformSpecificCondVar::set_resource_naming_prefix(std::string prefix)
{
    internal_naming_prefix = prefix + "RLM";
}

PlatformSpecificCondVar::PlatformSpecificCondVar()
{
}






void PlatformSpecificCondVar::close() noexcept
{
    if (uses_emulation) { // true if emulating a process shared condvar
        uses_emulation = false;
        ::close(m_fd_read);
        ::close(m_fd_write);
        return; // we don't need to clean up the SharedPart
    }
    // we don't do anything to the shared part, other CondVars may shared it
    m_shared_part = nullptr;
}


PlatformSpecificCondVar::~PlatformSpecificCondVar() noexcept
{
    close();
}



void PlatformSpecificCondVar::set_shared_part(SharedPart& shared_part, std::string base_path, size_t offset_of_condvar)
{
    close();
    uses_emulation = true;
    m_shared_part = &shared_part;
    static_cast<void>(base_path);
    static_cast<void>(offset_of_condvar);
#ifdef REALM_CONDVAR_EMULATION
#if !TARGET_OS_TV
    auto path = base_path + ".cv";

    // Create and open the named pipe
    int ret = mkfifo(path.c_str(), 0600);
    if (ret == -1) {
        int err = errno;
        if (err == ENOTSUP) {
            // Filesystem doesn't support named pipes, so try putting it in tmp instead
            // Hash collisions are okay here because they just result in doing
            // extra work, as opposed to correctness problems
            std::ostringstream ss;
            ss << getenv("TMPDIR");
            ss << "realm_" << std::hash<std::string>()(path) << ".cv";
            path = ss.str();
            ret = mkfifo(path.c_str(), 0600);
            err = errno;
        }
        // the fifo already existing isn't an error
        if (ret == -1 && err != EEXIST) {
            throw std::system_error(err, std::system_category());
        }
    }

    m_fd_write = open(path.c_str(), O_RDWR);
    if (m_fd_write == -1) {
        throw std::system_error(errno, std::system_category());
    }

    // Make writing to the pipe return -1 when the pipe's buffer is full
    // rather than blocking until there's space available
    ret = fcntl(m_fd_write, F_SETFL, O_NONBLOCK);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }

    m_fd_read = open(path.c_str(), O_RDONLY);
    if (m_fd_read == -1) {
        throw std::system_error(errno, std::system_category());
    }

    // Make reading from the pipe return -1 when the pipe's buffer is empty
    // rather than blocking until there's data available
    ret = fcntl(m_fd_read, F_SETFL, O_NONBLOCK);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }


#else // !TARGET_OS_TV

    // tvOS does not support named pipes, so use an anonymous pipe instead
    int notification_pipe[2];
    int ret = pipe(notification_pipe);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }

    m_fd_read = notification_pipe[0];
    m_fd_write = notification_pipe[1];

#endif // TARGET_OS_TV
#endif
}

void PlatformSpecificCondVar::init_shared_part(SharedPart& shared_part) {
#ifdef REALM_CONDVAR_EMULATION
    shared_part.wait_counter = 0;
    shared_part.signal_counter = 0;
#else
    new (&shared_part) CondVar(CondVar::process_shared_tag());
#endif // REALM_CONDVAR_EMULATION
}



void PlatformSpecificCondVar::wait(EmulatedRobustMutex& m, const struct timespec* tp)
{
    REALM_ASSERT(m_shared_part);
#ifdef REALM_CONDVAR_EMULATION
    uint64_t my_wait_counter = ++m_shared_part->wait_counter;
    for (;;) {

        struct pollfd poll_d;
        poll_d.fd = m_fd_read;
        poll_d.events = POLLIN;
        poll_d.revents = 0;

        m.unlock();
        int r;
        if (tp)
            r = poll(&poll_d, 1, tp->tv_sec*1000 + tp->tv_nsec/1000000);
        else
            r = poll(&poll_d, 1, -1);
        m.lock();
        uint64_t my_signal_counter = m_shared_part->signal_counter;

        // if wait returns with no ready fd, we retry
        if (r == 0)
            continue;

        if (r == -1) {
            // if wait returns due to a signal, we must retry:
            if (errno == EINTR)
                continue;
            // but if it returns due to timeout, we must exit the wait loop
            if (errno == ETIMEDOUT) {
                return;
            }
        }
        // If we've been woken up, but actually arrived later than the
        // signal sent, we allow someone else to catch the signal.
        if (my_signal_counter < my_wait_counter) {
            sched_yield();
            continue;
        }
        // We need to actually consume the pipe data, if not, subsequent
        // waits will have their call to poll() return immediately.
        char c;
        int ret = read(m_fd_read,&c,1);
        if (ret == -1)
            continue;
        return;
    }
#else
    m_shared_part->wait(*m.m_shared_part, [](){}, tp);
#endif
}






void PlatformSpecificCondVar::notify() noexcept
{
    REALM_ASSERT(m_shared_part);
#ifdef REALM_CONDVAR_EMULATION
//    m_shared_part->signal_counter++;
//    if (m_shared_part->waiters) {
        notify_fd(m_fd_write);
//        --m_shared_part->waiters;
//    }
#else
    m_shared_part->notify();
#endif
}





void PlatformSpecificCondVar::notify_all() noexcept
{
    REALM_ASSERT(m_shared_part);
#ifdef REALM_CONDVAR_EMULATION
    while (m_shared_part->wait_counter > m_shared_part->signal_counter) {
        m_shared_part->signal_counter++;
        notify_fd(m_fd_write);
//        --m_shared_part->waiters;
    }
#else
    m_shared_part->notify_all();
#endif
}


