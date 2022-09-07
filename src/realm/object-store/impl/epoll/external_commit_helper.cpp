////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/impl/external_commit_helper.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/util/fifo_helper.hpp>

#include <realm/util/assert.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/db.hpp>

#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <sstream>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <unistd.h>

#ifdef __ANDROID__
#include <android/log.h>
#define ANDROID_LOG __android_log_print
#else
#define ANDROID_LOG(...)
#endif

using namespace realm;
using namespace realm::_impl;

#define LOGE(...)                                                                                                    \
    do {                                                                                                             \
        fprintf(stderr, __VA_ARGS__);                                                                                \
        ANDROID_LOG(ANDROID_LOG_ERROR, "REALM", __VA_ARGS__);                                                        \
    } while (0)

namespace {

// Make writing to the pipe return -1 when there is no data to read, or no space in the buffer to write to, rather
// than blocking.
void make_non_blocking(int fd)
{
    int ret = fcntl(fd, F_SETFL, O_NONBLOCK);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }
}

// Write a byte to a pipe to notify anyone waiting for data on the pipe.
// But first consume all bytes in the pipe, since linux may only notify on transition from not ready to ready.
// If a process dies after reading but before writing, it can consume a pending notification, and possibly prevent
// other processes from observing it. This is a transient issue and the next notification will work correctly.
void notify_fd(int fd, bool read_first = true)
{
    while (true) {
        if (read_first) {
            while (true) {
                uint8_t buff[1024];
                ssize_t actual = read(fd, buff, sizeof(buff));
                if (actual == 0) {
                    break; // Not sure why we would see EOF here, but defer error handling to the writer.
                }
                if (actual < 0) {
                    int err = errno;
                    if (err == EWOULDBLOCK || err == EAGAIN)
                        break;
                    throw std::system_error(err, std::system_category());
                }
            }
        }

        char c = 0;
        ssize_t ret = write(fd, &c, 1);
        if (ret == 1) {
            break;
        }

        REALM_ASSERT_RELEASE(ret < 0);
        int err = errno;
        if (err == EWOULDBLOCK || err == EAGAIN) {
            REALM_ASSERT_RELEASE(read_first); // otherwise this is just an infinite loop.
            continue;
        }
        throw std::system_error(err, std::system_category());
    }
}

class DaemonThread {
public:
    DaemonThread();
    ~DaemonThread();

    void add(int fd, RealmCoordinator*) REQUIRES(!m_mutex);
    void remove(int fd, RealmCoordinator*) REQUIRES(!m_mutex, !m_running_on_change_mutex);

    static DaemonThread& shared();

private:
    void listen() REQUIRES(!m_mutex, !m_running_on_change_mutex);

    // The listener thread
    std::thread m_thread;
    // File descriptor for epoll
    FdHolder m_epoll_fd;
    // The two ends of an anonymous pipe used to notify the thread that it should be shut down.
    FdHolder m_shutdown_read_fd;
    FdHolder m_shutdown_write_fd;

    util::CheckedMutex m_mutex;
    // Safely removing things from epoll is somewhat difficult. epoll_ctl
    // itself is thread-safe, but EPOLL_CTL_DEL does not remove the fd from the
    // ready list, and of course we may be processing an event on the fd at the
    // same time as it's removed. To deal with this, we keep track of the
    // currently RealmCoordinators and when we get an event, check that the
    // pointer is still in this vector while holding the mutex.
    std::vector<RealmCoordinator*> m_live_coordinators GUARDED_BY(m_mutex);

    // We want destroying an ExternalCommitHelper to block if it's currently
    // running on a background thread to ensure that `Realm::close()`
    // synchronously closes the file even if notifiers are currently running.
    // To avoid lock-order inversions, this needs to be done with a separate
    // mutex from the one which guards `m_live_notifiers`.
    util::CheckedMutex m_running_on_change_mutex;
};

DaemonThread::DaemonThread()
{
    m_epoll_fd = epoll_create(1);
    if (m_epoll_fd == -1) {
        throw std::system_error(errno, std::system_category());
    }

    // Create the anonymous pipe
    int pipe_fd[2];
    int ret = pipe(pipe_fd);
    if (ret == -1) {
        throw std::system_error(errno, std::system_category());
    }

    m_shutdown_read_fd = pipe_fd[0];
    m_shutdown_write_fd = pipe_fd[1];

    make_non_blocking(m_shutdown_read_fd);
    make_non_blocking(m_shutdown_write_fd);

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.ptr = this;
    ret = epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_shutdown_read_fd, &event);
    if (ret != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }

    m_thread = std::thread([this] {
        try {
            listen();
        }
        catch (std::exception const& e) {
            LOGE("uncaught exception in notifier thread: %s: %s\n", typeid(e).name(), e.what());
            throw;
        }
        catch (...) {
            LOGE("uncaught exception in notifier thread\n");
            throw;
        }
    });
}

DaemonThread::~DaemonThread()
{
    // Not reading first since we know we have never written, and it is illegal
    // to read from the write-side of the pipe. Unlike a fifo, where in and out
    // sides share an fd, with an anonymous pipe, they each have a dedicated fd.
    notify_fd(m_shutdown_write_fd, /*read_first=*/false);
    m_thread.join(); // Wait for the thread to exit
}

DaemonThread& DaemonThread::shared()
{
    static DaemonThread daemon_thread;
    return daemon_thread;
}

void DaemonThread::add(int fd, RealmCoordinator* coordinator)
{
    {
        util::CheckedLockGuard lock(m_mutex);
        m_live_coordinators.push_back(coordinator);
    }

    epoll_event event{};
    event.events = EPOLLIN | EPOLLET;
    event.data.ptr = coordinator;
    int ret = epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, fd, &event);
    if (ret != 0) {
        int err = errno;
        throw std::system_error(err, std::system_category());
    }
}

void DaemonThread::remove(int fd, RealmCoordinator* coordinator)
{
    {
        util::CheckedLockGuard lock_1(m_running_on_change_mutex);
        util::CheckedLockGuard lock_2(m_mutex);
        // std::erase(m_live_coordinators, coordinator); in c++20
        auto it = std::find(m_live_coordinators.begin(), m_live_coordinators.end(), coordinator);
        if (it != m_live_coordinators.end()) {
            m_live_coordinators.erase(it);
        }
    }
    epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
}

void DaemonThread::listen()
{
    pthread_setname_np(pthread_self(), "Realm notification listener");

    int ret;

    while (true) {
        epoll_event ev{};
        ret = epoll_wait(m_epoll_fd, &ev, 1, -1);

        if (ret == -1 && errno == EINTR) {
            // Interrupted system call, try again.
            continue;
        }

        if (ret == -1) {
            int err = errno;
            throw std::system_error(err, std::system_category());
        }
        if (ret == 0) {
            // Spurious wakeup; just wait again
            continue;
        }

        if (ev.data.ptr == this) {
            // Shutdown fd was notified, so exit
            return;
        }

        // One of the ExternalCommitHelper fds was notified. We need to check
        // if the target is still alive while holding m_mutex, but we can't
        // hold it while calling on_change() as that would lead to a lock-order
        // inversions with one of RealmCoordinator's mutexes.
        // m_running_on_change_mutex guarantees that the coordinator is not
        // torn down while we're inside on_change(), while allowing new
        // coordinators to be added.
        util::CheckedLockGuard lock(m_running_on_change_mutex);
        RealmCoordinator* coordinator = static_cast<RealmCoordinator*>(ev.data.ptr);
        {
            util::CheckedLockGuard lock(m_mutex);
            auto it = std::find(m_live_coordinators.begin(), m_live_coordinators.end(), coordinator);
            if (it == m_live_coordinators.end()) {
                continue;
            }
        }

        coordinator->on_change();
    }
}

} // anonymous namespace

void FdHolder::close()
{
    if (m_fd != -1) {
        ::close(m_fd);
    }
    m_fd = -1;
}

ExternalCommitHelper::ExternalCommitHelper(RealmCoordinator& parent, const RealmConfig& config)
    : m_parent(parent)
{
    std::string path;
    std::string temp_dir = util::normalize_dir(config.fifo_files_fallback_path);
    std::string sys_temp_dir = util::normalize_dir(DBOptions::get_sys_tmp_dir());

    // Object Store needs to create a named pipe in order to coordinate notifications.
    // This can be a problem on some file systems (e.g. FAT32) or due to security policies in SELinux. Most commonly
    // it is a problem when saving Realms on external storage:
    // https://stackoverflow.com/questions/2740321/how-to-create-named-pipe-mkfifo-in-android
    //
    // For this reason we attempt to create this file in a temporary location known to be safe to write these files.
    //
    // In order of priority we attempt to write the file in the following locations:
    //  1) Next to the Realm file itself
    //  2) A location defined by `Realm::Config::fifo_files_fallback_path`
    //  3) A location defined by `DBOptions::set_sys_tmp_dir()`
    //
    // Core has a similar policy for its named pipes.
    //
    // Also see https://github.com/realm/realm-java/issues/3140
    // Note that hash collisions are okay here because they just result in doing extra work instead of resulting
    // in correctness problems.

    path = config.path + ".note";
    bool fifo_created = util::try_create_fifo(path);
    if (!fifo_created && !temp_dir.empty()) {
        path = util::format("%1realm_%2.note", temp_dir, std::hash<std::string>()(config.path));
        fifo_created = util::try_create_fifo(path);
    }
    if (!fifo_created && !sys_temp_dir.empty()) {
        path = util::format("%1realm_%2.note", sys_temp_dir, std::hash<std::string>()(config.path));
        util::create_fifo(path);
    }

    m_notify_fd = open(path.c_str(), O_RDWR);
    if (m_notify_fd == -1) {
        throw std::system_error(errno, std::system_category());
    }

    make_non_blocking(m_notify_fd);

    DaemonThread::shared().add(m_notify_fd, &m_parent);
}

ExternalCommitHelper::~ExternalCommitHelper()
{
    DaemonThread::shared().remove(m_notify_fd, &m_parent);
}

void ExternalCommitHelper::notify_others()
{
    notify_fd(m_notify_fd);
}
