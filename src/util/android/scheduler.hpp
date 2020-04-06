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

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <android/log.h>
#include <android/looper.h>

#define LOGE(...) do { \
    fprintf(stderr, __VA_ARGS__); \
    __android_log_print(ANDROID_LOG_ERROR, "REALM", __VA_ARGS__); \
} while (0)

namespace {
using namespace realm;

// Write a byte to a pipe to notify anyone waiting for data on the pipe
void notify_fd(int write_fd)
{
    char c = 0;
    ssize_t ret = write(write_fd, &c, 1);
    if (ret == 1) {
        return;
    }

    // If the pipe's buffer is full, ALOOPER_EVENT_INPUT will be triggered anyway. Also the buffer clearing happens
    // before calling the callback. So after this call, the callback will be called. Just return here.
    if (ret != 0) {
        int err = errno;
        if (err != EAGAIN) {
            throw std::system_error(err, std::system_category());
        }
    }
}

// ALooper doesn't have any functionality for managing the lifetime of the data
// pointer object for the callback, and also doesn't make any guarantees about
// the behavior that make it possible to safely manage the lifetime externally.
//
// Our solution is to keep track of the addresses of currently live schedulers.
// The looper callback checks if the data pointer is in this list, and if so
// attempts to acquire a strong reference to it. If the callback runs after
// the scheduler's destructor the first check will fail; if it runs during the
// scheduler's destructor the second will fail.
//
// There is a possible false-positive here where a scheduler could be destroyed
// and then a new one is allocated at the same memory address, which will result
// in the new one's callback being invoked.
std::mutex s_schedulers_mutex;
std::vector<void*> s_live_schedulers;

class ALooperScheduler : public util::Scheduler, public std::enable_shared_from_this<ALooperScheduler> {
public:
    ALooperScheduler()
    {
        if (m_looper)
            ALooper_acquire(m_looper);
    }

    ~ALooperScheduler()
    {
        if (!m_looper)
            return;

        if (m_initialized) {
            ALooper_removeFd(m_looper, m_message_pipe.read);
            ::close(m_message_pipe.write);
            ::close(m_message_pipe.read);
            {
                std::unique_lock<std::mutex> lock(s_schedulers_mutex);
                s_live_schedulers.erase(std::remove(s_live_schedulers.begin(), s_live_schedulers.end(), this),
                                        s_live_schedulers.end());
            }
        }
        ALooper_release(m_looper);
    }

    void set_notify_callback(std::function<void()> fn) override
    {
        m_callback = std::move(fn);
    }

    void notify() override
    {
        if (m_looper) {
            init();
            notify_fd(m_message_pipe.write);
        }
    }

    bool can_deliver_notifications() const noexcept override { return true; }
    bool is_on_thread() const noexcept override
    {
        return m_thread == pthread_self();
    }

private:
    std::function<void()> m_callback;
    ALooper* m_looper = ALooper_forThread();
    pthread_t m_thread = pthread_self();
    bool m_initialized = false;

    // pipe file descriptor pair we use to signal ALooper
    struct {
      int read = -1;
      int write = -1;
    } m_message_pipe;

    void init()
    {
        if (m_initialized)
            return;
        m_initialized = true;

        {
            std::unique_lock<std::mutex> lock(s_schedulers_mutex);
            s_live_schedulers.push_back(this);
        }

        int message_pipe[2];
        // pipe2 became part of bionic from API 9. But there are some devices with API > 10 that still have problems.
        // See https://github.com/realm/realm-java/issues/3945 .
        if (pipe(message_pipe)) {
            int err = errno;
            LOGE("could not create WeakRealmNotifier ALooper message pipe: %s.", strerror(err));
            return;
        }
        if (fcntl(message_pipe[0], F_SETFL, O_NONBLOCK) == -1 || fcntl(message_pipe[1], F_SETFL, O_NONBLOCK) == -1) {
            int err = errno;
            LOGE("could not set ALooper message pipe non-blocking: %s.", strerror(err));
            // It still works in blocking mode.
        }

        if (ALooper_addFd(m_looper, message_pipe[0], ALOOPER_POLL_CALLBACK,
                          ALOOPER_EVENT_INPUT,
                          &looper_callback, this) != 1) {
            LOGE("Error adding WeakRealmNotifier callback to looper.");
            ::close(message_pipe[0]);
            ::close(message_pipe[1]);

            return;
        }

        m_message_pipe.read = message_pipe[0];
        m_message_pipe.write = message_pipe[1];
    }

    static int looper_callback(int fd, int events, void* data)
    {
        if ((events & ALOOPER_EVENT_INPUT) != 0) {
            std::shared_ptr<ALooperScheduler> shared;
            {
                std::lock_guard<std::mutex> lock(s_schedulers_mutex);
                if (std::find(s_live_schedulers.begin(), s_live_schedulers.end(), data) != s_live_schedulers.end()) {
                    // Even if the weak_ptr can be found in the list, the object still can be destroyed in between.
                    // But share_ptr can ensure we either have a valid pointer or the object has gone.
                    shared = static_cast<ALooperScheduler*>(data)->shared_from_this();
                }
            }
            if (shared) {
                // Clear the buffer. Note that there might be a small chance than more than 1024 bytes left in the pipe,
                // but it is OK. Since we also want to support blocking read here.
                // Clear here instead of in the notify is because of whenever there are bytes left in the pipe, the
                // ALOOPER_EVENT_INPUT will be triggered.
                std::vector<uint8_t> buff(1024);
                read(fd, buff.data(), buff.size());
                // By holding a shared_ptr, this object won't be destroyed in the m_callback.
                shared->m_callback();
            }
        }

        if ((events & ALOOPER_EVENT_HANGUP) != 0) {
            return 0;
        }

        if ((events & ALOOPER_EVENT_ERROR) != 0) {
            LOGE("Unexpected error on WeakRealmNotifier's ALooper message pipe.");
        }

        // return 1 to continue receiving events
        return 1;
    }
};
} // anonymous namespace

namespace realm {
namespace util {
std::shared_ptr<Scheduler> Scheduler::make_default()
{
    return std::make_shared<ALooperScheduler>();
}
} // namespace util
} // namespace realm
