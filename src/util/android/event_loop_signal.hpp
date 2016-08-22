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

#include <atomic>
#include <memory>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <android/log.h>
#include <android/looper.h>

#define LOGE(fmt...) do { \
    fprintf(stderr, fmt); \
    __android_log_print(ANDROID_LOG_ERROR, "REALM", fmt); \
} while (0)

namespace realm {
namespace util {
template<typename Callback>
class EventLoopSignal : public std::enable_shared_from_this<EventLoopSignal<Callback>> {
public:
    EventLoopSignal(Callback&& callback)
    : m_callback(std::move(callback))
    {
        ALooper* looper = ALooper_forThread();
        if (!looper) {
            return;
        }

        int message_pipe[2];
        if (pipe2(message_pipe, O_CLOEXEC | O_NONBLOCK)) {
            int err = errno;
            LOGE("could not create WeakRealmNotifier ALooper message pipe: %s", strerror(err));
            return;
        }

        if (ALooper_addFd(looper, message_pipe[0], 3 /* LOOPER_ID_USER */,
                          ALOOPER_EVENT_INPUT | ALOOPER_EVENT_HANGUP,
                          &looper_callback, nullptr) != 1) {
            LOGE("Error adding WeakRealmNotifier callback to looper.");
            ::close(message_pipe[0]);
            ::close(message_pipe[1]);

            return;
        }

        m_message_pipe.read = message_pipe[0];
        m_message_pipe.write = message_pipe[1];
        m_thread_has_looper = true;
    }

    ~EventLoopSignal()
    {
        bool flag = true;
        if (m_thread_has_looper.compare_exchange_strong(flag, false)) {
            // closing one end of the pipe here will trigger ALOOPER_EVENT_HANGUP
            //in the callback which will do the rest of the cleanup
            ::close(m_message_pipe.write);
        }
    }

    EventLoopSignal(EventLoopSignal&&) = delete;
    EventLoopSignal& operator=(EventLoopSignal&&) = delete;
    EventLoopSignal(EventLoopSignal const&) = delete;
    EventLoopSignal& operator=(EventLoopSignal const&) = delete;

    void notify()
    {
        if (m_thread_has_looper) {
            // Pass ourself over the pipe so that we can od work on the target
            // thread. This requires forming a new shared_ptr to ensure we
            // continue to exist until then.
            auto ptr = new std::shared_ptr<EventLoopSignal>(this->shared_from_this());
            if (write(m_message_pipe.write, &ptr, sizeof(ptr)) != sizeof(ptr)) {
                delete ptr;
                LOGE("Buffer overrun when writing to WeakRealmNotifier's ALooper message pipe.");
            }
        }
    }

private:
    Callback m_callback;
    std::atomic<bool> m_thread_has_looper{false};

    // pipe file descriptor pair we use to signal ALooper
    struct {
      int read = -1;
      int write = -1;
    } m_message_pipe;

    static int looper_callback(int fd, int events, void*)
    {
        if ((events & ALOOPER_EVENT_INPUT) != 0) {
            std::shared_ptr<EventLoopSignal>* ptr = nullptr;
            while (read(fd, &ptr, sizeof(ptr)) == sizeof(ptr)) {
                (*ptr)->m_callback();
                delete ptr;
            }
        }

        if ((events & ALOOPER_EVENT_HANGUP) != 0) {
            // this callback is always invoked on the looper thread so it's fine to get the looper like this
            ALooper_removeFd(ALooper_forThread(), fd);
            ::close(fd);
        }

        if ((events & ALOOPER_EVENT_ERROR) != 0) {
            LOGE("Unexpected error on WeakRealmNotifier's ALooper message pipe.");
        }

        // return 1 to continue receiving events
        return 1;
    }
};
} // namespace util
} // namespace realm
