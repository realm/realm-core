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
#include <thread>
#include <uv.h>

namespace {
using namespace realm;

class UvMainLoopScheduler : public util::Scheduler {
public:
    UvMainLoopScheduler() = default;
    ~UvMainLoopScheduler()
    {
        if (m_handle && m_handle->data) {
            static_cast<Data*>(m_handle->data)->close_requested = true;
            uv_async_send(m_handle);
            // Don't delete anything here as we need to delete it from within the event loop instead
        }
        else {
            delete m_handle;
        }
    }

    bool is_on_thread() const noexcept override { return m_id == std::this_thread::get_id(); }
    bool can_deliver_notifications() const noexcept override { return true; }

    void set_notify_callback(std::function<void()> fn) override
    {
        m_handle = new uv_async_t;
        m_handle->data = new Data{std::move(fn), {false}};

        // This assumes that only one thread matters: the main thread (default loop).
        uv_async_init(uv_default_loop(), m_handle, [](uv_async_t* handle) {
            auto& data = *static_cast<Data*>(handle->data);
            if (data.close_requested) {
                uv_close(reinterpret_cast<uv_handle_t*>(handle), [](uv_handle_t* handle) {
                    delete reinterpret_cast<Data*>(handle->data);
                    delete reinterpret_cast<uv_async_t*>(handle);
                });
            } else {
                data.callback();
            }
        });
    }

    void notify() override
    {
        uv_async_send(m_handle);
    }

private:
    struct Data {
        std::function<void()> callback;
        std::atomic<bool> close_requested;
    };
    uv_async_t* m_handle = nullptr;
    std::thread::id m_id = std::this_thread::get_id();
};

} // anonymous namespace

namespace realm {
namespace util {
std::shared_ptr<Scheduler> Scheduler::make_default()
{
    return std::make_shared<UvMainLoopScheduler>();
}
} // namespace util
} // namespace realm
