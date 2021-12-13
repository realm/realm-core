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
#include <realm/object-store/util/scheduler.hpp>
#include <realm/util/assert.hpp>

namespace realm::util {

class UvMainLoopScheduler : public util::Scheduler {
public:
    UvMainLoopScheduler() = default;
    void remove_handle(uv_async_t*& handle)
    {
        if (handle && handle->data) {
            static_cast<Data*>(handle->data)->close_requested = true;
            uv_async_send(handle);
            // Don't delete anything here as we need to delete it from within the event loop instead
        }
        else {
            delete handle;
        }
        handle = nullptr;
    }
    ~UvMainLoopScheduler()
    {
        remove_handle(m_notification_handle);
        remove_handle(m_write_handle);
        remove_handle(m_completion_handle);
    }

    bool is_on_thread() const noexcept override
    {
        return m_id == std::this_thread::get_id();
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        auto o = dynamic_cast<const UvMainLoopScheduler*>(other);
        return (o && (o->m_id == m_id));
    }
    bool can_deliver_notifications() const noexcept override
    {
        return true;
    }

    uv_async_t* add_callback(std::function<void()> fn)
    {
        auto the_handle = new uv_async_t;
        the_handle->data = new Data{std::move(fn), {false}};

        // This assumes that only one thread matters: the main thread (default loop).
        uv_async_init(uv_default_loop(), the_handle, [](uv_async_t* handle) {
            auto& data = *static_cast<Data*>(handle->data);
            if (data.close_requested) {
                uv_close(reinterpret_cast<uv_handle_t*>(handle), [](uv_handle_t* handle) {
                    delete reinterpret_cast<Data*>(handle->data);
                    delete reinterpret_cast<uv_async_t*>(handle);
                });
            }
            else {
                data.callback();
            }
        });
        return the_handle;
    }
    void set_notify_callback(std::function<void()> fn) override
    {
        if (m_notification_handle && m_notification_handle->data) {
            return; // danger!
            // We would like to, but cant do this check:
            // REALM_ASSERT((reinterpret_cast<Data*>(m_notification_handle->data)->callback) == fn);
        }
        m_notification_handle = add_callback(std::move(fn));
    }

    void notify() override
    {
        uv_async_send(m_notification_handle);
    }
    void schedule_writes() override
    {
        uv_async_send(m_write_handle);
    }
    void schedule_completions() override
    {
        uv_async_send(m_completion_handle);
    }
    bool can_schedule_writes() const noexcept override
    {
        return true;
    }
    bool can_schedule_completions() const noexcept override
    {
        return true;
    }
    void set_schedule_writes_callback(std::function<void()> fn) override
    {
        if (m_write_handle && m_write_handle->data) {
            return; // danger!
            // We would like to, but cant do this check:
            // REALM_ASSERT((reinterpret_cast<Data*>(m_notification_handle->data)->callback) == fn);
        }
        m_write_handle = add_callback(std::move(fn));
    }
    void set_schedule_completions_callback(std::function<void()> fn) override
    {
        if (m_completion_handle && m_completion_handle->data) {
            return; // danger!
            // We would like to, but cant do this check:
            // REALM_ASSERT((reinterpret_cast<Data*>(m_notification_handle->data)->callback) == fn);
        }
        m_completion_handle = add_callback(std::move(fn));
    }

    bool set_timeout_callback(uint64_t timeout, std::function<void()> fn) override
    {
        if (!m_timer || !m_timer->data) {
            auto m_timer = new uv_timer_t;
            m_timer->data = new Data{std::move(fn), {false}};

            uv_timer_init(uv_default_loop(), m_timer);
            uv_timer_start(
                m_timer,
                [](uv_timer_t* handle) {
                    auto& data = *static_cast<Data*>(handle->data);
                    data.callback();
                },
                timeout, 0);
        }

        return true;
    }

private:
    struct Data {
        std::function<void()> callback;
        std::atomic<bool> close_requested;
    };
    uv_async_t* m_notification_handle = nullptr;
    uv_async_t* m_write_handle = nullptr;
    uv_async_t* m_completion_handle = nullptr;
    uv_timer_t* m_timer = nullptr;
    std::thread::id m_id = std::this_thread::get_id();
};

} // namespace realm::util
