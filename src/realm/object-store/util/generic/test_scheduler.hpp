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

#pragma once

#include <deque>
#include <thread>

#include <realm/util/bind_ptr.hpp>
#include <realm/util/function_ref.hpp>
#include <realm/util/functional.hpp>
#include <realm/object-store/util/scheduler.hpp>

namespace realm {

class TestScheduler : public util::Scheduler {
public:
    TestScheduler() = default;

    bool is_on_thread() const noexcept override
    {
        return m_id == std::this_thread::get_id();
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        auto o = dynamic_cast<const TestScheduler*>(other);
        return (o && (o->m_id == m_id));
    }

    bool can_deliver_notifications() const noexcept override
    {
        return true;
    }

    bool can_schedule_writes() const noexcept override
    {
        return true;
    }

    bool can_schedule_completions() const noexcept override
    {
        return true;
    }

    void set_notify_callback(std::function<void()> cb) override
    {
        m_notification_cb = std::move(cb);
    }

    void set_schedule_writes_callback(util::UniqueFunction<void()> cb) override
    {
        m_write_cb = std::move(cb);
    }

    void set_schedule_completions_callback(util::UniqueFunction<void()> cb) override
    {
        m_completion_cb = std::move(cb);
    }

    void notify() override
    {
        if (!m_notification_cb) {
            return;
        }

        std::lock_guard<std::mutex> lk(m_work_items_mutex);
        m_work_items.push_back(WorkItem(util::FunctionRef<void()>(m_notification_cb)));
    }

    void schedule_writes() override
    {
        if (!m_write_cb) {
            return;
        }

        std::lock_guard<std::mutex> lk(m_work_items_mutex);
        m_work_items.push_back(WorkItem(util::FunctionRef<void()>(m_write_cb)));
    }

    void schedule_completions() override
    {
        if (!m_completion_cb) {
            return;
        }

        std::lock_guard<std::mutex> lk(m_work_items_mutex);
        m_work_items.push_back(WorkItem(util::FunctionRef<void()>(m_completion_cb)));
    }

    void perform(util::UniqueFunction<void()> cb)
    {
        std::lock_guard<std::mutex> lk(m_work_items_mutex);
        m_work_items.push_back(WorkItem(std::move(cb)));
    }

    void run_until(util::UniqueFunction<bool()> pred)
    {
        while (!pred()) {
            std::unique_lock<std::mutex> lk(m_work_items_mutex);
            auto front = std::move(m_work_items.front());
            m_work_items.pop_front();
            front.cb();
        }
    }

private:
    util::UniqueFunction<void()> m_notification_cb;
    util::UniqueFunction<void()> m_write_cb;
    util::UniqueFunction<void()> m_completion_cb;

    struct WorkItem {
        explicit WorkItem(util::FunctionRef<void()> ref)
            : cb(ref)
        {
        }

        explicit WorkItem(util::UniqueFunction<void()> fn)
            : owned(std::move(fn))
            , cb(owned)
        {
        }

        util::UniqueFunction<void()> owned;
        util::FunctionRef<void()> cb;
    };
    std::mutex m_work_items_mutex;
    std::deque<WorkItem> m_work_items;

    std::thread::id m_id = std::this_thread::get_id();
};

} // namespace realm
