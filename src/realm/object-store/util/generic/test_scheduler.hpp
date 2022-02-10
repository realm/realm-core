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

#include <mutex>
#include <thread>
#include <vector>

#include <realm/util/bind_ptr.hpp>
#include <realm/util/function_ref.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/object-store/util/scheduler.hpp>

namespace realm::util {

class TestScheduler : public util::Scheduler {
public:
    TestScheduler() = default;

    ~TestScheduler()
    {
        get_global_work_queue()->clear_for(m_id);
    }

    bool is_on_thread() const noexcept override
    {
        return m_id == std::this_thread::get_id();
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        auto o = dynamic_cast<const TestScheduler*>(other);
        return (o && o->m_id == m_id);
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
        if (m_notification_cb) {
            return;
        }
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

        get_global_work_queue()->add_item(WorkItem(m_id, util::FunctionRef<void()>(m_notification_cb)));
    }

    void schedule_writes() override
    {
        if (!m_write_cb) {
            return;
        }

        get_global_work_queue()->add_item(WorkItem(m_id, util::FunctionRef<void()>(m_write_cb)));
    }

    void schedule_completions() override
    {
        if (!m_completion_cb) {
            return;
        }

        get_global_work_queue()->add_item(WorkItem(m_id, util::FunctionRef<void()>(m_completion_cb)));
    }

    void perform(util::UniqueFunction<void()> cb)
    {
        get_global_work_queue()->add_item(WorkItem(m_id, std::move(cb)));
    }

    void run_until(util::FunctionRef<bool()> pred)
    {
        while (!pred()) {
            get_global_work_queue()->process_work_items();
        }
    }

private:
    struct WorkItem {
        explicit WorkItem(std::thread::id owner, util::FunctionRef<void()> ref)
            : owner(owner)
            , cb(ref)
        {
        }

        explicit WorkItem(std::thread::id owner, util::UniqueFunction<void()> fn)
            : owner(owner)
            , owned(std::move(fn))
            , cb(owned)
        {
        }

        std::thread::id owner;
        util::UniqueFunction<void()> owned;
        util::FunctionRef<void()> cb;
    };

    struct WorkQueue {
        mutable std::mutex m_mutex;
        std::vector<WorkItem> m_work_items;

        void add_item(WorkItem item)
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_work_items.push_back(std::move(item));
        }

        void clear_for(std::thread::id owner)
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_work_items.erase(std::remove_if(m_work_items.begin(), m_work_items.end(),
                                              [&](const auto& item) {
                                                  return (item.owner == owner);
                                              }),
                               m_work_items.end());
        }

        void process_work_items()
        {
            std::unique_lock<std::mutex> lk(m_mutex);
            std::vector<WorkItem> items;
            std::swap(m_work_items, items);
            lk.unlock();
            for (auto& item : items) {
                item.cb();
            }
        }
    };

    const std::shared_ptr<WorkQueue>& get_global_work_queue()
    {
        static std::shared_ptr<WorkQueue> g_work_queue = std::make_shared<WorkQueue>();
        return g_work_queue;
    }

    util::UniqueFunction<void()> m_notification_cb;
    util::UniqueFunction<void()> m_write_cb;
    util::UniqueFunction<void()> m_completion_cb;
    std::thread::id m_id = std::this_thread::get_id();
};

} // namespace realm::util
