////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#include <thread>
#include <realm/object-store/util/scheduler.hpp>

namespace realm::util {
class GenericScheduler : public realm::util::Scheduler {
public:
    GenericScheduler() = default;

    bool is_on_thread() const noexcept override
    {
        return m_id == std::this_thread::get_id();
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        auto o = dynamic_cast<const GenericScheduler*>(other);
        return (o && (o->m_id == m_id));
    }
    bool can_deliver_notifications() const noexcept override
    {
        return false;
    }

    void set_notify_callback(std::function<void()>) override {}
    void notify() override {}

    void schedule_writes() override {}
    void schedule_completions() override {}
    bool can_schedule_writes() const noexcept override
    {
        return false;
    }
    bool can_schedule_completions() const noexcept override
    {
        return false;
    }
    void set_schedule_writes_callback(std::function<void()>) override {}
    void set_schedule_completions_callback(std::function<void()>) override {}

private:
    std::thread::id m_id = std::this_thread::get_id();
};
} // namespace realm::util
