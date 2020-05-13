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

namespace {
std::function<std::shared_ptr<realm::util::Scheduler>()> s_factory;

class GenericScheduler : public realm::util::Scheduler {
public:
    GenericScheduler() = default;

    bool is_on_thread() const noexcept override { return m_id == std::this_thread::get_id(); }
    bool can_deliver_notifications() const noexcept override { return false; }

    void set_notify_callback(std::function<void()>) override { }
    void notify() override { }

private:
    std::thread::id m_id = std::this_thread::get_id();
};
} // anonymous namespace

namespace realm {
namespace util {
void Scheduler::set_default_factory(std::function<std::shared_ptr<Scheduler>()> factory)
{
    s_factory = std::move(factory);
}

std::shared_ptr<Scheduler> Scheduler::make_default()
{
    return s_factory ? s_factory() : std::make_shared<GenericScheduler>();
}
} // namespace util
} // namespace realm
