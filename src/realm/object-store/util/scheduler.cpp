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

#include <realm/object-store/util/scheduler.hpp>
#include <realm/util/terminate.hpp>
#include <realm/version_id.hpp>

#if REALM_HAVE_UV
#include <realm/object-store/util/uv/scheduler.hpp>
#endif

#if REALM_PLATFORM_APPLE
#include <realm/object-store/util/apple/scheduler.hpp>
#endif

#if REALM_ANDROID
#include <realm/object-store/util/android/scheduler.hpp>
#endif

#include <realm/object-store/util/generic/scheduler.hpp>

namespace realm::util {
namespace {

util::UniqueFunction<std::shared_ptr<Scheduler>()> s_factory = &Scheduler::make_platform_default;

class FrozenScheduler : public util::Scheduler {
public:
    FrozenScheduler(VersionID version)
        : m_version(version)
    {
    }

    void invoke(UniqueFunction<void()>&&) override {}
    bool is_on_thread() const noexcept override
    {
        return true;
    }
    bool is_same_as(const Scheduler* other) const noexcept override
    {
        auto o = dynamic_cast<const FrozenScheduler*>(other);
        return (o && (o->m_version == m_version));
    }
    bool can_invoke() const noexcept override
    {
        return false;
    }

private:
    VersionID m_version;
};
} // anonymous namespace

void InvocationQueue::push(util::UniqueFunction<void()>&& fn)
{
    std::lock_guard lock(m_mutex);
    m_functions.push_back(std::move(fn));
}

void InvocationQueue::invoke_all()
{
    std::vector<util::UniqueFunction<void()>> functions;
    {
        std::lock_guard lock(m_mutex);
        functions.swap(m_functions);
    }
    for (auto&& fn : functions) {
        fn();
    }
}

Scheduler::~Scheduler() = default;

void Scheduler::set_default_factory(util::UniqueFunction<std::shared_ptr<Scheduler>()> factory)
{
    s_factory = std::move(factory);
}

std::shared_ptr<Scheduler> Scheduler::make_default()
{
    return s_factory();
}

std::shared_ptr<Scheduler> Scheduler::make_platform_default()
{
#if REALM_USE_UV
    return make_uv();
#else
#if REALM_PLATFORM_APPLE
    return make_runloop(nullptr);
#elif REALM_ANDROID
    return make_alooper();
#else
    REALM_TERMINATE("No built-in scheduler implementation for this platform. Register your own with "
                    "Scheduler::set_default_factory()");
#endif
#endif // REALM_USE_UV
}

std::shared_ptr<Scheduler> Scheduler::make_generic()
{
    return std::make_shared<GenericScheduler>();
}

std::shared_ptr<Scheduler> Scheduler::make_frozen(VersionID version)
{
    return std::make_shared<FrozenScheduler>(version);
}

#if REALM_PLATFORM_APPLE
std::shared_ptr<Scheduler> Scheduler::make_runloop(CFRunLoopRef run_loop)
{
    return std::make_shared<RunLoopScheduler>(run_loop ?: CFRunLoopGetCurrent());
}

std::shared_ptr<Scheduler> Scheduler::make_dispatch(void* queue)
{
    return std::make_shared<DispatchQueueScheduler>(static_cast<dispatch_queue_t>(queue));
}
#endif // REALM_PLATFORM_APPLE

#if REALM_ANDROID
std::shared_ptr<Scheduler> Scheduler::make_alooper()
{
    return std::make_shared<ALooperScheduler>();
}
#endif // REALM_ANDROID

#if REALM_HAVE_UV
std::shared_ptr<Scheduler> Scheduler::make_uv()
{
    return std::make_shared<UvMainLoopScheduler>();
}
#endif // REALM_HAVE_UV

} // namespace realm::util
