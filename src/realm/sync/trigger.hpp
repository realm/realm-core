///////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
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
///////////////////////////////////////////////////////////////////////////////

#pragma once

#include <realm/status.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/sync/socket_provider.hpp>

namespace realm::sync {

/// Register a function whose invocation can be triggered repeatedly.
///
/// While the function is always executed by the event loop thread, the
/// triggering of its execution can be done by any thread. The event loop
/// is provided through Service type and it must have
/// `post(SyncSocketProvider::FunctionHandler&&)` method.
///
/// The function is guaranteed to not be called after the Trigger object is
/// destroyed.
///
/// Note that even though the trigger() function is thread-safe, the Trigger
/// object, as a whole, is not. In particular, construction and destruction must
/// not be considered thread-safe.
template <class Service>
class Trigger final {
public:
    Trigger(Service* service, SyncSocketProvider::FunctionHandler&& handler);
    ~Trigger() noexcept;

    Trigger() noexcept = delete;
    Trigger(Trigger&&) noexcept = default;
    Trigger& operator=(Trigger&&) noexcept = default;

    /// Trigger another invocation of the associated function.
    ///
    /// An invocation of trigger() puts the Trigger object into the triggered
    /// state. It remains in the triggered state until shortly before the
    /// function starts to execute. While the Trigger object is in the triggered
    /// state, trigger() has no effect. This means that the number of executions
    /// of the function will generally be less that the number of times
    /// trigger() is invoked.
    ///
    /// A particular invocation of trigger() ensures that there will be at least
    /// one invocation of the associated function whose execution begins after
    /// the beginning of the execution of trigger(), so long as the event loop
    /// thread does not exit prematurely from run().
    ///
    /// If trigger() is invoked from the event loop thread, the next execution
    /// of the associated function will not begin until after trigger() returns,
    /// effectively preventing reentrancy for the associated function.
    ///
    /// If trigger() is invoked from another thread, the associated function may
    /// start to execute before trigger() returns.
    ///
    /// Note that the associated function can retrigger itself, i.e., if the
    /// associated function calls trigger(), then that will lead to another
    /// invocation of the associated function, but not until the first
    /// invocation ends (no reentrance).
    ///
    /// This function is thread-safe.
    void trigger();

private:
    Service* m_service;

    struct HandlerInfo : public util::AtomicRefCountBase {
        enum class State { Idle, Triggered, Destroyed };

        HandlerInfo(SyncSocketProvider::FunctionHandler&& handler)
            : handler(std::move(handler))
            , state(State::Idle)
        {
        }

        SyncSocketProvider::FunctionHandler handler;
        util::Mutex mutex;
        State state;
    };
    util::bind_ptr<HandlerInfo> m_handler_info;
};

template <class Service>
inline Trigger<Service>::Trigger(Service* service, SyncSocketProvider::FunctionHandler&& handler)
    : m_service(service)
    , m_handler_info(new HandlerInfo(std::move(handler)))
{
}

template <class Service>
inline Trigger<Service>::~Trigger() noexcept
{
    if (m_handler_info) {
        util::LockGuard lock{m_handler_info->mutex};
        REALM_ASSERT(m_handler_info->state != HandlerInfo::State::Destroyed);
        m_handler_info->state = HandlerInfo::State::Destroyed;
    }
}

template <class Service>
inline void Trigger<Service>::trigger()
{
    REALM_ASSERT(m_service);
    REALM_ASSERT(m_handler_info);

    util::LockGuard lock{m_handler_info->mutex};
    REALM_ASSERT(m_handler_info->state != HandlerInfo::State::Destroyed);

    if (m_handler_info->state == HandlerInfo::State::Triggered) {
        return;
    }
    m_handler_info->state = HandlerInfo::State::Triggered;

    auto handler = [handler_info = util::bind_ptr(m_handler_info)](Status status) {
        {
            util::LockGuard lock{handler_info->mutex};
            // Do not execute the handler if the Trigger does not exist anymore.
            if (handler_info->state == HandlerInfo::State::Destroyed) {
                return;
            }
            handler_info->state = HandlerInfo::State::Idle;
        }
        handler_info->handler(status);
    };
    m_service->post(std::move(handler));
}

} // namespace realm::sync
