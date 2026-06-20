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
    template <typename Handler, typename... Args>
    Trigger(Service& service, Handler&& handler, Args&&... args);
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
    Service& m_service;

    struct HandlerBase : public util::AtomicRefCountBase {
        enum class State { Idle, Triggered, Destroyed };
        std::mutex mutex;
        State state = State::Idle;
        virtual void call() = 0;
    };

    template <typename Handler, typename... Args>
    struct HandlerImpl : HandlerBase {
        Handler handler;
        std::tuple<Args...> args;
        HandlerImpl(Handler&& h, Args&&... a)
            : handler(std::forward<Handler>(h))
            , args(std::forward<Args>(a)...)
        {
        }
        void call() override
        {
            std::apply(handler, args);
        }
    };
    util::bind_ptr<HandlerBase> m_handler;
};

template <class Service>
template <typename H, typename... A>
inline Trigger<Service>::Trigger(Service& service, H&& handler, A&&... args)
    : m_service(service)
    , m_handler(new HandlerImpl<H, A...>(std::forward<H>(handler), std::forward<A>(args)...))
{
}

template <class Service>
inline Trigger<Service>::~Trigger() noexcept
{
    if (m_handler) {
        std::lock_guard lock{m_handler->mutex};
        REALM_ASSERT(m_handler->state != HandlerBase::State::Destroyed);
        m_handler->state = HandlerBase::State::Destroyed;
    }
}

template <class Service>
inline void Trigger<Service>::trigger()
{
    REALM_ASSERT(m_handler);

    std::lock_guard lock{m_handler->mutex};
    REALM_ASSERT(m_handler->state != HandlerBase::State::Destroyed);

    if (m_handler->state == HandlerBase::State::Triggered) {
        return;
    }
    m_handler->state = HandlerBase::State::Triggered;

    m_service.post([handler = util::bind_ptr(m_handler)](Status status) {
        if (status == ErrorCodes::OperationAborted)
            return;
        if (!status.is_ok())
            throw Exception(status);

        {
            std::lock_guard lock{handler->mutex};
            // Do not execute the handler if the Trigger does not exist anymore.
            if (handler->state == HandlerBase::State::Destroyed) {
                return;
            }
            handler->state = HandlerBase::State::Idle;
        }
        handler->call();
    });
}

} // namespace realm::sync
