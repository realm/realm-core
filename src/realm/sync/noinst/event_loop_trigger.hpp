#pragma once

#include <atomic>

#include "realm/util/bind_ptr.hpp"
#include "realm/util/functional.hpp"

namespace realm::sync {

/// \brief Register a function whose invocation can be triggered repeatedly.
///
/// While the function is always executed by the event loop thread, the
/// triggering of its execution can be done by any thread, and the triggering
/// operation is guaranteed to never throw.
///
/// The function is guaranteed to not be called after the Trigger object is
/// destroyed.
///
/// It is safe to destroy the Trigger object during execution of the function.
///
/// Note that even though the trigger() function is thread-safe, the Trigger
/// object, as a whole, is not. In particular, construction and destruction must
/// not be considered thread-safe.
///
/// ### Relation to post()
///
/// For a particular execution of trigger() and a particular invocation of
/// Service::post(), if the execution of trigger() ends before the execution of
/// Service::post() begins, then it is guaranteed that the function associated
/// with the trigger gets to execute at least once after the execution of
/// trigger() begins, and before the post handler gets to execute.
template <typename EventLoop>
class EventLoopTrigger {
public:
    template <typename Func>
    EventLoopTrigger(EventLoop& loop, Func&& handler)
        : m_loop(&loop)
        , m_handler(util::make_bind<TriggerHolder>(std::forward<Func>(handler)))
    {
    }

    ~EventLoopTrigger()
    {
        if (!m_handler) {
            return;
        }
        m_handler->state = State::Destroyed;
    }

    EventLoopTrigger() = default;

    EventLoopTrigger(const EventLoopTrigger&) = default;
    EventLoopTrigger& operator=(const EventLoopTrigger&) = default;
    EventLoopTrigger(EventLoopTrigger&&) = default;
    EventLoopTrigger& operator=(EventLoopTrigger&&) = default;

    /// \brief Trigger another invocation of the associated function.
    ///
    /// An invocation of trigger() puts the Trigger object into the triggered
    /// state. It remains in the triggered state until shortly before the
    /// function starts to execute. While the Trigger object is in the triggered
    /// state, trigger() has no effect. This means that the number of executions
    /// of the function will generally be less that the number of times
    /// trigger() is invoked().
    ///
    /// A particular invocation of trigger() ensures that there will be at least
    /// one invocation of the associated function whose execution begins after
    /// the beginning of the execution of trigger(), so long as the event loop
    /// thread does not exit prematurely from run().
    ///
    /// If trigger() is invoked from the event loop thread, the next execution
    /// of the associated function will not begin until after trigger returns(),
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
    void trigger()
    {
        State expected_state = State::Idle;
        if (m_handler->state.compare_exchange_strong(expected_state, State::Triggered)) {
            m_loop->post([handler = m_handler] {
                State expected_state = State::Triggered;
                if (!handler->state.compare_exchange_strong(expected_state, State::Idle)) {
                    return;
                }
                handler->handler();
            });
        }
    }

private:
    enum class State { Idle, Triggered, Destroyed };

    struct TriggerHolder : public util::AtomicRefCountBase {
        template <typename Func>
        TriggerHolder(Func&& handler)
            : handler(std::forward<Func>(handler))
        {
        }


        std::atomic<State> state = State::Idle;
        util::UniqueFunction<void()> handler;
    };

    EventLoop* m_loop = nullptr;
    util::bind_ptr<TriggerHolder> m_handler;
};

} // namespace realm::sync
