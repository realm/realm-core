#ifndef REALM_UTIL_CLIENT_EVENTLOOP
#define REALM_UTIL_CLIENT_EVENTLOOP

#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <system_error>
#include <thread>
#include <utility>

#include <realm/util/eventloop_observer.hpp>
#include <realm/util/functional.hpp>

namespace realm::util::websocket {

/// @brief Base class for the event loop used by the SyncClient.
///
/// All callback and event operations in the SyncClient must be completed in the
/// order in which they were issued (via post(), trigger or timer) to the event
/// loop and cannot be run in parallel. It is up to the custom websocket
/// implementation to determine if these are run on the same thread or a thread
/// pool as long as it is guaranteed that the callback handler functions are
/// processed in order and not run concurrently.
///
/// The implementation of an EventLoopClient must support the following
/// operations that post handler functions (provided by the Sync client) onto
/// the event loop:
/// * Post a handler function directly onto the event loop
/// * Post a handler function when the specified timer duration expires
/// * Post a cached handler function when the trigger() function is called
///
/// The event loop is not required to be a single thread as long as the
/// following requirements are satisfied:
/// * handler functions are called in the order they were posted to the event
/// loop queue, and
/// * a handler function runs to completion before the next handler function is
/// called.
///
/// WebSocket callbacks via the WebSocketObserver will post handler functions
/// onto the event loop, but the WebSocket internal operations are not required
/// to be handled by the same event loop
class EventLoopClient {
public:
    /// @brief The timer object used to track a timer that was started.
    ///
    /// This object provides the cancel() mechanism to cancel the timer. The
    /// callback handler for this timer will be called with the aborted error
    /// code if the timer is cancelled.
    ///
    /// Create a subclass of this class that provides access to the underlying
    /// implementation for cancelling the timer.
    struct Timer {
        /// Be sure to cancel the timer in the subclass destructor
        virtual ~Timer() = default;

        /// @brief Cancel the timer immediately. Does nothing if the timer has
        /// already expired.
        virtual void cancel() = 0;
    };

    /// @brief The trigger object used to activate the trigger callback handler.
    ///
    /// This object provides a trigger mechanism to run a callback handler one or
    /// more times. The trigger will remain registered and can be called multiple
    /// times until the Trigger object is destroyed.
    ///
    /// Create a subclass of this class that provides access to the underlying
    /// implementation for posting the handler function when trigger() is called.
    struct Trigger {
        virtual ~Trigger() = default;

        /// @brief Trigger the event that posts the handler onto the event loop.
        virtual void trigger() = 0;
    };

    /// The event loop implementation must ensure the event loop is stopped and
    /// flushed when the object is destroyed.
    virtual ~EventLoopClient() = default;

    /// Register an observer that will be notified when the event loop starts to
    /// run, is about to exit, or if an exception occurs.
    virtual void register_event_loop_observer(EventLoopObserver*) = 0;

    /// @brief Start the event loop
    ///
    /// Any calls to post(), create_timer() or create_trigger() prior to start()
    /// being called must be successful, however, anything posted onto the event
    /// loop should not be processed until after start() has been called. Only
    /// one event loop should be started regardless of the number of calls to
    /// start().
    virtual void start() = 0;

    /// @brief Stop the event loop
    ///
    /// Any future calls to post events or create timers or triggers will do
    /// nothing. Needs to support being called from within the event loop or any
    /// other thread. Any outstanding timers must be cancelled with an error code
    /// of util::error::operation_aborted so the handlers can properly clean up.
    /// Once stop() is called, the event loop will not be started again.
    virtual void stop() = 0;

    /// @brief Return true if the event loop has already been started.
    ///
    /// @return true if start() has already been called, otherwise false.
    virtual bool is_started() = 0;

    /// @brief Return true if the event loop has been stopped or is stopping, or
    /// return false if the event loop has not been started yet or is currently
    /// running.
    ///
    /// @return true if stop() has been called, otherwise true
    virtual bool is_stopped() = 0;

    /// @brief Submit a completion handler (function) to be executed by the
    /// event loop thread.
    ///
    /// Register the sepcified completion handler to be queued on the event loop
    /// for immediate asynchronous execution. The specified handler will be
    /// executed by an expression on the form `handler()`. If the the handler
    /// object is movable, it will never be copied. Otherwise, it will be copied
    /// as necessary.
    ///
    /// This function is thread-safe and can be called by any thread. It can
    /// also be called from other post() completion handlers.
    ///
    /// The handler will never be called as part of the execution of post(). It
    /// will always be called by the event loop that is started by calling
    /// EventLoopClient::start(). If the event loop has not been started, the
    /// handler will be queued until the event loop is started. If post() is
    /// called on a thread separate from the event loop, the handler may be
    /// called before post() returns.
    ///
    /// Completion handlers added through post() must be executed in the order
    /// that they are added. More precisely, if post() is called twice to add
    /// two handlers, A and B, and the execution of post(A) ends before the
    /// beginning of the execution of post(B), then A is guaranteed to execute
    /// before B.
    /// @param handler The completion handler function to be queued on the event
    ///                loop to be run.
    virtual void post(util::UniqueFunction<void()>&& handler) = 0;

    /// @brief Create and register a new timer whose completion handler will be
    /// posted to the event loop when the provided delay expires.
    ///
    /// This is a one shot timer and the Timer class returned becomes invalid
    /// once the timer has expired. A new timer will need to be created to wait
    /// again.
    /// @param delay The duration to wait before the timer expires.
    /// @param handler The completion handler to be called on the event loop
    ///                when the timer expires.
    /// @return A pointer to the Timer object that can be used to cancel the
    /// timer. The timer will also be canceled if the Timer object returned is
    /// destroyed.
    template <class R, class P>
    std::unique_ptr<Timer> create_timer(std::chrono::duration<R, P> delay,
                                        util::UniqueFunction<void(std::error_code)>&& handler)
    {
        return do_create_timer(std::chrono::duration_cast<std::chrono::milliseconds>(delay), std::move(handler));
    }

    /// @brief Create a trigger that posts a completion handler onto the event
    /// loop when the trigger is activated by calling `trigger()` on the Trigger
    /// object returned by this function.
    /// @param handler The completion handler to be called on the event loop
    ///                when the Trigger is activated.
    /// @return A pointer to a Trigger object that can be used to activate the
    ///         trigger.
    virtual std::unique_ptr<Trigger> create_trigger(util::UniqueFunction<void()>&& handler) = 0;

protected:
    /// @brief Internal implementation that creates a timer.
    /// @param delay The time in milliseconds to wait before the timer expires.
    /// @param handler The completion handler to be called on the event loop
    ///                when the timer expires.
    /// @return A pointer to a Timer object that can be used to cancel the
    ///         timer.
    virtual std::unique_ptr<Timer> do_create_timer(std::chrono::milliseconds delay,
                                                   util::UniqueFunction<void(std::error_code)>&& handler) = 0;
};

// Defines to help make the code a bit cleaner
using EventLoopTimer = std::unique_ptr<EventLoopClient::Timer>;
using EventLoopTrigger = std::unique_ptr<EventLoopClient::Trigger>;

} // namespace realm::util::websocket

#endif // REALM_UTIL_CLIENT_EVENTLOOP
