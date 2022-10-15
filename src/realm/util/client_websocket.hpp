#ifndef REALM_UTIL_CLIENT_WEBSOCKET
#define REALM_UTIL_CLIENT_WEBSOCKET

#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <system_error>
#include <thread>
#include <utility>

#include <realm/sync/config.hpp>
#include <realm/util/http.hpp>

namespace realm::util::websocket {
using port_type = sync::port_type;


/// @brief Base class for the event loop used by the SyncClient
/// All callback operations to the SyncClient must be completed in the order in
/// which they were issued (via post(), trigger or timer) to the event loop and
/// cannot be run in parallel. It is up to the custom websocket implementation to
/// determine if these are run on the same thread or a thread pool as long as it
/// is guaranteed that no callback operations occur concurrently.
class EventLoopClient {
public:
    /// \brief The timer object used to track a timer that was started.
    ///
    /// This object provides the cancel() mechanism to cancel the timer. The callback handler for this timer
    /// will be called with the aborted error code if the timer is cancelled.
    class Timer {
    public:
        /// @brief Cancel the timer immediately. Does nothing if hte timer has already expired.
        virtual void cancel() noexcept = 0;
        /// Be sure to cancel the timer in the subclass destructor
        virtual ~Timer() noexcept = default;
    };

    /// \brief The trigger object used to activate the trigger callback handler.
    ///
    /// This object provides a trigger mechanism to run a callback handler one or more times. The trigger
    /// will remain registered and can be called multiple times until the Trigger object is destroyed.
    class Trigger {
    public:
        /// @brief Trigger the event that posts the handler onto the event loop.
        virtual void trigger() noexcept = 0;
        virtual ~Trigger() noexcept = default;
    };

    struct EventLoopObserver {
        // If set, this is called as soon as the event loop thread is started
        std::optional<util::UniqueFunction<void()>> starting_event_loop;
        // If set, this is called as the the event loop thread is existing
        std::optional<util::UniqueFunction<void()>> stopping_event_loop;
        // If set, this is called if an exception is thrown by the service in the event loop thread
        std::optional<util::UniqueFunction<void(std::exception const&)>> event_loop_error;
    };

    /// The event loop implementation must ensure the event loop is stopped and flushed when
    /// the object is destroyed.
    virtual ~EventLoopClient() = default;

    /// Register an observer that will be notified when the event loop starts to run, is about
    /// to exit, or if an exception occurs. This is primarily intended to be used with the
    /// default websocket factory implementation for testing.
    virtual void register_event_loop_observer(EventLoopObserver&&) {}

    /// Start the event loop - if any of the post, trigger or timer events are registered
    /// prior to calling start(), the event loop could be started early, depending on the
    /// implementation. Only one event thread loop will be running regardless of the
    /// number of calls to start().
    virtual void start() = 0;

    /// Stop the event loop - any future calls to post events or create timers or triggers
    /// will do nothing. Can be called from within the event loop thread.
    virtual void stop() = 0;

    /// Return true if the event loop has been stopped or is stopping.
    virtual bool is_stopped() = 0;

    /// \brief Submit a handler to be executed by the event loop thread.
    ///
    /// Register the sepcified completion handler for immediate asynchronous
    /// execution. The specified handler will be executed by an expression on
    /// the form `handler()`. If the the handler object is movable, it will
    /// never be copied. Otherwise, it will be copied as necessary.
    ///
    /// This function is thread-safe, that is, it may be called by any
    /// thread. It may also be called from other completion handlers.
    ///
    /// The handler will never be called as part of the execution of post(). It
    /// will always be called by a thread that is executing run(). If no thread
    /// is currently executing run(), the handler will not be executed until a
    /// thread starts executing run(). If post() is called while another thread
    /// is executing run(), the handler may be called before post() returns. If
    /// post() is called from another completion handler, the submitted handler
    /// is guaranteed to not be called during the execution of post().
    ///
    /// Completion handlers added through post() will be executed in the order
    /// that they are added. More precisely, if post() is called twice to add
    /// two handlers, A and B, and the execution of post(A) ends before the
    /// beginning of the execution of post(B), then A is guaranteed to execute
    /// before B.
    virtual void post(util::UniqueFunction<void()>&& handler) = 0;

    /// @brief Create and register a new timer on the event loop with the provided delay.
    /// This is a one shot timer and the Timer class returned becomes invalid once the
    /// timer has expired. A new timer will need to be created to wait for the timer again.
    /// @param delay The duration to wait before the timer expires.
    /// @param handler The function to be called when the timer expires.
    /// @return A pointer to the Timer object that can be used to cancel the timer. The
    ///         timer will also be canceled if the Timer object returned is destroyed.
    template <class R, class P>
    std::unique_ptr<Timer> create_timer(std::chrono::duration<R, P> delay,
                                        util::UniqueFunction<void(std::error_code)>&& handler)
    {
        return do_create_timer(std::chrono::duration_cast<std::chrono::milliseconds>(delay), std::move(handler));
    }

    /// @brief Create a trigger that posts a handler onto the event loop when the trigger()
    /// function is called on the Trigger object returned by this function.
    /// @param handler The function to be called when the Trigger is activated.
    /// @return A pointer to the Trigger object that can be used to trigger the event
    ///         and post the handler onto the event loop.
    virtual std::unique_ptr<Trigger> create_trigger(util::UniqueFunction<void()>&& handler) = 0;

protected:
    /// @brief Internal implementation for creating a timer.
    /// @param delay The time in milliseconds to wait before the timer expires.
    /// @param handler The function to be called when the timer expires.
    /// @return A pointer to the Timer object that can be used to cancel the timer.
    virtual std::unique_ptr<Timer> do_create_timer(std::chrono::milliseconds delay,
                                                   util::UniqueFunction<void(std::error_code)>&& handler) = 0;
};

// Defines to help make the code a bit cleaner
using EventLoopTimer = std::unique_ptr<EventLoopClient::Timer>;
using EventLoopTrigger = std::unique_ptr<EventLoopClient::Trigger>;

/// @brief Base class for the observer that receives the websocket events during operation.
/// IMPORTANT: These functions are called on the IO processing thread and not the event loop.
/// These handlers should do any necessary initialization and then post onto the event loop
/// to process the event. Failure to do so will cause poor performance of the websocket.
class WebSocketObserver {
public:
    /// websocket_handshake_completion_handler() is called when the websocket is connected, .i.e.
    /// after the handshake is done. It is not allowed to send messages on the socket before the
    /// handshake is done. No message_received callbacks will be called before the handshake is done.
    virtual void websocket_handshake_completion_handler(const std::string& protocol) = 0;

    //@{
    /// websocket_read_error_handler() and websocket_write_error_handler() are called when an
    /// error occurs on the underlying stream given by the async_read and async_write functions above.
    /// The error_code is passed through.
    ///
    /// websocket_handshake_error_handler() will be called when there is an error in the handshake
    /// such as "404 Not found".
    ///
    /// websocket_protocol_error_handler() is called when there is an protocol error in the incoming
    /// websocket messages.
    ///
    /// After calling any of these error callbacks, the Socket will move into the stopped state, and
    /// no more messages should be sent, or will be received.
    /// It is safe to destroy the WebSocket object in these handlers.
    /// TODO there are too many error handlers. Try to get down to just one.
    virtual void websocket_connect_error_handler(std::error_code) = 0;
    virtual void websocket_ssl_handshake_error_handler(std::error_code) = 0;
    virtual void websocket_read_or_write_error_handler(std::error_code) = 0;
    virtual void websocket_handshake_error_handler(std::error_code, const std::string_view* body) = 0;
    virtual void websocket_protocol_error_handler(std::error_code) = 0;
    //@}

    //@{
    /// The five callback functions below are called whenever a full message has arrived.
    /// The Socket defragments fragmented messages internally and delivers a full message.
    /// \param data size The message is delivered in this buffer
    /// The buffer is only valid until the function returns.
    /// \return value designates whether the WebSocket object should continue
    /// processing messages. The normal return value is true. False must be returned if the
    /// websocket object is destroyed during execution of the function.
    virtual bool websocket_binary_message_received(const char* data, size_t size) = 0;
    virtual bool websocket_close_message_received(std::error_code error_code, StringData message) = 0;
    //@}

protected:
    virtual ~WebSocketObserver() = default;
};

struct Endpoint {
    std::string address;
    port_type port;
    std::string path;      // Includes auth token in query.
    std::string protocols; // separated with ", "
    bool is_ssl;

    // The remaining fields are just passing through values from the SyncConfig. They can be ignored if SDK chooses
    // not to support the related config options. This may be necessary when using websocket libraries without
    // low-level control.
    std::map<std::string, std::string> headers; // Only includes "custom" headers.
    bool verify_servers_ssl_certificate;
    util::Optional<std::string> ssl_trust_certificate_path;
    std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback;
    util::Optional<SyncConfig::ProxyConfig> proxy;
};

class WebSocket {
public:
    WebSocket(const std::string& user_agent_string)
        : m_user_agent(user_agent_string)
    {
    }

    virtual ~WebSocket() = default;

    virtual void async_write_binary(const char* data, size_t size, util::UniqueFunction<void()>&& handler) = 0;

protected:
    // The User Agent string to include in the websocket headers
    std::string m_user_agent;
};

class WebSocketFactory {
public:
    WebSocketFactory(const std::string& user_agent_string)
        : m_user_agent(user_agent_string)
    {
    }

    virtual ~WebSocketFactory() = default;

    /// Create a new event loop object for posting events onto the event loop. This will only be called
    /// once per client instantiation, so a fresh event loop should be create with each call to this
    /// function.
    virtual std::shared_ptr<EventLoopClient> create_event_loop() = 0;

    /// Create a new websocket pointed to the server designated by endpoint. Any events that occur during
    /// the execution of the websocket will call directly to the handlers provided by the observer. These
    /// events are not posted to the event loop; it is up to the observer to post events on the event loop.
    virtual std::unique_ptr<WebSocket> connect(WebSocketObserver* observer, Endpoint&& endpoint) = 0;

protected:
    // The User Agent string to include in the websocket headers
    std::string m_user_agent;
};

} // namespace realm::util::websocket

#endif // REALM_UTIL_CLIENT_WEBSOCKET
