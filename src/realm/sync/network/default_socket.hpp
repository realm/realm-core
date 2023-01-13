#pragma once

#include <random>
#include <system_error>
#include <map>
#include <set>

#include <realm/sync/config.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/http.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/util/future.hpp>
#include <realm/util/random.hpp>

namespace realm::sync::network {
class Service;
} // namespace realm::sync::network

namespace realm::sync::websocket {
using port_type = sync::port_type;

class DefaultSocketProvider : public SyncSocketProvider {
public:
    class Timer : public SyncSocketProvider::Timer {
    public:
        friend class DefaultSocketProvider;

        /// Cancels the timer and destroys the timer instance.
        ~Timer() = default;

        /// Cancel the timer immediately
        void cancel() override
        {
            m_timer.cancel();
        }

    protected:
        Timer(network::Service& service, std::chrono::milliseconds delay, FunctionHandler&& handler)
            : m_timer{service}
        {
            m_timer.async_wait(delay, std::move(handler));
        }

    private:
        network::DeadlineTimer m_timer;
    };

    DefaultSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string user_agent);

    // Don't allow move or copy constructor
    DefaultSocketProvider(DefaultSocketProvider&&) = delete;

    ~DefaultSocketProvider();

    /// Temporary workaround until client shutdown has been updated in a separate PR - these functions
    /// will be handled internally when this happens.
    /// Stops the internal event loop (provided by network::Service)
    void stop(bool wait_for_stop = false) override;

    std::unique_ptr<WebSocketInterface> connect(WebSocketObserver*, WebSocketEndpoint&&) override;

    void post(FunctionHandler&& handler) override
    {
        // Don't post empty handlers onto the event loop
        if (!handler)
            return;
        m_service.post(std::move(handler));
    }

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) override
    {
        return std::unique_ptr<Timer>(new DefaultSocketProvider::Timer(m_service, delay, std::move(handler)));
    }

private:
    enum class State : int { NotStarted, Starting, Started, Running, Stopping, Stopped };

    // Start the event loop
    void start();

    bool state_check(State state);
    /// Return true if state matches any of the expected states
    bool state_check(std::set<State>&& expected_states);
    /// Only update the state and return true if it matches an expected state
    bool state_transition(std::set<State>&& expected_states, State new_state);
    /// Only update the state and return true if it matches the expected state
    bool state_transition(State expected_state, State new_state);
    /// Update the state
    void state_update(State new_state);
    /// Block until the state reaches the expected or later state - return true if state matches expected state
    bool state_wait_for(State expected_state);
    /// Internal function for updating the state and signaling the wait_for_state condvar
    void state_do_update(State new_state);

    std::function<void()> make_event_loop();

    // TODO: Revisit Service::run() so the keep running timer is no longer needed
    void start_keep_running_timer()
    {
        auto handler = [this](Status status) {
            if (status.code() != ErrorCodes::OperationAborted)
                start_keep_running_timer();
        };
        m_keep_running_timer = create_timer(std::chrono::hours(1000), std::move(handler)); // Throws
    }

    std::shared_ptr<util::Logger> m_logger_ptr;
    network::Service m_service;
    std::mt19937_64 m_random;
    const std::string m_user_agent;
    SyncTimer m_keep_running_timer;
    std::mutex m_state_mutex;
    State m_state;                      // protected by m_state_mutex
    std::condition_variable m_state_cv; // uses m_state_mutex
    std::mutex m_thread_mutex;
    std::thread m_thread; // protected by m_thread_mutex
};

} // namespace realm::sync::websocket
