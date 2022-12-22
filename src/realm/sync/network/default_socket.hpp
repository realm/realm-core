#pragma once

#include <random>
#include <system_error>
#include <map>

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

    // The current state of the event loop
    enum State { NotStarted, Started, Running, Stopping, Stopped };

    DefaultSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string user_agent);

    // Don't allow move or copy constructor
    DefaultSocketProvider(DefaultSocketProvider&&) = delete;

    ~DefaultSocketProvider();

    /// @{
    /// Temporary workaround until client shutdown has been updated in a separate PR - these functions
    /// will be handled internally when this happens.
    /// Start the internal event loop (provided by network::Service) or restart the event loop
    /// if it has been previously stopped - returns whether or not the thread was started
    /// successfully.
    void start() override;
    /// Stop the internal event loop (provided by network::Service)
    void stop(bool wait_for_stop = false) override;
    /// }@

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
    void thread_update_state(State new_state)
    {
        std::lock_guard<std::mutex> lock{m_state_mutex};
        m_state = new_state;
    }

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
    State m_state;                                   // protected by m_state_mutex
    std::thread m_thread;                            // protected by m_state_mutex
    std::optional<util::Future<void>> m_stop_future; // protected by m_state_mutex
};

} // namespace realm::sync::websocket
