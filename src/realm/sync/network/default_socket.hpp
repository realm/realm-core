#pragma once

#include <random>
#include <system_error>
#include <map>

#include <realm/sync/config.hpp>
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/http.hpp>
#include <realm/sync/network/network.hpp>
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

    DefaultSocketProvider(const std::shared_ptr<util::Logger>& logger, const std::string user_agent)
        : m_logger_ptr{logger}
        , m_service{std::make_shared<network::Service>()}
        , m_random{}
        , m_user_agent{user_agent}
    {
        REALM_ASSERT(m_logger_ptr);                     // Make sure the logger is valid
        REALM_ASSERT(m_service);                        // Make sure the service is valid
        util::seed_prng_nondeterministically(m_random); // Throws
        start_keep_running_timer();
    }

    // Don't allow move or copy constructor
    DefaultSocketProvider(DefaultSocketProvider&&) = delete;

    // Temporary workaround until event loop is completely moved here
    void run() override
    {
        m_service->run();
    }

    void stop() override
    {
        m_service->stop();
    }

    std::unique_ptr<WebSocketInterface> connect(WebSocketObserver*, WebSocketEndpoint&&) override;

    void post(FunctionHandler&& handler) override
    {
        REALM_ASSERT(m_service);
        // Don't post empty handlers onto the event loop
        if (!handler)
            return;
        m_service->post(std::move(handler));
    }

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) override
    {
        return std::unique_ptr<Timer>(new DefaultSocketProvider::Timer(*m_service, delay, std::move(handler)));
    }

private:
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
    std::shared_ptr<network::Service> m_service;
    std::mt19937_64 m_random;
    const std::string m_user_agent;
    SyncTimer m_keep_running_timer;
};

} // namespace realm::sync::websocket
