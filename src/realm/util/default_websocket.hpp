#ifndef REALM_UTIL_DEFAULT_WEBSOCKET_HPP
#define REALM_UTIL_DEFAULT_WEBSOCKET_HPP

#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <system_error>
#include <thread>
#include <utility>

#include <realm/sync/config.hpp>
#include <realm/util/checked_mutex.hpp>
#include <realm/util/client_eventloop.hpp>
#include <realm/util/client_websocket.hpp>
#include <realm/util/http.hpp>
#include <realm/util/network.hpp>

namespace realm::util::network {
class Service;
}

namespace realm::util::websocket {

/// Base EventLoopClient class used by the DefaultWebSocketImpl to get the service instance
struct DefaultServiceClient : public EventLoopClient {
    virtual ~DefaultServiceClient() = default;

    // Return a reference to the network::Service owned by this instance
    virtual util::network::Service& get_service() = 0;
};


class DefaultWebSocketFactory : public WebSocketFactory {
public:
    DefaultWebSocketFactory(const std::string& user_agent_string, const std::shared_ptr<util::Logger>& logger)
        : m_user_agent(user_agent_string)
        , m_logger_ptr(logger)
    {
    }

    virtual ~DefaultWebSocketFactory()
    {
        if (m_event_loop != nullptr) {
            m_event_loop->stop();
        }
    }

    std::shared_ptr<EventLoopClient> create_event_loop() override;

    /// No default copy constructor
    DefaultWebSocketFactory(DefaultWebSocketFactory&) = delete;

    std::unique_ptr<WebSocket> connect(WebSocketObserver* observer, Endpoint&& endpoint) override;

private:
    std::string m_user_agent;
    std::shared_ptr<util::Logger> m_logger_ptr;
    std::shared_ptr<DefaultServiceClient> m_event_loop;
};

} // namespace realm::util::websocket

#endif // REALM_UTIL_DEFAULT_WEBSOCKET_HPP
