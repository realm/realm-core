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

// TODO figure out what belongs on config and what belongs on endpoint.
struct EZEndpoint {
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

class EZObserver {
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
    ~EZObserver() = default;
};

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
    network::Service& get_service()
    {
        return *m_service;
    }

    // Temporary workaround until Client::Connection is updated to use WebSocketObserver
    std::unique_ptr<WebSocketInterface> connect_legacy(EZObserver* observer, EZEndpoint&& endpoint);

    std::unique_ptr<WebSocketInterface> connect(WebSocketObserver*, WebSocketEndpoint&&) override
    {
        return nullptr;
    }

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
