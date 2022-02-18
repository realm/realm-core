#ifndef REALM_UTIL_EZ_WEBSOCKET_HPP
#define REALM_UTIL_EZ_WEBSOCKET_HPP

#include <random>
#include <system_error>
#include <map>

#include <realm/sync/config.hpp>
#include <realm/util/http.hpp>

namespace realm::util::network {
class Service;
}

namespace realm::util::websocket {
using port_type = sync::port_type;

// TODO figure out what belongs on config and what belongs on endpoint.
struct EZConfig {
    util::Logger& logger;
    std::mt19937_64& random;
    util::network::Service& service;
    std::string user_agent;
};

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

class EZSocket {
public:
    virtual ~EZSocket();

    virtual void async_write_binary(const char* data, size_t size, util::UniqueFunction<void()>&& handler) = 0;
};

class EZSocketFactory {
public:
    EZSocketFactory(EZConfig config)
        : m_config(config)
    {
    }

    EZSocketFactory(EZSocketFactory&&) = delete;

    std::unique_ptr<EZSocket> connect(EZObserver* observer, EZEndpoint&& endpoint);

private:
    EZConfig m_config;
};

} // namespace realm::util::websocket

#endif // REALM_UTIL_EZ_WEBSOCKET_HPP
