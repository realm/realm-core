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

    /// Called for any error that is not an in-band WebSocket close message.
    ///
    /// Implementations such as browsers that cannot distinguish network errors from close messages should use
    /// websocket_close_message_received() with code 1005.
    ///
    /// The error_code is primarily for logging, callers do not need to put any specific codes for specific cases. All
    /// cases that need to be handled specially have their own methods. The provided code will be passed to any
    /// completion handlers attached to the realm::SyncSession if this causes them to fail.
    ///
    /// Pass an empty message if there is nothing to add to what is in the error_code.
    ///
    /// Until REALMC-10531 is resolved, HTTP 401 errors must be reported via websocket_401_unauthorized_error_handler.
    virtual void websocket_network_error_handler(std::error_code, StringData message) = 0;

    /// This is called when the server replies to the websocket HTTP handshake with a 401 unauthorized error.
    ///
    /// TODO remove this once REALMC-10531 is resolved.
    virtual void websocket_401_unauthorized_error_handler() = 0;

    //@{
    /// The callback functions below are called whenever a full binary or close message has arrived.
    ///
    /// Other message kinds should not be reported to this observer.
    ///
    /// \param data size The message is delivered in this buffer
    /// The buffer is only valid until the function returns.
    /// \return value designates whether the WebSocket object should continue
    /// processing messages. The normal return value is true. False must be returned if the
    /// websocket object is destroyed during execution of the function.
    virtual bool websocket_binary_message_received(const char* data, size_t size) = 0;
    virtual bool websocket_close_message_received(int error_code, StringData message) = 0;
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
