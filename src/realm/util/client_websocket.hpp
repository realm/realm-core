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
#include <realm/util/client_eventloop.hpp>
#include <realm/util/http.hpp>

namespace realm::util::websocket {
using port_type = sync::port_type;

/// @brief Base class for the observer that receives the websocket events during operation.
class WebSocketObserver {
public:
    virtual ~WebSocketObserver() = default;

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
    /// TODO: there are too many error handlers. Try to get down to just one.
    virtual void websocket_connect_error_handler(std::error_code) = 0;
    virtual void websocket_ssl_handshake_error_handler(std::error_code) = 0;
    virtual void websocket_read_or_write_error_handler(std::error_code) = 0;
    virtual void websocket_handshake_error_handler(std::error_code, const std::string_view* body) = 0;
    virtual void websocket_protocol_error_handler(std::error_code) = 0;
    //@}

    //@{
    /// These callback functions below are called whenever a full message has arrived.
    /// The Socket defragments fragmented messages internally and delivers a full message.
    ///
    /// @param data The message is delivered in this buffer. The buffer is only valid until
    ///             the function returns.
    /// @param size The number of bytes in the data buffer.
    ///
    /// @return bool designates whether the WebSocket object should continue processing
    /// messages. The normal return value is true. False must be returned if the websocket
    /// object has been destroyed during execution of the function.
    virtual bool websocket_binary_message_received(const char* data, size_t size) = 0;
    virtual bool websocket_close_message_received(std::error_code error_code, StringData message) = 0;
    //@}
};

struct Endpoint {
    std::string address;   // Host address
    port_type port;        // Host port number
    std::string path;      // Includes access token in query.
    std::string protocols; // One or more websocket protocols, separated with ", "
    bool is_ssl;           // true if SSL should be used

    // The remaining fields are just passing through values from the SyncConfig. They may not be provided if the
    // SDK chooses not to support the related config options. This may be necessary when using websocket libraries
    // without low-level control.
    std::map<std::string, std::string> headers; // Only includes "custom" headers.
    bool verify_servers_ssl_certificate; // If true, verify server ssl certificate when connecting
    util::Optional<std::string> ssl_trust_certificate_path;
    std::function<SyncConfig::SSLVerifyCallback> ssl_verify_callback;
    util::Optional<SyncConfig::ProxyConfig> proxy; // Send traffic through a network proxy
};

/// The WebSocket base class that is used by the SyncClient to send data over the WebSocket connection with the
/// server. This is the class that is returned by WebSocketFactory::connect() when the connection to an endpoint
/// is requested. If an error occurs while establishing the connection, the error is presented to the
/// WebSocketObserver provided when the WebSocket was created.
struct WebSocket {
    /// The destructor must close the websocket connection when the WebSocket object is destroyed
    virtual ~WebSocket() = default;

    /// Write data asynchronously to the WebSocket connection. The handler function will be called when the
    /// data has been sent successfully. The WebSocketOberver provided when the WebSocket was created will be
    /// called if any errors occur during the write.
    virtual void async_write_binary(const char* data, size_t size, util::UniqueFunction<void()>&& handler) = 0;
};

struct WebSocketFactory {
    /// The WebSocketFactory should not be destroyed until after the WebSocket connections have been
    /// closed and the event loop has been stopped.
    virtual ~WebSocketFactory() = default;

    /// Create a new event loop object for posting events onto the event loop. This will only be called
    /// once per client instantiation, so a fresh event loop should be created with each call to this
    /// function.
    virtual std::shared_ptr<EventLoopClient> create_event_loop() = 0;

    /// Create a new websocket pointed to the server designated by endpoint and connect to the server.
    /// Any events that occur during the execution of the websocket will call directly to the handlers
    /// provided by the observer. The handlers are expected to be called from the event loop thread
    /// so the operations are properly synchronized.
    virtual std::unique_ptr<WebSocket> connect(WebSocketObserver* observer, Endpoint&& endpoint) = 0;
};

} // namespace realm::util::websocket

#endif // REALM_UTIL_CLIENT_WEBSOCKET
