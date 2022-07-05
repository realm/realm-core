/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/
#ifndef REALM_CLIENT_WEBSOCKET_HPP
#define REALM_CLIENT_WEBSOCKET_HPP

#include <random>
#include <system_error>
#include <map>

#include <realm/sync/config.hpp>
#include <realm/util/http.hpp>
#include <realm/util/network.hpp>

namespace realm::util::network {
class Service;
}

namespace realm::util::websocket {
using port_type = sync::port_type;

struct SocketFactoryConfig {
    std::string user_agent;
};

// Legacy socket configuration
struct DefaultSocketFactoryConfig {
    util::Logger& logger;
    std::mt19937_64& random;
    util::network::Service& service;
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

class SocketObserver {
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
    ~SocketObserver() = default;
};

class WebSocket {
public:
    virtual ~WebSocket();

    virtual void async_write_binary(const char* data, size_t size, util::UniqueFunction<void()>&& handler) = 0;
};

class SocketFactory {
public:
    SocketFactory(DefaultSocketFactoryConfig config)
    : defaultSocketConfig(config)
    {
    }

    virtual ~SocketFactory() {}

    virtual std::unique_ptr<WebSocket> connect(SocketObserver* observer, Endpoint&& endpoint) = 0;

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
    template <class H>
    void post(H handler);
private:
    DefaultSocketFactoryConfig defaultSocketConfig;
};

// Legacy Core websocket implementation
class DefaultSocketFactory : public SocketFactory {
public:
    DefaultSocketFactory(SocketFactoryConfig config, DefaultSocketFactoryConfig defaultSocketConfig)
    : SocketFactory(defaultSocketConfig)
    , m_config(config)
    , m_defaultSocketConfig(defaultSocketConfig)
    {
    }

    virtual std::unique_ptr<WebSocket> connect(SocketObserver* observer, Endpoint&& endpoint) override;
private:
    SocketFactoryConfig m_config;
    DefaultSocketFactoryConfig m_defaultSocketConfig;
};

template <class H>
inline void SocketFactory::post(H handler)
{
    defaultSocketConfig.service.post(std::move(handler)); // Throws
}

} // namespace realm::util::websocket

#endif // REALM_CLIENT_WEBSOCKET_HPP
