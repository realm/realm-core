#pragma once

#include "util/unit_test.hpp"
#include <realm/sync/socket_provider.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/sync/network/network_ssl.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/network/websocket_error.hpp>
#include <realm/util/future.hpp>

#include <string>
#include <queue>
#include <mutex>
#include <thread>

namespace realm::sync {
struct WebSocketEvent {
    enum Type { ReadError, WriteError, ProtocolError, HandshakeComplete, BinaryMessage, CloseFrame } type;
    std::string payload;
    websocket::WebSocketError close_code = websocket::WebSocketError::websocket_ok;
    bool was_clean = true;
};

class WebSocketEventQueue {
public:
    ~WebSocketEventQueue();

    template <typename... Args>
    void add_event(WebSocketEvent::Type type, Args... args)
    {
        WebSocketEvent event{type, args...};
        std::lock_guard lk(mutex);
        events.push(std::move(event));
        cv.notify_one();
    }

    WebSocketEvent next_event();

    // TODO This extra state should go away when RCORE-2136 is done.
    void stop_client()
    {
        std::lock_guard lk(mutex);
        m_stopped = true;
    }

    bool client_is_stopped()
    {
        std::lock_guard lk(mutex);
        return m_stopped;
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<WebSocketEvent> events;
    bool m_stopped = false;
};

class TestWebSocketServer {
public:
    struct Config {
        Config();

        std::string tls_cert_path;
        std::string tls_key_path;
        bool use_ssl{true};
    };

    TestWebSocketServer(test_util::unit_test::TestContext& test_context, Config&& config = {});
    ~TestWebSocketServer();

    WebSocketEndpoint endpoint() const;
    void post(util::UniqueFunction<void()>&& fn);

    struct Conn : public websocket::Config, util::AtomicRefCountBase {
        Conn(int conn_id, network::Service& service, std::optional<network::ssl::Context>& tls_context,
             test_util::unit_test::TestContext& test_context);
        ~Conn();

        void close();
        util::Future<void> send_binary_message(util::Span<char const> data);
        util::Future<void> send_close_frame(websocket::WebSocketError error, std::string_view msg);
        util::Future<HTTPRequest> initiate_server_handshake();
        util::Future<void> complete_server_handshake(HTTPRequest&& req);
        util::Future<void> send_http_response(HTTPResponse&& resp);
        void do_server_handshake();
        WebSocketEvent next_event();

    protected:
        friend struct HTTPServer<Conn>;
        friend struct HTTPParser<Conn>;
        friend class websocket::Socket;
        friend class TestWebSocketServer;
        void shutdown_websocket();

        template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
        util::Future<T> async_future_adapter_on_service(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args);

        // Implement the websocket::Config interface
        const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept override
        {
            return logger;
        }

        std::mt19937_64& websocket_get_random() noexcept override
        {
            return random;
        }

        void async_write(const char* data, size_t size, websocket::WriteCompletionHandler handler) override;
        void async_read(char* buffer, size_t size, websocket::ReadCompletionHandler handler) override;
        void async_read_until(char* buffer, size_t size, char delim,
                              websocket::ReadCompletionHandler handler) override;

        void websocket_handshake_completion_handler(const HTTPHeaders&) override;
        void websocket_read_error_handler(std::error_code) override;
        void websocket_write_error_handler(std::error_code) override;
        void websocket_handshake_error_handler(std::error_code, const HTTPHeaders*, std::string_view) override;
        void websocket_protocol_error_handler(std::error_code) override;
        bool websocket_text_message_received(const char*, size_t) override;
        bool websocket_binary_message_received(const char* data, size_t size) override;
        bool websocket_close_message_received(websocket::WebSocketError code, std::string_view message) override;
        bool websocket_ping_message_received(const char*, size_t) override;
        bool websocket_pong_message_received(const char*, size_t) override;

        std::mt19937_64 random;
        const std::shared_ptr<util::Logger> logger;
        network::Service& service;

        network::ReadAheadBuffer read_buffer;
        network::Socket socket;
        std::optional<network::ssl::Stream> tls_stream;
        enum State { Accepted, TlsHandshakeComplete, WebsocketHandshakeComplete, Closed };
        std::atomic<State> state{Accepted};
        HTTPServer<Conn> http_server;
        websocket::Socket websocket;

        WebSocketEventQueue events;
    };

    util::Future<util::bind_ptr<Conn>> accept_connection();

private:
    test_util::unit_test::TestContext& m_test_context;
    const std::shared_ptr<util::Logger> m_logger;
    network::Service m_service;
    network::Acceptor m_acceptor;
    network::Endpoint m_endpoint;
    std::optional<network::ssl::Context> m_tls_context;
    std::thread m_server_thread;
    int m_conn_count = 0;
};

} // namespace realm::sync
