#include "test.hpp"
#include "util/test_path.hpp"
#include "util/random.hpp"

#include <realm/util/async_future_adapter.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/sync/network/network_error.hpp>
#include <realm/sync/network/network_ssl.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/sync/network/default_socket.hpp>

#include <queue>

using namespace realm;
using namespace realm::sync;
using namespace realm::sync::websocket;

namespace realm::util {
template <>
Status status_from_error<std::error_code>(std::error_code ec)
{
    return network::get_status_from_network_error(ec);
}

} // namespace realm::util

struct WebSocketEvent {
    enum Type { ReadError, WriteError, ProtocolError, HandshakeComplete, BinaryMessage, CloseFrame } type;
    std::string payload;
    WebSocketError close_code = WebSocketError::websocket_ok;
    bool was_clean = true;
};

class WebSocketEventQueue {
public:
    ~WebSocketEventQueue()
    {
        REALM_ASSERT_RELEASE(events.empty());
    }

    template <typename... Args>
    void add_event(WebSocketEvent::Type type, Args... args)
    {
        WebSocketEvent event{type, args...};
        std::lock_guard lk(mutex);
        events.push(std::move(event));
        cv.notify_one();
    }

    WebSocketEvent next_event()
    {
        std::unique_lock lk(mutex);
        cv.wait(lk, [&] {
            return !events.empty();
        });
        auto ret = std::move(events.front());
        events.pop();
        return ret;
    }

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

class WebSocketEventQueueObserver : public WebSocketObserver {
public:
    static auto make_observer_and_queue()
    {
        struct ObserverAndQueue {
            std::unique_ptr<WebSocketEventQueueObserver> observer;
            std::shared_ptr<WebSocketEventQueue> queue = std::make_shared<WebSocketEventQueue>();
        };

        ObserverAndQueue ret;
        ret.observer = std::make_unique<WebSocketEventQueueObserver>(ret.queue);
        return ret;
    }

    explicit WebSocketEventQueueObserver(std::shared_ptr<WebSocketEventQueue> queue)
        : m_queue(std::move(queue))
    {
    }

    void websocket_connected_handler(const std::string& protocol) override
    {
        m_queue->add_event(WebSocketEvent::HandshakeComplete, protocol);
    }

    void websocket_error_handler() override
    {
        m_queue->add_event(WebSocketEvent::ReadError);
    }

    bool websocket_binary_message_received(util::Span<const char> data) override
    {
        m_queue->add_event(WebSocketEvent::BinaryMessage, std::string{data.data(), data.size()});
        return !m_queue->client_is_stopped();
    }

    bool websocket_closed_handler(bool was_clean, WebSocketError error, std::string_view msg) override
    {
        m_queue->add_event(WebSocketEvent::CloseFrame, std::string{msg}, error, was_clean);
        return false;
    }

private:
    std::shared_ptr<WebSocketEventQueue> m_queue;
};

template <typename T, typename Service, typename Func>
static T do_synchronous_post(Service& service, Func&& func)
{
    auto pf = util::make_promise_future<T>();
    service.post([&](Status status) {
        REALM_ASSERT(status.is_ok());
        if constexpr (std::is_void_v<T>) {
            func();
            pf.promise.emplace_value();
        }
        else {
            pf.promise.emplace_value(func());
        }
    });

    if constexpr (std::is_move_constructible_v<T>) {
        return std::move(pf.future.get());
    }
    else {
        return pf.future.get();
    }
}

class WrappedWebSocket : public WebSocketInterface {
public:
    WrappedWebSocket(SyncSocketProvider& provider, std::unique_ptr<WebSocketInterface>&& socket)
        : m_provider(provider)
        , m_socket(std::move(socket))
    {
    }

    ~WrappedWebSocket()
    {
        do_synchronous_post<void>(m_provider, [&] {
            m_socket.reset();
        });
    }

    std::string_view get_appservices_request_id() const noexcept override
    {
        return do_synchronous_post<std::string_view>(m_provider, [&] {
            return m_socket->get_appservices_request_id();
        });
    }

    void async_write_binary(util::Span<const char> data, SyncSocketProvider::FunctionHandler&& handler) override
    {
        do_synchronous_post<void>(m_provider, [&] {
            m_socket->async_write_binary(data, std::move(handler));
        });
    }

    util::Future<void> write_binary(util::Span<const char> data)
    {
        return util::async_future_adapter<void, Status>(*this, &WrappedWebSocket::async_write_binary, data);
    }

private:
    SyncSocketProvider& m_provider;
    std::unique_ptr<WebSocketInterface> m_socket;
};

static std::unique_ptr<WrappedWebSocket> do_connect(DefaultSocketProvider& provider,
                                                    std::unique_ptr<WebSocketObserver> observer, WebSocketEndpoint ep)
{
    auto socket = do_synchronous_post<std::unique_ptr<WebSocketInterface>>(provider, [&] {
        return provider.connect(std::move(observer), std::move(ep));
    });
    return std::make_unique<WrappedWebSocket>(provider, std::move(socket));
}

class TestWebSocketServer {
public:
    struct Config {
        Config()
            : use_ssl(true)
        {
            auto ca_dir = test_util::get_test_resource_path();
            tls_cert_path = ca_dir + "localhost-chain.crt.pem";
            tls_key_path = ca_dir + "localhost-server.key.pem";
        }
        std::string tls_cert_path;
        std::string tls_key_path;
        bool use_ssl{true};
    };
    TestWebSocketServer(test_util::unit_test::TestContext& test_context, Config&& config = {})
        : m_test_context(test_context)
        , m_logger(std::make_shared<util::PrefixLogger>("TestWebSocketServer ", m_test_context.logger))
        , m_acceptor(m_service)
        , m_server_thread([this] {
            m_service.run_until_stopped();
        })
    {
#if REALM_MOBILE
        config.use_ssl = false;
#endif
        do_synchronous_post<void>(m_service, [&]() mutable {
            if (config.use_ssl) {
                m_tls_context.emplace();
                m_tls_context->use_certificate_chain_file(config.tls_cert_path);
                m_tls_context->use_private_key_file(config.tls_key_path);
            }
            m_acceptor.open(m_endpoint.protocol());
            m_acceptor.bind(m_endpoint);
            m_endpoint = m_acceptor.local_endpoint();
            m_acceptor.listen();
            m_logger->debug("Listening on port %1", m_endpoint.port());
        });
    }

    ~TestWebSocketServer()
    {
        do_synchronous_post<void>(m_service, [&] {
            m_acceptor.cancel();
            m_acceptor.close();
        });
        m_service.stop();
        m_server_thread.join();
    }

    WebSocketEndpoint endpoint() const
    {
        WebSocketEndpoint ep;
        ep.port = m_endpoint.port();
        ep.path = "/";
        ep.address = "localhost";
        if (m_tls_context) {
            ep.is_ssl = true;
            ep.ssl_trust_certificate_path = test_util::get_test_resource_path() + "crt.pem";
            ep.verify_servers_ssl_certificate = true;
        }
        ep.protocols = {"RealmTestWebSocket#1"};
        return ep;
    }

    void post(util::UniqueFunction<void()>&& fn)
    {
        m_service.post([fn = std::move(fn)](Status status) {
            REALM_ASSERT(status.is_ok());
            fn();
        });
    }

    struct Conn : websocket::Config, util::AtomicRefCountBase {
        Conn(int conn_id, network::Service& service, std::optional<network::ssl::Context>& tls_context,
             test_util::unit_test::TestContext& test_context)
            : random{test_util::produce_nondeterministic_random_seed()}
            , logger(std::make_shared<util::PrefixLogger>(util::format("Server conn %1: ", conn_id),
                                                          test_context.logger))
            , service(service)
            , socket(service)
            , http_server(*this, logger)
            , websocket(*this)
        {
            if (tls_context) {
                tls_stream.emplace(socket, *tls_context, network::ssl::Stream::server);
                tls_stream->set_logger(logger.get());
            }
        }

        ~Conn()
        {
            // If we haven't finished the TLS handshake, then the Conn has only lived
            // on the event loop and we can tear it down here.
            if (state.load() == Accepted) {
                shutdown_websocket();
            }
            else {
                close();
            }
        }

        util::Future<void> send_binary_message(util::Span<char const> data)
        {
            return async_future_adapter<size_t, std::error_code>(websocket, &websocket::Socket::async_write_binary,
                                                                 data.data(), data.size())
                .ignore_value();
        }

        util::Future<void> send_close_frame(WebSocketError error, std::string_view msg)
        {
            struct Anchor {
                util::bind_ptr<Conn> self;
                std::vector<char> msg;
            };
            auto anchor = std::make_unique<Anchor>();
            anchor->self = util::bind_ptr(this);
            auto& msg_data = anchor->msg;
            msg_data.resize(2 + msg.size());
            uint16_t error_short = htons(static_cast<uint16_t>(error));
            msg_data[0] = error_short & 0xff;
            msg_data[1] = (error_short >> 8) & 0xff;
            std::copy(msg.begin(), msg.end(), msg_data.begin() + 2);
            return async_future_adapter<size_t, std::error_code>(websocket, &websocket::Socket::async_write_close,
                                                                 msg_data.data(), msg_data.size())
                .ignore_value()
                .on_completion([anchor = std::move(anchor)](Status status) {
                    return status;
                });
        }

        util::Future<HTTPRequest> initiate_server_handshake()
        {
            return async_future_adapter<HTTPRequest, std::error_code>(http_server,
                                                                      &decltype(http_server)::async_receive_request);
        }

        util::Future<void> send_http_response(HTTPResponse&& resp)
        {
            if (resp.status != HTTPStatus::SwitchingProtocols && resp.status != HTTPStatus::Ok) {
                resp.headers["Connection"] = "close";
            }
            if (resp.body) {
                resp.headers["Content-Length"] = util::to_string(resp.body->size());
            }
            // You can't capture an HTTPResponse in a lambda that will be the callback for service.post()
            // because the std::map that contains the headers is not std::is_nothrow_move_constructible_v.
            // So we wrap it in a unique_ptr here.
            // auto unique_resp = std::make_unique<HTTPResponse>(std::move(resp));
            auto pf = util::make_promise_future();
            service.post([this, promise = std::move(pf.promise), resp = std::move(resp)](Status status) mutable {
                if (!status.is_ok()) {
                    promise.set_error(status);
                }
                promise.set_from(util::async_future_adapter<void, std::error_code>(
                    http_server, &HTTPServer<Conn>::async_send_response, std::move(resp)));
            });
            return std::move(pf.future);
        }

        util::Future<void> complete_server_handshake(HTTPRequest&& req)
        {
            auto protocol_it = req.headers.find("Sec-WebSocket-Protocol");
            REALM_ASSERT(protocol_it != req.headers.end());
            auto protocols = protocol_it->second;

            auto first_comma = protocols.find(',');
            std::string protocol;
            if (first_comma == std::string::npos) {
                protocol = protocols;
            }
            else {
                protocol = protocols.substr(0, first_comma);
            }
            std::error_code ec;
            auto maybe_resp = websocket::make_http_response(req, protocol, ec);
            REALM_ASSERT(maybe_resp);
            REALM_ASSERT(!ec);

            return send_http_response(std::move(*maybe_resp)).then([this] {
                state.store(WebsocketHandshakeComplete);
                websocket.initiate_server_websocket_after_handshake();
            });
        }

        void do_server_handshake()
        {
            initiate_server_handshake()
                .then([this](HTTPRequest&& req) {
                    return complete_server_handshake(std::move(req));
                })
                .get_async([self = util::bind_ptr(this)](Status status) {
                    if (status.is_ok()) {
                        self->logger->debug("handshake complete on server side");
                        self->events.add_event(WebSocketEvent::HandshakeComplete);
                    }
                    else {
                        self->logger->debug("handshake error: %1", status);
                        self->events.add_event(WebSocketEvent::ReadError);
                    }
                });
        }

        WebSocketEvent next_event()
        {
            return events.next_event();
        }

        void close()
        {
            do_synchronous_post<void>(service, [&] {
                shutdown_websocket();
            });
        }

    protected:
        friend struct HTTPServer<Conn>;
        friend struct HTTPParser<Conn>;
        friend class websocket::Socket;
        friend class TestWebSocketServer;

        void shutdown_websocket()
        {
            auto old_state = state.exchange(Closed);
            if (old_state == Closed) {
                return;
            }
            if (old_state == WebsocketHandshakeComplete) {
                websocket.stop();
            }
            if (tls_stream && (old_state == TlsHandshakeComplete || old_state == WebsocketHandshakeComplete)) {
                std::error_code ec;
                tls_stream->shutdown(ec);
                if (ec) {
                    logger->warn("Error shutting down tls stream on server side: %1", ec);
                }
            }
            socket.close();
        }


        template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
        util::Future<T> async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args)
        {
            auto pf = util::make_promise_future<T>();
            service.post([promise = std::move(pf.promise), &obj, fn_ptr, args...](Status status) mutable {
                if (!status.is_ok()) {
                    promise.set_error(status);
                }
                promise.set_from(util::async_future_adapter<T, Error>(obj, fn_ptr, args...));
            });
            return std::move(pf.future);
        }


        // Implement the websocket::Config interface
        const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept override
        {
            return logger;
        }

        std::mt19937_64& websocket_get_random() noexcept override
        {
            return random;
        }

        void async_write(const char* data, size_t size, WriteCompletionHandler handler) override
        {
            if (tls_stream)
                tls_stream->async_write(data, size, std::move(handler));
            else
                socket.async_write(data, size, std::move(handler));
        }

        void async_read(char* buffer, size_t size, ReadCompletionHandler handler) override
        {
            if (tls_stream)
                tls_stream->async_read(buffer, size, read_buffer, std::move(handler));
            else
                socket.async_read(buffer, size, read_buffer, std::move(handler));
        }

        void async_read_until(char* buffer, size_t size, char delim, ReadCompletionHandler handler) override
        {
            if (tls_stream)
                tls_stream->async_read_until(buffer, size, delim, read_buffer, std::move(handler));
            else
                socket.async_read_until(buffer, size, delim, read_buffer, std::move(handler));
        }

        void websocket_handshake_completion_handler(const HTTPHeaders&) override
        {
            // We always complete the websocket handshake by calling initiate_server_websocket_after_handshake()
            // so this should never be called.
            REALM_UNREACHABLE();
        }

        void websocket_read_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::ReadError);
            shutdown_websocket();
        }

        void websocket_write_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::WriteError);
            shutdown_websocket();
        }

        void websocket_handshake_error_handler(std::error_code, const HTTPHeaders*, std::string_view) override
        {
            REALM_UNREACHABLE();
        }

        void websocket_protocol_error_handler(std::error_code) override
        {
            events.add_event(WebSocketEvent::ProtocolError);
            shutdown_websocket();
        }

        bool websocket_text_message_received(const char*, size_t) override
        {
            REALM_UNREACHABLE();
        }

        bool websocket_binary_message_received(const char* data, size_t size) override
        {
            events.add_event(WebSocketEvent::BinaryMessage, std::string(data, size));
            return true;
        }

        bool websocket_close_message_received(websocket::WebSocketError code, std::string_view message) override
        {
            events.add_event(WebSocketEvent::CloseFrame, std::string{message}, code);
            return false;
        }

        bool websocket_ping_message_received(const char*, size_t) override
        {
            REALM_UNREACHABLE();
        }

        bool websocket_pong_message_received(const char*, size_t) override
        {
            REALM_UNREACHABLE();
        }


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

    util::Future<util::bind_ptr<Conn>> accept_connection()
    {
        auto pf = util::make_promise_future<util::bind_ptr<Conn>>();
        auto fut_return = std::move(pf.future).then([this](util::bind_ptr<Conn> conn) {
            if (!m_tls_context) {
                return util::Future<util::bind_ptr<Conn>>(conn);
            }
            return util::async_future_adapter<void, std::error_code>(
                       conn->tls_stream,
                       &network::ssl::Stream::async_handshake<util::UniqueFunction<void(std::error_code)>>)
                .then([conn] {
                    conn->state.store(Conn::TlsHandshakeComplete);
                    return conn;
                })
                .on_error([conn](Status status) {
                    conn->logger->warn("Error accepting server connection: %1", status);
                    return StatusWith<util::bind_ptr<Conn>>(status);
                });
        });
        post([this, promise = std::move(pf.promise)]() mutable {
            auto conn = util::make_bind<Conn>(++m_conn_count, m_service, m_tls_context, m_test_context);
            m_acceptor.async_accept(conn->socket, [conn, promise = std::move(promise)](std::error_code ec) mutable {
                if (ec) {
                    promise.set_error(network::get_status_from_network_error(ec));
                    return;
                }

                promise.emplace_value(std::move(conn));
            });
        });
        return fut_return;
    }

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

TEST(DefaultSocketProvider_ConnectAndSend)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    auto server_conn = std::move(server_conn_fut.get());
    server_conn->do_server_handshake();

    CHECK(client_events->next_event().type == WebSocketEvent::HandshakeComplete);
    CHECK(server_conn->next_event().type == WebSocketEvent::HandshakeComplete);

    std::string message_to_send = "hello, world!\n";

    client->write_binary(message_to_send).get();

    auto bin_msg_event = server_conn->next_event();
    CHECK(bin_msg_event.type == WebSocketEvent::BinaryMessage);
    CHECK(bin_msg_event.payload == message_to_send);

    server_conn->send_binary_message(message_to_send).get();

    bin_msg_event = client_events->next_event();
    CHECK(bin_msg_event.type == WebSocketEvent::BinaryMessage);
    CHECK(bin_msg_event.payload == message_to_send);

    server_conn->close();

    auto read_write_err = client_events->next_event();
    auto close_call = client_events->next_event();
    CHECK(read_write_err.type == WebSocketEvent::ReadError);
    CHECK(close_call.type == WebSocketEvent::CloseFrame);
    CHECK(close_call.close_code == WebSocketError::websocket_read_error);
    CHECK_NOT(close_call.was_clean);

    client_events->stop_client();
}

TEST(DefaultSocketProvider_CleanCloseFrame)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    auto server_conn = std::move(server_conn_fut.get());
    server_conn->do_server_handshake();

    CHECK(client_events->next_event().type == WebSocketEvent::HandshakeComplete);
    CHECK(server_conn->next_event().type == WebSocketEvent::HandshakeComplete);

    server_conn->send_close_frame(WebSocketError::websocket_ok, "Shutdown okay").get();
    server_conn->close();

    auto close_call = client_events->next_event();
    CHECK(close_call.type == WebSocketEvent::CloseFrame);
    CHECK(close_call.close_code == WebSocketError::websocket_ok);
    CHECK(close_call.payload == "Shutdown okay");
    CHECK(close_call.was_clean);
}

TEST(DefaultSocketProvider_ClientDisconnects)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    auto server_conn = std::move(server_conn_fut.get());
    server_conn->do_server_handshake();

    CHECK(client_events->next_event().type == WebSocketEvent::HandshakeComplete);
    CHECK(server_conn->next_event().type == WebSocketEvent::HandshakeComplete);

    client_events->stop_client();
    client.reset();

    auto read_write_err = server_conn->next_event();
    CHECK(read_write_err.type == WebSocketEvent::ReadError);
}

#if !REALM_MOBILE
TEST(DefaultSocketProvider_BadTLSCertificate)
{
    auto ca_path = test_util::get_test_resource_path();
    TestWebSocketServer::Config server_config;
    server_config.tls_cert_path = ca_path + "dns-chain.crt.pem";
    server_config.tls_key_path = ca_path + "dns-checked-server.key.pem";
    TestWebSocketServer server(test_context, std::move(server_config));
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
    auto server_conn_fut = server.accept_connection();
    auto client = do_connect(client_provider, std::move(observer), server.endpoint());

    auto sw_server_conn = server_conn_fut.get_no_throw();
    // On apple platforms this gets reported as an end_of_input error, but on openssl
    // platforms it gets represented as a tls handshake failure.
    CHECK(sw_server_conn == ErrorCodes::ConnectionClosed || sw_server_conn == ErrorCodes::TlsHandshakeFailed);

    auto read_error_event = client_events->next_event();
    CHECK(read_error_event.type == WebSocketEvent::ReadError);
    auto tls_error_event = client_events->next_event();
    CHECK(tls_error_event.type == WebSocketEvent::CloseFrame);
    CHECK(tls_error_event.close_code == WebSocketError::websocket_tls_handshake_failed);
    CHECK_NOT(tls_error_event.was_clean);
}
#endif

TEST(DefaultSocketProvider_WebsocketRequestFailures)
{
    TestWebSocketServer server(test_context);
    DefaultSocketProvider client_provider(test_context.logger, "DefaultSocketProvider");

    auto make_resp = [](HTTPStatus status) {
        HTTPResponse resp;
        resp.status = status;
        resp.reason = util::to_string(status);
        return resp;
    };

    {
        auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
        auto server_conn_fut = server.accept_connection();
        auto client = do_connect(client_provider, std::move(observer), server.endpoint());

        auto server_conn = server_conn_fut.get();
        server_conn->initiate_server_handshake()
            .then([server_conn](HTTPRequest&&) {
                HTTPResponse resp;
                resp.status = HTTPStatus::MovedPermanently;
                resp.reason = util::to_string(resp.status);
                resp.headers["Location"] = "http://example.com";

                return server_conn->send_http_response(std::move(resp));
            })
            .get();
        auto handshake_error_event = client_events->next_event();
        CHECK(handshake_error_event.type == WebSocketEvent::CloseFrame);
        CHECK(handshake_error_event.close_code == WebSocketError::websocket_moved_permanently);
        CHECK(handshake_error_event.was_clean);
    }

    {
        auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
        auto server_conn_fut = server.accept_connection();
        auto client = do_connect(client_provider, std::move(observer), server.endpoint());

        auto server_conn = server_conn_fut.get();
        server_conn->initiate_server_handshake()
            .then([server_conn, &make_resp](HTTPRequest&&) {
                return server_conn->send_http_response(make_resp(HTTPStatus::Unauthorized));
            })
            .get();
        auto handshake_error_event = client_events->next_event();
        CHECK(handshake_error_event.type == WebSocketEvent::CloseFrame);
        CHECK(handshake_error_event.close_code == WebSocketError::websocket_unauthorized);
        CHECK(handshake_error_event.was_clean);
    }

    {
        auto&& [observer, client_events] = WebSocketEventQueueObserver::make_observer_and_queue();
        auto server_conn_fut = server.accept_connection();
        auto client = do_connect(client_provider, std::move(observer), server.endpoint());

        auto server_conn = server_conn_fut.get();
        server_conn->initiate_server_handshake()
            .then([server_conn, &make_resp](HTTPRequest&&) {
                return server_conn->send_http_response(make_resp(HTTPStatus::Forbidden));
            })
            .get();
        auto handshake_error_event = client_events->next_event();
        CHECK(handshake_error_event.type == WebSocketEvent::CloseFrame);
        CHECK(handshake_error_event.close_code == WebSocketError::websocket_forbidden);
        CHECK(handshake_error_event.was_clean);
    }
}
