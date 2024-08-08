
#include "websocket_server.hpp"
#include "util/test_path.hpp"
#include "util/random.hpp"

#include <realm/sync/network/network_error.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::sync::websocket;

static Status status_from_error(std::error_code ec)
{
    return network::get_status_from_network_error(ec);
}

template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
auto do_async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args)
{
    auto pf = util::make_promise_future<T>();
    auto fn = std::mem_fn(fn_ptr);
    if constexpr (std::is_void_v<T>) {
        fn(obj, args..., [promise = std::move(pf.promise)](Error ec) mutable {
            if constexpr (std::is_same_v<Error, Status>) {
                if (!ec.is_ok()) {
                    promise.set_error(ec);
                    return;
                }
            }
            else {
                auto status = status_from_error(ec);
                if (!status.is_ok()) {
                    promise.set_error(status);
                    return;
                }
            }

            promise.emplace_value();
        });
    }
    else {
        struct Callable {
            util::Promise<T> promise;

            void operator()(Error ec, T result)
            {
                if constexpr (std::is_same_v<Error, Status>) {
                    if (!ec.is_ok()) {
                        promise.set_error(ec);
                        return;
                    }
                }
                else {
                    auto status = status_from_error(ec);
                    if (!status.is_ok()) {
                        promise.set_error(status);
                        return;
                    }
                }
                promise.emplace_value(std::move(result));
            }

            void operator()(T result, Error ec)
            {
                (*this)(ec, std::move(result));
            }
        } callback{std::move(pf.promise)};
        fn(obj, args..., std::move(callback));
    }
    return std::move(pf.future);
}

template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
auto async_future_adapter(OperObj& obj, AsyncFn&& fn_ptr, Args&&... args)
{
    return do_async_future_adapter<T, Error>(obj, fn_ptr, args...);
}

WebSocketEventQueue::~WebSocketEventQueue()
{
    REALM_ASSERT_RELEASE(events.empty());
}

WebSocketEvent WebSocketEventQueue::next_event()
{
    std::unique_lock lk(mutex);
    cv.wait(lk, [&] {
        return !events.empty();
    });
    auto ret = std::move(events.front());
    events.pop();
    return ret;
}

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

template <typename T, typename Error, typename OperObj, typename AsyncFn, typename... Args>
util::Future<T> TestWebSocketServer::Conn::async_future_adapter_on_service(OperObj& obj, AsyncFn&& fn_ptr,
                                                                           Args&&... args)
{
    auto pf = util::make_promise_future<T>();
    service.post([promise = std::move(pf.promise), &obj, fn_ptr, args...](Status status) mutable {
        if (!status.is_ok()) {
            promise.set_error(status);
        }
        promise.set_from(async_future_adapter<T, Error>(obj, fn_ptr, args...));
    });
    return std::move(pf.future);
}

TestWebSocketServer::Config::Config()
    : use_ssl(true)
{
    auto ca_dir = test_util::get_test_resource_path();
    tls_cert_path = ca_dir + "localhost-chain.crt.pem";
    tls_key_path = ca_dir + "localhost-server.key.pem";
}

TestWebSocketServer::TestWebSocketServer(test_util::unit_test::TestContext& test_context, Config&& config)
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

TestWebSocketServer::~TestWebSocketServer()
{
    do_synchronous_post<void>(m_service, [&] {
        m_acceptor.cancel();
        m_acceptor.close();
    });
    m_service.stop();
    m_server_thread.join();
}

WebSocketEndpoint TestWebSocketServer::endpoint() const
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

void TestWebSocketServer::post(util::UniqueFunction<void()>&& fn)
{
    m_service.post([fn = std::move(fn)](Status status) {
        REALM_ASSERT(status.is_ok());
        fn();
    });
}

TestWebSocketServer::Conn::Conn(int conn_id, network::Service& service,
                                std::optional<network::ssl::Context>& tls_context,
                                test_util::unit_test::TestContext& test_context)
    : random{test_util::produce_nondeterministic_random_seed()}
    , logger(std::make_shared<util::PrefixLogger>(util::format("Server conn %1: ", conn_id), test_context.logger))
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

TestWebSocketServer::Conn::~Conn()
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

util::Future<void> TestWebSocketServer::Conn::send_binary_message(util::Span<char const> data)
{
    return async_future_adapter_on_service<size_t, std::error_code>(websocket, &websocket::Socket::async_write_binary,
                                                                    data.data(), data.size())
        .ignore_value();
}

util::Future<void> TestWebSocketServer::Conn::send_close_frame(WebSocketError error, std::string_view msg)
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
    return async_future_adapter_on_service<size_t, std::error_code>(websocket, &websocket::Socket::async_write_close,
                                                                    msg_data.data(), msg_data.size())
        .ignore_value()
        .on_completion([anchor = std::move(anchor)](Status status) {
            return status;
        });
}

util::Future<HTTPRequest> TestWebSocketServer::Conn::initiate_server_handshake()
{
    return async_future_adapter_on_service<HTTPRequest, std::error_code>(
        http_server, &decltype(http_server)::async_receive_request);
}

util::Future<void> TestWebSocketServer::Conn::send_http_response(HTTPResponse&& resp)
{
    if (resp.status != HTTPStatus::SwitchingProtocols && resp.status != HTTPStatus::Ok) {
        resp.headers["Connection"] = "close";
    }
    if (resp.body) {
        resp.headers["Content-Length"] = util::to_string(resp.body->size());
    }
    auto pf = util::make_promise_future();
    service.post([this, promise = std::move(pf.promise), resp = std::move(resp)](Status status) mutable {
        if (!status.is_ok()) {
            promise.set_error(status);
        }
        promise.set_from(async_future_adapter_on_service<void, std::error_code>(
            http_server, &HTTPServer<Conn>::async_send_response, std::move(resp)));
    });
    return std::move(pf.future);
}

util::Future<void> TestWebSocketServer::Conn::complete_server_handshake(HTTPRequest&& req)
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

void TestWebSocketServer::Conn::do_server_handshake()
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

WebSocketEvent TestWebSocketServer::Conn::next_event()
{
    return events.next_event();
}

void TestWebSocketServer::Conn::close()
{
    do_synchronous_post<void>(service, [&] {
        shutdown_websocket();
    });
}

void TestWebSocketServer::Conn::shutdown_websocket()
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
        if (tls_stream->shutdown(ec)) {
            logger->warn("Error shutting down tls stream on server side: %1", ec);
        }
    }
    socket.close();
}

util::Future<util::bind_ptr<TestWebSocketServer::Conn>> TestWebSocketServer::accept_connection()
{
    auto pf = util::make_promise_future<util::bind_ptr<Conn>>();
    auto fut_return = std::move(pf.future).then([this](util::bind_ptr<Conn> conn) {
        if (!m_tls_context) {
            return util::Future<util::bind_ptr<Conn>>(conn);
        }
        return async_future_adapter<void, std::error_code>(
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

void TestWebSocketServer::Conn::async_write(const char* data, size_t size, websocket::WriteCompletionHandler handler)
{
    if (tls_stream)
        tls_stream->async_write(data, size, std::move(handler));
    else
        socket.async_write(data, size, std::move(handler));
}

void TestWebSocketServer::Conn::async_read(char* buffer, size_t size, websocket::ReadCompletionHandler handler)
{
    if (tls_stream)
        tls_stream->async_read(buffer, size, read_buffer, std::move(handler));
    else
        socket.async_read(buffer, size, read_buffer, std::move(handler));
}

void TestWebSocketServer::Conn::async_read_until(char* buffer, size_t size, char delim,
                                                 websocket::ReadCompletionHandler handler)
{
    if (tls_stream)
        tls_stream->async_read_until(buffer, size, delim, read_buffer, std::move(handler));
    else
        socket.async_read_until(buffer, size, delim, read_buffer, std::move(handler));
}

void TestWebSocketServer::Conn::websocket_handshake_completion_handler(const HTTPHeaders&)
{
    // We always complete the websocket handshake by calling initiate_server_websocket_after_handshake()
    // so this should never be called.
    REALM_UNREACHABLE();
}

void TestWebSocketServer::Conn::websocket_read_error_handler(std::error_code)
{
    events.add_event(WebSocketEvent::ReadError);
    shutdown_websocket();
}

void TestWebSocketServer::Conn::websocket_write_error_handler(std::error_code)
{
    events.add_event(WebSocketEvent::WriteError);
    shutdown_websocket();
}

void TestWebSocketServer::Conn::websocket_handshake_error_handler(std::error_code, const HTTPHeaders*,
                                                                  std::string_view)
{
    REALM_UNREACHABLE();
}

void TestWebSocketServer::Conn::websocket_protocol_error_handler(std::error_code)
{
    events.add_event(WebSocketEvent::ProtocolError);
    shutdown_websocket();
}

bool TestWebSocketServer::Conn::websocket_text_message_received(const char*, size_t)
{
    REALM_UNREACHABLE();
}

bool TestWebSocketServer::Conn::websocket_binary_message_received(const char* data, size_t size)
{
    events.add_event(WebSocketEvent::BinaryMessage, std::string(data, size));
    return true;
}

bool TestWebSocketServer::Conn::websocket_close_message_received(websocket::WebSocketError code,
                                                                 std::string_view message)
{
    events.add_event(WebSocketEvent::CloseFrame, std::string{message}, code);
    return false;
}

bool TestWebSocketServer::Conn::websocket_ping_message_received(const char*, size_t)
{
    REALM_UNREACHABLE();
}

bool TestWebSocketServer::Conn::websocket_pong_message_received(const char*, size_t)
{
    REALM_UNREACHABLE();
}
