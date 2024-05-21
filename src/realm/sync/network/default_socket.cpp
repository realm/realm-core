#include <realm/sync/network/default_socket.hpp>

#include <realm/sync/binding_callback_thread_observer.hpp>
#include <realm/sync/network/network.hpp>
#include <realm/sync/network/network_ssl.hpp>
#include <realm/sync/network/websocket.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/random.hpp>
#include <realm/util/scope_exit.hpp>

namespace realm::sync::websocket {

namespace {

///
/// DefaultWebSocketImpl - websocket implementation for the default socket provider
///
class DefaultWebSocketImpl final : public DefaultWebSocket, public Config, public util::RefCountBase {
public:
    DefaultWebSocketImpl(const std::shared_ptr<util::Logger>& logger_ptr, network::Service& service,
                         std::mt19937_64& random, const std::string user_agent,
                         util::UniqueFunction<void(WebSocketEvent&&)> observer, WebSocketEndpoint&& endpoint)
        : m_logger_ptr{logger_ptr}
        , m_network_logger{*m_logger_ptr}
        , m_random{random}
        , m_service{service}
        , m_user_agent{user_agent}
        , m_observer{std::move(observer)}
        , m_endpoint{std::move(endpoint)}
        , m_websocket(*this)
    {
        bind_ptr();
        initiate_resolve();
    }

    virtual ~DefaultWebSocketImpl() = default;

    void async_write_binary(util::Span<const char> data, SyncSocketProvider::FunctionHandler&& handler) override
    {
        if (m_closed) {
            handler({ErrorCodes::OperationAborted, "WebSocket already closed"});
            return;
        }
        m_websocket.async_write_binary(data.data(), data.size(),
                                       [write_handler = std::move(handler)](std::error_code ec, size_t) {
                                           write_handler(DefaultWebSocketImpl::get_status_from_util_error(ec));
                                       });
    }

    void close()
    {
        m_closed = true;
        m_socket.reset();
        m_ssl_stream.reset();
    }

    std::string_view get_appservices_request_id() const noexcept override
    {
        return m_app_services_coid;
    }

    void force_handshake_response_for_testing(int status_code, std::string body = "") override
    {
        m_websocket.force_handshake_response_for_testing(status_code, body);
    }

    // public for HTTPClient CRTP, but not on the EZSocket interface, so de-facto private
    void async_read(char*, std::size_t, ReadCompletionHandler) override;
    void async_read_until(char*, std::size_t, char, ReadCompletionHandler) override;
    void async_write(const char*, std::size_t, WriteCompletionHandler) override;

private:
    using milliseconds_type = std::int_fast64_t;

    const std::shared_ptr<util::Logger>& websocket_get_logger() noexcept override
    {
        return m_logger_ptr;
    }
    std::mt19937_64& websocket_get_random() noexcept override
    {
        return m_random;
    }

    void websocket_handshake_completion_handler(const HTTPHeaders& headers) override
    {
        if (m_closed) {
            return;
        }
        const std::string empty;
        if (auto it = headers.find("X-Appservices-Request-Id"); it != headers.end()) {
            m_app_services_coid = it->second;
        }
        auto it = headers.find("Sec-WebSocket-Protocol");
        m_observer(WebSocketEvent{WebSocketEvent::Open{it == headers.end() ? empty : it->second}});
    }
    void websocket_read_error_handler(std::error_code ec) override
    {
        m_network_logger.error("Reading failed: %1", ec.message()); // Throws
        constexpr bool was_clean = false;
        websocket_error_and_close_handler(was_clean, WebSocketError::websocket_read_error, ec.message());
    }
    void websocket_write_error_handler(std::error_code ec) override
    {
        m_network_logger.error("Writing failed: %1", ec.message()); // Throws
        constexpr bool was_clean = false;
        websocket_error_and_close_handler(was_clean, WebSocketError::websocket_write_error, ec.message());
    }
    void websocket_handshake_error_handler(std::error_code ec, const HTTPHeaders*, std::string_view body) override
    {
        WebSocketError error = WebSocketError::websocket_ok;
        bool was_clean = true;

        if (ec == websocket::HttpError::bad_response_301_moved_permanently ||
            ec == websocket::HttpError::bad_response_308_permanent_redirect) {
            error = WebSocketError::websocket_moved_permanently;
        }
        else if (ec == websocket::HttpError::bad_response_3xx_redirection) {
            error = WebSocketError::websocket_retry_error;
            was_clean = false;
        }
        else if (ec == websocket::HttpError::bad_response_401_unauthorized) {
            error = WebSocketError::websocket_unauthorized;
        }
        else if (ec == websocket::HttpError::bad_response_403_forbidden) {
            error = WebSocketError::websocket_forbidden;
        }
        else if (ec == websocket::HttpError::bad_response_5xx_server_error ||
                 ec == websocket::HttpError::bad_response_500_internal_server_error ||
                 ec == websocket::HttpError::bad_response_502_bad_gateway ||
                 ec == websocket::HttpError::bad_response_503_service_unavailable ||
                 ec == websocket::HttpError::bad_response_504_gateway_timeout) {
            error = WebSocketError::websocket_internal_server_error;
            was_clean = false;
        }
        else {
            error = WebSocketError::websocket_fatal_error;
            was_clean = false;
            if (!body.empty()) {
                std::string_view identifier = "REALM_SYNC_PROTOCOL_MISMATCH";
                auto i = body.find(identifier);
                if (i != std::string_view::npos) {
                    std::string_view rest = body.substr(i + identifier.size());
                    // FIXME: Use std::string_view::begins_with() in C++20.
                    auto begins_with = [](std::string_view string, std::string_view prefix) {
                        return (string.size() >= prefix.size() &&
                                std::equal(string.data(), string.data() + prefix.size(), prefix.data()));
                    };
                    if (begins_with(rest, ":CLIENT_TOO_OLD")) {
                        error = WebSocketError::websocket_client_too_old;
                    }
                    else if (begins_with(rest, ":CLIENT_TOO_NEW")) {
                        error = WebSocketError::websocket_client_too_new;
                    }
                    else {
                        // Other more complicated forms of mismatch
                        error = WebSocketError::websocket_protocol_mismatch;
                    }
                    was_clean = true;
                }
            }
        }

        websocket_error_and_close_handler(was_clean, error, ec.message());
    }
    void websocket_protocol_error_handler(std::error_code ec) override
    {
        constexpr bool was_clean = false;
        websocket_error_and_close_handler(was_clean, WebSocketError::websocket_protocol_error, ec.message());
    }
    bool websocket_close_message_received(WebSocketError code, std::string_view message) override
    {
        constexpr bool was_clean = true;

        return websocket_error_and_close_handler(was_clean, code, message);
    }
    bool websocket_error_and_close_handler(bool was_clean, WebSocketError code, std::string_view reason)
    {
        if (m_closed) {
            unbind_ptr();
            return false;
        }
        if (!was_clean) {
            m_observer(WebSocketEvent{WebSocketEvent::Error{}});
        }
        m_observer(WebSocketEvent{WebSocketEvent::Close{was_clean, code, reason}});
        unbind_ptr();
        return false;
    }
    bool websocket_binary_message_received(const char* ptr, std::size_t size) override
    {
        if (m_closed) {
            return false;
        }
        m_observer(WebSocketEvent{WebSocketEvent::Message{util::Span<const char>(ptr, size)}});
        if (m_closed) {
            unbind_ptr();
            return false;
        }
        return true;
    }

    static Status get_status_from_util_error(std::error_code ec)
    {
        if (!ec) {
            return Status::OK();
        }
        switch (ec.value()) {
            case util::error::operation_aborted:
                return {ErrorCodes::Error::OperationAborted, "Write operation cancelled"};
            case util::error::address_family_not_supported:
                [[fallthrough]];
            case util::error::invalid_argument:
                return {ErrorCodes::Error::InvalidArgument, ec.message()};
            case util::error::no_memory:
                return {ErrorCodes::Error::OutOfMemory, ec.message()};
            case util::error::connection_aborted:
                [[fallthrough]];
            case util::error::connection_reset:
                [[fallthrough]];
            case util::error::broken_pipe:
                [[fallthrough]];
            case util::error::resource_unavailable_try_again:
                return {ErrorCodes::Error::ConnectionClosed, ec.message()};
            default:
                return {ErrorCodes::Error::UnknownError, ec.message()};
        }
    }

    void initiate_resolve();
    void handle_resolve(std::error_code, network::Endpoint::List);
    void initiate_tcp_connect(network::Endpoint::List, std::size_t);
    void handle_tcp_connect(std::error_code, network::Endpoint::List, std::size_t);
    void initiate_http_tunnel();
    void initiate_websocket_or_ssl_handshake();
    void initiate_ssl_handshake();
    void handle_ssl_handshake(std::error_code);
    void initiate_websocket_handshake();
    void handle_connection_established();

    void schedule_urgent_ping();
    void initiate_ping_delay(milliseconds_type now);
    void handle_ping_delay();
    void initiate_pong_timeout();
    void handle_pong_timeout();

    const std::shared_ptr<util::Logger> m_logger_ptr;
    util::Logger& m_network_logger;
    std::mt19937_64& m_random;
    network::Service& m_service;
    const std::string m_user_agent;
    std::string m_app_services_coid;

    util::UniqueFunction<void(WebSocketEvent&&)> m_observer;
    bool m_closed = false;

    const WebSocketEndpoint m_endpoint;

    util::Optional<network::Resolver> m_resolver;
    util::Optional<network::Socket> m_socket;
    util::Optional<network::ssl::Context> m_ssl_context;
    util::Optional<network::ssl::Stream> m_ssl_stream;
    network::ReadAheadBuffer m_read_ahead_buffer;
    websocket::Socket m_websocket;
    util::Optional<HTTPClient<DefaultWebSocketImpl>> m_proxy_client;
};

void DefaultWebSocketImpl::async_read(char* buffer, std::size_t size, ReadCompletionHandler handler)
{
    if (m_closed) {
        handler(util::error::connection_reset, 0);
        return;
    }
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read(buffer, size, m_read_ahead_buffer, std::move(handler)); // Throws
    }
    else {
        m_socket->async_read(buffer, size, m_read_ahead_buffer, std::move(handler)); // Throws
    }
}


void DefaultWebSocketImpl::async_read_until(char* buffer, std::size_t size, char delim, ReadCompletionHandler handler)
{
    if (m_closed) {
        handler(util::error::connection_reset, 0);
        return;
    }
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read_until(buffer, size, delim, m_read_ahead_buffer, std::move(handler)); // Throws
    }
    else {
        m_socket->async_read_until(buffer, size, delim, m_read_ahead_buffer, std::move(handler)); // Throws
    }
}


void DefaultWebSocketImpl::async_write(const char* data, std::size_t size, WriteCompletionHandler handler)
{
    if (m_closed) {
        handler(util::error::connection_reset, 0);
        return;
    }
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_write(data, size, std::move(handler)); // Throws
    }
    else {
        m_socket->async_write(data, size, std::move(handler)); // Throws
    }
}


void DefaultWebSocketImpl::initiate_resolve()
{
    const std::string& address = m_endpoint.proxy ? m_endpoint.proxy->address : m_endpoint.address;
    const port_type& port = m_endpoint.proxy ? m_endpoint.proxy->port : m_endpoint.port;

    if (m_endpoint.proxy) {
        // logger.detail("Using %1 proxy", proxy->type); // Throws
    }

    m_network_logger.detail("Resolving '%1:%2'", address, port); // Throws

    network::Resolver::Query query(address, util::to_string(port)); // Throws
    auto handler = [self = util::bind_ptr(this)](std::error_code ec, network::Endpoint::List endpoints) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            self->handle_resolve(ec, std::move(endpoints)); // Throws
    };
    m_resolver.emplace(m_service);                                   // Throws
    m_resolver->async_resolve(std::move(query), std::move(handler)); // Throws
}


void DefaultWebSocketImpl::handle_resolve(std::error_code ec, network::Endpoint::List endpoints)
{
    if (ec) {
        m_network_logger.error("Failed to resolve '%1:%2': %3", m_endpoint.address, m_endpoint.port,
                               ec.message()); // Throws
        constexpr bool was_clean = false;
        websocket_error_and_close_handler(was_clean, WebSocketError::websocket_resolve_failed,
                                          ec.message()); // Throws
        return;
    }

    m_resolver.reset();
    initiate_tcp_connect(std::move(endpoints), 0); // Throws
}


void DefaultWebSocketImpl::initiate_tcp_connect(network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());

    network::Endpoint ep = *(endpoints.begin() + i);
    std::size_t n = endpoints.size();
    m_socket.emplace(m_service); // Throws
    m_socket->async_connect(
        ep, [self = util::bind_ptr(this), endpoints = std::move(endpoints), i](std::error_code ec) mutable {
            // If the operation is aborted, the connection object may have been
            // destroyed.
            if (ec != util::error::operation_aborted)
                self->handle_tcp_connect(ec, std::move(endpoints), i); // Throws
        });
    m_network_logger.detail("Connecting to endpoint '%1:%2' (%3/%4)", ep.address(), ep.port(), (i + 1), n); // Throws
}

void DefaultWebSocketImpl::handle_tcp_connect(std::error_code ec, network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());
    const network::Endpoint& ep = *(endpoints.begin() + i);
    if (ec) {
        m_network_logger.error("Failed to connect to endpoint '%1:%2': %3", ep.address(), ep.port(),
                               ec.message()); // Throws
        std::size_t i_2 = i + 1;
        if (i_2 < endpoints.size()) {
            initiate_tcp_connect(std::move(endpoints), i_2); // Throws
            return;
        }
        // All endpoints failed
        m_network_logger.error("Failed to connect to '%1:%2': All endpoints failed", m_endpoint.address,
                               m_endpoint.port);
        constexpr bool was_clean = false;
        websocket_error_and_close_handler(was_clean, WebSocketError::websocket_connection_failed,
                                          ec.message()); // Throws
        return;
    }

    REALM_ASSERT(m_socket);
    network::Endpoint ep_2 = m_socket->local_endpoint();
    m_network_logger.info("Connected to endpoint '%1:%2' (from '%3:%4')", ep.address(), ep.port(), ep_2.address(),
                          ep_2.port()); // Throws

    // TODO: Handle HTTPS proxies
    if (m_endpoint.proxy) {
        initiate_http_tunnel(); // Throws
        return;
    }

    initiate_websocket_or_ssl_handshake(); // Throws
}

void DefaultWebSocketImpl::initiate_websocket_or_ssl_handshake()
{
    if (m_endpoint.is_ssl) {
        initiate_ssl_handshake(); // Throws
    }
    else {
        initiate_websocket_handshake(); // Throws
    }
}

void DefaultWebSocketImpl::initiate_http_tunnel()
{
    HTTPRequest req;
    req.method = HTTPMethod::Connect;
    req.headers.emplace("Host", util::format("%1:%2", m_endpoint.address, m_endpoint.port));
    // TODO handle proxy authorization

    m_proxy_client.emplace(*this, m_logger_ptr);
    auto handler = [self = util::bind_ptr(this)](HTTPResponse response, std::error_code ec) {
        if (ec && ec != util::error::operation_aborted) {
            self->m_network_logger.error("Failed to establish HTTP tunnel: %1", ec.message());
            constexpr bool was_clean = false;
            self->websocket_error_and_close_handler(was_clean, WebSocketError::websocket_connection_failed,
                                                    ec.message()); // Throws
            return;
        }

        if (response.status != HTTPStatus::Ok) {
            self->m_network_logger.error("Proxy server returned response '%1 %2'", response.status,
                                         response.reason); // Throws
            constexpr bool was_clean = false;
            self->websocket_error_and_close_handler(was_clean, WebSocketError::websocket_connection_failed,
                                                    response.reason); // Throws
            return;
        }

        self->initiate_websocket_or_ssl_handshake(); // Throws
    };

    m_proxy_client->async_request(req, std::move(handler)); // Throws
}

void DefaultWebSocketImpl::initiate_ssl_handshake()
{
    using namespace network::ssl;

    if (!m_ssl_context) {
        m_ssl_context.emplace(); // Throws
        if (m_endpoint.verify_servers_ssl_certificate) {
            if (m_endpoint.ssl_trust_certificate_path) {
                m_ssl_context->use_verify_file(*m_endpoint.ssl_trust_certificate_path); // Throws
            }
            else if (!m_endpoint.ssl_verify_callback) {
                m_ssl_context->use_default_verify(); // Throws
#if REALM_INCLUDE_CERTS
                // On platforms like Windows or Android where OpenSSL is not normally found
                // `use_default_verify()` won't actually be able to load any default certificates.
                // That's why we bundle a set of trusted certificates ourselves.
                m_ssl_context->use_included_certificate_roots(); // Throws
#endif
            }
        }
    }

    m_ssl_stream.emplace(*m_socket, *m_ssl_context, Stream::client); // Throws
    m_ssl_stream->set_logger(m_logger_ptr.get());
    m_ssl_stream->set_host_name(m_endpoint.address); // Throws
    if (m_endpoint.verify_servers_ssl_certificate) {
        m_ssl_stream->set_verify_mode(VerifyMode::peer); // Throws
        m_ssl_stream->set_server_port(m_endpoint.port);
        if (!m_endpoint.ssl_trust_certificate_path) {
            if (m_endpoint.ssl_verify_callback) {
                m_ssl_stream->use_verify_callback(m_endpoint.ssl_verify_callback);
            }
        }
    }

    auto handler = [self = util::bind_ptr(this)](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            self->handle_ssl_handshake(ec); // Throws
    };
    m_ssl_stream->async_handshake(std::move(handler)); // Throws

    // FIXME: We also need to perform the SSL shutdown operation somewhere
}


void DefaultWebSocketImpl::handle_ssl_handshake(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        constexpr bool was_clean = false;
        WebSocketError parsed_error_code;
        if (ec == network::ssl::Errors::tls_handshake_failed) {
            parsed_error_code = WebSocketError::websocket_tls_handshake_failed;
        }
        else {
            parsed_error_code = WebSocketError::websocket_connection_failed;
        }

        websocket_error_and_close_handler(was_clean, parsed_error_code, ec.message()); // Throws
        return;
    }

    initiate_websocket_handshake(); // Throws
}


void DefaultWebSocketImpl::initiate_websocket_handshake()
{
    auto headers = HTTPHeaders(m_endpoint.headers.begin(), m_endpoint.headers.end());
    headers["User-Agent"] = m_user_agent;

    // Compute the value of the "Host" header.
    const std::uint_fast16_t default_port = (m_endpoint.is_ssl ? 443 : 80);
    auto host = m_endpoint.port == default_port ? m_endpoint.address
                                                : util::format("%1:%2", m_endpoint.address, m_endpoint.port);

    // Convert the list of protocols to a string
    std::ostringstream protocol_list;
    protocol_list.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    protocol_list.imbue(std::locale::classic());
    if (m_endpoint.protocols.size() > 1)
        std::copy(m_endpoint.protocols.begin(), m_endpoint.protocols.end() - 1,
                  std::ostream_iterator<std::string>(protocol_list, ", "));
    protocol_list << m_endpoint.protocols.back();

    m_websocket.initiate_client_handshake(m_endpoint.path, std::move(host), protocol_list.str(),
                                          std::move(headers)); // Throws
}
} // namespace

///
/// DefaultSocketProvider - default socket provider implementation
///

DefaultSocketProvider::DefaultSocketProvider(const std::shared_ptr<util::Logger>& logger,
                                             const std::string& user_agent,
                                             const std::shared_ptr<BindingCallbackThreadObserver>& observer_ptr,
                                             AutoStart auto_start)
    : m_logger_ptr{std::make_shared<util::CategoryLogger>(util::LogCategory::network, logger)}
    , m_observer_ptr{observer_ptr}
    , m_user_agent{user_agent}
    , m_state{State::Stopped}
{
    REALM_ASSERT(m_logger_ptr);                     // Make sure the logger is valid
    util::seed_prng_nondeterministically(m_random); // Throws
    if (auto_start) {
        start();
    }
}

DefaultSocketProvider::~DefaultSocketProvider()
{
    m_logger_ptr->trace("Default event loop teardown");
    // Wait for the thread to stop
    stop(true);
    // Shutting down - no need to lock mutex before check
    REALM_ASSERT(m_state == State::Stopped);
}

void DefaultSocketProvider::start()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    // Has the thread already been started or is running
    if (m_state == State::Starting || m_state == State::Running)
        return; // early return

    // If the thread has been previously run, make sure it has been joined first
    if (m_state == State::Stopping) {
        state_wait_for(lock, State::Stopped);
    }

    m_logger_ptr->trace("Default event loop: start()");
    REALM_ASSERT(m_state == State::Stopped);

    do_state_update(lock, State::Starting);
    m_thread = std::thread{&DefaultSocketProvider::event_loop, this};
    // Wait for the thread to start before continuing
    state_wait_for(lock, State::Running);
}

void DefaultSocketProvider::OnlyForTesting::run_event_loop_on_current_thread(DefaultSocketProvider* provider)
{
    {
        std::unique_lock<std::mutex> lk(provider->m_mutex);
        REALM_ASSERT(provider->m_state == State::Stopped);
        provider->do_state_update(lk, State::Starting);
    }

    provider->event_loop();
}

void DefaultSocketProvider::OnlyForTesting::prep_event_loop_for_restart(DefaultSocketProvider* provider)
{
    std::unique_lock<std::mutex> lk(provider->m_mutex);
    REALM_ASSERT(provider->m_state == State::Stopped);
    provider->m_service.reset();
}

void DefaultSocketProvider::event_loop()
{
    m_logger_ptr->trace("Default event loop: thread running");
    // Calls will_destroy_thread() when destroyed
    auto will_destroy_thread = util::make_scope_exit([&]() noexcept {
        m_logger_ptr->trace("Default event loop: thread exiting");
        if (m_observer_ptr)
            m_observer_ptr->will_destroy_thread();

        std::unique_lock<std::mutex> lock(m_mutex);
        // Did we get here due to an unhandled exception?
        if (m_state != State::Stopping) {
            m_logger_ptr->error("Default event loop: thread exited unexpectedly");
        }
        m_state = State::Stopped;
        lock.unlock();
        m_state_cv.notify_all();
    });

    if (m_observer_ptr)
        m_observer_ptr->did_create_thread();

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        REALM_ASSERT(m_state == State::Starting);
    }

    // We update the state to Running from inside the event loop so that start() is blocked until
    // the event loop is actually ready to receive work.
    m_service.post([this, my_generation = ++m_event_loop_generation](Status status) {
        if (status == ErrorCodes::OperationAborted) {
            return;
        }

        REALM_ASSERT(status.is_ok());

        std::unique_lock<std::mutex> lock(m_mutex);
        // This is a callback from a previous generation
        if (m_event_loop_generation != my_generation) {
            return;
        }
        if (m_state == State::Stopping) {
            return;
        }
        m_logger_ptr->trace("Default event loop: service run");
        REALM_ASSERT(m_state == State::Starting);
        do_state_update(lock, State::Running);
    });

    // If there is no event loop observer or handle_error function registered, then just
    // allow the exception to bubble to the top so we can get a true stack trace
    if (!m_observer_ptr || !m_observer_ptr->has_handle_error()) {
        m_service.run_until_stopped(); // Throws
    }
    else {
        try {
            m_service.run_until_stopped(); // Throws
        }
        catch (const std::exception& e) {
            REALM_ASSERT(m_observer_ptr); // should not change while event loop is running
            std::unique_lock<std::mutex> lock(m_mutex);
            // Service is no longer running, event loop thread is stopping
            do_state_update(lock, State::Stopping);
            lock.unlock();
            m_logger_ptr->error("Default event loop exception: ", e.what());
            // If the error was not handled by the thread loop observer, then rethrow
            if (!m_observer_ptr->handle_error(e))
                throw;
        }
    }
}

void DefaultSocketProvider::stop(bool wait_for_stop)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    // Do nothing if the thread is not started or running or stop has already been called
    if (m_state == State::Starting || m_state == State::Running) {
        m_logger_ptr->trace("Default event loop: stop()");
        do_state_update(lock, State::Stopping);
        // Updating state to Stopping will free a start() if it is waiting for the thread to
        // start and may cause the thread to exit early before calling service.run()
        m_service.stop(); // Unblocks m_service.run()
    }

    // Wait until the thread is stopped (exited) if requested
    if (wait_for_stop) {
        m_logger_ptr->trace("Default event loop: wait for stop");
        state_wait_for(lock, State::Stopped);
        if (m_thread.joinable()) {
            m_thread.join();
        }
    }
}

//                    +---------------------------------------+
//                   \/                                       |
// State Machine: Stopped -> Starting -> Running -> Stopping -+
//                              |           |          ^
//                              +----------------------+

void DefaultSocketProvider::do_state_update(std::unique_lock<std::mutex>&, State new_state)
{
    // m_state_mutex should already be locked...
    m_state = new_state;
    m_state_cv.notify_all(); // Let any waiters check the state
}

void DefaultSocketProvider::state_wait_for(std::unique_lock<std::mutex>& lock, State expected_state)
{
    // Check for condition already met or superseded
    if (m_state >= expected_state)
        return;

    m_state_cv.wait(lock, [this, expected_state]() {
        // are we there yet?
        if (m_state < expected_state)
            return false;
        return true;
    });
}

class DefaultWebSocketImplWrapper : public DefaultWebSocket {
public:
    DefaultWebSocketImplWrapper(const std::shared_ptr<util::Logger>& logger_ptr, network::Service& service,
                                std::mt19937_64& random, const std::string user_agent,
                                util::UniqueFunction<void(WebSocketEvent&&)> observer, WebSocketEndpoint&& endpoint)
        : m_impl(util::make_bind<DefaultWebSocketImpl>(logger_ptr, service, random, user_agent, std::move(observer),
                                                       std::move(endpoint)))
    {
    }

    ~DefaultWebSocketImplWrapper()
    {
        if (m_impl) {
            m_impl->close();
        }
    }

    void async_write_binary(util::Span<const char> data, SyncSocketProvider::FunctionHandler&& handler) override
    {
        m_impl->async_write_binary(data, std::move(handler));
    }
    std::string_view get_appservices_request_id() const noexcept override
    {
        return m_impl->get_appservices_request_id();
    }
    void force_handshake_response_for_testing(int status_code, std::string body) override
    {
        m_impl->force_handshake_response_for_testing(status_code, std::move(body));
    }

private:
    util::bind_ptr<DefaultWebSocketImpl> m_impl;
};

std::unique_ptr<WebSocketInterface>
DefaultSocketProvider::connect(util::UniqueFunction<void(WebSocketEvent&&)> observer, WebSocketEndpoint&& endpoint)
{
    return std::make_unique<DefaultWebSocketImplWrapper>(m_logger_ptr, m_service, m_random, m_user_agent,
                                                         std::move(observer), std::move(endpoint));
}

} // namespace realm::sync::websocket
