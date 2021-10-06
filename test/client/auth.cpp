#include <cstring>
#include <thread>
#include <chrono>

#include <realm/util/random.hpp>
#include <realm/util/json_parser.hpp>

#include "auth.hpp"

using namespace realm;
using Client = sync::auth::Client;

namespace {

const char* get_error_message(sync::auth::Error error)
{
    using Error = sync::auth::Error;
    switch (error) {
        case Error::unexpected_response_status:
            return "Unexpected HTTP reesponse status code";
        case Error::unauthorized:
            return "Unauthorized";
        case Error::bad_syntax:
            return "Bad syntax in HTTP response";
    }
    return nullptr;
}


class AuthErrorCategory : public std::error_category {
public:
    const char* name() const noexcept final override
    {
        return "realm::sync::auth::Error";
    }

    std::string message(int error_code) const override final
    {
        const char* msg = get_error_message(sync::auth::Error(error_code));
        return msg ? std::string{msg} : "unknown error";
    }
};

AuthErrorCategory g_auth_error_category;


util::StderrLogger g_fallback_logger;


std::string to_json(const std::string& str)
{
    for (char ch : str) {
        if (REALM_UNLIKELY(ch < 0x20 || ch > 0x7E || ch == '"' || ch == '\\'))
            throw std::runtime_error("Escaping not yet implemented");
    }
    return "\"" + str + "\""; // Throws
}


class ResponseParser {
public:
    bool access_token_found = false;
    bool refresh_token_found = false;
    std::string access_token;
    std::string refresh_token;

    ResponseParser(bool is_refresh)
        : m_is_refresh{is_refresh}
    {
    }

    std::error_condition operator()(util::JSONParser::Event event)
    {
        auto at_root_object = [&] {
            return (m_level == 1 && m_in_root_object);
        };
        auto end_of_value = [&] {
            REALM_ASSERT(at_root_object());
            REALM_ASSERT(m_next_token_is_value);
            m_next_token_is_value = false;
        };
        auto enter = [&] {
            if (at_root_object())
                REALM_ASSERT(m_next_token_is_value);
            ++m_level;
        };
        auto leave = [&] {
            REALM_ASSERT(m_level > 0);
            --m_level;
            if (at_root_object())
                end_of_value();
        };
        switch (event.type) {
            case util::JSONParser::EventType::number:
            case util::JSONParser::EventType::string:
            case util::JSONParser::EventType::boolean:
            case util::JSONParser::EventType::null:
                if (at_root_object()) {
                    bool is_key = !m_next_token_is_value;
                    if (is_key) {
                        REALM_ASSERT(event.type == util::JSONParser::EventType::string);
                        m_key = Key::other;
                        if (event.escaped_string_value() == "access_token") {
                            m_key = Key::access_token;
                        }
                        else if (event.escaped_string_value() == "refresh_token") {
                            if (!m_is_refresh)
                                m_key = Key::refresh_token;
                        }
                        m_next_token_is_value = true;
                    }
                    else {
                        switch (m_key) {
                            case Key::other:
                                break;
                            case Key::access_token:
                                access_token_found = true;
                                access_token = event.escaped_string_value(); // Throws
                                break;
                            case Key::refresh_token:
                                refresh_token_found = true;
                                refresh_token = event.escaped_string_value(); // Throws
                                break;
                        }
                        end_of_value();
                    }
                }
                return {};
            case util::JSONParser::EventType::array_begin:
                enter();
                return {};
            case util::JSONParser::EventType::array_end:
                leave();
                return {};
            case util::JSONParser::EventType::object_begin:
                enter();
                if (m_level == 1)
                    m_in_root_object = true;
                return {};
            case util::JSONParser::EventType::object_end:
                if (at_root_object())
                    m_in_root_object = false;
                leave();
                return {};
        }
        REALM_ASSERT(false);
        return {};
    }

private:
    enum class Key { other, access_token, refresh_token };
    const bool m_is_refresh;
    int m_level = 0;
    bool m_in_root_object = false;
    bool m_next_token_is_value = false;
    Key m_key;
};


bool get_tokens_from_login(const util::HTTPResponse& res, std::string& access_token, std::string& refresh_token)
{
    util::JSONParser json_parser{res.body};
    bool is_refresh = false;
    ResponseParser parser{is_refresh};
    std::error_condition ec = json_parser.parse(parser);
    bool good = (!ec && parser.access_token_found && parser.refresh_token_found);
    if (REALM_LIKELY(good)) {
        access_token = std::move(parser.access_token);
        refresh_token = std::move(parser.refresh_token);
        return true;
    }
    return false;
}


bool get_access_token_from_refresh(const util::HTTPResponse& res, std::string& access_token)
{
    util::JSONParser json_parser{res.body};
    bool is_refresh = true;
    ResponseParser parser{is_refresh};
    std::error_condition ec = json_parser.parse(parser);
    REALM_ASSERT(!parser.refresh_token_found);
    bool good = (!ec && parser.access_token_found);
    if (REALM_LIKELY(good)) {
        access_token = std::move(parser.access_token);
        return true;
    }
    return false;
}

} // unnamed namespace


const std::error_category& sync::auth::auth_error_category() noexcept
{
    return g_auth_error_category;
}


std::error_code sync::auth::make_error_code(auth::Error error) noexcept
{
    return std::error_code{int(error), g_auth_error_category};
}


class Client::Request {
public:
    util::Logger& logger;

    Request(Client& client, std::int_fast64_t request_counter);

    virtual ~Request() noexcept = default;

    void initiate();

    // These three socket like functions are needed to make the request object act
    // as a Socket for the HTTP handler.  They will also  be needed for
    // SSL support.
    void async_read(char* buffer, std::size_t size, std::function<void(std::error_code, std::size_t)> handler);

    void async_read_until(char* buffer, std::size_t size, char delim,
                          std::function<void(std::error_code, std::size_t)> handler);

    void async_write(const char* data, std::size_t size, std::function<void(std::error_code, std::size_t)> handler);

protected:
    Client& m_client;
    const std::int_fast64_t m_request_counter;
    util::Optional<util::HTTPClient<Request>> m_http_client;

    void disconnect_and_wait();
    void disconnect();

    void finalize();

private:
    // Used for reconnects.
    util::network::DeadlineTimer m_wait_timer;

    util::Optional<util::network::Resolver> m_resolver;
    util::Optional<util::network::Socket> m_socket;
    util::Optional<util::network::ssl::Context> m_ssl_context;
    util::Optional<util::network::ssl::Stream> m_ssl_stream;
    util::network::ReadAheadBuffer m_read_ahead_buffer;

    void initiate_resolve();
    void handle_resolve(std::error_code, util::network::Endpoint::List);
    void initiate_tcp_connect(util::network::Endpoint::List, std::size_t i);
    void handle_tcp_connect(std::error_code, util::network::Endpoint::List, std::size_t i);
    void initiate_ssl_handshake();
    void handle_ssl_handshake(std::error_code);

    void initiate_wait(std::uint_fast64_t delay);

    void initiate_http_request()
    {
        m_http_client.emplace(*this, logger); // Throws
        do_initiate_http_request();           // Throws
    }

    virtual void do_initiate_http_request() = 0;

    // Returns a new delay that is uniformly distributed between 0.5 and 1.5
    // times 'delay'. The argument 'delay' must not overflow by multiplication
    // by 150.
    std::uint_fast64_t randomize_delay(std::uint_fast64_t delay);
};


class Client::LoginRequest : public Request {
public:
    LoginRequest(Client&, std::int_fast64_t request_counter, std::function<LoginHandler>);
    LoginRequest(Client&, std::int_fast64_t request_counter, std::string username, std::string password,
                 std::function<LoginHandler>);

private:
    const bool m_has_user;
    const std::string m_username, m_password;
    std::function<LoginHandler> m_handler;

    void call_handler(std::error_code, std::string access_token, std::string refresh_token);

    void do_initiate_http_request() override final;

    void handle_http_request(const util::HTTPResponse&, std::error_code);
};


class Client::RefreshRequest : public Request {
public:
    RefreshRequest(Client&, std::int_fast64_t request_counter, std::string refresh_token,
                   std::function<RefreshHandler>);

private:
    const std::string m_refresh_token;
    std::function<RefreshHandler> m_handler;

    void call_handler(std::error_code, std::string access_token);

    void do_initiate_http_request() override final;

    void handle_http_request(const util::HTTPResponse&, std::error_code);
};


Client::Client(bool auth_ssl, std::string auth_address, port_type auth_port, std::string app_id, Config config)
    : logger{config.logger ? *config.logger : g_fallback_logger}
    , m_service{} // Throws
    , m_auth_ssl{auth_ssl}
    , m_auth_address{std::move(auth_address)}
    , m_auth_port{auth_port}
    , m_http_host{util::make_http_host(auth_ssl, auth_address, auth_port)} // Throws
    , m_max_number_of_connections{config.max_number_of_connections}
    , m_verify_servers_ssl_certificate{config.verify_servers_ssl_certificate}
    , m_ssl_trust_certificate_path{std::move(config.ssl_trust_certificate_path)}
    , m_ssl_verify_callback{std::move(config.ssl_verify_callback)}
    , m_request_base_path{std::move(config.request_base_path)}
    , m_app_request_path{m_request_base_path + "/app/" + app_id} // Throws
    , m_keep_running_timer{m_service}
{
    logger.info("Auth client started for server: [%1]:%2'", auth_address, auth_port);

    util::seed_prng_nondeterministically(m_random);
    start_keep_running_timer();
}


void Client::run()
{
    m_service.run();
}


void Client::stop() noexcept
{
    m_service.stop();
}


void Client::login_anon(std::function<LoginHandler> handler)
{
    auto handler_2 = [this, handler = std::move(handler)] {
        do_login_anon(std::move(handler)); // Throws
    };
    m_service.post(handler_2); // Throws
}


void Client::login_user(std::string username, std::string password, std::function<LoginHandler> handler)
{
    auto handler_2 = [this, username = std::move(username), password = std::move(password),
                      handler = std::move(handler)] {
        do_login_user(std::move(username), std::move(password), std::move(handler)); // Throws
    };
    m_service.post(handler_2); // Throws
}


void Client::refresh(std::string refresh_token, std::function<RefreshHandler> handler)
{
    auto handler_2 = [this, refresh_token = std::move(refresh_token), handler = std::move(handler)] {
        do_refresh(std::move(refresh_token), std::move(handler)); // Throws
    };
    m_service.post(handler_2); // Throws
}


util::network::Service& Client::get_service()
{
    return m_service;
}


const std::string& Client::get_auth_address()
{
    return m_auth_address;
}


auto Client::get_auth_port() -> port_type
{
    return m_auth_port;
}


void Client::request_is_done(std::int_fast64_t request_counter)
{
    std::size_t count = m_requests.erase(request_counter);
    REALM_ASSERT(count == 1);
    --m_active_requests;

    schedule_requests();
}


std::mt19937_64& Client::get_random()
{
    return m_random;
}


Client::~Client() noexcept {}


void Client::start_keep_running_timer()
{
    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted)
            start_keep_running_timer();
    };
    m_keep_running_timer.async_wait(std::chrono::hours(1000), handler);
}


void Client::do_login_anon(std::function<LoginHandler> handler)
{
    std::int_fast64_t request_counter = m_request_counter++;
    std::unique_ptr<Request> request =
        std::make_unique<LoginRequest>(*this, request_counter, std::move(handler)); // Throws
    m_requests.emplace(request_counter, std::move(request));                        // Throws
    schedule_requests();                                                            // Throws
}


void Client::do_login_user(std::string username, std::string password, std::function<LoginHandler> handler)
{
    std::int_fast64_t request_counter = m_request_counter++;
    std::unique_ptr<Request> request = std::make_unique<LoginRequest>(
        *this, request_counter, std::move(username), std::move(password), std::move(handler)); // Throws
    m_requests.emplace(request_counter, std::move(request));                                   // Throws
    schedule_requests();                                                                       // Throws
}


void Client::do_refresh(std::string access_token, std::function<RefreshHandler> handler)
{
    std::int_fast64_t request_counter = m_request_counter++;
    std::unique_ptr<Request> request =
        std::make_unique<RefreshRequest>(*this, request_counter, std::move(access_token),
                                         std::move(handler)); // Throws
    m_requests.emplace(request_counter, std::move(request));  // Throws
    schedule_requests();                                      // Throws
}


void Client::schedule_requests()
{
    while (m_pending_request_counter < m_request_counter && m_active_requests < m_max_number_of_connections) {
        m_requests.at(m_pending_request_counter)->initiate(); // Throws
        ++m_pending_request_counter;
        ++m_active_requests;
    }
}


// Request

Client::Request::Request(Client& client, std::int_fast64_t request_counter)
    : logger{client.logger}
    , m_client{client}
    , m_request_counter{request_counter}
    , m_wait_timer{client.get_service()}
{
}


void Client::Request::initiate()
{
    initiate_resolve();
}


void Client::Request::async_read(char* buffer, size_t size, std::function<void(std::error_code, std::size_t)> handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read(buffer, size, m_read_ahead_buffer, handler); // Throws
    }
    else {
        m_socket->async_read(buffer, size, m_read_ahead_buffer, handler); // Throws
    }
}


void Client::Request::async_read_until(char* buffer, std::size_t size, char delim,
                                       std::function<void(std::error_code, std::size_t)> handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_read_until(buffer, size, delim, m_read_ahead_buffer, handler); // Throws
    }
    else {
        m_socket->async_read_until(buffer, size, delim, m_read_ahead_buffer, handler); // Throws
    }
}


void Client::Request::async_write(const char* data, std::size_t size,
                                  std::function<void(std::error_code, std::size_t)> handler)
{
    REALM_ASSERT(m_socket);
    if (m_ssl_stream) {
        m_ssl_stream->async_write(data, size, handler); // Throws
    }
    else {
        m_socket->async_write(data, size, handler); // Throws
    }
}


void Client::Request::initiate_resolve()
{
    REALM_ASSERT(!m_socket);

    const std::string& auth_address = m_client.get_auth_address();
    port_type auth_port = m_client.get_auth_port();

    logger.debug("Resolving [%1]:%2", auth_address, auth_port);

    util::network::Resolver::Query query(auth_address, util::to_string(auth_port));
    auto handler = [this](std::error_code ec, util::network::Endpoint::List endpoints) {
        if (ec != util::error::operation_aborted)
            handle_resolve(ec, std::move(endpoints));
    };
    m_resolver.emplace(m_client.get_service());
    m_resolver->async_resolve(std::move(query), std::move(handler));
}


void Client::Request::handle_resolve(std::error_code ec, util::network::Endpoint::List endpoints)
{
    m_resolver = none;
    if (ec) {
        logger.error("Resolve failed with error = %1", ec);
        disconnect_and_wait();
        return;
    }
    initiate_tcp_connect(std::move(endpoints), 0);
}


void Client::Request::initiate_tcp_connect(util::network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());

    util::network::Endpoint ep = *(endpoints.begin() + i);
    logger.debug("Connecting to endpoint [%1]:%2 (%3/%4)", ep.address(), ep.port(), (i + 1), endpoints.size());

    auto handler = [this, endpoints = std::move(endpoints), i](std::error_code ec) mutable {
        if (ec != util::error::operation_aborted)
            handle_tcp_connect(ec, std::move(endpoints), i);
    };
    m_socket.emplace(m_client.get_service());
    m_socket->async_connect(ep, std::move(handler));
}


void Client::Request::handle_tcp_connect(std::error_code ec, util::network::Endpoint::List endpoints, std::size_t i)
{
    REALM_ASSERT(i < endpoints.size());
    const util::network::Endpoint& ep = *(endpoints.begin() + i);
    if (ec) {
        logger.debug("Failed to connect to endpoint [%1]:%2: %3", ep.address(), ep.port(), ec.message());
        std::size_t i_2 = i + 1;
        if (i_2 < endpoints.size()) {
            initiate_tcp_connect(std::move(endpoints), i_2);
            return;
        }
        // All endpoints failed
        logger.error("All connection attempts to the auth server failed.");
        disconnect_and_wait();
        return;
    }

    REALM_ASSERT(m_socket);
    util::network::Endpoint ep_2 = m_socket->local_endpoint();
    logger.debug("Connected to endpoint [%1]:%2 (from [%3]:%4)", ep.address(), ep.port(), ep_2.address(),
                 ep_2.port()); // Throws

    if (m_client.m_auth_ssl) {
        initiate_ssl_handshake(); // Throws
    }
    else {
        initiate_http_request(); // Throws
    }
}


void Client::Request::initiate_ssl_handshake()
{
    using namespace util::network::ssl;

    if (!m_ssl_context) {
        m_ssl_context.emplace(); // Throws
        if (m_client.m_verify_servers_ssl_certificate) {
            if (m_client.m_ssl_trust_certificate_path) {
                m_ssl_context->use_verify_file(*m_client.m_ssl_trust_certificate_path); // Throws
            }
            else if (!m_client.m_ssl_verify_callback) {
                m_ssl_context->use_default_verify(); // Throws
            }
        }
    }

    m_ssl_stream.emplace(*m_socket, *m_ssl_context, Stream::client); // Throws
    m_ssl_stream->set_logger(&logger);
    m_ssl_stream->set_host_name(m_client.m_auth_address); // Throws
    if (m_client.m_verify_servers_ssl_certificate) {
        m_ssl_stream->set_verify_mode(VerifyMode::peer); // Throws
        m_ssl_stream->set_server_port(m_client.m_auth_port);
        if (!m_client.m_ssl_trust_certificate_path) {
            if (m_client.m_ssl_verify_callback) {
                m_ssl_stream->use_verify_callback(m_client.m_ssl_verify_callback);
            }
            else {
                // The included certificates are used if neither the trust
                // certificate nor the callback function is set.
#if REALM_INCLUDE_CERTS
                m_ssl_stream->use_included_certificates(); // Throws
#endif
            }
        }
    }

    auto handler = [this](std::error_code ec) {
        // If the operation is aborted, the connection object may have been
        // destroyed.
        if (ec != util::error::operation_aborted)
            handle_ssl_handshake(ec); // Throws
    };
    m_ssl_stream->async_handshake(std::move(handler)); // Throws

    // FIXME: We also need to perform the SSL shutdown operation somewhere
}


void Client::Request::handle_ssl_handshake(std::error_code ec)
{
    if (ec) {
        REALM_ASSERT(ec != util::error::operation_aborted);
        logger.error("SSL handshake failed: %1", ec.message()); // Throws
        disconnect_and_wait();                                  // Throws
        return;
    }

    initiate_http_request(); // Throws
}


void Client::Request::initiate_wait(std::uint_fast64_t delay)
{
    logger.debug("Waiting %1 ms before connecting to the auth server", delay);

    auto handler = [this](std::error_code ec) {
        if (ec != util::error::operation_aborted) {
            initiate_resolve();
        }
    };
    std::chrono::milliseconds duration{delay};
    m_wait_timer.async_wait(duration, handler);
}


void Client::Request::disconnect_and_wait()
{
    disconnect();
    std::uint_fast64_t delay = randomize_delay(10000); // around 10 seconds
    initiate_wait(delay);
}


void Client::Request::disconnect()
{
    m_resolver = util::none;
    m_socket = util::none;
    m_ssl_stream = util::none;
    m_read_ahead_buffer.clear();
    m_http_client = util::none;
}


void Client::Request::finalize()
{
    disconnect();
    m_client.request_is_done(m_request_counter);
}


// FIXME: Use sync::milliseconds_type.
std::uint_fast64_t Client::Request::randomize_delay(std::uint_fast64_t delay)
{
    std::uniform_int_distribution<std::uint_fast64_t> dis(50, 150);
    std::uint_fast64_t number = dis(m_client.get_random());
    return delay * number / 100;
}


Client::LoginRequest::LoginRequest(Client& client, std::int_fast64_t request_counter,
                                   std::function<LoginHandler> handler)
    : Request{client, request_counter}
    , m_has_user{false}
    , m_handler{std::move(handler)}
{
}


Client::LoginRequest::LoginRequest(Client& client, std::int_fast64_t request_counter, std::string username,
                                   std::string password, std::function<LoginHandler> handler)
    : Request{client, request_counter}
    , m_has_user{true}
    , m_username{std::move(username)}
    , m_password{std::move(password)}
    , m_handler{std::move(handler)}
{
}


void Client::LoginRequest::call_handler(std::error_code ec, std::string access_token, std::string refresh_token)
{
    m_handler(ec, std::move(access_token), std::move(refresh_token)); // Throws
    m_handler = {};
}


void Client::LoginRequest::do_initiate_http_request()
{
    std::string path, body;
    if (m_has_user) {
        logger.debug("Requesting user login");                                       // Throws
        path = m_client.m_app_request_path + "/auth/providers/local-userpass/login"; // Throws
        body = "{"
               "\"provider\": \"local-userpass\", "
               "\"username\": " +
               to_json(m_username) +
               ", "
               "\"password\": " +
               to_json(m_password) + "}"; // Throws
    }
    else {
        logger.debug("Requesting anonymous login");                             // Throws
        path = m_client.m_app_request_path + "/auth/providers/anon-user/login"; // Throws
        body = "{\"provider\": \"anon-user\"}";                                 // Throws
    }
    util::HTTPRequest req;
    req.method = util::HTTPMethod::Post;
    req.path = std::move(path);
    req.body = std::move(body);
    req.headers["Content-Type"] = "application/json; charset=utf-8";   // Throws
    req.headers["Content-Length"] = util::to_string(req.body->size()); // Throws
    req.headers["Accept"] = "application/json";                        // Throws
    req.headers["Host"] = m_client.m_http_host;                        // Throws

    auto handler = [this](util::HTTPResponse res, std::error_code ec) {
        if (ec != util::error::operation_aborted)
            handle_http_request(res, ec); // Throws
    };
    m_http_client->async_request(req, handler); // Throws
}


void Client::LoginRequest::handle_http_request(const util::HTTPResponse& res, std::error_code ec)
{
    if (ec)
        return disconnect_and_wait();

    logger.trace("Login response: %1", res);

    if (res.status == util::HTTPStatus::Ok) {
        logger.debug("Login was successful");
        std::string access_token, refresh_token;
        if (get_tokens_from_login(res, access_token, refresh_token)) {           // Throws
            std::error_code ec;                                                  // Success
            call_handler(ec, std::move(access_token), std::move(refresh_token)); // Throws
            finalize();
        }
        else {
            logger.error("Login failed: Bad syntax in response");
            call_handler(make_error_code(Error::bad_syntax), {}, {}); // Throws
            finalize();
        }
    }
    else if (res.status == util::HTTPStatus::Unauthorized) {
        logger.debug("Login failed: Unauthorized");
        call_handler(make_error_code(Error::unauthorized), {}, {}); // Throws
        finalize();
    }
    else {
        logger.error("Login failed: Bad HTTP response status");
        call_handler(make_error_code(Error::unexpected_response_status), {}, {}); // Throws
        finalize();
    }
}


Client::RefreshRequest::RefreshRequest(Client& client, std::int_fast64_t request_counter, std::string refresh_token,
                                       std::function<RefreshHandler> handler)
    : Request{client, request_counter}
    , m_refresh_token{std::move(refresh_token)}
    , m_handler{std::move(handler)}
{
}


void Client::RefreshRequest::call_handler(std::error_code ec, std::string access_token)
{
    m_handler(ec, std::move(access_token)); // Throws
    m_handler = {};
}


void Client::RefreshRequest::do_initiate_http_request()
{
    logger.debug("Requesting access token refresh"); // Throws
    util::HTTPRequest req;
    req.method = util::HTTPMethod::Post;
    req.path = m_client.m_request_base_path + "/auth/session"; // Throws
    req.headers["Accept"] = "application/json";                // Throws
    req.headers["Authorization"] = "Bearer " + m_refresh_token;
    req.headers["Host"] = m_client.m_http_host; // Throws

    auto handler = [this](util::HTTPResponse res, std::error_code ec) {
        if (ec != util::error::operation_aborted)
            handle_http_request(res, ec); // Throws
    };
    m_http_client->async_request(req, handler); // Throws
}


void Client::RefreshRequest::handle_http_request(const util::HTTPResponse& res, std::error_code ec)
{
    if (ec)
        return disconnect_and_wait();

    logger.trace("Refresh response: %1", res);

    if (res.status == util::HTTPStatus::Created) {
        logger.debug("Refresh was successful");
        std::string access_token;
        if (get_access_token_from_refresh(res, access_token)) { // Throws
            std::error_code ec;                                 // Success
            call_handler(ec, std::move(access_token));          // Throws
            finalize();
        }
        else {
            logger.error("Refresh failed: Bad syntax in response");
            call_handler(make_error_code(Error::bad_syntax), {}); // Throws
            finalize();
        }
    }
    else if (res.status == util::HTTPStatus::Unauthorized) {
        logger.debug("Refresh failed: Unauthorized");
        call_handler(make_error_code(Error::unauthorized), {}); // Throws
        finalize();
    }
    else {
        logger.error("Refresh failed: Bad HTTP response status");
        call_handler(make_error_code(Error::unexpected_response_status), {}); // Throws
        finalize();
    }
}
