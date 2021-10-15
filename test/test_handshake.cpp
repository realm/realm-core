#include <realm/sync/client.hpp>
#include <realm/util/http.hpp>
#include <realm/util/websocket.hpp>

#include "test.hpp"
#include "util/thread_wrapper.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;

using namespace realm::test_util;

using port_type = util::network::Endpoint::port_type;
using ConnectionStateChangeListener = Session::ConnectionStateChangeListener;
using ErrorInfo = Session::ErrorInfo;

namespace {

// SurpriseServer is a server that listens on a port accepts a single
// connection waits for a HTTP request and returns a HTTP response.
// The response depends on the URL of the request. For instance,
// a request to /realm-sync/301 will send a
// HTTP/1.1 301 Moved Permanently response.
class SurpriseServer {
public:
    SurpriseServer(util::Logger& logger)
        : m_acceptor{m_service}
        , m_socket{m_service}
        , m_http_server{*this, logger}
    {
    }

    void start()
    {
        m_acceptor.open(util::network::StreamProtocol::ip_v4());
        m_acceptor.listen();

        auto handler = [this](std::error_code ec) {
            REALM_ASSERT(!ec);
            this->handle_accept(); // Throws
        };
        m_acceptor.async_accept(m_socket, handler); // Throws
    }

    void run()
    {
        m_service.run();
    }

    void stop()
    {
        m_service.stop();
    }

    util::network::Endpoint listen_endpoint() const
    {
        return m_acceptor.local_endpoint();
    }

    void async_read_until(char* buffer, size_t size, char delim, std::function<void(std::error_code, size_t)> handler)
    {
        m_socket.async_read_until(buffer, size, delim, m_read_ahead_buffer, handler); // Throws
    }

    void async_read(char* buffer, size_t size, std::function<void(std::error_code, size_t)> handler)
    {
        m_socket.async_read(buffer, size, m_read_ahead_buffer, handler); // Throws
    }

private:
    util::network::Service m_service;
    util::network::Acceptor m_acceptor;
    util::network::Socket m_socket;
    util::network::ReadAheadBuffer m_read_ahead_buffer;
    util::HTTPServer<SurpriseServer> m_http_server;
    std::string m_response;

    void handle_accept()
    {
        auto handler = [this](HTTPRequest request, std::error_code ec) {
            REALM_ASSERT(!ec);
            this->handle_http_request(request); // Throws
        };
        m_http_server.async_receive_request(std::move(handler)); // Throws
    }

    void handle_http_request(const HTTPRequest& request)
    {
        const std::string& path = request.path;
        const std::string expected_prefix = "/realm-sync/%2F";
        REALM_ASSERT(path.compare(0, expected_prefix.size(), expected_prefix) == 0);
        std::string key = path.substr(expected_prefix.size());
        if (key == "http_1_0")
            send_http_1_0();
        else if (key == "invalid-status-code")
            send_invalid_status_code();
        else if (key == "missing-websocket-headers")
            send_missing_websocket_headers();
        else if (key == "200")
            send_200();
        else if (key == "201")
            send_201();
        else if (key == "300")
            send_300();
        else if (key == "301")
            send_301();
        else if (key == "400")
            send_400();
        else if (key == "401")
            send_401();
        else if (key == "403")
            send_403();
        else if (key == "404")
            send_404();
        else if (key == "500")
            send_500();
        else if (key == "501")
            send_501();
        else if (key == "502")
            send_502();
        else if (key == "503")
            send_503();
        else if (key == "504")
            send_504();
        else
            send_nothing();
    }

    void send_response()
    {
        auto handler = [=](std::error_code ec, size_t nwritten) {
            REALM_ASSERT(!ec);
            REALM_ASSERT(nwritten == m_response.size());
        };
        m_socket.async_write(m_response.data(), m_response.size(), handler);
    }

    void send_http_1_0()
    {
        m_response = "HTTP/1.0 200 OK\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_invalid_status_code()
    {
        m_response = "HTTP/1.1 99999 Strange\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_missing_websocket_headers()
    {
        m_response = "HTTP/1.1 101 Switching Protocols\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_200()
    {
        m_response = "HTTP/1.1 200 OK\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_201()
    {
        m_response = "HTTP/1.1 201 Created\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_300()
    {
        m_response = "HTTP/1.1 300 Multiple Choices\r\n"
                     "Server: surprise-server\r\n"
                     "Location: http://10.0.0.0\r\n"
                     "\r\n";
        send_response();
    }

    void send_301()
    {
        m_response = "HTTP/1.1 301 Moved Permanently\r\n"
                     "Server: surprise-server\r\n"
                     "Location: http://10.0.0.0\r\n"
                     "\r\n";
        send_response();
    }

    void send_400()
    {
        m_response = "HTTP/1.1 400 Bad Request\r\n"
                     "Server: surprise-server\r\n"
                     "Location: http://10.0.0.0\r\n"
                     "\r\n";
        send_response();
    }

    void send_401()
    {
        m_response = "HTTP/1.1 401 Unauthorized\r\n"
                     "Server: surprise-server\r\n"
                     "Location: http://10.0.0.0\r\n"
                     "\r\n";
        send_response();
    }

    void send_403()
    {
        m_response = "HTTP/1.1 403 Forbidden\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_404()
    {
        m_response = "HTTP/1.1 404 Not Found\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_500()
    {
        m_response = "HTTP/1.1 500 Internal Server Error\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_501()
    {
        m_response = "HTTP/1.1 501 Not Implemented\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_502()
    {
        m_response = "HTTP/1.1 502 Bad Gateway\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_503()
    {
        m_response = "HTTP/1.1 503 Service Unavailable\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_504()
    {
        m_response = "HTTP/1.1 504 Gateway Timeout\r\n"
                     "Server: surprise-server\r\n"
                     "\r\n";
        send_response();
    }

    void send_nothing()
    {
        // no-op
    }
};

// This function creates a Surprise Server and a sync client, lets the sync
// client initiate a sync connection which the surprise server responds to.
// The response depends on the server path. The check is that the clients
// ConnectionStateChangeListener is called with the proper error code and
// is_fatal value.
void run_client_surprise_server(unit_test::TestContext& test_context, const std::string server_path,
                                std::error_code ec, bool is_fatal)
{
    SHARED_GROUP_TEST_PATH(path);

    util::Logger& logger = test_context.logger;
    util::PrefixLogger server_logger("Server: ", logger);
    util::PrefixLogger client_logger("Client: ", logger);

    SurpriseServer server{server_logger};
    server.start();
    ThreadWrapper server_thread;
    server_thread.start([&] {
        server.run();
    });

    Client::Config client_config;
    client_config.logger = &client_logger;
    client_config.one_connection_per_session = true;
    client_config.tcp_no_delay = true;
    Client client(client_config);

    ThreadWrapper client_thread;
    client_thread.start([&] {
        client.run();
    });

    Session::Config session_config;
    session_config.server_address = "localhost";
    session_config.server_port = server.listen_endpoint().port();
    session_config.server_path = server_path;

    Session session{client, path, session_config};

    std::function<ConnectionStateChangeListener> connection_state_listener = [&](ConnectionState connection_state,
                                                                                 const ErrorInfo* error_info) {
        if (error_info) {
            CHECK(connection_state == ConnectionState::disconnected);
            CHECK_EQUAL(ec, error_info->error_code);
            CHECK_EQUAL(is_fatal, error_info->is_fatal);
            client.stop();
        }
    };
    session.set_connection_state_change_listener(connection_state_listener);
    session.bind();
    session.wait_for_download_complete_or_client_stopped();

    client.stop();
    client_thread.join();
    server.stop();
    server_thread.join();
}

} // unnamed namespace


namespace {

TEST(Handshake_HTTP_Version)
{
    const std::string server_path = "/http_1_0";
    std::error_code ec = util::websocket::Error::bad_response_invalid_http;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_InvalidStatusCode)
{
    const std::string server_path = "/invalid-status-code";
    std::error_code ec = util::websocket::Error::bad_response_invalid_http;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_MissingWebSocketHeaders)
{
    const std::string server_path = "/missing-websocket-headers";
    std::error_code ec = util::websocket::Error::bad_response_header_protocol_violation;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_200)
{
    const std::string server_path = "/200";
    std::error_code ec = util::websocket::Error::bad_response_200_ok;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_201)
{
    const std::string server_path = "/201";
    std::error_code ec = util::websocket::Error::bad_response_2xx_successful;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_300)
{
    const std::string server_path = "/300";
    std::error_code ec = util::websocket::Error::bad_response_3xx_redirection;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_301)
{
    const std::string server_path = "/301";
    std::error_code ec = util::websocket::Error::bad_response_301_moved_permanently;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_400)
{
    const std::string server_path = "/400";
    std::error_code ec = util::websocket::Error::bad_response_4xx_client_errors;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_401)
{
    const std::string server_path = "/401";
    std::error_code ec = util::websocket::Error::bad_response_401_unauthorized;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_403)
{
    const std::string server_path = "/403";
    std::error_code ec = util::websocket::Error::bad_response_403_forbidden;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_404)
{
    const std::string server_path = "/404";
    std::error_code ec = util::websocket::Error::bad_response_404_not_found;
    bool is_fatal = true;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_500)
{
    const std::string server_path = "/500";
    std::error_code ec = util::websocket::Error::bad_response_500_internal_server_error;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_501)
{
    const std::string server_path = "/501";
    std::error_code ec = util::websocket::Error::bad_response_5xx_server_error;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_502)
{
    const std::string server_path = "/502";
    std::error_code ec = util::websocket::Error::bad_response_502_bad_gateway;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_503)
{
    const std::string server_path = "/503";
    std::error_code ec = util::websocket::Error::bad_response_503_service_unavailable;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

TEST(Handshake_504)
{
    const std::string server_path = "/504";
    std::error_code ec = util::websocket::Error::bad_response_504_gateway_timeout;
    bool is_fatal = false;
    run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

// Enable when the client gets a handshake timeout.
TEST_IF(Handshake_Timeout, false)
{
    // const std::string server_path = "/nothing";
    // std::error_code ec = util::websocket::Error::; // CHANGE
    // bool is_fatal = false;
    // run_client_surprise_server(test_context, server_path, ec, is_fatal);
}

// Test connection to external server. This test should Only be enabled
// during testing.
TEST_IF(Handshake_ExternalServer, false)
{
    const std::string server_address = "www.realm.io";
    port_type server_port = 80;

    SHARED_GROUP_TEST_PATH(path);
    util::Logger& logger = test_context.logger;
    util::PrefixLogger client_logger("Client: ", logger);

    Client::Config client_config;
    client_config.logger = &client_logger;
    client_config.one_connection_per_session = true;
    client_config.tcp_no_delay = true;
    Client client(client_config);

    ThreadWrapper client_thread;
    client_thread.start([&] {
        client.run();
    });

    Session::Config session_config;
    session_config.server_address = server_address;
    session_config.server_port = server_port;
    session_config.server_path = "/default";

    Session session{client, path, session_config};

    std::function<ConnectionStateChangeListener> connection_state_listener = [&](ConnectionState connection_state,
                                                                                 const ErrorInfo* error_info) {
        if (error_info) {
            CHECK(connection_state == ConnectionState::disconnected);
            std::error_code ec = util::websocket::Error::bad_response_301_moved_permanently;
            CHECK_EQUAL(ec, error_info->error_code);
            CHECK_EQUAL(true, error_info->is_fatal);
            client.stop();
        }
    };
    session.set_connection_state_change_listener(connection_state_listener);
    session.bind();
    session.wait_for_download_complete_or_client_stopped();

    client.stop();
    client_thread.join();
}

} // unnamed namespace
