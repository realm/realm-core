#include "test.hpp"
#include "../test/util/thread_wrapper.hpp"

#include <realm/util/memory_stream.hpp>
#include <realm/util/event_loop.hpp>

using namespace realm::util;
using namespace realm::test_util;

TEST_TYPES(EventLoop_Timer, ASIO)
{
    EventLoop<TEST_TYPE> loop;
    bool ran = false;
    auto timer = loop.async_timer(std::chrono::milliseconds(1), [&](std::error_code ec) {
        CHECK(!ec);
        ran = true;
    });
    CHECK(!ran);
    loop.run();
    CHECK(ran);
}


namespace {

network::endpoint bind_acceptor(network::acceptor& acceptor)
{
    network::io_service& service = acceptor.service();
    network::resolver resolver(service);
    network::resolver::query query("localhost",
                                   "", // Assign the port dynamically
                                   network::resolver::query::passive |
                                   network::resolver::query::address_configured);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);
    {
        auto i = endpoints.begin();
        auto end = endpoints.end();
        for (;;) {
            std::error_code ec;
            acceptor.bind(*i, ec);
            if (!ec)
                break;
            acceptor.close();
            if (++i == end)
                throw std::runtime_error("Failed to bind to localhost:*");
        }
    }
    return acceptor.local_endpoint();
}

char echo_body[] = {
    '\xC1', '\x2C', '\xEF', '\x48', '\x8C', '\xCD', '\x41', '\xFA',
    '\x12', '\xF9', '\xF4', '\x72', '\xDF', '\x92', '\x8E', '\x68',
    '\xAB', '\x8F', '\x6B', '\xDF', '\x80', '\x26', '\xD1', '\x60',
    '\x21', '\x91', '\x20', '\xC8', '\x94', '\x0C', '\xDB', '\x07',
    '\xB0', '\x1C', '\x3A', '\xDA', '\x5E', '\x9B', '\x62', '\xDE',
    '\x30', '\xA3', '\x7E', '\xED', '\xB4', '\x30', '\xD7', '\x43',
    '\x3F', '\xDE', '\xF2', '\x6D', '\x9A', '\x1D', '\xAE', '\xF4',
    '\xD5', '\xFB', '\xAC', '\xE8', '\x67', '\x37', '\xFD', '\xF3'
};

class AsyncServer {
public:
    AsyncServer(unit_test::TestResults& test_results):
        m_acceptor(m_service),
        m_socket(m_service),
        m_input_stream(m_socket),
        m_test_results(test_results)
    {
    }

    unsigned short init()
    {
        network::endpoint listen_endpoint = bind_acceptor(m_acceptor);
        network::endpoint::port_type listen_port = listen_endpoint.port();
        m_acceptor.listen();
        return listen_port;
    }

    void run()
    {
        auto handler = [=](std::error_code ec) {
            handle_accept(ec);
        };
        network::endpoint endpoint;
        m_acceptor.async_accept(m_socket, endpoint, handler);
        m_service.run();
    }

private:
    network::io_service m_service;
    network::acceptor m_acceptor;
    network::socket m_socket;
    network::buffered_input_stream m_input_stream;
    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestResults& m_test_results;

    void handle_accept(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t n) {
            handle_read_header(ec, n);
        };
        m_input_stream.async_read_until(m_header_buffer, s_max_header_size, '\n', handler);
    }

    void handle_read_header(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestResults& test_results = m_test_results;
        if (!CHECK_GREATER(n, 0))
            return;
        if (!CHECK_LESS_EQUAL(n, s_max_header_size+0))
            return;
        if (!CHECK_EQUAL(m_header_buffer[n-1], '\n'))
            return;
        MemoryInputStream in;
        in.set_buffer(m_header_buffer, m_header_buffer+(n-1));
        in.unsetf(std::ios_base::skipws);
        std::string message_type;
        in >> message_type;
        if (!CHECK_EQUAL(message_type, "echo"))
            return;
        char sp;
        in >> sp >> m_body_size;
        if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
            return;
        auto handler = [=](std::error_code ec, size_t n) {
            handle_read_body(ec, n);
        };
        m_body_buffer.reset(new char[m_body_size]);
        m_input_stream.async_read(m_body_buffer.get(), m_body_size, handler);
    }

    void handle_read_body(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestResults& test_results = m_test_results;
        if (!CHECK_EQUAL(n, m_body_size))
            return;
        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer+s_max_header_size);
        out << "was " << m_body_size << '\n';
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_header(ec);
        };
        m_socket.async_write(m_header_buffer, out.size(), handler);
    }

    void handle_write_header(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_body(ec);
        };
        m_socket.async_write(m_body_buffer.get(), m_body_size, handler);
    }

    void handle_write_body(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t) {
            handle_read_header_2(ec);
        };
        m_input_stream.async_read_until(m_header_buffer, s_max_header_size, '\n', handler);
    }

    void handle_read_header_2(std::error_code ec)
    {
        if (ec && ec != network::end_of_input)
            throw std::system_error(ec);
        unit_test::TestResults& test_results = m_test_results;
        CHECK(ec == network::end_of_input);
    }
};


template<class Provider>
class AsyncClient {
public:
    AsyncClient(unsigned short listen_port, unit_test::TestResults& test_results):
        m_listen_port(listen_port),
        m_test_results(test_results)
    {

    }

    void run()
    {
        m_loop.reset(new EventLoop<Provider>);
        m_socket = m_loop->async_connect("localhost", m_listen_port, [=](std::error_code ec) {
            auto& test_results = this->m_test_results;
            CHECK(!ec);
            this->connection_established();
        });

        m_loop->run();

        if (m_socket) {
            m_socket->close();
        }
    }

    void connection_established()
    {
        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer+s_max_header_size);
        out << "echo " << sizeof echo_body << '\n';
        auto handler = [=](std::error_code ec, size_t) {
            this->handle_write_header(ec);
        };
        m_socket->async_write(m_header_buffer, out.size(), handler);
    }

private:
    unsigned short m_listen_port;
    std::unique_ptr<EventLoop<Provider>> m_loop;
    std::unique_ptr<SocketBase> m_socket;

    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestResults& m_test_results;

    void handle_write_header(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_body(ec);
        };
        m_socket->async_write(echo_body, sizeof echo_body, handler);
    }

    void handle_write_body(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t n) {
            handle_read_header(ec, n);
        };
        m_socket->async_read_until(m_header_buffer, s_max_header_size, '\n', handler);
    }

    void handle_read_header(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestResults& test_results = m_test_results;
        if (!CHECK_GREATER(n, 0))
            return;
        if (!CHECK_LESS_EQUAL(n, s_max_header_size+0))
            return;
        if (!CHECK_EQUAL(m_header_buffer[n-1], '\n'))
            return;
        MemoryInputStream in;
        in.set_buffer(m_header_buffer, m_header_buffer+(n-1));
        in.unsetf(std::ios_base::skipws);
        std::string message_type;
        in >> message_type;
        if (!CHECK_EQUAL(message_type, "was"))
            return;
        char sp;
        in >> sp >> m_body_size;
        if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
            return;
        auto handler = [=](std::error_code ec, size_t n) {
            handle_read_body(ec, n);
        };
        m_body_buffer.reset(new char[m_body_size]);
        m_socket->async_read(m_body_buffer.get(), m_body_size, handler);
    }

    void handle_read_body(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestResults& test_results = m_test_results;
        if (!CHECK_EQUAL(n, m_body_size))
            return;
        if (!CHECK_EQUAL(m_body_size, sizeof echo_body))
            return;
        CHECK(std::equal(echo_body, echo_body + sizeof echo_body, m_body_buffer.get()));
    }
};

} // anonymous namespace

TEST_TYPES(EventLoop_AsyncCommunication, Apple)
{
    AsyncServer server(test_results);
    unsigned short listen_port = server.init();
    AsyncClient<TEST_TYPE> client(listen_port, test_results);

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] { server.run(); });
    client_thread.start([&] { client.run(); });
    client_thread.join();
    server_thread.join();
}

