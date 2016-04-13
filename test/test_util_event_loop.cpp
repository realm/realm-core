#include "test.hpp"

#include <realm/util/memory_stream.hpp>
#include <realm/util/event_loop.hpp>

using namespace realm::util;
using namespace realm::test_util;

template <class T> struct GetEventLoop;
struct POSIX {};
struct PlatformLocal {};

template <> struct GetEventLoop<POSIX> {
    GetEventLoop(): m_loop(get_posix_event_loop()), loop(*m_loop)
    {}
    std::unique_ptr<EventLoop> m_loop;
    EventLoop& loop;
};

template <> struct GetEventLoop<PlatformLocal> {
    GetEventLoop(): loop(get_native_event_loop())
    {}
    EventLoop& loop;
};

TEST_TYPES(EventLoop_Timer, POSIX, PlatformLocal)
{
    GetEventLoop<TEST_TYPE> get_loop;
    EventLoop& loop = get_loop.loop;
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
    AsyncServer(unit_test::TestContext& test_context):
        m_acceptor(m_service),
        m_socket(m_service),
        m_input_stream(m_socket),
        m_test_context(test_context)
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
    unit_test::TestContext& m_test_context;

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
        unit_test::TestContext& test_context = m_test_context;
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
        unit_test::TestContext& test_context = m_test_context;
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
        unit_test::TestContext& test_context = m_test_context;
        CHECK(ec == network::end_of_input);
    }
};


template<class Provider>
class AsyncClient {
public:
    AsyncClient(unsigned short listen_port, unit_test::TestContext& test_context):
        m_get_loop(),
        m_loop(m_get_loop.loop),
        m_listen_port(listen_port),
        m_test_context(test_context)
    {

    }

    void run()
    {
        m_socket = m_loop.async_connect("localhost", m_listen_port, SocketSecurity::None,
                                          [=](std::error_code ec) {
            auto& test_context = this->m_test_context;
            CHECK(!ec);
            this->connection_established();
        });

        m_loop.run();

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
    GetEventLoop<Provider> m_get_loop;
    EventLoop& m_loop;
    unsigned short m_listen_port;
    std::unique_ptr<Socket> m_socket;

    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestContext& m_test_context;

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
        unit_test::TestContext& test_context = m_test_context;
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
        unit_test::TestContext& test_context = m_test_context;
        if (!CHECK_EQUAL(n, m_body_size))
            return;
        if (!CHECK_EQUAL(m_body_size, sizeof echo_body))
            return;
        CHECK(std::equal(echo_body, echo_body + sizeof echo_body, m_body_buffer.get()));
    }
};

} // anonymous namespace

TEST_TYPES(EventLoop_AsyncCommunication, POSIX, PlatformLocal)
{
    AsyncServer server(test_context);
    unsigned short listen_port = server.init();

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] { server.run(); });
    client_thread.start([&] {
        AsyncClient<TEST_TYPE> client(listen_port, test_context);
        client.run();
    });
    client_thread.join();
    server_thread.join();
}

TEST_TYPES(EventLoop_DeadlineTimer, POSIX, PlatformLocal)
{
    GetEventLoop<TEST_TYPE> get_event_loop;
    EventLoop& event_loop = get_event_loop.loop;

    std::unique_ptr<DeadlineTimer> timer;

    // Check that the completion handler is executed
    bool completed = false;
    bool canceled = false;
    auto wait_handler = [&](std::error_code ec) {
        if (!ec)
            completed = true;
        if (ec == error::operation_aborted)
            canceled = true;
    };
    timer = event_loop.async_timer(std::chrono::seconds(0), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    event_loop.run();
    CHECK(completed);
    CHECK(!canceled);
    completed = false;

    // Check that an immediately completed wait operation can be canceled
    timer->async_wait(std::chrono::seconds(0), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    timer->cancel();
    CHECK(!completed);
    CHECK(!canceled);
    event_loop.run();
    CHECK(!completed);
    CHECK(canceled);
    canceled = false;

    // Check that a long running wait operation can be canceled
    timer->async_wait(std::chrono::hours(10000), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    timer->cancel();
    CHECK(!completed);
    CHECK(!canceled);
    event_loop.run();
    CHECK(!completed);
    CHECK(canceled);
}

TEST_TYPES(EventLoop_PostPropagatesExceptions, POSIX, PlatformLocal)
{
    // Check that throwing an exception propagates to the point of invocation
    // of the runloop.
    GetEventLoop<TEST_TYPE> service;
    service.loop.post([&]{ throw std::runtime_error(""); });
    CHECK_THROW(service.loop.run(), std::runtime_error);
}

