#include "test.hpp"

#include <realm/util/features.h>
#include <realm/util/memory_stream.hpp>
#include <realm/util/network.hpp>
#include <realm/util/event_loop.hpp>

using namespace realm::util;
using namespace realm::test_util;

namespace {

template<class Implementation> struct MakeEventLoop;

struct Posix {};
struct AppleCoreFoundation {};

template<> struct MakeEventLoop<Posix> {
    std::unique_ptr<EventLoop> operator()() const
    {
        return EventLoop::Implementation::get_posix().make_event_loop();
    }
};

template<> struct MakeEventLoop<AppleCoreFoundation> {
    std::unique_ptr<EventLoop> operator()() const
    {
        return EventLoop::Implementation::get_apple_cf().make_event_loop();
    }
};


#if REALM_PLATFORM_APPLE
#  define IMPLEMENTATIONS Posix, AppleCoreFoundation
#else
#  define IMPLEMENTATIONS Posix
#endif


TEST_TYPES(EventLoop_Timer, IMPLEMENTATIONS)
{
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    bool ran = false;
    std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
    auto handler = [&](std::error_code ec) {
        CHECK(!ec);
        ran = true;
    };
    timer->async_wait(std::chrono::milliseconds(1), std::move(handler));
    CHECK(!ran);
    event_loop->run();
    CHECK(ran);
}


TEST_TYPES(EventLoop_DeadlineTimer, IMPLEMENTATIONS)
{
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    // Check that the completion handler is executed
    std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
    bool completed = false;
    bool canceled = false;
    auto wait_handler = [&](std::error_code ec) {
        if (!ec)
            completed = true;
        if (ec == error::operation_aborted)
            canceled = true;
    };
    timer->async_wait(std::chrono::seconds(0), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    event_loop->run();
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
    event_loop->run();
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
    event_loop->run();
    CHECK(!completed);
    CHECK(canceled);
}


TEST_TYPES(EventLoop_PostPropagatesExceptions, IMPLEMENTATIONS)
{
    // Check that throwing an exception propagates to the point of invocation
    // of the runloop.
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    struct TestException: std::exception {};
    event_loop->post([]{ throw TestException(); });
    CHECK_THROW(event_loop->run(), TestException);
}


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


class AsyncClient {
public:
    AsyncClient(EventLoop& event_loop, unsigned short listen_port, unit_test::TestContext& test_context):
        m_event_loop(event_loop),
        m_socket(event_loop.make_socket()),
        m_listen_port(listen_port),
        m_test_context(test_context)
    {
    }

    void run()
    {
        auto handler = [=](std::error_code ec) {
            handle_connect(ec);
        };
        m_socket->async_connect("localhost", m_listen_port, SocketSecurity::None, handler);

        m_event_loop.run();

        m_socket->close();
    }

private:
    EventLoop& m_event_loop;
    std::unique_ptr<Socket> m_socket;
    unsigned short m_listen_port;

    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestContext& m_test_context;

    void handle_connect(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer+s_max_header_size);
        out << "echo " << sizeof echo_body << '\n';
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_header(ec);
        };
        m_socket->async_write(m_header_buffer, out.size(), handler);
    }

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


TEST_TYPES(EventLoop_AsyncCommunication, IMPLEMENTATIONS)
{
    AsyncServer server(test_context);
    unsigned short listen_port = server.init();

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    AsyncClient client(*event_loop, listen_port, test_context);

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] { server.run(); });
    client_thread.start([&] { client.run(); });
    CHECK_NOT(client_thread.join());
    CHECK_NOT(server_thread.join());
}

} // unnamed namespace
