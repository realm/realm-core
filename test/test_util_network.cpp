#include "testsettings.hpp"
#ifdef TEST_UTIL_NETWORK

#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <memory>

#include <realm/util/memory_stream.hpp>
#include <realm/util/network.hpp>

#include "test.hpp"
#include "../test/util/thread_wrapper.hpp"

using std::size_t;
using std::string;
using std::ostringstream;
using namespace realm::util;
using namespace realm::test_util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


TEST(Network_Hostname)
{
    // Just check that we call call network::host_name()
    network::host_name();
}


namespace {

void post_handler(int* var, int value)
{
    *var = value;
}

} // anonymous namespace

TEST(Network_Post)
{
    int var = 381;
    network::io_service service;
    service.post([&]{ post_handler(&var, 824); });
    CHECK_EQUAL(var, 381);
    service.run();
    CHECK_EQUAL(var, 824);
}


namespace {

void handle_accept_1(std::error_code ec, bool* was_canceled)
{
    if (ec == error::operation_aborted)
        *was_canceled = true;
}

void handle_accept_2(std::error_code ec, bool* was_accepted)
{
    if (!ec)
        *was_accepted = true;
}

void handle_read_write(std::error_code ec, bool* was_canceled)
{
    if (ec == error::operation_aborted)
        *was_canceled = true;
}

} // anonymous namespace

TEST(Network_CancelAsyncAccept)
{
    network::io_service service;
    network::acceptor acceptor(service);
    acceptor.open(network::protocol::ip_v4());
    network::socket socket(service);
    bool accept_was_canceled = false;
    auto handler = [&](std::error_code ec) {
        handle_accept_1(ec, &accept_was_canceled);
    };
    acceptor.async_accept(socket, handler);
    acceptor.close();
    service.run();
    CHECK(accept_was_canceled);
}


TEST(Network_CancelAsyncReadWrite)
{
    network::io_service service;
    network::acceptor acceptor(service);
    acceptor.open(network::protocol::ip_v4());
    acceptor.listen();
    network::socket socket_1(service);
    bool was_accepted = false;
    auto accept_handler = [&](std::error_code ec) {
        handle_accept_2(ec, &was_accepted);
    };
    acceptor.async_accept(socket_1, accept_handler);
    network::socket socket_2(service);
    socket_2.connect(acceptor.local_endpoint());
    service.run();
    CHECK(was_accepted);
    const size_t size = 1;
    char data[size] = { 'a' };
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        handle_read_write(ec, &write_was_canceled);
    };
    async_write(socket_2, data, size, write_handler);
    network::buffered_input_stream input(socket_2);
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        handle_read_write(ec, &read_was_canceled);
    };
    input.async_read(buffer, size, read_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}


namespace {

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

void sync_server(network::acceptor* acceptor, unit_test::TestResults* test_results_ptr)
{
    unit_test::TestResults& test_results = *test_results_ptr;

    network::io_service& service = acceptor->service();
    network::socket socket(service);
    network::endpoint endpoint;
    acceptor->accept(socket, endpoint);

    network::buffered_input_stream input_stream(socket);
    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    size_t n = input_stream.read_until(header_buffer, max_header_size, '\n');
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n-1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer+(n-1));
    in.unsetf(std::ios_base::skipws);
    string message_type;
    in >> message_type;
    if (!CHECK_EQUAL(message_type, "echo"))
        return;
    char sp;
    size_t body_size;
    in >> sp >> body_size;
    if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
        return;
    std::unique_ptr<char[]> body_buffer(new char[body_size]);
    size_t m = input_stream.read(body_buffer.get(), body_size);
    if (!CHECK_EQUAL(m, body_size))
        return;
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer+max_header_size);
    out << "was " << body_size << '\n';
    write(socket, header_buffer, out.size());
    write(socket, body_buffer.get(), body_size);
}


void sync_client(unsigned short listen_port, unit_test::TestResults* test_results_ptr)
{
    unit_test::TestResults& test_results = *test_results_ptr;

    network::io_service service;
    network::socket socket(service);
    {
        ostringstream out;
        out << listen_port;
        string listen_port_2 = out.str();
        network::resolver resolver(service);
        network::resolver::query query("localhost", listen_port_2);
        network::endpoint::list endpoints;
        resolver.resolve(query, endpoints);
        typedef network::endpoint::list::iterator iter;
        iter i = endpoints.begin();
        iter end = endpoints.end();
        for (;;) {
            std::error_code ec;
            socket.open(i->protocol(), ec);
            if (!ec) {
                socket.connect(*i, ec);
                if (!ec)
                    break;
                socket.close();
            }
            if (++i == end)
                throw std::runtime_error("Could not connect to server: All endpoints failed");
        }
    }

    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer+max_header_size);
    out << "echo " << sizeof echo_body << '\n';
    write(socket, header_buffer, out.size());
    write(socket, echo_body, sizeof echo_body);

    network::buffered_input_stream input_stream(socket);
    size_t n = input_stream.read_until(header_buffer, max_header_size, '\n');
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n-1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer+(n-1));
    in.unsetf(std::ios_base::skipws);
    string message_type;
    in >> message_type;
    if (!CHECK_EQUAL(message_type, "was"))
        return;
    char sp;
    size_t echo_size;
    in >> sp >> echo_size;
    if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
        return;
    std::unique_ptr<char[]> echo_buffer(new char[echo_size]);
    size_t m = input_stream.read(echo_buffer.get(), echo_size);
    if (!CHECK_EQUAL(m, echo_size))
        return;
    if (!CHECK_EQUAL(echo_size, sizeof echo_body))
        return;
    CHECK(std::equal(echo_body, echo_body + sizeof echo_body, echo_buffer.get()));
}

} // anonymous namespace


TEST(Network_Sync)
{
    network::io_service service;
    network::resolver resolver(service);
    network::acceptor acceptor(service);

    network::resolver::query query("localhost", "",
                                   network::resolver::query::passive |
                                   network::resolver::query::address_configured);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);

    typedef network::endpoint::list::iterator iter;
    iter i = endpoints.begin();
    iter end = endpoints.end();
    for (;;) {
        std::error_code ec;
        acceptor.open(i->protocol(), ec);
        if (!ec) {
            acceptor.bind(*i, ec);
            if (!ec)
                break;
            acceptor.close();
        }
        if (++i == end)
            throw std::runtime_error("Could not create a listening socket: All endpoints failed");
    }

    network::endpoint listen_endpoint = acceptor.local_endpoint();
    unsigned short listen_port = listen_endpoint.port();

    acceptor.listen();

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] { sync_server(&acceptor,   &test_results); });
    client_thread.start([&] { sync_client(listen_port, &test_results); });
    client_thread.join();
    server_thread.join();
}


namespace {

class async_server {
public:
    async_server(unit_test::TestResults& test_results):
        m_acceptor(m_service),
        m_socket(m_service),
        m_input_stream(m_socket),
        m_test_results(test_results)
    {
    }

    unsigned short init()
    {
        network::resolver resolver(m_service);

        network::resolver::query query("localhost", "",
                                       network::resolver::query::passive |
                                       network::resolver::query::address_configured);
        network::endpoint::list endpoints;
        resolver.resolve(query, endpoints);

        typedef network::endpoint::list::iterator iter;
        iter i = endpoints.begin();
        iter end = endpoints.end();
        for (;;) {
            std::error_code ec;
            m_acceptor.open(i->protocol(), ec);
            if (!ec) {
                m_acceptor.bind(*i, ec);
                if (!ec)
                    break;
                m_acceptor.close();
            }
            if (++i == end)
                throw std::runtime_error("Could not create a listening socket: "
                                         "All endpoints failed");
        }

        network::endpoint listen_endpoint = m_acceptor.local_endpoint();
        unsigned short listen_port = listen_endpoint.port();

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
        string message_type;
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
        async_write(m_socket, m_header_buffer, out.size(), handler);
    }

    void handle_write_header(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_body(ec);
        };
        async_write(m_socket, m_body_buffer.get(), m_body_size, handler);
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


class async_client {
public:
    async_client(unsigned short listen_port, unit_test::TestResults& test_results):
        m_listen_port(listen_port),
        m_socket(m_service),
        m_input_stream(m_socket),
        m_test_results(test_results)
    {
    }

    void run()
    {
        string service;
        {
            ostringstream out;
            out << m_listen_port;
            service = out.str();
        }

        network::resolver resolver(m_service);
        network::resolver::query query("localhost", service);
        network::endpoint::list endpoints;
        resolver.resolve(query, endpoints);

        typedef network::endpoint::list::iterator iter;
        iter i = endpoints.begin();
        iter end = endpoints.end();
        for (;;) {
            std::error_code ec;
            m_socket.open(i->protocol(), ec);
            if (!ec) {
                m_socket.connect(*i, ec);
                if (!ec)
                    break;
                m_socket.close();
            }
            if (++i == end)
                throw std::runtime_error("Could not connect to server: All endpoints failed");
        }

        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer+s_max_header_size);
        out << "echo " << sizeof echo_body << '\n';
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_header(ec);
        };
        async_write(m_socket, m_header_buffer, out.size(), handler);

        m_service.run();

        m_socket.close();
    }

private:
    unsigned short m_listen_port;
    network::io_service m_service;
    network::socket m_socket;
    network::buffered_input_stream m_input_stream;
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
        async_write(m_socket, echo_body, sizeof echo_body, handler);
    }

    void handle_write_body(std::error_code ec)
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
        string message_type;
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
        m_input_stream.async_read(m_body_buffer.get(), m_body_size, handler);
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


TEST(Network_Async)
{
    async_server server(test_results);
    unsigned short listen_port = server.init();
    async_client client(listen_port, test_results);

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] { server.run(); });
    client_thread.start([&] { client.run(); });
    client_thread.join();
    server_thread.join();
}

#endif // TEST_UTIL_NETWORK
