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

void connect_socket(network::socket& socket, std::string port)
{
    network::io_service& service = socket.service();
    network::resolver resolver(service);
    network::resolver::query query("localhost", port);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);

    auto i = endpoints.begin();
    auto end = endpoints.end();
    for (;;) {
        std::error_code ec;
        socket.connect(*i, ec);
        if (!ec)
            break;
        socket.close();
        if (++i == end)
            throw std::runtime_error("Failed to connect to localhost:"+port);
    }
}

class bowl_of_stones_semaphore {
public:
    bowl_of_stones_semaphore(int initial_number_of_stones = 0):
        m_num_stones(initial_number_of_stones)
    {
    }
    void get_stone()
    {
        LockGuard lock(m_mutex);
        while (m_num_stones == 0)
            m_cond_var.wait(lock);
        --m_num_stones;
    }
    void add_stone()
    {
        LockGuard lock(m_mutex);
        ++m_num_stones;
        m_cond_var.notify();
    }
private:
    Mutex m_mutex;
    int m_num_stones;
    CondVar m_cond_var;
};

} // anonymous namespace


TEST(Network_Hostname)
{
    // Just check that we call call network::host_name()
    network::host_name();
}


TEST(Network_PostOperation)
{
    network::io_service service;
    int var_1 = 381, var_2 = 743;
    service.post([&] { var_1 = 824; });
    service.post([&] { var_2 = 216; });
    CHECK_EQUAL(var_1, 381);
    CHECK_EQUAL(var_2, 743);
    service.run();
    CHECK_EQUAL(var_1, 824);
    CHECK_EQUAL(var_2, 216);
    service.post([&] { var_2 = 191; });
    service.post([&] { var_1 = 476; });
    CHECK_EQUAL(var_1, 824);
    CHECK_EQUAL(var_2, 216);
    service.run();
    CHECK_EQUAL(var_1, 476);
    CHECK_EQUAL(var_2, 191);
}


TEST(Network_EventLoopStopAndReset_1)
{
    network::io_service service;

    // Prestop
    int var = 381;
    service.stop();
    service.post([&]{ var = 824; });
    service.run(); // Must return immediately
    CHECK_EQUAL(var, 381);
    service.run(); // Must still return immediately
    CHECK_EQUAL(var, 381);

    // Reset
    service.reset();
    service.post([&]{ var = 824; });
    CHECK_EQUAL(var, 381);
    service.run();
    CHECK_EQUAL(var, 824);
    service.post([&]{ var = 476; });
    CHECK_EQUAL(var, 824);
    service.run();
    CHECK_EQUAL(var, 476);
}


TEST(Network_EventLoopStopAndReset_2)
{
    // Introduce a blocking operation that will keep the event loop running
    network::io_service service;
    network::acceptor acceptor(service);
    bind_acceptor(acceptor);
    acceptor.listen();
    network::socket socket(service);
    acceptor.async_accept(socket, [](std::error_code) {});

    // Start event loop execution in the background
    ThreadWrapper thread_1;
    thread_1.start([&]() { service.run(); });

    // Check that the event loop is actually running
    bowl_of_stones_semaphore bowl_1; // Empty
    service.post([&]() { bowl_1.add_stone(); });
    bowl_1.get_stone(); // Block until the stone is added

    // Stop the event loop
    service.stop();
    CHECK_NOT(thread_1.join());

    // Check that the event loop remains in the stopped state
    int var = 381;
    service.post([&]() { var = 824; });
    CHECK_EQUAL(var, 381);
    service.run(); // Still stopped, so run() must return immediately
    CHECK_EQUAL(var, 381);

    // Put the event loop back into the unstopped state, and restart it in the
    // background
    service.reset();
    ThreadWrapper thread_2;
    thread_2.start([&]() { service.run(); });

    // Check that the event loop is actually running
    bowl_of_stones_semaphore bowl_2; // Empty
    service.post([&]() { bowl_2.add_stone(); });
    bowl_2.get_stone(); // Block until the stone is added

    // Stop the event loop by canceling the blocking operation
    service.post([&]() { acceptor.cancel(); });
    CHECK_NOT(thread_2.join());

    CHECK_EQUAL(var, 824);
}


TEST(Network_GetSetSocketOption)
{
    network::io_service service;
    network::socket socket(service);
    socket.open(network::protocol::ip_v4());
    network::socket::reuse_address opt_reuse_addr;
    socket.get_option(opt_reuse_addr);
    CHECK_NOT(opt_reuse_addr.value());
    socket.set_option(network::socket::reuse_address(true));
    socket.get_option(opt_reuse_addr);
    CHECK(opt_reuse_addr.value());
}


TEST(Network_AsyncConnectAndAsyncAccept)
{
    network::io_service service;
    network::acceptor acceptor(service);
    network::endpoint listening_endpoint = bind_acceptor(acceptor);
    acceptor.listen();
    network::socket socket_1(service), socket_2(service);
    bool connected = false;
    auto connect_handler = [&](std::error_code ec) {
        if (ec)
            throw std::system_error(ec);
        connected = true;
    };
    bool accepted = false;
    auto accept_handler = [&](std::error_code ec) {
        if (ec)
            throw std::system_error(ec);
        accepted = true;
    };
    socket_1.async_connect(listening_endpoint, connect_handler);
    acceptor.async_accept(socket_2, accept_handler);
    service.run();
    CHECK(connected);
}


TEST(Network_ReadWrite)
{
    network::io_service service_1;
    network::acceptor acceptor(service_1);
    network::endpoint listening_endpoint = bind_acceptor(acceptor);
    acceptor.listen();

    char data[] = { 'X', 'F', 'M' };

    auto reader = [&] {
        network::socket socket_1(service_1);
        acceptor.accept(socket_1);
        network::buffered_input_stream input(socket_1);
        char buffer[sizeof data];
        size_t n = input.read(buffer, sizeof data);
        if (CHECK_EQUAL(sizeof data, n))
            CHECK(std::equal(buffer, buffer+n, data));
        std::error_code ec;
        n = input.read(buffer, 1, ec);
        CHECK_EQUAL(0, n);
        CHECK(ec == network::end_of_input);
    };
    ThreadWrapper thread;
    thread.start(reader);

    network::io_service service_2;
    network::socket socket_2(service_2);
    socket_2.connect(listening_endpoint);
    socket_2.write(data, sizeof data);
    socket_2.close();

    CHECK_NOT(thread.join());
}


TEST(Network_SocketAndAcceptorOpen)
{
    network::io_service service_1;
    network::acceptor acceptor(service_1);
    network::resolver resolver(service_1);
    network::resolver::query query("localhost", "",
                                   network::resolver::query::passive |
                                   network::resolver::query::address_configured);
    network::endpoint::list endpoints;
    resolver.resolve(query, endpoints);
    {
        auto i = endpoints.begin();
        auto end = endpoints.end();
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
                throw std::runtime_error("Failed to bind to localhost:*");
        }
    }
    network::endpoint listening_endpoint = acceptor.local_endpoint();
    acceptor.listen();
    network::socket socket_1(service_1);
    ThreadWrapper thread;
    thread.start([&] { acceptor.accept(socket_1); });

    network::io_service service_2;
    network::socket socket_2(service_2);
    socket_2.open(listening_endpoint.protocol());
    socket_2.connect(listening_endpoint);

    thread.join();
}


TEST(Network_CancelAsyncAccept)
{
    network::io_service service;
    network::acceptor acceptor(service);
    acceptor.open(network::protocol::ip_v4());
    network::socket socket(service);

    bool accept_was_canceled = false;
    auto handler = [&](std::error_code ec) {
        if (ec == error::operation_aborted)
            accept_was_canceled = true;
    };
    acceptor.async_accept(socket, handler);
    acceptor.cancel();
    service.run();
    CHECK(accept_was_canceled);

    accept_was_canceled = false;
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
        if (!ec)
            was_accepted = true;
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
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2.async_write(data, size, write_handler);
    network::buffered_input_stream input(socket_2);
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    input.async_read(buffer, size, read_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}


TEST(Network_CancelEmptyRead)
{
    // Make sure that an immediately completable read operation is still
    // cancelable

    network::io_service service;
    network::acceptor acceptor(service);
    acceptor.open(network::protocol::ip_v4());
    acceptor.listen();
    network::socket socket_1(service);
    bool was_accepted = false;
    auto accept_handler = [&](std::error_code ec) {
        if (!ec)
            was_accepted = true;
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
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2.async_write(data, size, write_handler);
    network::buffered_input_stream input(socket_2);
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    input.async_read(buffer, 0, read_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}


TEST(Network_AcceptorMixedAsyncSync)
{
    network::io_service service;
    network::acceptor acceptor(service);
    acceptor.open(network::protocol::ip_v4());
    acceptor.listen();
    network::endpoint ep = acceptor.local_endpoint();
    auto connect = [ep] {
        network::io_service service;
        network::socket socket(service);
        socket.connect(ep);
    };

    // Synchronous accept -> stay on blocking mode
    {
        ThreadWrapper thread;
        thread.start(connect);
        network::socket socket(service);
        acceptor.accept(socket);
        CHECK_NOT(thread.join());
    }

    // Asynchronous accept -> switch to nonblocking mode
    {
        ThreadWrapper thread;
        thread.start(connect);
        network::socket socket(service);
        bool was_accepted = false;
        auto accept_handler = [&](std::error_code ec) {
            if (!ec)
                was_accepted = true;
        };
        acceptor.async_accept(socket, accept_handler);
        service.run();
        CHECK(was_accepted);
        CHECK_NOT(thread.join());
    }

    // Synchronous accept -> switch back to blocking mode
    {
        ThreadWrapper thread;
        thread.start(connect);
        network::socket socket(service);
        acceptor.accept(socket);
        CHECK_NOT(thread.join());
    }
}


TEST(Network_SocketMixedAsyncSync)
{
    network::io_service acceptor_service;
    network::acceptor acceptor(acceptor_service);
    acceptor.open(network::protocol::ip_v4());
    acceptor.listen();
    network::endpoint ep = acceptor.local_endpoint();
    auto accept_and_echo = [&] {
        network::socket socket(acceptor_service);
        acceptor.accept(socket);
        network::buffered_input_stream in(socket);
        size_t buffer_size = 1024;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        size_t size = in.read_until(buffer.get(), buffer_size, '\n');
        socket.write(buffer.get(), size);
    };

    {
        ThreadWrapper thread;
        thread.start(accept_and_echo);
        network::io_service service;

        // Synchronous connect -> stay in blocking mode
        network::socket socket(service);
        socket.connect(ep);
        network::buffered_input_stream in(socket);

        // Asynchronous write -> switch to nonblocking mode
        const char* message = "Calabi–Yau\n";
        bool was_written = false;
        auto write_handler = [&](std::error_code ec, size_t) {
            if (!ec)
                was_written = true;
        };
        socket.async_write(message, strlen(message), write_handler);
        service.run();
        CHECK(was_written);

        // Synchronous read -> switch back to blocking mode
        size_t buffer_size = 1024;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        std::error_code ec;
        size_t size = in.read(buffer.get(), buffer_size, ec);
        if (CHECK_EQUAL(ec, network::end_of_input)) {
            if (CHECK_EQUAL(size, strlen(message)))
                CHECK(std::equal(buffer.get(), buffer.get()+size, message));
        }

        CHECK_NOT(thread.join());
    }

    {
        ThreadWrapper thread;
        thread.start(accept_and_echo);
        network::io_service service;

        // Asynchronous connect -> switch to nonblocking mode
        network::socket socket(service);
        bool is_connected = false;
        auto connect_handler = [&](std::error_code ec) {
            if (!ec)
                is_connected = true;
        };
        socket.async_connect(ep, connect_handler);
        service.run();
        CHECK(is_connected);
        network::buffered_input_stream in(socket);

        // Synchronous write -> switch back to blocking mode
        const char* message = "The Verlinde Algebra And The Cohomology Of The Grassmannian\n";
        socket.write(message, strlen(message));

        // Asynchronous read -> swich once again to nonblocking mode
        size_t buffer_size = 1024;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        auto read_handler = [&](std::error_code ec, size_t size) {
            if (CHECK_EQUAL(ec, network::end_of_input)) {
                if (CHECK_EQUAL(size, strlen(message)))
                    CHECK(std::equal(buffer.get(), buffer.get()+size, message));
            }
        };
        in.async_read(buffer.get(), buffer_size, read_handler);
        service.run();

        CHECK_NOT(thread.join());
    }
}


TEST(Network_DeadlineTimer)
{
    network::io_service service;
    network::deadline_timer timer(service);

    // Check that the completion handler is executed
    bool completed = false;
    bool canceled = false;
    auto wait_handler = [&](std::error_code ec) {
        if (!ec)
            completed = true;
        if (ec == error::operation_aborted)
            canceled = true;
    };
    timer.async_wait(std::chrono::seconds(0), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    service.run();
    CHECK(completed);
    CHECK(!canceled);
    completed = false;

    // Check that an immediately completed wait operation can be canceled
    timer.async_wait(std::chrono::seconds(0), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    timer.cancel();
    CHECK(!completed);
    CHECK(!canceled);
    service.run();
    CHECK(!completed);
    CHECK(canceled);
    canceled = false;

    // Check that a long running wait operation can be canceled
    timer.async_wait(std::chrono::hours(10000), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    timer.cancel();
    CHECK(!completed);
    CHECK(!canceled);
    service.run();
    CHECK(!completed);
    CHECK(canceled);
}


/*
TEST(Network_DeadlineTimer_Special)
{
    network::io_service service;
    network::deadline_timer timer_1(service);
    network::deadline_timer timer_2(service);
    network::deadline_timer timer_3(service);
    network::deadline_timer timer_4(service);
    network::deadline_timer timer_5(service);
    network::deadline_timer timer_6(service);
    using namespace std;
    timer_1.async_wait(chrono::seconds(3), [](error_code) { cerr << "*3*\n";   });
    timer_2.async_wait(chrono::seconds(2), [](error_code) { cerr << "*2*\n";   });
    timer_3.async_wait(chrono::seconds(3), [](error_code) { cerr << "*3-2*\n"; });
    timer_4.async_wait(chrono::seconds(2), [](error_code) { cerr << "*2-2*\n"; });
    timer_5.async_wait(chrono::seconds(1), [](error_code) { cerr << "*1*\n";   });
    timer_6.async_wait(chrono::seconds(2), [](error_code) { cerr << "*2-3*\n"; });
    service.run();
}
*/


TEST(Network_HandlerDealloc)
{
    // Check that dynamically allocated handlers are properly freed when the
    // service object is destroyed.
    {
        // m_post_handlers
        network::io_service service;
        service.post([] {});
    }
    {
        // m_imm_handlers
        network::io_service service;
        // By adding two post handlers that throw, one is going to be left
        // behind in `m_imm_handlers`
        service.post([&]{ throw std::runtime_error(""); });
        service.post([&]{ throw std::runtime_error(""); });
        CHECK_THROW(service.run(), std::runtime_error);
    }
    {
        // m_poll_handlers
        network::io_service service;
        network::acceptor acceptor(service);
        acceptor.open(network::protocol::ip_v4());
        network::socket socket(service);
        // This leaves behind a read handler in m_poll_handlers
        acceptor.async_accept(socket, [&](std::error_code) {});
    }
    {
        // m_cancel_handlers
        network::io_service service;
        network::acceptor acceptor(service);
        acceptor.open(network::protocol::ip_v4());
        network::socket socket(service);
        acceptor.async_accept(socket, [&](std::error_code) {});
        // This leaves behind a read handler in m_cancel_handlers
        acceptor.close();
    }
    {
        // m_poll_handlers
        network::io_service service_1;
        network::acceptor acceptor(service_1);
        network::endpoint listening_endpoint = bind_acceptor(acceptor);
        acceptor.listen();
        network::socket socket_1(service_1);
        ThreadWrapper thread;
        thread.start([&] { acceptor.accept(socket_1); });
        network::io_service service_2;
        network::socket socket_2(service_2);
        socket_2.connect(listening_endpoint);
        thread.join();
        network::buffered_input_stream input(socket_1);
        char buffer[1];
        char data[] = { 'X', 'F', 'M' };
        // This leaves behind both a read and a write handler in m_poll_handlers
        input.async_read(buffer, sizeof buffer, [](std::error_code, size_t) {});
        socket_1.async_write(data, sizeof data, [](std::error_code, size_t) {});
    }
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
    std::string message_type;
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
    socket.write(header_buffer, out.size());
    socket.write(body_buffer.get(), body_size);
}


void sync_client(unsigned short listen_port, unit_test::TestResults* test_results_ptr)
{
    unit_test::TestResults& test_results = *test_results_ptr;

    network::io_service service;
    network::socket socket(service);
    {
        std::ostringstream out;
        out << listen_port;
        std::string listen_port_2 = out.str();
        connect_socket(socket, listen_port_2);
    }

    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer+max_header_size);
    out << "echo " << sizeof echo_body << '\n';
    socket.write(header_buffer, out.size());
    socket.write(echo_body, sizeof echo_body);

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
    std::string message_type;
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
    network::acceptor acceptor(service);
    network::endpoint listen_endpoint = bind_acceptor(acceptor);
    network::endpoint::port_type listen_port = listen_endpoint.port();
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
        std::string service;
        {
            std::ostringstream out;
            out << m_listen_port;
            service = out.str();
        }
        connect_socket(m_socket, service);

        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer+s_max_header_size);
        out << "echo " << sizeof echo_body << '\n';
        auto handler = [=](std::error_code ec, size_t) {
            handle_write_header(ec);
        };
        m_socket.async_write(m_header_buffer, out.size(), handler);

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
        m_socket.async_write(echo_body, sizeof echo_body, handler);
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
