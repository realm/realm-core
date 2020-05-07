#include <thread>

#include <realm/util/network_ssl.hpp>

#include "test.hpp"
#include "util/semaphore.hpp"

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

network::Endpoint bind_acceptor(network::Acceptor& acceptor)
{
    network::Endpoint ep; // Wildcard
    acceptor.open(ep.protocol());
    acceptor.bind(ep);
    ep = acceptor.local_endpoint(); // Get actual bound endpoint
    acceptor.listen();
    return ep;
}

void connect_sockets(network::Socket& socket_1, network::Socket& socket_2)
{
    network::Service& service_1 = socket_1.get_service();
    network::Service& service_2 = socket_2.get_service();
    network::Acceptor acceptor(service_1);
    network::Endpoint ep = bind_acceptor(acceptor);
    bool accept_occurred = false, connect_occurred = false;
    auto accept_handler = [&](std::error_code ec) {
        REALM_ASSERT(!ec);
        accept_occurred = true;
    };
    auto connect_handler = [&](std::error_code ec) {
        REALM_ASSERT(!ec);
        connect_occurred = true;
    };
    acceptor.async_accept(socket_1, std::move(accept_handler));
    socket_2.async_connect(ep, std::move(connect_handler));
    if (&service_1 == &service_2) {
        service_1.run();
    }
    else {
        std::thread thread{[&] {
            service_1.run();
        }};
        service_2.run();
        thread.join();
    }
    REALM_ASSERT(accept_occurred);
    REALM_ASSERT(connect_occurred);
}

void configure_server_ssl_context_for_test(network::ssl::Context& ssl_context)
{
    ssl_context.use_certificate_chain_file(get_test_resource_path() + "test_util_network_ssl_ca.pem");
    ssl_context.use_private_key_file(get_test_resource_path() + "test_util_network_ssl_key.pem");
}

void connect_ssl_streams(network::ssl::Stream& server_stream, network::ssl::Stream& client_stream)
{
    network::Socket& server_socket = server_stream.lowest_layer();
    network::Socket& client_socket = client_stream.lowest_layer();
    connect_sockets(server_socket, client_socket);
    network::Service& server_service = server_socket.get_service();
    network::Service& client_service = client_socket.get_service();
    bool server_handshake_occurred = false, client_handshake_occurred = false;
    auto server_handshake_handler = [&](std::error_code ec) {
        REALM_ASSERT(!ec);
        server_handshake_occurred = true;
    };
    auto client_handshake_handler = [&](std::error_code ec) {
        REALM_ASSERT(!ec);
        client_handshake_occurred = true;
    };
    server_stream.async_handshake(std::move(server_handshake_handler));
    client_stream.async_handshake(std::move(client_handshake_handler));
    if (&server_service == &client_service) {
        server_service.run();
    }
    else {
        std::thread thread{[&] {
            server_service.run();
        }};
        client_service.run();
        thread.join();
    }
    REALM_ASSERT(server_handshake_occurred);
    REALM_ASSERT(client_handshake_occurred);
}


class PingPongDelayFixture {
public:
    PingPongDelayFixture(network::Service& service)
        : PingPongDelayFixture{service, service}
    {
    }

    PingPongDelayFixture(network::Service& server_service, network::Service& client_service)
        : m_server_socket{server_service}
        , m_client_socket{client_service}
    {
        connect_sockets(m_server_socket, m_client_socket);
    }

    // Must be called by thread associated with `server_service`
    void start_server()
    {
        initiate_server_read();
    }

    // Must be called by thread associated with `server_service`
    void stop_server()
    {
        m_server_socket.cancel();
    }

    // Must be called by thread associated with `client_service`
    void delay_client(std::function<void()> handler, int n = 512)
    {
        m_handler = std::move(handler);
        m_num = n;
        initiate_client_write();
    }

private:
    network::Socket m_server_socket, m_client_socket;
    char m_server_char = 0, m_client_char = 0;
    int m_num;
    std::function<void()> m_handler;

    void initiate_server_read()
    {
        auto handler = [this](std::error_code ec, size_t) {
            if (ec != error::operation_aborted)
                handle_server_read(ec);
        };
        m_server_socket.async_read(&m_server_char, 1, std::move(handler));
    }

    void handle_server_read(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        initiate_server_write();
    }

    void initiate_server_write()
    {
        auto handler = [this](std::error_code ec, size_t) {
            if (ec != error::operation_aborted)
                handle_server_write(ec);
        };
        m_server_socket.async_write(&m_server_char, 1, std::move(handler));
    }

    void handle_server_write(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        initiate_server_read();
    }

    void initiate_client_write()
    {
        if (m_num <= 0) {
            std::function<void()> handler = std::move(m_handler);
            m_handler = std::function<void()>();
            handler();
            return;
        }
        --m_num;

        auto handler = [this](std::error_code ec, size_t) {
            if (ec != error::operation_aborted)
                handle_client_write(ec);
        };
        m_client_socket.async_write(&m_client_char, 1, std::move(handler));
    }

    void handle_client_write(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        initiate_client_read();
    }

    void initiate_client_read()
    {
        auto handler = [this](std::error_code ec, size_t) {
            if (ec != error::operation_aborted)
                handle_client_read(ec);
        };
        m_client_socket.async_read(&m_client_char, 1, std::move(handler));
    }

    void handle_client_read(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        initiate_client_write();
    }
};

} // unnamed namespace


TEST(Util_Network_SSL_Handshake)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}


TEST(Util_Network_SSL_AsyncHandshake)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_sockets(socket_1, socket_2);

    bool connect_completed = false;
    auto connect_handler = [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        connect_completed = true;
    };
    bool accept_completed = false;
    auto accept_handler = [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        accept_completed = true;
    };

    ssl_stream_1.async_handshake(std::move(accept_handler));
    ssl_stream_2.async_handshake(std::move(connect_handler));
    service.run();
    CHECK(connect_completed);
    CHECK(accept_completed);
}


TEST(Util_Network_SSL_ReadWriteShutdown)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    const char* message = "hello";
    char buffer[256];

    auto writer = [&] {
        std::size_t n = ssl_stream_1.write(message, std::strlen(message));
        CHECK_EQUAL(std::strlen(message), n);
        ssl_stream_1.shutdown();
    };
    auto reader = [&] {
        std::error_code ec;
        std::size_t n = ssl_stream_2.read(buffer, sizeof buffer, ec);
        if (CHECK_EQUAL(MiscExtErrors::end_of_input, ec)) {
            if (CHECK_EQUAL(std::strlen(message), n))
                CHECK(std::equal(buffer, buffer + n, message));
        }
    };

    std::thread thread_1(std::move(writer));
    std::thread thread_2(std::move(reader));
    thread_1.join();
    thread_2.join();
}


TEST(Util_Network_SSL_AsyncReadWriteShutdown)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    const char* message = "hello";
    char buffer[256];

    bool shutdown_completed = false;
    auto shutdown_handler = [&](std::error_code ec) {
        CHECK_EQUAL(std::error_code(), ec);
        shutdown_completed = true;
    };
    auto write_handler = [&](std::error_code ec, std::size_t n) {
        CHECK_EQUAL(std::error_code(), ec);
        CHECK_EQUAL(std::strlen(message), n);
        ssl_stream_1.async_shutdown(std::move(shutdown_handler));
    };
    bool read_completed = false;
    auto read_handler = [&](std::error_code ec, std::size_t n) {
        CHECK_EQUAL(MiscExtErrors::end_of_input, ec);
        if (CHECK_EQUAL(std::strlen(message), n))
            CHECK(std::equal(buffer, buffer + n, message));
        read_completed = true;
    };

    ssl_stream_1.async_write(message, std::strlen(message), std::move(write_handler));
    ssl_stream_2.async_read(buffer, sizeof buffer, std::move(read_handler));
    service.run();
    CHECK(shutdown_completed);
    CHECK(read_completed);
}


TEST(Util_Network_SSL_PrematureEndOfInputOnHandshakeRead)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_sockets(socket_1, socket_2);

    socket_1.shutdown(network::Socket::shutdown_send);

    // Use a separate thread to consume data written by Stream::handshake(),
    // such that we can be sure not to block.
    auto consumer = [&] {
        constexpr std::size_t size = 4096;
        std::unique_ptr<char[]> buffer(new char[size]);
        std::error_code ec;
        do {
            socket_1.read_some(buffer.get(), size, ec);
        } while (!ec);
        REALM_ASSERT(ec == MiscExtErrors::end_of_input);
    };

    std::thread thread(std::move(consumer));

#if REALM_HAVE_OPENSSL
    CHECK_SYSTEM_ERROR(ssl_stream_2.handshake(), MiscExtErrors::premature_end_of_input);
#elif REALM_HAVE_SECURE_TRANSPORT
    // We replace the CHECK_SYSTEM_ERROR check for "premature end of input"
    // with a check for any error code, Mac OS occasionally reports another
    // system error. We can revisit the details of the error code later. The
    // detailed check is disabled for now to reduce the number of failed unit
    // test runs.
    CHECK_THROW(ssl_stream_2.handshake(), std::system_error);
#endif

    socket_2.close();
    thread.join();
}


#ifndef _WIN32 // FIXME: winsock doesn't have EPIPE, what's the equivalent?
TEST(Util_Network_SSL_BrokenPipeOnHandshakeWrite)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_sockets(socket_1, socket_2);

    socket_1.close();

    // Fill the kernel level write buffer, to provoke error::broken_pipe.
    constexpr std::size_t size = 4096;
    std::unique_ptr<char[]> buffer(new char[size]);
    std::fill(buffer.get(), buffer.get() + size, 0);
    std::error_code ec;
    do {
        socket_2.write_some(buffer.get(), size, ec);
    } while (!ec);
    REALM_ASSERT(ec == error::broken_pipe);

    CHECK_SYSTEM_ERROR(ssl_stream_2.handshake(), error::broken_pipe);
}
#endif


TEST(Util_Network_SSL_EndOfInputOnRead)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);
    ssl_stream_2.shutdown();
    socket_2.shutdown(network::Socket::shutdown_send);
    char ch;
    CHECK_SYSTEM_ERROR(ssl_stream_1.read_some(&ch, 1), MiscExtErrors::end_of_input);
}


TEST(Util_Network_SSL_PrematureEndOfInputOnRead)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    socket_2.shutdown(network::Socket::shutdown_send);
    char ch;
    CHECK_SYSTEM_ERROR(ssl_stream_1.read_some(&ch, 1), MiscExtErrors::premature_end_of_input);
}


#ifndef _WIN32 // FIXME: winsock doesn't have EPIPE, what's the equivalent?
TEST(Util_Network_SSL_BrokenPipeOnWrite)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    socket_1.close();

    // Fill the kernel level write buffer, to provoke error::broken_pipe.
    constexpr std::size_t size = 4096;
    std::unique_ptr<char[]> buffer(new char[size]);
    std::fill(buffer.get(), buffer.get() + size, 0);
    std::error_code ec;
    do {
        socket_2.write_some(buffer.get(), size, ec);
    } while (!ec);
    REALM_ASSERT(ec == error::broken_pipe);

    char ch = 0;
    CHECK_SYSTEM_ERROR(ssl_stream_2.write(&ch, 1), error::broken_pipe);
}


TEST(Util_Network_SSL_BrokenPipeOnShutdown)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    socket_1.close();

    // Fill the kernel level write buffer, to provoke error::broken_pipe.
    constexpr std::size_t size = 4096;
    std::unique_ptr<char[]> buffer(new char[size]);
    std::fill(buffer.get(), buffer.get() + size, 0);
    std::error_code ec;
    do {
        socket_2.write_some(buffer.get(), size, ec);
    } while (!ec);
    REALM_ASSERT(ec == error::broken_pipe);

    CHECK_SYSTEM_ERROR(ssl_stream_2.shutdown(), error::broken_pipe);
}
#endif


TEST(Util_Network_SSL_ShutdownBeforeCloseNotifyReceived)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    // Shut down peer 1's writing side before it has received a shutdown alert
    // from peer 2.
    ssl_stream_1.shutdown();
}


TEST(Util_Network_SSL_ShutdownAfterCloseNotifyReceived)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    // Make sure peer 2 gets an SSL shutdown alert.
    ssl_stream_1.shutdown();
    socket_1.shutdown(network::Socket::shutdown_send);

    // Make sure peer 2 received the shutdown alert from peer 1 before peer 2
    // writes.
    char ch;
    CHECK_SYSTEM_ERROR(ssl_stream_2.read_some(&ch, 1), MiscExtErrors::end_of_input);

    // Check that peer 2 can stil permform a shutdown operation.
    ssl_stream_2.shutdown();
}


TEST(Util_Network_SSL_WriteAfterCloseNotifyReceived)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    // Shut down peer 1's writing side, such that peer 2 gets an SSL shutdown
    // alert.
    ssl_stream_1.shutdown();
    socket_1.shutdown(network::Socket::shutdown_send);

    // Make sure peer 2 received the shutdown alert from peer 1 before peer 2
    // writes.
    char ch;
    CHECK_SYSTEM_ERROR(ssl_stream_2.read_some(&ch, 1), MiscExtErrors::end_of_input);

    // Make peer 2 Write a message, which must fail....????
    const char* message = "hello";
    CHECK_SYSTEM_ERROR(ssl_stream_2.write(message, std::strlen(message)), error::broken_pipe);
}

TEST(Util_Network_SSL_BasicSendAndReceive)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    // Make peer 2 Write a message.
    const char* message = "hello";
    ssl_stream_2.write(message, std::strlen(message));
    ssl_stream_2.shutdown();
    socket_2.shutdown(network::Socket::shutdown_send);

    // Check that peer 1 received the message correctly.
    char buffer[256];
    std::error_code ec;
    std::size_t n = ssl_stream_1.read(buffer, sizeof buffer, ec);
    CHECK_EQUAL(MiscExtErrors::end_of_input, ec);
    if (CHECK_EQUAL(std::strlen(message), n))
        CHECK(std::equal(buffer, buffer + n, message));
}


TEST(Util_Network_SSL_StressTest)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;
    configure_server_ssl_context_for_test(ssl_context_1);
    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);
    connect_ssl_streams(ssl_stream_1, ssl_stream_2);

    constexpr size_t original_size = 0x100000; // 1MiB
    std::unique_ptr<char[]> original_1, original_2;
    original_1.reset(new char[original_size]);
    original_2.reset(new char[original_size]);
    {
        std::mt19937_64 prng{std::random_device()()};
        using lim = std::numeric_limits<char>;
        std::uniform_int_distribution<short> dist{short(lim::min()), short(lim::max())};
        log("Initializing...");
        for (size_t i = 0; i < original_size; ++i)
            original_1[i] = char(dist(prng));
        for (size_t i = 0; i < original_size; ++i)
            original_2[i] = char(dist(prng));
        log("Initialized");
    }

    struct Stats {
        std::uint_fast64_t num_cancellations = 0;
        std::uint_fast64_t num_reads = 0, num_canceled_reads = 0;
        std::uint_fast64_t num_writes = 0, num_canceled_writes = 0;
    };

#ifdef _WIN32
    // With 512, it would take 9 minutes in 32-bit Debug mode on Windows
    constexpr int num_cycles = 32;
#else
    constexpr int num_cycles = 512;
#endif
    auto thread = [&](int id, network::ssl::Stream& ssl_stream, const char* read_original, const char* write_original,
                      Stats& stats) {
        std::unique_ptr<char[]> read_buffer{new char[original_size]};
        std::mt19937_64 prng{std::random_device()()};
        // Using range 1B -> 32KiB because that undershoots and overshoots in
        // equal amounts with respect to the SSL frame size of 16KiB.
        std::uniform_int_distribution<size_t> read_write_size_dist(1, 32 * 1024);
        std::uniform_int_distribution<int> delayed_read_write_dist(0, 49);
        network::Service& service = ssl_stream.lowest_layer().get_service();
        network::DeadlineTimer cancellation_timer{service};
        network::DeadlineTimer read_timer{service};
        network::DeadlineTimer write_timer{service};
        bool read_done = false, write_done = false;
        std::function<void()> shedule_cancellation = [&] {
            auto handler = [&](std::error_code ec) {
                REALM_ASSERT(!ec || ec == error::operation_aborted);
                if (ec == error::operation_aborted)
                    return;
                if (read_done && write_done)
                    return;
                ssl_stream.lowest_layer().cancel();
                ++stats.num_cancellations;
                shedule_cancellation();
            };
            cancellation_timer.async_wait(std::chrono::microseconds(10), std::move(handler));
        };
        //        shedule_cancellation();
        char* read_begin = read_buffer.get();
        char* read_end = read_buffer.get() + original_size;
        int num_read_cycles = 0;
        std::function<void()> read = [&] {
            if (read_begin == read_end) {
                log("<R%1>", id);
                CHECK(std::equal(read_original, read_original + original_size, read_buffer.get()));
                ++num_read_cycles;
                if (num_read_cycles == num_cycles) {
                    log("End of read %1", id);
                    read_done = true;
                    if (write_done)
                        cancellation_timer.cancel();
                    return;
                }
                read_begin = read_buffer.get();
                read_end = read_buffer.get() + original_size;
            }
            auto handler = [&](std::error_code ec, size_t n) {
                REALM_ASSERT(!ec || ec == error::operation_aborted);
                ++stats.num_reads;
                if (ec == error::operation_aborted) {
                    ++stats.num_canceled_reads;
                }
                else {
                    read_begin += n;
                }
                if (delayed_read_write_dist(prng) == 0) {
                    auto handler_2 = [&](std::error_code ec) {
                        REALM_ASSERT(!ec);
                        read();
                    };
                    read_timer.async_wait(std::chrono::microseconds(100), std::move(handler_2));
                }
                else {
                    read();
                }
            };
            char* buffer = read_begin;
            size_t size = read_write_size_dist(prng);
            size_t max_size = read_end - read_begin;
            if (size > max_size)
                size = max_size;
            ssl_stream.async_read_some(buffer, size, std::move(handler));
        };
        read();
        const char* write_begin = write_original;
        const char* write_end = write_original + original_size;
        int num_write_cycles = 0;
        std::function<void()> write = [&] {
            if (write_begin == write_end) {
                log("<W%1>", id);
                ++num_write_cycles;
                if (num_write_cycles == num_cycles) {
                    log("End of write %1", id);
                    write_done = true;
                    if (read_done)
                        cancellation_timer.cancel();
                    return;
                }
                write_begin = write_original;
                write_end = write_original + original_size;
            }
            auto handler = [&](std::error_code ec, size_t n) {
                REALM_ASSERT(!ec || ec == error::operation_aborted);
                ++stats.num_writes;
                if (ec == error::operation_aborted) {
                    ++stats.num_canceled_writes;
                }
                else {
                    write_begin += n;
                }
                if (delayed_read_write_dist(prng) == 0) {
                    auto handler_2 = [&](std::error_code ec) {
                        REALM_ASSERT(!ec);
                        write();
                    };
                    write_timer.async_wait(std::chrono::microseconds(100), std::move(handler_2));
                }
                else {
                    write();
                }
            };
            const char* data = write_begin;
            size_t size = read_write_size_dist(prng);
            size_t max_size = write_end - write_begin;
            if (size > max_size)
                size = max_size;
            ssl_stream.async_write_some(data, size, std::move(handler));
        };
        write();
        service.run();
    };

    Stats stats_1, stats_2;
    std::thread thread_1{[&] {
        thread(1, ssl_stream_1, original_1.get(), original_2.get(), stats_1);
    }};
    std::thread thread_2{[&] {
        thread(2, ssl_stream_2, original_2.get(), original_1.get(), stats_2);
    }};
    thread_1.join();
    thread_2.join();

    ssl_stream_1.shutdown();
    ssl_stream_2.shutdown();

    char ch;
    CHECK_SYSTEM_ERROR(ssl_stream_1.read_some(&ch, 1), MiscExtErrors::end_of_input);
    CHECK_SYSTEM_ERROR(ssl_stream_2.read_some(&ch, 1), MiscExtErrors::end_of_input);

    log("Cancellations: %1, %2", stats_1.num_cancellations, stats_2.num_cancellations);
    log("Reads:  %1 (%2 canceled), %3 (%4 canceled)", stats_1.num_reads, stats_1.num_canceled_reads,
        stats_2.num_reads, stats_2.num_canceled_reads);
    log("Writes: %1 (%2 canceled), %3 (%4 canceled)", stats_1.num_writes, stats_1.num_canceled_writes,
        stats_2.num_writes, stats_2.num_canceled_writes);
}

// The host name is contained in both the
// Common Name and the Subject Alternartive Name
// section of the server certificate.
TEST(Util_Network_SSL_Certificate_CN_SAN)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;

    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ssl_context_1.use_certificate_chain_file(ca_dir + "/certs/dns-chain.crt.pem");
    ssl_context_1.use_private_key_file(ca_dir + "/certs/dns-checked-server.key.pem");
    ssl_context_2.use_verify_file(ca_dir + "/root-ca/crt.pem");

    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);

    ssl_stream_2.set_verify_mode(realm::util::network::ssl::VerifyMode::peer);

    // We expect success because the certificate is signed for www.example.com
    // in both Common Name and SAN.
    ssl_stream_2.set_host_name("www.example.com");

    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}

// The host name is only contained in the
// Subject Alternative Name section of the certificate.
TEST(Util_Network_SSL_Certificate_SAN)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;

    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ssl_context_1.use_certificate_chain_file(ca_dir + "/certs/dns-chain.crt.pem");
    ssl_context_1.use_private_key_file(ca_dir + "/certs/dns-checked-server.key.pem");
    ssl_context_2.use_verify_file(ca_dir + "/root-ca/crt.pem");

    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);

    ssl_stream_2.set_verify_mode(realm::util::network::ssl::VerifyMode::peer);

    ssl_stream_2.set_host_name("support.example.com");

    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}


// FIXME: Verification of peer against Common Name is no longer supported in
// Catalina (macOS).
#if REALM_HAVE_OPENSSL || !REALM_HAVE_SECURE_TRANSPORT

// The host name www.example.com is contained in Common Name but not in SAN.
TEST(Util_Network_SSL_Certificate_CN)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;

    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ssl_context_1.use_certificate_chain_file(ca_dir + "/certs/ip-chain.crt.pem");
    ssl_context_1.use_private_key_file(ca_dir + "/certs/ip-server.key.pem");
    ssl_context_2.use_verify_file(ca_dir + "/root-ca/crt.pem");

    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);

    ssl_stream_2.set_verify_mode(realm::util::network::ssl::VerifyMode::peer);

    ssl_stream_2.set_host_name("www.example.com");

    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
        CHECK_EQUAL(std::error_code(), ec);
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}

#endif // REALM_HAVE_OPENSSL || !REALM_HAVE_SECURE_TRANSPORT

// The ip address is contained in the IP SAN section
// of the certificate. For OpenSSL, we expect failure because we only
// check for DNS. For Secure Transport we get success because the
// ip section is checked. This discrepancy could be resolved in the
// future if deemed important.
TEST(Util_Network_SSL_Certificate_IP)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;

    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ssl_context_1.use_certificate_chain_file(ca_dir + "/certs/ip-chain.crt.pem");
    ssl_context_1.use_private_key_file(ca_dir + "/certs/ip-server.key.pem");
    ssl_context_2.use_verify_file(ca_dir + "/root-ca/crt.pem");

    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);

    ssl_stream_2.set_verify_mode(realm::util::network::ssl::VerifyMode::peer);

    ssl_stream_2.set_host_name("127.0.0.1");

    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
#if REALM_HAVE_OPENSSL
        CHECK_NOT_EQUAL(std::error_code(), ec);
#elif REALM_HAVE_SECURE_TRANSPORT
        CHECK_EQUAL(std::error_code(), ec);
#endif
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
#if REALM_HAVE_OPENSSL
        CHECK_NOT_EQUAL(std::error_code(), ec);
#elif REALM_HAVE_SECURE_TRANSPORT
        CHECK_EQUAL(std::error_code(), ec);
#endif
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}

// The certificate contains incorrect host names.
// We expect the handshake to fail.
TEST(Util_Network_SSL_Certificate_Failure)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    network::ssl::Context ssl_context_1;
    network::ssl::Context ssl_context_2;

    std::string ca_dir = get_test_resource_path() + "../certificate-authority";

    ssl_context_1.use_certificate_chain_file(ca_dir + "/certs/dns-chain.crt.pem");
    ssl_context_1.use_private_key_file(ca_dir + "/certs/dns-checked-server.key.pem");
    ssl_context_2.use_verify_file(ca_dir + "/root-ca/crt.pem");

    network::ssl::Stream ssl_stream_1{socket_1, ssl_context_1, network::ssl::Stream::server};
    network::ssl::Stream ssl_stream_2{socket_2, ssl_context_2, network::ssl::Stream::client};
    ssl_stream_1.set_logger(&test_context.logger);
    ssl_stream_2.set_logger(&test_context.logger);

    ssl_stream_2.set_verify_mode(realm::util::network::ssl::VerifyMode::peer);

    // We expect failure because the certificate is signed for www.example.com
    ssl_stream_2.set_host_name("www.another-example.com");

    connect_sockets(socket_1, socket_2);

    auto connector = [&] {
        std::error_code ec;
        ssl_stream_2.handshake(ec);
        // Refine this
        CHECK_NOT_EQUAL(std::error_code(), ec);
    };
    auto acceptor = [&] {
        std::error_code ec;
        ssl_stream_1.handshake(ec);
        // Refine this
        CHECK_NOT_EQUAL(std::error_code(), ec);
    };

    std::thread thread_1(std::move(connector));
    std::thread thread_2(std::move(acceptor));
    thread_1.join();
    thread_2.join();
}
