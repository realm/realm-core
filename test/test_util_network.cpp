#include <algorithm>
#include <atomic>
#include <stdexcept>
#include <sstream>
#include <memory>
#include <thread>

#include <realm/util/memory_stream.hpp>
#include <realm/util/network.hpp>

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
    network::Acceptor acceptor{service_1};
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
    socket_1.set_option(network::SocketBase::no_delay(true));
    socket_2.set_option(network::SocketBase::no_delay(true));
}

void connect_socket(network::Socket& socket, std::string port)
{
    network::Service& service = socket.get_service();
    network::Resolver resolver{service};
    network::Resolver::Query query("localhost", port);
    network::Endpoint::List endpoints = resolver.resolve(query);

    auto i = endpoints.begin();
    auto end = endpoints.end();
    for (;;) {
        std::error_code ec;
        socket.connect(*i, ec);
        if (!ec)
            break;
        socket.close();
        if (++i == end)
            throw std::runtime_error("Failed to connect to localhost:" + port);
    }
}


TEST(Network_Hostname)
{
    // Just check that we call call network::host_name()
    network::host_name();
}


TEST(Network_PostOperation)
{
    network::Service service;
    int var_1 = 381, var_2 = 743;
    service.post([&] {
        var_1 = 824;
    });
    service.post([&] {
        var_2 = 216;
    });
    CHECK_EQUAL(var_1, 381);
    CHECK_EQUAL(var_2, 743);
    service.run();
    CHECK_EQUAL(var_1, 824);
    CHECK_EQUAL(var_2, 216);
    service.post([&] {
        var_2 = 191;
    });
    service.post([&] {
        var_1 = 476;
    });
    CHECK_EQUAL(var_1, 824);
    CHECK_EQUAL(var_2, 216);
    service.run();
    CHECK_EQUAL(var_1, 476);
    CHECK_EQUAL(var_2, 191);
}


TEST(Network_EventLoopStopAndReset_1)
{
    network::Service service;

    // Prestop
    int var = 381;
    service.stop();
    service.post([&] {
        var = 824;
    });
    service.run(); // Must return immediately
    CHECK_EQUAL(var, 381);
    service.run(); // Must still return immediately
    CHECK_EQUAL(var, 381);

    // Reset
    service.reset();
    service.post([&] {
        var = 824;
    });
    CHECK_EQUAL(var, 381);
    service.run();
    CHECK_EQUAL(var, 824);
    service.post([&] {
        var = 476;
    });
    CHECK_EQUAL(var, 824);
    service.run();
    CHECK_EQUAL(var, 476);
}


TEST(Network_EventLoopStopAndReset_2)
{
    // Introduce a blocking operation that will keep the event loop running
    network::Service service;
    network::Acceptor acceptor{service};
    bind_acceptor(acceptor);
    network::Socket socket{service};
    acceptor.async_accept(socket, [](std::error_code) {});

    // Start event loop execution in the background
    ThreadWrapper thread_1;
    thread_1.start([&] {
        service.run();
    });

    // Check that the event loop is actually running
    BowlOfStonesSemaphore bowl_1; // Empty
    service.post([&] {
        bowl_1.add_stone();
    });
    bowl_1.get_stone(); // Block until the stone is added

    // Stop the event loop
    service.stop();
    CHECK_NOT(thread_1.join());

    // Check that the event loop remains in the stopped state
    int var = 381;
    service.post([&] {
        var = 824;
    });
    CHECK_EQUAL(var, 381);
    service.run(); // Still stopped, so run() must return immediately
    CHECK_EQUAL(var, 381);

    // Put the event loop back into the unstopped state, and restart it in the
    // background
    service.reset();
    ThreadWrapper thread_2;
    thread_2.start([&] {
        service.run();
    });

    // Check that the event loop is actually running
    BowlOfStonesSemaphore bowl_2; // Empty
    service.post([&] {
        bowl_2.add_stone();
    });
    bowl_2.get_stone(); // Block until the stone is added

    // Stop the event loop by canceling the blocking operation
    service.post([&] {
        acceptor.cancel();
    });
    CHECK_NOT(thread_2.join());

    CHECK_EQUAL(var, 824);
}


TEST(Network_GetSetSocketOption)
{
    network::Service service;
    network::Socket socket{service};
    socket.open(network::StreamProtocol::ip_v4());
    network::Socket::reuse_address opt_reuse_addr;
    socket.get_option(opt_reuse_addr);
    CHECK_NOT(opt_reuse_addr.value());
    socket.set_option(network::Socket::reuse_address(true));
    socket.get_option(opt_reuse_addr);
    CHECK(opt_reuse_addr.value());
}


TEST(Network_AsyncConnectAndAsyncAccept)
{
    network::Service service;
    network::Acceptor acceptor{service};
    network::Endpoint listening_endpoint = bind_acceptor(acceptor);
    network::Socket socket_1{service}, socket_2{service};
    bool connected = false;
    auto connect_handler = [&](std::error_code ec) {
        if (ec)
            throw std::system_error(ec);
        connected = true;
        log("connected");
    };
    bool accepted = false;
    auto accept_handler = [&](std::error_code ec) {
        if (ec)
            throw std::system_error(ec);
        accepted = true;
        log("accepted");
    };
    socket_1.async_connect(listening_endpoint, connect_handler);
    acceptor.async_accept(socket_2, accept_handler);
    service.run();
    CHECK(connected);
    CHECK(accepted);
}


TEST(Network_ReadWrite)
{
    network::Service service_1;
    network::Acceptor acceptor{service_1};
    network::Endpoint listening_endpoint = bind_acceptor(acceptor);

    char data[] = {'X', 'F', 'M'};

    auto reader = [&] {
        network::Socket socket_1{service_1};
        acceptor.accept(socket_1);
        socket_1.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        char buffer[sizeof data];
        size_t n = socket_1.read(buffer, sizeof data, rab);
        if (CHECK_EQUAL(sizeof data, n))
            CHECK(std::equal(buffer, buffer + n, data));
        std::error_code ec;
        n = socket_1.read(buffer, 1, rab, ec);
        CHECK_EQUAL(0, n);
        CHECK(ec == MiscExtErrors::end_of_input);
    };
    ThreadWrapper thread;
    thread.start(reader);

    network::Service service_2;
    network::Socket socket_2{service_2};
    socket_2.connect(listening_endpoint);
    socket_2.set_option(network::SocketBase::no_delay(true));
    socket_2.write(data, sizeof data);
    socket_2.close();

    CHECK_NOT(thread.join());
}


TEST(Network_ReadWriteNativeHandle)
{
    network::Service service_1;
    network::Acceptor acceptor{service_1};
    network::Endpoint listening_endpoint = bind_acceptor(acceptor);

    char data[] = {'X', 'F', 'M'};

    auto reader = [&] {
        network::Socket socket_1{service_1};
        acceptor.accept(socket_1);
        socket_1.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        char buffer[sizeof data];
        size_t n = socket_1.read(buffer, sizeof data, rab);
        if (CHECK_EQUAL(sizeof data, n))
            CHECK(std::equal(buffer, buffer + n, data));
        std::error_code ec;
        n = socket_1.read(buffer, 1, rab, ec);
        CHECK_EQUAL(0, n);
        CHECK(ec == MiscExtErrors::end_of_input);
    };
    ThreadWrapper thread;
    thread.start(reader);

    network::Service service_2;

    // Connect with plain POSIX APIs.
    int family = listening_endpoint.protocol().family();
    int protocol = listening_endpoint.protocol().protocol();
    using native_handle_type = network::SocketBase::native_handle_type;
    native_handle_type sockfd = ::socket(family, SOCK_STREAM, protocol);
    CHECK_GREATER(sockfd, 0);

    int endpoint_size = listening_endpoint.protocol().is_ip_v4() ? sizeof(sockaddr_in) : sizeof(sockaddr_in6);
    int ret = ::connect(sockfd, listening_endpoint.data(), endpoint_size);
    CHECK_EQUAL(ret, 0);

    network::Socket socket_2{service_2, listening_endpoint.protocol(), sockfd};
    socket_2.write(data, sizeof data);
    socket_2.close();

    CHECK_NOT(thread.join());
}


TEST(Network_ReadWriteLargeAmount)
{
    network::Service service_1;
    network::Acceptor acceptor{service_1};
    network::Endpoint listening_endpoint = bind_acceptor(acceptor);

    size_t num_bytes_per_chunk = 1048576L / 2;
    std::unique_ptr<char[]> chunk(new char[num_bytes_per_chunk]);
    for (size_t i = 0; i < num_bytes_per_chunk; ++i)
        chunk[i] = char(i % 128);
    int num_chunks = 128;

    auto reader = [&] {
        network::Socket socket_1{service_1};
        acceptor.accept(socket_1);
        socket_1.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        size_t buffer_size = 8191; // Prime
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        size_t offset_in_chunk = 0;
        int chunk_index = 0;
        for (;;) {
            std::error_code ec;
            size_t n = socket_1.read(buffer.get(), buffer_size, rab, ec);
            bool equal = true;
            for (size_t i = 0; i < n; ++i) {
                if (chunk[offset_in_chunk] != buffer[i]) {
                    equal = false;
                    break;
                }
                if (++offset_in_chunk == num_bytes_per_chunk) {
                    offset_in_chunk = 0;
                    ++chunk_index;
                }
            }
            CHECK(equal);
            if (ec == MiscExtErrors::end_of_input)
                break;
            CHECK_NOT(ec);
        }
        CHECK_EQUAL(0, offset_in_chunk);
        CHECK_EQUAL(num_chunks, chunk_index);
    };
    ThreadWrapper thread;
    thread.start(reader);

    network::Service service_2;
    network::Socket socket_2{service_2};
    socket_2.connect(listening_endpoint);
    socket_2.set_option(network::SocketBase::no_delay(true));
    for (int i = 0; i < num_chunks; ++i)
        socket_2.write(chunk.get(), num_bytes_per_chunk);
    socket_2.close();

    CHECK_NOT(thread.join());
}


TEST(Network_AsyncReadWriteLargeAmount)
{
    network::Service service_1;
    network::Acceptor acceptor{service_1};
    network::Endpoint listening_endpoint = bind_acceptor(acceptor);

    size_t num_bytes_per_chunk = 1048576 / 2;
    std::unique_ptr<char[]> chunk(new char[num_bytes_per_chunk]);
    for (size_t i = 0; i < num_bytes_per_chunk; ++i)
        chunk[i] = char(i % 128);
    int num_chunks = 128;

    auto reader = [&] {
        network::Socket socket_1{service_1};
        acceptor.accept(socket_1);
        socket_1.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        size_t buffer_size = 8191; // Prime
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        size_t offset_in_chunk = 0;
        int chunk_index = 0;
        std::function<void()> read_chunk = [&] {
            auto handler = [&](std::error_code ec, size_t n) {
                bool equal = true;
                for (size_t i = 0; i < n; ++i) {
                    if (buffer[i] != chunk[offset_in_chunk]) {
                        equal = false;
                        break;
                    }
                    if (++offset_in_chunk == num_bytes_per_chunk) {
                        offset_in_chunk = 0;
                        ++chunk_index;
                    }
                }
                CHECK(equal);
                if (ec == MiscExtErrors::end_of_input)
                    return;
                CHECK_NOT(ec);
                read_chunk();
            };
            socket_1.async_read(buffer.get(), buffer_size, rab, handler);
        };
        read_chunk();
        service_1.run();
        CHECK_EQUAL(0, offset_in_chunk);
        CHECK_EQUAL(num_chunks, chunk_index);
    };
    ThreadWrapper thread;
    thread.start(reader);

    network::Service service_2;
    network::Socket socket_2{service_2};
    socket_2.connect(listening_endpoint);
    socket_2.set_option(network::SocketBase::no_delay(true));
    std::function<void(int)> write_chunk = [&](int i) {
        auto handler = [this, i, num_chunks, num_bytes_per_chunk, write_chunk](std::error_code ec, size_t n) {
            if (CHECK_NOT(ec)) {
                CHECK_EQUAL(num_bytes_per_chunk, n);
                if (i + 1 == num_chunks)
                    return;
                write_chunk(i + 1);
            }
        };
        socket_2.async_write(chunk.get(), num_bytes_per_chunk, handler);
    };
    write_chunk(0);
    service_2.run();
    socket_2.close();

    CHECK_NOT(thread.join());
}


TEST(Network_SocketAndAcceptorOpen)
{
    network::Service service_1;
    network::Acceptor acceptor{service_1};
    network::Resolver resolver{service_1};
    network::Resolver::Query query("localhost", "",
                                   network::Resolver::Query::passive | network::Resolver::Query::address_configured);
    network::Endpoint::List endpoints = resolver.resolve(query);
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
    network::Endpoint listening_endpoint = acceptor.local_endpoint();
    acceptor.listen();
    network::Socket socket_1{service_1};
    ThreadWrapper thread;
    thread.start([&] {
        acceptor.accept(socket_1);
    });

    network::Service service_2;
    network::Socket socket_2{service_2};
    socket_2.open(listening_endpoint.protocol());
    socket_2.connect(listening_endpoint);

    thread.join();
}


TEST(Network_CancelAsyncAccept)
{
    network::Service service;
    network::Acceptor acceptor{service};
    bind_acceptor(acceptor);
    network::Socket socket{service};

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


TEST(Network_CancelAsyncConnect)
{
    network::Service service;
    network::Acceptor acceptor{service};
    network::Endpoint ep = bind_acceptor(acceptor);
    network::Socket socket{service};

    bool connect_was_canceled = false;
    auto handler = [&](std::error_code ec) {
        if (ec == error::operation_aborted)
            connect_was_canceled = true;
    };
    socket.async_connect(ep, handler);
    socket.cancel();
    service.run();
    CHECK(connect_was_canceled);

    connect_was_canceled = false;
    socket.async_connect(ep, handler);
    socket.close();
    service.run();
    CHECK(connect_was_canceled);
}


TEST(Network_CancelAsyncReadWrite)
{
    network::Service service;
    network::Acceptor acceptor{service};
    bind_acceptor(acceptor);
    network::Socket socket_1{service};
    bool was_accepted = false;
    auto accept_handler = [&](std::error_code ec) {
        if (!ec)
            was_accepted = true;
    };
    acceptor.async_accept(socket_1, accept_handler);
    network::Socket socket_2{service};
    socket_2.connect(acceptor.local_endpoint());
    socket_2.set_option(network::SocketBase::no_delay(true));
    service.run();
    CHECK(was_accepted);
    socket_1.set_option(network::SocketBase::no_delay(true));
    const size_t size = 1;
    char data[size] = {'a'};
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2.async_write(data, size, write_handler);
    network::ReadAheadBuffer rab;
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    socket_2.async_read(buffer, size, rab, read_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}

TEST(Network_CancelEmptyRead)
{
    // Make sure that an immediately completable read operation is still
    // cancelable

    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    connect_sockets(socket_1, socket_2);
    network::ReadAheadBuffer rab;
    const size_t size = 1;
    char data[size] = {'a'};
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2.async_write(data, size, write_handler);
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    socket_2.async_read(buffer, 0, rab, read_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}

TEST(Network_CancelEmptyWrite)
{
    // Make sure that an immediately completable write operation is still
    // cancelable

    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    connect_sockets(socket_1, socket_2);
    network::ReadAheadBuffer rab;
    char buffer[1];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    socket_2.async_read(buffer, 1, rab, read_handler);
    char data[1] = {'a'};
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2.async_write(data, 0, write_handler);
    socket_2.close();
    service.run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}


TEST(Network_CancelReadByDestroy)
{
    // Check that canceled read operations never try to access socket, stream,
    // or input buffer objects, even if they were partially completed.

    const int num_connections = 16;
    network::Service service;
    std::unique_ptr<std::unique_ptr<network::Socket>[]> write_sockets;
    std::unique_ptr<std::unique_ptr<network::Socket>[]> read_sockets;
    std::unique_ptr<std::unique_ptr<network::ReadAheadBuffer>[]> read_ahead_buffers;
    write_sockets.reset(new std::unique_ptr<network::Socket>[num_connections]);
    read_sockets.reset(new std::unique_ptr<network::Socket>[num_connections]);
    read_ahead_buffers.reset(new std::unique_ptr<network::ReadAheadBuffer>[num_connections]);
    char output_buffer[2] = {'x', '\n'};
    std::unique_ptr<char[][2]> input_buffers(new char[num_connections][2]);
    for (int i = 0; i < num_connections; ++i) {
        write_sockets[i].reset(new network::Socket{service});
        read_sockets[i].reset(new network::Socket{service});
        connect_sockets(*write_sockets[i], *read_sockets[i]);
        read_ahead_buffers[i].reset(new network::ReadAheadBuffer);
    }
    for (int i = 0; i < num_connections; ++i) {
        auto read_handler = [&](std::error_code ec, size_t n) {
            CHECK(n == 0 || n == 1 || n == 2);
            if (n == 2) {
                CHECK_NOT(ec);
                for (int j = 0; j < num_connections; ++j)
                    read_sockets[j]->cancel();
                read_ahead_buffers.reset(); // Destroy all input streams
                read_sockets.reset();       // Destroy all read sockets
                input_buffers.reset();      // Destroy all input buffers
                return;
            }
            CHECK_EQUAL(error::operation_aborted, ec);
        };
        read_sockets[i]->async_read_until(input_buffers[i], 2, '\n', *read_ahead_buffers[i], read_handler);
        auto write_handler = [&](std::error_code ec, size_t) {
            CHECK_NOT(ec);
        };
        int n = (i == num_connections / 2 ? 2 : 1);
        write_sockets[i]->async_write(output_buffer, n, write_handler);
    }
    service.run();
}


TEST(Network_AcceptorMixedAsyncSync)
{
    network::Service service;
    network::Acceptor acceptor{service};
    bind_acceptor(acceptor);
    network::Endpoint ep = acceptor.local_endpoint();
    auto connect = [ep] {
        network::Service connect_service;
        network::Socket socket{connect_service};
        socket.connect(ep);
    };

    // Synchronous accept -> stay on blocking mode
    {
        ThreadWrapper thread;
        thread.start(connect);
        network::Socket socket{service};
        acceptor.accept(socket);
        CHECK_NOT(thread.join());
    }

    // Asynchronous accept -> switch to nonblocking mode
    {
        ThreadWrapper thread;
        thread.start(connect);
        network::Socket socket{service};
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
        network::Socket socket{service};
        acceptor.accept(socket);
        CHECK_NOT(thread.join());
    }
}


TEST(Network_SocketMixedAsyncSync)
{
    network::Service acceptor_service;
    network::Acceptor acceptor{acceptor_service};
    bind_acceptor(acceptor);
    network::Endpoint ep = acceptor.local_endpoint();
    auto accept_and_echo = [&] {
        network::Socket socket{acceptor_service};
        acceptor.accept(socket);
        socket.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        size_t buffer_size = 1024;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        size_t size = socket.read_until(buffer.get(), buffer_size, '\n', rab);
        socket.write(buffer.get(), size);
    };

    {
        ThreadWrapper thread;
        thread.start(accept_and_echo);
        network::Service service;

        // Synchronous connect -> stay in blocking mode
        network::Socket socket{service};
        socket.connect(ep);
        socket.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;

        // Asynchronous write -> switch to nonblocking mode
        const char* message = "Calabi-Yau\n";
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
        size_t size = socket.read(buffer.get(), buffer_size, rab, ec);
        if (CHECK_EQUAL(ec, MiscExtErrors::end_of_input)) {
            if (CHECK_EQUAL(size, strlen(message)))
                CHECK(std::equal(buffer.get(), buffer.get() + size, message));
        }

        CHECK_NOT(thread.join());
    }

    {
        ThreadWrapper thread;
        thread.start(accept_and_echo);
        network::Service service;

        // Asynchronous connect -> switch to nonblocking mode
        network::Socket socket{service};
        bool is_connected = false;
        auto connect_handler = [&](std::error_code ec) {
            if (!ec)
                is_connected = true;
        };
        socket.async_connect(ep, connect_handler);
        service.run();
        CHECK(is_connected);
        network::ReadAheadBuffer rab;

        // Synchronous write -> switch back to blocking mode
        const char* message = "The Verlinde Algebra And The Cohomology Of The Grassmannian\n";
        socket.write(message, strlen(message));

        // Asynchronous read -> swich once again to nonblocking mode
        size_t buffer_size = 1024;
        std::unique_ptr<char[]> buffer(new char[buffer_size]);
        auto read_handler = [&](std::error_code ec, size_t size) {
            if (CHECK_EQUAL(ec, MiscExtErrors::end_of_input)) {
                if (CHECK_EQUAL(size, strlen(message)))
                    CHECK(std::equal(buffer.get(), buffer.get() + size, message));
            }
        };
        socket.async_read(buffer.get(), buffer_size, rab, read_handler);
        service.run();

        CHECK_NOT(thread.join());
    }
}


TEST(Network_SocketShutdown)
{
    network::Service service;
    network::Socket socket_1{service}, socket_2{service};
    connect_sockets(socket_1, socket_2);
    network::ReadAheadBuffer read_ahead_buffer;

    bool end_of_input_seen = false;
    auto handler = [&](std::error_code ec, size_t) {
        if (ec == MiscExtErrors::end_of_input)
            end_of_input_seen = true;
    };
    char ch;
    socket_2.async_read(&ch, 1, read_ahead_buffer, std::move(handler));
    socket_1.shutdown(network::Socket::shutdown_send);
    service.run();
    CHECK(end_of_input_seen);
}


TEST(Network_DeadlineTimer)
{
    network::Service service;
    network::DeadlineTimer timer{service};

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
    network::Service service;
    network::DeadlineTimer timer_1{service};
    network::DeadlineTimer timer_2{service};
    network::DeadlineTimer timer_3{service};
    network::DeadlineTimer timer_4{service};
    network::DeadlineTimer timer_5{service};
    network::DeadlineTimer timer_6{service};
    timer_1.async_wait(std::chrono::seconds(3), [](error_code) { std::cerr << "*3*\n";   });
    timer_2.async_wait(std::chrono::seconds(2), [](error_code) { std::cerr << "*2*\n";   });
    timer_3.async_wait(std::chrono::seconds(3), [](error_code) { std::cerr << "*3-2*\n"; });
    timer_4.async_wait(std::chrono::seconds(2), [](error_code) { std::cerr << "*2-2*\n"; });
    timer_5.async_wait(std::chrono::seconds(1), [](error_code) { std::cerr << "*1*\n";   });
    timer_6.async_wait(std::chrono::seconds(2), [](error_code) { std::cerr << "*2-3*\n"; });
    service.run();
}
*/


TEST(Network_ThrowFromHandlers)
{
    // Check that exceptions can propagate correctly out from any type of
    // completion handler
    network::Service service;
    struct TestException1 {
    };
    service.post([] {
        throw TestException1();
    });
    CHECK_THROW(service.run(), TestException1);

    {
        network::Acceptor acceptor{service};
        network::Endpoint ep = bind_acceptor(acceptor);
        network::Socket socket_1{service};
        struct TestException2 {
        };
        acceptor.async_accept(socket_1, [](std::error_code) {
            throw TestException2();
        });
        network::Socket socket_2{service};
        socket_2.async_connect(ep, [](std::error_code) {});
        CHECK_THROW(service.run(), TestException2);
    }
    {
        network::Acceptor acceptor{service};
        network::Endpoint ep = bind_acceptor(acceptor);
        network::Socket socket_1{service};
        acceptor.async_accept(socket_1, [](std::error_code) {});
        network::Socket socket_2{service};
        struct TestException3 {
        };
        socket_2.async_connect(ep, [](std::error_code) {
            throw TestException3();
        });
        CHECK_THROW(service.run(), TestException3);
    }
    {
        network::Socket socket_1{service}, socket_2{service};
        connect_sockets(socket_1, socket_2);
        network::ReadAheadBuffer rab;
        char ch_1;
        struct TestException4 {
        };
        socket_1.async_read(&ch_1, 1, rab, [](std::error_code, size_t) {
            throw TestException4();
        });
        char ch_2 = 0;
        socket_2.async_write(&ch_2, 1, [](std::error_code, size_t) {});
        CHECK_THROW(service.run(), TestException4);
    }
    {
        network::Socket socket_1{service}, socket_2{service};
        connect_sockets(socket_1, socket_2);
        network::ReadAheadBuffer rab;
        char ch_1;
        socket_1.async_read(&ch_1, 1, rab, [](std::error_code, size_t) {});
        char ch_2 = 0;
        struct TestException5 {
        };
        socket_2.async_write(&ch_2, 1, [](std::error_code, size_t) {
            throw TestException5();
        });
        CHECK_THROW(service.run(), TestException5);
    }
    {
        network::DeadlineTimer timer{service};
        struct TestException6 {
        };
        timer.async_wait(std::chrono::seconds(0), [](std::error_code) {
            throw TestException6();
        });
        CHECK_THROW(service.run(), TestException6);
    }
}


TEST(Network_HandlerDealloc)
{
    // Check that dynamically allocated handlers are properly freed when the
    // service object is destroyed.
    {
        // m_post_handlers
        network::Service service;
        service.post([] {});
    }
    {
        // m_imm_handlers
        network::Service service;
        // By adding two post handlers that throw, one is going to be left
        // behind in `m_imm_handlers`
        service.post([&] {
            throw std::runtime_error("");
        });
        service.post([&] {
            throw std::runtime_error("");
        });
        CHECK_THROW(service.run(), std::runtime_error);
    }
    {
        // m_poll_handlers
        network::Service service;
        network::Acceptor acceptor{service};
        acceptor.open(network::StreamProtocol::ip_v4());
        network::Socket socket{service};
        // This leaves behind a read handler in m_poll_handlers
        acceptor.async_accept(socket, [&](std::error_code) {});
    }
    {
        // m_cancel_handlers
        network::Service service;
        network::Acceptor acceptor{service};
        acceptor.open(network::StreamProtocol::ip_v4());
        network::Socket socket{service};
        acceptor.async_accept(socket, [&](std::error_code) {});
        // This leaves behind a read handler in m_cancel_handlers
        acceptor.close();
    }
    {
        // m_poll_handlers
        network::Service service_1;
        network::Acceptor acceptor{service_1};
        network::Endpoint listening_endpoint = bind_acceptor(acceptor);
        network::Socket socket_1{service_1};
        ThreadWrapper thread;
        thread.start([&] {
            acceptor.accept(socket_1);
        });
        network::Service service_2;
        network::Socket socket_2{service_2};
        socket_2.connect(listening_endpoint);
        socket_2.set_option(network::SocketBase::no_delay(true));
        thread.join();
        socket_1.set_option(network::SocketBase::no_delay(true));
        network::ReadAheadBuffer rab;
        char buffer[1];
        char data[] = {'X', 'F', 'M'};
        // This leaves behind both a read and a write handler in m_poll_handlers
        socket_1.async_read(buffer, sizeof buffer, rab, [](std::error_code, size_t) {});
        socket_1.async_write(data, sizeof data, [](std::error_code, size_t) {});
    }
}


namespace {

template <int size>
struct PostReallocHandler {
    PostReallocHandler(int& v)
        : var(v)
    {
    }
    void operator()()
    {
        var = size;
    }
    int& var;
    char strut[size];
};

} // unnamed namespace

TEST(Network_PostRealloc)
{
    // Use progressively larger post handlers to check that memory reallocation
    // works

    network::Service service;
    int var = 0;
    for (int i = 0; i < 3; ++i) {
        service.post(PostReallocHandler<10>(var));
        service.run();
        CHECK_EQUAL(10, var);
        service.post(PostReallocHandler<100>(var));
        service.run();
        CHECK_EQUAL(100, var);
        service.post(PostReallocHandler<1000>(var));
        service.run();
        CHECK_EQUAL(1000, var);
    }
}


namespace {

struct AsyncReadWriteRealloc {
    network::Service service;
    network::Socket read_socket{service}, write_socket{service};
    network::ReadAheadBuffer rab;
    char read_buffer[3];
    char write_buffer[3] = {'0', '1', '2'};
    Random random{random_int<unsigned long>()}; // Seed from slow global generator

    const size_t num_bytes_to_write = 65536;
    size_t num_bytes_written = 0;
    size_t num_bytes_read = 0;

    template <int size>
    struct WriteHandler {
        WriteHandler(AsyncReadWriteRealloc& s)
            : state(s)
        {
        }
        void operator()(std::error_code ec, size_t n)
        {
            if (ec)
                throw std::system_error(ec);
            state.num_bytes_written += n;
            state.initiate_write();
        }
        AsyncReadWriteRealloc& state;
        char strut[size];
    };

    void initiate_write()
    {
        if (num_bytes_written >= num_bytes_to_write) {
            write_socket.close();
            return;
        }
        int v = random.draw_int_max(3);
        size_t n = std::min(size_t(v), size_t(num_bytes_to_write - num_bytes_written));
        switch (v) {
            case 0:
                write_socket.async_write(write_buffer, n, WriteHandler<1>(*this));
                return;
            case 1:
                write_socket.async_write(write_buffer, n, WriteHandler<10>(*this));
                return;
            case 2:
                write_socket.async_write(write_buffer, n, WriteHandler<100>(*this));
                return;
            case 3:
                write_socket.async_write(write_buffer, n, WriteHandler<1000>(*this));
                return;
        }
        REALM_ASSERT(false);
    }

    template <int size>
    struct ReadHandler {
        ReadHandler(AsyncReadWriteRealloc& s)
            : state(s)
        {
        }
        void operator()(std::error_code ec, size_t n)
        {
            if (ec && ec != MiscExtErrors::end_of_input)
                throw std::system_error(ec);
            state.num_bytes_read += n;
            if (ec != MiscExtErrors::end_of_input)
                state.initiate_read();
        }
        AsyncReadWriteRealloc& state;
        char strut[size];
    };

    void initiate_read()
    {
        int v = random.draw_int_max(3);
        size_t n = size_t(v);
        switch (v) {
            case 0:
                read_socket.async_read(read_buffer, n, rab, ReadHandler<1>(*this));
                return;
            case 1:
                read_socket.async_read(read_buffer, n, rab, ReadHandler<10>(*this));
                return;
            case 2:
                read_socket.async_read(read_buffer, n, rab, ReadHandler<100>(*this));
                return;
            case 3:
                read_socket.async_read(read_buffer, n, rab, ReadHandler<1000>(*this));
                return;
        }
        REALM_ASSERT(false);
    }
};

} // unnamed namespace

TEST(Network_AsyncReadWriteRealloc)
{
    // Use progressively larger completion handlers to check that memory
    // reallocation works

    AsyncReadWriteRealloc state;
    connect_sockets(state.read_socket, state.write_socket);
    state.initiate_read();
    state.initiate_write();
    state.service.run();
    CHECK_EQUAL(state.num_bytes_to_write, state.num_bytes_written);
    CHECK_EQUAL(state.num_bytes_written, state.num_bytes_read);
}


namespace {

char echo_body[] = {'\xC1', '\x2C', '\xEF', '\x48', '\x8C', '\xCD', '\x41', '\xFA', '\x12', '\xF9', '\xF4',
                    '\x72', '\xDF', '\x92', '\x8E', '\x68', '\xAB', '\x8F', '\x6B', '\xDF', '\x80', '\x26',
                    '\xD1', '\x60', '\x21', '\x91', '\x20', '\xC8', '\x94', '\x0C', '\xDB', '\x07', '\xB0',
                    '\x1C', '\x3A', '\xDA', '\x5E', '\x9B', '\x62', '\xDE', '\x30', '\xA3', '\x7E', '\xED',
                    '\xB4', '\x30', '\xD7', '\x43', '\x3F', '\xDE', '\xF2', '\x6D', '\x9A', '\x1D', '\xAE',
                    '\xF4', '\xD5', '\xFB', '\xAC', '\xE8', '\x67', '\x37', '\xFD', '\xF3'};

void sync_server(network::Acceptor& acceptor, unit_test::TestContext& test_context)
{
    network::Service& service = acceptor.get_service();
    network::Socket socket{service};
    network::Endpoint endpoint;
    acceptor.accept(socket, endpoint);
    socket.set_option(network::SocketBase::no_delay(true));

    network::ReadAheadBuffer rab;
    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    size_t n = socket.read_until(header_buffer, max_header_size, '\n', rab);
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n - 1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer + (n - 1));
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
    size_t m = socket.read(body_buffer.get(), body_size, rab);
    if (!CHECK_EQUAL(m, body_size))
        return;
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer + max_header_size);
    out << "was " << body_size << '\n';
    socket.write(header_buffer, out.size());
    socket.write(body_buffer.get(), body_size);
}


void sync_client(unsigned short listen_port, unit_test::TestContext& test_context)
{
    network::Service service;
    network::Socket socket{service};
    {
        std::ostringstream out;
        out << listen_port;
        std::string listen_port_2 = out.str();
        connect_socket(socket, listen_port_2);
    }
    socket.set_option(network::SocketBase::no_delay(true));

    const size_t max_header_size = 32;
    char header_buffer[max_header_size];
    MemoryOutputStream out;
    out.set_buffer(header_buffer, header_buffer + max_header_size);
    out << "echo " << sizeof echo_body << '\n';
    socket.write(header_buffer, out.size());
    socket.write(echo_body, sizeof echo_body);

    network::ReadAheadBuffer rab;
    size_t n = socket.read_until(header_buffer, max_header_size, '\n', rab);
    if (!CHECK_GREATER(n, 0))
        return;
    if (!CHECK_LESS_EQUAL(n, max_header_size))
        return;
    if (!CHECK_EQUAL(header_buffer[n - 1], '\n'))
        return;
    MemoryInputStream in;
    in.set_buffer(header_buffer, header_buffer + (n - 1));
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
    size_t m = socket.read(echo_buffer.get(), echo_size, rab);
    if (!CHECK_EQUAL(m, echo_size))
        return;
    if (!CHECK_EQUAL(echo_size, sizeof echo_body))
        return;
    CHECK(std::equal(echo_body, echo_body + sizeof echo_body, echo_buffer.get()));
}

} // anonymous namespace


TEST(Network_Sync)
{
    network::Service service;
    network::Acceptor acceptor{service};
    network::Endpoint listen_endpoint = bind_acceptor(acceptor);
    network::Endpoint::port_type listen_port = listen_endpoint.port();

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] {
        sync_server(acceptor, test_context);
    });
    client_thread.start([&] {
        sync_client(listen_port, test_context);
    });
    client_thread.join();
    server_thread.join();
}


namespace {

class async_server {
public:
    async_server(unit_test::TestContext& test_context)
        : m_acceptor{m_service}
        , m_socket{m_service}
        , m_test_context{test_context}
    {
    }

    unsigned short init()
    {
        network::Endpoint listen_endpoint = bind_acceptor(m_acceptor);
        network::Endpoint::port_type listen_port = listen_endpoint.port();
        return listen_port;
    }

    void run()
    {
        auto handler = [this](std::error_code ec) {
            handle_accept(ec);
        };
        network::Endpoint endpoint;
        m_acceptor.async_accept(m_socket, endpoint, handler);
        m_service.run();
    }

private:
    network::Service m_service;
    network::Acceptor m_acceptor;
    network::Socket m_socket;
    network::ReadAheadBuffer m_read_ahead_buffer;
    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestContext& m_test_context;

    void handle_accept(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        m_socket.set_option(network::SocketBase::no_delay(true));
        auto handler = [this](std::error_code handler_ec, size_t handler_n) {
            handle_read_header(handler_ec, handler_n);
        };
        m_socket.async_read_until(m_header_buffer, s_max_header_size, '\n', m_read_ahead_buffer, handler);
    }

    void handle_read_header(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestContext& test_context = m_test_context;
        if (!CHECK_GREATER(n, 0))
            return;
        if (!CHECK_LESS_EQUAL(n, s_max_header_size + 0))
            return;
        if (!CHECK_EQUAL(m_header_buffer[n - 1], '\n'))
            return;
        MemoryInputStream in;
        in.set_buffer(m_header_buffer, m_header_buffer + (n - 1));
        in.unsetf(std::ios_base::skipws);
        std::string message_type;
        in >> message_type;
        if (!CHECK_EQUAL(message_type, "echo"))
            return;
        char sp;
        in >> sp >> m_body_size;
        if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
            return;
        auto handler = [this](std::error_code handler_ec, size_t handler_n) {
            handle_read_body(handler_ec, handler_n);
        };
        m_body_buffer.reset(new char[m_body_size]);
        m_socket.async_read(m_body_buffer.get(), m_body_size, m_read_ahead_buffer, handler);
    }

    void handle_read_body(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestContext& test_context = m_test_context;
        if (!CHECK_EQUAL(n, m_body_size))
            return;
        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer + s_max_header_size);
        out << "was " << m_body_size << '\n';
        auto handler = [this](std::error_code handler_ec, size_t) {
            handle_write_header(handler_ec);
        };
        m_socket.async_write(m_header_buffer, out.size(), handler);
    }

    void handle_write_header(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [this](std::error_code handler_ec, size_t) {
            handle_write_body(handler_ec);
        };
        m_socket.async_write(m_body_buffer.get(), m_body_size, handler);
    }

    void handle_write_body(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [this](std::error_code handler_ec, size_t) {
            handle_read_header_2(handler_ec);
        };
        m_socket.async_read_until(m_header_buffer, s_max_header_size, '\n', m_read_ahead_buffer, handler);
    }

    void handle_read_header_2(std::error_code ec)
    {
        if (ec && ec != MiscExtErrors::end_of_input)
            throw std::system_error(ec);
        unit_test::TestContext& test_context = m_test_context;
        CHECK(ec == MiscExtErrors::end_of_input);
    }
};


class async_client {
public:
    async_client(unsigned short listen_port, unit_test::TestContext& test_context)
        : m_listen_port(listen_port)
        , m_socket(m_service)
        , m_test_context(test_context)
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
        m_socket.set_option(network::SocketBase::no_delay(true));

        MemoryOutputStream out;
        out.set_buffer(m_header_buffer, m_header_buffer + s_max_header_size);
        out << "echo " << sizeof echo_body << '\n';
        auto handler = [this](std::error_code ec, size_t) {
            handle_write_header(ec);
        };
        m_socket.async_write(m_header_buffer, out.size(), handler);

        m_service.run();

        m_socket.close();
    }

private:
    unsigned short m_listen_port;
    network::Service m_service;
    network::Socket m_socket;
    network::ReadAheadBuffer m_read_ahead_buffer;
    static const size_t s_max_header_size = 32;
    char m_header_buffer[s_max_header_size];
    size_t m_body_size;
    std::unique_ptr<char[]> m_body_buffer;
    unit_test::TestContext& m_test_context;

    void handle_write_header(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [this](std::error_code handler_ec, size_t) {
            handle_write_body(handler_ec);
        };
        m_socket.async_write(echo_body, sizeof echo_body, handler);
    }

    void handle_write_body(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        auto handler = [this](std::error_code handler_ec, size_t handler_n) {
            handle_read_header(handler_ec, handler_n);
        };
        m_socket.async_read_until(m_header_buffer, s_max_header_size, '\n', m_read_ahead_buffer, handler);
    }

    void handle_read_header(std::error_code ec, size_t n)
    {
        if (ec)
            throw std::system_error(ec);
        unit_test::TestContext& test_context = m_test_context;
        if (!CHECK_GREATER(n, 0))
            return;
        if (!CHECK_LESS_EQUAL(n, s_max_header_size + 0))
            return;
        if (!CHECK_EQUAL(m_header_buffer[n - 1], '\n'))
            return;
        MemoryInputStream in;
        in.set_buffer(m_header_buffer, m_header_buffer + (n - 1));
        in.unsetf(std::ios_base::skipws);
        std::string message_type;
        in >> message_type;
        if (!CHECK_EQUAL(message_type, "was"))
            return;
        char sp;
        in >> sp >> m_body_size;
        if (!CHECK(in) || !CHECK(in.eof()) || !CHECK_EQUAL(sp, ' '))
            return;
        auto handler = [this](std::error_code handler_ec, size_t handler_n) {
            handle_read_body(handler_ec, handler_n);
        };
        m_body_buffer.reset(new char[m_body_size]);
        m_socket.async_read(m_body_buffer.get(), m_body_size, m_read_ahead_buffer, handler);
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


TEST(Network_Async)
{
    async_server server(test_context);
    unsigned short listen_port = server.init();
    async_client client(listen_port, test_context);

    ThreadWrapper server_thread, client_thread;
    server_thread.start([&] {
        server.run();
    });
    client_thread.start([&] {
        client.run();
    });
    CHECK_NOT(client_thread.join());
    CHECK_NOT(server_thread.join());
}


TEST(Network_HeavyAsyncPost)
{
    network::Service service;
    network::DeadlineTimer dummy_timer{service};
    dummy_timer.async_wait(std::chrono::hours(10000), [](std::error_code) {});

    ThreadWrapper looper_thread;
    looper_thread.start([&] {
        service.run();
    });

    std::vector<std::pair<int, long>> entries;
    const long num_iterations = 10000L;
    auto func = [&](int thread_index) {
        for (long i = 0; i < num_iterations; ++i)
            service.post([&entries, thread_index, i] {
                entries.emplace_back(thread_index, i);
            });
    };

    const int num_threads = 8;
    std::unique_ptr<ThreadWrapper[]> threads(new ThreadWrapper[num_threads]);
    for (int i = 0; i < num_threads; ++i)
        threads[i].start([&func, i] {
            func(i);
        });
    for (int i = 0; i < num_threads; ++i)
        CHECK_NOT(threads[i].join());

    service.post([&] {
        dummy_timer.cancel();
    });
    CHECK_NOT(looper_thread.join());

    // Check that every post operation ran exactly once
    using longlong = long long;
    if (CHECK_EQUAL(num_threads * longlong(num_iterations), entries.size())) {
        bool every_post_operation_ran_exactly_once = true;
        std::sort(entries.begin(), entries.end());
        auto i = entries.begin();
        for (int i_1 = 0; i_1 < num_threads; ++i_1) {
            for (long i_2 = 0; i_2 < num_iterations; ++i_2) {
                int thread_index = i->first;
                long iteration_index = i->second;
                if (i_1 != thread_index || i_2 != iteration_index) {
                    every_post_operation_ran_exactly_once = false;
                    break;
                }
                ++i;
            }
        }
        CHECK(every_post_operation_ran_exactly_once);
    }
}


TEST(Network_RepeatedCancelAndRestartRead)
{
    Random random{random_int<unsigned long>()}; // Seed from slow global generator
    for (int i = 0; i < 1; ++i) {
        network::Service service_1, service_2;
        network::Socket socket_1{service_1}, socket_2{service_2};
        connect_sockets(socket_1, socket_2);
        network::ReadAheadBuffer rab;

        const size_t read_buffer_size = 1024;
        char read_buffer[read_buffer_size];
        size_t num_bytes_read = 0;
        bool end_of_input_seen = false;
        std::function<void()> initiate_read = [&] {
            auto handler = [&](std::error_code ec, size_t n) {
                num_bytes_read += n;
                if (ec == MiscExtErrors::end_of_input) {
                    end_of_input_seen = true;
                    return;
                }
                CHECK(!ec || ec == error::operation_aborted);
                initiate_read();
            };
            socket_2.async_read(read_buffer, read_buffer_size, rab, handler);
        };
        initiate_read();

        auto thread_func = [&] {
            try {
                service_2.run();
            }
            catch (...) {
                socket_2.close();
                throw;
            }
        };
        ThreadWrapper thread;
        thread.start(thread_func);

        const size_t write_buffer_size = 1024;
        const char write_buffer[write_buffer_size] = {'\0'};
        size_t num_bytes_to_write = 0x4000000; // 64 MiB
        size_t num_bytes_written = 0;
        while (num_bytes_written < num_bytes_to_write) {
            size_t n =
                std::min(random.draw_int<size_t>(1, write_buffer_size), num_bytes_to_write - num_bytes_written);
            socket_1.write(write_buffer, n);
            num_bytes_written += n;
            service_2.post([&] {
                socket_2.cancel();
            });
        }
        socket_1.close();

        CHECK_NOT(thread.join());
        CHECK_EQUAL(num_bytes_written, num_bytes_read);
    }
}


TEST(Network_StressTest)
{
    network::Service service_1, service_2;
    network::Socket socket_1{service_1}, socket_2{service_2};
    connect_sockets(socket_1, socket_2);
    constexpr size_t original_size = 0x100000; // 1MiB
    std::unique_ptr<char[]> original_1, original_2;
    original_1.reset(new char[original_size]);
    original_2.reset(new char[original_size]);
    {
        std::mt19937_64 prng{std::random_device()()};
        std::uniform_int_distribution<int> dist(std::numeric_limits<char>::min(), std::numeric_limits<char>::max());
        log("Initializing...");
        for (size_t i = 0; i < original_size; ++i)
            original_1[i] = dist(prng);
        for (size_t i = 0; i < original_size; ++i)
            original_2[i] = dist(prng);
        log("Initialized");
    }

    struct Stats {
        std::uint_fast64_t num_cancellations = 0;
        std::uint_fast64_t num_reads = 0, num_canceled_reads = 0;
        std::uint_fast64_t num_writes = 0, num_canceled_writes = 0;
    };

#ifdef _WIN32 // slow
    constexpr int num_cycles = 16;
#else
    constexpr int num_cycles = 512;
#endif
    auto thread = [&](int id, network::Socket& socket, const char* read_original, const char* write_original,
                      Stats& stats) {
        std::unique_ptr<char[]> read_buffer{new char[original_size]};
        std::mt19937_64 prng{std::random_device()()};
        std::uniform_int_distribution<size_t> read_write_size_dist(1, 32 * 1024);
        std::uniform_int_distribution<int> delayed_read_write_dist(0, 49);
        network::Service& service = socket.get_service();
        network::DeadlineTimer cancellation_timer{service};
        network::DeadlineTimer read_timer{service};
        network::DeadlineTimer write_timer{service};
        std::uint_fast64_t microseconds_per_cancellation = 10;
        bool progress = false;
        bool read_done = false, write_done = false;
        std::function<void()> shedule_cancellation = [&] {
            auto handler = [&](std::error_code ec) {
                REALM_ASSERT(!ec || ec == error::operation_aborted);
                if (ec == error::operation_aborted)
                    return;
                if (read_done && write_done)
                    return;
                socket.cancel();
                ++stats.num_cancellations;
                shedule_cancellation();
            };
            if (progress) {
                microseconds_per_cancellation /= 2;
                progress = false;
            }
            else {
                microseconds_per_cancellation *= 2;
            }
            if (microseconds_per_cancellation < 10)
                microseconds_per_cancellation = 10;
            cancellation_timer.async_wait(std::chrono::microseconds(microseconds_per_cancellation),
                                          std::move(handler));
        };
        shedule_cancellation();
        char* read_begin = read_buffer.get();
        char* read_end = read_buffer.get() + original_size;
        int num_read_cycles = 0;
        std::function<void()> read = [&] {
            if (read_begin == read_end) {
                //                log("<R%1>", id);
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
                    progress = true;
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
            socket.async_read_some(buffer, size, std::move(handler));
        };
        read();
        const char* write_begin = write_original;
        const char* write_end = write_original + original_size;
        int num_write_cycles = 0;
        std::function<void()> write = [&] {
            if (write_begin == write_end) {
                //                log("<W%1>", id);
                ++num_write_cycles;
                if (num_write_cycles == num_cycles) {
                    log("End of write %1", id);
                    write_done = true;
                    if (read_done)
                        cancellation_timer.cancel();
                    socket.shutdown(network::Socket::shutdown_send);
                    log("Properly shut down %1", id);
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
                    progress = true;
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
            socket.async_write_some(data, size, std::move(handler));
        };
        write();
        service.run();
    };

    Stats stats_1, stats_2;
    std::thread thread_1{[&] {
        thread(1, socket_1, original_1.get(), original_2.get(), stats_1);
    }};
    std::thread thread_2{[&] {
        thread(2, socket_2, original_2.get(), original_1.get(), stats_2);
    }};
    thread_1.join();
    thread_2.join();

    char ch;
    CHECK_SYSTEM_ERROR(socket_1.read_some(&ch, 1), MiscExtErrors::end_of_input);
    CHECK_SYSTEM_ERROR(socket_2.read_some(&ch, 1), MiscExtErrors::end_of_input);

    log("Cancellations: %1, %2", stats_1.num_cancellations, stats_2.num_cancellations);
    log("Reads:  %1 (%2 canceled), %3 (%4 canceled)", stats_1.num_reads, stats_1.num_canceled_reads,
        stats_2.num_reads, stats_2.num_canceled_reads);
    log("Writes: %1 (%2 canceled), %3 (%4 canceled)", stats_1.num_writes, stats_1.num_canceled_writes,
        stats_2.num_writes, stats_2.num_canceled_writes);
}


TEST(Network_Trigger_Basics)
{
    network::Service service;

    // Check that triggering works
    bool was_triggered = false;
    auto func = [&] {
        was_triggered = true;
    };
    network::Trigger trigger{service, std::move(func)};
    trigger.trigger();
    service.run();
    CHECK(was_triggered);

    // Check that the function is not called without triggering
    was_triggered = false;
    service.run();
    CHECK_NOT(was_triggered);

    // Check double-triggering
    was_triggered = false;
    trigger.trigger();
    trigger.trigger();
    service.run();
    CHECK(was_triggered);

    // Check that retriggering from triggered function works
    std::function<void()> func_2;
    network::Trigger trigger_2{service, [&] {
                                   func_2();
                               }};
    was_triggered = false;
    bool was_triggered_twice = false;
    func_2 = [&] {
        if (was_triggered) {
            was_triggered_twice = true;
        }
        else {
            was_triggered = true;
            trigger_2.trigger();
        }
    };
    trigger_2.trigger();
    service.run();
    CHECK(was_triggered_twice);

    // Check that the function is not called adfter destruction of the Trigger
    // object
    was_triggered = false;
    {
        auto func_3 = [&] {
            was_triggered = true;
        };
        network::Trigger trigger_3{service, std::move(func_3)};
        trigger_3.trigger();
    }
    service.run();
    CHECK_NOT(was_triggered);

    // Check that two functions can be triggered in an overlapping fashion
    bool was_triggered_4 = false;
    bool was_triggered_5 = false;
    auto func_4 = [&] {
        was_triggered_4 = true;
    };
    auto func_5 = [&] {
        was_triggered_5 = true;
    };
    network::Trigger trigger_4{service, std::move(func_4)};
    network::Trigger trigger_5{service, std::move(func_5)};
    trigger_4.trigger();
    trigger_5.trigger();
    service.run();
    CHECK(was_triggered_4);
    CHECK(was_triggered_5);
}


TEST(Network_Trigger_ThreadSafety)
{
    network::Service service;
    network::DeadlineTimer keep_alive{service};
    keep_alive.async_wait(std::chrono::hours(10000), [](std::error_code) {});
    long n_1 = 0, n_2 = 0;
    std::atomic<bool> flag{false};
    auto func = [&] {
        ++n_1;
        if (flag)
            ++n_2;
    };
    network::Trigger trigger{service, std::move(func)};
    ThreadWrapper thread;
    thread.start([&] {
        service.run();
    });
    long m = 1000000;
    for (long i = 0; i < m; ++i)
        trigger.trigger();
    flag = true;
    trigger.trigger();
    service.post([&] {
        keep_alive.cancel();
    });
    CHECK_NOT(thread.join());
    CHECK_GREATER_EQUAL(n_1, 1);
    CHECK_LESS_EQUAL(n_1, m + 1);
    CHECK_GREATER_EQUAL(n_2, 1);
    CHECK_LESS_EQUAL(n_2, 2);
}


TEST(Network_AsyncResolve_Basics)
{
    network::Service service;
    network::Resolver resolver{service};
    network::Resolver::Query query("localhost", "");
    bool was_called = false;
    auto handler = [&](std::error_code ec, network::Endpoint::List endpoints) {
        CHECK_NOT(ec);
        CHECK_GREATER(endpoints.size(), 0);
        was_called = true;
    };
    resolver.async_resolve(query, std::move(handler));
    service.run();
    CHECK(was_called);
}


TEST(Network_AsyncResolve_Cancellation)
{
    network::Service service;
    network::Resolver resolver{service};
    network::Resolver::Query query("localhost", "");
    bool was_called = false;
    auto handler = [&](std::error_code ec, network::Endpoint::List) {
        CHECK_EQUAL(error::operation_aborted, ec);
        was_called = true;
    };
    resolver.async_resolve(query, std::move(handler));
    resolver.cancel();
    service.run();
    CHECK(was_called);
}

} // unnamed namespace
