#include "test.hpp"
#include "util/semaphore.hpp"

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


#if REALM_PLATFORM_APPLE && !REALM_WATCHOS
// Apple Core Foundation based implementation currently disabled because it is
// broken.
//#  define IMPLEMENTATIONS Posix, AppleCoreFoundation
#  define IMPLEMENTATIONS Posix
#else
#  define IMPLEMENTATIONS Posix
#endif



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

void connect_sockets(network::socket& socket_1, Socket& socket_2)
{
    network::io_service& service = socket_1.service();
    network::acceptor acceptor(service);
    network::endpoint ep = bind_acceptor(acceptor);
    acceptor.listen();
    bool connect_completed = false;
    std::error_code ec_1, ec_2;
    acceptor.async_accept(socket_1, [&](std::error_code ec) { ec_1 = ec; });
    socket_2.async_connect("localhost", ep.port(), SocketSecurity::None,
                           [&](std::error_code ec) { ec_2 = ec; connect_completed = true; });
    ThreadWrapper thread;
    thread.start([&] { service.run(); });
    EventLoop& event_loop = socket_2.get_event_loop();
    event_loop.run();
    bool exception_in_thread = thread.join(); // FIXME: Transport exception instead
    REALM_ASSERT(!exception_in_thread);
    REALM_ASSERT(connect_completed);
    if (ec_1)
        throw std::system_error(ec_1);
    if (ec_2)
        throw std::system_error(ec_2);
}


class PingPongFixture {
public:
    PingPongFixture(network::io_service& service, EventLoop& event_loop):
        m_server_socket(service),
        m_client_socket(event_loop.make_socket())
    {
        connect_sockets(m_server_socket, *m_client_socket);
    }

    void start()
    {
        initiate_server_read();
    }

    void delay(int n, std::function<void()> handler)
    {
        m_handler = std::move(handler);
        m_num = n;
        initiate_client_write();
    }

private:
    network::socket m_server_socket;
    network::buffered_input_stream m_server_input_stream{m_server_socket};
    const std::unique_ptr<Socket> m_client_socket;
    char m_server_char = 0, m_client_char = 0;
    int m_num;
    std::function<void()> m_handler;

    void initiate_server_read()
    {
        auto handler = [this](std::error_code ec, size_t) {
            if (ec != error::operation_aborted)
                handle_server_read(ec);
        };
        m_server_input_stream.async_read(&m_server_char, 1, std::move(handler));
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
        m_client_socket->async_write(&m_client_char, 1, std::move(handler));
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
        m_client_socket->async_read(&m_client_char, 1, std::move(handler));
    }

    void handle_client_read(std::error_code ec)
    {
        if (ec)
            throw std::system_error(ec);
        initiate_client_write();
    }
};


// Try to fill the kernel level write buffer of a socket under the assumption
// the no one is reading from the remote side. It may fail to fill the buffer,
// but it should be exepcted to succeed often.
void fill_kernel_write_buffer(Socket& socket, network::io_service& service)
{
    EventLoop& event_loop = socket.get_event_loop();
    PingPongFixture ping_pong(service, event_loop);
    ping_pong.start();
    int num_successive_no_bytes_written = 0;
    char data[4096] = {};
    std::function<void()> initiate_write = [&] {
        auto handler = [&](std::error_code ec, size_t n) {
            if (ec && ec != error::operation_aborted)
                throw std::runtime_error("fill_kernel_write_buffer: Bad write");
            if (n > 0) {
                num_successive_no_bytes_written = 0;
            }
            else {
                ++num_successive_no_bytes_written;
                if (num_successive_no_bytes_written == 16) {
                    event_loop.stop();
                    return;
                }
            }
            initiate_write();
        };
        socket.async_write(data, sizeof data, std::move(handler));
    };
    std::function<void()> initiate_ping_pong = [&] {
        auto handler = [&] {
            socket.cancel();
            initiate_ping_pong();
        };
        ping_pong.delay(5, std::move(handler));
    };
    initiate_write();
    initiate_ping_pong();
    ThreadWrapper thread;
    thread.start([&] { service.run(); });
    event_loop.run();
    service.stop();
    if (thread.join())
        throw std::runtime_error("fill_kernel_write_buffer: Exception in thread");
    service.reset();
    event_loop.reset();
}



TEST_TYPES(EventLoop_Post_Basics, IMPLEMENTATIONS)
{
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    // Check that we can post while the event loop is not executing
    {
        int n = 0;
        event_loop->post([&] { ++n; });
        event_loop->run();
        CHECK_EQUAL(1, n);
    }

    // Check that we can post from a post handler (from the event loop thread).
    {
        int n = 0;
        event_loop->post([&] { event_loop->post([&] { ++n; }); });
        event_loop->run();
        CHECK_EQUAL(1, n);
    }

    // Check that post handlers are executed in the order that they are
    // submitted
    {
        std::vector<std::pair<int, int>> v;
        auto handler = [&](int i) {
            v.emplace_back(i, 0);
            event_loop->post([&v, i] { v.emplace_back(i, 1); });
        };
        int n = 100;
        for (int i = 0; i < n; ++i) {
            event_loop->post([=] { handler(i); });
        }
        event_loop->run();
        if (CHECK_EQUAL(2*n, v.size())) {
            size_t index = 0;
            for (int j = 0; j < 2; ++j) {
                for (int i = 0; i < n; ++i) {
                    CHECK_EQUAL(i, v[index].first);
                    CHECK_EQUAL(j, v[index].second);
                    ++index;
                }
            }
        }
    }

    // Check that post handlers are not executed directly from post()
    {
        int var_1 = 381, var_2 = 743;
        event_loop->post([&] { var_1 = 824; });
        event_loop->post([&] { var_2 = 216; });
        CHECK_EQUAL(var_1, 381);
        CHECK_EQUAL(var_2, 743);
        event_loop->run();
        CHECK_EQUAL(var_1, 824);
        CHECK_EQUAL(var_2, 216);
        event_loop->post([&] { var_2 = 191; });
        event_loop->post([&] { var_1 = 476; });
        CHECK_EQUAL(var_1, 824);
        CHECK_EQUAL(var_2, 216);
        event_loop->run();
        CHECK_EQUAL(var_1, 476);
        CHECK_EQUAL(var_2, 191);
    }
}


TEST_TYPES(EventLoop_Post_MultiThreaded, IMPLEMENTATIONS)
{
    // Introduce a blocking operation that will keep the event loop running
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
    timer->async_wait(std::chrono::hours(1000000), [](std::error_code) {});

    // Start event loop execution in the background
    ThreadWrapper event_loop_thread;
    event_loop_thread.start([&] { event_loop->run(); });

    // Wait for the event loop thread to get properly started
    BowlOfStonesSemaphore bowl_1;
    event_loop->post([&] { bowl_1.add_stone(); });
    bowl_1.get_stone();

    std::vector<std::pair<int, int>> vec_1, vec_2;

    int num_posts_per_thread = 4096;
    auto handler = [&](int thread_index) {
        for (int i = 0; i < num_posts_per_thread; ++i)
            event_loop->post([&vec_1, thread_index, i] { vec_1.emplace_back(thread_index, i); });
    };
    const int num_threads = 16;
    ThreadWrapper threads[num_threads];
    for (int i = 0; i < num_threads; ++i)
        threads[i].start([=] { handler(i); });
    for (int i = 0; i < num_threads; ++i)
        CHECK_NOT(threads[i].join());

    // Stop the event loop by canceling the blocking operation
    event_loop->post([&] { timer->cancel(); });
    CHECK_NOT(event_loop_thread.join());

    std::sort(vec_1.begin(), vec_1.end());

    for (int i = 0; i < num_threads; ++i) {
        for (int j = 0; j < num_posts_per_thread; ++j)
            vec_2.emplace_back(i,j);
    }

    CHECK(vec_1 == vec_2);
}


TEST_TYPES(EventLoop_StopAndReset_Basics, IMPLEMENTATIONS)
{
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    // Prestop
    int var = 381;
    event_loop->stop();
    event_loop->post([&]{ var = 824; });
    event_loop->run(); // Must return immediately
    CHECK_EQUAL(var, 381);
    event_loop->run(); // Must still return immediately
    CHECK_EQUAL(var, 381);

    // Reset
    event_loop->reset();
    event_loop->post([&]{ var = 824; });
    CHECK_EQUAL(var, 381);
    event_loop->run();
    CHECK_EQUAL(var, 824);
    event_loop->post([&]{ var = 476; });
    CHECK_EQUAL(var, 824);
    event_loop->run();
    CHECK_EQUAL(var, 476);
}


TEST_TYPES(EventLoop_StopAndReset_Advanced, IMPLEMENTATIONS)
{
    // Introduce a blocking operation that will keep the event loop running
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
    timer->async_wait(std::chrono::hours(1000000), [](std::error_code) {});

    // Start event loop execution in the background
    ThreadWrapper thread_1;
    thread_1.start([&] { event_loop->run(); });

    // Check that the event loop is actually running
    BowlOfStonesSemaphore bowl_1; // Empty
    event_loop->post([&] { bowl_1.add_stone(); });
    bowl_1.get_stone(); // Block until the stone is added

    // Stop the event loop
    event_loop->stop();
    CHECK_NOT(thread_1.join());

    // Check that the event loop remains in the stopped state
    int var = 381;
    event_loop->post([&] { var = 824; });
    CHECK_EQUAL(var, 381);
    event_loop->run(); // Still stopped, so run() must return immediately
    CHECK_EQUAL(var, 381);

    // Put the event loop back into the unstopped state, and restart it in the
    // background
    event_loop->reset();
    ThreadWrapper thread_2;
    thread_2.start([&] { event_loop->run(); });

    // Check that the event loop is actually running
    BowlOfStonesSemaphore bowl_2; // Empty
    event_loop->post([&] { bowl_2.add_stone(); });
    bowl_2.get_stone(); // Block until the stone is added

    // Stop the event loop by canceling the blocking operation
    event_loop->post([&] { timer->cancel(); });
    CHECK_NOT(thread_2.join());

    CHECK_EQUAL(var, 824);
}


TEST_TYPES(EventLoop_Connect_Basics, IMPLEMENTATIONS)
{
    network::io_service service;
    network::socket socket_1(service);
    network::acceptor acceptor(service);
    network::endpoint ep = bind_acceptor(acceptor);
    acceptor.listen();

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket_2 = event_loop->make_socket();

    std::error_code ec_1, ec_2;
    acceptor.async_accept(socket_1, [&](std::error_code ec) { ec_1 = ec; });
    socket_2->async_connect("localhost", ep.port(), SocketSecurity::None,
                            [&](std::error_code ec) { ec_2 = ec; });
    ThreadWrapper thread;
    thread.start([&] { service.run(); });
    event_loop->run();
    CHECK_NOT(thread.join());

    CHECK_NOT(ec_1);
    CHECK_NOT(ec_2);
}


TEST_TYPES(EventLoop_Connect_Failure, IMPLEMENTATIONS)
{
    network::io_service service;
    network::socket socket_1(service);
    network::acceptor acceptor(service);
    network::endpoint ep = bind_acceptor(acceptor);
    acceptor.listen();
    network::socket socket_2(service);
    {
        std::error_code ec_1, ec_2;
        acceptor.async_accept(socket_1, [&](std::error_code ec) { ec_1 = ec; });
        socket_2.async_connect(ep, [&](std::error_code ec) { ec_2 = ec; });
        service.run();
        CHECK_NOT(ec_1);
        CHECK_NOT(ec_2);
        ep = socket_2.local_endpoint();
    }

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket_3 = event_loop->make_socket();

    std::error_code ec;
    socket_3->async_connect("localhost", ep.port(), SocketSecurity::None,
                            [&](std::error_code ec_2) { ec = ec_2; });
    event_loop->run();
    CHECK(ec);
}


TEST_TYPES(EventLoop_Connect_EarlyFailureAndCancel, IMPLEMENTATIONS)
{
    // Check that an early failure (during DNS lookup) does not make the
    // operation uncancaleable.

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket = event_loop->make_socket();

    std::string bad_host_name = "G6iHvNo9dEVPfL3EcWHWSChQU3GD8cjEGuy92eeTeaVWGPt71kJa5MykiYHGlg3M";

    std::error_code ec;
    socket->async_connect(std::move(bad_host_name), 0, SocketSecurity::None,
                          [&](std::error_code ec_2) { ec = ec_2; });
    socket->cancel();
    event_loop->run();
    CHECK_EQUAL(error::operation_aborted, ec);
}


TEST_TYPES(EventLoop_Read_NoBytesAvailable, IMPLEMENTATIONS)
{
    network::io_service service;
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    const int num_sockets = 16;
    std::unique_ptr<network::socket> remote_sockets[num_sockets];
    std::unique_ptr<Socket> sockets[num_sockets];

    for (int i = 0; i < num_sockets; ++i) {
        remote_sockets[i].reset(new network::socket(service));
        sockets[i] = event_loop->make_socket();
        connect_sockets(*remote_sockets[i], *sockets[i]);
    }

    // Empty reads must never block
    {
        int remaining_reads[num_sockets];
        for (int i = 0; i < num_sockets; ++i)
            remaining_reads[i] = 256;
        bool error = false;
        std::function<void(int)> initiate_read = [&](int i) {
            if (remaining_reads[i] == 0)
                return;
            --remaining_reads[i];
            auto handler = [&error, &initiate_read, i](std::error_code ec, size_t) {
                if (ec) {
                    error = true;
                    return;
                }
                initiate_read(i);
            };
            sockets[i]->async_read(nullptr, 0, std::move(handler));
        };
        for (int i = 0; i < num_sockets; ++i)
            initiate_read(i);
        event_loop->run();
        CHECK_NOT(error);
    }

    // Nonempty reads must always block
    {
        int remaining_reads[num_sockets];
        for (int i = 0; i < num_sockets; ++i)
            remaining_reads[i] = 64;
        std::unique_ptr<PingPongFixture> ping_pongs[num_sockets];
        for (int i = 0; i < num_sockets; ++i)
            ping_pongs[i].reset(new PingPongFixture(service, *event_loop));
        for (int i = 0; i < num_sockets; ++i)
            ping_pongs[i]->start();
        char ch;
        bool error = false;
        std::function<void(int)> initiate_read = [&](int i) {
            if (remaining_reads[i] == 0)
                return;
            --remaining_reads[i];
            auto handler = [&error, &initiate_read, i](std::error_code ec, size_t) {
                if (ec != error::operation_aborted) {
                    error = true;
                    return;
                }
                initiate_read(i);
            };
            sockets[i]->async_read(&ch, 1, std::move(handler));
            ping_pongs[i]->delay(3, [&sockets, i] { sockets[i]->cancel(); });
        };
        for (int i = 0; i < num_sockets; ++i)
            initiate_read(i);
        ThreadWrapper thread;
        thread.start([&] { service.run(); });
        event_loop->run();
        service.stop();
        CHECK_NOT(thread.join());
        service.reset();
        CHECK_NOT(error);
    }
}


TEST_TYPES(EventLoop_Write_NoBytesAccepted, IMPLEMENTATIONS)
{
    network::io_service service;
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    const int num_sockets = 16;
    std::unique_ptr<network::socket> remote_sockets[num_sockets];
    std::unique_ptr<Socket> sockets[num_sockets];

    for (int i = 0; i < num_sockets; ++i) {
        remote_sockets[i].reset(new network::socket(service));
        sockets[i] = event_loop->make_socket();
        connect_sockets(*remote_sockets[i], *sockets[i]);
        fill_kernel_write_buffer(*sockets[i], service);
    }

    // Empty writes must never block
    {
        int remaining_writes[num_sockets];
        for (int i = 0; i < num_sockets; ++i)
            remaining_writes[i] = 256;
        bool error = false;
        std::function<void(int)> initiate_write = [&](int i) {
            if (remaining_writes[i] == 0)
                return;
            --remaining_writes[i];
            auto handler = [&error, &initiate_write, i](std::error_code ec, size_t) {
                if (ec) {
                    error = true;
                    return;
                }
                initiate_write(i);
            };
            sockets[i]->async_write(nullptr, 0, std::move(handler));
        };
        for (int i = 0; i < num_sockets; ++i)
            initiate_write(i);
        event_loop->run();
        CHECK_NOT(error);
    }
}


TEST_TYPES(EventLoop_Write_AfterCloseByRemotePeer, IMPLEMENTATIONS)
{
    network::io_service service;
    network::socket socket_1(service);

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket_2 = event_loop->make_socket();

    connect_sockets(socket_1, *socket_2);

    auto run = [&] {
        for (int i = 0; i < 10000; ++i) {
            char ch;
            socket_1.read_some(&ch, 1);
        }
        socket_1.close();
    };
    ThreadWrapper thread;
    thread.start(std::move(run));

    std::error_code ec;
    for (int i = 0; i < 16384; ++i) {
        const char data[4096] = { '\0' };
        auto handler = [&](std::error_code ec_2, size_t) {
            if (ec_2)
                ec = ec_2;
        };
        socket_2->async_write(data, sizeof data, std::move(handler));
        event_loop->run();
        if (ec)
            break;
    }

    CHECK_NOT(thread.join());

    CHECK(ec);
}


TEST_TYPES(EventLoop_DeadlineTimer, IMPLEMENTATIONS)
{
    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();

    // Check that the completion handler is executed
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
    timer->async_wait(std::chrono::hours(1000000), wait_handler);
    CHECK(!completed);
    CHECK(!canceled);
    timer->cancel();
    CHECK(!completed);
    CHECK(!canceled);
    event_loop->run();
    CHECK(!completed);
    CHECK(canceled);
}


struct ServerFixture {
    unit_test::TestContext& test_context;
    network::io_service service;
    network::acceptor acceptor{service};
    network::endpoint ep;
    network::socket socket{service};
    network::buffered_input_stream input_stream{socket};
    ThreadWrapper thread;
    ServerFixture(unit_test::TestContext& tc):
        test_context(tc)
    {
        ep = bind_acceptor(acceptor);
        acceptor.listen();
    }
    ~ServerFixture()
    {
        service.stop();
        if (thread.joinable())
            CHECK_NOT(thread.join());
    }
    void start()
    {
        BowlOfStonesSemaphore bowl;
        service.post([&] { bowl.add_stone(); });
        thread.start([&] { service.run(); });
        // Wait for the event loop thread to become active
        bowl.get_stone();
    }
};


TEST_TYPES(EventLoop_Cancellation_Basics, IMPLEMENTATIONS)
{

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    // Connect -> cancel
    {
        ServerFixture server{test_context};
        server.acceptor.async_accept(server.socket, [](std::error_code) {});
        server.start();
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        std::error_code ec;
        socket->async_connect("localhost", server.ep.port(), SocketSecurity::None,
                              [&](std::error_code ec_2) { ec = ec_2; });
        socket->cancel();
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Connect -> destroy
    {
        ServerFixture server{test_context};
        server.acceptor.async_accept(server.socket, [](std::error_code) {});
        server.start();
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        std::error_code ec;
        socket->async_connect("localhost", server.ep.port(), SocketSecurity::None,
                              [&](std::error_code ec_2) { ec = ec_2; });
        socket.reset(); // Implicit close -> cancel
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Read -> cancel
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch = 0;
        server.socket.async_write(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2;
        std::error_code ec;
        socket->async_read(&ch_2, 1, [&](std::error_code ec_2, size_t) { ec = ec_2; });
        socket->cancel();
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Read -> destroy
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch = 0;
        server.socket.async_write(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2;
        std::error_code ec;
        socket->async_read(&ch_2, 1, [&](std::error_code ec_2, size_t) { ec = ec_2; });
        socket.reset(); // Implicit close -> cancel
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Write -> cancel
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch;
        server.input_stream.async_read(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2 = 0;
        std::error_code ec;
        socket->async_write(&ch_2, 1, [&](std::error_code ec_2, size_t) { ec = ec_2; });
        socket->cancel();
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Write -> destroy
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch;
        server.input_stream.async_read(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2 = 0;
        std::error_code ec;
        socket->async_write(&ch_2, 1, [&](std::error_code ec_2, size_t) { ec = ec_2; });
        socket.reset(); // Implicit close -> cancel
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Wait -> cancel
    {
        std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
        std::error_code ec;
        timer->async_wait(std::chrono::hours(1000000), [&](std::error_code ec_2) { ec = ec_2; });
        timer->cancel();
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }

    // Wait -> destroy
    {
        std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
        std::error_code ec;
        timer->async_wait(std::chrono::hours(1000000), [&](std::error_code ec_2) { ec = ec_2; });
        timer.reset(); // Implicit cancel
        event_loop->run();
        CHECK_EQUAL(error::operation_aborted, ec);
    }
}


TEST_TYPES(EventLoop_Cancellation_EmptyRead, IMPLEMENTATIONS)
{
    // Make sure that an immediately completable read operation is still
    // cancelable

    network::io_service service;
    network::socket socket_1(service);

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket_2 = event_loop->make_socket();

    connect_sockets(socket_1, *socket_2);

    const size_t size = 1;
    char data[size] = { 'a' };
    char buffer[size];
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    socket_2->async_read(buffer, 0, read_handler);
    socket_2->cancel();
    event_loop->run();
    CHECK(read_was_canceled);

    // Do it again while a nonempty write is in progress
    read_was_canceled = false;
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2->async_write(data, size, write_handler);
    socket_2->async_read(buffer, 0, read_handler);
    socket_2->cancel();
    event_loop->run();
    CHECK(read_was_canceled);
    CHECK(write_was_canceled);
}


TEST_TYPES(EventLoop_Cancellation_EmptyWrite, IMPLEMENTATIONS)
{
    // Make sure that an immediately completable write operation is still
    // cancelable

    network::io_service service;
    network::socket socket_1(service);

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
    std::unique_ptr<Socket> socket_2 = event_loop->make_socket();

    connect_sockets(socket_1, *socket_2);

    const size_t size = 1;
    char data[size] = { 'a' };
    char buffer[size];
    bool write_was_canceled = false;
    auto write_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            write_was_canceled = true;
    };
    socket_2->async_write(data, 0, write_handler);
    socket_2->cancel();
    event_loop->run();
    CHECK(write_was_canceled);

    // Do it again while a nonempty read is in progress
    write_was_canceled = false;
    bool read_was_canceled = false;
    auto read_handler = [&](std::error_code ec, size_t) {
        if (ec == error::operation_aborted)
            read_was_canceled = true;
    };
    socket_2->async_read(buffer, size, read_handler);
    socket_2->async_write(data, 0, write_handler);
    socket_2->cancel();
    event_loop->run();
    CHECK(write_was_canceled);
    CHECK(read_was_canceled);
}


TEST_TYPES(EventLoop_Cancellation_RepeatedRestartRead, IMPLEMENTATIONS)
{
    Random random{random_int<unsigned long>()}; // Seed from slow global generator
    for (int i = 0; i < 1; ++i) {
        network::io_service service;
        network::socket socket_1(service);
        std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();
        std::unique_ptr<Socket> socket_2 = event_loop->make_socket();
        connect_sockets(socket_1, *socket_2);

        const size_t read_buffer_size = 1024;
        char read_buffer[read_buffer_size];
        size_t num_bytes_read = 0;
        bool end_of_input_seen = false;
        std::function<void()> initiate_read = [&] {
            auto handler = [&](std::error_code ec, size_t n) {
                num_bytes_read += n;
                if (ec == network::errors::end_of_input) {
                    end_of_input_seen = true;
                    return;
                }
                CHECK(!ec || ec == error::operation_aborted);
                initiate_read();
            };
            socket_2->async_read(read_buffer, read_buffer_size, std::move(handler));
        };
        initiate_read();

        auto thread_func = [&] {
            try {
                event_loop->run();
            }
            catch (...) {
                socket_2->close();
                throw;
            }
        };
        ThreadWrapper thread;
        thread.start(thread_func);

        const size_t write_buffer_size = 1024;
        const char write_buffer[write_buffer_size] = { '\0' };
        size_t num_bytes_to_write = 0x4000000; // 64 MiB
        size_t num_bytes_written = 0;
        while (num_bytes_written < num_bytes_to_write) {
            size_t n = std::min(random.draw_int<size_t>(1, write_buffer_size),
                                num_bytes_to_write-num_bytes_written);
            socket_1.write(write_buffer, n);
            num_bytes_written += n;
            event_loop->post([&] { socket_2->cancel(); });
        }
        socket_1.close();

        CHECK_NOT(thread.join());
        CHECK_EQUAL(num_bytes_written, num_bytes_read);
    }
}


TEST_TYPES(EventLoop_ThrowFromHandlers, IMPLEMENTATIONS)
{
    // Check that exceptions can propagate correctly out from any type of
    // completion handler

    std::unique_ptr<EventLoop> event_loop = MakeEventLoop<TEST_TYPE>()();

    // Post
    {
        struct TestException {};
        event_loop->post([] { throw TestException(); });
        CHECK_THROW(event_loop->run(), TestException);
    }

    // Connect
    {
        ServerFixture server{test_context};
        server.acceptor.async_accept(server.socket, [](std::error_code) {});
        server.start();
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        struct TestException {};
        socket->async_connect("localhost", server.ep.port(), SocketSecurity::None,
                              [](std::error_code) { throw TestException(); });
        CHECK_THROW(event_loop->run(), TestException);
    }

    // Read
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch = 0;
        server.socket.async_write(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2;
        struct TestException {};
        socket->async_read(&ch_2, 1, [](std::error_code, size_t) { throw TestException(); });
        CHECK_THROW(event_loop->run(), TestException);
    }

    // Write
    {
        ServerFixture server{test_context};
        std::unique_ptr<Socket> socket = event_loop->make_socket();
        connect_sockets(server.socket, *socket);
        char ch;
        server.input_stream.async_read(&ch, 1, [](std::error_code, size_t) {});
        server.start();
        char ch_2 = 0;
        struct TestException {};
        socket->async_write(&ch_2, 1, [](std::error_code, size_t) { throw TestException(); });
        CHECK_THROW(event_loop->run(), TestException);
    }

    // Wait
    {
        std::unique_ptr<DeadlineTimer> timer = event_loop->make_timer();
        struct TestException {};
        timer->async_wait(std::chrono::seconds(0), [](std::error_code) { throw TestException(); });
        CHECK_THROW(event_loop->run(), TestException);
    }
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
