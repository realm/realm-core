#include "realm/util/event_loop.hpp"
#include "realm/util/network.hpp"
#include "realm/util/optional.hpp"

namespace realm {
namespace util {

class EventLoopPOSIX: public EventLoopBase {
public:
    EventLoopPOSIX();
    ~EventLoopPOSIX();

    void run() override;
    void stop() override;
    void reset();

    std::unique_ptr<SocketBase> async_connect(std::string host, int port, SocketSecurity, OnConnectComplete) final;
    std::unique_ptr<DeadlineTimerBase> async_timer(Duration delay, OnTimeout) final;
    void post(OnPost) final;
protected:
    struct Resolver;
    struct Socket;
    struct DeadlineTimer;

    network::io_service m_io_service;
};


std::unique_ptr<EventLoopBase> get_posix_event_loop()
{
    return std::unique_ptr<EventLoopBase>{new EventLoopPOSIX};
}

#if !REALM_PLATFORM_APPLE
static REALM_THREAD_LOCAL EventLoopPOSIX* g_realm_event_loop = nullptr;
EventLoopBase& get_native_event_loop()
{
    if (g_realm_event_loop == nullptr) {
        g_realm_event_loop = new EventLoopPOSIX;
    }
    return *g_realm_event_loop;
}
#endif


EventLoopPOSIX::EventLoopPOSIX() {}
EventLoopPOSIX::~EventLoopPOSIX() {}

void EventLoopPOSIX::run()
{
    m_io_service.run();
}

void EventLoopPOSIX::stop()
{
    m_io_service.stop();
}

void EventLoopPOSIX::reset()
{
    m_io_service.reset();
}

struct EventLoopPOSIX::Socket: SocketBase {
    Socket(network::io_service& io_service, std::string host, int port, EventLoopBase::OnConnectComplete on_complete):
        m_on_complete(std::move(on_complete)),
        m_socket(io_service),
        m_stream(m_socket)
    {
        std::stringstream ss;
        ss << port;
        network::resolver::query query{host, ss.str()};
        network::resolver resolver{io_service};
        m_last_error = resolver.resolve(query, m_endpoints, m_last_error);

        m_try_endpoint = m_endpoints.begin();

        schedule_next_connection_attempt();
    }

    ~Socket()
    {
    }

    void schedule_next_connection_attempt()
    {
        if (m_try_endpoint != m_endpoints.end()) {
            m_socket.async_connect(*m_try_endpoint, [=](std::error_code ec) {
                m_last_error = ec;
                if (ec) {
                    ++m_try_endpoint;
                    schedule_next_connection_attempt();
                }
                else {
                    m_on_complete(ec);
                }
            });
        }
        else {
            m_on_complete(m_last_error);
        }
    }

    void cancel() final
    {
        m_socket.cancel();
    }

    void close() final
    {
        m_socket.close();
    }

    void async_write(const char* data, size_t size, SocketBase::OnWriteComplete on_complete) final
    {
        m_socket.async_write(data, size, std::move(on_complete));
    }

    void async_read(char* data, size_t size, SocketBase::OnReadComplete on_complete) final
    {
        m_stream.async_read(data, size, std::move(on_complete));
    }

    void async_read_until(char* data, size_t size, char delim, SocketBase::OnReadComplete on_complete) final
    {
        m_stream.async_read_until(data, size, delim, std::move(on_complete));
    }

    OnConnectComplete m_on_complete;
    network::socket m_socket;
    network::buffered_input_stream m_stream;
    network::endpoint::list m_endpoints;
    network::endpoint::list::iterator m_try_endpoint;
    std::error_code m_last_error;
};

std::unique_ptr<SocketBase>
EventLoopPOSIX::async_connect(std::string host, int port, SocketSecurity sec, EventLoopBase::OnConnectComplete on_complete)
{
    REALM_ASSERT_RELEASE(sec == SocketSecurity::None && "Not implemented yet");
    return std::unique_ptr<SocketBase>{new Socket{m_io_service, std::move(host), port, std::move(on_complete)}};
}


struct EventLoopPOSIX::DeadlineTimer: DeadlineTimerBase {
    DeadlineTimer(network::io_service& io_service, EventLoopBase::Duration delay, EventLoopBase::OnTimeout on_timeout):
        m_timer(io_service)
    {
        m_timer.async_wait(delay, std::move(on_timeout));
    }

    void async_wait(Duration delay, EventLoopBase::OnTimeout on_timeout)
    {
        m_timer.async_wait(delay, std::move(on_timeout));
    }

    void cancel() final
    {
        m_timer.cancel();
    }

    network::deadline_timer m_timer;
};

std::unique_ptr<DeadlineTimerBase>
EventLoopPOSIX::async_timer(EventLoopBase::Duration delay, EventLoopBase::OnTimeout on_timeout)
{
    return std::unique_ptr<DeadlineTimerBase>{new DeadlineTimer{m_io_service, delay, std::move(on_timeout)}};
}

void
EventLoopPOSIX::post(OnPost on_post)
{
    m_io_service.post(std::move(on_post));
}


} // namespace util
} // namespace realm

