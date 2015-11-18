#include "realm/util/event_loop.hpp"
#include "realm/util/network.hpp"


namespace realm {
namespace util {


EventLoop<ASIO>::EventLoop() {}
EventLoop<ASIO>::~EventLoop() {}

void EventLoop<ASIO>::run()
{
    m_io_service.run();
}

void EventLoop<ASIO>::stop() noexcept
{
    m_io_service.stop();
}

void EventLoop<ASIO>::reset() noexcept
{
    m_io_service.reset();
}

struct EventLoop<ASIO>::Socket: SocketBase {
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
EventLoop<ASIO>::async_connect(std::string host, int port, EventLoopBase::OnConnectComplete on_complete)
{
    return std::unique_ptr<SocketBase>{new Socket{m_io_service, std::move(host), port, std::move(on_complete)}};
}


struct EventLoop<ASIO>::DeadlineTimer: DeadlineTimerBase {
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
EventLoop<ASIO>::async_timer(EventLoopBase::Duration delay, EventLoopBase::OnTimeout on_timeout)
{
    return std::unique_ptr<DeadlineTimerBase>{new DeadlineTimer{m_io_service, delay, std::move(on_timeout)}};
}


} // namespace util
} // namespace realm

