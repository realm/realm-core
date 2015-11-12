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
    Socket(network::io_service& io_service, const Endpoint& endpoint, EventLoopBase::OnConnectComplete on_complete):
        m_socket(io_service),
        m_stream(m_socket)
    {
        m_socket.async_connect(endpoint, std::move(on_complete));
    }

    ~Socket()
    {
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

    network::socket m_socket;
    network::buffered_input_stream m_stream;
};

std::unique_ptr<SocketBase>
EventLoop<ASIO>::async_connect(const Endpoint& endpoint, EventLoopBase::OnConnectComplete on_complete)
{
    return std::unique_ptr<SocketBase>{new Socket{m_io_service, endpoint, std::move(on_complete)}};
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


struct EventLoop<ASIO>::Resolver: ResolverBase {
    Resolver(EventLoop<ASIO>& loop, DNSQuery query, EndpointList& endpoints, EventLoopBase::OnResolveComplete on_complete):
        m_on_complete(std::move(on_complete))
    {
        m_timer = loop.async_timer(Duration::zero(), [=](std::error_code ec) { this->complete(ec); });
        network::resolver resolver{loop.m_io_service};
        resolver.resolve(query, endpoints, m_ec);
    }

    void cancel() final
    {
        // FIXME: No-op until resolve becomes an actual async operation.
    }

    void complete(std::error_code ec)
    {
        if (!ec) {
            ec = m_ec;
        }
        m_on_complete(m_ec);
    }

    std::unique_ptr<DeadlineTimerBase> m_timer;
    std::error_code m_ec;
    EventLoopBase::OnResolveComplete m_on_complete;
};

std::unique_ptr<ResolverBase>
EventLoop<ASIO>::async_resolve(DNSQuery query, EndpointList& endpoints, EventLoopBase::OnResolveComplete on_complete)
{
    return std::unique_ptr<ResolverBase>{new Resolver{*this, query, endpoints, std::move(on_complete)}};
}

}
}

