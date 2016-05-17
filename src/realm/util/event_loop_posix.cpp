#include <realm/util/network.hpp>
#include <realm/util/event_loop.hpp>

using namespace realm;
using namespace realm::util;

namespace {

class SocketImpl;
class DeadlineTimerImpl;


class EventLoopImpl: public EventLoop {
public:
    EventLoopImpl():
        m_io_service() // Throws
    {
    }

    ~EventLoopImpl() noexcept override
    {
    }

    std::unique_ptr<Socket> make_socket() override;
    std::unique_ptr<DeadlineTimer> make_timer() override;

    void post(PostCompletionHandler handler) override
    {
        m_io_service.post(std::move(handler)); // Throws
    }

    void run() override
    {
        m_io_service.run(); // Throws
    }

    void stop() noexcept override
    {
        m_io_service.stop();
    }

    void reset() noexcept override
    {
        m_io_service.reset();
    }

private:
    network::io_service m_io_service;
};



class SocketImpl: public Socket {
public:
    SocketImpl(network::io_service& service):
        m_socket(service), // Throws
        m_input_stream(m_socket) // Throws
    {
    }

    ~SocketImpl() noexcept override
    {
    }

    void async_connect(std::string host, port_type port, SocketSecurity security,
                       ConnectCompletionHandler handler) override
    {
        if (security != SocketSecurity::None)
            throw std::runtime_error("Unsupported socket security");
        if (m_socket.is_open())
            throw std::runtime_error("Already connected");
        // Discard buffered input from when the connection was last established
        m_input_stream.reset();
        std::stringstream out;
        out.imbue(std::locale::classic());
        out << port;
        network::resolver::query query{host, out.str()};
        network::resolver resolver{m_socket.service()};
        // FIXME: Avoid synchronous DNS lookup
        std::error_code ec;
        resolver.resolve(query, m_endpoints, ec);
        if (ec) {
            // Direct callbacks are not allowed
            //
            // FIXME: Use [handler=std::move(handler), ec] in C++14 to avoid a
            // potentially expensive copy of user specified completion handler
            // into the lambda
            auto handler_2 = [=] {
                handler(ec); // Throws
            };
            m_socket.service().post(std::move(handler_2)); // Throws
            return;
        }
        // Try each endpoint in turn
        try_next_endpoint(m_endpoints.begin(), std::move(handler)); // Throws
        m_connect_in_progress = true;
    }

    void async_read(char* data, size_t size, ReadCompletionHandler handler) override
    {
        if (m_connect_in_progress || !m_socket.is_open())
            throw std::runtime_error("Not connected");
        m_input_stream.async_read(data, size, std::move(handler));
    }

    void async_read_until(char* data, size_t size, char delim,
                          ReadCompletionHandler handler) override
    {
        if (m_connect_in_progress || !m_socket.is_open())
            throw std::runtime_error("Not connected");
        m_input_stream.async_read_until(data, size, delim, std::move(handler));
    }

    void async_write(const char* data, size_t size, WriteCompletionHandler handler) override
    {
        if (m_connect_in_progress || !m_socket.is_open())
            throw std::runtime_error("Not connected");
        m_socket.async_write(data, size, std::move(handler));
    }

    void close() noexcept override
    {
        m_socket.close();
    }

    void cancel() noexcept override
    {
        if (m_connect_in_progress) {
            m_socket.close();
            m_connect_in_progress = false;
        }
        else {
            m_socket.cancel();
        }
    }

private:
    network::socket m_socket;
    network::buffered_input_stream m_input_stream;
    network::endpoint::list m_endpoints;
    bool m_connect_in_progress = false;

    void try_next_endpoint(network::endpoint::list::iterator i, ConnectCompletionHandler handler)
    {
        REALM_ASSERT(i != m_endpoints.end());
        // FIXME: Use [this, handler=std::move(handler)] in C++14 to avoid a
        // potentially expensive copy of user specified completion handler into
        // the lambda
        auto handler_2 = [=](std::error_code ec) {
            // Note: If `ec` is `operation_aborted`, the the socket object may
            // already have been destroyed.
            if (ec != error::operation_aborted) {
                REALM_ASSERT(m_connect_in_progress);
                if (ec) {
                    m_socket.close();
                    auto i_2 = i + 1;
                    if (i_2 < m_endpoints.end()) {
                        try_next_endpoint(i_2, std::move(handler)); // Throws
                        return;
                    }
                }
                m_connect_in_progress = false;
            }
            handler(ec); // Throws
        };
        m_socket.async_connect(*i, std::move(handler_2)); // Throws
    }
};



class DeadlineTimerImpl: public DeadlineTimer {
public:
    DeadlineTimerImpl(network::io_service& service):
        m_timer(service)
    {
    }

    void async_wait(Duration delay, WaitCompletionHandler handler) override
    {
        m_timer.async_wait(delay, std::move(handler));
    }

    void cancel() noexcept override
    {
        m_timer.cancel();
    }

private:
    network::deadline_timer m_timer;
};


inline std::unique_ptr<Socket> EventLoopImpl::make_socket()
{
    return std::unique_ptr<Socket>(new SocketImpl(m_io_service));
}

inline std::unique_ptr<DeadlineTimer> EventLoopImpl::make_timer()
{
    return std::unique_ptr<DeadlineTimer>(new DeadlineTimerImpl(m_io_service));
}


class ImplementationImpl: public EventLoop::Implementation {
public:
    std::string name() const
    {
        return "posix";
    }
    std::unique_ptr<EventLoop> make_event_loop()
    {
        return std::unique_ptr<EventLoop>(new EventLoopImpl()); // Throws
    }
};


ImplementationImpl g_implementation;

} // unnamed namespace


namespace realm {
namespace _impl {

EventLoop::Implementation& get_posix_event_loop_impl()
{
    return g_implementation;
}

} // namespace _impl
} // namespace realm
