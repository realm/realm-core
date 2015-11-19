#include "realm/util/event_loop.hpp"
#include "realm/util/network.hpp"

#if REALM_PLATFORM_APPLE
#include <CoreFoundation/CoreFoundation.h>
#include <CoreServices/CoreServices.h>
#endif // REALM_PLATFORM_APPLE


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


#if REALM_PLATFORM_APPLE


namespace {

class CFStreamErrorCategory: public std::error_category {
    const char* name() const noexcept override;
    std::string message(int) const override;
};
CFStreamErrorCategory g_cfstream_error_category{};

const char* CFStreamErrorCategory::name() const noexcept
{
    return "CFStream";
}

std::string CFStreamErrorCategory::message(int) const
{
    // FIXME
    REALM_ASSERT(false);
}

} // anonymous namespace


struct EventLoop<Apple>::Impl {
    CFRunLoopRef m_runloop;
};

EventLoop<Apple>::EventLoop(): m_impl(new Impl)
{
    m_impl->m_runloop = CFRunLoopGetCurrent();
    CFRetain(m_impl->m_runloop);
}

EventLoop<Apple>::~EventLoop()
{
    CFRelease(m_impl->m_runloop);
}

void EventLoop<Apple>::run()
{
    CFRunLoopRunResult r;
    do {
        r = CFRunLoopRunInMode(kCFRunLoopDefaultMode, 0, true);
    } while (r == kCFRunLoopRunTimedOut);
}

void EventLoop<Apple>::stop() noexcept
{
    CFRunLoopStop(m_impl->m_runloop);
}

struct EventLoop<Apple>::Socket: SocketBase {
    CFRunLoopRef m_runloop;
    OnConnectComplete m_on_connect_complete;

    CFReadStreamRef m_read_stream;
    CFWriteStreamRef m_write_stream;
    CFStreamClientContext m_context;

    bool m_read_scheduled = false;
    bool m_write_scheduled = false;
    size_t m_num_open_streams = 0;

    OnReadComplete m_on_read_complete;
    OnWriteComplete m_on_write_complete;


    Socket(CFRunLoopRef runloop, std::string host, int port, OnConnectComplete on_connect_complete):
        m_runloop(runloop), m_on_connect_complete(std::move(on_connect_complete))
    {
        CFRetain(m_runloop);

		CFStringRef cf_host = CFStringCreateWithCString(kCFAllocatorDefault, host.c_str(), kCFStringEncodingUTF8);
		CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, cf_host, static_cast<UInt32>(port), &m_read_stream, &m_write_stream);
		CFRelease(cf_host);

        m_context.version = 0;
        m_context.info = this;
        m_context.retain = nullptr;
        m_context.release = nullptr;
        m_context.copyDescription = nullptr;

        CFOptionFlags read_flags = kCFStreamEventOpenCompleted
                                 | kCFStreamEventErrorOccurred
                                 | kCFStreamEventEndEncountered;
        CFOptionFlags write_flags = kCFStreamEventOpenCompleted
                                  | kCFStreamEventErrorOccurred
                                  | kCFStreamEventEndEncountered;

        CFReadStreamSetClient(m_read_stream, read_flags, read_cb, &m_context);
        CFWriteStreamSetClient(m_write_stream, write_flags, write_cb, &m_context);
        CFReadStreamScheduleWithRunLoop(m_read_stream, m_runloop, kCFRunLoopDefaultMode);
        CFWriteStreamScheduleWithRunLoop(m_write_stream, m_runloop, kCFRunLoopDefaultMode);
        CFReadStreamOpen(m_read_stream);
        CFWriteStreamOpen(m_write_stream);
    }

    ~Socket()
    {
        CFRelease(m_read_stream);
        CFRelease(m_write_stream);
        CFRelease(m_runloop);
    }

    void close() override
    {
        CFReadStreamClose(m_read_stream);
        CFWriteStreamClose(m_write_stream);
        m_num_open_streams = 0;
    }

    void cancel() override
    {
        CFReadStreamUnscheduleFromRunLoop(m_read_stream, m_runloop, kCFRunLoopDefaultMode);
        CFWriteStreamUnscheduleFromRunLoop(m_write_stream, m_runloop, kCFRunLoopDefaultMode);
    }

    bool is_open() const
    {
        return m_num_open_streams == 2;
    }

    void async_write(const char* data, size_t size, OnWriteComplete on_write_complete) override
    {
        m_on_write_complete = std::move(on_write_complete);

    }

    void async_read(char* buffer, size_t size, OnReadComplete on_read_complete) override
    {
        // FIXME
    }

    void async_read_until(char* buffer, size_t size, char delim, OnReadComplete on_read_complete) override
    {
        // FIXME
    }

private:
    void handle_open_completed()
    {
        ++m_num_open_streams;
        if (is_open()) {
            m_on_connect_complete(std::error_code{});
        }
    }

    void read_cb(CFReadStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_read_stream);
        static_cast<void>(stream);

        switch (event_type) {
            case kCFStreamEventOpenCompleted:
                handle_open_completed();
                break;
        }
    }

    void write_cb(CFWriteStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_write_stream);
        static_cast<void>(stream);

        switch (event_type) {
            case kCFStreamEventOpenCompleted:
                handle_open_completed();
                break;
        }
    }

    static void read_cb(CFReadStreamRef stream, CFStreamEventType event_type, void* info)
    {
        Socket* self = reinterpret_cast<Socket*>(info);
        self->read_cb(stream, event_type);
    }

    static void write_cb(CFWriteStreamRef stream, CFStreamEventType event_type, void* info)
    {
        Socket* self = reinterpret_cast<Socket*>(info);
        self->write_cb(stream, event_type);
    }
};

std::unique_ptr<SocketBase> EventLoop<Apple>::async_connect(std::string host, int port, OnConnectComplete on_connect)
{
    return std::unique_ptr<SocketBase>(new Socket{m_impl->m_runloop, std::move(host), port, std::move(on_connect)});
}

std::unique_ptr<DeadlineTimerBase> EventLoop<Apple>::async_timer(Duration, OnTimeout)
{
    // FIXME
    return nullptr;
}

#endif // REALM_PLATFORM_APPLE

} // namespace util
} // namespace realm

