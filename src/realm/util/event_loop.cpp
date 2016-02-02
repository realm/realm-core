#include "realm/util/event_loop.hpp"
#include "realm/util/network.hpp"
#include "realm/util/optional.hpp"

#include <system_error>

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
    REALM_ASSERT(m_impl->m_runloop == CFRunLoopGetCurrent()); // Running a different runloop than expected.
    CFRunLoopRun();
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
    char* m_current_read_buffer = nullptr;
    size_t m_current_read_buffer_size = 0;
    size_t m_bytes_read = 0;

    OnWriteComplete m_on_write_complete;
    const char* m_current_write_buffer = nullptr;
    size_t m_current_write_buffer_size = 0;
    size_t m_bytes_written = 0;
    Optional<char> m_read_delim;


    Socket(CFRunLoopRef runloop, std::string host, int port, OnConnectComplete on_connect_complete):
        m_runloop(runloop), m_on_connect_complete(std::move(on_connect_complete))
    {
        CFStringRef cf_host = CFStringCreateWithCString(kCFAllocatorDefault, host.c_str(), kCFStringEncodingUTF8);
        CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, cf_host, static_cast<UInt32>(port), &m_read_stream, &m_write_stream);
        CFRelease(cf_host);

        m_context.version = 1;
        m_context.info = this;
        m_context.retain = nullptr;
        m_context.release = nullptr;
        m_context.copyDescription = nullptr;

        CFOptionFlags read_flags = kCFStreamEventOpenCompleted
                                 | kCFStreamEventErrorOccurred
                                 | kCFStreamEventEndEncountered
                                 | kCFStreamEventHasBytesAvailable;
        CFOptionFlags write_flags = kCFStreamEventOpenCompleted
                                  | kCFStreamEventErrorOccurred
                                  | kCFStreamEventEndEncountered
                                  | kCFStreamEventCanAcceptBytes;

        if (CFReadStreamSetClient(m_read_stream, read_flags, read_cb, &m_context)) {
            CFReadStreamScheduleWithRunLoop(m_read_stream, m_runloop, kCFRunLoopCommonModes);
        }
        else {
            m_on_connect_complete(convert_error_code(CFReadStreamCopyError(m_read_stream)));
            return;
        }

        if (CFWriteStreamSetClient(m_write_stream, write_flags, write_cb, &m_context)) {
            CFWriteStreamScheduleWithRunLoop(m_write_stream, m_runloop, kCFRunLoopCommonModes);
        }
        else {
            m_on_connect_complete(convert_error_code(CFWriteStreamCopyError(m_write_stream)));
            return;
        }

        if (CFReadStreamOpen(m_read_stream)) {
            CFErrorRef err = CFReadStreamCopyError(m_read_stream);
            if (err != nullptr) {
                m_on_connect_complete(convert_error_code(err));
                return;
            }

            if (CFWriteStreamOpen(m_write_stream)) {
                err = CFWriteStreamCopyError(m_write_stream);
                if (err != nullptr) {
                    m_on_connect_complete(convert_error_code(err));
                    return;
                }
            }
        }
    }

    ~Socket()
    {
        CFRelease(m_read_stream);
        CFRelease(m_write_stream);
    }

    void close() override
    {
        CFReadStreamClose(m_read_stream);
        CFWriteStreamClose(m_write_stream);
        m_num_open_streams = 0;
    }

    void cancel() override
    {
        CFReadStreamUnscheduleFromRunLoop(m_read_stream, m_runloop, kCFRunLoopCommonModes);
        CFWriteStreamUnscheduleFromRunLoop(m_write_stream, m_runloop, kCFRunLoopCommonModes);
    }

    bool is_open() const
    {
        return m_num_open_streams == 2;
    }

    void async_write(const char* data, size_t size, OnWriteComplete on_write_complete) override
    {
        REALM_ASSERT(m_current_write_buffer == nullptr);
        REALM_ASSERT(!m_on_write_complete);

        m_on_write_complete = std::move(on_write_complete);
        m_current_write_buffer = data;
        m_current_write_buffer_size = size;
    }

    void async_read(char* buffer, size_t size, OnReadComplete on_read_complete) override
    {
        REALM_ASSERT(m_current_read_buffer == nullptr);
        REALM_ASSERT(!m_on_read_complete);

        m_on_read_complete = std::move(on_read_complete);
        m_current_read_buffer = buffer;
        m_current_read_buffer_size = size;
    }

    void async_read_until(char* buffer, size_t size, char delim, OnReadComplete on_read_complete) override
    {
        REALM_ASSERT(m_read_delim == none);
        m_read_delim = delim;
        async_read(buffer, size, std::move(on_read_complete));
    }

private:
    std::error_code convert_error_code(CFErrorRef err)
    {
        CFStringRef domain = CFErrorGetDomain(err);
        CFIndex code = CFErrorGetCode(err);
        std::error_code ec;

        if (domain == kCFErrorDomainPOSIX) {
            ec = std::error_code{int(code), std::system_category()};
        }
        else if (domain == kCFErrorDomainOSStatus) {
            REALM_ASSERT(false); // FIXME
        }
        else if (domain == kCFErrorDomainMach) {
            REALM_ASSERT(false); // FIXME
        }
        else if (domain == kCFErrorDomainCocoa) {
            REALM_ASSERT(false); // FIXME
        }
        else {
            REALM_ASSERT(false); // FIXME
        }
        CFRelease(err);
        return ec;
    }

    void handle_open_completed()
    {
        ++m_num_open_streams;
        if (is_open()) {
            m_on_connect_complete(std::error_code{});
        }
    }

    void on_read_complete(std::error_code ec)
    {
        auto handler = std::move(m_on_read_complete);
        auto total_bytes_read = m_bytes_read;
        m_read_delim = none;
        m_on_read_complete = nullptr;
        m_current_read_buffer = nullptr;
        m_current_read_buffer_size = 0;
        m_bytes_read = 0;
        handler(ec, total_bytes_read);
    }

    void on_write_complete(std::error_code ec)
    {
        auto on_write_complete = std::move(m_on_write_complete);
        auto total_bytes_written = m_bytes_written;
        m_on_write_complete = nullptr;
        m_current_write_buffer = nullptr;
        m_current_write_buffer_size = 0;
        m_bytes_written = 0;
        on_write_complete(ec, total_bytes_written);
    }

    void read_cb(CFReadStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_read_stream);
        static_cast<void>(stream);

        switch (event_type) {
            case kCFStreamEventOpenCompleted:
                handle_open_completed();
                break;
            case kCFStreamEventHasBytesAvailable: {
                if (!m_on_read_complete) {
                    // No read operation in progress, so just leave the data in the buffer for now.
                    return;
                }

                UInt8* buffer = reinterpret_cast<UInt8*>(m_current_read_buffer);

                // FIXME: CFReadStreamRead advances the socket's read buffer internally,
                // so read_until cannot consume more than a single byte at a time, because
                // it needs to check if the delimiter was found. This is likely to be grossly
                // inefficient, but the alternative is to introduce another layer of buffering.
                // Mitigation: Investigate if CFReadStreamRead corresponds 1-to-1 to the read()
                // system call and if there is any performance to be gained from introducing
                // another layer of buffering.
                size_t bytes_to_read = m_read_delim ? 1 : m_current_read_buffer_size;

                size_t bytes_read = CFReadStreamRead(m_read_stream, buffer, bytes_to_read);
                m_bytes_read += bytes_read;

                if (m_read_delim && bytes_read > 0 && buffer[0] == UInt8(*m_read_delim)) {
                    // Completion because delimiter found.
                    on_read_complete(std::error_code{});
                }
                else if (bytes_read < m_current_read_buffer_size) {
                    // Not complete yet.
                    m_current_read_buffer_size -= bytes_read;
                    m_current_read_buffer += bytes_read;
                }
                else {
                    // Completion because buffer is full.
                    std::error_code ec{};
                    if (m_read_delim) {
                        ec = make_error_code(network::delim_not_found);
                    }
                    on_read_complete(ec);
                }
                break;
            }
            case kCFStreamEventErrorOccurred: {
                on_read_complete(convert_error_code(CFReadStreamCopyError(m_read_stream)));
                break;
            }
            case kCFStreamEventEndEncountered: {
                on_read_complete(make_error_code(network::end_of_input));
                break;
            }
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
            case kCFStreamEventCanAcceptBytes: {
                if (!m_on_write_complete) {
                    // No write operation in progress, so just don't do anything until one is.
                    return;
                }

                const UInt8* buffer = reinterpret_cast<const UInt8*>(m_current_write_buffer);
                size_t bytes_written = CFWriteStreamWrite(m_write_stream, buffer, m_current_write_buffer_size);
                m_bytes_written += bytes_written;
                if (bytes_written < m_current_write_buffer_size) {
                    m_current_write_buffer_size -= bytes_written;
                    m_current_write_buffer += bytes_written;
                }
                else {
                    on_write_complete(std::error_code{});
                }
                break;
            }
            case kCFStreamEventErrorOccurred: {
                on_write_complete(convert_error_code(CFWriteStreamCopyError(m_write_stream)));
                break;
            }
            case kCFStreamEventEndEncountered: {
                on_read_complete(make_error_code(network::end_of_input));
                break;
            }
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

