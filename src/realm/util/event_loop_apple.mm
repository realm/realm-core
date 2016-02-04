#include <realm/util/event_loop.hpp>
#include <realm/util/optional.hpp>

#import <Foundation/Foundation.h>

using namespace realm;
using namespace realm::util;

struct EventLoop<Apple>::Impl {
    NSRunLoop* m_runloop;
};

EventLoop<Apple>::EventLoop(): m_impl(new Impl)
{
    m_impl->m_runloop = [NSRunLoop currentRunLoop];
}

EventLoop<Apple>::~EventLoop()
{
}

void EventLoop<Apple>::run()
{
    REALM_ASSERT(m_impl->m_runloop == [NSRunLoop currentRunLoop]); // Running a different runloop than expected.
    [m_impl->m_runloop run];
}

void EventLoop<Apple>::stop()
{
    CFRunLoopRef runloop = [m_impl->m_runloop getCFRunLoop];
    CFRunLoopStop(runloop);
}

void EventLoop<Apple>::reset()
{
    // Do nothing.
}

struct EventLoop<Apple>::Socket: SocketBase {
    NSRunLoop* m_runloop;
    OnConnectComplete m_on_connect_complete;

    NSInputStream* m_read_stream;
    NSOutputStream* m_write_stream;
    CFStreamClientContext m_context;

    size_t m_num_open_streams = 0;
    size_t m_num_operations_scheduled = 0;

    OnReadComplete m_on_read_complete;
    char* m_current_read_buffer = nullptr;
    size_t m_current_read_buffer_size = 0;
    size_t m_bytes_read = 0;

    OnWriteComplete m_on_write_complete;
    const char* m_current_write_buffer = nullptr;
    size_t m_current_write_buffer_size = 0;
    size_t m_bytes_written = 0;
    Optional<char> m_read_delim;


    Socket(NSRunLoop* runloop, std::string host, int port, SocketSecurity sec, OnConnectComplete on_connect_complete):
        m_runloop(runloop), m_on_connect_complete(std::move(on_connect_complete))
    {
        CFReadStreamRef read_stream;
        CFWriteStreamRef write_stream;

        CFStringRef cf_host = CFStringCreateWithCString(kCFAllocatorDefault, host.c_str(), kCFStringEncodingUTF8);
        CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, cf_host, static_cast<UInt32>(port), &read_stream, &write_stream);
        CFRelease(cf_host);

        m_read_stream = static_cast<NSInputStream*>(read_stream);
        m_write_stream = static_cast<NSOutputStream*>(write_stream);

        if (sec == SocketSecurity::TLSv1) {
            [m_read_stream  setProperty:NSStreamSocketSecurityLevelTLSv1 forKey:NSStreamSocketSecurityLevelKey];
            [m_write_stream setProperty:NSStreamSocketSecurityLevelTLSv1 forKey:NSStreamSocketSecurityLevelKey];
        }
        else if (sec == SocketSecurity::None) {}
        else {
            throw std::runtime_error{"Unsupported socket security level."};
        }

        m_context.version = 1;
        m_context.info = this;
        m_context.retain = nullptr;
        m_context.release = nullptr;
        m_context.copyDescription = nullptr;

        std::error_code err = activate();
        if (err) {
            m_on_connect_complete(err);
            return;
        }

        [m_read_stream  open];
        [m_write_stream open];
    }

    ~Socket()
    {
    }

    std::error_code activate()
    {
        CFOptionFlags read_flags = kCFStreamEventOpenCompleted
                                 | kCFStreamEventErrorOccurred
                                 | kCFStreamEventEndEncountered
                                 | kCFStreamEventHasBytesAvailable;
        CFOptionFlags write_flags = kCFStreamEventOpenCompleted
                                  | kCFStreamEventErrorOccurred
                                  | kCFStreamEventEndEncountered
                                  | kCFStreamEventCanAcceptBytes;

        CFReadStreamRef read_stream = static_cast<CFReadStreamRef>(m_read_stream);
        CFWriteStreamRef write_stream = static_cast<CFWriteStreamRef>(m_write_stream);

        if (CFReadStreamSetClient(read_stream, read_flags, read_cb, &m_context)) {
            [m_read_stream scheduleInRunLoop:m_runloop forMode:NSRunLoopCommonModes];
        }
        else {
            return convert_error_code(CFReadStreamCopyError(read_stream));
        }

        if (CFWriteStreamSetClient(write_stream, write_flags, write_cb, &m_context)) {
            [m_write_stream scheduleInRunLoop:m_runloop forMode:NSRunLoopCommonModes];
        }
        else {
            [m_read_stream removeFromRunLoop:m_runloop forMode:NSRunLoopCommonModes];
            return convert_error_code(CFWriteStreamCopyError(write_stream));
        }

        return std::error_code{}; // no error
    }

    void close() override
    {
        [m_read_stream close];
        [m_write_stream close];
        m_num_open_streams = 0;
    }

    void cancel() override
    {
        [m_read_stream  removeFromRunLoop:m_runloop forMode:NSRunLoopCommonModes];
        [m_write_stream removeFromRunLoop:m_runloop forMode:NSRunLoopCommonModes];
        if (m_on_read_complete) {
            on_read_complete(error::operation_aborted);
        }
        if (m_on_write_complete) {
            on_write_complete(error::operation_aborted);
        }
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
        ++m_num_operations_scheduled;
    }

    void async_read(char* buffer, size_t size, OnReadComplete on_read_complete) override
    {
        REALM_ASSERT(m_current_read_buffer == nullptr);
        REALM_ASSERT(!m_on_read_complete);

        m_on_read_complete = std::move(on_read_complete);
        m_current_read_buffer = buffer;
        m_current_read_buffer_size = size;
        ++m_num_operations_scheduled;
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
        --m_num_operations_scheduled;
        if (m_num_operations_scheduled == 0)
            cancel();
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
        --m_num_operations_scheduled;
        if (m_num_operations_scheduled == 0)
            cancel();
    }

    void read_cb(CFReadStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == static_cast<CFReadStreamRef>(m_read_stream));

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

                size_t bytes_read = CFReadStreamRead(stream, buffer, bytes_to_read);
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
                on_read_complete(convert_error_code(CFReadStreamCopyError(stream)));
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
        REALM_ASSERT(stream == static_cast<CFWriteStreamRef>(m_write_stream));

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
                size_t bytes_written = CFWriteStreamWrite(stream, buffer, m_current_write_buffer_size);
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
                on_write_complete(convert_error_code(CFWriteStreamCopyError(stream)));
                break;
            }
            case kCFStreamEventEndEncountered: {
                on_write_complete(make_error_code(network::end_of_input));
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

std::unique_ptr<SocketBase>
EventLoop<Apple>::async_connect(std::string host, int port, SocketSecurity sec,
                                OnConnectComplete on_connect)
{
    return std::unique_ptr<SocketBase>(new Socket{m_impl->m_runloop, std::move(host), port,
                                                  sec, std::move(on_connect)});
}

std::unique_ptr<DeadlineTimerBase> EventLoop<Apple>::async_timer(Duration, OnTimeout)
{
    REALM_ASSERT_RELEASE(false && "Not yet implemented");
}

void EventLoop<Apple>::post(OnPost on_post)
{
    static_cast<void>(on_post);
    REALM_ASSERT_RELEASE(false && "Not yet implemented");
}

