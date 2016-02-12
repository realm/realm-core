#include <realm/util/event_loop.hpp>
#include <realm/util/optional.hpp>

#import <Foundation/Foundation.h>

using namespace realm;
using namespace realm::util;

static NSString* kRealmEventLoopStateKey = @"RealmEventLoopState";
static NSString* kRealmRunLoopMode = @"RealmRunLoopMode";

class EventLoopApple;

@interface EventLoopState: NSObject {
    std::unique_ptr<EventLoopApple> _eventLoop;
}
-(id)init;
@property(readonly) EventLoopApple* eventLoop;
@end

static EventLoopApple& get_apple_event_loop()
{
    NSMutableDictionary* dict = [NSThread currentThread].threadDictionary;
    auto state = static_cast<EventLoopState*>(dict[kRealmEventLoopStateKey]);
    if (state == nil) {
        state = [[EventLoopState alloc] init];
        dict[kRealmEventLoopStateKey] = state;
    }
    return *state.eventLoop;
}

class EventLoopApple: public EventLoop {
public:
    EventLoopApple();
    ~EventLoopApple();

    void run() override;
    void stop() override;

    std::unique_ptr<Socket> async_connect(std::string host, int port, SocketSecurity, OnConnectComplete) final;
    std::unique_ptr<DeadlineTimer> async_timer(Duration delay, OnTimeout) final;
    void post(OnPost) final;

    struct SocketImpl;
    struct DeadlineTimerImpl;

    NSRunLoop* m_runloop = nil;
    NSThread* m_thread = nil;
    std::atomic<size_t> m_active_events;
    std::exception_ptr m_caught_exception;
    std::atomic<bool> m_running;
};


@implementation EventLoopState
-(EventLoopApple*)eventLoop {
    return self->_eventLoop.get();
}
-(id)init {
    self = [super init];
    if (self) {
        self->_eventLoop.reset(new EventLoopApple);
    }
    return self;
}
@end


EventLoopApple::EventLoopApple()
{
    m_active_events = 0;
    m_runloop = [NSRunLoop currentRunLoop];
    m_thread = [NSThread currentThread];
}

EventLoopApple::~EventLoopApple()
{
}

void EventLoopApple::run()
{
    m_running = true;
    do {
        // FIXME: NSRunLoop doesn't seem to have a built-in mode that exactly matches the
        // behavior we want, which is to run until no events are scheduled, but terminate
        // when there actually are no more events that we have scheduled ourselves.
        //
        // -[NSRunLoop run] never terminates.
        //
        // Additional complication arise from the fact that NSRunLoop ignores exceptions
        // thrown from a task scheduled by [NSObject performSelector:onThread:], which we
        // use to implement post(). The current solution waits for 100ms before
        // terminating when there are no more events, or if an exception is caught.
        [m_runloop runMode:kRealmRunLoopMode beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.1]];

        if (m_caught_exception) {
            auto exception = std::move(m_caught_exception);
            m_caught_exception = nullptr;
            std::rethrow_exception(exception);
        }
    } while (m_running && m_active_events > 0);
}

void EventLoopApple::stop()
{
    m_running = false;
}

struct EventLoopApple::SocketImpl: Socket {
    EventLoopApple& m_owner;
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


    SocketImpl(EventLoopApple& runloop, std::string host, int port, SocketSecurity sec, OnConnectComplete connect_complete_handler):
        m_owner(runloop), m_on_connect_complete(std::move(connect_complete_handler))
    {
        CFReadStreamRef read_stream;
        CFWriteStreamRef write_stream;

        CFStringRef cf_host = CFStringCreateWithCString(kCFAllocatorDefault, host.c_str(), kCFStringEncodingUTF8);
        CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, cf_host, static_cast<UInt32>(port), &read_stream, &write_stream);
        CFRelease(cf_host);

        m_read_stream = (__bridge NSInputStream*)read_stream;
        m_write_stream = (__bridge NSOutputStream*)write_stream;

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
            on_open_complete(err);
            return;
        }

        [m_read_stream  open];
        [m_write_stream open];
    }

    ~SocketImpl()
    {
        cancel(); // cancel() is virtual but final, so OK to call in destructor.
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

        CFReadStreamRef read_stream = (__bridge CFReadStreamRef)m_read_stream;
        CFWriteStreamRef write_stream = (__bridge CFWriteStreamRef)m_write_stream;

        if (CFReadStreamSetClient(read_stream, read_flags, read_cb, &m_context)) {
            [m_read_stream scheduleInRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
            ++m_owner.m_active_events;
        }
        else {
            return convert_error_code(CFReadStreamCopyError(read_stream));
        }

        if (CFWriteStreamSetClient(write_stream, write_flags, write_cb, &m_context)) {
            [m_write_stream scheduleInRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
            ++m_owner.m_active_events;
        }
        else {
            [m_read_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
            --m_owner.m_active_events; // because read stream was removed
            return convert_error_code(CFWriteStreamCopyError(write_stream));
        }

        return std::error_code{}; // no error
    }

    void close() override
    {
        [m_read_stream close];
        [m_write_stream close];
        m_num_open_streams = 0;
        cancel_with_error(error::connection_aborted);
    }

    void cancel() override
    {
        cancel_with_error(error::operation_aborted);
    }

    void cancel_with_error(std::error_code ec)
    {
        if (m_on_connect_complete || m_on_read_complete) {
            [m_read_stream  removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
            if (m_on_connect_complete) {
                --m_owner.m_active_events;
                m_on_connect_complete(ec); // Invoke directly instead of through on_open_complete()
            }
            else {
                on_read_complete(ec);
            }
        }

        if (m_on_connect_complete || m_on_write_complete) {
            [m_write_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
            if (m_on_connect_complete) {
                --m_owner.m_active_events;
                // Completion handler already invoked above.
            }
            else {
                on_write_complete(ec);
            }
        }

        m_on_connect_complete = nullptr;
        REALM_ASSERT(m_on_read_complete == nullptr);
        REALM_ASSERT(m_on_write_complete == nullptr);
    }

    bool is_open() const
    {
        return m_num_open_streams == 2;
    }

    void async_write(const char* data, size_t size, OnWriteComplete on_write_complete) override
    {
        REALM_ASSERT(m_current_write_buffer == nullptr);
        REALM_ASSERT(!m_on_connect_complete); // not yet connected
        REALM_ASSERT(!m_on_write_complete);

        m_on_write_complete = std::move(on_write_complete);
        m_current_write_buffer = data;
        m_current_write_buffer_size = size;
        ++m_num_operations_scheduled;
        ++m_owner.m_active_events;
        [m_write_stream scheduleInRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
    }

    void async_read(char* buffer, size_t size, OnReadComplete on_read_complete) override
    {
        REALM_ASSERT(m_current_read_buffer == nullptr);
        REALM_ASSERT(!m_on_connect_complete); // not yet connected
        REALM_ASSERT(!m_on_read_complete);

        m_on_read_complete = std::move(on_read_complete);
        m_current_read_buffer = buffer;
        m_current_read_buffer_size = size;
        ++m_num_operations_scheduled;
        ++m_owner.m_active_events;
        [m_read_stream scheduleInRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
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

    void on_open_complete(std::error_code)
    {
        [m_read_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
        [m_write_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
        m_owner.m_active_events -= 2;

        auto handler = std::move(m_on_connect_complete);
        m_on_connect_complete = nullptr;
        handler(std::error_code{});
    }

    void handle_open_completed()
    {
        ++m_num_open_streams;
        if (is_open()) {
            on_open_complete(std::error_code{});
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
        --m_num_operations_scheduled;
        --m_owner.m_active_events;
        [m_read_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
        handler(ec, total_bytes_read);
    }

    void on_write_complete(std::error_code ec)
    {
        auto handler = std::move(m_on_write_complete);
        auto total_bytes_written = m_bytes_written;
        m_on_write_complete = nullptr;
        m_current_write_buffer = nullptr;
        m_current_write_buffer_size = 0;
        m_bytes_written = 0;
        --m_num_operations_scheduled;
        --m_owner.m_active_events;
        [m_write_stream removeFromRunLoop:m_owner.m_runloop forMode:kRealmRunLoopMode];
        handler(ec, total_bytes_written);
    }

    void read_cb(CFReadStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == (__bridge CFReadStreamRef)m_read_stream);

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
                auto ec = convert_error_code(CFReadStreamCopyError(stream));
                if (m_on_connect_complete) {
                    on_open_complete(ec);
                }
                else if (m_on_read_complete) {
                    on_read_complete(ec);
                }
                break;
            }
            case kCFStreamEventEndEncountered: {
                REALM_ASSERT(m_on_read_complete);
                on_read_complete(make_error_code(network::end_of_input));
                break;
            }
        }
    }

    void write_cb(CFWriteStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == (__bridge CFWriteStreamRef)m_write_stream);

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
                auto ec = convert_error_code(CFWriteStreamCopyError(stream));
                if (m_on_connect_complete) {
                    on_open_complete(ec);
                }
                else if (m_on_write_complete) {
                    on_write_complete(ec);
                }
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
        SocketImpl* self = reinterpret_cast<SocketImpl*>(info);
        self->read_cb(stream, event_type);
    }

    static void write_cb(CFWriteStreamRef stream, CFStreamEventType event_type, void* info)
    {
        SocketImpl* self = reinterpret_cast<SocketImpl*>(info);
        self->write_cb(stream, event_type);
    }
};

std::unique_ptr<Socket>
EventLoopApple::async_connect(std::string host, int port, SocketSecurity sec,
                              OnConnectComplete on_connect)
{
    REALM_ASSERT(m_thread == [NSThread currentThread]); // Not thread-safe
    return std::unique_ptr<Socket>(new SocketImpl{*this, std::move(host), port,
                                                  sec, std::move(on_connect)});
}


struct EventLoopApple::DeadlineTimerImpl: DeadlineTimer {
public:
    DeadlineTimerImpl(EventLoopApple& owner, Duration duration, OnTimeout on_timeout):
        m_owner(owner), m_timer(nil)
    {
        m_context.version = 0;
        m_context.info = this;
        m_context.retain = nullptr;
        m_context.release = nullptr;
        m_context.copyDescription = nullptr;

        async_wait(duration, std::move(on_timeout));
    }

    ~DeadlineTimerImpl()
    {
        if (m_timer) {
            [m_timer invalidate];
            m_timer = nil;
        }
        if (m_on_timeout) {
            --m_owner.m_active_events;
            m_on_timeout(error::operation_aborted);
        }
    }

    void cancel() final
    {
        REALM_ASSERT(m_timer);
        REALM_ASSERT(m_on_timeout);
        m_error = std::error_code{error::operation_aborted};
        [m_timer setFireDate:[NSDate distantPast]];
    }

    void async_wait(Duration duration, OnTimeout on_timeout) final
    {
        REALM_ASSERT(!m_on_timeout);
        REALM_ASSERT(!m_timer);

        m_on_timeout = std::move(on_timeout);

        // CFAbsoluteTime is a double representing seconds.
        // Duration::period::den is the number of ticks per second.
        static_assert(Duration::period::num == 1, "Duration type does not have sub-second precision.");
        CFAbsoluteTime fire_date = CFAbsoluteTimeGetCurrent() + double(duration.count()) / double(Duration::period::den);
        CFTimeInterval interval = 0; // fire once only
        CFOptionFlags flags = 0; // unused; must be 0.
        CFIndex order = 0; // unused; must be 0;

        CFRunLoopTimerRef timer = CFRunLoopTimerCreate(kCFAllocatorDefault,
                                                       fire_date,
                                                       interval,
                                                       flags,
                                                       order,
                                                       on_timer_cb,
                                                       &m_context);
        m_timer = (__bridge NSTimer*)timer;
        [m_owner.m_runloop addTimer:m_timer forMode:kRealmRunLoopMode];
        ++m_owner.m_active_events;
    }

private:
    EventLoopApple& m_owner;
    NSTimer* m_timer;
    OnTimeout m_on_timeout;
    CFRunLoopTimerContext m_context;
    std::error_code m_error;

    void timer_cb(CFRunLoopTimerRef timer)
    {
        REALM_ASSERT(timer == (__bridge CFRunLoopTimerRef)m_timer);
        --m_owner.m_active_events;
        m_on_timeout(m_error);
        m_on_timeout = nullptr;
        m_timer = nil;
        m_error = std::error_code{};
    }

    static void on_timer_cb(CFRunLoopTimerRef timer, void* info)
    {
        auto self = reinterpret_cast<DeadlineTimerImpl*>(info);
        self->timer_cb(timer);
    }
};


std::unique_ptr<DeadlineTimer>
EventLoopApple::async_timer(Duration duration, OnTimeout on_timeout)
{
    REALM_ASSERT(m_thread == [NSThread currentThread]); // Not thread-safe
    return std::unique_ptr<DeadlineTimer>(new DeadlineTimerImpl{*this, duration, std::move(on_timeout)});
}

void EventLoopApple::post(OnPost on_post)
{
    CFRunLoopRef runloop = [m_runloop getCFRunLoop];
    ++m_active_events;
    CFRunLoopPerformBlock(runloop, (__bridge CFStringRef)kRealmRunLoopMode, ^{
        // NSRunLoop eats exceptions for breakfast, but we don't want that behavior. We want to
        // stop the event loop and propagate the exception to the caller instead.
        --m_active_events;

        if (m_caught_exception) {
            // If an exception was caught from a different post() handler, skip this one.
            return;
        }

        try {
            on_post();
        }
        catch (...) {
            m_caught_exception = std::current_exception();
        }
    });
    CFRunLoopWakeUp(runloop);
}

namespace realm {
namespace util {
EventLoop& get_native_event_loop()
{
    return get_apple_event_loop();
}
} // namespace util
} // namespace realm

