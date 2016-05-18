#include <type_traits>
#include <algorithm>
#include <stdexcept>
#include <utility>
#include <set>

#include <realm/util/optional.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/misc_errors.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/network.hpp>
#include <realm/util/event_loop.hpp>

// https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/NetworkingTopics/Articles/UsingSocketsandSocketStreams.html#//apple_ref/doc/uid/CH73-SW4
// https://developer.apple.com/library/mac/documentation/CoreFoundation/Conceptual/CFMemoryMgmt/CFMemoryMgmt.html

#include <CoreFoundation/CoreFoundation.h>
#include <CFNetwork/CFNetwork.h>

using namespace realm;
using namespace realm::util;

namespace {

template<class Ref> class ReleaseGuard {
public:
    explicit ReleaseGuard(Ref ref = nullptr) noexcept:
        m_ref(ref)
    {
    }

    ReleaseGuard(ReleaseGuard&& rg) noexcept:
        m_ref(rg.m_ref)
    {
        rg.m_ref = nullptr;
    }

    ~ReleaseGuard() noexcept
    {
        if (m_ref)
            CFRelease(m_ref);
    }

    ReleaseGuard &operator=(ReleaseGuard&& rg) noexcept
    {
        REALM_ASSERT(!m_ref || m_ref != rg.m_ref);
        if (m_ref)
            CFRelease(m_ref);
        m_ref = rg.m_ref;
        rg.m_ref = nullptr;
        return *this;
    }

    explicit operator bool() const noexcept
    {
        return bool(m_ref);
    }

    Ref get() const noexcept
    {
        return m_ref;
    }

    Ref release() noexcept
    {
        Ref ref = m_ref;
        m_ref = nullptr;
        return ref;
    }

    void reset(Ref ref = nullptr) noexcept
    {
        REALM_ASSERT(!m_ref || m_ref != ref);
        if (m_ref)
            CFRelease(m_ref);
        m_ref = ref;
    }

private:
    Ref m_ref;
};


ReleaseGuard<CFStringRef> make_cf_string(std::string str)
{
    static_assert(std::is_same<UInt8, char>::value || std::is_same<UInt8, unsigned char>::value,
                  "Unexpected byte type");
    const UInt8* bytes = reinterpret_cast<const UInt8*>(str.data());
    Boolean isExternalRepresentation = FALSE;
    CFStringRef str_2 = CFStringCreateWithBytes(kCFAllocatorDefault, bytes, str.size(),
                                                kCFStringEncodingUTF8, isExternalRepresentation);
    if (!str_2)
        throw std::bad_alloc();
    return ReleaseGuard<CFStringRef>(str_2);
}


class Oper {
public:
    // Execute completion handler
    virtual void execute() = 0;

    virtual ~Oper() noexcept {}

private:
    Oper* m_next = nullptr;
    friend class OperQueue;
};


class PostOper: public Oper {
public:
    EventLoop::PostCompletionHandler handler;

    void execute() override
    {
        handler(); // Throws
    }
};

class ConnectOper: public Oper {
public:
    Socket::ConnectCompletionHandler handler;
    std::error_code ec;

    void execute() override
    {
        handler(ec); // Throws
    }
};

class ReadOper: public Oper {
public:
    Socket::ReadCompletionHandler handler;
    std::error_code ec;
    size_t n = 0;

    void execute() override
    {
        handler(ec, n); // Throws
    }
};

class WriteOper: public Oper {
public:
    Socket::WriteCompletionHandler handler;
    std::error_code ec;
    size_t n = 0;

    void execute() override
    {
        handler(ec, n); // Throws
    }
};

class WaitOper: public Oper {
public:
    DeadlineTimer::WaitCompletionHandler handler;
    std::error_code ec;

    void execute() override
    {
        handler(ec); // Throws
    }
};



// A queue of operations with a nonthrowing push_back() method. This is
// necessary to support a nonthrowing cancel() method on sockets and timers.
class OperQueue {
public:
    bool empty() const noexcept
    {
        return !m_back;
    }
    void push_back(std::unique_ptr<Oper> op) noexcept
    {
        REALM_ASSERT(!op->m_next);
        if (m_back) {
            op->m_next = m_back->m_next;
            m_back->m_next = op.get();
        }
        else {
            op->m_next = op.get();
        }
        m_back = op.release();
    }
    void push_back(OperQueue& q) noexcept
    {
        if (!q.m_back)
            return;
        if (m_back)
            std::swap(m_back->m_next, q.m_back->m_next);
        m_back = q.m_back;
        q.m_back = nullptr;
    }
    std::unique_ptr<Oper> pop_front() noexcept
    {
        Oper* op = nullptr;
        if (m_back) {
            op = m_back->m_next;
            if (op != m_back) {
                m_back->m_next = op->m_next;
            }
            else {
                m_back = nullptr;
            }
            op->m_next = nullptr;
        }
        return std::unique_ptr<Oper>(op);
    }
    void clear() noexcept
    {
        if (m_back) {
            std::unique_ptr<Oper> op(m_back);
            while (op->m_next != m_back)
                op.reset(op->m_next);
            m_back = nullptr;
        }
    }
    ~OperQueue() noexcept
    {
        clear();
    }

private:
    Oper* m_back = nullptr;
};


class SocketImpl;
class DeadlineTimerImpl;


class EventLoopImpl: public EventLoop {
public:
    int_fast64_t num_operations_in_progress = 0;

    EventLoopImpl();
    ~EventLoopImpl() noexcept override;

    std::unique_ptr<Socket> make_socket() override;
    std::unique_ptr<DeadlineTimer> make_timer() override;
    void post(PostCompletionHandler) override;
    void run() override;
    void stop() noexcept override;
    void reset() noexcept override;

    void remove_socket(SocketImpl* socket) noexcept
    {
        m_sockets.erase(socket);
    }

    void remove_timer(DeadlineTimerImpl* timer) noexcept
    {
        m_timers.erase(timer);
    }

    void add_completed_operation(std::unique_ptr<Oper> oper) noexcept
    {
        m_completed_operations.push_back(std::move(oper));
    }

    void process_completed_operations()
    {
        // Note: Each handler execution can complete new operations, such as by
        // canceling operations in progress.
        while (std::unique_ptr<Oper> op = m_completed_operations.pop_front())
            op->execute(); // Throws

        if (num_operations_in_progress == 0) {
            CFRunLoopStop(m_cf_run_loop); // Out of work
            m_returning = true;
        }
    }

private:
    ReleaseGuard<CFRunLoopSourceRef> m_wake_up_source;

    // Operations whose completion handlers are ready to be executed
    OperQueue m_completed_operations;

    Mutex m_mutex;

    // `m_cf_run_loop` is protected by `m_mutex` because wake_up() needs to
    // access it. Note that wake_up() generally executes asynchronously with
    // respect to the event loop thread.
    CFRunLoopRef m_cf_run_loop = nullptr; // Protected by m_mutex

    bool m_stopped = false; // Protected by m_mutex

    bool m_returning = false;

    OperQueue m_post_operations; // Protected by m_mutex

    std::set<SocketImpl*> m_sockets;
    std::set<DeadlineTimerImpl*> m_timers;

    void attach_to_cf_run_loop() noexcept;
    void detach_from_cf_run_loop() noexcept;
    void wake_up() noexcept;

    static void wake_up_callback(void* info)
    {
        EventLoopImpl& event_loop = *static_cast<EventLoopImpl*>(info);
        event_loop.wake_up_callback(); // Throws
    }

    void wake_up_callback()
    {
        if (m_returning)
            return;
        {
            LockGuard lg(m_mutex);
            if (m_stopped) {
                CFRunLoopStop(m_cf_run_loop);
                m_returning = true;
                return;
            }
            m_completed_operations.push_back(m_post_operations); // Does not throw
        }
        process_completed_operations(); // Throws
    }
};



class SocketImpl: public Socket {
public:
    SocketImpl(EventLoopImpl& event_loop):
        m_event_loop(event_loop),
        m_read_buffer(new char[s_read_buffer_size]) // Throws
    {
    }

    ~SocketImpl() noexcept override
    {
        close(); // close() is virtual, but final

        m_event_loop.remove_socket(this);
    }

    void async_connect(std::string host, port_type port, SocketSecurity security,
                       ConnectCompletionHandler handler) override
    {
        do_async_connect(std::move(host), port, security, std::move(handler)); // Throws
    }

    void async_read(char* buffer, size_t size, ReadCompletionHandler handler) override
    {
        Optional<char> delim;
        do_async_read(buffer, size, delim, std::move(handler)); // Throws
    }

    void async_read_until(char* buffer, size_t size, char delim,
                          ReadCompletionHandler handler) override
    {
        do_async_read(buffer, size, delim, std::move(handler)); // Throws
    }

    void async_write(const char* data, size_t size, WriteCompletionHandler handler) override
    {
        do_async_write(data, size, std::move(handler)); // Throws
    }

    void close() noexcept override final
    {
        cancel();
        if (m_read_stream)
            discard_streams();
        m_is_connected = false;
    }

    void cancel() noexcept override final
    {
        if (m_connect_oper) {
            REALM_ASSERT(!m_is_connected);
            REALM_ASSERT(!m_read_oper && !m_write_oper);
            on_connect_complete(error::operation_aborted);
            discard_streams();
            return;
        }
        if (m_read_oper)
            on_read_complete(error::operation_aborted);
        if (m_write_oper)
            on_write_complete(error::operation_aborted);
    }

    void attach_to_cf_run_loop(CFRunLoopRef cf_run_loop) noexcept
    {
        m_cf_run_loop = cf_run_loop;
        if (m_connect_oper || m_read_oper) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }
        if (m_write_oper) {
            CFWriteStreamScheduleWithRunLoop(m_write_stream.get(), m_cf_run_loop,
                                             kCFRunLoopDefaultMode);
        }
    }

    void detach_from_cf_run_loop() noexcept
    {
        if (m_connect_oper || m_read_oper) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
        if (m_write_oper) {
            CFWriteStreamUnscheduleFromRunLoop(m_write_stream.get(), m_cf_run_loop,
                                               kCFRunLoopDefaultMode);
        }
        m_cf_run_loop = nullptr;
    }

private:
    EventLoopImpl& m_event_loop;

    // Not null when, and only when the event loop is running (a thread is
    // executing EventLoopImpl::run()).
    CFRunLoopRef m_cf_run_loop = nullptr;

    ReleaseGuard<CFReadStreamRef> m_read_stream;
    ReleaseGuard<CFWriteStreamRef> m_write_stream;

    std::unique_ptr<ConnectOper> m_connect_oper;
    std::unique_ptr<ReadOper> m_read_oper;
    std::unique_ptr<WriteOper> m_write_oper;

    bool m_is_connected = false;
    Optional<char> m_read_delim;

    char* m_read_begin;
    char* m_read_curr;
    char* m_read_end;

    const char* m_write_begin;
    const char* m_write_curr;
    const char* m_write_end;

    char* m_read_buffer_begin;
    char* m_read_buffer_end;

    static const size_t s_read_buffer_size = 1024;
    const std::unique_ptr<char[]> m_read_buffer;

    void do_async_connect(std::string host, port_type port, SocketSecurity security,
                          ConnectCompletionHandler handler)
    {
        REALM_ASSERT(!m_is_connected);
        // A connect operation must not be in progress
        REALM_ASSERT(!m_read_stream && !m_write_stream);
        REALM_ASSERT(!m_read_oper && !m_write_oper);

        ReleaseGuard<CFStringRef> host_2 = make_cf_string(std::move(host)); // Throws
        UInt32 port_2 = UInt32(port);
        CFReadStreamRef  read_stream;
        CFWriteStreamRef write_stream;
        CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, host_2.get(), port_2,
                                           &read_stream, &write_stream);
        ReleaseGuard<CFReadStreamRef> read_stream_2(read_stream);
        ReleaseGuard<CFWriteStreamRef> write_stream_2(write_stream);
        if (!read_stream_2 || !write_stream_2)
            throw std::bad_alloc();

        set_security_level(read_stream_2.get(), write_stream_2.get(), security); // Throws
        set_io_callbacks(read_stream_2.get(), write_stream_2.get()); // Throws

        Boolean success_1 = CFReadStreamOpen(read_stream_2.get());
        Boolean success_2 = CFWriteStreamOpen(write_stream_2.get());
        if (!success_1 || !success_2) {
            if (success_1)
                CFReadStreamClose(read_stream_2.get());
            if (success_2)
                CFWriteStreamClose(write_stream_2.get());
            throw std::runtime_error("Failed to open socket streams");
        }

        std::unique_ptr<ConnectOper> oper(new ConnectOper); // Throws
        oper->handler = std::move(handler);

        m_read_stream  = std::move(read_stream_2);
        m_write_stream = std::move(write_stream_2);
        m_connect_oper = std::move(oper);
        ++m_event_loop.num_operations_in_progress;

        if (m_cf_run_loop) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }

        // Discard previopusly buffered input
        m_read_buffer_begin = m_read_buffer.get();
        m_read_buffer_end   = m_read_buffer_begin;
    }

    void do_async_read(char* buffer, size_t size, Optional<char> delim,
                       ReadCompletionHandler handler)
    {
        REALM_ASSERT(m_is_connected);

        // A read operation must not be in progress
        REALM_ASSERT(!m_read_oper);

        std::unique_ptr<ReadOper> oper(new ReadOper); // Throws
        oper->handler = std::move(handler);

        m_read_begin = buffer;
        m_read_curr  = buffer;
        m_read_end   = buffer + size;
        m_read_delim = delim;
        m_read_oper  = std::move(oper);
        ++m_event_loop.num_operations_in_progress;

        bool did_complete = process_buffered_input();

        if (!did_complete && m_cf_run_loop) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }
    }

    void do_async_write(const char* data, size_t size, WriteCompletionHandler handler)
    {
        REALM_ASSERT(m_is_connected);

        // A write operation must not be in progress
        REALM_ASSERT(!m_write_oper);

        std::unique_ptr<WriteOper> oper(new WriteOper); // Throws
        oper->handler = std::move(handler);

        m_write_begin = data;
        m_write_curr  = data;
        m_write_end   = data + size;
        m_write_oper  = std::move(oper);
        ++m_event_loop.num_operations_in_progress;

        bool is_complete = (m_write_curr == m_write_end);
        if (is_complete) {
            on_write_complete();
        }
        else if (m_cf_run_loop) {
            CFWriteStreamScheduleWithRunLoop(m_write_stream.get(), m_cf_run_loop,
                                             kCFRunLoopDefaultMode);
        }
    }

    static void set_security_level(CFReadStreamRef  read_stream, CFWriteStreamRef write_stream,
                                   SocketSecurity sec)
    {
        switch (sec) {
            case SocketSecurity::None:
                return;
            case SocketSecurity::TLSv1: {
                Boolean success_1 = CFReadStreamSetProperty(read_stream,
                                                            kCFStreamPropertySocketSecurityLevel,
                                                            kCFStreamSocketSecurityLevelTLSv1);
                Boolean success_2 = CFWriteStreamSetProperty(write_stream,
                                                             kCFStreamPropertySocketSecurityLevel,
                                                             kCFStreamSocketSecurityLevelTLSv1);
                if (!success_1 || !success_2)
                    throw std::runtime_error("Failed to enable TLSv1");
                return;
            }
        }
        REALM_ASSERT(false);
    }

    void set_io_callbacks(CFReadStreamRef  read_stream, CFWriteStreamRef write_stream)
    {
        CFStreamClientContext context;
        context.version = 0;
        context.info = this;
        context.retain = nullptr;
        context.release = nullptr;
        context.copyDescription = nullptr;

        CFOptionFlags read_flags = (kCFStreamEventOpenCompleted  |
                                    kCFStreamEventErrorOccurred  |
                                    kCFStreamEventEndEncountered |
                                    kCFStreamEventHasBytesAvailable);

        CFOptionFlags write_flags = (kCFStreamEventErrorOccurred  |
                                     kCFStreamEventEndEncountered |
                                     kCFStreamEventCanAcceptBytes);

        Boolean success_1 = CFReadStreamSetClient(read_stream, read_flags,
                                                  &SocketImpl::read_callback, &context);
        Boolean success_2 = CFWriteStreamSetClient(write_stream, write_flags,
                                                   &SocketImpl::write_callback, &context);
        if (!success_1 || !success_2)
            throw std::runtime_error("Failed to set I/O callbacks");
    }

    static void read_callback(CFReadStreamRef stream, CFStreamEventType event_type, void* info)
    {
        SocketImpl& socket = *static_cast<SocketImpl*>(info);
        socket.read_callback(stream, event_type); // Throws
    }

    static void write_callback(CFWriteStreamRef stream, CFStreamEventType event_type, void* info)
    {
        SocketImpl& socket = *static_cast<SocketImpl*>(info);
        socket.write_callback(stream, event_type); // Throws
    }

    void read_callback(CFReadStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_read_stream.get());
        switch (event_type) {
            case kCFStreamEventOpenCompleted: {
                m_is_connected = true;
                on_connect_complete();
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventHasBytesAvailable: {
                std::error_code ec; // Success
                size_t n = read_some(m_read_buffer.get(), s_read_buffer_size, ec); // Throws
                if (REALM_UNLIKELY(n == 0)) {
                    // Read error
                    REALM_ASSERT(ec);
                    on_read_complete(ec);
                    m_event_loop.process_completed_operations(); // Throws
                    return;
                }
                REALM_ASSERT(!ec);
                m_read_buffer_begin = m_read_buffer.get();
                m_read_buffer_end = m_read_buffer_begin + n;
                bool did_complete = process_buffered_input();
                if (did_complete)
                    m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventErrorOccurred: {
                bool is_write_error = false;
                std::error_code ec = get_error(is_write_error); // Throws
                REALM_ASSERT(m_connect_oper || m_read_oper);
                if (m_connect_oper) {
                    on_connect_complete(ec);
                }
                else {
                    on_read_complete(ec);
                }
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventEndEncountered: {
                on_read_complete(network::end_of_input);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
        }
        REALM_ASSERT(false);
    }

    void write_callback(CFWriteStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_write_stream.get());
        switch (event_type) {
            case kCFStreamEventCanAcceptBytes: {
                std::error_code ec; // Success
                size_t n = write_some(m_write_curr, m_write_end - m_write_curr, ec); // Throws
                if (REALM_UNLIKELY(n == 0)) {
                    // Write error
                    REALM_ASSERT(ec);
                    on_write_complete(ec);
                    m_event_loop.process_completed_operations(); // Throws
                    return;
                }
                REALM_ASSERT(!ec);
                REALM_ASSERT(n <= size_t(m_write_end - m_write_curr));
                m_write_curr += n;
                bool is_complete = (m_write_curr == m_write_end);
                if (is_complete) {
                    on_write_complete(ec);
                    m_event_loop.process_completed_operations(); // Throws
                }
                return;
            }
            case kCFStreamEventErrorOccurred: {
                bool is_write_error = true;
                std::error_code ec = get_error(is_write_error); // Throws
                on_write_complete(ec);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventEndEncountered: {
                on_write_complete(error::connection_reset);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
        }
        REALM_ASSERT(false);
    }


    // Equivalent to network::socket::read_some(). `ec` untouched on success.
    size_t read_some(char* buffer, size_t size, std::error_code& ec)
    {
        static_assert(std::is_same<UInt8, char>::value || std::is_same<UInt8, unsigned char>::value,
                      "Unexpected byte type");
        UInt8* buffer_2 = reinterpret_cast<UInt8*>(buffer);
        REALM_ASSERT(!int_cast_has_overflow<CFIndex>(size));
        CFIndex buffer_length = CFIndex(size);
        CFIndex n = CFReadStreamRead(m_read_stream.get(), buffer_2, buffer_length);
        if (REALM_LIKELY(n > 0))
            return n;
        if (REALM_LIKELY(n == 0)) {
            ec = make_error_code(network::end_of_input);
            return 0;
        }
        REALM_ASSERT(n == -1);
        bool is_write_error = false;
        ec = get_error(is_write_error); // Throws
        return 0;
    }

    // Equivalent to network::socket::write_some(). `ec` untouched on success.
    size_t write_some(const char* data, size_t size, std::error_code& ec)
    {
        static_assert(std::is_same<UInt8, char>::value || std::is_same<UInt8, unsigned char>::value,
                      "Unexpected byte type");
        const UInt8* buffer = reinterpret_cast<const UInt8*>(data);
        REALM_ASSERT(!int_cast_has_overflow<CFIndex>(size));
        CFIndex buffer_length = CFIndex(size);
        CFIndex n = CFWriteStreamWrite(m_write_stream.get(), buffer, buffer_length);
        if (REALM_LIKELY(n > 0))
            return n;
        REALM_ASSERT(n == -1);
        bool is_write_error = true;
        ec = get_error(is_write_error); // Throws
        return 0;
    }

    bool process_buffered_input() noexcept
    {
        size_t in_avail = m_read_buffer_end - m_read_buffer_begin;
        size_t out_avail = m_read_end - m_read_curr;
        size_t n = std::min(in_avail, out_avail);
        char* i = !m_read_delim ? m_read_buffer_begin + n :
            std::find(m_read_buffer_begin, m_read_buffer_begin + n, *m_read_delim);
        m_read_curr = std::copy(m_read_buffer_begin, i, m_read_curr);
        m_read_buffer_begin = i;
        std::error_code ec; // Success
        if (m_read_curr == m_read_end) {
            if (m_read_delim)
                ec = network::delim_not_found;
        }
        else {
            if (m_read_buffer_begin == m_read_buffer_end)
                return false; // Incomplete
            REALM_ASSERT(m_read_delim);
            *m_read_curr++ = *m_read_buffer_begin++; // Transfer delimiter
        }
        on_read_complete(ec);
        return true; // Complete
    }

    void on_connect_complete(std::error_code ec = std::error_code()) noexcept
    {
        REALM_ASSERT(m_connect_oper);
        m_connect_oper->ec = ec;
        m_event_loop.add_completed_operation(std::move(m_connect_oper));
        m_connect_oper.reset();
        --m_event_loop.num_operations_in_progress;
        if (m_cf_run_loop) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
    }

    void on_read_complete(std::error_code ec = std::error_code()) noexcept
    {
        REALM_ASSERT(m_read_oper);
        m_read_oper->ec = ec;
        m_read_oper->n = m_read_curr - m_read_begin;
        m_event_loop.add_completed_operation(std::move(m_read_oper));
        m_read_oper.reset();
        --m_event_loop.num_operations_in_progress;
        if (m_cf_run_loop) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
    }

    void on_write_complete(std::error_code ec = std::error_code()) noexcept
    {
        REALM_ASSERT(m_write_oper);
        m_write_oper->ec = ec;
        m_event_loop.add_completed_operation(std::move(m_write_oper));
        m_write_oper.reset();
        --m_event_loop.num_operations_in_progress;
        if (m_cf_run_loop) {
            CFWriteStreamUnscheduleFromRunLoop(m_write_stream.get(), m_cf_run_loop,
                                               kCFRunLoopDefaultMode);
        }
    }

    void discard_streams() noexcept
    {
        REALM_ASSERT(m_read_stream && m_write_stream);
        CFReadStreamClose(m_read_stream.get());
        CFWriteStreamClose(m_write_stream.get());
        m_read_stream.reset();
        m_write_stream.reset();
    }

    std::error_code get_error(bool is_write_error)
    {
        ReleaseGuard<CFErrorRef> error;
        if (is_write_error) {
            REALM_ASSERT(CFWriteStreamGetStatus(m_write_stream.get()) == kCFStreamStatusError);
            error.reset(CFWriteStreamCopyError(m_write_stream.get()));
        }
        else {
            REALM_ASSERT(CFReadStreamGetStatus(m_read_stream.get()) == kCFStreamStatusError);
            error.reset(CFReadStreamCopyError(m_read_stream.get()));
        }
        if (!error)
            throw std::bad_alloc();
        CFStringRef domain = CFErrorGetDomain(error.get());
        if (CFStringCompare(domain, kCFErrorDomainPOSIX, 0) == kCFCompareEqualTo) {
            CFIndex code = CFErrorGetCode(error.get());
            return make_basic_system_error_code(int(code));
        }
        else if (CFStringCompare(domain, kCFErrorDomainCFNetwork, 0) == kCFCompareEqualTo) {
            CFIndex code = CFErrorGetCode(error.get());
            if (code == 2) {
                ReleaseGuard<CFDictionaryRef> user_info(CFErrorCopyUserInfo(error.get()));
                if (!user_info)
                    throw std::bad_alloc();
                const void* value =
                    CFDictionaryGetValue(user_info.get(), kCFGetAddrInfoFailureKey);
                if (value && CFGetTypeID(value) == CFNumberGetTypeID()) {
                    CFNumberRef value_2 = static_cast<CFNumberRef>(value);
                    int addrinfo_error_code = 0;
                    if (CFNumberGetValue(value_2, kCFNumberIntType, &addrinfo_error_code))
                        return translate_addrinfo_error(addrinfo_error_code);
                }
            }
        }
/*
        ReleaseGuard<CFDataRef> domain_2;
        domain_2.reset(CFStringCreateExternalRepresentation(kCFAllocatorDefault, domain,
                                                            kCFStringEncodingUTF8, '?'));
        if (!domain_2)
            throw std::bad_alloc();
        static_assert(std::is_same<UInt8, char>::value || std::is_same<UInt8, unsigned char>::value,
                      "Unexpected byte type");
        std::string domain_3(reinterpret_cast<const char*>(CFDataGetBytePtr(domain_2.get())),
                             CFDataGetLength(domain_2.get())); // Throws
        std::cerr << "Error domain is '" << domain_3 << "'\n";
        std::cerr << "Error code is   " << CFErrorGetCode(error.get()) << "\n";
*/
        return error::unknown;
    }

    std::error_code translate_addrinfo_error(int err) noexcept
    {
        switch (err) {
            case EAI_AGAIN:
                return network::host_not_found_try_again;
            case EAI_BADFLAGS:
                return error::invalid_argument;
            case EAI_FAIL:
                return network::no_recovery;
            case EAI_FAMILY:
                return error::address_family_not_supported;
            case EAI_MEMORY:
                return error::no_memory;
            case EAI_NONAME:
#if defined(EAI_ADDRFAMILY)
            case EAI_ADDRFAMILY:
#endif
#if defined(EAI_NODATA) && (EAI_NODATA != EAI_NONAME)
            case EAI_NODATA:
#endif
                return network::host_not_found;
            case EAI_SERVICE:
                return network::service_not_found;
            case EAI_SOCKTYPE:
                return network::socket_type_not_supported;
            default:
                return error::unknown;
        }
    }
};



class DeadlineTimerImpl: public DeadlineTimer {
public:
    DeadlineTimerImpl(EventLoopImpl& event_loop):
        m_event_loop(event_loop)
    {
        CFAbsoluteTime fire_date = 0; // Set later
        CFTimeInterval interval = 1.0; // Enable repetition to prevent invalidation
        CFOptionFlags flags = 0;
        CFIndex order = 0;
        CFRunLoopTimerCallBack callout = &DeadlineTimerImpl::wait_callback;
        CFRunLoopTimerContext context;
        context.version = 0;
        context.info = this;
        context.retain = nullptr;
        context.release = nullptr;
        context.copyDescription = nullptr;
        ReleaseGuard<CFRunLoopTimerRef> cf_timer;
        cf_timer.reset(CFRunLoopTimerCreate(kCFAllocatorDefault, fire_date, interval, flags, order,
                                            callout, &context));
        if (!cf_timer)
            throw std::bad_alloc();

        m_cf_timer = std::move(cf_timer);
    }

    ~DeadlineTimerImpl() noexcept override
    {
        cancel(); // cancel() is virtual, but final

        m_event_loop.remove_timer(this);
    }

    void async_wait(Duration duration, WaitCompletionHandler handler) override
    {
        // A wait operation must not be in progress
        REALM_ASSERT(!m_wait_oper);

        std::unique_ptr<WaitOper> oper(new WaitOper); // Throws
        oper->handler = std::move(handler);

        CFAbsoluteTime fire_date = CFAbsoluteTimeGetCurrent() + double(duration.count()) / 1000;
        CFRunLoopTimerSetNextFireDate(m_cf_timer.get(), fire_date);

        m_wait_oper = std::move(oper);
        ++m_event_loop.num_operations_in_progress;

        if (m_cf_run_loop)
            CFRunLoopAddTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
    }

    void cancel() noexcept override final
    {
        if (m_wait_oper)
            on_wait_complete(error::operation_aborted);
    }

    void attach_to_cf_run_loop(CFRunLoopRef cf_run_loop) noexcept
    {
        m_cf_run_loop = cf_run_loop;
        if (m_wait_oper)
            CFRunLoopAddTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
    }

    void detach_from_cf_run_loop() noexcept
    {
        if (m_wait_oper)
            CFRunLoopRemoveTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
        m_cf_run_loop = nullptr;
    }

private:
    EventLoopImpl& m_event_loop;

    // Not null when, and only when the event loop is running (a thread is
    // executing EventLoopImpl::run()).
    CFRunLoopRef m_cf_run_loop = nullptr;

    ReleaseGuard<CFRunLoopTimerRef> m_cf_timer;

    std::unique_ptr<WaitOper> m_wait_oper;

    static void wait_callback(CFRunLoopTimerRef cf_timer, void* info)
    {
        DeadlineTimerImpl& timer = *static_cast<DeadlineTimerImpl*>(info);
        timer.wait_callback(cf_timer); // Throws
    }

    void wait_callback(CFRunLoopTimerRef cf_timer)
    {
        REALM_ASSERT(cf_timer == m_cf_timer.get());
        on_wait_complete(); // Throws
        m_event_loop.process_completed_operations(); // Throws
    }

    void on_wait_complete(std::error_code ec = std::error_code()) noexcept
    {
        REALM_ASSERT(m_wait_oper);
        m_wait_oper->ec = ec;
        m_event_loop.add_completed_operation(std::move(m_wait_oper));
        m_wait_oper.reset();
        --m_event_loop.num_operations_in_progress;
        if (m_cf_run_loop)
            CFRunLoopRemoveTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
    }
};


EventLoopImpl::EventLoopImpl()
{
    CFIndex order = 0;
    CFRunLoopSourceContext context;
    context.version = 0;
    context.info = this;
    context.retain = nullptr;
    context.release = nullptr;
    context.copyDescription = nullptr;
    context.equal = nullptr;
    context.hash = nullptr;
    context.schedule = nullptr; // Called when added to run loop
    context.cancel = nullptr; // Called when removed from run loop
    context.perform = &EventLoopImpl::wake_up_callback;
    ReleaseGuard<CFRunLoopSourceRef> source;
    source.reset(CFRunLoopSourceCreate(kCFAllocatorDefault, order, &context));
    if (!source)
        throw std::bad_alloc();

    m_wake_up_source = std::move(source);
}


EventLoopImpl::~EventLoopImpl() noexcept
{
    REALM_ASSERT(m_sockets.empty());
    REALM_ASSERT(m_timers.empty());
}


std::unique_ptr<Socket> EventLoopImpl::make_socket()
{
    std::unique_ptr<SocketImpl> socket(new SocketImpl(*this)); // Throws
    m_sockets.insert(socket.get()); // Throws
    if (m_cf_run_loop)
        socket->attach_to_cf_run_loop(m_cf_run_loop);
    return std::move(socket);
}


std::unique_ptr<DeadlineTimer> EventLoopImpl::make_timer()
{
    std::unique_ptr<DeadlineTimerImpl> timer(new DeadlineTimerImpl(*this)); // Throws
    m_timers.insert(timer.get()); // Throws
    if (m_cf_run_loop)
        timer->attach_to_cf_run_loop(m_cf_run_loop);
    return std::move(timer);
}


void EventLoopImpl::post(PostCompletionHandler handler)
{
    std::unique_ptr<PostOper> oper(new PostOper); // Throws
    oper->handler = std::move(handler);
    LockGuard lg(m_mutex);
    m_post_operations.push_back(std::move(oper));
    wake_up();
}


void EventLoopImpl::run()
{
    {
        LockGuard lg(m_mutex);
        attach_to_cf_run_loop();
    }

    auto handler = [this]() noexcept {
        LockGuard lg(m_mutex);
        detach_from_cf_run_loop();
    };
    auto seg = make_scope_exit(handler);

    m_returning = false;
    for (;;) {
        // Make sure that the wake up handler is invoked before CFRunLoopRun()
        // goes to sleep. In general this will happen when required, because
        // CFRunLoopSourceSignal() is called from wake_up(), but we need to make
        // sure it is called again in cases such as when a completion handler
        // throws and leaves post handlers behind.
        CFRunLoopSourceSignal(m_wake_up_source.get());

        // Because of the presence of the custom wake up source, CFRunLoopRun() only
        // exits when a handler calls CFRunLoopStop() on this run loop.
        //
        // Exceptions thrown by input source handlers will propagate out through
        // CFRunLoopRun().
        //
        // FIXME: What about timer handlers and exceptions?
        CFRunLoopRun(); // Throws

        if (m_returning)
            break;
    }
}


void EventLoopImpl::stop() noexcept
{
    LockGuard lg(m_mutex);
    m_stopped = true;
    wake_up();
}


void EventLoopImpl::reset() noexcept
{
    LockGuard lg(m_mutex);
    m_stopped = false;
}


// Caller must hold a lock on `m_mutex`.
void EventLoopImpl::attach_to_cf_run_loop() noexcept
{
    m_cf_run_loop = CFRunLoopGetCurrent();

    CFRunLoopAddSource(m_cf_run_loop, m_wake_up_source.get(), kCFRunLoopDefaultMode);

    for (SocketImpl* s: m_sockets)
        s->attach_to_cf_run_loop(m_cf_run_loop);
    for (DeadlineTimerImpl* t: m_timers)
        t->attach_to_cf_run_loop(m_cf_run_loop);
}


// Caller must hold a lock on `m_mutex`.
void EventLoopImpl::detach_from_cf_run_loop() noexcept
{
    CFRunLoopRemoveSource(m_cf_run_loop, m_wake_up_source.get(), kCFRunLoopDefaultMode);

    for (SocketImpl* s: m_sockets)
        s->detach_from_cf_run_loop();
    for (DeadlineTimerImpl* t: m_timers)
        t->detach_from_cf_run_loop();

    m_cf_run_loop = nullptr;
}


// Caller must hold a lock on `m_mutex`.
void EventLoopImpl::wake_up() noexcept
{
    CFRunLoopSourceSignal(m_wake_up_source.get());
    if (m_cf_run_loop)
        CFRunLoopWakeUp(m_cf_run_loop);
}


class ImplementationImpl: public EventLoop::Implementation {
public:
    std::string name() const
    {
        return "apple-cf";
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

EventLoop::Implementation& get_apple_cf_event_loop_impl()
{
    return g_implementation;
}

} // namespace _impl
} // namespace realm
