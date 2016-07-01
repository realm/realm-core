#include <realm/util/features.h>
#include <realm/util/event_loop.hpp>

#if REALM_PLATFORM_APPLE && !REALM_WATCHOS
// The Apple Core Foundation based implementation is currently not working
// correctly. When reenabling it, remember to also reenable it in
// test_util_event_loop.cpp.
#  define HAVE_APPLE_CF_IMPLEMENTATION 0
//#  define HAVE_APPLE_CF_IMPLEMENTATION 1
#else
#  define HAVE_APPLE_CF_IMPLEMENTATION 0
#endif


#if HAVE_APPLE_CF_IMPLEMENTATION

#include <type_traits>
#include <algorithm>
#include <stdexcept>
#include <utility>
#include <set>

#include <realm/util/optional.hpp>
#include <realm/util/bind_ptr.hpp>
#include <realm/util/cf_ptr.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/safe_int_ops.hpp>
#include <realm/util/misc_errors.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/network.hpp>

// https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/NetworkingTopics/Articles/UsingSocketsandSocketStreams.html#//apple_ref/doc/uid/CH73-SW4
// https://developer.apple.com/library/mac/documentation/CoreFoundation/Conceptual/CFMemoryMgmt/CFMemoryMgmt.html

#include <CoreFoundation/CoreFoundation.h>
#include <CFNetwork/CFNetwork.h>

using namespace realm;
using namespace realm::util;

namespace {

CFPtr<CFStringRef> make_cf_string(std::string str)
{
    static_assert(std::is_same<UInt8, char>::value || std::is_same<UInt8, unsigned char>::value,
                  "Unexpected byte type");
    const UInt8* bytes = reinterpret_cast<const UInt8*>(str.data());
    Boolean is_external_representation = FALSE;
    CFStringRef str_2 = CFStringCreateWithBytes(kCFAllocatorDefault, bytes, str.size(),
                                                kCFStringEncodingUTF8, is_external_representation);
    if (!str_2)
        throw std::bad_alloc();
    return CFPtr<CFStringRef>(str_2);
}


class Oper: public RefCountBase {
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
    void initiate(EventLoop::PostCompletionHandler handler) noexcept
    {
        REALM_ASSERT(!m_handler);
        m_handler = std::move(handler);
    }
    void execute() override final
    {
        REALM_ASSERT(m_handler);
        EventLoop::PostCompletionHandler handler = std::move(m_handler);
        m_handler = EventLoop::PostCompletionHandler();
        handler(); // Throws
    }
private:
    EventLoop::PostCompletionHandler m_handler;
};


class ConnectOper: public Oper {
public:
    bool in_progress() const noexcept
    {
        return bool(m_handler);
    }
    bool is_complete() const noexcept
    {
        return in_progress() && m_complete;
    }
    bool is_incomplete() const noexcept
    {
        return in_progress() && !m_complete;
    }
    void initiate(Socket::ConnectCompletionHandler handler) noexcept
    {
        REALM_ASSERT(!in_progress());
        m_handler = std::move(handler);
        m_complete = false;
    }
    void cancel() noexcept
    {
        REALM_ASSERT(in_progress());
        m_error_code = error::operation_aborted;
    }
    void complete(std::error_code ec) noexcept
    {
        REALM_ASSERT(is_incomplete());
        m_error_code = ec;
        m_complete = true;
    }
    void execute() override final
    {
        REALM_ASSERT(is_complete());
        Socket::ConnectCompletionHandler handler = std::move(m_handler);
        m_handler = Socket::ConnectCompletionHandler();
        handler(m_error_code); // Throws
    }
private:
    Socket::ConnectCompletionHandler m_handler;
    std::error_code m_error_code;
    bool m_complete = false;
};


class ReadOper: public Oper {
public:
    bool in_progress() const noexcept
    {
        return bool(m_handler);
    }
    bool is_complete() const noexcept
    {
        return in_progress() && m_complete;
    }
    bool is_incomplete() const noexcept
    {
        return in_progress() && !m_complete;
    }
    void initiate(Socket::ReadCompletionHandler handler) noexcept
    {
        REALM_ASSERT(!in_progress());
        m_handler = std::move(handler);
        m_complete = false;
    }
    void cancel() noexcept
    {
        REALM_ASSERT(in_progress());
        m_error_code = error::operation_aborted;
    }
    void complete(std::error_code ec, size_t num_bytes_read) noexcept
    {
        REALM_ASSERT(is_incomplete());
        m_error_code = ec;
        m_num_bytes_read = num_bytes_read;
        m_complete = true;
    }
    void execute() override final
    {
        REALM_ASSERT(is_complete());
        Socket::ReadCompletionHandler handler = std::move(m_handler);
        m_handler = Socket::ReadCompletionHandler();
        handler(m_error_code, m_num_bytes_read); // Throws
    }
private:
    Socket::ReadCompletionHandler m_handler;
    std::error_code m_error_code;
    size_t m_num_bytes_read = 0;
    bool m_complete = false;
};


class WriteOper: public Oper {
public:
    bool in_progress() const noexcept
    {
        return bool(m_handler);
    }
    bool is_complete() const noexcept
    {
        return in_progress() && m_complete;
    }
    bool is_incomplete() const noexcept
    {
        return in_progress() && !m_complete;
    }
    void initiate(Socket::WriteCompletionHandler handler) noexcept
    {
        REALM_ASSERT(!in_progress());
        m_handler = std::move(handler);
        m_complete = false;
    }
    void cancel() noexcept
    {
        REALM_ASSERT(in_progress());
        m_error_code = error::operation_aborted;
    }
    void complete(std::error_code ec, size_t num_bytes_written) noexcept
    {
        REALM_ASSERT(is_incomplete());
        m_error_code = ec;
        m_num_bytes_written = num_bytes_written;
        m_complete = true;
    }
    void execute() override final
    {
        REALM_ASSERT(is_complete());
        Socket::WriteCompletionHandler handler = std::move(m_handler);
        m_handler = Socket::WriteCompletionHandler();
        handler(m_error_code, m_num_bytes_written); // Throws
    }
private:
    Socket::WriteCompletionHandler m_handler;
    std::error_code m_error_code;
    size_t m_num_bytes_written = 0;
    bool m_complete = false;
};


class WaitOper: public Oper {
public:
    bool in_progress() const noexcept
    {
        return bool(m_handler);
    }
    bool is_complete() const noexcept
    {
        return in_progress() && m_complete;
    }
    bool is_incomplete() const noexcept
    {
        return in_progress() && !m_complete;
    }
    void initiate(DeadlineTimer::WaitCompletionHandler handler) noexcept
    {
        REALM_ASSERT(!in_progress());
        m_handler = std::move(handler);
        m_complete = false;
    }
    void cancel() noexcept
    {
        REALM_ASSERT(in_progress());
        m_error_code = error::operation_aborted;
    }
    void complete(std::error_code ec) noexcept
    {
        REALM_ASSERT(is_incomplete());
        m_error_code = ec;
        m_complete = true;
    }
    void execute() override final
    {
        REALM_ASSERT(is_complete());
        DeadlineTimer::WaitCompletionHandler handler = std::move(m_handler);
        m_handler = DeadlineTimer::WaitCompletionHandler();
        handler(m_error_code); // Throws
    }
private:
    DeadlineTimer::WaitCompletionHandler m_handler;
    std::error_code m_error_code;
    bool m_complete = false;
};



// A queue of operations with a nonthrowing push_back() method. This is
// necessary to support a nonthrowing cancel() method on sockets and timers.
class OperQueue {
public:
    bool empty() const noexcept
    {
        return !m_back;
    }
    void push_back(bind_ptr<Oper> op) noexcept
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
    bind_ptr<Oper> pop_front() noexcept
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
        return bind_ptr<Oper>(op);
    }
    void clear() noexcept
    {
        if (m_back) {
            bind_ptr<Oper> op(m_back);
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
    // Number of asynchronous operations in progress (connect, read, write,
    // wait)
    int_fast64_t num_operations_in_progress = 0;

    EventLoopImpl();
    ~EventLoopImpl() noexcept override final;

    std::unique_ptr<Socket> make_socket() override final;
    std::unique_ptr<DeadlineTimer> make_timer() override final;
    void post(PostCompletionHandler) override final;
    void run() override final;
    void stop() noexcept override final;
    void reset() noexcept override final;

    void remove_socket(SocketImpl* socket) noexcept
    {
        m_sockets.erase(socket);
    }

    void remove_timer(DeadlineTimerImpl* timer) noexcept
    {
        m_timers.erase(timer);
    }

    void add_completed_operation(bind_ptr<Oper> oper) noexcept
    {
        m_completed_operations.push_back(std::move(oper));
    }

    void process_completed_operations()
    {
        // Note: Each handler execution can complete new operations, such as by
        // canceling operations in progress.
        while (bind_ptr<Oper> op = m_completed_operations.pop_front())
            op->execute(); // Throws

        // Stop event loop if there are no asynchronous operations in progress
        // and no post handlers waiting to be transferred from m_post_operations
        // to m_completed_operations
        if (num_operations_in_progress == 0) {
            bool no_pending_post_operations;
            {
                LockGuard lg(m_mutex);
                no_pending_post_operations = m_post_operations.empty();
            }
            if (no_pending_post_operations) {
                CFRunLoopStop(m_cf_run_loop); // Out of work
                m_returning = true;
            }
        }
    }

private:
    CFPtr<CFRunLoopSourceRef> m_wake_up_source;

    // Operations whose completion handlers are ready to be executed
    OperQueue m_completed_operations;

    Mutex m_mutex;

    // Refers to the CFRunLoop of the thread that is currently executing
    // run(). It is null when no thread is executing run(). It may refer to
    // different CFRunLoop objects at different times, if different threads call
    // run(), but run may only be executed by one thread at a time.
    //
    // If there are streams and timers attached to the CFRunLoop object when
    // run() returns, those streams and timers will be detached from the run
    // loop at that time. When the same thread, or another thread later calls
    // run() again, those streams and timers will be reattached to the CFRunLoop
    // of that thread, which may be a different thread than the one that called
    // run() previously.
    //
    // `m_cf_run_loop` is protected by `m_mutex` because wake_up() needs to
    // access it. Note that wake_up() generally executes asynchronously with
    // respect to the event loop thread. This is so, because wake_up() is called
    // from post() and stop() which both need to be thread-safe.
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
        m_connect_oper(new ConnectOper),
        m_read_oper(new ReadOper),
        m_write_oper(new WriteOper),
        m_read_buffer(new char[s_read_buffer_size]) // Throws
    {
    }

    ~SocketImpl() noexcept override final
    {
        close(); // close() is virtual, but final

        m_event_loop.remove_socket(this);
    }

    void async_connect(std::string host, port_type port, SocketSecurity security,
                       ConnectCompletionHandler handler) override final
    {
        do_async_connect(std::move(host), port, security, std::move(handler)); // Throws
    }

    void async_read(char* buffer, size_t size, ReadCompletionHandler handler) override final
    {
        Optional<char> delim;
        do_async_read(buffer, size, delim, std::move(handler)); // Throws
    }

    void async_read_until(char* buffer, size_t size, char delim,
                          ReadCompletionHandler handler) override final
    {
        do_async_read(buffer, size, delim, std::move(handler)); // Throws
    }

    void async_write(const char* data, size_t size, WriteCompletionHandler handler) override final
    {
        do_async_write(data, size, std::move(handler)); // Throws
    }

    void close() noexcept override final
    {
        cancel();
        discard_streams();
        m_is_connected = false;
    }

    void cancel() noexcept override final
    {
        if (m_connect_oper->in_progress()) {
            REALM_ASSERT(!m_is_connected);
            REALM_ASSERT(!m_read_oper->in_progress() && !m_write_oper->in_progress());
            if (m_connect_oper->is_incomplete())
                complete_connect();
            m_connect_oper->cancel();
            discard_streams();
            return;
        }
        if (m_read_oper->in_progress()) {
            if (m_read_oper->is_incomplete())
                complete_read();
            m_read_oper->cancel();
        }
        if (m_write_oper->in_progress()) {
            if (m_write_oper->is_incomplete())
                complete_write();
            m_write_oper->cancel();
        }
    }

    EventLoop& get_event_loop() noexcept override final
    {
        return m_event_loop;
    }

    void attach_to_cf_run_loop(CFRunLoopRef cf_run_loop) noexcept
    {
        REALM_ASSERT(!m_cf_run_loop);
        m_cf_run_loop = cf_run_loop;
        if (m_connect_oper->is_incomplete() || m_read_oper->is_incomplete()) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }
        if (m_write_oper->is_incomplete()) {
            CFWriteStreamScheduleWithRunLoop(m_write_stream.get(), m_cf_run_loop,
                                             kCFRunLoopDefaultMode);
        }
    }

    void detach_from_cf_run_loop() noexcept
    {
        REALM_ASSERT(m_cf_run_loop);
        if (m_connect_oper->is_incomplete() || m_read_oper->is_incomplete()) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
        if (m_write_oper->is_incomplete()) {
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

    CFPtr<CFReadStreamRef> m_read_stream;
    CFPtr<CFWriteStreamRef> m_write_stream;

    const bind_ptr<ConnectOper> m_connect_oper;
    const bind_ptr<ReadOper> m_read_oper;
    const bind_ptr<WriteOper> m_write_oper;

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
        REALM_ASSERT(!m_connect_oper->in_progress());
        REALM_ASSERT(!m_read_stream && !m_write_stream);
        REALM_ASSERT(!m_read_oper->in_progress() && !m_write_oper->in_progress());

        CFPtr<CFReadStreamRef> read_stream;
        CFPtr<CFWriteStreamRef> write_stream;
        {
            CFPtr<CFStringRef> host_2 = make_cf_string(std::move(host)); // Throws
            UInt32 port_2 = UInt32(port);
            CFReadStreamRef  read_stream_2;
            CFWriteStreamRef write_stream_2;
            CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, host_2.get(), port_2,
                                               &read_stream_2, &write_stream_2);
            read_stream.reset(read_stream_2);
            write_stream.reset(write_stream_2);
        }
        if (!read_stream || !write_stream)
            throw std::bad_alloc();

        set_security_level(read_stream.get(), write_stream.get(), security); // Throws
        set_io_callbacks(read_stream.get(), write_stream.get()); // Throws

        std::error_code ec;
        {
            Boolean success_1 = CFReadStreamOpen(read_stream.get());
            Boolean success_2 = CFWriteStreamOpen(write_stream.get());
            if (!success_1 || !success_2) {
                auto seh = [&]() noexcept {
                    if (success_1)
                        CFReadStreamClose(read_stream.get());
                    if (success_2)
                        CFWriteStreamClose(write_stream.get());
                    read_stream.reset();
                    write_stream.reset();
                };
                auto seg = util::make_scope_exit(seh);
                CFStreamStatus status_1 = CFReadStreamGetStatus(read_stream.get());
                CFStreamStatus status_2 = CFWriteStreamGetStatus(write_stream.get());
                if (success_1) {
                    REALM_ASSERT(status_1 != kCFStreamStatusNotOpen &&
                                 status_1 != kCFStreamStatusError);
                }
                else {
                    REALM_ASSERT(status_1 == kCFStreamStatusNotOpen ||
                                 status_1 == kCFStreamStatusError);
                    if (status_1 != kCFStreamStatusError)
                        throw std::runtime_error("Failed to open read stream");
                }
                if (success_2) {
                    REALM_ASSERT(status_2 != kCFStreamStatusNotOpen &&
                                 status_2 != kCFStreamStatusError);
                }
                else {
                    REALM_ASSERT(status_2 == kCFStreamStatusNotOpen ||
                                 status_2 == kCFStreamStatusError);
                    if (status_2 != kCFStreamStatusError)
                        throw std::runtime_error("Failed to open write stream");
                }
                if (!success_1) {
                    ec = get_error(read_stream.get()); // Throws
                }
                else {
                    ec = get_error(write_stream.get()); // Throws
                }
                if (!ec)
                    ec = error::unknown;
            }
        }

        m_read_stream  = std::move(read_stream);
        m_write_stream = std::move(write_stream);
        m_connect_oper->initiate(std::move(handler));
        ++m_event_loop.num_operations_in_progress;

        bool is_complete = bool(ec);
        if (is_complete) {
            bool not_yet_attached_to_cf_run_loop = true;
            complete_connect(ec, not_yet_attached_to_cf_run_loop);
        }
        else if (m_cf_run_loop) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }

        // Discard previously buffered input
        m_read_buffer_begin = m_read_buffer.get();
        m_read_buffer_end   = m_read_buffer_begin;
    }

    void do_async_read(char* buffer, size_t size, Optional<char> delim,
                       ReadCompletionHandler handler)
    {
        REALM_ASSERT(m_is_connected);

        // A read operation must not be in progress
        REALM_ASSERT(!m_read_oper->in_progress());

        m_read_begin = buffer;
        m_read_curr  = buffer;
        m_read_end   = buffer + size;
        m_read_delim = delim;
        m_read_oper->initiate(std::move(handler));
        ++m_event_loop.num_operations_in_progress;

        std::error_code ec; // Success
        bool is_complete = process_buffered_input(ec);
        if (is_complete) {
            bool not_yet_attached_to_cf_run_loop = true;
            complete_read(ec, not_yet_attached_to_cf_run_loop);
        }
        else if (m_cf_run_loop) {
            CFReadStreamScheduleWithRunLoop(m_read_stream.get(), m_cf_run_loop,
                                            kCFRunLoopDefaultMode);
        }
    }

    void do_async_write(const char* data, size_t size, WriteCompletionHandler handler)
    {
        REALM_ASSERT(m_is_connected);

        // A write operation must not be in progress
        REALM_ASSERT(!m_write_oper->in_progress());

        m_write_begin = data;
        m_write_curr  = data;
        m_write_end   = data + size;
        m_write_oper->initiate(std::move(handler));
        ++m_event_loop.num_operations_in_progress;

        std::error_code ec; // Success
        bool is_complete = (m_write_curr == m_write_end);
        if (is_complete) {
            bool not_yet_attached_to_cf_run_loop = true;
            complete_write(ec, not_yet_attached_to_cf_run_loop);
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
                complete_connect();
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventHasBytesAvailable: {
                REALM_ASSERT(m_read_oper->is_incomplete());
                std::error_code ec; // Success
                size_t n = read_some(m_read_buffer.get(), s_read_buffer_size, ec); // Throws
                if (REALM_UNLIKELY(n == 0)) {
                    // Read error
                    REALM_ASSERT(ec);
                    complete_read(ec);
                    m_event_loop.process_completed_operations(); // Throws
                    return;
                }
                REALM_ASSERT(!ec);
                m_read_buffer_begin = m_read_buffer.get();
                m_read_buffer_end = m_read_buffer_begin + n;
                bool is_complete = process_buffered_input(ec);
                if (is_complete) {
                    complete_read(ec);
                    m_event_loop.process_completed_operations(); // Throws
                }
                return;
            }
            case kCFStreamEventErrorOccurred: {
                // FIXME: It seems this this event never happens. Why is that?
                // (I have seen it happening once now, but it is still a mystery
                // why it happens so rarelygiven that we do simulate read errors
                // by closing the connection on the remote side)
                REALM_ASSERT(m_connect_oper->is_incomplete() != m_read_oper->is_incomplete());
                std::error_code ec = get_error(m_read_stream.get()); // Throws
                if (m_connect_oper->is_incomplete()) {
                    complete_connect(ec);
                }
                else {
                    complete_read(ec);
                }
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventEndEncountered: {
                // FIXME: It seems this this event never happens. Why is that?
                complete_read(network::end_of_input);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventNone:
                break;
        }
        REALM_ASSERT(false);
    }

    void write_callback(CFWriteStreamRef stream, CFStreamEventType event_type)
    {
        REALM_ASSERT(stream == m_write_stream.get());
        switch (event_type) {
            case kCFStreamEventCanAcceptBytes: {
                REALM_ASSERT(m_write_oper->is_incomplete());
                std::error_code ec; // Success
                size_t n = write_some(m_write_curr, m_write_end - m_write_curr, ec); // Throws
                if (REALM_UNLIKELY(n == 0)) {
                    // Write error
                    REALM_ASSERT(ec);
                    complete_write(ec);
                    m_event_loop.process_completed_operations(); // Throws
                    return;
                }
                REALM_ASSERT(!ec);
                REALM_ASSERT(n <= size_t(m_write_end - m_write_curr));
                m_write_curr += n;
                bool is_complete = (m_write_curr == m_write_end);
                if (is_complete) {
                    complete_write(ec);
                    m_event_loop.process_completed_operations(); // Throws
                }
                return;
            }
            case kCFStreamEventErrorOccurred: {
                // FIXME: It seems this this event never happens. Why is that?
                REALM_ASSERT(m_write_oper->is_incomplete());
                std::error_code ec = get_error(m_write_stream.get()); // Throws
                complete_write(ec);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventEndEncountered: {
                // FIXME: It seems this this event never happens. Why is that?
                complete_write(error::connection_reset);
                m_event_loop.process_completed_operations(); // Throws
                return;
            }
            case kCFStreamEventNone:
            case kCFStreamEventOpenCompleted:
            case kCFStreamEventHasBytesAvailable:
                break;
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
        ec = get_error(m_read_stream.get()); // Throws
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
        ec = get_error(m_write_stream.get()); // Throws
        return 0;
    }

    // Returns true on completion of read operation. Leaves `ec` untouched if
    // incomplete and if complete with success.
    bool process_buffered_input(std::error_code& ec) noexcept
    {
        size_t in_avail = m_read_buffer_end - m_read_buffer_begin;
        size_t out_avail = m_read_end - m_read_curr;
        size_t n = std::min(in_avail, out_avail);
        char* i = !m_read_delim ? m_read_buffer_begin + n :
            std::find(m_read_buffer_begin, m_read_buffer_begin + n, *m_read_delim);
        m_read_curr = std::copy(m_read_buffer_begin, i, m_read_curr);
        m_read_buffer_begin = i;
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
        return true; // Complete
    }

    void complete_connect(std::error_code ec = std::error_code(),
                          bool not_yet_attached_to_cf_run_loop = false) noexcept
    {
        m_connect_oper->complete(ec);
        bind_ptr<ConnectOper> oper = m_connect_oper;
        m_event_loop.add_completed_operation(std::move(oper));
        --m_event_loop.num_operations_in_progress;
        if (!not_yet_attached_to_cf_run_loop && m_cf_run_loop) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
    }

    void complete_read(std::error_code ec = std::error_code(),
                       bool not_yet_attached_to_cf_run_loop = false) noexcept
    {
        size_t n = size_t(m_read_curr - m_read_begin);
        m_read_oper->complete(ec, n);
        bind_ptr<ReadOper> oper = m_read_oper;
        m_event_loop.add_completed_operation(std::move(oper));
        --m_event_loop.num_operations_in_progress;
        if (!not_yet_attached_to_cf_run_loop && m_cf_run_loop) {
            CFReadStreamUnscheduleFromRunLoop(m_read_stream.get(), m_cf_run_loop,
                                              kCFRunLoopDefaultMode);
        }
    }

    void complete_write(std::error_code ec = std::error_code(),
                        bool not_yet_attached_to_cf_run_loop = false) noexcept
    {
        size_t n = size_t(m_write_curr - m_write_begin);
        m_write_oper->complete(ec, n);
        bind_ptr<WriteOper> oper = m_write_oper;
        m_event_loop.add_completed_operation(std::move(oper));
        --m_event_loop.num_operations_in_progress;
        if (!not_yet_attached_to_cf_run_loop && m_cf_run_loop) {
            CFWriteStreamUnscheduleFromRunLoop(m_write_stream.get(), m_cf_run_loop,
                                               kCFRunLoopDefaultMode);
        }
    }

    void discard_streams() noexcept
    {
        if (m_read_stream)
            CFReadStreamClose(m_read_stream.get());
        if (m_write_stream)
            CFWriteStreamClose(m_write_stream.get());
        m_read_stream.reset();
        m_write_stream.reset();
    }

    static std::error_code get_error(CFReadStreamRef read_stream)
    {
        REALM_ASSERT(CFReadStreamGetStatus(read_stream) == kCFStreamStatusError);
        CFPtr<CFErrorRef> error = adoptCF(CFReadStreamCopyError(read_stream));
        if (!error)
            throw std::bad_alloc();
        return translate_error(error.get()); // Throws
    }

    static std::error_code get_error(CFWriteStreamRef write_stream)
    {
        REALM_ASSERT(CFWriteStreamGetStatus(write_stream) == kCFStreamStatusError);
        CFPtr<CFErrorRef> error = adoptCF(CFWriteStreamCopyError(write_stream));
        if (!error)
            throw std::bad_alloc();
        return translate_error(error.get()); // Throws
    }

    static std::error_code translate_error(CFErrorRef error)
    {
        CFStringRef domain = CFErrorGetDomain(error);
        if (CFStringCompare(domain, kCFErrorDomainPOSIX, 0) == kCFCompareEqualTo) {
            CFIndex code = CFErrorGetCode(error);
            return make_basic_system_error_code(int(code));
        }
        else if (CFStringCompare(domain, kCFErrorDomainCFNetwork, 0) == kCFCompareEqualTo) {
            CFIndex code = CFErrorGetCode(error);
            if (code == 2) {
                CFPtr<CFDictionaryRef> user_info(CFErrorCopyUserInfo(error));
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
        CFPtr<CFDataRef> domain_2 = adoptCF(CFStringCreateExternalRepresentation(kCFAllocatorDefault, domain,
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

    static std::error_code translate_addrinfo_error(int err) noexcept
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
        m_event_loop(event_loop),
        m_wait_oper(new WaitOper) // Throws
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
        CFPtr<CFRunLoopTimerRef> cf_timer = adoptCF(CFRunLoopTimerCreate(kCFAllocatorDefault, fire_date, interval,
                                                                         flags, order, callout, &context));
        if (!cf_timer)
            throw std::bad_alloc();

        m_cf_timer = std::move(cf_timer);
    }

    ~DeadlineTimerImpl() noexcept override final
    {
        cancel();

        m_event_loop.remove_timer(this);
    }

    void async_wait(Duration duration, WaitCompletionHandler handler) override final
    {
        // A wait operation must not be in progress
        REALM_ASSERT(!m_wait_oper->in_progress());

        CFAbsoluteTime fire_date = CFAbsoluteTimeGetCurrent() + double(duration.count()) / 1000;
        CFRunLoopTimerSetNextFireDate(m_cf_timer.get(), fire_date);

        m_wait_oper->initiate(std::move(handler));
        ++m_event_loop.num_operations_in_progress;

        if (m_cf_run_loop)
            CFRunLoopAddTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
    }

    void cancel() noexcept override final
    {
        if (m_wait_oper->in_progress()) {
            if (m_wait_oper->is_incomplete())
                complete_wait();
            m_wait_oper->cancel();
        }
    }

    EventLoop& get_event_loop() noexcept override final
    {
        return m_event_loop;
    }

    void attach_to_cf_run_loop(CFRunLoopRef cf_run_loop) noexcept
    {
        REALM_ASSERT(!m_cf_run_loop);
        m_cf_run_loop = cf_run_loop;
        if (m_wait_oper->is_incomplete())
            CFRunLoopAddTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
    }

    void detach_from_cf_run_loop() noexcept
    {
        REALM_ASSERT(m_cf_run_loop);
        if (m_wait_oper->is_incomplete())
            CFRunLoopRemoveTimer(m_cf_run_loop, m_cf_timer.get(), kCFRunLoopDefaultMode);
        m_cf_run_loop = nullptr;
    }

private:
    EventLoopImpl& m_event_loop;

    // Not null when, and only when the event loop is running (a thread is
    // executing EventLoopImpl::run()).
    CFRunLoopRef m_cf_run_loop = nullptr;

    CFPtr<CFRunLoopTimerRef> m_cf_timer;

    const bind_ptr<WaitOper> m_wait_oper;

    static void wait_callback(CFRunLoopTimerRef cf_timer, void* info)
    {
        DeadlineTimerImpl& timer = *static_cast<DeadlineTimerImpl*>(info);
        timer.wait_callback(cf_timer); // Throws
    }

    void wait_callback(CFRunLoopTimerRef cf_timer)
    {
        REALM_ASSERT(cf_timer == m_cf_timer.get());
        complete_wait(); // Throws
        m_event_loop.process_completed_operations(); // Throws
    }

    void complete_wait(std::error_code ec = std::error_code()) noexcept
    {
        m_wait_oper->complete(ec);
        bind_ptr<WaitOper> oper = m_wait_oper;
        m_event_loop.add_completed_operation(std::move(oper));
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
    CFPtr<CFRunLoopSourceRef> source = adoptCF(CFRunLoopSourceCreate(kCFAllocatorDefault, order, &context));
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
    bind_ptr<PostOper> oper(new PostOper); // Throws
    oper->initiate(std::move(handler));
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
    REALM_ASSERT(!m_cf_run_loop);
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
    REALM_ASSERT(m_cf_run_loop);
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

#endif // HAVE_APPLE_CF_IMPLEMENTATION


namespace realm {
namespace _impl {

realm::util::EventLoop::Implementation* get_apple_cf_event_loop_impl()
{
#if HAVE_APPLE_CF_IMPLEMENTATION
    return &g_implementation;
#else
    return nullptr;
#endif
}

} // namespace _impl
} // namespace realm
