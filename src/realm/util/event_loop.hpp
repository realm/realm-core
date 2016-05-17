/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#ifndef REALM_UTIL_EVENT_LOOP_HPP
#define REALM_UTIL_EVENT_LOOP_HPP

#include <realm/util/network.hpp>
#include <realm/util/logger.hpp>

namespace realm {
namespace util {

class Socket;
class DeadlineTimer;

enum class SocketSecurity {
    None,    /// No socket security (cleartext).
    TLSv1,   /// Transport Layer Security v1 (encrypted).
};

/// Event Loops are an abstraction over asynchronous I/O.
///
/// The interface described by EventLoop is a "proactor pattern" approach to
/// asynchronous I/O. All operations are started with a completion handler, which is
/// invoked once the operation "completes", i.e. succeeds, fails, or is cancelled.
///
/// In general, completion handlers are always invoked, regardless of whether or not
/// the operation was successful.
///
/// Most operations return an abstract handle through a smart pointer, which can be
/// used to cancel the operation or reschedule a new operation. In general, if the handler
/// is destroyed and an operation is in progress, the operation is cancelled.
///
/// Operations on an event-loop are generally not thread-safe, with the exception of post()
/// and stop().
///
/// \sa Socket
/// \sa DeadlineTimer
class EventLoop {
public:
    virtual ~EventLoop() {}

    using Duration = std::chrono::milliseconds;

    using OnConnectComplete = std::function<void(std::error_code)>;
    using OnTimeout = std::function<void(std::error_code)>;
    using OnPost = std::function<void()>;

    /// Run the event loop until all callbacks have been triggered.
    virtual void run() = 0;

    /// Forcibly terminate the event loop. It may be restarted at a later time.
    /// This operation is thread-safe.
    virtual void stop() = 0;

    /// Establish a socket connection to \a host on port \a port.
    ///
    /// This establishes a buffered two-way socket connection. When the connection
    /// is established, or an error occurs, \a on_complete is invoked with an error
    /// code indicating success or failure.
    ///
    /// \a security indicates the desired security level for the connection. If the
    /// desired security level cannot be achieved, the completion handler will be
    /// invoked with an error code indicating the failure.
    ///
    /// This operation is NOT thread-safe.
    virtual std::unique_ptr<Socket>
    async_connect(std::string host, int port, SocketSecurity security, OnConnectComplete on_complete) = 0;

    /// Set a timer on the event loop.
    ///
    /// \a delay is a duration in milliseconds, after which \a on_timeout will be invoked.
    /// The delay may be zero, but the completion handler will still only be invoked
    /// from the event loop.
    ///
    /// This operation is NOT thread-safe.
    virtual std::unique_ptr<DeadlineTimer>
    async_timer(Duration delay, OnTimeout on_timeout) = 0;

    /// Invoke function on event loop.
    ///
    /// This is equivalent to setting a timer with a delay of zero, except that the
    /// caller is not required to hold a handle. Therefore, the callback also cannot
    /// be cancelled.
    ///
    /// This operation is thread-safe.
    ///
    /// \sa EventLoop::async_timer()
    virtual void post(OnPost callback) = 0;
};

/// Get a thread-local instance of an event loop that uses whatever implementation matches
/// the current platform best. On Apple platforms, the returned event loop will internally
/// represent an NSRunLoop.
EventLoop& get_native_event_loop();

/// Get an instance of an event loop that is implemented in terms of POSIX primitives.
/// Please note: Networking operations on event loops of this type will not work correctly
/// on iOS, because POSIX-level socket operations do no correctly activate the antennae.
std::unique_ptr<EventLoop> get_posix_event_loop();

/// Socket describes an event handler for socket operations.
///
/// It is also used to schedule individual I/O operations on a socket.
class Socket {
public:
    virtual ~Socket() {}

    using OnWriteComplete = std::function<void(std::error_code, size_t num_bytes_transferred)>;
    using OnReadComplete = std::function<void(std::error_code, size_t num_bytes_transferred)>;

    /// Close the associated socket. An error may be reported to any scheduled read/write
    /// operations on the socket.
    virtual void close() = 0;

    /// Cancel the socket, removing it from the event loop.
    virtual void cancel() = 0;

    /// Write \a size bytes to the socket, invoking \a on_complete when the operation is complete.
    virtual void async_write(const char* data, size_t size, OnWriteComplete on_complete) = 0;

    /// Read \a size bytes from the socket, invoking \a on_complete when the operation is complete.
    virtual void async_read(char* buffer, size_t size, OnReadComplete on_complete) = 0;

    /// Fill \a buffer with data from the socket, until \a delim is found. At most \a size
    /// bytes are read. If the buffer is filled without \a delim being found, \a on_complete
    /// is invoked with realm::error::delim_not_found.
    virtual void async_read_until(char* buffer, size_t size, char delim, OnReadComplete on_complete) = 0;
};

class DeadlineTimer {
public:
    virtual ~DeadlineTimer() {}

    using OnComplete = std::function<void(std::error_code)>;
    using Duration = EventLoop::Duration;

    /// Cancel the timer, invoking the completion handler with error::operation_aborted.
    virtual void cancel() = 0;

    /// Reschedule the timer with a new \a delay and \a callback.
    /// If the timer has already been scheduled, it must first be cancelled.
    virtual void async_wait(Duration delay, OnComplete callback) = 0;
};


#if REALM_DEBUG

struct SocketLogger: Socket {
    SocketLogger(std::unique_ptr<Socket> base, util::Logger& logger):
        m_base(std::move(base)), m_logger(logger)
    {}

    void close() final
    {
        m_logger.log("((Socket*)%1)->close()", m_base.get());
        m_base->close();
    }

    void cancel() final
    {
        m_logger.log("((Socket*)%1)->cancel()", m_base.get());
        m_base->cancel();
    }

    void async_write(const char* data, size_t size, OnWriteComplete on_complete) final
    {
        m_logger.log("((Socket*)%1)->async_write(\"...\", %2, ...)", m_base.get(), size);
        m_base->async_write(data, size, [=](std::error_code ec, size_t n) {
            m_logger.log("((Socket*)%1)->async_write->on_complete(%2, %3)", m_base.get(), ec, n);
            on_complete(ec, n);
        });
    }

    void async_read(char* buffer, size_t size, OnReadComplete on_complete) final
    {
        m_logger.log("((Socket*)%1)->async_read(%2, %3, ...)", m_base.get(), static_cast<void*>(buffer), size);
        m_base->async_read(buffer, size, [=](std::error_code ec, size_t n) {
            m_logger.log("((Socket*)%1)->async_read->on_complete(%2, %3)", m_base.get(), ec, n);
            on_complete(ec, n);
        });
    }

    void async_read_until(char* buffer, size_t size, char delim, OnReadComplete on_complete) final
    {
        m_logger.log("((Socket*)%1)->async_read_until(%2, %3, %4, ...)", m_base.get(), static_cast<void*>(buffer), size, static_cast<int>(delim));
        m_base->async_read_until(buffer, size, delim, [=](std::error_code ec, size_t n) {
            m_logger.log("((Socket*)%1)->async_read_until->on_complete(%2, %3)", m_base.get(), ec, n);
            on_complete(ec, n);
        });
    }
private:
    std::unique_ptr<Socket> m_base;
    util::Logger& m_logger;
};


struct DeadlineTimerLogger: DeadlineTimer {
    DeadlineTimerLogger(std::unique_ptr<DeadlineTimer> base, util::Logger& logger):
        m_base(std::move(base)), m_logger(logger)
    {}

    void cancel() final
    {
        m_logger.log("((DeadlineTimer*)%1)->cancel()", m_base.get());
        m_base->cancel();
    }

    void async_wait(Duration delay, OnComplete callback)
    {
        m_logger.log("((DeadlineTimer*)%1)->async_wait(%2, ...)", m_base.get(), delay.count());
        auto logger_callback = [=](std::error_code ec) {
            m_logger.log("((DeadlineTimer*)%1)->async_wait->on_complete(%2)", m_base.get(), ec);
            callback(ec);
        };
        m_base->async_wait(delay, logger_callback);
    }
private:
    std::unique_ptr<DeadlineTimer> m_base;
    util::Logger& m_logger;
};


class EventLoopLogger: public EventLoop {
public:
    EventLoopLogger(EventLoop& base, util::Logger& logger):
        m_base(base), m_logger(logger)
    {}

    void run() final
    {
        m_logger.log("((EventLoop*)%1)->run()", &m_base);
        m_base.run();
    }

    void stop() final
    {
        m_logger.log("((EventLoop*)%1)->stop()", &m_base);
        m_base.stop();
    }

    std::unique_ptr<Socket> async_connect(std::string host, int port, SocketSecurity sec,
                                              OnConnectComplete on_complete) final
    {
        m_logger.log("((EventLoop*)%1)->async_connect(\"%2\", %3, %4, ...)", &m_base, host, port, socket_security_as_string(sec));
        auto logging_on_complete = [=](std::error_code ec) {
            m_logger.log("((EventLoop*)%1)->async_connect->on_complete(%2)", &m_base, ec);
            on_complete(ec);
        };
        return std::unique_ptr<Socket>(
                new SocketLogger{m_base.async_connect(std::move(host), port, sec, logging_on_complete), m_logger});
    }

    std::unique_ptr<DeadlineTimer> async_timer(Duration delay, OnTimeout on_timeout) final
    {
        m_logger.log("((EventLoop*)%1)->async_timer(%2, ...)", &m_base, delay.count());
        auto logging_on_timeout = [=](std::error_code ec) {
            m_logger.log("((EventLoop*)%1)->async_timer->on_timeout(%2)", &m_base, ec);
            on_timeout(ec);
        };
        return std::unique_ptr<DeadlineTimer>(
                new DeadlineTimerLogger{m_base.async_timer(delay, logging_on_timeout), m_logger});
    }

    void post(OnPost on_post) final
    {
        m_logger.log("((EventLoop*)%1)->post()", &m_base);
        auto logging_on_post = [=]() {
            m_logger.log("((EventLoop*)%1)->post->on_post()", &m_base);
            on_post();
        };
        m_base.post(logging_on_post);
    }
private:
    EventLoop& m_base;
    util::Logger& m_logger;


    static const char* socket_security_as_string(SocketSecurity sec) {
        switch (sec) {
            case SocketSecurity::None:  return "SocketSecurity::None";
            case SocketSecurity::TLSv1: return "SocketSecurity::TLSv1";
        }
        REALM_UNREACHABLE();
    }
};

#endif // REALM_DEBUG

} // namespace util
} // namespace realm


#endif // REALM_UTIL_EVENT_LOOP_HPP

