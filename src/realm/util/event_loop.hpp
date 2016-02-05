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

namespace realm {
namespace util {

class SocketBase;
class DeadlineTimerBase;

enum class SocketSecurity {
    None,    /// No socket security (cleartext).
    TLSv1,   /// Transport Layer Security v1 (encrypted).
};

/// Event Loops are an abstraction over asynchronous I/O.
///
/// The interface described by EventLoopBase is a "proactor pattern" approach to
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
/// \sa SocketBase
/// \sa DeadlineTimerBase
class EventLoopBase {
public:
    virtual ~EventLoopBase() {}

    using Duration = std::chrono::milliseconds;

    using OnConnectComplete = std::function<void(std::error_code)>;
    using OnTimeout = std::function<void(std::error_code)>;
    using OnPost = std::function<void()>;

    /// Run the event loop until all callbacks have been triggered.
    virtual void run() = 0;

    /// Forcibly terminate the event loop. It may be restarted at a later time.
    virtual void stop() = 0;

    /// Reset the state of the event loop.
    ///
    /// In some implementations, this may do nothing.
    virtual void reset() = 0;

    /// Establish a socket connection to \a host on port \a port.
    ///
    /// This establishes a buffered two-way socket connection. When the connection
    /// is established, or an error occurs, \a on_complete is invoked with an error
    /// code indicating success or failure.
    ///
    /// \a security indicates the desired security level for the connection. If the
    /// desired security level cannot be achieved, the completion handler will be
    /// invoked with an error code indicating the failure.
    virtual std::unique_ptr<SocketBase>
    async_connect(std::string host, int port, SocketSecurity security, OnConnectComplete on_complete) = 0;

    /// Set a timer on the event loop.
    ///
    /// \a delay is a duration in milliseconds, after which \a on_timeout will be invoked.
    /// The delay may be zero, but the completion handler will still only be invoked
    /// from the event loop.
    virtual std::unique_ptr<DeadlineTimerBase>
    async_timer(Duration delay, OnTimeout on_timeout) = 0;

    /// Invoke function on event loop.
    ///
    /// This is equivalent to setting a timer with a delay of zero, except that the
    /// caller is not required to hold a handle. Therefore, the callback also cannot
    /// be cancelled.
    ///
    /// \sa EventLoopBase::async_timer()
    virtual void post(OnPost callback) = 0;
};

/// SocketBase describes an event handler for socket operations.
///
/// It is also used to schedule individual I/O operations on a socket.
class SocketBase {
public:
    virtual ~SocketBase() {}

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

class DeadlineTimerBase {
public:
    virtual ~DeadlineTimerBase() {}

    using OnComplete = std::function<void(std::error_code)>;
    using Duration = EventLoopBase::Duration;

    /// Cancel the timer, invoking the completion handler with error::operation_aborted.
    virtual void cancel() = 0;

    /// Reschedule the timer with a new \a delay and \a callback.
    /// If the timer has already been scheduled, it must first be cancelled.
    virtual void async_wait(Duration delay, OnComplete callback) = 0;
};

template<class EventLoopProvider> class EventLoop;

using ASIO = network::io_service;

template<>
class EventLoop<ASIO>: public EventLoopBase {
public:
    EventLoop();
    ~EventLoop();

    void run() override;
    void stop() override;
    void reset() override;

    std::unique_ptr<SocketBase> async_connect(std::string host, int port, SocketSecurity, OnConnectComplete) final;
    std::unique_ptr<DeadlineTimerBase> async_timer(Duration delay, OnTimeout) final;
    void post(OnPost) final;
protected:
    struct Resolver;
    struct Socket;
    struct DeadlineTimer;

    ASIO m_io_service;
};

#if REALM_PLATFORM_APPLE

class Apple {};
template<>
class EventLoop<Apple>: public EventLoopBase {
public:
    EventLoop();
    ~EventLoop();

    void run() override;
    void stop() override;
    void reset() override;

    std::unique_ptr<SocketBase> async_connect(std::string host, int port, SocketSecurity, OnConnectComplete) final;
    std::unique_ptr<DeadlineTimerBase> async_timer(Duration delay, OnTimeout) final;
    void post(OnPost) final;
protected:
    struct Impl;

    struct Socket;
    struct DeadlineTimer;

    std::unique_ptr<Impl> m_impl;
};

#endif // REALM_PLATFORM_APPLE

} // namespace util
} // namespace realm


#endif // REALM_UTIL_EVENT_LOOP_HPP

