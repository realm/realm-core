
#define _WINSOCK_DEPRECATED_NO_WARNINGS

#include <cerrno>
#include <limits>
#include <algorithm>
#include <vector>
#include <stdexcept>
#include <thread>

#include <fcntl.h>

#ifndef _WIN32
#include <netinet/tcp.h>
#include <unistd.h>
#include <poll.h>
#include <realm/util/to_string.hpp>
#endif

#include <realm/util/features.h>
#include <realm/util/optional.hpp>
#include <realm/util/misc_errors.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/priority_queue.hpp>
#include <realm/util/network.hpp>

#if defined _GNU_SOURCE && !REALM_ANDROID
#define HAVE_LINUX_PIPE2 1
#else
#define HAVE_LINUX_PIPE2 0
#endif

// Note: Linux specific accept4() is not available on Android.
#if defined _GNU_SOURCE && defined SOCK_NONBLOCK && defined SOCK_CLOEXEC && !REALM_ANDROID
#define HAVE_LINUX_ACCEPT4 1
#else
#define HAVE_LINUX_ACCEPT4 0
#endif

#if defined _GNU_SOURCE && defined SOCK_CLOEXEC
#define HAVE_LINUX_SOCK_CLOEXEC 1
#else
#define HAVE_LINUX_SOCK_CLOEXEC 0
#endif

#ifndef _WIN32

#if REALM_NETWORK_USE_EPOLL
#include <linux/version.h>
#include <sys/epoll.h>
#elif REALM_HAVE_KQUEUE
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#else
#include <poll.h>
#endif

#endif

// On Linux kernels earlier than 2.6.37, epoll can't handle timeout values
// bigger than (LONG_MAX - 999ULL)/HZ.  HZ in the wild can be as big as 1000,
// and LONG_MAX can be as small as (2**31)-1, so the largest number of
// milliseconds we can be sure to support on those early kernels is 2147482.
#if REALM_NETWORK_USE_EPOLL
#if LINUX_VERSION_CODE < KERNEL_VERSION(2, 6, 37)
#define EPOLL_LARGE_TIMEOUT_BUG 1
#endif
#endif

using namespace realm::util;
using namespace realm::util::network;


namespace {

using native_handle_type = SocketBase::native_handle_type;

#ifdef _WIN32

// This Winsock initialization call is required prior to any other Winsock API call
// made by the process. It is OK if a process calls it multiple times.
struct ProcessInitialization {
    ProcessInitialization()
    {
        WSADATA wsaData;
        int i = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (i != 0) {
            throw std::system_error(i, std::system_category(), "WSAStartup() Winsock initialization failed");
        }
    }

    ~ProcessInitialization()
    {
        // Must be called 1 time for each call to WSAStartup() that has taken place
        WSACleanup();
    }
};

ProcessInitialization g_process_initialization;

std::error_code make_winsock_error_code(int error_code)
{
    switch (error_code) {
        case WSAEAFNOSUPPORT:
            return make_basic_system_error_code(EAFNOSUPPORT);
        case WSAEINVAL:
            return make_basic_system_error_code(EINVAL);
        case WSAECANCELLED:
            return make_basic_system_error_code(ECANCELED);
        case WSAECONNABORTED:
            return make_basic_system_error_code(ECONNABORTED);
        case WSAECONNRESET:
            return make_basic_system_error_code(ECONNRESET);
        case WSAEWOULDBLOCK:
            return make_basic_system_error_code(EAGAIN);
    }

    // Microsoft's STL can map win32 (and winsock!) error codes to known (posix-compatible) errc ones.
    auto ec = std::system_category().default_error_condition(error_code);
    if (ec.category() == std::generic_category())
        return make_basic_system_error_code(ec.value());
    return std::error_code(ec.value(), ec.category());
}

#endif // defined _WIN32

inline bool check_socket_error(int ret, std::error_code& ec)
{
#ifdef _WIN32
    if (REALM_UNLIKELY(ret == SOCKET_ERROR)) {
        ec = make_winsock_error_code(WSAGetLastError());
        return true;
    }
#else
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return true;
    }
#endif
    return false;
}

// Set file status flag O_NONBLOCK if `value` is true, otherwise clear it.
//
// Note that these flags are set at the file description level, and are therfore
// shared between duplicated descriptors (dup()).
//
// `ec` untouched on success.
std::error_code set_nonblock_flag(native_handle_type fd, bool value, std::error_code& ec) noexcept
{
#ifdef _WIN32
    u_long flags = value ? 1 : 0;
    int r = ioctlsocket(fd, FIONBIO, &flags);
    if (r == SOCKET_ERROR) {
        ec = make_winsock_error_code(WSAGetLastError());
        return ec;
    }
#else
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (REALM_UNLIKELY(flags == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    flags &= ~O_NONBLOCK;
    flags |= (value ? O_NONBLOCK : 0);
    int ret = ::fcntl(fd, F_SETFL, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
#endif

    return std::error_code(); // Success
}

// Set file status flag O_NONBLOCK. See set_nonblock_flag(int, bool,
// std::error_code&) for details. Throws std::system_error on failure.
void set_nonblock_flag(native_handle_type fd, bool value = true)
{
    std::error_code ec;
    if (set_nonblock_flag(fd, value, ec))
        throw std::system_error(ec);
}

// Set file descriptor flag FD_CLOEXEC if `value` is true, otherwise clear it.
//
// Note that this method of setting FD_CLOEXEC is subject to a race condition if
// another thread calls any of the exec functions concurrently. For that reason,
// this function should only be used when there is no better alternative. For
// example, Linux generally offers ways to set this flag atomically with the
// creation of a new file descriptor.
//
// `ec` untouched on success.
std::error_code set_cloexec_flag(native_handle_type fd, bool value, std::error_code& ec) noexcept
{
#ifndef _WIN32
    int flags = ::fcntl(fd, F_GETFD, 0);
    if (REALM_UNLIKELY(flags == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    flags &= ~FD_CLOEXEC;
    flags |= (value ? FD_CLOEXEC : 0);
    int ret = ::fcntl(fd, F_SETFD, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
#endif
    return std::error_code(); // Success
}

// Set file descriptor flag FD_CLOEXEC. See set_cloexec_flag(int, bool,
// std::error_code&) for details. Throws std::system_error on failure.
REALM_UNUSED inline void set_cloexec_flag(native_handle_type fd, bool value = true)
{
    std::error_code ec;
    if (set_cloexec_flag(fd, value, ec))
        throw std::system_error(ec);
}


inline void checked_close(native_handle_type fd) noexcept
{
#ifdef _WIN32
    int status = closesocket(fd);
    if (status == -1) {
        BOOL b = CloseHandle((HANDLE)fd);
        REALM_ASSERT(b || GetLastError() != ERROR_INVALID_HANDLE);
    }
#else
    int ret = ::close(fd);
    // We can accept various errors from close(), but they must be ignored as
    // the file descriptor is closed in any case (not necessarily according to
    // POSIX, but we shall assume it anyway). `EBADF`, however, would indicate
    // an implementation bug, so we don't want to ignore that.
    REALM_ASSERT(ret != -1 || errno != EBADF);
#endif
}


class CloseGuard {
public:
    CloseGuard() noexcept {}
    explicit CloseGuard(native_handle_type fd) noexcept
        : m_fd{fd}
    {
        REALM_ASSERT(fd != -1);
    }
    CloseGuard(CloseGuard&& cg) noexcept
        : m_fd{cg.release()}
    {
    }
    ~CloseGuard() noexcept
    {
        if (m_fd != -1)
            checked_close(m_fd);
    }
    void reset(native_handle_type fd) noexcept
    {
        REALM_ASSERT(fd != -1);
        if (m_fd != -1)
            checked_close(m_fd);
        m_fd = fd;
    }
    operator native_handle_type() const noexcept
    {
        return m_fd;
    }
    native_handle_type release() noexcept
    {
        native_handle_type fd = m_fd;
        m_fd = -1;
        return fd;
    }

private:
    native_handle_type m_fd = -1;
};


#ifndef _WIN32

class WakeupPipe {
public:
    WakeupPipe()
    {
        int fildes[2];
#if HAVE_LINUX_PIPE2
        int flags = O_CLOEXEC;
        int ret = ::pipe2(fildes, flags);
#else
        int ret = ::pipe(fildes);
#endif
        if (REALM_UNLIKELY(ret == -1)) {
            std::error_code ec = make_basic_system_error_code(errno);
            throw std::system_error(ec);
        }
        m_read_fd.reset(fildes[0]);
        m_write_fd.reset(fildes[1]);
#if !HAVE_LINUX_PIPE2
        set_cloexec_flag(m_read_fd);  // Throws
        set_cloexec_flag(m_write_fd); // Throws
#endif
    }

    // Thread-safe.
    int wait_fd() const noexcept
    {
        return m_read_fd;
    }

    // Cause the wait descriptor (wait_fd()) to become readable within a short
    // amount of time.
    //
    // Thread-safe.
    void signal() noexcept
    {
        LockGuard lock{m_mutex};
        if (!m_signaled) {
            char c = 0;
            ssize_t ret = ::write(m_write_fd, &c, 1);
            REALM_ASSERT_RELEASE(ret == 1);
            m_signaled = true;
        }
    }

    // Must be called after the wait descriptor (wait_fd()) becomes readable.
    //
    // Thread-safe.
    void acknowledge_signal() noexcept
    {
        LockGuard lock{m_mutex};
        if (m_signaled) {
            char c;
            ssize_t ret = ::read(m_read_fd, &c, 1);
            REALM_ASSERT_RELEASE(ret == 1);
            m_signaled = false;
        }
    }

private:
    CloseGuard m_read_fd, m_write_fd;
    Mutex m_mutex;
    bool m_signaled = false; // Protected by `m_mutex`.
};

#else // defined _WIN32

class WakeupPipe {
public:
    SOCKET wait_fd() const noexcept
    {
        return INVALID_SOCKET;
    }

    void signal() noexcept
    {
        m_signal_count++;
    }

    bool is_signaled() const noexcept
    {
        return m_signal_count > 0;
    }

    void acknowledge_signal() noexcept
    {
        m_signal_count--;
    }

private:
    std::atomic<uint32_t> m_signal_count = 0;
};

#endif // defined _WIN32


std::error_code translate_addrinfo_error(int err) noexcept
{
    switch (err) {
        case EAI_AGAIN:
            return ResolveErrors::host_not_found_try_again;
        case EAI_BADFLAGS:
            return error::invalid_argument;
        case EAI_FAIL:
            return ResolveErrors::no_recovery;
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
            return ResolveErrors::host_not_found;
        case EAI_SERVICE:
            return ResolveErrors::service_not_found;
        case EAI_SOCKTYPE:
            return ResolveErrors::socket_type_not_supported;
        default:
            return error::unknown;
    }
}


struct GetaddrinfoResultOwner {
    struct addrinfo* ptr;
    GetaddrinfoResultOwner(struct addrinfo* p)
        : ptr{p}
    {
    }
    ~GetaddrinfoResultOwner() noexcept
    {
        if (ptr)
            freeaddrinfo(ptr);
    }
};

} // unnamed namespace


class Service::IoReactor {
public:
    IoReactor();
    ~IoReactor() noexcept;

    // Add an initiated I/O operation that did not complete immediately.
    void add_oper(Descriptor&, LendersIoOperPtr, Want);
    void remove_canceled_ops(Descriptor&, OperQueue<AsyncOper>& completed_ops) noexcept;

    bool wait_and_advance(clock::time_point timeout, clock::time_point now, bool& interrupted,
                          OperQueue<AsyncOper>& completed_ops);

    // The reactor is considered empty when no operations are currently managed
    // by it. An operation is managed by a reactor if it was added through
    // add_oper() and not yet passed out through `completed_ops` of
    // wait_and_advance().
    bool empty() const noexcept;

    // Cause wait_and_advance() to return within a short amount of time.
    //
    // Thread-safe.
    void interrupt() noexcept;

#if REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE
    void register_desc(Descriptor&);
    void deregister_desc(Descriptor&) noexcept;
#endif

#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
    clock::duration get_and_reset_sleep_time() noexcept;
#endif

private:
#if REALM_NETWORK_USE_EPOLL

    static constexpr int s_epoll_event_buffer_size = 256;
    const std::unique_ptr<epoll_event[]> m_epoll_event_buffer;
    const CloseGuard m_epoll_fd;

    static std::unique_ptr<epoll_event[]> make_epoll_event_buffer();
    static CloseGuard make_epoll_fd();

#elif REALM_HAVE_KQUEUE // !REALM_NETWORK_USE_EPOLL && REALM_HAVE_KQUEUE

    static constexpr int s_kevent_buffer_size = 256;
    const std::unique_ptr<struct kevent[]> m_kevent_buffer;
    const CloseGuard m_kqueue_fd;

    static std::unique_ptr<struct kevent[]> make_kevent_buffer();
    static CloseGuard make_kqueue_fd();

#endif // !REALM_NETWORK_USE_EPOLL && REALM_HAVE_KQUEUE

#if REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE

    OperQueue<IoOper> m_active_ops;

    // If there are already active operations, just activate as many additional
    // operations as can be done without blocking. Otherwise, block until at
    // least one operation can be activated or the timeout is reached. Then, if
    // the timeout was not reached, activate as many additional operations as
    // can be done without any further blocking.
    //
    // May occasionally return with no active operations and before the timeout
    // has been reached, but this can be assumed to happen rarely enough that it
    // will never amount to a performance problem.
    //
    // Argument `now` is unused if `timeout.time_since_epoch() <= 0`.
    //
    // Returns true if, and only if a wakeup pipe signal was
    // received. Operations may already have been activated in this case.
    bool wait_and_activate(clock::time_point timeout, clock::time_point now);

    void advance_active_ops(OperQueue<AsyncOper>& completed_ops) noexcept;

#else // !(REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE)

    struct OperSlot {
        std::size_t pollfd_slot_ndx = 0; // Zero when slot is unused
        OperQueue<IoOper> read_ops, write_ops;
    };

    std::vector<OperSlot> m_operations; // Indexed by file descriptor

    // First entry in `m_pollfd_slots` is always the read end of the wakeup
    // pipe. There is then an additional entry for each entry in `m_operations`
    // where `pollfd_slot_ndx` is nonzero. All entries always have `pollfd::fd`
    // >= 0.
    //
    // INVARIANT: m_pollfd_slots.size() == 1 + N, where N is the number of
    // entries in m_operations where pollfd_slot_ndx is nonzero.
    std::vector<pollfd> m_pollfd_slots;

    void discard_pollfd_slot_by_move_last_over(OperSlot&) noexcept;

#endif // !(REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE)

    std::size_t m_num_operations = 0;
    WakeupPipe m_wakeup_pipe;

#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
    clock::duration m_sleep_time = clock::duration::zero();
#endif
};


inline bool Service::IoReactor::empty() const noexcept
{
    return (m_num_operations == 0);
}


inline void Service::IoReactor::interrupt() noexcept
{
    m_wakeup_pipe.signal();
}


#if REALM_NETWORK_USE_EPOLL

inline Service::IoReactor::IoReactor()
    : m_epoll_event_buffer{make_epoll_event_buffer()} // Throws
    , m_epoll_fd{make_epoll_fd()}                     // Throws
    , m_wakeup_pipe{}                                 // Throws
{
    epoll_event event = epoll_event(); // Clear
    event.events = EPOLLIN;
    event.data.ptr = nullptr;
    int ret = epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, m_wakeup_pipe.wait_fd(), &event);
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
}


inline Service::IoReactor::~IoReactor() noexcept {}


inline void Service::IoReactor::register_desc(Descriptor& desc)
{
    epoll_event event = epoll_event();                        // Clear
    event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET; // Enable edge triggering
    event.data.ptr = &desc;
    int ret = epoll_ctl(m_epoll_fd, EPOLL_CTL_ADD, desc.m_fd, &event);
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
}


inline void Service::IoReactor::deregister_desc(Descriptor& desc) noexcept
{
    epoll_event event = epoll_event(); // Clear
    int ret = epoll_ctl(m_epoll_fd, EPOLL_CTL_DEL, desc.m_fd, &event);
    REALM_ASSERT(ret != -1);
}


inline std::unique_ptr<epoll_event[]> Service::IoReactor::make_epoll_event_buffer()
{
    return std::make_unique<epoll_event[]>(s_epoll_event_buffer_size); // Throws
}


inline CloseGuard Service::IoReactor::make_epoll_fd()
{
    int flags = 0;
    flags |= EPOLL_CLOEXEC;
    int ret = epoll_create1(flags);
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
    int epoll_fd = ret;
    return CloseGuard{epoll_fd};
}


bool Service::IoReactor::wait_and_activate(clock::time_point timeout, clock::time_point now)
{
    int max_wait_millis = 0;
    bool allow_blocking_wait = m_active_ops.empty();
    if (allow_blocking_wait) {
        if (timeout.time_since_epoch().count() <= 0) {
            max_wait_millis = -1; // Allow indefinite blocking
        }
        else if (now < timeout) {
            auto diff = timeout - now;
            int max_int_millis = std::numeric_limits<int>::max();
            // 17592186044415 is the largest value (45-bit signed integer)
            // garanteed to be supported by std::chrono::milliseconds. In the
            // worst case, `int` is a 16-bit integer, meaning that we can only
            // wait about 30 seconds at a time. In the best case
            // (17592186044415) we can wait more than 500 years at a time. In
            // the typical case (`int` has 32 bits), we can wait 24 days at a
            // time.
            long long max_chrono_millis = 17592186044415;
            if (max_chrono_millis < max_int_millis)
                max_int_millis = int(max_chrono_millis);
#if EPOLL_LARGE_TIMEOUT_BUG
            long max_safe_millis = 2147482; // Circa 35 minutes
            if (max_safe_millis < max_int_millis)
                max_int_millis = int(max_safe_millis);
#endif
            if (diff > std::chrono::milliseconds(max_int_millis)) {
                max_wait_millis = max_int_millis;
            }
            else {
                // Overflow is impossible here, due to the preceding check
                auto diff_millis = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                // The conversion to milliseconds will round down if the tick
                // period of `diff` is less than a millisecond, which it usually
                // is. This is a problem, because it can lead to premature
                // wakeups, which in turn could cause extranous iterations in
                // the event loop. This is especially problematic when a small
                // `diff` is rounded down to zero milliseconds, becuase that can
                // easily produce a "busy wait" condition for up to a
                // millisecond every time this happens. Obviously, the solution
                // is to round up, instead of down.
                if (diff_millis < diff) {
                    // Note that the following increment cannot overflow,
                    // because diff_millis < diff <= max_int_millis <=
                    // std::numeric_limits<int>::max().
                    ++diff_millis;
                }
                max_wait_millis = int(diff_millis.count());
            }
        }
    }
    for (int i = 0; i < 2; ++i) {
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        clock::time_point sleep_start_time = clock::now();
#endif
        int ret = epoll_wait(m_epoll_fd, m_epoll_event_buffer.get(), s_epoll_event_buffer_size, max_wait_millis);
        if (REALM_UNLIKELY(ret == -1)) {
            int err = errno;
            if (err == EINTR)
                return false; // Infrequent premature return is ok
            std::error_code ec = make_basic_system_error_code(err);
            throw std::system_error(ec);
        }
        REALM_ASSERT(ret >= 0);
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        m_sleep_time += clock::now() - sleep_start_time;
#endif
        int n = ret;
        bool got_wakeup_pipe_signal = false;
        for (int j = 0; j < n; ++j) {
            const epoll_event& event = m_epoll_event_buffer[j];
            bool is_wakeup_pipe_signal = !event.data.ptr;
            if (REALM_UNLIKELY(is_wakeup_pipe_signal)) {
                m_wakeup_pipe.acknowledge_signal();
                got_wakeup_pipe_signal = true;
                continue;
            }
            Descriptor& desc = *static_cast<Descriptor*>(event.data.ptr);
            if ((event.events & (EPOLLIN | EPOLLHUP | EPOLLERR)) != 0) {
                if (!desc.m_read_ready) {
                    desc.m_read_ready = true;
                    m_active_ops.push_back(desc.m_suspended_read_ops);
                }
            }
            if ((event.events & (EPOLLOUT | EPOLLHUP | EPOLLERR)) != 0) {
                if (!desc.m_write_ready) {
                    desc.m_write_ready = true;
                    m_active_ops.push_back(desc.m_suspended_write_ops);
                }
            }
            if ((event.events & EPOLLRDHUP) != 0)
                desc.m_imminent_end_of_input = true;
        }
        if (got_wakeup_pipe_signal)
            return true;
        if (n < s_epoll_event_buffer_size)
            break;
        max_wait_millis = 0;
    }
    return false;
}


#elif REALM_HAVE_KQUEUE // !REALM_NETWORK_USE_EPOLL && REALM_HAVE_KQUEUE


inline Service::IoReactor::IoReactor()
    : m_kevent_buffer{make_kevent_buffer()} // Throws
    , m_kqueue_fd{make_kqueue_fd()}         // Throws
    , m_wakeup_pipe{}                       // Throws
{
    struct kevent event;
    EV_SET(&event, m_wakeup_pipe.wait_fd(), EVFILT_READ, EV_ADD, 0, 0, nullptr);
    int ret = ::kevent(m_kqueue_fd, &event, 1, nullptr, 0, nullptr);
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
}


inline Service::IoReactor::~IoReactor() noexcept {}


inline void Service::IoReactor::register_desc(Descriptor& desc)
{
    struct kevent events[2];
    // EV_CLEAR enables edge-triggered behavior
    EV_SET(&events[0], desc.m_fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, &desc);
    EV_SET(&events[1], desc.m_fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, &desc);
    int ret = ::kevent(m_kqueue_fd, events, 2, nullptr, 0, nullptr);
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
}


inline void Service::IoReactor::deregister_desc(Descriptor& desc) noexcept
{
    struct kevent events[2];
    EV_SET(&events[0], desc.m_fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&events[1], desc.m_fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    int ret = ::kevent(m_kqueue_fd, events, 2, nullptr, 0, nullptr);
    REALM_ASSERT(ret != -1);
}


inline std::unique_ptr<struct kevent[]> Service::IoReactor::make_kevent_buffer()
{
    return std::make_unique<struct kevent[]>(s_kevent_buffer_size); // Throws
}


inline CloseGuard Service::IoReactor::make_kqueue_fd()
{
    int ret = ::kqueue();
    if (REALM_UNLIKELY(ret == -1)) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
    int epoll_fd = ret;
    return CloseGuard{epoll_fd};
}


bool Service::IoReactor::wait_and_activate(clock::time_point timeout, clock::time_point now)
{
    timespec max_wait_time{}; // Clear to zero
    bool allow_blocking_wait = m_active_ops.empty();
    if (allow_blocking_wait) {
        // Note that ::kevent() will silently clamp `max_wait_time` to 24 hours
        // (86400 seconds), but that is ok, because the caller is prepared for
        // premature return as long as it happens infrequently enough to not
        // pose a performance problem.
        constexpr std::time_t max_wait_seconds = 86400;
        if (timeout.time_since_epoch().count() <= 0) {
            max_wait_time.tv_sec = max_wait_seconds;
        }
        else if (now < timeout) {
            auto diff = timeout - now;
            auto secs = std::chrono::duration_cast<std::chrono::seconds>(diff);
            auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(diff - secs);
            auto secs_2 = std::min(secs.count(), std::chrono::seconds::rep(max_wait_seconds));
            max_wait_time.tv_sec = std::time_t(secs_2);
            max_wait_time.tv_nsec = long(nsecs.count());
        }
    }
    for (int i = 0; i < 4; ++i) {
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        clock::time_point sleep_start_time = clock::now();
#endif
        int ret = ::kevent(m_kqueue_fd, nullptr, 0, m_kevent_buffer.get(), s_kevent_buffer_size, &max_wait_time);
        if (REALM_UNLIKELY(ret == -1)) {
            int err = errno;
            if (err == EINTR)
                return false; // Infrequent premature return is ok
            std::error_code ec = make_basic_system_error_code(err);
            throw std::system_error(ec);
        }
        REALM_ASSERT(ret >= 0);
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        m_sleep_time += clock::now() - sleep_start_time;
#endif
        int n = ret;
        bool got_wakeup_pipe_signal = false;
        for (int j = 0; j < n; ++j) {
            const struct kevent& event = m_kevent_buffer[j];
            bool is_wakeup_pipe_signal = !event.udata;
            if (REALM_UNLIKELY(is_wakeup_pipe_signal)) {
                REALM_ASSERT(m_wakeup_pipe.wait_fd() == int(event.ident));
                m_wakeup_pipe.acknowledge_signal();
                got_wakeup_pipe_signal = true;
                continue;
            }
            Descriptor& desc = *static_cast<Descriptor*>(event.udata);
            REALM_ASSERT(desc.m_fd == int(event.ident));
            if (event.filter == EVFILT_READ) {
                if (!desc.m_read_ready) {
                    desc.m_read_ready = true;
                    m_active_ops.push_back(desc.m_suspended_read_ops);
                }
                if ((event.flags & EV_EOF) != 0)
                    desc.m_imminent_end_of_input = true;
            }
            if (event.filter == EVFILT_WRITE) {
                if (!desc.m_write_ready) {
                    desc.m_write_ready = true;
                    m_active_ops.push_back(desc.m_suspended_write_ops);
                }
            }
        }
        if (got_wakeup_pipe_signal)
            return true;
        if (n < s_kevent_buffer_size)
            break;
        // Clear to zero to disable blocking for any additional opportunistic
        // event extractions.
        max_wait_time = timespec{};
    }
    return false;
}

#endif // !REALM_NETWORK_USE_EPOLL && REALM_HAVE_KQUEUE


#if REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE

void Service::IoReactor::add_oper(Descriptor& desc, LendersIoOperPtr op, Want want)
{
    if (REALM_UNLIKELY(!desc.m_is_registered)) {
        register_desc(desc); // Throws
        desc.m_is_registered = true;
    }

    switch (want) {
        case Want::read:
            if (REALM_UNLIKELY(desc.m_read_ready))
                goto active;
            desc.m_suspended_read_ops.push_back(std::move(op));
            goto proceed;
        case Want::write:
            if (REALM_UNLIKELY(desc.m_write_ready))
                goto active;
            desc.m_suspended_write_ops.push_back(std::move(op));
            goto proceed;
        case Want::nothing:
            break;
    }
    REALM_ASSERT(false);

active:
    m_active_ops.push_back(std::move(op));

proceed:
    ++m_num_operations;
}


void Service::IoReactor::remove_canceled_ops(Descriptor& desc, OperQueue<AsyncOper>& completed_ops) noexcept
{
    // Note: Canceled operations that are currently active (in m_active_ops)
    // will be removed later by advance_active_ops().

    while (LendersIoOperPtr op = desc.m_suspended_read_ops.pop_front()) {
        completed_ops.push_back(std::move(op));
        --m_num_operations;
    }
    while (LendersIoOperPtr op = desc.m_suspended_write_ops.pop_front()) {
        completed_ops.push_back(std::move(op));
        --m_num_operations;
    }
}


bool Service::IoReactor::wait_and_advance(clock::time_point timeout, clock::time_point now, bool& interrupted,
                                          OperQueue<AsyncOper>& completed_ops)
{
    clock::time_point now_2 = now;
    for (;;) {
        bool wakeup_pipe_signal = wait_and_activate(timeout, now_2); // Throws
        if (REALM_UNLIKELY(wakeup_pipe_signal)) {
            interrupted = true;
            return false;
        }
        advance_active_ops(completed_ops);
        if (!completed_ops.empty())
            return true;
        if (timeout.time_since_epoch().count() > 0) {
            now_2 = clock::now();
            bool timed_out = (now_2 >= timeout);
            if (timed_out)
                return false;
        }
    }
}


void Service::IoReactor::advance_active_ops(OperQueue<AsyncOper>& completed_ops) noexcept
{
    OperQueue<IoOper> new_active_ops;
    while (LendersIoOperPtr op = m_active_ops.pop_front()) {
        if (op->is_canceled()) {
            completed_ops.push_back(std::move(op));
            --m_num_operations;
            continue;
        }
        Want want = op->advance();
        switch (want) {
            case Want::nothing:
                REALM_ASSERT(op->is_complete());
                completed_ops.push_back(std::move(op));
                --m_num_operations;
                continue;
            case Want::read: {
                Descriptor& desc = op->descriptor();
                if (REALM_UNLIKELY(desc.m_read_ready))
                    goto still_active;
                desc.m_suspended_read_ops.push_back(std::move(op));
                continue;
            }
            case Want::write: {
                Descriptor& desc = op->descriptor();
                if (REALM_UNLIKELY(desc.m_write_ready))
                    goto still_active;
                desc.m_suspended_write_ops.push_back(std::move(op));
                continue;
            }
        }
        REALM_ASSERT(false);

    still_active:
        new_active_ops.push_back(std::move(op));
    }
    m_active_ops.push_back(new_active_ops);
}


#else // !(REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE)


inline Service::IoReactor::IoReactor()
    : m_wakeup_pipe{} // Throws
{
    pollfd slot = pollfd(); // Cleared slot
    slot.fd = m_wakeup_pipe.wait_fd();
    slot.events = POLLRDNORM;
    m_pollfd_slots.emplace_back(slot); // Throws
}


inline Service::IoReactor::~IoReactor() noexcept
{
#if REALM_ASSERTIONS_ENABLED
    std::size_t n = 0;
    for (std::size_t i = 0; i < m_operations.size(); ++i) {
        OperSlot& oper_slot = m_operations[i];
        while (oper_slot.read_ops.pop_front())
            ++n;
        while (oper_slot.write_ops.pop_front())
            ++n;
    }
    REALM_ASSERT(n == m_num_operations);
#endif
}


void Service::IoReactor::add_oper(Descriptor& desc, LendersIoOperPtr op, Want want)
{
    native_handle_type fd = desc.m_fd;

    // Make sure there are enough slots in m_operations
    {
        std::size_t n = std::size_t(fd) + 1; // FIXME: Check for arithmetic overflow
        if (m_operations.size() < n)
            m_operations.resize(n); // Throws
    }

    // Allocate a pollfd_slot unless we already have one
    OperSlot& oper_slot = m_operations[fd];
    if (oper_slot.pollfd_slot_ndx == 0) {
        pollfd pollfd_slot = pollfd(); // Cleared slot
        pollfd_slot.fd = fd;
        std::size_t pollfd_slot_ndx = m_pollfd_slots.size();
        REALM_ASSERT(pollfd_slot_ndx > 0);
        m_pollfd_slots.emplace_back(pollfd_slot); // Throws
        oper_slot.pollfd_slot_ndx = pollfd_slot_ndx;
    }

    pollfd& pollfd_slot = m_pollfd_slots[oper_slot.pollfd_slot_ndx];
    REALM_ASSERT(pollfd_slot.fd == fd);
    REALM_ASSERT(((pollfd_slot.events & POLLRDNORM) != 0) == !oper_slot.read_ops.empty());
    REALM_ASSERT(((pollfd_slot.events & POLLWRNORM) != 0) == !oper_slot.write_ops.empty());
    REALM_ASSERT((pollfd_slot.events & ~(POLLRDNORM | POLLWRNORM)) == 0);
    switch (want) {
        case Want::nothing:
            break;
        case Want::read:
            pollfd_slot.events |= POLLRDNORM;
            oper_slot.read_ops.push_back(std::move(op));
            goto finish;
        case Want::write:
            pollfd_slot.events |= POLLWRNORM;
            oper_slot.write_ops.push_back(std::move(op));
            goto finish;
    }
    REALM_ASSERT(false);
    return;

finish:
    ++m_num_operations;
}


void Service::IoReactor::remove_canceled_ops(Descriptor& desc, OperQueue<AsyncOper>& completed_ops) noexcept
{
    native_handle_type fd = desc.m_fd;
    REALM_ASSERT(fd >= 0);
    REALM_ASSERT(std::size_t(fd) < m_operations.size());
    OperSlot& oper_slot = m_operations[fd];
    REALM_ASSERT(oper_slot.pollfd_slot_ndx > 0);
    REALM_ASSERT(!oper_slot.read_ops.empty() || !oper_slot.write_ops.empty());
    pollfd& pollfd_slot = m_pollfd_slots[oper_slot.pollfd_slot_ndx];
    REALM_ASSERT(pollfd_slot.fd == fd);
    while (LendersIoOperPtr op = oper_slot.read_ops.pop_front()) {
        completed_ops.push_back(std::move(op));
        --m_num_operations;
    }
    while (LendersIoOperPtr op = oper_slot.write_ops.pop_front()) {
        completed_ops.push_back(std::move(op));
        --m_num_operations;
    }
    discard_pollfd_slot_by_move_last_over(oper_slot);
}


bool Service::IoReactor::wait_and_advance(clock::time_point timeout, clock::time_point now, bool& interrupted,
                                          OperQueue<AsyncOper>& completed_ops)
{
#ifdef _WIN32
    using nfds_type = std::size_t;
#else
    using nfds_type = nfds_t;
#endif
    clock::time_point now_2 = now;
    std::size_t num_ready_descriptors = 0;
    {
        // std::vector guarantees contiguous storage
        pollfd* fds = &m_pollfd_slots.front();
        nfds_type nfds = nfds_type(m_pollfd_slots.size());
        for (;;) {
            int max_wait_millis = -1; // Wait indefinitely
            if (timeout.time_since_epoch().count() > 0) {
                if (now_2 >= timeout)
                    return false; // No operations completed
                auto diff = timeout - now_2;
                int max_int_millis = std::numeric_limits<int>::max();
                // 17592186044415 is the largest value (45-bit signed integer)
                // garanteed to be supported by std::chrono::milliseconds. In
                // the worst case, `int` is a 16-bit integer, meaning that we
                // can only wait about 30 seconds at a time. In the best case
                // (17592186044415) we can wait more than 500 years at a
                // time. In the typical case (`int` has 32 bits), we can wait 24
                // days at a time.
                long long max_chrono_millis = 17592186044415;
                if (max_int_millis > max_chrono_millis)
                    max_int_millis = int(max_chrono_millis);
                if (diff > std::chrono::milliseconds(max_int_millis)) {
                    max_wait_millis = max_int_millis;
                }
                else {
                    // Overflow is impossible here, due to the preceeding check
                    auto diff_millis = std::chrono::duration_cast<std::chrono::milliseconds>(diff);
                    // The conversion to milliseconds will round down if the
                    // tick period of `diff` is less than a millisecond, which
                    // it usually is. This is a problem, because it can lead to
                    // premature wakeups, which in turn could cause extranous
                    // iterations in the event loop. This is especially
                    // problematic when a small `diff` is rounded down to zero
                    // milliseconds, becuase that can easily produce a "busy
                    // wait" condition for up to a millisecond every time this
                    // happens. Obviously, the solution is to round up, instead
                    // of down.
                    if (diff_millis < diff) {
                        // Note that the following increment cannot overflow,
                        // because diff_millis < diff <= max_int_millis <=
                        // std::numeric_limits<int>::max().
                        ++diff_millis;
                    }
                    max_wait_millis = int(diff_millis.count());
                }
            }

#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
            clock::time_point sleep_start_time = clock::now();
#endif

#ifdef _WIN32
            max_wait_millis = 1000;

            // Windows does not have a single API call to wait for pipes and
            // sockets with a timeout. So we repeatedly poll them individually
            // in a loop until max_wait_millis has elapsed or an event happend.
            //
            // FIXME: Maybe switch to Windows IOCP instead.

            // Following variable is the poll time for the sockets in
            // miliseconds. Adjust it to find a balance between CPU usage and
            // response time:
            constexpr INT socket_poll_timeout = 10;

            for (size_t t = 0; t < m_pollfd_slots.size(); t++)
                m_pollfd_slots[t].revents = 0;

            using namespace std::chrono;
            auto started = steady_clock::now();
            int ret = 0;

            do {
                if (m_pollfd_slots.size() > 1) {
                    // Poll all network sockets
                    ret = WSAPoll(LPWSAPOLLFD(&m_pollfd_slots[1]), ULONG(m_pollfd_slots.size() - 1),
                                  socket_poll_timeout);
                    REALM_ASSERT(ret != SOCKET_ERROR);
                }

                if (m_wakeup_pipe.is_signaled()) {
                    m_pollfd_slots[0].revents = POLLIN;
                    ret++;
                }

            } while (ret == 0 &&
                     (duration_cast<milliseconds>(steady_clock::now() - started).count() < max_wait_millis));

#else // !defined _WIN32
            int ret = ::poll(fds, nfds, max_wait_millis);
#endif
            bool interrupted_2 = false;
            if (REALM_UNLIKELY(ret == -1)) {
#ifndef _WIN32
                int err = errno;
                if (REALM_UNLIKELY(err != EINTR)) {
                    std::error_code ec = make_basic_system_error_code(err);
                    throw std::system_error(ec);
                }
#endif
                interrupted_2 = true;
            }

#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
            m_sleep_time += clock::now() - sleep_start_time;
#endif

            if (REALM_LIKELY(!interrupted_2)) {
                REALM_ASSERT(ret >= 0);
                num_ready_descriptors = ret;
                break;
            }

            // Retry on interruption by system signal
            if (timeout.time_since_epoch().count() > 0)
                now_2 = clock::now();
        }
    }

    if (num_ready_descriptors == 0)
        return false; // No operations completed

    // Check wake-up descriptor
    if (m_pollfd_slots[0].revents != 0) {
        REALM_ASSERT((m_pollfd_slots[0].revents & POLLNVAL) == 0);
        m_wakeup_pipe.acknowledge_signal();
        interrupted = true;
        return false;
    }

    std::size_t orig_num_operations = m_num_operations;
    std::size_t num_pollfd_slots = m_pollfd_slots.size();
    std::size_t pollfd_slot_ndx = 1;
    while (pollfd_slot_ndx < num_pollfd_slots && num_ready_descriptors > 0) {
        pollfd& pollfd_slot = m_pollfd_slots[pollfd_slot_ndx];
        REALM_ASSERT(pollfd_slot.fd >= 0);
        if (REALM_LIKELY(pollfd_slot.revents == 0)) {
            ++pollfd_slot_ndx;
            continue;
        }
        --num_ready_descriptors;

        REALM_ASSERT((pollfd_slot.revents & POLLNVAL) == 0);

        // Treat errors like read and/or write-readiness
        if ((pollfd_slot.revents & (POLLHUP | POLLERR)) != 0) {
            REALM_ASSERT((pollfd_slot.events & (POLLRDNORM | POLLWRNORM)) != 0);
            if ((pollfd_slot.events & POLLRDNORM) != 0)
                pollfd_slot.revents |= POLLRDNORM;
            if ((pollfd_slot.events & POLLWRNORM) != 0)
                pollfd_slot.revents |= POLLWRNORM;
        }

        OperSlot& oper_slot = m_operations[pollfd_slot.fd];
        REALM_ASSERT(oper_slot.pollfd_slot_ndx == pollfd_slot_ndx);

        OperQueue<IoOper> new_read_ops, new_write_ops;
        auto advance_ops = [&](OperQueue<IoOper>& ops) noexcept {
            while (LendersIoOperPtr op = ops.pop_front()) {
                Want want = op->advance();
                switch (want) {
                    case Want::nothing:
                        REALM_ASSERT(op->is_complete());
                        completed_ops.push_back(std::move(op));
                        --m_num_operations;
                        continue;
                    case Want::read:
                        new_read_ops.push_back(std::move(op));
                        continue;
                    case Want::write:
                        new_write_ops.push_back(std::move(op));
                        continue;
                }
                REALM_ASSERT(false);
            }
        };

        // Check read-readiness
        if ((pollfd_slot.revents & POLLRDNORM) != 0) {
            REALM_ASSERT(!oper_slot.read_ops.empty());
            advance_ops(oper_slot.read_ops);
            pollfd_slot.events &= ~POLLRDNORM;
        }

        // Check write-readiness
        if ((pollfd_slot.revents & POLLWRNORM) != 0) {
            REALM_ASSERT(!oper_slot.write_ops.empty());
            advance_ops(oper_slot.write_ops);
            pollfd_slot.events &= ~POLLWRNORM;
        }

        if (!new_read_ops.empty()) {
            oper_slot.read_ops.push_back(new_read_ops);
            pollfd_slot.events |= POLLRDNORM;
        }

        if (!new_write_ops.empty()) {
            oper_slot.write_ops.push_back(new_write_ops);
            pollfd_slot.events |= POLLWRNORM;
        }

        if (pollfd_slot.events == 0) {
            discard_pollfd_slot_by_move_last_over(oper_slot);
            --num_pollfd_slots;
        }
        else {
            ++pollfd_slot_ndx;
        }
    }

    REALM_ASSERT(num_ready_descriptors == 0);

    bool any_operations_completed = (m_num_operations < orig_num_operations);
    return any_operations_completed;
}


void Service::IoReactor::discard_pollfd_slot_by_move_last_over(OperSlot& oper_slot) noexcept
{
    std::size_t pollfd_slot_ndx = oper_slot.pollfd_slot_ndx;
    oper_slot.pollfd_slot_ndx = 0; // Mark unused
    if (pollfd_slot_ndx < m_pollfd_slots.size() - 1) {
        pollfd& last_pollfd_slot = m_pollfd_slots.back();
        m_operations[last_pollfd_slot.fd].pollfd_slot_ndx = pollfd_slot_ndx;
        m_pollfd_slots[pollfd_slot_ndx] = last_pollfd_slot;
    }
    m_pollfd_slots.pop_back();
}

#endif // !(REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE)


#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS

auto Service::IoReactor::get_and_reset_sleep_time() noexcept -> clock::duration
{
    clock::duration sleep_time = m_sleep_time;
    m_sleep_time = clock::duration::zero();
    return sleep_time;
}

#endif // REALM_UTIL_NETWORK_EVENT_LOOP_METRICS


class Service::Impl {
public:
    Service& service;
    IoReactor io_reactor;

    Impl(Service& s)
        : service{s}
        , io_reactor{} // Throws
    {
    }

    ~Impl()
    {
        bool resolver_thread_started = m_resolver_thread.joinable();
        if (resolver_thread_started) {
            {
                LockGuard lock{m_mutex};
                m_stop_resolver_thread = true;
                m_resolver_cond.notify_all();
            }
            m_resolver_thread.join();
        }

        // Avoid calls to recycle_post_oper() after destruction has begun.
        m_completed_operations.clear();
    }

    void report_event_loop_metrics(util::UniqueFunction<EventLoopMetricsHandler> handler)
    {
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        m_event_loop_metrics_timer.emplace(service);
        auto handler_2 = [this, handler = std::move(handler)](std::error_code ec) {
            REALM_ASSERT(!ec);
            clock::time_point now = clock::now();
            clock::duration elapsed_time = now - m_event_loop_metrics_start_time;
            clock::duration sleep_time = io_reactor.get_and_reset_sleep_time();
            clock::duration nonsleep_time = elapsed_time - sleep_time;
            double saturation = double(nonsleep_time.count()) / double(elapsed_time.count());
            clock::duration internal_exec_time = nonsleep_time - m_handler_exec_time;
            internal_exec_time += now - m_handler_exec_start_time;
            double inefficiency = double(internal_exec_time.count()) / double(elapsed_time.count());
            m_event_loop_metrics_start_time = now;
            m_handler_exec_start_time = now;
            m_handler_exec_time = clock::duration::zero();
            handler(saturation, inefficiency);             // Throws
            report_event_loop_metrics(std::move(handler)); // Throws
        };
        m_event_loop_metrics_timer->async_wait(std::chrono::seconds{30},
                                               std::move(handler_2)); // Throws
#else
        static_cast<void>(handler);
#endif
    }

    void run()
    {
        bool no_incomplete_resolve_operations;

    on_handlers_executed_or_interrupted : {
        LockGuard lock{m_mutex};
        if (m_stopped)
            return;
        // Note: Order of post operations must be preserved.
        m_completed_operations.push_back(m_completed_operations_2);
        no_incomplete_resolve_operations = (!m_resolve_in_progress && m_resolve_operations.empty());

        if (m_completed_operations.empty())
            goto on_time_progressed;
    }

    on_operations_completed : {
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        m_handler_exec_start_time = clock::now();
#endif
        while (LendersOperPtr op = m_completed_operations.pop_front())
            execute(op); // Throws
#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
        m_handler_exec_time += clock::now() - m_handler_exec_start_time;
#endif
        goto on_handlers_executed_or_interrupted;
    }

    on_time_progressed : {
        clock::time_point now = clock::now();
        if (process_timers(now))
            goto on_operations_completed;

        bool no_incomplete_operations =
            (io_reactor.empty() && m_wait_operations.empty() && no_incomplete_resolve_operations);
        if (no_incomplete_operations) {
            // We can only get to this point when there are no completion
            // handlers ready to execute. It happens either because of a
            // fall-through from on_operations_completed, or because of a
            // jump to on_time_progressed, but that only happens if no
            // completions handlers became ready during
            // wait_and_process_io().
            //
            // We can also only get to this point when there are no
            // asynchronous operations in progress (due to the preceeding
            // if-condition.
            //
            // It is possible that an other thread has added new post
            // operations since we checked, but there is really no point in
            // rechecking that, as it is always possible, even after a
            // recheck, that new post handlers get added after we decide to
            // return, but before we actually do return. Also, if would
            // offer no additional guarantees to the application.
            return; // Out of work
        }

        // Blocking wait for I/O
        bool interrupted = false;
        if (wait_and_process_io(now, interrupted)) // Throws
            goto on_operations_completed;
        if (interrupted)
            goto on_handlers_executed_or_interrupted;
        goto on_time_progressed;
    }
    }

    void stop() noexcept
    {
        {
            LockGuard lock{m_mutex};
            if (m_stopped)
                return;
            m_stopped = true;
        }
        io_reactor.interrupt();
    }

    void reset() noexcept
    {
        LockGuard lock{m_mutex};
        m_stopped = false;
    }

    static Endpoint::List resolve(const Resolver::Query&, std::error_code&);

    void add_resolve_oper(LendersResolveOperPtr op)
    {
        {
            LockGuard lock{m_mutex};
            m_resolve_operations.push_back(std::move(op)); // Throws
            m_resolver_cond.notify_all();
        }
        bool resolver_thread_started = m_resolver_thread.joinable();
        if (resolver_thread_started)
            return;
        auto func = [this]() noexcept {
            resolver_thread();
        };
        m_resolver_thread = std::thread{std::move(func)};
    }

    void add_wait_oper(LendersWaitOperPtr op)
    {
        m_wait_operations.push(std::move(op)); // Throws
    }

    void post(PostOperConstr constr, std::size_t size, void* cookie)
    {
        {
            LockGuard lock{m_mutex};
            std::unique_ptr<char[]> mem;
            if (m_post_oper && m_post_oper->m_size >= size) {
                // Reuse old memory
                AsyncOper* op = m_post_oper.release();
                REALM_ASSERT(dynamic_cast<UnusedOper*>(op));
                static_cast<UnusedOper*>(op)->UnusedOper::~UnusedOper(); // Static dispatch
                mem.reset(static_cast<char*>(static_cast<void*>(op)));
            }
            else {
                // Allocate new memory
                mem.reset(new char[size]); // Throws
            }

            std::unique_ptr<PostOperBase, LendersOperDeleter> op;
            op.reset((*constr)(mem.get(), size, *this, cookie)); // Throws
            mem.release();
            m_completed_operations_2.push_back(std::move(op));
        }
        io_reactor.interrupt();
    }

    void recycle_post_oper(PostOperBase* op) noexcept
    {
        std::size_t size = op->m_size;
        op->~PostOperBase();                           // Dynamic dispatch
        OwnersOperPtr op_2(new (op) UnusedOper(size)); // Does not throw

        // Keep the larger memory chunk (`op_2` or m_post_oper)
        {
            LockGuard lock{m_mutex};
            if (!m_post_oper || m_post_oper->m_size < size)
                swap(op_2, m_post_oper);
        }
    }

    void trigger_exec(TriggerExecOperBase& op) noexcept
    {
        {
            LockGuard lock{m_mutex};
            if (op.m_in_use)
                return;
            op.m_in_use = true;
            bind_ptr<TriggerExecOperBase> op_2{&op}; // Increment use count
            LendersOperPtr op_3{op_2.release()};
            m_completed_operations_2.push_back(std::move(op_3));
        }
        io_reactor.interrupt();
    }

    void reset_trigger_exec(TriggerExecOperBase& op) noexcept
    {
        LockGuard lock{m_mutex};
        op.m_in_use = false;
    }

    void add_completed_oper(LendersOperPtr op) noexcept
    {
        m_completed_operations.push_back(std::move(op));
    }

    void remove_canceled_ops(Descriptor& desc) noexcept
    {
        io_reactor.remove_canceled_ops(desc, m_completed_operations);
    }

    void cancel_resolve_oper(ResolveOperBase& op) noexcept
    {
        LockGuard lock{m_mutex};
        op.cancel();
    }

    void cancel_incomplete_wait_oper(WaitOperBase& op) noexcept
    {
        auto p = std::equal_range(m_wait_operations.begin(), m_wait_operations.end(), op.m_expiration_time,
                                  WaitOperCompare{});
        auto pred = [&op](const LendersWaitOperPtr& op_2) {
            return &*op_2 == &op;
        };
        auto i = std::find_if(p.first, p.second, pred);
        REALM_ASSERT(i != p.second);
        m_completed_operations.push_back(m_wait_operations.erase(i));
    }

private:
    OperQueue<AsyncOper> m_completed_operations; // Completed, canceled, and post operations

    struct WaitOperCompare {
        bool operator()(const LendersWaitOperPtr& a, clock::time_point b)
        {
            return a->m_expiration_time > b;
        }
        bool operator()(clock::time_point a, const LendersWaitOperPtr& b)
        {
            return a > b->m_expiration_time;
        }
        bool operator()(const LendersWaitOperPtr& a, const LendersWaitOperPtr& b)
        {
            return a->m_expiration_time > b->m_expiration_time;
        }
    };

    using WaitQueue = util::PriorityQueue<LendersWaitOperPtr, std::vector<LendersWaitOperPtr>, WaitOperCompare>;
    WaitQueue m_wait_operations;

    Mutex m_mutex;
    OwnersOperPtr m_post_oper;                       // Protected by `m_mutex`
    OperQueue<ResolveOperBase> m_resolve_operations; // Protected by `m_mutex`
    OperQueue<AsyncOper> m_completed_operations_2;   // Protected by `m_mutex`
    bool m_stopped = false;                          // Protected by `m_mutex`
    bool m_stop_resolver_thread = false;             // Protected by `m_mutex`
    bool m_resolve_in_progress = false;              // Protected by `m_mutex`
    CondVar m_resolver_cond;                         // Protected by `m_mutex`

    std::thread m_resolver_thread;

#ifdef REALM_UTIL_NETWORK_EVENT_LOOP_METRICS
    util::Optional<DeadlineTimer> m_event_loop_metrics_timer;
    clock::time_point m_event_loop_metrics_start_time = clock::now();
    clock::time_point m_handler_exec_start_time;
    clock::duration m_handler_exec_time = clock::duration::zero();
#endif

    bool process_timers(clock::time_point now)
    {
        bool any_operations_completed = false;
        for (;;) {
            if (m_wait_operations.empty())
                break;
            auto& op = m_wait_operations.top();
            if (now < op->m_expiration_time)
                break;
            op->complete();
            m_completed_operations.push_back(m_wait_operations.pop_top());
            any_operations_completed = true;
        }
        return any_operations_completed;
    }

    bool wait_and_process_io(clock::time_point now, bool& interrupted)
    {
        clock::time_point timeout;
        if (!m_wait_operations.empty())
            timeout = m_wait_operations.top()->m_expiration_time;
        bool operations_completed = io_reactor.wait_and_advance(timeout, now, interrupted,
                                                                m_completed_operations); // Throws
        return operations_completed;
    }

    static void execute(LendersOperPtr& lenders_ptr)
    {
        lenders_ptr.release()->recycle_and_execute(); // Throws
    }

    void resolver_thread() noexcept
    {
        LendersResolveOperPtr op;
        for (;;) {
            {
                LockGuard lock{m_mutex};
                if (op) {
                    m_completed_operations_2.push_back(std::move(op));
                    io_reactor.interrupt();
                }
                m_resolve_in_progress = false;
                while (m_resolve_operations.empty() && !m_stop_resolver_thread)
                    m_resolver_cond.wait(lock);
                if (m_stop_resolver_thread)
                    return;
                op = m_resolve_operations.pop_front();
                m_resolve_in_progress = true;
                if (op->is_canceled())
                    continue;
            }
            try {
                op->m_endpoints = resolve(op->m_query, op->m_error_code); // Throws only std::bad_alloc
            }
            catch (std::bad_alloc&) {
                op->m_error_code = make_basic_system_error_code(ENOMEM);
            }
            op->complete();
        }
    }
};


// This function promises to only ever throw std::bad_alloc.
Endpoint::List Service::Impl::resolve(const Resolver::Query& query, std::error_code& ec)
{
    Endpoint::List list;

    using addrinfo_type = struct addrinfo;
    addrinfo_type hints = addrinfo_type(); // Clear
    hints.ai_flags = query.m_flags;
    hints.ai_family = query.m_protocol.m_family;
    hints.ai_socktype = query.m_protocol.m_socktype;
    hints.ai_protocol = query.m_protocol.m_protocol;

    const char* query_host = query.m_host.empty() ? 0 : query.m_host.c_str();
    const char* query_service = query.m_service.empty() ? 0 : query.m_service.c_str();
    struct addrinfo* first = nullptr;
    int ret = ::getaddrinfo(query_host, query_service, &hints, &first);
    if (REALM_UNLIKELY(ret != 0)) {
#ifdef EAI_SYSTEM
        if (ret == EAI_SYSTEM) {
            if (errno != 0) {
                ec = make_basic_system_error_code(errno);
            }
            else {
                ec = error::unknown;
            }
            return list;
        }
#endif
        ec = translate_addrinfo_error(ret);
        return list;
    }

    GetaddrinfoResultOwner gro(first);

    // Count number of IPv4/IPv6 endpoints
    std::size_t num_endpoints = 0;
    {
        struct addrinfo* curr = first;
        while (curr) {
            bool ip_v4 = curr->ai_family == AF_INET;
            bool ip_v6 = curr->ai_family == AF_INET6;
            if (ip_v4 || ip_v6)
                ++num_endpoints;
            curr = curr->ai_next;
        }
    }
    REALM_ASSERT(num_endpoints >= 1);

    // Copy the IPv4/IPv6 endpoints
    list.m_endpoints.set_size(num_endpoints); // Throws
    struct addrinfo* curr = first;
    std::size_t endpoint_ndx = 0;
    while (curr) {
        bool ip_v4 = curr->ai_family == AF_INET;
        bool ip_v6 = curr->ai_family == AF_INET6;
        if (ip_v4 || ip_v6) {
            REALM_ASSERT((ip_v4 && curr->ai_addrlen == sizeof(Endpoint::sockaddr_ip_v4_type)) ||
                         (ip_v6 && curr->ai_addrlen == sizeof(Endpoint::sockaddr_ip_v6_type)));
            Endpoint& ep = list.m_endpoints[endpoint_ndx];
            ep.m_protocol.m_family = curr->ai_family;
            ep.m_protocol.m_socktype = curr->ai_socktype;
            ep.m_protocol.m_protocol = curr->ai_protocol;
            if (ip_v4) {
                ep.m_sockaddr_union.m_ip_v4 = reinterpret_cast<Endpoint::sockaddr_ip_v4_type&>(*curr->ai_addr);
            }
            else {
                ep.m_sockaddr_union.m_ip_v6 = reinterpret_cast<Endpoint::sockaddr_ip_v6_type&>(*curr->ai_addr);
            }
            ++endpoint_ndx;
        }
        curr = curr->ai_next;
    }

    ec = std::error_code(); // Success
    return list;
}


Service::Service()
    : m_impl{std::make_unique<Impl>(*this)} // Throws
{
}


Service::~Service() noexcept {}


void Service::run()
{
    m_impl->run(); // Throws
}


void Service::stop() noexcept
{
    m_impl->stop();
}


void Service::reset() noexcept
{
    m_impl->reset();
}


void Service::report_event_loop_metrics(util::UniqueFunction<EventLoopMetricsHandler> handler)
{
    m_impl->report_event_loop_metrics(std::move(handler)); // Throws
}


void Service::do_post(PostOperConstr constr, std::size_t size, void* cookie)
{
    m_impl->post(constr, size, cookie); // Throws
}


void Service::recycle_post_oper(Impl& impl, PostOperBase* op) noexcept
{
    impl.recycle_post_oper(op);
}


void Service::trigger_exec(Impl& impl, TriggerExecOperBase& op) noexcept
{
    impl.trigger_exec(op);
}


void Service::reset_trigger_exec(Impl& impl, TriggerExecOperBase& op) noexcept
{
    impl.reset_trigger_exec(op);
}


void Service::Descriptor::accept(Descriptor& desc, StreamProtocol protocol, Endpoint* ep,
                                 std::error_code& ec) noexcept
{
    REALM_ASSERT(is_open());

    union union_type {
        Endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof(Endpoint::sockaddr_union_type) + 1];
    };
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    CloseGuard new_sock_fd;
    for (;;) {
#if HAVE_LINUX_ACCEPT4
        // On Linux (HAVE_LINUX_ACCEPT4), make the accepted socket inherit the
        // O_NONBLOCK status flag from the accepting socket to avoid an extra
        // call to fcntl(). Note, it is deemed most likely that the accepted
        // socket is going to be used in nonblocking when, and only when the
        // accepting socket is used in nonblocking mode. Other platforms are
        // handled below.
        int flags = SOCK_CLOEXEC;
        if (!in_blocking_mode())
            flags |= SOCK_NONBLOCK;
        native_handle_type ret = ::accept4(m_fd, addr, &addr_len, flags);
#else
        native_handle_type ret = ::accept(m_fd, addr, &addr_len);
#endif
#ifdef _WIN32
        if (ret == INVALID_SOCKET) {
            int err = WSAGetLastError();
            if (err == WSAEINTR)
                continue; // Retry on interruption by system signal
            set_read_ready(err != WSAEWOULDBLOCK);
            ec = make_winsock_error_code(err); // Failure
            return;
        }
#else
        if (REALM_UNLIKELY(ret == -1)) {
            int err = errno;
            if (err == EINTR)
                continue; // Retry on interruption by system signal
            if (err == EWOULDBLOCK)
                err = EAGAIN;
            set_read_ready(err != EAGAIN);
            ec = make_basic_system_error_code(err); // Failure
            return;
        }
#endif
        new_sock_fd.reset(ret);
        set_read_ready(true);
        break;
    }
    socklen_t expected_addr_len =
        protocol.is_ip_v4() ? sizeof(Endpoint::sockaddr_ip_v4_type) : sizeof(Endpoint::sockaddr_ip_v6_type);
    if (REALM_UNLIKELY(addr_len != expected_addr_len))
        REALM_TERMINATE("Unexpected peer address length");

#if !HAVE_LINUX_ACCEPT4
    {
        bool value = true;
        if (REALM_UNLIKELY(set_cloexec_flag(new_sock_fd, value, ec)))
            return;
    }
#endif

    // On some platforms (such as Mac OS X), the accepted socket automatically
    // inherits file status flags from the accepting socket, but on other
    // systems, this is not the case. In the case of Linux (HAVE_LINUX_ACCEPT4),
    // the inheriting behaviour is obtained by using the Linux specific
    // accept4() system call.
    //
    // For other platforms, we need to be sure that m_in_blocking_mode for the
    // new socket is initialized to reflect the actual state of O_NONBLOCK on
    // the new socket.
    //
    // Note: This implementation currently never modifies status flags other
    // than O_NONBLOCK, so we only need to consider that flag.

#if !REALM_PLATFORM_APPLE && !HAVE_LINUX_ACCEPT4
    // Make the accepted socket inherit the state of O_NONBLOCK from the
    // accepting socket.
    {
        bool value = !m_in_blocking_mode;
        if (::set_nonblock_flag(new_sock_fd, value, ec))
            return;
    }
#endif

#if REALM_PLATFORM_APPLE
    int optval = 1;
    int ret = ::setsockopt(new_sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return;
    }
#endif

    desc.assign(new_sock_fd.release(), m_in_blocking_mode);
    desc.set_write_ready(true);
    if (ep) {
        ep->m_protocol = protocol;
        ep->m_sockaddr_union = buffer.m_sockaddr_union;
    }
    ec = std::error_code(); // Success
}


std::size_t Service::Descriptor::read_some(char* buffer, std::size_t size, std::error_code& ec) noexcept
{
    if (REALM_UNLIKELY(assume_read_would_block())) {
        ec = error::resource_unavailable_try_again; // Failure
        return 0;
    }
    for (;;) {
        int flags = 0;
#ifdef _WIN32
        ssize_t ret = ::recv(m_fd, buffer, int(size), flags);
        if (ret == SOCKET_ERROR) {
            int err = WSAGetLastError();
            // Retry on interruption by system signal
            if (err == WSAEINTR)
                continue;
            set_read_ready(err != WSAEWOULDBLOCK);
            ec = make_winsock_error_code(err); // Failure
            return 0;
        }
#else
        ssize_t ret = ::recv(m_fd, buffer, size, flags);
        if (ret == -1) {
            int err = errno;
            // Retry on interruption by system signal
            if (err == EINTR)
                continue;
            if (err == EWOULDBLOCK)
                err = EAGAIN;
            set_read_ready(err != EAGAIN);
            ec = make_basic_system_error_code(err); // Failure
            return 0;
        }
#endif
        if (REALM_UNLIKELY(ret == 0)) {
            set_read_ready(true);
            ec = MiscExtErrors::end_of_input;
            return 0;
        }
        REALM_ASSERT(ret > 0);
        std::size_t n = std::size_t(ret);
        REALM_ASSERT(n <= size);
#if REALM_NETWORK_USE_EPOLL
        // On Linux a partial read (n < size) on a nonblocking stream-mode
        // socket is guaranteed to only ever happen if a complete read would
        // have been impossible without blocking (i.e., without failing with
        // EAGAIN/EWOULDBLOCK), or if the end of input from the remote peer was
        // detected by the Linux kernel.
        //
        // Further more, after a partial read, and when working with Linux epoll
        // in edge-triggered mode (EPOLLET), it is safe to suspend further
        // reading until a new read-readiness notification is received, provided
        // that we registered interest in EPOLLRDHUP events, and an EPOLLRDHUP
        // event was not received prior to the partial read. This is safe in the
        // sense that reading is guaranteed to be resumed in a timely fashion
        // (without unnessesary blocking), and in a manner that is free of race
        // conditions. Note in particular that if a read was partial because the
        // kernel had detected the end of input prior to that read, but the
        // EPOLLRDHUP event was not received prior the that read, then reading
        // will still be resumed immediately by the pending EPOLLRDHUP event.
        //
        // Note that without this extra "loss of read-readiness" trigger, it
        // would have been necessary for the caller to immediately follow up
        // with an (otherwise redundant) additional invocation of read_some()
        // just to detect the loss of read-readiness.
        //
        // FIXME: Will this scheme also work with Kqueue on FreeBSD and macOS?
        // In particular, do we know that a partial read (n < size) on a
        // nonblocking stream-mode socket is guaranteed to only ever happen if a
        // complete read would have been impossible without blocking, or if the
        // end of input from the remote peer was detected by the FreeBSD and/or
        // macOS kernel? See http://stackoverflow.com/q/40123626/1698548.
        set_read_ready(n == size || m_imminent_end_of_input);
#else
        set_read_ready(true);
#endif
        ec = std::error_code(); // Success
        return n;
    }
}


std::size_t Service::Descriptor::write_some(const char* data, std::size_t size, std::error_code& ec) noexcept
{
    if (REALM_UNLIKELY(assume_write_would_block())) {
        ec = error::resource_unavailable_try_again; // Failure
        return 0;
    }
    for (;;) {
        int flags = 0;
#ifdef __linux__
        // Prevent SIGPIPE when remote peer has closed the connection.
        flags |= MSG_NOSIGNAL;
#endif
#ifdef _WIN32
        ssize_t ret = ::send(m_fd, data, int(size), flags);
        if (ret == SOCKET_ERROR) {
            int err = WSAGetLastError();
            // Retry on interruption by system signal
            if (err == WSAEINTR)
                continue;
            set_write_ready(err != WSAEWOULDBLOCK);
            ec = make_winsock_error_code(err); // Failure
            return 0;
        }
#else
        ssize_t ret = ::send(m_fd, data, size, flags);
        if (ret == -1) {
            int err = errno;
            // Retry on interruption by system signal
            if (err == EINTR)
                continue;
#if REALM_PLATFORM_APPLE
            // The macOS kernel can generate an undocumented EPROTOTYPE in
            // certain cases where the peer has closed the connection (in
            // tcp_usr_send() in bsd/netinet/tcp_usrreq.c) See also
            // http://erickt.github.io/blog/2014/11/19/adventures-in-debugging-a-potential-osx-kernel-bug/.
            if (REALM_UNLIKELY(err == EPROTOTYPE))
                err = EPIPE;
#endif
            if (err == EWOULDBLOCK)
                err = EAGAIN;
            set_write_ready(err != EAGAIN);
            ec = make_basic_system_error_code(err); // Failure
            return 0;
        }
#endif
        REALM_ASSERT(ret >= 0);
        std::size_t n = std::size_t(ret);
        REALM_ASSERT(n <= size);
#if REALM_NETWORK_USE_EPOLL
        // On Linux a partial write (n < size) on a nonblocking stream-mode
        // socket is guaranteed to only ever happen if a complete write would
        // have been impossible without blocking (i.e., without failing with
        // EAGAIN/EWOULDBLOCK).
        //
        // Further more, after a partial write, and when working with Linux
        // epoll in edge-triggered mode (EPOLLET), it is safe to suspend further
        // writing until a new write-readiness notification is received. This is
        // safe in the sense that writing is guaranteed to be resumed in a
        // timely fashion (without unnessesary blocking), and in a manner that
        // is free of race conditions.
        //
        // Note that without this extra "loss of write-readiness" trigger, it
        // would have been necessary for the caller to immediately follow up
        // with an (otherwise redundant) additional invocation of write_some()
        // just to detect the loss of write-readiness.
        //
        // FIXME: Will this scheme also work with Kqueue on FreeBSD and macOS?
        // In particular, do we know that a partial write (n < size) on a
        // nonblocking stream-mode socket is guaranteed to only ever happen if a
        // complete write would have been impossible without blocking? See
        // http://stackoverflow.com/q/40123626/1698548.
        set_write_ready(n == size);
#else
        set_write_ready(true);
#endif
        ec = std::error_code(); // Success
        return n;
    }
}


#if REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE

void Service::Descriptor::deregister_for_async() noexcept
{
    service_impl.io_reactor.deregister_desc(*this);
}

#endif // REALM_NETWORK_USE_EPOLL || REALM_HAVE_KQUEUE


void Service::Descriptor::set_nonblock_flag(bool value)
{
    ::set_nonblock_flag(m_fd, value); // Throws
}


void Service::Descriptor::add_initiated_oper(LendersIoOperPtr op, Want want)
{
    if (REALM_UNLIKELY(want == Want::nothing)) {
        REALM_ASSERT(op->is_complete());
        service_impl.add_completed_oper(std::move(op));
        return;
    }
    REALM_ASSERT(!op->is_complete());
    service_impl.io_reactor.add_oper(*this, std::move(op), want); // Throws
}


void Service::Descriptor::do_close() noexcept
{
    checked_close(m_fd);
    m_fd = -1;
}


auto Service::Descriptor::do_release() noexcept -> native_handle_type
{
    native_handle_type fd = m_fd;
    m_fd = -1;
    return fd;
}


Service& Resolver::get_service() noexcept
{
    return m_service_impl.service;
}


Endpoint::List Resolver::resolve(const Query& query, std::error_code& ec)
{
    return Service::Impl::resolve(query, ec); // Throws
}


void Resolver::cancel() noexcept
{
    if (m_resolve_oper && m_resolve_oper->in_use() && !m_resolve_oper->is_canceled()) {
        Service::ResolveOperBase& op = static_cast<Service::ResolveOperBase&>(*m_resolve_oper);
        m_service_impl.cancel_resolve_oper(op);
    }
}


void Resolver::initiate_oper(Service::LendersResolveOperPtr op)
{
    m_service_impl.add_resolve_oper(std::move(op)); // Throws
}


Service& SocketBase::get_service() noexcept
{
    return m_desc.service_impl.service;
}


void SocketBase::cancel() noexcept
{
    bool any_incomplete = false;
    if (m_read_oper && m_read_oper->in_use() && !m_read_oper->is_canceled()) {
        m_read_oper->cancel();
        if (!m_read_oper->is_complete())
            any_incomplete = true;
    }
    if (m_write_oper && m_write_oper->in_use() && !m_write_oper->is_canceled()) {
        m_write_oper->cancel();
        if (!m_write_oper->is_complete())
            any_incomplete = true;
    }
    if (any_incomplete)
        m_desc.service_impl.remove_canceled_ops(m_desc);
}


std::error_code SocketBase::bind(const Endpoint& ep, std::error_code& ec)
{
    if (!is_open()) {
        if (REALM_UNLIKELY(open(ep.protocol(), ec)))
            return ec;
    }

    native_handle_type sock_fd = m_desc.native_handle();
    socklen_t addr_len =
        ep.m_protocol.is_ip_v4() ? sizeof(Endpoint::sockaddr_ip_v4_type) : sizeof(Endpoint::sockaddr_ip_v6_type);

    int ret = ::bind(sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ec;
    ec = std::error_code(); // Success
    return ec;
}


Endpoint SocketBase::local_endpoint(std::error_code& ec) const
{
    Endpoint ep;
    union union_type {
        Endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof(Endpoint::sockaddr_union_type) + 1];
    };
    native_handle_type sock_fd = m_desc.native_handle();
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    int ret = ::getsockname(sock_fd, addr, &addr_len);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ep;

    socklen_t expected_addr_len =
        m_protocol.is_ip_v4() ? sizeof(Endpoint::sockaddr_ip_v4_type) : sizeof(Endpoint::sockaddr_ip_v6_type);
    if (addr_len != expected_addr_len)
        throw util::runtime_error("Unexpected local address length");
    ep.m_protocol = m_protocol;
    ep.m_sockaddr_union = buffer.m_sockaddr_union;
    ec = std::error_code(); // Success
#ifdef _WIN32
    ep.m_sockaddr_union.m_ip_v4.sin_addr.s_addr = inet_addr("127.0.0.1");
#endif
    return ep;
}


std::error_code SocketBase::open(const StreamProtocol& prot, std::error_code& ec)
{
    if (REALM_UNLIKELY(is_open()))
        throw util::runtime_error("Socket is already open");
    int type = prot.m_socktype;
#if HAVE_LINUX_SOCK_CLOEXEC
    type |= SOCK_CLOEXEC;
#endif
    native_handle_type ret = ::socket(prot.m_family, type, prot.m_protocol);
#ifdef _WIN32
    if (REALM_UNLIKELY(ret == INVALID_SOCKET)) {
        ec = make_winsock_error_code(WSAGetLastError());
        return ec;
    }
#else
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
#endif

    CloseGuard sock_fd{ret};

#if !HAVE_LINUX_SOCK_CLOEXEC
    {
        bool value = true;
        if (REALM_UNLIKELY(set_cloexec_flag(sock_fd, value, ec)))
            return ec;
    }
#endif

#if REALM_PLATFORM_APPLE
    {
        int optval = 1;
        int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
        if (REALM_UNLIKELY(ret == -1)) {
            ec = make_basic_system_error_code(errno);
            return ec;
        }
    }
#endif

    bool in_blocking_mode = true; // New sockets are in blocking mode by default
    m_desc.assign(sock_fd.release(), in_blocking_mode);
    m_protocol = prot;
    ec = std::error_code(); // Success
    return ec;
}


std::error_code SocketBase::do_assign(const StreamProtocol& prot, native_handle_type sock_fd, std::error_code& ec)
{
    if (REALM_UNLIKELY(is_open()))
        throw util::runtime_error("Socket is already open");

    // We need to know whether the specified socket is in blocking or in
    // nonblocking mode. Rather than reading the current mode, we set it to
    // blocking mode (disable nonblocking mode), and initialize
    // `m_in_blocking_mode` to true.
    {
        bool value = false;
        if (::set_nonblock_flag(sock_fd, value, ec))
            return ec;
    }

    bool in_blocking_mode = true; // New sockets are in blocking mode by default
    m_desc.assign(sock_fd, in_blocking_mode);
    m_protocol = prot;
    ec = std::error_code(); // Success
    return ec;
}


void SocketBase::get_option(opt_enum opt, void* value_data, std::size_t& value_size, std::error_code& ec) const
{
    int level = 0;
    int option_name = 0;
    map_option(opt, level, option_name);

    native_handle_type sock_fd = m_desc.native_handle();
    socklen_t option_len = socklen_t(value_size);
    int ret = ::getsockopt(sock_fd, level, option_name, static_cast<char*>(value_data), &option_len);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return;
    value_size = std::size_t(option_len);
    ec = std::error_code(); // Success
}


void SocketBase::set_option(opt_enum opt, const void* value_data, std::size_t value_size, std::error_code& ec)
{
    int level = 0;
    int option_name = 0;
    map_option(opt, level, option_name);

    native_handle_type sock_fd = m_desc.native_handle();
    int ret = ::setsockopt(sock_fd, level, option_name, static_cast<const char*>(value_data), socklen_t(value_size));
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return;
    ec = std::error_code(); // Success
}


void SocketBase::map_option(opt_enum opt, int& level, int& option_name) const
{
    switch (opt) {
        case opt_ReuseAddr:
            level = SOL_SOCKET;
            option_name = SO_REUSEADDR;
            return;
        case opt_Linger:
            level = SOL_SOCKET;
#if REALM_PLATFORM_APPLE
            // By default, SO_LINGER on Darwin uses "ticks" instead of
            // seconds for better accuracy, but we want to be cross-platform.
            option_name = SO_LINGER_SEC;
#else
            option_name = SO_LINGER;
#endif // REALM_PLATFORM_APPLE
            return;
        case opt_NoDelay:
            level = IPPROTO_TCP;
            option_name = TCP_NODELAY; // Specified by POSIX.1-2001
            return;
    }
    REALM_ASSERT(false);
}


std::error_code Socket::connect(const Endpoint& ep, std::error_code& ec)
{
    REALM_ASSERT(!m_write_oper || !m_write_oper->in_use());

    if (!is_open()) {
        if (REALM_UNLIKELY(open(ep.protocol(), ec)))
            return ec;
    }

    m_desc.ensure_blocking_mode(); // Throws

    native_handle_type sock_fd = m_desc.native_handle();
    socklen_t addr_len =
        (ep.m_protocol.is_ip_v4() ? sizeof(Endpoint::sockaddr_ip_v4_type) : sizeof(Endpoint::sockaddr_ip_v6_type));
    int ret = ::connect(sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ec;
    ec = std::error_code(); // Success
    return ec;
}


std::error_code Socket::shutdown(shutdown_type what, std::error_code& ec)
{
    native_handle_type sock_fd = m_desc.native_handle();
    int how = what;
    int ret = ::shutdown(sock_fd, how);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ec;
    ec = std::error_code(); // Success
    return ec;
}


bool Socket::initiate_async_connect(const Endpoint& ep, std::error_code& ec)
{
    if (!is_open()) {
        if (REALM_UNLIKELY(open(ep.protocol(), ec)))
            return true; // Failure
    }
    m_desc.ensure_nonblocking_mode(); // Throws

    // Initiate connect operation.
    native_handle_type sock_fd = m_desc.native_handle();
    socklen_t addr_len =
        ep.m_protocol.is_ip_v4() ? sizeof(Endpoint::sockaddr_ip_v4_type) : sizeof(Endpoint::sockaddr_ip_v6_type);
    int ret = ::connect(sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (ret != -1) {
        ec = std::error_code(); // Success
        return true;            // Immediate completion.
    }

    // EINPROGRESS (and on Windows, also WSAEWOULDBLOCK) indicates that the
    // underlying connect operation was successfully initiated, but not
    // immediately completed, and EALREADY indicates that an underlying connect
    // operation was already initiated, and still not completed, presumably
    // because a previous call to connect() or async_connect() failed, or was
    // canceled.

#ifdef _WIN32
    int err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK) {
        ec = make_winsock_error_code(err);
        return true; // Failure
    }
#else
    int err = errno;
    if (REALM_UNLIKELY(err != EINPROGRESS && err != EALREADY)) {
        ec = make_basic_system_error_code(err);
        return true; // Failure
    }
#endif

    return false; // Successful initiation, but no immediate completion.
}


std::error_code Socket::finalize_async_connect(std::error_code& ec) noexcept
{
    native_handle_type sock_fd = m_desc.native_handle();
    int connect_errno = 0;
    socklen_t connect_errno_size = sizeof connect_errno;
    int ret =
        ::getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&connect_errno), &connect_errno_size);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ec; // getsockopt() failed
    if (REALM_UNLIKELY(connect_errno)) {
        ec = make_basic_system_error_code(connect_errno);
        return ec; // connect failed
    }
    return std::error_code(); // Success
}


std::error_code Acceptor::listen(int backlog, std::error_code& ec)
{
    native_handle_type sock_fd = m_desc.native_handle();
    int ret = ::listen(sock_fd, backlog);
    if (REALM_UNLIKELY(check_socket_error(ret, ec)))
        return ec;
    ec = std::error_code(); // Success
    return ec;
}


Service& DeadlineTimer::get_service() noexcept
{
    return m_service_impl.service;
}


void DeadlineTimer::cancel() noexcept
{
    if (m_wait_oper && m_wait_oper->in_use() && !m_wait_oper->is_canceled()) {
        m_wait_oper->cancel();
        if (!m_wait_oper->is_complete()) {
            using WaitOperBase = Service::WaitOperBase;
            WaitOperBase& wait_operation = static_cast<WaitOperBase&>(*m_wait_oper);
            m_service_impl.cancel_incomplete_wait_oper(wait_operation);
        }
    }
}


void DeadlineTimer::initiate_oper(Service::LendersWaitOperPtr op)
{
    m_service_impl.add_wait_oper(std::move(op)); // Throws
}


bool ReadAheadBuffer::read(char*& begin, char* end, int delim, std::error_code& ec) noexcept
{
    std::size_t in_avail = m_end - m_begin;
    std::size_t out_avail = end - begin;
    std::size_t n = std::min(in_avail, out_avail);
    bool delim_mode = (delim != std::char_traits<char>::eof());
    char* i =
        (!delim_mode ? m_begin + n : std::find(m_begin, m_begin + n, std::char_traits<char>::to_char_type(delim)));
    begin = std::copy(m_begin, i, begin);
    m_begin = i;
    if (begin == end) {
        if (delim_mode)
            ec = MiscExtErrors::delim_not_found;
    }
    else {
        if (m_begin == m_end)
            return false;
        REALM_ASSERT(delim_mode);
        *begin++ = *m_begin++; // Transfer delimiter
    }
    return true;
}


namespace realm {
namespace util {
namespace network {

std::string host_name()
{
    // POSIX allows for gethostname() to report success even if the buffer is
    // too small to hold the name, and in that case POSIX requires that the
    // buffer is filled, but not that it contains a final null-termination.
    char small_stack_buffer[256];
    int ret = ::gethostname(small_stack_buffer, sizeof small_stack_buffer);
    if (ret != -1) {
        // Check that a null-termination was included
        char* end = small_stack_buffer + sizeof small_stack_buffer;
        char* i = std::find(small_stack_buffer, end, 0);
        if (i != end)
            return std::string(small_stack_buffer, i);
    }
    constexpr std::size_t large_heap_buffer_size = 4096;
    std::unique_ptr<char[]> large_heap_buffer(new char[large_heap_buffer_size]); // Throws
    ret = ::gethostname(large_heap_buffer.get(), large_heap_buffer_size);
    if (REALM_LIKELY(ret != -1)) {
        // Check that a null-termination was included
        char* end = large_heap_buffer.get() + large_heap_buffer_size;
        char* i = std::find(large_heap_buffer.get(), end, 0);
        if (i != end)
            return std::string(large_heap_buffer.get(), i);
    }
    throw std::system_error(errno, std::system_category(), "gethostname() failed");
}


Address make_address(const char* c_str, std::error_code& ec) noexcept
{
    Address addr;
    int ret = ::inet_pton(AF_INET6, c_str, &addr.m_union);
    REALM_ASSERT(ret == 0 || ret == 1);
    if (ret == 1) {
        addr.m_is_ip_v6 = true;
        ec = std::error_code(); // Success (IPv6)
        return addr;
    }
    ret = ::inet_pton(AF_INET, c_str, &addr.m_union);
    REALM_ASSERT(ret == 0 || ret == 1);
    if (ret == 1) {
        ec = std::error_code(); // Success (IPv4)
        return addr;
    }
    ec = error::invalid_argument;
    return Address();

    // FIXME: Currently. `addr.m_ip_v6_scope_id` is always set to zero. It nees
    // to be set based on a combined inspection of the original string
    // representation, and the parsed address. The following code is "borrowed"
    // from ASIO:
    /*
        *scope_id = 0;
        if (const char* if_name = strchr(src, '%'))
        {
          in6_addr_type* ipv6_address = static_cast<in6_addr_type*>(dest);
          bool is_link_local = ((ipv6_address->s6_addr[0] == 0xfe)
              && ((ipv6_address->s6_addr[1] & 0xc0) == 0x80));
          bool is_multicast_link_local = ((ipv6_address->s6_addr[0] == 0xff)
              && ((ipv6_address->s6_addr[1] & 0x0f) == 0x02));
          if (is_link_local || is_multicast_link_local)
            *scope_id = if_nametoindex(if_name + 1);
          if (*scope_id == 0)
            *scope_id = atoi(if_name + 1);
        }
    */
}


ResolveErrorCategory resolve_error_category;


const char* ResolveErrorCategory::name() const noexcept
{
    return "realm.util.network.resolve";
}


std::string ResolveErrorCategory::message(int value) const
{
    switch (ResolveErrors(value)) {
        case ResolveErrors::host_not_found:
            return "Host not found (authoritative)";
        case ResolveErrors::host_not_found_try_again:
            return "Host not found (non-authoritative)";
        case ResolveErrors::no_data:
            return "The query is valid but does not have associated address data";
        case ResolveErrors::no_recovery:
            return "A non-recoverable error occurred";
        case ResolveErrors::service_not_found:
            return "The service is not supported for the given socket type";
        case ResolveErrors::socket_type_not_supported:
            return "The socket type is not supported";
    }
    REALM_ASSERT(false);
    return {};
}

} // namespace network
} // namespace util
} // namespace realm
