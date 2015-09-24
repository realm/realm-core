#include <errno.h>
#include <algorithm>
#include <vector>
#include <stdexcept>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include <realm/util/misc_errors.hpp>
#include <realm/util/thread.hpp>
#include <realm/util/network.hpp>

using namespace realm::util;
using namespace realm::util::network;


namespace {

std::error_code set_nonblocking(int fd, bool enable, std::error_code& ec) noexcept
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (REALM_UNLIKELY(flags == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    flags &= ~O_NONBLOCK;
    if (enable)
        flags |= O_NONBLOCK;
    int ret = ::fcntl(fd, F_SETFL, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    ec = std::error_code(); // Success
    return ec;
}

void set_nonblocking(int fd, bool enable)
{
    std::error_code ec;
    if (set_nonblocking(fd, enable, ec))
        throw std::system_error(ec);
}


std::error_code translate_addrinfo_error(int err)
{
    switch (err) {
        case EAI_AGAIN:
            return host_not_found_try_again;
        case EAI_BADFLAGS:
            return error::invalid_argument;
        case EAI_FAIL:
            return no_recovery;
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
            return host_not_found;
        case EAI_SERVICE:
            return service_not_found;
        case EAI_SOCKTYPE:
            return socket_type_not_supported;
        default:
            return error::unknown;
    }
}


struct getaddrinfo_result_owner {
    struct addrinfo* ptr;
    getaddrinfo_result_owner(struct addrinfo* p):
        ptr(p)
    {
    }
    ~getaddrinfo_result_owner()
    {
        if (ptr)
            freeaddrinfo(ptr);
    }
};


class network_error_category: public std::error_category {
    const char* name() const noexcept override;
    std::string message(int) const override;
};

network_error_category g_network_error_category;

const char* network_error_category::name() const noexcept
{
    return "realm.network";
}

std::string network_error_category::message(int value) const
{
    switch (errors(value)) {
        case end_of_input:
            return "End of input";
        case delim_not_found:
            return "Delimiter not found";
        case host_not_found:
            return "Host not found (authoritative)";
        case host_not_found_try_again:
            return "Host not found (non-authoritative)";
        case no_data:
            return "The query is valid but does not have associated address data";
        case no_recovery:
            return "A non-recoverable error occurred";
        case service_not_found:
            return "The service is not supported for the given socket type";
        case socket_type_not_supported:
            return "The socket type is not supported";
    }
    REALM_ASSERT(false);
    return std::string();
}

} // anonymous namespace


class io_service::oper_queue {
public:
    bool empty() const noexcept
    {
        return !m_back;
    }
    void push_back(std::unique_ptr<async_oper> op) noexcept
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
    void push_back(oper_queue& q) noexcept
    {
        if (!q.m_back)
            return;
        if (m_back)
            std::swap(m_back->m_next, q.m_back->m_next);
        m_back = q.m_back;
        q.m_back = 0;
    }
    std::unique_ptr<async_oper> pop_front() noexcept
    {
        async_oper* op = 0;
        if (m_back) {
            op = m_back->m_next;
            if (op != m_back) {
                m_back->m_next = op->m_next;
            }
            else {
                m_back = 0;
            }
            op->m_next = 0;
        }
        return std::unique_ptr<async_oper>(op);
    }
    ~oper_queue() noexcept
    {
        if (m_back) {
            async_oper* op = m_back;
            for (;;) {
                async_oper* next = op->m_next;
                delete op;
                if (next == m_back)
                    break;
                op = next;
            }
        }
    }
private:
    async_oper* m_back = 0;
};


class io_service::impl {
public:
    impl()
    {
        int fildes[2];
        int ret = ::pipe(fildes);
        if (REALM_UNLIKELY(ret == -1)) {
            std::error_code ec = make_basic_system_error_code(errno);
            throw std::system_error(ec);
        }
        try {
            set_nonblocking(fildes[0], true); // Throws
            set_nonblocking(fildes[1], true); // Throws
            m_wakeup_pipe_read_fd  = fildes[0];
            m_wakeup_pipe_write_fd = fildes[1];
            pollfd slot = pollfd(); // Cleared slot
            slot.fd = m_wakeup_pipe_read_fd;
            slot.events = POLLRDNORM;
            m_pollfd_slots.push_back(slot); // Throws
        }
        catch (...) {
            ::close(fildes[0]);
            ::close(fildes[1]);
            throw;
        }
    }

    ~impl()
    {
        ::close(m_wakeup_pipe_read_fd);
        ::close(m_wakeup_pipe_write_fd);
#if REALM_ASSERTIONS_ENABLED
        size_t n = 0;
        for (size_t i = 0; i < m_io_operations.size(); ++i) {
            if (m_io_operations[i].read_oper)
                ++n;
            if (m_io_operations[i].write_oper)
                ++n;
        }
        REALM_ASSERT(n == m_num_active_io_operations);
#endif
    }

    void run()
    {
        for (;;) {
            {
                LockGuard l(m_mutex);
                if (m_stopped)
                    break;
                m_completed_operations.push_back(m_post_operations);
            }

            if (!m_completed_operations.empty()) {
                while (std::unique_ptr<async_oper> op = m_completed_operations.pop_front())
                    op->exec_handler(); // Throws
                continue;
            }

            if (m_num_active_io_operations > 0) {
                // Blocking wait for I/O
                wait_for_io(); // Throws
                continue;
            }

            break; // Out of work
        }
    }

    void stop() noexcept
    {
        {
            LockGuard l(m_mutex);
            if (m_stopped)
                return;
            m_stopped = true;
        }
        wake_up_poll_thread();
    }

    void reset() noexcept
    {
        LockGuard l(m_mutex);
        m_stopped = false;
    }

    void add_io_oper(int fd, std::unique_ptr<async_oper> op, io_op type)
    {
        REALM_ASSERT(fd >= 0);

        size_t n = m_io_operations.size();
        REALM_ASSERT(n == m_pollfd_slots.size() - 1);
        size_t n_2 = fd + 1;
        if (n_2 > n) {
            pollfd pollfd_slot = pollfd(); // Cleared slot
            pollfd_slot.fd = -1; // Unused
            m_pollfd_slots.reserve(n_2+1); // Throws
            m_io_operations.resize(n_2); // Throws
            m_pollfd_slots.resize(n_2+1, pollfd_slot);
        }

        pollfd& pollfd_slot = m_pollfd_slots[fd+1];
        io_oper_slot& oper_slot = m_io_operations[fd];
        REALM_ASSERT(pollfd_slot.fd == -1 || pollfd_slot.fd == fd);
        REALM_ASSERT((pollfd_slot.fd == -1) == (pollfd_slot.events == 0));
        REALM_ASSERT(((pollfd_slot.events & POLLRDNORM) != 0) ==
                       bool(oper_slot.read_oper));
        REALM_ASSERT(((pollfd_slot.events & POLLWRNORM) != 0) ==
                       bool(oper_slot.write_oper));
        REALM_ASSERT((pollfd_slot.events & ~(POLLRDNORM|POLLWRNORM)) == 0);
        switch (type) {
            case io_op_Read:
                REALM_ASSERT(!oper_slot.read_oper);
                pollfd_slot.events |= POLLRDNORM;
                oper_slot.read_oper = move(op);
                goto finish;
            case io_op_Write:
                REALM_ASSERT(!oper_slot.write_oper);
                pollfd_slot.events |= POLLWRNORM;
                oper_slot.write_oper = move(op);
                goto finish;
        }
        REALM_ASSERT(false);
        return;

      finish:
        pollfd_slot.fd = fd;
        ++m_num_active_io_operations;
    }

    void add_completed_oper(std::unique_ptr<async_oper> op) noexcept
    {
        m_completed_operations.push_back(move(op));
    }

    void add_post_oper(std::unique_ptr<async_oper> op) noexcept
    {
        {
            LockGuard l(m_mutex);
            m_post_operations.push_back(move(op));
        }
        wake_up_poll_thread();
    }

    void cancel_incomplete_io_ops(int fd) noexcept
    {
        REALM_ASSERT(fd >= 0);
        REALM_ASSERT(unsigned(fd) < m_io_operations.size());
        REALM_ASSERT(m_io_operations.size() == m_pollfd_slots.size() - 1);
        pollfd& pollfd_slot = m_pollfd_slots[fd+1];
        REALM_ASSERT(pollfd_slot.fd == fd);
        pollfd_slot.fd = -1; // Mark unused
        pollfd_slot.events = 0;
        io_oper_slot& oper_slot = m_io_operations[fd];
        REALM_ASSERT(oper_slot.read_oper || oper_slot.write_oper);
        if (oper_slot.read_oper) {
            m_completed_operations.push_back(move(oper_slot.read_oper));
            --m_num_active_io_operations;
        }
        if (oper_slot.write_oper) {
            m_completed_operations.push_back(move(oper_slot.write_oper));
            --m_num_active_io_operations;
        }
    }

private:
    typedef struct pollfd pollfd;

    struct io_oper_slot {
        std::unique_ptr<async_oper> read_oper, write_oper;
    };

    int m_wakeup_pipe_read_fd, m_wakeup_pipe_write_fd;

    oper_queue m_completed_operations; // Completed, canceled, and post operations
    std::vector<pollfd> m_pollfd_slots;
    std::vector<io_oper_slot> m_io_operations;
    size_t m_num_active_io_operations = 0;

    Mutex m_mutex;
    oper_queue m_post_operations; // Protected by `m_mutex` (including the enqueued operations).
    bool m_stopped = false; // Protected by `m_mutex`

    void wait_for_io()
    {
        size_t num_ready_descriptors = 0;
        {
            // std::vector guarantees contiguous storage
            pollfd* fds = &m_pollfd_slots.front();
            nfds_t nfds = m_pollfd_slots.size();
            for (;;) {
                int timeout = -1; // Wait indefinitely
                int ret = ::poll(fds, nfds, timeout);
                if (ret != -1) {
                    REALM_ASSERT(ret >= 1);
                    num_ready_descriptors = ret;
                    break;
                }
                if (REALM_UNLIKELY(errno != EINTR)) {
                    std::error_code ec = make_basic_system_error_code(errno);
                    throw std::system_error(ec);
                }
                // Retry on interruption by system signal
            }
        }

        // Check wake-up descriptor
        if ((m_pollfd_slots[0].revents & (POLLRDNORM|POLLERR|POLLHUP)) != 0) {
            clear_wake_up_pipe();
            return;
        }

        REALM_ASSERT(m_pollfd_slots[0].revents == 0);
        size_t n = m_io_operations.size();
        REALM_ASSERT(n == m_pollfd_slots.size() - 1);
        for (size_t fd = 0; fd < n; ++fd) {
            pollfd& pollfd_slot = m_pollfd_slots[fd+1];
            if (REALM_LIKELY(pollfd_slot.revents == 0))
                continue;

            REALM_ASSERT(pollfd_slot.fd >= 0);
            REALM_ASSERT((pollfd_slot.revents & POLLNVAL) == 0);

            if ((pollfd_slot.revents & (POLLHUP|POLLERR)) != 0) {
                REALM_ASSERT((pollfd_slot.events & (POLLRDNORM|POLLWRNORM)) != 0);
                if ((pollfd_slot.events & POLLRDNORM) != 0)
                    pollfd_slot.revents |= POLLRDNORM;
                if ((pollfd_slot.events & POLLWRNORM) != 0)
                    pollfd_slot.revents |= POLLWRNORM;
            }

            io_oper_slot& oper_slot = m_io_operations[fd];

            // Check read readiness
            if ((pollfd_slot.revents & POLLRDNORM) != 0) {
                oper_slot.read_oper->proceed();
                if (oper_slot.read_oper->complete) {
                    pollfd_slot.events &= ~POLLRDNORM;
                    if (pollfd_slot.events == 0)
                        pollfd_slot.fd = -1;
                    m_completed_operations.push_back(move(oper_slot.read_oper));
                    --m_num_active_io_operations;
                }
            }

            // Check write readiness
            if ((pollfd_slot.revents & POLLWRNORM) != 0) {
                oper_slot.write_oper->proceed();
                if (oper_slot.write_oper->complete) {
                    pollfd_slot.events &= ~POLLWRNORM;
                    if (pollfd_slot.events == 0)
                        pollfd_slot.fd = -1;
                    m_completed_operations.push_back(move(oper_slot.write_oper));
                    --m_num_active_io_operations;
                }
            }

            if (--num_ready_descriptors == 0)
                break;
        }

        REALM_ASSERT(num_ready_descriptors == 0);
    }

    void wake_up_poll_thread() noexcept
    {
        char c = 0;
        ssize_t ret = ::write(m_wakeup_pipe_write_fd, &c, 1); // Nonblocking mode
        // EAGAIN/EWOULDBLOCK can be ignored in this case, as it would imply
        // that a previous "signal" is already pending.
        if (REALM_UNLIKELY(ret == -1 && errno != EAGAIN && errno != EWOULDBLOCK))
            REALM_TERMINATE("Failed to write to wakeup pipe");
    }

    void clear_wake_up_pipe() noexcept
    {
        char buffer[64];
        for (;;) {
            ssize_t ret = ::read(m_wakeup_pipe_read_fd, buffer, sizeof buffer); // Nonblocking mode
            if (ret == -1) {
                if (REALM_UNLIKELY(errno != EAGAIN && errno != EWOULDBLOCK))
                    REALM_TERMINATE("Failed to read from wakeup pipe");
                break;
            }
            REALM_ASSERT_RELEASE(ret > 0);
            // Read as much as we can without blocking
        }
    }
};


io_service::io_service():
    m_impl(new impl) // Throws
{
}

io_service::~io_service() noexcept
{
}

void io_service::run()
{
    m_impl->run(); // Throws
}

void io_service::stop() noexcept
{
    m_impl->stop();
}

void io_service::reset() noexcept
{
    m_impl->reset();
}

void io_service::add_io_oper(int fd, std::unique_ptr<async_oper> op, io_op type)
{
    m_impl->add_io_oper(fd, move(op), type); // Throws
}

void io_service::add_completed_oper(std::unique_ptr<async_oper> op) noexcept
{
    m_impl->add_completed_oper(move(op));
}

void io_service::add_post_oper(std::unique_ptr<async_oper> op) noexcept
{
    m_impl->add_post_oper(move(op));
}


std::error_code resolver::resolve(const query& query, endpoint::list& list, std::error_code& ec)
{
    typedef struct addrinfo addrinfo_type;
    addrinfo_type hints = addrinfo_type(); // Clear
    hints.ai_flags    = query.m_flags;
    hints.ai_family   = query.m_protocol.m_family;
    hints.ai_socktype = query.m_protocol.m_socktype;
    hints.ai_protocol = query.m_protocol.m_protocol;

    const char* host = query.m_host.empty() ? 0 : query.m_host.c_str();
    const char* service = query.m_service.empty() ? 0 : query.m_service.c_str();
    struct addrinfo* first = nullptr;
    int ret = ::getaddrinfo(host, service, &hints, &first);
    if (REALM_UNLIKELY(ret != 0)) {
#ifdef EAI_SYSTEM
        if (ret == EAI_SYSTEM) {
            ec = make_basic_system_error_code(errno);
            return ec;
        }
#endif
        ec = translate_addrinfo_error(ret);
        return ec;
    }

    getaddrinfo_result_owner gro(first);

    // Count number of IPv4/IPv6 endpoints
    size_t num_endpoints = 0;
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

    // Copy the IPv4/IPv6 endpoints
    list.m_endpoints.set_size(num_endpoints); // Throws
    struct addrinfo* curr = first;
    size_t endpoint_ndx = 0;
    while (curr) {
        bool ip_v4 = curr->ai_family == AF_INET;
        bool ip_v6 = curr->ai_family == AF_INET6;
        if (ip_v4 || ip_v6) {
            REALM_ASSERT((ip_v4 && curr->ai_addrlen == sizeof (endpoint::sockaddr_ip_v4_type)) ||
                           (ip_v6 && curr->ai_addrlen == sizeof (endpoint::sockaddr_ip_v6_type)));
            endpoint& ep = list.m_endpoints[endpoint_ndx];
            ep.m_protocol.m_family   = curr->ai_family;
            ep.m_protocol.m_socktype = curr->ai_socktype;
            ep.m_protocol.m_protocol = curr->ai_protocol;
            if (ip_v4) {
                ep.m_sockaddr_union.m_ip_v4 =
                    reinterpret_cast<endpoint::sockaddr_ip_v4_type&>(*curr->ai_addr);
            }
            else {
                ep.m_sockaddr_union.m_ip_v6 =
                    reinterpret_cast<endpoint::sockaddr_ip_v6_type&>(*curr->ai_addr);
            }
            ++endpoint_ndx;
        }
        curr = curr->ai_next;
    }

    ec = std::error_code(); // Success
    return ec;
}


void socket_base::cancel() noexcept
{
    bool any_incomplete = false;
    if (m_read_oper) {
        if (!m_read_oper->complete)
            any_incomplete = true;
        m_read_oper->canceled = true;
        m_read_oper = 0;
    }
    if (m_write_oper) {
        if (!m_write_oper->complete)
            any_incomplete = true;
        m_write_oper->canceled = true;
        m_write_oper = 0;
    }
    if (any_incomplete)
        m_service.m_impl->cancel_incomplete_io_ops(m_sock_fd);
}


std::error_code socket_base::bind(const endpoint& ep, std::error_code& ec)
{
    if (!is_open()) {
        if (REALM_UNLIKELY(open(ep.protocol(), ec)))
            return ec;
    }

    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::bind(m_sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = std::error_code(); // Success
    }
    return ec;
}


endpoint socket_base::local_endpoint(std::error_code& ec) const
{
    endpoint ep;
    union union_type {
        endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof (endpoint::sockaddr_union_type) + 1];
    };
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    int ret = ::getsockname(m_sock_fd, addr, &addr_len);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ep;
    }
    socklen_t expected_addr_len = m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    if (addr_len != expected_addr_len)
        throw std::runtime_error("Unexpected local address length");
    ep.m_protocol = m_protocol;
    ep.m_sockaddr_union = buffer.m_sockaddr_union;
    ec = std::error_code(); // Success
    return ep;
}


void socket_base::do_open(const protocol& prot, std::error_code& ec)
{
    if (REALM_UNLIKELY(is_open()))
        throw std::runtime_error("Socket is already open");
    int sock_fd = ::socket(prot.m_family, prot.m_socktype, prot.m_protocol);
    if (REALM_UNLIKELY(sock_fd == -1)) {
        ec = make_basic_system_error_code(errno);
        return;
    }

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    {
        int optval = 1;
        int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
        if (REALM_UNLIKELY(ret == -1)) {
            ec = make_basic_system_error_code(errno);
            ::close(sock_fd);
            return;
        }
    }
#endif

    m_protocol = prot;
    m_sock_fd = sock_fd;
    ec = std::error_code(); // Success
}


void socket_base::do_close() noexcept
{
    int ret = ::close(m_sock_fd);
    // We can accept various errors from close(), but they must be ignored as
    // the file descriptor is closed in any case (not necessarily accorinf to
    // POSIX, but we shall assume it anyway). `EBADF`, however, would indicate
    // an implementation bug, so we don't want to ignore that.
    REALM_ASSERT(ret != -1 || errno != EBADF);
    static_cast<void>(ret);
    m_sock_fd = -1;
}


void socket_base::get_option(opt_enum opt, void* value_data, size_t& value_size,
                             std::error_code& ec) const
{
    int level = 0;
    int option_name = 0;
    map_option(opt, level, option_name);

    socklen_t option_len = socklen_t(value_size);
    int ret = ::getsockopt(m_sock_fd, level, option_name, value_data, &option_len);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return;
    }
    value_size = size_t(option_len);
    ec = std::error_code(); // Success
}


void socket_base::set_option(opt_enum opt, const void* value_data, size_t value_size,
                             std::error_code& ec)
{
    int level = 0;
    int option_name = 0;
    map_option(opt, level, option_name);

    int ret = ::setsockopt(m_sock_fd, level, option_name, value_data, value_size);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return;
    }
    ec = std::error_code(); // Success
}


void socket_base::map_option(opt_enum opt, int& level, int& option_name) const
{
    switch (opt) {
        case opt_ReuseAddr:
            level       = SOL_SOCKET;
            option_name = SO_REUSEADDR;
            return;
    }
    REALM_ASSERT(false);
}


std::error_code socket::connect(const endpoint& ep, std::error_code& ec)
{
    REALM_ASSERT(!m_write_oper);

    if (initiate_connect(ep, ec))
        return ec; // Failure, or immediate completion

    // Wait for file descriptor to become writable
    {
        using pollfd = struct pollfd;
        pollfd slot = pollfd(); // Cleared slot
        slot.fd = get_sock_fd();
        slot.events = POLLWRNORM;
        nfds_t nfds = 1;
        int timeout = -1; // Wait indefinitely
        for (;;) {
            int ret = ::poll(&slot, nfds, timeout);
            if (ret >= 0) {
                REALM_ASSERT(ret == 1);
                break;
            }
            if (REALM_UNLIKELY(errno != EINTR)) {
                ec = make_basic_system_error_code(errno);
                return ec;
            }
            // Retry on interruption by system signal
        }
    }

    // Retry connect. This time, if it succeeds, it must succeed immediately.
    if (finalize_connect(ep, ec))
        return ec; // Failure

    ec = std::error_code(); // Success
    return ec;
}


std::error_code socket::write(const char* data, size_t size, std::error_code& ec) noexcept
{
    REALM_ASSERT(!m_write_oper);
    const char* begin = data;
    const char* end = data + size;
    while (begin != end) {
        size_t n = write_some(begin, end-begin, ec);
        if (REALM_UNLIKELY(ec))
            return ec;
        REALM_ASSERT(n > 0);
        REALM_ASSERT(n <= size_t(end-begin));
        begin += n;
    }
    ec = std::error_code(); // Success
    return ec;
}


size_t socket::read_some(char* buffer, size_t size, std::error_code& ec) noexcept
{
    int flags = 0;
    for (;;) {
        ssize_t ret = ::recv(get_sock_fd(), buffer, size, flags);
        if (ret != -1) {
            if (ret == 0) {
                ec = end_of_input;
            }
            else {
                ec = std::error_code(); // Success
            }
            return size_t(ret);
        }
        if (REALM_UNLIKELY(errno != EINTR)) {
            ec = make_basic_system_error_code(errno);
            return 0;
        }
        // Retry on interruption by system signal
    }
}


size_t socket::write_some(const char* data, size_t size, std::error_code& ec) noexcept
{
    int flags = 0;
#ifdef __linux__
    // Prevent SIGPIPE when remote peer has closed the connection.
    flags |= MSG_NOSIGNAL;
#endif
    for (;;) {
        ssize_t ret = ::send(get_sock_fd(), data, size, flags);
        if (ret != -1) {
            ec = std::error_code(); // Success
            return size_t(ret);
        }
        if (REALM_UNLIKELY(errno != EINTR)) {
            ec = make_basic_system_error_code(errno);
            return 0;
        }
        // Retry on interruption by system signal
    }
}


void socket::do_open(const protocol& prot, std::error_code& ec)
{
    socket_base::do_open(prot, ec); // Throws
    if (ec)
        return;
    if (set_nonblocking(get_sock_fd(), true, ec)) {
        do_close();
        return;
    }
}


bool socket::initiate_connect(const endpoint& ep, std::error_code& ec) noexcept
{
    if (!is_open()) {
        if (REALM_UNLIKELY(open(ep.protocol(), ec)))
            return true; // Failure
    }

    // Initiate connect operation.
    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::connect(get_sock_fd(), &ep.m_sockaddr_union.m_base, addr_len);
    if (ret != -1) {
        ec = std::error_code(); // Success
        return true; // Immediate completion.
    }

    // EINPROGRESS indicates that the underlying connect operation was
    // successfully initiated, but not immediately completd, and EALREADY
    // indicates that an underlying connect operation was already initiated, and
    // still not completed, presumably because a previous call to connect() or
    // async_connect() failed, or was canceled.
    if (REALM_UNLIKELY(errno != EINPROGRESS && errno != EALREADY)) {
        ec = make_basic_system_error_code(errno);
        return true; // Failure
    }

    ec = std::error_code(); // Success
    return false; // Successful initiation, but no immediate completion.
}


std::error_code socket::finalize_connect(const endpoint& ep, std::error_code& ec) noexcept
{
    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::connect(get_sock_fd(), &ep.m_sockaddr_union.m_base, addr_len);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }

    // Disable nonblocking mode
    if (REALM_UNLIKELY(set_nonblocking(get_sock_fd(), false, ec)))
        return ec;

    ec = std::error_code(); // Success
    return ec;
}


std::error_code acceptor::listen(int backlog, std::error_code& ec)
{
    int ret = ::listen(get_sock_fd(), backlog);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = std::error_code(); // Success
    }
    return ec;
}


std::error_code acceptor::do_accept(socket& sock, endpoint* ep, std::error_code& ec) noexcept
{
    REALM_ASSERT(is_open());

    union union_type {
        endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof (endpoint::sockaddr_union_type) + 1];
    };
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    int sock_fd;
    for (;;) {
        sock_fd = ::accept(get_sock_fd(), addr, &addr_len);
        if (sock_fd != -1)
            break;
        if (REALM_UNLIKELY(errno != EINTR)) {
            ec = make_basic_system_error_code(errno);
            return ec;
        }
        // Retry on interruption by system signal
    }
    socklen_t expected_addr_len = m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    if (REALM_UNLIKELY(addr_len != expected_addr_len))
        REALM_TERMINATE("Unexpected peer address length");

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    int optval = 1;
    int ret = ::setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        ::close(sock_fd);
        return ec;
    }
#endif

    sock.m_sock_fd = sock_fd;
    if (ep) {
        ep->m_protocol = m_protocol;
        ep->m_sockaddr_union = buffer.m_sockaddr_union;
    }
    ec = std::error_code(); // Success
    return ec;
}


size_t buffered_input_stream::do_read(char* buffer, size_t size, int delim,
                                      std::error_code& ec) noexcept
{
    REALM_ASSERT(!m_socket.m_read_oper);
    char* out_begin = buffer;
    char* out_end = buffer + size;
    for (;;) {
        size_t in_avail = m_end - m_begin;
        size_t out_avail = out_end - out_begin;
        size_t n = std::min(in_avail, out_avail);
        bool delim_mode = delim != std::char_traits<char>::eof();
        char* i = !delim_mode ? m_begin + n :
            std::find(m_begin, m_begin + n, std::char_traits<char>::to_char_type(delim));
        out_begin = std::copy(m_begin, i, out_begin);
        m_begin = i;
        if (out_begin == out_end) {
            if (delim_mode) {
                ec = network::delim_not_found;
                return out_begin - buffer;
            }
            break;
        }
        if (m_begin != m_end) {
            REALM_ASSERT(delim_mode);
            *out_begin++ = *m_begin++; // Transfer delimiter
            break;
        }
        size_t m = m_socket.read_some(m_buffer.get(), s_buffer_size, ec);
        if (REALM_UNLIKELY(ec))
            return out_begin - buffer;
        REALM_ASSERT(m > 0);
        REALM_ASSERT(m <= s_buffer_size);
        m_begin = m_buffer.get();
        m_end = m_begin + m;
    }
    ec = std::error_code(); // Success
    return out_begin - buffer;
}


void buffered_input_stream::read_oper_base::process_buffered_input() noexcept
{
    REALM_ASSERT(!complete);
    REALM_ASSERT(!canceled);
    size_t in_avail = m_stream.m_end - m_stream.m_begin;
    size_t out_avail = m_out_end - m_out_curr;
    size_t n = std::min(in_avail, out_avail);
    bool delim_mode = m_delim != std::char_traits<char>::eof();
    char* i = !delim_mode ? m_stream.m_begin + n :
        std::find(m_stream.m_begin, m_stream.m_begin + n,
                  std::char_traits<char>::to_char_type(m_delim));
    m_out_curr = std::copy(m_stream.m_begin, i, m_out_curr);
    m_stream.m_begin = i;
    if (m_out_curr == m_out_end) {
        if (delim_mode)
            m_error_code = network::delim_not_found;
    }
    else {
        if (m_stream.m_begin == m_stream.m_end)
            return;
        REALM_ASSERT(delim_mode);
        *m_out_curr++ = *m_stream.m_begin++; // Transfer delimiter
    }
    complete = true;
}


void buffered_input_stream::read_oper_base::proceed() noexcept
{
    REALM_ASSERT(!complete);
    REALM_ASSERT(!canceled);
    REALM_ASSERT(!m_error_code);
    REALM_ASSERT(m_stream.m_begin == m_stream.m_end);
    REALM_ASSERT(m_out_curr < m_out_end);
    size_t n = m_stream.m_socket.read_some(m_stream.m_buffer.get(), s_buffer_size, m_error_code);
    if (REALM_UNLIKELY(m_error_code)) {
        complete = true;
        return;
    }
    REALM_ASSERT(n > 0);
    REALM_ASSERT(n <= s_buffer_size);
    m_stream.m_begin = m_stream.m_buffer.get();
    m_stream.m_end = m_stream.m_begin + n;
    process_buffered_input();
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
    const size_t large_heap_buffer_size = 4096;
    std::unique_ptr<char[]> large_heap_buffer(new char[large_heap_buffer_size]); // Throws
    ret = ::gethostname(large_heap_buffer.get(), large_heap_buffer_size);
    if (REALM_LIKELY(ret != -1)) {
        // Check that a null-termination was included
        char* end = large_heap_buffer.get() + large_heap_buffer_size;
        char* i = std::find(large_heap_buffer.get(), end, 0);
        if (i != end)
            return std::string(large_heap_buffer.get(), i);
    }
    throw std::runtime_error("gethostname() failed");
}


std::error_code make_error_code(errors err)
{
    return std::error_code(err, g_network_error_category);
}

} // namespace network
} // namespace util
} // namespace realm
