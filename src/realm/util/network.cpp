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

void make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if(flags == -1) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
    flags |= O_NONBLOCK;
    int ret = fcntl(fd, F_SETFL, flags);
    if(ret == -1) {
        std::error_code ec = make_basic_system_error_code(errno);
        throw std::system_error(ec);
    }
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


class io_service::impl {
public:
    impl():
        m_num_poll_handlers(0),
        m_stopped(false)
    {
        int fildes[2];
        int ret = pipe(fildes);
        if (ret == -1) {
            std::error_code ec = make_basic_system_error_code(errno);
            throw std::system_error(ec);
        }
        try {
            make_nonblocking(fildes[0]); // Throws
            make_nonblocking(fildes[1]); // Throws
            m_wakeup_pipe_read_fd  = fildes[0];
            m_wakeup_pipe_write_fd = fildes[1];
            pollfd slot = pollfd(); // Cleared slot
            slot.fd = m_wakeup_pipe_read_fd;
            slot.events = POLLRDNORM;
            m_pollfd_slots.push_back(slot); // Throws
        }
        catch (...) {
            close(fildes[0]);
            close(fildes[1]);
            throw;
        }
    }

    ~impl()
    {
        close(m_wakeup_pipe_read_fd);
        close(m_wakeup_pipe_write_fd);
        {
            size_t n = m_post_handlers.size();
            for (size_t i = 0; i < n; ++i)
                delete m_post_handlers[i];
        }
        {
            size_t n = m_imm_handlers.size();
            for (size_t i = 0; i < n; ++i)
                delete m_imm_handlers[i];
        }
        {
            size_t n = m_cancel_handlers.size();
            for (size_t i = 0; i < n; ++i)
                delete m_cancel_handlers[i];
        }
        {
            size_t n = m_poll_handlers.size();
            for (size_t i = 0; i < n; ++i) {
                delete m_poll_handlers[i].read_handler;
                delete m_poll_handlers[i].write_handler;
            }
        }
#if REALM_ASSERTIONS_ENABLED
        size_t num_poll_handlers = 0;
        for (size_t i = 0; i < m_poll_handlers.size(); ++i) {
            if (m_poll_handlers[i].read_handler)
                ++num_poll_handlers;
            if (m_poll_handlers[i].write_handler)
                ++num_poll_handlers;
        }
        REALM_ASSERT(num_poll_handlers == m_num_poll_handlers);
#endif
    }

    void run()
    {
      restart:
        clear_wake_up_pipe();
        for (;;) {
            {
                LockGuard l(m_mutex);
                if (m_stopped)
                    break;
                if (m_imm_handlers.empty()) {
                    if (m_post_handlers.empty()) {
                        if (m_num_poll_handlers == 0 && m_cancel_handlers.empty())
                            break; // Out of work
                    }
                    else {
                        swap(m_post_handlers, m_imm_handlers);
                    }
                }
            }

            while (!m_imm_handlers.empty()) {
                std::unique_ptr<async_handler> h(m_imm_handlers.back());
                m_imm_handlers.pop_back();
                h->exec(); // Throws
            }

            while (!m_cancel_handlers.empty()) {
                std::unique_ptr<async_handler> h(m_cancel_handlers.back());
                m_cancel_handlers.pop_back();
                h->cancel(); // Throws
            }

            if (m_num_poll_handlers == 0)
                continue;

            size_t num_ready_descriptors = 0;
            {
                pollfd* fds = &m_pollfd_slots.front(); // std::vector guarantees contiguous storage
                nfds_t nfds = m_pollfd_slots.size();
                for (;;) {
                    int timeout = -1;
                    int ret = poll(fds, nfds, timeout);
                    if (ret >= 0) {
                        num_ready_descriptors = ret;
                        break;
                    }
                    // Ignore interruptions due to system signals
                    if (errno != EINTR) {
                        std::error_code ec = make_basic_system_error_code(errno);
                        throw std::system_error(ec);
                    }
                }
            }
            REALM_ASSERT(num_ready_descriptors >= 1);
            // Check wake-up descriptor
            if ((m_pollfd_slots[0].revents & (POLLRDNORM|POLLERR|POLLHUP)) != 0)
                goto restart;
            REALM_ASSERT(m_pollfd_slots[0].revents == 0);
            size_t n = m_poll_handlers.size();
            REALM_ASSERT(n == m_pollfd_slots.size() - 1);
            for (size_t fd = 0; fd < n; ++fd) {
                pollfd* pollfd_slot = &m_pollfd_slots[fd+1];
                if (REALM_LIKELY(pollfd_slot->revents == 0))
                    continue;

                REALM_ASSERT(pollfd_slot->fd >= 0);
                REALM_ASSERT((pollfd_slot->revents & POLLNVAL) == 0);

                if ((pollfd_slot->revents & (POLLHUP|POLLERR)) != 0) {
                    REALM_ASSERT((pollfd_slot->events & (POLLRDNORM|POLLWRNORM)) != 0);
                    if ((pollfd_slot->events & POLLRDNORM) != 0)
                        pollfd_slot->revents |= POLLRDNORM;
                    if ((pollfd_slot->events & POLLWRNORM) != 0)
                        pollfd_slot->revents |= POLLWRNORM;
                }

                // Check read readiness
                if ((pollfd_slot->revents & POLLRDNORM) != 0) {
                    pollfd_slot->events &= ~POLLRDNORM;
                    if (pollfd_slot->events == 0)
                        pollfd_slot->fd = -1;
                    std::unique_ptr<async_handler> handler;
                    poll_handler_slot& handler_slot = m_poll_handlers[fd];
                    handler.reset(handler_slot.read_handler);
                    handler_slot.read_handler = 0;
                    --m_num_poll_handlers;
                    bool done = handler->exec(); // Throws
                    if (done) {
                        // A user handler has been executed, which may have
                        // closed the current socket, or initiated new
                        // asynchronous operations. The latter may have caused
                        // the m_pollfd_slots vector to reallocate its
                        // underlying memory.
                        pollfd_slot = &m_pollfd_slots[fd+1];
                        if (pollfd_slot->fd < 0)
                            continue;
                    }
                    else {
                        // Users handler is not executed in this case
                        pollfd_slot->fd = fd;
                        pollfd_slot->events |= POLLRDNORM;
                        handler_slot.read_handler = handler.release();
                        ++m_num_poll_handlers;
                    }
                }


                // Check write readiness
                if ((pollfd_slot->revents & POLLWRNORM) != 0) {
                    pollfd_slot->events &= ~POLLWRNORM;
                    if (pollfd_slot->events == 0)
                        pollfd_slot->fd = -1;
                    std::unique_ptr<async_handler> handler;
                    poll_handler_slot& handler_slot = m_poll_handlers[fd];
                    handler.reset(handler_slot.write_handler);
                    handler_slot.write_handler = 0;
                    --m_num_poll_handlers;
                    bool done = handler->exec(); // Throws
                    if (!done) {
                        // Users handler is not executed in this case
                        pollfd_slot->fd = fd;
                        pollfd_slot->events |= POLLWRNORM;
                        handler_slot.write_handler = handler.release();
                        ++m_num_poll_handlers;
                    }
                }
            }
        }
    }

    void stop()
    {
        {
            LockGuard l(m_mutex);
            if (m_stopped)
                return;
            m_stopped = true;
        }

        wake_up_poll_thread(); // Throws
    }

    void reset()
    {
        LockGuard l(m_mutex);
        m_stopped = false;
   }

    void add_io_handler(int fd, async_handler* handler, io_op op)
    {
        REALM_ASSERT(fd >= 0);
        std::unique_ptr<async_handler> h(handler);

        size_t n = m_poll_handlers.size();
        REALM_ASSERT(n == m_pollfd_slots.size() - 1);
        size_t n_2 = fd + 1;
        if (n_2 > n) {
            poll_handler_slot handler_slot = poll_handler_slot(); // Cleared slot
            pollfd pollfd_slot = pollfd(); // Cleared slot
            pollfd_slot.fd = -1; // Unused
            m_pollfd_slots.reserve(n_2+1); // Throws
            m_poll_handlers.resize(n_2, handler_slot); // Throws
            m_pollfd_slots.resize(n_2+1, pollfd_slot);
        }

        pollfd&            pollfd_slot  = m_pollfd_slots[fd+1];
        poll_handler_slot& handler_slot = m_poll_handlers[fd];
        REALM_ASSERT(pollfd_slot.fd == -1 || pollfd_slot.fd == fd);
        REALM_ASSERT((pollfd_slot.fd == -1) == (pollfd_slot.events == 0));
        REALM_ASSERT(((pollfd_slot.events & POLLRDNORM) != 0) ==
                       (handler_slot.read_handler != 0));
        REALM_ASSERT(((pollfd_slot.events & POLLWRNORM) != 0) ==
                       (handler_slot.write_handler != 0));
        REALM_ASSERT((pollfd_slot.events & ~(POLLRDNORM|POLLWRNORM)) == 0);
        switch (op) {
            case op_Read:
                REALM_ASSERT(!handler_slot.read_handler);
                pollfd_slot.events |= POLLRDNORM;
                handler_slot.read_handler = handler;
                goto finish;
            case op_Write:
                REALM_ASSERT(!handler_slot.write_handler);
                pollfd_slot.events |= POLLWRNORM;
                handler_slot.write_handler = handler;
                goto finish;
        }
        REALM_ASSERT(false);
        return;

      finish:
        pollfd_slot.fd = fd;
        h.release();
        ++m_num_poll_handlers;
    }

    void add_imm_handler(async_handler* handler)
    {
        std::unique_ptr<async_handler> h(handler);
        m_imm_handlers.push_back(handler); // Throws
        h.release();
    }

    void add_post_handler(async_handler* handler)
    {
        std::unique_ptr<async_handler> h(handler);
        {
            LockGuard l(m_mutex);
            m_post_handlers.push_back(handler); // Throws
        }
        h.release();
        wake_up_poll_thread(); // Throws
    }

    void cancel_io_ops(int fd)
    {
        REALM_ASSERT(fd >= 0);
        if (unsigned(fd) < m_poll_handlers.size()) {
            REALM_ASSERT(m_poll_handlers.size() == m_pollfd_slots.size() - 1);
            pollfd&            pollfd_slot  = m_pollfd_slots[fd+1];
            poll_handler_slot& handler_slot = m_poll_handlers[fd];
            if (pollfd_slot.fd >= 0) {
                m_cancel_handlers.reserve(m_cancel_handlers.size() + 2); // Throws
                pollfd_slot.fd = -1; // Mark unused
                pollfd_slot.events = 0;
                if (handler_slot.read_handler) {
                    m_cancel_handlers.push_back(handler_slot.read_handler);
                    handler_slot.read_handler = 0;
                    --m_num_poll_handlers;
                }
                if (handler_slot.write_handler) {
                    m_cancel_handlers.push_back(handler_slot.write_handler);
                    handler_slot.write_handler = 0;
                    --m_num_poll_handlers;
                }
            }
        }
    }

private:
    typedef struct pollfd pollfd;

    struct poll_handler_slot {
        async_handler* read_handler;
        async_handler* write_handler;
    };

    int m_wakeup_pipe_read_fd;
    int m_wakeup_pipe_write_fd;

    std::vector<async_handler*> m_imm_handlers;
    std::vector<async_handler*> m_cancel_handlers;
    std::vector<pollfd> m_pollfd_slots;
    std::vector<poll_handler_slot> m_poll_handlers;
    size_t m_num_poll_handlers;

    Mutex m_mutex;
    std::vector<async_handler*> m_post_handlers; // Protected by `m_mutex`
    bool m_stopped; // Protected by `m_mutex`

    void wake_up_poll_thread()
    {
        char c = 0;
        ssize_t ret = ::write(m_wakeup_pipe_write_fd, &c, 1);
        // EAGAIN can be ignored in this case, as it would imply that a previous
        // "signal" is already pending.
        if (ret == -1 && errno != EAGAIN) {
            std::error_code ec = make_basic_system_error_code(errno);
            throw std::system_error(ec);
        }
    }

    void clear_wake_up_pipe()
    {
        char buffer[64];
        for (;;) {
            ssize_t ret = ::read(m_wakeup_pipe_read_fd, buffer, sizeof buffer);
            // EAGAIN can be ignored in this case, as it would imply that a previous
            // "signal" is already pending.
            if (ret < 0) {
                if (errno == EAGAIN)
                    break;
                std::error_code ec = make_basic_system_error_code(errno);
                throw std::system_error(ec);
            }
            REALM_ASSERT_RELEASE(ret > 0);
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
    m_impl->run();
}

void io_service::stop()
{
    m_impl->stop();
}

void io_service::reset()
{
    m_impl->reset();
}

void io_service::add_io_handler(int fd, async_handler* handler, io_op op)
{
    m_impl->add_io_handler(fd, handler, op);
}

void io_service::add_imm_handler(async_handler* handler)
{
    m_impl->add_imm_handler(handler);
}

void io_service::add_post_handler(async_handler* handler)
{
    m_impl->add_post_handler(handler);
}

void io_service::cancel_io_ops(int fd)
{
    m_impl->cancel_io_ops(fd);
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
    int ret = getaddrinfo(host, service, &hints, &first);
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


std::error_code socket_base::open(const protocol& prot, std::error_code& ec)
{
    if (REALM_UNLIKELY(is_open()))
        throw std::runtime_error("Socket is already open");
    int sock_fd = ::socket(prot.m_family, prot.m_socktype, prot.m_protocol);
    if (REALM_UNLIKELY(sock_fd == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    int optval = 1;
    int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        ::close(sock_fd);
        return ec;
    }
#endif

    m_protocol = prot;
    m_sock_fd = sock_fd;
    ec = std::error_code(); // Success
    return ec;
}


std::error_code socket_base::close(std::error_code& ec)
{
    if (is_open()) {
        int ret = ::close(m_sock_fd);
        if (REALM_UNLIKELY(ret == -1)) {
            ec = make_basic_system_error_code(errno);
            return ec;
        }
        m_service.cancel_io_ops(m_sock_fd);
        m_sock_fd = -1;
    }
    ec = std::error_code(); // Success
    return ec;
}


std::error_code socket_base::bind(const endpoint& ep, std::error_code& ec)
{
    if (!is_open()) {
        open(ep.protocol(), ec);
        if (REALM_UNLIKELY(ec))
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
    int ret = getsockname(m_sock_fd, addr, &addr_len);
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


void socket_base::get_option(opt_enum opt, void* value_data, size_t& value_size,
                             std::error_code& ec) const
{
    int level = 0;
    int option_name = 0;
    map_option(opt, level, option_name);

    socklen_t option_len = socklen_t(value_size);
    int ret = getsockopt(m_sock_fd, level, option_name, value_data, &option_len);
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

    int ret = setsockopt(m_sock_fd, level, option_name, value_data, value_size);
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
    if (!is_open()) {
        open(ep.protocol(), ec);
        if (REALM_UNLIKELY(ec))
            return ec;
    }

    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::connect(m_sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = std::error_code(); // Success
    }
    return ec;
}


size_t socket::read_some(char* buffer, size_t size, std::error_code& ec) noexcept
{
    int flags = 0;
    ssize_t ret = recv(m_sock_fd, buffer, size, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return 0;
    }
    if (ret == 0) {
        ec = end_of_input;
        return 0;
    }
    ec = std::error_code(); // Success
    return size_t(ret);
}


size_t socket::write_some(const char* data, size_t size, std::error_code& ec) noexcept
{
    int flags = 0;
#ifdef __linux__
    flags |= MSG_NOSIGNAL;
#endif
    ssize_t ret = send(m_sock_fd, data, size, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
        return 0;
    }
    ec = std::error_code(); // Success
    return size_t(ret);
}


std::error_code acceptor::listen(int backlog, std::error_code& ec)
{
    int ret = ::listen(m_sock_fd, backlog);
    if (REALM_UNLIKELY(ret == -1)) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = std::error_code(); // Success
    }
    return ec;
}


std::error_code acceptor::accept(socket& sock, endpoint* ep, std::error_code& ec)
{
    if (REALM_UNLIKELY(sock.is_open()))
        throw std::runtime_error("Socket is already open");
    union union_type {
        endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof (endpoint::sockaddr_union_type) + 1];
    };
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    int sock_fd = ::accept(m_sock_fd, addr, &addr_len);
    if (REALM_UNLIKELY(sock_fd == -1)) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    socklen_t expected_addr_len = m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    if (REALM_UNLIKELY(addr_len != expected_addr_len)) {
        ::close(sock_fd);
        throw std::runtime_error("Unexpected peer address length");
    }

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    int optval = 1;
    int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
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


size_t buffered_input_stream::read(char* buffer, size_t size, int delim,
                                   std::error_code& ec) noexcept
{
    char* out_begin = buffer;
    char* out_end = buffer + size;
    for (;;) {
        size_t in_avail = m_end - m_begin;
        size_t out_avail = out_end - out_begin;
        size_t n = std::min(in_avail, out_avail);
        char* i = delim == std::char_traits<char>::eof() ? m_begin + n :
            std::find(m_begin, m_begin + n, std::char_traits<char>::to_char_type(delim));
        out_begin = std::copy(m_begin, i, out_begin);
        m_begin = i;
        if (out_begin == out_end)
            break;
        if (m_begin != m_end) {
            REALM_ASSERT(delim != std::char_traits<char>::eof());
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


void buffered_input_stream::read_handler_base::process_input() noexcept
{
    REALM_ASSERT(!m_complete);
    size_t in_avail = m_stream.m_end - m_stream.m_begin;
    size_t out_avail = m_out_end - m_out_curr;
    size_t n = std::min(in_avail, out_avail);
    char* i = m_delim == std::char_traits<char>::eof() ? m_stream.m_begin + n :
        std::find(m_stream.m_begin, m_stream.m_begin + n,
                  std::char_traits<char>::to_char_type(m_delim));
    m_out_curr = std::copy(m_stream.m_begin, i, m_out_curr);
    m_stream.m_begin = i;
    if (m_out_curr != m_out_end) {
        if (m_stream.m_begin == m_stream.m_end)
            return;
        REALM_ASSERT(m_delim != std::char_traits<char>::eof());
        *m_out_curr++ = *m_stream.m_begin++; // Transfer delimiter
    }
    m_complete = true;
}


void buffered_input_stream::read_handler_base::read_some(std::error_code& ec) noexcept
{
    REALM_ASSERT(!m_complete);
    size_t n = m_stream.m_socket.read_some(m_stream.m_buffer.get(), s_buffer_size, ec);
    if (REALM_UNLIKELY(ec))
        return;
    REALM_ASSERT(n > 0);
    REALM_ASSERT(n <= s_buffer_size);
    m_stream.m_begin = m_stream.m_buffer.get();
    m_stream.m_end = m_stream.m_begin + n;
    process_input();
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
    int ret = gethostname(small_stack_buffer, sizeof small_stack_buffer);
    if (ret != -1) {
        // Check that a null-termination was included
        char* end = small_stack_buffer + sizeof small_stack_buffer;
        char* i = std::find(small_stack_buffer, end, 0);
        if (i != end)
            return std::string(small_stack_buffer, i);
    }
    const size_t large_heap_buffer_size = 4096;
    std::unique_ptr<char[]> large_heap_buffer(new char[large_heap_buffer_size]); // Throws
    ret = gethostname(large_heap_buffer.get(), large_heap_buffer_size);
    if (REALM_LIKELY(ret != -1)) {
        // Check that a null-termination was included
        char* end = large_heap_buffer.get() + large_heap_buffer_size;
        char* i = std::find(large_heap_buffer.get(), end, 0);
        if (i != end)
            return std::string(large_heap_buffer.get(), i);
    }
    throw std::runtime_error("gethostname() failed");
}

std::error_code write(socket& sock, const char* data, size_t size, std::error_code& ec)
    noexcept
{
    const char* begin = data;
    const char* end = data + size;
    while (begin != end) {
        size_t n = sock.write_some(begin, end-begin, ec);
        if (REALM_UNLIKELY(ec))
            return ec;
        REALM_ASSERT(n > 0);
        REALM_ASSERT(n <= size_t(end-begin));
        begin += n;
    }
    ec = std::error_code(); // Success
    return ec;
}

std::error_code make_error_code(errors err)
{
    return std::error_code(err, g_network_error_category);
}

} // namespace network
} // namespace util
} // namespace realm
