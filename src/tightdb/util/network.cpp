#include <cerrno>
#include <algorithm>
#include <stdexcept>

#include <unistd.h>

#include <tightdb/util/network.hpp>

using std::size_t;
using std::string;
using std::runtime_error;
using namespace tightdb::util;
using namespace tightdb::util::network;


namespace {

error_code translate_addrinfo_error(int err)
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


class network_error_category: public error_category {
    string message(int) const TIGHTDB_OVERRIDE;
};

network_error_category g_network_error_category;

string network_error_category::message(int value) const
{
    switch (errors(value)) {
        case end_of_input:
            return "End of input";
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
    TIGHTDB_ASSERT(false);
    return string();
}

} // anonymous namespace


error_code resolver::resolve(const query& query, endpoint::list& list, error_code& ec)
{
    typedef struct addrinfo addrinfo_type;
    addrinfo_type hints = addrinfo_type(); // Clear
    hints.ai_flags    = query.m_flags;
    hints.ai_family   = query.m_protocol.m_family;
    hints.ai_socktype = query.m_protocol.m_socktype;
    hints.ai_protocol = query.m_protocol.m_protocol;

    const char* host = query.m_host.empty() ? 0 : query.m_host.c_str();
    const char* service = query.m_service.empty() ? 0 : query.m_service.c_str();
    struct addrinfo* first = 0;
    int ret = getaddrinfo(host, service, &hints, &first);
    if (ret != 0) {
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
            TIGHTDB_ASSERT((ip_v4 && curr->ai_addrlen == sizeof (endpoint::sockaddr_ip_v4_type)) ||
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

    ec = error_code(); // Success
    return ec;
}


error_code socket::open(const protocol& prot, error_code& ec)
{
    if (is_open())
        throw runtime_error("Socket is already open");
    int sock_fd = ::socket(prot.m_family, prot.m_socktype, prot.m_protocol);
    if (sock_fd == -1) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    int optval = 1;
    int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
        ::close(sock_fd);
        return ec;
    }
#endif

    m_protocol = prot;
    m_sock_fd = sock_fd;
    ec = error_code(); // Success
    return ec;
}


error_code socket::bind(const endpoint& ep, error_code& ec)
{
    if (!is_open()) {
        open(ep.protocol(), ec);
        if (ec)
            return ec;
    }

    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::bind(m_sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = error_code(); // Success
    }
    return ec;
}


endpoint socket::local_endpoint(error_code& ec)
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
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
        return ep;
    }
    socklen_t expected_addr_len = m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    if (addr_len != expected_addr_len)
        throw runtime_error("Unexpected local address length");
    ep.m_protocol = m_protocol;
    ep.m_sockaddr_union = buffer.m_sockaddr_union;
    ec = error_code(); // Success
    return ep;
}


error_code socket::connect(const endpoint& ep, error_code& ec)
{
    if (!is_open()) {
        open(ep.protocol(), ec);
        if (ec)
            return ec;
    }

    socklen_t addr_len = ep.m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    int ret = ::connect(m_sock_fd, &ep.m_sockaddr_union.m_base, addr_len);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = error_code(); // Success
    }
    return ec;
}


size_t socket::read_some(char* buffer, size_t size, error_code& ec) TIGHTDB_NOEXCEPT
{
    int flags = 0;
    ssize_t ret = recv(m_sock_fd, buffer, size, flags);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
        return 0;
    }
    if (ret == 0) {
        ec = end_of_input;
        return 0;
    }
    ec = error_code(); // Success
    return size_t(ret);
}


size_t socket::write_some(const char* data, size_t size, error_code& ec) TIGHTDB_NOEXCEPT
{
    int flags = 0;
#ifdef __linux__
    flags |= MSG_NOSIGNAL;
#endif
    ssize_t ret = send(m_sock_fd, data, size, flags);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
        return 0;
    }
    ec = error_code(); // Success
    return size_t(ret);
}


error_code socket::close(error_code& ec)
{
    if (is_open()) {
        int ret = ::close(m_sock_fd);
        if (ret == -1) {
            ec = make_basic_system_error_code(errno);
            return ec;
        }
        m_sock_fd = -1;
    }
    ec = error_code(); // Success
    return ec;
}


error_code acceptor::listen(int backlog, error_code& ec)
{
    int ret = ::listen(m_sock_fd, backlog);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
    }
    else {
        ec = error_code(); // Success
    }
    return ec;
}


error_code acceptor::accept(socket& sock, endpoint& ep, error_code& ec)
{
    if (sock.is_open())
        throw runtime_error("Socket is already open");
    union union_type {
        endpoint::sockaddr_union_type m_sockaddr_union;
        char m_extra_byte[sizeof (endpoint::sockaddr_union_type) + 1];
    };
    union_type buffer;
    struct sockaddr* addr = &buffer.m_sockaddr_union.m_base;
    socklen_t addr_len = sizeof buffer;
    int sock_fd = ::accept(m_sock_fd, addr, &addr_len);
    if (sock_fd == -1) {
        ec = make_basic_system_error_code(errno);
        return ec;
    }
    socklen_t expected_addr_len = m_protocol.is_ip_v4() ?
        sizeof (endpoint::sockaddr_ip_v4_type) : sizeof (endpoint::sockaddr_ip_v6_type);
    if (addr_len != expected_addr_len) {
        ::close(sock_fd);
        throw runtime_error("Unexpected peer address length");
    }

#if defined(__MACH__) && defined(__APPLE__) || defined(__FreeBSD__)
    int optval = 1;
    int ret = setsockopt(sock_fd, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof optval);
    if (ret == -1) {
        ec = make_basic_system_error_code(errno);
        ::close(sock_fd);
        return ec;
    }
#endif

    sock.m_sock_fd = sock_fd;
    ep.m_protocol = m_protocol;
    ep.m_sockaddr_union = buffer.m_sockaddr_union;
    ec = error_code(); // Success
    return ec;
}


size_t buffered_input_stream::read(char* buffer, size_t size, error_code& ec) TIGHTDB_NOEXCEPT
{
    char* out_begin = buffer;
    char* out_end = buffer + size;
    for (;;) {
        size_t in_avail = m_end - m_begin;
        size_t out_avail = out_end - out_begin;
        size_t n = std::min(in_avail, out_avail);
        char* i = m_begin + n;
        out_begin = std::copy(m_begin, i, out_begin);
        m_begin = i;
        if (out_begin == out_end)
            break;
        TIGHTDB_ASSERT(m_begin == m_end);
        size_t m = m_socket.read_some(m_buffer.get(), s_buffer_size, ec);
        if (ec)
            return out_begin - buffer;
        m_begin = m_buffer.get();
        m_end = m_begin + m;
    }
    ec = error_code(); // Success
    return out_begin - buffer;
}


size_t buffered_input_stream::read_until(char* buffer, size_t size, char delim,
                                         error_code& ec) TIGHTDB_NOEXCEPT
{
    char* out_begin = buffer;
    char* out_end = buffer + size;
    for (;;) {
        size_t in_avail = m_end - m_begin;
        size_t out_avail = out_end - out_begin;
        size_t n = std::min(in_avail, out_avail);
        char* i = std::find(m_begin, m_begin + n, delim);
        out_begin = std::copy(m_begin, i, out_begin);
        m_begin = i;
        if (out_begin == out_end)
            break;
        if (m_begin != m_end) {
            *out_begin++ = *m_begin++; // Transfer delimiter
            break;
        }
        size_t m = m_socket.read_some(m_buffer.get(), s_buffer_size, ec);
        if (ec)
            return out_begin - buffer;
        TIGHTDB_ASSERT(m > 0);
        TIGHTDB_ASSERT(m <= s_buffer_size);
        m_begin = m_buffer.get();
        m_end = m_begin + m;
    }
    ec = error_code(); // Success
    return out_begin - buffer;
}


namespace tightdb {
namespace util {
namespace network {

error_code write(socket& sock, const char* data, size_t size, error_code& ec) TIGHTDB_NOEXCEPT
{
    const char* begin = data;
    const char* end = data + size;
    while (begin != end) {
        size_t n = sock.write_some(begin, end-begin, ec);
        if (ec)
            return ec;
        TIGHTDB_ASSERT(n > 0);
        TIGHTDB_ASSERT(n <= size_t(end-begin));
        begin += n;
    }
    ec = error_code(); // Success
    return ec;
}

error_code make_error_code(errors err)
{
    return error_code(err, g_network_error_category);
}

} // namespace network
} // namespace util
} // namespace tightdb
