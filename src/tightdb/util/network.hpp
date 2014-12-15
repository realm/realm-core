/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_UTIL_NETWORK_HPP
#define TIGHTDB_UTIL_NETWORK_HPP

#include <cstddef>
#include <string>
#include <ostream>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <tightdb/util/features.h>
#include <tightdb/util/assert.hpp>
#include <tightdb/util/buffer.hpp>
#include <tightdb/util/basic_system_errors.hpp>
#include <tightdb/util/system_error.hpp>

namespace tightdb {
namespace util {
namespace network {


class protocol {
public:
    static protocol ip_v4();
    static protocol ip_v6();

    bool is_ip_v4() const;
    bool is_ip_v6() const;

    protocol();
    ~protocol() TIGHTDB_NOEXCEPT {}

private:
    int m_family;
    int m_socktype;
    int m_protocol;

    friend class resolver;
    friend class socket;
};


class address {
public:
    bool is_ip_v4() const;
    bool is_ip_v6() const;

    template<class C, class T>
    friend std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const address&);

    address();
    ~address() TIGHTDB_NOEXCEPT {}

private:
    typedef in_addr  ip_v4_type;
    typedef in6_addr ip_v6_type;
    union union_type {
        ip_v4_type m_ip_v4;
        ip_v6_type m_ip_v6;
    };
    union_type m_union;
    bool m_is_ip_v6;

    friend class endpoint;
};


class endpoint {
public:
    class list;

    class protocol protocol() const;
    class address address() const;
    unsigned short port() const;

    endpoint();
    ~endpoint() TIGHTDB_NOEXCEPT {}

private:
    class protocol m_protocol;

    typedef sockaddr     sockaddr_base_type;
    typedef sockaddr_in  sockaddr_ip_v4_type;
    typedef sockaddr_in6 sockaddr_ip_v6_type;
    union sockaddr_union_type {
        sockaddr_base_type  m_base;
        sockaddr_ip_v4_type m_ip_v4;
        sockaddr_ip_v6_type m_ip_v6;
    };
    sockaddr_union_type m_sockaddr_union;

    friend class resolver;
    friend class socket;
    friend class acceptor;
};


class endpoint::list {
public:
    typedef const endpoint* iterator;

    iterator begin() const;
    iterator end() const;

    ~list() TIGHTDB_NOEXCEPT {}

private:
    Buffer<endpoint> m_endpoints;

    friend class resolver;
};


class resolver {
public:
    class query;

    void resolve(const query&, endpoint::list&);
    error_code resolve(const query&, endpoint::list&, error_code&);

    ~resolver() TIGHTDB_NOEXCEPT {}
};


class resolver::query {
public:
    enum {
        ///< Locally bound socket endpoint
        passive = AI_PASSIVE,

        ///< Ignore families without a configured non-loopback address
        address_configured = AI_ADDRCONFIG
    };

    query(std::string service, int flags = passive|address_configured);
    query(const protocol&, std::string service, int flags = passive|address_configured);
    query(std::string host, std::string service, int flags = address_configured);
    query(const protocol&, std::string host, std::string service, int flags = address_configured);

    ~query() TIGHTDB_NOEXCEPT {}

    int flags() const;
    class protocol protocol() const;
    std::string host() const;
    std::string service() const;

private:
    int m_flags;
    class protocol m_protocol;
    std::string m_host;    // hostname
    std::string m_service; // port

    friend class resolver;
};


class socket {
public:
    socket();
    ~socket() TIGHTDB_NOEXCEPT;

    bool is_open() const;

    void open(const protocol&);
    error_code open(const protocol&, error_code&);

    void bind(const endpoint&);
    error_code bind(const endpoint&, error_code&);

    endpoint local_endpoint();
    endpoint local_endpoint(error_code&);

    void connect(const endpoint&);
    error_code connect(const endpoint&, error_code&);

    std::size_t read_some(char* buffer, std::size_t size);
    std::size_t read_some(char* buffer, std::size_t size, error_code&) TIGHTDB_NOEXCEPT;

    std::size_t write_some(const char* data, std::size_t size);
    std::size_t write_some(const char* data, std::size_t size, error_code&) TIGHTDB_NOEXCEPT;

    void close();
    error_code close(error_code&);

private:
    protocol m_protocol;
    int m_sock_fd;

    friend class acceptor;
};


class acceptor: private socket {
public:
    ~acceptor() TIGHTDB_NOEXCEPT {}

    bool is_open() const;

    void open(const protocol&);
    error_code open(const protocol&, error_code&);

    void bind(const endpoint&);
    error_code bind(const endpoint&, error_code&);

    endpoint local_endpoint();
    endpoint local_endpoint(error_code&);

    static const int max_connections = SOMAXCONN;

    void listen(int backlog = max_connections);
    error_code listen(int backlog, error_code&);

    void accept(socket&);
    void accept(socket&, endpoint&);
    error_code accept(socket&, error_code&);
    error_code accept(socket&, endpoint&, error_code&);

    void close();
    error_code close(error_code&);
};


class buffered_input_stream {
public:
    buffered_input_stream(socket&);
    ~buffered_input_stream() TIGHTDB_NOEXCEPT {}

    std::size_t read(char* buffer, std::size_t size);
    std::size_t read(char* buffer, std::size_t size, error_code&) TIGHTDB_NOEXCEPT;

    std::size_t read_until(char* buffer, std::size_t size, char delim);
    std::size_t read_until(char* buffer, std::size_t size, char delim,
                           error_code&) TIGHTDB_NOEXCEPT;

private:
    socket& m_socket;
    static const std::size_t s_buffer_size = 1024;
    UniquePtr<char[]> m_buffer;
    char* m_begin;
    char* m_end;
};


error_code write(socket&, const char* data, std::size_t size, error_code&) TIGHTDB_NOEXCEPT;
void write(socket&, const char* data, std::size_t size);


enum errors {
    /// End of input.
    end_of_input = 1,

    /// Host not found (authoritative).
    host_not_found,

    /// Host not found (non-authoritative).
    host_not_found_try_again,

    /// The query is valid but does not have associated address data.
    no_data,

    /// A non-recoverable error occurred.
    no_recovery,

    /// The service is not supported for the given socket type.
    service_not_found,

    /// The socket type is not supported.
    socket_type_not_supported
};

error_code make_error_code(errors);





// Implementation

inline protocol protocol::ip_v4()
{
    protocol prot;
    prot.m_family = AF_INET;
    return prot;
}

inline protocol protocol::ip_v6()
{
    protocol prot;
    prot.m_family = AF_INET6;
    return prot;
}

inline bool protocol::is_ip_v4() const
{
    return m_family == AF_INET;
}

inline bool protocol::is_ip_v6() const
{
    return m_family == AF_INET6;
}

inline protocol::protocol():
    m_family(AF_UNSPEC),     // Allow both IPv4 and IPv6
    m_socktype(SOCK_STREAM), // Or SOCK_DGRAM for UDP
    m_protocol(0)            // Any protocol
{
}

inline bool address::is_ip_v4() const
{
    return !m_is_ip_v6;
}

inline bool address::is_ip_v6() const
{
    return m_is_ip_v6;
}

template<class C, class T>
inline std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const address& addr)
{
    union buffer_union {
        char ip_v4[INET_ADDRSTRLEN];
        char ip_v6[INET6_ADDRSTRLEN];
    };
    char buffer[sizeof (buffer_union)];
    int af = addr.m_is_ip_v6 ? AF_INET6 : AF_INET;
    const char* ret = inet_ntop(af, &addr.m_union, buffer, sizeof buffer);
    if (ret == 0) {
        error_code ec = make_basic_system_error_code(errno);
        throw system_error(ec);
    }
    out << ret;
    return out;
}

inline address::address():
    m_is_ip_v6(false)
{
    m_union.m_ip_v4 = ip_v4_type();
}

inline protocol endpoint::protocol() const
{
    return m_protocol;
}

inline address endpoint::address() const
{
    class address addr;
    if (m_protocol.is_ip_v4()) {
        addr.m_union.m_ip_v4 = m_sockaddr_union.m_ip_v4.sin_addr;
    }
    else {
        addr.m_union.m_ip_v6 = m_sockaddr_union.m_ip_v6.sin6_addr;
        addr.m_is_ip_v6 = true;
    }
    return addr;
}

inline unsigned short endpoint::port() const
{
    return ntohs(m_protocol.is_ip_v4() ? m_sockaddr_union.m_ip_v4.sin_port :
                 m_sockaddr_union.m_ip_v6.sin6_port);
}

inline endpoint::endpoint():
    m_protocol(protocol::ip_v4())
{
    m_sockaddr_union.m_ip_v4 = sockaddr_ip_v4_type();
}

inline endpoint::list::iterator endpoint::list::begin() const
{
    return m_endpoints.data();
}

inline endpoint::list::iterator endpoint::list::end() const
{
    return m_endpoints.data() + m_endpoints.size();
}

inline void resolver::resolve(const query& q, endpoint::list& l)
{
    error_code ec;
    if (resolve(q, l, ec))
        throw system_error(ec);
}

inline resolver::query::query(std::string service, int flags):
    m_flags(flags),
    m_service(service)
{
}

inline resolver::query::query(const class protocol& prot, std::string service, int flags):
    m_flags(flags),
    m_protocol(prot),
    m_service(service)
{
}

inline resolver::query::query(std::string host, std::string service, int flags):
    m_flags(flags),
    m_host(host),
    m_service(service)
{
}

inline resolver::query::query(const class protocol& prot, std::string host, std::string service,
                              int flags):
    m_flags(flags),
    m_protocol(prot),
    m_host(host),
    m_service(service)
{
}

inline int resolver::query::flags() const
{
    return m_flags;
}

inline class protocol resolver::query::protocol() const
{
    return m_protocol;
}

inline std::string resolver::query::host() const
{
    return m_host;
}

inline std::string resolver::query::service() const
{
    return m_service;
}

inline socket::socket():
    m_sock_fd(-1)
{
}

inline socket::~socket() TIGHTDB_NOEXCEPT
{
    error_code ec;
    close(ec);
    // Ignore errors
}

inline bool socket::is_open() const
{
    return m_sock_fd != -1;
}

inline void socket::open(const protocol& prot)
{
    error_code ec;
    if (open(prot, ec))
        throw system_error(ec);
}

inline void socket::bind(const endpoint& ep)
{
    error_code ec;
    if (bind(ep, ec))
        throw system_error(ec);
}

inline endpoint socket::local_endpoint()
{
    error_code ec;
    endpoint ep = local_endpoint(ec);
    if (ec)
        throw system_error(ec);
    return ep;
}

inline void socket::connect(const endpoint& ep)
{
    error_code ec;
    if (connect(ep, ec))
        throw system_error(ec);
}

inline std::size_t socket::read_some(char* buffer, std::size_t size)
{
    error_code ec;
    std::size_t n = read_some(buffer, size, ec);
    if (ec)
        throw system_error(ec);
    return n;
}

inline std::size_t socket::write_some(const char* data, std::size_t size)
{
    error_code ec;
    std::size_t n = write_some(data, size, ec);
    if (ec)
        throw system_error(ec);
    return n;
}

inline void socket::close()
{
    error_code ec;
    if (close(ec))
        throw system_error(ec);
}

inline bool acceptor::is_open() const
{
    return socket::is_open();
}

inline void acceptor::open(const protocol& prot)
{
    socket::open(prot);
}

inline error_code acceptor::open(const protocol& prot, error_code& ec)
{
    return socket::open(prot, ec);
}

inline void acceptor::bind(const endpoint& ep)
{
    socket::bind(ep);
}

inline error_code acceptor::bind(const endpoint& ep, error_code& ec)
{
    return socket::bind(ep, ec);
}

inline endpoint acceptor::local_endpoint()
{
    return socket::local_endpoint();
}

inline endpoint acceptor::local_endpoint(error_code& ec)
{
    return socket::local_endpoint(ec);
}

inline void acceptor::listen(int backlog)
{
    error_code ec;
    if (listen(backlog, ec))
        throw system_error(ec);
}

inline void acceptor::accept(socket& sock)
{
    endpoint ep; // Dummy
    accept(sock, ep);
}

inline void acceptor::accept(socket& sock, endpoint& ep)
{
    error_code ec;
    if (accept(sock, ep, ec))
        throw system_error(ec);
}

inline error_code acceptor::accept(socket& sock, error_code& ec)
{
    endpoint ep; // Dummy
    return accept(sock, ep, ec);
}

inline void acceptor::close()
{
    socket::close();
}

inline error_code acceptor::close(error_code& ec)
{
    return socket::close(ec);
}

inline buffered_input_stream::buffered_input_stream(socket& sock):
    m_socket(sock),
    m_buffer(new char[s_buffer_size]),
    m_begin(m_buffer.get()),
    m_end(m_buffer.get())
{
}

inline std::size_t buffered_input_stream::read(char* buffer, std::size_t size)
{
    error_code ec;
    std::size_t n = read(buffer, size, ec);
    if (ec)
        throw system_error(ec);
    return n;
}

inline std::size_t buffered_input_stream::read_until(char* buffer, std::size_t size, char delim)
{
    error_code ec;
    std::size_t n = read_until(buffer, size, delim, ec);
    if (ec)
        throw system_error(ec);
    return n;
}

void write(socket& sock, const char* data, std::size_t size)
{
    error_code ec;
    if (write(sock, data, size, ec))
        throw system_error(ec);
}


} // namespace network
} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_NETWORK_HPP
